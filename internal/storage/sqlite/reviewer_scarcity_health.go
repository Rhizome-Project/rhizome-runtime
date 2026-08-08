package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

type ReviewerScarcityHealthSnapshot struct {
	State                      string   `json:"state,omitempty"`
	Message                    string   `json:"message,omitempty"`
	ReferenceAt                string   `json:"reference_at,omitempty"`
	WorkspaceCount             int      `json:"workspace_count"`
	SaturatedWorkspaceCount    int      `json:"saturated_workspace_count"`
	ScarceWorkspaceCount       int      `json:"scarce_workspace_count"`
	UnknownWorkspaceCount      int      `json:"unknown_workspace_count"`
	SaturatedWorkspaceExamples []string `json:"saturated_workspace_examples,omitempty"`
	ScarceWorkspaceExamples    []string `json:"scarce_workspace_examples,omitempty"`
	UnknownWorkspaceExamples   []string `json:"unknown_workspace_examples,omitempty"`
}

const reviewerScarcityHealthWorkspaceExampleLimit = 3

func appendReviewerScarcityWorkspaceExample(examples []string, workspaceID string) []string {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" || len(examples) >= reviewerScarcityHealthWorkspaceExampleLimit {
		return examples
	}
	return append(examples, workspaceID)
}

func reviewerScarcityHealthShouldCountUnknown(snapshot ReviewerMeshScarcitySnapshot) bool {
	if strings.ToUpper(strings.TrimSpace(snapshot.Status)) != "UNKNOWN" {
		return false
	}
	// Fresh or empty workspaces should not degrade service health just because
	// reviewer mesh evidence has not been established yet.
	if snapshot.RegisteredAgents == 0 &&
		snapshot.OnlineAgents == 0 &&
		snapshot.RegisteredTypedReviewers == 0 &&
		snapshot.OnlineTypedReviewers == 0 &&
		snapshot.LiveCoalitions == 0 &&
		snapshot.ActiveReviewerAssignments == 0 &&
		snapshot.OpenSessionCollaborationAssignments == 0 {
		return false
	}
	// Stopped workspaces can retain live coalition rows from a prior supervised
	// run. With no online agents and no open collaboration queue load, that
	// residue is not a live reviewer-scarcity condition.
	if snapshot.OnlineAgents == 0 &&
		snapshot.OpenSessionCollaborationAssignments == 0 {
		return false
	}
	// Early-stage workspaces with registered agents but no live reviewer load
	// should stay green until there is concrete reviewer-work evidence.
	if snapshot.LiveCoalitions == 0 &&
		snapshot.ActiveReviewerAssignments == 0 &&
		snapshot.OpenSessionCollaborationAssignments == 0 {
		return false
	}
	// A live coalition without assigned reviewer load is not, by itself,
	// reviewer scarcity when the roster has online reviewer capacity. First
	// supervised runs can legitimately create tensions before explicit reviewer
	// load is materialized; readiness should stay focused on real scarcity or
	// missing capacity evidence rather than unobserved utilization.
	if snapshot.LiveCoalitions > 0 &&
		snapshot.ActiveReviewerAssignments == 0 &&
		snapshot.OpenSessionCollaborationAssignments == 0 &&
		snapshot.OnlineTypedReviewers > 0 {
		return false
	}
	return true
}

func (s *Store) CurrentReviewerScarcityHealthSnapshot(ctx context.Context) ReviewerScarcityHealthSnapshot {
	referenceAt := time.Now().UTC()
	snapshot := ReviewerScarcityHealthSnapshot{
		State:       "ok",
		Message:     "no workspace has scarce or saturated reviewer scarcity",
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
	}
	if s == nil {
		snapshot.State = "unsupported"
		snapshot.Message = "sqlite store unavailable"
		return snapshot
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT workspace_id
		   FROM workspaces
		  WHERE status = ?
		  ORDER BY workspace_id ASC`,
		model.WorkspaceStatusActive,
	)
	if err != nil {
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("query workspaces for reviewer scarcity health: %v", err)
		return snapshot
	}
	defer rows.Close()

	for rows.Next() {
		var workspaceID string
		if err := rows.Scan(&workspaceID); err != nil {
			snapshot.State = "error"
			snapshot.Message = fmt.Sprintf("scan workspace for reviewer scarcity health: %v", err)
			return snapshot
		}
		workspaceID = strings.TrimSpace(workspaceID)
		if workspaceID == "" {
			continue
		}
		snapshot.WorkspaceCount++

		scarcity, err := s.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
		if err != nil {
			snapshot.State = "error"
			snapshot.Message = fmt.Sprintf("collect reviewer scarcity for workspace %s: %v", workspaceID, err)
			return snapshot
		}
		switch strings.ToUpper(strings.TrimSpace(scarcity.Status)) {
		case "SATURATED":
			snapshot.SaturatedWorkspaceCount++
			snapshot.SaturatedWorkspaceExamples = appendReviewerScarcityWorkspaceExample(snapshot.SaturatedWorkspaceExamples, workspaceID)
		case "SCARCE":
			snapshot.ScarceWorkspaceCount++
			snapshot.ScarceWorkspaceExamples = appendReviewerScarcityWorkspaceExample(snapshot.ScarceWorkspaceExamples, workspaceID)
		default:
			if !reviewerScarcityHealthShouldCountUnknown(scarcity) {
				continue
			}
			snapshot.UnknownWorkspaceCount++
			snapshot.UnknownWorkspaceExamples = appendReviewerScarcityWorkspaceExample(snapshot.UnknownWorkspaceExamples, workspaceID)
		}
	}
	if err := rows.Err(); err != nil {
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("iterate workspaces for reviewer scarcity health: %v", err)
		return snapshot
	}

	if snapshot.SaturatedWorkspaceCount > 0 || snapshot.ScarceWorkspaceCount > 0 || snapshot.UnknownWorkspaceCount > 0 {
		snapshot.State = "degraded"
		parts := []string{}
		if snapshot.SaturatedWorkspaceCount > 0 {
			parts = append(parts, fmt.Sprintf("saturated_workspaces=%d", snapshot.SaturatedWorkspaceCount))
		}
		if snapshot.ScarceWorkspaceCount > 0 {
			parts = append(parts, fmt.Sprintf("scarce_workspaces=%d", snapshot.ScarceWorkspaceCount))
		}
		if snapshot.UnknownWorkspaceCount > 0 {
			parts = append(parts, fmt.Sprintf("unknown_workspaces=%d", snapshot.UnknownWorkspaceCount))
		}
		if snapshot.SaturatedWorkspaceCount > 0 || snapshot.ScarceWorkspaceCount > 0 {
			snapshot.Message = "reviewer scarcity detected: " + strings.Join(parts, ", ")
		} else {
			snapshot.Message = "reviewer scarcity health is partial: " + strings.Join(parts, ", ")
		}
	}

	return snapshot
}
