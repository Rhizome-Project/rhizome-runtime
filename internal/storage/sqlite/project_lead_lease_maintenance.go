package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	projectLeadLeaseMaintenanceRenewBefore  = 15 * time.Minute
	projectLeadLeaseMaintenanceLeaseSeconds = 60 * 60
)

type ProjectStrategicLeadLeaseMaintenanceInput struct {
	Scope       string
	ActorType   string
	ActorID     string
	ReferenceAt string
	Limit       int
}

type ProjectStrategicLeadLeaseMaintenanceItem struct {
	WorkspaceID            string `json:"workspace_id"`
	ProjectID              string `json:"project_id"`
	AgentID                string `json:"agent_id"`
	PreviousRoleID         string `json:"previous_role_id"`
	RoleID                 string `json:"role_id,omitempty"`
	PreviousLeaseExpiresAt string `json:"previous_lease_expires_at,omitempty"`
	LeaseExpiresAt         string `json:"lease_expires_at,omitempty"`
	LastSeenAt             string `json:"last_seen_at,omitempty"`
	Action                 string `json:"action"`
	SkipReason             string `json:"skip_reason,omitempty"`
	Error                  string `json:"error,omitempty"`
}

type ProjectStrategicLeadLeaseMaintenanceResult struct {
	Scope       string                                     `json:"scope"`
	ReferenceAt string                                     `json:"reference_at"`
	Scanned     int                                        `json:"scanned"`
	Renewed     int                                        `json:"renewed"`
	Skipped     int                                        `json:"skipped"`
	Problems    int                                        `json:"problems"`
	Items       []ProjectStrategicLeadLeaseMaintenanceItem `json:"items,omitempty"`
}

type projectStrategicLeadLeaseMaintenanceCandidate struct {
	Role       ProjectRoleRecord
	LastSeenAt string
}

func (s *Store) ReconcileProjectStrategicLeadLeases(ctx context.Context, input ProjectStrategicLeadLeaseMaintenanceInput) (ProjectStrategicLeadLeaseMaintenanceResult, error) {
	if s == nil || s.db == nil {
		return ProjectStrategicLeadLeaseMaintenanceResult{}, errors.New("store is nil")
	}
	referenceAt, err := parseSessionReclaimReferenceAt(input.ReferenceAt)
	if err != nil {
		return ProjectStrategicLeadLeaseMaintenanceResult{}, err
	}
	limit := input.Limit
	if limit <= 0 {
		limit = 32
	}
	if limit > 256 {
		limit = 256
	}
	scope := strings.TrimSpace(input.Scope)
	if scope == "" {
		scope = authorityScopeWorkspace
	}
	result := ProjectStrategicLeadLeaseMaintenanceResult{
		Scope:       scope,
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
		Items:       make([]ProjectStrategicLeadLeaseMaintenanceItem, 0, limit),
	}
	candidates, err := s.projectStrategicLeadLeaseMaintenanceCandidates(ctx, referenceAt, limit)
	if err != nil {
		return result, err
	}
	for _, candidate := range candidates {
		role := candidate.Role
		item := ProjectStrategicLeadLeaseMaintenanceItem{
			WorkspaceID:            role.WorkspaceID,
			ProjectID:              role.ProjectID,
			AgentID:                role.AgentID,
			PreviousRoleID:         role.RoleID,
			PreviousLeaseExpiresAt: role.LeaseExpiresAt,
			LastSeenAt:             candidate.LastSeenAt,
		}
		result.Scanned++
		fresh := projectStrategicLeadMaintenanceAgentFreshAt(candidate.LastSeenAt, referenceAt)
		if !fresh {
			// R50 blocker-1(B): a lead holding a live NON-reflection claim in its project is
			// demonstrably mid-product-work and is not a zombie, even if its heartbeat (last_seen_at)
			// lagged, allowing a degraded heartbeat loop to let the lease lapse
			// on a working lead. Idle-reflection claims are excluded (see the helper) so a genuinely
			// idle lead at the empty frontier is NOT kept alive forever.
			live, liveErr := s.projectLeadAgentHasLiveNonReflectionClaim(ctx, role.WorkspaceID, role.ProjectID, role.AgentID)
			if liveErr != nil {
				item.Action = "error"
				item.Error = liveErr.Error()
				result.Problems++
				result.Items = append(result.Items, item)
				continue
			}
			fresh = live
		}
		if !fresh {
			item.Action = "skipped"
			item.SkipReason = "lead_agent_stale"
			result.Skipped++
			result.Items = append(result.Items, item)
			continue
		}
		renewed, _, err := s.ClaimProjectStrategicLeadWithEvent(ctx, ProjectLeadClaimInput{
			WorkspaceID:  role.WorkspaceID,
			ProjectID:    role.ProjectID,
			AgentID:      role.AgentID,
			ActorID:      role.AgentID,
			ActorType:    "agent",
			LeaseSeconds: projectLeadLeaseMaintenanceLeaseSeconds,
			Summary:      "Lease maintenance refreshed a fresh strategic lead before implementation gates closed.",
			PromptContextEnvelope: BuildProjectPromptContextEnvelope(
				"project.lead.claim",
				"server_rpc",
				role.WorkspaceID,
				"agent",
				role.AgentID,
			),
			PromptContextSurface: "project.lead.claim",
		})
		if err != nil {
			item.Action = "error"
			item.Error = err.Error()
			result.Problems++
			result.Items = append(result.Items, item)
			continue
		}
		item.Action = "renewed"
		item.RoleID = renewed.RoleID
		item.LeaseExpiresAt = renewed.LeaseExpiresAt
		result.Renewed++
		result.Items = append(result.Items, item)
	}
	return result, nil
}

func (s *Store) projectStrategicLeadLeaseMaintenanceCandidates(ctx context.Context, referenceAt time.Time, limit int) ([]projectStrategicLeadLeaseMaintenanceCandidate, error) {
	renewBefore := referenceAt.Add(projectLeadLeaseMaintenanceRenewBefore).Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `
SELECT r.role_id, r.workspace_id, r.project_id, r.agent_id, r.role_type, r.status, r.write_scope_json,
       r.lease_token, r.lease_expires_at, r.summary, r.claimed_at, r.released_at, r.updated_by,
       r.created_at, r.updated_at, COALESCE(a.last_seen_at, '')
  FROM project_agent_roles r
  LEFT JOIN agents a ON a.workspace_id = r.workspace_id AND a.agent_id = r.agent_id
 WHERE r.role_type = ? AND r.status = ? AND r.lease_expires_at <> '' AND r.lease_expires_at <= ?
 ORDER BY r.lease_expires_at ASC, r.updated_at ASC
 LIMIT ?`,
		ProjectRoleStrategicLead, ProjectRoleStatusActive, renewBefore, limit)
	if err != nil {
		return nil, fmt.Errorf("list project strategic lead lease maintenance candidates: %w", err)
	}
	defer rows.Close()
	candidates := make([]projectStrategicLeadLeaseMaintenanceCandidate, 0)
	for rows.Next() {
		var role ProjectRoleRecord
		var lastSeenAt sql.NullString
		if err := rows.Scan(
			&role.RoleID,
			&role.WorkspaceID,
			&role.ProjectID,
			&role.AgentID,
			&role.RoleType,
			&role.Status,
			&role.WriteScopeJSON,
			&role.LeaseToken,
			&role.LeaseExpiresAt,
			&role.Summary,
			&role.ClaimedAt,
			&role.ReleasedAt,
			&role.UpdatedBy,
			&role.CreatedAt,
			&role.UpdatedAt,
			&lastSeenAt,
		); err != nil {
			return nil, fmt.Errorf("scan project strategic lead lease maintenance candidate: %w", err)
		}
		candidates = append(candidates, projectStrategicLeadLeaseMaintenanceCandidate{
			Role:       role,
			LastSeenAt: strings.TrimSpace(lastSeenAt.String),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate project strategic lead lease maintenance candidates: %w", err)
	}
	return candidates, nil
}

func projectStrategicLeadMaintenanceAgentFreshAt(lastSeenAt string, referenceAt time.Time) bool {
	lastSeenAt = strings.TrimSpace(lastSeenAt)
	if lastSeenAt == "" {
		return false
	}
	seenAt, err := time.Parse(time.RFC3339Nano, lastSeenAt)
	if err != nil {
		return false
	}
	if seenAt.After(referenceAt) {
		return true
	}
	return referenceAt.Sub(seenAt) < projectClaimRepairLeadStaleAfter
}

// projectLeadAgentHasLiveNonReflectionClaim (R50 blocker-1(B)) reports whether the lead agent holds
// a non-terminal claim on a NON-reflection task in its project - an alternative liveness signal to
// last_seen_at. A lead demonstrably mid-product-work is not a zombie even if its heartbeat lagged.
// Idle-reflection / proactive-metacognition tasks are EXCLUDED: at the empty product frontier the
// only non-terminal task the lead holds is the idle-reflection itself, so counting it would keep a
// genuinely-idle lead alive forever (the zombie-lead the lease TTL exists to prevent).
func (s *Store) projectLeadAgentHasLiveNonReflectionClaim(ctx context.Context, workspaceID, projectID, agentID string) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	agentID = strings.TrimSpace(agentID)
	if s == nil || s.db == nil || workspaceID == "" || projectID == "" || agentID == "" {
		return false, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT t.task_id, COALESCE(t.title, ''), COALESCE(t.description, ''), t.task_kind, t.task_template,
       COALESCE(t.task_class, ''), COALESCE(t.tags_json, '[]'), COALESCE(t.task_requirements_json, '{}')
  FROM task_claims tc
  JOIN tasks t ON t.task_id = tc.task_id
 WHERE tc.workspace_id = ?
   AND tc.agent_id = ?
   AND COALESCE(t.project_id, '') = ?
   AND tc.claim_status NOT IN (?, ?, ?, ?)
   AND t.status NOT IN (?, ?, ?)`,
		workspaceID, agentID, projectID,
		model.TaskClaimStatusReleased, model.TaskClaimStatusCompleted, model.TaskClaimStatusFailed, model.TaskClaimStatusCancelled,
		model.TaskStatusResolved, model.TaskStatusCancelled, model.TaskStatusFailed)
	if err != nil {
		return false, fmt.Errorf("query lead agent live claims: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var task WorkspaceTaskRecord
		var tagsJSON string
		if err := rows.Scan(
			&task.TaskID, &task.Title, &task.Description, &task.TaskKind, &task.TaskTemplate,
			&task.TaskClass, &tagsJSON, &task.TaskRequirementsJSON,
		); err != nil {
			return false, fmt.Errorf("scan lead agent live claim: %w", err)
		}
		task.Tags = parseTaskTagsJSON(tagsJSON)
		if !agentWorkTaskIsProactiveMetacognition(task) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate lead agent live claims: %w", err)
	}
	return false, nil
}
