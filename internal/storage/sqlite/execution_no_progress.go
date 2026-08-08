package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	executionNoProgressConsecutiveThreshold = 3
)

type ExecutionNoProgressSnapshot struct {
	State                    string `json:"state,omitempty"`
	Message                  string `json:"message,omitempty"`
	ReferenceAt              string `json:"reference_at,omitempty"`
	ActiveRunCount           int    `json:"active_run_count"`
	BlockedRunCount          int    `json:"blocked_run_count"`
	NoProgressRunCount       int    `json:"no_progress_run_count"`
	OldestNoProgressAt       string `json:"oldest_no_progress_at,omitempty"`
	TriggeredWorkspaceID     string `json:"triggered_workspace_id,omitempty"`
	TriggeredRunID           string `json:"triggered_run_id,omitempty"`
	TriggeredSessionID       string `json:"triggered_session_id,omitempty"`
	TriggeredTaskID          string `json:"triggered_task_id,omitempty"`
	TriggeredAgentID         string `json:"triggered_agent_id,omitempty"`
	TriggeredStepID          string `json:"triggered_step_id,omitempty"`
	TriggeredStepPhase       string `json:"triggered_step_phase,omitempty"`
	TriggeredStepStatus      string `json:"triggered_step_status,omitempty"`
	TriggeredStepTitle       string `json:"triggered_step_title,omitempty"`
	TriggeredConsecutiveRuns int    `json:"triggered_consecutive_runs"`
	RecommendedAction        string `json:"recommended_action,omitempty"`
}

type executionNoProgressCandidate struct {
	WorkspaceID     string
	RunID           string
	SessionID       string
	TaskID          string
	AgentID         string
	StepID          string
	StepPhase       string
	StepStatus      string
	StepTitle       string
	ConsecutiveRuns int
	RepeatedSince   time.Time
	LastUpdatedAt   time.Time
	CheckpointToken string
}

func (c *executionNoProgressCandidate) moreUrgent(other *executionNoProgressCandidate) bool {
	if c == nil {
		return false
	}
	if other == nil {
		return true
	}
	if c.ConsecutiveRuns != other.ConsecutiveRuns {
		return c.ConsecutiveRuns > other.ConsecutiveRuns
	}
	if !c.LastUpdatedAt.Equal(other.LastUpdatedAt) {
		return c.LastUpdatedAt.Before(other.LastUpdatedAt)
	}
	if c.WorkspaceID != other.WorkspaceID {
		return c.WorkspaceID < other.WorkspaceID
	}
	if c.RunID != other.RunID {
		return c.RunID < other.RunID
	}
	return c.StepID < other.StepID
}

func (s *Store) CurrentExecutionNoProgressSnapshot(ctx context.Context) ExecutionNoProgressSnapshot {
	referenceAt := time.Now().UTC()
	snapshot := ExecutionNoProgressSnapshot{
		State:             "ok",
		Message:           "no active execution runs are repeating the same durable progress signature",
		ReferenceAt:       referenceAt.Format(time.RFC3339Nano),
		RecommendedAction: "continue",
	}
	if s == nil {
		snapshot.State = "unsupported"
		snapshot.Message = "sqlite store unavailable"
		snapshot.RecommendedAction = "continue"
		return snapshot
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT r.workspace_id,
       r.run_id,
       COALESCE(r.session_id, ''),
       COALESCE(r.task_id, ''),
       COALESCE(r.agent_id, ''),
       r.status,
       r.updated_at
  FROM execution_runs r
  JOIN workspaces w
    ON w.workspace_id = r.workspace_id
 WHERE w.status = ?
   AND r.status IN (?, ?, ?)
 ORDER BY r.updated_at DESC, r.run_id DESC`,
		model.WorkspaceStatusActive,
		"ACTIVE",
		"BLOCKED",
		"VERIFYING",
	)
	if err != nil {
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("query active execution runs for no-progress health: %v", err)
		snapshot.RecommendedAction = "needs_operator"
		return snapshot
	}
	defer rows.Close()

	var oldestNoProgress time.Time
	var candidate *executionNoProgressCandidate
	for rows.Next() {
		var runWorkspaceID, runID, sessionID, taskID, agentID, runStatus, updatedAt string
		if err := rows.Scan(&runWorkspaceID, &runID, &sessionID, &taskID, &agentID, &runStatus, &updatedAt); err != nil {
			snapshot.State = "error"
			snapshot.Message = fmt.Sprintf("scan execution run for no-progress health: %v", err)
			snapshot.RecommendedAction = "needs_operator"
			return snapshot
		}
		snapshot.ActiveRunCount++

		detail, err := s.GetExecutionRun(ctx, runWorkspaceID, runID)
		if err != nil {
			snapshot.State = "error"
			snapshot.Message = fmt.Sprintf("load execution run %s/%s for no-progress health: %v", runWorkspaceID, runID, err)
			snapshot.RecommendedAction = "needs_operator"
			return snapshot
		}

		if strings.EqualFold(strings.TrimSpace(runStatus), "BLOCKED") {
			snapshot.BlockedRunCount++
		}
		consecutiveRuns, repeatedSince, latestStep, checkpointToken := executionNoProgressConsecutiveRuns(detail.Run, detail.Steps)
		if consecutiveRuns < executionNoProgressConsecutiveThreshold {
			continue
		}

		snapshot.NoProgressRunCount++
		repeatedSinceUTC := repeatedSince.UTC()
		if oldestNoProgress.IsZero() || repeatedSinceUTC.Before(oldestNoProgress) {
			oldestNoProgress = repeatedSinceUTC
		}

		nextCandidate := &executionNoProgressCandidate{
			WorkspaceID:     detail.Run.WorkspaceID,
			RunID:           detail.Run.RunID,
			SessionID:       detail.Run.SessionID,
			TaskID:          detail.Run.TaskID,
			AgentID:         detail.Run.AgentID,
			StepID:          latestStep.StepID,
			StepPhase:       latestStep.Phase,
			StepStatus:      latestStep.Status,
			StepTitle:       latestStep.Title,
			ConsecutiveRuns: consecutiveRuns,
			RepeatedSince:   repeatedSinceUTC,
			CheckpointToken: checkpointToken,
		}
		if updatedAt = strings.TrimSpace(detail.Run.UpdatedAt); updatedAt != "" {
			if parsed, parseErr := time.Parse(time.RFC3339Nano, updatedAt); parseErr == nil {
				nextCandidate.LastUpdatedAt = parsed
			}
		}
		if candidate == nil || nextCandidate.moreUrgent(candidate) {
			candidate = nextCandidate
		}
	}
	if err := rows.Err(); err != nil {
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("iterate execution runs for no-progress health: %v", err)
		snapshot.RecommendedAction = "needs_operator"
		return snapshot
	}

	if !oldestNoProgress.IsZero() {
		snapshot.OldestNoProgressAt = oldestNoProgress.UTC().Format(time.RFC3339Nano)
	}

	if snapshot.NoProgressRunCount > 0 {
		snapshot.State = "blocked"
		snapshot.RecommendedAction = "needs_operator"
		parts := []string{
			fmt.Sprintf("no_progress_runs=%d", snapshot.NoProgressRunCount),
			fmt.Sprintf("threshold=%d", executionNoProgressConsecutiveThreshold),
		}
		if snapshot.BlockedRunCount > 0 {
			parts = append(parts, fmt.Sprintf("blocked_runs=%d", snapshot.BlockedRunCount))
		}
		if snapshot.OldestNoProgressAt != "" {
			parts = append(parts, "oldest_no_progress_at="+snapshot.OldestNoProgressAt)
		}
		if candidate != nil {
			snapshot.TriggeredWorkspaceID = candidate.WorkspaceID
			snapshot.TriggeredRunID = candidate.RunID
			snapshot.TriggeredSessionID = candidate.SessionID
			snapshot.TriggeredTaskID = candidate.TaskID
			snapshot.TriggeredAgentID = candidate.AgentID
			snapshot.TriggeredStepID = candidate.StepID
			snapshot.TriggeredStepPhase = candidate.StepPhase
			snapshot.TriggeredStepStatus = candidate.StepStatus
			snapshot.TriggeredStepTitle = candidate.StepTitle
			snapshot.TriggeredConsecutiveRuns = candidate.ConsecutiveRuns
			parts = append(parts,
				"run="+candidate.RunID,
				"session="+candidate.SessionID,
				"step="+candidate.StepID,
				"phase="+candidate.StepPhase,
				"status="+candidate.StepStatus,
				"checkpoint="+candidate.CheckpointToken,
				fmt.Sprintf("consecutive_cycles=%d", candidate.ConsecutiveRuns),
			)
		}
		snapshot.Message = "durable no-progress detected: " + strings.Join(parts, ", ")
	}

	return snapshot
}

func executionNoProgressConsecutiveRuns(run ExecutionRunRecord, steps []ExecutionStepRecord) (int, time.Time, ExecutionStepRecord, string) {
	if len(steps) == 0 {
		return 0, time.Time{}, ExecutionStepRecord{}, ""
	}

	orderedSteps := append([]ExecutionStepRecord(nil), steps...)
	sort.SliceStable(orderedSteps, func(i, j int) bool {
		return executionNoProgressStepBefore(orderedSteps[i], orderedSteps[j])
	})

	latest := orderedSteps[len(orderedSteps)-1]
	latestSignature := executionNoProgressSignature(run, latest)
	consecutive := 1
	earliestIdx := len(orderedSteps) - 1

	for idx := len(orderedSteps) - 2; idx >= 0; idx-- {
		if executionNoProgressSignature(run, orderedSteps[idx]) != latestSignature {
			break
		}
		consecutive++
		earliestIdx = idx
	}

	repeatedSince := executionNoProgressStepTime(orderedSteps[earliestIdx])
	return consecutive, repeatedSince, latest, executionNoProgressCheckpointToken(run.VerificationJSON, latest.VerificationJSON)
}

func executionNoProgressSignature(run ExecutionRunRecord, step ExecutionStepRecord) string {
	fingerprint := map[string]any{
		"phase":      strings.TrimSpace(step.Phase),
		"status":     strings.TrimSpace(step.Status),
		"checkpoint": executionNoProgressCheckpointToken(run.VerificationJSON, step.VerificationJSON),
	}
	if raw, err := json.Marshal(fingerprint); err == nil {
		return string(raw)
	}
	return fmt.Sprintf("%s|%s|%s",
		strings.TrimSpace(step.Phase),
		strings.TrimSpace(step.Status),
		executionNoProgressCheckpointToken(run.VerificationJSON, step.VerificationJSON),
	)
}

func executionNoProgressCheckpointToken(values ...map[string]any) string {
	keys := []string{
		"checkpoint_id",
		"checkpoint",
		"progress_id",
		"progress",
		"cursor_id",
		"cursor",
		"step_id",
		"next_step_id",
		"coordination_revision_id",
		"evidence_revision_id",
		"patch_queue_revision_id",
		"operation_id",
		"cas_patch_digest",
		"materialization_digest",
		"reviewer_advisory_digest",
		"operator_enablement_digest",
		"frontier_generation_id",
		"role_request_task_id",
		"reset_epoch",
	}
	parts := make([]string, 0, len(values)*len(keys))
	for _, valueMap := range values {
		if len(valueMap) == 0 {
			continue
		}
		for _, key := range keys {
			if token := executionNoProgressJSONToken(valueMap[key]); token != "" {
				parts = append(parts, key+"="+token)
			}
		}
	}
	return strings.Join(parts, ";")
}

func executionNoProgressJSONToken(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case []byte:
		return strings.TrimSpace(string(typed))
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(raw))
	}
}

func executionNoProgressStepTime(step ExecutionStepRecord) time.Time {
	if step.UpdatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(step.UpdatedAt)); err == nil {
			return parsed
		}
	}
	if step.CreatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(step.CreatedAt)); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func executionNoProgressStepBefore(left, right ExecutionStepRecord) bool {
	leftTime := executionNoProgressStepTime(left)
	rightTime := executionNoProgressStepTime(right)
	if !leftTime.Equal(rightTime) {
		if leftTime.IsZero() {
			return true
		}
		if rightTime.IsZero() {
			return false
		}
		return leftTime.Before(rightTime)
	}
	if left.SortOrder != right.SortOrder {
		return left.SortOrder < right.SortOrder
	}
	if left.Phase != right.Phase {
		return left.Phase < right.Phase
	}
	return left.StepID < right.StepID
}
