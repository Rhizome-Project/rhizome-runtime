package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const projectClaimAdmissionOverlapHoldDuration = 2 * time.Minute
const taskClaimConflictHoldDuration = 45 * time.Second

func isProjectClaimOverlapAdmissionError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(text, "task claim project admission invalid") &&
		strings.Contains(text, "overlap")
}

func (r *Runtime) handleProjectClaimOverlapAdmissionError(ctx context.Context, task WorkspaceTaskRecord, err error) (bool, error) {
	if !isProjectClaimOverlapAdmissionError(err) {
		return false, nil
	}
	if r == nil {
		return true, nil
	}
	now := time.Now().UTC()
	alreadyHeld := r.projectClaimHoldActiveForTask(task, now)
	summary := projectClaimOverlapHoldSummary(task, err)
	if writeErr := r.recordProjectClaimHoldScratch(ctx, task, summary, now); writeErr != nil {
		return true, writeErr
	}
	if alreadyHeld || r == nil || r.client == nil {
		return true, nil
	}
	payload := projectClaimOverlapHoldPayload(r.cfg, task, err, summary, now)
	raw, _ := json.Marshal(payload)
	if err := r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		UpdateType:  "coordination",
		Summary:     summary,
		PayloadJSON: string(raw),
	}); err != nil {
		return true, err
	}
	return true, r.enqueueProjectClaimRepair(ctx, task, nil, err.Error())
}

func projectClaimOverlapHoldPayload(cfg RuntimeConfig, task WorkspaceTaskRecord, err error, summary string, now time.Time) map[string]any {
	conflictTaskID := projectClaimAdmissionErrorToken(err, "task_id")
	conflictBranchID := projectClaimAdmissionErrorToken(err, "branch_id")
	if now.IsZero() {
		now = time.Now().UTC()
	}
	holdUntil := now.Add(projectClaimAdmissionOverlapHoldDuration).UTC().Format(time.RFC3339Nano)
	payload := map[string]any{
		"delegation_state":        "delegation_project_claim_overlap",
		"project_id":              strings.TrimSpace(task.ProjectID),
		"task_id":                 strings.TrimSpace(task.TaskID),
		"blocked_task_id":         strings.TrimSpace(task.TaskID),
		"agent_id":                strings.TrimSpace(cfg.AgentID),
		"to_agent_id":             strings.TrimSpace(cfg.AgentID),
		"hold_kind":               "project_claim_overlap",
		"coordination_mode":       coordinationModeLabel(cfg),
		"coverage_state":          "covered_by_active_overlapping_claim",
		"summary":                 strings.TrimSpace(summary),
		"observed_error":          oneLine(errString(err)),
		"observed_at":             now.UTC().Format(time.RFC3339Nano),
		"hold_until":              holdUntil,
		"expires_at":              holdUntil,
		"do_not_retry_same_task":  true,
		"recommended_next_action": "treat this task as not delegated; wait for or inspect the active overlapping owner lane, or create a narrower non-overlapping follow-up",
	}
	if conflictTaskID != "" {
		payload["conflict_task_id"] = conflictTaskID
	}
	if conflictBranchID != "" {
		payload["conflict_branch_id"] = conflictBranchID
	}
	return payload
}

func projectClaimAdmissionErrorToken(err error, key string) string {
	text := errString(err)
	if strings.TrimSpace(text) == "" || strings.TrimSpace(key) == "" {
		return ""
	}
	needle := strings.TrimSpace(key) + "="
	idx := strings.Index(text, needle)
	if idx < 0 {
		return ""
	}
	value := text[idx+len(needle):]
	if end := strings.IndexAny(value, " \t\r\n,;)"); end >= 0 {
		value = value[:end]
	}
	return strings.Trim(strings.TrimSpace(value), "`\"'")
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func projectClaimOverlapTrustFirstSummary(task WorkspaceTaskRecord, err error) string {
	taskID := firstNonEmpty(strings.TrimSpace(task.TaskID), "unknown task")
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID != "" {
		return fmt.Sprintf("Trust-first advisory for task %s in project %s: overlapping project claim observed; treating scope conflict as coordination telemetry instead of passive hold.", taskID, projectID)
	}
	return fmt.Sprintf("Trust-first advisory for task %s: overlapping project claim observed; treating scope conflict as coordination telemetry instead of passive hold.", taskID)
}

func projectClaimOverlapHoldSummary(task WorkspaceTaskRecord, err error) string {
	taskID := firstNonEmpty(strings.TrimSpace(task.TaskID), "unknown task")
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID != "" {
		return fmt.Sprintf("Deferring project claim for task %s in project %s because an active overlapping implementation claim already owns this write scope.", taskID, projectID)
	}
	return fmt.Sprintf("Deferring project claim for task %s because an active overlapping implementation claim already owns this write scope.", taskID)
}

func (r *Runtime) recordProjectClaimHoldScratch(ctx context.Context, task WorkspaceTaskRecord, summary string, now time.Time) error {
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.ProjectClaimHoldKind = "project_claim_overlap"
		state.ProjectClaimHoldTaskID = strings.TrimSpace(task.TaskID)
		state.ProjectClaimHoldProjectID = strings.TrimSpace(task.ProjectID)
		state.ProjectClaimHoldUntil = now.Add(projectClaimAdmissionOverlapHoldDuration).Format(time.RFC3339Nano)
		state.ProjectClaimHoldSummary = strings.TrimSpace(summary)
		state.LastWakeTrigger = "project_claim_admission"
		state.LastWakeReason = "project_claim_overlap"
		state.LastWakeSummary = strings.TrimSpace(summary)
		state.LastWakeTaskID = strings.TrimSpace(task.TaskID)
		state.LastWakeSessionID = ""
		state.LastWakeAt = now.Format(time.RFC3339Nano)
		state.LastSummary = strings.TrimSpace(summary)
	})
}

func (r *Runtime) recordTaskClaimConflictHoldScratch(ctx context.Context, task WorkspaceTaskRecord, err error, now time.Time) error {
	if r == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	taskID := firstNonEmpty(strings.TrimSpace(task.TaskID), "unknown task")
	summary := fmt.Sprintf("Deferring task %s after claim conflict; another worker likely owns this lane.", taskID)
	if detail := strings.TrimSpace(errString(err)); detail != "" {
		summary += " " + oneLine(detail)
	}
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.ProjectClaimHoldKind = "task_claim_conflict"
		state.ProjectClaimHoldTaskID = strings.TrimSpace(task.TaskID)
		state.ProjectClaimHoldProjectID = strings.TrimSpace(task.ProjectID)
		state.ProjectClaimHoldUntil = now.Add(taskClaimConflictHoldDuration).Format(time.RFC3339Nano)
		state.ProjectClaimHoldSummary = strings.TrimSpace(summary)
		state.LastWakeTrigger = "task_claim_conflict"
		state.LastWakeReason = "task_claim_conflict"
		state.LastWakeSummary = strings.TrimSpace(summary)
		state.LastWakeTaskID = strings.TrimSpace(task.TaskID)
		state.LastWakeSessionID = ""
		state.LastWakeAt = now.Format(time.RFC3339Nano)
		state.LastSummary = strings.TrimSpace(summary)
	})
}

func (r *Runtime) projectClaimHoldActiveForTask(task WorkspaceTaskRecord, now time.Time) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return projectClaimHoldActive(r.scratch, task, now)
}

func projectClaimHoldActive(state RuntimeScratchState, task WorkspaceTaskRecord, now time.Time) bool {
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" || strings.TrimSpace(state.ProjectClaimHoldTaskID) != taskID {
		return false
	}
	if projectID := strings.TrimSpace(state.ProjectClaimHoldProjectID); projectID != "" && strings.TrimSpace(task.ProjectID) != "" && projectID != strings.TrimSpace(task.ProjectID) {
		return false
	}
	holdUntil, ok := parseRFC3339Nano(strings.TrimSpace(state.ProjectClaimHoldUntil))
	return ok && holdUntil != nil && now.Before(*holdUntil)
}

func (r *Runtime) clearProjectClaimHoldForTask(task WorkspaceTaskRecord) {
	if r == nil {
		return
	}
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if strings.TrimSpace(r.scratch.ProjectClaimHoldTaskID) != taskID {
		return
	}
	r.scratch.ProjectClaimHoldKind = ""
	r.scratch.ProjectClaimHoldTaskID = ""
	r.scratch.ProjectClaimHoldProjectID = ""
	r.scratch.ProjectClaimHoldUntil = ""
	r.scratch.ProjectClaimHoldSummary = ""
}
