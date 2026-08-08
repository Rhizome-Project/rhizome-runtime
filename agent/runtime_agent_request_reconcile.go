package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

var (
	runtimeSwitchAdmissionReconcileAttempts = 7
	runtimeSwitchAdmissionReconcileDelay    = 500 * time.Millisecond
)

type runtimeSwitchAdmissionReceipt struct {
	Task    WorkspaceTaskRecord
	Session AgentSessionStateRecord
	RunID   string
	Summary string
}

func (r *Runtime) confirmMaterializedRuntimeSwitchTask(ctx context.Context, taskID string) (runtimeSwitchAdmissionReceipt, bool, string) {
	taskID = strings.TrimSpace(taskID)
	if r == nil || r.client == nil || taskID == "" {
		return runtimeSwitchAdmissionReceipt{}, false, "runtime_switch_task admission state could not be reconciled: runtime/client/task_id unavailable"
	}
	attempts := runtimeSwitchAdmissionReconcileAttempts
	if attempts < 1 {
		attempts = 1
	}
	var lastDetail string
	for attempt := 0; attempt < attempts; attempt++ {
		receipt, ok, detail := r.runtimeSwitchAdmissionReceiptOnce(ctx, taskID)
		if ok {
			return receipt, true, ""
		}
		if strings.TrimSpace(detail) != "" {
			lastDetail = detail
		}
		if attempt+1 >= attempts || runtimeSwitchAdmissionReconcileDelay <= 0 {
			break
		}
		if !sleepContext(ctx, runtimeSwitchAdmissionReconcileDelay) {
			break
		}
	}
	if strings.TrimSpace(lastDetail) == "" {
		lastDetail = "target task did not expose same-agent CLAIMED ownership plus runnable session"
	}
	return runtimeSwitchAdmissionReceipt{}, false, lastDetail
}

func (r *Runtime) runtimeSwitchAdmissionReceiptOnce(ctx context.Context, taskID string) (runtimeSwitchAdmissionReceipt, bool, string) {
	if receipt, ok := r.localRuntimeSwitchAdmissionReceipt(ctx, taskID); ok {
		return receipt, true, ""
	}

	input := TaskHydrationInput{
		WorkspaceID:      r.cfg.WorkspaceID,
		TaskID:           taskID,
		UpdatesLimit:     1,
		ArtifactLimit:    1,
		RelatedTaskLimit: 1,
	}
	bundle, err := r.client.HydrateTask(ctx, input)
	if err != nil {
		return runtimeSwitchAdmissionReceipt{}, false, "hydrate failed during runtime_switch_task admission reconciliation: " + err.Error()
	}
	task := bundle.WorkspaceTask
	if task == nil || strings.TrimSpace(task.TaskID) != taskID {
		return runtimeSwitchAdmissionReceipt{}, false, fmt.Sprintf("hydrate returned task=%s", selectedTaskID(task))
	}
	claimAgentID := strings.TrimSpace(pointerValue(task.ClaimAgentID))
	claimStatus := taskClaimStatus(*task)
	if claimAgentID != strings.TrimSpace(r.cfg.AgentID) || !taskClaimStatusIsActiveOwnership(claimStatus) {
		return runtimeSwitchAdmissionReceipt{}, false, fmt.Sprintf("claim_agent_id=%s claim_status=%s", firstNonEmpty(claimAgentID, "<empty>"), firstNonEmpty(claimStatus, "<empty>"))
	}

	sessions, err := r.client.ListSessions(ctx, r.cfg.WorkspaceID, true, 100)
	if err != nil {
		return runtimeSwitchAdmissionReceipt{}, false, "session list failed during runtime_switch_task admission reconciliation: " + err.Error()
	}
	for _, session := range sessions {
		if strings.TrimSpace(session.AgentID) != strings.TrimSpace(r.cfg.AgentID) || strings.TrimSpace(session.TaskID) != taskID || !isSessionRunnable(&session) || strings.TrimSpace(session.SessionID) == "" {
			continue
		}
		receipt, ok, detail := r.adoptRuntimeSwitchAdmissionReceipt(ctx, *task, session, &bundle, "reconciled")
		if ok {
			return receipt, true, ""
		}
		return runtimeSwitchAdmissionReceipt{}, false, detail
	}
	return runtimeSwitchAdmissionReceipt{}, false, fmt.Sprintf("claim_agent_id=%s claim_status=%s but no runnable session for task", firstNonEmpty(claimAgentID, "<empty>"), firstNonEmpty(claimStatus, "<empty>"))
}

func (r *Runtime) localRuntimeSwitchAdmissionReceipt(ctx context.Context, taskID string) (runtimeSwitchAdmissionReceipt, bool) {
	r.mu.Lock()
	if r.activeTask == nil || r.activeSession == nil {
		r.mu.Unlock()
		return runtimeSwitchAdmissionReceipt{}, false
	}
	task := *r.activeTask
	session := *r.activeSession
	runID := strings.TrimSpace(r.activeRunID)
	r.mu.Unlock()
	if strings.TrimSpace(task.TaskID) != taskID || strings.TrimSpace(session.TaskID) != taskID || strings.TrimSpace(session.AgentID) != strings.TrimSpace(r.cfg.AgentID) || !isSessionRunnable(&session) || strings.TrimSpace(session.SessionID) == "" {
		return runtimeSwitchAdmissionReceipt{}, false
	}
	claimAgentID := strings.TrimSpace(pointerValue(task.ClaimAgentID))
	claimStatus := taskClaimStatus(task)
	if claimAgentID != strings.TrimSpace(r.cfg.AgentID) || !taskClaimStatusIsActiveOwnership(claimStatus) {
		return runtimeSwitchAdmissionReceipt{}, false
	}
	if runID == "" {
		var err error
		runID, err = r.ensureExecutionRunForMaterializedSession(ctx, task, session)
		if err != nil {
			return runtimeSwitchAdmissionReceipt{}, false
		}
	}
	summary := runtimeSwitchAdmissionSummary(taskID, claimAgentID, claimStatus, session.SessionID, "local")
	return runtimeSwitchAdmissionReceipt{Task: task, Session: session, RunID: runID, Summary: summary}, true
}

func (r *Runtime) adoptRuntimeSwitchAdmissionReceipt(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, hydration *TaskHydrationBundle, source string) (runtimeSwitchAdmissionReceipt, bool, string) {
	runID, err := r.ensureExecutionRunForMaterializedSession(ctx, task, session)
	if err != nil {
		return runtimeSwitchAdmissionReceipt{}, false, "execution run reconciliation failed: " + err.Error()
	}
	taskID := strings.TrimSpace(task.TaskID)
	claimAgentID := strings.TrimSpace(pointerValue(task.ClaimAgentID))
	claimStatus := taskClaimStatus(task)

	r.mu.Lock()
	r.activeTask = &task
	r.activeSession = &session
	r.activeRunID = runID
	r.scratch.ActiveTaskID = taskID
	r.scratch.ActiveSessionID = strings.TrimSpace(session.SessionID)
	r.scratch.ActiveRunID = runID
	r.scratch.LastWakeTrigger = "runtime_switch_task"
	r.scratch.LastWakeReason = "runtime_switch_task_reconciled"
	r.scratch.LastWakeSummary = runtimeSwitchAdmissionSummary(taskID, claimAgentID, claimStatus, session.SessionID, source)
	r.scratch.LastWakeTaskID = taskID
	r.scratch.LastWakeSessionID = strings.TrimSpace(session.SessionID)
	r.scratch.LastWakeAt = time.Now().UTC().Format(time.RFC3339Nano)
	r.scratch.LastSummary = r.scratch.LastWakeSummary
	if strings.TrimSpace(r.scratch.PendingTriggerTask) == taskID && normalizeWorkTrigger(r.scratch.PendingTrigger) == "runtime_switch_task" {
		clearPendingTriggerFields(&r.scratch)
	}
	if strings.TrimSpace(r.scratch.ProjectClaimHoldTaskID) == taskID {
		r.scratch.ProjectClaimHoldKind = ""
		r.scratch.ProjectClaimHoldTaskID = ""
		r.scratch.ProjectClaimHoldProjectID = ""
		r.scratch.ProjectClaimHoldUntil = ""
		r.scratch.ProjectClaimHoldSummary = ""
	}
	r.invalidateFocusLocked()
	stateCopy := r.scratch
	r.mu.Unlock()

	if hydration != nil {
		r.cacheHydration(hydration, TaskHydrationInput{
			WorkspaceID:      r.cfg.WorkspaceID,
			TaskID:           taskID,
			UpdatesLimit:     1,
			ArtifactLimit:    1,
			RelatedTaskLimit: 1,
		})
	} else {
		r.clearProjectClaimHoldForTask(task)
	}
	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		return runtimeSwitchAdmissionReceipt{}, false, "scratch reconciliation save failed: " + err.Error()
	}
	if err := r.syncCurrentContextDoc(ctx, &task, &session, nil); err != nil {
		return runtimeSwitchAdmissionReceipt{}, false, "current context reconciliation sync failed: " + err.Error()
	}
	if err := r.syncClaimedWorkDoc(ctx, &task, &session, runID, nil); err != nil {
		return runtimeSwitchAdmissionReceipt{}, false, "claimed work reconciliation sync failed: " + err.Error()
	}
	summary := runtimeSwitchAdmissionSummary(taskID, claimAgentID, claimStatus, session.SessionID, source)
	return runtimeSwitchAdmissionReceipt{Task: task, Session: session, RunID: runID, Summary: summary}, true, ""
}

func runtimeSwitchAdmissionSummary(taskID, claimAgentID, claimStatus, sessionID, source string) string {
	suffix := ""
	if strings.TrimSpace(source) != "" {
		suffix = " receipt_source=" + strings.TrimSpace(source)
	}
	return fmt.Sprintf("task_id=%s claim_agent_id=%s claim_status=%s active_session_id=%s%s",
		strings.TrimSpace(taskID),
		firstNonEmpty(strings.TrimSpace(claimAgentID), "<empty>"),
		firstNonEmpty(strings.TrimSpace(claimStatus), "<empty>"),
		firstNonEmpty(strings.TrimSpace(sessionID), "<empty>"),
		suffix,
	)
}
