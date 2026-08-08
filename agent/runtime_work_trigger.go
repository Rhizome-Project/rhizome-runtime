package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type pendingWorkTrigger struct {
	Trigger   string
	TaskID    string
	SessionID string
}

const authorityPreemptionYieldMarker = "runtime_authority_preemption_yield"

func normalizeWorkTrigger(trigger string) string {
	switch strings.ToLower(strings.TrimSpace(trigger)) {
	case "inbound_message":
		return "inbound_message"
	case "runtime_resume":
		return "runtime_resume"
	case "request_resume":
		return "request_resume"
	case "runtime_switch_task":
		return "runtime_switch_task"
	case "runtime_switch_tension":
		return "runtime_switch_tension"
	case "control_switch_task":
		return "control_switch_task"
	case "control_switch_tension":
		return "control_switch_tension"
	case "recovery":
		return "recovery"
	case "system_news":
		return "system_news"
	case "task_project_fields_updated":
		return "task_project_fields_updated"
	default:
		return ""
	}
}

func (r *Runtime) currentPendingWorkTrigger() pendingWorkTrigger {
	r.mu.Lock()
	defer r.mu.Unlock()
	return pendingWorkTrigger{
		Trigger:   normalizeWorkTrigger(r.scratch.PendingTrigger),
		TaskID:    strings.TrimSpace(r.scratch.PendingTriggerTask),
		SessionID: strings.TrimSpace(r.scratch.PendingTriggerSession),
	}
}

func (r *Runtime) setPendingWorkTrigger(ctx context.Context, trigger, taskID, sessionID string) error {
	trigger = normalizeWorkTrigger(trigger)
	if trigger == "" {
		return nil
	}
	taskID = strings.TrimSpace(taskID)
	sessionID = strings.TrimSpace(sessionID)
	incoming := pendingWorkTrigger{Trigger: trigger, TaskID: taskID, SessionID: sessionID}
	if r.shouldDropRequestResumeTrigger(incoming) {
		return nil
	}
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		if shouldPreservePendingAuthorityTransition(*state, incoming) {
			return
		}
		state.PendingTrigger = trigger
		state.PendingTriggerTask = taskID
		state.PendingTriggerSession = sessionID
		state.PendingTriggerAt = time.Now().UTC().Format(time.RFC3339Nano)
	}); err != nil {
		return err
	}
	r.markHydrationStale()
	r.invalidateFocus()
	r.wakePlanner()
	return nil
}

func (r *Runtime) setPendingWorkTriggerLocal(trigger, taskID, sessionID string) bool {
	trigger = normalizeWorkTrigger(trigger)
	if trigger == "" {
		return false
	}
	taskID = strings.TrimSpace(taskID)
	sessionID = strings.TrimSpace(sessionID)
	incoming := pendingWorkTrigger{Trigger: trigger, TaskID: taskID, SessionID: sessionID}
	if r.shouldDropRequestResumeTrigger(incoming) {
		return false
	}
	r.mu.Lock()
	if shouldPreservePendingAuthorityTransition(r.scratch, incoming) {
		r.mu.Unlock()
		r.markHydrationStale()
		r.invalidateFocus()
		r.wakePlanner()
		return true
	}
	r.scratch.PendingTrigger = trigger
	r.scratch.PendingTriggerTask = taskID
	r.scratch.PendingTriggerSession = sessionID
	r.scratch.PendingTriggerAt = time.Now().UTC().Format(time.RFC3339Nano)
	r.mu.Unlock()
	r.markHydrationStale()
	r.invalidateFocus()
	r.wakePlanner()
	return true
}

func (r *Runtime) clearPendingWorkTrigger(ctx context.Context) error {
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.PendingTrigger = ""
		state.PendingTriggerTask = ""
		state.PendingTriggerSession = ""
		state.PendingTriggerAt = ""
	}); err != nil {
		return err
	}
	r.invalidateFocus()
	return nil
}

func (r *Runtime) clearConsumedPendingWorkTrigger(ctx context.Context, trigger pendingWorkTrigger, selectedTaskID string) error {
	if normalizeWorkTrigger(trigger.Trigger) == "" || strings.TrimSpace(selectedTaskID) == "" {
		return nil
	}
	selectedTaskID = strings.TrimSpace(selectedTaskID)
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		if normalizeWorkTrigger(state.PendingTrigger) == "" {
			return
		}
		if normalizeWorkTrigger(state.PendingTrigger) != normalizeWorkTrigger(trigger.Trigger) {
			return
		}
		if strings.TrimSpace(state.PendingTriggerTask) != selectedTaskID {
			return
		}
		clearPendingTriggerFields(state)
	})
}

func (r *Runtime) shouldDropRequestResumeTrigger(trigger pendingWorkTrigger) bool {
	if r == nil || normalizeWorkTrigger(trigger.Trigger) != "request_resume" {
		return false
	}
	taskID := strings.TrimSpace(trigger.TaskID)
	if taskID == "" {
		return false
	}
	r.mu.Lock()
	tasks := append([]WorkspaceTaskRecord(nil), r.bootstrap.Snapshot.Tasks...)
	sessions := append([]AgentSessionStateRecord(nil), r.bootstrap.Snapshot.Sessions...)
	agentID := strings.TrimSpace(r.cfg.AgentID)
	var activeTask *WorkspaceTaskRecord
	if r.activeTask != nil {
		copy := *r.activeTask
		activeTask = &copy
	}
	var activeSession *AgentSessionStateRecord
	if r.activeSession != nil {
		copy := *r.activeSession
		activeSession = &copy
	}
	r.mu.Unlock()
	if activeTask != nil && strings.TrimSpace(activeTask.TaskID) == taskID {
		return !requestResumeActiveTargetRecoverableForAgent(*activeTask, activeSession, strings.TrimSpace(trigger.SessionID), agentID)
	}
	if len(tasks) == 0 {
		return false
	}
	task, ok := findBootstrapTask(tasks, taskID)
	if !ok {
		return false
	}
	if bootstrapTaskStatusIsTerminal(task.Status) {
		return true
	}
	if requestResumeTaskHasClaimEvidence(task) {
		if !requestResumeTaskRecoverableForAgent(task, agentID) {
			return true
		}
		if !requestResumeSessionRecoverableForAgent(sessions, strings.TrimSpace(trigger.SessionID), taskID, agentID) {
			return true
		}
		return false
	}
	return !requestResumeBootstrapSessionRecoverableForAgent(sessions, strings.TrimSpace(trigger.SessionID), taskID, agentID)
}

func requestResumeActiveTargetRecoverableForAgent(task WorkspaceTaskRecord, session *AgentSessionStateRecord, sessionID, agentID string) bool {
	if bootstrapTaskStatusIsTerminal(task.Status) {
		return false
	}
	if requestResumeTaskHasClaimEvidence(task) && !requestResumeTaskRecoverableForAgent(task, agentID) {
		return false
	}
	if strings.TrimSpace(sessionID) == "" {
		if requestResumeTaskHasClaimEvidence(task) {
			return true
		}
		return session != nil && requestResumeSessionRecordRecoverableForAgent(*session, strings.TrimSpace(task.TaskID), agentID)
	}
	if session == nil || strings.TrimSpace(session.SessionID) != strings.TrimSpace(sessionID) {
		return false
	}
	return requestResumeSessionRecordRecoverableForAgent(*session, strings.TrimSpace(task.TaskID), agentID)
}

func requestResumeTaskHasClaimEvidence(task WorkspaceTaskRecord) bool {
	return task.ClaimAgentID != nil || task.ClaimStatus != nil
}

func requestResumeBootstrapSessionRecoverableForAgent(sessions []AgentSessionStateRecord, sessionID, taskID, agentID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || len(sessions) == 0 {
		return false
	}
	return requestResumeSessionRecoverableForAgent(sessions, sessionID, taskID, agentID)
}

func requestResumeSessionRecordRecoverableForAgent(session AgentSessionStateRecord, taskID, agentID string) bool {
	if taskID != "" && strings.TrimSpace(session.TaskID) != strings.TrimSpace(taskID) {
		return false
	}
	if agentID != "" {
		sessionAgentID := strings.TrimSpace(session.AgentID)
		if sessionAgentID != "" && sessionAgentID != strings.TrimSpace(agentID) {
			return false
		}
	}
	switch strings.ToUpper(strings.TrimSpace(session.Status)) {
	case "ACTIVE", "RUNNING", "BLOCKED":
		return true
	default:
		return false
	}
}

func requestResumeTaskRecoverableForAgent(task WorkspaceTaskRecord, agentID string) bool {
	if bootstrapTaskStatusIsTerminal(task.Status) {
		return false
	}
	if !isClaimOwnedBy(task, agentID) {
		return false
	}
	switch taskClaimStatus(task) {
	case "CLAIMED", "BLOCKED":
		return true
	default:
		return false
	}
}

func requestResumeSessionRecoverableForAgent(sessions []AgentSessionStateRecord, sessionID, taskID, agentID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || len(sessions) == 0 {
		return true
	}
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	for i := range sessions {
		session := sessions[i]
		if strings.TrimSpace(session.SessionID) != sessionID {
			continue
		}
		return requestResumeSessionRecordRecoverableForAgent(session, taskID, agentID)
	}
	return false
}

func shouldPreservePendingAuthorityTransition(current RuntimeScratchState, incoming pendingWorkTrigger) bool {
	if normalizeWorkTrigger(current.PendingTrigger) != "runtime_switch_task" {
		return false
	}
	currentTaskID := strings.TrimSpace(current.PendingTriggerTask)
	if !isRuntimeAuthorityTransitionTaskID(currentTaskID) {
		return false
	}
	incoming.Trigger = normalizeWorkTrigger(incoming.Trigger)
	incoming.TaskID = strings.TrimSpace(incoming.TaskID)
	if incoming.Trigger == "" || incoming.TaskID == "" || incoming.TaskID == currentTaskID {
		return false
	}
	switch incoming.Trigger {
	case "request_resume", "runtime_resume", "system_news", "task_project_fields_updated", "recovery":
		return true
	default:
		return false
	}
}

func scratchPendingTriggerMatches(state RuntimeScratchState, trigger pendingWorkTrigger) bool {
	if normalizeWorkTrigger(state.PendingTrigger) != normalizeWorkTrigger(trigger.Trigger) {
		return false
	}
	stateTaskID := strings.TrimSpace(state.PendingTriggerTask)
	triggerTaskID := strings.TrimSpace(trigger.TaskID)
	if stateTaskID != "" || triggerTaskID != "" {
		return stateTaskID == triggerTaskID
	}
	return strings.TrimSpace(state.PendingTriggerSession) == strings.TrimSpace(trigger.SessionID)
}

func clearPendingTriggerFields(state *RuntimeScratchState) {
	if state == nil {
		return
	}
	state.PendingTrigger = ""
	state.PendingTriggerTask = ""
	state.PendingTriggerSession = ""
	state.PendingTriggerAt = ""
}

func clearConsumedPendingTriggerInState(state RuntimeScratchState) RuntimeScratchState {
	if scratchStatePendingTriggerConsumed(state) {
		clearPendingTriggerFields(&state)
	}
	return state
}

func scratchStatePendingTriggerConsumed(state RuntimeScratchState) bool {
	trigger := normalizeWorkTrigger(state.PendingTrigger)
	if trigger == "" {
		return false
	}
	activeTaskID := strings.TrimSpace(state.ActiveTaskID)
	pendingTaskID := strings.TrimSpace(state.PendingTriggerTask)
	if activeTaskID == "" || pendingTaskID == "" || activeTaskID != pendingTaskID {
		return false
	}
	if trigger == "runtime_switch_task" &&
		strings.TrimSpace(state.ActiveSessionID) != "" &&
		(strings.TrimSpace(state.PendingTriggerSession) == "" || strings.TrimSpace(state.PendingTriggerSession) == strings.TrimSpace(state.ActiveSessionID)) {
		return true
	}
	return normalizeWorkTrigger(state.LastWakeTrigger) == trigger
}

func isProjectRoleScopeAuthorityTaskID(taskID string) bool {
	return strings.HasPrefix(strings.TrimSpace(taskID), "task-role-scope-")
}

func isRuntimeAuthorityTransitionTaskID(taskID string) bool {
	taskID = strings.TrimSpace(taskID)
	return strings.HasPrefix(taskID, "task-role-scope-") ||
		strings.HasPrefix(taskID, "task-project-claim-repair-")
}

func (r *Runtime) pendingAuthorityTransitionPreemptsActiveTask() (pendingWorkTrigger, string, bool) {
	if r == nil {
		return pendingWorkTrigger{}, "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	trigger := pendingWorkTrigger{
		Trigger:   normalizeWorkTrigger(r.scratch.PendingTrigger),
		TaskID:    strings.TrimSpace(r.scratch.PendingTriggerTask),
		SessionID: strings.TrimSpace(r.scratch.PendingTriggerSession),
	}
	if trigger.Trigger != "runtime_switch_task" || !isRuntimeAuthorityTransitionTaskID(trigger.TaskID) {
		return pendingWorkTrigger{}, "", false
	}
	activeTaskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	if r.activeTask != nil {
		activeTaskID = firstNonEmpty(strings.TrimSpace(r.activeTask.TaskID), activeTaskID)
	}
	if activeTaskID == "" || activeTaskID == trigger.TaskID {
		return pendingWorkTrigger{}, "", false
	}
	if r.activeSession == nil || !isSessionRunnable(r.activeSession) {
		return pendingWorkTrigger{}, "", false
	}
	return trigger, activeTaskID, true
}

func authorityPreemptionYieldToolResult(toolName string, trigger pendingWorkTrigger, activeTaskID string) ToolResult {
	toolName = firstNonEmpty(strings.TrimSpace(toolName), "unknown")
	pendingTaskID := firstNonEmpty(strings.TrimSpace(trigger.TaskID), "unknown")
	activeTaskID = firstNonEmpty(strings.TrimSpace(activeTaskID), "unknown")
	return ToolResult{
		Output:  fmt.Sprintf("%s: blocked tool %s because pending runtime_switch_task for authority transition %s must materialize before continuing active task %s. Stop this task-cycle now and return outcome=continue with next_action=yield_to_pending_authority_transition; do not send agent_request, status, docs, or other coordination chatter from the preempted lane.", authorityPreemptionYieldMarker, toolName, pendingTaskID, activeTaskID),
		IsError: true,
	}
}

func authorityPreemptionYieldObserved(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	for _, receipt := range trace.ToolReceipts {
		if strings.Contains(receipt.Output, authorityPreemptionYieldMarker) {
			return true
		}
	}
	return false
}

func authorityPreemptionYieldResult(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace) StructuredTaskResult {
	if !authorityPreemptionYieldObserved(trace) {
		return result
	}
	taskID := firstNonEmpty(strings.TrimSpace(task.TaskID), "active task")
	result.Outcome = "continue"
	result.Summary = "Yielding active task cycle to pending authority transition"
	result.Details = appendAutonomyNote(result.Details, "Runtime observed a pending authority-bearing runtime_switch_task while "+taskID+" was still executing. Tool calls from the preempted lane were blocked so the planner can materialize the authority task as durable claim/session receipt.")
	result.NextAction = "Yield to the pending authority transition; the next planner cycle must claim/start the pending task before resuming this lane."
	result.RequiresHuman = false
	result.OwnerAction = ""
	result.HumanReason = ""
	result.DecisionType = ""
	return result
}

func (r *Runtime) wakePlanner() {
	if r == nil || r.eventWakePlanner == nil {
		return
	}
	select {
	case r.eventWakePlanner <- struct{}{}:
	default:
	}
}

func (r *Runtime) drainPlannerWake() {
	if r == nil || r.eventWakePlanner == nil {
		return
	}
	for {
		select {
		case <-r.eventWakePlanner:
		default:
			return
		}
	}
}

func (r *Runtime) maybeQueueResumeTrigger(ctx context.Context, trigger, taskID string) error {
	if r.runtimePaused() {
		return nil
	}
	r.mu.Lock()
	sessionID := ""
	if r.activeSession != nil {
		sessionID = strings.TrimSpace(r.activeSession.SessionID)
		if taskID == "" {
			taskID = strings.TrimSpace(r.activeSession.TaskID)
		}
	}
	r.mu.Unlock()
	if strings.TrimSpace(taskID) == "" && strings.TrimSpace(sessionID) == "" {
		return nil
	}
	return r.setPendingWorkTrigger(ctx, trigger, taskID, sessionID)
}

func (r *Runtime) queueSystemNewsTrigger(ctx context.Context) error {
	if r.runtimePaused() {
		return nil
	}
	r.mu.Lock()
	taskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	sessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	sessionStatus := ""
	if r.activeSession != nil {
		sessionID = firstNonEmpty(strings.TrimSpace(r.activeSession.SessionID), sessionID)
		taskID = firstNonEmpty(strings.TrimSpace(r.activeSession.TaskID), taskID)
		sessionStatus = strings.ToUpper(strings.TrimSpace(r.activeSession.Status))
	}
	if r.activeTask != nil {
		taskID = firstNonEmpty(strings.TrimSpace(r.activeTask.TaskID), taskID)
	}
	r.mu.Unlock()
	switch sessionStatus {
	case "BLOCKED", "WAITING_DECISION", "HANDOFF_PENDING":
		return nil
	}
	return r.setPendingWorkTrigger(ctx, "system_news", taskID, sessionID)
}
