package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type projectCoordinationEventRefs struct {
	ProjectID string
	TaskID    string
	SessionID string
}

func (r *Runtime) prepareStartupPlannerWakeLocked(now time.Time) bool {
	if r == nil || r.scratch.ControlPaused {
		return false
	}

	taskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	sessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	if taskID == "" && sessionID == "" {
		if normalizeWorkTrigger(r.scratch.PendingTrigger) == "request_resume" {
			r.scratch.PendingTrigger = ""
			r.scratch.PendingTriggerTask = ""
			r.scratch.PendingTriggerSession = ""
			r.scratch.PendingTriggerAt = ""
		}
		r.clearContinuationHoldLocked()
		r.clearHydrationLocked()
		r.activeWorkPacket = nil
		r.invalidateFocusLocked()
		return true
	}

	if !shouldPreservePendingAuthorityTransition(r.scratch, pendingWorkTrigger{Trigger: "request_resume", TaskID: taskID, SessionID: sessionID}) {
		r.scratch.PendingTrigger = "request_resume"
		r.scratch.PendingTriggerTask = taskID
		r.scratch.PendingTriggerSession = sessionID
		r.scratch.PendingTriggerAt = now.UTC().Format(time.RFC3339Nano)
	}
	r.clearContinuationHoldLocked()
	r.clearHydrationLocked()
	r.activeWorkPacket = nil
	r.invalidateFocusLocked()
	return true
}

func (r *Runtime) handleProjectCoordinationRuntimeEvent(ctx context.Context, evt RhizomeEvent) error {
	if r == nil || r.runtimePaused() {
		return nil
	}
	if !isProjectCoordinationWakeEvent(evt.Type) {
		return nil
	}
	if evt.WorkspaceID != "" && r.cfg.WorkspaceID != "" && evt.WorkspaceID != r.cfg.WorkspaceID {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(evt.Type), "task.blocked") && strings.EqualFold(projectCoordinationAgentIDFromEvent(evt), strings.TrimSpace(r.cfg.AgentID)) {
		return nil
	}
	if _, err := r.handleGovernanceVoteOpenRuntimeEvent(ctx, evt); err != nil {
		return err
	}

	refs := projectCoordinationRefsFromEvent(evt)
	if strings.EqualFold(strings.TrimSpace(evt.Type), "project.role.assigned") &&
		strings.EqualFold(projectCoordinationAssignedRoleAgentIDFromEvent(evt), strings.TrimSpace(r.cfg.AgentID)) {
		if _, err := r.clearProjectClaimHoldForRefs(ctx, refs); err != nil {
			return err
		}
	}
	taskID, sessionID, queueResume := r.projectCoordinationWakeTarget(refs)
	r.invalidateBootstrap()
	if !queueResume {
		r.wakePlanner()
		return nil
	}
	return r.setPendingWorkTrigger(ctx, "request_resume", taskID, sessionID)
}

func isProjectCoordinationWakeEvent(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "project.updated",
		"project.phase.transitioned",
		"task.created",
		"task.claimed",
		"task.blocked",
		"task.completed",
		"task.released",
		"task.closed",
		"project.lead.claimed",
		"project.lead.renewed",
		"project.lead.released",
		"project.lead.transferred",
		"project.lead.changed",
		"project.role.assigned",
		"project.role.released",
		"project.role.changed",
		"project.repository.upserted",
		"project.repository.changed",
		"project.checkout.registered",
		"project.checkout.changed",
		"project.branch.registered",
		"project.branch.changed",
		"governance.challenge.raised",
		"governance.challenge.defended",
		"governance.vote.cast",
		"governance.challenge.resolved",
		"governance.challenge.changed",
		"project.patch_queue.submitted",
		"project.patch_queue.changed",
		"project.patch_queue.claimed",
		"project.patch_queue.released",
		"project.patch_queue.operation_bound",
		"project.patch_queue.cas_recorded",
		"project.patch_queue.materialization_recorded",
		"project.patch_queue.rollback_recorded",
		"project.patch_queue.reviewer_advisory_recorded",
		"project.patch_queue.operator_enablement_recorded",
		"project.patch_queue.decided",
		"project.patch_queue.actuator_started",
		"project.patch_queue.actuator_applied",
		"workspace.ops.updated",
		"workspace.ops.resolved",
		"workspace.ops.escalated":
		return true
	default:
		return false
	}
}

func projectCoordinationRefsFromEvent(evt RhizomeEvent) projectCoordinationEventRefs {
	var payload map[string]any
	if raw := strings.TrimSpace(evt.PayloadJSON); raw != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}

	refs := projectCoordinationEventRefs{
		ProjectID: payloadStringField(payload, "project_id", "projectID"),
		TaskID:    payloadStringField(payload, "task_id", "taskID"),
		SessionID: payloadStringField(payload, "session_id", "sessionID"),
	}
	if refs.ProjectID == "" {
		refs.ProjectID = payloadStringFieldFromObject(payload, "repository", "project_id", "projectID")
	}
	if refs.ProjectID == "" {
		refs.ProjectID = payloadStringFieldFromObject(payload, "checkout", "project_id", "projectID")
	}
	if refs.ProjectID == "" {
		refs.ProjectID = payloadStringFieldFromObject(payload, "branch", "project_id", "projectID")
	}
	if refs.ProjectID == "" && strings.EqualFold(payloadStringField(payload, "source_kind", "sourceKind"), "project") {
		refs.ProjectID = payloadStringField(payload, "source_id", "sourceID")
	}
	if refs.TaskID == "" && strings.EqualFold(payloadStringField(payload, "source_kind", "sourceKind"), "task") {
		refs.TaskID = payloadStringField(payload, "source_id", "sourceID")
	}
	if refs.SessionID == "" && strings.EqualFold(payloadStringField(payload, "source_kind", "sourceKind"), "session") {
		refs.SessionID = payloadStringField(payload, "source_id", "sourceID")
	}
	return refs
}

func projectCoordinationAgentIDFromEvent(evt RhizomeEvent) string {
	if agentID := strings.TrimSpace(evt.AgentID); agentID != "" {
		return agentID
	}
	var payload map[string]any
	if raw := strings.TrimSpace(evt.PayloadJSON); raw != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	return payloadStringField(payload, "agent_id", "agentID", "actor_id", "actorID")
}

func projectCoordinationAssignedRoleAgentIDFromEvent(evt RhizomeEvent) string {
	var payload map[string]any
	if raw := strings.TrimSpace(evt.PayloadJSON); raw != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	if agentID := payloadStringField(payload, "agent_id", "agentID"); agentID != "" {
		return agentID
	}
	return payloadStringFieldFromObject(payload, "role", "agent_id", "agentID")
}

func (r *Runtime) clearProjectClaimHoldForRefs(ctx context.Context, refs projectCoordinationEventRefs) (bool, error) {
	if r == nil {
		return false, nil
	}
	r.mu.Lock()
	shouldClear := projectClaimHoldClearMatchesRoleAssignment(r.scratch, refs)
	r.mu.Unlock()
	if !shouldClear {
		return false, nil
	}
	cleared := false
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		if !projectClaimHoldClearMatchesRoleAssignment(*state, refs) {
			return
		}
		state.ProjectClaimHoldKind = ""
		state.ProjectClaimHoldTaskID = ""
		state.ProjectClaimHoldProjectID = ""
		state.ProjectClaimHoldUntil = ""
		state.ProjectClaimHoldSummary = ""
		cleared = true
	}); err != nil {
		return false, err
	}
	if cleared {
		r.mu.Lock()
		r.clearHydrationLocked()
		r.activeWorkPacket = nil
		r.clearContinuationHoldLocked()
		r.invalidateFocusLocked()
		r.mu.Unlock()
	}
	return cleared, nil
}

func projectClaimHoldClearMatchesRoleAssignment(state RuntimeScratchState, refs projectCoordinationEventRefs) bool {
	holdKind := strings.TrimSpace(state.ProjectClaimHoldKind)
	if holdKind == "" {
		holdKind = strings.TrimSpace(state.LastWakeReason)
	}
	if !strings.EqualFold(holdKind, "project_claim_overlap") {
		return false
	}
	refTaskID := strings.TrimSpace(refs.TaskID)
	refProjectID := strings.TrimSpace(refs.ProjectID)
	if refTaskID == "" && refProjectID == "" {
		return false
	}
	holdTaskID := strings.TrimSpace(state.ProjectClaimHoldTaskID)
	if holdTaskID == "" {
		return false
	}
	if refTaskID != "" && refTaskID != holdTaskID {
		return false
	}
	holdProjectID := strings.TrimSpace(state.ProjectClaimHoldProjectID)
	if refProjectID != "" && holdProjectID != "" && refProjectID != holdProjectID {
		return false
	}
	return true
}

func (r *Runtime) handleGovernanceVoteOpenRuntimeEvent(ctx context.Context, evt RhizomeEvent) (bool, error) {
	if r == nil {
		return false, nil
	}
	eventType := strings.TrimSpace(evt.Type)
	if eventType != "governance.challenge.defended" && eventType != "governance.challenge.changed" {
		return false, nil
	}
	var payload map[string]any
	if raw := strings.TrimSpace(evt.PayloadJSON); raw != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	state := payloadStringField(payload, "state")
	if state == "" {
		state = payloadStringFieldFromObject(payload, "challenge", "state")
	}
	if !strings.EqualFold(state, "VOTING") {
		return false, nil
	}
	signal := governanceVoteOpenRuntimeAdvisorySignal(payload)
	if signal == "" {
		return false, nil
	}
	changed := false
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		var next []string
		next, changed = appendGovernanceVoteOpenRuntimeAdvisory(state.AdvisorySignals, signal, governancePayloadChallengeID(payload))
		state.AdvisorySignals = next
		if changed {
			state.LastSummary = "Governance vote opened"
		}
	}); err != nil {
		return false, err
	}
	if changed {
		r.invalidateFocus()
		r.markBootstrapStale()
	}
	return changed, nil
}

func governanceVoteOpenRuntimeAdvisorySignal(payload map[string]any) string {
	challengeID := governancePayloadChallengeID(payload)
	projectID := firstNonEmpty(payloadStringField(payload, "project_id"), payloadStringFieldFromObject(payload, "challenge", "project_id"))
	if challengeID == "" || projectID == "" {
		return ""
	}
	round := payloadIntField(payload, "current_round")
	if round <= 0 {
		round = payloadIntFieldFromObject(payload, "challenge", "current_round")
	}
	if round <= 0 {
		round = 1
	}
	parts := []string{
		fmt.Sprintf("GOVERNANCE VOTE OPEN: challenge %s for project %s is in VOTING round %d.", challengeID, projectID, round),
		"Use project_governance_challenge action=list to inspect active challenges, then cast one vote with action=vote ballot=UPHOLD_LEAD|REASSIGN|ABSTAIN.",
	}
	if doc := firstNonEmpty(payloadStringField(payload, "argument_doc_key"), payloadStringFieldFromObject(payload, "challenge", "argument_doc_key")); doc != "" {
		parts = append(parts, "Argument doc: "+doc+".")
	}
	if doc := firstNonEmpty(payloadStringField(payload, "defense_doc_key"), payloadStringFieldFromObject(payload, "challenge", "defense_doc_key")); doc != "" {
		parts = append(parts, "Defense doc: "+doc+".")
	}
	if successor := firstNonEmpty(payloadStringField(payload, "nominated_successor_agent_id"), payloadStringFieldFromObject(payload, "challenge", "nominated_successor_agent_id")); successor != "" {
		parts = append(parts, "Reassign candidate: "+successor+".")
	}
	if deadline := firstNonEmpty(payloadStringField(payload, "voting_deadline_at"), payloadStringFieldFromObject(payload, "challenge", "voting_deadline_at")); deadline != "" {
		parts = append(parts, "Voting deadline: "+deadline+".")
	}
	return strings.Join(parts, " ")
}

func governancePayloadChallengeID(payload map[string]any) string {
	return firstNonEmpty(payloadStringField(payload, "challenge_id"), payloadStringFieldFromObject(payload, "challenge", "challenge_id"))
}

func appendGovernanceVoteOpenRuntimeAdvisory(signals []string, signal, challengeID string) ([]string, bool) {
	signal = strings.TrimSpace(signal)
	challengeID = strings.TrimSpace(challengeID)
	if signal == "" {
		return signals, false
	}
	for _, existing := range signals {
		existing = strings.TrimSpace(existing)
		if existing == signal {
			return signals, false
		}
		if challengeID != "" && strings.Contains(existing, "GOVERNANCE VOTE OPEN: challenge "+challengeID) {
			return signals, false
		}
	}
	return appendCappedAdvisorySignal(signals, signal), true
}

func payloadIntField(payload map[string]any, field string) int {
	if payload == nil {
		return 0
	}
	switch value := payload[field].(type) {
	case float64:
		return int(value)
	case int:
		return value
	case json.Number:
		parsed, _ := value.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func payloadIntFieldFromObject(payload map[string]any, objectField, field string) int {
	object, ok := payload[objectField].(map[string]any)
	if !ok {
		return 0
	}
	return payloadIntField(object, field)
}

func payloadStringField(payload map[string]any, fields ...string) string {
	for _, field := range fields {
		if value, ok := payload[field].(string); ok {
			if value = strings.TrimSpace(value); value != "" {
				return value
			}
		}
	}
	return ""
}

func payloadStringFieldFromObject(payload map[string]any, objectField string, fields ...string) string {
	object, ok := payload[objectField].(map[string]any)
	if !ok {
		return ""
	}
	return payloadStringField(object, fields...)
}

func (r *Runtime) projectCoordinationWakeTarget(refs projectCoordinationEventRefs) (string, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	taskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	sessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	projectID := ""
	if r.activeTask != nil {
		taskID = firstNonEmpty(strings.TrimSpace(r.activeTask.TaskID), taskID)
		projectID = strings.TrimSpace(r.activeTask.ProjectID)
	}
	if r.activeSession != nil {
		sessionID = firstNonEmpty(strings.TrimSpace(r.activeSession.SessionID), sessionID)
		taskID = firstNonEmpty(strings.TrimSpace(r.activeSession.TaskID), taskID)
	}
	if projectID == "" && taskID != "" {
		if task, ok := findBootstrapTask(r.bootstrap.Snapshot.Tasks, taskID); ok {
			projectID = strings.TrimSpace(task.ProjectID)
		}
	}
	if taskID == "" && sessionID == "" {
		if blockedTaskID, ok := parkedBlockedProjectClaimWakeTarget(r.bootstrap.Snapshot.Tasks, refs, r.cfg.AgentID); ok {
			r.clearHydrationLocked()
			r.activeWorkPacket = nil
			r.clearContinuationHoldLocked()
			r.invalidateFocusLocked()
			return blockedTaskID, "", true
		}
		return "", "", false
	}

	refTaskID := strings.TrimSpace(refs.TaskID)
	if refTaskID != "" && taskID != "" && refTaskID != taskID {
		refTaskProjectID := ""
		if task, ok := findBootstrapTask(r.bootstrap.Snapshot.Tasks, refTaskID); ok {
			refTaskProjectID = strings.TrimSpace(task.ProjectID)
		}
		if projectID == "" || refTaskProjectID == "" || refTaskProjectID != projectID {
			return "", "", false
		}
	}
	refSessionID := strings.TrimSpace(refs.SessionID)
	if refSessionID != "" && sessionID != "" && refSessionID != sessionID {
		return "", "", false
	}
	refProjectID := strings.TrimSpace(refs.ProjectID)
	if refProjectID != "" {
		if projectID == "" || refProjectID != projectID {
			return "", "", false
		}
	}

	r.clearHydrationLocked()
	r.activeWorkPacket = nil
	r.clearContinuationHoldLocked()
	r.invalidateFocusLocked()
	return taskID, sessionID, true
}

func parkedBlockedProjectClaimWakeTarget(tasks []WorkspaceTaskRecord, refs projectCoordinationEventRefs, agentID string) (string, bool) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return "", false
	}
	refTaskID := strings.TrimSpace(refs.TaskID)
	refProjectID := strings.TrimSpace(refs.ProjectID)
	if refProjectID == "" && refTaskID != "" {
		if task, ok := findBootstrapTask(tasks, refTaskID); ok {
			refProjectID = strings.TrimSpace(task.ProjectID)
		}
	}

	for _, task := range tasks {
		if !parkedBlockedProjectClaimMatchesAgent(task, agentID) {
			continue
		}
		taskID := strings.TrimSpace(task.TaskID)
		if refTaskID != "" && refTaskID == taskID {
			return taskID, true
		}
		taskProjectID := strings.TrimSpace(task.ProjectID)
		if refProjectID != "" && taskProjectID != "" && refProjectID == taskProjectID {
			return taskID, true
		}
	}
	return "", false
}

func parkedBlockedProjectClaimMatchesAgent(task WorkspaceTaskRecord, agentID string) bool {
	if strings.TrimSpace(task.ProjectID) == "" || !isClaimOwnedBy(task, agentID) || taskClaimStatus(task) != "BLOCKED" {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(task.Status)) {
	case "RESOLVED", "FAILED", "CANCELLED", "CANCELED":
		return false
	default:
		return true
	}
}
