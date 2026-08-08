package main

import (
	"strings"
	"time"
)

func (r *Runtime) continuationHoldDuration() time.Duration {
	delay := r.cfg.PlannerEvery * 3
	if delay < 2*time.Minute {
		delay = 2 * time.Minute
	}
	if delay > 10*time.Minute {
		delay = 10 * time.Minute
	}
	return delay
}

func (r *Runtime) continuationHoldActiveLocked(now time.Time) bool {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	holdUntil, ok := parseRFC3339Nano(strings.TrimSpace(r.scratch.ContinuationHoldUntil))
	if !ok || holdUntil == nil || !holdUntil.After(now) {
		return false
	}

	holdTaskID := strings.TrimSpace(r.scratch.ContinuationHoldTaskID)
	holdSessionID := strings.TrimSpace(r.scratch.ContinuationHoldSessionID)
	activeTaskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	activeSessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	if r.activeTask != nil {
		activeTaskID = firstNonEmpty(strings.TrimSpace(r.activeTask.TaskID), activeTaskID)
	}
	if r.activeSession != nil {
		activeSessionID = firstNonEmpty(strings.TrimSpace(r.activeSession.SessionID), activeSessionID)
	}

	if holdTaskID != "" {
		if activeTaskID == "" || holdTaskID != activeTaskID {
			return false
		}
	}
	if holdSessionID != "" {
		if activeSessionID == "" || holdSessionID != activeSessionID {
			return false
		}
	}
	return holdTaskID != "" || holdSessionID != ""
}

func (r *Runtime) setContinuationHoldLocked(task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, now time.Time) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	taskID := strings.TrimSpace(task.TaskID)
	sessionID := strings.TrimSpace(session.SessionID)
	count := 1
	if strings.TrimSpace(r.scratch.ContinuationHoldTaskID) == taskID &&
		strings.TrimSpace(r.scratch.ContinuationHoldSessionID) == sessionID &&
		r.scratch.ContinuationHoldCount > 0 {
		count = r.scratch.ContinuationHoldCount + 1
	}

	r.scratch.ContinuationHoldTaskID = taskID
	r.scratch.ContinuationHoldSessionID = sessionID
	r.scratch.ContinuationHoldRunID = strings.TrimSpace(runID)
	r.scratch.ContinuationHoldUntil = now.Add(r.continuationHoldDuration()).UTC().Format(time.RFC3339Nano)
	r.scratch.ContinuationHoldSummary = firstNonEmpty(result.NextAction, result.Summary)
	r.scratch.ContinuationHoldCount = count
}

func clearContinuationHold(state *RuntimeScratchState) {
	if state == nil {
		return
	}
	state.ContinuationHoldTaskID = ""
	state.ContinuationHoldSessionID = ""
	state.ContinuationHoldRunID = ""
	state.ContinuationHoldUntil = ""
	state.ContinuationHoldSummary = ""
	state.ContinuationHoldCount = 0
}

func (r *Runtime) clearContinuationHoldLocked() {
	clearContinuationHold(&r.scratch)
}

func (r *Runtime) maybeQueueImmediateProjectContinuationResumeLocked(task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, trace *TaskRunTrace, now time.Time) bool {
	signature, ok := r.immediateProjectContinuationResumeSignature(task, session, runID, result, trace)
	if !ok {
		return false
	}

	taskID := strings.TrimSpace(task.TaskID)
	sessionID := strings.TrimSpace(session.SessionID)
	runID = strings.TrimSpace(runID)
	pendingTrigger := normalizeWorkTrigger(r.scratch.PendingTrigger)
	if pendingTrigger != "" && pendingTrigger != "request_resume" {
		return false
	}
	if pendingTask := strings.TrimSpace(r.scratch.PendingTriggerTask); pendingTask != "" && pendingTask != taskID {
		return false
	}
	if pendingSession := strings.TrimSpace(r.scratch.PendingTriggerSession); pendingSession != "" && pendingSession != sessionID {
		return false
	}

	sameIdentity := strings.TrimSpace(r.scratch.ImmediateProjectResumeTaskID) == taskID &&
		strings.TrimSpace(r.scratch.ImmediateProjectResumeSessionID) == sessionID &&
		strings.TrimSpace(r.scratch.ImmediateProjectResumeRunID) == runID
	if sameIdentity && strings.TrimSpace(r.scratch.ImmediateProjectResumeSignature) == signature {
		return false
	}
	count := 1
	if sameIdentity && r.scratch.ImmediateProjectResumeCount > 0 {
		count = r.scratch.ImmediateProjectResumeCount + 1
	}
	if count > 3 {
		return false
	}

	if now.IsZero() {
		now = time.Now().UTC()
	}
	r.clearContinuationHoldLocked()
	r.scratch.PendingTrigger = "request_resume"
	r.scratch.PendingTriggerTask = taskID
	r.scratch.PendingTriggerSession = sessionID
	r.scratch.PendingTriggerAt = now.UTC().Format(time.RFC3339Nano)
	r.scratch.ImmediateProjectResumeTaskID = taskID
	r.scratch.ImmediateProjectResumeSessionID = sessionID
	r.scratch.ImmediateProjectResumeRunID = runID
	r.scratch.ImmediateProjectResumeSignature = signature
	r.scratch.ImmediateProjectResumeAt = now.UTC().Format(time.RFC3339Nano)
	r.scratch.ImmediateProjectResumeCount = count
	return true
}

func (r *Runtime) immediateProjectContinuationResumeSignature(task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, trace *TaskRunTrace) (string, bool) {
	if r == nil || !runtimeTrustFirst(r.cfg) || normalizeOutcome(result.Outcome) != "continue" {
		return "", false
	}
	if result.RequiresHuman || len(result.BlockedOn) > 0 || strings.TrimSpace(task.ProjectID) == "" {
		return "", false
	}
	if strings.TrimSpace(task.TaskID) == "" || strings.TrimSpace(session.SessionID) == "" || strings.TrimSpace(runID) == "" {
		return "", false
	}
	if !projectDeliveryTaskAllowsImmediateContinuationResume(task) {
		return "", false
	}
	selfActionableRecovery := projectImplementationRealityCheckLooksSelfActionable(result) ||
		projectImplementationRecoverableWorkLooksSelfActionable(result) ||
		projectImplementationUncommittedWorkLooksSelfActionable(result) ||
		projectGateDemotedGitPublicationTransition(result)
	if !taskTraceHasSuccessfulProjectProgress(trace) && !selfActionableRecovery {
		return "", false
	}
	nextMove := projectContinuationNextMove(result)
	if !projectContinuationNextMoveLooksSelfActionable(nextMove) && !selfActionableRecovery {
		return "", false
	}
	var successfulTools []string
	if trace != nil {
		successfulTools = uniqueTrimmedCSVStrings(trace.SuccessfulToolCalls)
	}
	realityCheckSignature := ""
	if projectImplementationRealityCheckLooksSelfActionable(result) {
		realityCheckSignature = projectImplementationRealityCheckSignature(result)
	} else if projectImplementationRecoverableWorkLooksSelfActionable(result) {
		realityCheckSignature = projectImplementationRecoverableWorkSignature(result)
	}
	signatureParts := []string{
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(session.SessionID),
		strings.TrimSpace(runID),
		strings.Join(successfulTools, ","),
		strings.ToLower(strings.TrimSpace(nextMove)),
		realityCheckSignature,
	}
	return shortHash(strings.Join(signatureParts, "|")), true
}

func projectImplementationRealityCheckLooksSelfActionable(result StructuredTaskResult) bool {
	if strings.TrimSpace(result.Materialize.DocKey) == "" || strings.TrimSpace(result.Materialize.DocContent) == "" {
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		result.Summary,
		result.Details,
		result.NextAction,
		result.Materialize.DocKey,
		result.Materialize.DocTitle,
		result.Materialize.DocContent,
	}, "\n"))
	return projectImplementationRealityCheckTextLooksSelfActionable(text)
}

func projectImplementationRealityCheckTextLooksSelfActionable(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if !containsAny(text,
		"artifact_reality_check",
		"artifact reality check",
		"candidate provenance",
		"provenance_review_status",
		"provenance review status",
	) {
		return false
	}
	return containsAny(text,
		"not review-ready",
		"not review ready",
		"not_reviewable",
		"not reviewable",
		"smallest repair",
		"repair direction",
		"stock starter",
		"starter content",
		"scaffold only",
		"missing product",
		"missing implementation",
		"not acceptance",
	)
}

func projectImplementationRecoverableWorkLooksSelfActionable(result StructuredTaskResult) bool {
	text := strings.ToLower(strings.Join([]string{
		result.Summary,
		result.Details,
		result.NextAction,
		blockedDetails(result.BlockedOn),
	}, "\n"))
	if !containsAny(text, "recoverable_project_work", "recoverable project work") {
		return false
	}
	return containsAny(text,
		"project_branch_commit",
		"project_branch_review_ready",
		"review-ready",
		"ready_for_review",
		"commit",
		"publish",
		"repair",
		"uncommitted",
		"dirty",
		"unpublished",
	)
}

func projectImplementationUncommittedWorkLooksSelfActionable(result StructuredTaskResult) bool {
	text := strings.ToLower(strings.Join([]string{
		result.Summary,
		result.Details,
		result.NextAction,
		blockedDetails(result.BlockedOn),
	}, "\n"))
	if !containsAny(text, "uncommitted work", "owned git publication work") {
		return false
	}
	return containsAny(text, "project_branch_commit") &&
		containsAny(text, "project_branch_review_ready", "review-ready", "ready_for_review") &&
		containsAny(text, "push=true", "durable commit", "next action")
}

func projectImplementationRealityCheckSignature(result StructuredTaskResult) string {
	text := strings.ToLower(strings.Join([]string{
		result.Materialize.DocKey,
		result.Materialize.DocTitle,
		result.Materialize.DocContent,
		result.NextAction,
	}, "\n"))
	if len(text) > 1000 {
		text = text[:1000]
	}
	return "reality:" + shortHash(text)
}

func projectImplementationRecoverableWorkSignature(result StructuredTaskResult) string {
	text := strings.ToLower(strings.Join([]string{
		result.Summary,
		result.Details,
		result.NextAction,
	}, "\n"))
	if len(text) > 1000 {
		text = text[:1000]
	}
	return "recoverable:" + shortHash(text)
}

func projectGateDemotedGitPublicationTransition(result StructuredTaskResult) bool {
	if !projectSelfActionableGitPublicationTransition(result) {
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		result.Summary,
		result.Details,
		result.NextAction,
	}, "\n"))
	return containsAny(text,
		"completion deferred: project implementation evidence",
		"owned git publication step",
		"self-actionable, not an external blocker",
	)
}

func projectDeliveryTaskAllowsImmediateContinuationResume(task WorkspaceTaskRecord) bool {
	if projectContinuationLaneAllowsImmediateResume(task.ProjectLane) {
		return true
	}
	return taskPointerValue(task.ClaimBranchID) != "" ||
		taskPointerValue(task.ClaimCheckoutID) != "" ||
		taskPointerValue(task.ClaimWriteScopeJSON) != ""
}

func projectContinuationLaneAllowsImmediateResume(projectLane string) bool {
	switch strings.ToLower(strings.TrimSpace(projectLane)) {
	case "implementation", "implement", "coding", "code",
		"frontend", "front-end", "ui",
		"backend", "back-end", "api",
		"fullstack", "full-stack",
		"revision", "revise", "rebuild", "repair",
		"integration":
		return true
	default:
		return false
	}
}

func taskTraceHasSuccessfulProjectProgress(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	for _, toolName := range trace.SuccessfulToolCalls {
		switch strings.ToLower(strings.TrimSpace(toolName)) {
		case "write_file",
			"project_checkout_materialize",
			"project_checkout_register",
			"project_branch_commit",
			"project_branch_review_ready",
			"project_patch_queue_submit",
			"project_patch_queue_materialize",
			"project_patch_queue_integrate":
			return true
		}
	}
	return false
}

func projectContinuationNextMove(result StructuredTaskResult) string {
	parts := []string{strings.TrimSpace(result.NextAction)}
	if result.Reflection != nil {
		parts = append(parts, strings.TrimSpace(result.Reflection.NextUsefulMove))
	}
	return strings.TrimSpace(strings.Join(parts, "\n"))
}

func projectContinuationNextMoveLooksSelfActionable(nextMove string) bool {
	text := strings.ToLower(strings.TrimSpace(nextMove))
	if text == "" {
		return false
	}
	blockerText := strings.ReplaceAll(text, "not an external blocker", "")
	blockerText = strings.ReplaceAll(blockerText, "not external blocker", "")
	if containsAny(blockerText,
		"when useful",
		"wait",
		"cooldown",
		"ask peer",
		"peer review",
		"agent_request",
		"delegate_task",
		"human",
		"operator approval",
		"external blocker",
	) {
		return false
	}
	return containsAny(text,
		"project_checkout_materialize",
		"project_branch_commit",
		"project_branch_review_ready",
		"project_patch_queue_submit",
		"project_patch_queue_integrate",
		"push=true",
		"ready_for_review",
		"review-ready",
		"materialize checkout",
		"run build",
		"npm run build",
		"build",
		"smoke",
		"test",
		"commit",
		"push",
		"publish",
		"fix",
		"repair",
	)
}
