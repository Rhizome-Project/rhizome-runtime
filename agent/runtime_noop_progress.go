package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const noopContinueBlockThreshold = 2
const patchQueueRevisionPublicationBlockThreshold = 4
const requiredTransitionBlockThreshold = 2
const requiredTransitionReceiptYieldMarker = "runtime_required_transition_receipt_yield"

func taskCycleHasDurableProgress(result StructuredTaskResult, trace *TaskRunTrace, repairInfo *structuredOutputRepairInfo) bool {
	switch normalizeOutcome(result.Outcome) {
	case "completed", "blocked", "failed":
		return true
	}
	return taskCycleHasNonOutcomeDurableProgress(result, trace, repairInfo)
}

// taskCycleHasNonOutcomeDurableProgress reports durable progress evidence that is
// independent of the bare outcome label (completed/blocked/failed). It is used to
// distinguish a cycle that actually advanced state (materialized a doc, wrote
// memory, attempted a structured-output repair, recorded a durable tool receipt,
// or carries a typed human-transition blocker) from a cycle whose only "progress"
// is that it reported blocked/failed. See recordTaskCycleProgress (CA-07).
func taskCycleHasNonOutcomeDurableProgress(result StructuredTaskResult, trace *TaskRunTrace, repairInfo *structuredOutputRepairInfo) bool {
	if repairInfo != nil && repairInfo.Attempted {
		return true
	}
	if result.RequiresHuman && taskResultHasTypedTransitionBlocker(result) {
		return true
	}
	if strings.TrimSpace(result.Materialize.DocKey) != "" && strings.TrimSpace(result.Materialize.DocContent) != "" {
		return true
	}
	if strings.TrimSpace(result.MemoryBody) != "" {
		return true
	}
	if traceHasDurableSuccessfulToolCall(trace) {
		return true
	}
	if traceHasDurableTerminalToolReceipt(trace) {
		return true
	}
	return false
}

// taskCycleProgressSignature is the churn-detection key for blocked/failed cycles:
// two cycles with the same outcome, summary, next action, and blocker set are
// treated as the same (no-progress) repetition (CA-07).
func taskCycleProgressSignature(result StructuredTaskResult) string {
	parts := []string{
		normalizeOutcome(result.Outcome),
		strings.ToLower(strings.TrimSpace(result.Summary)),
		strings.ToLower(strings.TrimSpace(result.NextAction)),
		strings.ToLower(strings.TrimSpace(blockedDetails(result.BlockedOn))),
	}
	return shortHash(strings.Join(parts, "|"))
}

func traceHasDurableSuccessfulToolCall(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	for _, toolName := range trace.SuccessfulToolCalls {
		if !successfulToolCallIsDurableProgress(toolName) {
			continue
		}
		return true
	}
	return false
}

func successfulToolCallIsDurableProgress(toolName string) bool {
	switch strings.ToLower(strings.TrimSpace(toolName)) {
	case "":
		return false
	case "agent_request",
		"read_file",
		"list_directory",
		"workspace_doc_get",
		"memory_read",
		"memory_search",
		"memory_coherence_read",
		"memory_promotion_read",
		"coalition_status",
		"reviewer_scarcity",
		"agent_task_hydrate",
		"project_coordination_get",
		"project_checkout_materialize",
		"project_patch_queue_list",
		"tool_bundle_registry",
		"tool_bundle_registry:list",
		"tool_bundle_registry:status",
		"tool_bundle_registry:read_only",
		"tool_bundle_registry:refresh",
		"write_file:ephemeral",
		"shell":
		return false
	case "write_file",
		"workspace_doc_put",
		"memory_write",
		"task_submit",
		"browser_session",
		"tool_bundle_registry:mutated",
		"project_branch_commit",
		"project_branch_review_ready",
		"project_bootstrap",
		"project_patch_queue_lifecycle",
		"project_patch_queue_followup",
		"project_patch_queue_integrate",
		"project_patch_queue_materialize",
		"project_patch_queue_cas_record",
		"project_patch_queue_submit",
		"project_role_assign",
		"side_effect_resolve",
		"project_phase_transition",
		"project_repo_materialize",
		"project_repo_register",
		"project_checkout_register",
		"project_branch_register":
		return true
	default:
		return false
	}
}

func traceHasDurableTerminalToolReceipt(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	for _, receipt := range trace.ToolReceipts {
		if requiredTransitionFailedReceiptIsTerminal(receipt.ToolName, receipt) {
			return true
		}
	}
	return false
}

func (r *Runtime) applyNoopContinueGuard(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result *StructuredTaskResult, trace *TaskRunTrace) error {
	if result == nil {
		return nil
	}
	// Telic Oracle Loop: for the pinned build lane the telic exit gate fully owns the outcome
	// (description cannot close the cycle). Inert for every other lane.
	if r.telicGate(task, result, trace) {
		return nil
	}
	selfActionableNoop := taskCycleIsSelfActionableToolTransitionNoop(task, *result, trace)
	durableNoWorldChange := taskCycleIsDurableNoWorldChangeContinue(*result, trace)
	unpublishedRevision := taskCycleIsPatchQueueRevisionUnpublishedContinue(task, *result, trace)
	if !taskCycleIsAssistantOnlyNoopContinue(*result, trace) && !selfActionableNoop && !durableNoWorldChange && !unpublishedRevision {
		if taskCycleHasDurableProgress(*result, trace, nil) {
			return r.clearNoopContinueCounter(ctx)
		}
		return nil
	}

	taskID := strings.TrimSpace(task.TaskID)
	sessionID := strings.TrimSpace(session.SessionID)
	runID = strings.TrimSpace(runID)
	signature := noopContinueSignature(task, session, runID, *result)
	now := time.Now().UTC()
	count := 1
	blockThreshold := noopContinueBlockThreshold
	if unpublishedRevision {
		blockThreshold = patchQueueRevisionPublicationBlockThreshold
	}
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		sameActiveNoop := strings.TrimSpace(state.NoopContinueTaskID) == taskID &&
			strings.TrimSpace(state.NoopContinueSessionID) == sessionID
		sameSignature := strings.TrimSpace(state.NoopContinueSignature) == "" ||
			strings.TrimSpace(state.NoopContinueSignature) == signature
		if sameActiveNoop && state.NoopContinueCount > 0 {
			if !unpublishedRevision || sameSignature {
				count = state.NoopContinueCount + 1
			}
		}
		state.NoopContinueTaskID = taskID
		state.NoopContinueSessionID = sessionID
		state.NoopContinueRunID = runID
		state.NoopContinueSignature = signature
		state.NoopContinueSummary = firstNonEmpty(result.NextAction, result.Summary)
		state.NoopContinueAt = now.Format(time.RFC3339Nano)
		state.NoopContinueCount = count
	}); err != nil {
		return err
	}
	if count < blockThreshold {
		return nil
	}

	previousSummary := strings.TrimSpace(result.Summary)
	if selfActionableNoop {
		result.Outcome = "blocked"
		result.Summary = fmt.Sprintf("Self-actionable tool transition was not executed after %d status-only cycles", count)
		result.Details = appendAutonomyNote(result.Details, "Runtime self-action guard observed repeated continue outcomes that named project_branch_commit and project_branch_review_ready as the required owner action, but the cycle did not execute the required publication tools. Last assistant summary: "+truncate(previousSummary, 360))
		result.NextAction = "Resume the same owner only to execute project_branch_commit with push=true followed by project_branch_review_ready, or release/take over the lane after cooldown; do not open additional validation-only work for this blocker."
		result.RequiresHuman = false
		result.OwnerAction = ""
		result.HumanReason = ""
		result.DecisionType = ""
		result.BlockedOn = []BlockedRef{{
			Kind:   "blocked_on_self_action_not_executed",
			Detail: "runtime self-action guard: required project_branch_commit -> project_branch_review_ready was narrated but not executed",
		}}
		return nil
	}
	if unpublishedRevision {
		result.Outcome = "blocked"
		result.Summary = fmt.Sprintf("Patch queue revision was not published after %d unpublished repair cycle(s)", count)
		result.Details = appendAutonomyNote(result.Details, "Runtime revision-publication guard observed repeated repair work on a patch queue revision task without terminal project_patch_queue_submit evidence. Last assistant summary: "+truncate(previousSummary, 360))
		result.NextAction = "Resume the owner only to materialize the live repair checkout if needed, finish the bounded fix, then publish through project_branch_commit with push=true, project_branch_review_ready, and project_patch_queue_submit. Treat the blocked source branch/head as read-only evidence."
		result.RequiresHuman = false
		result.OwnerAction = ""
		result.HumanReason = ""
		result.DecisionType = ""
		result.BlockedOn = []BlockedRef{{
			Kind:   "blocked_on_revision_publication_not_executed",
			Detail: "runtime revision-publication guard: patch queue revision work repeated without terminal patch_queue_submit evidence",
		}}
		return nil
	}
	if durableNoWorldChange {
		result.Outcome = "blocked"
		result.Summary = fmt.Sprintf("No world-state progress after %d status/documentation-only cycles", count)
		result.Details = appendAutonomyNote(result.Details, "Runtime no-progress snapshot guard observed repeated continue outcomes whose successful tools only changed status/doc/update memory surfaces, without a project/git/patch/role/task transition receipt. Last assistant summary: "+truncate(previousSummary, 360))
		result.NextAction = "Resume only with a concrete project/git/patch/role/task transition receipt, or park/reassign this lane with a typed blocker."
		result.RequiresHuman = false
		result.OwnerAction = ""
		result.HumanReason = ""
		result.DecisionType = ""
		result.BlockedOn = []BlockedRef{{
			Kind:   "blocked_on_no_world_state_progress",
			Detail: "runtime no-progress snapshot guard: status/doc/update chatter repeated without durable world-state transition",
		}}
		return nil
	}

	result.Outcome = "blocked"
	result.Summary = fmt.Sprintf("No durable progress after %d assistant-only continue cycles; parking task for replan", count)
	result.Details = appendAutonomyNote(result.Details, "Runtime no-progress guard observed repeated continue outcomes without tool calls, materialized docs, memory, project evidence, or a concrete blocker. Last assistant summary: "+truncate(previousSummary, 360))
	result.NextAction = "Re-enter through fresh work selection or create concrete workspace/project evidence before resuming this task."
	result.RequiresHuman = false
	result.OwnerAction = ""
	result.HumanReason = ""
	result.DecisionType = ""
	result.BlockedOn = []BlockedRef{{
		Kind:   "dependency",
		Detail: "runtime no-progress guard: active task needs concrete tool/doc/project evidence or replanning before another session continues",
	}}
	return nil
}

func taskCycleIsDurableNoWorldChangeContinue(result StructuredTaskResult, trace *TaskRunTrace) bool {
	if normalizeOutcome(result.Outcome) != "continue" || trace == nil {
		return false
	}
	successful := trace.SuccessfulToolCalls
	if len(successful) == 0 {
		return false
	}
	sawStatusSurface := false
	for _, toolName := range successful {
		switch strings.ToLower(strings.TrimSpace(toolName)) {
		case "", "read_file", "list_directory", "workspace_doc_get", "agent_task_hydrate", "project_coordination_get", "project_checkout_materialize", "project_patch_queue_list", "shell", "write_file:ephemeral":
			continue
		case "agent_request":
			return false
		case "workspace_doc_put", "agent.update.post", "workspace.update.post", "memory_write":
			sawStatusSurface = true
		default:
			if successfulToolCallIsDurableProgress(toolName) {
				return false
			}
		}
	}
	return sawStatusSurface
}

func taskCycleIsPatchQueueRevisionUnpublishedContinue(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace) bool {
	if normalizeOutcome(result.Outcome) != "continue" || trace == nil {
		return false
	}
	if !taskRequiresPatchQueueRevisionPublication(task) {
		return false
	}
	if traceHasAnySuccessfulToolCall(trace, "project_patch_queue_submit") {
		return false
	}
	return traceHasToolCallNamed(trace, "write_file") ||
		traceHasToolCallNamed(trace, "shell") ||
		traceHasToolCallNamed(trace, "project_checkout_materialize") ||
		traceHasToolCallNamed(trace, "project_checkout_register") ||
		traceHasToolCallNamed(trace, "project_branch_commit") ||
		traceHasToolCallNamed(trace, "project_branch_review_ready")
}

func taskRequiresPatchQueueRevisionPublication(task WorkspaceTaskRecord) bool {
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(task.TaskRequirementsJSON)), &payload); err == nil {
		kind := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "patch_queue_task_kind")))
		required := strings.TrimSpace(stringMapValue(payload, "required_transition"))
		terminalTool := strings.TrimSpace(stringMapValue(payload, "required_terminal_tool"))
		if kind == "revision" {
			return strings.EqualFold(required, "project_patch_queue_revision_commit_review_submit") ||
				strings.EqualFold(terminalTool, "project_patch_queue_submit") ||
				boolMapValue(payload, "live_repair_branch_required")
		}
	}
	if agentTaskHasAnyTag(task, "owner-bound-kind:patch_queue_revision", "patch_queue_revision", "revision") {
		text := strings.ToLower(strings.Join([]string{
			task.TaskID,
			task.Title,
			task.Description,
			task.ProjectLane,
			strings.Join(task.Tags, " "),
		}, "\n"))
		return strings.Contains(text, "project_patch_queue_revision_commit_review_submit") ||
			strings.Contains(text, "live_repair_branch_required") ||
			strings.Contains(text, "required_terminal_tool") && strings.Contains(text, "project_patch_queue_submit")
	}
	return false
}

func traceHasAnySuccessfulToolCall(trace *TaskRunTrace, names ...string) bool {
	if trace == nil {
		return false
	}
	wanted := map[string]struct{}{}
	for _, name := range names {
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" {
			wanted[name] = struct{}{}
		}
	}
	for _, existing := range trace.SuccessfulToolCalls {
		normalized := strings.ToLower(strings.TrimSpace(existing))
		if _, ok := wanted[normalized]; ok {
			return true
		}
		if idx := strings.Index(normalized, ":"); idx > 0 {
			if _, ok := wanted[normalized[:idx]]; ok {
				return true
			}
		}
	}
	return false
}

func (r *Runtime) applyRequiredTransitionExecutionGate(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result *StructuredTaskResult, trace *TaskRunTrace, workPacket *AgentWorkPacket) error {
	if result == nil {
		return nil
	}
	if taskLooksProjectClaimRepairReceiptTask(task) {
		if receipt, ok, err := r.projectClaimRepairDurablePatchQueueReceipt(ctx, task); err != nil {
			return err
		} else if ok {
			completeProjectClaimRepairResult(result, receipt)
			return r.clearRequiredTransitionCounter(ctx)
		}
	}
	if taskRequiresPatchQueueRevisionPublication(task) {
		if err := r.applyPatchQueueRevisionPublicationRequiredTransitionGate(ctx, result, trace); err != nil {
			return err
		}
	}
	toolName := requiredTransitionToolForTaskCycle(task, workPacket)
	taskBoundRequiredTool := false
	if toolName == "" {
		toolName = taskBoundRequiredToolForTaskCycle(task)
		taskBoundRequiredTool = toolName != ""
	}
	if toolName == "" {
		return r.clearRequiredTransitionCounter(ctx)
	}
	if taskBoundRequiredTool && traceHasTaskBoundRequiredToolReceipt(trace, toolName, *result) {
		return r.clearRequiredTransitionCounter(ctx)
	}
	if !taskBoundRequiredTool && (traceHasRequiredTransitionReceipt(trace, toolName) || taskResultHasTypedTransitionBlocker(*result)) {
		return r.clearRequiredTransitionCounter(ctx)
	}

	taskID := strings.TrimSpace(task.TaskID)
	sessionID := strings.TrimSpace(session.SessionID)
	runID = strings.TrimSpace(runID)
	now := time.Now().UTC()
	count := 1
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		sameActiveTransition := strings.TrimSpace(state.RequiredTransitionTaskID) == taskID &&
			strings.TrimSpace(state.RequiredTransitionSessionID) == sessionID &&
			strings.TrimSpace(state.RequiredTransitionTool) == toolName
		if sameActiveTransition && state.RequiredTransitionCount > 0 {
			count = state.RequiredTransitionCount + 1
		}
		state.RequiredTransitionTaskID = taskID
		state.RequiredTransitionSessionID = sessionID
		state.RequiredTransitionRunID = runID
		state.RequiredTransitionTool = toolName
		state.RequiredTransitionSummary = firstNonEmpty(result.NextAction, result.Summary)
		state.RequiredTransitionAt = now.Format(time.RFC3339Nano)
		state.RequiredTransitionCount = count
	}); err != nil {
		return err
	}

	previousSummary := strings.TrimSpace(result.Summary)
	if count < requiredTransitionBlockThreshold {
		result.Outcome = "continue"
		if taskBoundRequiredTool {
			result.Summary = "Required task-bound tool was not executed"
			result.Details = appendAutonomyNote(result.Details, fmt.Sprintf("Runtime required-tool gate observed task_requirements.required_tool=%s, but the cycle did not call %s. Last assistant summary: %s", toolName, toolName, truncate(previousSummary, 360)))
			result.NextAction = requiredTaskBoundToolNextAction(toolName)
		} else {
			result.Summary = "Required durable transition was not executed"
			result.Details = appendAutonomyNote(result.Details, fmt.Sprintf("Runtime required-transition gate observed work packet preferred_transition=%s, but the cycle did not execute %s. Last assistant summary: %s", toolName, toolName, truncate(previousSummary, 360)))
			result.NextAction = requiredTransitionNextAction(toolName)
		}
		result.RequiresHuman = false
		result.OwnerAction = ""
		result.HumanReason = ""
		result.DecisionType = ""
		return nil
	}

	result.Outcome = "blocked"
	if taskBoundRequiredTool {
		result.Summary = fmt.Sprintf("Required task-bound tool %s was not executed after %d cycle(s)", toolName, count)
		result.Details = appendAutonomyNote(result.Details, fmt.Sprintf("Runtime required-tool gate blocked status/doc substitution. task_requirements.required_tool=%s must be called before this task can complete or publish a blocker doc. Last assistant summary: %s", toolName, truncate(previousSummary, 360)))
		result.NextAction = requiredTaskBoundToolNextAction(toolName)
	} else {
		result.Summary = fmt.Sprintf("Required durable transition %s was not executed after %d cycle(s)", toolName, count)
		result.Details = appendAutonomyNote(result.Details, fmt.Sprintf("Runtime required-transition gate blocked status/doc substitution. preferred_transition=%s must produce a durable tool receipt or a typed blocker. Last assistant summary: %s", toolName, truncate(previousSummary, 360)))
		result.NextAction = requiredTransitionNextAction(toolName)
	}
	result.RequiresHuman = false
	result.OwnerAction = ""
	result.HumanReason = ""
	result.DecisionType = ""
	blockerKind := "required_transition_not_executed"
	blockerDetail := "runtime required-transition gate: " + toolName + " was required by the work packet but was not executed"
	if strings.EqualFold(toolName, projectRoleScopeAuthorityTransitionTool) && runtimeTaskLooksProjectRoleAssignAuthorityTransition(task) {
		blockerKind = "authority_transition_terminal_blocker"
		blockerDetail = formatAuthorityTransitionTerminalBlockerReason(task, "project_role_assign did not produce an applied or denied durable receipt")
		result.Summary = blockerDetail
		result.NextAction = "Release this authority-transition carrier and continue from a corrected successor only if the scope transition is still required; do not retry this blocked carrier."
	}
	if taskBoundRequiredTool {
		blockerKind = "required_tool_not_executed_or_failed"
		blockerDetail = "runtime required-tool gate: " + toolName + " was required by task_requirements.required_tool but did not produce a successful receipt or typed blocker"
	}
	result.BlockedOn = []BlockedRef{{
		Kind:   blockerKind,
		Detail: blockerDetail,
	}}
	return nil
}

func (r *Runtime) applyPatchQueueRevisionPublicationRequiredTransitionGate(ctx context.Context, result *StructuredTaskResult, trace *TaskRunTrace) error {
	if result == nil {
		return nil
	}
	if traceHasAnySuccessfulToolCall(trace, "project_patch_queue_submit") {
		return r.clearRequiredTransitionCounter(ctx)
	}
	if taskResultHasTypedTransitionBlocker(*result) && traceHasFailedToolReceiptNamed(trace, "project_patch_queue_submit") {
		return r.clearRequiredTransitionCounter(ctx)
	}
	if normalizeOutcome(result.Outcome) != "completed" {
		return nil
	}
	previousSummary := strings.TrimSpace(result.Summary)
	result.Outcome = "blocked"
	result.Summary = "Patch queue revision completion requires project_patch_queue_submit"
	result.Details = appendAutonomyNote(result.Details, "Runtime revision-publication gate rejected completion without terminal project_patch_queue_submit evidence. Last assistant summary: "+truncate(previousSummary, 360))
	result.NextAction = "Finish the repair publication ladder: project_branch_commit with push=true, project_branch_review_ready, then project_patch_queue_submit with a fresh item_id/supersession evidence. Do not complete the revision task from commit or review-ready evidence alone."
	result.RequiresHuman = false
	result.OwnerAction = ""
	result.HumanReason = ""
	result.DecisionType = ""
	result.BlockedOn = []BlockedRef{{
		Kind:   "required_transition_not_executed",
		Detail: "runtime revision-publication gate: project_patch_queue_submit is required before completing this patch queue revision task",
	}}
	return nil
}

func requiredTransitionToolForTaskCycle(task WorkspaceTaskRecord, workPacket *AgentWorkPacket) string {
	if taskLooksProjectClaimRepairReceiptTask(task) {
		return "project_claim_repair_receipt"
	}
	if workPacket != nil {
		switch strings.TrimSpace(workPacket.PreferredTransition) {
		case "project_role_assign":
			return "project_role_assign"
		case "side_effect_resolve":
			return "side_effect_resolve"
		}
	}
	if runtimeTaskLooksProjectRoleAssignAuthorityTransition(task) {
		return "project_role_assign"
	}
	if taskRequiresSideEffectResolveTransition(task) {
		return "side_effect_resolve"
	}
	return ""
}

func requiredTransitionReceiptPreemptsToolCall(task WorkspaceTaskRecord, workPacket *AgentWorkPacket, trace *TaskRunTrace) (string, bool) {
	toolName := requiredTransitionToolForTaskCycle(task, workPacket)
	if strings.TrimSpace(toolName) == "" {
		return "", false
	}
	if !traceHasRequiredTransitionReceipt(trace, toolName) {
		return "", false
	}
	return toolName, true
}

func taskBoundRequiredToolForTaskCycle(task WorkspaceTaskRecord) string {
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(task.TaskRequirementsJSON)), &payload); err == nil {
		nonIntegrationPatchQueueTask := taskRequirementsDescribePatchQueueNonIntegrationTask(payload)
		if tool := strings.TrimSpace(stringMapValue(payload, "required_tool")); tool != "" {
			if nonIntegrationPatchQueueTask && strings.EqualFold(tool, "project_patch_queue_integrate") {
				return ""
			}
			return tool
		}
		requiredTransition := strings.TrimSpace(stringMapValue(payload, "required_transition"))
		if strings.EqualFold(requiredTransition, "empty_product_frontier_outcome") {
			return "empty_product_frontier_outcome"
		}
		patchQueueKind := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "patch_queue_task_kind")))
		if strings.EqualFold(requiredTransition, "project_patch_queue_integrate_then_full_product_verify") ||
			patchQueueKind == "integration" {
			if nonIntegrationPatchQueueTask {
				return ""
			}
			return "project_patch_queue_integrate"
		}
		if nonIntegrationPatchQueueTask {
			return ""
		}
	}
	if strings.EqualFold(strings.TrimSpace(task.ProjectLane), "integration") && runtimeProjectTaskLooksPatchQueueIntegrationConvergence(task) {
		return "project_patch_queue_integrate"
	}
	return ""
}

func taskRequirementsDescribePatchQueueNonIntegrationTask(payload map[string]any) bool {
	if len(payload) == 0 {
		return false
	}
	kind := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "patch_queue_task_kind")))
	return kind != "" && kind != "integration"
}

func traceHasToolCallNamed(trace *TaskRunTrace, toolName string) bool {
	if trace == nil || strings.TrimSpace(toolName) == "" {
		return false
	}
	for _, existing := range trace.ToolCalls {
		if strings.EqualFold(strings.TrimSpace(existing), strings.TrimSpace(toolName)) {
			return true
		}
	}
	return false
}

func traceHasTaskBoundRequiredToolReceipt(trace *TaskRunTrace, toolName string, result StructuredTaskResult) bool {
	if traceHasRequiredTransitionReceipt(trace, toolName) {
		return true
	}
	if !taskResultHasTypedTransitionBlocker(result) {
		return false
	}
	return traceHasFailedToolReceiptNamed(trace, toolName)
}

// traceHasProjectPatchQueueLifecycleDecisionReceipt reports whether the trace contains a
// successful project_patch_queue_lifecycle DECISION (accept/block/reject) - distinguished from a
// mere claim by the decision marker the tool emits. RPF-58A: this is what satisfies a review
// task's required_tool gate, so a reviewer that claims-then-wanders (R58) cannot complete the
// review without recording a durable decision.
func traceHasProjectPatchQueueLifecycleDecisionReceipt(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	for _, receipt := range trace.ToolReceipts {
		if receipt.IsError {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), "project_patch_queue_lifecycle") {
			continue
		}
		if strings.Contains(receipt.Output, projectPatchQueueLifecycleDecisionRecordedMarker) {
			return true
		}
	}
	return false
}

func traceHasFailedToolReceiptNamed(trace *TaskRunTrace, toolName string) bool {
	if trace == nil || strings.TrimSpace(toolName) == "" {
		return false
	}
	for _, receipt := range trace.ToolReceipts {
		if receipt.IsError && strings.EqualFold(strings.TrimSpace(receipt.ToolName), strings.TrimSpace(toolName)) {
			return true
		}
	}
	return false
}

func requiredTransitionReceiptYieldToolResult(toolName, requiredTool string) ToolResult {
	toolName = firstNonEmpty(strings.TrimSpace(toolName), "unknown")
	requiredTool = firstNonEmpty(strings.TrimSpace(requiredTool), "required_transition")
	return ToolResult{
		Output:  fmt.Sprintf("%s: blocked tool %s because required transition receipt %s is already recorded in this task cycle. Stop calling tools and return a terminal structured result now; runtime will convert the receipt into the durable task outcome.", requiredTransitionReceiptYieldMarker, toolName, requiredTool),
		IsError: true,
	}
}

func taskBoundRequiredToolPreemptsSubstituteToolCall(task WorkspaceTaskRecord, trace *TaskRunTrace, toolName string) (string, string, bool) {
	requiredTool := taskBoundRequiredToolForTaskCycle(task)
	if requiredTool == "" || strings.TrimSpace(toolName) == "" {
		return "", "", false
	}
	toolName = strings.TrimSpace(toolName)
	if strings.EqualFold(requiredTool, "project_patch_queue_integrate") {
		return taskBoundProjectPatchQueueIntegratePreemptsSubstituteToolCall(trace, toolName)
	}
	if strings.EqualFold(requiredTool, "browser_session") {
		hasRequiredAttempt := traceHasToolCallNamed(trace, requiredTool)
		hasSuccessfulRequiredReceipt := traceHasSuccessfulRequiredTransition(trace, requiredTool)
		switch strings.ToLower(toolName) {
		case "browser_visual_probe":
			if taskRequirementsHasForbiddenSubstitute(task, "browser_visual_probe_only") ||
				taskRequirementsHasForbiddenSubstitute(task, "browser_visual_probe") {
				if !hasSuccessfulRequiredReceipt {
					return requiredTool, "browser_visual_probe is probe-only evidence and cannot satisfy this task before a successful browser_session receipt", true
				}
			}
		case "workspace_doc_put":
			if !hasRequiredAttempt {
				return requiredTool, "workspace_doc_put cannot publish a blocker, status, or completion before the required browser_session attempt", true
			}
		}
	}
	return "", "", false
}

func taskBoundProjectPatchQueueIntegratePreemptsSubstituteToolCall(trace *TaskRunTrace, toolName string) (string, string, bool) {
	const requiredTool = "project_patch_queue_integrate"
	normalizedTool := strings.ToLower(strings.TrimSpace(toolName))
	if normalizedTool == "" || normalizedTool == requiredTool {
		return "", "", false
	}
	hasAttempt := traceHasToolCallNamed(trace, requiredTool)
	hasSuccessfulReceipt := traceHasSuccessfulRequiredTransition(trace, requiredTool)
	hasEffectiveIntegrationReceipt := traceHasProjectPatchQueueEffectiveIntegrationReceipt(trace)
	if hasSuccessfulReceipt || hasEffectiveIntegrationReceipt {
		return "", "", false
	}
	hasTerminalReceipt := traceHasRequiredTransitionReceipt(trace, requiredTool)
	switch normalizedTool {
	case "project_patch_queue_followup":
		if hasTerminalReceipt {
			return "", "", false
		}
		if hasAttempt {
			return requiredTool, fmt.Sprintf("%s already failed but has not produced a terminal integration/refusal/repair receipt yet; do not create speculative follow-ups before the integration tool emits the durable next gate", requiredTool), true
		}
		return requiredTool, fmt.Sprintf("%s must be attempted before patch queue repair follow-up creation on a patch queue integration task", requiredTool), true
	case "shell", "write_file", "workspace_doc_put", "task_submit",
		"project_checkout_materialize", "project_checkout_register",
		"project_branch_register", "project_branch_commit", "project_branch_review_ready",
		"project_patch_queue_materialize", "project_patch_queue_cas_record",
		"project_bootstrap", "project_repo_materialize", "project_repo_register",
		"project_phase_transition",
		// BP-01 (static-audit 2026-06-02): these terminalize/re-route the accepted item or
		// mint authority and could substitute for a real integration receipt; block them too
		// before the integration tool emits a terminal receipt (read-only inspection tools
		// remain allowed via the default case).
		"project_patch_queue_lifecycle", "project_patch_queue_submit",
		"project_role_assign", "project_role_request":
		if hasTerminalReceipt {
			return requiredTool, fmt.Sprintf("%s produced a terminal integration/refusal/repair receipt; do not substitute local shell/doc/task progress. Use the emitted project_patch_queue_followup path or return the typed blocker", requiredTool), true
		}
		if hasAttempt {
			return requiredTool, fmt.Sprintf("%s already failed or did not produce an integration receipt; do not substitute shell/doc/task progress. Return a typed blocker or use the repair/follow-up path emitted by %s", requiredTool, requiredTool), true
		}
		return requiredTool, fmt.Sprintf("%s must be attempted before shell/doc/task progress on a patch queue integration task; local merge, checkout materialization, status docs, or new tasks are not durable integration receipts", requiredTool), true
	default:
		return "", "", false
	}
}

func taskRequirementsHasForbiddenSubstitute(task WorkspaceTaskRecord, value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return false
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(task.TaskRequirementsJSON)), &payload); err != nil {
		return false
	}
	for _, item := range stringSliceFromAny(payload["forbidden_substitutes"]) {
		if strings.ToLower(strings.TrimSpace(item)) == value {
			return true
		}
	}
	return false
}

func taskBoundRequiredToolSubstituteYieldToolResult(toolName, requiredTool, reason string) ToolResult {
	toolName = firstNonEmpty(strings.TrimSpace(toolName), "unknown")
	requiredTool = firstNonEmpty(strings.TrimSpace(requiredTool), "required_tool")
	reason = firstNonEmpty(strings.TrimSpace(reason), "substitute tool cannot satisfy the current task-bound required tool")
	return ToolResult{
		Output:  fmt.Sprintf("required_tool_substitute_blocked: blocked tool %s because task_requirements.required_tool=%s has not produced the required receipt for this substitute path. %s. Use %s now, or return a typed blocker only after calling %s.", toolName, requiredTool, reason, requiredTool, requiredTool),
		IsError: true,
	}
}

func taskRequiresSideEffectResolveTransition(task WorkspaceTaskRecord) bool {
	if agentTaskHasAnyTag(task, "side-effect-classification", "side_effect_classification") {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.TaskRequirementsJSON,
	}, "\n"))
	return strings.Contains(text, "artifact_bound_side_effect_classification_task.v1") ||
		strings.Contains(text, `"abpc_task_class":"side_effect_classification"`) ||
		strings.Contains(text, `"abpc_task_class": "side_effect_classification"`) ||
		strings.Contains(text, `"abpc_task_class":"side_effect_verification"`) ||
		strings.Contains(text, `"abpc_task_class": "side_effect_verification"`) ||
		(strings.Contains(text, `"classification_decision":"`) &&
			strings.Contains(text, "side_effect_refs") &&
			strings.Contains(text, "source_task_id")) ||
		(strings.Contains(text, "artifact_bound_side_effect_resolution_followup.v1") &&
			(strings.Contains(text, "abpc_recovery_action") ||
				strings.Contains(text, `"action_kind":"verify_bucket"`) ||
				strings.Contains(text, `"action_kind": "verify_bucket"`)))
}

func agentTaskHasAnyTag(task WorkspaceTaskRecord, tags ...string) bool {
	for _, existing := range task.Tags {
		existing = strings.ToLower(strings.TrimSpace(existing))
		for _, tag := range tags {
			if existing == strings.ToLower(strings.TrimSpace(tag)) {
				return true
			}
		}
	}
	return false
}

func traceHasSuccessfulRequiredTransition(trace *TaskRunTrace, toolName string) bool {
	if trace == nil || strings.TrimSpace(toolName) == "" {
		return false
	}
	for _, successful := range trace.SuccessfulToolCalls {
		if strings.TrimSpace(successful) == strings.TrimSpace(toolName) {
			return true
		}
	}
	return false
}

func traceHasRequiredTransitionReceipt(trace *TaskRunTrace, toolName string) bool {
	if strings.EqualFold(strings.TrimSpace(toolName), "empty_product_frontier_outcome") {
		return traceHasEmptyProductFrontierOutcomeReceipt(trace)
	}
	if strings.EqualFold(strings.TrimSpace(toolName), "project_claim_repair_receipt") {
		_, ok := terminalProjectClaimRepairReceipt(trace)
		return ok
	}
	if strings.EqualFold(strings.TrimSpace(toolName), "project_patch_queue_lifecycle") {
		// RPF-58A: the lifecycle tool is multi-action; a successful CLAIM also lands in
		// SuccessfulToolCalls. A review task's required_tool gate must only be satisfied by a
		// durable DECISION (accept/block/reject), never by the claim that precedes it. Match the
		// decision marker the tool emits on a recorded decision.
		return traceHasProjectPatchQueueLifecycleDecisionReceipt(trace)
	}
	if traceHasSuccessfulRequiredTransition(trace, toolName) {
		return true
	}
	if trace == nil || strings.TrimSpace(toolName) == "" {
		return false
	}
	for _, receipt := range trace.ToolReceipts {
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), strings.TrimSpace(toolName)) {
			continue
		}
		if requiredTransitionFailedReceiptIsTerminal(toolName, receipt) {
			return true
		}
	}
	return false
}

func requiredTransitionFailedReceiptIsTerminal(toolName string, receipt TaskRunToolReceipt) bool {
	switch strings.TrimSpace(toolName) {
	case "project_role_assign":
		return projectRoleAssignDeniedReceiptIsTerminal(receipt)
	case "project_patch_queue_integrate":
		return projectPatchQueueIntegrateFailedReceiptIsTerminal(receipt)
	default:
		return false
	}
}

func traceHasEmptyProductFrontierOutcomeReceipt(trace *TaskRunTrace) bool {
	return traceHasAnySuccessfulToolCall(trace, "task_submit", "project_phase_transition")
}

func projectPatchQueueIntegrateFailedReceiptIsTerminal(receipt TaskRunToolReceipt) bool {
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(receipt.Output)), &payload); err == nil {
		if projectPatchQueueIntegrateReceiptHasEffectiveIntegration(payload) {
			return true
		}
		if !receipt.IsError {
			return false
		}
		if strings.EqualFold(strings.TrimSpace(stringMapValue(payload, "pre_integration_gate")), "defeasible_acceptance") &&
			boolMapValue(payload, "no_canonical_mutation") {
			return true
		}
		if boolMapValue(payload, "repair_receipt_recorded") {
			return true
		}
		if boolMapValue(payload, "repair_receipt_already_recorded") ||
			boolMapValue(payload, "repair_recording_dedup_key_already_exists") {
			return true
		}
		if strings.TrimSpace(stringMapValue(payload, "required_transition")) == "project_patch_queue_followup_revision_before_integration_retry" {
			return true
		}
	}
	if !receipt.IsError {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(receipt.Output))
	if strings.Contains(text, `"already_integrated":true`) ||
		strings.Contains(text, `"already_integrated": true`) ||
		strings.Contains(text, "already_integrated=true") {
		return true
	}
	if strings.Contains(text, "pre_integration_gate") &&
		strings.Contains(text, "defeasible_acceptance") &&
		strings.Contains(text, "no_canonical_mutation") {
		return true
	}
	return strings.Contains(text, "project_patch_queue_integrate failed and could not record durable repair receipt") &&
		strings.Contains(text, "project.patch_queue.integration_repair") &&
		strings.Contains(text, "dedup_key") &&
		strings.Contains(text, "already exists")
}

func traceHasProjectPatchQueueEffectiveIntegrationReceipt(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	for _, receipt := range trace.ToolReceipts {
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), "project_patch_queue_integrate") {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(receipt.Output)), &payload); err == nil {
			if projectPatchQueueIntegrateReceiptHasEffectiveIntegration(payload) {
				return true
			}
		}
	}
	return false
}

func projectPatchQueueIntegrateReceiptHasEffectiveIntegration(payload map[string]any) bool {
	if len(payload) == 0 || boolMapValue(payload, "no_canonical_mutation") {
		return false
	}
	if boolMapValue(payload, "already_integrated") ||
		boolMapValue(payload, "integrated") ||
		boolMapValue(payload, "integration_recorded") {
		return true
	}
	return strings.TrimSpace(stringMapValue(payload, "target_head_after")) != "" ||
		strings.TrimSpace(stringMapValue(payload, "remote_target_head_after")) != "" ||
		strings.TrimSpace(stringMapValue(payload, "canonical_target_head")) != ""
}

func projectPatchQueueIntegrateReceiptIsBlockingRepairOrRefusal(receipt TaskRunToolReceipt) bool {
	if !receipt.IsError {
		return false
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(receipt.Output)), &payload); err == nil {
		if projectPatchQueueIntegrateReceiptHasEffectiveIntegration(payload) {
			return false
		}
		if strings.EqualFold(strings.TrimSpace(stringMapValue(payload, "pre_integration_gate")), "defeasible_acceptance") &&
			boolMapValue(payload, "no_canonical_mutation") {
			return true
		}
		if boolMapValue(payload, "repair_receipt_recorded") {
			return true
		}
		if boolMapValue(payload, "repair_receipt_already_recorded") ||
			boolMapValue(payload, "repair_recording_dedup_key_already_exists") {
			return true
		}
		if strings.TrimSpace(stringMapValue(payload, "required_transition")) == "project_patch_queue_followup_revision_before_integration_retry" {
			return true
		}
		return false
	}
	text := strings.ToLower(strings.TrimSpace(receipt.Output))
	if strings.Contains(text, `"already_integrated":true`) ||
		strings.Contains(text, `"already_integrated": true`) ||
		strings.Contains(text, "already_integrated=true") {
		return false
	}
	if strings.Contains(text, "pre_integration_gate") &&
		strings.Contains(text, "defeasible_acceptance") &&
		strings.Contains(text, "no_canonical_mutation") {
		return true
	}
	return strings.Contains(text, "project_patch_queue_integrate failed and could not record durable repair receipt") &&
		strings.Contains(text, "project.patch_queue.integration_repair") &&
		strings.Contains(text, "dedup_key") &&
		strings.Contains(text, "already exists")
}

func projectRoleAssignDeniedReceiptIsTerminal(receipt TaskRunToolReceipt) bool {
	if !receipt.IsError {
		return false
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(receipt.Output)), &payload); err != nil {
		return false
	}
	if !boolMapValue(payload, "denial_recorded") {
		return false
	}
	state := strings.TrimSpace(stringMapValue(payload, "boundary_transition_state"))
	switch state {
	case "boundary_expansion_denied_overlap", "role_scope_conflict", "ready_for_review_branch_rebind_blocked":
		return boolMapValue(payload, "transition_denied")
	default:
		return false
	}
}

func completeRecordedAuthorityTransitionDenial(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace, workPacket *AgentWorkPacket) StructuredTaskResult {
	if normalizeOutcome(result.Outcome) == "completed" {
		return result
	}
	if requiredTransitionToolForTaskCycle(task, workPacket) != "project_role_assign" {
		return result
	}
	if !taskCycleLooksProjectRoleScopeAuthorityTransition(task, workPacket) {
		return result
	}
	payload, ok := terminalProjectRoleAssignDenialPayload(trace)
	if !ok {
		return result
	}

	state := firstNonEmpty(stringMapValue(payload, "boundary_transition_state"), "boundary_transition_denied")
	reason := firstNonEmpty(stringMapValue(payload, "denial_reason"), "project_role_assign returned a recorded denial")
	preferred := firstNonEmpty(stringMapValue(payload, "preferred_transition"), "wait_or_split_existing_owner_lane")
	allowedNext := stringListMapValue(payload, "allowed_next")
	note := fmt.Sprintf("Authority transition completed with terminal denial: %s (%s).", state, reason)
	if len(allowedNext) > 0 {
		note += " allowed_next=" + strings.Join(allowedNext, ", ")
	}

	completed := result
	completed.Outcome = "completed"
	completed.RequiresHuman = false
	completed.OwnerAction = ""
	completed.HumanReason = ""
	completed.DecisionType = ""
	completed.BlockedOn = nil
	completed.Summary = firstNonEmpty(strings.TrimSpace(completed.Summary), "Authority transition completed with recorded denial")
	completed.Details = appendAutonomyNote(completed.Details, note)
	completed.NextAction = firstNonEmpty(strings.TrimSpace(completed.NextAction), preferred)
	return completed
}

func completeRecordedAuthorityTransitionApplied(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace, workPacket *AgentWorkPacket) StructuredTaskResult {
	if normalizeOutcome(result.Outcome) == "completed" {
		return result
	}
	if requiredTransitionToolForTaskCycle(task, workPacket) != "project_role_assign" {
		return result
	}
	if !taskCycleLooksProjectRoleScopeAuthorityTransition(task, workPacket) {
		return result
	}
	payload, ok := terminalProjectRoleAssignAppliedPayload(task, trace)
	if !ok {
		return result
	}

	state := firstNonEmpty(stringMapValue(payload, "boundary_transition_state"), "role_scope_assigned")
	roleType := firstNonEmpty(stringMapValue(payload, "role_type"), stringMapValue(payload, "requested_role_type"), "project_role")
	targetAgentID := firstNonEmpty(stringMapValue(payload, "agent_id"), stringMapValue(payload, "target_agent_id"), "target_agent")
	note := fmt.Sprintf("Authority transition completed with applied role/scope receipt: %s (%s for %s).", state, roleType, targetAgentID)
	if roleID := strings.TrimSpace(stringMapValue(payload, "role_id")); roleID != "" {
		note += " role_id=" + roleID + "."
	}

	completed := result
	completed.Outcome = "completed"
	completed.RequiresHuman = false
	completed.OwnerAction = ""
	completed.HumanReason = ""
	completed.DecisionType = ""
	completed.BlockedOn = nil
	completed.Summary = firstNonEmpty(strings.TrimSpace(completed.Summary), "Authority transition completed with applied role/scope receipt")
	completed.Details = appendAutonomyNote(completed.Details, note)
	completed.NextAction = firstNonEmpty(strings.TrimSpace(completed.NextAction), "Release this authority-transition carrier; the affected product lane can retry with the applied role/scope evidence.")
	return completed
}

func completeProjectClaimRepairReceipt(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace) StructuredTaskResult {
	if normalizeOutcome(result.Outcome) == "completed" {
		return result
	}
	if !taskLooksProjectClaimRepairReceiptTask(task) {
		return result
	}
	receipt, ok := terminalProjectClaimRepairReceipt(trace)
	if !ok {
		return result
	}

	completed := result
	completeProjectClaimRepairResult(&completed, receipt)
	return completed
}

func completeProjectClaimRepairResult(result *StructuredTaskResult, receipt string) {
	if result == nil {
		return
	}
	result.Outcome = "completed"
	result.RequiresHuman = false
	result.OwnerAction = ""
	result.HumanReason = ""
	result.DecisionType = ""
	result.BlockedOn = nil
	result.Summary = firstNonEmpty(strings.TrimSpace(result.Summary), "Project claim repair authority transition completed")
	result.Details = appendAutonomyNote(result.Details, receipt)
	result.NextAction = firstNonEmpty(strings.TrimSpace(result.NextAction), "Release the project-claim repair lane and let the blocked implementation owner resume from the durable role/claim/branch state.")
}

func completeProjectPatchQueueIntegrationTerminalReceipt(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace) StructuredTaskResult {
	if taskBoundRequiredToolForTaskCycle(task) != "project_patch_queue_integrate" {
		return result
	}
	receipt, ok := terminalProjectPatchQueueIntegrationReceipt(trace)
	if !ok {
		return result
	}

	blocked := result
	blocked.Outcome = "blocked"
	blocked.RequiresHuman = false
	blocked.OwnerAction = ""
	blocked.HumanReason = ""
	blocked.DecisionType = ""
	blocked.Summary = firstNonEmpty(strings.TrimSpace(blocked.Summary), "Patch queue integration parked on terminal repair/refusal receipt")
	blocked.Details = appendAutonomyNote(blocked.Details, receipt)
	blocked.NextAction = "Wait for the emitted patch-queue repair/revision follow-up; do not retry integration of the same accepted head until a fresh queue item is accepted."
	blocked.BlockedOn = normalizedBlockedRefs(blocked.BlockedOn, receipt)
	return blocked
}

func terminalProjectPatchQueueIntegrationReceipt(trace *TaskRunTrace) (string, bool) {
	if trace == nil {
		return "", false
	}
	for _, receipt := range trace.ToolReceipts {
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), "project_patch_queue_integrate") {
			continue
		}
		if !projectPatchQueueIntegrateReceiptIsBlockingRepairOrRefusal(receipt) {
			continue
		}
		output := strings.TrimSpace(receipt.Output)
		var payload map[string]any
		if err := json.Unmarshal([]byte(output), &payload); err == nil {
			parts := []string{"project_patch_queue_integrate produced a terminal integration repair/refusal receipt"}
			if queueID := strings.TrimSpace(stringMapValue(payload, "queue_id")); queueID != "" {
				parts = append(parts, "queue_id="+queueID)
			}
			if itemID := strings.TrimSpace(stringMapValue(payload, "item_id")); itemID != "" {
				parts = append(parts, "item_id="+itemID)
			}
			if branchID := strings.TrimSpace(stringMapValue(payload, "branch_id")); branchID != "" {
				parts = append(parts, "branch_id="+branchID)
			}
			if gate := strings.TrimSpace(stringMapValue(payload, "pre_integration_gate")); gate != "" {
				parts = append(parts, "gate="+gate)
			}
			if transition := strings.TrimSpace(stringMapValue(payload, "required_transition")); transition != "" {
				parts = append(parts, "required_transition="+transition)
			}
			if boolMapValue(payload, "repair_receipt_recorded") || boolMapValue(payload, "repair_receipt_already_recorded") {
				parts = append(parts, "repair_receipt_recorded=true")
			}
			if defeater := strings.TrimSpace(stringMapValue(payload, "defeating_item_id")); defeater != "" {
				parts = append(parts, "defeating_item_id="+defeater)
			}
			return strings.Join(parts, "; ") + ".", true
		}
		return "project_patch_queue_integrate produced a terminal integration repair/refusal receipt: " + truncate(output, 360), true
	}
	return "", false
}

func completeSideEffectResolutionSuccessorReceipt(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace) StructuredTaskResult {
	if normalizeOutcome(result.Outcome) == "completed" {
		return result
	}
	record, ok := sideEffectResolutionFollowupRecordFromTask(task)
	if !ok || !strings.EqualFold(strings.TrimSpace(record.AdmissionKind), "abpc_recovery_action") {
		return result
	}
	payload, ok := sideEffectResolveSuccessorDecisionPayload(trace, task.TaskID, record)
	if !ok {
		return result
	}

	decision := firstNonEmpty(strings.TrimSpace(stringMapValue(payload, "decision")), strings.TrimSpace(record.Decision), "side_effect_resolve")
	integrationStatus := strings.TrimSpace(stringMapValue(payload, "integration_status"))
	followupTaskID := firstNonEmpty(strings.TrimSpace(stringMapValue(payload, "followup_task_id")), strings.TrimSpace(stringMapValue(payload, "existing_task_id")))
	next := strings.TrimSpace(stringMapValue(payload, "next_transition"))
	note := "Side-effect recovery successor recorded terminal side_effect_resolve receipt"
	if decision != "" {
		note += " decision=" + decision
	}
	if integrationStatus != "" {
		note += " integration_status=" + integrationStatus
	}
	if followupTaskID != "" && followupTaskID != strings.TrimSpace(task.TaskID) {
		note += " followup=" + followupTaskID
	}
	note += "."

	completed := result
	completed.Outcome = "completed"
	completed.RequiresHuman = false
	completed.OwnerAction = ""
	completed.HumanReason = ""
	completed.DecisionType = ""
	completed.BlockedOn = nil
	completed.Summary = firstNonEmpty(strings.TrimSpace(completed.Summary), "Side-effect recovery successor recorded durable decision")
	completed.Details = appendAutonomyNote(completed.Details, note)
	if followupTaskID != "" && followupTaskID != strings.TrimSpace(task.TaskID) {
		completed.NextAction = "Release this ABPC recovery successor and route to successor " + followupTaskID + "."
	} else if next != "" {
		completed.NextAction = "Release this ABPC recovery successor; durable decision recorded, next transition: " + next + "."
	} else {
		completed.NextAction = "Release this ABPC recovery successor; durable side-effect decision is recorded."
	}
	return completed
}

func sideEffectResolveSuccessorDecisionPayload(trace *TaskRunTrace, taskID string, record sideEffectResolutionFollowupRecord) (map[string]any, bool) {
	if trace == nil {
		return nil, false
	}
	taskID = strings.TrimSpace(taskID)
	successorKey := strings.TrimSpace(record.SuccessorKey)
	for _, receipt := range trace.ToolReceipts {
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), "side_effect_resolve") || receipt.IsError {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(receipt.Output)), &payload); err != nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(stringMapValue(payload, "status")), "decision_recorded") {
			continue
		}
		if payloadMatchesSideEffectSuccessor(payload, taskID, successorKey) {
			return payload, true
		}
	}
	return nil, false
}

func payloadMatchesSideEffectSuccessor(payload map[string]any, taskID, successorKey string) bool {
	for _, key := range []string{"followup_task_id", "existing_task_id"} {
		if taskID != "" && strings.TrimSpace(stringMapValue(payload, key)) == taskID {
			return true
		}
	}
	for _, key := range []string{"successor_key", "followup_successor_key"} {
		if successorKey != "" && strings.TrimSpace(stringMapValue(payload, key)) == successorKey {
			return true
		}
	}
	return false
}

func completeSideEffectClassifierSuccessorHandoff(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace) StructuredTaskResult {
	if normalizeOutcome(result.Outcome) == "completed" {
		return result
	}
	if !taskLooksSideEffectClassificationParent(task) {
		return result
	}
	payload, ok := sideEffectResolveSuccessorHandoffPayload(trace)
	if !ok {
		return result
	}

	followupTaskID := firstNonEmpty(
		stringMapValue(payload, "followup_task_id"),
		stringMapValue(payload, "existing_task_id"),
	)
	actionKind := firstNonEmpty(
		stringMapValue(payload, "followup_action_kind"),
		stringMapValue(payload, "next_transition"),
		stringMapValue(payload, "decision"),
		"side_effect_resolution_successor",
	)
	note := "Side-effect classifier handed off to recovery successor"
	if followupTaskID != "" {
		note += " " + followupTaskID
	}
	note += " via " + actionKind + "."

	completed := result
	completed.Outcome = "completed"
	completed.RequiresHuman = false
	completed.OwnerAction = ""
	completed.HumanReason = ""
	completed.DecisionType = ""
	completed.BlockedOn = nil
	completed.Summary = firstNonEmpty(strings.TrimSpace(completed.Summary), "Side-effect classifier handed off to recovery successor")
	completed.Details = appendAutonomyNote(completed.Details, note)
	if followupTaskID != "" {
		completed.NextAction = "Release the parent classifier and route to ABPC recovery successor " + followupTaskID + "; the successor must produce the next side_effect_resolve verdict."
	} else {
		completed.NextAction = "Release the parent classifier and route to the ABPC recovery successor; the successor must produce the next side_effect_resolve verdict."
	}
	return completed
}

func completeSideEffectClassifierTerminalDecision(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace) StructuredTaskResult {
	if normalizeOutcome(result.Outcome) == "completed" {
		return result
	}
	if !taskLooksSideEffectClassificationParent(task) {
		return result
	}
	payload, ok := sideEffectResolveTerminalDecisionPayload(trace, task.TaskID)
	if !ok {
		return result
	}

	state := strings.TrimSpace(stringMapValue(payload, "boundary_transition_state"))
	decision := strings.TrimSpace(stringMapValue(payload, "decision"))
	preferred := strings.TrimSpace(stringMapValue(payload, "preferred_transition"))
	allowedNext := stringListMapValue(payload, "allowed_next")
	refs := stringListMapValue(payload, "side_effect_refs")

	note := "Side-effect classifier completed with terminal side_effect_resolve receipt"
	if decision != "" {
		note += " decision=" + decision
	}
	if state != "" {
		note += " state=" + state
	}
	if len(refs) > 0 {
		note += " refs=" + strings.Join(refs, ", ")
	}
	if len(allowedNext) > 0 {
		note += " allowed_next=" + strings.Join(allowedNext, ", ")
	}
	note += "."

	completed := result
	completed.Outcome = "completed"
	completed.RequiresHuman = false
	completed.OwnerAction = ""
	completed.HumanReason = ""
	completed.DecisionType = ""
	completed.BlockedOn = nil
	completed.Summary = firstNonEmpty(strings.TrimSpace(completed.Summary), "Side-effect classifier recorded terminal decision")
	completed.Details = appendAutonomyNote(completed.Details, note)
	completed.NextAction = firstNonEmpty(strings.TrimSpace(completed.NextAction), sideEffectResolveTerminalDecisionNextAction(payload, preferred))
	return completed
}

func sideEffectResolveSuccessorHandoffPayload(trace *TaskRunTrace) (map[string]any, bool) {
	if trace == nil {
		return nil, false
	}
	for _, receipt := range trace.ToolReceipts {
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), "side_effect_resolve") || receipt.IsError {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(receipt.Output)), &payload); err != nil {
			continue
		}
		parentState := strings.TrimSpace(stringMapValue(payload, "parent_classification_state"))
		if !strings.EqualFold(parentState, "waiting_on_successors") &&
			!strings.EqualFold(parentState, "resolved_waiting_on_successors") {
			continue
		}
		if strings.TrimSpace(stringMapValue(payload, "followup_task_id")) == "" &&
			strings.TrimSpace(stringMapValue(payload, "existing_task_id")) == "" &&
			strings.TrimSpace(stringMapValue(payload, "successor_key")) == "" {
			continue
		}
		return payload, true
	}
	return nil, false
}

func sideEffectResolveTerminalDecisionPayload(trace *TaskRunTrace, taskID string) (map[string]any, bool) {
	if trace == nil {
		return nil, false
	}
	taskID = strings.TrimSpace(taskID)
	for _, receipt := range trace.ToolReceipts {
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), "side_effect_resolve") || receipt.IsError {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(receipt.Output)), &payload); err != nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(stringMapValue(payload, "status")), "decision_recorded") {
			continue
		}
		if classifierID := strings.TrimSpace(stringMapValue(payload, "classification_task_id")); classifierID != "" && taskID != "" && classifierID != taskID {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(stringMapValue(payload, "parent_classification_state")), "waiting_on_successors") {
			continue
		}
		if !sideEffectResolvePayloadIsTerminalDecision(payload) {
			continue
		}
		return payload, true
	}
	return nil, false
}

func sideEffectResolvePayloadIsTerminalDecision(payload map[string]any) bool {
	if len(payload) == 0 {
		return false
	}
	state := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "boundary_transition_state")))
	switch state {
	case "authority_transition_applied", "role_scope_assigned", "role_scope_already_satisfied":
		return boolMapValue(payload, "transition_executed") ||
			boolMapValue(payload, "authority_transition_applied") ||
			boolMapValue(payload, "already_satisfied")
	case "boundary_expansion_denied_overlap", "ready_for_review_branch_rebind_blocked":
		return boolMapValue(payload, "transition_denied") || boolMapValue(payload, "do_not_retry")
	case "authority_transition_already_pending", "lead_scope_request_pending", "branch_claim_rebind_missing":
		return false
	}
	decision := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "decision")))
	switch decision {
	case "quarantine", "revert":
		return true
	case "reroute_to_active_lane":
		return boolMapValue(payload, "transition_executed")
	default:
		return false
	}
}

func sideEffectResolveTerminalDecisionNextAction(payload map[string]any, preferred string) string {
	state := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "boundary_transition_state")))
	switch state {
	case "boundary_expansion_denied_overlap":
		return firstNonEmpty(strings.TrimSpace(preferred), "Release the side-effect classifier and route the source lane to wait/adopt/narrow/split using the recorded overlap denial; do not retry the same boundary expansion.")
	case "ready_for_review_branch_rebind_blocked":
		return firstNonEmpty(strings.TrimSpace(preferred), "Release the side-effect classifier and route through patch-queue follow-up, revision, or release of the READY_FOR_REVIEW branch; do not mutate that branch boundary in place.")
	case "authority_transition_applied", "role_scope_assigned", "role_scope_already_satisfied":
		return "Release the side-effect classifier; the affected owner lane should retry materialization/commit using the applied branch and claim boundary."
	}
	if strings.EqualFold(strings.TrimSpace(stringMapValue(payload, "decision")), "reroute_to_active_lane") {
		return "Release the side-effect classifier and route to the already active lane recorded by side_effect_resolve."
	}
	switch strings.ToLower(strings.TrimSpace(stringMapValue(payload, "decision"))) {
	case "quarantine", "revert":
		return "Release the side-effect classifier; the recorded side-effect resolution decision is terminal for this lane."
	}
	return "Release the side-effect classifier and continue from the terminal side-effect decision receipt."
}

func terminalProjectClaimRepairReceipt(trace *TaskRunTrace) (string, bool) {
	if note, ok := terminalProjectClaimRepairPatchQueueFollowupReceipt(trace); ok {
		return note, true
	}
	if note, ok := terminalProjectClaimRepairTaskSubmitReceipt(trace); ok {
		return note, true
	}
	return "", false
}

type projectClaimRepairPatchQueueConflict struct {
	ProjectID string
	RepoID    string
	QueueID   string
	ItemID    string
	BranchID  string
	HeadSHA   string
}

func (r *Runtime) projectClaimRepairDurablePatchQueueReceipt(ctx context.Context, task WorkspaceTaskRecord) (string, bool, error) {
	if r == nil || r.client == nil || !taskLooksProjectClaimRepairReceiptTask(task) {
		return "", false, nil
	}
	conflict := projectClaimRepairPatchQueueConflictFromTask(task)
	projectID := firstNonEmpty(strings.TrimSpace(task.ProjectID), strings.TrimSpace(conflict.ProjectID))
	if projectID == "" || !conflict.Actionable() {
		return "", false, nil
	}
	items, err := r.client.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListInput{
		WorkspaceID: r.cfg.WorkspaceID,
		ProjectID:   projectID,
		RepoID:      strings.TrimSpace(conflict.RepoID),
		BranchID:    strings.TrimSpace(conflict.BranchID),
	})
	if err != nil {
		return "", false, nil
	}
	for _, item := range items {
		if !projectClaimRepairPatchQueueConflictMatchesItem(conflict, item) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(item.State), "INTEGRATED") {
			continue
		}
		parts := []string{"Project claim repair conflict is already resolved by durable patch queue integration"}
		if item.QueueID != "" || item.ItemID != "" {
			parts = append(parts, "item="+strings.Trim(strings.TrimSpace(item.QueueID)+"/"+strings.TrimSpace(item.ItemID), "/"))
		}
		if item.BranchID != "" {
			parts = append(parts, "branch_id="+strings.TrimSpace(item.BranchID))
		}
		if item.HeadSHA != "" {
			parts = append(parts, "head_sha="+strings.TrimSpace(item.HeadSHA))
		}
		return strings.Join(parts, "; ") + ".", true, nil
	}
	return "", false, nil
}

func (c projectClaimRepairPatchQueueConflict) Actionable() bool {
	return strings.TrimSpace(c.ItemID) != "" ||
		strings.TrimSpace(c.BranchID) != "" ||
		strings.TrimSpace(c.HeadSHA) != ""
}

func projectClaimRepairPatchQueueConflictFromTask(task WorkspaceTaskRecord) projectClaimRepairPatchQueueConflict {
	text := strings.Join([]string{
		task.Description,
		task.TaskRequirementsJSON,
		strings.Join(task.Tags, "\n"),
	}, "\n")
	return projectClaimRepairPatchQueueConflict{
		ProjectID: projectClaimRepairTextField(text, "project_id"),
		RepoID:    projectClaimRepairTextField(text, "repo_id"),
		QueueID:   projectClaimRepairTextField(text, "conflict_patch_queue_id", "patch_queue_id", "queue_id"),
		ItemID:    projectClaimRepairTextField(text, "conflict_patch_item_id", "patch_item_id", "item_id"),
		BranchID:  projectClaimRepairTextField(text, "conflict_branch_id", "branch_id"),
		HeadSHA:   projectClaimRepairTextField(text, "conflict_branch_head_sha", "head_sha"),
	}
}

func projectClaimRepairTextField(text string, keys ...string) string {
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value := stringMapValue(jsonObjectFromString(text), key); strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "-"))
			if line == "" {
				continue
			}
			name, value, ok := strings.Cut(line, ":")
			if !ok || !strings.EqualFold(strings.TrimSpace(name), key) {
				continue
			}
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func jsonObjectFromString(raw string) map[string]any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}
	return payload
}

func projectClaimRepairPatchQueueConflictMatchesItem(conflict projectClaimRepairPatchQueueConflict, item ProjectPatchQueueItemRecord) bool {
	if itemID := strings.TrimSpace(conflict.ItemID); itemID != "" && strings.TrimSpace(item.ItemID) != itemID {
		return false
	}
	if queueID := strings.TrimSpace(conflict.QueueID); queueID != "" && strings.TrimSpace(item.QueueID) != queueID {
		return false
	}
	if branchID := strings.TrimSpace(conflict.BranchID); branchID != "" && strings.TrimSpace(item.BranchID) != branchID {
		return false
	}
	if headSHA := strings.TrimSpace(conflict.HeadSHA); headSHA != "" && strings.TrimSpace(item.HeadSHA) != headSHA {
		return false
	}
	return strings.TrimSpace(conflict.ItemID) != "" ||
		strings.TrimSpace(conflict.QueueID) != "" ||
		strings.TrimSpace(conflict.BranchID) != "" ||
		strings.TrimSpace(conflict.HeadSHA) != ""
}

func terminalProjectClaimRepairRoleAssignReceipt(trace *TaskRunTrace) (string, bool) {
	if payload, ok := terminalProjectRoleAssignDenialPayload(trace); ok {
		state := firstNonEmpty(stringMapValue(payload, "boundary_transition_state"), "boundary_transition_denied")
		reason := firstNonEmpty(stringMapValue(payload, "denial_reason"), "project_role_assign returned a recorded denial")
		return fmt.Sprintf("Project claim repair reached terminal project_role_assign denial: %s (%s).", state, reason), true
	}
	return "", false
}

func terminalProjectClaimRepairPatchQueueFollowupReceipt(trace *TaskRunTrace) (string, bool) {
	if trace == nil {
		return "", false
	}
	for _, receipt := range trace.ToolReceipts {
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), "project_patch_queue_followup") || receipt.IsError {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(receipt.Output)), &payload); err != nil {
			continue
		}
		if boolMapValue(payload, "terminal_existing") ||
			strings.EqualFold(strings.TrimSpace(stringMapValue(payload, "reason")), "existing_followup_task_terminal") {
			continue
		}
		taskID := firstNonEmpty(
			stringMapValue(payload, "existing_task_id"),
			stringMapValue(payload, "followup_task_id"),
			stringMapValue(payload, "task_id"),
		)
		if taskID == "" {
			continue
		}
		kind := firstNonEmpty(stringMapValue(payload, "followup_kind"), "patch_queue_followup")
		state := strings.ToUpper(strings.TrimSpace(stringMapValue(payload, "state")))
		branchID := strings.TrimSpace(stringMapValue(payload, "branch_id"))
		note := "Project claim repair handed off to executable patch queue " + kind + " follow-up " + taskID
		if state != "" {
			note += " for " + state + " queue item"
		}
		if branchID != "" {
			note += " on branch " + branchID
		}
		note += "."
		return note, true
	}
	return "", false
}

func terminalProjectClaimRepairTaskSubmitReceipt(trace *TaskRunTrace) (string, bool) {
	if trace == nil {
		return "", false
	}
	for _, receipt := range trace.ToolReceipts {
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), "task_submit") || receipt.IsError {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(receipt.Output)), &payload); err != nil {
			continue
		}
		taskID := firstNonEmpty(
			stringMapValue(payload, "existing_task_id"),
			stringMapValue(payload, "task_id"),
		)
		if taskID == "" {
			continue
		}
		status := strings.ToUpper(strings.TrimSpace(firstNonEmpty(
			stringMapValue(payload, "existing_status"),
			stringMapValue(payload, "status"),
		)))
		if projectClaimRepairSubmittedTaskStatusTerminal(status) {
			continue
		}
		if !projectClaimRepairTaskSubmitLooksExecutableFollowThrough(payload, taskID) {
			continue
		}
		branchID := strings.TrimSpace(stringMapValue(payload, "branch_id"))
		note := "Project claim repair handed off to executable submitted task " + taskID
		if status != "" {
			note += " (" + status + ")"
		}
		if branchID != "" {
			note += " on branch " + branchID
		}
		note += "."
		return note, true
	}
	return "", false
}

func projectClaimRepairTaskSubmitLooksExecutableFollowThrough(payload map[string]any, taskID string) bool {
	if len(payload) == 0 {
		return false
	}
	if projectClaimRepairTaskSubmitBlockedCoordinationSplit(payload) {
		return false
	}
	if projectClaimRepairTaskSubmitLooksPatchQueueFollowThrough(payload, taskID) {
		return true
	}

	taskKind := firstNonEmpty(
		stringMapValue(payload, "task_kind"),
		stringMapValue(payload, "existing_task_kind"),
	)
	projectLane := firstNonEmpty(
		stringMapValue(payload, "project_lane"),
		stringMapValue(payload, "existing_project_lane"),
	)
	record := WorkspaceTaskRecord{
		TaskID:      strings.TrimSpace(taskID),
		TaskKind:    strings.TrimSpace(taskKind),
		ProjectLane: strings.TrimSpace(projectLane),
	}
	if taskRecordIsProductLane(record) {
		if strings.EqualFold(strings.TrimSpace(taskKind), "COORDINATION") && !projectClaimRepairTaskSubmitLooksPatchQueueFollowThrough(payload, taskID) {
			return false
		}
		return true
	}
	requiresGate := boolMapValue(payload, "requires_project_gate") || boolMapValue(payload, "existing_requires_project_gate")
	if requiresGate && projectClaimRepairTaskSubmitLaneCanCarryProductWork(projectLane) {
		return true
	}
	return false
}

func projectClaimRepairTaskSubmitBlockedCoordinationSplit(payload map[string]any) bool {
	gate := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "task_submit_gate")))
	blockedKind := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "blocked_creation_kind")))
	return gate == "product_lane_liveness" || blockedKind == "coordination_split"
}

func projectClaimRepairTaskSubmitLooksPatchQueueFollowThrough(payload map[string]any, taskID string) bool {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(taskID)), "task-patchq-") {
		return true
	}
	gate := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "task_submit_gate")))
	switch gate {
	case "patch_queue_identity_duplicate":
		return true
	}
	if strings.TrimSpace(stringMapValue(payload, "queue_id")) != "" && strings.TrimSpace(stringMapValue(payload, "item_id")) != "" {
		return true
	}
	for _, key := range []string{"task_requirements", "existing_task_requirements"} {
		requirements, ok := payload[key].(map[string]any)
		if !ok {
			continue
		}
		if strings.TrimSpace(stringMapValue(requirements, "patch_queue_task_kind")) != "" {
			return true
		}
		if strings.TrimSpace(stringMapValue(requirements, "queue_id")) != "" && strings.TrimSpace(stringMapValue(requirements, "item_id")) != "" {
			return true
		}
	}
	return false
}

func projectClaimRepairTaskSubmitLaneCanCarryProductWork(projectLane string) bool {
	lane := strings.ToLower(strings.TrimSpace(projectLane))
	switch lane {
	case "", "coordination", "strategy", "strategic", "planning", "plan", "spec", "specification", "requirements", "design", "framing", "handoff", "report", "final", "synthesis", "summary", "summarization":
		return false
	default:
		return true
	}
}

func projectClaimRepairSubmittedTaskStatusTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "COMPLETED", "RESOLVED", "CANCELED", "CANCELLED", "FAILED", "REJECTED", "SUPERSEDED":
		return true
	default:
		return false
	}
}

func taskLooksProjectClaimRepairReceiptTask(task WorkspaceTaskRecord) bool {
	if strings.HasPrefix(strings.TrimSpace(task.TaskID), "task-project-claim-repair-") {
		return true
	}
	return agentTaskHasAnyTag(task, "project-claim-repair")
}

func optionalReceiptSuffix(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return " (" + value + ")"
}

func taskLooksSideEffectClassificationParent(task WorkspaceTaskRecord) bool {
	if agentTaskHasAnyTag(task, "side-effect-classification", "side_effect_classification") {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.TaskRequirementsJSON,
	}, "\n"))
	return strings.Contains(text, "artifact_bound_side_effect_classification_task.v1") ||
		strings.Contains(text, `"abpc_task_class":"side_effect_classification"`) ||
		strings.Contains(text, `"abpc_task_class": "side_effect_classification"`)
}

func taskCycleLooksProjectRoleScopeAuthorityTransition(task WorkspaceTaskRecord, workPacket *AgentWorkPacket) bool {
	if requiredTransitionToolForTaskCycle(task, workPacket) != projectRoleScopeAuthorityTransitionTool {
		return false
	}
	if runtimeTaskLooksStrategicLeadRoleScopeRequest(task) {
		return true
	}
	if strings.HasPrefix(strings.TrimSpace(task.TaskID), "task-role-scope-") {
		if workPacket != nil && strings.EqualFold(strings.TrimSpace(workPacket.PreferredTransition), projectRoleScopeAuthorityTransitionTool) {
			return true
		}
		return runtimeTaskLooksDedicatedProjectRoleScopeAuthorityCarrier(task) ||
			runtimeTaskHasProjectRoleScopeAuthorityTransition(task)
	}
	return runtimeTaskLooksProjectRoleAssignAuthorityTransition(task)
}

func terminalProjectRoleAssignDenialPayload(trace *TaskRunTrace) (map[string]any, bool) {
	if trace == nil {
		return nil, false
	}
	for _, receipt := range trace.ToolReceipts {
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), "project_role_assign") {
			continue
		}
		if !projectRoleAssignDeniedReceiptIsTerminal(receipt) {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(receipt.Output)), &payload); err != nil {
			continue
		}
		return payload, true
	}
	return nil, false
}

func terminalProjectRoleAssignAppliedPayload(task WorkspaceTaskRecord, trace *TaskRunTrace) (map[string]any, bool) {
	if trace == nil {
		return nil, false
	}
	for _, receipt := range trace.ToolReceipts {
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), "project_role_assign") || receipt.IsError {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(receipt.Output)), &payload); err != nil {
			continue
		}
		if !projectRoleAssignAppliedPayloadLooksTerminal(payload) {
			continue
		}
		if !projectRoleAssignAppliedPayloadMatchesTask(task, payload) {
			continue
		}
		return payload, true
	}
	return nil, false
}

func projectRoleAssignAppliedPayloadLooksTerminal(payload map[string]any) bool {
	if len(payload) == 0 {
		return false
	}
	state := strings.TrimSpace(stringMapValue(payload, "boundary_transition_state"))
	if stringInSet(state, "authority_transition_applied", "role_scope_assigned", "role_scope_already_satisfied") {
		return true
	}
	if boolMapValue(payload, "authority_transition_applied") || boolMapValue(payload, "already_satisfied") {
		return true
	}
	if strings.TrimSpace(stringMapValue(payload, "role_id")) != "" &&
		strings.TrimSpace(stringMapValue(payload, "agent_id")) != "" &&
		strings.TrimSpace(stringMapValue(payload, "role_type")) != "" {
		status := strings.ToUpper(strings.TrimSpace(stringMapValue(payload, "status")))
		return status == "" || status == "ACTIVE"
	}
	return false
}

func projectRoleAssignAppliedPayloadMatchesTask(task WorkspaceTaskRecord, payload map[string]any) bool {
	if len(payload) == 0 {
		return false
	}
	var requirements map[string]any
	_ = json.Unmarshal([]byte(strings.TrimSpace(task.TaskRequirementsJSON)), &requirements)
	if projectID := strings.TrimSpace(stringMapValue(requirements, "project_id")); projectID != "" {
		if got := strings.TrimSpace(stringMapValue(payload, "project_id")); got != "" && got != projectID {
			return false
		}
	}
	if targetAgentID := strings.TrimSpace(stringMapValue(requirements, "target_agent_id")); targetAgentID != "" {
		got := firstNonEmpty(stringMapValue(payload, "agent_id"), stringMapValue(payload, "target_agent_id"))
		if strings.TrimSpace(got) != "" && strings.TrimSpace(got) != targetAgentID {
			return false
		}
	}
	if roleType := strings.ToUpper(strings.TrimSpace(stringMapValue(requirements, "role_type"))); roleType != "" {
		got := strings.ToUpper(strings.TrimSpace(firstNonEmpty(stringMapValue(payload, "role_type"), stringMapValue(payload, "requested_role_type"))))
		if got != "" && got != roleType {
			return false
		}
	}
	return true
}

func stringListMapValue(values map[string]any, key string) []string {
	if len(values) == 0 {
		return nil
	}
	raw, ok := values[key]
	if !ok {
		return nil
	}
	var out []string
	switch typed := raw.(type) {
	case []string:
		for _, value := range typed {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	case []any:
		for _, value := range typed {
			if trimmed := strings.TrimSpace(fmt.Sprint(value)); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	case string:
		for _, value := range strings.Split(typed, ",") {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out
}

func boolMapValue(values map[string]any, key string) bool {
	value, ok := values[key]
	if !ok {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func taskResultHasTypedTransitionBlocker(result StructuredTaskResult) bool {
	if normalizeOutcome(result.Outcome) != "blocked" && !result.RequiresHuman {
		return false
	}
	for _, blocker := range result.BlockedOn {
		rawKind := strings.ToLower(strings.TrimSpace(blocker.Kind))
		kind := normalizeBlockedKind(blocker.Kind, blocker.Detail)
		if rawKind != "" && rawKind != "unknown" && kind != "" && kind != "unknown" {
			return true
		}
	}
	return result.RequiresHuman && (strings.TrimSpace(result.OwnerAction) != "" || strings.TrimSpace(result.DecisionType) != "")
}

func requiredTransitionNextAction(toolName string) string {
	switch strings.TrimSpace(toolName) {
	case "project_role_assign":
		return "Run project_role_assign for the current authority-transition task, or return a typed blocker/denial explaining why the boundary mutation cannot be applied. Do not answer with status, model.ask approval, or publication evidence."
	case "side_effect_resolve":
		return "Run side_effect_resolve with a durable decision for the pending side-effect refs, or return a typed blocker explaining why classification cannot proceed. Do not retry commit/materialization or publish branch evidence before the side effect is resolved."
	case "project_claim_repair_receipt":
		return "Produce a durable project-claim repair receipt: use project_role_assign only for necessary role/scope repair, then materialize executable follow-through with project_patch_queue_followup or product/patch task_submit, or return a typed blocker/denial. Role assignment alone is not a repair-complete receipt. Do not answer with status, model.ask, workspace docs, or delegated validation chatter only."
	default:
		return "Execute the required durable transition named by the work packet, or return a typed blocker with the exact missing authority/evidence."
	}
}

func requiredTaskBoundToolNextAction(toolName string) string {
	toolName = strings.TrimSpace(toolName)
	switch toolName {
	case "browser_session":
		return "Run browser_session for the task-bound browser audit and record its receipts before publishing status, blocker, or completion docs. A successful browser_session receipt or a failed browser_session receipt with a typed blocker is acceptable runtime evidence; skipping browser_session is not."
	case "project_patch_queue_integrate":
		return "Run project_patch_queue_integrate for the task-bound patch queue integration boundary before shell merges, workspace docs, task_submit, or completion. A successful integration receipt, or the tool's typed repair/defeasible-acceptance blocker, is the durable transition."
	case "empty_product_frontier_outcome":
		return "Resolve the empty project frontier with a durable receipt: either create exactly one bounded follow-up via task_submit, or call project_phase_transition with to_phase=DONE and convergence evidence. A recommendation/status doc alone is not terminal."
	default:
		return "Run the task-bound required tool " + firstNonEmpty(toolName, "required_tool") + " and record its receipt before publishing status, blocker, or completion docs."
	}
}

func (r *Runtime) clearRequiredTransitionCounter(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	hasCounter := r.scratch.RequiredTransitionTaskID != "" ||
		r.scratch.RequiredTransitionSessionID != "" ||
		r.scratch.RequiredTransitionRunID != "" ||
		r.scratch.RequiredTransitionTool != "" ||
		r.scratch.RequiredTransitionCount != 0
	r.mu.Unlock()
	if !hasCounter {
		return nil
	}
	return r.updateScratch(ctx, clearRequiredTransition)
}

func clearRequiredTransition(state *RuntimeScratchState) {
	if state == nil {
		return
	}
	state.RequiredTransitionTaskID = ""
	state.RequiredTransitionSessionID = ""
	state.RequiredTransitionRunID = ""
	state.RequiredTransitionTool = ""
	state.RequiredTransitionSummary = ""
	state.RequiredTransitionAt = ""
	state.RequiredTransitionCount = 0
}

func taskCycleIsAssistantOnlyNoopContinue(result StructuredTaskResult, trace *TaskRunTrace) bool {
	if normalizeOutcome(result.Outcome) != "continue" {
		return false
	}
	return !taskCycleHasDurableProgress(result, trace, nil)
}

func taskCycleIsSelfActionableToolTransitionNoop(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace) bool {
	if normalizeOutcome(result.Outcome) != "continue" {
		return false
	}
	if !runtimeProjectCompletionEvidenceGateRequired(task) {
		return false
	}
	if !projectSelfActionableGitPublicationTransition(result) {
		return false
	}
	return !traceHasSelfActionableGitPublicationProgress(trace)
}

func projectSelfActionableGitPublicationTransition(result StructuredTaskResult) bool {
	text := strings.ToLower(strings.Join([]string{
		result.Summary,
		result.Details,
		result.NextAction,
		result.Materialize.DocKey,
		result.Materialize.DocTitle,
		result.Materialize.DocContent,
		blockedDetails(result.BlockedOn),
	}, "\n"))
	if result.Reflection != nil {
		text += "\n" + strings.ToLower(strings.Join([]string{
			result.Reflection.CurrentIntent,
			result.Reflection.FreshEvidence,
			result.Reflection.BlockerFreshness,
			result.Reflection.NextUsefulMove,
		}, "\n"))
	}
	if !containsAny(text, "project_branch_commit") || !containsAny(text, "project_branch_review_ready", "ready_for_review", "review-ready") {
		return false
	}
	return containsAny(text,
		"self-actionable",
		"not an external blocker",
		"owned git publication",
		"call project_branch_commit",
		"run project_branch_commit",
		"commit them with project_branch_commit",
	)
}

func traceHasSelfActionableGitPublicationProgress(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	for _, toolName := range trace.SuccessfulToolCalls {
		switch strings.ToLower(strings.TrimSpace(toolName)) {
		case "project_branch_commit",
			"project_branch_review_ready",
			"write_file",
			"side_effect_resolve",
			"project_role_assign":
			return true
		}
	}
	return false
}

func (r *Runtime) clearNoopContinueCounter(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	hasCounter := r.scratch.NoopContinueTaskID != "" ||
		r.scratch.NoopContinueSessionID != "" ||
		r.scratch.NoopContinueRunID != "" ||
		r.scratch.NoopContinueCount != 0
	r.mu.Unlock()
	if !hasCounter {
		return nil
	}
	return r.updateScratch(ctx, clearNoopContinue)
}

func clearNoopContinue(state *RuntimeScratchState) {
	if state == nil {
		return
	}
	state.NoopContinueTaskID = ""
	state.NoopContinueSessionID = ""
	state.NoopContinueRunID = ""
	state.NoopContinueSignature = ""
	state.NoopContinueSummary = ""
	state.NoopContinueAt = ""
	state.NoopContinueCount = 0
}

func noopContinueSignature(task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult) string {
	parts := []string{
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(session.SessionID),
		strings.TrimSpace(runID),
		strings.ToLower(strings.TrimSpace(result.Summary)),
		strings.ToLower(strings.TrimSpace(result.NextAction)),
	}
	if result.Reflection != nil {
		parts = append(parts,
			strings.ToLower(strings.TrimSpace(result.Reflection.CurrentIntent)),
			strings.ToLower(strings.TrimSpace(result.Reflection.NextUsefulMove)),
		)
	}
	return shortHash(strings.Join(parts, "|"))
}
