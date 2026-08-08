package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	agentRequestKindQuestion            = "question"
	agentRequestKindReview              = "review"
	agentRequestKindDelegateTask        = "delegate_task"
	agentRequestKindAuthorityTransition = "authority_transition"
)

type delegatedAgentTaskRequest struct {
	TaskID      string
	RequestKind string
	Prompt      string
}

type delegatedAgentTaskActiveLaneNudge struct {
	ActiveTaskID    string
	ActiveSessionID string
	DelegatedTaskID string
	ActiveTask      WorkspaceTaskRecord
	DelegatedTask   WorkspaceTaskRecord
}

func normalizeAgentRequestKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "question", "ask", "info", "information":
		return agentRequestKindQuestion
	case "review", "peer_review", "review_task":
		return agentRequestKindReview
	case "delegate_task", "delegate", "handoff", "claim_task", "implement", "work":
		return agentRequestKindDelegateTask
	case "authority_transition", "authority", "boundary_transition", "role_scope_transition":
		return agentRequestKindAuthorityTransition
	default:
		return ""
	}
}

func inferAgentRequestKind(prompt, taskID string) string {
	if strings.TrimSpace(taskID) != "" && promptLooksLikeTaskDelegation(prompt) {
		return agentRequestKindDelegateTask
	}
	if promptLooksLikeTaskDelegation(prompt) && firstAgentRequestTaskID(prompt) != "" {
		return agentRequestKindDelegateTask
	}
	return agentRequestKindQuestion
}

func delegatedAgentTaskRequestFromRecord(request AgentRequestRecord) (delegatedAgentTaskRequest, bool) {
	var payload map[string]any
	if raw := strings.TrimSpace(request.Payload); raw != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	prompt := strings.TrimSpace(stringMapValue(payload, "prompt"))
	if prompt == "" {
		prompt = strings.TrimSpace(request.Payload)
	}
	kind := normalizeAgentRequestKind(stringMapValue(payload, "request_kind"))
	taskID := strings.TrimSpace(stringMapValue(payload, "task_id"))
	if taskID == "" {
		taskID = firstAgentRequestTaskID(prompt)
	}
	switch kind {
	case agentRequestKindDelegateTask, agentRequestKindAuthorityTransition:
		if taskID == "" {
			return delegatedAgentTaskRequest{}, false
		}
		return delegatedAgentTaskRequest{TaskID: taskID, RequestKind: kind, Prompt: prompt}, true
	case agentRequestKindReview, agentRequestKindQuestion:
		return delegatedAgentTaskRequest{}, false
	}
	if taskID != "" && promptLooksLikeTaskDelegation(prompt) {
		return delegatedAgentTaskRequest{TaskID: taskID, RequestKind: agentRequestKindDelegateTask, Prompt: prompt}, true
	}
	return delegatedAgentTaskRequest{}, false
}

func firstAgentRequestTaskID(text string) string {
	for _, match := range agentRequestResponseTaskIDPattern.FindAllString(strings.TrimSpace(text), -1) {
		taskID := strings.Trim(strings.TrimSpace(match), ".,;:()[]{}<>\"'")
		if taskID != "" {
			return taskID
		}
	}
	return ""
}

func promptLooksLikeTaskDelegation(prompt string) bool {
	lower := strings.ToLower(strings.TrimSpace(prompt))
	if lower == "" {
		return false
	}
	for _, marker := range []string{
		"claim task",
		"claim the task",
		"claim implementation task",
		"claim the implementation task",
		"take task",
		"take the task",
		"take implementation task",
		"take the implementation task",
		"pick up task",
		"pick up the task",
		"pick up implementation task",
		"pick up the implementation task",
		"own task",
		"own the task",
		"start work on task",
		"work on task",
		"handle task",
		"handle the task",
		"materialize checkout",
		"create checkout",
		"create branch",
		"commit branch",
		"run browser smoke",
		"browser smoke",
		"visual qa",
		"capture screenshot",
		"publish evidence",
		"publish visual evidence",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	if promptLooksLikeReadOnlyReview(lower) {
		return false
	}
	for _, marker := range []string{
		"execute task",
		"execute the task",
		"run task",
		"run the task",
		"implement task",
		"implement the task",
		"implementation lane",
		"bounded implementation",
		"deliverable",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func promptLooksLikeReadOnlyReview(lowerPrompt string) bool {
	for _, marker := range []string{
		"review this task before",
		"review how to",
		"please review",
		"peer review",
		"review request",
		"answer whether",
		"can you answer",
		"whether i should",
		"should i",
		"give bounded, concrete feedback",
		"give feedback",
		"approve/blockers",
		"missing tests",
		"before terminal completion",
		"read-only",
		"inspect",
		"verify",
		"audit",
		"question",
		"answer",
	} {
		if strings.Contains(lowerPrompt, marker) {
			return true
		}
	}
	return false
}

func stringMapValue(values map[string]any, key string) string {
	if len(values) == 0 {
		return ""
	}
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}

func (r *Runtime) handleDelegatedAgentTaskRequest(ctx context.Context, request AgentRequestRecord, delegated delegatedAgentTaskRequest) error {
	taskID := strings.TrimSpace(delegated.TaskID)
	if taskID == "" {
		return nil
	}
	authorityTransition := delegated.RequestKind == agentRequestKindAuthorityTransition
	if !authorityTransition {
		if nudge, ok := r.delegatedAgentTaskActiveLaneNudge(ctx, taskID); ok {
			if blocker := r.delegatedPendingTriggerQueueBlocker(ctx, request, "request_resume", nudge.ActiveTaskID, nudge.ActiveSessionID); blocker != "" {
				response := formatDelegatedAgentTaskDeclineResponse(request, delegated, blocker)
				if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
					return err
				}
				if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
					log.Printf("[requests] delegated active-lane nudge queue decline evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
				}
				if err := r.postDelegatedTaskDeclinedUpdate(ctx, request, delegated, blocker); err != nil && ctx.Err() == nil {
					log.Printf("[requests] delegated active-lane nudge queue decline update failed for %s: %v", strings.TrimSpace(request.RequestID), err)
				}
				return nil
			}
			response := formatDelegatedAgentTaskActiveLaneNudgeResponse(request, delegated, nudge)
			if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
				return err
			}
			if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
				log.Printf("[requests] delegated active-lane nudge evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
			}
			if err := r.postDelegatedTaskActiveLaneNudgeUpdate(ctx, request, delegated, nudge); err != nil && ctx.Err() == nil {
				log.Printf("[requests] delegated active-lane nudge update failed for %s: %v", strings.TrimSpace(request.RequestID), err)
			}
			return nil
		}
	}
	if !authorityTransition && delegatedAgentTaskRequestMayBeActiveLaneNudge(delegated) {
		if nudge, ok := r.delegatedAgentTaskStoreActiveLaneNudge(ctx, taskID); ok {
			if blocker := r.delegatedPendingTriggerQueueBlocker(ctx, request, "request_resume", nudge.ActiveTaskID, nudge.ActiveSessionID); blocker != "" {
				response := formatDelegatedAgentTaskDeclineResponse(request, delegated, blocker)
				if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
					return err
				}
				if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
					log.Printf("[requests] delegated store active-lane nudge queue decline evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
				}
				if err := r.postDelegatedTaskDeclinedUpdate(ctx, request, delegated, blocker); err != nil && ctx.Err() == nil {
					log.Printf("[requests] delegated store active-lane nudge queue decline update failed for %s: %v", strings.TrimSpace(request.RequestID), err)
				}
				return nil
			}
			response := formatDelegatedAgentTaskActiveLaneNudgeResponse(request, delegated, nudge)
			if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
				return err
			}
			if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
				log.Printf("[requests] delegated store active-lane nudge evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
			}
			if err := r.postDelegatedTaskActiveLaneNudgeUpdate(ctx, request, delegated, nudge); err != nil && ctx.Err() == nil {
				log.Printf("[requests] delegated store active-lane nudge update failed for %s: %v", strings.TrimSpace(request.RequestID), err)
			}
			return nil
		}
	}
	if blocker := r.runtimeSwitchAdmissionFollowThroughBlocker(taskID); blocker != "" {
		response := formatDelegatedAgentTaskDeclineResponse(request, delegated, blocker)
		if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
			return err
		}
		if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
			log.Printf("[requests] delegated task single-active decline evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
		}
		if err := r.postDelegatedTaskDeclinedUpdate(ctx, request, delegated, blocker); err != nil && ctx.Err() == nil {
			log.Printf("[requests] delegated task single-active decline update failed for %s: %v", strings.TrimSpace(request.RequestID), err)
		}
		return nil
	}
	if blocker := r.delegatedAgentTaskAcceptanceBlocker(ctx, delegated); blocker != "" {
		if authorityTransition && authorityTransitionAdmissionBlockerIsNoClaimablePath(blocker) {
			if terminalBlocker, ok, blockErr := r.publishAuthorityTransitionTerminalBlocker(ctx, delegated, blocker); blockErr != nil {
				log.Printf("[requests] authority transition terminal blocker publication failed for %s after admission decline: %v", taskID, blockErr)
			} else if ok {
				blocker = terminalBlocker
			}
		} else if !authorityTransition && runtimeSwitchAdmissionBlockerIsNoClaimablePath(blocker) {
			if terminalBlocker, ok, blockErr := r.publishRuntimeSwitchTerminalBlocker(ctx, delegated, blocker); blockErr != nil {
				log.Printf("[requests] runtime switch terminal blocker publication failed for %s after admission decline: %v", taskID, blockErr)
			} else if ok {
				blocker = terminalBlocker
			}
		}
		response := formatDelegatedAgentTaskDeclineResponse(request, delegated, blocker)
		if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
			return err
		}
		if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
			log.Printf("[requests] delegated task decline evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
		}
		if err := r.postDelegatedTaskDeclinedUpdate(ctx, request, delegated, blocker); err != nil && ctx.Err() == nil {
			log.Printf("[requests] delegated task decline update failed for %s: %v", strings.TrimSpace(request.RequestID), err)
		}
		return nil
	}
	if blocker := r.delegatedPendingTriggerQueueBlocker(ctx, request, "runtime_switch_task", taskID, ""); blocker != "" {
		response := formatDelegatedAgentTaskDeclineResponse(request, delegated, blocker)
		if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
			return err
		}
		if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
			log.Printf("[requests] delegated task queue decline evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
		}
		if err := r.postDelegatedTaskDeclinedUpdate(ctx, request, delegated, blocker); err != nil && ctx.Err() == nil {
			log.Printf("[requests] delegated task queue decline update failed for %s: %v", strings.TrimSpace(request.RequestID), err)
		}
		return nil
	}
	if authorityTransition {
		claimSummary, blocker := r.materializeQueuedAuthorityTransition(ctx, delegated)
		if blocker != "" {
			response := formatDelegatedAgentTaskDeclineResponse(request, delegated, blocker)
			if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
				return err
			}
			if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
				log.Printf("[requests] authority transition admission decline evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
			}
			if err := r.postDelegatedTaskDeclinedUpdate(ctx, request, delegated, blocker); err != nil && ctx.Err() == nil {
				log.Printf("[requests] authority transition admission decline update failed for %s: %v", strings.TrimSpace(request.RequestID), err)
			}
			return nil
		}
		response := formatDelegatedAuthorityTransitionAdmittedResponse(request, delegated, claimSummary)
		if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
			return err
		}
		if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
			log.Printf("[requests] authority transition claim_admitted evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
		}
		if err := r.postDelegatedAuthorityTransitionClaimAdmittedUpdate(ctx, request, delegated, claimSummary); err != nil && ctx.Err() == nil {
			log.Printf("[requests] authority transition claim_admitted update failed for %s: %v", strings.TrimSpace(request.RequestID), err)
		}
		return nil
	}
	claimSummary, blocker := r.materializeQueuedDelegatedTask(ctx, delegated)
	if blocker != "" {
		response := formatDelegatedAgentTaskDeclineResponse(request, delegated, blocker)
		if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
			return err
		}
		if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
			log.Printf("[requests] delegated task admission decline evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
		}
		if err := r.postDelegatedTaskDeclinedUpdate(ctx, request, delegated, blocker); err != nil && ctx.Err() == nil {
			log.Printf("[requests] delegated task admission decline update failed for %s: %v", strings.TrimSpace(request.RequestID), err)
		}
		return nil
	}
	response := formatDelegatedTaskClaimAdmittedResponse(request, delegated, claimSummary)
	if err := r.client.RespondRequest(ctx, request.RequestID, response); err != nil {
		return err
	}
	if err := r.publishAgentRequestResponseEvidence(ctx, request, response); err != nil && ctx.Err() == nil {
		log.Printf("[requests] delegated task claim_admitted evidence publish failed for %s: %v", strings.TrimSpace(request.RequestID), err)
	}
	if err := r.postDelegatedTaskClaimAdmittedUpdate(ctx, request, delegated, claimSummary); err != nil && ctx.Err() == nil {
		log.Printf("[requests] delegated task claim_admitted update failed for %s: %v", strings.TrimSpace(request.RequestID), err)
	}
	return nil
}

func (r *Runtime) delegatedPendingTriggerQueueBlocker(ctx context.Context, request AgentRequestRecord, trigger, taskID, sessionID string) string {
	trigger = normalizeWorkTrigger(trigger)
	taskID = strings.TrimSpace(taskID)
	sessionID = strings.TrimSpace(sessionID)
	if trigger == "" {
		return "delegated task could not queue work: invalid runtime trigger"
	}
	if trigger == "runtime_switch_task" {
		if blocker := r.runtimeSwitchAdmissionFollowThroughBlocker(taskID); blocker != "" {
			return blocker
		}
	}
	if err := r.setPendingWorkTrigger(ctx, trigger, taskID, sessionID); err != nil {
		if ctx.Err() != nil {
			return fmt.Sprintf("delegated task could not queue %s before acceptance: %v", trigger, err)
		}
		log.Printf("[requests] delegated %s durable queue failed for %s before response ack: %v", trigger, strings.TrimSpace(request.RequestID), err)
		return fmt.Sprintf("delegated task could not durably queue %s before acceptance: %v", trigger, err)
	}
	return ""
}

func authorityTransitionAdmissionBlockerIsNoClaimablePath(blocker string) bool {
	return runtimeSwitchAdmissionBlockerIsNoClaimablePath(blocker)
}

func (r *Runtime) runtimeSwitchAdmissionFollowThroughBlocker(taskID string) string {
	taskID = strings.TrimSpace(taskID)
	if r == nil || taskID == "" {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if normalizeWorkTrigger(r.scratch.PendingTrigger) == "runtime_switch_task" && !scratchStatePendingTriggerConsumed(r.scratch) {
		pendingTaskID := strings.TrimSpace(r.scratch.PendingTriggerTask)
		if pendingTaskID != "" && pendingTaskID != taskID {
			return fmt.Sprintf("runtime_switch_task admission blocked by single-active follow-through invariant: pending runtime_switch_task for task %s must materialize claim/session or publish a terminal blocker before accepting task %s", pendingTaskID, taskID)
		}
	}

	activeTaskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	activeSessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	activeCarrier := normalizeWorkTrigger(r.scratch.LastWakeTrigger) == "runtime_switch_task"
	if r.activeTask != nil {
		activeTaskID = firstNonEmpty(strings.TrimSpace(r.activeTask.TaskID), activeTaskID)
		if runtimeSwitchTaskHasTerminalBlocker(*r.activeTask) {
			activeCarrier = false
		} else if isRuntimeAuthorityTransitionTaskID(activeTaskID) || runtimeProjectClaimRepairTask(*r.activeTask) || delegatedAgentTaskIsPatchQueueRevisionFollowup(*r.activeTask) || runtimeSwitchTaskRequiresDecisivePath(*r.activeTask) {
			activeCarrier = true
		}
	}
	activeSessionRunnable := false
	if r.activeSession != nil {
		activeSessionID = firstNonEmpty(strings.TrimSpace(r.activeSession.SessionID), activeSessionID)
		activeSessionRunnable = isSessionRunnable(r.activeSession)
		if sessionTaskID := strings.TrimSpace(r.activeSession.TaskID); sessionTaskID != "" {
			activeTaskID = firstNonEmpty(activeTaskID, sessionTaskID)
		}
	}
	if activeCarrier && activeSessionRunnable && activeTaskID != "" && activeTaskID != taskID {
		return fmt.Sprintf("runtime_switch_task admission blocked by single-active follow-through invariant: active runtime_switch_task carrier task %s/session %s is still runnable; task %s cannot be accepted until the active carrier ends, yields, or publishes a terminal blocker. queued handoff is not progress without single-active claim/session follow-through", activeTaskID, firstNonEmpty(activeSessionID, "<unknown>"), taskID)
	}
	return ""
}

func (r *Runtime) materializeQueuedAuthorityTransition(ctx context.Context, delegated delegatedAgentTaskRequest) (string, string) {
	if r == nil {
		return "", "authority transition admission not confirmed: runtime is unavailable"
	}
	taskID := strings.TrimSpace(delegated.TaskID)
	if taskID == "" {
		return "", "authority transition admission not confirmed: missing task_id"
	}
	task, err := r.ensureRunnableTask(ctx)
	if err != nil {
		if receipt, ok, _ := r.confirmMaterializedRuntimeSwitchTask(ctx, taskID); ok {
			return receipt.Summary, ""
		}
		return "", fmt.Sprintf("authority transition admission not confirmed after runtime_switch_task queue: %v", err)
	}
	if task == nil || strings.TrimSpace(task.TaskID) != taskID {
		if receipt, ok, _ := r.confirmMaterializedRuntimeSwitchTask(ctx, taskID); ok {
			return receipt.Summary, ""
		}
		if blocker, ok, blockErr := r.publishAuthorityTransitionTerminalBlocker(ctx, delegated, fmt.Sprintf("fresh runtime_switch_task admission selected %s instead of %s", selectedTaskID(task), taskID)); blockErr != nil {
			return "", fmt.Sprintf("authority transition admission not confirmed after runtime_switch_task queue, and terminal blocker publication failed: %v", blockErr)
		} else if ok {
			return "", blocker
		}
		activeTaskID, activeSessionID, claimAgentID, claimStatus := r.activeAuthorityTransitionState()
		return "", fmt.Sprintf("authority transition admission not confirmed after runtime_switch_task queue: wanted task %s, selected task=%s active_task_id=%s active_session_id=%s claim_agent_id=%s claim_status=%s", taskID, selectedTaskID(task), firstNonEmpty(activeTaskID, "<empty>"), firstNonEmpty(activeSessionID, "<empty>"), firstNonEmpty(claimAgentID, "<empty>"), firstNonEmpty(claimStatus, "<empty>"))
	}
	activeTaskID, activeSessionID, claimAgentID, claimStatus := r.activeAuthorityTransitionState()
	if activeTaskID != taskID || activeSessionID == "" || !strings.EqualFold(claimAgentID, strings.TrimSpace(r.cfg.AgentID)) || !taskClaimStatusIsActiveOwnership(claimStatus) {
		if receipt, ok, _ := r.confirmMaterializedRuntimeSwitchTask(ctx, taskID); ok {
			return receipt.Summary, ""
		}
		return "", fmt.Sprintf("authority transition admission not confirmed after runtime_switch_task queue: active_task_id=%s active_session_id=%s claim_agent_id=%s claim_status=%s", firstNonEmpty(activeTaskID, "<empty>"), firstNonEmpty(activeSessionID, "<empty>"), firstNonEmpty(claimAgentID, "<empty>"), firstNonEmpty(claimStatus, "<empty>"))
	}
	if err := r.clearMaterializedRuntimeSwitchTaskTrigger(ctx, taskID); err != nil {
		return "", fmt.Sprintf("authority transition admission not confirmed after runtime_switch_task queue: clear consumed trigger failed: %v", err)
	}
	return fmt.Sprintf("task_id=%s claim_agent_id=%s claim_status=%s active_session_id=%s", taskID, claimAgentID, claimStatus, activeSessionID), ""
}

func (r *Runtime) publishAuthorityTransitionTerminalBlocker(ctx context.Context, delegated delegatedAgentTaskRequest, cause string) (string, bool, error) {
	if r == nil || r.client == nil {
		return "", false, nil
	}
	taskID := strings.TrimSpace(delegated.TaskID)
	if taskID == "" {
		return "", false, nil
	}
	bundle, err := r.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID:      r.cfg.WorkspaceID,
		TaskID:           taskID,
		UpdatesLimit:     1,
		ArtifactLimit:    1,
		RelatedTaskLimit: 1,
	})
	if err != nil {
		return "", false, fmt.Errorf("hydrate authority transition task %s: %w", taskID, err)
	}
	if bundle.WorkspaceTask == nil || strings.TrimSpace(bundle.WorkspaceTask.TaskID) != taskID {
		return "", false, nil
	}
	task := *bundle.WorkspaceTask
	if taskSubmitTaskIsTerminal(task) {
		return "", false, nil
	}
	if !runtimeTaskLooksDedicatedAuthorityTransitionCarrier(task) {
		return "", false, nil
	}
	if authorityTransitionTaskHasTerminalBlocker(task) {
		return authorityTransitionTerminalBlockerMessage(task), true, nil
	}
	claimStatus := taskClaimStatus(task)
	if taskClaimStatusIsActiveOwnership(claimStatus) {
		return "", false, nil
	}

	reason := formatAuthorityTransitionTerminalBlockerReason(task, cause)
	// Route through the shared decisive-path primitive (root A). By this point the carrier is not terminal
	// (:491), is a dedicated authority-transition carrier (:494), and is not in active ownership (:501), so
	// the route resolves to the same typed terminal blocker (CANCELLED-close of the unclaimed carrier) the
	// prior direct call produced - now sharing the one fail-closed router (closes the R23->R24 "same bug on
	// the second publisher" trap: #1 and #2 can no longer diverge in how an unrecognized kind terminalizes).
	in := runtimeCarrierDecisivePathInput(task, decisivePathKindAuthorityTransition)
	if _, err := r.executeRuntimeCarrierDecisivePath(ctx, taskID, in, reason, "authority transition"); err != nil {
		return "", false, err
	}
	if err := r.recordAuthorityTransitionTerminalBlockerScratch(ctx, taskID, reason); err != nil {
		return "", false, err
	}
	if err := r.postAuthorityTransitionTerminalBlockerUpdate(ctx, task, reason); err != nil && ctx.Err() == nil {
		log.Printf("[requests] authority transition terminal blocker update failed for %s: %v", taskID, err)
	}
	return reason, true, nil
}

func formatAuthorityTransitionTerminalBlockerReason(task WorkspaceTaskRecord, cause string) string {
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" {
		projectID = "<unknown>"
	}
	cause = firstNonEmpty(strings.TrimSpace(cause), "no fresh claimable runtime_switch_task path materialized")
	contract := authorityTransitionTerminalContractForTask(task)
	return fmt.Sprintf("typed terminal blocker: %s task_id=%s project_id=%s required_transition=%s blocker_kind=%s cause=%s safety=%s", authorityTransitionTerminalBlockerSchema, taskID, projectID, contract.RequiredTransition, contract.BlockerKind, oneLine(cause), contract.SafetyInvariant)
}

func (r *Runtime) recordAuthorityTransitionTerminalBlockerScratch(ctx context.Context, taskID, summary string) error {
	taskID = strings.TrimSpace(taskID)
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		if normalizeWorkTrigger(state.PendingTrigger) == "runtime_switch_task" && strings.TrimSpace(state.PendingTriggerTask) == taskID {
			clearPendingTriggerFields(state)
		}
		if strings.TrimSpace(state.ActiveTaskID) == taskID {
			state.ActiveTaskID = ""
			state.ActiveSessionID = ""
			state.ActiveRunID = ""
		}
		state.LastWakeTrigger = "runtime_switch_task"
		state.LastWakeReason = "authority_transition_terminal_blocker"
		state.LastWakeSummary = strings.TrimSpace(summary)
		state.LastWakeTaskID = taskID
		state.LastWakeSessionID = ""
		state.LastWakeAt = time.Now().UTC().Format(time.RFC3339Nano)
		state.LastSummary = strings.TrimSpace(summary)
	})
}

func (r *Runtime) postAuthorityTransitionTerminalBlockerUpdate(ctx context.Context, task WorkspaceTaskRecord, reason string) error {
	if r == nil || r.client == nil {
		return nil
	}
	contract := authorityTransitionTerminalContractForTask(task)
	payload := map[string]any{
		"schema":              authorityTransitionTerminalBlockerSchema,
		"delegation_state":    "terminal_blocker",
		"request_kind":        agentRequestKindAuthorityTransition,
		"task_id":             strings.TrimSpace(task.TaskID),
		"project_id":          strings.TrimSpace(task.ProjectID),
		"terminal_blocker":    true,
		"blocker_kind":        contract.BlockerKind,
		"required_transition": contract.RequiredTransition,
		"coverage_state":      "not_claimed_terminal",
		"summary":             strings.TrimSpace(reason),
		"safety_invariant":    contract.SafetyInvariant,
	}
	raw, _ := json.Marshal(payload)
	summaryPath := "authority-transition path"
	if strings.EqualFold(contract.RequiredTransition, projectRoleScopeAuthorityTransitionTool) {
		summaryPath = "scope-grant path"
	}
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		UpdateType:  "coordination",
		Summary:     fmt.Sprintf("Authority transition task %s published a typed terminal blocker after no fresh claimable %s materialized.", strings.TrimSpace(task.TaskID), summaryPath),
		PayloadJSON: string(raw),
	})
}

func (r *Runtime) materializeQueuedDelegatedTask(ctx context.Context, delegated delegatedAgentTaskRequest) (string, string) {
	if r == nil {
		return "", "delegated task admission not confirmed: runtime is unavailable"
	}
	taskID := strings.TrimSpace(delegated.TaskID)
	if taskID == "" {
		return "", "delegated task admission not confirmed: missing task_id"
	}
	task, err := r.ensureRunnableTask(ctx)
	if err != nil {
		if receipt, ok, _ := r.confirmMaterializedRuntimeSwitchTask(ctx, taskID); ok {
			return receipt.Summary, ""
		}
		return "", fmt.Sprintf("delegated task admission not confirmed after runtime_switch_task queue: %v", err)
	}
	if task == nil || strings.TrimSpace(task.TaskID) != taskID {
		if receipt, ok, _ := r.confirmMaterializedRuntimeSwitchTask(ctx, taskID); ok {
			return receipt.Summary, ""
		}
		if blocker, ok, blockErr := r.publishRuntimeSwitchTerminalBlocker(ctx, delegated, fmt.Sprintf("fresh runtime_switch_task admission selected %s instead of %s", selectedTaskID(task), taskID)); blockErr != nil {
			return "", fmt.Sprintf("delegated task admission not confirmed after runtime_switch_task queue, and terminal blocker publication failed: %v", blockErr)
		} else if ok {
			return "", blocker
		}
		activeTaskID, activeSessionID, claimAgentID, claimStatus := r.activeDelegatedTaskState()
		return "", fmt.Sprintf("delegated task admission not confirmed after runtime_switch_task queue: wanted task %s, selected task=%s active_task_id=%s active_session_id=%s claim_agent_id=%s claim_status=%s", taskID, selectedTaskID(task), firstNonEmpty(activeTaskID, "<empty>"), firstNonEmpty(activeSessionID, "<empty>"), firstNonEmpty(claimAgentID, "<empty>"), firstNonEmpty(claimStatus, "<empty>"))
	}
	if gap := delegatedTaskProjectCoverageGap(*task); gap != "" {
		return "", fmt.Sprintf("delegated task admission not confirmed after runtime_switch_task queue: %s", gap)
	}
	activeTaskID, activeSessionID, claimAgentID, claimStatus := r.activeDelegatedTaskState()
	if activeTaskID != taskID || activeSessionID == "" || !strings.EqualFold(claimAgentID, strings.TrimSpace(r.cfg.AgentID)) || !taskClaimStatusIsActiveOwnership(claimStatus) {
		if receipt, ok, _ := r.confirmMaterializedRuntimeSwitchTask(ctx, taskID); ok {
			return receipt.Summary, ""
		}
		if blocker, ok, blockErr := r.publishRuntimeSwitchTerminalBlocker(ctx, delegated, fmt.Sprintf("fresh runtime_switch_task admission did not materialize same-agent claim/session: active_task_id=%s active_session_id=%s claim_agent_id=%s claim_status=%s", firstNonEmpty(activeTaskID, "<empty>"), firstNonEmpty(activeSessionID, "<empty>"), firstNonEmpty(claimAgentID, "<empty>"), firstNonEmpty(claimStatus, "<empty>"))); blockErr != nil {
			return "", fmt.Sprintf("delegated task admission not confirmed after runtime_switch_task queue, and terminal blocker publication failed: %v", blockErr)
		} else if ok {
			return "", blocker
		}
		return "", fmt.Sprintf("delegated task admission not confirmed after runtime_switch_task queue: active_task_id=%s active_session_id=%s claim_agent_id=%s claim_status=%s", firstNonEmpty(activeTaskID, "<empty>"), firstNonEmpty(activeSessionID, "<empty>"), firstNonEmpty(claimAgentID, "<empty>"), firstNonEmpty(claimStatus, "<empty>"))
	}
	if err := r.clearMaterializedRuntimeSwitchTaskTrigger(ctx, taskID); err != nil {
		return "", fmt.Sprintf("delegated task admission not confirmed after runtime_switch_task queue: clear consumed trigger failed: %v", err)
	}
	return fmt.Sprintf("task_id=%s claim_agent_id=%s claim_status=%s active_session_id=%s", taskID, claimAgentID, claimStatus, activeSessionID), ""
}

func (r *Runtime) clearMaterializedRuntimeSwitchTaskTrigger(ctx context.Context, taskID string) error {
	if r == nil {
		return nil
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil
	}
	r.mu.Lock()
	stateCopy := r.scratch
	if normalizeWorkTrigger(stateCopy.PendingTrigger) == "runtime_switch_task" && strings.TrimSpace(stateCopy.PendingTriggerTask) == taskID {
		clearPendingTriggerFields(&stateCopy)
		r.scratch = stateCopy
	}
	r.mu.Unlock()
	stateCopy = normalizeScratchStateForPersist(clearConsumedPendingTriggerInState(stateCopy))
	return saveScratchState(ctx, r.client, r.cfg.WorkspaceID, r.cfg.AgentID, stateCopy)
}

func selectedTaskID(task *WorkspaceTaskRecord) string {
	if task == nil {
		return "<nil>"
	}
	return firstNonEmpty(strings.TrimSpace(task.TaskID), "<empty>")
}

func (r *Runtime) activeAuthorityTransitionState() (taskID, sessionID, claimAgentID, claimStatus string) {
	return r.activeDelegatedTaskState()
}

func (r *Runtime) activeDelegatedTaskState() (taskID, sessionID, claimAgentID, claimStatus string) {
	if r == nil {
		return "", "", "", ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeTask != nil {
		taskID = strings.TrimSpace(r.activeTask.TaskID)
		claimAgentID = strings.TrimSpace(pointerValue(r.activeTask.ClaimAgentID))
		claimStatus = taskClaimStatus(*r.activeTask)
	}
	if r.activeSession != nil {
		sessionID = strings.TrimSpace(r.activeSession.SessionID)
	}
	return taskID, sessionID, claimAgentID, claimStatus
}

func (r *Runtime) delegatedAgentTaskActiveLaneNudge(ctx context.Context, taskID string) (delegatedAgentTaskActiveLaneNudge, bool) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" || r == nil || r.client == nil || strings.TrimSpace(r.cfg.WorkspaceID) == "" {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}

	r.mu.Lock()
	activeTask := r.activeTask
	activeSession := r.activeSession
	if activeTask == nil || strings.TrimSpace(activeTask.TaskID) == "" || strings.TrimSpace(activeTask.TaskID) == taskID || !isSessionRunnable(activeSession) {
		r.mu.Unlock()
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	activeSnapshot := *activeTask
	sessionID := ""
	if activeSession != nil {
		sessionID = strings.TrimSpace(activeSession.SessionID)
	}
	r.mu.Unlock()

	if isRuntimeAuthorityTransitionTaskID(strings.TrimSpace(activeSnapshot.TaskID)) ||
		runtimeProjectClaimRepairTask(activeSnapshot) ||
		delegatedAgentTaskIsPatchQueueRevisionFollowup(activeSnapshot) {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	if delegatedAgentTaskCanConsiderPreemptingActiveTask(activeSnapshot) {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	if strings.TrimSpace(activeSnapshot.ProjectID) == "" {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}

	bundle, err := r.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID:      r.cfg.WorkspaceID,
		TaskID:           taskID,
		UpdatesLimit:     1,
		ArtifactLimit:    1,
		RelatedTaskLimit: 1,
	})
	if err != nil || bundle.WorkspaceTask == nil || strings.TrimSpace(bundle.WorkspaceTask.TaskID) == "" {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	delegatedTask := *bundle.WorkspaceTask
	switch strings.ToUpper(strings.TrimSpace(delegatedTask.Status)) {
	case "DONE", "COMPLETED", "RESOLVED", "CLOSED", "CANCELLED", "CANCELED", "FAILED":
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	if idleReflectionPatchQueueTaskRefsFromTask(delegatedTask).PatchQueueTask {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	claimAgentID := strings.TrimSpace(pointerValue(delegatedTask.ClaimAgentID))
	if claimAgentID != "" && claimAgentID != strings.TrimSpace(r.cfg.AgentID) {
		switch taskClaimStatus(delegatedTask) {
		case "CLAIMED", "RUNNING", "ACTIVE":
			return delegatedAgentTaskActiveLaneNudge{}, false
		}
	}
	if !delegatedAgentTaskShouldNudgeActiveLane(activeSnapshot, delegatedTask) {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	return delegatedAgentTaskActiveLaneNudge{
		ActiveTaskID:    strings.TrimSpace(activeSnapshot.TaskID),
		ActiveSessionID: sessionID,
		DelegatedTaskID: strings.TrimSpace(delegatedTask.TaskID),
		ActiveTask:      activeSnapshot,
		DelegatedTask:   delegatedTask,
	}, true
}

func (r *Runtime) delegatedAgentTaskStoreActiveLaneNudge(ctx context.Context, taskID string) (delegatedAgentTaskActiveLaneNudge, bool) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" || r == nil || r.client == nil || strings.TrimSpace(r.cfg.WorkspaceID) == "" || strings.TrimSpace(r.cfg.AgentID) == "" {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	work, err := r.client.WorkNext(ctx, WorkNextInput{
		WorkspaceID:      r.cfg.WorkspaceID,
		AgentID:          r.cfg.AgentID,
		IncludePacket:    boolPtr(true),
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		CoordinationMode: r.cfg.CoordinationMode,
	})
	if err != nil || !work.HasWork || work.Task == nil || strings.TrimSpace(work.Task.TaskID) == "" || strings.TrimSpace(work.Task.TaskID) == taskID {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	if work.Session == nil || strings.TrimSpace(work.Session.SessionID) == "" {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	bundle, err := r.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID:      r.cfg.WorkspaceID,
		TaskID:           taskID,
		UpdatesLimit:     1,
		ArtifactLimit:    1,
		RelatedTaskLimit: 1,
	})
	if err != nil || bundle.WorkspaceTask == nil || strings.TrimSpace(bundle.WorkspaceTask.TaskID) == "" {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	activeTask := *work.Task
	delegatedTask := *bundle.WorkspaceTask
	if !delegatedAgentTaskShouldNudgeActiveLane(activeTask, delegatedTask) {
		return delegatedAgentTaskActiveLaneNudge{}, false
	}
	return delegatedAgentTaskActiveLaneNudge{
		ActiveTaskID:    strings.TrimSpace(activeTask.TaskID),
		ActiveSessionID: strings.TrimSpace(work.Session.SessionID),
		DelegatedTaskID: taskID,
		ActiveTask:      activeTask,
		DelegatedTask:   delegatedTask,
	}, true
}

func (r *Runtime) delegatedAgentTaskAcceptanceBlocker(ctx context.Context, delegated delegatedAgentTaskRequest) string {
	taskID := strings.TrimSpace(delegated.TaskID)
	authorityTransition := delegated.RequestKind == agentRequestKindAuthorityTransition
	taskID = strings.TrimSpace(taskID)
	if taskID == "" || r == nil {
		return ""
	}
	var activeForPreemption *WorkspaceTaskRecord
	r.mu.Lock()
	activeTask := r.activeTask
	activeSession := r.activeSession
	if activeTask != nil && strings.TrimSpace(activeTask.TaskID) != "" && strings.TrimSpace(activeTask.TaskID) != taskID && isSessionRunnable(activeSession) {
		activeSnapshot := *activeTask
		activeTaskID := strings.TrimSpace(activeTask.TaskID)
		if !authorityTransition &&
			!delegatedAgentTaskCanConsiderPreemptingActiveTask(activeSnapshot) &&
			!(delegatedAgentTaskIDLooksStrategicRepair(taskID) && strings.TrimSpace(activeSnapshot.ProjectID) != "") {
			if strings.TrimSpace(activeSnapshot.ProjectID) == "" {
				r.mu.Unlock()
				return fmt.Sprintf("agent is already actively working on task %s; delegation to %s was not accepted to avoid false coordination evidence", activeTaskID, taskID)
			}
		}
		activeForPreemption = &activeSnapshot
		r.mu.Unlock()
	} else {
		r.mu.Unlock()
	}

	if r.client == nil || strings.TrimSpace(r.cfg.WorkspaceID) == "" {
		if activeForPreemption != nil && !isIdleReflectionTask(*activeForPreemption) {
			return fmt.Sprintf("task %s could not be verified before accepting preemption of active task %s", taskID, strings.TrimSpace(activeForPreemption.TaskID))
		}
		return ""
	}
	bundle, err := r.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID:      r.cfg.WorkspaceID,
		TaskID:           taskID,
		UpdatesLimit:     1,
		ArtifactLimit:    1,
		RelatedTaskLimit: 1,
	})
	if err != nil {
		return fmt.Sprintf("task %s could not be verified before acceptance: %v", taskID, err)
	}
	task := bundle.WorkspaceTask
	if task == nil || strings.TrimSpace(task.TaskID) == "" {
		return fmt.Sprintf("task %s does not exist or is not visible in workspace %s; create the task before delegating it", taskID, strings.TrimSpace(r.cfg.WorkspaceID))
	}
	if authorityTransition {
		if blocker := authorityTransitionDedicatedTaskBlocker(*task); blocker != "" {
			return blocker
		}
		if authorityTransitionTaskHasTerminalBlocker(*task) {
			return authorityTransitionTerminalBlockerMessage(*task)
		}
	} else if message, ok := runtimeSwitchTerminalBlockerMessage(*task); ok {
		return message
	}
	switch strings.ToUpper(strings.TrimSpace(task.Status)) {
	case "DONE", "COMPLETED", "RESOLVED", "CLOSED", "CANCELLED", "CANCELED", "FAILED":
		return fmt.Sprintf("task %s is already terminal with status %s", taskID, strings.TrimSpace(task.Status))
	}
	claimAgentID := strings.TrimSpace(pointerValue(task.ClaimAgentID))
	if claimAgentID != "" && claimAgentID != strings.TrimSpace(r.cfg.AgentID) {
		claimActive := taskClaimStatusIsActiveOwnership(taskClaimStatus(*task))
		if authorityTransition {
			claimActive = taskClaimStatusIsActiveAuthorityTransitionOwnership(*task)
		}
		if claimActive {
			return fmt.Sprintf("task %s is already claimed by %s", taskID, claimAgentID)
		}
	}
	if blocker := r.delegatedPatchQueueTaskBlocker(ctx, *task); blocker != "" {
		return blocker
	}
	if blocker := r.delegatedOwnerBoundTaskBlocker(ctx, *task); blocker != "" {
		return blocker
	}
	if blocker := r.delegatedAgentProjectRoleBlocker(ctx, *task); blocker != "" {
		return blocker
	}
	if !authorityTransition && activeForPreemption != nil && !delegatedAgentTaskCanPreemptActiveTask(*activeForPreemption, *task) {
		return fmt.Sprintf("agent is already actively working on task %s; delegation to %s was not accepted to avoid false coordination evidence", strings.TrimSpace(activeForPreemption.TaskID), taskID)
	}
	if blocker := r.delegatedAgentWorkNextGateBlocker(ctx, *task); blocker != "" {
		return blocker
	}
	return ""
}

func (r *Runtime) delegatedPatchQueueTaskBlocker(ctx context.Context, task WorkspaceTaskRecord) string {
	if r == nil || r.client == nil || strings.TrimSpace(task.ProjectID) == "" {
		return ""
	}
	refs := idleReflectionPatchQueueTaskRefsFromTask(task)
	if !refs.PatchQueueTask {
		return ""
	}
	projectID := strings.TrimSpace(task.ProjectID)
	coordination, err := r.client.GetProjectCoordination(ctx, strings.TrimSpace(r.cfg.WorkspaceID), projectID)
	if err != nil {
		return fmt.Sprintf("patch-queue task %s current project coordination could not be verified before acceptance: %v", strings.TrimSpace(task.TaskID), err)
	}
	if stale, reason := idleReflectionPatchQueueTaskResolvedByCoordination(task, coordination); stale {
		return fmt.Sprintf("patch-queue task %s is stale in current project coordination: %s. Do not delegate or queue runtime_switch_task for this stale task; inspect current patch queue state, terminal task result docs, and accepted/integration evidence instead", strings.TrimSpace(task.TaskID), strings.TrimSpace(reason))
	}
	return ""
}

func delegatedAgentTaskCanConsiderPreemptingActiveTask(task WorkspaceTaskRecord) bool {
	return isIdleReflectionTask(task) || delegatedAgentTaskIsProjectUmbrellaAnchor(task)
}

func delegatedAgentTaskIDLooksStrategicRepair(taskID string) bool {
	taskID = strings.TrimSpace(taskID)
	return strings.HasPrefix(taskID, "task-project-claim-repair-") ||
		strings.HasPrefix(taskID, "task-role-scope-")
}

func authorityTransitionDedicatedTaskBlocker(task WorkspaceTaskRecord) string {
	if authorityTransitionTaskLooksDedicated(task) {
		return ""
	}
	return fmt.Sprintf("task %s is not a dedicated authority transition task (task_kind=%s project_lane=%s). authority_transition must target a concrete lead/role-scope or project-claim-repair task such as task-role-scope-* or task-project-claim-repair-*; do not use an implementation/review/product lane as the transition carrier", strings.TrimSpace(task.TaskID), firstNonEmpty(strings.TrimSpace(task.TaskKind), "<empty>"), firstNonEmpty(strings.TrimSpace(task.ProjectLane), "<empty>"))
}

func authorityTransitionTaskHasTerminalBlocker(task WorkspaceTaskRecord) bool {
	if !authorityTransitionTaskLooksDedicated(task) {
		return false
	}
	if taskClaimStatus(task) == "BLOCKED" {
		return true
	}
	if !taskSubmitTaskIsTerminal(task) {
		return false
	}
	contract := authorityTransitionTerminalContractForTask(task)
	return taskHasTypedTerminalBlockerText(task, authorityTransitionTerminalBlockerSchema, contract.BlockerKind, contract.RequiredTransition)
}

func authorityTransitionTerminalBlockerMessage(task WorkspaceTaskRecord) string {
	summary := taskTerminalBlockerSummaryText(task)
	summarySuffix := ""
	if summary != "" {
		summarySuffix = " summary=" + oneLine(summary)
	}
	return fmt.Sprintf("authority transition task %s already published a terminal blocker (claim_agent_id=%s claim_status=%s%s). Inspect that blocker and create a corrected successor/follow-up authority task if the transition is still required; do not send another authority_transition wake for the same carrier",
		strings.TrimSpace(task.TaskID),
		firstNonEmpty(strings.TrimSpace(pointerValue(task.ClaimAgentID)), "<unclaimed>"),
		firstNonEmpty(taskClaimStatus(task), "<empty>"),
		summarySuffix,
	)
}

func authorityTransitionTaskLooksDedicated(task WorkspaceTaskRecord) bool {
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" {
		return false
	}
	if runtimeProjectClaimRepairTask(task) {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), "COORDINATION") {
		return false
	}
	if delegatedAgentTaskIDLooksStrategicRepair(taskID) && strings.TrimSpace(task.ProjectID) != "" {
		return true
	}
	if runtimeTaskHasTag(task, "project-role-scope") || runtimeTaskHasTag(task, "project-claim-repair") {
		return strings.TrimSpace(task.ProjectID) != ""
	}
	// CW-B1: repository-repair is a dedicated authority carrier on the storage side
	// (projectRepositoryRepairTask in agent_work.go, admitted by CR-49). Recognize it here too so a
	// repo-repair authority task is not hard-declined by sender preflight / receiver admission while
	// storage would admit it (the same storage-admits / agent-rejects split CR-45..49 closed for
	// role-scope + claim-repair).
	if runtimeTaskHasTag(task, "project-repo-repair") || runtimeTaskHasTag(task, "project-repository-repair") ||
		runtimeTaskHasTag(task, "repo-repair") || runtimeTaskHasTag(task, "repository-repair") {
		return strings.TrimSpace(task.ProjectID) != ""
	}
	requirements := strings.ToLower(strings.TrimSpace(task.TaskRequirementsJSON))
	if strings.Contains(requirements, "project_role_scope_authority_transition.v1") ||
		strings.Contains(requirements, "project_claim_repair") ||
		strings.Contains(requirements, "project.repository.upsert") ||
		strings.Contains(requirements, "project_repo_register") ||
		strings.Contains(requirements, "project_repo_materialize") {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		taskID,
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
	}, "\n"))
	return strings.TrimSpace(task.ProjectID) != "" &&
		containsAnySignal(text, []string{"authority transition", "role/scope", "role scope", "strategic lead"}) &&
		containsAnySignal(text, []string{"repair", "assign", "boundary", "scope"})
}

func delegatedAgentTaskIsPatchQueueRevisionFollowup(task WorkspaceTaskRecord) bool {
	refs := idleReflectionPatchQueueTaskRefsFromTask(task)
	if !refs.PatchQueueTask {
		return false
	}
	if strings.TrimSpace(refs.QueueID) == "" || strings.TrimSpace(refs.ItemID) == "" || strings.TrimSpace(refs.BranchID) == "" {
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.TaskKind,
		task.TaskTemplate,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
	}, "\n"))
	if !containsAnySignal(text, []string{
		"revision",
		"revise",
		"owner-bound-kind:patch_queue_revision",
		"owner_bound_kind:patch_queue_revision",
		"patch_queue_revision",
	}) {
		return false
	}
	return containsAnySignal(text, []string{"blocked", "rejected", "state: blocked", "state: rejected"})
}

func delegatedAgentTaskCanPreemptActiveTask(activeTask, delegatedTask WorkspaceTaskRecord) bool {
	if isIdleReflectionTask(activeTask) {
		return true
	}
	if runtimeProjectClaimRepairTask(delegatedTask) {
		activeProjectID := strings.TrimSpace(activeTask.ProjectID)
		delegatedProjectID := strings.TrimSpace(delegatedTask.ProjectID)
		if delegatedProjectID == "" {
			return false
		}
		if activeProjectID != "" && activeProjectID != delegatedProjectID {
			return false
		}
		return delegatedAgentTaskActiveTaskCanYieldForStrategicRepair(activeTask)
	}
	if delegatedAgentTaskIsPatchQueueRevisionFollowup(delegatedTask) {
		activeProjectID := strings.TrimSpace(activeTask.ProjectID)
		delegatedProjectID := strings.TrimSpace(delegatedTask.ProjectID)
		return activeProjectID != "" &&
			delegatedProjectID != "" &&
			activeProjectID == delegatedProjectID &&
			delegatedAgentTaskActiveTaskCanYieldForPatchQueueRevision(activeTask)
	}
	if !delegatedAgentTaskIsProjectUmbrellaAnchor(activeTask) {
		return false
	}
	activeProjectID := strings.TrimSpace(activeTask.ProjectID)
	delegatedProjectID := strings.TrimSpace(delegatedTask.ProjectID)
	if activeProjectID == "" || delegatedProjectID == "" || activeProjectID != delegatedProjectID {
		return false
	}
	return !delegatedAgentTaskIsProjectUmbrellaAnchor(delegatedTask)
}

func delegatedAgentTaskActiveTaskCanYieldForStrategicRepair(task WorkspaceTaskRecord) bool {
	if isIdleReflectionTask(task) || delegatedAgentTaskIsProjectUmbrellaAnchor(task) || runtimeTaskLooksAutonomousProjectRoot(task) {
		return true
	}
	if strings.TrimSpace(task.ProjectID) != "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), "COORDINATION") {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane != "" && !delegatedAgentTaskLaneCanResumeActiveWork(lane) {
		return false
	}
	taskID := strings.ToLower(strings.TrimSpace(task.TaskID))
	if strings.HasPrefix(taskID, "task-ambient-project-") {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		taskID,
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
	}, "\n"))
	return containsAnySignal(text, []string{
		"ambient project",
		"project coordination",
		"coordination docs",
		"coordination document",
		"canonical",
		"boundary record",
		"scope repair",
	})
}

func delegatedAgentTaskActiveTaskCanYieldForPatchQueueRevision(task WorkspaceTaskRecord) bool {
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.TaskKind,
		task.TaskTemplate,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
	}, "\n"))
	if !containsAnySignal(text, []string{
		"publication",
		"provenance",
		"review-ready",
		"review ready",
		"review packet",
		"branch state",
		"candidate evidence",
		"evidence packet",
		"handoff packet",
		"coordination packet",
		"final report",
		"publish review-ready",
		"publish review ready",
		"publish branch",
	}) {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(task.ProjectLane)) {
	case "coordination", "documentation", "docs", "handoff", "report", "summary", "summarization", "synthesis":
		return true
	default:
		return false
	}
}

func delegatedAgentTaskShouldNudgeActiveLane(activeTask, delegatedTask WorkspaceTaskRecord) bool {
	activeProjectID := strings.TrimSpace(activeTask.ProjectID)
	delegatedProjectID := strings.TrimSpace(delegatedTask.ProjectID)
	if activeProjectID == "" || delegatedProjectID == "" || activeProjectID != delegatedProjectID {
		return false
	}
	if delegatedAgentTaskIsProjectUmbrellaAnchor(delegatedTask) {
		return false
	}
	if !delegatedAgentTaskLooksActiveLaneNudge(delegatedTask) {
		return false
	}
	activeLane := strings.ToLower(strings.TrimSpace(activeTask.ProjectLane))
	delegatedLane := strings.ToLower(strings.TrimSpace(delegatedTask.ProjectLane))
	if delegatedLane == "" || delegatedLane == activeLane {
		return true
	}
	return delegatedAgentTaskLaneCanResumeActiveWork(delegatedLane)
}

func delegatedAgentTaskRequestMayBeActiveLaneNudge(delegated delegatedAgentTaskRequest) bool {
	text := strings.ToLower(strings.Join([]string{
		delegated.TaskID,
		delegated.Prompt,
	}, " "))
	return containsAnySignal(text, []string{
		"publication",
		"provenance",
		"publish current",
		"publish exact",
		"lane status",
		"current status",
		"current state",
		"lane state",
		"implementation state",
		"branch state",
		"review-ready",
		"review ready",
		"ready for review",
	})
}

func delegatedAgentTaskLooksActiveLaneNudge(task WorkspaceTaskRecord) bool {
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.TaskKind,
		task.TaskTemplate,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
	}, " "))
	return containsAnySignal(text, []string{
		"status",
		"progress",
		"candidate evidence",
		"evidence",
		"publish",
		"handoff",
		"ready for review",
		"review packet",
		"current state",
		"lane state",
		"implementation state",
		"branch state",
		"surface state",
		"proof",
		"coverage",
		"unblock",
	})
}

func delegatedAgentTaskLaneCanResumeActiveWork(lane string) bool {
	switch strings.ToLower(strings.TrimSpace(lane)) {
	case "coordination", "strategy", "strategic", "planning", "plan", "review", "reviewer", "qa", "test", "testing", "verification", "validation", "acceptance", "quality", "quality-review", "ux", "ux-review", "usability", "accessibility", "a11y", "smoke", "browser-smoke", "synthesis", "summary", "summarization", "documentation", "docs", "handoff", "report", "final", "integration", "integrator":
		return true
	default:
		return false
	}
}

func delegatedAgentTaskIsProjectUmbrellaAnchor(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), "COORDINATION") {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane != "" && lane != "strategy" && lane != "planning" && lane != "coordination" {
		return false
	}
	taskID := strings.ToLower(strings.TrimSpace(task.TaskID))
	if strings.HasPrefix(taskID, "root-") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(task.TaskTemplate), "project") {
		return true
	}
	return containsAnySignal(strings.ToLower(strings.Join([]string{taskID, task.Title, task.Description}, "\n")), []string{
		"clean-room",
		"clean room",
		"root task",
		"project strategy",
		"autonomous coordinator",
	})
}

func (r *Runtime) delegatedAgentProjectRoleBlocker(ctx context.Context, task WorkspaceTaskRecord) string {
	if r == nil || r.client == nil || strings.TrimSpace(task.ProjectID) == "" || !runtimeProjectTaskRequiresImplementationGate(task) {
		return ""
	}
	if runtimeTrustFirst(r.cfg) {
		return ""
	}
	projectID := strings.TrimSpace(task.ProjectID)
	coordination, err := r.client.GetProjectCoordination(ctx, strings.TrimSpace(r.cfg.WorkspaceID), projectID)
	if err != nil {
		return fmt.Sprintf("task %s project coordination could not be verified before acceptance: %v", strings.TrimSpace(task.TaskID), err)
	}
	if !projectCoordinationHasActiveExecutionRoles(coordination) {
		return ""
	}
	if _, _, ok := selectProjectClaimWriteScope(coordination, r.cfg.AgentID); ok {
		return ""
	}
	return fmt.Sprintf("task %s requires an active IMPLEMENTER role with write_scope_json for project %s; agent %s is not role-matched", strings.TrimSpace(task.TaskID), projectID, strings.TrimSpace(r.cfg.AgentID))
}

func (r *Runtime) delegatedAgentWorkNextGateBlocker(ctx context.Context, task WorkspaceTaskRecord) string {
	if r == nil || r.client == nil || strings.TrimSpace(r.cfg.WorkspaceID) == "" || strings.TrimSpace(r.cfg.AgentID) == "" || strings.TrimSpace(task.TaskID) == "" {
		return ""
	}
	if !delegatedAgentTaskMayNeedWorkNextGateCheck(task) {
		return ""
	}
	work, err := r.client.WorkNext(ctx, WorkNextInput{
		WorkspaceID:      r.cfg.WorkspaceID,
		AgentID:          r.cfg.AgentID,
		IncludePacket:    boolPtr(true),
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  strings.TrimSpace(task.TaskID),
		CoordinationMode: r.cfg.CoordinationMode,
	})
	if err != nil {
		return fmt.Sprintf("agent.work.next preflight failed for runtime_switch_task on task %s before acceptance: %v. Do not queue switch_queued without a durable admission outcome", strings.TrimSpace(task.TaskID), err)
	}
	if work.HasWork {
		if work.Task != nil && strings.TrimSpace(work.Task.TaskID) == strings.TrimSpace(task.TaskID) {
			return ""
		}
		routedTaskID := ""
		if work.Task != nil {
			routedTaskID = strings.TrimSpace(work.Task.TaskID)
		}
		return fmt.Sprintf("agent.work.next did not admit runtime_switch_task directly to task %s before acceptance; routed to %s instead. Do not queue switch_queued for a task that will not become the active durable lane", strings.TrimSpace(task.TaskID), firstNonEmpty(routedTaskID, "another lane"))
	}
	state := delegatedSwitchBlockedState(work)
	if state == "" {
		state = firstNonEmpty(strings.TrimSpace(work.Reason), "trigger_no_work")
	}
	return fmt.Sprintf("agent.work.next rejected runtime_switch_task for task %s before acceptance: %s. Do not queue switch_queued for this task until the required admission condition is satisfied", strings.TrimSpace(task.TaskID), delegatedWorkNextBlockSummary(work, state))
}

func delegatedAgentTaskMayNeedWorkNextGateCheck(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.TaskID) == "" {
		return false
	}
	if strings.TrimSpace(task.ProjectID) != "" {
		return true
	}
	if strings.TrimSpace(task.ProjectLane) != "" || strings.TrimSpace(task.TaskRequirementsJSON) != "" || len(task.WriteScopeHints) > 0 {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.TaskKind,
		task.TaskTemplate,
		strings.Join(task.Tags, " "),
	}, " "))
	return containsAnySignal(text, []string{
		"claim-repair",
		"owner-bound",
		"patch queue",
		"patch_queue",
		"review-ready",
		"review ready",
		"visual acceptance",
		"browser smoke",
	})
}

func delegatedWorkNextBlockSummary(work AgentWorkNextResult, fallback string) string {
	if work.Packet != nil {
		if work.Packet.Gate != nil && strings.TrimSpace(work.Packet.Gate.Summary) != "" {
			return strings.TrimSpace(work.Packet.Gate.Summary)
		}
		if strings.TrimSpace(work.Packet.WhyNow) != "" {
			return strings.TrimSpace(work.Packet.WhyNow)
		}
	}
	return firstNonEmpty(work.ProfileGateSummary, work.Reason, fallback)
}

func projectCoordinationHasActiveExecutionRoles(coordination ProjectCoordinationRecord) bool {
	for _, role := range coordination.Roles {
		if !strings.EqualFold(strings.TrimSpace(role.Status), "ACTIVE") {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(role.RoleType), "STRATEGIC_LEAD") {
			continue
		}
		return true
	}
	return false
}

func formatDelegatedAuthorityTransitionAdmittedResponse(request AgentRequestRecord, delegated delegatedAgentTaskRequest, claimSummary string) string {
	return fmt.Sprintf("Authority transition claim_admitted for task %s from %s: %s. The transition is still not complete until this task publishes project_role_assign applied/denied evidence or a typed terminal blocker.", strings.TrimSpace(delegated.TaskID), firstNonEmpty(request.FromAgentID, "peer"), firstNonEmpty(strings.TrimSpace(claimSummary), "target task is active"))
}

func formatDelegatedTaskClaimAdmittedResponse(request AgentRequestRecord, delegated delegatedAgentTaskRequest, claimSummary string) string {
	return fmt.Sprintf("Delegated task claim_admitted for task %s from %s: %s. Work is not complete until this task publishes the required durable product/review/planning evidence (branch-bound evidence for code-writing work) or a typed terminal blocker.", strings.TrimSpace(delegated.TaskID), firstNonEmpty(request.FromAgentID, "peer"), firstNonEmpty(strings.TrimSpace(claimSummary), "target task is active"))
}

func formatDelegatedAgentTaskDeclineResponse(request AgentRequestRecord, delegated delegatedAgentTaskRequest, reason string) string {
	guidance := "Please leave the task visible for frontier self-selection, delegate to an idle suitable agent, or update the task fit/write-scope hints before relying on this lane."
	if strings.Contains(strings.ToLower(strings.TrimSpace(reason)), "already terminal") {
		guidance = "Inspect the terminal task result doc before declaring its evidence missing; create a new validation task only if the terminal result is absent, stale, or does not cover the current branch/head."
	}
	if delegated.RequestKind == agentRequestKindAuthorityTransition {
		guidance = "Authority transitions must run on a dedicated authority task owned by the authorized peer. Create or reuse the task-role-scope-* / lead repair task and send that task_id; do not use the requester's currently claimed lane as the transition carrier."
		if strings.Contains(strings.ToLower(strings.TrimSpace(reason)), "not a dedicated authority transition task") {
			guidance = "Call project_role_assign from the non-lead context to create or reuse the canonical task-role-scope-* repair task, then send authority_transition with that task_id."
		}
		return fmt.Sprintf("Cannot accept authority transition task %s from %s: %s. %s", strings.TrimSpace(delegated.TaskID), firstNonEmpty(request.FromAgentID, "peer"), strings.TrimSpace(reason), guidance)
	}
	return fmt.Sprintf("Cannot accept delegated task %s from %s: %s. %s", strings.TrimSpace(delegated.TaskID), firstNonEmpty(request.FromAgentID, "peer"), strings.TrimSpace(reason), guidance)
}

func formatDelegatedAgentTaskActiveLaneNudgeResponse(request AgentRequestRecord, delegated delegatedAgentTaskRequest, nudge delegatedAgentTaskActiveLaneNudge) string {
	return fmt.Sprintf("Queued request_resume on active task %s for delegated same-project lane nudge %s from %s. This does not claim the sidecar task; coverage remains tied to the active task until it publishes current status/evidence or a claim_admitted branch-bound task takes over.", strings.TrimSpace(nudge.ActiveTaskID), strings.TrimSpace(delegated.TaskID), firstNonEmpty(request.FromAgentID, "peer"))
}

func (r *Runtime) postDelegatedAuthorityTransitionClaimAdmittedUpdate(ctx context.Context, request AgentRequestRecord, delegated delegatedAgentTaskRequest, claimSummary string) error {
	if r == nil || r.client == nil {
		return nil
	}
	payload := map[string]any{
		"delegation_state": "claim_admitted",
		"request_id":       strings.TrimSpace(request.RequestID),
		"from_agent_id":    strings.TrimSpace(request.FromAgentID),
		"to_agent_id":      strings.TrimSpace(request.ToAgentID),
		"task_id":          strings.TrimSpace(delegated.TaskID),
		"request_kind":     agentRequestKindAuthorityTransition,
		"next_trigger":     "",
		"coverage_state":   "authority_task_claim_admitted",
		"claim_summary":    strings.TrimSpace(claimSummary),
	}
	raw, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: firstNonEmpty(request.WorkspaceID, r.cfg.WorkspaceID),
		AgentID:     firstNonEmpty(r.cfg.AgentID, request.ToAgentID),
		UpdateType:  "coordination",
		Summary:     fmt.Sprintf("Authority transition task %s claim_admitted for %s; waiting for project_role_assign applied/denied evidence.", strings.TrimSpace(delegated.TaskID), firstNonEmpty(request.ToAgentID, r.cfg.AgentID, "agent")),
		PayloadJSON: string(raw),
	})
}

func (r *Runtime) postDelegatedTaskClaimAdmittedUpdate(ctx context.Context, request AgentRequestRecord, delegated delegatedAgentTaskRequest, claimSummary string) error {
	if r == nil || r.client == nil {
		return nil
	}
	payload := map[string]any{
		"delegation_state": "claim_admitted",
		"request_id":       strings.TrimSpace(request.RequestID),
		"from_agent_id":    strings.TrimSpace(request.FromAgentID),
		"to_agent_id":      strings.TrimSpace(request.ToAgentID),
		"task_id":          strings.TrimSpace(delegated.TaskID),
		"request_kind":     agentRequestKindDelegateTask,
		"next_trigger":     "",
		"coverage_state":   "delegated_task_claim_admitted",
		"claim_summary":    strings.TrimSpace(claimSummary),
	}
	raw, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: firstNonEmpty(request.WorkspaceID, r.cfg.WorkspaceID),
		AgentID:     firstNonEmpty(r.cfg.AgentID, request.ToAgentID),
		UpdateType:  "coordination",
		Summary:     fmt.Sprintf("Delegated task %s claim_admitted for %s; waiting for product/review evidence or typed blocker.", strings.TrimSpace(delegated.TaskID), firstNonEmpty(request.ToAgentID, r.cfg.AgentID, "agent")),
		PayloadJSON: string(raw),
	})
}

func (r *Runtime) postDelegatedTaskActiveLaneNudgeUpdate(ctx context.Context, request AgentRequestRecord, delegated delegatedAgentTaskRequest, nudge delegatedAgentTaskActiveLaneNudge) error {
	if r == nil || r.client == nil {
		return nil
	}
	payload := map[string]any{
		"delegation_state":  "active_lane_resume_queued",
		"request_id":        strings.TrimSpace(request.RequestID),
		"from_agent_id":     strings.TrimSpace(request.FromAgentID),
		"to_agent_id":       strings.TrimSpace(request.ToAgentID),
		"delegated_task_id": strings.TrimSpace(delegated.TaskID),
		"active_task_id":    strings.TrimSpace(nudge.ActiveTaskID),
		"active_session_id": strings.TrimSpace(nudge.ActiveSessionID),
		"request_kind":      agentRequestKindDelegateTask,
		"next_trigger":      "request_resume",
		"coverage_state":    "active_task_resume_only",
	}
	raw, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: firstNonEmpty(request.WorkspaceID, r.cfg.WorkspaceID),
		AgentID:     firstNonEmpty(r.cfg.AgentID, request.ToAgentID),
		UpdateType:  "coordination",
		Summary:     fmt.Sprintf("Delegated same-project lane nudge %s resumed active task %s for %s; sidecar task remains unclaimed until explicit ownership evidence exists.", strings.TrimSpace(delegated.TaskID), strings.TrimSpace(nudge.ActiveTaskID), firstNonEmpty(request.ToAgentID, r.cfg.AgentID, "agent")),
		PayloadJSON: string(raw),
	})
}

func (r *Runtime) postDelegatedTaskDeclinedUpdate(ctx context.Context, request AgentRequestRecord, delegated delegatedAgentTaskRequest, reason string) error {
	if r == nil || r.client == nil {
		return nil
	}
	requestKind := firstNonEmpty(strings.TrimSpace(delegated.RequestKind), agentRequestKindDelegateTask)
	suggestedRoute, preferredTransition := delegatedTaskDeclineRouteForKind(requestKind, reason)
	payload := map[string]any{
		"delegation_state":     "declined",
		"request_id":           strings.TrimSpace(request.RequestID),
		"from_agent_id":        strings.TrimSpace(request.FromAgentID),
		"to_agent_id":          strings.TrimSpace(request.ToAgentID),
		"task_id":              strings.TrimSpace(delegated.TaskID),
		"request_kind":         requestKind,
		"coverage_state":       "not_claimed",
		"decline_reason":       strings.TrimSpace(reason),
		"suggested_route":      suggestedRoute,
		"preferred_transition": preferredTransition,
	}
	raw, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: firstNonEmpty(request.WorkspaceID, r.cfg.WorkspaceID),
		AgentID:     firstNonEmpty(r.cfg.AgentID, request.ToAgentID),
		UpdateType:  "coordination",
		Summary:     fmt.Sprintf("%s %s declined for %s; suggested_route=%s.", delegatedAgentRequestKindLabel(requestKind), strings.TrimSpace(delegated.TaskID), firstNonEmpty(request.ToAgentID, r.cfg.AgentID, "agent"), suggestedRoute),
		PayloadJSON: string(raw),
	})
}

func delegatedTaskDeclineRoute(reason string) (string, string) {
	return delegatedTaskDeclineRouteForKind(agentRequestKindDelegateTask, reason)
}

func delegatedTaskDeclineRouteForKind(requestKind, reason string) (string, string) {
	lower := strings.ToLower(strings.TrimSpace(reason))
	if requestKind == agentRequestKindAuthorityTransition {
		switch {
		case strings.Contains(lower, "already claimed") || strings.Contains(lower, "already actively working") || strings.Contains(lower, "active lane"):
			return "create_or_reuse_dedicated_authority_transition_task", "authority_transition_task_claim_repair"
		case strings.Contains(lower, "already terminal") || strings.Contains(lower, "terminal"):
			return "inspect_authority_transition_terminal_receipt", "inspect_terminal_authority_evidence"
		case strings.Contains(lower, "role") || strings.Contains(lower, "write_scope"):
			return "run_or_queue_project_role_assign_on_authority_task", "project_role_assign"
		default:
			return "leave_authority_task_visible_for_authorized_peer_or_create_canonical_role_scope_task", "queue_authority_transition_task"
		}
	}
	switch {
	case strings.Contains(lower, "profile_task_mode_mismatch") || strings.Contains(lower, "fresh-selection mode") || strings.Contains(lower, "profile"):
		return "assign_to_eligible_profile_or_leave_for_frontier_self_selection", "route_to_eligible_profile"
	case strings.Contains(lower, "project_claim_scope_busy") || strings.Contains(lower, "scope busy") || strings.Contains(lower, "active branch"):
		return "resolve_scope_conflict_or_delegate_to_current_owner", "claim_scope_resolve"
	case strings.Contains(lower, "project_validation_artifact_missing") || strings.Contains(lower, "reviewable artifact"):
		return "wait_for_reviewable_artifact_or_create_artifact_producing_action_route", "wait_for_artifact"
	case strings.Contains(lower, "dependency") || strings.Contains(lower, "blocked by"):
		return "route_to_dependency_resolution_before_redelegation", "resolve_dependency"
	case strings.Contains(lower, "already claimed") || strings.Contains(lower, "already actively working") || strings.Contains(lower, "active lane"):
		return "request_status_resume_or_offer_assistance_to_active_lane", "request_resume"
	case strings.Contains(lower, "already terminal") || strings.Contains(lower, "terminal"):
		return "inspect_terminal_result_before_creating_follow_up", "inspect_terminal_evidence"
	case strings.Contains(lower, "role") || strings.Contains(lower, "write_scope"):
		return "repair_role_or_write_scope_then_retry_targeted_delegation", "repair_assignment_boundary"
	default:
		return "leave_visible_for_frontier_self_selection_or_delegate_to_suitable_agent", "route_or_redelegate"
	}
}

func delegatedAgentRequestKindLabel(requestKind string) string {
	if requestKind == agentRequestKindAuthorityTransition {
		return "Authority transition task"
	}
	return "Delegated task"
}

func (r *Runtime) maybePostDelegatedSwitchBlockedUpdate(ctx context.Context, work AgentWorkNextResult, trigger pendingWorkTrigger) error {
	if r == nil || r.client == nil || normalizeWorkTrigger(trigger.Trigger) != "runtime_switch_task" || strings.TrimSpace(trigger.TaskID) == "" || work.HasWork {
		return nil
	}
	state := delegatedSwitchBlockedState(work)
	if state == "" {
		return nil
	}
	packet := work.Packet
	var blockers []string
	gateSummary := ""
	if !delegatedSwitchProfileReasonIsNonBlocking(work.ProfileGateReason) {
		gateSummary = strings.TrimSpace(work.ProfileGateSummary)
	}
	if packet != nil {
		blockers = uniqueTrimmedAgentRequestStrings(packet.ContextHints.AnchorConflictTaskIDs)
		if gateSummary == "" && packet.Gate != nil {
			gateSummary = strings.TrimSpace(packet.Gate.Summary)
		}
		if gateSummary == "" {
			gateSummary = strings.TrimSpace(packet.WhyNow)
		}
	}
	profileSummary := ""
	if !delegatedSwitchProfileReasonIsNonBlocking(work.ProfileGateReason) {
		profileSummary = strings.TrimSpace(work.ProfileGateSummary)
	}
	summary := fmt.Sprintf("Delegated task %s could not reach claim_admitted for %s: %s.", strings.TrimSpace(trigger.TaskID), strings.TrimSpace(r.cfg.AgentID), firstNonEmpty(gateSummary, profileSummary, work.Reason, state))
	suggestedRoute, preferredTransition := delegatedTaskDeclineRoute(strings.Join([]string{
		state,
		strings.TrimSpace(work.Reason),
		strings.TrimSpace(work.ProfileGateReason),
		gateSummary,
		profileSummary,
	}, " "))
	payload := map[string]any{
		"delegation_state":     state,
		"task_id":              strings.TrimSpace(trigger.TaskID),
		"to_agent_id":          strings.TrimSpace(r.cfg.AgentID),
		"trigger":              "runtime_switch_task",
		"coverage_state":       "not_claimed",
		"work_reason":          strings.TrimSpace(work.Reason),
		"profile_gate_reason":  strings.TrimSpace(work.ProfileGateReason),
		"profile_gate_summary": strings.TrimSpace(work.ProfileGateSummary),
		"gate_summary":         gateSummary,
		"blocker_task_ids":     blockers,
		"suggested_route":      suggestedRoute,
		"preferred_transition": preferredTransition,
		"summary":              summary,
	}
	raw, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: firstNonEmpty(strings.TrimSpace(work.WorkspaceID), r.cfg.WorkspaceID),
		AgentID:     strings.TrimSpace(r.cfg.AgentID),
		UpdateType:  "coordination",
		Summary:     summary,
		PayloadJSON: string(raw),
	})
}

func delegatedSwitchNoWorkShouldClearPending(work AgentWorkNextResult, trigger pendingWorkTrigger) bool {
	if normalizeWorkTrigger(trigger.Trigger) != "runtime_switch_task" || strings.TrimSpace(trigger.TaskID) == "" || work.HasWork {
		return false
	}
	if isClosedGateWorkNext(work) {
		return false
	}
	state := delegatedSwitchBlockedState(work)
	if strings.HasPrefix(state, "trigger_") {
		return true
	}
	switch state {
	case "blocked_profile", "blocked_role", "blocked_project_gate", "blocked_owner_bound", "blocked_validation_artifact", "blocked_targeted_delegation", "blocked_dependency", "claim_rejected":
		return true
	default:
		return false
	}
}

func delegatedSwitchBlockedState(work AgentWorkNextResult) string {
	reason := strings.ToLower(strings.TrimSpace(work.Reason))
	profileReason := strings.ToLower(strings.TrimSpace(work.ProfileGateReason))
	switch {
	case reason == "task_dependency_blocked":
		return "blocked_dependency"
	case work.ProfileGateBlockedWork && (strings.Contains(reason, "profile") || strings.Contains(profileReason, "profile")):
		return "blocked_profile"
	case !delegatedSwitchProfileReasonIsNonBlocking(profileReason) && (strings.Contains(reason, "profile") || strings.Contains(profileReason, "profile")):
		return "blocked_profile"
	case strings.HasPrefix(reason, "trigger_"):
		return reason
	case reason == "project_gate_closed":
		return "blocked_project_gate"
	case reason == "project_validation_artifact_missing":
		return "blocked_validation_artifact"
	case reason == "project_targeted_delegation_required":
		return "blocked_targeted_delegation"
	case strings.Contains(reason, "owner_bound"):
		return "blocked_owner_bound"
	case strings.Contains(reason, "role") || strings.Contains(profileReason, "role"):
		return "blocked_role"
	case strings.Contains(reason, "claim"):
		return "claim_rejected"
	default:
		return ""
	}
}

func delegatedSwitchProfileReasonIsNonBlocking(reason string) bool {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "", "profile_allows_autonomous_execution":
		return true
	default:
		return false
	}
}

func uniqueTrimmedAgentRequestStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}
