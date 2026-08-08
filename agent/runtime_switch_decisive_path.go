package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

const runtimeSwitchTerminalBlockerSchema = "runtime_switch_terminal_blocker.v1"

type runtimeSwitchTerminalBlockerContract struct {
	Schema             string
	CarrierKind        string
	RequestKind        string
	RequiredTransition string
	BlockerKind        string
	SafetyInvariant    string
	CoverageState      string
	SummaryPath        string
	WakeReason         string
}

func runtimeSwitchTerminalBlockerContractForTask(task WorkspaceTaskRecord) (runtimeSwitchTerminalBlockerContract, bool) {
	if runtimeTaskLooksDedicatedAuthorityTransitionCarrier(task) {
		authority := authorityTransitionTerminalContractForTask(task)
		summaryPath := "authority-transition path"
		if strings.EqualFold(authority.RequiredTransition, projectRoleScopeAuthorityTransitionTool) {
			summaryPath = "scope-grant path"
		}
		return runtimeSwitchTerminalBlockerContract{
			Schema:             authorityTransitionTerminalBlockerSchema,
			CarrierKind:        "authority_transition",
			RequestKind:        agentRequestKindAuthorityTransition,
			RequiredTransition: authority.RequiredTransition,
			BlockerKind:        authority.BlockerKind,
			SafetyInvariant:    authority.SafetyInvariant,
			CoverageState:      "not_claimed_terminal",
			SummaryPath:        summaryPath,
			WakeReason:         "authority_transition_terminal_blocker",
		}, true
	}
	if record, ok := sideEffectResolutionFollowupRecordFromTask(task); ok && strings.EqualFold(record.AdmissionKind, "abpc_recovery_action") {
		action := firstNonEmpty(strings.TrimSpace(record.ActionKind), strings.TrimSpace(record.Decision), "resolve_bucket")
		return runtimeSwitchTerminalBlockerContract{
			Schema:             runtimeSwitchTerminalBlockerSchema,
			CarrierKind:        "side_effect_resolution_successor",
			RequestKind:        agentRequestKindDelegateTask,
			RequiredTransition: "side_effect_resolution_followup:" + action,
			BlockerKind:        "no_fresh_claimable_side_effect_successor_path",
			SafetyInvariant:    "no_completion_without_side_effect_resolution_receipt_or_claim_admitted_successor",
			CoverageState:      "not_claimed_terminal",
			SummaryPath:        "side-effect successor path",
			WakeReason:         "runtime_switch_terminal_blocker",
		}, true
	}
	if delegatedAgentTaskIsPatchQueueRevisionFollowup(task) {
		return runtimeSwitchTerminalBlockerContract{
			Schema:             runtimeSwitchTerminalBlockerSchema,
			CarrierKind:        "patch_queue_revision_followup",
			RequestKind:        agentRequestKindDelegateTask,
			RequiredTransition: "patch_queue_revision_claim_or_revision_receipt",
			BlockerKind:        "no_fresh_claimable_patch_queue_revision_path",
			SafetyInvariant:    "no_completion_without_patch_queue_revision_evidence_or_claim_admitted_successor",
			CoverageState:      "not_claimed_terminal",
			SummaryPath:        "patch-queue revision path",
			WakeReason:         "runtime_switch_terminal_blocker",
		}, true
	}
	return runtimeSwitchTerminalBlockerContract{}, false
}

func runtimeSwitchTaskRequiresDecisivePath(task WorkspaceTaskRecord) bool {
	_, ok := runtimeSwitchTerminalBlockerContractForTask(task)
	return ok
}

func runtimeSwitchTaskHasTerminalBlocker(task WorkspaceTaskRecord) bool {
	contract, ok := runtimeSwitchTerminalBlockerContractForTask(task)
	if !ok {
		return false
	}
	if strings.EqualFold(contract.Schema, authorityTransitionTerminalBlockerSchema) {
		return authorityTransitionTaskHasTerminalBlocker(task)
	}
	if taskClaimStatus(task) != "BLOCKED" && !taskSubmitTaskIsTerminal(task) {
		return false
	}
	return taskHasTypedTerminalBlockerText(task, contract.Schema, contract.BlockerKind, contract.CarrierKind)
}

func runtimeSwitchTerminalBlockerMessage(task WorkspaceTaskRecord) (string, bool) {
	contract, ok := runtimeSwitchTerminalBlockerContractForTask(task)
	if !ok || !runtimeSwitchTaskHasTerminalBlocker(task) {
		return "", false
	}
	if strings.EqualFold(contract.Schema, authorityTransitionTerminalBlockerSchema) {
		return authorityTransitionTerminalBlockerMessage(task), true
	}
	summary := taskTerminalBlockerSummaryText(task)
	summarySuffix := ""
	if summary != "" {
		summarySuffix = " summary=" + oneLine(summary)
	}
	return fmt.Sprintf("runtime switch carrier task %s already published a terminal blocker (carrier_kind=%s claim_agent_id=%s claim_status=%s%s). Inspect that blocker and create a corrected successor/follow-up task if the transition is still required; do not send another runtime_switch_task wake for the same carrier",
		strings.TrimSpace(task.TaskID),
		contract.CarrierKind,
		firstNonEmpty(strings.TrimSpace(pointerValue(task.ClaimAgentID)), "<unclaimed>"),
		firstNonEmpty(taskClaimStatus(task), "<empty>"),
		summarySuffix,
	), true
}

func runtimeSwitchAdmissionBlockerIsNoClaimablePath(blocker string) bool {
	lower := strings.ToLower(strings.TrimSpace(blocker))
	if lower == "" {
		return false
	}
	if strings.Contains(lower, "agent.work.next preflight failed") {
		return false
	}
	return strings.Contains(lower, "agent.work.next rejected runtime_switch_task") ||
		strings.Contains(lower, "agent.work.next did not admit runtime_switch_task directly") ||
		strings.Contains(lower, "delegated task admission not confirmed after runtime_switch_task queue")
}

func (r *Runtime) publishRuntimeSwitchTerminalBlocker(ctx context.Context, delegated delegatedAgentTaskRequest, cause string) (string, bool, error) {
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
		return "", false, fmt.Errorf("hydrate runtime switch carrier task %s: %w", taskID, err)
	}
	if bundle.WorkspaceTask == nil || strings.TrimSpace(bundle.WorkspaceTask.TaskID) != taskID {
		return "", false, nil
	}
	task := *bundle.WorkspaceTask
	if taskSubmitTaskIsTerminal(task) {
		return "", false, nil
	}
	contract, ok := runtimeSwitchTerminalBlockerContractForTask(task)
	if !ok || strings.EqualFold(contract.Schema, authorityTransitionTerminalBlockerSchema) {
		return "", false, nil
	}
	if message, ok := runtimeSwitchTerminalBlockerMessage(task); ok {
		return message, true, nil
	}
	if taskClaimStatusIsActiveOwnership(taskClaimStatus(task)) {
		return "", false, nil
	}

	reason := formatRuntimeSwitchTerminalBlockerReason(task, contract, cause)
	// Route through the shared decisive-path primitive (root A). By this point the carrier is not terminal
	// (:151) and not in active ownership (:161), so the route resolves to a typed terminal blocker
	// (CANCELLED-close of the unclaimed carrier) - identical to the prior direct call, but now the decision
	// flows through the one fail-closed router every carrier kind shares.
	in := runtimeCarrierDecisivePathInput(task, contract.CarrierKind)
	if _, err := r.executeRuntimeCarrierDecisivePath(ctx, taskID, in, reason, "runtime switch carrier"); err != nil {
		return "", false, err
	}
	if err := r.recordRuntimeSwitchTerminalBlockerScratch(ctx, taskID, reason, contract); err != nil {
		return "", false, err
	}
	if err := r.postRuntimeSwitchTerminalBlockerUpdate(ctx, task, contract, reason); err != nil && ctx.Err() == nil {
		log.Printf("[requests] runtime switch terminal blocker update failed for %s: %v", taskID, err)
	}
	return reason, true, nil
}

func (r *Runtime) publishTypedTerminalBlockerTaskOutcome(ctx context.Context, taskID, reason, carrierLabel string) error {
	taskID = strings.TrimSpace(taskID)
	carrierLabel = firstNonEmpty(strings.TrimSpace(carrierLabel), "runtime switch carrier")
	if r == nil || r.client == nil || taskID == "" {
		return nil
	}
	if err := r.client.BlockTask(ctx, TaskBlockInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		TaskID:      taskID,
		Reason:      reason,
	}); err != nil {
		if !runtimeSwitchTerminalBlockerMissingClaim(err) {
			return fmt.Errorf("block %s task %s: %w", carrierLabel, taskID, err)
		}
		if closeErr := r.client.CloseTask(ctx, TaskCloseInput{
			WorkspaceID: r.cfg.WorkspaceID,
			TaskID:      taskID,
			ActorID:     r.cfg.AgentID,
			Resolution:  "CANCELLED",
			Reason:      reason,
		}); closeErr != nil {
			return fmt.Errorf("close unclaimed %s task %s after missing claim: %w (block error: %v)", carrierLabel, taskID, closeErr, err)
		}
	}
	return nil
}

func runtimeSwitchTerminalBlockerMissingClaim(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "task claim not found") ||
		strings.Contains(text, "claim not found") ||
		strings.Contains(text, "not claimed")
}

func taskHasTypedTerminalBlockerText(task WorkspaceTaskRecord, markers ...string) bool {
	text := strings.ToLower(taskTerminalBlockerSummaryText(task))
	if !strings.Contains(text, "typed terminal blocker") {
		return false
	}
	for _, marker := range markers {
		marker = strings.ToLower(strings.TrimSpace(marker))
		if marker != "" && strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func taskTerminalBlockerSummaryText(task WorkspaceTaskRecord) string {
	return strings.TrimSpace(strings.Join([]string{
		strings.TrimSpace(pointerValue(task.ClaimSummary)),
		strings.TrimSpace(task.CloseReason),
	}, " "))
}

func formatRuntimeSwitchTerminalBlockerReason(task WorkspaceTaskRecord, contract runtimeSwitchTerminalBlockerContract, cause string) string {
	if strings.EqualFold(contract.Schema, authorityTransitionTerminalBlockerSchema) {
		return formatAuthorityTransitionTerminalBlockerReason(task, cause)
	}
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" {
		projectID = "<unknown>"
	}
	cause = firstNonEmpty(strings.TrimSpace(cause), "no fresh claimable runtime_switch_task path materialized")
	return fmt.Sprintf("typed terminal blocker: %s carrier_kind=%s task_id=%s project_id=%s required_transition=%s blocker_kind=%s cause=%s safety=%s",
		contract.Schema,
		contract.CarrierKind,
		taskID,
		projectID,
		contract.RequiredTransition,
		contract.BlockerKind,
		oneLine(cause),
		contract.SafetyInvariant,
	)
}

func (r *Runtime) recordRuntimeSwitchTerminalBlockerScratch(ctx context.Context, taskID, summary string, contract runtimeSwitchTerminalBlockerContract) error {
	taskID = strings.TrimSpace(taskID)
	wakeReason := firstNonEmpty(strings.TrimSpace(contract.WakeReason), "runtime_switch_terminal_blocker")
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
		state.LastWakeReason = wakeReason
		state.LastWakeSummary = strings.TrimSpace(summary)
		state.LastWakeTaskID = taskID
		state.LastWakeSessionID = ""
		state.LastWakeAt = time.Now().UTC().Format(time.RFC3339Nano)
		state.LastSummary = strings.TrimSpace(summary)
	})
}

func (r *Runtime) postRuntimeSwitchTerminalBlockerUpdate(ctx context.Context, task WorkspaceTaskRecord, contract runtimeSwitchTerminalBlockerContract, reason string) error {
	if r == nil || r.client == nil {
		return nil
	}
	payload := map[string]any{
		"schema":              contract.Schema,
		"delegation_state":    "terminal_blocker",
		"request_kind":        firstNonEmpty(strings.TrimSpace(contract.RequestKind), agentRequestKindDelegateTask),
		"task_id":             strings.TrimSpace(task.TaskID),
		"project_id":          strings.TrimSpace(task.ProjectID),
		"carrier_kind":        strings.TrimSpace(contract.CarrierKind),
		"terminal_blocker":    true,
		"blocker_kind":        strings.TrimSpace(contract.BlockerKind),
		"required_transition": strings.TrimSpace(contract.RequiredTransition),
		"coverage_state":      firstNonEmpty(strings.TrimSpace(contract.CoverageState), "not_claimed_terminal"),
		"summary":             strings.TrimSpace(reason),
		"safety_invariant":    strings.TrimSpace(contract.SafetyInvariant),
	}
	raw, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		UpdateType:  "coordination",
		Summary:     fmt.Sprintf("Runtime switch carrier task %s published a typed terminal blocker after no fresh claimable %s materialized.", strings.TrimSpace(task.TaskID), firstNonEmpty(strings.TrimSpace(contract.SummaryPath), "runtime_switch_task path")),
		PayloadJSON: string(raw),
	})
}
