package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

type taskFrontierChoice struct {
	Decision       string `json:"decision"`
	TaskID         string `json:"task_id"`
	SelfFitSummary string `json:"self_fit_summary"`
	Reason         string `json:"reason"`
}

func isTaskFrontierWorkNext(work AgentWorkNextResult) bool {
	return work.Packet != nil &&
		work.Packet.Frontier != nil &&
		(strings.EqualFold(strings.TrimSpace(work.Reason), "task_frontier_available") ||
			strings.EqualFold(strings.TrimSpace(work.Packet.WorkType), "task_frontier_available"))
}

func (r *Runtime) selectPinnedTaskFromFrontier(ctx context.Context, work *AgentWorkNextResult, pinnedTaskID string) (bool, error) {
	pinnedTaskID = strings.TrimSpace(pinnedTaskID)
	if pinnedTaskID == "" || work == nil || work.Packet == nil || work.Packet.Frontier == nil {
		return false, nil
	}
	frontier := work.Packet.Frontier
	for _, candidate := range frontier.Candidates {
		taskID := strings.TrimSpace(candidate.Task.TaskID)
		if taskID != pinnedTaskID {
			continue
		}
		if candidate.Blocked {
			frontier.DeclineSummary = firstNonEmpty(strings.TrimSpace(candidate.BlockSummary), strings.TrimSpace(candidate.BlockReason), "pinned task is blocked in frontier")
			clearTaskFrontierRunnableWork(work, "pinned_task_blocked")
			if err := r.recordTaskFrontierDecision(ctx, *frontier, "pinned_blocked", pinnedTaskID, frontier.DeclineSummary); err != nil {
				return false, err
			}
			return false, nil
		}
		frontier.SelectedTaskID = pinnedTaskID
		frontier.SelfFitSummary = firstNonEmpty(taskFrontierCandidateFitSummary(candidate), "operator-authored pinned task selected")
		taskCopy := candidate.Task
		work.HasWork = true
		work.Task = &taskCopy
		work.Reason = "frontier_pinned_selected"
		work.ClaimAction = firstNonEmpty(candidate.ClaimAction, "claim_required")
		work.SessionAction = firstNonEmpty(candidate.SessionAction, "start_new")
		work.ProjectID = strings.TrimSpace(taskCopy.ProjectID)
		work.TaskKind = strings.TrimSpace(taskCopy.TaskKind)
		work.ProjectLane = strings.TrimSpace(taskCopy.ProjectLane)
		work.RequiresProjectGate = taskCopy.RequiresProjectGate
		applyStrategicLeadCorrectionFrontierPacket(work, taskCopy)
		if err := r.recordTaskFrontierDecision(ctx, *frontier, "selected_pinned", pinnedTaskID, frontier.SelfFitSummary); err != nil {
			return false, err
		}
		bundle, err := r.client.HydrateTask(ctx, TaskHydrationInput{
			WorkspaceID:      r.cfg.WorkspaceID,
			TaskID:           pinnedTaskID,
			DocKeys:          defaultHydrationDocKeys(pinnedTaskID),
			IncludeAllDocs:   boolPtr(false),
			UpdatesLimit:     10,
			ArtifactLimit:    10,
			RelatedTaskLimit: 10,
		})
		if err != nil {
			log.Printf("[task-frontier] pinned hydration failed for task %s: %v", pinnedTaskID, err)
			frontier.DeclineSummary = "pinned task failed hydration: " + err.Error()
			clearTaskFrontierRunnableWork(work, "pinned_task_hydration_failed")
			if recordErr := r.recordTaskFrontierDecision(ctx, *frontier, "pinned_hydration_failed", pinnedTaskID, frontier.DeclineSummary); recordErr != nil {
				return false, recordErr
			}
			return false, nil
		}
		work.Hydration = &bundle
		return true, nil
	}
	frontier.DeclineSummary = "pinned task " + pinnedTaskID + " was not present in frontier"
	clearTaskFrontierRunnableWork(work, "pinned_task_not_in_frontier")
	if err := r.recordTaskFrontierDecision(ctx, *frontier, "pinned_not_found", pinnedTaskID, frontier.DeclineSummary); err != nil {
		return false, err
	}
	return false, nil
}

func (r *Runtime) selectTaskFromFrontier(ctx context.Context, work *AgentWorkNextResult) (bool, error) {
	if work == nil || work.Packet == nil || work.Packet.Frontier == nil {
		return false, nil
	}
	frontier := work.Packet.Frontier
	candidates := frontier.Candidates
	if len(candidates) == 0 {
		frontier.DeclineSummary = "frontier contained no candidates"
		clearTaskFrontierRunnableWork(work, "task_frontier_empty")
		if err := r.recordTaskFrontierDecision(ctx, *frontier, "declined", "", frontier.DeclineSummary); err != nil {
			return false, err
		}
		return false, nil
	}
	if !r.taskFrontierHasSelectableCandidate(*frontier) && !taskFrontierHasUnblockedCandidate(candidates) {
		if handled, err := r.materializeBlockedProductFrontier(ctx, work, frontier); handled || err != nil {
			return false, err
		}
		frontier.DeclineSummary = "frontier contained no selectable candidates"
		clearTaskFrontierRunnableWork(work, "task_frontier_no_selectable_candidate")
		if err := r.recordTaskFrontierDecision(ctx, *frontier, "declined", "", frontier.DeclineSummary); err != nil {
			return false, err
		}
		return false, nil
	}

	choice, forced := r.requiredStrategicLeadCorrectionTaskFrontierChoice(*frontier)
	if forced {
		log.Printf("[task-frontier] selecting strategic-lead authority transition %s before model self-selection", choice.TaskID)
	}
	if !forced {
		if productChoice, ok := r.productPressureTaskFrontierChoice(ctx, work, *frontier); ok {
			choice = productChoice
			forced = true
			log.Printf("[task-frontier] selecting product-lane task %s under patch/review pressure before model self-selection", choice.TaskID)
		}
	}
	if !forced {
		var err error
		choice, err = r.askTaskFrontierChoice(ctx, *frontier)
		if err != nil {
			if fallback, ok := requiredOwnerBoundTaskFrontierChoice(*frontier, r.cfg.AgentID); ok {
				log.Printf("[task-frontier] model choice failed; selecting required owner-bound task %s: %v", fallback.TaskID, err)
				choice = fallback
			} else {
				log.Printf("[task-frontier] model choice failed; declining frontier instead of auto-claiming: %v", err)
				frontier.DeclineSummary = "frontier model choice failed: " + err.Error()
				clearTaskFrontierRunnableWork(work, "task_frontier_model_failed")
				if err := r.recordTaskFrontierDecision(ctx, *frontier, "model_failed", "", frontier.DeclineSummary); err != nil {
					return false, err
				}
				return false, nil
			}
		}
	}
	choice.Decision = strings.ToLower(strings.TrimSpace(choice.Decision))
	choice.TaskID = strings.TrimSpace(choice.TaskID)
	choice.SelfFitSummary = strings.TrimSpace(choice.SelfFitSummary)
	choice.Reason = strings.TrimSpace(choice.Reason)
	if choice.Decision == "" && choice.TaskID != "" {
		choice.Decision = "claim"
	}
	if choice.Decision != "claim" {
		if fallback, ok := requiredOwnerBoundTaskFrontierChoice(*frontier, r.cfg.AgentID); ok {
			log.Printf("[task-frontier] model declined frontier; selecting required owner-bound task %s", fallback.TaskID)
			choice = fallback
		} else if fallback, ok := r.taskFrontierDeclineFallbackChoice(*frontier); ok {
			log.Printf("[task-frontier] model declined frontier; selecting bounded recommended product task %s", fallback.TaskID)
			choice = fallback
		} else {
			frontier.DeclineSummary = firstNonEmpty(choice.Reason, choice.SelfFitSummary, "agent declined all frontier candidates")
			clearTaskFrontierRunnableWork(work, "task_frontier_declined")
			if err := r.recordTaskFrontierDecision(ctx, *frontier, "declined", "", frontier.DeclineSummary); err != nil {
				return false, err
			}
			return false, nil
		}
	}
	if choice.TaskID == "" {
		if fallback, ok := requiredOwnerBoundTaskFrontierChoice(*frontier, r.cfg.AgentID); ok {
			log.Printf("[task-frontier] model did not choose a task_id; selecting required owner-bound task %s", fallback.TaskID)
			choice = fallback
		} else {
			frontier.DeclineSummary = firstNonEmpty(choice.Reason, choice.SelfFitSummary, "frontier model did not choose a task_id")
			clearTaskFrontierRunnableWork(work, "task_frontier_declined")
			if err := r.recordTaskFrontierDecision(ctx, *frontier, "declined", "", frontier.DeclineSummary); err != nil {
				return false, err
			}
			return false, nil
		}
	}
	selected, ok := r.selectableTaskFrontierCandidate(candidates, frontier.Roster, choice.TaskID)
	if !ok {
		if fallback, fallbackOK := requiredOwnerBoundTaskFrontierChoice(*frontier, r.cfg.AgentID); fallbackOK {
			log.Printf("[task-frontier] model chose blocked/missing/held task; selecting required owner-bound task %s", fallback.TaskID)
			choice = fallback
			selected, ok = r.selectableTaskFrontierCandidate(candidates, frontier.Roster, choice.TaskID)
		}
		if !ok {
			if fallback, fallbackOK := r.taskFrontierDeclineFallbackChoice(*frontier); fallbackOK {
				log.Printf("[task-frontier] model chose blocked/missing/held task; selecting next bounded product task %s", fallback.TaskID)
				choice = fallback
				selected, ok = r.selectableTaskFrontierCandidate(candidates, frontier.Roster, choice.TaskID)
			}
			if !ok {
				frontier.DeclineSummary = firstNonEmpty(choice.Reason, "frontier choice was blocked, missing, or unsafe to claim")
				clearTaskFrontierRunnableWork(work, "task_frontier_declined")
				if err := r.recordTaskFrontierDecision(ctx, *frontier, "declined", "", frontier.DeclineSummary); err != nil {
					return false, err
				}
				return false, nil
			}
		}
	}

	frontier.SelectedTaskID = strings.TrimSpace(selected.Task.TaskID)
	frontier.SelfFitSummary = firstNonEmpty(choice.SelfFitSummary, choice.Reason, taskFrontierCandidateFitSummary(selected))
	taskCopy := selected.Task
	work.HasWork = true
	work.Task = &taskCopy
	work.Reason = "frontier_self_selected"
	work.ClaimAction = firstNonEmpty(selected.ClaimAction, "claim_required")
	work.SessionAction = firstNonEmpty(selected.SessionAction, "start_new")
	work.ProjectID = strings.TrimSpace(taskCopy.ProjectID)
	work.TaskKind = strings.TrimSpace(taskCopy.TaskKind)
	work.ProjectLane = strings.TrimSpace(taskCopy.ProjectLane)
	work.RequiresProjectGate = taskCopy.RequiresProjectGate
	applyStrategicLeadCorrectionFrontierPacket(work, taskCopy)
	if err := r.recordTaskFrontierDecision(ctx, *frontier, "selected", strings.TrimSpace(taskCopy.TaskID), frontier.SelfFitSummary); err != nil {
		return false, err
	}

	bundle, err := r.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID:      r.cfg.WorkspaceID,
		TaskID:           strings.TrimSpace(taskCopy.TaskID),
		DocKeys:          defaultHydrationDocKeys(taskCopy.TaskID),
		IncludeAllDocs:   boolPtr(false),
		UpdatesLimit:     10,
		ArtifactLimit:    10,
		RelatedTaskLimit: 10,
	})
	if err != nil {
		log.Printf("[task-frontier] hydration after self-selection failed for task %s: %v", strings.TrimSpace(taskCopy.TaskID), err)
		frontier.DeclineSummary = "frontier selected task failed hydration: " + err.Error()
		clearTaskFrontierRunnableWork(work, "task_frontier_hydration_failed")
		if recordErr := r.recordTaskFrontierDecision(ctx, *frontier, "hydration_failed", strings.TrimSpace(taskCopy.TaskID), frontier.DeclineSummary); recordErr != nil {
			return false, recordErr
		}
		return false, nil
	}
	work.Hydration = &bundle
	return true, nil
}

func clearTaskFrontierRunnableWork(work *AgentWorkNextResult, reason string) {
	if work == nil {
		return
	}
	work.HasWork = false
	work.Task = nil
	work.Session = nil
	work.Hydration = nil
	work.ClaimAction = ""
	work.SessionAction = ""
	work.ProjectID = ""
	work.TaskKind = ""
	work.ProjectLane = ""
	work.RequiresProjectGate = nil
	work.Reason = firstNonEmpty(strings.TrimSpace(reason), "task_frontier_declined")
}

func (r *Runtime) recordTaskFrontierDecision(ctx context.Context, frontier AgentWorkTaskFrontier, state, selectedTaskID, summary string) error {
	if r == nil || r.client == nil {
		return nil
	}
	generationID := strings.TrimSpace(frontier.GenerationID)
	workspaceID := strings.TrimSpace(r.cfg.WorkspaceID)
	agentID := strings.TrimSpace(r.cfg.AgentID)
	if generationID == "" || workspaceID == "" || agentID == "" {
		return fmt.Errorf("cannot record task frontier decision: workspace_id, agent_id, and frontier_generation_id are required")
	}
	return r.client.RecordTaskFrontierDecision(ctx, TaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: generationID,
		DecisionState:        strings.TrimSpace(state),
		SelectedTaskID:       strings.TrimSpace(selectedTaskID),
		Summary:              strings.TrimSpace(summary),
	})
}

func (r *Runtime) recordSelectedTaskFrontierFailure(ctx context.Context, work AgentWorkNextResult, task WorkspaceTaskRecord, state string, cause error) error {
	if work.Packet == nil || work.Packet.Frontier == nil {
		return nil
	}
	frontier := *work.Packet.Frontier
	taskID := strings.TrimSpace(task.TaskID)
	if strings.TrimSpace(frontier.SelectedTaskID) != taskID {
		return nil
	}
	state = firstNonEmpty(strings.TrimSpace(state), taskFrontierFailureState(cause))
	summary := "frontier selected task failed before executable session"
	if cause != nil {
		summary += ": " + strings.TrimSpace(cause.Error())
	}
	frontier.DeclineSummary = summary
	return r.recordTaskFrontierDecision(ctx, frontier, state, taskID, summary)
}

func taskFrontierFailureState(cause error) string {
	var missing *projectClaimRoleMissingError
	var gateClosed *projectClaimGateClosedError
	switch {
	case errors.As(cause, &missing), errors.As(cause, &gateClosed):
		return "admission_failed"
	case strings.Contains(strings.ToLower(strings.TrimSpace(fmt.Sprint(cause))), "project claim admission"):
		return "admission_failed"
	default:
		return "claim_failed"
	}
}

func (r *Runtime) requiredStrategicLeadCorrectionTaskFrontierChoice(frontier AgentWorkTaskFrontier) (taskFrontierChoice, bool) {
	bestIndex := -1
	bestScore := -1
	for i, candidate := range frontier.Candidates {
		if !runtimeTaskLooksStrategicLeadCorrectionFrontierTask(candidate.Task) {
			continue
		}
		if _, ok := r.selectableTaskFrontierCandidate(frontier.Candidates, frontier.Roster, candidate.Task.TaskID); !ok {
			continue
		}
		score := candidate.Fit.Score
		switch strings.ToLower(strings.TrimSpace(candidate.Fit.Level)) {
		case "recommended":
			score += 1000
		case "plausible":
			score += 500
		}
		if score > bestScore {
			bestScore = score
			bestIndex = i
		}
	}
	if bestIndex < 0 {
		return taskFrontierChoice{}, false
	}
	candidate := frontier.Candidates[bestIndex]
	return taskFrontierChoice{
		Decision:       "claim",
		TaskID:         strings.TrimSpace(candidate.Task.TaskID),
		SelfFitSummary: "strategic-lead authority transition selected before ordinary frontier self-selection: " + taskFrontierCandidateFitSummary(candidate),
		Reason:         "authority-bearing repair work blocks another project lane and must preempt root/ambient continuation",
	}, true
}

func runtimeTaskLooksStrategicLeadCorrectionFrontierTask(task WorkspaceTaskRecord) bool {
	if task.RequiresProjectGate != nil && *task.RequiresProjectGate {
		return false
	}
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if strings.ToUpper(strings.TrimSpace(task.TaskKind)) != "COORDINATION" {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane != "coordination" && !runtimeProjectLaneIsStrategy(lane) {
		return false
	}
	taskID := strings.ToLower(strings.TrimSpace(task.TaskID))
	title := strings.ToLower(strings.TrimSpace(task.Title))
	if strings.HasPrefix(taskID, "task-project-claim-repair-") && strings.Contains(title, "repair project claim") {
		return true
	}
	if strings.HasPrefix(taskID, "task-role-scope-") && runtimeTaskHasProjectRoleScopeAuthorityTransition(task) {
		return true
	}
	description := strings.ToLower(strings.TrimSpace(task.Description))
	requirements := strings.ToLower(strings.TrimSpace(task.TaskRequirementsJSON))
	text := strings.ToLower(strings.Join([]string{taskID, title, description, requirements, strings.Join(task.Tags, " ")}, "\n"))
	hasRepo := strings.Contains(text, "repo") || strings.Contains(text, "repository")
	hasLead := strings.Contains(text, "strategic lead") || strings.Contains(text, "strategic-lead") || strings.Contains(text, "active lead")
	return hasRepo && hasLead && strings.Contains(text, "repair")
}

func applyStrategicLeadCorrectionFrontierPacket(work *AgentWorkNextResult, task WorkspaceTaskRecord) {
	if work == nil || work.Packet == nil || !runtimeTaskLooksStrategicLeadCorrectionFrontierTask(task) {
		return
	}
	taskID := strings.TrimSpace(task.TaskID)
	work.Packet.ProjectID = strings.TrimSpace(task.ProjectID)
	work.Packet.TaskKind = strings.TrimSpace(task.TaskKind)
	work.Packet.ProjectLane = strings.TrimSpace(task.ProjectLane)
	work.Packet.RequiresProjectGate = task.RequiresProjectGate
	work.Packet.ContextHints.AnchorTaskIDs = uniqueNonEmptyStrings(append(work.Packet.ContextHints.AnchorTaskIDs, taskID))
	if strings.HasPrefix(taskID, "task-role-scope-") {
		work.Packet.WorkType = "project_role_scope_authority_transition"
		work.Packet.CoordinationState = "authority_transition"
		work.Packet.PreferredTransition = "project_role_assign"
		work.Packet.WhyNow = "project role/scope authority-transition task requires project_role_assign; chat/status approval is not a transition"
		work.Packet.Gate = &AgentWorkGate{
			GateState:  "open",
			GateType:   "project_role_scope_authority_transition",
			NeededFrom: "project_role_assign",
			Summary:    "project role/scope authority-transition task requires project_role_assign; chat/status approval is not a transition",
		}
		return
	}
	if strings.HasPrefix(taskID, "task-project-claim-repair-") {
		work.Packet.WorkType = "project_claim_repair_authority_transition"
		work.Packet.CoordinationState = "authority_transition"
		work.Packet.PreferredTransition = "project_claim_repair_receipt"
		work.Packet.WhyNow = "project claim repair task must produce a durable repair receipt; status, docs, or delegated validation chatter are not a transition"
		work.Packet.Gate = &AgentWorkGate{
			GateState:  "open",
			GateType:   "project_claim_repair_authority_transition",
			NeededFrom: "project_claim_repair_receipt",
			Summary:    "project claim repair task must produce durable executable follow-through via project_patch_queue_followup, product/patch task_submit, or typed denial/blocker; project_role_assign alone only repairs role/scope metadata",
		}
	}
}

func (r *Runtime) productPressureTaskFrontierChoice(ctx context.Context, work *AgentWorkNextResult, frontier AgentWorkTaskFrontier) (taskFrontierChoice, bool) {
	if !taskFrontierHasGenericCoordinationCandidate(frontier) {
		return taskFrontierChoice{}, false
	}
	bestIndex := -1
	bestScore := -1
	now := time.Now().UTC()
	for i, candidate := range frontier.Candidates {
		if !taskFrontierCandidateIsProductLane(candidate) {
			continue
		}
		if _, ok := r.selectableTaskFrontierCandidate(frontier.Candidates, frontier.Roster, candidate.Task.TaskID); !ok {
			continue
		}
		if r != nil && r.projectClaimHoldActiveForTask(candidate.Task, now) {
			continue
		}
		projectID := strings.TrimSpace(candidate.Task.ProjectID)
		if !r.taskFrontierProjectHasProductPressure(ctx, work, projectID) {
			continue
		}
		score := candidate.Fit.Score
		switch strings.ToLower(strings.TrimSpace(candidate.Fit.Level)) {
		case "recommended":
			score += 1000
		case "plausible":
			score += 500
		}
		if score > bestScore {
			bestScore = score
			bestIndex = i
		}
	}
	if bestIndex < 0 {
		return taskFrontierChoice{}, false
	}
	candidate := frontier.Candidates[bestIndex]
	return taskFrontierChoice{
		Decision:       "claim",
		TaskID:         strings.TrimSpace(candidate.Task.TaskID),
		SelfFitSummary: "product-lane pressure selected before coordination self-selection: " + taskFrontierCandidateFitSummary(candidate),
		Reason:         "project has blocked patch queue or READY_FOR_REVIEW branch pressure; pending product/revision/review/integration work must preempt fresh coordination splits",
	}, true
}

func taskFrontierHasGenericCoordinationCandidate(frontier AgentWorkTaskFrontier) bool {
	for _, candidate := range frontier.Candidates {
		if candidate.Blocked || taskSubmitTaskIsTerminal(candidate.Task) {
			continue
		}
		if taskRecordLooksGenericCoordinationSplit(candidate.Task) {
			return true
		}
	}
	return false
}

func taskFrontierHasUnblockedCandidate(candidates []AgentWorkTaskFrontierCandidate) bool {
	for _, candidate := range candidates {
		if taskFrontierCandidateLocallyUnblocked(candidate) {
			return true
		}
	}
	return false
}

func (r *Runtime) taskFrontierHasSelectableCandidate(frontier AgentWorkTaskFrontier) bool {
	for _, candidate := range frontier.Candidates {
		if _, ok := r.selectableTaskFrontierCandidate(frontier.Candidates, frontier.Roster, candidate.Task.TaskID); ok {
			return true
		}
	}
	return false
}

func (r *Runtime) materializeBlockedProductFrontier(ctx context.Context, work *AgentWorkNextResult, frontier *AgentWorkTaskFrontier) (bool, error) {
	if work == nil || frontier == nil || !taskFrontierHasProductLanePressureBlock(*frontier) {
		return false, nil
	}
	for _, candidate := range frontier.Candidates {
		workType, preferredTransition, ok := blockedProductFrontierAction(candidate)
		if !ok {
			continue
		}
		projectID := strings.TrimSpace(candidate.Task.ProjectID)
		if projectID == "" || !r.taskFrontierProjectHasProductPressure(ctx, work, projectID) {
			continue
		}
		summary := firstNonEmpty(
			strings.TrimSpace(candidate.BlockSummary),
			fmt.Sprintf("frontier product candidate %s is blocked by %s", strings.TrimSpace(candidate.Task.TaskID), strings.TrimSpace(candidate.BlockReason)),
		)
		frontier.DeclineSummary = summary
		clearTaskFrontierRunnableWork(work, workType)
		work.ProjectID = projectID
		work.TaskKind = strings.TrimSpace(candidate.Task.TaskKind)
		work.ProjectLane = strings.TrimSpace(candidate.Task.ProjectLane)
		work.RequiresProjectGate = cloneBoolPtr(candidate.Task.RequiresProjectGate)
		packet := cloneAgentWorkPacket(work.Packet)
		if packet == nil {
			packet = &AgentWorkPacket{}
		}
		packet.WorkType = workType
		packet.CoordinationState = workType
		packet.PreferredTransition = firstNonEmpty(preferredTransition, packet.PreferredTransition)
		packet.WhyNow = summary
		packet.ProjectID = projectID
		packet.TaskKind = strings.TrimSpace(candidate.Task.TaskKind)
		packet.ProjectLane = strings.TrimSpace(candidate.Task.ProjectLane)
		packet.RequiresProjectGate = cloneBoolPtr(candidate.Task.RequiresProjectGate)
		packet.ContextHints.AnchorTaskIDs = uniqueNonEmptyStrings(append(packet.ContextHints.AnchorTaskIDs, strings.TrimSpace(candidate.Task.TaskID)))
		packet.Frontier = frontier
		if workType == "project_gate_closed" {
			packet.Gate = &AgentWorkGate{
				GateState:  "closed",
				GateType:   "project_implementation_gate",
				NeededFrom: "resolve_gate",
				Summary:    summary,
			}
		}
		work.Packet = packet
		if err := r.recordTaskFrontierDecision(ctx, *frontier, "admission_failed", strings.TrimSpace(candidate.Task.TaskID), summary); err != nil {
			return true, err
		}
		return true, nil
	}
	return false, nil
}

func taskFrontierHasProductLanePressureBlock(frontier AgentWorkTaskFrontier) bool {
	for _, candidate := range frontier.Candidates {
		if strings.EqualFold(strings.TrimSpace(candidate.BlockReason), "product_lane_pressure") {
			return true
		}
	}
	return false
}

func blockedProductFrontierAction(candidate AgentWorkTaskFrontierCandidate) (string, string, bool) {
	if !candidate.Blocked || !blockedTaskFrontierCandidateIsProductLane(candidate) {
		return "", "", false
	}
	switch strings.ToLower(strings.TrimSpace(candidate.BlockReason)) {
	case "project_claim_scope_busy":
		return "project_claim_scope_busy", "project_claim_repair_receipt", true
	case "project_role_lane_required":
		return "project_role_lane_required", "project_role_assign", true
	case "profile_task_mode_mismatch":
		return "project_targeted_delegation_required", "runtime_switch_task", true
	case "project_targeted_delegation_required":
		return "project_targeted_delegation_required", "runtime_switch_task", true
	case "project_gate_closed":
		return "project_gate_closed", "resolve_gate", true
	default:
		return "", "", false
	}
}

func blockedTaskFrontierCandidateIsProductLane(candidate AgentWorkTaskFrontierCandidate) bool {
	task := candidate.Task
	if strings.TrimSpace(task.TaskID) == "" || strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if taskSubmitTaskIsTerminal(task) || authorityTransitionTaskLooksDedicated(task) {
		return false
	}
	return taskRecordIsProductLane(task)
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	return boolPtr(*value)
}

func taskFrontierCandidateIsProductLane(candidate AgentWorkTaskFrontierCandidate) bool {
	if candidate.Blocked {
		return false
	}
	task := candidate.Task
	if strings.TrimSpace(task.TaskID) == "" || strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if taskSubmitTaskIsTerminal(task) || authorityTransitionTaskLooksDedicated(task) {
		return false
	}
	return taskRecordIsProductLane(task)
}

func taskRecordIsProductLane(task WorkspaceTaskRecord) bool {
	if taskRecordIsPathlessSideEffectRecoveryCarrier(task) {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	switch lane {
	case "review", "reviewer", "qa", "test", "testing", "verification", "validation", "acceptance", "integration", "integrator", "integrate", "revision", "revise", "repair":
		return true
	}
	if strings.EqualFold(strings.TrimSpace(task.TaskKind), "EXECUTION") && runtimeProjectLaneRequiresImplementationGate(lane) {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(task.TaskKind), "REVIEW") || strings.EqualFold(strings.TrimSpace(task.TaskKind), "INTEGRATION") {
		return true
	}
	return false
}

func taskRecordIsPathlessSideEffectRecoveryCarrier(task WorkspaceTaskRecord) bool {
	record, ok := sideEffectResolutionFollowupRecordFromTask(task)
	if !ok || !strings.EqualFold(strings.TrimSpace(record.AdmissionKind), "abpc_recovery_action") {
		return false
	}
	if len(record.DirtyPaths) > 0 || len(task.WriteScopeHints) > 0 || len(projectWriteScopeHintsFromTaskRequirementsJSON(task.TaskRequirementsJSON)) > 0 {
		return false
	}
	return len(record.SideEffectRefs) > 0
}

func taskRecordIsPathlessSideEffectFoundationCarrier(task WorkspaceTaskRecord) bool {
	record, ok := sideEffectResolutionFollowupRecordFromTask(task)
	if !ok || !sideEffectResolutionPathlessFoundationCarrier(record) {
		return false
	}
	if len(task.WriteScopeHints) > 0 || len(projectWriteScopeHintsFromTaskRequirementsJSON(task.TaskRequirementsJSON)) > 0 {
		return false
	}
	return true
}

func taskRecordLooksGenericCoordinationSplit(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" || !strings.EqualFold(strings.TrimSpace(task.TaskKind), "COORDINATION") {
		return false
	}
	if authorityTransitionTaskLooksDedicated(task) {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane == "" || lane == "coordination" || lane == "strategy" || lane == "planning" || lane == "plan" || lane == "spec" || lane == "design" || lane == "discovery" || lane == "reflection" {
		return true
	}
	text := strings.ToLower(strings.Join([]string{task.TaskID, task.Title, task.Description, strings.Join(task.Tags, " "), task.TaskRequirementsJSON}, "\n"))
	return containsAnySignal(text, []string{"coordinate", "coordination", "split", "fanout", "role scope", "scope request", "handoff"})
}

func (r *Runtime) taskFrontierProjectHasProductPressure(ctx context.Context, work *AgentWorkNextResult, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return false
	}
	for _, raw := range taskFrontierProjectCoordinationRawMessages(work) {
		if coordination, ok := projectCoordinationFromRaw(raw); ok && projectCoordinationMatchesProject(coordination, projectID) {
			return projectCoordinationHasProductLanePressure(coordination)
		}
	}
	if r == nil || r.client == nil || strings.TrimSpace(r.cfg.WorkspaceID) == "" {
		return false
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, projectID)
	if err != nil {
		log.Printf("[task-frontier] product-lane pressure snapshot skipped for project %s: %v", projectID, err)
		return false
	}
	return projectCoordinationHasProductLanePressure(coordination)
}

func taskFrontierProjectCoordinationRawMessages(work *AgentWorkNextResult) []json.RawMessage {
	if work == nil {
		return nil
	}
	out := make([]json.RawMessage, 0, 2)
	if len(work.ProjectCoordination) > 0 {
		out = append(out, work.ProjectCoordination)
	}
	if work.Packet != nil && len(work.Packet.ProjectCoordination) > 0 {
		out = append(out, work.Packet.ProjectCoordination)
	}
	return out
}

func projectCoordinationFromRaw(raw json.RawMessage) (ProjectCoordinationRecord, bool) {
	if len(raw) == 0 {
		return ProjectCoordinationRecord{}, false
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(raw, &coordination); err == nil && strings.TrimSpace(coordination.Project.ProjectID) != "" {
		return coordination, true
	}
	var envelope struct {
		Coordination ProjectCoordinationRecord `json:"coordination"`
	}
	if err := json.Unmarshal(raw, &envelope); err == nil && strings.TrimSpace(envelope.Coordination.Project.ProjectID) != "" {
		return envelope.Coordination, true
	}
	return ProjectCoordinationRecord{}, false
}

func projectCoordinationMatchesProject(coordination ProjectCoordinationRecord, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(coordination.Project.ProjectID), projectID) ||
		strings.EqualFold(strings.TrimSpace(coordination.Profile.ProjectID), projectID)
}

func projectCoordinationHasProductLanePressure(coordination ProjectCoordinationRecord) bool {
	for _, item := range coordination.PatchQueueItems {
		if strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
			return true
		}
	}
	for _, branch := range coordination.Branches {
		if strings.EqualFold(strings.TrimSpace(branch.Status), "READY_FOR_REVIEW") {
			return true
		}
	}
	return false
}

func requiredOwnerBoundTaskFrontierChoice(frontier AgentWorkTaskFrontier, agentID string) (taskFrontierChoice, bool) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return taskFrontierChoice{}, false
	}
	bestIndex := -1
	bestScore := -1
	for i, candidate := range frontier.Candidates {
		if candidate.Blocked || !taskFrontierCandidateRequiredForAgent(candidate, agentID) {
			continue
		}
		score := candidate.Fit.Score
		switch strings.ToLower(strings.TrimSpace(candidate.Fit.Level)) {
		case "recommended":
			score += 1000
		case "plausible":
			score += 500
		}
		if score > bestScore {
			bestScore = score
			bestIndex = i
		}
	}
	if bestIndex < 0 {
		return taskFrontierChoice{}, false
	}
	candidate := frontier.Candidates[bestIndex]
	return taskFrontierChoice{
		Decision:       "claim",
		TaskID:         strings.TrimSpace(candidate.Task.TaskID),
		SelfFitSummary: "required owner-bound frontier selected for branch-owner follow-up: " + taskFrontierCandidateFitSummary(candidate),
		Reason:         "candidate is explicitly owner-bound to this agent",
	}, true
}

func (r *Runtime) taskFrontierDeclineFallbackChoice(frontier AgentWorkTaskFrontier) (taskFrontierChoice, bool) {
	bestIndex := -1
	bestScore := -1
	now := time.Now().UTC()
	for i, candidate := range frontier.Candidates {
		if !taskFrontierCandidateAllowsDeclineFallback(candidate) {
			continue
		}
		if r != nil && r.projectClaimHoldActiveForTask(candidate.Task, now) {
			continue
		}
		score := candidate.Fit.Score
		switch strings.ToLower(strings.TrimSpace(candidate.Fit.Level)) {
		case "recommended":
			score += 1000
		case "plausible":
			score += 500
		}
		if score > bestScore {
			bestScore = score
			bestIndex = i
		}
	}
	if bestIndex < 0 {
		return taskFrontierChoice{}, false
	}
	candidate := frontier.Candidates[bestIndex]
	return taskFrontierChoice{
		Decision:       "claim",
		TaskID:         strings.TrimSpace(candidate.Task.TaskID),
		SelfFitSummary: "frontier decline fallback selected bounded product candidate: " + taskFrontierCandidateFitSummary(candidate),
		Reason:         "bounded recommended product candidate remained unclaimed after model decline",
	}, true
}

func taskFrontierCandidateAllowsDeclineFallback(candidate AgentWorkTaskFrontierCandidate) bool {
	if candidate.Blocked {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(candidate.Fit.Level)) {
	case "recommended", "plausible":
	default:
		return false
	}
	task := candidate.Task
	if strings.TrimSpace(task.TaskID) == "" || strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.ProjectLane), "implementation") && !runtimeTaskShouldPrepareProjectClaimAdmission(task) {
		return false
	}
	if scope := projectWriteScopeJSONFromPaths(projectTaskWriteScopeHintsForAdmission(task, task.WriteScopeHints)); scope != "" {
		return true
	}
	return projectWriteScopeJSONFromPaths(projectTaskWriteScopeHintsForAdmission(task, projectWriteScopeHintsFromTaskRequirementsJSON(task.TaskRequirementsJSON))) != ""
}

func taskFrontierCandidateRequiredForAgent(candidate AgentWorkTaskFrontierCandidate, agentID string) bool {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false
	}
	const requiredAgentPrefix = "required-agent:"
	hasOwnerBoundSignal := false
	requiredAgentID := ""
	for _, tag := range candidate.Task.Tags {
		tag = strings.TrimSpace(tag)
		lower := strings.ToLower(tag)
		switch {
		case lower == "owner-bound" || strings.HasPrefix(lower, "owner-bound-kind:"):
			hasOwnerBoundSignal = true
		case strings.HasPrefix(lower, requiredAgentPrefix):
			requiredAgentID = strings.TrimSpace(tag[len(requiredAgentPrefix):])
		}
	}
	if hasOwnerBoundSignal && requiredAgentID != "" && strings.EqualFold(strings.TrimSpace(requiredAgentID), agentID) {
		return true
	}
	return taskFrontierCandidateReleasedForAgent(candidate.Task, agentID)
}

func taskFrontierCandidateReleasedForAgent(task WorkspaceTaskRecord, agentID string) bool {
	if strings.TrimSpace(agentID) == "" || !isClaimOwnedBy(task, agentID) {
		return false
	}
	if taskClaimStatus(task) != "RELEASED" || taskSubmitTaskIsTerminal(task) {
		return false
	}
	return strings.TrimSpace(task.TaskID) != ""
}

func (r *Runtime) askTaskFrontierChoice(ctx context.Context, frontier AgentWorkTaskFrontier) (taskFrontierChoice, error) {
	if r == nil || r.agent == nil || r.agent.LLM == nil {
		return taskFrontierChoice{}, fmt.Errorf("runtime LLM is unavailable")
	}
	prompt := renderTaskFrontierChoicePrompt(frontier)
	resp, err := r.agent.LLM.Chat(ctx, []Message{
		{
			Role: "system",
			Content: strings.Join([]string{
				"You are the agent's local work-selection loop.",
				"Choose a task only when it genuinely fits your role, tools, and current workload.",
				"Never choose a blocked candidate.",
				"Return only JSON.",
			}, "\n"),
		},
		{Role: "user", Content: prompt},
	}, nil)
	if err != nil {
		return taskFrontierChoice{}, err
	}
	if resp == nil {
		return taskFrontierChoice{}, fmt.Errorf("empty LLM response")
	}
	choice, err := parseTaskFrontierChoice(resp.Content)
	if err != nil {
		return taskFrontierChoice{}, err
	}
	return choice, nil
}

func renderTaskFrontierChoicePrompt(frontier AgentWorkTaskFrontier) string {
	var b strings.Builder
	b.WriteString("Task frontier generation_id: " + firstNonEmpty(frontier.GenerationID, "-") + "\n")
	if frontier.Summary != "" {
		b.WriteString("Summary: " + oneLine(frontier.Summary) + "\n")
	}
	b.WriteString("\nCandidates:\n")
	for i, candidate := range frontier.Candidates {
		if i >= 8 {
			break
		}
		block := "no"
		if candidate.Blocked {
			block = firstNonEmpty(candidate.BlockReason, "yes")
		}
		b.WriteString(fmt.Sprintf("- task_id=%s | title=%s | fit=%s/%d | blocked=%s | modes=%s | skills=%s | tools=%s | reasons=%s\n",
			firstNonEmpty(candidate.Task.TaskID, "-"),
			truncate(oneLine(candidate.Task.Title), 90),
			firstNonEmpty(candidate.Fit.Level, "-"),
			candidate.Fit.Score,
			block,
			joinOrDash(candidate.Fit.RequiredWorkModes, 4),
			joinOrDash(candidate.Fit.PreferredSkills, 6),
			joinOrDash(candidate.Fit.PreferredTools, 6),
			truncate(oneLine(strings.Join(candidate.Fit.Reasons, "; ")), 240),
		))
		if candidate.Task.Description != "" {
			b.WriteString("  description: " + truncate(oneLine(candidate.Task.Description), 220) + "\n")
		}
		if candidate.BlockSummary != "" {
			b.WriteString("  block_summary: " + truncate(oneLine(candidate.BlockSummary), 180) + "\n")
		}
	}
	if len(frontier.Roster) > 0 {
		b.WriteString("\nVisible agents:\n")
		for i, agent := range frontier.Roster {
			if i >= 10 {
				break
			}
			b.WriteString(fmt.Sprintf("- agent_id=%s | role=%s | busyness=%s | active_tasks=%d | tools=%s | tags=%s\n",
				firstNonEmpty(agent.AgentID, "-"),
				firstNonEmpty(agent.Role, agent.ProfileSpecialization, "-"),
				firstNonEmpty(agent.Busyness, "-"),
				agent.ActiveTaskCount,
				joinOrDash(agent.ToolsAccess, 6),
				joinOrDash(agent.ProfileTags, 6),
			))
		}
	}
	b.WriteString("\nReturn JSON exactly like one of:\n")
	b.WriteString(`{"decision":"claim","task_id":"task-id","self_fit_summary":"why this is mine now","reason":"short evidence"}` + "\n")
	b.WriteString(`{"decision":"decline","task_id":"","self_fit_summary":"","reason":"why no candidate fits"}` + "\n")
	return b.String()
}

func parseTaskFrontierChoice(raw string) (taskFrontierChoice, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return taskFrontierChoice{}, fmt.Errorf("empty task frontier choice")
	}
	var choice taskFrontierChoice
	if err := json.Unmarshal([]byte(raw), &choice); err == nil {
		return choice, nil
	}
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start >= 0 && end > start {
		if err := json.Unmarshal([]byte(raw[start:end+1]), &choice); err == nil {
			return choice, nil
		}
	}
	return taskFrontierChoice{}, fmt.Errorf("task frontier choice was not valid JSON")
}

func unblockedTaskFrontierCandidate(candidates []AgentWorkTaskFrontierCandidate, taskID string) (AgentWorkTaskFrontierCandidate, bool) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return AgentWorkTaskFrontierCandidate{}, false
	}
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.Task.TaskID) == taskID && taskFrontierCandidateLocallyUnblocked(candidate) {
			return candidate, true
		}
	}
	return AgentWorkTaskFrontierCandidate{}, false
}

func taskFrontierCandidateLocallyUnblocked(candidate AgentWorkTaskFrontierCandidate) bool {
	if candidate.Blocked || strings.TrimSpace(candidate.Task.TaskID) == "" {
		return false
	}
	return !taskRecordIsPathlessSideEffectRecoveryCarrier(candidate.Task)
}

func (r *Runtime) selectableTaskFrontierCandidate(candidates []AgentWorkTaskFrontierCandidate, roster []AgentWorkRosterAgent, taskID string) (AgentWorkTaskFrontierCandidate, bool) {
	candidate, ok := unblockedTaskFrontierCandidate(candidates, taskID)
	if !ok {
		return AgentWorkTaskFrontierCandidate{}, false
	}
	if r != nil && r.projectClaimHoldActiveForTask(candidate.Task, time.Now().UTC()) {
		return AgentWorkTaskFrontierCandidate{}, false
	}
	if r != nil && taskFrontierCandidateHardAssignedToDifferentAgent(candidate, r.cfg.AgentID, roster) {
		return AgentWorkTaskFrontierCandidate{}, false
	}
	return candidate, true
}

func taskFrontierCandidateHardAssignedToDifferentAgent(candidate AgentWorkTaskFrontierCandidate, agentID string, roster []AgentWorkRosterAgent) bool {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false
	}
	task := candidate.Task
	ownerID := strings.TrimSpace(task.OwnerUserID)
	if ownerID == "" || ownerID == "system" || ownerID == agentID {
		return false
	}
	if !taskFrontierRosterContainsAgent(roster, ownerID) {
		return false
	}
	if !taskFrontierTaskOwnerUserIDIsHardClaimRoute(task) {
		return false
	}
	if taskFrontierRequiredAgentMatches(task, agentID) {
		return false
	}
	return true
}

func taskFrontierTaskOwnerUserIDIsHardClaimRoute(task WorkspaceTaskRecord) bool {
	if !taskFrontierPatchQueueDecisionContinuationTask(task) {
		return false
	}
	if taskFrontierOwnerRouteIsRoleRoutedPatchQueueIntegration(task) {
		return false
	}
	return true
}

func taskFrontierPatchQueueDecisionContinuationTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	kind := strings.ToLower(strings.TrimSpace(taskFrontierRequirementString(task, "patch_queue_task_kind")))
	switch kind {
	case "integration", "revision", "validation", "rebuild":
		return true
	default:
		return taskFrontierTaskHasTag(task, "patch_queue", "patch-queue") && taskFrontierTaskHasTag(task, "decision_continuation")
	}
}

func taskFrontierOwnerRouteIsRoleRoutedPatchQueueIntegration(task WorkspaceTaskRecord) bool {
	if !strings.EqualFold(taskFrontierRequirementString(task, "patch_queue_task_kind"), "integration") {
		return false
	}
	return strings.EqualFold(taskFrontierRequirementString(task, "required_project_role"), "INTEGRATOR") &&
		strings.EqualFold(taskFrontierRequirementString(task, "required_tool"), "project_patch_queue_integrate")
}

func taskFrontierRequiredAgentMatches(task WorkspaceTaskRecord, agentID string) bool {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false
	}
	for _, tag := range task.Tags {
		tag = strings.TrimSpace(tag)
		lower := strings.ToLower(tag)
		for _, prefix := range []string{"required-agent:", "required-agent=", "required_agent:", "required_agent=", "owner-agent:", "owner-agent=", "owner_agent:", "owner_agent="} {
			if strings.HasPrefix(lower, prefix) && strings.EqualFold(strings.TrimSpace(tag[len(prefix):]), agentID) {
				return true
			}
		}
	}
	return false
}

func taskFrontierRosterContainsAgent(roster []AgentWorkRosterAgent, agentID string) bool {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false
	}
	for _, agent := range roster {
		if strings.EqualFold(strings.TrimSpace(agent.AgentID), agentID) {
			return true
		}
	}
	return false
}

func taskFrontierTaskHasTag(task WorkspaceTaskRecord, tags ...string) bool {
	for _, rawTag := range task.Tags {
		normalized := strings.ToLower(strings.TrimSpace(rawTag))
		for _, tag := range tags {
			if normalized == strings.ToLower(strings.TrimSpace(tag)) {
				return true
			}
		}
	}
	return false
}

func taskFrontierRequirementString(task WorkspaceTaskRecord, key string) string {
	key = strings.TrimSpace(key)
	if key == "" || strings.TrimSpace(task.TaskRequirementsJSON) == "" {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(task.TaskRequirementsJSON)), &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(stringMapValue(payload, key))
}

func fallbackTaskFrontierChoice(frontier AgentWorkTaskFrontier) taskFrontierChoice {
	bestIndex := -1
	bestScore := -1
	for i, candidate := range frontier.Candidates {
		if candidate.Blocked {
			continue
		}
		score := candidate.Fit.Score
		switch strings.ToLower(strings.TrimSpace(candidate.Fit.Level)) {
		case "recommended":
			score += 1000
		case "plausible":
			score += 500
		}
		if score > bestScore {
			bestScore = score
			bestIndex = i
		}
	}
	if bestIndex < 0 {
		return taskFrontierChoice{Decision: "decline", Reason: "all frontier candidates are blocked or unsafe to claim"}
	}
	candidate := frontier.Candidates[bestIndex]
	return taskFrontierChoice{
		Decision:       "claim",
		TaskID:         strings.TrimSpace(candidate.Task.TaskID),
		SelfFitSummary: "frontier fallback selected highest-fit unblocked candidate: " + taskFrontierCandidateFitSummary(candidate),
		Reason:         "highest-fit unblocked frontier candidate",
	}
}

func taskFrontierCandidateFitSummary(candidate AgentWorkTaskFrontierCandidate) string {
	parts := []string{
		fmt.Sprintf("fit=%s/%d", firstNonEmpty(candidate.Fit.Level, "unknown"), candidate.Fit.Score),
		"title=" + firstNonEmpty(candidate.Task.Title, candidate.Task.TaskID),
	}
	if len(candidate.Fit.Reasons) > 0 {
		parts = append(parts, "reasons="+strings.Join(candidate.Fit.Reasons, "; "))
	}
	return truncate(oneLine(strings.Join(parts, " | ")), 500)
}
