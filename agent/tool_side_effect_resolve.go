package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type SideEffectResolveTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
	ownerUserID string
}

func NewSideEffectResolveTool(client *RhizomeClient, workspaceID, agentID, ownerUserID string) *SideEffectResolveTool {
	return &SideEffectResolveTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		ownerUserID: strings.TrimSpace(ownerUserID),
	}
}

func (t *SideEffectResolveTool) Name() string { return "side_effect_resolve" }

func (t *SideEffectResolveTool) Description() string {
	return "Record an executable Artifact-Bound Process Control decision for pending side effects, then route follow-up work such as split_tension, expand_boundary, reassign, request_verification, quarantine, or revert."
}

func (t *SideEffectResolveTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{
				"type":        "string",
				"description": "Project id for the affected artifact, if any.",
			},
			"decision": map[string]any{
				"type":        "string",
				"description": "Decision: accept, quarantine, revert, split_tension, expand_boundary, reassign, request_verification, or reroute_to_active_lane.",
			},
			"side_effect_refs": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Side-effect refs being classified.",
			},
			"justification": map[string]any{
				"type":        "string",
				"description": "Why this decision is valid. Required; this is the authority/evidence trail.",
			},
			"owner_agent_id": map[string]any{
				"type":        "string",
				"description": "Current lane owner for the side effect, if known.",
			},
			"target_agent_id": map[string]any{
				"type":        "string",
				"description": "Target owner for reassign/expand decisions. Defaults to owner_agent_id or this agent.",
			},
			"active_task_id": map[string]any{
				"type":        "string",
				"description": "Original active task/lane id that produced the side effect.",
			},
			"branch_id": map[string]any{
				"type":        "string",
				"description": "Adapter branch id, if the source artifact is a git branch.",
			},
			"branch_name": map[string]any{
				"type":        "string",
				"description": "Adapter branch name, if known.",
			},
			"dirty_paths": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Materialized regions/paths needing resolution.",
			},
			"current_write_scope_json": map[string]any{
				"type":        "string",
				"description": "Current operational boundary JSON, for accept/expand decisions.",
			},
			"expanded_write_scope_json": map[string]any{
				"type":        "string",
				"description": "Explicit expanded boundary JSON. If absent, accept/expand derive one from current_write_scope_json + dirty_paths.",
			},
			"classification_task_id": map[string]any{
				"type":        "string",
				"description": "Classification task id being resolved, if this decision came from a side-effect classification lane.",
			},
		},
		"required": []string{"decision", "side_effect_refs", "justification"},
	}
}

func (t *SideEffectResolveTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "side_effect_resolve is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	projectID := strings.TrimSpace(stringArg(args, "project_id"))
	decision := normalizeSideEffectResolveDecision(stringArg(args, "decision"))
	if decision == "" {
		return &ToolResult{Output: "decision must be one of accept, quarantine, revert, split_tension, expand_boundary, reassign, request_verification, or reroute_to_active_lane", IsError: true}
	}
	justification := strings.TrimSpace(stringArg(args, "justification"))
	if justification == "" {
		return &ToolResult{Output: "justification is required so the boundary transition is auditable", IsError: true}
	}
	refs := uniqueNonEmptyStrings(firstNonEmptyStringSlice(
		stringSliceArg(args, "side_effect_refs"),
		stringSliceArg(args, "side_effect_ref"),
	))
	if len(refs) == 0 {
		return &ToolResult{Output: "side_effect_refs is required", IsError: true}
	}
	sort.Strings(refs)

	input := sideEffectResolveInput{
		ProjectID:              projectID,
		Decision:               decision,
		SideEffectRefs:         refs,
		Justification:          justification,
		OwnerAgentID:           strings.TrimSpace(stringArg(args, "owner_agent_id")),
		TargetAgentID:          strings.TrimSpace(stringArg(args, "target_agent_id")),
		ActiveTaskID:           strings.TrimSpace(stringArg(args, "active_task_id")),
		BranchID:               strings.TrimSpace(stringArg(args, "branch_id")),
		BranchName:             strings.TrimSpace(stringArg(args, "branch_name")),
		DirtyPaths:             normalizeProjectWriteScopePaths(firstNonEmptyStringSlice(stringSliceArg(args, "dirty_paths"), stringSliceArg(args, "regions"))),
		CurrentWriteScopeJSON:  strings.TrimSpace(stringArg(args, "current_write_scope_json")),
		ExpandedWriteScopeJSON: strings.TrimSpace(stringArg(args, "expanded_write_scope_json")),
		ClassificationTaskID:   strings.TrimSpace(stringArg(args, "classification_task_id")),
	}
	if input.TargetAgentID == "" {
		input.TargetAgentID = firstNonEmpty(input.OwnerAgentID, t.agentID)
	}

	output := map[string]any{
		"status":                   "decision_pending_transition",
		"decision":                 input.Decision,
		"integration_status":       "pending_classification",
		"side_effect_refs":         input.SideEffectRefs,
		"project_id":               input.ProjectID,
		"branch_id":                input.BranchID,
		"branch_name":              input.BranchName,
		"active_task_id":           input.ActiveTaskID,
		"classification_task_id":   input.ClassificationTaskID,
		"next_transition":          sideEffectResolveNextTransition(input.Decision),
		"decision_record_deferred": true,
	}

	transitionOutput, transitionErr := t.executeSideEffectResolutionTransition(ctx, input)
	for key, value := range transitionOutput {
		output[key] = value
	}
	if transitionErr != nil {
		output["status"] = "transition_blocked_decision_not_recorded"
		output["transition_error"] = transitionErr.Error()
		raw, _ := json.MarshalIndent(output, "", "  ")
		return &ToolResult{Output: string(raw), IsError: true}
	}

	decisionUpdateErr := t.postSideEffectResolutionUpdate(ctx, input, transitionOutput)
	output["status"] = "decision_recorded"
	output["integration_status"] = sideEffectResolveIntegrationStatus(input.Decision)
	output["decision_record_deferred"] = false
	if decisionUpdateErr != nil {
		output["status"] = "decision_record_failed"
		output["error"] = decisionUpdateErr.Error()
		raw, _ := json.MarshalIndent(output, "", "  ")
		return &ToolResult{Output: string(raw), IsError: true}
	}
	raw, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("side_effect_resolve recorded %s for %d side effect(s)", input.Decision, len(input.SideEffectRefs))}
	}
	return &ToolResult{Output: string(raw)}
}

type sideEffectResolveInput struct {
	ProjectID              string
	Decision               string
	SideEffectRefs         []string
	Justification          string
	OwnerAgentID           string
	TargetAgentID          string
	ActiveTaskID           string
	BranchID               string
	BranchName             string
	DirtyPaths             []string
	CurrentWriteScopeJSON  string
	ExpandedWriteScopeJSON string
	ClassificationTaskID   string
}

func (t *SideEffectResolveTool) postSideEffectResolutionUpdate(ctx context.Context, input sideEffectResolveInput, transitionOutput map[string]any) error {
	resolutionStatus := "resolved"
	if transitionState := strings.TrimSpace(stringMapValue(transitionOutput, "boundary_transition_state")); transitionState != "" {
		if executed, ok := transitionOutput["transition_executed"].(bool); ok && !executed {
			resolutionStatus = transitionState
		}
	}
	payload := map[string]any{
		"schema":                 "artifact_bound_side_effect_resolution.v1",
		"status":                 resolutionStatus,
		"next_action":            sideEffectResolveNextTransition(input.Decision),
		"project_id":             input.ProjectID,
		"branch_id":              input.BranchID,
		"branch_name":            input.BranchName,
		"active_task_id":         input.ActiveTaskID,
		"classification_task_id": input.ClassificationTaskID,
		"decision":               input.Decision,
		"integration_status":     sideEffectResolveIntegrationStatus(input.Decision),
		"justification":          input.Justification,
		"side_effect_refs":       input.SideEffectRefs,
		"side_effects":           sideEffectResolveDecisionEffects(t.workspaceID, t.agentID, input),
	}
	for _, key := range []string{
		"boundary_transition_state",
		"boundary_transition_key",
		"existing_task_id",
		"followup_task_id",
		"followup_task_class",
		"followup_core_task_class",
		"followup_task_lane",
		"followup_action_kind",
		"followup_successor_key",
		"followup_created",
		"followup_warning",
		"lead_task_id",
		"transition_executed",
		"transition_already_pending",
		"transition_denied",
		"denial_reason",
		"preferred_transition",
		"allowed_next",
		"conflicting_task_id",
		"conflicting_owner_agent_id",
		"conflicting_branch_id",
		"conflicting_active_task_id",
		"active_claim_rebind",
		"do_not_retry",
		"parent_classification_state",
		"parent_classification_task_id",
		"parent_classification_blocked",
		"parent_classification_resolved_split",
		"parent_classification_warning",
		"parent_classification_close_warning",
		"successor_key",
		"resolution_saga_key",
		"active_recovery_successor_reused",
		"active_recovery_successor_previous_action_kind",
		"self_recursive_recovery_collapsed",
		"cross_decision_recovery_pivot_collapsed",
		"superseded_followup_task_ids",
		"successor_supersede_warnings",
		"successor_reconcile_warning",
		"terminal_blocker_type",
		"terminal_blocker_summary",
		"typed_terminal_blocker",
		"pathless_carrier_suppressed",
	} {
		if value, ok := transitionOutput[key]; ok {
			payload[key] = value
		}
	}
	if len(input.DirtyPaths) > 0 {
		payload["dirty_paths"] = input.DirtyPaths
	}
	if input.OwnerAgentID != "" {
		payload["owner_agent_id"] = input.OwnerAgentID
	}
	if input.TargetAgentID != "" {
		payload["target_agent_id"] = input.TargetAgentID
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode side-effect resolution payload: %w", err)
	}
	return t.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID:   t.workspaceID,
		AgentID:       t.agentID,
		UpdateType:    "side_effect_resolution",
		Summary:       fmt.Sprintf("side_effect_resolve %s for %s", input.Decision, strings.Join(input.SideEffectRefs, ", ")),
		PayloadJSON:   string(raw),
		RequiresHuman: false,
	})
}

func (t *SideEffectResolveTool) executeSideEffectResolutionTransition(ctx context.Context, input sideEffectResolveInput) (map[string]any, error) {
	switch input.Decision {
	case "accept", "expand_boundary":
		return t.executeSideEffectBoundaryExpansion(ctx, input)
	case "split_tension":
		return t.createSideEffectResolutionTask(ctx, input, sideEffectResolutionTaskSpec{
			ABPCTaskClass: "side_effect_foundation",
			CoreTaskClass: "INTEGRATION",
			ProjectLane:   "implementation",
			TaskKind:      "EXECUTION",
			Priority:      "high",
			TitlePrefix:   "Resolve foundation side effect",
			Tags:          []string{"side-effect-resolution", "foundation-effect", "operational-boundary", "abpc"},
		})
	case "request_verification":
		return t.createSideEffectResolutionTask(ctx, input, sideEffectResolutionTaskSpec{
			ABPCTaskClass: "side_effect_verification",
			CoreTaskClass: "PROOF",
			ProjectLane:   "verification",
			TaskKind:      "EXECUTION",
			Priority:      "high",
			TitlePrefix:   "Verify side effect classification",
			Tags:          []string{"side-effect-resolution", "verification", "review", "abpc"},
		})
	case "reassign":
		return t.createSideEffectResolutionTask(ctx, input, sideEffectResolutionTaskSpec{
			ABPCTaskClass: "side_effect_reassignment",
			CoreTaskClass: "INCIDENT",
			ProjectLane:   "coordination",
			TaskKind:      "COORDINATION",
			Priority:      "high",
			TitlePrefix:   "Reassign side effect ownership",
			Tags:          []string{"side-effect-resolution", "reassign", "coordination", "abpc"},
		})
	case "quarantine", "revert":
		return t.createSideEffectResolutionTask(ctx, input, sideEffectResolutionTaskSpec{
			ABPCTaskClass: "side_effect_cleanup",
			CoreTaskClass: "INCIDENT",
			ProjectLane:   "coordination",
			TaskKind:      "COORDINATION",
			Priority:      "high",
			TitlePrefix:   "Execute side effect " + input.Decision,
			Tags:          []string{"side-effect-resolution", input.Decision, "cleanup", "abpc"},
		})
	case "reroute_to_active_lane":
		return map[string]any{
			"transition_executed": true,
			"routed_to":           firstNonEmpty(input.ActiveTaskID, input.OwnerAgentID, "active_lane"),
			"guidance":            "Existing active lane should resume/status/assist rather than creating a duplicate claim.",
		}, nil
	default:
		return nil, fmt.Errorf("unsupported side-effect decision %q", input.Decision)
	}
}

func (t *SideEffectResolveTool) executeSideEffectBoundaryExpansion(ctx context.Context, input sideEffectResolveInput) (map[string]any, error) {
	if input.ProjectID == "" {
		return nil, fmt.Errorf("%s requires project_id", input.Decision)
	}
	expandedScope := strings.TrimSpace(input.ExpandedWriteScopeJSON)
	if expandedScope == "" {
		var err error
		expandedScope, err = sideEffectResolveExpandedWriteScope(input.CurrentWriteScopeJSON, input.DirtyPaths)
		if err != nil {
			return nil, err
		}
	}
	if !json.Valid([]byte(expandedScope)) {
		return nil, fmt.Errorf("expanded_write_scope_json must be valid JSON")
	}
	target := firstNonEmpty(input.TargetAgentID, input.OwnerAgentID, t.agentID)
	roleResult := NewProjectRoleAssignTool(t.client, t.workspaceID, t.agentID).Execute(ctx, map[string]any{
		"project_id":              input.ProjectID,
		"agent_id":                target,
		"role_type":               "IMPLEMENTER",
		"write_scope_json":        expandedScope,
		"summary":                 "ABPC side-effect resolution " + input.Decision + ": " + input.Justification,
		"boundary_transition_key": sideEffectBoundaryTransitionKey(t.workspaceID, input),
		"side_effect_refs":        input.SideEffectRefs,
		"active_task_id":          input.ActiveTaskID,
		"branch_id":               input.BranchID,
		"branch_name":             input.BranchName,
		"suppress_denial_update":  true,
	})
	output := map[string]any{
		"transition_executed":       roleResult != nil && !roleResult.IsError,
		"boundary_transition":       input.Decision,
		"target_agent_id":           target,
		"expanded_write_scope_json": expandedScope,
	}
	if roleResult != nil {
		output["project_role_assign_output"] = roleResult.Output
		rolePayload, _ := sideEffectResolveMapFromJSON(roleResult.Output)
		sideEffectResolveCopyRoleTransitionFields(output, rolePayload)
		if roleResult.IsError {
			if strings.EqualFold(strings.TrimSpace(stringMapValue(rolePayload, "boundary_transition_state")), "boundary_expansion_denied_overlap") ||
				sideEffectResolveBoolField(rolePayload, "transition_denied") {
				output["transition_executed"] = false
				if _, ok := output["boundary_transition_state"]; !ok {
					output["boundary_transition_state"] = "boundary_expansion_denied_overlap"
				}
				output["do_not_retry"] = true
				return output, nil
			}
			return output, fmt.Errorf("boundary expansion role/scope transition failed")
		}
		executed, state, err := sideEffectBoundaryExpansionRoleResultExecuted(input, roleResult.Output)
		output["boundary_transition_state"] = state
		if err != nil {
			output["transition_executed"] = false
			return output, err
		}
		output["transition_executed"] = executed
	}
	return output, nil
}

func sideEffectBoundaryExpansionRoleResultExecuted(input sideEffectResolveInput, raw string) (bool, string, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &payload); err != nil {
		return false, "unstructured_role_assignment_result", fmt.Errorf("boundary expansion role/scope transition returned unstructured evidence: %w", err)
	}
	if sideEffectResolveBoolField(payload, "authority_transition_applied") ||
		strings.EqualFold(strings.TrimSpace(stringMapValue(payload, "boundary_transition_state")), "authority_transition_applied") {
		return true, "authority_transition_applied", nil
	}
	if sideEffectResolveBoolField(payload, "already_satisfied") {
		if sideEffectBoundaryExpansionRequiresBranchRebind(input) {
			rebind, _ := payload["active_claim_rebind"].(map[string]any)
			switch strings.ToLower(strings.TrimSpace(stringMapValue(rebind, "state"))) {
			case "updated", "already_applied":
				return true, "role_scope_already_satisfied", nil
			default:
				return false, "branch_claim_rebind_missing", fmt.Errorf("boundary expansion role already exists, but branch-bound side effects require active claim/branch rebind evidence")
			}
		}
		return true, "role_scope_already_satisfied", nil
	}
	if stringMapValue(payload, "lead_task_id") != "" || stringMapValue(payload, "lead_request_id") != "" || sideEffectResolveBoolField(payload, "do_not_retry") {
		if sideEffectResolveBoolField(payload, "transition_already_pending") {
			return false, "authority_transition_already_pending", nil
		}
		return false, "lead_scope_request_pending", fmt.Errorf("boundary expansion was routed to strategic lead role/scope repair, not executed durably")
	}
	if stringMapValue(payload, "role_id") == "" {
		return false, "missing_role_assignment_evidence", fmt.Errorf("boundary expansion did not return durable project role evidence")
	}
	if sideEffectBoundaryExpansionRequiresBranchRebind(input) {
		rebind, _ := payload["active_claim_rebind"].(map[string]any)
		if strings.ToLower(strings.TrimSpace(stringMapValue(rebind, "state"))) != "updated" {
			return false, "branch_claim_rebind_missing", fmt.Errorf("boundary expansion did not update the active claim/branch boundary for the affected lane")
		}
	}
	return true, "role_scope_assigned", nil
}

func sideEffectBoundaryExpansionRequiresBranchRebind(input sideEffectResolveInput) bool {
	return strings.TrimSpace(input.BranchID) != "" || strings.TrimSpace(input.ActiveTaskID) != ""
}

func sideEffectResolveMapFromJSON(raw string) (map[string]any, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func sideEffectResolveCopyRoleTransitionFields(output map[string]any, payload map[string]any) {
	if output == nil || payload == nil {
		return
	}
	for _, key := range []string{
		"boundary_transition_state",
		"boundary_transition_key",
		"existing_task_id",
		"lead_task_id",
		"transition_already_pending",
		"transition_denied",
		"denial_reason",
		"preferred_transition",
		"next_transition",
		"next_action",
		"allowed_next",
		"conflicting_task_id",
		"conflicting_owner_agent_id",
		"conflicting_branch_id",
		"conflicting_active_task_id",
		"active_claim_rebind",
		"do_not_retry",
		"warning",
	} {
		if value, ok := payload[key]; ok {
			output[key] = value
		}
	}
}

func sideEffectBoundaryTransitionKey(workspaceID string, input sideEffectResolveInput) string {
	parts := []string{
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(input.ProjectID),
		strings.TrimSpace(firstNonEmpty(input.TargetAgentID, input.OwnerAgentID)),
		strings.TrimSpace(input.BranchID),
		strings.TrimSpace(input.ActiveTaskID),
		strings.TrimSpace(input.Decision),
		strings.Join(uniqueNonEmptyStrings(input.SideEffectRefs), "|"),
	}
	return "abpc-boundary-transition:" + shortRefHash(strings.Join(parts, "\x00"))
}

func sideEffectResolveBoolField(values map[string]any, key string) bool {
	switch typed := values[key].(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true") || strings.EqualFold(strings.TrimSpace(typed), "1")
	case float64:
		return typed != 0
	default:
		return false
	}
}

type sideEffectResolutionTaskSpec struct {
	ABPCTaskClass string
	CoreTaskClass string
	ProjectLane   string
	TaskKind      string
	Priority      string
	TitlePrefix   string
	Tags          []string
}

func (t *SideEffectResolveTool) createSideEffectResolutionTask(ctx context.Context, input sideEffectResolveInput, spec sideEffectResolutionTaskSpec) (map[string]any, error) {
	requiresGate := false
	abpcTaskClass := firstNonEmpty(strings.TrimSpace(spec.ABPCTaskClass), "side_effect_resolution")
	coreTaskClass := firstNonEmpty(strings.TrimSpace(spec.CoreTaskClass), "INCIDENT")
	taskID := sideEffectResolutionFollowupTaskID(t.workspaceID, input, abpcTaskClass)
	actionKind := sideEffectResolutionActionKind(input.Decision, abpcTaskClass)
	successorKey := sideEffectResolutionSuccessorKey(t.workspaceID, input, actionKind)
	if output, ok := t.reuseActiveSideEffectResolutionSuccessor(ctx, input, spec, abpcTaskClass, coreTaskClass, actionKind, successorKey); ok {
		return output, nil
	}
	if output, ok := t.reuseExistingSideEffectResolutionSuccessor(ctx, input, spec, abpcTaskClass, coreTaskClass, actionKind, successorKey); ok {
		return output, nil
	}
	if sideEffectResolutionCreationWouldBePathlessFoundationCarrier(input, abpcTaskClass, actionKind) {
		return map[string]any{
			"transition_executed":         true,
			"followup_created":            false,
			"followup_terminal_blocker":   true,
			"followup_task_class":         abpcTaskClass,
			"followup_core_task_class":    coreTaskClass,
			"followup_task_lane":          spec.ProjectLane,
			"followup_action_kind":        actionKind,
			"followup_successor_key":      successorKey,
			"successor_key":               successorKey,
			"resolution_saga_key":         sideEffectResolutionSagaKey(t.workspaceID, input),
			"terminal_blocker_type":       "abpc_pathless_foundation_carrier_no_substrate_identity",
			"terminal_blocker_summary":    "split_tension cannot mint a foundation side-effect carrier without dirty-path/write-scope identity",
			"typed_terminal_blocker":      true,
			"do_not_retry":                true,
			"next_transition":             "typed_terminal_blocker",
			"pathless_carrier_suppressed": true,
		}, nil
	}
	result, err := t.client.SubmitTask(ctx, TaskSubmitInput{
		WorkspaceID:         t.workspaceID,
		TaskID:              taskID,
		OwnerUserID:         firstNonEmpty(t.ownerUserID, t.agentID),
		Priority:            firstNonEmpty(spec.Priority, "high"),
		Title:               sideEffectResolutionTaskTitle(input, spec),
		Description:         renderSideEffectResolutionTaskDescription(input, spec),
		TaskKind:            firstNonEmpty(spec.TaskKind, "COORDINATION"),
		TaskTemplate:        "generic",
		TaskClass:           coreTaskClass,
		TaskClassSource:     "EXPLICIT",
		ProjectID:           input.ProjectID,
		ProjectLane:         spec.ProjectLane,
		RequiresProjectGate: &requiresGate,
		WriteScopeHints:     input.DirtyPaths,
		TaskRequirements: map[string]any{
			"schema":                          "artifact_bound_side_effect_resolution_followup.v1",
			"abpc_task_class":                 abpcTaskClass,
			"admission_kind":                  "abpc_recovery_action",
			"action_kind":                     actionKind,
			"successor_key":                   successorKey,
			"resolution_saga_key":             sideEffectResolutionSagaKey(t.workspaceID, input),
			"decision":                        input.Decision,
			"integration_status":              sideEffectResolveIntegrationStatus(input.Decision),
			"side_effect_refs":                input.SideEffectRefs,
			"project_id":                      input.ProjectID,
			"branch_id":                       input.BranchID,
			"branch_name":                     input.BranchName,
			"owner_agent_id":                  input.OwnerAgentID,
			"target_agent_id":                 input.TargetAgentID,
			"active_task_id":                  input.ActiveTaskID,
			"classification_task_id":          input.ClassificationTaskID,
			"parent_classifier_task_id":       input.ClassificationTaskID,
			"dirty_paths":                     input.DirtyPaths,
			"path_bucket":                     input.DirtyPaths,
			"write_scope_hints":               input.DirtyPaths,
			"preserve_write_scope_hints":      true,
			"write_scope_hints_authoritative": true,
			"justification":                   input.Justification,
			"next_transition":                 sideEffectResolveNextTransition(input.Decision),
		},
		Tags:     spec.Tags,
		LinkedBy: t.agentID,
	})
	created := true
	warning := ""
	if err != nil {
		if !isDuplicateTaskSubmitError(err) {
			return nil, err
		}
		created = false
		warning = "resolution follow-up task already exists"
	} else if strings.TrimSpace(result.TaskID) != "" {
		taskID = strings.TrimSpace(result.TaskID)
	}
	reconcile := t.reconcileSideEffectResolutionSuccessors(ctx, input, taskID, actionKind, successorKey)
	output := map[string]any{
		"transition_executed":      true,
		"followup_task_id":         taskID,
		"followup_task_class":      abpcTaskClass,
		"followup_core_task_class": coreTaskClass,
		"followup_task_lane":       spec.ProjectLane,
		"followup_action_kind":     actionKind,
		"followup_successor_key":   successorKey,
		"followup_created":         created,
	}
	if warning != "" {
		output["followup_warning"] = warning
	}
	for key, value := range reconcile {
		output[key] = value
	}
	return output, nil
}

func (t *SideEffectResolveTool) reuseExistingSideEffectResolutionSuccessor(ctx context.Context, input sideEffectResolveInput, spec sideEffectResolutionTaskSpec, abpcTaskClass, coreTaskClass, actionKind, successorKey string) (map[string]any, bool) {
	if t == nil || t.client == nil || actionKind == "" {
		return nil, false
	}
	tasks, err := t.client.ListTasks(ctx, t.workspaceID)
	if err != nil {
		return nil, false
	}
	records := sideEffectResolutionFollowupRecords(tasks)
	current := sideEffectResolutionFollowupRecord{
		Status:               "PENDING",
		UpdatedAt:            "9999-12-31T23:59:59Z",
		ABPCTaskClass:        abpcTaskClass,
		AdmissionKind:        "abpc_recovery_action",
		ActionKind:           actionKind,
		SuccessorKey:         successorKey,
		ResolutionSagaKey:    sideEffectResolutionSagaKey(t.workspaceID, input),
		Decision:             input.Decision,
		ClassificationTaskID: strings.TrimSpace(input.ClassificationTaskID),
		ProjectID:            strings.TrimSpace(input.ProjectID),
		BranchID:             strings.TrimSpace(input.BranchID),
		BranchName:           strings.TrimSpace(input.BranchName),
		ActiveTaskID:         strings.TrimSpace(input.ActiveTaskID),
		OwnerAgentID:         strings.TrimSpace(input.OwnerAgentID),
		TargetAgentID:        strings.TrimSpace(input.TargetAgentID),
		SideEffectRefs:       uniqueNonEmptyStrings(input.SideEffectRefs),
		DirtyPaths:           normalizeProjectWriteScopePaths(input.DirtyPaths),
	}
	var best sideEffectResolutionFollowupRecord
	bestScore := -1
	for _, record := range records {
		if strings.TrimSpace(record.TaskID) == "" || sideEffectResolutionTaskTerminal(record.Status) {
			continue
		}
		if !sideEffectResolutionSameSaga(current, record) {
			continue
		}
		if strings.TrimSpace(record.ActionKind) != "" && !strings.EqualFold(strings.TrimSpace(record.ActionKind), actionKind) {
			continue
		}
		if !sideEffectResolutionExistingSuccessorCoversCurrent(record, current) {
			continue
		}
		if sideEffectResolutionPathlessFoundationCarrier(record) {
			continue
		}
		score := 0
		if strings.EqualFold(strings.TrimSpace(record.ActionKind), actionKind) {
			score += 8
		}
		if sideEffectResolutionStringSetsEqual(record.SideEffectRefs, input.SideEffectRefs, true) {
			score += 6
		} else if sideEffectResolutionStringSliceCovers(record.SideEffectRefs, input.SideEffectRefs, true) {
			score += 4
		}
		if len(record.DirtyPaths) > 0 {
			score += 3
			if sideEffectResolutionStringSliceCovers(record.DirtyPaths, current.DirtyPaths, false) {
				score += 3
			}
		}
		if sideEffectResolutionClaimAvailable(record.ClaimStatus) {
			score += 2
		}
		if record.TargetAgentID != "" && strings.EqualFold(record.TargetAgentID, input.TargetAgentID) {
			score++
		}
		if score > bestScore || (score == bestScore && sideEffectResolutionRecordNewer(record, best)) {
			best = record
			bestScore = score
		}
	}
	if bestScore < 0 {
		return nil, false
	}
	reusedSuccessorKey := firstNonEmpty(best.SuccessorKey, successorKey)
	reusedSagaKey := firstNonEmpty(best.ResolutionSagaKey, sideEffectResolutionSagaKey(t.workspaceID, input))
	output := map[string]any{
		"transition_executed":              true,
		"followup_task_id":                 best.TaskID,
		"existing_task_id":                 best.TaskID,
		"followup_task_class":              firstNonEmpty(best.ABPCTaskClass, abpcTaskClass),
		"followup_core_task_class":         coreTaskClass,
		"followup_task_lane":               firstNonEmpty(spec.ProjectLane, "coordination"),
		"followup_action_kind":             actionKind,
		"followup_successor_key":           reusedSuccessorKey,
		"followup_created":                 false,
		"followup_warning":                 "existing ABPC recovery successor already owns this side-effect saga",
		"active_recovery_successor_reused": true,
		"successor_key":                    reusedSuccessorKey,
		"resolution_saga_key":              reusedSagaKey,
		"next_transition":                  sideEffectResolveNextTransition(input.Decision),
	}
	for key, value := range t.reconcileSideEffectResolutionSuccessors(ctx, input, best.TaskID, actionKind, reusedSuccessorKey) {
		output[key] = value
	}
	return output, true
}

func (t *SideEffectResolveTool) reuseActiveSideEffectResolutionSuccessor(ctx context.Context, input sideEffectResolveInput, spec sideEffectResolutionTaskSpec, abpcTaskClass, coreTaskClass, actionKind, successorKey string) (map[string]any, bool) {
	activeTaskID := strings.TrimSpace(input.ActiveTaskID)
	if t == nil || t.client == nil || activeTaskID == "" || actionKind == "" {
		return nil, false
	}
	if !strings.HasPrefix(activeTaskID, "task-side-effect-") {
		return nil, false
	}
	tasks, err := t.client.ListTasks(ctx, t.workspaceID)
	if err != nil {
		return nil, false
	}
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) != activeTaskID {
			continue
		}
		record, ok := sideEffectResolutionFollowupRecordFromTask(task)
		if !ok || sideEffectResolutionTaskTerminal(record.Status) {
			return nil, false
		}
		if !strings.EqualFold(strings.TrimSpace(record.AdmissionKind), "abpc_recovery_action") {
			return nil, false
		}
		if !sideEffectResolutionActiveSuccessorMatchesInput(t.workspaceID, record, input) {
			return nil, false
		}
		previousActionKind := strings.TrimSpace(record.ActionKind)
		actionPivot := previousActionKind != "" && !strings.EqualFold(previousActionKind, strings.TrimSpace(actionKind))
		reusedSuccessorKey := firstNonEmpty(record.SuccessorKey, successorKey)
		reusedSagaKey := firstNonEmpty(record.ResolutionSagaKey, sideEffectResolutionSagaKey(t.workspaceID, input))
		warning := "active ABPC recovery successor already owns this action"
		if actionPivot {
			warning = "active ABPC recovery successor already owns this side-effect saga; recorded decision pivot on current successor"
		}
		return map[string]any{
			"transition_executed":                            true,
			"followup_task_id":                               activeTaskID,
			"existing_task_id":                               activeTaskID,
			"followup_task_class":                            firstNonEmpty(record.ABPCTaskClass, abpcTaskClass),
			"followup_core_task_class":                       coreTaskClass,
			"followup_task_lane":                             firstNonEmpty(strings.TrimSpace(task.ProjectLane), spec.ProjectLane),
			"followup_action_kind":                           actionKind,
			"followup_successor_key":                         reusedSuccessorKey,
			"followup_created":                               false,
			"followup_warning":                               warning,
			"active_recovery_successor_reused":               true,
			"active_recovery_successor_previous_action_kind": previousActionKind,
			"self_recursive_recovery_collapsed":              !actionPivot,
			"cross_decision_recovery_pivot_collapsed":        actionPivot,
			"successor_key":                                  reusedSuccessorKey,
			"resolution_saga_key":                            reusedSagaKey,
			"next_transition":                                sideEffectResolveNextTransition(input.Decision),
		}, true
	}
	return nil, false
}

func sideEffectResolutionActiveSuccessorMatchesInput(workspaceID string, record sideEffectResolutionFollowupRecord, input sideEffectResolveInput) bool {
	if record.ResolutionSagaKey != "" {
		if record.ResolutionSagaKey == sideEffectResolutionSagaKey(workspaceID, input) {
			return true
		}
		if record.ActiveTaskID != "" {
			originalInput := input
			originalInput.ActiveTaskID = record.ActiveTaskID
			if record.ResolutionSagaKey == sideEffectResolutionSagaKey(workspaceID, originalInput) {
				return true
			}
		}
	}
	return sideEffectResolutionStableBucketMatch(record, input)
}

func sideEffectResolutionStableBucketMatch(record sideEffectResolutionFollowupRecord, input sideEffectResolveInput) bool {
	matched := false
	matchedStrong := false
	if record.ProjectID != "" && input.ProjectID != "" {
		if !strings.EqualFold(record.ProjectID, input.ProjectID) {
			return false
		}
		matched = true
	}
	if record.BranchID != "" && input.BranchID != "" {
		if !strings.EqualFold(record.BranchID, input.BranchID) {
			return false
		}
		matched = true
	}
	if record.BranchName != "" && input.BranchName != "" {
		if record.BranchName != input.BranchName {
			return false
		}
		matched = true
	}
	if record.ClassificationTaskID != "" && input.ClassificationTaskID != "" {
		if !strings.EqualFold(record.ClassificationTaskID, input.ClassificationTaskID) {
			return false
		}
		matched = true
		matchedStrong = true
	}
	if len(record.SideEffectRefs) > 0 && len(input.SideEffectRefs) > 0 {
		if !sideEffectResolutionStringSetsEqual(record.SideEffectRefs, input.SideEffectRefs, true) {
			return false
		}
		matched = true
		matchedStrong = true
	}
	if len(record.DirtyPaths) > 0 && len(input.DirtyPaths) > 0 {
		if !sideEffectResolutionStringSetsEqual(record.DirtyPaths, normalizeProjectWriteScopePaths(input.DirtyPaths), false) {
			return false
		}
		matched = true
	}
	return matched && matchedStrong
}

func sideEffectResolutionStringSetsEqual(left, right []string, caseFold bool) bool {
	left = uniqueNonEmptyStrings(left)
	right = uniqueNonEmptyStrings(right)
	if len(left) != len(right) {
		return false
	}
	index := make(map[string]struct{}, len(left))
	for _, value := range left {
		value = strings.TrimSpace(value)
		if caseFold {
			value = strings.ToLower(value)
		}
		index[value] = struct{}{}
	}
	for _, value := range right {
		value = strings.TrimSpace(value)
		if caseFold {
			value = strings.ToLower(value)
		}
		if _, ok := index[value]; !ok {
			return false
		}
	}
	return true
}

func sideEffectResolutionStringSliceCovers(left, right []string, caseFold bool) bool {
	left = uniqueNonEmptyStrings(left)
	right = uniqueNonEmptyStrings(right)
	if len(right) == 0 {
		return len(left) > 0
	}
	index := make(map[string]struct{}, len(left))
	for _, value := range left {
		value = strings.TrimSpace(value)
		if caseFold {
			value = strings.ToLower(value)
		}
		index[value] = struct{}{}
	}
	for _, value := range right {
		value = strings.TrimSpace(value)
		if caseFold {
			value = strings.ToLower(value)
		}
		if _, ok := index[value]; !ok {
			return false
		}
	}
	return true
}

func sideEffectResolutionStringSlicesIntersect(left, right []string, caseFold bool) bool {
	left = uniqueNonEmptyStrings(left)
	right = uniqueNonEmptyStrings(right)
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	index := make(map[string]struct{}, len(left))
	for _, value := range left {
		value = strings.TrimSpace(value)
		if caseFold {
			value = strings.ToLower(value)
		}
		index[value] = struct{}{}
	}
	for _, value := range right {
		value = strings.TrimSpace(value)
		if caseFold {
			value = strings.ToLower(value)
		}
		if _, ok := index[value]; ok {
			return true
		}
	}
	return false
}

type sideEffectResolutionFollowupRecord struct {
	TaskID               string
	Status               string
	UpdatedAt            string
	ClaimStatus          string
	Schema               string
	ABPCTaskClass        string
	AdmissionKind        string
	ActionKind           string
	SuccessorKey         string
	ResolutionSagaKey    string
	Decision             string
	ClassificationTaskID string
	ProjectID            string
	BranchID             string
	BranchName           string
	ActiveTaskID         string
	OwnerAgentID         string
	TargetAgentID        string
	SideEffectRefs       []string
	DirtyPaths           []string
}

func (t *SideEffectResolveTool) reconcileSideEffectResolutionSuccessors(ctx context.Context, input sideEffectResolveInput, taskID, actionKind, successorKey string) map[string]any {
	out := map[string]any{
		"parent_classification_task_id": input.ClassificationTaskID,
		"parent_classification_state":   "waiting_on_successors",
		"successor_key":                 successorKey,
		"resolution_saga_key":           sideEffectResolutionSagaKey(t.workspaceID, input),
	}
	if t.client == nil {
		return out
	}
	tasks, err := t.client.ListTasks(ctx, t.workspaceID)
	if err != nil {
		out["successor_reconcile_warning"] = "workspace.tasks.list failed: " + err.Error()
		return out
	}
	records := sideEffectResolutionFollowupRecords(tasks)
	current := sideEffectResolutionFollowupRecord{
		TaskID:               strings.TrimSpace(taskID),
		Status:               "PENDING",
		UpdatedAt:            "9999-12-31T23:59:59Z",
		ABPCTaskClass:        "side_effect_resolution",
		AdmissionKind:        "abpc_recovery_action",
		ActionKind:           actionKind,
		SuccessorKey:         successorKey,
		ResolutionSagaKey:    sideEffectResolutionSagaKey(t.workspaceID, input),
		Decision:             input.Decision,
		ClassificationTaskID: strings.TrimSpace(input.ClassificationTaskID),
		ProjectID:            strings.TrimSpace(input.ProjectID),
		BranchID:             strings.TrimSpace(input.BranchID),
		BranchName:           strings.TrimSpace(input.BranchName),
		ActiveTaskID:         strings.TrimSpace(input.ActiveTaskID),
		OwnerAgentID:         strings.TrimSpace(input.OwnerAgentID),
		TargetAgentID:        strings.TrimSpace(input.TargetAgentID),
		SideEffectRefs:       uniqueNonEmptyStrings(input.SideEffectRefs),
		DirtyPaths:           normalizeProjectWriteScopePaths(input.DirtyPaths),
	}
	if !sideEffectResolutionRecordsContain(records, current.TaskID) {
		records = append(records, current)
	}
	superseded := make([]string, 0, 4)
	warnings := make([]string, 0, 2)
	for _, candidate := range records {
		if !sideEffectResolutionSameSaga(current, candidate) || strings.TrimSpace(candidate.TaskID) == "" || strings.TrimSpace(candidate.TaskID) == strings.TrimSpace(current.TaskID) {
			continue
		}
		if sideEffectResolutionTaskTerminal(candidate.Status) {
			continue
		}
		if !sideEffectResolutionClaimAvailable(candidate.ClaimStatus) {
			continue
		}
		if !sideEffectResolutionFollowupCoveredByNewer(candidate, records, current.TaskID) {
			continue
		}
		reason := fmt.Sprintf("superseded_by_side_effect_resolution_successor:%s", current.TaskID)
		if err := t.client.CloseTask(ctx, TaskCloseInput{
			WorkspaceID: t.workspaceID,
			TaskID:      candidate.TaskID,
			ActorID:     t.agentID,
			Resolution:  "CANCELLED",
			Reason:      reason,
		}); err != nil {
			warnings = append(warnings, candidate.TaskID+": "+err.Error())
			continue
		}
		superseded = append(superseded, candidate.TaskID)
	}
	if len(superseded) > 0 {
		out["superseded_followup_task_ids"] = superseded
	}
	if len(warnings) > 0 {
		out["successor_supersede_warnings"] = warnings
	}
	if strings.TrimSpace(input.ClassificationTaskID) != "" {
		if err := t.client.CloseTask(ctx, TaskCloseInput{
			WorkspaceID: t.workspaceID,
			TaskID:      strings.TrimSpace(input.ClassificationTaskID),
			ActorID:     t.agentID,
			Resolution:  "RESOLVED",
			Reason:      "resolved_split_waiting_on_side_effect_resolution_successor:" + strings.TrimSpace(taskID),
		}); err != nil {
			out["parent_classification_close_warning"] = "task.close failed: " + err.Error()
			if blockErr := t.client.BlockTask(ctx, TaskBlockInput{
				WorkspaceID: t.workspaceID,
				AgentID:     t.agentID,
				TaskID:      strings.TrimSpace(input.ClassificationTaskID),
				Reason:      "waiting_on_side_effect_resolution_successors:" + strings.TrimSpace(taskID),
			}); blockErr != nil {
				out["parent_classification_warning"] = "agent.task.block fallback failed: " + blockErr.Error()
				if sideEffectResolutionParentBlockMissingClaim(blockErr) {
					out["parent_classification_warning"] = out["parent_classification_warning"].(string) + "; parent claim missing"
				}
			} else {
				out["parent_classification_blocked"] = true
			}
		} else {
			out["parent_classification_resolved_split"] = true
			out["parent_classification_state"] = "resolved_waiting_on_successors"
		}
	}
	return out
}

func sideEffectResolutionParentBlockMissingClaim(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "task claim not found") ||
		strings.Contains(text, "claim not found") ||
		strings.Contains(text, "not claimed")
}

func sideEffectResolutionFollowupRecords(tasks []WorkspaceTaskRecord) []sideEffectResolutionFollowupRecord {
	records := make([]sideEffectResolutionFollowupRecord, 0, len(tasks))
	for _, task := range tasks {
		record, ok := sideEffectResolutionFollowupRecordFromTask(task)
		if ok {
			records = append(records, record)
		}
	}
	return records
}

func sideEffectResolutionFollowupRecordFromTask(task WorkspaceTaskRecord) (sideEffectResolutionFollowupRecord, bool) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(task.TaskRequirementsJSON)), &payload); err != nil {
		return sideEffectResolutionFollowupRecord{}, false
	}
	schema := strings.TrimSpace(stringMapValue(payload, "schema"))
	abpcClass := strings.TrimSpace(stringMapValue(payload, "abpc_task_class"))
	admissionKind := strings.TrimSpace(stringMapValue(payload, "admission_kind"))
	if schema != "artifact_bound_side_effect_resolution_followup.v1" && admissionKind != "abpc_recovery_action" {
		return sideEffectResolutionFollowupRecord{}, false
	}
	claimStatus := ""
	if task.ClaimStatus != nil {
		claimStatus = strings.TrimSpace(*task.ClaimStatus)
	}
	record := sideEffectResolutionFollowupRecord{
		TaskID:               strings.TrimSpace(task.TaskID),
		Status:               strings.TrimSpace(task.Status),
		UpdatedAt:            strings.TrimSpace(task.UpdatedAt),
		ClaimStatus:          claimStatus,
		Schema:               schema,
		ABPCTaskClass:        abpcClass,
		AdmissionKind:        admissionKind,
		ActionKind:           strings.TrimSpace(stringMapValue(payload, "action_kind")),
		SuccessorKey:         strings.TrimSpace(stringMapValue(payload, "successor_key")),
		ResolutionSagaKey:    strings.TrimSpace(stringMapValue(payload, "resolution_saga_key")),
		Decision:             strings.TrimSpace(stringMapValue(payload, "decision")),
		ClassificationTaskID: firstNonEmpty(strings.TrimSpace(stringMapValue(payload, "parent_classifier_task_id")), strings.TrimSpace(stringMapValue(payload, "classification_task_id"))),
		ProjectID:            strings.TrimSpace(stringMapValue(payload, "project_id")),
		BranchID:             strings.TrimSpace(stringMapValue(payload, "branch_id")),
		BranchName:           strings.TrimSpace(stringMapValue(payload, "branch_name")),
		ActiveTaskID:         strings.TrimSpace(stringMapValue(payload, "active_task_id")),
		OwnerAgentID:         strings.TrimSpace(stringMapValue(payload, "owner_agent_id")),
		TargetAgentID:        strings.TrimSpace(stringMapValue(payload, "target_agent_id")),
		SideEffectRefs:       uniqueNonEmptyStrings(stringSliceFromAny(payload["side_effect_refs"])),
		DirtyPaths:           normalizeProjectWriteScopePaths(firstNonEmptyStringSlice(stringSliceFromAny(payload["path_bucket"]), stringSliceFromAny(payload["dirty_paths"]))),
	}
	if record.ActionKind == "" {
		record.ActionKind = sideEffectResolutionActionKind(record.Decision, record.ABPCTaskClass)
	}
	return record, true
}

func sideEffectResolutionRecordsContain(records []sideEffectResolutionFollowupRecord, taskID string) bool {
	taskID = strings.TrimSpace(taskID)
	for _, record := range records {
		if strings.TrimSpace(record.TaskID) == taskID {
			return true
		}
	}
	return false
}

func sideEffectResolutionSameSaga(left, right sideEffectResolutionFollowupRecord) bool {
	if left.ResolutionSagaKey != "" && right.ResolutionSagaKey != "" {
		if left.ResolutionSagaKey == right.ResolutionSagaKey {
			return true
		}
		if len(left.SideEffectRefs) > 0 && len(right.SideEffectRefs) > 0 &&
			!sideEffectResolutionStringSetsEqual(left.SideEffectRefs, right.SideEffectRefs, true) &&
			!sideEffectResolutionSameStableBranchBucket(left, right) {
			return false
		}
	}
	if left.ClassificationTaskID != "" && right.ClassificationTaskID != "" {
		if left.ClassificationTaskID == right.ClassificationTaskID {
			return true
		}
		if !sideEffectResolutionSameStableBranchBucket(left, right) {
			return false
		}
	}
	if len(left.SideEffectRefs) > 0 && len(right.SideEffectRefs) > 0 &&
		!sideEffectResolutionStringSetsEqual(left.SideEffectRefs, right.SideEffectRefs, true) &&
		!sideEffectResolutionSameStableBranchBucket(left, right) {
		return false
	}
	if sideEffectResolutionSameStableBranchBucket(left, right) {
		return true
	}
	return left.ProjectID == right.ProjectID &&
		left.BranchID == right.BranchID &&
		left.BranchName == right.BranchName &&
		left.ActiveTaskID == right.ActiveTaskID
}

func sideEffectResolutionSameStableBranchBucket(left, right sideEffectResolutionFollowupRecord) bool {
	if left.ProjectID != "" && right.ProjectID != "" && !strings.EqualFold(left.ProjectID, right.ProjectID) {
		return false
	}
	branchMatched := false
	if left.BranchID != "" && right.BranchID != "" {
		if !strings.EqualFold(left.BranchID, right.BranchID) {
			return false
		}
		branchMatched = true
	} else if left.BranchName != "" && right.BranchName != "" {
		if left.BranchName != right.BranchName {
			return false
		}
		branchMatched = true
	}
	if !branchMatched {
		return false
	}
	return sideEffectResolutionStringSlicesIntersect(left.SideEffectRefs, right.SideEffectRefs, true) ||
		sideEffectResolutionStringSlicesIntersect(left.DirtyPaths, right.DirtyPaths, false)
}

func sideEffectResolutionExistingSuccessorCoversCurrent(existing, current sideEffectResolutionFollowupRecord) bool {
	if len(current.DirtyPaths) > 0 && len(existing.DirtyPaths) > 0 &&
		sideEffectResolutionStringSliceCovers(existing.DirtyPaths, current.DirtyPaths, false) {
		return true
	}
	if len(current.SideEffectRefs) > 0 && len(existing.SideEffectRefs) > 0 &&
		sideEffectResolutionStringSliceCovers(existing.SideEffectRefs, current.SideEffectRefs, true) {
		return true
	}
	return false
}

func sideEffectResolutionPathlessRecoveryCarrier(record sideEffectResolutionFollowupRecord) bool {
	if !strings.EqualFold(strings.TrimSpace(record.AdmissionKind), "abpc_recovery_action") {
		return false
	}
	return len(record.DirtyPaths) == 0 && len(record.SideEffectRefs) > 0
}

func sideEffectResolutionPathlessFoundationCarrier(record sideEffectResolutionFollowupRecord) bool {
	if !sideEffectResolutionPathlessRecoveryCarrier(record) {
		return false
	}
	taskClass := strings.TrimSpace(record.ABPCTaskClass)
	actionKind := strings.TrimSpace(record.ActionKind)
	return strings.EqualFold(taskClass, "side_effect_foundation") ||
		strings.EqualFold(actionKind, "split_foundation_bucket")
}

func sideEffectResolutionCreationWouldBePathlessFoundationCarrier(input sideEffectResolveInput, abpcTaskClass, actionKind string) bool {
	if !strings.EqualFold(strings.TrimSpace(abpcTaskClass), "side_effect_foundation") ||
		!strings.EqualFold(strings.TrimSpace(actionKind), "split_foundation_bucket") {
		return false
	}
	return len(normalizeProjectWriteScopePaths(input.DirtyPaths)) == 0
}

func sideEffectResolutionFollowupCoveredByNewer(candidate sideEffectResolutionFollowupRecord, records []sideEffectResolutionFollowupRecord, currentTaskID string) bool {
	refCoverage := map[string]struct{}{}
	pathCoverage := map[string]struct{}{}
	coveredByOther := false
	for _, record := range records {
		if strings.TrimSpace(record.TaskID) == "" || strings.TrimSpace(record.TaskID) == strings.TrimSpace(candidate.TaskID) {
			continue
		}
		if !sideEffectResolutionSameSaga(candidate, record) || sideEffectResolutionTaskTerminal(record.Status) {
			continue
		}
		if strings.TrimSpace(record.TaskID) != strings.TrimSpace(currentTaskID) && !sideEffectResolutionRecordNewer(record, candidate) {
			continue
		}
		coveredByOther = true
		for _, ref := range record.SideEffectRefs {
			refCoverage[strings.TrimSpace(ref)] = struct{}{}
		}
		for _, path := range record.DirtyPaths {
			pathCoverage[strings.TrimSpace(path)] = struct{}{}
		}
	}
	if !coveredByOther {
		return false
	}
	if len(candidate.DirtyPaths) > 0 {
		return stringSetCovers(pathCoverage, candidate.DirtyPaths)
	}
	return stringSetCovers(refCoverage, candidate.SideEffectRefs)
}

func sideEffectResolutionRecordNewer(record, candidate sideEffectResolutionFollowupRecord) bool {
	recordUpdated := strings.TrimSpace(record.UpdatedAt)
	candidateUpdated := strings.TrimSpace(candidate.UpdatedAt)
	if recordUpdated == "" {
		return false
	}
	if candidateUpdated == "" {
		return true
	}
	return strings.Compare(recordUpdated, candidateUpdated) > 0
}

func stringSetCovers(index map[string]struct{}, values []string) bool {
	if len(values) == 0 {
		return len(index) > 0
	}
	for _, value := range values {
		if _, ok := index[strings.TrimSpace(value)]; !ok {
			return false
		}
	}
	return true
}

func sideEffectResolutionTaskTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "RESOLVED", "FAILED", "CANCELLED":
		return true
	default:
		return false
	}
}

func sideEffectResolutionClaimAvailable(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "", "RELEASED":
		return true
	default:
		return false
	}
}

func stringSliceFromAny(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if str, ok := item.(string); ok {
				out = append(out, str)
			}
		}
		return out
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return []string{typed}
	default:
		return nil
	}
}

func sideEffectResolveDecisionEffects(workspaceID, actor string, input sideEffectResolveInput) []AgentUpdateSideEffectV1 {
	status := sideEffectResolveIntegrationStatus(input.Decision)
	regionRefs := sideEffectResolveRegionRefs(input)
	effects := make([]AgentUpdateSideEffectV1, 0, len(input.SideEffectRefs))
	for index, ref := range input.SideEffectRefs {
		regionRef := regionRefs[min(index, len(regionRefs)-1)]
		effects = append(effects, AgentUpdateSideEffectV1{
			Schema:               "artifact_bound_side_effect.v1",
			SideEffectRef:        ref,
			Actor:                actor,
			LaneRef:              sideEffectResolveLaneRef(workspaceID, input),
			TensionRef:           firstNonEmpty(input.ActiveTaskID, input.ProjectID, "tension:side_effect_resolution"),
			ArtifactRef:          sideEffectResolveArtifactRef(input),
			RegionRef:            regionRef,
			Operation:            "classify",
			SourceKind:           "core:side_effect_resolution",
			BoundaryRef:          "boundary:side_effect_resolution:" + sanitizeRefSegment(firstNonEmpty(input.BranchID, input.ProjectID, "unknown")),
			BoundaryRelation:     sideEffectResolveBoundaryRelation(input.Decision),
			MaterializationState: "present_unintegrated",
			IntegrationIntent:    "repair",
			IntegrationStatus:    status,
			Decision:             input.Decision,
			Justification:        input.Justification,
			DerivedRegionRefs:    []string{regionRef},
		})
	}
	return effects
}

func sideEffectResolveRegionRefs(input sideEffectResolveInput) []string {
	if len(input.DirtyPaths) == 0 {
		return []string{"region:side_effect:" + shortRefHash(strings.Join(input.SideEffectRefs, "|"))}
	}
	out := make([]string, 0, len(input.DirtyPaths))
	for _, path := range input.DirtyPaths {
		if path = strings.TrimSpace(path); path != "" {
			out = append(out, "region:adapter:git:path:"+path)
		}
	}
	if len(out) == 0 {
		out = append(out, "region:side_effect:"+shortRefHash(strings.Join(input.SideEffectRefs, "|")))
	}
	return out
}

func sideEffectResolveArtifactRef(input sideEffectResolveInput) string {
	if input.ProjectID == "" && input.BranchID == "" && input.BranchName == "" {
		return "artifact:side_effect_resolution"
	}
	return "artifact:project:" + sanitizeRefSegment(firstNonEmpty(input.ProjectID, "project")) +
		":branch:" + sanitizeRefSegment(firstNonEmpty(input.BranchID, input.BranchName, "branch"))
}

func sideEffectResolveLaneRef(workspaceID string, input sideEffectResolveInput) string {
	parts := []string{
		"lane",
		sanitizeRefSegment(firstNonEmpty(workspaceID, "workspace")),
		sanitizeRefSegment(firstNonEmpty(input.ProjectID, "project")),
		sanitizeRefSegment(firstNonEmpty(input.ActiveTaskID, input.BranchID, input.BranchName, "side_effect_resolution")),
	}
	return strings.Join(parts, ":")
}

func sideEffectResolveBoundaryRelation(decision string) string {
	switch decision {
	case "accept", "expand_boundary":
		return "in_boundary"
	case "split_tension":
		return "adjacent_foundation_candidate"
	case "request_verification", "reroute_to_active_lane":
		return "ambiguous"
	default:
		return "out_of_boundary"
	}
}

func sideEffectResolveExpandedWriteScope(currentRaw string, dirtyPaths []string) (string, error) {
	paths, err := pathsetFromWriteScopeJSON(currentRaw)
	if err != nil {
		return "", fmt.Errorf("current_write_scope_json must be valid JSON when deriving an expanded boundary: %w", err)
	}
	if len(paths) == 0 && len(dirtyPaths) == 0 {
		return "", fmt.Errorf("expanded_write_scope_json or dirty_paths is required for boundary expansion")
	}
	paths = append(paths, dirtyPaths...)
	paths = normalizeProjectWriteScopePaths(paths)
	raw, err := json.Marshal(map[string][]string{"paths": paths})
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func sideEffectResolutionTaskTitle(input sideEffectResolveInput, spec sideEffectResolutionTaskSpec) string {
	target := firstNonEmpty(input.BranchName, input.BranchID, input.ProjectID, "artifact region")
	return strings.TrimSpace(spec.TitlePrefix + " for " + target)
}

func renderSideEffectResolutionTaskDescription(input sideEffectResolveInput, spec sideEffectResolutionTaskSpec) string {
	var b strings.Builder
	b.WriteString("# Side Effect Resolution Path\n\n")
	b.WriteString("- decision: " + input.Decision + "\n")
	b.WriteString("- integration_status: " + sideEffectResolveIntegrationStatus(input.Decision) + "\n")
	b.WriteString("- project_id: " + firstNonEmpty(input.ProjectID, "(unknown)") + "\n")
	b.WriteString("- branch_id: " + firstNonEmpty(input.BranchID, "(unknown)") + "\n")
	b.WriteString("- branch_name: " + firstNonEmpty(input.BranchName, "(unknown)") + "\n")
	b.WriteString("- owner_agent_id: " + firstNonEmpty(input.OwnerAgentID, "(unknown)") + "\n")
	b.WriteString("- target_agent_id: " + firstNonEmpty(input.TargetAgentID, "(unknown)") + "\n")
	b.WriteString("- active_task_id: " + firstNonEmpty(input.ActiveTaskID, "(unknown)") + "\n")
	b.WriteString("- classification_task_id: " + firstNonEmpty(input.ClassificationTaskID, "(none)") + "\n")
	b.WriteString("- dirty_regions: " + firstNonEmpty(strings.Join(input.DirtyPaths, ", "), "(none supplied)") + "\n")
	b.WriteString("- side_effect_refs: " + strings.Join(input.SideEffectRefs, ", ") + "\n\n")
	b.WriteString("## Required Action\n")
	switch input.Decision {
	case "split_tension":
		b.WriteString("Own the adjacent/foundation region as a separate lane. Produce bounded implementation evidence, then let the original lane resume only after this foundation is accepted or removed.\n")
	case "request_verification":
		b.WriteString("Verify whether the side effect is valid expansion, useful out-of-scope work, harmful drift, ambiguous, dependency_revealed, or foundation_effect. Then run side_effect_resolve with the final decision.\n")
	case "reassign":
		b.WriteString("Move the side effect to a better owner or create a corrected task/role boundary. Do not re-claim the original tension as duplicate implementation work.\n")
	case "quarantine":
		b.WriteString("Keep the materialized effect out of integration while preserving evidence. The original branch must not retry commit until the quarantined materialization is removed or isolated.\n")
	case "revert":
		b.WriteString("Remove the materialized drift through a bounded owner action, then retry the original lane only after the dirty integration blocker is gone.\n")
	default:
		b.WriteString("Carry out the recorded boundary transition, preserving the side-effect refs and justification as durable evidence.\n")
	}
	b.WriteString("\n## Justification\n")
	b.WriteString(input.Justification + "\n")
	_ = spec
	return strings.TrimSpace(b.String())
}

func sideEffectResolutionFollowupTaskID(workspaceID string, input sideEffectResolveInput, taskClass string) string {
	parts := []string{
		sideEffectResolutionSagaKey(workspaceID, input),
		strings.TrimSpace(taskClass),
		sideEffectResolutionActionKind(input.Decision, taskClass),
	}
	return "task-side-effect-" + shortRefHash(strings.Join(parts, "\x00"))
}

func sideEffectResolutionActionKind(decision, taskClass string) string {
	switch strings.TrimSpace(decision) {
	case "request_verification":
		return "verify_bucket"
	case "reassign":
		return "reassign_bucket"
	case "revert":
		return "revert_bucket"
	case "quarantine":
		return "quarantine_bucket"
	case "split_tension":
		return "split_foundation_bucket"
	}
	switch strings.TrimSpace(taskClass) {
	case "side_effect_verification":
		return "verify_bucket"
	case "side_effect_reassignment":
		return "reassign_bucket"
	case "side_effect_cleanup":
		return "cleanup_bucket"
	case "side_effect_foundation":
		return "split_foundation_bucket"
	default:
		return "resolve_bucket"
	}
}

func sideEffectResolutionSagaKey(workspaceID string, input sideEffectResolveInput) string {
	parts := []string{
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(input.ProjectID),
		strings.TrimSpace(input.BranchID),
		strings.TrimSpace(input.BranchName),
		strings.TrimSpace(input.ActiveTaskID),
		strings.TrimSpace(input.ClassificationTaskID),
		strings.Join(uniqueNonEmptyStrings(input.SideEffectRefs), "|"),
	}
	return "abpc-resolution-saga:" + shortRefHash(strings.Join(parts, "\x00"))
}

func sideEffectResolutionSuccessorKey(workspaceID string, input sideEffectResolveInput, actionKind string) string {
	parts := []string{
		sideEffectResolutionSagaKey(workspaceID, input),
		strings.TrimSpace(actionKind),
	}
	return "abpc-resolution-successor:" + shortRefHash(strings.Join(parts, "\x00"))
}

func normalizeSideEffectResolveDecision(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "-", "_")
	value = strings.ReplaceAll(value, " ", "_")
	switch value {
	case "accept", "quarantine", "revert", "split_tension", "expand_boundary", "reassign", "request_verification", "reroute_to_active_lane":
		return value
	default:
		return ""
	}
}

func sideEffectResolveIntegrationStatus(decision string) string {
	switch decision {
	case "accept":
		return "accepted"
	case "quarantine":
		return "quarantined"
	case "revert":
		return "rejected"
	case "split_tension", "reassign":
		return "split_requested"
	case "expand_boundary":
		return "boundary_expansion_requested"
	case "request_verification", "reroute_to_active_lane":
		return "verification_requested"
	default:
		return "pending_classification"
	}
}

func sideEffectResolveNextTransition(decision string) string {
	switch decision {
	case "accept", "expand_boundary":
		return "expand_boundary"
	case "split_tension":
		return "create_foundation_lane"
	case "request_verification":
		return "route_to_verifier"
	case "reassign":
		return "route_to_new_owner"
	case "quarantine":
		return "quarantine_materialization"
	case "revert":
		return "remove_materialization"
	case "reroute_to_active_lane":
		return "reroute_to_active_lane"
	default:
		return "classify_side_effect"
	}
}
