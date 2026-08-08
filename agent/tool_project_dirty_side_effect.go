package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type projectDirtySideEffectBlocker struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
	ownerUserID string
}

type projectDirtySideEffectBlockInput struct {
	SourceTool          string
	ProjectID           string
	Repo                ProjectRepositoryRecord
	Branch              ProjectBranchRecord
	Args                map[string]any
	DirtyPaths          []string
	Pathset             []string
	BlockedDetail       string
	UpdateSummary       string
	MandatoryNextAction string
}

type projectSideEffectTaskRoute struct {
	TaskID             string
	Created            bool
	Warning            string
	RouteType          string
	TerminalTaskID     string
	TerminalTaskStatus string
}

func (b projectDirtySideEffectBlocker) detectDirtySideEffects(input projectDirtySideEffectBlockInput) []AgentUpdateSideEffectV1 {
	return projectBranchCommitSideEffectsForBoundary(b.workspaceID, b.agentID, input.ProjectID, input.Repo, input.Branch, input.Args, input.Pathset, input.DirtyPaths)
}

func (b projectDirtySideEffectBlocker) returnSideEffectBlock(ctx context.Context, input projectDirtySideEffectBlockInput, sideEffects []AgentUpdateSideEffectV1) *ToolResult {
	sourceTool := firstNonEmpty(strings.TrimSpace(input.SourceTool), "artifact_checkpoint")
	blockedDetail := firstNonEmpty(strings.TrimSpace(input.BlockedDetail), sourceTool+" found dirty paths outside the operational boundary")
	updateSummary := firstNonEmpty(strings.TrimSpace(input.UpdateSummary), sourceTool+" blocked pending side-effect classification")
	mandatoryNextAction := firstNonEmpty(strings.TrimSpace(input.MandatoryNextAction), "classify side effects before retrying "+sourceTool)
	// Single hydration shared by all prior-decision deterministic checks (Class 4),
	// so adding the recorded-resolution reuse does not add an RPC round trip beyond
	// the existing boundary-denial lookup.
	bundle, haveBundle := b.hydrateActiveTaskForSideEffects(ctx, input)
	if denial, ok := b.existingBoundaryDenialForSideEffects(ctx, input, sideEffects, bundle, haveBundle); ok {
		return projectDirtySideEffectExistingDenialBlock(sourceTool, input, sideEffects, denial)
	}
	if reused := b.reuseRecordedResolution(input, sideEffects, bundle, haveBundle); reused != nil {
		return reused
	}
	if fast := b.resolveSideEffectFastPath(ctx, input, sideEffects); fast != nil {
		return fast
	}
	payload := map[string]any{
		"status":       "blocked",
		"source_tool":  sourceTool,
		"next_action":  "side_effect_classification",
		"task_ids":     uniqueNonEmptyStrings([]string{strings.TrimSpace(input.Branch.ActiveTaskID), strings.TrimSpace(input.Branch.ActiveClaimID)}),
		"blocked_on":   []map[string]string{{"kind": "side_effect_classification", "detail": blockedDetail}},
		"side_effects": sideEffects,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("%s blocked out-of-boundary dirty paths but failed encoding side-effect evidence: %v", sourceTool, err), IsError: true}
	}
	if err := b.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID:   b.workspaceID,
		AgentID:       b.agentID,
		UpdateType:    "blocked",
		Summary:       updateSummary,
		PayloadJSON:   string(raw),
		RequiresHuman: false,
	}); err != nil {
		return &ToolResult{Output: fmt.Sprintf("%s blocked dirty paths outside branch write scope but failed posting required side-effect evidence: %v", sourceTool, err), IsError: true}
	}
	taskRoute := b.ensureSideEffectClassificationTask(ctx, input, sideEffects)
	classificationTaskAvailable := strings.TrimSpace(taskRoute.TaskID) != "" && taskRoute.RouteType != "reconciliation"
	output := map[string]any{
		"status":                         "blocked",
		"source_tool":                    sourceTool,
		"gate_state":                     "blocked",
		"gate_type":                      "side_effect_classification",
		"next_action":                    "side_effect_classification",
		"next_transition":                "request_verification",
		"allowed_transitions":            []string{"request_verification", "split_tension", "expand_boundary", "reassign"},
		"next_executable_lane":           "side_effect_classification_task",
		"classification_task_created":    classificationTaskAvailable && taskRoute.Created,
		"classification_task_available":  classificationTaskAvailable,
		"classification_task_abpc_class": "side_effect_classification",
		"project_id":                     input.ProjectID,
		"branch_id":                      input.Branch.BranchID,
		"branch_name":                    input.Branch.BranchName,
		"dirty_paths":                    input.DirtyPaths,
		"write_scope_json":               projectBranchCommitWriteScopeJSON(input.Pathset),
		"side_effects":                   sideEffects,
		"git_add_attempted":              false,
		"commit_created":                 false,
		"integration_status":             "pending_classification",
		"mandatory_next_action":          mandatoryNextAction,
		"do_not_retry_until":             "side_effect_classified",
		"classification_options":         []string{"accept", "quarantine", "revert", "split_tension", "expand_boundary", "reassign", "request_verification"},
	}
	if taskRoute.RouteType == "reconciliation" {
		output["gate_type"] = "side_effect_reconciliation"
		output["next_action"] = "side_effect_reconciliation"
		output["next_transition"] = "repair_checkout_identity"
		output["allowed_transitions"] = []string{"repair_checkout_identity", "terminalize_stale_checkout", "request_verification", "split_tension"}
		output["next_executable_lane"] = "side_effect_reconciliation_task"
		output["reconciliation_task_id"] = taskRoute.TaskID
		output["reconciliation_task_created"] = taskRoute.Created
		output["stale_classification_task_id"] = taskRoute.TerminalTaskID
		output["stale_classification_task_status"] = taskRoute.TerminalTaskStatus
		output["integration_status"] = "needs_reconciliation"
		output["mandatory_next_action"] = "reconcile the terminal side-effect classification route before retrying " + sourceTool
		output["do_not_retry_until"] = "side_effect_reconciled"
	} else if strings.TrimSpace(taskRoute.TaskID) != "" {
		output["classification_task_id"] = taskRoute.TaskID
	}
	if strings.TrimSpace(taskRoute.Warning) != "" {
		output["classification_task_warning"] = taskRoute.Warning
	}
	out, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return &ToolResult{Output: sourceTool + " blocked dirty paths outside branch write scope pending side-effect classification", IsError: true}
	}
	return &ToolResult{Output: string(out), IsError: true}
}

// hydrateActiveTaskForSideEffects fetches the active task hydration bundle once so
// the deterministic prior-decision checks (boundary denial reuse and recorded
// resolution reuse) can share it without issuing two hydration RPCs.
func (b projectDirtySideEffectBlocker) hydrateActiveTaskForSideEffects(ctx context.Context, input projectDirtySideEffectBlockInput) (TaskHydrationBundle, bool) {
	if b.client == nil || b.workspaceID == "" {
		return TaskHydrationBundle{}, false
	}
	taskID := firstNonEmpty(strings.TrimSpace(input.Branch.ActiveTaskID), strings.TrimSpace(input.Branch.ActiveClaimID))
	if taskID == "" {
		return TaskHydrationBundle{}, false
	}
	bundle, err := b.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID:      b.workspaceID,
		TaskID:           taskID,
		UpdatesLimit:     30,
		RelatedTaskLimit: 20,
		ArtifactLimit:    10,
		IncludeAllDocs:   boolPtr(false),
	})
	if err != nil {
		return TaskHydrationBundle{}, false
	}
	return bundle, true
}

func (b projectDirtySideEffectBlocker) existingBoundaryDenialForSideEffects(ctx context.Context, input projectDirtySideEffectBlockInput, sideEffects []AgentUpdateSideEffectV1, bundle TaskHydrationBundle, haveBundle bool) (map[string]any, bool) {
	if b.client == nil || b.workspaceID == "" || len(sideEffects) == 0 || !haveBundle {
		return nil, false
	}
	refs := projectBranchCommitSideEffectRefs(sideEffects)
	for _, update := range bundle.Updates {
		payload, ok := projectDirtySideEffectJSONMap(update.PayloadJSON)
		if !ok || !projectDirtySideEffectRefsIntersect(payload, refs) {
			continue
		}
		if !projectDirtySideEffectPayloadIsBoundaryDenial(update, payload) {
			continue
		}
		if !b.boundaryDenialStillApplies(ctx, input, payload) {
			continue
		}
		payload["existing_update_id"] = strings.TrimSpace(update.UpdateID)
		payload["existing_update_type"] = strings.TrimSpace(update.UpdateType)
		payload["denial_recorded"] = true
		return payload, true
	}
	return nil, false
}

func (b projectDirtySideEffectBlocker) boundaryDenialStillApplies(ctx context.Context, input projectDirtySideEffectBlockInput, denial map[string]any) bool {
	conflictingBranchID := strings.TrimSpace(stringMapValue(denial, "conflicting_branch_id"))
	if b.client == nil || b.workspaceID == "" || strings.TrimSpace(input.ProjectID) == "" || conflictingBranchID == "" {
		return true
	}
	coordination, err := b.client.GetProjectCoordination(ctx, b.workspaceID, input.ProjectID)
	if err != nil {
		return true
	}
	conflictingTaskID := firstNonEmpty(
		strings.TrimSpace(stringMapValue(denial, "conflicting_active_task_id")),
		strings.TrimSpace(stringMapValue(denial, "conflicting_task_id")),
	)
	conflictingOwnerID := strings.TrimSpace(stringMapValue(denial, "conflicting_owner_agent_id"))
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchID) != conflictingBranchID {
			continue
		}
		if projectBranchTerminalState(branch.Status) || !runtimeProjectBranchOwnsWriteScope(branch) {
			return false
		}
		if conflictingTaskID != "" && strings.TrimSpace(branch.ActiveTaskID) != "" && strings.TrimSpace(branch.ActiveTaskID) != conflictingTaskID {
			return false
		}
		if conflictingOwnerID != "" && strings.TrimSpace(branch.AgentID) != "" && strings.TrimSpace(branch.AgentID) != conflictingOwnerID {
			return false
		}
		return true
	}
	return false
}

func projectDirtySideEffectExistingDenialBlock(sourceTool string, input projectDirtySideEffectBlockInput, sideEffects []AgentUpdateSideEffectV1, denial map[string]any) *ToolResult {
	sideEffectRefs := projectBranchCommitSideEffectRefs(sideEffects)
	output := map[string]any{
		"status":                         "blocked",
		"source_tool":                    sourceTool,
		"gate_state":                     "blocked",
		"gate_type":                      "boundary_transition_denied",
		"boundary_transition_state":      firstNonEmpty(stringMapValue(denial, "boundary_transition_state"), "boundary_expansion_denied_overlap"),
		"transition_denied":              true,
		"denial_recorded":                true,
		"denial_reason":                  firstNonEmpty(stringMapValue(denial, "denial_reason"), "overlaps_live_owner_lane"),
		"preferred_transition":           firstNonEmpty(stringMapValue(denial, "preferred_transition"), "wait_or_split_existing_owner_lane"),
		"next_transition":                firstNonEmpty(stringMapValue(denial, "preferred_transition"), "wait_or_split_existing_owner_lane"),
		"allowed_next":                   projectDirtySideEffectAllowedNext(denial),
		"existing_update_id":             stringMapValue(denial, "existing_update_id"),
		"existing_update_type":           stringMapValue(denial, "existing_update_type"),
		"project_id":                     input.ProjectID,
		"branch_id":                      input.Branch.BranchID,
		"branch_name":                    input.Branch.BranchName,
		"active_task_id":                 input.Branch.ActiveTaskID,
		"dirty_paths":                    input.DirtyPaths,
		"side_effect_refs":               sideEffectRefs,
		"git_add_attempted":              false,
		"commit_created":                 false,
		"classification_task_created":    false,
		"classification_task_available":  false,
		"classification_task_abpc_class": "side_effect_classification",
		"do_not_retry":                   true,
		"do_not_retry_until":             "conflicting_boundary_route_changes",
		"mandatory_next_action":          "Do not retry the same boundary expansion or commit against this denied pathset. Wait for the conflicting lane, request split/merge/adopt, narrow the dirty materialization, or ask the conflicting owner to release scope.",
		"blocked_on": []map[string]string{{
			"kind":   "boundary_expansion_denied_overlap",
			"detail": "existing durable boundary-transition denial covers this dirty side-effect pathset",
		}},
	}
	for _, key := range []string{
		"boundary_transition_key",
		"conflicting_task_id",
		"conflicting_owner_agent_id",
		"conflicting_branch_id",
		"conflicting_active_task_id",
		"lead_task_id",
		"existing_task_id",
	} {
		if value, ok := denial[key]; ok {
			output[key] = value
		}
	}
	raw, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return &ToolResult{Output: sourceTool + " blocked because a durable boundary expansion denial already exists for this side effect", IsError: true}
	}
	return &ToolResult{Output: string(raw), IsError: true}
}

func projectDirtySideEffectJSONMap(raw string) (map[string]any, bool) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &payload); err != nil {
		return nil, false
	}
	return payload, true
}

func projectDirtySideEffectPayloadIsBoundaryDenial(update AgentUpdateRecord, payload map[string]any) bool {
	state := strings.TrimSpace(stringMapValue(payload, "boundary_transition_state"))
	if state != "boundary_expansion_denied_overlap" && state != "role_scope_conflict" {
		return false
	}
	if !projectDirtySideEffectBoolValue(payload, "transition_denied") && strings.TrimSpace(update.UpdateType) != "boundary_transition_denial" {
		return false
	}
	return true
}

func projectDirtySideEffectRefsIntersect(payload map[string]any, refs []string) bool {
	if len(refs) == 0 {
		return false
	}
	have := map[string]struct{}{}
	for _, ref := range projectDirtySideEffectPayloadRefs(payload) {
		if ref = strings.TrimSpace(ref); ref != "" {
			have[ref] = struct{}{}
		}
	}
	for _, ref := range refs {
		if _, ok := have[strings.TrimSpace(ref)]; ok {
			return true
		}
	}
	return false
}

func projectDirtySideEffectPayloadRefs(payload map[string]any) []string {
	refs := []string{}
	refs = append(refs, projectDirtySideEffectStringSlice(payload["side_effect_refs"])...)
	refs = append(refs, projectDirtySideEffectStringSlice(payload["side_effect_ref"])...)
	for _, rawEffect := range projectDirtySideEffectAnySlice(payload["side_effects"]) {
		effect, ok := rawEffect.(map[string]any)
		if !ok {
			continue
		}
		if ref := strings.TrimSpace(stringMapValue(effect, "side_effect_ref")); ref != "" {
			refs = append(refs, ref)
		}
	}
	return uniqueNonEmptyStrings(refs)
}

func projectDirtySideEffectAllowedNext(payload map[string]any) []string {
	values := projectDirtySideEffectStringSlice(payload["allowed_next"])
	if len(values) > 0 {
		return values
	}
	return []string{"wait_for_conflicting_lane_publication", "request_split", "request_merge_or_adopt", "reassign_downstream_after_owner", "ask_conflicting_owner_release_scope"}
}

func projectDirtySideEffectStringSlice(value any) []string {
	out := []string{}
	for _, item := range projectDirtySideEffectAnySlice(value) {
		switch typed := item.(type) {
		case string:
			if strings.TrimSpace(typed) != "" {
				out = append(out, strings.TrimSpace(typed))
			}
		default:
			text := strings.TrimSpace(fmt.Sprint(typed))
			if text != "" {
				out = append(out, text)
			}
		}
	}
	return out
}

func projectDirtySideEffectAnySlice(value any) []any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []any:
		return typed
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return []any{typed}
	default:
		return []any{typed}
	}
}

func projectDirtySideEffectBoolValue(payload map[string]any, key string) bool {
	value, ok := payload[key]
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

func (b projectDirtySideEffectBlocker) ensureSideEffectClassificationTask(ctx context.Context, input projectDirtySideEffectBlockInput, sideEffects []AgentUpdateSideEffectV1) projectSideEffectTaskRoute {
	if b.client == nil || b.workspaceID == "" || len(sideEffects) == 0 {
		return projectSideEffectTaskRoute{RouteType: "classification"}
	}
	taskID := projectBranchCommitSideEffectClassificationTaskID(b.workspaceID, input.ProjectID, input.Branch, sideEffects)
	requiresGate := false
	result, err := b.client.SubmitTask(ctx, TaskSubmitInput{
		WorkspaceID:         b.workspaceID,
		TaskID:              taskID,
		OwnerUserID:         firstNonEmpty(b.ownerUserID, b.agentID),
		Priority:            "high",
		Title:               "Classify side effects for " + firstNonEmpty(strings.TrimSpace(input.Branch.BranchName), strings.TrimSpace(input.Branch.BranchID), "project branch"),
		Description:         renderProjectBranchCommitSideEffectClassificationTask(input.ProjectID, input.Branch, input.DirtyPaths, input.Pathset, sideEffects),
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		TaskClass:           "INCIDENT",
		TaskClassSource:     "EXPLICIT",
		ProjectID:           input.ProjectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: &requiresGate,
		TaskRequirements: map[string]any{
			"schema":                   "artifact_bound_side_effect_classification_task.v1",
			"abpc_task_class":          "side_effect_classification",
			"source_kind":              "adapter:git",
			"source_tool":              firstNonEmpty(strings.TrimSpace(input.SourceTool), "artifact_checkpoint"),
			"branch_id":                input.Branch.BranchID,
			"branch_name":              input.Branch.BranchName,
			"owner_agent_id":           input.Branch.AgentID,
			"active_task_id":           input.Branch.ActiveTaskID,
			"current_write_scope":      input.Pathset,
			"dirty_paths":              input.DirtyPaths,
			"side_effect_refs":         projectBranchCommitSideEffectRefs(sideEffects),
			"allowed_decisions":        []string{"accept", "quarantine", "revert", "split_tension", "expand_boundary", "reassign", "request_verification"},
			"classification_authority": "strategic_owner_or_reviewer",
		},
		Tags:     []string{"side-effect-classification", "operational-boundary", "abpc", "project-coordination"},
		LinkedBy: b.agentID,
	})
	if err != nil {
		if isDuplicateTaskSubmitError(err) {
			status, found, statusErr := b.workspaceTaskStatus(ctx, taskID)
			if statusErr == nil && found && sideEffectResolutionTaskTerminal(status) {
				reconciliationID, reconciliationCreated, reconciliationWarning := b.ensureSideEffectReconciliationTask(ctx, input, sideEffects, taskID, status)
				warning := "classification task already terminal with status " + strings.ToUpper(strings.TrimSpace(status))
				if strings.TrimSpace(reconciliationWarning) != "" {
					warning += "; " + reconciliationWarning
				}
				return projectSideEffectTaskRoute{
					TaskID:             reconciliationID,
					Created:            reconciliationCreated,
					Warning:            warning,
					RouteType:          "reconciliation",
					TerminalTaskID:     taskID,
					TerminalTaskStatus: strings.ToUpper(strings.TrimSpace(status)),
				}
			}
			warning := "classification task already exists"
			if statusErr != nil {
				warning += "; status lookup failed: " + statusErr.Error()
			}
			return projectSideEffectTaskRoute{TaskID: taskID, RouteType: "classification", Warning: warning}
		}
		return projectSideEffectTaskRoute{RouteType: "classification", Warning: err.Error()}
	}
	if strings.TrimSpace(result.TaskID) != "" {
		taskID = strings.TrimSpace(result.TaskID)
	}
	return projectSideEffectTaskRoute{TaskID: taskID, Created: true, RouteType: "classification"}
}

func (b projectDirtySideEffectBlocker) workspaceTaskStatus(ctx context.Context, taskID string) (string, bool, error) {
	if b.client == nil || b.workspaceID == "" || strings.TrimSpace(taskID) == "" {
		return "", false, nil
	}
	tasks, err := b.client.ListTasks(ctx, b.workspaceID)
	if err != nil {
		return "", false, err
	}
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) == strings.TrimSpace(taskID) {
			return strings.TrimSpace(task.Status), true, nil
		}
	}
	return "", false, nil
}

func (b projectDirtySideEffectBlocker) ensureSideEffectReconciliationTask(ctx context.Context, input projectDirtySideEffectBlockInput, sideEffects []AgentUpdateSideEffectV1, terminalTaskID, terminalStatus string) (string, bool, string) {
	if b.client == nil || b.workspaceID == "" {
		return "", false, ""
	}
	taskID := projectBranchCommitSideEffectReconciliationTaskID(b.workspaceID, input.ProjectID, input.Branch, sideEffects, terminalTaskID)
	requiresGate := false
	result, err := b.client.SubmitTask(ctx, TaskSubmitInput{
		WorkspaceID:         b.workspaceID,
		TaskID:              taskID,
		OwnerUserID:         firstNonEmpty(b.ownerUserID, b.agentID),
		Priority:            "high",
		Title:               "Reconcile terminal side-effect route for " + firstNonEmpty(strings.TrimSpace(input.Branch.BranchName), strings.TrimSpace(input.Branch.BranchID), "project branch"),
		Description:         renderProjectBranchCommitSideEffectReconciliationTask(input, sideEffects, terminalTaskID, terminalStatus),
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		TaskClass:           "INCIDENT",
		TaskClassSource:     "EXPLICIT",
		ProjectID:           input.ProjectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: &requiresGate,
		TaskRequirements: map[string]any{
			"schema":                          "artifact_bound_side_effect_reconciliation_task.v1",
			"abpc_task_class":                 "side_effect_reconciliation",
			"source_kind":                     "adapter:git",
			"source_tool":                     firstNonEmpty(strings.TrimSpace(input.SourceTool), "artifact_checkpoint"),
			"branch_id":                       input.Branch.BranchID,
			"branch_name":                     input.Branch.BranchName,
			"owner_agent_id":                  input.Branch.AgentID,
			"active_task_id":                  input.Branch.ActiveTaskID,
			"current_write_scope":             input.Pathset,
			"dirty_paths":                     input.DirtyPaths,
			"side_effect_refs":                projectBranchCommitSideEffectRefs(sideEffects),
			"stale_classification_task_id":    terminalTaskID,
			"stale_classification_status":     strings.ToUpper(strings.TrimSpace(terminalStatus)),
			"required_terminal_route":         "repair_checkout_identity_or_terminalize_stale_checkout",
			"allowed_decisions":               []string{"repair_checkout_identity", "adopt_dirty_checkout_identity", "terminalize_stale_checkout", "request_verification", "split_tension"},
			"classification_authority":        "strategic_owner_or_reviewer",
			"must_not_delegate_terminal_task": true,
		},
		Tags:     []string{"side-effect-reconciliation", "terminal-route", "operational-boundary", "abpc", "project-coordination"},
		LinkedBy: b.agentID,
	})
	if err != nil {
		if isDuplicateTaskSubmitError(err) {
			return taskID, false, "reconciliation task already exists"
		}
		return "", false, err.Error()
	}
	if strings.TrimSpace(result.TaskID) != "" {
		taskID = strings.TrimSpace(result.TaskID)
	}
	return taskID, true, ""
}

func projectBranchCommitSideEffectReconciliationTaskID(workspaceID, projectID string, branch ProjectBranchRecord, sideEffects []AgentUpdateSideEffectV1, terminalTaskID string) string {
	refs := projectBranchCommitSideEffectRefs(sideEffects)
	refs = append(refs, strings.TrimSpace(terminalTaskID))
	refs = uniqueNonEmptyStrings(refs)
	return "task-side-effect-reconcile-" + shortRefHash(strings.Join([]string{
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		firstNonEmpty(strings.TrimSpace(branch.BranchID), strings.TrimSpace(branch.BranchName), "branch"),
		strings.Join(refs, "|"),
	}, "\x00"))
}

func renderProjectBranchCommitSideEffectReconciliationTask(input projectDirtySideEffectBlockInput, sideEffects []AgentUpdateSideEffectV1, terminalTaskID, terminalStatus string) string {
	var b strings.Builder
	b.WriteString("A side-effect classification route for this branch is already terminal, but the checkout still has dirty side effects and cannot safely continue through that closed task.\n\n")
	b.WriteString("- project_id: " + firstNonEmpty(strings.TrimSpace(input.ProjectID), "(unknown)") + "\n")
	b.WriteString("- source_tool: " + firstNonEmpty(strings.TrimSpace(input.SourceTool), "(unknown)") + "\n")
	b.WriteString("- branch_id: " + firstNonEmpty(strings.TrimSpace(input.Branch.BranchID), "(unknown)") + "\n")
	b.WriteString("- branch_name: " + firstNonEmpty(strings.TrimSpace(input.Branch.BranchName), "(unknown)") + "\n")
	b.WriteString("- active_task_id: " + firstNonEmpty(strings.TrimSpace(input.Branch.ActiveTaskID), "(unknown)") + "\n")
	b.WriteString("- stale_classification_task_id: " + firstNonEmpty(strings.TrimSpace(terminalTaskID), "(unknown)") + "\n")
	b.WriteString("- stale_classification_status: " + firstNonEmpty(strings.ToUpper(strings.TrimSpace(terminalStatus)), "(unknown)") + "\n")
	b.WriteString("- current_boundary: " + strings.Join(input.Pathset, ", ") + "\n")
	b.WriteString("- dirty_regions: " + strings.Join(input.DirtyPaths, ", ") + "\n\n")
	b.WriteString("Resolve the branch/checkout ownership route before retrying the blocked tool. Valid decisions: repair_checkout_identity, adopt_dirty_checkout_identity with evidence, terminalize_stale_checkout, request_verification, or split_tension.\n")
	b.WriteString("Do not delegate the terminal classification task again and do not silently widen scope or publish a branch whose checkout identity does not match durable evidence.\n\n")
	b.WriteString("Side-effect refs:\n")
	for _, effect := range sideEffects {
		b.WriteString("- " + firstNonEmpty(strings.TrimSpace(effect.SideEffectRef), "side-effect") +
			" | relation=" + firstNonEmpty(strings.TrimSpace(effect.BoundaryRelation), "unknown") +
			" | status=" + firstNonEmpty(strings.TrimSpace(effect.IntegrationStatus), "pending_classification") +
			" | region=" + firstNonEmpty(strings.TrimSpace(effect.RegionRef), "unknown") + "\n")
	}
	return b.String()
}

// --- TE-50: deterministic side-effect fast paths -------------------------------
//
// A deterministic pre-classifier that resolves common out-of-boundary write cases
// WITHOUT any LLM-bound classification task. It is a pure function over the
// (pathset, side effects) already computed by projectBranchCommitSideEffectsForBoundary,
// returning a typed decision: resolve-as-<existing decision> or escalate. Resolution
// is always routed through the existing side_effect_resolve machinery (never a
// hand-crafted state transition), so artifact_bound_side_effect.v1 refs and the
// resolution update/task surface stay identical to the LLM path.
//
// Fail-closed: any pathset that does not match a single homogeneous deterministic
// class falls through to the unchanged escalation path (one classification task).
// A mixed/ambiguous pathset NEVER fans out into more than one bounded path.

type sideEffectFastPathKind int

const (
	sideEffectFastPathEscalate sideEffectFastPathKind = iota
	sideEffectFastPathResolve
)

// sideEffectFastPathDecision is the typed result of the pure pre-classifier.
type sideEffectFastPathDecision struct {
	Kind     sideEffectFastPathKind
	Decision string // existing side_effect_resolve decision vocabulary when Kind == resolve
	Class    string // human/audit label for the deterministic class
}

// classifySideEffectFastPath is the pure pre-classifier. Given the branch write
// scope and the detected side effects, it returns a deterministic resolution when
// EVERY side effect maps to the SAME deterministic class, otherwise it escalates.
//
// Requiring a single homogeneous class is what enforces the no-mesh constraint:
// a mixed pathset cannot be partially fast-pathed and partially escalated, so it
// produces exactly one escalation rather than several bounded paths.
//
// Only the in-scope class is resolved deterministically here. Generated artifacts
// and run-bookkeeping paths are NOT resolved: untracked instances are already
// gitignored upstream (so they never surface as dirty paths), which means any such
// path that reaches this detector is a TRACKED file. Tracked generated/bookkeeping
// drift is exactly what the existing system escalates for human/owner classification,
// and TE-50 must leave that behavior unchanged. Class 4 (exact overlap with an
// already-recorded boundary decision) is handled separately by the prior-decision
// reuse paths (existingBoundaryDenialForSideEffects and reuseRecordedResolution),
// which return the SAME stored successor/decision rather than re-classifying.
func classifySideEffectFastPath(pathset []string, sideEffects []AgentUpdateSideEffectV1) sideEffectFastPathDecision {
	escalate := sideEffectFastPathDecision{Kind: sideEffectFastPathEscalate}
	if len(sideEffects) == 0 {
		// No materialized drift -> nothing for the pre-classifier to resolve.
		return escalate
	}
	class := ""
	for _, effect := range sideEffects {
		// Malformed payloads fail closed: a side effect missing its stable ref or
		// without a git path region cannot be deterministically resolved.
		if strings.TrimSpace(effect.SideEffectRef) == "" {
			return escalate
		}
		path := sideEffectFastPathGitPath(effect)
		if path == "" {
			return escalate
		}
		var pathClass string
		switch {
		case pathsetIsCoveredByScope([]string{path}, pathset):
			// Class 3: already inside the declared write scope (should not have been
			// flagged -- detectDirtySideEffects skips in-scope paths). If one still
			// reaches here it is an anomaly that belongs to the active lane, so route
			// it back via the lightweight reroute_to_active_lane decision rather than
			// mutating the boundary (accept/expand) or opening a classification task.
			pathClass = "in_scope"
		default:
			// Ambiguous (including tracked generated/bookkeeping drift): fall back to
			// the existing escalation path unchanged.
			return escalate
		}
		if class == "" {
			class = pathClass
		} else if class != pathClass {
			// Mixed deterministic classes are themselves ambiguous: fail closed to a
			// single escalation rather than spawning multiple bounded resolutions.
			return escalate
		}
	}
	decision := sideEffectFastPathDecisionForClass(class)
	if decision == "" {
		return escalate
	}
	return sideEffectFastPathDecision{Kind: sideEffectFastPathResolve, Decision: decision, Class: class}
}

func sideEffectFastPathDecisionForClass(class string) string {
	switch class {
	case "in_scope":
		return "reroute_to_active_lane"
	default:
		return ""
	}
}

// sideEffectFastPathGitPath extracts the adapter:git path a side effect describes.
// Returns "" when the side effect is not a single concrete git path region, which
// keeps the fast path fail-closed for anything it cannot reason about exactly.
func sideEffectFastPathGitPath(effect AgentUpdateSideEffectV1) string {
	const prefix = "region:adapter:git:path:"
	region := strings.TrimSpace(effect.RegionRef)
	if !strings.HasPrefix(region, prefix) {
		return ""
	}
	path := strings.TrimSpace(strings.TrimPrefix(region, prefix))
	if path == "" {
		return ""
	}
	// Only trust the path when the derived region refs corroborate it exactly.
	for _, derived := range effect.DerivedRegionRefs {
		if strings.TrimSpace(derived) == region {
			return path
		}
	}
	if len(effect.DerivedRegionRefs) == 0 {
		return path
	}
	return ""
}

// reuseRecordedResolution implements deterministic Class 4: when a prior
// side_effect_resolve decision already covers this exact pathset (same side-effect
// refs), reuse that recorded decision instead of spawning a fresh classification
// task. This is the core fix for the R51 motivation -- a single out-of-boundary
// write that was already classified must not be re-escalated across sessions.
//
// It is read-only and reuses the existing hydration machinery; it never fabricates
// a new state transition. If no recorded resolution covers the refs (or the lookup
// fails), it returns nil and the standard escalation path proceeds unchanged.
func (b projectDirtySideEffectBlocker) reuseRecordedResolution(input projectDirtySideEffectBlockInput, sideEffects []AgentUpdateSideEffectV1, bundle TaskHydrationBundle, haveBundle bool) *ToolResult {
	if len(sideEffects) == 0 || !haveBundle {
		return nil
	}
	refs := projectBranchCommitSideEffectRefs(sideEffects)
	for _, update := range bundle.Updates {
		if strings.TrimSpace(update.UpdateType) != "side_effect_resolution" {
			continue
		}
		payload, ok := projectDirtySideEffectJSONMap(update.PayloadJSON)
		if !ok {
			continue
		}
		if strings.TrimSpace(stringMapValue(payload, "schema")) != "artifact_bound_side_effect_resolution.v1" {
			continue
		}
		// Require the recorded resolution to cover EVERY current ref exactly, so an
		// ambiguous superset/subset never reuses a partial decision (fail closed).
		if !projectDirtySideEffectRefsCoverAll(payload, refs) {
			continue
		}
		decision := strings.TrimSpace(stringMapValue(payload, "decision"))
		if decision == "" {
			continue
		}
		return b.recordedResolutionReuseBlock(input, sideEffects, update, payload, decision)
	}
	return nil
}

func projectDirtySideEffectRefsCoverAll(payload map[string]any, refs []string) bool {
	if len(refs) == 0 {
		return false
	}
	have := map[string]struct{}{}
	for _, ref := range projectDirtySideEffectPayloadRefs(payload) {
		if ref = strings.TrimSpace(ref); ref != "" {
			have[ref] = struct{}{}
		}
	}
	for _, ref := range refs {
		if _, ok := have[strings.TrimSpace(ref)]; !ok {
			return false
		}
	}
	return true
}

func (b projectDirtySideEffectBlocker) recordedResolutionReuseBlock(input projectDirtySideEffectBlockInput, sideEffects []AgentUpdateSideEffectV1, update AgentUpdateRecord, payload map[string]any, decision string) *ToolResult {
	sourceTool := firstNonEmpty(strings.TrimSpace(input.SourceTool), "artifact_checkpoint")
	output := map[string]any{
		"status":                        "side_effect_resolution_reused",
		"source_tool":                   sourceTool,
		"gate_state":                    "resolved",
		"gate_type":                     "side_effect_resolution_reused",
		"reused_decision":               decision,
		"integration_status":            firstNonEmpty(strings.TrimSpace(stringMapValue(payload, "integration_status")), sideEffectResolveIntegrationStatus(decision)),
		"next_transition":               sideEffectResolveNextTransition(decision),
		"reused_resolution_update_id":   strings.TrimSpace(update.UpdateID),
		"reused_resolution_update_type": strings.TrimSpace(update.UpdateType),
		"deterministic_resolution":      true,
		"llm_classification_skipped":    true,
		"classification_task_created":   false,
		"classification_task_available": false,
		"project_id":                    input.ProjectID,
		"branch_id":                     input.Branch.BranchID,
		"branch_name":                   input.Branch.BranchName,
		"dirty_paths":                   input.DirtyPaths,
		"write_scope_json":              projectBranchCommitWriteScopeJSON(input.Pathset),
		"side_effect_refs":              projectBranchCommitSideEffectRefs(sideEffects),
		"git_add_attempted":             false,
		"commit_created":                false,
		"mandatory_next_action":         "this exact side-effect pathset was already classified; reuse the recorded decision instead of opening a new classification task",
	}
	for _, key := range []string{"followup_task_id", "classification_task_id", "successor_key", "resolution_saga_key"} {
		if value, ok := payload[key]; ok {
			output[key] = value
		}
	}
	out, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return &ToolResult{Output: sourceTool + " reused recorded side-effect decision " + decision, IsError: true}
	}
	return &ToolResult{Output: string(out), IsError: true}
}

// resolveSideEffectFastPath runs the pure pre-classifier and, when it yields a
// deterministic resolution, records it through the existing SideEffectResolveTool
// instead of enqueuing an LLM-bound classification task. Returns nil to fall
// through to the unchanged escalation path.
func (b projectDirtySideEffectBlocker) resolveSideEffectFastPath(ctx context.Context, input projectDirtySideEffectBlockInput, sideEffects []AgentUpdateSideEffectV1) *ToolResult {
	if b.client == nil || b.workspaceID == "" || b.agentID == "" {
		return nil
	}
	decision := classifySideEffectFastPath(input.Pathset, sideEffects)
	if decision.Kind != sideEffectFastPathResolve {
		return nil
	}
	refs := projectBranchCommitSideEffectRefs(sideEffects)
	if len(refs) == 0 {
		return nil
	}
	activeTaskID, activeClaimID := projectBranchCommitActiveTaskClaimIDs(input.Args, input.Branch)
	sourceTool := firstNonEmpty(strings.TrimSpace(input.SourceTool), "artifact_checkpoint")
	justification := fmt.Sprintf(
		"deterministic side-effect fast path (%s): %s resolved %d region(s) %s without LLM classification",
		decision.Class, sourceTool, len(input.DirtyPaths), strings.Join(input.DirtyPaths, ", "),
	)
	resolveArgs := map[string]any{
		"project_id":               input.ProjectID,
		"decision":                 decision.Decision,
		"side_effect_refs":         refs,
		"justification":            justification,
		"owner_agent_id":           firstNonEmpty(strings.TrimSpace(input.Branch.AgentID), b.agentID),
		"active_task_id":           firstNonEmpty(activeTaskID, activeClaimID),
		"branch_id":                strings.TrimSpace(input.Branch.BranchID),
		"branch_name":              strings.TrimSpace(input.Branch.BranchName),
		"dirty_paths":              input.DirtyPaths,
		"current_write_scope_json": projectBranchCommitWriteScopeJSON(input.Pathset),
	}
	resolveResult := NewSideEffectResolveTool(b.client, b.workspaceID, b.agentID, b.ownerUserID).Execute(ctx, resolveArgs)
	if resolveResult == nil {
		return nil
	}
	// Fail closed: if the resolution machinery itself could not record the decision,
	// do not fabricate a resolved state. Fall through to the standard escalation.
	if resolveResult.IsError {
		return nil
	}
	output := map[string]any{
		"status":                        "side_effect_fast_path_resolved",
		"source_tool":                   sourceTool,
		"gate_state":                    "resolved",
		"gate_type":                     "side_effect_fast_path",
		"fast_path_class":               decision.Class,
		"fast_path_decision":            decision.Decision,
		"integration_status":            sideEffectResolveIntegrationStatus(decision.Decision),
		"next_transition":               sideEffectResolveNextTransition(decision.Decision),
		"classification_task_created":   false,
		"classification_task_available": false,
		"deterministic_resolution":      true,
		"llm_classification_skipped":    true,
		"project_id":                    input.ProjectID,
		"branch_id":                     input.Branch.BranchID,
		"branch_name":                   input.Branch.BranchName,
		"dirty_paths":                   input.DirtyPaths,
		"write_scope_json":              projectBranchCommitWriteScopeJSON(input.Pathset),
		"side_effect_refs":              refs,
		"git_add_attempted":             false,
		"commit_created":                false,
		"mandatory_next_action":         "deterministic side-effect resolution recorded; do not enqueue a classification task for this pathset",
		"side_effect_resolve_output":    resolveResult.Output,
	}
	out, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return &ToolResult{Output: sourceTool + " deterministically resolved out-of-boundary dirty paths via side_effect_resolve " + decision.Decision, IsError: true}
	}
	// Still an error result: the source mutation (commit) did not proceed, so the
	// caller must re-evaluate. The resolution is recorded; the blocker is cleared
	// deterministically rather than waiting on an LLM classification lane.
	return &ToolResult{Output: string(out), IsError: true}
}
