package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

type TaskSubmitTool struct {
	client           *RhizomeClient
	workspaceID      string
	agentID          string
	ownerUserID      string
	coordinationMode string
	runtimeBinding   func() AgentRuntimeBinding
}

func NewTaskSubmitTool(client *RhizomeClient, workspaceID, agentID, ownerUserID string) *TaskSubmitTool {
	return &TaskSubmitTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		ownerUserID: strings.TrimSpace(ownerUserID),
	}
}

func (t *TaskSubmitTool) WithCoordinationMode(mode string) *TaskSubmitTool {
	if t != nil {
		t.coordinationMode = normalizeCoordinationMode(mode)
	}
	return t
}

func (t *TaskSubmitTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *TaskSubmitTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *TaskSubmitTool) Name() string { return "task_submit" }

func (t *TaskSubmitTool) Description() string {
	return "Create a new Rhizome workspace task when decomposing active work or when an autonomous no-task heartbeat discovers a concrete gap. Use this instead of asking the operator to manually split work. After a strategic lead opens a project to IMPLEMENTATION, use this to create concrete product/code tasks with semantic fit, tool, acceptance-criteria, and write-scope hints so agents can self-select them from the frontier."
}

func (t *TaskSubmitTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"task_id": map[string]any{
				"type":        "string",
				"description": "Optional stable task id. Leave blank to let the tool generate one.",
			},
			"title": map[string]any{
				"type":        "string",
				"description": "Short actionable task title.",
			},
			"description": map[string]any{
				"type":        "string",
				"description": "Task context, desired outcome, dependencies, and review/test expectations.",
			},
			"priority": map[string]any{
				"type":        "string",
				"description": "Task priority: low, normal, high, or critical.",
			},
			"task_kind": map[string]any{
				"type":        "string",
				"description": "Task kind, usually EXECUTION or COORDINATION.",
			},
			"project_id": map[string]any{
				"type":        "string",
				"description": "Optional project id to attach this task to.",
			},
			"project_lane": map[string]any{
				"type":        "string",
				"description": "Optional project lane hint such as implementation, review, docs, or coordination.",
			},
			"requires_project_gate": map[string]any{
				"type":        "boolean",
				"description": "Whether completion should wait for a project-level coordination gate. Project implementation/code lanes are hard-gated automatically.",
			},
			"task_template": map[string]any{
				"type":        "string",
				"description": "Task template, usually generic.",
			},
			"dependency_task_ids": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Legacy hard upstream task ids. For same-project implementation siblings, use advisory_dependency_task_ids unless this task truly cannot start until the dependency resolves.",
			},
			"hard_dependency_task_ids": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Explicit hard upstream task ids. These become server BLOCKS links; use only when this task cannot start until the listed tasks resolve.",
			},
			"advisory_dependency_task_ids": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Non-blocking related task ids for preferred sequencing or shared context. These are stored in task_requirements and do not block frontier claims.",
			},
			"write_scope_hints": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional intended write scope hints, such as repository path globs. Project implementation tasks should expose the expected touched areas for self-selection/admission; frontend/backend scaffold tasks should include root config globs such as package.json, tsconfig*.json, and vite.config.* when they own those files.",
			},
			"required_work_modes": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional fit hints such as implementation, review, strategy, synthesis, validation, or design.",
			},
			"preferred_skills": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional skill/profile hints used by agents during frontier self-selection.",
			},
			"preferred_tools": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional tool hints such as browser, chrome-devtools, figma, git, or test runner names.",
			},
			"required_tools": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional hard tool expectations. Until server-side readiness is fresh, agents treat these as fit evidence rather than an automatic claim gate.",
			},
			"task_requirements": map[string]any{
				"type":        "object",
				"description": "Optional structured durable task requirements. Use for non-prose lineage such as patch queue queue_id/item_id/branch_id/head_sha, source refs, and required transitions.",
			},
			"graph": map[string]any{
				"type":        "object",
				"description": "Optional DAG graph payload using nodes with node_id, type, and depends_on.",
			},
			"tags": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional routing tags such as frontend, backend, review, integration.",
			},
			"auto_delegate": map[string]any{
				"type":        "boolean",
				"description": "For implementation-lane project tasks, optionally queue a delegate_task agent_request to the first suggested suitable non-self agent. Defaults to false; normal routing is frontier self-selection.",
			},
		},
		"required": []string{"title", "description"},
	}
}

func (t *TaskSubmitTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "task_submit is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	title := strings.TrimSpace(stringArg(args, "title"))
	description := strings.TrimSpace(stringArg(args, "description"))
	if title == "" {
		return &ToolResult{Output: "title is required", IsError: true}
	}
	if description == "" {
		return &ToolResult{Output: "description is required", IsError: true}
	}
	taskID := strings.TrimSpace(stringArg(args, "task_id"))
	if taskID == "" {
		taskID = newRuntimeID("task")
	}
	priority := firstNonEmpty(strings.ToLower(strings.TrimSpace(stringArg(args, "priority"))), "normal")
	taskKind := firstNonEmpty(strings.ToUpper(strings.TrimSpace(stringArg(args, "task_kind"))), "EXECUTION")
	taskTemplate := firstNonEmpty(strings.ToLower(strings.TrimSpace(stringArg(args, "task_template"))), "generic")
	projectID := strings.TrimSpace(stringArg(args, "project_id"))
	projectLane := strings.TrimSpace(stringArg(args, "project_lane"))
	taskKind, projectLane = normalizeTaskSubmitTaskKindAndLane(taskKind, projectLane)
	requiresProjectGate := optionalBoolArg(args, "requires_project_gate")
	legacyDependencyTaskIDs := firstNonEmptyStringSlice(stringSliceArg(args, "dependency_task_ids"), stringSliceArg(args, "dependencies"))
	explicitHardDependencyTaskIDs := stringSliceArg(args, "hard_dependency_task_ids")
	advisoryDependencyTaskIDs := stringSliceArg(args, "advisory_dependency_task_ids")
	dependencyTaskIDs := uniqueTrimmedCSVStrings(legacyDependencyTaskIDs, explicitHardDependencyTaskIDs)
	writeScopeHints := firstNonEmptyStringSlice(stringSliceArg(args, "write_scope_hints"), stringSliceArg(args, "write_scope"))
	tags := stringSliceArg(args, "tags")
	var ignoredWriteScopeHints []string
	if invalid := invalidProjectWriteScopeHints(writeScopeHints); len(invalid) > 0 {
		if taskSubmitCanIgnoreProseWriteScopeHints(taskKind, projectLane, title, description, tags, writeScopeHints) {
			ignoredWriteScopeHints = invalid
		} else {
			return &ToolResult{Output: fmt.Sprintf("write_scope_hints must be repository-relative path globs, not prose labels: %s", strings.Join(invalid, ", ")), IsError: true}
		}
	}
	writeScopeHints = normalizeProjectWriteScopePaths(writeScopeHints)
	graph := objectArg(args, "graph")
	taskRequirements := taskSubmitRequirementsFromArgs(args, writeScopeHints)
	var canonicalizeErr *ToolResult
	projectID, taskKind, projectLane, tags, taskRequirements, canonicalizeErr = t.canonicalizeAuthorityRepairSubmission(taskID, projectID, taskKind, projectLane, tags, taskRequirements)
	if canonicalizeErr != nil {
		return canonicalizeErr
	}
	if gate := t.activeClaimFollowThroughGate(taskID, projectID, taskKind, projectLane, tags, taskRequirements); gate != nil {
		return gate
	}
	projectGateEnforced := taskSubmitRequiresHardProjectGate(projectID, taskKind, projectLane)
	if projectGateEnforced {
		requiresGate := true
		requiresProjectGate = &requiresGate
	}
	writeScopeHints = taskSubmitSemanticWriteScopeHints(taskID, title, description, taskKind, projectID, projectLane, tags, taskRequirements, writeScopeHints)
	if len(writeScopeHints) > 0 {
		taskRequirements["write_scope_hints"] = append([]string(nil), writeScopeHints...)
	}
	if len(ignoredWriteScopeHints) > 0 {
		taskRequirements["ignored_write_scope_hints"] = ignoredWriteScopeHints
		taskRequirements["write_scope_hints_normalization"] = "ignored_prose_for_non_mutating_task"
	}
	patchQueueIdentity := taskSubmitPatchQueueIdentityFromInput(title, description, projectLane, tags, taskRequirements)
	patchQueueIdentity.ProjectID = firstNonEmpty(patchQueueIdentity.ProjectID, projectID)
	taskRequirements = taskSubmitRequirementsWithPatchQueueIdentity(taskRequirements, patchQueueIdentity)
	taskRequirements = taskSubmitRequirementsWithCanonicalIntegrationConvergence(taskRequirements, title, description, projectLane, tags)
	taskRequirements = taskSubmitRequirementsWithProjectRoleScopeAuthorityTransition(taskRequirements, taskID, taskKind, projectID, projectLane, tags)
	if err := validateTaskSubmitImplementationDeliverable(taskKind, projectLane, title, description); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	workspaceTasks, workspaceTasksOK := t.listWorkspaceTasksForSubmit(ctx)
	if existing, ok := findExactTaskIDInList(workspaceTasks, workspaceTasksOK, taskID); ok {
		return taskSubmitExistingExactTaskResult(existing)
	}
	if existing, ok := findPatchQueueDuplicateTaskInList(workspaceTasks, workspaceTasksOK, taskID, title, description, projectID, projectLane, tags, patchQueueIdentity); ok {
		return taskSubmitExistingPatchQueueTaskResult(existing, patchQueueIdentity)
	}
	if gate := t.productLaneLivenessGate(ctx, workspaceTasks, workspaceTasksOK, taskID, title, description, projectID, taskKind, projectLane, tags, taskRequirements); gate != nil {
		return gate
	}
	requestedPatchQueueKind := firstNonEmpty(patchQueueIdentity.Kind, taskSubmitPatchQueueTaskKindFromLaneAndText(projectLane, title, description, tags))
	canonicalRetry := taskSubmitPatchQueueRequestIsCanonicalRetry(taskID, title, description, tags)
	skipVisualReceiptLookup := taskSubmitBoolArg(args, "skip_visual_receipt_lookup") || taskSubmitRequirementBool(taskRequirements, "skip_visual_receipt_lookup")
	if !canonicalRetry && !skipVisualReceiptLookup && taskSubmitPatchQueueKindNeedsVisualEvidenceReceipt(requestedPatchQueueKind) {
		if receipt, ok, err := patchQueueVisualEvidenceReceiptForIdentity(ctx, t.client, t.workspaceID, patchQueueIdentity); err != nil {
			return &ToolResult{Output: fmt.Sprintf("task_submit failed checking visual evidence receipt for patch queue candidate: %v", err), IsError: true}
		} else if ok {
			return taskSubmitExistingPatchQueueVisualEvidenceResult(receipt, patchQueueIdentity)
		}
	}
	if existing, score, ok := findSimilarOpenTaskInList(workspaceTasks, workspaceTasksOK, taskID, title, description, projectID); ok {
		payload := map[string]any{
			"created":          false,
			"task_submit_gate": "similar_active_task",
			"existing_task_id": strings.TrimSpace(existing.TaskID),
			"existing_title":   strings.TrimSpace(existing.Title),
			"existing_status":  strings.TrimSpace(existing.Status),
			"similarity":       score,
			"guidance":         "Do not create a duplicate task. Claim or contribute to the existing task, publish missing evidence in its task document, or create a narrower non-overlapping subtask.",
		}
		taskSubmitAnnotateExistingTaskPayload(payload, existing)
		raw, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("task_submit skipped: similar active task %s already exists; do not create a duplicate task", existing.TaskID)}
		}
		return &ToolResult{Output: string(raw)}
	}
	filteredDependencies := filterTaskSubmitDependencies(dependencyTaskIDs, explicitHardDependencyTaskIDs, workspaceTasks, workspaceTasksOK, projectID, taskKind, projectLane)
	dependencyTaskIDs = filteredDependencies.HardDependencyTaskIDs
	advisoryDependencyTaskIDs = uniqueTrimmedCSVStrings(advisoryDependencyTaskIDs, filteredDependencies.AdvisoryDependencyTaskIDs)
	if len(advisoryDependencyTaskIDs) > 0 {
		taskRequirements = taskSubmitRequirementsWithAdvisoryDependencies(taskRequirements, advisoryDependencyTaskIDs, filteredDependencies.DemotedDependencyTaskIDs)
	}
	result, err := t.client.SubmitTask(ctx, TaskSubmitInput{
		WorkspaceID:         t.workspaceID,
		TaskID:              taskID,
		OwnerUserID:         firstNonEmpty(t.ownerUserID, t.agentID),
		Priority:            priority,
		Title:               title,
		Description:         description,
		TaskKind:            taskKind,
		TaskTemplate:        taskTemplate,
		ProjectID:           projectID,
		ProjectLane:         projectLane,
		RequiresProjectGate: requiresProjectGate,
		DependencyTaskIDs:   dependencyTaskIDs,
		RelatedTaskIDs:      filteredDependencies.RelatedTaskIDs,
		WriteScopeHints:     writeScopeHints,
		TaskRequirements:    taskRequirements,
		Graph:               graph,
		Tags:                tags,
		LinkedBy:            t.agentID,
	})
	if err != nil {
		if gateResult := taskSubmitIntegratedAcceptanceGateResult(err); gateResult != nil {
			return gateResult
		}
		// OE-4 DEFERRED: this error result's text is pattern-matched by the patch-queue followup
		// tool to decide its noop/hydrate path, so even a text-only enrichment changes downstream
		// behavior. Left as-is; safe enrichment requires run validation of the followup coupling.
		return &ToolResult{Output: fmt.Sprintf("task_submit failed for %s: %v", taskID, err), IsError: true}
	}
	createdTaskID := firstNonEmpty(result.TaskID, taskID)
	docTaskRequirements := firstNonEmptyTaskSubmitRequirements(result.TaskRequirementsJSON, taskRequirements)
	docKey := "task." + createdTaskID
	docTitle := "Task Brief - " + title
	docContent := renderTaskSubmitCanonicalDoc(TaskSubmitDocInput{
		TaskID:              createdTaskID,
		Title:               title,
		Description:         description,
		Priority:            priority,
		TaskKind:            taskKind,
		TaskTemplate:        taskTemplate,
		ProjectID:           projectID,
		ProjectLane:         projectLane,
		RequiresProjectGate: requiresProjectGate,
		DependencyTaskIDs:   dependencyTaskIDs,
		WriteScopeHints:     writeScopeHints,
		TaskRequirements:    docTaskRequirements,
		Tags:                tags,
		CreatedBy:           t.agentID,
	})
	docSHA, docErr := t.client.PutDoc(ctx, WorkspaceDocPutInput{
		WorkspaceID: t.workspaceID,
		DocKey:      docKey,
		Title:       docTitle,
		Content:     docContent,
		UpdatedBy:   t.agentID,
	})
	payload := map[string]any{
		"task_id":      createdTaskID,
		"workspace_id": firstNonEmpty(result.WorkspaceID, t.workspaceID),
		"status":       firstNonEmpty(result.Status, "PENDING"),
		"created_by":   t.agentID,
		"task_kind":    taskKind,
		"canonical_doc": map[string]any{
			"doc_key": docKey,
			"title":   docTitle,
		},
	}
	if projectID != "" {
		payload["project_id"] = projectID
	}
	if projectLane != "" {
		payload["project_lane"] = projectLane
	}
	if requiresProjectGate != nil {
		payload["requires_project_gate"] = *requiresProjectGate
	}
	if projectGateEnforced {
		payload["project_gate_enforced"] = true
		payload["gate_guidance"] = "Project implementation/code lanes are hard-gated until the product contract, implementation plan, repo readiness, strategic lead, and implementation phase gates are satisfied."
	}
	if normalizeCoordinationMode(t.coordinationMode) == CoordinationModeTrustFirst {
		payload["coordination_mode"] = CoordinationModeTrustFirst
		if !projectGateEnforced {
			payload["gate_guidance"] = "trust_first may keep non-code coordination gates advisory, but project implementation/code lanes remain hard-gated."
		}
	}
	if len(dependencyTaskIDs) > 0 {
		payload["dependency_task_ids"] = dependencyTaskIDs
	}
	if len(advisoryDependencyTaskIDs) > 0 {
		payload["advisory_dependency_task_ids"] = advisoryDependencyTaskIDs
	}
	if len(filteredDependencies.RelatedTaskIDs) > 0 {
		payload["related_task_ids"] = filteredDependencies.RelatedTaskIDs
	}
	if len(filteredDependencies.DemotedDependencyTaskIDs) > 0 {
		payload["demoted_dependency_task_ids"] = filteredDependencies.DemotedDependencyTaskIDs
	}
	if len(filteredDependencies.DroppedDependencyTaskIDs) > 0 {
		payload["dropped_dependency_task_ids"] = filteredDependencies.DroppedDependencyTaskIDs
	}
	if len(filteredDependencies.DemotedDependencyTaskIDs) > 0 || len(filteredDependencies.DroppedDependencyTaskIDs) > 0 {
		payload["dependency_guidance"] = "Server dependency_task_ids create hard BLOCKS links. Running project root/strategy tasks are context anchors, and same-project implementation sibling sequencing is advisory unless supplied through hard_dependency_task_ids."
	}
	if len(writeScopeHints) > 0 {
		payload["write_scope_hints"] = writeScopeHints
	}
	if len(docTaskRequirements) > 0 {
		payload["task_requirements"] = docTaskRequirements
	}
	if docErr != nil {
		payload["canonical_doc_status"] = "FAILED"
		payload["canonical_doc_error"] = docErr.Error()
		raw, marshalErr := json.MarshalIndent(payload, "", "  ")
		if marshalErr != nil {
			return &ToolResult{Output: fmt.Sprintf("created task %s but failed to create canonical doc %s: %v; do not retry task_submit blindly, repair with workspace_doc_put", createdTaskID, docKey, docErr), IsError: true}
		}
		return &ToolResult{Output: string(raw) + "\ncanonical task doc creation failed; do not retry task_submit blindly, repair with workspace_doc_put", IsError: true}
	}
	payload["canonical_doc_status"] = "SAVED"
	payload["canonical_doc"].(map[string]any)["sha"] = docSHA
	if len(result.Suggested) > 0 {
		payload["suggested_agents"] = result.Suggested
	}
	if taskSubmitIsImplementationLane(taskKind, projectLane) {
		delegatePrompt := taskSubmitDelegateRequestPrompt(createdTaskID)
		payload["selection_guidance"] = "Task is visible for autonomous agent.work.next frontier self-selection. Use agent_request request_kind=delegate_task with this exact task_id only as an optional targeted wake for a semantically suitable agent."
		payload["delegate_request_template"] = delegatePrompt
		if taskSubmitAutoDelegateEnabled(args) {
			payload["auto_delegate"] = t.autoDelegateImplementationTask(ctx, createdTaskID, delegatePrompt, result.Suggested)
		}
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("created task %s", firstNonEmpty(result.TaskID, taskID))}
	}
	return &ToolResult{Output: string(raw)}
}

func (t *TaskSubmitTool) activeClaimFollowThroughGate(taskID, projectID, taskKind, projectLane string, tags []string, requirements map[string]any) *ToolResult {
	if t == nil || t.runtimeBinding == nil {
		return nil
	}
	binding := t.runtimeBinding()
	activeTaskID := strings.TrimSpace(binding.TaskID)
	if activeTaskID == "" || strings.TrimSpace(taskID) == activeTaskID {
		return nil
	}
	hasProjectClaim := strings.TrimSpace(binding.ProjectID) != "" &&
		(strings.TrimSpace(binding.ClaimCheckoutID) != "" ||
			strings.TrimSpace(binding.ClaimBranchID) != "" ||
			strings.TrimSpace(binding.ClaimWriteScopeJSON) != "")
	if !hasProjectClaim {
		return nil
	}
	if taskSubmitAuthorityRepairKind(taskID) != "" {
		return nil
	}
	requested := WorkspaceTaskRecord{
		TaskID:               strings.TrimSpace(taskID),
		Title:                "",
		Description:          "",
		TaskKind:             strings.TrimSpace(taskKind),
		ProjectID:            strings.TrimSpace(projectID),
		ProjectLane:          strings.TrimSpace(projectLane),
		TaskRequirementsJSON: taskSubmitRequirementsJSON(requirements),
		Tags:                 tags,
	}
	if authorityTransitionTaskLooksDedicated(requested) {
		return nil
	}
	requestedProjectID := firstNonEmpty(strings.TrimSpace(projectID), strings.TrimSpace(binding.ProjectID))
	payload := map[string]any{
		"created":             false,
		"task_submit_gate":    "active_claim_follow_through",
		"active_task_id":      activeTaskID,
		"requested_task_id":   strings.TrimSpace(taskID),
		"project_id":          requestedProjectID,
		"claim_checkout_id":   strings.TrimSpace(binding.ClaimCheckoutID),
		"claim_branch_id":     strings.TrimSpace(binding.ClaimBranchID),
		"claim_write_scope":   strings.TrimSpace(binding.ClaimWriteScopeJSON),
		"required_transition": "implement_current_claim_or_publish_terminal_blocker",
		"guidance":            "This runtime is already bound to an admitted project claim/checkout/branch. Do not create successor, follow-up, split, or switch-target tasks from this active implementation claim. Work in the active claim checkout, commit/review-ready the current branch when product evidence exists, or publish a terminal blocker/result on the active task.",
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: "task_submit refused: active project claim requires follow-through on the current task", IsError: true}
	}
	return &ToolResult{Output: string(raw), IsError: true}
}

func taskSubmitRequirementsJSON(requirements map[string]any) string {
	if len(requirements) == 0 {
		return ""
	}
	raw, err := json.Marshal(requirements)
	if err != nil {
		return ""
	}
	return string(raw)
}

func taskSubmitIntegratedAcceptanceGateResult(err error) *ToolResult {
	if err == nil {
		return nil
	}
	text := strings.TrimSpace(err.Error())
	if !strings.Contains(text, "task_submit_gate=integrated_acceptance_contract_satisfied") {
		return nil
	}
	fields := map[string]string{}
	for _, part := range strings.Split(text, ":") {
		part = strings.TrimSpace(part)
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		fields[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	payload := map[string]any{
		"created":          false,
		"task_submit_gate": "integrated_acceptance_contract_satisfied",
		"guidance":         "The requested implementation task's acceptance contract is already satisfied by durable integrated project state. Do not recreate or claim it; open a new task only for an uncovered acceptance delta.",
	}
	for _, key := range []string{"existing_task_id", "existing_status", "required_transition", "queue_id", "item_id", "branch_id", "head_sha"} {
		if value := strings.TrimSpace(fields[key]); value != "" {
			payload[key] = value
		}
	}
	raw, marshalErr := json.MarshalIndent(payload, "", "  ")
	if marshalErr != nil {
		return &ToolResult{Output: "task_submit skipped: integrated acceptance contract is already satisfied; open only an uncovered acceptance delta"}
	}
	return &ToolResult{Output: string(raw)}
}

func firstNonEmptyTaskSubmitRequirements(raw string, fallback map[string]any) map[string]any {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return fallback
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil || len(parsed) == 0 {
		return fallback
	}
	return parsed
}

func taskSubmitDelegateRequestPrompt(taskID string) string {
	return fmt.Sprintf("Please inspect task %s through your normal planner/frontier loop. Claim it only if the deliverable fits your role, tools, current workload, and project acceptance criteria; if you claim it, materialize the needed checkout/branch, implement the product/code deliverable, run focused verification, commit with project_branch_commit push=true when appropriate, and publish project_branch_review_ready evidence.", strings.TrimSpace(taskID))
}

func taskSubmitRequirementsFromArgs(args map[string]any, writeScopeHints []string) map[string]any {
	requirements := objectArg(args, "task_requirements")
	if requirements == nil {
		requirements = map[string]any{}
	}
	if values := stringSliceArg(args, "required_work_modes"); len(values) > 0 {
		requirements["required_work_modes"] = values
	}
	if values := stringSliceArg(args, "preferred_skills"); len(values) > 0 {
		requirements["preferred_skills"] = values
	}
	if values := stringSliceArg(args, "preferred_tools"); len(values) > 0 {
		requirements["preferred_tools"] = values
	}
	if values := stringSliceArg(args, "required_tools"); len(values) > 0 {
		requirements["required_tools"] = values
	}
	if len(writeScopeHints) > 0 {
		requirements["write_scope_hints"] = append([]string(nil), writeScopeHints...)
	}
	if len(requirements) > 0 {
		requirements["schema"] = "task_requirements.v1"
	}
	return requirements
}

func (t *TaskSubmitTool) canonicalizeAuthorityRepairSubmission(taskID, projectID, taskKind, projectLane string, tags []string, requirements map[string]any) (string, string, string, []string, map[string]any, *ToolResult) {
	repairKind := taskSubmitAuthorityRepairKind(taskID)
	if repairKind == "" {
		return projectID, taskKind, projectLane, tags, requirements, nil
	}
	resolvedProjectID, err := resolveProjectToolActiveProjectID("task_submit authority repair", t.workspaceID, projectID, t.runtimeBinding)
	if err != nil {
		payload := map[string]any{
			"created":                   false,
			"task_submit_gate":          "authority_repair_requires_project",
			"task_id":                   strings.TrimSpace(taskID),
			"authority_repair_kind":     repairKind,
			"required_transition_shape": "project-bound dedicated authority carrier",
			"guidance":                  "Pass project_id or run task_submit from an active project-bound task. Authority repair carriers must be project-bound at creation so claim admission and terminalization share the same durable context.",
			"error":                     err.Error(),
		}
		raw, marshalErr := json.MarshalIndent(payload, "", "  ")
		if marshalErr != nil {
			return projectID, taskKind, projectLane, tags, requirements, &ToolResult{Output: "task_submit rejected authority repair task: project_id is required", IsError: true}
		}
		return projectID, taskKind, projectLane, tags, requirements, &ToolResult{Output: string(raw), IsError: true}
	}
	projectID = resolvedProjectID
	taskKind = "COORDINATION"
	if requirements == nil {
		requirements = map[string]any{}
	}
	requirements["project_id"] = projectID
	switch repairKind {
	case "project_role_scope":
		if !strings.EqualFold(strings.TrimSpace(projectLane), "coordination") && !runtimeProjectLaneIsStrategy(projectLane) {
			projectLane = "coordination"
		}
		tags = uniqueTrimmedCSVStrings(tags, []string{"project-role-scope", "authority-transition", "strategic-lead", "coordination", "blocker-unblock"})
		requirements = taskSubmitRequirementsWithProjectRoleScopeAuthorityTransition(requirements, taskID, taskKind, projectID, projectLane, tags)
	case "project_claim_repair":
		if !strings.EqualFold(strings.TrimSpace(projectLane), "coordination") && !runtimeProjectLaneIsStrategy(projectLane) {
			projectLane = "strategy"
		}
		tags = uniqueTrimmedCSVStrings(tags, []string{"project-claim-repair", "strategic-lead", "coordination", "blocker-unblock"})
		requirements["schema"] = "project_claim_repair_task_v1"
		requirements["project_claim_repair"] = true
		requirements["task_requirement_kind"] = "project_claim_repair"
		requirements["authority_transition_kind"] = "project_claim_repair"
		if strings.TrimSpace(taskSubmitRequirementString(requirements, "required_transition")) == "" {
			requirements["required_transition"] = "project_claim_repair_receipt"
		}
	}
	return projectID, taskKind, projectLane, tags, requirements, nil
}

func taskSubmitAuthorityRepairKind(taskID string) string {
	taskID = strings.ToLower(strings.TrimSpace(taskID))
	switch {
	case strings.HasPrefix(taskID, "task-role-scope-"):
		return "project_role_scope"
	case strings.HasPrefix(taskID, "task-project-claim-repair-"):
		return "project_claim_repair"
	default:
		return ""
	}
}

func taskSubmitSemanticWriteScopeHints(taskID, title, description, taskKind, projectID, projectLane string, tags []string, requirements map[string]any, writeScopeHints []string) []string {
	writeScopeHints = normalizeProjectWriteScopePaths(writeScopeHints)
	if len(writeScopeHints) == 0 {
		return writeScopeHints
	}
	raw, _ := json.Marshal(requirements)
	task := WorkspaceTaskRecord{
		TaskID:               strings.TrimSpace(taskID),
		Title:                strings.TrimSpace(title),
		Description:          strings.TrimSpace(description),
		TaskKind:             strings.TrimSpace(taskKind),
		ProjectID:            strings.TrimSpace(projectID),
		ProjectLane:          strings.TrimSpace(projectLane),
		TaskRequirementsJSON: string(raw),
		WriteScopeHints:      writeScopeHints,
		Tags:                 append([]string(nil), tags...),
	}
	narrowed := projectLuaAcceptanceWriteScopeHints(task)
	if len(narrowed) == 0 || !projectLuaAcceptanceScopeShouldNarrow(writeScopeHints, narrowed) {
		return writeScopeHints
	}
	return narrowed
}

func taskSubmitRequirementsWithAdvisoryDependencies(requirements map[string]any, advisoryDependencyTaskIDs, demotedDependencyTaskIDs []string) map[string]any {
	advisoryDependencyTaskIDs = uniqueTrimmedCSVStrings(advisoryDependencyTaskIDs)
	demotedDependencyTaskIDs = uniqueTrimmedCSVStrings(demotedDependencyTaskIDs)
	if len(advisoryDependencyTaskIDs) == 0 && len(demotedDependencyTaskIDs) == 0 {
		return requirements
	}
	if requirements == nil {
		requirements = map[string]any{}
	}
	requirements["schema"] = "task_requirements.v1"
	if len(advisoryDependencyTaskIDs) > 0 {
		requirements["advisory_dependency_task_ids"] = advisoryDependencyTaskIDs
	}
	if len(demotedDependencyTaskIDs) > 0 {
		requirements["demoted_dependency_task_ids"] = demotedDependencyTaskIDs
		requirements["dependency_contract"] = "legacy_dependency_task_ids_demoted_for_parallel_project_implementation"
	}
	return requirements
}

type taskSubmitPatchQueueIdentity struct {
	ProjectID string
	QueueID   string
	ItemID    string
	BranchID  string
	HeadSHA   string
	Kind      string
}

var (
	taskSubmitPatchQueueIDPattern     = regexp.MustCompile(`(?i)\bpatchq-[A-Za-z0-9_.-]+`)
	taskSubmitPatchQueueItemPattern   = regexp.MustCompile(`(?i)\bpatchitem-[A-Za-z0-9_.-]+`)
	taskSubmitPatchQueueBranchPattern = regexp.MustCompile(`(?i)\bprojbranch-[A-Za-z0-9_.-]+`)
	taskSubmitPatchQueueHeadPattern   = regexp.MustCompile(`(?i)\b(?:expected[_ -]?head[_ -]?sha|head[_ -]?sha|expected[_ -]?head|head)\b\s*[:=]?\s*` + "`?" + `([0-9a-f]{7,40})` + "`?")
)

func taskSubmitPatchQueueIdentityFromInput(title, description, projectLane string, tags []string, requirements map[string]any) taskSubmitPatchQueueIdentity {
	identity := taskSubmitPatchQueueIdentity{
		ProjectID: firstNonEmpty(taskSubmitRequirementString(requirements, "project_id"), taskSubmitRequirementString(requirements, "candidate_project_id")),
		QueueID:   firstNonEmpty(taskSubmitRequirementString(requirements, "queue_id"), taskSubmitRequirementString(requirements, "patch_queue_id")),
		ItemID:    firstNonEmpty(taskSubmitRequirementString(requirements, "item_id"), taskSubmitRequirementString(requirements, "patch_item_id")),
		BranchID:  firstNonEmpty(taskSubmitRequirementString(requirements, "branch_id"), taskSubmitRequirementString(requirements, "target_branch_id"), taskSubmitRequirementString(requirements, "candidate_branch_id")),
		HeadSHA:   firstNonEmpty(taskSubmitRequirementString(requirements, "head_sha"), taskSubmitRequirementString(requirements, "expected_head_sha"), taskSubmitRequirementString(requirements, "candidate_head_sha")),
		Kind:      taskSubmitRequirementString(requirements, "patch_queue_task_kind"),
	}
	text := strings.Join([]string{title, description, strings.Join(tags, "\n")}, "\n")
	taskLike := strings.ToLower(strings.Join([]string{projectLane, text}, "\n"))
	if !containsAnySignal(taskLike, []string{"patch_queue", "patch-queue", "patch queue", "patchq", "patchitem", "blocked integration candidate", "visual acceptance", "visual evidence", "browser evidence", "source-fidelity evidence", "rhizome_visual_acceptance_v1"}) &&
		!projectTaskLooksCanonicalIntegrationConvergence(projectLane, title, description, tags, requirements) {
		return identity
	}
	taskRecord := WorkspaceTaskRecord{Title: title, Description: description, Tags: tags}
	identity.QueueID = firstNonEmpty(identity.QueueID, projectPatchQueueFollowupTaskFieldValue(taskRecord, "queue_id", "queue-id", "patch_queue_id", "patch-queue-id"), taskSubmitFirstRegexRef(taskSubmitPatchQueueIDPattern, text))
	identity.ItemID = firstNonEmpty(identity.ItemID, projectPatchQueueFollowupTaskFieldValue(taskRecord, "item_id", "item-id", "patch_item_id", "patch-item-id"), taskSubmitFirstRegexRef(taskSubmitPatchQueueItemPattern, text))
	identity.BranchID = firstNonEmpty(identity.BranchID, projectPatchQueueFollowupTaskFieldValue(taskRecord, "branch_id", "branch-id", "target_branch_id", "target-branch-id", "candidate_branch_id", "candidate-branch-id"), taskSubmitFirstRegexRef(taskSubmitPatchQueueBranchPattern, text))
	identity.HeadSHA = firstNonEmpty(identity.HeadSHA, projectPatchQueueFollowupTaskFieldValue(taskRecord, "head_sha", "head-sha", "expected_head_sha", "expected-head-sha", "candidate_head_sha", "candidate-head-sha", "head"), taskSubmitFirstHeadRef(text))
	if strings.TrimSpace(identity.Kind) == "" {
		switch {
		case taskSubmitLooksPatchQueueClaimStewardship(title, description, tags, requirements):
			identity.Kind = "claim_stewardship"
		case projectTaskLooksCanonicalIntegrationConvergence(projectLane, title, description, tags, requirements):
			identity.Kind = "integration"
		case containsAnySignal(taskLike, []string{"revision", "revise", "unblock integration candidate", "repair"}):
			identity.Kind = "revision"
		case containsAnySignal(taskLike, []string{"review"}):
			identity.Kind = "review"
		case containsAnySignal(taskLike, []string{"validation", "validate", "visual", "browser", "source-fidelity", "evidence"}):
			identity.Kind = "validation"
		}
	}
	return taskSubmitNormalizePatchQueueIdentity(identity)
}

func taskSubmitRequirementsWithCanonicalIntegrationConvergence(requirements map[string]any, title, description, projectLane string, tags []string) map[string]any {
	if taskSubmitRequirementsDescribePatchQueueNonIntegrationTask(requirements) {
		if requirements != nil {
			if strings.EqualFold(strings.TrimSpace(taskSubmitRequirementString(requirements, "required_tool")), "project_patch_queue_integrate") {
				delete(requirements, "required_tool")
			}
			if strings.EqualFold(strings.TrimSpace(taskSubmitRequirementString(requirements, "required_transition")), "project_patch_queue_integrate_then_full_product_verify") {
				delete(requirements, "required_transition")
			}
			if strings.EqualFold(strings.TrimSpace(taskSubmitRequirementString(requirements, "integration_contract")), "canonical_integration_before_full_product_validation.v1") {
				delete(requirements, "integration_contract")
			}
		}
		return requirements
	}
	if !projectTaskLooksCanonicalIntegrationConvergence(projectLane, title, description, tags, requirements) {
		return requirements
	}
	if requirements == nil {
		requirements = map[string]any{}
	}
	requirements["schema"] = "task_requirements.v1"
	if strings.TrimSpace(taskSubmitRequirementString(requirements, "required_tool")) == "" {
		requirements["required_tool"] = "project_patch_queue_integrate"
	}
	if strings.TrimSpace(taskSubmitRequirementString(requirements, "required_transition")) == "" {
		requirements["required_transition"] = "project_patch_queue_integrate_then_full_product_verify"
	}
	if strings.TrimSpace(taskSubmitRequirementString(requirements, "patch_queue_task_kind")) == "" {
		requirements["patch_queue_task_kind"] = "integration"
	}
	if strings.TrimSpace(taskSubmitRequirementString(requirements, "integration_contract")) == "" {
		requirements["integration_contract"] = "canonical_integration_before_full_product_validation.v1"
	}
	return requirements
}

func projectTaskLooksCanonicalIntegrationConvergence(projectLane, title, description string, tags []string, requirements map[string]any) bool {
	if taskSubmitRequirementsDescribePatchQueueNonIntegrationTask(requirements) {
		return false
	}
	if taskSubmitLooksPatchQueueClaimStewardship(title, description, tags, requirements) {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(taskSubmitRequirementString(requirements, "required_tool")), "project_patch_queue_integrate") ||
		strings.EqualFold(strings.TrimSpace(taskSubmitRequirementString(requirements, "required_transition")), "project_patch_queue_integrate_then_full_product_verify") ||
		strings.EqualFold(strings.TrimSpace(taskSubmitRequirementString(requirements, "patch_queue_task_kind")), "integration") {
		return true
	}
	if taskSubmitTagsContainPatchQueueIntegrationIdentity(tags) {
		return true
	}
	lane := strings.ToLower(strings.TrimSpace(projectLane))
	if lane != "integration" && lane != "integrator" && lane != "integrate" {
		return false
	}
	return taskSubmitRequirementsContainPatchQueueIdentity(requirements) || taskSubmitRequirementsDescribeCanonicalIntegrationValidation(requirements)
}

func taskSubmitRequirementsContainPatchQueueIdentity(requirements map[string]any) bool {
	if len(requirements) == 0 {
		return false
	}
	for _, key := range []string{"patch_queue_task_identity", "queue_id", "item_id", "branch_id", "head_sha", "candidate_branch_id", "candidate_head_sha"} {
		if strings.TrimSpace(taskSubmitRequirementString(requirements, key)) != "" {
			return true
		}
	}
	return false
}

func taskSubmitRequirementsDescribeCanonicalIntegrationValidation(requirements map[string]any) bool {
	if len(requirements) == 0 || taskSubmitRequirementsDescribePatchQueueNonIntegrationTask(requirements) {
		return false
	}
	modes := strings.ToLower(strings.Join(stringSliceFromAny(requirements["required_work_modes"]), "\n"))
	evidence := strings.ToLower(strings.Join(stringSliceFromAny(requirements["evidence_needed"]), "\n"))
	hasValidationMode := containsAnySignal(modes, []string{"validation", "review"})
	hasBranchHeadEvidence := containsAnySignal(evidence, []string{"branch/head", "branch head", "head sha", "head_sha", "checkout path", "exact branch"})
	hasBuildOrTestEvidence := containsAnySignal(evidence, []string{"go build", "go test", "build/test", "smoke"})
	return hasValidationMode && hasBranchHeadEvidence && hasBuildOrTestEvidence
}

func taskSubmitTagsContainPatchQueueIntegrationIdentity(tags []string) bool {
	hasPatchQueue := false
	hasIntegration := false
	for _, tag := range tags {
		normalized := strings.ToLower(strings.TrimSpace(tag))
		normalized = strings.ReplaceAll(normalized, "_", "-")
		switch normalized {
		case "patch-queue-integration", "project-patch-queue-integration", "canonical-integration", "integration-convergence":
			return true
		case "patch-queue", "patchq", "patch-queue-task":
			hasPatchQueue = true
		case "integration", "integrator", "integrate":
			hasIntegration = true
		}
	}
	return hasPatchQueue && hasIntegration
}

func taskSubmitRequirementsDescribePatchQueueNonIntegrationTask(requirements map[string]any) bool {
	if len(requirements) == 0 {
		return false
	}
	kind := strings.ToLower(strings.TrimSpace(taskSubmitRequirementString(requirements, "patch_queue_task_kind")))
	return kind != "" && kind != "integration"
}

func taskSubmitLooksPatchQueueClaimStewardship(title, description string, tags []string, requirements map[string]any) bool {
	kind := strings.ToLower(strings.TrimSpace(taskSubmitRequirementString(requirements, "patch_queue_task_kind")))
	kind = strings.ReplaceAll(kind, "-", "_")
	switch kind {
	case "claim_stewardship", "claim_steward", "claim_repair":
		return true
	}
	requiredTransition := strings.ToLower(strings.TrimSpace(taskSubmitRequirementString(requirements, "required_transition")))
	if strings.Contains(requiredTransition, "project_patch_queue_lifecycle") &&
		(taskSubmitRequirementsContainPatchQueueIdentity(requirements) ||
			containsAnySignal(strings.ToLower(strings.Join([]string{title, description, strings.Join(tags, "\n")}, "\n")), []string{"patch queue", "patch_queue", "patch-queue"})) {
		return true
	}
	text := strings.ToLower(strings.Join([]string{title, description, strings.Join(tags, "\n")}, "\n"))
	if !containsAnySignal(text, []string{"patch queue", "patch_queue", "patch-queue", "patchq", "patchitem"}) {
		return false
	}
	return containsAnySignal(text, []string{
		"project_patch_queue_claim_stewardship_available",
		"claim stewardship",
		"claim-stewardship",
		"claim_stewardship",
		"queue stewardship",
		"queue-stewardship",
		"queue_stewardship",
		"claimed-decision",
		"claimed decision",
		"claimed patch queue",
	})
}

func taskSubmitRequirementsWithPatchQueueIdentity(requirements map[string]any, identity taskSubmitPatchQueueIdentity) map[string]any {
	identity = taskSubmitNormalizePatchQueueIdentity(identity)
	if !taskSubmitPatchQueueIdentityActionable(identity) {
		return requirements
	}
	if requirements == nil {
		requirements = map[string]any{}
	}
	requirements["schema"] = "task_requirements.v1"
	requirements["patch_queue_task_identity"] = "rhizome_patch_queue_task_identity.v1"
	if identity.ProjectID != "" {
		requirements["project_id"] = identity.ProjectID
	}
	if identity.Kind != "" {
		requirements["patch_queue_task_kind"] = identity.Kind
	}
	if identity.QueueID != "" {
		requirements["queue_id"] = identity.QueueID
	}
	if identity.ItemID != "" {
		requirements["item_id"] = identity.ItemID
	}
	if identity.BranchID != "" {
		requirements["branch_id"] = identity.BranchID
	}
	if identity.HeadSHA != "" {
		requirements["head_sha"] = identity.HeadSHA
	}
	return requirements
}

func taskSubmitRequirementString(requirements map[string]any, key string) string {
	if len(requirements) == 0 {
		return ""
	}
	raw, ok := requirements[key]
	if !ok || raw == nil {
		return ""
	}
	switch value := raw.(type) {
	case string:
		return normalizeProjectToolRef(value)
	case []string:
		if len(value) > 0 {
			return normalizeProjectToolRef(value[0])
		}
	case []any:
		if len(value) > 0 {
			return normalizeProjectToolRef(fmt.Sprint(value[0]))
		}
	default:
		return normalizeProjectToolRef(fmt.Sprint(value))
	}
	return ""
}

func taskSubmitRequirementBool(requirements map[string]any, key string) bool {
	if len(requirements) == 0 {
		return false
	}
	raw, ok := requirements[key]
	if !ok || raw == nil {
		return false
	}
	switch value := raw.(type) {
	case bool:
		return value
	case string:
		normalized := strings.ToLower(strings.TrimSpace(value))
		return normalized == "true" || normalized == "1" || normalized == "yes"
	default:
		return false
	}
}

func taskSubmitBoolArg(args map[string]any, key string) bool {
	raw := optionalBoolArg(args, key)
	return raw != nil && *raw
}

func taskSubmitFirstRegexRef(pattern *regexp.Regexp, text string) string {
	if pattern == nil {
		return ""
	}
	match := pattern.FindString(text)
	return normalizeProjectToolRef(match)
}

func taskSubmitFirstHeadRef(text string) string {
	match := taskSubmitPatchQueueHeadPattern.FindStringSubmatch(text)
	if len(match) < 2 {
		return ""
	}
	return normalizeProjectToolRef(match[1])
}

func taskSubmitNormalizePatchQueueIdentity(identity taskSubmitPatchQueueIdentity) taskSubmitPatchQueueIdentity {
	identity.ProjectID = normalizeProjectToolRef(identity.ProjectID)
	identity.QueueID = normalizeProjectToolRef(identity.QueueID)
	identity.ItemID = normalizeProjectToolRef(identity.ItemID)
	identity.BranchID = normalizeProjectToolRef(identity.BranchID)
	identity.HeadSHA = strings.ToLower(normalizeProjectToolRef(identity.HeadSHA))
	identity.Kind = strings.ToLower(strings.TrimSpace(identity.Kind))
	return identity
}

func taskSubmitPatchQueueIdentityActionable(identity taskSubmitPatchQueueIdentity) bool {
	identity = taskSubmitNormalizePatchQueueIdentity(identity)
	if identity.QueueID != "" && identity.ItemID != "" {
		return true
	}
	if identity.BranchID != "" && identity.HeadSHA != "" {
		return true
	}
	if identity.ItemID != "" && identity.HeadSHA != "" {
		return true
	}
	if identity.ProjectID != "" && identity.HeadSHA != "" {
		return true
	}
	return false
}

func taskSubmitRequirementsWithSourceDocKeys(requirements map[string]any, sourceDocKeys []string) map[string]any {
	sourceDocKeys = uniqueTrimmedCSVStrings(sourceDocKeys)
	if len(sourceDocKeys) == 0 {
		return requirements
	}
	if requirements == nil {
		requirements = map[string]any{}
	}
	existing := sourceDocKeysFromMap(requirements)
	if len(existing) > 0 {
		sourceDocKeys = uniqueTrimmedCSVStrings(append(existing, sourceDocKeys...))
	}
	requirements["source_doc_keys"] = sourceDocKeys
	requirements["spec_fidelity_contract"] = "source_artifact_fidelity.v1"
	requirements["schema"] = "task_requirements.v1"
	return requirements
}

func taskSubmitAutoDelegateEnabled(args map[string]any) bool {
	if explicit := optionalBoolArg(args, "auto_delegate"); explicit != nil {
		return *explicit
	}
	return false
}

func (t *TaskSubmitTool) autoDelegateImplementationTask(ctx context.Context, taskID, prompt string, suggested []string) map[string]any {
	target := firstTaskSubmitDelegationTarget(suggested, t.agentID)
	if target == "" {
		return map[string]any{
			"status":  "SKIPPED",
			"reason":  "no suggested suitable non-self agent returned by task.submit",
			"task_id": strings.TrimSpace(taskID),
		}
	}
	payload, err := json.Marshal(map[string]any{
		"prompt":       strings.TrimSpace(prompt),
		"request_kind": agentRequestKindDelegateTask,
		"task_id":      strings.TrimSpace(taskID),
		"source":       "task_submit.auto_delegate",
	})
	if err != nil {
		return map[string]any{
			"status":  "FAILED",
			"target":  target,
			"task_id": strings.TrimSpace(taskID),
			"error":   err.Error(),
		}
	}
	created, err := t.client.RequestAgent(ctx, AgentRequestInput{
		WorkspaceID: t.workspaceID,
		FromAgentID: t.agentID,
		ToAgentID:   target,
		Method:      "model.ask",
		PayloadJSON: string(payload),
		TimeoutSec:  300,
	})
	if err != nil {
		return map[string]any{
			"status":  "FAILED",
			"target":  target,
			"task_id": strings.TrimSpace(taskID),
			"error":   err.Error(),
		}
	}
	return map[string]any{
		"status":     "QUEUED",
		"target":     target,
		"task_id":    strings.TrimSpace(taskID),
		"request_id": created.RequestID,
	}
}

func firstTaskSubmitDelegationTarget(suggested []string, selfAgentID string) string {
	selfAgentID = strings.TrimSpace(selfAgentID)
	for _, agentID := range suggested {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" || agentID == selfAgentID {
			continue
		}
		return agentID
	}
	return ""
}

type TaskSubmitDocInput struct {
	TaskID              string
	Title               string
	Description         string
	Priority            string
	TaskKind            string
	TaskTemplate        string
	ProjectID           string
	ProjectLane         string
	RequiresProjectGate *bool
	DependencyTaskIDs   []string
	WriteScopeHints     []string
	TaskRequirements    map[string]any
	Tags                []string
	CreatedBy           string
}

func renderTaskSubmitCanonicalDoc(input TaskSubmitDocInput) string {
	var b strings.Builder
	b.WriteString("# Task Brief - ")
	b.WriteString(strings.TrimSpace(input.Title))
	b.WriteString("\n\n")
	b.WriteString("- task_id: ")
	b.WriteString(strings.TrimSpace(input.TaskID))
	b.WriteString("\n")
	b.WriteString("- created_by: ")
	b.WriteString(strings.TrimSpace(input.CreatedBy))
	b.WriteString("\n")
	b.WriteString("- priority: ")
	b.WriteString(strings.TrimSpace(input.Priority))
	b.WriteString("\n")
	b.WriteString("- task_kind: ")
	b.WriteString(strings.TrimSpace(input.TaskKind))
	b.WriteString("\n")
	b.WriteString("- task_template: ")
	b.WriteString(strings.TrimSpace(input.TaskTemplate))
	b.WriteString("\n")
	if strings.TrimSpace(input.ProjectID) != "" {
		b.WriteString("- project_id: ")
		b.WriteString(strings.TrimSpace(input.ProjectID))
		b.WriteString("\n")
	}
	if strings.TrimSpace(input.ProjectLane) != "" {
		b.WriteString("- project_lane: ")
		b.WriteString(strings.TrimSpace(input.ProjectLane))
		b.WriteString("\n")
	}
	if input.RequiresProjectGate != nil {
		b.WriteString("- requires_project_gate: ")
		b.WriteString(fmt.Sprint(*input.RequiresProjectGate))
		b.WriteString("\n")
	}
	if len(input.DependencyTaskIDs) > 0 {
		b.WriteString("- dependency_task_ids: ")
		b.WriteString(strings.Join(input.DependencyTaskIDs, ", "))
		b.WriteString("\n")
	}
	if len(input.WriteScopeHints) > 0 {
		b.WriteString("- write_scope_hints: ")
		b.WriteString(strings.Join(input.WriteScopeHints, ", "))
		b.WriteString("\n")
	}
	if len(input.TaskRequirements) > 0 {
		b.WriteString("- task_requirements_schema: task_requirements.v1\n")
	}
	if len(input.Tags) > 0 {
		b.WriteString("- tags: ")
		b.WriteString(strings.Join(input.Tags, ", "))
		b.WriteString("\n")
	}
	b.WriteString("\n## Description\n")
	b.WriteString(strings.TrimSpace(input.Description))
	if len(input.TaskRequirements) > 0 {
		if raw, err := json.MarshalIndent(input.TaskRequirements, "", "  "); err == nil {
			b.WriteString("\n\n## Task Fit Requirements\n")
			b.WriteString("```json\n")
			b.Write(raw)
			b.WriteString("\n```\n")
		}
	}
	if strings.TrimSpace(input.ProjectID) != "" {
		b.WriteString("\n\n## Spec Fidelity\n")
		if sourceDocKeys := sourceDocKeysFromMap(input.TaskRequirements); len(sourceDocKeys) > 0 {
			b.WriteString("- Source doc keys are non-droppable intent anchors for this task: ")
			b.WriteString(strings.Join(sourceDocKeys, ", "))
			b.WriteString(". Read them before implementation or review when available in hydration.\n")
			b.WriteString("- Compare this task against `")
			b.WriteString(projectSourceRequirementsTraceDocKey(input.ProjectID))
			b.WriteString("` or the project planning doc section `rhizome_source_requirements_trace_v1`; block SPEC_DRIFT when a runnable artifact satisfies only a compressed adjacent flow.\n")
		} else {
			b.WriteString("- If `")
			b.WriteString(projectSourceRefsDocKey(input.ProjectID))
			b.WriteString("` is present in hydration, treat its source_doc_keys as non-droppable intent anchors for this task.\n")
			b.WriteString("- Compare this task against `")
			b.WriteString(projectSourceRequirementsTraceDocKey(input.ProjectID))
			b.WriteString("` or the project planning doc section `rhizome_source_requirements_trace_v1`; block SPEC_DRIFT when a runnable artifact satisfies only a compressed adjacent flow.\n")
		}
		b.WriteString("- Before implementation or review, read the root task/operator brief and the project acceptance criteria doc when present, especially project.")
		b.WriteString(strings.TrimSpace(input.ProjectID))
		b.WriteString(".acceptance_criteria.\n")
		b.WriteString("- Also inspect the Product Contract and Critical Plan Review when present: project.")
		b.WriteString(strings.TrimSpace(input.ProjectID))
		b.WriteString(".product_contract and project.")
		b.WriteString(strings.TrimSpace(input.ProjectID))
		b.WriteString(".plan_review. If they are missing or vague before implementation work, create/refine them or route a bounded planning review.\n")
		b.WriteString("- This task should name which acceptance-criteria IDs it covers in its evidence or review notes; if criteria are missing or vague, create/refine the checklist before judging completion.\n")
		b.WriteString("- Implementation/review evidence should mention the core_user_promise/happy_path step served by this lane and any adjacent wrong product/non-goal it must avoid.\n")
		b.WriteString("- A runnable artifact is not enough when required criteria remain unimplemented; create concrete follow-up tasks for unmet criteria.\n")
	}
	b.WriteString("\n\n## Coordination Contract\n")
	b.WriteString("- This canonical task document was created by task_submit at `task.<task_id>`.\n")
	b.WriteString("- Start from this document before claiming missing-context blockers.\n")
	b.WriteString("- If the description names other workspace docs, read those docs before implementation or review.\n")
	b.WriteString("- Record implementation evidence, review feedback, blockers, and final synthesis in durable workspace docs.\n")
	if taskSubmitIsImplementationLane(input.TaskKind, input.ProjectLane) {
		b.WriteString("- Implementation-lane completion requires product/code changes in the project checkout, a bounded project_branch_commit, and project_branch_review_ready evidence. Branch/checkout/admission evidence alone is not this task's deliverable.\n")
	}
	return strings.TrimSpace(b.String())
}

func normalizeTaskSubmitTaskKindAndLane(taskKind, projectLane string) (string, string) {
	normalizedKind := strings.ToUpper(strings.TrimSpace(taskKind))
	normalizedLane := strings.TrimSpace(projectLane)
	if normalizedKind == "" {
		return "EXECUTION", normalizedLane
	}
	switch normalizedKind {
	case "EXECUTION", "COORDINATION":
		return normalizedKind, normalizedLane
	case "INTAKE", "SPEC", "PLANNING", "IMPLEMENTATION", "REVIEW", "INTEGRATION", "VALIDATION", "OPS":
		if normalizedLane == "" {
			normalizedLane = strings.ToLower(normalizedKind)
		}
		return "EXECUTION", normalizedLane
	default:
		return normalizedKind, normalizedLane
	}
}

func validateTaskSubmitImplementationDeliverable(taskKind, projectLane, title, description string) error {
	if !taskSubmitIsImplementationLane(taskKind, projectLane) {
		return nil
	}
	if taskSubmitLooksLikePublicationOnlyImplementationTask(title, description) {
		return fmt.Errorf("task_submit rejected publication/provenance-only implementation task: implementation tasks must first name concrete product/code changes to build, revise, or test. Publish provenance, branch evidence, and review-ready status after an owned checkout has real product/code changes")
	}
	if !taskSubmitLooksLikeLaneAdmissionTask(title, description) {
		return nil
	}
	return fmt.Errorf("task_submit rejected implementation-lane meta task: implementation tasks must name the product/code artifact to build, revise, test, or review. Do not create tasks whose deliverable is merely opening an implementation lane, claiming admission, registering checkout/branch evidence, or satisfying project gates; those are runtime/tooling steps after a real product task is claimed")
}

func taskSubmitIsImplementationLane(taskKind, projectLane string) bool {
	return strings.EqualFold(strings.TrimSpace(taskKind), "EXECUTION") &&
		strings.EqualFold(strings.TrimSpace(projectLane), "implementation")
}

func taskSubmitRequiresHardProjectGate(projectID, taskKind, projectLane string) bool {
	if strings.TrimSpace(projectID) == "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(taskKind), "EXECUTION") {
		return false
	}
	return runtimeProjectLaneRequiresImplementationGate(projectLane)
}

func taskSubmitCanIgnoreProseWriteScopeHints(taskKind, projectLane, title, description string, tags, hints []string) bool {
	if strings.EqualFold(strings.TrimSpace(taskKind), "EXECUTION") &&
		runtimeProjectLaneRequiresImplementationGate(projectLane) {
		return false
	}
	if len(normalizeProjectWriteScopePaths(hints)) > 0 {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(projectLane))
	if runtimeProjectLaneBypassesImplementationGate(lane) {
		return true
	}
	text := strings.ToLower(strings.Join([]string{title, description, lane, strings.Join(tags, " ")}, "\n"))
	return containsAnySignal(text, []string{
		"workspace doc",
		"workspace docs",
		"docs only",
		"documentation",
		"evidence",
		"validation",
		"verify",
		"review",
		"visual",
		"browser",
		"blocker packet",
		"no repo mutation",
		"non-mutating",
	})
}

func taskSubmitLooksLikePublicationOnlyImplementationTask(title, description string) bool {
	text := strings.ToLower(strings.Join([]string{title, description}, "\n"))
	titleText := strings.ToLower(strings.TrimSpace(title))
	if !taskSubmitHasPublicationMetaMarker(text, titleText) {
		return false
	}
	return !taskSubmitHasConcreteProductBuildIntent(text)
}

func taskSubmitHasPublicationMetaMarker(text, titleText string) bool {
	markers := []string{
		"candidate provenance",
		"publish provenance",
		"provenance status",
		"publication/provenance",
		"review-ready evidence",
		"review ready evidence",
		"browser acceptance",
		"qa validation",
		"exact runnable candidate",
		"runnable candidate provenance",
		"branch/head",
		"head sha",
		"checkout path",
	}
	for _, marker := range markers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return strings.Contains(titleText, "publish") && strings.Contains(titleText, "candidate")
}

func taskSubmitHasConcreteProductBuildIntent(text string) bool {
	buildMarkers := []string{
		"implement ",
		"build ",
		"code ",
		"edit ",
		"wire ",
		"develop ",
		"repair ",
		"revise ",
		"test coverage",
		"unit test",
		"integration test",
		"e2e",
		"editor",
		"auth",
		"profile",
		"article",
		"persistence",
		"route",
		"component",
		"localstorage",
		"indexeddb",
		"shortcut",
		"rich-text",
		"rich text",
		"ui",
		"ux",
		"data model",
	}
	for _, marker := range buildMarkers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func taskSubmitLooksLikeLaneAdmissionTask(title, description string) bool {
	text := strings.ToLower(strings.Join([]string{title, description}, "\n"))
	titleText := strings.ToLower(strings.TrimSpace(title))
	if strings.Contains(titleText, "implementation lane") ||
		strings.Contains(titleText, "implementation-shaped") ||
		strings.Contains(titleText, "open implementation") ||
		strings.Contains(titleText, "unlock implementation") ||
		strings.Contains(titleText, "start implementation lane") ||
		strings.Contains(titleText, "set up implementation lane") ||
		strings.Contains(titleText, "prepare implementation lane") ||
		strings.Contains(titleText, "открыть implementation") ||
		strings.Contains(titleText, "открыть lane") ||
		strings.Contains(titleText, "открыть implementation lane") {
		return true
	}
	metaMarkers := []string{
		"implementation lane",
		"implementation-shaped",
		"open implementation",
		"unlock implementation",
		"start implementation lane",
		"set up implementation lane",
		"prepare implementation lane",
		"create implementation task",
		"claim admission",
		"lane admission",
		"gate-opening",
		"gate opening",
		"project gate",
		"satisfy project gates",
		"satisfy gates",
		"project_checkout_materialize",
		"project_branch_review_ready",
		"checkout/branch",
		"checkout branch",
		"branch evidence",
		"register checkout",
		"registered checkout",
		"materialize checkout",
		"claim/rebind",
		"claim admission",
		"local implementation workspace",
		"открыть implementation",
		"зарегистрировать checkout",
		"зарегистрировать branch",
		"зарегистрировать вет",
		"перейти к локальной реализации",
	}
	score := 0
	for _, marker := range metaMarkers {
		if strings.Contains(text, marker) {
			score++
		}
	}
	if score >= 2 {
		return true
	}
	if score == 1 && strings.Contains(text, "expected result") && strings.Contains(text, "claimed implementation task") {
		return true
	}
	if score == 1 && strings.Contains(text, "ожидаемый результат") && strings.Contains(text, "claimed implementation task") {
		return true
	}
	return false
}

func (t *TaskSubmitTool) findSimilarOpenTask(ctx context.Context, requestedTaskID, title, description, projectID string) (WorkspaceTaskRecord, float64, bool) {
	tasks, ok := t.listWorkspaceTasksForSubmit(ctx)
	return findSimilarOpenTaskInList(tasks, ok, requestedTaskID, title, description, projectID)
}

func (t *TaskSubmitTool) listWorkspaceTasksForSubmit(ctx context.Context) ([]WorkspaceTaskRecord, bool) {
	if t == nil || t.client == nil || strings.TrimSpace(t.workspaceID) == "" {
		return nil, false
	}
	tasks, err := t.client.ListTasks(ctx, t.workspaceID)
	if err != nil {
		return nil, false
	}
	return tasks, true
}

func (t *TaskSubmitTool) productLaneLivenessGate(ctx context.Context, tasks []WorkspaceTaskRecord, tasksOK bool, taskID, title, description, projectID, taskKind, projectLane string, tags []string, taskRequirements map[string]any) *ToolResult {
	if !taskSubmitIsGenericCoordinationSplit(taskID, title, description, projectID, taskKind, projectLane, tags, taskRequirements) {
		return nil
	}
	openProductTasks := taskSubmitOpenProductTasks(tasks, tasksOK, projectID)
	productPressure := len(openProductTasks) > 0
	if !productPressure && t != nil && t.client != nil && strings.TrimSpace(t.workspaceID) != "" && strings.TrimSpace(projectID) != "" {
		coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
		if err == nil {
			productPressure = projectCoordinationHasProductLanePressure(coordination)
		}
	}
	if !productPressure {
		return nil
	}
	productTaskIDs := make([]string, 0, len(openProductTasks))
	for _, task := range openProductTasks {
		if taskID := strings.TrimSpace(task.TaskID); taskID != "" {
			productTaskIDs = append(productTaskIDs, taskID)
		}
	}
	payload := map[string]any{
		"created":                   false,
		"task_submit_gate":          "product_lane_liveness",
		"project_id":                strings.TrimSpace(projectID),
		"blocked_creation_kind":     "coordination_split",
		"product_pressure":          productPressure,
		"required_transition":       "claim_or_delegate_visible_product_lane",
		"existing_product_task_ids": productTaskIDs,
		"guidance":                  "A visible product/revision/review/integration gap already exists for this project. Do not create another generic coordination split; claim or delegate a visible product-lane task, use project_patch_queue_followup for blocked queue work, or create a concrete product-lane task if none is visible. Dedicated authority carriers such as task-role-scope-* and task-project-claim-repair-* remain allowed.",
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: "task_submit skipped: project has product-lane pressure; claim or delegate visible product work instead of creating another coordination split"}
	}
	return &ToolResult{Output: string(raw)}
}

func taskSubmitIsGenericCoordinationSplit(taskID, title, description, projectID, taskKind, projectLane string, tags []string, taskRequirements map[string]any) bool {
	if strings.TrimSpace(projectID) == "" || !strings.EqualFold(strings.TrimSpace(taskKind), "COORDINATION") {
		return false
	}
	requirementsJSON := ""
	if len(taskRequirements) > 0 {
		if raw, err := json.Marshal(taskRequirements); err == nil {
			requirementsJSON = string(raw)
		}
	}
	requested := WorkspaceTaskRecord{
		TaskID:               strings.TrimSpace(taskID),
		Title:                strings.TrimSpace(title),
		Description:          strings.TrimSpace(description),
		TaskKind:             strings.TrimSpace(taskKind),
		ProjectID:            strings.TrimSpace(projectID),
		ProjectLane:          strings.TrimSpace(projectLane),
		TaskRequirementsJSON: requirementsJSON,
		Tags:                 tags,
	}
	if authorityTransitionTaskLooksDedicated(requested) {
		return false
	}
	return taskRecordLooksGenericCoordinationSplit(requested)
}

func taskSubmitOpenProductTasks(tasks []WorkspaceTaskRecord, ok bool, projectID string) []WorkspaceTaskRecord {
	if !ok {
		return nil
	}
	var out []WorkspaceTaskRecord
	for _, task := range tasks {
		if taskSubmitTaskIsTerminal(task) {
			continue
		}
		if !taskSubmitSameDedupeProject(projectID, task.ProjectID) {
			continue
		}
		if !taskRecordIsProductLane(task) {
			continue
		}
		out = append(out, task)
	}
	return out
}

func findExactTaskIDInList(tasks []WorkspaceTaskRecord, ok bool, taskID string) (WorkspaceTaskRecord, bool) {
	if !ok {
		return WorkspaceTaskRecord{}, false
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return WorkspaceTaskRecord{}, false
	}
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) == taskID {
			return task, true
		}
	}
	return WorkspaceTaskRecord{}, false
}

func taskSubmitExistingExactTaskResult(task WorkspaceTaskRecord) *ToolResult {
	taskID := strings.TrimSpace(task.TaskID)
	status := strings.ToUpper(strings.TrimSpace(task.Status))
	terminal := taskSubmitTaskIsTerminal(task)
	guidance := "Task id already exists in this workspace. Do not create a duplicate task; claim, delegate, or contribute to the existing active task."
	if terminal {
		guidance = "Task id already exists and is terminal. Do not recreate or claim it under the same id; inspect its result doc and create a new, narrower task only if current evidence proves fresh work remains."
	}
	payload := map[string]any{
		"created":             false,
		"task_submit_gate":    "exact_task_id_exists",
		"existing_task_id":    taskID,
		"existing_title":      strings.TrimSpace(task.Title),
		"existing_status":     status,
		"terminal_existing":   terminal,
		"guidance":            guidance,
		"task_result_doc_key": "task." + taskID + ".result",
	}
	taskSubmitAnnotateExistingTaskPayload(payload, task)
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("task_submit skipped: task id %s already exists with status %s", taskID, status)}
	}
	return &ToolResult{Output: string(raw)}
}

func taskSubmitAnnotateExistingTaskPayload(payload map[string]any, task WorkspaceTaskRecord) {
	if payload == nil {
		return
	}
	if taskKind := strings.TrimSpace(task.TaskKind); taskKind != "" {
		payload["existing_task_kind"] = taskKind
	}
	if projectID := strings.TrimSpace(task.ProjectID); projectID != "" {
		payload["existing_project_id"] = projectID
	}
	if projectLane := strings.TrimSpace(task.ProjectLane); projectLane != "" {
		payload["existing_project_lane"] = projectLane
	}
	if task.RequiresProjectGate != nil {
		payload["existing_requires_project_gate"] = *task.RequiresProjectGate
	}
	if raw := strings.TrimSpace(task.TaskRequirementsJSON); raw != "" && raw != "{}" {
		var requirements map[string]any
		if err := json.Unmarshal([]byte(raw), &requirements); err == nil && len(requirements) > 0 {
			payload["existing_task_requirements"] = requirements
		}
	}
}

func findSimilarOpenTaskInList(tasks []WorkspaceTaskRecord, ok bool, requestedTaskID, title, description, projectID string) (WorkspaceTaskRecord, float64, bool) {
	if !ok {
		return WorkspaceTaskRecord{}, 0, false
	}
	requestedTaskID = strings.TrimSpace(requestedTaskID)
	best := WorkspaceTaskRecord{}
	bestScore := 0.0
	for _, task := range tasks {
		if taskSubmitTaskIsTerminal(task) {
			continue
		}
		if !taskSubmitSameDedupeProject(projectID, task.ProjectID) {
			continue
		}
		score := taskSubmitSimilarityScore(requestedTaskID, title, description, task)
		if score > bestScore {
			best = task
			bestScore = score
		}
	}
	if bestScore >= 0.82 {
		return best, bestScore, true
	}
	return WorkspaceTaskRecord{}, 0, false
}

func findPatchQueueDuplicateTaskInList(tasks []WorkspaceTaskRecord, ok bool, requestedTaskID, title, description, projectID, projectLane string, tags []string, requested taskSubmitPatchQueueIdentity) (WorkspaceTaskRecord, bool) {
	if !ok || !taskSubmitPatchQueueIdentityActionable(requested) {
		return WorkspaceTaskRecord{}, false
	}
	requested = taskSubmitNormalizePatchQueueIdentity(requested)
	requestedKind := firstNonEmpty(requested.Kind, taskSubmitPatchQueueTaskKindFromLaneAndText(projectLane, title, description, tags))
	if requestedKind == "" {
		return WorkspaceTaskRecord{}, false
	}
	canonicalRetry := taskSubmitPatchQueueRequestIsCanonicalRetry(requestedTaskID, title, description, tags)
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) == strings.TrimSpace(requestedTaskID) {
			continue
		}
		if !taskSubmitSameDedupeProject(projectID, task.ProjectID) {
			continue
		}
		existing := taskSubmitPatchQueueIdentityFromTask(task)
		if !taskSubmitPatchQueueIdentityActionable(existing) {
			continue
		}
		existingKind := firstNonEmpty(existing.Kind, taskSubmitPatchQueueTaskKindFromLaneAndText(task.ProjectLane, task.Title, task.Description, task.Tags))
		if !taskSubmitPatchQueueKindsOverlap(requestedKind, existingKind) {
			continue
		}
		if taskSubmitPatchQueueIdentityMatches(requested, existing) {
			if canonicalRetry && taskSubmitTaskIsTerminal(task) {
				continue
			}
			return task, true
		}
	}
	return WorkspaceTaskRecord{}, false
}

func taskSubmitPatchQueueRequestIsCanonicalRetry(taskID, title, description string, tags []string) bool {
	text := strings.ToLower(strings.Join([]string{
		taskID,
		title,
		description,
		strings.Join(tags, "\n"),
	}, "\n"))
	if strings.Contains(text, "retry_of_terminal_followup_task") {
		return true
	}
	return strings.Contains(text, "patchq") && strings.Contains(text, "retry")
}

func taskSubmitExistingPatchQueueTaskResult(task WorkspaceTaskRecord, identity taskSubmitPatchQueueIdentity) *ToolResult {
	taskID := strings.TrimSpace(task.TaskID)
	terminal := taskSubmitTaskIsTerminal(task)
	guidance := "A live patch-queue/head-bound validation or review task already exists. Do not create a parallel visual/source validation sidecar; claim, resume, or contribute to the existing task, or wait for its terminal blocker/evidence before opening a narrower successor."
	if terminal {
		guidance = "A terminal patch-queue/head-bound validation or review task already exists for this candidate. Do not create a generic parallel sidecar; use project_patch_queue_followup so the terminal blocker becomes a canonical retry/revision continuation tied to the patch queue item."
	}
	payload := map[string]any{
		"created":             false,
		"task_submit_gate":    "patch_queue_identity_duplicate",
		"existing_task_id":    taskID,
		"existing_title":      strings.TrimSpace(task.Title),
		"existing_status":     strings.ToUpper(strings.TrimSpace(task.Status)),
		"terminal_existing":   terminal,
		"required_transition": "project_patch_queue_followup",
		"guidance":            guidance,
	}
	taskSubmitAnnotateExistingTaskPayload(payload, task)
	identity = taskSubmitNormalizePatchQueueIdentity(identity)
	if identity.QueueID != "" {
		payload["queue_id"] = identity.QueueID
	}
	if identity.ItemID != "" {
		payload["item_id"] = identity.ItemID
	}
	if identity.BranchID != "" {
		payload["branch_id"] = identity.BranchID
	}
	if identity.HeadSHA != "" {
		payload["head_sha"] = identity.HeadSHA
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("task_submit skipped: live patch-queue duplicate task %s already exists", taskID)}
	}
	return &ToolResult{Output: string(raw)}
}

func taskSubmitPatchQueueIdentityFromTask(task WorkspaceTaskRecord) taskSubmitPatchQueueIdentity {
	var requirements map[string]any
	if raw := strings.TrimSpace(task.TaskRequirementsJSON); raw != "" && raw != "{}" {
		_ = json.Unmarshal([]byte(raw), &requirements)
	}
	identity := taskSubmitPatchQueueIdentityFromInput(task.Title, task.Description, task.ProjectLane, task.Tags, requirements)
	identity.ProjectID = firstNonEmpty(identity.ProjectID, task.ProjectID)
	if identity.BranchID == "" && task.ClaimBranchID != nil {
		identity.BranchID = *task.ClaimBranchID
	}
	return taskSubmitNormalizePatchQueueIdentity(identity)
}

func taskSubmitPatchQueueTaskKindFromLaneAndText(projectLane, title, description string, tags []string) string {
	text := strings.ToLower(strings.Join([]string{projectLane, title, description, strings.Join(tags, "\n")}, "\n"))
	switch {
	case containsAnySignal(text, []string{"claim stewardship", "claim-stewardship", "claim_stewardship", "queue stewardship", "queue-stewardship", "queue_stewardship", "claimed-decision", "claimed decision", "claimed patch queue"}):
		return "claim_stewardship"
	case containsAnySignal(text, []string{"revision", "revise", "repair", "unblock integration candidate"}):
		return "revision"
	case containsAnySignal(text, []string{"review"}):
		return "review"
	case containsAnySignal(text, []string{"validation", "validate", "visual", "browser", "source-fidelity", "evidence"}):
		return "validation"
	default:
		return ""
	}
}

func taskSubmitPatchQueueKindsOverlap(left, right string) bool {
	left = strings.ToLower(strings.TrimSpace(left))
	right = strings.ToLower(strings.TrimSpace(right))
	if left == "" || right == "" {
		return false
	}
	if left == right {
		return true
	}
	validationLike := map[string]bool{"validation": true, "review": true}
	return validationLike[left] && validationLike[right]
}

func taskSubmitPatchQueueKindNeedsVisualEvidenceReceipt(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "validation", "review":
		return true
	default:
		return false
	}
}

func taskSubmitPatchQueueIdentityMatches(left, right taskSubmitPatchQueueIdentity) bool {
	left = taskSubmitNormalizePatchQueueIdentity(left)
	right = taskSubmitNormalizePatchQueueIdentity(right)
	if left.QueueID != "" && left.ItemID != "" && right.QueueID != "" && right.ItemID != "" &&
		strings.EqualFold(left.QueueID, right.QueueID) && strings.EqualFold(left.ItemID, right.ItemID) {
		return taskSubmitHeadsCompatible(left.HeadSHA, right.HeadSHA)
	}
	if left.BranchID != "" && right.BranchID != "" && strings.EqualFold(left.BranchID, right.BranchID) {
		return taskSubmitHeadsCompatible(left.HeadSHA, right.HeadSHA)
	}
	if left.ItemID != "" && right.ItemID != "" && strings.EqualFold(left.ItemID, right.ItemID) {
		return taskSubmitHeadsCompatible(left.HeadSHA, right.HeadSHA)
	}
	if left.ProjectID != "" && right.ProjectID != "" && strings.EqualFold(left.ProjectID, right.ProjectID) &&
		left.HeadSHA != "" && right.HeadSHA != "" {
		return taskSubmitHeadsCompatible(left.HeadSHA, right.HeadSHA)
	}
	return false
}

type patchQueueVisualEvidenceReceipt struct {
	DocKey   string
	Title    string
	Verdict  string
	Blocking bool
	HeadSHA  string
}

var patchQueueVisualEvidenceHeadTokenPattern = regexp.MustCompile(`(?i)\b[0-9a-f]{7,40}\b`)

func patchQueueVisualEvidenceReceiptForItem(ctx context.Context, client *RhizomeClient, workspaceID string, item ProjectPatchQueueItemRecord) (patchQueueVisualEvidenceReceipt, bool, error) {
	identity := taskSubmitPatchQueueIdentity{
		ProjectID: item.ProjectID,
		QueueID:   item.QueueID,
		ItemID:    item.ItemID,
		BranchID:  item.BranchID,
		HeadSHA:   item.HeadSHA,
		Kind:      "validation",
	}
	return patchQueueVisualEvidenceReceiptForIdentity(ctx, client, workspaceID, identity)
}

func patchQueueVisualEvidenceReceiptForIdentity(ctx context.Context, client *RhizomeClient, workspaceID string, identity taskSubmitPatchQueueIdentity) (patchQueueVisualEvidenceReceipt, bool, error) {
	if client == nil || strings.TrimSpace(workspaceID) == "" || !taskSubmitPatchQueueIdentityActionable(identity) {
		return patchQueueVisualEvidenceReceipt{}, false, nil
	}
	identity = taskSubmitNormalizePatchQueueIdentity(identity)
	summaries, err := client.ListDocs(ctx, workspaceID, 250)
	if err != nil {
		return patchQueueVisualEvidenceReceipt{}, false, err
	}
	for _, summary := range summaries {
		if !patchQueueVisualEvidenceSummaryCouldMatch(summary, identity) {
			continue
		}
		doc := summary
		if strings.TrimSpace(doc.Content) == "" {
			full, ok, err := client.GetDoc(ctx, workspaceID, summary.DocKey)
			if err != nil {
				return patchQueueVisualEvidenceReceipt{}, false, err
			}
			if !ok {
				continue
			}
			doc = full
		}
		if receipt, ok := patchQueueVisualEvidenceReceiptFromDoc(doc, identity); ok {
			return receipt, true, nil
		}
	}
	return patchQueueVisualEvidenceReceipt{}, false, nil
}

func patchQueueVisualEvidenceSummaryCouldMatch(doc WorkspaceDocRecord, identity taskSubmitPatchQueueIdentity) bool {
	text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n"))
	if strings.Contains(text, "rhizome_visual_acceptance_v1") ||
		strings.Contains(text, "visual_acceptance") ||
		strings.Contains(text, "visual acceptance") {
		return true
	}
	for _, ref := range []string{identity.HeadSHA, identity.BranchID, identity.ItemID, identity.QueueID} {
		ref = strings.ToLower(strings.TrimSpace(ref))
		if ref != "" && strings.Contains(text, ref) {
			return true
		}
		if len(ref) >= 7 && strings.Contains(text, ref[:7]) {
			return true
		}
	}
	return false
}

func patchQueueVisualEvidenceReceiptFromDoc(doc WorkspaceDocRecord, identity taskSubmitPatchQueueIdentity) (patchQueueVisualEvidenceReceipt, bool) {
	text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n"))
	if !strings.Contains(text, "rhizome_visual_acceptance_v1") {
		return patchQueueVisualEvidenceReceipt{}, false
	}
	if !patchQueueVisualEvidenceMatchesIdentity(text, identity) {
		return patchQueueVisualEvidenceReceipt{}, false
	}
	verdict, blocking := patchQueueVisualEvidenceVerdict(text)
	if verdict == "" {
		return patchQueueVisualEvidenceReceipt{}, false
	}
	return patchQueueVisualEvidenceReceipt{
		DocKey:   strings.TrimSpace(doc.DocKey),
		Title:    strings.TrimSpace(doc.Title),
		Verdict:  verdict,
		Blocking: blocking,
		HeadSHA:  strings.TrimSpace(identity.HeadSHA),
	}, true
}

func patchQueueVisualEvidenceMatchesIdentity(text string, identity taskSubmitPatchQueueIdentity) bool {
	identity = taskSubmitNormalizePatchQueueIdentity(identity)
	if identity.HeadSHA != "" {
		foundHead := false
		for _, token := range patchQueueVisualEvidenceHeadTokenPattern.FindAllString(text, -1) {
			if taskSubmitHeadsCompatible(token, identity.HeadSHA) {
				foundHead = true
				break
			}
		}
		if !foundHead {
			return false
		}
	}
	refMatched := false
	for _, ref := range []string{identity.BranchID, identity.ItemID, identity.QueueID} {
		ref = strings.ToLower(strings.TrimSpace(ref))
		if ref == "" {
			continue
		}
		if strings.Contains(text, ref) {
			refMatched = true
			break
		}
	}
	if refMatched {
		return true
	}
	projectID := strings.ToLower(strings.TrimSpace(identity.ProjectID))
	return projectID != "" && strings.Contains(text, projectID)
}

func patchQueueVisualEvidenceVerdict(text string) (string, bool) {
	text = strings.ToLower(strings.TrimSpace(text))
	switch {
	case containsAnySignal(text, []string{"visual_verdict: pass", "visual_verdict=pass", `"visual_verdict": "pass"`, `"visual_verdict":"pass"`, "visual verdict: pass"}):
		return "pass", false
	case containsAnySignal(text, []string{"visual_verdict: fail", "visual_verdict=fail", `"visual_verdict": "fail"`, `"visual_verdict":"fail"`, "visual verdict: fail", "visual_verdict: failed", "visual_verdict=failed"}):
		return "fail", true
	case containsAnySignal(text, []string{"visual_verdict: block", "visual_verdict=block", `"visual_verdict": "block"`, `"visual_verdict":"block"`, "visual verdict: block", "visual_verdict: blocked", "visual_verdict=blocked"}):
		return "block", true
	case containsAnySignal(text, []string{"severity: blocking", "blocking_findings", "blocking visual"}):
		return "block", true
	default:
		return "", false
	}
}

func taskSubmitHeadsCompatible(left, right string) bool {
	left = strings.ToLower(strings.TrimSpace(left))
	right = strings.ToLower(strings.TrimSpace(right))
	if left == "" || right == "" {
		return true
	}
	if left == right {
		return true
	}
	if len(left) >= 7 && strings.HasPrefix(right, left) {
		return true
	}
	if len(right) >= 7 && strings.HasPrefix(left, right) {
		return true
	}
	return false
}

func taskSubmitExistingPatchQueueVisualEvidenceResult(receipt patchQueueVisualEvidenceReceipt, identity taskSubmitPatchQueueIdentity) *ToolResult {
	identity = taskSubmitNormalizePatchQueueIdentity(identity)
	requiredTransition := "project_patch_queue_lifecycle"
	guidance := "Exact-head visual evidence already exists for this patch queue candidate. Do not create another missing-evidence validation task; route the patch queue item through lifecycle/supersede review."
	if receipt.Blocking {
		requiredTransition = "project_patch_queue_followup"
		guidance = "Exact-head visual evidence already exists and records a blocking verdict. Do not create another missing-evidence validation task; create or claim the queue-bound revision follow-up."
	}
	payload := map[string]any{
		"created":                   false,
		"task_submit_gate":          "patch_queue_visual_evidence_exists",
		"existing_evidence_doc_key": strings.TrimSpace(receipt.DocKey),
		"existing_evidence_title":   strings.TrimSpace(receipt.Title),
		"visual_verdict":            strings.TrimSpace(receipt.Verdict),
		"blocking_evidence":         receipt.Blocking,
		"required_transition":       requiredTransition,
		"guidance":                  guidance,
	}
	if identity.QueueID != "" {
		payload["queue_id"] = identity.QueueID
	}
	if identity.ItemID != "" {
		payload["item_id"] = identity.ItemID
	}
	if identity.BranchID != "" {
		payload["branch_id"] = identity.BranchID
	}
	if identity.HeadSHA != "" {
		payload["head_sha"] = identity.HeadSHA
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: "task_submit skipped: exact-head visual evidence already exists for this patch queue candidate"}
	}
	return &ToolResult{Output: string(raw)}
}

type taskSubmitDependencyFilterResult struct {
	HardDependencyTaskIDs     []string
	AdvisoryDependencyTaskIDs []string
	RelatedTaskIDs            []string
	DemotedDependencyTaskIDs  []string
	DroppedDependencyTaskIDs  []string
}

func filterTaskSubmitDependencies(dependencyTaskIDs, explicitHardDependencyTaskIDs []string, tasks []WorkspaceTaskRecord, ok bool, projectID, taskKind, projectLane string) taskSubmitDependencyFilterResult {
	if len(dependencyTaskIDs) == 0 || !ok {
		return taskSubmitDependencyFilterResult{HardDependencyTaskIDs: dependencyTaskIDs}
	}
	byID := make(map[string]WorkspaceTaskRecord, len(tasks))
	for _, task := range tasks {
		if taskID := strings.TrimSpace(task.TaskID); taskID != "" {
			byID[taskID] = task
		}
	}
	explicitHard := make(map[string]struct{}, len(explicitHardDependencyTaskIDs))
	for _, dependencyTaskID := range explicitHardDependencyTaskIDs {
		dependencyTaskID = strings.TrimSpace(dependencyTaskID)
		if dependencyTaskID != "" {
			explicitHard[dependencyTaskID] = struct{}{}
		}
	}
	out := make([]string, 0, len(dependencyTaskIDs))
	var advisory []string
	var demoted []string
	var dropped []string
	for _, dependencyTaskID := range dependencyTaskIDs {
		dependencyTaskID = strings.TrimSpace(dependencyTaskID)
		if dependencyTaskID == "" {
			continue
		}
		if task, exists := byID[dependencyTaskID]; exists && taskSubmitDependencyIsProjectRootContext(task, projectID) {
			dropped = append(dropped, dependencyTaskID)
			continue
		}
		if _, hard := explicitHard[dependencyTaskID]; !hard {
			if task, exists := byID[dependencyTaskID]; exists && taskSubmitDependencyIsParallelImplementationSibling(task, projectID, taskKind, projectLane) {
				advisory = append(advisory, dependencyTaskID)
				demoted = append(demoted, dependencyTaskID)
				continue
			}
		}
		out = append(out, dependencyTaskID)
	}
	return taskSubmitDependencyFilterResult{
		HardDependencyTaskIDs:     out,
		AdvisoryDependencyTaskIDs: advisory,
		RelatedTaskIDs:            advisory,
		DemotedDependencyTaskIDs:  demoted,
		DroppedDependencyTaskIDs:  dropped,
	}
}

func taskSubmitDependencyIsProjectRootContext(task WorkspaceTaskRecord, projectID string) bool {
	if !strings.EqualFold(strings.TrimSpace(task.Status), "RUNNING") {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), "COORDINATION") {
		return false
	}
	if strings.TrimSpace(projectID) != "" && !taskSubmitSameDedupeProject(projectID, task.ProjectID) {
		return false
	}
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane == "strategy" || lane == "coordination" || lane == "planning" {
		return true
	}
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(task.TaskID)), "root-")
}

func taskSubmitDependencyIsParallelImplementationSibling(task WorkspaceTaskRecord, projectID, taskKind, projectLane string) bool {
	if strings.TrimSpace(projectID) == "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(taskKind), "EXECUTION") || !runtimeProjectLaneRequiresImplementationGate(projectLane) {
		return false
	}
	if taskSubmitTaskIsTerminal(task) {
		return false
	}
	if !taskSubmitSameDedupeProject(projectID, task.ProjectID) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), "EXECUTION") {
		return false
	}
	return runtimeProjectLaneRequiresImplementationGate(task.ProjectLane)
}

func taskSubmitSameDedupeProject(requestedProjectID, existingProjectID string) bool {
	requestedProjectID = strings.TrimSpace(requestedProjectID)
	existingProjectID = strings.TrimSpace(existingProjectID)
	if requestedProjectID == "" {
		return true
	}
	return strings.EqualFold(requestedProjectID, existingProjectID) ||
		sanitizeDocKeySegment(requestedProjectID) == sanitizeDocKeySegment(existingProjectID)
}

func taskSubmitTaskIsTerminal(task WorkspaceTaskRecord) bool {
	switch strings.ToUpper(strings.TrimSpace(task.Status)) {
	case "DONE", "COMPLETED", "RESOLVED", "CANCELLED", "CLOSED", "ARCHIVED", "FAILED":
		return true
	default:
		return false
	}
}

func taskSubmitSimilarityScore(requestedTaskID, title, description string, task WorkspaceTaskRecord) float64 {
	if requestedTaskID != "" && strings.TrimSpace(task.TaskID) == requestedTaskID {
		return 1
	}
	if normalizedTaskTitle(title) != "" && normalizedTaskTitle(title) == normalizedTaskTitle(task.Title) {
		return 1
	}

	titleScore := tokenJaccard(taskSubmitTokens(title), taskSubmitTokens(task.Title))
	combinedScore := tokenJaccard(
		taskSubmitTokens(strings.Join([]string{title, description}, " ")),
		taskSubmitTokens(strings.Join([]string{task.Title, task.Description}, " ")),
	)
	if titleScore >= 0.72 && taskSubmitSharedTokenCount(title, task.Title) >= 3 {
		return maxFloat64(titleScore, combinedScore)
	}
	if titleScore >= 0.58 && combinedScore >= 0.72 {
		return combinedScore
	}
	return maxFloat64(titleScore*0.75, combinedScore*0.9)
}

func normalizedTaskTitle(value string) string {
	return strings.Join(taskSubmitTokens(value), " ")
}

func taskSubmitTokens(value string) []string {
	stop := map[string]struct{}{
		"a": {}, "an": {}, "and": {}, "for": {}, "in": {}, "of": {}, "on": {}, "or": {}, "the": {}, "to": {}, "with": {},
		"task": {}, "work": {}, "make": {}, "create": {}, "build": {}, "implement": {}, "update": {},
	}
	seen := map[string]struct{}{}
	var tokens []string
	var current []rune
	flush := func() {
		if len(current) == 0 {
			return
		}
		token := strings.TrimSpace(string(current))
		current = current[:0]
		if len([]rune(token)) < 2 {
			return
		}
		if _, ok := stop[token]; ok {
			return
		}
		if _, ok := seen[token]; ok {
			return
		}
		seen[token] = struct{}{}
		tokens = append(tokens, token)
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			current = append(current, unicode.ToLower(r))
			continue
		}
		flush()
	}
	flush()
	return tokens
}

func taskSubmitSharedTokenCount(left, right string) int {
	leftTokens := taskSubmitTokens(left)
	rightTokens := taskSubmitTokens(right)
	rightSet := make(map[string]struct{}, len(rightTokens))
	for _, token := range rightTokens {
		rightSet[token] = struct{}{}
	}
	count := 0
	for _, token := range leftTokens {
		if _, ok := rightSet[token]; ok {
			count++
		}
	}
	return count
}

func tokenJaccard(left, right []string) float64 {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	leftSet := make(map[string]struct{}, len(left))
	for _, token := range left {
		leftSet[token] = struct{}{}
	}
	rightSet := make(map[string]struct{}, len(right))
	for _, token := range right {
		rightSet[token] = struct{}{}
	}
	intersection := 0
	for token := range leftSet {
		if _, ok := rightSet[token]; ok {
			intersection++
		}
	}
	union := len(leftSet) + len(rightSet) - intersection
	if union <= 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}

func maxFloat64(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}

func stringSliceArg(args map[string]any, key string) []string {
	raw, ok := args[key]
	if !ok || raw == nil {
		return nil
	}
	var values []string
	switch v := raw.(type) {
	case []string:
		values = v
	case []any:
		for _, item := range v {
			if s := strings.TrimSpace(fmt.Sprint(item)); s != "" {
				values = append(values, s)
			}
		}
	case string:
		for _, part := range strings.Split(v, ",") {
			if s := strings.TrimSpace(part); s != "" {
				values = append(values, s)
			}
		}
	}
	return uniqueTrimmedCSVStrings(values)
}

func optionalBoolArg(args map[string]any, key string) *bool {
	raw, ok := args[key]
	if !ok || raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case bool:
		return boolPtr(v)
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "yes", "1":
			return boolPtr(true)
		case "false", "no", "0":
			return boolPtr(false)
		}
	}
	return nil
}

func firstNonEmptyStringSlice(values ...[]string) []string {
	for _, value := range values {
		if len(value) > 0 {
			return value
		}
	}
	return nil
}

func objectArg(args map[string]any, key string) map[string]any {
	raw, ok := args[key]
	if !ok || raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, value := range v {
			if strings.TrimSpace(key) != "" {
				out[key] = value
			}
		}
		if len(out) == 0 {
			return nil
		}
		return out
	case json.RawMessage:
		var out map[string]any
		if err := json.Unmarshal(v, &out); err == nil && len(out) > 0 {
			return out
		}
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil
		}
		var out map[string]any
		if err := json.Unmarshal([]byte(trimmed), &out); err == nil && len(out) > 0 {
			return out
		}
	}
	return nil
}
