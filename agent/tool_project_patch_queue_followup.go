package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

type ProjectPatchQueueFollowupTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	ownerUserID    string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectPatchQueueFollowupTool(client *RhizomeClient, workspaceID, agentID, ownerUserID string) *ProjectPatchQueueFollowupTool {
	return &ProjectPatchQueueFollowupTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		ownerUserID: strings.TrimSpace(ownerUserID),
	}
}

func (t *ProjectPatchQueueFollowupTool) Name() string { return "project_patch_queue_followup" }

func (t *ProjectPatchQueueFollowupTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectPatchQueueFollowupTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectPatchQueueFollowupTool) Description() string {
	return "Create an explicit project follow-up task from a terminal patch queue decision. Do not call this from an already-created patch queue follow-up task. The tool only creates coordination work; the resulting task may integrate, revise, validate, or rebuild the referenced candidate through bounded project tools."
}

func (t *ProjectPatchQueueFollowupTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{"type": "string", "description": "Project id."},
			"queue_id":   map[string]any{"type": "string", "description": "Patch queue id."},
			"item_id":    map[string]any{"type": "string", "description": "Patch queue item id."},
			"branch_id": map[string]any{
				"type":        "string",
				"description": "Optional branch id when queue_id/item_id are unknown.",
			},
			"followup_kind": map[string]any{
				"type":        "string",
				"description": "Optional follow-up kind: auto, integration, validation, revision, or rebuild. Auto maps ACCEPTED to integration and REJECTED/BLOCKED to revision unless BLOCKED only needs validation evidence. Use rebuild for ACCEPTED candidates whose source branch/head is proven unavailable.",
				"enum":        []string{"auto", "integration", "validation", "revision", "rebuild"},
			},
			"dependency_task_ids": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional upstream task ids the follow-up should depend on.",
			},
			"extra_context": map[string]any{
				"type":        "string",
				"description": "Optional short context to append to the generated follow-up task brief.",
			},
			"negative_evidence_doc_key": map[string]any{
				"type":        "string",
				"description": "Required for rebuild when extra_context is empty: workspace doc key containing concrete evidence that the accepted source branch/head is unavailable.",
			},
			"skip_visual_receipt_lookup": map[string]any{
				"type":        "boolean",
				"description": "Internal lifecycle handoff flag. When true, do not scan workspace docs for already-published visual evidence before creating a validation follow-up.",
			},
		},
		"required": []string{"project_id"},
	}
}

func (t *ProjectPatchQueueFollowupTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_patch_queue_followup is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_patch_queue_followup", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_followup failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	item, err := selectProjectPatchQueueFollowupItem(coordination.PatchQueueItems, stringArg(args, "queue_id"), stringArg(args, "item_id"), stringArg(args, "branch_id"))
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if strings.TrimSpace(item.ProjectID) == "" {
		item.ProjectID = projectID
	}
	kind, err := normalizeProjectPatchQueueFollowupKind(stringArg(args, "followup_kind"), item)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	extraContext := projectPatchQueueFollowupExtraContext(args)
	if kind == "" {
		return projectPatchQueueFollowupNoTaskResult(projectID, item)
	}
	if kind == "rebuild" && extraContext == "" {
		return &ToolResult{Output: "rebuild follow-up requires concrete negative evidence in extra_context or negative_evidence_doc_key; verify missing refs and `git cat-file -e <head>^{commit}` before bypassing normal integration", IsError: true}
	}
	if strings.TrimSpace(item.DecisionSummary) == "" {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_followup requires decision_summary on terminal item %s/%s before creating follow-up work", item.QueueID, item.ItemID), IsError: true}
	}

	skipVisualReceiptLookup := false
	if raw := optionalBoolArg(args, "skip_visual_receipt_lookup"); raw != nil {
		skipVisualReceiptLookup = *raw
	}

	existingRelated, existingKind, existingRelatedOK := projectPatchQueueFollowupExistingRelatedTask(coordination.Tasks, item, kind)
	if existingRelatedOK {
		staleValidationCandidate := kind == "validation" &&
			existingKind == "validation" &&
			strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") &&
			!skipVisualReceiptLookup
		if !staleValidationCandidate {
			return projectPatchQueueFollowupExistingTaskResult(projectID, item, existingKind, existingRelated)
		}
	}
	if kind == "validation" && strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") && !skipVisualReceiptLookup {
		if receipt, ok, err := patchQueueVisualEvidenceReceiptForItem(ctx, t.client, t.workspaceID, item); err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_patch_queue_followup failed checking visual evidence receipt for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
		} else if ok && receipt.Blocking {
			kind = "revision"
			extraContext = joinNonEmptyLines(extraContext, fmt.Sprintf("visual_evidence_doc_key: %s\nvisual_verdict: %s\nvisual_evidence_route: blocking visual evidence already exists; create a revision instead of another validation refresh.", strings.TrimSpace(receipt.DocKey), strings.TrimSpace(receipt.Verdict)))
			if existingRelated, existingKind, ok := projectPatchQueueFollowupExistingRelatedTask(coordination.Tasks, item, kind); ok {
				return projectPatchQueueFollowupExistingTaskResult(projectID, item, existingKind, existingRelated)
			}
		} else if ok {
			return taskSubmitExistingPatchQueueVisualEvidenceResult(receipt, taskSubmitPatchQueueIdentity{
				QueueID:  item.QueueID,
				ItemID:   item.ItemID,
				BranchID: item.BranchID,
				HeadSHA:  item.HeadSHA,
				Kind:     "validation",
			})
		}
	}
	if existingRelatedOK && existingKind == kind {
		return projectPatchQueueFollowupExistingTaskResult(projectID, item, existingKind, existingRelated)
	}

	taskID := projectPatchQueueFollowupTaskID(projectID, item, kind)
	if existing, ok := projectPatchQueueFollowupExistingTask(coordination.Tasks, taskID); ok {
		if retryTaskID, retryContext, retryOK := projectPatchQueueFollowupRetryForTerminalBlockedTask(projectID, item, kind, existing); retryOK {
			taskID = retryTaskID
			extraContext = joinNonEmptyLines(extraContext, retryContext)
			if existingRetry, retryExists := projectPatchQueueFollowupExistingTask(coordination.Tasks, taskID); retryExists {
				return projectPatchQueueFollowupExistingTaskResult(projectID, item, kind, existingRetry)
			}
		} else {
			return projectPatchQueueFollowupExistingTaskResult(projectID, item, kind, existing)
		}
	}
	branchOwnerAgentID := projectPatchQueueFollowupBranchOwnerAgentID(coordination, item)
	submitOwnerUserID := t.ownerUserID
	if projectPatchQueueFollowupShouldRouteToBranchOwner(item, kind) && strings.TrimSpace(branchOwnerAgentID) != "" {
		submitOwnerUserID = strings.TrimSpace(branchOwnerAgentID)
	}
	taskArgs := map[string]any{
		"task_id":               taskID,
		"title":                 projectPatchQueueFollowupTitle(item, kind),
		"description":           renderProjectPatchQueueFollowupDescription(item, kind, extraContext),
		"priority":              projectPatchQueueFollowupPriority(item.State),
		"task_kind":             "EXECUTION",
		"project_id":            projectID,
		"project_lane":          projectPatchQueueFollowupLane(kind),
		"requires_project_gate": true,
		"dependency_task_ids":   stringSliceArg(args, "dependency_task_ids"),
		"write_scope_hints":     projectPatchQueueFollowupClaimScopeHints(item, kind),
		"task_requirements":     projectPatchQueueFollowupTaskRequirements(item, kind),
		"tags":                  projectPatchQueueFollowupTags(item, kind, branchOwnerAgentID),
	}
	if skipVisualReceiptLookup {
		if requirements, ok := taskArgs["task_requirements"].(map[string]any); ok {
			requirements["skip_visual_receipt_lookup"] = true
		}
	}
	taskResult := NewTaskSubmitTool(t.client, t.workspaceID, t.agentID, submitOwnerUserID).WithRuntimeBinding(t.runtimeBinding).Execute(ctx, taskArgs)
	if taskResult == nil {
		return &ToolResult{Output: "project_patch_queue_followup failed: task_submit returned no result", IsError: true}
	}
	if taskResult.IsError && projectPatchQueueFollowupLooksDuplicateTaskError(taskResult.Output) {
		if existing, ok, hydrateErr := t.hydrateExistingFollowupTask(ctx, taskID); hydrateErr == nil && ok {
			return projectPatchQueueFollowupExistingTaskResult(projectID, item, kind, existing)
		}
	}
	return projectPatchQueueFollowupResult(projectID, item, kind, taskID, extraContext, taskResult)
}

func projectPatchQueueFollowupShouldRouteToBranchOwner(item ProjectPatchQueueItemRecord, kind string) bool {
	return strings.EqualFold(strings.TrimSpace(kind), "revision") &&
		strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED")
}

func projectPatchQueueFollowupExtraContext(args map[string]any) string {
	var parts []string
	if extra := strings.TrimSpace(stringArg(args, "extra_context")); extra != "" {
		parts = append(parts, extra)
	}
	if docKey := strings.TrimSpace(stringArg(args, "negative_evidence_doc_key")); docKey != "" {
		parts = append(parts, "negative_evidence_doc_key: "+docKey)
	}
	return strings.TrimSpace(strings.Join(parts, "\n"))
}

func selectProjectPatchQueueFollowupItem(items []ProjectPatchQueueItemRecord, queueID, itemID, branchID string) (ProjectPatchQueueItemRecord, error) {
	queueID = strings.TrimSpace(queueID)
	itemID = strings.TrimSpace(itemID)
	branchID = strings.TrimSpace(branchID)
	if queueID == "" && itemID == "" && branchID == "" {
		return ProjectPatchQueueItemRecord{}, projectPatchQueueSelectorMissingError()
	}
	var matches []ProjectPatchQueueItemRecord
	for _, item := range items {
		if queueID != "" && strings.TrimSpace(item.QueueID) != queueID {
			continue
		}
		if itemID != "" && strings.TrimSpace(item.ItemID) != itemID {
			continue
		}
		if branchID != "" && strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		matches = append(matches, item)
	}
	switch len(matches) {
	case 0:
		return ProjectPatchQueueItemRecord{}, projectPatchQueueSelectorNotFoundError()
	case 1:
		return matches[0], nil
	default:
		return ProjectPatchQueueItemRecord{}, projectPatchQueueSelectorAmbiguousError(matches)
	}
}

func normalizeProjectPatchQueueFollowupKind(value string, item ProjectPatchQueueItemRecord) (string, error) {
	kind := strings.ToLower(strings.TrimSpace(value))
	kind = strings.ReplaceAll(kind, "-", "_")
	kind = strings.ReplaceAll(kind, " ", "_")
	state := strings.ToUpper(strings.TrimSpace(item.State))
	if kind == "" || kind == "auto" {
		switch state {
		case "ACCEPTED":
			return "integration", nil
		case "BLOCKED":
			if projectPatchQueueBlockedNeedsRevision(item) {
				return "revision", nil
			}
			if projectPatchQueueBlockedNeedsValidation(item) {
				return "validation", nil
			}
			return "revision", nil
		case "REJECTED":
			return "revision", nil
		case "CANCELED", "CANCELLED":
			return "", nil
		default:
			return "", fmt.Errorf("project_patch_queue_followup requires a terminal decision state, got %s", state)
		}
	}
	switch kind {
	case "integrate", "integration", "merge":
		if state != "ACCEPTED" {
			return "", fmt.Errorf("integration follow-up requires ACCEPTED patch queue item, got %s", state)
		}
		return "integration", nil
	case "validate", "validation":
		if state != "ACCEPTED" && state != "BLOCKED" {
			return "", fmt.Errorf("validation follow-up requires ACCEPTED or validation-blocked patch queue item, got %s", state)
		}
		return "validation", nil
	case "revise", "revision", "unblock":
		if state != "REJECTED" && state != "BLOCKED" {
			return "", fmt.Errorf("revision follow-up requires REJECTED or BLOCKED patch queue item, got %s", state)
		}
		return "revision", nil
	case "rebuild", "replacement", "replace", "regenerate", "reimplement", "fresh_candidate":
		if state != "ACCEPTED" {
			return "", fmt.Errorf("rebuild follow-up requires ACCEPTED patch queue item with unavailable source evidence, got %s", state)
		}
		return "rebuild", nil
	default:
		return "", fmt.Errorf("followup_kind must be auto, integration, validation, revision, or rebuild")
	}
}

func projectPatchQueueBlockedNeedsValidation(item ProjectPatchQueueItemRecord) bool {
	if !strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
		return false
	}
	if projectPatchQueueBlockedNeedsRevision(item) {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(item.DecisionSummary + "\n" + item.DecisionDocKey + "\n" + item.ReviewDocKey))
	if text == "" {
		return false
	}
	validationHints := []string{"verification", "verify", "browser", "build", "test", "smoke", "evidence", "manual", "validation"}
	for _, hint := range validationHints {
		if strings.Contains(text, hint) {
			return true
		}
	}
	return false
}

func projectPatchQueueBlockedNeedsRevision(item ProjectPatchQueueItemRecord) bool {
	if !strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(item.DecisionSummary + "\n" + item.DecisionDocKey + "\n" + item.ReviewDocKey))
	if text == "" {
		return false
	}
	revisionHints := []string{
		"blocked_spec_drift",
		"bug",
		"broken",
		"code defect",
		"compile error",
		"corrupt",
		"corrupted",
		"defect",
		"incorrect",
		"jsx",
		"regression",
		"repair",
		"runtime error",
		"tsx",
		"type error",
		"ui string",
		"user-facing",
		"visual_verdict: fail",
		`"visual_verdict": "fail"`,
		`"visual_verdict":"fail"`,
		"blocking_findings",
		"blocking visual",
		"below first viewport",
		"below the first viewport",
		"below the fold",
		"not visible on first load",
		"first viewport does not show",
		"primary game surface is not visible",
		"responsive_fit: fail",
		"first_viewport_empty_state: fail",
		"result_state_coverage: fail",
		"missing result-state implementation",
		"missing implementation",
		"no implementation change",
		"no product implementation",
		"no runtime change",
		"no runtime implementation",
		"not implemented",
		"does not deliver",
		"does not implement",
		"doesn't implement",
		"smallest repair direction",
		"spec drift",
		"test-only",
		"tests-only",
		"only contract tests",
		"wrong",
	}
	for _, hint := range revisionHints {
		if strings.Contains(text, hint) {
			return true
		}
	}
	if containsAnySignal(text, []string{"src/", "app.tsx", "app.jsx", ".tsx", ".jsx"}) &&
		containsAnySignal(text, []string{"contains", "corrupt", "broken", "incorrect", "wrong", "fix", "repair"}) {
		return true
	}
	return false
}

func projectPatchQueueFollowupNoTaskResult(projectID string, item ProjectPatchQueueItemRecord) *ToolResult {
	payload := map[string]any{
		"created":           false,
		"reason":            "canceled_patch_queue_item",
		"project_id":        projectID,
		"repo_id":           item.RepoID,
		"branch_id":         item.BranchID,
		"queue_id":          item.QueueID,
		"item_id":           item.ItemID,
		"state":             item.State,
		"no_git_mutation":   true,
		"followup_guidance": "Canceled patch queue items do not create integration, validation, or revision work by default.",
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: "project_patch_queue_followup skipped canceled patch queue item"}
	}
	return &ToolResult{Output: string(raw)}
}

func (t *ProjectPatchQueueFollowupTool) hydrateExistingFollowupTask(ctx context.Context, taskID string) (WorkspaceTaskRecord, bool, error) {
	if t == nil || t.client == nil || strings.TrimSpace(taskID) == "" {
		return WorkspaceTaskRecord{}, false, nil
	}
	bundle, err := t.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID:      t.workspaceID,
		TaskID:           strings.TrimSpace(taskID),
		UpdatesLimit:     1,
		ArtifactLimit:    1,
		RelatedTaskLimit: 1,
	})
	if err != nil {
		return WorkspaceTaskRecord{}, false, err
	}
	if bundle.WorkspaceTask == nil || strings.TrimSpace(bundle.WorkspaceTask.TaskID) == "" {
		return WorkspaceTaskRecord{}, false, nil
	}
	return *bundle.WorkspaceTask, true, nil
}

func projectPatchQueueFollowupExistingTask(tasks []WorkspaceTaskRecord, taskID string) (WorkspaceTaskRecord, bool) {
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

func projectPatchQueueFollowupExistingRelatedTask(tasks []WorkspaceTaskRecord, item ProjectPatchQueueItemRecord, desiredKind string) (WorkspaceTaskRecord, string, bool) {
	desiredKind = strings.ToLower(strings.TrimSpace(desiredKind))
	if desiredKind == "" {
		return WorkspaceTaskRecord{}, "", false
	}
	kinds := []string{desiredKind}
	if desiredKind == "validation" && strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
		kinds = []string{"revision", "validation"}
	}
	for _, kind := range kinds {
		for _, task := range tasks {
			if taskSubmitTaskIsTerminal(task) {
				continue
			}
			if projectPatchQueueFollowupExistingTaskKind(task) != kind {
				continue
			}
			if projectPatchQueueFollowupSupersededByQueueBoundTask(tasks, task, item) {
				continue
			}
			if projectPatchQueueFollowupTaskReferencesItem(task, item) {
				return task, kind, true
			}
		}
	}
	return WorkspaceTaskRecord{}, "", false
}

func projectPatchQueueFollowupSupersededByQueueBoundTask(tasks []WorkspaceTaskRecord, candidate WorkspaceTaskRecord, item ProjectPatchQueueItemRecord) bool {
	if !projectPatchQueueFollowupGenericValidationSidecarCandidate(candidate) {
		return false
	}
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) == strings.TrimSpace(candidate.TaskID) {
			continue
		}
		if !projectPatchQueueFollowupQueueBoundValidationOrReviewTask(task) {
			continue
		}
		if projectPatchQueueFollowupTaskReferencesItem(task, item) {
			return true
		}
	}
	return false
}

func projectPatchQueueFollowupGenericValidationSidecarCandidate(task WorkspaceTaskRecord) bool {
	if projectPatchQueueFollowupQueueBoundValidationOrReviewTask(task) {
		return false
	}
	return projectPatchQueueFollowupExistingTaskKind(task) == "validation"
}

func projectPatchQueueFollowupQueueBoundValidationOrReviewTask(task WorkspaceTaskRecord) bool {
	if !projectPatchQueueFollowupValidationOrReviewScoped(task) {
		return false
	}
	if projectPatchQueueFollowupDeterministicValidationOrReviewTask(task) {
		return true
	}
	queueID := projectPatchQueueFollowupTaskFieldValue(task, "queue_id", "queue-id", "queue")
	itemID := projectPatchQueueFollowupTaskFieldValue(task, "item_id", "item-id", "item")
	return strings.TrimSpace(queueID) != "" && strings.TrimSpace(itemID) != ""
}

func projectPatchQueueFollowupValidationOrReviewScoped(task WorkspaceTaskRecord) bool {
	if projectPatchQueueFollowupExistingTaskKind(task) == "validation" {
		return true
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane == "review" || lane == "qa" || lane == "validation" {
		return true
	}
	text := strings.ToLower(strings.Join([]string{task.Title, task.Description, strings.Join(task.Tags, " ")}, "\n"))
	return strings.Contains(text, "validation") ||
		strings.Contains(text, "visual acceptance") ||
		strings.Contains(text, "browser evidence") ||
		strings.Contains(text, "review")
}

func projectPatchQueueFollowupDeterministicValidationOrReviewTask(task WorkspaceTaskRecord) bool {
	taskID := strings.ToLower(strings.TrimSpace(task.TaskID))
	return strings.HasPrefix(taskID, "task-patchq-validation-") ||
		strings.HasPrefix(taskID, "task-patchq-review-") ||
		(strings.HasPrefix(taskID, "task-review-") && strings.Contains(taskID, "patchq") && strings.Contains(taskID, "patchitem"))
}

func projectPatchQueueFollowupExistingTaskKind(task WorkspaceTaskRecord) string {
	for _, tag := range task.Tags {
		normalized := strings.ToLower(strings.TrimSpace(tag))
		switch normalized {
		case "revision", "patch_queue_revision", "patch-queue-revision":
			return "revision"
		case "validation", "patch_queue_validation", "patch-queue-validation":
			return "validation"
		case "integration", "patch_queue_integration", "patch-queue-integration":
			return "integration"
		case "rebuild", "patch_queue_rebuild", "patch-queue-rebuild":
			return "rebuild"
		}
		if strings.Contains(normalized, "owner-bound-kind:patch_queue_revision") || strings.Contains(normalized, "owner_bound_kind:patch_queue_revision") {
			return "revision"
		}
	}
	searchText := strings.ToLower(strings.Join([]string{task.Title, task.Description}, "\n"))
	if strings.EqualFold(strings.TrimSpace(task.ProjectLane), "validation") ||
		strings.Contains(searchText, "validate blocked integration candidate") ||
		strings.Contains(searchText, "validate accepted integration candidate") ||
		strings.Contains(searchText, "validation follow-up") {
		return "validation"
	}
	if strings.Contains(searchText, "rebuild unavailable accepted candidate") ||
		strings.Contains(searchText, "rebuild follow-up") {
		return "rebuild"
	}
	if strings.EqualFold(strings.TrimSpace(task.ProjectLane), "integration") ||
		strings.Contains(searchText, "integrate accepted candidate") ||
		strings.Contains(searchText, "integration follow-up") {
		return "integration"
	}
	if strings.EqualFold(strings.TrimSpace(task.ProjectLane), "implementation") &&
		(strings.Contains(searchText, "unblock integration candidate") ||
			strings.Contains(searchText, "revise") ||
			strings.Contains(searchText, "revision") ||
			strings.Contains(searchText, "patch queue decision follow-up")) {
		return "revision"
	}
	return ""
}

func projectPatchQueueFollowupTaskReferencesItem(task WorkspaceTaskRecord, item ProjectPatchQueueItemRecord) bool {
	identityText := projectPatchQueueFollowupTaskIdentityText(task)
	if identityText == "" {
		return false
	}
	queueID := strings.TrimSpace(item.QueueID)
	itemID := strings.TrimSpace(item.ItemID)
	branchID := strings.TrimSpace(item.BranchID)
	taskQueueID := projectPatchQueueFollowupTaskFieldValue(task, "queue_id", "queue-id", "queue")
	taskItemID := projectPatchQueueFollowupTaskFieldValue(task, "item_id", "item-id", "item")
	taskBranchID := projectPatchQueueFollowupTaskFieldValue(task, "branch_id", "branch-id", "branch")
	if queueID != "" && itemID != "" && taskQueueID == queueID && taskItemID == itemID {
		return projectPatchQueueFollowupExactItemHeadCompatible(task, item)
	}
	if branchID != "" && taskBranchID == branchID && projectPatchQueueFollowupBranchHeadMatches(task, item) {
		return true
	}
	queueMatch := queueID != "" && projectPatchQueueFollowupIdentityContainsRef(identityText, queueID)
	itemMatch := itemID != "" && projectPatchQueueFollowupIdentityContainsRef(identityText, itemID)
	branchMatch := branchID != "" && projectPatchQueueFollowupIdentityContainsRef(identityText, branchID)
	if queueID != "" && itemID != "" && queueMatch && itemMatch {
		return projectPatchQueueFollowupExactItemHeadCompatible(task, item)
	}
	if branchMatch && projectPatchQueueFollowupBranchHeadMatches(task, item) {
		return true
	}
	return false
}

func projectPatchQueueFollowupIdentityContainsRef(text, ref string) bool {
	text = strings.TrimSpace(text)
	ref = strings.TrimSpace(ref)
	if text == "" || ref == "" {
		return false
	}
	offset := 0
	for {
		idx := strings.Index(text[offset:], ref)
		if idx < 0 {
			return false
		}
		idx += offset
		end := idx + len(ref)
		leftOK := idx == 0 || !projectPatchQueueFollowupRefChar(text[idx-1])
		rightOK := end >= len(text) || !projectPatchQueueFollowupRefChar(text[end])
		if leftOK && rightOK {
			return true
		}
		offset = end
		if offset >= len(text) {
			return false
		}
	}
}

func projectPatchQueueFollowupRefChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_' ||
		ch == '-' ||
		ch == '.'
}

func projectPatchQueueFollowupTaskIdentityText(task WorkspaceTaskRecord) string {
	var parts []string
	for _, value := range []string{task.TaskID, task.Title, task.Description, task.ProjectLane} {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	for _, tag := range task.Tags {
		if trimmed := strings.TrimSpace(tag); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return strings.Join(parts, "\n")
}

func projectPatchQueueFollowupExactItemHeadCompatible(task WorkspaceTaskRecord, item ProjectPatchQueueItemRecord) bool {
	itemHead := strings.TrimSpace(item.HeadSHA)
	if itemHead == "" {
		return true
	}
	taskHead := projectPatchQueueFollowupTaskFieldValue(task, "head_sha", "head")
	return taskHead == "" || strings.EqualFold(taskHead, itemHead)
}

func projectPatchQueueFollowupBranchHeadMatches(task WorkspaceTaskRecord, item ProjectPatchQueueItemRecord) bool {
	itemHead := strings.TrimSpace(item.HeadSHA)
	if itemHead == "" {
		return true
	}
	taskHead := projectPatchQueueFollowupTaskFieldValue(task, "head_sha", "head")
	return taskHead != "" && strings.EqualFold(taskHead, itemHead)
}

func projectPatchQueueFollowupTaskFieldValue(task WorkspaceTaskRecord, keys ...string) string {
	if len(keys) == 0 {
		return ""
	}
	keySet := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if trimmed := strings.ToLower(strings.TrimSpace(key)); trimmed != "" {
			keySet[trimmed] = struct{}{}
		}
	}
	for _, text := range []string{task.Description, task.Title, strings.Join(task.Tags, "\n")} {
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "-*"))
			if line == "" {
				continue
			}
			key, value, ok := strings.Cut(line, ":")
			if !ok {
				key, value, ok = strings.Cut(line, "=")
			}
			if !ok {
				continue
			}
			if _, want := keySet[strings.ToLower(strings.TrimSpace(key))]; !want {
				continue
			}
			return normalizeProjectToolRef(value)
		}
	}
	return ""
}

func projectPatchQueueFollowupLooksDuplicateTaskError(output string) bool {
	lower := strings.ToLower(strings.TrimSpace(output))
	if lower == "" {
		return false
	}
	if !strings.Contains(lower, "task_submit failed") && !strings.Contains(lower, "task submit failed") {
		return false
	}
	return strings.Contains(lower, "already exists") ||
		strings.Contains(lower, "duplicate") ||
		strings.Contains(lower, "unique constraint") ||
		strings.Contains(lower, "unique conflict")
}

func projectPatchQueueFollowupExistingTaskResult(projectID string, item ProjectPatchQueueItemRecord, kind string, task WorkspaceTaskRecord) *ToolResult {
	taskID := strings.TrimSpace(task.TaskID)
	status := strings.ToUpper(strings.TrimSpace(task.Status))
	terminal := taskSubmitTaskIsTerminal(task)
	reason := "existing_followup_task_active"
	guidance := "A branch/item-bound patch queue follow-up task already exists and is still active. Claim or delegate that exact task instead of creating duplicate private or parallel work."
	if terminal {
		reason = "existing_followup_task_terminal"
		guidance = "The branch/item-bound patch queue follow-up task already reached a terminal status. Do not claim or delegate it; inspect current project coordination and fresh branch/head-bound evidence docs before deciding whether genuinely new work remains."
	}
	payload := map[string]any{
		"created":           false,
		"reason":            reason,
		"project_id":        projectID,
		"repo_id":           item.RepoID,
		"branch_id":         item.BranchID,
		"queue_id":          item.QueueID,
		"item_id":           item.ItemID,
		"state":             item.State,
		"followup_kind":     kind,
		"task_id":           taskID,
		"existing_task_id":  taskID,
		"existing_status":   status,
		"terminal_existing": terminal,
		"no_git_mutation":   true,
		"followup_guidance": guidance,
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: guidance}
	}
	return &ToolResult{Output: string(raw)}
}

func projectPatchQueueFollowupRetryForTerminalBlockedTask(projectID string, item ProjectPatchQueueItemRecord, kind string, task WorkspaceTaskRecord) (string, string, bool) {
	if !taskSubmitTaskIsTerminal(task) || !strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
		return "", "", false
	}
	if kind != "validation" && kind != "revision" {
		return "", "", false
	}
	baseTaskID := strings.TrimSpace(task.TaskID)
	seed := strings.Join([]string{
		projectID,
		item.QueueID,
		item.ItemID,
		kind,
		baseTaskID,
		strings.ToUpper(strings.TrimSpace(task.Status)),
		strings.TrimSpace(task.UpdatedAt),
		strings.TrimSpace(item.UpdatedAt),
		strings.TrimSpace(item.DecisionSummary),
	}, "|")
	hash := sha256.Sum256([]byte(seed))
	suffix := hex.EncodeToString(hash[:])[:12]
	slug := projectPatchQueueFollowupSlug(strings.TrimPrefix(baseTaskID, "task-") + "-retry")
	const maxSlugLen = 120
	maxBaseLen := maxSlugLen - len(suffix) - 1
	if len(slug) > maxBaseLen {
		slug = strings.Trim(slug[:maxBaseLen], "-")
	}
	if slug == "" {
		slug = "patchq-followup-retry"
	}
	retryTaskID := "task-" + slug + "-" + suffix
	context := fmt.Sprintf("retry_of_terminal_followup_task: %s\nretry_existing_status: %s\nretry_reason: blocked patch queue item still needs %s after the deterministic follow-up reached a terminal state.", baseTaskID, strings.ToUpper(strings.TrimSpace(task.Status)), kind)
	return retryTaskID, context, true
}

func joinNonEmptyLines(parts ...string) string {
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			kept = append(kept, trimmed)
		}
	}
	return strings.Join(kept, "\n")
}

func projectPatchQueueFollowupResult(projectID string, item ProjectPatchQueueItemRecord, kind, taskID, extraContext string, taskResult *ToolResult) *ToolResult {
	taskPayload := any(strings.TrimSpace(taskResult.Output))
	var parsed any
	if err := json.Unmarshal([]byte(strings.TrimSpace(taskResult.Output)), &parsed); err == nil {
		taskPayload = parsed
	}
	guidance := "The patch queue decision now has explicit follow-up work. Agents should claim the task instead of continuing private or duplicated integration loops."
	if taskResult.IsError {
		guidance = "No follow-up task was created. Do not claim or delegate the deterministic follow-up task id until task_submit succeeds or an existing task is hydrated; repair the submit error first."
	}
	payload := map[string]any{
		"project_id":         projectID,
		"repo_id":            item.RepoID,
		"branch_id":          item.BranchID,
		"queue_id":           item.QueueID,
		"item_id":            item.ItemID,
		"state":              item.State,
		"followup_kind":      kind,
		"task_id":            taskID,
		"task_submit_result": taskPayload,
		"no_git_mutation":    true,
		"followup_guidance":  guidance,
	}
	if strings.TrimSpace(extraContext) != "" {
		payload["followup_context"] = strings.TrimSpace(extraContext)
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: strings.TrimSpace(taskResult.Output), IsError: taskResult.IsError}
	}
	return &ToolResult{Output: string(raw), IsError: taskResult.IsError}
}

func projectPatchQueueFollowupTaskID(projectID string, item ProjectPatchQueueItemRecord, kind string) string {
	parts := []string{"patchq", kind, projectID, item.QueueID, item.ItemID}
	raw := strings.Join(parts, "-")
	hash := sha256.Sum256([]byte(raw))
	suffix := hex.EncodeToString(hash[:])[:12]
	slug := projectPatchQueueFollowupSlug(raw)
	const maxSlugLen = 120
	maxBaseLen := maxSlugLen - len(suffix) - 1
	if len(slug) > maxBaseLen {
		slug = strings.Trim(slug[:maxBaseLen], "-")
	}
	if slug == "" {
		slug = "item"
	}
	return "task-" + slug + "-" + suffix
}

func projectPatchQueueFollowupTitle(item ProjectPatchQueueItemRecord, kind string) string {
	branch := firstNonEmpty(strings.TrimSpace(item.BranchID), strings.TrimSpace(item.ItemID), "patch queue item")
	switch kind {
	case "integration":
		return fmt.Sprintf("Integrate accepted candidate %s", branch)
	case "rebuild":
		return fmt.Sprintf("Rebuild unavailable accepted candidate %s", branch)
	case "validation":
		if strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
			return fmt.Sprintf("Validate blocked integration candidate %s", branch)
		}
		return fmt.Sprintf("Validate accepted integration candidate %s", branch)
	default:
		if strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
			return fmt.Sprintf("Unblock integration candidate %s", branch)
		}
		return fmt.Sprintf("Revise rejected integration candidate %s", branch)
	}
}

func projectPatchQueueFollowupPriority(state string) string {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "ACCEPTED", "BLOCKED":
		return "high"
	default:
		return "normal"
	}
}

func projectPatchQueueFollowupLane(kind string) string {
	switch kind {
	case "integration":
		return "integration"
	case "validation":
		return "validation"
	case "rebuild":
		return "implementation"
	default:
		return "implementation"
	}
}

func projectPatchQueueFollowupTags(item ProjectPatchQueueItemRecord, kind, branchOwnerAgentID string) []string {
	tags := []string{"project", "patch-queue", kind}
	if s := strings.ToLower(strings.TrimSpace(item.State)); s != "" {
		tags = append(tags, s)
	}
	if kind == "revision" && strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
		if branchID := strings.TrimSpace(item.BranchID); branchID != "" {
			tags = append(tags, "owner-bound", "owner-bound-kind:patch_queue_revision", "owner-branch:"+branchID)
		}
		if owner := strings.TrimSpace(branchOwnerAgentID); owner != "" {
			tags = append(tags, "owner-agent:"+owner, "required-agent:"+owner)
		}
	}
	return uniqueTrimmedCSVStrings(tags)
}

func projectPatchQueueFollowupTaskRequirements(item ProjectPatchQueueItemRecord, kind string) map[string]any {
	requirements := map[string]any{
		"schema":                    "task_requirements.v1",
		"patch_queue_task_identity": "rhizome_patch_queue_task_identity.v1",
		"patch_queue_task_kind":     strings.ToLower(strings.TrimSpace(kind)),
		"repo_id":                   strings.TrimSpace(item.RepoID),
		"branch_id":                 strings.TrimSpace(item.BranchID),
		"queue_id":                  strings.TrimSpace(item.QueueID),
		"item_id":                   strings.TrimSpace(item.ItemID),
		"state":                     strings.TrimSpace(item.State),
	}
	if strings.TrimSpace(item.HeadSHA) != "" {
		requirements["head_sha"] = strings.TrimSpace(item.HeadSHA)
	}
	if strings.TrimSpace(item.ReviewDocKey) != "" {
		requirements["review_doc_key"] = strings.TrimSpace(item.ReviewDocKey)
	}
	if strings.TrimSpace(item.DecisionDocKey) != "" {
		requirements["decision_doc_key"] = strings.TrimSpace(item.DecisionDocKey)
	}
	if strings.TrimSpace(item.SupersedesQueueID) != "" {
		requirements["supersedes_queue_id"] = strings.TrimSpace(item.SupersedesQueueID)
	}
	if strings.TrimSpace(item.SupersedesItemID) != "" {
		requirements["supersedes_item_id"] = strings.TrimSpace(item.SupersedesItemID)
	}
	if paths := projectPatchQueueFollowupPathset(item); len(paths) > 0 {
		if strings.EqualFold(strings.TrimSpace(kind), "revision") {
			requirements["candidate_pathset"] = paths
			requirements["candidate_pathset_role"] = "historical_changed_path_evidence_not_claim_scope"
		} else {
			requirements["write_scope_hints"] = paths
		}
	}
	if kind == "validation" && strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
		requirements["required_transition"] = "project_patch_queue_submit_or_revision_followup"
		requirements["blocked_validation_recovery"] = "same_head_evidence_or_bounded_revision"
	}
	if kind == "revision" && strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
		requirements["required_transition"] = "project_patch_queue_revision_commit_review_submit"
		requirements["required_first_publication_tool"] = "project_branch_commit"
		requirements["required_tool_sequence"] = []string{"project_branch_commit", "project_branch_review_ready", "project_patch_queue_submit"}
		requirements["required_terminal_tool"] = "project_patch_queue_submit"
		requirements["historical_source_branch_role"] = "read_only_defeated_source_branch_evidence"
		requirements["live_repair_branch_required"] = true
		requirements["forbidden_substitutes"] = []string{"status_only", "workspace_doc_put_only", "historical_branch_mutation", "same_head_retry_without_revision"}
	}
	if kind == "integration" {
		applyProjectPatchQueueAcceptedIntegrationRequirements(requirements, item.RepoAuthorityMode, item.MaterializationSchema, item.MaterializationDigest, item.MaterializationAccepted)
	}
	return requirements
}

func applyProjectPatchQueueAcceptedIntegrationRequirements(requirements map[string]any, repoAuthorityMode, materializationSchema, materializationDigest string, materializationAccepted bool) map[string]any {
	if requirements == nil {
		requirements = map[string]any{}
	}
	requirements["required_tool"] = "project_patch_queue_integrate"
	requirements["required_transition"] = "project_patch_queue_integrate_then_full_product_verify"
	requirements["integration_boundary"] = "accepted_lane_candidate_must_be_assembled_before_full_product_acceptance"
	requirements["required_evidence"] = []string{"integration_receipt", "canonical_target_head", "build_or_test_command", "full_product_verdict"}
	if projectPatchQueueAcceptedIntegrationNeedsDirectMerge(repoAuthorityMode, materializationSchema, materializationDigest, materializationAccepted) {
		requirements["integration_mode"] = "direct_merge"
		requirements["required_tool_args"] = map[string]any{"integration_mode": "direct_merge"}
		requirements["integration_authorization"] = "direct_merge_for_accepted_unmaterialized_controlled_queue"
		requirements["controlled_queue_materialization_state"] = "missing_before_acceptance"
		requirements["required_evidence"] = []string{"project.patch_queue.integrated direct_merge receipt", "canonical target head", "post-integration build/test evidence"}
	} else if strings.TrimSpace(repoAuthorityMode) == "repoauthority_controlled_queue" {
		requirements["integration_mode"] = "materialization"
	}
	return requirements
}

func projectPatchQueueAcceptedIntegrationNeedsDirectMerge(repoAuthorityMode, materializationSchema, materializationDigest string, materializationAccepted bool) bool {
	if strings.TrimSpace(repoAuthorityMode) != "repoauthority_controlled_queue" {
		return false
	}
	return !materializationAccepted ||
		strings.TrimSpace(materializationSchema) == "" ||
		strings.TrimSpace(materializationDigest) == ""
}

func projectPatchQueueFollowupBranchOwnerAgentID(coordination ProjectCoordinationRecord, item ProjectPatchQueueItemRecord) string {
	fallbackOwner := strings.TrimSpace(item.AgentID)
	branchID := strings.TrimSpace(item.BranchID)
	if branchID == "" {
		return fallbackOwner
	}
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchID) != branchID {
			continue
		}
		if owner := strings.TrimSpace(branch.AgentID); owner != "" {
			return owner
		}
	}
	return fallbackOwner
}

func renderProjectPatchQueueFollowupDescription(item ProjectPatchQueueItemRecord, kind, extraContext string) string {
	var b strings.Builder
	b.WriteString("Patch queue decision follow-up.\n\n")
	b.WriteString("- queue_id: ")
	b.WriteString(strings.TrimSpace(item.QueueID))
	b.WriteString("\n- item_id: ")
	b.WriteString(strings.TrimSpace(item.ItemID))
	b.WriteString("\n- repo_id: ")
	b.WriteString(strings.TrimSpace(item.RepoID))
	b.WriteString("\n- branch_id: ")
	b.WriteString(strings.TrimSpace(item.BranchID))
	b.WriteString("\n- state: ")
	b.WriteString(strings.TrimSpace(item.State))
	if strings.TrimSpace(item.ReviewDocKey) != "" {
		b.WriteString("\n- review_doc_key: ")
		b.WriteString(strings.TrimSpace(item.ReviewDocKey))
	}
	if strings.TrimSpace(item.DecisionDocKey) != "" {
		b.WriteString("\n- decision_doc_key: ")
		b.WriteString(strings.TrimSpace(item.DecisionDocKey))
	}
	if strings.TrimSpace(item.DecidedBy) != "" {
		b.WriteString("\n- decided_by: ")
		b.WriteString(strings.TrimSpace(item.DecidedBy))
	}
	if strings.TrimSpace(item.BaseRef) != "" {
		b.WriteString("\n- base_ref: ")
		b.WriteString(strings.TrimSpace(item.BaseRef))
	}
	if strings.TrimSpace(item.BaseSHA) != "" {
		b.WriteString("\n- base_sha: ")
		b.WriteString(strings.TrimSpace(item.BaseSHA))
	}
	if strings.TrimSpace(item.HeadSHA) != "" {
		b.WriteString("\n- head_sha: ")
		b.WriteString(strings.TrimSpace(item.HeadSHA))
	}
	if paths := projectPatchQueueFollowupPathset(item); len(paths) > 0 {
		if kind == "revision" {
			b.WriteString("\n- candidate_pathset: ")
		} else {
			b.WriteString("\n- write_scope_hints: ")
		}
		b.WriteString(strings.Join(paths, ", "))
	}
	b.WriteString("\n\nDecision summary:\n")
	b.WriteString(strings.TrimSpace(item.DecisionSummary))
	b.WriteString("\n\nRequired work:\n")
	switch kind {
	case "integration":
		b.WriteString("- Integrate the accepted candidate into the canonical project target with project_patch_queue_integrate; acceptance alone is not a canonical baseline update.\n")
		if projectPatchQueueAcceptedIntegrationNeedsDirectMerge(item.RepoAuthorityMode, item.MaterializationSchema, item.MaterializationDigest, item.MaterializationAccepted) {
			b.WriteString("- This accepted controlled-queue item has no durable materialization receipt; the integration task explicitly authorizes project_patch_queue_integrate with integration_mode=direct_merge rather than asking for impossible post-ACCEPTED materialization.\n")
		}
		b.WriteString("- Use the referenced queue/item/branch/head as the integration source. Do not repair older rejected or blocked candidates before attempting this accepted candidate.\n")
		b.WriteString("- After integration, run bounded build/test/smoke checks on the canonical target branch and publish exact command/status/head evidence.\n")
		b.WriteString("- Treat this as the full-product assembly boundary: compare the integrated target against the operator brief, acceptance criteria, and source refs. Lane ACCEPTED evidence is necessary but not sufficient for product completion.\n")
		b.WriteString("- If verifier/review mesh tooling is available, route or run it against the integrated target here; this is where cross-lane bugs should be caught, not in the first lane review.\n")
		b.WriteString("- If integration fails because the source branch/head is unavailable, verify with `git cat-file -e <head>^{commit}` and visible refs, then create/claim a rebuild follow-up instead of repeatedly retrying the same dead source.\n")
	case "rebuild":
		b.WriteString("- The accepted candidate is reviewed product evidence, but its source branch/head is unavailable in current repository reality.\n")
		b.WriteString("- Do not keep retrying the missing head_sha or try to recreate the exact lost commit. Build a fresh equivalent candidate from the root/operator brief, acceptance criteria, and the referenced review/decision evidence.\n")
		b.WriteString("- Before coding, restate the product interpretation and compare it with the operator request; treat an adjacent scaffold or editor that misses the requested workflow as a spec-fidelity failure.\n")
		b.WriteString("- Implement the smallest complete replacement that satisfies the accepted candidate's intent, run bounded build/test/smoke checks, then publish a new project_branch_review_ready packet and submit a fresh patch queue item for the new head.\n")
		b.WriteString("- Include negative source-recovery evidence and the new branch/head/test evidence in the task result so integrators stop chasing the dead SHA.\n")
	case "validation":
		if strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
			b.WriteString("- This validation task exists because a terminal BLOCKED patch queue decision names missing evidence, not because new implementation is needed.\n")
			b.WriteString("- First attempt to produce the missing evidence in your own validation checkout, including build/test/browser/smoke checks named by the decision summary or evidence-gap docs.\n")
			b.WriteString("- Publish durable validation evidence with exact command/status/head details. Evidence only supersedes this blocker when it names the same branch_id and head_sha, also names the exact queue_id and item_id, carries an explicit pass verdict, is newer than the latest BLOCKED decision, and is not a heartbeat, reflection board, agent state, or coordination handoff note.\n")
			b.WriteString("- If the evidence passes and you own the referenced branch, call project_patch_queue_submit with a fresh item_id plus supersedes_item_id, supersedes_queue_id, and validation_doc_key/evidence_doc_key so reviewers see a queue-facing candidate instead of this terminal item.\n")
			b.WriteString("- If the evidence passes and you are not the branch owner, create or delegate a concrete follow-up for the branch owner or an eligible integrator with exact project_id, branch_id, head_sha, old queue_id/item_id, suggested fresh item_id, and validation_doc_key. Completing with only a recommendation is incomplete.\n")
			b.WriteString("- If the evidence fails because the product or code is wrong, create a revision follow-up or materialize a new owned revision branch from the referenced candidate branch/head instead of requeueing stale evidence or repackaging the same-head blocker.\n")
			b.WriteString("- Block only after a concrete bounded attempt proves the missing evidence cannot be produced in this cycle.\n")
		} else {
			b.WriteString("- Validate the accepted candidate against the review packet and decision evidence.\n")
			b.WriteString("- Publish test/review evidence before marking project convergence complete.\n")
		}
	default:
		b.WriteString("- Revise or unblock the candidate according to the decision evidence.\n")
		b.WriteString("- Read this task's evidence_gap plus any linked validation/review task evidence docs; newer concrete build/test/browser failures supersede older generic missing-evidence summaries.\n")
		b.WriteString("- Treat candidate_pathset as historical changed-path evidence, not automatic claim scope; split or narrow the repair lane when another active branch owns part of that candidate pathset.\n")
		b.WriteString("- Publish through the durable repair ladder before completion: project_branch_commit with push=true, then project_branch_review_ready, then project_patch_queue_submit with a fresh item_id/supersession evidence.\n")
	}
	switch kind {
	case "integration":
		b.WriteString("- Do not create a parallel private implementation of the same branch scope.\n")
		b.WriteString("- Materialize the referenced candidate or canonical integration checkout only as needed to run project_patch_queue_integrate and post-integration smoke evidence.\n")
	case "rebuild":
		b.WriteString("- Use a new branch/checkout for the replacement candidate; the old accepted branch is historical evidence, not an editable working branch.\n")
		b.WriteString("- Avoid private-only completion: publish durable review-ready and patch queue evidence before marking this lane done.\n")
	case "validation":
		b.WriteString("- Do not create a parallel private implementation of the same branch scope.\n")
		b.WriteString("- Materialize the referenced candidate branch in your own validation checkout when needed; do not merge, rebase, push, or mutate the candidate branch.\n")
	default:
		b.WriteString("- Continue on the referenced implementation branch/checkout when you are the branch owner.\n")
		b.WriteString("- If this task references a terminal BLOCKED head, treat that branch/head as read-only historical evidence even if you were its original owner. If you are not the referenced branch owner, do not edit a validation/review checkout directly. Materialize a new owned revision branch from the referenced candidate branch/head: use the referenced branch as base_branch, leave branch_name omitted or choose a new owned branch name, then commit with project_branch_commit push=true, publish project_branch_review_ready, and submit a fresh patch queue item_id.\n")
		b.WriteString("- Bounded commits and pushes through project_branch_commit are authorized when needed to publish the revised HEAD for peer review; git merge, rebase, destructive branch replacement, and unrelated workspace mutation remain unauthorized.\n")
	}
	if strings.TrimSpace(extraContext) != "" {
		b.WriteString("\nAdditional context:\n")
		b.WriteString(strings.TrimSpace(extraContext))
		b.WriteString("\n")
	}
	return strings.TrimSpace(b.String())
}

func projectPatchQueueFollowupClaimScopeHints(item ProjectPatchQueueItemRecord, kind string) []string {
	if strings.EqualFold(strings.TrimSpace(kind), "revision") {
		return nil
	}
	return projectPatchQueueFollowupPathset(item)
}

func projectPatchQueueFollowupPathset(item ProjectPatchQueueItemRecord) []string {
	paths := uniqueTrimmedCSVStrings(item.Pathset)
	if len(paths) > 0 {
		return paths
	}
	raw := strings.TrimSpace(item.PathsetJSON)
	if raw == "" {
		return nil
	}
	var payload struct {
		Paths []string `json:"paths"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err == nil && len(payload.Paths) > 0 {
		return uniqueTrimmedCSVStrings(payload.Paths)
	}
	var array []string
	if err := json.Unmarshal([]byte(raw), &array); err == nil && len(array) > 0 {
		return uniqueTrimmedCSVStrings(array)
	}
	return nil
}

func projectPatchQueueFollowupSlug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash && b.Len() > 0 {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "item"
	}
	if len(out) > 120 {
		out = strings.Trim(out[:120], "-")
	}
	return out
}
