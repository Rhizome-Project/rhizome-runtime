package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path"
	"sort"
	"strings"
)

type ProjectBranchReviewReadyTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	workdir        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectBranchReviewReadyTool(client *RhizomeClient, workspaceID, agentID, workdir string) *ProjectBranchReviewReadyTool {
	return &ProjectBranchReviewReadyTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		workdir:     strings.TrimSpace(workdir),
	}
}

func (t *ProjectBranchReviewReadyTool) Name() string { return "project_branch_review_ready" }

func (t *ProjectBranchReviewReadyTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectBranchReviewReadyTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectBranchReviewReadyTool) Description() string {
	return "Publish a project branch review packet, mark the branch READY_FOR_REVIEW, and when runtime binding is available submit the controlled patch queue handoff. It does not merge, push, rebase, or mutate git state."
}

func (t *ProjectBranchReviewReadyTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id":          map[string]any{"type": "string", "description": "Project id."},
			"branch_id":           map[string]any{"type": "string", "description": "Existing project branch id."},
			"branch_name":         map[string]any{"type": "string", "description": "Existing branch name when branch_id is unknown."},
			"repo_id":             map[string]any{"type": "string", "description": "Optional repository id guard."},
			"local_path":          map[string]any{"type": "string", "description": "Optional local checkout path for read-only git evidence. Defaults to branch checkout path or agent workdir."},
			"head_sha":            map[string]any{"type": "string", "description": "Observed branch HEAD SHA. If omitted, the tool tries read-only git rev-parse HEAD."},
			"base_sha":            map[string]any{"type": "string", "description": "Optional observed base SHA. If omitted, the tool tries read-only git rev-parse on the base branch."},
			"base_branch":         map[string]any{"type": "string", "description": "Optional base branch override."},
			"write_scope_json":    map[string]any{"type": "string", "description": "Optional JSON write scope override. Defaults to the existing branch scope."},
			"review_summary":      map[string]any{"type": "string", "description": "Short factual review packet summary."},
			"verification_status": map[string]any{"type": "string", "description": "passed or not_applicable. Required before READY_FOR_REVIEW."},
			"verification_command": map[string]any{
				"type":        "string",
				"description": "Command or check used for verification. Required when verification_status is passed unless test_doc_keys are provided.",
			},
			"verification_exit_code":      map[string]any{"type": "integer", "description": "Optional verification exit code."},
			"verification_output_summary": map[string]any{"type": "string", "description": "Short verification output summary."},
			"test_doc_keys":               map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Optional workspace doc keys with test/review evidence."},
			"risk_notes":                  map[string]any{"type": "string", "description": "Known residual risks or manual-review notes."},
			"submit_to_patch_queue":       map[string]any{"type": "boolean", "description": "Optional override. Defaults to true when runtime binding is available, creating the controlled patch queue handoff immediately after READY_FOR_REVIEW."},
		},
		"required": []string{"project_id", "review_summary", "verification_status"},
	}
}

func (t *ProjectBranchReviewReadyTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_branch_review_ready is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_branch_review_ready", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	runtimeBinding := AgentRuntimeBinding{}
	if t.runtimeBinding != nil {
		runtimeBinding = t.runtimeBinding()
	}
	branchID := projectToolRefArg(args, "branch_id")
	branchName := projectToolRefArg(args, "branch_name")
	if branchID == "" && branchName == "" {
		return &ToolResult{Output: "branch_id or branch_name is required", IsError: true}
	}
	reviewSummary := strings.TrimSpace(stringArg(args, "review_summary"))
	if reviewSummary == "" {
		return &ToolResult{Output: "review_summary is required", IsError: true}
	}
	verificationStatus, err := normalizeProjectBranchReviewVerificationStatus(stringArg(args, "verification_status"))
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	testDocKeys := stringSliceArg(args, "test_doc_keys")
	verificationCommand := strings.TrimSpace(stringArg(args, "verification_command"))
	if verificationStatus == "passed" && verificationCommand == "" && len(testDocKeys) == 0 {
		return &ToolResult{Output: "verification_command or test_doc_keys is required when verification_status is passed", IsError: true}
	}
	// CW-D1: a "passed" verification must not carry a non-zero exit code. Distinguish an explicitly
	// supplied exit code from an absent one (default 0) so the common path is unaffected; reject the
	// self-contradictory passed+failed packet before it is recorded for the review mesh.
	if verificationStatus == "passed" {
		if raw, ok := args["verification_exit_code"]; ok {
			if code := intArg(raw, 0); code != 0 {
				return &ToolResult{Output: fmt.Sprintf("verification_status is passed but verification_exit_code is %d (a passed verification requires exit code 0)", code), IsError: true}
			}
		}
	}

	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	branch, err := selectProjectBranchReviewBranch(coordination.Branches, t.agentID, branchID, branchName, projectToolRefArg(args, "repo_id"))
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	repo, ok := selectProjectBranchReviewRepo(coordination.Repositories, branch.RepoID)
	if !ok {
		return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready requires branch repo %s to be READY with remote_url", branch.RepoID), IsError: true}
	}
	sourceTask, sourceTaskFound := projectBranchReviewSourceTask(coordination.Tasks, branch.ActiveTaskID)
	existingPathset, err := pathsetFromWriteScopeJSON(branch.WriteScopeJSON)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("existing branch write_scope_json pathset is invalid: %v", err), IsError: true}
	}
	if len(existingPathset) == 0 {
		return &ToolResult{Output: "existing branch write_scope_json must include non-empty paths before READY_FOR_REVIEW", IsError: true}
	}
	writeScopeOverride := strings.TrimSpace(stringArg(args, "write_scope_json"))
	writeScopeJSON := firstNonEmpty(writeScopeOverride, branch.WriteScopeJSON)
	if writeScopeJSON == "" || !json.Valid([]byte(writeScopeJSON)) {
		return &ToolResult{Output: "valid write_scope_json is required before READY_FOR_REVIEW", IsError: true}
	}
	pathset, err := pathsetFromWriteScopeJSON(writeScopeJSON)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("write_scope_json pathset is invalid: %v", err), IsError: true}
	}
	if len(pathset) == 0 {
		return &ToolResult{Output: "write_scope_json must include non-empty paths before READY_FOR_REVIEW", IsError: true}
	}
	if writeScopeOverride != "" && !pathsetIsCoveredByScope(pathset, existingPathset) {
		return &ToolResult{Output: "write_scope_json override cannot widen existing branch scope before READY_FOR_REVIEW", IsError: true}
	}
	if err := projectBranchReviewReadyValidateTaskIdentity(branch, sourceTaskFound, runtimeBinding); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	localPath := strings.TrimSpace(stringArg(args, "local_path"))
	if localPath == "" {
		task := WorkspaceTaskRecord{ProjectID: projectID}
		task.TaskID = strings.TrimSpace(runtimeBinding.TaskID)
		task.ProjectID = firstNonEmpty(strings.TrimSpace(runtimeBinding.ProjectID), projectID)
		if checkout, ok := selectProjectCompletionCheckout(coordination, branch, t.agentID, task); ok {
			localPath = strings.TrimSpace(checkout.LocalPath)
		}
	}
	localPath = firstNonEmpty(localPath, projectBranchReviewCheckoutPath(coordination.Checkouts, branch.CheckoutID), t.workdir)
	if localPath != "" && isGitCheckout(ctx, localPath) {
		if err := ensureGitGeneratedArtifactExcludes(ctx, localPath); err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready failed preparing generated artifact excludes: %v", err), IsError: true}
		}
		dirty, err := gitWorktreeDirty(ctx, localPath)
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready failed checking local git status: %v", err), IsError: true}
		}
		if dirty {
			return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready requires a clean committed checkout before READY_FOR_REVIEW; commit or clean local changes in %s", localPath), IsError: true}
		}
	}
	headSHA := strings.TrimSpace(stringArg(args, "head_sha"))
	if headSHA == "" && localPath != "" {
		headSHA, _ = gitRevParse(ctx, localPath, "HEAD")
	}
	if headSHA == "" {
		return &ToolResult{Output: "head_sha is required; provide it or run from a readable git checkout", IsError: true}
	}
	baseBranch := firstNonEmpty(strings.TrimSpace(stringArg(args, "base_branch")), branch.BaseBranch, repo.DefaultBranch, coordination.Profile.RepoDefaultBranch, "main")
	baseSHA := strings.TrimSpace(stringArg(args, "base_sha"))
	canonicalBaseSHA := ""
	if localPath != "" {
		canonicalBaseSHA = projectBranchCommitResolveBaseSHA(ctx, localPath, baseBranch)
	}
	if baseSHA == "" {
		baseSHA = canonicalBaseSHA
	} else if canonicalBaseSHA != "" && baseSHA != canonicalBaseSHA {
		return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready blocked stale base_sha %s: current %s is %s. Rebase or recreate the branch from the integrated base before submitting review evidence.", baseSHA, firstNonEmpty(strings.TrimSpace(baseBranch), "base"), canonicalBaseSHA), IsError: true}
	}
	if strings.TrimSpace(baseSHA) != "" && strings.TrimSpace(headSHA) == strings.TrimSpace(baseSHA) {
		return &ToolResult{Output: "project_branch_review_ready requires head_sha to differ from base_sha before READY_FOR_REVIEW", IsError: true}
	}
	if localPath != "" && isGitCheckout(ctx, localPath) && strings.TrimSpace(baseSHA) != "" && strings.TrimSpace(headSHA) != "" && !gitCommitIsAncestor(ctx, localPath, strings.TrimSpace(baseSHA), strings.TrimSpace(headSHA)) {
		return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready blocked stale branch lineage: head_sha %s is not descended from current base_sha %s. Rebase or recreate the branch from the integrated base before submitting review evidence.", strings.TrimSpace(headSHA), strings.TrimSpace(baseSHA)), IsError: true}
	}
	var changedPaths []string
	if localPath != "" && isGitCheckout(ctx, localPath) && strings.TrimSpace(baseSHA) != "" && strings.TrimSpace(headSHA) != "" {
		changedPaths, err = projectBranchReviewChangedPaths(ctx, localPath, baseSHA, headSHA)
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready failed deriving committed diff paths: %v", err), IsError: true}
		}
		if len(changedPaths) > 0 {
			if effectivePathset, derivedScopePaths := projectBranchCommitEffectiveWriteScope(pathset, changedPaths); len(derivedScopePaths) > 0 {
				pathset = effectivePathset
				writeScopeJSON = projectBranchCommitWriteScopeJSON(pathset)
			}
			if outOfScope := projectBranchCommitOutOfScopePaths(changedPaths, pathset); len(outOfScope) > 0 {
				return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready committed diff paths exceed branch write scope: %s. If these are legitimate lane sidecars or foundation files, repair the branch/role write_scope_json first; otherwise revise the branch before publishing review-ready evidence.", strings.Join(outOfScope, ", ")), IsError: true}
			}
		}
	}
	if err := projectBranchReviewReadyValidateTaskBinding(branch, sourceTask, sourceTaskFound, runtimeBinding, pathset, changedPaths); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	terminalDecision, hasTerminalDecision := latestProjectPatchQueueTerminalItemForBranchHead(coordination.PatchQueueItems, branch.BranchID, headSHA)
	if hasTerminalDecision && strings.EqualFold(strings.TrimSpace(terminalDecision.State), "REJECTED") {
		return &ToolResult{Output: projectPatchQueueSameHeadRejectedMessage("project_branch_review_ready", terminalDecision), IsError: true}
	}
	remoteHeadSHA, err := projectRemoteBranchHead(ctx, repo, branch.BranchName)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready could not verify published branch %q on the shared project remote: %v. Run project_branch_commit with push=true, then retry project_branch_review_ready.", branch.BranchName, err), IsError: true}
	}
	if strings.TrimSpace(remoteHeadSHA) == "" {
		return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready requires branch %q to be published on the shared project remote before READY_FOR_REVIEW. Run project_branch_commit with push=true so peer agents can validate origin/%s, then retry project_branch_review_ready.", branch.BranchName, branch.BranchName), IsError: true}
	}
	if strings.TrimSpace(remoteHeadSHA) != strings.TrimSpace(headSHA) {
		return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready remote branch %q is at %s, but requested head_sha is %s. Run project_branch_commit with push=true for the current HEAD, then retry project_branch_review_ready.", branch.BranchName, remoteHeadSHA, headSHA), IsError: true}
	}
	reviewDocKey := projectBranchReviewDocKey(projectID, branch.BranchID)
	patchQueueCandidate := projectBranchReviewPatchQueueCandidate(projectID, repo.RepoID, branch.BranchID, pathset, baseBranch, baseSHA, headSHA)
	sourceRefsDocKey := ""
	if candidateSourceRefsDocKey := projectSourceRefsDocKey(projectID); strings.TrimSpace(candidateSourceRefsDocKey) != "" {
		sourceRefsDoc, ok, err := t.client.GetDoc(ctx, t.workspaceID, candidateSourceRefsDocKey)
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready failed checking project source refs doc %s: %v", candidateSourceRefsDocKey, err), IsError: true}
		}
		if ok && sourceRefsDoc.ArchivedAt == nil && len(sourceDocKeysFromSourceRefsText(sourceRefsDoc.Content)) > 0 {
			sourceRefsDocKey = candidateSourceRefsDocKey
		}
	}
	docContent := renderProjectBranchReviewDoc(projectBranchReviewDocInput{
		ProjectID:                 projectID,
		Repo:                      repo,
		Branch:                    branch,
		SourceTask:                sourceTask,
		SourceTaskFound:           sourceTaskFound,
		BaseBranch:                baseBranch,
		BaseSHA:                   baseSHA,
		HeadSHA:                   headSHA,
		WriteScopeJSON:            writeScopeJSON,
		Pathset:                   pathset,
		LocalPath:                 localPath,
		ReviewSummary:             reviewSummary,
		VerificationStatus:        verificationStatus,
		VerificationCommand:       verificationCommand,
		VerificationExitCode:      intArg(args["verification_exit_code"], 0),
		VerificationOutputSummary: strings.TrimSpace(stringArg(args, "verification_output_summary")),
		TestDocKeys:               testDocKeys,
		RiskNotes:                 strings.TrimSpace(stringArg(args, "risk_notes")),
		PatchQueueCandidate:       patchQueueCandidate,
		SourceRefsDocKey:          sourceRefsDocKey,
	})
	if _, err := t.client.PutDoc(ctx, WorkspaceDocPutInput{
		WorkspaceID: t.workspaceID,
		DocKey:      reviewDocKey,
		Title:       "Branch Review Packet - " + branch.BranchName,
		Content:     docContent,
		UpdatedBy:   t.agentID,
	}); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready failed writing review packet %s: %v", reviewDocKey, err), IsError: true}
	}
	updatedBranch, err := t.client.RegisterProjectBranch(ctx, ProjectBranchRegisterInput{
		WorkspaceID:    t.workspaceID,
		ProjectID:      projectID,
		ActorID:        t.agentID,
		BranchID:       branch.BranchID,
		RepoID:         repo.RepoID,
		CheckoutID:     branch.CheckoutID,
		AgentID:        t.agentID,
		ActiveTaskID:   strings.TrimSpace(branch.ActiveTaskID),
		ActiveClaimID:  strings.TrimSpace(branch.ActiveClaimID),
		BranchName:     branch.BranchName,
		BranchKind:     firstNonEmpty(branch.BranchKind, "feature"),
		BaseBranch:     baseBranch,
		HeadSHA:        headSHA,
		BaseSHA:        baseSHA,
		WriteScopeJSON: writeScopeJSON,
		ReviewDocKey:   reviewDocKey,
		Status:         "READY_FOR_REVIEW",
	})
	if err != nil {
		statusDocKey, statusErr := t.writeProjectBranchReviewReadyFailureStatus(ctx, projectBranchReviewFailureStatusInput{
			ProjectID:    projectID,
			Branch:       branch,
			ReviewDocKey: reviewDocKey,
			HeadSHA:      headSHA,
			BaseSHA:      baseSHA,
			ErrorText:    err.Error(),
		})
		output := fmt.Sprintf("project_branch_review_ready failed marking branch READY_FOR_REVIEW: %v", err)
		output += ". Review packet is not a receipt; READY_FOR_REVIEW requires a branch registry transition with review_doc_key and mandatory_next_tool=project_patch_queue_submit. A patch queue item is a separate project_patch_queue_submit receipt."
		if statusDocKey != "" {
			output += fmt.Sprintf(" Wrote non-receipt status doc %s so this failure is durable and the owner can repair the review-ready precondition instead of treating the packet as published.", statusDocKey)
		}
		if statusErr != nil {
			output += fmt.Sprintf(" Failed writing non-receipt status doc: %v", statusErr)
		}
		return &ToolResult{Output: output, IsError: true}
	}
	payload := map[string]any{
		"project_id":              projectID,
		"repo_id":                 repo.RepoID,
		"branch_id":               updatedBranch.BranchID,
		"branch_name":             updatedBranch.BranchName,
		"branch_status":           updatedBranch.Status,
		"review_doc_key":          reviewDocKey,
		"head_sha":                headSHA,
		"remote_head_sha":         remoteHeadSHA,
		"base_branch":             baseBranch,
		"base_sha":                baseSHA,
		"pathset":                 pathset,
		"remote_branch_published": true,
		"patch_queue_candidate":   patchQueueCandidate,
		"receipt_state":           "branch_registry_ready_for_review",
		"receipt_requirements": []string{
			"project_branch_registry.status=READY_FOR_REVIEW",
			"project_branch_registry.review_doc_key=" + reviewDocKey,
			"mandatory_next_tool=project_patch_queue_submit before integration/review agents treat the candidate as queued",
		},
		"mandatory_next_tool": "project_patch_queue_submit",
		"no_git_mutation":     true,
		"auto_merge_enabled":  false,
		"guidance":            "Branch is ready for review evidence. Auto-merge remains disabled; integration must use explicit review/patch queue gates.",
	}
	if hasTerminalDecision && strings.EqualFold(strings.TrimSpace(terminalDecision.State), "BLOCKED") {
		payload["queue_state_blocking"] = true
		payload["patch_queue_terminal_decision"] = projectPatchQueueTerminalDecisionPayload(terminalDecision)
		payload["patch_queue_guidance"] = projectPatchQueueSameHeadBlockedGuidance(terminalDecision)
		payload["guidance"] = "Branch review evidence was refreshed, but durable patch queue state remains BLOCKED for this same head. Resolve the queue-facing blocker before claiming completion or integration readiness."
	}
	autoSubmitDisabled := projectBranchReviewAutoSubmitDisabled(args)
	blockingTerminalDecision := projectBranchReviewHasBlockingTerminalDecision(hasTerminalDecision, terminalDecision)
	if autoSubmitDisabled {
		payload["patch_queue_auto_submit"] = "skipped"
		payload["patch_queue_auto_submit_skipped_reason"] = "submit_to_patch_queue=false"
	} else if blockingTerminalDecision {
		payload["patch_queue_auto_submit"] = "skipped"
		payload["patch_queue_auto_submit_skipped_reason"] = "same-head negative terminal patch queue decision must be resolved before fresh submission"
	} else {
		if submitResult, attempted := t.autoSubmitPatchQueue(ctx, projectID, updatedBranch.BranchID, reviewDocKey, headSHA, localPath); attempted {
			payload["patch_queue_auto_submit_attempted"] = true
			if submitResult == nil {
				payload["patch_queue_auto_submit"] = "failed"
				payload["patch_queue_auto_submit_error"] = "project_patch_queue_submit returned no result"
				raw, _ := json.MarshalIndent(payload, "", "  ")
				return &ToolResult{Output: string(raw), IsError: true}
			}
			if submitResult.IsError {
				payload["patch_queue_auto_submit"] = "failed"
				payload["patch_queue_auto_submit_error"] = submitResult.Output
				raw, _ := json.MarshalIndent(payload, "", "  ")
				return &ToolResult{Output: string(raw), IsError: true}
			}
			payload["patch_queue_auto_submit"] = "submitted"
			var submitPayload map[string]any
			if err := json.Unmarshal([]byte(submitResult.Output), &submitPayload); err == nil && len(submitPayload) > 0 {
				payload["patch_queue_submit_receipt"] = submitPayload
			} else {
				payload["patch_queue_submit_receipt"] = submitResult.Output
			}
			payload["mandatory_next_tool"] = "none"
			payload["guidance"] = "Branch is READY_FOR_REVIEW and visible in the durable patch queue. Integration still requires explicit review/validation gates."
			if requirements, ok := payload["receipt_requirements"].([]string); ok {
				payload["receipt_requirements"] = append(requirements, "project_patch_queue_items contains controlled_queue item for branch/head")
			}
		} else {
			payload["patch_queue_auto_submit"] = "skipped"
			payload["patch_queue_auto_submit_skipped_reason"] = "runtime binding unavailable; project_patch_queue_submit remains mandatory"
		}
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_review_ready completed for %s", updatedBranch.BranchID)}
	}
	return &ToolResult{Output: string(raw)}
}

func projectBranchReviewAutoSubmitDisabled(args map[string]any) bool {
	if args == nil {
		return false
	}
	value, ok := args["submit_to_patch_queue"]
	if !ok {
		return false
	}
	enabled, ok := value.(bool)
	if !ok {
		raw := strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
		enabled = raw == "1" || raw == "true" || raw == "yes" || raw == "on"
	}
	return !enabled
}

func projectBranchReviewHasBlockingTerminalDecision(hasDecision bool, decision ProjectPatchQueueItemRecord) bool {
	return hasDecision && projectPatchQueueNegativeTerminalState(decision.State)
}

func (t *ProjectBranchReviewReadyTool) autoSubmitPatchQueue(ctx context.Context, projectID, branchID, reviewDocKey, headSHA, localPath string) (*ToolResult, bool) {
	if t == nil || t.runtimeBinding == nil {
		return nil, false
	}
	binding := t.runtimeBinding()
	if strings.TrimSpace(binding.TaskID) == "" &&
		strings.TrimSpace(binding.SessionID) == "" &&
		strings.TrimSpace(binding.RunID) == "" &&
		strings.TrimSpace(binding.CapabilitySnapshotID) == "" {
		return nil, false
	}
	submitTool := NewProjectPatchQueueSubmitTool(t.client, t.workspaceID, t.agentID).
		WithWorkdir(t.workdir).
		WithRuntimeBinding(t.runtimeBinding)
	return submitTool.Execute(ctx, map[string]any{
		"project_id":          strings.TrimSpace(projectID),
		"branch_id":           strings.TrimSpace(branchID),
		"review_doc_key":      strings.TrimSpace(reviewDocKey),
		"head_sha":            strings.TrimSpace(headSHA),
		"local_path":          strings.TrimSpace(localPath),
		"controlled_queue":    true,
		"repo_authority_mode": "repoauthority_controlled_queue",
		"capability_snapshot_schema": firstNonEmpty(
			strings.TrimSpace(binding.CapabilitySnapshotSchema),
			daemonCapabilitySnapshotSchema,
		),
	}), true
}

type projectBranchReviewFailureStatusInput struct {
	ProjectID    string
	Branch       ProjectBranchRecord
	ReviewDocKey string
	HeadSHA      string
	BaseSHA      string
	ErrorText    string
}

func (t *ProjectBranchReviewReadyTool) writeProjectBranchReviewReadyFailureStatus(ctx context.Context, input projectBranchReviewFailureStatusInput) (string, error) {
	taskID := strings.TrimSpace(input.Branch.ActiveTaskID)
	if taskID == "" {
		return "", nil
	}
	docKey := "task." + sanitizeDocKeySegment(taskID) + ".review_ready_status"
	content := renderProjectBranchReviewReadyFailureStatusDoc(input)
	if _, err := t.client.PutDoc(ctx, WorkspaceDocPutInput{
		WorkspaceID: t.workspaceID,
		DocKey:      docKey,
		Title:       "Review Ready Status - " + strings.TrimSpace(input.Branch.BranchName),
		Content:     content,
		UpdatedBy:   t.agentID,
	}); err != nil {
		return docKey, err
	}
	return docKey, nil
}

func renderProjectBranchReviewReadyFailureStatusDoc(input projectBranchReviewFailureStatusInput) string {
	var b strings.Builder
	b.WriteString("schema: rhizome_review_ready_status_v1\n")
	b.WriteString("project_id: ")
	b.WriteString(strings.TrimSpace(input.ProjectID))
	b.WriteString("\n")
	b.WriteString("branch_id: ")
	b.WriteString(strings.TrimSpace(input.Branch.BranchID))
	b.WriteString("\n")
	b.WriteString("branch_name: ")
	b.WriteString(strings.TrimSpace(input.Branch.BranchName))
	b.WriteString("\n")
	if taskID := strings.TrimSpace(input.Branch.ActiveTaskID); taskID != "" {
		b.WriteString("task_id: ")
		b.WriteString(taskID)
		b.WriteString("\n")
	}
	b.WriteString("head_sha: ")
	b.WriteString(strings.TrimSpace(input.HeadSHA))
	b.WriteString("\n")
	if baseSHA := strings.TrimSpace(input.BaseSHA); baseSHA != "" {
		b.WriteString("base_sha: ")
		b.WriteString(baseSHA)
		b.WriteString("\n")
	}
	b.WriteString("review_doc_key: ")
	b.WriteString(strings.TrimSpace(input.ReviewDocKey))
	b.WriteString("\n")
	b.WriteString("review_packet_status: draft_not_receipt\n")
	b.WriteString("receipt_status: failed_before_branch_registry_transition\n")
	b.WriteString("failure: ")
	b.WriteString(strings.ReplaceAll(strings.TrimSpace(input.ErrorText), "\n", " "))
	b.WriteString("\n")
	b.WriteString("required_receipt:\n")
	b.WriteString("  - project_branch_registry.status=READY_FOR_REVIEW\n")
	b.WriteString("  - project_branch_registry.review_doc_key=")
	b.WriteString(strings.TrimSpace(input.ReviewDocKey))
	b.WriteString("\n")
	b.WriteString("  - mandatory_next_tool=project_patch_queue_submit before downstream review/integration treats the candidate as queued\n")
	b.WriteString("patch_queue_item_receipt: produced only by project_patch_queue_submit\n")
	b.WriteString("next_action: Repair the failed review-ready precondition, then rerun project_branch_review_ready for this head; do not treat the review packet document alone as READY_FOR_REVIEW progress.\n")
	return strings.TrimSpace(b.String())
}

type projectBranchReviewDocInput struct {
	ProjectID                 string
	Repo                      ProjectRepositoryRecord
	Branch                    ProjectBranchRecord
	SourceTask                WorkspaceTaskRecord
	SourceTaskFound           bool
	BaseBranch                string
	BaseSHA                   string
	HeadSHA                   string
	WriteScopeJSON            string
	Pathset                   []string
	LocalPath                 string
	ReviewSummary             string
	VerificationStatus        string
	VerificationCommand       string
	VerificationExitCode      int
	VerificationOutputSummary string
	TestDocKeys               []string
	RiskNotes                 string
	PatchQueueCandidate       map[string]any
	SourceRefsDocKey          string
}

func normalizeProjectBranchReviewVerificationStatus(value string) (string, error) {
	status := strings.ToLower(strings.TrimSpace(value))
	switch status {
	case "passed", "not_applicable":
		return status, nil
	default:
		return "", fmt.Errorf("verification_status must be passed or not_applicable before READY_FOR_REVIEW")
	}
}

func projectToolRefArg(args map[string]any, key string) string {
	return normalizeProjectToolRef(stringArg(args, key))
}

func resolveProjectToolActiveProjectID(toolName, workspaceID, requestedProjectID string, runtimeBinding func() AgentRuntimeBinding) (string, error) {
	toolName = firstNonEmpty(strings.TrimSpace(toolName), "project tool")
	requestedProjectID = normalizeProjectToolRef(requestedProjectID)
	workspaceID = strings.TrimSpace(workspaceID)
	activeProjectID := ""
	if runtimeBinding != nil {
		activeProjectID = normalizeProjectToolRef(runtimeBinding().ProjectID)
	}
	if activeProjectID == "" {
		if requestedProjectID == "" {
			return "", fmt.Errorf("%s project_id is required", toolName)
		}
		return requestedProjectID, nil
	}
	if requestedProjectID == "" || sameWorkspaceDocProjectID(requestedProjectID, activeProjectID) {
		return activeProjectID, nil
	}
	if workspaceID != "" && strings.EqualFold(requestedProjectID, workspaceID) {
		return activeProjectID, nil
	}
	return "", fmt.Errorf("%s blocked project_id %s because the active task is bound to project_id %s; use the active project_id instead of switching projects", toolName, requestedProjectID, activeProjectID)
}

func normalizeProjectToolRef(value string) string {
	value = strings.TrimSpace(value)
	for {
		trimmed := strings.Trim(value, " \t\r\n`'\"")
		if trimmed == value {
			return value
		}
		value = trimmed
	}
}

func selectProjectBranchReviewBranch(branches []ProjectBranchRecord, agentID, branchID, branchName, repoID string) (ProjectBranchRecord, error) {
	return selectProjectBranchForOwnedTool("project_branch_review_ready", branches, agentID, branchID, branchName, repoID)
}

func selectProjectBranchForOwnedTool(toolName string, branches []ProjectBranchRecord, agentID, branchID, branchName, repoID string) (ProjectBranchRecord, error) {
	toolName = firstNonEmpty(strings.TrimSpace(toolName), "project branch tool")
	agentID = strings.TrimSpace(agentID)
	branchID = strings.TrimSpace(branchID)
	branchName = strings.TrimSpace(branchName)
	repoID = strings.TrimSpace(repoID)
	var matches []ProjectBranchRecord
	for _, branch := range branches {
		if branchID != "" && strings.TrimSpace(branch.BranchID) != branchID {
			continue
		}
		if branchID == "" && branchName != "" && strings.TrimSpace(branch.BranchName) != branchName {
			continue
		}
		if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			continue
		}
		if strings.TrimSpace(branch.AgentID) != "" && strings.TrimSpace(branch.AgentID) != agentID {
			return ProjectBranchRecord{}, fmt.Errorf("%s", projectBranchOwnerRoutingMessage(toolName, agentID, branch))
		}
		if projectBranchTerminalState(branch.Status) {
			if branchID != "" {
				return ProjectBranchRecord{}, fmt.Errorf("%s requires an existing non-terminal branch owned by this agent", toolName)
			}
			continue
		}
		if strings.TrimSpace(branch.BranchID) != "" && strings.TrimSpace(branch.BranchName) != "" {
			matches = append(matches, branch)
		}
	}
	if len(matches) == 0 {
		return ProjectBranchRecord{}, fmt.Errorf("%s requires an existing non-terminal branch owned by this agent", toolName)
	}
	if branchID == "" && len(matches) > 1 {
		return ProjectBranchRecord{}, fmt.Errorf("%s branch_name is ambiguous across repositories; provide branch_id or repo_id", toolName)
	}
	return matches[0], nil
}

func projectBranchTerminalState(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "MERGED", "INTEGRATED", "CLOSED", "CANCELLED", "CANCELED", "ABANDONED", "ARCHIVED":
		return true
	default:
		return false
	}
}

func projectBranchOwnerRoutingMessage(toolName, agentID string, branch ProjectBranchRecord) string {
	ownerID := firstNonEmpty(strings.TrimSpace(branch.AgentID), "<unknown>")
	activeTaskID := strings.TrimSpace(branch.ActiveTaskID)
	var b strings.Builder
	b.WriteString(toolName)
	b.WriteString(" requires an existing non-terminal branch owned by this agent")
	b.WriteString("; branch_id=" + firstNonEmpty(strings.TrimSpace(branch.BranchID), "<unknown>"))
	if strings.TrimSpace(branch.BranchName) != "" {
		b.WriteString(" branch_name=" + strings.TrimSpace(branch.BranchName))
	}
	b.WriteString(" is owned by agent " + ownerID)
	b.WriteString(", not " + firstNonEmpty(strings.TrimSpace(agentID), "<unknown>") + ".")
	b.WriteString(" Do not retry this owner-only tool as the non-owner.")
	if activeTaskID != "" {
		b.WriteString(" Route the next owner action to " + ownerID + " with agent_request request_kind=delegate_task task_id=" + activeTaskID + ".")
	} else {
		b.WriteString(" Create or delegate one bounded owner follow-up for " + ownerID + " with the exact project_id, branch_id, branch_name, and head_sha.")
	}
	if strings.TrimSpace(branch.HeadSHA) != "" {
		b.WriteString(" For independent revision work, materialize a new owned revision branch from " + strings.TrimSpace(branch.BranchName) + " at head_sha=" + strings.TrimSpace(branch.HeadSHA) + " instead of mutating the foreign branch.")
	}
	return b.String()
}

func projectBranchReviewChangedPaths(ctx context.Context, localPath, baseSHA, headSHA string) ([]string, error) {
	out, err := gitCombined(ctx, localPath, "diff", "--name-only", "--diff-filter=ACDMRT", strings.TrimSpace(baseSHA)+".."+strings.TrimSpace(headSHA))
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	paths := make([]string, 0)
	for _, raw := range strings.Split(out, "\n") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		normalized, err := normalizeReviewPath(raw)
		if err != nil {
			return nil, fmt.Errorf("committed path %q is invalid: %w", raw, err)
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		paths = append(paths, normalized)
	}
	sort.Strings(paths)
	return paths, nil
}

func selectProjectBranchReviewRepo(repos []ProjectRepositoryRecord, repoID string) (ProjectRepositoryRecord, bool) {
	repoID = strings.TrimSpace(repoID)
	for _, repo := range repos {
		if strings.TrimSpace(repo.RepoID) == repoID && projectCheckoutRegisterRepoReady(repo) {
			return repo, true
		}
	}
	return ProjectRepositoryRecord{}, false
}

func projectBranchReviewCheckoutPath(checkouts []ProjectCheckoutRecord, checkoutID string) string {
	checkoutID = strings.TrimSpace(checkoutID)
	if checkoutID == "" {
		return ""
	}
	for _, checkout := range checkouts {
		if strings.TrimSpace(checkout.CheckoutID) == checkoutID {
			if managerProjectCheckoutStatusTerminal(checkout.Status) {
				return ""
			}
			return strings.TrimSpace(checkout.LocalPath)
		}
	}
	return ""
}

func pathsetFromWriteScopeJSON(raw string) ([]string, error) {
	var payload struct {
		Paths []string `json:"paths"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &payload); err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(payload.Paths))
	seen := map[string]struct{}{}
	for _, item := range payload.Paths {
		normalized, err := normalizeReviewPath(item)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		paths = append(paths, normalized)
	}
	sort.Strings(paths)
	return paths, nil
}

func pathsetIsCoveredByScope(childPaths, parentScope []string) bool {
	if len(childPaths) == 0 || len(parentScope) == 0 {
		return false
	}
	for _, child := range childPaths {
		covered := false
		for _, parent := range parentScope {
			if scopePathCovers(parent, child) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func scopePathCovers(parent, child string) bool {
	parent = strings.TrimSpace(parent)
	child = strings.TrimSpace(child)
	if parent == "*" || parent == "**" {
		return true
	}
	if parent == child {
		return true
	}
	if strings.HasSuffix(parent, "/**") {
		prefix := strings.TrimSuffix(parent, "/**")
		return child == prefix || strings.HasPrefix(child, prefix+"/")
	}
	if strings.HasSuffix(parent, "/*") {
		prefix := strings.TrimSuffix(parent, "/*")
		if child == prefix || !strings.HasPrefix(child, prefix+"/") {
			return false
		}
		return !strings.Contains(strings.TrimPrefix(child, prefix+"/"), "/")
	}
	if scopePathGlobCovers(parent, child) {
		return true
	}
	return false
}

func scopePathIsScoped(scope string) bool {
	scope = strings.TrimSpace(scope)
	return scope == "*" || scope == "**" || strings.HasSuffix(scope, "/*") || strings.HasSuffix(scope, "/**") || scopePathIsValidGlob(scope)
}

func scopePathGlobCovers(scope, child string) bool {
	if !scopePathHasGlob(scope) {
		return false
	}
	matched, err := path.Match(scope, child)
	return err == nil && matched
}

func scopePathIsValidGlob(scope string) bool {
	if !scopePathHasGlob(scope) {
		return false
	}
	_, err := path.Match(scope, "")
	return err == nil
}

func scopePathHasGlob(scope string) bool {
	return strings.ContainsAny(scope, "*?[")
}

func normalizeReviewPath(raw string) (string, error) {
	value := strings.TrimSpace(strings.ReplaceAll(raw, "\\", "/"))
	if value == "" {
		return "", fmt.Errorf("path is required")
	}
	if strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("path contains NUL byte")
	}
	if len(value) >= 2 && value[1] == ':' {
		return "", fmt.Errorf("absolute paths are not allowed: %q", raw)
	}
	if strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("absolute paths are not allowed: %q", raw)
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("path escapes repo root: %q", raw)
	}
	return cleaned, nil
}

func gitRevParse(ctx context.Context, localPath, ref string) (string, error) {
	out, err := exec.CommandContext(ctx, "git", "-C", strings.TrimSpace(localPath), "rev-parse", strings.TrimSpace(ref)).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func projectRemoteBranchHead(ctx context.Context, repo ProjectRepositoryRecord, branchName string) (string, error) {
	remoteURL := strings.TrimSpace(repo.RemoteURL)
	branchName = strings.TrimSpace(branchName)
	if remoteURL == "" {
		return "", fmt.Errorf("repository %s has no remote_url", strings.TrimSpace(repo.RepoID))
	}
	if branchName == "" {
		return "", fmt.Errorf("branch_name is required")
	}
	out, err := exec.CommandContext(ctx, "git", "ls-remote", "--heads", remoteURL, branchName).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git ls-remote failed: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	expectedRef := "refs/heads/" + branchName
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 {
			continue
		}
		if fields[1] == expectedRef && strings.TrimSpace(fields[0]) != "" {
			return strings.TrimSpace(fields[0]), nil
		}
	}
	return "", nil
}

func projectBranchReviewDocKey(projectID, branchID string) string {
	return "project." + sanitizeDocKeySegment(projectID) + ".branch." + sanitizeDocKeySegment(branchID) + ".review"
}

func projectBranchReviewPatchQueueCandidate(projectID, repoID, branchID string, pathset []string, baseBranch, baseSHA, headSHA string) map[string]any {
	return map[string]any{
		"queue_id":   sanitizeRefSegment("patchq-" + projectID + "-" + repoID),
		"item_id":    sanitizeRefSegment("patchitem-" + branchID),
		"pathset":    pathset,
		"base_ref":   strings.TrimSpace(baseBranch),
		"base_sha":   strings.TrimSpace(baseSHA),
		"head_sha":   strings.TrimSpace(headSHA),
		"auto_merge": false,
		"next_gate":  "manual_review_or_patch_queue_validation",
	}
}

func projectBranchReviewSourceTask(tasks []WorkspaceTaskRecord, taskID string) (WorkspaceTaskRecord, bool) {
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

func projectBranchReviewReadyValidateTaskIdentity(branch ProjectBranchRecord, sourceTaskFound bool, binding AgentRuntimeBinding) error {
	taskID := strings.TrimSpace(binding.TaskID)
	if taskID == "" {
		return nil
	}
	branchTaskID := strings.TrimSpace(branch.ActiveTaskID)
	if branchTaskID == "" {
		return fmt.Errorf("project_branch_review_ready blocked branch %s: runtime active task %s is not bound to this branch; recreate or repair the branch active_task_id before publishing READY_FOR_REVIEW", strings.TrimSpace(branch.BranchID), taskID)
	}
	if branchTaskID != taskID {
		return fmt.Errorf("project_branch_review_ready blocked branch %s active task binding: branch active_task_id=%s but runtime task_id=%s; do not relabel an owned branch under a different claimed task", strings.TrimSpace(branch.BranchID), branchTaskID, taskID)
	}
	if !sourceTaskFound {
		return fmt.Errorf("project_branch_review_ready blocked branch %s: claimed task %s is missing from project coordination, so lane/pathset binding cannot be verified fail-closed", strings.TrimSpace(branch.BranchID), taskID)
	}
	return nil
}

func projectBranchReviewReadyValidateTaskBinding(branch ProjectBranchRecord, sourceTask WorkspaceTaskRecord, sourceTaskFound bool, binding AgentRuntimeBinding, pathset, changedPaths []string) error {
	if err := projectBranchReviewReadyValidateTaskIdentity(branch, sourceTaskFound, binding); err != nil {
		return err
	}
	taskID := strings.TrimSpace(binding.TaskID)
	if taskID == "" {
		return nil
	}
	allowedScope, err := projectBranchReviewClaimedTaskScope(sourceTask, binding)
	if err != nil {
		return fmt.Errorf("project_branch_review_ready blocked branch %s: claimed task %s pathset is invalid: %v", strings.TrimSpace(branch.BranchID), taskID, err)
	}
	if len(allowedScope) == 0 {
		return fmt.Errorf("project_branch_review_ready blocked branch %s: claimed task %s has no write_scope_hints or claim_write_scope_json, so READY_FOR_REVIEW pathset binding cannot be verified fail-closed", strings.TrimSpace(branch.BranchID), taskID)
	}
	if len(pathset) == 0 {
		return fmt.Errorf("project_branch_review_ready blocked branch %s: branch pathset is empty, so claimed task %s pathset binding cannot be verified", strings.TrimSpace(branch.BranchID), taskID)
	}
	if len(changedPaths) > 0 {
		if effectiveScope, derivedScopePaths := projectBranchCommitEffectiveWriteScope(allowedScope, changedPaths); len(derivedScopePaths) > 0 {
			allowedScope = effectiveScope
		}
		if !pathsetIsCoveredByScope(changedPaths, allowedScope) {
			return fmt.Errorf("project_branch_review_ready blocked branch %s: committed diff paths %s are outside claimed task %s write scope %s; repair task/branch binding before review-ready publication", strings.TrimSpace(branch.BranchID), strings.Join(changedPaths, ", "), taskID, strings.Join(allowedScope, ", "))
		}
		return nil
	}
	if !pathsetIsCoveredByScope(pathset, allowedScope) {
		return fmt.Errorf("project_branch_review_ready blocked branch %s: branch pathset %s is outside claimed task %s write scope %s; do not publish one task lane under another task's label", strings.TrimSpace(branch.BranchID), strings.Join(pathset, ", "), taskID, strings.Join(allowedScope, ", "))
	}
	return nil
}

func projectBranchReviewClaimedTaskScope(task WorkspaceTaskRecord, binding AgentRuntimeBinding) ([]string, error) {
	paths := make([]string, 0)
	paths = append(paths, task.WriteScopeHints...)
	if raw := strings.TrimSpace(task.TaskRequirementsJSON); raw != "" && raw != "{}" {
		var payload map[string]any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			return nil, err
		}
		for _, key := range []string{"write_scope_hints", "candidate_pathset", "pathset"} {
			paths = append(paths, stringSliceFromAny(payload[key])...)
		}
	}
	if claimScopeJSON := strings.TrimSpace(binding.ClaimWriteScopeJSON); claimScopeJSON != "" {
		claimPaths, err := pathsetFromWriteScopeJSON(claimScopeJSON)
		if err != nil {
			return nil, err
		}
		paths = append(paths, claimPaths...)
	}
	return normalizeProjectBranchReviewScopePaths(paths)
}

func normalizeProjectBranchReviewScopePaths(values []string) ([]string, error) {
	seen := map[string]struct{}{}
	paths := make([]string, 0, len(values))
	for _, value := range values {
		normalized, err := normalizeReviewPath(value)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		paths = append(paths, normalized)
	}
	sort.Strings(paths)
	return paths, nil
}

func renderProjectBranchReviewDoc(input projectBranchReviewDocInput) string {
	rawPatchQueue, _ := json.MarshalIndent(input.PatchQueueCandidate, "", "  ")
	var b strings.Builder
	b.WriteString("# Branch Review Packet - ")
	b.WriteString(strings.TrimSpace(input.Branch.BranchName))
	b.WriteString("\n\n")
	b.WriteString("- project_id: ")
	b.WriteString(strings.TrimSpace(input.ProjectID))
	b.WriteString("\n")
	b.WriteString("- repo_id: ")
	b.WriteString(strings.TrimSpace(input.Repo.RepoID))
	b.WriteString("\n")
	b.WriteString("- branch_id: ")
	b.WriteString(strings.TrimSpace(input.Branch.BranchID))
	b.WriteString("\n")
	b.WriteString("- branch_name: ")
	b.WriteString(strings.TrimSpace(input.Branch.BranchName))
	b.WriteString("\n")
	b.WriteString("- status: READY_FOR_REVIEW\n")
	b.WriteString("- base_branch: ")
	b.WriteString(strings.TrimSpace(input.BaseBranch))
	b.WriteString("\n")
	b.WriteString("- base_sha: ")
	b.WriteString(strings.TrimSpace(input.BaseSHA))
	b.WriteString("\n")
	b.WriteString("- head_sha: ")
	b.WriteString(strings.TrimSpace(input.HeadSHA))
	b.WriteString("\n")
	b.WriteString("- auto_merge_enabled: false\n\n")
	b.WriteString("## Runnable/Review Provenance\n")
	if strings.TrimSpace(input.LocalPath) != "" {
		b.WriteString("- checkout_path: ")
		b.WriteString(strings.TrimSpace(input.LocalPath))
		b.WriteString("\n")
	}
	b.WriteString("- branch_name: ")
	b.WriteString(strings.TrimSpace(input.Branch.BranchName))
	b.WriteString("\n")
	b.WriteString("- head_sha: ")
	b.WriteString(strings.TrimSpace(input.HeadSHA))
	b.WriteString("\n")
	b.WriteString("- review_granularity: lane_scoped_candidate\n")
	if taskID := strings.TrimSpace(input.Branch.ActiveTaskID); taskID != "" {
		b.WriteString("- source_task_id: ")
		b.WriteString(taskID)
		b.WriteString("\n")
	}
	if input.SourceTaskFound {
		if lane := strings.TrimSpace(input.SourceTask.ProjectLane); lane != "" {
			b.WriteString("- source_task_lane: ")
			b.WriteString(lane)
			b.WriteString("\n")
		}
		if title := strings.TrimSpace(input.SourceTask.Title); title != "" {
			b.WriteString("- source_task_title: ")
			b.WriteString(title)
			b.WriteString("\n")
		}
		if len(input.SourceTask.WriteScopeHints) > 0 {
			b.WriteString("- source_task_write_scope_hints: ")
			b.WriteString(strings.Join(input.SourceTask.WriteScopeHints, ", "))
			b.WriteString("\n")
		}
	}
	b.WriteString("- lane_acceptance_refs: source task, review summary, verification evidence, write scope, pathset, and changed files at this head\n")
	b.WriteString("- full_product_acceptance_refs: operator brief, acceptance criteria, source refs, and final integration evidence; use these to catch false full-product claims, not to reject unrelated missing lanes from a scoped implementation slice\n")
	b.WriteString("- reviewer_expectation: verify this branch as a lane-scoped candidate. Judge the owned pathset and task/lane claims against the operator brief, acceptance criteria, and core user transformation; do not block only because sibling lanes or full-product integration remain unfinished.\n")
	b.WriteString("- lane_review_rule: ACCEPTED may mean this lane is correct and ready for integration, not that the whole product is complete. Use BLOCKED_SPEC_DRIFT only for defects, missing evidence, or false claims inside this lane's stated scope.\n\n")
	if strings.TrimSpace(input.SourceRefsDocKey) != "" {
		b.WriteString("## Source Artifact Fidelity\n")
		b.WriteString("- source_refs_doc_key: ")
		b.WriteString(strings.TrimSpace(input.SourceRefsDocKey))
		b.WriteString("\n")
		b.WriteString("- source_requirements_trace_doc_key: ")
		b.WriteString(projectSourceRequirementsTraceDocKey(input.ProjectID))
		b.WriteString("\n")
		b.WriteString("- reviewer_expectation: compare the lane candidate against `rhizome_source_requirements_trace_v1` for the acceptance-critical anchors it claims to cover. Do not require this branch to satisfy unrelated sibling-lane anchors before integration.\n\n")
		b.WriteString("- patch_queue_acceptance_gate: ACCEPTED decisions must include `rhizome_spec_fidelity_review_v1` or `source_fidelity_status: passed`; use BLOCKED_SPEC_DRIFT when product identity, lane-owned anchors, or non-droppable claims do not match.\n\n")
	}
	b.WriteString("## Summary\n")
	b.WriteString(strings.TrimSpace(input.ReviewSummary))
	b.WriteString("\n\n")
	b.WriteString("## Verification\n")
	b.WriteString("- status: ")
	b.WriteString(input.VerificationStatus)
	b.WriteString("\n")
	if input.VerificationCommand != "" {
		b.WriteString("- command: ")
		b.WriteString(input.VerificationCommand)
		b.WriteString("\n")
	}
	b.WriteString("- exit_code: ")
	b.WriteString(fmt.Sprint(input.VerificationExitCode))
	b.WriteString("\n")
	if input.VerificationOutputSummary != "" {
		b.WriteString("- output_summary: ")
		b.WriteString(input.VerificationOutputSummary)
		b.WriteString("\n")
	}
	if len(input.TestDocKeys) > 0 {
		b.WriteString("- test_doc_keys: ")
		b.WriteString(strings.Join(input.TestDocKeys, ", "))
		b.WriteString("\n")
	}
	b.WriteString("\n## Write Scope\n```json\n")
	b.WriteString(strings.TrimSpace(input.WriteScopeJSON))
	b.WriteString("\n```\n\n")
	b.WriteString("## Pathset\n")
	for _, p := range input.Pathset {
		b.WriteString("- ")
		b.WriteString(p)
		b.WriteString("\n")
	}
	b.WriteString("\n## Patch Queue Candidate\n```json\n")
	b.Write(rawPatchQueue)
	b.WriteString("\n```\n\n")
	b.WriteString("## Integration Rule\n")
	b.WriteString("This packet is review evidence only. It does not authorize automatic merge, push, rebase, or branch mutation. Integration must pass explicit review or patch queue validation gates.\n")
	if guidance := visualAcceptanceReviewPacketGuidance(input.Pathset); strings.TrimSpace(guidance) != "" {
		b.WriteString("\n## Visual Acceptance Protocol\n")
		b.WriteString(guidance)
	}
	if input.RiskNotes != "" {
		b.WriteString("\n## Risk Notes\n")
		b.WriteString(input.RiskNotes)
		b.WriteString("\n")
	}
	return b.String()
}
