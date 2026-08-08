package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type ProjectCheckoutMaterializeTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	ownerUserID    string
	workdir        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectCheckoutMaterializeTool(client *RhizomeClient, workspaceID, agentID, ownerUserID, workdir string) *ProjectCheckoutMaterializeTool {
	return &ProjectCheckoutMaterializeTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		ownerUserID: strings.TrimSpace(ownerUserID),
		workdir:     strings.TrimSpace(workdir),
	}
}

func (t *ProjectCheckoutMaterializeTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectCheckoutMaterializeTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectCheckoutMaterializeTool) Name() string { return "project_checkout_materialize" }

func (t *ProjectCheckoutMaterializeTool) Description() string {
	return "Clone or reuse a READY project repository inside this agent workdir. Implementation-shaped tasks create/switch a feature branch and register branch evidence; review/validation/QA tasks create a validation checkout without reserving implementation branch ownership. If the requested branch exists on origin, it checks out that remote branch for peer review. It does not commit, push, merge, or delete files."
}

func (t *ProjectCheckoutMaterializeTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id":  map[string]any{"type": "string", "description": "Project id."},
			"repo_id":     map[string]any{"type": "string", "description": "Repository id. Defaults to canonical READY repository."},
			"local_path":  map[string]any{"type": "string", "description": "Checkout destination inside this agent workdir. Defaults to a short deterministic project-checkouts/<project-repo-slug> path."},
			"branch_name": map[string]any{"type": "string", "description": "Branch to create/switch to. Defaults to a short deterministic agent branch for implementation tasks, or the canonical base branch for validation checkouts."},
			"base_branch": map[string]any{"type": "string", "description": "Base branch. Defaults to repo/default project branch or main."},
			"expected_head_sha": map[string]any{
				"type":        "string",
				"description": "Optional expected HEAD SHA for peer review/validation. When register_branch=false, this pins the checkout to the advertised candidate evidence.",
			},
			"active_task_id":   map[string]any{"type": "string", "description": "Optional active task id for an already CLAIMED task; also used as active_claim_id when active_claim_id is omitted."},
			"active_claim_id":  map[string]any{"type": "string", "description": "Optional active claim id for an already CLAIMED task."},
			"write_scope_json": map[string]any{"type": "string", "description": "JSON write scope for branch ownership. Defaults to this agent's active project role scope if available."},
			"register_branch":  map[string]any{"type": "boolean", "description": "Whether to register implementation branch evidence. Defaults true for implementation-shaped tasks and false for review/validation/QA tasks."},
			"allow_dirty_existing": map[string]any{
				"type":        "boolean",
				"description": "Allow reusing an existing checkout with uncommitted changes. Defaults false.",
			},
		},
		"required": []string{"project_id"},
	}
}

func (t *ProjectCheckoutMaterializeTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_checkout_materialize is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	if strings.TrimSpace(t.workdir) == "" {
		return &ToolResult{Output: "project_checkout_materialize requires an agent workdir", IsError: true}
	}
	projectID := strings.TrimSpace(stringArg(args, "project_id"))
	if projectID == "" {
		return &ToolResult{Output: "project_id is required", IsError: true}
	}
	activeTaskID := strings.TrimSpace(stringArg(args, "active_task_id"))
	activeClaimID := strings.TrimSpace(stringArg(args, "active_claim_id"))
	if activeTaskID != "" && activeClaimID == "" {
		activeClaimID = activeTaskID
	}
	if activeClaimID != "" && activeTaskID == "" {
		activeTaskID = activeClaimID
	}

	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	if activeTaskID != "" && !projectCoordinationContainsOpenTask(coordination, activeTaskID) {
		return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize blocked stale active_task_id %s: it is not an open task in project %s. Use a current implementation task from project coordination instead of reusing old cross-project task context.", activeTaskID, projectID), IsError: true}
	}
	registerBranch := true
	registerBranchExplicit := false
	if explicit := optionalBoolArg(args, "register_branch"); explicit != nil {
		registerBranch = *explicit
		registerBranchExplicit = true
	}
	inferredActiveTaskID := ""
	branchTaskID := activeTaskID
	if branchTaskID == "" && activeClaimID == "" {
		inferredActiveTaskID = inferProjectCheckoutMaterializeTaskID(coordination)
		if inferredActiveTaskID != "" {
			branchTaskID = inferredActiveTaskID
		}
	}
	if registerBranchRuntimeBound(t.runtimeBinding) {
		binding := t.runtimeBinding()
		if activeTaskID == "" && binding.TaskID != "" && (binding.ProjectID == "" || sameWorkspaceDocProjectID(binding.ProjectID, projectID)) {
			activeTaskID = binding.TaskID
			activeClaimID = firstNonEmpty(activeClaimID, binding.TaskID)
			branchTaskID = binding.TaskID
		}
		if err := validateProjectCheckoutRuntimeBinding(binding, projectID, firstNonEmpty(activeTaskID, branchTaskID), "project_checkout_materialize"); err != nil {
			return &ToolResult{Output: err.Error(), IsError: true}
		}
		if registerBranch && !registerBranchExplicit {
			if task, ok := projectCoordinationTaskByID(coordination, activeTaskID); ok && projectCheckoutTaskDefaultsToValidationCheckout(task) {
				registerBranch = false
			}
		}
		if registerBranch {
			if err := validateProjectCheckoutBranchTask(coordination, activeTaskID, "project_checkout_materialize"); err != nil {
				return &ToolResult{Output: err.Error(), IsError: true}
			}
		}
	}
	repo, ok := selectProjectCheckoutRegisterRepo(coordination, strings.TrimSpace(stringArg(args, "repo_id")))
	if !ok {
		return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize requires a READY repository with remote_url for project %s", projectID), IsError: true}
	}
	writeScopeJSON := strings.TrimSpace(stringArg(args, "write_scope_json"))
	if registerBranch {
		if claimWriteScopeJSON, _, ok := selectProjectActiveClaimWriteScope(coordination, t.agentID, activeTaskID); ok {
			writeScopeJSON = claimWriteScopeJSON
		} else if writeScopeJSON == "" {
			writeScopeJSON, _, _ = selectProjectClaimWriteScope(coordination, t.agentID)
		}
		if writeScopeJSON == "" || !json.Valid([]byte(writeScopeJSON)) {
			return &ToolResult{Output: "write_scope_json is required and must be valid JSON before materializing a branch checkout", IsError: true}
		}
		if err := validateProjectWriteScopeJSON(writeScopeJSON); err != nil {
			return &ToolResult{Output: fmt.Sprintf("write_scope_json is invalid before materializing a branch checkout: %v", err), IsError: true}
		}
	}

	baseBranchArg := strings.TrimSpace(stringArg(args, "base_branch"))
	baseBranch := firstNonEmpty(baseBranchArg, repo.DefaultBranch, coordination.Profile.RepoDefaultBranch, "main")
	branchName := strings.TrimSpace(stringArg(args, "branch_name"))
	requestedBranchName := branchName
	if registerBranch && branchName == "" && activeTaskID != "" {
		if activeBranch, ok := projectCheckoutMaterializeActiveTaskBranch(coordination, repo.RepoID, t.agentID, activeTaskID); ok {
			branchName = strings.TrimSpace(activeBranch.BranchName)
		}
	}
	if branchName == "" {
		if registerBranch {
			branchName = projectClaimBranchName(t.agentID, projectID, firstNonEmpty(branchTaskID, "manual"))
		} else {
			branchName = baseBranch
		}
	}
	revisionBaseBranch := ""
	requestedBranchRedirected := false
	revisionBaseHeadSHA := ""
	if registerBranch && strings.TrimSpace(requestedBranchName) != "" {
		if foreignBranch, ok := projectCheckoutMaterializeForeignOwnedBranch(coordination, repo.RepoID, requestedBranchName, t.agentID); ok {
			return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize refuses to register implementation branch %q because it is already owned by agent %s as branch_id=%s. For a revision of that candidate, use branch_name only in a read-only validation checkout, or set base_branch=%q and omit branch_name so this agent creates a new owned revision branch before project_branch_commit/project_branch_review_ready.", requestedBranchName, firstNonEmpty(strings.TrimSpace(foreignBranch.AgentID), "<unknown>"), strings.TrimSpace(foreignBranch.BranchID), requestedBranchName), IsError: true}
		}
		if priorBranch, ok := projectCheckoutMaterializePriorSameAgentCandidateBranch(coordination, repo.RepoID, requestedBranchName, t.agentID, activeTaskID); ok {
			revisionBaseBranch = requestedBranchName
			requestedBranchRedirected = true
			revisionBaseHeadSHA = strings.TrimSpace(priorBranch.HeadSHA)
			if baseBranchArg == "" {
				baseBranch = requestedBranchName
			}
			branchName = projectClaimBranchName(t.agentID, projectID, firstNonEmpty(branchTaskID, activeTaskID, "manual"))
		}
	}
	expectedHeadSHA := firstNonEmpty(strings.TrimSpace(stringArg(args, "expected_head_sha")), revisionBaseHeadSHA, projectCheckoutMaterializeExpectedHead(coordination, repo.RepoID, branchName))
	requestedLocalPath := strings.TrimSpace(stringArg(args, "local_path"))
	if requestedLocalPath != "" {
		canonicalRequestedLocalPath, err := resolveProjectCheckoutMaterializePath(t.workdir, requestedLocalPath)
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize invalid local_path: %v", err), IsError: true}
		}
		requestedLocalPath = canonicalRequestedLocalPath
	}
	if registerBranch {
		if output, ok := projectCheckoutMaterializeCommittedBranchOutput(ctx, projectCheckoutMaterializeCommittedBranchInput{
			Workdir:            t.workdir,
			ProjectID:          projectID,
			Coordination:       coordination,
			Repo:               repo,
			AgentID:            t.agentID,
			ActiveTaskID:       activeTaskID,
			BranchName:         branchName,
			ExpectedHeadSHA:    expectedHeadSHA,
			RequestedLocalPath: requestedLocalPath,
		}); ok {
			return &ToolResult{Output: output}
		}
	}
	localPath := requestedLocalPath
	if localPath == "" {
		if registerBranch {
			localPath = projectCheckoutMaterializeDefaultPath(t.workdir, projectID, repo)
		} else {
			localPath = projectCheckoutMaterializeValidationPath(t.workdir, projectID, repo, branchName, expectedHeadSHA)
		}
	}
	canonicalLocalPath, err := resolveProjectCheckoutMaterializePath(t.workdir, localPath)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize invalid local_path: %v", err), IsError: true}
	}
	localPath = canonicalLocalPath
	if err := ensureProjectCheckoutMaterializePath(t.workdir, localPath); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize invalid local_path: %v", err), IsError: true}
	}

	allowDirtyExisting := optionalBoolArgValue(args, "allow_dirty_existing", false)
	if !registerBranch {
		allowDirtyExisting = false
	}
	dirtyPathsBeforeMaterialize, dirtyPreflightBlock := t.projectCheckoutMaterializeDirtyPreflight(ctx, projectID, coordination, repo, projectCheckoutMaterializeSideEffectInput{
		Args:           args,
		LocalPath:      localPath,
		BranchName:     branchName,
		WriteScopeJSON: writeScopeJSON,
		ActiveTaskID:   activeTaskID,
		ActiveClaimID:  activeClaimID,
	})
	if dirtyPreflightBlock != nil {
		return dirtyPreflightBlock
	}
	preservedDirtyCheckout := ""
	preservedDirtyBranch := ""
	if registerBranch && requestedLocalPath == "" {
		redirectedPath := ""
		dirtyCheckout := ""
		dirtyBranch := ""
		redirected := false
		if requestedBranchRedirected {
			redirectedPath = projectCheckoutMaterializeBranchPath(t.workdir, projectID, repo, branchName, expectedHeadSHA)
			redirected = true
		} else {
			redirectedPath, dirtyCheckout, dirtyBranch, redirected = projectCheckoutMaterializeRedirectDirtyForeignDefault(ctx, t.workdir, projectID, repo, localPath, branchName, expectedHeadSHA)
		}
		if redirected && !sameProjectLocalPath(localPath, redirectedPath) {
			localPath = redirectedPath
			preservedDirtyCheckout = dirtyCheckout
			preservedDirtyBranch = dirtyBranch
			dirtyPathsBeforeMaterialize = nil
			canonicalLocalPath, err := resolveProjectCheckoutMaterializePath(t.workdir, localPath)
			if err != nil {
				return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize invalid redirected local_path: %v", err), IsError: true}
			}
			localPath = canonicalLocalPath
			if err := ensureProjectCheckoutMaterializePath(t.workdir, localPath); err != nil {
				return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize invalid redirected local_path: %v", err), IsError: true}
			}
			dirtyPathsBeforeMaterialize, dirtyPreflightBlock = t.projectCheckoutMaterializeDirtyPreflight(ctx, projectID, coordination, repo, projectCheckoutMaterializeSideEffectInput{
				Args:           args,
				LocalPath:      localPath,
				BranchName:     branchName,
				WriteScopeJSON: writeScopeJSON,
				ActiveTaskID:   activeTaskID,
				ActiveClaimID:  activeClaimID,
			})
			if dirtyPreflightBlock != nil {
				return dirtyPreflightBlock
			}
		}
	}
	clonePerformed, reusedExisting, err := materializeGitCheckout(ctx, localPath, repo, baseBranch, allowDirtyExisting)
	if err != nil {
		if dirtyErr, ok := projectCheckoutDirtyExisting(err); ok && registerBranch {
			if result := t.blockProjectCheckoutMaterializeForSideEffects(ctx, projectID, coordination, repo, projectCheckoutMaterializeSideEffectInput{
				Args:           args,
				LocalPath:      firstNonEmpty(dirtyErr.LocalPath, localPath),
				BranchName:     branchName,
				WriteScopeJSON: writeScopeJSON,
				ActiveTaskID:   activeTaskID,
				ActiveClaimID:  activeClaimID,
			}); result != nil {
				return result
			}
		}
		return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize git checkout failed: %v.%s", err, projectCheckoutMaterializeGitErrorGuidance()), IsError: true}
	}
	cleanupNewCheckout := clonePerformed && strings.TrimSpace(localPath) != ""
	registrationSucceeded := false
	defer func() {
		if cleanupNewCheckout && !registrationSucceeded {
			_ = os.RemoveAll(localPath)
		}
	}()
	remoteBranchUsed := false
	if strings.TrimSpace(branchName) != "" {
		remoteBranchUsed, err = checkoutProjectMaterializedBranch(ctx, localPath, branchName, !registerBranch, expectedHeadSHA)
		if err != nil {
			if !clonePerformed {
				writeProjectCheckoutInvalidMarker(localPath, err)
			}
			return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize branch checkout failed: %v.%s", err, projectCheckoutMaterializeGitErrorGuidance()), IsError: true}
		}
	}
	actualHeadSHA := ""
	if registerBranch && requestedBranchRedirected && strings.TrimSpace(expectedHeadSHA) != "" && projectCheckoutMaterializeRegisteredBranchHead(coordination, repo.RepoID, branchName) == "" {
		actualHeadSHA, err = gitRevParse(ctx, localPath, "HEAD")
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize failed reading checkout HEAD: %v", err), IsError: true}
		}
		if actualHeadSHA != strings.TrimSpace(expectedHeadSHA) && gitCommitObjectExists(ctx, localPath, expectedHeadSHA) {
			if dirty, dirtyErr := gitWorktreeDirty(ctx, localPath); dirtyErr != nil {
				return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize failed checking checkout cleanliness before aligning revision base: %v", dirtyErr), IsError: true}
			} else if dirty {
				return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize refuses to align revision branch %q to expected_head_sha %s because checkout is dirty", branchName, strings.TrimSpace(expectedHeadSHA)), IsError: true}
			}
			if out, checkoutErr := gitCombined(ctx, localPath, "checkout", "-B", branchName, strings.TrimSpace(expectedHeadSHA)); checkoutErr != nil {
				return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize failed aligning revision branch %q to expected_head_sha %s: %v\n%s", branchName, strings.TrimSpace(expectedHeadSHA), checkoutErr, out), IsError: true}
			}
			actualHeadSHA = strings.TrimSpace(expectedHeadSHA)
		}
	}
	if strings.TrimSpace(branchName) != "" || strings.TrimSpace(expectedHeadSHA) != "" {
		if actualHeadSHA == "" {
			actualHeadSHA, err = gitRevParse(ctx, localPath, "HEAD")
			if err != nil {
				return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize failed reading checkout HEAD: %v", err), IsError: true}
			}
		}
		if strings.TrimSpace(expectedHeadSHA) != "" && actualHeadSHA != strings.TrimSpace(expectedHeadSHA) {
			if !clonePerformed {
				writeProjectCheckoutInvalidMarker(localPath, fmt.Errorf("checkout HEAD %s does not match expected_head_sha %s for branch %q", actualHeadSHA, strings.TrimSpace(expectedHeadSHA), branchName))
			}
			return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize checkout HEAD %s does not match expected_head_sha %s for branch %q", actualHeadSHA, strings.TrimSpace(expectedHeadSHA), branchName), IsError: true}
		}
	}

	register := NewProjectCheckoutRegisterTool(t.client, t.workspaceID, t.agentID, t.ownerUserID, t.workdir).WithRuntimeBinding(t.runtimeBinding)
	checkoutKind := "clone"
	if !registerBranch {
		checkoutKind = "review"
	}
	dirtyState := "clean"
	if len(dirtyPathsBeforeMaterialize) > 0 {
		dirtyState = "dirty"
	}
	registerArgs := map[string]any{
		"project_id":        projectID,
		"repo_id":           repo.RepoID,
		"local_path":        localPath,
		"checkout_kind":     checkoutKind,
		"branch_name":       branchName,
		"base_branch":       baseBranch,
		"active_task_id":    activeTaskID,
		"active_claim_id":   activeClaimID,
		"write_scope_json":  writeScopeJSON,
		"register_branch":   registerBranch,
		"verify_git_remote": true,
		"dirty_state":       dirtyState,
		"checkout_status":   "ACTIVE",
	}
	registerResult := register.Execute(ctx, registerArgs)
	if registerResult == nil {
		return &ToolResult{Output: "project_checkout_materialize registered checkout returned nil result", IsError: true}
	}
	if registerResult.IsError {
		cleanupNote := ""
		if clonePerformed && strings.TrimSpace(localPath) != "" {
			if cleanupErr := os.RemoveAll(localPath); cleanupErr != nil {
				cleanupNote = fmt.Sprintf(" cleanup failed for newly cloned checkout %s: %v.", localPath, cleanupErr)
			} else {
				cleanupNote = fmt.Sprintf(" newly cloned checkout %s was removed so it cannot be mistaken for writable evidence.", localPath)
				cleanupNewCheckout = false
			}
		} else {
			writeProjectCheckoutInvalidMarker(localPath, fmt.Errorf("%s", registerResult.Output))
		}
		if projectToolResultIsEvidenceReconciliation(registerResult) {
			return registerResult
		}
		return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize materialized git checkout but evidence registration failed: %s.%s", registerResult.Output, cleanupNote), IsError: true}
	}
	registrationSucceeded = true

	payload := map[string]any{
		"project_id":                  projectID,
		"repo_id":                     repo.RepoID,
		"remote_url":                  repo.RemoteURL,
		"local_path":                  localPath,
		"base_branch":                 baseBranch,
		"branch_name":                 branchName,
		"requested_branch_name":       requestedBranchName,
		"requested_branch_redirected": requestedBranchRedirected,
		"revision_base_branch":        revisionBaseBranch,
		"checkout_kind":               checkoutKind,
		"validation_checkout":         !registerBranch,
		"write_allowed":               registerBranch,
		"head_sha":                    actualHeadSHA,
		"expected_head_sha":           expectedHeadSHA,
		"clone_performed":             clonePerformed,
		"inferred_task_id":            inferredActiveTaskID,
		"reused_existing":             reusedExisting,
		"dirty_state":                 dirtyState,
		"dirty_paths_preflight":       dirtyPathsBeforeMaterialize,
		"register_branch":             registerBranch,
		"remote_branch_used":          remoteBranchUsed,
		"git_commit_push":             false,
		"git_merge":                   false,
		"preserved_dirty_checkout":    preservedDirtyCheckout,
		"preserved_dirty_branch":      preservedDirtyBranch,
		"registration_result":         registerResult.Output,
	}
	if !registerBranch {
		payload["validation_guidance"] = "This is a review/validation checkout. Do not edit or commit it as product work; if validation finds a code/product defect, create or claim a revision implementation task and materialize an owned branch."
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_checkout_materialize completed for %s", localPath)}
	}
	return &ToolResult{Output: string(raw)}
}

func projectToolResultIsEvidenceReconciliation(result *ToolResult) bool {
	if result == nil {
		return false
	}
	output := strings.TrimSpace(result.Output)
	return strings.Contains(output, `"gate_type": "evidence_reconciliation"`) ||
		strings.Contains(output, `"gate_type":"evidence_reconciliation"`)
}

func projectCheckoutMaterializeActiveTaskBranch(coordination ProjectCoordinationRecord, repoID, agentID, activeTaskID string) (ProjectBranchRecord, bool) {
	repoID = strings.TrimSpace(repoID)
	agentID = strings.TrimSpace(agentID)
	activeTaskID = strings.TrimSpace(activeTaskID)
	if agentID == "" || activeTaskID == "" {
		return ProjectBranchRecord{}, false
	}
	var best ProjectBranchRecord
	bestScore := -1
	for _, branch := range coordination.Branches {
		if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			continue
		}
		if strings.TrimSpace(branch.AgentID) != agentID {
			continue
		}
		if strings.TrimSpace(branch.ActiveTaskID) != activeTaskID {
			continue
		}
		if strings.TrimSpace(branch.BranchName) == "" {
			continue
		}
		if projectCheckoutMaterializeTerminalBranchStatus(branch.Status) {
			continue
		}
		score := 0
		switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
		case "ACTIVE":
			score += 8
		case "RESERVED":
			score += 6
		case "READY_FOR_REVIEW":
			score += 4
		}
		if strings.TrimSpace(branch.CheckoutID) != "" {
			score += 2
		}
		if strings.TrimSpace(branch.HeadSHA) != "" {
			score++
		}
		if score > bestScore {
			best = branch
			bestScore = score
		}
	}
	if bestScore < 0 {
		return ProjectBranchRecord{}, false
	}
	return best, true
}

type projectCheckoutMaterializeSideEffectInput struct {
	Args           map[string]any
	LocalPath      string
	BranchName     string
	WriteScopeJSON string
	ActiveTaskID   string
	ActiveClaimID  string
}

func (t *ProjectCheckoutMaterializeTool) projectCheckoutMaterializeDirtyPreflight(ctx context.Context, projectID string, coordination ProjectCoordinationRecord, repo ProjectRepositoryRecord, input projectCheckoutMaterializeSideEffectInput) ([]string, *ToolResult) {
	localPath := strings.TrimSpace(input.LocalPath)
	if localPath == "" || !isGitCheckout(ctx, localPath) {
		return nil, nil
	}
	dirtyPaths, err := projectBranchCommitDirtyPaths(ctx, localPath)
	if err != nil {
		return nil, &ToolResult{Output: fmt.Sprintf("project_checkout_materialize failed reading dirty checkout paths before reuse: %v", err), IsError: true}
	}
	if len(dirtyPaths) == 0 {
		return nil, nil
	}
	if projectCheckoutDirtyOnlyInvalidMarker(ctx, localPath) {
		_ = os.Remove(filepath.Join(localPath, ".rhizome-invalid-checkout"))
		return nil, nil
	}
	if result := t.blockProjectCheckoutMaterializeForSideEffects(ctx, projectID, coordination, repo, input); result != nil {
		return dirtyPaths, result
	}
	return dirtyPaths, nil
}

func (t *ProjectCheckoutMaterializeTool) blockProjectCheckoutMaterializeForSideEffects(ctx context.Context, projectID string, coordination ProjectCoordinationRecord, repo ProjectRepositoryRecord, input projectCheckoutMaterializeSideEffectInput) *ToolResult {
	localPath := strings.TrimSpace(input.LocalPath)
	if localPath == "" {
		return nil
	}
	dirtyPaths, err := projectBranchCommitDirtyPaths(ctx, localPath)
	if err != nil || len(dirtyPaths) == 0 {
		return nil
	}
	branch := projectCheckoutMaterializeSideEffectBranch(ctx, coordination, repo.RepoID, t.agentID, input)
	boundaryScopeJSON := strings.TrimSpace(branch.WriteScopeJSON)
	if boundaryScopeJSON == "" {
		boundaryScopeJSON = strings.TrimSpace(input.WriteScopeJSON)
		branch.WriteScopeJSON = boundaryScopeJSON
	}
	pathset, err := pathsetFromWriteScopeJSON(boundaryScopeJSON)
	if err != nil {
		pathset = nil
	}
	sideEffectArgs := map[string]any{
		"active_task_id":  strings.TrimSpace(input.ActiveTaskID),
		"active_claim_id": strings.TrimSpace(input.ActiveClaimID),
	}
	for key, value := range input.Args {
		sideEffectArgs[key] = value
	}
	sideEffectBlocker := projectDirtySideEffectBlocker{
		client:      t.client,
		workspaceID: t.workspaceID,
		agentID:     t.agentID,
		ownerUserID: t.ownerUserID,
	}
	blockInput := projectDirtySideEffectBlockInput{
		SourceTool:          "project_checkout_materialize",
		ProjectID:           projectID,
		Repo:                repo,
		Branch:              branch,
		Args:                sideEffectArgs,
		DirtyPaths:          dirtyPaths,
		Pathset:             pathset,
		BlockedDetail:       "project_checkout_materialize found an existing dirty checkout outside the operational boundary before fetch or branch checkout",
		UpdateSummary:       fmt.Sprintf("project_checkout_materialize blocked pending side-effect classification for checkout %s", localPath),
		MandatoryNextAction: "classify side effects before retrying project_checkout_materialize or project_branch_commit",
	}
	sideEffects := sideEffectBlocker.detectDirtySideEffects(blockInput)
	if len(sideEffects) == 0 {
		return nil
	}
	return sideEffectBlocker.returnSideEffectBlock(ctx, blockInput, sideEffects)
}

func projectCheckoutMaterializeSideEffectBranch(ctx context.Context, coordination ProjectCoordinationRecord, repoID, agentID string, input projectCheckoutMaterializeSideEffectInput) ProjectBranchRecord {
	observedBranchName := ""
	if localPath := strings.TrimSpace(input.LocalPath); localPath != "" {
		if branchName, err := currentGitBranch(ctx, localPath); err == nil {
			observedBranchName = strings.TrimSpace(branchName)
		}
	}
	if observedBranchName != "" {
		if branch, err := selectProjectBranchCommitBranch(coordination.Branches, coordination.PatchQueueItems, agentID, "", observedBranchName, repoID); err == nil {
			return branch
		}
	}
	branchName := strings.TrimSpace(input.BranchName)
	if branchName != "" {
		if branch, err := selectProjectBranchCommitBranch(coordination.Branches, coordination.PatchQueueItems, agentID, "", branchName, repoID); err == nil {
			return branch
		}
	}
	if activeTaskID := strings.TrimSpace(input.ActiveTaskID); activeTaskID != "" {
		if branch, ok := projectCheckoutMaterializeActiveTaskBranch(coordination, repoID, agentID, activeTaskID); ok {
			return branch
		}
	}
	if currentBranch, err := currentGitBranch(ctx, input.LocalPath); err == nil && strings.TrimSpace(currentBranch) != "" {
		branchName = strings.TrimSpace(currentBranch)
	}
	return ProjectBranchRecord{
		RepoID:         strings.TrimSpace(repoID),
		AgentID:        strings.TrimSpace(agentID),
		BranchName:     firstNonEmpty(observedBranchName, branchName),
		WriteScopeJSON: strings.TrimSpace(input.WriteScopeJSON),
		ActiveTaskID:   strings.TrimSpace(input.ActiveTaskID),
		ActiveClaimID:  strings.TrimSpace(input.ActiveClaimID),
		Status:         "ACTIVE",
	}
}

type projectCheckoutMaterializeCommittedBranchInput struct {
	Workdir            string
	ProjectID          string
	Coordination       ProjectCoordinationRecord
	Repo               ProjectRepositoryRecord
	AgentID            string
	ActiveTaskID       string
	BranchName         string
	ExpectedHeadSHA    string
	RequestedLocalPath string
}

// projectCheckoutMaterializeGitErrorGuidance (OE-3, static-audit 2026-06-02) appends concrete
// remediation to an otherwise-opaque raw git error at the cold-start edge of every lane, so the
// agent has a self-correction path instead of a Run25-style dead-end. The original git error is
// preserved as the message prefix (kept as a substring for any downstream matching).
func projectCheckoutMaterializeGitErrorGuidance() string {
	return "\nNext: if the repo remote is unreachable, retry once. If the destination checkout is occupied or dirty, pass a fresh local_path (or classify the side effect first). If you are validating a peer's candidate branch, confirm the branch and expected_head_sha are published on origin (ask the owner to run project_branch_commit with push=true) before retrying. Do not retry blindly with the same inputs."
}

func projectCheckoutMaterializeCommittedBranchOutput(ctx context.Context, input projectCheckoutMaterializeCommittedBranchInput) (string, bool) {
	branch, checkout, ok := projectCheckoutMaterializeCommittedBranch(ctx, input)
	if !ok {
		return "", false
	}
	nextGate := "project_branch_review_ready"
	mandatoryNextTool := "project_branch_review_ready"
	guidance := "This owned implementation branch is already committed and materialized. Do not call project_checkout_materialize again for this task; publish the review packet with project_branch_review_ready, then submit the patch queue item."
	if strings.EqualFold(strings.TrimSpace(branch.Status), "READY_FOR_REVIEW") {
		nextGate = "project_patch_queue_submit"
		mandatoryNextTool = "project_patch_queue_submit"
		guidance = "This owned branch is already READY_FOR_REVIEW. Do not rematerialize or edit it; submit it to the patch queue."
	}
	payload := map[string]any{
		"project_id":              firstNonEmpty(strings.TrimSpace(input.ProjectID), strings.TrimSpace(input.Coordination.Project.ProjectID)),
		"repo_id":                 input.Repo.RepoID,
		"remote_url":              input.Repo.RemoteURL,
		"local_path":              checkout.LocalPath,
		"checkout_id":             checkout.CheckoutID,
		"branch_id":               branch.BranchID,
		"branch_name":             branch.BranchName,
		"base_branch":             firstNonEmpty(strings.TrimSpace(branch.BaseBranch), strings.TrimSpace(checkout.BaseBranch), strings.TrimSpace(input.Repo.DefaultBranch), strings.TrimSpace(input.Coordination.Profile.RepoDefaultBranch), "main"),
		"head_sha":                branch.HeadSHA,
		"expected_head_sha":       input.ExpectedHeadSHA,
		"already_materialized":    true,
		"committed_branch_reused": true,
		"clone_performed":         false,
		"reused_existing":         true,
		"register_branch":         true,
		"write_allowed":           true,
		"git_commit_push":         false,
		"git_merge":               false,
		"next_gate":               nextGate,
		"mandatory_next_tool":     mandatoryNextTool,
		"lifecycle_guidance":      guidance,
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Sprintf("project_checkout_materialize found existing committed branch %s; next_gate=%s", branch.BranchID, nextGate), true
	}
	return string(raw), true
}

func projectCheckoutMaterializeCommittedBranch(ctx context.Context, input projectCheckoutMaterializeCommittedBranchInput) (ProjectBranchRecord, ProjectCheckoutRecord, bool) {
	branchName := strings.TrimSpace(input.BranchName)
	agentID := strings.TrimSpace(input.AgentID)
	activeTaskID := strings.TrimSpace(input.ActiveTaskID)
	repoID := strings.TrimSpace(input.Repo.RepoID)
	expectedHeadSHA := strings.TrimSpace(input.ExpectedHeadSHA)
	if branchName == "" || agentID == "" {
		return ProjectBranchRecord{}, ProjectCheckoutRecord{}, false
	}
	for _, branch := range input.Coordination.Branches {
		if strings.TrimSpace(branch.BranchName) != branchName {
			continue
		}
		if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			continue
		}
		if strings.TrimSpace(branch.AgentID) != agentID {
			continue
		}
		if activeTaskID != "" && strings.TrimSpace(branch.ActiveTaskID) != activeTaskID {
			continue
		}
		if projectCheckoutMaterializeTerminalBranchStatus(branch.Status) {
			continue
		}
		headSHA := strings.TrimSpace(branch.HeadSHA)
		if headSHA == "" {
			continue
		}
		if expectedHeadSHA != "" && expectedHeadSHA != headSHA {
			continue
		}
		checkout, ok := projectCheckoutMaterializeCommittedCheckout(ctx, input.Workdir, input.Repo, input.Coordination.Checkouts, branch, agentID, activeTaskID, headSHA, input.RequestedLocalPath)
		if !ok {
			continue
		}
		return branch, checkout, true
	}
	return ProjectBranchRecord{}, ProjectCheckoutRecord{}, false
}

func projectCheckoutMaterializeTerminalBranchStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "MERGED", "ABANDONED", "ARCHIVED":
		return true
	default:
		return false
	}
}

func projectCheckoutMaterializeCommittedCheckout(ctx context.Context, workdir string, repo ProjectRepositoryRecord, checkouts []ProjectCheckoutRecord, branch ProjectBranchRecord, agentID, activeTaskID, headSHA, requestedPath string) (ProjectCheckoutRecord, bool) {
	for _, checkout := range checkouts {
		if strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(agentID) {
			continue
		}
		if !projectCheckoutWriteStatusActive(checkout.Status) {
			continue
		}
		if repoID := strings.TrimSpace(repo.RepoID); repoID != "" && strings.TrimSpace(checkout.RepoID) != repoID {
			continue
		}
		if checkoutID := strings.TrimSpace(branch.CheckoutID); checkoutID != "" && strings.TrimSpace(checkout.CheckoutID) != checkoutID {
			continue
		}
		if strings.TrimSpace(checkout.BranchName) != strings.TrimSpace(branch.BranchName) {
			continue
		}
		if activeTaskID != "" && strings.TrimSpace(checkout.ActiveTaskID) != strings.TrimSpace(activeTaskID) {
			continue
		}
		if kind := strings.ToLower(strings.TrimSpace(checkout.CheckoutKind)); kind == "review" || kind == "validation" {
			continue
		}
		localPath := strings.TrimSpace(checkout.LocalPath)
		if localPath == "" {
			continue
		}
		if requestedLocalPath := strings.TrimSpace(requestedPath); requestedLocalPath != "" && !sameProjectLocalPathWithinWorkdir(workdir, localPath, requestedLocalPath) {
			continue
		}
		if err := ensureProjectCheckoutMaterializePath(workdir, localPath); err != nil {
			continue
		}
		if err := validateProjectCheckoutWorkdir(ctx, localPath, repo); err != nil {
			continue
		}
		if err := ensureGitGeneratedArtifactExcludes(ctx, localPath); err != nil {
			continue
		}
		dirty, err := gitWorktreeDirty(ctx, localPath)
		if err != nil || dirty {
			continue
		}
		currentBranch, err := currentGitBranch(ctx, localPath)
		if err != nil || strings.TrimSpace(currentBranch) != strings.TrimSpace(branch.BranchName) {
			continue
		}
		actualHead, err := gitRevParse(ctx, localPath, "HEAD")
		if err != nil || strings.TrimSpace(actualHead) != strings.TrimSpace(headSHA) {
			continue
		}
		return checkout, true
	}
	return ProjectCheckoutRecord{}, false
}

func projectCheckoutMaterializeForeignOwnedBranch(coordination ProjectCoordinationRecord, repoID, branchName, agentID string) (ProjectBranchRecord, bool) {
	repoID = strings.TrimSpace(repoID)
	branchName = strings.TrimSpace(branchName)
	agentID = strings.TrimSpace(agentID)
	if branchName == "" {
		return ProjectBranchRecord{}, false
	}
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchName) != branchName {
			continue
		}
		if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			continue
		}
		owner := strings.TrimSpace(branch.AgentID)
		if owner == "" || owner == agentID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
		case "MERGED", "ABANDONED", "ARCHIVED":
			continue
		default:
			return branch, true
		}
	}
	return ProjectBranchRecord{}, false
}

func projectCheckoutMaterializePriorSameAgentCandidateBranch(coordination ProjectCoordinationRecord, repoID, branchName, agentID, activeTaskID string) (ProjectBranchRecord, bool) {
	repoID = strings.TrimSpace(repoID)
	branchName = strings.TrimSpace(branchName)
	agentID = strings.TrimSpace(agentID)
	activeTaskID = strings.TrimSpace(activeTaskID)
	if branchName == "" || agentID == "" || activeTaskID == "" {
		return ProjectBranchRecord{}, false
	}
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchName) != branchName {
			continue
		}
		if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			continue
		}
		if strings.TrimSpace(branch.AgentID) != agentID {
			continue
		}
		if projectCheckoutMaterializeTerminalBranchStatus(branch.Status) {
			continue
		}
		needsRevisionBranch := projectCheckoutMaterializeBranchHasRevisionPatchQueueItem(coordination.PatchQueueItems, branch)
		if strings.TrimSpace(branch.ActiveTaskID) == activeTaskID && !needsRevisionBranch {
			continue
		}
		if !needsRevisionBranch && !projectCheckoutMaterializePublishedCandidateBranch(coordination.PatchQueueItems, branch) {
			continue
		}
		return branch, true
	}
	return ProjectBranchRecord{}, false
}

func projectCheckoutMaterializePublishedCandidateBranch(items []ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
	case "READY_FOR_REVIEW":
		return true
	}
	if strings.TrimSpace(branch.ReviewDocKey) != "" {
		return true
	}
	branchID := strings.TrimSpace(branch.BranchID)
	if branchID == "" {
		return false
	}
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case "", "CANCELED":
			continue
		default:
			return true
		}
	}
	return false
}

func projectCheckoutMaterializeBranchHasRevisionPatchQueueItem(items []ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	branchID := strings.TrimSpace(branch.BranchID)
	if branchID == "" {
		return false
	}
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case "BLOCKED", "REJECTED":
			return true
		}
	}
	return false
}

func projectCheckoutMaterializeDefaultPath(workdir, projectID string, repo ProjectRepositoryRecord) string {
	projectHash := shortRefHash(firstNonEmpty(projectID, "project"))
	repoHash := shortRefHash(firstNonEmpty(repo.RepoID, repo.RemoteURL, repo.Name, "repo"))
	return filepath.Join(workdir, "project-checkouts", "p-"+projectHash+"-r-"+repoHash)
}

func projectCheckoutMaterializeValidationPath(workdir, projectID string, repo ProjectRepositoryRecord, branchName, expectedHeadSHA string) string {
	key := strings.Join([]string{
		strings.TrimSpace(projectID),
		strings.TrimSpace(repo.RepoID),
		strings.TrimSpace(repo.RemoteURL),
		strings.TrimSpace(branchName),
		strings.TrimSpace(expectedHeadSHA),
	}, "|")
	return filepath.Join(workdir, "project-checkouts", "review-"+shortRefHash(firstNonEmpty(key, "validation")))
}

func projectCheckoutMaterializeBranchPath(workdir, projectID string, repo ProjectRepositoryRecord, branchName, expectedHeadSHA string) string {
	base := projectCheckoutMaterializeDefaultPath(workdir, projectID, repo)
	key := strings.Join([]string{strings.TrimSpace(branchName), strings.TrimSpace(expectedHeadSHA)}, "|")
	return base + "-b-" + shortRefHash(firstNonEmpty(key, "branch"))
}

func projectCheckoutMaterializeRedirectDirtyForeignDefault(ctx context.Context, workdir, projectID string, repo ProjectRepositoryRecord, localPath, branchName, expectedHeadSHA string) (string, string, string, bool) {
	if strings.TrimSpace(localPath) == "" || !isGitCheckout(ctx, localPath) {
		return localPath, "", "", false
	}
	dirty, err := gitWorktreeDirty(ctx, localPath)
	if err != nil || !dirty {
		return localPath, "", "", false
	}
	currentBranch, err := currentGitBranch(ctx, localPath)
	if err != nil || strings.TrimSpace(currentBranch) == "" || strings.TrimSpace(currentBranch) == strings.TrimSpace(branchName) {
		return localPath, "", "", false
	}
	return projectCheckoutMaterializeBranchPath(workdir, projectID, repo, branchName, expectedHeadSHA), localPath, strings.TrimSpace(currentBranch), true
}

func inferProjectCheckoutMaterializeTaskID(coordination ProjectCoordinationRecord) string {
	var implementation []string
	var nonCoordination []string
	var open []string
	for _, task := range coordination.Tasks {
		taskID := strings.TrimSpace(task.TaskID)
		if taskID == "" || projectCheckoutMaterializeTerminalTaskStatus(task.Status) {
			continue
		}
		open = append(open, taskID)
		lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
		kind := strings.ToUpper(strings.TrimSpace(task.TaskKind))
		if lane == "implementation" || lane == "implement" || kind == "EXECUTION" {
			implementation = append(implementation, taskID)
			continue
		}
		if lane != "strategy" && kind != "COORDINATION" {
			nonCoordination = append(nonCoordination, taskID)
		}
	}
	if taskID := singleProjectCheckoutMaterializeTaskID(implementation); taskID != "" {
		return taskID
	}
	if taskID := singleProjectCheckoutMaterializeTaskID(nonCoordination); taskID != "" {
		return taskID
	}
	return singleProjectCheckoutMaterializeTaskID(open)
}

func projectCheckoutMaterializeTerminalTaskStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "DONE", "COMPLETED", "CLOSED", "CANCELED", "CANCELLED", "FAILED", "ARCHIVED":
		return true
	default:
		return false
	}
}

func projectCheckoutTaskDefaultsToValidationCheckout(task WorkspaceTaskRecord) bool {
	if projectCheckoutTaskAllowsBranchEvidence(task) {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	switch lane {
	case "qa", "review", "validation", "validate", "test", "testing", "verification", "verifier", "accessibility", "usability":
		return true
	case "strategy", "planning", "plan", "spec", "design", "coordination":
		return false
	}
	kind := strings.ToUpper(strings.TrimSpace(task.TaskKind))
	if kind != "EXECUTION" {
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
	}, " "))
	return containsAnySignal(text, []string{"qa", "review", "validation", "validate", "browser smoke", "smoke", "accessibility", "usability", "test"})
}

func singleProjectCheckoutMaterializeTaskID(taskIDs []string) string {
	seen := map[string]struct{}{}
	var single string
	for _, taskID := range taskIDs {
		taskID = strings.TrimSpace(taskID)
		if taskID == "" {
			continue
		}
		if _, ok := seen[taskID]; ok {
			continue
		}
		seen[taskID] = struct{}{}
		if single != "" {
			return ""
		}
		single = taskID
	}
	return single
}

func projectCheckoutMaterializeExpectedHead(coordination ProjectCoordinationRecord, repoID, branchName string) string {
	repoID = strings.TrimSpace(repoID)
	branchName = strings.TrimSpace(branchName)
	if branchName == "" {
		return ""
	}
	branchID := ""
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchName) != branchName {
			continue
		}
		if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			continue
		}
		branchID = strings.TrimSpace(branch.BranchID)
		if head := strings.TrimSpace(branch.HeadSHA); head != "" {
			return head
		}
	}
	for _, item := range coordination.PatchQueueItems {
		if branchID == "" || strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		if repoID != "" && strings.TrimSpace(item.RepoID) != repoID {
			continue
		}
		if head := strings.TrimSpace(item.HeadSHA); head != "" {
			return head
		}
	}
	return ""
}

func projectCheckoutMaterializeRegisteredBranchHead(coordination ProjectCoordinationRecord, repoID, branchName string) string {
	repoID = strings.TrimSpace(repoID)
	branchName = strings.TrimSpace(branchName)
	if branchName == "" {
		return ""
	}
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchName) != branchName {
			continue
		}
		if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			continue
		}
		if head := strings.TrimSpace(branch.HeadSHA); head != "" {
			return head
		}
	}
	return ""
}

func ensureProjectCheckoutMaterializePath(workdir, localPath string) error {
	_, err := resolveProjectCheckoutMaterializePath(workdir, localPath)
	return err
}

func sameProjectLocalPathWithinWorkdir(workdir, a, b string) bool {
	left, ok := normalizeProjectCheckoutPathWithinWorkdir(workdir, a)
	if !ok {
		return false
	}
	right, ok := normalizeProjectCheckoutPathWithinWorkdir(workdir, b)
	if !ok {
		return false
	}
	return strings.EqualFold(left, right)
}

func normalizeProjectCheckoutPathWithinWorkdir(workdir, path string) (string, bool) {
	base, err := filepath.Abs(strings.TrimSpace(workdir))
	if err != nil || strings.TrimSpace(path) == "" {
		return "", false
	}
	target := strings.TrimSpace(path)
	if !filepath.IsAbs(target) {
		target = filepath.Join(base, target)
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return "", false
	}
	return filepath.Clean(target), true
}

func resolveProjectCheckoutMaterializePath(workdir, localPath string) (string, error) {
	base, err := filepath.Abs(strings.TrimSpace(workdir))
	if err != nil {
		return "", err
	}
	rawTarget := strings.TrimSpace(localPath)
	if rawTarget == "" {
		return "", fmt.Errorf("local_path is required")
	}
	if !filepath.IsAbs(rawTarget) {
		rawTarget = filepath.Join(base, rawTarget)
	}
	target, err := filepath.Abs(rawTarget)
	if err != nil {
		return "", err
	}
	target = filepath.Clean(target)
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return "", err
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", fmt.Errorf("%q is outside agent workdir %q", localPath, workdir)
	}
	return target, nil
}

func materializeGitCheckout(ctx context.Context, localPath string, repo ProjectRepositoryRecord, baseBranch string, allowDirtyExisting bool) (bool, bool, error) {
	if isGitCheckout(ctx, localPath) {
		if err := validateProjectCheckoutWorkdir(ctx, localPath, repo); err != nil {
			return false, true, err
		}
		if err := ensureGitGeneratedArtifactExcludes(ctx, localPath); err != nil {
			return false, true, err
		}
		if !allowDirtyExisting {
			if dirty, err := gitWorktreeDirty(ctx, localPath); err != nil {
				return false, true, err
			} else if dirty {
				if projectCheckoutDirtyOnlyInvalidMarker(ctx, localPath) {
					_ = os.Remove(filepath.Join(localPath, ".rhizome-invalid-checkout"))
				} else {
					return false, true, projectCheckoutDirtyExistingError{LocalPath: localPath}
				}
			}
		}
		if out, err := gitCombined(ctx, localPath, "fetch", "origin"); err != nil {
			return false, true, fmt.Errorf("git fetch failed: %w\n%s", err, out)
		}
		return false, true, nil
	}
	if err := ensureCloneDestinationAvailable(localPath); err != nil {
		return false, false, err
	}
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return false, false, err
	}
	out, err := gitCommand(ctx, "clone", strings.TrimSpace(repo.RemoteURL), localPath).CombinedOutput()
	if err != nil {
		return false, false, fmt.Errorf("git clone failed: %w\n%s", err, string(out))
	}
	if strings.TrimSpace(baseBranch) != "" {
		if out, err := gitCombined(ctx, localPath, "checkout", baseBranch); err != nil {
			return true, false, fmt.Errorf("git checkout base branch failed: %w\n%s", err, out)
		}
	}
	return true, false, nil
}

type projectCheckoutDirtyExistingError struct {
	LocalPath string
}

func (e projectCheckoutDirtyExistingError) Error() string {
	return fmt.Sprintf("existing checkout %q is dirty; commit, clean, or pass allow_dirty_existing=true", e.LocalPath)
}

func projectCheckoutDirtyExisting(err error) (projectCheckoutDirtyExistingError, bool) {
	var dirtyErr projectCheckoutDirtyExistingError
	if errors.As(err, &dirtyErr) {
		return dirtyErr, true
	}
	return projectCheckoutDirtyExistingError{}, false
}

func checkoutProjectMaterializedBranch(ctx context.Context, localPath, branchName string, preferRemote bool, expectedHeadSHA string) (bool, error) {
	branchName = strings.TrimSpace(branchName)
	if branchName == "" {
		return false, nil
	}
	expectedHeadSHA = strings.TrimSpace(expectedHeadSHA)
	remoteRef := "refs/remotes/origin/" + branchName
	localRef := "refs/heads/" + branchName
	remoteSHA, remoteExists := gitRefSHA(ctx, localPath, remoteRef)
	if preferRemote && !remoteExists {
		return false, fmt.Errorf("peer validation requires branch %q to exist on origin; ask the branch owner to push/publish the candidate before validating", branchName)
	}
	localExists := gitRefExists(ctx, localPath, localRef)
	if preferRemote && expectedHeadSHA != "" && remoteExists && strings.TrimSpace(remoteSHA) != expectedHeadSHA && gitCommitObjectExists(ctx, localPath, expectedHeadSHA) {
		if out, err := gitCombined(ctx, localPath, "checkout", "--detach", expectedHeadSHA); err != nil {
			return false, fmt.Errorf("git checkout expected candidate SHA %s for branch %s failed after origin tip drifted to %s: %w\n%s", expectedHeadSHA, branchName, remoteSHA, err, out)
		}
		return true, nil
	}
	if remoteExists && (preferRemote || !localExists) {
		if out, err := gitCombined(ctx, localPath, "checkout", "-B", branchName, remoteRef); err != nil {
			if preferRemote {
				candidateSHA := firstNonEmpty(expectedHeadSHA, remoteSHA)
				if candidateSHA != "" && gitCommitObjectExists(ctx, localPath, candidateSHA) {
					if fallbackOut, fallbackErr := gitCombined(ctx, localPath, "checkout", "--detach", candidateSHA); fallbackErr == nil {
						return true, nil
					} else {
						return false, fmt.Errorf("git checkout remote branch %s failed and detached SHA fallback %s failed: %w\nprimary output:\n%s\nfallback output:\n%s", remoteRef, candidateSHA, fallbackErr, out, fallbackOut)
					}
				}
			}
			return false, fmt.Errorf("git checkout remote branch %s failed: %w\n%s", remoteRef, err, out)
		}
		return true, nil
	}
	if localExists {
		if out, err := gitCombined(ctx, localPath, "checkout", branchName); err != nil {
			return false, fmt.Errorf("git checkout local branch %s failed: %w\n%s", branchName, err, out)
		}
		return false, nil
	}
	if out, err := gitCombined(ctx, localPath, "checkout", "-B", branchName); err != nil {
		return false, fmt.Errorf("git checkout new branch %s failed: %w\n%s", branchName, err, out)
	}
	return false, nil
}

func gitRefExists(ctx context.Context, localPath, ref string) bool {
	if strings.TrimSpace(ref) == "" {
		return false
	}
	_, ok := gitRefSHA(ctx, localPath, ref)
	return ok
}

func gitRefSHA(ctx context.Context, localPath, ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", false
	}
	out, err := gitCombined(ctx, localPath, "show-ref")
	if err != nil && strings.TrimSpace(out) == "" {
		return "", false
	}
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 {
			continue
		}
		if fields[1] == ref && strings.TrimSpace(fields[0]) != "" {
			return strings.TrimSpace(fields[0]), true
		}
	}
	return "", false
}

func gitCommitObjectExists(ctx context.Context, localPath, sha string) bool {
	sha = strings.TrimSpace(sha)
	if sha == "" {
		return false
	}
	_, err := gitCombined(ctx, localPath, "cat-file", "-e", sha+"^{commit}")
	return err == nil
}

func ensureCloneDestinationAvailable(localPath string) error {
	info, err := os.Stat(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("destination %q exists and is not a directory", localPath)
	}
	entries, err := os.ReadDir(localPath)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("destination %q exists but is not a git checkout and is not empty", localPath)
	}
	return nil
}

func writeProjectCheckoutInvalidMarker(localPath string, cause error) {
	localPath = strings.TrimSpace(localPath)
	if localPath == "" {
		return
	}
	if !isGitCheckout(context.Background(), localPath) {
		return
	}
	message := "invalid checkout evidence; materialize a fresh registered checkout before editing product files"
	if cause != nil {
		message += "\n" + strings.TrimSpace(cause.Error())
	}
	_ = os.WriteFile(filepath.Join(localPath, ".rhizome-invalid-checkout"), []byte(message+"\n"), 0o644)
}

func isGitCheckout(ctx context.Context, localPath string) bool {
	out, err := exec.CommandContext(ctx, "git", "-C", strings.TrimSpace(localPath), "rev-parse", "--show-toplevel").Output()
	return err == nil && strings.TrimSpace(string(out)) != ""
}

func gitWorktreeDirty(ctx context.Context, localPath string) (bool, error) {
	out, err := gitStatusPorcelain(ctx, localPath)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) != "", nil
}

func projectCheckoutDirtyOnlyInvalidMarker(ctx context.Context, localPath string) bool {
	out, err := gitStatusPorcelain(ctx, localPath)
	if err != nil {
		return false
	}
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return false
	}
	for _, line := range strings.Split(trimmed, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasSuffix(line, ".rhizome-invalid-checkout") {
			return false
		}
	}
	return true
}

func gitCombined(ctx context.Context, localPath string, args ...string) (string, error) {
	cmdArgs := append([]string{"-C", strings.TrimSpace(localPath)}, args...)
	out, err := gitCommand(ctx, cmdArgs...).CombinedOutput()
	return string(out), err
}

func gitCommand(ctx context.Context, args ...string) *exec.Cmd {
	cmdArgs := append([]string{
		"-c", "commit.gpgsign=false",
		"-c", "tag.gpgsign=false",
		"-c", "credential.interactive=false",
	}, args...)
	cmd := exec.CommandContext(ctx, "git", cmdArgs...)
	cmd.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GCM_INTERACTIVE=Never",
	)
	return cmd
}

func optionalBoolArgValue(args map[string]any, key string, fallback bool) bool {
	if explicit := optionalBoolArg(args, key); explicit != nil {
		return *explicit
	}
	return fallback
}
