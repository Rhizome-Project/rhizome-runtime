package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type ProjectBranchCommitTool struct {
	client           *RhizomeClient
	workspaceID      string
	agentID          string
	ownerUserID      string
	workdir          string
	coordinationMode string
	runtimeBinding   func() AgentRuntimeBinding
}

func NewProjectBranchCommitTool(client *RhizomeClient, workspaceID, agentID, ownerUserID, workdir string) *ProjectBranchCommitTool {
	return &ProjectBranchCommitTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		ownerUserID: strings.TrimSpace(ownerUserID),
		workdir:     strings.TrimSpace(workdir),
	}
}

func (t *ProjectBranchCommitTool) WithCoordinationMode(mode string) *ProjectBranchCommitTool {
	if t != nil {
		t.coordinationMode = normalizeCoordinationMode(mode)
	}
	return t
}

func (t *ProjectBranchCommitTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectBranchCommitTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectBranchCommitTool) Name() string { return "project_branch_commit" }

func (t *ProjectBranchCommitTool) Description() string {
	return "Create a bounded git commit for this agent's owned project branch inside its registered checkout, update branch/checkout evidence, and optionally push the branch. Out-of-boundary dirty paths are recorded as side-effect evidence before any git add/commit and require explicit classification."
}

func (t *ProjectBranchCommitTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{"type": "string", "description": "Project id."},
			"branch_id":  map[string]any{"type": "string", "description": "Existing owned project branch id."},
			"branch_name": map[string]any{
				"type":        "string",
				"description": "Existing owned branch name when branch_id is unknown.",
			},
			"repo_id": map[string]any{"type": "string", "description": "Optional repository id guard."},
			"local_path": map[string]any{
				"type":        "string",
				"description": "Optional checkout path inside this agent workdir. Defaults to the registered branch checkout path.",
			},
			"commit_message": map[string]any{
				"type":        "string",
				"description": "Git commit subject/body. Defaults to a short project branch message.",
			},
			"candidate_paths": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional exact dirty paths to commit. If supplied, all dirty paths must be listed and inside the branch write scope.",
			},
			"base_branch": map[string]any{
				"type":        "string",
				"description": "Optional base branch override. Defaults to branch/repo/project default branch.",
			},
			"active_task_id":  map[string]any{"type": "string", "description": "Optional active task id for evidence rebinding."},
			"active_claim_id": map[string]any{"type": "string", "description": "Optional active claim id for evidence rebinding."},
			"push": map[string]any{
				"type":        "boolean",
				"description": "When true, push the committed branch to the configured remote after local evidence is registered. Defaults false.",
			},
			"remote_name": map[string]any{
				"type":        "string",
				"description": "Remote name to push to when push=true. Defaults origin.",
			},
		},
		"required": []string{"project_id"},
	}
}

func (t *ProjectBranchCommitTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_branch_commit is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	if strings.TrimSpace(t.workdir) == "" {
		return &ToolResult{Output: "project_branch_commit requires an agent workdir", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_branch_commit", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	branchID := projectToolRefArg(args, "branch_id")
	branchName := projectToolRefArg(args, "branch_name")
	if branchID == "" && branchName == "" {
		return &ToolResult{Output: "branch_id or branch_name is required", IsError: true}
	}

	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	branch, err := selectProjectBranchCommitBranch(coordination.Branches, coordination.PatchQueueItems, t.agentID, branchID, branchName, projectToolRefArg(args, "repo_id"))
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	repo, ok := selectProjectBranchReviewRepo(coordination.Repositories, branch.RepoID)
	if !ok {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit requires branch repo %s to be READY with remote_url", branch.RepoID), IsError: true}
	}
	pathset, err := pathsetFromWriteScopeJSON(branch.WriteScopeJSON)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("branch write_scope_json pathset is invalid: %v", err), IsError: true}
	}
	if len(pathset) == 0 {
		return &ToolResult{Output: "branch write_scope_json must include non-empty paths before commit", IsError: true}
	}
	effectivePathset, derivedScopePaths := projectBranchCommitEffectiveWriteScope(pathset, nil)
	branchWriteScopeJSON := strings.TrimSpace(branch.WriteScopeJSON)
	localPath := firstNonEmpty(strings.TrimSpace(stringArg(args, "local_path")), projectBranchReviewCheckoutPath(coordination.Checkouts, branch.CheckoutID))
	if localPath == "" {
		return &ToolResult{Output: "local_path is required because the branch has no registered checkout path", IsError: true}
	}
	canonicalLocalPath, err := resolveProjectCheckoutMaterializePath(t.workdir, localPath)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit invalid local_path: %v", err), IsError: true}
	}
	localPath = canonicalLocalPath
	if err := ensureProjectCheckoutMaterializePath(t.workdir, localPath); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit invalid local_path: %v", err), IsError: true}
	}
	if err := validateProjectCheckoutWorkdir(ctx, localPath, repo); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit local verification failed: %v", err), IsError: true}
	}
	currentBranch, err := currentGitBranch(ctx, localPath)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit cannot read current git branch for %s: %v", localPath, err), IsError: true}
	}
	baseBranch := firstNonEmpty(strings.TrimSpace(stringArg(args, "base_branch")), branch.BaseBranch, repo.DefaultBranch, coordination.Profile.RepoDefaultBranch, "main")
	baseSHA := projectBranchCommitCanonicalBaseSHA(ctx, localPath, baseBranch, branch.BaseSHA)
	if currentBranch != strings.TrimSpace(branch.BranchName) {
		headSHA, headErr := gitRevParse(ctx, localPath, "HEAD")
		cause := fmt.Errorf("checkout branch %s does not match registered branch %s", currentBranch, branch.BranchName)
		if headErr != nil {
			cause = fmt.Errorf("%w; failed reading checkout HEAD: %v", cause, headErr)
		}
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     branch.CheckoutID,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        strings.TrimSpace(headSHA),
			WriteScopeJSON: branchWriteScopeJSON,
			Stage:          "pre_commit_branch_mismatch",
			CommitCreated:  false,
			Cause:          cause,
		})
	}

	if err := ensureGitGeneratedArtifactExcludes(ctx, localPath); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit failed preparing generated-artifact excludes: %v", err), IsError: true}
	}
	dirtyPaths, err := projectBranchCommitDirtyPaths(ctx, localPath)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit failed reading dirty paths: %v", err), IsError: true}
	}
	if len(dirtyPaths) == 0 {
		if boolArg(args, "push", false) {
			return t.pushCleanProjectBranchCommit(ctx, args, projectID, coordination, repo, branch, localPath, currentBranch, branchWriteScopeJSON)
		}
		return &ToolResult{Output: "project_branch_commit found no git changes to commit. If this branch already has a local commit that peers need to inspect, rerun project_branch_commit with push=true to publish the existing HEAD.", IsError: true}
	}
	effectivePathset, derivedScopePaths = projectBranchCommitEffectiveWriteScope(pathset, dirtyPaths)
	if len(derivedScopePaths) > 0 {
		branchWriteScopeJSON = projectBranchCommitWriteScopeJSON(effectivePathset)
	}
	sideEffectBlocker := projectDirtySideEffectBlocker{
		client:      t.client,
		workspaceID: t.workspaceID,
		agentID:     t.agentID,
		ownerUserID: t.ownerUserID,
	}
	if sideEffects := sideEffectBlocker.detectDirtySideEffects(projectDirtySideEffectBlockInput{
		SourceTool: "project_branch_commit",
		ProjectID:  projectID,
		Repo:       repo,
		Branch:     branch,
		Args:       args,
		DirtyPaths: dirtyPaths,
		Pathset:    effectivePathset,
	}); len(sideEffects) > 0 {
		return sideEffectBlocker.returnSideEffectBlock(ctx, projectDirtySideEffectBlockInput{
			SourceTool:          "project_branch_commit",
			ProjectID:           projectID,
			Repo:                repo,
			Branch:              branch,
			Args:                args,
			DirtyPaths:          dirtyPaths,
			Pathset:             effectivePathset,
			BlockedDetail:       "project_branch_commit found dirty paths outside the operational boundary before git add",
			UpdateSummary:       fmt.Sprintf("project_branch_commit blocked pending side-effect classification for branch %s", firstNonEmpty(strings.TrimSpace(branch.BranchID), strings.TrimSpace(branch.BranchName), "<unknown>")),
			MandatoryNextAction: "classify side effects before retrying project_branch_commit",
		}, sideEffects)
	}
	commitPaths, err := projectBranchCommitPaths(dirtyPaths, stringSliceArg(args, "candidate_paths"), effectivePathset)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}

	commitMessage := projectBranchCommitMessage(args, projectID, branch)
	activeTaskID, activeClaimID := projectBranchCommitActiveTaskClaimIDs(args, branch)
	machineID := runtimeMachineID()
	checkoutIDForEvidence := strings.TrimSpace(branch.CheckoutID)
	checkout, checkoutFound := findProjectBranchCommitCheckoutByID(coordination.Checkouts, checkoutIDForEvidence)
	if !checkoutFound {
		checkout = ProjectCheckoutRecord{CheckoutID: checkoutIDForEvidence}
	}
	if pathCheckout, ok := findProjectBranchCommitCheckoutByPath(coordination.Checkouts, repo.RepoID, t.agentID, machineID, localPath, t.workdir); ok {
		if checkoutIDForEvidence == "" || checkoutIDForEvidence != strings.TrimSpace(pathCheckout.CheckoutID) {
			if !projectBranchCommitCanAdoptPathCheckout(pathCheckout, activeTaskID, activeClaimID) {
				return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
					ProjectID:      projectID,
					RepoID:         repo.RepoID,
					Branch:         branch,
					CheckoutID:     checkoutIDForEvidence,
					LocalPath:      localPath,
					CurrentBranch:  currentBranch,
					BaseBranch:     baseBranch,
					BaseSHA:        baseSHA,
					HeadSHA:        projectBranchCommitBestEffortHeadSHA(ctx, localPath),
					WriteScopeJSON: branchWriteScopeJSON,
					Stage:          "pre_commit_checkout_ownership",
					CommitCreated:  false,
					Cause:          fmt.Errorf("active checkout path %s belongs to checkout=%s active_task_id=%s active_claim_id=%s; resolve evidence ownership before mutating local git state", localPath, pathCheckout.CheckoutID, pathCheckout.ActiveTaskID, pathCheckout.ActiveClaimID),
				})
			}
			checkout = pathCheckout
			checkoutFound = true
			checkoutIDForEvidence = strings.TrimSpace(pathCheckout.CheckoutID)
		}
	}
	if err := projectBranchCommitValidateCheckoutForEvidence(checkout, checkoutFound, machineID, localPath, t.workdir, activeTaskID, activeClaimID); err != nil {
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     checkoutIDForEvidence,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        projectBranchCommitBestEffortHeadSHA(ctx, localPath),
			WriteScopeJSON: branchWriteScopeJSON,
			Stage:          "pre_commit_checkout_ownership",
			CommitCreated:  false,
			Cause:          err,
		})
	}
	if out, err := projectBranchGitAdd(ctx, localPath, commitPaths); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit git add failed: %v\n%s", err, out), IsError: true}
	}
	stagedPaths, err := projectBranchCommitStagedPaths(ctx, localPath)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit failed reading staged paths: %v", err), IsError: true}
	}
	if len(stagedPaths) == 0 {
		return &ToolResult{Output: "project_branch_commit found no staged changes after git add", IsError: true}
	}
	if !pathsetIsCoveredByScope(stagedPaths, effectivePathset) {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit staged paths exceed branch write scope: %s", strings.Join(stagedPaths, ", ")), IsError: true}
	}
	if out, err := projectBranchGitCommit(ctx, localPath, t.agentID, commitMessage); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit git commit failed: %v\n%s", err, out), IsError: true}
	}
	headSHA, err := gitRevParse(ctx, localPath, "HEAD")
	if err != nil || strings.TrimSpace(headSHA) == "" {
		headErr := err
		if headErr == nil {
			headErr = fmt.Errorf("empty HEAD")
		}
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     checkoutIDForEvidence,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        strings.TrimSpace(headSHA),
			CommittedPaths: stagedPaths,
			WriteScopeJSON: branchWriteScopeJSON,
			Stage:          "read_head",
			CommitCreated:  true,
			Cause:          fmt.Errorf("committed but could not read HEAD: %w", headErr),
		})
	}
	if strings.TrimSpace(baseSHA) != "" && headSHA == strings.TrimSpace(baseSHA) {
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     checkoutIDForEvidence,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        headSHA,
			CommittedPaths: stagedPaths,
			WriteScopeJSON: branchWriteScopeJSON,
			Stage:          "post_commit_head_validation",
			CommitCreated:  true,
			Cause:          fmt.Errorf("committed head %s equals base_sha %s", headSHA, strings.TrimSpace(baseSHA)),
		})
	}
	if strings.TrimSpace(baseSHA) != "" && !gitCommitIsAncestor(ctx, localPath, strings.TrimSpace(baseSHA), headSHA) {
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     checkoutIDForEvidence,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        headSHA,
			CommittedPaths: stagedPaths,
			WriteScopeJSON: branchWriteScopeJSON,
			Stage:          "post_commit_base_lineage_validation",
			CommitCreated:  true,
			Cause:          fmt.Errorf("committed head %s is not descended from current base_sha %s", headSHA, strings.TrimSpace(baseSHA)),
		})
	}
	if dirty, err := gitWorktreeDirty(ctx, localPath); err != nil {
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     checkoutIDForEvidence,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        headSHA,
			CommittedPaths: stagedPaths,
			WriteScopeJSON: branchWriteScopeJSON,
			Stage:          "post_commit_cleanliness_check",
			CommitCreated:  true,
			Cause:          fmt.Errorf("failed checking post-commit cleanliness: %w", err),
		})
	} else if dirty {
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     checkoutIDForEvidence,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        headSHA,
			CommittedPaths: stagedPaths,
			WriteScopeJSON: branchWriteScopeJSON,
			Stage:          "post_commit_dirty_state",
			CommitCreated:  true,
			Cause:          fmt.Errorf("checkout left dirty after commit"),
		})
	}

	registeredCheckout, err := t.client.RegisterProjectCheckout(ctx, ProjectCheckoutRegisterInput{
		WorkspaceID:   t.workspaceID,
		ProjectID:     projectID,
		ActorID:       t.agentID,
		CheckoutID:    checkoutIDForEvidence,
		RepoID:        repo.RepoID,
		MachineID:     firstNonEmpty(checkout.MachineID, machineID),
		MachineLabel:  firstNonEmpty(checkout.MachineLabel, machineID),
		OwnerUserID:   firstNonEmpty(checkout.OwnerUserID, t.ownerUserID),
		AgentID:       t.agentID,
		LocalPath:     localPath,
		CheckoutKind:  firstNonEmpty(checkout.CheckoutKind, "clone"),
		BranchName:    currentBranch,
		BaseBranch:    baseBranch,
		HeadSHA:       headSHA,
		BaseSHA:       baseSHA,
		DirtyState:    "clean",
		ActiveTaskID:  activeTaskID,
		ActiveClaimID: activeClaimID,
		Status:        "ACTIVE",
	})
	if err != nil {
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     checkoutIDForEvidence,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        headSHA,
			CommittedPaths: stagedPaths,
			WriteScopeJSON: branchWriteScopeJSON,
			Stage:          "checkout_register",
			CommitCreated:  true,
			Cause:          err,
		})
	}
	updatedBranch, err := t.client.RegisterProjectBranch(ctx, ProjectBranchRegisterInput{
		WorkspaceID:    t.workspaceID,
		ProjectID:      projectID,
		ActorID:        t.agentID,
		BranchID:       branch.BranchID,
		RepoID:         repo.RepoID,
		CheckoutID:     registeredCheckout.CheckoutID,
		AgentID:        t.agentID,
		ActiveTaskID:   activeTaskID,
		ActiveClaimID:  activeClaimID,
		BranchName:     branch.BranchName,
		BranchKind:     firstNonEmpty(branch.BranchKind, "feature"),
		BaseBranch:     baseBranch,
		HeadSHA:        headSHA,
		BaseSHA:        baseSHA,
		WriteScopeJSON: branchWriteScopeJSON,
		Status:         "ACTIVE",
	})
	if err != nil {
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     registeredCheckout.CheckoutID,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        headSHA,
			CommittedPaths: stagedPaths,
			WriteScopeJSON: branchWriteScopeJSON,
			Stage:          "branch_register",
			CommitCreated:  true,
			Cause:          err,
		})
	}

	pushAttempted := boolArg(args, "push", false)
	pushSucceeded := false
	pushOutput := ""
	if pushAttempted {
		remoteName := firstNonEmpty(strings.TrimSpace(stringArg(args, "remote_name")), "origin")
		out, err := gitCombined(ctx, localPath, "push", "-u", remoteName, branch.BranchName)
		pushOutput = strings.TrimSpace(out)
		if err != nil {
			payload := projectBranchCommitOutput(projectBranchCommitResult{
				ProjectID:      projectID,
				RepoID:         repo.RepoID,
				CheckoutID:     registeredCheckout.CheckoutID,
				BranchID:       updatedBranch.BranchID,
				BranchName:     updatedBranch.BranchName,
				BaseBranch:     baseBranch,
				BaseSHA:        baseSHA,
				HeadSHA:        headSHA,
				CommittedPaths: stagedPaths,
				WriteScopeJSON: branchWriteScopeJSON,
				DerivedScope:   derivedScopePaths,
				PushAttempted:  true,
				PushSucceeded:  false,
				PushOutput:     pushOutput,
				CommitCreated:  true,
			})
			return &ToolResult{Output: fmt.Sprintf("project_branch_commit committed and registered %s but git push failed: %v\n%s", headSHA, err, payload), IsError: true}
		}
		pushSucceeded = true
	}

	return &ToolResult{Output: projectBranchCommitOutput(projectBranchCommitResult{
		ProjectID:      projectID,
		RepoID:         repo.RepoID,
		CheckoutID:     registeredCheckout.CheckoutID,
		BranchID:       updatedBranch.BranchID,
		BranchName:     updatedBranch.BranchName,
		BaseBranch:     baseBranch,
		BaseSHA:        baseSHA,
		HeadSHA:        headSHA,
		CommittedPaths: stagedPaths,
		WriteScopeJSON: branchWriteScopeJSON,
		DerivedScope:   derivedScopePaths,
		PushAttempted:  pushAttempted,
		PushSucceeded:  pushSucceeded,
		PushOutput:     pushOutput,
		CommitCreated:  true,
	})}
}

func (t *ProjectBranchCommitTool) pushCleanProjectBranchCommit(ctx context.Context, args map[string]any, projectID string, coordination ProjectCoordinationRecord, repo ProjectRepositoryRecord, branch ProjectBranchRecord, localPath, currentBranch, writeScopeJSON string) *ToolResult {
	baseBranch := firstNonEmpty(strings.TrimSpace(stringArg(args, "base_branch")), branch.BaseBranch, repo.DefaultBranch, coordination.Profile.RepoDefaultBranch, "main")
	baseSHA := projectBranchCommitCanonicalBaseSHA(ctx, localPath, baseBranch, branch.BaseSHA)
	headSHA, err := gitRevParse(ctx, localPath, "HEAD")
	if err != nil || strings.TrimSpace(headSHA) == "" {
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit push=true found no dirty changes but could not read existing HEAD: %v", err), IsError: true}
	}
	if strings.TrimSpace(baseSHA) != "" && strings.TrimSpace(headSHA) == strings.TrimSpace(baseSHA) {
		return &ToolResult{Output: "project_branch_commit push=true found no dirty changes and existing HEAD equals base_sha; there is no implementation commit to publish", IsError: true}
	}
	if strings.TrimSpace(baseSHA) != "" && !gitCommitIsAncestor(ctx, localPath, strings.TrimSpace(baseSHA), strings.TrimSpace(headSHA)) {
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     branch.CheckoutID,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        strings.TrimSpace(headSHA),
			WriteScopeJSON: writeScopeJSON,
			Stage:          "pre_push_stale_branch",
			CommitCreated:  false,
			Cause:          fmt.Errorf("existing HEAD %s is not descended from current base_sha %s", strings.TrimSpace(headSHA), strings.TrimSpace(baseSHA)),
		})
	}
	activeTaskID := firstNonEmpty(strings.TrimSpace(stringArg(args, "active_task_id")), strings.TrimSpace(branch.ActiveTaskID))
	activeClaimID := firstNonEmpty(strings.TrimSpace(stringArg(args, "active_claim_id")), strings.TrimSpace(branch.ActiveClaimID))
	if activeTaskID != "" && activeClaimID == "" {
		activeClaimID = activeTaskID
	}
	if activeClaimID != "" && activeTaskID == "" {
		activeTaskID = activeClaimID
	}
	checkoutIDForEvidence := strings.TrimSpace(branch.CheckoutID)
	machineID := runtimeMachineID()
	checkout, checkoutFound := findProjectBranchCommitCheckoutByID(coordination.Checkouts, checkoutIDForEvidence)
	if !checkoutFound {
		checkout = ProjectCheckoutRecord{CheckoutID: checkoutIDForEvidence}
	}
	if pathCheckout, ok := findProjectBranchCommitCheckoutByPath(coordination.Checkouts, repo.RepoID, t.agentID, machineID, localPath, t.workdir); ok {
		if checkoutIDForEvidence == "" || checkoutIDForEvidence != strings.TrimSpace(pathCheckout.CheckoutID) {
			if !projectBranchCommitCanAdoptPathCheckout(pathCheckout, activeTaskID, activeClaimID) {
				return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
					ProjectID:      projectID,
					RepoID:         repo.RepoID,
					Branch:         branch,
					CheckoutID:     checkoutIDForEvidence,
					LocalPath:      localPath,
					CurrentBranch:  currentBranch,
					BaseBranch:     baseBranch,
					BaseSHA:        baseSHA,
					HeadSHA:        strings.TrimSpace(headSHA),
					WriteScopeJSON: writeScopeJSON,
					Stage:          "pre_push_checkout_ownership",
					CommitCreated:  false,
					Cause:          fmt.Errorf("active checkout path %s belongs to checkout=%s active_task_id=%s active_claim_id=%s; resolve evidence ownership before publishing HEAD", localPath, pathCheckout.CheckoutID, pathCheckout.ActiveTaskID, pathCheckout.ActiveClaimID),
				})
			}
			checkout = pathCheckout
			checkoutFound = true
			checkoutIDForEvidence = strings.TrimSpace(pathCheckout.CheckoutID)
		}
	}
	if err := projectBranchCommitValidateCheckoutForEvidence(checkout, checkoutFound, machineID, localPath, t.workdir, activeTaskID, activeClaimID); err != nil {
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     checkoutIDForEvidence,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        strings.TrimSpace(headSHA),
			WriteScopeJSON: writeScopeJSON,
			Stage:          "pre_push_checkout_ownership",
			CommitCreated:  false,
			Cause:          err,
		})
	}
	registeredCheckout, err := t.client.RegisterProjectCheckout(ctx, ProjectCheckoutRegisterInput{
		WorkspaceID:   t.workspaceID,
		ProjectID:     projectID,
		ActorID:       t.agentID,
		CheckoutID:    checkoutIDForEvidence,
		RepoID:        repo.RepoID,
		MachineID:     firstNonEmpty(checkout.MachineID, machineID),
		MachineLabel:  firstNonEmpty(checkout.MachineLabel, machineID),
		OwnerUserID:   firstNonEmpty(checkout.OwnerUserID, t.ownerUserID),
		AgentID:       t.agentID,
		LocalPath:     localPath,
		CheckoutKind:  firstNonEmpty(checkout.CheckoutKind, "clone"),
		BranchName:    currentBranch,
		BaseBranch:    baseBranch,
		HeadSHA:       headSHA,
		BaseSHA:       baseSHA,
		DirtyState:    "clean",
		ActiveTaskID:  activeTaskID,
		ActiveClaimID: activeClaimID,
		Status:        "ACTIVE",
	})
	if err != nil {
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     checkoutIDForEvidence,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        headSHA,
			CommittedPaths: nil,
			WriteScopeJSON: writeScopeJSON,
			Stage:          "checkout_register",
			CommitCreated:  false,
			Cause:          err,
		})
	}
	updatedBranch, err := t.client.RegisterProjectBranch(ctx, ProjectBranchRegisterInput{
		WorkspaceID:    t.workspaceID,
		ProjectID:      projectID,
		ActorID:        t.agentID,
		BranchID:       branch.BranchID,
		RepoID:         repo.RepoID,
		CheckoutID:     registeredCheckout.CheckoutID,
		AgentID:        t.agentID,
		ActiveTaskID:   activeTaskID,
		ActiveClaimID:  activeClaimID,
		BranchName:     branch.BranchName,
		BranchKind:     firstNonEmpty(branch.BranchKind, "feature"),
		BaseBranch:     baseBranch,
		HeadSHA:        headSHA,
		BaseSHA:        baseSHA,
		WriteScopeJSON: firstNonEmpty(writeScopeJSON, branch.WriteScopeJSON),
		Status:         "ACTIVE",
	})
	if err != nil {
		return t.returnEvidenceReconciliationBlock(ctx, projectBranchCommitEvidenceReconciliationInput{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			Branch:         branch,
			CheckoutID:     registeredCheckout.CheckoutID,
			LocalPath:      localPath,
			CurrentBranch:  currentBranch,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        headSHA,
			CommittedPaths: nil,
			WriteScopeJSON: firstNonEmpty(writeScopeJSON, branch.WriteScopeJSON),
			Stage:          "branch_register",
			CommitCreated:  false,
			Cause:          err,
		})
	}
	remoteName := firstNonEmpty(strings.TrimSpace(stringArg(args, "remote_name")), "origin")
	out, err := gitCombined(ctx, localPath, "push", "-u", remoteName, branch.BranchName)
	pushOutput := strings.TrimSpace(out)
	if err != nil {
		payload := projectBranchCommitOutput(projectBranchCommitResult{
			ProjectID:      projectID,
			RepoID:         repo.RepoID,
			CheckoutID:     registeredCheckout.CheckoutID,
			BranchID:       updatedBranch.BranchID,
			BranchName:     updatedBranch.BranchName,
			BaseBranch:     baseBranch,
			BaseSHA:        baseSHA,
			HeadSHA:        headSHA,
			CommittedPaths: nil,
			PushAttempted:  true,
			PushSucceeded:  false,
			PushOutput:     pushOutput,
			CommitCreated:  false,
		})
		return &ToolResult{Output: fmt.Sprintf("project_branch_commit push=true registered existing %s but git push failed: %v\n%s", headSHA, err, payload), IsError: true}
	}
	return &ToolResult{Output: projectBranchCommitOutput(projectBranchCommitResult{
		ProjectID:      projectID,
		RepoID:         repo.RepoID,
		CheckoutID:     registeredCheckout.CheckoutID,
		BranchID:       updatedBranch.BranchID,
		BranchName:     updatedBranch.BranchName,
		BaseBranch:     baseBranch,
		BaseSHA:        baseSHA,
		HeadSHA:        headSHA,
		CommittedPaths: nil,
		PushAttempted:  true,
		PushSucceeded:  true,
		PushOutput:     pushOutput,
		CommitCreated:  false,
	})}
}

func (t *ProjectBranchCommitTool) blockProjectBranchCommitForSideEffects(ctx context.Context, projectID string, branch ProjectBranchRecord, dirtyPaths, pathset []string, sideEffects []AgentUpdateSideEffectV1) *ToolResult {
	return projectDirtySideEffectBlocker{
		client:      t.client,
		workspaceID: t.workspaceID,
		agentID:     t.agentID,
		ownerUserID: t.ownerUserID,
	}.returnSideEffectBlock(ctx, projectDirtySideEffectBlockInput{
		SourceTool:          "project_branch_commit",
		ProjectID:           projectID,
		Branch:              branch,
		DirtyPaths:          dirtyPaths,
		Pathset:             pathset,
		BlockedDetail:       "project_branch_commit found dirty paths outside the operational boundary before git add",
		UpdateSummary:       fmt.Sprintf("project_branch_commit blocked pending side-effect classification for branch %s", firstNonEmpty(strings.TrimSpace(branch.BranchID), strings.TrimSpace(branch.BranchName), "<unknown>")),
		MandatoryNextAction: "classify side effects before retrying project_branch_commit",
	}, sideEffects)
}

func (t *ProjectBranchCommitTool) ensureProjectBranchCommitSideEffectClassificationTask(ctx context.Context, projectID string, branch ProjectBranchRecord, dirtyPaths, pathset []string, sideEffects []AgentUpdateSideEffectV1) (string, bool, string) {
	route := projectDirtySideEffectBlocker{
		client:      t.client,
		workspaceID: t.workspaceID,
		agentID:     t.agentID,
		ownerUserID: t.ownerUserID,
	}.ensureSideEffectClassificationTask(ctx, projectDirtySideEffectBlockInput{
		ProjectID:  projectID,
		Branch:     branch,
		DirtyPaths: dirtyPaths,
		Pathset:    pathset,
	}, sideEffects)
	return route.TaskID, route.Created, route.Warning
}

func projectBranchCommitSideEffectClassificationTaskID(workspaceID, projectID string, branch ProjectBranchRecord, sideEffects []AgentUpdateSideEffectV1) string {
	refs := projectBranchCommitSideEffectRefs(sideEffects)
	sort.Strings(refs)
	fingerprint := strings.Join([]string{
		workspaceID,
		projectID,
		firstNonEmpty(branch.BranchID, branch.BranchName, "branch"),
		strings.Join(refs, "|"),
	}, "\x00")
	return "task-side-effect-classify-" + shortRefHash(fingerprint)
}

func projectBranchCommitSideEffectRefs(sideEffects []AgentUpdateSideEffectV1) []string {
	refs := make([]string, 0, len(sideEffects))
	for _, effect := range sideEffects {
		if ref := strings.TrimSpace(effect.SideEffectRef); ref != "" {
			refs = append(refs, ref)
		}
	}
	return uniqueNonEmptyStrings(refs)
}

func renderProjectBranchCommitSideEffectClassificationTask(projectID string, branch ProjectBranchRecord, dirtyPaths, pathset []string, sideEffects []AgentUpdateSideEffectV1) string {
	var b strings.Builder
	b.WriteString("A project branch integration attempt produced unintegrated side effects outside its operational boundary.\n\n")
	b.WriteString("- project_id: " + firstNonEmpty(strings.TrimSpace(projectID), "(unknown)") + "\n")
	b.WriteString("- branch_id: " + firstNonEmpty(strings.TrimSpace(branch.BranchID), "(unknown)") + "\n")
	b.WriteString("- branch_name: " + firstNonEmpty(strings.TrimSpace(branch.BranchName), "(unknown)") + "\n")
	b.WriteString("- owner_agent_id: " + firstNonEmpty(strings.TrimSpace(branch.AgentID), "(unknown)") + "\n")
	b.WriteString("- active_task_id: " + firstNonEmpty(strings.TrimSpace(branch.ActiveTaskID), "(unknown)") + "\n")
	b.WriteString("- current_boundary: " + strings.Join(pathset, ", ") + "\n")
	b.WriteString("- dirty_regions: " + strings.Join(dirtyPaths, ", ") + "\n\n")
	b.WriteString("Classify the side effects before any integration continues. Valid decisions: accept, quarantine, revert, split_tension, expand_boundary, reassign, request_verification.\n")
	b.WriteString("Do not silently widen scope. If expansion is valid, use the explicit project role/scope transition and include the reason. If the effect is useful but belongs elsewhere, split or reassign it. If it is drift, quarantine or revert it.\n\n")
	b.WriteString("Side-effect refs:\n")
	for _, effect := range sideEffects {
		b.WriteString("- " + firstNonEmpty(strings.TrimSpace(effect.SideEffectRef), "side-effect") +
			" | relation=" + firstNonEmpty(strings.TrimSpace(effect.BoundaryRelation), "unknown") +
			" | status=" + firstNonEmpty(strings.TrimSpace(effect.IntegrationStatus), "pending_classification") +
			" | decision=" + firstNonEmpty(strings.TrimSpace(effect.Decision), "none_pending") +
			" | region=" + firstNonEmpty(strings.TrimSpace(effect.RegionRef), "unknown") + "\n")
	}
	return b.String()
}

func projectBranchCommitSideEffectsForBoundary(workspaceID, agentID, projectID string, repo ProjectRepositoryRecord, branch ProjectBranchRecord, args map[string]any, pathset, dirtyPaths []string) []AgentUpdateSideEffectV1 {
	effects := make([]AgentUpdateSideEffectV1, 0)
	activeTaskID, activeClaimID := projectBranchCommitActiveTaskClaimIDs(args, branch)
	laneRef := projectBranchCommitLaneRef(workspaceID, projectID, branch, pathset)
	tensionRef := firstNonEmpty(activeTaskID, activeClaimID, projectID)
	artifactRef := "artifact:project:" + sanitizeRefSegment(projectID) + ":repo:" + sanitizeRefSegment(firstNonEmpty(repo.RepoID, branch.RepoID, "repo")) + ":branch:" + sanitizeRefSegment(firstNonEmpty(branch.BranchID, branch.BranchName, "branch"))
	boundaryRef := "boundary:project_branch:" + sanitizeRefSegment(firstNonEmpty(branch.BranchID, branch.BranchName, "branch"))
	for _, dirty := range dirtyPaths {
		dirty = strings.TrimSpace(dirty)
		if dirty == "" || pathsetIsCoveredByScope([]string{dirty}, pathset) {
			continue
		}
		relation := "out_of_boundary"
		intent := "unknown"
		if projectBranchCommitAdjacentFoundationCandidate(pathset, dirty) {
			relation = "adjacent_foundation_candidate"
			intent = "foundation_effect"
		}
		effects = append(effects, AgentUpdateSideEffectV1{
			Schema:               "artifact_bound_side_effect.v1",
			SideEffectRef:        projectBranchCommitSideEffectRef(workspaceID, projectID, branch, dirty),
			Actor:                agentID,
			LaneRef:              laneRef,
			TensionRef:           tensionRef,
			ArtifactRef:          artifactRef,
			RegionRef:            "region:adapter:git:path:" + dirty,
			Operation:            "change",
			SourceKind:           "adapter:git",
			BoundaryRef:          boundaryRef,
			BoundaryRelation:     relation,
			MaterializationState: "present_unintegrated",
			IntegrationIntent:    intent,
			IntegrationStatus:    "pending_classification",
			Decision:             "none_pending",
			Justification:        "project_branch_commit observed dirty path outside the branch operational boundary before git add; classify, split, expand, reassign, or verify before integration",
			DerivedRegionRefs:    []string{"region:adapter:git:path:" + dirty},
		})
	}
	return effects
}

func projectBranchCommitActiveTaskClaimIDs(args map[string]any, branch ProjectBranchRecord) (string, string) {
	activeTaskID := firstNonEmpty(strings.TrimSpace(stringArg(args, "active_task_id")), strings.TrimSpace(branch.ActiveTaskID))
	activeClaimID := firstNonEmpty(strings.TrimSpace(stringArg(args, "active_claim_id")), strings.TrimSpace(branch.ActiveClaimID))
	if activeTaskID != "" && activeClaimID == "" {
		activeClaimID = activeTaskID
	}
	if activeClaimID != "" && activeTaskID == "" {
		activeTaskID = activeClaimID
	}
	return activeTaskID, activeClaimID
}

func projectBranchCommitAdjacentFoundationCandidate(pathset []string, dirtyPath string) bool {
	switch strings.TrimSpace(dirtyPath) {
	case "README.md", ".gitignore":
		return projectBranchCommitScopeAllowsRootScaffoldSidecars(pathset)
	case "go.mod", "go.sum", "main.go":
		return projectBranchCommitScopeAllowsGoRootScaffoldSidecars(pathset)
	default:
		return false
	}
}

func projectBranchCommitLaneRef(workspaceID, projectID string, branch ProjectBranchRecord, pathset []string) string {
	parts := []string{
		"lane",
		sanitizeRefSegment(firstNonEmpty(workspaceID, "workspace")),
		sanitizeRefSegment(firstNonEmpty(projectID, "project")),
		sanitizeRefSegment(firstNonEmpty(branch.RepoID, "repo")),
		sanitizeRefSegment(firstNonEmpty(branch.BranchID, branch.BranchName, "branch")),
		sanitizeRefSegment(firstNonEmpty(branch.AgentID, "owner")),
	}
	for _, path := range pathset {
		parts = append(parts, sanitizeRefSegment(path))
	}
	return strings.Join(parts, ":")
}

func projectBranchCommitSideEffectRef(workspaceID, projectID string, branch ProjectBranchRecord, dirtyPath string) string {
	return strings.Join([]string{
		"side-effect",
		sanitizeRefSegment(firstNonEmpty(workspaceID, "workspace")),
		sanitizeRefSegment(firstNonEmpty(projectID, "project")),
		sanitizeRefSegment(firstNonEmpty(branch.BranchID, branch.BranchName, "branch")),
		sanitizeRefSegment(dirtyPath),
	}, ":")
}

type projectBranchCommitResult struct {
	ProjectID      string
	RepoID         string
	CheckoutID     string
	BranchID       string
	BranchName     string
	BaseBranch     string
	BaseSHA        string
	HeadSHA        string
	CommittedPaths []string
	WriteScopeJSON string
	DerivedScope   []string
	PushAttempted  bool
	PushSucceeded  bool
	PushOutput     string
	CommitCreated  bool
}

func selectProjectBranchCommitBranch(branches []ProjectBranchRecord, patchQueueItems []ProjectPatchQueueItemRecord, agentID, branchID, branchName, repoID string) (ProjectBranchRecord, error) {
	branch, err := selectProjectBranchReviewBranch(branches, agentID, branchID, branchName, repoID)
	if err != nil {
		return ProjectBranchRecord{}, fmt.Errorf("project_branch_commit requires an existing non-terminal branch owned by this agent")
	}
	switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
	case "RESERVED", "ACTIVE", "BLOCKED":
		return branch, nil
	case "READY_FOR_REVIEW":
		if projectBranchHasOpenPatchQueueItem(patchQueueItems, branch.BranchID) {
			return ProjectBranchRecord{}, fmt.Errorf("project_branch_commit refuses READY_FOR_REVIEW branch %s because it already has an open patch queue/integration item; create a follow-up revision task instead", branch.BranchID)
		}
		if item, ok := projectBranchAcceptedPatchQueueBoundary(patchQueueItems, branch.BranchID, branch.HeadSHA); ok {
			return ProjectBranchRecord{}, fmt.Errorf("project_branch_commit refuses READY_FOR_REVIEW branch %s because patch queue item %s/%s already ACCEPTED head %s; accepted source boundary is frozen until integration, rebuild, or explicit supersession", branch.BranchID, item.QueueID, item.ItemID, firstNonEmpty(strings.TrimSpace(item.HeadSHA), strings.TrimSpace(branch.HeadSHA)))
		}
		return branch, nil
	default:
		return ProjectBranchRecord{}, fmt.Errorf("project_branch_commit cannot commit branch %s with status %s", branch.BranchID, branch.Status)
	}
}

func projectBranchHasOpenPatchQueueItem(items []ProjectPatchQueueItemRecord, branchID string) bool {
	branchID = strings.TrimSpace(branchID)
	if branchID == "" {
		return false
	}
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		if !projectPatchQueueStateIsTerminal(item.State) {
			return true
		}
	}
	return false
}

func projectBranchAcceptedPatchQueueBoundary(items []ProjectPatchQueueItemRecord, branchID, _ string) (ProjectPatchQueueItemRecord, bool) {
	branchID = strings.TrimSpace(branchID)
	if branchID == "" {
		return ProjectPatchQueueItemRecord{}, false
	}
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
			return item, true
		}
	}
	return ProjectPatchQueueItemRecord{}, false
}

func projectPatchQueueStateIsTerminal(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "", "PROPOSED", "PENDING", "CLAIMED", "PROCESSING", "BOUND", "CAS_READY", "READY":
		return false
	case "ACCEPTED", "APPLIED", "MATERIALIZED", "INTEGRATED", "MERGED", "REJECTED", "FAILED", "CANCELLED", "CANCELED", "ABANDONED", "ARCHIVED", "DEAD_LETTERED":
		return true
	case "BLOCKED":
		return true
	default:
		return false
	}
}

func ensureGitGeneratedArtifactExcludes(ctx context.Context, localPath string) error {
	excludePath, err := gitCombined(ctx, localPath, "rev-parse", "--git-path", "info/exclude")
	if err != nil {
		return err
	}
	excludePath = strings.TrimSpace(excludePath)
	if excludePath == "" {
		return nil
	}
	if !filepath.IsAbs(excludePath) {
		excludePath = filepath.Join(strings.TrimSpace(localPath), excludePath)
	}
	const marker = "# Rhizome generated artifacts"
	patterns := []string{
		"__pycache__/",
		"*/__pycache__/",
		".pytest_cache/",
		"node_modules/",
		"*/node_modules/",
		".tmp/",
		"*/.tmp/",
		".tmp*/",
		"*/.tmp*/",
		"tmp/",
		"*/tmp/",
		".vite/",
		"*/.vite/",
		"dist/",
		"*/dist/",
		"Microsoft/Windows/PowerShell/ModuleAnalysisCache",
		"Microsoft/Windows/PowerShell/StartupProfileData-*.json",
		"%SystemDrive%/ProgramData/Microsoft/Windows/PowerShell/ModuleAnalysisCache",
		"%SystemDrive%/ProgramData/Microsoft/Windows/PowerShell/StartupProfileData-*.json",
		"*.tsbuildinfo",
	}
	raw, err := os.ReadFile(excludePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	content := string(raw)
	padded := "\n" + content + "\n"
	missing := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		if !strings.Contains(padded, "\n"+pattern+"\n") {
			missing = append(missing, pattern)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(excludePath), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(excludePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if strings.TrimSpace(content) != "" && !strings.HasSuffix(content, "\n") {
		if _, err := f.WriteString("\n"); err != nil {
			return err
		}
	}
	if !strings.Contains(padded, "\n"+marker+"\n") {
		if _, err := f.WriteString("\n" + marker + "\n"); err != nil {
			return err
		}
	}
	for _, pattern := range missing {
		if _, err := f.WriteString(pattern + "\n"); err != nil {
			return err
		}
	}
	return nil
}

func sameStringSliceSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, item := range a {
		counts[item]++
	}
	for _, item := range b {
		if counts[item] == 0 {
			return false
		}
		counts[item]--
	}
	return true
}

func projectBranchCommitDirtyPaths(ctx context.Context, localPath string) ([]string, error) {
	pathSets := [][]string{
		{"diff", "--name-only"},
		{"diff", "--cached", "--name-only"},
		{"ls-files", "--others", "--exclude-standard"},
	}
	seen := map[string]struct{}{}
	for _, args := range pathSets {
		out, err := gitCombined(ctx, localPath, args...)
		if err != nil {
			return nil, err
		}
		for _, raw := range gitOutputNonDiagnosticLines(out) {
			normalized, err := normalizeReviewPath(raw)
			if err != nil {
				return nil, err
			}
			seen[normalized] = struct{}{}
		}
	}
	paths := make([]string, 0, len(seen))
	for path := range seen {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, nil
}

func gitOutputNonDiagnosticLines(out string) []string {
	lines := make([]string, 0)
	for _, line := range strings.Split(out, "\n") {
		raw := strings.TrimSpace(line)
		if raw == "" || isGitDiagnosticLine(raw) {
			continue
		}
		lines = append(lines, raw)
	}
	return lines
}

func isGitDiagnosticLine(line string) bool {
	line = strings.ToLower(strings.TrimSpace(line))
	return strings.HasPrefix(line, "warning: ") || strings.HasPrefix(line, "hint: ")
}

func projectBranchCommitPaths(dirtyPaths, explicitPaths, pathset []string) ([]string, error) {
	if len(dirtyPaths) == 0 {
		return nil, fmt.Errorf("no dirty paths to commit")
	}
	outOfScope := projectBranchCommitOutOfScopePaths(dirtyPaths, pathset)
	if len(outOfScope) > 0 {
		return nil, fmt.Errorf("project_branch_commit dirty paths are outside branch write scope: %s. If these are real product/scaffold files, run project_role_assign with an expanded write_scope_json such as %s. Strategic leads can apply the correction directly; non-lead agents should call project_role_assign anyway so Rhizome creates a durable lead coordination task/wake instead of a model.ask-only request. Generated dependency/cache paths such as node_modules/, Microsoft/Windows/PowerShell/ModuleAnalysisCache, and *.tsbuildinfo are ignored automatically when untracked", strings.Join(outOfScope, ", "), projectBranchCommitWriteScopeCorrectionSuggestion(pathset, outOfScope))
	}
	if len(explicitPaths) == 0 {
		return append([]string(nil), dirtyPaths...), nil
	}
	commitPaths, err := projectPatchQueueNormalizeCandidatePaths(explicitPaths, pathset)
	if err != nil {
		return nil, fmt.Errorf("candidate_paths invalid: %w", err)
	}
	commitSet := map[string]struct{}{}
	for _, item := range commitPaths {
		commitSet[item] = struct{}{}
	}
	missing := make([]string, 0)
	for _, dirty := range dirtyPaths {
		if _, ok := commitSet[dirty]; !ok {
			missing = append(missing, dirty)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("project_branch_commit candidate_paths omit dirty paths that would remain uncommitted: %s", strings.Join(missing, ", "))
	}
	return commitPaths, nil
}

func projectBranchCommitOutOfScopePaths(paths, pathset []string) []string {
	outOfScope := make([]string, 0)
	for _, path := range paths {
		if !pathsetIsCoveredByScope([]string{path}, pathset) {
			outOfScope = append(outOfScope, path)
		}
	}
	outOfScope = uniqueNonEmptyStrings(outOfScope)
	sort.Strings(outOfScope)
	return outOfScope
}

func projectBranchCommitMergeScope(pathset, extra []string) []string {
	merged := append([]string(nil), pathset...)
	for _, item := range extra {
		if strings.TrimSpace(item) == "" || pathsetIsCoveredByScope([]string{item}, merged) {
			continue
		}
		merged = append(merged, item)
	}
	merged = uniqueNonEmptyStrings(merged)
	sort.Strings(merged)
	return merged
}

func projectBranchCommitEffectiveWriteScope(pathset, dirtyPaths []string) ([]string, []string) {
	effective := append([]string(nil), pathset...)
	rootSidecarsAllowed := projectBranchCommitScopeAllowsRootScaffoldSidecars(pathset)
	goRootAllowed := projectBranchCommitScopeAllowsGoRootScaffoldSidecars(pathset)
	goModuleSidecarsAllowed := projectBranchCommitScopeAllowsGoModuleSidecars(pathset)
	if !rootSidecarsAllowed && !goRootAllowed && !goModuleSidecarsAllowed {
		return effective, nil
	}
	derived := make([]string, 0, 5)
	sidecars := []string{}
	if rootSidecarsAllowed || goRootAllowed {
		sidecars = append(sidecars, "README.md", ".gitignore")
	}
	if goRootAllowed || goModuleSidecarsAllowed {
		sidecars = append(sidecars, "go.mod", "go.sum")
	}
	if goRootAllowed {
		sidecars = append(sidecars, "main.go")
	}
	for _, sidecar := range sidecars {
		if !stringSliceContains(dirtyPaths, sidecar) || pathsetIsCoveredByScope([]string{sidecar}, effective) {
			continue
		}
		effective = append(effective, sidecar)
		derived = append(derived, sidecar)
	}
	if len(derived) == 0 {
		return effective, nil
	}
	sort.Strings(effective)
	return effective, derived
}

func projectBranchCommitScopeAllowsRootScaffoldSidecars(pathset []string) bool {
	hasPackageManifest := pathsetCoversAny(pathset, []string{"package.json"})
	hasRootEntryOrConfig := pathsetCoversAny(pathset, []string{
		"index.html",
		"vite.config.ts",
		"vite.config.js",
		"next.config.js",
		"next.config.mjs",
		"tsconfig.json",
	})
	hasAppSurface := pathsetCoversAny(pathset, []string{
		"src/main.tsx",
		"src/main.ts",
		"src/index.tsx",
		"src/index.ts",
		"src/app/App.tsx",
		"src/app/app.tsx",
		"src/styles/global.css",
		"public/index.html",
		"app/page.tsx",
		"pages/index.tsx",
	})
	return hasPackageManifest && hasRootEntryOrConfig && hasAppSurface
}

func projectBranchCommitScopeAllowsGoRootScaffoldSidecars(pathset []string) bool {
	hasCommandSurface := pathsetCoversAny(pathset, []string{
		"cmd/**",
		"cmd/*",
		"cmd/rq/**",
		"main.go",
	})
	hasCLISurface := pathsetCoversAny(pathset, []string{
		"internal/cli/**",
		"internal/repl/**",
	})
	hasGoImplementationSurface := pathsetCoversAny(pathset, []string{
		"internal/**",
		"pkg/**",
	})
	return hasCommandSurface && (hasCLISurface || hasGoImplementationSurface)
}

func projectBranchCommitScopeAllowsGoModuleSidecars(pathset []string) bool {
	for _, item := range pathset {
		normalized := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(item, "\\", "/")))
		normalized = strings.TrimPrefix(normalized, "./")
		switch {
		case normalized == "main.go",
			strings.HasPrefix(normalized, "cmd/"),
			normalized == "cmd/**",
			normalized == "cmd/*",
			strings.HasPrefix(normalized, "internal/"),
			normalized == "internal/**",
			strings.HasPrefix(normalized, "pkg/"),
			normalized == "pkg/**":
			return true
		}
	}
	return false
}

func pathsetCoversAny(pathset, candidates []string) bool {
	for _, candidate := range candidates {
		if pathsetIsCoveredByScope([]string{candidate}, pathset) {
			return true
		}
	}
	return false
}

func projectBranchCommitWriteScopeCorrectionSuggestion(pathset, outOfScope []string) string {
	merged := append([]string(nil), pathset...)
	for _, item := range outOfScope {
		if strings.TrimSpace(item) == "" || pathsetIsCoveredByScope([]string{item}, merged) {
			continue
		}
		merged = append(merged, item)
	}
	sort.Strings(merged)
	return projectBranchCommitWriteScopeJSON(merged)
}

func projectBranchCommitWriteScopeJSON(pathset []string) string {
	raw, err := json.Marshal(map[string][]string{"paths": pathset})
	if err != nil {
		return `{"paths":[]}`
	}
	return string(raw)
}

func projectBranchGitAdd(ctx context.Context, localPath string, paths []string) (string, error) {
	args := append([]string{"add", "--"}, paths...)
	return gitCombined(ctx, localPath, args...)
}

func projectBranchCommitStagedPaths(ctx context.Context, localPath string) ([]string, error) {
	out, err := gitCombined(ctx, localPath, "diff", "--cached", "--name-only")
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0)
	seen := map[string]struct{}{}
	for _, raw := range gitOutputNonDiagnosticLines(out) {
		normalized, err := normalizeReviewPath(raw)
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

func projectBranchGitCommit(ctx context.Context, localPath, agentID, message string) (string, error) {
	authorSegment := sanitizeRefSegment(firstNonEmpty(agentID, "agent"))
	authorName := "Rhizome Agent " + authorSegment
	authorEmail := authorSegment + "@rhizome.local"
	return gitCombined(ctx, localPath,
		"-c", "user.name="+authorName,
		"-c", "user.email="+authorEmail,
		"commit", "--no-gpg-sign", "-m", strings.TrimSpace(message),
	)
}

func projectBranchCommitResolveBaseSHA(ctx context.Context, localPath, baseBranch string) string {
	baseBranch = strings.TrimSpace(baseBranch)
	for _, ref := range []string{"refs/remotes/origin/" + baseBranch, "origin/" + baseBranch, baseBranch} {
		ref = strings.TrimSpace(ref)
		if ref == "" || ref == "origin/" || ref == "refs/remotes/origin/" {
			continue
		}
		if sha, err := gitRevParse(ctx, localPath, ref); err == nil && strings.TrimSpace(sha) != "" {
			return strings.TrimSpace(sha)
		}
	}
	return ""
}

func projectBranchCommitCanonicalBaseSHA(ctx context.Context, localPath, baseBranch, recordedBaseSHA string) string {
	canonical := projectBranchCommitResolveBaseSHA(ctx, localPath, baseBranch)
	if canonical != "" {
		return canonical
	}
	return strings.TrimSpace(recordedBaseSHA)
}

func gitCommitIsAncestor(ctx context.Context, localPath, ancestor, descendant string) bool {
	ancestor = strings.TrimSpace(ancestor)
	descendant = strings.TrimSpace(descendant)
	if ancestor == "" || descendant == "" {
		return false
	}
	_, err := gitCombined(ctx, localPath, "merge-base", "--is-ancestor", ancestor, descendant)
	return err == nil
}

func projectBranchCommitMessage(args map[string]any, projectID string, branch ProjectBranchRecord) string {
	message := strings.TrimSpace(stringArg(args, "commit_message"))
	if message != "" {
		return message
	}
	return fmt.Sprintf("Update %s branch %s", sanitizeRefSegment(projectID), sanitizeRefSegment(branch.BranchName))
}

func findProjectBranchCommitCheckout(checkouts []ProjectCheckoutRecord, checkoutID string) ProjectCheckoutRecord {
	if checkout, ok := findProjectBranchCommitCheckoutByID(checkouts, checkoutID); ok {
		return checkout
	}
	return ProjectCheckoutRecord{CheckoutID: strings.TrimSpace(checkoutID)}
}

func findProjectBranchCommitCheckoutByID(checkouts []ProjectCheckoutRecord, checkoutID string) (ProjectCheckoutRecord, bool) {
	checkoutID = strings.TrimSpace(checkoutID)
	for _, checkout := range checkouts {
		if strings.TrimSpace(checkout.CheckoutID) == checkoutID {
			return checkout, true
		}
	}
	return ProjectCheckoutRecord{}, false
}

func findProjectBranchCommitCheckoutByPath(checkouts []ProjectCheckoutRecord, repoID, agentID, machineID, localPath, workdir string) (ProjectCheckoutRecord, bool) {
	machineID = strings.TrimSpace(machineID)
	for _, checkout := range checkouts {
		if strings.TrimSpace(checkout.RepoID) != strings.TrimSpace(repoID) {
			continue
		}
		if strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(agentID) {
			continue
		}
		if machineID == "" || strings.TrimSpace(checkout.MachineID) == "" || strings.TrimSpace(checkout.MachineID) != machineID {
			continue
		}
		if !projectCheckoutWriteStatusActive(checkout.Status) {
			continue
		}
		if sameProjectLocalPathWithinWorkdir(workdir, checkout.LocalPath, localPath) || sameRuntimePath(checkout.LocalPath, localPath) {
			return checkout, true
		}
	}
	return ProjectCheckoutRecord{}, false
}

func projectBranchCommitValidateCheckoutForEvidence(checkout ProjectCheckoutRecord, found bool, machineID, localPath, workdir, activeTaskID, activeClaimID string) error {
	if !found || strings.TrimSpace(checkout.CheckoutID) == "" {
		return nil
	}
	checkoutMachineID := strings.TrimSpace(checkout.MachineID)
	if checkoutMachineID != "" && strings.TrimSpace(machineID) != "" && checkoutMachineID != strings.TrimSpace(machineID) {
		return fmt.Errorf("registered branch checkout_id=%s belongs to machine_id=%s, current machine_id=%s; resolve checkout evidence before mutating local git state", checkout.CheckoutID, checkoutMachineID, strings.TrimSpace(machineID))
	}
	if strings.TrimSpace(checkout.LocalPath) != "" && !sameProjectLocalPathWithinWorkdir(workdir, checkout.LocalPath, localPath) && !sameRuntimePath(checkout.LocalPath, localPath) {
		return fmt.Errorf("registered branch checkout_id=%s points to local_path=%s, not current local_path=%s; resolve checkout evidence before mutating local git state", checkout.CheckoutID, checkout.LocalPath, localPath)
	}
	if !projectBranchCommitCanAdoptPathCheckout(checkout, activeTaskID, activeClaimID) {
		return fmt.Errorf("registered branch checkout_id=%s belongs to active_task_id=%s active_claim_id=%s, not current active_task_id=%s active_claim_id=%s", checkout.CheckoutID, checkout.ActiveTaskID, checkout.ActiveClaimID, strings.TrimSpace(activeTaskID), strings.TrimSpace(activeClaimID))
	}
	return nil
}

func projectBranchCommitCanAdoptPathCheckout(checkout ProjectCheckoutRecord, activeTaskID, activeClaimID string) bool {
	activeTaskID = strings.TrimSpace(activeTaskID)
	activeClaimID = strings.TrimSpace(activeClaimID)
	checkoutTaskID := strings.TrimSpace(checkout.ActiveTaskID)
	checkoutClaimID := strings.TrimSpace(checkout.ActiveClaimID)
	if checkoutTaskID == "" && checkoutClaimID == "" {
		return true
	}
	if checkoutTaskID != "" && activeTaskID != "" && checkoutTaskID != activeTaskID {
		return false
	}
	if checkoutClaimID != "" && activeClaimID != "" && checkoutClaimID != activeClaimID {
		return false
	}
	if checkoutTaskID != "" && activeTaskID == "" {
		return false
	}
	if checkoutClaimID != "" && activeClaimID == "" {
		return false
	}
	return true
}

type projectBranchCommitEvidenceReconciliationInput struct {
	ProjectID      string
	RepoID         string
	Branch         ProjectBranchRecord
	CheckoutID     string
	LocalPath      string
	CurrentBranch  string
	BaseBranch     string
	BaseSHA        string
	HeadSHA        string
	CommittedPaths []string
	WriteScopeJSON string
	Stage          string
	CommitCreated  bool
	Cause          error
	SourceTool     string
}

func (t *ProjectBranchCommitTool) returnEvidenceReconciliationBlock(ctx context.Context, input projectBranchCommitEvidenceReconciliationInput) *ToolResult {
	return returnProjectBranchEvidenceReconciliationBlock(ctx, projectBranchEvidenceReconciliationContext{
		client:      t.client,
		workspaceID: t.workspaceID,
		agentID:     t.agentID,
		ownerUserID: t.ownerUserID,
		sourceTool:  "project_branch_commit",
	}, input)
}

type projectBranchEvidenceReconciliationContext struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
	ownerUserID string
	sourceTool  string
}

func returnProjectBranchEvidenceReconciliationBlock(ctx context.Context, owner projectBranchEvidenceReconciliationContext, input projectBranchCommitEvidenceReconciliationInput) *ToolResult {
	cause := ""
	if input.Cause != nil {
		cause = strings.TrimSpace(input.Cause.Error())
	}
	sourceTool := firstNonEmpty(strings.TrimSpace(owner.sourceTool), "project_branch_commit")
	input.SourceTool = sourceTool
	attemptRef := projectBranchCommitMutationAttemptRef(owner.workspaceID, input.ProjectID, input.Branch, input.HeadSHA)
	taskID := projectBranchCommitEvidenceReconciliationTaskID(owner.workspaceID, input.ProjectID, input.Branch, input.HeadSHA)
	activeTaskID := strings.TrimSpace(input.Branch.ActiveTaskID)
	activeClaimID := strings.TrimSpace(input.Branch.ActiveClaimID)
	materializationState := "checkout_identity_unresolved"
	blockedDetail := sourceTool + " could not prove checkout/branch ownership; repair the durable route instead of retrying the same mutation"
	mandatoryNextTool := "repair checkout/branch ownership evidence, then retry the appropriate project_checkout_materialize or project_branch_commit step"
	if input.CommitCreated {
		materializationState = "committed_unregistered"
		blockedDetail = sourceTool + " created or adopted a local commit but could not register checkout/branch evidence; adopt the existing head instead of retrying git commit"
		mandatoryNextTool = "project_branch_commit push=true after evidence ownership is repaired, then project_branch_review_ready"
	}
	payload := map[string]any{
		"schema":                 "project_branch_commit_mutation_attempt.v1",
		"status":                 "blocked",
		"gate_state":             "blocked",
		"gate_type":              "evidence_reconciliation",
		"next_action":            "evidence_reconciliation",
		"next_executable_lane":   "evidence_reconciliation_task",
		"mutation_attempt_ref":   attemptRef,
		"mutation_attempt_state": "NEEDS_RECONCILIATION",
		"stage":                  firstNonEmpty(strings.TrimSpace(input.Stage), "evidence_register"),
		"source_tool":            sourceTool,
		"project_id":             input.ProjectID,
		"repo_id":                input.RepoID,
		"branch_id":              input.Branch.BranchID,
		"branch_name":            input.Branch.BranchName,
		"checkout_id":            input.CheckoutID,
		"local_path":             input.LocalPath,
		"current_branch":         input.CurrentBranch,
		"base_branch":            input.BaseBranch,
		"base_sha":               input.BaseSHA,
		"head_sha":               input.HeadSHA,
		"committed_paths":        input.CommittedPaths,
		"write_scope_json":       input.WriteScopeJSON,
		"commit_created":         input.CommitCreated,
		"materialization_state":  materializationState,
		"integration_status":     "needs_reconciliation",
		"blocked_on": []map[string]string{{
			"kind":   "evidence_reconciliation",
			"detail": blockedDetail,
		}},
		"task_ids":            uniqueNonEmptyStrings([]string{activeTaskID, activeClaimID}),
		"cause":               cause,
		"mandatory_next_tool": mandatoryNextTool,
		"do_not_retry_tools":  uniqueNonEmptyStrings([]string{sourceTool, "project_checkout_materialize", "git commit"}),
	}
	rawPayload, payloadErr := json.Marshal(payload)
	updateWarning := ""
	if payloadErr != nil {
		updateWarning = payloadErr.Error()
	} else if owner.client != nil {
		if err := owner.client.PostUpdate(ctx, UpdatePostInput{
			WorkspaceID:   owner.workspaceID,
			AgentID:       owner.agentID,
			UpdateType:    "blocked",
			Summary:       fmt.Sprintf("%s needs evidence reconciliation for branch %s at %s", sourceTool, firstNonEmpty(strings.TrimSpace(input.Branch.BranchID), strings.TrimSpace(input.Branch.BranchName), "branch"), strings.TrimSpace(input.HeadSHA)),
			PayloadJSON:   string(rawPayload),
			RequiresHuman: false,
		}); err != nil {
			updateWarning = err.Error()
		}
	}
	classificationTaskCreated := false
	classificationTaskWarning := ""
	if owner.client != nil && owner.workspaceID != "" {
		requiresGate := false
		if result, err := owner.client.SubmitTask(ctx, TaskSubmitInput{
			WorkspaceID:         owner.workspaceID,
			TaskID:              taskID,
			OwnerUserID:         firstNonEmpty(owner.ownerUserID, owner.agentID),
			Priority:            "high",
			Title:               "Reconcile project branch evidence for " + firstNonEmpty(strings.TrimSpace(input.Branch.BranchName), strings.TrimSpace(input.Branch.BranchID), "branch"),
			Description:         renderProjectBranchCommitEvidenceReconciliationTask(input, attemptRef),
			TaskKind:            "COORDINATION",
			TaskTemplate:        "generic",
			TaskClass:           "INCIDENT",
			TaskClassSource:     "EXPLICIT",
			ProjectID:           input.ProjectID,
			ProjectLane:         "coordination",
			RequiresProjectGate: &requiresGate,
			TaskRequirements: map[string]any{
				"schema":                 "project_branch_commit_evidence_reconciliation_task.v1",
				"mutation_attempt_ref":   attemptRef,
				"mutation_attempt_state": "NEEDS_RECONCILIATION",
				"source_tool":            sourceTool,
				"branch_id":              input.Branch.BranchID,
				"branch_name":            input.Branch.BranchName,
				"checkout_id":            input.CheckoutID,
				"local_path":             input.LocalPath,
				"head_sha":               input.HeadSHA,
				"base_sha":               input.BaseSHA,
				"committed_paths":        input.CommittedPaths,
				"commit_created":         input.CommitCreated,
				"stage":                  firstNonEmpty(strings.TrimSpace(input.Stage), "evidence_register"),
				"allowed_decisions":      []string{"repair_checkout_identity", "adopt_dirty_checkout_identity", "adopt_existing_commit", "terminalize_stale_checkout", "request_verification"},
			},
			Tags:     []string{"evidence-reconciliation", "mutation-attempt", "artifact-bound-process-control", "project-coordination"},
			LinkedBy: owner.agentID,
		}); err != nil {
			if isDuplicateTaskSubmitError(err) {
				classificationTaskWarning = "reconciliation task already exists"
			} else {
				classificationTaskWarning = err.Error()
			}
		} else {
			classificationTaskCreated = true
			if strings.TrimSpace(result.TaskID) != "" {
				taskID = strings.TrimSpace(result.TaskID)
			}
		}
	}
	output := payload
	output["reconciliation_task_id"] = taskID
	output["reconciliation_task_created"] = classificationTaskCreated
	if strings.TrimSpace(updateWarning) != "" {
		output["mutation_attempt_update_warning"] = updateWarning
	}
	if strings.TrimSpace(classificationTaskWarning) != "" {
		output["reconciliation_task_warning"] = classificationTaskWarning
	}
	out, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("%s committed/adopted %s but needs evidence_reconciliation: %s", sourceTool, input.HeadSHA, cause), IsError: true}
	}
	return &ToolResult{Output: string(out), IsError: true}
}

func projectBranchCommitBestEffortHeadSHA(ctx context.Context, localPath string) string {
	headSHA, err := gitRevParse(ctx, localPath, "HEAD")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(headSHA)
}

func projectBranchCommitMutationAttemptRef(workspaceID, projectID string, branch ProjectBranchRecord, headSHA string) string {
	return "mutation-attempt:" + shortRefHash(strings.Join([]string{
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		firstNonEmpty(strings.TrimSpace(branch.BranchID), strings.TrimSpace(branch.BranchName), "branch"),
		strings.TrimSpace(headSHA),
	}, "\x00"))
}

func projectBranchCommitEvidenceReconciliationTaskID(workspaceID, projectID string, branch ProjectBranchRecord, headSHA string) string {
	return "task-evidence-reconcile-" + shortRefHash(strings.Join([]string{
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		firstNonEmpty(strings.TrimSpace(branch.BranchID), strings.TrimSpace(branch.BranchName), "branch"),
		strings.TrimSpace(headSHA),
	}, "\x00"))
}

func renderProjectBranchCommitEvidenceReconciliationTask(input projectBranchCommitEvidenceReconciliationInput, attemptRef string) string {
	var b strings.Builder
	sourceTool := firstNonEmpty(strings.TrimSpace(input.SourceTool), "project_branch_commit")
	if input.CommitCreated {
		b.WriteString("A " + sourceTool + " mutation materialized or adopted a local commit but failed durable checkout/branch evidence registration.\n\n")
	} else {
		b.WriteString("A " + sourceTool + " operation hit a checkout/branch ownership guard, so the branch needs explicit reconciliation before retry.\n\n")
	}
	b.WriteString("- mutation_attempt_ref: " + firstNonEmpty(strings.TrimSpace(attemptRef), "(unknown)") + "\n")
	b.WriteString("- project_id: " + firstNonEmpty(strings.TrimSpace(input.ProjectID), "(unknown)") + "\n")
	b.WriteString("- repo_id: " + firstNonEmpty(strings.TrimSpace(input.RepoID), "(unknown)") + "\n")
	b.WriteString("- branch_id: " + firstNonEmpty(strings.TrimSpace(input.Branch.BranchID), "(unknown)") + "\n")
	b.WriteString("- branch_name: " + firstNonEmpty(strings.TrimSpace(input.Branch.BranchName), "(unknown)") + "\n")
	b.WriteString("- checkout_id: " + firstNonEmpty(strings.TrimSpace(input.CheckoutID), "(unknown)") + "\n")
	b.WriteString("- local_path: " + firstNonEmpty(strings.TrimSpace(input.LocalPath), "(unknown)") + "\n")
	b.WriteString("- current_branch: " + firstNonEmpty(strings.TrimSpace(input.CurrentBranch), "(unknown)") + "\n")
	b.WriteString("- head_sha: " + firstNonEmpty(strings.TrimSpace(input.HeadSHA), "(unknown)") + "\n")
	b.WriteString("- base_sha: " + firstNonEmpty(strings.TrimSpace(input.BaseSHA), "(unknown)") + "\n")
	b.WriteString("- failed_stage: " + firstNonEmpty(strings.TrimSpace(input.Stage), "evidence_register") + "\n")
	b.WriteString("- source_tool: " + sourceTool + "\n")
	if input.Cause != nil {
		b.WriteString("- cause: " + strings.TrimSpace(input.Cause.Error()) + "\n")
	}
	if len(input.CommittedPaths) > 0 {
		b.WriteString("- committed_paths: " + strings.Join(input.CommittedPaths, ", ") + "\n")
	}
	if input.CommitCreated {
		b.WriteString("\nResolve the durable evidence mismatch. Do not retry git commit. Repair/adopt the existing local HEAD by fixing checkout identity or terminalizing stale checkout evidence, then rerun project_branch_commit with push=true and publish project_branch_review_ready.\n")
	} else {
		b.WriteString("\nResolve the branch/checkout ownership mismatch. Do not publish from a checkout whose branch identity or base lineage differs from durable branch evidence. Repair/adopt the correct checkout identity, terminalize stale checkout evidence, or request verification before retrying.\n")
	}
	return b.String()
}

func projectBranchCommitOutput(result projectBranchCommitResult) string {
	payload := map[string]any{
		"project_id":                result.ProjectID,
		"repo_id":                   result.RepoID,
		"checkout_id":               result.CheckoutID,
		"branch_id":                 result.BranchID,
		"branch_name":               result.BranchName,
		"base_branch":               result.BaseBranch,
		"base_sha":                  result.BaseSHA,
		"head_sha":                  result.HeadSHA,
		"committed_paths":           result.CommittedPaths,
		"write_scope_json":          result.WriteScopeJSON,
		"derived_write_scope_paths": result.DerivedScope,
		"commit_created":            result.CommitCreated,
		"push_attempted":            result.PushAttempted,
		"push_succeeded":            result.PushSucceeded,
		"push_output":               result.PushOutput,
		"worktree_clean":            true,
		"auto_merge":                false,
		"next_gate":                 "project_branch_review_ready",
		"mandatory_next_tool":       "project_branch_review_ready",
		"do_not_retry_tools":        []string{"project_checkout_materialize"},
		"integration_rule":          "Run project_branch_review_ready with verification evidence before project_patch_queue_submit. This tool did not merge or rebase.",
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Sprintf("project_branch_commit completed branch %s at %s", result.BranchID, result.HeadSHA)
	}
	return string(raw)
}
