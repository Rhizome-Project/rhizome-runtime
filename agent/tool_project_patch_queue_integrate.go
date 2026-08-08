package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
)

type ProjectPatchQueueIntegrateTool struct {
	client           *RhizomeClient
	workspaceID      string
	agentID          string
	ownerUserID      string
	workdir          string
	coordinationMode string
	runtimeBinding   func() AgentRuntimeBinding
}

func NewProjectPatchQueueIntegrateTool(client *RhizomeClient, workspaceID, agentID, ownerUserID, workdir string) *ProjectPatchQueueIntegrateTool {
	return &ProjectPatchQueueIntegrateTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		ownerUserID: strings.TrimSpace(ownerUserID),
		workdir:     strings.TrimSpace(workdir),
	}
}

func (t *ProjectPatchQueueIntegrateTool) WithCoordinationMode(mode string) *ProjectPatchQueueIntegrateTool {
	if t != nil {
		t.coordinationMode = normalizeCoordinationMode(mode)
	}
	return t
}

func (t *ProjectPatchQueueIntegrateTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectPatchQueueIntegrateTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectPatchQueueIntegrateTool) Name() string { return "project_patch_queue_integrate" }

func (t *ProjectPatchQueueIntegrateTool) Description() string {
	return "Merge an ACCEPTED project patch queue branch into the canonical integration/default branch, push the target branch, and register durable patch queue integration or repair evidence. Requires active integration authority even in trust_first mode; controlled queue direct-merge requires explicit integration_mode=direct_merge or an active task-bound integration contract for the same queue item."
}

func (t *ProjectPatchQueueIntegrateTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id":    map[string]any{"type": "string", "description": "Project id."},
			"queue_id":      map[string]any{"type": "string", "description": "Patch queue id."},
			"item_id":       map[string]any{"type": "string", "description": "Patch queue item id."},
			"branch_id":     map[string]any{"type": "string", "description": "Optional branch id when queue_id/item_id are unknown."},
			"repo_id":       map[string]any{"type": "string", "description": "Optional repository id guard."},
			"target_branch": map[string]any{"type": "string", "description": "Canonical target branch. Defaults to repo integration/default branch or project repo default branch."},
			"local_path":    map[string]any{"type": "string", "description": "Optional integration checkout path inside this agent workdir."},
			"merge_message": map[string]any{"type": "string", "description": "Optional git merge commit message."},
			"push":          map[string]any{"type": "boolean", "description": "Push the updated target branch to origin. Defaults true."},
			"remote_name":   map[string]any{"type": "string", "description": "Remote name to fetch/push. Defaults origin."},
			"integration_mode": map[string]any{
				"type":        "string",
				"description": "Optional integration mode. Controlled queue items require materialization evidence unless integration_mode=direct_merge is explicitly supplied.",
				"enum":        []string{"materialization", "direct_merge"},
			},
		},
		"required": []string{"project_id"},
	}
}

func (t *ProjectPatchQueueIntegrateTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_patch_queue_integrate is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	if strings.TrimSpace(t.workdir) == "" {
		return &ToolResult{Output: "project_patch_queue_integrate requires an agent workdir", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_patch_queue_integrate", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}

	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_integrate failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	trustFirst := runtimeTrustFirst(RuntimeConfig{CoordinationMode: t.coordinationMode})
	hasAuthority := projectAgentHasPatchQueueIntegrationAuthority(coordination, t.agentID)
	authorityMode := "project_role"
	if !hasAuthority {
		message := "project_patch_queue_integrate requires this agent to hold an active INTEGRATOR role or strategic lead lease before canonical branch mutation"
		if trustFirst {
			message += "; trust_first authority is advisory and cannot push canonical integration"
		}
		return &ToolResult{Output: message, IsError: true}
	}
	repoGuard := projectToolRefArg(args, "repo_id")
	item, err := selectProjectPatchQueueIntegrationItem(coordination.PatchQueueItems, stringArg(args, "queue_id"), stringArg(args, "item_id"), stringArg(args, "branch_id"), repoGuard)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if !strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_integrate requires an ACCEPTED patch queue item, got state=%s for %s/%s", item.State, item.QueueID, item.ItemID), IsError: true}
	}
	if defeater, ok := projectPatchQueueIntegrateDefeatingItem(coordination.PatchQueueItems, item); ok {
		return t.projectPatchQueueIntegrationDefeatedResult(ctx, projectID, item, defeater)
	}
	if repoGuard != "" && strings.TrimSpace(item.RepoID) != repoGuard {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_integrate repo_id guard %s does not match item repo_id %s", repoGuard, item.RepoID), IsError: true}
	}

	repo, ok := selectProjectBranchReviewRepo(coordination.Repositories, item.RepoID)
	if !ok {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_integrate requires item repo %s to be READY with remote_url", item.RepoID), IsError: true}
	}
	branch, hasSourceBranch := findProjectPatchQueueIntegrationBranch(coordination.Branches, item)
	sourceBranch := strings.TrimSpace(branch.BranchName)
	sourceHead := firstNonEmpty(strings.TrimSpace(item.HeadSHA), strings.TrimSpace(branch.HeadSHA))
	if sourceHead == "" {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_integrate requires head_sha evidence for item %s/%s", item.QueueID, item.ItemID), IsError: true}
	}

	targetBranch := firstNonEmpty(strings.TrimSpace(stringArg(args, "target_branch")), repo.IntegrationBranch, repo.DefaultBranch, coordination.Profile.RepoDefaultBranch, "main")
	if sourceBranch != "" && targetBranch == sourceBranch {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_integrate target_branch %s must differ from source branch", targetBranch), IsError: true}
	}
	integrationModeArg := strings.TrimSpace(stringArg(args, "integration_mode"))
	if integrationModeArg == "" && projectPatchQueueIntegrateRuntimeTaskAuthorizesDirectMerge(coordination.Tasks, t.currentRuntimeBinding(), projectID, item) {
		integrationModeArg = "direct_merge"
	}
	integrationMode, err := projectPatchQueueIntegrationModeForItem(item, integrationModeArg)
	if err != nil {
		return &ToolResult{Output: "project_patch_queue_integrate " + err.Error(), IsError: true}
	}
	if _, err := t.recordProjectPatchQueueIntegrationReceipt(ctx, projectPatchQueueIntegrationReceiptInput{
		WorkspaceID:       t.workspaceID,
		ProjectID:         projectID,
		ActorID:           t.agentID,
		QueueID:           item.QueueID,
		ItemID:            item.ItemID,
		RepoID:            item.RepoID,
		SourceBranchID:    item.BranchID,
		SourceHeadSHA:     sourceHead,
		TargetBranch:      targetBranch,
		Outcome:           "admitted",
		IntegrationMode:   integrationMode,
		AuthorityMode:     authorityMode,
		AlreadyIntegrated: false,
	}); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_integrate requires durable server integration admission before git mutation: %v", err), IsError: true}
	}
	var (
		localPath             string
		targetHeadBefore      string
		targetHeadAfter       string
		remoteTargetHeadAfter string
		targetUsedRemote      bool
		clonePerformed        bool
		reusedExisting        bool
		alreadyIntegrated     bool
		mergePerformed        bool
		pushAttempted         bool
		pushSucceeded         bool
		pushOutput            string
	)
	postAdmissionRepair := func(reason string) *ToolResult {
		return t.projectPatchQueueIntegrationRepairResult(ctx, projectPatchQueueIntegrateOutput{
			ProjectID:             projectID,
			RepoID:                repo.RepoID,
			QueueID:               item.QueueID,
			ItemID:                item.ItemID,
			SourceBranchID:        strings.TrimSpace(item.BranchID),
			SourceBranchName:      sourceBranch,
			SourceHeadSHA:         sourceHead,
			TargetBranchName:      targetBranch,
			TargetHeadBefore:      targetHeadBefore,
			TargetHeadAfter:       targetHeadAfter,
			RemoteTargetHeadAfter: remoteTargetHeadAfter,
			TargetUsedRemote:      targetUsedRemote,
			LocalPath:             localPath,
			ClonePerformed:        clonePerformed,
			ReusedExisting:        reusedExisting,
			AlreadyIntegrated:     alreadyIntegrated,
			MergePerformed:        mergePerformed,
			PushAttempted:         pushAttempted,
			PushSucceeded:         pushSucceeded,
			PushOutput:            pushOutput,
			AuthorityMode:         authorityMode,
			IntegrationMode:       integrationMode,
		}, projectPatchQueueIntegrationReceiptInput{
			WorkspaceID:           t.workspaceID,
			ProjectID:             projectID,
			ActorID:               t.agentID,
			QueueID:               item.QueueID,
			ItemID:                item.ItemID,
			RepoID:                item.RepoID,
			SourceBranchID:        item.BranchID,
			SourceHeadSHA:         sourceHead,
			TargetBranch:          targetBranch,
			TargetHeadBefore:      targetHeadBefore,
			TargetHeadAfter:       targetHeadAfter,
			RemoteTargetHeadAfter: remoteTargetHeadAfter,
			Outcome:               "repair",
			IntegrationMode:       integrationMode,
			AuthorityMode:         authorityMode,
			MergePerformed:        mergePerformed,
			PushAttempted:         pushAttempted,
			PushSucceeded:         pushSucceeded,
			AlreadyIntegrated:     alreadyIntegrated,
			RepairReason:          reason,
		})
	}
	localPath = firstNonEmpty(strings.TrimSpace(stringArg(args, "local_path")), projectPatchQueueIntegrateDefaultPath(t.workdir, projectID, repo))
	if err := ensureProjectCheckoutMaterializePath(t.workdir, localPath); err != nil {
		return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate invalid local_path after integration admission: %v", err))
	}
	remoteName := firstNonEmpty(strings.TrimSpace(stringArg(args, "remote_name")), "origin")

	baseBranch := firstNonEmpty(repo.DefaultBranch, coordination.Profile.RepoDefaultBranch, targetBranch, "main")
	clonePerformed, reusedExisting, err = materializeGitCheckout(ctx, localPath, repo, baseBranch, false)
	if err != nil {
		return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate git checkout failed after integration admission: %v%s", err, projectPatchQueueIntegrateRecoveryGuidance(ctx, repo, sourceBranch, sourceHead)))
	}
	if out, err := gitCombined(ctx, localPath, "fetch", remoteName); err != nil {
		return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate git fetch failed after integration admission: %v\n%s", err, out))
	}
	targetHeadBefore, targetUsedRemote, err = checkoutProjectPatchQueueIntegrationTarget(ctx, localPath, targetBranch, baseBranch)
	if err != nil {
		return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate target checkout failed after integration admission: %v", err))
	}
	if dirty, err := gitWorktreeDirty(ctx, localPath); err != nil {
		return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate failed checking pre-merge cleanliness after integration admission: %v", err))
	} else if dirty {
		return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate requires clean integration checkout before merge after integration admission: %s", localPath))
	}

	alreadyIntegrated, err = gitMergeBaseIsAncestor(ctx, localPath, sourceHead, "HEAD")
	if err != nil {
		return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate failed checking ancestry for %s after integration admission: %v%s", sourceHead, err, projectPatchQueueIntegrateRecoveryGuidance(ctx, repo, sourceBranch, sourceHead)))
	}
	if !alreadyIntegrated {
		if !hasSourceBranch {
			return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate requires source branch evidence for branch_id %s after integration admission; head %s is not present in canonical target %s", strings.TrimSpace(item.BranchID), sourceHead, targetBranch))
		}
		if sourceBranch == "" {
			return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate requires branch_name evidence for branch_id %s after integration admission; head %s is not present in canonical target %s", branch.BranchID, sourceHead, targetBranch))
		}
		switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
		case "ABANDONED", "ARCHIVED":
			return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate cannot integrate branch %s with status %s after integration admission", branch.BranchID, branch.Status))
		}
		if err := validateProjectPatchQueueIntegrationSource(ctx, localPath, sourceBranch, sourceHead); err != nil {
			return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate source verification failed after integration admission: %v%s", err, projectPatchQueueIntegrateRecoveryGuidance(ctx, repo, sourceBranch, sourceHead)))
		}
		message := projectPatchQueueIntegrateMergeMessage(args, projectID, item, branch, targetBranch)
		if out, err := projectPatchQueueGitMerge(ctx, localPath, t.agentID, sourceHead, message); err != nil {
			_, _ = gitCombined(ctx, localPath, "merge", "--abort")
			return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate git merge failed for %s into %s after integration admission: %v\n%s", sourceBranch, targetBranch, err, out))
		}
		mergePerformed = true
	}
	targetHeadAfter, err = gitRevParse(ctx, localPath, "HEAD")
	if err != nil || strings.TrimSpace(targetHeadAfter) == "" {
		return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate merged but could not read target HEAD after integration admission: %v", err))
	}
	if dirty, err := gitWorktreeDirty(ctx, localPath); err != nil {
		return postAdmissionRepair(fmt.Sprintf("project_patch_queue_integrate failed checking post-merge cleanliness after integration admission: %v", err))
	} else if dirty {
		return postAdmissionRepair("project_patch_queue_integrate left the integration checkout dirty after merge after integration admission; resolve/commit or abort before retrying")
	}

	pushAttempted = optionalBoolArgValue(args, "push", true) && mergePerformed
	if pushAttempted {
		out, err := gitCombined(ctx, localPath, "push", remoteName, targetBranch)
		pushOutput = strings.TrimSpace(out)
		if err != nil {
			return t.projectPatchQueueIntegrationRepairResult(ctx, projectPatchQueueIntegrateOutput{
				ProjectID:         projectID,
				RepoID:            repo.RepoID,
				QueueID:           item.QueueID,
				ItemID:            item.ItemID,
				SourceBranchID:    strings.TrimSpace(item.BranchID),
				SourceBranchName:  sourceBranch,
				SourceHeadSHA:     sourceHead,
				TargetBranchName:  targetBranch,
				TargetHeadBefore:  targetHeadBefore,
				TargetHeadAfter:   targetHeadAfter,
				TargetUsedRemote:  targetUsedRemote,
				LocalPath:         localPath,
				ClonePerformed:    clonePerformed,
				ReusedExisting:    reusedExisting,
				AlreadyIntegrated: alreadyIntegrated,
				MergePerformed:    mergePerformed,
				PushAttempted:     pushAttempted,
				PushSucceeded:     false,
				PushOutput:        pushOutput,
				AuthorityMode:     authorityMode,
				IntegrationMode:   integrationMode,
			}, projectPatchQueueIntegrationReceiptInput{
				WorkspaceID:       t.workspaceID,
				ProjectID:         projectID,
				ActorID:           t.agentID,
				QueueID:           item.QueueID,
				ItemID:            item.ItemID,
				RepoID:            item.RepoID,
				SourceBranchID:    item.BranchID,
				SourceHeadSHA:     sourceHead,
				TargetBranch:      targetBranch,
				TargetHeadBefore:  targetHeadBefore,
				TargetHeadAfter:   targetHeadAfter,
				Outcome:           "repair",
				IntegrationMode:   integrationMode,
				AuthorityMode:     authorityMode,
				MergePerformed:    mergePerformed,
				PushAttempted:     pushAttempted,
				PushSucceeded:     false,
				AlreadyIntegrated: alreadyIntegrated,
				RepairReason:      fmt.Sprintf("git push failed after local merge: %v %s", err, pushOutput),
			})
		}
		pushSucceeded = true
	}
	remoteTargetHeadAfter, err = verifyProjectPatchQueueCanonicalRemoteTarget(ctx, localPath, remoteName, targetBranch, targetHeadAfter)
	if err != nil {
		return t.projectPatchQueueIntegrationRepairResult(ctx, projectPatchQueueIntegrateOutput{
			ProjectID:             projectID,
			RepoID:                repo.RepoID,
			QueueID:               item.QueueID,
			ItemID:                item.ItemID,
			SourceBranchID:        strings.TrimSpace(item.BranchID),
			SourceBranchName:      sourceBranch,
			SourceHeadSHA:         sourceHead,
			TargetBranchName:      targetBranch,
			TargetHeadBefore:      targetHeadBefore,
			TargetHeadAfter:       targetHeadAfter,
			RemoteTargetHeadAfter: remoteTargetHeadAfter,
			TargetUsedRemote:      targetUsedRemote,
			LocalPath:             localPath,
			ClonePerformed:        clonePerformed,
			ReusedExisting:        reusedExisting,
			AlreadyIntegrated:     alreadyIntegrated,
			MergePerformed:        mergePerformed,
			PushAttempted:         pushAttempted,
			PushSucceeded:         pushSucceeded,
			PushOutput:            pushOutput,
			AuthorityMode:         authorityMode,
			IntegrationMode:       integrationMode,
		}, projectPatchQueueIntegrationReceiptInput{
			WorkspaceID:           t.workspaceID,
			ProjectID:             projectID,
			ActorID:               t.agentID,
			QueueID:               item.QueueID,
			ItemID:                item.ItemID,
			RepoID:                item.RepoID,
			SourceBranchID:        item.BranchID,
			SourceHeadSHA:         sourceHead,
			TargetBranch:          targetBranch,
			TargetHeadBefore:      targetHeadBefore,
			TargetHeadAfter:       targetHeadAfter,
			RemoteTargetHeadAfter: remoteTargetHeadAfter,
			Outcome:               "repair",
			IntegrationMode:       integrationMode,
			AuthorityMode:         authorityMode,
			MergePerformed:        mergePerformed,
			PushAttempted:         pushAttempted,
			PushSucceeded:         pushSucceeded,
			AlreadyIntegrated:     alreadyIntegrated,
			RepairReason:          fmt.Sprintf("canonical remote target verification failed after integration attempt: %v", err),
		})
	}

	checkout, integrationBranch, err := t.registerProjectPatchQueueIntegrationTargetEvidence(ctx, coordination, projectID, repo, localPath, targetBranch, targetHeadBefore, targetHeadAfter)
	if err != nil {
		return t.projectPatchQueueIntegrationRepairResult(ctx, projectPatchQueueIntegrateOutput{
			ProjectID:             projectID,
			RepoID:                repo.RepoID,
			QueueID:               item.QueueID,
			ItemID:                item.ItemID,
			SourceBranchID:        strings.TrimSpace(item.BranchID),
			SourceBranchName:      sourceBranch,
			SourceHeadSHA:         sourceHead,
			TargetBranchName:      targetBranch,
			TargetHeadBefore:      targetHeadBefore,
			TargetHeadAfter:       targetHeadAfter,
			RemoteTargetHeadAfter: remoteTargetHeadAfter,
			TargetUsedRemote:      targetUsedRemote,
			LocalPath:             localPath,
			ClonePerformed:        clonePerformed,
			ReusedExisting:        reusedExisting,
			AlreadyIntegrated:     alreadyIntegrated,
			MergePerformed:        mergePerformed,
			PushAttempted:         pushAttempted,
			PushSucceeded:         pushSucceeded,
			PushOutput:            pushOutput,
			AuthorityMode:         authorityMode,
			IntegrationMode:       integrationMode,
		}, projectPatchQueueIntegrationReceiptInput{
			WorkspaceID:       t.workspaceID,
			ProjectID:         projectID,
			ActorID:           t.agentID,
			QueueID:           item.QueueID,
			ItemID:            item.ItemID,
			RepoID:            item.RepoID,
			SourceBranchID:    item.BranchID,
			SourceHeadSHA:     sourceHead,
			TargetBranch:      targetBranch,
			TargetHeadBefore:  targetHeadBefore,
			TargetHeadAfter:   targetHeadAfter,
			Outcome:           "repair",
			IntegrationMode:   integrationMode,
			AuthorityMode:     authorityMode,
			MergePerformed:    mergePerformed,
			PushAttempted:     pushAttempted,
			PushSucceeded:     pushSucceeded,
			AlreadyIntegrated: alreadyIntegrated,
			RepairReason:      fmt.Sprintf("failed registering integration target evidence after git mutation: %v", err),
		})
	}
	sourceBranchStatus := strings.TrimSpace(branch.Status)
	sourceBranchEvidenceMissing := !hasSourceBranch || strings.TrimSpace(branch.BranchName) == ""
	if sourceBranchEvidenceMissing {
		sourceBranchStatus = firstNonEmpty(sourceBranchStatus, "MISSING_SOURCE_BRANCH_EVIDENCE")
		if _, err := t.recordProjectPatchQueueIntegrationReceipt(ctx, projectPatchQueueIntegrationReceiptInput{
			WorkspaceID:           t.workspaceID,
			ProjectID:             projectID,
			ActorID:               t.agentID,
			QueueID:               item.QueueID,
			ItemID:                item.ItemID,
			RepoID:                item.RepoID,
			SourceBranchID:        item.BranchID,
			SourceHeadSHA:         sourceHead,
			TargetBranch:          targetBranch,
			TargetHeadBefore:      targetHeadBefore,
			TargetHeadAfter:       targetHeadAfter,
			RemoteTargetHeadAfter: remoteTargetHeadAfter,
			Outcome:               "integrated",
			IntegrationMode:       integrationMode,
			AuthorityMode:         authorityMode,
			MergePerformed:        mergePerformed,
			PushAttempted:         pushAttempted,
			PushSucceeded:         pushSucceeded,
			AlreadyIntegrated:     alreadyIntegrated,
		}); err != nil {
			return t.projectPatchQueueIntegrationRepairResult(ctx, projectPatchQueueIntegrateOutput{
				ProjectID:                   projectID,
				RepoID:                      repo.RepoID,
				QueueID:                     item.QueueID,
				ItemID:                      item.ItemID,
				SourceBranchID:              strings.TrimSpace(item.BranchID),
				SourceBranchName:            sourceBranch,
				SourceHeadSHA:               sourceHead,
				SourceBranchStatus:          sourceBranchStatus,
				SourceBranchEvidenceMissing: true,
				TargetBranchID:              integrationBranch.BranchID,
				TargetBranchName:            targetBranch,
				TargetBranchStatus:          integrationBranch.Status,
				TargetHeadBefore:            targetHeadBefore,
				TargetHeadAfter:             targetHeadAfter,
				RemoteTargetHeadAfter:       remoteTargetHeadAfter,
				TargetUsedRemote:            targetUsedRemote,
				LocalPath:                   localPath,
				CheckoutID:                  checkout.CheckoutID,
				ClonePerformed:              clonePerformed,
				ReusedExisting:              reusedExisting,
				AlreadyIntegrated:           alreadyIntegrated,
				MergePerformed:              mergePerformed,
				PushAttempted:               pushAttempted,
				PushSucceeded:               pushSucceeded,
				PushOutput:                  pushOutput,
				AuthorityMode:               authorityMode,
				IntegrationMode:             integrationMode,
			}, projectPatchQueueIntegrationReceiptInput{
				WorkspaceID:       t.workspaceID,
				ProjectID:         projectID,
				ActorID:           t.agentID,
				QueueID:           item.QueueID,
				ItemID:            item.ItemID,
				RepoID:            item.RepoID,
				SourceBranchID:    item.BranchID,
				SourceHeadSHA:     sourceHead,
				TargetBranch:      targetBranch,
				TargetHeadBefore:  targetHeadBefore,
				TargetHeadAfter:   targetHeadAfter,
				Outcome:           "repair",
				IntegrationMode:   integrationMode,
				AuthorityMode:     authorityMode,
				MergePerformed:    mergePerformed,
				PushAttempted:     pushAttempted,
				PushSucceeded:     pushSucceeded,
				AlreadyIntegrated: alreadyIntegrated,
				RepairReason:      fmt.Sprintf("failed recording durable integrated receipt after target evidence registration: %v", err),
			})
		}
		return projectPatchQueueIntegrateResult(projectPatchQueueIntegrateOutput{
			ProjectID:                   projectID,
			RepoID:                      repo.RepoID,
			QueueID:                     item.QueueID,
			ItemID:                      item.ItemID,
			SourceBranchID:              strings.TrimSpace(item.BranchID),
			SourceBranchName:            sourceBranch,
			SourceHeadSHA:               sourceHead,
			SourceBranchStatus:          sourceBranchStatus,
			SourceBranchEvidenceMissing: true,
			TargetBranchID:              integrationBranch.BranchID,
			TargetBranchName:            targetBranch,
			TargetBranchStatus:          integrationBranch.Status,
			TargetHeadBefore:            targetHeadBefore,
			TargetHeadAfter:             targetHeadAfter,
			RemoteTargetHeadAfter:       remoteTargetHeadAfter,
			TargetUsedRemote:            targetUsedRemote,
			LocalPath:                   localPath,
			CheckoutID:                  checkout.CheckoutID,
			ClonePerformed:              clonePerformed,
			ReusedExisting:              reusedExisting,
			AlreadyIntegrated:           alreadyIntegrated,
			MergePerformed:              mergePerformed,
			PushAttempted:               pushAttempted,
			PushSucceeded:               pushSucceeded,
			PushOutput:                  pushOutput,
			IntegrationRecorded:         true,
			AuthorityMode:               authorityMode,
			IntegrationMode:             integrationMode,
		})
	}
	if _, err := t.recordProjectPatchQueueIntegrationReceipt(ctx, projectPatchQueueIntegrationReceiptInput{
		WorkspaceID:           t.workspaceID,
		ProjectID:             projectID,
		ActorID:               t.agentID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		RepoID:                item.RepoID,
		SourceBranchID:        item.BranchID,
		SourceHeadSHA:         sourceHead,
		TargetBranch:          targetBranch,
		TargetHeadBefore:      targetHeadBefore,
		TargetHeadAfter:       targetHeadAfter,
		RemoteTargetHeadAfter: remoteTargetHeadAfter,
		Outcome:               "integrated",
		IntegrationMode:       integrationMode,
		AuthorityMode:         authorityMode,
		MergePerformed:        mergePerformed,
		PushAttempted:         pushAttempted,
		PushSucceeded:         pushSucceeded,
		AlreadyIntegrated:     alreadyIntegrated,
	}); err != nil {
		return t.projectPatchQueueIntegrationRepairResult(ctx, projectPatchQueueIntegrateOutput{
			ProjectID:             projectID,
			RepoID:                repo.RepoID,
			QueueID:               item.QueueID,
			ItemID:                item.ItemID,
			SourceBranchID:        branch.BranchID,
			SourceBranchName:      sourceBranch,
			SourceHeadSHA:         sourceHead,
			SourceBranchStatus:    sourceBranchStatus,
			TargetBranchID:        integrationBranch.BranchID,
			TargetBranchName:      targetBranch,
			TargetBranchStatus:    integrationBranch.Status,
			TargetHeadBefore:      targetHeadBefore,
			TargetHeadAfter:       targetHeadAfter,
			RemoteTargetHeadAfter: remoteTargetHeadAfter,
			TargetUsedRemote:      targetUsedRemote,
			LocalPath:             localPath,
			CheckoutID:            checkout.CheckoutID,
			ClonePerformed:        clonePerformed,
			ReusedExisting:        reusedExisting,
			AlreadyIntegrated:     alreadyIntegrated,
			MergePerformed:        mergePerformed,
			PushAttempted:         pushAttempted,
			PushSucceeded:         pushSucceeded,
			PushOutput:            pushOutput,
			AuthorityMode:         authorityMode,
			IntegrationMode:       integrationMode,
		}, projectPatchQueueIntegrationReceiptInput{
			WorkspaceID:       t.workspaceID,
			ProjectID:         projectID,
			ActorID:           t.agentID,
			QueueID:           item.QueueID,
			ItemID:            item.ItemID,
			RepoID:            item.RepoID,
			SourceBranchID:    item.BranchID,
			SourceHeadSHA:     sourceHead,
			TargetBranch:      targetBranch,
			TargetHeadBefore:  targetHeadBefore,
			TargetHeadAfter:   targetHeadAfter,
			Outcome:           "repair",
			IntegrationMode:   integrationMode,
			AuthorityMode:     authorityMode,
			MergePerformed:    mergePerformed,
			PushAttempted:     pushAttempted,
			PushSucceeded:     pushSucceeded,
			AlreadyIntegrated: alreadyIntegrated,
			RepairReason:      fmt.Sprintf("failed recording durable integrated receipt before source branch close: %v", err),
		})
	}
	sourceBranchRecord, err := t.client.RegisterProjectBranch(ctx, ProjectBranchRegisterInput{
		WorkspaceID:    t.workspaceID,
		ProjectID:      projectID,
		ActorID:        t.agentID,
		BranchID:       branch.BranchID,
		RepoID:         repo.RepoID,
		CheckoutID:     branch.CheckoutID,
		AgentID:        branch.AgentID,
		BranchName:     branch.BranchName,
		BranchKind:     firstNonEmpty(branch.BranchKind, "feature"),
		BaseBranch:     firstNonEmpty(branch.BaseBranch, item.BaseRef, baseBranch),
		HeadSHA:        sourceHead,
		BaseSHA:        firstNonEmpty(branch.BaseSHA, item.BaseSHA),
		WriteScopeJSON: branch.WriteScopeJSON,
		ReviewDocKey:   firstNonEmpty(branch.ReviewDocKey, item.ReviewDocKey),
		Status:         "MERGED",
	})
	if err != nil {
		return t.projectPatchQueueIntegrationRepairResult(ctx, projectPatchQueueIntegrateOutput{
			ProjectID:              projectID,
			RepoID:                 repo.RepoID,
			QueueID:                item.QueueID,
			ItemID:                 item.ItemID,
			SourceBranchID:         branch.BranchID,
			SourceBranchName:       sourceBranch,
			SourceHeadSHA:          sourceHead,
			SourceBranchStatus:     firstNonEmpty(sourceBranchStatus, "MERGED_MARK_FAILED"),
			SourceBranchCloseError: err.Error(),
			TargetBranchID:         integrationBranch.BranchID,
			TargetBranchName:       targetBranch,
			TargetBranchStatus:     integrationBranch.Status,
			TargetHeadBefore:       targetHeadBefore,
			TargetHeadAfter:        targetHeadAfter,
			RemoteTargetHeadAfter:  remoteTargetHeadAfter,
			TargetUsedRemote:       targetUsedRemote,
			LocalPath:              localPath,
			CheckoutID:             checkout.CheckoutID,
			ClonePerformed:         clonePerformed,
			ReusedExisting:         reusedExisting,
			AlreadyIntegrated:      alreadyIntegrated,
			MergePerformed:         mergePerformed,
			PushAttempted:          pushAttempted,
			PushSucceeded:          pushSucceeded,
			PushOutput:             pushOutput,
			IntegrationRecorded:    true,
			AuthorityMode:          authorityMode,
			IntegrationMode:        integrationMode,
		}, projectPatchQueueIntegrationReceiptInput{
			WorkspaceID:           t.workspaceID,
			ProjectID:             projectID,
			ActorID:               t.agentID,
			QueueID:               item.QueueID,
			ItemID:                item.ItemID,
			RepoID:                item.RepoID,
			SourceBranchID:        item.BranchID,
			SourceHeadSHA:         sourceHead,
			TargetBranch:          targetBranch,
			TargetHeadBefore:      targetHeadBefore,
			TargetHeadAfter:       targetHeadAfter,
			RemoteTargetHeadAfter: remoteTargetHeadAfter,
			IntegrationMode:       integrationMode,
			AuthorityMode:         authorityMode,
			MergePerformed:        mergePerformed,
			PushAttempted:         pushAttempted,
			PushSucceeded:         pushSucceeded,
			AlreadyIntegrated:     alreadyIntegrated,
			RepairReason:          "source_branch_close_failed_after_integrated_receipt: " + err.Error(),
		})
	}

	return projectPatchQueueIntegrateResult(projectPatchQueueIntegrateOutput{
		ProjectID:             projectID,
		RepoID:                repo.RepoID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		SourceBranchID:        branch.BranchID,
		SourceBranchName:      sourceBranch,
		SourceHeadSHA:         sourceHead,
		SourceBranchStatus:    sourceBranchRecord.Status,
		TargetBranchID:        integrationBranch.BranchID,
		TargetBranchName:      targetBranch,
		TargetBranchStatus:    integrationBranch.Status,
		TargetHeadBefore:      targetHeadBefore,
		TargetHeadAfter:       targetHeadAfter,
		RemoteTargetHeadAfter: remoteTargetHeadAfter,
		TargetUsedRemote:      targetUsedRemote,
		LocalPath:             localPath,
		CheckoutID:            checkout.CheckoutID,
		ClonePerformed:        clonePerformed,
		ReusedExisting:        reusedExisting,
		AlreadyIntegrated:     alreadyIntegrated,
		MergePerformed:        mergePerformed,
		PushAttempted:         pushAttempted,
		PushSucceeded:         pushSucceeded,
		PushOutput:            pushOutput,
		IntegrationRecorded:   true,
		AuthorityMode:         authorityMode,
		IntegrationMode:       integrationMode,
	})
}

type projectPatchQueueIntegrateOutput struct {
	ProjectID                   string
	RepoID                      string
	QueueID                     string
	ItemID                      string
	SourceBranchID              string
	SourceBranchName            string
	SourceHeadSHA               string
	SourceBranchStatus          string
	SourceBranchEvidenceMissing bool
	SourceBranchCloseError      string
	TargetBranchID              string
	TargetBranchName            string
	TargetBranchStatus          string
	TargetHeadBefore            string
	TargetHeadAfter             string
	RemoteTargetHeadAfter       string
	TargetUsedRemote            bool
	LocalPath                   string
	CheckoutID                  string
	ClonePerformed              bool
	ReusedExisting              bool
	AlreadyIntegrated           bool
	MergePerformed              bool
	PushAttempted               bool
	PushSucceeded               bool
	PushOutput                  string
	IntegrationRecorded         bool
	RepairReceiptRecorded       bool
	AuthorityMode               string
	IntegrationMode             string
}

func projectPatchQueueIntegrateResult(output projectPatchQueueIntegrateOutput) *ToolResult {
	payload := map[string]any{
		"project_id":                     output.ProjectID,
		"repo_id":                        output.RepoID,
		"queue_id":                       output.QueueID,
		"item_id":                        output.ItemID,
		"source_branch_id":               output.SourceBranchID,
		"source_branch_name":             output.SourceBranchName,
		"source_head_sha":                output.SourceHeadSHA,
		"source_branch_status":           output.SourceBranchStatus,
		"source_branch_evidence_missing": output.SourceBranchEvidenceMissing,
		"source_branch_close_error":      output.SourceBranchCloseError,
		"target_branch_id":               output.TargetBranchID,
		"target_branch_name":             output.TargetBranchName,
		"target_branch_status":           output.TargetBranchStatus,
		"target_head_before":             output.TargetHeadBefore,
		"target_head_after":              output.TargetHeadAfter,
		"remote_target_head_after":       output.RemoteTargetHeadAfter,
		"target_used_remote":             output.TargetUsedRemote,
		"local_path":                     output.LocalPath,
		"checkout_id":                    output.CheckoutID,
		"clone_performed":                output.ClonePerformed,
		"reused_existing":                output.ReusedExisting,
		"already_integrated":             output.AlreadyIntegrated,
		"merge_performed":                output.MergePerformed,
		"push_attempted":                 output.PushAttempted,
		"push_succeeded":                 output.PushSucceeded,
		"push_output":                    output.PushOutput,
		"integration_recorded":           output.IntegrationRecorded,
		"repair_receipt_recorded":        output.RepairReceiptRecorded,
		"authority_mode":                 firstNonEmpty(output.AuthorityMode, "project_role"),
		"integration_mode":               firstNonEmpty(output.IntegrationMode, "direct_merge"),
		"trust_first_authority_advisory": strings.EqualFold(output.AuthorityMode, "trust_first_advisory"),
		"integrated":                     output.IntegrationRecorded,
		"worktree_clean":                 true,
		"git_merge":                      output.MergePerformed,
		"git_push":                       output.PushSucceeded,
		"next_gate":                      "downstream_agents_should_materialize_or_fetch_the_canonical_target_branch_before_building_on_this_change",
		"integration_guidance":           "The accepted patch queue branch is now part of the canonical target branch. Downstream implementation, validation, and finalization lanes should start from target_head_after.",
		"no_private_integration":         true,
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_integrate completed %s at %s", output.TargetBranchName, output.TargetHeadAfter)}
	}
	return &ToolResult{Output: string(raw)}
}

type projectPatchQueueIntegrationReceiptInput struct {
	WorkspaceID           string `json:"workspace_id"`
	ProjectID             string `json:"project_id"`
	ActorID               string `json:"actor_id"`
	QueueID               string `json:"queue_id"`
	ItemID                string `json:"item_id"`
	RepoID                string `json:"repo_id,omitempty"`
	SourceBranchID        string `json:"source_branch_id,omitempty"`
	SourceHeadSHA         string `json:"source_head_sha,omitempty"`
	TargetBranch          string `json:"target_branch,omitempty"`
	TargetHeadBefore      string `json:"target_head_before,omitempty"`
	TargetHeadAfter       string `json:"target_head_after,omitempty"`
	RemoteTargetHeadAfter string `json:"remote_target_head_after,omitempty"`
	Outcome               string `json:"outcome"`
	IntegrationMode       string `json:"integration_mode"`
	AuthorityMode         string `json:"authority_mode,omitempty"`
	MergePerformed        bool   `json:"merge_performed,omitempty"`
	PushAttempted         bool   `json:"push_attempted,omitempty"`
	PushSucceeded         bool   `json:"push_succeeded,omitempty"`
	AlreadyIntegrated     bool   `json:"already_integrated,omitempty"`
	RepairReason          string `json:"repair_reason,omitempty"`
}

func (t *ProjectPatchQueueIntegrateTool) recordProjectPatchQueueIntegrationReceipt(ctx context.Context, input projectPatchQueueIntegrationReceiptInput) (ProjectPatchQueueItemRecord, error) {
	method := "project.patch_queue.integration_record"
	if strings.EqualFold(strings.TrimSpace(input.Outcome), "repair") {
		method = "project.patch_queue.integration_repair"
	}
	var resp struct {
		PatchQueueItem ProjectPatchQueueItemRecord `json:"patch_queue_item"`
	}
	err := t.client.call(ctx, method, input, &resp)
	return resp.PatchQueueItem, err
}

func projectPatchQueueIntegrationRepairAlreadyRecordedError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(text, "project.patch_queue.integration_repair") &&
		strings.Contains(text, "dedup_key") &&
		strings.Contains(text, "already exists")
}

func (t *ProjectPatchQueueIntegrateTool) projectPatchQueueIntegrationRepairResult(ctx context.Context, output projectPatchQueueIntegrateOutput, receipt projectPatchQueueIntegrationReceiptInput) *ToolResult {
	receipt.Outcome = "repair"
	if strings.TrimSpace(receipt.RepairReason) == "" {
		receipt.RepairReason = "canonical integration failed after mutation attempt"
	}
	recordedItem, err := t.recordProjectPatchQueueIntegrationReceipt(ctx, receipt)
	if err != nil {
		if projectPatchQueueIntegrationRepairAlreadyRecordedError(err) {
			output.RepairReceiptRecorded = true
			payload := map[string]any{
				"project_id":                                output.ProjectID,
				"repo_id":                                   output.RepoID,
				"queue_id":                                  output.QueueID,
				"item_id":                                   output.ItemID,
				"source_branch_id":                          output.SourceBranchID,
				"source_branch_name":                        output.SourceBranchName,
				"source_branch_close_error":                 output.SourceBranchCloseError,
				"source_head_sha":                           output.SourceHeadSHA,
				"target_branch_id":                          output.TargetBranchID,
				"target_branch_name":                        output.TargetBranchName,
				"target_branch_status":                      output.TargetBranchStatus,
				"target_head_before":                        output.TargetHeadBefore,
				"target_head_after":                         output.TargetHeadAfter,
				"remote_target_head_after":                  output.RemoteTargetHeadAfter,
				"local_path":                                output.LocalPath,
				"merge_performed":                           output.MergePerformed,
				"push_attempted":                            output.PushAttempted,
				"push_succeeded":                            output.PushSucceeded,
				"push_output":                               output.PushOutput,
				"already_integrated":                        output.AlreadyIntegrated,
				"integration_recorded":                      output.IntegrationRecorded,
				"repair_receipt_recorded":                   true,
				"repair_receipt_already_recorded":           true,
				"repair_recording_dedup_key_already_exists": true,
				"repair_receipt_recording_error":            err.Error(),
				"integrated":                                output.IntegrationRecorded,
				"authority_mode":                            firstNonEmpty(output.AuthorityMode, "project_role"),
				"integration_mode":                          firstNonEmpty(output.IntegrationMode, receipt.IntegrationMode, "direct_merge"),
				"repair_reason":                             receipt.RepairReason,
				"next_gate":                                 "inspect_project_patch_queue_integration_repair_receipt_before_retrying_canonical_mutation",
				"required_transition":                       "project_patch_queue_followup_revision_before_integration_retry",
				"no_additional_canonical_mutation_was_applied": true,
			}
			raw, marshalErr := json.MarshalIndent(payload, "", "  ")
			if marshalErr == nil {
				return &ToolResult{Output: string(raw), IsError: true}
			}
		}
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_integrate failed and could not record durable repair receipt: %v; original repair reason: %s", err, receipt.RepairReason), IsError: true}
	}
	if strings.EqualFold(strings.TrimSpace(recordedItem.State), "INTEGRATED") {
		output.IntegrationRecorded = true
		output.AlreadyIntegrated = true
		output.RepairReceiptRecorded = false
		return projectPatchQueueIntegrateResult(output)
	}
	output.RepairReceiptRecorded = true
	payload := map[string]any{
		"project_id":                output.ProjectID,
		"repo_id":                   output.RepoID,
		"queue_id":                  output.QueueID,
		"item_id":                   output.ItemID,
		"source_branch_id":          output.SourceBranchID,
		"source_branch_name":        output.SourceBranchName,
		"source_branch_close_error": output.SourceBranchCloseError,
		"source_head_sha":           output.SourceHeadSHA,
		"target_branch_id":          output.TargetBranchID,
		"target_branch_name":        output.TargetBranchName,
		"target_branch_status":      output.TargetBranchStatus,
		"target_head_before":        output.TargetHeadBefore,
		"target_head_after":         output.TargetHeadAfter,
		"remote_target_head_after":  output.RemoteTargetHeadAfter,
		"local_path":                output.LocalPath,
		"merge_performed":           output.MergePerformed,
		"push_attempted":            output.PushAttempted,
		"push_succeeded":            output.PushSucceeded,
		"push_output":               output.PushOutput,
		"already_integrated":        output.AlreadyIntegrated,
		"integration_recorded":      output.IntegrationRecorded,
		"repair_receipt_recorded":   true,
		"integrated":                output.IntegrationRecorded,
		"authority_mode":            firstNonEmpty(output.AuthorityMode, "project_role"),
		"integration_mode":          firstNonEmpty(output.IntegrationMode, receipt.IntegrationMode, "direct_merge"),
		"repair_reason":             receipt.RepairReason,
		"next_gate":                 "inspect_project_patch_queue_integration_repair_receipt_before_retrying_canonical_mutation",
	}
	raw, marshalErr := json.MarshalIndent(payload, "", "  ")
	if marshalErr != nil {
		return &ToolResult{Output: "project_patch_queue_integrate failed and recorded durable repair receipt: " + receipt.RepairReason, IsError: true}
	}
	return &ToolResult{Output: string(raw), IsError: true}
}

func (t *ProjectPatchQueueIntegrateTool) currentRuntimeBinding() AgentRuntimeBinding {
	if t == nil || t.runtimeBinding == nil {
		return AgentRuntimeBinding{}
	}
	return t.runtimeBinding()
}

func projectPatchQueueIntegrationModeForItem(item ProjectPatchQueueItemRecord, requested string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(requested))
	switch mode {
	case "", "materialization", "direct_merge":
	default:
		return "", fmt.Errorf("unsupported integration_mode %s", requested)
	}
	if strings.TrimSpace(item.RepoAuthorityMode) != "repoauthority_controlled_queue" {
		return firstNonEmpty(mode, "direct_merge"), nil
	}
	if projectPatchQueueIntegrationItemMaterialized(item) {
		return firstNonEmpty(mode, "materialization"), nil
	}
	if mode != "direct_merge" {
		return "", fmt.Errorf("controlled patch queue item %s/%s requires durable materialization before integration or explicit integration_mode=direct_merge", item.QueueID, item.ItemID)
	}
	return mode, nil
}

func projectPatchQueueIntegrateRuntimeTaskAuthorizesDirectMerge(tasks []WorkspaceTaskRecord, binding AgentRuntimeBinding, projectID string, item ProjectPatchQueueItemRecord) bool {
	if strings.TrimSpace(binding.TaskID) == "" {
		return false
	}
	if strings.TrimSpace(item.RepoAuthorityMode) != "repoauthority_controlled_queue" || projectPatchQueueIntegrationItemMaterialized(item) {
		return false
	}
	var task WorkspaceTaskRecord
	found := false
	for _, candidate := range tasks {
		if strings.TrimSpace(candidate.TaskID) == strings.TrimSpace(binding.TaskID) {
			task = candidate
			found = true
			break
		}
	}
	if !found {
		return false
	}
	requirements := map[string]any{}
	if raw := strings.TrimSpace(task.TaskRequirementsJSON); raw != "" && raw != "{}" {
		if err := json.Unmarshal([]byte(raw), &requirements); err != nil {
			return false
		}
	}
	if !projectPatchQueueIntegrateTaskRequiresIntegrationTool(task, requirements) {
		return false
	}
	if taskProjectID := strings.TrimSpace(task.ProjectID); taskProjectID != "" && taskProjectID != strings.TrimSpace(projectID) {
		return false
	}
	if reqProjectID := taskSubmitRequirementString(requirements, "project_id"); reqProjectID != "" && reqProjectID != strings.TrimSpace(projectID) {
		return false
	}
	return projectPatchQueueIntegrateTaskMatchesItem(requirements, item)
}

func projectPatchQueueIntegrateTaskRequiresIntegrationTool(task WorkspaceTaskRecord, requirements map[string]any) bool {
	if strings.EqualFold(taskSubmitRequirementString(requirements, "required_tool"), "project_patch_queue_integrate") {
		return true
	}
	if strings.EqualFold(taskSubmitRequirementString(requirements, "required_transition"), "project_patch_queue_integrate_then_full_product_verify") {
		return true
	}
	if strings.EqualFold(taskSubmitRequirementString(requirements, "patch_queue_task_kind"), "integration") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(task.ProjectLane), "integration") {
		return true
	}
	return false
}

func projectPatchQueueIntegrateTaskMatchesItem(requirements map[string]any, item ProjectPatchQueueItemRecord) bool {
	matched := false
	for key, value := range map[string]string{
		"queue_id":  item.QueueID,
		"item_id":   item.ItemID,
		"branch_id": item.BranchID,
		"head_sha":  item.HeadSHA,
	} {
		required := taskSubmitRequirementString(requirements, key)
		if required == "" {
			continue
		}
		matched = true
		if required != strings.TrimSpace(value) {
			return false
		}
	}
	return matched
}

func projectPatchQueueIntegrationItemMaterialized(item ProjectPatchQueueItemRecord) bool {
	return item.MaterializationAccepted &&
		strings.TrimSpace(item.MaterializationSchema) != "" &&
		strings.TrimSpace(item.MaterializationDigest) != ""
}

func projectPatchQueueIntegrateItemHasDefeatingAdvisory(item ProjectPatchQueueItemRecord) bool {
	if !item.ReviewerAdvisoryAccepted {
		return false
	}
	advisory := projectPatchQueueIntegrateReviewerAdvisory(item)
	if advisory.DefeatsAcceptance {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(advisory.HeadSHA), strings.TrimSpace(item.HeadSHA)) {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(advisory.Scope), "lane_correctness") &&
		(strings.EqualFold(strings.TrimSpace(advisory.Verdict), "repair_required") || projectPatchQueueIntegrateNegativeAdvisorySummary(advisory.Summary)) {
		return true
	}
	return false
}

func projectPatchQueueIntegrateReviewerAdvisory(item ProjectPatchQueueItemRecord) PatchQueueReviewerAdvisory {
	advisory := item.ReviewerAdvisory
	if projectPatchQueueIntegrateReviewerAdvisoryPresent(advisory) {
		return advisory
	}
	if raw := strings.TrimSpace(item.ReviewerAdvisoryJSON); raw != "" {
		_ = json.Unmarshal([]byte(raw), &advisory)
	}
	return advisory
}

func projectPatchQueueIntegrateReviewerAdvisoryPresent(advisory PatchQueueReviewerAdvisory) bool {
	return advisory.DefeatsAcceptance ||
		strings.TrimSpace(advisory.HeadSHA) != "" ||
		strings.TrimSpace(advisory.Scope) != "" ||
		strings.TrimSpace(advisory.Verdict) != "" ||
		strings.TrimSpace(advisory.Summary) != ""
}

func projectPatchQueueIntegrateDefeatingItem(items []ProjectPatchQueueItemRecord, accepted ProjectPatchQueueItemRecord) (ProjectPatchQueueItemRecord, bool) {
	if projectPatchQueueIntegrateItemHasDefeatingAdvisory(accepted) {
		return accepted, true
	}
	for _, candidate := range items {
		if projectPatchQueueIntegrateSameItem(candidate, accepted) {
			continue
		}
		if !projectPatchQueueIntegrateSameCandidateHead(candidate, accepted) {
			continue
		}
		if !projectPatchQueueIntegrateDefeatingState(candidate.State) {
			continue
		}
		if projectPatchQueueIntegrateItemHasDefeatingAdvisory(candidate) {
			return candidate, true
		}
	}
	return ProjectPatchQueueItemRecord{}, false
}

func projectPatchQueueIntegrateSameItem(left, right ProjectPatchQueueItemRecord) bool {
	return strings.TrimSpace(left.QueueID) == strings.TrimSpace(right.QueueID) &&
		strings.TrimSpace(left.ItemID) == strings.TrimSpace(right.ItemID)
}

func projectPatchQueueIntegrateSameCandidateHead(left, right ProjectPatchQueueItemRecord) bool {
	return strings.TrimSpace(left.RepoID) != "" &&
		strings.TrimSpace(left.RepoID) == strings.TrimSpace(right.RepoID) &&
		strings.TrimSpace(left.BranchID) != "" &&
		strings.TrimSpace(left.BranchID) == strings.TrimSpace(right.BranchID) &&
		strings.TrimSpace(left.HeadSHA) != "" &&
		strings.EqualFold(strings.TrimSpace(left.HeadSHA), strings.TrimSpace(right.HeadSHA))
}

func projectPatchQueueIntegrateDefeatingState(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "BLOCKED", "REJECTED":
		return true
	default:
		return false
	}
}

func (t *ProjectPatchQueueIntegrateTool) projectPatchQueueIntegrationDefeatedResult(ctx context.Context, projectID string, accepted, defeater ProjectPatchQueueItemRecord) *ToolResult {
	followupPayload := any(nil)
	if projectPatchQueueIntegrateDefeatingState(defeater.State) {
		followup := NewProjectPatchQueueFollowupTool(t.client, t.workspaceID, t.agentID, t.ownerUserID).Execute(ctx, map[string]any{
			"project_id":    projectID,
			"queue_id":      defeater.QueueID,
			"item_id":       defeater.ItemID,
			"followup_kind": "revision",
			"extra_context": fmt.Sprintf("pre_integration_defeater_for_accepted_item: %s/%s\naccepted_branch_id: %s\naccepted_head_sha: %s", strings.TrimSpace(accepted.QueueID), strings.TrimSpace(accepted.ItemID), strings.TrimSpace(accepted.BranchID), strings.TrimSpace(accepted.HeadSHA)),
		})
		if followup != nil {
			followupPayload = strings.TrimSpace(followup.Output)
			var parsed any
			if err := json.Unmarshal([]byte(strings.TrimSpace(followup.Output)), &parsed); err == nil {
				followupPayload = parsed
			}
		}
	}
	payload := map[string]any{
		"project_id":                    projectID,
		"repo_id":                       accepted.RepoID,
		"queue_id":                      accepted.QueueID,
		"item_id":                       accepted.ItemID,
		"branch_id":                     accepted.BranchID,
		"head_sha":                      accepted.HeadSHA,
		"integrated":                    false,
		"git_merge":                     false,
		"git_push":                      false,
		"pre_integration_gate":          "defeasible_acceptance",
		"defeating_queue_id":            defeater.QueueID,
		"defeating_item_id":             defeater.ItemID,
		"defeating_state":               defeater.State,
		"defeating_branch_id":           defeater.BranchID,
		"defeating_head_sha":            defeater.HeadSHA,
		"reviewer_advisory_accepted":    defeater.ReviewerAdvisoryAccepted,
		"reviewer_advisory_digest":      defeater.ReviewerAdvisoryDigest,
		"reviewer_advisory_review_doc":  firstNonEmpty(projectPatchQueueIntegrateReviewerAdvisory(defeater).ReviewDocKey, defeater.ReviewDocKey),
		"no_canonical_mutation":         true,
		"followup_result":               followupPayload,
		"required_transition":           "project_patch_queue_followup_revision_before_integration_retry",
		"integration_refusal_reason":    "accepted item has unresolved same-head reviewer defect evidence",
		"integration_refusal_guidance":  "Repair or supersede the defeating same-head candidate, publish a fresh head and review-ready packet, then submit a new accepted patch queue item before retrying integration.",
		"defeasible_acceptance_applied": true,
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_integrate refused %s/%s before canonical mutation: accepted head %s has unresolved same-head reviewer defect evidence from %s/%s", accepted.QueueID, accepted.ItemID, accepted.HeadSHA, defeater.QueueID, defeater.ItemID), IsError: true}
	}
	return &ToolResult{Output: string(raw), IsError: true}
}

func projectPatchQueueIntegrateNegativeAdvisorySummary(summary string) bool {
	text := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(summary)), " "))
	for _, marker := range []string{"defect", "bug", "broken", "incorrect", "invalid", "reject", "not accepted-ready", "repair required", "repair needed", "blocking"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func selectProjectPatchQueueIntegrationItem(items []ProjectPatchQueueItemRecord, queueID, itemID, branchID, repoID string) (ProjectPatchQueueItemRecord, error) {
	queueID = strings.TrimSpace(queueID)
	itemID = strings.TrimSpace(itemID)
	branchID = strings.TrimSpace(branchID)
	repoID = strings.TrimSpace(repoID)
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
		if repoID != "" && strings.TrimSpace(item.RepoID) != repoID {
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
		var accepted []ProjectPatchQueueItemRecord
		for _, match := range matches {
			if strings.EqualFold(strings.TrimSpace(match.State), "ACCEPTED") {
				accepted = append(accepted, match)
			}
		}
		if len(accepted) == 1 {
			return accepted[0], nil
		}
		return ProjectPatchQueueItemRecord{}, projectPatchQueueSelectorAmbiguousError(matches)
	}
}

func selectProjectPatchQueueIntegrationBranch(branches []ProjectBranchRecord, item ProjectPatchQueueItemRecord) (ProjectBranchRecord, error) {
	branch, ok := findProjectPatchQueueIntegrationBranch(branches, item)
	if !ok {
		return ProjectBranchRecord{}, fmt.Errorf("project_patch_queue_integrate requires source branch evidence for branch_id %s", strings.TrimSpace(item.BranchID))
	}
	if strings.TrimSpace(branch.BranchName) == "" {
		return ProjectBranchRecord{}, fmt.Errorf("project_patch_queue_integrate branch %s has no branch_name evidence", strings.TrimSpace(item.BranchID))
	}
	switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
	case "ABANDONED", "ARCHIVED":
		return ProjectBranchRecord{}, fmt.Errorf("project_patch_queue_integrate cannot integrate branch %s with status %s", strings.TrimSpace(item.BranchID), branch.Status)
	default:
		return branch, nil
	}
}

func findProjectPatchQueueIntegrationBranch(branches []ProjectBranchRecord, item ProjectPatchQueueItemRecord) (ProjectBranchRecord, bool) {
	branchID := strings.TrimSpace(item.BranchID)
	repoID := strings.TrimSpace(item.RepoID)
	if branchID == "" {
		return ProjectBranchRecord{}, false
	}
	for _, branch := range branches {
		if strings.TrimSpace(branch.BranchID) != branchID {
			continue
		}
		if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			return ProjectBranchRecord{}, false
		}
		return branch, true
	}
	return ProjectBranchRecord{}, false
}

func (t *ProjectPatchQueueIntegrateTool) registerProjectPatchQueueIntegrationTargetEvidence(ctx context.Context, coordination ProjectCoordinationRecord, projectID string, repo ProjectRepositoryRecord, localPath, targetBranch, targetHeadBefore, targetHeadAfter string) (ProjectCheckoutRecord, ProjectBranchRecord, error) {
	machineID := runtimeMachineID()
	checkoutID := projectPatchQueueIntegrationCheckoutID(coordination.Checkouts, repo.RepoID, t.agentID, machineID, localPath, targetBranch)
	checkout, err := t.client.RegisterProjectCheckout(ctx, ProjectCheckoutRegisterInput{
		WorkspaceID:  t.workspaceID,
		ProjectID:    projectID,
		ActorID:      t.agentID,
		CheckoutID:   checkoutID,
		RepoID:       repo.RepoID,
		MachineID:    machineID,
		MachineLabel: machineID,
		OwnerUserID:  t.ownerUserID,
		AgentID:      t.agentID,
		LocalPath:    localPath,
		CheckoutKind: "integration",
		BranchName:   targetBranch,
		BaseBranch:   targetBranch,
		HeadSHA:      targetHeadAfter,
		BaseSHA:      targetHeadBefore,
		DirtyState:   "clean",
		Status:       "ACTIVE",
	})
	if err != nil {
		return ProjectCheckoutRecord{}, ProjectBranchRecord{}, fmt.Errorf("register integration checkout: %w", err)
	}
	integrationBranch, err := t.client.RegisterProjectBranch(ctx, ProjectBranchRegisterInput{
		WorkspaceID:    t.workspaceID,
		ProjectID:      projectID,
		ActorID:        t.agentID,
		BranchID:       projectPatchQueueIntegrationBranchID(coordination.Branches, repo.RepoID, t.agentID, targetBranch),
		RepoID:         repo.RepoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        t.agentID,
		BranchName:     targetBranch,
		BranchKind:     "integration",
		BaseBranch:     targetBranch,
		HeadSHA:        targetHeadAfter,
		BaseSHA:        targetHeadBefore,
		WriteScopeJSON: `{"paths":["**"]}`,
		Status:         "MERGED",
	})
	if err != nil {
		return ProjectCheckoutRecord{}, ProjectBranchRecord{}, fmt.Errorf("register integration branch: %w", err)
	}
	return checkout, integrationBranch, nil
}

func projectPatchQueueIntegrateRecoveryGuidance(ctx context.Context, repo ProjectRepositoryRecord, sourceBranch, sourceHead string) string {
	localPath, isLocal := projectRepoMaterializeLocalRemotePath(repo)
	if isLocal && !projectRepoMaterializeBareRepoReady(ctx, localPath) {
		return fmt.Sprintf("\nrecovery_guidance: canonical READY local remote %q is missing or is not a valid bare git repository. Call project_repo_materialize to recreate repository infrastructure, then regenerate and republish the accepted source branch %q at head %s before retrying integration. Prior branch refs/objects are not recovered from Rhizome server evidence alone. Verify commit availability with `git cat-file -e <head>^{commit}`, not `git rev-parse --verify` alone.", strings.TrimSpace(repo.RemoteURL), strings.TrimSpace(sourceBranch), strings.TrimSpace(sourceHead))
	}
	if strings.TrimSpace(sourceHead) != "" {
		return fmt.Sprintf("\nrecovery_guidance: accepted source head %s is not available in this checkout. Fetch/materialize the source branch %q if it still exists; verify commit availability with `git cat-file -e %s^{commit}`, not `git rev-parse --verify` alone. If fresh repo/checkouts/refs prove the source is unavailable, call project_patch_queue_followup with followup_kind=rebuild and include the negative evidence so agents build a fresh equivalent review-ready candidate instead of retrying this dead head.", strings.TrimSpace(sourceHead), strings.TrimSpace(sourceBranch), strings.TrimSpace(sourceHead))
	}
	return "\nrecovery_guidance: accepted source branch materialization failed. Re-establish repository/branch evidence before retrying integration; if the source cannot be recovered after concrete git checks, create a project_patch_queue_followup with followup_kind=rebuild."
}

func projectPatchQueueIntegrateDefaultPath(workdir, projectID string, repo ProjectRepositoryRecord) string {
	projectHash := shortRefHash(firstNonEmpty(projectID, "project"))
	repoHash := shortRefHash(firstNonEmpty(repo.RepoID, repo.RemoteURL, repo.Name, "repo"))
	return filepath.Join(workdir, "project-integration", "p-"+projectHash+"-r-"+repoHash)
}

func checkoutProjectPatchQueueIntegrationTarget(ctx context.Context, localPath, targetBranch, baseBranch string) (string, bool, error) {
	targetBranch = strings.TrimSpace(targetBranch)
	baseBranch = strings.TrimSpace(baseBranch)
	if targetBranch == "" {
		return "", false, fmt.Errorf("target_branch is required")
	}
	remoteTarget := "refs/remotes/origin/" + targetBranch
	if _, ok := gitRefSHA(ctx, localPath, remoteTarget); ok {
		if out, err := gitCombined(ctx, localPath, "checkout", "-B", targetBranch, remoteTarget); err != nil {
			return "", true, fmt.Errorf("git checkout target branch %s from origin failed: %w\n%s", targetBranch, err, out)
		}
		head, err := gitRevParse(ctx, localPath, "HEAD")
		return head, true, err
	}
	localTarget := "refs/heads/" + targetBranch
	if gitRefExists(ctx, localPath, localTarget) {
		if out, err := gitCombined(ctx, localPath, "checkout", targetBranch); err != nil {
			return "", false, fmt.Errorf("git checkout local target branch %s failed: %w\n%s", targetBranch, err, out)
		}
		head, err := gitRevParse(ctx, localPath, "HEAD")
		return head, false, err
	}
	baseRef := firstNonEmpty(baseBranch, "main")
	if baseBranch != "" {
		if _, ok := gitRefSHA(ctx, localPath, "refs/remotes/origin/"+baseBranch); ok {
			baseRef = "refs/remotes/origin/" + baseBranch
		}
	}
	if out, err := gitCombined(ctx, localPath, "checkout", "-B", targetBranch, baseRef); err != nil {
		return "", false, fmt.Errorf("git create target branch %s from %s failed: %w\n%s", targetBranch, baseRef, err, out)
	}
	head, err := gitRevParse(ctx, localPath, "HEAD")
	return head, false, err
}

func validateProjectPatchQueueIntegrationSource(ctx context.Context, localPath, sourceBranch, sourceHead string) error {
	sourceBranch = strings.TrimSpace(sourceBranch)
	sourceHead = strings.TrimSpace(sourceHead)
	if sourceBranch == "" || sourceHead == "" {
		return fmt.Errorf("source branch and head_sha are required")
	}
	remoteRef := "refs/remotes/origin/" + sourceBranch
	if remoteSHA, ok := gitRefSHA(ctx, localPath, remoteRef); ok {
		if remoteSHA != sourceHead {
			return fmt.Errorf("origin/%s head %s does not match accepted item head_sha %s", sourceBranch, remoteSHA, sourceHead)
		}
	} else if localSHA, ok := gitRefSHA(ctx, localPath, "refs/heads/"+sourceBranch); ok {
		if localSHA != sourceHead {
			return fmt.Errorf("local %s head %s does not match accepted item head_sha %s", sourceBranch, localSHA, sourceHead)
		}
	} else {
		return fmt.Errorf("source branch %s is not available from origin; branch owner must push before integration", sourceBranch)
	}
	if !gitCommitObjectExists(ctx, localPath, sourceHead) {
		return fmt.Errorf("accepted item head_sha %s is not present as a commit object", sourceHead)
	}
	return nil
}

func verifyProjectPatchQueueCanonicalRemoteTarget(ctx context.Context, localPath, remoteName, targetBranch, expectedHead string) (string, error) {
	remoteName = strings.TrimSpace(firstNonEmpty(remoteName, "origin"))
	targetBranch = strings.TrimSpace(targetBranch)
	expectedHead = strings.TrimSpace(expectedHead)
	if remoteName == "" || targetBranch == "" || expectedHead == "" {
		return "", fmt.Errorf("remote_name, target_branch, and expected target head are required")
	}
	out, err := gitCombined(ctx, localPath, "ls-remote", "--heads", remoteName, targetBranch)
	if err != nil {
		return "", fmt.Errorf("git ls-remote %s %s failed: %w\n%s", remoteName, targetBranch, err, strings.TrimSpace(out))
	}
	wantRef := "refs/heads/" + targetBranch
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || strings.TrimSpace(fields[1]) != wantRef {
			continue
		}
		remoteHead := strings.TrimSpace(fields[0])
		if remoteHead == "" {
			continue
		}
		if remoteHead != expectedHead {
			return remoteHead, fmt.Errorf("canonical remote target branch %s/%s is at %s, expected %s", remoteName, targetBranch, remoteHead, expectedHead)
		}
		return remoteHead, nil
	}
	return "", fmt.Errorf("canonical remote target branch %s/%s was not found", remoteName, targetBranch)
}

func gitMergeBaseIsAncestor(ctx context.Context, localPath, ancestor, descendant string) (bool, error) {
	ancestor = strings.TrimSpace(ancestor)
	descendant = strings.TrimSpace(descendant)
	if ancestor == "" || descendant == "" {
		return false, fmt.Errorf("ancestor and descendant refs are required")
	}
	out, err := gitCombined(ctx, localPath, "merge-base", "--is-ancestor", ancestor, descendant)
	if err == nil {
		return true, nil
	}
	if strings.TrimSpace(out) != "" {
		return false, fmt.Errorf("git merge-base failed: %s", strings.TrimSpace(out))
	}
	return false, nil
}

func projectPatchQueueGitMerge(ctx context.Context, localPath, agentID, sourceHead, message string) (string, error) {
	authorSegment := sanitizeRefSegment(firstNonEmpty(agentID, "integrator"))
	authorName := "Rhizome Integrator " + authorSegment
	authorEmail := authorSegment + "@rhizome.local"
	return gitCombined(ctx, localPath,
		"-c", "user.name="+authorName,
		"-c", "user.email="+authorEmail,
		"merge", "--no-ff", "--no-gpg-sign", "-m", strings.TrimSpace(message), strings.TrimSpace(sourceHead),
	)
}

func projectPatchQueueIntegrateMergeMessage(args map[string]any, projectID string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord, targetBranch string) string {
	if message := strings.TrimSpace(stringArg(args, "merge_message")); message != "" {
		return message
	}
	return fmt.Sprintf("Integrate %s into %s", sanitizeRefSegment(firstNonEmpty(branch.BranchName, item.BranchID, projectID)), sanitizeRefSegment(targetBranch))
}

func projectPatchQueueIntegrationCheckoutID(checkouts []ProjectCheckoutRecord, repoID, agentID, machineID, localPath, targetBranch string) string {
	for _, checkout := range checkouts {
		if strings.TrimSpace(checkout.RepoID) != strings.TrimSpace(repoID) ||
			strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(agentID) ||
			!strings.EqualFold(strings.TrimSpace(checkout.Status), "ACTIVE") {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(checkout.CheckoutKind), "integration") &&
			strings.TrimSpace(checkout.BranchName) == strings.TrimSpace(targetBranch) &&
			(strings.TrimSpace(checkout.MachineID) == strings.TrimSpace(machineID) || sameRuntimePath(checkout.LocalPath, localPath)) {
			return strings.TrimSpace(checkout.CheckoutID)
		}
		if sameRuntimePath(checkout.LocalPath, localPath) {
			return strings.TrimSpace(checkout.CheckoutID)
		}
	}
	return ""
}

func projectPatchQueueIntegrationBranchID(branches []ProjectBranchRecord, repoID, agentID, targetBranch string) string {
	for _, branch := range branches {
		if strings.TrimSpace(branch.RepoID) == strings.TrimSpace(repoID) &&
			strings.TrimSpace(branch.AgentID) == strings.TrimSpace(agentID) &&
			strings.TrimSpace(branch.BranchName) == strings.TrimSpace(targetBranch) &&
			strings.EqualFold(strings.TrimSpace(branch.BranchKind), "integration") {
			return strings.TrimSpace(branch.BranchID)
		}
	}
	return ""
}
