package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

type ProjectCheckoutRegisterTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	ownerUserID    string
	workdir        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectCheckoutRegisterTool(client *RhizomeClient, workspaceID, agentID, ownerUserID, workdir string) *ProjectCheckoutRegisterTool {
	return &ProjectCheckoutRegisterTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		ownerUserID: strings.TrimSpace(ownerUserID),
		workdir:     strings.TrimSpace(workdir),
	}
}

func (t *ProjectCheckoutRegisterTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectCheckoutRegisterTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectCheckoutRegisterTool) Name() string { return "project_checkout_register" }

func (t *ProjectCheckoutRegisterTool) Description() string {
	return "Register this agent's local project checkout and optional branch reservation evidence. It verifies the checkout by default and never clones, commits, pushes, merges, or switches branches."
}

func (t *ProjectCheckoutRegisterTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{"type": "string", "description": "Project id."},
			"repo_id":    map[string]any{"type": "string", "description": "Repository id. Defaults to canonical READY repository."},
			"local_path": map[string]any{"type": "string", "description": "Local checkout path. Defaults to this agent workdir."},
			"checkout_id": map[string]any{
				"type":        "string",
				"description": "Optional existing checkout id to heartbeat/update.",
			},
			"checkout_kind": map[string]any{
				"type":        "string",
				"description": "Checkout kind: clone, worktree, integration, review, or scratch. Defaults clone.",
			},
			"branch_name": map[string]any{
				"type":        "string",
				"description": "Branch name to reserve. Defaults to a short deterministic agent branch for the current project/task.",
			},
			"current_branch": map[string]any{
				"type":        "string",
				"description": "Observed current checkout branch. When verify_git_remote is true, the tool reads this from git instead.",
			},
			"branch_kind": map[string]any{
				"type":        "string",
				"description": "Branch kind: feature, integration, review, release, or scratch. Defaults feature.",
			},
			"base_branch": map[string]any{
				"type":        "string",
				"description": "Base branch. Defaults to repo/default project branch or main.",
			},
			"write_scope_json": map[string]any{
				"type":        "string",
				"description": "JSON write scope for branch ownership. Defaults to this agent's active project role scope if available.",
			},
			"active_task_id":  map[string]any{"type": "string", "description": "Optional active task id; must be paired with active_claim_id."},
			"active_claim_id": map[string]any{"type": "string", "description": "Optional active claim id; must be paired with active_task_id."},
			"dirty_state":     map[string]any{"type": "string", "description": "clean, dirty, or unknown. Defaults unknown."},
			"checkout_status": map[string]any{"type": "string", "description": "ACTIVE, STALE, BLOCKED, ABANDONED, or ARCHIVED. Defaults ACTIVE."},
			"branch_status":   map[string]any{"type": "string", "description": "RESERVED, ACTIVE, BLOCKED, READY_FOR_REVIEW, MERGED, ABANDONED, or ARCHIVED. Defaults RESERVED."},
			"register_branch": map[string]any{"type": "boolean", "description": "Whether to register a branch reservation after checkout registration. Defaults true."},
			"machine_id":      map[string]any{"type": "string", "description": "Optional stable machine id. Defaults to runtime hostname."},
			"machine_label":   map[string]any{"type": "string", "description": "Optional human-readable machine label."},
			"verify_git_remote": map[string]any{
				"type":        "boolean",
				"description": "Verify local_path is a git checkout whose remotes match the project repository. Defaults true.",
			},
		},
		"required": []string{"project_id"},
	}
}

func (t *ProjectCheckoutRegisterTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_checkout_register is disabled: missing client, workspace, or agent identity", IsError: true}
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
		return &ToolResult{Output: fmt.Sprintf("project_checkout_register failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	if activeTaskID != "" && !projectCoordinationContainsOpenTask(coordination, activeTaskID) {
		return &ToolResult{Output: fmt.Sprintf("project_checkout_register blocked stale active_task_id %s: it is not an open task in project %s. Use a current implementation task from project coordination instead of reusing old cross-project task context.", activeTaskID, projectID), IsError: true}
	}
	registerBranch := true
	if explicit := optionalBoolArg(args, "register_branch"); explicit != nil {
		registerBranch = *explicit
	}
	terminalEvidenceClosure := projectCheckoutRegisterTerminalEvidenceClosure(args, registerBranch)
	if registerBranchRuntimeBound(t.runtimeBinding) && (registerBranch || terminalEvidenceClosure) {
		binding := t.runtimeBinding()
		validationActiveTaskID := activeTaskID
		validationActiveClaimID := activeClaimID
		if validationActiveTaskID == "" && binding.TaskID != "" && (binding.ProjectID == "" || sameWorkspaceDocProjectID(binding.ProjectID, projectID)) {
			validationActiveTaskID = binding.TaskID
			validationActiveClaimID = firstNonEmpty(validationActiveClaimID, binding.TaskID)
		}
		if err := validateProjectCheckoutRuntimeBinding(binding, projectID, validationActiveTaskID, "project_checkout_register"); err != nil {
			return &ToolResult{Output: err.Error(), IsError: true}
		}
		if !terminalEvidenceClosure && registerBranch {
			if err := validateProjectCheckoutBranchTask(coordination, validationActiveTaskID, "project_checkout_register"); err != nil {
				return &ToolResult{Output: err.Error(), IsError: true}
			}
			activeTaskID = validationActiveTaskID
			activeClaimID = validationActiveClaimID
		} else if err := validateProjectCheckoutTerminalEvidenceClosureArgs(args, registerBranch); err != nil {
			return &ToolResult{Output: err.Error(), IsError: true}
		}
	}
	if terminalEvidenceClosure {
		activeTaskID = ""
		activeClaimID = ""
	}
	repoID := strings.TrimSpace(stringArg(args, "repo_id"))
	repo, ok := selectProjectCheckoutRegisterRepo(coordination, repoID)
	if !ok {
		return &ToolResult{Output: fmt.Sprintf("project_checkout_register requires a READY repository with remote_url for project %s", projectID), IsError: true}
	}
	localPath := firstNonEmpty(strings.TrimSpace(stringArg(args, "local_path")), t.workdir)
	if localPath == "" {
		return &ToolResult{Output: "local_path is required when agent workdir is not configured", IsError: true}
	}
	canonicalLocalPath, err := resolveProjectCheckoutRegisterPath(t.workdir, localPath)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_checkout_register invalid local_path: %v", err), IsError: true}
	}
	localPath = canonicalLocalPath
	verifyGitRemote := true
	if explicit := optionalBoolArg(args, "verify_git_remote"); explicit != nil {
		verifyGitRemote = *explicit
	}
	if verifyGitRemote {
		if err := validateProjectCheckoutWorkdir(ctx, localPath, repo); err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_checkout_register local verification failed: %v", err), IsError: true}
		}
	}
	checkoutBranchName := strings.TrimSpace(stringArg(args, "current_branch"))
	localHeadSHA := ""
	if verifyGitRemote {
		currentBranch, err := currentGitBranch(ctx, localPath)
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_checkout_register local verification failed reading current branch: %v", err), IsError: true}
		}
		checkoutBranchName = currentBranch
		localHeadSHA, err = gitRevParse(ctx, localPath, "HEAD")
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_checkout_register local verification failed reading HEAD: %v", err), IsError: true}
		}
	}

	machineID := firstNonEmpty(strings.TrimSpace(stringArg(args, "machine_id")), runtimeMachineID())
	checkoutID := strings.TrimSpace(stringArg(args, "checkout_id"))
	var hasExistingCheckout bool
	if checkoutID != "" {
		if existing, exists := findProjectCheckoutRegisterExistingCheckoutByID(coordination.Checkouts, checkoutID); exists {
			if strings.TrimSpace(existing.RepoID) != strings.TrimSpace(repo.RepoID) || strings.TrimSpace(existing.AgentID) != strings.TrimSpace(t.agentID) {
				return &ToolResult{Output: fmt.Sprintf("project_checkout_register blocked foreign checkout_id %s: existing checkout belongs to repo_id=%s agent_id=%s, not repo_id=%s agent_id=%s", checkoutID, existing.RepoID, existing.AgentID, repo.RepoID, t.agentID), IsError: true}
			}
			if err := validateProjectCheckoutRegisterExistingCheckout(existing, machineID, localPath, t.workdir, activeTaskID, activeClaimID, terminalEvidenceClosure); err != nil {
				return &ToolResult{Output: fmt.Sprintf("project_checkout_register blocked stale checkout_id %s: %v", checkoutID, err), IsError: true}
			}
			hasExistingCheckout = true
		}
	}
	if checkoutID == "" {
		if existing, ok := selectProjectCheckoutRegisterExistingCheckout(coordination.Checkouts, repo.RepoID, t.agentID, machineID, localPath, t.workdir, activeTaskID, activeClaimID); ok {
			checkoutID = existing.CheckoutID
			hasExistingCheckout = true
		}
	}
	if terminalEvidenceClosure && !hasExistingCheckout {
		return &ToolResult{Output: "project_checkout_register terminal evidence closure found no existing same-agent checkout to close; stale checkout evidence is already absent or belongs to another agent", IsError: true}
	}
	baseBranch := firstNonEmpty(strings.TrimSpace(stringArg(args, "base_branch")), repo.DefaultBranch, coordination.Profile.RepoDefaultBranch, "main")
	branchName := strings.TrimSpace(stringArg(args, "branch_name"))
	if branchName == "" {
		branchName = projectClaimBranchName(t.agentID, projectID, firstNonEmpty(activeTaskID, "manual"))
	}
	var branchID string
	var existingBranch ProjectBranchRecord
	var hasExistingBranch bool
	var branchStatus string
	if registerBranch {
		branchID = strings.TrimSpace(stringArg(args, "branch_id"))
		if branchID != "" {
			if existing, ok := selectProjectCheckoutRegisterExistingBranchByID(coordination.Branches, repo.RepoID, t.agentID, branchID); ok {
				existingBranch = existing
				hasExistingBranch = true
			}
		}
		if branchID == "" {
			if existing, ok := selectProjectCheckoutRegisterExistingBranch(coordination.Branches, coordination.PatchQueueItems, repo.RepoID, t.agentID, branchName, activeTaskID); ok {
				branchID = existing.BranchID
				existingBranch = existing
				hasExistingBranch = true
			}
		}
		branchStatus = strings.ToUpper(strings.TrimSpace(stringArg(args, "branch_status")))
		if branchStatus == "" && hasExistingBranch {
			branchStatus = strings.ToUpper(strings.TrimSpace(existingBranch.Status))
		}
		if terminalEvidenceClosure {
			if !hasExistingBranch {
				return &ToolResult{Output: "project_checkout_register terminal evidence closure found no existing same-agent branch to close; stale branch evidence is already absent or belongs to another agent", IsError: true}
			}
			if strings.TrimSpace(existingBranch.CheckoutID) != "" && strings.TrimSpace(existingBranch.CheckoutID) != strings.TrimSpace(checkoutID) {
				return &ToolResult{Output: fmt.Sprintf("project_checkout_register terminal evidence closure branch %s belongs to checkout %s, not checkout %s", existingBranch.BranchID, existingBranch.CheckoutID, checkoutID), IsError: true}
			}
			if !projectCheckoutRegisterTerminalBranchStatus(branchStatus) {
				return &ToolResult{Output: "project_checkout_register terminal evidence closure requires an existing terminal branch status MERGED, ARCHIVED, or ABANDONED", IsError: true}
			}
		} else {
			if err := validateProjectCheckoutRegisterActiveClaimIDs(t.runtimeBinding, checkoutID, branchID, "project_checkout_register"); err != nil {
				return &ToolResult{Output: err.Error(), IsError: true}
			}
			if verifyGitRemote {
				if err := validateProjectCheckoutRegisterCurrentBranch(localPath, checkoutBranchName, branchName); err != nil {
					return &ToolResult{Output: err.Error(), IsError: true}
				}
			}
		}
	}
	requestedWriteScopeJSON := strings.TrimSpace(stringArg(args, "write_scope_json"))
	writeScopeJSON := requestedWriteScopeJSON
	if registerBranch {
		if terminalEvidenceClosure {
			existingWriteScopeJSON := strings.TrimSpace(existingBranch.WriteScopeJSON)
			if existingWriteScopeJSON == "" || !json.Valid([]byte(existingWriteScopeJSON)) {
				return &ToolResult{Output: "project_checkout_register terminal evidence closure requires existing branch write_scope_json to preserve historical evidence", IsError: true}
			}
			if requestedWriteScopeJSON != "" && requestedWriteScopeJSON != existingWriteScopeJSON {
				return &ToolResult{Output: "project_checkout_register terminal evidence closure must preserve existing branch write_scope_json without widening or replacement", IsError: true}
			}
			writeScopeJSON = existingWriteScopeJSON
		} else if claimWriteScopeJSON, _, ok := selectProjectActiveClaimWriteScope(coordination, t.agentID, activeTaskID); ok {
			writeScopeJSON = claimWriteScopeJSON
		} else if writeScopeJSON == "" {
			writeScopeJSON, _, _ = selectProjectClaimWriteScope(coordination, t.agentID)
		}
		if writeScopeJSON != "" && !json.Valid([]byte(writeScopeJSON)) {
			return &ToolResult{Output: "write_scope_json is required and must be valid JSON when register_branch is true", IsError: true}
		}
		if writeScopeJSON == "" && !terminalEvidenceClosure {
			return &ToolResult{Output: "write_scope_json is required and must be valid JSON when register_branch is true", IsError: true}
		}
		if !terminalEvidenceClosure {
			if err := validateProjectWriteScopeJSON(writeScopeJSON); err != nil {
				return &ToolResult{Output: fmt.Sprintf("write_scope_json is invalid when register_branch is true: %v", err), IsError: true}
			}
		}
	}
	checkout, err := t.client.RegisterProjectCheckout(ctx, ProjectCheckoutRegisterInput{
		WorkspaceID:   t.workspaceID,
		ProjectID:     projectID,
		ActorID:       t.agentID,
		CheckoutID:    checkoutID,
		RepoID:        repo.RepoID,
		MachineID:     machineID,
		MachineLabel:  firstNonEmpty(strings.TrimSpace(stringArg(args, "machine_label")), machineID),
		OwnerUserID:   t.ownerUserID,
		AgentID:       t.agentID,
		LocalPath:     localPath,
		CheckoutKind:  firstNonEmpty(strings.ToLower(strings.TrimSpace(stringArg(args, "checkout_kind"))), "clone"),
		BranchName:    checkoutBranchName,
		BaseBranch:    baseBranch,
		HeadSHA:       localHeadSHA,
		DirtyState:    firstNonEmpty(strings.ToLower(strings.TrimSpace(stringArg(args, "dirty_state"))), "unknown"),
		ActiveTaskID:  activeTaskID,
		ActiveClaimID: activeClaimID,
		Status:        firstNonEmpty(strings.ToUpper(strings.TrimSpace(stringArg(args, "checkout_status"))), "ACTIVE"),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_checkout_register failed registering checkout evidence: %v", err), IsError: true}
	}

	var branch ProjectBranchRecord
	if registerBranch {
		branch, err = t.client.RegisterProjectBranch(ctx, ProjectBranchRegisterInput{
			WorkspaceID:    t.workspaceID,
			ProjectID:      projectID,
			ActorID:        t.agentID,
			BranchID:       branchID,
			RepoID:         repo.RepoID,
			CheckoutID:     checkout.CheckoutID,
			AgentID:        t.agentID,
			ActiveTaskID:   activeTaskID,
			ActiveClaimID:  activeClaimID,
			BranchName:     branchName,
			BranchKind:     firstNonEmpty(strings.ToLower(strings.TrimSpace(stringArg(args, "branch_kind"))), "feature"),
			BaseBranch:     baseBranch,
			HeadSHA:        localHeadSHA,
			WriteScopeJSON: writeScopeJSON,
			Status:         firstNonEmpty(branchStatus, "RESERVED"),
		})
		if err != nil {
			reconciliationCause := err
			if cleanupErr := t.compensateCheckoutBranchFailure(ctx, checkout, repo, machineID, localPath, baseBranch, checkoutBranchName); cleanupErr != nil {
				reconciliationCause = fmt.Errorf("%w; checkout cleanup also failed: %v", err, cleanupErr)
			}
			reconciliationBranch := existingBranch
			if strings.TrimSpace(reconciliationBranch.BranchID) == "" && strings.TrimSpace(reconciliationBranch.BranchName) == "" {
				reconciliationBranch = ProjectBranchRecord{
					BranchID:       branchID,
					WorkspaceID:    t.workspaceID,
					ProjectID:      projectID,
					RepoID:         repo.RepoID,
					CheckoutID:     checkout.CheckoutID,
					AgentID:        t.agentID,
					ActiveTaskID:   activeTaskID,
					ActiveClaimID:  activeClaimID,
					BranchName:     branchName,
					BranchKind:     firstNonEmpty(strings.ToLower(strings.TrimSpace(stringArg(args, "branch_kind"))), "feature"),
					BaseBranch:     baseBranch,
					WriteScopeJSON: writeScopeJSON,
					Status:         firstNonEmpty(branchStatus, "RESERVED"),
				}
			}
			return returnProjectBranchEvidenceReconciliationBlock(ctx, projectBranchEvidenceReconciliationContext{
				client:      t.client,
				workspaceID: t.workspaceID,
				agentID:     t.agentID,
				ownerUserID: t.ownerUserID,
				sourceTool:  "project_checkout_register",
			}, projectBranchCommitEvidenceReconciliationInput{
				ProjectID:      projectID,
				RepoID:         repo.RepoID,
				Branch:         reconciliationBranch,
				CheckoutID:     checkout.CheckoutID,
				LocalPath:      localPath,
				CurrentBranch:  checkoutBranchName,
				BaseBranch:     baseBranch,
				BaseSHA:        projectBranchCommitCanonicalBaseSHA(ctx, localPath, baseBranch, reconciliationBranch.BaseSHA),
				HeadSHA:        projectBranchCommitBestEffortHeadSHA(ctx, localPath),
				WriteScopeJSON: writeScopeJSON,
				Stage:          "branch_register",
				CommitCreated:  false,
				Cause:          reconciliationCause,
			})
		}
	}

	coordinationAfter, coordErr := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	coordinationState := coordination.GateStatus.OverallState
	coordinationReady := coordination.GateStatus.ImplementationReady
	if coordErr == nil {
		coordinationState = coordinationAfter.GateStatus.OverallState
		coordinationReady = coordinationAfter.GateStatus.ImplementationReady
	}
	payload := map[string]any{
		"project_id":                 projectID,
		"repo_id":                    repo.RepoID,
		"checkout_id":                checkout.CheckoutID,
		"branch_id":                  branch.BranchID,
		"local_path":                 checkout.LocalPath,
		"branch_name":                firstNonEmpty(branch.BranchName, branchName),
		"base_branch":                baseBranch,
		"verification_performed":     verifyGitRemote,
		"branch_registered":          registerBranch,
		"coordination_overall_state": coordinationState,
		"implementation_ready":       coordinationReady,
		"no_git_mutation":            true,
		"guidance":                   "Checkout and branch evidence is durable. This tool did not clone, commit, push, merge, or switch branches.",
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_checkout_register completed for %s", checkout.CheckoutID)}
	}
	return &ToolResult{Output: string(raw)}
}

func selectProjectCheckoutRegisterRepo(coordination ProjectCoordinationRecord, repoID string) (ProjectRepositoryRecord, bool) {
	repoID = strings.TrimSpace(repoID)
	if repoID != "" {
		for _, repo := range coordination.Repositories {
			if strings.TrimSpace(repo.RepoID) == repoID && projectCheckoutRegisterRepoReady(repo) {
				return repo, true
			}
		}
		return ProjectRepositoryRecord{}, false
	}
	repo, ok := selectProjectClaimRepository(coordination)
	if !ok || !projectCheckoutRegisterRepoReady(repo) {
		return ProjectRepositoryRecord{}, false
	}
	return repo, true
}

func projectCheckoutRegisterRepoReady(repo ProjectRepositoryRecord) bool {
	return strings.EqualFold(strings.TrimSpace(repo.RepoStatus), "READY") && strings.TrimSpace(repo.RemoteURL) != ""
}

func projectCoordinationContainsOpenTask(coordination ProjectCoordinationRecord, taskID string) bool {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return false
	}
	for _, task := range coordination.Tasks {
		if strings.TrimSpace(task.TaskID) != taskID {
			continue
		}
		return !projectCheckoutMaterializeTerminalTaskStatus(task.Status)
	}
	return false
}

func registerBranchRuntimeBound(provider func() AgentRuntimeBinding) bool {
	return provider != nil
}

func validateProjectCheckoutRuntimeBinding(binding AgentRuntimeBinding, projectID, activeTaskID, toolName string) error {
	toolName = firstNonEmpty(strings.TrimSpace(toolName), "project checkout tool")
	projectID = strings.TrimSpace(projectID)
	activeTaskID = strings.TrimSpace(activeTaskID)
	bindingTaskID := strings.TrimSpace(binding.TaskID)
	if bindingTaskID == "" {
		return fmt.Errorf("%s requires an active claimed runtime task before registering project checkout/branch evidence. Peer model.ask responses are read-only; first claim the relevant project task through the planner/work loop, then materialize the checkout from that task cycle.", toolName)
	}
	if activeTaskID == "" {
		return fmt.Errorf("%s requires active_task_id for the current claimed runtime task %s before registering project checkout/branch evidence", toolName, bindingTaskID)
	}
	if !sameWorkspaceDocTaskID(activeTaskID, bindingTaskID) {
		return fmt.Errorf("%s blocked checkout for active_task_id %s because this runtime is currently bound to claimed task %s. Do not implement from peer-request context; claim the target task first.", toolName, activeTaskID, bindingTaskID)
	}
	bindingProjectID := strings.TrimSpace(binding.ProjectID)
	if bindingProjectID != "" && projectID != "" && !sameWorkspaceDocProjectID(projectID, bindingProjectID) {
		return fmt.Errorf("%s blocked checkout for project_id %s because this runtime is currently bound to project_id %s", toolName, projectID, bindingProjectID)
	}
	return nil
}

func validateProjectCheckoutRegisterActiveClaimIDs(provider func() AgentRuntimeBinding, checkoutID, branchID, toolName string) error {
	if provider == nil {
		return nil
	}
	binding := provider()
	toolName = firstNonEmpty(strings.TrimSpace(toolName), "project_checkout_register")
	claimCheckoutID := strings.TrimSpace(binding.ClaimCheckoutID)
	if claimCheckoutID != "" && strings.TrimSpace(checkoutID) != claimCheckoutID {
		return fmt.Errorf("%s refuses to register checkout_id %s because current task %s is bound to active claim checkout %s. Use project_checkout_materialize output for this claim; do not rebind stale checkout evidence", toolName, firstNonEmpty(strings.TrimSpace(checkoutID), "<new>"), strings.TrimSpace(binding.TaskID), claimCheckoutID)
	}
	claimBranchID := strings.TrimSpace(binding.ClaimBranchID)
	if claimBranchID != "" && strings.TrimSpace(branchID) != claimBranchID {
		return fmt.Errorf("%s refuses to register branch_id %s because current task %s is bound to active claim branch %s. Registering a stale or alternate branch would publish evidence outside the claimed implementation lane", toolName, firstNonEmpty(strings.TrimSpace(branchID), "<new>"), strings.TrimSpace(binding.TaskID), claimBranchID)
	}
	return nil
}

func validateProjectCheckoutRegisterCurrentBranch(localPath, checkoutBranchName, branchName string) error {
	checkoutBranchName = strings.TrimSpace(checkoutBranchName)
	branchName = strings.TrimSpace(branchName)
	if branchName == "" {
		return nil
	}
	if checkoutBranchName == "" {
		return fmt.Errorf("project_checkout_register refuses to register branch_name %s because local checkout %s has no current branch; this tool does not switch detached or ambiguous checkouts", branchName, localPath)
	}
	if checkoutBranchName != branchName {
		return fmt.Errorf("project_checkout_register refuses to register branch_name %s because local checkout %s is currently on branch %s. This tool does not switch branches or publish stale remote branch evidence", branchName, localPath, checkoutBranchName)
	}
	return nil
}

func validateProjectCheckoutBranchTask(coordination ProjectCoordinationRecord, activeTaskID, toolName string) error {
	task, ok := projectCoordinationTaskByID(coordination, activeTaskID)
	if !ok {
		return fmt.Errorf("%s requires active_task_id %s to exist in project coordination before registering branch evidence", firstNonEmpty(strings.TrimSpace(toolName), "project checkout tool"), strings.TrimSpace(activeTaskID))
	}
	if projectCheckoutTaskAllowsBranchEvidence(task) {
		return nil
	}
	lane := strings.TrimSpace(task.ProjectLane)
	kind := strings.TrimSpace(task.TaskKind)
	return fmt.Errorf("%s blocked branch checkout evidence for task %s because lane=%s kind=%s is not implementation-shaped. Strategy/review/validation tasks may coordinate or inspect, but implementation write-scope branches must be opened from a claimed implementation task.", firstNonEmpty(strings.TrimSpace(toolName), "project checkout tool"), strings.TrimSpace(task.TaskID), firstNonEmpty(lane, "<empty>"), firstNonEmpty(kind, "<empty>"))
}

func projectCoordinationTaskByID(coordination ProjectCoordinationRecord, taskID string) (WorkspaceTaskRecord, bool) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return WorkspaceTaskRecord{}, false
	}
	for _, task := range coordination.Tasks {
		if strings.TrimSpace(task.TaskID) == taskID {
			return task, true
		}
	}
	return WorkspaceTaskRecord{}, false
}

func projectCheckoutTaskAllowsBranchEvidence(task WorkspaceTaskRecord) bool {
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	switch lane {
	case "implementation", "implement", "build", "builder", "frontend", "backend", "api", "ui", "integration":
		return true
	case "strategy", "planning", "plan", "spec", "design", "review", "validation", "test", "testing", "synthesis":
		return false
	}
	kind := strings.ToUpper(strings.TrimSpace(task.TaskKind))
	if kind == "EXECUTION" && lane == "" {
		return true
	}
	return false
}

func projectCheckoutRegisterTerminalEvidenceClosure(args map[string]any, registerBranch bool) bool {
	if !projectCheckoutRegisterInactiveCheckoutStatus(stringArg(args, "checkout_status")) {
		return false
	}
	if !registerBranch {
		return true
	}
	return projectCheckoutRegisterTerminalBranchStatus(stringArg(args, "branch_status"))
}

func validateProjectCheckoutTerminalEvidenceClosureArgs(args map[string]any, registerBranch bool) error {
	if !projectCheckoutRegisterInactiveCheckoutStatus(stringArg(args, "checkout_status")) {
		return fmt.Errorf("project_checkout_register terminal evidence closure requires checkout_status ARCHIVED, ABANDONED, or STALE")
	}
	if !registerBranch {
		return nil
	}
	if !projectCheckoutRegisterTerminalBranchStatus(stringArg(args, "branch_status")) {
		return fmt.Errorf("project_checkout_register terminal evidence closure requires branch_status MERGED, ARCHIVED, or ABANDONED")
	}
	if strings.TrimSpace(stringArg(args, "branch_id")) == "" && strings.TrimSpace(stringArg(args, "branch_name")) == "" {
		return fmt.Errorf("project_checkout_register terminal evidence closure requires branch_id or branch_name")
	}
	return nil
}

func projectCheckoutRegisterInactiveCheckoutStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ARCHIVED", "ABANDONED", "STALE":
		return true
	default:
		return false
	}
}

func projectCheckoutRegisterTerminalBranchStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "MERGED", "ARCHIVED", "ABANDONED":
		return true
	default:
		return false
	}
}

func resolveProjectCheckoutRegisterPath(workdir, localPath string) (string, error) {
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
	if strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", fmt.Errorf("%q is outside agent workdir %q", localPath, workdir)
	}
	return target, nil
}

func currentGitBranch(ctx context.Context, localPath string) (string, error) {
	out, err := exec.CommandContext(ctx, "git", "-C", strings.TrimSpace(localPath), "branch", "--show-current").Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (t *ProjectCheckoutRegisterTool) compensateCheckoutBranchFailure(ctx context.Context, checkout ProjectCheckoutRecord, repo ProjectRepositoryRecord, machineID, localPath, baseBranch, checkoutBranchName string) error {
	if strings.TrimSpace(checkout.CheckoutID) == "" {
		return nil
	}
	_, err := t.client.RegisterProjectCheckout(ctx, ProjectCheckoutRegisterInput{
		WorkspaceID:  t.workspaceID,
		ProjectID:    checkout.ProjectID,
		ActorID:      t.agentID,
		CheckoutID:   checkout.CheckoutID,
		RepoID:       repo.RepoID,
		MachineID:    machineID,
		MachineLabel: firstNonEmpty(strings.TrimSpace(checkout.MachineLabel), machineID),
		OwnerUserID:  firstNonEmpty(strings.TrimSpace(checkout.OwnerUserID), t.ownerUserID),
		AgentID:      t.agentID,
		LocalPath:    localPath,
		CheckoutKind: firstNonEmpty(strings.TrimSpace(checkout.CheckoutKind), "clone"),
		BranchName:   checkoutBranchName,
		BaseBranch:   baseBranch,
		DirtyState:   firstNonEmpty(strings.TrimSpace(checkout.DirtyState), "unknown"),
		Status:       "ABANDONED",
	})
	return err
}

func selectProjectCheckoutRegisterExistingCheckout(checkouts []ProjectCheckoutRecord, repoID, agentID, machineID, localPath, workdir, activeTaskID, activeClaimID string) (ProjectCheckoutRecord, bool) {
	for _, checkout := range checkouts {
		if strings.TrimSpace(checkout.RepoID) != strings.TrimSpace(repoID) ||
			strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(agentID) ||
			strings.TrimSpace(checkout.MachineID) != strings.TrimSpace(machineID) ||
			!strings.EqualFold(strings.TrimSpace(checkout.Status), "ACTIVE") {
			continue
		}
		if !projectCheckoutRegisterCanAdoptExistingCheckout(checkout, activeTaskID, activeClaimID) {
			continue
		}
		if sameProjectLocalPathWithinWorkdir(workdir, checkout.LocalPath, localPath) || sameRuntimePath(checkout.LocalPath, localPath) {
			return checkout, true
		}
	}
	return ProjectCheckoutRecord{}, false
}

func projectCheckoutRegisterCanAdoptExistingCheckout(checkout ProjectCheckoutRecord, activeTaskID, activeClaimID string) bool {
	activeTaskID = strings.TrimSpace(activeTaskID)
	activeClaimID = strings.TrimSpace(activeClaimID)
	checkoutTaskID := strings.TrimSpace(checkout.ActiveTaskID)
	checkoutClaimID := strings.TrimSpace(checkout.ActiveClaimID)
	if checkoutTaskID == "" && checkoutClaimID == "" {
		return true
	}
	if checkoutTaskID != "" && activeTaskID == "" {
		return false
	}
	if checkoutClaimID != "" && activeClaimID == "" {
		return false
	}
	if checkoutTaskID != "" && checkoutTaskID != activeTaskID {
		return false
	}
	if checkoutClaimID != "" && checkoutClaimID != activeClaimID {
		return false
	}
	return true
}

func validateProjectCheckoutRegisterExistingCheckout(checkout ProjectCheckoutRecord, machineID, localPath, workdir, activeTaskID, activeClaimID string, terminalEvidenceClosure bool) error {
	checkoutMachineID := strings.TrimSpace(checkout.MachineID)
	if checkoutMachineID != "" && strings.TrimSpace(machineID) != "" && checkoutMachineID != strings.TrimSpace(machineID) {
		return fmt.Errorf("existing checkout belongs to machine_id=%s, current machine_id=%s", checkoutMachineID, strings.TrimSpace(machineID))
	}
	if strings.TrimSpace(checkout.LocalPath) != "" && !sameProjectLocalPathWithinWorkdir(workdir, checkout.LocalPath, localPath) && !sameRuntimePath(checkout.LocalPath, localPath) {
		return fmt.Errorf("existing checkout local_path=%s does not match requested local_path=%s", checkout.LocalPath, localPath)
	}
	if !terminalEvidenceClosure && !projectCheckoutRegisterCanAdoptExistingCheckout(checkout, activeTaskID, activeClaimID) {
		return fmt.Errorf("existing checkout belongs to active_task_id=%s active_claim_id=%s, not requested active_task_id=%s active_claim_id=%s", checkout.ActiveTaskID, checkout.ActiveClaimID, strings.TrimSpace(activeTaskID), strings.TrimSpace(activeClaimID))
	}
	return nil
}

func selectProjectCheckoutRegisterExistingCheckoutByID(checkouts []ProjectCheckoutRecord, repoID, agentID, checkoutID string) (ProjectCheckoutRecord, bool) {
	checkout, ok := findProjectCheckoutRegisterExistingCheckoutByID(checkouts, checkoutID)
	if !ok {
		return ProjectCheckoutRecord{}, false
	}
	if strings.TrimSpace(checkout.RepoID) != strings.TrimSpace(repoID) ||
		strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(agentID) {
		return ProjectCheckoutRecord{}, false
	}
	return checkout, true
}

func findProjectCheckoutRegisterExistingCheckoutByID(checkouts []ProjectCheckoutRecord, checkoutID string) (ProjectCheckoutRecord, bool) {
	checkoutID = strings.TrimSpace(checkoutID)
	if checkoutID == "" {
		return ProjectCheckoutRecord{}, false
	}
	for _, checkout := range checkouts {
		if strings.TrimSpace(checkout.CheckoutID) != checkoutID {
			continue
		}
		return checkout, true
	}
	return ProjectCheckoutRecord{}, false
}

func selectProjectCheckoutRegisterExistingBranch(branches []ProjectBranchRecord, items []ProjectPatchQueueItemRecord, repoID, agentID, branchName, activeTaskID string) (ProjectBranchRecord, bool) {
	for _, branch := range branches {
		if strings.TrimSpace(branch.RepoID) != strings.TrimSpace(repoID) ||
			strings.TrimSpace(branch.AgentID) != strings.TrimSpace(agentID) {
			continue
		}
		if strings.TrimSpace(branch.BranchName) == strings.TrimSpace(branchName) {
			return branch, true
		}
		if activeTaskID != "" && strings.TrimSpace(branch.ActiveTaskID) == strings.TrimSpace(activeTaskID) {
			if strings.TrimSpace(branchName) != "" && strings.TrimSpace(branch.BranchName) != strings.TrimSpace(branchName) && projectCheckoutMaterializeBranchHasRevisionPatchQueueItem(items, branch) {
				continue
			}
			return branch, true
		}
	}
	return ProjectBranchRecord{}, false
}

func selectProjectCheckoutRegisterExistingBranchByID(branches []ProjectBranchRecord, repoID, agentID, branchID string) (ProjectBranchRecord, bool) {
	branchID = strings.TrimSpace(branchID)
	if branchID == "" {
		return ProjectBranchRecord{}, false
	}
	for _, branch := range branches {
		if strings.TrimSpace(branch.BranchID) != branchID ||
			strings.TrimSpace(branch.RepoID) != strings.TrimSpace(repoID) ||
			strings.TrimSpace(branch.AgentID) != strings.TrimSpace(agentID) {
			continue
		}
		return branch, true
	}
	return ProjectBranchRecord{}, false
}
