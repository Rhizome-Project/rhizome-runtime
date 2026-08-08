package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

type agentWorkClaimAdmissionDryRunInput struct {
	Input               TaskClaimInput
	Applicable          bool
	VirtualProjectClaim bool
}

func (s *Store) agentWorkClaimAdmissionSelectionBlock(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord, trustFirst bool) (*AgentWorkPacket, bool, error) {
	if agentWorkTaskHasOwnerBoundSignal(task) {
		if packet, blocked, err := s.projectOwnerBoundSelectionBlock(ctx, workspaceID, agentID, task); err != nil || blocked {
			return packet, blocked, err
		}
	}
	if packet, blocked, err := s.agentWorkForeignOwnedTaskSelectionBlock(ctx, workspaceID, agentID, task); err != nil || blocked {
		return packet, blocked, err
	}
	if packet, blocked, err := s.agentWorkActiveBranchSelectionBlock(ctx, workspaceID, agentID, task); err != nil || blocked {
		return packet, blocked, err
	}
	claimInput, err := s.agentWorkClaimAdmissionReadModelInput(ctx, workspaceID, agentID, task, trustFirst)
	if err != nil || !claimInput.Applicable {
		return nil, false, err
	}
	err = s.agentWorkClaimAdmissionDryRun(ctx, claimInput)
	if err == nil {
		return nil, false, nil
	}
	if !agentWorkClaimAdmissionErrorIsSelectionBlock(err) {
		return nil, false, err
	}
	if agentWorkClaimAdmissionErrorLooksOwnerBound(err) {
		if packet, blocked, packetErr := s.projectOwnerBoundSelectionBlock(ctx, workspaceID, agentID, task); packetErr != nil {
			return nil, false, packetErr
		} else if blocked {
			return packet, true, nil
		}
	}
	packet, packetErr := s.agentWorkClaimAdmissionBlockPacket(ctx, workspaceID, task, err, claimInput.Input)
	if packetErr != nil {
		return nil, false, packetErr
	}
	return packet, true, nil
}

// agentWorkForeignOwnedTaskSelectionBlock is the read-frontier half of the owner_user_id hard-claim-route
// fence (R25 claimability-parity fix). The write claim path rejects a non-owner of a hard-claim-routed task
// - a patch-queue decision continuation (validation/revision) assigned to a specific agent via
// owner_user_id - through rejectForeignAgentOwnedTaskClaimTx at workspace.go:3767, which runs OUTSIDE
// validateTaskClaimProjectAdmissionWithOptionsTx. The read-model dry-run only consults the latter, so
// without this the work-next surface advertised a delta-assigned validation successor to beta/gamma/
// epsilon/zeta/eta, who then burned cycles on authoritatively-rejected claims while the rightful owner was
// wedged. This delegates to the same predicate in a read-only transaction so the read frontier and
// the write admission cannot diverge - no re-implementation of the ownership rule. The cheap
// taskOwnerUserIDIsHardClaimRoute gate (projectID + requirements only) keeps this to one extra read-only tx
// for the small hard-claim-routed subset and skips it for every other candidate task.
func (s *Store) agentWorkForeignOwnedTaskSelectionBlock(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (*AgentWorkPacket, bool, error) {
	if !taskOwnerUserIDIsHardClaimRoute(task.ProjectID, task.TaskRequirementsJSON) {
		return nil, false, nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, false, fmt.Errorf("begin foreign-owned task selection-block tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rejectErr := s.rejectForeignAgentOwnedTaskClaimTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(task.TaskID), strings.TrimSpace(agentID))
	if rejectErr == nil {
		return nil, false, nil
	}
	if !agentWorkClaimAdmissionErrorIsSelectionBlock(rejectErr) {
		return nil, false, rejectErr
	}
	packet, packetErr := s.agentWorkClaimAdmissionBlockPacket(ctx, workspaceID, task, rejectErr, TaskClaimInput{
		WorkspaceID: strings.TrimSpace(workspaceID),
		TaskID:      strings.TrimSpace(task.TaskID),
		AgentID:     strings.TrimSpace(agentID),
	})
	if packetErr != nil {
		return nil, false, packetErr
	}
	return packet, true, nil
}

func (s *Store) agentWorkClaimAdmissionReadModelInput(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord, trustFirst bool) (agentWorkClaimAdmissionDryRunInput, error) {
	input := TaskClaimInput{
		WorkspaceID:      strings.TrimSpace(workspaceID),
		TaskID:           strings.TrimSpace(task.TaskID),
		AgentID:          strings.TrimSpace(agentID),
		CoordinationMode: CoordinationModeStrict,
		Summary:          "agent.work.next claimability dry-run",
	}
	if trustFirst {
		input.CoordinationMode = CoordinationModeTrustFirst
	}
	out := agentWorkClaimAdmissionDryRunInput{Input: input}
	projectID := strings.TrimSpace(task.ProjectID)
	reuseClaimBindings := projectID == "" || isResumableClaimForAgent(task, strings.TrimSpace(agentID))
	setIfEmpty := func(dst *string, value string) {
		if strings.TrimSpace(*dst) == "" {
			*dst = strings.TrimSpace(value)
		}
	}
	explicitClaimWriteScope := false
	if reuseClaimBindings {
		setIfEmpty(&out.Input.ProjectRoleID, workspaceTaskPointerValue(task.ClaimProjectRoleID))
		setIfEmpty(&out.Input.RepoID, workspaceTaskPointerValue(task.ClaimRepoID))
		setIfEmpty(&out.Input.CheckoutID, workspaceTaskPointerValue(task.ClaimCheckoutID))
		setIfEmpty(&out.Input.BranchID, workspaceTaskPointerValue(task.ClaimBranchID))
		setIfEmpty(&out.Input.WriteScopeJSON, workspaceTaskPointerValue(task.ClaimWriteScopeJSON))
		explicitClaimWriteScope = strings.TrimSpace(workspaceTaskPointerValue(task.ClaimWriteScopeJSON)) != ""
		if out.Input.ProjectRoleID != "" || out.Input.RepoID != "" || out.Input.CheckoutID != "" || out.Input.BranchID != "" || out.Input.WriteScopeJSON != "" {
			out.Applicable = true
		}
	}
	if scopeJSON, ok := taskClaimAuthoritativeWriteScopeForTask(task); ok && len(writeScopePaths(scopeJSON)) > 0 {
		setIfEmpty(&out.Input.WriteScopeJSON, scopeJSON)
		out.Applicable = true
	}
	if projectID == "" || !projectTaskRequiresImplementationGate(task) || task.Status != model.TaskStatusPending || !claimAvailable(task.ClaimStatus) {
		return out, nil
	}
	coordination, err := s.GetProjectCoordination(ctx, strings.TrimSpace(workspaceID), projectID)
	if err != nil {
		return out, err
	}
	repo, ok := agentWorkImplementationClaimRepository(coordination)
	if !ok {
		return out, nil
	}
	setIfEmpty(&out.Input.RepoID, repo.RepoID)
	if branch, ok, err := s.agentWorkClaimAdmissionOwnerBoundBranchBinding(ctx, workspaceID, strings.TrimSpace(repo.RepoID), strings.TrimSpace(agentID), task); err != nil {
		return out, err
	} else if ok {
		setIfEmpty(&out.Input.BranchID, branch.BranchID)
		setIfEmpty(&out.Input.CheckoutID, branch.CheckoutID)
		if strings.TrimSpace(out.Input.WriteScopeJSON) == "" || !explicitClaimWriteScope {
			out.Input.WriteScopeJSON = strings.TrimSpace(branch.WriteScopeJSON)
		}
		out.Applicable = true
	}
	if trustFirst {
		if branch, ok, err := s.agentWorkAdmissionRevisionSourceBranchBinding(ctx, workspaceID, task, strings.TrimSpace(repo.RepoID), strings.TrimSpace(agentID)); err != nil {
			return out, err
		} else if ok {
			setIfEmpty(&out.Input.BranchID, branch.BranchID)
			setIfEmpty(&out.Input.CheckoutID, branch.CheckoutID)
			if !explicitClaimWriteScope {
				out.Input.WriteScopeJSON = strings.TrimSpace(branch.WriteScopeJSON)
			} else {
				setIfEmpty(&out.Input.WriteScopeJSON, branch.WriteScopeJSON)
			}
			out.Applicable = true
			if strings.TrimSpace(out.Input.BranchID) == "" || strings.TrimSpace(out.Input.CheckoutID) == "" {
				out.VirtualProjectClaim = true
			}
		}
	}
	roleID, writeScopeJSON, ok := agentWorkProjectedClaimBindingForReadModel(coordination, task, strings.TrimSpace(repo.RepoID), strings.TrimSpace(agentID), trustFirst)
	if ok {
		setIfEmpty(&out.Input.ProjectRoleID, roleID)
		if strings.TrimSpace(roleID) != "" && strings.TrimSpace(writeScopeJSON) != "" && !explicitClaimWriteScope {
			out.Input.WriteScopeJSON = strings.TrimSpace(writeScopeJSON)
		} else {
			setIfEmpty(&out.Input.WriteScopeJSON, writeScopeJSON)
		}
		out.Applicable = true
		if strings.TrimSpace(out.Input.BranchID) == "" || strings.TrimSpace(out.Input.CheckoutID) == "" {
			out.VirtualProjectClaim = true
		}
	}
	return out, nil
}

func (s *Store) agentWorkClaimAdmissionOwnerBoundBranchBinding(ctx context.Context, workspaceID, repoID, agentID string, task WorkspaceTaskRecord) (ProjectBranchRecord, bool, error) {
	req, ok, err := s.agentWorkOwnerBoundRequirement(ctx, strings.TrimSpace(workspaceID), task)
	if err != nil || !ok {
		return ProjectBranchRecord{}, false, err
	}
	if req.RepairNeeded || strings.TrimSpace(req.BranchID) == "" || strings.TrimSpace(req.RequiredAgentID) != strings.TrimSpace(agentID) || agentWorkOwnerBoundClaimAdmissionDryRunBypass(task, req) {
		return ProjectBranchRecord{}, false, nil
	}
	branch, ok, err := s.agentWorkClaimAdmissionBranchByID(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(task.ProjectID), strings.TrimSpace(repoID), strings.TrimSpace(req.BranchID))
	if err != nil || !ok {
		return ProjectBranchRecord{}, false, err
	}
	if strings.TrimSpace(branch.CheckoutID) == "" || len(writeScopePaths(branch.WriteScopeJSON)) == 0 {
		return ProjectBranchRecord{}, false, nil
	}
	return branch, true, nil
}

func agentWorkOwnerBoundClaimAdmissionDryRunBypass(task WorkspaceTaskRecord, req agentWorkOwnerBoundRequirement) bool {
	if !strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindCoordination) {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(task.ProjectLane), "coordination")
}

func (s *Store) agentWorkClaimAdmissionBranchByID(ctx context.Context, workspaceID, projectID, repoID, branchID string) (ProjectBranchRecord, bool, error) {
	branchID = strings.TrimSpace(branchID)
	if branchID == "" {
		return ProjectBranchRecord{}, false, nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ProjectBranchRecord{}, false, fmt.Errorf("begin owner-bound branch claimability tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	branch, ok, err := validateProjectBranchIDScopeTx(ctx, tx, branchID, strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(repoID))
	if err != nil || !ok {
		return ProjectBranchRecord{}, ok, err
	}
	return branch, true, nil
}

func (s *Store) agentWorkAdmissionRevisionSourceBranchBinding(ctx context.Context, workspaceID string, task WorkspaceTaskRecord, repoID, agentID string) (ProjectBranchRecord, bool, error) {
	if !agentWorkPatchQueueRevisionFollowupTask(task) {
		return ProjectBranchRecord{}, false, nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ProjectBranchRecord{}, false, fmt.Errorf("begin revision source branch claimability tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	branches, err := s.taskClaimRevisionSourceBranchesTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(task.TaskID), strings.TrimSpace(agentID), strings.TrimSpace(repoID))
	if err != nil {
		return ProjectBranchRecord{}, false, err
	}
	var selected ProjectBranchRecord
	for branchID := range branches {
		branch, err := getProjectBranchByIDTx(ctx, tx, branchID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return ProjectBranchRecord{}, false, err
		}
		candidateScope := strings.TrimSpace(branch.WriteScopeJSON)
		if len(writeScopePaths(candidateScope)) == 0 {
			continue
		}
		if strings.TrimSpace(selected.BranchID) != "" {
			return ProjectBranchRecord{}, false, nil
		}
		selected = branch
	}
	if strings.TrimSpace(selected.BranchID) == "" {
		return ProjectBranchRecord{}, false, nil
	}
	return selected, true, nil
}

func (s *Store) agentWorkClaimAdmissionDryRun(ctx context.Context, input agentWorkClaimAdmissionDryRunInput) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin claimability read-model admission tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = s.validateTaskClaimProjectAdmissionWithOptionsTx(ctx, tx, input.Input.WorkspaceID, input.Input.TaskID, input.Input.AgentID, input.Input, taskClaimProjectAdmissionOptions{
		DryRun:                   true,
		AllowVirtualProjectClaim: input.VirtualProjectClaim,
	})
	return err
}

func agentWorkProjectedClaimBindingForReadModel(coordination ProjectCoordinationRecord, task WorkspaceTaskRecord, repoID, agentID string, trustFirst bool) (string, string, bool) {
	repoID = strings.TrimSpace(repoID)
	agentID = strings.TrimSpace(agentID)
	taskWriteScopeJSON := ""
	taskScopeOK := false
	revisionBranchWriteScopeJSON := ""
	revisionBranchScopeOK := false
	roleWriteScopeJSON := ""
	roleScopeOK := false
	var role ProjectRoleRecord
	if trustFirst {
		revisionBranchWriteScopeJSON, revisionBranchScopeOK = agentWorkRevisionSourceBranchWriteScope(coordination, task, repoID, agentID)
		taskWriteScopeJSON, taskScopeOK = agentWorkImplementationTaskClaimWriteScope(task)
		role, roleScopeOK = agentWorkImplementationClaimRole(coordination, agentID)
		if roleScopeOK {
			roleWriteScopeJSON = strings.TrimSpace(role.WriteScopeJSON)
		}
		if revisionBranchScopeOK {
			return "", revisionBranchWriteScopeJSON, true
		}
		if taskScopeOK {
			if roleScopeOK && agentWorkShouldPreferRoleScopeForTrustFirstTask(task, taskWriteScopeJSON, role) {
				return strings.TrimSpace(role.RoleID), roleWriteScopeJSON, true
			}
			return "", taskWriteScopeJSON, true
		}
	}
	if !roleScopeOK {
		role, roleScopeOK = agentWorkImplementationClaimRole(coordination, agentID)
		if roleScopeOK {
			roleWriteScopeJSON = strings.TrimSpace(role.WriteScopeJSON)
		}
	}
	if roleScopeOK {
		return strings.TrimSpace(role.RoleID), roleWriteScopeJSON, true
	}
	if !taskScopeOK {
		taskWriteScopeJSON, taskScopeOK = agentWorkImplementationTaskClaimWriteScope(task)
	}
	if taskScopeOK {
		return "", taskWriteScopeJSON, true
	}
	return "", "", false
}

func agentWorkClaimAdmissionErrorIsSelectionBlock(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrTaskClaimAdmissionInvalid) || errors.Is(err, ErrProjectRoleWriteScopeConflict)
}

func (s *Store) agentWorkClaimAdmissionBlockPacket(ctx context.Context, workspaceID string, task WorkspaceTaskRecord, err error, input TaskClaimInput) (*AgentWorkPacket, error) {
	summary := strings.TrimSpace(err.Error())
	if strings.Contains(summary, "project claim bindings require a project task") ||
		(strings.Contains(summary, "requires branch_id, checkout_id, and write_scope_json") && strings.TrimSpace(task.ProjectID) == "") {
		if workspaceTaskPointerValue(task.ClaimProjectRoleID) != "" ||
			workspaceTaskPointerValue(task.ClaimRepoID) != "" ||
			workspaceTaskPointerValue(task.ClaimCheckoutID) != "" ||
			workspaceTaskPointerValue(task.ClaimBranchID) != "" ||
			workspaceTaskPointerValue(task.ClaimWriteScopeJSON) != "" {
			summary = "Task carries project claim binding residue but project_id is empty; read-model must not offer it as claimable work. Admission rejected: " + summary
		} else if scopeJSON, ok := taskClaimAuthoritativeWriteScopeForTask(task); ok && len(writeScopePaths(scopeJSON)) > 0 {
			summary = "Task has implementation/project claim shape and would receive authoritative write_scope_json during claim admission, but project_id is empty; read-model must not offer it as claimable work. Admission rejected: " + summary
		}
		return projectClaimAdmissionUnclaimablePacket(task, summary), nil
	}
	if agentWorkClaimAdmissionErrorLooksScopeBusy(err) {
		conflictBranchID := agentWorkAdmissionErrorToken(summary, "branch_id")
		conflictOwnerAgentID := agentWorkAdmissionErrorToken(summary, "agent_id")
		if conflictOwnerAgentID == "" {
			var lookupErr error
			conflictOwnerAgentID, lookupErr = s.agentWorkClaimAdmissionConflictBranchOwner(ctx, workspaceID, conflictBranchID)
			if lookupErr != nil {
				return nil, lookupErr
			}
		}
		return projectImplementationClaimScopeBusyPacket(task, strings.TrimSpace(task.ProjectID), summary, agentWorkAdmissionErrorToken(summary, "task_id"), conflictBranchID, conflictOwnerAgentID), nil
	}
	packet := projectClaimAdmissionUnclaimablePacket(task, summary)
	packet.WorkType = "project_claim_admission_blocked"
	packet.CoordinationState = "claim_admission_blocked"
	packet.PreferredTransition = "repair_task_claim_inputs_or_terminalize_stale_task"
	if packet.Gate != nil {
		packet.Gate.GateType = "project_claim_admission_authority"
		packet.Gate.NeededFrom = "claim_admission_predicate"
	}
	return packet, nil
}

func (s *Store) agentWorkClaimAdmissionConflictBranchOwner(ctx context.Context, workspaceID, branchID string) (string, error) {
	branchID = strings.TrimSpace(branchID)
	if branchID == "" {
		return "", nil
	}
	var ownerAgentID, branchWorkspaceID string
	err := s.db.QueryRowContext(ctx, `
SELECT COALESCE(agent_id, ''), COALESCE(workspace_id, '')
  FROM project_branch_registry
 WHERE branch_id = ?
 LIMIT 1`,
		branchID,
	).Scan(&ownerAgentID, &branchWorkspaceID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("load branch owner for claimability packet %s: %w", branchID, err)
	}
	if strings.TrimSpace(workspaceID) != "" && strings.TrimSpace(branchWorkspaceID) != strings.TrimSpace(workspaceID) {
		return "", nil
	}
	return strings.TrimSpace(ownerAgentID), nil
}

func agentWorkClaimAdmissionErrorLooksScopeBusy(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	return errors.Is(err, ErrProjectRoleWriteScopeConflict) ||
		strings.Contains(text, "write_scope_json overlaps active claim") ||
		strings.Contains(text, "write_scope_json overlaps live branch") ||
		strings.Contains(text, "write_scope_json overlaps active") ||
		strings.Contains(text, "write_scope_json overlaps live")
}

func agentWorkClaimAdmissionErrorLooksOwnerBound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(strings.TrimSpace(err.Error())), "owner-bound")
}

func agentWorkAdmissionErrorToken(text, key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	marker := key + "="
	idx := strings.Index(text, marker)
	if idx < 0 {
		return ""
	}
	value := strings.TrimSpace(text[idx+len(marker):])
	if value == "" {
		return ""
	}
	end := len(value)
	for i, r := range value {
		if strings.ContainsRune(" \t\r\n,;)", r) {
			end = i
			break
		}
	}
	return strings.Trim(strings.TrimSpace(value[:end]), "`'\"")
}
