package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

type taskClaimProjectAdmissionRecord struct {
	ProjectID                  string
	ProjectRoleID              string
	RepoID                     string
	CheckoutID                 string
	BranchID                   string
	WriteScopeJSON             string
	ProjectRoleAutoProvisioned bool
}

type taskClaimProjectContext struct {
	ProjectID               string
	ProjectLane             string
	RequiresProjectGate     bool
	CurrentPhase            string
	RepoRequired            bool
	RepoStatus              string
	RepoURL                 string
	DesignDocID             string
	ImplementationPlanDocID string
	ReferenceAt             string
}

type taskClaimProjectAdmissionClearOptions struct {
	PreserveBranchStatus                     bool
	AllowReceiptBackedCompletedBranchRelease bool
}

type taskClaimProjectAdmissionOptions struct {
	DryRun                   bool
	AllowVirtualProjectClaim bool
}

func (r taskClaimProjectAdmissionRecord) hasBindings() bool {
	return r.ProjectRoleID != "" || r.RepoID != "" || r.CheckoutID != "" || r.BranchID != "" || r.WriteScopeJSON != ""
}

func (s *Store) validateTaskClaimProjectAdmissionTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string, input TaskClaimInput) (taskClaimProjectAdmissionRecord, error) {
	return s.validateTaskClaimProjectAdmissionWithOptionsTx(ctx, tx, workspaceID, taskID, agentID, input, taskClaimProjectAdmissionOptions{})
}

func (s *Store) validateTaskClaimProjectAdmissionWithOptionsTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string, input TaskClaimInput, opts taskClaimProjectAdmissionOptions) (taskClaimProjectAdmissionRecord, error) {
	admission := taskClaimProjectAdmissionRecord{
		ProjectRoleID:  strings.TrimSpace(input.ProjectRoleID),
		RepoID:         strings.TrimSpace(input.RepoID),
		CheckoutID:     strings.TrimSpace(input.CheckoutID),
		BranchID:       strings.TrimSpace(input.BranchID),
		WriteScopeJSON: strings.TrimSpace(input.WriteScopeJSON),
	}
	admissionPaths, err := validateWriteScopeJSONPathset(admission.WriteScopeJSON)
	if err != nil {
		return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: %v", ErrTaskClaimAdmissionInvalid, err)
	}
	taskCtx, err := s.taskProjectContextForClaimTx(ctx, tx, workspaceID, taskID)
	if err != nil {
		return taskClaimProjectAdmissionRecord{}, err
	}
	if summary, blocked, err := s.taskClaimProjectImplementationGateClosedTx(ctx, tx, workspaceID, taskCtx); err != nil {
		return taskClaimProjectAdmissionRecord{}, err
	} else if blocked {
		return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: project implementation task %s is blocked by project gate: %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskID), summary)
	}
	admission.ProjectID = taskCtx.ProjectID
	admissionRequired := taskCtx.projectAdmissionRequired()
	roleAdmissionRequired := taskCtx.projectRoleAdmissionRequired()
	trustFirst := coordinationModeTrustFirst(input.CoordinationMode)
	ownerBoundReq, ownerBound, err := s.taskClaimOwnerBoundRequirementTx(ctx, tx, workspaceID, taskID, taskCtx)
	if err != nil {
		return taskClaimProjectAdmissionRecord{}, err
	}
	ownerBoundOwnerMatch := false
	if ownerBound {
		if ownerBoundReq.RepairNeeded || strings.TrimSpace(ownerBoundReq.RequiredAgentID) == "" {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: owner-bound task %s requires strategic repair before claim: %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskID), firstNonEmpty(strings.TrimSpace(ownerBoundReq.Reason), "missing branch owner"))
		}
		if satisfied, err := taskClaimOwnerBoundTerminalAcceptedTx(ctx, tx, workspaceID, taskCtx.ProjectID, ownerBoundReq); err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		} else if satisfied {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: owner-bound task %s is stale because branch_id %s already has an ACCEPTED same-head patch queue decision", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskID), strings.TrimSpace(ownerBoundReq.BranchID))
		}
		if strings.TrimSpace(ownerBoundReq.RequiredAgentID) != strings.TrimSpace(agentID) {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: owner-bound task %s requires agent %s for branch_id %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskID), strings.TrimSpace(ownerBoundReq.RequiredAgentID), strings.TrimSpace(ownerBoundReq.BranchID))
		}
		ownerBoundOwnerMatch = true
		if strings.EqualFold(strings.TrimSpace(ownerBoundReq.Kind), "active_lane_publication") {
			if activeTaskID, activeSessionID, ok, err := taskClaimOwnerActiveImplementationSessionTx(ctx, tx, workspaceID, taskCtx.ProjectID, agentID, taskID); err != nil {
				return taskClaimProjectAdmissionRecord{}, err
			} else if ok {
				return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: owner-bound active-lane publication task %s must resume active implementation task %s session %s before claiming sidecar publication/provenance work", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskID), activeTaskID, activeSessionID)
			}
		}
	}
	roleFitAdvisory := trustFirst && len(projectClaimRequiredRoleTypesForLane(taskCtx.ProjectLane)) > 0
	if !roleFitAdvisory && !ownerBoundOwnerMatch {
		if err := s.validateTaskClaimProjectRoleFitTx(ctx, tx, workspaceID, taskID, agentID, taskCtx); err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
	}
	var suppliedRole ProjectRoleRecord
	if admission.ProjectRoleID != "" {
		suppliedRole, err = s.validateTaskClaimProjectRoleTx(ctx, tx, workspaceID, taskCtx.ProjectID, agentID, admission.ProjectRoleID)
		if err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
	}
	if !admission.hasBindings() {
		if admissionRequired {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: project implementation claim requires branch_id, checkout_id, and write_scope_json", ErrTaskClaimAdmissionInvalid)
		}
		if roleAdmissionRequired {
			if err := s.ensureTaskClaimProjectRoleBindingTx(ctx, tx, workspaceID, taskID, agentID, taskCtx, &admission, suppliedRole, trustFirst || ownerBoundOwnerMatch, opts); err != nil {
				return taskClaimProjectAdmissionRecord{}, err
			}
		}
		return admission, nil
	}
	if taskCtx.ProjectID == "" {
		return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: project claim bindings require a project task", ErrTaskClaimAdmissionInvalid)
	}
	authoritativeScopeJSON := ""
	hasAuthoritativeScope := false
	if admission.BranchID != "" || len(admissionPaths) > 0 {
		authoritativeScopeJSON, hasAuthoritativeScope, err = taskClaimAuthoritativeWriteScopeJSONTx(ctx, tx, workspaceID, taskID)
		if err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
		if !hasAuthoritativeScope {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: branch/write_scope claims for task %s require an authoritative implementation lane scope", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskID))
		}
		if len(admissionPaths) > 0 && !writeScopePathsCoveredByWithAdjacentSidecars(admissionPaths, projectBranchReviewScopePaths(authoritativeScopeJSON)) {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: write_scope_json is outside authoritative lane scope for task %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskID))
		}
	}
	var branch ProjectBranchRecord
	allowCheckoutActiveRebind := false
	allowReviewReadyReclaim := false
	allowActiveBranchRebind := false
	if admission.BranchID != "" {
		branch, err = getProjectBranchByIDTx(ctx, tx, admission.BranchID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: branch_id %s not found", ErrTaskClaimAdmissionInvalid, admission.BranchID)
			}
			return taskClaimProjectAdmissionRecord{}, err
		}
		allowReviewReadyReclaim, err = s.taskClaimCanReuseReviewReadyBranchTx(ctx, tx, workspaceID, taskID, agentID, admission.BranchID)
		if err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
		allowActiveBranchRebind, err = s.taskClaimCanRebindActiveBranchTx(ctx, tx, workspaceID, taskID, agentID, branch)
		if err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
		if err := validateTaskClaimBranchBinding(branch, workspaceID, taskCtx.ProjectID, taskID, agentID, allowReviewReadyReclaim, allowActiveBranchRebind); err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
		if err := bindAdmissionRepo(&admission, branch.RepoID, "branch"); err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
		if admission.CheckoutID == "" {
			admission.CheckoutID = strings.TrimSpace(branch.CheckoutID)
		} else if branch.CheckoutID != "" && admission.CheckoutID != branch.CheckoutID {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: branch checkout_id %s conflicts with claim checkout_id %s", ErrTaskClaimAdmissionInvalid, branch.CheckoutID, admission.CheckoutID)
		}
		admission.WriteScopeJSON = firstNonEmpty(admission.WriteScopeJSON, strings.TrimSpace(branch.WriteScopeJSON))
		allowCheckoutActiveRebind = strings.TrimSpace(branch.CheckoutID) != "" &&
			strings.TrimSpace(admission.CheckoutID) == strings.TrimSpace(branch.CheckoutID) &&
			strings.TrimSpace(branch.AgentID) == strings.TrimSpace(agentID)
	}
	if admission.CheckoutID != "" {
		checkout, err := getProjectCheckoutByIDTx(ctx, tx, admission.CheckoutID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: checkout_id %s not found", ErrTaskClaimAdmissionInvalid, admission.CheckoutID)
			}
			return taskClaimProjectAdmissionRecord{}, err
		}
		// CA-19: validateTaskClaimCheckoutBinding below compares the raw stored Status,
		// which can read ACTIVE while the checkout's heartbeat (last_seen_at) is stale --
		// the coordination read model already derives such a checkout as STALE. Reconcile
		// the derived status here (same 1h freshness window as the read path) so admission
		// does not bind work onto a heartbeat-dead checkout/host operators consider
		// inactive. Reached for both a directly-supplied and a branch-inherited checkout_id.
		if deriveProjectCheckoutStatus(checkout, time.Now().UTC(), time.Hour) == ProjectCheckoutStatusStale {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: checkout_id %s is stale (last_seen_at beyond freshness window); re-register the checkout before claiming", ErrTaskClaimAdmissionInvalid, checkout.CheckoutID)
		}
		// CA-04: the branch-derived allowCheckoutActiveRebind above only proves the
		// branch and agent match; it does NOT prove the checkout's *prior* active task
		// is terminal/BLOCKED. Without this, an agent claiming a second task that
		// resolves to the same checkout via a different branch could self-steal the
		// working tree of its own still-RUNNING sibling task. Downgrade the rebind
		// permission unless the checkout's prior active task is releasable. Skip the
		// downgrade when the branch path already authorized the reuse (review-ready
		// reclaim or an active-branch rebind), since those are legitimate same-branch
		// follow-ups rather than a cross-branch checkout steal.
		if allowCheckoutActiveRebind && !allowReviewReadyReclaim && !allowActiveBranchRebind {
			releasable, err := s.taskClaimCanRebindActiveCheckoutTx(ctx, tx, workspaceID, taskID, agentID, checkout)
			if err != nil {
				return taskClaimProjectAdmissionRecord{}, err
			}
			allowCheckoutActiveRebind = releasable
		}
		if err := validateTaskClaimCheckoutBinding(checkout, workspaceID, taskCtx.ProjectID, taskID, agentID, allowCheckoutActiveRebind); err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
		if err := validateTaskClaimCheckoutMatchesBranch(checkout, branch); err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
		if err := bindAdmissionRepo(&admission, checkout.RepoID, "checkout"); err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
	}
	if admission.RepoID != "" {
		repo, err := getProjectRepositoryTx(ctx, tx, workspaceID, taskCtx.ProjectID, admission.RepoID)
		if err != nil {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: repo_id %s invalid: %v", ErrTaskClaimAdmissionInvalid, admission.RepoID, err)
		}
		if repo.WorkspaceID != workspaceID || repo.ProjectID != taskCtx.ProjectID {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: repo_id %s belongs to workspace=%s project=%s", ErrTaskClaimAdmissionInvalid, repo.RepoID, repo.WorkspaceID, repo.ProjectID)
		}
	}
	if admission.BranchID != "" && admission.CheckoutID == "" {
		return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: branch_id claims require a registered checkout_id", ErrTaskClaimAdmissionInvalid)
	}
	if admissionRequired {
		admissionPaths, err = validateWriteScopeJSONPathset(admission.WriteScopeJSON)
		if err != nil {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: %v", ErrTaskClaimAdmissionInvalid, err)
		}
		if admission.WriteScopeJSON == "" || (!opts.AllowVirtualProjectClaim && (admission.BranchID == "" || admission.CheckoutID == "")) {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: project implementation claim requires branch_id, checkout_id, and write_scope_json", ErrTaskClaimAdmissionInvalid)
		}
		if len(admissionPaths) == 0 {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: write_scope_json must include non-empty paths for project implementation claims", ErrTaskClaimAdmissionInvalid)
		}
		if hasAuthoritativeScope && !writeScopePathsCoveredByWithAdjacentSidecars(admissionPaths, projectBranchReviewScopePaths(authoritativeScopeJSON)) {
			return taskClaimProjectAdmissionRecord{}, fmt.Errorf("%w: write_scope_json is outside authoritative lane scope for task %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskID))
		}
	}
	if admission.RepoID != "" && admission.WriteScopeJSON != "" {
		if err := s.ensureTaskClaimWriteScopeAvailableTx(ctx, tx, workspaceID, taskID, agentID, admission); err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
	}
	if roleAdmissionRequired {
		if err := s.ensureTaskClaimProjectRoleBindingTx(ctx, tx, workspaceID, taskID, agentID, taskCtx, &admission, suppliedRole, trustFirst || ownerBoundOwnerMatch, opts); err != nil {
			return taskClaimProjectAdmissionRecord{}, err
		}
	}
	return admission, nil
}

func taskClaimAuthoritativeWriteScopeJSONTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) (string, bool, error) {
	var row WorkspaceTaskRecord
	var requiresProjectGate int
	var tagsJSON, writeScopeHintsJSON string
	if err := tx.QueryRowContext(ctx, `
SELECT t.task_id, COALESCE(t.title, ''), COALESCE(t.description, ''), COALESCE(t.project_id, ''),
       COALESCE(t.project_lane, ''), COALESCE(t.requires_project_gate, 0),
       COALESCE(t.task_requirements_json, '{}'), COALESCE(t.write_scope_hints_json, '[]'),
       COALESCE(t.tags_json, '[]')
  FROM workspace_tasks wt
  JOIN tasks t ON t.task_id = wt.task_id
 WHERE wt.workspace_id = ? AND wt.task_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(taskID),
	).Scan(
		&row.TaskID,
		&row.Title,
		&row.Description,
		&row.ProjectID,
		&row.ProjectLane,
		&requiresProjectGate,
		&row.TaskRequirementsJSON,
		&writeScopeHintsJSON,
		&tagsJSON,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, ErrWorkspaceTaskAbsent
		}
		return "", false, fmt.Errorf("load task authoritative write scope: %w", err)
	}
	row.RequiresProjectGate = sqliteIntToBool(requiresProjectGate)
	row.TaskRequirementsJSON = normalizeTaskRequirementsJSON(row.TaskRequirementsJSON)
	row.WriteScopeHints = parseTaskWriteScopeHintsJSON(writeScopeHintsJSON)
	row.Tags = parseTaskTagsJSON(tagsJSON)
	scopeJSON, ok := taskClaimAuthoritativeWriteScopeForTask(row)
	if !ok || len(writeScopePaths(scopeJSON)) == 0 {
		return "", false, nil
	}
	return scopeJSON, true, nil
}

func taskClaimAuthoritativeWriteScopeForTask(task WorkspaceTaskRecord) (string, bool) {
	if !projectTaskRequiresImplementationGate(task) {
		return "", false
	}
	scopeJSON, ok := agentWorkImplementationTaskClaimWriteScope(task)
	if !ok || len(writeScopePaths(scopeJSON)) == 0 {
		return "", false
	}
	return scopeJSON, true
}

func (s *Store) ensureTaskClaimProjectRoleBindingTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string, taskCtx taskClaimProjectContext, admission *taskClaimProjectAdmissionRecord, suppliedRole ProjectRoleRecord, allowAutoProvision bool, opts taskClaimProjectAdmissionOptions) error {
	if admission == nil {
		return fmt.Errorf("%w: project claim admission is required", ErrTaskClaimAdmissionInvalid)
	}
	projectID := strings.TrimSpace(taskCtx.ProjectID)
	requiredRoles := projectClaimRequiredRoleTypesForLane(taskCtx.ProjectLane)
	if projectID == "" || len(requiredRoles) == 0 {
		return nil
	}
	if strings.TrimSpace(admission.ProjectRoleID) != "" {
		if suppliedRole.RoleID == "" {
			role, err := s.validateTaskClaimProjectRoleTx(ctx, tx, workspaceID, projectID, agentID, admission.ProjectRoleID)
			if err != nil {
				return err
			}
			suppliedRole = role
		}
		if !projectClaimRoleTypeAllowed(suppliedRole.RoleType, requiredRoles) {
			return fmt.Errorf("%w: project_role_id %s has role_type %s but task %s requires %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(admission.ProjectRoleID), strings.TrimSpace(suppliedRole.RoleType), strings.TrimSpace(taskID), projectClaimRequiredRoleSummary(requiredRoles))
		}
		if strings.TrimSpace(suppliedRole.RoleType) == ProjectRoleImplementer && !writeScopePathsCoverScopeJSON(admission.WriteScopeJSON, suppliedRole.WriteScopeJSON) {
			return fmt.Errorf("%w: project_role_id %s write_scope_json does not cover claim write_scope_json for task %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(admission.ProjectRoleID), strings.TrimSpace(taskID))
		}
		return nil
	}
	role, hasCoveringRole, hasActiveRequiredRole, err := s.activeProjectRoleForClaimScopeTx(ctx, tx, workspaceID, projectID, agentID, admission.WriteScopeJSON, requiredRoles)
	if err != nil {
		return err
	}
	if hasCoveringRole {
		admission.ProjectRoleID = role.RoleID
		return nil
	}
	if len(requiredRoles) == 1 {
		roleType := strings.ToUpper(strings.TrimSpace(requiredRoles[0]))
		canAutoProvision := false
		switch {
		case roleType == ProjectRoleImplementer:
			// IMPLEMENTER bootstrap stays claim-mode gated (trust_first / owner-bound match).
			canAutoProvision = allowAutoProvision
		case roleType == ProjectRoleIntegrator || roleType == ProjectRoleReviewer:
			// A6 read==write parity (INTEGRATOR and the empirically-proven-symmetric REVIEWER): the read-frontier
			// (agentMaySelectProjectRoleLaneTask) OFFERS a role-routed integration/review task to a profile/
			// registered eligible owner REGARDLESS of claim mode, and the read-dry-run is skipped for non-
			// implementation lanes (agent_work_claimability.go: !projectTaskRequiresImplementationGate). In strict
			// mode allowAutoProvision is false for a role-routed (non-owner-bound) task, which would strand that
			// offer as offered-but-rejected - on the integration-unfreeze path (#4(b)) and on the review step one
			// earlier. Gate these on eligibility (the SAME projectClaimCanAutoProvisionNonImplementerRoleTx the read
			// uses), not on claim mode, so the claim honors what the read already promised. Both gaps were
			// reproduced by independent scratch repros (strict offered=true, admitted=false; trust_first admits).
			canAutoProvision, err = s.projectClaimCanAutoProvisionNonImplementerRoleTx(ctx, tx, workspaceID, projectID, agentID, taskCtx, requiredRoles)
			if err != nil {
				return err
			}
		case allowAutoProvision:
			// Any other future non-IMPLEMENTER role: existing claim-mode-gated auto-provision (backstop). The
			// eligibility predicate hard-refuses IMPLEMENTER, so this can never auto-grant an IMPLEMENTER role.
			canAutoProvision, err = s.projectClaimCanAutoProvisionNonImplementerRoleTx(ctx, tx, workspaceID, projectID, agentID, taskCtx, requiredRoles)
			if err != nil {
				return err
			}
		}
		if canAutoProvision {
			writeScopeJSON := strings.TrimSpace(admission.WriteScopeJSON)
			if roleType != ProjectRoleImplementer && writeScopeJSON == "" {
				writeScopeJSON = "{}"
			}
			normalizedScope, err := normalizeProjectRoleWriteScope(roleType, writeScopeJSON)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrTaskClaimAdmissionInvalid, err)
			}
			now := time.Now().UTC().Format(time.RFC3339Nano)
			role := ProjectRoleRecord{
				RoleID:         nextID("projrole"),
				WorkspaceID:    strings.TrimSpace(workspaceID),
				ProjectID:      projectID,
				AgentID:        strings.TrimSpace(agentID),
				RoleType:       roleType,
				Status:         ProjectRoleStatusActive,
				WriteScopeJSON: normalizedScope,
				Summary:        "Auto-provisioned by project task claim " + strings.TrimSpace(taskID),
				ClaimedAt:      now,
				UpdatedBy:      strings.TrimSpace(agentID),
				CreatedAt:      now,
				UpdatedAt:      now,
			}
			if roleType == ProjectRoleImplementer {
				if err := s.ensureProjectRoleImplementerScopeAvailableTx(ctx, tx, role); err != nil {
					return err
				}
			}
			if opts.DryRun {
				admission.ProjectRoleID = firstNonEmpty(strings.TrimSpace(admission.ProjectRoleID), "dry-run:auto-provision:"+roleType)
				admission.ProjectRoleAutoProvisioned = true
				return nil
			}
			if err := insertProjectRoleTx(ctx, tx, role); err != nil {
				return err
			}
			admission.ProjectRoleID = role.RoleID
			admission.ProjectRoleAutoProvisioned = true
			return nil
		}
	}
	if hasActiveRequiredRole {
		if len(requiredRoles) == 1 && strings.ToUpper(strings.TrimSpace(requiredRoles[0])) == ProjectRoleImplementer {
			return fmt.Errorf("%w: active IMPLEMENTER role for agent %s on project %s does not cover claim write_scope_json for task %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(agentID), projectID, strings.TrimSpace(taskID))
		}
		return fmt.Errorf("%w: active %s role for agent %s on project %s is not usable for task %s", ErrTaskClaimAdmissionInvalid, projectClaimRequiredRoleSummary(requiredRoles), strings.TrimSpace(agentID), projectID, strings.TrimSpace(taskID))
	}
	return fmt.Errorf("%w: project %s claim requires project_role_id for task %s", ErrTaskClaimAdmissionInvalid, projectClaimRequiredRoleSummary(requiredRoles), strings.TrimSpace(taskID))
}

func (s *Store) projectClaimCanAutoProvisionNonImplementerRoleTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, agentID string, taskCtx taskClaimProjectContext, requiredRoles []string) (bool, error) {
	if len(requiredRoles) == 0 {
		return false, nil
	}
	for _, role := range requiredRoles {
		if strings.ToUpper(strings.TrimSpace(role)) == ProjectRoleImplementer {
			return false, nil
		}
	}
	now := strings.TrimSpace(taskCtx.ReferenceAt)
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
	lead, hasActiveLead, err := s.getActiveProjectStrategicLeadTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), now)
	if err != nil {
		return false, fmt.Errorf("load active strategic lead for project role auto-provision: %w", err)
	}
	if hasActiveLead && strings.TrimSpace(lead.AgentID) == strings.TrimSpace(agentID) {
		return true, nil
	}
	allowedByRegisteredRole, err := s.projectClaimRegisteredAgentRoleAllowsLaneTx(ctx, tx, workspaceID, agentID, taskCtx.ProjectLane)
	if err != nil {
		return false, err
	}
	if allowedByRegisteredRole {
		return true, nil
	}
	agent, err := getAgentIdentityForProjectClaimRepairTx(ctx, tx, workspaceID, agentID)
	if err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("load agent identity for project role auto-provision: %w", err)
	}
	profile, err := s.getAgentProfileTx(ctx, tx, workspaceID, agentID)
	if err != nil {
		return false, err
	}
	profile = agentWorkProfileWithAgentFallback(profile, agent)
	return agentProfileAllowsProjectRoleLane(profile, requiredRoles), nil
}

func (s *Store) activeProjectImplementerRoleForClaimScopeTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, agentID, claimScopeJSON string) (ProjectRoleRecord, bool, bool, error) {
	return s.activeProjectRoleForClaimScopeTx(ctx, tx, workspaceID, projectID, agentID, claimScopeJSON, []string{ProjectRoleImplementer})
}

func (s *Store) activeProjectRoleForClaimScopeTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, agentID, claimScopeJSON string, requiredRoles []string) (ProjectRoleRecord, bool, bool, error) {
	claimPaths := projectBranchReviewScopePaths(claimScopeJSON)
	rows, err := tx.QueryContext(ctx, `
SELECT role_id, workspace_id, project_id, agent_id, role_type, status, write_scope_json, lease_token,
       lease_expires_at, summary, claimed_at, released_at, updated_by, created_at, updated_at
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND status = ?
 ORDER BY created_at DESC, role_id DESC`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(agentID),
		ProjectRoleStatusActive)
	if err != nil {
		return ProjectRoleRecord{}, false, false, fmt.Errorf("list active project roles for project task claim: %w", err)
	}
	defer rows.Close()
	hasActiveRequiredRole := false
	var firstActiveRole ProjectRoleRecord
	for rows.Next() {
		role, err := scanProjectRole(rows)
		if err != nil {
			return ProjectRoleRecord{}, false, false, fmt.Errorf("scan active project role for project task claim: %w", err)
		}
		roleType := strings.ToUpper(strings.TrimSpace(role.RoleType))
		if !projectClaimRoleTypeAllowed(roleType, requiredRoles) {
			continue
		}
		if firstActiveRole.RoleID == "" {
			firstActiveRole = role
		}
		if roleType != ProjectRoleImplementer {
			return role, true, true, nil
		}
		rolePaths := projectBranchReviewScopePaths(role.WriteScopeJSON)
		if len(rolePaths) == 0 {
			continue
		}
		hasActiveRequiredRole = true
		if writeScopePathsCoveredByWithAdjacentSidecars(claimPaths, rolePaths) {
			return role, true, true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return ProjectRoleRecord{}, false, false, fmt.Errorf("iterate active project roles for project task claim: %w", err)
	}
	return firstActiveRole, false, hasActiveRequiredRole, nil
}

func (s *Store) validateTaskClaimProjectRoleFitTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string, taskCtx taskClaimProjectContext) error {
	projectID := strings.TrimSpace(taskCtx.ProjectID)
	requiredRoles := projectClaimRequiredRoleTypesForLane(taskCtx.ProjectLane)
	if projectID == "" || len(requiredRoles) == 0 {
		return nil
	}
	now := strings.TrimSpace(taskCtx.ReferenceAt)
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
	lead, hasActiveLead, err := s.getActiveProjectStrategicLeadTx(ctx, tx, strings.TrimSpace(workspaceID), projectID, now)
	if err != nil {
		return fmt.Errorf("load active strategic lead for task claim fit: %w", err)
	}
	if hasActiveLead && strings.TrimSpace(lead.AgentID) == strings.TrimSpace(agentID) {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT agent_id, role_type, write_scope_json
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND status = ?`,
		strings.TrimSpace(workspaceID), projectID, ProjectRoleStatusActive)
	if err != nil {
		return fmt.Errorf("list project roles for task claim fit: %w", err)
	}
	defer rows.Close()

	hasActiveExecutionRoles := false
	agentHasRequiredRole := false
	for rows.Next() {
		var roleAgentID, roleType, writeScopeJSON string
		if err := rows.Scan(&roleAgentID, &roleType, &writeScopeJSON); err != nil {
			return fmt.Errorf("scan project role for task claim fit: %w", err)
		}
		roleType = strings.ToUpper(strings.TrimSpace(roleType))
		if roleType == ProjectRoleStrategicLead {
			continue
		}
		hasActiveExecutionRoles = true
		if strings.TrimSpace(roleAgentID) != strings.TrimSpace(agentID) || !projectClaimRoleTypeAllowed(roleType, requiredRoles) {
			continue
		}
		if roleType == ProjectRoleImplementer && len(writeScopePaths(writeScopeJSON)) == 0 {
			continue
		}
		agentHasRequiredRole = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate project roles for task claim fit: %w", err)
	}
	if agentHasRequiredRole {
		return nil
	}
	if !hasActiveExecutionRoles {
		allowedByRegisteredRole, err := s.projectClaimRegisteredAgentRoleAllowsLaneTx(ctx, tx, workspaceID, agentID, taskCtx.ProjectLane)
		if err != nil {
			return err
		}
		if allowedByRegisteredRole {
			return nil
		}
	}
	if !hasActiveLead && !hasActiveExecutionRoles && projectClaimRequiredRolesAllowBootstrap(requiredRoles) {
		return nil
	}
	if len(requiredRoles) == 1 && strings.ToUpper(strings.TrimSpace(requiredRoles[0])) == ProjectRoleImplementer {
		return fmt.Errorf("%w: project implementation task %s requires an active IMPLEMENTER role with write_scope_json for agent %s on project %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskID), strings.TrimSpace(agentID), projectID)
	}
	return fmt.Errorf("%w: project %s task %s requires active %s role for agent %s on project %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskCtx.ProjectLane), strings.TrimSpace(taskID), projectClaimRequiredRoleSummary(requiredRoles), strings.TrimSpace(agentID), projectID)
}

func projectClaimRequiredRolesAllowBootstrap(requiredRoles []string) bool {
	return len(requiredRoles) == 1 && strings.ToUpper(strings.TrimSpace(requiredRoles[0])) == ProjectRoleImplementer
}

func projectClaimRequiredRoleTypesForLane(projectLane string) []string {
	lane := strings.ToLower(strings.TrimSpace(projectLane))
	if lane == "" {
		return nil
	}
	if projectLaneRequiresImplementationGate(lane) {
		return []string{ProjectRoleImplementer}
	}
	switch lane {
	case "review", "reviewer":
		return []string{ProjectRoleReviewer}
	case "integration", "integrator", "integrate":
		return []string{ProjectRoleIntegrator}
	default:
		return nil
	}
}

func projectClaimRoleTypeAllowed(roleType string, allowed []string) bool {
	roleType = strings.ToUpper(strings.TrimSpace(roleType))
	for _, candidate := range allowed {
		if roleType == strings.ToUpper(strings.TrimSpace(candidate)) {
			return true
		}
	}
	return false
}

func projectClaimRequiredRoleSummary(roles []string) string {
	normalized := make([]string, 0, len(roles))
	for _, role := range roles {
		role = strings.ToUpper(strings.TrimSpace(role))
		if role != "" {
			normalized = append(normalized, role)
		}
	}
	if len(normalized) == 0 {
		return "PROJECT"
	}
	return strings.Join(normalized, "/")
}

func (s *Store) projectClaimRegisteredAgentRoleAllowsLaneTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID, projectLane string) (bool, error) {
	agent, err := getAgentIdentityForProjectClaimRepairTx(ctx, tx, workspaceID, agentID)
	if err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("load registered agent role for project lane fit: %w", err)
	}
	profile, err := s.getAgentProfileTx(ctx, tx, workspaceID, agentID)
	if err != nil {
		return false, err
	}
	return agentWorkProfileOrRegisteredRoleAllowsProjectLane(profile, agent, projectLane), nil
}

func projectClaimRegisteredRoleAllowsLane(projectLane, registeredRole string) bool {
	switch strings.ToLower(strings.TrimSpace(projectLane)) {
	case "integration", "integrator", "integrate":
		return projectRegisteredAgentRoleAllowsIntegrationLane(registeredRole)
	case "review", "reviewer":
		return projectRegisteredAgentRoleAllowsReviewLane(registeredRole)
	default:
		return false
	}
}

func (s *Store) taskProjectContextForClaimTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) (taskClaimProjectContext, error) {
	var taskCtx taskClaimProjectContext
	var requiresProjectGate int
	var row WorkspaceTaskRecord
	var tagsJSON, writeScopeHintsJSON string
	err := tx.QueryRowContext(ctx, `
SELECT t.task_id, COALESCE(t.title, ''), COALESCE(t.description, ''),
       COALESCE(t.project_id, ''), COALESCE(t.project_lane, ''), COALESCE(t.requires_project_gate, 0),
       COALESCE(t.task_requirements_json, '{}'), COALESCE(t.write_scope_hints_json, '[]'),
       COALESCE(t.tags_json, '[]')
  FROM workspace_tasks wt
  JOIN tasks t ON t.task_id = wt.task_id
 WHERE wt.workspace_id = ? AND wt.task_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(taskID)).Scan(
		&row.TaskID,
		&row.Title,
		&row.Description,
		&row.ProjectID,
		&row.ProjectLane,
		&requiresProjectGate,
		&row.TaskRequirementsJSON,
		&writeScopeHintsJSON,
		&tagsJSON,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return taskClaimProjectContext{}, ErrWorkspaceTaskAbsent
		}
		return taskClaimProjectContext{}, fmt.Errorf("load task project for claim admission: %w", err)
	}
	row.RequiresProjectGate = sqliteIntToBool(requiresProjectGate)
	row.TaskRequirementsJSON = normalizeTaskRequirementsJSON(row.TaskRequirementsJSON)
	row.WriteScopeHints = parseTaskWriteScopeHintsJSON(writeScopeHintsJSON)
	row.Tags = parseTaskTagsJSON(tagsJSON)
	taskCtx.ProjectID = strings.TrimSpace(row.ProjectID)
	taskCtx.ProjectLane = strings.ToLower(strings.TrimSpace(row.ProjectLane))
	taskCtx.RequiresProjectGate = row.RequiresProjectGate
	taskCtx.ReferenceAt = time.Now().UTC().Format(time.RFC3339Nano)
	if taskCtx.ProjectID == "" {
		return taskCtx, nil
	}
	if projectLaneRequiresImplementationGate(taskCtx.ProjectLane) {
		taskCtx.RequiresProjectGate = !agentWorkTaskBypassesImplementationGateByStructuredContract(row)
	}
	var repoRequired int
	err = tx.QueryRowContext(ctx, `
SELECT current_phase, repo_required, repo_status, COALESCE(repo_url, ''), COALESCE(design_doc_id, ''), COALESCE(implementation_plan_doc_id, '')
  FROM project_profiles
 WHERE workspace_id = ? AND project_id = ?`,
		strings.TrimSpace(workspaceID), taskCtx.ProjectID).Scan(&taskCtx.CurrentPhase, &repoRequired, &taskCtx.RepoStatus, &taskCtx.RepoURL, &taskCtx.DesignDocID, &taskCtx.ImplementationPlanDocID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return taskCtx, nil
		}
		return taskClaimProjectContext{}, fmt.Errorf("load project profile for claim admission: %w", err)
	}
	taskCtx.CurrentPhase = strings.ToUpper(strings.TrimSpace(taskCtx.CurrentPhase))
	taskCtx.RepoRequired = sqliteIntToBool(repoRequired)
	taskCtx.RepoStatus = strings.ToUpper(strings.TrimSpace(taskCtx.RepoStatus))
	taskCtx.RepoURL = strings.TrimSpace(taskCtx.RepoURL)
	taskCtx.DesignDocID = strings.TrimSpace(taskCtx.DesignDocID)
	taskCtx.ImplementationPlanDocID = strings.TrimSpace(taskCtx.ImplementationPlanDocID)
	if canonicalStatus, canonicalRemoteURL, ok, err := canonicalRepositoryProfileStatusForProjectTx(ctx, tx, workspaceID, taskCtx.ProjectID); err != nil {
		return taskClaimProjectContext{}, err
	} else if ok {
		taskCtx.RepoRequired = true
		taskCtx.RepoStatus = canonicalStatus
		taskCtx.RepoURL = strings.TrimSpace(canonicalRemoteURL)
	}
	return taskCtx, nil
}

func (s *Store) taskClaimOwnerBoundRequirementTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string, taskCtx taskClaimProjectContext) (agentWorkOwnerBoundRequirement, bool, error) {
	projectID := strings.TrimSpace(taskCtx.ProjectID)
	if projectID == "" {
		return agentWorkOwnerBoundRequirement{}, false, nil
	}
	var title, description, tagsJSON, taskTemplate, projectLane string
	err := tx.QueryRowContext(ctx, `
SELECT COALESCE(title, ''), COALESCE(description, ''), COALESCE(tags_json, '[]'), COALESCE(task_template, ''), COALESCE(project_lane, '')
  FROM tasks
 WHERE task_id = ?`,
		strings.TrimSpace(taskID),
	).Scan(&title, &description, &tagsJSON, &taskTemplate, &projectLane)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return agentWorkOwnerBoundRequirement{}, false, nil
		}
		return agentWorkOwnerBoundRequirement{}, false, fmt.Errorf("load owner-bound task metadata for claim admission: %w", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:       strings.TrimSpace(taskID),
		Title:        title,
		Description:  description,
		TaskTemplate: taskTemplate,
		Tags:         parseTaskTagsJSON(tagsJSON),
		ProjectID:    projectID,
		ProjectLane:  projectLane,
	}
	if !agentWorkTaskHasOwnerBoundSignal(task) {
		return agentWorkOwnerBoundRequirement{}, false, nil
	}
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	implicitKind := agentWorkImplicitOwnerBoundKind(task)
	req := agentWorkOwnerBoundRequirement{
		Kind:            firstNonEmpty(agentWorkTaskTagValue(task, "owner-bound-kind:", "owner-bound-kind=", "owner_bound_kind:", "owner_bound_kind="), implicitKind, "patch_queue_submit"),
		ProjectID:       projectID,
		QueueID:         firstNonEmpty(agentWorkTaskTagValue(task, "queue:", "queue=", "queue-id:", "queue-id=", "queue_id:", "queue_id="), queueID),
		ItemID:          firstNonEmpty(agentWorkTaskTagValue(task, "item:", "item=", "item-id:", "item-id=", "item_id:", "item_id="), itemID),
		BranchID:        firstNonEmpty(agentWorkTaskTagValue(task, "owner-branch:", "owner-branch=", "owner_branch:", "owner_branch=", "branch:", "branch=", "branch-id:", "branch-id=", "branch_id:", "branch_id="), branchID),
		BranchName:      firstNonEmpty(agentWorkTaskTextFieldValue([]string{task.Title, task.Description}, "branch_name"), agentWorkTaskTextFieldValue([]string{task.Title, task.Description}, "Branch name")),
		RequiredAgentID: agentWorkTaskTagValue(task, "required-agent:", "required-agent=", "required_agent:", "required_agent=", "required-agent-id:", "required-agent-id=", "owner-agent:", "owner-agent=", "owner_agent:", "owner_agent="),
	}
	if req.BranchID == "" && (req.QueueID != "" || req.ItemID != "") {
		if req.QueueID == "" || req.ItemID == "" {
			req.RepairNeeded = true
			req.Reason = "owner-bound patch queue reference must include both queue_id and item_id when branch_id is absent"
			return req, true, nil
		}
		if itemBranchID, ok, err := taskClaimOwnerBoundPatchQueueBranchTx(ctx, tx, workspaceID, projectID, req.QueueID, req.ItemID); err != nil {
			return agentWorkOwnerBoundRequirement{}, false, err
		} else if ok {
			req.BranchID = itemBranchID
		} else {
			req.RepairNeeded = true
			req.Reason = "owner-bound patch queue reference did not resolve to a branch"
			return req, true, nil
		}
	}
	if strings.EqualFold(strings.TrimSpace(req.Kind), "active_lane_publication") &&
		strings.TrimSpace(req.BranchID) == "" &&
		strings.TrimSpace(req.BranchName) == "" &&
		strings.TrimSpace(req.RequiredAgentID) == "" {
		branches, err := taskClaimOwnerBoundBranchesForProjectTx(ctx, tx, workspaceID, projectID)
		if err != nil {
			return agentWorkOwnerBoundRequirement{}, false, err
		}
		if branch, ok, ambiguous := agentWorkOwnerBoundUniqueOpenBranchForProject(branches); ok {
			req.BranchID = strings.TrimSpace(branch.BranchID)
			req.BranchName = strings.TrimSpace(branch.BranchName)
		} else if ambiguous {
			req.RepairNeeded = true
			req.Reason = "active-lane publication task matches multiple open project branches"
			return req, true, nil
		} else {
			req.RepairNeeded = true
			req.Reason = "active-lane publication task has no open project branch owner"
			return req, true, nil
		}
	}
	if strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") &&
		strings.TrimSpace(req.BranchID) == "" &&
		strings.TrimSpace(req.BranchName) == "" &&
		strings.TrimSpace(req.RequiredAgentID) != "" {
		branches, err := taskClaimOwnerBoundBranchesForOwnerTx(ctx, tx, workspaceID, projectID, req.RequiredAgentID)
		if err != nil {
			return agentWorkOwnerBoundRequirement{}, false, err
		}
		if branch, ok, ambiguous := agentWorkOwnerBoundUniqueOpenBranchForOwner(branches, req.RequiredAgentID); ok {
			req.BranchID = strings.TrimSpace(branch.BranchID)
			req.BranchName = strings.TrimSpace(branch.BranchName)
		} else if ambiguous {
			req.RepairNeeded = true
			req.Reason = fmt.Sprintf("owner-bound patch queue submit task matches multiple open branches for required agent %s", strings.TrimSpace(req.RequiredAgentID))
			return req, true, nil
		}
	}
	if strings.TrimSpace(req.BranchID) == "" && strings.TrimSpace(req.BranchName) == "" {
		branches, err := taskClaimOwnerBoundBranchesForProjectTx(ctx, tx, workspaceID, projectID)
		if err != nil {
			return agentWorkOwnerBoundRequirement{}, false, err
		}
		if branch, ok, ambiguous := agentWorkOwnerBoundBranchMentionedInTask(branches, task); ok {
			req.BranchID = strings.TrimSpace(branch.BranchID)
			req.BranchName = strings.TrimSpace(branch.BranchName)
		} else if ambiguous {
			req.RepairNeeded = true
			req.Reason = "owner-bound task mentions multiple registered branches"
			return req, true, nil
		}
	}
	if req.BranchID != "" {
		branch, err := getProjectBranchByIDTx(ctx, tx, req.BranchID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				req.RepairNeeded = true
				req.Reason = "owner-bound branch is not registered in project coordination"
				return req, true, nil
			}
			return agentWorkOwnerBoundRequirement{}, false, err
		}
		if strings.TrimSpace(branch.WorkspaceID) != strings.TrimSpace(workspaceID) || strings.TrimSpace(branch.ProjectID) != projectID {
			req.RepairNeeded = true
			req.Reason = "owner-bound branch belongs to a different workspace or project"
			return req, true, nil
		}
		req.BranchName = strings.TrimSpace(branch.BranchName)
		if owner := strings.TrimSpace(branch.AgentID); owner != "" {
			if taggedOwner := strings.TrimSpace(req.RequiredAgentID); taggedOwner != "" && taggedOwner != owner {
				req.RequiredAgentID = owner
				req.RepairNeeded = true
				req.Reason = fmt.Sprintf("owner-bound required agent %s conflicts with branch owner %s", taggedOwner, owner)
			} else {
				req.RequiredAgentID = owner
			}
		} else {
			req.RepairNeeded = true
			req.Reason = "owner-bound branch has no recorded owner"
		}
	} else if req.BranchName != "" {
		branch, ok, err := taskClaimOwnerBoundBranchByNameTx(ctx, tx, workspaceID, projectID, req.BranchName)
		if err != nil {
			return agentWorkOwnerBoundRequirement{}, false, err
		}
		if ok {
			req.BranchID = strings.TrimSpace(branch.BranchID)
			if owner := strings.TrimSpace(branch.AgentID); owner != "" {
				if taggedOwner := strings.TrimSpace(req.RequiredAgentID); taggedOwner != "" && taggedOwner != owner {
					req.RequiredAgentID = owner
					req.RepairNeeded = true
					req.Reason = fmt.Sprintf("owner-bound required agent %s conflicts with branch owner %s", taggedOwner, owner)
				} else {
					req.RequiredAgentID = owner
				}
			} else {
				req.RepairNeeded = true
				req.Reason = "owner-bound branch has no recorded owner"
			}
		}
	}
	if strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") && strings.TrimSpace(req.BranchID) == "" && !req.RepairNeeded {
		req.RepairNeeded = true
		req.Reason = "owner-bound patch queue submit task does not identify a concrete branch"
	}
	if strings.TrimSpace(req.RequiredAgentID) == "" && !req.RepairNeeded {
		req.RepairNeeded = true
		req.Reason = "owner-bound task does not identify a required agent"
	}
	return req, true, nil
}

func taskClaimOwnerBoundTerminalAcceptedTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string, req agentWorkOwnerBoundRequirement) (bool, error) {
	if !strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") || strings.TrimSpace(req.BranchID) == "" {
		return false, nil
	}
	branch, err := getProjectBranchByIDTx(ctx, tx, strings.TrimSpace(req.BranchID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if strings.TrimSpace(branch.WorkspaceID) != strings.TrimSpace(workspaceID) || strings.TrimSpace(branch.ProjectID) != strings.TrimSpace(projectID) {
		return false, nil
	}
	if !projectBranchStatusIsTerminal(branch.Status) || strings.TrimSpace(branch.HeadSHA) == "" {
		return false, nil
	}
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND project_id = ?
   AND branch_id = ?
   AND head_sha = ?
   AND state IN (?, ?)`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(branch.BranchID),
		strings.TrimSpace(branch.HeadSHA),
		ProjectPatchQueueStateAccepted,
		ProjectPatchQueueStateIntegrated,
	).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func taskClaimOwnerActiveImplementationSessionTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, agentID, sidecarTaskID string) (string, string, bool, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT s.session_id, COALESCE(s.status, ''), t.task_id, COALESCE(t.status, ''), COALESCE(t.project_lane, ''), COALESCE(tc.claim_status, ''), COALESCE(tc.agent_id, '')
  FROM agent_sessions s
  JOIN tasks t ON t.task_id = s.task_id
  JOIN workspace_tasks wt ON wt.workspace_id = s.workspace_id AND wt.task_id = s.task_id
  LEFT JOIN task_claims tc ON tc.workspace_id = s.workspace_id AND tc.task_id = s.task_id
 WHERE s.workspace_id = ?
   AND s.agent_id = ?
   AND COALESCE(t.project_id, '') = ?
   AND s.task_id <> ?
 ORDER BY s.started_at DESC, s.session_id DESC`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(agentID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(sidecarTaskID),
	)
	if err != nil {
		return "", "", false, fmt.Errorf("query active implementation session for owner-bound publication claim: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sessionID, sessionStatus, taskID, taskStatus, projectLane, claimStatus, claimAgentID string
		if err := rows.Scan(&sessionID, &sessionStatus, &taskID, &taskStatus, &projectLane, &claimStatus, &claimAgentID); err != nil {
			return "", "", false, fmt.Errorf("scan active implementation session for owner-bound publication claim: %w", err)
		}
		if isEndedAgentWorkSessionStatus(sessionStatus) || isTerminalTaskStatus(taskStatus) {
			continue
		}
		if strings.ToUpper(strings.TrimSpace(claimStatus)) != model.TaskClaimStatusClaimed || strings.TrimSpace(claimAgentID) != strings.TrimSpace(agentID) {
			continue
		}
		if !projectLaneRequiresImplementationGate(projectLane) {
			continue
		}
		return strings.TrimSpace(taskID), strings.TrimSpace(sessionID), true, nil
	}
	if err := rows.Err(); err != nil {
		return "", "", false, fmt.Errorf("iterate active implementation sessions for owner-bound publication claim: %w", err)
	}
	return "", "", false, nil
}

func taskClaimOwnerBoundPatchQueueBranchTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, queueID, itemID string) (string, bool, error) {
	query := `SELECT COALESCE(branch_id, '') FROM project_patch_queue_items WHERE workspace_id = ? AND project_id = ?`
	args := []any{strings.TrimSpace(workspaceID), strings.TrimSpace(projectID)}
	if queueID = strings.TrimSpace(queueID); queueID != "" {
		query += ` AND queue_id = ?`
		args = append(args, queueID)
	}
	if itemID = strings.TrimSpace(itemID); itemID != "" {
		query += ` AND item_id = ?`
		args = append(args, itemID)
	}
	query += ` ORDER BY updated_at DESC LIMIT 1`
	var branchID string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&branchID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("load owner-bound patch queue branch: %w", err)
	}
	branchID = strings.TrimSpace(branchID)
	return branchID, branchID != "", nil
}

func taskClaimOwnerBoundBranchesForOwnerTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, ownerAgentID string) ([]ProjectBranchRecord, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, active_task_id,
       active_claim_id, branch_name, branch_kind, base_branch, head_sha, base_sha,
       write_scope_json, review_doc_key, status, updated_by, created_at, updated_at
  FROM project_branch_registry
 WHERE workspace_id = ? AND project_id = ? AND agent_id = ?
 ORDER BY updated_at DESC, branch_id ASC`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(ownerAgentID),
	)
	if err != nil {
		return nil, fmt.Errorf("list owner-bound owner branches: %w", err)
	}
	defer rows.Close()
	var out []ProjectBranchRecord
	for rows.Next() {
		branch, err := scanProjectBranch(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, branch)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate owner-bound owner branches: %w", err)
	}
	return out, nil
}

func taskClaimOwnerBoundBranchesForProjectTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) ([]ProjectBranchRecord, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, active_task_id,
       active_claim_id, branch_name, branch_kind, base_branch, head_sha, base_sha,
       write_scope_json, review_doc_key, status, updated_by, created_at, updated_at
  FROM project_branch_registry
 WHERE workspace_id = ? AND project_id = ?
 ORDER BY updated_at DESC, branch_id ASC`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
	)
	if err != nil {
		return nil, fmt.Errorf("list owner-bound project branches: %w", err)
	}
	defer rows.Close()
	var out []ProjectBranchRecord
	for rows.Next() {
		branch, err := scanProjectBranch(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, branch)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate owner-bound project branches: %w", err)
	}
	return out, nil
}

func taskClaimOwnerBoundBranchByNameTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, branchName string) (ProjectBranchRecord, bool, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, active_task_id,
       active_claim_id, branch_name, branch_kind, base_branch, head_sha, base_sha,
       write_scope_json, review_doc_key, status, updated_by, created_at, updated_at
  FROM project_branch_registry
 WHERE workspace_id = ? AND project_id = ? AND branch_name = ?
 ORDER BY updated_at DESC, branch_id ASC`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(branchName))
	if err != nil {
		return ProjectBranchRecord{}, false, fmt.Errorf("load owner-bound branch by name: %w", err)
	}
	defer rows.Close()
	var live []ProjectBranchRecord
	var sawTerminal bool
	for rows.Next() {
		branch, err := scanProjectBranch(rows)
		if err != nil {
			return ProjectBranchRecord{}, false, err
		}
		if taskClaimRevisionSourceBranchStatusLive(branch.Status) {
			live = append(live, branch)
			continue
		}
		if projectBranchStatusIsTerminal(branch.Status) {
			sawTerminal = true
		}
	}
	if err := rows.Err(); err != nil {
		return ProjectBranchRecord{}, false, fmt.Errorf("iterate owner-bound branch by name: %w", err)
	}
	switch len(live) {
	case 0:
		if sawTerminal {
			return ProjectBranchRecord{}, false, fmt.Errorf("%w: branch_name %s only resolves to terminal branch registry rows", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(branchName))
		}
		return ProjectBranchRecord{}, false, nil
	case 1:
		return live[0], true, nil
	default:
		return ProjectBranchRecord{}, false, fmt.Errorf("%w: branch_name %s resolves to multiple live branches", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(branchName))
	}
}

func canonicalRepositoryProfileStatusForProjectTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) (string, string, bool, error) {
	var repoStatus, remoteURL string
	err := tx.QueryRowContext(ctx, `
SELECT repo_status, COALESCE(remote_url, '')
  FROM project_repositories
 WHERE workspace_id = ? AND project_id = ? AND is_canonical = 1 AND repo_status <> 'ARCHIVED'
 ORDER BY updated_at DESC, repo_id ASC
 LIMIT 1`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID)).Scan(&repoStatus, &remoteURL)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", false, nil
		}
		return "", "", false, fmt.Errorf("load canonical project repository for claim admission: %w", err)
	}
	return projectProfileRepoStatusFromRepositoryStatus(repoStatus), strings.TrimSpace(remoteURL), true, nil
}

func (c taskClaimProjectContext) projectAdmissionRequired() bool {
	if c.ProjectID == "" || !c.RequiresProjectGate || !c.RepoRequired || c.RepoStatus != ProjectRepoStatusReady {
		return false
	}
	switch c.CurrentPhase {
	case ProjectPhaseImplementation, ProjectPhaseReview, ProjectPhaseIntegration, ProjectPhaseValidation:
	default:
		return false
	}
	return projectLaneRequiresImplementationGate(c.ProjectLane)
}

func (c taskClaimProjectContext) projectRoleAdmissionRequired() bool {
	if strings.TrimSpace(c.ProjectID) == "" || !c.RequiresProjectGate {
		return false
	}
	requiredRoles := projectClaimRequiredRoleTypesForLane(c.ProjectLane)
	if len(requiredRoles) == 0 {
		return false
	}
	if projectLaneRequiresImplementationGate(c.ProjectLane) {
		return c.projectAdmissionRequired()
	}
	return true
}

func (s *Store) taskClaimProjectImplementationGateClosedTx(ctx context.Context, tx *sql.Tx, workspaceID string, c taskClaimProjectContext) (string, bool, error) {
	if strings.TrimSpace(c.ProjectID) == "" || !c.RequiresProjectGate || !projectLaneRequiresImplementationGate(c.ProjectLane) {
		return "", false, nil
	}
	if !projectPhaseAllowsImplementationWork(c.CurrentPhase) {
		return "implementation_phase_open: Project phase must be IMPLEMENTATION or later active delivery phase before implementation work", true, nil
	}
	if strings.TrimSpace(c.DesignDocID) == "" {
		return "design_doc_ready: Design document is required before implementation work", true, nil
	}
	if strings.TrimSpace(c.ImplementationPlanDocID) == "" {
		return "implementation_plan_ready: Implementation plan is required before implementation work", true, nil
	}
	repoStatus := strings.ToUpper(strings.TrimSpace(c.RepoStatus))
	if c.RepoRequired && repoStatus != ProjectRepoStatusReady && repoStatus != ProjectRepoStatusNotRequired {
		return "repo_ready_or_not_required: Repository must be ready or explicitly not required", true, nil
	}
	if c.RepoRequired && strings.TrimSpace(c.RepoURL) == "" && repoStatus != ProjectRepoStatusReady {
		return "repo_materialization_allowed: Repository materialization must be explicitly possible before code work", true, nil
	}
	referenceAt := firstNonEmpty(strings.TrimSpace(c.ReferenceAt), time.Now().UTC().Format(time.RFC3339Nano))
	if _, ok, err := s.getActiveProjectStrategicLeadTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(c.ProjectID), referenceAt); err != nil {
		return "", false, err
	} else if !ok {
		return "strategic_lead_active: Active strategic lead is required before implementation work", true, nil
	}
	return "", false, nil
}

func (s *Store) validateTaskClaimProjectRoleTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, agentID, roleID string) (ProjectRoleRecord, error) {
	role, err := s.getProjectRoleTx(ctx, tx, workspaceID, projectID, roleID)
	if err != nil {
		return ProjectRoleRecord{}, fmt.Errorf("%w: %v", ErrTaskClaimAdmissionInvalid, err)
	}
	if role.Status != ProjectRoleStatusActive {
		return ProjectRoleRecord{}, fmt.Errorf("%w: project_role_id %s status is %s", ErrTaskClaimAdmissionInvalid, roleID, role.Status)
	}
	if role.AgentID != strings.TrimSpace(agentID) {
		return ProjectRoleRecord{}, fmt.Errorf("%w: project_role_id %s belongs to agent %s", ErrTaskClaimAdmissionInvalid, roleID, role.AgentID)
	}
	return role, nil
}

func (s *Store) taskClaimCanReuseReviewReadyBranchTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID, branchID string) (bool, error) {
	var claimStatus string
	err := tx.QueryRowContext(ctx, `
SELECT claim_status
  FROM task_claims
 WHERE workspace_id = ?
   AND task_id = ?
   AND agent_id = ?
   AND branch_id = ?`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(taskID),
		strings.TrimSpace(agentID),
		strings.TrimSpace(branchID),
	).Scan(&claimStatus)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return taskClaimDescriptionReferencesBranchTx(ctx, tx, taskID, branchID)
		}
		return false, fmt.Errorf("load existing task claim for review-ready branch reuse: %w", err)
	}
	switch strings.TrimSpace(claimStatus) {
	case model.TaskClaimStatusClaimed, model.TaskClaimStatusBlocked, model.TaskClaimStatusReleased:
		return true, nil
	default:
		return taskClaimDescriptionReferencesBranchTx(ctx, tx, taskID, branchID)
	}
}

func taskClaimDescriptionReferencesBranchTx(ctx context.Context, tx *sql.Tx, taskID, branchID string) (bool, error) {
	branchID = strings.TrimSpace(branchID)
	if branchID == "" {
		return false, nil
	}
	var title, description, tagsJSON string
	err := tx.QueryRowContext(ctx, `
SELECT COALESCE(title, ''), COALESCE(description, ''), COALESCE(tags_json, '[]')
  FROM tasks
 WHERE task_id = ?`,
		strings.TrimSpace(taskID),
	).Scan(&title, &description, &tagsJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("load task text for review-ready branch reuse: %w", err)
	}
	if taskClaimTextFieldMatches([]string{title, description}, "branch_id", branchID) ||
		taskClaimTextFieldMatches([]string{title, description}, "Branch ID", branchID) {
		return true, nil
	}
	task := WorkspaceTaskRecord{
		TaskID:      strings.TrimSpace(taskID),
		Title:       title,
		Description: description,
		Tags:        parseTaskTagsJSON(tagsJSON),
	}
	_, _, parsedBranchID := agentWorkPatchQueueRefsFromTask(task)
	if strings.TrimSpace(parsedBranchID) == branchID {
		return true, nil
	}
	tagBranchID := agentWorkTaskTagValue(task, "owner-branch:", "owner-branch=", "owner_branch:", "owner_branch=", "branch:", "branch=", "branch-id:", "branch-id=", "branch_id:", "branch_id=")
	return strings.TrimSpace(tagBranchID) == branchID, nil
}

func taskClaimTextFieldMatches(texts []string, key, want string) bool {
	want = strings.Trim(strings.TrimSpace(want), "`'\"")
	if want == "" {
		return false
	}
	for _, text := range texts {
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "-*"))
			fieldKey, value, ok := strings.Cut(line, ":")
			if !ok || !strings.EqualFold(strings.TrimSpace(fieldKey), strings.TrimSpace(key)) {
				continue
			}
			if strings.Trim(strings.TrimSpace(value), "`'\"") == want {
				return true
			}
		}
	}
	return false
}

func taskClaimTextFieldValue(texts []string, key string) string {
	for _, text := range texts {
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "-*"))
			fieldKey, value, ok := strings.Cut(line, ":")
			if !ok || !strings.EqualFold(strings.TrimSpace(fieldKey), strings.TrimSpace(key)) {
				continue
			}
			if value = strings.Trim(strings.TrimSpace(value), "`'\""); value != "" {
				return value
			}
		}
	}
	return ""
}

func taskClaimTagSet(tagsJSON string) map[string]bool {
	tags := map[string]bool{}
	var raw []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(firstNonEmpty(tagsJSON, "[]"))), &raw); err != nil {
		return tags
	}
	for _, tag := range raw {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag != "" {
			tags[tag] = true
		}
	}
	return tags
}

func (s *Store) taskClaimCanRebindActiveBranchTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string, branch ProjectBranchRecord) (bool, error) {
	authorizedBranches, err := s.taskClaimRevisionSourceBranchesTx(ctx, tx, workspaceID, taskID, agentID, branch.RepoID)
	if err != nil {
		return false, err
	}
	if _, ok := authorizedBranches[strings.TrimSpace(branch.BranchID)]; !ok {
		return false, nil
	}
	refs := uniqueSortedStrings([]string{branch.ActiveTaskID, branch.ActiveClaimID})
	filteredRefs := make([]string, 0, len(refs))
	for _, refTaskID := range refs {
		if strings.TrimSpace(refTaskID) == strings.TrimSpace(taskID) {
			continue
		}
		filteredRefs = append(filteredRefs, refTaskID)
	}
	if len(filteredRefs) == 0 {
		return false, nil
	}
	for _, refTaskID := range filteredRefs {
		var refAgentID, refStatus, refBranchID string
		err := tx.QueryRowContext(ctx, `
SELECT agent_id, claim_status, COALESCE(branch_id, '')
  FROM task_claims
 WHERE workspace_id = ?
   AND task_id = ?`,
			strings.TrimSpace(workspaceID), refTaskID).Scan(&refAgentID, &refStatus, &refBranchID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			return false, fmt.Errorf("load active branch claim %s for rebind: %w", refTaskID, err)
		}
		if strings.TrimSpace(refAgentID) != strings.TrimSpace(agentID) {
			return false, nil
		}
		if strings.TrimSpace(refBranchID) != "" && strings.TrimSpace(refBranchID) != strings.TrimSpace(branch.BranchID) {
			return false, nil
		}
		if strings.TrimSpace(refStatus) != model.TaskClaimStatusBlocked {
			hasPatchQueueItem, err := taskClaimBranchHasPatchQueueItemTx(ctx, tx, workspaceID, branch.ProjectID, branch.BranchID)
			if err != nil {
				return false, err
			}
			if !hasPatchQueueItem {
				return false, nil
			}
		}
	}
	return true, nil
}

// taskClaimCanRebindActiveCheckoutTx reports whether a checkout's active reference
// may be rebound to a new task by the same agent (CA-04). It returns true only when
// the checkout has no other active task, or that task's claim is owned by the same
// agent and is terminal/BLOCKED (releasable). A different owner, or a same-agent
// task still RUNNING/CLAIMED, returns false so the active-conflict guard rejects the
// rebind instead of letting an agent steal a live sibling task's working tree.
func (s *Store) taskClaimCanRebindActiveCheckoutTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string, checkout ProjectCheckoutRecord) (bool, error) {
	refs := uniqueSortedStrings([]string{checkout.ActiveTaskID, checkout.ActiveClaimID})
	filteredRefs := make([]string, 0, len(refs))
	for _, refTaskID := range refs {
		refTaskID = strings.TrimSpace(refTaskID)
		if refTaskID == "" || refTaskID == strings.TrimSpace(taskID) {
			continue
		}
		filteredRefs = append(filteredRefs, refTaskID)
	}
	if len(filteredRefs) == 0 {
		return true, nil
	}
	for _, refTaskID := range filteredRefs {
		var refAgentID, refStatus string
		err := tx.QueryRowContext(ctx, `
SELECT agent_id, claim_status
  FROM task_claims
 WHERE workspace_id = ?
   AND task_id = ?`,
			strings.TrimSpace(workspaceID), refTaskID).Scan(&refAgentID, &refStatus)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// No claim row backing the stale active reference: not a live owner.
				continue
			}
			return false, fmt.Errorf("load active checkout claim %s for rebind: %w", refTaskID, err)
		}
		if strings.TrimSpace(refAgentID) != strings.TrimSpace(agentID) {
			return false, nil
		}
		if !isTerminalTaskClaimStatus(refStatus) && strings.TrimSpace(refStatus) != model.TaskClaimStatusBlocked {
			return false, nil
		}
	}
	return true, nil
}

func validateTaskClaimBranchBinding(branch ProjectBranchRecord, workspaceID, projectID, taskID, agentID string, allowReviewReadyReclaim, allowActiveRebind bool) error {
	if branch.WorkspaceID != strings.TrimSpace(workspaceID) || branch.ProjectID != strings.TrimSpace(projectID) {
		return fmt.Errorf("%w: branch_id %s belongs to workspace=%s project=%s", ErrTaskClaimAdmissionInvalid, branch.BranchID, branch.WorkspaceID, branch.ProjectID)
	}
	switch branch.Status {
	case ProjectBranchStatusReserved, ProjectBranchStatusActive:
	case ProjectBranchStatusReadyForReview:
		if !allowReviewReadyReclaim {
			return fmt.Errorf("%w: branch_id %s status is %s", ErrTaskClaimAdmissionInvalid, branch.BranchID, branch.Status)
		}
	default:
		return fmt.Errorf("%w: branch_id %s status is %s", ErrTaskClaimAdmissionInvalid, branch.BranchID, branch.Status)
	}
	if branch.AgentID != "" && branch.AgentID != strings.TrimSpace(agentID) {
		return fmt.Errorf("%w: branch_id %s belongs to agent %s", ErrTaskClaimAdmissionInvalid, branch.BranchID, branch.AgentID)
	}
	if branch.ActiveTaskID != "" && branch.ActiveTaskID != strings.TrimSpace(taskID) && !allowActiveRebind {
		return fmt.Errorf("%w: branch_id %s is active on task %s", ErrTaskClaimAdmissionInvalid, branch.BranchID, branch.ActiveTaskID)
	}
	if branch.ActiveClaimID != "" && branch.ActiveClaimID != strings.TrimSpace(taskID) && !allowActiveRebind {
		return fmt.Errorf("%w: branch_id %s is active on claim %s", ErrTaskClaimAdmissionInvalid, branch.BranchID, branch.ActiveClaimID)
	}
	return nil
}

func validateTaskClaimCheckoutBinding(checkout ProjectCheckoutRecord, workspaceID, projectID, taskID, agentID string, allowActiveRebind bool) error {
	if checkout.WorkspaceID != strings.TrimSpace(workspaceID) || checkout.ProjectID != strings.TrimSpace(projectID) {
		return fmt.Errorf("%w: checkout_id %s belongs to workspace=%s project=%s", ErrTaskClaimAdmissionInvalid, checkout.CheckoutID, checkout.WorkspaceID, checkout.ProjectID)
	}
	if checkout.Status != ProjectCheckoutStatusActive {
		return fmt.Errorf("%w: checkout_id %s status is %s", ErrTaskClaimAdmissionInvalid, checkout.CheckoutID, checkout.Status)
	}
	if checkout.AgentID != "" && checkout.AgentID != strings.TrimSpace(agentID) {
		return fmt.Errorf("%w: checkout_id %s belongs to agent %s", ErrTaskClaimAdmissionInvalid, checkout.CheckoutID, checkout.AgentID)
	}
	if checkout.ActiveTaskID != "" && checkout.ActiveTaskID != strings.TrimSpace(taskID) && !allowActiveRebind {
		return fmt.Errorf("%w: checkout_id %s is active on task %s", ErrTaskClaimAdmissionInvalid, checkout.CheckoutID, checkout.ActiveTaskID)
	}
	if checkout.ActiveClaimID != "" && checkout.ActiveClaimID != strings.TrimSpace(taskID) && !allowActiveRebind {
		return fmt.Errorf("%w: checkout_id %s is active on claim %s", ErrTaskClaimAdmissionInvalid, checkout.CheckoutID, checkout.ActiveClaimID)
	}
	dirtyState := strings.ToLower(strings.TrimSpace(checkout.DirtyState))
	sameTaskRef := strings.TrimSpace(checkout.ActiveTaskID) == strings.TrimSpace(taskID) || strings.TrimSpace(checkout.ActiveClaimID) == strings.TrimSpace(taskID)
	if dirtyState == "dirty" && !sameTaskRef {
		return fmt.Errorf("%w: checkout_id %s dirty_state is dirty without an existing same-task active reference", ErrTaskClaimAdmissionInvalid, checkout.CheckoutID)
	}
	return nil
}

func validateTaskClaimCheckoutMatchesBranch(checkout ProjectCheckoutRecord, branch ProjectBranchRecord) error {
	if strings.TrimSpace(branch.BranchID) == "" || strings.TrimSpace(checkout.CheckoutID) == "" {
		return nil
	}
	checkoutBranch := strings.TrimSpace(checkout.BranchName)
	if checkoutBranch == "" {
		return nil
	}
	branchName := strings.TrimSpace(branch.BranchName)
	if checkoutBranch == branchName {
		return validateTaskClaimCheckoutHeadMatchesBranch(checkout, branch)
	}
	return fmt.Errorf("%w: checkout_id %s is registered for branch_name %s, not branch %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(checkout.CheckoutID), checkoutBranch, branchName)
}

func validateTaskClaimCheckoutHeadMatchesBranch(checkout ProjectCheckoutRecord, branch ProjectBranchRecord) error {
	checkoutHead := strings.TrimSpace(checkout.HeadSHA)
	branchHead := strings.TrimSpace(branch.HeadSHA)
	if checkoutHead == "" || branchHead == "" || checkoutHead == branchHead {
		return nil
	}
	return fmt.Errorf("%w: checkout_id %s head_sha %s does not match branch_id %s head_sha %s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(checkout.CheckoutID), checkoutHead, strings.TrimSpace(branch.BranchID), branchHead)
}

func bindAdmissionRepo(admission *taskClaimProjectAdmissionRecord, repoID, source string) error {
	repoID = strings.TrimSpace(repoID)
	if repoID == "" {
		return nil
	}
	if admission.RepoID == "" {
		admission.RepoID = repoID
		return nil
	}
	if admission.RepoID != repoID {
		return fmt.Errorf("%w: %s repo_id %s conflicts with claim repo_id %s", ErrTaskClaimAdmissionInvalid, source, repoID, admission.RepoID)
	}
	return nil
}

func (s *Store) ensureTaskClaimWriteScopeAvailableTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string, admission taskClaimProjectAdmissionRecord) error {
	claimPaths := writeScopePaths(admission.WriteScopeJSON)
	if len(claimPaths) == 0 {
		return nil
	}
	allowedSourceBranches, err := s.taskClaimRevisionSourceBranchesTx(ctx, tx, workspaceID, taskID, agentID, admission.RepoID)
	if err != nil {
		return err
	}
	preserveTerminalPatchQueueScopes, err := s.taskClaimPreservesTerminalPatchQueueBranchScopesTx(ctx, tx, workspaceID, taskID)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `
SELECT tc.task_id, tc.agent_id, tc.branch_id, tc.write_scope_json,
       COALESCE(pb.branch_id, ''), COALESCE(pb.repo_id, ''), COALESCE(pb.agent_id, ''),
       COALESCE(pb.active_task_id, ''), COALESCE(pb.active_claim_id, ''),
       COALESCE(pb.head_sha, ''), COALESCE(pb.review_doc_key, ''),
       COALESCE(pb.status, ''), COALESCE(pb.write_scope_json, ''),
       COALESCE(at.status, ''), COALESCE(ac.claim_status, '')
  FROM task_claims tc
  LEFT JOIN project_branch_registry pb
    ON pb.workspace_id = tc.workspace_id
   AND pb.repo_id = tc.repo_id
   AND pb.branch_id = tc.branch_id
  LEFT JOIN tasks at
    ON at.task_id = pb.active_task_id
  LEFT JOIN task_claims ac
    ON ac.workspace_id = pb.workspace_id
   AND ac.task_id = pb.active_claim_id
   AND ac.branch_id = pb.branch_id
 WHERE tc.workspace_id = ?
   AND tc.repo_id = ?
   AND tc.task_id <> ?
   AND tc.claim_status = ?
   AND tc.write_scope_json <> ''`,
		strings.TrimSpace(workspaceID), admission.RepoID, strings.TrimSpace(taskID), model.TaskClaimStatusClaimed)
	if err != nil {
		return fmt.Errorf("scan live task claim write scopes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var otherTaskID, otherAgentID, otherBranchID, rawScope string
		var activeTaskStatus, activeClaimStatus string
		var branch ProjectBranchRecord
		if err := rows.Scan(
			&otherTaskID,
			&otherAgentID,
			&otherBranchID,
			&rawScope,
			&branch.BranchID,
			&branch.RepoID,
			&branch.AgentID,
			&branch.ActiveTaskID,
			&branch.ActiveClaimID,
			&branch.HeadSHA,
			&branch.ReviewDocKey,
			&branch.Status,
			&branch.WriteScopeJSON,
			&activeTaskStatus,
			&activeClaimStatus,
		); err != nil {
			return fmt.Errorf("scan live task claim write scope: %w", err)
		}
		if admission.BranchID != "" && strings.TrimSpace(otherBranchID) == admission.BranchID {
			continue
		}
		if _, ok := allowedSourceBranches[strings.TrimSpace(otherBranchID)]; ok && strings.TrimSpace(otherAgentID) == strings.TrimSpace(agentID) {
			continue
		}
		if strings.TrimSpace(branch.BranchID) != "" && projectBranchOwnsWriteScope(branch) {
			released, err := taskClaimTerminalPatchQueueBranchTerminalRefsReleaseScopeTx(ctx, tx, workspaceID, branch.BranchID, branch.HeadSHA, branch.ActiveTaskID, activeTaskStatus, branch.ActiveClaimID, activeClaimStatus)
			if err != nil {
				return err
			}
			if released {
				continue
			}
			if strings.TrimSpace(otherAgentID) == strings.TrimSpace(agentID) &&
				taskClaimBranchActiveRefsTerminalOrEmpty(branch.ActiveTaskID, activeTaskStatus, branch.ActiveClaimID, activeClaimStatus) {
				allowed, err := s.taskClaimAllowsOlderPatchQueueRevisionPredecessorBranchTx(ctx, tx, workspaceID, taskID, admission.RepoID, branch.BranchID, branch.HeadSHA)
				if err != nil {
					return err
				}
				if allowed {
					continue
				}
			}
		}
		effectiveScope := taskClaimEffectiveWriteScopeJSONFromLiveBranch(rawScope, otherTaskID, otherAgentID, otherBranchID, admission.RepoID, branch)
		if writeScopesOverlap(claimPaths, writeScopePaths(effectiveScope)) {
			return fmt.Errorf("%w: write_scope_json overlaps active claim task_id=%s branch_id=%s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(otherTaskID), strings.TrimSpace(otherBranchID))
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate live task claim write scopes: %w", err)
	}
	rows, err = tx.QueryContext(ctx, `
SELECT pb.branch_id, pb.active_task_id, pb.active_claim_id, pb.head_sha, pb.review_doc_key, pb.status, pb.write_scope_json, pb.agent_id,
       COALESCE(t.task_id, ''), COALESCE(t.project_lane, ''), COALESCE(t.requires_project_gate, 0),
       COALESCE(t.status, ''), COALESCE(tc.claim_status, '')
  FROM project_branch_registry pb
  LEFT JOIN tasks t ON t.task_id = pb.active_task_id
  LEFT JOIN task_claims tc ON tc.workspace_id = pb.workspace_id AND tc.task_id = pb.active_claim_id
 WHERE pb.workspace_id = ?
   AND pb.repo_id = ?
   AND pb.branch_id <> ?
   AND pb.status IN ('RESERVED', 'ACTIVE', 'BLOCKED', 'READY_FOR_REVIEW')
   AND pb.write_scope_json <> ''`,
		strings.TrimSpace(workspaceID), admission.RepoID, admission.BranchID)
	if err != nil {
		return fmt.Errorf("scan live project branch write scopes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var otherBranchID, otherTaskID, otherClaimID, headSHA, reviewDocKey, status, rawScope, otherAgentID, joinedTaskID, projectLane, joinedTaskStatus, joinedClaimStatus string
		var requiresProjectGate int
		if err := rows.Scan(&otherBranchID, &otherTaskID, &otherClaimID, &headSHA, &reviewDocKey, &status, &rawScope, &otherAgentID, &joinedTaskID, &projectLane, &requiresProjectGate, &joinedTaskStatus, &joinedClaimStatus); err != nil {
			return fmt.Errorf("scan live project branch write scope: %w", err)
		}
		if !projectBranchOwnsWriteScope(ProjectBranchRecord{
			BranchID:      otherBranchID,
			ActiveTaskID:  otherTaskID,
			ActiveClaimID: otherClaimID,
			HeadSHA:       headSHA,
			ReviewDocKey:  reviewDocKey,
			Status:        status,
		}) {
			continue
		}
		if taskClaimReservedNonImplementationBranchDoesNotOwnWriteScope(status, otherTaskID, joinedTaskID, projectLane, sqliteIntToBool(requiresProjectGate), headSHA, reviewDocKey) {
			continue
		}
		if strings.TrimSpace(otherTaskID) == strings.TrimSpace(taskID) {
			continue
		}
		if _, ok := allowedSourceBranches[strings.TrimSpace(otherBranchID)]; ok {
			continue
		}
		if strings.TrimSpace(otherAgentID) == strings.TrimSpace(agentID) &&
			taskClaimBranchActiveRefsTerminalOrEmpty(otherTaskID, joinedTaskStatus, otherClaimID, joinedClaimStatus) {
			allowed, err := s.taskClaimAllowsOlderPatchQueueRevisionPredecessorBranchTx(ctx, tx, workspaceID, taskID, admission.RepoID, otherBranchID, headSHA)
			if err != nil {
				return err
			}
			if allowed {
				continue
			}
		}
		if allowed, err := s.taskClaimAllowsTerminalPatchQueueRevisionSourceBranchTx(ctx, tx, workspaceID, taskID, admission.RepoID, otherBranchID, headSHA); err != nil {
			return err
		} else if allowed {
			continue
		}
		if released, err := taskClaimTerminalPatchQueueBranchTerminalRefsReleaseScopeTx(ctx, tx, workspaceID, otherBranchID, headSHA, otherTaskID, joinedTaskStatus, otherClaimID, joinedClaimStatus); err != nil {
			return err
		} else if released {
			continue
		}
		if !preserveTerminalPatchQueueScopes && strings.TrimSpace(otherTaskID) == "" && strings.TrimSpace(otherClaimID) == "" {
			released, err := taskClaimBranchHasTerminalPatchQueueDecisionTx(ctx, tx, workspaceID, otherBranchID, headSHA)
			if err != nil {
				return err
			}
			if released {
				continue
			}
		}
		if writeScopesOverlapExcludingSharedSidecars(claimPaths, writeScopePaths(rawScope)) {
			return fmt.Errorf("%w: write_scope_json overlaps live branch_id=%s active_task_id=%s", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(otherBranchID), strings.TrimSpace(otherTaskID))
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate live project branch write scopes: %w", err)
	}
	return nil
}

func taskClaimBranchHasPatchQueueItemTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, branchID string) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND project_id = ?
   AND branch_id = ?
   AND state <> ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(branchID), ProjectPatchQueueStateCanceled).Scan(&count); err != nil {
		return false, fmt.Errorf("query branch patch queue item for claim admission: %w", err)
	}
	return count > 0, nil
}

func taskClaimBranchActiveRefsTerminalOrEmpty(activeTaskID, activeTaskStatus, activeClaimID, activeClaimStatus string) bool {
	if strings.TrimSpace(activeTaskID) == "" && strings.TrimSpace(activeClaimID) == "" {
		return true
	}
	if strings.TrimSpace(activeTaskID) != "" && !isTerminalTaskStatus(activeTaskStatus) {
		return false
	}
	if strings.TrimSpace(activeClaimID) != "" && !taskClaimBranchActiveClaimRefInactive(activeClaimStatus) {
		return false
	}
	return true
}

func taskClaimBranchActiveClaimRefInactive(status string) bool {
	if strings.TrimSpace(status) == model.TaskClaimStatusReleased {
		return true
	}
	return isTerminalTaskClaimStatus(status)
}

func taskClaimTerminalPatchQueueBranchTerminalRefsReleaseScopeTx(ctx context.Context, tx *sql.Tx, workspaceID, branchID, headSHA, activeTaskID, activeTaskStatus, activeClaimID, activeClaimStatus string) (bool, error) {
	if released, err := taskClaimBranchHasOwnerSubmitBlockReceiptTx(ctx, tx, workspaceID, branchID, headSHA); err != nil || released {
		return released, err
	}
	if strings.TrimSpace(activeTaskID) == "" && strings.TrimSpace(activeClaimID) == "" {
		return false, nil
	}
	if !taskClaimBranchActiveRefsTerminalOrEmpty(activeTaskID, activeTaskStatus, activeClaimID, activeClaimStatus) {
		return false, nil
	}
	return taskClaimBranchHasTerminalPatchQueueDecisionTx(ctx, tx, workspaceID, branchID, headSHA)
}

func taskClaimBranchHasOwnerSubmitBlockReceiptTx(ctx context.Context, tx *sql.Tx, workspaceID, branchID, headSHA string) (bool, error) {
	branchID = strings.TrimSpace(branchID)
	if branchID == "" {
		return false, nil
	}
	if live, err := taskClaimBranchHasLivePatchQueueItemTx(ctx, tx, workspaceID, branchID, headSHA); err != nil || live {
		return false, err
	}
	rows, err := tx.QueryContext(ctx, `
SELECT t.task_id, COALESCE(t.title, ''), COALESCE(t.description, ''), COALESCE(t.task_kind, ''),
       COALESCE(t.task_template, ''), COALESCE(t.tags_json, '[]'), COALESCE(t.project_id, ''),
       COALESCE(t.project_lane, '')
  FROM tasks t
  JOIN runtime_events re
    ON re.workspace_id = ?
   AND re.task_id = t.task_id
   AND re.event_type = 'task.blocked'
   AND re.entity_type = 'task'
   AND re.entity_id = t.task_id
 WHERE t.task_id IN (
       SELECT task_id
         FROM workspace_tasks
        WHERE workspace_id = ?
          AND task_id = t.task_id
       )`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(workspaceID))
	if err != nil {
		return false, fmt.Errorf("check owner-submit block receipt scope release: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var task WorkspaceTaskRecord
		var tagsJSON string
		if err := rows.Scan(&task.TaskID, &task.Title, &task.Description, &task.TaskKind, &task.TaskTemplate, &tagsJSON, &task.ProjectID, &task.ProjectLane); err != nil {
			return false, fmt.Errorf("scan owner-submit block receipt scope release: %w", err)
		}
		task.Tags = parseTaskTagsJSON(tagsJSON)
		if !agentWorkTaskHasOwnerBoundSignal(task) {
			continue
		}
		_, _, taskBranchID := agentWorkPatchQueueRefsFromTask(task)
		kind := firstNonEmpty(agentWorkTaskTagValue(task, "owner-bound-kind:", "owner-bound-kind=", "owner_bound_kind:", "owner_bound_kind="), agentWorkImplicitOwnerBoundKind(task), "patch_queue_submit")
		taskBranchID = firstNonEmpty(agentWorkTaskTagValue(task, "owner-branch:", "owner-branch=", "owner_branch:", "owner_branch=", "branch:", "branch=", "branch-id:", "branch-id=", "branch_id:", "branch_id="), taskBranchID)
		if strings.EqualFold(strings.TrimSpace(kind), "patch_queue_submit") && strings.TrimSpace(taskBranchID) == branchID {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate owner-submit block receipt scope release: %w", err)
	}
	return false, nil
}

func taskClaimBranchHasLivePatchQueueItemTx(ctx context.Context, tx *sql.Tx, workspaceID, branchID, headSHA string) (bool, error) {
	branchID = strings.TrimSpace(branchID)
	if branchID == "" {
		return false, nil
	}
	args := []any{strings.TrimSpace(workspaceID), branchID, ProjectPatchQueueStateProposed, ProjectPatchQueueStateClaimed}
	headClause := ""
	if strings.TrimSpace(headSHA) != "" {
		headClause = " AND head_sha = ?"
		args = append(args, strings.TrimSpace(headSHA))
	}
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND branch_id = ?
   AND state IN (?, ?)`+headClause,
		args...,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("check live patch queue branch item: %w", err)
	}
	return count > 0, nil
}

func taskClaimReservedNonImplementationBranchDoesNotOwnWriteScope(status, activeTaskID, joinedTaskID, projectLane string, requiresProjectGate bool, headSHA, reviewDocKey string) bool {
	if strings.ToUpper(strings.TrimSpace(status)) != ProjectBranchStatusReserved {
		return false
	}
	if strings.TrimSpace(headSHA) != "" || strings.TrimSpace(reviewDocKey) != "" {
		return false
	}
	if strings.TrimSpace(activeTaskID) == "" {
		return false
	}
	if strings.TrimSpace(joinedTaskID) != strings.TrimSpace(activeTaskID) {
		return false
	}
	task := WorkspaceTaskRecord{
		TaskID:              strings.TrimSpace(activeTaskID),
		ProjectLane:         strings.TrimSpace(projectLane),
		RequiresProjectGate: requiresProjectGate,
	}
	return !projectTaskRequiresImplementationGate(task)
}

func (s *Store) taskClaimAllowsTerminalPatchQueueRevisionSourceBranchTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, repoID, branchID, headSHA string) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	repoID = strings.TrimSpace(repoID)
	branchID = strings.TrimSpace(branchID)
	headSHA = strings.TrimSpace(headSHA)
	if workspaceID == "" || taskID == "" || repoID == "" || branchID == "" || headSHA == "" {
		return false, nil
	}
	var title, description, tagsJSON, taskKind, taskTemplate, projectID, projectLane string
	err := tx.QueryRowContext(ctx, `
SELECT COALESCE(title, ''), COALESCE(description, ''), COALESCE(tags_json, '[]'),
       COALESCE(task_kind, ''), COALESCE(task_template, ''), COALESCE(project_id, ''),
       COALESCE(project_lane, '')
  FROM tasks
 WHERE task_id = ?`,
		taskID,
	).Scan(&title, &description, &tagsJSON, &taskKind, &taskTemplate, &projectID, &projectLane)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("load task metadata for terminal revision source branch allowance: %w", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:       taskID,
		Title:        title,
		Description:  description,
		TaskKind:     taskKind,
		TaskTemplate: taskTemplate,
		Tags:         parseTaskTagsJSON(tagsJSON),
		ProjectID:    projectID,
		ProjectLane:  projectLane,
	}
	if !agentWorkPatchQueueReplacementSupersessionCandidate(task) {
		return false, nil
	}
	queueID, itemID, taskBranchID := agentWorkPatchQueueRefsFromTask(task)
	texts := []string{title, description}
	taskHeadSHA := firstNonEmpty(taskClaimTextFieldValue(texts, "head_sha"), taskClaimTextFieldValue(texts, "head"))
	if strings.TrimSpace(taskBranchID) != "" && strings.TrimSpace(taskBranchID) != branchID {
		return false, nil
	}
	items, err := taskClaimPatchQueueRevisionCandidateItemsTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) != branchID || strings.TrimSpace(item.HeadSHA) != headSHA {
			continue
		}
		if taskClaimPatchQueueRevisionItemMatchesTask(task, item, repoID, queueID, itemID, branchID, taskHeadSHA) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) taskClaimAllowsOlderPatchQueueRevisionPredecessorBranchTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, repoID, branchID, headSHA string) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	repoID = strings.TrimSpace(repoID)
	branchID = strings.TrimSpace(branchID)
	headSHA = strings.TrimSpace(headSHA)
	if workspaceID == "" || taskID == "" || repoID == "" || branchID == "" || headSHA == "" {
		return false, nil
	}
	var title, description, tagsJSON, taskKind, taskTemplate, projectID, projectLane string
	err := tx.QueryRowContext(ctx, `
SELECT COALESCE(title, ''), COALESCE(description, ''), COALESCE(tags_json, '[]'),
       COALESCE(task_kind, ''), COALESCE(task_template, ''), COALESCE(project_id, ''),
       COALESCE(project_lane, '')
  FROM tasks
 WHERE task_id = ?`,
		taskID,
	).Scan(&title, &description, &tagsJSON, &taskKind, &taskTemplate, &projectID, &projectLane)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("load task metadata for older terminal revision predecessor allowance: %w", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:       taskID,
		Title:        title,
		Description:  description,
		TaskKind:     taskKind,
		TaskTemplate: taskTemplate,
		Tags:         parseTaskTagsJSON(tagsJSON),
		ProjectID:    projectID,
		ProjectLane:  projectLane,
	}
	if !agentWorkPatchQueueRevisionFollowupTask(task) {
		return false, nil
	}
	queueID, itemID, sourceBranchID := agentWorkPatchQueueRefsFromTask(task)
	sourceHeadSHA := agentWorkTaskTextFieldValue([]string{task.Title, task.Description}, "head_sha")
	items, err := taskClaimPatchQueueRevisionCandidateItemsTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return false, err
	}
	var source ProjectPatchQueueItemRecord
	sourceOK := false
	var predecessor ProjectPatchQueueItemRecord
	predecessorOK := false
	for _, item := range items {
		if strings.TrimSpace(item.RepoID) != repoID {
			continue
		}
		if strings.TrimSpace(item.BranchID) == branchID && strings.TrimSpace(item.HeadSHA) == headSHA {
			predecessor = item
			predecessorOK = true
			continue
		}
		if taskClaimPatchQueueRevisionItemMatchesTask(task, item, repoID, queueID, itemID, sourceBranchID, sourceHeadSHA) {
			if !sourceOK || agentWorkPatchQueueItemDecidedAfter(item, source) {
				source = item
				sourceOK = true
			}
		}
	}
	if !sourceOK || !predecessorOK {
		return false, nil
	}
	if !taskClaimPatchQueueRevisionPredecessorTerminal(predecessor) {
		return false, nil
	}
	if strings.TrimSpace(source.BranchID) == branchID {
		return false, nil
	}
	return agentWorkPatchQueueItemDecidedAfter(source, predecessor), nil
}

func (s *Store) taskClaimPreservesTerminalPatchQueueBranchScopesTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) (bool, error) {
	var title, description, tagsJSON, taskKind, taskTemplate, projectID, projectLane string
	err := tx.QueryRowContext(ctx, `
SELECT COALESCE(title, ''), COALESCE(description, ''), COALESCE(tags_json, '[]'),
       COALESCE(task_kind, ''), COALESCE(task_template, ''), COALESCE(project_id, ''),
       COALESCE(project_lane, '')
  FROM tasks
 WHERE task_id = ?`,
		strings.TrimSpace(taskID),
	).Scan(&title, &description, &tagsJSON, &taskKind, &taskTemplate, &projectID, &projectLane)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("load task metadata for terminal patch queue branch scope policy: %w", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:       strings.TrimSpace(taskID),
		Title:        title,
		Description:  description,
		TaskKind:     taskKind,
		TaskTemplate: taskTemplate,
		Tags:         parseTaskTagsJSON(tagsJSON),
		ProjectID:    projectID,
		ProjectLane:  projectLane,
	}
	return agentWorkPatchQueueReplacementSupersessionCandidate(task), nil
}

func taskClaimBranchHasTerminalPatchQueueDecisionTx(ctx context.Context, tx *sql.Tx, workspaceID, branchID, headSHA string) (bool, error) {
	branchID = strings.TrimSpace(branchID)
	headSHA = strings.TrimSpace(headSHA)
	if branchID == "" || headSHA == "" {
		return false, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT COALESCE(state, ''), COALESCE(head_sha, '')
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND branch_id = ?`,
		strings.TrimSpace(workspaceID), branchID,
	)
	if err != nil {
		return false, fmt.Errorf("check terminal patch queue branch decision: %w", err)
	}
	defer rows.Close()
	var items []ProjectPatchQueueItemRecord
	for rows.Next() {
		var state, itemHeadSHA string
		if err := rows.Scan(&state, &itemHeadSHA); err != nil {
			return false, fmt.Errorf("scan terminal patch queue branch decision: %w", err)
		}
		items = append(items, ProjectPatchQueueItemRecord{
			BranchID: branchID,
			State:    state,
			HeadSHA:  itemHeadSHA,
		})
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate terminal patch queue branch decisions: %w", err)
	}
	return projectPatchQueueItemsReleaseBranchWriteScope(branchID, headSHA, items), nil
}

func taskClaimBranchHasIntegratedPatchQueueDecisionTx(ctx context.Context, tx *sql.Tx, workspaceID, branchID, headSHA string) (bool, error) {
	branchID = strings.TrimSpace(branchID)
	headSHA = strings.TrimSpace(headSHA)
	if branchID == "" || headSHA == "" {
		return false, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT COALESCE(state, ''), COALESCE(head_sha, '')
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND branch_id = ?`,
		strings.TrimSpace(workspaceID), branchID,
	)
	if err != nil {
		return false, fmt.Errorf("check integrated patch queue branch decision: %w", err)
	}
	defer rows.Close()
	hasIntegratedHead := false
	for rows.Next() {
		var state, itemHeadSHA string
		if err := rows.Scan(&state, &itemHeadSHA); err != nil {
			return false, fmt.Errorf("scan integrated patch queue branch decision: %w", err)
		}
		switch strings.ToUpper(strings.TrimSpace(state)) {
		case ProjectPatchQueueStateProposed, ProjectPatchQueueStateClaimed:
			return false, nil
		case ProjectPatchQueueStateAccepted:
			if strings.TrimSpace(itemHeadSHA) == headSHA {
				return false, nil
			}
		case ProjectPatchQueueStateIntegrated:
			if strings.TrimSpace(itemHeadSHA) == headSHA {
				hasIntegratedHead = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate integrated patch queue branch decisions: %w", err)
	}
	return hasIntegratedHead, nil
}

func taskClaimEffectiveWriteScopeJSONFromLiveBranch(rawScope, otherTaskID, otherAgentID, otherBranchID, repoID string, branch ProjectBranchRecord) string {
	rawScope = strings.TrimSpace(rawScope)
	if !taskClaimBranchMatchesActiveClaim(branch, otherTaskID, otherAgentID, otherBranchID, repoID) {
		return rawScope
	}
	scopeJSON := strings.TrimSpace(branch.WriteScopeJSON)
	if len(writeScopePaths(scopeJSON)) == 0 {
		return rawScope
	}
	if !writeScopePathsSameSet(writeScopePaths(scopeJSON), writeScopePaths(rawScope)) {
		return rawScope
	}
	return scopeJSON
}

func taskClaimBranchMatchesActiveClaim(branch ProjectBranchRecord, otherTaskID, otherAgentID, otherBranchID, repoID string) bool {
	if strings.TrimSpace(otherBranchID) == "" || strings.TrimSpace(branch.BranchID) != strings.TrimSpace(otherBranchID) {
		return false
	}
	if strings.TrimSpace(repoID) == "" || strings.TrimSpace(branch.RepoID) != strings.TrimSpace(repoID) {
		return false
	}
	if !projectBranchOwnsWriteScope(branch) {
		return false
	}
	if branchAgentID := strings.TrimSpace(branch.AgentID); strings.TrimSpace(otherAgentID) != "" && branchAgentID != "" && branchAgentID != strings.TrimSpace(otherAgentID) {
		return false
	}
	activeTaskID := strings.TrimSpace(branch.ActiveTaskID)
	activeClaimID := strings.TrimSpace(branch.ActiveClaimID)
	taskID := strings.TrimSpace(otherTaskID)
	return activeTaskID == taskID || activeClaimID == taskID
}

func projectBranchOwnsWriteScope(branch ProjectBranchRecord) bool {
	switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
	case ProjectBranchStatusReserved, ProjectBranchStatusActive, ProjectBranchStatusBlocked:
		return strings.TrimSpace(branch.ActiveTaskID) != "" || strings.TrimSpace(branch.ActiveClaimID) != ""
	case ProjectBranchStatusReadyForReview:
		return strings.TrimSpace(branch.HeadSHA) != "" && strings.TrimSpace(branch.ReviewDocKey) != ""
	default:
		return false
	}
}

func (s *Store) taskClaimRevisionSourceBranchesTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID, repoID string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	repoID = strings.TrimSpace(repoID)
	if workspaceID == "" || taskID == "" || agentID == "" || repoID == "" {
		return out, nil
	}
	var title, description, tagsJSON, projectID, projectLane string
	err := tx.QueryRowContext(ctx, `
SELECT COALESCE(title, ''), COALESCE(description, ''), COALESCE(tags_json, '[]'),
       COALESCE(project_id, ''), COALESCE(project_lane, '')
  FROM tasks
 WHERE task_id = ?`,
		taskID,
	).Scan(&title, &description, &tagsJSON, &projectID, &projectLane)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return out, nil
		}
		return nil, fmt.Errorf("load task metadata for revision source branch allowance: %w", err)
	}
	projectID = strings.TrimSpace(projectID)
	if projectID == "" || !strings.EqualFold(strings.TrimSpace(projectLane), "implementation") {
		return out, nil
	}
	tags := taskClaimTagSet(tagsJSON)
	if !(tags["patch-queue"] || tags["patch_queue"]) || !tags["revision"] {
		return out, nil
	}
	task := WorkspaceTaskRecord{
		TaskID:      taskID,
		Title:       title,
		Description: description,
		Tags:        parseTaskTagsJSON(tagsJSON),
		ProjectID:   projectID,
		ProjectLane: projectLane,
	}
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	texts := []string{title, description}
	headSHA := firstNonEmpty(taskClaimTextFieldValue(texts, "head_sha"), taskClaimTextFieldValue(texts, "head"))
	candidates := map[string]struct{}{}
	if strings.TrimSpace(queueID) != "" && strings.TrimSpace(itemID) != "" {
		item, ok, err := taskClaimPatchQueueItemByIdentityTx(ctx, tx, workspaceID, projectID, queueID, itemID)
		if err != nil {
			return nil, err
		}
		if ok && taskClaimPatchQueueRevisionItemMatchesTask(task, item, repoID, queueID, itemID, branchID, headSHA) {
			candidates[strings.TrimSpace(item.BranchID)] = struct{}{}
		}
	} else {
		items, err := taskClaimPatchQueueRevisionCandidateItemsTx(ctx, tx, workspaceID, projectID)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if taskClaimPatchQueueRevisionItemMatchesTask(task, item, repoID, queueID, itemID, branchID, headSHA) {
				candidates[strings.TrimSpace(item.BranchID)] = struct{}{}
			}
		}
	}
	for branchID := range candidates {
		branch, err := getProjectBranchByIDTx(ctx, tx, branchID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, fmt.Errorf("load revision source branch %s: %w", branchID, err)
		}
		if strings.TrimSpace(branch.WorkspaceID) != workspaceID ||
			strings.TrimSpace(branch.ProjectID) != projectID ||
			strings.TrimSpace(branch.RepoID) != repoID ||
			strings.TrimSpace(branch.AgentID) != agentID ||
			!taskClaimRevisionSourceBranchStatusLive(branch.Status) {
			continue
		}
		out[branchID] = struct{}{}
	}
	return out, nil
}

func taskClaimPatchQueueRevisionCandidateItemsTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) ([]ProjectPatchQueueItemRecord, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items
 WHERE workspace_id = ? AND project_id = ?
   AND state IN (?, ?)
 ORDER BY updated_at DESC, item_id DESC`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		ProjectPatchQueueStateBlocked,
		ProjectPatchQueueStateRejected,
	)
	if err != nil {
		return nil, fmt.Errorf("query revision patch queue items: %w", err)
	}
	defer rows.Close()
	var items []ProjectPatchQueueItemRecord
	for rows.Next() {
		item, err := scanProjectPatchQueueItem(rows)
		if err != nil {
			return nil, fmt.Errorf("scan revision patch queue item: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate revision patch queue items: %w", err)
	}
	return items, nil
}

func taskClaimPatchQueueRevisionItemMatchesTask(task WorkspaceTaskRecord, item ProjectPatchQueueItemRecord, repoID, queueID, itemID, branchID, headSHA string) bool {
	if strings.TrimSpace(item.RepoID) != strings.TrimSpace(repoID) ||
		strings.TrimSpace(item.BranchID) == "" ||
		strings.TrimSpace(item.HeadSHA) == "" ||
		!taskClaimPatchQueueRevisionItemState(item.State) {
		return false
	}
	if strings.TrimSpace(queueID) != "" {
		if !strings.EqualFold(strings.TrimSpace(item.QueueID), strings.TrimSpace(queueID)) {
			return false
		}
	} else if !agentWorkPatchQueueTaskContainsRef(task, item.QueueID) {
		return false
	}
	if strings.TrimSpace(itemID) != "" {
		if !strings.EqualFold(strings.TrimSpace(item.ItemID), strings.TrimSpace(itemID)) {
			return false
		}
	} else if !agentWorkPatchQueueTaskContainsRef(task, item.ItemID) {
		return false
	}
	if strings.TrimSpace(branchID) != "" {
		if !strings.EqualFold(strings.TrimSpace(item.BranchID), strings.TrimSpace(branchID)) {
			return false
		}
	} else if !agentWorkPatchQueueTaskContainsRef(task, item.BranchID) {
		return false
	}
	if strings.TrimSpace(headSHA) != "" {
		return strings.EqualFold(strings.TrimSpace(item.HeadSHA), strings.TrimSpace(headSHA))
	}
	return agentWorkPatchQueueTaskContainsRef(task, item.HeadSHA)
}

func taskClaimPatchQueueItemByIdentityTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, queueID, itemID string) (ProjectPatchQueueItemRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items
 WHERE workspace_id = ? AND project_id = ? AND queue_id = ? AND item_id = ?
 ORDER BY updated_at DESC
 LIMIT 1`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(queueID),
		strings.TrimSpace(itemID),
	)
	item, err := scanProjectPatchQueueItem(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectPatchQueueItemRecord{}, false, nil
		}
		return ProjectPatchQueueItemRecord{}, false, fmt.Errorf("load revision patch queue item: %w", err)
	}
	return item, true, nil
}

func taskClaimPatchQueueRevisionItemState(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected:
		return true
	default:
		return false
	}
}

func taskClaimPatchQueueRevisionPredecessorTerminal(item ProjectPatchQueueItemRecord) bool {
	return taskClaimPatchQueueRevisionItemState(item.State)
}

func taskClaimRevisionSourceBranchStatusLive(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case ProjectBranchStatusReserved, ProjectBranchStatusActive, ProjectBranchStatusBlocked, ProjectBranchStatusReadyForReview:
		return true
	default:
		return false
	}
}

func writeScopePaths(raw string) []string {
	paths, invalid, ok := collectWriteScopePaths(raw)
	if !ok {
		return nil
	}
	if len(invalid) > 0 {
		// Fail closed for pre-existing malformed live scopes. New writes are
		// rejected by validateWriteScopeJSONPathset before they can persist, but
		// old/prose scopes must not bypass overlap checks.
		return []string{"**"}
	}
	return paths
}

func validateWriteScopeJSONPathset(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	paths, invalid, ok := collectWriteScopePaths(raw)
	if !ok {
		return nil, errors.New("write_scope_json must be valid JSON")
	}
	if len(invalid) > 0 {
		return nil, fmt.Errorf("write_scope_json paths must be repository-relative path globs, not prose: %s", strings.Join(invalid, ", "))
	}
	return paths, nil
}

func collectWriteScopePaths(raw string) ([]string, []string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || !json.Valid([]byte(raw)) {
		return nil, nil, false
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, nil, false
	}
	seen := map[string]struct{}{}
	var paths []string
	var invalid []string
	add := func(value string) {
		original := strings.TrimSpace(value)
		value = normalizeWriteScopePath(value)
		if value == "" {
			return
		}
		if reason := invalidWriteScopePathReason(original); reason != "" {
			invalid = append(invalid, fmt.Sprintf("%q (%s)", original, reason))
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		paths = append(paths, value)
	}
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case string:
			add(typed)
		case []any:
			for _, item := range typed {
				walk(item)
			}
		}
	}
	if object, ok := decoded.(map[string]any); ok {
		for _, key := range []string{"paths", "files", "path_prefixes", "write_paths", "scopes"} {
			walk(object[key])
		}
		return paths, invalid, true
	}
	walk(decoded)
	return paths, invalid, true
}

func invalidWriteScopePathReason(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, `\`, `/`))
	value = strings.Trim(value, "`\"'[]{} ")
	if value == "" || value == "." {
		return ""
	}
	value = strings.TrimPrefix(value, "./")
	switch {
	case strings.HasPrefix(value, "/"):
		return "absolute paths are not allowed"
	case strings.Contains(value, ":"):
		return "paths must be repo-relative and cannot contain ':'"
	case strings.Contains(value, ".."):
		return "parent-directory traversal is not allowed"
	case strings.IndexFunc(value, unicode.IsSpace) >= 0:
		return "whitespace usually indicates prose rather than a repo path"
	}
	if value == "*" || value == "**" || strings.ContainsAny(value, "/*.") {
		return ""
	}
	if writeScopeAllowedSingleSegment(value) {
		return ""
	}
	return "single-segment scope is ambiguous; use a repo path or glob such as src/**"
}

func writeScopeAllowedSingleSegment(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "agent", "api", "app", "assets", "cmd", "components", "data", "deploy", "docs", "examples", "internal", "lib", "pkg", "plans", "protocols", "public", "scripts", "spec", "src", "static", "styles", "tasks", "test", "tests", "tools", "ui", "web",
		"dockerfile", "makefile", "readme", "license":
		return true
	default:
		return false
	}
}

func normalizeWriteScopePath(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, `\`, `/`))
	value = strings.TrimSuffix(value, "/**")
	value = strings.TrimSuffix(value, "/*")
	value = strings.Trim(value, "/")
	if value == "." {
		return ""
	}
	return strings.ToLower(value)
}

func writeScopesOverlap(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	for _, a := range left {
		for _, b := range right {
			if writeScopePathOverlaps(a, b) {
				return true
			}
		}
	}
	return false
}

func writeScopesOverlapExcludingSharedSidecars(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	for _, a := range left {
		for _, b := range right {
			if !writeScopePathOverlaps(a, b) {
				continue
			}
			if writeScopeSharedSidecarPath(a) && writeScopeSharedSidecarPath(b) {
				continue
			}
			return true
		}
	}
	return false
}

func writeScopeSharedSidecarPath(value string) bool {
	path := normalizeWriteScopePath(value)
	switch path {
	case "go.mod", "go.sum", "go.work", "go.work.sum",
		"package.json", "package-lock.json", "npm-shrinkwrap.json", "pnpm-lock.yaml", "yarn.lock", "bun.lockb",
		"tsconfig.json", "jsconfig.json", "vite.config.js", "vite.config.ts", "vite.config.mjs", "vite.config.cjs",
		"webpack.config.js", "webpack.config.ts", "eslint.config.js", "eslint.config.mjs", "eslint.config.cjs",
		"index.html":
		return true
	default:
		return (strings.HasPrefix(path, "package") && strings.HasSuffix(path, ".json")) ||
			strings.HasPrefix(path, "tsconfig.") ||
			strings.HasPrefix(path, "vite.config.") ||
			strings.HasPrefix(path, "webpack.config.") ||
			strings.HasPrefix(path, "eslint.config.")
	}
}

func writeScopeGlobalTestFilePattern(value string) bool {
	switch normalizeWriteScopePath(value) {
	case "**/*_test.go", "*_test.go", "**/*test.go", "*test.go":
		return true
	default:
		return false
	}
}

func writeScopeConcreteTestFilePath(value string) bool {
	path := normalizeWriteScopePath(value)
	return path != "" &&
		!strings.Contains(path, "*") &&
		(strings.HasSuffix(path, "_test.go") || strings.HasSuffix(path, "test.go"))
}

func writeScopePathOverlaps(a, b string) bool {
	a = normalizeWriteScopePath(a)
	b = normalizeWriteScopePath(b)
	if a == "" || b == "" {
		return true
	}
	if a == "*" || a == "**" || b == "*" || b == "**" {
		return true
	}
	aGlobalTestPattern := writeScopeGlobalTestFilePattern(a)
	bGlobalTestPattern := writeScopeGlobalTestFilePattern(b)
	if aGlobalTestPattern || bGlobalTestPattern {
		if aGlobalTestPattern && bGlobalTestPattern {
			return true
		}
		if aGlobalTestPattern {
			return writeScopeConcreteTestFilePath(b)
		}
		return writeScopeConcreteTestFilePath(a)
	}
	aHasWildcard := strings.Contains(a, "*")
	bHasWildcard := strings.Contains(b, "*")
	if aHasWildcard && bHasWildcard {
		return writeScopeStaticPrefixesOverlap(writeScopeWildcardStaticPrefix(a), writeScopeWildcardStaticPrefix(b))
	}
	if aHasWildcard {
		return writeScopeWildcardMayOverlapPath(a, b)
	}
	if bHasWildcard {
		return writeScopeWildcardMayOverlapPath(b, a)
	}
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func writeScopeWildcardMayOverlapPath(pattern, pathValue string) bool {
	pattern = normalizeWriteScopePath(pattern)
	pathValue = normalizeWriteScopePath(pathValue)
	if pattern == "" || pathValue == "" {
		return true
	}
	if pattern == "*" || pattern == "**" || pathValue == "*" || pathValue == "**" {
		return true
	}
	staticPrefix := writeScopeWildcardStaticPrefix(pattern)
	if staticPrefix == "" {
		return true
	}
	return writeScopeStaticPrefixesOverlap(staticPrefix, pathValue)
}

func writeScopeWildcardStaticPrefix(value string) string {
	value = normalizeWriteScopePath(value)
	idx := strings.Index(value, "*")
	if idx < 0 {
		return value
	}
	prefix := value[:idx]
	prefix = strings.TrimSuffix(prefix, "/")
	return strings.Trim(prefix, "/")
}

func writeScopeStaticPrefixesOverlap(a, b string) bool {
	a = strings.Trim(strings.TrimSpace(a), "/")
	b = strings.Trim(strings.TrimSpace(b), "/")
	if a == "" || b == "" {
		return true
	}
	if a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/") {
		return true
	}
	if sameWriteScopeParentDir(a, b) && (strings.HasPrefix(a, b) || strings.HasPrefix(b, a)) {
		return true
	}
	return false
}

func sameWriteScopeParentDir(a, b string) bool {
	return writeScopeParentDir(a) == writeScopeParentDir(b)
}

func writeScopeParentDir(value string) string {
	value = strings.Trim(strings.TrimSpace(value), "/")
	if slash := strings.LastIndex(value, "/"); slash >= 0 {
		return value[:slash]
	}
	return ""
}

func (s *Store) bindTaskClaimProjectAdmissionTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string, admission taskClaimProjectAdmissionRecord, now string) error {
	if admission.BranchID != "" {
		if _, err := tx.ExecContext(ctx, `
UPDATE project_branch_registry
   SET agent_id = CASE WHEN agent_id = '' THEN ? ELSE agent_id END,
       active_task_id = ?,
       active_claim_id = ?,
       write_scope_json = CASE WHEN ? <> '' THEN ? ELSE write_scope_json END,
       status = CASE WHEN status = ? THEN ? ELSE status END,
       updated_by = ?,
       updated_at = ?
 WHERE branch_id = ?`,
			strings.TrimSpace(agentID), strings.TrimSpace(taskID), strings.TrimSpace(taskID),
			strings.TrimSpace(admission.WriteScopeJSON), strings.TrimSpace(admission.WriteScopeJSON),
			ProjectBranchStatusReserved, ProjectBranchStatusActive,
			strings.TrimSpace(agentID), strings.TrimSpace(now), admission.BranchID); err != nil {
			return fmt.Errorf("bind task claim branch admission: %w", err)
		}
	}
	if admission.CheckoutID != "" {
		if _, err := tx.ExecContext(ctx, `
UPDATE project_checkout_registry
   SET agent_id = CASE WHEN agent_id = '' THEN ? ELSE agent_id END,
       active_task_id = ?,
       active_claim_id = ?,
       updated_by = ?,
       updated_at = ?,
       last_seen_at = ?
 WHERE checkout_id = ?`,
			strings.TrimSpace(agentID), strings.TrimSpace(taskID), strings.TrimSpace(taskID),
			strings.TrimSpace(agentID), strings.TrimSpace(now), strings.TrimSpace(now), admission.CheckoutID); err != nil {
			return fmt.Errorf("bind task claim checkout admission: %w", err)
		}
	}
	return nil
}

func clearTaskClaimProjectAdmissionTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID, targetStatus, now string, snapshot taskClaimTransitionSnapshot, options taskClaimProjectAdmissionClearOptions) (map[string]any, error) {
	result := map[string]any{}
	branchID := strings.TrimSpace(snapshot.BranchID)
	checkoutID := strings.TrimSpace(snapshot.CheckoutID)
	if branchID != "" {
		if targetStatus == model.TaskClaimStatusCompleted {
			if !options.AllowReceiptBackedCompletedBranchRelease {
				if err := ensureTaskClaimCompletionCanReleaseProjectBranchTx(ctx, tx, workspaceID, taskID, agentID, branchID); err != nil {
					return nil, err
				}
			}
		}
		nextStatus := ""
		branchStatusSummary := "UNCHANGED_PENDING_REVIEW_EVIDENCE"
		switch targetStatus {
		case model.TaskClaimStatusCompleted:
			nextStatus = ""
			branchStatusSummary = ProjectBranchStatusReadyForReview
			if options.AllowReceiptBackedCompletedBranchRelease {
				branchStatusSummary = "UNCHANGED_RECEIPT_BACKED_COMPLETION"
			}
		case model.TaskClaimStatusReleased, model.TaskClaimStatusFailed, model.TaskClaimStatusCancelled:
			if options.PreserveBranchStatus {
				branchStatusSummary = "UNCHANGED_RECLAIM_RELEASE"
			} else {
				nextStatus = ProjectBranchStatusAbandoned
				branchStatusSummary = nextStatus
			}
		}
		if targetStatus == model.TaskClaimStatusCompleted || nextStatus != "" || options.PreserveBranchStatus {
			res, err := tx.ExecContext(ctx, `
UPDATE project_branch_registry
   SET active_task_id = '',
       active_claim_id = '',
       status = CASE WHEN ? <> '' AND status IN ('RESERVED', 'ACTIVE', 'BLOCKED') THEN ? ELSE status END,
       updated_by = ?,
       updated_at = ?
 WHERE workspace_id = ?
   AND branch_id = ?
   AND agent_id = ?
   AND (active_task_id = ? OR active_claim_id = ?)`,
				nextStatus, nextStatus, strings.TrimSpace(agentID), strings.TrimSpace(now),
				strings.TrimSpace(workspaceID), branchID, strings.TrimSpace(agentID),
				strings.TrimSpace(taskID), strings.TrimSpace(taskID))
			if err != nil {
				return nil, fmt.Errorf("clear task claim branch admission: %w", err)
			}
			if affected, _ := res.RowsAffected(); affected > 0 {
				result["branch_id"] = branchID
				result["branch_status"] = branchStatusSummary
				result["branch_active_refs_cleared"] = true
				if targetStatus == model.TaskClaimStatusCompleted {
					result["branch_review_evidence_required"] = false
					if options.AllowReceiptBackedCompletedBranchRelease {
						result["branch_completion_release_validation"] = "receipt_backed"
					}
				}
			}
		}
	}
	if checkoutID != "" {
		res, err := tx.ExecContext(ctx, `
UPDATE project_checkout_registry
   SET active_task_id = '',
       active_claim_id = '',
       updated_by = ?,
       updated_at = ?,
       last_seen_at = ?
 WHERE workspace_id = ?
   AND checkout_id = ?
   AND agent_id = ?
   AND (active_task_id = ? OR active_claim_id = ?)`,
			strings.TrimSpace(agentID), strings.TrimSpace(now), strings.TrimSpace(now),
			strings.TrimSpace(workspaceID), checkoutID, strings.TrimSpace(agentID),
			strings.TrimSpace(taskID), strings.TrimSpace(taskID))
		if err != nil {
			return nil, fmt.Errorf("clear task claim checkout admission: %w", err)
		}
		if affected, _ := res.RowsAffected(); affected > 0 {
			result["checkout_id"] = checkoutID
			result["checkout_active_refs_cleared"] = true
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	if snapshot.RepoID != "" {
		result["repo_id"] = snapshot.RepoID
	}
	if snapshot.WriteScopeJSON != "" {
		result["write_scope_json"] = snapshot.WriteScopeJSON
	}
	return result, nil
}

func ensureTaskClaimCompletionCanReleaseProjectBranchTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID, branchID string) error {
	var branch ProjectBranchRecord
	err := tx.QueryRowContext(ctx, `
SELECT branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, active_task_id,
       active_claim_id, branch_name, branch_kind, base_branch, head_sha, base_sha,
       write_scope_json, review_doc_key, status, updated_by, created_at, updated_at
  FROM project_branch_registry
 WHERE workspace_id = ? AND branch_id = ?
 LIMIT 1`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(branchID),
	).Scan(
		&branch.BranchID,
		&branch.WorkspaceID,
		&branch.ProjectID,
		&branch.RepoID,
		&branch.CheckoutID,
		&branch.AgentID,
		&branch.ActiveTaskID,
		&branch.ActiveClaimID,
		&branch.BranchName,
		&branch.BranchKind,
		&branch.BaseBranch,
		&branch.HeadSHA,
		&branch.BaseSHA,
		&branch.WriteScopeJSON,
		&branch.ReviewDocKey,
		&branch.Status,
		&branch.UpdatedBy,
		&branch.CreatedAt,
		&branch.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: branch_id %s not found before completion release", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(branchID))
		}
		return fmt.Errorf("load project branch before completion release: %w", err)
	}
	if strings.TrimSpace(branch.AgentID) != strings.TrimSpace(agentID) ||
		(strings.TrimSpace(branch.ActiveTaskID) != strings.TrimSpace(taskID) && strings.TrimSpace(branch.ActiveClaimID) != strings.TrimSpace(taskID)) {
		return fmt.Errorf("%w: branch_id %s is not actively bound to task %s for completion release", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(branchID), strings.TrimSpace(taskID))
	}
	if strings.ToUpper(strings.TrimSpace(branch.Status)) != ProjectBranchStatusReadyForReview {
		return fmt.Errorf("%w: completion of branch-backed task %s requires branch_id %s to be READY_FOR_REVIEW before active refs are released", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskID), strings.TrimSpace(branchID))
	}
	if strings.TrimSpace(branch.ReviewDocKey) == "" || strings.TrimSpace(branch.HeadSHA) == "" {
		return fmt.Errorf("%w: completion of branch-backed task %s requires READY_FOR_REVIEW review_doc_key and head_sha before active refs are released", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(taskID))
	}
	return nil
}

func addTaskClaimAdmissionPayload(payload map[string]any, admission taskClaimProjectAdmissionRecord) {
	if payload == nil || !admission.hasBindings() {
		return
	}
	if admission.ProjectRoleID != "" {
		payload["project_role_id"] = admission.ProjectRoleID
	}
	if admission.ProjectRoleAutoProvisioned {
		payload["project_role_auto_provisioned"] = true
	}
	if admission.RepoID != "" {
		payload["repo_id"] = admission.RepoID
	}
	if admission.CheckoutID != "" {
		payload["checkout_id"] = admission.CheckoutID
	}
	if admission.BranchID != "" {
		payload["branch_id"] = admission.BranchID
	}
	if admission.WriteScopeJSON != "" {
		payload["write_scope_json"] = admission.WriteScopeJSON
	}
}
