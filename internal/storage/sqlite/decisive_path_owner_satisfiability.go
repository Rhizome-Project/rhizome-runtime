package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// decisivePathOwnerSatisfiabilityTx resolves the tri-state owner-satisfiability MODALITY for an owner-bound
// carrier at birth (stage 4). It is the creation-time half of the read==write parity recipe (3ec326e1) and
// must DELEGATE - not re-implement - the predicates claim admission uses, or it drifts exactly like R25
// (the gap the recipe exists to close). It therefore reuses:
//   - projectClaimRequiredRoleTypesForLane (the SAME lane->required-role mapping claim admission uses), and
//   - activeProjectRoleForClaimScopeTx (the SAME per-agent role+scope fit fn claim admission uses at :316),
//     so an IMPLEMENTER lane's WRITE-SCOPE coverage is evaluated identically at birth and at claim. A raw
//     role_type-only check would say NOW for a scoped-IMPLEMENTER carrier whose owner does not cover the
//     scope, while claim admission rejects it -> mint-claimable-but-nobody-can-claim = an R25-class loop.
//
// Modalities (the route fn maps each):
//   - role-routed lane (INTEGRATOR/REVIEWER, non-scoped): an active holder exists -> NOW (owner = holder);
//     none -> AWAITING_ROLE (the role is acquirable; defer + a BOUNDED, sweep-driven re-attempt).
//   - IMPLEMENTER-scoped lane (owner_user_id hard-bound): the proposed owner must hold an IMPLEMENTER role
//     covering carrierScope (delegated check). Fits -> NOW; cannot satisfy now -> AWAITING_ROLE (fail-safe:
//     defer, never a false NOW; the bounded sweep escalates a never-resolving entry to typed-terminal).
//   - no lane role gate: a registered agent owner -> NOW; "system" / non-agent workspace-user owner (the R29
//     claim-repair carrier) -> NEVER.
//
// Only CarrierKind-stamped carriers are consulted (ordinary user-owned operator tasks are not carriers).
func (s *Store) decisivePathOwnerSatisfiabilityTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, proposedOwner, projectLane, carrierScopeJSON string) (modality string, resolvedOwner string, err error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	proposedOwner = strings.TrimSpace(proposedOwner)

	roles := projectClaimRequiredRoleTypesForLane(projectLane)

	// No lane role gate -> owner_user_id hard binding. Satisfiable iff the owner is a real agent.
	if len(roles) == 0 {
		if proposedOwner == "" || strings.EqualFold(proposedOwner, "system") {
			return ownerSatisfiabilityNever, proposedOwner, nil
		}
		registered, err := s.agentRegisteredTx(ctx, tx, workspaceID, proposedOwner)
		if err != nil {
			return "", proposedOwner, err
		}
		if registered {
			return ownerSatisfiabilityNow, proposedOwner, nil
		}
		return ownerSatisfiabilityNever, proposedOwner, nil
	}

	// Scoped IMPLEMENTER lane: the OWNER must hold an IMPLEMENTER role covering carrierScope. DELEGATE the
	// exact claim-time role+scope fit so birth and claim cannot diverge on write-scope coverage.
	if projectLaneRequiresImplementationGate(projectLane) {
		if proposedOwner == "" || strings.EqualFold(proposedOwner, "system") {
			return ownerSatisfiabilityNever, proposedOwner, nil
		}
		registered, err := s.agentRegisteredTx(ctx, tx, workspaceID, proposedOwner)
		if err != nil {
			return "", proposedOwner, err
		}
		if !registered {
			return ownerSatisfiabilityNever, proposedOwner, nil
		}
		_, fits, hasRole, err := s.activeProjectRoleForClaimScopeTx(ctx, tx, workspaceID, projectID, proposedOwner, carrierScopeJSON, roles)
		if err != nil {
			return "", proposedOwner, err
		}
		// Discriminate FIXED-at-birth scope vs AGENT-CHOSEN scope, delegating to the SAME claim-time fn:
		//   - non-empty carrierScope = a fixed scope the carrier pins -> the owner's role must COVER it: fits.
		//   - empty carrierScope = the claim scope is the agent's own choice at claim time (e.g. a revision
		//     continuation whose candidate_pathset is explicitly "not_claim_scope") -> holding an active
		//     IMPLEMENTER role with a non-empty scope (hasActiveRequiredRole) is the precise predictor: the
		//     agent will claim within a scope its role covers, so role-ownership is sufficient -> NOW.
		// CRITICAL: writeScopePathsCoveredBy returns FALSE for an empty child, so "" can NEVER use fits (that
		// would mis-classify every revision continuation with a role-holding owner as AWAITING -> defer ->
		// TTL-terminal -> revision silently stops materializing). The discriminator is which scope is fixed.
		satisfied := fits
		if strings.TrimSpace(carrierScopeJSON) == "" {
			satisfied = hasRole
		}
		if satisfied {
			return ownerSatisfiabilityNow, proposedOwner, nil
		}
		// Registered owner that cannot satisfy the scoped role now: never a false NOW. Defer; the bounded
		// sweep re-checks and escalates to typed-terminal if it never resolves.
		return ownerSatisfiabilityAwaitingRole, proposedOwner, nil
	}

	// Role-routed lane (INTEGRATOR / REVIEWER, non-scoped): owner = an active holder of the required role.
	holder, ok, err := s.activeProjectRoleHolderTx(ctx, tx, workspaceID, projectID, roles[0])
	if err != nil {
		return "", proposedOwner, err
	}
	if ok {
		return ownerSatisfiabilityNow, holder, nil
	}
	return ownerSatisfiabilityAwaitingRole, "", nil
}

// activeProjectRoleHolderTx returns an active holder agent of the given project role in lease-prefer order.
// This is THE shared role-holder finder: stage 4's S4.3 re-points the inline integration-owner lookup in
// projectPatchQueueDecisionContinuationOwnerTx (projects_patch_queue.go:5493) at it so creation-side
// owner-resolution is one query, not two. ok=false means the role is currently unheld (acquirable ->
// awaiting_role). It reads project_agent_roles, the SAME table claim-time role admission reads.
func (s *Store) activeProjectRoleHolderTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, roleType string) (string, bool, error) {
	var agentID string
	err := tx.QueryRowContext(ctx, `
SELECT agent_id
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND status = ? AND role_type = ?
 ORDER BY CASE WHEN lease_expires_at = '' THEN 0 ELSE 1 END, updated_at DESC, agent_id ASC
 LIMIT 1`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		ProjectRoleStatusActive,
		strings.ToUpper(strings.TrimSpace(roleType)),
	).Scan(&agentID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("select active project role holder: %w", err)
	}
	agentID = strings.TrimSpace(agentID)
	return agentID, agentID != "", nil
}

func (s *Store) agentRegisteredTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID string) (bool, error) {
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(agentID)).Scan(&n); err != nil {
		return false, fmt.Errorf("check agent registration for owner satisfiability: %w", err)
	}
	return n > 0, nil
}
