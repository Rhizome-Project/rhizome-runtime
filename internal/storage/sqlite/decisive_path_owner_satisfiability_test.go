package sqlite

import (
	"context"
	"database/sql"
	"testing"
)

// Stage-4 owner-satisfiability drift-guard matrix. decisivePathOwnerSatisfiabilityTx is the creation-time
// half of the read==write parity recipe; it MUST DELEGATE to the same claim-time predicates and agree with
// them across the modality matrix, or it drifts like R25. The FIRST case is the direct red->green regression
// for the empty-scope bug: before the hasActiveRequiredRole fix, a revision continuation (lane
// "implementation", agent-chosen scope passed as "") whose owner HOLDS an active IMPLEMENTER role was
// mis-classified AWAITING (writeScopePathsCoveredBy returns FALSE for an empty child), so revision would
// silently stop materializing. It must classify NOW.
func TestDecisivePathOwnerSatisfiabilityMatrix(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		ws        = "ws-stage4-owner-sat"
		project   = "project-stage4-owner-sat"
		integ     = "delta" // holds INTEGRATOR
		impl      = "gamma" // holds IMPLEMENTER scoped to internal/lexer/**
		bare      = "eta"   // registered agent, NO project role
		nonAgent  = "user-not-an-agent"
		implScope = `{"paths":["internal/lexer/lexer.go"]}`
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{WorkspaceID: ws, Title: ws, CreatedBy: "developer"}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, a := range []string{integ, impl, bare} {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{WorkspaceID: ws, AgentID: a, OwnerUserID: "developer", DisplayName: a}); err != nil {
			t.Fatalf("register agent %s: %v", a, err)
		}
	}
	claimTestWorkspaceAuthority(t, ctx, store, ws)
	if err := store.CreateProject(ctx, ProjectCreateInput{ProjectID: project, WorkspaceID: ws, Title: project, CreatedBy: "developer", ActorID: "developer", ActorType: "user"}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	assign := func(agent, role, scope string) {
		if _, _, err := store.AssignProjectRoleWithEvent(ctx, ProjectRoleAssignInput{
			WorkspaceID: ws, ProjectID: project, AgentID: agent, RoleType: role, WriteScopeJSON: scope,
			ActorID: "developer", ActorType: "user",
			PromptContextEnvelope: BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", ws, "user", "developer"),
			PromptContextSurface:  "project.role.assign",
		}); err != nil {
			t.Fatalf("assign %s %s: %v", agent, role, err)
		}
	}
	assign(integ, ProjectRoleIntegrator, "")
	assign(impl, ProjectRoleImplementer, implScope)

	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	cases := []struct {
		name               string
		owner, lane, scope string
		wantModality       string
		wantOwner          string // "" = don't assert resolved owner
	}{
		{"#6-revision: IMPLEMENTER-holding owner, agent-chosen scope -> NOW (regression)", impl, "implementation", "", ownerSatisfiabilityNow, impl},
		{"fixed-scope IMPLEMENTER covered -> NOW", impl, "implementation", implScope, ownerSatisfiabilityNow, impl},
		{"fixed-scope IMPLEMENTER uncovered -> AWAITING", impl, "implementation", `{"paths":["internal/parser/parser.go"]}`, ownerSatisfiabilityAwaitingRole, ""},
		{"registered owner, no IMPLEMENTER role, agent-chosen scope -> AWAITING", bare, "implementation", "", ownerSatisfiabilityAwaitingRole, ""},
		{"non-agent-user owner, no lane role gate (R29 #4(a)) -> NEVER", nonAgent, "coordination", "", ownerSatisfiabilityNever, nonAgent},
		{"system owner, no lane role gate -> NEVER", "system", "coordination", "", ownerSatisfiabilityNever, "system"},
		{"registered agent owner, no lane role gate -> NOW", bare, "coordination", "", ownerSatisfiabilityNow, bare},
		{"integration lane, INTEGRATOR holder exists -> NOW(holder)", "system", "integration", "", ownerSatisfiabilityNow, integ},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			modality, owner, err := store.decisivePathOwnerSatisfiabilityTx(ctx, tx, ws, project, tc.owner, tc.lane, tc.scope)
			if err != nil {
				t.Fatalf("owner-satisfiability: %v", err)
			}
			if modality != tc.wantModality {
				t.Fatalf("modality = %q, want %q", modality, tc.wantModality)
			}
			if tc.wantOwner != "" && owner != tc.wantOwner {
				t.Fatalf("resolved owner = %q, want %q", owner, tc.wantOwner)
			}
		})
	}

	// Parity pin (checklist #7): the birth-time IMPLEMENTER decision delegates to the SAME claim-time fn, so
	// NOW iff claim admission would admit. Lock it: for the covered/uncovered fixed-scope cases the predicate's
	// modality must track activeProjectRoleForClaimScopeTx's fits, and the same fn is the one claim admission
	// uses via projectClaimRequiredRoleTypesForLane(lane) - asserted here, not by grep.
	if roles := projectClaimRequiredRoleTypesForLane("implementation"); len(roles) != 1 || roles[0] != ProjectRoleImplementer {
		t.Fatalf("claim+birth share lane->role source: implementation must require [IMPLEMENTER], got %v", roles)
	}
	_, fitsCovered, hasRole, err := store.activeProjectRoleForClaimScopeTx(ctx, tx, ws, project, impl, implScope, []string{ProjectRoleImplementer})
	if err != nil {
		t.Fatalf("claim-time fit (covered): %v", err)
	}
	if !fitsCovered || !hasRole {
		t.Fatalf("claim-time must admit the covered IMPLEMENTER owner (fits=%v hasRole=%v); birth-time said NOW - they must agree", fitsCovered, hasRole)
	}
	_, fitsUncovered, _, err := store.activeProjectRoleForClaimScopeTx(ctx, tx, ws, project, impl, `{"paths":["internal/parser/parser.go"]}`, []string{ProjectRoleImplementer})
	if err != nil {
		t.Fatalf("claim-time fit (uncovered): %v", err)
	}
	if fitsUncovered {
		t.Fatalf("claim-time must REJECT the uncovered IMPLEMENTER scope; birth-time said AWAITING - they must agree")
	}
}
