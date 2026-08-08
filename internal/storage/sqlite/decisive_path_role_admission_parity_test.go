package sqlite

import (
	"context"
	"database/sql"
	"testing"
)

// TestProjectClaimRoleRoutedAutoProvisionReadWriteParity is the read==write drift-guard for the role-routed
// auto-provision lanes (INTEGRATOR + the proven-symmetric REVIEWER). The read-frontier
// (agentMaySelectProjectRoleLaneTask) and the claim's auto-provision leaf
// (projectClaimCanAutoProvisionNonImplementerRoleTx) are SEPARATE functions that share leaves but can drift on
// composition - the A6 gap was exactly such a drift (read offered, strict claim rejected). For an agent with NO
// active project role and NO other active execution roles (the eligibility regime), OFFERED (read) MUST equal
// ADMITTED (claim auto-provision) across the eligibility cells - else a role-routed integration/review task is
// offered-but-rejected (the freeze this unit clears) or stranded-claimable. This is the same drift-lock medicine
// the S4.2 owner-satisfiability matrix applied to birth==claim parity.
func TestProjectClaimRoleRoutedAutoProvisionReadWriteParity(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		ws      = "ws-role-routed-parity"
		project = "project-role-routed-parity"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{WorkspaceID: ws, Title: ws, CreatedBy: "developer"}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, ws)
	if err := store.CreateProject(ctx, ProjectCreateInput{ProjectID: project, WorkspaceID: ws, Title: project, CreatedBy: "developer", ActorID: "developer", ActorType: "user"}); err != nil {
		t.Fatalf("create project: %v", err)
	}

	cases := []struct {
		name         string
		lane         string
		role         string
		registeredAs string // agent.Role at registration
		want         bool   // expected offered==admitted value
	}{
		{"integration registered-eligible", "integration", ProjectRoleIntegrator, "integrator", true},
		{"integration non-eligible", "integration", ProjectRoleIntegrator, "worker", false},
		{"review registered-eligible", "review", ProjectRoleReviewer, "reviewer", true},
		{"review non-eligible", "review", ProjectRoleReviewer, "worker", false},
	}
	for i, tc := range cases {
		tc := tc
		i := i
		t.Run(tc.name, func(t *testing.T) {
			agentID := "agent-" + string(rune('a'+i))
			if err := store.RegisterAgent(ctx, AgentRegisterInput{WorkspaceID: ws, AgentID: agentID, OwnerUserID: "developer", DisplayName: agentID, Role: tc.registeredAs}); err != nil {
				t.Fatalf("register agent: %v", err)
			}
			// No project role is assigned and no other execution roles exist, so both sides evaluate their
			// eligibility leaf for the SAME (agent, lane).
			task := WorkspaceTaskRecord{TaskID: "task-" + agentID, ProjectID: project, ProjectLane: tc.lane}
			offered, err := store.agentMaySelectProjectRoleLaneTask(ctx, ws, agentID, task)
			if err != nil {
				t.Fatalf("read-frontier offer: %v", err)
			}
			tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatalf("begin read tx: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			// "admitted" exercises the FULL claim role-binding COMPOSITION in STRICT, role-routed mode
			// (allowAutoProvision=false - the exact A6 gap condition), not just the shared leaf. DryRun avoids the
			// role INSERT. If the INTEGRATOR/REVIEWER eligibility-gating regresses to mode-gating, an eligible
			// agent is rejected here while the read still offers -> this matrix fails. That is the drift-lock.
			admission := &taskClaimProjectAdmissionRecord{}
			bindErr := store.ensureTaskClaimProjectRoleBindingTx(ctx, tx, ws, task.TaskID, agentID, taskClaimProjectContext{ProjectID: project, ProjectLane: tc.lane}, admission, ProjectRoleRecord{}, false /* strict + role-routed: allowAutoProvision=false */, taskClaimProjectAdmissionOptions{DryRun: true})
			admitted := bindErr == nil
			if offered != admitted {
				t.Fatalf("read==claim PARITY BROKEN for %s: read offered=%v but STRICT claim admitted=%v (bindErr=%v) - a role-routed task would be offered-but-rejected or stranded", tc.name, offered, admitted, bindErr)
			}
			if offered != tc.want {
				t.Fatalf("%s: expected offered==admitted==%v, got %v", tc.name, tc.want, offered)
			}
		})
	}
}
