package sqlite_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestClaimTaskTrustFirstAutoProvisionsIntegratorRoleForGatedIntegrationTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-integration-role-autoprovision"
		projectID   = "project-claim-integration-role-autoprovision"
		leadID      = "alpha"
		agentID     = "zeta"
		repoID      = "repo-main"
		taskID      = "task-integrate-accepted-candidate"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, agentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	setAgentRegisteredRoleForProjectAdmissionTest(t, ctx, store, workspaceID, agentID, "integrator")
	createGatedProjectRoleLaneTask(t, ctx, store, workspaceID, projectID, taskID, "integration", "Integrate accepted patch queue candidate")

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               agentID,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim accepted candidate integration follow-up",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, agentID),
	}); err != nil {
		t.Fatalf("claim gated integration task: %v", err)
	}

	var roleID, repoBinding, branchBinding, scopeBinding string
	if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(project_role_id, ''), COALESCE(repo_id, ''), COALESCE(branch_id, ''), COALESCE(write_scope_json, '')
  FROM task_claims
 WHERE workspace_id = ? AND task_id = ?`,
		workspaceID, taskID).Scan(&roleID, &repoBinding, &branchBinding, &scopeBinding); err != nil {
		t.Fatalf("load task claim bindings: %v", err)
	}
	if strings.TrimSpace(roleID) == "" {
		t.Fatal("integration task claim did not bind an auto-provisioned project role")
	}
	if repoBinding != "" || branchBinding != "" || scopeBinding != "" {
		t.Fatalf("integration role claim should not require implementation branch bindings, got repo=%q branch=%q scope=%q", repoBinding, branchBinding, scopeBinding)
	}

	var roleType, roleStatus, roleScope string
	if err := store.DB().QueryRowContext(ctx, `
SELECT role_type, status, COALESCE(write_scope_json, '')
  FROM project_agent_roles
 WHERE role_id = ?`,
		roleID).Scan(&roleType, &roleStatus, &roleScope); err != nil {
		t.Fatalf("load auto-provisioned role: %v", err)
	}
	if roleType != sqlite.ProjectRoleIntegrator || roleStatus != sqlite.ProjectRoleStatusActive || roleScope != "{}" {
		t.Fatalf("unexpected auto-provisioned role: type=%q status=%q scope=%q", roleType, roleStatus, roleScope)
	}
}

func TestClaimTaskTrustFirstRejectsUnfitIntegrationRoleAutoProvision(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-integration-role-reject-unfit"
		projectID   = "project-claim-integration-role-reject-unfit"
		leadID      = "alpha"
		agentID     = "beta"
		repoID      = "repo-main"
		taskID      = "task-integrate-unfit-candidate"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, agentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	setAgentRegisteredRoleForProjectAdmissionTest(t, ctx, store, workspaceID, agentID, "builder")
	createGatedProjectRoleLaneTask(t, ctx, store, workspaceID, projectID, taskID, "integration", "Integrate accepted patch queue candidate")

	_, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               agentID,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim accepted candidate integration follow-up",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, agentID),
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "requires project_role_id") {
		t.Fatalf("expected unfit integration claim to require explicit INTEGRATOR role, got %v", err)
	}

	var count int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND role_type = ?`,
		workspaceID, projectID, agentID, sqlite.ProjectRoleIntegrator).Scan(&count); err != nil {
		t.Fatalf("count unexpected integrator roles: %v", err)
	}
	if count != 0 {
		t.Fatalf("unfit trust_first claim auto-provisioned %d integrator roles", count)
	}
}

func TestClaimTaskIntegrateAliasRequiresIntegratorRole(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-integrate-alias-role"
		projectID   = "project-claim-integrate-alias-role"
		leadID      = "alpha"
		agentID     = "zeta"
		repoID      = "repo-main"
		taskID      = "task-integrate-alias-candidate"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, agentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	setAgentRegisteredRoleForProjectAdmissionTest(t, ctx, store, workspaceID, agentID, "integrator")
	createGatedProjectRoleLaneTask(t, ctx, store, workspaceID, projectID, taskID, "integrate", "Integrate accepted patch queue candidate")

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               agentID,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim accepted candidate integration follow-up",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, agentID),
	}); err != nil {
		t.Fatalf("claim integrate alias task: %v", err)
	}

	var roleType string
	if err := store.DB().QueryRowContext(ctx, `
SELECT par.role_type
  FROM task_claims tc
  JOIN project_agent_roles par ON par.role_id = tc.project_role_id
 WHERE tc.workspace_id = ? AND tc.task_id = ?`,
		workspaceID, taskID).Scan(&roleType); err != nil {
		t.Fatalf("load integrate alias role binding: %v", err)
	}
	if roleType != sqlite.ProjectRoleIntegrator {
		t.Fatalf("integrate alias claim bound role_type=%q, want INTEGRATOR", roleType)
	}
}

func TestClaimTaskGatedIntegrationTaskRejectsImplementerRoleBinding(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-integration-reject-implementer"
		projectID   = "project-claim-integration-reject-implementer"
		leadID      = "alpha"
		agentID     = "beta"
		repoID      = "repo-main"
		taskID      = "task-integrate-with-wrong-role"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, agentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createGatedProjectRoleLaneTask(t, ctx, store, workspaceID, projectID, taskID, "integration", "Integrate accepted patch queue candidate")
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, agentID, leadID, `{"paths":["cmd/**"]}`)

	var implementerRoleID string
	if err := store.DB().QueryRowContext(ctx, `
SELECT role_id
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND role_type = ? AND status = ?`,
		workspaceID, projectID, agentID, sqlite.ProjectRoleImplementer, sqlite.ProjectRoleStatusActive).Scan(&implementerRoleID); err != nil {
		t.Fatalf("load implementer role: %v", err)
	}

	_, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               agentID,
		ProjectRoleID:         implementerRoleID,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim integration with wrong role",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, agentID),
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "requires INTEGRATOR") {
		t.Fatalf("expected integration claim to reject IMPLEMENTER role binding, got %v", err)
	}
}

func setAgentRegisteredRoleForProjectAdmissionTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, role string) {
	t.Helper()

	result, err := store.DB().ExecContext(ctx, `
UPDATE agents
   SET role = ?
 WHERE workspace_id = ? AND agent_id = ?`,
		role, workspaceID, agentID)
	if err != nil {
		t.Fatalf("set registered agent role: %v", err)
	}
	if rows, err := result.RowsAffected(); err != nil {
		t.Fatalf("read registered role update rows: %v", err)
	} else if rows != 1 {
		t.Fatalf("set registered agent role affected %d rows, want 1", rows)
	}
}

func createGatedProjectRoleLaneTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID, lane, title string) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               title,
		Description:         "Patch queue decision follow-up requiring the lane role before execution.",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "patch_queue", lane, "decision_continuation"},
		ProjectID:           projectID,
		ProjectLane:         lane,
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create gated %s task: %v", lane, err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach gated %s task: %v", lane, err)
	}
}
