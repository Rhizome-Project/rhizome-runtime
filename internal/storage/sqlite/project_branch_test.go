package sqlite_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectBranchRegistry(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-branch-registry"
		projectID   = "project-branch-registry"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-branch-slice"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["src/**","tests/**","docs/**"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE tasks
   SET write_scope_hints_json = ?,
       task_requirements_json = ?
 WHERE task_id = ?`,
		`["src/**","tests/**"]`, `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`, taskID); err != nil {
		t.Fatalf("scope initial branch task: %v", err)
	}
	checkout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\project-branch-registry`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		BranchName:            "agent/worker-agent/project-branch-registry",
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register checkout: %v", err)
	}

	branch, event, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/project-branch-registry",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["src/**","tests/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if event.EventType != "project.branch.registered" || branch.BranchID == "" || branch.AgentID != workerID || branch.ActiveTaskID != "" {
		t.Fatalf("unexpected branch register branch=%+v event=%+v", branch, event)
	}
	claimEvent, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        `{"paths":["src/**","tests/**"]}`,
		Summary:               "claim branch-backed implementation slice",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	})
	if err != nil {
		t.Fatalf("claim task with branch admission: %v", err)
	}
	if !containsForTest(claimEvent.PayloadJSON, branch.BranchID) || !containsForTest(claimEvent.PayloadJSON, checkout.CheckoutID) {
		t.Fatalf("claim event should include branch/checkout admission evidence, got %s", claimEvent.PayloadJSON)
	}

	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list project branches: %v", err)
	}
	if len(branches) != 1 || branches[0].BranchID != branch.BranchID || branches[0].CheckoutID != checkout.CheckoutID || branches[0].ActiveTaskID != taskID || branches[0].Status != sqlite.ProjectBranchStatusActive {
		t.Fatalf("unexpected project branch list: %+v", branches)
	}
	checkouts, err := store.ListProjectCheckouts(ctx, sqlite.ProjectCheckoutListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list project checkouts: %v", err)
	}
	if len(checkouts) != 1 || checkouts[0].ActiveTaskID != taskID || checkouts[0].ActiveClaimID != taskID {
		t.Fatalf("expected claim admission to bind checkout active refs, got %+v", checkouts)
	}
	_, err = store.CompleteTaskWithEvent(ctx, sqlite.TaskCompleteInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Summary:               "branch-backed implementation slice complete",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.complete", workspaceID, taskID, workerID),
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "READY_FOR_REVIEW") {
		t.Fatalf("expected completion before READY_FOR_REVIEW to be rejected, got %v", err)
	}
	branches, err = store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list project branches after rejected complete: %v", err)
	}
	if len(branches) != 1 || branches[0].ActiveTaskID != taskID || branches[0].ActiveClaimID != taskID || branches[0].Status != sqlite.ProjectBranchStatusActive {
		t.Fatalf("expected rejected completion to preserve branch active refs, got %+v", branches)
	}
	checkouts, err = store.ListProjectCheckouts(ctx, sqlite.ProjectCheckoutListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list project checkouts after rejected complete: %v", err)
	}
	if len(checkouts) != 1 || checkouts[0].ActiveTaskID != taskID || checkouts[0].ActiveClaimID != taskID {
		t.Fatalf("expected rejected completion to preserve checkout active refs, got %+v", checkouts)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET status = ?, review_doc_key = ?, head_sha = ?, base_sha = ?
 WHERE workspace_id = ? AND branch_id = ?`,
		sqlite.ProjectBranchStatusReadyForReview,
		"doc-ready",
		"0123456789abcdef0123456789abcdef01234567",
		"1111111111111111111111111111111111111111",
		workspaceID,
		branch.BranchID,
	); err != nil {
		t.Fatalf("mark branch ready for completion guard: %v", err)
	}
	completeEvent, err := store.CompleteTaskWithEvent(ctx, sqlite.TaskCompleteInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Summary:               "branch-backed implementation slice review-ready",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.complete", workspaceID, taskID, workerID),
	})
	if err != nil {
		t.Fatalf("complete review-ready branch-backed task: %v", err)
	}
	if !containsForTest(completeEvent.PayloadJSON, `"project_admission_transition"`) || !containsForTest(completeEvent.PayloadJSON, `"branch_review_evidence_required":false`) {
		t.Fatalf("complete event should include review-ready project admission lifecycle transition, got %s", completeEvent.PayloadJSON)
	}
	branches, err = store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list project branches after review-ready complete: %v", err)
	}
	if len(branches) != 1 || branches[0].ActiveTaskID != "" || branches[0].ActiveClaimID != "" || branches[0].Status != sqlite.ProjectBranchStatusReadyForReview || branches[0].ReviewDocKey == "" {
		t.Fatalf("expected review-ready completion to clear branch active refs and preserve review evidence, got %+v", branches)
	}
	checkouts, err = store.ListProjectCheckouts(ctx, sqlite.ProjectCheckoutListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list project checkouts after review-ready complete: %v", err)
	}
	if len(checkouts) != 1 || checkouts[0].ActiveTaskID != "" || checkouts[0].ActiveClaimID != "" {
		t.Fatalf("expected review-ready completion to clear checkout active refs, got %+v", checkouts)
	}
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["docs/**"]}`)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-branch-next")
	if _, err := store.DB().ExecContext(ctx, `
UPDATE tasks
   SET write_scope_hints_json = ?,
       task_requirements_json = ?
 WHERE task_id = ?`,
		`["docs/**"]`, `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`, "task-branch-next"); err != nil {
		t.Fatalf("scope next task: %v", err)
	}
	nextBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/project-branch-next",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["docs/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register next branch on completed checkout: %v", err)
	}
	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\project-branch-registry`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		BranchName:            "agent/worker-agent/project-branch-next",
		DirtyState:            "clean",
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	}); err != nil {
		t.Fatalf("refresh checkout branch before next claim: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-branch-next",
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              nextBranch.BranchID,
		WriteScopeJSON:        `{"paths":["docs/**"]}`,
		Summary:               "claim next branch-backed implementation slice",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-branch-next", workerID),
	}); err != nil {
		t.Fatalf("claim next task after completion should reuse cleared checkout: %v", err)
	}
	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	claimedTask := findWorkspaceTaskForBranchTest(t, tasks, taskID)
	if claimedTask.ClaimBranchID == nil || *claimedTask.ClaimBranchID != branch.BranchID || claimedTask.ClaimCheckoutID == nil || *claimedTask.ClaimCheckoutID != checkout.CheckoutID {
		t.Fatalf("workspace task should surface claim admission fields, got %+v", claimedTask)
	}
	coordination, err := store.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get project coordination: %v", err)
	}
	if !branchListContainsForTest(coordination.Branches, branch.BranchID) || !branchListContainsForTest(coordination.Branches, nextBranch.BranchID) {
		t.Fatalf("coordination should include branch registry, got %+v", coordination.Branches)
	}
	if coordination.CoordinationVersion == "" || !containsForTest(coordination.CoordinationVersion, "|branches:") {
		t.Fatalf("coordination version should include branch freshness, got %q", coordination.CoordinationVersion)
	}
}

func TestProjectTaskClaimAdmissionRejectsDirtyUnboundCheckout(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-dirty-checkout-claim"
		projectID   = "project-dirty-checkout-claim"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-dirty-checkout-claim"
		taskID      = "task-dirty-checkout-claim"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["src/**"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	checkout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\dirty-checkout`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		BranchName:            "agent/worker-agent/dirty-checkout",
		DirtyState:            "dirty",
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register dirty checkout: %v", err)
	}
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/dirty-checkout",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		Summary:               "dirty checkout must not claim new task",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "dirty_state is dirty") {
		t.Fatalf("expected dirty unbound checkout claim to be rejected, got %v", err)
	}
	branchHead := "0123456789abcdef0123456789abcdef01234567"
	checkoutHead := "1111111111111111111111111111111111111111"
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_checkout_registry
   SET dirty_state = 'clean', head_sha = ?
 WHERE workspace_id = ? AND checkout_id = ?`,
		checkoutHead,
		workspaceID,
		checkout.CheckoutID,
	); err != nil {
		t.Fatalf("mark checkout clean with mismatched head: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET head_sha = ?
 WHERE workspace_id = ? AND branch_id = ?`,
		branchHead,
		workspaceID,
		branch.BranchID,
	); err != nil {
		t.Fatalf("mark branch head: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		Summary:               "checkout head must match branch head",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "head_sha") {
		t.Fatalf("expected checkout head mismatch to be rejected, got %v", err)
	}
}

func TestProjectWriteScopeRejectsProsePathsets(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-prose-write-scope"
		projectID   = "project-prose-write-scope"
		leadID      = "alpha"
		workerID    = "beta"
		repoID      = "repo-prose-write-scope"
		taskID      = "task-prose-write-scope"
	)
	invalidScope := `{"paths":["existing Clearpress shell/workspace checkout","app shell","routing","review-ready evidence for the article workspace slice"]}`
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        invalidScope,
		ActorID:               "developer",
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "human", "developer"),
		PromptContextSurface:  "project.role.assign",
	}); err == nil || !strings.Contains(err.Error(), "not prose") {
		t.Fatalf("expected prose implementer role write scope to be rejected, got %v", err)
	}

	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/**","package.json"]}`,
		ActorID:               "developer",
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "human", "developer"),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign valid implementer role: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\beta\prose-scope`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/beta/prose-scope",
		WriteScopeJSON:        invalidScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "not prose") {
		t.Fatalf("expected prose branch write scope to be rejected, got %v", err)
	}

	validBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/beta/valid-scope",
		WriteScopeJSON:        `{"paths":["src/**","package.json"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register valid branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              validBranch.BranchID,
		WriteScopeJSON:        invalidScope,
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "not prose") {
		t.Fatalf("expected prose claim write scope to be rejected, got %v", err)
	}
}

func TestProjectClaimWriteScopeFailsClosedForLegacyProseScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-legacy-prose-scope"
		projectID   = "project-legacy-prose-scope"
		leadID      = "alpha"
		ownerID     = "beta"
		agentID     = "gamma"
		repoID      = "repo-legacy-prose-scope"
		ownerTaskID = "task-owner-scope"
		nextTaskID  = "task-next-scope"
	)
	legacyInvalidScope := `{"paths":["existing Clearpress shell/workspace checkout","app shell","routing"]}`
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, agentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, ownerTaskID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, nextTaskID)
	for _, item := range []struct {
		agent string
		scope string
	}{
		{ownerID, `{"paths":["src/**"]}`},
		{agentID, `{"paths":["src/editor/**"]}`},
	} {
		if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			AgentID:               item.agent,
			RoleType:              sqlite.ProjectRoleImplementer,
			WriteScopeJSON:        item.scope,
			ActorID:               "developer",
			ActorType:             "human",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "human", "developer"),
			PromptContextSurface:  "project.role.assign",
		}); err != nil {
			t.Fatalf("assign role for %s: %v", item.agent, err)
		}
	}
	ownerCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\beta\legacy-prose-owner`)
	ownerBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            ownerCheckout.CheckoutID,
		AgentID:               ownerID,
		BranchName:            "agent/beta/legacy-prose-owner",
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register owner branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                ownerTaskID,
		AgentID:               ownerID,
		RepoID:                repoID,
		CheckoutID:            ownerCheckout.CheckoutID,
		BranchID:              ownerBranch.BranchID,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, ownerTaskID, ownerID),
	}); err != nil {
		t.Fatalf("claim owner task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE task_claims SET write_scope_json = ? WHERE workspace_id = ? AND task_id = ?`, legacyInvalidScope, workspaceID, ownerTaskID); err != nil {
		t.Fatalf("poison legacy task claim scope: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE project_branch_registry SET write_scope_json = ? WHERE workspace_id = ? AND branch_id = ?`, legacyInvalidScope, workspaceID, ownerBranch.BranchID); err != nil {
		t.Fatalf("poison legacy branch scope: %v", err)
	}

	nextCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, agentID, `C:\fixtures\agents\gamma\legacy-prose-next`)
	nextBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            nextCheckout.CheckoutID,
		AgentID:               agentID,
		BranchName:            "agent/gamma/legacy-prose-next",
		WriteScopeJSON:        `{"paths":["src/editor/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               agentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register next branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                nextTaskID,
		AgentID:               agentID,
		RepoID:                repoID,
		CheckoutID:            nextCheckout.CheckoutID,
		BranchID:              nextBranch.BranchID,
		WriteScopeJSON:        `{"paths":["src/editor/**"]}`,
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, nextTaskID, agentID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "overlaps active claim") {
		t.Fatalf("expected legacy invalid scope to fail closed as busy, got %v", err)
	}
}

func TestTrustFirstProjectImplementationClaimAutoProvisionsProjectRole(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-trust-first-branch-auto-role"
		projectID   = "project-trust-first-branch-auto-role"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-branch-auto-role"
		scopeJSON   = `{"paths":["src/lib/**","tests/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\trust-first-auto-role`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/trust-first-auto-role",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch without project role: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        scopeJSON,
		Summary:               "strict claim without project role should fail",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "IMPLEMENTER role") {
		t.Fatalf("expected strict no-role implementation claim rejection, got %v", err)
	}
	event, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        scopeJSON,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "trust-first self-selected branch-backed implementation claim",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	})
	if err != nil {
		t.Fatalf("expected trust-first branch claim to auto-provision role: %v", err)
	}
	if !containsForTest(event.PayloadJSON, branch.BranchID) || !containsForTest(event.PayloadJSON, `"project_role_id"`) || !containsForTest(event.PayloadJSON, `"project_role_auto_provisioned":true`) {
		t.Fatalf("unexpected trust-first auto-role claim event payload: %s", event.PayloadJSON)
	}
	var claimRoleID, claimScope string
	if err := store.DB().QueryRowContext(ctx, `SELECT COALESCE(project_role_id, ''), COALESCE(write_scope_json, '') FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimRoleID, &claimScope); err != nil {
		t.Fatalf("query claim admission row: %v", err)
	}
	if claimRoleID == "" || claimScope != scopeJSON {
		t.Fatalf("unexpected trust-first claim bindings role=%q scope=%q", claimRoleID, claimScope)
	}
	var roleAgentID, roleType, roleStatus, roleScope string
	if err := store.DB().QueryRowContext(ctx, `SELECT agent_id, role_type, status, write_scope_json FROM project_agent_roles WHERE workspace_id = ? AND project_id = ? AND role_id = ?`, workspaceID, projectID, claimRoleID).Scan(&roleAgentID, &roleType, &roleStatus, &roleScope); err != nil {
		t.Fatalf("query auto-provisioned project role: %v", err)
	}
	if roleAgentID != workerID || roleType != sqlite.ProjectRoleImplementer || roleStatus != sqlite.ProjectRoleStatusActive || roleScope != scopeJSON {
		t.Fatalf("unexpected auto-provisioned role agent=%q type=%q status=%q scope=%q", roleAgentID, roleType, roleStatus, roleScope)
	}
}

func TestProjectImplementationClaimRejectsSuppliedRoleOutsideClaimScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-role-scope-claim-bound"
		projectID   = "project-role-scope-claim-bound"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-role-scope-claim-bound"
		roleScope   = `{"paths":["src/alpha/**"]}`
		claimScope  = `{"paths":["src/beta/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, roleScope)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE tasks
   SET write_scope_hints_json = ?,
       task_requirements_json = ?
 WHERE task_id = ?`,
		`["src/beta/**"]`,
		`{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`,
		taskID); err != nil {
		t.Fatalf("set task authoritative scope: %v", err)
	}
	var roleID string
	if err := store.DB().QueryRowContext(ctx, `SELECT role_id FROM project_agent_roles WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND role_type = ? AND status = ?`, workspaceID, projectID, workerID, sqlite.ProjectRoleImplementer, sqlite.ProjectRoleStatusActive).Scan(&roleID); err != nil {
		t.Fatalf("query implementer role: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\role-scope-claim-bound`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/role-scope-claim-bound",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        claimScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		ProjectRoleID:         roleID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        claimScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "supplied role must cover claim scope",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "does not cover claim write_scope_json") {
		t.Fatalf("expected supplied non-covering role to fail closed, got %v", err)
	}
}

func TestTrustFirstProjectImplementationClaimWithExistingNonCoveringRoleAutoProvisionsScopedRole(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-existing-non-covering-role-auto"
		projectID   = "project-existing-non-covering-role-auto"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-existing-non-covering-role-auto"
		oldScope    = `{"paths":["src/alpha/**"]}`
		claimScope  = `{"paths":["src/beta/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, oldScope)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE tasks
   SET write_scope_hints_json = ?,
       task_requirements_json = ?
 WHERE task_id = ?`,
		`["src/beta/**"]`,
		`{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`,
		taskID); err != nil {
		t.Fatalf("set task authoritative scope: %v", err)
	}
	var oldRoleID string
	if err := store.DB().QueryRowContext(ctx, `SELECT role_id FROM project_agent_roles WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND role_type = ? AND status = ?`, workspaceID, projectID, workerID, sqlite.ProjectRoleImplementer, sqlite.ProjectRoleStatusActive).Scan(&oldRoleID); err != nil {
		t.Fatalf("query old implementer role: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\non-covering-role-auto`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/non-covering-role-auto",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        claimScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	event, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        claimScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "trust-first claim should create a scoped role instead of borrowing a stale role",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	})
	if err != nil {
		t.Fatalf("expected trust-first claim to auto-provision scoped role: %v", err)
	}
	var claimRoleID, claimScopeJSON string
	if err := store.DB().QueryRowContext(ctx, `SELECT COALESCE(project_role_id, ''), COALESCE(write_scope_json, '') FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimRoleID, &claimScopeJSON); err != nil {
		t.Fatalf("query claim role: %v", err)
	}
	if claimRoleID == "" || claimRoleID == oldRoleID || claimScopeJSON != claimScope {
		t.Fatalf("unexpected claim role binding old=%q claim=%q scope=%q", oldRoleID, claimRoleID, claimScopeJSON)
	}
	var roleAgentID, roleStatus, roleScope string
	if err := store.DB().QueryRowContext(ctx, `SELECT agent_id, status, write_scope_json FROM project_agent_roles WHERE workspace_id = ? AND project_id = ? AND role_id = ?`, workspaceID, projectID, claimRoleID).Scan(&roleAgentID, &roleStatus, &roleScope); err != nil {
		t.Fatalf("query auto-provisioned role: %v", err)
	}
	if roleAgentID != workerID || roleStatus != sqlite.ProjectRoleStatusActive || roleScope != claimScope {
		t.Fatalf("unexpected auto-provisioned role agent=%q status=%q scope=%q", roleAgentID, roleStatus, roleScope)
	}
	if !containsForTest(event.PayloadJSON, `"project_role_auto_provisioned":true`) {
		t.Fatalf("expected auto-provision event payload, got %s", event.PayloadJSON)
	}
}

func TestTaskClaimAdmissionKeepsClaimScopeAuthoritativeWhenBranchIsNarrower(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID     = "ws-effective-branch-scope-claim"
		projectID       = "project-effective-branch-scope-claim"
		leadID          = "lead-agent"
		workerAID       = "worker-a"
		workerBID       = "worker-b"
		repoID          = "repo-main"
		taskAID         = "task-alpha-slice"
		taskBID         = "task-beta-slice"
		staleBroadScope = `{"paths":["src/**","tests/**"]}`
		narrowScope     = `{"paths":["src/alpha/**"]}`
		probeScope      = `{"paths":["src/beta/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerAID, workerBID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskAID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskBID)

	checkoutA := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerAID, `C:\fixtures\agents\worker-a\effective-branch-scope`)
	branchA, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutA.CheckoutID,
		AgentID:               workerAID,
		BranchName:            "agent/worker-a/effective-branch-scope",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        staleBroadScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerAID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerAID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register source branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskAID,
		AgentID:               workerAID,
		RepoID:                repoID,
		CheckoutID:            checkoutA.CheckoutID,
		BranchID:              branchA.BranchID,
		WriteScopeJSON:        staleBroadScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "initial broad alpha claim",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskAID, workerAID),
	}); err != nil {
		t.Fatalf("claim source task: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutA.CheckoutID,
		AgentID:               workerAID,
		BranchID:              branchA.BranchID,
		ActiveTaskID:          taskAID,
		ActiveClaimID:         taskAID,
		BranchName:            branchA.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        narrowScope,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               workerAID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerAID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "must match active claim scope") {
		t.Fatalf("expected branch-only scope narrowing to be rejected until claim is rebound, got %v", err)
	}

	checkoutB := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerBID, `C:\fixtures\agents\worker-b\effective-branch-scope`)
	branchB, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		AgentID:               workerBID,
		BranchName:            "agent/worker-b/effective-branch-scope",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        probeScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerBID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerBID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register probe branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskBID,
		AgentID:               workerBID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		BranchID:              branchB.BranchID,
		WriteScopeJSON:        probeScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "probe beta claim should remain blocked by persisted broad claim scope",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskBID, workerBID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "overlaps active claim task_id="+taskAID) {
		t.Fatalf("expected persisted broad claim scope to remain authoritative, got %v", err)
	}
}

func TestProjectBranchRegisterRejectsActiveClaimScopeWidening(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-active-claim-branch-scope-cap"
		projectID   = "project-active-claim-branch-scope-cap"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-auth-slice"
		broadScope  = `{"paths":["package.json","src/**","tests/**"]}`
		narrowScope = `{"paths":["src/auth/**","tests/auth/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, broadScope)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\active-claim-scope-cap`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/active-claim-scope-cap",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        broadScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register broad reserved branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        narrowScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim narrow auth slice",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim narrow branch-backed task: %v", err)
	}
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list branches after claim: %v", err)
	}
	if len(branches) != 1 || branches[0].WriteScopeJSON != narrowScope || branches[0].ActiveTaskID != taskID || branches[0].Status != sqlite.ProjectBranchStatusActive {
		t.Fatalf("claim admission should bind branch to admitted claim scope, got %+v", branches)
	}

	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            branch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        broadScope,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "must match active claim scope") {
		t.Fatalf("expected active branch scope drift to be rejected, got %v", err)
	}
	branches, err = store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list branches after rejected widening: %v", err)
	}
	if len(branches) != 1 || branches[0].WriteScopeJSON != narrowScope {
		t.Fatalf("rejected widening must leave branch scope unchanged, got %+v", branches)
	}
}

func TestProjectBranchRegisterAllowsGoModuleSidecarScopeFromActiveClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-active-claim-go-module-sidecar"
		projectID    = "project-active-claim-go-module-sidecar"
		leadID       = "lead-agent"
		workerID     = "worker-agent"
		repoID       = "repo-main"
		taskID       = "task-lexer-lane"
		reviewKey    = "project.project-active-claim-go-module-sidecar.branch.branch-lexer.review"
		claimScope   = `{"paths":["internal/lexer/**","internal/token/**"]}`
		sidecarScope = `{"paths":["go.mod","internal/lexer/**","internal/token/**"]}`
		badScope     = `{"paths":["README.md","internal/lexer/**","internal/token/**"]}`
	)
	baseSHA := strings.Repeat("1", 40)
	headSHA := strings.Repeat("2", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, claimScope)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\go-module-sidecar`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-lexer",
		BranchName:            "agent/worker-agent/go-module-sidecar",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        claimScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register reserved branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        claimScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim lexer lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim lexer branch-backed task: %v", err)
	}
	active, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            branch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        sidecarScope,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("active branch should allow go.mod as derived Go module sidecar: %v", err)
	}
	if active.WriteScopeJSON != sidecarScope {
		t.Fatalf("active branch write scope = %s, want %s", active.WriteScopeJSON, sidecarScope)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            branch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        badScope,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "must match active claim scope") {
		t.Fatalf("expected non-Go sidecar widening to remain rejected, got %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nLexer lane with go.mod module sidecar.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	ready, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            branch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        sidecarScope,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("READY_FOR_REVIEW should preserve allowed go.mod sidecar scope: %v", err)
	}
	if ready.Status != sqlite.ProjectBranchStatusReadyForReview || ready.WriteScopeJSON != sidecarScope {
		t.Fatalf("unexpected ready sidecar branch: %+v", ready)
	}
}

func TestProjectBranchReadyForReviewRequiresExistingActiveTaskBinding(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-ready-requires-active-binding"
		projectID   = "project-ready-requires-active-binding"
		repoID      = "repo-main"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		taskID      = "task-parser-lane"
		reviewKey   = "project.project-ready-requires-active-binding.branch.branch-parser.review"
		scopeJSON   = `{"paths":["internal/parser/**"]}`
	)
	baseSHA := strings.Repeat("1", 40)
	headSHA := strings.Repeat("2", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, scopeJSON)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\ready-requires-active-binding`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-parser",
		BranchName:            "agent/worker-agent/parser",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        scopeJSON,
		Summary:               "claim parser branch",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim parser branch: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nREADY_FOR_REVIEW evidence.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		BranchName:            branch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        scopeJSON,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchActiveReferenceInvalid) || !strings.Contains(err.Error(), "requires active_task_id and active_claim_id") {
		t.Fatalf("expected READY_FOR_REVIEW without existing active task binding to fail closed, got %v", err)
	}
}

func TestProjectBranchReadyForReviewRejectsForeignClaimRelabelAndAllowsOwnClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-ready-foreign-claim-relabel"
		projectID    = "project-ready-foreign-claim-relabel"
		repoID       = "repo-main"
		leadID       = "lead-agent"
		workerID     = "worker-agent"
		parserTask   = "task-parser-lane"
		stdlibTask   = "task-stdlib-lane"
		parserBranch = "branch-parser-candidate"
		stdlibBranch = "branch-stdlib-candidate"
		parserScope  = `{"paths":["internal/parser/**"]}`
		stdlibScope  = `{"paths":["internal/stdlib/**"]}`
		parserReview = "project.project-ready-foreign-claim-relabel.branch.branch-parser-candidate.review"
		stdlibReview = "project.project-ready-foreign-claim-relabel.branch.branch-stdlib-candidate.review"
	)
	baseSHA := strings.Repeat("1", 40)
	headSHA := strings.Repeat("2", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["internal/parser/**","internal/ast/**","internal/stdlib/**","internal/builtins/**","internal/builtin/**","internal/functions/**","internal/lambda/**","go.mod","go.sum"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, parserTask)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, stdlibTask)
	parserCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\parser-candidate`)
	stdlibCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\stdlib-candidate`)
	parser, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		AgentID:               workerID,
		BranchID:              parserBranch,
		BranchName:            "agent/worker-agent/parser-candidate",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        parserScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register parser branch: %v", err)
	}
	stdlib, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            stdlibCheckout.CheckoutID,
		AgentID:               workerID,
		BranchID:              stdlibBranch,
		BranchName:            "agent/worker-agent/stdlib-candidate",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        stdlibScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register stdlib branch: %v", err)
	}
	for _, doc := range []struct {
		key   string
		title string
	}{
		{parserReview, "Parser Branch Review Packet"},
		{stdlibReview, "Stdlib Branch Review Packet"},
	} {
		if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      doc.key,
			Title:       doc.title,
			Content:     "# Branch Review Packet\n\nREADY_FOR_REVIEW evidence.",
			UpdatedBy:   workerID,
		}); err != nil {
			t.Fatalf("write %s: %v", doc.title, err)
		}
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		AgentID:               workerID,
		BranchID:              parser.BranchID,
		BranchName:            parser.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        parserScope,
		ReviewDocKey:          parserReview,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchActiveReferenceInvalid) || !strings.Contains(err.Error(), "requires active_task_id and active_claim_id") {
		t.Fatalf("expected first-time unbound READY_FOR_REVIEW to fail closed, got %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                stdlibTask,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            stdlibCheckout.CheckoutID,
		BranchID:              stdlib.BranchID,
		WriteScopeJSON:        stdlibScope,
		Summary:               "claim stdlib lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, stdlibTask, workerID),
	}); err != nil {
		t.Fatalf("claim stdlib task: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		AgentID:               workerID,
		BranchID:              parser.BranchID,
		ActiveTaskID:          stdlibTask,
		ActiveClaimID:         stdlibTask,
		BranchName:            parser.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        stdlibScope,
		ReviewDocKey:          parserReview,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "cannot widen") {
		t.Fatalf("expected explicit foreign stdlib scope to hit branch widen guard, got %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		AgentID:               workerID,
		BranchID:              parser.BranchID,
		ActiveTaskID:          stdlibTask,
		ActiveClaimID:         stdlibTask,
		BranchName:            parser.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		ReviewDocKey:          parserReview,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchActiveReferenceInvalid) || !strings.Contains(err.Error(), "not CLAIMED on this branch") {
		t.Fatalf("expected empty-scope foreign claim relabel to fail branch-bound claim check, got %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                parserTask,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		BranchID:              parser.BranchID,
		WriteScopeJSON:        parserScope,
		Summary:               "claim parser lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, parserTask, workerID),
	}); err != nil {
		t.Fatalf("claim parser task: %v", err)
	}
	ready, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		AgentID:               workerID,
		BranchID:              parser.BranchID,
		ActiveTaskID:          parserTask,
		ActiveClaimID:         parserTask,
		BranchName:            parser.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               strings.Repeat("3", 40),
		WriteScopeJSON:        `{"paths":["internal/parser/**","go.mod"]}`,
		ReviewDocKey:          parserReview,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("genuine parser claim should publish parser branch: %v", err)
	}
	if ready.ActiveTaskID != parserTask || ready.ActiveClaimID != parserTask || ready.Status != sqlite.ProjectBranchStatusReadyForReview {
		t.Fatalf("unexpected genuine parser ready branch: %+v", ready)
	}
}

func TestK2_ClaimParserBranchUnderStdlibTaskRejectsBeforeMutation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-k2-parser-branch-stdlib-claim"
		projectID   = "project-k2-parser-branch-stdlib-claim"
		repoID      = "repo-main"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		stdlibTask  = "task-stdlib-lane"
		branchID    = "branch-parser-candidate"
		parserScope = `{"paths":["internal/parser/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["internal/parser/**","internal/stdlib/**","internal/builtins/**","go.mod","go.sum"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, stdlibTask)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\k2-parser-branch-stdlib-claim`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branchID,
		BranchName:            "agent/worker-agent/k2-parser-candidate",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        parserScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register parser branch: %v", err)
	}

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                stdlibTask,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        parserScope,
		Summary:               "attempt to relabel parser branch under stdlib task",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, stdlibTask, workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "outside authoritative lane scope") {
		t.Fatalf("expected stdlib task parser-branch claim to be rejected by authoritative lane scope, got %v", err)
	}
	var claimRows int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(1) FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, stdlibTask).Scan(&claimRows); err != nil {
		t.Fatalf("count rejected claim rows: %v", err)
	}
	if claimRows != 0 {
		t.Fatalf("rejected stdlib relabel claim must not mutate task_claims, got %d row(s)", claimRows)
	}
}

func TestK2C_GenericImplementationTaskWithoutHintsUsesSemanticLaneScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-k2c-generic-impl-semantic-scope"
		projectID      = "project-k2c-generic-impl-semantic-scope"
		repoID         = "repo-main"
		leadID         = "lead-agent"
		workerID       = "worker-agent"
		evaluatorTask  = "task-evaluator-lane"
		parserBranchID = "branch-parser-candidate"
		evalBranchID   = "branch-evaluator-candidate"
		parserScope    = `{"paths":["internal/parser/**"]}`
		evaluatorScope = `{"paths":["internal/eval/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["internal/parser/**","internal/ast/**","internal/eval/**","internal/jsonctx/**","internal/evaluator/**","internal/runtime/**","internal/value/**","internal/path/**","internal/jsonpath/**","go.mod","go.sum"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, evaluatorTask)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\k2c-generic-impl-semantic-scope`)
	for _, candidate := range []struct {
		branchID string
		name     string
		scope    string
	}{
		{parserBranchID, "agent/worker-agent/k2c-parser-candidate", parserScope},
		{evalBranchID, "agent/worker-agent/k2c-evaluator-candidate", evaluatorScope},
	} {
		if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			RepoID:                repoID,
			CheckoutID:            checkout.CheckoutID,
			AgentID:               workerID,
			BranchID:              candidate.branchID,
			BranchName:            candidate.name,
			BranchKind:            sqlite.ProjectBranchKindFeature,
			BaseBranch:            "main",
			WriteScopeJSON:        candidate.scope,
			Status:                sqlite.ProjectBranchStatusReserved,
			ActorID:               workerID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
			PromptContextSurface:  "project.branch.register",
		}); err != nil {
			t.Fatalf("register branch %s: %v", candidate.branchID, err)
		}
	}

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                evaluatorTask,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              parserBranchID,
		WriteScopeJSON:        parserScope,
		Summary:               "attempt evaluator task over parser branch",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, evaluatorTask, workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "outside authoritative lane scope") {
		t.Fatalf("expected evaluator task parser-branch claim to be rejected by semantic lane scope, got %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                evaluatorTask,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              evalBranchID,
		WriteScopeJSON:        evaluatorScope,
		Summary:               "claim evaluator lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, evaluatorTask, workerID),
	}); err != nil {
		t.Fatalf("expected evaluator task to claim evaluator branch without explicit hints: %v", err)
	}
	var claimScope string
	if err := store.DB().QueryRowContext(ctx, `SELECT COALESCE(write_scope_json, '') FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, evaluatorTask).Scan(&claimScope); err != nil {
		t.Fatalf("load evaluator claim scope: %v", err)
	}
	if !strings.Contains(claimScope, "internal/eval") {
		t.Fatalf("expected authoritative evaluator claim scope to cover evaluator branch scope, got %s", claimScope)
	}
}

func TestTaskClaimAdmissionFallsBackToStaleClaimScopeWhenBranchBindingMismatches(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID     = "ws-effective-branch-scope-mismatch"
		projectID       = "project-effective-branch-scope-mismatch"
		leadID          = "lead-agent"
		workerAID       = "worker-a"
		workerBID       = "worker-b"
		repoID          = "repo-main"
		taskAID         = "task-alpha-slice"
		taskBID         = "task-beta-slice"
		staleBroadScope = `{"paths":["src/**","tests/**"]}`
		narrowScope     = `{"paths":["src/alpha/**"]}`
		probeScope      = `{"paths":["src/beta/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerAID, workerBID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskAID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskBID)

	checkoutA := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerAID, `C:\fixtures\agents\worker-a\effective-branch-mismatch`)
	branchA, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutA.CheckoutID,
		AgentID:               workerAID,
		BranchName:            "agent/worker-a/effective-branch-mismatch",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        staleBroadScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerAID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerAID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register source branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskAID,
		AgentID:               workerAID,
		RepoID:                repoID,
		CheckoutID:            checkoutA.CheckoutID,
		BranchID:              branchA.BranchID,
		WriteScopeJSON:        staleBroadScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "initial broad alpha claim",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskAID, workerAID),
	}); err != nil {
		t.Fatalf("claim source task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET active_task_id = ?, active_claim_id = ?, write_scope_json = ?
 WHERE workspace_id = ? AND branch_id = ?`,
		"task-stale-other", "task-stale-other", narrowScope, workspaceID, branchA.BranchID); err != nil {
		t.Fatalf("force mismatched branch binding: %v", err)
	}

	checkoutB := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerBID, `C:\fixtures\agents\worker-b\effective-branch-mismatch`)
	branchB, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		AgentID:               workerBID,
		BranchName:            "agent/worker-b/effective-branch-mismatch",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        probeScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerBID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerBID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register probe branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskBID,
		AgentID:               workerBID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		BranchID:              branchB.BranchID,
		WriteScopeJSON:        probeScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "probe beta claim should fail closed on mismatched branch binding",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskBID, workerBID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "overlaps active claim task_id="+taskAID) {
		t.Fatalf("expected mismatched branch binding to fall back to broad active claim scope, got %v", err)
	}
	reviewKey := "project." + projectID + ".branch." + branchA.BranchID + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Review\n\nReady without active refs.",
		UpdatedBy:   workerAID,
	}); err != nil {
		t.Fatalf("write review doc for no-ref branch: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET active_task_id = '', active_claim_id = '', status = ?, head_sha = ?, review_doc_key = ?, write_scope_json = ?
 WHERE workspace_id = ? AND branch_id = ?`,
		sqlite.ProjectBranchStatusReadyForReview, strings.Repeat("c", 40), reviewKey, narrowScope, workspaceID, branchA.BranchID); err != nil {
		t.Fatalf("force no-ref ready branch binding: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskBID,
		AgentID:               workerBID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		BranchID:              branchB.BranchID,
		WriteScopeJSON:        probeScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "probe beta claim should fail closed when source branch has no active refs",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskBID, workerBID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "overlaps active claim task_id="+taskAID) {
		t.Fatalf("expected no-ref ready branch to fall back to broad active claim scope, got %v", err)
	}
}

func TestTrustFirstProjectImplementationClaimRejectsOverlappingReadyBranchScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-trust-first-ready-overlap"
		projectID   = "project-trust-first-ready-overlap"
		leadID      = "lead-agent"
		workerAID   = "worker-a"
		workerBID   = "worker-b"
		repoID      = "repo-main"
		taskAID     = "task-ready-source"
		taskBID     = "task-overlap-probe"
		sourceScope = `{"paths":["src/**"]}`
		probeScope  = `{"paths":["src/components/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerAID, workerBID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskAID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskBID)

	checkoutA := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerAID, `C:\fixtures\agents\worker-a\trust-first-ready-overlap`)
	branchA, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutA.CheckoutID,
		AgentID:               workerAID,
		BranchName:            "agent/worker-a/trust-first-ready-overlap",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        sourceScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerAID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerAID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register source branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskAID,
		AgentID:               workerAID,
		RepoID:                repoID,
		CheckoutID:            checkoutA.CheckoutID,
		BranchID:              branchA.BranchID,
		WriteScopeJSON:        sourceScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "trust-first source implementation claim",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskAID, workerAID),
	}); err != nil {
		t.Fatalf("claim trust-first source branch: %v", err)
	}
	reviewDocKey := "project." + projectID + ".branch." + branchA.BranchID + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewDocKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nREADY_FOR_REVIEW evidence.",
		UpdatedBy:   workerAID,
	}); err != nil {
		t.Fatalf("write review packet: %v", err)
	}
	readyBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branchA.BranchID,
		CheckoutID:            checkoutA.CheckoutID,
		AgentID:               workerAID,
		ActiveTaskID:          taskAID,
		ActiveClaimID:         taskAID,
		BranchName:            branchA.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("b", 40),
		HeadSHA:               strings.Repeat("a", 40),
		WriteScopeJSON:        sourceScope,
		ReviewDocKey:          reviewDocKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerAID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerAID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("mark source branch ready for review: %v", err)
	}
	if readyBranch.Status != sqlite.ProjectBranchStatusReadyForReview {
		t.Fatalf("expected source branch to be ready for review, got %+v", readyBranch)
	}
	if _, err := store.ReleaseTaskClaimWithEvent(ctx, sqlite.TaskReleaseInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskAID,
		AgentID:               workerAID,
		Reason:                "ready branch published; release active task claim before overlap probe",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.release", workspaceID, taskAID, workerAID),
	}); err != nil {
		t.Fatalf("release source task after ready branch publication: %v", err)
	}
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerAID,
	})
	if err != nil {
		t.Fatalf("list source branches after completion: %v", err)
	}
	if len(branches) != 1 || branches[0].ActiveTaskID != "" || branches[0].ActiveClaimID != "" {
		t.Fatalf("completed ready branch should release active refs, got %+v", branches)
	}

	checkoutB := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerBID, `C:\fixtures\agents\worker-b\trust-first-overlap-probe`)
	branchB, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		AgentID:               workerBID,
		BranchName:            "agent/worker-b/trust-first-overlap-probe",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        probeScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerBID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerBID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register overlapping probe branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskBID,
		AgentID:               workerBID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		BranchID:              branchB.BranchID,
		WriteScopeJSON:        probeScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "trust-first overlapping implementation claim should fail",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskBID, workerBID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "overlaps live branch_id="+readyBranch.BranchID) {
		t.Fatalf("expected trust-first overlapping READY_FOR_REVIEW scope rejection, got %v", err)
	}
}

func TestTaskClaimAdmissionIgnoresIntegratedReadyBranchWithTerminalActiveRefs(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-integrated-ready-terminal-refs"
		projectID   = "project-integrated-ready-terminal-refs"
		leadID      = "alpha"
		oldOwnerID  = "delta"
		newOwnerID  = "beta"
		reviewerID  = "gamma"
		repoID      = "repo-main"
		oldBranchID = "branch-integrated-terminal-refs"
		oldTaskID   = "task-integrated-terminal-refs"
		newTaskID   = "task-claim-after-integrated-terminal-refs"
		scopeJSON   = `{"paths":["internal/eval/**","internal/runtime/**","internal/value/**","internal/runner/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, oldOwnerID, newOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, oldOwnerID, leadID, scopeJSON)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, newOwnerID, leadID, scopeJSON)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, oldTaskID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, newTaskID)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, oldBranchID, oldOwnerID, reviewerID, scopeJSON, sqlite.ProjectPatchQueueStateAccepted)
	integrated := integrateAgentWorkPatchQueueItem(t, ctx, store, workspaceID, projectID, leadID, item)
	if integrated.State != sqlite.ProjectPatchQueueStateIntegrated {
		t.Fatalf("expected integrated item, got %+v", integrated)
	}

	if _, err := store.DB().ExecContext(ctx, `
UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ?`,
		model.TaskStatusResolved, "2026-06-20T00:00:00Z", oldTaskID); err != nil {
		t.Fatalf("mark old task resolved: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at, project_role_id, repo_id, checkout_id, branch_id, write_scope_json)
VALUES (?, ?, ?, ?, ?, ?, NULL, ?, '', ?, '', ?, ?)`,
		oldTaskID, workspaceID, oldOwnerID, model.TaskClaimStatusCompleted, "completed stale integrated branch owner task", "2026-06-20T00:00:00Z", "2026-06-20T00:00:00Z", repoID, oldBranchID, scopeJSON); err != nil {
		t.Fatalf("insert terminal old claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET active_task_id = ?, active_claim_id = ?, updated_at = ?
 WHERE workspace_id = ? AND branch_id = ?`,
		oldTaskID, oldTaskID, "2026-06-20T00:00:01Z", workspaceID, oldBranchID); err != nil {
		t.Fatalf("restore stale active refs on integrated branch: %v", err)
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, newOwnerID, `C:\fixtures\agents\beta\integrated-terminal-refs`)
	nextBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               newOwnerID,
		BranchName:            "agent/beta/integrated-terminal-refs",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               newOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", newOwnerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register next branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                newTaskID,
		AgentID:               newOwnerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              nextBranch.BranchID,
		WriteScopeJSON:        scopeJSON,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim same scope after stale refs point at an integrated terminal branch",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, newTaskID, newOwnerID),
	}); err != nil {
		t.Fatalf("expected integrated terminal branch refs not to block same scope claim: %v", err)
	}
}

func TestTaskClaimAdmissionIgnoresUnboundLiveLookingBranchScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-unbound-branch-scope-ignored"
		projectID   = "project-unbound-branch-scope-ignored"
		leadID      = "lead-agent"
		workerAID   = "worker-a"
		workerBID   = "worker-b"
		repoID      = "repo-main"
		taskBID     = "task-claim-after-unbound-branch"
		scopeJSON   = `{"paths":["src/**","public/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerAID, workerBID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerBID, leadID, scopeJSON)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskBID)

	checkoutA := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerAID, `C:\fixtures\agents\worker-a\unbound-branch`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutA.CheckoutID,
		AgentID:               workerAID,
		BranchName:            "agent/worker-a/unbound-branch",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerAID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerAID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register unbound branch: %v", err)
	}

	checkoutB := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerBID, `C:\fixtures\agents\worker-b\claim-after-unbound`)
	branchB, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		AgentID:               workerBID,
		BranchName:            "agent/worker-b/claim-after-unbound",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerBID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerBID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register claiming branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskBID,
		AgentID:               workerBID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		BranchID:              branchB.BranchID,
		WriteScopeJSON:        scopeJSON,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "unbound provisional branch rows should not own write scope",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskBID, workerBID),
	}); err != nil {
		t.Fatalf("expected claim to ignore unbound live-looking branch scope: %v", err)
	}
}

func TestProjectBranchReclaimReleasePreservesBranchForSameAgentRecovery(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-branch-reclaim-preserve"
		projectID   = "project-branch-reclaim-preserve"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-branch-reclaim-preserve"
		scopeJSON   = `{"paths":["src/**","package.json"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, scopeJSON)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\reclaim-preserve`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/reclaim-preserve",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        scopeJSON,
		Summary:               "claim before operator pause",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim task with branch admission: %v", err)
	}

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		staleAt, staleAt, workspaceID, workerID,
	); err != nil {
		t.Fatalf("age agent heartbeat: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE task_claims SET claimed_at = ?, updated_at = ? WHERE workspace_id = ? AND task_id = ?`,
		staleAt, staleAt, workspaceID, taskID,
	); err != nil {
		t.Fatalf("age task claim: %v", err)
	}

	result, err := store.ReclaimOrphanedClaimedTasks(ctx, sqlite.LocalTaskClaimOwnershipReclaimInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "local_lease_manager",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reclaim orphaned project claim: %v", err)
	}
	if result.TaskClaimsReleased != 1 || result.Problems != 0 {
		t.Fatalf("unexpected reclaim result: %+v", result)
	}

	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		RepoID:          repoID,
		AgentID:         workerID,
		IncludeInactive: true,
	})
	if err != nil {
		t.Fatalf("list branches after reclaim: %v", err)
	}
	if len(branches) != 1 || branches[0].BranchID != branch.BranchID || branches[0].Status != sqlite.ProjectBranchStatusActive || branches[0].ActiveTaskID != "" || branches[0].ActiveClaimID != "" || branches[0].WriteScopeJSON != scopeJSON {
		t.Fatalf("expected reclaim to preserve active branch evidence while clearing refs, got %+v", branches)
	}
	releasedEvent := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.released",
		EntityType:  "task",
		EntityID:    taskID,
		Limit:       5,
	})
	if !containsForTest(releasedEvent.PayloadJSON, `"branch_status":"UNCHANGED_RECLAIM_RELEASE"`) || !containsForTest(releasedEvent.PayloadJSON, `"branch_active_refs_cleared":true`) {
		t.Fatalf("reclaim release event should preserve branch status while clearing refs, got %s", releasedEvent.PayloadJSON)
	}

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        scopeJSON,
		Summary:               "same agent resumes after reclaim",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("same agent should reclaim preserved branch: %v", err)
	}
	branches, err = store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list branches after resume claim: %v", err)
	}
	if len(branches) != 1 || branches[0].BranchID != branch.BranchID || branches[0].Status != sqlite.ProjectBranchStatusActive || branches[0].ActiveTaskID != taskID || branches[0].ActiveClaimID != taskID {
		t.Fatalf("expected same agent resume to rebind preserved branch, got %+v", branches)
	}
}

func TestProjectBranchExplicitReleaseStillAbandonsActiveBranch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-branch-explicit-release"
		projectID   = "project-branch-explicit-release"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-branch-explicit-release"
		scopeJSON   = `{"paths":["src/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, scopeJSON)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\explicit-release`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/explicit-release",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        scopeJSON,
		Summary:               "claim before explicit release",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim task with branch admission: %v", err)
	}
	if _, err := store.ReleaseTaskClaimWithEvent(ctx, sqlite.TaskReleaseInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Reason:                "agent deliberately abandons this slice",
		PromptContextEnvelope: taskReleasePromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("explicit release task claim: %v", err)
	}
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		RepoID:          repoID,
		AgentID:         workerID,
		IncludeInactive: true,
	})
	if err != nil {
		t.Fatalf("list branches after explicit release: %v", err)
	}
	if len(branches) != 1 || branches[0].BranchID != branch.BranchID || branches[0].Status != sqlite.ProjectBranchStatusAbandoned || branches[0].ActiveTaskID != "" || branches[0].ActiveClaimID != "" {
		t.Fatalf("expected explicit release to abandon active branch and clear refs, got %+v", branches)
	}
}

func TestProjectBranchRegistryRejectsHijackAndLiveNameCollision(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-branch-hijack"
		projectID   = "project-branch-hijack"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		otherID     = "other-agent"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, otherID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\branch-hijack`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register checkout: %v", err)
	}
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/shared-slice",
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}

	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		AgentID:               otherID,
		BranchName:            branch.BranchName,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) {
		t.Fatalf("expected live branch_name collision to be rejected, got %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		AgentID:               otherID,
		BranchName:            "agent/other-agent/hijack-by-id",
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) {
		t.Fatalf("expected branch_id hijack to be rejected, got %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               otherID,
		BranchName:            "agent/other-agent/borrowed-checkout",
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) {
		t.Fatalf("expected borrowed checkout to be rejected, got %v", err)
	}
}

func TestProjectBranchRegistryAllowsIntegrationAuthorityToCloseAcceptedSourceBranch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-project-branch-cross-agent-merge"
		projectID    = "project-branch-cross-agent-merge"
		leadID       = "lead-agent"
		workerID     = "worker-agent"
		integratorID = "integrator-agent"
		otherID      = "other-agent"
		repoID       = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, integratorID, otherID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               integratorID,
		RoleType:              sqlite.ProjectRoleIntegrator,
		Summary:               "Integration authority for accepted source branch close.",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign integrator role: %v", err)
	}

	integratorBranch := createAcceptedReadyBranchForMergeTest(t, ctx, store, workspaceID, projectID, repoID, workerID, integratorID, "branch-cross-agent-integrator", "agent/worker-agent/cross-agent-integrator", strings.Repeat("a", 40), strings.Repeat("b", 40), `C:\fixtures\agents\worker-agent\cross-agent-integrator`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              integratorBranch.BranchID,
		CheckoutID:            integratorBranch.CheckoutID,
		AgentID:               workerID,
		BranchName:            integratorBranch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            integratorBranch.BaseBranch,
		BaseSHA:               integratorBranch.BaseSHA,
		HeadSHA:               strings.Repeat("e", 40),
		WriteScopeJSON:        integratorBranch.WriteScopeJSON,
		ReviewDocKey:          integratorBranch.ReviewDocKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "accepted source boundary is frozen") {
		t.Fatalf("expected accepted source boundary drift to be rejected, got %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              integratorBranch.BranchID,
		CheckoutID:            integratorBranch.CheckoutID,
		AgentID:               workerID,
		BranchName:            integratorBranch.BranchName + "-renamed",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            integratorBranch.BaseBranch,
		BaseSHA:               integratorBranch.BaseSHA,
		HeadSHA:               integratorBranch.HeadSHA,
		WriteScopeJSON:        integratorBranch.WriteScopeJSON,
		ReviewDocKey:          integratorBranch.ReviewDocKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "accepted source boundary is frozen") {
		t.Fatalf("expected accepted branch name drift to be rejected, got %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              integratorBranch.BranchID,
		CheckoutID:            integratorBranch.CheckoutID,
		AgentID:               workerID,
		BranchName:            integratorBranch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindIntegration,
		BaseBranch:            integratorBranch.BaseBranch,
		BaseSHA:               integratorBranch.BaseSHA,
		HeadSHA:               integratorBranch.HeadSHA,
		WriteScopeJSON:        integratorBranch.WriteScopeJSON,
		ReviewDocKey:          integratorBranch.ReviewDocKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "accepted source boundary is frozen") {
		t.Fatalf("expected accepted branch_kind drift to be rejected, got %v", err)
	}
	if _, _, err := closeBranchAsMergedForTest(ctx, store, workspaceID, projectID, repoID, integratorBranch, workerID, workerID); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "integration authority") {
		t.Fatalf("expected branch owner without integration authority to fail MERGED close, got %v", err)
	}
	if _, _, err := closeBranchAsMergedForTest(ctx, store, workspaceID, projectID, repoID, integratorBranch, workerID, otherID); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) {
		t.Fatalf("expected unauthorized cross-agent merge close to fail, got %v", err)
	}
	if _, _, err := closeBranchAsMergedForTest(ctx, store, workspaceID, projectID, repoID, integratorBranch, workerID, integratorID); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "durable integrated patch queue receipt") {
		t.Fatalf("expected accepted source close before integrated receipt to fail, got %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET branch_kind = ?
 WHERE workspace_id = ? AND project_id = ? AND branch_id = ?`,
		sqlite.ProjectBranchKindIntegration, workspaceID, projectID, integratorBranch.BranchID); err != nil {
		t.Fatalf("force legacy branch_kind drift: %v", err)
	}
	legacyDriftedBranch := integratorBranch
	legacyDriftedBranch.BranchKind = sqlite.ProjectBranchKindIntegration
	if _, _, err := closeBranchAsMergedForTest(ctx, store, workspaceID, projectID, repoID, legacyDriftedBranch, workerID, integratorID); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "durable integrated patch queue receipt") {
		t.Fatalf("expected legacy branch_kind integration drift to still require integrated receipt, got %v", err)
	}
	legacyDriftedBranch.HeadSHA = strings.Repeat("9", 40)
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET branch_kind = ?, head_sha = ?
 WHERE workspace_id = ? AND project_id = ? AND branch_id = ?`,
		sqlite.ProjectBranchKindIntegration, legacyDriftedBranch.HeadSHA, workspaceID, projectID, integratorBranch.BranchID); err != nil {
		t.Fatalf("force legacy branch_kind/head drift: %v", err)
	}
	if _, _, err := closeBranchAsMergedForTest(ctx, store, workspaceID, projectID, repoID, legacyDriftedBranch, workerID, integratorID); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "ACCEPTED patch queue evidence") {
		t.Fatalf("expected legacy branch_kind/head drift to reject integration-target exemption, got %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET branch_kind = ?, head_sha = ?
 WHERE workspace_id = ? AND project_id = ? AND branch_id = ?`,
		sqlite.ProjectBranchKindFeature, integratorBranch.HeadSHA, workspaceID, projectID, integratorBranch.BranchID); err != nil {
		t.Fatalf("restore branch_kind after legacy drift probe: %v", err)
	}
	recordIntegratedReceiptForAcceptedBranchForMergeTest(t, ctx, store, workspaceID, projectID, integratorBranch, integratorID, "main", strings.Repeat("1", 40), strings.Repeat("2", 40))
	mergedByIntegrator, _, err := closeBranchAsMergedForTest(ctx, store, workspaceID, projectID, repoID, integratorBranch, workerID, integratorID)
	if err != nil {
		t.Fatalf("integrator should close accepted worker branch as MERGED: %v", err)
	}
	if mergedByIntegrator.Status != sqlite.ProjectBranchStatusMerged || mergedByIntegrator.AgentID != workerID || mergedByIntegrator.UpdatedBy != integratorID {
		t.Fatalf("unexpected integrator merge close result: %+v", mergedByIntegrator)
	}

	leadBranch := createAcceptedReadyBranchForMergeTest(t, ctx, store, workspaceID, projectID, repoID, workerID, leadID, "branch-cross-agent-lead", "agent/worker-agent/cross-agent-lead", strings.Repeat("c", 40), strings.Repeat("d", 40), `C:\fixtures\agents\worker-agent\cross-agent-lead`)
	recordIntegratedReceiptForAcceptedBranchForMergeTest(t, ctx, store, workspaceID, projectID, leadBranch, leadID, "main", strings.Repeat("3", 40), strings.Repeat("4", 40))
	mergedByLead, _, err := closeBranchAsMergedForTest(ctx, store, workspaceID, projectID, repoID, leadBranch, workerID, leadID)
	if err != nil {
		t.Fatalf("strategic lead should close accepted worker branch as MERGED: %v", err)
	}
	if mergedByLead.Status != sqlite.ProjectBranchStatusMerged || mergedByLead.UpdatedBy != leadID {
		t.Fatalf("unexpected lead merge close result: %+v", mergedByLead)
	}

	ownerAuthorityBranch := createAcceptedReadyBranchForMergeTest(t, ctx, store, workspaceID, projectID, repoID, integratorID, leadID, "branch-cross-agent-owner-authority-drift", "agent/integrator-agent/owner-authority-drift", strings.Repeat("0", 40), strings.Repeat("9", 40), `C:\fixtures\agents\integrator-agent\owner-authority-drift`)
	ownerAuthorityDriftedBranch := ownerAuthorityBranch
	ownerAuthorityDriftedBranch.BranchKind = sqlite.ProjectBranchKindIntegration
	ownerAuthorityDriftedBranch.HeadSHA = strings.Repeat("8", 40)
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET branch_kind = ?, head_sha = ?
 WHERE workspace_id = ? AND project_id = ? AND branch_id = ?`,
		ownerAuthorityDriftedBranch.BranchKind, ownerAuthorityDriftedBranch.HeadSHA, workspaceID, projectID, ownerAuthorityBranch.BranchID); err != nil {
		t.Fatalf("force owner-authority legacy branch_kind/head drift: %v", err)
	}
	if _, _, err := closeBranchAsMergedForTest(ctx, store, workspaceID, projectID, repoID, ownerAuthorityDriftedBranch, integratorID, integratorID); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "accepted source boundary cannot use integration-target MERGED exemption") {
		t.Fatalf("expected owner-authority drift to reject integration-target exemption, got %v", err)
	}

	activeCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\cross-agent-active`)
	activeBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              "branch-cross-agent-active",
		CheckoutID:            activeCheckout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/cross-agent-active",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("e", 40),
		HeadSHA:               strings.Repeat("f", 40),
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register active worker branch: %v", err)
	}
	if _, _, err := closeBranchAsMergedForTest(ctx, store, workspaceID, projectID, repoID, activeBranch, workerID, integratorID); !errors.Is(err, sqlite.ErrProjectBranchActiveReferenceInvalid) {
		t.Fatalf("expected active branch cross-agent merge close to fail, got %v", err)
	}

	integrationCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, integratorID, `C:\fixtures\agents\integrator-agent\cross-agent-integration-target`)
	integrationBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              "branch-cross-agent-integration-target",
		CheckoutID:            integrationCheckout.CheckoutID,
		AgentID:               integratorID,
		BranchName:            "main",
		BranchKind:            sqlite.ProjectBranchKindIntegration,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("5", 40),
		HeadSHA:               strings.Repeat("6", 40),
		WriteScopeJSON:        `{"paths":["**"]}`,
		Status:                sqlite.ProjectBranchStatusMerged,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register integration target branch: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              integrationBranch.BranchID,
		CheckoutID:            integrationCheckout.CheckoutID,
		AgentID:               integratorID,
		BranchName:            integrationBranch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindIntegration,
		BaseBranch:            integrationBranch.BaseBranch,
		BaseSHA:               integrationBranch.HeadSHA,
		HeadSHA:               strings.Repeat("7", 40),
		WriteScopeJSON:        integrationBranch.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusMerged,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "belongs to agent") {
		t.Fatalf("expected non-owner integration target update to fail, got %v", err)
	}
	updatedIntegrationBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              integrationBranch.BranchID,
		CheckoutID:            integrationCheckout.CheckoutID,
		AgentID:               integratorID,
		BranchName:            integrationBranch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindIntegration,
		BaseBranch:            integrationBranch.BaseBranch,
		BaseSHA:               integrationBranch.HeadSHA,
		HeadSHA:               strings.Repeat("8", 40),
		WriteScopeJSON:        integrationBranch.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusMerged,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("integration target branch update should not require accepted source evidence: %v", err)
	}
	if updatedIntegrationBranch.HeadSHA != strings.Repeat("8", 40) || updatedIntegrationBranch.UpdatedBy != integratorID {
		t.Fatalf("unexpected integration target branch update result: %+v", updatedIntegrationBranch)
	}
}

func TestProjectBranchRegistryIdempotentTerminalIntegrationTargetReregister(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-project-branch-integration-target-reregister"
		projectID    = "project-branch-integration-target-reregister"
		leadID       = "lead-agent"
		integratorA  = "integrator-a"
		integratorB  = "integrator-b"
		observerID   = "observer-agent"
		repoID       = "repo-main"
		targetBranch = "main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, integratorA, integratorB, observerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	for _, agentID := range []string{integratorA, integratorB} {
		if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			AgentID:               agentID,
			RoleType:              sqlite.ProjectRoleIntegrator,
			Summary:               "Integration authority for terminal target evidence.",
			ActorID:               leadID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
			PromptContextSurface:  "project.role.assign",
		}); err != nil {
			t.Fatalf("assign integrator role to %s: %v", agentID, err)
		}
	}

	checkoutA := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, integratorA, `C:\fixtures\agents\integrator-a\project-integration`)
	head := strings.Repeat("9", 40)
	first, event, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutA.CheckoutID,
		AgentID:               integratorA,
		BranchName:            targetBranch,
		BranchKind:            sqlite.ProjectBranchKindIntegration,
		BaseBranch:            targetBranch,
		BaseSHA:               head,
		HeadSHA:               head,
		WriteScopeJSON:        `{"paths":["**"]}`,
		Status:                sqlite.ProjectBranchStatusMerged,
		ActorID:               integratorA,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", integratorA),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register first integration target branch: %v", err)
	}
	if first.BranchID == "" || event.EntityID != first.BranchID {
		t.Fatalf("unexpected first integration target branch=%+v event=%+v", first, event)
	}

	checkoutB := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, integratorB, `C:\fixtures\agents\integrator-b\project-integration`)
	reregistered, event, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		AgentID:               integratorB,
		BranchName:            targetBranch,
		BranchKind:            sqlite.ProjectBranchKindIntegration,
		BaseBranch:            targetBranch,
		BaseSHA:               head,
		HeadSHA:               head,
		WriteScopeJSON:        `{"paths":["**"]}`,
		Status:                sqlite.ProjectBranchStatusMerged,
		ActorID:               integratorB,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", integratorB),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("reregister terminal integration target branch: %v", err)
	}
	if reregistered.BranchID != first.BranchID || event.EntityID != first.BranchID {
		t.Fatalf("expected terminal integration target reregister to reuse %s, got branch=%+v event=%+v", first.BranchID, reregistered, event)
	}
	if reregistered.AgentID != integratorA || reregistered.CheckoutID != checkoutA.CheckoutID || reregistered.UpdatedBy != integratorA {
		t.Fatalf("expected idempotent reregister to preserve canonical branch owner/checkout, got %+v", reregistered)
	}
	var sameHeadCount int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM project_branch_registry
 WHERE workspace_id = ? AND project_id = ? AND repo_id = ?
   AND branch_kind = ? AND branch_name = ? AND status = ? AND TRIM(head_sha) = ?`,
		workspaceID, projectID, repoID, sqlite.ProjectBranchKindIntegration, targetBranch, sqlite.ProjectBranchStatusMerged, head).Scan(&sameHeadCount); err != nil {
		t.Fatalf("count same-head integration target branches: %v", err)
	}
	if sameHeadCount != 1 {
		t.Fatalf("expected one same-head integration target branch, got %d", sameHeadCount)
	}

	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		AgentID:               observerID,
		BranchName:            targetBranch,
		BranchKind:            sqlite.ProjectBranchKindIntegration,
		BaseBranch:            targetBranch,
		BaseSHA:               head,
		HeadSHA:               head,
		WriteScopeJSON:        `{"paths":["**"]}`,
		Status:                sqlite.ProjectBranchStatusMerged,
		ActorID:               observerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", observerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "integration authority") {
		t.Fatalf("expected non-integrator terminal target reregister to fail, got %v", err)
	}

	nextHead := strings.Repeat("a", 40)
	next, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		AgentID:               integratorB,
		BranchName:            targetBranch,
		BranchKind:            sqlite.ProjectBranchKindIntegration,
		BaseBranch:            targetBranch,
		BaseSHA:               head,
		HeadSHA:               nextHead,
		WriteScopeJSON:        `{"paths":["**"]}`,
		Status:                sqlite.ProjectBranchStatusMerged,
		ActorID:               integratorB,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", integratorB),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register next-head integration target branch: %v", err)
	}
	if next.BranchID == first.BranchID {
		t.Fatalf("expected next head to receive a distinct integration target branch, got %s", next.BranchID)
	}
}

func TestProjectBranchRegistryRejectsTerminalBranchResurrection(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-branch-terminal"
		projectID   = "project-branch-terminal"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\branch-terminal`)

	for _, terminalStatus := range []string{
		sqlite.ProjectBranchStatusAbandoned,
		sqlite.ProjectBranchStatusMerged,
		sqlite.ProjectBranchStatusArchived,
	} {
		branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			RepoID:                repoID,
			CheckoutID:            checkout.CheckoutID,
			AgentID:               workerID,
			BranchName:            "agent/worker-agent/terminal-" + strings.ToLower(terminalStatus),
			WriteScopeJSON:        `{"paths":["src/**"]}`,
			Status:                terminalStatus,
			ActorID:               workerID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
			PromptContextSurface:  "project.branch.register",
		})
		if err != nil {
			t.Fatalf("register terminal branch %s: %v", terminalStatus, err)
		}
		if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			RepoID:                repoID,
			BranchID:              branch.BranchID,
			CheckoutID:            checkout.CheckoutID,
			AgentID:               workerID,
			BranchName:            branch.BranchName,
			WriteScopeJSON:        `{"paths":["src/**"]}`,
			Status:                sqlite.ProjectBranchStatusReadyForReview,
			ActorID:               workerID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
			PromptContextSurface:  "project.branch.register",
		}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "terminal branch") {
			t.Fatalf("expected terminal %s branch resurrection to be rejected, got %v", terminalStatus, err)
		}
	}
}

func TestProjectBranchRegistryAbandonsOnlyUnclaimedReservedBranch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-branch-abandon-reserved"
		projectID   = "project-branch-abandon-reserved"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["src/**","tests/**"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\branch-abandon-reserved`)

	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/project-branch-abandon-reserved",
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register reserved branch: %v", err)
	}
	abandoned, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            branch.BranchName,
		WriteScopeJSON:        branch.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusAbandoned,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("abandon unclaimed reserved branch: %v", err)
	}
	if abandoned.Status != sqlite.ProjectBranchStatusAbandoned || abandoned.ActiveTaskID != "" || abandoned.ActiveClaimID != "" {
		t.Fatalf("expected unclaimed reserved branch to become abandoned, got %+v", abandoned)
	}

	taskID := "task-project-branch-abandon-active"
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	activeBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/project-branch-abandon-active",
		WriteScopeJSON:        `{"paths":["tests/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch to claim: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              activeBranch.BranchID,
		WriteScopeJSON:        `{"paths":["tests/**"]}`,
		Summary:               "claim branch before unsafe abandon",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim task with branch admission: %v", err)
	}
	poisonBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/project-branch-abandon-poison",
		WriteScopeJSON:        `{"paths":["docs/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch for active-ref poison attempt: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              poisonBranch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            poisonBranch.BranchName,
		WriteScopeJSON:        poisonBranch.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusAbandoned,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchActiveReferenceInvalid) {
		t.Fatalf("expected abandoned branch with supplied active refs to be rejected, got %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              activeBranch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            activeBranch.BranchName,
		WriteScopeJSON:        activeBranch.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusAbandoned,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchActiveReferenceInvalid) {
		t.Fatalf("expected active branch abandon to be rejected, got %v", err)
	}
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list branches after rejected abandon: %v", err)
	}
	var foundActive bool
	var foundPoisonReserved bool
	for _, got := range branches {
		if got.BranchID == activeBranch.BranchID {
			foundActive = true
			if got.Status != sqlite.ProjectBranchStatusActive || got.ActiveTaskID != taskID || got.ActiveClaimID != taskID {
				t.Fatalf("active branch should survive rejected abandon unchanged, got %+v", got)
			}
		}
		if got.BranchID == poisonBranch.BranchID {
			foundPoisonReserved = true
			if got.Status != sqlite.ProjectBranchStatusReserved || got.ActiveTaskID != "" || got.ActiveClaimID != "" {
				t.Fatalf("poison branch should remain unclaimed reserved after rejected abandon, got %+v", got)
			}
		}
	}
	if !foundActive {
		t.Fatalf("expected active branch %s to remain listed, got %+v", activeBranch.BranchID, branches)
	}
	if !foundPoisonReserved {
		t.Fatalf("expected poison branch %s to remain listed, got %+v", poisonBranch.BranchID, branches)
	}
}

func TestProjectBranchRegistryReadyForReviewRequiresEvidenceAndStableScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-branch-review-evidence"
		projectID   = "project-branch-review-evidence"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		reviewKey   = "project.project-branch-review-evidence.branch.branch-review.review"
	)
	readyBaseSHA := strings.Repeat("a", 40)
	readyHeadSHA := strings.Repeat("b", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\branch-review-evidence`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-review",
		BranchName:            "agent/worker-agent/review-evidence",
		WriteScopeJSON:        `{"paths":["web/app.js"]}`,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	taskID := "task-" + branch.BranchID
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, branch.BranchID, taskID, `{"paths":["web/app.js"]}`)

	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            branch.BranchName,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		WriteScopeJSON:        `{"paths":["web/app.js"]}`,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) {
		t.Fatalf("expected READY_FOR_REVIEW without review_doc_key to fail, got %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            branch.BranchName,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		WriteScopeJSON:        `{"paths":["web/app.js"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected missing review_doc_key to fail, got %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nREADY_FOR_REVIEW evidence",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            branch.BranchName,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		WriteScopeJSON:        `{"paths":["web/app.js","api/server.go"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "cannot widen") {
		t.Fatalf("expected READY_FOR_REVIEW scope widening to fail, got %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            branch.BranchName,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		WriteScopeJSON:        `{"paths":["web/app.js"]}`,
		ReviewDocKey:          reviewKey,
		BaseSHA:               "base123",
		HeadSHA:               "head123",
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "head_sha") {
		t.Fatalf("expected non-canonical READY_FOR_REVIEW object ids to fail, got %v", err)
	}
	globBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-review-glob",
		BranchName:            "agent/worker-agent/review-glob-evidence",
		WriteScopeJSON:        `{"paths":["lib/*.go"]}`,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register glob branch: %v", err)
	}
	globTaskID := "task-" + globBranch.BranchID
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, globBranch.BranchID, globTaskID, `{"paths":["lib/*.go"]}`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              globBranch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            globBranch.BranchName,
		ActiveTaskID:          globTaskID,
		ActiveClaimID:         globTaskID,
		WriteScopeJSON:        `{"paths":["lib/components/**"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "cannot widen") {
		t.Fatalf("expected non-trailing glob scope widening to fail, got %v", err)
	}
	ready, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            branch.BranchName,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		WriteScopeJSON:        `{"paths":["web/app.js"]}`,
		ReviewDocKey:          reviewKey,
		BaseSHA:               readyBaseSHA,
		HeadSHA:               readyHeadSHA,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("mark branch ready with review evidence: %v", err)
	}
	if ready.Status != sqlite.ProjectBranchStatusReadyForReview || ready.ReviewDocKey != reviewKey || ready.HeadSHA != readyHeadSHA || ready.BaseSHA != readyBaseSHA || ready.WriteScopeJSON != `{"paths":["web/app.js"]}` {
		t.Fatalf("unexpected ready branch: %+v", ready)
	}
}

func TestProjectTaskClaimAdmissionAllowsOwnedReviewReadyReclaimForFinalization(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-review-ready-reclaim"
		projectID   = "project-review-ready-reclaim"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-review-ready-finalize"
		reviewKey   = "project.project-review-ready-reclaim.branch.branch-finalize.review"
	)
	baseSHA := strings.Repeat("1", 40)
	headSHA := strings.Repeat("2", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["app/**"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\review-ready-reclaim`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/review-ready-finalize",
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Summary:               "claim implementation branch",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim implementation branch: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nREADY_FOR_REVIEW evidence",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	ready, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            branch.BranchName,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		ReviewDocKey:          reviewKey,
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("mark branch ready for review: %v", err)
	}
	if ready.Status != sqlite.ProjectBranchStatusReadyForReview || ready.ActiveTaskID != taskID || ready.ActiveClaimID != taskID {
		t.Fatalf("READY_FOR_REVIEW should preserve active task binding before finalization reclaim, got %+v", ready)
	}
	reregistered, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              ready.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            branch.BranchName,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("reregister review-ready branch with no new evidence: %v", err)
	}
	if reregistered.Status != sqlite.ProjectBranchStatusReadyForReview || reregistered.ActiveTaskID != taskID || reregistered.ActiveClaimID != taskID || reregistered.ReviewDocKey != reviewKey {
		t.Fatalf("expected no-op reregister to preserve review-ready evidence while binding active refs, got %+v", reregistered)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Reason:                "review-ready evidence exists; finalization cycle needs to resume",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.block", workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("block after review-ready: %v", err)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-review-ready-borrow")
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-review-ready-borrow",
		AgentID:               workerID,
		BranchID:              ready.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Summary:               "borrow review-ready branch for another task",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-review-ready-borrow", workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "READY_FOR_REVIEW") {
		t.Fatalf("expected review-ready branch borrow to fail closed, got %v", err)
	}

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		BranchID:              ready.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Summary:               "reclaim review-ready branch for finalization",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("reclaim review-ready branch for finalization: %v", err)
	}
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list branches after review-ready reclaim: %v", err)
	}
	if len(branches) != 1 || branches[0].Status != sqlite.ProjectBranchStatusReadyForReview || branches[0].ActiveTaskID != taskID || branches[0].ActiveClaimID != taskID {
		t.Fatalf("expected finalization reclaim to bind active refs without reopening branch status, got %+v", branches)
	}
	if _, err := store.CompleteTaskWithEvent(ctx, sqlite.TaskCompleteInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Summary:               "implementation finalized after review-ready evidence",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.complete", workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("complete task after review-ready reclaim: %v", err)
	}
	branches, err = store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list branches after completion: %v", err)
	}
	if len(branches) != 1 || branches[0].Status != sqlite.ProjectBranchStatusReadyForReview || branches[0].ActiveTaskID != "" || branches[0].ActiveClaimID != "" || branches[0].ReviewDocKey != reviewKey {
		t.Fatalf("expected completion to clear finalization refs while preserving review evidence, got %+v", branches)
	}
}

func TestTaskClaimAdmissionAllowsReferencedReviewReadyBranchRevisionFollowup(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-review-ready-revision"
		projectID   = "project-review-ready-revision"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-original-implementation"
		followupID  = "task-patchq-revision-followup"
		branchID    = "branch-ready-revision"
		reviewKey   = "project.project-review-ready-revision.branch.branch-ready-revision.review"
	)
	baseSHA := strings.Repeat("3", 40)
	headSHA := strings.Repeat("4", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["app/**"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\review-ready-revision`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branchID,
		BranchName:            "agent/worker-agent/review-ready-revision",
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Summary:               "claim original branch",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim original branch: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nREADY_FOR_REVIEW evidence",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	ready, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            branch.BranchName,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		ReviewDocKey:          reviewKey,
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("mark branch ready for review: %v", err)
	}
	if _, err := store.ReleaseTaskClaimWithEvent(ctx, sqlite.TaskReleaseInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Reason:                "review-ready branch published; hand off to revision follow-up",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.release", workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("release original claim after review-ready branch publication: %v", err)
	}
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, followupID)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE tasks SET title = ?, description = ? WHERE task_id = ?`,
		"Unblock integration candidate "+ready.BranchID,
		"Patch queue decision follow-up.\n\n- branch_id: "+ready.BranchID+"\n- state: BLOCKED",
		followupID,
	); err != nil {
		t.Fatalf("write follow-up branch hint: %v", err)
	}

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                followupID,
		AgentID:               workerID,
		BranchID:              ready.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Summary:               "claim referenced review-ready branch for revision follow-up",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, followupID, workerID),
	}); err != nil {
		t.Fatalf("claim referenced review-ready branch follow-up: %v", err)
	}
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list branches after follow-up claim: %v", err)
	}
	if len(branches) != 1 || branches[0].Status != sqlite.ProjectBranchStatusReadyForReview || branches[0].ActiveTaskID != followupID || branches[0].ActiveClaimID != followupID {
		t.Fatalf("expected follow-up claim to bind existing review-ready branch, got %+v", branches)
	}
}

func TestTaskClaimAdmissionAllowsReferencedBlockedBranchRevisionFollowup(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-blocked-revision"
		projectID   = "project-blocked-revision"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		reviewerID  = "reviewer-agent"
		repoID      = "repo-main"
		taskID      = "task-original-implementation"
		followupID  = "task-patchq-blocked-revision-followup"
		branchID    = "branch-blocked-revision"
	)
	baseSHA := strings.Repeat("c", 40)
	headSHA := strings.Repeat("d", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["app/**"]}`)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               reviewerID,
		RoleType:              sqlite.ProjectRoleReviewer,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign reviewer role: %v", err)
	}
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\blocked-revision`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branchID,
		BranchName:            "agent/worker-agent/blocked-revision",
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Summary:               "claim original branch",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim original branch: %v", err)
	}
	reviewKey := "project." + projectID + ".branch." + branch.BranchID + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Blocked Revision Review Packet",
		Content:     "# Review Packet\n\nReady for patch queue decision.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write blocked revision review doc: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchID:              branch.BranchID,
		BranchName:            branch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("mark blocked revision branch ready: %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-blocked-revision",
		RunID:                    "run-blocked-revision",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-blocked-revision",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{"app/main.go": "sha256:app"},
		RepoLeaseID:              "lease-blocked-revision",
		LeaseTerm:                7,
		ActorID:                  workerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit blocked revision patch queue item: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		LeaseSeconds:          900,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim blocked revision patch queue item: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Needs an owner revision before integration.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("decide blocked revision patch queue item: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Reason:                "review blocked branch pending revision",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.block", workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("block original task: %v", err)
	}
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list branches before follow-up: %v", err)
	}
	if len(branches) != 1 || branches[0].ActiveTaskID != taskID || branches[0].ActiveClaimID != taskID {
		t.Fatalf("expected blocked original task to retain branch active refs, got %+v", branches)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, followupID)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE tasks SET title = ?, description = ?, tags_json = ? WHERE task_id = ?`,
		"Unblock integration candidate "+branch.BranchID,
		"Patch queue decision follow-up.\n\n- queue_id: "+item.QueueID+"\n- item_id: "+item.ItemID+"\n- branch_id: "+branch.BranchID+"\n- head_sha: "+headSHA+"\n- state: BLOCKED",
		`["project","patch-queue","revision","blocked"]`,
		followupID,
	); err != nil {
		t.Fatalf("write follow-up branch hint: %v", err)
	}

	const ordinaryID = "task-ordinary-branch-mention"
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, ordinaryID)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE tasks SET title = ?, description = ? WHERE task_id = ?`,
		"Mention existing branch "+branch.BranchID,
		"Ordinary coordination note.\n\n- branch_id: "+branch.BranchID,
		ordinaryID,
	); err != nil {
		t.Fatalf("write ordinary branch mention: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                ordinaryID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Summary:               "ordinary mention must not steal blocked branch",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, ordinaryID, workerID),
	}); err == nil {
		t.Fatalf("ordinary branch mention unexpectedly rebound blocked branch")
	}

	const missingStateID = "task-patchq-revision-missing-state"
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, missingStateID)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE tasks SET title = ?, description = ?, tags_json = ? WHERE task_id = ?`,
		"Unblock integration candidate without explicit state "+branch.BranchID,
		"Patch queue decision follow-up.\n\n- queue_id: patchq-missing-state\n- item_id: patchitem-missing-state\n- branch_id: "+branch.BranchID,
		`["project","patch-queue","revision","blocked"]`,
		missingStateID,
	); err != nil {
		t.Fatalf("write missing-state follow-up branch hint: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                missingStateID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Summary:               "patch queue tags without explicit state must not rebind",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, missingStateID, workerID),
	}); err == nil {
		t.Fatalf("patch queue revision tags without explicit state unexpectedly rebound blocked branch")
	}

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                followupID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Summary:               "claim blocked branch for revision follow-up",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, followupID, workerID),
	}); err != nil {
		t.Fatalf("claim blocked branch follow-up: %v", err)
	}
	branches, err = store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list branches after follow-up: %v", err)
	}
	if len(branches) != 1 || branches[0].BranchID != branch.BranchID || branches[0].ActiveTaskID != followupID || branches[0].ActiveClaimID != followupID {
		t.Fatalf("expected follow-up claim to rebind blocked branch active refs, got %+v", branches)
	}
}

func TestProjectBranchRegistryValidatesActiveReferences(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-branch-active-refs"
		projectID   = "project-branch-active-refs"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		otherID     = "other-agent"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, otherID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, otherID, leadID, `{"paths":["src/**"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-claimed-by-other")
	otherCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, otherID, `C:\fixtures\agents\other-agent\project-branch-active-refs`)
	otherBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            otherCheckout.CheckoutID,
		AgentID:               otherID,
		BranchName:            "agent/other-agent/project-branch-active-refs",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register other branch: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-claimed-by-other",
		AgentID:               otherID,
		RepoID:                repoID,
		CheckoutID:            otherCheckout.CheckoutID,
		BranchID:              otherBranch.BranchID,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claimed by another branch owner",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-claimed-by-other", otherID),
	}); err != nil {
		t.Fatalf("claim task by other agent: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		AgentID:               workerID,
		ActiveTaskID:          "task-claimed-by-other",
		ActiveClaimID:         "task-claimed-by-other",
		BranchName:            "agent/worker-agent/wrong-claim",
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchActiveReferenceInvalid) {
		t.Fatalf("expected wrong active claim owner to be rejected, got %v", err)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-unclaimed")
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		AgentID:               workerID,
		ActiveTaskID:          "task-unclaimed",
		BranchName:            "agent/worker-agent/missing-claim",
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchActiveReferenceInvalid) {
		t.Fatalf("expected active_task_id without claim to be rejected, got %v", err)
	}

	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/bad-json",
		WriteScopeJSON:        `{"paths":`,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); err == nil {
		t.Fatal("expected invalid write_scope_json to be rejected")
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-claim-borrowed-branch")
	otherBranch, _, err = store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		AgentID:               otherID,
		BranchName:            "agent/other-agent/owned-branch",
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register other branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-claim-borrowed-branch",
		AgentID:               workerID,
		BranchID:              otherBranch.BranchID,
		Summary:               "borrow another agent branch",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-claim-borrowed-branch", workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) {
		t.Fatalf("expected claim admission to reject borrowed branch, got %v", err)
	}
}

func TestProjectTaskClaimAdmissionRequiresBranchCheckoutAndWriteScopeWhenImplementationOpen(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-claim-admission"
		projectID   = "project-claim-admission"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		otherID     = "other-agent"
		repoID      = "repo-main"
		taskID      = "task-admission-required"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, otherID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["app/**","src/core/**","src/lib/**","src/workers/**","lib/components/**","tests/**"]}`)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, otherID, leadID, `{"paths":["src/ui/**","src/styles/**","src/components/**","src/App.*","lib/**/*.go"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Summary:               "claim without branch admission",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) {
		t.Fatalf("expected project implementation claim without admission to fail closed, got %v", err)
	}
	orphanBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/no-checkout",
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register orphan branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		BranchID:              orphanBranch.BranchID,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		Summary:               "claim branch without checkout",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) {
		t.Fatalf("expected branch claim without checkout to fail closed, got %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		AgentID:               workerID,
		BranchID:              orphanBranch.BranchID,
		BranchName:            "agent/worker-agent/no-checkout",
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		Status:                sqlite.ProjectBranchStatusAbandoned,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("abandon orphan branch after failed admission: %v", err)
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\claim-admission`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/admission",
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch with checkout: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Summary:               "claim with full project admission",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim with full project admission: %v", err)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-ui-scope")
	uiCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, otherID, `C:\fixtures\agents\other-agent\claim-ui-scope`)
	uiBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            uiCheckout.CheckoutID,
		AgentID:               otherID,
		BranchName:            "agent/other-agent/ui-scope",
		WriteScopeJSON:        `{"paths":["src/ui/**","src/styles/**","src/components/**","src/App.*"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ui-scope branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-ui-scope",
		AgentID:               otherID,
		BranchID:              uiBranch.BranchID,
		CheckoutID:            uiCheckout.CheckoutID,
		WriteScopeJSON:        `{"paths":["src/ui/**","src/styles/**","src/components/**","src/App.*"]}`,
		Summary:               "claim UI implementation slice",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-ui-scope", otherID),
	}); err != nil {
		t.Fatalf("claim UI implementation slice: %v", err)
	}
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-core-scope")
	coreCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\claim-core-scope`)
	coreBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            coreCheckout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/core-scope",
		WriteScopeJSON:        `{"paths":["src/core/**","src/lib/**","tests/**","src/workers/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register core-scope branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-core-scope",
		AgentID:               workerID,
		BranchID:              coreBranch.BranchID,
		CheckoutID:            coreCheckout.CheckoutID,
		WriteScopeJSON:        `{"paths":["src/core/**","src/lib/**","tests/**","src/workers/**"]}`,
		Summary:               "claim core implementation slice",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-core-scope", workerID),
	}); err != nil {
		t.Fatalf("core scope should not overlap sibling UI/App glob scope: %v", err)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-overlap")
	otherCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, otherID, `C:\fixtures\agents\other-agent\claim-admission`)
	otherBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            otherCheckout.CheckoutID,
		AgentID:               otherID,
		BranchName:            "agent/other-agent/overlap",
		WriteScopeJSON:        `{"paths":["src/components/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register overlapping branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-overlap",
		AgentID:               otherID,
		BranchID:              otherBranch.BranchID,
		CheckoutID:            otherCheckout.CheckoutID,
		WriteScopeJSON:        `{"paths":["src/components/**"]}`,
		Summary:               "claim overlapping implementation slice",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-overlap", otherID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) {
		t.Fatalf("expected overlapping write scope to fail closed, got %v", err)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-glob-owner")
	globOwnerCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, otherID, `C:\fixtures\agents\other-agent\claim-glob-owner`)
	globOwnerBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            globOwnerCheckout.CheckoutID,
		AgentID:               otherID,
		BranchName:            "agent/other-agent/glob-owner",
		WriteScopeJSON:        `{"paths":["lib/**/*.go"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register glob owner branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-glob-owner",
		AgentID:               otherID,
		BranchID:              globOwnerBranch.BranchID,
		CheckoutID:            globOwnerCheckout.CheckoutID,
		WriteScopeJSON:        `{"paths":["lib/**/*.go"]}`,
		Summary:               "claim glob owner slice",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-glob-owner", otherID),
	}); err != nil {
		t.Fatalf("claim glob owner slice: %v", err)
	}
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-glob-overlap")
	globOverlapCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\claim-glob-overlap`)
	globOverlapBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            globOverlapCheckout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/glob-overlap",
		WriteScopeJSON:        `{"paths":["lib/components/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register glob overlap branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-glob-overlap",
		AgentID:               workerID,
		BranchID:              globOverlapBranch.BranchID,
		CheckoutID:            globOverlapCheckout.CheckoutID,
		WriteScopeJSON:        `{"paths":["lib/components/**"]}`,
		Summary:               "claim overlapping glob slice",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-glob-overlap", workerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) {
		t.Fatalf("expected non-trailing wildcard write scope to overlap nested path, got %v", err)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-wildcard-overlap")
	wildcardCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, otherID, `C:\fixtures\agents\other-agent\claim-wildcard`)
	wildcardBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            wildcardCheckout.CheckoutID,
		AgentID:               otherID,
		BranchName:            "agent/other-agent/wildcard-overlap",
		WriteScopeJSON:        `{"paths":["**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register wildcard branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-wildcard-overlap",
		AgentID:               otherID,
		BranchID:              wildcardBranch.BranchID,
		CheckoutID:            wildcardCheckout.CheckoutID,
		WriteScopeJSON:        `{"paths":["**"]}`,
		Summary:               "claim wildcard implementation slice",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-wildcard-overlap", otherID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) {
		t.Fatalf("expected wildcard write scope to overlap existing live branch, got %v", err)
	}
}

func TestProjectTaskClaimAdmissionValidatesAuthoritativeTaskWriteScopeWithoutWidening(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-authoritative-task-scope"
		projectID      = "project-authoritative-task-scope"
		leadID         = "lead-agent"
		workerID       = "worker-agent"
		repoID         = "repo-main"
		taskID         = "task-signal01-rqs1-acceptance-tests"
		canonicalScope = `{"paths":["internal/**","cmd/**","README.md"]}`
		narrowScope    = `{"paths":["cmd/**","internal/cli/**","internal/repl/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["**"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, taskID, "implementation", true)
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE tasks
   SET title = ?,
       description = ?,
       task_requirements_json = ?,
       write_scope_hints_json = ?
 WHERE task_id = ?`,
		"Add full acceptance test matrix",
		"Add or extend Go tests so the product contract is executable: happy paths, edge cases, CLI file mode, and REPL non-crash behavior. Do not weaken existing keystone tests.",
		`{"schema":"product_first_task_requirements.v1","preserve_write_scope_hints":true,"product_slice":"acceptance_tests","must_add_tests":true}`,
		`["internal/**","cmd/**","README.md"]`,
		taskID,
	); err != nil {
		t.Fatalf("mark task scope authoritative: %v", err)
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\authoritative-task-scope`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/authoritative-task-scope",
		WriteScopeJSON:        narrowScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register narrow branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        narrowScope,
		Summary:               "claim with stale/narrow runtime admission scope",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim with authoritative task scope: %v", err)
	}

	var claimScope, branchScope string
	if err := store.DB().QueryRowContext(ctx, `SELECT write_scope_json FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimScope); err != nil {
		t.Fatalf("query claim scope: %v", err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT write_scope_json FROM project_branch_registry WHERE workspace_id = ? AND branch_id = ?`, workspaceID, branch.BranchID).Scan(&branchScope); err != nil {
		t.Fatalf("query branch scope: %v", err)
	}
	if claimScope != narrowScope || branchScope != narrowScope {
		t.Fatalf("narrow scope should remain after authoritative validation: claim=%s branch=%s want=%s", claimScope, branchScope, narrowScope)
	}
}

func TestProjectTaskClaimAdmissionTrustFirstRequiresMaterialBoundary(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-claim-trust-first-boundary"
		projectID   = "project-claim-trust-first-boundary"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["app/**"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)

	for _, tc := range []struct {
		name  string
		claim sqlite.TaskClaimInput
	}{
		{
			name: "no bindings",
		},
		{
			name:  "repo only",
			claim: sqlite.TaskClaimInput{RepoID: repoID},
		},
		{
			name:  "repo and scope only",
			claim: sqlite.TaskClaimInput{RepoID: repoID, WriteScopeJSON: `{"paths":["app/**"]}`},
		},
	} {
		taskID := "task-trust-first-boundary-" + strings.ReplaceAll(tc.name, " ", "-")
		createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
		claim := tc.claim
		claim.WorkspaceID = workspaceID
		claim.TaskID = taskID
		claim.AgentID = workerID
		claim.CoordinationMode = sqlite.CoordinationModeTrustFirst
		claim.Summary = "trust-first " + tc.name
		claim.PromptContextEnvelope = taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID)
		if _, err := store.ClaimTaskWithEvent(ctx, claim); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "requires branch_id, checkout_id, and write_scope_json") {
			t.Fatalf("expected trust-first %s to fail closed on material boundary, got %v", tc.name, err)
		}
	}

	taskID := "task-trust-first-boundary-happy"
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\trust-first-boundary`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/trust-first-boundary",
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register trust-first branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "trust-first full material boundary",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("trust-first full material boundary should claim: %v", err)
	}
}

func TestProjectRoleAssignSupersedesAndRebindsSingleActiveImplementationClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-role-rebind-claim"
		projectID   = "project-role-rebind-claim"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-role-rebind-claim"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	oldRole, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/ui/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "initial mistaken UI scope",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign old role: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\role-rebind`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/role-rebind",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["src/ui/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		ProjectRoleID:         oldRole.RoleID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        `{"paths":["src/ui/**"]}`,
		Summary:               "claim with initially wrong role scope",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim with old scope: %v", err)
	}

	newRole, event, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/core/**","tests/core/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "correct worker scope after role/task mismatch",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign corrected role: %v", err)
	}
	if !strings.Contains(event.PayloadJSON, `"superseded_role_ids"`) || !strings.Contains(event.PayloadJSON, `"active_claim_rebind"`) || !strings.Contains(event.PayloadJSON, `"state":"updated"`) {
		t.Fatalf("role assignment event should expose supersede/rebind evidence, got %s", event.PayloadJSON)
	}

	var oldStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM project_agent_roles WHERE role_id = ?`, oldRole.RoleID).Scan(&oldStatus); err != nil {
		t.Fatalf("query old role status: %v", err)
	}
	if oldStatus != sqlite.ProjectRoleStatusReleased {
		t.Fatalf("old role status = %q, want %q", oldStatus, sqlite.ProjectRoleStatusReleased)
	}
	var claimRoleID, claimScope string
	if err := store.DB().QueryRowContext(ctx, `SELECT project_role_id, write_scope_json FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimRoleID, &claimScope); err != nil {
		t.Fatalf("query rebound task claim: %v", err)
	}
	if claimRoleID != newRole.RoleID || claimScope != newRole.WriteScopeJSON {
		t.Fatalf("claim role/scope = %q %q, want %q %q", claimRoleID, claimScope, newRole.RoleID, newRole.WriteScopeJSON)
	}
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	})
	if err != nil {
		t.Fatalf("list branches after role rebind: %v", err)
	}
	if len(branches) != 1 || branches[0].BranchID != branch.BranchID || branches[0].WriteScopeJSON != newRole.WriteScopeJSON {
		t.Fatalf("expected active branch scope to follow corrected role, got %+v", branches)
	}
	activeRoles, err := store.ListProjectRoles(ctx, workspaceID, projectID, false)
	if err != nil {
		t.Fatalf("list active roles: %v", err)
	}
	activeImplementerRoles := 0
	for _, role := range activeRoles {
		if role.AgentID == workerID && role.RoleType == sqlite.ProjectRoleImplementer {
			activeImplementerRoles++
			if role.RoleID != newRole.RoleID {
				t.Fatalf("active implementer role = %s, want %s", role.RoleID, newRole.RoleID)
			}
		}
	}
	if activeImplementerRoles != 1 {
		t.Fatalf("active implementer roles for worker = %d, want 1; roles=%+v", activeImplementerRoles, activeRoles)
	}
}

func TestProjectRoleAssignResolvesBlockedScopeConflictClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-role-rebind-blocked-scope"
		projectID   = "project-role-rebind-blocked-scope"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-role-rebind-blocked-scope"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	oldRole, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/editor/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "initial mistaken editor scope",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign old role: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\role-rebind-blocked-scope`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/role-rebind-blocked-scope",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        oldRole.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		ProjectRoleID:         oldRole.RoleID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        oldRole.WriteScopeJSON,
		Summary:               "claim with initially wrong scope",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim with old scope: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Reason:                "Fresh checkout verification blocked because claimed write scope only covers src/editor/**; role/scope repair is pending.",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.block", workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("block scope-conflict claim: %v", err)
	}

	newRole, event, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/app/**","src/routes/**","package.json"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "lead-approved non-overlapping foundation ownership",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign corrected role: %v", err)
	}
	for _, want := range []string{`"previous_claim_status":"BLOCKED"`, `"claim_status":"CLAIMED"`, `"block_type":"blocked_scope_conflict"`, `"block_resolution_state":"blocked_resolved_repair"`, `"preferred_transition":"request_resume"`} {
		if !strings.Contains(event.PayloadJSON, want) {
			t.Fatalf("role rebind payload missing %s: %s", want, event.PayloadJSON)
		}
	}

	var claimRoleID, claimStatus, claimScope, claimSummary string
	if err := store.DB().QueryRowContext(ctx, `SELECT project_role_id, claim_status, write_scope_json, summary FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimRoleID, &claimStatus, &claimScope, &claimSummary); err != nil {
		t.Fatalf("query rebound blocked claim: %v", err)
	}
	if claimRoleID != newRole.RoleID || claimStatus != "CLAIMED" || claimScope != newRole.WriteScopeJSON {
		t.Fatalf("claim role/status/scope = %q %q %q, want %q CLAIMED %q", claimRoleID, claimStatus, claimScope, newRole.RoleID, newRole.WriteScopeJSON)
	}
	if !strings.Contains(claimSummary, "Role/scope repair resolved") {
		t.Fatalf("expected resumable repair summary, got %q", claimSummary)
	}
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{WorkspaceID: workspaceID, ProjectID: projectID, RepoID: repoID, AgentID: workerID})
	if err != nil {
		t.Fatalf("list branches after blocked role rebind: %v", err)
	}
	if len(branches) != 1 || branches[0].BranchID != branch.BranchID || branches[0].WriteScopeJSON != newRole.WriteScopeJSON || branches[0].ActiveTaskID != taskID || branches[0].ActiveClaimID != taskID {
		t.Fatalf("expected active branch boundary to follow repaired claim, got %+v", branches)
	}
}

func TestProjectRoleAssignResolvesSatisfiedRoleScopeAuthorityTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID     = "ws-project-role-scope-authority-resolve"
		projectID       = "project-role-scope-authority-resolve"
		leadID          = "lead-agent"
		workerID        = "worker-agent"
		repoID          = "repo-main"
		taskID          = "task-role-scope-owner-lane"
		authorityTaskID = "task-role-scope-authority-carrier"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	oldRole, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/editor/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "initial narrow scope",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign old role: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\role-scope-authority-resolve`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/role-scope-authority-resolve",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        oldRole.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		ProjectRoleID:         oldRole.RoleID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        oldRole.WriteScopeJSON,
		Summary:               "claim with initially narrow scope",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim with old scope: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Reason:                "Role/scope repair is pending for the branch owner lane.",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.block", workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("block owner lane: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate authority graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               authorityTaskID,
		OwnerUserID:          workerID,
		Priority:             "high",
		Title:                "Apply worker role/scope authority transition",
		Description:          "# Strategic Lead Role/Scope Request\n\nWorker lane needs project_role_assign.",
		TaskKind:             model.TaskKindCoordination,
		TaskTemplate:         "generic",
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		RequiresProjectGate:  false,
		Tags:                 []string{"project-role-scope", "authority-transition", "strategic-lead", "coordination"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign","project_id":"` + projectID + `","target_agent_id":"` + workerID + `","role_type":"IMPLEMENTER","write_scope_json":"{\"paths\":[\"src/app/**\",\"src/routes/**\",\"package.json\"]}","active_task_id":"` + taskID + `","branch_id":"` + branch.BranchID + `"}`,
	}, graph); err != nil {
		t.Fatalf("create authority task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      authorityTaskID,
		LinkedBy:    workerID,
	}); err != nil {
		t.Fatalf("attach authority task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                authorityTaskID,
		AgentID:               leadID,
		Summary:               "lead claimed authority carrier",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, authorityTaskID, leadID),
	}); err != nil {
		t.Fatalf("claim authority task: %v", err)
	}

	newRole, event, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/app/**","src/routes/**","package.json"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "lead-approved owner lane boundary",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign corrected role: %v", err)
	}
	for _, want := range []string{authorityTaskID, `"role_scope_authority_transition_lifecycle":"resolved_no_longer_blocking"`, `"resolved_project_role_scope_authority_task_ids"`} {
		if !strings.Contains(event.PayloadJSON, want) {
			t.Fatalf("role assign payload missing %s: %s", want, event.PayloadJSON)
		}
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, authorityTaskID)
	if err != nil {
		t.Fatalf("get authority task status: %v", err)
	}
	if status.Status != model.TaskStatusResolved {
		t.Fatalf("authority task status = %s, want RESOLVED", status.Status)
	}
	var authorityClaimStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, authorityTaskID).Scan(&authorityClaimStatus); err != nil {
		t.Fatalf("query authority claim status: %v", err)
	}
	if authorityClaimStatus != model.TaskClaimStatusCompleted {
		t.Fatalf("authority claim status = %s, want COMPLETED", authorityClaimStatus)
	}

	var claimRoleID, claimStatus, claimScope string
	if err := store.DB().QueryRowContext(ctx, `SELECT project_role_id, claim_status, write_scope_json FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimRoleID, &claimStatus, &claimScope); err != nil {
		t.Fatalf("query rebound owner claim: %v", err)
	}
	if claimRoleID != newRole.RoleID || claimStatus != model.TaskClaimStatusClaimed || claimScope != newRole.WriteScopeJSON {
		t.Fatalf("owner claim role/status/scope = %q %q %q, want %q CLAIMED %q", claimRoleID, claimStatus, claimScope, newRole.RoleID, newRole.WriteScopeJSON)
	}
}

func TestProjectRoleAssignPreservesBlockedDependencyClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-role-rebind-blocked-dependency"
		projectID   = "project-role-rebind-blocked-dependency"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-role-rebind-blocked-dependency"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	oldRole, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/app/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign old role: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\role-rebind-blocked-dependency`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/role-rebind-blocked-dependency",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        oldRole.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		ProjectRoleID:         oldRole.RoleID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        oldRole.WriteScopeJSON,
		Summary:               "claim before upstream dependency",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim with old role: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Reason:                "Blocked on upstream approval from product reviewer before implementation can continue.",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.block", workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("block dependency claim: %v", err)
	}

	newRole, event, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/app/**","src/routes/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "adjust app-shell ownership while dependency remains open",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign adjusted role: %v", err)
	}
	for _, want := range []string{`"previous_claim_status":"BLOCKED"`, `"claim_status":"BLOCKED"`, `"block_type":"blocked_dependency"`, `"block_resolution_state":"dependency_unresolved"`} {
		if !strings.Contains(event.PayloadJSON, want) {
			t.Fatalf("dependency rebind payload missing %s: %s", want, event.PayloadJSON)
		}
	}
	if strings.Contains(event.PayloadJSON, `"preferred_transition":"request_resume"`) {
		t.Fatalf("dependency block must not request owner resume: %s", event.PayloadJSON)
	}
	var claimStatus, claimScope string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status, write_scope_json FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus, &claimScope); err != nil {
		t.Fatalf("query dependency claim: %v", err)
	}
	if claimStatus != "BLOCKED" || claimScope != newRole.WriteScopeJSON {
		t.Fatalf("dependency claim status/scope = %q %q, want BLOCKED %q", claimStatus, claimScope, newRole.WriteScopeJSON)
	}
}

func TestProjectRoleAssignPreservesBlockedSideEffectClassificationClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-role-rebind-blocked-side-effect"
		projectID   = "project-role-rebind-blocked-side-effect"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-role-rebind-blocked-side-effect"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	oldRole, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/editor/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign old role: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\role-rebind-blocked-side-effect`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/role-rebind-blocked-side-effect",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        oldRole.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		ProjectRoleID:         oldRole.RoleID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        oldRole.WriteScopeJSON,
		Summary:               "claim before side-effect classification",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim with old role: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Reason:                "Blocked on side_effect_classification: pending classification for out-of-boundary root scaffold before integration.",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.block", workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("block side-effect classification claim: %v", err)
	}
	newRole, event, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/editor/**","package.json"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "adjust editor ownership while classification remains pending",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign adjusted role: %v", err)
	}
	for _, want := range []string{`"previous_claim_status":"BLOCKED"`, `"claim_status":"BLOCKED"`, `"block_type":"blocked_side_effect_classification"`, `"block_resolution_state":"pending_side_effect_resolution"`} {
		if !strings.Contains(event.PayloadJSON, want) {
			t.Fatalf("side-effect rebind payload missing %s: %s", want, event.PayloadJSON)
		}
	}
	if strings.Contains(event.PayloadJSON, `"preferred_transition":"request_resume"`) {
		t.Fatalf("side-effect classification block must not request owner resume: %s", event.PayloadJSON)
	}
	var claimStatus, claimScope string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status, write_scope_json FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus, &claimScope); err != nil {
		t.Fatalf("query side-effect claim: %v", err)
	}
	if claimStatus != "BLOCKED" || claimScope != newRole.WriteScopeJSON {
		t.Fatalf("side-effect claim status/scope = %q %q, want BLOCKED %q", claimStatus, claimScope, newRole.WriteScopeJSON)
	}
}

func TestProjectRoleAssignResolvesBlockedSideEffectClassificationAfterBoundaryExpansion(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-role-rebind-side-effect-expanded"
		projectID   = "project-role-rebind-side-effect-expanded"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		taskID      = "task-role-rebind-side-effect-expanded"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	oldRole, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/editor/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign old role: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\role-rebind-side-effect-expanded`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/role-rebind-side-effect-expanded",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        oldRole.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		ProjectRoleID:         oldRole.RoleID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        oldRole.WriteScopeJSON,
		Summary:               "claim before side-effect classification",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim with old role: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		Reason:                "Blocked on side_effect_classification: pending classification for out-of-boundary root scaffold before integration.",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.block", workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("block side-effect classification claim: %v", err)
	}
	const classificationTaskID = "task-side-effect-classify-role-rebind-expanded"
	classificationGraph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "classification", Type: "generic"}}})
	if err := dag.ValidateGraph(classificationGraph); err != nil {
		t.Fatalf("validate classification graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               classificationTaskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Classify side effects for blocked branch",
		Description:          "Classify the out-of-boundary scaffold side effect before retrying integration.",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		TaskClass:            "INCIDENT",
		TaskClassSource:      "EXPLICIT",
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		RequiresProjectGate:  false,
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification","source_kind":"adapter:git","source_tool":"project_branch_commit","branch_id":"` + branch.BranchID + `","owner_agent_id":"` + workerID + `","active_task_id":"` + taskID + `","side_effect_refs":["side-effect:role-rebind"],"dirty_paths":["package.json","vite.config.ts"]}`,
	}, classificationGraph); err != nil {
		t.Fatalf("create side-effect classification task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      classificationTaskID,
		LinkedBy:    workerID,
	}); err != nil {
		t.Fatalf("attach side-effect classification task: %v", err)
	}
	const otherBranchClassificationTaskID = "task-side-effect-classify-other-branch"
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               otherBranchClassificationTaskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Classify side effects for another branch",
		Description:          "This classification belongs to the same task but another branch and must not be collapsed by this rebind.",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		TaskClass:            "INCIDENT",
		TaskClassSource:      "EXPLICIT",
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		RequiresProjectGate:  false,
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification","source_kind":"adapter:git","source_tool":"project_branch_commit","branch_id":"branch-other","owner_agent_id":"` + workerID + `","active_task_id":"` + taskID + `","side_effect_refs":["side-effect:other-branch"],"dirty_paths":["README.md"]}`,
	}, classificationGraph); err != nil {
		t.Fatalf("create other-branch side-effect classification task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      otherBranchClassificationTaskID,
		LinkedBy:    workerID,
	}); err != nil {
		t.Fatalf("attach other-branch side-effect classification task: %v", err)
	}
	const uncoveredClassificationTaskID = "task-side-effect-classify-uncovered"
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               uncoveredClassificationTaskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Classify uncovered side effect",
		Description:          "This classification belongs to the same task and branch but is not covered by the applied boundary expansion.",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		TaskClass:            "INCIDENT",
		TaskClassSource:      "EXPLICIT",
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		RequiresProjectGate:  false,
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification","source_kind":"adapter:git","source_tool":"project_branch_commit","branch_id":"` + branch.BranchID + `","owner_agent_id":"` + workerID + `","active_task_id":"` + taskID + `","side_effect_refs":["side-effect:uncovered"],"dirty_paths":["README.md"]}`,
	}, classificationGraph); err != nil {
		t.Fatalf("create uncovered side-effect classification task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      uncoveredClassificationTaskID,
		LinkedBy:    workerID,
	}); err != nil {
		t.Fatalf("attach uncovered side-effect classification task: %v", err)
	}
	createRecoverySuccessor := func(taskIDValue, branchIDValue, pathsJSON string) {
		t.Helper()
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:          workspaceID,
			TaskID:               taskIDValue,
			OwnerUserID:          "developer",
			Priority:             "high",
			Title:                "Verify side-effect recovery bucket",
			Description:          "ABPC recovery action: verify bucket before retrying the owner lane.",
			TaskKind:             "EXECUTION",
			TaskTemplate:         "generic",
			TaskClass:            "PROOF",
			TaskClassSource:      "EXPLICIT",
			ProjectID:            projectID,
			ProjectLane:          "verification",
			RequiresProjectGate:  false,
			Tags:                 []string{"side-effect-resolution", "verification", "abpc"},
			WriteScopeHints:      []string{"package.json", "vite.config.ts"},
			TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_verification","action_kind":"verify_bucket","decision":"request_verification","parent_classifier_task_id":"` + classificationTaskID + `","classification_task_id":"` + classificationTaskID + `","side_effect_refs":["side-effect:role-rebind"],"active_task_id":"` + taskID + `","branch_id":"` + branchIDValue + `","owner_agent_id":"","target_agent_id":"` + workerID + `","dirty_paths":` + pathsJSON + `,"path_bucket":` + pathsJSON + `,"next_transition":"route_to_verifier"}`,
		}, classificationGraph); err != nil {
			t.Fatalf("create side-effect recovery successor task %s: %v", taskIDValue, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      taskIDValue,
			LinkedBy:    workerID,
		}); err != nil {
			t.Fatalf("attach side-effect recovery successor task %s: %v", taskIDValue, err)
		}
	}
	const coveredSuccessorTaskID = "task-side-effect-successor-covered"
	createRecoverySuccessor(coveredSuccessorTaskID, branch.BranchID, `["package.json","vite.config.ts"]`)
	const uncoveredSuccessorTaskID = "task-side-effect-successor-uncovered"
	createRecoverySuccessor(uncoveredSuccessorTaskID, branch.BranchID, `["README.md"]`)
	const otherBranchSuccessorTaskID = "task-side-effect-successor-other-branch"
	createRecoverySuccessor(otherBranchSuccessorTaskID, "branch-other", `["package.json","vite.config.ts"]`)

	newRole, event, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/editor/**","package.json","vite.config.*"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "ABPC side-effect resolution expand_boundary: lead-approved boundary expansion for scaffold side effect.",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign expanded role: %v", err)
	}
	for _, want := range []string{`"previous_claim_status":"BLOCKED"`, `"claim_status":"CLAIMED"`, `"block_type":"blocked_side_effect_classification"`, `"block_resolution_state":"side_effect_boundary_resolved"`, `"preferred_transition":"request_resume"`, `"side_effect_classification_lifecycle":"resolved_no_longer_blocking"`, `"side_effect_recovery_successor_lifecycle":"cancelled_no_longer_blocking"`, classificationTaskID, coveredSuccessorTaskID} {
		if !strings.Contains(event.PayloadJSON, want) {
			t.Fatalf("side-effect expansion rebind payload missing %s: %s", want, event.PayloadJSON)
		}
	}

	var claimStatus, claimScope, claimSummary string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status, write_scope_json, summary FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus, &claimScope, &claimSummary); err != nil {
		t.Fatalf("query side-effect claim: %v", err)
	}
	if claimStatus != "CLAIMED" || claimScope != newRole.WriteScopeJSON {
		t.Fatalf("side-effect claim status/scope = %q %q, want CLAIMED %q", claimStatus, claimScope, newRole.WriteScopeJSON)
	}
	if !strings.Contains(claimSummary, "ABPC boundary expansion resolved") {
		t.Fatalf("expected boundary expansion resume summary, got %q", claimSummary)
	}
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{WorkspaceID: workspaceID, ProjectID: projectID, RepoID: repoID, AgentID: workerID})
	if err != nil {
		t.Fatalf("list branches after side-effect expansion: %v", err)
	}
	if len(branches) != 1 || branches[0].BranchID != branch.BranchID || branches[0].WriteScopeJSON != newRole.WriteScopeJSON || branches[0].ActiveTaskID != taskID || branches[0].ActiveClaimID != taskID {
		t.Fatalf("expected active branch boundary to follow side-effect expansion, got %+v", branches)
	}
	classificationStatus, err := store.GetTaskStatus(ctx, workspaceID, classificationTaskID)
	if err != nil {
		t.Fatalf("get classification task status: %v", err)
	}
	if classificationStatus.Status != model.TaskStatusResolved {
		t.Fatalf("classification task should collapse after boundary expansion, got %+v", classificationStatus)
	}
	coveredSuccessorStatus, err := store.GetTaskStatus(ctx, workspaceID, coveredSuccessorTaskID)
	if err != nil {
		t.Fatalf("get covered recovery successor task status: %v", err)
	}
	if coveredSuccessorStatus.Status != model.TaskStatusCancelled {
		t.Fatalf("covered recovery successor should collapse after boundary expansion, got %+v", coveredSuccessorStatus)
	}
	uncoveredSuccessorStatus, err := store.GetTaskStatus(ctx, workspaceID, uncoveredSuccessorTaskID)
	if err != nil {
		t.Fatalf("get uncovered recovery successor task status: %v", err)
	}
	if uncoveredSuccessorStatus.Status != model.TaskStatusPending {
		t.Fatalf("uncovered recovery successor should remain pending, got %+v", uncoveredSuccessorStatus)
	}
	otherBranchSuccessorStatus, err := store.GetTaskStatus(ctx, workspaceID, otherBranchSuccessorTaskID)
	if err != nil {
		t.Fatalf("get other-branch recovery successor task status: %v", err)
	}
	if otherBranchSuccessorStatus.Status != model.TaskStatusPending {
		t.Fatalf("other-branch recovery successor should remain pending, got %+v", otherBranchSuccessorStatus)
	}
	otherClassificationStatus, err := store.GetTaskStatus(ctx, workspaceID, otherBranchClassificationTaskID)
	if err != nil {
		t.Fatalf("get other classification task status: %v", err)
	}
	if otherClassificationStatus.Status != model.TaskStatusPending {
		t.Fatalf("classification task for another branch should remain pending, got %+v", otherClassificationStatus)
	}
	uncoveredClassificationStatus, err := store.GetTaskStatus(ctx, workspaceID, uncoveredClassificationTaskID)
	if err != nil {
		t.Fatalf("get uncovered classification task status: %v", err)
	}
	if uncoveredClassificationStatus.Status != model.TaskStatusPending {
		t.Fatalf("classification task for uncovered same-branch side effect should remain pending, got %+v", uncoveredClassificationStatus)
	}
}

func TestProjectRoleAssignBlocksCrossOwnerLiveScopeOverlap(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-role-rebind-overlap"
		projectID   = "project-role-rebind-overlap"
		leadID      = "lead-agent"
		workerAID   = "worker-a"
		workerBID   = "worker-b"
		workerCID   = "worker-c"
		repoID      = "repo-main"
		taskAID     = "task-role-rebind-overlap-a"
		taskBID     = "task-role-rebind-overlap-b"
		taskCID     = "task-role-rebind-overlap-c"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerAID, workerBID, workerCID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskAID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskBID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskCID)

	oldRoleA, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerAID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/editor/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "initial editor scope",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign old role A: %v", err)
	}
	roleB, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerBID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/panels/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "panels scope",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign role B: %v", err)
	}
	roleC, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerCID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "fresh overlap probe",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign role C: %v", err)
	}

	checkoutA := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerAID, `C:\fixtures\agents\worker-a\role-rebind-overlap`)
	branchA, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutA.CheckoutID,
		AgentID:               workerAID,
		BranchName:            "agent/worker-a/role-rebind-overlap",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        oldRoleA.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerAID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerAID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch A: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskAID,
		AgentID:               workerAID,
		ProjectRoleID:         oldRoleA.RoleID,
		RepoID:                repoID,
		CheckoutID:            checkoutA.CheckoutID,
		BranchID:              branchA.BranchID,
		WriteScopeJSON:        oldRoleA.WriteScopeJSON,
		Summary:               "claim A with narrow scope",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskAID, workerAID),
	}); err != nil {
		t.Fatalf("claim A: %v", err)
	}

	checkoutB := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerBID, `C:\fixtures\agents\worker-b\role-rebind-overlap`)
	branchB, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		AgentID:               workerBID,
		BranchName:            "agent/worker-b/role-rebind-overlap",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        roleB.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerBID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerBID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch B: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskBID,
		AgentID:               workerBID,
		ProjectRoleID:         roleB.RoleID,
		RepoID:                repoID,
		CheckoutID:            checkoutB.CheckoutID,
		BranchID:              branchB.BranchID,
		WriteScopeJSON:        roleB.WriteScopeJSON,
		Summary:               "claim B with sibling scope",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskBID, workerBID),
	}); err != nil {
		t.Fatalf("claim B: %v", err)
	}

	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerAID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/**","package.json"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "lead-approved broad correction",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); !errors.Is(err, sqlite.ErrProjectRoleWriteScopeConflict) {
		t.Fatalf("expected broad correction to be blocked by worker B live scope, got %v", err)
	}

	var claimARoleID, claimAScope, claimBRoleID, claimBScope string
	if err := store.DB().QueryRowContext(ctx, `SELECT project_role_id, write_scope_json FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskAID).Scan(&claimARoleID, &claimAScope); err != nil {
		t.Fatalf("query claim A: %v", err)
	}
	if claimARoleID != oldRoleA.RoleID || claimAScope != oldRoleA.WriteScopeJSON {
		t.Fatalf("blocked correction should leave claim A unchanged, got role/scope = %q %q", claimARoleID, claimAScope)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT project_role_id, write_scope_json FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskBID).Scan(&claimBRoleID, &claimBScope); err != nil {
		t.Fatalf("query claim B: %v", err)
	}
	if claimBRoleID != roleB.RoleID || claimBScope != roleB.WriteScopeJSON {
		t.Fatalf("claim B should remain unchanged, got role/scope = %q %q", claimBRoleID, claimBScope)
	}
	branchesA, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{WorkspaceID: workspaceID, ProjectID: projectID, RepoID: repoID, AgentID: workerAID})
	if err != nil {
		t.Fatalf("list branches A: %v", err)
	}
	if len(branchesA) != 1 || branchesA[0].BranchID != branchA.BranchID || branchesA[0].WriteScopeJSON != oldRoleA.WriteScopeJSON {
		t.Fatalf("blocked correction should leave branch A unchanged, got %+v", branchesA)
	}
	branchesB, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{WorkspaceID: workspaceID, ProjectID: projectID, RepoID: repoID, AgentID: workerBID})
	if err != nil {
		t.Fatalf("list branches B: %v", err)
	}
	if len(branchesB) != 1 || branchesB[0].BranchID != branchB.BranchID || branchesB[0].WriteScopeJSON != roleB.WriteScopeJSON {
		t.Fatalf("branch B should remain unchanged, got %+v", branchesB)
	}

	newRoleA, event, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerAID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/editor/**","tests/editor/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		Summary:               "lead-approved non-overlapping correction",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign non-overlapping corrected role A: %v", err)
	}
	if !strings.Contains(event.PayloadJSON, `"state":"updated"`) || strings.Contains(event.PayloadJSON, "write_scope_overlap_advisory") {
		t.Fatalf("non-overlapping role assignment should rebind without overlap advisory, got %s", event.PayloadJSON)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT project_role_id, write_scope_json FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskAID).Scan(&claimARoleID, &claimAScope); err != nil {
		t.Fatalf("query rebound claim A: %v", err)
	}
	if claimARoleID != newRoleA.RoleID || claimAScope != newRoleA.WriteScopeJSON {
		t.Fatalf("claim A role/scope = %q %q, want %q %q", claimARoleID, claimAScope, newRoleA.RoleID, newRoleA.WriteScopeJSON)
	}
	branchesA, err = store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{WorkspaceID: workspaceID, ProjectID: projectID, RepoID: repoID, AgentID: workerAID})
	if err != nil {
		t.Fatalf("list branches A after rebind: %v", err)
	}
	if len(branchesA) != 1 || branchesA[0].BranchID != branchA.BranchID || branchesA[0].WriteScopeJSON != newRoleA.WriteScopeJSON {
		t.Fatalf("branch A should follow non-overlapping corrected role, got %+v", branchesA)
	}

	checkoutC := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerCID, `C:\fixtures\agents\worker-c\role-rebind-overlap`)
	branchC, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutC.CheckoutID,
		AgentID:               workerCID,
		BranchName:            "agent/worker-c/role-rebind-overlap",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        roleC.WriteScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerCID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerCID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch C: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskCID,
		AgentID:               workerCID,
		ProjectRoleID:         roleC.RoleID,
		RepoID:                repoID,
		CheckoutID:            checkoutC.CheckoutID,
		BranchID:              branchC.BranchID,
		WriteScopeJSON:        roleC.WriteScopeJSON,
		Summary:               "fresh overlapping claim should still be rejected",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskCID, workerCID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) {
		t.Fatalf("fresh overlapping claim should remain strict, got %v", err)
	}
}

func containsForTest(value, needle string) bool {
	return strings.Contains(value, needle)
}

func branchListContainsForTest(branches []sqlite.ProjectBranchRecord, branchID string) bool {
	for _, branch := range branches {
		if branch.BranchID == branchID {
			return true
		}
	}
	return false
}

func findWorkspaceTaskForBranchTest(t *testing.T, tasks []sqlite.WorkspaceTaskRecord, taskID string) sqlite.WorkspaceTaskRecord {
	t.Helper()
	for _, task := range tasks {
		if task.TaskID == taskID {
			return task
		}
	}
	t.Fatalf("task %q not found in %+v", taskID, tasks)
	return sqlite.WorkspaceTaskRecord{}
}

func createAcceptedReadyBranchForMergeTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, workerID, integratorID, branchID, branchName, headSHA, baseSHA, localPath string) sqlite.ProjectBranchRecord {
	t.Helper()
	reviewKey := "project." + projectID + ".branch." + branchID + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for integration review.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, localPath)
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, branchID, branchName, `{"paths":["src/**"]}`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branchID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            branchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ready branch: %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit patch queue item: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim patch queue item: %v", err)
	}
	decisionKey := "project." + projectID + ".branch." + branchID + ".decision"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue Decision",
		Content:     "# Patch Queue Decision\n\nAccepted.",
		UpdatedBy:   integratorID,
	}); err != nil {
		t.Fatalf("write decision doc: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        decisionKey,
		DecisionSummary:       "Accepted source branch for canonical integration.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("accept patch queue item: %v", err)
	}
	return branch
}

func recordIntegratedReceiptForAcceptedBranchForMergeTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID string, branch sqlite.ProjectBranchRecord, actorID, targetBranch, targetHeadBefore, targetHeadAfter string) sqlite.RuntimeEventRecord {
	t.Helper()
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    branch.BranchID,
		State:       sqlite.ProjectPatchQueueStateAccepted,
	})
	if err != nil {
		t.Fatalf("list accepted patch queue item for branch %s: %v", branch.BranchID, err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one accepted patch queue item for branch %s, got %+v", branch.BranchID, items)
	}
	item := items[0]
	if targetBranch == "" {
		targetBranch = "main"
	}
	if targetHeadBefore == "" {
		targetHeadBefore = branch.BaseSHA
	}
	if targetHeadAfter == "" {
		targetHeadAfter = branch.HeadSHA
	}
	_, event, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               actorID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetBranch:          targetBranch,
		TargetHeadBefore:      targetHeadBefore,
		TargetHeadAfter:       targetHeadAfter,
		RemoteTargetHeadAfter: targetHeadAfter,
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		PushAttempted:         true,
		PushSucceeded:         true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("record integrated receipt for branch %s: %v", branch.BranchID, err)
	}
	if event.EventType != sqlite.ProjectPatchQueueIntegratedEventType {
		t.Fatalf("expected integrated receipt event, got %+v", event)
	}
	return event
}

func closeBranchAsMergedForTest(ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID string, branch sqlite.ProjectBranchRecord, ownerAgentID, actorID string) (sqlite.ProjectBranchRecord, sqlite.RuntimeEventRecord, error) {
	return store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		CheckoutID:            branch.CheckoutID,
		AgentID:               ownerAgentID,
		BranchName:            branch.BranchName,
		BranchKind:            branch.BranchKind,
		BaseBranch:            branch.BaseBranch,
		BaseSHA:               branch.BaseSHA,
		HeadSHA:               branch.HeadSHA,
		WriteScopeJSON:        branch.WriteScopeJSON,
		ReviewDocKey:          branch.ReviewDocKey,
		Status:                sqlite.ProjectBranchStatusMerged,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.branch.register",
	})
}

func transitionProjectPhaseForBranchTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID, phase string) {
	t.Helper()
	profile, err := store.GetProjectProfile(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get project profile before phase transition to %s: %v", phase, err)
	}
	if profile.CurrentPhase == phase {
		return
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               phase,
		Reason:                "branch admission test phase transition",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase to %s: %v", phase, err)
	}
}

func registerCheckoutForBranchTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, agentID, path string) sqlite.ProjectCheckoutRecord {
	t.Helper()
	checkout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               agentID,
		LocalPath:             path,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               agentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register checkout for %s: %v", agentID, err)
	}
	return checkout
}
