package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestClaimExecutableNodesClaimsOnlyAuthorizedSingleWorkspaceTasks(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	seedD1CExecutableTaskFixture(t, ctx, store, "ws-d1c-exec-local", "task-d1c-exec-local", "node-1", true)
	seedD1CExecutableTaskFixture(t, ctx, store, "ws-d1c-exec-foreign", "task-d1c-exec-foreign", "node-1", false)

	claimed, err := store.ClaimExecutableNodes(ctx, 10, "daemon-test")
	if err != nil {
		t.Fatalf("claim executable nodes: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected one claimed node, got %+v", claimed)
	}
	if claimed[0].WorkspaceID != "ws-d1c-exec-local" || claimed[0].TaskID != "task-d1c-exec-local" || claimed[0].NodeID != "node-1" {
		t.Fatalf("unexpected claimed node %+v", claimed[0])
	}

	assertDagNodeStatusAndAttempt(t, ctx, store, "task-d1c-exec-local", "node-1", model.NodeStatusRunning, 1)
	assertTaskStatus(t, ctx, store, "task-d1c-exec-local", model.TaskStatusRunning)
	assertDagNodeStatusAndAttempt(t, ctx, store, "task-d1c-exec-foreign", "node-1", model.NodeStatusPending, 0)
	assertTaskStatus(t, ctx, store, "task-d1c-exec-foreign", model.TaskStatusPending)
}

func TestClaimExecutableNodesSkipsProjectGatedExecutionTasks(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-d1c-project-gated-exec"
		projectID   = "project-d1c-project-gated-exec"
		leadID      = "lead-d1c-project-gated-exec"
		gatedTaskID = "task-d1c-project-gated-exec"
		plainTaskID = "task-d1c-plain-exec"
		nodeID      = "node-1"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "compute"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              gatedTaskID,
		OwnerUserID:         "tests",
		Priority:            "critical",
		TaskKind:            model.TaskKindExecution,
		TaskTemplate:        model.TaskTemplateGeneric,
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create project-gated executable task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      gatedTaskID,
		LinkedBy:    "tests",
	}); err != nil {
		t.Fatalf("attach project-gated task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:       plainTaskID,
		OwnerUserID:  "tests",
		Priority:     "normal",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateGeneric,
	}, graph); err != nil {
		t.Fatalf("create plain executable task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      plainTaskID,
		LinkedBy:    "tests",
	}); err != nil {
		t.Fatalf("attach plain task: %v", err)
	}

	claimed, err := store.ClaimExecutableNodes(ctx, 10, "daemon-test")
	if err != nil {
		t.Fatalf("claim executable nodes: %v", err)
	}
	if len(claimed) != 1 || claimed[0].TaskID != plainTaskID {
		t.Fatalf("expected only plain executable task to be claimed, got %+v", claimed)
	}

	assertDagNodeStatusAndAttempt(t, ctx, store, gatedTaskID, nodeID, model.NodeStatusPending, 0)
	assertTaskStatus(t, ctx, store, gatedTaskID, model.TaskStatusPending)
	assertDagNodeStatusAndAttempt(t, ctx, store, plainTaskID, nodeID, model.NodeStatusRunning, 1)
	assertTaskStatus(t, ctx, store, plainTaskID, model.TaskStatusRunning)
}

func TestClaimExecutableNodesSkipsAmbiguousWorkspaceAttachment(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		taskID = "task-d1c-exec-ambiguous"
		nodeID = "node-1"
	)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "compute"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "tests",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}

	for _, workspaceID := range []string{"ws-d1c-exec-ambiguous-a", "ws-d1c-exec-ambiguous-b"} {
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "Ambiguous executable task",
			CreatedBy:   "tests",
		}); err != nil {
			t.Fatalf("create workspace %s: %v", workspaceID, err)
		}
		claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			LinkedBy:    "tests",
		}); err != nil {
			t.Fatalf("attach task to workspace %s: %v", workspaceID, err)
		}
	}

	claimed, err := store.ClaimExecutableNodes(ctx, 10, "daemon-test")
	if err != nil {
		t.Fatalf("claim executable nodes: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("expected ambiguous multi-workspace task to be skipped, got %+v", claimed)
	}

	assertDagNodeStatusAndAttempt(t, ctx, store, taskID, nodeID, model.NodeStatusPending, 0)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusPending)
}

func TestClaimExecutableNodesRejectsExpiredAuthorityWorkspaceInsteadOfSkippingMixedBatch(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		expiredWorkspaceID = "ws-d1c-exec-expired-mixed"
		expiredTaskID      = "task-d1c-exec-expired-mixed"
		healthyWorkspaceID = "ws-d1c-exec-healthy-mixed"
		healthyTaskID      = "task-d1c-exec-healthy-mixed"
		nodeID             = "node-1"
	)

	seedD1CExecutableTaskFixture(t, ctx, store, expiredWorkspaceID, expiredTaskID, nodeID, true)
	current, err := store.GetWorkspaceAuthority(ctx, expiredWorkspaceID, "workspace")
	if err != nil {
		t.Fatalf("get expired workspace authority seed: %v", err)
	}
	beforeRejects := countAuthorityRejectEvents(t, ctx, store, expiredWorkspaceID)
	expireExternalWorkspaceAuthority(t, ctx, store, expiredWorkspaceID, current)

	seedD1CExecutableTaskFixture(t, ctx, store, healthyWorkspaceID, healthyTaskID, nodeID, true)

	claimed, err := store.ClaimExecutableNodes(ctx, 10, "daemon-test")
	if err == nil {
		t.Fatalf("expected authority reject for mixed batch with expired local workspace, got claims %+v", claimed)
	}
	assertLeaseExpiredReject(t, err)
	if len(claimed) != 0 {
		t.Fatalf("expected mixed batch to roll back all claims on authority reject, got %+v", claimed)
	}
	assertAuthorityRejectEventIncrement(t, ctx, store, expiredWorkspaceID, beforeRejects, sqlite.AuthorityRejectLeaseExpired)
	assertDagNodeStatusAndAttempt(t, ctx, store, expiredTaskID, nodeID, model.NodeStatusPending, 0)
	assertTaskStatus(t, ctx, store, expiredTaskID, model.TaskStatusPending)
	assertDagNodeStatusAndAttempt(t, ctx, store, healthyTaskID, nodeID, model.NodeStatusPending, 0)
	assertTaskStatus(t, ctx, store, healthyTaskID, model.TaskStatusPending)
}

func seedD1CExecutableTaskFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, nodeID string, claimAuthority bool) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "compute"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "tests",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Executable claim authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if claimAuthority {
		claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "tests",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
}
