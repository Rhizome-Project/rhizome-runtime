package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestReclaimExecutableNodesAfterRestartResetsRunningNodeToPending(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-exec-reclaim"
		taskID      = "task-sh1b-exec-reclaim"
		nodeID      = "node-sh1b-exec-reclaim"
	)

	createSingleNodeTask(t, ctx, store, taskID, nodeID)
	createExecutionWorkspaceAttachment(t, ctx, store, workspaceID, taskID)

	claimed, err := store.ClaimExecutableNodes(ctx, 5, "daemon-before-crash")
	if err != nil {
		t.Fatalf("claim executable nodes: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected one claimed executable node, got %+v", claimed)
	}
	assertDagNodeStatusAndAttempt(t, ctx, store, taskID, nodeID, model.NodeStatusRunning, 1)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)

	reclaimed, err := store.ReclaimExecutableNodesAfterRestart(ctx, "daemon-after-restart")
	if err != nil {
		t.Fatalf("reclaim executable nodes after restart: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("expected one reclaimed executable node, got %+v", reclaimed)
	}
	if reclaimed[0].WorkspaceID != workspaceID || reclaimed[0].TaskID != taskID || reclaimed[0].NodeID != nodeID {
		t.Fatalf("unexpected reclaimed executable node %+v", reclaimed[0])
	}
	if reclaimed[0].Status != model.NodeStatusPending {
		t.Fatalf("expected reclaimed executable node to be pending, got %+v", reclaimed[0])
	}

	assertDagNodeStatusAndAttempt(t, ctx, store, taskID, nodeID, model.NodeStatusPending, 1)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusPending)
}

func TestReclaimExecutableNodesAfterRestartRejectsExpiredAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-exec-reclaim-expired"
		taskID      = "task-sh1b-exec-reclaim-expired"
		nodeID      = "node-sh1b-exec-reclaim-expired"
	)

	createSingleNodeTask(t, ctx, store, taskID, nodeID)
	createExecutionWorkspaceAttachment(t, ctx, store, workspaceID, taskID)

	claimed, err := store.ClaimExecutableNodes(ctx, 5, "daemon-before-expiry")
	if err != nil {
		t.Fatalf("claim executable nodes: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected one claimed executable node, got %+v", claimed)
	}

	if _, err := store.ForceBreakWorkspaceAuthority(ctx, sqlite.ForceBreakWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "operator",
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("force-break workspace authority: %v", err)
	}

	if _, err := store.ReclaimExecutableNodesAfterRestart(ctx, "daemon-after-expiry"); err == nil {
		t.Fatal("expected expired authority to reject executable reclaim")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject == nil || reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected authority_lease_expired reject, got %v", err)
	}

	assertDagNodeStatusAndAttempt(t, ctx, store, taskID, nodeID, model.NodeStatusRunning, 1)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)
}

func TestReclaimExecutableNodesAfterRestartIsIdempotent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-sh1b-exec-reclaim-idempotent"
		taskID      = "task-sh1b-exec-reclaim-idempotent"
		nodeID      = "node-sh1b-exec-reclaim-idempotent"
	)

	createSingleNodeTask(t, ctx, store, taskID, nodeID)
	createExecutionWorkspaceAttachment(t, ctx, store, workspaceID, taskID)

	claimed, err := store.ClaimExecutableNodes(ctx, 5, "daemon-before-crash")
	if err != nil {
		t.Fatalf("claim executable nodes: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected one claimed executable node, got %+v", claimed)
	}

	first, err := store.ReclaimExecutableNodesAfterRestart(ctx, "daemon-after-restart")
	if err != nil {
		t.Fatalf("first reclaim executable nodes after restart: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("expected one reclaimed executable node, got %+v", first)
	}

	second, err := store.ReclaimExecutableNodesAfterRestart(ctx, "daemon-after-restart")
	if err != nil {
		t.Fatalf("second reclaim executable nodes after restart: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("expected second reclaim to be a no-op, got %+v", second)
	}

	assertDagNodeStatusAndAttempt(t, ctx, store, taskID, nodeID, model.NodeStatusPending, 1)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusPending)
}

func createExecutionWorkspaceAttachment(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH1B Execution Reclaim Workspace",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "tests",
	}); err != nil {
		t.Fatalf("attach task to workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
}
