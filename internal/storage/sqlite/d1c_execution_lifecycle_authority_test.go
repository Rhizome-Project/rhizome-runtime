package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestUpdateTaskStatusFromNodesWithWorkspaceAuthorityRejectsMissingAuthority(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-task-status-missing"
		taskID      = "task-d1c-task-status-missing"
		nodeID      = "node-1"
	)
	seedD1CExecutableTaskFixture(t, ctx, store, workspaceID, taskID, nodeID, false)

	_, err := store.UpdateTaskStatusFromNodesWithWorkspaceAuthority(ctx, workspaceID, taskID, "daemon-test", "node_execution_claimed")
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", reject)
	}

	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusPending)
	assertDagNodeStatusAndAttempt(t, ctx, store, taskID, nodeID, model.NodeStatusPending, 0)
}

func TestSetNodeStatusAndUpdateTaskStatusWithWorkspaceAuthorityRejectsExpiredAuthority(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d1c-node-status-expired"
		taskID      = "task-d1c-node-status-expired"
		nodeID      = "node-1"
	)
	seedD1CExecutableTaskFixture(t, ctx, store, workspaceID, taskID, nodeID, true)
	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	referenceAt := time.Now().UTC().Round(0)
	if _, _, err := store.ExpireWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityExpireInput{
		WorkspaceID:           workspaceID,
		Scope:                 "workspace",
		HolderAuthorityNodeID: current.HolderAuthorityNodeID,
		LeaseToken:            current.LeaseToken,
		Term:                  current.Term,
		CommitWatermark:       current.CommitWatermark,
		AppliedWatermark:      current.AppliedWatermark,
		ActorType:             "system",
		ActorID:               "tests",
		ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("expire workspace authority: %v", err)
	}

	_, err := store.SetNodeStatusAndUpdateTaskStatusWithWorkspaceAuthority(ctx, workspaceID, sqlite.NodeStatusUpdateInput{
		TaskID:    taskID,
		NodeID:    nodeID,
		NewStatus: model.NodeStatusRunning,
		Reason:    "node_execution_claimed",
		ActorID:   "daemon-test",
	}, "node_execution_claimed")
	if err == nil {
		t.Fatal("expected expired workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected lease-expired authority reject, got %+v", reject)
	}

	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusPending)
	assertDagNodeStatusAndAttempt(t, ctx, store, taskID, nodeID, model.NodeStatusPending, 0)
}
