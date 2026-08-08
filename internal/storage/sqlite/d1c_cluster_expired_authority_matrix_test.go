package sqlite_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestFirstProtectedMutationClusterRejectsExpiredAuthorityAcrossRepresentativeWrites(t *testing.T) {
	t.Parallel()

	t.Run("control command request", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		workspaceID := "ws-d1c-expired-control-command"
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "D1C Expired Control Command",
			CreatedBy:   "tests",
		}); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
		current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
		beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
		expireExternalWorkspaceAuthority(t, ctx, store, workspaceID, current)

		_, _, err := store.RequestControlCommandWithEvent(ctx, sqlite.ControlCommandInput{
			WorkspaceID: workspaceID,
			CommandType: sqlite.ControlCommandRefreshKernel,
			AgentID:     "agent-a",
			Reason:      "expired authority should fail closed",
			RequestedBy: "operator-a",
		})
		assertLeaseExpiredReject(t, err)
		assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectLeaseExpired)
		if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   "control.command.requested",
			EntityType:  "control_command",
			Limit:       10,
		}); got != 0 {
			t.Fatalf("expected no control.command.requested rows under expired authority, got %d", got)
		}
	})

	t.Run("queue upsert", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		workspaceID := "ws-d1c-expired-queue-upsert"
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "D1C Expired Queue Upsert",
			CreatedBy:   "tests",
		}); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
		current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
		beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
		expireExternalWorkspaceAuthority(t, ctx, store, workspaceID, current)

		_, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
			WorkspaceID: workspaceID,
			QueueKey:    "queue:d1c-expired-upsert",
			QueueType:   "generic",
			Title:       "Expired authority queue upsert",
			Summary:     "Expired authority queue upsert",
			Details:     "should fail closed",
			AssignedTo:  "reviewer-a",
			Urgency:     "HIGH",
			SourceKind:  "operator",
			SourceID:    "source-d1c-expired-upsert",
		})
		assertLeaseExpiredReject(t, err)
		assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectLeaseExpired)
		if items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{WorkspaceID: workspaceID, Limit: 10}); err != nil {
			t.Fatalf("list operator queue after expired authority reject: %v", err)
		} else if len(items) != 0 {
			t.Fatalf("expected no operator queue rows under expired authority, got %+v", items)
		}
	})

	t.Run("action create", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()

		const (
			workspaceID = "ws-d1c-expired-action-create"
			taskID      = "task-d1c-expired-action-create"
			agentID     = "agent-d1c-expired-action-create"
		)
		current := seedD1CActionAuthorityFixture(t, ctx, store, workspaceID, taskID, agentID)
		beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
		expireExternalWorkspaceAuthority(t, ctx, store, workspaceID, current)

		_, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			AgentID:     agentID,
			AssignedTo:  "reviewer-a",
			Title:       "Expired authority action create",
			Description: "should fail closed",
			Blocking:    true,
		})
		assertLeaseExpiredReject(t, err)
		assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectLeaseExpired)
		assertHumanActionCount(t, ctx, store, workspaceID, 0)
		assertTaskClaimBlockerCount(t, ctx, store, workspaceID, taskID, 0)
		assertHumanActionRuntimeEventCount(t, ctx, store, workspaceID, 0)
	})

	t.Run("node claim", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()

		const (
			workspaceID = "ws-d1c-expired-node-claim"
			taskID      = "task-d1c-expired-node-claim"
			nodeID      = "node-expired"
			agentID     = "agent-d1c-expired-node-claim"
		)
		seedD1CExecutableTaskFixture(t, ctx, store, workspaceID, taskID, nodeID, false)
		current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
		beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
		expireExternalWorkspaceAuthority(t, ctx, store, workspaceID, current)

		err := store.ClaimNode(ctx, sqlite.NodeClaimInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			NodeID:      nodeID,
			AgentID:     agentID,
			Summary:     "expired authority should fail closed",
		})
		assertLeaseExpiredReject(t, err)
		assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectLeaseExpired)
		assertTaskStatus(t, ctx, store, taskID, model.TaskStatusPending)
		assertDagNodeStatusAndAttempt(t, ctx, store, taskID, nodeID, model.NodeStatusPending, 0)
	})

	t.Run("capability policy put", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		workspaceID := "ws-d1c-expired-capability-policy"
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       "D1C Expired Capability Policy",
			CreatedBy:   "tests",
		}); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
		current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
		beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
		expireExternalWorkspaceAuthority(t, ctx, store, workspaceID, current)

		_, _, err := store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
			WorkspaceID: workspaceID,
			SubjectType: "workspace",
			SubjectID:   workspaceID,
			Capability:  "rsp.governed_hints.live",
			ToolID:      "*",
			Effect:      "ALLOW",
			Reason:      "expired authority should fail closed",
			CreatedBy:   "operator-a",
		})
		assertLeaseExpiredReject(t, err)
		assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectLeaseExpired)
		assertCapabilityPolicyCount(t, ctx, store, workspaceID, 0)
		assertCapabilityPolicyRuntimeEventCount(t, ctx, store, workspaceID, 0)
	})
}

func expireExternalWorkspaceAuthority(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, current sqlite.WorkspaceAuthorityRecord) {
	t.Helper()

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
}

func assertLeaseExpiredReject(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("expected lease-expired authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected lease-expired authority reject, got %+v", reject)
	}
}

func countAuthorityRejectEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) int {
	t.Helper()
	return countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   sqlite.AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       20,
	})
}

func assertAuthorityRejectEventIncrement(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, before int, wantCode sqlite.AuthorityRejectCode) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   sqlite.AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list authority rejected runtime events: %v", err)
	}
	if len(events) != before+1 {
		t.Fatalf("expected authority.rejected count to grow from %d to %d, got %+v", before, before+1, events)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority rejected payload: %v", err)
	}
	if payload["reject_code"] != string(wantCode) {
		t.Fatalf("expected latest authority.rejected payload reject_code %q, got %+v", wantCode, payload)
	}
}
