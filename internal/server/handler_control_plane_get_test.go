package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceOpsGetReturnsExistingQueueByKey(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	workspaceID := "ws-ops-get"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Ops Get",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, _, err := store.UpsertOperatorQueueItemWithEvent(context.Background(), sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "external_gate:explicit_approval:rnar.task.task-42",
		QueueType:   "DECISION",
		Title:       "Approve rollout",
		Summary:     "Operator approval is required",
		AssignedTo:  "reviewer-1",
	})
	if err != nil {
		t.Fatalf("seed operator queue: %v", err)
	}

	raw, err := json.Marshal(workspaceOpsGetParams{
		WorkspaceID: workspaceID,
		QueueKey:    record.QueueKey,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.workspaceOpsGet(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceOpsGet rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	item, ok := payload["item"].(sqlite.OperatorQueueRecord)
	if !ok {
		t.Fatalf("unexpected item type %T", payload["item"])
	}
	if item.QueueID != record.QueueID || item.QueueKey != record.QueueKey || item.Revision != record.Revision {
		t.Fatalf("unexpected queue item %+v", item)
	}
}

func TestWorkspaceOpsGetUsesDedicatedNotFoundCode(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	workspaceID := "ws-ops-get-not-found-code"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Ops Get Not Found Code",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceOpsGetParams{
		WorkspaceID: workspaceID,
		QueueKey:    "external_gate:explicit_approval:missing",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.workspaceOpsGet(ctx, raw)
	if rpcErr == nil {
		t.Fatalf("expected queue not-found rpc error, got result=%+v", result)
	}
	if rpcErr.Code != errCodeOperatorQueueNotFound {
		t.Fatalf("expected operator queue not-found code, got %+v", rpcErr)
	}
}
