package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestActionChatSendRejectsMissingWorkspaceAuthorityWithNoSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-action-chat-missing-authority-rpc"
		taskID      = "task-d2a-action-chat-missing-authority-rpc"
		agentID     = "agent-d2a-action-chat-missing-authority-rpc"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	actionID := mustCreateServerAuthorityBackedAction(t, ctx, h, workspaceID, taskID, agentID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(actionChatSendParams{
		ActionID: actionID,
		FromID:   "reviewer-a",
		Content:  "should fail closed before action chat side effects",
	})
	if err != nil {
		t.Fatalf("marshal action.chat.send params: %v", err)
	}

	result, rpcErr := h.actionChatSend(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "action.chat.send")

	messages, err := store.ListActionMessages(ctx, actionID)
	if err != nil {
		t.Fatalf("list action messages after authority reject: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected no action messages after authority reject, got %+v", messages)
	}
	if got := len(snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.chat",
		EntityType:  "action_message",
		Limit:       10,
	})); got != 0 {
		t.Fatalf("expected no action.chat events after authority reject, got %d", got)
	}
	if got := len(snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		Limit:       10,
	})); got != 0 {
		t.Fatalf("expected no agent_message.sent events after authority reject, got %d", got)
	}
	if got := countServerWorkspaceRows(t, ctx, store, workspaceID, "agent_messages"); got != 0 {
		t.Fatalf("expected no agent_messages rows after authority reject, got %d", got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestActionChatSendRejectsStaleWorkspaceAuthorityWithNoSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-action-chat-stale-authority-rpc"
		taskID      = "task-d2a-action-chat-stale-authority-rpc"
		agentID     = "agent-d2a-action-chat-stale-authority-rpc"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	actionID := mustCreateServerAuthorityBackedAction(t, ctx, h, workspaceID, taskID, agentID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-2911")

	raw, err := json.Marshal(actionChatSendParams{
		ActionID: actionID,
		FromID:   "reviewer-a",
		Content:  "should fail closed under stale authority",
	})
	if err != nil {
		t.Fatalf("marshal action.chat.send params: %v", err)
	}

	result, rpcErr := h.actionChatSend(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "action.chat.send")

	messages, err := store.ListActionMessages(ctx, actionID)
	if err != nil {
		t.Fatalf("list action messages after stale authority reject: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected no action messages after stale authority reject, got %+v", messages)
	}
	if got := len(snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.chat",
		EntityType:  "action_message",
		Limit:       10,
	})); got != 0 {
		t.Fatalf("expected no action.chat events after stale authority reject, got %d", got)
	}
	if got := len(snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		Limit:       10,
	})); got != 0 {
		t.Fatalf("expected no agent_message.sent events after stale authority reject, got %d", got)
	}
	if got := countServerWorkspaceRows(t, ctx, store, workspaceID, "agent_messages"); got != 0 {
		t.Fatalf("expected no agent_messages rows after stale authority reject, got %d", got)
	}
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
}

func mustCreateServerAuthorityBackedAction(t *testing.T, ctx context.Context, h *Handler, workspaceID, taskID, agentID string) string {
	t.Helper()

	raw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Authority-backed action chat",
		Description: "Action chat should fail closed without authority.",
		Blocking:    boolPtr(true),
	})
	if err != nil {
		t.Fatalf("marshal action.create params: %v", err)
	}
	result, rpcErr := h.actionCreate(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", result)
	}
	actionID, _ := resp["action_id"].(string)
	if actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", resp)
	}
	return actionID
}

func countServerWorkspaceRows(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, table string) int {
	t.Helper()

	var count int
	query := "SELECT COUNT(*) FROM " + table + " WHERE workspace_id = ?"
	if err := store.DB().QueryRowContext(ctx, query, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count %s rows for %s: %v", table, workspaceID, err)
	}
	return count
}
