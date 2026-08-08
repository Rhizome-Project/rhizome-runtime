package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestHighRiskToolCallApprovalRejectsStaleWorkspaceAuthorityBeforeQueueOrRuntimeSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-tool-call-approval-stale-authority-rpc"
	authCtx := testAuthContext(workspaceID, "agent", "agent-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Tool Call Approval Stale Authority RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	registerHighRiskBridgeTool(t, ctx, store, h, workspaceID, "dangerous-provider")
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-3014")
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(toolCallParams{
		ToolID:      "dangerous-provider",
		WorkspaceID: workspaceID,
		ActorType:   "agent",
		ActorID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("marshal tool.call params: %v", err)
	}

	result, rpcErr := h.toolCall(authCtx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "tool.call")

	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.approval_required", "dangerous-provider"); got != 0 {
		t.Fatalf("expected no approval-required runtime events after stale-authority reject, got %d", got)
	}
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.denied", "dangerous-provider"); got != 0 {
		t.Fatalf("expected no denied runtime events after stale-authority reject, got %d", got)
	}
	if got := countOperatorQueueRowsForToolCall(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no operator queue rows after stale-authority reject, got %d", got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected stale authority preflight reject not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestMCPToolCallRejectsMissingWorkspaceAuthorityWithNoRuntimeSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-mcp-tool-call-missing-authority-rpc"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A MCP Tool Call Missing Authority RPC",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "notion-missing",
		WorkspaceID:  workspaceID,
		DisplayName:  "Notion MCP Missing Authority",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}

	raw, err := json.Marshal(mcpToolCallParams{
		ServerID:  "notion-missing",
		ToolName:  "search_docs",
		Arguments: map[string]any{"query": "authority"},
	})
	if err != nil {
		t.Fatalf("marshal mcp.tool.call params: %v", err)
	}

	result, rpcErr := h.mcpToolCall(authCtx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "mcp.tool.call")

	toolID := mcpWorkspaceToolID("notion-missing", "search_docs")
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.executed", toolID); got != 0 {
		t.Fatalf("expected no tool.call.executed events after missing-authority reject, got %d", got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing authority preflight reject not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func countToolCallRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, eventType, toolID string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "tool",
		EntityID:    toolID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events for %s: %v", eventType, toolID, err)
	}
	return len(events)
}

func countOperatorQueueRowsForToolCall(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_queue_items WHERE workspace_id = ?`, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count operator_queue rows: %v", err)
	}
	return count
}
