package server

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestToolRegisterRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-tool-register-missing-authority-rpc"
		toolID      = "tool-d2a-register-missing-authority-rpc"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Tool Register Missing Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(toolRegisterParams{
		WorkspaceID: workspaceID,
		ToolID:      toolID,
		DisplayName: "Authority Missing Tool RPC",
		OwnerUserID: "operator-a",
		Kind:        model.ToolKindOther,
		Status:      model.ToolStatusActive,
	})
	if err != nil {
		t.Fatalf("marshal tool.register params: %v", err)
	}

	result, rpcErr := h.toolRegister(testAuthContext(workspaceID, "human", "operator-a"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "tool.register")

	if _, err := store.GetWorkspaceTool(ctx, workspaceID, toolID); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected tool row to be absent after authority reject, got %v", err)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list workspace_tool.registered events: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no workspace_tool.registered events after authority reject, got %+v", events)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestToolRemoveRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-tool-remove-stale-authority-rpc"
		toolID      = "tool-d2a-remove-stale-authority-rpc"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Tool Remove Stale Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID: workspaceID,
		ToolID:      toolID,
		DisplayName: "Authority Stale Tool RPC",
		OwnerUserID: "operator-a",
		Kind:        model.ToolKindOther,
		Status:      model.ToolStatusActive,
	}); err != nil {
		t.Fatalf("register workspace tool: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-3012")

	raw, err := json.Marshal(toolRemoveParams{
		WorkspaceID: workspaceID,
		ToolID:      toolID,
		RemovedBy:   "operator-b",
	})
	if err != nil {
		t.Fatalf("marshal tool.remove params: %v", err)
	}

	result, rpcErr := h.toolRemove(testAuthContext(workspaceID, "human", "operator-b"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "tool.remove")

	if _, err := store.GetWorkspaceTool(ctx, workspaceID, toolID); err != nil {
		t.Fatalf("expected tool row to remain after stale-authority reject: %v", err)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.removed",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list workspace_tool.removed events: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no workspace_tool.removed events after stale-authority reject, got %+v", events)
	}
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestMCPToolDiscoverRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-mcp-discover-missing-authority-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A MCP Discover Missing Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "notion",
		WorkspaceID:  workspaceID,
		DisplayName:  "Notion MCP",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "operator-a",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}

	raw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "notion"})
	if err != nil {
		t.Fatalf("marshal mcp.tool.discover params: %v", err)
	}

	result, rpcErr := h.mcpToolDiscover(testAuthContext(workspaceID, "human", "operator-a"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "mcp.tool.discover")

	if _, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("notion", "search_docs")); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected no MCP workspace-tool alias after authority reject, got %v", err)
	}
	tools, err := h.mcpStore.ListServerTools(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list cached server tools: %v", err)
	}
	if len(tools) != 0 {
		t.Fatalf("expected no cached discovered tools after authority reject, got %+v", tools)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}
