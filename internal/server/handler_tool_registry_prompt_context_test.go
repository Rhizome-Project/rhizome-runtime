package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestToolRegisterRemoveRuntimeEventsCarryPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-server-tool-registry-prompt-context"
		toolID      = "server-tool-registry-prompt"
		principalID = "operator-rpc"
		ownerID     = "owner-recorded"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Server Tool Registry Prompt Context",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	registerRaw, err := json.Marshal(toolRegisterParams{
		WorkspaceID:  workspaceID,
		ToolID:       toolID,
		DisplayName:  "Server Tool Registry Prompt",
		OwnerUserID:  ownerID,
		Kind:         model.ToolKindIntegration,
		Status:       model.ToolStatusActive,
		AccessLevel:  model.ToolAccessWorkspace,
		Endpoint:     "local:server-tool",
		Capabilities: []string{"tool.call"},
		ManifestJSON: `{"route":{"kind":"local"}}`,
	})
	if err != nil {
		t.Fatalf("marshal tool register params: %v", err)
	}
	if _, rpcErr := h.toolRegister(testAuthContext(workspaceID, "human", principalID), registerRaw); rpcErr != nil {
		t.Fatalf("tool register rpc error: %+v", rpcErr)
	}
	registered := requireServerRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	assertToolRegistryPromptContextEnvelope(t, registered.PayloadJSON, map[string]string{
		"context_kind":   "authority_bearing_tool_registry_write",
		"surface":        "tool.register",
		"origin":         "server_rpc",
		"workspace_id":   workspaceID,
		"principal_type": "human",
		"principal_id":   principalID,
		"tool_id":        toolID,
		"owner_user_id":  ownerID,
		"event_type":     "workspace_tool.registered",
		"entity_type":    "workspace_tool",
		"entity_id":      toolID,
	})

	removeRaw, err := json.Marshal(toolRemoveParams{
		WorkspaceID: workspaceID,
		ToolID:      toolID,
		RemovedBy:   "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal tool remove params: %v", err)
	}
	if _, rpcErr := h.toolRemove(testAuthContext(workspaceID, "human", principalID), removeRaw); rpcErr != nil {
		t.Fatalf("tool remove rpc error: %+v", rpcErr)
	}
	removed := requireServerRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.removed",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	assertToolRegistryPromptContextEnvelope(t, removed.PayloadJSON, map[string]string{
		"surface":        "tool.remove",
		"origin":         "server_rpc",
		"workspace_id":   workspaceID,
		"principal_type": "human",
		"principal_id":   principalID,
		"tool_id":        toolID,
		"removed_by":     "dashboard",
		"event_type":     "workspace_tool.removed",
		"entity_type":    "workspace_tool",
		"entity_id":      toolID,
	})
}

func TestMCPDiscoveredWorkspaceToolProjectionCarriesDerivedPromptContext(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-server-mcp-registry-projection-context"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Server MCP Registry Projection Context",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	server := mcp.ServerRecord{
		ServerID:     "registry-mcp",
		WorkspaceID:  workspaceID,
		DisplayName:  "Registry MCP",
		Transport:    "streamable-http",
		URL:          "https://mcp.example.test",
		RegisteredBy: "agent:planner",
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, server, []mcp.Tool{{
		Name:        "search_docs",
		Description: "Search docs",
	}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("register discovered mcp workspace tools: %v", err)
	}
	toolID := mcpWorkspaceToolID("registry-mcp", "search_docs")
	event := requireServerRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    toolID,
		Limit:       10,
	})
	assertToolRegistryPromptContextEnvelope(t, event.PayloadJSON, map[string]string{
		"context_kind":              "authority_bearing_tool_registry_write",
		"surface":                   "mcp.workspace_tool.project",
		"origin":                    "server_mcp_projection",
		"workspace_id":              workspaceID,
		"principal_type":            "agent",
		"principal_id":              "planner",
		"tool_id":                   toolID,
		"projection_action":         "register",
		"projection_source":         "mcp_workspace_tool_reconcile",
		"projection_source_surface": "mcp.tool.discover",
		"projection_operation_id":   testMCPProjectionOperationID,
		"mcp_server_id":             "registry-mcp",
		"mcp_tool_name":             "search_docs",
	})
}

func assertToolRegistryPromptContextEnvelope(t *testing.T, payloadJSON string, expected map[string]string) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode tool-registry payload: %v; payload=%q", err, payloadJSON)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected tool-registry prompt_context_envelope in payload %+v", payload)
	}
	for key, want := range expected {
		if got, _ := envelope[key].(string); got != want {
			t.Fatalf("expected envelope %s=%q, got %q in %+v", key, want, got, envelope)
		}
	}
}

func requireServerRuntimeEvent(t *testing.T, store *sqlite.Store, ctx context.Context, filter sqlite.RuntimeEventFilter) sqlite.RuntimeEventRecord {
	t.Helper()
	if filter.Limit <= 0 {
		filter.Limit = 10
	}
	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one runtime event for filter %+v, got %d: %+v", filter, len(events), events)
	}
	return events[0]
}
