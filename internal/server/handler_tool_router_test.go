package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestMCPToolDiscoverRegistersWorkspaceToolAlias(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-mcp-alias"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP Alias",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "notion",
		WorkspaceID:  workspaceID,
		DisplayName:  "Notion MCP",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}

	raw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "notion"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	respAny, rpcErr := h.mcpToolDiscover(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("mcpToolDiscover rpc error: %+v", rpcErr)
	}
	resp, ok := respAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected mcpToolDiscover response type %T", respAny)
	}
	operationID, ok := resp["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("mcpToolDiscover response missing operation_id: %+v", resp)
	}
	if resp["mcp_transport"] != "streamable-http" || resp["tools_discovered"] != 1 {
		t.Fatalf("mcpToolDiscover response missing diagnostics: %+v", resp)
	}

	record, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("notion", "search_docs"))
	if err != nil {
		t.Fatalf("get workspace tool alias: %v", err)
	}
	manifest := parseWorkspaceToolManifest(record.ManifestJSON)
	if manifest.Route == nil || manifest.Route.Kind != "mcp" || manifest.Route.ServerID != "notion" || manifest.Route.ToolName != "search_docs" {
		t.Fatalf("unexpected manifest route %+v", manifest.Route)
	}
	if manifest.InputSchema == nil {
		t.Fatalf("expected input schema to be mirrored into manifest, got %+v", manifest)
	}
	aliasEvent := requireServerRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_tool.registered",
		EntityType:  "workspace_tool",
		EntityID:    mcpWorkspaceToolID("notion", "search_docs"),
		Limit:       10,
	})
	assertToolRegistryPromptContextEnvelope(t, aliasEvent.PayloadJSON, map[string]string{
		"surface":                   "mcp.workspace_tool.project",
		"origin":                    "server_mcp_projection",
		"projection_source_surface": "mcp.tool.discover",
		"projection_operation_id":   operationID,
	})
	run := latestMCPDiscoverOperationRun(t, ctx, store, workspaceID)
	if run.Status != "COMPLETED" || run.Outcome != "COMPLETED" {
		t.Fatalf("unexpected discover run state: status=%s outcome=%s", run.Status, run.Outcome)
	}
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "operation_id"); got != operationID {
		t.Fatalf("discover response operation_id = %q, ledger operation_id = %q", operationID, got)
	}
	if got := stringLedgerField(t, ledger, "operation_kind"); got != "mcp_discover" {
		t.Fatalf("discover operation_kind = %q", got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "operation_id"); got != operationID {
		t.Fatalf("discover result operation_id = %q, want %q", got, operationID)
	}
	if got := stringLedgerField(t, resultLedger, "mcp_transport"); got != "streamable-http" {
		t.Fatalf("discover result mcp_transport = %q, want streamable-http", got)
	}
	if count, ok := resultLedger["tools_discovered"].(float64); !ok || int(count) != 1 {
		t.Fatalf("discover result tools_discovered = %T %+v, want 1", resultLedger["tools_discovered"], resultLedger["tools_discovered"])
	}
}

func TestToolCallRoutesThroughMCPWorkspaceTool(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-mcp-route"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP Route",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "notion",
		WorkspaceID:  workspaceID,
		DisplayName:  "Notion MCP",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}
	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "notion"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	if _, rpcErr := h.mcpToolDiscover(ctx, discoverRaw); rpcErr != nil {
		t.Fatalf("mcpToolDiscover rpc error: %+v", rpcErr)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)
	runtimeFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    mcpWorkspaceToolID("notion", "search_docs"),
		Limit:       10,
	}

	callRaw, err := json.Marshal(toolCallParams{
		ToolID:      mcpWorkspaceToolID("notion", "search_docs"),
		WorkspaceID: workspaceID,
		Arguments:   map[string]any{"query": "runtime state"},
	})
	if err != nil {
		t.Fatalf("marshal tool call params: %v", err)
	}
	respAny, rpcErr := h.toolCall(ctx, callRaw)
	if rpcErr != nil {
		t.Fatalf("toolCall rpc error: %+v", rpcErr)
	}
	resp, ok := respAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected tool call response type %T", respAny)
	}
	if resp["router_kind"] != "mcp" || resp["tool_name"] != "search_docs" || resp["server_id"] != "notion" {
		t.Fatalf("unexpected routed tool response %+v", resp)
	}
	if resp["stdout"] != "found matching docs" {
		t.Fatalf("unexpected routed stdout %+v", resp)
	}
	if resp["mcp_transport"] != "streamable-http" {
		t.Fatalf("unexpected routed mcp_transport %+v", resp)
	}
	run := latestToolOperationRun(t, ctx, store, workspaceID)
	ledger := operationLedgerFromRun(t, run)
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "mcp_transport"); got != "streamable-http" {
		t.Fatalf("ledger result mcp_transport = %q, want streamable-http", got)
	}
	firstRuntime := mustRuntimeEvent(t, ctx, store, runtimeFilter)
	assertServerRuntimeEventAuthorityMetadata(t, firstRuntime, authority)
	live := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, live, firstRuntime, "tool.call.executed")

	seenEventIDs := snapshotRuntimeEventIDs(t, ctx, store, runtimeFilter)
	if _, rpcErr := h.toolCall(ctx, callRaw); rpcErr != nil {
		t.Fatalf("second toolCall rpc error: %+v", rpcErr)
	}
	secondRuntime := mustNewRuntimeEvent(t, ctx, store, runtimeFilter, seenEventIDs)
	secondLive := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, secondLive, secondRuntime, "tool.call.executed")
	if secondRuntime.EventID == firstRuntime.EventID || secondRuntime.IngestSeq <= firstRuntime.IngestSeq {
		t.Fatalf("expected repeated routed tool call to create a distinct runtime row, got first=%+v second=%+v", firstRuntime, secondRuntime)
	}
}

func TestMCPToolCallAppendsAndMirrorsCanonicalRuntimeEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-mcp-direct-route"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "MCP Direct Route",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "notion-direct",
		WorkspaceID:  workspaceID,
		DisplayName:  "Notion MCP Direct",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}
	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "notion-direct"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	if _, rpcErr := h.mcpToolDiscover(ctx, discoverRaw); rpcErr != nil {
		t.Fatalf("mcpToolDiscover rpc error: %+v", rpcErr)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)
	runtimeFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    mcpWorkspaceToolID("notion-direct", "search_docs"),
		Limit:       10,
	}

	callRaw, err := json.Marshal(mcpToolCallParams{
		ServerID:  "notion-direct",
		ToolName:  "search_docs",
		Arguments: map[string]any{"query": "runtime state"},
	})
	if err != nil {
		t.Fatalf("marshal mcp tool call params: %v", err)
	}
	respAny, rpcErr := h.mcpToolCall(ctx, callRaw)
	if rpcErr != nil {
		t.Fatalf("mcpToolCall rpc error: %+v", rpcErr)
	}
	resp, ok := respAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected mcp tool call response type %T", respAny)
	}
	if resp["tool_name"] != "search_docs" || resp["server_id"] != "notion-direct" || resp["is_error"] != false {
		t.Fatalf("unexpected direct mcp tool response %+v", resp)
	}
	operationID, ok := resp["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("direct mcp tool response missing operation_id: %+v", resp)
	}
	if resp["mcp_transport"] != "streamable-http" || resp["router_kind"] != "mcp" || resp["timed_out"] != false {
		t.Fatalf("direct mcp tool response missing delegated diagnostics: %+v", resp)
	}
	if resp["exit_code"] != 0 || resp["stdout"] != "found matching docs" {
		t.Fatalf("direct mcp tool response missing delegated result fields: %+v", resp)
	}
	run := latestToolOperationRun(t, ctx, store, workspaceID)
	ledger := operationLedgerFromRun(t, run)
	if got := stringLedgerField(t, ledger, "operation_id"); got != operationID {
		t.Fatalf("direct mcp response operation_id = %q, ledger operation_id = %q", operationID, got)
	}
	resultLedger := nestedLedgerMap(t, ledger, "result")
	if got := stringLedgerField(t, resultLedger, "operation_id"); got != operationID {
		t.Fatalf("ledger result operation_id = %q, want %q", got, operationID)
	}
	if got := stringLedgerField(t, resultLedger, "mcp_transport"); got != "streamable-http" {
		t.Fatalf("ledger result mcp_transport = %q, want streamable-http", got)
	}
	firstRuntime := mustRuntimeEvent(t, ctx, store, runtimeFilter)
	assertServerRuntimeEventAuthorityMetadata(t, firstRuntime, authority)
	live := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, live, firstRuntime, "tool.call.executed")

	seenEventIDs := snapshotRuntimeEventIDs(t, ctx, store, runtimeFilter)
	if _, rpcErr := h.mcpToolCall(ctx, callRaw); rpcErr != nil {
		t.Fatalf("second mcpToolCall rpc error: %+v", rpcErr)
	}
	secondRuntime := mustNewRuntimeEvent(t, ctx, store, runtimeFilter, seenEventIDs)
	secondLive := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, secondLive, secondRuntime, "tool.call.executed")
	if secondRuntime.EventID == firstRuntime.EventID || secondRuntime.IngestSeq <= firstRuntime.IngestSeq {
		t.Fatalf("expected repeated direct mcp tool call to create a distinct runtime row, got first=%+v second=%+v", firstRuntime, secondRuntime)
	}
}

func newFakeMCPServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req struct {
			Method string `json:"method"`
			ID     any    `json:"id"`
		}
		var envelope map[string]any
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			t.Fatalf("decode mcp request: %v", err)
		}
		req.Method, _ = envelope["method"].(string)
		req.ID = envelope["id"]

		switch {
		case req.Method == "initialize":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"serverInfo": map[string]any{
						"name":    "fake-mcp",
						"version": "1.0.0",
					},
				},
			})
		case strings.HasPrefix(req.Method, "notifications/"):
			w.WriteHeader(http.StatusAccepted)
		case req.Method == "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result": map[string]any{
					"tools": []any{
						map[string]any{
							"name":        "search_docs",
							"description": "Search docs",
							"inputSchema": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"query": map[string]any{"type": "string"},
								},
								"required": []any{"query"},
							},
						},
					},
				},
			})
		case req.Method == "tools/call":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result": map[string]any{
					"content": []any{
						map[string]any{"type": "text", "text": "found matching docs"},
					},
					"isError": false,
				},
			})
		default:
			t.Fatalf("unexpected fake MCP method %q", req.Method)
		}
	}))
}
