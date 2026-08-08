package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/bridgepolicy"
	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

const testMCPProjectionOperationID = "test-mcp-discover-operation"

func registerDiscoveredMCPWorkspaceToolsForTest(t *testing.T, h *Handler, ctx context.Context, server mcp.ServerRecord, tools []mcp.Tool, operationID string) error {
	t.Helper()
	seedMCPProjectionOperationRunForTest(t, ctx, h.store, server.WorkspaceID, operationID, "mcp.tool.discover")
	return h.registerDiscoveredMCPWorkspaceTools(ctx, server, tools, operationID)
}

func seedMCPProjectionOperationRunForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, runID, surface string) {
	t.Helper()
	operationKind := map[string]string{
		"mcp.tool.discover":   "mcp_discover",
		"mcp.server.register": "mcp_server_register",
		"mcp.server.remove":   "mcp_server_remove",
	}[surface]
	_, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       runID,
		Title:       "MCP projection parent operation",
		Status:      "ACTIVE",
		Verification: sqlite.AttachExecutionPromptContextEnvelope(
			map[string]any{
				"operation_ledger": map[string]any{
					"schema":         "operation_ledger.v1",
					"operation_id":   runID,
					"operation_kind": operationKind,
					"capability_snapshot": map[string]any{
						"requested_capability": surface,
					},
				},
			},
			sqlite.BuildExecutionPromptContextEnvelope(surface, "server_operation_ledger", workspaceID, "system", "mcp_projection_test"),
		),
	})
	if err != nil {
		t.Fatalf("seed mcp projection operation run %s/%s: %v", workspaceID, runID, err)
	}
}

func TestMCPToolCallRequiresDiscoveredWorkspaceAlias(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-alias-required"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Alias Required",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "notion-undiscovered",
		WorkspaceID:  workspaceID,
		DisplayName:  "Notion Undiscovered",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}

	raw, err := json.Marshal(mcpToolCallParams{
		ServerID:  "notion-undiscovered",
		ToolName:  "search_docs",
		Arguments: map[string]any{"query": "authority"},
	})
	if err != nil {
		t.Fatalf("marshal mcp.tool.call params: %v", err)
	}

	result, rpcErr := h.mcpToolCall(authCtx, raw)
	if rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for undiscovered alias, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "discover") {
		t.Fatalf("expected discover-first error, got %+v", rpcErr)
	}
	toolID := mcpWorkspaceToolID("notion-undiscovered", "search_docs")
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.executed", toolID); got != 0 {
		t.Fatalf("expected no executed runtime events for undiscovered alias, got %d", got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected undiscovered alias reject not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestMCPToolCallUsesWorkspaceAliasCapabilityPolicy(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-alias-policy"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Alias Policy",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "notion-policy",
		WorkspaceID:  workspaceID,
		DisplayName:  "Notion Policy",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}
	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "notion-policy"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	if _, rpcErr := h.mcpToolDiscover(authCtx, discoverRaw); rpcErr != nil {
		t.Fatalf("mcpToolDiscover rpc error: %+v", rpcErr)
	}

	toolID := mcpWorkspaceToolID("notion-policy", "search_docs")
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "human",
		SubjectID:   "developer",
		Capability:  "tool.call",
		ToolID:      toolID,
		Effect:      "DENY",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("put capability policy: %v", err)
	}

	raw, err := json.Marshal(mcpToolCallParams{
		ServerID:  "notion-policy",
		ToolName:  "search_docs",
		Arguments: map[string]any{"query": "authority"},
	})
	if err != nil {
		t.Fatalf("marshal mcp.tool.call params: %v", err)
	}

	result, rpcErr := h.mcpToolCall(authCtx, raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied via workspace alias policy, got result=%+v err=%+v", result, rpcErr)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.denied",
		EntityType:  "tool",
		EntityID:    toolID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list denied runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one denied runtime event, got %+v", events)
	}
	if events[0].ActorType != "human" || events[0].ActorID != "developer" {
		t.Fatalf("expected denied event to bind to authenticated principal, got %+v", events[0])
	}
	assertServerRuntimeEventAuthorityMetadata(t, events[0], authority)
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.executed", toolID); got != 0 {
		t.Fatalf("expected no executed runtime events after deny, got %d", got)
	}
}

func TestMCPServerListFiltersVisibilityForAgentPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-server-list-visibility"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Server Visibility",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	for _, input := range []mcp.RegisterInput{
		{
			ServerID:     "human-owned",
			WorkspaceID:  workspaceID,
			DisplayName:  "Human Owned",
			Transport:    "streamable-http",
			URL:          "https://human.invalid/mcp",
			HeadersJSON:  `{"authorization":"secret"}`,
			RegisteredBy: "developer",
		},
		{
			ServerID:     "system-owned",
			WorkspaceID:  workspaceID,
			DisplayName:  "System Owned",
			Transport:    "streamable-http",
			URL:          "https://system.invalid/mcp",
			EnvJSON:      `{"TOKEN":"secret"}`,
			RegisteredBy: "system:daemon",
		},
		{
			ServerID:     "agent-owned",
			WorkspaceID:  workspaceID,
			DisplayName:  "Agent Owned",
			Transport:    "streamable-http",
			URL:          "https://agent.invalid/mcp",
			RegisteredBy: "agent:partner",
		},
	} {
		if err := h.mcpStore.RegisterServer(ctx, input); err != nil {
			t.Fatalf("register server %s: %v", input.ServerID, err)
		}
	}

	raw, err := json.Marshal(mcpServerListParams{WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("marshal server list params: %v", err)
	}
	respAny, rpcErr := h.mcpServerList(testAuthContext(workspaceID, "agent", "partner"), raw)
	if rpcErr != nil {
		t.Fatalf("mcpServerList rpc error: %+v", rpcErr)
	}
	resp, ok := respAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected response type %T", respAny)
	}
	serversRaw, ok := resp["servers"].([]mcp.ServerRecord)
	if !ok {
		t.Fatalf("unexpected servers payload %T", resp["servers"])
	}
	if got, want := len(serversRaw), 2; got != want {
		t.Fatalf("expected %d visible servers, got %+v", want, serversRaw)
	}
	ids := map[string]mcp.ServerRecord{}
	for _, server := range serversRaw {
		ids[server.ServerID] = server
	}
	if _, ok := ids["agent-owned"]; !ok {
		t.Fatalf("expected agent-owned server to stay visible, got %+v", ids)
	}
	if _, ok := ids["system-owned"]; !ok {
		t.Fatalf("expected system-owned server to stay visible, got %+v", ids)
	}
	if _, ok := ids["human-owned"]; ok {
		t.Fatalf("expected human-owned server to be hidden from agent principal, got %+v", ids)
	}
	if ids["system-owned"].EnvJSON != "\"[REDACTED]\"" {
		t.Fatalf("expected env redaction to stay intact, got %+v", ids["system-owned"])
	}
}

func TestMCPToolListFiltersVisibilityForAgentPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-tool-list-visibility"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Tool Visibility",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	humanServer := mcp.ServerRecord{
		ServerID:     "human-owned",
		WorkspaceID:  workspaceID,
		DisplayName:  "Human Owned",
		Transport:    "streamable-http",
		URL:          "https://human.invalid/mcp",
		RegisteredBy: "developer",
		Status:       "ACTIVE",
	}
	systemServer := mcp.ServerRecord{
		ServerID:     "system-owned",
		WorkspaceID:  workspaceID,
		DisplayName:  "System Owned",
		Transport:    "streamable-http",
		URL:          "https://system.invalid/mcp",
		RegisteredBy: "system:daemon",
		Status:       "ACTIVE",
	}
	for _, input := range []mcp.RegisterInput{
		{
			ServerID:     humanServer.ServerID,
			WorkspaceID:  workspaceID,
			DisplayName:  humanServer.DisplayName,
			Transport:    humanServer.Transport,
			URL:          humanServer.URL,
			RegisteredBy: humanServer.RegisteredBy,
		},
		{
			ServerID:     systemServer.ServerID,
			WorkspaceID:  workspaceID,
			DisplayName:  systemServer.DisplayName,
			Transport:    systemServer.Transport,
			URL:          systemServer.URL,
			RegisteredBy: systemServer.RegisteredBy,
		},
	} {
		if err := h.mcpStore.RegisterServer(ctx, input); err != nil {
			t.Fatalf("register server %s: %v", input.ServerID, err)
		}
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, humanServer, []mcp.Tool{{Name: "search_docs", Description: "Search docs"}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("register human alias: %v", err)
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, systemServer, []mcp.Tool{{Name: "shared_search", Description: "Shared search"}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("register system alias: %v", err)
	}
	if err := h.mcpStore.SaveDiscoveredTools(ctx, workspaceID, humanServer.ServerID, []mcp.Tool{{Name: "search_docs", Description: "Search docs"}}); err != nil {
		t.Fatalf("save human tools: %v", err)
	}
	if err := h.mcpStore.SaveDiscoveredTools(ctx, workspaceID, systemServer.ServerID, []mcp.Tool{{Name: "shared_search", Description: "Shared search"}}); err != nil {
		t.Fatalf("save system tools: %v", err)
	}

	raw, err := json.Marshal(mcpToolListParams{WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("marshal tool list params: %v", err)
	}
	respAny, rpcErr := h.mcpToolList(testAuthContext(workspaceID, "agent", "partner"), raw)
	if rpcErr != nil {
		t.Fatalf("mcpToolList rpc error: %+v", rpcErr)
	}
	resp, ok := respAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected response type %T", respAny)
	}
	toolsRaw, ok := resp["tools"].([]mcp.ServerToolRecord)
	if !ok {
		t.Fatalf("unexpected tools payload %T", resp["tools"])
	}
	if got, want := len(toolsRaw), 1; got != want {
		t.Fatalf("expected %d visible tool, got %+v", want, toolsRaw)
	}
	if toolsRaw[0].ServerID != "system-owned" || toolsRaw[0].ToolName != "shared_search" {
		t.Fatalf("expected only system-owned shared tool, got %+v", toolsRaw)
	}
}

func TestMCPToolDiscoverReconcilesRemovedAliasesAndCache(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-discover-reconcile"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Discover Reconcile",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	discoveredTools := []mcp.Tool{
		{Name: "search_docs", Description: "Search docs", InputSchema: []byte(`{"type":"object"}`)},
		{Name: "search_code", Description: "Search code", InputSchema: []byte(`{"type":"object"}`)},
	}
	mcpServer := newFakeMCPServerWithToolSet(t, &discoveredTools)
	defer mcpServer.Close()

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "notion-reconcile",
		WorkspaceID:  workspaceID,
		DisplayName:  "Notion Reconcile",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}

	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "notion-reconcile"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	if _, rpcErr := h.mcpToolDiscover(authCtx, discoverRaw); rpcErr != nil {
		t.Fatalf("first discover rpc error: %+v", rpcErr)
	}

	discoveredTools = discoveredTools[:1]
	if _, rpcErr := h.mcpToolDiscover(authCtx, discoverRaw); rpcErr != nil {
		t.Fatalf("second discover rpc error: %+v", rpcErr)
	}

	removedToolID := mcpWorkspaceToolID("notion-reconcile", "search_code")
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, removedToolID); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected removed alias to be deleted after rediscover, got err=%v", err)
	}

	listRaw, err := json.Marshal(mcpToolListParams{WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("marshal tool list params: %v", err)
	}
	respAny, rpcErr := h.mcpToolList(authCtx, listRaw)
	if rpcErr != nil {
		t.Fatalf("mcpToolList rpc error: %+v", rpcErr)
	}
	resp := respAny.(map[string]any)
	toolRows := resp["tools"].([]mcp.ServerToolRecord)
	if got, want := len(toolRows), 1; got != want {
		t.Fatalf("expected %d discovered tool after rediscover, got %+v", want, toolRows)
	}
	if toolRows[0].ToolName != "search_docs" {
		t.Fatalf("expected remaining discovered tool to be search_docs, got %+v", toolRows)
	}

	callRaw, err := json.Marshal(mcpToolCallParams{
		ServerID:  "notion-reconcile",
		ToolName:  "search_code",
		Arguments: map[string]any{"query": "authority"},
	})
	if err != nil {
		t.Fatalf("marshal removed tool call params: %v", err)
	}
	if result, rpcErr := h.mcpToolCall(authCtx, callRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected removed alias to reject after rediscover, got result=%+v err=%+v", result, rpcErr)
	}
}

func TestMCPToolDiscoverCollisionDoesNotPublishPartialAliasState(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-discover-collision"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Discover Collision",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	discoveredTools := []mcp.Tool{
		{Name: "search_docs", Description: "Search docs", InputSchema: []byte(`{"type":"object"}`)},
		{Name: "search_code", Description: "Search code", InputSchema: []byte(`{"type":"object"}`)},
	}
	mcpServer := newFakeMCPServerWithToolSet(t, &discoveredTools)
	defer mcpServer.Close()

	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID: workspaceID,
		ToolID:      mcpWorkspaceToolID("notion-collision", "search_code"),
		DisplayName: "Existing Colliding Tool",
		Description: "Not an MCP alias for the same server",
		OwnerUserID: "developer",
		Kind:        model.ToolKindOther,
		Status:      model.ToolStatusActive,
		AccessLevel: model.ToolAccessWorkspace,
		Endpoint:    "existing://tool",
	}); err != nil {
		t.Fatalf("register colliding workspace tool: %v", err)
	}

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "notion-collision",
		WorkspaceID:  workspaceID,
		DisplayName:  "Notion Collision",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}

	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "notion-collision"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	result, rpcErr := h.mcpToolDiscover(authCtx, discoverRaw)
	if rpcErr == nil || rpcErr.Code != errCodeInternal {
		t.Fatalf("expected discover collision to fail, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "collision") {
		t.Fatalf("expected collision error, got %+v", rpcErr)
	}

	partialToolID := mcpWorkspaceToolID("notion-collision", "search_docs")
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, partialToolID); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected no partial alias publish on failed discover, got err=%v", err)
	}

	cachedTools, err := h.mcpStore.ListServerTools(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list cached mcp tools: %v", err)
	}
	for _, tool := range cachedTools {
		if tool.ServerID == "notion-collision" {
			t.Fatalf("expected failed discover not to publish cached tools, got %+v", cachedTools)
		}
	}
}

func TestPublishDiscoveredMCPWorkspaceStateRejectsMutatedServerSnapshot(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-stale-discover-mutation"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Stale Discover Mutation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "stale-http",
		WorkspaceID:  workspaceID,
		DisplayName:  "Stale HTTP",
		Transport:    "streamable-http",
		URL:          "https://old.invalid/mcp",
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register original server: %v", err)
	}
	staleSnapshot, err := h.mcpStore.GetServer(ctx, workspaceID, "stale-http")
	if err != nil {
		t.Fatalf("get stale snapshot: %v", err)
	}
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "stale-http",
		WorkspaceID:  workspaceID,
		DisplayName:  "Stale HTTP",
		Transport:    "streamable-http",
		URL:          "https://new.invalid/mcp",
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("mutate active server: %v", err)
	}

	err = h.publishDiscoveredMCPWorkspaceState(ctx, authority, staleSnapshot, []mcp.Tool{{
		Name:        "search_docs",
		Description: "Search docs",
		InputSchema: []byte(`{"type":"object"}`),
	}}, "mcpdiscover-test-stale")
	if !errors.Is(err, errMCPDiscoverStaleServerSnapshot) {
		t.Fatalf("expected stale discover snapshot reject, got %v", err)
	}
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("stale-http", "search_docs")); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected stale discover publish to leave no alias, got %v", err)
	}
	if got := countCachedServerTools(t, ctx, h, workspaceID, "stale-http"); got != 0 {
		t.Fatalf("expected stale discover publish to leave no cached tools, got %d", got)
	}
}

func TestPublishDiscoveredMCPWorkspaceStateRejectsRemovedServerSnapshot(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-stale-discover-remove"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Stale Discover Remove",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "removed-http",
		WorkspaceID:  workspaceID,
		DisplayName:  "Removed HTTP",
		Transport:    "streamable-http",
		URL:          "https://removed.invalid/mcp",
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register original server: %v", err)
	}
	staleSnapshot, err := h.mcpStore.GetServer(ctx, workspaceID, "removed-http")
	if err != nil {
		t.Fatalf("get stale snapshot: %v", err)
	}
	if err := h.mcpStore.RemoveServer(ctx, workspaceID, "removed-http"); err != nil {
		t.Fatalf("remove active server: %v", err)
	}

	err = h.publishDiscoveredMCPWorkspaceState(ctx, authority, staleSnapshot, []mcp.Tool{{
		Name:        "search_docs",
		Description: "Search docs",
		InputSchema: []byte(`{"type":"object"}`),
	}}, "mcpdiscover-test-removed")
	if !errors.Is(err, errMCPDiscoverStaleServerSnapshot) {
		t.Fatalf("expected removed discover snapshot reject, got %v", err)
	}
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("removed-http", "search_docs")); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected removed discover publish to leave no alias, got %v", err)
	}
	if got := countCachedServerTools(t, ctx, h, workspaceID, "removed-http"); got != 0 {
		t.Fatalf("expected removed discover publish to leave no cached tools, got %d", got)
	}
}

func TestMCPServerAndToolListRemainWorkspaceSovereignForHumanPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-human-sovereign-list"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Human Sovereign List",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	servers := []mcp.RegisterInput{
		{
			ServerID:     "human-owned",
			WorkspaceID:  workspaceID,
			DisplayName:  "Human Owned",
			Transport:    "streamable-http",
			URL:          "https://human.invalid/mcp",
			RegisteredBy: "owner-a",
		},
		{
			ServerID:     "agent-owned",
			WorkspaceID:  workspaceID,
			DisplayName:  "Agent Owned",
			Transport:    "streamable-http",
			URL:          "https://agent.invalid/mcp",
			RegisteredBy: "agent:partner",
		},
		{
			ServerID:     "system-owned",
			WorkspaceID:  workspaceID,
			DisplayName:  "System Owned",
			Transport:    "streamable-http",
			URL:          "https://system.invalid/mcp",
			RegisteredBy: "system:daemon",
		},
	}
	for _, input := range servers {
		if err := h.mcpStore.RegisterServer(ctx, input); err != nil {
			t.Fatalf("register server %s: %v", input.ServerID, err)
		}
		server, err := h.mcpStore.GetServer(ctx, workspaceID, input.ServerID)
		if err != nil {
			t.Fatalf("get server %s: %v", input.ServerID, err)
		}
		if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, server, []mcp.Tool{{
			Name:        "search_docs",
			Description: "Search docs",
		}}, testMCPProjectionOperationID); err != nil {
			t.Fatalf("register alias for %s: %v", input.ServerID, err)
		}
		if err := h.mcpStore.SaveDiscoveredTools(ctx, workspaceID, input.ServerID, []mcp.Tool{{
			Name:        "search_docs",
			Description: "Search docs",
		}}); err != nil {
			t.Fatalf("save tools for %s: %v", input.ServerID, err)
		}
	}

	serverListRaw, err := json.Marshal(mcpServerListParams{WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("marshal server list params: %v", err)
	}
	serverRespAny, rpcErr := h.mcpServerList(testAuthContext(workspaceID, "human", "operator-b"), serverListRaw)
	if rpcErr != nil {
		t.Fatalf("mcpServerList rpc error: %+v", rpcErr)
	}
	serverResp, ok := serverRespAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected server list response type %T", serverRespAny)
	}
	serverRows, ok := serverResp["servers"].([]mcp.ServerRecord)
	if !ok || len(serverRows) != 3 {
		t.Fatalf("expected human principal to see all servers, got %+v", serverResp["servers"])
	}

	toolListRaw, err := json.Marshal(mcpToolListParams{WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("marshal tool list params: %v", err)
	}
	toolRespAny, rpcErr := h.mcpToolList(testAuthContext(workspaceID, "human", "operator-b"), toolListRaw)
	if rpcErr != nil {
		t.Fatalf("mcpToolList rpc error: %+v", rpcErr)
	}
	toolResp, ok := toolRespAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected tool list response type %T", toolRespAny)
	}
	toolRows, ok := toolResp["tools"].([]mcp.ServerToolRecord)
	if !ok || len(toolRows) != 3 {
		t.Fatalf("expected human principal to see all discovered tools, got %+v", toolResp["tools"])
	}
}

func newFakeMCPServerWithToolSet(t *testing.T, tools *[]mcp.Tool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var envelope map[string]any
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			t.Fatalf("decode mcp request: %v", err)
		}
		method, _ := envelope["method"].(string)
		switch {
		case method == "initialize":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      envelope["id"],
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"serverInfo": map[string]any{
						"name":    "fake-mcp-dynamic",
						"version": "1.0.0",
					},
				},
			})
		case strings.HasPrefix(method, "notifications/"):
			w.WriteHeader(http.StatusAccepted)
		case method == "tools/list":
			rows := make([]any, 0, len(*tools))
			for _, tool := range *tools {
				var inputSchema any = map[string]any{"type": "object"}
				if len(tool.InputSchema) > 0 {
					_ = json.Unmarshal(tool.InputSchema, &inputSchema)
				}
				rows = append(rows, map[string]any{
					"name":        tool.Name,
					"description": tool.Description,
					"inputSchema": inputSchema,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      envelope["id"],
				"result": map[string]any{
					"tools": rows,
				},
			})
		case method == "tools/call":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      envelope["id"],
				"result": map[string]any{
					"content": []any{
						map[string]any{"type": "text", "text": "ok"},
					},
					"isError": false,
				},
			})
		default:
			t.Fatalf("unexpected fake MCP method %q", method)
		}
	}))
}

func TestMCPToolDiscoverRejectsMissingWorkspaceAuthorityBeforeStartingStdioProcess(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-discover-stdio-missing-authority"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Discover Stdio Missing Authority",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "stdio-missing",
		WorkspaceID:  workspaceID,
		DisplayName:  "Stdio Missing Authority",
		Transport:    "stdio",
		Command:      "definitely-not-a-real-mcp-command",
		RegisteredBy: "system",
	}); err != nil {
		t.Fatalf("register stdio mcp server: %v", err)
	}

	raw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "stdio-missing"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}

	result, rpcErr := h.mcpToolDiscover(authCtx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject before stdio process start")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "mcp.tool.discover")
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("stdio-missing", "search_docs")); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected no discovered alias after authority reject, got %v", err)
	}
	tools, err := h.mcpStore.ListServerTools(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list cached server tools: %v", err)
	}
	if len(tools) != 0 {
		t.Fatalf("expected no cached tools after authority reject, got %+v", tools)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected authority preflight reject not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestMCPToolDiscoverRejectsCrossOwnerServer(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-discover-cross-owner"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Discover Cross Owner",
		CreatedBy:   "owner-a",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "owned-http",
		WorkspaceID:  workspaceID,
		DisplayName:  "Owned HTTP",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "owner-a",
	}); err != nil {
		t.Fatalf("register owned http server: %v", err)
	}

	raw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "owned-http"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}

	result, rpcErr := h.mcpToolDiscover(testAuthContext(workspaceID, "human", "intruder"), raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected cross-owner discover reject, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "owned by another principal") {
		t.Fatalf("expected ownership reject, got %+v", rpcErr)
	}
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("owned-http", "search_docs")); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected no alias after cross-owner discover reject, got %v", err)
	}
}

func TestMCPToolDiscoverRejectsStdioForNonSystemPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-discover-stdio-human"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Discover Stdio Human",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "stdio-human-discover",
		WorkspaceID:  workspaceID,
		DisplayName:  "Stdio Human Discover",
		Transport:    "stdio",
		Command:      "definitely-not-a-real-mcp-command",
		RegisteredBy: "system",
	}); err != nil {
		t.Fatalf("register stdio mcp server: %v", err)
	}

	raw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "stdio-human-discover"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}

	result, rpcErr := h.mcpToolDiscover(authCtx, raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected stdio discover reject for non-system principal, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "system path") {
		t.Fatalf("expected stdio system-path reject, got %+v", rpcErr)
	}
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("stdio-human-discover", "search_docs")); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected no alias after stdio discover reject, got %v", err)
	}
	if got := countCachedServerTools(t, ctx, h, workspaceID, "stdio-human-discover"); got != 0 {
		t.Fatalf("expected no cached tool rows after stdio discover reject, got %d", got)
	}
}

func TestMCPToolDiscoverRejectsWorkspaceAliasCollision(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-alias-collision"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Alias Collision",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()

	for _, serverID := range []string{"server-a", "server_a"} {
		if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
			ServerID:     serverID,
			WorkspaceID:  workspaceID,
			DisplayName:  "Collision " + serverID,
			Transport:    "streamable-http",
			URL:          mcpServer.URL,
			RegisteredBy: "developer",
		}); err != nil {
			t.Fatalf("register mcp server %s: %v", serverID, err)
		}
	}

	firstDiscoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "server-a"})
	if err != nil {
		t.Fatalf("marshal first discover params: %v", err)
	}
	if _, rpcErr := h.mcpToolDiscover(authCtx, firstDiscoverRaw); rpcErr != nil {
		t.Fatalf("first mcpToolDiscover rpc error: %+v", rpcErr)
	}

	secondDiscoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "server_a"})
	if err != nil {
		t.Fatalf("marshal second discover params: %v", err)
	}
	result, rpcErr := h.mcpToolDiscover(authCtx, secondDiscoverRaw)
	if rpcErr == nil {
		t.Fatalf("expected alias collision reject, got result=%+v", result)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "alias collision") {
		t.Fatalf("expected alias collision error, got %+v", rpcErr)
	}

	record, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("server-a", "search_docs"))
	if err != nil {
		t.Fatalf("load collided alias after reject: %v", err)
	}
	manifest := parseWorkspaceToolManifest(record.ManifestJSON)
	if manifest.Route == nil || manifest.Route.ServerID != "server-a" || manifest.Route.ToolName != "search_docs" {
		t.Fatalf("expected original alias route to stay intact, got %+v", manifest.Route)
	}
}

func TestMCPToolCallRejectsCrossOwnerAliasForAgentPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-call-cross-owner"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Call Cross Owner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "human-owned",
		WorkspaceID:  workspaceID,
		DisplayName:  "Human Owned",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register server: %v", err)
	}
	server, err := h.mcpStore.GetServer(ctx, workspaceID, "human-owned")
	if err != nil {
		t.Fatalf("get server: %v", err)
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, server, []mcp.Tool{{Name: "search_docs", Description: "Search docs"}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("register alias: %v", err)
	}
	if err := h.mcpStore.SaveDiscoveredTools(ctx, workspaceID, "human-owned", []mcp.Tool{{Name: "search_docs", Description: "Search docs"}}); err != nil {
		t.Fatalf("save tools: %v", err)
	}

	raw, err := json.Marshal(mcpToolCallParams{
		WorkspaceID: workspaceID,
		ServerID:    "human-owned",
		ToolName:    "search_docs",
		Arguments:   map[string]any{"query": "authority"},
	})
	if err != nil {
		t.Fatalf("marshal mcp.tool.call params: %v", err)
	}
	result, rpcErr := h.mcpToolCall(testAuthContext(workspaceID, "agent", "partner"), raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected cross-owner mcp.tool.call reject, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "owned by another principal") {
		t.Fatalf("expected ownership reject, got %+v", rpcErr)
	}
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.executed", mcpWorkspaceToolID("human-owned", "search_docs")); got != 0 {
		t.Fatalf("expected no executed events after cross-owner mcp.tool.call reject, got %d", got)
	}
}

func TestToolCallRejectsCrossOwnerMCPAliasForAgentPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-tool-call-cross-owner"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B Tool Call Cross Owner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "human-owned",
		WorkspaceID:  workspaceID,
		DisplayName:  "Human Owned",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register server: %v", err)
	}
	server, err := h.mcpStore.GetServer(ctx, workspaceID, "human-owned")
	if err != nil {
		t.Fatalf("get server: %v", err)
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, server, []mcp.Tool{{Name: "search_docs", Description: "Search docs"}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("register alias: %v", err)
	}
	if err := h.mcpStore.SaveDiscoveredTools(ctx, workspaceID, "human-owned", []mcp.Tool{{Name: "search_docs", Description: "Search docs"}}); err != nil {
		t.Fatalf("save tools: %v", err)
	}

	raw, err := json.Marshal(toolCallParams{
		WorkspaceID: workspaceID,
		ToolID:      mcpWorkspaceToolID("human-owned", "search_docs"),
		Arguments:   map[string]any{"query": "authority"},
	})
	if err != nil {
		t.Fatalf("marshal tool.call params: %v", err)
	}
	result, rpcErr := h.toolCall(testAuthContext(workspaceID, "agent", "partner"), raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected generic tool.call cross-owner reject, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "owned by another principal") {
		t.Fatalf("expected ownership reject, got %+v", rpcErr)
	}
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.executed", mcpWorkspaceToolID("human-owned", "search_docs")); got != 0 {
		t.Fatalf("expected no executed events after generic tool.call reject, got %d", got)
	}
}

func TestMCPToolCallAllowsSystemOwnedAliasForAgentPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-call-system-owned"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Call System Owned",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "system-owned",
		WorkspaceID:  workspaceID,
		DisplayName:  "System Owned",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "system:daemon",
	}); err != nil {
		t.Fatalf("register system-owned server: %v", err)
	}
	server, err := h.mcpStore.GetServer(ctx, workspaceID, "system-owned")
	if err != nil {
		t.Fatalf("get system-owned server: %v", err)
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, server, []mcp.Tool{{Name: "search_docs", Description: "Search docs"}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("register system alias: %v", err)
	}
	if err := h.mcpStore.SaveDiscoveredTools(ctx, workspaceID, "system-owned", []mcp.Tool{{Name: "search_docs", Description: "Search docs"}}); err != nil {
		t.Fatalf("save system tools: %v", err)
	}

	raw, err := json.Marshal(mcpToolCallParams{
		WorkspaceID: workspaceID,
		ServerID:    "system-owned",
		ToolName:    "search_docs",
		Arguments:   map[string]any{"query": "authority"},
	})
	if err != nil {
		t.Fatalf("marshal mcp.tool.call params: %v", err)
	}
	result, rpcErr := h.mcpToolCall(testAuthContext(workspaceID, "agent", "partner"), raw)
	if rpcErr != nil {
		t.Fatalf("expected system-owned alias to stay callable for agent principal, got result=%+v err=%+v", result, rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected response type %T", result)
	}
	if resp["tool_name"] != "search_docs" {
		t.Fatalf("unexpected mcp tool response %+v", resp)
	}
}

func TestMCPToolAndGenericToolCallRemainWorkspaceSovereignForHumanPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-human-sovereign-use"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Human Sovereign Use",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "agent-owned",
		WorkspaceID:  workspaceID,
		DisplayName:  "Agent Owned",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "agent:partner",
	}); err != nil {
		t.Fatalf("register agent-owned server: %v", err)
	}
	server, err := h.mcpStore.GetServer(ctx, workspaceID, "agent-owned")
	if err != nil {
		t.Fatalf("get agent-owned server: %v", err)
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, server, []mcp.Tool{{Name: "search_docs", Description: "Search docs"}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("register alias: %v", err)
	}
	if err := h.mcpStore.SaveDiscoveredTools(ctx, workspaceID, "agent-owned", []mcp.Tool{{Name: "search_docs", Description: "Search docs"}}); err != nil {
		t.Fatalf("save tools: %v", err)
	}

	humanCtx := testAuthContext(workspaceID, "human", "operator-b")
	mcpRaw, err := json.Marshal(mcpToolCallParams{
		WorkspaceID: workspaceID,
		ServerID:    "agent-owned",
		ToolName:    "search_docs",
		Arguments:   map[string]any{"query": "authority"},
	})
	if err != nil {
		t.Fatalf("marshal mcp.tool.call params: %v", err)
	}
	mcpRespAny, rpcErr := h.mcpToolCall(humanCtx, mcpRaw)
	if rpcErr != nil {
		t.Fatalf("expected human principal to call foreign alias via mcp.tool.call, got %+v", rpcErr)
	}
	mcpResp, ok := mcpRespAny.(map[string]any)
	if !ok || mcpResp["tool_name"] != "search_docs" {
		t.Fatalf("unexpected mcp.tool.call response %+v", mcpRespAny)
	}

	toolRaw, err := json.Marshal(toolCallParams{
		WorkspaceID: workspaceID,
		ToolID:      mcpWorkspaceToolID("agent-owned", "search_docs"),
		Arguments:   map[string]any{"query": "authority"},
	})
	if err != nil {
		t.Fatalf("marshal tool.call params: %v", err)
	}
	toolRespAny, rpcErr := h.toolCall(humanCtx, toolRaw)
	if rpcErr != nil {
		t.Fatalf("expected human principal to call foreign alias via tool.call, got %+v", rpcErr)
	}
	toolResp, ok := toolRespAny.(map[string]any)
	if !ok || toolResp["router_kind"] != "mcp" {
		t.Fatalf("unexpected tool.call routed response %+v", toolRespAny)
	}
}

func TestMCPToolDiscoverRegistersPolicyEnvelopedHTTPAlias(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-http-policy-envelope"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP HTTP Policy Envelope",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "http-policy",
		WorkspaceID:  workspaceID,
		DisplayName:  "HTTP Policy",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}

	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "http-policy"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	if _, rpcErr := h.mcpToolDiscover(authCtx, discoverRaw); rpcErr != nil {
		t.Fatalf("mcpToolDiscover rpc error: %+v", rpcErr)
	}

	record, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("http-policy", "search_docs"))
	if err != nil {
		t.Fatalf("get discovered http alias: %v", err)
	}
	if !containsCapability(record.Capabilities, "tool.call") || !containsCapability(record.Capabilities, "bridge.policy_enveloped") {
		t.Fatalf("expected discovered http alias to stay policy-enveloped, got capabilities %+v", record.Capabilities)
	}
	if containsCapability(record.Capabilities, "bridge.high_risk") || containsCapability(record.Capabilities, "bridge.operator_control_required") {
		t.Fatalf("expected discovered http alias to avoid high-risk stdio tags, got capabilities %+v", record.Capabilities)
	}
	if record.PolicyEnvelope == nil {
		t.Fatal("expected discovered http alias to carry a policy envelope")
	}
	if record.PolicyEnvelope.Surface != "mcp/streamable-http" || record.PolicyEnvelope.PrimaryTier != bridgepolicy.TierNetworked || record.PolicyEnvelope.HighRisk {
		t.Fatalf("unexpected http alias policy envelope %+v", record.PolicyEnvelope)
	}
}

func TestRegisterDiscoveredMCPWorkspaceToolsMarksStdioAliasesHighRisk(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-stdio-high-risk"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Stdio High Risk",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	server := mcp.ServerRecord{
		ServerID:     "stdio-risk",
		WorkspaceID:  workspaceID,
		DisplayName:  "Stdio Risk",
		Transport:    "stdio",
		RegisteredBy: "system",
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, server, []mcp.Tool{{
		Name:        "dangerous_exec",
		Description: "Dangerous exec",
	}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("register discovered stdio alias: %v", err)
	}

	record, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("stdio-risk", "dangerous_exec"))
	if err != nil {
		t.Fatalf("get discovered stdio alias: %v", err)
	}
	for _, required := range []string{"tool.call", "bridge.policy_enveloped", "bridge.high_risk", "bridge.operator_control_required"} {
		if !containsCapability(record.Capabilities, required) {
			t.Fatalf("expected discovered stdio alias capabilities to include %q, got %+v", required, record.Capabilities)
		}
	}
	if record.PolicyEnvelope == nil {
		t.Fatal("expected discovered stdio alias to carry a policy envelope")
	}
	if record.PolicyEnvelope.Surface != "mcp/stdio" ||
		record.PolicyEnvelope.PrimaryTier != bridgepolicy.TierCodeExec ||
		!record.PolicyEnvelope.HighRisk ||
		!record.PolicyEnvelope.OperatorControlRequired {
		t.Fatalf("unexpected stdio alias policy envelope %+v", record.PolicyEnvelope)
	}
}

func TestMCPToolCallRequiresApprovalForDiscoveredStdioAlias(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-stdio-approval"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Stdio Approval",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "stdio-approval",
		WorkspaceID:  workspaceID,
		DisplayName:  "Stdio Approval",
		Transport:    "stdio",
		Command:      "definitely-not-a-real-mcp-command",
		RegisteredBy: "system",
	}); err != nil {
		t.Fatalf("register stdio mcp server: %v", err)
	}
	server, err := h.mcpStore.GetServer(ctx, workspaceID, "stdio-approval")
	if err != nil {
		t.Fatalf("get stdio mcp server: %v", err)
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, server, []mcp.Tool{{
		Name:        "dangerous_exec",
		Description: "Dangerous exec",
	}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("register discovered stdio alias: %v", err)
	}

	raw, err := json.Marshal(mcpToolCallParams{
		ServerID:  "stdio-approval",
		ToolName:  "dangerous_exec",
		Arguments: map[string]any{"query": "authority"},
	})
	if err != nil {
		t.Fatalf("marshal mcp.tool.call params: %v", err)
	}

	result, rpcErr := h.mcpToolCall(authCtx, raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected approval-required reject for discovered stdio alias, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "approval") {
		t.Fatalf("expected approval-required message, got %+v", rpcErr)
	}

	toolID := mcpWorkspaceToolID("stdio-approval", "dangerous_exec")
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.approval_required",
		EntityType:  "tool",
		EntityID:    toolID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list approval-required runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one approval-required runtime event, got %+v", events)
	}
	if events[0].ActorType != "human" || events[0].ActorID != "developer" {
		t.Fatalf("expected approval-required event to bind to authenticated principal, got %+v", events[0])
	}
	assertServerRuntimeEventAuthorityMetadata(t, events[0], authority)
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.executed", toolID); got != 0 {
		t.Fatalf("expected no executed runtime events after approval-required reject, got %d", got)
	}

	queueKey := externalGateQueueKey("EXPLICIT_APPROVAL", bridgepolicy.ApprovalRequestKey(toolID, "human", "developer", "tool.call"))
	queue, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey)
	if err != nil {
		t.Fatalf("get approval queue: %v", err)
	}
	if !strings.EqualFold(queue.Status, "OPEN") {
		t.Fatalf("expected approval queue to stay open, got %+v", queue)
	}
}

func TestMCPToolCallRejectsStaleDiscoveredAliasClassification(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-stale-alias-classification"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Stale Alias Classification",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "stdio-stale",
		WorkspaceID:  workspaceID,
		DisplayName:  "Stdio Stale",
		Transport:    "stdio",
		Command:      "definitely-not-a-real-mcp-command",
		RegisteredBy: "system",
	}); err != nil {
		t.Fatalf("register stdio mcp server: %v", err)
	}

	staleManifestBytes, err := json.Marshal(map[string]any{
		"route": map[string]any{
			"kind":      "mcp",
			"server_id": "stdio-stale",
			"tool_name": "dangerous_exec",
		},
	})
	if err != nil {
		t.Fatalf("marshal stale manifest: %v", err)
	}
	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID:  workspaceID,
		ToolID:       mcpWorkspaceToolID("stdio-stale", "dangerous_exec"),
		DisplayName:  "dangerous_exec",
		Description:  "Stale alias",
		OwnerUserID:  "system",
		Kind:         "INTEGRATION",
		Status:       "ACTIVE",
		AccessLevel:  "WORKSPACE",
		Endpoint:     "mcp:stdio-stale",
		Capabilities: []string{"tool.call"},
		ManifestJSON: string(staleManifestBytes),
	}); err != nil {
		t.Fatalf("register stale workspace alias: %v", err)
	}

	raw, err := json.Marshal(mcpToolCallParams{
		ServerID:  "stdio-stale",
		ToolName:  "dangerous_exec",
		Arguments: map[string]any{"query": "authority"},
	})
	if err != nil {
		t.Fatalf("marshal mcp.tool.call params: %v", err)
	}

	result, rpcErr := h.mcpToolCall(authCtx, raw)
	if rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected stale-alias invalid params reject, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "stale") || !strings.Contains(strings.ToLower(rpcErr.Message), "discover") {
		t.Fatalf("expected stale alias re-discover guidance, got %+v", rpcErr)
	}

	toolID := mcpWorkspaceToolID("stdio-stale", "dangerous_exec")
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.executed", toolID); got != 0 {
		t.Fatalf("expected no executed runtime events after stale alias reject, got %d", got)
	}
}

func TestMCPServerRegisterRejectsRegisteredBySpoofAgainstAuthenticatedPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-register-spoof"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Register Spoof",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	raw, err := json.Marshal(mcpServerRegisterParams{
		WorkspaceID:  workspaceID,
		ServerID:     "spoofed",
		DisplayName:  "Spoofed",
		Transport:    "streamable-http",
		URL:          "https://example.invalid/mcp",
		RegisteredBy: "system",
	})
	if err != nil {
		t.Fatalf("marshal mcp.server.register params: %v", err)
	}

	result, rpcErr := h.mcpServerRegister(authCtx, raw)
	if rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected registered_by spoof reject, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "registered_by") {
		t.Fatalf("expected registered_by spoof message, got %+v", rpcErr)
	}
	if _, err := h.mcpStore.GetServer(ctx, workspaceID, "spoofed"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected no spoofed server registration, got %v", err)
	}
}

func TestMCPServerRegisterRejectsStdioForNonSystemPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-register-stdio-human"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Register Stdio Human",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	raw, err := json.Marshal(mcpServerRegisterParams{
		WorkspaceID: workspaceID,
		ServerID:    "stdio-human",
		DisplayName: "Stdio Human",
		Transport:   "stdio",
		Command:     "definitely-not-a-real-mcp-command",
	})
	if err != nil {
		t.Fatalf("marshal stdio register params: %v", err)
	}

	result, rpcErr := h.mcpServerRegister(authCtx, raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected stdio human register reject, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "system path") {
		t.Fatalf("expected stdio system-path message, got %+v", rpcErr)
	}
	if _, err := h.mcpStore.GetServer(ctx, workspaceID, "stdio-human"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected no stdio server registration for human principal, got %v", err)
	}
}

func TestMCPServerRegisterRejectsCrossOwnerUpdate(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-register-cross-owner"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Cross Owner Update",
		CreatedBy:   "owner-a",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "shared-http",
		WorkspaceID:  workspaceID,
		DisplayName:  "Shared HTTP",
		Transport:    "streamable-http",
		URL:          "https://original.invalid/mcp",
		RegisteredBy: "owner-a",
	}); err != nil {
		t.Fatalf("seed server: %v", err)
	}

	raw, err := json.Marshal(mcpServerRegisterParams{
		WorkspaceID: workspaceID,
		ServerID:    "shared-http",
		DisplayName: "Hijacked HTTP",
		Transport:   "streamable-http",
		URL:         "https://hijacked.invalid/mcp",
	})
	if err != nil {
		t.Fatalf("marshal cross-owner update params: %v", err)
	}

	result, rpcErr := h.mcpServerRegister(testAuthContext(workspaceID, "human", "intruder"), raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected cross-owner update reject, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "owned by another principal") {
		t.Fatalf("expected cross-owner ownership message, got %+v", rpcErr)
	}

	server, err := h.mcpStore.GetServer(ctx, workspaceID, "shared-http")
	if err != nil {
		t.Fatalf("get seeded server: %v", err)
	}
	if server.DisplayName != "Shared HTTP" || server.URL != "https://original.invalid/mcp" {
		t.Fatalf("expected original server registration to stay intact, got %+v", server)
	}
}

func TestMCPServerRegisterRejectsRemovedServerReclaimByAnotherPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-reregister-removed-cross-owner"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Removed Cross Owner",
		CreatedBy:   "owner-a",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "removed-http",
		WorkspaceID:  workspaceID,
		DisplayName:  "Removed HTTP",
		Transport:    "streamable-http",
		URL:          "https://removed.invalid/mcp",
		RegisteredBy: "owner-a",
	}); err != nil {
		t.Fatalf("seed server: %v", err)
	}
	if err := h.mcpStore.RemoveServer(ctx, workspaceID, "removed-http"); err != nil {
		t.Fatalf("seed remove server: %v", err)
	}

	raw, err := json.Marshal(mcpServerRegisterParams{
		WorkspaceID: workspaceID,
		ServerID:    "removed-http",
		DisplayName: "Removed HTTP Hijacked",
		Transport:   "streamable-http",
		URL:         "https://hijacked.invalid/mcp",
	})
	if err != nil {
		t.Fatalf("marshal removed re-register params: %v", err)
	}

	result, rpcErr := h.mcpServerRegister(testAuthContext(workspaceID, "human", "intruder"), raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected removed-row reclaim reject, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "owned by another principal") {
		t.Fatalf("expected ownership reject, got %+v", rpcErr)
	}
	if _, err := h.mcpStore.GetServer(ctx, workspaceID, "removed-http"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected removed row to stay inactive after reject, got %v", err)
	}
	servers, err := h.mcpStore.ListServers(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list servers after reject: %v", err)
	}
	if len(servers) != 0 {
		t.Fatalf("expected removed server to stay absent from active list, got %+v", servers)
	}
}

func TestMCPServerRegisterRejectsCrossTypeOwnerCollision(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-cross-type-owner"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Cross Type Owner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "human-owned",
		WorkspaceID:  workspaceID,
		DisplayName:  "Human Owned",
		Transport:    "streamable-http",
		URL:          "https://human.invalid/mcp",
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("seed human-owned server: %v", err)
	}

	raw, err := json.Marshal(mcpServerRegisterParams{
		WorkspaceID: workspaceID,
		ServerID:    "human-owned",
		DisplayName: "Agent Hijack",
		Transport:   "streamable-http",
		URL:         "https://agent.invalid/mcp",
	})
	if err != nil {
		t.Fatalf("marshal cross-type register params: %v", err)
	}

	result, rpcErr := h.mcpServerRegister(testAuthContext(workspaceID, "agent", "developer"), raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected cross-type owner reject, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "owned by another principal") {
		t.Fatalf("expected ownership reject, got %+v", rpcErr)
	}
	server, err := h.mcpStore.GetServer(ctx, workspaceID, "human-owned")
	if err != nil {
		t.Fatalf("get human-owned server: %v", err)
	}
	if server.URL != "https://human.invalid/mcp" || server.RegisteredBy != "developer" {
		t.Fatalf("expected human-owned server to stay intact, got %+v", server)
	}
}

func TestMCPServerRegisterClearsDiscoveredStateOnMutation(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-register-clears-state"
	authCtx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Register Clears State",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "mutable-http",
		WorkspaceID:  workspaceID,
		DisplayName:  "Mutable HTTP",
		Transport:    "streamable-http",
		URL:          "https://before.invalid/mcp",
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("seed server: %v", err)
	}
	server, err := h.mcpStore.GetServer(ctx, workspaceID, "mutable-http")
	if err != nil {
		t.Fatalf("get mutable server: %v", err)
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, server, []mcp.Tool{{
		Name:        "search_docs",
		Description: "Search docs",
	}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("seed discovered alias: %v", err)
	}
	if err := h.mcpStore.SaveDiscoveredTools(ctx, workspaceID, "mutable-http", []mcp.Tool{{
		Name:        "search_docs",
		Description: "Search docs",
	}}); err != nil {
		t.Fatalf("seed discovered cache: %v", err)
	}

	raw, err := json.Marshal(mcpServerRegisterParams{
		WorkspaceID: workspaceID,
		ServerID:    "mutable-http",
		DisplayName: "Mutable HTTP Updated",
		Transport:   "streamable-http",
		URL:         "https://after.invalid/mcp",
	})
	if err != nil {
		t.Fatalf("marshal update params: %v", err)
	}
	result, rpcErr := h.mcpServerRegister(authCtx, raw)
	if rpcErr != nil {
		t.Fatalf("mcpServerRegister rpc error: %+v (result=%+v)", rpcErr, result)
	}

	if _, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("mutable-http", "search_docs")); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected mutable-http alias to be cleared after register mutation, got %v", err)
	}
	if got := countCachedServerTools(t, ctx, h, workspaceID, "mutable-http"); got != 0 {
		t.Fatalf("expected discovered tool cache to be cleared after register mutation, got %d", got)
	}
	server, err = h.mcpStore.GetServer(ctx, workspaceID, "mutable-http")
	if err != nil {
		t.Fatalf("get updated server: %v", err)
	}
	if server.URL != "https://after.invalid/mcp" || server.RegisteredBy != "developer" {
		t.Fatalf("unexpected updated server %+v", server)
	}
}

func TestMCPServerRemoveRejectsCrossOwnerAndClearsDiscoveredState(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-remove-owner"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Remove Owner",
		CreatedBy:   "owner-a",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "owned-http",
		WorkspaceID:  workspaceID,
		DisplayName:  "Owned HTTP",
		Transport:    "streamable-http",
		URL:          "https://owned.invalid/mcp",
		RegisteredBy: "owner-a",
	}); err != nil {
		t.Fatalf("seed server: %v", err)
	}
	server, err := h.mcpStore.GetServer(ctx, workspaceID, "owned-http")
	if err != nil {
		t.Fatalf("get owned server: %v", err)
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, server, []mcp.Tool{{
		Name:        "search_docs",
		Description: "Search docs",
	}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("seed discovered alias: %v", err)
	}
	if err := h.mcpStore.SaveDiscoveredTools(ctx, workspaceID, "owned-http", []mcp.Tool{{
		Name:        "search_docs",
		Description: "Search docs",
	}}); err != nil {
		t.Fatalf("seed discovered cache: %v", err)
	}

	rejectRaw, err := json.Marshal(mcpServerRemoveParams{
		WorkspaceID: workspaceID,
		ServerID:    "owned-http",
	})
	if err != nil {
		t.Fatalf("marshal remove params: %v", err)
	}
	result, rpcErr := h.mcpServerRemove(testAuthContext(workspaceID, "human", "intruder"), rejectRaw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected cross-owner remove reject, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "owned by another principal") {
		t.Fatalf("expected cross-owner remove ownership message, got %+v", rpcErr)
	}
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("owned-http", "search_docs")); err != nil {
		t.Fatalf("expected alias to remain after cross-owner reject, got %v", err)
	}
	if got := countCachedServerTools(t, ctx, h, workspaceID, "owned-http"); got != 1 {
		t.Fatalf("expected discovered cache to remain after cross-owner reject, got %d", got)
	}

	result, rpcErr = h.mcpServerRemove(testAuthContext(workspaceID, "human", "owner-a"), rejectRaw)
	if rpcErr != nil {
		t.Fatalf("owner remove rpc error: %+v (result=%+v)", rpcErr, result)
	}
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("owned-http", "search_docs")); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected alias cleared after owner remove, got %v", err)
	}
	if got := countCachedServerTools(t, ctx, h, workspaceID, "owned-http"); got != 0 {
		t.Fatalf("expected discovered cache cleared after owner remove, got %d", got)
	}
	servers, err := h.mcpStore.ListServers(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list servers after owner remove: %v", err)
	}
	if len(servers) != 0 {
		t.Fatalf("expected removed server to disappear from active list, got %+v", servers)
	}
	if _, err := h.mcpStore.GetServer(ctx, workspaceID, "owned-http"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected removed server to be undiscoverable as active, got %v", err)
	}
	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{WorkspaceID: workspaceID, ServerID: "owned-http"})
	if err != nil {
		t.Fatalf("marshal post-remove discover params: %v", err)
	}
	result, rpcErr = h.mcpToolDiscover(testAuthContext(workspaceID, "human", "owner-a"), discoverRaw)
	if rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected discover reject for removed server, got result=%+v err=%+v", result, rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "server not found") {
		t.Fatalf("expected removed server not found message, got %+v", rpcErr)
	}
}

func TestMCPServerRemoveClearsOrphanAliasWithoutCachedToolRow(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-sh2b-mcp-remove-orphan-alias"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "SH2B MCP Remove Orphan Alias",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "orphan-http",
		WorkspaceID:  workspaceID,
		DisplayName:  "Orphan HTTP",
		Transport:    "streamable-http",
		URL:          "https://orphan.invalid/mcp",
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("seed server: %v", err)
	}
	server, err := h.mcpStore.GetServer(ctx, workspaceID, "orphan-http")
	if err != nil {
		t.Fatalf("get orphan server: %v", err)
	}
	if err := registerDiscoveredMCPWorkspaceToolsForTest(t, h, ctx, server, []mcp.Tool{{
		Name:        "search_docs",
		Description: "Search docs",
	}}, testMCPProjectionOperationID); err != nil {
		t.Fatalf("seed orphan alias: %v", err)
	}
	if got := countCachedServerTools(t, ctx, h, workspaceID, "orphan-http"); got != 0 {
		t.Fatalf("expected no discovered cache rows in orphan setup, got %d", got)
	}

	removeRaw, err := json.Marshal(mcpServerRemoveParams{
		WorkspaceID: workspaceID,
		ServerID:    "orphan-http",
	})
	if err != nil {
		t.Fatalf("marshal orphan remove params: %v", err)
	}
	result, rpcErr := h.mcpServerRemove(testAuthContext(workspaceID, "human", "developer"), removeRaw)
	if rpcErr != nil {
		t.Fatalf("owner remove rpc error: %+v (result=%+v)", rpcErr, result)
	}
	if _, err := store.GetWorkspaceTool(ctx, workspaceID, mcpWorkspaceToolID("orphan-http", "search_docs")); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected orphan alias cleared even without cached tool rows, got %v", err)
	}
	if _, err := h.mcpStore.GetServer(ctx, workspaceID, "orphan-http"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected removed orphan server to be inactive, got %v", err)
	}
}

func containsCapability(capabilities []string, want string) bool {
	for _, capability := range capabilities {
		if strings.EqualFold(strings.TrimSpace(capability), strings.TrimSpace(want)) {
			return true
		}
	}
	return false
}

func countCachedServerTools(t *testing.T, ctx context.Context, h *Handler, workspaceID, serverID string) int {
	t.Helper()
	tools, err := h.mcpStore.ListServerTools(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list cached server tools: %v", err)
	}
	count := 0
	for _, tool := range tools {
		if strings.EqualFold(strings.TrimSpace(tool.ServerID), strings.TrimSpace(serverID)) {
			count++
		}
	}
	return count
}
