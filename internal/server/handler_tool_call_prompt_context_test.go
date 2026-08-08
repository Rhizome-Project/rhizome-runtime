package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestToolCallExecutedRuntimeEventCarriesPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-tool-call-runtime-prompt"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool call runtime prompt",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	mcpServer := newFakeMCPServer(t)
	defer mcpServer.Close()
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "prompt-mcp",
		WorkspaceID:  workspaceID,
		DisplayName:  "Prompt MCP",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "developer",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}
	discoverRaw, err := json.Marshal(mcpToolDiscoverParams{ServerID: "prompt-mcp"})
	if err != nil {
		t.Fatalf("marshal discover params: %v", err)
	}
	if _, rpcErr := h.mcpToolDiscover(ctx, discoverRaw); rpcErr != nil {
		t.Fatalf("mcpToolDiscover rpc error: %+v", rpcErr)
	}

	toolID := mcpWorkspaceToolID("prompt-mcp", "search_docs")
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)
	callRaw, err := json.Marshal(toolCallParams{
		ToolID:      toolID,
		WorkspaceID: workspaceID,
		Arguments:   map[string]any{"query": "prompt context"},
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
	operationID, ok := resp["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("tool call response missing operation_id: %+v", resp)
	}

	runtimeEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    toolID,
		Limit:       10,
	})
	assertServerRuntimeEventAuthorityMetadata(t, runtimeEvent, authority)
	live := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, live, runtimeEvent, "tool.call.executed")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, live.PayloadJSON), runtimeEvent.PayloadJSON)

	payload := decodeEventPayloadMap(t, runtimeEvent.PayloadJSON)
	assertToolCallRuntimePayloadPromptContext(t, payload, toolCallRuntimePromptContextWant{
		workspaceID:         workspaceID,
		toolID:              toolID,
		eventType:           "tool.call.executed",
		principalType:       "human",
		principalID:         "developer",
		actorType:           "human",
		actorID:             "developer",
		requestedCapability: "tool.call",
		operationID:         operationID,
	})
}

func TestDirectToolCallExecutedRuntimeEventCarriesPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-direct-tool-call-runtime-prompt"
		agentID     = "agent-direct-tool-call-runtime-prompt"
		toolID      = "direct-tool-call-runtime-prompt"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Direct tool call runtime prompt",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerToolLedgerAgent(t, ctx, store, workspaceID, agentID)
	deployToolForOperationLedgerTest(t, h, ctx, workspaceID, toolID, `
let body = "";
process.stdin.on("data", chunk => body += chunk);
process.stdin.on("end", () => {
  const args = JSON.parse(body || "{}");
  console.log("direct-runtime-context:" + String(args.value || ""));
});
`)

	callRaw, err := json.Marshal(toolCallParams{
		ToolID:      toolID,
		WorkspaceID: workspaceID,
		Arguments:   map[string]any{"value": "ok"},
		ActorType:   "agent",
		ActorID:     agentID,
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
	operationID, ok := resp["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("tool call response missing operation_id: %+v", resp)
	}
	runtimeEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    toolID,
		Limit:       10,
	})
	assertServerRuntimeEventAuthorityMetadata(t, runtimeEvent, authority)
	assertToolCallRuntimePromptContext(t, runtimeEvent, "tool.call.executed", workspaceID, "agent", agentID, "agent", agentID, toolID, "tool.call", operationID)
}

func TestToolCallDeniedRuntimeEventCarriesPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-tool-call-denied-prompt"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool call denied prompt",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "human",
		SubjectID:   "developer",
		Capability:  "tool.call",
		ToolID:      "blocked-tool",
		Effect:      "DENY",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("put capability policy: %v", err)
	}

	raw, err := json.Marshal(toolCallParams{
		ToolID:      "blocked-tool",
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("marshal tool call params: %v", err)
	}
	if _, rpcErr := h.toolCall(ctx, raw); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied, got %+v", rpcErr)
	}

	runtimeEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.denied",
		EntityType:  "tool",
		EntityID:    "blocked-tool",
		Limit:       10,
	})
	assertServerRuntimeEventAuthorityMetadata(t, runtimeEvent, authority)
	payload := decodeEventPayloadMap(t, runtimeEvent.PayloadJSON)
	if payload["policy_verdict"] != "DENY" {
		t.Fatalf("expected policy_verdict DENY in runtime payload, got %+v", payload)
	}
	assertToolCallRuntimePayloadPromptContext(t, payload, toolCallRuntimePromptContextWant{
		workspaceID:         workspaceID,
		toolID:              "blocked-tool",
		eventType:           "tool.call.denied",
		principalType:       "human",
		principalID:         "developer",
		actorType:           "human",
		actorID:             "developer",
		requestedCapability: "tool.call",
	})
}

type toolCallRuntimePromptContextWant struct {
	workspaceID         string
	toolID              string
	eventType           string
	principalType       string
	principalID         string
	actorType           string
	actorID             string
	requestedCapability string
	operationID         string
}

func assertToolCallRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, eventType, workspaceID, principalType, principalID, actorType, actorID, toolID, requestedCapability, operationID string) {
	t.Helper()
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	assertToolCallRuntimePayloadPromptContext(t, payload, toolCallRuntimePromptContextWant{
		workspaceID:         workspaceID,
		toolID:              toolID,
		eventType:           eventType,
		principalType:       principalType,
		principalID:         principalID,
		actorType:           actorType,
		actorID:             actorID,
		requestedCapability: requestedCapability,
		operationID:         operationID,
	})
}

func assertToolCallRuntimePayloadPromptContext(t *testing.T, payload map[string]any, want toolCallRuntimePromptContextWant) {
	t.Helper()

	assertPayloadStringField(t, payload, "workspace_id", want.workspaceID)
	assertPayloadStringField(t, payload, "tool_id", want.toolID)
	assertPayloadStringField(t, payload, "event_type", want.eventType)
	assertPayloadStringField(t, payload, "entity_type", "tool")
	assertPayloadStringField(t, payload, "entity_id", want.toolID)
	assertPayloadStringField(t, payload, "actor_type", want.actorType)
	assertPayloadStringField(t, payload, "actor_id", want.actorID)
	assertPayloadStringField(t, payload, "requested_capability", want.requestedCapability)
	assertPayloadStringField(t, payload, "authority_event_scope", "tool.call")
	if want.operationID != "" {
		assertPayloadStringField(t, payload, "operation_id", want.operationID)
	} else if _, ok := payload["operation_id"]; ok {
		t.Fatalf("unexpected operation_id in non-operation runtime payload: %+v", payload)
	}

	rawEnvelope, ok := payload["prompt_context_envelope"]
	if !ok {
		t.Fatalf("missing prompt_context_envelope in runtime payload %+v", payload)
	}
	envelope, ok := rawEnvelope.(map[string]any)
	if !ok {
		t.Fatalf("prompt_context_envelope has type %T, want map[string]any", rawEnvelope)
	}
	assertPayloadStringField(t, envelope, "contract", "prompt_context_envelope.v1")
	assertPayloadStringField(t, envelope, "context_kind", "authority_bearing_tool_call")
	assertPayloadStringField(t, envelope, "surface", "tool.call")
	assertPayloadStringField(t, envelope, "origin", "server_rpc")
	assertPayloadStringField(t, envelope, "workspace_id", want.workspaceID)
	assertPayloadStringField(t, envelope, "principal_type", want.principalType)
	assertPayloadStringField(t, envelope, "principal_id", want.principalID)
	assertPayloadStringField(t, envelope, "authority_model", "workspace_authority")
	assertPayloadStringField(t, envelope, "compiler_status", "non_daemon_context_envelope")
	assertPayloadStringField(t, envelope, "daemon_prompt_compiler_convergence", "not_claimed")
	assertPayloadStringField(t, envelope, "prompt_capability_evidence", "not_present")
	assertPayloadStringField(t, envelope, "tool_id", want.toolID)
	assertPayloadStringField(t, envelope, "event_type", want.eventType)
	assertPayloadStringField(t, envelope, "entity_type", "tool")
	assertPayloadStringField(t, envelope, "entity_id", want.toolID)
	assertPayloadStringField(t, envelope, "actor_type", want.actorType)
	assertPayloadStringField(t, envelope, "actor_id", want.actorID)
	assertPayloadStringField(t, envelope, "requested_capability", want.requestedCapability)
	assertPayloadStringField(t, envelope, "authority_event_scope", "tool.call")
	if want.operationID != "" {
		assertPayloadStringField(t, envelope, "operation_id", want.operationID)
	} else if _, ok := envelope["operation_id"]; ok {
		t.Fatalf("unexpected operation_id in non-operation runtime envelope: %+v", envelope)
	}
}

func assertPayloadStringField(t *testing.T, payload map[string]any, key, want string) {
	t.Helper()
	got, ok := payload[key].(string)
	if !ok {
		t.Fatalf("payload[%s] has type %T, want string in %+v", key, payload[key], payload)
	}
	if got != want {
		t.Fatalf("payload[%s] = %q, want %q in %+v", key, got, want, payload)
	}
}
