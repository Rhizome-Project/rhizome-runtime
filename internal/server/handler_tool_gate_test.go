package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/bridgepolicy"
	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestHighRiskToolCallUsesAuthenticatedPrincipalWhenActorContextOmitted(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-high-risk-actor"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "High Risk Actor",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerHighRiskBridgeTool(t, ctx, store, h, workspaceID, "dangerous-provider")

	raw, err := json.Marshal(toolCallParams{
		ToolID:      "dangerous-provider",
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("marshal tool call params: %v", err)
	}

	_, rpcErr := h.toolCall(ctx, raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected approval-required denial for actorless high-risk tool call, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected rpc details map, got %T", rpcErr.Details)
	}
	if _, ok := details["approval_queue"]; !ok {
		t.Fatalf("expected approval queue details, got %+v", details)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.approval_required",
		EntityType:  "tool",
		EntityID:    "dangerous-provider",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected approval-required runtime event, got %+v", events)
	}
	if events[0].ActorType != "human" || events[0].ActorID != "developer" {
		t.Fatalf("expected approval event to bind to authenticated principal, got %+v", events[0])
	}
	if !strings.Contains(events[0].PayloadJSON, "explicit_allow_policy_required") {
		t.Fatalf("expected approval-required runtime event payload, got %+v", events[0])
	}
	assertServerRuntimeEventAuthorityMetadata(t, events[0], authority)
	assertToolCallRuntimePromptContext(t, events[0], "tool.call.approval_required", workspaceID, "human", "developer", "human", "developer", "dangerous-provider", "tool.call", "")
}

func TestHighRiskToolCallRequiresApprovalAndCreatesOperatorQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-high-risk-approval"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "High Risk Approval",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	registerHighRiskBridgeTool(t, ctx, store, h, workspaceID, "dangerous-provider")

	raw, err := json.Marshal(toolCallParams{
		ToolID:      "dangerous-provider",
		WorkspaceID: workspaceID,
		ActorType:   "agent",
		ActorID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("marshal tool call params: %v", err)
	}

	_, rpcErr := h.toolCall(ctx, raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected approval-required denial, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected rpc details map, got %T", rpcErr.Details)
	}
	queueAny, ok := details["approval_queue"]
	if !ok {
		t.Fatalf("expected approval queue details, got %+v", details)
	}
	queueMap, ok := queueAny.(sqlite.OperatorQueueRecord)
	if !ok {
		t.Fatalf("expected approval queue record, got %T", queueAny)
	}
	if queueMap.QueueType != "DECISION" || !strings.EqualFold(queueMap.Status, "OPEN") {
		t.Fatalf("unexpected approval queue %+v", queueMap)
	}
	if !strings.Contains(queueMap.PayloadJSON, "explicit_allow_policy_required") {
		t.Fatalf("expected gate payload in queue, got %+v", queueMap)
	}

	approvalEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.approval_required",
		EntityType:  "tool",
		EntityID:    "dangerous-provider",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list approval runtime events: %v", err)
	}
	if len(approvalEvents) != 1 || !strings.Contains(approvalEvents[0].PayloadJSON, "implicit_allow_suppressed") {
		t.Fatalf("expected approval-required runtime event with suppressed implicit allow, got %+v", approvalEvents)
	}
	assertServerRuntimeEventAuthorityMetadata(t, approvalEvents[0], authority)
	assertToolCallRuntimePromptContext(t, approvalEvents[0], "tool.call.approval_required", workspaceID, "agent", "agent-a", "agent", "agent-a", "dangerous-provider", "tool.call", "")
}

func TestHighRiskToolCallAllowsExplicitPolicyAndResolvesQueue(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-high-risk-allow"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "High Risk Allow",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	registerHighRiskBridgeTool(t, ctx, store, h, workspaceID, "dangerous-provider")

	callRaw, err := json.Marshal(toolCallParams{
		ToolID:      "dangerous-provider",
		WorkspaceID: workspaceID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		Arguments:   map[string]any{"query": "runtime state"},
	})
	if err != nil {
		t.Fatalf("marshal tool call params: %v", err)
	}
	if _, rpcErr := h.toolCall(ctx, callRaw); rpcErr == nil {
		t.Fatal("expected first high-risk tool call to require approval")
	}

	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "dangerous-provider",
		Effect:      "ALLOW",
		Reason:      "operator allowed legacy bridge for this agent",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("put capability policy: %v", err)
	}

	respAny, rpcErr := h.toolCall(ctx, callRaw)
	if rpcErr != nil {
		t.Fatalf("toolCall rpc error after allow policy: %+v", rpcErr)
	}
	resp, ok := respAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected response type %T", respAny)
	}
	if resp["router_kind"] != "mcp" {
		t.Fatalf("expected routed tool execution after approval, got %+v", resp)
	}

	queueKey := externalGateQueueKey("EXPLICIT_APPROVAL", bridgepolicy.ApprovalRequestKey("dangerous-provider", "agent", "agent-a", "tool.call"))
	queue, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey)
	if err != nil {
		t.Fatalf("get approval queue: %v", err)
	}
	if !strings.EqualFold(queue.Status, "RESOLVED") {
		t.Fatalf("expected approval queue to resolve after explicit allow, got %+v", queue)
	}
}

func TestToolCallCapabilityPolicyUsesAuthenticatedPrincipalWhenActorContextOmitted(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-tool-policy-auth-principal"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Policy Auth Principal",
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
		ToolID:      "dangerous-tool",
		Effect:      "DENY",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("put capability policy: %v", err)
	}

	raw, err := json.Marshal(toolCallParams{
		ToolID:      "dangerous-tool",
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("marshal tool call params: %v", err)
	}

	_, rpcErr := h.toolCall(ctx, raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied for omitted actor context, got %+v", rpcErr)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.denied",
		EntityType:  "tool",
		EntityID:    "dangerous-tool",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected denied runtime event, got %+v", events)
	}
	if events[0].ActorType != "human" || events[0].ActorID != "developer" {
		t.Fatalf("expected denied event to bind to authenticated principal, got %+v", events[0])
	}
	if !strings.Contains(events[0].PayloadJSON, "\"requested_capability\":\"tool.call\"") {
		t.Fatalf("expected canonical requested capability in payload, got %+v", events[0])
	}
	assertServerRuntimeEventAuthorityMetadata(t, events[0], authority)
	assertToolCallRuntimePromptContext(t, events[0], "tool.call.denied", workspaceID, "human", "developer", "human", "developer", "dangerous-tool", "tool.call", "")
}

func TestToolCallRejectsActorContextMismatchAgainstAuthenticatedPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-tool-policy-actor-mismatch"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Policy Actor Mismatch",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(toolCallParams{
		ToolID:      "dangerous-tool",
		WorkspaceID: workspaceID,
		ActorType:   "agent",
		ActorID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("marshal tool call params: %v", err)
	}

	_, rpcErr := h.toolCall(ctx, raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied for mismatched actor context, got %+v", rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "authenticated principal") {
		t.Fatalf("expected authenticated-principal mismatch message, got %+v", rpcErr)
	}
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.denied", "dangerous-tool"); got != 0 {
		t.Fatalf("expected no denied runtime events on actor mismatch, got %d", got)
	}
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.approval_required", "dangerous-tool"); got != 0 {
		t.Fatalf("expected no approval runtime events on actor mismatch, got %d", got)
	}
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.executed", "dangerous-tool"); got != 0 {
		t.Fatalf("expected no executed runtime events on actor mismatch, got %d", got)
	}
	if got := countOperatorQueueRowsForToolCall(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no operator queue rows on actor mismatch, got %d", got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected actor mismatch reject not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestToolCallRejectsUnsupportedRequestedCapability(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-tool-policy-capability-steering"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Policy Capability Steering",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(toolCallParams{
		ToolID:              "dangerous-tool",
		WorkspaceID:         workspaceID,
		RequestedCapability: "workspace.policy.put",
	})
	if err != nil {
		t.Fatalf("marshal tool call params: %v", err)
	}

	_, rpcErr := h.toolCall(ctx, raw)
	if rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for unsupported requested capability, got %+v", rpcErr)
	}
	if !strings.Contains(strings.ToLower(rpcErr.Message), "requested_capability") {
		t.Fatalf("expected requested_capability error, got %+v", rpcErr)
	}
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.denied", "dangerous-tool"); got != 0 {
		t.Fatalf("expected no denied runtime events on capability steering reject, got %d", got)
	}
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.approval_required", "dangerous-tool"); got != 0 {
		t.Fatalf("expected no approval runtime events on capability steering reject, got %d", got)
	}
	if got := countToolCallRuntimeEvents(t, ctx, store, workspaceID, "tool.call.executed", "dangerous-tool"); got != 0 {
		t.Fatalf("expected no executed runtime events on capability steering reject, got %d", got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected capability steering reject not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func registerHighRiskBridgeTool(t *testing.T, ctx context.Context, store *sqlite.Store, h *Handler, workspaceID, toolID string) {
	t.Helper()

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "risk-bridge",
		OwnerUserID: "developer",
		DisplayName: "Risk Bridge",
	}); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		t.Fatalf("register bridge agent: %v", err)
	}

	mcpServer := newFakeMCPServer(t)
	t.Cleanup(mcpServer.Close)
	if err := h.mcpStore.RegisterServer(ctx, mcp.RegisterInput{
		ServerID:     "bridge-gate",
		WorkspaceID:  workspaceID,
		DisplayName:  "Bridge Gate MCP",
		Transport:    "streamable-http",
		URL:          mcpServer.URL,
		RegisteredBy: "system:risk-bridge",
	}); err != nil {
		t.Fatalf("register mcp server: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	server, err := h.mcpStore.GetServer(ctx, workspaceID, "bridge-gate")
	if err != nil {
		t.Fatalf("get bridge server: %v", err)
	}

	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID:  workspaceID,
		ToolID:       toolID,
		DisplayName:  "Dangerous Provider",
		Description:  "Legacy high-risk bridge",
		OwnerUserID:  "system",
		OwnerAgentID: "risk-bridge",
		Kind:         "BRIDGE",
		Status:       "ACTIVE",
		AccessLevel:  "AGENT_ONLY",
		Endpoint:     "mcp:bridge-gate",
		Capabilities: []string{"tool.call", "bridge.policy_enveloped", "bridge.high_risk", "bridge.operator_control_required"},
		ManifestJSON: mcpWorkspaceToolManifest(server, mcp.Tool{Name: "search_docs", Description: "Legacy high-risk bridge"}),
	}); err != nil {
		t.Fatalf("register high-risk workspace tool: %v", err)
	}
}
