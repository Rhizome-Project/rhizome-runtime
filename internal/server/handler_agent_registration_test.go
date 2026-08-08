package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentRegisterAndHeartbeatSharedTruthContract(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-agent-contract", "human", "developer")

	if err := store.CreateWorkspace(context.Background(), sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agent-contract",
		Title:       "Agent Contract",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	registerResult, rpcErr := h.agentRegister(ctx, mustJSONRaw(agentRegisterParams{
		WorkspaceID:     "ws-agent-contract",
		AgentID:         "agent-partner",
		OwnerUserID:     "developer",
		DisplayName:     "Partner Agent",
		Role:            "reviewer",
		ProtocolVersion: "partner-runtime/v2",
		Capabilities:    mustJSONRaw([]string{"analysis", "coordination", "analysis", "review"}),
		Summary:         "Registered for partner queue",
	}))
	if rpcErr != nil {
		t.Fatalf("agentRegister rpc error: %+v", rpcErr)
	}
	registerPayload, ok := registerResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected register result type %T", registerResult)
	}
	registered, ok := registerPayload["agent"].(sqlite.AgentRecord)
	if !ok {
		t.Fatalf("unexpected register payload: %+v", registerPayload)
	}
	if registered.Role != "reviewer" || registered.ProtocolVersion != "partner-runtime/v2" {
		t.Fatalf("expected register to persist role/protocol, got %+v", registered)
	}
	if got := registered.Capabilities; len(got) != 3 || got[0] != "analysis" || got[1] != "coordination" || got[2] != "review" {
		t.Fatalf("expected normalized capabilities from register, got %+v", got)
	}
	if registered.LastSeenAt != nil || registered.IsOnline {
		t.Fatalf("expected register result to remain offline until heartbeat, got %+v", registered)
	}

	listResult, rpcErr := h.workspaceAgentsList(ctx, mustJSONRaw(workspaceAgentsListParams{
		WorkspaceID: "ws-agent-contract",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceAgentsList after register rpc error: %+v", rpcErr)
	}
	listPayload, ok := listResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected list result type %T", listResult)
	}
	listed, ok := listPayload["agents"].([]sqlite.AgentRecord)
	if !ok || len(listed) != 1 {
		t.Fatalf("expected one listed agent after register, got %+v", listPayload["agents"])
	}
	if listed[0].LastSeenAt != nil || listed[0].IsOnline {
		t.Fatalf("expected list view to stay offline before heartbeat, got %+v", listed[0])
	}
	if listed[0].ProtocolVersion != registered.ProtocolVersion || listed[0].Summary != registered.Summary {
		t.Fatalf("expected list view to mirror registered truth, got %+v", listed[0])
	}

	agentCtx := testAuthContext("ws-agent-contract", "agent", "agent-partner")
	heartbeatResult, rpcErr := h.agentHeartbeat(agentCtx, mustJSONRaw(agentHeartbeatParams{
		WorkspaceID: "ws-agent-contract",
		AgentID:     "agent-partner",
		Status:      "active",
		Summary:     "Serving partner queue",
	}))
	if rpcErr != nil {
		t.Fatalf("agentHeartbeat rpc error: %+v", rpcErr)
	}
	heartbeatPayload, ok := heartbeatResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected heartbeat result type %T", heartbeatResult)
	}
	if heartbeatPayload["status"] != "ACTIVE" {
		t.Fatalf("expected heartbeat to normalize status to ACTIVE, got %+v", heartbeatPayload)
	}

	listResult, rpcErr = h.workspaceAgentsList(ctx, mustJSONRaw(workspaceAgentsListParams{
		WorkspaceID: "ws-agent-contract",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceAgentsList after heartbeat rpc error: %+v", rpcErr)
	}
	listPayload, ok = listResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected list result type after heartbeat %T", listResult)
	}
	listed, ok = listPayload["agents"].([]sqlite.AgentRecord)
	if !ok || len(listed) != 1 {
		t.Fatalf("expected one listed agent after heartbeat, got %+v", listPayload["agents"])
	}
	if listed[0].LastSeenAt == nil || !listed[0].IsOnline {
		t.Fatalf("expected heartbeat-backed online presence in list view, got %+v", listed[0])
	}
	if listed[0].Role != "reviewer" || listed[0].ProtocolVersion != "partner-runtime/v2" {
		t.Fatalf("expected heartbeat not to rewrite identity metadata, got %+v", listed[0])
	}
	if got := listed[0].Capabilities; len(got) != 3 || got[0] != "analysis" || got[1] != "coordination" || got[2] != "review" {
		t.Fatalf("expected list view to preserve capabilities after heartbeat, got %+v", got)
	}
	if listed[0].Summary != "Serving partner queue" || listed[0].Status != "ACTIVE" {
		t.Fatalf("expected list view to reflect heartbeat-updated status/summary, got %+v", listed[0])
	}
}

func TestAgentHeartbeatRejectsNonAgentOrMismatchedPrincipal(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-agent-heartbeat-principal-binding"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Heartbeat Principal Binding",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register %s: %v", agentID, err)
		}
	}

	raw := mustJSONRaw(agentHeartbeatParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		Status:      "active",
		Summary:     "spoofed heartbeat",
	})
	result, rpcErr := h.agentHeartbeat(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected mismatched heartbeat principal to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no heartbeat result on mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match agent_id" {
		t.Fatalf("unexpected heartbeat mismatch error %+v", rpcErr)
	}
	agentB, err := store.GetAgent(ctx, workspaceID, "agent-b")
	if err != nil {
		t.Fatalf("get agent-b after rejected heartbeat: %v", err)
	}
	if agentB.LastSeenAt != nil || agentB.IsOnline {
		t.Fatalf("expected rejected heartbeat not to mark agent-b online, got %+v", agentB)
	}

	humanCtx := testAuthContext(workspaceID, "human", "developer")
	result, rpcErr = h.agentHeartbeat(humanCtx, mustJSONRaw(agentHeartbeatParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		Status:      "active",
		Summary:     "human should not heartbeat as agent",
	}))
	if rpcErr == nil {
		t.Fatal("expected human heartbeat principal to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no heartbeat result for human principal, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "agent principal required" {
		t.Fatalf("unexpected human heartbeat error %+v", rpcErr)
	}
}

func TestAgentBootstrapRejectsMismatchedAgentPrincipal(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-agent-bootstrap-principal-binding"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Bootstrap Principal Binding",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register %s: %v", agentID, err)
		}
	}

	result, rpcErr := h.agentBootstrap(ctx, mustJSONRaw(agentBootstrapParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
	}))
	if rpcErr == nil {
		t.Fatal("expected mismatched bootstrap principal to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no bootstrap result on mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match agent_id" {
		t.Fatalf("unexpected bootstrap mismatch error %+v", rpcErr)
	}
}

func TestAgentDeleteRejectsMismatchedAgentPrincipal(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-agent-delete-principal-binding"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Delete Principal Binding",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register %s: %v", agentID, err)
		}
	}

	result, rpcErr := h.agentDelete(ctx, mustJSONRaw(agentDeleteParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		Actor:       "agent-a",
	}))
	if rpcErr == nil {
		t.Fatal("expected mismatched agent delete principal to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no delete result on mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match agent_id" {
		t.Fatalf("unexpected agent delete mismatch error %+v", rpcErr)
	}
	if _, err := store.GetAgent(ctx, workspaceID, "agent-b"); err != nil {
		t.Fatalf("expected rejected delete not to remove agent-b: %v", err)
	}
}

func TestAgentDeleteAllowsHumanActorBoundToPrincipal(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-agent-delete-human-binding"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Delete Human Binding",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-delete-me",
		OwnerUserID: "developer",
		DisplayName: "agent-delete-me",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	result, rpcErr := h.agentDelete(ctx, mustJSONRaw(agentDeleteParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-delete-me",
		Actor:       "developer",
	}))
	if rpcErr != nil {
		t.Fatalf("expected human-bound agent delete to pass, got %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok || payload["status"] != "DELETED" {
		t.Fatalf("unexpected delete result %+v", result)
	}
	agents, err := store.ListWorkspaceAgents(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list agents after delete: %v", err)
	}
	for _, agent := range agents {
		if agent.AgentID == "agent-delete-me" {
			t.Fatalf("expected deleted agent to disappear from workspace agent list, got %+v", agents)
		}
	}
}

func TestAgentDeleteRecordsActorBoundPromptContextEvent(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-agent-delete-evidence"
	const actorID = "developer"
	const agentID = "agent-delete-evidence"
	ctx := testAuthContext(workspaceID, "human", actorID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Delete Evidence",
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: actorID,
		DisplayName: "Agent Delete Evidence",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	result, rpcErr := h.agentDelete(ctx, mustJSONRaw(agentDeleteParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Actor:       actorID,
	}))
	if rpcErr != nil {
		t.Fatalf("agent.delete rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok || payload["status"] != "DELETED" || payload["event"] == nil {
		t.Fatalf("unexpected delete result %+v", result)
	}
	if _, err := store.GetAgent(ctx, workspaceID, agentID); err == nil {
		t.Fatal("expected deleted agent to be removed")
	}

	runtime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent.deleted",
		EntityType:  "agent",
		EntityID:    agentID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "agent.deleted"), runtime, "agent.deleted")
	assertServerRuntimeEventAuthorityMetadata(t, runtime, authority)
	eventPayload := decodeEventPayloadMap(t, runtime.PayloadJSON)
	assertAgentLifecyclePromptContext(t, eventPayload, "agent.delete", workspaceID, "human", actorID, agentID, actorID)
	if got := eventPayload["display_name"]; got != "Agent Delete Evidence" {
		t.Fatalf("agent.deleted display_name = %+v", got)
	}
}

func TestAgentRegisterPreservesMetadataOnPartialReregister(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-agent-register-reregister", "human", "developer")

	if err := store.CreateWorkspace(context.Background(), sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-agent-register-reregister",
		Title:       "Agent Register Reregister",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	registerResult, rpcErr := h.agentRegister(ctx, mustJSONRaw(agentRegisterParams{
		WorkspaceID:     "ws-agent-register-reregister",
		AgentID:         "agent-reregister",
		OwnerUserID:     "developer",
		DisplayName:     "Registered Partner",
		Role:            "reviewer",
		ProtocolVersion: "partner-runtime/v5",
		Capabilities:    mustJSONRaw([]string{"analysis", "tool.call"}),
		Summary:         "registered summary",
	}))
	if rpcErr != nil {
		t.Fatalf("initial agentRegister rpc error: %+v", rpcErr)
	}
	registerPayload, ok := registerResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected initial register result type %T", registerResult)
	}
	initial, ok := registerPayload["agent"].(sqlite.AgentRecord)
	if !ok {
		t.Fatalf("unexpected initial register payload: %+v", registerPayload)
	}
	if initial.DisplayName != "Registered Partner" || initial.Role != "reviewer" {
		t.Fatalf("expected initial register to persist metadata, got %+v", initial)
	}

	if err := store.RecordAgentHeartbeat(context.Background(), sqlite.AgentHeartbeatInput{
		WorkspaceID: "ws-agent-register-reregister",
		AgentID:     "agent-reregister",
		Status:      "active",
		Summary:     "live summary",
	}); err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}

	partialResult, rpcErr := h.agentRegister(ctx, mustJSONRaw(agentRegisterParams{
		WorkspaceID: "ws-agent-register-reregister",
		AgentID:     "agent-reregister",
	}))
	if rpcErr != nil {
		t.Fatalf("partial agentRegister rpc error: %+v", rpcErr)
	}
	partialPayload, ok := partialResult.(map[string]any)
	if !ok {
		t.Fatalf("unexpected partial register result type %T", partialResult)
	}
	after, ok := partialPayload["agent"].(sqlite.AgentRecord)
	if !ok {
		t.Fatalf("unexpected partial register payload: %+v", partialPayload)
	}
	if after.OwnerUserID != "developer" || after.DisplayName != "Registered Partner" {
		t.Fatalf("expected partial re-register to preserve owner/display, got %+v", after)
	}
	if after.Role != "reviewer" || after.ProtocolVersion != "partner-runtime/v5" {
		t.Fatalf("expected partial re-register to preserve role/protocol, got %+v", after)
	}
	if len(after.Capabilities) != 2 || after.Capabilities[0] != "analysis" || after.Capabilities[1] != "tool.call" {
		t.Fatalf("expected partial re-register to preserve capabilities, got %+v", after)
	}
	if after.Summary != "live summary" || !after.IsOnline || after.LastSeenAt == nil {
		t.Fatalf("expected partial re-register to preserve live summary and liveness, got %+v", after)
	}
}

func assertAgentLifecyclePromptContext(t *testing.T, payload map[string]any, surface, workspaceID, principalType, principalID, agentID, actorID string) {
	t.Helper()
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected agent lifecycle prompt_context_envelope in payload, got %+v", payload)
	}
	for key, want := range map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_agent_lifecycle_write",
		"surface":                            surface,
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"agent_id":                           agentID,
		"actor_id":                           actorID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	} {
		if got, ok := envelope[key].(string); !ok || got != want {
			t.Fatalf("prompt_context_envelope[%s] = %v, want %q in %+v", key, envelope[key], want, envelope)
		}
	}
}
