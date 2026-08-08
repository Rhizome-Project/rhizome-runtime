package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentMessageSendRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-message-send-missing-authority-rpc"
	seedServerMessagingWorkspaceAndAgents(t, ctx, store, workspaceID, false, "agent-a", "agent-b")
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "should fail closed without workspace authority",
	})
	if err != nil {
		t.Fatalf("marshal agent.message.send params: %v", err)
	}

	result, rpcErr := h.agentMessageSend(testAuthContext(workspaceID, "agent", "agent-a"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "agent.message.send")

	if got := countServerMessagingRows(t, ctx, store, workspaceID, "agent_messages"); got != 0 {
		t.Fatalf("expected no agent_messages rows after authority reject, got %d", got)
	}
	if got := countServerMessagingRuntimeEvents(t, ctx, store, workspaceID, "agent_message.sent", "agent_message", ""); got != 0 {
		t.Fatalf("expected no agent_message.sent events after authority reject, got %d", got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestAgentMessageSendActorMismatchDoesNotAppendRuntimeEnvelopeEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-message-send-actor-mismatch"
	seedServerMessagingWorkspaceAndAgents(t, ctx, store, workspaceID, true, "agent-a", "agent-b")

	raw, err := json.Marshal(agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "mismatch should not create convincing runtime event",
	})
	if err != nil {
		t.Fatalf("marshal agent.message.send params: %v", err)
	}

	result, rpcErr := h.agentMessageSend(testAuthContext(workspaceID, "agent", "agent-b"), raw)
	if rpcErr == nil {
		t.Fatal("expected actor mismatch reject")
	}
	if result != nil {
		t.Fatalf("expected no result on actor mismatch, got %+v", result)
	}
	if got := countServerMessagingRows(t, ctx, store, workspaceID, "agent_messages"); got != 0 {
		t.Fatalf("expected no agent_messages rows after actor mismatch, got %d", got)
	}
	if got := countServerMessagingRuntimeEvents(t, ctx, store, workspaceID, "agent_message.sent", "agent_message", ""); got != 0 {
		t.Fatalf("expected no agent_message.sent events after actor mismatch, got %d", got)
	}
}

func TestAgentRequestRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-agent-request-missing-authority-rpc"
	seedServerMessagingWorkspaceAndAgents(t, ctx, store, workspaceID, false, "agent-a", "agent-b")
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(agentRequestParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"kind":"missing-authority"}`,
	})
	if err != nil {
		t.Fatalf("marshal agent.request params: %v", err)
	}

	result, rpcErr := h.agentRequest(testAuthContext(workspaceID, "agent", "agent-a"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "agent.request")

	if got := countServerMessagingRows(t, ctx, store, workspaceID, "agent_requests"); got != 0 {
		t.Fatalf("expected no agent_requests rows after authority reject, got %d", got)
	}
	if got := countServerMessagingRuntimeEvents(t, ctx, store, workspaceID, "agent_request.sent", "agent_request", ""); got != 0 {
		t.Fatalf("expected no agent_request.sent events after authority reject, got %d", got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestAgentRequestActorMismatchDoesNotAppendRuntimeEnvelopeEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-agent-request-actor-mismatch"
	seedServerMessagingWorkspaceAndAgents(t, ctx, store, workspaceID, true, "agent-a", "agent-b")

	raw, err := json.Marshal(agentRequestParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"kind":"actor-mismatch"}`,
	})
	if err != nil {
		t.Fatalf("marshal agent.request params: %v", err)
	}

	result, rpcErr := h.agentRequest(testAuthContext(workspaceID, "agent", "agent-b"), raw)
	if rpcErr == nil {
		t.Fatal("expected actor mismatch reject")
	}
	if result != nil {
		t.Fatalf("expected no result on actor mismatch, got %+v", result)
	}
	if got := countServerMessagingRows(t, ctx, store, workspaceID, "agent_requests"); got != 0 {
		t.Fatalf("expected no agent_requests rows after actor mismatch, got %d", got)
	}
	if got := countServerMessagingRuntimeEvents(t, ctx, store, workspaceID, "agent_request.sent", "agent_request", ""); got != 0 {
		t.Fatalf("expected no agent_request.sent events after actor mismatch, got %d", got)
	}
}

func TestAgentRequestListRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-agent-request-list-missing-authority-rpc"
	seedServerMessagingWorkspaceAndAgents(t, ctx, store, workspaceID, false, "agent-a", "agent-b")
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(agentRequestListParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
	})
	if err != nil {
		t.Fatalf("marshal agent.request.list params: %v", err)
	}

	result, rpcErr := h.agentRequestList(testAuthContext(workspaceID, "agent", "agent-b"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "agent.request.list")
	if got := countServerMessagingRuntimeEvents(t, ctx, store, workspaceID, "agent_request.claimed", "agent_request", ""); got != 0 {
		t.Fatalf("expected no agent_request.claimed events after authority reject, got %d", got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestAgentRespondRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-agent-respond-stale-authority-rpc"
	seedServerMessagingWorkspaceAndAgents(t, ctx, store, workspaceID, true, "agent-a", "agent-b")
	current, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get current workspace authority: %v", err)
	}
	requestID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"kind":"stale-authority"}`,
	})
	if err != nil {
		t.Fatalf("seed agent request: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-2702")

	raw, err := json.Marshal(agentRespondParams{
		RequestID: requestID,
		Response:  `{"status":"blocked"}`,
	})
	if err != nil {
		t.Fatalf("marshal agent.respond params: %v", err)
	}

	result, rpcErr := h.agentRespond(testAuthContext(workspaceID, "agent", "agent-b"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "agent.respond")

	record, err := store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("reload request after authority reject: %v", err)
	}
	if record.Status != "PENDING" || record.Response != "" || record.RespondedAt != "" {
		t.Fatalf("expected request to remain pending after stale-authority reject, got %+v", record)
	}
	if got := countServerMessagingRuntimeEvents(t, ctx, store, workspaceID, "agent_response.recorded", "agent_request", requestID); got != 0 {
		t.Fatalf("expected no agent_response.recorded events after stale-authority reject, got %d", got)
	}
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestAgentRespondActorMismatchDoesNotAppendRuntimeEnvelopeEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-agent-respond-actor-mismatch"
	seedServerMessagingWorkspaceAndAgents(t, ctx, store, workspaceID, true, "agent-a", "agent-b")

	requestID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"kind":"actor-mismatch"}`,
	})
	if err != nil {
		t.Fatalf("seed agent request: %v", err)
	}
	raw, err := json.Marshal(agentRespondParams{
		RequestID: requestID,
		Response:  `{"status":"wrong-actor"}`,
	})
	if err != nil {
		t.Fatalf("marshal agent.respond params: %v", err)
	}

	result, rpcErr := h.agentRespond(testAuthContext(workspaceID, "agent", "agent-a"), raw)
	if rpcErr == nil {
		t.Fatal("expected actor mismatch reject")
	}
	if result != nil {
		t.Fatalf("expected no result on actor mismatch, got %+v", result)
	}
	record, err := store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("reload request after actor mismatch: %v", err)
	}
	if record.Status != "PENDING" || record.Response != "" || record.RespondedAt != "" {
		t.Fatalf("expected request to remain pending after actor mismatch, got %+v", record)
	}
	if got := countServerMessagingRuntimeEvents(t, ctx, store, workspaceID, "agent_response.recorded", "agent_request", requestID); got != 0 {
		t.Fatalf("expected no agent_response.recorded events after actor mismatch, got %d", got)
	}
}

func TestAgentMessageSendPersistsAuthorityMetadataOnRuntimeEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-message-send-authority-metadata-rpc"
	seedServerMessagingWorkspaceAndAgents(t, ctx, store, workspaceID, true, "agent-a", "agent-b")
	authority, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get workspace authority: %v", err)
	}

	raw, err := json.Marshal(agentMessageSendParams{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "public messaging should stamp authority metadata",
	})
	if err != nil {
		t.Fatalf("marshal agent.message.send params: %v", err)
	}

	result, rpcErr := h.agentMessageSend(testAuthContext(workspaceID, "agent", "agent-a"), raw)
	if rpcErr != nil {
		t.Fatalf("agent.message.send returned rpc error: %+v", rpcErr)
	}
	response, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected message send result map, got %+v", result)
	}
	messageID, _ := response["message_id"].(string)
	if messageID == "" {
		t.Fatalf("expected message_id in response, got %+v", result)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    messageID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent_message.sent events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one agent_message.sent event, got %d", len(events))
	}
	assertServerRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func seedServerMessagingWorkspaceAndAgents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, claimAuthority bool, agentIDs ...string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "tests",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s in %s: %v", agentID, workspaceID, err)
		}
	}
	if claimAuthority {
		claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	}
}

func countServerMessagingRows(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, table string) int {
	t.Helper()

	var count int
	query := "SELECT COUNT(*) FROM " + table + " WHERE workspace_id = ?"
	if err := store.DB().QueryRowContext(ctx, query, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count %s rows for %s: %v", table, workspaceID, err)
	}
	return count
}

func countServerMessagingRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, eventType, entityType, entityID string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  entityType,
		EntityID:    entityID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events for %s/%s: %v", eventType, entityType, entityID, err)
	}
	return len(events)
}
