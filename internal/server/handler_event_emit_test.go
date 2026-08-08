package server

import (
	"encoding/json"
	"testing"
)

func TestEventEmitPublishesEphemeralWorkspaceEvent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-event-emit"
	ctx := testAuthContext(workspaceID, "agent", "agent-emit")
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(eventEmitParams{
		Type:        "ephemeral.debug.note",
		WorkspaceID: workspaceID,
		Summary:     "Ephemeral debug note",
		PayloadJSON: `{"ok":true}`,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.eventEmit(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("eventEmit rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if payload["status"] != "EMITTED" || payload["workspace_id"] != workspaceID {
		t.Fatalf("unexpected eventEmit response %+v", payload)
	}

	evt := nextEvent(t, ch)
	if evt.Type != "ephemeral.debug.note" {
		t.Fatalf("unexpected event type %+v", evt)
	}
	if evt.WorkspaceID != workspaceID || evt.AgentID != "agent-emit" || evt.Summary != "Ephemeral debug note" || evt.PayloadJSON != `{"ok":true}` {
		t.Fatalf("unexpected emitted event %+v", evt)
	}
	if evt.EventID != "" || evt.IngestSeq != 0 || evt.EntityType != "" || evt.EntityID != "" {
		t.Fatalf("ephemeral event should not impersonate runtime-journal envelope %+v", evt)
	}
	assertValidEventTimestamp(t, evt.Timestamp)
}

func TestEventEmitRejectsCanonicalRuntimeNamespace(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	raw, err := json.Marshal(eventEmitParams{
		Type:        "workspace.memory.recorded",
		WorkspaceID: "ws-event-emit-reject",
		Summary:     "Should fail",
		PayloadJSON: `{}`,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.eventEmit(testAuthContext("ws-event-emit-reject", "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected rpc error")
	}
	if rpcErr.Code != errCodeInvalidParams || rpcErr.Message != "event.emit only supports ephemeral.* event types; canonical runtime events must use dedicated handlers" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestEventEmitRejectsWorkspaceIsolationAndBadPayload(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	raw, err := json.Marshal(eventEmitParams{
		Type:        "ephemeral.debug.note",
		WorkspaceID: "ws-event-emit-bad",
		Summary:     "Should fail",
		PayloadJSON: "{not-json",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	_, rpcErr := h.eventEmit(testAuthContext("ws-other", "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected rpc error")
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected workspace rpc error %+v", rpcErr)
	}

	_, rpcErr = h.eventEmit(testAuthContext("ws-event-emit-bad", "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected payload rpc error")
	}
	if rpcErr.Code != errCodeInvalidParams || rpcErr.Message != "payload_json must be valid JSON" {
		t.Fatalf("unexpected payload rpc error %+v", rpcErr)
	}
}
