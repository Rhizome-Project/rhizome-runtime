package sqlite_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRecordRuntimeEventWithAuthorityCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-runtime-event-with-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Runtime Event With Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	event, err := store.RecordRuntimeEventWithAuthority(ctx, authority, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "runtime.authority_probe",
		EntityType:  "runtime_probe",
		EntityID:    "probe-d2a-authority-helper",
		ActorType:   "agent",
		ActorID:     "agent-a",
		PayloadJSON: `{"probe":"authority_metadata"}`,
	})
	if err != nil {
		t.Fatalf("record runtime event with authority: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "runtime.authority_probe",
		EntityType:  "runtime_probe",
		EntityID:    "probe-d2a-authority-helper",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one runtime event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestRecordRuntimeEventWithAuthorityRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-runtime-event-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Runtime Event Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-3013")

	_, err := store.RecordRuntimeEventWithAuthority(ctx, current, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "runtime.authority_probe",
		EntityType:  "runtime_probe",
		EntityID:    "probe-d2a-stale-authority-helper",
		ActorType:   "agent",
		ActorID:     "agent-a",
		PayloadJSON: `{"probe":"stale_authority"}`,
	})
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", reject)
	}

	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "runtime.authority_probe",
		EntityType:  "runtime_probe",
		EntityID:    "probe-d2a-stale-authority-helper",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no runtime event after stale-authority reject, got %d", got)
	}
	assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectStale)
}

func TestRecordRuntimeEventRejectsToolCallWithoutAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tool-call-runtime-requires-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Call Runtime Requires Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	payloadJSON := toolCallRuntimeEventPayloadJSONForTest(
		t,
		workspaceID,
		"tool-requires-authority",
		"tool.call.executed",
		"agent",
		"agent-a",
		"tool.call",
		"operation-requires-authority",
		map[string]any{"exit_code": 0, "timed_out": false},
	)
	_, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    "tool-requires-authority",
		ActorType:   "agent",
		ActorID:     "agent-a",
		PayloadJSON: payloadJSON,
	})
	if err == nil {
		t.Fatal("expected raw tool.call runtime event without authority metadata to fail")
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    "tool-requires-authority",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no raw tool.call runtime event after validation failure, got %d", got)
	}
}

func TestRecordRuntimeEventRejectsRawToolCallWithManualAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tool-call-runtime-rejects-manual-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Call Runtime Rejects Manual Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	_, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID:                    workspaceID,
		EventType:                      "tool.call.executed",
		EntityType:                     "tool",
		EntityID:                       "tool-manual-authority",
		ActorType:                      "agent",
		ActorID:                        "agent-a",
		AuthorityHolderNodeID:          "authnode-manual",
		AuthorityTerm:                  1,
		AuthorityLeaseTokenFingerprint: "manual-fingerprint",
		PayloadJSON: toolCallRuntimeEventPayloadJSONForTest(
			t,
			workspaceID,
			"tool-manual-authority",
			"tool.call.executed",
			"agent",
			"agent-a",
			"tool.call",
			"operation-manual-authority",
			map[string]any{"exit_code": 0, "timed_out": false},
		),
	})
	if err == nil {
		t.Fatal("expected raw tool.call runtime event with manual authority metadata to fail")
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    "tool-manual-authority",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no raw tool.call runtime event after manual authority validation failure, got %d", got)
	}
}

func TestRecordRuntimeEventWithAuthorityRejectsToolCallWithoutPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tool-call-runtime-requires-envelope"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Call Runtime Requires Envelope",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	_, err := store.RecordRuntimeEventWithAuthority(ctx, authority, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.denied",
		EntityType:  "tool",
		EntityID:    "tool-requires-envelope",
		ActorType:   "agent",
		ActorID:     "agent-a",
		PayloadJSON: `{"requested_capability":"tool.call"}`,
	})
	if err == nil {
		t.Fatal("expected authority-backed tool.call runtime event without prompt_context_envelope to fail")
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.denied",
		EntityType:  "tool",
		EntityID:    "tool-requires-envelope",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no raw tool.call runtime event after envelope validation failure, got %d", got)
	}
}

func TestRecordRuntimeEventWithAuthorityRejectsForgedToolCallPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tool-call-runtime-rejects-forged-envelope"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Call Runtime Rejects Forged Envelope",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	payloadJSON := toolCallRuntimeEventPayloadJSONForTest(
		t,
		workspaceID,
		"tool-forged-envelope",
		"tool.call.denied",
		"agent",
		"agent-a",
		"tool.call",
		"",
		map[string]any{},
	)
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode generated payload: %v", err)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("generated payload missing prompt_context_envelope: %+v", payload)
	}
	envelope["tool_id"] = "tool-other"
	forged, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal forged payload: %v", err)
	}
	_, err = store.RecordRuntimeEventWithAuthority(ctx, authority, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.denied",
		EntityType:  "tool",
		EntityID:    "tool-forged-envelope",
		ActorType:   "agent",
		ActorID:     "agent-a",
		PayloadJSON: string(forged),
	})
	if err == nil {
		t.Fatal("expected forged tool.call prompt_context_envelope to fail")
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.denied",
		EntityType:  "tool",
		EntityID:    "tool-forged-envelope",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no raw tool.call runtime event after forged envelope validation failure, got %d", got)
	}
}

func TestRecordRuntimeEventWithAuthorityRejectsExecutedToolCallWithoutOperationID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tool-call-runtime-requires-operation-id"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Call Runtime Requires Operation ID",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	_, err := store.RecordRuntimeEventWithAuthority(ctx, authority, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    "tool-missing-operation",
		ActorType:   "agent",
		ActorID:     "agent-a",
		PayloadJSON: toolCallRuntimeEventPayloadJSONForTest(
			t,
			workspaceID,
			"tool-missing-operation",
			"tool.call.executed",
			"agent",
			"agent-a",
			"tool.call",
			"",
			map[string]any{"exit_code": 0, "timed_out": false},
		),
	})
	if err == nil {
		t.Fatal("expected executed tool.call runtime event without operation_id to fail")
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    "tool-missing-operation",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no executed tool.call runtime event without operation_id, got %d", got)
	}
}

func TestRecordRuntimeEventWithAuthorityRejectsToolCallWithoutActor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tool-call-runtime-requires-actor"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Call Runtime Requires Actor",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	tests := []struct {
		eventType   string
		toolID      string
		operationID string
	}{
		{eventType: "tool.call.denied", toolID: "tool-missing-denied-actor"},
		{eventType: "tool.call.executed", toolID: "tool-missing-executed-actor", operationID: "operation-missing-actor"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.eventType, func(t *testing.T) {
			_, err := store.RecordRuntimeEventWithAuthority(ctx, authority, sqlite.RuntimeEventInput{
				WorkspaceID: workspaceID,
				EventType:   tt.eventType,
				EntityType:  "tool",
				EntityID:    tt.toolID,
				PayloadJSON: toolCallRuntimeEventPayloadJSONForTest(
					t,
					workspaceID,
					tt.toolID,
					tt.eventType,
					"",
					"",
					"tool.call",
					tt.operationID,
					map[string]any{"exit_code": 0, "timed_out": false},
				),
			})
			if err == nil {
				t.Fatalf("expected %s runtime event without actor to fail", tt.eventType)
			}
			if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   tt.eventType,
				EntityType:  "tool",
				EntityID:    tt.toolID,
				Limit:       10,
			}); got != 0 {
				t.Fatalf("expected no %s runtime event without actor, got %d", tt.eventType, got)
			}
		})
	}
}

func TestRecordRuntimeEventWithAuthorityAcceptsBoundToolCallPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-tool-call-runtime-accepts-bound-envelope"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Call Runtime Accepts Bound Envelope",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	event, err := store.RecordRuntimeEventWithAuthority(ctx, authority, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    "tool-bound-envelope",
		ActorType:   "agent",
		ActorID:     "agent-a",
		PayloadJSON: toolCallRuntimeEventPayloadJSONForTest(
			t,
			workspaceID,
			"tool-bound-envelope",
			"tool.call.executed",
			"agent",
			"agent-a",
			"tool.call",
			"operation-bound-envelope",
			map[string]any{"exit_code": 0, "timed_out": false},
		),
	})
	if err != nil {
		t.Fatalf("record authority-backed tool.call runtime event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, event, authority)
}

func TestRecordRuntimeEventAcceptsNonToolEventWithoutPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-runtime-event-raw-non-tool-still-accepted"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Raw Non Tool Runtime Event Still Accepted",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	event, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "bridge.signal",
		EntityType:  "bridge",
		EntityID:    "bridge-a",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"signal":"ok"}`,
	})
	if err != nil {
		t.Fatalf("record non-tool runtime event without prompt context envelope: %v", err)
	}
	if event.EventType != "bridge.signal" || event.PayloadJSON == "" {
		t.Fatalf("unexpected non-tool runtime event: %+v", event)
	}
}
