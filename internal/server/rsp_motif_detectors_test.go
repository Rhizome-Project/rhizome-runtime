package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
)

func assertInternalEphemeralMotifEvent(t *testing.T, msg EventMessage, wantType, wantWorkspaceID, wantAgentID string) map[string]any {
	t.Helper()

	if msg.Type != wantType {
		t.Fatalf("expected live event type %q, got %+v", wantType, msg)
	}
	if msg.WorkspaceID != wantWorkspaceID || msg.AgentID != wantAgentID {
		t.Fatalf("expected workspace/agent %s/%s, got %+v", wantWorkspaceID, wantAgentID, msg)
	}
	if msg.EventID != "" || msg.IngestSeq != 0 || msg.DedupKey != "" ||
		msg.EntityType != "" || msg.EntityID != "" ||
		msg.RootCauseID != "" || msg.ProvenanceGroupID != "" || msg.ParentRefsJSON != "" {
		t.Fatalf("expected motif signal to stay internal-ephemeral without canonical envelope, got %+v", msg)
	}
	assertValidEventTimestamp(t, msg.Timestamp)
	return decodeEventPayloadMap(t, msg.PayloadJSON)
}

func assertNoWorkspaceRuntimeEventsOfType(t *testing.T, store *sqlite.Store, workspaceID, eventType string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(context.Background(), sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events for %s: %v", eventType, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected %s to stay off runtime journal, got %+v", eventType, events)
	}
}

func TestRSPMotif_Thrash(t *testing.T) {
	dbPath := fmt.Sprintf("file:TestRSPMotif_Thrash_%d?mode=memory&cache=shared", time.Now().UnixNano())
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	if err := store.ApplyMigrations(context.Background()); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}
	defer store.Close()

	eb := NewEventBus()
	md := NewMotifDetectors(store, eb)
	eb.RegisterMiddleware(md.Middleware)

	workspaceID := "ws-thrash"
	store.CreateWorkspace(context.Background(), sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Thrash WS",
		CreatedBy:   "motif",
	})
	claimServerTestWorkspaceAuthority(t, context.Background(), store, workspaceID)
	store.RegisterAgent(context.Background(), sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-1",
		OwnerUserID: "motif",
		DisplayName: "Agent Thrash",
	})

	// Create tension-1 to satisfy foreign key constraints for workspace_tension_exclusions
	_, err = store.DB().ExecContext(context.Background(),
		"INSERT INTO workspace_tensions(tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, created_at, updated_at) VALUES (?, ?, 'cluster1', 'ANOMALY', 'ACTIVE', 'PENDING', ?, ?)",
		"tension-1", workspaceID, time.Now().Format(time.RFC3339), time.Now().Format(time.RFC3339))
	if err != nil {
		t.Fatalf("failed to insert test tension: %v", err)
	}

	// Subscribe to verify anomalous events are emitted
	ch := eb.Subscribe(workspaceID)

	// Simulate 3 failures
	for i := 0; i < 3; i++ {
		eb.Publish(EventMessage{
			Type:        "tension.lifecycle.update",
			WorkspaceID: workspaceID,
			AgentID:     "agent-1",
			PayloadJSON: `{"lifecycle_state":"FAILED", "tension_id":"tension-1"}`,
		})
	}

	// Need to check if anom.thrash_e was published
	timeout := time.After(1 * time.Second)
	thrashEvents := 0
	flushInstructions := 0
	var thrashSignal EventMessage
	var flushSignal EventMessage
loop:
	for {
		select {
		case msg := <-ch:
			if msg.Type == motifThrashEphemeralEventType {
				thrashEvents++
				if thrashSignal.Type == "" {
					thrashSignal = msg
				}
			} else if msg.Type == motifInstructionEphemeralEventType && msg.PayloadJSON == `{"instruction":"FLUSH_P1_CACHE"}` {
				flushInstructions++
				if flushSignal.Type == "" {
					flushSignal = msg
				}
			}
		case <-timeout:
			break loop
		}
	}

	assert.True(t, thrashEvents >= 1, "Expected at least 1 internal-ephemeral thrash event")
	assert.True(t, flushInstructions >= 1, "Expected FLUSH_P1_CACHE instruction")
	thrashPayload := assertInternalEphemeralMotifEvent(t, thrashSignal, motifThrashEphemeralEventType, workspaceID, "agent-1")
	if thrashPayload["tension_id"] != "tension-1" || thrashPayload["reason"] != "N>=3 verifier fails in 10m" {
		t.Fatalf("unexpected thrash payload %+v", thrashPayload)
	}
	flushPayload := assertInternalEphemeralMotifEvent(t, flushSignal, motifInstructionEphemeralEventType, workspaceID, "agent-1")
	if flushPayload["instruction"] != "FLUSH_P1_CACHE" {
		t.Fatalf("unexpected flush instruction payload %+v", flushPayload)
	}

	// Verify DB Exclusion is present
	exclusions, err := store.GetAgentTensionExclusions(context.Background(), workspaceID, "agent-1")
	assert.NoError(t, err)
	assert.Contains(t, exclusions, "tension-1", "Expected tension-1 to be excluded for agent-1")

	runtimeEvents, err := store.ListRuntimeEvents(context.Background(), sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "control.command.requested",
		Limit:       10,
	})
	assert.NoError(t, err)
	assert.Len(t, runtimeEvents, 1, "motif middleware should persist one canonical control command request under authority fence")
	assert.Equal(t, "control.command.requested", runtimeEvents[0].EventType)
	assertNoWorkspaceRuntimeEventsOfType(t, store, workspaceID, "ANOMALY_ALERT")
	assertNoWorkspaceRuntimeEventsOfType(t, store, workspaceID, motifThrashEphemeralEventType)
	assertNoWorkspaceRuntimeEventsOfType(t, store, workspaceID, motifInstructionEphemeralEventType)
}

func TestRSPMotif_Bounce(t *testing.T) {
	dbPath := fmt.Sprintf("file:TestRSPMotif_Bounce_%d?mode=memory&cache=shared", time.Now().UnixNano())
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	if err := store.ApplyMigrations(context.Background()); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}
	defer store.Close()

	eb := NewEventBus()
	md := NewMotifDetectors(store, eb)
	eb.RegisterMiddleware(md.Middleware)

	workspaceID := "ws-bounce"
	ch := eb.Subscribe(workspaceID)

	eb.Publish(EventMessage{
		Type:        "workspace.artifact.write",
		WorkspaceID: workspaceID,
		AgentID:     "agent-1",
		PayloadJSON: `{"artifact_ref":"file.go"}`,
	})

	eb.Publish(EventMessage{
		Type:        "workspace.artifact.write",
		WorkspaceID: workspaceID,
		AgentID:     "agent-2",
		PayloadJSON: `{"artifact_ref":"file.go"}`,
	})

	timeout := time.After(1 * time.Second)
	bounceEvents := 0
	metaTensions := 0
	var bounceSignal EventMessage
	var metaSignal EventMessage
loop:
	for {
		select {
		case msg := <-ch:
			if msg.Type == motifBounceEphemeralEventType {
				bounceEvents++
				if bounceSignal.Type == "" {
					bounceSignal = msg
				}
			} else if msg.Type == motifMetaTensionEphemeralEventType {
				metaTensions++
				if metaSignal.Type == "" {
					metaSignal = msg
				}
			}
		case <-timeout:
			break loop
		}
	}

	assert.True(t, bounceEvents >= 1, "Expected at least 1 internal-ephemeral bounce event")
	assert.True(t, metaTensions >= 1, "Expected at least 1 DISSENT meta tension request")
	bouncePayload := assertInternalEphemeralMotifEvent(t, bounceSignal, motifBounceEphemeralEventType, workspaceID, "agent-2")
	if bouncePayload["artifact_ref"] != "file.go" || bouncePayload["conflicting_agent"] != "agent-1" {
		t.Fatalf("unexpected bounce payload %+v", bouncePayload)
	}
	metaPayload := assertInternalEphemeralMotifEvent(t, metaSignal, motifMetaTensionEphemeralEventType, workspaceID, "system:motif")
	agents, ok := metaPayload["agents"].([]any)
	if !ok || len(agents) != 2 {
		t.Fatalf("expected two agents in meta-tension payload, got %+v", metaPayload)
	}
	if metaPayload["type"] != "DISSENT" || metaPayload["target_artifact"] != "file.go" || metaPayload["reason"] != "Bounce anomaly" {
		t.Fatalf("unexpected meta-tension payload %+v", metaPayload)
	}

	runtimeEvents, err := store.ListRuntimeEvents(context.Background(), sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	assert.NoError(t, err)
	assert.Len(t, runtimeEvents, 0, "motif middleware must stay internal-ephemeral; persisted anomaly contract is ANOMALY_ALERT elsewhere")
	assertNoWorkspaceRuntimeEventsOfType(t, store, workspaceID, "ANOMALY_ALERT")
	assertNoWorkspaceRuntimeEventsOfType(t, store, workspaceID, motifBounceEphemeralEventType)
	assertNoWorkspaceRuntimeEventsOfType(t, store, workspaceID, motifMetaTensionEphemeralEventType)
}
