package sqlite

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRSPHeavyCalibrationRun(t *testing.T) {
	store := NewTestStore(t)
	// We don't defer store.Close() because newInstrumentationInternalTestStore might not return something we can close directly or does it itself.
	// Actually Store has Close(). We can defer.
	defer store.Close()

	ctx := context.Background()
	workspaceID := "ws-calib"

	// 1. Construct fresh test environment using existing test utils
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")

	// Allow Firehose async loop to securely spin up
	time.Sleep(100 * time.Millisecond)

	t.Log("Beginning simulated heavy workload to saturate telemetry logs with motifs")

	// 2. Mock 50 cycles of agent behaviors
	for i := 0; i < 50; i++ {
		tx, err := store.BeginTxImmediate(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// Agent A: Mock Thrash_e
		store.appendRuntimeEventTx(ctx, tx, RuntimeEventInput{
			WorkspaceID: workspaceID, AgentID: "agent-a", EntityType: "FACT", EntityID: "doc-1", EventType: "memory.patch", ActorType: "AGENT", ActorID: "agent-a",
		})
		store.appendRuntimeEventTx(ctx, tx, RuntimeEventInput{
			WorkspaceID: workspaceID, AgentID: "agent-a", EntityType: "FACT", EntityID: "doc-1", EventType: "verifier.fail", ActorType: "AGENT", ActorID: "agent-a",
		})
		store.appendRuntimeEventTx(ctx, tx, RuntimeEventInput{
			WorkspaceID: workspaceID, AgentID: "agent-a", EntityType: "FACT", EntityID: "doc-1", EventType: "memory.patch", ActorType: "AGENT", ActorID: "agent-a",
		})

		// Agent B: Mock Stale hit decay to trigger continuous Persistence -> Actuator P2 flush
		store.appendRuntimeEventTx(ctx, tx, RuntimeEventInput{
			WorkspaceID: workspaceID, AgentID: "agent-b", EntityType: "AGENT", EntityID: "agent-b", EventType: "cache.hit", ActorType: "AGENT", ActorID: "agent-b",
		})
		store.appendRuntimeEventTx(ctx, tx, RuntimeEventInput{
			WorkspaceID: workspaceID, AgentID: "agent-b", EntityType: "AGENT", EntityID: "agent-b", EventType: "stale_hit", ActorType: "AGENT", ActorID: "agent-b",
		})

		tx.Commit()
	}

	t.Log("Workload concluded. Waiting for asynchronous observation layer to finalize analysis.")
	time.Sleep(2 * time.Second)

	// 3. Dump the Telemetry via API structure
	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{WorkspaceID: workspaceID, Limit: 5000})
	if err != nil {
		t.Fatal(err)
	}
	if dump.SchemaVersion != rspCalibrationSchemaVersion {
		t.Fatalf("expected calibration dump schema version %q, got %+v", rspCalibrationSchemaVersion, dump)
	}
	if dump.CalibrationContracts.Belief.Status != rspCalibrationStatusProvisional ||
		dump.CalibrationContracts.Anomaly.Status != rspCalibrationStatusShadowOnly ||
		dump.CalibrationContracts.State.Status != rspCalibrationStatusShadowOnly {
		t.Fatalf("expected dump to expose explicit versioned calibration contracts, got %+v", dump.CalibrationContracts)
	}

	// Make sure we caught our motifs
	if len(dump.AnomalyLogs) == 0 {
		t.Fatal("Expected anomaly logs to be captured by Motif windowing")
	}

	// 4. Record Calibration JSON
	b, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(filepath.Join(t.TempDir(), "calibration_dump.json"), b, 0644)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Successfully captured calibration metrics: %d beliefs, %d anomalies, %d agent states", len(dump.BeliefLogs), len(dump.AnomalyLogs), len(dump.StateLogs))
}
