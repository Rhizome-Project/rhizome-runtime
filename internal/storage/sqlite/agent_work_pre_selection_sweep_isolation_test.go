package sqlite_test

import (
	"context"
	"errors"
	"testing"

	sqlite "github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// TestAgentWorkNextSurvivesPreSelectionSweepFailure locks the A1 invariant (P5): a pre-selection
// liveness sweep that FAILS (the c2 root was the receipt sweep's side-effect-resolution query
// exceeding the RPC deadline) must NOT wedge work.next - candidate selection must still reach and
// surface a claimable task. A deterministic fault is injected into the receipt sweep; the fix (P1)
// swallows+journals it and proceeds. BEFORE the fix - when the receipt sweep error returned wholesale
// from GetAgentWorkNext - this test fails with that error (the c2 integration stall, reproduced as a
// unit invariant; verified by reverting the swallow to a return).
func TestAgentWorkNextSurvivesPreSelectionSweepFailure(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-sweep-isolation", []string{"agent-a"})
	createAgentWorkTask(t, ctx, store, "ws-sweep-isolation", "task-claimable", "normal")

	// Deterministically fail the receipt-terminalization sweep, exactly the c2 abort shape.
	restore := sqlite.SetAgentWorkRequiredReceiptSweepFaultForTest(func() error {
		return errors.New("query side-effect resolution decision receipt: context deadline exceeded")
	})
	defer restore()

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: "ws-sweep-isolation",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("GetAgentWorkNext must survive a pre-selection sweep failure (A1), got err: %v", err)
	}
	if !result.HasWork {
		t.Fatalf("expected work surfaced despite a sweep failure, got HasWork=false (result=%+v)", result)
	}
	if result.Task == nil || result.Task.TaskID != "task-claimable" {
		t.Fatalf("expected task-claimable surfaced despite the sweep failure, got %+v", result.Task)
	}
}
