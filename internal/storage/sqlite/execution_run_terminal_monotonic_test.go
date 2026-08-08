package sqlite

import (
	"context"
	"errors"
	"testing"
)

// CT-07: terminalization is monotonic. Once an execution run reaches a terminal
// status it must not be re-materialized as live by a late or out-of-order
// projection (e.g. a stray session-state sync arriving after the session ended).
func TestUpsertExecutionRunRejectsTerminalToLiveRegression(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-execution-run-terminal-monotonic"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution Run Terminal Monotonic",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	const runID = "run-terminal-monotonic"
	if _, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Title:       "Live run",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("upsert active run: %v", err)
	}

	terminal, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Title:       "Completed run",
		Status:      "COMPLETED",
	})
	if err != nil {
		t.Fatalf("upsert completed run: %v", err)
	}
	if terminal.ClosedAt == nil {
		t.Fatalf("expected terminalized run to have closed_at set, got %+v", terminal)
	}

	// A late projection that would revive the run to a live status must be rejected.
	if _, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Title:       "Stray revival",
		Status:      "ACTIVE",
	}); !errors.Is(err, ErrExecutionRunTerminalRegression) {
		t.Fatalf("expected ErrExecutionRunTerminalRegression on terminal->live revival, got %v", err)
	}

	// Likewise BLOCKED (live) over a terminal run must be rejected.
	if _, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Title:       "Stray block",
		Status:      "BLOCKED",
	}); !errors.Is(err, ErrExecutionRunTerminalRegression) {
		t.Fatalf("expected ErrExecutionRunTerminalRegression on terminal->blocked revival, got %v", err)
	}

	// The persisted run must remain terminal and closed.
	detail, err := store.GetExecutionRun(ctx, workspaceID, runID)
	if err != nil {
		t.Fatalf("get execution run: %v", err)
	}
	if detail.Run.Status != "COMPLETED" {
		t.Fatalf("expected run to remain COMPLETED after rejected revivals, got %s", detail.Run.Status)
	}
	if detail.Run.ClosedAt == nil {
		t.Fatalf("expected run to remain closed after rejected revivals, got %+v", detail.Run)
	}

	// Re-terminalizing to the same terminal status stays allowed (idempotent).
	if _, err := store.UpsertExecutionRun(ctx, ExecutionRunInput{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Title:       "Idempotent terminal",
		Status:      "COMPLETED",
	}); err != nil {
		t.Fatalf("expected idempotent terminal re-write to succeed, got %v", err)
	}
}
