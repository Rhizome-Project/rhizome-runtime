package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestRuntimeEventFirehoseOutboxDoesNotDispatchBeforeCommit(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	if store.rspFirehose != nil {
		store.rspFirehose.Stop()
	}

	const workspaceID = "ws-firehose-outbox-commit-only"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Firehose Outbox Commit Only",
		CreatedBy:   "test",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := store.appendRuntimeEventTx(ctx, tx, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "runtime.firehose.rollback_probe",
		EntityType:  "runtime_probe",
		EntityID:    "rollback-probe",
		ActorType:   "system",
		ActorID:     "test",
		PayloadJSON: "{}",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("append runtime event in tx: %v", err)
	}

	select {
	case event := <-store.rspFirehose.eventCh:
		_ = tx.Rollback()
		t.Fatalf("runtime event was dispatched to firehose before commit: %+v", event)
	default:
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}
	if got := runtimeEventFirehosePendingOutboxCount(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("rolled-back runtime event left firehose outbox rows: %d", got)
	}
}

func TestRuntimeEventFirehoseOutboxDrainsCommittedEvents(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	if store.rspFirehose != nil {
		store.rspFirehose.Stop()
	}

	const workspaceID = "ws-firehose-outbox-drains"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Firehose Outbox Drains",
		CreatedBy:   "test",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := store.appendRuntimeEventTx(ctx, tx, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "runtime.firehose.commit_probe",
		EntityType:  "runtime_probe",
		EntityID:    "commit-probe",
		ActorType:   "system",
		ActorID:     "test",
		PayloadJSON: "{}",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("append runtime event in tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	if got := runtimeEventFirehosePendingOutboxCount(t, ctx, store, workspaceID); got != 1 {
		t.Fatalf("expected one pending committed firehose outbox row, got %d", got)
	}
	store.rspFirehose.drainCommittedOutbox(ctx)
	if got := runtimeEventFirehosePendingOutboxCount(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected committed firehose outbox row to drain, got %d", got)
	}
}

func runtimeEventFirehosePendingOutboxCount(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(1)
		   FROM runtime_event_firehose_outbox
		  WHERE workspace_id = ? AND dispatched_at IS NULL`,
		workspaceID,
	).Scan(&count); err != nil {
		t.Fatalf("count firehose outbox rows: %v", err)
	}
	return count
}
