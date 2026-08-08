package sqlite

import (
	"context"
	"testing"
)

func TestLoadOperatorQueueItemByKeyTxReturnsLiveBaseVersion(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-live-base-version"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Live Base Version",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, err := store.UpsertOperatorQueueItem(ctx, OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:live-base-version",
		QueueType:   "FOLLOW_UP",
		Title:       "Live base version queue",
		Summary:     "initial summary",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("upsert operator queue item: %v", err)
	}

	updated, err := store.UpsertOperatorQueueItem(ctx, OperatorQueueUpsertInput{
		WorkspaceID:             workspaceID,
		QueueID:                 queue.QueueID,
		QueueKey:                queue.QueueKey,
		QueueType:               queue.QueueType,
		Title:                   queue.Title,
		Summary:                 "fresh summary",
		SourceKind:              queue.SourceKind,
		SourceID:                queue.SourceID,
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  queue.Revision,
		RequireCurrentUpdatedAt: queue.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("fresh operator queue update: %v", err)
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	t.Cleanup(func() {
		_ = tx.Rollback()
	})

	loaded, found, err := store.loadOperatorQueueItemByKeyTx(ctx, tx, workspaceID, queue.QueueKey)
	if err != nil {
		t.Fatalf("load operator queue item by key: %v", err)
	}
	if !found {
		t.Fatalf("expected queue %q to be found", queue.QueueKey)
	}
	if loaded.QueueID != updated.QueueID {
		t.Fatalf("expected queue id %q, got %q", updated.QueueID, loaded.QueueID)
	}
	if loaded.Revision != updated.Revision {
		t.Fatalf("expected live revision %d, got %d", updated.Revision, loaded.Revision)
	}
	if loaded.UpdatedAt != updated.UpdatedAt {
		t.Fatalf("expected live updated_at %q, got %q", updated.UpdatedAt, loaded.UpdatedAt)
	}

	missing, found, err := store.loadOperatorQueueItemByKeyTx(ctx, tx, workspaceID, "queue:missing")
	if err != nil {
		t.Fatalf("load missing operator queue item by key: %v", err)
	}
	if found {
		t.Fatalf("expected missing queue lookup to report not found, got %+v", missing)
	}
}
