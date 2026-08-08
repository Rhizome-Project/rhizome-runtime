package sqlite_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestResolveOperatorQueueItemWithEventRejectsWorkspaceIDSpoof(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceA = "ws-sqlite-p0-spoof-queue-a"
		workspaceB = "ws-sqlite-p0-spoof-queue-b"
	)
	for _, workspaceID := range []string{workspaceA, workspaceB} {
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       workspaceID,
			CreatedBy:   "tests",
		}); err != nil {
			t.Fatalf("create workspace %s: %v", workspaceID, err)
		}
		claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	}

	queueA, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceA,
		QueueKey:    "queue:p0-spoof-resolve-a",
		QueueType:   "FOLLOW_UP",
		Title:       "Queue in workspace A",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("create source queue: %v", err)
	}

	result, event, err := store.ResolveOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceB,
		QueueID:     queueA.QueueID,
		ResolvedBy:  "operator-b",
		Resolution:  "cross-workspace queue_id spoof must fail",
	})
	if err == nil {
		t.Fatalf("expected cross-workspace queue_id spoof to fail closed, got result=%+v event=%+v", result, event)
	}
	if !errors.Is(err, sqlite.ErrOperatorQueueItemNotFound) || !strings.Contains(err.Error(), "operator queue item not found") {
		t.Fatalf("unexpected cross-workspace queue_id spoof error: %v", err)
	}

	reloaded, err := store.GetOperatorQueueItem(ctx, workspaceA, queueA.QueueID, "")
	if err != nil {
		t.Fatalf("reload source queue after spoof reject: %v", err)
	}
	if reloaded.Status != "OPEN" || reloaded.Resolution != "" || reloaded.ResolvedAt != nil || reloaded.ResolvedBy != nil {
		t.Fatalf("expected source queue to remain unresolved after spoof reject, got %+v", reloaded)
	}
	assertNoP0SpoofQueueResolvedEvents(t, ctx, store, workspaceA, queueA.QueueID)
	assertNoP0SpoofQueueResolvedEvents(t, ctx, store, workspaceB, queueA.QueueID)
}

func assertNoP0SpoofQueueResolvedEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list resolved events for %s/%s: %v", workspaceID, queueID, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no resolved events for %s/%s after spoof reject, got %+v", workspaceID, queueID, events)
	}
}
