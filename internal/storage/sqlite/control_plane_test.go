package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRuntimeEventsAndOperatorQueueLifecycle(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-control-events",
		Title:       "Control Plane Events",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-control-events")

	first, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: "ws-control-events",
		EventType:   "custom.first",
		EntityType:  "test_entity",
		EntityID:    "first",
		ActorType:   "operator",
		ActorID:     "developer",
	})
	if err != nil {
		t.Fatalf("record first runtime event: %v", err)
	}
	second, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: "ws-control-events",
		EventType:   "custom.second",
		EntityType:  "test_entity",
		EntityID:    "second",
		ActorType:   "operator",
		ActorID:     "developer",
	})
	if err != nil {
		t.Fatalf("record second runtime event: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-control-events",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected at least two runtime events, got %+v", events)
	}
	if events[0].EventID != second.EventID || events[1].EventID != first.EventID {
		t.Fatalf("expected reverse chronological runtime events, got %+v", events[:2])
	}

	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       "ws-control-events",
		QueueKey:          "manual:operator-followup",
		QueueType:         "FOLLOW_UP",
		Title:             "Operator follow-up",
		Summary:           "Confirm rollout status",
		AssignedTo:        "developer",
		Urgency:           "HIGH",
		SourceKind:        "manual",
		SourceID:          "developer",
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("upsert operator queue item: %v", err)
	}
	if queue.QueueType != "FOLLOW_UP" || queue.Status != "OPEN" {
		t.Fatalf("unexpected operator queue item %+v", queue)
	}
	if queue.TimeAuthority.WorkspaceID != "ws-control-events" || queue.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected queue time authority, got %+v", queue.TimeAuthority)
	}

	resolved, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: "ws-control-events",
		QueueID:     queue.QueueID,
		Status:      "RESOLVED",
		ResolvedBy:  "developer",
		Resolution:  "rollout confirmed",
	})
	if err != nil {
		t.Fatalf("resolve operator queue item: %v", err)
	}
	if resolved.Status != "RESOLVED" {
		t.Fatalf("expected resolved operator queue item, got %+v", resolved)
	}
	if resolved.TimeAuthority.WorkspaceID != "ws-control-events" || resolved.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected resolved queue time authority, got %+v", resolved.TimeAuthority)
	}

	items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: "ws-control-events",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list operator queue items: %v", err)
	}
	if len(items) != 1 || items[0].Resolution != "rollout confirmed" {
		t.Fatalf("unexpected operator queue items %+v", items)
	}
	if items[0].TimeAuthority.WorkspaceID != "ws-control-events" || items[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected listed queue time authority, got %+v", items[0].TimeAuthority)
	}
}

func TestOperatorQueueWithEventReturnsExactPersistedRowsOnRepeatedLifecycle(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-exact"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Exact",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	firstRecord, firstEvent, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "queue:exact-row",
		QueueType:         "FOLLOW_UP",
		Title:             "Exact row queue",
		Summary:           "first summary",
		AssignedTo:        "developer",
		Urgency:           "NORMAL",
		SourceKind:        "manual",
		SourceID:          "tests",
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("first upsert queue with event: %v", err)
	}
	if firstRecord.QueueID == "" || firstEvent.EventID == "" {
		t.Fatalf("expected queue record and exact runtime event, got record=%+v event=%+v", firstRecord, firstEvent)
	}
	firstPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.created",
		EntityType:  "operator_queue",
		EntityID:    firstRecord.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list created queue runtime events: %v", err)
	}
	if len(firstPersisted) != 1 || firstPersisted[0] != firstEvent {
		t.Fatalf("expected first upsert exact runtime row, returned=%+v persisted=%+v", firstEvent, firstPersisted)
	}

	secondRecord, secondEvent, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "queue:exact-row",
		QueueType:         "FOLLOW_UP",
		Title:             "Exact row queue",
		Summary:           "second summary",
		AssignedTo:        "developer",
		Urgency:           "HIGH",
		SourceKind:        "manual",
		SourceID:          "tests",
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("second upsert queue with event: %v", err)
	}
	if secondRecord.QueueID != firstRecord.QueueID {
		t.Fatalf("expected repeated upsert to keep queue id, first=%+v second=%+v", firstRecord, secondRecord)
	}
	secondPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    firstRecord.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list updated queue runtime events: %v", err)
	}
	if len(secondPersisted) == 0 || secondPersisted[0] != secondEvent {
		t.Fatalf("expected second upsert exact runtime row, returned=%+v persisted=%+v", secondEvent, secondPersisted)
	}
	if secondEvent.EventID == firstEvent.EventID || secondEvent.IngestSeq <= firstEvent.IngestSeq {
		t.Fatalf("expected repeated upsert to return newer runtime row, first=%+v second=%+v", firstEvent, secondEvent)
	}
	var updatedPayload map[string]any
	if err := json.Unmarshal([]byte(secondEvent.PayloadJSON), &updatedPayload); err != nil {
		t.Fatalf("decode updated queue payload: %v", err)
	}
	if strings.TrimSpace(updatedPayload["summary"].(string)) != "second summary" {
		t.Fatalf("expected updated queue payload summary, got %+v", updatedPayload)
	}

	firstEscalation, firstEscalationEvent, _, err := store.EscalateOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueEscalateInput{
		WorkspaceID: workspaceID,
		QueueID:     firstRecord.QueueID,
		EscalatedBy: "developer",
		Reason:      "first escalation",
		Urgency:     "HIGH",
	})
	if err != nil {
		t.Fatalf("first escalate queue with event: %v", err)
	}
	firstEscalationPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    firstRecord.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list first escalated queue runtime events: %v", err)
	}
	if len(firstEscalationPersisted) == 0 || firstEscalationPersisted[0] != firstEscalationEvent {
		t.Fatalf("expected first escalation exact runtime row, returned=%+v persisted=%+v", firstEscalationEvent, firstEscalationPersisted)
	}

	secondEscalation, secondEscalationEvent, _, err := store.EscalateOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueEscalateInput{
		WorkspaceID: workspaceID,
		QueueID:     firstRecord.QueueID,
		EscalatedBy: "developer",
		Reason:      "second escalation",
		Urgency:     "HIGH",
	})
	if err != nil {
		t.Fatalf("second escalate queue with event: %v", err)
	}
	if secondEscalation.EscalationCount <= firstEscalation.EscalationCount {
		t.Fatalf("expected escalation count to advance, first=%+v second=%+v", firstEscalation, secondEscalation)
	}
	secondEscalationPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    firstRecord.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second escalated queue runtime events: %v", err)
	}
	if len(secondEscalationPersisted) < 2 || secondEscalationPersisted[0] != secondEscalationEvent {
		t.Fatalf("expected second escalation exact runtime row, returned=%+v persisted=%+v", secondEscalationEvent, secondEscalationPersisted)
	}
	if secondEscalationEvent.EventID == firstEscalationEvent.EventID || secondEscalationEvent.IngestSeq <= firstEscalationEvent.IngestSeq {
		t.Fatalf("expected repeated escalation to return newer runtime row, first=%+v second=%+v", firstEscalationEvent, secondEscalationEvent)
	}

	resolvedRecord, resolvedEvent, err := store.ResolveOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     firstRecord.QueueID,
		ResolvedBy:  "developer",
		Resolution:  "resolved after escalation",
	})
	if err != nil {
		t.Fatalf("resolve queue with event: %v", err)
	}
	if resolvedRecord.Status != "RESOLVED" {
		t.Fatalf("expected resolved queue record, got %+v", resolvedRecord)
	}
	resolvedPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    firstRecord.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list resolved queue runtime events: %v", err)
	}
	if len(resolvedPersisted) == 0 || resolvedPersisted[0] != resolvedEvent {
		t.Fatalf("expected resolved queue exact runtime row, returned=%+v persisted=%+v", resolvedEvent, resolvedPersisted)
	}
}

func TestListOperatorQueueItemsNegativeLimitReturnsFullFollowupSet(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-unbounded-limit"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Unbounded Limit",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	const total = 206
	const oldestKey = "queue:unbounded-000"
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("queue:unbounded-%03d", i)
		if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
			WorkspaceID:       workspaceID,
			QueueKey:          key,
			QueueType:         "FOLLOW_UP",
			Title:             fmt.Sprintf("Unbounded Queue %03d", i),
			Summary:           "Queue item for negative-limit full scan coverage.",
			AssignedTo:        "developer",
			Urgency:           "LOW",
			SourceKind:        "manual",
			SourceID:          fmt.Sprintf("source-%03d", i),
			KeepSessionActive: false,
		}); err != nil {
			t.Fatalf("upsert queue item %d: %v", i, err)
		}
	}

	items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Status:      "OPEN",
		Limit:       -1,
	})
	if err != nil {
		t.Fatalf("list operator queue items with negative limit: %v", err)
	}
	if len(items) != total {
		t.Fatalf("listed %d items, want %d", len(items), total)
	}
	if items[len(items)-1].QueueKey != oldestKey {
		t.Fatalf("last queue key = %q, want %q to prove the full set was returned", items[len(items)-1].QueueKey, oldestKey)
	}
}

func TestOperatorQueueResolveRequiresOpenByDefault(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-require-open"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Require Open",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:require-open",
		QueueType:   "FOLLOW_UP",
		Title:       "Require open queue",
		Summary:     "Queue should only resolve once unless reopened explicitly.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("upsert queue: %v", err)
	}
	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		ResolvedBy:  "developer",
		Resolution:  "done",
	}); err != nil {
		t.Fatalf("resolve queue first time: %v", err)
	}
	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		ResolvedBy:  "developer",
		Resolution:  "done again",
	}); err == nil || !strings.Contains(err.Error(), "operator queue item is not open") {
		t.Fatalf("expected repeated resolve to require OPEN state, got %v", err)
	}
}

func TestOperatorQueueUpsertRejectsStaleUpdatedAt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-stale-upsert"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Stale Upsert",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:stale-upsert",
		QueueType:   "FOLLOW_UP",
		Title:       "Stale upsert queue",
		Summary:     "Initial summary",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("upsert queue: %v", err)
	}
	staleUpdatedAt := queue.UpdatedAt
	staleRevision := queue.Revision

	updated, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:             workspaceID,
		QueueID:                 queue.QueueID,
		QueueKey:                queue.QueueKey,
		QueueType:               queue.QueueType,
		Title:                   queue.Title,
		Summary:                 "Fresh summary",
		SourceKind:              queue.SourceKind,
		SourceID:                queue.SourceID,
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  staleRevision,
		RequireCurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("fresh upsert queue: %v", err)
	}
	if updated.UpdatedAt == staleUpdatedAt {
		t.Fatalf("expected fresh upsert to advance updated_at, got %q", updated.UpdatedAt)
	}
	if updated.Revision != staleRevision+1 {
		t.Fatalf("expected fresh upsert to advance revision from %d to %d, got %d", staleRevision, staleRevision+1, updated.Revision)
	}

	if _, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:             workspaceID,
		QueueID:                 queue.QueueID,
		QueueKey:                queue.QueueKey,
		QueueType:               queue.QueueType,
		Title:                   queue.Title,
		Summary:                 "Stale overwrite should fail",
		SourceKind:              queue.SourceKind,
		SourceID:                queue.SourceID,
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  staleRevision,
		RequireCurrentUpdatedAt: staleUpdatedAt,
	}); err == nil || !strings.Contains(err.Error(), "updated concurrently") {
		t.Fatalf("expected stale upsert to fail with revision guard, got %v", err)
	}
}

func TestOperatorQueueUpsertRequireMissingRejectsConcurrentCreate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-require-missing"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Require Missing",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	created, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:    workspaceID,
		QueueKey:       "queue:require-missing",
		QueueType:      "FOLLOW_UP",
		Title:          "Require missing queue",
		Summary:        "Initial create should succeed while row is still missing.",
		SourceKind:     "manual",
		SourceID:       "tests",
		RequireMissing: true,
	})
	if err != nil {
		t.Fatalf("initial require-missing upsert: %v", err)
	}
	if created.QueueID == "" {
		t.Fatalf("expected created queue id, got empty")
	}
	if created.Revision != 1 {
		t.Fatalf("expected initial queue revision 1, got %d", created.Revision)
	}

	if _, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:    workspaceID,
		QueueKey:       "queue:require-missing",
		QueueType:      "FOLLOW_UP",
		Title:          "Require missing queue",
		Summary:        "Concurrent create should fail closed.",
		SourceKind:     "manual",
		SourceID:       "tests",
		RequireMissing: true,
	}); err == nil || !strings.Contains(err.Error(), "updated concurrently") {
		t.Fatalf("expected require-missing upsert to fail with revision guard, got %v", err)
	}
}

func TestOperatorQueueResolveRejectsStaleUpdatedAt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-stale-resolve"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Stale Resolve",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:stale-resolve",
		QueueType:   "FOLLOW_UP",
		Title:       "Stale resolve queue",
		Summary:     "Initial summary",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("upsert queue: %v", err)
	}
	staleUpdatedAt := queue.UpdatedAt
	staleRevision := queue.Revision

	updated, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:             workspaceID,
		QueueID:                 queue.QueueID,
		QueueKey:                queue.QueueKey,
		QueueType:               queue.QueueType,
		Title:                   queue.Title,
		Summary:                 "Queue updated before resolve",
		SourceKind:              queue.SourceKind,
		SourceID:                queue.SourceID,
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  staleRevision,
		RequireCurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("fresh upsert before resolve: %v", err)
	}
	if updated.UpdatedAt == staleUpdatedAt {
		t.Fatalf("expected fresh upsert to advance updated_at, got %q", updated.UpdatedAt)
	}
	if updated.Revision != staleRevision+1 {
		t.Fatalf("expected fresh upsert to advance revision from %d to %d, got %d", staleRevision, staleRevision+1, updated.Revision)
	}

	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID:             workspaceID,
		QueueID:                 queue.QueueID,
		Status:                  "RESOLVED",
		ResolvedBy:              "developer",
		Resolution:              "stale resolve should fail",
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  staleRevision,
		RequireCurrentUpdatedAt: staleUpdatedAt,
	}); err == nil || !strings.Contains(err.Error(), "updated concurrently") {
		t.Fatalf("expected stale resolve to fail with revision guard, got %v", err)
	}
}

func TestOperatorQueueEscalateRejectsStaleUpdatedAt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-stale-escalate"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Stale Escalate",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:stale-escalate",
		QueueType:   "FOLLOW_UP",
		Title:       "Stale escalate queue",
		Summary:     "Initial summary",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("upsert queue: %v", err)
	}
	staleUpdatedAt := queue.UpdatedAt
	staleRevision := queue.Revision

	fresh, err := store.EscalateOperatorQueueItem(ctx, sqlite.OperatorQueueEscalateInput{
		WorkspaceID:             workspaceID,
		QueueID:                 queue.QueueID,
		EscalatedBy:             "developer",
		Reason:                  "fresh escalation before stale retry",
		AssignedTo:              "reviewer-fresh",
		Urgency:                 "CRITICAL",
		DueAt:                   "2099-02-01T00:00:00Z",
		RequireCurrentRevision:  staleRevision,
		RequireCurrentUpdatedAt: staleUpdatedAt,
	})
	if err != nil {
		t.Fatalf("fresh escalate before stale retry: %v", err)
	}
	if fresh.UpdatedAt == staleUpdatedAt {
		t.Fatalf("expected fresh escalate to advance updated_at, got %q", fresh.UpdatedAt)
	}
	if fresh.Revision != staleRevision+1 {
		t.Fatalf("expected fresh escalate to advance revision from %d to %d, got %d", staleRevision, staleRevision+1, fresh.Revision)
	}

	if _, err := store.EscalateOperatorQueueItem(ctx, sqlite.OperatorQueueEscalateInput{
		WorkspaceID:             workspaceID,
		QueueID:                 queue.QueueID,
		EscalatedBy:             "developer",
		Reason:                  "stale escalate should fail",
		AssignedTo:              "reviewer-stale",
		Urgency:                 "LOW",
		DueAt:                   "2099-03-01T00:00:00Z",
		RequireCurrentRevision:  staleRevision,
		RequireCurrentUpdatedAt: staleUpdatedAt,
	}); err == nil || !strings.Contains(err.Error(), "updated concurrently") {
		t.Fatalf("expected stale escalate to fail with revision guard, got %v", err)
	}
}

func TestOperatorQueueUpsertBlocksImplicitTerminalReopen(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-no-implicit-reopen"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue No Implicit Reopen",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:no-implicit-reopen",
		QueueType:   "FOLLOW_UP",
		Title:       "No implicit reopen queue",
		Summary:     "Queue should not reopen without an explicit terminal precondition.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("upsert queue: %v", err)
	}
	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		ResolvedBy:  "developer",
		Resolution:  "done",
	}); err != nil {
		t.Fatalf("resolve queue: %v", err)
	}
	if _, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    queue.QueueKey,
		QueueType:   queue.QueueType,
		Title:       queue.Title,
		Summary:     "Implicit reopen should be rejected",
		SourceKind:  queue.SourceKind,
		SourceID:    queue.SourceID,
	}); err == nil || !strings.Contains(err.Error(), "operator queue item is not open") {
		t.Fatalf("expected implicit terminal reopen to be rejected, got %v", err)
	}
}

func TestOperatorQueueUpsertAllowsExplicitTerminalReopen(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-explicit-reopen"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Explicit Reopen",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:explicit-reopen",
		QueueType:   "FOLLOW_UP",
		Title:       "Explicit reopen queue",
		Summary:     "Queue may reopen only with an explicit terminal precondition.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("upsert queue: %v", err)
	}
	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		ResolvedBy:  "developer",
		Resolution:  "done",
	}); err != nil {
		t.Fatalf("resolve queue: %v", err)
	}

	reopened, event, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:          workspaceID,
		QueueID:              queue.QueueID,
		QueueKey:             queue.QueueKey,
		QueueType:            queue.QueueType,
		Title:                queue.Title,
		Summary:              "Explicit reopen succeeded",
		SourceKind:           queue.SourceKind,
		SourceID:             queue.SourceID,
		RequireCurrentStatus: "RESOLVED",
	})
	if err != nil {
		t.Fatalf("explicit reopen queue: %v", err)
	}
	if reopened.Status != "OPEN" || reopened.Resolution != "" || reopened.ResolvedAt != nil || reopened.ResolvedBy != nil {
		t.Fatalf("expected reopened queue to clear terminal fields, got %+v", reopened)
	}
	if event.EventType != "operator_queue.reopened" {
		t.Fatalf("expected explicit reopen event, got %+v", event)
	}
}

func TestOperatorQueueUpsertRejectsReservedSessionQueueNamespace(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-operator-queue-reserved-session"
		queueKey    = "session:sess-reserved:blocker"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Reserved Session Namespace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    queueKey,
		QueueType:   "BLOCKER",
		Title:       "Spoofed session blocker",
		Summary:     "Generic queue upsert must not create reserved session queues.",
		SourceKind:  "session_event",
		SourceID:    "sess-reserved",
		SessionID:   "sess-reserved",
		AgentID:     "agent-reserved",
	}); err == nil || !strings.Contains(err.Error(), "queue_key has invalid value") {
		t.Fatalf("expected reserved session namespace reject, got %v", err)
	}

	assertNoOperatorQueueSideEffects(t, ctx, store, workspaceID, queueKey, 0)
}

func TestOperatorQueueUpsertRejectsUpdatesToExistingReservedSessionQueue(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-operator-queue-reserved-session-update"
		agentID     = "agent-reserved-session-update"
		sessionID   = "sess-reserved-session-update"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Reserved Session Update",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Reserved Session Update Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "Blocked session queue should remain workflow-managed",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve rollout"}},
	})
	if err != nil {
		t.Fatalf("record blocked session state: %v", err)
	}
	syncResult, err := store.SyncOperatorQueueFromSessionState(ctx, state)
	if err != nil {
		t.Fatalf("sync operator queue from session state: %v", err)
	}
	if len(syncResult.Opened) != 1 {
		t.Fatalf("expected exactly one synced session queue, got %+v", syncResult)
	}
	queue := syncResult.Opened[0].Record
	beforeEvents := countOperatorQueueEvents(t, ctx, store, workspaceID)

	cases := []struct {
		name       string
		sourceKind string
		sourceID   string
	}{
		{name: "manual_provenance", sourceKind: "manual", sourceID: "tests"},
		{name: "spoofed_session_event", sourceKind: "session_event", sourceID: sessionID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
				QueueID:     queue.QueueID,
				WorkspaceID: workspaceID,
				QueueKey:    queue.QueueKey,
				QueueType:   queue.QueueType,
				Title:       queue.Title,
				Summary:     "tampered summary",
				AssignedTo:  "operator-x",
				SourceKind:  tc.sourceKind,
				SourceID:    tc.sourceID,
				SessionID:   sessionID,
				AgentID:     agentID,
			}); err == nil || !strings.Contains(err.Error(), "queue_key has invalid value") {
				t.Fatalf("expected reserved session update reject, got %v", err)
			}

			current, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
			if err != nil {
				t.Fatalf("reload queue after reserved update reject: %v", err)
			}
			if current.Summary != queue.Summary || current.AssignedTo != queue.AssignedTo || current.SourceKind != queue.SourceKind || current.SourceID != queue.SourceID || current.UpdatedAt != queue.UpdatedAt {
				t.Fatalf("reserved session queue changed after reject: got %+v want %+v", current, queue)
			}
			if got := countOperatorQueueEvents(t, ctx, store, workspaceID); got != beforeEvents {
				t.Fatalf("reserved session update reject should not append queue events, before=%d after=%d", beforeEvents, got)
			}
		})
	}
}

func TestSyncOperatorQueueFromSessionEndResolvesSessionSourcedExternalGateQueue(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-session-end-external-gate-queue"
		agentID     = "agent-session-end-external-gate-queue"
		sessionID   = "sess-session-end-external-gate-queue"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session End External Gate Queue",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Session End External Gate Queue Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "session starts before an external gate opens",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}

	externalGate, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "external_gate:credential_auth:rnar.task.example",
		QueueType:         "BLOCKER",
		Title:             "Complete the required authorization step",
		Summary:           "external gate tied to this session",
		Urgency:           "NORMAL",
		SourceKind:        "session",
		SourceID:          sessionID,
		SessionID:         sessionID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("upsert external gate queue: %v", err)
	}
	manualQueue, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "manual:follow-up:same-session",
		QueueType:         "FOLLOW_UP",
		Title:             "Manual follow-up on same session",
		Summary:           "manual queues are not session-owned cleanup targets",
		Urgency:           "NORMAL",
		SourceKind:        "manual",
		SourceID:          "operator",
		SessionID:         sessionID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("upsert manual queue: %v", err)
	}

	keepInactive := false
	ended, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:         model.SessionEventEnd,
		WorkspaceID:       workspaceID,
		SessionID:         sessionID,
		AgentID:           agentID,
		Summary:           "session ended by manager stop",
		KeepSessionActive: &keepInactive,
	})
	if err != nil {
		t.Fatalf("record session end: %v", err)
	}
	syncResult, err := store.SyncOperatorQueueFromSessionState(ctx, ended)
	if err != nil {
		t.Fatalf("sync session end operator queues: %v", err)
	}
	if len(syncResult.Resolved) != 1 || syncResult.Resolved[0].Record.QueueID != externalGate.QueueID {
		t.Fatalf("expected only external gate queue resolved, got %+v", syncResult)
	}

	resolved, err := store.GetOperatorQueueItem(ctx, workspaceID, externalGate.QueueID, "")
	if err != nil {
		t.Fatalf("get resolved external gate queue: %v", err)
	}
	if resolved.Status != "RESOLVED" || resolved.Resolution != "cleared_by_session_end" || resolved.ResolvedBy == nil || *resolved.ResolvedBy != agentID {
		t.Fatalf("unexpected external gate resolution: %+v", resolved)
	}
	stillOpen, err := store.GetOperatorQueueItem(ctx, workspaceID, manualQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get manual queue: %v", err)
	}
	if stillOpen.Status != "OPEN" {
		t.Fatalf("manual queue should remain open, got %+v", stillOpen)
	}
}

func TestOperatorQueueUpsertRejectsMissingWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Missing Authority",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:missing-authority",
		QueueType:   "FOLLOW_UP",
		Title:       "Missing authority queue",
		Summary:     "should fail closed",
		SourceKind:  "manual",
		SourceID:    "tests",
	}); err == nil {
		t.Fatal("expected missing workspace authority to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected authority missing reject, got %+v", err)
	}

	assertNoOperatorQueueSideEffects(t, ctx, store, workspaceID, "queue:missing-authority", 0)
}

func TestOperatorQueueUpsertRejectsStaleWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-stale-authority-upsert"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Stale Authority Upsert",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:stale-authority-upsert",
		QueueType:   "FOLLOW_UP",
		Title:       "Stale authority queue",
		Summary:     "original summary",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	queueEventsBefore := countOperatorQueueEvents(t, ctx, store, workspaceID)

	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-200")

	if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		QueueKey:    queue.QueueKey,
		QueueType:   queue.QueueType,
		Title:       queue.Title,
		Summary:     "stale overwrite should fail",
		SourceKind:  queue.SourceKind,
		SourceID:    queue.SourceID,
	}); err == nil {
		t.Fatal("expected stale workspace authority to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected authority stale reject, got %+v", err)
	}

	reloaded, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after stale reject: %v", err)
	}
	if reloaded.Summary != "original summary" {
		t.Fatalf("expected stale authority reject not to mutate queue, got %+v", reloaded)
	}
	assertNoOperatorQueueSideEffects(t, ctx, store, workspaceID, queue.QueueKey, queueEventsBefore)
}

func TestOperatorQueueResolveRejectsMissingWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-missing-authority-resolve"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Missing Authority Resolve",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:missing-authority-resolve",
		QueueType:   "FOLLOW_UP",
		Title:       "Resolve missing authority queue",
		Summary:     "seed queue",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	queueEventsBefore := countOperatorQueueEvents(t, ctx, store, workspaceID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	if _, _, err := store.ResolveOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		ResolvedBy:  "developer",
		Resolution:  "should fail closed",
	}); err == nil {
		t.Fatal("expected missing workspace authority to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected authority missing reject, got %+v", err)
	}

	reloaded, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after missing-authority reject: %v", err)
	}
	if reloaded.Status != "OPEN" || reloaded.Resolution != "" {
		t.Fatalf("expected missing authority reject not to resolve queue, got %+v", reloaded)
	}
	assertNoOperatorQueueSideEffects(t, ctx, store, workspaceID, queue.QueueKey, queueEventsBefore)
}

func TestOperatorQueueResolveRejectsStaleWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-stale-authority-resolve"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Stale Authority Resolve",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	queue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "queue:stale-authority-resolve",
		QueueType:   "FOLLOW_UP",
		Title:       "Resolve stale authority queue",
		Summary:     "seed queue",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	queueEventsBefore := countOperatorQueueEvents(t, ctx, store, workspaceID)

	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-201")

	if _, _, err := store.ResolveOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     queue.QueueID,
		ResolvedBy:  "developer",
		Resolution:  "stale resolve should fail",
	}); err == nil {
		t.Fatal("expected stale workspace authority to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected authority stale reject, got %+v", err)
	}

	reloaded, err := store.GetOperatorQueueItem(ctx, workspaceID, queue.QueueID, "")
	if err != nil {
		t.Fatalf("reload queue after stale-authority reject: %v", err)
	}
	if reloaded.Status != "OPEN" || reloaded.Resolution != "" {
		t.Fatalf("expected stale authority reject not to resolve queue, got %+v", reloaded)
	}
	assertNoOperatorQueueSideEffects(t, ctx, store, workspaceID, queue.QueueKey, queueEventsBefore)
}

func transferWorkspaceAuthorityToExternalPeer(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, current sqlite.WorkspaceAuthorityRecord, peerNodeID string) {
	t.Helper()

	referenceAt := time.Now().UTC().Round(0)
	var journalHead int64
	if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&journalHead); err != nil {
		t.Fatalf("query runtime journal head before transfer: %v", err)
	}
	commitWatermark := current.CommitWatermark + 1
	if journalHead > commitWatermark {
		commitWatermark = journalHead
	}
	appliedWatermark := current.AppliedWatermark + 1
	if appliedWatermark > commitWatermark {
		appliedWatermark = commitWatermark
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		peerNodeID,
		"sqlite_peer_store",
		"peer-host",
		"boot-"+peerNodeID,
		referenceAt.Format(time.RFC3339Nano),
		referenceAt.Format(time.RFC3339Nano),
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityTransferInput{
		WorkspaceID:                  workspaceID,
		Scope:                        "workspace",
		CurrentHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		CurrentLeaseToken:            current.LeaseToken,
		CurrentTerm:                  current.Term,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-" + peerNodeID,
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             appliedWatermark,
		ActorType:                    "system",
		ActorID:                      "tests",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority to peer: %v", err)
	}
}

func countOperatorQueueEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("list operator queue runtime events: %v", err)
	}
	return len(events)
}

func assertNoOperatorQueueSideEffects(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueKey string, wantQueueEventCount int) {
	t.Helper()

	items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("list operator queue items: %v", err)
	}
	if queueKey != "" {
		found := false
		for _, item := range items {
			if item.QueueKey == queueKey {
				found = true
				break
			}
		}
		if wantQueueEventCount == 0 && found {
			t.Fatalf("expected no queue item for %s, got %+v", queueKey, items)
		}
	}
	if got := countOperatorQueueEvents(t, ctx, store, workspaceID); got != wantQueueEventCount {
		t.Fatalf("expected %d operator_queue runtime events, got %d", wantQueueEventCount, got)
	}
}

func TestKnowledgeClaimWithEventReturnsExactPersistedRowsOnRepeatedLifecycle(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-claim-exact"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Exact",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-claim-exact",
		OwnerUserID: "developer",
		DisplayName: "Claim Exact Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	firstClaim, firstWrittenEvent, err := store.RecordKnowledgeClaimWithEvent(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-exact-row",
		WorkspaceID: workspaceID,
		ClaimType:   "DECISION",
		Subject:     "Prefer exact returned runtime rows",
		Body:        "Knowledge claim writes should return the appended journal row.",
		Summary:     "first summary",
		Confidence:  0.7,
		SourceKind:  "manual",
		SourceID:    "tests",
		AgentID:     "agent-claim-exact",
	})
	if err != nil {
		t.Fatalf("record first knowledge claim with event: %v", err)
	}
	firstWrittenPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    firstClaim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list first written claim runtime events: %v", err)
	}
	if len(firstWrittenPersisted) != 1 || firstWrittenPersisted[0] != firstWrittenEvent {
		t.Fatalf("expected first written exact runtime row, returned=%+v persisted=%+v", firstWrittenEvent, firstWrittenPersisted)
	}

	secondClaim, secondWrittenEvent, err := store.RecordKnowledgeClaimWithEvent(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     firstClaim.ClaimID,
		WorkspaceID: workspaceID,
		ClaimType:   "DECISION",
		Subject:     "Prefer exact returned runtime rows",
		Body:        "Knowledge claim writes should return the newest appended journal row.",
		Summary:     "second summary",
		Confidence:  0.9,
		SourceKind:  "manual",
		SourceID:    "tests",
		AgentID:     "agent-claim-exact",
	})
	if err != nil {
		t.Fatalf("record second knowledge claim with event: %v", err)
	}
	if secondClaim.ClaimID != firstClaim.ClaimID {
		t.Fatalf("expected repeated claim write to keep claim id, first=%+v second=%+v", firstClaim, secondClaim)
	}
	secondWrittenPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    firstClaim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second written claim runtime events: %v", err)
	}
	if len(secondWrittenPersisted) < 2 || secondWrittenPersisted[0] != secondWrittenEvent {
		t.Fatalf("expected second written exact runtime row, returned=%+v persisted=%+v", secondWrittenEvent, secondWrittenPersisted)
	}
	if secondWrittenEvent.EventID == firstWrittenEvent.EventID || secondWrittenEvent.IngestSeq <= firstWrittenEvent.IngestSeq {
		t.Fatalf("expected repeated claim write to return newer runtime row, first=%+v second=%+v", firstWrittenEvent, secondWrittenEvent)
	}
	var writtenPayload map[string]any
	if err := json.Unmarshal([]byte(secondWrittenEvent.PayloadJSON), &writtenPayload); err != nil {
		t.Fatalf("decode written claim payload: %v", err)
	}
	if strings.TrimSpace(writtenPayload["summary"].(string)) != "second summary" {
		t.Fatalf("expected second written payload summary, got %+v", writtenPayload)
	}

	firstReview, firstReviewEvent, err := store.RequestKnowledgeClaimReviewWithEvent(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     firstClaim.ClaimID,
		ActorID:     "developer",
		Reason:      "first review request",
		ReviewDueAt: "2026-03-29T10:00:00Z",
		AssignedTo:  "reviewer-a",
	})
	if err != nil {
		t.Fatalf("request first claim review with event: %v", err)
	}
	firstReviewPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_requested",
		EntityType:  "knowledge_claim",
		EntityID:    firstClaim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list first review runtime events: %v", err)
	}
	if len(firstReviewPersisted) != 1 || firstReviewPersisted[0] != firstReviewEvent {
		t.Fatalf("expected first review exact runtime row, returned=%+v persisted=%+v", firstReviewEvent, firstReviewPersisted)
	}

	secondReview, secondReviewEvent, err := store.RequestKnowledgeClaimReviewWithEvent(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     firstClaim.ClaimID,
		ActorID:     "developer",
		Reason:      "second review request",
		ReviewDueAt: "2026-03-30T12:00:00Z",
		AssignedTo:  "reviewer-b",
	})
	if err != nil {
		t.Fatalf("request second claim review with event: %v", err)
	}
	if secondReview.Status != "REVIEW" || secondReview.ReviewDueAt == nil || *secondReview.ReviewDueAt != "2026-03-30T12:00:00Z" {
		t.Fatalf("expected repeated review request to return updated claim, first=%+v second=%+v", firstReview, secondReview)
	}
	secondReviewPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_requested",
		EntityType:  "knowledge_claim",
		EntityID:    firstClaim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second review runtime events: %v", err)
	}
	if len(secondReviewPersisted) < 2 || secondReviewPersisted[0] != secondReviewEvent {
		t.Fatalf("expected second review exact runtime row, returned=%+v persisted=%+v", secondReviewEvent, secondReviewPersisted)
	}
	if secondReviewEvent.EventID == firstReviewEvent.EventID || secondReviewEvent.IngestSeq <= firstReviewEvent.IngestSeq {
		t.Fatalf("expected repeated review request to return newer runtime row, first=%+v second=%+v", firstReviewEvent, secondReviewEvent)
	}
	var reviewPayload map[string]any
	if err := json.Unmarshal([]byte(secondReviewEvent.PayloadJSON), &reviewPayload); err != nil {
		t.Fatalf("decode review claim payload: %v", err)
	}
	if strings.TrimSpace(reviewPayload["reason"].(string)) != "second review request" || strings.TrimSpace(reviewPayload["review_due_at"].(string)) != "2026-03-30T12:00:00Z" {
		t.Fatalf("expected second review payload to win, got %+v", reviewPayload)
	}

	firstArchived, firstArchivedEvent, err := store.ArchiveKnowledgeClaimWithEvent(ctx, sqlite.KnowledgeClaimArchiveInput{
		WorkspaceID: workspaceID,
		ClaimID:     firstClaim.ClaimID,
		ArchivedBy:  "developer",
		Reason:      "first archive",
	})
	if err != nil {
		t.Fatalf("archive first claim with event: %v", err)
	}
	firstArchivedPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    firstClaim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list first archived claim runtime events: %v", err)
	}
	if len(firstArchivedPersisted) != 1 || firstArchivedPersisted[0] != firstArchivedEvent {
		t.Fatalf("expected first archive exact runtime row, returned=%+v persisted=%+v", firstArchivedEvent, firstArchivedPersisted)
	}

	secondArchived, secondArchivedEvent, err := store.ArchiveKnowledgeClaimWithEvent(ctx, sqlite.KnowledgeClaimArchiveInput{
		WorkspaceID: workspaceID,
		ClaimID:     firstClaim.ClaimID,
		ArchivedBy:  "developer",
		Reason:      "second archive",
	})
	if err != nil {
		t.Fatalf("archive second claim with event: %v", err)
	}
	if secondArchived.Status != "ARCHIVED" || secondArchived.LifecycleReason != "second archive" {
		t.Fatalf("expected repeated archive to return updated archived claim, first=%+v second=%+v", firstArchived, secondArchived)
	}
	secondArchivedPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    firstClaim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second archived claim runtime events: %v", err)
	}
	if len(secondArchivedPersisted) < 2 || secondArchivedPersisted[0] != secondArchivedEvent {
		t.Fatalf("expected second archive exact runtime row, returned=%+v persisted=%+v", secondArchivedEvent, secondArchivedPersisted)
	}
	if secondArchivedEvent.EventID == firstArchivedEvent.EventID || secondArchivedEvent.IngestSeq <= firstArchivedEvent.IngestSeq {
		t.Fatalf("expected repeated archive to return newer runtime row, first=%+v second=%+v", firstArchivedEvent, secondArchivedEvent)
	}
	var archivedPayload map[string]any
	if err := json.Unmarshal([]byte(secondArchivedEvent.PayloadJSON), &archivedPayload); err != nil {
		t.Fatalf("decode archived claim payload: %v", err)
	}
	if strings.TrimSpace(archivedPayload["reason"].(string)) != "second archive" {
		t.Fatalf("expected second archive payload reason, got %+v", archivedPayload)
	}
}

func TestClaimReviewEscalationWorkflow(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-claim-escalation"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Escalation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-escalation",
		OwnerUserID: "developer",
		DisplayName: "Escalation Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "DECISION",
		Subject:     "Use runtime journal for operator truth",
		Body:        "Claim review should escalate through the follow-up queue.",
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     "agent-escalation",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}
	if _, err := store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "developer",
		Reason:      "needs explicit operator validation",
		ReviewDueAt: "2026-03-23T10:00:00Z",
		AssignedTo:  "reviewer-a",
	}); err != nil {
		t.Fatalf("request claim review: %v", err)
	}

	escalated, err := store.EscalateKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "developer",
		Reason:      "review is nearing SLA breach",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-b",
		Urgency:     "CRITICAL",
	})
	if err != nil {
		t.Fatalf("escalate claim review: %v", err)
	}
	if escalated.Claim.Status != "REVIEW" || escalated.Claim.ReviewDueAt == nil || *escalated.Claim.ReviewDueAt != "2099-01-01T00:00:00Z" {
		t.Fatalf("unexpected escalated claim %+v", escalated.Claim)
	}
	if escalated.Queue.Status != "OPEN" || escalated.Queue.AssignedTo != "reviewer-b" || escalated.Queue.Urgency != "CRITICAL" || escalated.Queue.EscalationCount != 1 {
		t.Fatalf("unexpected escalated queue %+v", escalated.Queue)
	}
	if escalated.Queue.TimeAuthority.WorkspaceID != workspaceID || escalated.Queue.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected escalated queue time authority, got %+v", escalated.Queue.TimeAuthority)
	}
	if escalated.Queue.KeepSessionActive {
		t.Fatalf("expected escalated review queue to remain non-session-active, got %+v", escalated.Queue)
	}
	if escalated.Queue.LastEscalatedAt == nil || *escalated.Queue.LastEscalatedAt == "" {
		t.Fatalf("expected escalated queue to record last_escalated_at, got %+v", escalated.Queue)
	}
	if escalated.Queue.LastEscalatedBy == nil || *escalated.Queue.LastEscalatedBy != "developer" {
		t.Fatalf("expected escalated_by metadata, got %+v", escalated.Queue)
	}

	items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queue items: %v", err)
	}
	if len(items) != 1 || items[0].EscalationCount != 1 {
		t.Fatalf("expected escalated follow-up queue, got %+v", items)
	}
	if items[0].KeepSessionActive {
		t.Fatalf("expected listed follow-up queue to remain non-session-active, got %+v", items[0])
	}
	if items[0].LastEscalatedAt == nil || *items[0].LastEscalatedAt == "" {
		t.Fatalf("expected listed follow-up queue to preserve last_escalated_at, got %+v", items[0])
	}
	if items[0].LastEscalatedBy == nil || *items[0].LastEscalatedBy != "developer" {
		t.Fatalf("expected listed follow-up queue to preserve last_escalated_by, got %+v", items[0])
	}
	if items[0].TimeAuthority.WorkspaceID != workspaceID || items[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected listed follow-up queue time authority, got %+v", items[0].TimeAuthority)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	seen := map[string]bool{}
	for _, event := range events {
		seen[event.EventType] = true
	}
	for _, eventType := range []string{"knowledge_claim.review_requested", "knowledge_claim.review_escalated", "operator_queue.escalated"} {
		if !seen[eventType] {
			t.Fatalf("expected runtime event %s, got %+v", eventType, events)
		}
	}
}

func TestEscalateKnowledgeClaimReviewWithEventsReturnsExactRowsOnRepeatedEscalation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-claim-escalation-exact"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Escalation Exact",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-claim-escalation-exact",
		OwnerUserID: "developer",
		DisplayName: "Claim Escalation Exact Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	claim, _, err := store.RecordKnowledgeClaimWithEvent(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:     "claim-escalation-exact",
		WorkspaceID: workspaceID,
		ClaimType:   "DECISION",
		Subject:     "Escalate from exact returned rows",
		Body:        "Repeated review escalations should return the new claim and queue runtime rows.",
		Summary:     "needs escalation",
		Confidence:  0.8,
		SourceKind:  "manual",
		SourceID:    "tests",
		AgentID:     "agent-claim-escalation-exact",
	})
	if err != nil {
		t.Fatalf("record claim for repeated escalation: %v", err)
	}
	if _, _, err := store.RequestKnowledgeClaimReviewWithEvent(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "developer",
		Reason:      "prime the review queue",
		ReviewDueAt: "2026-03-29T09:00:00Z",
		AssignedTo:  "reviewer-a",
	}); err != nil {
		t.Fatalf("request initial claim review: %v", err)
	}

	firstEscalation, firstClaimEvent, firstQueueEvent, err := store.EscalateKnowledgeClaimReviewWithEvents(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "developer",
		Reason:      "first review escalation",
		ReviewDueAt: "2026-03-30T10:00:00Z",
		AssignedTo:  "reviewer-b",
		Urgency:     "HIGH",
	})
	if err != nil {
		t.Fatalf("first escalate claim review with events: %v", err)
	}
	firstClaimPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_escalated",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list first claim escalation runtime events: %v", err)
	}
	if len(firstClaimPersisted) != 1 || firstClaimPersisted[0] != firstClaimEvent {
		t.Fatalf("expected first escalated claim exact runtime row, returned=%+v persisted=%+v", firstClaimEvent, firstClaimPersisted)
	}
	firstQueuePersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    firstEscalation.Queue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list first queue escalation runtime events: %v", err)
	}
	if len(firstQueuePersisted) != 1 || firstQueuePersisted[0] != firstQueueEvent {
		t.Fatalf("expected first escalated queue exact runtime row, returned=%+v persisted=%+v", firstQueueEvent, firstQueuePersisted)
	}

	secondEscalation, secondClaimEvent, secondQueueEvent, err := store.EscalateKnowledgeClaimReviewWithEvents(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "developer",
		Reason:      "second review escalation",
		ReviewDueAt: "2026-03-31T11:00:00Z",
		AssignedTo:  "reviewer-c",
		Urgency:     "CRITICAL",
	})
	if err != nil {
		t.Fatalf("second escalate claim review with events: %v", err)
	}
	if secondEscalation.Queue.QueueID != firstEscalation.Queue.QueueID || secondEscalation.Queue.EscalationCount <= firstEscalation.Queue.EscalationCount {
		t.Fatalf("expected repeated claim escalation to advance queue on same queue id, first=%+v second=%+v", firstEscalation.Queue, secondEscalation.Queue)
	}
	secondClaimPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_escalated",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second claim escalation runtime events: %v", err)
	}
	if len(secondClaimPersisted) < 2 || secondClaimPersisted[0] != secondClaimEvent {
		t.Fatalf("expected second escalated claim exact runtime row, returned=%+v persisted=%+v", secondClaimEvent, secondClaimPersisted)
	}
	if secondClaimEvent.EventID == firstClaimEvent.EventID || secondClaimEvent.IngestSeq <= firstClaimEvent.IngestSeq {
		t.Fatalf("expected repeated claim escalation to return newer claim runtime row, first=%+v second=%+v", firstClaimEvent, secondClaimEvent)
	}
	secondQueuePersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.escalated",
		EntityType:  "operator_queue",
		EntityID:    firstEscalation.Queue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second queue escalation runtime events: %v", err)
	}
	if len(secondQueuePersisted) < 2 || secondQueuePersisted[0] != secondQueueEvent {
		t.Fatalf("expected second escalated queue exact runtime row, returned=%+v persisted=%+v", secondQueueEvent, secondQueuePersisted)
	}
	if secondQueueEvent.EventID == firstQueueEvent.EventID || secondQueueEvent.IngestSeq <= firstQueueEvent.IngestSeq {
		t.Fatalf("expected repeated claim escalation to return newer queue runtime row, first=%+v second=%+v", firstQueueEvent, secondQueueEvent)
	}

	var escalatedClaimPayload map[string]any
	if err := json.Unmarshal([]byte(secondClaimEvent.PayloadJSON), &escalatedClaimPayload); err != nil {
		t.Fatalf("decode escalated claim payload: %v", err)
	}
	if strings.TrimSpace(escalatedClaimPayload["reason"].(string)) != "second review escalation" || strings.TrimSpace(escalatedClaimPayload["assigned_to"].(string)) != "reviewer-c" {
		t.Fatalf("expected second escalated claim payload to win, got %+v", escalatedClaimPayload)
	}
}

func TestKnowledgeClaimsSearchArchiveAndWorkspaceSearch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-control-claims",
		Title:       "Control Plane Claims",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-control-claims")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-control-claims",
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	memory, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-control-claims",
		MemoryType:  "decision",
		Title:       "Use runtime journal",
		Body:        "Runtime events are the canonical source of operational truth.",
		Summary:     "Use runtime journal as the source of truth.",
		AgentID:     "agent-a",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	promotedClaims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: "ws-control-claims",
		MemoryID:    memory.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted knowledge claims: %v", err)
	}
	if len(promotedClaims) != 1 || promotedClaims[0].ClaimID != "claim:memory:"+memory.MemoryID || promotedClaims[0].ClaimType != "DECISION" {
		t.Fatalf("expected one promoted claim for memory, got %+v", promotedClaims)
	}

	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: "ws-control-claims",
		ClaimType:   "decision",
		Status:      "active",
		Subject:     "Runtime journal is canonical",
		Body:        "Operators and agents should reason from runtime events rather than archived traces.",
		Summary:     "Use runtime events, not archived traces.",
		Confidence:  0.9,
		SourceKind:  "workspace_memory",
		SourceID:    memory.MemoryID,
		MemoryID:    memory.MemoryID,
		AgentID:     "agent-a",
		Tags:        []string{"runtime", "truth"},
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	searchResults, err := store.SearchKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: "ws-control-claims",
		Query:       "archived traces",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search knowledge claims: %v", err)
	}
	if len(searchResults) != 1 || searchResults[0].ClaimID != claim.ClaimID {
		t.Fatalf("unexpected claim search results %+v", searchResults)
	}

	workspaceResults, err := store.SearchWorkspace(ctx, sqlite.WorkspaceSearchFilter{
		WorkspaceID: "ws-control-claims",
		Query:       "canonical",
		EntityType:  "claim",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("workspace search: %v", err)
	}
	if len(workspaceResults) != 2 {
		t.Fatalf("expected both manual and promoted claims in workspace search, got %+v", workspaceResults)
	}
	foundManual := false
	foundPromoted := false
	for _, item := range workspaceResults {
		if item.EntityType != "claim" {
			t.Fatalf("unexpected workspace result %+v", item)
		}
		if item.EntityID == claim.ClaimID {
			foundManual = true
		}
		if item.EntityID == "claim:memory:"+memory.MemoryID {
			foundPromoted = true
		}
	}
	if !foundManual || !foundPromoted {
		t.Fatalf("unexpected workspace claim results %+v", workspaceResults)
	}

	archived, err := store.ArchiveKnowledgeClaim(ctx, sqlite.KnowledgeClaimArchiveInput{
		WorkspaceID: "ws-control-claims",
		ClaimID:     claim.ClaimID,
		ArchivedBy:  "developer",
		Reason:      "superseded",
	})
	if err != nil {
		t.Fatalf("archive knowledge claim: %v", err)
	}
	if archived.ArchivedAt == nil || archived.ArchivedBy == nil || *archived.ArchivedBy != "developer" {
		t.Fatalf("expected archived knowledge claim metadata, got %+v", archived)
	}

	activeClaims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: "ws-control-claims",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list active knowledge claims: %v", err)
	}
	if len(activeClaims) != 1 || activeClaims[0].ClaimID != "claim:memory:"+memory.MemoryID {
		t.Fatalf("expected only promoted claim to remain active, got %+v", activeClaims)
	}

	allClaims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID:     "ws-control-claims",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("list archived knowledge claims: %v", err)
	}
	if len(allClaims) != 2 {
		t.Fatalf("unexpected archived claim list %+v", allClaims)
	}
	foundArchivedManual := false
	foundActivePromoted := false
	for _, item := range allClaims {
		if item.ClaimID == claim.ClaimID && item.ArchivedAt != nil {
			foundArchivedManual = true
		}
		if item.ClaimID == "claim:memory:"+memory.MemoryID && item.ArchivedAt == nil {
			foundActivePromoted = true
		}
	}
	if !foundArchivedManual || !foundActivePromoted {
		t.Fatalf("unexpected archived claim state %+v", allClaims)
	}
}

func TestKnowledgeClaimPreservesDissentTypeAndFilter(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-claim-dissent"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Dissent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "dissent",
		Status:      "active",
		Subject:     "Alternative rollout path",
		Body:        "The dissenting path should remain visible instead of collapsing into a generic fact.",
		Summary:     "Preserve dissent.",
		Confidence:  0.55,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record dissent claim: %v", err)
	}
	if claim.ClaimType != "DISSENT" {
		t.Fatalf("expected DISSENT claim type, got %+v", claim)
	}

	items, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		ClaimType:   "DISSENT",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list dissent claims: %v", err)
	}
	if len(items) != 1 || items[0].ClaimID != claim.ClaimID || items[0].ClaimType != "DISSENT" {
		t.Fatalf("expected dissent filter to return the dissent claim, got %+v", items)
	}
}

func TestKnowledgeClaimPreservesDifferentiatedDissentTypesAndFilters(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-claim-dissent-split"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Dissent Split",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	cases := []struct {
		id        string
		claimType string
		subject   string
	}{
		{id: "claim-dissent-marker", claimType: "dissent_marker", subject: "Rollback disagreement exists"},
		{id: "claim-dissent-content", claimType: "dissent_content", subject: "Rollback counter-argument"},
	}
	for _, tc := range cases {
		tc := tc
		claim, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
			WorkspaceID: workspaceID,
			ClaimID:     tc.id,
			ClaimType:   tc.claimType,
			Status:      "active",
			Subject:     tc.subject,
			Body:        "Bounded dissent split should preserve the authored claim type.",
			Summary:     tc.subject,
			Confidence:  0.6,
			SourceKind:  "manual",
			SourceID:    "developer",
		})
		if err != nil {
			t.Fatalf("record %s claim: %v", tc.claimType, err)
		}
		wantType := strings.ToUpper(tc.claimType)
		if claim.ClaimType != wantType {
			t.Fatalf("expected %s claim type, got %+v", wantType, claim)
		}

		items, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
			WorkspaceID: workspaceID,
			ClaimType:   wantType,
			Limit:       10,
		})
		if err != nil {
			t.Fatalf("list %s claims: %v", wantType, err)
		}
		if len(items) != 1 || items[0].ClaimID != claim.ClaimID || items[0].ClaimType != wantType {
			t.Fatalf("expected %s filter to return the matching claim, got %+v", wantType, items)
		}
	}
}

func TestKnowledgeClaimLifecycleTransitionsAndRanking(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-claim-lifecycle",
		Title:       "Claim Lifecycle",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-claim-lifecycle")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-claim-lifecycle",
		AgentID:     "agent-lifecycle",
		OwnerUserID: "developer",
		DisplayName: "Lifecycle Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	claimA, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: "ws-claim-lifecycle",
		ClaimType:   "FACT",
		Subject:     "Runtime journal is canonical",
		Body:        "Use runtime events rather than archived traces.",
		Summary:     "Runtime journal beats archived traces.",
		Confidence:  0.4,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     "agent-lifecycle",
	})
	if err != nil {
		t.Fatalf("record claim A: %v", err)
	}

	reviewed, err := store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: "ws-claim-lifecycle",
		ClaimID:     claimA.ClaimID,
		ActorID:     "developer",
		Reason:      "needs explicit confirmation",
		ReviewDueAt: "2026-03-23T09:00:00Z",
		AssignedTo:  "reviewer-a",
	})
	if err != nil {
		t.Fatalf("request claim review: %v", err)
	}
	if reviewed.Status != "REVIEW" || reviewed.ReviewDueAt == nil || *reviewed.ReviewDueAt != "2026-03-23T09:00:00Z" {
		t.Fatalf("unexpected reviewed claim %+v", reviewed)
	}
	ops, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: "ws-claim-lifecycle",
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queue items: %v", err)
	}
	if len(ops) != 1 || ops[0].SourceID != claimA.ClaimID || ops[0].AssignedTo != "reviewer-a" || ops[0].Status != "OPEN" {
		t.Fatalf("unexpected review queue items %+v", ops)
	}

	confirmed, err := store.ConfirmKnowledgeClaim(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: "ws-claim-lifecycle",
		ClaimID:     claimA.ClaimID,
		ActorID:     "developer",
		Reason:      "validated against live runtime",
	})
	if err != nil {
		t.Fatalf("confirm claim: %v", err)
	}
	if confirmed.Status != "CONFIRMED" || confirmed.ReviewedBy == nil || *confirmed.ReviewedBy != "developer" {
		t.Fatalf("unexpected confirmed claim %+v", confirmed)
	}
	ops, err = store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: "ws-claim-lifecycle",
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list follow-up queue items after confirm: %v", err)
	}
	if len(ops) != 1 || ops[0].Status != "RESOLVED" {
		t.Fatalf("expected resolved follow-up queue after confirm, got %+v", ops)
	}

	claimB, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: "ws-claim-lifecycle",
		ClaimType:   "FACT",
		Subject:     "Runtime journal is canonical",
		Body:        "Fresh confirming claim for the same operational fact.",
		Summary:     "Fresh canonical fact.",
		Confidence:  0.2,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     "agent-lifecycle",
	})
	if err != nil {
		t.Fatalf("record claim B: %v", err)
	}
	claimB, err = store.ConfirmKnowledgeClaim(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: "ws-claim-lifecycle",
		ClaimID:     claimB.ClaimID,
		ActorID:     "developer",
		Reason:      "fresh canonical wording",
	})
	if err != nil {
		t.Fatalf("confirm claim B: %v", err)
	}

	disputed, err := store.DisputeKnowledgeClaim(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:      "ws-claim-lifecycle",
		ClaimID:          claimA.ClaimID,
		ActorID:          "developer",
		Reason:           "older phrasing needs replacement",
		ConflictsClaimID: claimB.ClaimID,
		AssignedTo:       "reviewer-b",
	})
	if err != nil {
		t.Fatalf("dispute claim A: %v", err)
	}
	if disputed.Status != "DISPUTED" || disputed.ConflictsClaimID != claimB.ClaimID {
		t.Fatalf("unexpected disputed claim %+v", disputed)
	}

	superseded, err := store.SupersedeKnowledgeClaim(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:        "ws-claim-lifecycle",
		ClaimID:            claimA.ClaimID,
		ActorID:            "developer",
		Reason:             "newer claim supersedes the old one",
		SupersedingClaimID: claimB.ClaimID,
	})
	if err != nil {
		t.Fatalf("supersede claim A: %v", err)
	}
	if superseded.Status != "SUPERSEDED" || superseded.SupersededByClaimID != claimB.ClaimID {
		t.Fatalf("unexpected superseded claim %+v", superseded)
	}

	results, err := store.SearchKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: "ws-claim-lifecycle",
		Query:       "runtime journal canonical",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search knowledge claims: %v", err)
	}
	if len(results) < 2 || results[0].ClaimID != claimB.ClaimID {
		t.Fatalf("expected confirmed claim B to rank first, got %+v", results)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-claim-lifecycle",
		EntityType:  "knowledge_claim",
		EntityID:    claimA.ClaimID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list claim runtime events: %v", err)
	}
	seen := map[string]bool{}
	for _, event := range events {
		seen[event.EventType] = true
	}
	for _, eventType := range []string{"knowledge_claim.review_requested", "knowledge_claim.confirmed", "knowledge_claim.disputed", "knowledge_claim.superseded"} {
		if !seen[eventType] {
			t.Fatalf("expected runtime event %s, got %+v", eventType, events)
		}
	}
}

func TestExecutionRunDetailAndRuntimeEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-control-exec",
		Title:       "Control Plane Execution",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-control-exec")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-control-exec",
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	run, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: "ws-control-exec",
		RunID:       "run-control-exec",
		AgentID:     "agent-a",
		Title:       "Bridge recovery rollout",
		Summary:     "Recover bridge state after reset.",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"source_queue_id": "queue-control-exec",
		},
	})
	if err != nil {
		t.Fatalf("upsert execution run: %v", err)
	}

	step, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		RunID:       run.RunID,
		WorkspaceID: run.WorkspaceID,
		Phase:       "EXECUTE",
		Title:       "Verify bridge wake path",
		Summary:     "Validate wake, ack, and dedupe after reset.",
		Status:      "ACTIVE",
		SortOrder:   10,
		Evidence:    []string{"doc:deploy/runbook", "artifact:bridge.log"},
		Verification: map[string]any{
			"health_url": "http://127.0.0.1:8420/health",
		},
	})
	if err != nil {
		t.Fatalf("record execution step: %v", err)
	}

	detail, err := store.GetExecutionRun(ctx, run.WorkspaceID, run.RunID)
	if err != nil {
		t.Fatalf("get execution run detail: %v", err)
	}
	if detail.TimeAuthority.WorkspaceID != run.WorkspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected execution run detail time authority, got %+v", detail.TimeAuthority)
	}
	if detail.Run.RunID != run.RunID || len(detail.Steps) != 1 {
		t.Fatalf("unexpected execution run detail %+v", detail)
	}
	if detail.Run.VerificationJSON["source_queue_id"] != "queue-control-exec" {
		t.Fatalf("unexpected execution run verification %+v", detail.Run.VerificationJSON)
	}
	if detail.Steps[0].StepID != step.StepID || detail.Steps[0].VerificationJSON["health_url"] != "http://127.0.0.1:8420/health" {
		t.Fatalf("unexpected execution step detail %+v", detail.Steps[0])
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-control-exec",
		EntityType:  "execution_step",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list execution step runtime events: %v", err)
	}
	if len(events) != 1 || events[0].EntityID != step.StepID {
		t.Fatalf("unexpected execution step runtime events %+v", events)
	}
}

func TestExecutionRunListFiltersAndOrdersMostRecentFirst(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-control-exec-list",
		Title:       "Control Plane Execution List",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-control-exec-list")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-control-exec-list",
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	runA, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: "ws-control-exec-list",
		RunID:       "run-a",
		AgentID:     "agent-a",
		Title:       "Bridge recovery rollout",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("upsert execution run a: %v", err)
	}
	runB, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: "ws-control-exec-list",
		RunID:       "run-b",
		AgentID:     "agent-a",
		Title:       "Bridge stabilization audit",
		Status:      "BLOCKED",
	})
	if err != nil {
		t.Fatalf("upsert execution run b: %v", err)
	}
	if _, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		RunID:       runA.RunID,
		WorkspaceID: runA.WorkspaceID,
		Phase:       "EXECUTE",
		Title:       "Touch run a to refresh sort order",
		Status:      "ACTIVE",
		SortOrder:   1,
	}); err != nil {
		t.Fatalf("record execution step for run a: %v", err)
	}

	items, err := store.ListExecutionRuns(ctx, sqlite.ExecutionRunFilter{
		WorkspaceID: "ws-control-exec-list",
		AgentID:     "agent-a",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list execution runs: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected two execution runs, got %+v", items)
	}
	if items[0].RunID != runA.RunID || items[1].RunID != runB.RunID {
		t.Fatalf("expected most recent execution run first, got %+v", items)
	}

	blocked, err := store.ListExecutionRuns(ctx, sqlite.ExecutionRunFilter{
		WorkspaceID: "ws-control-exec-list",
		AgentID:     "agent-a",
		Status:      "BLOCKED",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list blocked execution runs: %v", err)
	}
	if len(blocked) != 1 || blocked[0].RunID != runB.RunID {
		t.Fatalf("expected blocked run filter to return run-b, got %+v", blocked)
	}

	detail, err := store.GetExecutionRun(ctx, runA.WorkspaceID, runA.RunID)
	if err != nil {
		t.Fatalf("get execution run a: %v", err)
	}
	if detail.Run.RunID != runA.RunID || len(detail.Steps) != 1 {
		t.Fatalf("unexpected execution run detail %+v", detail)
	}
}

func TestCancelExecutionRunsForAgentStopTerminalizesRunsAndSteps(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-control-exec-stop"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Plane Execution Stop",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{"agent-stop", "agent-peer"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	runActive, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-stop-active",
		AgentID:     "agent-stop",
		Title:       "Active managed run",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("upsert active run: %v", err)
	}
	runVerifying, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-stop-verifying",
		AgentID:     "agent-stop",
		Title:       "Verifying managed run",
		Status:      "VERIFYING",
	})
	if err != nil {
		t.Fatalf("upsert verifying run: %v", err)
	}
	runPeer, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-peer",
		AgentID:     "agent-peer",
		Title:       "Peer run",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("upsert peer run: %v", err)
	}
	if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-stop-done",
		AgentID:     "agent-stop",
		Title:       "Already done run",
		Status:      "COMPLETED",
	}); err != nil {
		t.Fatalf("upsert completed run: %v", err)
	}
	for _, run := range []sqlite.ExecutionRunRecord{runActive, runVerifying, runPeer} {
		if _, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
			RunID:       run.RunID,
			WorkspaceID: workspaceID,
			Phase:       "EXECUTE",
			Title:       "Nonterminal step for " + run.RunID,
			Status:      "ACTIVE",
			SortOrder:   1,
		}); err != nil {
			t.Fatalf("record step for %s: %v", run.RunID, err)
		}
	}

	result, event, err := store.CancelExecutionRunsForAgentStopWithEvent(ctx, sqlite.ExecutionAgentRunsCancelInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-stop",
		ActorType:   "manager",
		ActorID:     "rhizome-bot",
		Summary:     "Stopped by test manager.",
	})
	if err != nil {
		t.Fatalf("cancel execution runs for stopped agent: %v", err)
	}
	if result.RunsCancelled != 2 || result.StepsCancelled != 2 || result.Outcome != "STOPPED_BY_MANAGER" || event.EventType != "workspace.execution.agent_runs.cancelled" {
		t.Fatalf("unexpected cancel result=%+v event=%+v", result, event)
	}
	for _, runID := range []string{"run-stop-active", "run-stop-verifying"} {
		detail, err := store.GetExecutionRun(ctx, workspaceID, runID)
		if err != nil {
			t.Fatalf("get cancelled run %s: %v", runID, err)
		}
		if detail.Run.Status != "CANCELLED" || detail.Run.Outcome != "STOPPED_BY_MANAGER" || detail.Run.ClosedAt == nil {
			t.Fatalf("run %s not terminalized: %+v", runID, detail.Run)
		}
		if len(detail.Steps) != 1 || detail.Steps[0].Status != "CANCELLED" || detail.Steps[0].CompletedAt == nil {
			t.Fatalf("run %s steps not terminalized: %+v", runID, detail.Steps)
		}
	}
	peerDetail, err := store.GetExecutionRun(ctx, workspaceID, runPeer.RunID)
	if err != nil {
		t.Fatalf("get peer run: %v", err)
	}
	if peerDetail.Run.Status != "ACTIVE" || len(peerDetail.Steps) != 1 || peerDetail.Steps[0].Status != "ACTIVE" {
		t.Fatalf("peer run should remain active: %+v", peerDetail)
	}
}

func TestExecutionRunAndStepWithEventReturnExactPersistedRowsOnRepeat(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-control-exec-exact"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Plane Execution Exact",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-exec-exact",
		OwnerUserID: "developer",
		DisplayName: "Execution Exact Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	firstRun, firstRunEvent, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-exact-row",
		AgentID:     "agent-exec-exact",
		Title:       "Exact execution run",
		Summary:     "first run summary",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("first upsert execution run with event: %v", err)
	}
	firstRunPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    firstRun.RunID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list first execution run runtime events: %v", err)
	}
	if len(firstRunPersisted) != 1 || firstRunPersisted[0] != firstRunEvent {
		t.Fatalf("expected first execution run exact runtime row, returned=%+v persisted=%+v", firstRunEvent, firstRunPersisted)
	}

	secondRun, secondRunEvent, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       firstRun.RunID,
		AgentID:     "agent-exec-exact",
		Title:       "Exact execution run",
		Summary:     "second run summary",
		Status:      "BLOCKED",
		Verification: map[string]any{
			"repair_tension_id": "tens-repair-exact-row",
		},
	})
	if err != nil {
		t.Fatalf("second upsert execution run with event: %v", err)
	}
	if secondRun.RunID != firstRun.RunID {
		t.Fatalf("expected repeated execution run write to preserve run id, first=%+v second=%+v", firstRun, secondRun)
	}
	secondRunPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    firstRun.RunID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second execution run runtime events: %v", err)
	}
	if len(secondRunPersisted) < 2 || secondRunPersisted[0] != secondRunEvent {
		t.Fatalf("expected second execution run exact runtime row, returned=%+v persisted=%+v", secondRunEvent, secondRunPersisted)
	}
	if secondRunEvent.EventID == firstRunEvent.EventID || secondRunEvent.IngestSeq <= firstRunEvent.IngestSeq {
		t.Fatalf("expected repeated execution run write to return newer runtime row, first=%+v second=%+v", firstRunEvent, secondRunEvent)
	}
	var runPayload map[string]any
	if err := json.Unmarshal([]byte(secondRunEvent.PayloadJSON), &runPayload); err != nil {
		t.Fatalf("decode execution run payload: %v", err)
	}
	if strings.TrimSpace(runPayload["summary"].(string)) != "second run summary" || strings.TrimSpace(runPayload["status"].(string)) != "BLOCKED" {
		t.Fatalf("expected second execution run payload to win, got %+v", runPayload)
	}
	verificationPayload, ok := runPayload["verification"].(map[string]any)
	if !ok || verificationPayload["repair_tension_id"] != "tens-repair-exact-row" {
		t.Fatalf("expected execution run verification payload to persist, got %+v", runPayload["verification"])
	}

	firstStep, firstStepEvent, err := store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		StepID:      "step-exact-row",
		RunID:       firstRun.RunID,
		WorkspaceID: workspaceID,
		Phase:       "EXECUTE",
		Title:       "Exact execution step",
		Summary:     "first step summary",
		Status:      "ACTIVE",
		SortOrder:   10,
	})
	if err != nil {
		t.Fatalf("first record execution step with event: %v", err)
	}
	firstStepPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    firstStep.StepID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list first execution step runtime events: %v", err)
	}
	if len(firstStepPersisted) != 1 || firstStepPersisted[0] != firstStepEvent {
		t.Fatalf("expected first execution step exact runtime row, returned=%+v persisted=%+v", firstStepEvent, firstStepPersisted)
	}

	secondStep, secondStepEvent, err := store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		StepID:      firstStep.StepID,
		RunID:       firstRun.RunID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Exact execution step",
		Summary:     "second step summary",
		Status:      "BLOCKED",
		SortOrder:   20,
	})
	if err != nil {
		t.Fatalf("second record execution step with event: %v", err)
	}
	if secondStep.StepID != firstStep.StepID {
		t.Fatalf("expected repeated execution step write to preserve step id, first=%+v second=%+v", firstStep, secondStep)
	}
	secondStepPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    firstStep.StepID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second execution step runtime events: %v", err)
	}
	if len(secondStepPersisted) < 2 || secondStepPersisted[0] != secondStepEvent {
		t.Fatalf("expected second execution step exact runtime row, returned=%+v persisted=%+v", secondStepEvent, secondStepPersisted)
	}
	if secondStepEvent.EventID == firstStepEvent.EventID || secondStepEvent.IngestSeq <= firstStepEvent.IngestSeq {
		t.Fatalf("expected repeated execution step write to return newer runtime row, first=%+v second=%+v", firstStepEvent, secondStepEvent)
	}
	var stepPayload map[string]any
	if err := json.Unmarshal([]byte(secondStepEvent.PayloadJSON), &stepPayload); err != nil {
		t.Fatalf("decode execution step payload: %v", err)
	}
	if strings.TrimSpace(stepPayload["summary"].(string)) != "second step summary" || strings.TrimSpace(stepPayload["phase"].(string)) != "VERIFY" || strings.TrimSpace(stepPayload["status"].(string)) != "BLOCKED" {
		t.Fatalf("expected second execution step payload to win, got %+v", stepPayload)
	}
}

func TestToolCallExecutionRunRejectsForeignStaleOrReceiptlessBinding(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-tool-exec-binding"
		ownerAgentID = "agent-tool-owner"
		peerAgentID  = "agent-tool-peer"
		taskID       = "task-tool-owner"
		sessionID    = "sess-tool-owner"
		parentRunID  = "run-tool-parent"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Tool Execution Binding",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{ownerAgentID, peerAgentID} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: taskID, AgentID: ownerAgentID}); err != nil {
		t.Fatalf("claim owner task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		WorkspaceID: workspaceID,
		AgentID:     ownerAgentID,
		SessionID:   sessionID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create owner session with receipt: %v", err)
	}
	if _, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID:           workspaceID,
		RunID:                 parentRunID,
		AgentID:               ownerAgentID,
		TaskID:                taskID,
		SessionID:             sessionID,
		Title:                 "Parent run",
		Status:                "ACTIVE",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.run.write", "server_rpc", workspaceID, "agent", ownerAgentID),
	}); err != nil {
		t.Fatalf("create parent run: %v", err)
	}

	validRun, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-tool-valid",
		AgentID:     ownerAgentID,
		TaskID:      taskID,
		SessionID:   sessionID,
		Title:       "Bound tool call",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"operation_ledger": map[string]any{
				"binding": map[string]any{
					"parent_run_id": parentRunID,
				},
			},
		},
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("tool.call", "server_operation_ledger", workspaceID, "agent", ownerAgentID),
	})
	if err != nil {
		t.Fatalf("valid tool call execution run should bind: %v", err)
	}
	proof, ok := validRun.VerificationJSON["live_execution_binding"].(map[string]any)
	if !ok ||
		strings.TrimSpace(fmt.Sprint(proof["claim_status_at_start"])) != model.TaskClaimStatusClaimed ||
		strings.TrimSpace(fmt.Sprint(proof["session_status_at_start"])) == "" ||
		strings.TrimSpace(fmt.Sprint(proof["parent_run_status_at_start"])) != "ACTIVE" {
		t.Fatalf("expected live execution binding proof on valid tool run, got %+v", validRun.VerificationJSON["live_execution_binding"])
	}

	_, _, err = store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID:           workspaceID,
		RunID:                 "run-workspace-omitted-agent-forged-principal",
		TaskID:                taskID,
		SessionID:             sessionID,
		Title:                 "Forged omitted agent",
		Status:                "ACTIVE",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.run.write", "server_rpc", workspaceID, "agent", peerAgentID),
	})
	if err == nil || !strings.Contains(err.Error(), "does not match record agent_id") {
		t.Fatalf("expected derived binding to reject forged prompt principal after agent fill, got %v", err)
	}

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-tool-exec-binding-other",
		Title:       "Other Tool Execution Binding",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create other workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-tool-exec-binding-other")
	_, _, err = store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: "ws-tool-exec-binding-other",
		RunID:       validRun.RunID,
		Title:       "Cross workspace duplicate run",
		Status:      "ACTIVE",
	})
	if !errors.Is(err, sqlite.ErrExecutionRunBindingInvalid) || !strings.Contains(err.Error(), "workspace mismatch") {
		t.Fatalf("expected cross-workspace run_id binding reject, got %v", err)
	}

	createWorkspaceTask(t, ctx, store, workspaceID, "task-tool-peer")
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: "task-tool-peer", AgentID: peerAgentID}); err != nil {
		t.Fatalf("claim peer task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		WorkspaceID: workspaceID,
		AgentID:     peerAgentID,
		SessionID:   "sess-tool-peer",
		TaskID:      "task-tool-peer",
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create peer session with receipt: %v", err)
	}

	cases := []struct {
		name      string
		runID     string
		agentID   string
		taskID    string
		sessionID string
	}{
		{
			name:      "foreign active session",
			runID:     "run-tool-foreign-session",
			agentID:   ownerAgentID,
			taskID:    "task-tool-peer",
			sessionID: "sess-tool-peer",
		},
		{
			name:      "mismatched task session",
			runID:     "run-tool-mismatched-task-session",
			agentID:   ownerAgentID,
			taskID:    "task-tool-peer",
			sessionID: sessionID,
		},
	}
	for _, tc := range cases {
		_, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
			WorkspaceID:           workspaceID,
			RunID:                 tc.runID,
			AgentID:               tc.agentID,
			TaskID:                tc.taskID,
			SessionID:             tc.sessionID,
			Title:                 tc.name,
			Status:                "ACTIVE",
			PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("tool.call", "server_operation_ledger", workspaceID, "agent", tc.agentID),
		})
		if !errors.Is(err, sqlite.ErrExecutionRunBindingInvalid) {
			t.Fatalf("%s: expected execution binding reject, got %v", tc.name, err)
		}
	}

	createWorkspaceTask(t, ctx, store, workspaceID, "task-tool-receiptless")
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: "task-tool-receiptless", AgentID: ownerAgentID}); err != nil {
		t.Fatalf("claim receiptless task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		WorkspaceID: workspaceID,
		AgentID:     ownerAgentID,
		SessionID:   "sess-tool-receiptless",
		TaskID:      "task-tool-receiptless",
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create receiptless raw session: %v", err)
	}
	_, _, err = store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID:           workspaceID,
		RunID:                 "run-tool-receiptless",
		AgentID:               ownerAgentID,
		TaskID:                "task-tool-receiptless",
		SessionID:             "sess-tool-receiptless",
		Title:                 "Receiptless session tool call",
		Status:                "ACTIVE",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("tool.call", "server_operation_ledger", workspaceID, "agent", ownerAgentID),
	})
	if !errors.Is(err, sqlite.ErrExecutionRunBindingInvalid) || !strings.Contains(err.Error(), "no durable start receipt") {
		t.Fatalf("expected receiptless session binding reject, got %v", err)
	}
	forgedLiveBindingProof := map[string]any{
		"live_execution_binding": map[string]any{
			"binding_receipt_contract": "execution_run.live_binding.v1",
			"workspace_id":             workspaceID,
			"agent_id":                 ownerAgentID,
			"task_id":                  "task-tool-receiptless",
			"session_id":               "sess-tool-receiptless",
		},
	}
	_, _, err = store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID:           workspaceID,
		RunID:                 "run-tool-receiptless-forged-proof",
		AgentID:               ownerAgentID,
		TaskID:                "task-tool-receiptless",
		SessionID:             "sess-tool-receiptless",
		Title:                 "Receiptless session forged live proof",
		Status:                "ACTIVE",
		Verification:          forgedLiveBindingProof,
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("tool.call", "server_operation_ledger", workspaceID, "agent", ownerAgentID),
	})
	if !errors.Is(err, sqlite.ErrExecutionRunBindingInvalid) || !strings.Contains(err.Error(), "no durable start receipt") {
		t.Fatalf("expected caller-supplied live binding proof to be ignored and receiptless session rejected, got %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO execution_runs(run_id, workspace_id, task_id, session_id, agent_id, title, summary, status, outcome, verification_json, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, '', 'ACTIVE', '', '{}', ?, ?)`,
		"run-legacy-active-without-binding-proof", workspaceID, taskID, sessionID, ownerAgentID, "Legacy active without binding proof", now, now,
	); err != nil {
		t.Fatalf("insert legacy active execution run: %v", err)
	}
	if _, err := store.ReleaseTaskClaimWithEvent(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     ownerAgentID,
		Reason:      "test release before stale tool call",
	}); err != nil {
		t.Fatalf("release owner task: %v", err)
	}
	_, _, err = store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID:           workspaceID,
		RunID:                 "run-tool-released-claim",
		AgentID:               ownerAgentID,
		TaskID:                taskID,
		SessionID:             sessionID,
		Title:                 "Released claim tool call",
		Status:                "ACTIVE",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("tool.call", "server_operation_ledger", workspaceID, "agent", ownerAgentID),
	})
	if !errors.Is(err, sqlite.ErrExecutionRunBindingInvalid) || !strings.Contains(err.Error(), "not live same-owner") {
		t.Fatalf("expected released claim binding reject, got %v", err)
	}

	_, _, err = store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID:           workspaceID,
		RunID:                 "run-legacy-active-without-binding-proof",
		AgentID:               ownerAgentID,
		TaskID:                taskID,
		SessionID:             sessionID,
		Title:                 "Legacy active terminal close",
		Status:                "COMPLETED",
		Outcome:               "ok",
		Verification:          forgedLiveBindingProof,
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.run.write", "server_rpc", workspaceID, "agent", ownerAgentID),
	})
	if !errors.Is(err, sqlite.ErrExecutionRunBindingInvalid) || !strings.Contains(err.Error(), "not live same-owner") {
		t.Fatalf("expected legacy terminal close without original binding proof to revalidate and reject, got %v", err)
	}
}

func TestBlockedExecutionRunBindingAllowsSameOwnerBlockedClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-blocked-exec-binding"
		agentID     = "agent-zeta"
		taskID      = "task-integrate"
		sessionID   = "sess-integrate"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Blocked Execution Binding",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		OwnerUserID:     "owner-zeta",
		DisplayName:     "Zeta",
		Role:            "integrator",
		Status:          model.AgentStatusActive,
		ProtocolVersion: "rnar/v1",
		Capabilities:    []string{"tool.call"},
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: taskID, AgentID: agentID}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		SessionID:   sessionID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session with receipt: %v", err)
	}
	if _, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID:           workspaceID,
		RunID:                 "run-active-before-block",
		AgentID:               agentID,
		TaskID:                taskID,
		SessionID:             sessionID,
		Title:                 "Active integration run",
		Status:                "ACTIVE",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.run.write", "server_rpc", workspaceID, "agent", agentID),
	}); err != nil {
		t.Fatalf("write active execution run: %v", err)
	}
	blockEvent, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "blocked by negative same-head review",
	})
	if err != nil {
		t.Fatalf("block task: %v", err)
	}
	var blockPayload map[string]any
	decodeRuntimePayload(t, blockEvent.PayloadJSON, &blockPayload)
	if blockPayload["execution_runs_closed"] != true {
		t.Fatalf("expected task.blocked to close in-flight execution runs, got payload %+v", blockPayload)
	}
	detail, err := store.GetExecutionRun(ctx, workspaceID, "run-active-before-block")
	if err != nil {
		t.Fatalf("get blocked active run: %v", err)
	}
	if detail.Run.Status != "BLOCKED" || detail.Run.Outcome != "BLOCKED" {
		t.Fatalf("expected active run to be marked BLOCKED after task.blocked, got status=%s outcome=%s", detail.Run.Status, detail.Run.Outcome)
	}

	blockedRun, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID:           workspaceID,
		RunID:                 "run-blocked-after-claim",
		AgentID:               agentID,
		TaskID:                taskID,
		SessionID:             sessionID,
		Title:                 "Blocked integration evidence",
		Status:                "BLOCKED",
		Outcome:               "BLOCKED",
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("workspace.execution.run.write", "server_rpc", workspaceID, "agent", agentID),
	})
	if err != nil {
		t.Fatalf("write blocked execution run after blocked claim: %v", err)
	}
	proof, ok := blockedRun.VerificationJSON["live_execution_binding"].(map[string]any)
	if !ok {
		t.Fatalf("expected blocked binding proof, got %+v", blockedRun.VerificationJSON)
	}
	if got := strings.TrimSpace(fmt.Sprint(proof["binding_receipt_contract"])); got != "execution_run.blocked_binding.v1" {
		t.Fatalf("expected blocked binding contract, got %q proof=%+v", got, proof)
	}
	if got := strings.TrimSpace(fmt.Sprint(proof["claim_status_at_start"])); got != model.TaskClaimStatusBlocked {
		t.Fatalf("expected blocked claim status in proof, got %q proof=%+v", got, proof)
	}
}

func TestExecutionRunPreservesVerificationWhenOmittedOnRepeat(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-control-exec-verification-preserve"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Execution Run Verification Preserve",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	firstRun, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-preserve-verification",
		Title:       "Exact execution run",
		Status:      "ACTIVE",
		Verification: map[string]any{
			"source_queue_id": "queue-preserve-verification",
		},
	})
	if err != nil {
		t.Fatalf("first upsert execution run: %v", err)
	}

	secondRun, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       firstRun.RunID,
		Title:       "Exact execution run",
		Status:      "FAILED",
	})
	if err != nil {
		t.Fatalf("second upsert execution run: %v", err)
	}

	if secondRun.VerificationJSON["source_queue_id"] != "queue-preserve-verification" {
		t.Fatalf("expected repeated execution run write to preserve verification, got %+v", secondRun.VerificationJSON)
	}
}

func TestReplayRuntimeJournalEvaluatesCoordinationGaps(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-control-replay",
		Title:       "Control Plane Replay",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-control-replay",
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "Replay Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	keepAlive := true
	sessionBlockedPayload, err := json.Marshal(sqlite.AgentSessionStateRecord{
		SessionID:         "session-blocked",
		WorkspaceID:       "ws-control-replay",
		AgentID:           "agent-a",
		Status:            model.SessionStatusBlocked,
		Summary:           "Waiting for bridge wake acknowledgement",
		BlockedOn:         []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake ack timeout"}},
		KeepSessionActive: &keepAlive,
		UpdatedAt:         "2026-03-22T10:00:00Z",
		StartedAt:         "2026-03-22T09:55:00Z",
	})
	if err != nil {
		t.Fatalf("marshal blocked session payload: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-blocked",
		WorkspaceID: "ws-control-replay",
		EventType:   "session.blocked",
		EntityType:  "agent_session",
		EntityID:    "session-blocked",
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		PayloadJSON: string(sessionBlockedPayload),
		CreatedAt:   "2026-03-22T10:00:00Z",
	}); err != nil {
		t.Fatalf("record blocked session event: %v", err)
	}

	endedKeepAlive := false
	sessionEndedPayload, err := json.Marshal(sqlite.AgentSessionStateRecord{
		SessionID:         "session-ended",
		WorkspaceID:       "ws-control-replay",
		AgentID:           "agent-a",
		Status:            model.SessionStatusEnded,
		Summary:           "Rollout finished",
		KeepSessionActive: &endedKeepAlive,
		UpdatedAt:         "2026-03-22T10:05:00Z",
		StartedAt:         "2026-03-22T09:40:00Z",
		CompletedAt:       "2026-03-22T10:05:00Z",
	})
	if err != nil {
		t.Fatalf("marshal ended session payload: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-ended",
		WorkspaceID: "ws-control-replay",
		EventType:   "session.end",
		EntityType:  "agent_session",
		EntityID:    "session-ended",
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		PayloadJSON: string(sessionEndedPayload),
		CreatedAt:   "2026-03-22T10:05:00Z",
	}); err != nil {
		t.Fatalf("record ended session event: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-run",
		WorkspaceID: "ws-control-replay",
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    "run-stale",
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		PayloadJSON: `{"status":"ACTIVE","title":"Verify bridge recovery","summary":"Still marked active after session end.","session_id":"session-ended","agent_id":"agent-a"}`,
		CreatedAt:   "2026-03-22T10:06:00Z",
	}); err != nil {
		t.Fatalf("record execution run event: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-claim",
		WorkspaceID: "ws-control-replay",
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim-runtime-gap",
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		PayloadJSON: `{"claim_type":"DECISION","status":"ACTIVE","subject":"Runtime journal is canonical","summary":"Use runtime events instead of archived traces.","source_kind":"workspace_memory","confidence":0.9}`,
		CreatedAt:   "2026-03-22T10:07:00Z",
	}); err != nil {
		t.Fatalf("record knowledge claim event: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-control-replay",
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.Truncated {
		t.Fatalf("expected replay report to be complete, got %+v", report)
	}
	if len(report.Sessions) != 2 || len(report.ExecutionRuns) != 1 || len(report.Claims) != 1 {
		t.Fatalf("unexpected replay report counts %+v", report)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected warn replay verdict, got %+v", report.Evaluation)
	}

	findingCodes := map[string]bool{}
	for _, finding := range report.Evaluation.Findings {
		findingCodes[finding.Code] = true
	}
	for _, code := range []string{
		"missing_operator_queue",
		"missing_execution_run",
		"execution_run_out_of_sync",
		"execution_run_without_steps",
		"claim_missing_memory_link",
	} {
		if !findingCodes[code] {
			t.Fatalf("expected replay finding %s, got %+v", code, report.Evaluation.Findings)
		}
	}
}

func TestRuntimeEventCanonicalEnvelopeRoundTripsNewFields(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-envelope-roundtrip",
		Title:       "Runtime Envelope Roundtrip",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	seedRuntimeEventParents(t, ctx, store, "ws-runtime-envelope-roundtrip", "rtev-parent-a")

	record, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-envelope-roundtrip",
		DedupKey:          "D-envelope-1",
		WorkspaceID:       "ws-runtime-envelope-roundtrip",
		EventType:         "runtime.envelope",
		EntityType:        "runtime_event",
		EntityID:          "runtime-envelope",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "RC-envelope-1",
		ProvenanceGroupID: "PG-envelope-1",
		ParentRefsJSON:    `["rtev-parent-a"]`,
		PayloadJSON:       `{"message":"canonical envelope","dedup_key":"D-envelope-1","root_cause_id":"RC-envelope-1","provenance_group_id":"PG-envelope-1","parent_refs_json":["rtev-parent-a"]}`,
		CreatedAt:         "2026-03-22T13:00:00Z",
	})
	if err != nil {
		t.Fatalf("record runtime event: %v", err)
	}
	if record.EventID != "rtev-envelope-roundtrip" {
		t.Fatalf("expected event_id to round-trip, got %+v", record)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-runtime-envelope-roundtrip",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 2 || events[0].EventID != record.EventID {
		t.Fatalf("expected canonical runtime event followed by seeded parent, got %+v", events)
	}
	if events[0].DedupKey != "D-envelope-1" || events[0].RootCauseID != "RC-envelope-1" || events[0].ProvenanceGroupID != "PG-envelope-1" {
		t.Fatalf("expected canonical envelope fields to round-trip, got %+v", events[0])
	}
	if events[0].ParentRefsJSON != `["rtev-parent-a"]` {
		t.Fatalf("expected parent refs json to round-trip, got %+v", events[0])
	}

	var envelope struct {
		Message           string   `json:"message"`
		DedupKey          string   `json:"dedup_key"`
		RootCauseID       string   `json:"root_cause_id"`
		ProvenanceGroupID string   `json:"provenance_group_id"`
		ParentRefsJSON    []string `json:"parent_refs_json"`
	}
	decodeRuntimePayload(t, events[0].PayloadJSON, &envelope)
	if envelope.DedupKey != "D-envelope-1" || envelope.RootCauseID != "RC-envelope-1" || envelope.ProvenanceGroupID != "PG-envelope-1" {
		t.Fatalf("unexpected canonical envelope payload %+v", envelope)
	}
	if len(envelope.ParentRefsJSON) != 1 || envelope.ParentRefsJSON[0] != "rtev-parent-a" {
		t.Fatalf("expected parent refs to round-trip, got %+v", envelope.ParentRefsJSON)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-envelope-roundtrip",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.Evaluation.Verdict != "pass" {
		t.Fatalf("expected replay verdict to stay pass for additive envelope, got %+v", report.Evaluation)
	}
	if len(report.Events) != 2 || report.Events[0].EventID != record.EventID {
		t.Fatalf("expected replay to preserve runtime event identity, got %+v", report.Events)
	}
	if report.Events[0].DedupKey != record.DedupKey || report.Events[0].RootCauseID != record.RootCauseID || report.Events[0].ProvenanceGroupID != record.ProvenanceGroupID {
		t.Fatalf("expected replay to preserve canonical runtime envelope fields, got %+v", report.Events[0])
	}
	if report.Events[0].ParentRefsJSON != record.ParentRefsJSON {
		t.Fatalf("expected replay to preserve parent refs json, got %+v", report.Events[0])
	}
}

func TestRuntimeEventCanonicalEnvelopeEquivalentWritesAreIdempotent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-envelope-idempotent",
		Title:       "Runtime Envelope Idempotent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	first, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-envelope-idempotent-a",
		WorkspaceID: "ws-runtime-envelope-idempotent",
		EventType:   "runtime.envelope",
		EntityType:  "runtime_event",
		EntityID:    "runtime-envelope",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"message":"canonical envelope","dedup_key":"D-envelope-2","root_cause_id":"RC-envelope-2","provenance_group_id":"PG-envelope-2"}`,
		CreatedAt:   "2026-03-22T13:10:00Z",
	})
	if err != nil {
		t.Fatalf("record first canonical runtime event: %v", err)
	}
	second, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-envelope-idempotent-b",
		DedupKey:          "D-envelope-2",
		WorkspaceID:       "ws-runtime-envelope-idempotent",
		EventType:         "runtime.envelope",
		EntityType:        "runtime_event",
		EntityID:          "runtime-envelope",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "RC-envelope-2",
		ProvenanceGroupID: "PG-envelope-2",
		PayloadJSON:       `{"provenance_group_id":"PG-envelope-2","root_cause_id":"RC-envelope-2","dedup_key":"D-envelope-2","message":"canonical envelope"}`,
		CreatedAt:         "2026-03-22T13:11:00Z",
	})
	if err != nil {
		t.Fatalf("record second canonical runtime event: %v", err)
	}
	if second.EventID != first.EventID {
		t.Fatalf("expected equivalent canonical event write to be idempotent, first=%+v second=%+v", first, second)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-runtime-envelope-idempotent",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != first.EventID {
		t.Fatalf("expected only one stored runtime event after idempotent write, got %+v", events)
	}
	if events[0].DedupKey != "D-envelope-2" || events[0].ParentRefsJSON != "[]" {
		t.Fatalf("expected idempotent runtime event to preserve canonical fields, got %+v", events[0])
	}
}

func TestRuntimeEventCanonicalEnvelopeConflictingWritesFail(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-envelope-conflict",
		Title:       "Runtime Envelope Conflict",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-envelope-conflict-a",
		WorkspaceID: "ws-runtime-envelope-conflict",
		EventType:   "runtime.envelope",
		EntityType:  "runtime_event",
		EntityID:    "runtime-envelope",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"message":"canonical envelope","dedup_key":"D-envelope-3","root_cause_id":"RC-envelope-3","provenance_group_id":"PG-envelope-3"}`,
		CreatedAt:   "2026-03-22T13:20:00Z",
	}); err != nil {
		t.Fatalf("record canonical runtime event: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-envelope-conflict-b",
		DedupKey:          "D-envelope-3",
		WorkspaceID:       "ws-runtime-envelope-conflict",
		EventType:         "runtime.envelope",
		EntityType:        "runtime_event",
		EntityID:          "runtime-envelope",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "RC-envelope-3",
		ProvenanceGroupID: "PG-envelope-3",
		PayloadJSON:       `{"message":"canonical envelope changed","dedup_key":"D-envelope-3","root_cause_id":"RC-envelope-3","provenance_group_id":"PG-envelope-3"}`,
		CreatedAt:         "2026-03-22T13:21:00Z",
	}); err == nil {
		t.Fatal("expected conflicting runtime event write to fail")
	} else if !strings.Contains(err.Error(), "dedup_key") {
		t.Fatalf("expected dedup_key conflict, got %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-runtime-envelope-conflict",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected a single canonical runtime event after conflict rejection, got %+v", events)
	}
}

func TestRuntimeEventParentRefsSetEquivalentWritesReuseExistingEventWhenCanonicalized(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-parent-set-idempotent",
		Title:       "Runtime Parent Set Idempotent",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	seedRuntimeEventParents(t, ctx, store, "ws-runtime-parent-set-idempotent", "rtev-parent-a", "rtev-parent-b")

	t.Run("dedup key reuse", func(t *testing.T) {
		first, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:           "rtev-parent-set-dedup-a",
			DedupKey:          "D-parent-set-1",
			WorkspaceID:       "ws-runtime-parent-set-idempotent",
			EventType:         "runtime.envelope",
			EntityType:        "runtime_event",
			EntityID:          "runtime-parent-set-dedup",
			ActorType:         "system",
			ActorID:           "tests",
			RootCauseID:       "RC-parent-set-1",
			ProvenanceGroupID: "PG-parent-set-1",
			ParentRefsJSON:    `["rtev-parent-a","rtev-parent-b"]`,
			PayloadJSON:       `{"message":"canonical parent set","dedup_key":"D-parent-set-1","root_cause_id":"RC-parent-set-1","provenance_group_id":"PG-parent-set-1","parent_refs_json":["rtev-parent-a","rtev-parent-b"]}`,
			CreatedAt:         "2026-03-23T10:00:00Z",
		})
		if err != nil {
			t.Fatalf("record first parent-set event: %v", err)
		}

		second, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:           "rtev-parent-set-dedup-b",
			DedupKey:          "D-parent-set-1",
			WorkspaceID:       "ws-runtime-parent-set-idempotent",
			EventType:         "runtime.envelope",
			EntityType:        "runtime_event",
			EntityID:          "runtime-parent-set-dedup",
			ActorType:         "system",
			ActorID:           "tests",
			RootCauseID:       "RC-parent-set-1",
			ProvenanceGroupID: "PG-parent-set-1",
			ParentRefsJSON:    `["rtev-parent-b","rtev-parent-a"]`,
			PayloadJSON:       `{"message":"canonical parent set","dedup_key":"D-parent-set-1","root_cause_id":"RC-parent-set-1","provenance_group_id":"PG-parent-set-1","parent_refs_json":["rtev-parent-b","rtev-parent-a"]}`,
			CreatedAt:         "2026-03-23T10:01:00Z",
		})
		if err != nil {
			t.Fatalf("record second parent-set event: %v", err)
		}
		if second.EventID != first.EventID {
			t.Fatalf("expected reordered parent_refs to reuse canonical event, first=%+v second=%+v", first, second)
		}
	})

	t.Run("event id reuse", func(t *testing.T) {
		first, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:           "rtev-parent-set-event-id",
			WorkspaceID:       "ws-runtime-parent-set-idempotent",
			EventType:         "runtime.envelope",
			EntityType:        "runtime_event",
			EntityID:          "runtime-parent-set-event-id",
			ActorType:         "system",
			ActorID:           "tests",
			RootCauseID:       "RC-parent-set-2",
			ProvenanceGroupID: "PG-parent-set-2",
			ParentRefsJSON:    `["rtev-parent-a","rtev-parent-b"]`,
			PayloadJSON:       `{"message":"canonical parent set by event_id","root_cause_id":"RC-parent-set-2","provenance_group_id":"PG-parent-set-2","parent_refs_json":["rtev-parent-a","rtev-parent-b"]}`,
			CreatedAt:         "2026-03-23T10:02:00Z",
		})
		if err != nil {
			t.Fatalf("record event-id parent-set event: %v", err)
		}

		second, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
			EventID:           "rtev-parent-set-event-id",
			WorkspaceID:       "ws-runtime-parent-set-idempotent",
			EventType:         "runtime.envelope",
			EntityType:        "runtime_event",
			EntityID:          "runtime-parent-set-event-id",
			ActorType:         "system",
			ActorID:           "tests",
			RootCauseID:       "RC-parent-set-2",
			ProvenanceGroupID: "PG-parent-set-2",
			ParentRefsJSON:    `["rtev-parent-b","rtev-parent-a"]`,
			PayloadJSON:       `{"message":"canonical parent set by event_id","root_cause_id":"RC-parent-set-2","provenance_group_id":"PG-parent-set-2","parent_refs_json":["rtev-parent-b","rtev-parent-a"]}`,
			CreatedAt:         "2026-03-23T10:03:00Z",
		})
		if err != nil {
			t.Fatalf("record reordered parent-set event-id write: %v", err)
		}
		if second.EventID != first.EventID {
			t.Fatalf("expected reordered parent_refs to reuse existing event_id row, first=%+v second=%+v", first, second)
		}
	})
}

func TestRuntimeEventParentRefsDuplicateRefsCollapseWhenCanonicalized(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-parent-dup-collapse",
		Title:       "Runtime Parent Duplicate Collapse",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	seedRuntimeEventParents(t, ctx, store, "ws-runtime-parent-dup-collapse", "rtev-parent-a", "rtev-parent-b")

	record, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-parent-dup-collapse",
		DedupKey:          "D-parent-dup-collapse",
		WorkspaceID:       "ws-runtime-parent-dup-collapse",
		EventType:         "runtime.envelope",
		EntityType:        "runtime_event",
		EntityID:          "runtime-parent-dup-collapse",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "RC-parent-dup-collapse",
		ProvenanceGroupID: "PG-parent-dup-collapse",
		ParentRefsJSON:    `["rtev-parent-a","rtev-parent-b","rtev-parent-a","rtev-parent-b"]`,
		PayloadJSON:       `{"message":"duplicate parent refs should collapse","dedup_key":"D-parent-dup-collapse","root_cause_id":"RC-parent-dup-collapse","provenance_group_id":"PG-parent-dup-collapse","parent_refs_json":["rtev-parent-b","rtev-parent-a","rtev-parent-b","rtev-parent-a"]}`,
		CreatedAt:         "2026-03-23T10:10:00Z",
	})
	if err != nil {
		t.Fatalf("record duplicate parent refs runtime event: %v", err)
	}

	parentRefs := decodeRuntimeEventParentRefsSet(t, record.ParentRefsJSON)
	if len(parentRefs) != 2 || parentRefs["rtev-parent-a"] != 1 || parentRefs["rtev-parent-b"] != 1 {
		t.Fatalf("expected duplicate parent refs to canonicalize to a unique set, got %+v", record)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-runtime-parent-dup-collapse",
		EntityType:  "runtime_event",
		EntityID:    "runtime-parent-dup-collapse",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list duplicate parent refs runtime event: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected a single canonical duplicate-collapse event, got %+v", events)
	}
	if got := decodeRuntimeEventParentRefsSet(t, events[0].ParentRefsJSON); len(got) != 2 || got["rtev-parent-a"] != 1 || got["rtev-parent-b"] != 1 {
		t.Fatalf("expected stored parent refs to stay deduplicated, got %+v", events[0])
	}
}

func TestRuntimeEventParentRefsRequireExistingSameWorkspaceEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-parent-validation",
		Title:       "Runtime Parent Validation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-parent-valid",
		WorkspaceID: "ws-runtime-parent-validation",
		EventType:   "runtime.parent",
		EntityType:  "runtime_event_parent",
		EntityID:    "rtev-parent-valid",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"seed":"parent"}`,
		CreatedAt:   "2026-03-22T12:59:00Z",
	}); err != nil {
		t.Fatalf("record valid parent event: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:        "rtev-child-missing",
		WorkspaceID:    "ws-runtime-parent-validation",
		EventType:      "runtime.child",
		EntityType:     "runtime_event",
		EntityID:       "child-missing",
		ActorType:      "system",
		ActorID:        "tests",
		ParentRefsJSON: `["rtev-parent-missing"]`,
		PayloadJSON:    `{"message":"missing parent"}`,
		CreatedAt:      "2026-03-22T13:00:00Z",
	}); err == nil {
		t.Fatal("expected missing parent_ref validation error")
	} else if !strings.Contains(err.Error(), "parent_ref") || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected missing parent_ref error, got %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:        "rtev-child-valid",
		WorkspaceID:    "ws-runtime-parent-validation",
		EventType:      "runtime.child",
		EntityType:     "runtime_event",
		EntityID:       "child-valid",
		ActorType:      "system",
		ActorID:        "tests",
		ParentRefsJSON: `["rtev-parent-valid"]`,
		PayloadJSON:    `{"message":"valid parent"}`,
		CreatedAt:      "2026-03-22T13:01:00Z",
	}); err != nil {
		t.Fatalf("expected same-workspace parent_ref to validate, got %v", err)
	}
}

func TestRuntimeEventParentRefsRejectSelfCrossWorkspaceAndCycles(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-parent-a",
		Title:       "Runtime Parent A",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace a: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-parent-b",
		Title:       "Runtime Parent B",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace b: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-parent-cross",
		WorkspaceID: "ws-runtime-parent-b",
		EventType:   "runtime.parent",
		EntityType:  "runtime_event_parent",
		EntityID:    "rtev-parent-cross",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"seed":"cross-workspace-parent"}`,
		CreatedAt:   "2026-03-22T12:00:00Z",
	}); err != nil {
		t.Fatalf("record cross-workspace parent event: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:        "rtev-self-parent",
		WorkspaceID:    "ws-runtime-parent-a",
		EventType:      "runtime.child",
		EntityType:     "runtime_event",
		EntityID:       "self-parent",
		ActorType:      "system",
		ActorID:        "tests",
		ParentRefsJSON: `["rtev-self-parent"]`,
		PayloadJSON:    `{"message":"self parent"}`,
		CreatedAt:      "2026-03-22T13:00:00Z",
	}); err == nil {
		t.Fatal("expected self parent_ref validation error")
	} else if !strings.Contains(err.Error(), "must not reference event_id") {
		t.Fatalf("expected self parent_ref error, got %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:        "rtev-cross-parent-child",
		WorkspaceID:    "ws-runtime-parent-a",
		EventType:      "runtime.child",
		EntityType:     "runtime_event",
		EntityID:       "cross-parent",
		ActorType:      "system",
		ActorID:        "tests",
		ParentRefsJSON: `["rtev-parent-cross"]`,
		PayloadJSON:    `{"message":"cross workspace parent"}`,
		CreatedAt:      "2026-03-22T13:01:00Z",
	}); err == nil {
		t.Fatal("expected cross-workspace parent_ref validation error")
	} else if !strings.Contains(err.Error(), "not found in workspace runtime journal") {
		t.Fatalf("expected cross-workspace parent_ref rejection, got %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO runtime_events(
		event_id, dedup_key, workspace_id, event_type, entity_type, entity_id,
		actor_type, actor_id, agent_id, session_id, task_id,
		root_cause_id, provenance_group_id, parent_refs_json, payload_json, created_at, ingest_seq
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rtev-cycle-parent",
		nil,
		"ws-runtime-parent-a",
		"legacy.signal",
		"legacy_event",
		"legacy-cycle-parent",
		"system",
		"tests",
		nil,
		nil,
		nil,
		nil,
		nil,
		`["rtev-cycle-child"]`,
		`{"message":"legacy cycle parent"}`,
		"2026-03-22T12:58:00Z",
		1,
	); err != nil {
		t.Fatalf("insert legacy cycle parent event: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:        "rtev-cycle-child",
		WorkspaceID:    "ws-runtime-parent-a",
		EventType:      "runtime.child",
		EntityType:     "runtime_event",
		EntityID:       "cycle-child",
		ActorType:      "system",
		ActorID:        "tests",
		ParentRefsJSON: `["rtev-cycle-parent"]`,
		PayloadJSON:    `{"message":"cycle child"}`,
		CreatedAt:      "2026-03-22T13:02:00Z",
	}); err == nil {
		t.Fatal("expected lineage cycle validation error")
	} else if !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected cycle validation error, got %v", err)
	}
}

func TestRuntimeEventParentRefsCanonicalizeAsSetAndPreserveDedupEquivalence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-parent-canonical",
		Title:       "Runtime Parent Canonical",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, parent := range []sqlite.RuntimeEventInput{
		{
			EventID:     "rtev-parent-a",
			WorkspaceID: "ws-runtime-parent-canonical",
			EventType:   "runtime.parent",
			EntityType:  "runtime_event_parent",
			EntityID:    "parent-a",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"seed":"parent-a"}`,
			CreatedAt:   "2026-03-22T12:00:00Z",
		},
		{
			EventID:     "rtev-parent-b",
			WorkspaceID: "ws-runtime-parent-canonical",
			EventType:   "runtime.parent",
			EntityType:  "runtime_event_parent",
			EntityID:    "parent-b",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"seed":"parent-b"}`,
			CreatedAt:   "2026-03-22T12:01:00Z",
		},
	} {
		if _, err := store.RecordRuntimeEvent(ctx, parent); err != nil {
			t.Fatalf("record parent %s: %v", parent.EventID, err)
		}
	}

	first, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-parent-set-a",
		DedupKey:          "D-parent-set-1",
		WorkspaceID:       "ws-runtime-parent-canonical",
		EventType:         "runtime.child",
		EntityType:        "runtime_event",
		EntityID:          "child-parent-set",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "root-parent-set-1",
		ProvenanceGroupID: "prov-parent-set-1",
		ParentRefsJSON:    `["rtev-parent-b","rtev-parent-a"]`,
		PayloadJSON:       `{"message":"canonical parent set"}`,
		CreatedAt:         "2026-03-22T12:02:00Z",
	})
	if err != nil {
		t.Fatalf("record canonical parent-set runtime event: %v", err)
	}
	if first.ParentRefsJSON != `["rtev-parent-a","rtev-parent-b"]` {
		t.Fatalf("expected parent refs to canonicalize as sorted set, got %+v", first)
	}

	second, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-parent-set-b",
		DedupKey:          "D-parent-set-1",
		WorkspaceID:       "ws-runtime-parent-canonical",
		EventType:         "runtime.child",
		EntityType:        "runtime_event",
		EntityID:          "child-parent-set",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "root-parent-set-1",
		ProvenanceGroupID: "prov-parent-set-1",
		ParentRefsJSON:    `["rtev-parent-a","rtev-parent-b","rtev-parent-a"]`,
		PayloadJSON:       `{"message":"canonical parent set"}`,
		CreatedAt:         "2026-03-22T12:03:00Z",
	})
	if err != nil {
		t.Fatalf("record equivalent parent-set runtime event: %v", err)
	}
	if second.EventID != first.EventID || second.IngestSeq != first.IngestSeq {
		t.Fatalf("expected dedup-equivalent parent-set event to reuse existing row, first=%+v second=%+v", first, second)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-runtime-parent-canonical",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected two parents plus one canonical child event, got %+v", events)
	}
	for _, event := range events {
		if event.EventID == first.EventID && event.ParentRefsJSON != `["rtev-parent-a","rtev-parent-b"]` {
			t.Fatalf("expected persisted parent refs to stay canonicalized, got %+v", event)
		}
	}
}

func TestCapabilityPolicySpecificityAndStrength(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-control-policy",
		Title:       "Control Plane Policy",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-control-policy")

	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: "ws-control-policy",
		SubjectType: "agent",
		Capability:  "tool.call",
		Effect:      "ALLOW",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("put allow capability policy: %v", err)
	}
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: "ws-control-policy",
		SubjectType: "agent",
		Capability:  "tool.call",
		ToolID:      "dangerous-tool",
		Effect:      "REQUIRE_APPROVAL",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("put require approval capability policy: %v", err)
	}
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: "ws-control-policy",
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "dangerous-tool",
		Effect:      "DENY",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("put deny capability policy: %v", err)
	}

	denyCheck, err := store.CheckCapabilityPolicy(ctx, sqlite.CapabilityCheckInput{
		WorkspaceID: "ws-control-policy",
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "dangerous-tool",
	})
	if err != nil {
		t.Fatalf("check deny capability policy: %v", err)
	}
	if denyCheck.Verdict != "DENY" || denyCheck.MatchedPolicy == nil || denyCheck.MatchedPolicy.SubjectID != "agent-a" {
		t.Fatalf("unexpected deny check %+v", denyCheck)
	}

	approvalCheck, err := store.CheckCapabilityPolicy(ctx, sqlite.CapabilityCheckInput{
		WorkspaceID: "ws-control-policy",
		SubjectType: "agent",
		SubjectID:   "agent-b",
		Capability:  "tool.call",
		ToolID:      "dangerous-tool",
	})
	if err != nil {
		t.Fatalf("check require approval capability policy: %v", err)
	}
	if approvalCheck.Verdict != "REQUIRE_APPROVAL" {
		t.Fatalf("expected REQUIRE_APPROVAL, got %+v", approvalCheck)
	}

	allowCheck, err := store.CheckCapabilityPolicy(ctx, sqlite.CapabilityCheckInput{
		WorkspaceID: "ws-control-policy",
		SubjectType: "agent",
		SubjectID:   "agent-b",
		Capability:  "tool.call",
		ToolID:      "safe-tool",
	})
	if err != nil {
		t.Fatalf("check allow capability policy: %v", err)
	}
	if allowCheck.Verdict != "ALLOW" {
		t.Fatalf("expected ALLOW, got %+v", allowCheck)
	}
}

func TestCapabilityPolicyWithEventReturnsExactPersistedRowOnRepeat(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-control-policy-exact"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Control Plane Policy Exact",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	firstPolicy, firstEvent, err := store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "dangerous-tool",
		Effect:      "DENY",
		Reason:      "first policy",
		CreatedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("first put capability policy with event: %v", err)
	}
	firstPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		EntityID:    firstPolicy.PolicyID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list first capability policy runtime events: %v", err)
	}
	if len(firstPersisted) != 1 || firstPersisted[0] != firstEvent {
		t.Fatalf("expected first capability policy exact runtime row, returned=%+v persisted=%+v", firstEvent, firstPersisted)
	}

	secondPolicy, secondEvent, err := store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "dangerous-tool",
		Effect:      "REQUIRE_APPROVAL",
		Reason:      "second policy",
		CreatedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("second put capability policy with event: %v", err)
	}
	if secondPolicy.PolicyID != firstPolicy.PolicyID {
		t.Fatalf("expected repeated policy put to preserve policy id, first=%+v second=%+v", firstPolicy, secondPolicy)
	}
	secondPersisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		EntityID:    firstPolicy.PolicyID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list second capability policy runtime events: %v", err)
	}
	if len(secondPersisted) < 2 || secondPersisted[0] != secondEvent {
		t.Fatalf("expected second capability policy exact runtime row, returned=%+v persisted=%+v", secondEvent, secondPersisted)
	}
	if secondEvent.EventID == firstEvent.EventID || secondEvent.IngestSeq <= firstEvent.IngestSeq {
		t.Fatalf("expected repeated policy put to return newer runtime row, first=%+v second=%+v", firstEvent, secondEvent)
	}
	var policyPayload map[string]any
	if err := json.Unmarshal([]byte(secondEvent.PayloadJSON), &policyPayload); err != nil {
		t.Fatalf("decode capability policy payload: %v", err)
	}
	if strings.TrimSpace(policyPayload["effect"].(string)) != "REQUIRE_APPROVAL" || strings.TrimSpace(policyPayload["tool_id"].(string)) != "dangerous-tool" {
		t.Fatalf("expected second capability policy payload to win, got %+v", policyPayload)
	}
	if strings.TrimSpace(policyPayload["reason"].(string)) != "second policy" || strings.TrimSpace(policyPayload["created_by"].(string)) != "developer" {
		t.Fatalf("expected capability policy payload to keep replay-critical context, got %+v", policyPayload)
	}
}

func decodeRuntimeEventParentRefsSet(t *testing.T, raw string) map[string]int {
	t.Helper()

	var refs []string
	if err := json.Unmarshal([]byte(raw), &refs); err != nil {
		t.Fatalf("decode runtime event parent refs %q: %v", raw, err)
	}
	out := make(map[string]int, len(refs))
	for _, ref := range refs {
		out[strings.TrimSpace(ref)]++
	}
	return out
}
