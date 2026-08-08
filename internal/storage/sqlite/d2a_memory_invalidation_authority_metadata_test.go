package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestPollMemoryInvalidationsWithEventsCarriesAuthorityMetadataWhenMarkDelivered(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-memory-invalidation-delivered-authority-metadata"
		agentID     = "agent-d2a-memory-invalidation-delivered-authority-metadata"
		docKey      = "delivered-authority-metadata-doc"
	)
	invalidation := seedMemoryInvalidationAuthorityMetadataFixture(t, ctx, store, workspaceID, agentID, docKey)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	items, _, err := store.PollMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll memory invalidations with events: %v", err)
	}
	if len(items) != 1 || items[0].InvalidationID != invalidation.InvalidationID {
		t.Fatalf("expected delivered invalidation %s, got %+v", invalidation.InvalidationID, items)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_delivered",
		EntityType:  "memory_invalidation",
		EntityID:    invalidation.InvalidationID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory.invalidation_delivered events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory.invalidation_delivered event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestAckMemoryInvalidationsWithEventsCarriesAuthorityMetadata(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-memory-invalidation-acked-authority-metadata"
		agentID     = "agent-d2a-memory-invalidation-acked-authority-metadata"
		docKey      = "acked-authority-metadata-doc"
	)
	invalidation := seedMemoryInvalidationAuthorityMetadataFixture(t, ctx, store, workspaceID, agentID, docKey)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, _, err := store.PollMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}); err != nil {
		t.Fatalf("deliver invalidation before ack: %v", err)
	}

	acked, _, err := store.AckMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationAckInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{invalidation.InvalidationID},
	})
	if err != nil {
		t.Fatalf("ack memory invalidations with events: %v", err)
	}
	if len(acked) != 1 || acked[0].State != "ACKED" {
		t.Fatalf("expected one acked invalidation, got %+v", acked)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_acked",
		EntityType:  "memory_invalidation",
		EntityID:    invalidation.InvalidationID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory.invalidation_acked events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory.invalidation_acked event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestFailMemoryInvalidationsWithEventsCarriesAuthorityMetadata(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-memory-invalidation-failed-authority-metadata"
		agentID     = "agent-d2a-memory-invalidation-failed-authority-metadata"
		docKey      = "failed-authority-metadata-doc"
	)
	invalidation := seedMemoryInvalidationAuthorityMetadataFixture(t, ctx, store, workspaceID, agentID, docKey)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, _, err := store.PollMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}); err != nil {
		t.Fatalf("deliver invalidation before fail: %v", err)
	}

	failed, _, err := store.FailMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationFailInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{invalidation.InvalidationID},
		FailureReason:   "AGENT_ERROR",
	})
	if err != nil {
		t.Fatalf("fail memory invalidations with events: %v", err)
	}
	if len(failed) != 1 {
		t.Fatalf("expected one failed invalidation, got %+v", failed)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_failed",
		EntityType:  "memory_invalidation",
		EntityID:    invalidation.InvalidationID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory.invalidation_failed events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory.invalidation_failed event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestRequeueMemoryInvalidationsWithEventsCarriesAuthorityMetadata(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-memory-invalidation-requeued-authority-metadata"
		agentID     = "agent-d2a-memory-invalidation-requeued-authority-metadata"
		docKey      = "requeued-authority-metadata-doc"
	)
	invalidation := seedMemoryInvalidationAuthorityMetadataFixture(t, ctx, store, workspaceID, agentID, docKey)
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, _, err := store.PollMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	}); err != nil {
		t.Fatalf("deliver invalidation before requeue: %v", err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		failed, _, err := store.FailMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationFailInput{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{invalidation.InvalidationID},
			FailureReason:   "AGENT_ERROR",
		})
		if err != nil {
			t.Fatalf("fail invalidation attempt %d: %v", attempt+1, err)
		}
		if len(failed) != 1 {
			t.Fatalf("expected one failed invalidation on attempt %d, got %+v", attempt+1, failed)
		}
		if attempt < 2 {
			redeliverMemoryInvalidationAuthorityMetadataTest(t, ctx, store, workspaceID, agentID, invalidation.InvalidationID)
		}
	}

	requeued, _, err := store.RequeueMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationRequeueInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{invalidation.InvalidationID},
	})
	if err != nil {
		t.Fatalf("requeue memory invalidations with events: %v", err)
	}
	if len(requeued) != 1 || requeued[0].State != "OPEN" {
		t.Fatalf("expected one requeued invalidation, got %+v", requeued)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_requeued",
		EntityType:  "memory_invalidation",
		EntityID:    requeued[0].InvalidationID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory.invalidation_requeued events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory.invalidation_requeued event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func seedMemoryInvalidationAuthorityMetadataFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, docKey string) sqlite.MemoryInvalidationRecord {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       docKey,
		Content:     "# " + docKey + "\nVersion A",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:" + workspaceID,
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       docKey,
		Content:     "# " + docKey + "\nVersion B",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	items, err := store.PollMemoryInvalidations(ctx, sqlite.MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("seed poll invalidations: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one seeded invalidation, got %+v", items)
	}
	return items[0]
}

func redeliverMemoryInvalidationAuthorityMetadataTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, invalidationID string) {
	t.Helper()

	if _, err := store.DB().ExecContext(ctx, `UPDATE memory_invalidation_queue SET next_delivery_at = ?, updated_at = ? WHERE invalidation_id = ?`, "2000-01-01T00:00:00Z", "2000-01-01T00:00:00Z", invalidationID); err != nil {
		t.Fatalf("re-arm invalidation %s for redelivery: %v", invalidationID, err)
	}
	items, _, err := store.PollMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("redeliver invalidation %s: %v", invalidationID, err)
	}
	found := false
	for _, item := range items {
		if item.InvalidationID == invalidationID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected invalidation %s to be redelivered, got %+v", invalidationID, items)
	}
}
