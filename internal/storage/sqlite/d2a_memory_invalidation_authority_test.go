package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestPollMemoryInvalidationsWithEventsRejectsMissingWorkspaceAuthorityWhenMarkDelivered(t *testing.T) {
	t.Parallel()

	store, ctx, seeded := seedOpenDocMemoryInvalidation(t, "ws-d2a-memory-invalidation-poll-missing-authority", "agent-d2a-memory-invalidation-poll-missing-authority", "poll-missing-authority-doc")
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, seeded.WorkspaceID, authorityScopeWorkspace); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeRejects := countMemoryInvalidationAuthorityRejectEvents(t, ctx, store, seeded.WorkspaceID)

	items, events, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   seeded.WorkspaceID,
		AgentID:       seeded.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", reject)
	}
	if len(items) != 0 || len(events) != 0 {
		t.Fatalf("expected no delivered rows or events on authority reject, got items=%+v events=%+v", items, events)
	}

	reloaded, err := store.GetMemoryInvalidation(ctx, seeded.WorkspaceID, seeded.AgentID, seeded.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation after authority reject: %v", err)
	}
	if reloaded.DeliveredAt != "" || reloaded.LeaseExpiresAt != "" || reloaded.DeliveryAttemptCount != seeded.DeliveryAttemptCount {
		t.Fatalf("expected authority reject not to mutate delivery state, got %+v", reloaded)
	}
	if got := countMemoryInvalidationLifecycleEvents(t, ctx, store, seeded.WorkspaceID, seeded.InvalidationID, "memory.invalidation_delivered"); got != 0 {
		t.Fatalf("expected no memory.invalidation_delivered rows after authority reject, got %d", got)
	}
	if got := countMemoryInvalidationAuthorityRejectEvents(t, ctx, store, seeded.WorkspaceID); got != beforeRejects {
		t.Fatalf("expected missing authority reject not to fabricate authority.rejected evidence without expected term, before=%d after=%d", beforeRejects, got)
	}
}

func TestAckMemoryInvalidationsWithEventsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store, ctx, seeded := seedOpenDocMemoryInvalidation(t, "ws-d2a-memory-invalidation-ack-stale-authority", "agent-d2a-memory-invalidation-ack-stale-authority", "ack-stale-authority-doc")
	current := claimTestWorkspaceAuthority(t, ctx, store, seeded.WorkspaceID)
	delivered, _, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   seeded.WorkspaceID,
		AgentID:       seeded.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("seed delivered invalidation: %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("expected one delivered invalidation, got %+v", delivered)
	}
	before := delivered[0]
	beforeRejects := countMemoryInvalidationAuthorityRejectEvents(t, ctx, store, seeded.WorkspaceID)
	transferMemoryInvalidationWorkspaceAuthorityToPeer(t, ctx, store, seeded.WorkspaceID, current, "authnode-999-3201")

	items, events, err := store.AckMemoryInvalidationsWithEvents(ctx, MemoryInvalidationAckInput{
		WorkspaceID:     seeded.WorkspaceID,
		AgentID:         seeded.AgentID,
		InvalidationIDs: []string{seeded.InvalidationID},
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", reject)
	}
	if len(items) != 0 || len(events) != 0 {
		t.Fatalf("expected no ack rows or events on authority reject, got items=%+v events=%+v", items, events)
	}

	reloaded, err := store.GetMemoryInvalidation(ctx, seeded.WorkspaceID, seeded.AgentID, seeded.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation after stale authority reject: %v", err)
	}
	if reloaded.State != before.State || reloaded.AcknowledgedAt != before.AcknowledgedAt || reloaded.LeaseExpiresAt != before.LeaseExpiresAt || reloaded.UpdatedAt != before.UpdatedAt {
		t.Fatalf("expected stale authority reject not to ack invalidation, before=%+v after=%+v", before, reloaded)
	}
	if got := countMemoryInvalidationLifecycleEvents(t, ctx, store, seeded.WorkspaceID, seeded.InvalidationID, "memory.invalidation_acked"); got != 0 {
		t.Fatalf("expected no memory.invalidation_acked rows after authority reject, got %d", got)
	}
	assertMemoryInvalidationAuthorityRejectEventIncrement(t, ctx, store, seeded.WorkspaceID, beforeRejects, AuthorityRejectStale)
}

func TestFailMemoryInvalidationsWithEventsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store, ctx, seeded := seedOpenDocMemoryInvalidation(t, "ws-d2a-memory-invalidation-fail-stale-authority", "agent-d2a-memory-invalidation-fail-stale-authority", "fail-stale-authority-doc")
	current := claimTestWorkspaceAuthority(t, ctx, store, seeded.WorkspaceID)
	delivered, _, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   seeded.WorkspaceID,
		AgentID:       seeded.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("seed delivered invalidation: %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("expected one delivered invalidation, got %+v", delivered)
	}
	before := delivered[0]
	beforeRejects := countMemoryInvalidationAuthorityRejectEvents(t, ctx, store, seeded.WorkspaceID)
	transferMemoryInvalidationWorkspaceAuthorityToPeer(t, ctx, store, seeded.WorkspaceID, current, "authnode-999-3202")

	items, events, err := store.FailMemoryInvalidationsWithEvents(ctx, MemoryInvalidationFailInput{
		WorkspaceID:     seeded.WorkspaceID,
		AgentID:         seeded.AgentID,
		InvalidationIDs: []string{seeded.InvalidationID},
		FailureReason:   "AGENT_ERROR",
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", reject)
	}
	if len(items) != 0 || len(events) != 0 {
		t.Fatalf("expected no fail rows or events on authority reject, got items=%+v events=%+v", items, events)
	}

	reloaded, err := store.GetMemoryInvalidation(ctx, seeded.WorkspaceID, seeded.AgentID, seeded.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation after stale authority reject: %v", err)
	}
	if reloaded.State != before.State || reloaded.FailureCount != before.FailureCount || reloaded.LastFailureAt != before.LastFailureAt || reloaded.UpdatedAt != before.UpdatedAt {
		t.Fatalf("expected stale authority reject not to fail invalidation, before=%+v after=%+v", before, reloaded)
	}
	if got := countMemoryInvalidationLifecycleEvents(t, ctx, store, seeded.WorkspaceID, seeded.InvalidationID, "memory.invalidation_failed"); got != 0 {
		t.Fatalf("expected no memory.invalidation_failed rows after authority reject, got %d", got)
	}
	if got := countMemoryInvalidationLifecycleEvents(t, ctx, store, seeded.WorkspaceID, seeded.InvalidationID, "memory.invalidation_dead_lettered"); got != 0 {
		t.Fatalf("expected no memory.invalidation_dead_lettered rows after authority reject, got %d", got)
	}
	assertMemoryInvalidationAuthorityRejectEventIncrement(t, ctx, store, seeded.WorkspaceID, beforeRejects, AuthorityRejectStale)
}

func TestRequeueMemoryInvalidationsWithEventsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store, ctx, seeded := seedOpenDocMemoryInvalidation(t, "ws-d2a-memory-invalidation-requeue-stale-authority", "agent-d2a-memory-invalidation-requeue-stale-authority", "requeue-stale-authority-doc")
	current := claimTestWorkspaceAuthority(t, ctx, store, seeded.WorkspaceID)
	delivered, _, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   seeded.WorkspaceID,
		AgentID:       seeded.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("seed delivered invalidation: %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("expected one delivered invalidation, got %+v", delivered)
	}
	for attempt := 0; attempt < memoryInvalidationDeadLetterThreshold; attempt++ {
		failed, _, err := store.FailMemoryInvalidationsWithEvents(ctx, MemoryInvalidationFailInput{
			WorkspaceID:     seeded.WorkspaceID,
			AgentID:         seeded.AgentID,
			InvalidationIDs: []string{seeded.InvalidationID},
			FailureReason:   "AGENT_ERROR",
		})
		if err != nil {
			t.Fatalf("seed dead-letter attempt %d: %v", attempt+1, err)
		}
		if len(failed) != 1 {
			t.Fatalf("expected one failed invalidation on attempt %d, got %+v", attempt+1, failed)
		}
		if attempt+1 < memoryInvalidationDeadLetterThreshold {
			redeliverMemoryInvalidationForTest(t, store, ctx, seeded.WorkspaceID, seeded.AgentID, seeded.InvalidationID)
		}
	}
	before, err := store.GetMemoryInvalidation(ctx, seeded.WorkspaceID, seeded.AgentID, seeded.InvalidationID)
	if err != nil {
		t.Fatalf("reload dead-letter invalidation: %v", err)
	}
	if before.State != "DEAD_LETTER" {
		t.Fatalf("expected dead-letter invalidation before requeue reject, got %+v", before)
	}
	beforeRejects := countMemoryInvalidationAuthorityRejectEvents(t, ctx, store, seeded.WorkspaceID)
	transferMemoryInvalidationWorkspaceAuthorityToPeer(t, ctx, store, seeded.WorkspaceID, current, "authnode-999-3203")

	items, events, err := store.RequeueMemoryInvalidationsWithEvents(ctx, MemoryInvalidationRequeueInput{
		WorkspaceID:     seeded.WorkspaceID,
		AgentID:         seeded.AgentID,
		InvalidationIDs: []string{seeded.InvalidationID},
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", reject)
	}
	if len(items) != 0 || len(events) != 0 {
		t.Fatalf("expected no requeue rows or events on authority reject, got items=%+v events=%+v", items, events)
	}

	reloaded, err := store.GetMemoryInvalidation(ctx, seeded.WorkspaceID, seeded.AgentID, seeded.InvalidationID)
	if err != nil {
		t.Fatalf("reload invalidation after stale authority reject: %v", err)
	}
	if reloaded.State != before.State || reloaded.RecoveredFromInvalidationID != before.RecoveredFromInvalidationID || reloaded.UpdatedAt != before.UpdatedAt {
		t.Fatalf("expected stale authority reject not to requeue invalidation, before=%+v after=%+v", before, reloaded)
	}
	if got := countMemoryInvalidationLifecycleEvents(t, ctx, store, seeded.WorkspaceID, seeded.InvalidationID, "memory.invalidation_requeued"); got != 0 {
		t.Fatalf("expected no memory.invalidation_requeued rows after authority reject, got %d", got)
	}
	assertMemoryInvalidationAuthorityRejectEventIncrement(t, ctx, store, seeded.WorkspaceID, beforeRejects, AuthorityRejectStale)
}

func countMemoryInvalidationLifecycleEvents(t *testing.T, ctx context.Context, store *Store, workspaceID, invalidationID, eventType string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "memory_invalidation",
		EntityID:    invalidationID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events for %s: %v", eventType, invalidationID, err)
	}
	return len(events)
}

func countMemoryInvalidationAuthorityRejectEvents(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list authority reject runtime events: %v", err)
	}
	return len(events)
}

func assertMemoryInvalidationAuthorityRejectEventIncrement(t *testing.T, ctx context.Context, store *Store, workspaceID string, before int, wantCode AuthorityRejectCode) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list authority reject runtime events: %v", err)
	}
	if len(events) != before+1 {
		t.Fatalf("expected authority.rejected count to grow from %d to %d, got %+v", before, before+1, events)
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority reject payload: %v", err)
	}
	if payload["reject_code"] != string(wantCode) {
		t.Fatalf("expected latest authority reject code %q, got %+v", wantCode, payload)
	}
}

func transferMemoryInvalidationWorkspaceAuthorityToPeer(t *testing.T, ctx context.Context, store *Store, workspaceID string, current WorkspaceAuthorityRecord, peerNodeID string) {
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
		string(RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("register peer runtime node %s: %v", peerNodeID, err)
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, WorkspaceAuthorityTransferInput{
		WorkspaceID:                  workspaceID,
		Scope:                        authorityScopeWorkspace,
		CurrentHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		CurrentLeaseToken:            current.LeaseToken,
		CurrentTerm:                  current.Term,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-" + nextID("transfer"),
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             appliedWatermark,
		ActorType:                    authorityActorTypeSystem,
		ActorID:                      "tests",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority to %s: %v", peerNodeID, err)
	}
}
