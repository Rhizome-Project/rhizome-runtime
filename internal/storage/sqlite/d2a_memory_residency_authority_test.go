package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestReportMemoryResidencyRejectsMissingWorkspaceAuthorityWithoutResidencyOrInvalidationSideEffects(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	workspaceID, agentID, _, staleDocToken := seedMemoryResidencyAuthorityScenario(t, ctx, store, "ws-d2a-memory-residency-missing-authority", "agent-d2a-memory-residency-missing-authority", "memres-missing-authority-doc")
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, authorityScopeWorkspace); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeRejects := countMemoryResidencyAuthorityRejectEvents(t, ctx, store, workspaceID)

	_, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    "memres-d2a-missing-authority",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:d2a-missing-authority",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "memres-missing-authority-doc", VersionToken: staleDocToken, Weight: 1},
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %v", err)
	}

	assertNoMemoryResidencyReportsForAgent(t, ctx, store, workspaceID, agentID)
	assertNoMemoryInvalidationsForAgent(t, ctx, store, workspaceID, agentID)
	assertNoMemoryResidencyRuntimeEvents(t, ctx, store, workspaceID, "memres-d2a-missing-authority", "memory.residency_reported")
	assertNoMemoryResidencyRuntimeEvents(t, ctx, store, workspaceID, "", "memory.invalidation_enqueued")
	assertNoMemoryResidencyRuntimeEvents(t, ctx, store, workspaceID, "", "memory.invalidation_refreshed")
	if got := countMemoryResidencyAuthorityRejectEvents(t, ctx, store, workspaceID); got != beforeRejects {
		t.Fatalf("expected missing authority reject not to fabricate authority.rejected evidence without expected term, before=%d after=%d", beforeRejects, got)
	}
}

func TestReportMemoryResidencyRejectsStaleWorkspaceAuthorityWithoutResidencyOrInvalidationSideEffects(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	workspaceID, agentID, _, staleDocToken := seedMemoryResidencyAuthorityScenario(t, ctx, store, "ws-d2a-memory-residency-stale-authority", "agent-d2a-memory-residency-stale-authority", "memres-stale-authority-doc")
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeUpdatedAt := mustMemoryResidencyAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeRejects := countMemoryResidencyAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferMemoryResidencyWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-3601")

	_, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    "memres-d2a-stale-authority",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "agent",
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:d2a-stale-authority",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "memres-stale-authority-doc", VersionToken: staleDocToken, Weight: 1},
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %v", err)
	}

	assertNoMemoryResidencyReportsForAgent(t, ctx, store, workspaceID, agentID)
	assertNoMemoryInvalidationsForAgent(t, ctx, store, workspaceID, agentID)
	assertNoMemoryResidencyRuntimeEvents(t, ctx, store, workspaceID, "memres-d2a-stale-authority", "memory.residency_reported")
	assertNoMemoryResidencyRuntimeEvents(t, ctx, store, workspaceID, "", "memory.invalidation_enqueued")
	assertNoMemoryResidencyRuntimeEvents(t, ctx, store, workspaceID, "", "memory.invalidation_refreshed")
	assertMemoryResidencyAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, AuthorityRejectStale)
	if afterUpdatedAt := mustMemoryResidencyAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected stale-authority reject to advance workspace updated_at due to reject journaling, still %q", afterUpdatedAt)
	}
}

func seedMemoryResidencyAuthorityScenario(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID, docKey string) (string, string, string, string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Residency Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Memory Residency Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Memory Residency Authority Doc",
		Content:     "# Doc\nVersion A",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert workspace doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get workspace doc v1: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Memory Residency Authority Doc",
		Content:     "# Doc\nVersion B",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert workspace doc v2: %v", err)
	}
	return workspaceID, agentID, docKey, docV1.SHA
}

func assertNoMemoryResidencyReportsForAgent(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID string) {
	t.Helper()
	items, err := store.ListMemoryResidencyReports(ctx, MemoryResidencyReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list memory residency reports: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no memory residency reports, got %+v", items)
	}
}

func assertNoMemoryInvalidationsForAgent(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID string) {
	t.Helper()
	items, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list memory invalidations: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no memory invalidations, got %+v", items)
	}
}

func assertNoMemoryResidencyRuntimeEvents(t *testing.T, ctx context.Context, store *Store, workspaceID, entityID, eventType string) {
	t.Helper()
	filter := RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		Limit:       20,
	}
	if entityID != "" {
		filter.EntityID = entityID
	}
	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events %+v: %v", filter, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s runtime events, got %+v", eventType, events)
	}
}

func mustMemoryResidencyAuthorityWorkspaceUpdatedAt(t *testing.T, ctx context.Context, store *Store, workspaceID string) string {
	t.Helper()

	var updatedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT updated_at FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&updatedAt); err != nil {
		t.Fatalf("query workspace updated_at: %v", err)
	}
	return updatedAt
}

func transferMemoryResidencyWorkspaceAuthorityToPeer(t *testing.T, ctx context.Context, store *Store, workspaceID string, current WorkspaceAuthorityRecord, peerNodeID string) {
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
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, WorkspaceAuthorityTransferInput{
		WorkspaceID:                  workspaceID,
		Scope:                        authorityScopeWorkspace,
		CurrentHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		CurrentLeaseToken:            current.LeaseToken,
		CurrentTerm:                  current.Term,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-" + peerNodeID,
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             appliedWatermark,
		ActorType:                    authorityActorTypeSystem,
		ActorID:                      "tests",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority to peer: %v", err)
	}
}

func countMemoryResidencyAuthorityRejectEvents(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM runtime_events
 WHERE workspace_id = ?
   AND event_type = ?
   AND entity_type = 'workspace_authority'`,
		workspaceID,
		AuthorityEventRejected,
	).Scan(&count); err != nil {
		t.Fatalf("count authority rejected runtime events: %v", err)
	}
	return count
}

func assertMemoryResidencyAuthorityRejectEventIncrement(t *testing.T, ctx context.Context, store *Store, workspaceID string, before int, wantRejectCode AuthorityRejectCode) {
	t.Helper()

	after := countMemoryResidencyAuthorityRejectEvents(t, ctx, store, workspaceID)
	if after != before+1 {
		t.Fatalf("expected one new authority.rejected event, before=%d after=%d", before, after)
	}
	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list authority rejected runtime events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected authority.rejected runtime event")
	}
	payload := decodeMemoryResidencyAuthorityRejectPayload(t, events[0].PayloadJSON)
	if payload["reject_code"] != string(wantRejectCode) {
		t.Fatalf("expected authority reject code %q, got %+v", wantRejectCode, payload)
	}
}

func decodeMemoryResidencyAuthorityRejectPayload(t *testing.T, payloadJSON string) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode authority reject payload: %v", err)
	}
	return payload
}
