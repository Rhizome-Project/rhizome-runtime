package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestSnapshotMemoryCoherenceReportRejectsMissingWorkspaceAuthorityWithoutRuntimeSideEffects(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	workspaceID, agentID, _ := seedMemoryCoherenceAuthorityScenario(t, ctx, store, "ws-d2a-memory-coherence-missing-authority", "agent-d2a-memory-coherence-missing-authority", "doc-d2a-memory-coherence-missing-authority")
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, authorityScopeWorkspace); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeRejects := countMemoryCoherenceAuthorityRejectEvents(t, ctx, store, workspaceID)

	_, err := store.SnapshotMemoryCoherenceReport(ctx, MemoryCoherenceReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "AGENT",
		Limit:       10,
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %v", err)
	}

	assertNoMemoryCoherenceRuntimeEvents(t, ctx, store, workspaceID)
	if got := countMemoryCoherenceAuthorityRejectEvents(t, ctx, store, workspaceID); got != beforeRejects {
		t.Fatalf("expected missing authority reject not to fabricate authority.rejected evidence without expected term, before=%d after=%d", beforeRejects, got)
	}
}

func TestSnapshotMemoryCoherenceReportRejectsStaleWorkspaceAuthorityWithoutRuntimeSideEffects(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	workspaceID, agentID, current := seedMemoryCoherenceAuthorityScenario(t, ctx, store, "ws-d2a-memory-coherence-stale-authority", "agent-d2a-memory-coherence-stale-authority", "doc-d2a-memory-coherence-stale-authority")
	beforeRejects := countMemoryCoherenceAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferMemoryCoherenceWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-4001")

	_, err := store.SnapshotMemoryCoherenceReport(ctx, MemoryCoherenceReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "AGENT",
		Limit:       10,
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %v", err)
	}

	assertNoMemoryCoherenceRuntimeEvents(t, ctx, store, workspaceID)
	assertMemoryCoherenceAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, AuthorityRejectStale)
}

func TestSnapshotMemoryCoherenceReportCarriesAuthorityMetadata(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	workspaceID, agentID, authority := seedMemoryCoherenceAuthorityScenario(t, ctx, store, "ws-d2a-memory-coherence-authority-metadata", "agent-d2a-memory-coherence-authority-metadata", "doc-d2a-memory-coherence-authority-metadata")

	result, err := store.SnapshotMemoryCoherenceReport(ctx, MemoryCoherenceReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "AGENT",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("snapshot memory coherence report: %v", err)
	}
	assertMemoryCoherenceAuthorityMetadata(t, result.Event, authority)

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.coherence_snapshot",
		EntityType:  "memory_coherence",
		EntityID:    result.Event.EntityID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list memory coherence runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory coherence runtime event, got %+v", events)
	}
	assertMemoryCoherenceAuthorityMetadata(t, events[0], authority)
}

func seedMemoryCoherenceAuthorityScenario(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID, docKey string) (string, string, WorkspaceAuthorityRecord) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Coherence Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Memory Coherence Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	authority := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Memory Coherence Authority Doc",
		Content:     "# Memory Coherence Authority\nVersion A",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	doc, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get workspace doc: %v", err)
	}
	if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		ReportID:               "memmet-" + workspaceID,
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		ReportScope:            "AGENT",
		LookupCount:            8,
		L1HitCount:             4,
		L2HitCount:             2,
		P3HitCount:             1,
		StaleHitCount:          1,
		PotentialSharedOpCount: 10,
	}); err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ReportScope: "AGENT",
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:" + workspaceID,
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: doc.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}
	return workspaceID, agentID, authority
}

func assertNoMemoryCoherenceRuntimeEvents(t *testing.T, ctx context.Context, store *Store, workspaceID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.coherence_snapshot",
		EntityType:  "memory_coherence",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory coherence runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no memory.coherence_snapshot events, got %+v", events)
	}
}

func transferMemoryCoherenceWorkspaceAuthorityToPeer(t *testing.T, ctx context.Context, store *Store, workspaceID string, current WorkspaceAuthorityRecord, peerNodeID string) {
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

func countMemoryCoherenceAuthorityRejectEvents(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
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

func assertMemoryCoherenceAuthorityRejectEventIncrement(t *testing.T, ctx context.Context, store *Store, workspaceID string, before int, wantRejectCode AuthorityRejectCode) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list authority rejected runtime events: %v", err)
	}
	if len(events) != before+1 {
		t.Fatalf("expected authority.rejected count to grow from %d to %d, got %+v", before, before+1, events)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority rejected payload: %v", err)
	}
	if got, _ := payload["reject_code"].(string); got != string(wantRejectCode) {
		t.Fatalf("expected authority reject code %q, got payload %+v", wantRejectCode, payload)
	}
}

func assertMemoryCoherenceAuthorityMetadata(t *testing.T, event RuntimeEventRecord, authority WorkspaceAuthorityRecord) {
	t.Helper()

	if event.AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected authority holder %q, got %+v", authority.HolderAuthorityNodeID, event)
	}
	if event.AuthorityTerm != authority.Term {
		t.Fatalf("expected authority term %d, got %+v", authority.Term, event)
	}
	if event.AuthorityLeaseTokenFingerprint != authorityLeaseTokenFingerprint(authority.LeaseToken) {
		t.Fatalf("expected authority lease fingerprint %q, got %+v", authorityLeaseTokenFingerprint(authority.LeaseToken), event)
	}
}
