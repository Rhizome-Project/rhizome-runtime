package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestReportMemoryMetricsRejectsMissingWorkspaceAuthorityWithoutMetricsOrRuntimeSideEffects(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	workspaceID, agentID := seedMemoryMetricsAuthorityScenario(t, ctx, store, "ws-d2a-memory-metrics-missing-authority", "agent-d2a-memory-metrics-missing-authority")
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, authorityScopeWorkspace); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeRejects := countMemoryMetricsAuthorityRejectEvents(t, ctx, store, workspaceID)

	_, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		ReportID:               "memmet-d2a-missing-authority",
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		ReportScope:            "agent",
		LookupCount:            6,
		L1HitCount:             3,
		L2HitCount:             2,
		P3HitCount:             1,
		PotentialSharedOpCount: 8,
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %v", err)
	}

	assertNoMemoryMetricsReports(t, ctx, store, workspaceID, agentID)
	assertNoMemoryMetricsRuntimeEvents(t, ctx, store, workspaceID, "memmet-d2a-missing-authority")
	if got := countMemoryMetricsAuthorityRejectEvents(t, ctx, store, workspaceID); got != beforeRejects {
		t.Fatalf("expected missing authority reject not to fabricate authority.rejected evidence without expected term, before=%d after=%d", beforeRejects, got)
	}
}

func TestReportMemoryMetricsRejectsStaleWorkspaceAuthorityWithoutMetricsSideEffects(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	workspaceID, agentID := seedMemoryMetricsAuthorityScenario(t, ctx, store, "ws-d2a-memory-metrics-stale-authority", "agent-d2a-memory-metrics-stale-authority")
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeRejects := countMemoryMetricsAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferMemoryMetricsWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-3801")

	_, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		ReportID:               "memmet-d2a-stale-authority",
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		ReportScope:            "agent",
		LookupCount:            7,
		L1HitCount:             4,
		L2HitCount:             2,
		P3HitCount:             1,
		PotentialSharedOpCount: 9,
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %v", err)
	}

	assertNoMemoryMetricsReports(t, ctx, store, workspaceID, agentID)
	assertNoMemoryMetricsRuntimeEvents(t, ctx, store, workspaceID, "memmet-d2a-stale-authority")
	assertMemoryMetricsAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, AuthorityRejectStale)
}

func TestReportMemoryMetricsCarriesAuthorityMetadata(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	workspaceID, agentID := seedMemoryMetricsAuthorityScenario(t, ctx, store, "ws-d2a-memory-metrics-authority-metadata", "agent-d2a-memory-metrics-authority-metadata")
	authority := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	result, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		ReportID:               "memmet-d2a-authority-metadata",
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		ReportScope:            "agent",
		LookupCount:            9,
		L1HitCount:             5,
		L2HitCount:             3,
		P3HitCount:             1,
		PotentialSharedOpCount: 12,
	})
	if err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}
	assertMemoryMetricsAuthorityMetadata(t, result.Event, authority)

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.metrics_reported",
		EntityType:  "memory_metrics",
		EntityID:    result.Report.ReportID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list memory metrics runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory metrics runtime event, got %+v", events)
	}
	assertMemoryMetricsAuthorityMetadata(t, events[0], authority)
}

func seedMemoryMetricsAuthorityScenario(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID string) (string, string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Metrics Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Memory Metrics Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	return workspaceID, agentID
}

func assertNoMemoryMetricsReports(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID string) {
	t.Helper()

	items, err := store.ListMemoryMetricsReports(ctx, MemoryMetricsReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list memory metrics reports: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no memory metrics reports, got %+v", items)
	}
}

func assertNoMemoryMetricsRuntimeEvents(t *testing.T, ctx context.Context, store *Store, workspaceID, reportID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.metrics_reported",
		EntityType:  "memory_metrics",
		EntityID:    reportID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory metrics runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no memory.metrics_reported events, got %+v", events)
	}
}

func transferMemoryMetricsWorkspaceAuthorityToPeer(t *testing.T, ctx context.Context, store *Store, workspaceID string, current WorkspaceAuthorityRecord, peerNodeID string) {
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

func countMemoryMetricsAuthorityRejectEvents(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
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

func assertMemoryMetricsAuthorityRejectEventIncrement(t *testing.T, ctx context.Context, store *Store, workspaceID string, before int, wantRejectCode AuthorityRejectCode) {
	t.Helper()

	after := countMemoryMetricsAuthorityRejectEvents(t, ctx, store, workspaceID)
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
	payload := decodeMemoryMetricsRuntimePayload(t, events[0].PayloadJSON)
	if payload["reject_code"] != string(wantRejectCode) {
		t.Fatalf("expected authority reject code %q, got %+v", wantRejectCode, payload)
	}
}

func assertMemoryMetricsAuthorityMetadata(t *testing.T, event RuntimeEventRecord, authority WorkspaceAuthorityRecord) {
	t.Helper()

	if event.AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected authority holder %q, got %+v", authority.HolderAuthorityNodeID, event)
	}
	if event.AuthorityTerm != authority.Term {
		t.Fatalf("expected authority term %d, got %+v", authority.Term, event)
	}
	expectedFingerprint := authorityLeaseTokenFingerprint(authority.LeaseToken)
	if event.AuthorityLeaseTokenFingerprint != expectedFingerprint {
		t.Fatalf("expected authority fingerprint %q, got %+v", expectedFingerprint, event)
	}
}
