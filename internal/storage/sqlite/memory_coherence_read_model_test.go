package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestBuildMemoryCoherenceReportAggregatesLatestMetricsResidencyAndInvalidationState(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-coherence-report", "agent-memory-coherence-report", "coherence-doc")

	if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		WorkspaceID:             item.WorkspaceID,
		AgentID:                 item.AgentID,
		ReportID:                "memmet-coherence",
		LookupCount:             10,
		L1HitCount:              4,
		L2HitCount:              2,
		StaleHitCount:           2,
		PromotionCount:          2,
		PromotionReuseCount:     1,
		FlushCount:              2,
		FlushPositiveCount:      1,
		LocalConsolidationCount: 1,
		PotentialSharedOpCount:  1,
	}); err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}

	report, err := store.BuildMemoryCoherenceReport(ctx, MemoryCoherenceReportFilter{
		WorkspaceID: item.WorkspaceID,
		AgentID:     item.AgentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build memory coherence report: %v", err)
	}
	if report.ScopeCount != 1 || len(report.Items) != 1 {
		t.Fatalf("expected one coherence scope, got %+v", report)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected memory coherence report generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	scope := report.Items[0]
	if scope.AgentID != item.AgentID || scope.ReportScope != "AGENT" {
		t.Fatalf("unexpected coherence scope %+v", scope)
	}
	if scope.MetricsReportID != "memmet-coherence" || scope.ResidencyReportID == "" {
		t.Fatalf("expected latest metrics and residency ids, got %+v", scope)
	}
	if scope.ReadyInvalidationCount != 1 || scope.OpenInvalidationCount != 1 {
		t.Fatalf("expected one ready open invalidation, got %+v", scope)
	}
	if scope.CoherenceBandHint != "DEGRADED" || !scope.NeedsAttention {
		t.Fatalf("expected degraded coherence hint, got %+v", scope)
	}
	if report.AttentionScopeCount != 1 || report.ReadyInvalidationCount != 1 {
		t.Fatalf("unexpected coherence report totals %+v", report)
	}
}

func TestBuildMemoryCoherenceReportTracksLeaseTransitionsAndWatermarks(t *testing.T) {
	t.Parallel()

	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-coherence-watermark", "agent-memory-coherence-watermark", "watermark-doc")

	initial, err := store.GetMemoryCoherenceScope(ctx, item.WorkspaceID, item.AgentID, "", "")
	if err != nil {
		t.Fatalf("get initial memory coherence scope: %v", err)
	}
	if initial.OpenInvalidationCount != 1 || initial.LeasedInvalidationCount != 0 || initial.ReadyInvalidationCount != 1 {
		t.Fatalf("expected seeded invalidation to start ready, got %+v", initial)
	}
	if initial.InvalidationUpdatedAt == "" || initial.LastObservedAt != initial.InvalidationUpdatedAt {
		t.Fatalf("expected initial watermark to follow invalidation updates, got %+v", initial)
	}

	leasedItems, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   item.WorkspaceID,
		AgentID:       item.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll memory invalidations to create lease: %v", err)
	}
	if len(leasedItems) != 1 || leasedItems[0].InvalidationID != item.InvalidationID {
		t.Fatalf("expected one delivered invalidation lease, got %+v", leasedItems)
	}

	leasedScope, err := store.GetMemoryCoherenceScope(ctx, item.WorkspaceID, item.AgentID, "", "")
	if err != nil {
		t.Fatalf("get memory coherence scope after lease delivery: %v", err)
	}
	if leasedScope.OpenInvalidationCount != 1 || leasedScope.LeasedInvalidationCount != 1 || leasedScope.ReadyInvalidationCount != 0 {
		t.Fatalf("expected delivered invalidation lease to suppress ready counts, got %+v", leasedScope)
	}

	expiredAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	setMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at", expiredAt)

	afterExpiry, err := store.GetMemoryCoherenceScope(ctx, item.WorkspaceID, item.AgentID, "", "")
	if err != nil {
		t.Fatalf("get memory coherence scope after lease expiry: %v", err)
	}
	if afterExpiry.OpenInvalidationCount != 1 || afterExpiry.LeasedInvalidationCount != 0 || afterExpiry.ReadyInvalidationCount != 1 {
		t.Fatalf("expected expired lease to become ready again, got %+v", afterExpiry)
	}
	if afterExpiry.InvalidationUpdatedAt == "" || afterExpiry.InvalidationUpdatedAt == initial.InvalidationUpdatedAt {
		t.Fatalf("expected lease expiry update to advance invalidation watermark, got initial=%+v after=%+v", initial, afterExpiry)
	}
	if afterExpiry.LastObservedAt != afterExpiry.InvalidationUpdatedAt {
		t.Fatalf("expected last observed time to follow invalidation watermark, got %+v", afterExpiry)
	}
}

func TestBuildMemoryCoherenceReportUsesWorkspaceReferenceTimeForReadyInvalidations(t *testing.T) {
	t.Parallel()

	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-coherence-reference", "agent-memory-coherence-reference", "reference-doc")

	pendingAt := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339Nano)
	anchorAt := time.Now().UTC().Add(20 * time.Minute).Format(time.RFC3339Nano)
	setMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at", "")
	setMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "next_delivery_at", pendingAt)

	before, err := store.GetMemoryCoherenceScope(ctx, item.WorkspaceID, item.AgentID, "", "")
	if err != nil {
		t.Fatalf("get memory coherence scope before reference anchor: %v", err)
	}
	if before.ReadyInvalidationCount != 0 || before.BackoffInvalidationCount != 1 {
		t.Fatalf("expected future delivery watermark to stay in backoff before reference anchor, got %+v", before)
	}

	setWorkspaceControlEpochAnchor(t, store, item.WorkspaceID, anchorAt)

	after, err := store.GetMemoryCoherenceScope(ctx, item.WorkspaceID, item.AgentID, "", "")
	if err != nil {
		t.Fatalf("get memory coherence scope after reference anchor: %v", err)
	}
	if after.ReadyInvalidationCount != 1 || after.BackoffInvalidationCount != 0 {
		t.Fatalf("expected workspace reference time to promote invalidation into ready state, got %+v", after)
	}

	authority, err := store.GetWorkspaceTimeAuthority(ctx, item.WorkspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	if authority.WorkspaceID != item.WorkspaceID || authority.ReferenceAt != anchorAt {
		t.Fatalf("expected memory surface to keep authority pair inspectable, got %+v", authority)
	}
}

func TestSnapshotMemoryCoherenceReportAppendsSyntheticEvent(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-coherence-snapshot", "agent-memory-coherence-snapshot", "coherence-snapshot-doc")

	if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		WorkspaceID:   item.WorkspaceID,
		AgentID:       item.AgentID,
		ReportID:      "memmet-coherence-snapshot",
		LookupCount:   4,
		L1HitCount:    2,
		StaleHitCount: 1,
	}); err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}

	result, err := store.SnapshotMemoryCoherenceReport(ctx, MemoryCoherenceReportFilter{
		WorkspaceID: item.WorkspaceID,
		AgentID:     item.AgentID,
	})
	if err != nil {
		t.Fatalf("snapshot memory coherence report: %v", err)
	}
	if result.Event.EventType != "memory.coherence_snapshot" || result.Event.EntityType != "memory_coherence" {
		t.Fatalf("unexpected memory coherence snapshot event %+v", result.Event)
	}
	if !isSyntheticOperationalEvent(result.Event) {
		t.Fatalf("expected memory coherence snapshot to stay synthetic %+v", result.Event)
	}
	if result.Report.GeneratedAt != result.Report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected memory coherence snapshot report generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", result.Report.GeneratedAt, result.Report.TimeAuthority.ReferenceAt)
	}
	if result.Report.ScopeCount != 1 || len(result.Report.Items) != 1 {
		t.Fatalf("unexpected memory coherence snapshot report %+v", result.Report)
	}
}

func TestGetMemoryCoherenceScopeDefaultsToAgentScope(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-coherence-scope", "agent-memory-coherence-scope", "coherence-scope-doc")

	scope, err := store.GetMemoryCoherenceScope(ctx, item.WorkspaceID, item.AgentID, "", "")
	if err != nil {
		t.Fatalf("get memory coherence scope: %v", err)
	}
	if scope.ReportScope != "AGENT" || scope.AgentID != item.AgentID {
		t.Fatalf("unexpected coherence scope %+v", scope)
	}
}

func TestBuildMemoryCoherenceReportRequiresWorkspace(t *testing.T) {
	store := NewTestStore(t)
	if _, err := store.BuildMemoryCoherenceReport(context.Background(), MemoryCoherenceReportFilter{}); err == nil {
		t.Fatal("expected missing workspace_id error")
	}
}
