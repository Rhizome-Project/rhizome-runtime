package sqlite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSearchMemoryNodesReturnsCompactHitsAndFiltersArchivedByDefault(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-search"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Search",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	active, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Canonical rollout truth",
		Body:        "Canonical rollout truth stays above cached local guesses.",
		Summary:     "Canonical rollout truth.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record active memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	archived, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Archived rollout truth",
		Body:        "Archived rollout truth should stay hidden by default.",
		Summary:     "Archived rollout truth.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.5,
		Confidence:  0.7,
	})
	if err != nil {
		t.Fatalf("record archived memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	if _, err := store.ArchiveWorkspaceMemory(ctx, WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    archived.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "old context",
	}); err != nil {
		t.Fatalf("archive memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	result, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "rollout truth",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search memory nodes: %v", err)
	}
	if result.Count != 1 {
		t.Fatalf("expected one active hit by default, got %+v", result.Hits)
	}
	if result.TimeAuthority.WorkspaceID != workspaceID || result.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected search time authority, got %+v", result.TimeAuthority)
	}
	if result.GeneratedAt != result.TimeAuthority.ReferenceAt {
		t.Fatalf("expected search generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", result.GeneratedAt, result.TimeAuthority.ReferenceAt)
	}
	hit := result.Hits[0]
	if hit.MemoryID != "memnode:workspace_memory:"+active.MemoryID {
		t.Fatalf("expected active memory hit, got %+v", hit)
	}
	if hit.Snippet == "" || hit.RefCount == 0 || hit.DriftState == "" {
		t.Fatalf("expected compact diagnostics on hit, got %+v", hit)
	}

	withArchived, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID:     workspaceID,
		Query:           "rollout truth",
		OriginKind:      "workspace_memory",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("search memory nodes with archived: %v", err)
	}
	if withArchived.Count < 2 {
		t.Fatalf("expected archived hit when explicitly included, got %+v", withArchived.Hits)
	}
}

func TestSearchMemoryNodesSurfacesDriftStateAfterSourceDocUpdate(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-node-search-drift"
		docKey      = "runbook-search-drift"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Search Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nCanonical rollout truth.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert initial workspace doc: %v", err)
	}
	if _, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Canonical rollout truth",
		Body:        "Canonical rollout truth stays above cached local guesses.",
		Summary:     "Canonical rollout truth.",
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Importance:  0.8,
		Confidence:  0.9,
	}); err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	before, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "rollout truth",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search memory nodes before doc update: %v", err)
	}
	if before.Count != 1 || len(before.Hits) != 1 || before.Hits[0].DriftState != "CURRENT" {
		t.Fatalf("expected current drift state before doc update, got %+v", before)
	}
	if before.TimeAuthority.WorkspaceID != workspaceID || before.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected pre-update search time authority, got %+v", before.TimeAuthority)
	}

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nUpdated rollout truth with rollback steps.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert updated workspace doc: %v", err)
	}

	after, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "rollout truth",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search memory nodes after doc update: %v", err)
	}
	if after.Count != 1 || len(after.Hits) != 1 {
		t.Fatalf("expected one hit after doc update, got %+v", after)
	}
	if after.TimeAuthority.WorkspaceID != workspaceID || after.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected post-update search time authority, got %+v", after.TimeAuthority)
	}
	if after.Hits[0].DriftState != "STALE" || after.Hits[0].DriftScore <= 0 {
		t.Fatalf("expected stale drift state after doc update, got %+v", after.Hits[0])
	}
}

func TestSearchMemoryNodesIsReadOnly(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-readonly"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Read Only",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Preserve dissent",
		Body:        "Preserve dissent during memory compaction.",
		Summary:     "Preserve dissent.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.7,
		Confidence:  0.8,
	}); err != nil {
		t.Fatalf("record memory: %v", err)
	}

	before := map[string]int{
		"runtime_events":       countMemoryPacketRows(t, ctx, store, "runtime_events"),
		"memory_nodes":         countMemoryPacketRows(t, ctx, store, "memory_nodes"),
		"memory_node_refs":     countMemoryPacketRows(t, ctx, store, "memory_node_refs"),
		"memory_node_versions": countMemoryPacketRows(t, ctx, store, "memory_node_versions"),
		"memory_edges":         countMemoryPacketRows(t, ctx, store, "memory_edges"),
	}

	if _, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "dissent",
		Limit:       10,
	}); err != nil {
		t.Fatalf("search memory nodes: %v", err)
	}

	for table, beforeCount := range before {
		if afterCount := countMemoryPacketRows(t, ctx, store, table); afterCount != beforeCount {
			t.Fatalf("expected search to leave %s unchanged, before=%d after=%d", table, beforeCount, afterCount)
		}
	}
}

func TestSearchMemoryNodesMarksKnowledgeClaimProjectionLagUntilExplicitReconcile(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-search-claim-projection-lag"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Search Claim Projection Lag",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "decision",
		Status:      "active",
		Subject:     "Keep projection lag honest",
		Body:        "Derived search must stay partial until explicit reconcile materializes the compatibility node.",
		Summary:     "Projection lag stays honest.",
		Confidence:  0.8,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	before, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "projection lag",
		OriginKind:  "knowledge_claim",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search memory nodes before reconcile: %v", err)
	}
	if before.Count != 0 || len(before.Hits) != 0 {
		t.Fatalf("expected no derived hits before reconcile, got %+v", before)
	}
	if before.BoundaryContract.ProjectionCoverage != "PARTIAL" || before.BoundaryContract.ProjectionLagState != "degraded" {
		t.Fatalf("expected partial degraded boundary before reconcile, got %+v", before.BoundaryContract)
	}
	if before.BoundaryContract.ProjectionPendingCount < 1 {
		t.Fatalf("expected pending projection count before reconcile, got %+v", before.BoundaryContract)
	}
	if before.BoundaryContract.CanonicalShape != "retain_current_canonical_shape" || before.BoundaryContract.SurfaceAuthority != "compatibility_only" {
		t.Fatalf("expected compatibility-only boundary contract, got %+v", before.BoundaryContract)
	}

	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	after, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "projection lag",
		OriginKind:  "knowledge_claim",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search memory nodes after reconcile: %v", err)
	}
	if after.Count != 1 || len(after.Hits) != 1 {
		t.Fatalf("expected one derived hit after reconcile, got %+v", after)
	}
	if after.Hits[0].MemoryID != "memnode:knowledge_claim:"+claim.ClaimID {
		t.Fatalf("expected knowledge claim hit after reconcile, got %+v", after.Hits[0])
	}
	if after.BoundaryContract.ProjectionCoverage != "CURRENT" || after.BoundaryContract.ProjectionLagState != "ok" {
		t.Fatalf("expected current boundary after reconcile, got %+v", after.BoundaryContract)
	}
	if after.BoundaryContract.ProjectionPendingCount != 0 || after.BoundaryContract.ProjectionProcessingCount != 0 || after.BoundaryContract.ProjectionFailedCount != 0 {
		t.Fatalf("expected cleared projection lag counters after reconcile, got %+v", after.BoundaryContract)
	}
}

func TestSearchMemoryNodesDoesNotHealArchivedKnowledgeClaimProjectionOnRead(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-search-claim-archive-lag"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Search Claim Archive Lag",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "decision",
		Status:      "active",
		Subject:     "Archive boundary stays lagged",
		Body:        "Repeated reads must not auto-heal archived knowledge-claim graph projection lag.",
		Summary:     "Archive lag stays bounded.",
		Confidence:  0.72,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	if _, err := store.ArchiveKnowledgeClaim(ctx, KnowledgeClaimArchiveInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ArchivedBy:  "developer",
		Reason:      "cleanup",
	}); err != nil {
		t.Fatalf("archive knowledge claim: %v", err)
	}

	beforeNodes := countMemoryPacketRows(t, ctx, store, "memory_nodes")

	first, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID:     workspaceID,
		Query:           "auto-heal archived knowledge",
		OriginKind:      "knowledge_claim",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("search memory nodes after archive before reconcile: %v", err)
	}
	if first.Count != 1 || len(first.Hits) != 1 {
		t.Fatalf("expected stale derived hit before reconcile, got %+v", first)
	}
	if first.BoundaryContract.ProjectionCoverage != "PARTIAL" || first.BoundaryContract.ProjectionLagState != "degraded" || first.BoundaryContract.ProjectionPendingCount < 1 {
		t.Fatalf("expected partial degraded boundary after archive before reconcile, got %+v", first.BoundaryContract)
	}
	if first.Hits[0].LifecycleState == "ARCHIVED" {
		t.Fatalf("expected derived hit to remain pre-archive until explicit reconcile, got %+v", first.Hits[0])
	}

	second, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID:     workspaceID,
		Query:           "auto-heal archived knowledge",
		OriginKind:      "knowledge_claim",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("second search memory nodes after archive before reconcile: %v", err)
	}
	if second.Count != first.Count || len(second.Hits) != len(first.Hits) {
		t.Fatalf("expected repeated reads to preserve stale search shape, first=%+v second=%+v", first, second)
	}
	if second.BoundaryContract != first.BoundaryContract {
		t.Fatalf("expected repeated reads to preserve lag boundary, first=%+v second=%+v", first.BoundaryContract, second.BoundaryContract)
	}
	if second.Hits[0].LifecycleState != first.Hits[0].LifecycleState {
		t.Fatalf("expected repeated reads not to heal lifecycle state, first=%+v second=%+v", first.Hits[0], second.Hits[0])
	}
	if afterNodes := countMemoryPacketRows(t, ctx, store, "memory_nodes"); afterNodes != beforeNodes {
		t.Fatalf("expected repeated reads to leave memory_nodes unchanged, before=%d after=%d", beforeNodes, afterNodes)
	}

	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	after, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID:     workspaceID,
		Query:           "auto-heal archived knowledge",
		OriginKind:      "knowledge_claim",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("search memory nodes after explicit reconcile: %v", err)
	}
	if after.Count != 1 || len(after.Hits) != 1 {
		t.Fatalf("expected archived hit after explicit reconcile, got %+v", after)
	}
	if after.BoundaryContract.ProjectionCoverage != "CURRENT" || after.BoundaryContract.ProjectionLagState != "ok" {
		t.Fatalf("expected current boundary after explicit reconcile, got %+v", after.BoundaryContract)
	}
	if after.Hits[0].LifecycleState != "ARCHIVED" {
		t.Fatalf("expected archived lifecycle after explicit reconcile, got %+v", after.Hits[0])
	}
}

func TestSearchMemoryNodesSurfacesAnchorStateFieldsWithoutBreakingCompactHitContract(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-search-anchor-state"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Search Anchor State",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Anchor-state search",
		Body:        "Anchor-state fields should surface without breaking compact hit shape.",
		Summary:     "Anchor-state search.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	lineageID := "lineage:" + record.MemoryID
	lastAnyAccess := time.Now().UTC().Add(-15 * time.Minute).Format(time.RFC3339Nano)
	lastTrustedAccess := time.Now().UTC().Add(-90 * time.Minute).Format(time.RFC3339Nano)
	tLife := 86400.0
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE memory_nodes
		   SET semantic_lineage_id = ?, revision = ?, protect = ?, unresolved = ?
		 WHERE workspace_id = ? AND memory_id = ?
	`, lineageID, 7, 1, 1, workspaceID, nodeID); err != nil {
		t.Fatalf("seed memory node anchor-state fields: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(memory_id) DO UPDATE SET
			t_i_star = excluded.t_i_star,
			t_i_acc = excluded.t_i_acc,
			h_i = excluded.h_i,
			t_hot = excluded.t_hot,
			t_warm = excluded.t_warm,
			t_gc = excluded.t_gc,
			updated_at = excluded.updated_at
	`, nodeID, workspaceID, 0.9, lastTrustedAccess, lastAnyAccess, 4, 0.2, tLife, lastAnyAccess, lastAnyAccess, lastAnyAccess, lastAnyAccess); err != nil {
		t.Fatalf("seed memory node salience: %v", err)
	}

	result, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "Anchor-state search",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search memory nodes: %v", err)
	}
	if result.Count != 1 || len(result.Hits) != 1 {
		t.Fatalf("expected one search hit, got %+v", result)
	}
	if result.TimeAuthority.WorkspaceID != workspaceID || result.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected search time authority, got %+v", result.TimeAuthority)
	}
	hit := result.Hits[0]
	if hit.Snippet == "" || hit.RefCount == 0 || hit.MemoryID != nodeID {
		t.Fatalf("expected compact hit contract to stay intact, got %+v", hit)
	}

	hitJSON, err := json.Marshal(hit)
	if err != nil {
		t.Fatalf("marshal search hit: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(hitJSON, &payload); err != nil {
		t.Fatalf("decode search hit: %v", err)
	}

	if payload["semantic_lineage_id"] != lineageID {
		t.Fatalf("expected semantic_lineage_id %q, got payload %+v", lineageID, payload)
	}
	if got, ok := payload["revision"].(float64); !ok || int(got) != 7 {
		t.Fatalf("expected revision 7, got payload %+v", payload)
	}
	if got, ok := payload["protect"].(bool); !ok || !got {
		t.Fatalf("expected protect=true, got payload %+v", payload)
	}
	if got, ok := payload["unresolved"].(bool); !ok || !got {
		t.Fatalf("expected unresolved=true, got payload %+v", payload)
	}
	if payload["last_any_access"] != lastAnyAccess {
		t.Fatalf("expected last_any_access %q, got payload %+v", lastAnyAccess, payload)
	}
	if payload["last_trusted_access"] != lastTrustedAccess {
		t.Fatalf("expected last_trusted_access %q, got payload %+v", lastTrustedAccess, payload)
	}
	if got, ok := payload["t_life"].(float64); !ok || got != tLife {
		t.Fatalf("expected t_life %v, got payload %+v", tLife, payload)
	}
	if payload["retention_band"] != "PRUNABLE" {
		t.Fatalf("expected retention_band PRUNABLE from expired thresholds, got payload %+v", payload)
	}
	if got, ok := payload["retention_prunable"].(bool); !ok || got {
		t.Fatalf("expected protected/unresolved hit to stay non-prunable, got payload %+v", payload)
	}
	if payload["retention_guard_reason"] != "PROTECT" {
		t.Fatalf("expected protect guard reason to win in compact hit payload, got %+v", payload)
	}
	if payload["retention_hot_until"] != lastAnyAccess || payload["retention_warm_until"] != lastAnyAccess || payload["retention_expires_at"] != lastAnyAccess {
		t.Fatalf("expected additive retention thresholds in compact hit payload, got %+v", payload)
	}
}

func TestSearchMemoryNodesSurfacesRetentionBandAndExpiryWithoutBreakingCompactHitContract(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-search-retention"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Search Retention",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Retention search",
		Body:        "Retention band and expiry should surface without breaking compact hit shape.",
		Summary:     "Retention search.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	now := time.Now().UTC()
	hotAt := now.Add(20 * time.Minute).Format(time.RFC3339Nano)
	warmAt := now.Add(80 * time.Minute).Format(time.RFC3339Nano)
	gcAt := now.Add(4 * time.Hour).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(memory_id) DO UPDATE SET
			a_i = excluded.a_i,
			t_i_star = excluded.t_i_star,
			t_i_acc = excluded.t_i_acc,
			n_i = excluded.n_i,
			q_i = excluded.q_i,
			h_i = excluded.h_i,
			t_hot = excluded.t_hot,
			t_warm = excluded.t_warm,
			t_gc = excluded.t_gc,
			updated_at = excluded.updated_at
	`, nodeID, workspaceID, 0.9, now.Add(-90*time.Minute).Format(time.RFC3339Nano), now.Add(-10*time.Minute).Format(time.RFC3339Nano), 4, 0.2, 86400.0, hotAt, warmAt, gcAt, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed memory node salience: %v", err)
	}

	result, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "Retention search",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search memory nodes: %v", err)
	}
	if result.Count != 1 || len(result.Hits) != 1 {
		t.Fatalf("expected one search hit, got %+v", result)
	}
	hit := result.Hits[0]
	if hit.Snippet == "" || hit.RefCount == 0 || hit.MemoryID != nodeID {
		t.Fatalf("expected compact hit contract to stay intact, got %+v", hit)
	}

	hitJSON, err := json.Marshal(hit)
	if err != nil {
		t.Fatalf("marshal search hit: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(hitJSON, &payload); err != nil {
		t.Fatalf("decode search hit: %v", err)
	}

	if payload["retention_hot_until"] != hotAt || payload["retention_warm_until"] != warmAt || payload["retention_expires_at"] != gcAt {
		t.Fatalf("expected exact retention expiry surfaces, got %+v", payload)
	}
	if got, ok := payload["retention_band"].(string); !ok || strings.TrimSpace(got) == "" {
		t.Fatalf("expected non-empty retention_band, got %+v", payload)
	}
	if got, ok := payload["retention_prunable"].(bool); !ok || got {
		t.Fatalf("expected future-threshold hit to stay non-prunable, got %+v", payload)
	}
	if _, ok := payload["retention_guard_reason"]; ok {
		t.Fatalf("did not expect guard reason for ordinary retention hit, got %+v", payload)
	}
	if _, ok := payload["semantic_lineage_id"]; !ok || payload["snippet"] == "" {
		t.Fatalf("expected anchor/compact hit contract to remain intact, got %+v", payload)
	}
}

func TestSearchMemoryNodesShowsAutoPrunedWorkspaceMemoryUntilRestore(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-node-search-auto-pruned"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Search Auto-Pruned",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Recoverable rollout note",
		Body:        "Auto-pruned workspace memory should stay searchable through include_archived and recover on restore.",
		Summary:     "Recoverable rollout note.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.7,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	now := time.Now().UTC()
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	pastStar := now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	pastAcc := now.Add(-4 * time.Hour).Format(time.RFC3339Nano)
	pastHot := now.Add(-3 * time.Hour).Format(time.RFC3339Nano)
	pastWarm := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	pastGc := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?)
	`, nodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc); err != nil {
		t.Fatalf("seed salience row for auto-pruned search: %v", err)
	}

	pruned, err := store.RMPRunBatchedPruning(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("run batched pruning: %v", err)
	}
	if len(pruned) != 1 || pruned[0] != nodeID {
		t.Fatalf("expected auto-pruned node set, got %+v", pruned)
	}

	defaultResult, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "Recoverable rollout note",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search memory nodes after auto-prune: %v", err)
	}
	if defaultResult.Count != 0 {
		t.Fatalf("expected auto-pruned node to stay hidden from default search, got %+v", defaultResult.Hits)
	}

	withArchived, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID:     workspaceID,
		Query:           "Recoverable rollout note",
		OriginKind:      "workspace_memory",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("search memory nodes with archived after auto-prune: %v", err)
	}
	if withArchived.Count != 1 {
		t.Fatalf("expected archived auto-pruned node with include_archived, got %+v", withArchived.Hits)
	}
	if withArchived.Hits[0].MemoryID != nodeID || withArchived.Hits[0].LifecycleState != "ARCHIVED" {
		t.Fatalf("expected archived auto-pruned hit, got %+v", withArchived.Hits[0])
	}
	if withArchived.Hits[0].ArchivedReason != "rmp_gc_expired" || withArchived.Hits[0].RecoveryCandidate || withArchived.Hits[0].RecoveryGuardReason != "NO_TRIGGERED_LINKAGE" {
		t.Fatalf("expected archived auto-pruned hit to surface bounded recovery hook semantics, got %+v", withArchived.Hits[0])
	}

	if _, err := store.RestoreWorkspaceMemory(ctx, WorkspaceMemoryRestoreInput{
		WorkspaceID:    workspaceID,
		MemoryID:       record.MemoryID,
		RestoredBy:     "developer",
		RecoveryReason: "rmp_gc_reactivated",
	}); err != nil {
		t.Fatalf("restore auto-pruned workspace memory: %v", err)
	}

	restoredResult, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "Recoverable rollout note",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search memory nodes after restore: %v", err)
	}
	if restoredResult.Count != 1 {
		t.Fatalf("expected restored node back in default search, got %+v", restoredResult.Hits)
	}
	if restoredResult.Hits[0].MemoryID != nodeID || restoredResult.Hits[0].LifecycleState != "ACTIVE" {
		t.Fatalf("expected restored active hit after recoverable archive, got %+v", restoredResult.Hits[0])
	}
	if restoredResult.Hits[0].RecoveryReason != "rmp_gc_reactivated" {
		t.Fatalf("expected restored hit to keep recovery reason, got %+v", restoredResult.Hits[0])
	}
}

func TestSearchMemoryNodesSurfacesRecoveryCandidateForArchivedExpiredDriftedDocMemory(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-node-search-recovery-candidate"
		docKey      = "runbook-search-recovery-candidate"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Node Search Recovery Candidate",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Recovery Candidate Runbook",
		Content:     "# Recovery\nDocumented rollout contract.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Drifted recovery candidate",
		Body:        "Expired archived doc-backed memory should surface a recovery candidate when its doc drifts.",
		Summary:     "Drifted recovery candidate.",
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Importance:  0.6,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "rmp_pruner",
		Reason:      rmpArchivedReasonExpired,
	}); err != nil {
		t.Fatalf("archive workspace memory as expired: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Recovery Candidate Runbook",
		Content:     "# Recovery\nDocumented rollout contract changed.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("update workspace doc: %v", err)
	}

	result, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID:     workspaceID,
		Query:           "Drifted recovery candidate",
		OriginKind:      "workspace_memory",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("search archived drifted memory nodes: %v", err)
	}
	if result.Count != 1 {
		t.Fatalf("expected one archived drifted hit, got %+v", result.Hits)
	}
	hit := result.Hits[0]
	if hit.DriftState != "STALE" || !hit.RecoveryCandidate || hit.RecoveryTriggerCount == 0 {
		t.Fatalf("expected archived drifted hit to surface recovery candidate, got %+v", hit)
	}
	if len(hit.RecoveryTriggerKinds) != 1 || hit.RecoveryTriggerKinds[0] != "workspace_doc" {
		t.Fatalf("expected workspace_doc recovery trigger kind, got %+v", hit)
	}
	if hit.RecoveryGuardReason != "" || hit.ArchivedReason != "rmp_gc_expired" {
		t.Fatalf("expected bounded expired-archive recovery surface on hit, got %+v", hit)
	}
}
