package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestRepairWorkspaceMemoryProjectionWorkspaceRepairsConflictingAnchorNodeID(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	record := seedWorkspaceMemoryInvariantRecord(t, ctx, store, "ws-memory-projection-repair")
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, record.WorkspaceID)
	expectedNodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, record.WorkspaceID, expectedNodeID); err != nil {
		t.Fatalf("delete expected compatibility anchor: %v", err)
	}
	if err := insertConflictingWorkspaceMemoryProjectionAnchor(ctx, store, record, "memnode:workspace_memory:legacy:"+record.MemoryID, "stale conflicting summary"); err != nil {
		t.Fatalf("insert conflicting compatibility anchor: %v", err)
	}

	result, err := store.RepairWorkspaceMemoryProjectionWorkspace(ctx, WorkspaceMemoryProjectionRepairFilter{
		WorkspaceID: record.WorkspaceID,
		MemoryIDs:   []string{record.MemoryID},
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("repair workspace memory projection workspace: %v", err)
	}
	if result.Examined != 1 || result.Repaired != 1 {
		t.Fatalf("expected one repaired workspace memory projection, got %+v", result)
	}
	if result.DeletedCompetingAnchors != 1 {
		t.Fatalf("expected one competing anchor deletion, got %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].Action != workspaceMemoryProjectionRepairActionRepaired {
		t.Fatalf("expected repaired action item, got %+v", result.Items)
	}

	lag, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag snapshot: %v", err)
	}
	invariant, err := store.EvaluateWorkspaceMemoryProjectionInvariant(ctx, record, lag)
	if err != nil {
		t.Fatalf("evaluate repaired invariant: %v", err)
	}
	if invariant.State != WorkspaceMemoryProjectionInvariantCurrent || len(invariant.Issues) != 0 {
		t.Fatalf("expected repaired invariant to become current, got %+v", invariant)
	}
	if invariant.Node.Summary != record.Summary {
		t.Fatalf("expected repaired summary %q, got %+v", record.Summary, invariant.Node)
	}
}

func TestRepairWorkspaceMemoryProjectionWorkspaceIsBoundedByLimit(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-repair-limit"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Projection Repair Limit",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	first, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "First canonical row",
		Body:        "First canonical row for bounded repair.",
		Summary:     "First bounded repair row.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record first workspace memory: %v", err)
	}
	second, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Second canonical row",
		Body:        "Second canonical row for bounded repair.",
		Summary:     "Second bounded repair row.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record second workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	for _, memoryID := range []string{first.MemoryID, second.MemoryID} {
		if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`,
			workspaceID,
			memoryGraphNodeID("workspace_memory", memoryID),
		); err != nil {
			t.Fatalf("delete compatibility anchor for %s: %v", memoryID, err)
		}
	}

	result, err := store.RepairWorkspaceMemoryProjectionWorkspace(ctx, WorkspaceMemoryProjectionRepairFilter{
		WorkspaceID: workspaceID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("bounded repair workspace memory projection workspace: %v", err)
	}
	if result.Examined != 1 || result.Repaired != 1 {
		t.Fatalf("expected bounded repair to process only one row, got %+v", result)
	}

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(1) FROM memory_nodes WHERE workspace_id = ? AND origin_kind = 'workspace_memory'`, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count repaired workspace memory anchors: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly one repaired anchor after bounded run, got %d", count)
	}
}

func TestRepairWorkspaceMemoryProjectionWorkspaceSkipsBacklogOnlyRows(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	record := seedWorkspaceMemoryInvariantRecord(t, ctx, store, "ws-memory-projection-repair-backlog")
	now := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	if err := insertMemoryProjectionOutboxRow(ctx, store, memoryProjectionOutboxSeed{
		projectionID:   "mproj:backlog-only:" + record.MemoryID,
		workspaceID:    record.WorkspaceID,
		projectionKind: memoryProjectionKindWorkspaceMemory,
		originID:       record.MemoryID,
		status:         memoryProjectionStatusProcessing,
		availableAt:    now,
		enqueuedAt:     now,
		updatedAt:      now,
	}); err != nil {
		t.Fatalf("seed processing projection row: %v", err)
	}

	result, err := store.RepairWorkspaceMemoryProjectionWorkspace(ctx, WorkspaceMemoryProjectionRepairFilter{
		WorkspaceID: record.WorkspaceID,
		MemoryIDs:   []string{record.MemoryID},
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("repair backlog-only workspace memory projection: %v", err)
	}
	if result.Repaired != 0 || result.SkippedBacklogOnly != 1 {
		t.Fatalf("expected backlog-only repair to no-op, got %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].Action != workspaceMemoryProjectionRepairActionSkipBacklogOnly {
		t.Fatalf("expected backlog-only skip action, got %+v", result.Items)
	}
}

func TestRepairWorkspaceMemoryProjectionWorkspaceSkipsRepairWhenLagUntrusted(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	record := seedWorkspaceMemoryInvariantRecord(t, ctx, store, "ws-memory-projection-repair-untrusted-lag")
	expectedNodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, record.WorkspaceID, expectedNodeID); err != nil {
		t.Fatalf("delete expected compatibility anchor: %v", err)
	}
	now := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	if err := insertMemoryProjectionOutboxRow(ctx, store, memoryProjectionOutboxSeed{
		projectionID:   "mproj:untrusted-lag:" + record.MemoryID,
		workspaceID:    record.WorkspaceID,
		projectionKind: memoryProjectionKindWorkspaceMemory,
		originID:       record.MemoryID,
		status:         memoryProjectionStatusProcessing,
		availableAt:    now,
		enqueuedAt:     now,
		updatedAt:      now,
	}); err != nil {
		t.Fatalf("seed processing projection row: %v", err)
	}

	result, err := store.RepairWorkspaceMemoryProjectionWorkspace(ctx, WorkspaceMemoryProjectionRepairFilter{
		WorkspaceID: record.WorkspaceID,
		MemoryIDs:   []string{record.MemoryID},
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("repair workspace memory projection with untrusted lag: %v", err)
	}
	if result.Repaired != 0 || result.SkippedUntrustedLag != 1 {
		t.Fatalf("expected untrusted lag to block repair, got %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].Action != workspaceMemoryProjectionRepairActionSkipUntrustedLag {
		t.Fatalf("expected untrusted-lag skip action, got %+v", result.Items)
	}
	if _, err := store.GetMemoryGraphNode(ctx, record.WorkspaceID, expectedNodeID); err == nil {
		t.Fatalf("expected missing compatibility anchor to remain missing while lag is untrusted")
	}
}

func TestRepairWorkspaceMemoryProjectionWorkspaceIsIdempotentAfterRepair(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	record := seedWorkspaceMemoryInvariantRecord(t, ctx, store, "ws-memory-projection-repair-idempotent")
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, record.WorkspaceID)
	expectedNodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, record.WorkspaceID, expectedNodeID); err != nil {
		t.Fatalf("delete expected compatibility anchor: %v", err)
	}

	first, err := store.RepairWorkspaceMemoryProjectionWorkspace(ctx, WorkspaceMemoryProjectionRepairFilter{
		WorkspaceID: record.WorkspaceID,
		MemoryIDs:   []string{record.MemoryID},
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("first repair workspace memory projection: %v", err)
	}
	if first.Repaired != 1 || first.Examined != 1 {
		t.Fatalf("expected first repair to restore one anchor, got %+v", first)
	}

	second, err := store.RepairWorkspaceMemoryProjectionWorkspace(ctx, WorkspaceMemoryProjectionRepairFilter{
		WorkspaceID: record.WorkspaceID,
		MemoryIDs:   []string{record.MemoryID},
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("second repair workspace memory projection: %v", err)
	}
	if second.Repaired != 0 || second.SkippedCurrent != 1 {
		t.Fatalf("expected second repair to no-op on current anchor, got %+v", second)
	}
	if len(second.Items) != 1 || second.Items[0].Action != workspaceMemoryProjectionRepairActionSkipCurrent {
		t.Fatalf("expected second repair to skip current anchor, got %+v", second.Items)
	}
}

func TestWorkspaceMemoryProjectionRepairActionForSkipsUnknownLagEvenWhenIssueIsRepairable(t *testing.T) {
	action := workspaceMemoryProjectionRepairActionFor(WorkspaceMemoryProjectionInvariant{
		State:              WorkspaceMemoryProjectionInvariantUnknown,
		ProjectionLagState: "unknown",
		Issues: []WorkspaceMemoryProjectionInvariantIssue{
			workspaceMemoryProjectionIssue("SUMMARY_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "summary drift"),
		},
	})
	if action != workspaceMemoryProjectionRepairActionSkipUntrustedLag {
		t.Fatalf("expected unknown lag to block repair, got %q", action)
	}
}

func insertConflictingWorkspaceMemoryProjectionAnchor(ctx context.Context, store *Store, record WorkspaceMemoryRecord, competingNodeID, summary string) error {
	node, _, _ := memoryGraphNodeFromWorkspaceMemory(record)
	node.MemoryID = competingNodeID
	node.Summary = summary
	now := time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano)
	_, err := store.DB().ExecContext(
		ctx,
		`INSERT INTO memory_nodes(
		    memory_id, workspace_id, memory_type, compat_type, semantic_lineage_id, revision, protect, unresolved,
		    visibility, memory_layer, epistemic_status, lifecycle_state, origin_kind, origin_id, source_kind, source_id,
		    agent_id, session_id, task_id, title, body, summary, claim_subject, claim_predicate, claim_object,
		    claim_qualifiers_json, claim_time_scope_json, claim_modality, source_set_json, provenance_json,
		    temperature, importance, confidence, activation, drift, volatility, pin_strength,
		    archived_at, archived_reason, recovery_reason, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		node.MemoryID,
		node.WorkspaceID,
		node.MemoryType,
		node.CompatType,
		node.SemanticLineageID,
		node.Revision,
		node.Protect,
		node.Unresolved,
		node.Visibility,
		node.MemoryLayer,
		node.EpistemicStatus,
		node.LifecycleState,
		node.OriginKind,
		node.OriginID,
		node.SourceKind,
		node.SourceID,
		node.AgentID,
		node.SessionID,
		node.TaskID,
		node.Title,
		node.Body,
		node.Summary,
		node.ClaimSubject,
		node.ClaimPredicate,
		node.ClaimObject,
		node.ClaimQualifiersJSON,
		node.ClaimTimeScopeJSON,
		node.ClaimModality,
		node.SourceSetJSON,
		node.ProvenanceJSON,
		node.Temperature,
		node.Importance,
		node.Confidence,
		node.Activation,
		node.Drift,
		node.Volatility,
		node.PinStrength,
		node.ArchivedAt,
		node.ArchivedReason,
		node.RecoveryReason,
		node.CreatedAt,
		now,
	)
	return err
}
