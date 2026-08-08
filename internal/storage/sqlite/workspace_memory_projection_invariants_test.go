package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestEvaluateWorkspaceMemoryProjectionInvariant_MissingAnchorSettledIsDrift(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	record := seedWorkspaceMemoryInvariantRecord(t, ctx, store, "ws-memory-invariant-missing")
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, record.WorkspaceID)
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, record.WorkspaceID, nodeID); err != nil {
		t.Fatalf("delete expected compatibility anchor: %v", err)
	}

	lag, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag snapshot: %v", err)
	}
	invariant, err := store.EvaluateWorkspaceMemoryProjectionInvariant(ctx, record, lag)
	if err != nil {
		t.Fatalf("evaluate invariant: %v", err)
	}
	if invariant.State != WorkspaceMemoryProjectionInvariantDrift {
		t.Fatalf("expected settled missing anchor to be drift, got %+v", invariant)
	}
	if !workspaceMemoryProjectionIssuePresent(invariant.Issues, "MISSING_ANCHOR") {
		t.Fatalf("expected missing-anchor issue, got %+v", invariant.Issues)
	}
}

func TestEvaluateWorkspaceMemoryProjectionInvariant_BacklogMissingAnchorIsLagging(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	record := seedWorkspaceMemoryInvariantRecord(t, ctx, store, "ws-memory-invariant-lagging")
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, record.WorkspaceID, nodeID); err != nil {
		t.Fatalf("delete expected compatibility anchor: %v", err)
	}

	now := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	if err := insertMemoryProjectionOutboxRow(ctx, store, memoryProjectionOutboxSeed{
		projectionID:   "mproj:lagging:" + record.MemoryID,
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

	lag, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag snapshot: %v", err)
	}
	invariant, err := store.EvaluateWorkspaceMemoryProjectionInvariant(ctx, record, lag)
	if err != nil {
		t.Fatalf("evaluate invariant: %v", err)
	}
	if invariant.State != WorkspaceMemoryProjectionInvariantLagging {
		t.Fatalf("expected lagging state for missing anchor during backlog, got %+v", invariant)
	}
	if !workspaceMemoryProjectionIssuePresent(invariant.Issues, "MISSING_ANCHOR") {
		t.Fatalf("expected missing-anchor issue, got %+v", invariant.Issues)
	}
}

func TestEvaluateWorkspaceMemoryProjectionInvariant_CorruptedOriginIsDrift(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	record := seedWorkspaceMemoryInvariantRecord(t, ctx, store, "ws-memory-invariant-origin")
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	if _, err := store.DB().ExecContext(ctx, `UPDATE memory_nodes SET origin_kind = ?, updated_at = ? WHERE workspace_id = ? AND memory_id = ?`,
		"knowledge_claim",
		time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		record.WorkspaceID,
		nodeID,
	); err != nil {
		t.Fatalf("corrupt derived origin_kind: %v", err)
	}

	lag, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag snapshot: %v", err)
	}
	invariant, err := store.EvaluateWorkspaceMemoryProjectionInvariant(ctx, record, lag)
	if err != nil {
		t.Fatalf("evaluate invariant: %v", err)
	}
	if invariant.State != WorkspaceMemoryProjectionInvariantDrift {
		t.Fatalf("expected corrupted origin to be drift, got %+v", invariant)
	}
	if !workspaceMemoryProjectionIssuePresent(invariant.Issues, "ORIGIN_KIND_MISMATCH") {
		t.Fatalf("expected origin-kind mismatch issue, got %+v", invariant.Issues)
	}
}

func TestEvaluateWorkspaceMemoryProjectionInvariant_SettledContentMismatchIsDrift(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	record := seedWorkspaceMemoryInvariantRecord(t, ctx, store, "ws-memory-invariant-mismatch")
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, record.WorkspaceID)
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	if _, err := store.DB().ExecContext(ctx, `UPDATE memory_nodes SET summary = ?, updated_at = ? WHERE workspace_id = ? AND memory_id = ?`,
		"stale derived summary",
		time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		record.WorkspaceID,
		nodeID,
	); err != nil {
		t.Fatalf("mutate derived summary: %v", err)
	}

	lag, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag snapshot: %v", err)
	}
	invariant, err := store.EvaluateWorkspaceMemoryProjectionInvariant(ctx, record, lag)
	if err != nil {
		t.Fatalf("evaluate invariant: %v", err)
	}
	if invariant.State != WorkspaceMemoryProjectionInvariantDrift {
		t.Fatalf("expected settled content mismatch to be drift, got %+v", invariant)
	}
	if !workspaceMemoryProjectionIssuePresent(invariant.Issues, "SUMMARY_MISMATCH") {
		t.Fatalf("expected summary-mismatch issue, got %+v", invariant.Issues)
	}
}

func TestEvaluateWorkspaceMemoryProjectionInvariant_UnknownLagDoesNotReportCurrent(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	record := seedWorkspaceMemoryInvariantRecord(t, ctx, store, "ws-memory-invariant-unknown")
	invariant, err := store.EvaluateWorkspaceMemoryProjectionInvariant(ctx, record, MemoryProjectionLagSnapshot{
		State:   "unknown",
		Message: "projection lag snapshot unavailable",
		Error:   "forced test error",
	})
	if err != nil {
		t.Fatalf("evaluate invariant: %v", err)
	}
	if invariant.State != WorkspaceMemoryProjectionInvariantUnknown {
		t.Fatalf("expected unknown lag to avoid current invariant state, got %+v", invariant)
	}
}

func seedWorkspaceMemoryInvariantRecord(t *testing.T, ctx context.Context, store *Store, workspaceID string) WorkspaceMemoryRecord {
	t.Helper()
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Projection Invariant",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Canonical workspace memory",
		Body:        "Canonical workspace memory should remain authoritative over derived replicas.",
		Summary:     "Canonical workspace memory summary.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	return record
}

func workspaceMemoryProjectionIssuePresent(items []WorkspaceMemoryProjectionInvariantIssue, code string) bool {
	for _, item := range items {
		if item.Code == code {
			return true
		}
	}
	return false
}
