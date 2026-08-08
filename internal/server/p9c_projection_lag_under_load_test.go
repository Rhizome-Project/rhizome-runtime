package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryProjectionLagUnderLoadStaysCompatibilityOnlyUntilReconcile(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-p9c-projection-lag-under-load"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Projection Lag Under Load",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	records := make([]sqlite.WorkspaceMemoryRecord, 0, 6)
	for i := 0; i < 6; i++ {
		record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
			WorkspaceID: workspaceID,
			MemoryType:  "lesson",
			Title:       "Load validation memory " + string(rune('A'+i)),
			Body:        "Canonical workspace memory must stay authoritative while projection reconciliation is active for item " + string(rune('A'+i)) + ".",
			Summary:     "Projection lag should not promote derived authority for item " + string(rune('A'+i)) + ".",
			SourceKind:  "manual",
			SourceID:    "developer-" + string(rune('A'+i)),
		})
		if err != nil {
			t.Fatalf("record workspace memory %d: %v", i, err)
		}
		records = append(records, record)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceMemoryListParams{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}

	beforeResult, rpcErr := h.workspaceMemoryList(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList before backlog rpc error: %+v", rpcErr)
	}
	beforeEntries := beforeResult.(map[string]any)["entries"].([]workspaceMemoryView)
	if len(beforeEntries) != len(records) {
		t.Fatalf("expected %d baseline entries, got %d", len(records), len(beforeEntries))
	}
	assertWorkspaceMemoryAuthorityState(t, beforeEntries, "DERIVED_READY", sqlite.WorkspaceMemoryProjectionInvariantCurrent, "ok")

	rebuildResult, err := store.RebuildMemoryProjectionWorkspace(ctx, sqlite.MemoryProjectionRebuildFilter{
		WorkspaceID: workspaceID,
		Kinds:       []string{"workspace_memory"},
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("rebuild memory projection workspace: %v", err)
	}
	if rebuildResult.TargetsQueued != len(records) {
		t.Fatalf("expected %d queued projection targets, got %+v", len(records), rebuildResult)
	}
	if rebuildResult.Processed != 1 || rebuildResult.Pending != len(records)-1 || rebuildResult.FailedPending != 0 {
		t.Fatalf("expected bounded backlog after rebuild, got %+v", rebuildResult)
	}

	processingProjectionID := markOneWorkspaceMemoryProjectionOutboxRowProcessing(t, store, ctx, workspaceID)

	lagBeforeRead, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag before read: %v", err)
	}
	if lagBeforeRead.State != "degraded" {
		t.Fatalf("expected degraded lag state before read, got %+v", lagBeforeRead)
	}
	if lagBeforeRead.PendingCount != len(records)-2 || lagBeforeRead.ProcessingCount != 1 || lagBeforeRead.FailedCount != 0 {
		t.Fatalf("unexpected lag snapshot before read: %+v", lagBeforeRead)
	}

	duringResult, rpcErr := h.workspaceMemoryList(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList during lag rpc error: %+v", rpcErr)
	}
	duringEntries := duringResult.(map[string]any)["entries"].([]workspaceMemoryView)
	if len(duringEntries) != len(records) {
		t.Fatalf("expected %d entries during lag, got %d", len(records), len(duringEntries))
	}
	assertWorkspaceMemoryAuthorityState(t, duringEntries, "DERIVED_STALE", sqlite.WorkspaceMemoryProjectionInvariantLagging, "degraded")

	lagAfterRead, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag after read: %v", err)
	}
	if lagAfterRead.State != lagBeforeRead.State ||
		lagAfterRead.PendingCount != lagBeforeRead.PendingCount ||
		lagAfterRead.ProcessingCount != lagBeforeRead.ProcessingCount ||
		lagAfterRead.FailedCount != lagBeforeRead.FailedCount {
		t.Fatalf("expected read-side inspection to avoid healing lag, before=%+v after=%+v", lagBeforeRead, lagAfterRead)
	}

	markWorkspaceMemoryProjectionOutboxRowPending(t, store, ctx, workspaceID, processingProjectionID)

	reconcileResult, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, len(records))
	if err != nil {
		t.Fatalf("reconcile memory projection workspace: %v", err)
	}
	if reconcileResult.Processed != len(records)-1 || reconcileResult.Failed != 0 {
		t.Fatalf("expected remaining backlog to reconcile cleanly, got %+v", reconcileResult)
	}

	lagSettled, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag after reconcile: %v", err)
	}
	if lagSettled.State != "ok" || lagSettled.PendingCount != 0 || lagSettled.ProcessingCount != 0 || lagSettled.FailedCount != 0 {
		t.Fatalf("expected settled lag snapshot after reconcile, got %+v", lagSettled)
	}

	afterResult, rpcErr := h.workspaceMemoryList(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList after reconcile rpc error: %+v", rpcErr)
	}
	afterEntries := afterResult.(map[string]any)["entries"].([]workspaceMemoryView)
	if len(afterEntries) != len(records) {
		t.Fatalf("expected %d entries after reconcile, got %d", len(records), len(afterEntries))
	}
	assertWorkspaceMemoryAuthorityState(t, afterEntries, "DERIVED_READY", sqlite.WorkspaceMemoryProjectionInvariantCurrent, "ok")
}

func assertWorkspaceMemoryAuthorityState(t *testing.T, entries []workspaceMemoryView, wantStatus, wantInvariant, wantLag string) {
	t.Helper()
	if len(entries) == 0 {
		t.Fatalf("expected non-empty workspace memory entry set")
	}
	for _, entry := range entries {
		if entry.Record.MemoryID == "" {
			t.Fatalf("expected canonical workspace_memory record to remain readable, got %+v", entry)
		}
		if entry.Meta.CanonicalAuthority != "workspace_memory" || entry.Meta.AnchorAuthority != "compatibility_only" {
			t.Fatalf("expected mixed-state authority markers, got %+v", entry.Meta)
		}
		if entry.Meta.AnchorStatus != wantStatus {
			t.Fatalf("expected anchor status %s, got %+v", wantStatus, entry.Meta)
		}
		if entry.Meta.AnchorInvariantState != wantInvariant {
			t.Fatalf("expected anchor invariant %s, got %+v", wantInvariant, entry.Meta)
		}
		if entry.Meta.AnchorProjectionLagState != wantLag {
			t.Fatalf("expected anchor lag state %s, got %+v", wantLag, entry.Meta)
		}
		if entry.Meta.AnchorSemanticLineageID != "workspace_memory:"+entry.Record.MemoryID {
			t.Fatalf("expected anchor semantic lineage to remain tied to canonical row, got %+v", entry.Meta)
		}
		if entry.Meta.AnchorRevision < 1 {
			t.Fatalf("expected derived anchor revision to stay populated, got %+v", entry.Meta)
		}
		if wantStatus == "DERIVED_STALE" && entry.Meta.AnchorStatusReason == "" {
			t.Fatalf("expected stale anchor to explain lag semantics, got %+v", entry.Meta)
		}
	}
}

func markOneWorkspaceMemoryProjectionOutboxRowProcessing(t *testing.T, store *sqlite.Store, ctx context.Context, workspaceID string) string {
	t.Helper()

	var projectionID string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT projection_id
		  FROM memory_projection_outbox
		 WHERE workspace_id = ?
		   AND projection_kind = 'WORKSPACE_MEMORY'
		   AND status = 'PENDING'
		 ORDER BY projection_id ASC
		 LIMIT 1
	`, workspaceID).Scan(&projectionID); err != nil {
		t.Fatalf("select pending workspace-memory projection row: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE memory_projection_outbox
		   SET status = 'PROCESSING',
		       started_at = ?,
		       updated_at = ?
		 WHERE workspace_id = ?
		   AND projection_id = ?
	`, now, now, workspaceID, projectionID); err != nil {
		t.Fatalf("mark workspace-memory projection row processing: %v", err)
	}
	return projectionID
}

func markWorkspaceMemoryProjectionOutboxRowPending(t *testing.T, store *sqlite.Store, ctx context.Context, workspaceID, projectionID string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE memory_projection_outbox
		   SET status = 'PENDING',
		       started_at = NULL,
		       updated_at = ?
		 WHERE workspace_id = ?
		   AND projection_id = ?
	`, now, workspaceID, projectionID); err != nil {
		t.Fatalf("restore workspace-memory projection row pending: %v", err)
	}
}
