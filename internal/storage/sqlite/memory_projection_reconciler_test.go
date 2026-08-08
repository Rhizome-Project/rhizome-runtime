package sqlite

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestMemoryProjectionLagSnapshot_Empty(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	snapshot, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag snapshot: %v", err)
	}
	if snapshot.State != "ok" {
		t.Fatalf("expected ok state for empty outbox, got %+v", snapshot)
	}
	if snapshot.PendingCount != 0 || snapshot.FailedCount != 0 {
		t.Fatalf("expected zero counts for empty outbox, got %+v", snapshot)
	}
}

func TestMemoryProjectionLagSnapshot_ProcessingBacklog(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-lag-processing"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Projection Lag Processing",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	now := time.Now().UTC().Add(-3 * time.Minute).Format(time.RFC3339Nano)
	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO memory_projection_outbox(
	    projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
	    available_at, enqueued_at, started_at, completed_at, updated_at
	  ) VALUES (?, ?, ?, ?, ?, 1, '', ?, ?, ?, NULL, ?)`,
		"mproj-processing-only",
		workspaceID,
		memoryProjectionKindWorkspaceMemory,
		"memory-processing-only",
		memoryProjectionStatusProcessing,
		now,
		now,
		now,
		now,
	); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert processing backlog row: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit processing backlog seed: %v", err)
	}

	snapshot, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag snapshot: %v", err)
	}
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded state for processing backlog, got %+v", snapshot)
	}
	if snapshot.PendingCount != 0 || snapshot.ProcessingCount != 1 || snapshot.FailedCount != 0 {
		t.Fatalf("unexpected processing backlog counts: %+v", snapshot)
	}
	if snapshot.Message == "" || !strings.Contains(snapshot.Message, "processing projection") {
		t.Fatalf("expected processing backlog message, got %+v", snapshot)
	}
}

func TestMemoryProjectionLagSnapshot_PendingBacklog(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-lag-pending"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Projection Lag Pending",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	oldestPendingAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	newerPendingAt := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339Nano)

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	for _, row := range []struct {
		projectionID   string
		projectionKind string
		originID       string
		status         string
		availableAt    string
		enqueuedAt     string
		updatedAt      string
		lastError      string
	}{
		{
			projectionID:   "mproj-pending-1",
			projectionKind: memoryProjectionKindKnowledgeClaim,
			originID:       "claim-1",
			status:         memoryProjectionStatusPending,
			availableAt:    oldestPendingAt,
			enqueuedAt:     oldestPendingAt,
			updatedAt:      oldestPendingAt,
		},
		{
			projectionID:   "mproj-pending-2",
			projectionKind: memoryProjectionKindEpisodePack,
			originID:       "pack-1",
			status:         memoryProjectionStatusPending,
			availableAt:    newerPendingAt,
			enqueuedAt:     newerPendingAt,
			updatedAt:      newerPendingAt,
		},
		{
			projectionID:   "mproj-failed-1",
			projectionKind: memoryProjectionKindWorkspaceMemory,
			originID:       "memory-1",
			status:         memoryProjectionStatusFailed,
			availableAt:    newerPendingAt,
			enqueuedAt:     newerPendingAt,
			updatedAt:      newerPendingAt,
			lastError:      "boom",
		},
	} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO memory_projection_outbox(
		    projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
		    available_at, enqueued_at, started_at, completed_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, NULL, NULL, ?)`,
			row.projectionID,
			workspaceID,
			row.projectionKind,
			row.originID,
			row.status,
			row.lastError,
			row.availableAt,
			row.enqueuedAt,
			row.updatedAt,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert memory projection outbox row: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit projection backlog seed: %v", err)
	}

	snapshot, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag snapshot: %v", err)
	}
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded state for backlog, got %+v", snapshot)
	}
	if snapshot.PendingCount != 2 || snapshot.FailedCount != 1 {
		t.Fatalf("unexpected backlog counts: %+v", snapshot)
	}
	if snapshot.OldestPendingAt != oldestPendingAt {
		t.Fatalf("unexpected oldest pending timestamp: got %q want %q", snapshot.OldestPendingAt, oldestPendingAt)
	}
	if snapshot.OldestPendingAgeSeconds == nil || *snapshot.OldestPendingAgeSeconds < 500 {
		t.Fatalf("expected oldest pending age to be populated, got %+v", snapshot)
	}
	if snapshot.Message == "" {
		t.Fatalf("expected backlog message, got empty snapshot: %+v", snapshot)
	}
}

func TestReclaimStaleMemoryProjectionProcessingMarksOldRowsFailed(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-reclaim-processing"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Projection Reclaim Processing",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	staleAt := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339Nano)
	freshAt := time.Now().UTC().Add(-15 * time.Second).Format(time.RFC3339Nano)
	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	for _, row := range []struct {
		projectionID string
		originID     string
		startedAt    string
	}{
		{projectionID: "mproj-reclaim-stale", originID: "memory-stale", startedAt: staleAt},
		{projectionID: "mproj-reclaim-fresh", originID: "memory-fresh", startedAt: freshAt},
	} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO memory_projection_outbox(
		    projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
		    available_at, enqueued_at, started_at, completed_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, 1, '', ?, ?, ?, NULL, ?)`,
			row.projectionID,
			workspaceID,
			memoryProjectionKindWorkspaceMemory,
			row.originID,
			memoryProjectionStatusProcessing,
			row.startedAt,
			row.startedAt,
			row.startedAt,
			row.startedAt,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert processing row %s: %v", row.projectionID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	reclaimed, err := store.ReclaimStaleMemoryProjectionProcessing(ctx, 2*time.Minute, 10)
	if err != nil {
		t.Fatalf("reclaim stale processing rows: %v", err)
	}
	if reclaimed != 1 {
		t.Fatalf("expected exactly one stale processing row reclaimed, got %d", reclaimed)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox rows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected two outbox rows, got %+v", rows)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ProjectionID < rows[j].ProjectionID })
	if rows[0].ProjectionID != "mproj-reclaim-fresh" || rows[0].Status != memoryProjectionStatusProcessing {
		t.Fatalf("expected fresh row to remain processing, got %+v", rows[0])
	}
	if rows[1].ProjectionID != "mproj-reclaim-stale" || rows[1].Status != memoryProjectionStatusFailed {
		t.Fatalf("expected stale row to be reclaimed to failed, got %+v", rows[1])
	}
	if rows[1].StartedAt != nil {
		t.Fatalf("expected reclaimed stale row to clear started_at, got %+v", rows[1])
	}
	if rows[1].LastError != "processing row reclaimed after stale timeout" {
		t.Fatalf("expected reclaim reason on stale row, got %+v", rows[1])
	}
}

func TestReclaimStaleMemoryProjectionProcessingFallsBackWhenStartedAtMalformed(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-reclaim-malformed-started"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Projection Reclaim Malformed Started",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	staleAt := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339Nano)
	freshAt := time.Now().UTC().Add(-15 * time.Second).Format(time.RFC3339Nano)
	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	for _, row := range []struct {
		projectionID string
		originID     string
		updatedAt    string
	}{
		{projectionID: "mproj-reclaim-malformed-started-stale", originID: "memory-malformed-stale", updatedAt: staleAt},
		{projectionID: "mproj-reclaim-malformed-started-fresh", originID: "memory-malformed-fresh", updatedAt: freshAt},
	} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO memory_projection_outbox(
		    projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
		    available_at, enqueued_at, started_at, completed_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, 1, '', ?, ?, ?, NULL, ?)`,
			row.projectionID,
			workspaceID,
			memoryProjectionKindWorkspaceMemory,
			row.originID,
			memoryProjectionStatusProcessing,
			row.updatedAt,
			row.updatedAt,
			"not-a-timestamp",
			row.updatedAt,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert processing row %s: %v", row.projectionID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	reclaimed, err := store.ReclaimStaleMemoryProjectionProcessing(ctx, 2*time.Minute, 10)
	if err != nil {
		t.Fatalf("reclaim stale processing rows with malformed started_at: %v", err)
	}
	if reclaimed != 1 {
		t.Fatalf("expected exactly one malformed stale processing row reclaimed, got %d", reclaimed)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox rows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected two outbox rows, got %+v", rows)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ProjectionID < rows[j].ProjectionID })
	if rows[0].ProjectionID != "mproj-reclaim-malformed-started-fresh" || rows[0].Status != memoryProjectionStatusProcessing {
		t.Fatalf("expected fresh malformed row to remain processing, got %+v", rows[0])
	}
	if rows[1].ProjectionID != "mproj-reclaim-malformed-started-stale" || rows[1].Status != memoryProjectionStatusFailed {
		t.Fatalf("expected stale malformed row to be reclaimed to failed, got %+v", rows[1])
	}
	if rows[1].StartedAt != nil {
		t.Fatalf("expected reclaimed malformed row to clear started_at, got %+v", rows[1])
	}
	if !strings.Contains(rows[1].LastError, "malformed started_at") {
		t.Fatalf("expected malformed started_at reclaim reason, got %+v", rows[1])
	}
}

func TestMemoryProjectionLagSnapshot_FailedBacklog(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-lag-failed"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Projection Lag Failed",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO memory_projection_outbox(
	    projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
	    available_at, enqueued_at, started_at, completed_at, updated_at
	  ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, NULL, NULL, ?)`,
		"mproj-failed-only",
		workspaceID,
		memoryProjectionKindKnowledgeClaim,
		"claim-failed-only",
		memoryProjectionStatusFailed,
		"failed during reconcile",
		now,
		now,
		now,
	); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert failed backlog row: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed backlog seed: %v", err)
	}

	snapshot, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag snapshot: %v", err)
	}
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded state for failed backlog, got %+v", snapshot)
	}
	if snapshot.PendingCount != 0 || snapshot.FailedCount != 1 {
		t.Fatalf("unexpected failed backlog counts: %+v", snapshot)
	}
	if snapshot.OldestPendingAt != "" {
		t.Fatalf("expected no pending timestamp for failed-only backlog, got %+v", snapshot)
	}
}

func TestReconcileMemoryProjectionWorkspaceSkipsFailedRowsUntilAvailableAt(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-retry-gate"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Projection Retry Gate",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	future := time.Now().UTC().Add(2 * time.Minute).Format(time.RFC3339Nano)
	if err := insertMemoryProjectionOutboxRow(ctx, store, memoryProjectionOutboxSeed{
		projectionID:   "mproj-failed-future",
		workspaceID:    workspaceID,
		projectionKind: memoryProjectionKindKnowledgeClaim,
		originID:       "claim-future",
		status:         memoryProjectionStatusFailed,
		attemptCount:   3,
		lastError:      "boom",
		availableAt:    future,
		enqueuedAt:     future,
		updatedAt:      future,
	}); err != nil {
		t.Fatalf("seed future failed outbox row: %v", err)
	}

	result, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("reconcile should ignore future failed rows, got %v", err)
	}
	if result.Processed != 0 || result.Failed != 0 {
		t.Fatalf("expected no work while retry window is closed, got %+v", result)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox rows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one outbox row, got %+v", rows)
	}
	if rows[0].AttemptCount != 3 || rows[0].Status != memoryProjectionStatusFailed || rows[0].AvailableAt != future {
		t.Fatalf("expected retry gate to leave row untouched, got %+v", rows[0])
	}
}

func TestListPendingMemoryProjectionOutboxAppliesLimitAndSkipsMalformedRows(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-limit"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Projection Limit",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	oldest := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339Nano)
	newer := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	future := time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339Nano)
	for _, row := range []memoryProjectionOutboxSeed{
		{
			projectionID:   "mproj-limit-oldest",
			workspaceID:    workspaceID,
			projectionKind: memoryProjectionKindWorkspaceMemory,
			originID:       "memory-oldest",
			status:         memoryProjectionStatusPending,
			availableAt:    oldest,
			enqueuedAt:     oldest,
			updatedAt:      oldest,
		},
		{
			projectionID:   "mproj-limit-malformed",
			workspaceID:    workspaceID,
			projectionKind: memoryProjectionKindKnowledgeClaim,
			originID:       "claim-malformed",
			status:         memoryProjectionStatusPending,
			availableAt:    "not-a-timestamp",
			enqueuedAt:     oldest,
			updatedAt:      oldest,
		},
		{
			projectionID:   "mproj-limit-newer",
			workspaceID:    workspaceID,
			projectionKind: memoryProjectionKindEpisodePack,
			originID:       "pack-newer",
			status:         memoryProjectionStatusPending,
			availableAt:    newer,
			enqueuedAt:     newer,
			updatedAt:      newer,
		},
		{
			projectionID:   "mproj-limit-future",
			workspaceID:    workspaceID,
			projectionKind: memoryProjectionKindKnowledgeClaim,
			originID:       "claim-future",
			status:         memoryProjectionStatusFailed,
			availableAt:    future,
			enqueuedAt:     future,
			updatedAt:      future,
		},
	} {
		if err := insertMemoryProjectionOutboxRow(ctx, store, row); err != nil {
			t.Fatalf("seed outbox row %s: %v", row.projectionID, err)
		}
	}

	rows, err := store.listPendingMemoryProjectionOutbox(ctx, workspaceID, 1)
	if err != nil {
		t.Fatalf("list pending memory projection outbox: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly one eligible row, got %+v", rows)
	}
	if rows[0].ProjectionID != "mproj-limit-oldest" {
		t.Fatalf("expected earliest eligible projection, got %+v", rows[0])
	}

	rows, err = store.listPendingMemoryProjectionOutbox(ctx, workspaceID, 2)
	if err != nil {
		t.Fatalf("list pending memory projection outbox with wider limit: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected two eligible rows, got %+v", rows)
	}
	if rows[0].ProjectionID != "mproj-limit-oldest" || rows[1].ProjectionID != "mproj-limit-newer" {
		t.Fatalf("expected malformed and future rows to be skipped, got %+v", rows)
	}
}

func TestReconcileMemoryProjectionWorkspaceQuarantinesMalformedAvailableAt(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-malformed-available"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Projection Malformed Available",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-malformed-available",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Malformed projection timestamps recover",
		Body:        "A malformed available_at should be quarantined and made retryable by reconcile.",
		Summary:     "Malformed projection timestamps recover.",
		Confidence:  0.73,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	oldUpdatedAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE memory_projection_outbox
	    SET available_at = ?, updated_at = ?
	  WHERE workspace_id = ? AND projection_kind = ? AND origin_id = ?`,
		"not-a-timestamp",
		oldUpdatedAt,
		workspaceID,
		memoryProjectionKindKnowledgeClaim,
		record.ClaimID,
	); err != nil {
		t.Fatalf("corrupt projection available_at: %v", err)
	}

	result, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("reconcile malformed available_at projection: %v", err)
	}
	if result.Processed != 1 || result.Failed != 0 {
		t.Fatalf("expected malformed available_at row to recover through reconcile, got %+v", result)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox rows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one projection row, got %+v", rows)
	}
	if rows[0].Status != memoryProjectionStatusDone || rows[0].LastError != "" {
		t.Fatalf("expected recovered projection row to be done with cleared error, got %+v", rows[0])
	}
	if _, ok := memoryProjectionParseTimestamp(rows[0].AvailableAt); !ok {
		t.Fatalf("expected recovered projection row to have a valid available_at, got %+v", rows[0])
	}
}

func TestReconcileMemoryProjectionWorkspaceQuarantinesOffsetAvailableAt(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-offset-available"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Projection Offset Available",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-offset-available",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Offset projection timestamps recover",
		Body:        "An offset available_at should be normalized before queue selection.",
		Summary:     "Offset projection timestamps recover.",
		Confidence:  0.73,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	offsetAvailableAt := time.Now().UTC().Add(-time.Minute).Format("2006-01-02T15:04:05.999999999+00:00")
	if memoryProjectionAvailableBy(offsetAvailableAt, time.Now().UTC()) {
		t.Fatalf("expected offset-form available_at to fail closed for queue eligibility")
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE memory_projection_outbox
	    SET available_at = ?, updated_at = ?
	  WHERE workspace_id = ? AND projection_kind = ? AND origin_id = ?`,
		offsetAvailableAt,
		time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
		workspaceID,
		memoryProjectionKindKnowledgeClaim,
		record.ClaimID,
	); err != nil {
		t.Fatalf("set offset projection available_at: %v", err)
	}

	result, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("reconcile offset available_at projection: %v", err)
	}
	if result.Processed != 1 || result.Failed != 0 {
		t.Fatalf("expected offset available_at row to recover through reconcile, got %+v", result)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox rows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one projection row, got %+v", rows)
	}
	if rows[0].Status != memoryProjectionStatusDone || rows[0].LastError != "" {
		t.Fatalf("expected recovered projection row to be done with cleared error, got %+v", rows[0])
	}
	if _, ok := memoryProjectionQueueTimestamp(rows[0].AvailableAt); !ok {
		t.Fatalf("expected recovered projection row to have queue-eligible available_at, got %+v", rows[0])
	}
}

func TestReconcileMemoryProjectionWorkspaceBacksOffFailedRows(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-retry-backoff"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Projection Retry Backoff",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	now := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	if err := insertMemoryProjectionOutboxRow(ctx, store, memoryProjectionOutboxSeed{
		projectionID:   "mproj-unsupported-kind",
		workspaceID:    workspaceID,
		projectionKind: "UNSUPPORTED_KIND",
		originID:       "origin-unsupported",
		status:         memoryProjectionStatusPending,
		availableAt:    now,
		enqueuedAt:     now,
		updatedAt:      now,
	}); err != nil {
		t.Fatalf("seed unsupported projection row: %v", err)
	}

	result, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, 10)
	if err == nil {
		t.Fatalf("expected unsupported projection kind to fail reconcile, got %+v", result)
	}
	if result.Failed != 1 {
		t.Fatalf("expected one failed reconcile row, got %+v", result)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox rows after failure: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one outbox row after failure, got %+v", rows)
	}
	row := rows[0]
	if row.Status != memoryProjectionStatusFailed {
		t.Fatalf("expected failed retry row, got %+v", row)
	}
	if row.AttemptCount != 1 {
		t.Fatalf("expected attempt count to increment, got %+v", row)
	}
	if row.LastError == "" {
		t.Fatalf("expected failure reason to be recorded, got %+v", row)
	}
	availableTS, err := time.Parse(time.RFC3339Nano, row.AvailableAt)
	if err != nil {
		t.Fatalf("parse retry available_at: %v", err)
	}
	updatedTS, err := time.Parse(time.RFC3339Nano, row.UpdatedAt)
	if err != nil {
		t.Fatalf("parse retry updated_at: %v", err)
	}
	if !availableTS.After(updatedTS) {
		t.Fatalf("expected retry window to move available_at forward, got row=%+v", row)
	}
}

func TestReconcileMemoryProjectionWorkspaceContinuesAfterSingleRowFailure(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-continue"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Projection Continue",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Healthy projection",
		Body:        "A healthy projection should still complete when an adjacent row fails.",
		Summary:     "Healthy projection.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	now := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	if err := insertMemoryProjectionOutboxRow(ctx, store, memoryProjectionOutboxSeed{
		projectionID:   "mproj-unsupported-adjacent",
		workspaceID:    workspaceID,
		projectionKind: "UNSUPPORTED_KIND",
		originID:       "origin-unsupported",
		status:         memoryProjectionStatusPending,
		availableAt:    now,
		enqueuedAt:     now,
		updatedAt:      now,
	}); err != nil {
		t.Fatalf("seed unsupported projection row: %v", err)
	}

	result, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, 10)
	if err == nil {
		t.Fatalf("expected reconcile to surface the unsupported row failure, got %+v", result)
	}
	if result.Failed != 1 || result.Processed < 1 {
		t.Fatalf("expected one failed row and at least one successfully processed row, got %+v", result)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox rows after reconcile: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("expected reconcile to leave multiple outbox rows to inspect, got %+v", rows)
	}

	var failedRow, doneRow *MemoryProjectionOutboxRecord
	for idx := range rows {
		row := rows[idx]
		switch {
		case row.ProjectionKind == "UNSUPPORTED_KIND":
			failedRow = &row
		case row.ProjectionKind == memoryProjectionKindWorkspaceMemory && row.OriginID == record.MemoryID:
			doneRow = &row
		}
	}
	if failedRow == nil || doneRow == nil {
		t.Fatalf("expected one failed unsupported row and one done workspace-memory row, got %+v", rows)
	}
	if failedRow.Status != memoryProjectionStatusFailed || failedRow.AttemptCount != 1 || failedRow.LastError == "" {
		t.Fatalf("expected unsupported row to fail once with recorded error, got %+v", *failedRow)
	}
	if doneRow.Status != memoryProjectionStatusDone {
		t.Fatalf("expected healthy workspace-memory row to finish despite adjacent failure, got %+v", *doneRow)
	}
}

func TestRebuildMemoryProjectionWorkspaceQueuesAllTargetsBeforeBoundedDrain(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-rebuild-bounded-drain"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Projection Rebuild Bounded Drain",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	memoryIDs := make([]string, 0, 3)
	for idx := 0; idx < 3; idx++ {
		record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
			WorkspaceID: workspaceID,
			MemoryType:  "lesson",
			Title:       fmt.Sprintf("Bounded rebuild memory %d", idx),
			Body:        fmt.Sprintf("Canonical row %d should be queued even when drain stays bounded.", idx),
			Summary:     "bounded rebuild queueing",
			SourceKind:  "manual",
			SourceID:    fmt.Sprintf("developer-%d", idx),
		})
		if err != nil {
			t.Fatalf("record workspace memory %d: %v", idx, err)
		}
		memoryIDs = append(memoryIDs, record.MemoryID)
	}

	result, err := store.RebuildMemoryProjectionWorkspace(ctx, MemoryProjectionRebuildFilter{
		WorkspaceID: workspaceID,
		Kinds:       []string{memoryProjectionKindWorkspaceMemory},
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("bounded rebuild: %v", err)
	}
	if result.TargetsQueued != len(memoryIDs) {
		t.Fatalf("expected rebuild to queue every target before bounded drain, got %+v", result)
	}
	if result.Processed != 1 {
		t.Fatalf("expected bounded drain to process exactly one row, got %+v", result)
	}
	if result.Pending != len(memoryIDs)-1 {
		t.Fatalf("expected remaining rows to stay pending after bounded drain, got %+v", result)
	}
	if result.Done != 1 || result.FailedPending != 0 || result.Processing != 0 {
		t.Fatalf("unexpected rebuild summary after bounded drain: %+v", result)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox rows after bounded rebuild: %v", err)
	}

	workspaceMemoryDone := 0
	workspaceMemoryPending := 0
	knowledgeClaimPending := 0
	workspaceMemoryRows := 0
	seen := make(map[string]string, len(rows))
	for _, row := range rows {
		seen[row.OriginID] = row.Status
		switch row.ProjectionKind {
		case memoryProjectionKindWorkspaceMemory:
			workspaceMemoryRows++
			switch row.Status {
			case memoryProjectionStatusDone:
				workspaceMemoryDone++
			case memoryProjectionStatusPending:
				workspaceMemoryPending++
			default:
				t.Fatalf("expected bounded rebuild to leave workspace-memory rows in done/pending only, got %+v", row)
			}
		case memoryProjectionKindKnowledgeClaim:
			if row.Status != memoryProjectionStatusPending {
				t.Fatalf("expected unrelated knowledge-claim backlog to stay pending, got %+v", row)
			}
			knowledgeClaimPending++
		default:
			t.Fatalf("unexpected projection kind after bounded rebuild: %+v", row)
		}
	}
	if workspaceMemoryRows != len(memoryIDs) {
		t.Fatalf("expected one durable workspace-memory outbox row per target, got %+v", rows)
	}
	if workspaceMemoryDone != 1 || workspaceMemoryPending != len(memoryIDs)-1 {
		t.Fatalf("unexpected bounded rebuild status counts: rows=%+v", rows)
	}
	if knowledgeClaimPending != len(memoryIDs) {
		t.Fatalf("expected promoted-claim backlog to remain untouched, got %+v", rows)
	}
	for _, memoryID := range memoryIDs {
		if _, ok := seen[memoryID]; !ok {
			t.Fatalf("expected durable outbox row for memory %s, got %+v", memoryID, rows)
		}
	}
}

func TestRebuildMemoryProjectionWorkspaceBoundedDrainSelectsDeterministicFirstTarget(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-rebuild-deterministic-drain"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Projection Rebuild Deterministic Drain",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	memoryIDs := make([]string, 0, 3)
	for idx := 0; idx < 3; idx++ {
		record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
			WorkspaceID: workspaceID,
			MemoryType:  "lesson",
			Title:       fmt.Sprintf("Deterministic rebuild memory %d", idx),
			Body:        fmt.Sprintf("Canonical row %d should drain in stable order.", idx),
			Summary:     "deterministic bounded rebuild",
			SourceKind:  "manual",
			SourceID:    fmt.Sprintf("developer-drain-%d", idx),
		})
		if err != nil {
			t.Fatalf("record workspace memory %d: %v", idx, err)
		}
		memoryIDs = append(memoryIDs, record.MemoryID)
	}
	sort.Strings(memoryIDs)

	result, err := store.RebuildMemoryProjectionWorkspace(ctx, MemoryProjectionRebuildFilter{
		WorkspaceID: workspaceID,
		Kinds:       []string{memoryProjectionKindWorkspaceMemory},
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("bounded rebuild: %v", err)
	}
	if result.TargetsQueued != len(memoryIDs) || result.Processed != 1 || result.Failed != 0 {
		t.Fatalf("unexpected bounded rebuild result: %+v", result)
	}
	if result.Done != 1 || result.Pending != len(memoryIDs)-1 {
		t.Fatalf("unexpected bounded rebuild summary: %+v", result)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox rows: %v", err)
	}

	workspaceMemoryStatus := make(map[string]string, len(memoryIDs))
	knowledgeClaimPending := 0
	for _, row := range rows {
		switch row.ProjectionKind {
		case memoryProjectionKindWorkspaceMemory:
			workspaceMemoryStatus[row.OriginID] = row.Status
		case memoryProjectionKindKnowledgeClaim:
			if row.Status != memoryProjectionStatusPending {
				t.Fatalf("expected unrelated knowledge-claim backlog to stay pending, got %+v", row)
			}
			knowledgeClaimPending++
		}
	}
	if knowledgeClaimPending != len(memoryIDs) {
		t.Fatalf("expected promoted claim backlog to stay untouched, got %+v", rows)
	}
	for idx, memoryID := range memoryIDs {
		got := workspaceMemoryStatus[memoryID]
		want := memoryProjectionStatusPending
		if idx == 0 {
			want = memoryProjectionStatusDone
		}
		if got != want {
			t.Fatalf("expected workspace-memory %s to be %s after first bounded drain, got %q (rows=%+v)", memoryID, want, got, rows)
		}
	}
}

func TestMemoryProjectionAvailableByFailsClosedOnMalformedTimestamp(t *testing.T) {
	if memoryProjectionAvailableBy("not-a-timestamp", time.Now().UTC()) {
		t.Fatalf("expected malformed available_at to fail closed")
	}
}

func TestRecordWorkspaceMemoryWithEffectsTxKeepsAnchorInlineAndDefersProjectionDetail(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-projection-workspace-memory"
		agentID     = "agent-memory-projection-workspace-memory"
		sessionID   = "sess-memory-projection-workspace-memory"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Projection Outbox",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Projection Workspace Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	record, _, _, err := store.recordWorkspaceMemoryWithEffectsTx(ctx, tx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Projection detail leaves hot tx",
		Body:        "Workspace memory should keep only the compatibility anchor inline while refs, versions, and edges reconcile later.",
		Summary:     "Projection detail leaves hot tx.",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "manual",
		SourceID:    "developer",
	}, "2026-04-09T12:00:00Z")
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("record workspace memory tx: %v", err)
	}

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	var nodeCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, workspaceID, nodeID).Scan(&nodeCount); err != nil {
		_ = tx.Rollback()
		t.Fatalf("count anchor nodes in tx: %v", err)
	}
	if nodeCount != 1 {
		_ = tx.Rollback()
		t.Fatalf("expected one inline compatibility anchor node, got %d", nodeCount)
	}

	var refCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM memory_node_refs WHERE workspace_id = ? AND memory_id = ?`, workspaceID, nodeID).Scan(&refCount); err != nil {
		_ = tx.Rollback()
		t.Fatalf("count anchor refs in tx: %v", err)
	}
	if refCount != 0 {
		_ = tx.Rollback()
		t.Fatalf("expected no anchor refs inside hot tx, got %d", refCount)
	}

	var versionCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM memory_node_versions WHERE workspace_id = ? AND memory_id = ?`, workspaceID, nodeID).Scan(&versionCount); err != nil {
		_ = tx.Rollback()
		t.Fatalf("count anchor versions in tx: %v", err)
	}
	if versionCount != 1 {
		_ = tx.Rollback()
		t.Fatalf("expected one minimal anchor version inside hot tx, got %d", versionCount)
	}
	var versionRefKind, versionRefID string
	if err := tx.QueryRowContext(ctx, `SELECT ref_kind, ref_id FROM memory_node_versions WHERE workspace_id = ? AND memory_id = ? LIMIT 1`, workspaceID, nodeID).Scan(&versionRefKind, &versionRefID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("load anchor version inside hot tx: %v", err)
	}
	if versionRefKind != "workspace_memory" || versionRefID != record.MemoryID {
		_ = tx.Rollback()
		t.Fatalf("expected only canonical workspace_memory anchor version inside hot tx, got %s/%s", versionRefKind, versionRefID)
	}

	var edgeCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM memory_edges WHERE workspace_id = ? AND source_kind = ? AND source_id = ?`, workspaceID, "workspace_memory", record.MemoryID).Scan(&edgeCount); err != nil {
		_ = tx.Rollback()
		t.Fatalf("count anchor edges in tx: %v", err)
	}
	if edgeCount != 0 {
		_ = tx.Rollback()
		t.Fatalf("expected no anchor edges inside hot tx, got %d", edgeCount)
	}

	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM memory_projection_outbox WHERE workspace_id = ? AND projection_kind = ? AND origin_id = ?`, workspaceID, memoryProjectionKindWorkspaceMemory, record.MemoryID).Scan(&status); err != nil {
		_ = tx.Rollback()
		t.Fatalf("load workspace-memory outbox row in tx: %v", err)
	}
	if status != memoryProjectionStatusPending {
		_ = tx.Rollback()
		t.Fatalf("expected pending workspace-memory outbox status, got %s", status)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit workspace memory tx: %v", err)
	}

	if _, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, 10); err != nil {
		t.Fatalf("reconcile workspace-memory projection: %v", err)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get reconciled workspace-memory node: %v", err)
	}
	if detail.Node.OriginKind != "workspace_memory" || detail.Node.OriginID != record.MemoryID {
		t.Fatalf("unexpected reconciled workspace-memory node %+v", detail.Node)
	}
	if len(detail.Refs) == 0 || len(detail.Versions) == 0 {
		t.Fatalf("expected reconciled workspace-memory projection detail, got %+v", detail)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox after reconcile: %v", err)
	}
	var workspaceMemoryDone bool
	for _, row := range rows {
		if row.ProjectionKind == memoryProjectionKindWorkspaceMemory && row.OriginID == record.MemoryID && row.Status == memoryProjectionStatusDone {
			workspaceMemoryDone = true
		}
	}
	if !workspaceMemoryDone {
		t.Fatalf("expected done workspace-memory outbox row, got %+v", rows)
	}
}

func TestUpsertKnowledgeClaimTxEnqueuesProjectionUntilReconciled(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-projection-claim"
		agentID     = "agent-memory-projection-claim"
		sessionID   = "sess-memory-projection-claim"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Projection Claim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Projection Claim Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	claim := KnowledgeClaimRecord{
		ClaimID:     "claim-projection-outbox",
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Projection outbox is durable",
		Body:        "Claim graph sync should leave the hot tx and reconcile after commit.",
		Summary:     "Projection outbox fact",
		Confidence:  0.72,
		SourceKind:  "manual",
		SourceID:    "developer",
		AgentID:     agentID,
		SessionID:   sessionID,
	}
	if _, _, _, err := store.upsertKnowledgeClaimTx(ctx, tx, claim, "2026-04-08T12:00:00Z"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert knowledge claim tx: %v", err)
	}

	nodeID := memoryGraphNodeID("knowledge_claim", claim.ClaimID)
	var nodeCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, workspaceID, nodeID).Scan(&nodeCount); err != nil {
		_ = tx.Rollback()
		t.Fatalf("count graph nodes in tx: %v", err)
	}
	if nodeCount != 0 {
		_ = tx.Rollback()
		t.Fatalf("expected no graph node inside hot tx, got %d", nodeCount)
	}

	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM memory_projection_outbox WHERE workspace_id = ? AND projection_kind = ? AND origin_id = ?`, workspaceID, memoryProjectionKindKnowledgeClaim, claim.ClaimID).Scan(&status); err != nil {
		_ = tx.Rollback()
		t.Fatalf("load outbox row in tx: %v", err)
	}
	if status != memoryProjectionStatusPending {
		_ = tx.Rollback()
		t.Fatalf("expected pending outbox status, got %s", status)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit claim tx: %v", err)
	}

	if _, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, 10); err != nil {
		t.Fatalf("reconcile claim projection: %v", err)
	}
	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get reconciled claim node: %v", err)
	}
	if detail.Node.OriginKind != "knowledge_claim" || detail.Node.OriginID != claim.ClaimID {
		t.Fatalf("unexpected reconciled node %+v", detail.Node)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox after reconcile: %v", err)
	}
	if len(rows) != 1 || rows[0].Status != memoryProjectionStatusDone {
		t.Fatalf("expected one done outbox row, got %+v", rows)
	}
}

func TestRecordKnowledgeClaimLeavesProjectionPendingUntilExplicitReconcile(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-claim-top-level"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Top-Level Claim Projection",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-top-level-pending",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Top-level claim writes stay canonical-first",
		Body:        "RecordKnowledgeClaim should not silently reconcile the derived graph projection.",
		Summary:     "Top-level claim projection stays pending.",
		Confidence:  0.71,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record top-level knowledge claim: %v", err)
	}

	if strings.TrimSpace(record.ClaimID) == "" || record.WorkspaceID != workspaceID {
		t.Fatalf("expected canonical claim row, got %+v", record)
	}

	nodeID := memoryGraphNodeID("knowledge_claim", record.ClaimID)
	if _, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID); err == nil {
		t.Fatalf("expected derived claim graph node to lag until explicit reconcile")
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox after top-level claim write: %v", err)
	}
	if len(rows) != 1 || rows[0].ProjectionKind != memoryProjectionKindKnowledgeClaim || rows[0].OriginID != record.ClaimID || rows[0].Status != memoryProjectionStatusPending {
		t.Fatalf("expected one pending claim projection row, got %+v", rows)
	}

	snapshot, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag snapshot: %v", err)
	}
	if snapshot.State != "degraded" || snapshot.PendingCount == 0 {
		t.Fatalf("expected degraded lag snapshot while claim projection is pending, got %+v", snapshot)
	}

	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get reconciled top-level claim node: %v", err)
	}
	if detail.Node.OriginKind != "knowledge_claim" || detail.Node.OriginID != record.ClaimID {
		t.Fatalf("unexpected reconciled top-level claim node %+v", detail.Node)
	}

	rows, err = store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox after explicit reconcile: %v", err)
	}
	if len(rows) != 1 || rows[0].Status != memoryProjectionStatusDone {
		t.Fatalf("expected done outbox row after explicit reconcile, got %+v", rows)
	}
}

func TestArchiveKnowledgeClaimLeavesProjectionPendingUntilExplicitReconcile(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-claim-archive-top-level"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Top-Level Claim Archive Projection",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-top-level-archive",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Archive waits for explicit projection reconcile",
		Body:        "Archiving a claim should update canonical truth first and the derived graph only after reconcile.",
		Summary:     "Archive lag is explicit.",
		Confidence:  0.64,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := memoryGraphNodeID("knowledge_claim", record.ClaimID)
	beforeArchive, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get pre-archive claim node: %v", err)
	}
	if beforeArchive.Node.LifecycleState == "ARCHIVED" {
		t.Fatalf("expected active lifecycle before archive, got %+v", beforeArchive.Node)
	}

	archived, err := store.ArchiveKnowledgeClaim(ctx, KnowledgeClaimArchiveInput{
		WorkspaceID: workspaceID,
		ClaimID:     record.ClaimID,
		ArchivedBy:  "developer",
		Reason:      "cleanup",
	})
	if err != nil {
		t.Fatalf("archive knowledge claim: %v", err)
	}
	if archived.ArchivedAt == nil || strings.TrimSpace(*archived.ArchivedAt) == "" {
		t.Fatalf("expected canonical archive timestamp, got %+v", archived)
	}

	staleDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get stale pre-reconcile claim node: %v", err)
	}
	if staleDetail.Node.LifecycleState == "ARCHIVED" {
		t.Fatalf("expected derived node to stay pre-archive until explicit reconcile, got %+v", staleDetail.Node)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox after archive: %v", err)
	}
	if len(rows) != 1 || rows[0].Status != memoryProjectionStatusPending {
		t.Fatalf("expected pending claim projection row after archive, got %+v", rows)
	}

	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	reconciledDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get reconciled archived claim node: %v", err)
	}
	if reconciledDetail.Node.LifecycleState != "ARCHIVED" {
		t.Fatalf("expected archived lifecycle after explicit reconcile, got %+v", reconciledDetail.Node)
	}
}

func TestUpsertEpisodePackTxEnqueuesProjectionUntilReconciled(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-projection-pack"
		agentID     = "agent-memory-projection-pack"
		sessionID   = "sess-memory-projection-pack"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Projection Pack",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Projection Pack Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	pack, err := store.upsertEpisodePackTx(ctx, tx, EpisodePackRecord{
		PackID:              "pack-projection-outbox",
		PackKey:             "episode-pack-projection-outbox",
		WorkspaceID:         workspaceID,
		PackType:            episodePackTypeCompaction,
		PackMode:            episodePackModeFallback,
		SchemaVersion:       episodePackSchemaVersion,
		SessionID:           sessionID,
		LineageSessionID:    sessionID,
		AgentID:             agentID,
		TriggerKind:         "token_budget_exceeded",
		SourceWindowStart:   0,
		SourceWindowEnd:     2,
		SourceWindowDigest:  "digest-projection-outbox",
		SummaryText:         "Fallback episode pack",
		SummaryDigest:       "summary-digest-projection-outbox",
		NarrativeSummary:    "Projection outbox episode pack",
		DissentState:        episodePackDissentNone,
		MessageCountBefore:  4,
		MessageCountAfter:   2,
		MessageTokensBefore: 100,
		MessageTokensAfter:  50,
		TotalInputTokens:    120,
		TotalOutputTokens:   60,
		CreatedAt:           "2026-04-08T12:00:00Z",
		UpdatedAt:           "2026-04-08T12:00:00Z",
	})
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert episode pack tx: %v", err)
	}

	var nodeCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, workspaceID, pack.CanonicalMemoryID).Scan(&nodeCount); err != nil {
		_ = tx.Rollback()
		t.Fatalf("count episode pack graph nodes in tx: %v", err)
	}
	if nodeCount != 0 {
		_ = tx.Rollback()
		t.Fatalf("expected no episode-pack node inside hot tx, got %d", nodeCount)
	}

	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM memory_projection_outbox WHERE workspace_id = ? AND projection_kind = ? AND origin_id = ?`, workspaceID, memoryProjectionKindEpisodePack, pack.PackID).Scan(&status); err != nil {
		_ = tx.Rollback()
		t.Fatalf("load episode pack outbox row in tx: %v", err)
	}
	if status != memoryProjectionStatusPending {
		_ = tx.Rollback()
		t.Fatalf("expected pending episode-pack outbox status, got %s", status)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit episode pack tx: %v", err)
	}
	if _, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, 10); err != nil {
		t.Fatalf("reconcile episode pack projection: %v", err)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, pack.CanonicalMemoryID)
	if err != nil {
		t.Fatalf("get reconciled episode pack node: %v", err)
	}
	if detail.Node.OriginKind != "episode_pack" || detail.Node.OriginID != pack.PackID {
		t.Fatalf("unexpected reconciled episode pack node %+v", detail.Node)
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list episode pack outbox after reconcile: %v", err)
	}
	if len(rows) != 1 || rows[0].Status != memoryProjectionStatusDone {
		t.Fatalf("expected one done episode-pack outbox row, got %+v", rows)
	}
}

func TestRebuildMemoryProjectionWorkspaceIsIdempotent(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-rebuild-idempotent"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Projection Rebuild",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Rebuildable workspace memory",
		Body:        "Derived projections should rebuild repeatably from canonical rows.",
		Summary:     "Rebuildable memory.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rebuild-idempotent",
		ClaimType:   "LESSON",
		Status:      "ACTIVE",
		Subject:     "Projection rebuilds are idempotent",
		Body:        "Re-running a rebuild should repair the same read-side state without duplication.",
		Summary:     "Projection rebuild idempotence.",
		Confidence:  0.81,
		SourceKind:  "manual",
		SourceID:    "developer",
		MemoryID:    record.MemoryID,
	}); err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_edges; DELETE FROM memory_node_versions; DELETE FROM memory_node_refs; DELETE FROM memory_nodes;`); err != nil {
		t.Fatalf("clear graph tables: %v", err)
	}

	first, err := store.RebuildMemoryProjectionWorkspace(ctx, MemoryProjectionRebuildFilter{
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("first rebuild: %v", err)
	}
	if first.TargetsQueued < 2 || first.Pending != 0 || first.FailedPending != 0 {
		t.Fatalf("unexpected first rebuild result: %+v", first)
	}

	items, err := store.ListMemoryGraphNodes(ctx, MemoryGraphNodeFilter{WorkspaceID: workspaceID, Limit: 10})
	if err != nil {
		t.Fatalf("list graph nodes after first rebuild: %v", err)
	}
	if len(items) < 2 {
		t.Fatalf("expected rebuilt graph nodes, got %+v", items)
	}

	second, err := store.RebuildMemoryProjectionWorkspace(ctx, MemoryProjectionRebuildFilter{
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	if second.Pending != 0 || second.FailedPending != 0 {
		t.Fatalf("unexpected second rebuild result: %+v", second)
	}

	itemsAfter, err := store.ListMemoryGraphNodes(ctx, MemoryGraphNodeFilter{WorkspaceID: workspaceID, Limit: 10})
	if err != nil {
		t.Fatalf("list graph nodes after second rebuild: %v", err)
	}
	if len(itemsAfter) != len(items) {
		t.Fatalf("expected stable node count after repeated rebuild, got before=%d after=%d", len(items), len(itemsAfter))
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox after rebuilds: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("expected durable outbox rows for rebuilt projections, got %+v", rows)
	}
	for _, row := range rows {
		if row.Status != memoryProjectionStatusDone {
			t.Fatalf("expected done outbox rows after rebuilds, got %+v", rows)
		}
	}
}

func TestRebuildMemoryProjectionWorkspaceDeletesStaleClaimProjection(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-projection-rebuild-delete"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Projection Rebuild Delete",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-rebuild-delete",
		ClaimType:   "LESSON",
		Status:      "ACTIVE",
		Subject:     "Stale projection should disappear",
		Body:        "If the canonical row vanishes, rebuild should remove the derived node.",
		Summary:     "Stale projection cleanup.",
		Confidence:  0.66,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := memoryGraphNodeID("knowledge_claim", claim.ClaimID)
	if _, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID); err != nil {
		t.Fatalf("expected existing claim projection node: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `DELETE FROM knowledge_claims WHERE workspace_id = ? AND claim_id = ?`, workspaceID, claim.ClaimID); err != nil {
		t.Fatalf("delete canonical claim row: %v", err)
	}

	result, err := store.RebuildMemoryProjectionWorkspace(ctx, MemoryProjectionRebuildFilter{
		WorkspaceID: workspaceID,
		Kinds:       []string{memoryProjectionKindKnowledgeClaim},
	})
	if err != nil {
		t.Fatalf("rebuild stale claim projection: %v", err)
	}
	if result.Pending != 0 || result.FailedPending != 0 {
		t.Fatalf("unexpected rebuild result for stale claim cleanup: %+v", result)
	}

	if _, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID); err == nil {
		t.Fatalf("expected stale claim projection node to be removed")
	}

	rows, err := store.listMemoryProjectionOutbox(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list outbox after stale cleanup: %v", err)
	}
	if len(rows) != 1 || rows[0].Status != memoryProjectionStatusDone {
		t.Fatalf("expected one done outbox row after stale cleanup, got %+v", rows)
	}
}

type memoryProjectionOutboxSeed struct {
	projectionID   string
	workspaceID    string
	projectionKind string
	originID       string
	status         string
	attemptCount   int
	lastError      string
	availableAt    string
	enqueuedAt     string
	updatedAt      string
}

func insertMemoryProjectionOutboxRow(ctx context.Context, store *Store, row memoryProjectionOutboxSeed) error {
	_, err := store.DB().ExecContext(
		ctx,
		`INSERT INTO memory_projection_outbox(
		    projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
		    available_at, enqueued_at, started_at, completed_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
		  ON CONFLICT(workspace_id, projection_kind, origin_id) DO UPDATE SET
		    projection_id = excluded.projection_id,
		    status = excluded.status,
		    attempt_count = excluded.attempt_count,
		    last_error = excluded.last_error,
		    available_at = excluded.available_at,
		    enqueued_at = excluded.enqueued_at,
		    started_at = NULL,
		    completed_at = NULL,
		    updated_at = excluded.updated_at`,
		row.projectionID,
		row.workspaceID,
		row.projectionKind,
		row.originID,
		row.status,
		row.attemptCount,
		row.lastError,
		row.availableAt,
		row.enqueuedAt,
		row.updatedAt,
	)
	return err
}
