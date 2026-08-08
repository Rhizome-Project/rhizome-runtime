package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRecordWorkspaceMemoryWithEffectsRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-memory-record-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Memory Record Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceMemoryAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	_, _, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "Missing authority should fail closed",
		Body:        "No memory row or runtime event should be created without workspace authority.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing workspace authority reject, got %+v", reject)
	}

	assertWorkspaceMemoryCount(t, ctx, store, workspaceID, 0)
	assertNoWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, "", "workspace_memory.recorded")
	assertWorkspaceMemoryProjectionOutboxCount(t, ctx, store, workspaceID, "", 0)
	if afterUpdatedAt := mustWorkspaceMemoryAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestArchiveWorkspaceMemoryWithEffectsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-memory-archive-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Memory Archive Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	record, _, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "Archive stale authority",
		Body:        "Stale authority should not archive workspace memory.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed workspace memory: %v", err)
	}
	beforeRecord := mustWorkspaceMemoryRecordForAuthorityReject(t, ctx, store, workspaceID, record.MemoryID)
	beforeUpdatedAt := mustWorkspaceMemoryAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeArchivedEvents := countWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, record.MemoryID, "workspace_memory.archived")
	beforeOutbox := countWorkspaceMemoryProjectionOutboxRows(t, ctx, store, workspaceID, record.MemoryID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-1801")

	_, _, _, err = store.ArchiveWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "tests",
		Reason:      "should fail closed under stale authority",
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale workspace authority reject, got %+v", reject)
	}

	afterRecord := mustWorkspaceMemoryRecordForAuthorityReject(t, ctx, store, workspaceID, record.MemoryID)
	if afterRecord.ArchivedAt != beforeRecord.ArchivedAt || afterRecord.UpdatedAt != beforeRecord.UpdatedAt || afterRecord.ArchivedReason != beforeRecord.ArchivedReason {
		t.Fatalf("expected stale authority reject not to archive record, before=%+v after=%+v", beforeRecord, afterRecord)
	}
	if got := countWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, record.MemoryID, "workspace_memory.archived"); got != beforeArchivedEvents {
		t.Fatalf("expected no new workspace_memory.archived event after authority reject, before=%d after=%d", beforeArchivedEvents, got)
	}
	if got := countWorkspaceMemoryProjectionOutboxRows(t, ctx, store, workspaceID, record.MemoryID); got != beforeOutbox {
		t.Fatalf("expected no new workspace memory projection outbox rows after authority reject, before=%d after=%d", beforeOutbox, got)
	}
	assertTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceMemoryAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestRestoreWorkspaceMemoryWithEffectsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-memory-restore-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Memory Restore Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	record, _, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "Restore stale authority",
		Body:        "Stale authority should not restore workspace memory.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed workspace memory: %v", err)
	}
	record, _, _, err = store.ArchiveWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "tests",
		Reason:      "archive before stale restore",
	})
	if err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}
	beforeRecord := mustWorkspaceMemoryRecordForAuthorityReject(t, ctx, store, workspaceID, record.MemoryID)
	beforeUpdatedAt := mustWorkspaceMemoryAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeRestoredEvents := countWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, record.MemoryID, "workspace_memory.restored")
	beforeOutbox := countWorkspaceMemoryProjectionOutboxRows(t, ctx, store, workspaceID, record.MemoryID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-1802")

	_, _, _, err = store.RestoreWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		RestoredBy:  "tests",
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale workspace authority reject, got %+v", reject)
	}

	afterRecord := mustWorkspaceMemoryRecordForAuthorityReject(t, ctx, store, workspaceID, record.MemoryID)
	if afterRecord.ArchivedAt == nil || beforeRecord.ArchivedAt == nil || *afterRecord.ArchivedAt != *beforeRecord.ArchivedAt || afterRecord.UpdatedAt != beforeRecord.UpdatedAt {
		t.Fatalf("expected stale authority reject to keep record archived, before=%+v after=%+v", beforeRecord, afterRecord)
	}
	if got := countWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, record.MemoryID, "workspace_memory.restored"); got != beforeRestoredEvents {
		t.Fatalf("expected no new workspace_memory.restored event after authority reject, before=%d after=%d", beforeRestoredEvents, got)
	}
	if got := countWorkspaceMemoryProjectionOutboxRows(t, ctx, store, workspaceID, record.MemoryID); got != beforeOutbox {
		t.Fatalf("expected no new workspace memory projection outbox rows after authority reject, before=%d after=%d", beforeOutbox, got)
	}
	assertTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceMemoryAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestRMPRunBatchedPruningRejectsMissingWorkspaceAuthorityWithoutPruning(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-memory-pruning-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Memory Pruning Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	record, _, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Pruning missing authority",
		Body:        "Missing authority should not let RMP pruning archive workspace memory.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed workspace memory: %v", err)
	}
	seedPastGcWorkspaceMemoryForPruningAuthorityTest(t, ctx, store, workspaceID, record.MemoryID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceMemoryAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)

	_, err = store.RMPRunBatchedPruning(ctx, workspaceID, 10)
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing workspace authority reject, got %+v", reject)
	}

	afterRecord := mustWorkspaceMemoryRecordForAuthorityReject(t, ctx, store, workspaceID, record.MemoryID)
	if afterRecord.ArchivedAt != nil || afterRecord.UpdatedAt != record.UpdatedAt {
		t.Fatalf("expected missing-authority prune reject to keep workspace memory active, got %+v", afterRecord)
	}
	if got := countWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, record.MemoryID, "workspace_memory.archived"); got != 0 {
		t.Fatalf("expected no workspace_memory.archived events after missing-authority prune, got %d", got)
	}
	if got := countAuthorityRejectEvents(t, ctx, store, workspaceID); got != beforeRejects {
		t.Fatalf("expected no authority.rejected event without fence input, before=%d after=%d", beforeRejects, got)
	}
	if afterUpdatedAt := mustWorkspaceMemoryAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority prune reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, "memnode:workspace_memory:"+record.MemoryID)
	if err != nil {
		t.Fatalf("get memory graph node after missing-authority prune: %v", err)
	}
	if detail.Node.LifecycleState != "ACTIVE" || detail.Node.ArchivedAt != nil {
		t.Fatalf("expected memory graph node to remain active after missing-authority prune, got %+v", detail.Node)
	}
}

func TestRMPRunBatchedPruningRejectsStaleWorkspaceAuthorityWithoutPruning(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-memory-pruning-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Memory Pruning Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	record, _, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Pruning stale authority",
		Body:        "Stale authority should not let RMP pruning archive workspace memory.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed workspace memory: %v", err)
	}
	seedPastGcWorkspaceMemoryForPruningAuthorityTest(t, ctx, store, workspaceID, record.MemoryID)

	beforeRecord := mustWorkspaceMemoryRecordForAuthorityReject(t, ctx, store, workspaceID, record.MemoryID)
	beforeUpdatedAt := mustWorkspaceMemoryAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeArchivedEvents := countWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, record.MemoryID, "workspace_memory.archived")
	beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-1803-1")

	_, err = store.RMPRunBatchedPruning(ctx, workspaceID, 10)
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale workspace authority reject, got %+v", reject)
	}

	afterRecord := mustWorkspaceMemoryRecordForAuthorityReject(t, ctx, store, workspaceID, record.MemoryID)
	if afterRecord.ArchivedAt != beforeRecord.ArchivedAt || afterRecord.UpdatedAt != beforeRecord.UpdatedAt || afterRecord.ArchivedReason != beforeRecord.ArchivedReason {
		t.Fatalf("expected stale-authority prune reject not to archive record, before=%+v after=%+v", beforeRecord, afterRecord)
	}
	if got := countWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, record.MemoryID, "workspace_memory.archived"); got != beforeArchivedEvents {
		t.Fatalf("expected no new workspace_memory.archived event after stale-authority prune reject, before=%d after=%d", beforeArchivedEvents, got)
	}
	assertAuthorityRejectEventIncrement(t, ctx, store, workspaceID, beforeRejects, sqlite.AuthorityRejectStale)
	if afterUpdatedAt := mustWorkspaceMemoryAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to stale-authority prune journaling, still %q", afterUpdatedAt)
	}
	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, "memnode:workspace_memory:"+record.MemoryID)
	if err != nil {
		t.Fatalf("get memory graph node after stale-authority prune: %v", err)
	}
	if detail.Node.LifecycleState != "ACTIVE" || detail.Node.ArchivedAt != nil {
		t.Fatalf("expected memory graph node to remain active after stale-authority prune, got %+v", detail.Node)
	}
}

func mustWorkspaceMemoryAuthorityWorkspaceUpdatedAt(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) string {
	t.Helper()

	var updatedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT updated_at FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&updatedAt); err != nil {
		t.Fatalf("load workspace updated_at: %v", err)
	}
	return updatedAt
}

func mustWorkspaceMemoryRecordForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID string) sqlite.WorkspaceMemoryRecord {
	t.Helper()

	record, err := store.GetWorkspaceMemory(ctx, workspaceID, memoryID)
	if err != nil {
		t.Fatalf("get workspace memory %s/%s: %v", workspaceID, memoryID, err)
	}
	return record
}

func assertWorkspaceMemoryCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_memory WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count workspace_memory rows: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d workspace_memory rows, got %d", want, got)
	}
}

func countWorkspaceMemoryRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID, eventType string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "workspace_memory",
		EntityID:    memoryID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list %s runtime events for %s: %v", eventType, memoryID, err)
	}
	return len(events)
}

func assertNoWorkspaceMemoryRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID, eventType string) {
	t.Helper()

	if got := countWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, memoryID, eventType); got != 0 {
		t.Fatalf("expected no %s runtime events for %s, got %d", eventType, memoryID, got)
	}
}

func countWorkspaceMemoryProjectionOutboxRows(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID string) int {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM memory_projection_outbox WHERE workspace_id = ? AND projection_kind = ? AND (? = '' OR origin_id = ?)`, workspaceID, "workspace_memory", memoryID, memoryID).Scan(&got); err != nil {
		t.Fatalf("count workspace memory projection outbox rows: %v", err)
	}
	return got
}

func assertWorkspaceMemoryProjectionOutboxCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID string, want int) {
	t.Helper()

	if got := countWorkspaceMemoryProjectionOutboxRows(t, ctx, store, workspaceID, memoryID); got != want {
		t.Fatalf("expected %d workspace memory projection outbox rows for %s/%s, got %d", want, workspaceID, memoryID, got)
	}
}

func seedPastGcWorkspaceMemoryForPruningAuthorityTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID string) {
	t.Helper()

	now := time.Now().UTC()
	nodeID := "memnode:workspace_memory:" + memoryID
	pastStar := now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	pastAcc := now.Add(-4 * time.Hour).Format(time.RFC3339Nano)
	pastHot := now.Add(-3 * time.Hour).Format(time.RFC3339Nano)
	pastWarm := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	pastGc := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?)
		ON CONFLICT(memory_id) DO UPDATE SET
			a_i=excluded.a_i,
			t_i_star=excluded.t_i_star,
			t_i_acc=excluded.t_i_acc,
			n_i=excluded.n_i,
			q_i=excluded.q_i,
			h_i=excluded.h_i,
			t_hot=excluded.t_hot,
			t_warm=excluded.t_warm,
			t_gc=excluded.t_gc,
			updated_at=excluded.updated_at
	`, nodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc); err != nil {
		t.Fatalf("seed pruning salience row: %v", err)
	}
}
