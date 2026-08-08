package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryWriteRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-memory-write-missing-authority-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Memory Write Missing Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceMemoryWriteParams{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "Missing authority write",
		Body:        "workspace.memory.write should fail closed before row or runtime side effects.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("marshal workspace memory write params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryWrite(testAuthContext(workspaceID, "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertWorkspaceMemoryAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.memory.write")

	assertServerWorkspaceMemoryRowCount(t, ctx, store, workspaceID, 0)
	assertNoServerWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, "", "workspace_memory.recorded")
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceMemoryRemoveRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-memory-remove-stale-authority-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Memory Remove Stale Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	record, _, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "Stale remove",
		Body:        "workspace.memory.remove should fail closed under stale authority.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed workspace memory: %v", err)
	}
	beforeRecord := mustServerWorkspaceMemoryRecord(t, ctx, store, workspaceID, record.MemoryID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1901")

	raw, err := json.Marshal(workspaceMemoryRemoveParams{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		RemovedBy:   "developer",
		Reason:      "should fail closed under stale authority",
	})
	if err != nil {
		t.Fatalf("marshal workspace memory remove params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryRemove(testAuthContext(workspaceID, "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertWorkspaceMemoryAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.memory.remove")

	afterRecord := mustServerWorkspaceMemoryRecord(t, ctx, store, workspaceID, record.MemoryID)
	if afterRecord.ArchivedAt != beforeRecord.ArchivedAt || afterRecord.UpdatedAt != beforeRecord.UpdatedAt {
		t.Fatalf("expected stale authority reject not to archive memory, before=%+v after=%+v", beforeRecord, afterRecord)
	}
	assertNoServerWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, record.MemoryID, "workspace_memory.archived")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestWorkspaceMemoryRestoreRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-memory-restore-stale-authority-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Memory Restore Stale Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	record, _, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "Stale restore",
		Body:        "workspace.memory.restore should fail closed under stale authority.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed workspace memory: %v", err)
	}
	record, _, _, err = store.ArchiveWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "archive before stale restore",
	})
	if err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}
	beforeRecord := mustServerWorkspaceMemoryRecord(t, ctx, store, workspaceID, record.MemoryID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-1902")

	raw, err := json.Marshal(workspaceMemoryRestoreParams{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		RestoredBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal workspace memory restore params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryRestore(testAuthContext(workspaceID, "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertWorkspaceMemoryAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.memory.restore")

	afterRecord := mustServerWorkspaceMemoryRecord(t, ctx, store, workspaceID, record.MemoryID)
	if afterRecord.ArchivedAt == nil || beforeRecord.ArchivedAt == nil || *afterRecord.ArchivedAt != *beforeRecord.ArchivedAt || afterRecord.UpdatedAt != beforeRecord.UpdatedAt {
		t.Fatalf("expected stale authority reject to keep memory archived, before=%+v after=%+v", beforeRecord, afterRecord)
	}
	assertNoServerWorkspaceMemoryRuntimeEvents(t, ctx, store, workspaceID, record.MemoryID, "workspace_memory.restored")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func assertWorkspaceMemoryAuthorityRejectDetails(t *testing.T, rpcErr *RPCError, rejectCode sqlite.AuthorityRejectCode, surface string) {
	t.Helper()

	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(rejectCode) || details["surface"] != surface {
		t.Fatalf("unexpected authority reject details %+v", details)
	}
}

func mustServerWorkspaceMemoryRecord(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID string) sqlite.WorkspaceMemoryRecord {
	t.Helper()

	record, err := store.GetWorkspaceMemory(ctx, workspaceID, memoryID)
	if err != nil {
		t.Fatalf("get workspace memory %s/%s: %v", workspaceID, memoryID, err)
	}
	return record
}

func assertServerWorkspaceMemoryRowCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_memory WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count workspace_memory rows: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d workspace_memory rows, got %d", want, got)
	}
}

func assertNoServerWorkspaceMemoryRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID, eventType string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "workspace_memory",
		EntityID:    memoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events for %s: %v", eventType, memoryID, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s runtime events for %s, got %+v", eventType, memoryID, events)
	}
}
