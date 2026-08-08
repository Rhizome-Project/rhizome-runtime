package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceDocPutRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-doc-put-missing-authority-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Workspace Doc Put Missing Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceDocPutParams{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "CLI missing authority",
		Content:     "workspace.doc.put should fail closed before any doc or runtime side effect.",
		UpdatedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("marshal workspace doc put params: %v", err)
	}

	result, rpcErr := h.workspaceDocPut(testAuthContext(workspaceID, "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertWorkspaceDocAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.doc.put")

	assertServerWorkspaceDocCount(t, ctx, store, workspaceID, 0)
	assertNoServerWorkspaceDocRuntimeEvents(t, ctx, store, workspaceID, "runbook", "workspace_doc.upserted")
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceDocArchiveRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-doc-archive-stale-authority-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Workspace Doc Archive Stale Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Runbook",
		Content:     "workspace.doc.archive should fail closed under stale authority.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("seed workspace doc: %v", err)
	}
	beforeDoc := mustServerWorkspaceDocRecord(t, ctx, store, workspaceID, "runbook")
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-2201")

	raw, err := json.Marshal(workspaceDocArchiveParams{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		ArchivedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal workspace doc archive params: %v", err)
	}

	result, rpcErr := h.workspaceDocArchive(testAuthContext(workspaceID, "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertWorkspaceDocAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.doc.archive")

	afterDoc := mustServerWorkspaceDocRecord(t, ctx, store, workspaceID, "runbook")
	if afterDoc.ArchivedAt != beforeDoc.ArchivedAt || afterDoc.UpdatedAt != beforeDoc.UpdatedAt {
		t.Fatalf("expected stale authority reject not to archive doc, before=%+v after=%+v", beforeDoc, afterDoc)
	}
	assertNoServerWorkspaceDocRuntimeEvents(t, ctx, store, workspaceID, "runbook", "workspace_doc.archived")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestWorkspaceDocDeleteRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-d2a-doc-delete-stale-authority-rpc"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Workspace Doc Delete Stale Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Runbook",
		Content:     "workspace.doc.delete should fail closed under stale authority.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("seed workspace doc: %v", err)
	}
	beforeDoc := mustServerWorkspaceDocRecord(t, ctx, store, workspaceID, "runbook")
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-2202")

	raw, err := json.Marshal(workspaceDocDeleteParams{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		DeletedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("marshal workspace doc delete params: %v", err)
	}

	result, rpcErr := h.workspaceDocDelete(testAuthContext(workspaceID, "human", "developer"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertWorkspaceDocAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.doc.delete")

	afterDoc := mustServerWorkspaceDocRecord(t, ctx, store, workspaceID, "runbook")
	if afterDoc.ArchivedAt != beforeDoc.ArchivedAt || afterDoc.UpdatedAt != beforeDoc.UpdatedAt {
		t.Fatalf("expected stale authority reject not to delete doc, before=%+v after=%+v", beforeDoc, afterDoc)
	}
	assertNoServerWorkspaceDocRuntimeEvents(t, ctx, store, workspaceID, "runbook", "workspace_doc.deleted")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func assertWorkspaceDocAuthorityRejectDetails(t *testing.T, rpcErr *RPCError, rejectCode sqlite.AuthorityRejectCode, surface string) {
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

func mustServerWorkspaceDocRecord(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, docKey string) sqlite.WorkspaceDocRecord {
	t.Helper()

	record, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get workspace doc %s/%s: %v", workspaceID, docKey, err)
	}
	return record
}

func assertServerWorkspaceDocCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_docs WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count workspace docs: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d workspace docs, got %d", want, got)
	}
}

func assertNoServerWorkspaceDocRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, docKey, eventType string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "workspace_doc",
		EntityID:    docKey,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events for %s: %v", eventType, docKey, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s runtime events for %s, got %+v", eventType, docKey, events)
	}
}
