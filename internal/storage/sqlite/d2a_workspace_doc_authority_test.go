package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestUpsertWorkspaceDocWithEffectsRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-doc-put-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Workspace Doc Put Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceDocAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	_, _, err := store.UpsertWorkspaceDocWithEffects(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Missing authority should fail closed",
		Content:     "No workspace_doc row or runtime event should be created without workspace authority.",
		UpdatedBy:   "tests",
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

	assertWorkspaceDocCount(t, ctx, store, workspaceID, 0)
	assertWorkspaceDocRevisionCount(t, ctx, store, workspaceID, "runbook", 0)
	assertNoWorkspaceDocRuntimeEvents(t, ctx, store, workspaceID, "runbook", "workspace_doc.upserted")
	if afterUpdatedAt := mustWorkspaceDocAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestArchiveWorkspaceDocWithEffectsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-doc-archive-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Workspace Doc Archive Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Runbook",
		Content:     "Stale authority should not archive workspace docs.",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("seed workspace doc: %v", err)
	}
	beforeDoc := mustWorkspaceDocRecord(t, ctx, store, workspaceID, "runbook")
	beforeUpdatedAt := mustWorkspaceDocAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeArchivedEvents := countWorkspaceDocRuntimeEvents(t, ctx, store, workspaceID, "runbook", "workspace_doc.archived")
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-2101")

	_, _, err := store.ArchiveWorkspaceDocWithEffects(ctx, workspaceID, "runbook", "tests")
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

	afterDoc := mustWorkspaceDocRecord(t, ctx, store, workspaceID, "runbook")
	if afterDoc.ArchivedAt != beforeDoc.ArchivedAt || afterDoc.UpdatedAt != beforeDoc.UpdatedAt {
		t.Fatalf("expected stale authority reject not to archive doc, before=%+v after=%+v", beforeDoc, afterDoc)
	}
	if got := countWorkspaceDocRuntimeEvents(t, ctx, store, workspaceID, "runbook", "workspace_doc.archived"); got != beforeArchivedEvents {
		t.Fatalf("expected no new workspace_doc.archived event after authority reject, before=%d after=%d", beforeArchivedEvents, got)
	}
	assertTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceDocAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestDeleteWorkspaceDocWithEffectsRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-doc-delete-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Workspace Doc Delete Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Runbook",
		Content:     "Stale authority should not delete workspace docs.",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("seed workspace doc: %v", err)
	}
	beforeDoc := mustWorkspaceDocRecord(t, ctx, store, workspaceID, "runbook")
	beforeUpdatedAt := mustWorkspaceDocAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeDeletedEvents := countWorkspaceDocRuntimeEvents(t, ctx, store, workspaceID, "runbook", "workspace_doc.deleted")
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-2102")

	_, _, err := store.DeleteWorkspaceDocWithEffects(ctx, workspaceID, "runbook", "tests")
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

	afterDoc := mustWorkspaceDocRecord(t, ctx, store, workspaceID, "runbook")
	if afterDoc.UpdatedAt != beforeDoc.UpdatedAt || afterDoc.ArchivedAt != beforeDoc.ArchivedAt {
		t.Fatalf("expected stale authority reject not to mutate doc record, before=%+v after=%+v", beforeDoc, afterDoc)
	}
	if got := countWorkspaceDocRuntimeEvents(t, ctx, store, workspaceID, "runbook", "workspace_doc.deleted"); got != beforeDeletedEvents {
		t.Fatalf("expected no new workspace_doc.deleted event after authority reject, before=%d after=%d", beforeDeletedEvents, got)
	}
	assertTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceDocAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func mustWorkspaceDocAuthorityWorkspaceUpdatedAt(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) string {
	t.Helper()

	var updatedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT updated_at FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&updatedAt); err != nil {
		t.Fatalf("load workspace updated_at: %v", err)
	}
	return updatedAt
}

func mustWorkspaceDocRecord(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, docKey string) sqlite.WorkspaceDocRecord {
	t.Helper()

	record, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get workspace doc %s/%s: %v", workspaceID, docKey, err)
	}
	return record
}

func assertWorkspaceDocCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_docs WHERE workspace_id = ?`, workspaceID).Scan(&got); err != nil {
		t.Fatalf("count workspace docs: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d workspace docs, got %d", want, got)
	}
}

func assertWorkspaceDocRevisionCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, docKey string, want int) {
	t.Helper()

	var got int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_doc_revisions WHERE workspace_id = ? AND doc_key = ?`, workspaceID, docKey).Scan(&got); err != nil {
		t.Fatalf("count workspace doc revisions: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d workspace doc revisions, got %d", want, got)
	}
}

func countWorkspaceDocRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, docKey, eventType string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "workspace_doc",
		EntityID:    docKey,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list %s runtime events for %s: %v", eventType, docKey, err)
	}
	return len(events)
}

func assertNoWorkspaceDocRuntimeEvents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, docKey, eventType string) {
	t.Helper()

	if got := countWorkspaceDocRuntimeEvents(t, ctx, store, workspaceID, docKey, eventType); got != 0 {
		t.Fatalf("expected no %s runtime events for %s, got %d", eventType, docKey, got)
	}
}
