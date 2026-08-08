package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestRecordWorkspaceMemoryWithEffectsTxRejectsMissingWorkspaceAuthorityWithoutPersistedRows(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-runtime-memory-tx-record-missing-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Runtime Memory Internal Record Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.db.ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, authorityScopeWorkspace); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	_, _, _, err = store.recordWorkspaceMemoryWithEffectsTx(ctx, tx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "internal tx helper requires authority",
		Body:        "recordWorkspaceMemoryWithEffectsTx should fail closed without workspace authority.",
		SourceKind:  "manual",
		SourceID:    "tests",
	}, time.Now().UTC().Format(time.RFC3339Nano))
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectMissing {
		_ = tx.Rollback()
		t.Fatalf("expected missing authority reject, got %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}

	if got := countRuntimeMemoryHelperWorkspaceMemoryRows(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no persisted workspace_memory rows after missing-authority reject, got %d", got)
	}
	if got := countRuntimeMemoryHelperRuntimeEvents(t, ctx, store, workspaceID, "", "workspace_memory.recorded"); got != 0 {
		t.Fatalf("expected no workspace_memory.recorded events after missing-authority reject, got %d", got)
	}
	if got := countRuntimeMemoryHelperAuthorityRejectedEvents(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no authority.rejected events from internal tx helper reject, got %d", got)
	}
}

func TestArchiveWorkspaceMemoryWithEffectsTxRejectsStaleWorkspaceAuthorityWithoutPersistedRows(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-runtime-memory-tx-archive-stale-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Runtime Memory Internal Archive Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	record, _, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "stale internal archive helper",
		Body:        "archiveWorkspaceMemoryWithEffectsTx should fail closed when local authority is stale.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed workspace memory: %v", err)
	}
	transferRuntimeMemoryWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-4601")

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	_, _, _, err = store.archiveWorkspaceMemoryWithEffectsTx(ctx, tx, WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "tests",
		Reason:      "stale authority should fail closed",
	}, time.Now().UTC().Format(time.RFC3339Nano))
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectStale {
		_ = tx.Rollback()
		t.Fatalf("expected stale authority reject, got %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}

	reloaded, err := store.GetWorkspaceMemory(ctx, workspaceID, record.MemoryID)
	if err != nil {
		t.Fatalf("reload workspace memory: %v", err)
	}
	if reloaded.ArchivedAt != nil {
		t.Fatalf("expected stale-authority reject to keep workspace memory active, got %+v", reloaded)
	}
	if got := countRuntimeMemoryHelperRuntimeEvents(t, ctx, store, workspaceID, record.MemoryID, "workspace_memory.archived"); got != 0 {
		t.Fatalf("expected no workspace_memory.archived events after stale-authority helper reject, got %d", got)
	}
	if got := countRuntimeMemoryHelperAuthorityRejectedEvents(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no authority.rejected events from internal tx helper reject, got %d", got)
	}
}

func TestRestoreWorkspaceMemoryArchivedTxRejectsStaleWorkspaceAuthorityWithoutPersistedRows(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-runtime-memory-tx-restore-stale-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Runtime Memory Internal Restore Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	record, _, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "stale internal restore helper",
		Body:        "restoreWorkspaceMemoryArchivedTx should fail closed when local authority is stale.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed workspace memory: %v", err)
	}
	record, _, _, err = store.ArchiveWorkspaceMemoryWithEffects(ctx, WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "tests",
		Reason:      "archive before stale restore helper",
	})
	if err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}
	transferRuntimeMemoryWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-4602")

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	_, _, err = store.restoreWorkspaceMemoryArchivedTx(ctx, tx, record, "operator", "tests", "manual_reactivate", time.Now().UTC().Format(time.RFC3339Nano))
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectStale {
		_ = tx.Rollback()
		t.Fatalf("expected stale authority reject, got %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}

	reloaded, err := store.GetWorkspaceMemory(ctx, workspaceID, record.MemoryID)
	if err != nil {
		t.Fatalf("reload workspace memory: %v", err)
	}
	if reloaded.ArchivedAt == nil {
		t.Fatalf("expected stale-authority reject to keep workspace memory archived, got %+v", reloaded)
	}
	if got := countRuntimeMemoryHelperRuntimeEvents(t, ctx, store, workspaceID, record.MemoryID, "workspace_memory.restored"); got != 0 {
		t.Fatalf("expected no workspace_memory.restored events after stale-authority helper reject, got %d", got)
	}
	if got := countRuntimeMemoryHelperAuthorityRejectedEvents(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no authority.rejected events from internal tx helper reject, got %d", got)
	}
}

func transferRuntimeMemoryWorkspaceAuthorityToPeer(t *testing.T, ctx context.Context, store *Store, workspaceID string, current WorkspaceAuthorityRecord, peerNodeID string) {
	t.Helper()

	referenceAt := time.Now().UTC().Round(0)
	if _, err := store.db.ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(authority_node_id) DO UPDATE SET
	node_kind = excluded.node_kind,
	host_label = excluded.host_label,
	boot_instance_id = excluded.boot_instance_id,
	last_seen_at = excluded.last_seen_at,
	status = excluded.status
`,
		peerNodeID,
		"sqlite_peer_store",
		"peer-host",
		"boot-"+peerNodeID,
		referenceAt.Format(time.RFC3339Nano),
		referenceAt.Format(time.RFC3339Nano),
		string(RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("seed peer runtime node: %v", err)
	}

	commitWatermark := current.CommitWatermark + 1
	if applied := current.AppliedWatermark + 1; applied > commitWatermark {
		commitWatermark = applied
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, WorkspaceAuthorityTransferInput{
		WorkspaceID:                  workspaceID,
		Scope:                        authorityScopeWorkspace,
		CurrentHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		CurrentLeaseToken:            current.LeaseToken,
		CurrentTerm:                  current.Term,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-" + peerNodeID,
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             commitWatermark,
		ActorType:                    authorityActorTypeSystem,
		ActorID:                      "tests",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority: %v", err)
	}
}

func countRuntimeMemoryHelperWorkspaceMemoryRows(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
	t.Helper()

	var count int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_memory WHERE workspace_id = ?`, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count workspace_memory rows: %v", err)
	}
	return count
}

func countRuntimeMemoryHelperRuntimeEvents(t *testing.T, ctx context.Context, store *Store, workspaceID, entityID, eventType string) int {
	t.Helper()

	var count int
	if err := store.db.QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM runtime_events
 WHERE workspace_id = ?
   AND event_type = ?
   AND (? = '' OR entity_id = ?)
`, workspaceID, eventType, entityID, entityID).Scan(&count); err != nil {
		t.Fatalf("count runtime events for %s/%s: %v", eventType, entityID, err)
	}
	return count
}

func countRuntimeMemoryHelperAuthorityRejectedEvents(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
	t.Helper()

	var count int
	if err := store.db.QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM runtime_events
 WHERE workspace_id = ?
   AND event_type = ?
   AND entity_type = 'workspace_authority'
`, workspaceID, AuthorityEventRejected).Scan(&count); err != nil {
		t.Fatalf("count authority.rejected events: %v", err)
	}
	return count
}
