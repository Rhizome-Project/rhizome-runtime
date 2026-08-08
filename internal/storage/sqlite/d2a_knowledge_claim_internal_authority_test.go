package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestRecordKnowledgeClaimWithEffectsRejectsMissingWorkspaceAuthorityWithoutRuntimeSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-knowledge-claim-missing-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Knowledge Claim Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.db.ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, authorityScopeWorkspace); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	_, _, _, err := store.RecordKnowledgeClaimWithEffects(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "missing authority should fail closed",
		Body:        "generic RecordKnowledgeClaimWithEffects must not append raw knowledge_claim.written without authority.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %v", err)
	}

	if got := countKnowledgeClaimRowsForAuthorityTest(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no knowledge_claim rows after missing-authority reject, got %d", got)
	}
	if got := countKnowledgeClaimRuntimeEventsForAuthorityTest(t, ctx, store, workspaceID, "", "knowledge_claim.written"); got != 0 {
		t.Fatalf("expected no knowledge_claim.written events after missing-authority reject, got %d", got)
	}
	if got := countRuntimeMemoryHelperAuthorityRejectedEvents(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no authority.rejected event for pre-fence missing-authority reject, got %d", got)
	}
}

func TestRecordKnowledgeClaimRejectsStaleWorkspaceAuthorityWithoutPersistedRows(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-knowledge-claim-stale-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Knowledge Claim Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeRejects := countRuntimeMemoryHelperAuthorityRejectedEvents(t, ctx, store, workspaceID)
	transferRuntimeMemoryWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-4701")

	_, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "stale authority should fail closed",
		Body:        "generic RecordKnowledgeClaim must not persist knowledge claims under stale authority.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %v", err)
	}

	if got := countKnowledgeClaimRowsForAuthorityTest(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no knowledge_claim rows after stale-authority reject, got %d", got)
	}
	if got := countKnowledgeClaimRuntimeEventsForAuthorityTest(t, ctx, store, workspaceID, "", "knowledge_claim.written"); got != 0 {
		t.Fatalf("expected no knowledge_claim.written events after stale-authority reject, got %d", got)
	}
	if got := countRuntimeMemoryHelperAuthorityRejectedEvents(t, ctx, store, workspaceID); got != beforeRejects+1 {
		t.Fatalf("expected stale-authority reject to journal authority.rejected once, before=%d after=%d", beforeRejects, got)
	}
}

func TestUpsertKnowledgeClaimTxRejectsStaleWorkspaceAuthorityWithoutPersistedRows(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-knowledge-claim-tx-stale-authority"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Knowledge Claim Internal TX Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	transferRuntimeMemoryWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-4702")

	record, err := normalizeKnowledgeClaimInput(KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "internal tx helper stale authority",
		Body:        "upsertKnowledgeClaimTx should fail closed without persisted rows under stale authority.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("normalize knowledge claim input: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	_, _, _, err = store.upsertKnowledgeClaimTx(ctx, tx, record, time.Now().UTC().Format(time.RFC3339Nano))
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

	if got := countKnowledgeClaimRowsForAuthorityTest(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected no persisted knowledge_claim rows after internal stale-authority reject, got %d", got)
	}
	if got := countKnowledgeClaimRuntimeEventsForAuthorityTest(t, ctx, store, workspaceID, "", "knowledge_claim.written"); got != 0 {
		t.Fatalf("expected no persisted knowledge_claim.written events after internal stale-authority reject, got %d", got)
	}
	if got := countRuntimeMemoryHelperAuthorityRejectedEvents(t, ctx, store, workspaceID); got != 0 {
		t.Fatalf("expected internal helper reject not to journal authority.rejected, got %d", got)
	}
}

func countKnowledgeClaimRowsForAuthorityTest(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
	t.Helper()

	var count int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM knowledge_claims WHERE workspace_id = ?`, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count knowledge_claims rows: %v", err)
	}
	return count
}

func countKnowledgeClaimRuntimeEventsForAuthorityTest(t *testing.T, ctx context.Context, store *Store, workspaceID, entityID, eventType string) int {
	t.Helper()

	var count int
	if err := store.db.QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM runtime_events
 WHERE workspace_id = ?
   AND event_type = ?
   AND (? = '' OR entity_id = ?)
`, workspaceID, eventType, entityID, entityID).Scan(&count); err != nil {
		t.Fatalf("count knowledge claim runtime events for %s/%s: %v", eventType, entityID, err)
	}
	return count
}
