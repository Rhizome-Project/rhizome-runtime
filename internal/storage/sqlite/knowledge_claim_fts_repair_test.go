package sqlite

import (
	"context"
	"fmt"
	"testing"
)

func TestKnowledgeClaimsFTSNeedsRebuildDetectsConstructorFailure(t *testing.T) {
	err := assertErrString("SQL logic error: vtable constructor failed: knowledge_claims_fts")
	if !knowledgeClaimsFTSNeedsRebuild(err) {
		t.Fatalf("expected vtable constructor failure to request knowledge claim FTS repair")
	}
}

func TestKnowledgeClaimsFTSNeedsRebuildDoesNotGloballyCatchBareMalformed(t *testing.T) {
	err := assertErrString("database disk image is malformed")
	if knowledgeClaimsFTSNeedsRebuild(err) {
		t.Fatalf("expected bare malformed error to require an explicit knowledge claim FTS context")
	}
	if !knowledgeClaimsFTSNeedsContextualRebuild(err) {
		t.Fatalf("expected contextual FTS path to repair bare malformed errors")
	}
}

func TestRecreateKnowledgeClaimsFTSAfterSchemaRemoval(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-knowledge-claim-force-fts-repair"

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Knowledge Claim FTS Force Repair",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "Seed claim",
		Body:        "Seed the claim FTS schema before forced recreation.",
		SourceKind:  "manual",
		SourceID:    "tests",
	}); err != nil {
		t.Fatalf("record seed claim: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := removeBrokenKnowledgeClaimsFTSSchemaTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("remove broken knowledge claim FTS schema: %v", err)
	}
	if err := recreateKnowledgeClaimsFTSTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("recreate knowledge claim FTS: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit knowledge claim FTS recreation: %v", err)
	}

	repaired, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "Second claim after forced FTS repair",
		Body:        "Forced recreation should keep knowledge claim writes searchable.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record claim after forced FTS recreation: %v", err)
	}
	items, err := store.SearchKnowledgeClaims(ctx, KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		Query:       "forced recreation searchable",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search knowledge claims after forced FTS recreation: %v", err)
	}
	if len(items) == 0 || items[0].ClaimID != repaired.ClaimID {
		t.Fatalf("expected repaired claim to be searchable, repaired=%s items=%+v", repaired.ClaimID, items)
	}
}

func TestRecordKnowledgeClaimRebuildsCorruptFTSIndex(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-knowledge-claim-fts-repair"

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Knowledge Claim FTS Repair",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "First claim",
		Body:        "Seed the knowledge claim FTS index before corruption.",
		SourceKind:  "manual",
		SourceID:    "tests",
	}); err != nil {
		t.Fatalf("record seed claim: %v", err)
	}

	result, err := store.WriteDB().ExecContext(ctx, `
UPDATE knowledge_claims_fts_data
   SET block = zeroblob(length(block))
 WHERE id = (SELECT id FROM knowledge_claims_fts_data ORDER BY id LIMIT 1)`)
	if err != nil {
		t.Skipf("cannot corrupt knowledge_claims_fts_data in this sqlite build: %v", err)
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		t.Skip("knowledge_claims_fts_data had no rows to corrupt")
	}

	repaired, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "Second claim after FTS repair",
		Body:        "The write path should rebuild the knowledge claim FTS index and retry once.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record claim after FTS corruption: %v", err)
	}
	items, err := store.SearchKnowledgeClaims(ctx, KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		Query:       "write path rebuild retry",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search knowledge claims after write-path FTS repair: %v", err)
	}
	if len(items) == 0 || items[0].ClaimID != repaired.ClaimID {
		t.Fatalf("expected repaired claim to be searchable, repaired=%s items=%+v", repaired.ClaimID, items)
	}
}

func TestSearchKnowledgeClaimsRebuildsCorruptFTSIndex(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-knowledge-claim-search-fts-repair"

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Knowledge Claim Search FTS Repair",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	recorded, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "Search repair claim",
		Body:        "The search path should rebuild the knowledge claim FTS index and retry once.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}

	result, err := store.WriteDB().ExecContext(ctx, `
UPDATE knowledge_claims_fts_data
   SET block = zeroblob(length(block))
 WHERE id = (SELECT id FROM knowledge_claims_fts_data ORDER BY id LIMIT 1)`)
	if err != nil {
		t.Skipf("cannot corrupt knowledge_claims_fts_data in this sqlite build: %v", err)
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		t.Skip("knowledge_claims_fts_data had no rows to corrupt")
	}

	items, err := store.SearchKnowledgeClaims(ctx, KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		Query:       "search path rebuild retry",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search knowledge claims after FTS corruption: %v", err)
	}
	if len(items) == 0 || items[0].ClaimID != recorded.ClaimID {
		t.Fatalf("expected recorded claim to be searchable, recorded=%s items=%+v", recorded.ClaimID, items)
	}
}

func TestSearchKnowledgeClaimsRecoversFromFTSConstructorFailure(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-knowledge-claim-fts-constructor-repair"

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Knowledge Claim FTS Constructor Repair",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	recorded, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "Constructor repair claim",
		Body:        "The public search path should recreate a broken knowledge claim FTS virtual table.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record claim: %v", err)
	}
	forceKnowledgeClaimsFTSConstructorFailure(t, ctx, store)

	items, err := store.SearchKnowledgeClaims(ctx, KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		Query:       "public search recreate broken virtual table",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search knowledge claims after FTS constructor failure: %v", err)
	}
	if len(items) == 0 || items[0].ClaimID != recorded.ClaimID {
		t.Fatalf("expected recorded claim to be searchable after constructor repair, recorded=%s items=%+v", recorded.ClaimID, items)
	}
}

func forceKnowledgeClaimsFTSConstructorFailure(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin constructor failure tx: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	var schemaVersion int64
	if err := tx.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&schemaVersion); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA writable_schema=ON`); err != nil {
		t.Fatalf("enable writable_schema: %v", err)
	}
	writableSchemaOff := false
	defer func() {
		if !writableSchemaOff {
			_, _ = tx.ExecContext(context.Background(), `PRAGMA writable_schema=OFF`)
		}
	}()
	for _, stmt := range []string{
		`DELETE FROM sqlite_schema WHERE name = 'knowledge_claims_fts_config'`,
		`DELETE FROM sqlite_schema WHERE tbl_name = 'knowledge_claims_fts_config'`,
		`DELETE FROM sqlite_schema WHERE name = 'knowledge_claims_fts_docsize'`,
		`DELETE FROM sqlite_schema WHERE tbl_name = 'knowledge_claims_fts_docsize'`,
		fmt.Sprintf(`PRAGMA schema_version=%d`, schemaVersion+1),
		`PRAGMA writable_schema=OFF`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("force FTS constructor failure with %q: %v", stmt, err)
		}
		if stmt == `PRAGMA writable_schema=OFF` {
			writableSchemaOff = true
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit constructor failure tx: %v", err)
	}
	committed = true
}
