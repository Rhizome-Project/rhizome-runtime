package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func execKnowledgeClaimWriteWithFTSRepairTx(ctx context.Context, tx *sql.Tx, query string, args ...any) (sql.Result, error) {
	result, err := tx.ExecContext(ctx, query, args...)
	if err == nil {
		return result, nil
	}
	if !knowledgeClaimsFTSNeedsContextualRebuild(err) {
		return nil, err
	}
	if rebuildErr := rebuildKnowledgeClaimsFTSTx(ctx, tx); rebuildErr != nil {
		return nil, fmt.Errorf("%w; knowledge_claims_fts rebuild failed: %v", err, rebuildErr)
	}
	result, retryErr := tx.ExecContext(ctx, query, args...)
	if retryErr != nil {
		return nil, fmt.Errorf("retry after knowledge_claims_fts rebuild: %w (original error: %v)", retryErr, err)
	}
	return result, nil
}

func (s *Store) searchKnowledgeClaimsWithFTSRepair(ctx context.Context, query string, args ...any) ([]KnowledgeClaimRecord, error) {
	records, err := s.queryAndCollectKnowledgeClaims(ctx, query, args...)
	if err == nil {
		return records, nil
	}
	if !knowledgeClaimsFTSNeedsContextualRebuild(err) {
		return nil, err
	}
	if rebuildErr := s.repairKnowledgeClaimsFTS(ctx); rebuildErr != nil {
		return nil, fmt.Errorf("%w; knowledge_claims_fts rebuild failed: %v", err, rebuildErr)
	}
	records, retryErr := s.queryAndCollectKnowledgeClaims(ctx, query, args...)
	if retryErr != nil {
		return nil, fmt.Errorf("retry after knowledge_claims_fts rebuild: %w (original error: %v)", retryErr, err)
	}
	return records, nil
}

func (s *Store) queryAndCollectKnowledgeClaims(ctx context.Context, query string, args ...any) ([]KnowledgeClaimRecord, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectKnowledgeClaimRows(rows)
}

func (s *Store) repairKnowledgeClaimsFTS(ctx context.Context) error {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := rebuildKnowledgeClaimsFTSTx(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func rebuildKnowledgeClaimsFTSTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO knowledge_claims_fts(knowledge_claims_fts) VALUES ('rebuild')`); err != nil {
		if !knowledgeClaimsFTSNeedsRebuild(err) {
			return err
		}
		if recreateErr := recreateKnowledgeClaimsFTSTx(ctx, tx); recreateErr != nil {
			return fmt.Errorf("%w; recreate knowledge_claims_fts failed: %v", err, recreateErr)
		}
	}
	return nil
}

func recreateKnowledgeClaimsFTSTx(ctx context.Context, tx *sql.Tx) error {
	for _, stmt := range []string{
		`DROP TRIGGER IF EXISTS knowledge_claims_ai`,
		`DROP TRIGGER IF EXISTS knowledge_claims_ad`,
		`DROP TRIGGER IF EXISTS knowledge_claims_au`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS knowledge_claims_fts`); err != nil {
		if !knowledgeClaimsFTSNeedsRebuild(err) {
			return err
		}
		if err := removeBrokenKnowledgeClaimsFTSSchemaTx(ctx, tx); err != nil {
			return err
		}
	}
	for _, stmt := range []string{
		`CREATE VIRTUAL TABLE knowledge_claims_fts USING fts5(
    subject,
    body,
    summary,
    content='knowledge_claims',
    content_rowid='rowid'
)`,
		`CREATE TRIGGER knowledge_claims_ai AFTER INSERT ON knowledge_claims BEGIN
    INSERT INTO knowledge_claims_fts(rowid, subject, body, summary)
    VALUES (new.rowid, new.subject, new.body, new.summary);
END`,
		`CREATE TRIGGER knowledge_claims_ad AFTER DELETE ON knowledge_claims BEGIN
    INSERT INTO knowledge_claims_fts(knowledge_claims_fts, rowid, subject, body, summary)
    VALUES ('delete', old.rowid, old.subject, old.body, old.summary);
END`,
		`CREATE TRIGGER knowledge_claims_au AFTER UPDATE ON knowledge_claims BEGIN
    INSERT INTO knowledge_claims_fts(knowledge_claims_fts, rowid, subject, body, summary)
    VALUES ('delete', old.rowid, old.subject, old.body, old.summary);
    INSERT INTO knowledge_claims_fts(rowid, subject, body, summary)
    VALUES (new.rowid, new.subject, new.body, new.summary);
END`,
		`INSERT INTO knowledge_claims_fts(knowledge_claims_fts) VALUES ('rebuild')`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func removeBrokenKnowledgeClaimsFTSSchemaTx(ctx context.Context, tx *sql.Tx) error {
	var schemaVersion int64
	if err := tx.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&schemaVersion); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA writable_schema=ON`); err != nil {
		return err
	}
	writableSchemaOff := false
	defer func() {
		if !writableSchemaOff {
			_, _ = tx.ExecContext(context.Background(), `PRAGMA writable_schema=OFF`)
		}
	}()
	for _, stmt := range []string{
		`DELETE FROM sqlite_schema WHERE name = 'knowledge_claims_fts' OR name LIKE 'knowledge_claims_fts_%'`,
		`DELETE FROM sqlite_schema WHERE tbl_name = 'knowledge_claims_fts' OR tbl_name LIKE 'knowledge_claims_fts_%'`,
		fmt.Sprintf(`PRAGMA schema_version=%d`, schemaVersion+1),
		`PRAGMA writable_schema=OFF`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
		if stmt == `PRAGMA writable_schema=OFF` {
			writableSchemaOff = true
		}
	}
	return nil
}

func knowledgeClaimsFTSNeedsRebuild(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return (strings.Contains(msg, "fts5") || strings.Contains(msg, "knowledge_claims_fts")) &&
		(strings.Contains(msg, "run 'rebuild'") ||
			strings.Contains(msg, "invalid fts5") ||
			strings.Contains(msg, "vtable constructor failed") ||
			strings.Contains(msg, "database disk image is malformed") ||
			strings.Contains(msg, "malformed") ||
			strings.Contains(msg, "no such table"))
}

func knowledgeClaimsFTSNeedsContextualRebuild(err error) bool {
	if knowledgeClaimsFTSNeedsRebuild(err) {
		return true
	}
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database disk image is malformed")
}
