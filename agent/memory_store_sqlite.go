package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const (
	localMemorySQLiteSchemaVersion            = 1
	localMemoryPromotionPendingRetentionLimit = 128
	localMemoryPromotionHistoryRetentionLimit = 512
)

type localMemorySQLiteRefRecord struct {
	refType  string
	refValue string
}

func openSQLiteBackedLocalMemoryStore(workspaceID, agentID string) (*LocalMemoryStore, error) {
	path := localMemoryStorePath(workspaceID, agentID)
	store := &LocalMemoryStore{
		path:         path,
		legacyPath:   localMemoryLegacyStorePath(workspaceID, agentID),
		episodeIndex: map[string]int{},
		digestIndex:  map[string]int{},
		state: LocalMemoryState{
			Version:     localMemoryStateVersion,
			WorkspaceID: strings.TrimSpace(workspaceID),
			AgentID:     strings.TrimSpace(agentID),
			Episodes:    []LocalMemoryEpisodeRecord{},
			Digests:     []LocalMemoryDigestRecord{},
		},
	}
	db, err := openLocalMemorySQLiteWithRecovery(path)
	if err != nil {
		return nil, err
	}
	store.db = db
	if err := store.loadFromSQLiteOrRecover(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func openLocalMemorySQLiteWithRecovery(dbPath string) (*sql.DB, error) {
	db, err := openLocalMemorySQLite(dbPath)
	if err == nil {
		return db, nil
	}
	data, readErr := os.ReadFile(dbPath)
	if readErr != nil || len(data) == 0 {
		return nil, err
	}
	if quarantineErr := quarantineCorruptLocalMemorySQLite(dbPath); quarantineErr != nil {
		return nil, err
	}
	return openLocalMemorySQLite(dbPath)
}

func openLocalMemorySQLite(dbPath string) (*sql.DB, error) {
	if strings.TrimSpace(dbPath) == "" {
		return nil, fmt.Errorf("local memory db path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", sqliteDSNWithPragmas(dbPath,
		"busy_timeout=5000",
		"foreign_keys=ON",
	))
	if err != nil {
		return nil, fmt.Errorf("open local memory sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return reopenLocalMemorySQLiteAfterQuarantine(dbPath, fmt.Errorf("ping local memory sqlite: %w", err))
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		return reopenLocalMemorySQLiteAfterQuarantine(dbPath, fmt.Errorf("set local memory sqlite WAL mode: %w", err))
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		return reopenLocalMemorySQLiteAfterQuarantine(dbPath, fmt.Errorf("enable local memory sqlite foreign keys: %w", err))
	}
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000;"); err != nil {
		_ = db.Close()
		return reopenLocalMemorySQLiteAfterQuarantine(dbPath, fmt.Errorf("set local memory sqlite busy timeout: %w", err))
	}
	return db, nil
}

func reopenLocalMemorySQLiteAfterQuarantine(dbPath string, cause error) (*sql.DB, error) {
	data, readErr := os.ReadFile(dbPath)
	if readErr != nil || len(data) == 0 {
		return nil, cause
	}
	if quarantineErr := quarantineCorruptLocalMemorySQLite(dbPath); quarantineErr != nil {
		return nil, cause
	}
	db, err := sql.Open("sqlite", sqliteDSNWithPragmas(dbPath,
		"busy_timeout=5000",
		"foreign_keys=ON",
	))
	if err != nil {
		return nil, fmt.Errorf("reopen local memory sqlite after recovery: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, cause
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		return nil, cause
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		return nil, cause
	}
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000;"); err != nil {
		_ = db.Close()
		return nil, cause
	}
	return db, nil
}

func sqliteDSNWithPragmas(dbPath string, pragmas ...string) string {
	query := url.Values{}
	query.Set("_txlock", "immediate")
	for _, pragma := range pragmas {
		pragma = strings.TrimSpace(pragma)
		if pragma == "" {
			continue
		}
		query.Add("_pragma", pragma)
	}
	if len(query) == 0 {
		return dbPath
	}
	separator := "?"
	switch {
	case strings.Contains(dbPath, "?") && (strings.HasSuffix(dbPath, "?") || strings.HasSuffix(dbPath, "&")):
		separator = ""
	case strings.Contains(dbPath, "?"):
		separator = "&"
	}
	return dbPath + separator + query.Encode()
}

func (s *LocalMemoryStore) loadFromSQLiteOrRecover() error {
	if s == nil || s.db == nil {
		return nil
	}
	if err := ensureLocalMemorySQLiteSchema(s.db); err != nil {
		return s.recoverSQLiteState(err)
	}
	if err := pruneLocalMemoryPromotions(context.Background(), s.db); err != nil {
		return err
	}
	if err := s.maybeImportLegacyJSON(); err != nil {
		return err
	}
	state, err := loadLocalMemoryStateFromSQLite(s.db, s.state.WorkspaceID, s.state.AgentID)
	if err != nil {
		return s.recoverSQLiteState(err)
	}
	s.state = state
	s.reindexLocked()
	return nil
}

func (s *LocalMemoryStore) recoverSQLiteState(cause error) error {
	if s == nil {
		return cause
	}
	if s.db != nil {
		_ = s.db.Close()
		s.db = nil
	}
	if err := quarantineCorruptLocalMemorySQLite(s.path); err != nil {
		return fmt.Errorf("recover local memory sqlite after %v: %w", cause, err)
	}
	db, err := openLocalMemorySQLite(s.path)
	if err != nil {
		return fmt.Errorf("reopen local memory sqlite after recovery: %w", err)
	}
	s.db = db
	if err := ensureLocalMemorySQLiteSchema(s.db); err != nil {
		return err
	}
	if err := pruneLocalMemoryPromotions(context.Background(), s.db); err != nil {
		return err
	}
	if err := s.maybeImportLegacyJSON(); err != nil {
		return err
	}
	state, err := loadLocalMemoryStateFromSQLite(s.db, s.state.WorkspaceID, s.state.AgentID)
	if err != nil {
		return err
	}
	s.state = state
	s.reindexLocked()
	return nil
}

func quarantineCorruptLocalMemorySQLite(path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("local memory sqlite path is empty")
	}
	stamp := time.Now().UTC().Format("20060102T150405.000000000Z")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		target := path + suffix
		if _, err := os.Stat(target); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		backup := target + ".corrupt-" + stamp
		if err := os.Rename(target, backup); err != nil {
			return err
		}
	}
	return nil
}

func ensureLocalMemorySQLiteSchema(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS local_memory_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS local_memory_episodes (
			episode_id TEXT PRIMARY KEY,
			task_id TEXT,
			session_id TEXT,
			tension_id TEXT,
			proto_cluster_id TEXT,
			run_id TEXT,
			trigger TEXT,
			outcome TEXT,
			summary TEXT,
			digest_refs_json TEXT NOT NULL,
			doc_keys_json TEXT NOT NULL,
			artifact_refs_json TEXT NOT NULL,
			constraint_refs_json TEXT NOT NULL,
			segment_refs_json TEXT NOT NULL,
			tags_json TEXT NOT NULL,
			meta_json TEXT NOT NULL,
			created_at TEXT,
			superseded_at TEXT,
			invalidation_reasons_json TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS local_memory_digests (
			digest_id TEXT PRIMARY KEY,
			tier TEXT NOT NULL,
			kind TEXT NOT NULL,
			task_id TEXT,
			session_id TEXT,
			tension_id TEXT,
			proto_cluster_id TEXT,
			visibility TEXT,
			summary TEXT,
			body TEXT,
			source_episode_id TEXT,
			episode_digest_json TEXT NOT NULL,
			doc_keys_json TEXT NOT NULL,
			artifact_refs_json TEXT NOT NULL,
			constraint_refs_json TEXT NOT NULL,
			segment_refs_json TEXT NOT NULL,
			guards_json TEXT NOT NULL,
			tags_json TEXT NOT NULL,
			meta_json TEXT NOT NULL,
			created_at TEXT,
			updated_at TEXT,
			expires_at TEXT,
			last_accessed_at TEXT,
			stale INTEGER NOT NULL DEFAULT 0,
			invalidated_at TEXT,
			invalidation_reasons_json TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS local_memory_episode_refs (
			episode_id TEXT NOT NULL,
			ref_type TEXT NOT NULL,
			ref_value TEXT NOT NULL,
			PRIMARY KEY(episode_id, ref_type, ref_value)
		);`,
		`CREATE TABLE IF NOT EXISTS local_memory_digest_refs (
			digest_id TEXT NOT NULL,
			ref_type TEXT NOT NULL,
			ref_value TEXT NOT NULL,
			PRIMARY KEY(digest_id, ref_type, ref_value)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_local_memory_episodes_scope ON local_memory_episodes(task_id, session_id, tension_id, proto_cluster_id);`,
		`CREATE INDEX IF NOT EXISTS idx_local_memory_episodes_created ON local_memory_episodes(created_at);`,
		`CREATE INDEX IF NOT EXISTS idx_local_memory_digests_kind_scope ON local_memory_digests(kind, task_id, session_id, tension_id, proto_cluster_id, stale);`,
		`CREATE INDEX IF NOT EXISTS idx_local_memory_digests_updated ON local_memory_digests(updated_at);`,
		`CREATE INDEX IF NOT EXISTS idx_local_memory_episode_refs_lookup ON local_memory_episode_refs(ref_type, ref_value, episode_id);`,
		`CREATE INDEX IF NOT EXISTS idx_local_memory_digest_refs_lookup ON local_memory_digest_refs(ref_type, ref_value, digest_id);`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS local_memory_episodes_fts USING fts5(
			summary,
			trigger,
			outcome,
			tags,
			content=local_memory_episodes,
			content_rowid=rowid
		);`,
		`CREATE TRIGGER IF NOT EXISTS local_memory_episodes_ai AFTER INSERT ON local_memory_episodes BEGIN
			INSERT INTO local_memory_episodes_fts(rowid, summary, trigger, outcome, tags) VALUES (new.rowid, new.summary, new.trigger, new.outcome, new.tags_json);
		END;`,
		`CREATE TRIGGER IF NOT EXISTS local_memory_episodes_ad AFTER DELETE ON local_memory_episodes BEGIN
			INSERT INTO local_memory_episodes_fts(local_memory_episodes_fts, rowid, summary, trigger, outcome, tags) VALUES ('delete', old.rowid, old.summary, old.trigger, old.outcome, old.tags_json);
		END;`,
		`CREATE TRIGGER IF NOT EXISTS local_memory_episodes_au AFTER UPDATE ON local_memory_episodes BEGIN
			INSERT INTO local_memory_episodes_fts(local_memory_episodes_fts, rowid, summary, trigger, outcome, tags) VALUES ('delete', old.rowid, old.summary, old.trigger, old.outcome, old.tags_json);
			INSERT INTO local_memory_episodes_fts(rowid, summary, trigger, outcome, tags) VALUES (new.rowid, new.summary, new.trigger, new.outcome, new.tags_json);
		END;`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS local_memory_digests_fts USING fts5(
			summary,
			body,
			kind,
			tags,
			content=local_memory_digests,
			content_rowid=rowid
		);`,
		`CREATE TRIGGER IF NOT EXISTS local_memory_digests_ai AFTER INSERT ON local_memory_digests BEGIN
			INSERT INTO local_memory_digests_fts(rowid, summary, body, kind, tags) VALUES (new.rowid, new.summary, new.body, new.kind, new.tags_json);
		END;`,
		`CREATE TRIGGER IF NOT EXISTS local_memory_digests_ad AFTER DELETE ON local_memory_digests BEGIN
			INSERT INTO local_memory_digests_fts(local_memory_digests_fts, rowid, summary, body, kind, tags) VALUES ('delete', old.rowid, old.summary, old.body, old.kind, old.tags_json);
		END;`,
		`CREATE TRIGGER IF NOT EXISTS local_memory_digests_au AFTER UPDATE ON local_memory_digests BEGIN
			INSERT INTO local_memory_digests_fts(local_memory_digests_fts, rowid, summary, body, kind, tags) VALUES ('delete', old.rowid, old.summary, old.body, old.kind, old.tags_json);
			INSERT INTO local_memory_digests_fts(rowid, summary, body, kind, tags) VALUES (new.rowid, new.summary, new.body, new.kind, new.tags_json);
		END;`,
		`CREATE TABLE IF NOT EXISTS local_memory_promotions (
			candidate_id TEXT PRIMARY KEY,
			created_at TEXT,
			node_type TEXT NOT NULL,
			memory_type TEXT NOT NULL,
			source_id TEXT,
			title TEXT,
			body TEXT,
			summary TEXT,
			task_id TEXT,
			session_id TEXT,
			tension_id TEXT,
			proto_cluster_id TEXT,
			artifact_refs_json TEXT NOT NULL,
			doc_keys_json TEXT NOT NULL,
			constraint_refs_json TEXT NOT NULL,
			memory_id TEXT,
			claim_id TEXT,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			last_attempt_at TEXT,
			promoted_at TEXT,
			last_error TEXT,
			status TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_local_memory_promotions_status ON local_memory_promotions(status, created_at);`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO local_memory_meta(key, value) VALUES('schema_version', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;`, fmt.Sprintf("%d", localMemorySQLiteSchemaVersion)); err != nil {
		return err
	}
	return nil
}

func (s *LocalMemoryStore) maybeImportLegacyJSON() error {
	if s == nil || s.db == nil {
		return nil
	}
	empty, err := sqliteLocalMemoryEmpty(s.db)
	if err != nil || !empty {
		return err
	}
	legacy, ok, err := readLegacyLocalMemoryState(s.legacyPath, s.state.WorkspaceID, s.state.AgentID)
	if err != nil || !ok {
		return err
	}
	if err := writeWholeLocalMemoryState(s.db, legacy); err != nil {
		return err
	}
	backup := s.legacyPath + ".migrated-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	_ = os.Rename(s.legacyPath, backup)
	return nil
}

func sqliteLocalMemoryEmpty(db *sql.DB) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var episodes int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM local_memory_episodes;`).Scan(&episodes); err != nil {
		return false, err
	}
	var digests int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM local_memory_digests;`).Scan(&digests); err != nil {
		return false, err
	}
	return episodes == 0 && digests == 0, nil
}

func readLegacyLocalMemoryState(path, workspaceID, agentID string) (LocalMemoryState, bool, error) {
	if strings.TrimSpace(path) == "" {
		return LocalMemoryState{}, false, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return LocalMemoryState{}, false, nil
		}
		return LocalMemoryState{}, false, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return LocalMemoryState{}, false, nil
	}
	var state LocalMemoryState
	if err := json.Unmarshal(data, &state); err != nil {
		if quarantineErr := quarantineCorruptLocalMemoryFile(path, data); quarantineErr != nil {
			return LocalMemoryState{}, false, fmt.Errorf("decode legacy local memory state: %w", err)
		}
		return LocalMemoryState{}, false, nil
	}
	if state.Version == 0 {
		state.Version = localMemoryStateVersion
	}
	if state.WorkspaceID == "" {
		state.WorkspaceID = strings.TrimSpace(workspaceID)
	}
	if state.AgentID == "" {
		state.AgentID = strings.TrimSpace(agentID)
	}
	if state.Episodes == nil {
		state.Episodes = []LocalMemoryEpisodeRecord{}
	}
	if state.Digests == nil {
		state.Digests = []LocalMemoryDigestRecord{}
	}
	return cloneLocalMemoryState(state), true, nil
}

func loadLocalMemoryStateFromSQLite(db *sql.DB, workspaceID, agentID string) (LocalMemoryState, error) {
	state := LocalMemoryState{
		Version:     localMemoryStateVersion,
		WorkspaceID: strings.TrimSpace(workspaceID),
		AgentID:     strings.TrimSpace(agentID),
		Episodes:    []LocalMemoryEpisodeRecord{},
		Digests:     []LocalMemoryDigestRecord{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `SELECT episode_id, task_id, session_id, tension_id, proto_cluster_id, run_id, trigger, outcome, summary, digest_refs_json, doc_keys_json, artifact_refs_json, constraint_refs_json, segment_refs_json, tags_json, meta_json, created_at, superseded_at, invalidation_reasons_json FROM local_memory_episodes ORDER BY created_at, episode_id;`)
	if err != nil {
		return LocalMemoryState{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var record LocalMemoryEpisodeRecord
		var digestRefsJSON, docKeysJSON, artifactRefsJSON, constraintRefsJSON, segmentRefsJSON, tagsJSON, metaJSON, reasonsJSON string
		if err := rows.Scan(
			&record.EpisodeID,
			&record.Scope.TaskID,
			&record.Scope.SessionID,
			&record.Scope.TensionID,
			&record.Scope.ProtoClusterID,
			&record.RunID,
			&record.Trigger,
			&record.Outcome,
			&record.Summary,
			&digestRefsJSON,
			&docKeysJSON,
			&artifactRefsJSON,
			&constraintRefsJSON,
			&segmentRefsJSON,
			&tagsJSON,
			&metaJSON,
			&record.CreatedAt,
			&record.SupersededAt,
			&reasonsJSON,
		); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(digestRefsJSON, &record.DigestRefs); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(docKeysJSON, &record.DocKeys); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(artifactRefsJSON, &record.ArtifactRefs); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(constraintRefsJSON, &record.ConstraintRefs); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(segmentRefsJSON, &record.SegmentRefs); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(tagsJSON, &record.Tags); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(metaJSON, &record.Meta); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(reasonsJSON, &record.InvalidationReasons); err != nil {
			return LocalMemoryState{}, err
		}
		state.Episodes = append(state.Episodes, normalizeLocalMemoryEpisodeRecord(record, time.Time{}))
	}
	if err := rows.Err(); err != nil {
		return LocalMemoryState{}, err
	}

	rows, err = db.QueryContext(ctx, `SELECT digest_id, tier, kind, task_id, session_id, tension_id, proto_cluster_id, visibility, summary, body, source_episode_id, episode_digest_json, doc_keys_json, artifact_refs_json, constraint_refs_json, segment_refs_json, guards_json, tags_json, meta_json, created_at, updated_at, expires_at, last_accessed_at, stale, invalidated_at, invalidation_reasons_json FROM local_memory_digests ORDER BY updated_at, created_at, digest_id;`)
	if err != nil {
		return LocalMemoryState{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var record LocalMemoryDigestRecord
		var episodeDigestJSON, docKeysJSON, artifactRefsJSON, constraintRefsJSON, segmentRefsJSON, guardsJSON, tagsJSON, metaJSON, reasonsJSON string
		var stale int
		if err := rows.Scan(
			&record.DigestID,
			&record.Tier,
			&record.Kind,
			&record.Scope.TaskID,
			&record.Scope.SessionID,
			&record.Scope.TensionID,
			&record.Scope.ProtoClusterID,
			&record.Visibility,
			&record.Summary,
			&record.Body,
			&record.SourceEpisodeID,
			&episodeDigestJSON,
			&docKeysJSON,
			&artifactRefsJSON,
			&constraintRefsJSON,
			&segmentRefsJSON,
			&guardsJSON,
			&tagsJSON,
			&metaJSON,
			&record.CreatedAt,
			&record.UpdatedAt,
			&record.ExpiresAt,
			&record.LastAccessedAt,
			&stale,
			&record.InvalidatedAt,
			&reasonsJSON,
		); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(episodeDigestJSON, &record.EpisodeDigest); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(docKeysJSON, &record.DocKeys); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(artifactRefsJSON, &record.ArtifactRefs); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(constraintRefsJSON, &record.ConstraintRefs); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(segmentRefsJSON, &record.SegmentRefs); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(guardsJSON, &record.Guards); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(tagsJSON, &record.Tags); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(metaJSON, &record.Meta); err != nil {
			return LocalMemoryState{}, err
		}
		if err := decodeSQLiteJSON(reasonsJSON, &record.InvalidationReasons); err != nil {
			return LocalMemoryState{}, err
		}
		record.Stale = stale != 0
		state.Digests = append(state.Digests, normalizeLocalMemoryDigestRecord(record, time.Time{}))
	}
	if err := rows.Err(); err != nil {
		return LocalMemoryState{}, err
	}

	_ = db.QueryRowContext(ctx, `SELECT value FROM local_memory_meta WHERE key='updated_at';`).Scan(&state.UpdatedAt)
	return cloneLocalMemoryState(state), nil
}

func selectLocalMemoryEpisodeIDsByScopeAnchors(db *sql.DB, scope LocalMemoryScope) ([]string, error) {
	return selectLocalMemoryRecordIDsByScopeAnchors(
		db,
		"local_memory_episodes",
		"episode_id",
		"created_at, episode_id",
		scope,
	)
}

func selectLocalMemoryDigestIDsByScopeAnchors(db *sql.DB, scope LocalMemoryScope) ([]string, error) {
	return selectLocalMemoryRecordIDsByScopeAnchors(
		db,
		"local_memory_digests",
		"digest_id",
		"updated_at, created_at, digest_id",
		scope,
	)
}

func selectLocalMemoryRecordIDsByScopeAnchors(db *sql.DB, tableName, idColumn, orderBy string, scope LocalMemoryScope) ([]string, error) {
	if db == nil {
		return nil, nil
	}
	clauses, args := buildLocalMemoryScopeAnchorClauses(scope)
	if len(clauses) == 0 {
		return nil, nil
	}

	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE %s ORDER BY %s;`,
		idColumn,
		tableName,
		strings.Join(clauses, " OR "),
		orderBy,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0, 16)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			ids = append(ids, trimmed)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return uniqueTrimmedMemoryStrings(ids), nil
}

type localMemoryScoredCandidate struct {
	ID         string
	ScopeScore int
	RefScore   int
}

func selectScoredLocalMemoryEpisodeIDs(ctx context.Context, db *sql.DB, query localMemoryPacketQueryNormalized) ([]localMemoryScoredCandidate, error) {
	if db == nil || !query.hasSelectors() {
		return nil, nil
	}
	// episodes rank by scope, ref, then created_at
	return selectScoredLocalMemoryRecordIDs(ctx, db, "local_memory_episodes", "episode_id", "local_memory_episode_refs", "created_at", query)
}

func selectScoredLocalMemoryDigestIDs(ctx context.Context, db *sql.DB, query localMemoryPacketQueryNormalized) ([]localMemoryScoredCandidate, error) {
	if db == nil || !query.hasSelectors() {
		return nil, nil
	}
	// digests rank by scope, ref, then updated_at
	return selectScoredLocalMemoryRecordIDs(ctx, db, "local_memory_digests", "digest_id", "local_memory_digest_refs", "updated_at", query)
}

func selectScoredLocalMemoryRecordIDs(ctx context.Context, db *sql.DB, tableName, idColumn, refsTableName, tieBreakColumn string, query localMemoryPacketQueryNormalized) ([]localMemoryScoredCandidate, error) {
	scopeExprs := make([]string, 0, 4)
	args := make([]any, 0, 16)
	addScope := func(col, val string) {
		val = strings.TrimSpace(val)
		if val != "" {
			scopeExprs = append(scopeExprs, fmt.Sprintf("CASE WHEN e.%s = ? THEN 1 ELSE 0 END", col))
			args = append(args, val)
		}
	}
	addScope("task_id", query.scope.TaskID)
	addScope("session_id", query.scope.SessionID)
	addScope("tension_id", query.scope.TensionID)
	addScope("proto_cluster_id", query.scope.ProtoClusterID)

	scopeScoreSQL := "0"
	if len(scopeExprs) > 0 {
		scopeScoreSQL = "(" + strings.Join(scopeExprs, " + ") + ")"
	}

	conflictExprs := make([]string, 0, 4)
	addConflict := func(col, val string) {
		val = strings.TrimSpace(val)
		if val != "" {
			conflictExprs = append(conflictExprs, fmt.Sprintf("CASE WHEN e.%s != ? AND e.%s != '' THEN 1 ELSE 0 END", col, col))
			args = append(args, val)
		}
	}
	addConflict("task_id", query.scope.TaskID)
	addConflict("session_id", query.scope.SessionID)
	addConflict("tension_id", query.scope.TensionID)
	addConflict("proto_cluster_id", query.scope.ProtoClusterID)

	conflictSQL := "0"
	if len(conflictExprs) > 0 {
		conflictSQL = "(" + strings.Join(conflictExprs, " + ") + ")"
	}

	refClauses, refArgs := buildLocalMemoryRefSelectorClauses(query)
	args = append(args, refArgs...)
	refScoreSQL := "0"
	if len(refClauses) > 0 {
		refScoreSQL = fmt.Sprintf("(SELECT count(*) FROM %s r WHERE r.%s = e.%s AND (%s))", refsTableName, idColumn, idColumn, strings.Join(refClauses, " OR "))
	}

	limit := query.recentLimit * 2
	if limit < 16 {
		limit = 16
	}

	sqlQuery := fmt.Sprintf(`
		SELECT
			e.%s,
			%s AS scope_score,
			%s AS ref_score
		FROM %s e
		WHERE (%s > 0 OR %s > 0) AND %s = 0
		ORDER BY scope_score DESC, ref_score DESC, e.%s DESC
		LIMIT ?;
	`, idColumn, scopeScoreSQL, refScoreSQL, tableName, scopeScoreSQL, refScoreSQL, conflictSQL, tieBreakColumn)
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []localMemoryScoredCandidate
	for rows.Next() {
		var cand localMemoryScoredCandidate
		if err := rows.Scan(&cand.ID, &cand.ScopeScore, &cand.RefScore); err != nil {
			return nil, err
		}
		if cand.ID = strings.TrimSpace(cand.ID); cand.ID != "" {
			candidates = append(candidates, cand)
		}
	}
	return candidates, rows.Err()
}

func selectLocalMemoryEpisodeIDsByRefs(db *sql.DB, query localMemoryPacketQueryNormalized) ([]string, error) {
	return selectLocalMemoryRecordIDsByRefs(
		db,
		"local_memory_episode_refs",
		"episode_id",
		query,
	)
}

func selectLocalMemoryDigestIDsByRefs(db *sql.DB, query localMemoryPacketQueryNormalized) ([]string, error) {
	return selectLocalMemoryRecordIDsByRefs(
		db,
		"local_memory_digest_refs",
		"digest_id",
		query,
	)
}

func selectLocalMemoryRecordIDsByRefs(db *sql.DB, tableName, idColumn string, query localMemoryPacketQueryNormalized) ([]string, error) {
	if db == nil {
		return nil, nil
	}
	clauses, args := buildLocalMemoryRefSelectorClauses(query)
	if len(clauses) == 0 {
		return nil, nil
	}

	sqlQuery := fmt.Sprintf(
		`SELECT DISTINCT %s FROM %s WHERE %s ORDER BY %s;`,
		idColumn,
		tableName,
		strings.Join(clauses, " OR "),
		idColumn,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0, 16)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			ids = append(ids, trimmed)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return uniqueTrimmedMemoryStrings(ids), nil
}

func buildLocalMemoryScopeAnchorClauses(scope LocalMemoryScope) ([]string, []any) {
	clauses := make([]string, 0, 4)
	args := make([]any, 0, 4)
	add := func(column, value string) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return
		}
		clauses = append(clauses, column+` = ?`)
		args = append(args, trimmed)
	}
	add("task_id", scope.TaskID)
	add("session_id", scope.SessionID)
	add("tension_id", scope.TensionID)
	add("proto_cluster_id", scope.ProtoClusterID)
	return clauses, args
}

func buildLocalMemoryRefSelectorClauses(query localMemoryPacketQueryNormalized) ([]string, []any) {
	clauses := make([]string, 0, 4)
	args := make([]any, 0, len(query.docKeys)+len(query.artifactRefs)+len(query.constraintRefs)+len(query.segmentRefs)+4)
	add := func(refType string, selector map[string]struct{}) {
		values := make([]string, 0, len(selector))
		for value := range selector {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				values = append(values, trimmed)
			}
		}
		if len(values) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(values)), ",")
		clauses = append(clauses, fmt.Sprintf("(ref_type = ? AND ref_value IN (%s))", placeholders))
		args = append(args, refType)
		for _, value := range values {
			args = append(args, value)
		}
	}
	add("doc_key", query.docKeys)
	add("artifact_ref", query.artifactRefs)
	add("constraint_ref", query.constraintRefs)
	add("segment_ref", query.segmentRefs)
	return clauses, args
}

func (s *LocalMemoryStore) syncSQLiteLocked() error {
	if s == nil || s.db == nil {
		return nil
	}
	return writeWholeLocalMemoryState(s.db, s.state)
}

func updateLocalMemoryDigestAccesses(db *sql.DB, digestIDs []string, accessedAt string) error {
	if db == nil {
		return nil
	}
	digestIDs = uniqueTrimmedMemoryStrings(digestIDs)
	if len(digestIDs) == 0 {
		return nil
	}
	accessedAt = strings.TrimSpace(accessedAt)
	if accessedAt == "" {
		accessedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	stmt, err := tx.PrepareContext(ctx, `UPDATE local_memory_digests SET last_accessed_at = ?, updated_at = CASE WHEN COALESCE(updated_at, '') = '' THEN ? ELSE updated_at END WHERE digest_id = ?;`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, digestID := range digestIDs {
		if _, err := stmt.ExecContext(ctx, accessedAt, accessedAt, digestID); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO local_memory_meta(key, value) VALUES('updated_at', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;`, accessedAt); err != nil {
		return err
	}
	return tx.Commit()
}

func writeWholeLocalMemoryState(db *sql.DB, state LocalMemoryState) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, `DELETE FROM local_memory_episodes;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM local_memory_digests;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM local_memory_episode_refs;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM local_memory_digest_refs;`); err != nil {
		return err
	}

	episodeStmt, err := tx.PrepareContext(ctx, `INSERT INTO local_memory_episodes(episode_id, task_id, session_id, tension_id, proto_cluster_id, run_id, trigger, outcome, summary, digest_refs_json, doc_keys_json, artifact_refs_json, constraint_refs_json, segment_refs_json, tags_json, meta_json, created_at, superseded_at, invalidation_reasons_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return err
	}
	defer episodeStmt.Close()
	episodeRefStmt, err := tx.PrepareContext(ctx, `INSERT INTO local_memory_episode_refs(episode_id, ref_type, ref_value) VALUES(?, ?, ?);`)
	if err != nil {
		return err
	}
	defer episodeRefStmt.Close()
	for _, episode := range state.Episodes {
		if _, err := episodeStmt.ExecContext(ctx,
			episode.EpisodeID,
			episode.Scope.TaskID,
			episode.Scope.SessionID,
			episode.Scope.TensionID,
			episode.Scope.ProtoClusterID,
			episode.RunID,
			episode.Trigger,
			episode.Outcome,
			episode.Summary,
			encodeSQLiteJSON(episode.DigestRefs),
			encodeSQLiteJSON(episode.DocKeys),
			encodeSQLiteJSON(episode.ArtifactRefs),
			encodeSQLiteJSON(episode.ConstraintRefs),
			encodeSQLiteJSON(episode.SegmentRefs),
			encodeSQLiteJSON(episode.Tags),
			encodeSQLiteJSON(episode.Meta),
			episode.CreatedAt,
			episode.SupersededAt,
			encodeSQLiteJSON(episode.InvalidationReasons),
		); err != nil {
			return err
		}
		for _, ref := range localMemorySQLiteRefs(episode.DocKeys, episode.ArtifactRefs, episode.ConstraintRefs, episode.SegmentRefs) {
			if _, err := episodeRefStmt.ExecContext(ctx, episode.EpisodeID, ref.refType, ref.refValue); err != nil {
				return err
			}
		}
	}

	digestStmt, err := tx.PrepareContext(ctx, `INSERT INTO local_memory_digests(digest_id, tier, kind, task_id, session_id, tension_id, proto_cluster_id, visibility, summary, body, source_episode_id, episode_digest_json, doc_keys_json, artifact_refs_json, constraint_refs_json, segment_refs_json, guards_json, tags_json, meta_json, created_at, updated_at, expires_at, last_accessed_at, stale, invalidated_at, invalidation_reasons_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return err
	}
	defer digestStmt.Close()
	digestRefStmt, err := tx.PrepareContext(ctx, `INSERT INTO local_memory_digest_refs(digest_id, ref_type, ref_value) VALUES(?, ?, ?);`)
	if err != nil {
		return err
	}
	defer digestRefStmt.Close()
	for _, digest := range state.Digests {
		stale := 0
		if digest.Stale {
			stale = 1
		}
		if _, err := digestStmt.ExecContext(ctx,
			digest.DigestID,
			digest.Tier,
			digest.Kind,
			digest.Scope.TaskID,
			digest.Scope.SessionID,
			digest.Scope.TensionID,
			digest.Scope.ProtoClusterID,
			digest.Visibility,
			digest.Summary,
			digest.Body,
			digest.SourceEpisodeID,
			encodeSQLiteJSON(digest.EpisodeDigest),
			encodeSQLiteJSON(digest.DocKeys),
			encodeSQLiteJSON(digest.ArtifactRefs),
			encodeSQLiteJSON(digest.ConstraintRefs),
			encodeSQLiteJSON(digest.SegmentRefs),
			encodeSQLiteJSON(digest.Guards),
			encodeSQLiteJSON(digest.Tags),
			encodeSQLiteJSON(digest.Meta),
			digest.CreatedAt,
			digest.UpdatedAt,
			digest.ExpiresAt,
			digest.LastAccessedAt,
			stale,
			digest.InvalidatedAt,
			encodeSQLiteJSON(digest.InvalidationReasons),
		); err != nil {
			return err
		}
		for _, ref := range localMemorySQLiteRefs(digest.DocKeys, digest.ArtifactRefs, digest.ConstraintRefs, digest.SegmentRefs) {
			if _, err := digestRefStmt.ExecContext(ctx, digest.DigestID, ref.refType, ref.refValue); err != nil {
				return err
			}
		}
	}

	metaStmt, err := tx.PrepareContext(ctx, `INSERT INTO local_memory_meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;`)
	if err != nil {
		return err
	}
	defer metaStmt.Close()
	meta := map[string]string{
		"schema_version": fmt.Sprintf("%d", localMemorySQLiteSchemaVersion),
		"state_version":  fmt.Sprintf("%d", state.Version),
		"workspace_id":   strings.TrimSpace(state.WorkspaceID),
		"agent_id":       strings.TrimSpace(state.AgentID),
		"updated_at":     strings.TrimSpace(state.UpdatedAt),
	}
	for key, value := range meta {
		if _, err := metaStmt.ExecContext(ctx, key, value); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func encodeSQLiteJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) == 0 {
		return "null"
	}
	return string(raw)
}

func decodeSQLiteJSON(raw string, target any) error {
	value := strings.TrimSpace(raw)
	if value == "" {
		value = "null"
	}
	return json.Unmarshal([]byte(value), target)
}

func localMemorySQLiteRefs(docKeys, artifactRefs, constraintRefs, segmentRefs []string) []localMemorySQLiteRefRecord {
	out := make([]localMemorySQLiteRefRecord, 0, len(docKeys)+len(artifactRefs)+len(constraintRefs)+len(segmentRefs))
	add := func(refType string, values []string) {
		for _, value := range uniqueTrimmedMemoryStrings(values) {
			if value != "" {
				out = append(out, localMemorySQLiteRefRecord{
					refType:  refType,
					refValue: value,
				})
			}
		}
	}
	add("doc_key", docKeys)
	add("artifact_ref", artifactRefs)
	add("constraint_ref", constraintRefs)
	add("segment_ref", segmentRefs)
	return out
}

func SearchLocalMemoryFTS(ctx context.Context, db *sql.DB, query string, limit int) ([]string, []string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil, nil
	}
	if limit <= 0 {
		limit = 50
	}

	// Ensure query uses FTS quoting to avoid syntax errors with partial inputs
	safeQuery := `"` + strings.ReplaceAll(query, `"`, `""`) + `"`

	var digestIDs []string
	rowsD, err := db.QueryContext(ctx, `
		SELECT d.digest_id
		FROM local_memory_digests_fts f
		JOIN local_memory_digests d ON f.rowid = d.rowid
		WHERE f.local_memory_digests_fts MATCH ?
		ORDER BY f.rank LIMIT ?
	`, safeQuery, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("search digests fts: %w", err)
	}
	defer rowsD.Close()
	for rowsD.Next() {
		var id string
		if err := rowsD.Scan(&id); err == nil && id != "" {
			digestIDs = append(digestIDs, id)
		}
	}

	var episodeIDs []string
	rowsE, err := db.QueryContext(ctx, `
		SELECT e.episode_id
		FROM local_memory_episodes_fts f
		JOIN local_memory_episodes e ON f.rowid = e.rowid
		WHERE f.local_memory_episodes_fts MATCH ?
		ORDER BY f.rank LIMIT ?
	`, safeQuery, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("search episodes fts: %w", err)
	}
	defer rowsE.Close()
	for rowsE.Next() {
		var id string
		if err := rowsE.Scan(&id); err == nil && id != "" {
			episodeIDs = append(episodeIDs, id)
		}
	}

	return digestIDs, episodeIDs, nil
}

func upsertLocalMemoryPromotion(ctx context.Context, db *sql.DB, candidate LocalPromotionCandidate, status string) error {
	candidate = normalizeLocalMemoryPromotionCandidate(candidate)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO local_memory_promotions(
			candidate_id, created_at, node_type, memory_type, source_id,
			title, body, summary, task_id, session_id,
			tension_id, proto_cluster_id, artifact_refs_json, doc_keys_json,
			constraint_refs_json, memory_id, claim_id, attempt_count,
			last_attempt_at, promoted_at, last_error, status
		) VALUES (
			?, ?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?
		) ON CONFLICT(candidate_id) DO UPDATE SET
			memory_type = excluded.memory_type,
			title = excluded.title,
			body = excluded.body,
			summary = excluded.summary,
			artifact_refs_json = excluded.artifact_refs_json,
			doc_keys_json = excluded.doc_keys_json,
			constraint_refs_json = excluded.constraint_refs_json,
			memory_id = CASE WHEN excluded.memory_id != '' THEN excluded.memory_id ELSE local_memory_promotions.memory_id END,
			claim_id = CASE WHEN excluded.claim_id != '' THEN excluded.claim_id ELSE local_memory_promotions.claim_id END,
			attempt_count = excluded.attempt_count,
			last_attempt_at = excluded.last_attempt_at,
			promoted_at = excluded.promoted_at,
			last_error = excluded.last_error,
			status = excluded.status;
	`)
	if err != nil {
		return fmt.Errorf("prepare upsert promotion stmt: %w", err)
	}
	defer stmt.Close()

	if candidate.CreatedAt == "" {
		candidate.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	status = localMemoryWriteStateForPersistence(firstNonEmpty(status, candidate.WriteState))

	_, err = stmt.ExecContext(ctx,
		candidate.CandidateID,
		candidate.CreatedAt,
		string(candidate.NodeType),
		candidate.MemoryType,
		candidate.SourceID,
		firstNonEmpty(candidate.Title, ""),
		firstNonEmpty(candidate.Body, ""),
		firstNonEmpty(candidate.Summary, ""),
		candidate.TaskID,
		candidate.SessionID,
		candidate.TensionID,
		candidate.ProtoClusterID,
		encodeSQLiteJSON(uniqueTrimmedMemoryStrings(candidate.ArtifactRefs)),
		encodeSQLiteJSON(uniqueTrimmedMemoryStrings(candidate.DocKeys)),
		encodeSQLiteJSON(uniqueTrimmedMemoryStrings(candidate.ConstraintRefs)),
		firstNonEmpty(candidate.MemoryID, ""),
		firstNonEmpty(candidate.ClaimID, ""),
		candidate.AttemptCount,
		firstNonEmpty(candidate.LastAttemptAt, ""),
		firstNonEmpty(candidate.PromotedAt, ""),
		firstNonEmpty(candidate.LastError, ""),
		status,
	)
	if err != nil {
		return err
	}
	if err := pruneLocalMemoryPromotionsWithLimits(ctx, tx, localMemoryPromotionPendingRetentionLimit, localMemoryPromotionHistoryRetentionLimit); err != nil {
		return err
	}
	return tx.Commit()
}

func selectPendingLocalMemoryPromotions(ctx context.Context, db *sql.DB, limit int) ([]LocalPromotionCandidate, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := db.QueryContext(ctx, `
		SELECT
			candidate_id, created_at, node_type, memory_type, source_id,
			title, body, summary, task_id, session_id,
			tension_id, proto_cluster_id, artifact_refs_json, doc_keys_json,
			constraint_refs_json, memory_id, claim_id, attempt_count,
			last_attempt_at, promoted_at, last_error, status
		FROM local_memory_promotions
		WHERE status IN ('pending', 'draft', 'candidate')
		ORDER BY created_at ASC, candidate_id ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending promotions: %w", err)
	}
	defer rows.Close()

	var out []LocalPromotionCandidate
	for rows.Next() {
		var c LocalPromotionCandidate
		var nt string
		var artifactRefs, docKeys, constraintRefs, status string
		if err := rows.Scan(
			&c.CandidateID, &c.CreatedAt, &nt, &c.MemoryType, &c.SourceID,
			&c.Title, &c.Body, &c.Summary, &c.TaskID, &c.SessionID,
			&c.TensionID, &c.ProtoClusterID, &artifactRefs, &docKeys,
			&constraintRefs, &c.MemoryID, &c.ClaimID, &c.AttemptCount,
			&c.LastAttemptAt, &c.PromotedAt, &c.LastError, &status,
		); err != nil {
			return nil, err
		}
		c.NodeType = localMemoryNodeType(nt)
		_ = decodeSQLiteJSON(artifactRefs, &c.ArtifactRefs)
		_ = decodeSQLiteJSON(docKeys, &c.DocKeys)
		_ = decodeSQLiteJSON(constraintRefs, &c.ConstraintRefs)
		c.WriteState = normalizeLocalMemoryWriteState(status)
		c = normalizeLocalMemoryPromotionCandidate(c)
		out = append(out, c)
	}
	return out, nil
}

func updateLocalMemoryPromotionStatus(ctx context.Context, db *sql.DB, candidateID, memoryID, claimID, status, errorMessage string, incrementAttempt bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	safeTime := time.Now().UTC().Format(time.RFC3339Nano)
	status = localMemoryWriteStateForPersistence(status)
	promotedAt := ""
	if status == localMemoryWriteStatePromoted {
		promotedAt = safeTime
	}
	inc := 0
	if incrementAttempt {
		inc = 1
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE local_memory_promotions SET
			status = ?,
			memory_id = CASE WHEN ? != '' THEN ? ELSE memory_id END,
			claim_id = CASE WHEN ? != '' THEN ? ELSE claim_id END,
			last_error = ?,
			attempt_count = attempt_count + ?,
			last_attempt_at = ?,
			promoted_at = CASE WHEN ? != '' THEN ? ELSE promoted_at END
		WHERE candidate_id = ?
		`, status, memoryID, memoryID, claimID, claimID, errorMessage, inc, safeTime, promotedAt, promotedAt, candidateID)
	if err != nil {
		return err
	}
	if err := pruneLocalMemoryPromotionsWithLimits(ctx, tx, localMemoryPromotionPendingRetentionLimit, localMemoryPromotionHistoryRetentionLimit); err != nil {
		return err
	}
	return tx.Commit()
}

type localMemoryPromotionRetentionExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func pruneLocalMemoryPromotions(ctx context.Context, db *sql.DB) error {
	return pruneLocalMemoryPromotionsWithLimits(ctx, db, localMemoryPromotionPendingRetentionLimit, localMemoryPromotionHistoryRetentionLimit)
}

func pruneLocalMemoryPromotionsWithLimits(ctx context.Context, exec localMemoryPromotionRetentionExecutor, pendingLimit, historyLimit int) error {
	if exec == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if pendingLimit < 0 {
		pendingLimit = 0
	}
	if historyLimit < 0 {
		historyLimit = 0
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := pruneLocalMemoryPromotionBucket(ctx, exec, `status IN ('pending', 'draft', 'candidate')`, pendingLimit, `created_at ASC, candidate_id ASC`); err != nil {
		return err
	}
	if err := pruneLocalMemoryPromotionBucket(ctx, exec, `status NOT IN ('pending', 'draft', 'candidate')`, historyLimit, `COALESCE(NULLIF(promoted_at, ''), NULLIF(last_attempt_at, ''), NULLIF(created_at, '')) ASC, created_at ASC, candidate_id ASC`); err != nil {
		return err
	}
	return nil
}

func pruneLocalMemoryPromotionBucket(ctx context.Context, exec localMemoryPromotionRetentionExecutor, whereClause string, limit int, orderBy string) error {
	query := `SELECT COUNT(*) FROM local_memory_promotions`
	if strings.TrimSpace(whereClause) != "" {
		query += ` WHERE ` + whereClause
	}
	var count int
	if err := exec.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return err
	}
	if count <= limit {
		return nil
	}
	excess := count - limit

	deleteQuery := `DELETE FROM local_memory_promotions WHERE candidate_id IN (
		SELECT candidate_id FROM local_memory_promotions`
	if strings.TrimSpace(whereClause) != "" {
		deleteQuery += ` WHERE ` + whereClause
	}
	deleteQuery += ` ORDER BY ` + orderBy + ` LIMIT ?)`
	_, err := exec.ExecContext(ctx, deleteQuery, excess)
	return err
}
