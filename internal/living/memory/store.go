package memory

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// EntryType represents the category of a memory entry.
type EntryType = string

const (
	TypeExperience   EntryType = "experience"
	TypeReflection   EntryType = "reflection"
	TypeProcedure    EntryType = "procedure"
	TypeEntity       EntryType = "entity"
	TypeError        EntryType = "error"
	TypeLesson       EntryType = "lesson"
	TypeDecision     EntryType = "decision"
	TypeIncident     EntryType = "incident"
	TypeUpdateDigest EntryType = "update_digest"
	TypeSummary      EntryType = "summary"
)

// MemoryEntry represents a single entry in the agent's associative memory.
type MemoryEntry struct {
	ID        int64
	Timestamp time.Time
	Type      string
	Source    string
	Topic     string
	Content   string
	TaskID    string
	Rank      float64 // Only populated on search results (BM25 score).
}

// SearchOpts configures a Search query.
type SearchOpts struct {
	TypeFilter string
	Limit      int
}

// RecentOpts configures a GetRecent query.
type RecentOpts struct {
	TypeFilter string
	Limit      int
	TaskID     string
}

// MemoryStore provides high-level operations on the agent memory database.
type MemoryStore struct {
	db *MemoryDB
}

// NewMemoryStore creates a MemoryStore backed by the given MemoryDB.
func NewMemoryStore(db *MemoryDB) *MemoryStore {
	return &MemoryStore{db: db}
}

// Save inserts a MemoryEntry and returns the new row ID.
// FTS5 synchronisation happens automatically via database triggers.
func (s *MemoryStore) Save(ctx context.Context, entry MemoryEntry) (int64, error) {
	storedType := normalizeStoredMemoryType(entry.Type)
	result, err := s.db.DB().ExecContext(ctx,
		`INSERT INTO memory_entries (type, source, topic, content, task_id)
		 VALUES (?, ?, ?, ?, ?)`,
		storedType, entry.Source, entry.Topic, entry.Content, entry.TaskID,
	)
	if err != nil {
		return 0, fmt.Errorf("memory: save: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("memory: last insert id: %w", err)
	}
	return id, nil
}

// Search performs a BM25-ranked FTS5 search. Results are ordered by relevance
// (most relevant first). An empty query returns an empty slice immediately.
func (s *MemoryStore) Search(ctx context.Context, query string, opts SearchOpts) ([]MemoryEntry, error) {
	if query == "" {
		return []MemoryEntry{}, nil
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 10
	}

	var rows *sql.Rows
	var err error
	typeFilter := normalizeStoredMemoryType(opts.TypeFilter)

	if typeFilter != "" {
		rows, err = s.db.DB().QueryContext(ctx,
			`SELECT m.id, m.timestamp, m.type, m.source, m.topic, m.content, m.task_id, rank
			 FROM memory_entries_fts
			 JOIN memory_entries m ON m.id = memory_entries_fts.rowid
			 WHERE memory_entries_fts MATCH ?
			   AND m.type = ?
			 ORDER BY rank
			 LIMIT ?`,
			query, typeFilter, limit,
		)
	} else {
		rows, err = s.db.DB().QueryContext(ctx,
			`SELECT m.id, m.timestamp, m.type, m.source, m.topic, m.content, m.task_id, rank
			 FROM memory_entries_fts
			 JOIN memory_entries m ON m.id = memory_entries_fts.rowid
			 WHERE memory_entries_fts MATCH ?
			 ORDER BY rank
			 LIMIT ?`,
			query, limit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("memory: search: %w", err)
	}
	defer rows.Close()

	return scanEntries(rows, true)
}

// GetRecent returns the most recent memory entries, ordered by timestamp DESC.
func (s *MemoryStore) GetRecent(ctx context.Context, opts RecentOpts) ([]MemoryEntry, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 20
	}

	var rows *sql.Rows
	var err error
	typeFilter := normalizeStoredMemoryType(opts.TypeFilter)

	switch {
	case typeFilter != "" && opts.TaskID != "":
		rows, err = s.db.DB().QueryContext(ctx,
			`SELECT id, timestamp, type, source, topic, content, task_id
			 FROM memory_entries
			 WHERE type = ? AND task_id = ?
			 ORDER BY timestamp DESC, id DESC
			 LIMIT ?`,
			typeFilter, opts.TaskID, limit,
		)
	case typeFilter != "":
		rows, err = s.db.DB().QueryContext(ctx,
			`SELECT id, timestamp, type, source, topic, content, task_id
			 FROM memory_entries
			 WHERE type = ?
			 ORDER BY timestamp DESC, id DESC
			 LIMIT ?`,
			typeFilter, limit,
		)
	case opts.TaskID != "":
		rows, err = s.db.DB().QueryContext(ctx,
			`SELECT id, timestamp, type, source, topic, content, task_id
			 FROM memory_entries
			 WHERE task_id = ?
			 ORDER BY timestamp DESC, id DESC
			 LIMIT ?`,
			opts.TaskID, limit,
		)
	default:
		rows, err = s.db.DB().QueryContext(ctx,
			`SELECT id, timestamp, type, source, topic, content, task_id
			 FROM memory_entries
			 ORDER BY timestamp DESC, id DESC
			 LIMIT ?`,
			limit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("memory: get recent: %w", err)
	}
	defer rows.Close()

	return scanEntries(rows, false)
}

// Delete removes a memory entry by ID. The FTS5 index is cleaned up
// automatically via database triggers.
func (s *MemoryStore) Delete(ctx context.Context, id int64) error {
	_, err := s.db.DB().ExecContext(ctx,
		`DELETE FROM memory_entries WHERE id = ?`, id,
	)
	if err != nil {
		return fmt.Errorf("memory: delete: %w", err)
	}
	return nil
}

// Count returns the total number of memory entries.
func (s *MemoryStore) Count(ctx context.Context) (int, error) {
	var count int
	err := s.db.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM memory_entries`,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("memory: count: %w", err)
	}
	return count, nil
}

// scanEntries reads rows into a slice of MemoryEntry.
// If withRank is true, it expects a rank column at the end.
func scanEntries(rows *sql.Rows, withRank bool) ([]MemoryEntry, error) {
	var entries []MemoryEntry
	for rows.Next() {
		var e MemoryEntry
		var ts string
		if withRank {
			if err := rows.Scan(&e.ID, &ts, &e.Type, &e.Source, &e.Topic, &e.Content, &e.TaskID, &e.Rank); err != nil {
				return nil, fmt.Errorf("memory: scan row: %w", err)
			}
		} else {
			if err := rows.Scan(&e.ID, &ts, &e.Type, &e.Source, &e.Topic, &e.Content, &e.TaskID); err != nil {
				return nil, fmt.Errorf("memory: scan row: %w", err)
			}
		}
		// Parse SQLite timestamp (strftime('%Y-%m-%dT%H:%M:%f','now')).
		t, err := time.Parse("2006-01-02T15:04:05.000", ts)
		if err != nil {
			// Fallback: try without fractional seconds.
			t, err = time.Parse("2006-01-02T15:04:05", ts)
			if err != nil {
				return nil, fmt.Errorf("memory: parse timestamp %q: %w", ts, err)
			}
		}
		e.Timestamp = t
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: rows iteration: %w", err)
	}
	if entries == nil {
		entries = []MemoryEntry{}
	}
	return entries, nil
}

func normalizeStoredMemoryType(raw string) string {
	switch normalized := strings.TrimSpace(strings.ToLower(raw)); normalized {
	case string(TypeLesson), string(TypeDecision), string(TypeUpdateDigest), string(TypeSummary):
		return string(TypeReflection)
	case string(TypeIncident):
		return string(TypeError)
	default:
		return normalized
	}
}
