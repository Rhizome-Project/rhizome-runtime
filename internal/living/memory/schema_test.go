package memory

import (
	"testing"
)

func TestNewMemoryDB_CreatesSchema(t *testing.T) {
	mdb, err := NewMemoryDB(":memory:")
	if err != nil {
		t.Fatalf("NewMemoryDB failed: %v", err)
	}
	defer mdb.Close()

	// Verify memory_entries table exists by querying its columns.
	rows, err := mdb.DB().Query("PRAGMA table_info(memory_entries)")
	if err != nil {
		t.Fatalf("PRAGMA table_info failed: %v", err)
	}
	defer rows.Close()

	cols := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt *string
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		cols[name] = true
	}

	for _, want := range []string{"id", "timestamp", "type", "source", "topic", "content", "task_id"} {
		if !cols[want] {
			t.Errorf("missing column %q in memory_entries", want)
		}
	}

	// Verify FTS5 virtual table exists.
	var ftsName string
	err = mdb.DB().QueryRow(
		"SELECT name FROM sqlite_master WHERE type='table' AND name='memory_entries_fts'",
	).Scan(&ftsName)
	if err != nil {
		t.Fatalf("memory_entries_fts not found: %v", err)
	}
}

func TestMemoryDB_FTS5Works(t *testing.T) {
	mdb, err := NewMemoryDB(":memory:")
	if err != nil {
		t.Fatalf("NewMemoryDB failed: %v", err)
	}
	defer mdb.Close()

	// Insert a row.
	_, err = mdb.DB().Exec(
		"INSERT INTO memory_entries (type, source, topic, content) VALUES (?, ?, ?, ?)",
		"experience", "test", "golang testing", "wrote unit tests for the memory module",
	)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// BM25 search via FTS5.
	var rowID int
	var topic, content string
	err = mdb.DB().QueryRow(
		"SELECT rowid, topic, content FROM memory_entries_fts WHERE memory_entries_fts MATCH ? ORDER BY bm25(memory_entries_fts)",
		"memory module",
	).Scan(&rowID, &topic, &content)
	if err != nil {
		t.Fatalf("FTS5 query failed: %v", err)
	}
	if rowID != 1 {
		t.Errorf("expected rowid 1, got %d", rowID)
	}
	if topic != "golang testing" {
		t.Errorf("unexpected topic: %s", topic)
	}
}

func TestMemoryDB_TypeConstraint(t *testing.T) {
	mdb, err := NewMemoryDB(":memory:")
	if err != nil {
		t.Fatalf("NewMemoryDB failed: %v", err)
	}
	defer mdb.Close()

	// Valid types should succeed.
	for _, typ := range []string{"experience", "reflection", "procedure", "entity", "error"} {
		_, err := mdb.DB().Exec(
			"INSERT INTO memory_entries (type, content) VALUES (?, ?)",
			typ, "test content",
		)
		if err != nil {
			t.Errorf("valid type %q rejected: %v", typ, err)
		}
	}

	// Invalid type should fail.
	_, err = mdb.DB().Exec(
		"INSERT INTO memory_entries (type, content) VALUES (?, ?)",
		"invalid_type", "test content",
	)
	if err == nil {
		t.Error("expected error for invalid type, got nil")
	}
}

func TestMemoryDB_Indexes(t *testing.T) {
	mdb, err := NewMemoryDB(":memory:")
	if err != nil {
		t.Fatalf("NewMemoryDB failed: %v", err)
	}
	defer mdb.Close()

	rows, err := mdb.DB().Query("PRAGMA index_list(memory_entries)")
	if err != nil {
		t.Fatalf("PRAGMA index_list failed: %v", err)
	}
	defer rows.Close()

	indexes := make(map[string]bool)
	for rows.Next() {
		var seq int
		var name, origin string
		var unique, partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		indexes[name] = true
	}

	for _, want := range []string{"idx_memory_entries_type_timestamp", "idx_memory_entries_task_id"} {
		if !indexes[want] {
			t.Errorf("missing index %q", want)
		}
	}
}

func TestMemoryDB_FTSSyncOnDelete(t *testing.T) {
	mdb, err := NewMemoryDB(":memory:")
	if err != nil {
		t.Fatalf("NewMemoryDB failed: %v", err)
	}
	defer mdb.Close()

	// Insert and then delete.
	_, err = mdb.DB().Exec(
		"INSERT INTO memory_entries (type, source, topic, content) VALUES (?, ?, ?, ?)",
		"experience", "test", "unique_topic_xyz", "unique_content_abc",
	)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	_, err = mdb.DB().Exec("DELETE FROM memory_entries WHERE id = 1")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// FTS should no longer find the deleted row.
	var count int
	err = mdb.DB().QueryRow(
		"SELECT COUNT(*) FROM memory_entries_fts WHERE memory_entries_fts MATCH ?",
		"unique_content_abc",
	).Scan(&count)
	if err != nil {
		t.Fatalf("FTS5 count query failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 FTS results after delete, got %d", count)
	}
}

func TestMemoryDB_Idempotent(t *testing.T) {
	mdb, err := NewMemoryDB(":memory:")
	if err != nil {
		t.Fatalf("first NewMemoryDB failed: %v", err)
	}

	// Apply schema a second time on the same DB — should not error.
	_, err = mdb.DB().Exec(createSchema)
	if err != nil {
		t.Fatalf("re-applying schema failed: %v", err)
	}
	mdb.Close()
}
