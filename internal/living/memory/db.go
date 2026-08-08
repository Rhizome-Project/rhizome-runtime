package memory

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// MemoryDB wraps a SQLite database used for agent associative memory.
// This is separate from the main Rhizome store — each living agent
// instance has its own memory database.
type MemoryDB struct {
	db *sql.DB
}

// NewMemoryDB opens (or creates) a SQLite database at dbPath, enables
// WAL mode, and initialises the memory schema (tables, FTS5, indexes,
// triggers). Use ":memory:" for an in-memory database.
func NewMemoryDB(dbPath string) (*MemoryDB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("memory: open database: %w", err)
	}

	// Enable WAL mode for better concurrent read performance.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("memory: enable WAL: %w", err)
	}

	// Apply schema (idempotent — uses IF NOT EXISTS throughout).
	if _, err := db.Exec(createSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("memory: apply schema: %w", err)
	}

	return &MemoryDB{db: db}, nil
}

// DB returns the underlying *sql.DB for direct queries.
func (m *MemoryDB) DB() *sql.DB {
	return m.db
}

// Close closes the underlying database connection.
func (m *MemoryDB) Close() error {
	return m.db.Close()
}
