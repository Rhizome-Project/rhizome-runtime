package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func newMigrationTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "rhizome-migration-test.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		_ = store.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// T-1: Verifies R-2, R-3 — both tables exist after migration
func TestMigration0018_TablesExist(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	tables := []string{"agent_sessions", "agent_session_messages"}
	for _, table := range tables {
		var name string
		err := db.QueryRow(
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?", table,
		).Scan(&name)
		if err != nil {
			t.Fatalf("table %q not found: %v", table, err)
		}
	}
}

// T-2: Verifies R-2 — insert and read back a session row
func TestMigration0018_InsertSession(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	_, err := db.Exec(`INSERT INTO agent_sessions
		(session_id, agent_id, workspace_id, task_id, status, iterations,
		 total_input_tokens, total_output_tokens, tool_calls, started_at)
		VALUES ('sess-1', 'agent-1', 'ws-1', 'task-1', 'RUNNING', 5, 100, 50, 3, '2025-01-01T00:00:00.000')`)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}

	var sessionID, agentID, status string
	var iterations, inputTokens int
	err = db.QueryRow("SELECT session_id, agent_id, status, iterations, total_input_tokens FROM agent_sessions WHERE session_id='sess-1'").
		Scan(&sessionID, &agentID, &status, &iterations, &inputTokens)
	if err != nil {
		t.Fatalf("read session: %v", err)
	}
	if sessionID != "sess-1" {
		t.Fatalf("session_id: expected %q, got %q", "sess-1", sessionID)
	}
	if agentID != "agent-1" {
		t.Fatalf("agent_id: expected %q, got %q", "agent-1", agentID)
	}
	if status != "RUNNING" {
		t.Fatalf("status: expected %q, got %q", "RUNNING", status)
	}
	if iterations != 5 {
		t.Fatalf("iterations: expected 5, got %d", iterations)
	}
	if inputTokens != 100 {
		t.Fatalf("total_input_tokens: expected 100, got %d", inputTokens)
	}
}

// T-3: Verifies R-3 — insert and read back a message row
func TestMigration0018_InsertMessage(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	// Insert parent session first (FK constraint)
	_, err := db.Exec(`INSERT INTO agent_sessions
		(session_id, agent_id, workspace_id, started_at)
		VALUES ('sess-msg', 'agent-1', 'ws-1', '2025-01-01T00:00:00.000')`)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}

	_, err = db.Exec(`INSERT INTO agent_session_messages
		(session_id, sequence, role, content_json, token_count)
		VALUES ('sess-msg', 0, 'user', '[{"type":"text","text":"hello"}]', 10)`)
	if err != nil {
		t.Fatalf("insert message: %v", err)
	}

	var sessionID, role, contentJSON string
	var sequence, tokenCount int
	err = db.QueryRow("SELECT session_id, sequence, role, content_json, token_count FROM agent_session_messages WHERE session_id='sess-msg'").
		Scan(&sessionID, &sequence, &role, &contentJSON, &tokenCount)
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	if role != "user" {
		t.Fatalf("role: expected %q, got %q", "user", role)
	}
	if sequence != 0 {
		t.Fatalf("sequence: expected 0, got %d", sequence)
	}
	if tokenCount != 10 {
		t.Fatalf("token_count: expected 10, got %d", tokenCount)
	}
}

// T-4: Verifies R-5 — unique constraint on (session_id, sequence)
func TestMigration0018_UniqueConstraint(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	_, err := db.Exec(`INSERT INTO agent_sessions
		(session_id, agent_id, workspace_id, started_at)
		VALUES ('sess-uc', 'agent-1', 'ws-1', '2025-01-01T00:00:00.000')`)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}

	_, err = db.Exec(`INSERT INTO agent_session_messages
		(session_id, sequence, role, content_json) VALUES ('sess-uc', 0, 'user', '[]')`)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}

	_, err = db.Exec(`INSERT INTO agent_session_messages
		(session_id, sequence, role, content_json) VALUES ('sess-uc', 0, 'assistant', '[]')`)
	if err == nil {
		t.Fatal("expected unique constraint violation, got nil error")
	}
}

// NT-1: Verifies R-2 — default values work (status defaults to RUNNING)
func TestMigration0018_DefaultValues(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	_, err := db.Exec(`INSERT INTO agent_sessions
		(session_id, agent_id, workspace_id, started_at)
		VALUES ('sess-def', 'agent-1', 'ws-1', '2025-01-01T00:00:00.000')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var status string
	var iterations, inputTokens, outputTokens, toolCalls int
	err = db.QueryRow(`SELECT status, iterations, total_input_tokens, total_output_tokens, tool_calls
		FROM agent_sessions WHERE session_id='sess-def'`).
		Scan(&status, &iterations, &inputTokens, &outputTokens, &toolCalls)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if status != "RUNNING" {
		t.Fatalf("default status: expected %q, got %q", "RUNNING", status)
	}
	if iterations != 0 {
		t.Fatalf("default iterations: expected 0, got %d", iterations)
	}
	if inputTokens != 0 || outputTokens != 0 || toolCalls != 0 {
		t.Fatal("default counters should be 0")
	}
}

// NT-2: Verifies R-4 — indexes exist
func TestMigration0018_IndexesExist(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	expectedIndexes := []string{
		"idx_agent_sessions_agent_id",
		"idx_agent_sessions_workspace_id",
		"idx_agent_sessions_task_id",
		"idx_agent_session_messages_session_id",
	}
	for _, idx := range expectedIndexes {
		var name string
		err := db.QueryRow(
			"SELECT name FROM sqlite_master WHERE type='index' AND name=?", idx,
		).Scan(&name)
		if err != nil {
			t.Fatalf("index %q not found: %v", idx, err)
		}
	}
}

// NT-3: Verifies R-3 — created_at is auto-populated
func TestMigration0018_CreatedAtAutoPopulated(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	_, err := db.Exec(`INSERT INTO agent_sessions
		(session_id, agent_id, workspace_id, started_at)
		VALUES ('sess-ts', 'agent-1', 'ws-1', '2025-01-01T00:00:00.000')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var createdAt string
	err = db.QueryRow("SELECT created_at FROM agent_sessions WHERE session_id='sess-ts'").Scan(&createdAt)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if createdAt == "" {
		t.Fatal("created_at should be auto-populated")
	}
}
