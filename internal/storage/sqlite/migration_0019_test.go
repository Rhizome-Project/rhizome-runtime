package sqlite_test

import (
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"testing"
)

// T-1: Verifies R-1 — agent_definitions table exists after migration
func TestMigration0019_TableExists(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	var name string
	err := db.QueryRow(
		"SELECT name FROM sqlite_master WHERE type='table' AND name=?", "agent_definitions",
	).Scan(&name)
	if err != nil {
		t.Fatalf("table agent_definitions not found: %v", err)
	}
}

// T-2: Verifies R-1 — insert a row, select it back, verify all columns
func TestMigration0019_InsertAndSelect(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	_, err := db.Exec(`INSERT INTO agent_definitions
		(id, name, provider, model, system_prompt, tools_json, max_iterations, workspace_id)
		VALUES ('agent-001', 'My Agent', 'openai', 'gpt-4', 'You are helpful.', '["bash","read"]', 25, 'ws-1')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var id, name, provider, model, systemPrompt, toolsJSON, workspaceID string
	var maxIterations int
	var createdAt, updatedAt string
	err = db.QueryRow(`SELECT id, name, provider, model, system_prompt, tools_json, max_iterations, workspace_id, created_at, updated_at
		FROM agent_definitions WHERE id='agent-001'`).
		Scan(&id, &name, &provider, &model, &systemPrompt, &toolsJSON, &maxIterations, &workspaceID, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if id != "agent-001" {
		t.Fatalf("id: expected %q, got %q", "agent-001", id)
	}
	if name != "My Agent" {
		t.Fatalf("name: expected %q, got %q", "My Agent", name)
	}
	if provider != "openai" {
		t.Fatalf("provider: expected %q, got %q", "openai", provider)
	}
	if model != "gpt-4" {
		t.Fatalf("model: expected %q, got %q", "gpt-4", model)
	}
	if systemPrompt != "You are helpful." {
		t.Fatalf("system_prompt: expected %q, got %q", "You are helpful.", systemPrompt)
	}
	if toolsJSON != `["bash","read"]` {
		t.Fatalf("tools_json: expected %q, got %q", `["bash","read"]`, toolsJSON)
	}
	if maxIterations != 25 {
		t.Fatalf("max_iterations: expected 25, got %d", maxIterations)
	}
	if workspaceID != "ws-1" {
		t.Fatalf("workspace_id: expected %q, got %q", "ws-1", workspaceID)
	}
	if createdAt == "" {
		t.Fatal("created_at should be auto-populated")
	}
	if updatedAt == "" {
		t.Fatal("updated_at should be auto-populated")
	}
}

// Verifies R-1 — default values work correctly
func TestMigration0019_DefaultValues(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	_, err := db.Exec(`INSERT INTO agent_definitions (id, name, workspace_id) VALUES ('agent-def', 'Default Agent', 'ws-1')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var provider, model, systemPrompt, toolsJSON string
	var maxIterations int
	err = db.QueryRow(`SELECT provider, model, system_prompt, tools_json, max_iterations
		FROM agent_definitions WHERE id='agent-def'`).
		Scan(&provider, &model, &systemPrompt, &toolsJSON, &maxIterations)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if provider != "claude" {
		t.Fatalf("default provider: expected %q, got %q", "claude", provider)
	}
	if model != "" {
		t.Fatalf("default model: expected empty, got %q", model)
	}
	if systemPrompt != "" {
		t.Fatalf("default system_prompt: expected empty, got %q", systemPrompt)
	}
	if toolsJSON != "[]" {
		t.Fatalf("default tools_json: expected %q, got %q", "[]", toolsJSON)
	}
	if maxIterations != 50 {
		t.Fatalf("default max_iterations: expected 50, got %d", maxIterations)
	}
}

// Verifies R-2, R-3 — indexes exist
func TestMigration0019_IndexesExist(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	expectedIndexes := []string{
		"idx_agent_definitions_workspace_id",
		"idx_agent_definitions_name_workspace",
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

// Verifies R-3 — unique constraint on (name, workspace_id)
func TestMigration0019_UniqueNamePerWorkspace(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	db := store.DB()

	_, err := db.Exec(`INSERT INTO agent_definitions (id, name, workspace_id) VALUES ('agent-u1', 'Same Name', 'ws-1')`)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}

	_, err = db.Exec(`INSERT INTO agent_definitions (id, name, workspace_id) VALUES ('agent-u2', 'Same Name', 'ws-1')`)
	if err == nil {
		t.Fatal("expected unique constraint violation for duplicate name in same workspace, got nil error")
	}

	// Same name in different workspace should succeed
	_, err = db.Exec(`INSERT INTO agent_definitions (id, name, workspace_id) VALUES ('agent-u3', 'Same Name', 'ws-2')`)
	if err != nil {
		t.Fatalf("same name in different workspace should succeed: %v", err)
	}
}
