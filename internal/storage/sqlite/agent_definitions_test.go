package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// T-1: Verifies R-2 — create with all fields, verify returned record matches input.
func TestCreateAgentDefinition_Success(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	input := sqlite.AgentDefinitionCreateInput{
		ID:            "agentdef-test-1",
		Name:          "My Agent",
		Provider:      "claude",
		Model:         "claude-sonnet-4-20250514",
		SystemPrompt:  "You are a helpful assistant.",
		Tools:         []string{"read_file", "write_file", "bash"},
		MaxIterations: 25,
		WorkspaceID:   "ws-1",
	}

	got, err := store.CreateAgentDefinition(ctx, input)
	if err != nil {
		t.Fatalf("create agent definition: %v", err)
	}

	if got.ID != "agentdef-test-1" {
		t.Fatalf("expected id %q, got %q", "agentdef-test-1", got.ID)
	}
	if got.Name != "My Agent" {
		t.Fatalf("expected name %q, got %q", "My Agent", got.Name)
	}
	if got.Provider != "claude" {
		t.Fatalf("expected provider %q, got %q", "claude", got.Provider)
	}
	if got.Model != "claude-sonnet-4-20250514" {
		t.Fatalf("expected model %q, got %q", "claude-sonnet-4-20250514", got.Model)
	}
	if got.SystemPrompt != "You are a helpful assistant." {
		t.Fatalf("expected system_prompt %q, got %q", "You are a helpful assistant.", got.SystemPrompt)
	}
	if len(got.Tools) != 3 {
		t.Fatalf("expected 3 tools, got %d", len(got.Tools))
	}
	if got.Tools[0] != "read_file" || got.Tools[1] != "write_file" || got.Tools[2] != "bash" {
		t.Fatalf("tools mismatch: %v", got.Tools)
	}
	if got.MaxIterations != 25 {
		t.Fatalf("expected max_iterations 25, got %d", got.MaxIterations)
	}
	if got.WorkspaceID != "ws-1" {
		t.Fatalf("expected workspace_id %q, got %q", "ws-1", got.WorkspaceID)
	}
	if got.CreatedAt == "" {
		t.Fatal("created_at should not be empty")
	}
	if got.UpdatedAt == "" {
		t.Fatal("updated_at should not be empty")
	}
}

// T-2: Verifies R-2 — create with empty ID, verify ID is auto-generated with "agentdef-" prefix.
func TestCreateAgentDefinition_DefaultID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	input := sqlite.AgentDefinitionCreateInput{
		Name:        "Auto ID Agent",
		Provider:    "claude",
		WorkspaceID: "ws-1",
	}

	got, err := store.CreateAgentDefinition(ctx, input)
	if err != nil {
		t.Fatalf("create agent definition: %v", err)
	}

	if got.ID == "" {
		t.Fatal("expected auto-generated ID, got empty string")
	}
	if len(got.ID) < len("agentdef-") {
		t.Fatalf("expected ID with prefix 'agentdef-', got %q", got.ID)
	}
	if got.ID[:9] != "agentdef-" {
		t.Fatalf("expected ID prefix 'agentdef-', got %q", got.ID)
	}
}

// T-3: Verifies R-1, R-2 — create with MaxIterations=0, verify it becomes 50.
func TestCreateAgentDefinition_DefaultMaxIterations(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	input := sqlite.AgentDefinitionCreateInput{
		ID:            "agentdef-default-iter",
		Name:          "Default Iter Agent",
		Provider:      "openai",
		MaxIterations: 0,
		WorkspaceID:   "ws-1",
	}

	got, err := store.CreateAgentDefinition(ctx, input)
	if err != nil {
		t.Fatalf("create agent definition: %v", err)
	}

	if got.MaxIterations != 50 {
		t.Fatalf("expected max_iterations 50 (default), got %d", got.MaxIterations)
	}
}

// T-4: Verifies R-2 — validation errors.
func TestCreateAgentDefinition_ValidationErrors(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	// Empty name
	_, err := store.CreateAgentDefinition(ctx, sqlite.AgentDefinitionCreateInput{
		ID:          "agentdef-val-1",
		Name:        "",
		Provider:    "claude",
		WorkspaceID: "ws-1",
	})
	if err == nil {
		t.Fatal("expected error for empty name, got nil")
	}

	// Empty workspace_id
	_, err = store.CreateAgentDefinition(ctx, sqlite.AgentDefinitionCreateInput{
		ID:          "agentdef-val-2",
		Name:        "Some Agent",
		Provider:    "claude",
		WorkspaceID: "",
	})
	if err == nil {
		t.Fatal("expected error for empty workspace_id, got nil")
	}

	// Invalid provider
	_, err = store.CreateAgentDefinition(ctx, sqlite.AgentDefinitionCreateInput{
		ID:          "agentdef-val-3",
		Name:        "Some Agent",
		Provider:    "gemini",
		WorkspaceID: "ws-1",
	})
	if err == nil {
		t.Fatal("expected error for invalid provider, got nil")
	}
}

// T-5: Verifies R-4 — create then get, assert match.
func TestGetAgentDefinition_Success(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	input := sqlite.AgentDefinitionCreateInput{
		ID:            "agentdef-get-1",
		Name:          "Get Test Agent",
		Provider:      "claude",
		Model:         "claude-sonnet-4-20250514",
		SystemPrompt:  "Test prompt",
		Tools:         []string{"bash"},
		MaxIterations: 10,
		WorkspaceID:   "ws-1",
	}

	_, err := store.CreateAgentDefinition(ctx, input)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := store.GetAgentDefinition(ctx, "agentdef-get-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.ID != "agentdef-get-1" {
		t.Fatalf("expected id %q, got %q", "agentdef-get-1", got.ID)
	}
	if got.Name != "Get Test Agent" {
		t.Fatalf("expected name %q, got %q", "Get Test Agent", got.Name)
	}
	if got.Provider != "claude" {
		t.Fatalf("expected provider %q, got %q", "claude", got.Provider)
	}
	if got.Model != "claude-sonnet-4-20250514" {
		t.Fatalf("expected model %q, got %q", "claude-sonnet-4-20250514", got.Model)
	}
	if len(got.Tools) != 1 || got.Tools[0] != "bash" {
		t.Fatalf("expected tools [bash], got %v", got.Tools)
	}
	if got.MaxIterations != 10 {
		t.Fatalf("expected max_iterations 10, got %d", got.MaxIterations)
	}
}

// T-6: Verifies R-4, EC-2 — get non-existent ID returns ErrAgentDefinitionNotFound.
func TestGetAgentDefinition_NotFound(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	_, err := store.GetAgentDefinition(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sqlite.ErrAgentDefinitionNotFound) {
		t.Fatalf("expected ErrAgentDefinitionNotFound, got: %v", err)
	}
}

// T-7: Verifies R-5 — create 3 definitions in 2 workspaces, list all.
func TestListAgentDefinitions_All(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	defs := []sqlite.AgentDefinitionCreateInput{
		{ID: "agentdef-list-1", Name: "Agent A", Provider: "claude", WorkspaceID: "ws-1"},
		{ID: "agentdef-list-2", Name: "Agent B", Provider: "openai", WorkspaceID: "ws-2"},
		{ID: "agentdef-list-3", Name: "Agent C", Provider: "claude", WorkspaceID: "ws-1"},
	}

	for _, d := range defs {
		// Small sleep to ensure distinct created_at timestamps
		time.Sleep(10 * time.Millisecond)
		if _, err := store.CreateAgentDefinition(ctx, d); err != nil {
			t.Fatalf("create %s: %v", d.ID, err)
		}
	}

	got, err := store.ListAgentDefinitions(ctx, "")
	if err != nil {
		t.Fatalf("list all: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 definitions, got %d", len(got))
	}

	// Should be ordered by created_at DESC (newest first)
	if got[0].ID != "agentdef-list-3" {
		t.Fatalf("expected newest first (agentdef-list-3), got %q", got[0].ID)
	}
	if got[2].ID != "agentdef-list-1" {
		t.Fatalf("expected oldest last (agentdef-list-1), got %q", got[2].ID)
	}
}

// T-8: Verifies R-5 — filter by workspace_id.
func TestListAgentDefinitions_ByWorkspace(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	defs := []sqlite.AgentDefinitionCreateInput{
		{ID: "agentdef-ws-1", Name: "Agent A", Provider: "claude", WorkspaceID: "ws-1"},
		{ID: "agentdef-ws-2", Name: "Agent B", Provider: "openai", WorkspaceID: "ws-2"},
		{ID: "agentdef-ws-3", Name: "Agent C", Provider: "claude", WorkspaceID: "ws-1"},
	}

	for _, d := range defs {
		if _, err := store.CreateAgentDefinition(ctx, d); err != nil {
			t.Fatalf("create %s: %v", d.ID, err)
		}
	}

	got, err := store.ListAgentDefinitions(ctx, "ws-1")
	if err != nil {
		t.Fatalf("list by workspace: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 definitions for ws-1, got %d", len(got))
	}
	for _, d := range got {
		if d.WorkspaceID != "ws-1" {
			t.Fatalf("expected workspace_id ws-1, got %q", d.WorkspaceID)
		}
	}
}

// T-9: Verifies R-6 — create then delete, verify Get returns not found.
func TestDeleteAgentDefinition_Success(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	_, err := store.CreateAgentDefinition(ctx, sqlite.AgentDefinitionCreateInput{
		ID:          "agentdef-del-1",
		Name:        "To Delete",
		Provider:    "claude",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := store.DeleteAgentDefinition(ctx, "agentdef-del-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err = store.GetAgentDefinition(ctx, "agentdef-del-1")
	if !errors.Is(err, sqlite.ErrAgentDefinitionNotFound) {
		t.Fatalf("expected ErrAgentDefinitionNotFound after delete, got: %v", err)
	}
}

// T-10: Verifies R-6, EC-3 — delete non-existent returns ErrAgentDefinitionNotFound.
func TestDeleteAgentDefinition_NotFound(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	err := store.DeleteAgentDefinition(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sqlite.ErrAgentDefinitionNotFound) {
		t.Fatalf("expected ErrAgentDefinitionNotFound, got: %v", err)
	}
}

// T-11: Verifies EC-1 — create two with same name+workspace, second returns error.
func TestCreateAgentDefinition_DuplicateName(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	_, err := store.CreateAgentDefinition(ctx, sqlite.AgentDefinitionCreateInput{
		ID:          "agentdef-dup-1",
		Name:        "Duplicate Name",
		Provider:    "claude",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	_, err = store.CreateAgentDefinition(ctx, sqlite.AgentDefinitionCreateInput{
		ID:          "agentdef-dup-2",
		Name:        "Duplicate Name",
		Provider:    "claude",
		WorkspaceID: "ws-1",
	})
	if err == nil {
		t.Fatal("expected error on duplicate name+workspace, got nil")
	}
	if !errors.Is(err, sqlite.ErrAgentDefinitionNameConflict) {
		t.Fatalf("expected ErrAgentDefinitionNameConflict, got: %v", err)
	}
}

// T-12: Verifies EC-4 — nil tools stored as empty array, loaded back as empty slice.
func TestCreateAgentDefinition_NilTools(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	_, err := store.CreateAgentDefinition(ctx, sqlite.AgentDefinitionCreateInput{
		ID:          "agentdef-nil-tools",
		Name:        "Nil Tools Agent",
		Provider:    "claude",
		WorkspaceID: "ws-1",
		Tools:       nil,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := store.GetAgentDefinition(ctx, "agentdef-nil-tools")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Tools == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(got.Tools) != 0 {
		t.Fatalf("expected 0 tools, got %d", len(got.Tools))
	}
}
