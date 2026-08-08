package sqlite

import (
	"context"
	"strings"
	"testing"
)

func TestWorkspaceMemoryFTSNeedsRebuildDetectsConstructorFailure(t *testing.T) {
	err := assertErrString("SQL logic error: vtable constructor failed: workspace_memory_fts")
	if !workspaceMemoryFTSNeedsRebuild(err) {
		t.Fatalf("expected vtable constructor failure to request FTS repair")
	}
}

func TestRecreateWorkspaceMemoryFTSAfterSchemaRemoval(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: "ws-memory-force-fts-repair",
		Title:       "Runtime Memory FTS Force Repair",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: "ws-memory-force-fts-repair",
		AgentID:     "agent-memory",
		OwnerUserID: "developer",
		DisplayName: "Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-force-fts-repair",
		MemoryType:  "lesson",
		Title:       "Seed memory",
		Body:        "Seed the FTS schema before forced recreation.",
		AgentID:     "agent-memory",
		SourceKind:  "reflection",
		SourceID:    "agent-memory",
	}); err != nil {
		t.Fatalf("record seed workspace memory: %v", err)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := removeBrokenWorkspaceMemoryFTSSchemaTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("remove broken FTS schema: %v", err)
	}
	if err := recreateWorkspaceMemoryFTSTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("recreate workspace memory FTS: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit FTS recreation: %v", err)
	}

	repaired, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-force-fts-repair",
		MemoryType:  "lesson",
		Title:       "Second memory after forced FTS repair",
		Body:        "Forced recreation should keep workspace memory writes searchable.",
		AgentID:     "agent-memory",
		SourceKind:  "reflection",
		SourceID:    "agent-memory",
	})
	if err != nil {
		t.Fatalf("record workspace memory after forced FTS recreation: %v", err)
	}
	items, err := store.SearchWorkspaceMemory(ctx, WorkspaceMemoryFilter{
		WorkspaceID: "ws-memory-force-fts-repair",
		Query:       "forced recreation searchable",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search workspace memory after forced FTS recreation: %v", err)
	}
	if len(items) == 0 || items[0].MemoryID != repaired.MemoryID {
		t.Fatalf("expected repaired memory to be searchable, repaired=%s items=%+v", repaired.MemoryID, items)
	}
}

type assertErrString string

func (e assertErrString) Error() string { return strings.TrimSpace(string(e)) }
