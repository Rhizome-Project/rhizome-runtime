package sqlite_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// TestTaskIDCanonicalizationResolvesCaseVariant locks SA-3: a case/alias-variant task_id (e.g. an
// agent passing a lowercased copy of a canonical id that embeds an uppercase RFC3339 timestamp) must
// canonicalize to the stored task on GetTaskStatus and ClaimTask, mirroring the project_id fix (CR-21).
// Exact-cased lookups are unchanged (zero regression).
func TestTaskIDCanonicalizationResolvesCaseVariant(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-task-id-canon"
		agentID     = "agent-canon"
		taskID      = "task-signal01-canon-20260603T120000Z-root"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	alias := strings.ToLower(taskID)
	if alias == taskID {
		t.Fatal("alias must differ from canonical for this test")
	}

	// GetTaskStatus with the aliased id must resolve to the canonical task, not ErrTaskNotFound.
	status, err := store.GetTaskStatus(ctx, workspaceID, alias)
	if err != nil {
		t.Fatalf("GetTaskStatus(alias) should canonicalize, got error: %v", err)
	}
	if status.TaskID != taskID {
		t.Fatalf("GetTaskStatus(alias) returned task_id=%q, want canonical %q", status.TaskID, taskID)
	}

	// ClaimTask with the aliased id must canonicalize and succeed against the canonical task.
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      alias,
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("ClaimTask(alias) should canonicalize and succeed, got: %v", err)
	}

	// Exact-cased lookup remains correct (zero regression).
	exact, err := store.GetTaskStatus(ctx, workspaceID, taskID)
	if err != nil || exact.TaskID != taskID {
		t.Fatalf("GetTaskStatus(exact) regressed: status=%+v err=%v", exact, err)
	}
}
