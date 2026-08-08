package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func seedServerProjectBranchClaimForReady(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, agentID, checkoutID, branchID, taskID, scopeJSON string) {
	t.Helper()

	var existingTaskID string
	err := store.DB().QueryRowContext(ctx, `
SELECT wt.task_id
  FROM workspace_tasks wt
 WHERE wt.workspace_id = ?
   AND wt.task_id = ?
 LIMIT 1`, workspaceID, taskID).Scan(&existingTaskID)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("check project task %s: %v", taskID, err)
	}
	if err == sql.ErrNoRows {
		graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
		if err := dag.ValidateGraph(graph); err != nil {
			t.Fatalf("validate project task graph: %v", err)
		}
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:         workspaceID,
			TaskID:              taskID,
			OwnerUserID:         "developer",
			Priority:            "high",
			Title:               "Implement project slice",
			Description:         "Synthetic server RPC task backing a READY_FOR_REVIEW branch.",
			ProjectID:           projectID,
			TaskKind:            "EXECUTION",
			ProjectLane:         "implementation",
			RequiresProjectGate: true,
		}, graph); err != nil {
			t.Fatalf("create project task %s: %v", taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach project task %s: %v", taskID, err)
		}
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if hints := serverWriteScopeHintPathsForTest(scopeJSON); len(hints) > 0 {
		hintsJSON, _ := json.Marshal(hints)
		requirementsJSON, _ := json.Marshal(map[string]any{
			"schema":                     "task_requirements.v1",
			"preserve_write_scope_hints": true,
		})
		if _, err := store.DB().ExecContext(ctx, `
UPDATE tasks
   SET write_scope_hints_json = ?,
       task_requirements_json = ?,
       updated_at = ?
 WHERE task_id = ?`,
			string(hintsJSON), string(requirementsJSON), now, taskID); err != nil {
			t.Fatalf("seed ready branch task scope %s: %v", taskID, err)
		}
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT OR REPLACE INTO task_claims(
  task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at,
  project_role_id, repo_id, checkout_id, branch_id, write_scope_json
) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, '', ?, ?, ?, ?)`,
		taskID, workspaceID, agentID, model.TaskClaimStatusClaimed, "server rpc ready branch claim", now, now, repoID, checkoutID, branchID, scopeJSON); err != nil {
		t.Fatalf("seed branch-bound claim %s/%s: %v", branchID, taskID, err)
	}
}

func serverWriteScopeHintPathsForTest(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || !json.Valid([]byte(raw)) {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil
	}
	var paths []string
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case string:
			if path := strings.TrimSpace(typed); path != "" {
				paths = append(paths, path)
			}
		case []any:
			for _, item := range typed {
				walk(item)
			}
		}
	}
	if object, ok := decoded.(map[string]any); ok {
		for _, key := range []string{"paths", "files", "path_prefixes", "write_paths", "scopes"} {
			walk(object[key])
		}
		return paths
	}
	walk(decoded)
	return paths
}
