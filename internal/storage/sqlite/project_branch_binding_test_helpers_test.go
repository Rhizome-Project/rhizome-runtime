package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func ensureProjectImplementationTaskForBranchBindingTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID string) {
	t.Helper()

	var existing string
	err := store.DB().QueryRowContext(ctx, `
SELECT task_id
FROM workspace_tasks
WHERE workspace_id = ? AND task_id = ?
LIMIT 1`, workspaceID, taskID).Scan(&existing)
	if err == nil {
		return
	}
	if err != sql.ErrNoRows {
		t.Fatalf("check ready branch task %s: %v", taskID, err)
	}
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE tasks
   SET status = ?, updated_at = ?
 WHERE task_id = ?`,
		model.TaskStatusCancelled, now, taskID); err != nil {
		t.Fatalf("cancel synthetic ready branch task %s: %v", taskID, err)
	}
}

func seedProjectBranchClaimForReadyTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, ownerID, checkoutID, branchID, taskID, scopeJSON string) {
	t.Helper()

	ensureProjectImplementationTaskForBranchBindingTest(t, ctx, store, workspaceID, projectID, taskID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if hints := writeScopeHintPathsForTest(scopeJSON); len(hints) > 0 {
		hintsJSON, _ := json.Marshal(hints)
		requirements := map[string]any{
			"schema":                     "task_requirements.v1",
			"preserve_write_scope_hints": true,
		}
		var existingRequirements string
		if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(task_requirements_json, '{}')
  FROM tasks
 WHERE task_id = ?`, taskID).Scan(&existingRequirements); err == nil && json.Valid([]byte(existingRequirements)) {
			var decoded map[string]any
			if json.Unmarshal([]byte(existingRequirements), &decoded) == nil && decoded != nil {
				requirements = decoded
				schema, _ := requirements["schema"].(string)
				if strings.TrimSpace(schema) == "" {
					requirements["schema"] = "task_requirements.v1"
				}
				requirements["preserve_write_scope_hints"] = true
			}
		}
		requirementsJSON, _ := json.Marshal(requirements)
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
		taskID, workspaceID, ownerID, model.TaskClaimStatusClaimed, "test ready branch claim", now, now, repoID, checkoutID, branchID, scopeJSON); err != nil {
		t.Fatalf("seed ready branch claim %s: %v", branchID, err)
	}
}

func releaseProjectBranchClaimForReadyTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, branchID, taskID string) {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE task_claims
   SET claim_status = ?, released_at = ?, updated_at = ?
 WHERE workspace_id = ?
   AND task_id = ?
   AND branch_id = ?`,
		model.TaskClaimStatusReleased, now, now, workspaceID, taskID, branchID); err != nil {
		t.Fatalf("release ready branch claim %s/%s: %v", branchID, taskID, err)
	}
}

func registerReservedProjectBranchForReadyBindingTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, checkoutID, ownerID, branchID, branchName, scopeJSON string) sqlite.ProjectBranchRecord {
	t.Helper()

	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkoutID,
		AgentID:               ownerID,
		BranchID:              branchID,
		BranchName:            branchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register reserved ready-bound branch %s: %v", branchID, err)
	}
	return branch
}

func seedReservedReadyBranchBindingForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, checkoutID, ownerID, branchID, branchName, scopeJSON string) (sqlite.ProjectBranchRecord, string) {
	t.Helper()

	branch := registerReservedProjectBranchForReadyBindingTest(t, ctx, store, workspaceID, projectID, repoID, checkoutID, ownerID, branchID, branchName, scopeJSON)
	taskID := "task-" + branchID
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, checkoutID, branch.BranchID, taskID, scopeJSON)
	return branch, taskID
}

func rebindReadyBranchActiveTaskForTest(t *testing.T, ctx context.Context, store *sqlite.Store, branch sqlite.ProjectBranchRecord, taskID, actorID, scopeJSON string) sqlite.ProjectBranchRecord {
	t.Helper()

	if strings.TrimSpace(scopeJSON) == "" {
		scopeJSON = branch.WriteScopeJSON
	}
	seedProjectBranchClaimForReadyTest(t, ctx, store, branch.WorkspaceID, branch.ProjectID, branch.RepoID, branch.AgentID, branch.CheckoutID, branch.BranchID, taskID, scopeJSON)
	updated, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           branch.WorkspaceID,
		ProjectID:             branch.ProjectID,
		RepoID:                branch.RepoID,
		CheckoutID:            branch.CheckoutID,
		AgentID:               branch.AgentID,
		BranchID:              branch.BranchID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            branch.BranchName,
		BranchKind:            branch.BranchKind,
		BaseBranch:            branch.BaseBranch,
		BaseSHA:               branch.BaseSHA,
		HeadSHA:               branch.HeadSHA,
		WriteScopeJSON:        scopeJSON,
		ReviewDocKey:          branch.ReviewDocKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", branch.WorkspaceID, "agent", actorID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("rebind ready branch %s to task %s: %v", branch.BranchID, taskID, err)
	}
	return updated
}
