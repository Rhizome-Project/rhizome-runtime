package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTaskClaimOwnerBoundBranchByNameRequiresUniqueLiveBranch(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-owner-bound-branch-name"
		projectID   = "project-owner-bound-branch-name"
	)
	seedBranchNameResolverProject(t, ctx, store, workspaceID, projectID)
	insertBranchNameResolverBranch(t, ctx, store, workspaceID, projectID, "repo-a", "branch-live", "agent/alpha/live", ProjectBranchStatusActive)
	insertBranchNameResolverBranch(t, ctx, store, workspaceID, projectID, "repo-a", "branch-ambig-a", "agent/alpha/ambiguous", ProjectBranchStatusActive)
	insertBranchNameResolverBranch(t, ctx, store, workspaceID, projectID, "repo-b", "branch-ambig-b", "agent/alpha/ambiguous", ProjectBranchStatusReserved)
	insertBranchNameResolverBranch(t, ctx, store, workspaceID, projectID, "repo-a", "branch-terminal", "agent/alpha/terminal", ProjectBranchStatusAbandoned)

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	branch, ok, err := taskClaimOwnerBoundBranchByNameTx(ctx, tx, workspaceID, projectID, "agent/alpha/live")
	if err != nil {
		t.Fatalf("resolve unique live branch: %v", err)
	}
	if !ok || branch.BranchID != "branch-live" {
		t.Fatalf("expected unique live branch, ok=%v branch=%+v", ok, branch)
	}

	_, _, err = taskClaimOwnerBoundBranchByNameTx(ctx, tx, workspaceID, projectID, "agent/alpha/ambiguous")
	if !errors.Is(err, ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "multiple live branches") {
		t.Fatalf("expected ambiguous live branch name to be rejected, got %v", err)
	}

	_, _, err = taskClaimOwnerBoundBranchByNameTx(ctx, tx, workspaceID, projectID, "agent/alpha/terminal")
	if !errors.Is(err, ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("expected terminal-only branch name to be rejected, got %v", err)
	}

	if _, ok, err := taskClaimOwnerBoundBranchByNameTx(ctx, tx, workspaceID, projectID, "agent/alpha/missing"); err != nil || ok {
		t.Fatalf("missing branch name = ok=%v err=%v, want no match", ok, err)
	}
}

func seedBranchNameResolverProject(t *testing.T, ctx context.Context, store *Store, workspaceID, projectID string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.db.ExecContext(ctx, `
INSERT INTO workspaces(workspace_id, title, description, created_by, status, created_at, updated_at)
VALUES (?, ?, '', 'developer', 'ACTIVE', ?, ?)`,
		workspaceID,
		workspaceID,
		now,
		now,
	); err != nil {
		t.Fatalf("insert workspace: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
INSERT INTO projects(project_id, workspace_id, title, description, status, created_by, created_at, updated_at)
VALUES (?, ?, ?, '', 'ACTIVE', 'developer', ?, ?)`,
		projectID,
		workspaceID,
		projectID,
		now,
		now,
	); err != nil {
		t.Fatalf("insert project: %v", err)
	}
	for _, repoID := range []string{"repo-a", "repo-b"} {
		if _, err := store.db.ExecContext(ctx, `
INSERT INTO project_repositories(repo_id, workspace_id, project_id, remote_url, remote_kind, name, default_branch, repo_status, is_canonical, created_at, updated_at)
VALUES (?, ?, ?, ?, 'local', ?, 'main', 'READY', 0, ?, ?)`,
			repoID,
			workspaceID,
			projectID,
			"file:///"+repoID,
			repoID,
			now,
			now,
		); err != nil {
			t.Fatalf("insert repo %s: %v", repoID, err)
		}
	}
}

func insertBranchNameResolverBranch(t *testing.T, ctx context.Context, store *Store, workspaceID, projectID, repoID, branchID, branchName, status string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.db.ExecContext(ctx, `
INSERT INTO project_branch_registry(
  branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, branch_name, branch_kind,
  base_branch, write_scope_json, status, updated_by, created_at, updated_at
) VALUES (?, ?, ?, ?, '', 'agent-alpha', ?, 'feature', 'main', '{"paths":["src/**"]}', ?, 'agent-alpha', ?, ?)`,
		branchID,
		workspaceID,
		projectID,
		repoID,
		branchName,
		status,
		now,
		now,
	); err != nil {
		t.Fatalf("insert branch %s: %v", branchID, err)
	}
}
