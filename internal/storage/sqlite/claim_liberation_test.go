package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestNarrowProjectClaimAndBranchScopeUpdatesClaimAndBranchTogether(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-liberation-narrow"
		projectID   = "project-claim-liberation-narrow"
		repoID      = "repo-a"
		branchID    = "branch-claim-liberation-narrow"
		taskID      = "task-claim-liberation-narrow"
		agentID     = "delta"
	)
	seedClaimLiberationFixture(t, ctx, store, workspaceID, projectID, repoID, branchID, taskID, agentID, `{"paths":["internal/eval/**","go.mod","go.sum"]}`, ProjectBranchStatusActive)

	result, err := store.NarrowProjectClaimAndBranchScopeWithEvent(ctx, ClaimLiberationNarrowInput{
		WorkspaceID:       workspaceID,
		ProjectID:         projectID,
		BranchID:          branchID,
		TaskID:            taskID,
		AgentID:           agentID,
		NewWriteScopeJSON: `{"paths":["internal/eval/**"]}`,
		Reason:            "release shared sidecars for blocked revision",
		ActorID:           "claim_liberation_watchdog",
	})
	if err != nil {
		t.Fatalf("narrow claim and branch scope: %v", err)
	}
	if !result.Narrowed || result.Event.EventType != "claim.liberation_narrowed" {
		t.Fatalf("unexpected result: %+v", result)
	}
	assertClaimLiberationScope(t, ctx, store, workspaceID, taskID, `["internal/eval/**"]`, "task_claims")
	assertClaimLiberationBranchScope(t, ctx, store, workspaceID, branchID, `["internal/eval/**"]`)
}

func TestNarrowProjectClaimAndBranchScopeSkipsReadyForReviewAndLivePatchQueue(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-liberation-skip"
		projectID   = "project-claim-liberation-skip"
		repoID      = "repo-a"
		branchID    = "branch-claim-liberation-skip"
		taskID      = "task-claim-liberation-skip"
		agentID     = "delta"
	)
	seedClaimLiberationFixture(t, ctx, store, workspaceID, projectID, repoID, branchID, taskID, agentID, `{"paths":["internal/eval/**","go.mod"]}`, ProjectBranchStatusReadyForReview)
	result, err := store.NarrowProjectClaimAndBranchScopeWithEvent(ctx, ClaimLiberationNarrowInput{
		WorkspaceID:       workspaceID,
		ProjectID:         projectID,
		BranchID:          branchID,
		TaskID:            taskID,
		AgentID:           agentID,
		NewWriteScopeJSON: `{"paths":["internal/eval/**"]}`,
	})
	if err != nil {
		t.Fatalf("ready branch narrow should skip, not error: %v", err)
	}
	if result.Narrowed || !strings.Contains(result.SkipReason, "branch_status_not_narrowable") {
		t.Fatalf("expected ready branch skip, got %+v", result)
	}

	const (
		liveBranchID = "branch-claim-liberation-live-queue"
		liveTaskID   = "task-claim-liberation-live-queue"
	)
	seedClaimLiberationFixture(t, ctx, store, workspaceID, projectID, repoID, liveBranchID, liveTaskID, agentID, `{"paths":["internal/eval/**","go.mod"]}`, ProjectBranchStatusActive)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin live queue seed tx: %v", err)
	}
	if err := upsertProjectPatchQueueItemTx(ctx, tx, ProjectPatchQueueItemRecord{
		QueueID:           "queue-live",
		ItemID:            "item-live",
		WorkspaceID:       workspaceID,
		ProjectID:         projectID,
		RepoID:            repoID,
		BranchID:          liveBranchID,
		RepoAuthorityMode: ProjectPatchQueueAuthorityModeControlledQueue,
		State:             ProjectPatchQueueStateProposed,
		PathsetJSON:       `["internal/eval/**","go.mod"]`,
		HeadSHA:           "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BaseSHA:           "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		SubmittedBy:       agentID,
		AgentID:           agentID,
		PrincipalType:     "agent",
		PrincipalID:       agentID,
		CreatedAt:         now,
		UpdatedAt:         now,
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("seed live patch queue item: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit live queue seed: %v", err)
	}
	result, err = store.NarrowProjectClaimAndBranchScopeWithEvent(ctx, ClaimLiberationNarrowInput{
		WorkspaceID:       workspaceID,
		ProjectID:         projectID,
		BranchID:          liveBranchID,
		TaskID:            liveTaskID,
		AgentID:           agentID,
		NewWriteScopeJSON: `{"paths":["internal/eval/**"]}`,
	})
	if err != nil {
		t.Fatalf("live patch queue branch narrow should skip, not error: %v", err)
	}
	if result.Narrowed || !strings.Contains(result.SkipReason, "live_patch_queue_item") {
		t.Fatalf("expected live patch queue skip, got %+v", result)
	}
}

func TestNarrowProjectClaimAndBranchScopeRejectsBranchClaimMismatch(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-liberation-mismatch"
		projectID   = "project-claim-liberation-mismatch"
		repoID      = "repo-a"
		branchID    = "branch-claim-liberation-mismatch"
		taskID      = "task-claim-liberation-mismatch"
		agentID     = "delta"
	)
	seedClaimLiberationFixture(t, ctx, store, workspaceID, projectID, repoID, branchID, taskID, agentID, `{"paths":["internal/eval/**","go.mod"]}`, ProjectBranchStatusActive)
	if _, err := store.DB().ExecContext(ctx, `UPDATE project_branch_registry SET active_claim_id = ? WHERE workspace_id = ? AND branch_id = ?`, "other-task", workspaceID, branchID); err != nil {
		t.Fatalf("corrupt branch active claim: %v", err)
	}

	_, err := store.NarrowProjectClaimAndBranchScopeWithEvent(ctx, ClaimLiberationNarrowInput{
		WorkspaceID:       workspaceID,
		ProjectID:         projectID,
		BranchID:          branchID,
		TaskID:            taskID,
		AgentID:           agentID,
		NewWriteScopeJSON: `{"paths":["internal/eval/**"]}`,
	})
	if err == nil || !strings.Contains(err.Error(), "active_claim_id") {
		t.Fatalf("expected branch/claim mismatch rejection, got %v", err)
	}
}

func TestReconcileStaleProjectClaimLiberationNarrowsSidecarsAfterStaleReleaseRequest(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-liberation-reconcile"
		projectID   = "project-claim-liberation-reconcile"
		repoID      = "repo-a"
		branchID    = "branch-claim-liberation-reconcile"
		taskID      = "task-claim-liberation-reconcile"
		blockedTask = "task-claim-liberation-blocked"
		agentID     = "delta"
	)
	seedClaimLiberationFixture(t, ctx, store, workspaceID, projectID, repoID, branchID, taskID, agentID, `{"paths":["internal/eval/**","go.mod","go.sum"]}`, ProjectBranchStatusActive)
	staleAt := time.Now().UTC().Add(-(localOwnershipReclaimGrace + time.Minute)).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE task_claims SET updated_at = ? WHERE workspace_id = ? AND task_id = ?`, staleAt, workspaceID, taskID); err != nil {
		t.Fatalf("age claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE agents SET last_seen_at = ?, updated_at = ? WHERE workspace_id = ? AND agent_id = ?`, staleAt, staleAt, workspaceID, agentID); err != nil {
		t.Fatalf("age agent: %v", err)
	}
	seedClaimLiberationScopeBusyEvidence(t, ctx, store, workspaceID, projectID, branchID, taskID, blockedTask, agentID, staleAt, `{"paths":["internal/parser/**","go.mod"]}`)

	result, err := store.ReconcileStaleProjectClaimLiberationNarrow(ctx, ClaimLiberationReconcileInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "claim_liberation_watchdog",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reconcile claim liberation: %v", err)
	}
	if result.Narrowed != 1 || result.Problems != 0 {
		t.Fatalf("expected one narrow without problems, got %+v", result)
	}
	assertClaimLiberationScope(t, ctx, store, workspaceID, taskID, `["internal/eval"]`, "task_claims")
	assertClaimLiberationBranchScope(t, ctx, store, workspaceID, branchID, `["internal/eval"]`)

	var payload string
	if err := store.DB().QueryRowContext(ctx, `
SELECT payload_json
  FROM runtime_events
 WHERE workspace_id = ? AND event_type = 'claim.liberation_narrowed' AND entity_id = ?
 ORDER BY created_at DESC
 LIMIT 1`, workspaceID, taskID).Scan(&payload); err != nil {
		t.Fatalf("read liberation event: %v", err)
	}
	if !strings.Contains(payload, "update-claim-liberation-scope-busy") ||
		!strings.Contains(payload, "agent_request.project_claim_owner_resume_pending") ||
		!strings.Contains(payload, "owner_abandoned_after_grace") {
		t.Fatalf("expected structured watchdog evidence in event payload, got %s", payload)
	}
}

func TestReconcileStaleProjectClaimLiberationRequiresDurableReleaseRequest(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-liberation-missing-request"
		projectID   = "project-claim-liberation-missing-request"
		repoID      = "repo-a"
		branchID    = "branch-claim-liberation-missing-request"
		taskID      = "task-claim-liberation-missing-request"
		blockedTask = "task-claim-liberation-missing-request-blocked"
		agentID     = "delta-missing-request"
	)
	seedClaimLiberationFixture(t, ctx, store, workspaceID, projectID, repoID, branchID, taskID, agentID, `{"paths":["internal/eval/**","go.mod"]}`, ProjectBranchStatusActive)
	staleAt := time.Now().UTC().Add(-(localOwnershipReclaimGrace + time.Minute)).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE task_claims SET updated_at = ? WHERE workspace_id = ? AND task_id = ?`, staleAt, workspaceID, taskID); err != nil {
		t.Fatalf("age claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE agents SET last_seen_at = ?, updated_at = ? WHERE workspace_id = ? AND agent_id = ?`, staleAt, staleAt, workspaceID, agentID); err != nil {
		t.Fatalf("age agent: %v", err)
	}
	seedClaimLiberationScopeBusyEvidenceWithOptions(t, ctx, store, workspaceID, projectID, branchID, taskID, blockedTask, agentID, staleAt, `{"paths":["internal/parser/**","go.mod"]}`, claimLiberationEvidenceOptions{WithRequest: false})

	result, err := store.ReconcileStaleProjectClaimLiberationNarrow(ctx, ClaimLiberationReconcileInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "claim_liberation_watchdog",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reconcile claim liberation: %v", err)
	}
	if result.Scanned != 0 || result.Narrowed != 0 {
		t.Fatalf("agent_update-only evidence must not become a liberation candidate, got %+v", result)
	}
	assertClaimLiberationScope(t, ctx, store, workspaceID, taskID, `"go.mod"`, "task_claims")
}

func TestReconcileStaleProjectClaimLiberationRequiresLiveBlockedTask(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-liberation-terminal-blocked"
		projectID   = "project-claim-liberation-terminal-blocked"
		repoID      = "repo-a"
		branchID    = "branch-claim-liberation-terminal-blocked"
		taskID      = "task-claim-liberation-terminal-blocked"
		blockedTask = "task-claim-liberation-terminal-blocked-peer"
		agentID     = "delta-terminal-blocked"
	)
	seedClaimLiberationFixture(t, ctx, store, workspaceID, projectID, repoID, branchID, taskID, agentID, `{"paths":["internal/eval/**","go.mod"]}`, ProjectBranchStatusActive)
	staleAt := time.Now().UTC().Add(-(localOwnershipReclaimGrace + time.Minute)).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE task_claims SET updated_at = ? WHERE workspace_id = ? AND task_id = ?`, staleAt, workspaceID, taskID); err != nil {
		t.Fatalf("age claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE agents SET last_seen_at = ?, updated_at = ? WHERE workspace_id = ? AND agent_id = ?`, staleAt, staleAt, workspaceID, agentID); err != nil {
		t.Fatalf("age agent: %v", err)
	}
	seedClaimLiberationScopeBusyEvidenceWithOptions(t, ctx, store, workspaceID, projectID, branchID, taskID, blockedTask, agentID, staleAt, `{"paths":["internal/parser/**","go.mod"]}`, claimLiberationEvidenceOptions{WithRequest: true, BlockedStatus: model.TaskStatusResolved})

	result, err := store.ReconcileStaleProjectClaimLiberationNarrow(ctx, ClaimLiberationReconcileInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "claim_liberation_watchdog",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reconcile claim liberation: %v", err)
	}
	if result.Scanned != 0 || result.Narrowed != 0 {
		t.Fatalf("terminal blocked task must not remain a liberation candidate, got %+v", result)
	}
	assertClaimLiberationScope(t, ctx, store, workspaceID, taskID, `"go.mod"`, "task_claims")
}

func TestReconcileStaleProjectClaimLiberationSkipsActiveOwnerSession(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-liberation-active-session"
		projectID   = "project-claim-liberation-active-session"
		repoID      = "repo-a"
		branchID    = "branch-claim-liberation-active-session"
		taskID      = "task-claim-liberation-active-session"
		blockedTask = "task-claim-liberation-active-blocked"
		agentID     = "delta-active"
	)
	seedClaimLiberationFixture(t, ctx, store, workspaceID, projectID, repoID, branchID, taskID, agentID, `{"paths":["internal/eval/**","go.mod"]}`, ProjectBranchStatusActive)
	staleAt := time.Now().UTC().Add(-(localOwnershipReclaimGrace + time.Minute)).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE task_claims SET updated_at = ? WHERE workspace_id = ? AND task_id = ?`, staleAt, workspaceID, taskID); err != nil {
		t.Fatalf("age claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE agents SET last_seen_at = ?, updated_at = ? WHERE workspace_id = ? AND agent_id = ?`, staleAt, staleAt, workspaceID, agentID); err != nil {
		t.Fatalf("age agent: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO agent_sessions(session_id, agent_id, workspace_id, task_id, status, started_at)
VALUES ('session-active-owner', ?, ?, ?, 'RUNNING', ?)`,
		agentID, workspaceID, taskID, time.Now().UTC().Format(time.RFC3339Nano),
	); err != nil {
		t.Fatalf("insert active session: %v", err)
	}
	seedClaimLiberationScopeBusyEvidence(t, ctx, store, workspaceID, projectID, branchID, taskID, blockedTask, agentID, staleAt, `{"paths":["internal/parser/**","go.mod"]}`)

	result, err := store.ReconcileStaleProjectClaimLiberationNarrow(ctx, ClaimLiberationReconcileInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "claim_liberation_watchdog",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reconcile claim liberation: %v", err)
	}
	if result.Narrowed != 0 || result.Skipped != 1 || !strings.Contains(result.Items[0].SkipReason, "active_owner_session") {
		t.Fatalf("expected active owner session skip, got %+v", result)
	}
	assertClaimLiberationScope(t, ctx, store, workspaceID, taskID, `"go.mod"`, "task_claims")
}

func TestReconcileStaleProjectClaimLiberationSkipsActiveOwnerExecution(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-liberation-active-execution"
		projectID   = "project-claim-liberation-active-execution"
		repoID      = "repo-a"
		branchID    = "branch-claim-liberation-active-execution"
		taskID      = "task-claim-liberation-active-execution"
		blockedTask = "task-claim-liberation-active-execution-blocked"
		agentID     = "delta-active-execution"
	)
	seedClaimLiberationFixture(t, ctx, store, workspaceID, projectID, repoID, branchID, taskID, agentID, `{"paths":["internal/eval/**","go.mod"]}`, ProjectBranchStatusActive)
	staleAt := time.Now().UTC().Add(-(localOwnershipReclaimGrace + time.Minute)).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE task_claims SET updated_at = ? WHERE workspace_id = ? AND task_id = ?`, staleAt, workspaceID, taskID); err != nil {
		t.Fatalf("age claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE agents SET last_seen_at = ?, updated_at = ? WHERE workspace_id = ? AND agent_id = ?`, staleAt, staleAt, workspaceID, agentID); err != nil {
		t.Fatalf("age agent: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO dag_nodes(node_id, task_id, node_type, status, attempt_count, last_error, created_at, updated_at)
VALUES ('node-active-owner', ?, 'EXECUTE', ?, 0, NULL, ?, ?)`,
		taskID, model.NodeStatusRunning, now, now,
	); err != nil {
		t.Fatalf("insert active dag node: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO node_claims(task_id, node_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at)
VALUES (?, 'node-active-owner', ?, ?, ?, 'active execution evidence', ?, NULL, ?)`,
		taskID, workspaceID, agentID, model.TaskClaimStatusClaimed, now, now,
	); err != nil {
		t.Fatalf("insert active node claim: %v", err)
	}
	seedClaimLiberationScopeBusyEvidence(t, ctx, store, workspaceID, projectID, branchID, taskID, blockedTask, agentID, staleAt, `{"paths":["internal/parser/**","go.mod"]}`)

	result, err := store.ReconcileStaleProjectClaimLiberationNarrow(ctx, ClaimLiberationReconcileInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "claim_liberation_watchdog",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reconcile claim liberation: %v", err)
	}
	if result.Narrowed != 0 || result.Skipped != 1 || !strings.Contains(result.Items[0].SkipReason, "active_owner_execution") {
		t.Fatalf("expected active owner execution skip, got %+v", result)
	}
	assertClaimLiberationScope(t, ctx, store, workspaceID, taskID, `"go.mod"`, "task_claims")
}

func TestReconcileStaleProjectClaimLiberationSkipsOwnedOverlap(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-liberation-owned-overlap"
		projectID   = "project-claim-liberation-owned-overlap"
		repoID      = "repo-a"
		branchID    = "branch-claim-liberation-owned-overlap"
		taskID      = "task-claim-liberation-owned-overlap"
		blockedTask = "task-claim-liberation-owned-blocked"
		agentID     = "delta-owned"
	)
	seedClaimLiberationFixture(t, ctx, store, workspaceID, projectID, repoID, branchID, taskID, agentID, `{"paths":["internal/parser/**","go.mod"]}`, ProjectBranchStatusActive)
	staleAt := time.Now().UTC().Add(-(localOwnershipReclaimGrace + time.Minute)).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE task_claims SET updated_at = ? WHERE workspace_id = ? AND task_id = ?`, staleAt, workspaceID, taskID); err != nil {
		t.Fatalf("age claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE agents SET last_seen_at = ?, updated_at = ? WHERE workspace_id = ? AND agent_id = ?`, staleAt, staleAt, workspaceID, agentID); err != nil {
		t.Fatalf("age agent: %v", err)
	}
	seedClaimLiberationScopeBusyEvidence(t, ctx, store, workspaceID, projectID, branchID, taskID, blockedTask, agentID, staleAt, `{"paths":["internal/parser/**","go.mod"]}`)

	result, err := store.ReconcileStaleProjectClaimLiberationNarrow(ctx, ClaimLiberationReconcileInput{
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "claim_liberation_watchdog",
		ReferenceAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("reconcile claim liberation: %v", err)
	}
	if result.Narrowed != 0 || result.Skipped != 1 || result.Items[0].SkipReason != "owned_overlap_not_safe_for_auto_narrow" {
		t.Fatalf("expected owned-overlap skip, got %+v", result)
	}
	assertClaimLiberationScope(t, ctx, store, workspaceID, taskID, `"go.mod"`, "task_claims")
}

func seedClaimLiberationFixture(t *testing.T, ctx context.Context, store *Store, workspaceID, projectID, repoID, branchID, taskID, agentID, scopeJSON, branchStatus string) {
	t.Helper()
	var existingWorkspace int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&existingWorkspace); err != nil {
		t.Fatalf("query existing workspace: %v", err)
	}
	if existingWorkspace == 0 {
		seedBranchNameResolverProject(t, ctx, store, workspaceID, projectID)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO tasks(task_id, owner_user_id, title, description, status, priority, created_at, updated_at, task_kind, task_template, project_id, requires_project_gate)
VALUES (?, 'developer', ?, '', ?, 'high', ?, ?, 'EXECUTION', 'generic', ?, 1)`,
		taskID, taskID, model.TaskStatusRunning, now, now, projectID,
	); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT OR IGNORE INTO agents(workspace_id, agent_id, owner_user_id, display_name, role, status, protocol_version, capabilities_json, summary, created_at, updated_at, last_seen_at)
VALUES (?, ?, 'developer', ?, 'builder', 'ACTIVE', 'rhizome.v1', '{}', '', ?, ?, ?)`,
		workspaceID, agentID, agentID, now, now, now,
	); err != nil {
		t.Fatalf("insert agent: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT OR IGNORE INTO workspace_tasks(workspace_id, task_id, linked_by, created_at)
VALUES (?, ?, 'test', ?)`,
		workspaceID, taskID, now,
	); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, project_role_id, repo_id, checkout_id, branch_id, write_scope_json, updated_at)
VALUES (?, ?, ?, ?, 'seed claim', ?, NULL, '', ?, '', ?, ?, ?)`,
		taskID, workspaceID, agentID, model.TaskClaimStatusClaimed, now, repoID, branchID, scopeJSON, now,
	); err != nil {
		t.Fatalf("insert task claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO project_branch_registry(
  branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, active_task_id, active_claim_id,
  branch_name, branch_kind, base_branch, head_sha, base_sha, write_scope_json, review_doc_key, status, updated_by, created_at, updated_at
) VALUES (?, ?, ?, ?, '', ?, ?, ?, ?, 'feature', 'main', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', ?, 'review-doc', ?, ?, ?, ?)`,
		branchID, workspaceID, projectID, repoID, agentID, taskID, taskID, "agent/"+agentID+"/"+branchID, scopeJSON, branchStatus, agentID, now, now,
	); err != nil {
		t.Fatalf("insert branch: %v", err)
	}
}

type claimLiberationEvidenceOptions struct {
	WithRequest                  bool
	BlockedStatus                string
	CurrentBlockedWriteScopeJSON string
	EvidenceAgentID              string
	RequestID                    string
}

func seedClaimLiberationScopeBusyEvidence(t *testing.T, ctx context.Context, store *Store, workspaceID, projectID, branchID, taskID, blockedTaskID, agentID, createdAt, blockedScopeJSON string) {
	t.Helper()
	seedClaimLiberationScopeBusyEvidenceWithOptions(t, ctx, store, workspaceID, projectID, branchID, taskID, blockedTaskID, agentID, createdAt, blockedScopeJSON, claimLiberationEvidenceOptions{WithRequest: true})
}

func seedClaimLiberationScopeBusyEvidenceWithOptions(t *testing.T, ctx context.Context, store *Store, workspaceID, projectID, branchID, taskID, blockedTaskID, agentID, createdAt, blockedScopeJSON string, options claimLiberationEvidenceOptions) {
	t.Helper()
	requestID := strings.TrimSpace(options.RequestID)
	if requestID == "" {
		requestID = "req-claim-liberation-release"
	}
	evidenceAgentID := strings.TrimSpace(options.EvidenceAgentID)
	if evidenceAgentID == "" {
		evidenceAgentID = "claim-liberation-observer"
	}
	blockedStatus := strings.TrimSpace(options.BlockedStatus)
	if blockedStatus == "" {
		blockedStatus = model.TaskStatusRunning
	}
	currentBlockedScope := strings.TrimSpace(options.CurrentBlockedWriteScopeJSON)
	if currentBlockedScope == "" {
		currentBlockedScope = blockedScopeJSON
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT OR IGNORE INTO agents(workspace_id, agent_id, owner_user_id, display_name, role, status, protocol_version, capabilities_json, summary, created_at, updated_at, last_seen_at)
VALUES (?, ?, 'developer', ?, 'strategist', 'ACTIVE', 'rhizome.v1', '{}', '', ?, ?, ?)`,
		workspaceID, evidenceAgentID, evidenceAgentID, now, now, now,
	); err != nil {
		t.Fatalf("insert scope busy evidence agent: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT OR IGNORE INTO tasks(task_id, owner_user_id, title, description, status, priority, created_at, updated_at, task_kind, task_template, project_id, requires_project_gate, write_scope_hints_json)
VALUES (?, 'developer', ?, '', ?, 'high', ?, ?, 'EXECUTION', 'generic', ?, 1, ?)`,
		blockedTaskID, blockedTaskID, blockedStatus, now, now, projectID, mustJSON(writeScopePaths(currentBlockedScope)),
	); err != nil {
		t.Fatalf("insert blocked task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE tasks
   SET status = ?, updated_at = ?, project_id = ?, write_scope_hints_json = ?
 WHERE task_id = ?`,
		blockedStatus, now, projectID, mustJSON(writeScopePaths(currentBlockedScope)), blockedTaskID,
	); err != nil {
		t.Fatalf("update blocked task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT OR IGNORE INTO workspace_tasks(workspace_id, task_id, linked_by, created_at)
VALUES (?, ?, 'test', ?)`,
		workspaceID, blockedTaskID, now,
	); err != nil {
		t.Fatalf("insert blocked workspace task: %v", err)
	}
	if options.WithRequest {
		requestPayload := mustJSON(map[string]any{
			"schema":                    "project_claim_owner_resume_request.v1",
			"request_kind":              "project_claim_owner_resume",
			"evidence_kind":             "project_claim_owner_resume_request",
			"project_id":                projectID,
			"task_id":                   taskID,
			"active_task_id":            taskID,
			"blocked_task_id":           blockedTaskID,
			"conflict_branch_id":        branchID,
			"conflict_write_scope_json": blockedScopeJSON,
		})
		if _, err := store.DB().ExecContext(ctx, `
INSERT INTO agent_requests(request_id, workspace_id, from_agent_id, to_agent_id, method, payload, status, response, created_at, responded_at, timeout_sec)
VALUES (?, ?, ?, ?, 'project_claim_owner_resume', ?, 'PENDING', NULL, ?, NULL, 300)`,
			requestID, workspaceID, evidenceAgentID, agentID, requestPayload, createdAt,
		); err != nil {
			t.Fatalf("insert release request: %v", err)
		}
	}
	updateID := "update-claim-liberation-scope-busy"
	payload := mustJSON(map[string]any{
		"schema":                    "project_claim_scope_busy_evidence.v1",
		"evidence_kind":             "project_claim_scope_busy",
		"liberation_candidate_kind": "stale_claim_liberation_narrow_candidate",
		"project_id":                projectID,
		"blocked_task_id":           blockedTaskID,
		"blocked_write_scope_json":  blockedScopeJSON,
		"conflict_owner_agent_id":   agentID,
		"conflict_task_id":          taskID,
		"conflict_branch_id":        branchID,
		"release_request_id":        requestID,
		"release_request_kind":      "project_claim_owner_resume",
	})
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO agent_updates(update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at)
VALUES (?, ?, ?, 'coordination', 'scope busy evidence', ?, 0, ?)`,
		updateID, workspaceID, evidenceAgentID, payload, createdAt,
	); err != nil {
		t.Fatalf("insert scope busy evidence: %v", err)
	}
}

func assertClaimLiberationScope(t *testing.T, ctx context.Context, store *Store, workspaceID, taskID, wantPaths, label string) {
	t.Helper()
	var raw string
	if err := store.DB().QueryRowContext(ctx, `SELECT write_scope_json FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&raw); err != nil {
		t.Fatalf("query %s scope: %v", label, err)
	}
	if !strings.Contains(raw, wantPaths) {
		t.Fatalf("%s scope = %s, want paths %s", label, raw, wantPaths)
	}
}

func assertClaimLiberationBranchScope(t *testing.T, ctx context.Context, store *Store, workspaceID, branchID, wantPaths string) {
	t.Helper()
	var raw string
	if err := store.DB().QueryRowContext(ctx, `SELECT write_scope_json FROM project_branch_registry WHERE workspace_id = ? AND branch_id = ?`, workspaceID, branchID).Scan(&raw); err != nil {
		t.Fatalf("query branch scope: %v", err)
	}
	if !strings.Contains(raw, wantPaths) {
		t.Fatalf("branch scope = %s, want paths %s", raw, wantPaths)
	}
}
