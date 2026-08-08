package sqlite_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// R68 keystone (server authority half): a patch-queue REVISION task for a REJECTED candidate whose every
// write-scope path is already covered by an INTEGRATED sibling is redundant over-production and must NOT
// hold the DONE frontier open. The external-spec coverage gate remains the independent hard floor.

const r68TS = "2026-06-15T08:00:00Z"

// insertR68Branch registers a project branch with the given write-scope + status.
func insertR68Branch(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, branchID, scopeJSON, status string) {
	t.Helper()
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO project_branch_registry(
  branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, branch_name, branch_kind,
  base_branch, write_scope_json, status, updated_by, created_at, updated_at
) VALUES (?, ?, ?, 'repo-extspec', '', 'agent-eta', ?, 'feature', 'main', ?, ?, 'agent-eta', ?, ?)`,
		branchID, workspaceID, projectID, branchID, scopeJSON, status, r68TS, r68TS); err != nil {
		t.Fatalf("insert branch %s: %v", branchID, err)
	}
}

// insertR68Item registers a patch-queue item in the given state on the given branch, carrying its own
// pathset_json (the scope source the R68 override reads).
func insertR68Item(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, queueID, itemID, branchID, state, pathsetJSON string) {
	t.Helper()
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO project_patch_queue_items
  (queue_id, item_id, workspace_id, project_id, repo_id, branch_id, review_doc_key, state, pathset_json, head_sha, task_id, created_at, updated_at)
VALUES (?, ?, ?, ?, 'repo-extspec', ?, ?, ?, ?, ?, ?, ?, ?)`,
		queueID, itemID, workspaceID, projectID, branchID, "review-"+itemID, state, pathsetJSON, "head"+itemID, "task-"+itemID, r68TS, r68TS); err != nil {
		t.Fatalf("insert patch-queue item %s (%s): %v", itemID, state, err)
	}
}

// insertR68RevisionIdentity links a revision task to the candidate item it revises.
func insertR68RevisionIdentity(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID, queueID, itemID, branchID string) {
	t.Helper()
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO task_patch_queue_identities(task_id, workspace_id, project_id, queue_id, item_id, branch_id, head_sha, kind, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, 'revision', ?, ?)`,
		taskID, workspaceID, projectID, queueID, itemID, branchID, "head"+itemID, r68TS, r68TS); err != nil {
		t.Fatalf("insert revision identity for %s: %v", taskID, err)
	}
}

// insertR68Repo registers the repo-extspec project repository (the FK parent for branch/item rows) for a
// project that was set up without seedExternalSpecProject (e.g. a spec-less project).
func insertR68Repo(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID string) {
	t.Helper()
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO project_repositories(repo_id, workspace_id, project_id, remote_url, remote_kind, name, default_branch, repo_status, is_canonical, created_at, updated_at)
VALUES ('repo-extspec', ?, ?, 'file:///repo-extspec', 'local', 'repo-extspec', 'main', 'READY', 1, ?, ?)`,
		workspaceID, projectID, r68TS, r68TS); err != nil {
		t.Fatalf("insert repo: %v", err)
	}
}

// seedR68RejectedRevision wires the common shape: an integrated CLI sibling (whose ITEM pathset covers
// cmd/** + the evaluator package), a REJECTED duplicate candidate whose ITEM pathset is candidatePathset,
// and a blocking implementation-lane revision task targeting the rejected candidate. Coverage is fully
// attested so only the frontier predicate decides admission. The branch rows exist for FK/realism but
// their scope is irrelevant to the override (which reads the item pathset). The integrated sibling's
// branch is MERGED (the realistic state that previously hid its scope from the agent).
func seedR68RejectedRevision(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID, candidatePathset string) {
	t.Helper()
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)
	publishCoverageDoc(t, ctx, store, workspaceID, projectID, actorID,
		"- AC-LEX-01 | q-front/i-front | lexer integrated",
		"- AC-PARSE-01 | branch-front | parser integrated",
		"- AC-EVAL-01 | backhead000002bb | evaluator integrated",
		"- AC-CLI-01 | task-extspec-back | integrated CLI")

	// Integrated CLI sibling (branch MERGED): item pathset covers cmd/** and the evaluator package.
	insertR68Branch(t, ctx, store, workspaceID, projectID, "branch-cli", `{"paths":["cmd/**"]}`, "MERGED")
	insertR68Item(t, ctx, store, workspaceID, projectID, "queue-rq", "item-cli", "branch-cli", "INTEGRATED", `["cmd/**","internal/cli/**","internal/evaluator/**"]`)

	// Rejected duplicate candidate.
	insertR68Branch(t, ctx, store, workspaceID, projectID, "branch-16139", `{"paths":["cmd/rq"]}`, "READY_FOR_REVIEW")
	insertR68Item(t, ctx, store, workspaceID, projectID, "queue-rq", "item-16139", "branch-16139", "REJECTED", candidatePathset)

	// Blocking implementation-lane revision task targeting the rejected candidate.
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, "task-patchq-revision-16139", "implementation", false)
	insertR68RevisionIdentity(t, ctx, store, workspaceID, projectID, "task-patchq-revision-16139", "queue-rq", "item-16139", "branch-16139")
}

// REDUNDANT: every candidate path is covered by the integrated sibling -> the revision is discounted,
// the frontier is clear, coverage passes, DONE is admitted (no authority-transition machinery touched).
func TestPhaseDoneAdmitsWithRedundantRejectedRevisionOpen(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-r68-admit"
		projectID   = "project-r68-admit"
		actorID     = "eta"
	)
	seedR68RejectedRevision(t, ctx, store, workspaceID, projectID, actorID,
		`["cmd/rq/main.go","internal/evaluator/evaluator.go"]`)

	if err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst); err != nil {
		t.Fatalf("expected DONE admitted with only a redundant rejected-revision open, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseDone {
		t.Fatalf("expected phase DONE, got %q", phase)
	}
}

// DIRECTIONAL TRIPWIRE (red-team HIGH regression): a BROAD candidate directory (cmd/rq) is NOT covered by
// a NARROWER integrated file beneath it (cmd/rq/version.go). The override must keep the revision blocking
// (the broad candidate carries genuinely-uncovered files) -> DONE rejected at the frontier.
func TestPhaseDoneRejectsBroadCandidateOverNarrowIntegratedSibling(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-r68-dir"
		projectID   = "project-r68-dir"
		actorID     = "eta"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)
	publishCoverageDoc(t, ctx, store, workspaceID, projectID, actorID,
		"- AC-LEX-01 | q-front/i-front | lexer integrated",
		"- AC-PARSE-01 | branch-front | parser integrated",
		"- AC-EVAL-01 | backhead000002bb | evaluator integrated",
		"- AC-CLI-01 | task-extspec-back | integrated CLI")
	// Narrow integrated sibling: item pathset is only one concrete file under cmd/rq.
	insertR68Branch(t, ctx, store, workspaceID, projectID, "branch-narrow", `{"paths":["cmd/rq/version.go"]}`, "MERGED")
	insertR68Item(t, ctx, store, workspaceID, projectID, "queue-rq", "item-narrow", "branch-narrow", "INTEGRATED", `["cmd/rq/version.go"]`)
	// Broad rejected candidate: item pathset is the whole cmd/rq directory (includes uncovered files).
	insertR68Branch(t, ctx, store, workspaceID, projectID, "branch-broad", `{"paths":["cmd/rq"]}`, "READY_FOR_REVIEW")
	insertR68Item(t, ctx, store, workspaceID, projectID, "queue-rq", "item-broad", "branch-broad", "REJECTED", `["cmd/rq"]`)
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, "task-patchq-revision-broad", "implementation", false)
	insertR68RevisionIdentity(t, ctx, store, workspaceID, projectID, "task-patchq-revision-broad", "queue-rq", "item-broad", "branch-broad")

	err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst)
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) {
		t.Fatalf("expected frontier rejection for a broad candidate over a narrow integrated sibling, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseImplementation {
		t.Fatalf("expected phase to stay IMPLEMENTATION, got %q", phase)
	}
}

// B1 REGRESSION (spec-less): a SPEC-LESS project must NOT honor the R68 override (its coverage floor is
// inert, so the override would be an unbacked false-DONE vector). The redundant rejected-revision is fully
// covered, but the override is gated OFF for spec-less projects, so the impl-lane revision still blocks and
// DONE is rejected at the frontier.
func TestPhaseDoneSpecLessDoesNotHonorRedundantRevisionOverride(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-r68-specless"
		projectID   = "project-r68-specless"
		actorID     = "eta"
	)
	setupW21ProjectInImplementation(t, ctx, store, workspaceID, projectID, actorID) // spec-less: no source_refs
	insertR68Repo(t, ctx, store, workspaceID, projectID)
	insertR68Branch(t, ctx, store, workspaceID, projectID, "branch-cli", `{"paths":["cmd/**"]}`, "MERGED")
	insertR68Item(t, ctx, store, workspaceID, projectID, "queue-rq", "item-cli", "branch-cli", "INTEGRATED", `["cmd/**","internal/evaluator/**"]`)
	insertR68Branch(t, ctx, store, workspaceID, projectID, "branch-16139", `{"paths":["cmd/rq"]}`, "READY_FOR_REVIEW")
	insertR68Item(t, ctx, store, workspaceID, projectID, "queue-rq", "item-16139", "branch-16139", "REJECTED", `["cmd/rq/main.go","internal/evaluator/evaluator.go"]`)
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, "task-patchq-revision-16139", "implementation", false)
	insertR68RevisionIdentity(t, ctx, store, workspaceID, projectID, "task-patchq-revision-16139", "queue-rq", "item-16139", "branch-16139")

	err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst)
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) {
		t.Fatalf("spec-less project must NOT honor the R68 override (no coverage floor); the revision must keep blocking, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseImplementation {
		t.Fatalf("expected phase to stay IMPLEMENTATION, got %q", phase)
	}
}

// TRIPWIRE: a candidate path (internal/uncovered/x.go) has NO integrated sibling -> the revision is NOT
// discounted (it could be the only path to an AC), the frontier stays open, DONE is rejected. Coverage
// would also backstop, but the frontier check fires first.
func TestPhaseDoneRejectsWhenRejectedRevisionLaneNotFullyIntegrated(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-r68-reject"
		projectID   = "project-r68-reject"
		actorID     = "eta"
	)
	seedR68RejectedRevision(t, ctx, store, workspaceID, projectID, actorID,
		`["cmd/rq/main.go","internal/uncovered/x.go"]`)

	err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst)
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) {
		t.Fatalf("expected frontier rejection when a candidate path is uncovered, got %v", err)
	}
	if !strings.Contains(err.Error(), "convergence-blocking task") {
		t.Fatalf("expected reject naming the convergence-blocking task, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseImplementation {
		t.Fatalf("expected phase to stay IMPLEMENTATION, got %q", phase)
	}
}
