package sqlite_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// Convergence-cluster ROOT-FIX fixtures: the project-phase DONE frontier is "no open
// CONVERGENCE-BLOCKING task", not "no open task". A live roster mints discretionary housekeeping
// indefinitely, so requiring zero tasks meant the gate never admitted. These fixtures prove that
// discretionary work no longer blocks DONE, spec-required work still does, and the external-spec
// coverage gate (Blocker 2) remains an independent hard floor (defense-in-depth) that a
// misclassification cannot bypass.

// createOpenDiscretionaryTask materialises an OPEN (PENDING) coordination-lane housekeeping task with
// the given discretionary tags - the real shape the agent runtime mints (side-effect classification,
// claim/role repair) and that previously kept the frontier "busy" forever.
func createOpenDiscretionaryTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID string, tags ...string) {
	t.Helper()
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, taskID, "coordination", false)
	tagsJSON := `["` + strings.Join(tags, `","`) + `"]`
	if _, err := store.WriteDB().ExecContext(ctx,
		`UPDATE tasks SET task_kind = 'COORDINATION', tags_json = ? WHERE task_id = ?`,
		tagsJSON, taskID); err != nil {
		t.Fatalf("overlay discretionary task fields: %v", err)
	}
}

// FIXTURE 1 (operator-required): a discretionary task is OPEN and every required AC is attested over
// integrated state. DONE is admitted - discretionary housekeeping does not block convergence.
func TestPhaseDoneAdmitsWithDiscretionaryTaskOpenAndCoverageAttested(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-conv-admit"
		projectID   = "project-conv-admit"
		actorID     = "alpha"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)
	publishCoverageDoc(t, ctx, store, workspaceID, projectID, actorID,
		"- AC-LEX-01 | q-front/i-front | lexer integrated",
		"- AC-PARSE-01 | branch-front | parser integrated",
		"- AC-EVAL-01 | backhead000002bb | evaluator integrated",
		"- AC-CLI-01 | task-extspec-back | integrated CLI")
	createOpenDiscretionaryTask(t, ctx, store, workspaceID, projectID, "task-side-effect-classify", "side-effect-classification", "abpc")

	if err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst); err != nil {
		t.Fatalf("expected DONE admitted with an open discretionary task and full coverage, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseDone {
		t.Fatalf("expected phase DONE, got %q", phase)
	}
}

// FIXED-POINT (R65): a genuine ambient-provenance task (coerced OFF implementation lanes by the
// creation-side guard, here on lane=qa) is OPEN and coverage is fully attested. DONE is admitted -
// proactive overproduction is not spec-rooted and does not block. This is the convergence-unblocking
// case at the gate level: the prior round wedged because such proactive work was treated as blocking.
func TestPhaseDoneAdmitsWithAmbientNonImplTaskOpen(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-conv-ambient"
		projectID   = "project-conv-ambient"
		actorID     = "alpha"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)
	publishCoverageDoc(t, ctx, store, workspaceID, projectID, actorID,
		"- AC-LEX-01 | q-front/i-front | lexer integrated",
		"- AC-PARSE-01 | branch-front | parser integrated",
		"- AC-EVAL-01 | backhead000002bb | evaluator integrated",
		"- AC-CLI-01 | task-extspec-back | integrated CLI")
	// A genuine ambient task lands on a non-implementation lane (creation-side coercion).
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, "task-ambient-project-conv-ambient-2ec89f-abc", "qa", false)

	if err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst); err != nil {
		t.Fatalf("expected DONE admitted with an open ambient (non-impl) task + full coverage, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseDone {
		t.Fatalf("expected phase DONE, got %q", phase)
	}
}

// FIXTURE 2 (operator-required): a spec-required BLOCKING task (implementation lane) is open. DONE is
// rejected at the frontier even though coverage is fully attested - blocking work still gates DONE.
func TestPhaseDoneRejectsWithBlockingTaskOpen(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-conv-blocking"
		projectID   = "project-conv-blocking"
		actorID     = "beta"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)
	publishCoverageDoc(t, ctx, store, workspaceID, projectID, actorID,
		"- AC-LEX-01 | q-front/i-front | lexer integrated",
		"- AC-PARSE-01 | branch-front | parser integrated",
		"- AC-EVAL-01 | backhead000002bb | evaluator integrated",
		"- AC-CLI-01 | task-extspec-back | integrated CLI")
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, "task-impl-open", "implementation", false)

	err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst)
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) {
		t.Fatalf("expected frontier rejection for an open blocking task, got %v", err)
	}
	if !strings.Contains(err.Error(), "convergence-blocking task") {
		t.Fatalf("expected reject naming the convergence-blocking task, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseImplementation {
		t.Fatalf("expected phase to stay IMPLEMENTATION after blocking-frontier reject, got %q", phase)
	}
}

// FIXTURE 3 (operator-required, defense-in-depth): a task is classified discretionary (so the frontier
// is clear), but a required AC (AC-CLI-01) is NOT attested. DONE is rejected by the coverage gate -
// even a misclassified task cannot smuggle an uncovered criterion into DONE. The coverage gate is the
// backstop behind the classifier.
func TestPhaseDoneCoverageBackstopsDespiteDiscretionaryFrontier(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-conv-backstop"
		projectID   = "project-conv-backstop"
		actorID     = "gamma"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)
	// Frontier cleared of blocking work (only a discretionary task open) ...
	createOpenDiscretionaryTask(t, ctx, store, workspaceID, projectID, "task-side-effect-classify", "side-effect-classification")
	// ... but coverage is INCOMPLETE: AC-CLI-01 has no attestation entry.
	publishCoverageDoc(t, ctx, store, workspaceID, projectID, actorID,
		"- AC-LEX-01 | q-front/i-front | lexer integrated",
		"- AC-PARSE-01 | branch-front | parser integrated",
		"- AC-EVAL-01 | backhead000002bb | evaluator integrated")

	err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst)
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) {
		t.Fatalf("expected coverage rejection despite a clear (discretionary-only) frontier, got %v", err)
	}
	if !strings.Contains(err.Error(), "AC-CLI-01") {
		t.Fatalf("expected coverage reject naming uncovered AC-CLI-01, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseImplementation {
		t.Fatalf("expected phase to stay IMPLEMENTATION after coverage backstop, got %q", phase)
	}
}

// PARITY (R30 preserved + extended): a spec-less project with only a discretionary task open still
// self-DONEs - discretionary work does not block, and the coverage oracle stays inert with no spec.
func TestPhaseDoneAdmitsSpecLessWithDiscretionaryOpen(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-conv-specless"
		projectID   = "project-conv-specless"
		actorID     = "delta"
	)
	setupW21ProjectInImplementation(t, ctx, store, workspaceID, projectID, actorID)
	createOpenDiscretionaryTask(t, ctx, store, workspaceID, projectID, "task-claim-repair", "project-claim-repair")

	if err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst); err != nil {
		t.Fatalf("expected spec-less project to self-DONE with only a discretionary task open, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseDone {
		t.Fatalf("expected phase DONE for spec-less project, got %q", phase)
	}
}

// POINT #3 (W2.1 terminalization): a discretionary task left open at DONE is not wedged by the DONE
// state - it remains completable (it is not a submit-candidate and not subject to the reflection
// empty-frontier contract), so no dangling-uncompletable task is created by admitting DONE early.
func TestDiscretionaryTaskResolvableAfterProjectDone(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-conv-postdone"
		projectID   = "project-conv-postdone"
		actorID     = "eta"
		taskID      = "task-side-effect-classify"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)
	publishCoverageDoc(t, ctx, store, workspaceID, projectID, actorID,
		"- AC-LEX-01 | q-front/i-front | lexer integrated",
		"- AC-PARSE-01 | branch-front | parser integrated",
		"- AC-EVAL-01 | backhead000002bb | evaluator integrated",
		"- AC-CLI-01 | task-extspec-back | integrated CLI")
	createOpenDiscretionaryTask(t, ctx, store, workspaceID, projectID, taskID, "side-effect-classification")

	if err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst); err != nil {
		t.Fatalf("expected DONE admitted, got %v", err)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: taskID, AgentID: actorID}); err != nil {
		t.Fatalf("expected discretionary task still claimable after DONE, got %v", err)
	}
	if err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     actorID,
		Summary:     "classified side effects; project already converged",
	}); err != nil {
		t.Fatalf("expected discretionary task completion admitted in a DONE project, got %v", err)
	}
}
