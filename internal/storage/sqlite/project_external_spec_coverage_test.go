package sqlite_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// Blocker 2 (convergence cluster) fixtures. The external-spec convergence oracle makes DONE mean
// "every operator-rooted acceptance criterion is covered", not merely "no open task". The
// required-set (denominator) is the acceptance_criteria_refs list in the sealed
// source_requirements_trace; the covered-set is the strategic lead's typed DONE-evidence
// attestation doc, each entry verified against an EXISTING integrated patch-queue artifact. The
// gate reads acceptance_criteria_mapping NOWHERE (it is empty by construction).
//
// Design: plans/strategic/2026-06-14-convergence-cluster-design.md (Blocker 2, Option A).

const (
	extSpecSourceDocKey = "operator.signal01.spec"
)

func extSpecRefsDocKey(projectID string) string { return "project." + projectID + ".source_refs" }
func extSpecTraceDocKey(projectID string) string {
	return "project." + projectID + ".source_requirements_trace"
}
func extSpecCoverageDocKey(projectID string) string {
	return "project." + projectID + ".acceptance_coverage"
}

// seedExternalSpecProject brings a project to an empty IMPLEMENTATION frontier with the operator
// source-fidelity chain seeded (source_refs + a complete source_requirements_trace whose
// acceptance_criteria_refs are the four signal01 AC-IDs) and two INTEGRATED patch-queue artifacts.
// Critically it sets NO acceptance_criteria_mapping anywhere: it even materialises a RESOLVED
// implementation task with an empty task_requirements_json to prove the coverage gate admits while
// the per-task machine map is empty (no livelock, no dependence on the empty field).
func seedExternalSpecProject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID string) {
	t.Helper()
	setupW21ProjectInImplementation(t, ctx, store, workspaceID, projectID, actorID)

	upsertDoc := func(docKey, title, content string) {
		if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      docKey,
			Title:       title,
			Content:     content,
			UpdatedBy:   actorID,
		}); err != nil {
			t.Fatalf("upsert doc %s: %v", docKey, err)
		}
	}

	upsertDoc(extSpecSourceDocKey, "operator spec", strings.Join([]string{
		"# Signal-01 rq checkpoint spec",
		"AC-LEX-01: lexer produces positioned tokens.",
		"AC-PARSE-01: parser produces AST.",
		"AC-EVAL-01: evaluator runs expressions.",
		"AC-CLI-01: integrated product exposes a runnable Go CLI.",
	}, "\n"))

	upsertDoc(extSpecRefsDocKey(projectID), "source refs", strings.Join([]string{
		"```rhizome_source_refs_v1",
		"source_doc_keys:",
		"- " + extSpecSourceDocKey,
		"```",
	}, "\n"))

	upsertDoc(extSpecTraceDocKey(projectID), "source requirements trace", strings.Join([]string{
		"```rhizome_source_requirements_trace_v1",
		"source_doc_keys:",
		"- " + extSpecSourceDocKey,
		"acceptance_critical_anchors:",
		"- AC-LEX-01: lexer produces positioned tokenization",
		"- AC-PARSE-01: parser produces AST construction",
		"- AC-EVAL-01: evaluator runs JSON input evaluation",
		"- AC-CLI-01: integrated product exposes a runnable Go CLI",
		"acceptance_criteria_refs:",
		"- AC-LEX-01",
		"- AC-PARSE-01",
		"- AC-EVAL-01",
		"- AC-CLI-01",
		"adjacent_wrong_products:",
		"- stock scaffold with no rq behavior",
		"- isolated lane treated as full product",
		"non_goals:",
		"- full jq compatibility",
		"```",
	}, "\n"))

	// A RESOLVED implementation task with an EMPTY acceptance_criteria_mapping, plus its integrated
	// artifact. This is the real shape the operator confirmed: tasks complete and integrate, the
	// per-task machine map is never populated. The coverage gate must admit regardless.
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, "task-extspec-core", "implementation", false)
	if _, err := store.WriteDB().ExecContext(ctx,
		`UPDATE tasks SET status = 'RESOLVED', task_requirements_json = '{}' WHERE task_id = ?`,
		"task-extspec-core"); err != nil {
		t.Fatalf("resolve core task with empty mapping: %v", err)
	}

	// FK parents for the integrated patch-queue items: one repo and the two integrated branches.
	const ts = "2026-06-14T10:00:00Z"
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO project_repositories(repo_id, workspace_id, project_id, remote_url, remote_kind, name, default_branch, repo_status, is_canonical, created_at, updated_at)
VALUES (?, ?, ?, ?, 'local', ?, 'main', 'READY', 1, ?, ?)`,
		"repo-extspec", workspaceID, projectID, "file:///repo-extspec", "repo-extspec", ts, ts); err != nil {
		t.Fatalf("insert repo: %v", err)
	}
	insertBranch := func(branchID string) {
		if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO project_branch_registry(
  branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, branch_name, branch_kind,
  base_branch, write_scope_json, status, updated_by, created_at, updated_at
) VALUES (?, ?, ?, ?, '', ?, ?, 'feature', 'main', '{"paths":["src/**"]}', 'INTEGRATED', ?, ?, ?)`,
			branchID, workspaceID, projectID, "repo-extspec", actorID, branchID, actorID, ts, ts); err != nil {
			t.Fatalf("insert branch %s: %v", branchID, err)
		}
	}
	insertBranch("branch-front")
	insertBranch("branch-back")
	// An integrated branch whose id is itself AC-ID-shaped (qa-* parses as an AC-ID token). A lead
	// may legitimately reference it as an artifact; the gate must NOT drop it as if it were an AC-ID.
	insertBranch("qa-smoke-01")

	insertIntegrated := func(queueID, itemID, branchID, headSHA, taskID string) {
		if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO project_patch_queue_items
  (queue_id, item_id, workspace_id, project_id, repo_id, branch_id, review_doc_key, state, head_sha, task_id, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			queueID, itemID, workspaceID, projectID, "repo-extspec", branchID, "review-"+itemID,
			sqlite.ProjectPatchQueueStateIntegrated, headSHA, taskID, ts, ts); err != nil {
			t.Fatalf("insert integrated item %s/%s: %v", queueID, itemID, err)
		}
	}
	insertIntegrated("q-front", "i-front", "branch-front", "fronthead00001aa", "task-extspec-core")
	insertIntegrated("q-back", "i-back", "branch-back", "backhead000002bb", "task-extspec-back")
	insertIntegrated("q-qa", "i-qa", "qa-smoke-01", "qahead0000003cc", "task-extspec-core")
}

// FIXTURE 1 (decisive, anti-livelock): integrated artifacts exist, acceptance_criteria_mapping is
// EMPTY everywhere, and the lead publishes a full DONE-evidence attestation covering all four
// required AC-IDs (each via a different artifact-ref style: pair key, branch, head, task). DONE is
// admitted. This proves the gate is independent of the empty machine field - the predicted
// machine-coverage deadlock does NOT reproduce.
func TestExternalSpecCoverageAdmitsWithFullAttestationEmptyMapping(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-extspec-admit"
		projectID   = "project-extspec-admit"
		actorID     = "alpha"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      extSpecCoverageDocKey(projectID),
		Title:       "DONE evidence",
		UpdatedBy:   actorID,
		Content: strings.Join([]string{
			"```rhizome_acceptance_coverage_v1",
			"- AC-LEX-01 | q-front/i-front | lexer integrated into shared module, positioned tokens shipped",
			"- AC-PARSE-01 | branch-front | parser AST integrated on the front lane",
			"- AC-EVAL-01 | backhead000002bb | evaluator runs JSON input, integrated on back lane",
			"- AC-CLI-01 | task-extspec-back | integrated CLI exposes a runnable Go binary",
			"```",
		}, "\n"),
	}); err != nil {
		t.Fatalf("publish attestation: %v", err)
	}

	if err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst); err != nil {
		t.Fatalf("expected DONE admitted with full attestation over integrated artifacts (empty mapping), got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseDone {
		t.Fatalf("expected phase DONE, got %q", phase)
	}
}

// FIXTURE 2 (anti-self-lowering): the frontier is empty (easy work drained) and the trace is still
// complete, but one required AC-ID (AC-CLI-01) has NO attestation entry. DONE is rejected, naming
// the uncovered criterion - even under trust-first (the coverage gate is unconditional; only the
// strategic-lead authority gate is bypassed by trust-first).
func TestExternalSpecCoverageRejectsUncoveredCriterion(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-extspec-uncovered"
		projectID   = "project-extspec-uncovered"
		actorID     = "beta"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      extSpecCoverageDocKey(projectID),
		Title:       "DONE evidence (incomplete)",
		UpdatedBy:   actorID,
		Content: strings.Join([]string{
			"```rhizome_acceptance_coverage_v1",
			"- AC-LEX-01 | q-front/i-front | lexer integrated",
			"- AC-PARSE-01 | branch-front | parser integrated",
			"- AC-EVAL-01 | backhead000002bb | evaluator integrated",
			"```",
		}, "\n"),
	}); err != nil {
		t.Fatalf("publish partial attestation: %v", err)
	}

	err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst)
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) {
		t.Fatalf("expected completion-contract rejection for uncovered criterion, got %v", err)
	}
	if !strings.Contains(err.Error(), "AC-CLI-01") || !strings.Contains(err.Error(), "no entry") {
		t.Fatalf("expected reject naming uncovered AC-CLI-01 with no-entry reason, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseImplementation {
		t.Fatalf("expected phase to stay IMPLEMENTATION after uncovered reject, got %q", phase)
	}
}

// FIXTURE 3 (attestation cannot be a free pass): every required AC-ID is attested, but AC-CLI-01's
// artifact reference points to no existing integrated artifact. DONE is rejected, naming the
// criterion and the dangling reference - the attestation must anchor to real integrated state.
func TestExternalSpecCoverageRejectsNonexistentArtifact(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-extspec-dangling"
		projectID   = "project-extspec-dangling"
		actorID     = "gamma"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      extSpecCoverageDocKey(projectID),
		Title:       "DONE evidence (dangling ref)",
		UpdatedBy:   actorID,
		Content: strings.Join([]string{
			"```rhizome_acceptance_coverage_v1",
			"- AC-LEX-01 | q-front/i-front | lexer integrated",
			"- AC-PARSE-01 | branch-front | parser integrated",
			"- AC-EVAL-01 | backhead000002bb | evaluator integrated",
			"- AC-CLI-01 | q-ghost/i-ghost | claims a CLI item that was never integrated",
			"```",
		}, "\n"),
	}); err != nil {
		t.Fatalf("publish dangling attestation: %v", err)
	}

	err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst)
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) {
		t.Fatalf("expected completion-contract rejection for dangling artifact, got %v", err)
	}
	if !strings.Contains(err.Error(), "AC-CLI-01") || !strings.Contains(err.Error(), "no INTEGRATED") {
		t.Fatalf("expected reject naming AC-CLI-01 with no-INTEGRATED-artifact reason, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseImplementation {
		t.Fatalf("expected phase to stay IMPLEMENTATION after dangling-artifact reject, got %q", phase)
	}
}

// PARITY (R30 self-DONE preserved): a spec-less project (no source_refs doc) with an empty
// frontier still reaches DONE - the coverage oracle is inert where no operator spec exists, so the
// pre-existing frontier-only self-completion is unchanged.
func TestExternalSpecCoverageInertForSpecLessProject(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-extspec-specless"
		projectID   = "project-extspec-specless"
		actorID     = "delta"
	)
	setupW21ProjectInImplementation(t, ctx, store, workspaceID, projectID, actorID)

	if err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst); err != nil {
		t.Fatalf("expected spec-less project to self-DONE on empty frontier, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseDone {
		t.Fatalf("expected phase DONE for spec-less project, got %q", phase)
	}
}

func publishCoverageDoc(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID string, entries ...string) {
	t.Helper()
	lines := append([]string{"```rhizome_acceptance_coverage_v1"}, entries...)
	lines = append(lines, "```")
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      extSpecCoverageDocKey(projectID),
		Title:       "DONE evidence",
		UpdatedBy:   actorID,
		Content:     strings.Join(lines, "\n"),
	}); err != nil {
		t.Fatalf("publish attestation: %v", err)
	}
}

// REGRESSION (anti-shrink, red-team CRITICAL): the seal forces acceptance_critical_anchors to be
// present but never forces acceptance_criteria_refs to enumerate every anchor. A trace whose refs
// list a SUBSET of its anchors must NOT let the bar collapse to that subset - the denominator is
// the UNION of refs and anchors. Here refs name only AC-LEX-01 while the anchors name all four;
// attesting only AC-LEX-01 must still reject on an anchor-only criterion.
func TestExternalSpecCoverageDenominatorIncludesAnchors(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-extspec-union"
		projectID   = "project-extspec-union"
		actorID     = "epsilon"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)

	// Re-publish the trace with refs SHRUNK to a single AC-ID while anchors keep all four. This
	// still passes the seal (refs present + specific, anchors present), so it models a trace an
	// adversarial/sloppy author could ship.
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      extSpecTraceDocKey(projectID),
		Title:       "source requirements trace (refs shrunk)",
		UpdatedBy:   actorID,
		Content: strings.Join([]string{
			"```rhizome_source_requirements_trace_v1",
			"source_doc_keys:",
			"- " + extSpecSourceDocKey,
			"acceptance_critical_anchors:",
			"- AC-LEX-01: lexer produces positioned tokenization",
			"- AC-PARSE-01: parser produces AST construction",
			"- AC-EVAL-01: evaluator runs JSON input evaluation",
			"- AC-CLI-01: integrated product exposes a runnable Go CLI",
			"acceptance_criteria_refs:",
			"- AC-LEX-01",
			"adjacent_wrong_products:",
			"- stock scaffold with no rq behavior",
			"non_goals:",
			"- full jq compatibility",
			"```",
		}, "\n"),
	}); err != nil {
		t.Fatalf("re-upsert shrunk trace: %v", err)
	}

	publishCoverageDoc(t, ctx, store, workspaceID, projectID, actorID,
		"- AC-LEX-01 | q-front/i-front | lexer integrated")

	err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst)
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) {
		t.Fatalf("expected reject: anchor-only criteria are part of the denominator, got %v", err)
	}
	if !strings.Contains(err.Error(), "AC-PARSE-01") && !strings.Contains(err.Error(), "AC-EVAL-01") && !strings.Contains(err.Error(), "AC-CLI-01") {
		t.Fatalf("expected reject naming an anchor-only criterion, got %v", err)
	}
}

// REGRESSION (anti false-reject, red-team FINDING 1/2): an integrated artifact whose id is itself
// AC-ID-shaped (branch "qa-smoke-01") must be matchable when a lead references it in the artifact
// column - it must NOT be mistaken for an AC-ID and dropped from the candidate refs.
func TestExternalSpecCoverageMatchesAcIdShapedArtifactRef(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-extspec-acshaped"
		projectID   = "project-extspec-acshaped"
		actorID     = "zeta"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)

	publishCoverageDoc(t, ctx, store, workspaceID, projectID, actorID,
		"- AC-LEX-01 | q-front/i-front | lexer integrated",
		"- AC-PARSE-01 | branch-front | parser integrated",
		"- AC-EVAL-01 | backhead000002bb | evaluator integrated",
		"- AC-CLI-01 | qa-smoke-01 | CLI smoke covered by the integrated qa-smoke-01 branch")

	if err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst); err != nil {
		t.Fatalf("expected DONE admitted with an AC-ID-shaped artifact ref, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseDone {
		t.Fatalf("expected phase DONE, got %q", phase)
	}
}

// REGRESSION (anti self-lowering, red-team HIGH): a word in the free-text justification column must
// NOT become a phantom artifact ref. Here AC-CLI-01's artifact column is a dangling ref while its
// justification mentions a real integrated branch ("branch-front"); the gate must reject, because
// only the artifact column may satisfy the existence check.
func TestExternalSpecCoverageJustificationIsNotArtifactRef(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-extspec-justprose"
		projectID   = "project-extspec-justprose"
		actorID     = "eta"
	)
	seedExternalSpecProject(t, ctx, store, workspaceID, projectID, actorID)

	publishCoverageDoc(t, ctx, store, workspaceID, projectID, actorID,
		"- AC-LEX-01 | q-front/i-front | lexer integrated",
		"- AC-PARSE-01 | branch-front | parser integrated",
		"- AC-EVAL-01 | backhead000002bb | evaluator integrated",
		"- AC-CLI-01 | q-ghost/i-ghost | the CLI reuses branch-front output but its own item never integrated")

	err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst)
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) {
		t.Fatalf("expected reject: justification prose must not satisfy the artifact check, got %v", err)
	}
	if !strings.Contains(err.Error(), "AC-CLI-01") || !strings.Contains(err.Error(), "no INTEGRATED") {
		t.Fatalf("expected AC-CLI-01 artifact-missing reject, got %v", err)
	}
}
