package sqlite_test

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// W2.1 fixtures proving the unified terminal-admission predicate's NEW closures fire (the existing
// 19k-line suite proves no regression + ADMISSION#1 reflection reject + suppression parity; these
// lock in the behaviours that did not exist pre-W2.1). Design:
// plans/strategic/2026-06-13-w21-unified-terminal-admission-design.md.

func setupW21ProjectInImplementation(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID string) {
	t.Helper()
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{actorID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "W2.1 terminal admission",
		Description: "unified terminal-admission predicate fixtures",
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	// Satisfy the implementation gate so a lane=implementation task is claimable in the fixture.
	designDocID := "doc.design.w21"
	planDocID := "doc.plan.w21"
	repoRequired := false
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &planDocID,
		RepoRequired:            &repoRequired,
		ActorID:                 actorID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("upsert project profile: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               actorID,
		ActorID:               actorID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "lead for terminal-admission fixtures",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim strategic lead: %v", err)
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "open implementation phase for fixtures",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition to implementation: %v", err)
	}
}

func transitionPhaseToDone(ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID, coordinationMode string) error {
	_, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseDone,
		Reason:                "attempt convergence",
		ActorID:               actorID,
		ActorType:             "agent",
		CoordinationMode:      coordinationMode,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.phase.transition",
	})
	return err
}

func projectCurrentPhase(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID string) string {
	t.Helper()
	profile, err := store.GetProjectProfile(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get project profile: %v", err)
	}
	return profile.CurrentPhase
}

// P06: the DUAL HOLE behind R44. project.phase -> DONE must satisfy the completion frontier (no open
// non-reflection task) REGARDLESS of coordination mode; trust-first bypasses only the strategic-lead
// authority, never completion.
func TestProjectPhaseDoneRequiresEmptyNonReflectionFrontier(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-w21-phase-done"
		projectID   = "project-w21-phase-done"
		actorID     = "epsilon"
		openTaskID  = "task-w21-open-implementation"
	)
	setupW21ProjectInImplementation(t, ctx, store, workspaceID, projectID, actorID)
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, openTaskID, "implementation", false)

	// trust-first must NOT bypass the completion contract.
	err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst)
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) || !strings.Contains(err.Error(), "DONE") {
		t.Fatalf("expected phase-DONE completion contract rejection under trust-first, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseImplementation {
		t.Fatalf("expected phase to stay IMPLEMENTATION after rejected DONE, got %q", phase)
	}

	// Clear the frontier (operator/agent cancels the stale task), then DONE is admitted.
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET status = ? WHERE task_id = ?`, model.TaskStatusCancelled, openTaskID); err != nil {
		t.Fatalf("cancel open task: %v", err)
	}
	if err := transitionPhaseToDone(ctx, store, workspaceID, projectID, actorID, sqlite.CoordinationModeTrustFirst); err != nil {
		t.Fatalf("expected DONE admitted once frontier is clear, got %v", err)
	}
	if phase := projectCurrentPhase(t, ctx, store, workspaceID, projectID); phase != sqlite.ProjectPhaseDone {
		t.Fatalf("expected phase DONE after frontier cleared, got %q", phase)
	}
}

// ADMISSION#2 (NEW W2.1): the substantive R44 closure. An EXECUTION/implementation carrier with a
// non-empty acceptance contract may RESOLVE only if it produced its own patch-queue artifact OR is
// covered by integrated state. A no-evidence heartbeat is Rejected; once an own identity exists it
// is Admitted (the live-improvement / real-work survives guard).
func TestCompleteTaskRejectsExecutionCarrierWithoutOwnEvidence(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-w21-exec-carrier"
		projectID   = "project-w21-exec-carrier"
		actorID     = "beta"
		taskID      = "task-w21-exec-carrier"
	)
	setupW21ProjectInImplementation(t, ctx, store, workspaceID, projectID, actorID)
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, taskID, "implementation", false)
	// Make it a submit-candidate: non-empty acceptance_criteria_mapping.
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET task_requirements_json = ? WHERE task_id = ?`,
		`{"acceptance_criteria_mapping":["lambda map filter criterion","arithmetic precedence criterion"]}`, taskID); err != nil {
		t.Fatalf("set acceptance criteria mapping: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: taskID, AgentID: actorID}); err != nil {
		t.Fatalf("claim exec carrier: %v", err)
	}

	// Heartbeat completion with no own patch-queue identity and no integrated coverage -> Reject.
	err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     actorID,
		Summary:     "looks done from the repo, recording completion",
	})
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) || !strings.Contains(err.Error(), "execution carrier") {
		t.Fatalf("expected execution-carrier completion contract rejection, got %v", err)
	}
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)

	// Give it its own patch-queue artifact (did real work) -> Admit.
	now := "2026-06-13T12:00:00Z"
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO task_patch_queue_identities(task_id, workspace_id, project_id, queue_id, item_id, branch_id, head_sha, kind, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		taskID, workspaceID, projectID, "queue-w21", "item-w21", "branch-w21", "deadbeefcafe", "integration", now, now); err != nil {
		t.Fatalf("insert own patch-queue identity: %v", err)
	}
	if err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     actorID,
		Summary:     "submitted the deliverable to the patch queue",
	}); err != nil {
		t.Fatalf("expected completion admitted with own patch-queue identity, got %v", err)
	}
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusResolved)
}

// CI GUARD (anti-R44 invariant, mechanical): every function that writes a terminal task status or a
// project phase must be in an explicit allowlist. A NEW ungated terminal writer fails the build and
// forces the author to route it through evaluateTerminalAdmissionTx (or justify an allowlist entry).
// Keys on the FUNCTION boundary (not a SQL literal) because the terminal value is a bound parameter
// (status = ?, current_phase = ?) the guard cannot read statically (design H2).
//
// SCOPE (known, documented limitation - not a current exploit): the scan inspects single string
// literals in this package's non-test .go files only. A terminal write split across concatenated
// literals, built via fmt.Sprintf, or added in another Go package would escape. This is acceptable
// today because a repo-wide search confirms ZERO production terminal task-status / phase writers
// exist outside internal/storage/sqlite, and every current writer is a single literal. If terminal
// writes ever move cross-package or get composed dynamically, widen this scan accordingly.
func TestNoUngatedTerminalWriteEscapesAllowlist(t *testing.T) {
	t.Parallel()

	// All functions permitted to issue `UPDATE tasks ... SET status ...`. Terminal-status writers
	// are gated by evaluateTerminalAdmissionTx (or allowlisted with rationale); the rest write only
	// RUNNING/PENDING (non-terminal dispatch/release) and are not completion writes.
	taskStatusAllowlist := map[string]string{
		"terminalizeAgentWorkRequiredReceiptTask": "P01 reaper - SUPPRESSION via evaluateTerminalAdmissionTx; CAS-protected",
		"transitionTaskClaimWithEvent":            "P02 agent.task.complete/fail/cancel - ADMISSION via evaluateTerminalAdmissionTx",
		"closeTaskTx":                             "P03 task.close - ADMISSION via evaluateTerminalAdmissionTx (RESOLVED); CANCELLED short-circuits",
		"updateTaskStatusFromNodesTx":             "P05 node aggregation - ADMISSION via evaluateTerminalAdmissionTx (RESOLVED); the P04 path's sole gate",
		"terminalizeIntegratedPatchQueueWorkTx":   "P09 integration cascade - gated by upstream INTEGRATED git-push proof; head-only tightening is a documented follow-up (OD#2)",
		// Non-terminal task-status writers (RUNNING/PENDING dispatch + release); not completion writes.
		// (completeNodeClaimWithEvent intentionally absent: W2.1 removed its direct task-status write;
		// its node-completion terminal now flows solely through updateTaskStatusFromNodesTx (P05).)
		"releaseTaskClaimWithAuthorityActorTx":  "P18 claim release RUNNING->PENDING (abandonment, not completion)",
		"claimNodeWithEvent":                    "claim dispatch PENDING->RUNNING (non-terminal)",
		"claimTaskWithEvent":                    "claim dispatch PENDING->RUNNING (non-terminal)",
		"ClaimExecutableNodes":                  "claim dispatch PENDING->RUNNING (non-terminal)",
		"transferTaskClaimForSessionTakeoverTx": "session-takeover touch PENDING->RUNNING (non-terminal)",
	}
	phaseAllowlist := map[string]string{
		"TransitionProjectPhaseWithEvent": "P06 phase transition - DONE gated by evaluateProjectPhaseTerminalAdmissionTx; trust-first bypasses authority only",
		"ensureProjectProfileTx":          "initial profile creation writes current_phase=INTAKE (non-terminal)",
	}

	taskWriters := scanFunctionsWritingSQL(t, func(sql string) bool {
		return strings.Contains(sql, "UPDATE tasks") && strings.Contains(sql, "status")
	})
	phaseWriters := scanFunctionsWritingSQL(t, func(sql string) bool {
		// Require a write verb so SELECT ... current_phase FROM project_profiles readers are excluded.
		return strings.Contains(sql, "current_phase") &&
			(strings.Contains(sql, "UPDATE project_profiles") || strings.Contains(sql, "INSERT INTO project_profiles") || strings.Contains(sql, "INSERT OR")) &&
			strings.Contains(sql, "project_profiles")
	})

	assertWritersAllowlisted(t, "task-status", taskWriters, taskStatusAllowlist)
	assertWritersAllowlisted(t, "project-phase", phaseWriters, phaseAllowlist)
}

func assertWritersAllowlisted(t *testing.T, kind string, writers map[string][]string, allowlist map[string]string) {
	t.Helper()
	names := make([]string, 0, len(writers))
	for fn := range writers {
		names = append(names, fn)
	}
	sort.Strings(names)
	for _, fn := range names {
		if _, ok := allowlist[fn]; !ok {
			t.Errorf("UNGATED %s terminal write: function %q (%s) issues an UPDATE not in the allowlist; route it through evaluateTerminalAdmissionTx or add a justified allowlist entry. Sites: %s",
				kind, fn, "internal/storage/sqlite", strings.Join(writers[fn], ", "))
		}
	}
	t.Logf("%s writers discovered: %v", kind, names)
}

// scanFunctionsWritingSQL parses every non-test .go file in the package dir and returns the set of
// top-level function/method names whose body contains a string literal matching `match`, mapped to
// the file:line sites for diagnostics.
func scanFunctionsWritingSQL(t *testing.T, match func(string) bool) map[string][]string {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	out := map[string][]string{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				value := lit.Value
				if len(value) >= 2 {
					value = value[1 : len(value)-1] // strip quotes/backticks
				}
				if match(value) {
					pos := fset.Position(lit.Pos())
					out[fn.Name.Name] = append(out[fn.Name.Name], filepath.Base(pos.Filename)+":"+itoa(pos.Line))
				}
				return true
			})
		}
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
