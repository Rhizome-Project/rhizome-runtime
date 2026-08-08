package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// Stage-4 decisive-path continuation goldens. These exercise the NEW enactment machinery that the migrated
// happy-path tests do not reach: the periodic sweep (DEFERRED -> CONSUMED with the role-acquire HOOK removed),
// idempotency under repeated sweeps, the bounded-defer TTL escalation to a typed terminal, the DEFERRED-vs-
// SUPPRESSED re-attempt split, and the #4(b) integration-continuation defer-then-materialize unfreeze path.
// The knob across all of them is whether the satisfying role exists at materialization / sweep time.

func goldenContinuationSetup(t *testing.T, ctx context.Context, store *sqlite.Store, suffix string) (ws, proj, repo, lead, owner, reviewer, integratorCandidate string) {
	t.Helper()
	ws = "ws-dp-cont-" + suffix
	proj = "project-dp-cont-" + suffix
	repo = "repo-main"
	lead = "alpha"
	owner = "beta"
	reviewer = "reviewer"
	integratorCandidate = "eta"
	seedAgentWorkWorkspace(t, ctx, store, ws, []string{lead, owner, reviewer, integratorCandidate})
	createProjectForGitTest(t, ctx, store, ws, proj, lead)
	claimProjectLeadForGitTest(t, ctx, store, ws, proj, lead)
	assignAgentWorkProjectRole(t, ctx, store, ws, proj, reviewer, sqlite.ProjectRoleReviewer, lead)
	upsertRepositoryForGitTest(t, ctx, store, ws, proj, repo, lead)
	return ws, proj, repo, lead, owner, reviewer, integratorCandidate
}

func goldenContinuationState(t *testing.T, ctx context.Context, store *sqlite.Store, ws, proj string, item sqlite.ProjectPatchQueueItemRecord) sqlite.ProjectPatchQueueDecisionContinuationRecord {
	t.Helper()
	conts, err := store.ListProjectPatchQueueDecisionContinuations(ctx, sqlite.ProjectPatchQueueDecisionContinuationFilter{
		WorkspaceID: ws, ProjectID: proj, QueueID: item.QueueID, ItemID: item.ItemID,
	})
	if err != nil {
		t.Fatalf("list continuations: %v", err)
	}
	if len(conts) != 1 {
		t.Fatalf("expected exactly one continuation, got %d: %+v", len(conts), conts)
	}
	return conts[0]
}

func goldenWorkspaceTaskExists(t *testing.T, ctx context.Context, store *sqlite.Store, ws, taskID string) bool {
	t.Helper()
	tasks, err := store.ListWorkspaceTasks(ctx, ws)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	for _, task := range tasks {
		if task.TaskID == taskID {
			return true
		}
	}
	return false
}

func goldenWorkspaceTaskCount(t *testing.T, ctx context.Context, store *sqlite.Store, ws string) int {
	t.Helper()
	tasks, err := store.ListWorkspaceTasks(ctx, ws)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	return len(tasks)
}

// I2 (sweep-primary liveness) + I3 (idempotency): a revision continuation born without a claimable owner role
// DEFERS, then the periodic sweep ALONE (no role-acquire hook) materializes it once the role appears, and a
// second sweep does NOT double-mint.
func TestPatchQueueDeferredRevisionContinuationMaterializesViaSweepAloneThenIdempotent(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ws, proj, repo, lead, owner, reviewer, _ := goldenContinuationSetup(t, ctx, store, "sweep-materialize")

	// Owner holds NO implementer role yet -> the BLOCKED decision's revision continuation route-classifies
	// awaiting_role and DEFERS (no unclaimable task minted).
	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, ws, proj, repo, "branch-sweep-materialize", owner, reviewer, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	cont := goldenContinuationState(t, ctx, store, ws, proj, item)
	if cont.State != "DEFERRED" {
		t.Fatalf("revision continuation with no claimable owner must DEFER, got %+v", cont)
	}
	taskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(proj, item, "revision")
	if goldenWorkspaceTaskExists(t, ctx, store, ws, taskID) {
		t.Fatalf("deferred continuation must NOT mint a task, but %s exists", taskID)
	}

	// The awaited role appears. The PRIMARY re-attempt mechanism is the periodic sweep, NOT a role-acquire hook:
	// liveness must hold with only the sweep. Assign the role, then run the sweep alone.
	assignProjectImplementerRoleForGitTest(t, ctx, store, ws, proj, owner, lead, `{"paths":["src/**"]}`)
	if err := store.ReconcileDeferredProjectPatchQueueContinuations(ctx, ws); err != nil {
		t.Fatalf("sweep deferred continuations: %v", err)
	}
	cont = goldenContinuationState(t, ctx, store, ws, proj, item)
	if cont.State != "CONSUMED" {
		t.Fatalf("sweep alone must materialize the now-satisfiable continuation (CONSUMED), got %+v", cont)
	}
	if !goldenWorkspaceTaskExists(t, ctx, store, ws, taskID) {
		t.Fatalf("sweep must mint the claimable revision task %s", taskID)
	}

	// I3: a second sweep (or a sweep racing the role-acquire hook) must NOT double-mint.
	before := goldenWorkspaceTaskCount(t, ctx, store, ws)
	if err := store.ReconcileDeferredProjectPatchQueueContinuations(ctx, ws); err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if after := goldenWorkspaceTaskCount(t, ctx, store, ws); after != before {
		t.Fatalf("double sweep must mint exactly once: workspace task count %d -> %d", before, after)
	}
}

// I1 (bounded defer) + #10 (DEFERRED != SUPPRESSED): a defer whose awaited role never appears escalates to a
// typed terminal (SUPPRESSED, no task) once its TTL is exhausted, and the sweep thereafter SKIPS the SUPPRESSED
// row (intentionally inert) even if a role later appears - it is never revived.
func TestPatchQueueDeferredContinuationTTLEscalatesToTypedTerminalThenSweepSkipsSuppressed(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ws, proj, repo, lead, owner, reviewer, _ := goldenContinuationSetup(t, ctx, store, "ttl-terminal")

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, ws, proj, repo, "branch-ttl-terminal", owner, reviewer, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	cont := goldenContinuationState(t, ctx, store, ws, proj, item)
	if cont.State != "DEFERRED" {
		t.Fatalf("continuation must DEFER, got %+v", cont)
	}
	taskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(proj, item, "revision")

	// Backdate the defer clock (updated_at) past the bounded-defer TTL (2h). The awaited role never appears.
	staleAt := time.Now().UTC().Add(-3 * time.Hour).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE project_patch_queue_decision_continuation_outbox SET updated_at = ? WHERE outbox_id = ?`, staleAt, cont.OutboxID); err != nil {
		t.Fatalf("backdate defer clock: %v", err)
	}
	if err := store.ReconcileDeferredProjectPatchQueueContinuations(ctx, ws); err != nil {
		t.Fatalf("sweep TTL: %v", err)
	}
	cont = goldenContinuationState(t, ctx, store, ws, proj, item)
	if cont.State != "SUPPRESSED" {
		t.Fatalf("TTL-exhausted defer must escalate to typed-terminal (SUPPRESSED), got %+v", cont)
	}
	if goldenWorkspaceTaskExists(t, ctx, store, ws, taskID) {
		t.Fatalf("typed-terminal must mint NO task (fence by construction)")
	}

	// The sweep scans DEFERRED only; a SUPPRESSED row is intentionally inert and must be left untouched even if a
	// role now exists. Assign the role, sweep again, assert SUPPRESSED stays and still no task.
	assignProjectImplementerRoleForGitTest(t, ctx, store, ws, proj, owner, lead, `{"paths":["src/**"]}`)
	if err := store.ReconcileDeferredProjectPatchQueueContinuations(ctx, ws); err != nil {
		t.Fatalf("sweep after suppress: %v", err)
	}
	cont = goldenContinuationState(t, ctx, store, ws, proj, item)
	if cont.State != "SUPPRESSED" {
		t.Fatalf("sweep must SKIP suppressed rows (not revive), got %+v", cont)
	}
	if goldenWorkspaceTaskExists(t, ctx, store, ws, taskID) {
		t.Fatalf("suppressed continuation must never mint a task")
	}
}

// #4(b): an integration continuation born when NO active INTEGRATOR exists must DEFER (survive), NOT born-terminal
// - an integrator may appear later and the integration must not be permanently lost. Once an integrator appears,
// the sweep alone materializes the integration carrier (this is the integration-unfreeze path).
func TestPatchQueueIntegrationContinuationDefersWithoutIntegratorThenSweepMaterializes(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ws, proj, repo, lead, owner, reviewer, integratorCandidate := goldenContinuationSetup(t, ctx, store, "integration-defer")

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, ws, proj, repo, "branch-integration-defer", owner, reviewer, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateAccepted)
	cont := goldenContinuationState(t, ctx, store, ws, proj, item)
	if cont.FollowupKind != "integration" {
		t.Fatalf("accepted decision must route an integration continuation, got %+v", cont)
	}
	if cont.State != "DEFERRED" {
		t.Fatalf("integration continuation with no active INTEGRATOR must DEFER (survive, not terminal), got %+v", cont)
	}
	taskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(proj, item, "integration")
	if goldenWorkspaceTaskExists(t, ctx, store, ws, taskID) {
		t.Fatalf("deferred integration continuation must NOT mint a task")
	}

	// An INTEGRATOR appears; the sweep alone materializes the integration carrier.
	assignAgentWorkProjectRole(t, ctx, store, ws, proj, integratorCandidate, sqlite.ProjectRoleIntegrator, lead)
	if err := store.ReconcileDeferredProjectPatchQueueContinuations(ctx, ws); err != nil {
		t.Fatalf("sweep integration continuation: %v", err)
	}
	cont = goldenContinuationState(t, ctx, store, ws, proj, item)
	if cont.State != "CONSUMED" {
		t.Fatalf("sweep alone must materialize the integration continuation once an integrator exists, got %+v", cont)
	}
	if !goldenWorkspaceTaskExists(t, ctx, store, ws, taskID) {
		t.Fatalf("sweep must mint the integration task %s", taskID)
	}
}

// A1 fault-isolation + single-mint-chokepoint: a permanently poisoned DEFERRED continuation (its deterministic id
// squatted by a NON-reusable task, so the mint chokepoint's reuse pre-check rejects it every cycle) must NOT wedge
// the sweep. The sweep journals+skips the poison, still materializes healthy rows in the SAME pass (so the
// work-next slot it runs in is never wedged), and the bounded TTL eventually drains the poison to a typed
// terminal. Pre-fix the sweep aborted on the first per-row error and propagated it into work-next = fleet wedge.
func TestPatchQueueSweepPoisonedContinuationDoesNotWedgeAndTTLDrains(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ws, proj, repo, lead, poisonOwner, reviewer, healthyOwner := goldenContinuationSetup(t, ctx, store, "sweep-poison")

	// Poison row: a BLOCKED item whose revision continuation DEFERS (owner has no IMPLEMENTER yet). Squat the
	// deterministic continuation id with a task carrying a MISMATCHED outbox binding (non-reusable).
	poison := createAgentWorkDecidedPatchQueueItem(t, ctx, store, ws, proj, repo, "branch-poison", poisonOwner, reviewer, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	poisonCont := findPatchQueueContinuationByDecision(t, ctx, store, poison, sqlite.ProjectPatchQueueStateBlocked)
	if poisonCont.State != "DEFERRED" {
		t.Fatalf("poison continuation must start DEFERRED, got %+v", poisonCont)
	}
	poisonTaskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(proj, poison, "revision")
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID: ws, TaskID: poisonTaskID, OwnerUserID: reviewer, Priority: "high",
		Title: "squatting poison task", Description: "stale task reusing the deterministic continuation id with the wrong outbox binding",
		TaskKind: "EXECUTION", TaskTemplate: "generic", ProjectID: proj, ProjectLane: "implementation", RequiresProjectGate: true,
		TaskRequirementsJSON: `{"patch_queue_task_kind":"revision","project_id":"` + proj + `","queue_id":"` + poison.QueueID + `","item_id":"` + poison.ItemID + `","branch_id":"` + poison.BranchID + `","head_sha":"` + poison.HeadSHA + `","decision":"BLOCKED","decision_outbox_id":"wrong-outbox"}`,
	}, dag.DefaultGraph()); err != nil {
		t.Fatalf("inject squatting poison task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: ws, TaskID: poisonTaskID, LinkedBy: reviewer}); err != nil {
		t.Fatalf("attach poison task: %v", err)
	}

	// Healthy row: a second BLOCKED item whose revision continuation DEFERS, then becomes satisfiable.
	healthy := createAgentWorkDecidedPatchQueueItem(t, ctx, store, ws, proj, repo, "branch-healthy", healthyOwner, reviewer, `{"paths":["web/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	healthyTaskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(proj, healthy, "revision")

	// Both owners acquire IMPLEMENTER, so on the sweep both rows route satisfiable_now: the poison reaches the mint
	// chokepoint (and errors on the non-reusable squatter), the healthy one mints cleanly.
	assignProjectImplementerRoleForGitTest(t, ctx, store, ws, proj, poisonOwner, lead, `{"paths":["src/**"]}`)
	assignProjectImplementerRoleForGitTest(t, ctx, store, ws, proj, healthyOwner, lead, `{"paths":["web/**"]}`)

	// The sweep must NOT propagate the poison error, and must still materialize the healthy row past it.
	if err := store.ReconcileDeferredProjectPatchQueueContinuations(ctx, ws); err != nil {
		t.Fatalf("sweep must not propagate a per-row poison error (fault isolation), got %v", err)
	}
	if c := findPatchQueueContinuationByDecision(t, ctx, store, healthy, sqlite.ProjectPatchQueueStateBlocked); c.State != "CONSUMED" {
		t.Fatalf("sweep must materialize the healthy row past the poison, got %+v", c)
	}
	if !goldenWorkspaceTaskExists(t, ctx, store, ws, healthyTaskID) {
		t.Fatalf("healthy revision carrier %s must be minted", healthyTaskID)
	}
	if c := findPatchQueueContinuationByDecision(t, ctx, store, poison, sqlite.ProjectPatchQueueStateBlocked); c.State != "DEFERRED" {
		t.Fatalf("poison row must be skipped (left DEFERRED), not crashed/consumed, got %+v", c)
	}

	// The work-next slot the sweep runs in is NOT wedged by the poison.
	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{WorkspaceID: ws, AgentID: healthyOwner, CoordinationMode: "trust_first"}); err != nil {
		t.Fatalf("work-next must not be wedged by a poisoned continuation, got %v", err)
	}

	// Bounded TTL drains the permanently-poisoned row to a typed terminal.
	staleAt := time.Now().UTC().Add(-3 * time.Hour).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE project_patch_queue_decision_continuation_outbox SET updated_at = ? WHERE outbox_id = ?`, staleAt, poisonCont.OutboxID); err != nil {
		t.Fatalf("backdate poison defer clock: %v", err)
	}
	if err := store.ReconcileDeferredProjectPatchQueueContinuations(ctx, ws); err != nil {
		t.Fatalf("TTL sweep: %v", err)
	}
	if c := findPatchQueueContinuationByDecision(t, ctx, store, poison, sqlite.ProjectPatchQueueStateBlocked); c.State != "SUPPRESSED" {
		t.Fatalf("bounded TTL must escalate the poisoned row to a typed terminal (SUPPRESSED), got %+v", c)
	}
}

// Migration drain (the R13-R25 #6 backlog): the OLD code stranded revision/validation continuations in PENDING
// with no consumer. The sweep now scans PENDING too and re-routes via the modality. Crucially, a PENDING row's
// updated_at is its (possibly ancient) CREATION time, NOT a defer clock - so PENDING must SKIP the TTL-terminal
// shortcut and materialize when satisfiable, not get wrongly suppressed.
func TestPatchQueueSweepDrainsStrandedPendingContinuationWhenSatisfiable(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ws, proj, repo, lead, owner, reviewer, _ := goldenContinuationSetup(t, ctx, store, "pending-drain-now")

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, ws, proj, repo, "branch-pending-drain", owner, reviewer, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	cont := findPatchQueueContinuationByDecision(t, ctx, store, item, sqlite.ProjectPatchQueueStateBlocked)
	taskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(proj, item, "revision")

	// Simulate the legacy backlog: force the row to PENDING with an updated_at far older than the 2h TTL.
	ancient := time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE project_patch_queue_decision_continuation_outbox SET state='PENDING', updated_at=? WHERE outbox_id=?`, ancient, cont.OutboxID); err != nil {
		t.Fatalf("force stranded PENDING: %v", err)
	}

	// The owner holds IMPLEMENTER, so the modality says satisfiable_now: the sweep must DRAIN the stranded PENDING
	// to CONSUMED + mint the task - NOT TTL-terminalize it despite the ancient updated_at.
	assignProjectImplementerRoleForGitTest(t, ctx, store, ws, proj, owner, lead, `{"paths":["src/**"]}`)
	if err := store.ReconcileDeferredProjectPatchQueueContinuations(ctx, ws); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	cont = findPatchQueueContinuationByDecision(t, ctx, store, item, sqlite.ProjectPatchQueueStateBlocked)
	if cont.State != "CONSUMED" {
		t.Fatalf("sweep must DRAIN a satisfiable stranded PENDING continuation to CONSUMED (not TTL-terminal), got %+v", cont)
	}
	if !goldenWorkspaceTaskExists(t, ctx, store, ws, taskID) {
		t.Fatalf("sweep must mint the revision task when draining a satisfiable PENDING continuation")
	}
}

// Companion: a stranded PENDING continuation whose owner is NOT yet satisfiable must be transitioned PENDING ->
// DEFERRED (starting the bounded-TTL clock fresh), NOT TTL-terminalized off its ancient creation time.
func TestPatchQueueSweepDefersStrandedPendingContinuationWhenAwaitingRole(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ws, proj, repo, _, owner, reviewer, _ := goldenContinuationSetup(t, ctx, store, "pending-drain-await")

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, ws, proj, repo, "branch-pending-await", owner, reviewer, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	cont := findPatchQueueContinuationByDecision(t, ctx, store, item, sqlite.ProjectPatchQueueStateBlocked)
	taskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(proj, item, "revision")

	ancient := time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `UPDATE project_patch_queue_decision_continuation_outbox SET state='PENDING', updated_at=? WHERE outbox_id=?`, ancient, cont.OutboxID); err != nil {
		t.Fatalf("force stranded PENDING: %v", err)
	}

	// Owner holds NO implementer role -> modality awaiting_role. The sweep must move PENDING -> DEFERRED (fresh
	// clock), NOT suppress it via the ancient updated_at, and mint NO task.
	if err := store.ReconcileDeferredProjectPatchQueueContinuations(ctx, ws); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	cont = findPatchQueueContinuationByDecision(t, ctx, store, item, sqlite.ProjectPatchQueueStateBlocked)
	if cont.State != "DEFERRED" {
		t.Fatalf("sweep must move an awaiting stranded PENDING continuation to DEFERRED (fresh TTL), not suppress it, got %+v", cont)
	}
	if goldenWorkspaceTaskExists(t, ctx, store, ws, taskID) {
		t.Fatalf("awaiting PENDING drain must mint no task")
	}
}

// A6 read==write parity (empirically confirmed). The read-frontier OFFERS a role-routed integration continuation
// to a profile/registered eligible owner REGARDLESS of claim mode, but in STRICT mode the claim's role-binding
// gate used to veto auto-provision for a role-routed (non-owner-bound) integration task whose owner no longer
// holds an ACTIVE project INTEGRATOR role -> offered-but-rejected on the #4(b) integration-unfreeze path. The
// claim must now auto-provision an ELIGIBLE owner (gated on eligibility, not claim mode), honoring the offer.
func TestProjectPatchQueueStrictIntegrationContinuationAutoProvisionsEligibleOwner(t *testing.T) {
	t.Parallel()
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	item := createAcceptedControlledPatchQueueItemForTest(t, ctx, store, "a6-strict-integ")
	const integratorID = "delta"

	// delta is a REGISTERED integrator (auto-provision-eligible) and holds an ACTIVE project INTEGRATOR role at
	// materialization time, so the accepted item's integration continuation materializes OWNED BY delta.
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{WorkspaceID: item.WorkspaceID, AgentID: integratorID, OwnerUserID: "developer", DisplayName: integratorID, Role: "integrator"}); err != nil {
		t.Fatalf("register integrator: %v", err)
	}
	assignAgentWorkProjectRole(t, ctx, store, item.WorkspaceID, item.ProjectID, integratorID, sqlite.ProjectRoleIntegrator, "alpha")
	if err := store.ReconcileDeferredProjectPatchQueueContinuations(ctx, item.WorkspaceID); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	integrationTaskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(item.ProjectID, item, "integration")
	if !goldenWorkspaceTaskExists(t, ctx, store, item.WorkspaceID, integrationTaskID) {
		t.Fatalf("integration continuation must materialize owned by the integrator")
	}

	// All active project EXECUTION roles then lapse (the integrator rotated away and the reviewer's role expired),
	// leaving NO active execution role on the project and delta as the OWNER of the integration task with only
	// registered-integrator eligibility. This is the A6-reproducing cell: read-frontier OFFERS (eligible), strict
	// claim used to REJECT. (With another active execution role present, read and claim agree to deny - parity.)
	if _, err := store.DB().ExecContext(ctx, `UPDATE project_agent_roles SET status = ? WHERE workspace_id = ? AND project_id = ? AND role_type != ?`,
		"EXPIRED", item.WorkspaceID, item.ProjectID, "STRATEGIC_LEAD"); err != nil {
		t.Fatalf("expire project execution roles: %v", err)
	}

	// STRICT-mode claim (no supplied project_role_id) must now ADMIT via eligibility auto-provision, not reject
	// with "requires project_role_id" - otherwise the integration the read offered is stranded (A6).
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           item.WorkspaceID,
		TaskID:                integrationTaskID,
		AgentID:               integratorID,
		CoordinationMode:      sqlite.CoordinationModeStrict,
		Summary:               "claim integration continuation in strict mode via eligibility auto-provision",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(item.WorkspaceID, integrationTaskID, integratorID),
	}); err != nil {
		t.Fatalf("strict-mode integration claim must auto-provision an eligible owner the read already offered (A6), got %v", err)
	}
}
