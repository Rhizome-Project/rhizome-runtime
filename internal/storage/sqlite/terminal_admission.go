package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// W2.1 - the unified terminal-admission predicate.
//
// Every terminal write path (task/claim/node/phase -> terminal; patch-queue cascade) is gated by
// ONE dispatch function, evaluateTerminalAdmissionTx, instead of the historical museum of ~18
// per-path recipes scattered across half a dozen call sites. R44 proved per-path gates leak: a
// heartbeat completion slipped through agent.task.complete because that path did not consult the
// completion contract that the idle-reflection path did. A missed path is mission failure.
//
// The predicate reads ONE invariant - "is this lane's completion contract satisfied by durable
// state?" - in two directions (the exact dual of agent's EmptyProductFrontier):
//
//   - ADMISSION (caller-driven completion, e.g. P02/P03/P04/P05/P06): "may I close MYSELF because
//     MY contract is met (or I declare none / I am an abandonment release)?" Default Admit; a
//     registered contract may Reject. Subsumes/generalizes enforceEmptyProductFrontierCompletionTx.
//   - SUPPRESSION (reaper sweep P01, integration cascade P09): "must I close THIS still-open carrier
//     because the contract is met by SOMEONE ELSE's durable state and the carrier adds nothing of
//     its own?" This is agentWorkRequiredTerminalReceipt verbatim, re-shaped.
//
// Design + golden-translation + red-team + fixtures + lead decisions:
// All terminal transitions pass through this shared admission path.

// TerminalDecision is the verdict of the unified terminal-admission predicate.
type TerminalDecision int

const (
	// TerminalAdmit allows the caller's terminal write (admission) or is a suppression no-op.
	TerminalAdmit TerminalDecision = iota
	// TerminalReject refuses a caller-driven terminal write (completion contract unsatisfied).
	TerminalReject
	// TerminalTerminalize proactively terminalizes a still-open carrier (suppression side).
	TerminalTerminalize
)

// AdmissionSide selects the direction of the single coverage evaluation.
type AdmissionSide int

const (
	// SideAdmission is the caller-driven completion question: "may I close myself?".
	SideAdmission AdmissionSide = iota
	// SideSuppression is the reaper/cascade question: "must I close this proxy duplicate?".
	SideSuppression
)

// IntentKind classifies the terminal write so release/park paths short-circuit to Admit and are
// never gated as completions nor suppressed as duplicates.
type IntentKind int

const (
	// GenuineCompletion: an agent/caller claims this lane is complete.
	GenuineCompletion IntentKind = iota
	// SuppressionSweep: the reaper/cascade scans for carriers satisfied by others' durable state.
	SuppressionSweep
	// AbandonmentRelease: a claim release (RUNNING->PENDING); never a completion.
	AbandonmentRelease
	// NonTerminalPark: a block/park; non-terminal, the predicate is not consulted for a verdict.
	NonTerminalPark
)

// TerminalOrigin records which write path invoked the predicate (audit only).
type TerminalOrigin string

const (
	OriginP01 TerminalOrigin = "P01" // reaper required-receipt sweep
	OriginP02 TerminalOrigin = "P02" // transitionTaskClaimWithEvent (agent.task.complete/fail/cancel)
	OriginP03 TerminalOrigin = "P03" // closeTaskTx (task.close / operator close)
	OriginP04 TerminalOrigin = "P04" // completeNodeClaimWithEvent last-node auto-resolve
	OriginP05 TerminalOrigin = "P05" // updateTaskStatusFromNodesTx aggregation
	OriginP06 TerminalOrigin = "P06" // project phase -> DONE
	OriginP09 TerminalOrigin = "P09" // integration terminalization cascade
)

// Stable ReasonCode values stamped on EVERY verdict (including default-admits) for an observable
// audit trail. Suppression verdicts carry the legacy receipt.Kind as ReasonCode/ContractID.
const (
	reasonLiveCompletion           = "live_completion"
	reasonReleaseNotCompletion     = "release_not_completion"
	reasonNonTerminalPark          = "non_terminal_park"
	reasonNoContractDeclared       = "no_contract_declared"
	reasonEmptyProductFrontierOpen = "empty_product_frontier_open"
	reasonExecCarrierNoOwnEvidence = "execution_carrier_no_own_evidence"
	reasonExecCarrierOwnEvidence   = "execution_carrier_own_evidence"
	reasonExecCarrierCovered       = "execution_carrier_covered"
	reasonPhaseFrontierOpen        = "phase_frontier_open"
	reasonPhaseFrontierClear       = "phase_frontier_clear"
	reasonNonTerminalPhase         = "non_terminal_phase_transition"

	// External-spec convergence oracle (Blocker 2): DONE means "operator spec covered", not
	// merely "no open task". Every verdict stamps one of these so a coverage-less Admit is
	// auditable and a reject names which guarantee fired.
	reasonExternalSpecInert           = "external_spec_inert"            // spec-less project; frontier-only self-DONE preserved
	reasonExternalSpecContextBroken   = "external_spec_context_broken"   // source_refs archived/empty -> fail closed
	reasonExternalSpecTraceRegressed  = "external_spec_trace_regressed"  // trace seal no longer holds at DONE
	reasonExternalSpecNoCriteria      = "external_spec_no_criteria"      // Required trace but zero canonical AC-IDs
	reasonExternalSpecUncovered       = "external_spec_uncovered"        // required AC-ID has no attestation entry
	reasonExternalSpecArtifactMissing = "external_spec_artifact_missing" // attestation names no existing integrated artifact
	reasonExternalSpecCovered         = "external_spec_covered"          // every required AC-ID attested against integrated state

	contractEmptyProductFrontier      = "empty_product_frontier_completion"
	contractExecutionCarrier          = "execution_carrier_own_evidence_or_coverage"
	contractProjectPhaseDone          = "project_phase_done_frontier"
	contractProjectAcceptanceCoverage = "project_external_spec_acceptance_coverage"
)

// TerminalWriteIntent describes the terminal write a call site wants to perform.
type TerminalWriteIntent struct {
	Side             AdmissionSide
	Kind             IntentKind
	Resolution       string // target terminal status (RESOLVED/FAILED/CANCELLED/DONE)
	Origin           TerminalOrigin
	ActorID          string
	ActorType        string
	ClaimState       string // current claim status, when a claim exists
	CoordinationMode string // carried so P06 composes authority-vs-completion correctly
}

// TerminalAdmission is the verdict. ReasonCode/ContractID are populated on every verdict.
type TerminalAdmission struct {
	Decision   TerminalDecision
	Resolution string
	ReasonCode string
	ContractID string
	Summary    string
	AgentID    string
	Payload    map[string]any
	Err        error // wraps ErrTaskCompletionContract when Decision==TerminalReject

	// suppressionReceipt carries the legacy receipt for the reaper writer so
	// terminalizeAgentWorkRequiredReceiptTask stays byte-identical (verdict-preserving lift).
	suppressionReceipt *agentWorkRequiredTerminalReceipt
}

// evaluateTerminalAdmissionTx is the single gate every terminal write path consults.
func (s *Store) evaluateTerminalAdmissionTx(ctx context.Context, tx *sql.Tx, workspaceID string, task WorkspaceTaskRecord, intent TerminalWriteIntent) (TerminalAdmission, error) {
	switch intent.Kind {
	case AbandonmentRelease:
		return TerminalAdmission{Decision: TerminalAdmit, ReasonCode: reasonReleaseNotCompletion}, nil
	case NonTerminalPark:
		return TerminalAdmission{Decision: TerminalAdmit, ReasonCode: reasonNonTerminalPark}, nil
	}
	if intent.Side == SideSuppression {
		return s.evaluateTerminalSuppressionTx(ctx, tx, workspaceID, task, intent)
	}
	return s.evaluateTerminalCompletionAdmissionTx(ctx, tx, workspaceID, task, intent)
}

// evaluateTerminalSuppressionTx wraps the existing receipt registry verbatim. It reads s.db (the
// 20-conn read pool), exactly as the reaper does today; suppression safety is the caller's
// optimistic CAS (status=? AND updated_at=?), NOT snapshot sharing (design C1). The 7 direct
// checks then the 13 tabular entries run in their existing order with first-match short-circuit
// preserved inside agentWorkRequiredTerminalReceipt, so verdicts are byte-identical.
func (s *Store) evaluateTerminalSuppressionTx(ctx context.Context, tx *sql.Tx, workspaceID string, task WorkspaceTaskRecord, intent TerminalWriteIntent) (TerminalAdmission, error) {
	receipt, ok, err := s.agentWorkRequiredTerminalReceipt(ctx, workspaceID, task)
	if err != nil {
		return TerminalAdmission{}, err
	}
	if !ok {
		return TerminalAdmission{Decision: TerminalAdmit, ReasonCode: reasonNoContractDeclared}, nil
	}
	carried := receipt
	return TerminalAdmission{
		Decision:           TerminalTerminalize,
		Resolution:         receipt.Resolution,
		ReasonCode:         receipt.Kind,
		ContractID:         receipt.Kind,
		Summary:            receipt.Summary,
		AgentID:            receipt.AgentID,
		Payload:            receipt.Payload,
		suppressionReceipt: &carried,
	}, nil
}

// evaluateTerminalCompletionAdmissionTx is the admission side: "may this caller-driven terminal
// write proceed?". Reads run against the open tx (q=tx) so coverage/own-identity see the in-flight
// snapshot (resolves design C1 for admission). RESOLVED-only consults a contract (OD#1):
// FAILED/CANCELLED and any non-RESOLVED resolution are genuine terminals and short-circuit to Admit.
func (s *Store) evaluateTerminalCompletionAdmissionTx(ctx context.Context, tx *sql.Tx, workspaceID string, task WorkspaceTaskRecord, intent TerminalWriteIntent) (TerminalAdmission, error) {
	if intent.Resolution != model.TaskStatusResolved {
		return TerminalAdmission{Decision: TerminalAdmit, ReasonCode: reasonLiveCompletion}, nil
	}

	// ADMISSION#1 - empty-frontier reflection contract (R44/R27, byte-for-byte; reloads task by id).
	// An idle-reflection lane may resolve only when the project is DONE or a non-reflection sibling
	// remains open; a heartbeat with neither is Rejected.
	if err := s.enforceEmptyProductFrontierCompletionTx(ctx, tx, workspaceID, task.TaskID); err != nil {
		if errors.Is(err, ErrTaskCompletionContract) {
			return TerminalAdmission{
				Decision:   TerminalReject,
				ReasonCode: reasonEmptyProductFrontierOpen,
				ContractID: contractEmptyProductFrontier,
				Err:        err,
			}, nil
		}
		return TerminalAdmission{}, err
	}

	// ADMISSION#2 (NEW W2.1) - execution-carrier own-evidence-or-coverage. The substantive R44
	// closure: an EXECUTION/implementation carrier with a non-empty acceptance contract may RESOLVE
	// only if it produced its own patch-queue artifact (own identity, any state == did real work) OR
	// integrated state already covers its acceptance contract. A no-evidence heartbeat satisfies
	// neither and is Rejected. Non-submit-candidates (coordination/planning/doc completions, no
	// acceptance mapping) Admit unchanged - the precise false-positive guard.
	full := task
	if strings.TrimSpace(full.TaskRequirementsJSON) == "" || strings.TrimSpace(full.ProjectLane) == "" || strings.TrimSpace(full.TaskKind) == "" {
		loaded, ok, err := s.loadTerminalAdmissionTaskTx(ctx, tx, task.TaskID)
		if err != nil {
			return TerminalAdmission{}, err
		}
		if ok {
			full = loaded
		}
	}
	if !taskCompletionCoverageSubmitCandidate(full) {
		return TerminalAdmission{Decision: TerminalAdmit, ReasonCode: reasonLiveCompletion}, nil
	}
	hasIdentity, err := s.taskHasPatchQueueIdentityWithQuerier(ctx, tx, workspaceID, full.TaskID)
	if err != nil {
		return TerminalAdmission{}, err
	}
	if hasIdentity {
		return TerminalAdmission{Decision: TerminalAdmit, ReasonCode: reasonExecCarrierOwnEvidence}, nil
	}
	_, covered, err := s.taskIntegratedAcceptanceCoverageReceipt(ctx, tx, workspaceID, strings.TrimSpace(full.ProjectID), full)
	if err != nil {
		return TerminalAdmission{}, err
	}
	if covered {
		// Already done by others. Harmless to admit this RESOLVE (the work exists); the reaper
		// would otherwise CANCEL it as a coverage duplicate. Admitting avoids wedging a race.
		return TerminalAdmission{Decision: TerminalAdmit, ReasonCode: reasonExecCarrierCovered}, nil
	}
	return TerminalAdmission{
		Decision:   TerminalReject,
		ReasonCode: reasonExecCarrierNoOwnEvidence,
		ContractID: contractExecutionCarrier,
		Err: fmt.Errorf("%w: execution carrier %s has no own patch-queue artifact and is not covered by integrated state; produce and submit the deliverable before resolving",
			ErrTaskCompletionContract, strings.TrimSpace(full.TaskID)),
	}, nil
}

// loadTerminalAdmissionTaskTx loads the minimal task columns the admission contracts need
// (kind/lane/project/requirements) from inside the open write tx, so a call site that holds only a
// task_id can still evaluate the execution-carrier contract against in-flight state.
func (s *Store) loadTerminalAdmissionTaskTx(ctx context.Context, tx *sql.Tx, taskID string) (WorkspaceTaskRecord, bool, error) {
	taskID = strings.TrimSpace(taskID)
	if tx == nil || taskID == "" {
		return WorkspaceTaskRecord{}, false, nil
	}
	var t WorkspaceTaskRecord
	var reqs string
	err := tx.QueryRowContext(ctx, `
SELECT task_id, status, task_kind, COALESCE(project_id, ''), COALESCE(project_lane, ''), COALESCE(task_requirements_json, '{}')
  FROM tasks
 WHERE task_id = ?`, taskID).Scan(&t.TaskID, &t.Status, &t.TaskKind, &t.ProjectID, &t.ProjectLane, &reqs)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkspaceTaskRecord{}, false, nil
	}
	if err != nil {
		return WorkspaceTaskRecord{}, false, fmt.Errorf("load terminal admission task: %w", err)
	}
	t.TaskRequirementsJSON = normalizeTaskRequirementsJSON(reqs)
	return t, true, nil
}

// evaluateProjectPhaseTerminalAdmissionTx (P06) is the project-phase admission contract closing the
// DUAL HOLE behind R44: the project.phase -> DONE write that the reflection gate points lanes at was
// itself ungated. A project may transition to DONE only when no open non-terminal CONVERGENCE-BLOCKING
// (spec-required) task remains. This is the COMPLETION half; the caller composes it with the
// strategic-lead AUTHORITY gate so trust-first bypasses authority only, never completion. Deliberately
// NOT a reuse of enforceEmptyProductFrontierCompletionTx, which self-guards to proactive-metacognition
// tasks and is inert for a phase carrier (design M7).
//
// CONVERGENCE-CLUSTER ROOT FIX: the frontier predicate is taskIsConvergenceBlocking, NOT "any
// non-reflection task". "Zero open tasks" was the wrong trigger - a live roster mints discretionary
// housekeeping (polish, side-effect classification, claim/role/repo repair, coordination meta)
// indefinitely, so the gate never admitted. Discretionary work no longer blocks DONE; spec-required
// product/integration work still does, and the external-spec coverage gate below remains the
// independent hard floor (every required AC attested against integrated state). taskIsConvergenceBlocking
// is mirrored byte-identically into agent so the agent fires the convergence protocol under
// exactly the condition admitted here.
func (s *Store) evaluateProjectPhaseTerminalAdmissionTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, actorID, coordinationMode string) (TerminalAdmission, error) {
	tasks, err := s.listWorkspaceTasksFiltered(ctx, tx, workspaceID, projectID)
	if err != nil {
		return TerminalAdmission{}, err
	}
	// R68 keystone is gated to SPEC-BEARING projects only: the redundant rejected-revision override is
	// safe solely behind the external-spec coverage floor, which is inert for a spec-less project. A
	// spec-less project therefore stays on the pre-R68 frontier-only rule (no loosening). Fail closed on a
	// fidelity-context error.
	specFidelity, err := s.projectSourceFidelityContextTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return TerminalAdmission{}, err
	}
	for _, candidate := range tasks {
		if isTerminalTaskStatus(candidate.Status) {
			continue
		}
		if !taskIsConvergenceBlocking(candidate) {
			continue
		}
		// R68 keystone (operator-signed, spec-bearing only): a redundant rejected-revision over-production
		// task does NOT hold the frontier open. It revises a REJECTED candidate whose every pathset path is
		// already covered by an integrated sibling, so it covers zero uncovered AC. Errs toward blocking on
		// any query error; the external-spec coverage gate below remains the independent hard floor.
		if specFidelity.Required {
			redundant, err := s.taskIsRedundantRejectedRevisionTx(ctx, tx, workspaceID, projectID, candidate)
			if err != nil {
				return TerminalAdmission{}, err
			}
			if redundant {
				continue
			}
		}
		return TerminalAdmission{
			Decision:   TerminalReject,
			ReasonCode: reasonPhaseFrontierOpen,
			ContractID: contractProjectPhaseDone,
			Err: fmt.Errorf("%w: project %s cannot transition to DONE while convergence-blocking task %s remains open (%s); cancel or complete it first",
				ErrTaskCompletionContract, strings.TrimSpace(projectID), strings.TrimSpace(candidate.TaskID), strings.TrimSpace(candidate.Status)),
		}, nil
	}
	// Blocker 2 (convergence cluster): frontier-empty is NECESSARY but no longer SUFFICIENT. For a
	// spec-bearing project, DONE additionally requires every operator-rooted acceptance criterion to
	// be attested against integrated product state (external-spec convergence oracle). Spec-less
	// projects fall through to the unchanged frontier-only Admit (preserves R30 self-DONE).
	cov, err := s.evaluateProjectExternalSpecCoverageTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return TerminalAdmission{}, err
	}
	if cov.Decision == TerminalReject {
		return cov, nil
	}
	return TerminalAdmission{Decision: TerminalAdmit, ReasonCode: reasonPhaseFrontierClear, Payload: cov.Payload}, nil
}
