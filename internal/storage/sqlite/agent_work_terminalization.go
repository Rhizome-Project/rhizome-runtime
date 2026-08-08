package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const agentWorkRequiredReceiptTerminalizerActorID = "required_receipt_terminalizer"
const projectPatchQueueClaimReaperActorID = "project_patch_queue_claim_reaper"

// projectPatchQueueClaimReapGrace is how long past lease expiry a CLAIMED item is left for the
// agent-driven claim-stewardship frontier to reclaim+decide before the system reaper releases it.
// The grace makes the reaper a SAFETY NET (RPF-58C) that composes with stewardship rather than
// preempting it: a normal expired claim is picked up by a steward within seconds/minutes; only a
// claim that nobody resolved within the grace (roster stopped, all agents busy, or the steward
// itself failed as in R58) is force-released. Kept under the 40m observe stall window so an
// unattended round recovers the item before it reads as a stall.
const projectPatchQueueClaimReapGrace = 30 * time.Minute

// reconcileExpiredProjectPatchQueueClaims (RPF-58C) releases CLAIMED patch-queue items whose claim
// lease expired more than projectPatchQueueClaimReapGrace ago back to PROPOSED, so a reviewer that
// claimed-then-wandered (R58: verdict delivered over the read-only model.ask channel, item left
// CLAIMED forever) does not permanently wedge the queue when no steward resolves it. This is the
// only NON-VOLUNTARY unwedger for a stale CLAIMED item - release is otherwise owner-only.
// Operation-bound items are skipped (they must be decided or canceled, not silently released).
// Runs on every work-next poll, alongside the task receipt sweep.
func (s *Store) reconcileExpiredProjectPatchQueueClaims(ctx context.Context, workspaceID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil
	}
	staleBefore := time.Now().UTC().Add(-projectPatchQueueClaimReapGrace).Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `
SELECT queue_id, item_id
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND state = ?
   AND TRIM(COALESCE(claim_expires_at, '')) <> ''
   AND claim_expires_at < ?`,
		workspaceID, ProjectPatchQueueStateClaimed, staleBefore)
	if err != nil {
		return fmt.Errorf("query expired patch queue claims: %w", err)
	}
	type patchQueueItemRef struct{ queueID, itemID string }
	var refs []patchQueueItemRef
	for rows.Next() {
		var ref patchQueueItemRef
		if err := rows.Scan(&ref.queueID, &ref.itemID); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan expired patch queue claim: %w", err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate expired patch queue claims: %w", err)
	}
	_ = rows.Close()
	for _, ref := range refs {
		if err := s.releaseExpiredProjectPatchQueueClaim(ctx, workspaceID, ref.queueID, ref.itemID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) releaseExpiredProjectPatchQueueClaim(ctx context.Context, workspaceID, queueID, itemID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin expired patch queue claim release tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	changed := false
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		current, ok, err := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if err != nil {
			return err
		}
		// Re-validate under the lock: still CLAIMED, lease still expired, and not operation-bound.
		if !ok || current.WorkspaceID != workspaceID || current.State != ProjectPatchQueueStateClaimed {
			return nil
		}
		expiresAt := strings.TrimSpace(current.ClaimExpiresAt)
		staleBefore := time.Now().UTC().Add(-projectPatchQueueClaimReapGrace).Format(time.RFC3339Nano)
		if expiresAt == "" || expiresAt >= staleBefore {
			return nil
		}
		if projectPatchQueueOperationBindingEvidencePresent(current) {
			return nil
		}
		priorClaimedBy := strings.TrimSpace(current.ClaimedBy)
		item := current
		item.State = ProjectPatchQueueStateProposed
		item.ClaimedBy = ""
		item.ClaimToken = ""
		item.ClaimedAt = ""
		item.ClaimExpiresAt = ""
		item.UpdatedAt = now
		if err := updateProjectPatchQueueLifecycleTx(ctx, tx, item); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project patch queue claim lease expiry release"); err != nil {
			return err
		}
		payload := projectPatchQueueEventPayload(item, projectPatchQueueClaimReaperActorID, "patch_queue.claim_expired")
		payload["prior_claimed_by"] = priorClaimedBy
		payload["prior_claim_expires_at"] = expiresAt
		payload["reaped_at"] = now
		if _, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.patch_queue.claim_expired",
			EntityType:  "project_patch_queue_item",
			EntityID:    queueID + "/" + itemID,
			ActorType:   "system",
			ActorID:     projectPatchQueueClaimReaperActorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		}); err != nil {
			return err
		}
		changed = true
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if !changed {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit expired patch queue claim release tx: %w", err)
	}
	return nil
}

type agentWorkRequiredTerminalReceipt struct {
	Kind              string
	Resolution        string
	Summary           string
	AgentID           string
	AllowBlockedClaim bool
	AllowClaimedClaim bool
	Payload           map[string]any
}

func (s *Store) reconcileAgentWorkRequiredReceiptTerminals(ctx context.Context, workspaceID string, tasks []WorkspaceTaskRecord) (map[string]struct{}, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" || len(tasks) == 0 {
		return nil, nil
	}
	terminalizedTaskIDs := make(map[string]struct{})
	for _, task := range tasks {
		if !agentWorkRequiredReceiptTerminalizationCandidate(task) {
			continue
		}
		// W2.1: the reaper suppression side now routes through the unified terminal-admission
		// predicate. evaluateTerminalAdmissionTx{SideSuppression} wraps agentWorkRequiredTerminalReceipt
		// verbatim (reads s.db, order + first-match preserved), so verdicts are byte-identical.
		adm, err := s.evaluateTerminalAdmissionTx(ctx, nil, workspaceID, task, TerminalWriteIntent{
			Side:   SideSuppression,
			Kind:   SuppressionSweep,
			Origin: OriginP01,
		})
		if err != nil {
			return terminalizedTaskIDs, err
		}
		// A candidate WITHOUT a receipt is simply not ready to terminalize - the sweep
		// must keep scanning the remaining candidates. (F1/P0: an early return here made
		// the sweep die on the first fresh PENDING task, so any stale receipt-bearing row
		// that did not sort first survived and re-entered selection - the R41 class.)
		if adm.Decision != TerminalTerminalize || adm.suppressionReceipt == nil {
			continue
		}
		taskChanged, err := s.terminalizeAgentWorkRequiredReceiptTask(ctx, workspaceID, task, *adm.suppressionReceipt)
		if err != nil {
			return terminalizedTaskIDs, err
		}
		if taskChanged {
			terminalizedTaskIDs[strings.TrimSpace(task.TaskID)] = struct{}{}
		}
	}
	return terminalizedTaskIDs, nil
}

func agentWorkRequiredReceiptTerminalizationCandidate(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.TaskID) == "" || isTerminalTaskStatus(task.Status) {
		return false
	}
	claimStatus := strings.TrimSpace(workspaceTaskPointerValue(task.ClaimStatus))
	if claimStatus == "" || strings.EqualFold(claimStatus, model.TaskClaimStatusReleased) {
		return true
	}
	if strings.EqualFold(claimStatus, model.TaskClaimStatusBlocked) {
		// RPF-58B(ii): a claim-stewardship task whose steward session BLOCKED (e.g. R58: zeta
		// blocked trying to integrate a still-CLAIMED item) must still be sweep-eligible, so it
		// terminalizes once the underlying item leaves CLAIMED (decided or claim-expired).
		// Otherwise the steward task stays RUNNING forever after a roster stop.
		// R25-F2: same shape for a patch-queue review task whose reviewer self-blocked its claim after a
		// structured-output repair failure. The queue decision is already durably recorded, but a BLOCKED
		// review task was not a sweep candidate, so it stayed RUNNING and wedged the rightful owner (its
		// validation/integration successor never drained). The receipt check (agentWorkTerminalPatchQueue
		// ReviewTask) still only fires once the item has left PROPOSED/CLAIMED, so an in-progress review is
		// never reaped.
		return agentWorkSideEffectClassificationTask(task) ||
			agentWorkPatchQueueClaimStewardshipTask(task) ||
			agentWorkPatchQueueReviewTask(task) ||
			agentWorkPatchQueueRevisionReceiptTerminalizationCandidate(task)
	}
	if strings.EqualFold(claimStatus, model.TaskClaimStatusClaimed) {
		// Receipt-backed terminalization is stronger than lifecycle ownership: once the
		// required durable fact exists, the live claim is stale and must not re-enter resume.
		return true
	}
	return isTerminalTaskClaimStatus(claimStatus)
}

func agentWorkPatchQueueRevisionReceiptTerminalizationCandidate(task WorkspaceTaskRecord) bool {
	if strings.EqualFold(strings.TrimSpace(agentWorkTaskRequirementString(task, "patch_queue_task_kind")), "revision") {
		return true
	}
	return agentWorkPatchQueueRevisionFollowupTask(task)
}

func (s *Store) agentWorkTaskHasRequiredReceiptTerminalization(ctx context.Context, taskID string) (bool, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return false, nil
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM audit_events
 WHERE event_type = 'task_required_receipt_terminalized'
   AND entity_type = 'task'
   AND entity_id = ?`, taskID).Scan(&count); err != nil {
		return false, fmt.Errorf("query required receipt terminalization audit: %w", err)
	}
	return count > 0, nil
}

func (s *Store) agentWorkRequiredTerminalReceipt(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (agentWorkRequiredTerminalReceipt, bool, error) {
	if receipt, ok, err := s.agentWorkProjectRoleAssignTerminalReceipt(ctx, workspaceID, task); err != nil || ok {
		return receipt, ok, err
	}
	if receipt, ok, err := s.agentWorkAcceptedPatchQueueTaskReceipt(ctx, workspaceID, task); err != nil || ok {
		return receipt, ok, err
	}
	if receipt, ok, err := s.agentWorkIntegratedPatchQueueTaskReceipt(ctx, workspaceID, task); err != nil || ok {
		return receipt, ok, err
	}
	if receipt, ok, err := s.agentWorkIntegratedPatchQueueScopeTerminalReceipt(ctx, workspaceID, task); err != nil || ok {
		return receipt, ok, err
	}
	if receipt, ok, err := s.agentWorkIntegratedAcceptanceCoverageTerminalReceipt(ctx, workspaceID, task); err != nil || ok {
		return receipt, ok, err
	}
	if receipt, ok, err := s.agentWorkIntegratedBranchEvidenceReconcileReceipt(ctx, workspaceID, task); err != nil || ok {
		return receipt, ok, err
	}
	if receipt, ok, err := s.agentWorkABPCSideEffectClassificationTerminalReceipt(ctx, workspaceID, task); err != nil || ok {
		return receipt, ok, err
	}
	checks := []struct {
		kind       string
		resolution string
		summary    string
		allowBlock bool
		allowClaim bool
		check      func(context.Context, string, WorkspaceTaskRecord) (bool, error)
	}{
		{
			kind:       "patch_queue_claim_stewardship_receipt",
			resolution: model.TaskStatusResolved,
			summary:    "patch queue claim stewardship receipt exists; carrier is terminal.",
			allowBlock: true,
			allowClaim: true,
			check:      s.agentWorkResolvedPatchQueueClaimStewardshipTask,
		},
		{
			kind:       "patch_queue_validation_sidecar_replaced",
			resolution: model.TaskStatusCancelled,
			summary:    "newer queue-bound validation work supersedes this sidecar.",
			allowClaim: true,
			check:      s.agentWorkStalePatchQueueValidationSidecarBehindQueueBoundTask,
		},
		{
			kind:       "patch_queue_visual_evidence_receipt",
			resolution: model.TaskStatusResolved,
			summary:    "visual evidence receipt exists; validation sidecar is terminal.",
			allowClaim: true,
			check:      s.agentWorkStalePatchQueueValidationSidecarBehindVisualEvidenceDoc,
		},
		{
			kind:       "patch_queue_revision_followup_receipt",
			resolution: model.TaskStatusCancelled,
			summary:    "revision follow-up receipt supersedes this sidecar.",
			allowClaim: true,
			check:      s.agentWorkStalePatchQueueSidecarBehindRevisionFollowup,
		},
		{
			kind:       "patch_queue_review_terminal_receipt",
			resolution: model.TaskStatusResolved,
			summary:    "patch queue review terminal receipt exists.",
			// R25-F2: a review task self-blocks its claim when structured-output repair fails, but the queue
			// decision is already durably recorded (the check only fires once the item has left PROPOSED/
			// CLAIMED). Without allowBlock the BLOCKED-claim review task is skipped at :1293 and stays RUNNING,
			// wedging the rightful owner so its validation/integration successor never drains. Allow the
			// blocked-claim terminalization: the decision is recorded, so the review carrier is genuinely done.
			allowBlock: true,
			allowClaim: true,
			check:      s.agentWorkTerminalPatchQueueReviewTask,
		},
		{
			kind:       "owner_submit_blocked_handoff_receipt",
			resolution: model.TaskStatusCancelled,
			summary:    "owner-submit handoff already emitted a durable block receipt.",
			allowClaim: true,
			check:      s.agentWorkReleasedBlockedOwnerSubmitHandoffTask,
		},
		{
			kind:       "owner_submit_terminal_acceptance_receipt",
			resolution: model.TaskStatusResolved,
			summary:    "owner-submit patch queue item reached accepted or integrated terminal state.",
			allowClaim: true,
			check:      s.agentWorkStaleOwnerSubmitTerminalAcceptedTask,
		},
		{
			kind:       "patch_queue_replacement_receipt",
			resolution: model.TaskStatusCancelled,
			summary:    "replacement patch queue item supersedes this work.",
			allowBlock: true,
			allowClaim: true,
			check:      s.agentWorkStalePatchQueueReplacementTask,
		},
		{
			kind:       "accepted_candidate_receipt",
			resolution: model.TaskStatusCancelled,
			summary:    "accepted patch queue candidate supersedes this scaffold work.",
			allowClaim: true,
			check:      s.agentWorkStaleScaffoldTaskAfterAcceptedCandidate,
		},
		{
			kind:       "project_claim_repair_terminal_receipt",
			resolution: model.TaskStatusCancelled,
			summary:    "project claim repair target is already terminal or no longer live.",
			allowClaim: true,
			check:      s.agentWorkStaleProjectClaimRepairTask,
		},
		{
			kind:       "project_repository_ready_receipt",
			resolution: model.TaskStatusResolved,
			summary:    "project repository is registered and ready; repository-repair carrier is terminal.",
			allowClaim: true,
			check:      s.agentWorkRepositoryReadyRepoRepairTask,
		},
		{
			kind:       "patch_queue_revision_followup_terminal_item_receipt",
			resolution: model.TaskStatusCancelled,
			summary:    "revision follow-up's candidate is already integrated/terminal; revising it is moot.",
			allowBlock: true,
			allowClaim: true,
			check:      s.agentWorkStalePatchQueueRevisionFollowupBehindTerminalItem,
		},
		{
			kind:       "side_effect_resolution_decision_receipt",
			resolution: model.TaskStatusResolved,
			summary:    "durable side-effect resolution decision (quarantine/revert) is recorded; lane is terminal.",
			allowClaim: true,
			check:      s.agentWorkSideEffectResolutionDecisionReceipt,
		},
	}
	for _, candidate := range checks {
		ok, err := candidate.check(ctx, workspaceID, task)
		if err != nil || !ok {
			if err != nil {
				return agentWorkRequiredTerminalReceipt{}, false, err
			}
			continue
		}
		return agentWorkRequiredTerminalReceipt{
			Kind:              candidate.kind,
			Resolution:        candidate.resolution,
			Summary:           candidate.summary,
			AllowBlockedClaim: candidate.allowBlock,
			AllowClaimedClaim: candidate.allowClaim,
			Payload: map[string]any{
				"receipt_kind": candidate.kind,
			},
		}, true, nil
	}
	return agentWorkRequiredTerminalReceipt{}, false, nil
}

func (s *Store) agentWorkABPCSideEffectClassificationTerminalReceipt(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (agentWorkRequiredTerminalReceipt, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	if workspaceID == "" || taskID == "" || projectID == "" || !agentWorkSideEffectClassificationTask(task) {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT t.task_id, t.status, COALESCE(t.project_id, ''), COALESCE(t.project_lane, ''),
       COALESCE(t.task_requirements_json, '{}'), t.updated_at,
       COALESCE(tc.claim_status, ''), COALESCE(tc.agent_id, '')
  FROM tasks t
  JOIN workspace_tasks wt ON wt.task_id = t.task_id
  LEFT JOIN task_claims tc ON tc.task_id = t.task_id AND tc.workspace_id = wt.workspace_id
 WHERE wt.workspace_id = ?
   AND COALESCE(t.project_id, '') = ?
   AND json_extract(t.task_requirements_json, '$.schema') = 'artifact_bound_side_effect_resolution_followup.v1'`,
		workspaceID,
		projectID)
	if err != nil {
		return agentWorkRequiredTerminalReceipt{}, false, fmt.Errorf("query ABPC side-effect successor receipts: %w", err)
	}
	defer rows.Close()

	waitingSuccessorIDs := agentWorkSideEffectWaitingSuccessorIDs(task)
	var terminalSuccessorIDs []string
	var openSuccessorIDs []string
	terminalAgents := make([]string, 0, 2)
	matchKinds := make(map[string]struct{})
	for rows.Next() {
		var candidate WorkspaceTaskRecord
		var claimStatus, claimAgentID string
		if err := rows.Scan(
			&candidate.TaskID,
			&candidate.Status,
			&candidate.ProjectID,
			&candidate.ProjectLane,
			&candidate.TaskRequirementsJSON,
			&candidate.UpdatedAt,
			&claimStatus,
			&claimAgentID,
		); err != nil {
			return agentWorkRequiredTerminalReceipt{}, false, fmt.Errorf("scan ABPC side-effect successor receipt: %w", err)
		}
		matchKind, matched := agentWorkSideEffectSuccessorMatchesClassifier(task, candidate, waitingSuccessorIDs)
		if !matched {
			continue
		}
		if matchKind != "" {
			matchKinds[matchKind] = struct{}{}
		}
		if agentWorkTaskTerminalWithClaim(candidate.Status, claimStatus) {
			terminalSuccessorIDs = append(terminalSuccessorIDs, candidate.TaskID)
			if strings.TrimSpace(claimAgentID) != "" {
				terminalAgents = append(terminalAgents, strings.TrimSpace(claimAgentID))
			}
			continue
		}
		openSuccessorIDs = append(openSuccessorIDs, candidate.TaskID)
	}
	if err := rows.Err(); err != nil {
		return agentWorkRequiredTerminalReceipt{}, false, fmt.Errorf("iterate ABPC side-effect successor receipts: %w", err)
	}
	if len(terminalSuccessorIDs) == 0 || len(openSuccessorIDs) > 0 {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	return agentWorkRequiredTerminalReceipt{
		Kind:              "abpc_side_effect_successor_terminal_receipt",
		Resolution:        model.TaskStatusResolved,
		Summary:           "ABPC side-effect recovery successor receipt exists; classifier carrier is terminal.",
		AgentID:           firstNonEmpty(terminalAgents...),
		AllowBlockedClaim: true,
		AllowClaimedClaim: true,
		Payload: map[string]any{
			"receipt_kind":                "abpc_side_effect_successor_terminal_receipt",
			"classification_task_id":      taskID,
			"terminal_successor_task_ids": terminalSuccessorIDs,
			"match_kinds":                 agentWorkReceiptStringSetKeys(matchKinds),
		},
	}, true, nil
}

// agentWorkSideEffectResolutionDecisionReceipt (R22-F3) reports whether a side-effect
// resolution/verification lane task already has a durable terminal ABPC decision recorded:
// an agent_updates row of update_type 'side_effect_resolution' whose summary names one of the
// task's side-effect refs with decision quarantine or revert. In R22 both reviewers were
// pinned inside side-effect lanes that "repeatedly recorded quarantine decisions but stayed
// active": every re-selection started a fresh cycle whose trace had no receipt, so the gate
// demanded side_effect_resolve again - while the durable decision already existed. The sweep
// now terminalizes such lanes, freeing reviewer capacity (the R22 starvation class).
// Conservative by design: only quarantine/revert (decisions that END a lane); accept/
// expand_boundary/request_verification/split/reassign route follow-up work and stay live.
// Classification tasks keep their own successor-based receipt.
func (s *Store) agentWorkSideEffectResolutionDecisionReceipt(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID := strings.TrimSpace(task.TaskID)
	if workspaceID == "" || taskID == "" {
		return false, nil
	}
	if !strings.HasPrefix(taskID, "task-side-effect-") || agentWorkSideEffectClassificationTask(task) {
		return false, nil
	}
	refs := agentWorkSideEffectRefsFromTaskText(task)
	if len(refs) == 0 {
		return false, nil
	}
	// P2 (work.next selection liveness): SARGABLE form. The old query ran a leading-wildcard
	// `summary LIKE '%' || ref || '%'` full-scan of agent_updates PER ref - non-indexable, so on an
	// accreted agent_updates table it exceeded the work.next RPC deadline and aborted selection
	// roster-wide (the c2 integration stall). Now a SINGLE query bounded by
	// idx_agent_updates_workspace_type (workspace_id, update_type, created_at) with only ANCHORED
	// decision prefixes (no leading wildcard); the ref match - byte-identical to the old
	// summary-substring semantics - runs in Go over the tiny quarantine/revert result set.
	rows, err := s.db.QueryContext(ctx, `
SELECT summary
  FROM agent_updates
 WHERE workspace_id = ?
   AND update_type = 'side_effect_resolution'
   AND (summary LIKE 'side_effect_resolve quarantine %' OR summary LIKE 'side_effect_resolve revert %')`,
		workspaceID,
	)
	if err != nil {
		return false, fmt.Errorf("query side-effect resolution decision receipt: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var summary string
		if err := rows.Scan(&summary); err != nil {
			return false, fmt.Errorf("scan side-effect resolution decision receipt: %w", err)
		}
		for _, ref := range refs {
			if strings.Contains(summary, ref) {
				return true, nil
			}
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate side-effect resolution decision receipt: %w", err)
	}
	return false, nil
}

// agentWorkPreSelectionSweepBudget bounds each pre-selection liveness sweep's OWN sub-context so a
// slow sweep can never consume the shared work.next RPC deadline and poison downstream candidate
// selection (P1.5). It is a var (not const) so a regression test can force every sweep to time out
// and assert selection still proceeds (P5). Generous headroom: post-P2 the sweeps are sub-second, so
// this only bites a pathologically slow sweep, which is then swallowed+journaled.
var agentWorkPreSelectionSweepBudget = 5 * time.Second

// agentWorkRequiredReceiptSweepFault, when non-nil, replaces the receipt-terminalization sweep with
// the injected error (test-only; nil in production). It gives P5 a DETERMINISTIC way to exercise the
// A1 swallow path (an in-memory test cannot reliably force a sub-tick ctx deadline on a fast query).
var agentWorkRequiredReceiptSweepFault func() error

// journalAgentWorkPreSelectionSweepFailure records (best-effort, own short tx, swallows all errors) a
// pre-selection sweep-level failure so a permanently-degraded sweep is observable even though
// work.next swallows the error to keep selection alive (A1). Mirrors
// journalDeferredProjectPatchQueueContinuationSweepFailure.
func (s *Store) journalAgentWorkPreSelectionSweepFailure(ctx context.Context, workspaceID, sweep string, cause error) {
	if cause == nil {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := s.appendRuntimeEventTx(ctx, tx, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.work.next.pre_selection_sweep_failed",
		EntityType:  "agent_work_pre_selection_sweep",
		EntityID:    strings.TrimSpace(sweep),
		ActorType:   "system",
		ActorID:     "system",
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id": workspaceID,
			"sweep":        sweep,
			"cause":        cause.Error(),
			"summary":      "Pre-selection liveness sweep failed (swallowed to keep work.next selection alive); liveness for this sweep is degraded until it recovers",
		}),
		CreatedAt: now,
	}); err != nil {
		return
	}
	_ = tx.Commit()
}

func (s *Store) agentWorkAcceptedPatchQueueTaskReceipt(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (agentWorkRequiredTerminalReceipt, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	if workspaceID == "" || taskID == "" || projectID == "" {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	var item ProjectPatchQueueItemRecord
	err := s.db.QueryRowContext(ctx, `
SELECT queue_id, item_id, project_id, repo_id, branch_id, head_sha, submitted_by, claimed_by, decided_by, updated_at
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND project_id = ?
   AND task_id = ?
   AND state = ?
 ORDER BY updated_at DESC, queue_id DESC, item_id DESC
 LIMIT 1`,
		workspaceID,
		projectID,
		taskID,
		ProjectPatchQueueStateAccepted,
	).Scan(
		&item.QueueID,
		&item.ItemID,
		&item.ProjectID,
		&item.RepoID,
		&item.BranchID,
		&item.HeadSHA,
		&item.SubmittedBy,
		&item.ClaimedBy,
		&item.DecidedBy,
		&item.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	if err != nil {
		return agentWorkRequiredTerminalReceipt{}, false, fmt.Errorf("query accepted patch queue task receipt: %w", err)
	}
	return agentWorkRequiredTerminalReceipt{
		Kind:              "accepted_patch_queue_task_receipt",
		Resolution:        model.TaskStatusResolved,
		Summary:           "linked patch queue item reached accepted review state; integration continues via queue lane.",
		AgentID:           firstNonEmpty(strings.TrimSpace(item.DecidedBy), strings.TrimSpace(item.ClaimedBy), strings.TrimSpace(item.SubmittedBy)),
		AllowClaimedClaim: true,
		Payload: map[string]any{
			"receipt_kind":    "accepted_patch_queue_task_receipt",
			"queue_id":        item.QueueID,
			"item_id":         item.ItemID,
			"project_id":      item.ProjectID,
			"repo_id":         item.RepoID,
			"branch_id":       item.BranchID,
			"head_sha":        item.HeadSHA,
			"submitted_by":    item.SubmittedBy,
			"claimed_by":      item.ClaimedBy,
			"decided_by":      item.DecidedBy,
			"receipt_state":   ProjectPatchQueueStateAccepted,
			"receipt_task_id": taskID,
			"receipt_at":      item.UpdatedAt,
		},
	}, true, nil
}

// agentWorkSideEffectRefsFromTaskText extracts side-effect refs ("side-effect:...") from the
// task title/description/requirements text. Refs are whitespace/comma/backtick-delimited.
func agentWorkSideEffectRefsFromTaskText(task WorkspaceTaskRecord) []string {
	text := strings.Join([]string{task.Title, task.Description, task.TaskRequirementsJSON}, "\n")
	var refs []string
	seen := map[string]struct{}{}
	for {
		idx := strings.Index(text, "side-effect:")
		if idx < 0 {
			break
		}
		rest := text[idx:]
		end := strings.IndexFunc(rest, func(r rune) bool {
			switch r {
			case ' ', '\t', '\n', '\r', ',', '`', '"', '\'', ')', ']', '}':
				return true
			}
			return false
		})
		ref := rest
		if end > 0 {
			ref = rest[:end]
		}
		ref = strings.TrimSpace(ref)
		if len(ref) > len("side-effect:") {
			if _, ok := seen[ref]; !ok {
				seen[ref] = struct{}{}
				refs = append(refs, ref)
			}
		}
		text = rest[len("side-effect:"):]
	}
	return refs
}

func agentWorkSideEffectClassificationTask(task WorkspaceTaskRecord) bool {
	schema := strings.TrimSpace(agentWorkTaskRequirementString(task, "schema"))
	abpcClass := strings.TrimSpace(agentWorkTaskRequirementString(task, "abpc_task_class"))
	if schema == "artifact_bound_side_effect_classification_task.v1" || abpcClass == "side_effect_classification" {
		return true
	}
	return strings.HasPrefix(strings.TrimSpace(task.TaskID), "task-side-effect-classify-") &&
		agentWorkTaskHasTag(task, "side-effect-classification", "abpc")
}

func agentWorkSideEffectSuccessorMatchesClassifier(classifier, successor WorkspaceTaskRecord, waitingSuccessorIDs map[string]struct{}) (string, bool) {
	successorID := strings.TrimSpace(successor.TaskID)
	classifierID := strings.TrimSpace(classifier.TaskID)
	if successorID == "" || classifierID == "" {
		return "", false
	}
	if _, ok := waitingSuccessorIDs[successorID]; ok {
		return "waiting_successor_id", true
	}
	for _, key := range []string{"parent_classifier_task_id", "classification_task_id", "active_task_id"} {
		if strings.TrimSpace(agentWorkTaskRequirementString(successor, key)) == classifierID {
			return key, true
		}
	}
	recovery, ok := agentWorkABPCRecoveryAction(successor)
	if !ok || !agentWorkABPCRecoveryActionIsVerification(recovery) {
		return "", false
	}
	if classifierBranch := strings.TrimSpace(agentWorkTaskRequirementString(classifier, "branch_id")); classifierBranch != "" &&
		strings.TrimSpace(agentWorkTaskRequirementString(successor, "branch_id")) != classifierBranch {
		return "", false
	}
	if agentWorkTaskRequirementStringSetsIntersect(classifier, successor, "side_effect_refs") {
		return "side_effect_ref", true
	}
	return "", false
}

func agentWorkABPCRecoveryActionIsVerification(recovery agentWorkABPCRecoveryActionInfo) bool {
	return strings.TrimSpace(recovery.ABPCTaskClass) == "side_effect_verification" ||
		strings.TrimSpace(recovery.ActionKind) == "verify_bucket" ||
		strings.TrimSpace(recovery.Decision) == "request_verification"
}

func agentWorkTaskTerminalWithClaim(taskStatus, claimStatus string) bool {
	if !isTerminalTaskStatus(taskStatus) {
		return false
	}
	claimStatus = strings.TrimSpace(claimStatus)
	return claimStatus == "" || isTerminalTaskClaimStatus(claimStatus)
}

func agentWorkTaskRequirementStringSetsIntersect(left, right WorkspaceTaskRecord, key string) bool {
	leftSet := agentWorkTaskRequirementStringSet(left, key)
	if len(leftSet) == 0 {
		return false
	}
	for value := range agentWorkTaskRequirementStringSet(right, key) {
		if _, ok := leftSet[value]; ok {
			return true
		}
	}
	return false
}

func agentWorkTaskRequirementStringSet(task WorkspaceTaskRecord, key string) map[string]struct{} {
	out := make(map[string]struct{})
	payload := agentWorkTaskRequirementPayload(task)
	value, ok := payload[strings.TrimSpace(key)]
	if !ok {
		return out
	}
	add := func(raw string) {
		raw = strings.ToLower(strings.TrimSpace(raw))
		if raw != "" {
			out[raw] = struct{}{}
		}
	}
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			add(agentWorkStringValue(item))
		}
	case []string:
		for _, item := range typed {
			add(item)
		}
	case string:
		add(typed)
	}
	return out
}

func agentWorkTaskRequirementPayload(task WorkspaceTaskRecord) map[string]any {
	raw := normalizeTaskRequirementsJSON(task.TaskRequirementsJSON)
	if raw == "{}" {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}
	return payload
}

func agentWorkSideEffectWaitingSuccessorIDs(task WorkspaceTaskRecord) map[string]struct{} {
	out := make(map[string]struct{})
	summary := strings.TrimSpace(workspaceTaskPointerValue(task.ClaimSummary))
	const marker = "waiting_on_side_effect_resolution_successors:"
	idx := strings.Index(strings.ToLower(summary), marker)
	if idx < 0 {
		return out
	}
	tail := summary[idx+len(marker):]
	fields := strings.FieldsFunc(tail, func(r rune) bool {
		switch r {
		case ',', ';', '\n', '\r', '\t', ' ', '.', ')', ']', '}':
			return true
		default:
			return false
		}
	})
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if strings.HasPrefix(field, "task-side-effect-") {
			out[field] = struct{}{}
		}
	}
	return out
}

func agentWorkReceiptStringSetKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	return out
}

// agentWorkRepositoryReadyRepoRepairTask reports whether a repository-repair carrier's
// required durable receipt exists: the project repository it asked to register/
// materialize is READY (or explicitly NOT_REQUIRED). F2: repo-repair carriers previously
// had no receipt->terminal path at all, so a fresh run's early repo-repair carrier would
// outlive its own success and re-enter selection forever.
func (s *Store) agentWorkRepositoryReadyRepoRepairTask(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if !projectRepositoryRepairTask(task) {
		return false, nil
	}
	repos, err := s.ListProjectRepositories(ctx, ProjectRepositoryListFilter{
		WorkspaceID: strings.TrimSpace(workspaceID),
		ProjectID:   strings.TrimSpace(task.ProjectID),
	})
	if err != nil {
		return false, err
	}
	wantRepoID := agentWorkClaimRepairTextFieldValue(task, "repo_id")
	for _, repo := range repos {
		if wantRepoID != "" && strings.TrimSpace(repo.RepoID) != wantRepoID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(repo.RepoStatus)) {
		case ProjectRepoStatusReady, ProjectRepoStatusNotRequired:
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) agentWorkIntegratedPatchQueueTaskReceipt(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (agentWorkRequiredTerminalReceipt, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	if workspaceID == "" || taskID == "" || projectID == "" {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	var item ProjectPatchQueueItemRecord
	err := s.db.QueryRowContext(ctx, `
SELECT queue_id, item_id, project_id, repo_id, branch_id, head_sha, submitted_by, claimed_by, decided_by, updated_at
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND project_id = ?
   AND task_id = ?
   AND state = ?
 ORDER BY updated_at DESC, queue_id DESC, item_id DESC
 LIMIT 1`,
		workspaceID,
		projectID,
		taskID,
		ProjectPatchQueueStateIntegrated,
	).Scan(
		&item.QueueID,
		&item.ItemID,
		&item.ProjectID,
		&item.RepoID,
		&item.BranchID,
		&item.HeadSHA,
		&item.SubmittedBy,
		&item.ClaimedBy,
		&item.DecidedBy,
		&item.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	if err != nil {
		return agentWorkRequiredTerminalReceipt{}, false, fmt.Errorf("query integrated patch queue task receipt: %w", err)
	}
	return agentWorkRequiredTerminalReceipt{
		Kind:              "integrated_patch_queue_task_receipt",
		Resolution:        model.TaskStatusResolved,
		Summary:           "linked patch queue item reached integrated terminal state.",
		AgentID:           firstNonEmpty(strings.TrimSpace(item.DecidedBy), strings.TrimSpace(item.ClaimedBy), strings.TrimSpace(item.SubmittedBy)),
		AllowClaimedClaim: true,
		Payload: map[string]any{
			"receipt_kind":    "integrated_patch_queue_task_receipt",
			"queue_id":        item.QueueID,
			"item_id":         item.ItemID,
			"project_id":      item.ProjectID,
			"repo_id":         item.RepoID,
			"branch_id":       item.BranchID,
			"head_sha":        item.HeadSHA,
			"submitted_by":    item.SubmittedBy,
			"claimed_by":      item.ClaimedBy,
			"decided_by":      item.DecidedBy,
			"receipt_state":   ProjectPatchQueueStateIntegrated,
			"receipt_task_id": taskID,
			"receipt_at":      item.UpdatedAt,
		},
	}, true, nil
}

func (s *Store) agentWorkIntegratedPatchQueueScopeTerminalReceipt(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (agentWorkRequiredTerminalReceipt, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	if workspaceID == "" || taskID == "" || projectID == "" {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	if !agentWorkIntegratedPatchQueueScopeSidecarCandidate(task) {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	taskPaths := agentWorkIntegratedPatchQueueScopeSidecarPaths(task)
	if len(taskPaths) == 0 {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	upstreamItems, err := s.agentWorkIntegratedPatchQueueScopeUpstreamItems(ctx, workspaceID, projectID, task)
	if err != nil {
		return agentWorkRequiredTerminalReceipt{}, false, err
	}
	if len(upstreamItems) == 0 {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		State:       ProjectPatchQueueStateIntegrated,
	})
	if err != nil {
		return agentWorkRequiredTerminalReceipt{}, false, err
	}
	for _, item := range items {
		queueID := strings.TrimSpace(item.QueueID)
		itemID := strings.TrimSpace(item.ItemID)
		if queueID == "" || itemID == "" {
			continue
		}
		if _, isUpstream := upstreamItems[agentWorkPatchQueueItemPairKey(queueID, itemID)]; isUpstream {
			continue
		}
		itemPaths := agentWorkPatchQueueItemPathset(item)
		if !agentWorkIntegratedPatchQueueScopeReceiptCoversTask(taskPaths, itemPaths) {
			continue
		}
		return agentWorkRequiredTerminalReceipt{
			Kind:              "integrated_patch_queue_scope_receipt",
			Resolution:        model.TaskStatusCancelled,
			Summary:           "integrated patch queue scope receipt supersedes downstream sidecar.",
			AgentID:           firstNonEmpty(strings.TrimSpace(item.DecidedBy), strings.TrimSpace(item.ClaimedBy), strings.TrimSpace(item.SubmittedBy)),
			AllowClaimedClaim: true,
			Payload: map[string]any{
				"receipt_kind":          "integrated_patch_queue_scope_receipt",
				"queue_id":              item.QueueID,
				"item_id":               item.ItemID,
				"project_id":            item.ProjectID,
				"repo_id":               item.RepoID,
				"branch_id":             item.BranchID,
				"head_sha":              item.HeadSHA,
				"receipt_state":         ProjectPatchQueueStateIntegrated,
				"receipt_task_id":       taskID,
				"receipt_at":            item.UpdatedAt,
				"covered_write_scope":   taskPaths,
				"integrated_item_paths": itemPaths,
				"upstream_items":        agentWorkPatchQueueItemPairKeys(upstreamItems),
			},
		}, true, nil
	}
	return agentWorkRequiredTerminalReceipt{}, false, nil
}

func (s *Store) agentWorkIntegratedAcceptanceCoverageTerminalReceipt(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (agentWorkRequiredTerminalReceipt, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	if workspaceID == "" || taskID == "" || projectID == "" {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	if !taskCompletionCoverageTerminalizationCandidate(task) {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	hasIdentity, err := s.taskHasPatchQueueIdentityWithQuerier(ctx, s.db, workspaceID, taskID)
	if err != nil || hasIdentity {
		return agentWorkRequiredTerminalReceipt{}, false, err
	}
	coverage, ok, err := s.taskIntegratedAcceptanceCoverageReceipt(ctx, s.db, workspaceID, projectID, task)
	if err != nil || !ok {
		return agentWorkRequiredTerminalReceipt{}, false, err
	}
	return agentWorkRequiredTerminalReceipt{
		Kind:              "integrated_acceptance_contract_receipt",
		Resolution:        model.TaskStatusCancelled,
		Summary:           "integrated project state already satisfies this task's acceptance contract; duplicate implementation carrier is terminal.",
		AllowClaimedClaim: true,
		Payload: map[string]any{
			"receipt_kind":           "integrated_acceptance_contract_receipt",
			"receipt_task_id":        taskID,
			"project_id":             projectID,
			"requested_criteria":     coverage.RequestedCriteria,
			"covered_criteria":       coverage.CoveredCriteria,
			"source_task_ids":        coverage.SourceTaskIDs,
			"integrated_item_refs":   coverage.IntegratedItemRefs,
			"queue_id":               coverage.QueueID,
			"item_id":                coverage.ItemID,
			"branch_id":              coverage.BranchID,
			"head_sha":               coverage.HeadSHA,
			"terminalization_reason": "completion_contract_already_satisfied_by_integrated_state",
		},
	}, true, nil
}

func (s *Store) agentWorkIntegratedBranchEvidenceReconcileReceipt(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (agentWorkRequiredTerminalReceipt, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	branchID := strings.TrimSpace(agentWorkTaskRequirementString(task, "branch_id"))
	if workspaceID == "" || taskID == "" || projectID == "" || branchID == "" {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	if !strings.EqualFold(strings.TrimSpace(agentWorkTaskRequirementString(task, "schema")), "project_branch_commit_evidence_reconciliation_task.v1") {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	item, ok, err := s.integratedProjectPatchQueueItemForBranch(ctx, workspaceID, projectID, branchID)
	if err != nil || !ok {
		return agentWorkRequiredTerminalReceipt{}, false, err
	}
	return agentWorkRequiredTerminalReceipt{
		Kind:              "integrated_branch_evidence_reconcile_receipt",
		Resolution:        model.TaskStatusCancelled,
		Summary:           "branch evidence reconciliation target is already integrated; stale reconciliation carrier is terminal.",
		AgentID:           firstNonEmpty(strings.TrimSpace(item.DecidedBy), strings.TrimSpace(item.SubmittedBy), strings.TrimSpace(item.AgentID)),
		AllowClaimedClaim: true,
		Payload: map[string]any{
			"receipt_kind":             "integrated_branch_evidence_reconcile_receipt",
			"receipt_task_id":          taskID,
			"project_id":               projectID,
			"queue_id":                 item.QueueID,
			"item_id":                  item.ItemID,
			"branch_id":                item.BranchID,
			"requested_head_sha":       agentWorkTaskRequirementString(task, "head_sha", "candidate_head_sha"),
			"integrated_head_sha":      item.HeadSHA,
			"mutation_attempt_ref":     agentWorkTaskRequirementString(task, "mutation_attempt_ref"),
			"terminalization_reason":   "branch_already_integrated",
			"integrated_item_state":    item.State,
			"integrated_item_updated":  item.UpdatedAt,
			"integrated_item_task_id":  item.TaskID,
			"integrated_decision_by":   item.DecidedBy,
			"integrated_decision_at":   item.DecidedAt,
			"integrated_decision_note": item.DecisionSummary,
		},
	}, true, nil
}

func (s *Store) integratedProjectPatchQueueItemForBranch(ctx context.Context, workspaceID, projectID, branchID string) (ProjectPatchQueueItemRecord, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	branchID = strings.TrimSpace(branchID)
	if workspaceID == "" || projectID == "" || branchID == "" {
		return ProjectPatchQueueItemRecord{}, false, nil
	}
	item, err := scanProjectPatchQueueItem(s.db.QueryRowContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND project_id = ?
   AND branch_id = ?
   AND state = ?
 ORDER BY updated_at DESC, queue_id ASC, item_id ASC
 LIMIT 1`,
		workspaceID,
		projectID,
		branchID,
		ProjectPatchQueueStateIntegrated,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return ProjectPatchQueueItemRecord{}, false, nil
	}
	if err != nil {
		return ProjectPatchQueueItemRecord{}, false, fmt.Errorf("query integrated patch queue item for branch evidence reconcile: %w", err)
	}
	return item, true, nil
}

func agentWorkIntegratedPatchQueueScopeSidecarCandidate(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindExecution) {
		return false
	}
	if !projectLaneRequiresImplementationGate(task.ProjectLane) {
		return false
	}
	if strings.TrimSpace(agentWorkTaskRequirementString(task, "patch_queue_task_kind")) != "" {
		return false
	}
	if strings.TrimSpace(agentWorkTaskRequirementString(task, "required_tool", "required_terminal_tool", "required_transition")) != "" {
		return false
	}
	if len(agentWorkIntegratedPatchQueueScopeSidecarPaths(task)) == 0 {
		return false
	}
	if len(agentWorkTaskRequirementsStringSlice(task, "advisory_dependency_task_ids")) > 0 {
		return true
	}
	return strings.TrimSpace(agentWorkTaskRequirementString(task, "upstream_queue_id")) != "" ||
		strings.TrimSpace(agentWorkTaskRequirementString(task, "upstream_item_id")) != "" ||
		strings.TrimSpace(agentWorkTaskRequirementString(task, "upstream_candidate_provenance")) != ""
}

func agentWorkIntegratedPatchQueueScopeSidecarPaths(task WorkspaceTaskRecord) []string {
	paths := normalizeStringSlice(task.WriteScopeHints)
	if len(paths) == 0 {
		paths = agentWorkTaskRequirementWriteScopeHints(task.TaskRequirementsJSON)
	}
	return paths
}

func (s *Store) agentWorkIntegratedPatchQueueScopeUpstreamItems(ctx context.Context, workspaceID, projectID string, task WorkspaceTaskRecord) (map[string]struct{}, error) {
	out := make(map[string]struct{})
	add := func(queueID, itemID string) {
		queueID = strings.TrimSpace(queueID)
		itemID = strings.TrimSpace(itemID)
		if queueID != "" && itemID != "" {
			out[agentWorkPatchQueueItemPairKey(queueID, itemID)] = struct{}{}
		}
	}
	add(
		agentWorkTaskRequirementString(task, "upstream_queue_id", "queue_id"),
		agentWorkTaskRequirementString(task, "upstream_item_id", "item_id"),
	)
	for _, dependencyTaskID := range agentWorkTaskRequirementsStringSlice(task, "advisory_dependency_task_ids") {
		dependencyTaskID = strings.TrimSpace(dependencyTaskID)
		if dependencyTaskID == "" {
			continue
		}
		rows, err := s.db.QueryContext(ctx, `
SELECT i.queue_id, i.item_id
  FROM task_patch_queue_identities i
  JOIN tasks t ON t.task_id = i.task_id
 WHERE i.workspace_id = ?
   AND i.project_id = ?
   AND i.task_id = ?
   AND LOWER(TRIM(i.kind)) = 'integration'
   AND t.status IN (?, ?, ?)`,
			workspaceID,
			projectID,
			dependencyTaskID,
			model.TaskStatusResolved,
			model.TaskStatusFailed,
			model.TaskStatusCancelled,
		)
		if err != nil {
			return nil, fmt.Errorf("query integration dependency patch queue identity: %w", err)
		}
		for rows.Next() {
			var queueID, itemID string
			if err := rows.Scan(&queueID, &itemID); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan integration dependency patch queue identity: %w", err)
			}
			add(queueID, itemID)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("iterate integration dependency patch queue identity: %w", err)
		}
		_ = rows.Close()
	}
	return out, nil
}

func agentWorkIntegratedPatchQueueScopeReceiptCoversTask(taskPaths, itemPaths []string) bool {
	taskPaths = normalizeStringSlice(taskPaths)
	itemPaths = normalizeStringSlice(itemPaths)
	if len(taskPaths) == 0 || len(itemPaths) == 0 {
		return false
	}
	if writeScopePathsCoveredBy(taskPaths, itemPaths) {
		return true
	}
	return agentWorkIntegratedPatchQueueDocOnlyScope(taskPaths) && writeScopesOverlap(taskPaths, itemPaths)
}

func agentWorkIntegratedPatchQueueDocOnlyScope(paths []string) bool {
	paths = normalizeStringSlice(paths)
	if len(paths) == 0 {
		return false
	}
	for _, path := range paths {
		normalized := normalizeProjectBranchReviewScopePath(path)
		if normalized == "" {
			return false
		}
		if normalized == "readme.md" || normalized == "readme" || normalized == "docs" ||
			strings.HasPrefix(normalized, "docs/") || strings.HasSuffix(normalized, ".md") ||
			(strings.HasSuffix(normalized, "/**") && strings.TrimSuffix(normalized, "/**") == "docs") {
			continue
		}
		return false
	}
	return true
}

func agentWorkPatchQueueItemPairKey(queueID, itemID string) string {
	return strings.ToLower(strings.TrimSpace(queueID)) + "/" + strings.ToLower(strings.TrimSpace(itemID))
}

func agentWorkPatchQueueItemPairKeys(items map[string]struct{}) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for key := range items {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func (s *Store) agentWorkProjectRoleAssignTerminalReceipt(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (agentWorkRequiredTerminalReceipt, bool, error) {
	if strings.TrimSpace(task.ProjectID) == "" {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	if !agentWorkTaskRequiresProjectRoleAssignReceipt(task) {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	targetAgentID := firstNonEmpty(
		agentWorkTaskRequirementString(task, "target_agent_id"),
		agentWorkTaskRequirementString(task, "agent_id"),
		agentWorkTaskRequirementString(task, "requested_agent_id"),
	)
	if targetAgentID == "" {
		return agentWorkRequiredTerminalReceipt{}, false, nil
	}
	roleType := strings.ToUpper(firstNonEmpty(
		agentWorkTaskRequirementString(task, "role_type"),
		agentWorkTaskRequirementString(task, "requested_role_type"),
		agentWorkTaskRequirementString(task, "target_role_type"),
	))
	roleType = strings.ReplaceAll(strings.ReplaceAll(roleType, "-", "_"), " ", "_")
	writeScopeJSON := firstNonEmpty(
		agentWorkTaskRequirementString(task, "write_scope_json"),
		agentWorkTaskRequirementString(task, "requested_write_scope_json"),
		agentWorkTaskRequirementString(task, "target_write_scope_json"),
	)
	roles, err := s.ListProjectRoles(ctx, workspaceID, task.ProjectID, false)
	if err != nil {
		return agentWorkRequiredTerminalReceipt{}, false, err
	}
	for _, role := range roles {
		if strings.TrimSpace(role.AgentID) != strings.TrimSpace(targetAgentID) {
			continue
		}
		if roleType != "" && strings.ToUpper(strings.TrimSpace(role.RoleType)) != roleType {
			continue
		}
		if writeScopeJSON != "" {
			requiredPaths := projectBranchReviewScopePaths(writeScopeJSON)
			if len(requiredPaths) > 0 && !writeScopePathsCoverScopeJSON(writeScopeJSON, role.WriteScopeJSON) {
				continue
			}
		}
		return agentWorkRequiredTerminalReceipt{
			Kind:              "project_role_assign_receipt",
			Resolution:        model.TaskStatusResolved,
			Summary:           "project_role_assign durable receipt exists; role/scope carrier is terminal.",
			AgentID:           role.AgentID,
			AllowClaimedClaim: true,
			Payload: map[string]any{
				"receipt_kind":     "project_role_assign_receipt",
				"project_role_id":  role.RoleID,
				"project_id":       role.ProjectID,
				"target_agent_id":  role.AgentID,
				"role_type":        role.RoleType,
				"write_scope_json": role.WriteScopeJSON,
			},
		}, true, nil
	}
	return agentWorkRequiredTerminalReceipt{}, false, nil
}

func agentWorkTaskRequiresProjectRoleAssignReceipt(task WorkspaceTaskRecord) bool {
	if projectRoleScopeTask(task) {
		return true
	}
	if transition := strings.TrimSpace(agentWorkTaskRequirementString(task, "required_transition")); transition == "project_role_assign" {
		return true
	}
	if transition := strings.TrimSpace(agentWorkTaskRequirementString(task, "preferred_transition")); transition == "project_role_assign" {
		return true
	}
	return strings.TrimSpace(agentWorkTaskRequirementString(task, "schema")) == "project_role_scope_authority_transition.v1"
}

func (s *Store) terminalizeAgentWorkRequiredReceiptTask(ctx context.Context, workspaceID string, task WorkspaceTaskRecord, receipt agentWorkRequiredTerminalReceipt) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID := strings.TrimSpace(task.TaskID)
	if workspaceID == "" || taskID == "" {
		return false, errors.New("workspace_id and task_id are required for receipt terminalization")
	}
	resolution := strings.TrimSpace(receipt.Resolution)
	if resolution == "" {
		resolution = model.TaskStatusResolved
	}
	switch resolution {
	case model.TaskStatusResolved, model.TaskStatusFailed, model.TaskStatusCancelled:
	default:
		return false, fmt.Errorf("invalid receipt terminalization resolution: %s", resolution)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return false, err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return false, fmt.Errorf("begin receipt terminalization tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	changed := false
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		taskSnapshot, found, err := loadTaskTransitionSnapshotTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if !found {
			return ErrTaskNotFound
		}
		if isTerminalTaskStatus(taskSnapshot.Status) {
			return nil
		}
		claimSnapshot, hasClaim, err := loadTaskClaimTransitionSnapshotTx(ctx, tx, taskID, workspaceID)
		if err != nil {
			return err
		}
		if hasClaim {
			claimStatus := strings.TrimSpace(claimSnapshot.ClaimStatus)
			if claimStatus != "" &&
				!strings.EqualFold(claimStatus, model.TaskClaimStatusReleased) &&
				!isTerminalTaskClaimStatus(claimStatus) &&
				!(receipt.AllowBlockedClaim && strings.EqualFold(claimStatus, model.TaskClaimStatusBlocked)) &&
				!(receipt.AllowClaimedClaim && strings.EqualFold(claimStatus, model.TaskClaimStatusClaimed)) {
				return nil
			}
		}

		targetClaimStatus := taskResolutionToClaimStatus(resolution)
		targetNodeStatus := mapTaskResolutionToNodeStatus(resolution)
		nodeRows, err := tx.QueryContext(ctx, `SELECT node_id, status FROM dag_nodes WHERE task_id = ? ORDER BY node_id`, taskID)
		if err != nil {
			return fmt.Errorf("query task nodes for receipt terminalization: %w", err)
		}
		type nodeState struct {
			nodeID string
			status string
		}
		var nodes []nodeState
		for nodeRows.Next() {
			var node nodeState
			if err := nodeRows.Scan(&node.nodeID, &node.status); err != nil {
				_ = nodeRows.Close()
				return fmt.Errorf("scan task node for receipt terminalization: %w", err)
			}
			nodes = append(nodes, node)
		}
		if err := nodeRows.Err(); err != nil {
			_ = nodeRows.Close()
			return fmt.Errorf("iterate task nodes for receipt terminalization: %w", err)
		}
		_ = nodeRows.Close()
		actorID := firstNonEmpty(strings.TrimSpace(workspaceTaskPointerValue(task.ClaimAgentID)), strings.TrimSpace(receipt.AgentID), agentWorkRequiredReceiptTerminalizerActorID)
		if hasClaim {
			actorID = firstNonEmpty(strings.TrimSpace(claimSnapshot.AgentID), actorID)
		}
		runtimeAgentID := ""
		if candidateAgentID := strings.TrimSpace(actorID); candidateAgentID != "" {
			exists, err := s.agentExistsForRuntimeEventTx(ctx, tx, workspaceID, candidateAgentID)
			if err != nil {
				return err
			}
			if exists {
				runtimeAgentID = candidateAgentID
			}
		}
		for _, node := range nodes {
			if isTerminalNodeStatus(node.status) {
				continue
			}
			if _, err := tx.ExecContext(ctx,
				`UPDATE dag_nodes SET status = ?, updated_at = ? WHERE task_id = ? AND node_id = ?`,
				targetNodeStatus, now, taskID, node.nodeID); err != nil {
				return fmt.Errorf("update task node for receipt terminalization: %w", err)
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO node_state_transitions(
				  transition_id, task_id, node_id, from_status, to_status, reason, actor_id, created_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
				nextID("node_transition"), taskID, node.nodeID, node.status, targetNodeStatus, receipt.Summary, actorID, now); err != nil {
				return fmt.Errorf("insert node transition for receipt terminalization: %w", err)
			}
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE tasks
			    SET status = ?, close_reason = ?, updated_at = ?
			  WHERE task_id = ? AND status = ? AND updated_at = ?`,
			resolution,
			receipt.Summary,
			now,
			taskID,
			taskSnapshot.Status,
			taskSnapshot.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("update task for receipt terminalization: %w", err)
		}
		if affected, _ := res.RowsAffected(); affected != 1 {
			return ErrTaskClaimStaleTransition
		}
		if hasClaim {
			res, err = tx.ExecContext(ctx,
				`UPDATE task_claims
				    SET claim_status = ?, summary = ?, updated_at = ?
				  WHERE task_id = ? AND workspace_id = ? AND updated_at = ?`,
				targetClaimStatus,
				receipt.Summary,
				now,
				taskID,
				workspaceID,
				claimSnapshot.UpdatedAt,
			)
			if err != nil {
				return fmt.Errorf("update task claim for receipt terminalization: %w", err)
			}
			if affected, _ := res.RowsAffected(); affected != 1 {
				return ErrTaskClaimStaleTransition
			}
			clearOptions := taskClaimProjectAdmissionClearOptions{
				AllowReceiptBackedCompletedBranchRelease: true,
			}
			if _, err := clearTaskClaimProjectAdmissionTx(ctx, tx, workspaceID, taskID, claimSnapshot.AgentID, targetClaimStatus, now, claimSnapshot, clearOptions); err != nil {
				return err
			}
		}
		if err := s.resolveOpenOperatorQueuesForClosedTaskTx(ctx, tx, workspaceID, taskID, resolution, actorID, now); err != nil {
			return err
		}
		if _, err := closeTaskExecutionRunsTx(ctx, tx, workspaceID, taskID, resolution, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "agent work required receipt terminalization"); err != nil {
			return err
		}
		payload := map[string]any{
			"workspace_id":             workspaceID,
			"task_id":                  taskID,
			"agent_id":                 actorID,
			"claim_status":             targetClaimStatus,
			"resolution":               resolution,
			"summary":                  receipt.Summary,
			"terminalization_source":   "required_receipt_sweep",
			"required_receipt_kind":    receipt.Kind,
			"runtime_agent_id":         runtimeAgentID,
			"prior_task_status":        taskSnapshot.Status,
			"prior_task_updated_at":    taskSnapshot.UpdatedAt,
			"terminalization_actor_id": agentWorkRequiredReceiptTerminalizerActorID,
		}
		if hasClaim {
			payload["prior_claim_status"] = claimSnapshot.ClaimStatus
			payload["prior_claim_agent_id"] = claimSnapshot.AgentID
		}
		for key, value := range receipt.Payload {
			if _, exists := payload[key]; !exists {
				payload[key] = value
			}
		}
		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:     nextID("audit"),
			EventType:   "task_required_receipt_terminalized",
			EntityType:  "task",
			EntityID:    taskID,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
		}); err != nil {
			return err
		}
		eventType := "task.completed"
		switch resolution {
		case model.TaskStatusFailed:
			eventType = "task.failed"
		case model.TaskStatusCancelled:
			eventType = "task.cancelled"
		}
		if _, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   eventType,
			EntityType:  "task",
			EntityID:    taskID,
			ActorType:   "system",
			ActorID:     agentWorkRequiredReceiptTerminalizerActorID,
			AgentID:     runtimeAgentID,
			TaskID:      taskID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		}); err != nil {
			return err
		}
		changed = true
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return false, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if !changed {
		return false, nil
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit receipt terminalization tx: %w", err)
	}
	return true, nil
}
