package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// decisivePathContinuationDeferTTL bounds the deferred (awaiting-role) state (I1): a continuation whose
// awaited role never materializes within this window escalates to a typed terminal instead of deferring
// forever (an unbounded defer would be the silent leak in a new form).
const decisivePathContinuationDeferTTL = 2 * time.Hour

// Stage-4 decisive-path materialization for patch-queue decision continuations: every continuation born in
// the outbox is route-classified by owner-satisfiability MODALITY and enacted as exactly one of mint
// (claimable) / defer (awaiting an acquirable role) / typed-terminal (owner can never satisfy). This is the
// storage consumer of the shared decisivePathRoute primitive (root A); it delegates the satisfiability read
// to decisivePathOwnerSatisfiabilityTx (which itself delegates to the claim-time predicates).

// decisivePathCarrierKindForFollowup maps a continuation FollowupKind to its decisive-path carrier kind so
// the route fn classifies it (and the fail-closed default catches an unrecognized kind).
func decisivePathCarrierKindForFollowup(followupKind string) string {
	switch strings.ToLower(strings.TrimSpace(followupKind)) {
	case "integration":
		return decisivePathKindPatchQueueIntegrationCont
	case "validation":
		return decisivePathKindPatchQueueValidation
	case "revision", "rebuild":
		return decisivePathKindPatchQueueRevision
	default:
		return decisivePathKindPatchQueueDecisionCont
	}
}

// decisivePathContinuationModalityTx resolves the route for a continuation at materialization time. The
// proposed owner is the non-'system' candidate chain; the lane drives the required role; the carrier scope
// is "" because no current continuation kind pins a fixed owner-scope (integration is INTEGRATOR/non-scoped,
// validation is role-less, revision is IMPLEMENTER with an AGENT-CHOSEN claim scope - so "" -> the predicate
// uses role-ownership, never the broken empty-scope coverage check). Returns the route + the resolved owner
// (the role holder for a role-routed lane, or the proposed owner otherwise).
func (s *Store) decisivePathContinuationModalityTx(ctx context.Context, tx *sql.Tx, record ProjectPatchQueueDecisionContinuationRecord, item ProjectPatchQueueItemRecord, actorID string) (DecisivePathRoute, string, error) {
	proposedOwner := firstNonEmpty(
		strings.TrimSpace(item.AgentID),
		strings.TrimSpace(item.PrincipalID),
		strings.TrimSpace(item.SubmittedBy),
		strings.TrimSpace(item.DecidedBy),
		strings.TrimSpace(actorID),
	)
	lane := projectPatchQueueDecisionContinuationProjectLane(record)
	modality, resolvedOwner, err := s.decisivePathOwnerSatisfiabilityTx(ctx, tx, record.WorkspaceID, record.ProjectID, proposedOwner, lane, "")
	if err != nil {
		return DecisivePathRoute{}, "", err
	}
	route := decisivePathRoute(DecisivePathInput{
		CarrierKind:         decisivePathCarrierKindForFollowup(record.FollowupKind),
		OwnerBound:          true,
		OwnerSatisfiability: modality,
	})
	return route, resolvedOwner, nil
}

// markProjectPatchQueueDecisionContinuationStateGuardedTx transitions an outbox row's state ONLY if it is
// still in the expected fromState, and reports whether it actually changed. This is the race-safety
// primitive (checklist #3/a): when the sweep and the role-acquire hook (or two sweeps) chase the same
// DEFERRED row, the guarded WHERE makes the second writer a no-op (changed=false) instead of a second mint -
// idempotent under concurrency, not merely under repetition. (The task PK is a second backstop: two creates
// of the same continuation_task_id collide, but the guard makes the loser a clean no-op, not an error.)
func (s *Store) markProjectPatchQueueDecisionContinuationStateGuardedTx(ctx context.Context, tx *sql.Tx, outboxID, fromState, toState, now string) (bool, error) {
	outboxID = strings.TrimSpace(outboxID)
	if outboxID == "" || strings.TrimSpace(toState) == "" || strings.TrimSpace(fromState) == "" {
		return false, fmt.Errorf("%w: continuation outbox_id, from-state and to-state are required", ErrProjectPatchQueueInvalid)
	}
	res, err := tx.ExecContext(ctx, `
UPDATE project_patch_queue_decision_continuation_outbox
   SET state = ?, updated_at = ?
 WHERE outbox_id = ? AND state = ?`,
		strings.ToUpper(strings.TrimSpace(toState)),
		strings.TrimSpace(now),
		outboxID,
		strings.ToUpper(strings.TrimSpace(fromState)),
	)
	if err != nil {
		return false, fmt.Errorf("guarded mark project patch queue decision continuation state: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("guarded mark continuation rows affected: %w", err)
	}
	return affected > 0, nil
}

// markProjectPatchQueueDecisionContinuationTerminalTx terminalizes a continuation whose owner can NEVER
// satisfy the required role, or whose deferral TTL is exhausted. It mints NO task, so there is NO closeTaskTx
// and NO CANCELLED leg - the typed terminal IS the outbox marked SUPPRESSED plus a typed, observable runtime
// event (fence by-construction, checklist #6; the universal CANCELLED close stays unfenced-by-construction).
// Guarded so a racing sweep/hook is a clean no-op.
func (s *Store) markProjectPatchQueueDecisionContinuationTerminalTx(ctx context.Context, tx *sql.Tx, record ProjectPatchQueueDecisionContinuationRecord, reason, now string) error {
	// From-state guard (hardening): only a NON-materialized row (PENDING/DEFERRED) may be terminalized here. A
	// CONSUMED row owns a minted carrier; terminalizing it would orphan the task with no CANCELLED leg. SUPPRESSED
	// is already terminal. This is a backstop so a future caller (now 5: NEVER/undetermined materialize + sweep
	// TTL + sweep staleness + consume terminal) cannot accidentally suppress a minted continuation.
	if !strings.EqualFold(strings.TrimSpace(record.State), "PENDING") && !strings.EqualFold(strings.TrimSpace(record.State), "DEFERRED") {
		return nil
	}
	changed, err := s.markProjectPatchQueueDecisionContinuationStateGuardedTx(ctx, tx, record.OutboxID, record.State, "SUPPRESSED", now)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	payload := map[string]any{
		"workspace_id":    record.WorkspaceID,
		"project_id":      record.ProjectID,
		"queue_id":        record.QueueID,
		"item_id":         record.ItemID,
		"followup_kind":   record.FollowupKind,
		"outbox_id":       record.OutboxID,
		"terminal_reason": strings.TrimSpace(reason),
		"summary":         "Patch queue decision continuation terminalized (owner cannot satisfy required role or defer TTL exhausted); no unclaimable task minted",
	}
	_, err = s.appendRuntimeEventTx(ctx, tx, RuntimeEventInput{
		WorkspaceID: record.WorkspaceID,
		EventType:   "project.patch_queue.decision_continuation.terminal",
		EntityType:  "project_patch_queue_decision_continuation",
		EntityID:    strings.TrimSpace(record.OutboxID),
		ActorType:   "system",
		ActorID:     "system",
		PayloadJSON: mustJSON(payload),
		CreatedAt:   now,
	})
	return err
}

// decisivePathContinuationDeferExpired reports whether a DEFERRED continuation has exceeded the bounded TTL
// (I1). The clock is updated_at (set when the row was marked DEFERRED); an unparseable timestamp is
// conservatively NOT expired (the sweep re-checks). The sweep never re-marks a still-awaiting row, so the
// clock is preserved and the TTL actually fires.
func decisivePathContinuationDeferExpired(record ProjectPatchQueueDecisionContinuationRecord, now string) bool {
	deferredAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(record.UpdatedAt))
	if err != nil {
		return false
	}
	nowT, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(now))
	if err != nil {
		return false
	}
	return nowT.Sub(deferredAt) > decisivePathContinuationDeferTTL
}

// ReconcileDeferredProjectPatchQueueContinuations is the PRIMARY re-attempt mechanism (I2): a periodic sweep
// that guarantees liveness for DEFERRED continuations independent of any event hook (a hook can be missed or
// raced; this sweep always re-attempts). It scans BY STATE = DEFERRED, so a row deferred BEFORE the sweep
// ever ran (e.g. across a resume) is still picked up (checklist #8 boundary), and SUPPRESSED rows are never
// scanned (checklist #10). Each row is re-classified by the SAME predicate and either materialized (the
// awaited role now exists), left untouched (still awaiting, within TTL), or escalated to typed-terminal (TTL
// exhausted, I1). Idempotent under concurrency via the guarded transition (checklist #3/a).
func (s *Store) ReconcileDeferredProjectPatchQueueContinuations(ctx context.Context, workspaceID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil
	}
	// Scan DEFERRED (the normal awaiting-role state) AND PENDING (the pre-stage-4 #6 backlog the OLD code stranded
	// with no consumer = frozen items). Both are non-terminal "needs routing" states; the per-row reconcile
	// re-routes each via the modality (PENDING gets NO TTL - see reconcile). SUPPRESSED/CONSUMED are terminal/done
	// and never scanned (checklist #10). New code never rests PENDING (transient-within-create-tx), so this only
	// ever catches stranded legacy rows.
	deferred, err := s.ListProjectPatchQueueDecisionContinuations(ctx, ProjectPatchQueueDecisionContinuationFilter{
		WorkspaceID: workspaceID,
		State:       "DEFERRED",
	})
	if err != nil {
		// Journal the sweep-LEVEL failure (A1 hardening): work-next swallows this return value to keep selection
		// alive, so a permanently-broken list query would otherwise silently disable continuation liveness with no
		// signal at all. Best-effort journal, then propagate for any caller that does check.
		s.journalDeferredProjectPatchQueueContinuationSweepFailure(ctx, workspaceID, err)
		return err
	}
	pending, err := s.ListProjectPatchQueueDecisionContinuations(ctx, ProjectPatchQueueDecisionContinuationFilter{
		WorkspaceID: workspaceID,
		State:       "PENDING",
	})
	if err != nil {
		s.journalDeferredProjectPatchQueueContinuationSweepFailure(ctx, workspaceID, err)
		return err
	}
	deferred = append(deferred, pending...)
	for _, record := range deferred {
		if err := s.reconcileDeferredProjectPatchQueueContinuation(ctx, workspaceID, strings.TrimSpace(record.ProjectID), strings.TrimSpace(record.OutboxID)); err != nil {
			// Fault isolation (A1): one poisoned row (a transient per-row error, or a carrier the submit gate keeps
			// rejecting) must NOT abort the sweep. Aborting would starve every other DEFERRED row AND - because the
			// sweep runs in the work-next reconciler slot - wedge selection fleet-wide. Journal the skip and
			// continue; the next poll retries, and the bounded TTL escalates a permanently-poisoned row to a typed
			// terminal (the single-mint-chokepoint reuse pre-check already removes the common duplicate-identity poison).
			s.journalDeferredProjectPatchQueueContinuationSweepSkip(ctx, workspaceID, record, err)
			continue
		}
	}
	return nil
}

// journalDeferredProjectPatchQueueContinuationSweepSkip records (best-effort) that the sweep skipped a per-row
// error under fault isolation. It opens its own short tx and swallows all errors - it must never be able to wedge
// the very sweep it reports on.
func (s *Store) journalDeferredProjectPatchQueueContinuationSweepSkip(ctx context.Context, workspaceID string, record ProjectPatchQueueDecisionContinuationRecord, cause error) {
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
		EventType:   "project.patch_queue.decision_continuation.sweep_skipped",
		EntityType:  "project_patch_queue_decision_continuation",
		EntityID:    strings.TrimSpace(record.OutboxID),
		ActorType:   "system",
		ActorID:     "system",
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id":  workspaceID,
			"project_id":    strings.TrimSpace(record.ProjectID),
			"queue_id":      strings.TrimSpace(record.QueueID),
			"item_id":       strings.TrimSpace(record.ItemID),
			"outbox_id":     strings.TrimSpace(record.OutboxID),
			"followup_kind": strings.TrimSpace(record.FollowupKind),
			"decision":      strings.TrimSpace(record.Decision),
			"cause":         cause.Error(),
			"summary":       "Deferred continuation sweep skipped a per-row error (fault isolation); row left DEFERRED for retry and bounded-TTL escalation",
		}),
		CreatedAt: now,
	}); err != nil {
		return
	}
	_ = tx.Commit()
}

// journalDeferredProjectPatchQueueContinuationSweepFailure records (best-effort) a sweep-LEVEL failure (e.g. the
// DEFERRED list query failing) so a permanently-broken sweep is observable even though work-next swallows the
// error to keep selection alive. Opens its own short tx and swallows all errors - it must never wedge anything.
func (s *Store) journalDeferredProjectPatchQueueContinuationSweepFailure(ctx context.Context, workspaceID string, cause error) {
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
		EventType:   "project.patch_queue.decision_continuation.sweep_failed",
		EntityType:  "project_patch_queue_decision_continuation_sweep",
		EntityID:    strings.TrimSpace(workspaceID),
		ActorType:   "system",
		ActorID:     "system",
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id": workspaceID,
			"cause":        cause.Error(),
			"summary":      "Deferred continuation sweep failed at the sweep level (e.g. list query); DEFERRED-continuation liveness is degraded until it recovers",
		}),
		CreatedAt: now,
	}); err != nil {
		return
	}
	_ = tx.Commit()
}

func (s *Store) reconcileDeferredProjectPatchQueueContinuation(ctx context.Context, workspaceID, projectID, outboxID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin deferred continuation sweep tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		// Re-read under the write lock: the row may have transitioned since the scan (race).
		record, ok, err := projectPatchQueueDecisionContinuationTx(ctx, tx, workspaceID, projectID, outboxID, "", "")
		if err != nil {
			return err
		}
		// Drain both DEFERRED (awaiting a role) AND PENDING (the pre-stage-4 #6 backlog: continuations the OLD code
		// left PENDING-with-no-consumer = the frozen items). The modality below re-routes each to mint / defer /
		// terminal. For new code PENDING is transient-within-create-tx, so the sweep never observes it - this only
		// catches stranded legacy rows.
		rowState := strings.ToUpper(strings.TrimSpace(record.State))
		if !ok || (rowState != "DEFERRED" && rowState != "PENDING") {
			return nil
		}
		// TTL escalation (I1): bounded defer, never infinite - but ONLY for DEFERRED rows, whose updated_at IS the
		// defer clock. A stranded PENDING row's updated_at is its creation time (possibly days old), which is NOT a
		// defer clock; applying the TTL to it would wrongly terminalize the backlog instead of routing it. PENDING
		// goes straight to the modality below (and a defer outcome there restarts the clock fresh).
		if rowState == "DEFERRED" && decisivePathContinuationDeferExpired(record, now) {
			return s.markProjectPatchQueueDecisionContinuationTerminalTx(ctx, tx, record, "awaiting_role_ttl_exceeded:"+strings.TrimSpace(record.FollowupKind), now)
		}
		item, ok, err := getProjectPatchQueueItemTx(ctx, tx, record.QueueID, record.ItemID)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		// Staleness guard (mirrors the explicit-consume guard in ConsumeProjectPatchQueueDecisionContinuation):
		// a continuation's outbox row is keyed on the DECISION it was born for, but the item may since have left
		// that state - e.g. an ACCEPTED item (integration continuation deferred for want of an INTEGRATOR) later
		// defeated to BLOCKED. Re-materializing such a stale row would mint a dead-end carrier (an integration
		// task for a no-longer-ACCEPTED item can never pass the integration-receipt gate). Terminalize the
		// orphaned row instead - no task, SUPPRESSED, fence by construction - never re-classify/mint it.
		if !strings.EqualFold(strings.TrimSpace(item.State), strings.TrimSpace(record.Decision)) {
			return s.markProjectPatchQueueDecisionContinuationTerminalTx(ctx, tx, record, "item_state_diverged_from_decision:"+strings.TrimSpace(record.Decision), now)
		}
		// actorID fallback is "" - the proposed-owner chain already includes item.DecidedBy/SubmittedBy/etc.
		route, resolvedOwner, err := s.decisivePathContinuationModalityTx(ctx, tx, record, item, "")
		if err != nil {
			return err
		}
		switch route.Route {
		case decisivePathRouteYield:
			// The awaited role exists. Claim the row FIRST (guarded DEFERRED->CONSUMED) so a racing sweep/hook
			// is a no-op, THEN reuse-or-mint via the SINGLE chokepoint (A1: the same idempotent reuse pre-check
			// the materializer and consume use - so a continuation whose carrier already exists is reused, not
			// re-minted into a submit-gate duplicate that would error every cycle).
			changed, err := s.markProjectPatchQueueDecisionContinuationStateGuardedTx(ctx, tx, record.OutboxID, record.State, "CONSUMED", now)
			if err != nil {
				return err
			}
			if !changed {
				return nil
			}
			_, _, err = s.reuseOrMintProjectPatchQueueDecisionContinuationCarrierTx(ctx, tx, authority, record, item, resolvedOwner, "", "system", now)
			return err
		case decisivePathRouteDeferred:
			// Still awaiting a role. A DEFERRED row is left UNTOUCHED so updated_at (the TTL clock) is preserved. A
			// stranded PENDING row is transitioned PENDING->DEFERRED so the bounded-TTL clock STARTS now (guarded so
			// a racing writer is a clean no-op).
			if rowState == "PENDING" {
				_, err := s.markProjectPatchQueueDecisionContinuationStateGuardedTx(ctx, tx, record.OutboxID, record.State, "DEFERRED", now)
				return err
			}
			return nil
		default:
			return s.markProjectPatchQueueDecisionContinuationTerminalTx(ctx, tx, record, route.Reason, now)
		}
	}); err != nil {
		_ = tx.Rollback()
		return s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	return tx.Commit()
}
