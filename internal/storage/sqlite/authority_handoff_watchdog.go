package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// authorityHandoffWatchdogActorID is the SYSTEM sentinel actor that owns the
// authority/runtime block events emitted by this watchdog. It mirrors the
// naming convention used by the claim-liberation watchdog
// (claim_liberation.go ~345, "claim_liberation_watchdog"). It is used as the
// runtime/audit actor id, NOT necessarily as the task_claims.agent_id - see the
// agent_id selection note in terminalizeStaleAuthorityHandoffCarrier.
const authorityHandoffWatchdogActorID = "authority_handoff_watchdog"

// authorityHandoffFollowThroughBlockerSchema is the claim summary / runtime
// payload evidence schema written when this watchdog system-blocks a carrier,
// mirroring claim_liberation_watchdog_evidence.v1.
const authorityHandoffFollowThroughBlockerSchema = "authority_handoff_follow_through_blocker.v1"

// ReconcileStaleAuthorityHandoffCarriersInput mirrors the ClaimLiberation
// reconcile input shape (Scope/ActorType/ActorID/ReferenceAt/Limit).
type ReconcileStaleAuthorityHandoffCarriersInput struct {
	Scope       string `json:"scope,omitempty"`
	ActorType   string `json:"actor_type,omitempty"`
	ActorID     string `json:"actor_id,omitempty"`
	ReferenceAt string `json:"reference_at,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

// ReconcileStaleAuthorityHandoffCarriersItem records the per-carrier outcome.
type ReconcileStaleAuthorityHandoffCarriersItem struct {
	WorkspaceID         string `json:"workspace_id"`
	TaskID              string `json:"task_id"`
	ProjectID           string `json:"project_id,omitempty"`
	StaleSignalUpdateID string `json:"stale_signal_update_id,omitempty"`
	StaleSignalCreated  string `json:"stale_signal_created_at,omitempty"`
	BlockedAgentID      string `json:"blocked_agent_id,omitempty"`
	Action              string `json:"action"`
	SkipReason          string `json:"skip_reason,omitempty"`
	Error               string `json:"error,omitempty"`
}

// ReconcileStaleAuthorityHandoffCarriersResult mirrors the claim-liberation
// reconcile result shape (Scanned/Changed/Skipped/Problems/Items).
type ReconcileStaleAuthorityHandoffCarriersResult struct {
	Scope       string                                       `json:"scope"`
	ReferenceAt string                                       `json:"reference_at"`
	Scanned     int                                          `json:"scanned"`
	Changed     int                                          `json:"changed"`
	Skipped     int                                          `json:"skipped"`
	Problems    int                                          `json:"problems"`
	Items       []ReconcileStaleAuthorityHandoffCarriersItem `json:"items,omitempty"`
}

type staleAuthorityHandoffCarrierCandidate struct {
	WorkspaceID         string
	TaskID              string
	ProjectID           string
	StaleSignalUpdateID string
	StaleSignalCreated  string
	DelegationState     string
	// Existing claim row (if any). HasClaim is false for a claimless carrier.
	HasClaim       bool
	ClaimAgentID   string
	ClaimStatus    string
	ClaimUpdatedAt string
}

// ReconcileStaleAuthorityHandoffCarriers terminalizes stale queued/admitted
// authority-handoff carriers so the agent receiver recognizes them as
// terminal. The contract (verified against agent
// runtime_agent_request_delegate.go authorityTransitionTaskHasTerminalBlocker):
// the receiver treats a carrier as terminal iff
// authorityTransitionTaskLooksDedicated(task) && claim_status=="BLOCKED". So
// this watchdog drives the carrier's task_claims.claim_status to BLOCKED (an
// operator-queue row or agent_update alone is NOT sufficient).
//
// This is the PRIMARY slice only: detection + system-block terminalization.
// It intentionally does NOT touch claim-admission (workspace.go:3866) and does
// NOT implement B2 alternate-holder recovery.
func (s *Store) ReconcileStaleAuthorityHandoffCarriers(ctx context.Context, input ReconcileStaleAuthorityHandoffCarriersInput) (ReconcileStaleAuthorityHandoffCarriersResult, error) {
	scope := normalizeWorkspaceAuthorityScope(input.Scope)
	actorType, actorID, err := normalizeLocalAuthorityActor(input.ActorType, input.ActorID)
	if err != nil {
		return ReconcileStaleAuthorityHandoffCarriersResult{}, err
	}
	if actorType == authorityActorTypeSystem && strings.TrimSpace(actorID) == "authority_spine" {
		actorID = authorityHandoffWatchdogActorID
	}
	if strings.TrimSpace(actorID) == "" {
		actorType = authorityActorTypeSystem
		actorID = authorityHandoffWatchdogActorID
	}
	referenceAt, err := parseSessionReclaimReferenceAt(input.ReferenceAt)
	if err != nil {
		return ReconcileStaleAuthorityHandoffCarriersResult{}, err
	}
	limit := input.Limit
	if limit <= 0 {
		limit = 32
	}
	localNode, err := s.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		return ReconcileStaleAuthorityHandoffCarriersResult{}, fmt.Errorf("ensure local authority node for authority handoff watchdog: %w", err)
	}
	candidates, err := s.listStaleAuthorityHandoffCarrierCandidates(ctx, scope, strings.TrimSpace(localNode.AuthorityNodeID), referenceAt, limit)
	if err != nil {
		return ReconcileStaleAuthorityHandoffCarriersResult{}, err
	}
	result := ReconcileStaleAuthorityHandoffCarriersResult{
		Scope:       scope,
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
		Items:       make([]ReconcileStaleAuthorityHandoffCarriersItem, 0, len(candidates)),
	}
	for _, candidate := range candidates {
		result.Scanned++
		item := ReconcileStaleAuthorityHandoffCarriersItem{
			WorkspaceID:         candidate.WorkspaceID,
			TaskID:              candidate.TaskID,
			ProjectID:           candidate.ProjectID,
			StaleSignalUpdateID: candidate.StaleSignalUpdateID,
			StaleSignalCreated:  candidate.StaleSignalCreated,
			Action:              "UNCHANGED",
		}
		blocked, blockedAgentID, skipReason, err := s.terminalizeStaleAuthorityHandoffCarrier(ctx, scope, candidate, actorType, actorID, result.ReferenceAt)
		if err != nil {
			item.Action = "PROBLEM"
			item.Error = err.Error()
			result.Problems++
			result.Items = append(result.Items, item)
			continue
		}
		if !blocked {
			item.Action = "SKIPPED"
			item.SkipReason = firstNonEmpty(strings.TrimSpace(skipReason), "not_blocked")
			result.Skipped++
			result.Items = append(result.Items, item)
			continue
		}
		item.Action = "BLOCKED"
		item.BlockedAgentID = blockedAgentID
		result.Changed++
		result.Items = append(result.Items, item)
	}
	return result, nil
}

// listStaleAuthorityHandoffCarrierCandidates keys off the durable handoff signal
// in agent_updates (mirroring the claim-liberation candidate CTE over
// agent_updates with json_extract over payload_json). The signal schema is the
// one the agent delegate path actually writes (verified in
// runtime_agent_request_delegate.go postDelegatedAuthorityTransitionClaimAdmittedUpdate /
// postDelegatedTaskClaimAdmittedUpdate / postDelegatedTaskActiveLaneNudgeUpdate):
//
//   - update_type = 'coordination'
//   - payload_json.delegation_state in ('claim_admitted','active_lane_resume_queued')
//   - the carrier task id is at payload_json.task_id for claim_admitted and at
//     payload_json.delegated_task_id for active_lane_resume_queued. We COALESCE
//     both so a single predicate covers either shape.
//
// Dedup is keyed on the specific stale update id: a carrier that already has a
// task.blocked runtime_event referencing this update id (or any already-BLOCKED
// task_claims row) is excluded so re-runs do nothing.
func (s *Store) listStaleAuthorityHandoffCarrierCandidates(ctx context.Context, scope, holderAuthorityNodeID string, referenceAt time.Time, limit int) ([]staleAuthorityHandoffCarrierCandidate, error) {
	if limit <= 0 {
		limit = 32
	}
	signalCutoff := referenceAt.Add(-localOwnershipReclaimGrace).UTC().Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `
WITH signal AS (
SELECT u.update_id,
       u.workspace_id,
       u.created_at,
       TRIM(COALESCE(json_extract(u.payload_json, '$.delegation_state'), '')) AS delegation_state,
       TRIM(COALESCE(
            json_extract(u.payload_json, '$.task_id'),
            json_extract(u.payload_json, '$.delegated_task_id'),
            '')) AS carrier_task_id
  FROM agent_updates u
 WHERE json_valid(u.payload_json)
   AND TRIM(COALESCE(json_extract(u.payload_json, '$.delegation_state'), '')) IN ('claim_admitted', 'active_lane_resume_queued')
   AND u.created_at <= ?
)
SELECT sig.update_id,
       sig.workspace_id,
       sig.created_at,
       sig.delegation_state,
       sig.carrier_task_id,
       COALESCE(t.project_id, ''),
       CASE WHEN tc.task_id IS NULL THEN 0 ELSE 1 END AS has_claim,
       COALESCE(tc.agent_id, ''),
       COALESCE(tc.claim_status, ''),
       COALESCE(tc.updated_at, '')
  FROM signal sig
  JOIN workspace_authority wa
    ON wa.workspace_id = sig.workspace_id
   AND wa.scope = ?
   AND wa.status = ?
   AND wa.holder_authority_node_id = ?
  JOIN workspace_tasks wt
    ON wt.workspace_id = sig.workspace_id
   AND wt.task_id = sig.carrier_task_id
  JOIN tasks t
    ON t.task_id = wt.task_id
  LEFT JOIN task_claims tc
    ON tc.workspace_id = sig.workspace_id
   AND tc.task_id = sig.carrier_task_id
 WHERE sig.carrier_task_id <> ''
   -- carrier is a dedicated authority/handoff carrier: id prefix OR tag membership.
   AND (
        sig.carrier_task_id LIKE 'task-role-scope-%'
     OR sig.carrier_task_id LIKE 'task-project-claim-repair-%'
     OR EXISTS (
          SELECT 1 FROM json_each(COALESCE(t.tags_json, '[]')) tag
           WHERE LOWER(TRIM(tag.value)) IN ('project-role-scope', 'project-claim-repair', 'project-repo-repair')
        )
   )
   -- task status is non-terminal.
   AND t.status NOT IN (?, ?, ?)
   -- no live active-ownership claim: no row, RELEASED, or any non-active status
   -- that is NOT already CLAIMED and NOT already BLOCKED. (Already-BLOCKED is
   -- excluded here so it is treated as terminal/idempotent, not re-blocked.)
   AND (tc.task_id IS NULL OR tc.claim_status NOT IN (?, ?))
   -- no runnable/active session for the carrier task (any agent).
   AND NOT EXISTS (
        SELECT 1 FROM agent_sessions sess
         WHERE sess.workspace_id = sig.workspace_id
           AND sess.task_id = sig.carrier_task_id
           AND sess.status IN (?, ?, ?, ?, ?)
   )
   -- dedup: do not re-block a carrier we already system-blocked for this
   -- specific stale signal update id.
   AND NOT EXISTS (
        SELECT 1 FROM runtime_events ev
         WHERE ev.workspace_id = sig.workspace_id
           AND ev.event_type = 'task.blocked'
           AND ev.entity_type = 'task'
           AND ev.entity_id = sig.carrier_task_id
           AND ev.payload_json LIKE '%' || sig.update_id || '%'
   )
 ORDER BY sig.created_at ASC, sig.update_id ASC
 LIMIT ?`,
		signalCutoff,
		normalizeWorkspaceAuthorityScope(scope),
		string(WorkspaceAuthorityStatusActive),
		strings.TrimSpace(holderAuthorityNodeID),
		model.TaskStatusResolved,
		model.TaskStatusFailed,
		model.TaskStatusCancelled,
		model.TaskClaimStatusClaimed,
		model.TaskClaimStatusBlocked,
		model.SessionStatusActive,
		model.SessionStatusBlocked,
		model.SessionStatusWaitingDecision,
		model.SessionStatusHandoffPending,
		"RUNNING",
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query stale authority handoff carrier candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]staleAuthorityHandoffCarrierCandidate, 0, limit)
	for rows.Next() {
		var candidate staleAuthorityHandoffCarrierCandidate
		var hasClaim int
		if err := rows.Scan(
			&candidate.StaleSignalUpdateID,
			&candidate.WorkspaceID,
			&candidate.StaleSignalCreated,
			&candidate.DelegationState,
			&candidate.TaskID,
			&candidate.ProjectID,
			&hasClaim,
			&candidate.ClaimAgentID,
			&candidate.ClaimStatus,
			&candidate.ClaimUpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan stale authority handoff carrier candidate: %w", err)
		}
		candidate.HasClaim = hasClaim == 1
		candidate.TaskID = strings.TrimSpace(candidate.TaskID)
		candidate.ClaimAgentID = strings.TrimSpace(candidate.ClaimAgentID)
		candidate.ClaimStatus = strings.TrimSpace(candidate.ClaimStatus)
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stale authority handoff carrier candidates: %w", err)
	}
	return candidates, nil
}

// terminalizeStaleAuthorityHandoffCarrier system-blocks one carrier under the
// workspace authority fence, mirroring transitionTaskClaimWithEvent's block
// branch (UPDATE/INSERT task_claims -> claim_status=BLOCKED, addAuditEventTx,
// appendAuthorityBackedRuntimeEventTx type "task.blocked").
//
// agent_id selection (CRITICAL): task_claims.agent_id has
//
//	FOREIGN KEY (agent_id) REFERENCES agents(agent_id) ON DELETE CASCADE
//
// (migration 0007_agent_workspaces.sql). The BLOCKED claim is therefore always
// attributed to the carrier's EXISTING claim owner (the last agent that held the
// claim, now RELEASED/non-active) - a real registered agent, so the FK holds and
// no synthetic agent row is fabricated. A carrier with NO task_claims row at all
// (no prior owner) is SKIPPED ("claimless_no_prior_owner"): we never invent a
// sentinel agent, because an unexpected agents row would pollute the roster-
// registry parity preflight and could block the next round. In practice the
// targeted signals (claim_admitted / active_lane_resume_queued) imply a claim was
// admitted, so real targets have a claim row; the claimless case is a deliberate,
// logged no-op. The watchdog provenance is recorded in the evidence summary /
// runtime payload.
func (s *Store) terminalizeStaleAuthorityHandoffCarrier(ctx context.Context, scope string, candidate staleAuthorityHandoffCarrierCandidate, actorType, actorID, referenceAtRaw string) (bool, string, string, error) {
	workspaceID := strings.TrimSpace(candidate.WorkspaceID)
	taskID := strings.TrimSpace(candidate.TaskID)
	if workspaceID == "" || taskID == "" {
		return false, "", "", errors.New("workspace_id and task_id are required for authority handoff terminalization")
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, scope, referenceAtRaw)
	if err != nil {
		return false, "", "", err
	}

	evidence := mustJSON(map[string]any{
		"schema":                  authorityHandoffFollowThroughBlockerSchema,
		"stale_signal_update_id":  candidate.StaleSignalUpdateID,
		"stale_signal_created_at": candidate.StaleSignalCreated,
		"delegation_state":        candidate.DelegationState,
		"server_verified_predicates": []string{
			"carrier_open",
			"no_live_claim",
			"no_runnable_session",
			"handoff_signal_older_than_grace",
		},
	})

	var blocked bool
	var blockedAgentID string
	var skipReason string

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return false, "", "", fmt.Errorf("begin authority handoff terminalization tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		// Re-load the claim snapshot inside the fenced tx so the CAS and the
		// idempotency/liveness checks operate on a consistent view.
		snapshot, hasClaim, err := loadTaskClaimTransitionSnapshotTx(ctx, tx, taskID, workspaceID)
		if err != nil {
			return err
		}
		if hasClaim {
			switch snapshot.ClaimStatus {
			case model.TaskClaimStatusBlocked:
				// Already terminal - idempotent skip.
				skipReason = "already_blocked"
				return nil
			case model.TaskClaimStatusClaimed:
				// A live active-ownership claim reappeared since listing - leave it.
				skipReason = "live_claim_reappeared"
				return nil
			}
		}

		// Re-verify no runnable/active session for the task inside the fence.
		var activeSessionID string
		if err := tx.QueryRowContext(ctx, `
SELECT session_id
  FROM agent_sessions
 WHERE workspace_id = ?
   AND task_id = ?
   AND status IN (?, ?, ?, ?, ?)
 ORDER BY started_at DESC, session_id DESC
 LIMIT 1`,
			workspaceID,
			taskID,
			model.SessionStatusActive,
			model.SessionStatusBlocked,
			model.SessionStatusWaitingDecision,
			model.SessionStatusHandoffPending,
			"RUNNING",
		).Scan(&activeSessionID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("re-verify runnable session for authority handoff carrier: %w", err)
			}
		} else if strings.TrimSpace(activeSessionID) != "" {
			skipReason = "runnable_session_reappeared:" + strings.TrimSpace(activeSessionID)
			return nil
		}

		// FK-safe agent_id: attribute the BLOCKED claim to the carrier's existing
		// claim owner. A claimless carrier (no row, no prior owner) is skipped - we
		// never fabricate a sentinel agent (see method doc).
		if !hasClaim {
			skipReason = "claimless_no_prior_owner"
			return nil
		}
		blockAgentID := strings.TrimSpace(snapshot.AgentID)
		if blockAgentID == "" {
			skipReason = "claim_row_without_owner"
			return nil
		}

		// CAS on updated_at over the existing (RELEASED/non-active) row.
		res, err := tx.ExecContext(ctx,
			`UPDATE task_claims
			    SET agent_id = ?, claim_status = ?, summary = ?, updated_at = ?
			  WHERE task_id = ? AND workspace_id = ? AND updated_at = ?`,
			blockAgentID,
			model.TaskClaimStatusBlocked,
			evidence,
			referenceAtRaw,
			taskID,
			workspaceID,
			snapshot.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("system-block existing authority handoff carrier claim: %w", err)
		}
		if affected, _ := res.RowsAffected(); affected != 1 {
			return ErrTaskClaimStaleTransition
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, referenceAtRaw, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after authority handoff block: %w", err)
		}

		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "task_blocked",
			EntityType: "task_claim",
			EntityID:   taskID,
			ActorID:    actorID,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":           workspaceID,
				"task_id":                taskID,
				"agent_id":               blockAgentID,
				"watchdog":               authorityHandoffWatchdogActorID,
				"stale_signal_update_id": candidate.StaleSignalUpdateID,
				"reason":                 evidence,
			}),
		}); err != nil {
			return err
		}

		runtimeEventInput := RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "task.blocked",
			EntityType:  "task",
			EntityID:    taskID,
			ActorType:   actorType,
			ActorID:     actorID,
			AgentID:     blockAgentID,
			TaskID:      taskID,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":           workspaceID,
				"task_id":                taskID,
				"agent_id":               blockAgentID,
				"claim_status":           model.TaskClaimStatusBlocked,
				"reason":                 evidence,
				"watchdog":               authorityHandoffWatchdogActorID,
				"stale_signal_update_id": candidate.StaleSignalUpdateID,
			}),
			CreatedAt: referenceAtRaw,
		}
		if _, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, runtimeEventInput); err != nil {
			return err
		}

		blocked = true
		blockedAgentID = blockAgentID
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return false, "", "", s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if !blocked {
		return false, "", skipReason, nil
	}
	if err := tx.Commit(); err != nil {
		return false, "", "", fmt.Errorf("commit authority handoff terminalization tx: %w", err)
	}
	return true, blockedAgentID, "", nil
}
