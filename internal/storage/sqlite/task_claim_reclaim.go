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

const localOrphanClaimReclaimReason = "Local ownership reclaim after stale task claim without session"

type LocalTaskClaimOwnershipReclaimInput struct {
	Scope       string `json:"scope,omitempty"`
	ActorType   string `json:"actor_type,omitempty"`
	ActorID     string `json:"actor_id,omitempty"`
	ReferenceAt string `json:"reference_at,omitempty"`
}

type LocalTaskClaimOwnershipReclaimItem struct {
	WorkspaceID          string `json:"workspace_id"`
	TaskID               string `json:"task_id"`
	AgentID              string `json:"agent_id,omitempty"`
	ClaimUpdatedAt       string `json:"claim_updated_at,omitempty"`
	AgentLastSeenAt      string `json:"agent_last_seen_at,omitempty"`
	TaskAction           string `json:"task_action"`
	ActiveSessionID      string `json:"active_session_id,omitempty"`
	ActiveSessionAgentID string `json:"active_session_agent_id,omitempty"`
	ActiveExecutionState string `json:"active_execution_state,omitempty"`
	Error                string `json:"error,omitempty"`
}

type LocalTaskClaimOwnershipReclaimResult struct {
	Scope                  string                               `json:"scope"`
	ReferenceAt            string                               `json:"reference_at"`
	TaskClaimsReleased     int                                  `json:"task_claims_released"`
	SkippedRecent          int                                  `json:"skipped_recent"`
	SkippedActiveSession   int                                  `json:"skipped_active_session"`
	SkippedActiveExecution int                                  `json:"skipped_active_execution"`
	Problems               int                                  `json:"problems"`
	Items                  []LocalTaskClaimOwnershipReclaimItem `json:"items,omitempty"`
}

type localTaskClaimReclaimCandidate struct {
	WorkspaceID            string
	TaskID                 string
	AgentID                string
	HolderAuthorityNodeID  string
	ObservedAuthorityLease string
	ObservedAuthorityTerm  int64
}

func (s *Store) listLocalTaskClaimReclaimCandidates(ctx context.Context, scope, holderAuthorityNodeID string) ([]localTaskClaimReclaimCandidate, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT tc.workspace_id, tc.task_id, tc.agent_id, wa.holder_authority_node_id, wa.lease_token, wa.term
		   FROM workspace_authority wa
		   JOIN task_claims tc ON tc.workspace_id = wa.workspace_id
		  WHERE wa.scope = ?
		    AND wa.status = ?
		    AND wa.holder_authority_node_id = ?
		    AND tc.claim_status IN (?, ?)
		  ORDER BY tc.workspace_id, tc.task_id`,
		normalizeWorkspaceAuthorityScope(scope),
		string(WorkspaceAuthorityStatusActive),
		strings.TrimSpace(holderAuthorityNodeID),
		model.TaskClaimStatusClaimed,
		model.TaskClaimStatusBlocked,
	)
	if err != nil {
		return nil, fmt.Errorf("list local task claim reclaim candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]localTaskClaimReclaimCandidate, 0, 8)
	for rows.Next() {
		var candidate localTaskClaimReclaimCandidate
		if err := rows.Scan(
			&candidate.WorkspaceID,
			&candidate.TaskID,
			&candidate.AgentID,
			&candidate.HolderAuthorityNodeID,
			&candidate.ObservedAuthorityLease,
			&candidate.ObservedAuthorityTerm,
		); err != nil {
			return nil, fmt.Errorf("scan local task claim reclaim candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate local task claim reclaim candidates: %w", err)
	}
	return candidates, nil
}

func lookupActiveSessionForTaskTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string) (string, string, error) {
	expectedAgentID := strings.TrimSpace(agentID)
	if expectedAgentID == "" {
		return "", "", nil
	}
	var sessionID, sessionAgentID string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT session_id, agent_id
		   FROM agent_sessions
		  WHERE workspace_id = ?
		    AND task_id = ?
		    AND agent_id = ?
		    AND status IN (?, ?, ?, ?, ?)
		  ORDER BY started_at DESC, session_id DESC
		  LIMIT 1`,
		workspaceID,
		taskID,
		expectedAgentID,
		model.SessionStatusActive,
		model.SessionStatusBlocked,
		model.SessionStatusWaitingDecision,
		model.SessionStatusHandoffPending,
		"RUNNING",
	).Scan(&sessionID, &sessionAgentID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", nil
		}
		return "", "", fmt.Errorf("query active session for claimed task reclaim: %w", err)
	}
	sessionID = strings.TrimSpace(sessionID)
	sessionAgentID = strings.TrimSpace(sessionAgentID)
	if ok, err := executionSessionHasStartReceipt(ctx, tx, workspaceID, sessionID, sessionAgentID, taskID); err != nil {
		return "", "", err
	} else if !ok {
		return "", "", nil
	}
	return sessionID, sessionAgentID, nil
}

func lookupActiveExecutionEvidenceForTaskTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) (string, error) {
	var claimedNodeCount int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(1)
		   FROM node_claims
		  WHERE workspace_id = ?
		    AND task_id = ?
		    AND claim_status = ?`,
		workspaceID,
		taskID,
		model.TaskClaimStatusClaimed,
	).Scan(&claimedNodeCount); err != nil {
		return "", fmt.Errorf("query active node claims for orphan claim reclaim: %w", err)
	}

	var activeNodeCount int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(1)
		   FROM dag_nodes
		  WHERE task_id = ?
		    AND status IN (?, ?)`,
		taskID,
		model.NodeStatusRunning,
		model.NodeStatusAwaitingFunds,
	).Scan(&activeNodeCount); err != nil {
		return "", fmt.Errorf("query active dag nodes for orphan claim reclaim: %w", err)
	}

	evidence := make([]string, 0, 3)
	if claimedNodeCount > 0 {
		evidence = append(evidence, fmt.Sprintf("claimed_node_claims:%d", claimedNodeCount))
	}
	if activeNodeCount > 0 {
		evidence = append(evidence, fmt.Sprintf("active_nodes:%d", activeNodeCount))
	}
	return strings.Join(evidence, ","), nil
}

func (s *Store) ReclaimOrphanedClaimedTasks(ctx context.Context, input LocalTaskClaimOwnershipReclaimInput) (LocalTaskClaimOwnershipReclaimResult, error) {
	scope := normalizeWorkspaceAuthorityScope(input.Scope)
	actorType, actorID, err := normalizeLocalAuthorityActor(input.ActorType, input.ActorID)
	if err != nil {
		return LocalTaskClaimOwnershipReclaimResult{}, err
	}
	if actorType == authorityActorTypeSystem && strings.TrimSpace(actorID) == "authority_spine" {
		actorID = "local_reclaim_manager"
	}
	referenceAt, err := parseSessionReclaimReferenceAt(input.ReferenceAt)
	if err != nil {
		return LocalTaskClaimOwnershipReclaimResult{}, err
	}
	localNode, err := s.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		return LocalTaskClaimOwnershipReclaimResult{}, fmt.Errorf("ensure local authority node for orphan claim reclaim: %w", err)
	}
	candidates, err := s.listLocalTaskClaimReclaimCandidates(ctx, scope, localNode.AuthorityNodeID)
	if err != nil {
		return LocalTaskClaimOwnershipReclaimResult{}, err
	}

	result := LocalTaskClaimOwnershipReclaimResult{
		Scope:       scope,
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
		Items:       make([]LocalTaskClaimOwnershipReclaimItem, 0, len(candidates)),
	}
	for _, candidate := range candidates {
		item := LocalTaskClaimOwnershipReclaimItem{
			WorkspaceID: candidate.WorkspaceID,
			TaskID:      candidate.TaskID,
			AgentID:     candidate.AgentID,
			TaskAction:  "UNCHANGED",
		}

		if s.beforeLocalTaskClaimReclaimTxHook != nil {
			s.beforeLocalTaskClaimReclaimTxHook(ctx, candidate)
		}

		tx, txErr := s.BeginTxImmediate(ctx)
		if txErr != nil {
			item.TaskAction = "PROBLEM"
			item.Error = fmt.Sprintf("begin orphan claim reclaim tx: %v", txErr)
			result.Problems++
			result.Items = append(result.Items, item)
			continue
		}

		func() {
			defer func() { _ = tx.Rollback() }()

			fenceInput := WorkspaceAuthorityFenceInput{
				WorkspaceID:                   candidate.WorkspaceID,
				Scope:                         scope,
				ExpectedHolderAuthorityNodeID: strings.TrimSpace(candidate.HolderAuthorityNodeID),
				ExpectedLeaseToken:            strings.TrimSpace(candidate.ObservedAuthorityLease),
				ExpectedTerm:                  candidate.ObservedAuthorityTerm,
				ReferenceAt:                   result.ReferenceAt,
			}
			if fenceInput.ExpectedHolderAuthorityNodeID == "" || fenceInput.ExpectedLeaseToken == "" || fenceInput.ExpectedTerm <= 0 {
				item.TaskAction = "PROBLEM"
				item.Error = "orphan claim reclaim candidate is missing observed authority fence"
				result.Problems++
				return
			}
			if _, fenceErr := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
				var claimAgentID, claimStatus, claimUpdatedAt string
				if err := tx.QueryRowContext(
					ctx,
					`SELECT agent_id, claim_status, updated_at
					   FROM task_claims
					  WHERE workspace_id = ? AND task_id = ?`,
					candidate.WorkspaceID,
					candidate.TaskID,
				).Scan(&claimAgentID, &claimStatus, &claimUpdatedAt); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						item.TaskAction = "NO_CLAIM"
						return nil
					}
					return fmt.Errorf("query orphan claim candidate: %w", err)
				}
				item.AgentID = strings.TrimSpace(claimAgentID)
				item.ClaimUpdatedAt = strings.TrimSpace(claimUpdatedAt)
				if claimStatus != model.TaskClaimStatusClaimed && claimStatus != model.TaskClaimStatusBlocked {
					item.TaskAction = "UNCHANGED"
					return nil
				}

				activeSessionID, activeSessionAgentID, err := lookupActiveSessionForTaskTx(ctx, tx, candidate.WorkspaceID, candidate.TaskID, claimAgentID)
				if err != nil {
					return err
				}
				item.ActiveSessionID = activeSessionID
				item.ActiveSessionAgentID = activeSessionAgentID
				if activeSessionID != "" {
					item.TaskAction = "SKIPPED_ACTIVE_SESSION"
					result.SkippedActiveSession++
					return nil
				}

				activeExecutionState, err := lookupActiveExecutionEvidenceForTaskTx(ctx, tx, candidate.WorkspaceID, candidate.TaskID)
				if err != nil {
					return err
				}
				item.ActiveExecutionState = activeExecutionState
				if activeExecutionState != "" {
					item.TaskAction = "SKIPPED_ACTIVE_EXECUTION"
					result.SkippedActiveExecution++
					return nil
				}

				if claimStatus == model.TaskClaimStatusBlocked {
					pendingBlockingActions, err := s.countPendingBlockingActionsTx(ctx, tx, candidate.WorkspaceID, candidate.TaskID, "")
					if err != nil {
						return err
					}
					if pendingBlockingActions > 0 {
						item.TaskAction = "SKIPPED_PENDING_BLOCKER"
						return nil
					}
					if err := clearTaskClaimBlockerSnapshotTx(ctx, tx, candidate.TaskID, candidate.WorkspaceID); err != nil {
						return err
					}
				}

				lastSeenAt, err := lookupAgentLastSeenAtTx(ctx, tx, candidate.WorkspaceID, claimAgentID)
				if errors.Is(err, ErrAgentNotFound) {
					lastSeenAt = ""
				} else if err != nil {
					return err
				}
				item.AgentLastSeenAt = lastSeenAt
				abandoned, err := localSessionOwnershipAbandoned(claimUpdatedAt, lastSeenAt, referenceAt)
				if err != nil {
					return err
				}
				if !abandoned {
					item.TaskAction = "SKIPPED_RECENT"
					result.SkippedRecent++
					return nil
				}

				scarcity, err := s.ReviewerMeshScarcitySnapshot(ctx, candidate.WorkspaceID)
				if err != nil {
					return err
				}
				if strings.EqualFold(strings.TrimSpace(scarcity.Status), "SATURATED") {
					item.TaskAction = "SKIPPED_REVIEWER_SCARCITY"
					return nil
				}

				if _, err := s.releaseTaskClaimWithAuthorityActorTx(ctx, tx, authority, TaskReleaseInput{
					WorkspaceID: candidate.WorkspaceID,
					TaskID:      candidate.TaskID,
					AgentID:     claimAgentID,
					Reason:      localOrphanClaimReclaimReason,
				}, actorType, actorID, result.ReferenceAt, "reclaim_release"); err != nil {
					return err
				}
				item.TaskAction = "RELEASED"
				result.TaskClaimsReleased++
				return nil
			}); fenceErr != nil {
				_ = tx.Rollback()
				item.TaskAction = "PROBLEM"
				rejectErr := s.journalWorkspaceAuthorityReject(ctx, fenceErr, fenceInput)
				if rejectErr != nil {
					item.Error = rejectErr.Error()
				} else {
					item.Error = fenceErr.Error()
				}
				result.Problems++
				return
			}
			if err := tx.Commit(); err != nil {
				item.TaskAction = "PROBLEM"
				item.Error = fmt.Sprintf("commit orphan claim reclaim tx: %v", err)
				result.Problems++
				return
			}
		}()

		result.Items = append(result.Items, item)
	}

	return result, nil
}
