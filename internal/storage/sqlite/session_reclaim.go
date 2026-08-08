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

const (
	// Runtime daemons send heartbeats and session keepalives on the heartbeat loop,
	// which defaults to 2 minutes. Reclaim must tolerate at least one full
	// heartbeat window plus maintenance jitter, otherwise a live session can be
	// reclaimed before its next keepalive lands.
	localOwnershipReclaimGrace = 5 * time.Minute
)

type LocalSessionOwnershipReclaimInput struct {
	Scope       string `json:"scope,omitempty"`
	ActorType   string `json:"actor_type,omitempty"`
	ActorID     string `json:"actor_id,omitempty"`
	ReferenceAt string `json:"reference_at,omitempty"`
}

type LocalSessionOwnershipReclaimItem struct {
	WorkspaceID      string `json:"workspace_id"`
	SessionID        string `json:"session_id"`
	AgentID          string `json:"agent_id,omitempty"`
	TaskID           string `json:"task_id,omitempty"`
	SessionUpdatedAt string `json:"session_updated_at,omitempty"`
	AgentLastSeenAt  string `json:"agent_last_seen_at,omitempty"`
	SessionAction    string `json:"session_action"`
	QueueAction      string `json:"queue_action,omitempty"`
	TaskAction       string `json:"task_action,omitempty"`
	Error            string `json:"error,omitempty"`
}

type LocalSessionOwnershipReclaimResult struct {
	Scope                 string                             `json:"scope"`
	ReferenceAt           string                             `json:"reference_at"`
	SessionsEnded         int                                `json:"sessions_ended"`
	SessionQueuesResolved int                                `json:"session_queues_resolved"`
	TaskClaimsReleased    int                                `json:"task_claims_released"`
	SkippedRecent         int                                `json:"skipped_recent"`
	Problems              int                                `json:"problems"`
	Items                 []LocalSessionOwnershipReclaimItem `json:"items,omitempty"`
}

type localSessionReclaimCandidate struct {
	WorkspaceID            string
	SessionID              string
	HolderAuthorityNodeID  string
	ObservedAuthorityLease string
	ObservedAuthorityTerm  int64
}

func parseSessionReclaimReferenceAt(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Now().UTC(), nil
	}
	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts, nil
	}
	if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return ts, nil
	}
	return time.Time{}, fmt.Errorf("reference_at must be RFC3339/RFC3339Nano, got %q", raw)
}

func parseSessionReclaimObservedAt(fieldName, raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, nil
	}
	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts, nil
	}
	if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return ts, nil
	}
	return time.Time{}, fmt.Errorf("%s must be RFC3339/RFC3339Nano, got %q", fieldName, raw)
}

func localSessionOwnershipAbandoned(sessionUpdatedAt, agentLastSeenAt string, referenceAt time.Time) (bool, error) {
	sessionObservedAt, err := parseSessionReclaimObservedAt("session_updated_at", sessionUpdatedAt)
	if err != nil {
		return false, err
	}
	agentObservedAt, err := parseSessionReclaimObservedAt("agent_last_seen_at", agentLastSeenAt)
	if err != nil {
		return false, err
	}
	cutoff := referenceAt.Add(-localOwnershipReclaimGrace)
	if !sessionObservedAt.IsZero() && !sessionObservedAt.Before(cutoff) {
		return false, nil
	}
	if !agentObservedAt.IsZero() && !agentObservedAt.Before(cutoff) {
		return false, nil
	}
	return true, nil
}

func (s *Store) listLocalSessionReclaimCandidates(ctx context.Context, scope, holderAuthorityNodeID string) ([]localSessionReclaimCandidate, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT DISTINCT wa.workspace_id, sessions.session_id, wa.holder_authority_node_id, wa.lease_token, wa.term
		   FROM workspace_authority wa
		   JOIN agent_sessions sessions ON sessions.workspace_id = wa.workspace_id
		  WHERE wa.scope = ?
		    AND wa.status = ?
		    AND wa.holder_authority_node_id = ?
		    AND sessions.status IN (?, ?, ?, ?, ?)
		  ORDER BY wa.workspace_id, sessions.session_id`,
		normalizeWorkspaceAuthorityScope(scope),
		string(WorkspaceAuthorityStatusActive),
		strings.TrimSpace(holderAuthorityNodeID),
		model.SessionStatusActive,
		model.SessionStatusBlocked,
		model.SessionStatusWaitingDecision,
		model.SessionStatusHandoffPending,
		"RUNNING",
	)
	if err != nil {
		return nil, fmt.Errorf("list local session reclaim candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]localSessionReclaimCandidate, 0, 8)
	for rows.Next() {
		var candidate localSessionReclaimCandidate
		if err := rows.Scan(
			&candidate.WorkspaceID,
			&candidate.SessionID,
			&candidate.HolderAuthorityNodeID,
			&candidate.ObservedAuthorityLease,
			&candidate.ObservedAuthorityTerm,
		); err != nil {
			return nil, fmt.Errorf("scan local session reclaim candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate local session reclaim candidates: %w", err)
	}
	return candidates, nil
}

func lookupAgentLastSeenAtTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID string) (string, error) {
	var lastSeenAt sql.NullString
	if err := tx.QueryRowContext(
		ctx,
		`SELECT last_seen_at FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID,
		agentID,
	).Scan(&lastSeenAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrAgentNotFound
		}
		return "", fmt.Errorf("query agent last_seen_at: %w", err)
	}
	return strings.TrimSpace(lastSeenAt.String), nil
}

func (s *Store) ReclaimAbandonedSessionOwnership(ctx context.Context, input LocalSessionOwnershipReclaimInput) (LocalSessionOwnershipReclaimResult, error) {
	scope := normalizeWorkspaceAuthorityScope(input.Scope)
	actorType, actorID, err := normalizeLocalAuthorityActor(input.ActorType, input.ActorID)
	if err != nil {
		return LocalSessionOwnershipReclaimResult{}, err
	}
	if actorType == authorityActorTypeSystem && strings.TrimSpace(actorID) == "authority_spine" {
		actorID = "local_reclaim_manager"
	}
	referenceAt, err := parseSessionReclaimReferenceAt(input.ReferenceAt)
	if err != nil {
		return LocalSessionOwnershipReclaimResult{}, err
	}
	localNode, err := s.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		return LocalSessionOwnershipReclaimResult{}, fmt.Errorf("ensure local authority node for session reclaim: %w", err)
	}
	candidates, err := s.listLocalSessionReclaimCandidates(ctx, scope, localNode.AuthorityNodeID)
	if err != nil {
		return LocalSessionOwnershipReclaimResult{}, err
	}

	result := LocalSessionOwnershipReclaimResult{
		Scope:       scope,
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
		Items:       make([]LocalSessionOwnershipReclaimItem, 0, len(candidates)),
	}
	for _, candidate := range candidates {
		item := LocalSessionOwnershipReclaimItem{
			WorkspaceID:   candidate.WorkspaceID,
			SessionID:     candidate.SessionID,
			SessionAction: "UNCHANGED",
			QueueAction:   "UNCHANGED",
			TaskAction:    "UNCHANGED",
		}

		if s.beforeLocalSessionReclaimTxHook != nil {
			s.beforeLocalSessionReclaimTxHook(ctx, candidate)
		}

		tx, txErr := s.BeginTxImmediate(ctx)
		if txErr != nil {
			item.SessionAction = "PROBLEM"
			item.Error = fmt.Sprintf("begin session reclaim tx: %v", txErr)
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
				item.SessionAction = "PROBLEM"
				item.Error = "session reclaim candidate is missing observed authority fence"
				result.Problems++
				return
			}
			if _, fenceErr := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
				state, err := s.getAgentSessionStateTx(ctx, tx, candidate.WorkspaceID, candidate.SessionID)
				if err != nil {
					return err
				}
				item.AgentID = state.AgentID
				item.TaskID = state.TaskID
				item.SessionUpdatedAt = state.UpdatedAt
				if !model.IsSessionStatusActive(state.Status) {
					item.SessionAction = "UNCHANGED"
					return nil
				}

				lastSeenAt, err := lookupAgentLastSeenAtTx(ctx, tx, candidate.WorkspaceID, state.AgentID)
				if err != nil {
					if errors.Is(err, ErrAgentNotFound) {
						// CA-22 (assessed WontFix): treating a missing agents row as
						// reclaimable is intentional and tested
						// (TestReclaimAbandonedSessionOwnershipReclaimsMissingAgentSession): a
						// deregistered/removed agent is gone, so its stale session SHOULD be
						// reclaimed. The audit's edge (agent row deleted mid-work while the
						// session's own UpdatedAt is also >5m stale) conflicts with that
						// primary behavior; closing it safely needs a second independent
						// liveness signal (refresh state.UpdatedAt via keepalive agent_update)
						// rather than failing closed here, which would regress missing-agent
						// reclaim. Tracked as a follow-up in the coordination audit tracker.
						lastSeenAt = ""
					} else {
						return err
					}
				}
				item.AgentLastSeenAt = lastSeenAt

				abandoned, err := localSessionOwnershipAbandoned(state.UpdatedAt, lastSeenAt, referenceAt)
				if err != nil {
					return err
				}
				if !abandoned {
					item.SessionAction = "SKIPPED_RECENT"
					item.TaskAction = "SKIPPED_RECENT"
					result.SkippedRecent++
					return nil
				}

				endedState, _, err := s.recordAgentSessionCoordinationWithAuthorityActorTx(ctx, tx, authority, AgentSessionCoordinationInput{
					EventType:           model.SessionEventEnd,
					WorkspaceID:         candidate.WorkspaceID,
					SessionID:           candidate.SessionID,
					AgentID:             state.AgentID,
					TaskID:              state.TaskID,
					Summary:             "Local ownership reclaim after stale agent inactivity",
					Status:              model.SessionStatusEnded,
					OwnerScope:          state.OwnerScope,
					BlockedOn:           state.BlockedOn,
					DecisionNeededFrom:  state.DecisionNeededFrom,
					DecisionType:        state.DecisionType,
					KeepSessionActive:   localBoolPtr(false),
					HandoffTo:           "",
					RelatedDocKeys:      state.RelatedDocKeys,
					RelatedArtifactRefs: state.RelatedArtifactRefs,
					UpdatedAt:           result.ReferenceAt,
				}, actorType, actorID)
				if err != nil {
					return err
				}
				item.SessionAction = "RECLAIMED"
				result.SessionsEnded++

				queueSyncResult, err := s.syncOperatorQueueFromSessionStateWithAuthorityTx(ctx, tx, authority, endedState, result.ReferenceAt, actorID)
				if err != nil {
					return err
				}
				if len(queueSyncResult.Resolved) > 0 {
					item.QueueAction = "RESOLVED"
					result.SessionQueuesResolved += len(queueSyncResult.Resolved)
				} else {
					item.QueueAction = "NO_QUEUE"
				}

				if strings.TrimSpace(endedState.TaskID) == "" {
					item.TaskAction = "NO_TASK"
					return nil
				}

				var claimAgentID, claimStatus string
				if err := tx.QueryRowContext(
					ctx,
					`SELECT agent_id, claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
					candidate.WorkspaceID,
					endedState.TaskID,
				).Scan(&claimAgentID, &claimStatus); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						item.TaskAction = "NO_CLAIM"
						return nil
					}
					return fmt.Errorf("query task claim for reclaim: %w", err)
				}
				if !strings.EqualFold(strings.TrimSpace(claimAgentID), endedState.AgentID) || claimStatus != model.TaskClaimStatusClaimed {
					item.TaskAction = "UNCHANGED"
					return nil
				}

				activeSessionID, _, err := lookupActiveSessionForTaskTx(ctx, tx, candidate.WorkspaceID, endedState.TaskID, endedState.AgentID)
				if err != nil {
					return err
				}
				if activeSessionID != "" {
					item.TaskAction = "SKIPPED_ACTIVE_SESSION"
					return nil
				}

				activeExecutionState, err := lookupActiveExecutionEvidenceForTaskTx(ctx, tx, candidate.WorkspaceID, endedState.TaskID)
				if err != nil {
					return err
				}
				if activeExecutionState != "" {
					item.TaskAction = "SKIPPED_ACTIVE_EXECUTION"
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
					TaskID:      endedState.TaskID,
					AgentID:     endedState.AgentID,
					Reason:      "Local ownership reclaim after stale agent inactivity",
				}, actorType, actorID, result.ReferenceAt, "reclaim_release"); err != nil {
					return err
				}
				item.TaskAction = "RELEASED"
				result.TaskClaimsReleased++
				return nil
			}); fenceErr != nil {
				_ = tx.Rollback()
				item.SessionAction = "PROBLEM"
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
				item.SessionAction = "PROBLEM"
				item.Error = fmt.Sprintf("commit session reclaim tx: %v", err)
				result.Problems++
				return
			}
		}()

		result.Items = append(result.Items, item)
	}

	return result, nil
}

func localBoolPtr(value bool) *bool {
	return &value
}
