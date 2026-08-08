package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// OperatorQueueProjectionReconcileResult summarizes one sweep of the operator
// queue projection reconciler.
type OperatorQueueProjectionReconcileResult struct {
	Missing   int // sessions found missing their operator queue this pass
	Repaired  int // sessions for which the sweep created the missing queue
	Failed    int // sessions whose repair attempt errored
	Unchanged int // sessions still missing after the replay (e.g. session went terminal mid-sweep; CA-06 guard declined)
}

// TerminalSessionOperatorQueueReconcileInput configures the stale terminal-session
// operator queue cleanup pass.
type TerminalSessionOperatorQueueReconcileInput struct {
	Scope       string `json:"scope,omitempty"`
	ActorType   string `json:"actor_type,omitempty"`
	ActorID     string `json:"actor_id,omitempty"`
	ReferenceAt string `json:"reference_at,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

// TerminalSessionOperatorQueueReconcileItem records one terminal-session queue
// considered by the cleanup pass.
type TerminalSessionOperatorQueueReconcileItem struct {
	WorkspaceID string `json:"workspace_id"`
	QueueID     string `json:"queue_id"`
	QueueKey    string `json:"queue_key,omitempty"`
	SessionID   string `json:"session_id"`
	AgentID     string `json:"agent_id,omitempty"`
	Action      string `json:"action"`
	Error       string `json:"error,omitempty"`
}

// TerminalSessionOperatorQueueReconcileResult summarizes one cleanup pass for
// session-sourced queues whose source session is already terminal.
type TerminalSessionOperatorQueueReconcileResult struct {
	Scope       string                                      `json:"scope"`
	ReferenceAt string                                      `json:"reference_at"`
	Scanned     int                                         `json:"scanned"`
	Resolved    int                                         `json:"resolved"`
	Skipped     int                                         `json:"skipped"`
	Problems    int                                         `json:"problems"`
	Items       []TerminalSessionOperatorQueueReconcileItem `json:"items,omitempty"`
}

// ReconcileMissingSessionOperatorQueues is the stateless auto-repair sweep for the
// session -> operator-queue projection (CA-05 / AUD-038): it self-heals queues that
// are missing because a projection step failed, the process crashed between the
// session commit and the queue write, or the swallowed-error path left a gap.
//
// It is robust to all of those causes because it derives the work-list purely from
// canonical state (MissingSessionOperatorQueues), not from any durable outbox.
//
// Crucially, it synthesizes the driving session event from the canonical status
// (sessionEventForBlockingStatus) before replaying SyncOperatorQueueFromSessionState,
// because the queue is opened from the event type, not the status: a session that
// went BLOCKED and then emitted a keepalive reconstructs with UpdateType=session.status,
// which would open nothing. The CA-06 terminal-session guard inside the sync still
// prevents resurrecting a queue for a session that has since ended.
//
// limit bounds the per-pass work (<=0 = no limit). Per-session repair failures are
// counted and logged by the caller; the sweep continues past them and returns the
// first error encountered (so the serve loop can mark itself degraded) without
// aborting the rest of the pass.
func (s *Store) ReconcileMissingSessionOperatorQueues(ctx context.Context, limit int) (OperatorQueueProjectionReconcileResult, error) {
	result := OperatorQueueProjectionReconcileResult{}
	if s == nil {
		return result, fmt.Errorf("sqlite store unavailable")
	}
	missing, err := s.MissingSessionOperatorQueues(ctx, limit)
	if err != nil {
		return result, err
	}
	result.Missing = len(missing)

	var firstErr error
	for _, row := range missing {
		eventType := sessionEventForBlockingStatus(row.Status)
		if eventType == "" {
			// Status no longer maps to a blocking event (raced terminal); skip.
			result.Unchanged++
			continue
		}
		// Load the fully-hydrated canonical session state (recovers HandoffTo,
		// DecisionNeededFrom, Summary, BlockedOn etc. from the latest coordination
		// update) so the rebuilt queue keeps its assignee/details, then override only
		// UpdateType with the status-synthesized blocking event. The override is the
		// load-bearing fix: the queue is opened from the event type, and the latest
		// update is often a keepalive (session.status) that would open nothing.
		state, stateErr := s.GetAgentSessionState(ctx, row.WorkspaceID, row.SessionID)
		if stateErr != nil {
			// Fall back to the row's minimal identity if the full state can't load
			// (e.g. the session row vanished mid-sweep); the missing-queue durability
			// goal still holds, only assignee fidelity is reduced.
			state = AgentSessionStateRecord{
				SessionID:   row.SessionID,
				WorkspaceID: row.WorkspaceID,
				AgentID:     row.AgentID,
				TaskID:      row.TaskID,
				Status:      row.Status,
			}
		}
		// Re-confirm the canonical status is still the blocking one we detected; if it
		// moved (terminal/active) since detection, let the sync's own guards handle it.
		if state.Status == "" {
			state.Status = row.Status
		}
		state.UpdateType = sessionEventForBlockingStatus(state.Status)
		if state.UpdateType == "" {
			result.Unchanged++
			continue
		}
		syncResult, syncErr := s.SyncOperatorQueueFromSessionState(ctx, state)
		if syncErr != nil {
			result.Failed++
			if firstErr == nil {
				firstErr = fmt.Errorf("reconcile session %s operator queue: %w", row.SessionID, syncErr)
			}
			continue
		}
		if len(syncResult.Opened) > 0 {
			result.Repaired++
		} else {
			// The sync ran but opened nothing -- e.g. the CA-06 guard declined because
			// the session went terminal between detection and repair. Not an error.
			result.Unchanged++
		}
	}
	return result, firstErr
}

// ReconcileTerminalSessionOperatorQueues resolves non-canonical operator queues
// that are still owned by a session after that session has ended. The session-event
// projection already resolves canonical keys such as session:<id>:blocker; this
// pass covers external gates and other session-sourced queues created outside that
// canonical keyspace, including rows left behind before the event-sync fix landed.
func (s *Store) ReconcileTerminalSessionOperatorQueues(ctx context.Context, input TerminalSessionOperatorQueueReconcileInput) (TerminalSessionOperatorQueueReconcileResult, error) {
	result := TerminalSessionOperatorQueueReconcileResult{}
	if s == nil {
		return result, fmt.Errorf("sqlite store unavailable")
	}
	scope := normalizeWorkspaceAuthorityScope(input.Scope)
	_, actorID, err := normalizeLocalAuthorityActor(input.ActorType, input.ActorID)
	if err != nil {
		return result, err
	}
	if strings.TrimSpace(actorID) == "authority_spine" {
		actorID = "terminal_session_queue_reconciler"
	}
	referenceAt := strings.TrimSpace(input.ReferenceAt)
	if referenceAt == "" {
		referenceAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	result.Scope = scope
	result.ReferenceAt = referenceAt

	queues, err := s.listOpenTerminalSessionOperatorQueues(ctx, input.Limit)
	if err != nil {
		return result, err
	}
	result.Scanned = len(queues)
	result.Items = make([]TerminalSessionOperatorQueueReconcileItem, 0, len(queues))

	var firstErr error
	for _, queue := range queues {
		item := TerminalSessionOperatorQueueReconcileItem{
			WorkspaceID: queue.WorkspaceID,
			QueueID:     queue.QueueID,
			QueueKey:    queue.QueueKey,
			SessionID:   queue.SessionID,
			AgentID:     queue.AgentID,
			Action:      "UNCHANGED",
		}
		_, _, resolveErr := s.ResolveOperatorQueueItemWithEvent(ctx, OperatorQueueResolveInput{
			WorkspaceID:             queue.WorkspaceID,
			QueueID:                 queue.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              actorID,
			Resolution:              "cleared_by_terminal_session_reconcile",
			RequireCurrentStatus:    "OPEN",
			RequireCurrentRevision:  queue.Revision,
			RequireCurrentUpdatedAt: queue.UpdatedAt,
		})
		if resolveErr != nil {
			if terminalSessionQueueResolveRace(resolveErr) {
				item.Action = "SKIPPED_RACE"
				result.Skipped++
				result.Items = append(result.Items, item)
				continue
			}
			item.Action = "PROBLEM"
			item.Error = resolveErr.Error()
			result.Problems++
			result.Items = append(result.Items, item)
			if firstErr == nil {
				firstErr = fmt.Errorf("resolve terminal session operator queue %s: %w", queue.QueueID, resolveErr)
			}
			continue
		}
		item.Action = "RESOLVED"
		result.Resolved++
		result.Items = append(result.Items, item)
	}
	return result, firstErr
}

func terminalSessionQueueResolveRace(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrOperatorQueueItemNotFound) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "operator queue item is not open") ||
		strings.Contains(msg, "operator queue item was updated concurrently")
}

func (s *Store) listOpenTerminalSessionOperatorQueues(ctx context.Context, limit int) ([]OperatorQueueRecord, error) {
	query := `SELECT ` + operatorQueueSelectColumns("q") + `
  FROM operator_queue_items q
  JOIN agent_sessions sessions
    ON sessions.workspace_id = q.workspace_id
   AND sessions.session_id = q.session_id
 WHERE q.status = 'OPEN'
   AND COALESCE(q.session_id,'') != ''
   AND COALESCE(q.source_id,'') = COALESCE(q.session_id,'')
   AND LOWER(COALESCE(q.source_kind,'')) IN ('session', 'session_event')
   AND (COALESCE(sessions.completed_at,'') != '' OR sessions.status = ?)
 ORDER BY q.updated_at ASC, q.queue_id ASC`
	args := []any{model.SessionStatusEnded}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list open terminal-session operator queues: %w", err)
	}
	defer rows.Close()
	return collectOperatorQueueRows(rows)
}
