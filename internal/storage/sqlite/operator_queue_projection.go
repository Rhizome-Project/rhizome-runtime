package sqlite

import (
	"context"
	"fmt"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// MissingSessionOperatorQueue is one session that is in a blocking phase
// (BLOCKED / WAITING_DECISION / HANDOFF_PENDING) but is missing the OPEN
// operator queue its status requires. It carries every field the projection
// reconciler needs to rebuild a usable AgentSessionStateRecord and replay the
// session -> operator-queue sync. See ReconcileMissingSessionOperatorQueues.
type MissingSessionOperatorQueue struct {
	WorkspaceID       string
	SessionID         string
	AgentID           string
	TaskID            string
	Status            string
	RequiredQueueType string
}

// MissingSessionOperatorQueues returns the sessions whose canonical status
// requires an operator queue (the same predicate the operator-queue health
// snapshot counts as MissingOperatorQueueCount) but which currently have no OPEN
// session-derived queue of the required type. This is the row-returning query the
// stateless projection reconciler sweeps; the health snapshot only computes a
// count, so this is a distinct, wider query (it returns workspace/agent/task and
// the required queue type, not just session_id/status).
//
// Detection here keys off agent_sessions.status; the repair (the reconciler)
// must therefore synthesize the driving session event from status, because the
// queue is opened from the event type, not the status. See
// sessionEventForBlockingStatus.
//
// limit bounds the per-pass work (<= 0 means no limit). The query is global
// across workspaces, matching the global health predicate.
func (s *Store) MissingSessionOperatorQueues(ctx context.Context, limit int) ([]MissingSessionOperatorQueue, error) {
	if s == nil {
		return nil, fmt.Errorf("sqlite store unavailable")
	}
	query := `
SELECT s.workspace_id, s.session_id, COALESCE(s.agent_id, ''), COALESCE(s.task_id, ''), s.status
  FROM agent_sessions s
 WHERE s.status IN (?, ?, ?)
   AND NOT EXISTS (
       SELECT 1
         FROM operator_queue_items q
        WHERE q.workspace_id = s.workspace_id
          AND q.session_id = s.session_id
          AND q.status = 'OPEN'
          AND q.source_kind = 'session_event'
          AND q.source_id = s.session_id
          AND q.queue_type = CASE s.status
                                 WHEN ? THEN 'BLOCKER'
                                 WHEN ? THEN 'DECISION'
                                 WHEN ? THEN 'HANDOFF'
                             END
   )
 ORDER BY s.started_at ASC`
	args := []any{
		model.SessionStatusBlocked,
		model.SessionStatusWaitingDecision,
		model.SessionStatusHandoffPending,
		model.SessionStatusBlocked,
		model.SessionStatusWaitingDecision,
		model.SessionStatusHandoffPending,
	}
	if limit > 0 {
		query += `
 LIMIT ?`
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query missing session operator queues: %w", err)
	}
	defer rows.Close()

	var out []MissingSessionOperatorQueue
	for rows.Next() {
		var item MissingSessionOperatorQueue
		if err := rows.Scan(&item.WorkspaceID, &item.SessionID, &item.AgentID, &item.TaskID, &item.Status); err != nil {
			return nil, fmt.Errorf("scan missing session operator queue: %w", err)
		}
		item.RequiredQueueType = operatorQueueTypeForSessionStatus(item.Status)
		if item.RequiredQueueType == "" {
			// Status no longer maps to a queue type (defensive; the WHERE clause
			// already bounds status to the three blocking phases).
			continue
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate missing session operator queues: %w", err)
	}
	return out, nil
}

// sessionEventForBlockingStatus maps a canonical blocking session status back to
// the session event type that drives operator-queue creation in
// syncOperatorQueueFromSessionStateWithAuthorityTx. The reconciler must set this
// on the reconstructed AgentSessionStateRecord.UpdateType, because the queue is
// opened from the event type, not the status: a session that went BLOCKED and
// then received a keepalive reconstructs with UpdateType=session.status, which
// opens nothing. HANDOFF_PENDING maps to session.status (the queue is then driven
// by HandoffTo / the HANDOFF_PENDING status in the sync's HANDOFF branch).
func sessionEventForBlockingStatus(status string) string {
	switch model.NormalizeStatus(status) {
	case model.SessionStatusBlocked:
		return model.SessionEventBlocked
	case model.SessionStatusWaitingDecision:
		return model.SessionEventDecisionNeeded
	case model.SessionStatusHandoffPending:
		return model.SessionEventStatus
	default:
		return ""
	}
}
