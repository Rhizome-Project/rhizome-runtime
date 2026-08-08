package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

type OperatorQueueLagSnapshot struct {
	State                              string `json:"state,omitempty"`
	Message                            string `json:"message,omitempty"`
	ReferenceAt                        string `json:"reference_at,omitempty"`
	OpenQueueCount                     int    `json:"open_queue_count"`
	OverdueQueueCount                  int    `json:"overdue_queue_count"`
	OverdueClaimReviewUnescalatedCount int    `json:"overdue_claim_review_unescalated_count"`
	MissingOperatorQueueCount          int    `json:"missing_operator_queue_count"`
	StaleOpenQueueCount                int    `json:"stale_open_queue_count"`
	MalformedDueAtCount                int    `json:"malformed_due_at_count"`
	OldestOverdueDueAt                 string `json:"oldest_overdue_due_at,omitempty"`
	OldestStaleOpenUpdatedAt           string `json:"oldest_stale_open_updated_at,omitempty"`
}

func (s *Store) CurrentOperatorQueueLagSnapshot(ctx context.Context) OperatorQueueLagSnapshot {
	referenceAt := time.Now().UTC()
	snapshot := OperatorQueueLagSnapshot{
		State:       "ok",
		Message:     "no open operator queues are overdue",
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
	}
	if s == nil {
		snapshot.State = "unsupported"
		snapshot.Message = "sqlite store unavailable"
		return snapshot
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT q.queue_type, q.source_kind, q.escalation_count, q.due_at, q.updated_at, q.session_id, COALESCE(s.status,'')
  FROM operator_queue_items q
  LEFT JOIN agent_sessions s ON s.workspace_id = q.workspace_id AND s.session_id = q.session_id
 WHERE q.status = ?`,
		"OPEN",
	)
	if err != nil {
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("query open operator queues: %v", err)
		return snapshot
	}
	defer rows.Close()

	var oldestOverdue time.Time
	var oldestStaleOpen time.Time
	openQueuesBySessionType := make(map[string]map[string]struct{})
	for rows.Next() {
		var queueType string
		var sourceKind sql.NullString
		var escalationCount int
		var dueAt sql.NullString
		var updatedAt sql.NullString
		var sessionID sql.NullString
		var sessionStatus string
		if err := rows.Scan(&queueType, &sourceKind, &escalationCount, &dueAt, &updatedAt, &sessionID, &sessionStatus); err != nil {
			snapshot.State = "error"
			snapshot.Message = fmt.Sprintf("scan open operator queue: %v", err)
			return snapshot
		}
		snapshot.OpenQueueCount++
		normalizedSessionStatus := strings.TrimSpace(sessionStatus)
		if sessionID.Valid && strings.TrimSpace(sessionID.String) != "" {
			sessionKey := strings.TrimSpace(sessionID.String)
			if openQueuesBySessionType[sessionKey] == nil {
				openQueuesBySessionType[sessionKey] = make(map[string]struct{})
			}
			openQueuesBySessionType[sessionKey][normalizeOperatorQueueType(queueType)] = struct{}{}
		}
		if sessionID.Valid && strings.TrimSpace(sessionID.String) != "" && normalizedSessionStatus != "" && !model.IsSessionStatusActive(normalizedSessionStatus) {
			snapshot.StaleOpenQueueCount++
			if updatedAt.Valid {
				parsedUpdatedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(updatedAt.String))
				if err == nil && (oldestStaleOpen.IsZero() || parsedUpdatedAt.Before(oldestStaleOpen)) {
					oldestStaleOpen = parsedUpdatedAt
				}
			}
		}
		if !dueAt.Valid || strings.TrimSpace(dueAt.String) == "" {
			continue
		}
		parsedDueAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(dueAt.String))
		if err != nil {
			snapshot.MalformedDueAtCount++
			continue
		}
		if !parsedDueAt.Before(referenceAt) {
			continue
		}
		snapshot.OverdueQueueCount++
		if oldestOverdue.IsZero() || parsedDueAt.Before(oldestOverdue) {
			oldestOverdue = parsedDueAt
		}
		if strings.EqualFold(strings.TrimSpace(sourceKind.String), "knowledge_claim") &&
			normalizeOperatorQueueType(queueType) == "FOLLOW_UP" &&
			escalationCount == 0 {
			snapshot.OverdueClaimReviewUnescalatedCount++
		}
	}
	if err := rows.Err(); err != nil {
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("iterate open operator queues: %v", err)
		return snapshot
	}

	sessionRows, err := s.db.QueryContext(ctx, `
SELECT session_id, status
  FROM agent_sessions
 WHERE status IN (?, ?, ?)`,
		model.SessionStatusBlocked,
		model.SessionStatusWaitingDecision,
		model.SessionStatusHandoffPending,
	)
	if err != nil {
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("query operator-queue-required sessions: %v", err)
		return snapshot
	}
	defer sessionRows.Close()

	for sessionRows.Next() {
		var sessionID string
		var status string
		if err := sessionRows.Scan(&sessionID, &status); err != nil {
			snapshot.State = "error"
			snapshot.Message = fmt.Sprintf("scan operator-queue-required session: %v", err)
			return snapshot
		}
		requiredQueueType := ""
		switch strings.ToUpper(strings.TrimSpace(status)) {
		case model.SessionStatusBlocked:
			requiredQueueType = "BLOCKER"
		case model.SessionStatusWaitingDecision:
			requiredQueueType = "DECISION"
		case model.SessionStatusHandoffPending:
			requiredQueueType = "HANDOFF"
		}
		if requiredQueueType == "" {
			continue
		}
		if _, ok := openQueuesBySessionType[strings.TrimSpace(sessionID)][requiredQueueType]; !ok {
			snapshot.MissingOperatorQueueCount++
		}
	}
	if err := sessionRows.Err(); err != nil {
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("iterate operator-queue-required sessions: %v", err)
		return snapshot
	}

	if !oldestOverdue.IsZero() {
		snapshot.OldestOverdueDueAt = oldestOverdue.UTC().Format(time.RFC3339Nano)
	}
	if !oldestStaleOpen.IsZero() {
		snapshot.OldestStaleOpenUpdatedAt = oldestStaleOpen.UTC().Format(time.RFC3339Nano)
	}

	switch {
	case snapshot.MalformedDueAtCount > 0 || snapshot.OverdueQueueCount > 0 || snapshot.StaleOpenQueueCount > 0 || snapshot.MissingOperatorQueueCount > 0:
		snapshot.State = "degraded"
		parts := make([]string, 0, 4)
		if snapshot.OverdueQueueCount > 0 {
			parts = append(parts, fmt.Sprintf("overdue=%d", snapshot.OverdueQueueCount))
		}
		if snapshot.OverdueClaimReviewUnescalatedCount > 0 {
			parts = append(parts, fmt.Sprintf("claim_review_unescalated=%d", snapshot.OverdueClaimReviewUnescalatedCount))
		}
		if snapshot.MissingOperatorQueueCount > 0 {
			parts = append(parts, fmt.Sprintf("missing_operator_queue=%d", snapshot.MissingOperatorQueueCount))
		}
		if snapshot.StaleOpenQueueCount > 0 {
			parts = append(parts, fmt.Sprintf("stale_open=%d", snapshot.StaleOpenQueueCount))
		}
		if snapshot.MalformedDueAtCount > 0 {
			parts = append(parts, fmt.Sprintf("malformed_due_at=%d", snapshot.MalformedDueAtCount))
		}
		if snapshot.OldestOverdueDueAt != "" {
			parts = append(parts, "oldest_due_at="+snapshot.OldestOverdueDueAt)
		}
		if snapshot.OldestStaleOpenUpdatedAt != "" {
			parts = append(parts, "oldest_stale_open_updated_at="+snapshot.OldestStaleOpenUpdatedAt)
		}
		snapshot.Message = "operator queue lag detected: " + strings.Join(parts, ", ")
	default:
		snapshot.Message = "no open operator queues are overdue"
	}

	return snapshot
}
