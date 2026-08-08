package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

type StuckAgentSnapshot struct {
	State                     string                      `json:"state,omitempty"`
	Message                   string                      `json:"message,omitempty"`
	ReferenceAt               string                      `json:"reference_at,omitempty"`
	RecoveryManifest          *StuckAgentRecoveryManifest `json:"recovery_manifest,omitempty"`
	ActiveSessionCount        int                         `json:"active_session_count"`
	DeadHeartbeatSessionCount int                         `json:"dead_heartbeat_session_count"`
	StaleActivitySessionCount int                         `json:"stale_activity_session_count"`
	OfflineAgentSessionCount  int                         `json:"offline_agent_session_count"`
	MissingAgentSessionCount  int                         `json:"missing_agent_session_count"`
	StartupGraceSessionCount  int                         `json:"startup_grace_session_count"`
	OldestAffectedStartedAt   string                      `json:"oldest_affected_started_at,omitempty"`
	OldestOfflineAgentSeenAt  string                      `json:"oldest_offline_agent_seen_at,omitempty"`
	OldestStaleActivityAt     string                      `json:"oldest_stale_activity_at,omitempty"`
}

const stuckAgentStartupGrace = 2 * time.Minute
const stuckAgentHeartbeatStaleAfter = 5 * time.Minute
const stuckAgentActivityStaleAfter = 5 * time.Minute

func (s *Store) CurrentStuckAgentSnapshot(ctx context.Context) StuckAgentSnapshot {
	referenceAt := time.Now().UTC()
	snapshot := StuckAgentSnapshot{
		State:       "ok",
		Message:     "no active sessions are owned by offline or missing agents",
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
	}
	if s == nil {
		snapshot.State = "unsupported"
		snapshot.Message = "sqlite store unavailable"
		return snapshot
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT sessions.workspace_id,
       sessions.started_at,
       sessions.session_id,
       sessions.task_id,
       agents.agent_id,
       agents.last_seen_at,
       COALESCE(latest_updates.latest_update_at, '')
  FROM agent_sessions sessions
  JOIN workspaces
    ON workspaces.workspace_id = sessions.workspace_id
  LEFT JOIN agents
    ON agents.workspace_id = sessions.workspace_id
   AND agents.agent_id = sessions.agent_id
  LEFT JOIN (
       SELECT workspace_id,
              json_extract(payload_json, '$.session_id') AS session_id,
              MAX(created_at) AS latest_update_at
         FROM agent_updates
        WHERE update_type IN (?, ?, ?, ?, ?, ?)
        GROUP BY workspace_id, json_extract(payload_json, '$.session_id')
  ) latest_updates
    ON latest_updates.workspace_id = sessions.workspace_id
   AND latest_updates.session_id = sessions.session_id
 WHERE workspaces.status = ?
   AND sessions.status IN (?, ?, ?, ?, ?)`,
		model.SessionEventStart,
		model.SessionEventStatus,
		model.SessionEventBlocked,
		model.SessionEventDecisionNeeded,
		model.SessionEventKeepalive,
		model.SessionEventEnd,
		model.WorkspaceStatusActive,
		model.SessionStatusActive,
		model.SessionStatusBlocked,
		model.SessionStatusWaitingDecision,
		model.SessionStatusHandoffPending,
		"RUNNING",
	)
	if err != nil {
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("query active sessions for stuck agent risk: %v", err)
		return snapshot
	}
	defer rows.Close()

	var oldestAffected time.Time
	var oldestOfflineSeen time.Time
	var oldestStaleActivity time.Time
	var recoveryCandidate *stuckAgentRecoveryCandidate
	for rows.Next() {
		var workspaceID sql.NullString
		var startedAt sql.NullString
		var sessionID sql.NullString
		var taskID sql.NullString
		var joinedAgentID sql.NullString
		var lastSeenAt sql.NullString
		var latestActivityAt sql.NullString
		if err := rows.Scan(&workspaceID, &startedAt, &sessionID, &taskID, &joinedAgentID, &lastSeenAt, &latestActivityAt); err != nil {
			snapshot.State = "error"
			snapshot.Message = fmt.Sprintf("scan active session for stuck agent risk: %v", err)
			return snapshot
		}
		snapshot.ActiveSessionCount++

		var startedAtTime time.Time
		recentStartup := false
		if startedAt.Valid {
			if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(startedAt.String)); err == nil {
				startedAtTime = parsed
				recentStartup = referenceAt.Sub(parsed) < stuckAgentStartupGrace
			}
		}
		if recentStartup {
			snapshot.StartupGraceSessionCount++
			continue
		}

		lastSeenStale := false
		missingAgent := !joinedAgentID.Valid || strings.TrimSpace(joinedAgentID.String) == ""
		if missingAgent {
			snapshot.MissingAgentSessionCount++
			lastSeenStale = true
		} else if !lastSeenAt.Valid || strings.TrimSpace(lastSeenAt.String) == "" {
			lastSeenStale = true
			snapshot.OfflineAgentSessionCount++
		} else {
			if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(lastSeenAt.String)); err == nil {
				if referenceAt.Sub(parsed) > stuckAgentHeartbeatStaleAfter {
					lastSeenStale = true
					snapshot.OfflineAgentSessionCount++
					if oldestOfflineSeen.IsZero() || parsed.Before(oldestOfflineSeen) {
						oldestOfflineSeen = parsed
					}
				}
			} else {
				lastSeenStale = true
				snapshot.OfflineAgentSessionCount++
			}
		}

		sessionActivityAt := startedAtTime
		if latestActivityAt.Valid && strings.TrimSpace(latestActivityAt.String) != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(latestActivityAt.String)); err == nil {
				sessionActivityAt = parsed
			}
		}
		staleActivity := false
		if !sessionActivityAt.IsZero() && referenceAt.Sub(sessionActivityAt) > stuckAgentActivityStaleAfter {
			staleActivity = true
			snapshot.StaleActivitySessionCount++
			if oldestStaleActivity.IsZero() || sessionActivityAt.Before(oldestStaleActivity) {
				oldestStaleActivity = sessionActivityAt
			}
		}

		if (lastSeenStale || staleActivity) && !startedAtTime.IsZero() {
			if oldestAffected.IsZero() || startedAtTime.Before(oldestAffected) {
				oldestAffected = startedAtTime
			}
		}

		if lastSeenStale || staleActivity {
			candidate := &stuckAgentRecoveryCandidate{
				WorkspaceID:      strings.TrimSpace(workspaceID.String),
				SessionID:        strings.TrimSpace(sessionID.String),
				AgentID:          strings.TrimSpace(joinedAgentID.String),
				TaskID:           strings.TrimSpace(taskID.String),
				StartedAt:        strings.TrimSpace(startedAt.String),
				LastSeenAt:       strings.TrimSpace(lastSeenAt.String),
				LatestActivityAt: strings.TrimSpace(latestActivityAt.String),
				MissingAgent:     missingAgent,
				OfflineAgent:     lastSeenStale && !missingAgent,
				StaleActivity:    staleActivity,
				StartupGrace:     recentStartup,
				AffectedAt:       startedAtTime,
			}
			if recoveryCandidate == nil || recoveryCandidate.moreUrgent(candidate) {
				recoveryCandidate = candidate
			}
		}
	}
	if err := rows.Err(); err != nil {
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("iterate active sessions for stuck agent risk: %v", err)
		return snapshot
	}

	if !oldestAffected.IsZero() {
		snapshot.OldestAffectedStartedAt = oldestAffected.UTC().Format(time.RFC3339Nano)
	}
	if !oldestOfflineSeen.IsZero() {
		snapshot.OldestOfflineAgentSeenAt = oldestOfflineSeen.UTC().Format(time.RFC3339Nano)
	}
	if !oldestStaleActivity.IsZero() {
		snapshot.OldestStaleActivityAt = oldestStaleActivity.UTC().Format(time.RFC3339Nano)
	}

	snapshot.DeadHeartbeatSessionCount = snapshot.OfflineAgentSessionCount + snapshot.MissingAgentSessionCount

	if snapshot.DeadHeartbeatSessionCount > 0 || snapshot.StaleActivitySessionCount > 0 {
		snapshot.State = "degraded"
		parts := []string{
			fmt.Sprintf("dead_heartbeats=%d", snapshot.DeadHeartbeatSessionCount),
			fmt.Sprintf("stale_activity=%d", snapshot.StaleActivitySessionCount),
		}
		if snapshot.OfflineAgentSessionCount > 0 {
			parts = append(parts, fmt.Sprintf("offline_agent_sessions=%d", snapshot.OfflineAgentSessionCount))
		}
		if snapshot.MissingAgentSessionCount > 0 {
			parts = append(parts, fmt.Sprintf("missing_agent_sessions=%d", snapshot.MissingAgentSessionCount))
		}
		if snapshot.OldestAffectedStartedAt != "" {
			parts = append(parts, "oldest_affected_started_at="+snapshot.OldestAffectedStartedAt)
		}
		if snapshot.OldestOfflineAgentSeenAt != "" {
			parts = append(parts, "oldest_offline_agent_seen_at="+snapshot.OldestOfflineAgentSeenAt)
		}
		if snapshot.OldestStaleActivityAt != "" {
			parts = append(parts, "oldest_stale_activity_at="+snapshot.OldestStaleActivityAt)
		}
		snapshot.Message = "stuck agent risk detected: " + strings.Join(parts, ", ")
		if recoveryCandidate != nil {
			snapshot.RecoveryManifest = s.ReadDurableStuckAgentRecoveryManifest(ctx, referenceAt, *recoveryCandidate)
		}
	} else if snapshot.StartupGraceSessionCount > 0 {
		snapshot.Message = fmt.Sprintf("startup grace shielding %d recent active session(s) before first heartbeat", snapshot.StartupGraceSessionCount)
	}

	return snapshot
}
