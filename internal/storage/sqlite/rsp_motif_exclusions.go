package sqlite

import (
	"context"
	"strings"
	"time"
)

// ExcludeAgentFromTension forcibly prevents an agent from working on a specific tension
// until the expiration time. Used by the Motif Thrash Detector.
func (s *Store) ExcludeAgentFromTension(ctx context.Context, workspaceID, tensionID, agentID, reason string, ttl time.Duration) error {
	_, _, err := s.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		WorkspaceID: workspaceID,
		CommandType: ControlCommandExcludeAgentTension,
		TensionID:   tensionID,
		AgentID:     agentID,
		TTLSeconds:  int(ttl / time.Second),
		Reason:      reason,
		RequestedBy: defaultSystemControlCommandActorID,
		ActorType:   "system",
	})
	return err
}

// GetAgentTensionExclusions returns a list of tension IDs that the agent is excluded from.
func (s *Store) GetAgentTensionExclusions(ctx context.Context, workspaceID, agentID string) ([]string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	referenceAt, err := s.workspaceReferenceTimestamp(ctx, workspaceID, now)
	if err != nil {
		return nil, err
	}
	referenceAt = strings.TrimSpace(referenceAt)
	if referenceAt == "" {
		referenceAt = now
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT tension_id FROM workspace_tension_exclusions
		WHERE workspace_id = ? AND agent_id = ? AND expires_at > ?
	`, workspaceID, agentID, referenceAt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var excluded []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		excluded = append(excluded, id)
	}
	return excluded, nil
}
