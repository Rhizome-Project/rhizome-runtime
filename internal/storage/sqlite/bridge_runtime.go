package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func (s *Store) GetRuntimeState(ctx context.Context, bridgeID, workspaceID, stateKey string) (string, error) {
	key, err := composeRuntimeStateKey(bridgeID, workspaceID, stateKey)
	if err != nil {
		return "", err
	}
	return s.GetBridgeState(ctx, key)
}

func (s *Store) SetRuntimeState(ctx context.Context, bridgeID, workspaceID, stateKey, stateValue string) error {
	key, err := composeRuntimeStateKey(bridgeID, workspaceID, stateKey)
	if err != nil {
		return err
	}
	return s.SetBridgeState(ctx, key, stateValue)
}

func (s *Store) ListAgentUpdatesAfter(ctx context.Context, workspaceID, afterCreatedAt, afterUpdateID string, limit int) ([]AgentUpdateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if limit <= 0 {
		limit = 20
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT u.update_id, u.agent_id, a.display_name, u.update_type, u.summary, u.payload_json, u.requires_human, u.created_at
		 FROM agent_updates u
		 JOIN agents a ON a.agent_id = u.agent_id AND a.workspace_id = u.workspace_id
		 WHERE u.workspace_id = ?
		   AND (
		     ? = ''
		     OR u.created_at > ?
		     OR (u.created_at = ? AND u.update_id > ?)
		   )
		 ORDER BY u.created_at ASC, u.update_id ASC
		 LIMIT ?`,
		workspaceID,
		strings.TrimSpace(afterCreatedAt),
		strings.TrimSpace(afterCreatedAt),
		strings.TrimSpace(afterCreatedAt),
		strings.TrimSpace(afterUpdateID),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query agent updates after cursor: %w", err)
	}
	defer rows.Close()

	out := []AgentUpdateRecord{}
	for rows.Next() {
		var row AgentUpdateRecord
		var requiresHuman int
		if err := rows.Scan(
			&row.UpdateID,
			&row.AgentID,
			&row.AgentName,
			&row.UpdateType,
			&row.Summary,
			&row.PayloadJSON,
			&requiresHuman,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan agent update after cursor: %w", err)
		}
		row.RequiresHuman = requiresHuman != 0
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate agent updates after cursor: %w", err)
	}
	return out, nil
}

func (s *Store) RuntimeStateExists(ctx context.Context, bridgeID, workspaceID, stateKey string) (bool, error) {
	key, err := composeRuntimeStateKey(bridgeID, workspaceID, stateKey)
	if err != nil {
		return false, err
	}

	var count int
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT COUNT(1) FROM tg_bridge_state WHERE state_key = ?`,
		key,
	).Scan(&count); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("query runtime state existence: %w", err)
	}
	return count > 0, nil
}

func composeRuntimeStateKey(bridgeID, workspaceID, stateKey string) (string, error) {
	bridgeID = strings.TrimSpace(bridgeID)
	workspaceID = strings.TrimSpace(workspaceID)
	stateKey = strings.TrimSpace(stateKey)
	if bridgeID == "" {
		return "", errors.New("bridge_id is required")
	}
	if stateKey == "" {
		return "", errors.New("state_key is required")
	}
	if workspaceID == "" {
		workspaceID = "default"
	}
	return strings.Join([]string{"bridge", bridgeID, workspaceID, stateKey}, ":"), nil
}
