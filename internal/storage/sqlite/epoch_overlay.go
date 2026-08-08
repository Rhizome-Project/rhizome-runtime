package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type ControlEpochRecord struct {
	WorkspaceID       string `json:"workspace_id"`
	CurrentEpoch      int    `json:"current_epoch"`
	PolicyMode        string `json:"policy_mode"`
	LastIncrementedAt string `json:"last_incremented_at"`
}

type WorkspaceTimeAuthority struct {
	WorkspaceID          string                   `json:"workspace_id"`
	CurrentEpoch         int                      `json:"current_epoch"`
	PolicyMode           string                   `json:"policy_mode"`
	EpochAnchorAt        string                   `json:"epoch_anchor_at,omitempty"`
	RuntimeEventAnchorAt string                   `json:"runtime_event_anchor_at,omitempty"`
	ReferenceAt          string                   `json:"reference_at"`
	TemporalContract     *TemporalHorizonContract `json:"temporal_contract,omitempty"`
}

func (s *Store) GetControlEpoch(ctx context.Context, workspaceID string) (ControlEpochRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return ControlEpochRecord{}, errors.New("workspace_id required")
	}

	var rec ControlEpochRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT workspace_id, current_epoch, policy_mode, last_incremented_at
		 FROM workspace_control_epochs WHERE workspace_id = ?`,
		workspaceID).Scan(&rec.WorkspaceID, &rec.CurrentEpoch, &rec.PolicyMode, &rec.LastIncrementedAt)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Virtual epoch 0 if not initialized
			return ControlEpochRecord{
				WorkspaceID:       workspaceID,
				CurrentEpoch:      0,
				PolicyMode:        "shadow",
				LastIncrementedAt: time.Now().UTC().Format(time.RFC3339Nano),
			}, nil
		}
		return ControlEpochRecord{}, fmt.Errorf("query control epoch: %w", err)
	}

	return rec, nil
}

func (s *Store) persistedControlEpochAnchor(ctx context.Context, workspaceID string) (string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return "", errors.New("workspace_id required")
	}

	var lastIncrementedAt string
	err := s.db.QueryRowContext(ctx,
		`SELECT last_incremented_at
		 FROM workspace_control_epochs
		 WHERE workspace_id = ?`,
		workspaceID).Scan(&lastIncrementedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("query control epoch anchor: %w", err)
	}
	return strings.TrimSpace(lastIncrementedAt), nil
}

func (s *Store) latestWorkspaceRuntimeEventAt(ctx context.Context, workspaceID string) (string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return "", errors.New("workspace_id required")
	}

	var createdAt string
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(created_at), '')
		 FROM runtime_events
		 WHERE workspace_id = ?`,
		workspaceID).Scan(&createdAt)
	if err != nil {
		return "", fmt.Errorf("query workspace runtime anchor: %w", err)
	}
	return strings.TrimSpace(createdAt), nil
}

func (s *Store) workspaceReferenceTimestamp(ctx context.Context, workspaceID, fallback string) (string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return "", errors.New("workspace_id required")
	}

	referenceAt := strings.TrimSpace(fallback)
	epochAnchorAt, err := s.persistedControlEpochAnchor(ctx, workspaceID)
	if err != nil {
		return "", err
	}
	runtimeAnchorAt, err := s.latestWorkspaceRuntimeEventAt(ctx, workspaceID)
	if err != nil {
		return "", err
	}
	referenceAt = controlLaterTimestamp(referenceAt, epochAnchorAt)
	referenceAt = controlLaterTimestamp(referenceAt, runtimeAnchorAt)
	return referenceAt, nil
}

func (s *Store) GetWorkspaceTimeAuthority(ctx context.Context, workspaceID string) (WorkspaceTimeAuthority, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return WorkspaceTimeAuthority{}, errors.New("workspace_id required")
	}

	epoch, err := s.GetControlEpoch(ctx, workspaceID)
	if err != nil {
		return WorkspaceTimeAuthority{}, err
	}
	epochAnchorAt, err := s.persistedControlEpochAnchor(ctx, workspaceID)
	if err != nil {
		return WorkspaceTimeAuthority{}, err
	}
	runtimeAnchorAt, err := s.latestWorkspaceRuntimeEventAt(ctx, workspaceID)
	if err != nil {
		return WorkspaceTimeAuthority{}, err
	}
	referenceAt, err := s.workspaceReferenceTimestamp(ctx, workspaceID, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return WorkspaceTimeAuthority{}, err
	}

	authority := WorkspaceTimeAuthority{
		WorkspaceID:          workspaceID,
		CurrentEpoch:         epoch.CurrentEpoch,
		PolicyMode:           epoch.PolicyMode,
		EpochAnchorAt:        strings.TrimSpace(epochAnchorAt),
		RuntimeEventAnchorAt: strings.TrimSpace(runtimeAnchorAt),
		ReferenceAt:          strings.TrimSpace(referenceAt),
	}
	authority.TemporalContract = workspaceTimeAuthorityTemporalContract(authority)
	return authority, nil
}

// IncrementEpoch forcibly advances the global control epoch for a workspace by 1.
// Usually called by a background heartbeat/watchdog.
func (s *Store) IncrementEpoch(ctx context.Context, workspaceID string) (int, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return 0, errors.New("workspace_id required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	var rowEpoch int

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin epoch increment tx: %w", err)
	}

	err = tx.QueryRowContext(ctx,
		`INSERT INTO workspace_control_epochs (workspace_id, current_epoch, policy_mode, last_incremented_at)
		 VALUES (?, 1, 'shadow', ?)
		 ON CONFLICT(workspace_id) DO UPDATE SET
		 current_epoch = current_epoch + 1,
		 last_incremented_at = excluded.last_incremented_at
		 RETURNING current_epoch`,
		workspaceID, now).Scan(&rowEpoch)

	if err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("increment epoch query: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit epoch tx: %w", err)
	}

	return rowEpoch, nil
}

// SetPolicyMode transitions the engine between "shadow" (log only) and "active" (closed loop actuation).
func (s *Store) SetPolicyMode(ctx context.Context, workspaceID, mode string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode != "shadow" && mode != "active" {
		return errors.New("mode must be 'shadow' or 'active'")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO workspace_control_epochs (workspace_id, current_epoch, policy_mode, last_incremented_at)
		 VALUES (?, 0, ?, ?)
		 ON CONFLICT(workspace_id) DO UPDATE SET policy_mode = excluded.policy_mode`,
		workspaceID, mode, now)
	return err
}
