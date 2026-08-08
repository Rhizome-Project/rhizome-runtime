package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrStewardshipActive = errors.New("cluster already has an active steward")
	ErrStewardNotFound   = errors.New("cluster steward not found")
)

// ClusterSteward tracks the temporary lease of leadership over a segment cluster
// during a specific merge cycle/epoch, as mandated by RRP-1.2 Section 15.
type ClusterSteward struct {
	ClusterID      string    `json:"cluster_id"`
	EpochID        string    `json:"epoch_id"`
	StewardAgentID string    `json:"steward_agent_id"`
	GrantedAt      time.Time `json:"granted_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	Status         string    `json:"status"` // ACTIVE, EXPIRED, REVOKED
}

// ElectStewardInput contains the parameters to request temporary leadership.
type ElectStewardInput struct {
	ClusterID   string
	EpochID     string
	CandidateID string
	TTLSeconds  int
}

// ElectClusterSteward atomically grants stewardship to a candidate if no active
// steward currently holds the lease for the epoch.
func (s *Store) ElectClusterSteward(ctx context.Context, input ElectStewardInput) (ClusterSteward, error) {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ClusterSteward{}, err
	}
	defer tx.Rollback()

	steward, err := s.electClusterStewardTx(ctx, tx, input)
	if err != nil {
		return ClusterSteward{}, err
	}
	if err := tx.Commit(); err != nil {
		return ClusterSteward{}, err
	}
	return steward, nil
}

func normalizeElectStewardInput(input ElectStewardInput) (ElectStewardInput, error) {
	input.ClusterID = strings.TrimSpace(input.ClusterID)
	input.EpochID = strings.TrimSpace(input.EpochID)
	input.CandidateID = strings.TrimSpace(input.CandidateID)
	if input.ClusterID == "" || input.EpochID == "" || input.CandidateID == "" {
		return ElectStewardInput{}, errors.New("cluster_id, epoch_id, and candidate_id are required")
	}
	if input.TTLSeconds <= 0 {
		input.TTLSeconds = 300 // default 5 minute lease
	}
	return input, nil
}

func (s *Store) electClusterStewardTx(ctx context.Context, tx *sql.Tx, input ElectStewardInput) (ClusterSteward, error) {
	if tx == nil {
		return ClusterSteward{}, errors.New("tx is required")
	}
	var err error
	input, err = normalizeElectStewardInput(input)
	if err != nil {
		return ClusterSteward{}, err
	}

	referenceAt, err := s.clusterStewardReferenceTime(ctx, input.ClusterID, time.Now().UTC())
	if err != nil {
		return ClusterSteward{}, err
	}

	// 1. Invalidate any stewards whose TTL has organically expired
	_, err = tx.ExecContext(ctx, `
		UPDATE cluster_stewards
		SET status = 'EXPIRED'
		WHERE status = 'ACTIVE' AND expires_at <= ?
	`, referenceAt)
	if err != nil {
		return ClusterSteward{}, fmt.Errorf("cleanup expired stewards: %w", err)
	}

	// 2. Check for an actively held lease on this cluster
	var activeEpoch string
	var activeAgent string
	err = tx.QueryRowContext(ctx, `
		SELECT epoch_id, steward_agent_id
		FROM cluster_stewards
		WHERE cluster_id = ? AND status = 'ACTIVE' AND expires_at > ?
		ORDER BY granted_at DESC
		LIMIT 1
	`, input.ClusterID, referenceAt).Scan(&activeEpoch, &activeAgent)

	if err == nil {
		if activeAgent == input.CandidateID && activeEpoch == input.EpochID {
			var steward ClusterSteward
			err = tx.QueryRowContext(ctx, `
				SELECT cluster_id, epoch_id, steward_agent_id, granted_at, expires_at, status
				FROM cluster_stewards
				WHERE cluster_id = ? AND epoch_id = ?
			`, input.ClusterID, input.EpochID).Scan(&steward.ClusterID, &steward.EpochID, &steward.StewardAgentID, &steward.GrantedAt, &steward.ExpiresAt, &steward.Status)
			if err != nil {
				return ClusterSteward{}, fmt.Errorf("load active steward: %w", err)
			}
			return steward, nil
		}
		return ClusterSteward{}, ErrStewardshipActive
	} else if !errors.Is(err, sql.ErrNoRows) {
		return ClusterSteward{}, fmt.Errorf("check active steward: %w", err)
	}

	// 2.5 Centralization Penalty Check (Max 3 concurrent clusters)
	var concurrentCount int
	err = tx.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM cluster_stewards
		WHERE steward_agent_id = ? AND status = 'ACTIVE' AND expires_at > ?
	`, input.CandidateID, referenceAt).Scan(&concurrentCount)
	if err != nil {
		return ClusterSteward{}, fmt.Errorf("check centralization penalty: %w", err)
	}
	if concurrentCount >= 3 {
		return ClusterSteward{}, errors.New("centralization penalty: candidate exceeds maximum concurrent stewardships (3)")
	}

	// 3. Grant the lease
	grantedAt := referenceAt
	expiresAt := grantedAt.Add(time.Duration(input.TTLSeconds) * time.Second)

	_, err = tx.ExecContext(ctx, `
		INSERT INTO cluster_stewards (cluster_id, epoch_id, steward_agent_id, granted_at, expires_at, status)
		VALUES (?, ?, ?, ?, ?, 'ACTIVE')
		ON CONFLICT(cluster_id, epoch_id) DO UPDATE SET
			steward_agent_id = excluded.steward_agent_id,
			granted_at = excluded.granted_at,
			expires_at = excluded.expires_at,
			status = 'ACTIVE'
	`, input.ClusterID, input.EpochID, input.CandidateID, grantedAt, expiresAt)
	if err != nil {
		return ClusterSteward{}, fmt.Errorf("insert cluster steward: %w", err)
	}

	return ClusterSteward{
		ClusterID:      input.ClusterID,
		EpochID:        input.EpochID,
		StewardAgentID: input.CandidateID,
		GrantedAt:      grantedAt,
		ExpiresAt:      expiresAt,
		Status:         "ACTIVE",
	}, nil
}

// GetActiveSteward returns the current active steward for a cluster, if any.
func (s *Store) GetActiveSteward(ctx context.Context, clusterID string) (ClusterSteward, error) {
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return ClusterSteward{}, errors.New("cluster_id is required")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ClusterSteward{}, err
	}
	defer tx.Rollback()

	st, err := s.getActiveStewardTx(ctx, tx, clusterID)
	if errors.Is(err, ErrStewardNotFound) {
		if err := tx.Commit(); err != nil {
			return ClusterSteward{}, err
		}
		return ClusterSteward{}, ErrStewardNotFound
	}
	if err != nil {
		return ClusterSteward{}, err
	}
	if err := tx.Commit(); err != nil {
		return ClusterSteward{}, err
	}
	return st, nil
}

func (s *Store) getActiveStewardTx(ctx context.Context, tx *sql.Tx, clusterID string) (ClusterSteward, error) {
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return ClusterSteward{}, errors.New("cluster_id is required")
	}
	if tx == nil {
		return ClusterSteward{}, errors.New("tx is required")
	}

	referenceAt, err := s.clusterStewardReferenceTime(ctx, clusterID, time.Now().UTC())
	if err != nil {
		return ClusterSteward{}, err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE cluster_stewards
		SET status = 'EXPIRED'
		WHERE cluster_id = ? AND status = 'ACTIVE' AND expires_at <= ?
	`, clusterID, referenceAt); err != nil {
		return ClusterSteward{}, fmt.Errorf("cleanup expired stewards: %w", err)
	}

	var st ClusterSteward
	err = tx.QueryRowContext(ctx, `
		SELECT cluster_id, epoch_id, steward_agent_id, granted_at, expires_at, status
		FROM cluster_stewards
		WHERE cluster_id = ? AND status = 'ACTIVE' AND expires_at > ?
		ORDER BY granted_at DESC
		LIMIT 1
	`, clusterID, referenceAt).Scan(&st.ClusterID, &st.EpochID, &st.StewardAgentID, &st.GrantedAt, &st.ExpiresAt, &st.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return ClusterSteward{}, ErrStewardNotFound
	}
	if err != nil {
		return ClusterSteward{}, err
	}
	return st, nil
}

// RevokeStewardship proactively ends a steward's lease, typically called when the merge cycle completes.
func (s *Store) RevokeStewardship(ctx context.Context, clusterID string, epochID string) error {
	res, err := s.writeDB.ExecContext(ctx, `
		UPDATE cluster_stewards
		SET status = 'REVOKED'
		WHERE cluster_id = ? AND epoch_id = ? AND status = 'ACTIVE'
	`, clusterID, epochID)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return ErrStewardNotFound
	}
	return nil
}

func (s *Store) clusterStewardReferenceTime(ctx context.Context, clusterID string, fallback time.Time) (time.Time, error) {
	fallback = fallback.UTC()
	workspaceID := stewardWorkspaceIDFromClusterID(clusterID)
	if workspaceID == "" {
		return fallback, nil
	}

	referenceAt, err := s.workspaceReferenceTimestamp(ctx, workspaceID, fallback.Format(time.RFC3339Nano))
	if err != nil {
		return time.Time{}, err
	}
	referenceAt = strings.TrimSpace(referenceAt)
	if referenceAt == "" {
		return fallback, nil
	}

	parsed, err := time.Parse(time.RFC3339Nano, referenceAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse cluster steward reference time: %w", err)
	}
	return parsed.UTC(), nil
}

func stewardWorkspaceIDFromClusterID(clusterID string) string {
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return ""
	}
	parts := strings.SplitN(clusterID, ":", 2)
	if len(parts) != 2 {
		return ""
	}
	workspacePart, _, ok := strings.Cut(parts[1], "/")
	if !ok {
		return ""
	}
	return strings.TrimSpace(workspacePart)
}
