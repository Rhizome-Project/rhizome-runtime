package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	minAgentHeartbeatLeaseTTL = 5 * time.Second
	maxAgentHeartbeatLeaseTTL = 30 * time.Minute
)

// AgentHeartbeatLeaseInput describes an attempt to own one heartbeat run.
type AgentHeartbeatLeaseInput struct {
	WorkspaceID string
	AgentID     string
	HeartbeatID string
	OwnerID     string
	LeaseToken  string
	Locks       []string
	TTL         time.Duration
	Now         time.Time
}

// AgentHeartbeatLeaseResult reports whether the lease was acquired and, if not,
// which existing owner blocked it.
type AgentHeartbeatLeaseResult struct {
	Acquired            bool     `json:"acquired"`
	WorkspaceID         string   `json:"workspace_id,omitempty"`
	AgentID             string   `json:"agent_id,omitempty"`
	HeartbeatID         string   `json:"heartbeat_id,omitempty"`
	OwnerID             string   `json:"owner_id,omitempty"`
	LeaseToken          string   `json:"lease_token,omitempty"`
	Locks               []string `json:"locks,omitempty"`
	AcquiredAt          string   `json:"acquired_at,omitempty"`
	ExpiresAt           string   `json:"expires_at,omitempty"`
	ConflictReason      string   `json:"conflict_reason,omitempty"`
	ConflictHeartbeatID string   `json:"conflict_heartbeat_id,omitempty"`
	ConflictLock        string   `json:"conflict_lock,omitempty"`
	ConflictOwnerID     string   `json:"conflict_owner_id,omitempty"`
	ConflictLeaseToken  string   `json:"conflict_lease_token,omitempty"`
	ConflictExpiresAt   string   `json:"conflict_expires_at,omitempty"`
}

// AgentHeartbeatLeaseReleaseInput describes a best-effort release request.
type AgentHeartbeatLeaseReleaseInput struct {
	WorkspaceID string
	AgentID     string
	HeartbeatID string
	LeaseToken  string
}

func (s *Store) ensureAgentHeartbeatLeaseTables(ctx context.Context) error {
	const heartbeatLeases = `
CREATE TABLE IF NOT EXISTS agent_heartbeat_leases (
  workspace_id TEXT NOT NULL,
  agent_id     TEXT NOT NULL,
  heartbeat_id TEXT NOT NULL,
  owner_id     TEXT NOT NULL,
  lease_token  TEXT NOT NULL,
  locks_json   TEXT NOT NULL,
  acquired_at  TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  expires_at   TEXT NOT NULL,
  PRIMARY KEY (workspace_id, agent_id, heartbeat_id)
);`
	if _, err := s.writeDB.ExecContext(ctx, heartbeatLeases); err != nil {
		return fmt.Errorf("ensure agent heartbeat leases table: %w", err)
	}
	const lockLeases = `
CREATE TABLE IF NOT EXISTS agent_heartbeat_lock_leases (
  workspace_id TEXT NOT NULL,
  agent_id     TEXT NOT NULL,
  lock_name    TEXT NOT NULL,
  heartbeat_id TEXT NOT NULL,
  owner_id     TEXT NOT NULL,
  lease_token  TEXT NOT NULL,
  expires_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (workspace_id, agent_id, lock_name)
);`
	if _, err := s.writeDB.ExecContext(ctx, lockLeases); err != nil {
		return fmt.Errorf("ensure agent heartbeat lock leases table: %w", err)
	}
	const heartbeatLeaseExpiryIndex = `
CREATE INDEX IF NOT EXISTS idx_agent_heartbeat_leases_expiry
  ON agent_heartbeat_leases(workspace_id, agent_id, expires_at);`
	if _, err := s.writeDB.ExecContext(ctx, heartbeatLeaseExpiryIndex); err != nil {
		return fmt.Errorf("ensure agent heartbeat leases expiry index: %w", err)
	}
	const lockLeaseExpiryIndex = `
CREATE INDEX IF NOT EXISTS idx_agent_heartbeat_lock_leases_expiry
  ON agent_heartbeat_lock_leases(workspace_id, agent_id, expires_at);`
	if _, err := s.writeDB.ExecContext(ctx, lockLeaseExpiryIndex); err != nil {
		return fmt.Errorf("ensure agent heartbeat lock leases expiry index: %w", err)
	}
	return nil
}

// AcquireAgentHeartbeatLease atomically acquires or refreshes a heartbeat lease
// and its named locks. A different non-expired lease for the same heartbeat or
// any requested lock blocks the acquire.
func (s *Store) AcquireAgentHeartbeatLease(ctx context.Context, input AgentHeartbeatLeaseInput) (AgentHeartbeatLeaseResult, error) {
	if s == nil {
		return AgentHeartbeatLeaseResult{}, errors.New("store is nil")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	agentID := strings.TrimSpace(input.AgentID)
	heartbeatID := strings.TrimSpace(input.HeartbeatID)
	ownerID := strings.TrimSpace(input.OwnerID)
	leaseToken := strings.TrimSpace(input.LeaseToken)
	if workspaceID == "" || agentID == "" || heartbeatID == "" || ownerID == "" || leaseToken == "" {
		return AgentHeartbeatLeaseResult{}, errors.New("workspace_id, agent_id, heartbeat_id, owner_id, and lease_token are required")
	}
	locks := normalizeAgentHeartbeatLeaseLocks(input.Locks)
	ttl := clampAgentHeartbeatLeaseTTL(input.TTL)
	now := input.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	acquiredAt := now.Format(time.RFC3339Nano)
	expiresAt := now.Add(ttl).UTC().Format(time.RFC3339Nano)
	if err := s.ensureAgentHeartbeatLeaseTables(ctx); err != nil {
		return AgentHeartbeatLeaseResult{}, err
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return AgentHeartbeatLeaseResult{}, fmt.Errorf("begin agent heartbeat lease tx: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	if err := deleteExpiredAgentHeartbeatLeases(ctx, tx, workspaceID, agentID, acquiredAt); err != nil {
		return AgentHeartbeatLeaseResult{}, err
	}
	conflict, blocked, err := agentHeartbeatLeaseConflict(ctx, tx, workspaceID, agentID, heartbeatID, leaseToken, acquiredAt)
	if err != nil {
		return AgentHeartbeatLeaseResult{}, err
	}
	if blocked {
		return conflict, nil
	}
	for _, lockName := range locks {
		conflict, blocked, err = agentHeartbeatLockLeaseConflict(ctx, tx, workspaceID, agentID, lockName, leaseToken, acquiredAt)
		if err != nil {
			return AgentHeartbeatLeaseResult{}, err
		}
		if blocked {
			return conflict, nil
		}
	}

	locksJSON, err := json.Marshal(locks)
	if err != nil {
		return AgentHeartbeatLeaseResult{}, fmt.Errorf("marshal heartbeat locks: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO agent_heartbeat_leases
		   (workspace_id, agent_id, heartbeat_id, owner_id, lease_token, locks_json, acquired_at, updated_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(workspace_id, agent_id, heartbeat_id) DO UPDATE SET
		   owner_id = excluded.owner_id,
		   lease_token = excluded.lease_token,
		   locks_json = excluded.locks_json,
		   updated_at = excluded.updated_at,
		   expires_at = excluded.expires_at`,
		workspaceID, agentID, heartbeatID, ownerID, leaseToken, string(locksJSON), acquiredAt, acquiredAt, expiresAt,
	); err != nil {
		return AgentHeartbeatLeaseResult{}, fmt.Errorf("upsert agent heartbeat lease: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM agent_heartbeat_lock_leases
		  WHERE workspace_id = ? AND agent_id = ? AND (heartbeat_id = ? OR lease_token = ?)`,
		workspaceID, agentID, heartbeatID, leaseToken,
	); err != nil {
		return AgentHeartbeatLeaseResult{}, fmt.Errorf("replace heartbeat lock leases: %w", err)
	}
	for _, lockName := range locks {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO agent_heartbeat_lock_leases
			   (workspace_id, agent_id, lock_name, heartbeat_id, owner_id, lease_token, expires_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(workspace_id, agent_id, lock_name) DO UPDATE SET
			   heartbeat_id = excluded.heartbeat_id,
			   owner_id = excluded.owner_id,
			   lease_token = excluded.lease_token,
			   expires_at = excluded.expires_at,
			   updated_at = excluded.updated_at`,
			workspaceID, agentID, lockName, heartbeatID, ownerID, leaseToken, expiresAt, acquiredAt,
		); err != nil {
			return AgentHeartbeatLeaseResult{}, fmt.Errorf("upsert heartbeat lock lease %q: %w", lockName, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return AgentHeartbeatLeaseResult{}, fmt.Errorf("commit agent heartbeat lease tx: %w", err)
	}
	tx = nil
	return AgentHeartbeatLeaseResult{
		Acquired:    true,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		HeartbeatID: heartbeatID,
		OwnerID:     ownerID,
		LeaseToken:  leaseToken,
		Locks:       locks,
		AcquiredAt:  acquiredAt,
		ExpiresAt:   expiresAt,
	}, nil
}

// RefreshAgentHeartbeatLease refreshes an existing lease token or reacquires it
// when only expired rows remain.
func (s *Store) RefreshAgentHeartbeatLease(ctx context.Context, input AgentHeartbeatLeaseInput) (AgentHeartbeatLeaseResult, error) {
	if len(normalizeAgentHeartbeatLeaseLocks(input.Locks)) == 0 {
		locks, ok, err := s.agentHeartbeatLeaseLocksForToken(ctx, input)
		if err != nil {
			return AgentHeartbeatLeaseResult{}, err
		}
		if ok {
			input.Locks = locks
		}
	}
	return s.AcquireAgentHeartbeatLease(ctx, input)
}

// ReleaseAgentHeartbeatLease releases a lease only when the token matches.
func (s *Store) ReleaseAgentHeartbeatLease(ctx context.Context, input AgentHeartbeatLeaseReleaseInput) (bool, error) {
	if s == nil {
		return false, errors.New("store is nil")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	agentID := strings.TrimSpace(input.AgentID)
	heartbeatID := strings.TrimSpace(input.HeartbeatID)
	leaseToken := strings.TrimSpace(input.LeaseToken)
	if workspaceID == "" || agentID == "" || heartbeatID == "" || leaseToken == "" {
		return false, errors.New("workspace_id, agent_id, heartbeat_id, and lease_token are required")
	}
	if err := s.ensureAgentHeartbeatLeaseTables(ctx); err != nil {
		return false, err
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin release heartbeat lease tx: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM agent_heartbeat_lock_leases
		  WHERE workspace_id = ? AND agent_id = ? AND heartbeat_id = ? AND lease_token = ?`,
		workspaceID, agentID, heartbeatID, leaseToken,
	); err != nil {
		return false, fmt.Errorf("release heartbeat lock leases: %w", err)
	}
	res, err := tx.ExecContext(ctx,
		`DELETE FROM agent_heartbeat_leases
		  WHERE workspace_id = ? AND agent_id = ? AND heartbeat_id = ? AND lease_token = ?`,
		workspaceID, agentID, heartbeatID, leaseToken,
	)
	if err != nil {
		return false, fmt.Errorf("release heartbeat lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit release heartbeat lease tx: %w", err)
	}
	tx = nil
	rows, _ := res.RowsAffected()
	return rows > 0, nil
}

func deleteExpiredAgentHeartbeatLeases(ctx context.Context, tx *sql.Tx, workspaceID, agentID, now string) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM agent_heartbeat_lock_leases
		  WHERE workspace_id = ? AND agent_id = ? AND expires_at <= ?`,
		workspaceID, agentID, now,
	); err != nil {
		return fmt.Errorf("cleanup expired heartbeat lock leases: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM agent_heartbeat_leases
		  WHERE workspace_id = ? AND agent_id = ? AND expires_at <= ?`,
		workspaceID, agentID, now,
	); err != nil {
		return fmt.Errorf("cleanup expired heartbeat leases: %w", err)
	}
	return nil
}

func agentHeartbeatLeaseConflict(ctx context.Context, tx *sql.Tx, workspaceID, agentID, heartbeatID, leaseToken, now string) (AgentHeartbeatLeaseResult, bool, error) {
	var existingHeartbeatID, ownerID, token, expiresAt string
	err := tx.QueryRowContext(ctx,
		`SELECT heartbeat_id, owner_id, lease_token, expires_at
		   FROM agent_heartbeat_leases
		  WHERE workspace_id = ? AND agent_id = ? AND heartbeat_id = ? AND expires_at > ?`,
		workspaceID, agentID, heartbeatID, now,
	).Scan(&existingHeartbeatID, &ownerID, &token, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentHeartbeatLeaseResult{}, false, nil
	}
	if err != nil {
		return AgentHeartbeatLeaseResult{}, false, fmt.Errorf("check heartbeat lease conflict: %w", err)
	}
	if token == leaseToken {
		return AgentHeartbeatLeaseResult{}, false, nil
	}
	return AgentHeartbeatLeaseResult{
		Acquired:            false,
		WorkspaceID:         workspaceID,
		AgentID:             agentID,
		HeartbeatID:         heartbeatID,
		ConflictReason:      "heartbeat_already_leased",
		ConflictHeartbeatID: existingHeartbeatID,
		ConflictOwnerID:     ownerID,
		ConflictExpiresAt:   expiresAt,
	}, true, nil
}

func agentHeartbeatLockLeaseConflict(ctx context.Context, tx *sql.Tx, workspaceID, agentID, lockName, leaseToken, now string) (AgentHeartbeatLeaseResult, bool, error) {
	var heartbeatID, ownerID, token, expiresAt string
	err := tx.QueryRowContext(ctx,
		`SELECT heartbeat_id, owner_id, lease_token, expires_at
		   FROM agent_heartbeat_lock_leases
		  WHERE workspace_id = ? AND agent_id = ? AND lock_name = ? AND expires_at > ?`,
		workspaceID, agentID, lockName, now,
	).Scan(&heartbeatID, &ownerID, &token, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentHeartbeatLeaseResult{}, false, nil
	}
	if err != nil {
		return AgentHeartbeatLeaseResult{}, false, fmt.Errorf("check heartbeat lock lease conflict: %w", err)
	}
	if token == leaseToken {
		return AgentHeartbeatLeaseResult{}, false, nil
	}
	return AgentHeartbeatLeaseResult{
		Acquired:            false,
		WorkspaceID:         workspaceID,
		AgentID:             agentID,
		ConflictReason:      "lock_already_leased",
		ConflictHeartbeatID: heartbeatID,
		ConflictLock:        lockName,
		ConflictOwnerID:     ownerID,
		ConflictExpiresAt:   expiresAt,
	}, true, nil
}

func (s *Store) agentHeartbeatLeaseLocksForToken(ctx context.Context, input AgentHeartbeatLeaseInput) ([]string, bool, error) {
	if s == nil {
		return nil, false, errors.New("store is nil")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	agentID := strings.TrimSpace(input.AgentID)
	heartbeatID := strings.TrimSpace(input.HeartbeatID)
	leaseToken := strings.TrimSpace(input.LeaseToken)
	if workspaceID == "" || agentID == "" || heartbeatID == "" || leaseToken == "" {
		return nil, false, nil
	}
	if err := s.ensureAgentHeartbeatLeaseTables(ctx); err != nil {
		return nil, false, err
	}
	now := input.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var raw string
	err := s.db.QueryRowContext(ctx,
		`SELECT locks_json
		   FROM agent_heartbeat_leases
		  WHERE workspace_id = ? AND agent_id = ? AND heartbeat_id = ? AND lease_token = ? AND expires_at > ?`,
		workspaceID, agentID, heartbeatID, leaseToken, now.Format(time.RFC3339Nano),
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read heartbeat lease locks: %w", err)
	}
	var locks []string
	if err := json.Unmarshal([]byte(raw), &locks); err != nil {
		return nil, false, fmt.Errorf("decode heartbeat lease locks: %w", err)
	}
	return normalizeAgentHeartbeatLeaseLocks(locks), true, nil
}

func normalizeAgentHeartbeatLeaseLocks(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func clampAgentHeartbeatLeaseTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return time.Minute
	}
	if ttl < minAgentHeartbeatLeaseTTL {
		return minAgentHeartbeatLeaseTTL
	}
	if ttl > maxAgentHeartbeatLeaseTTL {
		return maxAgentHeartbeatLeaseTTL
	}
	return ttl
}
