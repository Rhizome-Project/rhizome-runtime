package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type WorkspaceReplicaRole string

const (
	WorkspaceReplicaRoleHolder   WorkspaceReplicaRole = "HOLDER"
	WorkspaceReplicaRoleFollower WorkspaceReplicaRole = "FOLLOWER"
)

type WorkspaceReplicaMembershipState string

const (
	WorkspaceReplicaMembershipProvisional   WorkspaceReplicaMembershipState = "PROVISIONAL"
	WorkspaceReplicaMembershipActive        WorkspaceReplicaMembershipState = "ACTIVE"
	WorkspaceReplicaMembershipCatchingUp    WorkspaceReplicaMembershipState = "CATCHING_UP"
	WorkspaceReplicaMembershipStale         WorkspaceReplicaMembershipState = "STALE"
	WorkspaceReplicaMembershipRejoinPending WorkspaceReplicaMembershipState = "REJOIN_PENDING"
	WorkspaceReplicaMembershipRejected      WorkspaceReplicaMembershipState = "REJECTED"
	WorkspaceReplicaMembershipDisbanded     WorkspaceReplicaMembershipState = "DISBANDED"
)

type WorkspaceReplicaStateRecord struct {
	WorkspaceID            string                          `json:"workspace_id"`
	Scope                  string                          `json:"scope"`
	ReplicaAuthorityNodeID string                          `json:"replica_authority_node_id"`
	ReplicaRole            WorkspaceReplicaRole            `json:"replica_role"`
	MembershipState        WorkspaceReplicaMembershipState `json:"membership_state"`
	LeaderAuthorityNodeID  string                          `json:"leader_authority_node_id,omitempty"`
	AuthorityTerm          int64                           `json:"authority_term"`
	CommitWatermark        int64                           `json:"commit_watermark"`
	AppliedWatermark       int64                           `json:"applied_watermark"`
	LastFetchAt            string                          `json:"last_fetch_at,omitempty"`
	LastApplyAt            string                          `json:"last_apply_at,omitempty"`
	MembershipReason       string                          `json:"membership_reason,omitempty"`
	UpdatedAt              string                          `json:"updated_at"`
}

func normalizeWorkspaceReplicaRole(role WorkspaceReplicaRole) (WorkspaceReplicaRole, error) {
	switch WorkspaceReplicaRole(strings.ToUpper(strings.TrimSpace(string(role)))) {
	case WorkspaceReplicaRoleHolder:
		return WorkspaceReplicaRoleHolder, nil
	case WorkspaceReplicaRoleFollower:
		return WorkspaceReplicaRoleFollower, nil
	default:
		return "", fmt.Errorf("unsupported replica_role %q", role)
	}
}

func normalizeWorkspaceReplicaMembershipState(state WorkspaceReplicaMembershipState) (WorkspaceReplicaMembershipState, error) {
	switch WorkspaceReplicaMembershipState(strings.ToUpper(strings.TrimSpace(string(state)))) {
	case WorkspaceReplicaMembershipProvisional:
		return WorkspaceReplicaMembershipProvisional, nil
	case WorkspaceReplicaMembershipActive:
		return WorkspaceReplicaMembershipActive, nil
	case WorkspaceReplicaMembershipCatchingUp:
		return WorkspaceReplicaMembershipCatchingUp, nil
	case WorkspaceReplicaMembershipStale:
		return WorkspaceReplicaMembershipStale, nil
	case WorkspaceReplicaMembershipRejoinPending:
		return WorkspaceReplicaMembershipRejoinPending, nil
	case WorkspaceReplicaMembershipRejected:
		return WorkspaceReplicaMembershipRejected, nil
	case WorkspaceReplicaMembershipDisbanded:
		return WorkspaceReplicaMembershipDisbanded, nil
	default:
		return "", fmt.Errorf("unsupported membership_state %q", state)
	}
}

func normalizeWorkspaceReplicaStateRecord(input WorkspaceReplicaStateRecord) (WorkspaceReplicaStateRecord, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return WorkspaceReplicaStateRecord{}, errors.New("workspace_id is required")
	}
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	input.ReplicaAuthorityNodeID = strings.TrimSpace(input.ReplicaAuthorityNodeID)
	if input.ReplicaAuthorityNodeID == "" {
		return WorkspaceReplicaStateRecord{}, errors.New("replica_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaStateRecord{}, fmt.Errorf("replica_authority_node_id is invalid: %w", err)
	}
	role, err := normalizeWorkspaceReplicaRole(input.ReplicaRole)
	if err != nil {
		return WorkspaceReplicaStateRecord{}, err
	}
	input.ReplicaRole = role
	state, err := normalizeWorkspaceReplicaMembershipState(input.MembershipState)
	if err != nil {
		return WorkspaceReplicaStateRecord{}, err
	}
	input.MembershipState = state
	input.LeaderAuthorityNodeID = strings.TrimSpace(input.LeaderAuthorityNodeID)
	if input.LeaderAuthorityNodeID != "" {
		if err := validateAuthorityNodeID(input.LeaderAuthorityNodeID); err != nil {
			return WorkspaceReplicaStateRecord{}, fmt.Errorf("leader_authority_node_id is invalid: %w", err)
		}
	}
	if input.AuthorityTerm <= 0 {
		return WorkspaceReplicaStateRecord{}, errors.New("authority_term must be > 0")
	}
	if input.CommitWatermark < 0 {
		return WorkspaceReplicaStateRecord{}, errors.New("commit_watermark must be >= 0")
	}
	if input.AppliedWatermark < 0 {
		return WorkspaceReplicaStateRecord{}, errors.New("applied_watermark must be >= 0")
	}
	if input.AppliedWatermark > input.CommitWatermark {
		return WorkspaceReplicaStateRecord{}, errors.New("applied_watermark cannot exceed commit_watermark")
	}
	switch input.ReplicaRole {
	case WorkspaceReplicaRoleHolder:
		if input.LeaderAuthorityNodeID == "" {
			input.LeaderAuthorityNodeID = input.ReplicaAuthorityNodeID
		}
		if input.LeaderAuthorityNodeID != input.ReplicaAuthorityNodeID {
			return WorkspaceReplicaStateRecord{}, errors.New("holder replica_role requires leader_authority_node_id to match replica_authority_node_id")
		}
	case WorkspaceReplicaRoleFollower:
		if input.LeaderAuthorityNodeID == "" {
			return WorkspaceReplicaStateRecord{}, errors.New("follower replica_role requires leader_authority_node_id")
		}
		if input.LeaderAuthorityNodeID == input.ReplicaAuthorityNodeID {
			return WorkspaceReplicaStateRecord{}, errors.New("follower replica_role requires leader_authority_node_id to differ from replica_authority_node_id")
		}
	}
	input.LastFetchAt = strings.TrimSpace(input.LastFetchAt)
	input.LastApplyAt = strings.TrimSpace(input.LastApplyAt)
	input.MembershipReason = strings.TrimSpace(input.MembershipReason)
	if _, err := parseAuthorityTimestamp("last_fetch_at", input.LastFetchAt, false); err != nil {
		return WorkspaceReplicaStateRecord{}, err
	}
	if _, err := parseAuthorityTimestamp("last_apply_at", input.LastApplyAt, false); err != nil {
		return WorkspaceReplicaStateRecord{}, err
	}
	input.UpdatedAt = strings.TrimSpace(input.UpdatedAt)
	if input.UpdatedAt == "" {
		input.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if _, err := parseAuthorityTimestamp("updated_at", input.UpdatedAt, true); err != nil {
		return WorkspaceReplicaStateRecord{}, err
	}
	return input, nil
}

func scanWorkspaceReplicaState(scanner interface{ Scan(dest ...any) error }, record *WorkspaceReplicaStateRecord) error {
	var leaderAuthorityNodeID sql.NullString
	var lastFetchAt sql.NullString
	var lastApplyAt sql.NullString
	var membershipReason sql.NullString
	if err := scanner.Scan(
		&record.WorkspaceID,
		&record.Scope,
		&record.ReplicaAuthorityNodeID,
		&record.ReplicaRole,
		&record.MembershipState,
		&leaderAuthorityNodeID,
		&record.AuthorityTerm,
		&record.CommitWatermark,
		&record.AppliedWatermark,
		&lastFetchAt,
		&lastApplyAt,
		&membershipReason,
		&record.UpdatedAt,
	); err != nil {
		return err
	}
	if leaderAuthorityNodeID.Valid {
		record.LeaderAuthorityNodeID = leaderAuthorityNodeID.String
	}
	if lastFetchAt.Valid {
		record.LastFetchAt = lastFetchAt.String
	}
	if lastApplyAt.Valid {
		record.LastApplyAt = lastApplyAt.String
	}
	if membershipReason.Valid {
		record.MembershipReason = membershipReason.String
	}
	return nil
}

func (s *Store) GetWorkspaceReplicaState(ctx context.Context, workspaceID, scope, replicaAuthorityNodeID string) (WorkspaceReplicaStateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return WorkspaceReplicaStateRecord{}, errors.New("workspace_id is required")
	}
	scope = normalizeWorkspaceAuthorityScope(scope)
	replicaAuthorityNodeID = strings.TrimSpace(replicaAuthorityNodeID)
	if replicaAuthorityNodeID == "" {
		return WorkspaceReplicaStateRecord{}, errors.New("replica_authority_node_id is required")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT workspace_id, scope, replica_authority_node_id, replica_role, membership_state,
       leader_authority_node_id, authority_term, commit_watermark, applied_watermark,
       last_fetch_at, last_apply_at, membership_reason, updated_at
  FROM workspace_replica_state
 WHERE workspace_id = ? AND scope = ? AND replica_authority_node_id = ?`,
		workspaceID,
		scope,
		replicaAuthorityNodeID,
	)
	var record WorkspaceReplicaStateRecord
	if err := scanWorkspaceReplicaState(row, &record); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaStateRecord{}, err
		}
		return WorkspaceReplicaStateRecord{}, fmt.Errorf("get workspace replica state: %w", err)
	}
	return record, nil
}

func (s *Store) ListWorkspaceReplicaStates(ctx context.Context, workspaceID, scope string) ([]WorkspaceReplicaStateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	scope = normalizeWorkspaceAuthorityScope(scope)
	rows, err := s.db.QueryContext(ctx, `
SELECT workspace_id, scope, replica_authority_node_id, replica_role, membership_state,
       leader_authority_node_id, authority_term, commit_watermark, applied_watermark,
       last_fetch_at, last_apply_at, membership_reason, updated_at
  FROM workspace_replica_state
 WHERE workspace_id = ? AND scope = ?
 ORDER BY
   CASE replica_role WHEN 'HOLDER' THEN 0 ELSE 1 END,
   authority_term DESC,
   updated_at DESC,
   replica_authority_node_id`,
		workspaceID,
		scope,
	)
	if err != nil {
		return nil, fmt.Errorf("list workspace replica states: %w", err)
	}
	defer rows.Close()
	var out []WorkspaceReplicaStateRecord
	for rows.Next() {
		var record WorkspaceReplicaStateRecord
		if err := scanWorkspaceReplicaState(rows, &record); err != nil {
			return nil, fmt.Errorf("scan workspace replica state: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace replica states: %w", err)
	}
	return out, nil
}

func (s *Store) UpsertWorkspaceReplicaState(ctx context.Context, input WorkspaceReplicaStateRecord) (WorkspaceReplicaStateRecord, error) {
	record, err := normalizeWorkspaceReplicaStateRecord(input)
	if err != nil {
		return WorkspaceReplicaStateRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceReplicaStateRecord{}, fmt.Errorf("begin workspace replica state tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	record, err = s.upsertWorkspaceReplicaStateTx(ctx, tx, record)
	if err != nil {
		return WorkspaceReplicaStateRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceReplicaStateRecord{}, fmt.Errorf("commit workspace replica state tx: %w", err)
	}
	return record, nil
}

func (s *Store) upsertWorkspaceReplicaStateTx(ctx context.Context, tx *sql.Tx, record WorkspaceReplicaStateRecord) (WorkspaceReplicaStateRecord, error) {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return WorkspaceReplicaStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, record.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaStateRecord{}, err
	}
	if record.LeaderAuthorityNodeID != "" {
		if err := s.ensureRuntimeNodeExistsTx(ctx, tx, record.LeaderAuthorityNodeID); err != nil {
			return WorkspaceReplicaStateRecord{}, err
		}
	}
	current, err := s.getWorkspaceReplicaStateTx(ctx, tx, record.WorkspaceID, record.Scope, record.ReplicaAuthorityNodeID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// New row.
	case err != nil:
		return WorkspaceReplicaStateRecord{}, err
	default:
		if record.AuthorityTerm < current.AuthorityTerm {
			return WorkspaceReplicaStateRecord{}, fmt.Errorf("authority_term %d regresses existing replica term %d", record.AuthorityTerm, current.AuthorityTerm)
		}
		if record.CommitWatermark < current.CommitWatermark {
			return WorkspaceReplicaStateRecord{}, fmt.Errorf("commit_watermark %d regresses existing replica watermark %d", record.CommitWatermark, current.CommitWatermark)
		}
		if record.AppliedWatermark < current.AppliedWatermark {
			return WorkspaceReplicaStateRecord{}, fmt.Errorf("applied_watermark %d regresses existing replica watermark %d", record.AppliedWatermark, current.AppliedWatermark)
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO workspace_replica_state(
	workspace_id, scope, replica_authority_node_id, replica_role, membership_state,
	leader_authority_node_id, authority_term, commit_watermark, applied_watermark,
	last_fetch_at, last_apply_at, membership_reason, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workspace_id, scope, replica_authority_node_id) DO UPDATE SET
	replica_role = excluded.replica_role,
	membership_state = excluded.membership_state,
	leader_authority_node_id = excluded.leader_authority_node_id,
	authority_term = excluded.authority_term,
	commit_watermark = excluded.commit_watermark,
	applied_watermark = excluded.applied_watermark,
	last_fetch_at = excluded.last_fetch_at,
	last_apply_at = excluded.last_apply_at,
	membership_reason = excluded.membership_reason,
	updated_at = excluded.updated_at`,
		record.WorkspaceID,
		record.Scope,
		record.ReplicaAuthorityNodeID,
		record.ReplicaRole,
		record.MembershipState,
		blankStringOrNil(record.LeaderAuthorityNodeID),
		record.AuthorityTerm,
		record.CommitWatermark,
		record.AppliedWatermark,
		record.LastFetchAt,
		record.LastApplyAt,
		record.MembershipReason,
		record.UpdatedAt,
	); err != nil {
		return WorkspaceReplicaStateRecord{}, fmt.Errorf("upsert workspace replica state: %w", err)
	}
	return s.getWorkspaceReplicaStateTx(ctx, tx, record.WorkspaceID, record.Scope, record.ReplicaAuthorityNodeID)
}

func (s *Store) getWorkspaceReplicaStateTx(ctx context.Context, tx *sql.Tx, workspaceID, scope, replicaAuthorityNodeID string) (WorkspaceReplicaStateRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT workspace_id, scope, replica_authority_node_id, replica_role, membership_state,
       leader_authority_node_id, authority_term, commit_watermark, applied_watermark,
       last_fetch_at, last_apply_at, membership_reason, updated_at
  FROM workspace_replica_state
 WHERE workspace_id = ? AND scope = ? AND replica_authority_node_id = ?`,
		workspaceID,
		scope,
		replicaAuthorityNodeID,
	)
	var record WorkspaceReplicaStateRecord
	if err := scanWorkspaceReplicaState(row, &record); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaStateRecord{}, err
		}
		return WorkspaceReplicaStateRecord{}, fmt.Errorf("get workspace replica state tx: %w", err)
	}
	return record, nil
}
