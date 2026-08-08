package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type WorkspaceReplicaInstallStatus string

const (
	WorkspaceReplicaInstallPending   WorkspaceReplicaInstallStatus = "PENDING"
	WorkspaceReplicaInstallInstalled WorkspaceReplicaInstallStatus = "INSTALLED"
)

type WorkspaceReplicaInstallStateRecord struct {
	WorkspaceID            string                        `json:"workspace_id"`
	Scope                  string                        `json:"scope"`
	ReplicaAuthorityNodeID string                        `json:"replica_authority_node_id"`
	LeaderAuthorityNodeID  string                        `json:"leader_authority_node_id"`
	AuthorityTerm          int64                         `json:"authority_term"`
	InstallStatus          WorkspaceReplicaInstallStatus `json:"install_status"`
	BaseCommitWatermark    int64                         `json:"base_commit_watermark"`
	InstallStartedAt       string                        `json:"install_started_at"`
	InstallCompletedAt     string                        `json:"install_completed_at,omitempty"`
	InstallReason          string                        `json:"install_reason,omitempty"`
	UpdatedAt              string                        `json:"updated_at"`
}

type WorkspaceReplicaInstallBeginInput struct {
	WorkspaceID            string
	Scope                  string
	ReplicaAuthorityNodeID string
	LeaderAuthorityNodeID  string
	AuthorityTerm          int64
	BaseCommitWatermark    int64
	InstallStartedAt       string
	InstallReason          string
}

type WorkspaceReplicaInstallCompleteInput struct {
	WorkspaceID            string
	Scope                  string
	ReplicaAuthorityNodeID string
	LeaderAuthorityNodeID  string
	AuthorityTerm          int64
	BaseCommitWatermark    int64
	InstallCompletedAt     string
	InstallReason          string
}

func normalizeWorkspaceReplicaInstallStatus(status WorkspaceReplicaInstallStatus) (WorkspaceReplicaInstallStatus, error) {
	switch WorkspaceReplicaInstallStatus(strings.ToUpper(strings.TrimSpace(string(status)))) {
	case WorkspaceReplicaInstallPending:
		return WorkspaceReplicaInstallPending, nil
	case WorkspaceReplicaInstallInstalled:
		return WorkspaceReplicaInstallInstalled, nil
	default:
		return "", fmt.Errorf("unsupported install_status %q", status)
	}
}

func normalizeWorkspaceReplicaInstallStateRecord(input WorkspaceReplicaInstallStateRecord) (WorkspaceReplicaInstallStateRecord, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return WorkspaceReplicaInstallStateRecord{}, errors.New("workspace_id is required")
	}
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	input.ReplicaAuthorityNodeID = strings.TrimSpace(input.ReplicaAuthorityNodeID)
	if input.ReplicaAuthorityNodeID == "" {
		return WorkspaceReplicaInstallStateRecord{}, errors.New("replica_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, fmt.Errorf("replica_authority_node_id is invalid: %w", err)
	}
	input.LeaderAuthorityNodeID = strings.TrimSpace(input.LeaderAuthorityNodeID)
	if input.LeaderAuthorityNodeID == "" {
		return WorkspaceReplicaInstallStateRecord{}, errors.New("leader_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.LeaderAuthorityNodeID); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, fmt.Errorf("leader_authority_node_id is invalid: %w", err)
	}
	if input.LeaderAuthorityNodeID == input.ReplicaAuthorityNodeID {
		return WorkspaceReplicaInstallStateRecord{}, errors.New("leader_authority_node_id must differ from replica_authority_node_id")
	}
	if input.AuthorityTerm <= 0 {
		return WorkspaceReplicaInstallStateRecord{}, errors.New("authority_term must be > 0")
	}
	if input.BaseCommitWatermark < 0 {
		return WorkspaceReplicaInstallStateRecord{}, errors.New("base_commit_watermark must be >= 0")
	}
	status, err := normalizeWorkspaceReplicaInstallStatus(input.InstallStatus)
	if err != nil {
		return WorkspaceReplicaInstallStateRecord{}, err
	}
	input.InstallStatus = status
	input.InstallReason = strings.TrimSpace(input.InstallReason)
	input.UpdatedAt = strings.TrimSpace(input.UpdatedAt)
	if input.UpdatedAt == "" {
		input.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if _, err := parseAuthorityTimestamp("updated_at", input.UpdatedAt, true); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, err
	}
	input.InstallStartedAt = strings.TrimSpace(input.InstallStartedAt)
	input.InstallCompletedAt = strings.TrimSpace(input.InstallCompletedAt)
	switch input.InstallStatus {
	case WorkspaceReplicaInstallPending:
		if input.InstallStartedAt == "" {
			input.InstallStartedAt = input.UpdatedAt
		}
		if _, err := parseAuthorityTimestamp("install_started_at", input.InstallStartedAt, true); err != nil {
			return WorkspaceReplicaInstallStateRecord{}, err
		}
		if input.InstallCompletedAt != "" {
			return WorkspaceReplicaInstallStateRecord{}, errors.New("pending install_state cannot set install_completed_at")
		}
	case WorkspaceReplicaInstallInstalled:
		if input.InstallStartedAt == "" {
			input.InstallStartedAt = input.UpdatedAt
		}
		if _, err := parseAuthorityTimestamp("install_started_at", input.InstallStartedAt, true); err != nil {
			return WorkspaceReplicaInstallStateRecord{}, err
		}
		if input.InstallCompletedAt == "" {
			input.InstallCompletedAt = input.UpdatedAt
		}
		startedAt, err := parseAuthorityTimestamp("install_started_at", input.InstallStartedAt, true)
		if err != nil {
			return WorkspaceReplicaInstallStateRecord{}, err
		}
		completedAt, err := parseAuthorityTimestamp("install_completed_at", input.InstallCompletedAt, true)
		if err != nil {
			return WorkspaceReplicaInstallStateRecord{}, err
		}
		if completedAt.Before(startedAt) {
			return WorkspaceReplicaInstallStateRecord{}, errors.New("install_completed_at cannot be earlier than install_started_at")
		}
	}
	return input, nil
}

func scanWorkspaceReplicaInstallState(scanner interface{ Scan(dest ...any) error }, record *WorkspaceReplicaInstallStateRecord) error {
	return scanner.Scan(
		&record.WorkspaceID,
		&record.Scope,
		&record.ReplicaAuthorityNodeID,
		&record.LeaderAuthorityNodeID,
		&record.AuthorityTerm,
		&record.InstallStatus,
		&record.BaseCommitWatermark,
		&record.InstallStartedAt,
		&record.InstallCompletedAt,
		&record.InstallReason,
		&record.UpdatedAt,
	)
}

func (s *Store) GetWorkspaceReplicaInstallState(ctx context.Context, workspaceID, scope, replicaAuthorityNodeID string) (WorkspaceReplicaInstallStateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return WorkspaceReplicaInstallStateRecord{}, errors.New("workspace_id is required")
	}
	scope = normalizeWorkspaceAuthorityScope(scope)
	replicaAuthorityNodeID = strings.TrimSpace(replicaAuthorityNodeID)
	if replicaAuthorityNodeID == "" {
		return WorkspaceReplicaInstallStateRecord{}, errors.New("replica_authority_node_id is required")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT workspace_id, scope, replica_authority_node_id, leader_authority_node_id,
       authority_term, install_status, base_commit_watermark, install_started_at,
       install_completed_at, install_reason, updated_at
  FROM workspace_replica_install_state
 WHERE workspace_id = ? AND scope = ? AND replica_authority_node_id = ?`,
		workspaceID,
		scope,
		replicaAuthorityNodeID,
	)
	var record WorkspaceReplicaInstallStateRecord
	if err := scanWorkspaceReplicaInstallState(row, &record); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaInstallStateRecord{}, err
		}
		return WorkspaceReplicaInstallStateRecord{}, fmt.Errorf("get workspace replica install state: %w", err)
	}
	return record, nil
}

func (s *Store) ListWorkspaceReplicaInstallStates(ctx context.Context, workspaceID, scope string) ([]WorkspaceReplicaInstallStateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	scope = normalizeWorkspaceAuthorityScope(scope)
	rows, err := s.db.QueryContext(ctx, `
SELECT workspace_id, scope, replica_authority_node_id, leader_authority_node_id,
       authority_term, install_status, base_commit_watermark, install_started_at,
       install_completed_at, install_reason, updated_at
  FROM workspace_replica_install_state
 WHERE workspace_id = ? AND scope = ?
 ORDER BY authority_term DESC, updated_at DESC, replica_authority_node_id`,
		workspaceID,
		scope,
	)
	if err != nil {
		return nil, fmt.Errorf("list workspace replica install states: %w", err)
	}
	defer rows.Close()
	var out []WorkspaceReplicaInstallStateRecord
	for rows.Next() {
		var record WorkspaceReplicaInstallStateRecord
		if err := scanWorkspaceReplicaInstallState(rows, &record); err != nil {
			return nil, fmt.Errorf("scan workspace replica install state: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace replica install states: %w", err)
	}
	return out, nil
}

func normalizeWorkspaceReplicaInstallBeginInput(input WorkspaceReplicaInstallBeginInput) (WorkspaceReplicaInstallStateRecord, error) {
	return normalizeWorkspaceReplicaInstallStateRecord(WorkspaceReplicaInstallStateRecord{
		WorkspaceID:            input.WorkspaceID,
		Scope:                  input.Scope,
		ReplicaAuthorityNodeID: input.ReplicaAuthorityNodeID,
		LeaderAuthorityNodeID:  input.LeaderAuthorityNodeID,
		AuthorityTerm:          input.AuthorityTerm,
		InstallStatus:          WorkspaceReplicaInstallPending,
		BaseCommitWatermark:    input.BaseCommitWatermark,
		InstallStartedAt:       input.InstallStartedAt,
		InstallReason:          input.InstallReason,
	})
}

func normalizeWorkspaceReplicaInstallCompleteInput(input WorkspaceReplicaInstallCompleteInput) (WorkspaceReplicaInstallStateRecord, error) {
	return normalizeWorkspaceReplicaInstallStateRecord(WorkspaceReplicaInstallStateRecord{
		WorkspaceID:            input.WorkspaceID,
		Scope:                  input.Scope,
		ReplicaAuthorityNodeID: input.ReplicaAuthorityNodeID,
		LeaderAuthorityNodeID:  input.LeaderAuthorityNodeID,
		AuthorityTerm:          input.AuthorityTerm,
		InstallStatus:          WorkspaceReplicaInstallInstalled,
		BaseCommitWatermark:    input.BaseCommitWatermark,
		InstallCompletedAt:     input.InstallCompletedAt,
		InstallReason:          input.InstallReason,
	})
}

func (s *Store) BeginWorkspaceReplicaInstall(ctx context.Context, input WorkspaceReplicaInstallBeginInput) (WorkspaceReplicaInstallStateRecord, error) {
	record, err := normalizeWorkspaceReplicaInstallBeginInput(input)
	if err != nil {
		return WorkspaceReplicaInstallStateRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceReplicaInstallStateRecord{}, fmt.Errorf("begin workspace replica install tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	record, err = s.beginWorkspaceReplicaInstallTx(ctx, tx, record)
	if err != nil {
		return WorkspaceReplicaInstallStateRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, fmt.Errorf("commit workspace replica install tx: %w", err)
	}
	return record, nil
}

func (s *Store) CompleteWorkspaceReplicaInstall(ctx context.Context, input WorkspaceReplicaInstallCompleteInput) (WorkspaceReplicaInstallStateRecord, WorkspaceReplicaStateRecord, error) {
	record, err := normalizeWorkspaceReplicaInstallCompleteInput(input)
	if err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("begin workspace replica install complete tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	record, replicaState, err := s.completeWorkspaceReplicaInstallTx(ctx, tx, record)
	if err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("commit workspace replica install complete tx: %w", err)
	}
	return record, replicaState, nil
}

func (s *Store) beginWorkspaceReplicaInstallTx(ctx context.Context, tx *sql.Tx, record WorkspaceReplicaInstallStateRecord) (WorkspaceReplicaInstallStateRecord, error) {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, record.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, record.LeaderAuthorityNodeID); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, err
	}
	authority, err := s.getWorkspaceAuthorityTx(ctx, tx, record.WorkspaceID, record.Scope)
	if err != nil {
		return WorkspaceReplicaInstallStateRecord{}, fmt.Errorf("get workspace authority for replica install begin: %w", err)
	}
	if err := validateWorkspaceReplicaInstallAgainstAuthority(authority, record); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, err
	}
	current, err := s.getWorkspaceReplicaInstallStateTx(ctx, tx, record.WorkspaceID, record.Scope, record.ReplicaAuthorityNodeID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return WorkspaceReplicaInstallStateRecord{}, err
	default:
		if err := validateWorkspaceReplicaInstallProgress(current, record); err != nil {
			return WorkspaceReplicaInstallStateRecord{}, err
		}
	}
	if err := s.upsertWorkspaceReplicaInstallStateTx(ctx, tx, record); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, err
	}
	return s.getWorkspaceReplicaInstallStateTx(ctx, tx, record.WorkspaceID, record.Scope, record.ReplicaAuthorityNodeID)
}

func (s *Store) completeWorkspaceReplicaInstallTx(ctx context.Context, tx *sql.Tx, record WorkspaceReplicaInstallStateRecord) (WorkspaceReplicaInstallStateRecord, WorkspaceReplicaStateRecord, error) {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, record.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, record.LeaderAuthorityNodeID); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	authority, err := s.getWorkspaceAuthorityTx(ctx, tx, record.WorkspaceID, record.Scope)
	if err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("get workspace authority for replica install complete: %w", err)
	}
	if err := validateWorkspaceReplicaInstallAgainstAuthority(authority, record); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	current, err := s.getWorkspaceReplicaInstallStateTx(ctx, tx, record.WorkspaceID, record.Scope, record.ReplicaAuthorityNodeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, errors.New("replica install must begin before completion")
		}
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if current.InstallStatus == WorkspaceReplicaInstallInstalled {
		if current.AuthorityTerm == record.AuthorityTerm &&
			current.LeaderAuthorityNodeID == record.LeaderAuthorityNodeID &&
			current.BaseCommitWatermark == record.BaseCommitWatermark {
			replicaState, err := s.getWorkspaceReplicaStateTx(ctx, tx, record.WorkspaceID, record.Scope, record.ReplicaAuthorityNodeID)
			if err != nil {
				return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, err
			}
			return current, replicaState, nil
		}
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, errors.New("installed replica base-state requires a new install cycle before replacement")
	}
	if current.AuthorityTerm != record.AuthorityTerm {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("replica install authority_term %d does not match pending install term %d", record.AuthorityTerm, current.AuthorityTerm)
	}
	if current.LeaderAuthorityNodeID != record.LeaderAuthorityNodeID {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, errors.New("replica install leader_authority_node_id does not match pending install")
	}
	if current.BaseCommitWatermark != record.BaseCommitWatermark {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, errors.New("replica install base_commit_watermark does not match pending install")
	}
	record.InstallStartedAt = current.InstallStartedAt
	if err := s.upsertWorkspaceReplicaInstallStateTx(ctx, tx, record); err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	replicaReason := "base-state install completed; incremental apply pending"
	if record.InstallReason != "" {
		replicaReason = replicaReason + ": " + record.InstallReason
	}
	replicaState, err := s.upsertWorkspaceReplicaStateTx(ctx, tx, WorkspaceReplicaStateRecord{
		WorkspaceID:            record.WorkspaceID,
		Scope:                  record.Scope,
		ReplicaAuthorityNodeID: record.ReplicaAuthorityNodeID,
		ReplicaRole:            WorkspaceReplicaRoleFollower,
		MembershipState:        WorkspaceReplicaMembershipCatchingUp,
		LeaderAuthorityNodeID:  record.LeaderAuthorityNodeID,
		AuthorityTerm:          record.AuthorityTerm,
		CommitWatermark:        record.BaseCommitWatermark,
		AppliedWatermark:       record.BaseCommitWatermark,
		LastFetchAt:            record.InstallCompletedAt,
		LastApplyAt:            record.InstallCompletedAt,
		MembershipReason:       replicaReason,
	})
	if err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	installRecord, err := s.getWorkspaceReplicaInstallStateTx(ctx, tx, record.WorkspaceID, record.Scope, record.ReplicaAuthorityNodeID)
	if err != nil {
		return WorkspaceReplicaInstallStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	return installRecord, replicaState, nil
}

func validateWorkspaceReplicaInstallAgainstAuthority(authority WorkspaceAuthorityRecord, record WorkspaceReplicaInstallStateRecord) error {
	if authority.Status != WorkspaceAuthorityStatusActive {
		return fmt.Errorf("workspace replica install requires active authority, got %s", authority.Status)
	}
	if strings.TrimSpace(authority.HolderAuthorityNodeID) != record.LeaderAuthorityNodeID {
		return fmt.Errorf("workspace replica install leader %q does not match current holder %q", record.LeaderAuthorityNodeID, authority.HolderAuthorityNodeID)
	}
	if authority.Term != record.AuthorityTerm {
		return fmt.Errorf("workspace replica install term %d does not match current authority term %d", record.AuthorityTerm, authority.Term)
	}
	if record.BaseCommitWatermark > authority.CommitWatermark {
		return fmt.Errorf("workspace replica install base_commit_watermark %d exceeds current authority commit_watermark %d", record.BaseCommitWatermark, authority.CommitWatermark)
	}
	return nil
}

func validateWorkspaceReplicaInstallProgress(current, next WorkspaceReplicaInstallStateRecord) error {
	if next.AuthorityTerm < current.AuthorityTerm {
		return fmt.Errorf("replica install authority_term %d regresses existing install term %d", next.AuthorityTerm, current.AuthorityTerm)
	}
	if next.BaseCommitWatermark < current.BaseCommitWatermark {
		return fmt.Errorf("replica install base_commit_watermark %d regresses existing install watermark %d", next.BaseCommitWatermark, current.BaseCommitWatermark)
	}
	if current.InstallStatus == WorkspaceReplicaInstallInstalled && next.InstallStatus == WorkspaceReplicaInstallPending {
		return errors.New("replica install cannot regress from INSTALLED to PENDING")
	}
	return nil
}

func (s *Store) upsertWorkspaceReplicaInstallStateTx(ctx context.Context, tx *sql.Tx, record WorkspaceReplicaInstallStateRecord) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO workspace_replica_install_state(
	workspace_id, scope, replica_authority_node_id, leader_authority_node_id,
	authority_term, install_status, base_commit_watermark, install_started_at,
	install_completed_at, install_reason, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workspace_id, scope, replica_authority_node_id) DO UPDATE SET
	leader_authority_node_id = excluded.leader_authority_node_id,
	authority_term = excluded.authority_term,
	install_status = excluded.install_status,
	base_commit_watermark = excluded.base_commit_watermark,
	install_started_at = excluded.install_started_at,
	install_completed_at = excluded.install_completed_at,
	install_reason = excluded.install_reason,
	updated_at = excluded.updated_at`,
		record.WorkspaceID,
		record.Scope,
		record.ReplicaAuthorityNodeID,
		record.LeaderAuthorityNodeID,
		record.AuthorityTerm,
		record.InstallStatus,
		record.BaseCommitWatermark,
		record.InstallStartedAt,
		record.InstallCompletedAt,
		record.InstallReason,
		record.UpdatedAt,
	); err != nil {
		return fmt.Errorf("upsert workspace replica install state: %w", err)
	}
	return nil
}

func (s *Store) getWorkspaceReplicaInstallStateTx(ctx context.Context, tx *sql.Tx, workspaceID, scope, replicaAuthorityNodeID string) (WorkspaceReplicaInstallStateRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT workspace_id, scope, replica_authority_node_id, leader_authority_node_id,
       authority_term, install_status, base_commit_watermark, install_started_at,
       install_completed_at, install_reason, updated_at
  FROM workspace_replica_install_state
 WHERE workspace_id = ? AND scope = ? AND replica_authority_node_id = ?`,
		workspaceID,
		scope,
		replicaAuthorityNodeID,
	)
	var record WorkspaceReplicaInstallStateRecord
	if err := scanWorkspaceReplicaInstallState(row, &record); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaInstallStateRecord{}, err
		}
		return WorkspaceReplicaInstallStateRecord{}, fmt.Errorf("get workspace replica install state tx: %w", err)
	}
	return record, nil
}
