package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type WorkspaceReplicaApplyStatus string

const (
	WorkspaceReplicaApplyStatusIdle         WorkspaceReplicaApplyStatus = "IDLE"
	WorkspaceReplicaApplyStatusRetryPending WorkspaceReplicaApplyStatus = "RETRY_PENDING"
	WorkspaceReplicaApplyStatusDeadLetter   WorkspaceReplicaApplyStatus = "DEAD_LETTER"
)

const (
	workspaceReplicaApplyMaxFailures = 3
	workspaceReplicaApplyRetryBase   = 30 * time.Second
	workspaceReplicaApplyRetryMax    = 5 * time.Minute
)

type WorkspaceReplicaApplyStateRecord struct {
	WorkspaceID                     string                      `json:"workspace_id"`
	Scope                           string                      `json:"scope"`
	ReplicaAuthorityNodeID          string                      `json:"replica_authority_node_id"`
	LeaderAuthorityNodeID           string                      `json:"leader_authority_node_id"`
	AuthorityTerm                   int64                       `json:"authority_term"`
	ApplyStatus                     WorkspaceReplicaApplyStatus `json:"apply_status"`
	ExportedHeadCommitWatermark     int64                       `json:"exported_head_commit_watermark"`
	AttemptedThroughCommitWatermark int64                       `json:"attempted_through_commit_watermark"`
	FailureCount                    int                         `json:"failure_count"`
	LastFailureAt                   string                      `json:"last_failure_at,omitempty"`
	LastFailureReason               string                      `json:"last_failure_reason,omitempty"`
	NextRetryAt                     string                      `json:"next_retry_at,omitempty"`
	DeadLetteredAt                  string                      `json:"dead_lettered_at,omitempty"`
	UpdatedAt                       string                      `json:"updated_at"`
}

type WorkspaceReplicaApplyFailureInput struct {
	WorkspaceID                     string
	Scope                           string
	ReplicaAuthorityNodeID          string
	LeaderAuthorityNodeID           string
	AuthorityTerm                   int64
	ExportedHeadCommitWatermark     int64
	AttemptedThroughCommitWatermark int64
	FailureAt                       string
	FailureReason                   string
	Retryable                       bool
}

func normalizeWorkspaceReplicaApplyStatus(status WorkspaceReplicaApplyStatus) (WorkspaceReplicaApplyStatus, error) {
	switch WorkspaceReplicaApplyStatus(strings.ToUpper(strings.TrimSpace(string(status)))) {
	case WorkspaceReplicaApplyStatusIdle:
		return WorkspaceReplicaApplyStatusIdle, nil
	case WorkspaceReplicaApplyStatusRetryPending:
		return WorkspaceReplicaApplyStatusRetryPending, nil
	case WorkspaceReplicaApplyStatusDeadLetter:
		return WorkspaceReplicaApplyStatusDeadLetter, nil
	default:
		return "", fmt.Errorf("unsupported apply_status %q", status)
	}
}

func normalizeWorkspaceReplicaApplyStateRecord(input WorkspaceReplicaApplyStateRecord) (WorkspaceReplicaApplyStateRecord, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("workspace_id is required")
	}
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	input.ReplicaAuthorityNodeID = strings.TrimSpace(input.ReplicaAuthorityNodeID)
	if input.ReplicaAuthorityNodeID == "" {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("replica_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("replica_authority_node_id is invalid: %w", err)
	}
	input.LeaderAuthorityNodeID = strings.TrimSpace(input.LeaderAuthorityNodeID)
	if input.LeaderAuthorityNodeID == "" {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("leader_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.LeaderAuthorityNodeID); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("leader_authority_node_id is invalid: %w", err)
	}
	if input.LeaderAuthorityNodeID == input.ReplicaAuthorityNodeID {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("leader_authority_node_id must differ from replica_authority_node_id")
	}
	if input.AuthorityTerm <= 0 {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("authority_term must be > 0")
	}
	status, err := normalizeWorkspaceReplicaApplyStatus(input.ApplyStatus)
	if err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	input.ApplyStatus = status
	if input.ExportedHeadCommitWatermark < 0 {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("exported_head_commit_watermark must be >= 0")
	}
	if input.AttemptedThroughCommitWatermark < 0 {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("attempted_through_commit_watermark must be >= 0")
	}
	if input.AttemptedThroughCommitWatermark > input.ExportedHeadCommitWatermark {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("attempted_through_commit_watermark cannot exceed exported_head_commit_watermark")
	}
	if input.FailureCount < 0 {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("failure_count must be >= 0")
	}
	input.LastFailureAt = strings.TrimSpace(input.LastFailureAt)
	input.LastFailureReason = strings.TrimSpace(input.LastFailureReason)
	input.NextRetryAt = strings.TrimSpace(input.NextRetryAt)
	input.DeadLetteredAt = strings.TrimSpace(input.DeadLetteredAt)
	if _, err := parseAuthorityTimestamp("last_failure_at", input.LastFailureAt, false); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	if _, err := parseAuthorityTimestamp("next_retry_at", input.NextRetryAt, false); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	if _, err := parseAuthorityTimestamp("dead_lettered_at", input.DeadLetteredAt, false); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	input.UpdatedAt = strings.TrimSpace(input.UpdatedAt)
	if input.UpdatedAt == "" {
		input.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if _, err := parseAuthorityTimestamp("updated_at", input.UpdatedAt, true); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	switch input.ApplyStatus {
	case WorkspaceReplicaApplyStatusIdle:
		input.FailureCount = 0
		input.LastFailureAt = ""
		input.LastFailureReason = ""
		input.NextRetryAt = ""
		input.DeadLetteredAt = ""
	case WorkspaceReplicaApplyStatusRetryPending:
		if input.FailureCount <= 0 {
			return WorkspaceReplicaApplyStateRecord{}, errors.New("retry_pending apply_state requires failure_count > 0")
		}
		if input.LastFailureAt == "" || input.LastFailureReason == "" || input.NextRetryAt == "" {
			return WorkspaceReplicaApplyStateRecord{}, errors.New("retry_pending apply_state requires last_failure_at, last_failure_reason, and next_retry_at")
		}
		if input.DeadLetteredAt != "" {
			return WorkspaceReplicaApplyStateRecord{}, errors.New("retry_pending apply_state cannot set dead_lettered_at")
		}
	case WorkspaceReplicaApplyStatusDeadLetter:
		if input.FailureCount <= 0 {
			return WorkspaceReplicaApplyStateRecord{}, errors.New("dead_letter apply_state requires failure_count > 0")
		}
		if input.LastFailureAt == "" || input.LastFailureReason == "" || input.DeadLetteredAt == "" {
			return WorkspaceReplicaApplyStateRecord{}, errors.New("dead_letter apply_state requires last_failure_at, last_failure_reason, and dead_lettered_at")
		}
		input.NextRetryAt = ""
	}
	return input, nil
}

func normalizeWorkspaceReplicaApplyFailureInput(input WorkspaceReplicaApplyFailureInput) (WorkspaceReplicaApplyFailureInput, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return WorkspaceReplicaApplyFailureInput{}, errors.New("workspace_id is required")
	}
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	input.ReplicaAuthorityNodeID = strings.TrimSpace(input.ReplicaAuthorityNodeID)
	if input.ReplicaAuthorityNodeID == "" {
		return WorkspaceReplicaApplyFailureInput{}, errors.New("replica_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaApplyFailureInput{}, fmt.Errorf("replica_authority_node_id is invalid: %w", err)
	}
	input.LeaderAuthorityNodeID = strings.TrimSpace(input.LeaderAuthorityNodeID)
	if input.LeaderAuthorityNodeID == "" {
		return WorkspaceReplicaApplyFailureInput{}, errors.New("leader_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.LeaderAuthorityNodeID); err != nil {
		return WorkspaceReplicaApplyFailureInput{}, fmt.Errorf("leader_authority_node_id is invalid: %w", err)
	}
	if input.LeaderAuthorityNodeID == input.ReplicaAuthorityNodeID {
		return WorkspaceReplicaApplyFailureInput{}, errors.New("leader_authority_node_id must differ from replica_authority_node_id")
	}
	if input.AuthorityTerm <= 0 {
		return WorkspaceReplicaApplyFailureInput{}, errors.New("authority_term must be > 0")
	}
	if input.ExportedHeadCommitWatermark < 0 {
		return WorkspaceReplicaApplyFailureInput{}, errors.New("exported_head_commit_watermark must be >= 0")
	}
	if input.AttemptedThroughCommitWatermark < 0 {
		return WorkspaceReplicaApplyFailureInput{}, errors.New("attempted_through_commit_watermark must be >= 0")
	}
	if input.AttemptedThroughCommitWatermark > input.ExportedHeadCommitWatermark {
		return WorkspaceReplicaApplyFailureInput{}, errors.New("attempted_through_commit_watermark cannot exceed exported_head_commit_watermark")
	}
	input.FailureReason = strings.TrimSpace(input.FailureReason)
	if input.FailureReason == "" {
		return WorkspaceReplicaApplyFailureInput{}, errors.New("failure_reason is required")
	}
	input.FailureAt = strings.TrimSpace(input.FailureAt)
	if input.FailureAt == "" {
		input.FailureAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if _, err := parseAuthorityTimestamp("failure_at", input.FailureAt, true); err != nil {
		return WorkspaceReplicaApplyFailureInput{}, err
	}
	return input, nil
}

func scanWorkspaceReplicaApplyState(scanner interface{ Scan(dest ...any) error }, record *WorkspaceReplicaApplyStateRecord) error {
	return scanner.Scan(
		&record.WorkspaceID,
		&record.Scope,
		&record.ReplicaAuthorityNodeID,
		&record.LeaderAuthorityNodeID,
		&record.AuthorityTerm,
		&record.ApplyStatus,
		&record.ExportedHeadCommitWatermark,
		&record.AttemptedThroughCommitWatermark,
		&record.FailureCount,
		&record.LastFailureAt,
		&record.LastFailureReason,
		&record.NextRetryAt,
		&record.DeadLetteredAt,
		&record.UpdatedAt,
	)
}

func (s *Store) GetWorkspaceReplicaApplyState(ctx context.Context, workspaceID, scope, replicaAuthorityNodeID string) (WorkspaceReplicaApplyStateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("workspace_id is required")
	}
	scope = normalizeWorkspaceAuthorityScope(scope)
	replicaAuthorityNodeID = strings.TrimSpace(replicaAuthorityNodeID)
	if replicaAuthorityNodeID == "" {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("replica_authority_node_id is required")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT workspace_id, scope, replica_authority_node_id, leader_authority_node_id,
       authority_term, apply_status, exported_head_commit_watermark,
       attempted_through_commit_watermark, failure_count, last_failure_at,
       last_failure_reason, next_retry_at, dead_lettered_at, updated_at
  FROM workspace_replica_apply_state
 WHERE workspace_id = ? AND scope = ? AND replica_authority_node_id = ?`,
		workspaceID,
		scope,
		replicaAuthorityNodeID,
	)
	var record WorkspaceReplicaApplyStateRecord
	if err := scanWorkspaceReplicaApplyState(row, &record); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaApplyStateRecord{}, err
		}
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("get workspace replica apply state: %w", err)
	}
	return record, nil
}

func (s *Store) RecordWorkspaceReplicaApplyFailure(ctx context.Context, input WorkspaceReplicaApplyFailureInput) (WorkspaceReplicaApplyStateRecord, error) {
	input, err := normalizeWorkspaceReplicaApplyFailureInput(input)
	if err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("begin workspace replica apply failure tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, err := s.recordWorkspaceReplicaApplyFailureTx(ctx, tx, input)
	if err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("commit workspace replica apply failure tx: %w", err)
	}
	return record, nil
}

func (s *Store) recordWorkspaceReplicaApplyFailureTx(ctx context.Context, tx *sql.Tx, input WorkspaceReplicaApplyFailureInput) (WorkspaceReplicaApplyStateRecord, error) {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, input.WorkspaceID); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, input.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, input.LeaderAuthorityNodeID); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	installState, err := s.getWorkspaceReplicaInstallStateTx(ctx, tx, input.WorkspaceID, input.Scope, input.ReplicaAuthorityNodeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaApplyStateRecord{}, errors.New("replica apply failure requires installed base-state")
		}
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	if installState.InstallStatus != WorkspaceReplicaInstallInstalled {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("replica apply failure requires completed base-state install")
	}
	if installState.LeaderAuthorityNodeID != input.LeaderAuthorityNodeID || installState.AuthorityTerm != input.AuthorityTerm {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("replica apply failure does not match installed transport line")
	}
	authority, err := s.getWorkspaceAuthorityTx(ctx, tx, input.WorkspaceID, input.Scope)
	if err != nil {
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("get workspace authority for replica apply failure: %w", err)
	}
	if authority.Status != WorkspaceAuthorityStatusActive {
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("workspace replica apply failure requires active authority, got %s", authority.Status)
	}
	if authority.HolderAuthorityNodeID != input.LeaderAuthorityNodeID || authority.Term != input.AuthorityTerm {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("replica apply failure does not match current authority line")
	}
	journalHead, err := s.workspaceRuntimeJournalHeadTx(ctx, tx, input.WorkspaceID)
	if err != nil {
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("query workspace runtime journal head for replica apply failure: %w", err)
	}
	if input.ExportedHeadCommitWatermark > journalHead {
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("replica apply failure exported_head_commit_watermark %d exceeds current journal head %d", input.ExportedHeadCommitWatermark, journalHead)
	}
	replicaState, err := s.getWorkspaceReplicaStateTx(ctx, tx, input.WorkspaceID, input.Scope, input.ReplicaAuthorityNodeID)
	if err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	if replicaState.ReplicaRole != WorkspaceReplicaRoleFollower || replicaState.LeaderAuthorityNodeID != input.LeaderAuthorityNodeID || replicaState.AuthorityTerm != input.AuthorityTerm {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("replica apply failure does not match follower replica line")
	}

	current, err := s.getWorkspaceReplicaApplyStateTx(ctx, tx, input.WorkspaceID, input.Scope, input.ReplicaAuthorityNodeID)
	hasCurrent := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	if hasCurrent && current.AuthorityTerm > input.AuthorityTerm {
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("replica apply failure term %d regresses existing apply-state term %d", input.AuthorityTerm, current.AuthorityTerm)
	}
	nextFailureCount := 1
	exportedHead := input.ExportedHeadCommitWatermark
	attemptedThrough := input.AttemptedThroughCommitWatermark
	if hasCurrent && current.AuthorityTerm == input.AuthorityTerm {
		nextFailureCount = current.FailureCount + 1
		exportedHead = maxInt64(current.ExportedHeadCommitWatermark, exportedHead)
		attemptedThrough = maxInt64(current.AttemptedThroughCommitWatermark, attemptedThrough)
	}

	nextState := WorkspaceReplicaApplyStateRecord{
		WorkspaceID:                     input.WorkspaceID,
		Scope:                           input.Scope,
		ReplicaAuthorityNodeID:          input.ReplicaAuthorityNodeID,
		LeaderAuthorityNodeID:           input.LeaderAuthorityNodeID,
		AuthorityTerm:                   input.AuthorityTerm,
		ExportedHeadCommitWatermark:     exportedHead,
		AttemptedThroughCommitWatermark: attemptedThrough,
		FailureCount:                    nextFailureCount,
		LastFailureAt:                   input.FailureAt,
		LastFailureReason:               input.FailureReason,
		UpdatedAt:                       input.FailureAt,
	}
	if !input.Retryable || nextFailureCount >= workspaceReplicaApplyMaxFailures {
		nextState.ApplyStatus = WorkspaceReplicaApplyStatusDeadLetter
		nextState.DeadLetteredAt = input.FailureAt
	} else {
		nextState.ApplyStatus = WorkspaceReplicaApplyStatusRetryPending
		nextState.NextRetryAt = workspaceReplicaApplyRetryAt(input.FailureAt, nextFailureCount)
	}
	nextState, err = normalizeWorkspaceReplicaApplyStateRecord(nextState)
	if err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	return s.upsertWorkspaceReplicaApplyStateTx(ctx, tx, nextState)
}

func workspaceReplicaApplyRetryAt(failureAt string, failureCount int) string {
	when, err := parseAuthorityTimestamp("failure_at", failureAt, true)
	if err != nil {
		return ""
	}
	delay := time.Duration(maxInt(failureCount, 1)) * workspaceReplicaApplyRetryBase
	if delay > workspaceReplicaApplyRetryMax {
		delay = workspaceReplicaApplyRetryMax
	}
	return when.Add(delay).UTC().Format(time.RFC3339Nano)
}

func (s *Store) gateWorkspaceReplicaApplyStateTx(ctx context.Context, tx *sql.Tx, workspaceID, scope, replicaAuthorityNodeID, leaderAuthorityNodeID string, authorityTerm int64, referenceAt string) (WorkspaceReplicaApplyStateRecord, error) {
	record, err := s.getWorkspaceReplicaApplyStateTx(ctx, tx, workspaceID, scope, replicaAuthorityNodeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaApplyStateRecord{}, nil
		}
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	if record.AuthorityTerm > authorityTerm {
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("replica apply term %d regresses existing apply-state term %d", authorityTerm, record.AuthorityTerm)
	}
	if record.AuthorityTerm < authorityTerm {
		return WorkspaceReplicaApplyStateRecord{}, nil
	}
	if record.LeaderAuthorityNodeID != leaderAuthorityNodeID {
		return WorkspaceReplicaApplyStateRecord{}, errors.New("replica apply leader does not match existing apply-state line")
	}
	switch record.ApplyStatus {
	case WorkspaceReplicaApplyStatusIdle:
		return record, nil
	case WorkspaceReplicaApplyStatusDeadLetter:
		return WorkspaceReplicaApplyStateRecord{}, errors.New("replica apply is dead-lettered for the current transport line")
	case WorkspaceReplicaApplyStatusRetryPending:
		referenceTime, err := parseAuthorityTimestamp("reference_at", referenceAt, true)
		if err != nil {
			return WorkspaceReplicaApplyStateRecord{}, err
		}
		nextRetryAt, err := parseAuthorityTimestamp("next_retry_at", record.NextRetryAt, true)
		if err != nil {
			return WorkspaceReplicaApplyStateRecord{}, err
		}
		if referenceTime.Before(nextRetryAt) {
			return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("replica apply retry window remains closed until %s", record.NextRetryAt)
		}
		return record, nil
	default:
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("unsupported replica apply status %q", record.ApplyStatus)
	}
}

func (s *Store) clearWorkspaceReplicaApplyStateTx(ctx context.Context, tx *sql.Tx, workspaceID, scope, replicaAuthorityNodeID, leaderAuthorityNodeID string, authorityTerm, exportedHeadCommitWatermark, attemptedThroughCommitWatermark int64, updatedAt string) (WorkspaceReplicaApplyStateRecord, error) {
	current, err := s.getWorkspaceReplicaApplyStateTx(ctx, tx, workspaceID, scope, replicaAuthorityNodeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			current = WorkspaceReplicaApplyStateRecord{}
		} else {
			return WorkspaceReplicaApplyStateRecord{}, err
		}
	}
	record := WorkspaceReplicaApplyStateRecord{
		WorkspaceID:                     workspaceID,
		Scope:                           scope,
		ReplicaAuthorityNodeID:          replicaAuthorityNodeID,
		LeaderAuthorityNodeID:           leaderAuthorityNodeID,
		AuthorityTerm:                   authorityTerm,
		ApplyStatus:                     WorkspaceReplicaApplyStatusIdle,
		ExportedHeadCommitWatermark:     maxInt64(current.ExportedHeadCommitWatermark, exportedHeadCommitWatermark),
		AttemptedThroughCommitWatermark: maxInt64(current.AttemptedThroughCommitWatermark, attemptedThroughCommitWatermark),
		UpdatedAt:                       updatedAt,
	}
	record, err = normalizeWorkspaceReplicaApplyStateRecord(record)
	if err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	return s.upsertWorkspaceReplicaApplyStateTx(ctx, tx, record)
}

func (s *Store) upsertWorkspaceReplicaApplyStateTx(ctx context.Context, tx *sql.Tx, record WorkspaceReplicaApplyStateRecord) (WorkspaceReplicaApplyStateRecord, error) {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, record.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, record.LeaderAuthorityNodeID); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, err
	}
	current, err := s.getWorkspaceReplicaApplyStateTx(ctx, tx, record.WorkspaceID, record.Scope, record.ReplicaAuthorityNodeID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return WorkspaceReplicaApplyStateRecord{}, err
	default:
		if record.AuthorityTerm < current.AuthorityTerm {
			return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("authority_term %d regresses existing apply-state term %d", record.AuthorityTerm, current.AuthorityTerm)
		}
		if record.AuthorityTerm == current.AuthorityTerm {
			if record.ExportedHeadCommitWatermark < current.ExportedHeadCommitWatermark {
				return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("exported_head_commit_watermark %d regresses existing apply-state watermark %d", record.ExportedHeadCommitWatermark, current.ExportedHeadCommitWatermark)
			}
			if record.AttemptedThroughCommitWatermark < current.AttemptedThroughCommitWatermark {
				return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("attempted_through_commit_watermark %d regresses existing apply-state watermark %d", record.AttemptedThroughCommitWatermark, current.AttemptedThroughCommitWatermark)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO workspace_replica_apply_state(
	workspace_id, scope, replica_authority_node_id, leader_authority_node_id,
	authority_term, apply_status, exported_head_commit_watermark,
	attempted_through_commit_watermark, failure_count, last_failure_at,
	last_failure_reason, next_retry_at, dead_lettered_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workspace_id, scope, replica_authority_node_id) DO UPDATE SET
	leader_authority_node_id = excluded.leader_authority_node_id,
	authority_term = excluded.authority_term,
	apply_status = excluded.apply_status,
	exported_head_commit_watermark = excluded.exported_head_commit_watermark,
	attempted_through_commit_watermark = excluded.attempted_through_commit_watermark,
	failure_count = excluded.failure_count,
	last_failure_at = excluded.last_failure_at,
	last_failure_reason = excluded.last_failure_reason,
	next_retry_at = excluded.next_retry_at,
	dead_lettered_at = excluded.dead_lettered_at,
	updated_at = excluded.updated_at`,
		record.WorkspaceID,
		record.Scope,
		record.ReplicaAuthorityNodeID,
		record.LeaderAuthorityNodeID,
		record.AuthorityTerm,
		record.ApplyStatus,
		record.ExportedHeadCommitWatermark,
		record.AttemptedThroughCommitWatermark,
		record.FailureCount,
		record.LastFailureAt,
		record.LastFailureReason,
		record.NextRetryAt,
		record.DeadLetteredAt,
		record.UpdatedAt,
	); err != nil {
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("upsert workspace replica apply state: %w", err)
	}
	return s.getWorkspaceReplicaApplyStateTx(ctx, tx, record.WorkspaceID, record.Scope, record.ReplicaAuthorityNodeID)
}

func (s *Store) getWorkspaceReplicaApplyStateTx(ctx context.Context, tx *sql.Tx, workspaceID, scope, replicaAuthorityNodeID string) (WorkspaceReplicaApplyStateRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT workspace_id, scope, replica_authority_node_id, leader_authority_node_id,
       authority_term, apply_status, exported_head_commit_watermark,
       attempted_through_commit_watermark, failure_count, last_failure_at,
       last_failure_reason, next_retry_at, dead_lettered_at, updated_at
  FROM workspace_replica_apply_state
 WHERE workspace_id = ? AND scope = ? AND replica_authority_node_id = ?`,
		workspaceID,
		scope,
		replicaAuthorityNodeID,
	)
	var record WorkspaceReplicaApplyStateRecord
	if err := scanWorkspaceReplicaApplyState(row, &record); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaApplyStateRecord{}, err
		}
		return WorkspaceReplicaApplyStateRecord{}, fmt.Errorf("get workspace replica apply state tx: %w", err)
	}
	return record, nil
}
