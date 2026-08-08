package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type WorkspaceReplicaFreshnessRecord struct {
	WorkspaceID                      string                          `json:"workspace_id"`
	Scope                            string                          `json:"scope"`
	ReplicaAuthorityNodeID           string                          `json:"replica_authority_node_id"`
	ReplicaRole                      WorkspaceReplicaRole            `json:"replica_role"`
	LeaderAuthorityNodeID            string                          `json:"leader_authority_node_id,omitempty"`
	AuthorityTerm                    int64                           `json:"authority_term"`
	MembershipState                  WorkspaceReplicaMembershipState `json:"membership_state"`
	LeaderJournalHeadCommitWatermark int64                           `json:"leader_journal_head_commit_watermark"`
	CommitWatermark                  int64                           `json:"commit_watermark"`
	AppliedWatermark                 int64                           `json:"applied_watermark"`
	ExportedHeadCommitWatermark      int64                           `json:"exported_head_commit_watermark"`
	FetchedThroughCommitWatermark    int64                           `json:"fetched_through_commit_watermark"`
	AcknowledgedCommitWatermark      int64                           `json:"acknowledged_commit_watermark"`
	AttemptedThroughCommitWatermark  int64                           `json:"attempted_through_commit_watermark"`
	ExportLag                        int64                           `json:"export_lag"`
	FetchLag                         int64                           `json:"fetch_lag"`
	AckLag                           int64                           `json:"ack_lag"`
	ApplyLag                         int64                           `json:"apply_lag"`
	LastFetchAt                      string                          `json:"last_fetch_at,omitempty"`
	LastAcknowledgedAt               string                          `json:"last_acknowledged_at,omitempty"`
	LastApplyAt                      string                          `json:"last_apply_at,omitempty"`
	ApplyStatus                      WorkspaceReplicaApplyStatus     `json:"apply_status"`
	FailureCount                     int                             `json:"failure_count"`
	NextRetryAt                      string                          `json:"next_retry_at,omitempty"`
	DeadLetteredAt                   string                          `json:"dead_lettered_at,omitempty"`
}

func (s *Store) GetWorkspaceReplicaFreshness(ctx context.Context, workspaceID, scope, replicaAuthorityNodeID string) (WorkspaceReplicaFreshnessRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return WorkspaceReplicaFreshnessRecord{}, errors.New("workspace_id is required")
	}
	scope = normalizeWorkspaceAuthorityScope(scope)
	replicaAuthorityNodeID = strings.TrimSpace(replicaAuthorityNodeID)
	if replicaAuthorityNodeID == "" {
		return WorkspaceReplicaFreshnessRecord{}, errors.New("replica_authority_node_id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkspaceReplicaFreshnessRecord{}, fmt.Errorf("begin workspace replica freshness tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	replicaState, err := s.getWorkspaceReplicaStateTx(ctx, tx, workspaceID, scope, replicaAuthorityNodeID)
	if err != nil {
		return WorkspaceReplicaFreshnessRecord{}, err
	}
	record, err := s.buildWorkspaceReplicaFreshnessTx(ctx, tx, replicaState)
	if err != nil {
		return WorkspaceReplicaFreshnessRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceReplicaFreshnessRecord{}, fmt.Errorf("commit workspace replica freshness tx: %w", err)
	}
	return record, nil
}

func (s *Store) ListWorkspaceReplicaFreshness(ctx context.Context, workspaceID, scope string) ([]WorkspaceReplicaFreshnessRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	scope = normalizeWorkspaceAuthorityScope(scope)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin workspace replica freshness list tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
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
		return nil, fmt.Errorf("list workspace replica freshness states: %w", err)
	}
	defer rows.Close()
	var out []WorkspaceReplicaFreshnessRecord
	for rows.Next() {
		var replicaState WorkspaceReplicaStateRecord
		if err := scanWorkspaceReplicaState(rows, &replicaState); err != nil {
			return nil, fmt.Errorf("scan workspace replica freshness state: %w", err)
		}
		record, err := s.buildWorkspaceReplicaFreshnessTx(ctx, tx, replicaState)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace replica freshness states: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit workspace replica freshness list tx: %w", err)
	}
	return out, nil
}

func (s *Store) buildWorkspaceReplicaFreshnessTx(ctx context.Context, tx *sql.Tx, replicaState WorkspaceReplicaStateRecord) (WorkspaceReplicaFreshnessRecord, error) {
	leaderHead, err := s.workspaceRuntimeJournalHeadTx(ctx, tx, replicaState.WorkspaceID)
	if err != nil {
		return WorkspaceReplicaFreshnessRecord{}, fmt.Errorf("query workspace runtime journal head for replica freshness: %w", err)
	}
	transportState, err := s.getWorkspaceReplicaTransportStateTx(ctx, tx, replicaState.WorkspaceID, replicaState.Scope, replicaState.ReplicaAuthorityNodeID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return WorkspaceReplicaFreshnessRecord{}, err
	}
	applyState, err := s.getWorkspaceReplicaApplyStateTx(ctx, tx, replicaState.WorkspaceID, replicaState.Scope, replicaState.ReplicaAuthorityNodeID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return WorkspaceReplicaFreshnessRecord{}, err
	}
	record := WorkspaceReplicaFreshnessRecord{
		WorkspaceID:                      replicaState.WorkspaceID,
		Scope:                            replicaState.Scope,
		ReplicaAuthorityNodeID:           replicaState.ReplicaAuthorityNodeID,
		ReplicaRole:                      replicaState.ReplicaRole,
		LeaderAuthorityNodeID:            replicaState.LeaderAuthorityNodeID,
		AuthorityTerm:                    replicaState.AuthorityTerm,
		MembershipState:                  replicaState.MembershipState,
		LeaderJournalHeadCommitWatermark: leaderHead,
		CommitWatermark:                  replicaState.CommitWatermark,
		AppliedWatermark:                 replicaState.AppliedWatermark,
		LastApplyAt:                      replicaState.LastApplyAt,
		ApplyStatus:                      WorkspaceReplicaApplyStatusIdle,
	}
	if err == nil {
		record.ApplyStatus = applyState.ApplyStatus
		record.ExportedHeadCommitWatermark = maxInt64(record.ExportedHeadCommitWatermark, applyState.ExportedHeadCommitWatermark)
		record.AttemptedThroughCommitWatermark = applyState.AttemptedThroughCommitWatermark
		record.FailureCount = applyState.FailureCount
		record.NextRetryAt = applyState.NextRetryAt
		record.DeadLetteredAt = applyState.DeadLetteredAt
	}
	if transportState.WorkspaceID != "" {
		record.ExportedHeadCommitWatermark = transportState.ExportedHeadCommitWatermark
		record.FetchedThroughCommitWatermark = transportState.FetchedThroughCommitWatermark
		record.AcknowledgedCommitWatermark = transportState.AcknowledgedCommitWatermark
		record.LastFetchAt = transportState.LastFetchAt
		record.LastAcknowledgedAt = transportState.LastAcknowledgedAt
	}
	record.ExportLag = nonNegativeLag(record.LeaderJournalHeadCommitWatermark, record.ExportedHeadCommitWatermark)
	record.FetchLag = nonNegativeLag(record.ExportedHeadCommitWatermark, record.FetchedThroughCommitWatermark)
	record.AckLag = nonNegativeLag(record.ExportedHeadCommitWatermark, record.AcknowledgedCommitWatermark)
	record.ApplyLag = nonNegativeLag(record.ExportedHeadCommitWatermark, record.AppliedWatermark)
	return record, nil
}

func nonNegativeLag(head, current int64) int64 {
	if head <= current {
		return 0
	}
	return head - current
}
