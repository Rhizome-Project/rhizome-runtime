package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type WorkspaceReplicaTransportStateRecord struct {
	WorkspaceID                   string `json:"workspace_id"`
	Scope                         string `json:"scope"`
	ReplicaAuthorityNodeID        string `json:"replica_authority_node_id"`
	LeaderAuthorityNodeID         string `json:"leader_authority_node_id"`
	AuthorityTerm                 int64  `json:"authority_term"`
	ExportedHeadCommitWatermark   int64  `json:"exported_head_commit_watermark"`
	FetchedThroughCommitWatermark int64  `json:"fetched_through_commit_watermark"`
	AcknowledgedCommitWatermark   int64  `json:"acknowledged_commit_watermark"`
	LastFetchAt                   string `json:"last_fetch_at,omitempty"`
	LastAcknowledgedAt            string `json:"last_acknowledged_at,omitempty"`
	UpdatedAt                     string `json:"updated_at"`
}

type WorkspaceReplicaApplyBatchInput struct {
	WorkspaceID                 string
	Scope                       string
	ReplicaAuthorityNodeID      string
	LeaderAuthorityNodeID       string
	AuthorityTerm               int64
	ExportedHeadCommitWatermark int64
	EventIDs                    []string
	FetchedAt                   string
	AcknowledgedAt              string
	AppliedAt                   string
	ApplyReason                 string
}

func normalizeWorkspaceReplicaTransportStateRecord(input WorkspaceReplicaTransportStateRecord) (WorkspaceReplicaTransportStateRecord, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("workspace_id is required")
	}
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	input.ReplicaAuthorityNodeID = strings.TrimSpace(input.ReplicaAuthorityNodeID)
	if input.ReplicaAuthorityNodeID == "" {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("replica_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, fmt.Errorf("replica_authority_node_id is invalid: %w", err)
	}
	input.LeaderAuthorityNodeID = strings.TrimSpace(input.LeaderAuthorityNodeID)
	if input.LeaderAuthorityNodeID == "" {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("leader_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.LeaderAuthorityNodeID); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, fmt.Errorf("leader_authority_node_id is invalid: %w", err)
	}
	if input.LeaderAuthorityNodeID == input.ReplicaAuthorityNodeID {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("leader_authority_node_id must differ from replica_authority_node_id")
	}
	if input.AuthorityTerm <= 0 {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("authority_term must be > 0")
	}
	if input.ExportedHeadCommitWatermark < 0 {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("exported_head_commit_watermark must be >= 0")
	}
	if input.FetchedThroughCommitWatermark < 0 {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("fetched_through_commit_watermark must be >= 0")
	}
	if input.AcknowledgedCommitWatermark < 0 {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("acknowledged_commit_watermark must be >= 0")
	}
	if input.FetchedThroughCommitWatermark > input.ExportedHeadCommitWatermark {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("fetched_through_commit_watermark cannot exceed exported_head_commit_watermark")
	}
	if input.AcknowledgedCommitWatermark > input.FetchedThroughCommitWatermark {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("acknowledged_commit_watermark cannot exceed fetched_through_commit_watermark")
	}
	input.LastFetchAt = strings.TrimSpace(input.LastFetchAt)
	input.LastAcknowledgedAt = strings.TrimSpace(input.LastAcknowledgedAt)
	if _, err := parseAuthorityTimestamp("last_fetch_at", input.LastFetchAt, false); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, err
	}
	if _, err := parseAuthorityTimestamp("last_acknowledged_at", input.LastAcknowledgedAt, false); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, err
	}
	input.UpdatedAt = strings.TrimSpace(input.UpdatedAt)
	if input.UpdatedAt == "" {
		input.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if _, err := parseAuthorityTimestamp("updated_at", input.UpdatedAt, true); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, err
	}
	return input, nil
}

func normalizeWorkspaceReplicaApplyBatchInput(input WorkspaceReplicaApplyBatchInput) (WorkspaceReplicaApplyBatchInput, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return WorkspaceReplicaApplyBatchInput{}, errors.New("workspace_id is required")
	}
	input.Scope = normalizeWorkspaceAuthorityScope(input.Scope)
	input.ReplicaAuthorityNodeID = strings.TrimSpace(input.ReplicaAuthorityNodeID)
	if input.ReplicaAuthorityNodeID == "" {
		return WorkspaceReplicaApplyBatchInput{}, errors.New("replica_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaApplyBatchInput{}, fmt.Errorf("replica_authority_node_id is invalid: %w", err)
	}
	input.LeaderAuthorityNodeID = strings.TrimSpace(input.LeaderAuthorityNodeID)
	if input.LeaderAuthorityNodeID == "" {
		return WorkspaceReplicaApplyBatchInput{}, errors.New("leader_authority_node_id is required")
	}
	if err := validateAuthorityNodeID(input.LeaderAuthorityNodeID); err != nil {
		return WorkspaceReplicaApplyBatchInput{}, fmt.Errorf("leader_authority_node_id is invalid: %w", err)
	}
	if input.LeaderAuthorityNodeID == input.ReplicaAuthorityNodeID {
		return WorkspaceReplicaApplyBatchInput{}, errors.New("leader_authority_node_id must differ from replica_authority_node_id")
	}
	if input.AuthorityTerm <= 0 {
		return WorkspaceReplicaApplyBatchInput{}, errors.New("authority_term must be > 0")
	}
	if input.ExportedHeadCommitWatermark < 0 {
		return WorkspaceReplicaApplyBatchInput{}, errors.New("exported_head_commit_watermark must be >= 0")
	}
	seen := make(map[string]struct{}, len(input.EventIDs))
	normalizedIDs := make([]string, 0, len(input.EventIDs))
	for _, raw := range input.EventIDs {
		eventID := strings.TrimSpace(raw)
		if eventID == "" {
			return WorkspaceReplicaApplyBatchInput{}, errors.New("event_ids cannot contain empty values")
		}
		if _, ok := seen[eventID]; ok {
			return WorkspaceReplicaApplyBatchInput{}, fmt.Errorf("event_ids contains duplicate %q", eventID)
		}
		seen[eventID] = struct{}{}
		normalizedIDs = append(normalizedIDs, eventID)
	}
	if len(normalizedIDs) == 0 {
		return WorkspaceReplicaApplyBatchInput{}, errors.New("event_ids is required")
	}
	input.EventIDs = normalizedIDs
	appliedAt := strings.TrimSpace(input.AppliedAt)
	if appliedAt == "" {
		appliedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if _, err := parseAuthorityTimestamp("applied_at", appliedAt, true); err != nil {
		return WorkspaceReplicaApplyBatchInput{}, err
	}
	fetchedAt := strings.TrimSpace(input.FetchedAt)
	if fetchedAt == "" {
		fetchedAt = appliedAt
	}
	if _, err := parseAuthorityTimestamp("fetched_at", fetchedAt, true); err != nil {
		return WorkspaceReplicaApplyBatchInput{}, err
	}
	acknowledgedAt := strings.TrimSpace(input.AcknowledgedAt)
	if acknowledgedAt == "" {
		acknowledgedAt = appliedAt
	}
	if _, err := parseAuthorityTimestamp("acknowledged_at", acknowledgedAt, true); err != nil {
		return WorkspaceReplicaApplyBatchInput{}, err
	}
	input.FetchedAt = fetchedAt
	input.AcknowledgedAt = acknowledgedAt
	input.AppliedAt = appliedAt
	input.ApplyReason = strings.TrimSpace(input.ApplyReason)
	return input, nil
}

func scanWorkspaceReplicaTransportState(scanner interface{ Scan(dest ...any) error }, record *WorkspaceReplicaTransportStateRecord) error {
	var lastFetchAt sql.NullString
	var lastAcknowledgedAt sql.NullString
	if err := scanner.Scan(
		&record.WorkspaceID,
		&record.Scope,
		&record.ReplicaAuthorityNodeID,
		&record.LeaderAuthorityNodeID,
		&record.AuthorityTerm,
		&record.ExportedHeadCommitWatermark,
		&record.FetchedThroughCommitWatermark,
		&record.AcknowledgedCommitWatermark,
		&lastFetchAt,
		&lastAcknowledgedAt,
		&record.UpdatedAt,
	); err != nil {
		return err
	}
	if lastFetchAt.Valid {
		record.LastFetchAt = lastFetchAt.String
	}
	if lastAcknowledgedAt.Valid {
		record.LastAcknowledgedAt = lastAcknowledgedAt.String
	}
	return nil
}

func (s *Store) GetWorkspaceReplicaTransportState(ctx context.Context, workspaceID, scope, replicaAuthorityNodeID string) (WorkspaceReplicaTransportStateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("workspace_id is required")
	}
	scope = normalizeWorkspaceAuthorityScope(scope)
	replicaAuthorityNodeID = strings.TrimSpace(replicaAuthorityNodeID)
	if replicaAuthorityNodeID == "" {
		return WorkspaceReplicaTransportStateRecord{}, errors.New("replica_authority_node_id is required")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT workspace_id, scope, replica_authority_node_id, leader_authority_node_id,
       authority_term, exported_head_commit_watermark, fetched_through_commit_watermark,
       acknowledged_commit_watermark, last_fetch_at, last_acknowledged_at, updated_at
  FROM workspace_replica_transport_state
 WHERE workspace_id = ? AND scope = ? AND replica_authority_node_id = ?`,
		workspaceID,
		scope,
		replicaAuthorityNodeID,
	)
	var record WorkspaceReplicaTransportStateRecord
	if err := scanWorkspaceReplicaTransportState(row, &record); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaTransportStateRecord{}, err
		}
		return WorkspaceReplicaTransportStateRecord{}, fmt.Errorf("get workspace replica transport state: %w", err)
	}
	return record, nil
}

func (s *Store) ListWorkspaceReplicaTransportStates(ctx context.Context, workspaceID, scope string) ([]WorkspaceReplicaTransportStateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	scope = normalizeWorkspaceAuthorityScope(scope)
	rows, err := s.db.QueryContext(ctx, `
SELECT workspace_id, scope, replica_authority_node_id, leader_authority_node_id,
       authority_term, exported_head_commit_watermark, fetched_through_commit_watermark,
       acknowledged_commit_watermark, last_fetch_at, last_acknowledged_at, updated_at
  FROM workspace_replica_transport_state
 WHERE workspace_id = ? AND scope = ?
 ORDER BY authority_term DESC, updated_at DESC, replica_authority_node_id`,
		workspaceID,
		scope,
	)
	if err != nil {
		return nil, fmt.Errorf("list workspace replica transport states: %w", err)
	}
	defer rows.Close()
	var out []WorkspaceReplicaTransportStateRecord
	for rows.Next() {
		var record WorkspaceReplicaTransportStateRecord
		if err := scanWorkspaceReplicaTransportState(rows, &record); err != nil {
			return nil, fmt.Errorf("scan workspace replica transport state: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace replica transport states: %w", err)
	}
	return out, nil
}

func (s *Store) ApplyWorkspaceReplicaBatch(ctx context.Context, input WorkspaceReplicaApplyBatchInput) (WorkspaceReplicaTransportStateRecord, WorkspaceReplicaStateRecord, error) {
	input, err := normalizeWorkspaceReplicaApplyBatchInput(input)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("begin workspace replica apply tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	transportState, replicaState, err := s.applyWorkspaceReplicaBatchTx(ctx, tx, input)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("commit workspace replica apply tx: %w", err)
	}
	return transportState, replicaState, nil
}

func (s *Store) applyWorkspaceReplicaBatchTx(ctx context.Context, tx *sql.Tx, input WorkspaceReplicaApplyBatchInput) (WorkspaceReplicaTransportStateRecord, WorkspaceReplicaStateRecord, error) {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, input.WorkspaceID); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, input.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, input.LeaderAuthorityNodeID); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}

	installState, err := s.getWorkspaceReplicaInstallStateTx(ctx, tx, input.WorkspaceID, input.Scope, input.ReplicaAuthorityNodeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, errors.New("replica apply requires installed base-state")
		}
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if installState.InstallStatus != WorkspaceReplicaInstallInstalled {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, errors.New("replica apply requires completed base-state install")
	}
	if installState.AuthorityTerm != input.AuthorityTerm {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("replica apply authority_term %d does not match installed term %d", input.AuthorityTerm, installState.AuthorityTerm)
	}
	if installState.LeaderAuthorityNodeID != input.LeaderAuthorityNodeID {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, errors.New("replica apply leader_authority_node_id does not match installed base-state")
	}

	authority, err := s.getWorkspaceAuthorityTx(ctx, tx, input.WorkspaceID, input.Scope)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("get workspace authority for replica apply: %w", err)
	}
	if authority.Status != WorkspaceAuthorityStatusActive {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("workspace replica apply requires active authority, got %s", authority.Status)
	}
	if strings.TrimSpace(authority.HolderAuthorityNodeID) != input.LeaderAuthorityNodeID {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("workspace replica apply leader %q does not match current holder %q", input.LeaderAuthorityNodeID, authority.HolderAuthorityNodeID)
	}
	if authority.Term != input.AuthorityTerm {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("workspace replica apply term %d does not match current authority term %d", input.AuthorityTerm, authority.Term)
	}

	journalHead, err := s.workspaceRuntimeJournalHeadTx(ctx, tx, input.WorkspaceID)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("query workspace runtime journal head for replica apply: %w", err)
	}
	if input.ExportedHeadCommitWatermark > journalHead {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("replica apply exported_head_commit_watermark %d exceeds current journal head %d", input.ExportedHeadCommitWatermark, journalHead)
	}

	replicaState, err := s.getWorkspaceReplicaStateTx(ctx, tx, input.WorkspaceID, input.Scope, input.ReplicaAuthorityNodeID)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if replicaState.ReplicaRole != WorkspaceReplicaRoleFollower {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, errors.New("replica apply requires follower replica_role")
	}
	if replicaState.LeaderAuthorityNodeID != input.LeaderAuthorityNodeID {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, errors.New("replica apply leader_authority_node_id does not match follower replica state")
	}
	if replicaState.AuthorityTerm != input.AuthorityTerm {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("replica apply term %d does not match follower replica term %d", input.AuthorityTerm, replicaState.AuthorityTerm)
	}

	currentTransport, err := s.getWorkspaceReplicaTransportStateTx(ctx, tx, input.WorkspaceID, input.Scope, input.ReplicaAuthorityNodeID)
	hasCurrentTransport := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if hasCurrentTransport && currentTransport.AuthorityTerm > input.AuthorityTerm {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("replica apply authority_term %d regresses existing transport term %d", input.AuthorityTerm, currentTransport.AuthorityTerm)
	}
	if _, err := s.gateWorkspaceReplicaApplyStateTx(ctx, tx, input.WorkspaceID, input.Scope, input.ReplicaAuthorityNodeID, input.LeaderAuthorityNodeID, input.AuthorityTerm, input.AppliedAt); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}

	events, err := s.loadReplicaApplyRuntimeEventsTx(ctx, tx, input.WorkspaceID, input.EventIDs)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}

	baselineApplied := replicaState.AppliedWatermark
	if baselineApplied < installState.BaseCommitWatermark {
		baselineApplied = installState.BaseCommitWatermark
	}
	currentExported := int64(0)
	currentFetched := int64(0)
	currentAcknowledged := int64(0)
	if hasCurrentTransport && currentTransport.AuthorityTerm == input.AuthorityTerm {
		currentExported = currentTransport.ExportedHeadCommitWatermark
		currentFetched = currentTransport.FetchedThroughCommitWatermark
		currentAcknowledged = currentTransport.AcknowledgedCommitWatermark
	}
	effectiveExported := maxInt64(currentExported, input.ExportedHeadCommitWatermark)
	firstSeq, throughWatermark, err := validateReplicaApplyRuntimeEvents(events, input.WorkspaceID, input.LeaderAuthorityNodeID, input.AuthorityTerm, effectiveExported, baselineApplied)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if firstSeq > baselineApplied+1 {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, fmt.Errorf("replica apply batch starts at ingest_seq %d but next expected watermark is %d", firstSeq, baselineApplied+1)
	}

	nextTransport := WorkspaceReplicaTransportStateRecord{
		WorkspaceID:                   input.WorkspaceID,
		Scope:                         input.Scope,
		ReplicaAuthorityNodeID:        input.ReplicaAuthorityNodeID,
		LeaderAuthorityNodeID:         input.LeaderAuthorityNodeID,
		AuthorityTerm:                 input.AuthorityTerm,
		ExportedHeadCommitWatermark:   effectiveExported,
		FetchedThroughCommitWatermark: maxInt64(currentFetched, throughWatermark),
		AcknowledgedCommitWatermark:   maxInt64(currentAcknowledged, throughWatermark),
		LastFetchAt:                   input.FetchedAt,
		LastAcknowledgedAt:            input.AcknowledgedAt,
		UpdatedAt:                     input.AppliedAt,
	}
	if hasCurrentTransport && currentTransport.AuthorityTerm == input.AuthorityTerm {
		if nextTransport.FetchedThroughCommitWatermark == currentTransport.FetchedThroughCommitWatermark {
			nextTransport.LastFetchAt = currentTransport.LastFetchAt
		}
		if nextTransport.AcknowledgedCommitWatermark == currentTransport.AcknowledgedCommitWatermark {
			nextTransport.LastAcknowledgedAt = currentTransport.LastAcknowledgedAt
		}
	}
	nextTransport, err = normalizeWorkspaceReplicaTransportStateRecord(nextTransport)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}

	nextReplica := replicaState
	nextReplica.LeaderAuthorityNodeID = input.LeaderAuthorityNodeID
	nextReplica.AuthorityTerm = input.AuthorityTerm
	nextReplica.CommitWatermark = maxInt64(replicaState.CommitWatermark, nextTransport.ExportedHeadCommitWatermark)
	nextReplica.AppliedWatermark = maxInt64(replicaState.AppliedWatermark, throughWatermark)
	nextReplica.MembershipState = WorkspaceReplicaMembershipCatchingUp
	nextReplica.MembershipReason = buildReplicaApplyReason(nextTransport.ExportedHeadCommitWatermark, nextReplica.AppliedWatermark, input.ApplyReason)
	nextReplica.UpdatedAt = input.AppliedAt
	if nextTransport.FetchedThroughCommitWatermark > currentFetched || replicaState.LastFetchAt == "" {
		nextReplica.LastFetchAt = input.FetchedAt
	}
	if nextReplica.AppliedWatermark > replicaState.AppliedWatermark || replicaState.LastApplyAt == "" {
		nextReplica.LastApplyAt = input.AppliedAt
	}
	nextReplica, err = normalizeWorkspaceReplicaStateRecord(nextReplica)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}

	transportUnchanged := hasCurrentTransport && currentTransport == nextTransport
	replicaUnchanged := replicaState == nextReplica
	if transportUnchanged && replicaUnchanged {
		return currentTransport, replicaState, nil
	}

	transportState, err := s.upsertWorkspaceReplicaTransportStateTx(ctx, tx, nextTransport)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	replicaState, err = s.upsertWorkspaceReplicaStateTx(ctx, tx, nextReplica)
	if err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	if _, err := s.clearWorkspaceReplicaApplyStateTx(ctx, tx, input.WorkspaceID, input.Scope, input.ReplicaAuthorityNodeID, input.LeaderAuthorityNodeID, input.AuthorityTerm, nextTransport.ExportedHeadCommitWatermark, nextReplica.AppliedWatermark, input.AppliedAt); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, WorkspaceReplicaStateRecord{}, err
	}
	return transportState, replicaState, nil
}

func (s *Store) loadReplicaApplyRuntimeEventsTx(ctx context.Context, tx *sql.Tx, workspaceID string, eventIDs []string) ([]RuntimeEventRecord, error) {
	events := make([]RuntimeEventRecord, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		record, err := s.getRuntimeEventByIDTx(ctx, tx, workspaceID, eventID)
		if err != nil {
			return nil, fmt.Errorf("load replica apply runtime event %q: %w", eventID, err)
		}
		events = append(events, record)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].IngestSeq == events[j].IngestSeq {
			return events[i].EventID < events[j].EventID
		}
		return events[i].IngestSeq < events[j].IngestSeq
	})
	return events, nil
}

func validateReplicaApplyRuntimeEvents(events []RuntimeEventRecord, workspaceID, leaderAuthorityNodeID string, authorityTerm, exportedHeadCommitWatermark, baselineApplied int64) (int64, int64, error) {
	if len(events) == 0 {
		return 0, 0, errors.New("replica apply requires at least one runtime event")
	}
	for i, event := range events {
		if strings.TrimSpace(event.WorkspaceID) != workspaceID {
			return 0, 0, fmt.Errorf("replica apply runtime event %q belongs to workspace %q, expected %q", event.EventID, event.WorkspaceID, workspaceID)
		}
		if event.IngestSeq <= 0 {
			return 0, 0, fmt.Errorf("replica apply runtime event %q is missing ingest_seq", event.EventID)
		}
		if strings.TrimSpace(event.AuthorityHolderNodeID) == "" || event.AuthorityTerm <= 0 {
			return 0, 0, fmt.Errorf("replica apply runtime event %q is not authority-backed", event.EventID)
		}
		if event.AuthorityHolderNodeID != leaderAuthorityNodeID {
			return 0, 0, fmt.Errorf("replica apply runtime event %q authority holder %q does not match leader %q", event.EventID, event.AuthorityHolderNodeID, leaderAuthorityNodeID)
		}
		if event.AuthorityTerm != authorityTerm {
			return 0, 0, fmt.Errorf("replica apply runtime event %q authority term %d does not match leader term %d", event.EventID, event.AuthorityTerm, authorityTerm)
		}
		if i > 0 && event.IngestSeq != events[i-1].IngestSeq+1 {
			return 0, 0, fmt.Errorf("replica apply batch is not contiguous at ingest_seq %d (previous %d)", event.IngestSeq, events[i-1].IngestSeq)
		}
	}
	firstSeq := events[0].IngestSeq
	throughWatermark := events[len(events)-1].IngestSeq
	if firstSeq > baselineApplied+1 {
		return 0, 0, fmt.Errorf("replica apply batch is missing tail before ingest_seq %d (applied watermark %d)", firstSeq, baselineApplied)
	}
	if throughWatermark > exportedHeadCommitWatermark {
		return 0, 0, fmt.Errorf("replica apply through watermark %d exceeds exported head %d", throughWatermark, exportedHeadCommitWatermark)
	}
	return firstSeq, throughWatermark, nil
}

func buildReplicaApplyReason(exportedHeadCommitWatermark, appliedWatermark int64, applyReason string) string {
	base := "applied through exported head; replica freshness/read readiness still pending"
	if exportedHeadCommitWatermark > appliedWatermark {
		base = "applied contiguous export batch; leader head still ahead"
	}
	if strings.TrimSpace(applyReason) == "" {
		return base
	}
	return base + ": " + strings.TrimSpace(applyReason)
}

func (s *Store) upsertWorkspaceReplicaTransportStateTx(ctx context.Context, tx *sql.Tx, record WorkspaceReplicaTransportStateRecord) (WorkspaceReplicaTransportStateRecord, error) {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, record.ReplicaAuthorityNodeID); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, err
	}
	if err := s.ensureRuntimeNodeExistsTx(ctx, tx, record.LeaderAuthorityNodeID); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, err
	}
	current, err := s.getWorkspaceReplicaTransportStateTx(ctx, tx, record.WorkspaceID, record.Scope, record.ReplicaAuthorityNodeID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return WorkspaceReplicaTransportStateRecord{}, err
	default:
		if record.AuthorityTerm < current.AuthorityTerm {
			return WorkspaceReplicaTransportStateRecord{}, fmt.Errorf("authority_term %d regresses existing transport term %d", record.AuthorityTerm, current.AuthorityTerm)
		}
		if record.AuthorityTerm == current.AuthorityTerm {
			if record.ExportedHeadCommitWatermark < current.ExportedHeadCommitWatermark {
				return WorkspaceReplicaTransportStateRecord{}, fmt.Errorf("exported_head_commit_watermark %d regresses existing transport watermark %d", record.ExportedHeadCommitWatermark, current.ExportedHeadCommitWatermark)
			}
			if record.FetchedThroughCommitWatermark < current.FetchedThroughCommitWatermark {
				return WorkspaceReplicaTransportStateRecord{}, fmt.Errorf("fetched_through_commit_watermark %d regresses existing transport watermark %d", record.FetchedThroughCommitWatermark, current.FetchedThroughCommitWatermark)
			}
			if record.AcknowledgedCommitWatermark < current.AcknowledgedCommitWatermark {
				return WorkspaceReplicaTransportStateRecord{}, fmt.Errorf("acknowledged_commit_watermark %d regresses existing transport watermark %d", record.AcknowledgedCommitWatermark, current.AcknowledgedCommitWatermark)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO workspace_replica_transport_state(
	workspace_id, scope, replica_authority_node_id, leader_authority_node_id,
	authority_term, exported_head_commit_watermark, fetched_through_commit_watermark,
	acknowledged_commit_watermark, last_fetch_at, last_acknowledged_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workspace_id, scope, replica_authority_node_id) DO UPDATE SET
	leader_authority_node_id = excluded.leader_authority_node_id,
	authority_term = excluded.authority_term,
	exported_head_commit_watermark = excluded.exported_head_commit_watermark,
	fetched_through_commit_watermark = excluded.fetched_through_commit_watermark,
	acknowledged_commit_watermark = excluded.acknowledged_commit_watermark,
	last_fetch_at = excluded.last_fetch_at,
	last_acknowledged_at = excluded.last_acknowledged_at,
	updated_at = excluded.updated_at`,
		record.WorkspaceID,
		record.Scope,
		record.ReplicaAuthorityNodeID,
		record.LeaderAuthorityNodeID,
		record.AuthorityTerm,
		record.ExportedHeadCommitWatermark,
		record.FetchedThroughCommitWatermark,
		record.AcknowledgedCommitWatermark,
		blankStringOrNil(record.LastFetchAt),
		blankStringOrNil(record.LastAcknowledgedAt),
		record.UpdatedAt,
	); err != nil {
		return WorkspaceReplicaTransportStateRecord{}, fmt.Errorf("upsert workspace replica transport state: %w", err)
	}
	return s.getWorkspaceReplicaTransportStateTx(ctx, tx, record.WorkspaceID, record.Scope, record.ReplicaAuthorityNodeID)
}

func (s *Store) getWorkspaceReplicaTransportStateTx(ctx context.Context, tx *sql.Tx, workspaceID, scope, replicaAuthorityNodeID string) (WorkspaceReplicaTransportStateRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT workspace_id, scope, replica_authority_node_id, leader_authority_node_id,
       authority_term, exported_head_commit_watermark, fetched_through_commit_watermark,
       acknowledged_commit_watermark, last_fetch_at, last_acknowledged_at, updated_at
  FROM workspace_replica_transport_state
 WHERE workspace_id = ? AND scope = ? AND replica_authority_node_id = ?`,
		workspaceID,
		scope,
		replicaAuthorityNodeID,
	)
	var record WorkspaceReplicaTransportStateRecord
	if err := scanWorkspaceReplicaTransportState(row, &record); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceReplicaTransportStateRecord{}, err
		}
		return WorkspaceReplicaTransportStateRecord{}, fmt.Errorf("get workspace replica transport state tx: %w", err)
	}
	return record, nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
