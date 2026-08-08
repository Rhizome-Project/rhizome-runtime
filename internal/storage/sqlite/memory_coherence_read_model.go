package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type MemoryCoherenceReportFilter struct {
	WorkspaceID string
	AgentID     string
	SessionID   string
	ReportScope string
	Limit       int
}

type MemoryCoherenceScopeReport struct {
	WorkspaceID              string                 `json:"workspace_id"`
	TimeAuthority            WorkspaceTimeAuthority `json:"time_authority"`
	AgentID                  string                 `json:"agent_id"`
	SessionID                string                 `json:"session_id,omitempty"`
	ReportScope              string                 `json:"report_scope"`
	MetricsReportID          string                 `json:"metrics_report_id,omitempty"`
	MetricsUpdatedAt         string                 `json:"metrics_updated_at,omitempty"`
	ResidencyReportID        string                 `json:"residency_report_id,omitempty"`
	ResidencyUpdatedAt       string                 `json:"residency_updated_at,omitempty"`
	InvalidationUpdatedAt    string                 `json:"invalidation_updated_at,omitempty"`
	LastObservedAt           string                 `json:"last_observed_at,omitempty"`
	LookupCount              int                    `json:"lookup_count"`
	StaleHitRate             float64                `json:"stale_hit_rate"`
	PromotionPrecision       float64                `json:"promotion_precision"`
	FlushUtility             float64                `json:"flush_utility"`
	OffloadRatio             float64                `json:"offload_ratio"`
	StaleReadRate            float64                `json:"stale_read_rate"`
	P1EntryCount             int                    `json:"p1_entry_count"`
	P2EntryCount             int                    `json:"p2_entry_count"`
	P3EntryCount             int                    `json:"p3_entry_count"`
	ReplicaCount             int                    `json:"replica_count"`
	InvalidatedReplicaCount  int                    `json:"invalidated_replica_count"`
	OpenInvalidationCount    int                    `json:"open_invalidation_count"`
	ReadyInvalidationCount   int                    `json:"ready_invalidation_count"`
	LeasedInvalidationCount  int                    `json:"leased_invalidation_count"`
	BackoffInvalidationCount int                    `json:"backoff_invalidation_count"`
	AckedInvalidationCount   int                    `json:"acked_invalidation_count"`
	DeadLetterCount          int                    `json:"dead_letter_count"`
	CoherenceBandHint        string                 `json:"coherence_band_hint"`
	NeedsAttention           bool                   `json:"needs_attention"`
	AttentionReasons         []string               `json:"attention_reasons,omitempty"`
	Summary                  string                 `json:"summary"`
}

type MemoryCoherenceReport struct {
	WorkspaceID            string                          `json:"workspace_id"`
	TimeAuthority          WorkspaceTimeAuthority          `json:"time_authority"`
	AgentID                string                          `json:"agent_id,omitempty"`
	SessionID              string                          `json:"session_id,omitempty"`
	ReportScope            string                          `json:"report_scope,omitempty"`
	Items                  []MemoryCoherenceScopeReport    `json:"items"`
	ClaimFreshness         *KnowledgeClaimFreshnessSummary `json:"claim_freshness,omitempty"`
	Count                  int                             `json:"count"`
	ScopeCount             int                             `json:"scope_count"`
	AttentionScopeCount    int                             `json:"attention_scope_count"`
	ReadyInvalidationCount int                             `json:"ready_invalidation_count"`
	DeadLetterCount        int                             `json:"dead_letter_count"`
	MaxStaleHitRate        float64                         `json:"max_stale_hit_rate"`
	NeedsAttention         bool                            `json:"needs_attention"`
	AttentionReasons       []string                        `json:"attention_reasons,omitempty"`
	GeneratedAt            string                          `json:"generated_at"`
}

type MemoryCoherenceSnapshotResult struct {
	Report MemoryCoherenceReport `json:"report"`
	Event  RuntimeEventRecord    `json:"event"`
}

type memoryCoherenceScopeKey struct {
	WorkspaceID    string
	AgentID        string
	SessionID      string
	ReportScope    string
	LastObservedAt string
}

func (s *Store) BuildMemoryCoherenceReport(ctx context.Context, filter MemoryCoherenceReportFilter) (MemoryCoherenceReport, error) {
	filter, err := normalizeMemoryCoherenceReportFilter(filter)
	if err != nil {
		return MemoryCoherenceReport{}, err
	}
	keys, err := listMemoryCoherenceScopeKeys(ctx, s.db, filter)
	if err != nil {
		return MemoryCoherenceReport{}, err
	}
	report := MemoryCoherenceReport{
		WorkspaceID: filter.WorkspaceID,
		AgentID:     filter.AgentID,
		SessionID:   filter.SessionID,
		ReportScope: filter.ReportScope,
		Items:       make([]MemoryCoherenceScopeReport, 0, len(keys)),
	}
	report.TimeAuthority, err = s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return MemoryCoherenceReport{}, err
	}
	report.GeneratedAt = generatedAtFromWorkspaceTimeAuthority(report.TimeAuthority)
	claimFreshness, err := s.BuildKnowledgeClaimFreshnessSummary(ctx, filter.WorkspaceID, report.GeneratedAt, knowledgeClaimFreshnessMaxItems)
	if err != nil {
		return MemoryCoherenceReport{}, err
	}
	report.ClaimFreshness = &claimFreshness
	if claimFreshness.NeedsAttention {
		report.NeedsAttention = true
		report.AttentionReasons = append(report.AttentionReasons, claimFreshness.AttentionReasons...)
	}
	for _, key := range keys {
		item, err := s.buildMemoryCoherenceScopeReport(ctx, key)
		if err != nil {
			return MemoryCoherenceReport{}, err
		}
		item.TimeAuthority = report.TimeAuthority
		report.Items = append(report.Items, item)
		report.ScopeCount++
		if item.NeedsAttention {
			report.AttentionScopeCount++
			report.NeedsAttention = true
			report.AttentionReasons = appendUniqueString(report.AttentionReasons, "SCOPE_ATTENTION")
		}
		report.ReadyInvalidationCount += item.ReadyInvalidationCount
		report.DeadLetterCount += item.DeadLetterCount
		if item.StaleHitRate > report.MaxStaleHitRate {
			report.MaxStaleHitRate = item.StaleHitRate
		}
	}
	report.Count = len(report.Items)
	return report, nil
}

func (s *Store) GetMemoryCoherenceScope(ctx context.Context, workspaceID, agentID, sessionID, reportScope string) (MemoryCoherenceScopeReport, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return MemoryCoherenceScopeReport{}, errors.New("workspace_id is required")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return MemoryCoherenceScopeReport{}, errors.New("agent_id is required")
	}
	sessionID = strings.TrimSpace(sessionID)
	reportScope = normalizeMemoryResidencyReportScope(reportScope)
	if reportScope == "" {
		if sessionID != "" {
			reportScope = "SESSION"
		} else {
			reportScope = "AGENT"
		}
	}
	report, err := s.buildMemoryCoherenceScopeReport(ctx, memoryCoherenceScopeKey{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		SessionID:   sessionID,
		ReportScope: reportScope,
	})
	if err != nil {
		return MemoryCoherenceScopeReport{}, err
	}
	report.TimeAuthority, err = s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return MemoryCoherenceScopeReport{}, err
	}
	return report, nil
}

func (s *Store) SnapshotMemoryCoherenceReport(ctx context.Context, filter MemoryCoherenceReportFilter) (MemoryCoherenceSnapshotResult, error) {
	report, err := s.BuildMemoryCoherenceReport(ctx, filter)
	if err != nil {
		return MemoryCoherenceSnapshotResult{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, report.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return MemoryCoherenceSnapshotResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return MemoryCoherenceSnapshotResult{}, fmt.Errorf("begin memory coherence snapshot tx: %w", err)
	}
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		event, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: report.WorkspaceID,
			EventType:   "memory.coherence_snapshot",
			EntityType:  "memory_coherence",
			EntityID:    memoryCoherenceSnapshotEntityID(report),
			ActorType:   "system",
			ActorID:     "memory_coherence",
			PayloadJSON: mustJSON(memoryCoherenceSnapshotPayload(report)),
			CreatedAt:   now,
		})
		if innerErr != nil {
			return fmt.Errorf("append memory coherence snapshot event: %w", innerErr)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return MemoryCoherenceSnapshotResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return MemoryCoherenceSnapshotResult{}, fmt.Errorf("commit memory coherence snapshot tx: %w", err)
	}
	return MemoryCoherenceSnapshotResult{Report: report, Event: event}, nil
}

func (s *Store) buildMemoryCoherenceScopeReport(ctx context.Context, key memoryCoherenceScopeKey) (MemoryCoherenceScopeReport, error) {
	metrics, err := s.getLatestMemoryMetricsReport(ctx, key)
	if err != nil {
		return MemoryCoherenceScopeReport{}, err
	}
	residency, err := s.getLatestMemoryResidencyReport(ctx, key)
	if err != nil {
		return MemoryCoherenceScopeReport{}, err
	}
	openCount, readyCount, leasedCount, backoffCount, ackedCount, deadLetterCount, invalidationUpdatedAt, err := s.getMemoryCoherenceInvalidationCounts(ctx, key)
	if err != nil {
		return MemoryCoherenceScopeReport{}, err
	}
	report := MemoryCoherenceScopeReport{
		WorkspaceID:              key.WorkspaceID,
		AgentID:                  key.AgentID,
		SessionID:                key.SessionID,
		ReportScope:              key.ReportScope,
		InvalidationUpdatedAt:    invalidationUpdatedAt,
		LastObservedAt:           latestNonEmptyTimestamp(key.LastObservedAt, invalidationUpdatedAt),
		OpenInvalidationCount:    openCount,
		ReadyInvalidationCount:   readyCount,
		LeasedInvalidationCount:  leasedCount,
		BackoffInvalidationCount: backoffCount,
		AckedInvalidationCount:   ackedCount,
		DeadLetterCount:          deadLetterCount,
	}
	if metrics != nil {
		report.MetricsReportID = metrics.ReportID
		report.MetricsUpdatedAt = metrics.UpdatedAt
		report.LookupCount = metrics.LookupCount
		report.StaleHitRate = metrics.StaleHitRate
		report.PromotionPrecision = metrics.PromotionPrecision
		report.FlushUtility = metrics.FlushUtility
		report.OffloadRatio = metrics.OffloadRatio
		report.LastObservedAt = latestNonEmptyTimestamp(report.LastObservedAt, metrics.UpdatedAt)
	}
	if residency != nil {
		report.ResidencyReportID = residency.Report.ReportID
		report.ResidencyUpdatedAt = residency.Report.UpdatedAt
		report.StaleReadRate = residency.Report.StaleReadRate
		report.P1EntryCount = residency.Report.P1EntryCount
		report.P2EntryCount = residency.Report.P2EntryCount
		report.P3EntryCount = residency.Report.P3EntryCount
		report.ReplicaCount = residency.Report.ReplicaCount
		report.InvalidatedReplicaCount = residency.Report.InvalidatedReplicaCount
		report.LastObservedAt = latestNonEmptyTimestamp(report.LastObservedAt, residency.Report.UpdatedAt)
	}
	report.CoherenceBandHint, report.AttentionReasons = classifyMemoryCoherenceBand(report)
	report.NeedsAttention = report.CoherenceBandHint != "STABLE"
	report.Summary = summarizeMemoryCoherenceScope(report)
	return report, nil
}

func (s *Store) buildMemoryCoherenceScopeReportTx(ctx context.Context, tx *sql.Tx, key memoryCoherenceScopeKey) (MemoryCoherenceScopeReport, error) {
	metrics, err := getLatestMemoryMetricsReportTx(ctx, tx, key)
	if err != nil {
		return MemoryCoherenceScopeReport{}, err
	}
	residency, err := s.getLatestMemoryResidencyReportTx(ctx, tx, key)
	if err != nil {
		return MemoryCoherenceScopeReport{}, err
	}
	openCount, readyCount, leasedCount, backoffCount, ackedCount, deadLetterCount, invalidationUpdatedAt, err := s.getMemoryCoherenceInvalidationCountsTx(ctx, tx, key)
	if err != nil {
		return MemoryCoherenceScopeReport{}, err
	}
	report := MemoryCoherenceScopeReport{
		WorkspaceID:              key.WorkspaceID,
		AgentID:                  key.AgentID,
		SessionID:                key.SessionID,
		ReportScope:              key.ReportScope,
		InvalidationUpdatedAt:    invalidationUpdatedAt,
		LastObservedAt:           latestNonEmptyTimestamp(key.LastObservedAt, invalidationUpdatedAt),
		OpenInvalidationCount:    openCount,
		ReadyInvalidationCount:   readyCount,
		LeasedInvalidationCount:  leasedCount,
		BackoffInvalidationCount: backoffCount,
		AckedInvalidationCount:   ackedCount,
		DeadLetterCount:          deadLetterCount,
	}
	if metrics != nil {
		report.MetricsReportID = metrics.ReportID
		report.MetricsUpdatedAt = metrics.UpdatedAt
		report.LookupCount = metrics.LookupCount
		report.StaleHitRate = metrics.StaleHitRate
		report.PromotionPrecision = metrics.PromotionPrecision
		report.FlushUtility = metrics.FlushUtility
		report.OffloadRatio = metrics.OffloadRatio
		report.LastObservedAt = latestNonEmptyTimestamp(report.LastObservedAt, metrics.UpdatedAt)
	}
	if residency != nil {
		report.ResidencyReportID = residency.Report.ReportID
		report.ResidencyUpdatedAt = residency.Report.UpdatedAt
		report.StaleReadRate = residency.Report.StaleReadRate
		report.P1EntryCount = residency.Report.P1EntryCount
		report.P2EntryCount = residency.Report.P2EntryCount
		report.P3EntryCount = residency.Report.P3EntryCount
		report.ReplicaCount = residency.Report.ReplicaCount
		report.InvalidatedReplicaCount = residency.Report.InvalidatedReplicaCount
		report.LastObservedAt = latestNonEmptyTimestamp(report.LastObservedAt, residency.Report.UpdatedAt)
	}
	report.CoherenceBandHint, report.AttentionReasons = classifyMemoryCoherenceBand(report)
	report.NeedsAttention = report.CoherenceBandHint != "STABLE"
	report.Summary = summarizeMemoryCoherenceScope(report)
	return report, nil
}

func (s *Store) getLatestMemoryMetricsReport(ctx context.Context, key memoryCoherenceScopeKey) (*MemoryMetricsReportRecord, error) {
	items, err := s.ListMemoryMetricsReports(ctx, MemoryMetricsReportFilter{
		WorkspaceID: key.WorkspaceID,
		AgentID:     key.AgentID,
		SessionID:   key.SessionID,
		ReportScope: key.ReportScope,
		Limit:       1,
	})
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	return &items[0], nil
}

func getLatestMemoryMetricsReportTx(ctx context.Context, tx *sql.Tx, key memoryCoherenceScopeKey) (*MemoryMetricsReportRecord, error) {
	query := strings.Builder{}
	query.WriteString(`SELECT report_id, workspace_id, agent_id, session_id, report_scope,
	        window_started_at, window_ended_at,
	        lookup_count, l1_hit_count, l2_hit_count, p3_hit_count, stale_hit_count,
	        promotion_count, promotion_reuse_count, flush_count, flush_positive_count,
	        local_consolidation_count, potential_shared_op_count,
	        dissent_hit_count, dissent_available_count, pollution_count, notes_json, created_at, updated_at
	   FROM memory_access_stats
	  WHERE workspace_id = ? AND agent_id = ? AND session_id = ? AND report_scope = ?
	  ORDER BY updated_at DESC, report_id DESC
	  LIMIT 1`)
	record, err := scanMemoryMetricsReportRecord(tx.QueryRowContext(ctx, query.String(), key.WorkspaceID, key.AgentID, key.SessionID, key.ReportScope))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &record, nil
}

func (s *Store) getLatestMemoryResidencyReport(ctx context.Context, key memoryCoherenceScopeKey) (*MemoryResidencyReportDetail, error) {
	items, err := s.ListMemoryResidencyReports(ctx, MemoryResidencyReportFilter{
		WorkspaceID: key.WorkspaceID,
		AgentID:     key.AgentID,
		SessionID:   key.SessionID,
		ReportScope: key.ReportScope,
		Limit:       1,
	})
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	detail, err := s.GetMemoryResidencyReport(ctx, key.WorkspaceID, items[0].ReportID)
	if err != nil {
		return nil, err
	}
	return &detail, nil
}

func (s *Store) getLatestMemoryResidencyReportTx(ctx context.Context, tx *sql.Tx, key memoryCoherenceScopeKey) (*MemoryResidencyReportDetail, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT report_id
		   FROM memory_residency_reports
		  WHERE workspace_id = ? AND agent_id = ? AND session_id = ? AND report_scope = ?
		  ORDER BY updated_at DESC, report_id DESC
		  LIMIT 1`,
		key.WorkspaceID,
		key.AgentID,
		key.SessionID,
		key.ReportScope,
	)
	var reportID string
	if err := row.Scan(&reportID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	detail, err := s.getMemoryResidencyReportTx(ctx, tx, key.WorkspaceID, reportID)
	if err != nil {
		return nil, err
	}
	return &detail, nil
}

func (s *Store) getMemoryCoherenceInvalidationCounts(ctx context.Context, key memoryCoherenceScopeKey) (int, int, int, int, int, int, string, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	referenceAt, err := s.workspaceReferenceTimestamp(ctx, key.WorkspaceID, now)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, "", err
	}
	row := s.db.QueryRowContext(
		ctx,
		`SELECT
		        COALESCE(SUM(CASE WHEN state = 'OPEN' THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN state = 'OPEN' AND (lease_expires_at = '' OR lease_expires_at <= ?) AND (next_delivery_at = '' OR next_delivery_at <= ?) THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN state = 'OPEN' AND lease_expires_at <> '' AND lease_expires_at > ? THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN state = 'OPEN' AND next_delivery_at <> '' AND next_delivery_at > ? THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN state = 'ACKED' THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN state = 'DEAD_LETTER' THEN 1 ELSE 0 END), 0),
		        COALESCE(MAX(updated_at), '')
		   FROM memory_invalidation_queue
		  WHERE workspace_id = ? AND agent_id = ? AND session_id = ? AND report_scope = ?`,
		referenceAt,
		referenceAt,
		referenceAt,
		referenceAt,
		key.WorkspaceID,
		key.AgentID,
		key.SessionID,
		key.ReportScope,
	)
	var (
		openCount       int
		readyCount      int
		leasedCount     int
		backoffCount    int
		ackedCount      int
		deadLetterCount int
		updatedAt       string
	)
	if err := row.Scan(&openCount, &readyCount, &leasedCount, &backoffCount, &ackedCount, &deadLetterCount, &updatedAt); err != nil {
		return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("query memory coherence invalidation counts: %w", err)
	}
	return openCount, readyCount, leasedCount, backoffCount, ackedCount, deadLetterCount, updatedAt, nil
}

func (s *Store) getMemoryCoherenceInvalidationCountsTx(ctx context.Context, tx *sql.Tx, key memoryCoherenceScopeKey) (int, int, int, int, int, int, string, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	referenceAt, err := s.workspaceReferenceTimestamp(ctx, key.WorkspaceID, now)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, "", err
	}
	row := tx.QueryRowContext(
		ctx,
		`SELECT
		        COALESCE(SUM(CASE WHEN state = 'OPEN' THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN state = 'OPEN' AND (lease_expires_at = '' OR lease_expires_at <= ?) AND (next_delivery_at = '' OR next_delivery_at <= ?) THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN state = 'OPEN' AND lease_expires_at <> '' AND lease_expires_at > ? THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN state = 'OPEN' AND next_delivery_at <> '' AND next_delivery_at > ? THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN state = 'ACKED' THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN state = 'DEAD_LETTER' THEN 1 ELSE 0 END), 0),
		        COALESCE(MAX(updated_at), '')
		   FROM memory_invalidation_queue
		  WHERE workspace_id = ? AND agent_id = ? AND session_id = ? AND report_scope = ?`,
		referenceAt,
		referenceAt,
		referenceAt,
		referenceAt,
		key.WorkspaceID,
		key.AgentID,
		key.SessionID,
		key.ReportScope,
	)
	var (
		openCount       int
		readyCount      int
		leasedCount     int
		backoffCount    int
		ackedCount      int
		deadLetterCount int
		updatedAt       string
	)
	if err := row.Scan(&openCount, &readyCount, &leasedCount, &backoffCount, &ackedCount, &deadLetterCount, &updatedAt); err != nil {
		return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("query memory coherence invalidation counts: %w", err)
	}
	return openCount, readyCount, leasedCount, backoffCount, ackedCount, deadLetterCount, updatedAt, nil
}

func listMemoryCoherenceScopeKeys(ctx context.Context, db *sql.DB, filter MemoryCoherenceReportFilter) ([]memoryCoherenceScopeKey, error) {
	query := strings.Builder{}
	query.WriteString(`SELECT agent_id, session_id, report_scope, MAX(updated_at) AS last_observed_at
	  FROM (
	        SELECT agent_id, session_id, report_scope, updated_at
	          FROM memory_residency_reports
	         WHERE workspace_id = ?
	        UNION ALL
	        SELECT agent_id, session_id, report_scope, updated_at
	          FROM memory_access_stats
	         WHERE workspace_id = ?
	        UNION ALL
	        SELECT agent_id, session_id, report_scope, updated_at
	          FROM memory_invalidation_queue
	         WHERE workspace_id = ?
	       ) scoped
	 WHERE 1 = 1`)
	args := []any{filter.WorkspaceID, filter.WorkspaceID, filter.WorkspaceID}
	if filter.AgentID != "" {
		query.WriteString(` AND agent_id = ?`)
		args = append(args, filter.AgentID)
	}
	if filter.SessionID != "" {
		query.WriteString(` AND session_id = ?`)
		args = append(args, filter.SessionID)
	}
	if filter.ReportScope != "" {
		query.WriteString(` AND report_scope = ?`)
		args = append(args, filter.ReportScope)
	}
	query.WriteString(` GROUP BY agent_id, session_id, report_scope
	 ORDER BY last_observed_at DESC, agent_id ASC, session_id ASC, report_scope ASC
	 LIMIT ?`)
	args = append(args, filter.Limit)
	rows, err := db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list memory coherence scope keys: %w", err)
	}
	defer rows.Close()
	out := make([]memoryCoherenceScopeKey, 0)
	for rows.Next() {
		var key memoryCoherenceScopeKey
		if err := rows.Scan(&key.AgentID, &key.SessionID, &key.ReportScope, &key.LastObservedAt); err != nil {
			return nil, fmt.Errorf("scan memory coherence scope key: %w", err)
		}
		key.WorkspaceID = filter.WorkspaceID
		out = append(out, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory coherence scope keys: %w", err)
	}
	return out, nil
}

func normalizeMemoryCoherenceReportFilter(filter MemoryCoherenceReportFilter) (MemoryCoherenceReportFilter, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return MemoryCoherenceReportFilter{}, errors.New("workspace_id is required")
	}
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	rawReportScope := strings.TrimSpace(filter.ReportScope)
	filter.ReportScope = normalizeMemoryResidencyReportScope(filter.ReportScope)
	if rawReportScope != "" && filter.ReportScope == "" {
		return MemoryCoherenceReportFilter{}, fmt.Errorf("invalid report_scope: %s", rawReportScope)
	}
	if filter.Limit <= 0 {
		filter.Limit = 20
	}
	return filter, nil
}

func classifyMemoryCoherenceBand(report MemoryCoherenceScopeReport) (string, []string) {
	reasons := make([]string, 0, 4)
	switch {
	case report.DeadLetterCount > 0:
		reasons = append(reasons, "DEAD_LETTER")
	case report.StaleHitRate >= 0.25 || report.StaleReadRate >= 0.25:
		reasons = append(reasons, "STALE_RATE")
	}
	if report.ReadyInvalidationCount > 0 {
		reasons = append(reasons, "READY_INVALIDATIONS")
	}
	if report.BackoffInvalidationCount > 0 {
		reasons = append(reasons, "BACKOFF_PENDING")
	}
	if report.InvalidatedReplicaCount > 0 {
		reasons = append(reasons, "INVALIDATED_REPLICAS")
	}
	switch {
	case report.DeadLetterCount > 0:
		return "CRITICAL", reasons
	case report.ReadyInvalidationCount > 0 || report.StaleHitRate >= 0.10 || report.StaleReadRate >= 0.10:
		return "DEGRADED", reasons
	case report.OpenInvalidationCount > 0 || report.AckedInvalidationCount > 0 || report.InvalidatedReplicaCount > 0 || report.StaleHitRate > 0 || report.StaleReadRate > 0:
		return "WATCH", reasons
	default:
		return "STABLE", reasons
	}
}

func summarizeMemoryCoherenceScope(report MemoryCoherenceScopeReport) string {
	target := report.AgentID
	if strings.TrimSpace(report.SessionID) != "" {
		target += "/" + report.SessionID
	}
	return fmt.Sprintf(
		"memory coherence %s for %s: %d ready invalidations, %d dead-letter, stale-hit %.2f",
		strings.ToLower(report.CoherenceBandHint),
		target,
		report.ReadyInvalidationCount,
		report.DeadLetterCount,
		report.StaleHitRate,
	)
}

func memoryCoherenceSnapshotPayload(report MemoryCoherenceReport) map[string]any {
	return map[string]any{
		"workspace_id":             report.WorkspaceID,
		"agent_id":                 report.AgentID,
		"session_id":               report.SessionID,
		"report_scope":             report.ReportScope,
		"scope_count":              report.ScopeCount,
		"attention_scope_count":    report.AttentionScopeCount,
		"ready_invalidation_count": report.ReadyInvalidationCount,
		"dead_letter_count":        report.DeadLetterCount,
		"max_stale_hit_rate":       report.MaxStaleHitRate,
		"claim_freshness":          report.ClaimFreshness,
		"needs_attention":          report.NeedsAttention,
		"attention_reasons":        report.AttentionReasons,
		"items":                    report.Items,
		"typed_event_type":         "MEMORY_COHERENCE_SNAPSHOT",
		"summary": fmt.Sprintf(
			"memory coherence snapshot for %s: %d scopes, %d needing attention",
			report.WorkspaceID,
			report.ScopeCount,
			report.AttentionScopeCount,
		),
	}
}

func memoryCoherenceSnapshotEntityID(report MemoryCoherenceReport) string {
	parts := []string{"memorycoh", report.WorkspaceID}
	if report.AgentID != "" {
		parts = append(parts, report.AgentID)
	}
	if report.SessionID != "" {
		parts = append(parts, report.SessionID)
	}
	if report.ReportScope != "" {
		parts = append(parts, report.ReportScope)
	}
	return strings.Join(parts, ":")
}

func latestNonEmptyTimestamp(values ...string) string {
	best := ""
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || value <= best {
			continue
		}
		best = value
	}
	return best
}
