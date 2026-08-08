package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type MemoryMetricsReportInput struct {
	ReportID                string         `json:"report_id,omitempty"`
	WorkspaceID             string         `json:"workspace_id"`
	AgentID                 string         `json:"agent_id"`
	SessionID               string         `json:"session_id,omitempty"`
	ReportScope             string         `json:"report_scope,omitempty"`
	WindowStartedAt         string         `json:"window_started_at,omitempty"`
	WindowEndedAt           string         `json:"window_ended_at,omitempty"`
	LookupCount             int            `json:"lookup_count,omitempty"`
	L1HitCount              int            `json:"l1_hit_count,omitempty"`
	L2HitCount              int            `json:"l2_hit_count,omitempty"`
	P3HitCount              int            `json:"p3_hit_count,omitempty"`
	StaleHitCount           int            `json:"stale_hit_count,omitempty"`
	PromotionCount          int            `json:"promotion_count,omitempty"`
	PromotionReuseCount     int            `json:"promotion_reuse_count,omitempty"`
	FlushCount              int            `json:"flush_count,omitempty"`
	FlushPositiveCount      int            `json:"flush_positive_count,omitempty"`
	LocalConsolidationCount int            `json:"local_consolidation_count,omitempty"`
	PotentialSharedOpCount  int            `json:"potential_shared_op_count,omitempty"`
	DissentHitCount         int            `json:"dissent_hit_count,omitempty"`
	DissentAvailableCount   int            `json:"dissent_available_count,omitempty"`
	PollutionCount          int            `json:"pollution_count,omitempty"`
	Notes                   map[string]any `json:"notes,omitempty"`
}

type MemoryMetricsReportFilter struct {
	WorkspaceID string
	AgentID     string
	SessionID   string
	ReportScope string
	Limit       int
}

type MemoryMetricsReportRecord struct {
	ReportID                string                 `json:"report_id"`
	WorkspaceID             string                 `json:"workspace_id"`
	AgentID                 string                 `json:"agent_id"`
	SessionID               string                 `json:"session_id,omitempty"`
	ReportScope             string                 `json:"report_scope"`
	WindowStartedAt         string                 `json:"window_started_at,omitempty"`
	WindowEndedAt           string                 `json:"window_ended_at,omitempty"`
	LookupCount             int                    `json:"lookup_count"`
	L1HitCount              int                    `json:"l1_hit_count"`
	L2HitCount              int                    `json:"l2_hit_count"`
	P3HitCount              int                    `json:"p3_hit_count"`
	StaleHitCount           int                    `json:"stale_hit_count"`
	PromotionCount          int                    `json:"promotion_count"`
	PromotionReuseCount     int                    `json:"promotion_reuse_count"`
	FlushCount              int                    `json:"flush_count"`
	FlushPositiveCount      int                    `json:"flush_positive_count"`
	LocalConsolidationCount int                    `json:"local_consolidation_count"`
	PotentialSharedOpCount  int                    `json:"potential_shared_op_count"`
	DissentHitCount         int                    `json:"dissent_hit_count"`
	DissentAvailableCount   int                    `json:"dissent_available_count"`
	PollutionCount          int                    `json:"pollution_count"`
	TotalHitCount           int                    `json:"total_hit_count"`
	L1HitRate               float64                `json:"l1_hit_rate"`
	L2HitRate               float64                `json:"l2_hit_rate"`
	P3HitRate               float64                `json:"p3_hit_rate"`
	StaleHitRate            float64                `json:"stale_hit_rate"`
	PromotionPrecision      float64                `json:"promotion_precision"`
	FlushUtility            float64                `json:"flush_utility"`
	OffloadRatio            float64                `json:"offload_ratio"`
	DissentRecall           float64                `json:"dissent_recall"`
	PollutionRate           float64                `json:"pollution_rate"`
	TimeAuthority           WorkspaceTimeAuthority `json:"time_authority"`
	Notes                   map[string]any         `json:"notes,omitempty"`
	CreatedAt               string                 `json:"created_at"`
	UpdatedAt               string                 `json:"updated_at"`
}

type MemoryMetricsReportWriteResult struct {
	Report MemoryMetricsReportRecord `json:"report"`
	Event  RuntimeEventRecord        `json:"event"`
}

func (s *Store) ReportMemoryMetrics(ctx context.Context, input MemoryMetricsReportInput) (MemoryMetricsReportWriteResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return MemoryMetricsReportWriteResult{}, errors.New("workspace_id is required")
	}
	input.AgentID = strings.TrimSpace(input.AgentID)
	if input.AgentID == "" {
		return MemoryMetricsReportWriteResult{}, errors.New("agent_id is required")
	}
	input.ReportID = firstNonEmpty(strings.TrimSpace(input.ReportID), nextID("memmet"))
	input.SessionID = strings.TrimSpace(input.SessionID)
	rawReportScope := strings.TrimSpace(input.ReportScope)
	input.ReportScope = normalizeMemoryResidencyReportScope(input.ReportScope)
	if rawReportScope != "" && input.ReportScope == "" {
		return MemoryMetricsReportWriteResult{}, fmt.Errorf("invalid report_scope: %s", rawReportScope)
	}
	input.ReportScope = firstNonEmpty(input.ReportScope, "AGENT")
	if input.ReportScope == "SESSION" && input.SessionID == "" {
		return MemoryMetricsReportWriteResult{}, errors.New("session_id is required for SESSION report_scope")
	}
	if err := normalizeMemoryMetricsInput(&input); err != nil {
		return MemoryMetricsReportWriteResult{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, input.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return MemoryMetricsReportWriteResult{}, err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return MemoryMetricsReportWriteResult{}, fmt.Errorf("begin memory metrics tx: %w", err)
	}
	var (
		report MemoryMetricsReportRecord
		event  RuntimeEventRecord
	)
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, input.WorkspaceID); err != nil {
			return err
		}
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, input.WorkspaceID, input.AgentID); err != nil {
			return err
		}
		if err := validateMemoryMetricsReportOwnershipTx(ctx, tx, input); err != nil {
			return err
		}
		if err := s.validateMemoryMetricsSessionScopeTx(ctx, tx, input); err != nil {
			return err
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO memory_access_stats(
		    report_id, workspace_id, agent_id, session_id, report_scope,
		    window_started_at, window_ended_at,
		    lookup_count, l1_hit_count, l2_hit_count, p3_hit_count, stale_hit_count,
		    promotion_count, promotion_reuse_count, flush_count, flush_positive_count,
		    local_consolidation_count, potential_shared_op_count,
		    dissent_hit_count, dissent_available_count, pollution_count,
		    notes_json, created_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		  ON CONFLICT(report_id) DO UPDATE SET
		    workspace_id = excluded.workspace_id,
		    agent_id = excluded.agent_id,
		    session_id = excluded.session_id,
		    report_scope = excluded.report_scope,
		    window_started_at = excluded.window_started_at,
		    window_ended_at = excluded.window_ended_at,
		    lookup_count = excluded.lookup_count,
		    l1_hit_count = excluded.l1_hit_count,
		    l2_hit_count = excluded.l2_hit_count,
		    p3_hit_count = excluded.p3_hit_count,
		    stale_hit_count = excluded.stale_hit_count,
		    promotion_count = excluded.promotion_count,
		    promotion_reuse_count = excluded.promotion_reuse_count,
		    flush_count = excluded.flush_count,
		    flush_positive_count = excluded.flush_positive_count,
		    local_consolidation_count = excluded.local_consolidation_count,
		    potential_shared_op_count = excluded.potential_shared_op_count,
		    dissent_hit_count = excluded.dissent_hit_count,
		    dissent_available_count = excluded.dissent_available_count,
		    pollution_count = excluded.pollution_count,
		    notes_json = excluded.notes_json,
		    updated_at = excluded.updated_at`,
			input.ReportID,
			input.WorkspaceID,
			input.AgentID,
			input.SessionID,
			input.ReportScope,
			input.WindowStartedAt,
			input.WindowEndedAt,
			input.LookupCount,
			input.L1HitCount,
			input.L2HitCount,
			input.P3HitCount,
			input.StaleHitCount,
			input.PromotionCount,
			input.PromotionReuseCount,
			input.FlushCount,
			input.FlushPositiveCount,
			input.LocalConsolidationCount,
			input.PotentialSharedOpCount,
			input.DissentHitCount,
			input.DissentAvailableCount,
			input.PollutionCount,
			encodeMemoryResidencyJSONMap(input.Notes),
			now,
			now,
		); err != nil {
			return fmt.Errorf("upsert memory metrics report: %w", err)
		}
		var innerErr error
		event, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: input.WorkspaceID,
			EventType:   "memory.metrics_reported",
			EntityType:  "memory_metrics",
			EntityID:    input.ReportID,
			ActorType:   "agent",
			ActorID:     input.AgentID,
			AgentID:     input.AgentID,
			SessionID:   input.SessionID,
			PayloadJSON: mustJSON(memoryMetricsRuntimeEventPayload(input)),
			CreatedAt:   now,
		})
		if innerErr != nil {
			return fmt.Errorf("append memory metrics runtime event: %w", innerErr)
		}
		report, innerErr = getMemoryMetricsReportTx(ctx, tx, input.WorkspaceID, input.ReportID)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return MemoryMetricsReportWriteResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return MemoryMetricsReportWriteResult{}, fmt.Errorf("commit memory metrics tx: %w", err)
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, input.WorkspaceID)
	if err != nil {
		return MemoryMetricsReportWriteResult{}, err
	}
	report.TimeAuthority = authority
	return MemoryMetricsReportWriteResult{Report: report, Event: event}, nil
}

func (s *Store) ListMemoryMetricsReports(ctx context.Context, filter MemoryMetricsReportFilter) ([]MemoryMetricsReportRecord, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	rawReportScope := strings.TrimSpace(filter.ReportScope)
	filter.ReportScope = normalizeMemoryResidencyReportScope(filter.ReportScope)
	if rawReportScope != "" && filter.ReportScope == "" {
		return nil, fmt.Errorf("invalid report_scope: %s", rawReportScope)
	}
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	query := strings.Builder{}
	query.WriteString(`SELECT report_id, workspace_id, agent_id, session_id, report_scope,
	        window_started_at, window_ended_at,
	        lookup_count, l1_hit_count, l2_hit_count, p3_hit_count, stale_hit_count,
	        promotion_count, promotion_reuse_count, flush_count, flush_positive_count,
	        local_consolidation_count, potential_shared_op_count,
	        dissent_hit_count, dissent_available_count, pollution_count, notes_json, created_at, updated_at
	   FROM memory_access_stats
	  WHERE workspace_id = ?`)
	args := []any{filter.WorkspaceID}
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
	query.WriteString(` ORDER BY updated_at DESC, report_id DESC LIMIT ?`)
	args = append(args, filter.Limit)
	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list memory metrics reports: %w", err)
	}
	defer rows.Close()
	items, err := collectMemoryMetricsReportRows(rows)
	if err != nil {
		return nil, err
	}
	return s.withMemoryMetricsTimeAuthority(ctx, filter.WorkspaceID, items)
}

func (s *Store) GetMemoryMetricsReport(ctx context.Context, workspaceID, reportID string) (MemoryMetricsReportRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return MemoryMetricsReportRecord{}, errors.New("workspace_id is required")
	}
	reportID = strings.TrimSpace(reportID)
	if reportID == "" {
		return MemoryMetricsReportRecord{}, errors.New("report_id is required")
	}
	row := s.db.QueryRowContext(
		ctx,
		`SELECT report_id, workspace_id, agent_id, session_id, report_scope,
		        window_started_at, window_ended_at,
		        lookup_count, l1_hit_count, l2_hit_count, p3_hit_count, stale_hit_count,
		        promotion_count, promotion_reuse_count, flush_count, flush_positive_count,
		        local_consolidation_count, potential_shared_op_count,
		        dissent_hit_count, dissent_available_count, pollution_count, notes_json, created_at, updated_at
		   FROM memory_access_stats
		  WHERE workspace_id = ? AND report_id = ?`,
		workspaceID,
		reportID,
	)
	record, err := scanMemoryMetricsReportRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MemoryMetricsReportRecord{}, fmt.Errorf("memory metrics report not found: %s/%s", workspaceID, reportID)
		}
		return MemoryMetricsReportRecord{}, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return MemoryMetricsReportRecord{}, err
	}
	record.TimeAuthority = authority
	return record, nil
}

func (s *Store) withMemoryMetricsTimeAuthority(ctx context.Context, workspaceID string, items []MemoryMetricsReportRecord) ([]MemoryMetricsReportRecord, error) {
	if len(items) == 0 {
		return items, nil
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	for i := range items {
		items[i].TimeAuthority = authority
	}
	return items, nil
}

func getMemoryMetricsReportTx(ctx context.Context, tx *sql.Tx, workspaceID, reportID string) (MemoryMetricsReportRecord, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT report_id, workspace_id, agent_id, session_id, report_scope,
		        window_started_at, window_ended_at,
		        lookup_count, l1_hit_count, l2_hit_count, p3_hit_count, stale_hit_count,
		        promotion_count, promotion_reuse_count, flush_count, flush_positive_count,
		        local_consolidation_count, potential_shared_op_count,
		        dissent_hit_count, dissent_available_count, pollution_count, notes_json, created_at, updated_at
		   FROM memory_access_stats
		  WHERE workspace_id = ? AND report_id = ?`,
		workspaceID,
		reportID,
	)
	record, err := scanMemoryMetricsReportRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MemoryMetricsReportRecord{}, fmt.Errorf("memory metrics report not found: %s/%s", workspaceID, reportID)
		}
		return MemoryMetricsReportRecord{}, err
	}
	return record, nil
}

func collectMemoryMetricsReportRows(rows *sql.Rows) ([]MemoryMetricsReportRecord, error) {
	out := make([]MemoryMetricsReportRecord, 0)
	for rows.Next() {
		record, err := scanMemoryMetricsReportRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory metrics reports: %w", err)
	}
	return out, nil
}

func scanMemoryMetricsReportRecord(scanner interface{ Scan(dest ...any) error }) (MemoryMetricsReportRecord, error) {
	var (
		record    MemoryMetricsReportRecord
		notesJSON string
	)
	if err := scanner.Scan(
		&record.ReportID,
		&record.WorkspaceID,
		&record.AgentID,
		&record.SessionID,
		&record.ReportScope,
		&record.WindowStartedAt,
		&record.WindowEndedAt,
		&record.LookupCount,
		&record.L1HitCount,
		&record.L2HitCount,
		&record.P3HitCount,
		&record.StaleHitCount,
		&record.PromotionCount,
		&record.PromotionReuseCount,
		&record.FlushCount,
		&record.FlushPositiveCount,
		&record.LocalConsolidationCount,
		&record.PotentialSharedOpCount,
		&record.DissentHitCount,
		&record.DissentAvailableCount,
		&record.PollutionCount,
		&notesJSON,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return MemoryMetricsReportRecord{}, fmt.Errorf("scan memory metrics report: %w", err)
	}
	applyDerivedMemoryMetrics(&record)
	record.Notes = decodeMemoryResidencyJSONMap(notesJSON)
	return record, nil
}

func normalizeMemoryMetricsInput(input *MemoryMetricsReportInput) error {
	var err error
	input.WindowStartedAt, err = normalizeMemoryMetricsTimestamp(input.WindowStartedAt)
	if err != nil {
		return err
	}
	input.WindowEndedAt, err = normalizeMemoryMetricsTimestamp(input.WindowEndedAt)
	if err != nil {
		return err
	}
	if input.WindowStartedAt != "" && input.WindowEndedAt != "" && input.WindowStartedAt > input.WindowEndedAt {
		return errors.New("window_started_at must be <= window_ended_at")
	}
	counts := []*int{
		&input.LookupCount,
		&input.L1HitCount,
		&input.L2HitCount,
		&input.P3HitCount,
		&input.StaleHitCount,
		&input.PromotionCount,
		&input.PromotionReuseCount,
		&input.FlushCount,
		&input.FlushPositiveCount,
		&input.LocalConsolidationCount,
		&input.PotentialSharedOpCount,
		&input.DissentHitCount,
		&input.DissentAvailableCount,
		&input.PollutionCount,
	}
	for _, count := range counts {
		if *count < 0 {
			return errors.New("memory metrics counts must be >= 0")
		}
	}
	if input.L1HitCount > input.LookupCount || input.L2HitCount > input.LookupCount || input.P3HitCount > input.LookupCount {
		return errors.New("hit counts cannot exceed lookup_count")
	}
	totalHits := input.L1HitCount + input.L2HitCount + input.P3HitCount
	if totalHits > input.LookupCount {
		return errors.New("total hit count cannot exceed lookup_count")
	}
	if input.StaleHitCount > totalHits {
		return errors.New("stale_hit_count cannot exceed total hit count")
	}
	if input.PromotionReuseCount > input.PromotionCount {
		return errors.New("promotion_reuse_count cannot exceed promotion_count")
	}
	if input.FlushPositiveCount > input.FlushCount {
		return errors.New("flush_positive_count cannot exceed flush_count")
	}
	return nil
}

func normalizeMemoryMetricsTimestamp(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return "", fmt.Errorf("invalid metrics timestamp: %s", raw)
	}
	return parsed.UTC().Format(time.RFC3339Nano), nil
}

func validateMemoryMetricsReportOwnershipTx(ctx context.Context, tx *sql.Tx, input MemoryMetricsReportInput) error {
	row := tx.QueryRowContext(
		ctx,
		`SELECT workspace_id, agent_id, session_id, report_scope
		   FROM memory_access_stats
		  WHERE report_id = ?`,
		input.ReportID,
	)
	var existingWorkspaceID, existingAgentID, existingSessionID, existingReportScope string
	if err := row.Scan(&existingWorkspaceID, &existingAgentID, &existingSessionID, &existingReportScope); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("query memory metrics report ownership: %w", err)
	}
	if strings.TrimSpace(existingWorkspaceID) == input.WorkspaceID &&
		strings.TrimSpace(existingAgentID) == input.AgentID &&
		strings.TrimSpace(existingSessionID) == input.SessionID &&
		strings.TrimSpace(existingReportScope) == input.ReportScope {
		return nil
	}
	return errors.New("report_id already belongs to another memory metrics owner")
}

func (s *Store) validateMemoryMetricsSessionScopeTx(ctx context.Context, tx *sql.Tx, input MemoryMetricsReportInput) error {
	if input.ReportScope != "SESSION" {
		return nil
	}
	if err := s.ensureAgentSessionInWorkspaceTx(ctx, tx, input.WorkspaceID, input.SessionID); err != nil {
		return err
	}
	row := tx.QueryRowContext(ctx, `SELECT agent_id FROM agent_sessions WHERE session_id = ?`, input.SessionID)
	var sessionAgentID string
	if err := row.Scan(&sessionAgentID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSessionNotFound
		}
		return fmt.Errorf("query agent session owner: %w", err)
	}
	if strings.TrimSpace(sessionAgentID) != input.AgentID {
		return errors.New("session_id belongs to another agent")
	}
	return nil
}

func applyDerivedMemoryMetrics(record *MemoryMetricsReportRecord) {
	record.TotalHitCount = record.L1HitCount + record.L2HitCount + record.P3HitCount
	record.L1HitRate = clampUnitInterval(safeDivideFloat64(record.L1HitCount, record.LookupCount))
	record.L2HitRate = clampUnitInterval(safeDivideFloat64(record.L2HitCount, memoryMetricsMaxInt(record.LookupCount-record.L1HitCount, 0)))
	record.P3HitRate = clampUnitInterval(safeDivideFloat64(record.P3HitCount, memoryMetricsMaxInt(record.LookupCount-record.L1HitCount-record.L2HitCount, 0)))
	record.StaleHitRate = clampUnitInterval(safeDivideFloat64(record.StaleHitCount, record.TotalHitCount))
	record.PromotionPrecision = clampUnitInterval(safeDivideFloat64(record.PromotionReuseCount, record.PromotionCount))
	record.FlushUtility = clampUnitInterval(safeDivideFloat64(record.FlushPositiveCount, record.FlushCount))
	record.OffloadRatio = clampUnitInterval(safeDivideFloat64(record.TotalHitCount+record.LocalConsolidationCount, record.PotentialSharedOpCount))
	record.DissentRecall = clampUnitInterval(safeDivideFloat64(record.DissentHitCount, memoryMetricsMaxInt(record.DissentAvailableCount, 1)))
	record.PollutionRate = clampUnitInterval(safeDivideFloat64(record.PollutionCount, memoryMetricsMaxInt(record.TotalHitCount, record.LookupCount)))
}

func safeDivideFloat64(numerator, denominator int) float64 {
	if denominator <= 0 || numerator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func memoryMetricsMaxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func memoryMetricsRuntimeEventPayload(input MemoryMetricsReportInput) map[string]any {
	record := MemoryMetricsReportRecord{
		ReportID:                input.ReportID,
		WorkspaceID:             input.WorkspaceID,
		AgentID:                 input.AgentID,
		SessionID:               input.SessionID,
		ReportScope:             input.ReportScope,
		WindowStartedAt:         input.WindowStartedAt,
		WindowEndedAt:           input.WindowEndedAt,
		LookupCount:             input.LookupCount,
		L1HitCount:              input.L1HitCount,
		L2HitCount:              input.L2HitCount,
		P3HitCount:              input.P3HitCount,
		StaleHitCount:           input.StaleHitCount,
		PromotionCount:          input.PromotionCount,
		PromotionReuseCount:     input.PromotionReuseCount,
		FlushCount:              input.FlushCount,
		FlushPositiveCount:      input.FlushPositiveCount,
		LocalConsolidationCount: input.LocalConsolidationCount,
		PotentialSharedOpCount:  input.PotentialSharedOpCount,
		DissentHitCount:         input.DissentHitCount,
		DissentAvailableCount:   input.DissentAvailableCount,
		PollutionCount:          input.PollutionCount,
	}
	applyDerivedMemoryMetrics(&record)
	return map[string]any{
		"typed_event_type":          "MEMORY_METRICS_REPORT",
		"report_id":                 record.ReportID,
		"workspace_id":              record.WorkspaceID,
		"agent_id":                  record.AgentID,
		"session_id":                record.SessionID,
		"report_scope":              record.ReportScope,
		"window_started_at":         record.WindowStartedAt,
		"window_ended_at":           record.WindowEndedAt,
		"lookup_count":              record.LookupCount,
		"l1_hit_count":              record.L1HitCount,
		"l2_hit_count":              record.L2HitCount,
		"p3_hit_count":              record.P3HitCount,
		"stale_hit_count":           record.StaleHitCount,
		"promotion_count":           record.PromotionCount,
		"promotion_reuse_count":     record.PromotionReuseCount,
		"flush_count":               record.FlushCount,
		"flush_positive_count":      record.FlushPositiveCount,
		"local_consolidation_count": record.LocalConsolidationCount,
		"potential_shared_op_count": record.PotentialSharedOpCount,
		"total_hit_count":           record.TotalHitCount,
		"l1_hit_rate":               record.L1HitRate,
		"l2_hit_rate":               record.L2HitRate,
		"p3_hit_rate":               record.P3HitRate,
		"stale_hit_rate":            record.StaleHitRate,
		"promotion_precision":       record.PromotionPrecision,
		"flush_utility":             record.FlushUtility,
		"offload_ratio":             record.OffloadRatio,
		"dissent_hit_count":         record.DissentHitCount,
		"dissent_available_count":   record.DissentAvailableCount,
		"pollution_count":           record.PollutionCount,
		"dissent_recall":            record.DissentRecall,
		"pollution_rate":            record.PollutionRate,
		"notes":                     input.Notes,
	}
}
