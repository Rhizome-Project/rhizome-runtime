package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type MemoryResidencyVersionGuard struct {
	RefKind      string  `json:"ref_kind"`
	RefID        string  `json:"ref_id"`
	VersionToken string  `json:"version_token,omitempty"`
	Weight       float64 `json:"weight"`
	State        string  `json:"state,omitempty"`
}

type MemoryReplicaStateInput struct {
	ResidencyTier     string                        `json:"residency_tier"`
	ReplicaKind       string                        `json:"replica_kind"`
	CoherenceClass    string                        `json:"coherence_class"`
	State             string                        `json:"state"`
	CanonicalMemoryID string                        `json:"canonical_memory_id,omitempty"`
	CacheKey          string                        `json:"cache_key,omitempty"`
	SourceKind        string                        `json:"source_kind,omitempty"`
	SourceID          string                        `json:"source_id,omitempty"`
	VersionGuards     []MemoryResidencyVersionGuard `json:"version_guards,omitempty"`
	HitCount          int                           `json:"hit_count,omitempty"`
	StaleRefCount     int                           `json:"stale_ref_count,omitempty"`
	LastAccessedAt    string                        `json:"last_accessed_at,omitempty"`
	LastValidatedAt   string                        `json:"last_validated_at,omitempty"`
	Metadata          map[string]any                `json:"metadata,omitempty"`
}

type MemoryResidencyReportInput struct {
	ReportID          string                    `json:"report_id,omitempty"`
	WorkspaceID       string                    `json:"workspace_id"`
	AgentID           string                    `json:"agent_id"`
	SessionID         string                    `json:"session_id,omitempty"`
	ReportScope       string                    `json:"report_scope,omitempty"`
	P1EntryCount      int                       `json:"p1_entry_count,omitempty"`
	P2EntryCount      int                       `json:"p2_entry_count,omitempty"`
	P3EntryCount      int                       `json:"p3_entry_count,omitempty"`
	HotHitRate        float64                   `json:"hot_hit_rate,omitempty"`
	PersistentHitRate float64                   `json:"persistent_hit_rate,omitempty"`
	ClusterHitRate    float64                   `json:"cluster_hit_rate,omitempty"`
	StaleReadRate     float64                   `json:"stale_read_rate,omitempty"`
	OffloadRatio      float64                   `json:"offload_ratio,omitempty"`
	Notes             map[string]any            `json:"notes,omitempty"`
	Replicas          []MemoryReplicaStateInput `json:"replicas,omitempty"`
}

type MemoryResidencyReportFilter struct {
	WorkspaceID string
	AgentID     string
	SessionID   string
	ReportScope string
	Limit       int
}

type MemoryResidencyReportRecord struct {
	ReportID                string         `json:"report_id"`
	WorkspaceID             string         `json:"workspace_id"`
	AgentID                 string         `json:"agent_id"`
	SessionID               string         `json:"session_id,omitempty"`
	ReportScope             string         `json:"report_scope"`
	P1EntryCount            int            `json:"p1_entry_count"`
	P2EntryCount            int            `json:"p2_entry_count"`
	P3EntryCount            int            `json:"p3_entry_count"`
	HotHitRate              float64        `json:"hot_hit_rate"`
	PersistentHitRate       float64        `json:"persistent_hit_rate"`
	ClusterHitRate          float64        `json:"cluster_hit_rate"`
	StaleReadRate           float64        `json:"stale_read_rate"`
	OffloadRatio            float64        `json:"offload_ratio"`
	InvalidatedReplicaCount int            `json:"invalidated_replica_count"`
	ReplicaCount            int            `json:"replica_count"`
	Notes                   map[string]any `json:"notes,omitempty"`
	CreatedAt               string         `json:"created_at"`
	UpdatedAt               string         `json:"updated_at"`
}

type MemoryReplicaStateRecord struct {
	ReplicaStateID    string                        `json:"replica_state_id"`
	ReportID          string                        `json:"report_id"`
	WorkspaceID       string                        `json:"workspace_id"`
	AgentID           string                        `json:"agent_id"`
	ResidencyTier     string                        `json:"residency_tier"`
	ReplicaKind       string                        `json:"replica_kind"`
	CoherenceClass    string                        `json:"coherence_class"`
	State             string                        `json:"state"`
	CanonicalMemoryID string                        `json:"canonical_memory_id,omitempty"`
	CacheKey          string                        `json:"cache_key,omitempty"`
	SourceKind        string                        `json:"source_kind,omitempty"`
	SourceID          string                        `json:"source_id,omitempty"`
	VersionGuards     []MemoryResidencyVersionGuard `json:"version_guards,omitempty"`
	HitCount          int                           `json:"hit_count"`
	StaleRefCount     int                           `json:"stale_ref_count"`
	LastAccessedAt    string                        `json:"last_accessed_at,omitempty"`
	LastValidatedAt   string                        `json:"last_validated_at,omitempty"`
	Metadata          map[string]any                `json:"metadata,omitempty"`
	CreatedAt         string                        `json:"created_at"`
	UpdatedAt         string                        `json:"updated_at"`
}

type MemoryResidencyReportDetail struct {
	TimeAuthority WorkspaceTimeAuthority      `json:"time_authority"`
	Report        MemoryResidencyReportRecord `json:"report"`
	Replicas      []MemoryReplicaStateRecord  `json:"replicas,omitempty"`
}

type MemoryResidencyReportWriteResult struct {
	Report             MemoryResidencyReportDetail `json:"report"`
	Event              RuntimeEventRecord          `json:"event"`
	InvalidationEvents []RuntimeEventRecord        `json:"invalidation_events,omitempty"`
}

func (s *Store) ReportMemoryResidency(ctx context.Context, input MemoryResidencyReportInput) (MemoryResidencyReportWriteResult, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return MemoryResidencyReportWriteResult{}, errors.New("workspace_id is required")
	}
	input.AgentID = strings.TrimSpace(input.AgentID)
	if input.AgentID == "" {
		return MemoryResidencyReportWriteResult{}, errors.New("agent_id is required")
	}
	input.ReportID = firstNonEmpty(strings.TrimSpace(input.ReportID), nextID("memres"))
	input.SessionID = strings.TrimSpace(input.SessionID)
	rawReportScope := strings.TrimSpace(input.ReportScope)
	input.ReportScope = normalizeMemoryResidencyReportScope(input.ReportScope)
	if rawReportScope != "" && input.ReportScope == "" {
		return MemoryResidencyReportWriteResult{}, fmt.Errorf("invalid report_scope: %s", rawReportScope)
	}
	input.ReportScope = firstNonEmpty(input.ReportScope, "AGENT")
	if input.ReportScope == "SESSION" && input.SessionID == "" {
		return MemoryResidencyReportWriteResult{}, errors.New("session_id is required for SESSION report_scope")
	}
	replicas, invalidatedCount, tierCounts, err := normalizeMemoryReplicaStateInputs(input.WorkspaceID, input.Replicas)
	if err != nil {
		return MemoryResidencyReportWriteResult{}, err
	}
	input.Replicas = replicas
	if input.P1EntryCount < tierCounts["P1"] {
		input.P1EntryCount = tierCounts["P1"]
	}
	if input.P2EntryCount < tierCounts["P2"] {
		input.P2EntryCount = tierCounts["P2"]
	}
	if input.P3EntryCount < tierCounts["P3"] {
		input.P3EntryCount = tierCounts["P3"]
	}
	input.HotHitRate = clampUnitInterval(input.HotHitRate)
	input.PersistentHitRate = clampUnitInterval(input.PersistentHitRate)
	input.ClusterHitRate = clampUnitInterval(input.ClusterHitRate)
	input.StaleReadRate = clampUnitInterval(input.StaleReadRate)
	input.OffloadRatio = clampUnitInterval(input.OffloadRatio)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, input.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return MemoryResidencyReportWriteResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return MemoryResidencyReportWriteResult{}, fmt.Errorf("begin memory residency tx: %w", err)
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, input.WorkspaceID); err != nil {
		_ = tx.Rollback()
		return MemoryResidencyReportWriteResult{}, err
	}
	if err := s.ensureAgentInWorkspaceTx(ctx, tx, input.WorkspaceID, input.AgentID); err != nil {
		_ = tx.Rollback()
		return MemoryResidencyReportWriteResult{}, err
	}
	if err := s.validateMemoryResidencySessionScopeTx(ctx, tx, input); err != nil {
		_ = tx.Rollback()
		return MemoryResidencyReportWriteResult{}, err
	}
	if err := validateMemoryResidencyReportOwnershipTx(ctx, tx, input); err != nil {
		_ = tx.Rollback()
		return MemoryResidencyReportWriteResult{}, err
	}
	var (
		invalidationEvents []RuntimeEventRecord
		event              RuntimeEventRecord
		detail             MemoryResidencyReportDetail
	)
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO memory_residency_reports(
		    report_id, workspace_id, agent_id, session_id, report_scope,
		    p1_entry_count, p2_entry_count, p3_entry_count,
		    hot_hit_rate, persistent_hit_rate, cluster_hit_rate, stale_read_rate, offload_ratio,
		    invalidated_replica_count, notes_json, created_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		  ON CONFLICT(report_id) DO UPDATE SET
		    workspace_id = excluded.workspace_id,
		    agent_id = excluded.agent_id,
		    session_id = excluded.session_id,
		    report_scope = excluded.report_scope,
		    p1_entry_count = excluded.p1_entry_count,
		    p2_entry_count = excluded.p2_entry_count,
		    p3_entry_count = excluded.p3_entry_count,
		    hot_hit_rate = excluded.hot_hit_rate,
		    persistent_hit_rate = excluded.persistent_hit_rate,
		    cluster_hit_rate = excluded.cluster_hit_rate,
		    stale_read_rate = excluded.stale_read_rate,
		    offload_ratio = excluded.offload_ratio,
		    invalidated_replica_count = excluded.invalidated_replica_count,
		    notes_json = excluded.notes_json,
		    updated_at = excluded.updated_at`,
			input.ReportID,
			input.WorkspaceID,
			input.AgentID,
			input.SessionID,
			input.ReportScope,
			input.P1EntryCount,
			input.P2EntryCount,
			input.P3EntryCount,
			input.HotHitRate,
			input.PersistentHitRate,
			input.ClusterHitRate,
			input.StaleReadRate,
			input.OffloadRatio,
			invalidatedCount,
			encodeMemoryResidencyJSONMap(input.Notes),
			now,
			now,
		); err != nil {
			return fmt.Errorf("upsert memory residency report: %w", err)
		}
		if err := syncMemoryReplicaStatesTx(ctx, tx, input, now); err != nil {
			return err
		}
		_, invalidationEvents, err = s.enqueueMemoryInvalidationsForResidencyReportTx(ctx, tx, authority, input)
		if err != nil {
			return fmt.Errorf("enqueue memory invalidations for residency report: %w", err)
		}
		event, err = s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: input.WorkspaceID,
			EventType:   "memory.residency_reported",
			EntityType:  "memory_residency",
			EntityID:    input.ReportID,
			ActorType:   "agent",
			ActorID:     input.AgentID,
			PayloadJSON: mustJSON(memoryResidencyRuntimeEventPayload(input, invalidatedCount, len(input.Replicas))),
			CreatedAt:   now,
		})
		if err != nil {
			return fmt.Errorf("append memory residency runtime event: %w", err)
		}
		detail, err = s.getMemoryResidencyReportTx(ctx, tx, input.WorkspaceID, input.ReportID)
		return err
	}); err != nil {
		_ = tx.Rollback()
		return MemoryResidencyReportWriteResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return MemoryResidencyReportWriteResult{}, fmt.Errorf("commit memory residency tx: %w", err)
	}
	return MemoryResidencyReportWriteResult{Report: detail, Event: event, InvalidationEvents: invalidationEvents}, nil
}

func (s *Store) ListMemoryResidencyReports(ctx context.Context, filter MemoryResidencyReportFilter) ([]MemoryResidencyReportRecord, error) {
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
	        p1_entry_count, p2_entry_count, p3_entry_count,
	        hot_hit_rate, persistent_hit_rate, cluster_hit_rate, stale_read_rate, offload_ratio,
	        invalidated_replica_count, notes_json,
	        (SELECT COUNT(1) FROM memory_replica_states rs WHERE rs.report_id = rr.report_id) AS replica_count,
	        created_at, updated_at
	   FROM memory_residency_reports rr
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
		return nil, fmt.Errorf("list memory residency reports: %w", err)
	}
	defer rows.Close()
	return collectMemoryResidencyReportRows(rows)
}

func (s *Store) GetMemoryResidencyReport(ctx context.Context, workspaceID, reportID string) (MemoryResidencyReportDetail, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return MemoryResidencyReportDetail{}, errors.New("workspace_id is required")
	}
	reportID = strings.TrimSpace(reportID)
	if reportID == "" {
		return MemoryResidencyReportDetail{}, errors.New("report_id is required")
	}
	return s.getMemoryResidencyReport(ctx, workspaceID, reportID)
}

func (s *Store) getMemoryResidencyReport(ctx context.Context, workspaceID, reportID string) (MemoryResidencyReportDetail, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT report_id, workspace_id, agent_id, session_id, report_scope,
		        p1_entry_count, p2_entry_count, p3_entry_count,
		        hot_hit_rate, persistent_hit_rate, cluster_hit_rate, stale_read_rate, offload_ratio,
		        invalidated_replica_count, notes_json,
		        (SELECT COUNT(1) FROM memory_replica_states rs WHERE rs.report_id = rr.report_id) AS replica_count,
		        created_at, updated_at
		   FROM memory_residency_reports rr
		  WHERE workspace_id = ? AND report_id = ?`,
		workspaceID,
		reportID,
	)
	report, err := scanMemoryResidencyReportRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MemoryResidencyReportDetail{}, fmt.Errorf("memory residency report not found: %s/%s", workspaceID, reportID)
		}
		return MemoryResidencyReportDetail{}, err
	}
	replicas, err := s.listMemoryReplicaStates(ctx, reportID)
	if err != nil {
		return MemoryResidencyReportDetail{}, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return MemoryResidencyReportDetail{}, err
	}
	return MemoryResidencyReportDetail{TimeAuthority: authority, Report: report, Replicas: replicas}, nil
}

func (s *Store) getMemoryResidencyReportTx(ctx context.Context, tx *sql.Tx, workspaceID, reportID string) (MemoryResidencyReportDetail, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT report_id, workspace_id, agent_id, session_id, report_scope,
		        p1_entry_count, p2_entry_count, p3_entry_count,
		        hot_hit_rate, persistent_hit_rate, cluster_hit_rate, stale_read_rate, offload_ratio,
		        invalidated_replica_count, notes_json,
		        (SELECT COUNT(1) FROM memory_replica_states rs WHERE rs.report_id = rr.report_id) AS replica_count,
		        created_at, updated_at
		   FROM memory_residency_reports rr
		  WHERE workspace_id = ? AND report_id = ?`,
		workspaceID,
		reportID,
	)
	report, err := scanMemoryResidencyReportRecord(row)
	if err != nil {
		return MemoryResidencyReportDetail{}, err
	}
	replicas, err := listMemoryReplicaStatesTx(ctx, tx, reportID)
	if err != nil {
		return MemoryResidencyReportDetail{}, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return MemoryResidencyReportDetail{}, err
	}
	return MemoryResidencyReportDetail{TimeAuthority: authority, Report: report, Replicas: replicas}, nil
}

func (s *Store) listMemoryReplicaStates(ctx context.Context, reportID string) ([]MemoryReplicaStateRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT replica_state_id, report_id, workspace_id, agent_id, residency_tier, replica_kind, coherence_class, state,
		        canonical_memory_id, cache_key, source_kind, source_id, version_guard_json,
		        hit_count, stale_ref_count, last_accessed_at, last_validated_at, metadata_json, created_at, updated_at
		   FROM memory_replica_states
		  WHERE report_id = ?
		  ORDER BY residency_tier, replica_kind, canonical_memory_id, cache_key, replica_state_id`,
		reportID,
	)
	if err != nil {
		return nil, fmt.Errorf("list memory replica states: %w", err)
	}
	defer rows.Close()
	return collectMemoryReplicaStateRows(rows)
}

func listMemoryReplicaStatesTx(ctx context.Context, tx *sql.Tx, reportID string) ([]MemoryReplicaStateRecord, error) {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT replica_state_id, report_id, workspace_id, agent_id, residency_tier, replica_kind, coherence_class, state,
		        canonical_memory_id, cache_key, source_kind, source_id, version_guard_json,
		        hit_count, stale_ref_count, last_accessed_at, last_validated_at, metadata_json, created_at, updated_at
		   FROM memory_replica_states
		  WHERE report_id = ?
		  ORDER BY residency_tier, replica_kind, canonical_memory_id, cache_key, replica_state_id`,
		reportID,
	)
	if err != nil {
		return nil, fmt.Errorf("list memory replica states: %w", err)
	}
	defer rows.Close()
	return collectMemoryReplicaStateRows(rows)
}

func collectMemoryResidencyReportRows(rows *sql.Rows) ([]MemoryResidencyReportRecord, error) {
	out := make([]MemoryResidencyReportRecord, 0)
	for rows.Next() {
		record, err := scanMemoryResidencyReportRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory residency reports: %w", err)
	}
	return out, nil
}

func scanMemoryResidencyReportRecord(scanner interface{ Scan(dest ...any) error }) (MemoryResidencyReportRecord, error) {
	var (
		record    MemoryResidencyReportRecord
		notesJSON string
	)
	if err := scanner.Scan(
		&record.ReportID,
		&record.WorkspaceID,
		&record.AgentID,
		&record.SessionID,
		&record.ReportScope,
		&record.P1EntryCount,
		&record.P2EntryCount,
		&record.P3EntryCount,
		&record.HotHitRate,
		&record.PersistentHitRate,
		&record.ClusterHitRate,
		&record.StaleReadRate,
		&record.OffloadRatio,
		&record.InvalidatedReplicaCount,
		&notesJSON,
		&record.ReplicaCount,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return MemoryResidencyReportRecord{}, err
	}
	record.Notes = decodeMemoryResidencyJSONMap(notesJSON)
	return record, nil
}

func collectMemoryReplicaStateRows(rows *sql.Rows) ([]MemoryReplicaStateRecord, error) {
	out := make([]MemoryReplicaStateRecord, 0)
	for rows.Next() {
		record, err := scanMemoryReplicaStateRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory replica states: %w", err)
	}
	return out, nil
}

func scanMemoryReplicaStateRecord(scanner interface{ Scan(dest ...any) error }) (MemoryReplicaStateRecord, error) {
	var (
		record           MemoryReplicaStateRecord
		versionGuardJSON string
		metadataJSON     string
	)
	if err := scanner.Scan(
		&record.ReplicaStateID,
		&record.ReportID,
		&record.WorkspaceID,
		&record.AgentID,
		&record.ResidencyTier,
		&record.ReplicaKind,
		&record.CoherenceClass,
		&record.State,
		&record.CanonicalMemoryID,
		&record.CacheKey,
		&record.SourceKind,
		&record.SourceID,
		&versionGuardJSON,
		&record.HitCount,
		&record.StaleRefCount,
		&record.LastAccessedAt,
		&record.LastValidatedAt,
		&metadataJSON,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return MemoryReplicaStateRecord{}, err
	}
	record.VersionGuards = decodeMemoryResidencyVersionGuards(versionGuardJSON)
	record.Metadata = decodeMemoryResidencyJSONMap(metadataJSON)
	return record, nil
}

func normalizeMemoryReplicaStateInputs(workspaceID string, items []MemoryReplicaStateInput) ([]MemoryReplicaStateInput, int, map[string]int, error) {
	if len(items) == 0 {
		return nil, 0, map[string]int{"P1": 0, "P2": 0, "P3": 0}, nil
	}
	index := make(map[string]MemoryReplicaStateInput, len(items))
	tierCounts := map[string]int{"P1": 0, "P2": 0, "P3": 0}
	invalidatedCount := 0
	for _, item := range items {
		item.ResidencyTier = normalizeMemoryResidencyTier(item.ResidencyTier)
		if item.ResidencyTier == "" {
			return nil, 0, nil, fmt.Errorf("invalid residency_tier: %s", item.ResidencyTier)
		}
		item.ReplicaKind = normalizeMemoryReplicaKind(item.ReplicaKind)
		if item.ReplicaKind == "" {
			return nil, 0, nil, errors.New("replica_kind is required")
		}
		item.CoherenceClass = normalizeMemoryCoherenceClass(item.CoherenceClass)
		if item.CoherenceClass == "" {
			return nil, 0, nil, fmt.Errorf("invalid coherence_class for %s", item.ReplicaKind)
		}
		item.State = normalizeMemoryReplicaLifecycleState(item.State)
		if item.State == "" {
			return nil, 0, nil, fmt.Errorf("invalid state for %s", item.ReplicaKind)
		}
		item.CanonicalMemoryID = strings.TrimSpace(item.CanonicalMemoryID)
		item.CacheKey = strings.TrimSpace(item.CacheKey)
		item.SourceKind = strings.TrimSpace(item.SourceKind)
		item.SourceID = strings.TrimSpace(item.SourceID)
		item.HitCount = maxInt(item.HitCount, 0)
		item.StaleRefCount = maxInt(item.StaleRefCount, memoryResidencyStaleGuardCount(item.VersionGuards))
		for i := range item.VersionGuards {
			rawRefKind := strings.TrimSpace(item.VersionGuards[i].RefKind)
			item.VersionGuards[i].RefKind = normalizeMemoryInvalidationRefKind(item.VersionGuards[i].RefKind)
			if rawRefKind != "" && item.VersionGuards[i].RefKind == "" {
				return nil, 0, nil, fmt.Errorf("invalid version_guard.ref_kind: %s", rawRefKind)
			}
			item.VersionGuards[i].RefID = strings.TrimSpace(item.VersionGuards[i].RefID)
			item.VersionGuards[i].VersionToken = strings.TrimSpace(item.VersionGuards[i].VersionToken)
			rawState := strings.TrimSpace(item.VersionGuards[i].State)
			item.VersionGuards[i].State = normalizeMemoryVersionGuardState(item.VersionGuards[i].State)
			if rawState != "" && item.VersionGuards[i].State == "" {
				return nil, 0, nil, fmt.Errorf("invalid version_guard.state: %s", rawState)
			}
			item.VersionGuards[i].Weight = clampUnitInterval(item.VersionGuards[i].Weight)
		}
		item.VersionGuards = dedupeMemoryVersionGuards(workspaceID, item.VersionGuards)
		sort.Slice(item.VersionGuards, func(i, j int) bool {
			left := item.VersionGuards[i]
			right := item.VersionGuards[j]
			if left.RefKind != right.RefKind {
				return left.RefKind < right.RefKind
			}
			if left.RefID != right.RefID {
				return left.RefID < right.RefID
			}
			if left.VersionToken != right.VersionToken {
				return left.VersionToken < right.VersionToken
			}
			if left.State != right.State {
				return left.State < right.State
			}
			return left.Weight < right.Weight
		})
		key := memoryReplicaStateIdentityKey(item)
		if existing, ok := index[key]; ok {
			if item.HitCount > existing.HitCount {
				existing.HitCount = item.HitCount
			}
			if item.StaleRefCount > existing.StaleRefCount {
				existing.StaleRefCount = item.StaleRefCount
			}
			existing.LastAccessedAt = firstNonEmpty(strings.TrimSpace(item.LastAccessedAt), strings.TrimSpace(existing.LastAccessedAt))
			existing.LastValidatedAt = firstNonEmpty(strings.TrimSpace(item.LastValidatedAt), strings.TrimSpace(existing.LastValidatedAt))
			if existing.State != "INVALIDATED" {
				existing.State = item.State
			}
			if len(existing.VersionGuards) == 0 && len(item.VersionGuards) > 0 {
				existing.VersionGuards = item.VersionGuards
			}
			if len(existing.Metadata) == 0 && len(item.Metadata) > 0 {
				existing.Metadata = item.Metadata
			}
			index[key] = existing
			continue
		}
		index[key] = item
	}
	out := make([]MemoryReplicaStateInput, 0, len(index))
	for _, item := range index {
		out = append(out, item)
		tierCounts[item.ResidencyTier]++
		if item.State == "INVALIDATED" {
			invalidatedCount++
		}
	}
	sort.Slice(out, func(i, j int) bool {
		leftKey := memoryReplicaStateIdentityKey(out[i])
		rightKey := memoryReplicaStateIdentityKey(out[j])
		if leftKey != rightKey {
			return leftKey < rightKey
		}
		if out[i].CoherenceClass != out[j].CoherenceClass {
			return out[i].CoherenceClass < out[j].CoherenceClass
		}
		return out[i].State < out[j].State
	})
	return out, invalidatedCount, tierCounts, nil
}

func normalizeMemoryResidencyTier(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "P1", "P2", "P3":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return ""
	}
}

func normalizeMemoryCoherenceClass(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "A", "B", "C", "D":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return ""
	}
}

func normalizeMemoryResidencyReportScope(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", "AGENT":
		return "AGENT"
	case "SESSION", "CLUSTER":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return ""
	}
}

func normalizeMemoryReplicaKind(raw string) string {
	return strings.ToUpper(strings.TrimSpace(raw))
}

func normalizeMemoryReplicaLifecycleState(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", "CURRENT":
		return "CURRENT"
	case "STALE", "INVALIDATED", "EVICTED":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return ""
	}
}

func normalizeMemoryVersionGuardState(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", "CURRENT":
		return "CURRENT"
	case "STALE", "MISSING_SOURCE", "UNRESOLVED", "INVALIDATED":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return ""
	}
}

func memoryResidencyStaleGuardCount(items []MemoryResidencyVersionGuard) int {
	count := 0
	for _, item := range items {
		switch normalizeMemoryVersionGuardState(item.State) {
		case "STALE", "MISSING_SOURCE", "INVALIDATED":
			count++
		}
	}
	return count
}

func encodeMemoryResidencyJSONMap(values map[string]any) string {
	if len(values) == 0 {
		return "{}"
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func decodeMemoryResidencyJSONMap(raw string) map[string]any {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return map[string]any{}
	}
	out := map[string]any{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return map[string]any{}
	}
	return out
}

func encodeMemoryResidencyVersionGuards(items []MemoryResidencyVersionGuard) string {
	if len(items) == 0 {
		return "[]"
	}
	raw, err := json.Marshal(items)
	if err != nil {
		return "[]"
	}
	return string(raw)
}

func decodeMemoryResidencyVersionGuards(raw string) []MemoryResidencyVersionGuard {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return nil
	}
	var out []MemoryResidencyVersionGuard
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}

func memoryResidencyRuntimeEventPayload(input MemoryResidencyReportInput, invalidatedCount, replicaCount int) map[string]any {
	summary := fmt.Sprintf(
		"memory residency report from %s (P1=%d P2=%d P3=%d, replicas=%d)",
		strings.TrimSpace(input.AgentID),
		input.P1EntryCount,
		input.P2EntryCount,
		input.P3EntryCount,
		replicaCount,
	)
	return map[string]any{
		"workspace_id":              strings.TrimSpace(input.WorkspaceID),
		"report_id":                 strings.TrimSpace(input.ReportID),
		"agent_id":                  strings.TrimSpace(input.AgentID),
		"session_id":                strings.TrimSpace(input.SessionID),
		"report_scope":              strings.TrimSpace(input.ReportScope),
		"p1_entry_count":            input.P1EntryCount,
		"p2_entry_count":            input.P2EntryCount,
		"p3_entry_count":            input.P3EntryCount,
		"hot_hit_rate":              input.HotHitRate,
		"persistent_hit_rate":       input.PersistentHitRate,
		"cluster_hit_rate":          input.ClusterHitRate,
		"stale_read_rate":           input.StaleReadRate,
		"offload_ratio":             input.OffloadRatio,
		"invalidated_replica_count": invalidatedCount,
		"replica_count":             replicaCount,
		"typed_event_type":          "MEMORY_RESIDENCY_REPORT",
		"summary":                   summary,
	}
}

func validateMemoryResidencyReportOwnershipTx(ctx context.Context, tx *sql.Tx, input MemoryResidencyReportInput) error {
	row := tx.QueryRowContext(
		ctx,
		`SELECT workspace_id, agent_id, session_id, report_scope
		   FROM memory_residency_reports
		  WHERE report_id = ?`,
		input.ReportID,
	)
	var existingWorkspaceID, existingAgentID, existingSessionID, existingReportScope string
	if err := row.Scan(&existingWorkspaceID, &existingAgentID, &existingSessionID, &existingReportScope); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("query memory residency report ownership: %w", err)
	}
	if strings.TrimSpace(existingWorkspaceID) == input.WorkspaceID &&
		strings.TrimSpace(existingAgentID) == input.AgentID &&
		strings.TrimSpace(existingSessionID) == input.SessionID &&
		strings.TrimSpace(existingReportScope) == input.ReportScope {
		return nil
	}
	return fmt.Errorf(
		"report_id %s already belongs to %s/%s/%s/%s",
		input.ReportID,
		strings.TrimSpace(existingWorkspaceID),
		strings.TrimSpace(existingAgentID),
		strings.TrimSpace(existingReportScope),
		strings.TrimSpace(existingSessionID),
	)
}

func (s *Store) validateMemoryResidencySessionScopeTx(ctx context.Context, tx *sql.Tx, input MemoryResidencyReportInput) error {
	if input.SessionID == "" {
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
		return fmt.Errorf("query residency session owner: %w", err)
	}
	if strings.TrimSpace(sessionAgentID) != input.AgentID {
		return errors.New("session_id belongs to another agent")
	}
	return nil
}

func syncMemoryReplicaStatesTx(ctx context.Context, tx *sql.Tx, input MemoryResidencyReportInput, now string) error {
	keepIDs := make([]string, 0, len(input.Replicas))
	for _, replica := range input.Replicas {
		replicaStateID := memoryReplicaStateID(input.ReportID, replica)
		keepIDs = append(keepIDs, replicaStateID)
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO memory_replica_states(
			    replica_state_id, report_id, workspace_id, agent_id, residency_tier, replica_kind, coherence_class, state,
			    canonical_memory_id, cache_key, source_kind, source_id, version_guard_json,
			    hit_count, stale_ref_count, last_accessed_at, last_validated_at, metadata_json, created_at, updated_at
			  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			  ON CONFLICT(replica_state_id) DO UPDATE SET
			    report_id = excluded.report_id,
			    workspace_id = excluded.workspace_id,
			    agent_id = excluded.agent_id,
			    residency_tier = excluded.residency_tier,
			    replica_kind = excluded.replica_kind,
			    coherence_class = excluded.coherence_class,
			    state = excluded.state,
			    canonical_memory_id = excluded.canonical_memory_id,
			    cache_key = excluded.cache_key,
			    source_kind = excluded.source_kind,
			    source_id = excluded.source_id,
			    version_guard_json = excluded.version_guard_json,
			    hit_count = excluded.hit_count,
			    stale_ref_count = excluded.stale_ref_count,
			    last_accessed_at = excluded.last_accessed_at,
			    last_validated_at = excluded.last_validated_at,
			    metadata_json = excluded.metadata_json,
			    updated_at = excluded.updated_at`,
			replicaStateID,
			input.ReportID,
			input.WorkspaceID,
			input.AgentID,
			replica.ResidencyTier,
			replica.ReplicaKind,
			replica.CoherenceClass,
			replica.State,
			replica.CanonicalMemoryID,
			replica.CacheKey,
			replica.SourceKind,
			replica.SourceID,
			encodeMemoryResidencyVersionGuards(replica.VersionGuards),
			maxInt(replica.HitCount, 0),
			maxInt(replica.StaleRefCount, 0),
			strings.TrimSpace(replica.LastAccessedAt),
			strings.TrimSpace(replica.LastValidatedAt),
			encodeMemoryResidencyJSONMap(replica.Metadata),
			now,
			now,
		); err != nil {
			return fmt.Errorf("upsert memory replica state: %w", err)
		}
	}
	if len(keepIDs) == 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM memory_replica_states WHERE report_id = ?`, input.ReportID); err != nil {
			return fmt.Errorf("clear memory replica states: %w", err)
		}
		return nil
	}
	args := make([]any, 0, len(keepIDs)+1)
	args = append(args, input.ReportID)
	placeholders := make([]string, 0, len(keepIDs))
	for _, keepID := range keepIDs {
		placeholders = append(placeholders, "?")
		args = append(args, keepID)
	}
	query := fmt.Sprintf(
		`DELETE FROM memory_replica_states
		  WHERE report_id = ?
		    AND replica_state_id NOT IN (%s)`,
		strings.Join(placeholders, ", "),
	)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("prune memory replica states: %w", err)
	}
	return nil
}

func memoryReplicaStateIdentityKey(item MemoryReplicaStateInput) string {
	return strings.Join([]string{
		item.ResidencyTier,
		item.ReplicaKind,
		item.CanonicalMemoryID,
		item.CacheKey,
		item.SourceKind,
		item.SourceID,
	}, "|")
}

func memoryReplicaStateID(reportID string, item MemoryReplicaStateInput) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(reportID) + "|" + memoryReplicaStateIdentityKey(item)))
	return "memrep:" + strings.TrimSpace(reportID) + ":" + hex.EncodeToString(sum[:8])
}
