package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	episodePackTypeCompaction      = "COMPACTION"
	episodePackTypeSessionEnd      = "SESSION_END"
	episodePackTypeSessionBlocked  = "SESSION_BLOCKED"
	episodePackTypeSessionDecision = "SESSION_DECISION_NEEDED"
	episodePackTypeSessionHandoff  = "SESSION_HANDOFF"
	episodePackTypeSessionTakeover = "SESSION_TAKEOVER"
	episodePackModeComplete        = "COMPLETE"
	episodePackModeFallback        = "DETERMINISTIC_FALLBACK"
	episodePackSchemaVersion       = "rmp-1.2/phase2"
	episodePackDissentUnknown      = "UNKNOWN"
	episodePackDissentNone         = "NONE"
	episodePackWindowStartFirst    = 0
)

type EpisodePackRecord struct {
	PackID                 string   `json:"pack_id"`
	PackKey                string   `json:"pack_key"`
	WorkspaceID            string   `json:"workspace_id"`
	PackType               string   `json:"pack_type"`
	PackMode               string   `json:"pack_mode"`
	SchemaVersion          string   `json:"schema_version"`
	SessionID              string   `json:"session_id"`
	LineageSessionID       string   `json:"lineage_session_id,omitempty"`
	AgentID                string   `json:"agent_id"`
	TaskID                 string   `json:"task_id,omitempty"`
	TriggerKind            string   `json:"trigger_kind"`
	CompactionSnapshotID   string   `json:"compaction_snapshot_id,omitempty"`
	LifecycleEventID       string   `json:"lifecycle_event_id,omitempty"`
	SourceWindowStart      int      `json:"source_window_start"`
	SourceWindowEnd        int      `json:"source_window_end"`
	SourceWindowDigest     string   `json:"source_window_digest,omitempty"`
	SummaryText            string   `json:"summary_text,omitempty"`
	SummaryDigest          string   `json:"summary_digest,omitempty"`
	NarrativeSummary       string   `json:"narrative_summary,omitempty"`
	DecisionLedger         []string `json:"decision_ledger,omitempty"`
	ArtifactDeltaLedger    []string `json:"artifact_delta_ledger,omitempty"`
	BlockerLedger          []string `json:"blocker_ledger,omitempty"`
	FailureRepairChain     []string `json:"failure_repair_chain,omitempty"`
	OpenLoops              []string `json:"open_loops,omitempty"`
	DissentState           string   `json:"dissent_state"`
	DissentSet             []string `json:"dissent_set,omitempty"`
	FactCandidates         []string `json:"fact_candidates,omitempty"`
	HypothesisCandidates   []string `json:"hypothesis_candidates,omitempty"`
	ProvenanceRefs         []string `json:"provenance_refs,omitempty"`
	SummaryWorkspaceMemory string   `json:"summary_workspace_memory,omitempty"`
	MessageCountBefore     int      `json:"message_count_before"`
	MessageCountAfter      int      `json:"message_count_after"`
	MessageTokensBefore    int      `json:"message_tokens_before"`
	MessageTokensAfter     int      `json:"message_tokens_after"`
	TotalInputTokens       int      `json:"total_input_tokens"`
	TotalOutputTokens      int      `json:"total_output_tokens"`
	TotalTokens            int      `json:"total_tokens"`
	CreatedAt              string   `json:"created_at"`
	UpdatedAt              string   `json:"updated_at"`
	CanonicalMemoryID      string   `json:"canonical_memory_id,omitempty"`
}

type EpisodePackFilter struct {
	WorkspaceID string
	PackType    string
	PackMode    string
	SessionID   string
	AgentID     string
	TaskID      string
	Limit       int
}

type EpisodePackSyncResult struct {
	WorkspaceID string `json:"workspace_id"`
	PacksSynced int    `json:"packs_synced"`
}

type episodePackCompactionContext struct {
	TaskID             string
	SessionStatus      string
	SourceWindowDigest string
	PackMode           string
}

type episodePackSourceWindow struct {
	MessageCount      int
	MessageTokens     int
	TotalInputTokens  int
	TotalOutputTokens int
	WindowStart       int
	WindowEnd         int
	Digest            string
}

func (s *Store) ListEpisodePacks(ctx context.Context, filter EpisodePackFilter) ([]EpisodePackRecord, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if err := validateEpisodePackModeFilter(filter.PackMode); err != nil {
		return nil, err
	}
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	query := strings.Builder{}
	query.WriteString(`SELECT pack_id, pack_key, workspace_id, pack_type, pack_mode, schema_version,
	        session_id, lineage_session_id, agent_id, COALESCE(task_id,''), trigger_kind,
	        COALESCE(compaction_snapshot_id,''), COALESCE(lifecycle_event_id,''), source_window_start, source_window_end, source_window_digest,
	        summary_text, summary_digest, narrative_summary,
	        decision_ledger_json, artifact_delta_ledger_json, blocker_ledger_json, failure_repair_chain_json,
	        open_loops_json, dissent_state, dissent_set_json, fact_candidates_json, hypothesis_candidates_json,
	        provenance_refs_json, COALESCE(summary_workspace_memory,''), message_count_before, message_count_after,
	        message_tokens_before, message_tokens_after, total_input_tokens, total_output_tokens, created_at, updated_at
	   FROM episode_packs
	  WHERE workspace_id = ?`)
	args := []any{filter.WorkspaceID}
	if trimmed := normalizeEpisodePackType(filter.PackType); trimmed != "" {
		query.WriteString(` AND pack_type = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.PackMode); trimmed != "" {
		query.WriteString(` AND pack_mode = ?`)
		args = append(args, normalizeEpisodePackMode(trimmed, ""))
	}
	if trimmed := strings.TrimSpace(filter.SessionID); trimmed != "" {
		query.WriteString(` AND session_id = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.AgentID); trimmed != "" {
		query.WriteString(` AND agent_id = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.TaskID); trimmed != "" {
		query.WriteString(` AND COALESCE(task_id,'') = ?`)
		args = append(args, trimmed)
	}
	query.WriteString(` ORDER BY updated_at DESC, pack_id DESC LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list episode packs: %w", err)
	}
	defer rows.Close()
	return collectEpisodePackRows(rows)
}

func (s *Store) GetEpisodePack(ctx context.Context, workspaceID, packID string) (EpisodePackRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	packID = strings.TrimSpace(packID)
	if workspaceID == "" {
		return EpisodePackRecord{}, errors.New("workspace_id is required")
	}
	if packID == "" {
		return EpisodePackRecord{}, errors.New("pack_id is required")
	}
	row := s.db.QueryRowContext(
		ctx,
		`SELECT pack_id, pack_key, workspace_id, pack_type, pack_mode, schema_version,
		        session_id, lineage_session_id, agent_id, COALESCE(task_id,''), trigger_kind,
		        COALESCE(compaction_snapshot_id,''), COALESCE(lifecycle_event_id,''), source_window_start, source_window_end, source_window_digest,
		        summary_text, summary_digest, narrative_summary,
		        decision_ledger_json, artifact_delta_ledger_json, blocker_ledger_json, failure_repair_chain_json,
		        open_loops_json, dissent_state, dissent_set_json, fact_candidates_json, hypothesis_candidates_json,
		        provenance_refs_json, COALESCE(summary_workspace_memory,''), message_count_before, message_count_after,
		        message_tokens_before, message_tokens_after, total_input_tokens, total_output_tokens, created_at, updated_at
		   FROM episode_packs
		  WHERE workspace_id = ? AND pack_id = ?`,
		workspaceID,
		packID,
	)
	record, err := scanEpisodePackRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EpisodePackRecord{}, fmt.Errorf("episode pack not found: %s/%s", workspaceID, packID)
		}
		return EpisodePackRecord{}, err
	}
	return record, nil
}

func (s *Store) SyncEpisodePacksWorkspace(ctx context.Context, workspaceID string) (EpisodePackSyncResult, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return EpisodePackSyncResult{}, errors.New("workspace_id is required")
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return EpisodePackSyncResult{}, fmt.Errorf("begin episode pack sync tx: %w", err)
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		_ = tx.Rollback()
		return EpisodePackSyncResult{}, err
	}
	result := EpisodePackSyncResult{WorkspaceID: workspaceID}
	snapshotRows, err := tx.QueryContext(
		ctx,
		`SELECT snapshot_id, session_id, workspace_id, agent_id, trigger_kind, token_budget,
		        message_count_before, message_count_after, message_tokens_before, message_tokens_after,
		        total_input_tokens, total_output_tokens, summary_text, COALESCE(summary_workspace_memory,''), created_at,
		        '' AS task_id, '' AS pack_mode, '' AS source_window_digest, '' AS episode_pack_id, '' AS canonical_memory_id
		   FROM session_compaction_snapshots
		  WHERE workspace_id = ?
		  ORDER BY created_at ASC, snapshot_id ASC`,
		workspaceID,
	)
	if err != nil {
		_ = tx.Rollback()
		return EpisodePackSyncResult{}, fmt.Errorf("query compaction snapshots for episode pack sync: %w", err)
	}
	defer snapshotRows.Close()
	snapshots, err := collectSessionCompactionSnapshotRows(snapshotRows)
	if err != nil {
		_ = tx.Rollback()
		return EpisodePackSyncResult{}, err
	}
	for _, snapshot := range snapshots {
		taskID, sessionStatus, err := loadEpisodePackSessionContextTx(ctx, tx, snapshot.SessionID)
		if err != nil {
			_ = tx.Rollback()
			return EpisodePackSyncResult{}, err
		}
		if _, err := s.recordCompactionEpisodePackTx(ctx, tx, snapshot, episodePackCompactionContext{
			TaskID:        taskID,
			SessionStatus: sessionStatus,
		}); err != nil {
			_ = tx.Rollback()
			return EpisodePackSyncResult{}, err
		}
		result.PacksSynced++
	}
	if err := tx.Commit(); err != nil {
		return EpisodePackSyncResult{}, fmt.Errorf("commit episode pack sync tx: %w", err)
	}
	if _, err := s.RebuildMemoryProjectionWorkspace(ctx, MemoryProjectionRebuildFilter{
		WorkspaceID: workspaceID,
		Kinds:       []string{memoryProjectionKindEpisodePack},
		Limit:       maxInt(result.PacksSynced, memoryProjectionDefaultReconcileLimit),
	}); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Store) recordCompactionEpisodePackTx(ctx context.Context, tx *sql.Tx, snapshot SessionCompactionSnapshotRecord, packCtx episodePackCompactionContext) (EpisodePackRecord, error) {
	record := episodePackFromCompactionSnapshot(snapshot, packCtx)
	return s.upsertEpisodePackTx(ctx, tx, record)
}

func (s *Store) upsertEpisodePackTx(ctx context.Context, tx *sql.Tx, record EpisodePackRecord) (EpisodePackRecord, error) {
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO episode_packs(
		    pack_id, pack_key, workspace_id, pack_type, pack_mode, schema_version,
		    session_id, lineage_session_id, agent_id, task_id, trigger_kind, compaction_snapshot_id, lifecycle_event_id,
		    source_window_start, source_window_end, source_window_digest,
		    summary_text, summary_digest, narrative_summary,
		    decision_ledger_json, artifact_delta_ledger_json, blocker_ledger_json, failure_repair_chain_json,
		    open_loops_json, dissent_state, dissent_set_json, fact_candidates_json, hypothesis_candidates_json,
		    provenance_refs_json, summary_workspace_memory,
		    message_count_before, message_count_after, message_tokens_before, message_tokens_after,
		    total_input_tokens, total_output_tokens, created_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		  ON CONFLICT(pack_key) DO UPDATE SET
		    workspace_id = excluded.workspace_id,
		    pack_type = excluded.pack_type,
		    pack_mode = excluded.pack_mode,
		    schema_version = excluded.schema_version,
		    session_id = excluded.session_id,
		    lineage_session_id = excluded.lineage_session_id,
		    agent_id = excluded.agent_id,
		    task_id = excluded.task_id,
		    trigger_kind = excluded.trigger_kind,
		    compaction_snapshot_id = excluded.compaction_snapshot_id,
		    lifecycle_event_id = excluded.lifecycle_event_id,
		    source_window_start = excluded.source_window_start,
		    source_window_end = excluded.source_window_end,
		    source_window_digest = excluded.source_window_digest,
		    summary_text = excluded.summary_text,
		    summary_digest = excluded.summary_digest,
		    narrative_summary = excluded.narrative_summary,
		    decision_ledger_json = excluded.decision_ledger_json,
		    artifact_delta_ledger_json = excluded.artifact_delta_ledger_json,
		    blocker_ledger_json = excluded.blocker_ledger_json,
		    failure_repair_chain_json = excluded.failure_repair_chain_json,
		    open_loops_json = excluded.open_loops_json,
		    dissent_state = excluded.dissent_state,
		    dissent_set_json = excluded.dissent_set_json,
		    fact_candidates_json = excluded.fact_candidates_json,
		    hypothesis_candidates_json = excluded.hypothesis_candidates_json,
		    provenance_refs_json = excluded.provenance_refs_json,
		    summary_workspace_memory = excluded.summary_workspace_memory,
		    message_count_before = excluded.message_count_before,
		    message_count_after = excluded.message_count_after,
		    message_tokens_before = excluded.message_tokens_before,
		    message_tokens_after = excluded.message_tokens_after,
		    total_input_tokens = excluded.total_input_tokens,
		    total_output_tokens = excluded.total_output_tokens,
		    updated_at = excluded.updated_at`,
		record.PackID,
		record.PackKey,
		record.WorkspaceID,
		record.PackType,
		record.PackMode,
		record.SchemaVersion,
		record.SessionID,
		record.LineageSessionID,
		record.AgentID,
		blankStringOrNil(record.TaskID),
		record.TriggerKind,
		blankStringOrNil(record.CompactionSnapshotID),
		record.LifecycleEventID,
		record.SourceWindowStart,
		record.SourceWindowEnd,
		record.SourceWindowDigest,
		record.SummaryText,
		record.SummaryDigest,
		record.NarrativeSummary,
		encodeStringArrayJSON(record.DecisionLedger),
		encodeStringArrayJSON(record.ArtifactDeltaLedger),
		encodeStringArrayJSON(record.BlockerLedger),
		encodeStringArrayJSON(record.FailureRepairChain),
		encodeStringArrayJSON(record.OpenLoops),
		record.DissentState,
		encodeStringArrayJSON(record.DissentSet),
		encodeStringArrayJSON(record.FactCandidates),
		encodeStringArrayJSON(record.HypothesisCandidates),
		encodeStringArrayJSON(record.ProvenanceRefs),
		blankStringOrNil(record.SummaryWorkspaceMemory),
		record.MessageCountBefore,
		record.MessageCountAfter,
		record.MessageTokensBefore,
		record.MessageTokensAfter,
		record.TotalInputTokens,
		record.TotalOutputTokens,
		record.CreatedAt,
		record.UpdatedAt,
	); err != nil {
		return EpisodePackRecord{}, fmt.Errorf("upsert episode pack: %w", err)
	}
	stored, err := s.loadEpisodePackTx(ctx, tx, record.WorkspaceID, record.PackID)
	if err != nil {
		return EpisodePackRecord{}, err
	}
	if err := s.enqueueMemoryProjectionOutboxTx(ctx, tx, stored.WorkspaceID, memoryProjectionKindEpisodePack, stored.PackID, stored.UpdatedAt); err != nil {
		return EpisodePackRecord{}, err
	}
	return stored, nil
}

func (s *Store) syncEpisodePackGraphProjectionTx(ctx context.Context, tx *sql.Tx, stored EpisodePackRecord) error {
	node, refs, versions, metrics, edges := memoryGraphNodeFromEpisodePack(stored)
	grounding, err := s.memoryGraphGroundingForEpisodePackTx(ctx, tx, node.MemoryID, stored)
	if err != nil {
		return err
	}
	refs = uniqueMemoryGraphNodeRefs(append(refs, grounding.refs...))
	versions = uniqueMemoryGraphNodeVersions(append(versions, grounding.versions...))
	edges = uniqueMemoryGraphEdges(append(edges, grounding.edges...))
	if _, err := s.upsertMemoryGraphNodeTx(ctx, tx, node); err != nil {
		return err
	}
	if err := s.replaceMemoryGraphNodeRefsTx(ctx, tx, node.MemoryID, stored.WorkspaceID, refs); err != nil {
		return err
	}
	if err := s.replaceMemoryGraphNodeVersionsTx(ctx, tx, node.MemoryID, stored.WorkspaceID, versions); err != nil {
		return err
	}
	if err := s.replaceMemoryGraphNodeMetricsTx(ctx, tx, node.MemoryID, stored.WorkspaceID, metrics); err != nil {
		return err
	}
	return s.replaceMemoryGraphEdgesForSourceTx(ctx, tx, stored.WorkspaceID, "episode_pack", stored.PackID, edges)
}

func episodePackFromCompactionSnapshot(snapshot SessionCompactionSnapshotRecord, packCtx episodePackCompactionContext) EpisodePackRecord {
	now := firstNonEmpty(strings.TrimSpace(snapshot.CreatedAt), time.Now().UTC().Format(time.RFC3339Nano))
	packID := strings.TrimSpace(snapshot.SnapshotID)
	if packID == "" {
		packID = nextID("pack")
	}
	sourceWindowStart := episodePackWindowStartFirst
	sourceWindowEnd := snapshot.MessageCountBefore - 1
	if sourceWindowEnd < 0 {
		sourceWindowEnd = -1
	}
	packMode := normalizeEpisodePackMode(packCtx.PackMode, snapshot.SummaryText)
	summaryText := strings.TrimSpace(snapshot.SummaryText)
	narrativeSummary := episodePackNarrativeSummary(snapshot, packMode)
	provenance := []string{
		"session_compaction_snapshot:" + strings.TrimSpace(snapshot.SnapshotID),
		"session:" + strings.TrimSpace(snapshot.SessionID),
		"agent:" + strings.TrimSpace(snapshot.AgentID),
		"trigger:" + strings.TrimSpace(snapshot.TriggerKind),
	}
	if trimmed := strings.TrimSpace(packCtx.TaskID); trimmed != "" {
		provenance = append(provenance, "task:"+trimmed)
	}
	if trimmed := strings.TrimSpace(snapshot.SummaryWorkspaceMemory); trimmed != "" {
		provenance = append(provenance, "workspace_memory:"+trimmed)
	}
	openLoops := episodePackOpenLoops(packCtx.SessionStatus, snapshot.SessionID)
	failureRepair := []string{}
	if packMode == episodePackModeFallback {
		failureRepair = append(failureRepair, "compaction_fallback:"+firstNonEmpty(strings.TrimSpace(snapshot.TriggerKind), "token_budget_exceeded"))
	}
	record := EpisodePackRecord{
		PackID:                 packID,
		PackKey:                episodePackKeyForCompaction(snapshot.SnapshotID),
		WorkspaceID:            strings.TrimSpace(snapshot.WorkspaceID),
		PackType:               episodePackTypeCompaction,
		PackMode:               packMode,
		SchemaVersion:          episodePackSchemaVersion,
		SessionID:              strings.TrimSpace(snapshot.SessionID),
		LineageSessionID:       strings.TrimSpace(snapshot.SessionID),
		AgentID:                strings.TrimSpace(snapshot.AgentID),
		TaskID:                 strings.TrimSpace(packCtx.TaskID),
		TriggerKind:            firstNonEmpty(strings.TrimSpace(snapshot.TriggerKind), "token_budget_exceeded"),
		CompactionSnapshotID:   strings.TrimSpace(snapshot.SnapshotID),
		SourceWindowStart:      sourceWindowStart,
		SourceWindowEnd:        sourceWindowEnd,
		SourceWindowDigest:     normalizeEpisodePackDigest(packCtx.SourceWindowDigest),
		SummaryText:            summaryText,
		SummaryDigest:          episodePackDigest(snapshot, packCtx, narrativeSummary, packMode),
		NarrativeSummary:       narrativeSummary,
		DecisionLedger:         []string{},
		ArtifactDeltaLedger:    []string{},
		BlockerLedger:          []string{},
		FailureRepairChain:     failureRepair,
		OpenLoops:              openLoops,
		DissentState:           episodePackDissentUnknown,
		DissentSet:             []string{},
		FactCandidates:         []string{},
		HypothesisCandidates:   []string{},
		ProvenanceRefs:         uniqueTrimmedStrings(provenance),
		SummaryWorkspaceMemory: strings.TrimSpace(snapshot.SummaryWorkspaceMemory),
		MessageCountBefore:     snapshot.MessageCountBefore,
		MessageCountAfter:      snapshot.MessageCountAfter,
		MessageTokensBefore:    snapshot.MessageTokensBefore,
		MessageTokensAfter:     snapshot.MessageTokensAfter,
		TotalInputTokens:       snapshot.TotalInputTokens,
		TotalOutputTokens:      snapshot.TotalOutputTokens,
		CreatedAt:              now,
		UpdatedAt:              now,
	}
	record.TotalTokens = record.TotalInputTokens + record.TotalOutputTokens
	record.CanonicalMemoryID = memoryGraphNodeID("episode_pack", record.PackID)
	return record
}

func (s *Store) recordSessionLifecycleEpisodePackTx(ctx context.Context, tx *sql.Tx, state AgentSessionStateRecord, eventType, lifecycleEventID string) (EpisodePackRecord, bool, error) {
	packType, ok := classifySessionLifecycleEpisodePack(eventType, state)
	if !ok {
		return EpisodePackRecord{}, false, nil
	}
	window, err := loadEpisodePackSourceWindowTx(ctx, tx, state.SessionID)
	if err != nil {
		return EpisodePackRecord{}, false, err
	}
	record := episodePackFromSessionLifecycle(state, eventType, lifecycleEventID, packType, window)
	stored, err := s.upsertEpisodePackTx(ctx, tx, record)
	if err != nil {
		return EpisodePackRecord{}, false, err
	}
	return stored, true, nil
}

func (s *Store) recordSessionTakeoverEpisodePackTx(ctx context.Context, tx *sql.Tx, sourceState, successorState AgentSessionStateRecord, summary, lifecycleEventID, updatedAt string) (EpisodePackRecord, error) {
	window, err := loadEpisodePackSourceWindowTx(ctx, tx, sourceState.SessionID)
	if err != nil {
		return EpisodePackRecord{}, err
	}
	record := episodePackFromSessionTakeover(sourceState, successorState, summary, lifecycleEventID, updatedAt, window)
	return s.upsertEpisodePackTx(ctx, tx, record)
}

func classifySessionLifecycleEpisodePack(eventType string, state AgentSessionStateRecord) (string, bool) {
	eventType = model.NormalizeSessionEventType(eventType)
	switch eventType {
	case model.SessionEventBlocked:
		return episodePackTypeSessionBlocked, true
	case model.SessionEventDecisionNeeded:
		return episodePackTypeSessionDecision, true
	case model.SessionEventEnd:
		return episodePackTypeSessionEnd, true
	case model.SessionEventStatus:
		if model.NormalizeStatus(state.Status) == model.SessionStatusHandoffPending || strings.TrimSpace(state.HandoffTo) != "" {
			return episodePackTypeSessionHandoff, true
		}
	}
	return "", false
}

func episodePackFromSessionLifecycle(state AgentSessionStateRecord, eventType, lifecycleEventID, packType string, window episodePackSourceWindow) EpisodePackRecord {
	now := firstNonEmpty(strings.TrimSpace(state.UpdatedAt), time.Now().UTC().Format(time.RFC3339Nano))
	packID := strings.TrimSpace(lifecycleEventID)
	if packID == "" {
		packID = nextID("pack")
	}
	provenance := []string{
		"session:" + strings.TrimSpace(state.SessionID),
		"agent:" + strings.TrimSpace(state.AgentID),
		"session_event:" + model.NormalizeSessionEventType(eventType),
	}
	if trimmed := strings.TrimSpace(lifecycleEventID); trimmed != "" {
		provenance = append(provenance, "runtime_event:"+trimmed)
	}
	if trimmed := strings.TrimSpace(state.TaskID); trimmed != "" {
		provenance = append(provenance, "task:"+trimmed)
	}
	for _, docKey := range uniqueTrimmedStrings(state.RelatedDocKeys) {
		provenance = append(provenance, "workspace_doc:"+docKey)
	}
	for _, artifact := range state.RelatedArtifactRefs {
		if trimmed := strings.TrimSpace(artifact.Ref); trimmed != "" {
			provenance = append(provenance, "artifact_ref:"+trimmed)
		}
	}
	if trimmed := strings.TrimSpace(state.DecisionNeededFrom); trimmed != "" {
		provenance = append(provenance, "decision_from:"+trimmed)
	}
	if trimmed := strings.TrimSpace(state.HandoffTo); trimmed != "" {
		provenance = append(provenance, "handoff_to:"+trimmed)
	}
	summaryText := strings.TrimSpace(state.Summary)
	narrativeSummary := sessionLifecycleEpisodePackNarrative(packType, state)
	record := EpisodePackRecord{
		PackID:               packID,
		PackKey:              episodePackKeyForLifecycle(lifecycleEventID, packType, state.SessionID, now),
		WorkspaceID:          strings.TrimSpace(state.WorkspaceID),
		PackType:             normalizeEpisodePackType(packType),
		PackMode:             episodePackModeComplete,
		SchemaVersion:        episodePackSchemaVersion,
		SessionID:            strings.TrimSpace(state.SessionID),
		LineageSessionID:     strings.TrimSpace(state.SessionID),
		AgentID:              strings.TrimSpace(state.AgentID),
		TaskID:               strings.TrimSpace(state.TaskID),
		TriggerKind:          model.NormalizeSessionEventType(eventType),
		LifecycleEventID:     strings.TrimSpace(lifecycleEventID),
		SourceWindowStart:    window.WindowStart,
		SourceWindowEnd:      window.WindowEnd,
		SourceWindowDigest:   window.Digest,
		SummaryText:          summaryText,
		SummaryDigest:        episodePackDigestStrings(packType, state.SessionID, state.AgentID, state.TaskID, model.NormalizeSessionEventType(eventType), summaryText, narrativeSummary, window.Digest),
		NarrativeSummary:     narrativeSummary,
		DecisionLedger:       sessionLifecycleDecisionLedger(packType, state),
		ArtifactDeltaLedger:  sessionLifecycleArtifactLedger(state),
		BlockerLedger:        sessionLifecycleBlockerLedger(state.BlockedOn),
		FailureRepairChain:   sessionLifecycleFailureRepairChain(packType, state),
		OpenLoops:            sessionLifecycleOpenLoops(packType, state),
		DissentState:         episodePackDissentUnknown,
		DissentSet:           []string{},
		FactCandidates:       []string{},
		HypothesisCandidates: []string{},
		ProvenanceRefs:       uniqueTrimmedStrings(provenance),
		MessageCountBefore:   window.MessageCount,
		MessageCountAfter:    window.MessageCount,
		MessageTokensBefore:  window.MessageTokens,
		MessageTokensAfter:   window.MessageTokens,
		TotalInputTokens:     window.TotalInputTokens,
		TotalOutputTokens:    window.TotalOutputTokens,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	record.TotalTokens = record.TotalInputTokens + record.TotalOutputTokens
	record.CanonicalMemoryID = memoryGraphNodeID("episode_pack", record.PackID)
	return record
}

func episodePackFromSessionTakeover(sourceState, successorState AgentSessionStateRecord, summary, lifecycleEventID, updatedAt string, window episodePackSourceWindow) EpisodePackRecord {
	now := firstNonEmpty(strings.TrimSpace(updatedAt), strings.TrimSpace(successorState.UpdatedAt), strings.TrimSpace(sourceState.UpdatedAt), time.Now().UTC().Format(time.RFC3339Nano))
	packID := strings.TrimSpace(lifecycleEventID)
	if packID == "" {
		packID = nextID("pack")
	}
	summary = strings.TrimSpace(summary)
	narrativeSummary := sessionTakeoverEpisodePackNarrative(sourceState, successorState, summary)
	provenance := []string{
		"session:" + strings.TrimSpace(sourceState.SessionID),
		"successor_session:" + strings.TrimSpace(successorState.SessionID),
		"agent:" + strings.TrimSpace(sourceState.AgentID),
		"takeover_agent:" + strings.TrimSpace(successorState.AgentID),
		"session_event:session.takeover",
	}
	if trimmed := strings.TrimSpace(lifecycleEventID); trimmed != "" {
		provenance = append(provenance, "runtime_event:"+trimmed)
	}
	if trimmed := firstNonEmpty(strings.TrimSpace(sourceState.TaskID), strings.TrimSpace(successorState.TaskID)); trimmed != "" {
		provenance = append(provenance, "task:"+trimmed)
	}
	for _, docKey := range uniqueTrimmedStrings(append(append([]string(nil), sourceState.RelatedDocKeys...), successorState.RelatedDocKeys...)) {
		provenance = append(provenance, "workspace_doc:"+docKey)
	}
	for _, artifact := range append(append([]model.AgentUpdateArtifactRef(nil), sourceState.RelatedArtifactRefs...), successorState.RelatedArtifactRefs...) {
		if trimmed := strings.TrimSpace(artifact.Ref); trimmed != "" {
			provenance = append(provenance, "artifact_ref:"+trimmed)
		}
	}
	record := EpisodePackRecord{
		PackID:               packID,
		PackKey:              episodePackKeyForLifecycle(lifecycleEventID, episodePackTypeSessionTakeover, sourceState.SessionID, now),
		WorkspaceID:          strings.TrimSpace(sourceState.WorkspaceID),
		PackType:             episodePackTypeSessionTakeover,
		PackMode:             episodePackModeComplete,
		SchemaVersion:        episodePackSchemaVersion,
		SessionID:            strings.TrimSpace(sourceState.SessionID),
		LineageSessionID:     strings.TrimSpace(successorState.SessionID),
		AgentID:              strings.TrimSpace(successorState.AgentID),
		TaskID:               firstNonEmpty(strings.TrimSpace(sourceState.TaskID), strings.TrimSpace(successorState.TaskID)),
		TriggerKind:          "session.takeover",
		LifecycleEventID:     strings.TrimSpace(lifecycleEventID),
		SourceWindowStart:    window.WindowStart,
		SourceWindowEnd:      window.WindowEnd,
		SourceWindowDigest:   window.Digest,
		SummaryText:          summary,
		SummaryDigest:        episodePackDigestStrings(episodePackTypeSessionTakeover, sourceState.SessionID, successorState.SessionID, sourceState.AgentID, successorState.AgentID, summary, narrativeSummary, window.Digest),
		NarrativeSummary:     narrativeSummary,
		DecisionLedger:       []string{},
		ArtifactDeltaLedger:  sessionLifecycleArtifactLedger(AgentSessionStateRecord{RelatedArtifactRefs: append(append([]model.AgentUpdateArtifactRef(nil), sourceState.RelatedArtifactRefs...), successorState.RelatedArtifactRefs...)}),
		BlockerLedger:        sessionLifecycleBlockerLedger(sourceState.BlockedOn),
		FailureRepairChain:   []string{},
		OpenLoops:            sessionTakeoverOpenLoops(successorState),
		DissentState:         episodePackDissentUnknown,
		DissentSet:           []string{},
		FactCandidates:       []string{},
		HypothesisCandidates: []string{},
		ProvenanceRefs:       uniqueTrimmedStrings(provenance),
		MessageCountBefore:   window.MessageCount,
		MessageCountAfter:    window.MessageCount,
		MessageTokensBefore:  window.MessageTokens,
		MessageTokensAfter:   window.MessageTokens,
		TotalInputTokens:     window.TotalInputTokens,
		TotalOutputTokens:    window.TotalOutputTokens,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	record.TotalTokens = record.TotalInputTokens + record.TotalOutputTokens
	record.CanonicalMemoryID = memoryGraphNodeID("episode_pack", record.PackID)
	return record
}

func loadEpisodePackSourceWindowTx(ctx context.Context, tx *sql.Tx, sessionID string) (episodePackSourceWindow, error) {
	window := episodePackSourceWindow{
		WindowStart: episodePackWindowStartFirst,
		WindowEnd:   -1,
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return window, nil
	}
	rows, err := tx.QueryContext(
		ctx,
		`SELECT sequence, role, content_json, token_count
		   FROM agent_session_messages
		  WHERE session_id = ?
		  ORDER BY sequence ASC`,
		sessionID,
	)
	if err != nil {
		return window, fmt.Errorf("query episode pack source window: %w", err)
	}
	defer rows.Close()
	digestParts := make([]string, 0)
	first := true
	for rows.Next() {
		var sequence, tokenCount int
		var role, contentJSON string
		if err := rows.Scan(&sequence, &role, &contentJSON, &tokenCount); err != nil {
			return window, fmt.Errorf("scan episode pack source window: %w", err)
		}
		if first {
			window.WindowStart = sequence
			first = false
		}
		window.WindowEnd = sequence
		window.MessageCount++
		window.MessageTokens += tokenCount
		digestParts = append(digestParts, fmt.Sprintf("%d|%s|%d|%s", sequence, strings.TrimSpace(role), tokenCount, strings.TrimSpace(contentJSON)))
	}
	if err := rows.Err(); err != nil {
		return window, fmt.Errorf("iterate episode pack source window: %w", err)
	}
	if len(digestParts) > 0 {
		window.Digest = episodePackDigestStrings(digestParts...)
	}
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(total_input_tokens,0), COALESCE(total_output_tokens,0)
		   FROM agent_sessions
		  WHERE session_id = ?`,
		sessionID,
	).Scan(&window.TotalInputTokens, &window.TotalOutputTokens); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return window, fmt.Errorf("load episode pack session token totals: %w", err)
	}
	return window, nil
}

func loadEpisodePackSessionContextTx(ctx context.Context, tx *sql.Tx, sessionID string) (taskID string, status string, err error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "", "", nil
	}
	row := tx.QueryRowContext(ctx, `SELECT COALESCE(task_id,''), COALESCE(status,'') FROM agent_sessions WHERE session_id = ?`, sessionID)
	if err := row.Scan(&taskID, &status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", nil
		}
		return "", "", fmt.Errorf("load episode pack session context: %w", err)
	}
	return strings.TrimSpace(taskID), strings.TrimSpace(status), nil
}

func collectEpisodePackRows(rows *sql.Rows) ([]EpisodePackRecord, error) {
	out := make([]EpisodePackRecord, 0)
	for rows.Next() {
		record, err := scanEpisodePackRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate episode packs: %w", err)
	}
	return out, nil
}

func scanEpisodePackRecord(scanner interface{ Scan(dest ...any) error }) (EpisodePackRecord, error) {
	var record EpisodePackRecord
	var decisionLedgerJSON, artifactDeltaJSON, blockerLedgerJSON, failureRepairJSON, openLoopsJSON, dissentSetJSON, factCandidatesJSON, hypothesisCandidatesJSON, provenanceRefsJSON string
	if err := scanner.Scan(
		&record.PackID,
		&record.PackKey,
		&record.WorkspaceID,
		&record.PackType,
		&record.PackMode,
		&record.SchemaVersion,
		&record.SessionID,
		&record.LineageSessionID,
		&record.AgentID,
		&record.TaskID,
		&record.TriggerKind,
		&record.CompactionSnapshotID,
		&record.LifecycleEventID,
		&record.SourceWindowStart,
		&record.SourceWindowEnd,
		&record.SourceWindowDigest,
		&record.SummaryText,
		&record.SummaryDigest,
		&record.NarrativeSummary,
		&decisionLedgerJSON,
		&artifactDeltaJSON,
		&blockerLedgerJSON,
		&failureRepairJSON,
		&openLoopsJSON,
		&record.DissentState,
		&dissentSetJSON,
		&factCandidatesJSON,
		&hypothesisCandidatesJSON,
		&provenanceRefsJSON,
		&record.SummaryWorkspaceMemory,
		&record.MessageCountBefore,
		&record.MessageCountAfter,
		&record.MessageTokensBefore,
		&record.MessageTokensAfter,
		&record.TotalInputTokens,
		&record.TotalOutputTokens,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return EpisodePackRecord{}, err
	}
	record.DecisionLedger = decodeStringArrayJSON(decisionLedgerJSON)
	record.ArtifactDeltaLedger = decodeStringArrayJSON(artifactDeltaJSON)
	record.BlockerLedger = decodeStringArrayJSON(blockerLedgerJSON)
	record.FailureRepairChain = decodeStringArrayJSON(failureRepairJSON)
	record.OpenLoops = decodeStringArrayJSON(openLoopsJSON)
	record.DissentSet = decodeStringArrayJSON(dissentSetJSON)
	record.FactCandidates = decodeStringArrayJSON(factCandidatesJSON)
	record.HypothesisCandidates = decodeStringArrayJSON(hypothesisCandidatesJSON)
	record.ProvenanceRefs = decodeStringArrayJSON(provenanceRefsJSON)
	record.TotalTokens = record.TotalInputTokens + record.TotalOutputTokens
	record.CanonicalMemoryID = memoryGraphNodeID("episode_pack", record.PackID)
	record.PackType = normalizeEpisodePackType(record.PackType)
	record.PackMode = normalizeEpisodePackMode(record.PackMode, record.SummaryText)
	record.DissentState = normalizeEpisodePackDissentState(record.DissentState)
	return record, nil
}

func (s *Store) loadEpisodePackTx(ctx context.Context, tx *sql.Tx, workspaceID, packID string) (EpisodePackRecord, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT pack_id, pack_key, workspace_id, pack_type, pack_mode, schema_version,
		        session_id, lineage_session_id, agent_id, COALESCE(task_id,''), trigger_kind,
		        COALESCE(compaction_snapshot_id,''), COALESCE(lifecycle_event_id,''), source_window_start, source_window_end, source_window_digest,
		        summary_text, summary_digest, narrative_summary,
		        decision_ledger_json, artifact_delta_ledger_json, blocker_ledger_json, failure_repair_chain_json,
		        open_loops_json, dissent_state, dissent_set_json, fact_candidates_json, hypothesis_candidates_json,
		        provenance_refs_json, COALESCE(summary_workspace_memory,''), message_count_before, message_count_after,
		        message_tokens_before, message_tokens_after, total_input_tokens, total_output_tokens, created_at, updated_at
		   FROM episode_packs
		  WHERE workspace_id = ? AND pack_id = ?`,
		workspaceID,
		packID,
	)
	record, err := scanEpisodePackRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EpisodePackRecord{}, fmt.Errorf("episode pack not found: %s/%s", workspaceID, packID)
		}
		return EpisodePackRecord{}, err
	}
	return record, nil
}

func normalizeEpisodePackType(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "",
		episodePackTypeCompaction,
		episodePackTypeSessionEnd,
		episodePackTypeSessionBlocked,
		episodePackTypeSessionDecision,
		episodePackTypeSessionHandoff,
		episodePackTypeSessionTakeover:
		if strings.TrimSpace(raw) == "" {
			return ""
		}
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return strings.ToUpper(strings.TrimSpace(raw))
	}
}

func normalizeEpisodePackMode(raw, summaryText string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case episodePackModeComplete:
		return episodePackModeComplete
	case episodePackModeFallback:
		return episodePackModeFallback
	}
	if episodePackLooksFallback(summaryText) {
		return episodePackModeFallback
	}
	return episodePackModeComplete
}

func validateEpisodePackModeFilter(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	switch strings.ToUpper(trimmed) {
	case episodePackModeComplete, episodePackModeFallback:
		return nil
	default:
		return fmt.Errorf("pack_mode must be one of %s or %s", episodePackModeComplete, episodePackModeFallback)
	}
}

func normalizeEpisodePackDissentState(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case episodePackDissentNone:
		return episodePackDissentNone
	case episodePackDissentUnknown:
		return episodePackDissentUnknown
	default:
		return episodePackDissentUnknown
	}
}

func episodePackKeyForCompaction(snapshotID string) string {
	return "compaction:" + strings.TrimSpace(snapshotID)
}

func episodePackKeyForLifecycle(lifecycleEventID, packType, sessionID, updatedAt string) string {
	if trimmed := strings.TrimSpace(lifecycleEventID); trimmed != "" {
		return "lifecycle:" + trimmed
	}
	return strings.Join([]string{
		"lifecycle",
		normalizeEpisodePackType(packType),
		strings.TrimSpace(sessionID),
		strings.TrimSpace(updatedAt),
	}, ":")
}

func episodePackNarrativeSummary(snapshot SessionCompactionSnapshotRecord, packMode string) string {
	summaryText := strings.TrimSpace(snapshot.SummaryText)
	if packMode == episodePackModeComplete {
		if cleaned := cleanupCompactionSummaryText(summaryText); cleaned != "" {
			return cleaned
		}
	}
	return deterministicEpisodePackFallbackNarrative(snapshot)
}

func cleanupCompactionSummaryText(summaryText string) string {
	summaryText = strings.TrimSpace(summaryText)
	switch {
	case strings.HasPrefix(summaryText, "[Conversation summary:") && strings.HasSuffix(summaryText, "]"):
		cleaned := strings.TrimSuffix(strings.TrimPrefix(summaryText, "[Conversation summary:"), "]")
		return strings.TrimSpace(cleaned)
	default:
		return summaryText
	}
}

func deterministicEpisodePackFallbackNarrative(snapshot SessionCompactionSnapshotRecord) string {
	parts := []string{
		"Deterministic compaction digest.",
		"Session " + strings.TrimSpace(snapshot.SessionID) + ".",
		"Trigger " + firstNonEmpty(strings.TrimSpace(snapshot.TriggerKind), "token_budget_exceeded") + ".",
		fmt.Sprintf("Messages %d -> %d.", snapshot.MessageCountBefore, snapshot.MessageCountAfter),
		fmt.Sprintf("Message tokens %d -> %d.", snapshot.MessageTokensBefore, snapshot.MessageTokensAfter),
		fmt.Sprintf("Runtime tokens %d input / %d output.", snapshot.TotalInputTokens, snapshot.TotalOutputTokens),
	}
	if trimmed := strings.TrimSpace(snapshot.SummaryWorkspaceMemory); trimmed != "" {
		parts = append(parts, "Legacy summary memory "+trimmed+".")
	}
	return strings.Join(parts, " ")
}

func episodePackLooksFallback(summaryText string) bool {
	summaryText = strings.TrimSpace(summaryText)
	return strings.HasPrefix(summaryText, "[Previous conversation history was truncated due to length.")
}

func normalizeEpisodePackDigest(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	return raw
}

func episodePackDigest(snapshot SessionCompactionSnapshotRecord, packCtx episodePackCompactionContext, narrativeSummary, packMode string) string {
	return episodePackDigestStrings(
		"session="+strings.TrimSpace(snapshot.SessionID),
		"agent="+strings.TrimSpace(snapshot.AgentID),
		"task="+strings.TrimSpace(packCtx.TaskID),
		"trigger="+firstNonEmpty(strings.TrimSpace(snapshot.TriggerKind), "token_budget_exceeded"),
		"pack_mode="+normalizeEpisodePackMode(packMode, snapshot.SummaryText),
		"summary="+strings.TrimSpace(narrativeSummary),
		"summary_memory="+strings.TrimSpace(snapshot.SummaryWorkspaceMemory),
		fmt.Sprintf("message_count=%d:%d", snapshot.MessageCountBefore, snapshot.MessageCountAfter),
		fmt.Sprintf("message_tokens=%d:%d", snapshot.MessageTokensBefore, snapshot.MessageTokensAfter),
		fmt.Sprintf("runtime_tokens=%d:%d", snapshot.TotalInputTokens, snapshot.TotalOutputTokens),
		"digest="+strings.TrimSpace(packCtx.SourceWindowDigest),
	)
}

func episodePackDigestStrings(parts ...string) string {
	payload := strings.Join(parts, "\n")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func episodePackOpenLoops(sessionStatus, sessionID string) []string {
	sessionStatus = strings.TrimSpace(sessionStatus)
	if sessionStatus == "" || strings.EqualFold(sessionStatus, "COMPLETED") || strings.EqualFold(sessionStatus, "FAILED") {
		return nil
	}
	return []string{"session:" + strings.TrimSpace(sessionID) + ":still_open"}
}

func sessionLifecycleEpisodePackNarrative(packType string, state AgentSessionStateRecord) string {
	switch normalizeEpisodePackType(packType) {
	case episodePackTypeSessionBlocked:
		kinds := []string{}
		for _, item := range state.BlockedOn {
			if trimmed := strings.TrimSpace(item.Kind); trimmed != "" {
				kinds = append(kinds, trimmed)
			}
		}
		base := "Session " + strings.TrimSpace(state.SessionID) + " became blocked"
		if len(kinds) > 0 {
			base += " on " + strings.Join(uniqueTrimmedStrings(kinds), ", ")
		}
		if summary := strings.TrimSpace(state.Summary); summary != "" {
			base += ". " + summary
		}
		return strings.TrimSpace(base)
	case episodePackTypeSessionDecision:
		base := "Session " + strings.TrimSpace(state.SessionID) + " requested a decision"
		if from := strings.TrimSpace(state.DecisionNeededFrom); from != "" {
			base += " from " + from
		}
		if decisionType := strings.TrimSpace(state.DecisionType); decisionType != "" {
			base += " (" + decisionType + ")"
		}
		if summary := strings.TrimSpace(state.Summary); summary != "" {
			base += ". " + summary
		}
		return strings.TrimSpace(base)
	case episodePackTypeSessionHandoff:
		base := "Session " + strings.TrimSpace(state.SessionID) + " is pending handoff"
		if target := strings.TrimSpace(state.HandoffTo); target != "" {
			base += " to " + target
		}
		if summary := strings.TrimSpace(state.Summary); summary != "" {
			base += ". " + summary
		}
		return strings.TrimSpace(base)
	case episodePackTypeSessionEnd:
		base := "Session " + strings.TrimSpace(state.SessionID) + " ended"
		if summary := strings.TrimSpace(state.Summary); summary != "" {
			base += ". " + summary
		}
		return strings.TrimSpace(base)
	default:
		return firstNonEmpty(strings.TrimSpace(state.Summary), "Session lifecycle episode pack")
	}
}

func sessionTakeoverEpisodePackNarrative(sourceState, successorState AgentSessionStateRecord, summary string) string {
	base := "Session " + strings.TrimSpace(sourceState.SessionID) + " transferred from " + strings.TrimSpace(sourceState.AgentID) + " to " + strings.TrimSpace(successorState.AgentID)
	if successor := strings.TrimSpace(successorState.SessionID); successor != "" {
		base += " via successor session " + successor
	}
	if summary != "" {
		base += ". " + summary
	}
	return strings.TrimSpace(base)
}

func sessionLifecycleDecisionLedger(packType string, state AgentSessionStateRecord) []string {
	values := make([]string, 0, 2)
	switch normalizeEpisodePackType(packType) {
	case episodePackTypeSessionDecision:
		if decisionType := strings.TrimSpace(state.DecisionType); decisionType != "" {
			values = append(values, "decision_type:"+decisionType)
		}
		if from := strings.TrimSpace(state.DecisionNeededFrom); from != "" {
			values = append(values, "decision_from:"+from)
		}
	case episodePackTypeSessionHandoff:
		if target := strings.TrimSpace(state.HandoffTo); target != "" {
			values = append(values, "handoff_to:"+target)
		}
	}
	if summary := strings.TrimSpace(state.Summary); summary != "" {
		values = append(values, "summary:"+summary)
	}
	return uniqueTrimmedStrings(values)
}

func sessionLifecycleArtifactLedger(state AgentSessionStateRecord) []string {
	values := make([]string, 0, len(state.RelatedArtifactRefs))
	for _, item := range state.RelatedArtifactRefs {
		ref := strings.TrimSpace(item.Ref)
		if ref == "" {
			continue
		}
		label := ref
		if kind := strings.TrimSpace(item.Kind); kind != "" {
			label = kind + ":" + ref
		}
		values = append(values, label)
	}
	return uniqueTrimmedStrings(values)
}

func sessionLifecycleBlockerLedger(items []model.AgentUpdateBlockedRef) []string {
	values := make([]string, 0, len(items))
	for _, item := range items {
		kind := strings.TrimSpace(item.Kind)
		detail := strings.TrimSpace(item.Detail)
		switch {
		case kind != "" && detail != "":
			values = append(values, kind+":"+detail)
		case kind != "":
			values = append(values, kind)
		case detail != "":
			values = append(values, detail)
		}
	}
	return uniqueTrimmedStrings(values)
}

func sessionLifecycleFailureRepairChain(packType string, state AgentSessionStateRecord) []string {
	if normalizeEpisodePackType(packType) != episodePackTypeSessionBlocked {
		return nil
	}
	values := make([]string, 0, len(state.BlockedOn))
	for _, item := range state.BlockedOn {
		if kind := strings.TrimSpace(item.Kind); kind != "" {
			values = append(values, "blocked:"+kind)
		}
	}
	return uniqueTrimmedStrings(values)
}

func sessionLifecycleOpenLoops(packType string, state AgentSessionStateRecord) []string {
	switch normalizeEpisodePackType(packType) {
	case episodePackTypeSessionBlocked, episodePackTypeSessionDecision, episodePackTypeSessionHandoff:
		return []string{"session:" + strings.TrimSpace(state.SessionID) + ":attention_open"}
	default:
		return nil
	}
}

func sessionTakeoverOpenLoops(successorState AgentSessionStateRecord) []string {
	if strings.TrimSpace(successorState.SessionID) == "" || !model.IsSessionStatusActive(successorState.Status) {
		return nil
	}
	return []string{"session:" + strings.TrimSpace(successorState.SessionID) + ":continued_after_takeover"}
}
