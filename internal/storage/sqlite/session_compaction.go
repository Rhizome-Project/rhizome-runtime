package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type SessionCompactionSnapshotInput struct {
	SnapshotID             string
	SessionID              string
	WorkspaceID            string
	AgentID                string
	TriggerKind            string
	PackMode               string
	SourceWindowDigest     string
	TokenBudget            int
	MessageCountBefore     int
	MessageCountAfter      int
	MessageTokensBefore    int
	MessageTokensAfter     int
	TotalInputTokens       int
	TotalOutputTokens      int
	SummaryText            string
	SummaryWorkspaceMemory string
}

type SessionCompactionSnapshotRecord struct {
	SnapshotID             string `json:"snapshot_id"`
	SessionID              string `json:"session_id"`
	WorkspaceID            string `json:"workspace_id"`
	AgentID                string `json:"agent_id"`
	TaskID                 string `json:"task_id,omitempty"`
	TriggerKind            string `json:"trigger_kind"`
	PackMode               string `json:"pack_mode,omitempty"`
	SourceWindowDigest     string `json:"source_window_digest,omitempty"`
	TokenBudget            int    `json:"token_budget"`
	MessageCountBefore     int    `json:"message_count_before"`
	MessageCountAfter      int    `json:"message_count_after"`
	MessageTokensBefore    int    `json:"message_tokens_before"`
	MessageTokensAfter     int    `json:"message_tokens_after"`
	TotalInputTokens       int    `json:"total_input_tokens"`
	TotalOutputTokens      int    `json:"total_output_tokens"`
	TotalTokens            int    `json:"total_tokens"`
	SummaryText            string `json:"summary_text,omitempty"`
	SummaryWorkspaceMemory string `json:"summary_workspace_memory,omitempty"`
	EpisodePackID          string `json:"episode_pack_id,omitempty"`
	CanonicalMemoryID      string `json:"canonical_memory_id,omitempty"`
	CreatedAt              string `json:"created_at"`
}

type SessionCompactionSnapshotFilter struct {
	WorkspaceID string
	SessionID   string
	AgentID     string
	Limit       int
}

func (s *Store) RecordSessionCompactionSnapshot(ctx context.Context, input SessionCompactionSnapshotInput) (SessionCompactionSnapshotRecord, error) {
	record := SessionCompactionSnapshotRecord{
		SnapshotID:             strings.TrimSpace(input.SnapshotID),
		SessionID:              strings.TrimSpace(input.SessionID),
		WorkspaceID:            strings.TrimSpace(input.WorkspaceID),
		AgentID:                strings.TrimSpace(input.AgentID),
		TriggerKind:            strings.TrimSpace(input.TriggerKind),
		PackMode:               strings.TrimSpace(input.PackMode),
		SourceWindowDigest:     strings.TrimSpace(input.SourceWindowDigest),
		TokenBudget:            input.TokenBudget,
		MessageCountBefore:     input.MessageCountBefore,
		MessageCountAfter:      input.MessageCountAfter,
		MessageTokensBefore:    input.MessageTokensBefore,
		MessageTokensAfter:     input.MessageTokensAfter,
		TotalInputTokens:       input.TotalInputTokens,
		TotalOutputTokens:      input.TotalOutputTokens,
		SummaryText:            strings.TrimSpace(input.SummaryText),
		SummaryWorkspaceMemory: strings.TrimSpace(input.SummaryWorkspaceMemory),
	}
	if record.SessionID == "" {
		return SessionCompactionSnapshotRecord{}, errors.New("session_id is required")
	}
	if record.WorkspaceID == "" {
		return SessionCompactionSnapshotRecord{}, errors.New("workspace_id is required")
	}
	if record.AgentID == "" {
		return SessionCompactionSnapshotRecord{}, errors.New("agent_id is required")
	}
	if record.TriggerKind == "" {
		record.TriggerKind = "token_budget_exceeded"
	}
	if record.SnapshotID == "" {
		record.SnapshotID = nextID("compaction")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return SessionCompactionSnapshotRecord{}, fmt.Errorf("begin compaction snapshot tx: %w", err)
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		_ = tx.Rollback()
		return SessionCompactionSnapshotRecord{}, err
	}
	if err := s.ensureAgentInWorkspaceTx(ctx, tx, record.WorkspaceID, record.AgentID); err != nil {
		_ = tx.Rollback()
		return SessionCompactionSnapshotRecord{}, err
	}
	if err := s.ensureAgentSessionInWorkspaceTx(ctx, tx, record.WorkspaceID, record.SessionID); err != nil {
		_ = tx.Rollback()
		return SessionCompactionSnapshotRecord{}, err
	}
	sessionState, err := s.getAgentSessionStateTx(ctx, tx, record.WorkspaceID, record.SessionID)
	if err != nil {
		_ = tx.Rollback()
		return SessionCompactionSnapshotRecord{}, err
	}
	if strings.TrimSpace(sessionState.AgentID) != record.AgentID {
		_ = tx.Rollback()
		return SessionCompactionSnapshotRecord{}, fmt.Errorf("session_id does not belong to agent_id")
	}
	if err := validateSessionCompactionSummaryWorkspaceMemoryTx(ctx, tx, record.WorkspaceID, record.SummaryWorkspaceMemory); err != nil {
		_ = tx.Rollback()
		return SessionCompactionSnapshotRecord{}, err
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO session_compaction_snapshots(
		    snapshot_id, session_id, workspace_id, agent_id, trigger_kind, token_budget,
		    message_count_before, message_count_after, message_tokens_before, message_tokens_after,
		    total_input_tokens, total_output_tokens, summary_text, summary_workspace_memory
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.SnapshotID,
		record.SessionID,
		record.WorkspaceID,
		record.AgentID,
		record.TriggerKind,
		record.TokenBudget,
		record.MessageCountBefore,
		record.MessageCountAfter,
		record.MessageTokensBefore,
		record.MessageTokensAfter,
		record.TotalInputTokens,
		record.TotalOutputTokens,
		record.SummaryText,
		blankStringOrNil(record.SummaryWorkspaceMemory),
	); err != nil {
		_ = tx.Rollback()
		return SessionCompactionSnapshotRecord{}, fmt.Errorf("insert compaction snapshot: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "session_compaction_recorded",
		EntityType: "agent_session",
		EntityID:   record.SessionID,
		ActorID:    record.AgentID,
		PayloadJSON: mustJSON(map[string]any{
			"snapshot_id":              record.SnapshotID,
			"workspace_id":             record.WorkspaceID,
			"trigger_kind":             record.TriggerKind,
			"token_budget":             record.TokenBudget,
			"message_count_before":     record.MessageCountBefore,
			"message_count_after":      record.MessageCountAfter,
			"message_tokens_before":    record.MessageTokensBefore,
			"message_tokens_after":     record.MessageTokensAfter,
			"summary_workspace_memory": record.SummaryWorkspaceMemory,
		}),
	}); err != nil {
		_ = tx.Rollback()
		return SessionCompactionSnapshotRecord{}, err
	}
	taskID, sessionStatus, err := loadEpisodePackSessionContextTx(ctx, tx, record.SessionID)
	if err != nil {
		_ = tx.Rollback()
		return SessionCompactionSnapshotRecord{}, err
	}
	pack, err := s.recordCompactionEpisodePackTx(ctx, tx, record, episodePackCompactionContext{
		TaskID:             taskID,
		SessionStatus:      sessionStatus,
		SourceWindowDigest: record.SourceWindowDigest,
		PackMode:           record.PackMode,
	})
	if err != nil {
		_ = tx.Rollback()
		return SessionCompactionSnapshotRecord{}, err
	}
	record.TaskID = pack.TaskID
	record.PackMode = pack.PackMode
	record.SourceWindowDigest = pack.SourceWindowDigest
	record.EpisodePackID = pack.PackID
	record.CanonicalMemoryID = pack.CanonicalMemoryID
	if err := tx.Commit(); err != nil {
		return SessionCompactionSnapshotRecord{}, fmt.Errorf("commit compaction snapshot tx: %w", err)
	}
	s.bestEffortReconcileMemoryProjectionWorkspace(ctx, record.WorkspaceID)
	return s.getSessionCompactionSnapshot(ctx, record.WorkspaceID, record.SnapshotID)
}

func (s *Store) ListSessionCompactionSnapshots(ctx context.Context, filter SessionCompactionSnapshotFilter) ([]SessionCompactionSnapshotRecord, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	query := strings.Builder{}
	query.WriteString(`SELECT session_compaction_snapshots.snapshot_id, session_compaction_snapshots.session_id, session_compaction_snapshots.workspace_id, session_compaction_snapshots.agent_id, session_compaction_snapshots.trigger_kind, session_compaction_snapshots.token_budget,
	        session_compaction_snapshots.message_count_before, session_compaction_snapshots.message_count_after, session_compaction_snapshots.message_tokens_before, session_compaction_snapshots.message_tokens_after,
	        session_compaction_snapshots.total_input_tokens, session_compaction_snapshots.total_output_tokens, session_compaction_snapshots.summary_text, COALESCE(session_compaction_snapshots.summary_workspace_memory,''), session_compaction_snapshots.created_at,
	        COALESCE(agent_sessions.task_id,''), COALESCE(episode_packs.pack_mode,''), COALESCE(episode_packs.source_window_digest,''),
	        COALESCE(episode_packs.pack_id,''), COALESCE(episode_packs.pack_id,'')
	   FROM session_compaction_snapshots
	   LEFT JOIN agent_sessions ON agent_sessions.session_id = session_compaction_snapshots.session_id
	   LEFT JOIN episode_packs ON episode_packs.compaction_snapshot_id = session_compaction_snapshots.snapshot_id
	  WHERE session_compaction_snapshots.workspace_id = ?`)
	args := []any{filter.WorkspaceID}
	if trimmed := strings.TrimSpace(filter.SessionID); trimmed != "" {
		query.WriteString(` AND session_compaction_snapshots.session_id = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.AgentID); trimmed != "" {
		query.WriteString(` AND session_compaction_snapshots.agent_id = ?`)
		args = append(args, trimmed)
	}
	query.WriteString(` ORDER BY session_compaction_snapshots.created_at DESC, session_compaction_snapshots.snapshot_id DESC LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list compaction snapshots: %w", err)
	}
	defer rows.Close()

	return collectSessionCompactionSnapshotRows(rows)
}

func validateSessionCompactionSummaryWorkspaceMemoryTx(ctx context.Context, tx *sql.Tx, workspaceID, memoryID string) error {
	memoryID = strings.TrimSpace(memoryID)
	if memoryID == "" {
		return nil
	}
	record, err := loadWorkspaceMemoryRecordTx(ctx, tx, workspaceID, memoryID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("summary_workspace_memory must reference an existing workspace memory in workspace_id")
		}
		return err
	}
	if record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != "" {
		return fmt.Errorf("summary_workspace_memory is archived: %s", memoryID)
	}
	if normalizeWorkspaceMemorySourceKind(record.SourceKind) != "compaction" {
		return fmt.Errorf("summary_workspace_memory must reference compaction workspace memory")
	}
	return nil
}

func collectSessionCompactionSnapshotRows(rows *sql.Rows) ([]SessionCompactionSnapshotRecord, error) {
	items := []SessionCompactionSnapshotRecord{}
	for rows.Next() {
		var item SessionCompactionSnapshotRecord
		if err := rows.Scan(
			&item.SnapshotID,
			&item.SessionID,
			&item.WorkspaceID,
			&item.AgentID,
			&item.TriggerKind,
			&item.TokenBudget,
			&item.MessageCountBefore,
			&item.MessageCountAfter,
			&item.MessageTokensBefore,
			&item.MessageTokensAfter,
			&item.TotalInputTokens,
			&item.TotalOutputTokens,
			&item.SummaryText,
			&item.SummaryWorkspaceMemory,
			&item.CreatedAt,
			&item.TaskID,
			&item.PackMode,
			&item.SourceWindowDigest,
			&item.EpisodePackID,
			&item.CanonicalMemoryID,
		); err != nil {
			return nil, fmt.Errorf("scan compaction snapshot: %w", err)
		}
		item.PackMode = normalizeEpisodePackMode(item.PackMode, item.SummaryText)
		item.TotalTokens = item.TotalInputTokens + item.TotalOutputTokens
		if strings.TrimSpace(item.EpisodePackID) != "" {
			item.CanonicalMemoryID = memoryGraphNodeID("episode_pack", item.EpisodePackID)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate compaction snapshots: %w", err)
	}
	return items, nil
}

func (s *Store) getSessionCompactionSnapshot(ctx context.Context, workspaceID, snapshotID string) (SessionCompactionSnapshotRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	snapshotID = strings.TrimSpace(snapshotID)
	if workspaceID == "" {
		return SessionCompactionSnapshotRecord{}, errors.New("workspace_id is required")
	}
	if snapshotID == "" {
		return SessionCompactionSnapshotRecord{}, errors.New("snapshot_id is required")
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT session_compaction_snapshots.snapshot_id, session_compaction_snapshots.session_id, session_compaction_snapshots.workspace_id, session_compaction_snapshots.agent_id, session_compaction_snapshots.trigger_kind, session_compaction_snapshots.token_budget,
		        session_compaction_snapshots.message_count_before, session_compaction_snapshots.message_count_after, session_compaction_snapshots.message_tokens_before, session_compaction_snapshots.message_tokens_after,
		        session_compaction_snapshots.total_input_tokens, session_compaction_snapshots.total_output_tokens, session_compaction_snapshots.summary_text, COALESCE(session_compaction_snapshots.summary_workspace_memory,''), session_compaction_snapshots.created_at,
		        COALESCE(agent_sessions.task_id,''), COALESCE(episode_packs.pack_mode,''), COALESCE(episode_packs.source_window_digest,''),
		        COALESCE(episode_packs.pack_id,''), COALESCE(episode_packs.pack_id,'')
		   FROM session_compaction_snapshots
		   LEFT JOIN agent_sessions ON agent_sessions.session_id = session_compaction_snapshots.session_id
		   LEFT JOIN episode_packs ON episode_packs.compaction_snapshot_id = session_compaction_snapshots.snapshot_id
		  WHERE session_compaction_snapshots.workspace_id = ? AND session_compaction_snapshots.snapshot_id = ?
		  LIMIT 1`,
		workspaceID,
		snapshotID,
	)
	if err != nil {
		return SessionCompactionSnapshotRecord{}, fmt.Errorf("get compaction snapshot: %w", err)
	}
	defer rows.Close()
	items, err := collectSessionCompactionSnapshotRows(rows)
	if err != nil {
		return SessionCompactionSnapshotRecord{}, err
	}
	if len(items) == 0 {
		return SessionCompactionSnapshotRecord{}, fmt.Errorf("compaction snapshot not found after insert: %s", snapshotID)
	}
	return items[0], nil
}
