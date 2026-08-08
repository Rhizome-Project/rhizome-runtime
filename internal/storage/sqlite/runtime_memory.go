package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

type WorkspaceMemoryInput struct {
	MemoryID              string
	WorkspaceID           string
	MemoryType            string
	Title                 string
	Body                  string
	Summary               string
	AgentID               string
	SessionID             string
	TaskID                string
	SourceKind            string
	SourceID              string
	Tags                  []string
	Importance            float64
	Confidence            float64
	PromptContextEnvelope map[string]any
}

type WorkspaceMemoryArchiveInput struct {
	WorkspaceID           string
	MemoryID              string
	ArchivedBy            string
	Reason                string
	PromptContextEnvelope map[string]any
}

type WorkspaceMemoryRestoreInput struct {
	WorkspaceID           string
	MemoryID              string
	RestoredBy            string
	RecoveryReason        string
	PromptContextEnvelope map[string]any
}

type WorkspaceMemoryFilter struct {
	WorkspaceID     string
	Query           string
	MemoryType      string
	AgentID         string
	SessionID       string
	TaskID          string
	SourceKind      string
	SourceID        string
	IncludeArchived bool
	Limit           int
}

type WorkspaceMemoryRecord struct {
	MemoryID       string   `json:"memory_id"`
	WorkspaceID    string   `json:"workspace_id"`
	MemoryType     string   `json:"memory_type"`
	Title          string   `json:"title"`
	Body           string   `json:"body"`
	Summary        string   `json:"summary"`
	AgentID        string   `json:"agent_id,omitempty"`
	SessionID      string   `json:"session_id,omitempty"`
	TaskID         string   `json:"task_id,omitempty"`
	SourceKind     string   `json:"source_kind"`
	SourceID       string   `json:"source_id,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	Importance     float64  `json:"importance"`
	Confidence     float64  `json:"confidence"`
	CreatedAt      string   `json:"created_at"`
	UpdatedAt      string   `json:"updated_at"`
	ArchivedAt     *string  `json:"archived_at,omitempty"`
	ArchivedBy     *string  `json:"archived_by,omitempty"`
	ArchivedReason string   `json:"archived_reason,omitempty"`
	RecoveryReason string   `json:"recovery_reason,omitempty"`
}

const workspaceMemoryRecoveryReasonPruneReactivated = "rmp_gc_reactivated"

type SessionCompactionFilter struct {
	WorkspaceID string
	AgentID     string
	ActiveOnly  bool
	MinMessages int
	MinTokens   int
	Limit       int
}

type SessionCompactionCandidate struct {
	SessionID         string `json:"session_id"`
	WorkspaceID       string `json:"workspace_id"`
	AgentID           string `json:"agent_id"`
	TaskID            string `json:"task_id,omitempty"`
	Status            string `json:"status"`
	MessageCount      int    `json:"message_count"`
	MessageTokens     int    `json:"message_tokens"`
	TotalInputTokens  int    `json:"total_input_tokens"`
	TotalOutputTokens int    `json:"total_output_tokens"`
	TotalTokens       int    `json:"total_tokens"`
	StartedAt         string `json:"started_at"`
	LastMessageAt     string `json:"last_message_at"`
}

func (s *Store) RecordWorkspaceMemory(ctx context.Context, input WorkspaceMemoryInput) (WorkspaceMemoryRecord, error) {
	record, _, err := s.RecordWorkspaceMemoryWithEvent(ctx, input)
	return record, err
}

func (s *Store) RecordWorkspaceMemoryWithEvent(ctx context.Context, input WorkspaceMemoryInput) (WorkspaceMemoryRecord, RuntimeEventRecord, error) {
	record, event, _, err := s.RecordWorkspaceMemoryWithEffects(ctx, input)
	return record, event, err
}

func (s *Store) RecordWorkspaceMemoryWithEffects(ctx context.Context, input WorkspaceMemoryInput) (WorkspaceMemoryRecord, RuntimeEventRecord, *PromotedKnowledgeClaimSyncEffects, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace_id is required")
	}
	if strings.TrimSpace(input.Body) == "" {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, errors.New("body is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("begin workspace memory tx: %w", err)
	}
	var (
		record  WorkspaceMemoryRecord
		event   RuntimeEventRecord
		effects *PromotedKnowledgeClaimSyncEffects
	)
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, event, effects, innerErr = s.recordWorkspaceMemoryWithAuthorityTx(ctx, tx, input, authority, now)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("commit workspace memory tx: %w", err)
	}
	resolved, err := s.GetWorkspaceMemory(ctx, record.WorkspaceID, record.MemoryID)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	return resolved, event, effects, nil
}

func (s *Store) recordWorkspaceMemoryWithEventTx(ctx context.Context, tx *sql.Tx, input WorkspaceMemoryInput, now string) (WorkspaceMemoryRecord, RuntimeEventRecord, error) {
	record, event, _, err := s.recordWorkspaceMemoryWithEffectsTx(ctx, tx, input, now)
	return record, event, err
}

func (s *Store) recordWorkspaceMemoryWithEffectsTx(ctx context.Context, tx *sql.Tx, input WorkspaceMemoryInput, now string) (WorkspaceMemoryRecord, RuntimeEventRecord, *PromotedKnowledgeClaimSyncEffects, error) {
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInputTx(ctx, tx, strings.TrimSpace(input.WorkspaceID), authorityScopeWorkspace, now)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	var (
		record  WorkspaceMemoryRecord
		event   RuntimeEventRecord
		effects *PromotedKnowledgeClaimSyncEffects
	)
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, event, effects, innerErr = s.recordWorkspaceMemoryWithAuthorityTx(ctx, tx, input, authority, now)
		return innerErr
	}); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	return record, event, effects, nil
}

func (s *Store) recordWorkspaceMemoryWithAuthorityTx(ctx context.Context, tx *sql.Tx, input WorkspaceMemoryInput, authority WorkspaceAuthorityRecord, now string) (WorkspaceMemoryRecord, RuntimeEventRecord, *PromotedKnowledgeClaimSyncEffects, error) {
	return s.recordWorkspaceMemoryWithOptionalAuthorityTx(ctx, tx, input, &authority, now)
}

func (s *Store) recordWorkspaceMemoryWithOptionalAuthorityTx(ctx context.Context, tx *sql.Tx, input WorkspaceMemoryInput, authority *WorkspaceAuthorityRecord, now string) (WorkspaceMemoryRecord, RuntimeEventRecord, *PromotedKnowledgeClaimSyncEffects, error) {
	if authority == nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace authority is required")
	}
	record := WorkspaceMemoryRecord{
		MemoryID:    strings.TrimSpace(input.MemoryID),
		WorkspaceID: strings.TrimSpace(input.WorkspaceID),
		MemoryType:  normalizeWorkspaceMemoryType(input.MemoryType),
		Title:       strings.TrimSpace(input.Title),
		Body:        strings.TrimSpace(input.Body),
		Summary:     strings.TrimSpace(input.Summary),
		AgentID:     strings.TrimSpace(input.AgentID),
		SessionID:   strings.TrimSpace(input.SessionID),
		TaskID:      strings.TrimSpace(input.TaskID),
		SourceKind:  normalizeWorkspaceMemorySourceKind(input.SourceKind),
		SourceID:    strings.TrimSpace(input.SourceID),
		Tags:        normalizeStringSlice(input.Tags),
		Importance:  clampUnitInterval(input.Importance),
		Confidence:  clampUnitInterval(input.Confidence),
	}
	if record.MemoryID == "" {
		record.MemoryID = nextID("memory")
	}
	record.CreatedAt = now
	record.UpdatedAt = now
	restoreEvent := RuntimeEventRecord{}

	tagsJSON, err := json.Marshal(record.Tags)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("encode workspace memory tags: %w", err)
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	if record.AgentID != "" {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, record.WorkspaceID, record.AgentID); err != nil {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
		}
	}
	if record.TaskID != "" {
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, record.WorkspaceID, record.TaskID); err != nil {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
		}
	}
	if record.SessionID != "" {
		if err := s.ensureAgentSessionInWorkspaceTx(ctx, tx, record.WorkspaceID, record.SessionID); err != nil {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
		}
	}
	if strings.TrimSpace(input.MemoryID) == "" {
		if existingID, createdAt, found, err := findWorkspaceMemoryDuplicateTx(ctx, tx, record); err != nil {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
		} else if found {
			record.MemoryID = existingID
			record.CreatedAt = createdAt
		} else if archivedRecord, found, err := findWorkspaceMemoryRecoverableDuplicateTx(ctx, tx, record); err != nil {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
		} else if found {
			record.MemoryID = archivedRecord.MemoryID
			record.CreatedAt = archivedRecord.CreatedAt
			record.RecoveryReason = workspaceMemoryRecoveryReasonPruneReactivated
			_, restoreEvent, err = s.restoreWorkspaceMemoryArchivedWithAuthorityTx(
				ctx,
				tx,
				archivedRecord,
				workspaceMemoryRuntimeActorType(record),
				firstNonEmpty(record.AgentID, record.SourceID, record.WorkspaceID),
				record.RecoveryReason,
				*authority,
				nil,
				"",
				now,
			)
			if err != nil {
				return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
			}
		}
	}

	if err := execWorkspaceMemoryUpsertWithFTSRepairTx(
		ctx,
		tx,
		`INSERT INTO workspace_memory(
		    memory_id, workspace_id, memory_type, title, body, summary,
		    agent_id, session_id, task_id, source_kind, source_id, tags_json,
		    importance, confidence, created_at, updated_at,
		    archived_at, archived_by, archived_reason, recovery_reason
		  )
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, '', ?)
		  ON CONFLICT(memory_id) DO UPDATE SET
		    workspace_id = excluded.workspace_id,
		    memory_type = excluded.memory_type,
		    title = excluded.title,
		    body = excluded.body,
		    summary = excluded.summary,
		    agent_id = excluded.agent_id,
		    session_id = excluded.session_id,
		    task_id = excluded.task_id,
		    source_kind = excluded.source_kind,
		    source_id = excluded.source_id,
		    tags_json = excluded.tags_json,
		    importance = excluded.importance,
		    confidence = excluded.confidence,
		    updated_at = excluded.updated_at,
		    archived_at = NULL,
		    archived_by = NULL,
		    archived_reason = '',
		    recovery_reason = excluded.recovery_reason`,
		record.MemoryID,
		record.WorkspaceID,
		record.MemoryType,
		record.Title,
		record.Body,
		record.Summary,
		blankStringOrNil(record.AgentID),
		blankStringOrNil(record.SessionID),
		blankStringOrNil(record.TaskID),
		record.SourceKind,
		record.SourceID,
		string(tagsJSON),
		record.Importance,
		record.Confidence,
		record.CreatedAt,
		record.UpdatedAt,
		record.RecoveryReason,
	); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("upsert workspace memory: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "workspace_memory_recorded",
		EntityType: "workspace_memory",
		EntityID:   record.MemoryID,
		ActorID:    firstNonEmpty(record.AgentID, record.SourceID, record.WorkspaceID),
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id": record.WorkspaceID,
			"memory_id":    record.MemoryID,
			"memory_type":  record.MemoryType,
			"source_kind":  record.SourceKind,
			"source_id":    record.SourceID,
			"session_id":   record.SessionID,
			"task_id":      record.TaskID,
			"tags":         record.Tags,
			"importance":   record.Importance,
			"confidence":   record.Confidence,
		}),
	}); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	event := RuntimeEventRecord{}
	eventPayload := map[string]any{
		"workspace_id": record.WorkspaceID,
		"memory_id":    record.MemoryID,
		"memory_type":  record.MemoryType,
		"source_kind":  record.SourceKind,
		"source_id":    record.SourceID,
	}
	eventPayload, err = attachWorkspaceMemoryPromptContextEnvelope(
		eventPayload,
		input.PromptContextEnvelope,
		workspaceMemoryRecordPromptContextSurface(input.PromptContextEnvelope),
		workspaceMemoryPromptContextFields(record, workspaceMemoryRuntimeActorType(record), firstNonEmpty(record.AgentID, record.SourceID, record.WorkspaceID)),
	)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	appendInput := RuntimeEventInput{
		EventID:        nextID("rtev"),
		WorkspaceID:    record.WorkspaceID,
		EventType:      "workspace_memory.recorded",
		EntityType:     "workspace_memory",
		EntityID:       record.MemoryID,
		ActorType:      workspaceMemoryRuntimeActorType(record),
		ActorID:        firstNonEmpty(record.AgentID, record.SourceID, record.WorkspaceID),
		AgentID:        record.AgentID,
		SessionID:      record.SessionID,
		TaskID:         record.TaskID,
		PayloadJSON:    mustJSON(eventPayload),
		CreatedAt:      record.UpdatedAt,
		ParentRefsJSON: runtimeEventParentRefsJSONForRecord(restoreEvent.EventID),
	}
	event, err = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, *authority, appendInput)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	syncEffects, err := s.syncPromotedKnowledgeClaimForMemoryTx(ctx, tx, *authority, record, firstNonEmpty(record.AgentID, record.SourceID, record.WorkspaceID), "workspace_memory_recorded", record.UpdatedAt)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	if err := s.syncWorkspaceMemoryGraphAnchorTx(ctx, tx, record); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("sync memory graph anchor: %w", err)
	}
	if err := s.enqueueMemoryProjectionOutboxTx(ctx, tx, record.WorkspaceID, memoryProjectionKindWorkspaceMemory, record.MemoryID, record.UpdatedAt); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("enqueue workspace memory projection: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, record.UpdatedAt, record.WorkspaceID); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("touch workspace after memory update: %w", err)
	}
	return record, event, syncEffects, nil
}

func execWorkspaceMemoryUpsertWithFTSRepairTx(ctx context.Context, tx *sql.Tx, query string, args ...any) error {
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		if !workspaceMemoryFTSNeedsRebuild(err) {
			return err
		}
		if rebuildErr := rebuildWorkspaceMemoryFTSTx(ctx, tx); rebuildErr != nil {
			return fmt.Errorf("%w; workspace_memory_fts rebuild failed: %v", err, rebuildErr)
		}
		if _, retryErr := tx.ExecContext(ctx, query, args...); retryErr != nil {
			return fmt.Errorf("retry after workspace_memory_fts rebuild: %w (original error: %v)", retryErr, err)
		}
	}
	return nil
}

func rebuildWorkspaceMemoryFTSTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO workspace_memory_fts(workspace_memory_fts) VALUES ('rebuild')`); err != nil {
		if !workspaceMemoryFTSNeedsRebuild(err) {
			return err
		}
		if recreateErr := recreateWorkspaceMemoryFTSTx(ctx, tx); recreateErr != nil {
			return fmt.Errorf("%w; recreate workspace_memory_fts failed: %v", err, recreateErr)
		}
	}
	return nil
}

func recreateWorkspaceMemoryFTSTx(ctx context.Context, tx *sql.Tx) error {
	for _, stmt := range []string{
		`DROP TRIGGER IF EXISTS workspace_memory_ai`,
		`DROP TRIGGER IF EXISTS workspace_memory_ad`,
		`DROP TRIGGER IF EXISTS workspace_memory_au`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS workspace_memory_fts`); err != nil {
		if !workspaceMemoryFTSNeedsRebuild(err) {
			return err
		}
		if err := removeBrokenWorkspaceMemoryFTSSchemaTx(ctx, tx); err != nil {
			return err
		}
	}
	for _, stmt := range []string{
		`CREATE VIRTUAL TABLE workspace_memory_fts USING fts5(
    title,
    body,
    summary,
    content='workspace_memory',
    content_rowid='rowid'
)`,
		`CREATE TRIGGER workspace_memory_ai AFTER INSERT ON workspace_memory BEGIN
    INSERT INTO workspace_memory_fts(rowid, title, body, summary)
    VALUES (new.rowid, new.title, new.body, new.summary);
END`,
		`CREATE TRIGGER workspace_memory_ad AFTER DELETE ON workspace_memory BEGIN
    INSERT INTO workspace_memory_fts(workspace_memory_fts, rowid, title, body, summary)
    VALUES ('delete', old.rowid, old.title, old.body, old.summary);
END`,
		`CREATE TRIGGER workspace_memory_au AFTER UPDATE ON workspace_memory BEGIN
    INSERT INTO workspace_memory_fts(workspace_memory_fts, rowid, title, body, summary)
    VALUES ('delete', old.rowid, old.title, old.body, old.summary);
    INSERT INTO workspace_memory_fts(rowid, title, body, summary)
    VALUES (new.rowid, new.title, new.body, new.summary);
END`,
		`INSERT INTO workspace_memory_fts(workspace_memory_fts) VALUES ('rebuild')`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func removeBrokenWorkspaceMemoryFTSSchemaTx(ctx context.Context, tx *sql.Tx) error {
	var schemaVersion int64
	if err := tx.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&schemaVersion); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA writable_schema=ON`); err != nil {
		return err
	}
	writableSchemaOff := false
	defer func() {
		if !writableSchemaOff {
			_, _ = tx.ExecContext(context.Background(), `PRAGMA writable_schema=OFF`)
		}
	}()
	for _, stmt := range []string{
		`DELETE FROM sqlite_schema WHERE name = 'workspace_memory_fts' OR name LIKE 'workspace_memory_fts_%'`,
		`DELETE FROM sqlite_schema WHERE tbl_name = 'workspace_memory_fts' OR tbl_name LIKE 'workspace_memory_fts_%'`,
		fmt.Sprintf(`PRAGMA schema_version=%d`, schemaVersion+1),
		`PRAGMA writable_schema=OFF`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
		if stmt == `PRAGMA writable_schema=OFF` {
			writableSchemaOff = true
		}
	}
	return nil
}

func workspaceMemoryFTSNeedsRebuild(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return (strings.Contains(msg, "fts5") || strings.Contains(msg, "workspace_memory_fts")) &&
		(strings.Contains(msg, "run 'rebuild'") ||
			strings.Contains(msg, "invalid fts5") ||
			strings.Contains(msg, "vtable constructor failed") ||
			strings.Contains(msg, "database disk image is malformed") ||
			strings.Contains(msg, "malformed"))
}

func findWorkspaceMemoryDuplicateTx(ctx context.Context, tx *sql.Tx, record WorkspaceMemoryRecord) (string, string, bool, error) {
	return findWorkspaceMemoryDuplicateWithExcludeTx(ctx, tx, record, "")
}

func findWorkspaceMemoryDuplicateWithExcludeTx(ctx context.Context, tx *sql.Tx, record WorkspaceMemoryRecord, excludeMemoryID string) (string, string, bool, error) {
	var memoryID, createdAt string
	query := `SELECT memory_id, created_at
		   FROM workspace_memory
		  WHERE workspace_id = ?
		    AND archived_at IS NULL
		    AND memory_type = ?
		    AND title = ?
		    AND body = ?
		    AND source_kind = ?
		    AND source_id = ?
		    AND COALESCE(agent_id, '') = ?
		    AND COALESCE(session_id, '') = ?
		    AND COALESCE(task_id, '') = ?`
	args := []any{
		record.WorkspaceID,
		record.MemoryType,
		record.Title,
		record.Body,
		record.SourceKind,
		record.SourceID,
		record.AgentID,
		record.SessionID,
		record.TaskID,
	}
	if trimmed := strings.TrimSpace(excludeMemoryID); trimmed != "" {
		query += ` AND memory_id <> ?`
		args = append(args, trimmed)
	}
	query += ` LIMIT 1`
	err := tx.QueryRowContext(ctx, query, args...).Scan(&memoryID, &createdAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", "", false, nil
	case err != nil:
		return "", "", false, fmt.Errorf("query duplicate workspace memory: %w", err)
	default:
		return memoryID, createdAt, true, nil
	}
}

func findWorkspaceMemoryRecoverableDuplicateTx(ctx context.Context, tx *sql.Tx, record WorkspaceMemoryRecord) (WorkspaceMemoryRecord, bool, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT memory_id, workspace_id, memory_type, title, body, summary,
		        COALESCE(agent_id,''), COALESCE(session_id,''), COALESCE(task_id,''),
		        source_kind, source_id, tags_json, importance, confidence,
		        created_at, updated_at, archived_at, archived_by, archived_reason, recovery_reason
		   FROM workspace_memory
		  WHERE workspace_id = ?
		    AND archived_at IS NOT NULL
		    AND archived_reason = ?
		    AND memory_type = ?
		    AND title = ?
		    AND body = ?
		    AND source_kind = ?
		    AND source_id = ?
		    AND COALESCE(agent_id, '') = ?
		    AND COALESCE(session_id, '') = ?
		    AND COALESCE(task_id, '') = ?
		  ORDER BY updated_at DESC, memory_id DESC
		  LIMIT 1`,
		record.WorkspaceID,
		rmpArchivedReasonExpired,
		record.MemoryType,
		record.Title,
		record.Body,
		record.SourceKind,
		record.SourceID,
		record.AgentID,
		record.SessionID,
		record.TaskID,
	)
	archivedRecord, err := scanWorkspaceMemoryRecord(row)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return WorkspaceMemoryRecord{}, false, nil
	case err != nil:
		return WorkspaceMemoryRecord{}, false, fmt.Errorf("query recoverable archived workspace memory: %w", err)
	default:
		return archivedRecord, true, nil
	}
}

func loadWorkspaceMemoryRecordTx(ctx context.Context, tx *sql.Tx, workspaceID, memoryID string) (WorkspaceMemoryRecord, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT memory_id, workspace_id, memory_type, title, body, summary,
		        COALESCE(agent_id,''), COALESCE(session_id,''), COALESCE(task_id,''),
		        source_kind, source_id, tags_json, importance, confidence,
		        created_at, updated_at, archived_at, archived_by, archived_reason, recovery_reason
		   FROM workspace_memory
		  WHERE workspace_id = ? AND memory_id = ?`,
		workspaceID,
		memoryID,
	)
	return scanWorkspaceMemoryRecord(row)
}

func (s *Store) GetWorkspaceMemory(ctx context.Context, workspaceID, memoryID string) (WorkspaceMemoryRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	memoryID = strings.TrimSpace(memoryID)
	if workspaceID == "" {
		return WorkspaceMemoryRecord{}, errors.New("workspace_id is required")
	}
	if memoryID == "" {
		return WorkspaceMemoryRecord{}, errors.New("memory_id is required")
	}

	row := s.db.QueryRowContext(
		ctx,
		`SELECT memory_id, workspace_id, memory_type, title, body, summary,
		        COALESCE(agent_id,''), COALESCE(session_id,''), COALESCE(task_id,''),
		        source_kind, source_id, tags_json, importance, confidence,
		        created_at, updated_at, archived_at, archived_by, archived_reason, recovery_reason
		   FROM workspace_memory
		  WHERE workspace_id = ? AND memory_id = ?`,
		workspaceID,
		memoryID,
	)
	record, err := scanWorkspaceMemoryRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceMemoryRecord{}, fmt.Errorf("workspace memory not found: %s/%s", workspaceID, memoryID)
		}
		return WorkspaceMemoryRecord{}, err
	}
	return record, nil
}

func (s *Store) ListWorkspaceMemory(ctx context.Context, filter WorkspaceMemoryFilter) ([]WorkspaceMemoryRecord, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	base := strings.Builder{}
	base.WriteString(`SELECT memory_id, workspace_id, memory_type, title, body, summary,
	        COALESCE(agent_id,''), COALESCE(session_id,''), COALESCE(task_id,''),
	        source_kind, source_id, tags_json, importance, confidence,
	        created_at, updated_at, archived_at, archived_by, archived_reason, recovery_reason
	   FROM workspace_memory`)
	args := make([]any, 0, 8)
	where := buildWorkspaceMemoryWhere(filter, "", &args)
	if len(where) > 0 {
		base.WriteString(" WHERE ")
		base.WriteString(strings.Join(where, " AND "))
	}
	base.WriteString(` ORDER BY updated_at DESC, importance DESC, memory_id DESC LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, base.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list workspace memory: %w", err)
	}
	defer rows.Close()
	return collectWorkspaceMemoryRows(rows)
}

func (s *Store) SearchWorkspaceMemory(ctx context.Context, filter WorkspaceMemoryFilter) ([]WorkspaceMemoryRecord, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 20
	}
	if strings.TrimSpace(filter.Query) == "" {
		return s.ListWorkspaceMemory(ctx, filter)
	}

	args := make([]any, 0, 8)
	where := buildWorkspaceMemoryWhere(filter, "m", &args)
	queryText := strings.TrimSpace(filter.Query)
	ftsQuery := buildWorkspaceMemoryFTSQuery(queryText)
	useFTS := ftsQuery != ""

	sqlText := strings.Builder{}
	sqlText.WriteString(`SELECT m.memory_id, m.workspace_id, m.memory_type, m.title, m.body, m.summary,
	        COALESCE(m.agent_id,''), COALESCE(m.session_id,''), COALESCE(m.task_id,''),
	        m.source_kind, m.source_id, m.tags_json, m.importance, m.confidence,
	        m.created_at, m.updated_at, m.archived_at, m.archived_by, m.archived_reason, m.recovery_reason
	   FROM workspace_memory m`)
	if useFTS {
		sqlText.WriteString(` JOIN workspace_memory_fts ON workspace_memory_fts.rowid = m.rowid`)
	}

	clauses := make([]string, 0, len(where)+1)
	clauses = append(clauses, where...)
	if useFTS {
		clauses = append(clauses, `workspace_memory_fts MATCH ?`)
		args = append(args, ftsQuery)
	} else {
		needle := "%" + queryText + "%"
		clauses = append(clauses, `(m.memory_type LIKE ? OR m.title LIKE ? OR m.body LIKE ? OR m.summary LIKE ? OR m.tags_json LIKE ?)`)
		args = append(args, needle, needle, needle, needle, needle)
	}
	sqlText.WriteString(" WHERE ")
	sqlText.WriteString(strings.Join(clauses, " AND "))
	typePriority := workspaceMemoryTypePrioritySQL("m")
	sourcePriority := workspaceMemorySourcePrioritySQL("m")
	if useFTS {
		sqlText.WriteString(` ORDER BY bm25(workspace_memory_fts), ` + typePriority + ` DESC, ` + sourcePriority + ` DESC, m.importance DESC, m.updated_at DESC, m.memory_id DESC`)
	} else {
		sqlText.WriteString(` ORDER BY ` + typePriority + ` DESC, ` + sourcePriority + ` DESC, m.importance DESC, m.updated_at DESC, m.memory_id DESC`)
	}
	sqlText.WriteString(` LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, sqlText.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("search workspace memory: %w", err)
	}
	defer rows.Close()
	return collectWorkspaceMemoryRows(rows)
}

func (s *Store) ListSessionCompactionCandidates(ctx context.Context, filter SessionCompactionFilter) ([]SessionCompactionCandidate, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	query := strings.Builder{}
	query.WriteString(`SELECT s.session_id, s.workspace_id, s.agent_id, COALESCE(s.task_id,''), s.status,
	        COUNT(m.id) AS message_count,
	        COALESCE(SUM(m.token_count), 0) AS message_tokens,
	        s.total_input_tokens, s.total_output_tokens,
	        s.started_at,
	        COALESCE(MAX(m.created_at), s.started_at) AS last_message_at
	   FROM agent_sessions s
	   LEFT JOIN agent_session_messages m ON m.session_id = s.session_id`)
	args := []any{workspaceID}
	where := []string{`s.workspace_id = ?`}
	if trimmed := strings.TrimSpace(filter.AgentID); trimmed != "" {
		where = append(where, `s.agent_id = ?`)
		args = append(args, trimmed)
	}
	if filter.ActiveOnly {
		where = append(where, `s.status IN (?, ?, ?, ?, ?)`)
		args = append(args, "RUNNING", "ACTIVE", "BLOCKED", "WAITING_DECISION", "HANDOFF_PENDING")
	}
	query.WriteString(" WHERE ")
	query.WriteString(strings.Join(where, " AND "))
	query.WriteString(` GROUP BY s.session_id, s.workspace_id, s.agent_id, s.task_id, s.status,
	        s.total_input_tokens, s.total_output_tokens, s.started_at`)

	having := make([]string, 0, 2)
	if filter.MinMessages > 0 {
		having = append(having, `COUNT(m.id) >= ?`)
		args = append(args, filter.MinMessages)
	}
	if filter.MinTokens > 0 {
		having = append(having, `COALESCE(NULLIF(SUM(m.token_count), 0), (s.total_input_tokens + s.total_output_tokens)) >= ?`)
		args = append(args, filter.MinTokens)
	}
	if len(having) > 0 {
		query.WriteString(" HAVING ")
		query.WriteString(strings.Join(having, " OR "))
	}
	query.WriteString(` ORDER BY last_message_at DESC, s.started_at DESC, s.session_id DESC LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list session compaction candidates: %w", err)
	}
	defer rows.Close()

	out := []SessionCompactionCandidate{}
	for rows.Next() {
		var row SessionCompactionCandidate
		if err := rows.Scan(
			&row.SessionID,
			&row.WorkspaceID,
			&row.AgentID,
			&row.TaskID,
			&row.Status,
			&row.MessageCount,
			&row.MessageTokens,
			&row.TotalInputTokens,
			&row.TotalOutputTokens,
			&row.StartedAt,
			&row.LastMessageAt,
		); err != nil {
			return nil, fmt.Errorf("scan session compaction candidate: %w", err)
		}
		row.TotalTokens = row.TotalInputTokens + row.TotalOutputTokens
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate session compaction candidates: %w", err)
	}
	return out, nil
}

func (s *Store) ArchiveWorkspaceMemory(ctx context.Context, input WorkspaceMemoryArchiveInput) (WorkspaceMemoryRecord, error) {
	record, _, err := s.ArchiveWorkspaceMemoryWithEvent(ctx, input)
	return record, err
}

func (s *Store) ArchiveWorkspaceMemoryWithEvent(ctx context.Context, input WorkspaceMemoryArchiveInput) (WorkspaceMemoryRecord, RuntimeEventRecord, error) {
	record, event, _, err := s.ArchiveWorkspaceMemoryWithEffects(ctx, input)
	return record, event, err
}

func (s *Store) ArchiveWorkspaceMemoryWithEffects(ctx context.Context, input WorkspaceMemoryArchiveInput) (WorkspaceMemoryRecord, RuntimeEventRecord, *PromotedKnowledgeClaimSyncEffects, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace_id is required")
	}
	memoryID := strings.TrimSpace(input.MemoryID)
	if memoryID == "" {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, errors.New("memory_id is required")
	}
	archivedBy := strings.TrimSpace(input.ArchivedBy)
	if archivedBy == "" {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, errors.New("archived_by is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("begin workspace memory archive tx: %w", err)
	}
	var (
		event   RuntimeEventRecord
		effects *PromotedKnowledgeClaimSyncEffects
	)
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		var innerErr error
		_, event, effects, innerErr = s.archiveWorkspaceMemoryWithAuthorityTx(ctx, tx, input, authority, now)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("commit workspace memory archive tx: %w", err)
	}
	resolved, err := s.GetWorkspaceMemory(ctx, workspaceID, memoryID)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	return resolved, event, effects, nil
}

func (s *Store) archiveWorkspaceMemoryWithEffectsTx(ctx context.Context, tx *sql.Tx, input WorkspaceMemoryArchiveInput, now string) (WorkspaceMemoryRecord, RuntimeEventRecord, *PromotedKnowledgeClaimSyncEffects, error) {
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInputTx(ctx, tx, strings.TrimSpace(input.WorkspaceID), authorityScopeWorkspace, now)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	var (
		record  WorkspaceMemoryRecord
		event   RuntimeEventRecord
		effects *PromotedKnowledgeClaimSyncEffects
	)
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, event, effects, innerErr = s.archiveWorkspaceMemoryWithAuthorityTx(ctx, tx, input, authority, now)
		return innerErr
	}); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	return record, event, effects, nil
}

func (s *Store) archiveWorkspaceMemoryWithAuthorityTx(ctx context.Context, tx *sql.Tx, input WorkspaceMemoryArchiveInput, authority WorkspaceAuthorityRecord, now string) (WorkspaceMemoryRecord, RuntimeEventRecord, *PromotedKnowledgeClaimSyncEffects, error) {
	return s.archiveWorkspaceMemoryWithOptionalAuthorityTx(ctx, tx, input, &authority, now)
}

func (s *Store) archiveWorkspaceMemoryWithOptionalAuthorityTx(ctx context.Context, tx *sql.Tx, input WorkspaceMemoryArchiveInput, authority *WorkspaceAuthorityRecord, now string) (WorkspaceMemoryRecord, RuntimeEventRecord, *PromotedKnowledgeClaimSyncEffects, error) {
	if authority == nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace authority is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	memoryID := strings.TrimSpace(input.MemoryID)
	archivedBy := strings.TrimSpace(input.ArchivedBy)
	reason := strings.TrimSpace(input.Reason)
	record, err := loadWorkspaceMemoryRecordTx(ctx, tx, workspaceID, memoryID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("workspace memory not found: %s/%s", workspaceID, memoryID)
		}
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("query workspace memory for archive: %w", err)
	}
	archivePromptFields := workspaceMemoryPromptContextFields(record, "operator", archivedBy)
	archivePromptFields["archived_by"] = archivedBy
	if input.PromptContextEnvelope != nil {
		if _, err := attachWorkspaceMemoryPromptContextEnvelope(map[string]any{}, input.PromptContextEnvelope, "workspace.memory.remove", archivePromptFields); err != nil {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
		}
	}

	event := RuntimeEventRecord{}
	if record.ArchivedAt == nil || strings.TrimSpace(*record.ArchivedAt) == "" {
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE workspace_memory
			    SET archived_at = ?, archived_by = ?, archived_reason = ?, updated_at = ?
			  WHERE workspace_id = ? AND memory_id = ?`,
			now,
			archivedBy,
			reason,
			now,
			workspaceID,
			memoryID,
		); err != nil {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("archive workspace memory: %w", err)
		}

		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "workspace_memory_archived",
			EntityType: "workspace_memory",
			EntityID:   memoryID,
			ActorID:    archivedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":    workspaceID,
				"memory_id":       memoryID,
				"archived_by":     archivedBy,
				"archived_reason": reason,
			}),
		}); err != nil {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
		}
		eventPayload := map[string]any{
			"workspace_id": workspaceID,
			"memory_id":    memoryID,
			"archived_by":  archivedBy,
			"reason":       reason,
		}
		eventPayload, err = attachWorkspaceMemoryPromptContextEnvelope(eventPayload, input.PromptContextEnvelope, "workspace.memory.remove", archivePromptFields)
		if err != nil {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
		}
		appendInput := RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: record.WorkspaceID,
			EventType:   "workspace_memory.archived",
			EntityType:  "workspace_memory",
			EntityID:    record.MemoryID,
			ActorType:   "operator",
			ActorID:     archivedBy,
			AgentID:     record.AgentID,
			SessionID:   record.SessionID,
			TaskID:      record.TaskID,
			PayloadJSON: mustJSON(eventPayload),
			CreatedAt:   now,
		}
		event, err = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, *authority, appendInput)
		if err != nil {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("touch workspace after memory archive: %w", err)
		}
		record.UpdatedAt = now
		record.ArchivedAt = &now
		record.ArchivedBy = &archivedBy
		record.ArchivedReason = reason
	}
	effects, err := s.syncPromotedKnowledgeClaimForMemoryTx(ctx, tx, *authority, record, archivedBy, reason, now)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}

	if err := s.syncWorkspaceMemoryGraphAnchorTx(ctx, tx, record); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("sync memory graph anchor: %w", err)
	}
	if err := s.enqueueMemoryProjectionOutboxTx(ctx, tx, record.WorkspaceID, memoryProjectionKindWorkspaceMemory, record.MemoryID, record.UpdatedAt); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("enqueue workspace memory projection: %w", err)
	}
	return record, event, effects, nil
}

func (s *Store) RestoreWorkspaceMemory(ctx context.Context, input WorkspaceMemoryRestoreInput) (WorkspaceMemoryRecord, error) {
	record, _, err := s.RestoreWorkspaceMemoryWithEvent(ctx, input)
	return record, err
}

func (s *Store) RestoreWorkspaceMemoryWithEvent(ctx context.Context, input WorkspaceMemoryRestoreInput) (WorkspaceMemoryRecord, RuntimeEventRecord, error) {
	record, event, _, err := s.RestoreWorkspaceMemoryWithEffects(ctx, input)
	return record, event, err
}

func (s *Store) RestoreWorkspaceMemoryWithEffects(ctx context.Context, input WorkspaceMemoryRestoreInput) (WorkspaceMemoryRecord, RuntimeEventRecord, *PromotedKnowledgeClaimSyncEffects, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, errors.New("workspace_id is required")
	}
	memoryID := strings.TrimSpace(input.MemoryID)
	if memoryID == "" {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, errors.New("memory_id is required")
	}
	restoredBy := strings.TrimSpace(input.RestoredBy)
	if restoredBy == "" {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, errors.New("restored_by is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("begin workspace memory restore tx: %w", err)
	}
	var (
		record  WorkspaceMemoryRecord
		event   RuntimeEventRecord
		effects *PromotedKnowledgeClaimSyncEffects
	)
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}

		var innerErr error
		record, innerErr = loadWorkspaceMemoryRecordTx(ctx, tx, workspaceID, memoryID)
		if innerErr != nil {
			if errors.Is(innerErr, sql.ErrNoRows) {
				return fmt.Errorf("workspace memory not found: %s/%s", workspaceID, memoryID)
			}
			return fmt.Errorf("query workspace memory for restore: %w", innerErr)
		}
		restorePromptFields := workspaceMemoryPromptContextFields(record, "operator", restoredBy)
		restorePromptFields["restored_by"] = restoredBy
		if input.PromptContextEnvelope != nil {
			if _, innerErr = attachWorkspaceMemoryPromptContextEnvelope(map[string]any{}, input.PromptContextEnvelope, "workspace.memory.restore", restorePromptFields); innerErr != nil {
				return innerErr
			}
		}

		if record.ArchivedAt == nil || strings.TrimSpace(*record.ArchivedAt) == "" {
			effects, innerErr = s.syncPromotedKnowledgeClaimForMemoryTx(ctx, tx, authority, record, restoredBy, "workspace_memory_restore_noop", now)
			return innerErr
		}

		if duplicateID, _, found, innerErr := findWorkspaceMemoryDuplicateWithExcludeTx(ctx, tx, record, record.MemoryID); innerErr != nil {
			return innerErr
		} else if found {
			return fmt.Errorf("workspace memory restore conflict: active duplicate exists: %s", duplicateID)
		}

		record, event, innerErr = s.restoreWorkspaceMemoryArchivedWithAuthorityTx(ctx, tx, record, "operator", restoredBy, input.RecoveryReason, authority, input.PromptContextEnvelope, "workspace.memory.restore", now)
		if innerErr != nil {
			return innerErr
		}
		effects, innerErr = s.syncPromotedKnowledgeClaimForMemoryTx(ctx, tx, authority, record, restoredBy, "workspace_memory_restored", now)
		if innerErr != nil {
			return innerErr
		}

		if innerErr := s.syncWorkspaceMemoryGraphAnchorTx(ctx, tx, record); innerErr != nil {
			return fmt.Errorf("sync memory graph anchor: %w", innerErr)
		}
		if innerErr := s.enqueueMemoryProjectionOutboxTx(ctx, tx, record.WorkspaceID, memoryProjectionKindWorkspaceMemory, record.MemoryID, record.UpdatedAt); innerErr != nil {
			return fmt.Errorf("enqueue workspace memory projection: %w", innerErr)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, fmt.Errorf("commit workspace memory restore tx: %w", err)
	}
	resolved, err := s.GetWorkspaceMemory(ctx, workspaceID, memoryID)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, nil, err
	}
	return resolved, event, effects, nil
}

func (s *Store) restoreWorkspaceMemoryArchivedTx(ctx context.Context, tx *sql.Tx, record WorkspaceMemoryRecord, actorType, actorID, recoveryReason, now string) (WorkspaceMemoryRecord, RuntimeEventRecord, error) {
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInputTx(ctx, tx, strings.TrimSpace(record.WorkspaceID), authorityScopeWorkspace, now)
	if err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, err
	}
	var (
		restored WorkspaceMemoryRecord
		event    RuntimeEventRecord
	)
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		restored, event, innerErr = s.restoreWorkspaceMemoryArchivedWithAuthorityTx(ctx, tx, record, actorType, actorID, recoveryReason, authority, nil, "", now)
		return innerErr
	}); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, err
	}
	return restored, event, nil
}

func (s *Store) restoreWorkspaceMemoryArchivedWithAuthorityTx(ctx context.Context, tx *sql.Tx, record WorkspaceMemoryRecord, actorType, actorID, recoveryReason string, authority WorkspaceAuthorityRecord, promptContextEnvelope map[string]any, expectedSurface string, now string) (WorkspaceMemoryRecord, RuntimeEventRecord, error) {
	return s.restoreWorkspaceMemoryArchivedWithOptionalAuthorityTx(ctx, tx, record, actorType, actorID, recoveryReason, &authority, promptContextEnvelope, expectedSurface, now)
}

func (s *Store) restoreWorkspaceMemoryArchivedWithOptionalAuthorityTx(ctx context.Context, tx *sql.Tx, record WorkspaceMemoryRecord, actorType, actorID, recoveryReason string, authority *WorkspaceAuthorityRecord, promptContextEnvelope map[string]any, expectedSurface string, now string) (WorkspaceMemoryRecord, RuntimeEventRecord, error) {
	if authority == nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, errors.New("workspace authority is required")
	}
	workspaceID := strings.TrimSpace(record.WorkspaceID)
	memoryID := strings.TrimSpace(record.MemoryID)
	actorType = firstNonEmpty(strings.TrimSpace(actorType), "operator")
	actorID = firstNonEmpty(strings.TrimSpace(actorID), workspaceID)
	if strings.TrimSpace(expectedSurface) == "" {
		expectedSurface = "workspace.memory.restore"
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE workspace_memory
		    SET archived_at = NULL, archived_by = NULL, archived_reason = '', recovery_reason = ?, updated_at = ?
		  WHERE workspace_id = ? AND memory_id = ?`,
		recoveryReason,
		now,
		workspaceID,
		memoryID,
	); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, fmt.Errorf("restore workspace memory: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "workspace_memory_restored",
		EntityType: "workspace_memory",
		EntityID:   memoryID,
		ActorID:    actorID,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id":    workspaceID,
			"memory_id":       memoryID,
			"restored_by":     actorID,
			"recovery_reason": recoveryReason,
		}),
	}); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, err
	}
	event := RuntimeEventRecord{}
	eventPayload := map[string]any{
		"workspace_id":    workspaceID,
		"memory_id":       memoryID,
		"restored_by":     actorID,
		"recovery_reason": recoveryReason,
	}
	restorePromptFields := workspaceMemoryPromptContextFields(record, actorType, actorID)
	restorePromptFields["restored_by"] = actorID
	eventPayload, attachErr := attachWorkspaceMemoryPromptContextEnvelope(eventPayload, promptContextEnvelope, expectedSurface, restorePromptFields)
	if attachErr != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, attachErr
	}
	appendInput := RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: record.WorkspaceID,
		EventType:   "workspace_memory.restored",
		EntityType:  "workspace_memory",
		EntityID:    record.MemoryID,
		ActorType:   actorType,
		ActorID:     actorID,
		AgentID:     record.AgentID,
		SessionID:   record.SessionID,
		TaskID:      record.TaskID,
		PayloadJSON: mustJSON(eventPayload),
		CreatedAt:   now,
	}
	var appendErr error
	event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, *authority, appendInput)
	if appendErr != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, appendErr
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		return WorkspaceMemoryRecord{}, RuntimeEventRecord{}, fmt.Errorf("touch workspace after memory restore: %w", err)
	}
	record.UpdatedAt = now
	record.ArchivedAt = nil
	record.ArchivedBy = nil
	record.ArchivedReason = ""
	record.RecoveryReason = recoveryReason
	return record, event, nil
}

func runtimeEventParentRefsJSONForRecord(parentIDs ...string) string {
	refs := make([]string, 0, len(parentIDs))
	for _, parentID := range parentIDs {
		if trimmed := strings.TrimSpace(parentID); trimmed != "" {
			refs = append(refs, trimmed)
		}
	}
	if len(refs) == 0 {
		return "[]"
	}
	encoded, err := json.Marshal(refs)
	if err != nil {
		return "[]"
	}
	normalized, err := normalizeRuntimeEventParentRefs(string(encoded))
	if err != nil {
		return "[]"
	}
	return normalized
}

func collectWorkspaceMemoryRows(rows *sql.Rows) ([]WorkspaceMemoryRecord, error) {
	out := []WorkspaceMemoryRecord{}
	for rows.Next() {
		record, err := scanWorkspaceMemoryRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace memory rows: %w", err)
	}
	return out, nil
}

func scanWorkspaceMemoryRecord(scanner interface {
	Scan(dest ...any) error
}) (WorkspaceMemoryRecord, error) {
	var record WorkspaceMemoryRecord
	var tagsJSON string
	var archivedAt sql.NullString
	var archivedBy sql.NullString
	if err := scanner.Scan(
		&record.MemoryID,
		&record.WorkspaceID,
		&record.MemoryType,
		&record.Title,
		&record.Body,
		&record.Summary,
		&record.AgentID,
		&record.SessionID,
		&record.TaskID,
		&record.SourceKind,
		&record.SourceID,
		&tagsJSON,
		&record.Importance,
		&record.Confidence,
		&record.CreatedAt,
		&record.UpdatedAt,
		&archivedAt,
		&archivedBy,
		&record.ArchivedReason,
		&record.RecoveryReason,
	); err != nil {
		return WorkspaceMemoryRecord{}, err
	}
	record.Tags = decodeCapabilities(tagsJSON)
	if record.Tags == nil {
		record.Tags = []string{}
	}
	record.ArchivedAt = nullStringPtr(archivedAt)
	record.ArchivedBy = nullStringPtr(archivedBy)
	return record, nil
}

func buildWorkspaceMemoryWhere(filter WorkspaceMemoryFilter, alias string, args *[]any) []string {
	prefix := ""
	if trimmed := strings.TrimSpace(alias); trimmed != "" {
		prefix = trimmed + "."
	}

	where := []string{prefix + `workspace_id = ?`}
	*args = append(*args, strings.TrimSpace(filter.WorkspaceID))
	if !filter.IncludeArchived {
		where = append(where, prefix+`archived_at IS NULL`)
	}

	if raw := strings.TrimSpace(filter.MemoryType); raw != "" {
		where = append(where, prefix+`memory_type = ?`)
		*args = append(*args, normalizeWorkspaceMemoryType(raw))
	}
	if trimmed := strings.TrimSpace(filter.AgentID); trimmed != "" {
		where = append(where, prefix+`agent_id = ?`)
		*args = append(*args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.SessionID); trimmed != "" {
		where = append(where, prefix+`session_id = ?`)
		*args = append(*args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.TaskID); trimmed != "" {
		where = append(where, prefix+`task_id = ?`)
		*args = append(*args, trimmed)
	}
	if raw := strings.TrimSpace(filter.SourceKind); raw != "" {
		where = append(where, prefix+`source_kind = ?`)
		*args = append(*args, normalizeWorkspaceMemorySourceKind(raw))
	}
	if trimmed := strings.TrimSpace(filter.SourceID); trimmed != "" {
		where = append(where, prefix+`source_id = ?`)
		*args = append(*args, trimmed)
	}
	return where
}

func buildWorkspaceMemoryFTSQuery(raw string) string {
	tokens := strings.Fields(strings.TrimSpace(raw))
	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		token = strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' {
				return r
			}
			return -1
		}, token)
		if token == "" {
			continue
		}
		out = append(out, token+"*")
	}
	return strings.Join(out, " AND ")
}

func workspaceMemoryTypePrioritySQL(alias string) string {
	prefix := ""
	if trimmed := strings.TrimSpace(alias); trimmed != "" {
		prefix = trimmed + "."
	}
	return `CASE ` + prefix + `memory_type
		WHEN 'DECISION' THEN 90
		WHEN 'PROCEDURE' THEN 80
		WHEN 'ANTI_PROCEDURE' THEN 80
		WHEN 'INCIDENT' THEN 75
		WHEN 'LESSON' THEN 70
		WHEN 'ENTITY' THEN 60
		WHEN 'EXPERIENCE' THEN 55
		WHEN 'UPDATE_DIGEST' THEN 45
		WHEN 'SUMMARY' THEN 35
		WHEN 'NOTE' THEN 20
		ELSE 10
	END`
}

func workspaceMemorySourcePrioritySQL(alias string) string {
	prefix := ""
	if trimmed := strings.TrimSpace(alias); trimmed != "" {
		prefix = trimmed + "."
	}
	return `CASE ` + prefix + `source_kind
		WHEN 'manual' THEN 40
		WHEN 'reflection' THEN 30
		WHEN 'session_event' THEN 25
		WHEN 'compaction' THEN 20
		WHEN 'system' THEN 15
		ELSE 10
	END`
}

func normalizeWorkspaceMemoryType(raw string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(raw))
	if trimmed == "" {
		return "NOTE"
	}
	return trimmed
}

func normalizeWorkspaceMemorySourceKind(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "manual"
	}
	return strings.ToLower(trimmed)
}

func workspaceMemoryRuntimeActorType(record WorkspaceMemoryRecord) string {
	if strings.TrimSpace(record.AgentID) != "" {
		return "agent"
	}
	return "system"
}

func workspaceMemoryPromptContextFields(record WorkspaceMemoryRecord, actorType, actorID string) map[string]string {
	return map[string]string{
		"workspace_id": strings.TrimSpace(record.WorkspaceID),
		"memory_id":    strings.TrimSpace(record.MemoryID),
		"memory_type":  strings.TrimSpace(record.MemoryType),
		"source_kind":  strings.TrimSpace(record.SourceKind),
		"source_id":    strings.TrimSpace(record.SourceID),
		"agent_id":     strings.TrimSpace(record.AgentID),
		"session_id":   strings.TrimSpace(record.SessionID),
		"task_id":      strings.TrimSpace(record.TaskID),
		"actor_type":   strings.TrimSpace(actorType),
		"actor_id":     strings.TrimSpace(actorID),
	}
}

func workspaceMemoryRecordPromptContextSurface(envelope map[string]any) string {
	if executionPromptRawString(envelope, "surface") == "workspace.memory.node.write" {
		return "workspace.memory.node.write"
	}
	return "workspace.memory.write"
}

func clampUnitInterval(v float64) float64 {
	switch {
	case v < 0:
		return 0
	case v > 1:
		return 1
	default:
		return v
	}
}

func (s *Store) ensureAgentSessionInWorkspaceTx(ctx context.Context, tx *sql.Tx, workspaceID, sessionID string) error {
	var count int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(1) FROM agent_sessions WHERE workspace_id = ? AND session_id = ?`,
		workspaceID,
		sessionID,
	).Scan(&count); err != nil {
		return fmt.Errorf("check agent session existence: %w", err)
	}
	if count == 0 {
		return ErrSessionNotFound
	}
	return nil
}
