package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrTelegramMessageNotFound = errors.New("telegram message mapping not found")

type PendingHumanUpdateRecord struct {
	UpdateID         string `json:"update_id"`
	WorkspaceID      string `json:"workspace_id"`
	AgentID          string `json:"agent_id"`
	AgentName        string `json:"agent_name"`
	AgentOwnerUserID string `json:"agent_owner_user_id"`
	UpdateType       string `json:"update_type"`
	Summary          string `json:"summary"`
	PayloadJSON      string `json:"payload_json,omitempty"`
	RequiresHuman    bool   `json:"requires_human"`
	CreatedAt        string `json:"created_at"`
}

type TelegramMessageMapInput struct {
	WorkspaceID       string
	SourceUpdateID    string
	TaskID            string
	AgentID           string
	TargetUserID      string
	TelegramChatID    int64
	TelegramMessageID int
}

type TelegramMessageMapRecord struct {
	WorkspaceID          string  `json:"workspace_id"`
	SourceUpdateID       string  `json:"source_update_id"`
	TaskID               *string `json:"task_id,omitempty"`
	AgentID              string  `json:"agent_id"`
	TargetUserID         *string `json:"target_user_id,omitempty"`
	TelegramChatID       int64   `json:"telegram_chat_id"`
	TelegramMessageID    int     `json:"telegram_message_id"`
	ReplyUpdateID        *string `json:"reply_update_id,omitempty"`
	SupersededByUpdateID *string `json:"superseded_by_update_id,omitempty"`
	SentAt               string  `json:"sent_at"`
	RepliedAt            *string `json:"replied_at,omitempty"`
	SupersededAt         *string `json:"superseded_at,omitempty"`
}

type TelegramDMInput struct {
	WorkspaceID       string
	MessageID         string
	FromAgentID       string
	ToAgentID         string
	TelegramChatID    int64
	TelegramMessageID int
}

type TelegramDMRecord struct {
	WorkspaceID       string  `json:"workspace_id"`
	MessageID         string  `json:"message_id"`
	FromAgentID       string  `json:"from_agent_id"`
	ToAgentID         string  `json:"to_agent_id"`
	TelegramChatID    int64   `json:"telegram_chat_id"`
	TelegramMessageID int     `json:"telegram_message_id"`
	ReplyMessageID    *string `json:"reply_message_id,omitempty"`
	SentAt            string  `json:"sent_at"`
	RepliedAt         *string `json:"replied_at,omitempty"`
}

type OpenHumanAlertRecord struct {
	PendingHumanUpdateRecord
	TelegramMessageID    *int    `json:"telegram_message_id,omitempty"`
	SentAt               *string `json:"sent_at,omitempty"`
	RepliedAt            *string `json:"replied_at,omitempty"`
	SupersededAt         *string `json:"superseded_at,omitempty"`
	SupersededByUpdateID *string `json:"superseded_by_update_id,omitempty"`
}

type TelegramReplyInput struct {
	ReplyUpdateID       string
	FollowUpUpdateID    string
	BridgeAgentID       string
	TelegramChatID      int64
	ReplyToMessageID    int
	UpdateType          string
	Summary             string
	PayloadJSON         string
	FollowUpUpdateType  string
	FollowUpSummary     string
	FollowUpPayloadJSON string
}

type TelegramAlertClosureInput struct {
	ClosureUpdateID string
	BridgeAgentID   string
	TelegramChatID  int64
	MessageID       int
	UpdateType      string
	Summary         string
	PayloadJSON     string
}

func (s *Store) ListPendingTelegramAlerts(ctx context.Context, workspaceID string, limit int) ([]PendingHumanUpdateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if limit <= 0 {
		limit = 25
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT u.update_id, u.workspace_id, u.agent_id, a.display_name, a.owner_user_id,
		        u.update_type, u.summary, u.payload_json, u.requires_human, u.created_at
		 FROM agent_updates u
		 JOIN agents a ON a.agent_id = u.agent_id AND a.workspace_id = u.workspace_id
		 LEFT JOIN tg_message_map m ON m.source_update_id = u.update_id
		 WHERE u.workspace_id = ?
		   AND m.source_update_id IS NULL
		   AND (
		     u.requires_human = 1
		     OR LOWER(u.update_type) IN ('blocker', 'escalation', 'needs_input')
		     OR INSTR(LOWER(COALESCE(u.payload_json, '')), '\"human_reason\"') > 0
		     OR INSTR(LOWER(COALESCE(u.payload_json, '')), '\"status\":\"blocked\"') > 0
		   )
		 ORDER BY u.created_at ASC, u.update_id ASC
		 LIMIT ?`,
		workspaceID,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query pending telegram alerts: %w", err)
	}
	defer rows.Close()

	out := []PendingHumanUpdateRecord{}
	for rows.Next() {
		var row PendingHumanUpdateRecord
		var requiresHuman int
		if err := rows.Scan(
			&row.UpdateID,
			&row.WorkspaceID,
			&row.AgentID,
			&row.AgentName,
			&row.AgentOwnerUserID,
			&row.UpdateType,
			&row.Summary,
			&row.PayloadJSON,
			&requiresHuman,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending telegram alert: %w", err)
		}
		row.RequiresHuman = requiresHuman != 0
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending telegram alerts: %w", err)
	}
	return out, nil
}

func (s *Store) ListOpenTelegramAlerts(ctx context.Context, workspaceID string, limit int) ([]OpenHumanAlertRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if limit <= 0 {
		limit = 25
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT u.update_id, u.workspace_id, u.agent_id, a.display_name, a.owner_user_id,
		        u.update_type, u.summary, u.payload_json, u.requires_human, u.created_at,
		        m.telegram_message_id, m.sent_at, m.replied_at, m.superseded_at, m.superseded_by_update_id
		 FROM agent_updates u
		 JOIN agents a ON a.agent_id = u.agent_id AND a.workspace_id = u.workspace_id
		 LEFT JOIN tg_message_map m ON m.source_update_id = u.update_id
		 WHERE u.workspace_id = ?
		   AND (
		     u.requires_human = 1
		     OR LOWER(u.update_type) IN ('blocker', 'escalation', 'needs_input')
		     OR INSTR(LOWER(COALESCE(u.payload_json, '')), '\"human_reason\"') > 0
		     OR INSTR(LOWER(COALESCE(u.payload_json, '')), '\"status\":\"blocked\"') > 0
		   )
		   AND (m.source_update_id IS NULL OR (m.replied_at IS NULL AND m.superseded_at IS NULL))
		   AND (
		     m.source_update_id IS NULL
		     OR NOT EXISTS (
		       SELECT 1
		       FROM tg_message_map newer
		       WHERE newer.workspace_id = m.workspace_id
		         AND newer.agent_id = m.agent_id
		         AND newer.source_update_id <> m.source_update_id
		         AND (
		           (newer.task_id = m.task_id)
		           OR (newer.task_id IS NULL AND m.task_id IS NULL)
		         )
		         AND newer.sent_at > m.sent_at
		     )
		   )
		 ORDER BY u.created_at DESC, u.update_id DESC
		 LIMIT ?`,
		workspaceID,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query open telegram alerts: %w", err)
	}
	defer rows.Close()

	out := []OpenHumanAlertRecord{}
	for rows.Next() {
		var row OpenHumanAlertRecord
		var requiresHuman int
		var telegramMessageID sql.NullInt64
		var sentAt, repliedAt, supersededAt, supersededByUpdateID sql.NullString
		if err := rows.Scan(
			&row.UpdateID,
			&row.WorkspaceID,
			&row.AgentID,
			&row.AgentName,
			&row.AgentOwnerUserID,
			&row.UpdateType,
			&row.Summary,
			&row.PayloadJSON,
			&requiresHuman,
			&row.CreatedAt,
			&telegramMessageID,
			&sentAt,
			&repliedAt,
			&supersededAt,
			&supersededByUpdateID,
		); err != nil {
			return nil, fmt.Errorf("scan open telegram alert: %w", err)
		}
		row.RequiresHuman = requiresHuman != 0
		if telegramMessageID.Valid {
			value := int(telegramMessageID.Int64)
			row.TelegramMessageID = &value
		}
		row.SentAt = nullStringPtr(sentAt)
		row.RepliedAt = nullStringPtr(repliedAt)
		row.SupersededAt = nullStringPtr(supersededAt)
		row.SupersededByUpdateID = nullStringPtr(supersededByUpdateID)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate open telegram alerts: %w", err)
	}
	return out, nil
}

func (s *Store) RecordTelegramMessage(ctx context.Context, input TelegramMessageMapInput) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	sourceUpdateID := strings.TrimSpace(input.SourceUpdateID)
	if sourceUpdateID == "" {
		return errors.New("source_update_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return errors.New("agent_id is required")
	}
	if input.TelegramMessageID <= 0 {
		return errors.New("telegram_message_id must be positive")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin telegram message map tx: %w", err)
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO tg_message_map(
		   workspace_id, source_update_id, task_id, agent_id, target_user_id,
		   telegram_chat_id, telegram_message_id, reply_update_id, sent_at, replied_at, superseded_by_update_id, superseded_at
		 ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, NULL)`,
		workspaceID,
		sourceUpdateID,
		nullIfBlank(input.TaskID),
		agentID,
		nullIfBlank(input.TargetUserID),
		input.TelegramChatID,
		input.TelegramMessageID,
		now,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert telegram message map: %w", err)
	}
	if err := s.supersedeTelegramAlertsTx(ctx, tx, workspaceID, strings.TrimSpace(input.TaskID), agentID, sourceUpdateID, sourceUpdateID, now); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit telegram message map tx: %w", err)
	}
	return nil
}

func (s *Store) RecordTelegramDM(ctx context.Context, input TelegramDMInput) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	messageID := strings.TrimSpace(input.MessageID)
	if messageID == "" {
		return errors.New("message_id is required")
	}
	if input.TelegramChatID == 0 {
		return errors.New("telegram_chat_id is required")
	}
	if input.TelegramMessageID == 0 {
		return errors.New("telegram_message_id is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO telegram_dm_map(
			workspace_id, message_id, from_agent_id, to_agent_id, telegram_chat_id, telegram_message_id, sent_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(telegram_chat_id, telegram_message_id) DO NOTHING`,
		workspaceID, messageID, input.FromAgentID, input.ToAgentID, input.TelegramChatID, input.TelegramMessageID, now,
	)
	if err != nil {
		return fmt.Errorf("insert telegram dm map: %w", err)
	}
	return nil
}

func (s *Store) GetTelegramDM(ctx context.Context, telegramChatID int64, telegramMessageID int) (TelegramDMRecord, error) {
	if telegramChatID == 0 {
		return TelegramDMRecord{}, errors.New("telegram_chat_id is required")
	}
	if telegramMessageID == 0 {
		return TelegramDMRecord{}, errors.New("telegram_message_id is required")
	}

	var row TelegramDMRecord
	var replyMsgID, repliedAt sql.NullString
	if err := s.db.QueryRowContext(ctx,
		`SELECT workspace_id, message_id, from_agent_id, to_agent_id, telegram_chat_id, telegram_message_id, reply_message_id, sent_at, replied_at
		   FROM telegram_dm_map
		  WHERE telegram_chat_id = ? AND telegram_message_id = ?`,
		telegramChatID, telegramMessageID,
	).Scan(
		&row.WorkspaceID, &row.MessageID, &row.FromAgentID, &row.ToAgentID,
		&row.TelegramChatID, &row.TelegramMessageID, &replyMsgID, &row.SentAt, &repliedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return TelegramDMRecord{}, ErrTelegramMessageNotFound
		}
		return TelegramDMRecord{}, fmt.Errorf("query telegram dm map: %w", err)
	}
	if replyMsgID.Valid {
		row.ReplyMessageID = &replyMsgID.String
	}
	if repliedAt.Valid {
		row.RepliedAt = &repliedAt.String
	}
	return row, nil
}

func (s *Store) GetTelegramMessageMap(ctx context.Context, chatID int64, messageID int) (TelegramMessageMapRecord, error) {
	if messageID <= 0 {
		return TelegramMessageMapRecord{}, errors.New("telegram_message_id must be positive")
	}

	var record TelegramMessageMapRecord
	var taskID, targetUserID, replyUpdateID, supersededByUpdateID, repliedAt, supersededAt sql.NullString
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT workspace_id, source_update_id, task_id, agent_id, target_user_id,
		        telegram_chat_id, telegram_message_id, reply_update_id, superseded_by_update_id, sent_at, replied_at, superseded_at
		 FROM tg_message_map
		 WHERE telegram_chat_id = ? AND telegram_message_id = ?`,
		chatID,
		messageID,
	).Scan(
		&record.WorkspaceID,
		&record.SourceUpdateID,
		&taskID,
		&record.AgentID,
		&targetUserID,
		&record.TelegramChatID,
		&record.TelegramMessageID,
		&replyUpdateID,
		&supersededByUpdateID,
		&record.SentAt,
		&repliedAt,
		&supersededAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return TelegramMessageMapRecord{}, ErrTelegramMessageNotFound
		}
		return TelegramMessageMapRecord{}, fmt.Errorf("query telegram message map: %w", err)
	}

	record.TaskID = nullStringPtr(taskID)
	record.TargetUserID = nullStringPtr(targetUserID)
	record.ReplyUpdateID = nullStringPtr(replyUpdateID)
	record.SupersededByUpdateID = nullStringPtr(supersededByUpdateID)
	record.RepliedAt = nullStringPtr(repliedAt)
	record.SupersededAt = nullStringPtr(supersededAt)
	return record, nil
}

func (s *Store) MarkTelegramMessageReplied(ctx context.Context, chatID int64, messageID int, replyUpdateID string) error {
	replyUpdateID = strings.TrimSpace(replyUpdateID)
	if replyUpdateID == "" {
		return errors.New("reply_update_id is required")
	}

	record, err := s.GetTelegramMessageMap(ctx, chatID, messageID)
	if err != nil {
		return err
	}
	if record.RepliedAt != nil {
		return nil
	}

	result, err := s.writeDB.ExecContext(
		ctx,
		`UPDATE tg_message_map
		 SET reply_update_id = ?, replied_at = ?
		 WHERE telegram_chat_id = ? AND telegram_message_id = ? AND replied_at IS NULL`,
		replyUpdateID,
		time.Now().UTC().Format(time.RFC3339Nano),
		chatID,
		messageID,
	)
	if err != nil {
		return fmt.Errorf("update telegram message reply: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for telegram reply update: %w", err)
	}
	if affected == 0 {
		return ErrTelegramMessageNotFound
	}
	return nil
}

func (s *Store) RecordTelegramReply(ctx context.Context, input TelegramReplyInput) error {
	replyUpdateID := strings.TrimSpace(input.ReplyUpdateID)
	if replyUpdateID == "" {
		return errors.New("reply_update_id is required")
	}
	followUpUpdateID := strings.TrimSpace(input.FollowUpUpdateID)
	bridgeAgentID := strings.TrimSpace(input.BridgeAgentID)
	if bridgeAgentID == "" {
		return errors.New("bridge_agent_id is required")
	}
	if input.ReplyToMessageID <= 0 {
		return errors.New("reply_to_message_id must be positive")
	}
	updateType := strings.TrimSpace(input.UpdateType)
	if updateType == "" {
		updateType = "human_reply"
	}
	summary := strings.TrimSpace(input.Summary)
	if summary == "" {
		return errors.New("summary is required")
	}
	followUpUpdateType := strings.TrimSpace(input.FollowUpUpdateType)
	followUpSummary := strings.TrimSpace(input.FollowUpSummary)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin telegram reply tx: %w", err)
	}

	var workspaceID, sourceUpdateID, agentID string
	var taskID, repliedAt sql.NullString
	if err := tx.QueryRowContext(
		ctx,
		`SELECT workspace_id, source_update_id, task_id, agent_id, replied_at
		 FROM tg_message_map
		 WHERE telegram_chat_id = ? AND telegram_message_id = ?`,
		input.TelegramChatID,
		input.ReplyToMessageID,
	).Scan(&workspaceID, &sourceUpdateID, &taskID, &agentID, &repliedAt); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTelegramMessageNotFound
		}
		return fmt.Errorf("query telegram reply context: %w", err)
	}
	if repliedAt.Valid {
		_ = tx.Rollback()
		return nil
	}

	if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, bridgeAgentID); err != nil {
		_ = tx.Rollback()
		return err
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO agent_updates(update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		replyUpdateID,
		workspaceID,
		bridgeAgentID,
		updateType,
		summary,
		strings.TrimSpace(input.PayloadJSON),
		0,
		now,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert telegram reply agent update: %w", err)
	}
	if followUpUpdateID != "" && followUpUpdateType != "" && followUpSummary != "" {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO agent_updates(update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			followUpUpdateID,
			workspaceID,
			bridgeAgentID,
			followUpUpdateType,
			followUpSummary,
			strings.TrimSpace(input.FollowUpPayloadJSON),
			0,
			now,
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert telegram follow-up agent update: %w", err)
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE tg_message_map
		 SET reply_update_id = ?, replied_at = ?
		 WHERE telegram_chat_id = ? AND telegram_message_id = ?`,
		replyUpdateID,
		now,
		input.TelegramChatID,
		input.ReplyToMessageID,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("mark telegram message replied: %w", err)
	}
	closureUpdateID := replyUpdateID
	if followUpUpdateID != "" {
		closureUpdateID = followUpUpdateID
	}
	if err := s.supersedeTelegramAlertsTx(ctx, tx, workspaceID, taskID.String, agentID, sourceUpdateID, closureUpdateID, now); err != nil {
		_ = tx.Rollback()
		return err
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("touch workspace after telegram reply: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "telegram_reply_recorded",
		EntityType: "agent_update",
		EntityID:   replyUpdateID,
		ActorID:    bridgeAgentID,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id":        workspaceID,
			"telegram_chat_id":    input.TelegramChatID,
			"reply_to_message_id": input.ReplyToMessageID,
			"bridge_agent_id":     bridgeAgentID,
			"update_type":         updateType,
		}),
	}); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit telegram reply tx: %w", err)
	}
	return nil
}

func (s *Store) CloseTelegramAlert(ctx context.Context, input TelegramAlertClosureInput) error {
	closureUpdateID := strings.TrimSpace(input.ClosureUpdateID)
	if closureUpdateID == "" {
		return errors.New("closure_update_id is required")
	}
	bridgeAgentID := strings.TrimSpace(input.BridgeAgentID)
	if bridgeAgentID == "" {
		return errors.New("bridge_agent_id is required")
	}
	if input.MessageID <= 0 {
		return errors.New("message_id must be positive")
	}
	updateType := strings.TrimSpace(input.UpdateType)
	if updateType == "" {
		updateType = "human_alert_closed"
	}
	summary := strings.TrimSpace(input.Summary)
	if summary == "" {
		return errors.New("summary is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin telegram alert close tx: %w", err)
	}

	var workspaceID, sourceUpdateID, agentID string
	var taskID, repliedAt, supersededAt sql.NullString
	if err := tx.QueryRowContext(
		ctx,
		`SELECT workspace_id, source_update_id, task_id, agent_id, replied_at, superseded_at
		 FROM tg_message_map
		 WHERE telegram_chat_id = ? AND telegram_message_id = ?`,
		input.TelegramChatID,
		input.MessageID,
	).Scan(&workspaceID, &sourceUpdateID, &taskID, &agentID, &repliedAt, &supersededAt); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTelegramMessageNotFound
		}
		return fmt.Errorf("query telegram alert close context: %w", err)
	}
	if repliedAt.Valid || supersededAt.Valid {
		_ = tx.Rollback()
		return nil
	}

	if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, bridgeAgentID); err != nil {
		_ = tx.Rollback()
		return err
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO agent_updates(update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		closureUpdateID,
		workspaceID,
		bridgeAgentID,
		updateType,
		summary,
		strings.TrimSpace(input.PayloadJSON),
		0,
		now,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert telegram alert closure update: %w", err)
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE tg_message_map
		 SET superseded_by_update_id = ?, superseded_at = ?
		 WHERE telegram_chat_id = ? AND telegram_message_id = ?`,
		closureUpdateID,
		now,
		input.TelegramChatID,
		input.MessageID,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("mark telegram alert closed: %w", err)
	}
	if err := s.supersedeTelegramAlertsTx(ctx, tx, workspaceID, taskID.String, agentID, sourceUpdateID, closureUpdateID, now); err != nil {
		_ = tx.Rollback()
		return err
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("touch workspace after telegram alert close: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "telegram_alert_closed",
		EntityType: "agent_update",
		EntityID:   closureUpdateID,
		ActorID:    bridgeAgentID,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id":        workspaceID,
			"telegram_chat_id":    input.TelegramChatID,
			"telegram_message_id": input.MessageID,
			"bridge_agent_id":     bridgeAgentID,
			"update_type":         updateType,
		}),
	}); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit telegram alert close tx: %w", err)
	}
	return nil
}

func (s *Store) GetTelegramBoundUserIDs(ctx context.Context, workspaceID string) (map[string]int64, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT human_id, telegram_user_id FROM workspace_humans
		 WHERE workspace_id = ? AND telegram_user_id IS NOT NULL`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("query bound telegram users: %w", err)
	}
	defer rows.Close()

	out := make(map[string]int64)
	for rows.Next() {
		var userID string
		var tgID int64
		if err := rows.Scan(&userID, &tgID); err != nil {
			return nil, fmt.Errorf("scan bound telegram user: %w", err)
		}
		out[userID] = tgID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bound telegram users: %w", err)
	}
	return out, nil
}

func (s *Store) GetBridgeState(ctx context.Context, stateKey string) (string, error) {
	stateKey = strings.TrimSpace(stateKey)
	if stateKey == "" {
		return "", errors.New("state_key is required")
	}

	var value string
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT state_value FROM tg_bridge_state WHERE state_key = ?`,
		stateKey,
	).Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("query bridge state: %w", err)
	}
	return value, nil
}

func (s *Store) SetBridgeState(ctx context.Context, stateKey, stateValue string) error {
	stateKey = strings.TrimSpace(stateKey)
	if stateKey == "" {
		return errors.New("state_key is required")
	}

	_, err := s.writeDB.ExecContext(
		ctx,
		`INSERT INTO tg_bridge_state(state_key, state_value, updated_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(state_key) DO UPDATE SET
		   state_value = excluded.state_value,
		   updated_at = excluded.updated_at`,
		stateKey,
		strings.TrimSpace(stateValue),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("upsert bridge state: %w", err)
	}
	return nil
}

func (s *Store) supersedeTelegramAlertsTx(
	ctx context.Context,
	tx *sql.Tx,
	workspaceID string,
	taskID string,
	agentID string,
	exceptSourceUpdateID string,
	closureUpdateID string,
	closedAt string,
) error {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	exceptSourceUpdateID = strings.TrimSpace(exceptSourceUpdateID)
	closureUpdateID = strings.TrimSpace(closureUpdateID)
	if workspaceID == "" || agentID == "" || exceptSourceUpdateID == "" || closureUpdateID == "" {
		return nil
	}

	query := `UPDATE tg_message_map
		 SET superseded_by_update_id = ?, superseded_at = ?
		 WHERE workspace_id = ?
		   AND agent_id = ?
		   AND source_update_id <> ?
		   AND replied_at IS NULL
		   AND superseded_at IS NULL`
	args := []any{closureUpdateID, closedAt, workspaceID, agentID, exceptSourceUpdateID}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		query += ` AND task_id IS NULL`
	} else {
		query += ` AND task_id = ?`
		args = append(args, taskID)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("supersede telegram alerts: %w", err)
	}
	return nil
}

// DismissPendingTelegramAlert marks an alert as bypassed so it is no longer considered pending.
// It inserts a synthetic record into tg_message_map using the source update's agent for FK integrity.
func (s *Store) DismissPendingTelegramAlert(ctx context.Context, workspaceID, sourceUpdateID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	sourceUpdateID = strings.TrimSpace(sourceUpdateID)
	if workspaceID == "" || sourceUpdateID == "" {
		return errors.New("workspace_id and source_update_id required to dismiss alert")
	}

	var agentID string
	if err := s.db.QueryRowContext(ctx,
		`SELECT agent_id FROM agent_updates WHERE workspace_id = ? AND update_id = ?`,
		workspaceID, sourceUpdateID,
	).Scan(&agentID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTelegramMessageNotFound
		}
		return fmt.Errorf("query dismissed telegram alert source update: %w", err)
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return errors.New("source update agent_id required to dismiss alert")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT OR IGNORE INTO tg_message_map(
			workspace_id, source_update_id, agent_id,
			telegram_chat_id, telegram_message_id, sent_at
		) VALUES (?, ?, ?, -1, ?, ?)`,
		workspaceID, sourceUpdateID, agentID, dismissedTelegramMessageID(sourceUpdateID), now,
	)
	if err != nil {
		return fmt.Errorf("dismiss pending telegram alert: %w", err)
	}
	return nil
}

func dismissedTelegramMessageID(sourceUpdateID string) int64 {
	sum := sha256.Sum256([]byte(strings.TrimSpace(sourceUpdateID)))
	value := int64(binary.BigEndian.Uint64(sum[:8]) & 0x3fffffffffffffff)
	if value == 0 {
		value = 1
	}
	return -value
}

func nullIfBlank(v string) any {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return nil
	}
	return trimmed
}
