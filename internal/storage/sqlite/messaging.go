package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

var ErrInvalidPollCursor = errors.New("after_created_at must be a valid poll cursor")

// MessageRecord represents a stored agent message.
//
// ReadAt is the legacy/global storage-level read marker from agent_messages.
// Agent poll visibility is now scoped per workspace+agent via agent_message_acks,
// so PollMessages does not use ReadAt to decide whether a specific agent has
// already acknowledged a message.
type MessageRecord struct {
	MessageID    string `json:"message_id"`
	WorkspaceID  string `json:"workspace_id"`
	FromAgentID  string `json:"from_agent_id"`
	ToAgentID    string `json:"to_agent_id"`
	Channel      string `json:"channel"`
	ContentType  string `json:"content_type"`
	Content      string `json:"content"`
	MetadataJSON string `json:"metadata_json"`
	CreatedAt    string `json:"created_at"`
	ReadAt       string `json:"read_at,omitempty"`
}

// MessageSendInput is the input for sending a message.
type MessageSendInput struct {
	WorkspaceID           string
	FromAgentID           string
	ToAgentID             string // empty = broadcast
	Channel               string
	ContentType           string
	Content               string
	MetadataJSON          string
	PromptContextEnvelope map[string]any
}

type messagingRuntimeContext struct {
	TaskID    string
	SessionID string
}

func messagingRuntimeEventPayload(workspaceID, messageID, fromAgentID, toAgentID, channel, contentType, status string, runtimeContext messagingRuntimeContext) map[string]any {
	payload := map[string]any{
		"workspace_id":  strings.TrimSpace(workspaceID),
		"message_id":    strings.TrimSpace(messageID),
		"from":          strings.TrimSpace(fromAgentID),
		"from_agent_id": strings.TrimSpace(fromAgentID),
		"to_agent_id":   strings.TrimSpace(toAgentID),
		"channel":       strings.TrimSpace(channel),
		"content_type":  strings.TrimSpace(contentType),
		"status":        strings.TrimSpace(status),
	}
	if trimmed := strings.TrimSpace(runtimeContext.TaskID); trimmed != "" {
		payload["task_id"] = trimmed
	}
	if trimmed := strings.TrimSpace(runtimeContext.SessionID); trimmed != "" {
		payload["session_id"] = trimmed
	}
	return payload
}

func agentRequestRuntimeEventPayload(workspaceID, requestID, fromAgentID, toAgentID, method, status string, runtimeContext messagingRuntimeContext) map[string]any {
	payload := map[string]any{
		"workspace_id":  strings.TrimSpace(workspaceID),
		"request_id":    strings.TrimSpace(requestID),
		"from":          strings.TrimSpace(fromAgentID),
		"from_agent_id": strings.TrimSpace(fromAgentID),
		"to_agent_id":   strings.TrimSpace(toAgentID),
		"method":        strings.TrimSpace(method),
		"status":        strings.TrimSpace(status),
	}
	if trimmed := strings.TrimSpace(runtimeContext.TaskID); trimmed != "" {
		payload["task_id"] = trimmed
	}
	if trimmed := strings.TrimSpace(runtimeContext.SessionID); trimmed != "" {
		payload["session_id"] = trimmed
	}
	return payload
}

func agentResponseRuntimeEventPayload(workspaceID, requestID, fromAgentID, toAgentID, method, status string, runtimeContext messagingRuntimeContext) map[string]any {
	payload := map[string]any{
		"workspace_id":  strings.TrimSpace(workspaceID),
		"request_id":    strings.TrimSpace(requestID),
		"from":          strings.TrimSpace(toAgentID),
		"from_agent_id": strings.TrimSpace(fromAgentID),
		"to_agent_id":   strings.TrimSpace(toAgentID),
		"method":        strings.TrimSpace(method),
		"status":        strings.TrimSpace(status),
	}
	if trimmed := strings.TrimSpace(runtimeContext.TaskID); trimmed != "" {
		payload["task_id"] = trimmed
	}
	if trimmed := strings.TrimSpace(runtimeContext.SessionID); trimmed != "" {
		payload["session_id"] = trimmed
	}
	return payload
}

func (s *Store) inferMessagingRuntimeContextTx(ctx context.Context, tx *sql.Tx, workspaceID string, agentIDs ...string) (messagingRuntimeContext, error) {
	normalizedAgentIDs := uniqueSortedStrings(agentIDs)
	if len(normalizedAgentIDs) == 0 {
		return messagingRuntimeContext{}, nil
	}

	type agentRuntimeSets struct {
		taskIDs    map[string]struct{}
		sessionIDs map[string]struct{}
	}

	setsByAgent := make(map[string]agentRuntimeSets, len(normalizedAgentIDs))
	for _, agentID := range normalizedAgentIDs {
		rows, err := tx.QueryContext(
		 ctx,
		 `SELECT session_id, COALESCE(task_id, ''), status
		 FROM agent_sessions
		 WHERE workspace_id = ? AND agent_id = ?`,
		 workspaceID,
		 agentID,
		)
		if err != nil {
		 return messagingRuntimeContext{}, fmt.Errorf("query messaging runtime context for %s: %w", agentID, err)
		}
		runtimeSets := agentRuntimeSets{
		 taskIDs:    map[string]struct{}{},
		 sessionIDs: map[string]struct{}{},
		}
		for rows.Next() {
		 var sessionID string
		 var taskID string
		 var status string
		 if err := rows.Scan(&sessionID, &taskID, &status); err != nil {
		 rows.Close()
		 return messagingRuntimeContext{}, fmt.Errorf("scan messaging runtime context for %s: %w", agentID, err)
		 }
		 if !model.IsSessionStatusActive(status) {
		 continue
		 }
		 if trimmed := strings.TrimSpace(sessionID); trimmed != "" {
		 runtimeSets.sessionIDs[trimmed] = struct{}{}
		 }
		 if trimmed := strings.TrimSpace(taskID); trimmed != "" {
		 runtimeSets.taskIDs[trimmed] = struct{}{}
		 }
		}
		if err := rows.Err(); err != nil {
		 rows.Close()
		 return messagingRuntimeContext{}, fmt.Errorf("iterate messaging runtime context for %s: %w", agentID, err)
		}
		rows.Close()
		setsByAgent[agentID] = runtimeSets
	}

	context := messagingRuntimeContext{}
	taskIntersection := map[string]struct{}{}
	taskIntersectionInitialized := false
	allObservedTasks := map[string]struct{}{}
	for _, agentID := range normalizedAgentIDs {
		runtimeSets := setsByAgent[agentID]
		if len(runtimeSets.taskIDs) == 0 {
		 continue
		}
		for taskID := range runtimeSets.taskIDs {
		 allObservedTasks[taskID] = struct{}{}
		}
		if !taskIntersectionInitialized {
		 for taskID := range runtimeSets.taskIDs {
		 taskIntersection[taskID] = struct{}{}
		 }
		 taskIntersectionInitialized = true
		 continue
		}
		for taskID := range taskIntersection {
		 if _, ok := runtimeSets.taskIDs[taskID]; !ok {
		 delete(taskIntersection, taskID)
		 }
		}
	}
	if len(taskIntersection) == 1 {
		context.TaskID = firstMapKey(taskIntersection)
	} else if len(allObservedTasks) == 1 {
		context.TaskID = firstMapKey(allObservedTasks)
	}

	primary := setsByAgent[normalizedAgentIDs[0]]
	if len(primary.sessionIDs) == 1 && len(normalizedAgentIDs) == 1 {
		context.SessionID = firstMapKey(primary.sessionIDs)
	}

	return context, nil
}

func firstMapKey(values map[string]struct{}) string {
	for value := range values {
		return value
	}
	return ""
}

func (s *Store) agentExistsForRuntimeEventTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID string) (bool, error) {
	var count int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(1) FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID,
		agentID,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("check agent existence for runtime event: %w", err)
	}
	return count > 0, nil
}

func (s *Store) workspaceExistsForRuntimeEventTx(ctx context.Context, tx *sql.Tx, workspaceID string) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&count); err != nil {
		return false, fmt.Errorf("check workspace existence for runtime event: %w", err)
	}
	return count > 0, nil
}

func (s *Store) appendMessagingRuntimeEventTx(ctx context.Context, tx *sql.Tx, input RuntimeEventInput) (RuntimeEventRecord, error) {
	return s.appendMessagingRuntimeEventWithOptionalAuthorityTx(ctx, tx, WorkspaceAuthorityRecord{}, input)
}

func (s *Store) appendMessagingRuntimeEventWithOptionalAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input RuntimeEventInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, nil
	}
	workspaceExists, err := s.workspaceExistsForRuntimeEventTx(ctx, tx, workspaceID)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	if !workspaceExists {
		return RuntimeEventRecord{}, nil
	}
	input.WorkspaceID = workspaceID
	if agentID := strings.TrimSpace(input.AgentID); agentID != "" {
		agentExists, err := s.agentExistsForRuntimeEventTx(ctx, tx, workspaceID, agentID)
		if err != nil {
		 return RuntimeEventRecord{}, err
		}
		if agentExists {
		 input.AgentID = agentID
		} else {
		 input.AgentID = ""
		}
	}

	record, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, input)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("append messaging runtime event: %w", err)
	}
	return record, nil
}

func normalizeMessageIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ids))
	normalized := make([]string, 0, len(ids))
	for _, id := range ids {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
		 continue
		}
		if _, ok := seen[trimmed]; ok {
		 continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}
	return normalized
}

func parseMessageCursor(cursor string) (createdAt string, messageID string) {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return "", ""
	}
	parts := strings.SplitN(cursor, "|", 2)
	createdAt = strings.TrimSpace(parts[0])
	if len(parts) == 2 {
		messageID = strings.TrimSpace(parts[1])
	}
	return createdAt, messageID
}

func ValidateMessageCursor(cursor string) error {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return nil
	}
	createdAt, messageID := parseMessageCursor(cursor)
	if _, err := time.Parse(time.RFC3339Nano, createdAt); err != nil {
		return ErrInvalidPollCursor
	}
	if strings.Contains(cursor, "|") && messageID == "" {
		return ErrInvalidPollCursor
	}
	return nil
}

func EncodeMessageCursor(createdAt, messageID string) string {
	createdAt = strings.TrimSpace(createdAt)
	messageID = strings.TrimSpace(messageID)
	if createdAt == "" {
		return ""
	}
	if messageID == "" {
		return createdAt
	}
	return createdAt + "|" + messageID
}

func messageTableAlias(alias string) string {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return "agent_messages"
	}
	return alias
}

func inboxVisibilityPredicate(alias string) string {
	alias = messageTableAlias(alias)
	return fmt.Sprintf("(%s.to_agent_id = ? OR %s.to_agent_id = '' OR %s.to_agent_id IS NULL OR %s.from_agent_id = ?)", alias, alias, alias, alias)
}

func legacyUnreadPredicate(alias string) string {
	alias = messageTableAlias(alias)
	return fmt.Sprintf("%s.read_at IS NULL", alias)
}

func inboxVisibilityArgs(agentID string) []any {
	agentID = strings.TrimSpace(agentID)
	return []any{agentID, agentID}
}

type messageRowScanner interface {
	Scan(dest ...any) error
}

func scanPollMessageRecord(rows messageRowScanner, m *MessageRecord) error {
	return rows.Scan(
		&m.MessageID, &m.WorkspaceID, &m.FromAgentID, &m.ToAgentID,
		&m.Channel, &m.ContentType, &m.Content, &m.MetadataJSON,
		&m.CreatedAt,
	)
}

func scanWorkspaceMessageRecord(rows messageRowScanner, m *MessageRecord) error {
	return rows.Scan(
		&m.MessageID, &m.WorkspaceID, &m.FromAgentID, &m.ToAgentID,
		&m.Channel, &m.ContentType, &m.Content, &m.MetadataJSON,
		&m.CreatedAt, &m.ReadAt,
	)
}

// GetMessageMetadataJSON returns just the metadata_json for a given message.
func (s *Store) GetMessageMetadataJSON(ctx context.Context, workspaceID, messageID string) (string, error) {
	var metadataJSON sql.NullString
	if err := s.db.QueryRowContext(ctx,
		`SELECT metadata_json FROM agent_messages WHERE workspace_id = ? AND message_id = ?`,
		workspaceID, messageID,
	).Scan(&metadataJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
		 return "", nil
		}
		return "", err
	}
	return metadataJSON.String, nil
}

// MarkMessageRead marks a message as read in the system.
func (s *Store) MarkMessageRead(ctx context.Context, workspaceID, messageID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	messageID = strings.TrimSpace(messageID)
	if workspaceID == "" || messageID == "" {
		return errors.New("workspace_id and message_id are required")
	}
	_, err := s.writeDB.ExecContext(ctx,
		`UPDATE agent_messages SET read_at = CURRENT_TIMESTAMP WHERE workspace_id = ? AND message_id = ? AND read_at IS NULL`,
		workspaceID, messageID,
	)
	return err
}

// SendMessage sends an agent message.
func (s *Store) SendMessage(ctx context.Context, input MessageSendInput) (string, error) {
	messageID, _, err := s.SendMessageWithEvent(ctx, input)
	return messageID, err
}

// SendMessageWithEvent sends an agent message and returns the appended runtime event row.
func (s *Store) SendMessageWithEvent(ctx context.Context, input MessageSendInput) (string, RuntimeEventRecord, error) {
	return s.SendMessageWithAuthorityEvent(ctx, input)
}

// SendMessageWithAuthorityEvent sends an agent message from a public authority-backed surface.
func (s *Store) SendMessageWithAuthorityEvent(ctx context.Context, input MessageSendInput) (string, RuntimeEventRecord, error) {
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, strings.TrimSpace(input.WorkspaceID), authorityScopeWorkspace, referenceAt)
	if err != nil {
		return "", RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return "", RuntimeEventRecord{}, fmt.Errorf("begin send message authority tx: %w", err)
	}
	defer func() {
		if tx != nil {
		 _ = tx.Rollback()
		}
	}()
	var messageID string
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		messageID, event, innerErr = s.sendMessageWithOptionalAuthorityTx(ctx, tx, authority, input)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return "", RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return "", RuntimeEventRecord{}, fmt.Errorf("commit send message authority tx: %w", err)
	}
	tx = nil
	return messageID, event, nil
}

func (s *Store) sendMessageWithOptionalAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input MessageSendInput) (string, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return "", RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	fromAgentID := strings.TrimSpace(input.FromAgentID)
	if fromAgentID == "" {
		return "", RuntimeEventRecord{}, errors.New("from_agent_id is required")
	}
	content := strings.TrimSpace(input.Content)
	if content == "" {
		return "", RuntimeEventRecord{}, errors.New("content is required")
	}

	channel := strings.TrimSpace(input.Channel)
	if channel == "" {
		channel = "default"
	}
	contentType := strings.TrimSpace(input.ContentType)
	if contentType == "" {
		contentType = "text/plain"
	}
	metadataJSON := strings.TrimSpace(input.MetadataJSON)
	if metadataJSON == "" {
		metadataJSON = "{}"
	}

	messageID := nextID("msg")
	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err := tx.ExecContext(ctx,
		`INSERT INTO agent_messages (message_id, workspace_id, from_agent_id, to_agent_id, channel, content_type, content, metadata_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		messageID, workspaceID, fromAgentID,
		strings.TrimSpace(input.ToAgentID),
		channel, contentType, content, metadataJSON, now,
	)
	if err != nil {
		return "", RuntimeEventRecord{}, fmt.Errorf("send message: %w", err)
	}
	runtimeContext, err := s.inferMessagingRuntimeContextTx(ctx, tx, workspaceID, fromAgentID, strings.TrimSpace(input.ToAgentID))
	if err != nil {
		return "", RuntimeEventRecord{}, err
	}
	payload := messagingRuntimeEventPayload(workspaceID, messageID, fromAgentID, strings.TrimSpace(input.ToAgentID), channel, contentType, "SENT", runtimeContext)
	if input.PromptContextEnvelope != nil {
		payload, err = AttachAgentMessagePromptContextEnvelope(payload, input.PromptContextEnvelope)
		if err != nil {
		 return "", RuntimeEventRecord{}, err
		}
		if err := validateAgentMessagePromptContextEnvelopeBinding(payload, input.PromptContextEnvelope); err != nil {
		 return "", RuntimeEventRecord{}, err
		}
	}
	event, err := s.appendMessagingRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    messageID,
		ActorType:   "agent",
		ActorID:     fromAgentID,
		AgentID:     fromAgentID,
		SessionID:   runtimeContext.SessionID,
		TaskID:      runtimeContext.TaskID,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   now,
	})
	if err != nil {
		return "", RuntimeEventRecord{}, err
	}
	return messageID, event, nil
}

func validateAgentMessagePromptContextEnvelopeBinding(payload map[string]any, envelope map[string]any) error {
	if envelope == nil {
		return nil
	}
	expected := map[string]string{
		"workspace_id":   promptContextPayloadString(payload, "workspace_id"),
		"principal_type": "agent",
		"principal_id":   promptContextPayloadString(payload, "from_agent_id"),
		"actor_agent_id": promptContextPayloadString(payload, "from_agent_id"),
		"from_agent_id":  promptContextPayloadString(payload, "from_agent_id"),
		"to_agent_id":    promptContextPayloadString(payload, "to_agent_id"),
		"channel":        promptContextPayloadString(payload, "channel"),
	}
	for key, want := range expected {
		got, ok := envelope[key].(string)
		if !ok {
		 return fmt.Errorf("agent_message prompt context envelope missing string %s", key)
		}
		if strings.TrimSpace(got) != got {
		 return fmt.Errorf("agent_message prompt context envelope has padded %s=%q", key, got)
		}
		if got != want {
		 return fmt.Errorf("agent_message prompt context envelope %s=%q does not match message payload %q", key, got, want)
		}
	}
	return nil
}

func validateAgentRequestPromptContextEnvelopeBinding(payload map[string]any, envelope map[string]any, expectedSurface string) error {
	if envelope == nil {
		return nil
	}
	surface := promptContextPayloadString(envelope, "surface")
	if surface != expectedSurface {
		return fmt.Errorf("agent_request prompt context envelope surface=%q does not match operation surface %q", surface, expectedSurface)
	}
	actorAgentID := promptContextPayloadString(payload, "from_agent_id")
	if expectedSurface == "agent.respond" || expectedSurface == "agent.request.list" {
		actorAgentID = promptContextPayloadString(payload, "to_agent_id")
	}
	expected := map[string]string{
		"workspace_id":   promptContextPayloadString(payload, "workspace_id"),
		"principal_type": "agent",
		"principal_id":   actorAgentID,
		"actor_agent_id": actorAgentID,
		"request_id":     promptContextPayloadString(payload, "request_id"),
		"from_agent_id":  promptContextPayloadString(payload, "from_agent_id"),
		"to_agent_id":    promptContextPayloadString(payload, "to_agent_id"),
		"method":         promptContextPayloadString(payload, "method"),
		"status":         promptContextPayloadString(payload, "status"),
	}
	if previousStatus := promptContextPayloadString(payload, "previous_status"); previousStatus != "" {
		expected["previous_status"] = previousStatus
	}
	for key, want := range expected {
		got, ok := envelope[key].(string)
		if !ok {
		 return fmt.Errorf("agent_request prompt context envelope missing string %s", key)
		}
		if strings.TrimSpace(got) != got {
		 return fmt.Errorf("agent_request prompt context envelope has padded %s=%q", key, got)
		}
		if got != want {
		 return fmt.Errorf("agent_request prompt context envelope %s=%q does not match request payload %q", key, got, want)
		}
	}
	return nil
}

func clonePromptContextEnvelope(envelope map[string]any) map[string]any {
	out := make(map[string]any, len(envelope))
	for key, value := range envelope {
		out[key] = value
	}
	return out
}

func promptContextPayloadString(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(text)
}

// PollMessages returns messages that are still unacknowledged for the given
// workspace+agent view (cursor-based).
// When afterCreatedAt is empty, defaults to lookbackHours hours ago to avoid
// returning ancient messages.
// Excludes messages already acknowledged by that same agent via
// agent_message_acks.
// It also keeps the legacy global read_at filter in place so pre-migration rows
// that were globally hidden do not resurface into per-agent inboxes.
// Includes messages TO the agent, broadcasts, AND self-sent messages.
func (s *Store) pollMessagesQuery(ctx context.Context, workspaceID, agentID, cursorPredicate string, cursorArgs []any, limit int) ([]MessageRecord, error) {
	queryArgs := []any{agentID, workspaceID}
	queryArgs = append(queryArgs, inboxVisibilityArgs(agentID)...)
	queryArgs = append(queryArgs, cursorArgs...)

	query := fmt.Sprintf(`SELECT m.message_id, m.workspace_id, m.from_agent_id, m.to_agent_id, m.channel, m.content_type, m.content, m.metadata_json, m.created_at
	 FROM agent_messages m
	 LEFT JOIN agent_message_acks a
	   ON a.workspace_id = m.workspace_id
	  AND a.agent_id = ?
	  AND a.message_id = m.message_id
	 WHERE m.workspace_id = ?
	   AND %s
	   AND %s
	   AND %s
	   AND a.message_id IS NULL
	 ORDER BY m.created_at ASC, m.rowid ASC`, inboxVisibilityPredicate("m"), cursorPredicate, legacyUnreadPredicate("m"))
	if limit > 0 {
		query += "\n LIMIT ?"
		queryArgs = append(queryArgs, limit)
	}

	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("poll messages: %w", err)
	}
	defer rows.Close()

	var messages []MessageRecord
	for rows.Next() {
		var m MessageRecord
		if err := scanPollMessageRecord(rows, &m); err != nil {
		 return nil, fmt.Errorf("scan message: %w", err)
		}
		messages = append(messages, m)
	}
	return messages, rows.Err()
}

func (s *Store) PollMessages(ctx context.Context, workspaceID, agentID, afterCreatedAt string, limit, lookbackHours int) ([]MessageRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, errors.New("agent_id is required")
	}
	if err := s.ensureAgentMessageAckTable(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 20
	}
	rawAfterCreatedAt := strings.TrimSpace(afterCreatedAt)
	if err := ValidateMessageCursor(rawAfterCreatedAt); err != nil {
		return nil, err
	}
	afterCreatedAt, afterMessageID := parseMessageCursor(afterCreatedAt)
	legacyTimestampCursor := rawAfterCreatedAt != "" && afterCreatedAt != "" && afterMessageID == ""
	if afterMessageID != "" {
		var exists int
		if err := s.db.QueryRowContext(ctx,
		 fmt.Sprintf(`SELECT 1
		 FROM agent_messages
		 WHERE workspace_id = ?
		 AND message_id = ?
		 AND created_at = ?
		 AND %s`, inboxVisibilityPredicate("agent_messages")),
		 workspaceID, afterMessageID, afterCreatedAt, agentID, agentID,
		).Scan(&exists); err != nil {
		 if errors.Is(err, sql.ErrNoRows) {
		 return nil, ErrInvalidPollCursor
		 }
		 return nil, fmt.Errorf("validate poll cursor: %w", err)
		}
	}

	// Default: if no cursor, start from lookbackHours ago instead of the dawn of time
	if afterCreatedAt == "" {
		if lookbackHours <= 0 {
		 lookbackHours = 24
		}
		afterCreatedAt = time.Now().UTC().Add(-time.Duration(lookbackHours) * time.Hour).Format(time.RFC3339Nano)
		legacyTimestampCursor = false
	}

	if legacyTimestampCursor {
		sameTimestampMessages, err := s.pollMessagesQuery(ctx, workspaceID, agentID, "m.created_at = ?", []any{afterCreatedAt}, 0)
		if err != nil {
		 return nil, err
		}
		if len(sameTimestampMessages) >= limit {
		 return sameTimestampMessages, nil
		}

		newerMessages, err := s.pollMessagesQuery(ctx, workspaceID, agentID, "m.created_at > ?", []any{afterCreatedAt}, limit-len(sameTimestampMessages))
		if err != nil {
		 return nil, err
		}
		return append(sameTimestampMessages, newerMessages...), nil
	}

	cursorPredicate := "(m.created_at > ? OR m.created_at = ?)"
	cursorArgs := []any{afterCreatedAt, afterCreatedAt}
	if afterMessageID != "" {
		cursorPredicate = `(m.created_at > ? OR (
		 m.created_at = ? AND
		 m.rowid > COALESCE((SELECT rowid FROM agent_messages WHERE message_id = ?), -1)
		))`
		cursorArgs = []any{afterCreatedAt, afterCreatedAt, afterMessageID}
	}
	return s.pollMessagesQuery(ctx, workspaceID, agentID, cursorPredicate, cursorArgs, limit)
}

// ListWorkspaceMessages returns the workspace-level message log, optionally
// filtered by channel. It does not apply agent_message_acks because dashboard
// and snapshot views are workspace-scoped, not per-agent inbox views.
func (s *Store) ListWorkspaceMessages(ctx context.Context, workspaceID, channel string, limit int) ([]MessageRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if limit <= 0 {
		limit = 50
	}

	var rows interface {
		Next() bool
		Scan(dest ...any) error
		Close() error
		Err() error
	}
	var err error

	if channel != "" {
		rows, err = s.db.QueryContext(ctx,
		 `SELECT message_id, workspace_id, from_agent_id, to_agent_id, channel, content_type, content, metadata_json, created_at, COALESCE(read_at, '')
		 FROM agent_messages
		 WHERE workspace_id = ? AND channel = ?
		 ORDER BY created_at DESC, rowid DESC
		 LIMIT ?`,
		 workspaceID, channel, limit,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
		 `SELECT message_id, workspace_id, from_agent_id, to_agent_id, channel, content_type, content, metadata_json, created_at, COALESCE(read_at, '')
		 FROM agent_messages
		 WHERE workspace_id = ?
		 ORDER BY created_at DESC, rowid DESC
		 LIMIT ?`,
		 workspaceID, limit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("list workspace messages: %w", err)
	}
	defer rows.Close()

	var messages []MessageRecord
	for rows.Next() {
		var m MessageRecord
		if err := scanWorkspaceMessageRecord(rows, &m); err != nil {
		 return nil, fmt.Errorf("scan message: %w", err)
		}
		messages = append(messages, m)
	}
	return messages, rows.Err()
}

// AckMessages marks messages as read for a specific workspace/agent view.
// It returns the number of acknowledgements actually recorded.
func (s *Store) AckMessages(ctx context.Context, workspaceID, agentID string, messageIDs []string) (int, error) {
	messageIDs = normalizeMessageIDs(messageIDs)
	if len(messageIDs) == 0 {
		return 0, nil
	}

	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return 0, errors.New("workspace_id is required")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return 0, errors.New("agent_id is required")
	}
	if err := s.ensureAgentMessageAckTable(ctx); err != nil {
		return 0, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	acknowledged := 0
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin ack tx: %w", err)
	}
	defer func() {
		if tx != nil {
		 _ = tx.Rollback()
		}
	}()

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO agent_message_acks (workspace_id, agent_id, message_id, read_at)
		SELECT workspace_id, ?, message_id, ?
		FROM agent_messages
		WHERE workspace_id = ?
		 AND message_id = ?
		 AND %s
		 AND %s
		ON CONFLICT(workspace_id, agent_id, message_id) DO NOTHING`, legacyUnreadPredicate("agent_messages"), inboxVisibilityPredicate("agent_messages")))
	if err != nil {
		return 0, fmt.Errorf("prepare ack statement: %w", err)
	}
	defer stmt.Close()

	for _, messageID := range messageIDs {
		stmtArgs := []any{agentID, now, workspaceID, messageID}
		stmtArgs = append(stmtArgs, inboxVisibilityArgs(agentID)...)
		result, err := stmt.ExecContext(ctx, stmtArgs...)
		if err != nil {
		 return 0, fmt.Errorf("ack message %s: %w", messageID, err)
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
		 return 0, fmt.Errorf("ack message %s rows affected: %w", messageID, err)
		}
		acknowledged += int(rowsAffected)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit ack tx: %w", err)
	}
	tx = nil
	return acknowledged, nil
}

// ── Agent State (per-agent KV store) ────────────────────────────────

// SetAgentState sets a key-value pair in the agent's private state.
func (s *Store) ensureAgentMessageAckTable(ctx context.Context) error {
	const query = `
CREATE TABLE IF NOT EXISTS agent_message_acks (
  workspace_id TEXT NOT NULL,
  agent_id     TEXT NOT NULL,
  message_id   TEXT NOT NULL,
  read_at      TEXT NOT NULL,
  PRIMARY KEY (workspace_id, agent_id, message_id)
);`
	if _, err := s.writeDB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("ensure agent_message_acks table: %w", err)
	}
	return nil
}

func (s *Store) SetAgentState(ctx context.Context, workspaceID, agentID, key, value string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	key = strings.TrimSpace(key)
	if workspaceID == "" || agentID == "" || key == "" {
		return errors.New("workspace_id, agent_id, and state_key are required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO agent_state (workspace_id, agent_id, state_key, state_value, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(workspace_id, agent_id, state_key) DO UPDATE SET
		 state_value = excluded.state_value,
		 updated_at = excluded.updated_at`,
		workspaceID, agentID, key, value, now,
	)
	if err != nil {
		return fmt.Errorf("set agent state: %w", err)
	}
	return nil
}

const runtimeScratchAgentStateKey = "rnar.runtime.v1"

type AgentStateSetWithReceiptResult struct {
	WorkspaceID          string
	AgentID              string
	Key                  string
	RuntimeEvent         RuntimeEventRecord
	RuntimeEventRecorded bool
}

type runtimeScratchPendingTriggerReceiptState struct {
	PendingTrigger        string `json:"pending_trigger"`
	PendingTriggerTask    string `json:"pending_trigger_task"`
	PendingTriggerSession string `json:"pending_trigger_session"`
	PendingTriggerAt      string `json:"pending_trigger_at"`
	ActiveTaskID          string `json:"active_task_id"`
	ActiveSessionID       string `json:"active_session_id"`
	LastWakeTrigger       string `json:"last_wake_trigger"`
}

func (s *Store) SetAgentRuntimeScratchStateWithPendingTriggerReceipt(ctx context.Context, workspaceID, agentID, value string) (AgentStateSetWithReceiptResult, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || agentID == "" {
		return AgentStateSetWithReceiptResult{}, errors.New("workspace_id and agent_id are required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return AgentStateSetWithReceiptResult{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return AgentStateSetWithReceiptResult{}, fmt.Errorf("begin agent scratch state tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result := AgentStateSetWithReceiptResult{WorkspaceID: workspaceID, AgentID: agentID, Key: runtimeScratchAgentStateKey}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		previousValue := ""
		err := tx.QueryRowContext(ctx,
		 `SELECT state_value FROM agent_state WHERE workspace_id = ? AND agent_id = ? AND state_key = ?`,
		 workspaceID, agentID, runtimeScratchAgentStateKey,
		).Scan(&previousValue)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
		 return fmt.Errorf("get previous agent scratch state: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
		 `INSERT INTO agent_state (workspace_id, agent_id, state_key, state_value, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(workspace_id, agent_id, state_key) DO UPDATE SET
		 state_value = excluded.state_value,
		 updated_at = excluded.updated_at`,
		 workspaceID, agentID, runtimeScratchAgentStateKey, value, now,
		); err != nil {
		 return fmt.Errorf("set agent scratch state: %w", err)
		}
		input, ok := runtimePendingTriggerReceiptInput(workspaceID, agentID, previousValue, value, now)
		if !ok {
		 return nil
		}
		event, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, input)
		if err != nil {
		 return err
		}
		result.RuntimeEvent = event
		result.RuntimeEventRecorded = true
		return nil
	}); err != nil {
		return AgentStateSetWithReceiptResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return AgentStateSetWithReceiptResult{}, fmt.Errorf("commit agent scratch state tx: %w", err)
	}
	return result, nil
}

func runtimePendingTriggerReceiptInput(workspaceID, agentID, previousRaw, nextRaw, createdAt string) (RuntimeEventInput, bool) {
	previous, previousOK := decodeRuntimeScratchPendingTriggerReceiptState(previousRaw)
	next, nextOK := decodeRuntimeScratchPendingTriggerReceiptState(nextRaw)
	if !previousOK && !nextOK {
		return RuntimeEventInput{}, false
	}
	eventType := ""
	trigger := normalizeRuntimePendingTriggerForReceipt(next.PendingTrigger)
	taskID := strings.TrimSpace(next.PendingTriggerTask)
	sessionID := strings.TrimSpace(next.PendingTriggerSession)
	if trigger != "" && !sameRuntimePendingTriggerReceipt(previous, next) {
		eventType = "runtime.pending_trigger.queued"
	} else if normalizeRuntimePendingTriggerForReceipt(previous.PendingTrigger) != "" &&
		normalizeRuntimePendingTriggerForReceipt(next.PendingTrigger) == "" &&
		runtimePendingTriggerConsumedByNextScratch(previous, next) {
		eventType = "runtime.pending_trigger.consumed"
		trigger = normalizeRuntimePendingTriggerForReceipt(previous.PendingTrigger)
		taskID = strings.TrimSpace(previous.PendingTriggerTask)
		sessionID = strings.TrimSpace(previous.PendingTriggerSession)
	}
	if eventType == "" {
		return RuntimeEventInput{}, false
	}
	payload := map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"trigger":      trigger,
		"state":        strings.TrimPrefix(eventType, "runtime.pending_trigger."),
	}
	if taskID != "" {
		payload["task_id"] = taskID
	}
	if sessionID != "" {
		payload["session_id"] = sessionID
	}
	if at := strings.TrimSpace(firstNonEmpty(next.PendingTriggerAt, previous.PendingTriggerAt)); at != "" {
		payload["pending_trigger_at"] = at
	}
	return RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "runtime_pending_trigger",
		EntityID:    runtimePendingTriggerReceiptEntityID(agentID, trigger, taskID, sessionID),
		ActorType:   "agent",
		ActorID:     agentID,
		AgentID:     agentID,
		PayloadJSON: string(mustJSON(payload)),
		CreatedAt:   createdAt,
	}, true
}

func decodeRuntimeScratchPendingTriggerReceiptState(raw string) (runtimeScratchPendingTriggerReceiptState, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return runtimeScratchPendingTriggerReceiptState{}, false
	}
	var state runtimeScratchPendingTriggerReceiptState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return runtimeScratchPendingTriggerReceiptState{}, false
	}
	return state, true
}

func normalizeRuntimePendingTriggerForReceipt(trigger string) string {
	switch strings.ToLower(strings.TrimSpace(trigger)) {
	case "inbound_message", "runtime_resume", "request_resume", "runtime_switch_task", "runtime_switch_tension", "control_switch_task", "control_switch_tension", "recovery", "system_news", "task_project_fields_updated":
		return strings.ToLower(strings.TrimSpace(trigger))
	default:
		return ""
	}
}

func sameRuntimePendingTriggerReceipt(left, right runtimeScratchPendingTriggerReceiptState) bool {
	return normalizeRuntimePendingTriggerForReceipt(left.PendingTrigger) == normalizeRuntimePendingTriggerForReceipt(right.PendingTrigger) &&
		strings.TrimSpace(left.PendingTriggerTask) == strings.TrimSpace(right.PendingTriggerTask) &&
		strings.TrimSpace(left.PendingTriggerSession) == strings.TrimSpace(right.PendingTriggerSession)
}

func runtimePendingTriggerConsumedByNextScratch(previous, next runtimeScratchPendingTriggerReceiptState) bool {
	trigger := normalizeRuntimePendingTriggerForReceipt(previous.PendingTrigger)
	if trigger == "" {
		return false
	}
	taskID := strings.TrimSpace(previous.PendingTriggerTask)
	sessionID := strings.TrimSpace(previous.PendingTriggerSession)
	if taskID != "" && strings.TrimSpace(next.ActiveTaskID) == taskID {
		return true
	}
	if sessionID != "" && strings.TrimSpace(next.ActiveSessionID) == sessionID {
		return true
	}
	return strings.TrimSpace(next.LastWakeTrigger) == trigger
}

func runtimePendingTriggerReceiptEntityID(agentID, trigger, taskID, sessionID string) string {
	parts := []string{"agent", strings.TrimSpace(agentID), strings.TrimSpace(trigger)}
	if taskID := strings.TrimSpace(taskID); taskID != "" {
		parts = append(parts, "task", taskID)
	}
	if sessionID := strings.TrimSpace(sessionID); sessionID != "" {
		parts = append(parts, "session", sessionID)
	}
	return strings.Join(parts, ":")
}

// GetAgentState gets a value from the agent's private state.
func (s *Store) GetAgentState(ctx context.Context, workspaceID, agentID, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx,
		`SELECT state_value FROM agent_state WHERE workspace_id = ? AND agent_id = ? AND state_key = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(agentID), strings.TrimSpace(key),
	).Scan(&value)
	if err != nil {
		return "", fmt.Errorf("get agent state: %w", err)
	}
	return value, nil
}

// ListAgentStateKeys lists all keys in the agent's state.
func (s *Store) ListAgentStateKeys(ctx context.Context, workspaceID, agentID string) ([]map[string]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT state_key, state_value, updated_at FROM agent_state WHERE workspace_id = ? AND agent_id = ? ORDER BY state_key`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(agentID),
	)
	if err != nil {
		return nil, fmt.Errorf("list agent state: %w", err)
	}
	defer rows.Close()

	var entries []map[string]string
	for rows.Next() {
		var key, value, updatedAt string
		if err := rows.Scan(&key, &value, &updatedAt); err != nil {
		 return nil, fmt.Errorf("scan agent state: %w", err)
		}
		entries = append(entries, map[string]string{
		 "key":        key,
		 "value":      value,
		 "updated_at": updatedAt,
		})
	}
	return entries, rows.Err()
}

// DeleteAgentState deletes a key from the agent's state.
func (s *Store) DeleteAgentState(ctx context.Context, workspaceID, agentID, key string) error {
	_, err := s.writeDB.ExecContext(ctx,
		`DELETE FROM agent_state WHERE workspace_id = ? AND agent_id = ? AND state_key = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(agentID), strings.TrimSpace(key),
	)
	if err != nil {
		return fmt.Errorf("delete agent state: %w", err)
	}
	return nil
}

// ── Agent-to-Agent RPC ──────────────────────────────────────────────

// AgentRequestRecord represents an agent-to-agent request.
type AgentRequestRecord struct {
	RequestID   string `json:"request_id"`
	WorkspaceID string `json:"workspace_id"`
	FromAgentID string `json:"from_agent_id"`
	ToAgentID   string `json:"to_agent_id"`
	Method      string `json:"method"`
	Payload     string `json:"payload"`
	Status      string `json:"status"`
	Response    string `json:"response,omitempty"`
	CreatedAt   string `json:"created_at"`
	RespondedAt string `json:"responded_at,omitempty"`
	TimeoutSec  int    `json:"timeout_sec"`
}

// AgentRequestInput is input for creating an agent request.
type AgentRequestInput struct {
	WorkspaceID           string
	FromAgentID           string
	ToAgentID             string
	Method                string
	Payload               string
	TimeoutSec            int
	PromptContextEnvelope map[string]any
}

type AgentRequestReuseInput struct {
	WorkspaceID       string
	RequestID         string
	ReusedByAgentID   string
	DelegateTaskID    string
	ReuseReason       string
	OriginalMethod    string
	OriginalStatus    string
	OriginalToAgentID string
}

// FindOpenDelegateTaskAgentRequest returns an existing open delegate request for
// the same target agent and task. It intentionally dedupes across senders:
// multiple peers noticing the same blocked task should not fan out identical
// nudges to the recipient runtime.
func (s *Store) FindOpenDelegateTaskAgentRequest(ctx context.Context, workspaceID, toAgentID, method, taskID string) (AgentRequestRecord, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	toAgentID = strings.TrimSpace(toAgentID)
	taskID = strings.TrimSpace(taskID)
	method = strings.TrimSpace(method)
	if method == "" {
		method = "default"
	}
	if workspaceID == "" || toAgentID == "" || taskID == "" {
		return AgentRequestRecord{}, false, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT request_id, workspace_id, from_agent_id, to_agent_id, method, payload, status, COALESCE(response, ''), created_at, COALESCE(responded_at, ''), timeout_sec
		 FROM agent_requests
		 WHERE workspace_id = ?
		 AND to_agent_id = ?
		 AND method = ?
		 AND status IN ('PENDING', 'PROCESSING')
		 ORDER BY created_at DESC
		 LIMIT 50`,
		workspaceID, toAgentID, method,
	)
	if err != nil {
		return AgentRequestRecord{}, false, fmt.Errorf("find open delegate task request: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var record AgentRequestRecord
		if err := rows.Scan(&record.RequestID, &record.WorkspaceID, &record.FromAgentID, &record.ToAgentID, &record.Method, &record.Payload, &record.Status, &record.Response, &record.CreatedAt, &record.RespondedAt, &record.TimeoutSec); err != nil {
		 return AgentRequestRecord{}, false, fmt.Errorf("scan open delegate task request: %w", err)
		}
		if agentRequestPayloadDelegateTaskID(record.Payload) == taskID {
		 return record, true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return AgentRequestRecord{}, false, err
	}
	return AgentRequestRecord{}, false, nil
}

func agentRequestPayloadDelegateTaskID(raw string) string {
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &payload); err != nil {
		return ""
	}
	taskID, _ := payload["task_id"].(string)
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return ""
	}
	kind, _ := payload["request_kind"].(string)
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "delegate_task", "delegate", "handoff", "claim_task", "implement", "work":
		return taskID
	default:
		return ""
	}
}

// CreateAgentRequest creates a new agent-to-agent request.
func (s *Store) CreateAgentRequest(ctx context.Context, input AgentRequestInput) (string, error) {
	requestID, _, err := s.CreateAgentRequestWithEvent(ctx, input)
	return requestID, err
}

// CreateAgentRequestWithEvent creates a new agent-to-agent request and returns the appended runtime event row.
func (s *Store) CreateAgentRequestWithEvent(ctx context.Context, input AgentRequestInput) (string, RuntimeEventRecord, error) {
	return s.CreateAgentRequestWithAuthorityEvent(ctx, input)
}

// CreateAgentRequestWithAuthorityEvent creates a new agent-to-agent request from a public authority-backed surface.
func (s *Store) CreateAgentRequestWithAuthorityEvent(ctx context.Context, input AgentRequestInput) (string, RuntimeEventRecord, error) {
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, strings.TrimSpace(input.WorkspaceID), authorityScopeWorkspace, referenceAt)
	if err != nil {
		return "", RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return "", RuntimeEventRecord{}, fmt.Errorf("begin create agent request authority tx: %w", err)
	}
	defer func() {
		if tx != nil {
		 _ = tx.Rollback()
		}
	}()
	var requestID string
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		requestID, event, innerErr = s.createAgentRequestWithOptionalAuthorityTx(ctx, tx, authority, input)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return "", RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return "", RuntimeEventRecord{}, fmt.Errorf("commit create agent request authority tx: %w", err)
	}
	tx = nil
	return requestID, event, nil
}

func (s *Store) createAgentRequestWithOptionalAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input AgentRequestInput) (string, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return "", RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	fromAgent := strings.TrimSpace(input.FromAgentID)
	if fromAgent == "" {
		return "", RuntimeEventRecord{}, errors.New("from_agent_id is required")
	}
	toAgent := strings.TrimSpace(input.ToAgentID)
	if toAgent == "" {
		return "", RuntimeEventRecord{}, errors.New("to_agent_id is required")
	}
	payload := strings.TrimSpace(input.Payload)
	if payload == "" {
		return "", RuntimeEventRecord{}, errors.New("payload is required")
	}

	method := strings.TrimSpace(input.Method)
	if method == "" {
		method = "default"
	}
	timeoutSec := input.TimeoutSec
	if timeoutSec <= 0 {
		timeoutSec = 300
	}

	requestID := nextID("areq")
	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err := tx.ExecContext(ctx,
		`INSERT INTO agent_requests (request_id, workspace_id, from_agent_id, to_agent_id, method, payload, status, created_at, timeout_sec)
		 VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)`,
		requestID, workspaceID, fromAgent, toAgent, method, payload, now, timeoutSec,
	)
	if err != nil {
		return "", RuntimeEventRecord{}, fmt.Errorf("create agent request: %w", err)
	}
	runtimeContext, err := s.inferMessagingRuntimeContextTx(ctx, tx, workspaceID, fromAgent, toAgent)
	if err != nil {
		return "", RuntimeEventRecord{}, err
	}
	payloadJSON := agentRequestRuntimeEventPayload(workspaceID, requestID, fromAgent, toAgent, method, "PENDING", runtimeContext)
	if input.PromptContextEnvelope != nil {
		envelope := clonePromptContextEnvelope(input.PromptContextEnvelope)
		envelope["request_id"] = requestID
		payloadJSON, err = AttachAgentRequestPromptContextEnvelope(payloadJSON, envelope)
		if err != nil {
		 return "", RuntimeEventRecord{}, err
		}
		if err := validateAgentRequestPromptContextEnvelopeBinding(payloadJSON, envelope, "agent.request"); err != nil {
		 return "", RuntimeEventRecord{}, err
		}
	}
	event, err := s.appendMessagingRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: workspaceID,
		EventType:   "agent_request.sent",
		EntityType:  "agent_request",
		EntityID:    requestID,
		ActorType:   "agent",
		ActorID:     fromAgent,
		AgentID:     fromAgent,
		SessionID:   runtimeContext.SessionID,
		TaskID:      runtimeContext.TaskID,
		PayloadJSON: mustJSON(payloadJSON),
		CreatedAt:   now,
	})
	if err != nil {
		return "", RuntimeEventRecord{}, err
	}
	return requestID, event, nil
}

func (s *Store) RecordAgentRequestReusedWithAuthorityEvent(ctx context.Context, input AgentRequestReuseInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	requestID := strings.TrimSpace(input.RequestID)
	reusedByAgentID := strings.TrimSpace(input.ReusedByAgentID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	if requestID == "" {
		return RuntimeEventRecord{}, errors.New("request_id is required")
	}
	if reusedByAgentID == "" {
		return RuntimeEventRecord{}, errors.New("reused_by_agent_id is required")
	}
	record, err := s.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	if strings.TrimSpace(record.WorkspaceID) != workspaceID {
		return RuntimeEventRecord{}, fmt.Errorf("agent request workspace mismatch for reuse receipt: request=%s input=%s", record.WorkspaceID, workspaceID)
	}
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, referenceAt)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin agent request reuse authority tx: %w", err)
	}
	defer func() {
		if tx != nil {
		 _ = tx.Rollback()
		}
	}()
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		runtimeContext, err := s.inferMessagingRuntimeContextTx(ctx, tx, workspaceID, reusedByAgentID, record.FromAgentID, record.ToAgentID)
		if err != nil {
		 return err
		}
		payloadJSON := agentRequestRuntimeEventPayload(workspaceID, requestID, record.FromAgentID, record.ToAgentID, record.Method, record.Status, runtimeContext)
		payloadJSON["deduped"] = true
		payloadJSON["reused"] = true
		payloadJSON["reused_by_agent_id"] = reusedByAgentID
		payloadJSON["reused_no_wait_authority"] = true
		payloadJSON["coordination_progress"] = false
		if taskID := strings.TrimSpace(input.DelegateTaskID); taskID != "" {
		 payloadJSON["task_id"] = taskID
		 payloadJSON["delegate_task_id"] = taskID
		}
		if reason := strings.TrimSpace(input.ReuseReason); reason != "" {
		 payloadJSON["reuse_reason"] = reason
		}
		var innerErr error
		event, innerErr = s.appendMessagingRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		 EventID:     nextID("rtev"),
		 WorkspaceID: workspaceID,
		 EventType:   "agent_request.reused",
		 EntityType:  "agent_request",
		 EntityID:    requestID,
		 ActorType:   "agent",
		 ActorID:     reusedByAgentID,
		 AgentID:     reusedByAgentID,
		 SessionID:   runtimeContext.SessionID,
		 PayloadJSON: mustJSON(payloadJSON),
		 CreatedAt:   referenceAt,
		})
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit agent request reuse authority tx: %w", err)
	}
	tx = nil
	return event, nil
}

// RespondAgentRequest responds to an agent request.
func (s *Store) RespondAgentRequest(ctx context.Context, requestID, response string) error {
	_, err := s.RespondAgentRequestWithEvent(ctx, requestID, response)
	return err
}

// RespondAgentRequestWithEvent responds to an agent request and returns the appended runtime event row.
func (s *Store) RespondAgentRequestWithEvent(ctx context.Context, requestID, response string) (RuntimeEventRecord, error) {
	return s.RespondAgentRequestWithAuthorityEvent(ctx, requestID, response)
}

// RespondAgentRequestWithAuthorityEvent responds to an agent request from a public authority-backed surface.
func (s *Store) RespondAgentRequestWithAuthorityEvent(ctx context.Context, requestID, response string) (RuntimeEventRecord, error) {
	return s.respondAgentRequestWithAuthorityEvent(ctx, requestID, response, nil)
}

// RespondAgentRequestWithPromptContextAuthorityEvent responds to an agent request and attaches a verified prompt context envelope.
func (s *Store) RespondAgentRequestWithPromptContextAuthorityEvent(ctx context.Context, requestID, response string, promptContextEnvelope map[string]any) (RuntimeEventRecord, error) {
	return s.respondAgentRequestWithAuthorityEvent(ctx, requestID, response, promptContextEnvelope)
}

func (s *Store) respondAgentRequestWithAuthorityEvent(ctx context.Context, requestID, response string, promptContextEnvelope map[string]any) (RuntimeEventRecord, error) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return RuntimeEventRecord{}, errors.New("request_id is required")
	}
	record, err := s.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, strings.TrimSpace(record.WorkspaceID), authorityScopeWorkspace, referenceAt)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin respond agent request authority tx: %w", err)
	}
	defer func() {
		if tx != nil {
		 _ = tx.Rollback()
		}
	}()
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		event, innerErr = s.respondAgentRequestWithOptionalAuthorityTx(ctx, tx, authority, requestID, response, promptContextEnvelope)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit respond agent request authority tx: %w", err)
	}
	tx = nil
	return event, nil
}

func (s *Store) respondAgentRequestWithOptionalAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, requestID, response string, promptContextEnvelope map[string]any) (RuntimeEventRecord, error) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return RuntimeEventRecord{}, errors.New("request_id is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	var workspaceID string
	var fromAgentID string
	var toAgentID string
	var method string
	if err := tx.QueryRowContext(ctx,
		`UPDATE agent_requests
		 SET status = 'COMPLETED', response = ?, responded_at = ?
		 WHERE request_id = ?
		 AND (
		 status IN ('PENDING', 'PROCESSING')
		 OR (status = 'TIMEOUT' AND response IS NULL)
		 )
		 RETURNING workspace_id, from_agent_id, to_agent_id, method`,
		strings.TrimSpace(response), now, requestID,
	).Scan(&workspaceID, &fromAgentID, &toAgentID, &method); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
		 return RuntimeEventRecord{}, errors.New("request not found or already completed")
		}
		return RuntimeEventRecord{}, fmt.Errorf("respond agent request: %w", err)
	}
	runtimeContext, err := s.inferMessagingRuntimeContextTx(ctx, tx, workspaceID, fromAgentID, toAgentID)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	payloadJSON := agentResponseRuntimeEventPayload(workspaceID, requestID, fromAgentID, toAgentID, method, "COMPLETED", runtimeContext)
	if promptContextEnvelope != nil {
		envelope := clonePromptContextEnvelope(promptContextEnvelope)
		envelope["request_id"] = requestID
		payloadJSON, err = AttachAgentRequestPromptContextEnvelope(payloadJSON, envelope)
		if err != nil {
		 return RuntimeEventRecord{}, err
		}
		if err := validateAgentRequestPromptContextEnvelopeBinding(payloadJSON, envelope, "agent.respond"); err != nil {
		 return RuntimeEventRecord{}, err
		}
	}
	event, err := s.appendMessagingRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: workspaceID,
		EventType:   "agent_response.recorded",
		EntityType:  "agent_request",
		EntityID:    requestID,
		ActorType:   "agent",
		ActorID:     toAgentID,
		AgentID:     toAgentID,
		SessionID:   runtimeContext.SessionID,
		TaskID:      runtimeContext.TaskID,
		PayloadJSON: mustJSON(payloadJSON),
		CreatedAt:   now,
	})
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	return event, nil
}

// GetAgentRequestResult gets the result of an agent request.
func (s *Store) GetAgentRequestResult(ctx context.Context, requestID string) (AgentRequestRecord, error) {
	var r AgentRequestRecord
	var response, respondedAt *string
	err := s.db.QueryRowContext(ctx,
		`SELECT request_id, workspace_id, from_agent_id, to_agent_id, method, payload, status, response, created_at, responded_at, timeout_sec
		 FROM agent_requests WHERE request_id = ?`,
		strings.TrimSpace(requestID),
	).Scan(&r.RequestID, &r.WorkspaceID, &r.FromAgentID, &r.ToAgentID, &r.Method, &r.Payload, &r.Status, &response, &r.CreatedAt, &respondedAt, &r.TimeoutSec)
	if err != nil {
		return AgentRequestRecord{}, fmt.Errorf("get agent request: %w", err)
	}
	if response != nil {
		r.Response = *response
	}
	if respondedAt != nil && r.Status != "PROCESSING" {
		r.RespondedAt = *respondedAt
	}
	return r, nil
}

// ListOpenAgentRequests returns open requests for a target agent without
// claiming or otherwise mutating those requests.
func (s *Store) ListOpenAgentRequests(ctx context.Context, workspaceID, toAgentID string, limit int) ([]AgentRequestRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	toAgentID = strings.TrimSpace(toAgentID)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT request_id, workspace_id, from_agent_id, to_agent_id, method, payload, status, COALESCE(response, ''), created_at, COALESCE(responded_at, ''), timeout_sec
		 FROM agent_requests
		 WHERE workspace_id = ?
		 AND to_agent_id = ?
		 AND (
		 status IN ('PENDING', 'PROCESSING')
		 OR (status = 'TIMEOUT' AND response IS NULL)
		 )
		 ORDER BY CASE status WHEN 'PENDING' THEN 0 WHEN 'PROCESSING' THEN 1 ELSE 2 END, created_at DESC
		 LIMIT ?`,
		workspaceID, toAgentID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list open agent requests: %w", err)
	}
	defer rows.Close()
	var out []AgentRequestRecord
	for rows.Next() {
		var record AgentRequestRecord
		if err := rows.Scan(&record.RequestID, &record.WorkspaceID, &record.FromAgentID, &record.ToAgentID, &record.Method, &record.Payload, &record.Status, &record.Response, &record.CreatedAt, &record.RespondedAt, &record.TimeoutSec); err != nil {
		 return nil, fmt.Errorf("scan open agent request: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ListPendingAgentRequests atomically claims queued or recoverable timed-out
// requests for a target agent by moving them into PROCESSING before returning
// them.
func (s *Store) ListPendingAgentRequests(ctx context.Context, workspaceID, toAgentID string) ([]AgentRequestRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	toAgentID = strings.TrimSpace(toAgentID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	rows, err := s.writeDB.QueryContext(ctx,
		`UPDATE agent_requests
		 SET status = 'PROCESSING', responded_at = ?
		 WHERE rowid IN (
		 SELECT rowid
		 FROM agent_requests
		 WHERE workspace_id = ? AND to_agent_id = ?
		 AND (
		 status = 'PENDING'
		 OR (status = 'TIMEOUT' AND response IS NULL)
		 )
		 ORDER BY CASE status WHEN 'PENDING' THEN 0 ELSE 1 END, created_at ASC
		 )
		 RETURNING request_id, workspace_id, from_agent_id, to_agent_id, method, payload, status, created_at, timeout_sec`,
		now, workspaceID, toAgentID,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending requests: %w", err)
	}
	defer rows.Close()

	var requests []AgentRequestRecord
	for rows.Next() {
		var r AgentRequestRecord
		if err := rows.Scan(&r.RequestID, &r.WorkspaceID, &r.FromAgentID, &r.ToAgentID, &r.Method, &r.Payload, &r.Status, &r.CreatedAt, &r.TimeoutSec); err != nil {
		 return nil, fmt.Errorf("scan request: %w", err)
		}
		requests = append(requests, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].CreatedAt < requests[j].CreatedAt
	})
	return requests, nil
}

type agentRequestClaimCandidate struct {
	RequestID      string
	PreviousStatus string
}

// ListPendingAgentRequestsWithPromptContextEvents atomically claims queued or
// recoverable timed-out requests and appends one durable claim event per
// transitioned request.
func (s *Store) ListPendingAgentRequestsWithPromptContextEvents(ctx context.Context, workspaceID, toAgentID string, promptContextEnvelope map[string]any) ([]AgentRequestRecord, []RuntimeEventRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	toAgentID = strings.TrimSpace(toAgentID)
	if promptContextEnvelope == nil {
		return nil, nil, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return nil, nil, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin list pending agent requests authority tx: %w", err)
	}
	defer func() {
		if tx != nil {
		 _ = tx.Rollback()
		}
	}()

	var requests []AgentRequestRecord
	var events []RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		requests, events, innerErr = s.listPendingAgentRequestsWithPromptContextEventsTx(ctx, tx, authority, workspaceID, toAgentID, promptContextEnvelope, now)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return nil, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit list pending agent requests authority tx: %w", err)
	}
	tx = nil
	return requests, events, nil
}

func (s *Store) listPendingAgentRequestsWithPromptContextEventsTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID, toAgentID string, promptContextEnvelope map[string]any, now string) ([]AgentRequestRecord, []RuntimeEventRecord, error) {
	candidateRows, err := tx.QueryContext(ctx,
		`SELECT request_id, status
		 FROM agent_requests
		 WHERE workspace_id = ? AND to_agent_id = ?
		 AND (
		 status = 'PENDING'
		 OR (status = 'TIMEOUT' AND response IS NULL)
		 )
		 ORDER BY CASE status WHEN 'PENDING' THEN 0 ELSE 1 END, created_at ASC`,
		workspaceID, toAgentID,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("list pending request candidates: %w", err)
	}
	var candidates []agentRequestClaimCandidate
	previousStatusByRequestID := map[string]string{}
	for candidateRows.Next() {
		var candidate agentRequestClaimCandidate
		if err := candidateRows.Scan(&candidate.RequestID, &candidate.PreviousStatus); err != nil {
		 candidateRows.Close()
		 return nil, nil, fmt.Errorf("scan pending request candidate: %w", err)
		}
		candidates = append(candidates, candidate)
		previousStatusByRequestID[candidate.RequestID] = candidate.PreviousStatus
	}
	if err := candidateRows.Err(); err != nil {
		candidateRows.Close()
		return nil, nil, err
	}
	candidateRows.Close()
	if len(candidates) == 0 {
		return nil, nil, nil
	}

	placeholders := make([]string, 0, len(candidates))
	args := make([]any, 0, len(candidates)+1)
	args = append(args, now)
	for _, candidate := range candidates {
		placeholders = append(placeholders, "?")
		args = append(args, candidate.RequestID)
	}
	rows, err := tx.QueryContext(ctx,
		fmt.Sprintf(`UPDATE agent_requests
		 SET status = 'PROCESSING', responded_at = ?
		 WHERE request_id IN (%s)
		 AND (
		 status = 'PENDING'
		 OR (status = 'TIMEOUT' AND response IS NULL)
		 )
		 RETURNING request_id, workspace_id, from_agent_id, to_agent_id, method, payload, status, created_at, timeout_sec`, strings.Join(placeholders, ",")),
		args...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("list pending requests: %w", err)
	}
	defer rows.Close()

	var requests []AgentRequestRecord
	for rows.Next() {
		var r AgentRequestRecord
		if err := rows.Scan(&r.RequestID, &r.WorkspaceID, &r.FromAgentID, &r.ToAgentID, &r.Method, &r.Payload, &r.Status, &r.CreatedAt, &r.TimeoutSec); err != nil {
		 return nil, nil, fmt.Errorf("scan request: %w", err)
		}
		requests = append(requests, r)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].CreatedAt < requests[j].CreatedAt
	})

	events := make([]RuntimeEventRecord, 0, len(requests))
	for _, request := range requests {
		runtimeContext, err := s.inferMessagingRuntimeContextTx(ctx, tx, request.WorkspaceID, request.FromAgentID, request.ToAgentID)
		if err != nil {
		 return nil, nil, err
		}
		previousStatus := previousStatusByRequestID[request.RequestID]
		if previousStatus == "" {
		 previousStatus = "PENDING"
		}
		payloadJSON := agentRequestRuntimeEventPayload(request.WorkspaceID, request.RequestID, request.FromAgentID, request.ToAgentID, request.Method, "PROCESSING", runtimeContext)
		payloadJSON["previous_status"] = previousStatus
		payloadJSON["claimed_at"] = now
		if promptContextEnvelope != nil {
		 envelope := clonePromptContextEnvelope(promptContextEnvelope)
		 envelope["request_id"] = request.RequestID
		 envelope["from_agent_id"] = request.FromAgentID
		 envelope["to_agent_id"] = request.ToAgentID
		 envelope["method"] = request.Method
		 envelope["status"] = "PROCESSING"
		 envelope["previous_status"] = previousStatus
		 payloadJSON, err = AttachAgentRequestPromptContextEnvelope(payloadJSON, envelope)
		 if err != nil {
		 return nil, nil, err
		 }
		 if err := validateAgentRequestPromptContextEnvelopeBinding(payloadJSON, envelope, "agent.request.list"); err != nil {
		 return nil, nil, err
		 }
		}
		event, err := s.appendMessagingRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		 EventID:     nextID("rtev"),
		 WorkspaceID: request.WorkspaceID,
		 EventType:   "agent_request.claimed",
		 EntityType:  "agent_request",
		 EntityID:    request.RequestID,
		 ActorType:   "agent",
		 ActorID:     request.ToAgentID,
		 AgentID:     request.ToAgentID,
		 SessionID:   runtimeContext.SessionID,
		 TaskID:      runtimeContext.TaskID,
		 PayloadJSON: mustJSON(payloadJSON),
		 CreatedAt:   now,
		})
		if err != nil {
		 return nil, nil, err
		}
		events = append(events, event)
	}
	return requests, events, nil
}

// ExpireRequests marks stale queued or claimed requests as TIMEOUT so a
// crashed worker cannot leave PROCESSING rows wedged forever.
func (s *Store) ExpireRequests(ctx context.Context) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.writeDB.ExecContext(ctx,
		`UPDATE agent_requests SET status = 'TIMEOUT', responded_at = ?
		 WHERE (
		 status = 'PENDING'
		 AND CAST(strftime('%s', ?) AS INTEGER) - CAST(strftime('%s', created_at) AS INTEGER) > timeout_sec
		 ) OR (
		 status = 'PROCESSING'
		 AND CAST(strftime('%s', ?) AS INTEGER) - CAST(strftime('%s', COALESCE(responded_at, created_at)) AS INTEGER) > timeout_sec
		 )`,
		now, now, now,
	)
	if err != nil {
		return 0, fmt.Errorf("expire requests: %w", err)
	}
	rows, _ := result.RowsAffected()
	return rows, nil
}
