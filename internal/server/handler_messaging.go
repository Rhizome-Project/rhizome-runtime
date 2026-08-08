package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// ── Agent Messaging ─────────────────────────────────────────────────

type agentMessageSendParams struct {
	WorkspaceID  string `json:"workspace_id"`
	FromAgentID  string `json:"from_agent_id"`
	ToAgentID    string `json:"to_agent_id"`
	Channel      string `json:"channel"`
	ContentType  string `json:"content_type"`
	Content      string `json:"content"`
	MetadataJSON string `json:"metadata_json"`
}

func requireTrimmedParam(value, name string) *RPCError {
	if strings.TrimSpace(value) == "" {
		return &RPCError{Code: errCodeInvalidParams, Message: name + " is required"}
	}
	return nil
}

func validateMessageCursor(cursor string) *RPCError {
	if err := sqlite.ValidateMessageCursor(cursor); err != nil {
		if errors.Is(err, sqlite.ErrInvalidPollCursor) {
			return &RPCError{Code: errCodeInvalidPollCursor, Message: sqlite.ErrInvalidPollCursor.Error()}
		}
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return nil
}

func pollCursorRPCError(err error) *RPCError {
	if err == nil {
		return nil
	}
	if errors.Is(err, sqlite.ErrInvalidPollCursor) {
		return &RPCError{Code: errCodeInvalidPollCursor, Message: sqlite.ErrInvalidPollCursor.Error()}
	}
	return &RPCError{Code: errCodeInternal, Message: err.Error()}
}

func requestLookupRPCError(err error) *RPCError {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return &RPCError{Code: errCodeInvalidParams, Message: "request not found"}
	}
	msg := err.Error()
	if strings.Contains(msg, "request not found or already completed") {
		return &RPCError{Code: errCodeInvalidParams, Message: "request not found or already completed"}
	}
	if strings.Contains(msg, "get agent request:") && strings.Contains(msg, "no rows") {
		return &RPCError{Code: errCodeInvalidParams, Message: "request not found"}
	}
	return &RPCError{Code: errCodeInternal, Message: msg}
}

func (h *Handler) agentMessageSend(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentMessageSendParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	workspaceID := strings.TrimSpace(p.WorkspaceID)
	fromAgentID := strings.TrimSpace(p.FromAgentID)
	toAgentID := strings.TrimSpace(p.ToAgentID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(fromAgentID, "from_agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Content, "content"); rpcErr != nil {
		return nil, rpcErr
	}
	metadataJSON := strings.TrimSpace(p.MetadataJSON)
	if metadataJSON != "" && !json.Valid([]byte(metadataJSON)) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "metadata_json must be valid JSON"}
	}

	principal, rpcErr := requireAgentPrincipal(ctx, workspaceID, fromAgentID, "from_agent_id")
	if rpcErr != nil {
		return nil, rpcErr
	}

	// Detect mojibake: 5+ consecutive ? suggests the client replaced non-ASCII chars
	if strings.Contains(p.Content, "?????") {
		return nil, &RPCError{
			Code:    errCodeInvalidParams,
			Message: "message content appears to contain mojibake (long runs of '?' replacing non-ASCII characters). Your client is likely not sending valid UTF-8. Please write your content in English or fix your HTTP client's encoding to UTF-8.",
		}
	}
	if _, err := h.store.GetWorkspace(ctx, workspaceID); err != nil {
		return nil, h.rpcErrorFor(err)
	}
	channel := strings.TrimSpace(p.Channel)
	if channel == "" {
		channel = "default"
	}
	contentType := strings.TrimSpace(p.ContentType)
	if contentType == "" {
		contentType = "text/plain"
	}
	envelope := sqlite.BuildAgentMessagePromptContextEnvelope("agent.message.send", "server_rpc", workspaceID, principal.PrincipalType, principal.PrincipalID)
	envelope["actor_agent_id"] = fromAgentID
	envelope["from_agent_id"] = fromAgentID
	envelope["to_agent_id"] = toAgentID
	envelope["channel"] = channel

	messageID, event, err := h.store.SendMessageWithAuthorityEvent(ctx, sqlite.MessageSendInput{
		WorkspaceID:           workspaceID,
		FromAgentID:           fromAgentID,
		ToAgentID:             toAgentID,
		Channel:               channel,
		ContentType:           contentType,
		Content:               p.Content,
		MetadataJSON:          p.MetadataJSON,
		PromptContextEnvelope: envelope,
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "agent.message.send")
	}

	// Notify long-polling subscribers from the persisted runtime-event row while
	// preserving recipient-targeted wake semantics on the live alias surface.
	liveAgentID := toAgentID
	h.publishRuntimeEventRecordAlias(event, "agent.message", &liveAgentID, "", "Message from "+fromAgentID)

	// Update sender's last_seen_at so agent shows as online.
	if fromAgentID != "" && workspaceID != "" {
		h.touchAgentActivity(ctx, workspaceID, fromAgentID)
	}

	// Fire webhooks for message.send event.
	h.fireWebhooksAsync(workspaceID, "message.send", map[string]any{
		"message_id":    messageID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
	})

	return map[string]any{
		"message_id":   messageID,
		"workspace_id": workspaceID,
		"status":       "SENT",
	}, nil
}

type agentMessagePollParams struct {
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	AfterCreatedAt string `json:"after_created_at"` // opaque next_cursor from prior page; raw RFC3339 timestamps remain compatibility-only
	Limit          int    `json:"limit"`
	TimeoutSec     *int   `json:"timeout_sec"`    // nil = default 30s long-poll, 0 = immediate, >0 = custom long-poll (max 120)
	LookbackHours  int    `json:"lookback_hours"` // when after_created_at is empty, how many hours to look back (default 24)
}

var agentMessagePollBeforeSubscribeHook func()
var agentMessagePollAfterSubscribeHook func()

type agentPollMessage struct {
	MessageID    string `json:"message_id"`
	WorkspaceID  string `json:"workspace_id"`
	FromAgentID  string `json:"from_agent_id"`
	ToAgentID    string `json:"to_agent_id"`
	Channel      string `json:"channel"`
	ContentType  string `json:"content_type"`
	Content      string `json:"content"`
	MetadataJSON string `json:"metadata_json"`
	CreatedAt    string `json:"created_at"`
}

func toAgentPollMessages(records []sqlite.MessageRecord) []agentPollMessage {
	messages := make([]agentPollMessage, 0, len(records))
	for _, record := range records {
		messages = append(messages, agentPollMessage{
			MessageID:    record.MessageID,
			WorkspaceID:  record.WorkspaceID,
			FromAgentID:  record.FromAgentID,
			ToAgentID:    record.ToAgentID,
			Channel:      record.Channel,
			ContentType:  record.ContentType,
			Content:      record.Content,
			MetadataJSON: record.MetadataJSON,
			CreatedAt:    record.CreatedAt,
		})
	}
	return messages
}

func nextMessageCursor(records []sqlite.MessageRecord) string {
	if len(records) == 0 {
		return ""
	}
	last := records[len(records)-1]
	return sqlite.EncodeMessageCursor(last.CreatedAt, last.MessageID)
}

func pollResponse(messages []sqlite.MessageRecord, fallbackCursor string) map[string]any {
	nextCursor := nextMessageCursor(messages)
	if nextCursor == "" {
		nextCursor = strings.TrimSpace(fallbackCursor)
	}
	return map[string]any{
		"messages":    toAgentPollMessages(messages),
		"count":       len(messages),
		"next_cursor": nextCursor,
	}
}

func messageEventTouchesAgent(evt EventMessage, agentID string) bool {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false
	}
	if strings.TrimSpace(evt.AgentID) == "" || strings.TrimSpace(evt.AgentID) == agentID {
		return true
	}
	if strings.TrimSpace(evt.PayloadJSON) == "" {
		return false
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(evt.PayloadJSON), &payload); err != nil {
		return false
	}
	return strings.TrimSpace(messagePayloadString(payload, "from")) == agentID ||
		strings.TrimSpace(messagePayloadString(payload, "from_agent_id")) == agentID
}

func messagePayloadString(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return text
}

func (h *Handler) agentMessagePoll(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentMessagePollParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := validateMessageCursor(p.AfterCreatedAt); rpcErr != nil {
		return nil, rpcErr
	}
	if p.Limit <= 0 {
		p.Limit = 20
	}
	if p.LookbackHours <= 0 {
		p.LookbackHours = 24 // default: 24 hours
	}

	principal, ok := authPrincipalFromContext(ctx)
	if !ok {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "unauthorized"}
	}
	if p.WorkspaceID != principal.WorkspaceID {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "workspace isolation violation"}
	}
	if p.AgentID != principal.PrincipalID {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match polling agent"}
	}

	// Resolve timeout: nil (not provided) → default 30s, 0 → immediate, >0 → custom.
	effectiveTimeout := 30 // default: long-poll 30s
	if p.TimeoutSec != nil {
		effectiveTimeout = *p.TimeoutSec
	}

	// First, try an immediate query.
	messages, err := h.store.PollMessages(ctx, p.WorkspaceID, p.AgentID, p.AfterCreatedAt, p.Limit, p.LookbackHours)
	if err != nil {
		return nil, pollCursorRPCError(err)
	}

	// If messages found or immediate mode requested, return immediately.
	if len(messages) > 0 || effectiveTimeout <= 0 {
		return pollResponse(messages, p.AfterCreatedAt), nil
	}

	// Long-poll: subscribe before waiting, then re-check once to close the
	// query/subscribe race where a message can land after the first read but
	// before the subscriber is registered.
	if effectiveTimeout > 120 {
		effectiveTimeout = 120 // cap at 2 minutes
	}

	if agentMessagePollBeforeSubscribeHook != nil {
		agentMessagePollBeforeSubscribeHook()
	}

	ch := h.eventBus.Subscribe(p.WorkspaceID)
	defer h.eventBus.Unsubscribe(p.WorkspaceID, ch)

	if agentMessagePollAfterSubscribeHook != nil {
		agentMessagePollAfterSubscribeHook()
	}

	messages, err = h.store.PollMessages(ctx, p.WorkspaceID, p.AgentID, p.AfterCreatedAt, p.Limit, p.LookbackHours)
	if err != nil {
		return nil, pollCursorRPCError(err)
	}
	if len(messages) > 0 {
		return pollResponse(messages, p.AfterCreatedAt), nil
	}

	timer := time.NewTimer(time.Duration(effectiveTimeout) * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Client disconnected.
			return pollResponse(nil, p.AfterCreatedAt), nil

		case <-timer.C:
			// Timeout expired, return empty.
			return pollResponse(nil, p.AfterCreatedAt), nil

		case evt := <-ch:
			// Wake for recipient-targeted events, broadcasts, and self-sent direct
			// messages where the polling agent is the sender recorded in payload.
			if evt.Type != "agent.message" {
				continue
			}
			if !messageEventTouchesAgent(evt, p.AgentID) {
				continue // message for a different agent view
			}
			// Re-query DB to get the actual message(s). If the wake-up event did not
			// yield any visible rows (spurious event, already-filtered message, etc.),
			// keep waiting instead of returning an empty result early.
			messages, err = h.store.PollMessages(ctx, p.WorkspaceID, p.AgentID, p.AfterCreatedAt, p.Limit, p.LookbackHours)
			if err != nil {
				return nil, pollCursorRPCError(err)
			}
			if len(messages) == 0 {
				continue
			}
			return pollResponse(messages, p.AfterCreatedAt), nil
		}
	}
}

type agentMessageAckParams struct {
	WorkspaceID string   `json:"workspace_id"`
	AgentID     string   `json:"agent_id"`
	MessageIDs  []string `json:"message_ids"`
}

func (h *Handler) agentMessageAck(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentMessageAckParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	validMessageIDs := 0
	for _, messageID := range p.MessageIDs {
		if strings.TrimSpace(messageID) != "" {
			validMessageIDs++
		}
	}
	if validMessageIDs == 0 {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "message_ids is required"}
	}

	principal, ok := authPrincipalFromContext(ctx)
	if !ok {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "unauthorized"}
	}
	if p.WorkspaceID != principal.WorkspaceID {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "workspace isolation violation"}
	}
	if p.AgentID != principal.PrincipalID {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match agent_id"}
	}

	acknowledged, err := h.store.AckMessages(ctx, p.WorkspaceID, p.AgentID, p.MessageIDs)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"acknowledged": acknowledged,
		"status":       "OK",
	}, nil
}

// ── Agent State (Memory) ────────────────────────────────────────────

type agentStateSetParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Key         string `json:"key"`
	Value       string `json:"value"`
}

func (h *Handler) agentStateSet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentStateSetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	agentID := strings.TrimSpace(p.AgentID)
	key := strings.TrimSpace(p.Key)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(key, "key"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireAgentPrincipal(ctx, workspaceID, agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	if key == runtimeScratchAgentStateKey {
		result, err := h.store.SetAgentRuntimeScratchStateWithPendingTriggerReceipt(ctx, workspaceID, agentID, p.Value)
		if err != nil {
			return nil, rpcErrorFromStoreAuthority(err, "agent.state.set")
		}
		if result.RuntimeEventRecorded {
			h.publishRuntimeEventRecordAlias(result.RuntimeEvent, strings.ReplaceAll(result.RuntimeEvent.EventType, "_", "."), &agentID, "", result.RuntimeEvent.EventType)
		}
	} else if err := h.store.SetAgentState(ctx, workspaceID, agentID, key, p.Value); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"key":          key,
		"status":       "SAVED",
	}, nil
}

const runtimeScratchAgentStateKey = "rnar.runtime.v1"

type runtimeScratchPendingTriggerReceiptState struct {
	PendingTrigger        string `json:"pending_trigger"`
	PendingTriggerTask    string `json:"pending_trigger_task"`
	PendingTriggerSession string `json:"pending_trigger_session"`
	PendingTriggerAt      string `json:"pending_trigger_at"`
	ActiveTaskID          string `json:"active_task_id"`
	ActiveSessionID       string `json:"active_session_id"`
	LastWakeTrigger       string `json:"last_wake_trigger"`
}

func (h *Handler) recordRuntimePendingTriggerReceipt(ctx context.Context, workspaceID, agentID, previousRaw, nextRaw string) (sqlite.RuntimeEventRecord, bool, *RPCError) {
	previous, previousOK := decodeRuntimeScratchPendingTriggerReceiptState(previousRaw)
	next, nextOK := decodeRuntimeScratchPendingTriggerReceiptState(nextRaw)
	if !previousOK && !nextOK {
		return sqlite.RuntimeEventRecord{}, false, nil
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
		return sqlite.RuntimeEventRecord{}, false, nil
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
	event, err := h.store.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "runtime_pending_trigger",
		EntityID:    runtimePendingTriggerReceiptEntityID(agentID, trigger, taskID, sessionID),
		ActorType:   "agent",
		ActorID:     agentID,
		AgentID:     agentID,
		PayloadJSON: string(mustJSON(payload)),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return sqlite.RuntimeEventRecord{}, false, rpcErrorFromStoreAuthority(err, "agent.state.set")
	}
	return event, true, nil
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
	return normalizeRuntimePendingTriggerForReceipt(next.LastWakeTrigger) == trigger
}

func runtimePendingTriggerReceiptEntityID(agentID, trigger, taskID, sessionID string) string {
	parts := []string{strings.TrimSpace(agentID), strings.TrimSpace(trigger)}
	if taskID != "" {
		parts = append(parts, strings.TrimSpace(taskID))
	} else if sessionID != "" {
		parts = append(parts, strings.TrimSpace(sessionID))
	}
	entityID := strings.Join(parts, ":")
	if strings.TrimSpace(entityID) == "" {
		return "runtime_pending_trigger"
	}
	return entityID
}

type agentStateGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Key         string `json:"key"`
}

func (h *Handler) agentStateGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentStateGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	agentID := strings.TrimSpace(p.AgentID)
	key := strings.TrimSpace(p.Key)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(key, "key"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireAgentPrincipal(ctx, workspaceID, agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	value, err := h.store.GetAgentState(ctx, workspaceID, agentID, key)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"key":          key,
		"value":        value,
	}, nil
}

type agentStateListParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
}

func (h *Handler) agentStateList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentStateListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	agentID := strings.TrimSpace(p.AgentID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireAgentPrincipal(ctx, workspaceID, agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	entries, err := h.store.ListAgentStateKeys(ctx, workspaceID, agentID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"entries": entries,
		"count":   len(entries),
	}, nil
}

type agentStateDeleteParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Key         string `json:"key"`
}

func (h *Handler) agentStateDelete(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentStateDeleteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	agentID := strings.TrimSpace(p.AgentID)
	key := strings.TrimSpace(p.Key)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(key, "key"); rpcErr != nil {
		return nil, rpcErr
	}

	principal, ok := authPrincipalFromContext(ctx)
	if !ok {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "unauthorized"}
	}
	if workspaceID != principal.WorkspaceID {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "workspace isolation violation"}
	}
	if agentID != principal.PrincipalID {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match agent_id"}
	}

	if err := h.store.DeleteAgentState(ctx, workspaceID, agentID, key); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"key":          key,
		"status":       "DELETED",
	}, nil
}

// ── Agent-to-Agent RPC ──────────────────────────────────────────────

// Agent Heartbeat Leases

type agentHeartbeatLeaseAcquireParams struct {
	WorkspaceID string   `json:"workspace_id"`
	AgentID     string   `json:"agent_id"`
	HeartbeatID string   `json:"heartbeat_id"`
	OwnerID     string   `json:"owner_id"`
	LeaseToken  string   `json:"lease_token"`
	Locks       []string `json:"locks"`
	TTLSec      int      `json:"ttl_sec"`
}

type agentHeartbeatLeaseReleaseParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	HeartbeatID string `json:"heartbeat_id"`
	LeaseToken  string `json:"lease_token"`
}

func (h *Handler) agentHeartbeatLeaseAcquire(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.agentHeartbeatLeaseAcquireWithMethod(ctx, raw, false)
}

func (h *Handler) agentHeartbeatLeaseRefresh(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.agentHeartbeatLeaseAcquireWithMethod(ctx, raw, true)
}

func (h *Handler) agentHeartbeatLeaseAcquireWithMethod(ctx context.Context, raw json.RawMessage, refresh bool) (any, *RPCError) {
	var p agentHeartbeatLeaseAcquireParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	agentID := strings.TrimSpace(p.AgentID)
	heartbeatID := strings.TrimSpace(p.HeartbeatID)
	ownerID := strings.TrimSpace(p.OwnerID)
	leaseToken := strings.TrimSpace(p.LeaseToken)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(heartbeatID, "heartbeat_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(ownerID, "owner_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(leaseToken, "lease_token"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireAgentPrincipal(ctx, workspaceID, agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	input := sqlite.AgentHeartbeatLeaseInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		HeartbeatID: heartbeatID,
		OwnerID:     ownerID,
		LeaseToken:  leaseToken,
		Locks:       p.Locks,
		TTL:         time.Duration(p.TTLSec) * time.Second,
	}
	var (
		result sqlite.AgentHeartbeatLeaseResult
		err    error
	)
	if refresh {
		result, err = h.store.RefreshAgentHeartbeatLease(ctx, input)
	} else {
		result, err = h.store.AcquireAgentHeartbeatLease(ctx, input)
	}
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	status := "CONFLICT"
	if result.Acquired {
		status = "ACQUIRED"
	}
	return map[string]any{
		"status": status,
		"lease":  result,
	}, nil
}

func (h *Handler) agentHeartbeatLeaseRelease(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentHeartbeatLeaseReleaseParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	agentID := strings.TrimSpace(p.AgentID)
	heartbeatID := strings.TrimSpace(p.HeartbeatID)
	leaseToken := strings.TrimSpace(p.LeaseToken)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(heartbeatID, "heartbeat_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(leaseToken, "lease_token"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireAgentPrincipal(ctx, workspaceID, agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	released, err := h.store.ReleaseAgentHeartbeatLease(ctx, sqlite.AgentHeartbeatLeaseReleaseInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		HeartbeatID: heartbeatID,
		LeaseToken:  leaseToken,
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	status := "NOOP"
	if released {
		status = "RELEASED"
	}
	return map[string]any{
		"workspace_id": workspaceID,
		"agent_id":     agentID,
		"heartbeat_id": heartbeatID,
		"released":     released,
		"status":       status,
	}, nil
}

type agentRequestParams struct {
	WorkspaceID string `json:"workspace_id"`
	FromAgentID string `json:"from_agent_id"`
	ToAgentID   string `json:"to_agent_id"`
	Method      string `json:"method"`
	Payload     string `json:"payload"`
	PayloadJSON string `json:"payload_json"`
	TimeoutSec  int    `json:"timeout_sec"`
}

func (h *Handler) agentRequest(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentRequestParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	// Accept payload_json as a fallback for payload (client compat).
	if strings.TrimSpace(p.Payload) == "" && strings.TrimSpace(p.PayloadJSON) != "" {
		p.Payload = p.PayloadJSON
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	fromAgentID := strings.TrimSpace(p.FromAgentID)
	toAgentID := strings.TrimSpace(p.ToAgentID)
	method := strings.TrimSpace(p.Method)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(fromAgentID, "from_agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(toAgentID, "to_agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Payload, "payload"); rpcErr != nil {
		return nil, rpcErr
	}
	if method == "" {
		method = "default"
	}

	principal, rpcErr := requireAgentPrincipal(ctx, workspaceID, fromAgentID, "from_agent_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	if _, err := h.store.GetWorkspace(ctx, workspaceID); err != nil {
		return nil, h.rpcErrorFor(err)
	}

	if taskID := agentRequestDelegateTaskID(p.Payload); taskID != "" {
		existing, ok, err := h.store.FindOpenDelegateTaskAgentRequest(ctx, workspaceID, toAgentID, method, taskID)
		if err != nil {
			return nil, h.rpcErrorFor(err)
		}
		if ok {
			event, err := h.store.RecordAgentRequestReusedWithAuthorityEvent(ctx, sqlite.AgentRequestReuseInput{
				WorkspaceID:     workspaceID,
				RequestID:       existing.RequestID,
				ReusedByAgentID: fromAgentID,
				DelegateTaskID:  taskID,
				ReuseReason:     "open delegate task request already exists for recipient/method/task",
			})
			if err != nil {
				return nil, rpcErrorFromStoreAuthority(err, "agent.request")
			}
			liveAgentID := fromAgentID
			h.publishRuntimeEventRecordAlias(event, "agent.request.reused", &liveAgentID, "", "Reused existing request "+existing.RequestID)
			return map[string]any{
				"request_id":               existing.RequestID,
				"workspace_id":             workspaceID,
				"to_agent_id":              toAgentID,
				"status":                   existing.Status,
				"deduped":                  true,
				"reused":                   true,
				"reused_no_wait_authority": true,
				"coordination_progress":    false,
				"task_id":                  taskID,
				"runtime_event_id":         event.EventID,
			}, nil
		}
	}

	envelope := sqlite.BuildAgentRequestPromptContextEnvelope("agent.request", "server_rpc", workspaceID, principal.PrincipalType, principal.PrincipalID)
	envelope["actor_agent_id"] = fromAgentID
	envelope["from_agent_id"] = fromAgentID
	envelope["to_agent_id"] = toAgentID
	envelope["method"] = method
	envelope["status"] = "PENDING"

	requestID, event, err := h.store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID:           workspaceID,
		FromAgentID:           fromAgentID,
		ToAgentID:             toAgentID,
		Method:                method,
		Payload:               p.Payload,
		TimeoutSec:            p.TimeoutSec,
		PromptContextEnvelope: envelope,
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "agent.request")
	}

	liveAgentID := toAgentID
	h.publishRuntimeEventRecordAlias(event, "agent.request", &liveAgentID, "", "Incoming request from "+fromAgentID)

	return map[string]any{
		"request_id":   requestID,
		"workspace_id": workspaceID,
		"to_agent_id":  toAgentID,
		"status":       "PENDING",
	}, nil
}

func agentRequestDelegateTaskID(raw string) string {
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

type agentRespondParams struct {
	RequestID string `json:"request_id"`
	Response  string `json:"response"`
}

func (h *Handler) agentRespond(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentRespondParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	requestID := strings.TrimSpace(p.RequestID)
	if rpcErr := requireTrimmedParam(requestID, "request_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Response, "response"); rpcErr != nil {
		return nil, rpcErr
	}

	req, err := h.store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		return nil, requestLookupRPCError(err)
	}

	principal, rpcErr := requireAgentPrincipal(ctx, req.WorkspaceID, req.ToAgentID, "the request recipient")
	if rpcErr != nil {
		return nil, rpcErr
	}
	if _, err := h.store.GetWorkspace(ctx, req.WorkspaceID); err != nil {
		return nil, h.rpcErrorFor(err)
	}

	envelope := sqlite.BuildAgentRequestPromptContextEnvelope("agent.respond", "server_rpc", req.WorkspaceID, principal.PrincipalType, principal.PrincipalID)
	envelope["actor_agent_id"] = req.ToAgentID
	envelope["request_id"] = requestID
	envelope["from_agent_id"] = req.FromAgentID
	envelope["to_agent_id"] = req.ToAgentID
	envelope["method"] = req.Method
	envelope["status"] = "COMPLETED"

	event, err := h.store.RespondAgentRequestWithPromptContextAuthorityEvent(ctx, requestID, p.Response, envelope)
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "agent.respond"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, requestLookupRPCError(err)
	}
	liveAgentID := req.FromAgentID
	h.publishRuntimeEventRecordAlias(event, "agent.response", &liveAgentID, "", "Response received for "+requestID)

	return map[string]any{
		"request_id": requestID,
		"status":     "COMPLETED",
	}, nil
}

type agentRequestResultParams struct {
	RequestID string `json:"request_id"`
}

func (h *Handler) agentRequestResult(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentRequestResultParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.RequestID, "request_id"); rpcErr != nil {
		return nil, rpcErr
	}

	principal, ok := authPrincipalFromContext(ctx)
	if !ok {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "unauthorized"}
	}
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "agent principal required"}
	}

	result, err := h.store.GetAgentRequestResult(ctx, p.RequestID)
	if err != nil {
		return nil, requestLookupRPCError(err)
	}

	if result.WorkspaceID != principal.WorkspaceID {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "workspace isolation violation"}
	}
	if result.ToAgentID != principal.PrincipalID && result.FromAgentID != principal.PrincipalID {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: you are neither the sender nor recipient"}
	}

	return result, nil
}

type agentRequestListParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
}

type agentRequestOpenListParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Limit       int    `json:"limit,omitempty"`
}

func (h *Handler) agentRequestOpenList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentRequestOpenListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	agentID := strings.TrimSpace(p.AgentID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireAgentPrincipal(ctx, workspaceID, agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	requests, err := h.store.ListOpenAgentRequests(ctx, workspaceID, agentID, p.Limit)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"requests": requests,
		"count":    len(requests),
	}, nil
}

func (h *Handler) agentRequestList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentRequestListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	agentID := strings.TrimSpace(p.AgentID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireAgentPrincipal(ctx, workspaceID, agentID, "agent_id")
	if rpcErr != nil {
		return nil, rpcErr
	}

	envelope := sqlite.BuildAgentRequestPromptContextEnvelope("agent.request.list", "server_rpc", workspaceID, principal.PrincipalType, principal.PrincipalID)
	envelope["actor_agent_id"] = agentID
	envelope["to_agent_id"] = agentID
	envelope["status"] = "PROCESSING"
	envelope["previous_status"] = "PENDING"

	requests, events, err := h.store.ListPendingAgentRequestsWithPromptContextEvents(ctx, workspaceID, agentID, envelope)
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "agent.request.list"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	for idx, event := range events {
		if idx >= len(requests) {
			break
		}
		liveAgentID := requests[idx].FromAgentID
		h.publishRuntimeEventRecordAlias(event, "agent.request.claimed", &liveAgentID, "", "Request claimed by "+agentID)
	}
	return map[string]any{
		"requests": requests,
		"count":    len(requests),
	}, nil
}
