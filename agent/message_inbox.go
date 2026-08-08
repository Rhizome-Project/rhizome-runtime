package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	messageInboxStateVersion       = 1
	messageInboxMaxLiveMessages    = 256
	messageInboxMaxArchiveMessages = 512
)

type MessageInboxStats struct {
	Total             int    `json:"total"`
	Unread            int    `json:"unread"`
	Pending           int    `json:"pending"`
	Unacked           int    `json:"unacked"`
	MissedSinceStart  int    `json:"missed_since_start"`
	CarryoverPending  int    `json:"carryover_pending"`
	RuntimeStartedAt  string `json:"runtime_started_at,omitempty"`
	PreviousStartedAt string `json:"previous_started_at,omitempty"`
	LastSyncedCursor  string `json:"last_synced_cursor,omitempty"`
	LastSyncedAt      string `json:"last_synced_at,omitempty"`
}

type messageInboxState struct {
	Version                  int                 `json:"version"`
	WorkspaceID              string              `json:"workspace_id"`
	AgentID                  string              `json:"agent_id"`
	RuntimeStartedAt         string              `json:"runtime_started_at,omitempty"`
	PreviousRuntimeStartedAt string              `json:"previous_runtime_started_at,omitempty"`
	LastSyncedCursor         string              `json:"last_synced_cursor,omitempty"`
	LastSyncedAt             string              `json:"last_synced_at,omitempty"`
	Messages                 []messageInboxEntry `json:"messages"`
}

type messageInboxEntry struct {
	Message          MessageRecord `json:"message"`
	FirstSeenAt      string        `json:"first_seen_at,omitempty"`
	LastSeenAt       string        `json:"last_seen_at,omitempty"`
	FirstSyncedAt    string        `json:"first_synced_at,omitempty"`
	LastSyncedAt     string        `json:"last_synced_at,omitempty"`
	ReadAt           string        `json:"read_at,omitempty"`
	HandledAt        string        `json:"handled_at,omitempty"`
	AckedAt          string        `json:"acked_at,omitempty"`
	LastError        string        `json:"last_error,omitempty"`
	DeliveryAttempts int           `json:"delivery_attempts,omitempty"`
	AckAttempts      int           `json:"ack_attempts,omitempty"`
}

type MessageInbox struct {
	mu    sync.Mutex
	path  string
	state messageInboxState
	index map[string]int
}

func OpenMessageInbox(workspaceID, agentID string) (*MessageInbox, error) {
	path := messageInboxPath(workspaceID, agentID)
	if path == "" {
		return nil, fmt.Errorf("agent config root is unavailable")
	}
	inbox := &MessageInbox{
		path:  path,
		index: map[string]int{},
		state: messageInboxState{
			Version:     messageInboxStateVersion,
			WorkspaceID: strings.TrimSpace(workspaceID),
			AgentID:     strings.TrimSpace(agentID),
			Messages:    []messageInboxEntry{},
		},
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return inbox, nil
		}
		return nil, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return inbox, nil
	}

	if err := json.Unmarshal(data, &inbox.state); err != nil {
		if quarantineErr := quarantineCorruptInboxFile(path, data); quarantineErr != nil {
			return nil, fmt.Errorf("decode message inbox: %w", err)
		}
		return inbox, nil
	}
	if inbox.state.Version == 0 {
		inbox.state.Version = messageInboxStateVersion
	}
	if inbox.state.Messages == nil {
		inbox.state.Messages = []messageInboxEntry{}
	}
	inbox.reindexLocked()
	return inbox, nil
}

func messageInboxPath(workspaceID, agentID string) string {
	workspacePart := sanitizePathComponent(firstNonEmpty(workspaceID, "workspace"))
	agentPart := sanitizePathComponent(firstNonEmpty(agentID, "agent"))
	return agentRuntimeConfigPath("inbox", workspacePart, agentPart+".json")
}

func sanitizePathComponent(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := strings.Trim(b.String(), "._")
	if out == "" {
		return "unknown"
	}
	return out
}

func quarantineCorruptInboxFile(path string, data []byte) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("message inbox path is empty")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	backupPath := path + ".corrupt-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	if err := os.WriteFile(backupPath, data, 0o600); err != nil {
		return err
	}
	_ = os.Remove(path)
	return nil
}

func (i *MessageInbox) MarkRuntimeStarted(at time.Time) error {
	if i == nil {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	i.mu.Lock()
	i.state.PreviousRuntimeStartedAt = i.state.RuntimeStartedAt
	i.state.RuntimeStartedAt = at.UTC().Format(time.RFC3339Nano)
	err := i.saveLocked()
	i.mu.Unlock()
	return err
}

func (i *MessageInbox) LastSyncedCursor() string {
	if i == nil {
		return ""
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return strings.TrimSpace(i.state.LastSyncedCursor)
}

func (i *MessageInbox) RuntimeStartedAt() string {
	if i == nil {
		return ""
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return strings.TrimSpace(i.state.RuntimeStartedAt)
}

func (i *MessageInbox) RecordBatch(messages []MessageRecord, syncedAt time.Time, cursor string) error {
	if i == nil || len(messages) == 0 {
		return nil
	}
	if syncedAt.IsZero() {
		syncedAt = time.Now().UTC()
	}
	cursor = strings.TrimSpace(cursor)

	i.mu.Lock()
	defer i.mu.Unlock()
	for _, message := range messages {
		i.upsertMessageLocked(message, syncedAt)
	}
	if cursor == "" {
		cursor = i.computeLastCursorLocked()
	}
	if cursor != "" {
		i.state.LastSyncedCursor = cursor
	}
	i.state.LastSyncedAt = syncedAt.UTC().Format(time.RFC3339Nano)
	return i.saveLocked()
}

func (i *MessageInbox) SetLastSyncedCursor(cursor string, at time.Time) error {
	if i == nil {
		return nil
	}
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	i.mu.Lock()
	i.state.LastSyncedCursor = cursor
	i.state.LastSyncedAt = at.UTC().Format(time.RFC3339Nano)
	err := i.saveLocked()
	i.mu.Unlock()
	return err
}

func (i *MessageInbox) ClearLastSyncedCursor(at time.Time) error {
	if i == nil {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	i.mu.Lock()
	i.state.LastSyncedCursor = ""
	i.state.LastSyncedAt = at.UTC().Format(time.RFC3339Nano)
	err := i.saveLocked()
	i.mu.Unlock()
	return err
}

func (i *MessageInbox) PendingMessages() ([]MessageRecord, error) {
	if i == nil {
		return nil, nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	items := make([]MessageRecord, 0, len(i.state.Messages))
	for _, entry := range i.state.Messages {
		if strings.TrimSpace(entry.HandledAt) != "" {
			continue
		}
		items = append(items, cloneMessageRecord(entry.Message))
	}
	sortMessagesForDelivery(items)
	return items, nil
}

func (i *MessageInbox) UnackedMessages() ([]MessageRecord, error) {
	if i == nil {
		return nil, nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	items := make([]MessageRecord, 0, len(i.state.Messages))
	for _, entry := range i.state.Messages {
		if strings.TrimSpace(entry.HandledAt) == "" || strings.TrimSpace(entry.AckedAt) != "" {
			continue
		}
		items = append(items, cloneMessageRecord(entry.Message))
	}
	sortMessagesForDelivery(items)
	return items, nil
}

func (i *MessageInbox) MessageStatus(messageID string) (handled bool, acked bool, exists bool) {
	if i == nil {
		return false, false, false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	entry, ok := i.findLocked(messageID)
	if !ok {
		return false, false, false
	}
	return strings.TrimSpace(entry.HandledAt) != "", strings.TrimSpace(entry.AckedAt) != "", true
}

func (i *MessageInbox) MarkDeliveryAttempt(messageID string, at time.Time) error {
	if i == nil {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	entry, ok := i.findLocked(messageID)
	if !ok {
		return fmt.Errorf("message %s not found", strings.TrimSpace(messageID))
	}
	entry.DeliveryAttempts++
	entry.LastSeenAt = at.UTC().Format(time.RFC3339Nano)
	return i.saveLocked()
}

func (i *MessageInbox) MarkDeliveryFailure(messageID string, at time.Time, err error) error {
	if i == nil {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	entry, ok := i.findLocked(messageID)
	if !ok {
		return fmt.Errorf("message %s not found", strings.TrimSpace(messageID))
	}
	entry.LastSeenAt = at.UTC().Format(time.RFC3339Nano)
	if err != nil {
		entry.LastError = err.Error()
	}
	return i.saveLocked()
}

func (i *MessageInbox) MarkHandled(messageID string, at time.Time) error {
	if i == nil {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	entry, ok := i.findLocked(messageID)
	if !ok {
		return fmt.Errorf("message %s not found", strings.TrimSpace(messageID))
	}
	ts := at.UTC().Format(time.RFC3339Nano)
	if strings.TrimSpace(entry.FirstSeenAt) == "" {
		entry.FirstSeenAt = ts
	}
	entry.LastSeenAt = ts
	entry.ReadAt = firstNonEmpty(entry.ReadAt, ts)
	entry.HandledAt = firstNonEmpty(entry.HandledAt, ts)
	entry.LastError = ""
	return i.saveLocked()
}

func (i *MessageInbox) MarkAckAttempt(messageIDs []string, at time.Time) error {
	if i == nil || len(messageIDs) == 0 {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	ts := at.UTC().Format(time.RFC3339Nano)
	for _, messageID := range uniqueStrings(messageIDs) {
		entry, ok := i.findLocked(messageID)
		if !ok {
			return fmt.Errorf("message %s not found", strings.TrimSpace(messageID))
		}
		entry.AckAttempts++
		entry.LastSeenAt = ts
	}
	return i.saveLocked()
}

func (i *MessageInbox) MarkAcked(messageIDs []string, at time.Time) error {
	if i == nil || len(messageIDs) == 0 {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	ts := at.UTC().Format(time.RFC3339Nano)
	for _, messageID := range uniqueStrings(messageIDs) {
		entry, ok := i.findLocked(messageID)
		if !ok {
			return fmt.Errorf("message %s not found", strings.TrimSpace(messageID))
		}
		if strings.TrimSpace(entry.HandledAt) == "" {
			entry.HandledAt = ts
		}
		if strings.TrimSpace(entry.ReadAt) == "" {
			entry.ReadAt = ts
		}
		entry.AckedAt = firstNonEmpty(entry.AckedAt, ts)
		entry.LastError = ""
		entry.LastSeenAt = ts
	}
	return i.saveLocked()
}

func (i *MessageInbox) MarkAckFailure(messageIDs []string, at time.Time, err error) error {
	if i == nil || len(messageIDs) == 0 {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	ts := at.UTC().Format(time.RFC3339Nano)
	for _, messageID := range uniqueStrings(messageIDs) {
		entry, ok := i.findLocked(messageID)
		if !ok {
			return fmt.Errorf("message %s not found", strings.TrimSpace(messageID))
		}
		entry.LastSeenAt = ts
		if err != nil {
			entry.LastError = err.Error()
		}
	}
	return i.saveLocked()
}

func (i *MessageInbox) Stats() MessageInboxStats {
	if i == nil {
		return MessageInboxStats{}
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	stats := MessageInboxStats{
		Total:             len(i.state.Messages),
		RuntimeStartedAt:  strings.TrimSpace(i.state.RuntimeStartedAt),
		PreviousStartedAt: strings.TrimSpace(i.state.PreviousRuntimeStartedAt),
		LastSyncedCursor:  strings.TrimSpace(i.state.LastSyncedCursor),
		LastSyncedAt:      strings.TrimSpace(i.state.LastSyncedAt),
	}

	runtimeStarted, _ := parseRFC3339Nano(strings.TrimSpace(i.state.RuntimeStartedAt))
	for _, entry := range i.state.Messages {
		if strings.TrimSpace(entry.ReadAt) == "" {
			stats.Unread++
		}
		if strings.TrimSpace(entry.HandledAt) == "" {
			stats.Pending++
		}
		if strings.TrimSpace(entry.HandledAt) != "" && strings.TrimSpace(entry.AckedAt) == "" {
			stats.Unacked++
		}

		firstSeen, okFirst := parseRFC3339Nano(strings.TrimSpace(entry.FirstSeenAt))
		createdAt, okCreated := parseRFC3339Nano(strings.TrimSpace(entry.Message.CreatedAt))
		if !okFirst || runtimeStarted == nil {
			continue
		}
		if okCreated && !createdAt.After(*runtimeStarted) && !firstSeen.Before(*runtimeStarted) {
			stats.MissedSinceStart++
		}
		if strings.TrimSpace(entry.HandledAt) == "" && firstSeen.Before(*runtimeStarted) {
			stats.CarryoverPending++
		}
	}
	return stats
}

func (i *MessageInbox) Summary() string {
	stats := i.Stats()
	if stats.Total == 0 {
		return ""
	}
	parts := []string{
		fmt.Sprintf("pending=%d", stats.Pending),
		fmt.Sprintf("unread=%d", stats.Unread),
		fmt.Sprintf("unacked=%d", stats.Unacked),
		fmt.Sprintf("missed=%d", stats.MissedSinceStart),
	}
	if stats.CarryoverPending > 0 {
		parts = append(parts, fmt.Sprintf("carryover=%d", stats.CarryoverPending))
	}
	return "inbox " + strings.Join(parts, " ")
}

func (i *MessageInbox) computeLastCursorLocked() string {
	lastCursor := ""
	for _, entry := range i.state.Messages {
		if cursor := messageCursorForRecord(entry.Message); cursor != "" {
			lastCursor = cursor
		}
	}
	return lastCursor
}

func (i *MessageInbox) upsertMessageLocked(message MessageRecord, seenAt time.Time) {
	message = cloneMessageRecord(message)
	message.MessageID = strings.TrimSpace(message.MessageID)
	message.WorkspaceID = strings.TrimSpace(message.WorkspaceID)
	message.FromAgentID = strings.TrimSpace(message.FromAgentID)
	message.ToAgentID = strings.TrimSpace(message.ToAgentID)
	message.Channel = strings.TrimSpace(message.Channel)
	message.ContentType = strings.TrimSpace(message.ContentType)
	message.Content = strings.TrimSpace(message.Content)
	message.MetadataJSON = strings.TrimSpace(message.MetadataJSON)
	message.CreatedAt = strings.TrimSpace(message.CreatedAt)
	if strings.TrimSpace(message.ReadAt) != "" {
		message.ReadAt = strings.TrimSpace(message.ReadAt)
	}

	if message.MessageID == "" {
		return
	}
	ts := seenAt.UTC().Format(time.RFC3339Nano)
	if entry, ok := i.findLocked(message.MessageID); ok {
		if strings.TrimSpace(entry.FirstSeenAt) == "" {
			entry.FirstSeenAt = ts
		}
		entry.LastSeenAt = ts
		if strings.TrimSpace(entry.FirstSyncedAt) == "" {
			entry.FirstSyncedAt = ts
		}
		entry.LastSyncedAt = ts
		entry.Message = message
		return
	}
	i.index[message.MessageID] = len(i.state.Messages)
	i.state.Messages = append(i.state.Messages, messageInboxEntry{
		Message:       message,
		FirstSeenAt:   ts,
		LastSeenAt:    ts,
		FirstSyncedAt: ts,
		LastSyncedAt:  ts,
	})
}

func (i *MessageInbox) findLocked(messageID string) (*messageInboxEntry, bool) {
	if i == nil {
		return nil, false
	}
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return nil, false
	}
	index, ok := i.index[messageID]
	if !ok || index < 0 || index >= len(i.state.Messages) {
		return nil, false
	}
	return &i.state.Messages[index], true
}

func (i *MessageInbox) reindexLocked() {
	i.index = make(map[string]int, len(i.state.Messages))
	for idx := range i.state.Messages {
		entry := i.state.Messages[idx]
		messageID := strings.TrimSpace(entry.Message.MessageID)
		if messageID == "" {
			continue
		}
		i.index[messageID] = idx
	}
}

func (i *MessageInbox) saveLocked() error {
	if i == nil {
		return nil
	}
	i.state.Version = messageInboxStateVersion
	if i.state.Messages == nil {
		i.state.Messages = []messageInboxEntry{}
	}

	i.compactLocked()

	raw, err := json.MarshalIndent(i.state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode message inbox: %w", err)
	}

	dir := filepath.Dir(i.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return atomicWriteFile(i.path, raw, 0o600)
}

func (i *MessageInbox) compactLocked() {
	i.compactWithLimitsLocked(messageInboxMaxLiveMessages, messageInboxMaxArchiveMessages)
}

func (i *MessageInbox) compactWithLimitsLocked(liveLimit, archiveLimit int) {
	if i == nil || len(i.state.Messages) == 0 {
		return
	}

	var live []messageInboxEntry
	var archive []messageInboxEntry
	for _, entry := range i.state.Messages {
		if messageInboxEntryIsArchived(entry) {
			archive = append(archive, entry)
			continue
		}
		live = append(live, entry)
	}

	sort.SliceStable(live, func(a, b int) bool {
		return messageInboxEntryRetentionLess(live[a], live[b])
	})
	sort.SliceStable(archive, func(a, b int) bool {
		return messageInboxEntryRetentionLess(archive[a], archive[b])
	})

	if liveLimit >= 0 && len(live) > liveLimit {
		live = live[len(live)-liveLimit:]
	}
	if archiveLimit >= 0 && len(archive) > archiveLimit {
		archive = archive[len(archive)-archiveLimit:]
	}

	merged := append(live, archive...)
	sort.SliceStable(merged, func(a, b int) bool {
		return messageInboxEntryRetentionLess(merged[a], merged[b])
	})
	i.state.Messages = merged
	i.reindexLocked()
}

func messageInboxEntryIsArchived(entry messageInboxEntry) bool {
	return strings.TrimSpace(entry.HandledAt) != "" && strings.TrimSpace(entry.AckedAt) != ""
}

func messageInboxEntryRetentionLess(a, b messageInboxEntry) bool {
	if less, decided := orderedRFC3339NanoLess(a.FirstSeenAt, b.FirstSeenAt); decided {
		return less
	}
	if less, decided := orderedRFC3339NanoLess(a.LastSeenAt, b.LastSeenAt); decided {
		return less
	}
	if less, decided := orderedRFC3339NanoLess(a.Message.CreatedAt, b.Message.CreatedAt); decided {
		return less
	}
	if a.Message.MessageID != b.Message.MessageID {
		return a.Message.MessageID < b.Message.MessageID
	}
	if a.HandledAt != b.HandledAt {
		return a.HandledAt < b.HandledAt
	}
	if a.AckedAt != b.AckedAt {
		return a.AckedAt < b.AckedAt
	}
	return false
}

func orderedRFC3339NanoLess(a, b string) (bool, bool) {
	ta, oka := parseRFC3339Nano(strings.TrimSpace(a))
	tb, okb := parseRFC3339Nano(strings.TrimSpace(b))
	switch {
	case !oka && !okb:
		return false, false
	case !oka:
		return true, true
	case !okb:
		return false, true
	case ta.Before(*tb):
		return true, true
	case tb.Before(*ta):
		return false, true
	default:
		return false, false
	}
}

func cloneMessageRecord(message MessageRecord) MessageRecord {
	message.MessageID = strings.TrimSpace(message.MessageID)
	message.WorkspaceID = strings.TrimSpace(message.WorkspaceID)
	message.FromAgentID = strings.TrimSpace(message.FromAgentID)
	message.ToAgentID = strings.TrimSpace(message.ToAgentID)
	message.Channel = strings.TrimSpace(message.Channel)
	message.ContentType = strings.TrimSpace(message.ContentType)
	message.Content = strings.TrimSpace(message.Content)
	message.MetadataJSON = strings.TrimSpace(message.MetadataJSON)
	message.CreatedAt = strings.TrimSpace(message.CreatedAt)
	message.ReadAt = strings.TrimSpace(message.ReadAt)
	return message
}

func parseRFC3339Nano(value string) (*time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return &parsed, true
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return &parsed, true
	}
	return nil, false
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
