package main

import (
	"crypto/sha256"
	"encoding/hex"
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
	agentInternalSessionStateVersion = 1
	agentInternalSessionFilename     = "internal_sessions.json"
)

type AgentInternalSessionRecord struct {
	SessionID        string            `json:"session_id"`
	HeartbeatID      string            `json:"heartbeat_id"`
	HeartbeatKind    string            `json:"heartbeat_kind,omitempty"`
	AnatomyDigest    string            `json:"anatomy_digest,omitempty"`
	Status           string            `json:"status"`
	Trigger          string            `json:"trigger,omitempty"`
	Summary          string            `json:"summary,omitempty"`
	Outcome          string            `json:"outcome,omitempty"`
	Error            string            `json:"error,omitempty"`
	TaskIDs          []string          `json:"task_ids,omitempty"`
	DocKeys          []string          `json:"doc_keys,omitempty"`
	ArtifactRefs     []string          `json:"artifact_refs,omitempty"`
	PromotedRefs     []string          `json:"promoted_refs,omitempty"`
	StartedAt        string            `json:"started_at,omitempty"`
	EndedAt          string            `json:"ended_at,omitempty"`
	DurationMillis   int64             `json:"duration_millis,omitempty"`
	PromotionBlocked bool              `json:"promotion_blocked,omitempty"`
	Meta             map[string]string `json:"meta,omitempty"`
}

type AgentPersonalBacklogItem struct {
	ItemID            string            `json:"item_id"`
	DedupKey          string            `json:"dedup_key,omitempty"`
	HeartbeatID       string            `json:"heartbeat_id,omitempty"`
	Kind              string            `json:"kind,omitempty"`
	Status            string            `json:"status"`
	Title             string            `json:"title,omitempty"`
	Summary           string            `json:"summary,omitempty"`
	Score             int               `json:"score,omitempty"`
	EvidenceRefs      []string          `json:"evidence_refs,omitempty"`
	PromotionRefs     []string          `json:"promotion_refs,omitempty"`
	TaskIDs           []string          `json:"task_ids,omitempty"`
	DocKeys           []string          `json:"doc_keys,omitempty"`
	CreatedAt         string            `json:"created_at,omitempty"`
	UpdatedAt         string            `json:"updated_at,omitempty"`
	LastSeenAt        string            `json:"last_seen_at,omitempty"`
	SeenCount         int               `json:"seen_count,omitempty"`
	CompletedAt       string            `json:"completed_at,omitempty"`
	SuppressedUntil   string            `json:"suppressed_until,omitempty"`
	Stale             bool              `json:"stale,omitempty"`
	StaleReasons      []string          `json:"stale_reasons,omitempty"`
	LastSessionID     string            `json:"last_session_id,omitempty"`
	LastPromotedAt    string            `json:"last_promoted_at,omitempty"`
	PromotionAttempts int               `json:"promotion_attempts,omitempty"`
	Meta              map[string]string `json:"meta,omitempty"`
}

type AgentInternalSessionState struct {
	Version     int                          `json:"version"`
	WorkspaceID string                       `json:"workspace_id"`
	AgentID     string                       `json:"agent_id"`
	UpdatedAt   string                       `json:"updated_at,omitempty"`
	Sessions    []AgentInternalSessionRecord `json:"sessions"`
	Backlog     []AgentPersonalBacklogItem   `json:"backlog"`
}

type AgentInternalSessionStats struct {
	Sessions      int    `json:"sessions"`
	Running       int    `json:"running"`
	Completed     int    `json:"completed"`
	Failed        int    `json:"failed"`
	Abandoned     int    `json:"abandoned"`
	Backlog       int    `json:"backlog"`
	OpenBacklog   int    `json:"open_backlog"`
	StaleBacklog  int    `json:"stale_backlog"`
	PromotedItems int    `json:"promoted_items"`
	UpdatedAt     string `json:"updated_at,omitempty"`
	Revision      int64  `json:"revision,omitempty"`
}

type AgentInternalSessionStore struct {
	mu           sync.Mutex
	path         string
	state        AgentInternalSessionState
	revision     int64
	sessionIndex map[string]int
	backlogIndex map[string]int
	dedupIndex   map[string]int
}

func agentInternalSessionStorePath(workspaceID, agentID string) string {
	root := localMemoryStoreRootPath(workspaceID, agentID)
	if root == "" {
		return ""
	}
	return filepath.Join(root, agentInternalSessionFilename)
}

func LoadAgentInternalSessionState(workspaceID, agentID string) AgentInternalSessionState {
	path := agentInternalSessionStorePath(workspaceID, agentID)
	if strings.TrimSpace(path) == "" {
		return AgentInternalSessionState{}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return AgentInternalSessionState{}
	}
	var state AgentInternalSessionState
	if err := json.Unmarshal(data, &state); err != nil {
		return AgentInternalSessionState{}
	}
	return normalizeAgentInternalSessionState(state, workspaceID, agentID, time.Time{})
}

func OpenAgentInternalSessionStore(workspaceID, agentID string) (*AgentInternalSessionStore, error) {
	path := agentInternalSessionStorePath(workspaceID, agentID)
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("agent config root is unavailable")
	}
	state := LoadAgentInternalSessionState(workspaceID, agentID)
	state = normalizeAgentInternalSessionState(state, workspaceID, agentID, time.Time{})
	store := &AgentInternalSessionStore{
		path:  path,
		state: state,
	}
	store.reindexLocked()
	return store, nil
}

func (s *AgentInternalSessionStore) Snapshot() AgentInternalSessionState {
	if s == nil {
		return AgentInternalSessionState{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneAgentInternalSessionState(s.state)
}

func (s *AgentInternalSessionStore) Stats() AgentInternalSessionStats {
	if s == nil {
		return AgentInternalSessionStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return agentInternalSessionStatsLocked(s.state, s.revision)
}

func (s *AgentInternalSessionStore) RecordSession(record AgentInternalSessionRecord) (AgentInternalSessionRecord, error) {
	if s == nil {
		return AgentInternalSessionRecord{}, nil
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	record = normalizeAgentInternalSessionRecord(record, now)
	if idx, ok := s.sessionIndex[record.SessionID]; ok {
		existing := s.state.Sessions[idx]
		record.StartedAt = firstNonEmpty(record.StartedAt, existing.StartedAt)
		s.state.Sessions[idx] = record
	} else {
		s.state.Sessions = append(s.state.Sessions, record)
	}
	s.reindexLocked()
	return record, s.saveLocked(now)
}

func (s *AgentInternalSessionStore) BeginHeartbeatSession(heartbeat AgentHeartbeatSpec, anatomyDigest, trigger string, at time.Time) (AgentInternalSessionRecord, error) {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	record := normalizeAgentInternalSessionRecord(AgentInternalSessionRecord{
		HeartbeatID:   heartbeat.ID,
		HeartbeatKind: heartbeat.Kind,
		AnatomyDigest: anatomyDigest,
		Status:        "running",
		Trigger:       trigger,
		StartedAt:     at.UTC().Format(time.RFC3339Nano),
	}, at)
	return s.RecordSession(record)
}

func (s *AgentInternalSessionStore) CompleteSession(sessionID, status, outcome, summary string, promotedRefs []string, errInfo error, at time.Time) error {
	if s == nil {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.sessionIndex[strings.TrimSpace(sessionID)]
	if !ok {
		return fmt.Errorf("internal session %q not found", sessionID)
	}
	record := s.state.Sessions[idx]
	record.Status = normalizeAgentInternalSessionStatus(status)
	record.Outcome = strings.TrimSpace(outcome)
	record.Summary = firstNonEmpty(summary, record.Summary)
	record.PromotedRefs = uniqueTrimmedCSVStrings(append(record.PromotedRefs, promotedRefs...))
	record.EndedAt = at.UTC().Format(time.RFC3339Nano)
	if errInfo != nil {
		record.Error = errInfo.Error()
		if record.Status == "completed" {
			record.Status = "failed"
		}
	}
	record.DurationMillis = internalSessionDurationMillis(record.StartedAt, record.EndedAt)
	s.state.Sessions[idx] = normalizeAgentInternalSessionRecord(record, at)
	s.reindexLocked()
	return s.saveLocked(at)
}

func (s *AgentInternalSessionStore) UpsertBacklogItem(item AgentPersonalBacklogItem) (AgentPersonalBacklogItem, error) {
	if s == nil {
		return AgentPersonalBacklogItem{}, nil
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	requestedStatus := strings.TrimSpace(item.Status)
	item = normalizeAgentPersonalBacklogItem(item, now)
	if item.DedupKey == "" {
		return AgentPersonalBacklogItem{}, fmt.Errorf("personal backlog item requires a dedupe key or enough title/summary context to derive one")
	}
	if idx, ok := s.matchBacklogIndexLocked(item); ok {
		existing := s.state.Backlog[idx]
		item = mergeAgentPersonalBacklogItem(existing, item, requestedStatus, now)
		s.state.Backlog[idx] = item
	} else {
		s.state.Backlog = append(s.state.Backlog, item)
	}
	s.reindexLocked()
	return item, s.saveLocked(now)
}

func (s *AgentInternalSessionStore) MarkBacklogItemPromoted(itemID string, refs []string, at time.Time) error {
	if s == nil {
		return nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.backlogIndex[strings.TrimSpace(itemID)]
	if !ok {
		return fmt.Errorf("personal backlog item %q not found", itemID)
	}
	item := s.state.Backlog[idx]
	refs = uniqueTrimmedCSVStrings(refs)
	if item.Status == "promoted" && allTrimmedStringsPresent(item.PromotionRefs, refs) {
		return nil
	}
	item.Status = "promoted"
	item.PromotionRefs = uniqueTrimmedCSVStrings(append(item.PromotionRefs, refs...))
	item.LastPromotedAt = at.UTC().Format(time.RFC3339Nano)
	item.UpdatedAt = item.LastPromotedAt
	item.PromotionAttempts++
	s.state.Backlog[idx] = normalizeAgentPersonalBacklogItem(item, at)
	s.reindexLocked()
	return s.saveLocked(at)
}

func (s *AgentInternalSessionStore) MarkBacklogItemsStaleByPromotionRef(ref, reason string, at time.Time) (int, error) {
	if s == nil {
		return 0, nil
	}
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return 0, nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := 0
	for idx, item := range s.state.Backlog {
		if !containsTrimmedString(item.PromotionRefs, ref) {
			continue
		}
		item.Stale = true
		item.Status = "stale"
		item.StaleReasons = uniqueTrimmedCSVStrings(append(item.StaleReasons, firstNonEmpty(reason, "promotion_ref_invalidated")))
		item.UpdatedAt = at.UTC().Format(time.RFC3339Nano)
		s.state.Backlog[idx] = normalizeAgentPersonalBacklogItem(item, at)
		changed++
	}
	if changed == 0 {
		return 0, nil
	}
	s.reindexLocked()
	return changed, s.saveLocked(at)
}

func (s *AgentInternalSessionStore) AbandonRunningSessions(reason string, at time.Time) (int, error) {
	if s == nil {
		return 0, nil
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := 0
	for idx, record := range s.state.Sessions {
		if record.Status != "running" {
			continue
		}
		record.Status = "abandoned"
		record.Outcome = firstNonEmpty(record.Outcome, "abandoned_on_runtime_start")
		record.Error = firstNonEmpty(record.Error, firstNonEmpty(reason, "runtime restarted before internal session completed"))
		record.EndedAt = at.UTC().Format(time.RFC3339Nano)
		record.DurationMillis = internalSessionDurationMillis(record.StartedAt, record.EndedAt)
		s.state.Sessions[idx] = normalizeAgentInternalSessionRecord(record, at)
		changed++
	}
	if changed == 0 {
		return 0, nil
	}
	s.reindexLocked()
	return changed, s.saveLocked(at)
}

func (s *AgentInternalSessionStore) saveLocked(now time.Time) error {
	if s == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	s.state.UpdatedAt = now.UTC().Format(time.RFC3339Nano)
	s.state = normalizeAgentInternalSessionState(s.state, s.state.WorkspaceID, s.state.AgentID, now)
	raw, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	if err := atomicWriteFile(s.path, raw, 0o600); err != nil {
		return err
	}
	s.reindexLocked()
	s.revision++
	return nil
}

func (s *AgentInternalSessionStore) reindexLocked() {
	s.sessionIndex = map[string]int{}
	s.backlogIndex = map[string]int{}
	s.dedupIndex = map[string]int{}
	for idx, record := range s.state.Sessions {
		if record.SessionID != "" {
			s.sessionIndex[record.SessionID] = idx
		}
	}
	for idx, item := range s.state.Backlog {
		if item.ItemID != "" {
			s.backlogIndex[item.ItemID] = idx
		}
		if item.DedupKey != "" {
			s.dedupIndex[item.DedupKey] = idx
		}
	}
}

func (s *AgentInternalSessionStore) matchBacklogIndexLocked(item AgentPersonalBacklogItem) (int, bool) {
	if item.DedupKey != "" {
		if idx, ok := s.dedupIndex[item.DedupKey]; ok {
			return idx, true
		}
	}
	if item.ItemID != "" {
		idx, ok := s.backlogIndex[item.ItemID]
		return idx, ok
	}
	return 0, false
}

func normalizeAgentInternalSessionState(state AgentInternalSessionState, workspaceID, agentID string, now time.Time) AgentInternalSessionState {
	state.Version = agentInternalSessionStateVersion
	state.WorkspaceID = firstNonEmpty(state.WorkspaceID, strings.TrimSpace(workspaceID), "workspace")
	state.AgentID = firstNonEmpty(state.AgentID, strings.TrimSpace(agentID), "agent")
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if strings.TrimSpace(state.UpdatedAt) == "" {
		state.UpdatedAt = now.UTC().Format(time.RFC3339Nano)
	}
	if state.Sessions == nil {
		state.Sessions = []AgentInternalSessionRecord{}
	}
	if state.Backlog == nil {
		state.Backlog = []AgentPersonalBacklogItem{}
	}
	for idx := range state.Sessions {
		state.Sessions[idx] = normalizeAgentInternalSessionRecord(state.Sessions[idx], now)
	}
	for idx := range state.Backlog {
		state.Backlog[idx] = normalizeAgentPersonalBacklogItem(state.Backlog[idx], now)
	}
	sort.SliceStable(state.Sessions, func(i, j int) bool {
		return state.Sessions[i].StartedAt < state.Sessions[j].StartedAt
	})
	sort.SliceStable(state.Backlog, func(i, j int) bool {
		if state.Backlog[i].Status != state.Backlog[j].Status {
			return state.Backlog[i].Status < state.Backlog[j].Status
		}
		return state.Backlog[i].ItemID < state.Backlog[j].ItemID
	})
	return state
}

func normalizeAgentInternalSessionRecord(record AgentInternalSessionRecord, now time.Time) AgentInternalSessionRecord {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	record.HeartbeatID = strings.TrimSpace(record.HeartbeatID)
	record.HeartbeatKind = strings.TrimSpace(record.HeartbeatKind)
	record.AnatomyDigest = strings.TrimSpace(record.AnatomyDigest)
	record.Status = normalizeAgentInternalSessionStatus(record.Status)
	record.Trigger = strings.TrimSpace(record.Trigger)
	record.Summary = strings.TrimSpace(record.Summary)
	record.Outcome = strings.TrimSpace(record.Outcome)
	record.Error = strings.TrimSpace(record.Error)
	record.TaskIDs = uniqueTrimmedCSVStrings(record.TaskIDs)
	record.DocKeys = uniqueTrimmedCSVStrings(record.DocKeys)
	record.ArtifactRefs = uniqueTrimmedCSVStrings(record.ArtifactRefs)
	record.PromotedRefs = uniqueTrimmedCSVStrings(record.PromotedRefs)
	record.StartedAt = firstNonEmpty(record.StartedAt, now.UTC().Format(time.RFC3339Nano))
	record.EndedAt = strings.TrimSpace(record.EndedAt)
	record.DurationMillis = internalSessionDurationMillis(record.StartedAt, record.EndedAt)
	record.Meta = normalizeStringMap(record.Meta)
	record.SessionID = firstNonEmpty(record.SessionID, internalSessionStableID(record.HeartbeatID, record.Trigger, record.StartedAt))
	return record
}

func normalizeAgentInternalSessionStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "running", "started", "active":
		return "running"
	case "completed", "complete", "done", "ok":
		return "completed"
	case "failed", "error":
		return "failed"
	case "abandoned", "orphaned":
		return "abandoned"
	case "skipped", "skip":
		return "skipped"
	case "blocked":
		return "blocked"
	default:
		return "observed"
	}
}

func normalizeAgentPersonalBacklogItem(item AgentPersonalBacklogItem, now time.Time) AgentPersonalBacklogItem {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	item.DedupKey = strings.ToLower(strings.TrimSpace(item.DedupKey))
	item.HeartbeatID = strings.TrimSpace(item.HeartbeatID)
	item.Kind = strings.TrimSpace(item.Kind)
	item.Status = normalizeAgentPersonalBacklogStatus(item.Status)
	item.Title = strings.TrimSpace(item.Title)
	item.Summary = strings.TrimSpace(item.Summary)
	if item.Score < 0 {
		item.Score = 0
	}
	item.EvidenceRefs = uniqueTrimmedCSVStrings(item.EvidenceRefs)
	item.PromotionRefs = uniqueTrimmedCSVStrings(item.PromotionRefs)
	item.TaskIDs = uniqueTrimmedCSVStrings(item.TaskIDs)
	item.DocKeys = uniqueTrimmedCSVStrings(item.DocKeys)
	item.StaleReasons = uniqueTrimmedCSVStrings(item.StaleReasons)
	item.LastSessionID = strings.TrimSpace(item.LastSessionID)
	item.LastPromotedAt = strings.TrimSpace(item.LastPromotedAt)
	item.SuppressedUntil = strings.TrimSpace(item.SuppressedUntil)
	item.CompletedAt = strings.TrimSpace(item.CompletedAt)
	item.LastSeenAt = strings.TrimSpace(item.LastSeenAt)
	if item.SeenCount < 0 {
		item.SeenCount = 0
	}
	item.Meta = normalizeStringMap(item.Meta)
	ts := now.UTC().Format(time.RFC3339Nano)
	item.CreatedAt = firstNonEmpty(item.CreatedAt, ts)
	item.UpdatedAt = firstNonEmpty(item.UpdatedAt, ts)
	item.LastSeenAt = firstNonEmpty(item.LastSeenAt, item.UpdatedAt)
	if item.SeenCount == 0 {
		item.SeenCount = 1
	}
	item.DedupKey = firstNonEmpty(item.DedupKey, deriveAgentPersonalBacklogDedupKey(item))
	item.ItemID = firstNonEmpty(item.ItemID, internalSessionStableID(item.HeartbeatID, item.DedupKey, item.Title, item.CreatedAt))
	return item
}

func deriveAgentPersonalBacklogDedupKey(item AgentPersonalBacklogItem) string {
	parts := uniqueTrimmedCSVStrings([]string{
		strings.ToLower(strings.TrimSpace(item.HeartbeatID)),
		strings.ToLower(strings.TrimSpace(item.Kind)),
		strings.ToLower(strings.TrimSpace(item.Title)),
		strings.ToLower(strings.TrimSpace(item.Summary)),
	})
	if len(parts) == 0 {
		return ""
	}
	return "auto:" + shortRefHash(strings.Join(parts, "\x00"))
}

func mergeAgentPersonalBacklogItem(existing, incoming AgentPersonalBacklogItem, requestedStatus string, now time.Time) AgentPersonalBacklogItem {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	incoming.ItemID = existing.ItemID
	incoming.DedupKey = firstNonEmpty(existing.DedupKey, incoming.DedupKey)
	incoming.CreatedAt = firstNonEmpty(existing.CreatedAt, incoming.CreatedAt)
	incoming.HeartbeatID = firstNonEmpty(incoming.HeartbeatID, existing.HeartbeatID)
	incoming.Kind = firstNonEmpty(incoming.Kind, existing.Kind)
	incoming.Title = richerBacklogText(existing.Title, incoming.Title)
	incoming.Summary = richerBacklogText(existing.Summary, incoming.Summary)
	if existing.Score > incoming.Score {
		incoming.Score = existing.Score
	}
	incoming.EvidenceRefs = uniqueTrimmedCSVStrings(existing.EvidenceRefs, incoming.EvidenceRefs)
	incoming.PromotionRefs = uniqueTrimmedCSVStrings(existing.PromotionRefs, incoming.PromotionRefs)
	incoming.TaskIDs = uniqueTrimmedCSVStrings(existing.TaskIDs, incoming.TaskIDs)
	incoming.DocKeys = uniqueTrimmedCSVStrings(existing.DocKeys, incoming.DocKeys)
	incoming.StaleReasons = uniqueTrimmedCSVStrings(existing.StaleReasons, incoming.StaleReasons)
	incoming.PromotionAttempts += existing.PromotionAttempts
	incoming.LastSessionID = firstNonEmpty(incoming.LastSessionID, existing.LastSessionID)
	incoming.LastPromotedAt = firstNonEmpty(existing.LastPromotedAt, incoming.LastPromotedAt)
	incoming.CompletedAt = firstNonEmpty(existing.CompletedAt, incoming.CompletedAt)
	incoming.SuppressedUntil = firstNonEmpty(existing.SuppressedUntil, incoming.SuppressedUntil)
	incoming.Stale = existing.Stale || incoming.Stale
	incoming.Meta = mergeStringMaps(existing.Meta, incoming.Meta)
	incoming.SeenCount = existing.SeenCount + maxPositiveInt(incoming.SeenCount, 1)
	incoming.LastSeenAt = now.UTC().Format(time.RFC3339Nano)
	incoming.UpdatedAt = incoming.LastSeenAt
	if shouldPreserveExistingBacklogStatus(existing.Status, incoming.Status, requestedStatus) {
		incoming.Status = existing.Status
	}
	return normalizeAgentPersonalBacklogItem(incoming, now)
}

func shouldPreserveExistingBacklogStatus(existingStatus, incomingStatus, requestedStatus string) bool {
	existingStatus = normalizeAgentPersonalBacklogStatus(existingStatus)
	incomingStatus = normalizeAgentPersonalBacklogStatus(incomingStatus)
	requestedStatus = strings.TrimSpace(requestedStatus)
	if existingStatus == "open" {
		return false
	}
	if incomingStatus == existingStatus {
		return true
	}
	if requestedStatus == "" || incomingStatus == "open" {
		return true
	}
	return false
}

func richerBacklogText(existing, incoming string) string {
	existing = strings.TrimSpace(existing)
	incoming = strings.TrimSpace(incoming)
	if incoming == "" {
		return existing
	}
	if existing == "" {
		return incoming
	}
	if len(incoming) > len(existing) {
		return incoming
	}
	return existing
}

func maxPositiveInt(values ...int) int {
	out := 0
	for _, value := range values {
		if value > out {
			out = value
		}
	}
	return out
}

func mergeStringMaps(left, right map[string]string) map[string]string {
	if len(left) == 0 && len(right) == 0 {
		return nil
	}
	out := make(map[string]string, len(left)+len(right))
	for key, value := range left {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			out[key] = value
		}
	}
	for key, value := range right {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func allTrimmedStringsPresent(have, want []string) bool {
	for _, value := range uniqueTrimmedCSVStrings(want) {
		if !containsTrimmedString(have, value) {
			return false
		}
	}
	return true
}

func normalizeAgentPersonalBacklogStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "open", "pending", "candidate":
		return "open"
	case "promoted", "promotion":
		return "promoted"
	case "completed", "done", "closed":
		return "completed"
	case "suppressed", "snoozed":
		return "suppressed"
	case "stale", "invalidated":
		return "stale"
	default:
		return "open"
	}
}

func agentInternalSessionStatsLocked(state AgentInternalSessionState, revision int64) AgentInternalSessionStats {
	stats := AgentInternalSessionStats{
		Sessions:  len(state.Sessions),
		Backlog:   len(state.Backlog),
		UpdatedAt: strings.TrimSpace(state.UpdatedAt),
		Revision:  revision,
	}
	for _, record := range state.Sessions {
		switch record.Status {
		case "running":
			stats.Running++
		case "failed":
			stats.Failed++
		case "abandoned":
			stats.Abandoned++
			stats.Failed++
		case "completed":
			stats.Completed++
		}
	}
	for _, item := range state.Backlog {
		if item.Status == "open" {
			stats.OpenBacklog++
		}
		if item.Stale || item.Status == "stale" {
			stats.StaleBacklog++
		}
		if item.Status == "promoted" || len(item.PromotionRefs) > 0 {
			stats.PromotedItems++
		}
	}
	return stats
}

func cloneAgentInternalSessionState(state AgentInternalSessionState) AgentInternalSessionState {
	raw, err := json.Marshal(state)
	if err != nil {
		return AgentInternalSessionState{}
	}
	var out AgentInternalSessionState
	if err := json.Unmarshal(raw, &out); err != nil {
		return AgentInternalSessionState{}
	}
	return out
}

func internalSessionStableID(parts ...string) string {
	joined := strings.Join(uniqueTrimmedCSVStrings(parts), "\x00")
	if joined == "" {
		joined = time.Now().UTC().Format(time.RFC3339Nano)
	}
	sum := sha256.Sum256([]byte(joined))
	return "ais-" + hex.EncodeToString(sum[:])[:16]
}

func internalSessionDurationMillis(startedAt, endedAt string) int64 {
	start, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(startedAt))
	if err != nil {
		return 0
	}
	end, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(endedAt))
	if err != nil || end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}

func normalizeStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func containsTrimmedString(values []string, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}
