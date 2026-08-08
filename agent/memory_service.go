package main

import (
	"bufio"
	"bytes"
	"context"
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
	agentMemoryStateVersion = 1
	agentMemoryRecentLimit  = 96
	agentMemoryPacketTTL    = 45 * time.Second
)

type cachedMemoryPacket struct {
	Key            string
	Compiled       string
	BuiltAt        time.Time
	Sequence       int64
	StoreUpdatedAt string
	StoreRevision  int64
}

type AgentMemoryService struct {
	mu          sync.Mutex
	basePath    string
	statePath   string
	logPath     string
	store       *LocalMemoryStore
	state       agentMemoryState
	packetCache map[string]cachedMemoryPacket
	// constitution holds the Layer A config copied from agent anatomy. When
	// disabled (the default), the memory packet is byte-identical to legacy.
	constitution AgentAnatomyConstitution
}

// SetConstitution installs the Layer A constitution config (from agent
// anatomy). It rebuilds derived procedures so MinEvidence-dependent Global
// flagging takes effect, and invalidates the packet cache so the always-on
// section is recomputed.
func (s *AgentMemoryService) SetConstitution(c AgentAnatomyConstitution) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.constitution = c
	s.refreshProceduresLocked()
	s.packetCache = map[string]cachedMemoryPacket{}
}

func (s *AgentMemoryService) constitutionConfig() AgentAnatomyConstitution {
	if s == nil {
		return AgentAnatomyConstitution{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.constitution
}

func OpenAgentMemoryService(workspaceID, agentID string) (*AgentMemoryService, error) {
	basePath := agentMemoryBasePath(workspaceID, agentID)
	if basePath == "" {
		return nil, fmt.Errorf("agent config root is unavailable")
	}
	store, err := OpenLocalMemoryStore(workspaceID, agentID)
	if err != nil {
		return nil, err
	}
	service := &AgentMemoryService{
		basePath:    basePath,
		statePath:   filepath.Join(basePath, "service_state.json"),
		logPath:     filepath.Join(basePath, "episodic.jsonl"),
		store:       store,
		packetCache: map[string]cachedMemoryPacket{},
		state: agentMemoryState{
			Version:          agentMemoryStateVersion,
			WorkspaceID:      strings.TrimSpace(workspaceID),
			AgentID:          strings.TrimSpace(agentID),
			RecentEvents:     []LocalMemoryEvent{},
			TaskDigests:      map[string]LocalEpisodeDigest{},
			TensionDigests:   map[string]LocalEpisodeDigest{},
			ClusterDigests:   map[string]LocalEpisodeDigest{},
			Procedures:       map[string]LocalProcedureMemory{},
			AntiProcedures:   map[string]LocalProcedureMemory{},
			DocVersions:      map[string]string{},
			ArtifactVersions: map[string]string{},
		},
	}
	if err := os.MkdirAll(basePath, 0o700); err != nil {
		return nil, err
	}
	if err := service.loadState(); err != nil {
		return nil, err
	}
	if err := service.repairPersistentStoreFromRecoverySources(); err != nil {
		return nil, err
	}
	if err := service.reconcileShadowFromStore(true); err != nil {
		return nil, err
	}
	return service, nil
}

func agentMemoryBasePath(workspaceID, agentID string) string {
	return agentRuntimeConfigPath("memory", sanitizePathComponent(firstNonEmpty(workspaceID, "workspace")), sanitizePathComponent(firstNonEmpty(agentID, "agent")))
}

func (s *AgentMemoryService) loadState() error {
	data, err := os.ReadFile(s.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil
	}
	var state agentMemoryState
	if err := json.Unmarshal(data, &state); err != nil {
		if quarantineErr := quarantineCorruptLocalMemoryFile(s.statePath, data); quarantineErr != nil {
			return fmt.Errorf("decode agent memory state: %w", err)
		}
		return nil
	}
	if state.Version == 0 {
		state.Version = agentMemoryStateVersion
	}
	if state.TaskDigests == nil {
		state.TaskDigests = map[string]LocalEpisodeDigest{}
	}
	if state.TensionDigests == nil {
		state.TensionDigests = map[string]LocalEpisodeDigest{}
	}
	if state.ClusterDigests == nil {
		state.ClusterDigests = map[string]LocalEpisodeDigest{}
	}
	if state.Procedures == nil {
		state.Procedures = map[string]LocalProcedureMemory{}
	}
	if state.AntiProcedures == nil {
		state.AntiProcedures = map[string]LocalProcedureMemory{}
	}
	if state.DocVersions == nil {
		state.DocVersions = map[string]string{}
	}
	if state.ArtifactVersions == nil {
		state.ArtifactVersions = map[string]string{}
	}
	if state.RecentEvents == nil {
		state.RecentEvents = []LocalMemoryEvent{}
	}
	s.state = state
	return nil
}

func (s *AgentMemoryService) saveStateLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.statePath), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(stripAgentMemoryShadowState(s.state), "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(s.statePath, raw, 0o600)
}

func (s *AgentMemoryService) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	store := s.store
	s.store = nil
	s.mu.Unlock()
	if store == nil {
		return nil
	}
	return store.Close()
}

func (s *AgentMemoryService) rememberDocVersions(docVersions map[string]string) error {
	if s == nil || len(docVersions) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.DocVersions == nil {
		s.state.DocVersions = map[string]string{}
	}
	changed := false
	for key, version := range docVersions {
		docKey := strings.TrimSpace(key)
		docVersion := strings.TrimSpace(version)
		if docKey == "" || docVersion == "" {
			continue
		}
		if s.state.DocVersions[docKey] == docVersion {
			continue
		}
		s.state.DocVersions[docKey] = docVersion
		changed = true
	}
	if !changed {
		return nil
	}
	s.packetCache = map[string]cachedMemoryPacket{}
	return s.saveStateLocked()
}

func (s *AgentMemoryService) rememberArtifactVersions(artifactVersions map[string]string) error {
	if s == nil || len(artifactVersions) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.ArtifactVersions == nil {
		s.state.ArtifactVersions = map[string]string{}
	}
	changed := false
	for ref, version := range artifactVersions {
		artifactRef := strings.TrimSpace(ref)
		artifactVersion := strings.TrimSpace(version)
		if artifactRef == "" || artifactVersion == "" {
			continue
		}
		if s.state.ArtifactVersions[artifactRef] == artifactVersion {
			continue
		}
		s.state.ArtifactVersions[artifactRef] = artifactVersion
		changed = true
	}
	if !changed {
		return nil
	}
	s.packetCache = map[string]cachedMemoryPacket{}
	return s.saveStateLocked()
}

func (s *AgentMemoryService) canonicalVersionSnapshot() (map[string]string, map[string]string) {
	if s == nil {
		return map[string]string{}, map[string]string{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneStringMap(s.state.DocVersions), cloneStringMap(s.state.ArtifactVersions)
}

func stripAgentMemoryShadowState(state agentMemoryState) agentMemoryState {
	state.RecentEvents = nil
	state.TaskDigests = nil
	state.TensionDigests = nil
	state.ClusterDigests = nil
	state.Procedures = nil
	state.AntiProcedures = nil
	return state
}

func hasAgentMemoryShadowState(state agentMemoryState) bool {
	return len(state.RecentEvents) > 0 ||
		len(state.TaskDigests) > 0 ||
		len(state.TensionDigests) > 0 ||
		len(state.ClusterDigests) > 0 ||
		len(state.Procedures) > 0 ||
		len(state.AntiProcedures) > 0
}

func (s *AgentMemoryService) reconcileShadowFromStore(force bool) error {
	if s == nil || s.store == nil {
		return nil
	}
	snapshot := s.store.Snapshot()
	events, taskDigests, tensionDigests, clusterDigests, docVersions, artifactVersions := buildShadowFromStoreSnapshot(snapshot)

	s.mu.Lock()
	defer s.mu.Unlock()

	changed := false
	if s.state.TaskDigests == nil {
		s.state.TaskDigests = map[string]LocalEpisodeDigest{}
	}
	if s.state.TensionDigests == nil {
		s.state.TensionDigests = map[string]LocalEpisodeDigest{}
	}
	if s.state.ClusterDigests == nil {
		s.state.ClusterDigests = map[string]LocalEpisodeDigest{}
	}
	if s.state.Procedures == nil {
		s.state.Procedures = map[string]LocalProcedureMemory{}
	}
	if s.state.AntiProcedures == nil {
		s.state.AntiProcedures = map[string]LocalProcedureMemory{}
	}
	if s.state.DocVersions == nil {
		s.state.DocVersions = map[string]string{}
	}
	if s.state.ArtifactVersions == nil {
		s.state.ArtifactVersions = map[string]string{}
	}
	if force || len(s.state.RecentEvents) == 0 {
		s.state.RecentEvents = append([]LocalMemoryEvent(nil), events...)
		changed = true
	}
	if force || len(s.state.TaskDigests) == 0 {
		s.state.TaskDigests = cloneEpisodeDigestMap(taskDigests)
		changed = true
	} else {
		changed = mergeDigestShadowLocked(s.state.TaskDigests, taskDigests) || changed
	}
	if force || len(s.state.TensionDigests) == 0 {
		s.state.TensionDigests = cloneEpisodeDigestMap(tensionDigests)
		changed = true
	} else {
		changed = mergeDigestShadowLocked(s.state.TensionDigests, tensionDigests) || changed
	}
	if force || len(s.state.ClusterDigests) == 0 {
		s.state.ClusterDigests = cloneEpisodeDigestMap(clusterDigests)
		changed = true
	} else {
		changed = mergeDigestShadowLocked(s.state.ClusterDigests, clusterDigests) || changed
	}
	for key, version := range docVersions {
		if key == "" || version == "" {
			continue
		}
		if s.state.DocVersions[key] == version {
			continue
		}
		s.state.DocVersions[key] = version
		changed = true
	}
	for ref, version := range artifactVersions {
		if ref == "" || version == "" {
			continue
		}
		if s.state.ArtifactVersions[ref] == version {
			continue
		}
		s.state.ArtifactVersions[ref] = version
		changed = true
	}
	s.refreshProceduresLocked()
	if !changed {
		return nil
	}
	s.refreshDerivedStatsLocked()
	s.state.Stats.LastShadowSyncAt = time.Now().UTC().Format(time.RFC3339Nano)
	s.packetCache = map[string]cachedMemoryPacket{}
	return s.saveStateLocked()
}

func (s *AgentMemoryService) PruneExpiredAndCold(cfg LocalMemoryTTLConfig) error {
	if s == nil || s.store == nil {
		return nil
	}
	now := time.Now().UTC()
	prunedDigests, prunedEpisodes, err := s.store.Prune(now, cfg)
	if err != nil {
		return fmt.Errorf("local memory store prune: %w", err)
	}
	if prunedDigests > 0 || prunedEpisodes > 0 {
		// Log message will be generated by the caller or we can log here
		// but since we don't import "log" in memory_service.go (wait, we do or might not)
		// I will just return the error. Let's check imports.
		if err := s.reconcileShadowFromStore(true); err != nil {
			return fmt.Errorf("reconcile shadow after prune: %w", err)
		}
	}
	return nil
}

func (s *AgentMemoryService) repairPersistentStoreFromRecoverySources() error {
	if s == nil || s.store == nil {
		return nil
	}
	storeSnapshot := s.store.Snapshot()
	logEvents, err := readLocalMemoryEpisodicLog(s.logPath)
	if err != nil {
		return err
	}
	maxStoreSequence := maxLocalMemoryEpisodeSequence(storeSnapshot.Episodes)
	maxLogSequence := maxLocalMemoryEventSequence(logEvents)

	switch {
	case len(logEvents) > 0 && maxLogSequence > maxStoreSequence:
		return s.rebuildPersistentStoreFromEvents(logEvents)
	case len(storeSnapshot.Episodes) == 0 && hasAgentMemoryShadowState(s.state):
		legacyEvents := cloneLocalMemoryEvents(s.state.RecentEvents)
		if len(legacyEvents) > 0 {
			return s.rebuildPersistentStoreFromEvents(legacyEvents)
		}
		return s.rebuildPersistentStoreFromShadow()
	default:
		return nil
	}
}

func mergeDigestShadowLocked(target, incoming map[string]LocalEpisodeDigest) bool {
	changed := false
	for key, digest := range incoming {
		existing, ok := target[key]
		if !ok || compareLocalMemoryTimestamps(digest.UpdatedAt, existing.UpdatedAt) > 0 {
			target[key] = digest
			changed = true
		}
	}
	return changed
}

func cloneLocalMemoryEvents(src []LocalMemoryEvent) []LocalMemoryEvent {
	if len(src) == 0 {
		return nil
	}
	out := make([]LocalMemoryEvent, 0, len(src))
	for _, event := range src {
		copy := event
		copy.ArtifactRefs = append([]string(nil), event.ArtifactRefs...)
		copy.DocKeys = append([]string(nil), event.DocKeys...)
		copy.SegmentRefs = append([]string(nil), event.SegmentRefs...)
		copy.ConstraintRefs = append([]string(nil), event.ConstraintRefs...)
		copy.BlockerKinds = append([]string(nil), event.BlockerKinds...)
		out = append(out, copy)
	}
	return out
}

func readLocalMemoryEpisodicLog(path string) ([]LocalMemoryEvent, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	buffer := make([]byte, 0, 64*1024)
	scanner.Buffer(buffer, 2*1024*1024)
	batchEvents := make([]LocalMemoryEvent, 0, 20000)
	salvageNeeded := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event LocalMemoryEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			salvageNeeded = true
			continue
		}
		event = normalizeLocalMemoryReplayEvent(event)
		if event.Sequence == 0 && event.OccurredAt == "" && event.EventKind == "" {
			salvageNeeded = true
			continue
		}
		batchEvents = append(batchEvents, event)
		if len(batchEvents) > 20000 {
			// Keep the last 10000 elements to bound memory and avoid OOM
			batchEvents = append([]LocalMemoryEvent(nil), batchEvents[10000:]...)
			salvageNeeded = true
		}
	}
	if err := scanner.Err(); err != nil {
		salvageNeeded = true
	}
	events := batchEvents
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Sequence != events[j].Sequence {
			return events[i].Sequence < events[j].Sequence
		}
		return compareLocalMemoryTimestamps(events[i].OccurredAt, events[j].OccurredAt) < 0
	})
	deduped, dedupedSomething := dedupeLocalMemoryReplayEvents(events)
	events = deduped
	if dedupedSomething {
		salvageNeeded = true
	}
	if salvageNeeded {
		if err := salvageLocalMemoryEpisodicLog(path, data, events); err != nil {
			return nil, err
		}
	}
	return events, nil
}

func dedupeLocalMemoryReplayEvents(events []LocalMemoryEvent) ([]LocalMemoryEvent, bool) {
	if len(events) == 0 {
		return nil, false
	}
	out := make([]LocalMemoryEvent, 0, len(events))
	seenSequences := map[int64]struct{}{}
	seenFallback := map[string]struct{}{}
	deduped := false
	for _, event := range events {
		if event.Sequence > 0 {
			if _, ok := seenSequences[event.Sequence]; ok {
				deduped = true
				continue
			}
			seenSequences[event.Sequence] = struct{}{}
			out = append(out, event)
			continue
		}
		key := strings.Join([]string{
			strings.TrimSpace(event.OccurredAt),
			strings.TrimSpace(event.EventKind),
			strings.TrimSpace(event.TaskID),
			strings.TrimSpace(event.SessionID),
			strings.TrimSpace(event.TensionID),
			strings.TrimSpace(event.ProtoClusterID),
			strings.TrimSpace(event.SourceID),
			strings.TrimSpace(event.Summary),
		}, "|")
		if _, ok := seenFallback[key]; ok {
			deduped = true
			continue
		}
		seenFallback[key] = struct{}{}
		out = append(out, event)
	}
	return out, deduped
}

func salvageLocalMemoryEpisodicLog(path string, original []byte, events []LocalMemoryEvent) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	if len(original) > 0 {
		if err := quarantineCorruptLocalMemoryFile(path, original); err != nil {
			return err
		}
	} else {
		_ = os.Remove(path)
	}
	if len(events) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	var b strings.Builder
	for _, event := range events {
		raw, err := json.Marshal(event)
		if err != nil {
			return err
		}
		b.Write(raw)
		b.WriteByte('\n')
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}

func normalizeLocalMemoryReplayEvent(event LocalMemoryEvent) LocalMemoryEvent {
	event.OccurredAt = strings.TrimSpace(event.OccurredAt)
	event.EventKind = strings.TrimSpace(strings.ToLower(event.EventKind))
	event.Summary = strings.TrimSpace(event.Summary)
	event.Details = strings.TrimSpace(event.Details)
	event.TaskID = strings.TrimSpace(event.TaskID)
	event.SessionID = strings.TrimSpace(event.SessionID)
	event.TensionID = strings.TrimSpace(event.TensionID)
	event.ProtoClusterID = strings.TrimSpace(event.ProtoClusterID)
	event.Outcome = strings.TrimSpace(event.Outcome)
	event.SourceID = strings.TrimSpace(event.SourceID)
	event.ArtifactRefs = uniqueTrimmedFocusStrings(event.ArtifactRefs)
	event.DocKeys = uniqueTrimmedFocusStrings(event.DocKeys)
	event.SegmentRefs = uniqueTrimmedFocusStrings(event.SegmentRefs)
	event.ConstraintRefs = uniqueTrimmedFocusStrings(event.ConstraintRefs)
	event.BlockerKinds = uniqueTrimmedFocusStrings(event.BlockerKinds)
	if event.NodeType == "" {
		event.NodeType = localMemoryNodeRawEvent
	}
	return event
}

func maxLocalMemoryEventSequence(events []LocalMemoryEvent) int64 {
	var maxSequence int64
	for _, event := range events {
		if event.Sequence > maxSequence {
			maxSequence = event.Sequence
		}
	}
	return maxSequence
}

func maxLocalMemoryEpisodeSequence(items []LocalMemoryEpisodeRecord) int64 {
	var maxSequence int64
	for _, item := range items {
		sequence, ok := parseLocalMemoryEpisodeSequence(item.EpisodeID)
		if ok && sequence > maxSequence {
			maxSequence = sequence
		}
	}
	return maxSequence
}

func parseLocalMemoryEpisodeSequence(episodeID string) (int64, bool) {
	episodeID = strings.TrimSpace(episodeID)
	if !strings.HasPrefix(episodeID, "event-") {
		return 0, false
	}
	value := strings.TrimPrefix(episodeID, "event-")
	sequence, err := parseInt64(value)
	if err != nil {
		return 0, false
	}
	return sequence, true
}

func parseInt64(value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("empty")
	}
	var out int64
	for _, ch := range trimmed {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("non-digit")
		}
		out = out*10 + int64(ch-'0')
	}
	return out, nil
}

func buildShadowFromStoreSnapshot(snapshot LocalMemoryState) ([]LocalMemoryEvent, map[string]LocalEpisodeDigest, map[string]LocalEpisodeDigest, map[string]LocalEpisodeDigest, map[string]string, map[string]string) {
	taskDigests := map[string]LocalEpisodeDigest{}
	tensionDigests := map[string]LocalEpisodeDigest{}
	clusterDigests := map[string]LocalEpisodeDigest{}
	docVersions := map[string]string{}
	artifactVersions := map[string]string{}

	for _, record := range snapshot.Digests {
		if record.Stale {
			continue
		}
		digest, ok := digestRecordToEpisodeDigest(record)
		if !ok || strings.TrimSpace(digest.ScopeKey) == "" {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(record.Kind)) {
		case "TASK_DIGEST":
			mergeDigestRecordIntoMap(taskDigests, digest)
		case "TENSION_DIGEST":
			mergeDigestRecordIntoMap(tensionDigests, digest)
		case "CLUSTER_DIGEST":
			mergeDigestRecordIntoMap(clusterDigests, digest)
		}
		for _, guard := range record.Guards {
			if strings.EqualFold(strings.TrimSpace(guard.GuardType), "doc_sha") {
				docKey := strings.TrimSpace(guard.Ref)
				if docKey != "" && strings.TrimSpace(guard.Version) != "" {
					docVersions[docKey] = strings.TrimSpace(guard.Version)
				}
				continue
			}
			if strings.EqualFold(strings.TrimSpace(guard.GuardType), "artifact_version") {
				artifactRef := strings.TrimSpace(guard.Ref)
				if artifactRef != "" && strings.TrimSpace(guard.Version) != "" {
					artifactVersions[artifactRef] = strings.TrimSpace(guard.Version)
				}
			}
		}
	}

	events := make([]LocalMemoryEvent, 0, len(snapshot.Episodes))
	activeEpisodes := make([]LocalMemoryEpisodeRecord, 0, len(snapshot.Episodes))
	for _, episode := range snapshot.Episodes {
		if strings.TrimSpace(episode.SupersededAt) != "" {
			continue
		}
		activeEpisodes = append(activeEpisodes, cloneEpisodeRecord(episode))
	}
	sort.SliceStable(activeEpisodes, func(i, j int) bool {
		return compareLocalMemoryTimestamps(activeEpisodes[i].CreatedAt, activeEpisodes[j].CreatedAt) < 0
	})
	if len(activeEpisodes) > agentMemoryRecentLimit {
		activeEpisodes = activeEpisodes[len(activeEpisodes)-agentMemoryRecentLimit:]
	}
	for idx, episode := range activeEpisodes {
		events = append(events, episodeRecordToMemoryEvent(episode, int64(idx+1)))
	}
	return events, taskDigests, tensionDigests, clusterDigests, docVersions, artifactVersions
}

func (s *AgentMemoryService) rebuildPersistentStoreFromEvents(events []LocalMemoryEvent) error {
	if s == nil || s.store == nil || len(events) == 0 {
		return nil
	}
	docVersions, artifactVersions := s.canonicalVersionSnapshot()
	state := buildLocalMemoryStoreStateFromEvents(events, s.store.state.WorkspaceID, s.store.state.AgentID, docVersions, artifactVersions)
	return s.store.ReplaceState(state)
}

func (s *AgentMemoryService) rebuildPersistentStoreFromShadow() error {
	if s == nil || s.store == nil {
		return nil
	}
	docVersions, artifactVersions := s.canonicalVersionSnapshot()
	state := buildLocalMemoryStoreStateFromShadow(s.state, s.store.state.WorkspaceID, s.store.state.AgentID, docVersions, artifactVersions)
	return s.store.ReplaceState(state)
}

func buildLocalMemoryStoreStateFromEvents(events []LocalMemoryEvent, workspaceID, agentID string, docVersions, artifactVersions map[string]string) LocalMemoryState {
	state := LocalMemoryState{
		Version:     localMemoryStateVersion,
		WorkspaceID: strings.TrimSpace(workspaceID),
		AgentID:     strings.TrimSpace(agentID),
		Episodes:    []LocalMemoryEpisodeRecord{},
		Digests:     []LocalMemoryDigestRecord{},
	}
	if len(events) == 0 {
		return state
	}
	sorted := cloneLocalMemoryEvents(events)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Sequence != sorted[j].Sequence {
			return sorted[i].Sequence < sorted[j].Sequence
		}
		return compareLocalMemoryTimestamps(sorted[i].OccurredAt, sorted[j].OccurredAt) < 0
	})

	taskDigests := map[string]LocalEpisodeDigest{}
	tensionDigests := map[string]LocalEpisodeDigest{}
	clusterDigests := map[string]LocalEpisodeDigest{}
	lastTaskEvents := map[string]LocalMemoryEvent{}
	lastTensionEvents := map[string]LocalMemoryEvent{}
	lastClusterEvents := map[string]LocalMemoryEvent{}
	lastUpdatedAt := ""

	for _, event := range sorted {
		event = normalizeLocalMemoryReplayEvent(event)
		if event.OccurredAt == "" {
			event.OccurredAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		if event.Sequence <= 0 {
			event.Sequence = int64(len(state.Episodes) + 1)
		}
		state.Episodes = append(state.Episodes, LocalMemoryEpisodeRecord{
			EpisodeID: fmt.Sprintf("event-%d", event.Sequence),
			Scope: LocalMemoryScope{
				TaskID:         event.TaskID,
				SessionID:      event.SessionID,
				TensionID:      event.TensionID,
				ProtoClusterID: event.ProtoClusterID,
			},
			RunID:          event.SourceID,
			Trigger:        event.EventKind,
			Outcome:        event.Outcome,
			Summary:        firstNonEmpty(event.Summary, event.Details),
			DocKeys:        append([]string(nil), event.DocKeys...),
			ArtifactRefs:   append([]string(nil), event.ArtifactRefs...),
			ConstraintRefs: append([]string(nil), event.ConstraintRefs...),
			SegmentRefs:    append([]string(nil), event.SegmentRefs...),
			Meta:           localMemoryEpisodeMetaFromEvent(event),
			CreatedAt:      event.OccurredAt,
		})
		lastUpdatedAt = event.OccurredAt

		if event.TaskID != "" {
			digest := taskDigests[event.TaskID]
			applyEventToDigest(&digest, "task", event.TaskID, event)
			taskDigests[event.TaskID] = digest
			lastTaskEvents[event.TaskID] = event
		}
		if event.TensionID != "" {
			digest := tensionDigests[event.TensionID]
			applyEventToDigest(&digest, "tension", event.TensionID, event)
			tensionDigests[event.TensionID] = digest
			lastTensionEvents[event.TensionID] = event
		}
		if clusterKey := strings.TrimSpace(event.ProtoClusterID); clusterKey != "" {
			digest := clusterDigests[clusterKey]
			applyEventToDigest(&digest, "cluster", clusterKey, event)
			clusterDigests[clusterKey] = digest
			lastClusterEvents[clusterKey] = event
		}
	}

	for taskID, digest := range taskDigests {
		state.Digests = append(state.Digests, buildStoreDigestRecord("task:"+taskID, "TASK_DIGEST", digest, lastTaskEvents[taskID], docVersions, artifactVersions))
	}
	for tensionID, digest := range tensionDigests {
		state.Digests = append(state.Digests, buildStoreDigestRecord("tension:"+tensionID, "TENSION_DIGEST", digest, lastTensionEvents[tensionID], docVersions, artifactVersions))
	}
	for clusterID, digest := range clusterDigests {
		state.Digests = append(state.Digests, buildStoreDigestRecord("cluster:"+clusterID, "CLUSTER_DIGEST", digest, lastClusterEvents[clusterID], docVersions, artifactVersions))
	}
	state.UpdatedAt = strings.TrimSpace(lastUpdatedAt)
	return state
}

func buildLocalMemoryStoreStateFromShadow(state agentMemoryState, workspaceID, agentID string, docVersions, artifactVersions map[string]string) LocalMemoryState {
	if len(state.RecentEvents) > 0 {
		return buildLocalMemoryStoreStateFromEvents(state.RecentEvents, workspaceID, agentID, docVersions, artifactVersions)
	}
	out := LocalMemoryState{
		Version:     localMemoryStateVersion,
		WorkspaceID: strings.TrimSpace(workspaceID),
		AgentID:     strings.TrimSpace(agentID),
		Episodes:    []LocalMemoryEpisodeRecord{},
		Digests:     []LocalMemoryDigestRecord{},
	}
	for taskID, digest := range state.TaskDigests {
		event := LocalMemoryEvent{
			TaskID:         strings.TrimSpace(taskID),
			SessionID:      strings.TrimSpace(digest.LatestSessionID),
			TensionID:      strings.TrimSpace(digest.LatestTensionID),
			ProtoClusterID: strings.TrimSpace(digest.ProtoClusterID),
			Summary:        strings.TrimSpace(digest.LastSummary),
			OccurredAt:     strings.TrimSpace(digest.UpdatedAt),
		}
		out.Digests = append(out.Digests, buildStoreDigestRecord("task:"+strings.TrimSpace(taskID), "TASK_DIGEST", digest, event, docVersions, artifactVersions))
	}
	for tensionID, digest := range state.TensionDigests {
		event := LocalMemoryEvent{
			TaskID:         "",
			SessionID:      strings.TrimSpace(digest.LatestSessionID),
			TensionID:      strings.TrimSpace(tensionID),
			ProtoClusterID: strings.TrimSpace(digest.ProtoClusterID),
			Summary:        strings.TrimSpace(digest.LastSummary),
			OccurredAt:     strings.TrimSpace(digest.UpdatedAt),
		}
		out.Digests = append(out.Digests, buildStoreDigestRecord("tension:"+strings.TrimSpace(tensionID), "TENSION_DIGEST", digest, event, docVersions, artifactVersions))
	}
	for clusterID, digest := range state.ClusterDigests {
		event := LocalMemoryEvent{
			TaskID:         "",
			SessionID:      strings.TrimSpace(digest.LatestSessionID),
			TensionID:      strings.TrimSpace(digest.LatestTensionID),
			ProtoClusterID: strings.TrimSpace(clusterID),
			Summary:        strings.TrimSpace(digest.LastSummary),
			OccurredAt:     strings.TrimSpace(digest.UpdatedAt),
		}
		out.Digests = append(out.Digests, buildStoreDigestRecord("cluster:"+strings.TrimSpace(clusterID), "CLUSTER_DIGEST", digest, event, docVersions, artifactVersions))
	}
	return out
}

func mergeDigestRecordIntoMap(target map[string]LocalEpisodeDigest, digest LocalEpisodeDigest) {
	existing, ok := target[digest.ScopeKey]
	if !ok || compareLocalMemoryTimestamps(digest.UpdatedAt, existing.UpdatedAt) > 0 {
		target[digest.ScopeKey] = digest
	}
}

func digestRecordToEpisodeDigest(record LocalMemoryDigestRecord) (LocalEpisodeDigest, bool) {
	if record.EpisodeDigest != nil {
		return *cloneLocalEpisodeDigestPtr(record.EpisodeDigest), true
	}
	digest := LocalEpisodeDigest{
		ScopeKey:        firstNonEmpty(strings.TrimSpace(record.Meta["scope_key"]), strings.TrimSpace(record.Scope.TaskID), strings.TrimSpace(record.Scope.TensionID), strings.TrimSpace(record.Scope.ProtoClusterID)),
		ScopeKind:       firstNonEmpty(strings.TrimSpace(record.Meta["scope_kind"]), inferDigestScopeKind(record)),
		UpdatedAt:       firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt)),
		LastSummary:     strings.TrimSpace(record.Summary),
		ProtoClusterID:  strings.TrimSpace(record.Scope.ProtoClusterID),
		LatestTensionID: strings.TrimSpace(record.Scope.TensionID),
		LatestSessionID: strings.TrimSpace(record.Scope.SessionID),
		DocKeys:         append([]string(nil), record.DocKeys...),
		ArtifactRefs:    append([]string(nil), record.ArtifactRefs...),
		ConstraintRefs:  append([]string(nil), record.ConstraintRefs...),
	}
	if digest.ScopeKey == "" || digest.ScopeKind == "" {
		return LocalEpisodeDigest{}, false
	}
	return digest, true
}

func inferDigestScopeKind(record LocalMemoryDigestRecord) string {
	switch strings.ToUpper(strings.TrimSpace(record.Kind)) {
	case "TASK_DIGEST":
		return "task"
	case "TENSION_DIGEST":
		return "tension"
	case "CLUSTER_DIGEST":
		return "cluster"
	default:
		return ""
	}
}

func episodeRecordToMemoryEvent(record LocalMemoryEpisodeRecord, sequence int64) LocalMemoryEvent {
	event := LocalMemoryEvent{
		Sequence:       sequence,
		OccurredAt:     firstNonEmpty(strings.TrimSpace(record.CreatedAt), strings.TrimSpace(record.SupersededAt)),
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      firstNonEmpty(strings.TrimSpace(record.Trigger), "p2_recall"),
		Summary:        strings.TrimSpace(record.Summary),
		Details:        strings.TrimSpace(record.Outcome),
		TaskID:         strings.TrimSpace(record.Scope.TaskID),
		SessionID:      strings.TrimSpace(record.Scope.SessionID),
		TensionID:      strings.TrimSpace(record.Scope.TensionID),
		ProtoClusterID: strings.TrimSpace(record.Scope.ProtoClusterID),
		ArtifactRefs:   append([]string(nil), record.ArtifactRefs...),
		DocKeys:        append([]string(nil), record.DocKeys...),
		SegmentRefs:    append([]string(nil), record.SegmentRefs...),
		ConstraintRefs: append([]string(nil), record.ConstraintRefs...),
		Outcome:        strings.TrimSpace(record.Outcome),
		SourceID:       firstNonEmpty(strings.TrimSpace(record.RunID), strings.TrimSpace(record.EpisodeID)),
	}
	hydrateLocalMemoryEventFromRecordMeta(&event, record.Meta)
	return event
}

func (s *AgentMemoryService) appendEvent(input LocalMemoryEvent) error {
	s.mu.Lock()

	now := time.Now().UTC()
	s.state.LastSequence++
	input.Sequence = s.state.LastSequence
	input.OccurredAt = firstNonEmpty(strings.TrimSpace(input.OccurredAt), now.Format(time.RFC3339Nano))
	input.EventKind = strings.TrimSpace(strings.ToLower(input.EventKind))
	input.Summary = strings.TrimSpace(input.Summary)
	input.Details = strings.TrimSpace(input.Details)
	input.TaskID = strings.TrimSpace(input.TaskID)
	input.SessionID = strings.TrimSpace(input.SessionID)
	input.TensionID = strings.TrimSpace(input.TensionID)
	input.ProtoClusterID = strings.TrimSpace(input.ProtoClusterID)
	input.SourceID = strings.TrimSpace(input.SourceID)
	input.ArtifactRefs = uniqueTrimmedFocusStrings(input.ArtifactRefs)
	input.DocKeys = uniqueTrimmedFocusStrings(input.DocKeys)
	input.SegmentRefs = uniqueTrimmedFocusStrings(input.SegmentRefs)
	input.ConstraintRefs = uniqueTrimmedFocusStrings(input.ConstraintRefs)
	input.BlockerKinds = uniqueTrimmedFocusStrings(input.BlockerKinds)
	if input.NodeType == "" {
		input.NodeType = localMemoryNodeRawEvent
	}

	// MEM-01 (static-audit 2026-06-02): every error path below must release s.mu before
	// returning. A bare `return err` while holding the lock (e.g. a transient disk-full /
	// permission failure on the append) permanently wedged the entire memory subsystem AND the
	// watchdog that calls controlSnapshot under the same lock. defer is intentionally NOT used:
	// the success path releases the lock before syncPersistentStore so the network sync runs
	// unlocked.
	if err := os.MkdirAll(filepath.Dir(s.logPath), 0o700); err != nil {
		s.mu.Unlock()
		return err
	}
	f, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		_ = f.Close()
		s.mu.Unlock()
		return err
	}
	writer := bufio.NewWriter(f)
	if _, err := writer.Write(append(encoded, '\n')); err != nil {
		_ = f.Close()
		s.mu.Unlock()
		return err
	}
	if err := writer.Flush(); err != nil {
		_ = f.Close()
		s.mu.Unlock()
		return err
	}
	if err := f.Close(); err != nil {
		s.mu.Unlock()
		return err
	}

	s.state.RecentEvents = append(s.state.RecentEvents, input)
	if len(s.state.RecentEvents) > agentMemoryRecentLimit {
		s.state.RecentEvents = append([]LocalMemoryEvent(nil), s.state.RecentEvents[len(s.state.RecentEvents)-agentMemoryRecentLimit:]...)
	}
	s.updateStatsForEventLocked(input)
	s.refreshDigestsLocked(input)
	s.refreshProceduresLocked()
	s.packetCache = map[string]cachedMemoryPacket{}
	taskDigest := s.state.TaskDigests[strings.TrimSpace(input.TaskID)]
	tensionDigest := s.state.TensionDigests[strings.TrimSpace(input.TensionID)]
	clusterDigest := s.state.ClusterDigests[strings.TrimSpace(input.ProtoClusterID)]
	if err := s.saveStateLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	return s.syncPersistentStore(input, taskDigest, tensionDigest, clusterDigest)
}

func (s *AgentMemoryService) updateStatsForEventLocked(event LocalMemoryEvent) {
	s.state.Stats.TotalEvents++
	switch event.NodeType {
	case localMemoryNodeRawMessage:
		s.state.Stats.RawMessages++
	default:
		s.state.Stats.RawEvents++
	}
	s.state.Stats.LastIngestedAt = event.OccurredAt
	s.refreshDerivedStatsLocked()
}

func (s *AgentMemoryService) refreshDigestsLocked(event LocalMemoryEvent) {
	if event.TaskID != "" {
		digest := s.state.TaskDigests[event.TaskID]
		applyEventToDigest(&digest, "task", event.TaskID, event)
		s.state.TaskDigests[event.TaskID] = digest
	}
	if event.TensionID != "" {
		digest := s.state.TensionDigests[event.TensionID]
		applyEventToDigest(&digest, "tension", event.TensionID, event)
		s.state.TensionDigests[event.TensionID] = digest
	}
	clusterKey := strings.TrimSpace(event.ProtoClusterID)
	if clusterKey != "" {
		digest := s.state.ClusterDigests[clusterKey]
		applyEventToDigest(&digest, "cluster", clusterKey, event)
		s.state.ClusterDigests[clusterKey] = digest
	}
	s.refreshDerivedStatsLocked()
}

func (s *AgentMemoryService) refreshProceduresLocked() {
	if s == nil {
		return
	}
	procedures, antiProcedures := deriveLocalProcedureMaps(s.state.RecentEvents, s.constitution.MinEvidence)
	s.state.Procedures = procedures
	s.state.AntiProcedures = antiProcedures
	s.refreshDerivedStatsLocked()
}

func (s *AgentMemoryService) refreshDerivedStatsLocked() {
	if s == nil {
		return
	}
	s.state.Stats.EpisodeDigests = len(s.state.TaskDigests) + len(s.state.TensionDigests) + len(s.state.ClusterDigests)
	s.state.Stats.Procedures = len(s.state.Procedures)
	s.state.Stats.AntiProcedures = len(s.state.AntiProcedures)
}

func applyEventToDigest(digest *LocalEpisodeDigest, scopeKind, scopeKey string, event LocalMemoryEvent) {
	if digest == nil {
		return
	}
	if digest.ScopeKind == "" {
		digest.ScopeKind = scopeKind
	}
	if digest.ScopeKey == "" {
		digest.ScopeKey = scopeKey
	}
	digest.UpdatedAt = event.OccurredAt
	digest.EventCount++
	if event.NodeType == localMemoryNodeRawMessage {
		digest.MessageCount++
	}
	digest.LastSummary = firstNonEmpty(event.Summary, digest.LastSummary)
	digest.LastOutcome = firstNonEmpty(strings.TrimSpace(event.Outcome), digest.LastOutcome)
	digest.ArtifactRefs = mergeLimitedStrings(digest.ArtifactRefs, event.ArtifactRefs, 8)
	digest.DocKeys = mergeLimitedStrings(digest.DocKeys, event.DocKeys, 8)
	digest.BlockerKinds = mergeLimitedStrings(digest.BlockerKinds, event.BlockerKinds, 6)
	digest.ConstraintRefs = mergeLimitedStrings(digest.ConstraintRefs, event.ConstraintRefs, 8)
	digest.RecentSummaries = pushLimitedString(digest.RecentSummaries, event.Summary, 6)
	digest.LatestSessionID = firstNonEmpty(event.SessionID, digest.LatestSessionID)
	digest.LatestTensionID = firstNonEmpty(event.TensionID, digest.LatestTensionID)
	digest.ProtoClusterID = firstNonEmpty(event.ProtoClusterID, digest.ProtoClusterID)
	if strings.EqualFold(strings.TrimSpace(event.Outcome), "continue") {
		digest.OpenLoops = pushLimitedString(digest.OpenLoops, firstNonEmpty(event.Summary, event.Details), 4)
	}
	if event.NodeType == localMemoryNodeDissent {
		digest.DissentSummaries = pushLimitedString(digest.DissentSummaries, firstNonEmpty(event.Summary, event.Details), 4)
	}
	if event.NodeType == localMemoryNodeHandoff {
		digest.HandoffSummary = firstNonEmpty(event.Summary, event.Details, digest.HandoffSummary)
	}
}

func mergeLimitedStrings(base, extras []string, limit int) []string {
	return pushLimitedStrings(base, extras, limit)
}

func pushLimitedStrings(base, extras []string, limit int) []string {
	out := append([]string(nil), base...)
	for _, extra := range extras {
		out = pushLimitedString(out, extra, limit)
	}
	return out
}

func pushLimitedString(base []string, value string, limit int) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return base
	}
	out := append([]string(nil), base...)
	for _, existing := range out {
		if strings.EqualFold(existing, value) {
			return out
		}
	}
	out = append(out, value)
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

func (s *AgentMemoryService) queuePromotion(candidate LocalPromotionCandidate) error {
	if s == nil || s.store == nil {
		return nil
	}
	candidate.CandidateID = firstNonEmpty(strings.TrimSpace(candidate.CandidateID), newRuntimeID("local-prom"))
	candidate.CreatedAt = firstNonEmpty(strings.TrimSpace(candidate.CreatedAt), time.Now().UTC().Format(time.RFC3339Nano))
	candidate.MemoryType = strings.ToUpper(strings.TrimSpace(candidate.MemoryType))
	candidate.SourceID = strings.TrimSpace(candidate.SourceID)
	candidate.Title = strings.TrimSpace(candidate.Title)
	candidate.Body = strings.TrimSpace(candidate.Body)
	candidate.Summary = strings.TrimSpace(candidate.Summary)
	candidate.TaskID = strings.TrimSpace(candidate.TaskID)
	candidate.SessionID = strings.TrimSpace(candidate.SessionID)
	candidate.TensionID = strings.TrimSpace(candidate.TensionID)
	candidate.ProtoClusterID = strings.TrimSpace(candidate.ProtoClusterID)
	candidate.ArtifactRefs = uniqueTrimmedFocusStrings(candidate.ArtifactRefs)
	candidate.DocKeys = uniqueTrimmedFocusStrings(candidate.DocKeys)
	candidate.ConstraintRefs = uniqueTrimmedFocusStrings(candidate.ConstraintRefs)
	candidate.MemoryID = strings.TrimSpace(candidate.MemoryID)
	candidate.ClaimID = strings.TrimSpace(candidate.ClaimID)
	candidate.LastAttemptAt = strings.TrimSpace(candidate.LastAttemptAt)
	candidate.LastError = strings.TrimSpace(candidate.LastError)
	if candidate.NodeType == "" {
		candidate.NodeType = localMemoryNodeEpisodePack
	}

	if err := s.store.PutPromotion(context.Background(), candidate); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Stats.PromotionQueue++
	s.state.Stats.LastPromotionQueuedAt = candidate.CreatedAt
	s.packetCache = map[string]cachedMemoryPacket{}
	return s.saveStateLocked()
}

func (s *AgentMemoryService) statsSnapshot() LocalMemoryStats {
	if s == nil {
		return LocalMemoryStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := s.state.Stats
	stats.EpisodeDigests = len(s.state.TaskDigests) + len(s.state.TensionDigests) + len(s.state.ClusterDigests)
	stats.Procedures = len(s.state.Procedures)
	stats.AntiProcedures = len(s.state.AntiProcedures)
	if s.store != nil {
		storeStats := s.store.Stats()
		if storeStats.Episodes > stats.TotalEvents {
			stats.TotalEvents = storeStats.Episodes
		}
	}
	return stats
}

func (s *AgentMemoryService) invalidatePacketCache() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.packetCache = map[string]cachedMemoryPacket{}
	s.mu.Unlock()
}

func (s *AgentMemoryService) syncPersistentStore(event LocalMemoryEvent, taskDigest, tensionDigest, clusterDigest LocalEpisodeDigest) error {
	if s == nil || s.store == nil {
		return nil
	}
	s.mu.Lock()
	docVersions := cloneStringMap(s.state.DocVersions)
	artifactVersions := cloneStringMap(s.state.ArtifactVersions)
	s.mu.Unlock()
	episode := LocalMemoryEpisodeRecord{
		EpisodeID: fmt.Sprintf("event-%d", event.Sequence),
		Scope: LocalMemoryScope{
			TaskID:         strings.TrimSpace(event.TaskID),
			SessionID:      strings.TrimSpace(event.SessionID),
			TensionID:      strings.TrimSpace(event.TensionID),
			ProtoClusterID: strings.TrimSpace(event.ProtoClusterID),
		},
		RunID:          strings.TrimSpace(event.SourceID),
		Trigger:        strings.TrimSpace(event.EventKind),
		Outcome:        strings.TrimSpace(event.Outcome),
		Summary:        firstNonEmpty(event.Summary, event.Details),
		DocKeys:        append([]string(nil), event.DocKeys...),
		ArtifactRefs:   append([]string(nil), event.ArtifactRefs...),
		ConstraintRefs: append([]string(nil), event.ConstraintRefs...),
		SegmentRefs:    append([]string(nil), event.SegmentRefs...),
		Meta:           localMemoryEpisodeMetaFromEvent(event),
		CreatedAt:      strings.TrimSpace(event.OccurredAt),
	}
	if err := s.store.UpsertEpisode(episode); err != nil {
		return err
	}
	if strings.TrimSpace(event.TaskID) != "" && taskDigest.ScopeKey != "" {
		if err := s.store.PutDigest(buildStoreDigestRecord("task:"+strings.TrimSpace(event.TaskID), "TASK_DIGEST", taskDigest, event, docVersions, artifactVersions)); err != nil {
			return err
		}
	}
	if strings.TrimSpace(event.TensionID) != "" && tensionDigest.ScopeKey != "" {
		if err := s.store.PutDigest(buildStoreDigestRecord("tension:"+strings.TrimSpace(event.TensionID), "TENSION_DIGEST", tensionDigest, event, docVersions, artifactVersions)); err != nil {
			return err
		}
	}
	clusterKey := strings.TrimSpace(event.ProtoClusterID)
	if clusterKey != "" && clusterDigest.ScopeKey != "" {
		if err := s.store.PutDigest(buildStoreDigestRecord("cluster:"+clusterKey, "CLUSTER_DIGEST", clusterDigest, event, docVersions, artifactVersions)); err != nil {
			return err
		}
	}
	return nil
}

func buildStoreDigestRecord(id, kind string, digest LocalEpisodeDigest, event LocalMemoryEvent, docVersions, artifactVersions map[string]string) LocalMemoryDigestRecord {
	digestCopy := digest
	return LocalMemoryDigestRecord{
		DigestID:        id,
		Tier:            "P2",
		Kind:            kind,
		Scope:           LocalMemoryScope{TaskID: event.TaskID, SessionID: event.SessionID, TensionID: event.TensionID, ProtoClusterID: firstNonEmpty(event.ProtoClusterID, digest.ProtoClusterID)},
		Visibility:      "PRIVATE",
		Summary:         firstNonEmpty(digest.LastSummary, event.Summary),
		Body:            renderLocalEpisodeDigest(digest),
		SourceEpisodeID: fmt.Sprintf("event-%d", event.Sequence),
		EpisodeDigest:   &digestCopy,
		DocKeys:         append([]string(nil), digest.DocKeys...),
		ArtifactRefs:    append([]string(nil), digest.ArtifactRefs...),
		ConstraintRefs:  append([]string(nil), digest.ConstraintRefs...),
		Guards:          buildLocalMemoryGuards(digest, docVersions, artifactVersions),
		Meta: map[string]string{
			"scope_kind": strings.TrimSpace(digest.ScopeKind),
			"scope_key":  strings.TrimSpace(digest.ScopeKey),
		},
	}
}

func renderLocalEpisodeDigest(digest LocalEpisodeDigest) string {
	lines := []string{
		fmt.Sprintf("events=%d", digest.EventCount),
		fmt.Sprintf("messages=%d", digest.MessageCount),
	}
	if digest.LastSummary != "" {
		lines = append(lines, "last_summary="+oneLine(digest.LastSummary))
	}
	if len(digest.OpenLoops) > 0 {
		lines = append(lines, "open_loops="+strings.Join(digest.OpenLoops, " | "))
	}
	if len(digest.DissentSummaries) > 0 {
		lines = append(lines, "dissent="+strings.Join(digest.DissentSummaries, " | "))
	}
	if len(digest.BlockerKinds) > 0 {
		lines = append(lines, "blockers="+strings.Join(digest.BlockerKinds, ", "))
	}
	return strings.Join(lines, "\n")
}

func buildLocalMemoryGuards(digest LocalEpisodeDigest, docVersions, artifactVersions map[string]string) []LocalMemoryGuard {
	guards := make([]LocalMemoryGuard, 0, len(digest.DocKeys)*2+len(digest.ArtifactRefs))
	for _, key := range digest.DocKeys {
		docKey := strings.TrimSpace(key)
		if docKey == "" {
			continue
		}
		guards = append(guards, LocalMemoryGuard{
			GuardType: "doc_key",
			Ref:       docKey,
		})
		if version := strings.TrimSpace(docVersions[docKey]); version != "" {
			guards = append(guards, LocalMemoryGuard{
				GuardType: "doc_sha",
				Ref:       docKey,
				Version:   version,
			})
		}
	}
	for _, ref := range digest.ArtifactRefs {
		artifactRef := strings.TrimSpace(ref)
		if artifactRef == "" {
			continue
		}
		if version := strings.TrimSpace(artifactVersions[artifactRef]); version != "" {
			guards = append(guards, LocalMemoryGuard{
				GuardType: "artifact_version",
				Ref:       artifactRef,
				Version:   version,
			})
		}
	}
	return guards
}

func (s *AgentMemoryService) invalidate(input LocalMemoryInvalidationInput) error {
	if s == nil {
		return nil
	}
	s.invalidatePacketCache()
	if s.store == nil {
		return nil
	}
	if _, err := s.store.Invalidate(input); err != nil {
		return err
	}
	return s.reconcileShadowFromStore(true)
}

func (s *AgentMemoryService) storeStatsSnapshot() LocalMemoryStoreStats {
	if s == nil || s.store == nil {
		return LocalMemoryStoreStats{}
	}
	return s.store.Stats()
}

func (s *AgentMemoryService) markPromotionsPromotedBySource(sourceID string) error {
	if s == nil || s.store == nil {
		return nil
	}
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return nil
	}

	pending, err := s.store.PendingPromotions(context.Background(), 1000)
	if err != nil {
		return err
	}
	changed := 0
	for _, p := range pending {
		if strings.TrimSpace(p.SourceID) == sourceID {
			if err := s.store.SetPromotionStatus(context.Background(), p.CandidateID, p.MemoryID, p.ClaimID, "promoted", nil, false); err == nil {
				changed++
			}
		}
	}
	if changed == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Stats.PromotionQueue >= changed {
		s.state.Stats.PromotionQueue -= changed
	} else {
		s.state.Stats.PromotionQueue = 0
	}
	return s.saveStateLocked()
}

func (s *AgentMemoryService) pendingPromotions(limit int) []LocalPromotionCandidate {
	if s == nil || s.store == nil {
		return nil
	}
	promotions, _ := s.store.PendingPromotions(context.Background(), limit)
	return promotions
}

func clonePromotionCandidate(item LocalPromotionCandidate) LocalPromotionCandidate {
	copy := item
	copy.ArtifactRefs = append([]string(nil), item.ArtifactRefs...)
	copy.DocKeys = append([]string(nil), item.DocKeys...)
	copy.ConstraintRefs = append([]string(nil), item.ConstraintRefs...)
	return copy
}

func (s *AgentMemoryService) recordPromotionAttempt(candidateID, memoryID, claimID string) error {
	return s.applyPromotionSyncState(candidateID, memoryID, claimID, "", false, true)
}

func (s *AgentMemoryService) markPromotionFailed(candidateID, memoryID, claimID string, cause error) error {
	message := ""
	if cause != nil {
		message = strings.TrimSpace(cause.Error())
	}
	return s.applyPromotionSyncState(candidateID, memoryID, claimID, message, false, false)
}

func (s *AgentMemoryService) markPromotionPromoted(candidateID, memoryID, claimID string) error {
	return s.applyPromotionSyncState(candidateID, memoryID, claimID, "", true, false)
}

func (s *AgentMemoryService) markPromotionSkipped(candidateID, memoryID, claimID, reason string) error {
	if s == nil || s.store == nil {
		return nil
	}
	candidateID = strings.TrimSpace(candidateID)
	if candidateID == "" {
		return nil
	}
	reason = strings.TrimSpace(reason)
	var errInfo error
	if reason != "" {
		errInfo = fmt.Errorf("%s", reason)
	}
	if err := s.store.SetPromotionStatus(context.Background(), candidateID, memoryID, claimID, localMemoryWriteStateSkipped, errInfo, false); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Stats.PromotionQueue > 0 {
		s.state.Stats.PromotionQueue--
	}
	return s.saveStateLocked()
}

func (s *AgentMemoryService) applyPromotionSyncState(candidateID, memoryID, claimID, lastError string, promoted bool, attemptOnly bool) error {
	if s == nil || s.store == nil {
		return nil
	}
	candidateID = strings.TrimSpace(candidateID)
	if candidateID == "" {
		return nil
	}

	status := "pending"
	if promoted {
		status = "promoted"
	}

	var errInfo error
	if lastError != "" {
		errInfo = fmt.Errorf("%s", lastError)
	}

	if err := s.store.SetPromotionStatus(context.Background(), candidateID, memoryID, claimID, status, errInfo, attemptOnly); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if attemptOnly {
		s.state.Stats.PromotionAttempts++
		s.state.Stats.LastPromotionAttemptAt = now
	} else if promoted {
		s.state.Stats.LastPromotionSyncedAt = now
		if s.state.Stats.PromotionQueue > 0 {
			s.state.Stats.PromotionQueue--
		}
	} else {
		s.state.Stats.PromotionFailures++
	}
	return s.saveStateLocked()
}

func (s *AgentMemoryService) markDigestAccesses(taskID, clusterID, tensionID string, at time.Time) {
	if s == nil || s.store == nil {
		return
	}
	taskID = strings.TrimSpace(taskID)
	clusterID = strings.TrimSpace(clusterID)
	tensionID = strings.TrimSpace(tensionID)
	if taskID != "" {
		if err := s.store.MarkDigestAccessed("task:"+taskID, at); err != nil && !strings.Contains(strings.ToLower(err.Error()), "not found") {
			return
		}
	}
	if tensionID != "" {
		if err := s.store.MarkDigestAccessed("tension:"+tensionID, at); err != nil && !strings.Contains(strings.ToLower(err.Error()), "not found") {
			return
		}
	}
	if clusterID != "" {
		_ = s.store.MarkDigestAccessed("cluster:"+clusterID, at)
	}
}

func (s *AgentMemoryService) SearchMemory(ctx context.Context, query string, limit int) ([]LocalMemoryDigestRecord, []LocalMemoryEpisodeRecord, error) {
	if s == nil || s.store == nil {
		return nil, nil, fmt.Errorf("local memory not initialized")
	}
	return s.store.Search(ctx, query, limit)
}
