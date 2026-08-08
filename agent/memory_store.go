package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const localMemoryStateVersion = 1

type LocalMemoryScope struct {
	TaskID         string `json:"task_id,omitempty"`
	SessionID      string `json:"session_id,omitempty"`
	TensionID      string `json:"tension_id,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
}

type LocalMemoryGuard struct {
	GuardType string `json:"guard_type"`
	Ref       string `json:"ref"`
	Version   string `json:"version,omitempty"`
}

type LocalMemoryEpisodeRecord struct {
	EpisodeID           string            `json:"episode_id"`
	Scope               LocalMemoryScope  `json:"scope,omitempty"`
	ClaimModality       string            `json:"claim_modality,omitempty"`
	WriteState          string            `json:"write_state,omitempty"`
	AnchorsJSON         string            `json:"anchors_json,omitempty"`
	RunID               string            `json:"run_id,omitempty"`
	Trigger             string            `json:"trigger,omitempty"`
	Outcome             string            `json:"outcome,omitempty"`
	Summary             string            `json:"summary,omitempty"`
	DigestRefs          []string          `json:"digest_refs,omitempty"`
	DocKeys             []string          `json:"doc_keys,omitempty"`
	ArtifactRefs        []string          `json:"artifact_refs,omitempty"`
	ConstraintRefs      []string          `json:"constraint_refs,omitempty"`
	SegmentRefs         []string          `json:"segment_refs,omitempty"`
	Tags                []string          `json:"tags,omitempty"`
	Meta                map[string]string `json:"meta,omitempty"`
	CreatedAt           string            `json:"created_at,omitempty"`
	SupersededAt        string            `json:"superseded_at,omitempty"`
	InvalidationReasons []string          `json:"invalidation_reasons,omitempty"`
}

type LocalMemoryDigestRecord struct {
	DigestID            string              `json:"digest_id"`
	Tier                string              `json:"tier"`
	Kind                string              `json:"kind"`
	Scope               LocalMemoryScope    `json:"scope,omitempty"`
	ClaimModality       string              `json:"claim_modality,omitempty"`
	WriteState          string              `json:"write_state,omitempty"`
	AnchorsJSON         string              `json:"anchors_json,omitempty"`
	Visibility          string              `json:"visibility,omitempty"`
	Summary             string              `json:"summary,omitempty"`
	Body                string              `json:"body,omitempty"`
	SourceEpisodeID     string              `json:"source_episode_id,omitempty"`
	EpisodeDigest       *LocalEpisodeDigest `json:"episode_digest,omitempty"`
	DocKeys             []string            `json:"doc_keys,omitempty"`
	ArtifactRefs        []string            `json:"artifact_refs,omitempty"`
	ConstraintRefs      []string            `json:"constraint_refs,omitempty"`
	SegmentRefs         []string            `json:"segment_refs,omitempty"`
	Guards              []LocalMemoryGuard  `json:"guards,omitempty"`
	Tags                []string            `json:"tags,omitempty"`
	Meta                map[string]string   `json:"meta,omitempty"`
	CreatedAt           string              `json:"created_at,omitempty"`
	UpdatedAt           string              `json:"updated_at,omitempty"`
	ExpiresAt           string              `json:"expires_at,omitempty"`
	LastAccessedAt      string              `json:"last_accessed_at,omitempty"`
	Stale               bool                `json:"stale,omitempty"`
	InvalidatedAt       string              `json:"invalidated_at,omitempty"`
	InvalidationReasons []string            `json:"invalidation_reasons,omitempty"`
}

type LocalMemoryState struct {
	Version     int                        `json:"version"`
	WorkspaceID string                     `json:"workspace_id"`
	AgentID     string                     `json:"agent_id"`
	UpdatedAt   string                     `json:"updated_at,omitempty"`
	Episodes    []LocalMemoryEpisodeRecord `json:"episodes"`
	Digests     []LocalMemoryDigestRecord  `json:"digests"`
}

type LocalMemoryStoreStats struct {
	Episodes     int    `json:"episodes"`
	Digests      int    `json:"digests"`
	P1Digests    int    `json:"p1_digests"`
	P2Digests    int    `json:"p2_digests"`
	StaleDigests int    `json:"stale_digests"`
	UpdatedAt    string `json:"updated_at,omitempty"`
	Revision     int64  `json:"revision,omitempty"`
}

type LocalMemoryPacketQuery struct {
	Scope          LocalMemoryScope `json:"scope,omitempty"`
	DocKeys        []string         `json:"doc_keys,omitempty"`
	ArtifactRefs   []string         `json:"artifact_refs,omitempty"`
	ConstraintRefs []string         `json:"constraint_refs,omitempty"`
	SegmentRefs    []string         `json:"segment_refs,omitempty"`
	RecentLimit    int              `json:"recent_limit,omitempty"`
}

type LocalMemoryPacketView struct {
	Episodes       []LocalMemoryEpisodeRecord `json:"episodes,omitempty"`
	TaskDigest     *LocalMemoryDigestRecord   `json:"task_digest,omitempty"`
	TensionDigest  *LocalMemoryDigestRecord   `json:"tension_digest,omitempty"`
	ClusterDigest  *LocalMemoryDigestRecord   `json:"cluster_digest,omitempty"`
	Matched        bool                       `json:"matched,omitempty"`
	StaleDigestIDs []string                   `json:"stale_digest_ids,omitempty"`
	UpdatedAt      string                     `json:"updated_at,omitempty"`
}

type LocalMemoryTTLConfig struct {
	MaxEpisodesPerTask    int           `json:"max_episodes_per_task"`
	MaxEpisodesPerSession int           `json:"max_episodes_per_session"`
	DigestTTL             time.Duration `json:"digest_ttl"`
	StaleColdTTL          time.Duration `json:"stale_cold_ttl"`
}

func DefaultLocalMemoryTTLConfig() LocalMemoryTTLConfig {
	return LocalMemoryTTLConfig{
		MaxEpisodesPerTask:    50,
		MaxEpisodesPerSession: 100,
		DigestTTL:             30 * 24 * time.Hour,
		StaleColdTTL:          7 * 24 * time.Hour,
	}
}

type localMemoryRecordSelectorIndex struct {
	taskIDs         map[string]map[string]struct{}
	sessionIDs      map[string]map[string]struct{}
	tensionIDs      map[string]map[string]struct{}
	protoClusterIDs map[string]map[string]struct{}
	docKeys         map[string]map[string]struct{}
	artifactRefs    map[string]map[string]struct{}
	constraintRefs  map[string]map[string]struct{}
	segmentRefs     map[string]map[string]struct{}
}

type LocalMemoryStore struct {
	mu           sync.Mutex
	path         string
	legacyPath   string
	db           *sql.DB
	state        LocalMemoryState
	revision     int64
	episodeIndex map[string]int
	digestIndex  map[string]int
	episodeRefs  localMemoryRecordSelectorIndex
	digestRefs   localMemoryRecordSelectorIndex
}

func OpenLocalMemoryStore(workspaceID, agentID string) (*LocalMemoryStore, error) {
	if localMemoryStoreRootPath(workspaceID, agentID) == "" {
		return nil, fmt.Errorf("agent config root is unavailable")
	}
	return openSQLiteBackedLocalMemoryStore(workspaceID, agentID)
}

func localMemoryStoreRootPath(workspaceID, agentID string) string {
	root := agentRuntimeConfigRoot()
	if root == "" {
		return ""
	}
	workspacePart := sanitizePathComponent(firstNonEmpty(workspaceID, "workspace"))
	agentPart := sanitizePathComponent(firstNonEmpty(agentID, "agent"))
	return filepath.Join(root, "memory", workspacePart, agentPart)
}

func localMemoryStorePath(workspaceID, agentID string) string {
	root := localMemoryStoreRootPath(workspaceID, agentID)
	if root == "" {
		return ""
	}
	return filepath.Join(root, "state.db")
}

func localMemoryLegacyStorePath(workspaceID, agentID string) string {
	root := localMemoryStoreRootPath(workspaceID, agentID)
	if root == "" {
		return ""
	}
	return filepath.Join(root, "state.json")
}

func quarantineCorruptLocalMemoryFile(path string, data []byte) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("local memory path is empty")
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

func (s *LocalMemoryStore) Snapshot() LocalMemoryState {
	if s == nil {
		return LocalMemoryState{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneLocalMemoryState(s.state)
}

func (s *LocalMemoryStore) ReplaceState(state LocalMemoryState) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = normalizeLocalMemoryState(cloneLocalMemoryState(state), time.Time{})
	if s.state.Version == 0 {
		s.state.Version = localMemoryStateVersion
	}
	if s.state.WorkspaceID == "" {
		s.state.WorkspaceID = strings.TrimSpace(state.WorkspaceID)
	}
	if s.state.AgentID == "" {
		s.state.AgentID = strings.TrimSpace(state.AgentID)
	}
	s.reindexLocked()
	return s.saveLocked(time.Now().UTC())
}

func (s *LocalMemoryStore) Stats() LocalMemoryStoreStats {
	if s == nil {
		return LocalMemoryStoreStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := LocalMemoryStoreStats{
		Episodes:  len(s.state.Episodes),
		Digests:   len(s.state.Digests),
		UpdatedAt: strings.TrimSpace(s.state.UpdatedAt),
		Revision:  s.revision,
	}
	for _, digest := range s.state.Digests {
		switch strings.ToUpper(strings.TrimSpace(digest.Tier)) {
		case "P1":
			stats.P1Digests++
		default:
			stats.P2Digests++
		}
		if digest.Stale {
			stats.StaleDigests++
		}
	}
	return stats
}

func (s *LocalMemoryStore) UpsertEpisode(record LocalMemoryEpisodeRecord) error {
	if s == nil {
		return nil
	}
	now := time.Now().UTC()
	record = normalizeLocalMemoryEpisodeRecord(record, now)
	if record.EpisodeID == "" {
		return fmt.Errorf("episode_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if idx, ok := s.episodeIndex[record.EpisodeID]; ok {
		existing := s.state.Episodes[idx]
		record.CreatedAt = firstNonEmpty(record.CreatedAt, existing.CreatedAt)
		record.SupersededAt = firstNonEmpty(record.SupersededAt, existing.SupersededAt)
		record.InvalidationReasons = mergeUniqueMemoryStrings(existing.InvalidationReasons, record.InvalidationReasons)
		s.state.Episodes[idx] = record
	} else {
		s.state.Episodes = append(s.state.Episodes, record)
		s.episodeIndex[record.EpisodeID] = len(s.state.Episodes) - 1
	}
	s.reindexLocked()
	return s.saveLocked(now)
}

func (s *LocalMemoryStore) PutDigest(record LocalMemoryDigestRecord) error {
	if s == nil {
		return nil
	}
	now := time.Now().UTC()
	record = normalizeLocalMemoryDigestRecord(record, now)
	if record.DigestID == "" {
		return fmt.Errorf("digest_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if idx, ok := s.digestIndex[record.DigestID]; ok {
		existing := s.state.Digests[idx]
		record.CreatedAt = firstNonEmpty(record.CreatedAt, existing.CreatedAt)
		record.LastAccessedAt = firstNonEmpty(record.LastAccessedAt, existing.LastAccessedAt)
		record.InvalidatedAt = firstNonEmpty(record.InvalidatedAt, existing.InvalidatedAt)
		record.Stale = record.Stale || existing.Stale
		record.InvalidationReasons = mergeUniqueMemoryStrings(existing.InvalidationReasons, record.InvalidationReasons)
		s.state.Digests[idx] = record
	} else {
		s.state.Digests = append(s.state.Digests, record)
		s.digestIndex[record.DigestID] = len(s.state.Digests) - 1
	}
	s.reindexLocked()
	return s.saveLocked(now)
}

func (s *LocalMemoryStore) MarkDigestAccessed(digestID string, at time.Time) error {
	if s == nil {
		return nil
	}
	digestID = strings.TrimSpace(digestID)
	if digestID == "" {
		return fmt.Errorf("digest_id is required")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.digestIndex[digestID]
	if !ok {
		return fmt.Errorf("digest %s not found", digestID)
	}
	digest := s.state.Digests[idx]
	digest.LastAccessedAt = at.UTC().Format(time.RFC3339Nano)
	digest.UpdatedAt = digest.LastAccessedAt
	s.state.Digests[idx] = digest
	return s.saveLocked(at)
}

func (s *LocalMemoryStore) Prune(now time.Time, cfg LocalMemoryTTLConfig) (int, int, error) {
	if s == nil {
		return 0, 0, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var prunedDigests, prunedEpisodes int
	activeDigests := make([]LocalMemoryDigestRecord, 0, len(s.state.Digests))

	for _, digest := range s.state.Digests {
		expired := false
		if strings.TrimSpace(digest.ExpiresAt) != "" {
			expTime, err := time.Parse(time.RFC3339Nano, digest.ExpiresAt)
			if err == nil && now.After(expTime) {
				expired = true
			}
		}

		if !expired && digest.Stale {
			accessed := digest.LastAccessedAt
			if accessed == "" {
				accessed = digest.UpdatedAt
			}
			if accessed == "" {
				accessed = digest.CreatedAt
			}
			if accessed != "" {
				accTime, err := time.Parse(time.RFC3339Nano, accessed)
				if err == nil && now.Sub(accTime) > cfg.StaleColdTTL {
					expired = true
				}
			} else {
				expired = true
			}
		}

		if expired {
			prunedDigests++
		} else {
			activeDigests = append(activeDigests, digest)
		}
	}

	type epKey struct {
		kind string
		id   string
	}
	epCounts := make(map[epKey]int)

	sortedEpisodes := append([]LocalMemoryEpisodeRecord(nil), s.state.Episodes...)
	sort.SliceStable(sortedEpisodes, func(i, j int) bool {
		return compareLocalMemoryTimestamps(sortedEpisodes[i].CreatedAt, sortedEpisodes[j].CreatedAt) > 0
	})

	activeEpisodesMap := make(map[string]struct{}, len(sortedEpisodes))
	for _, ep := range sortedEpisodes {
		keep := true

		if taskID := ep.Scope.TaskID; taskID != "" && cfg.MaxEpisodesPerTask > 0 {
			k := epKey{"task", taskID}
			if epCounts[k] >= cfg.MaxEpisodesPerTask {
				keep = false
			} else {
				epCounts[k]++
			}
		}

		if keep && ep.Scope.SessionID != "" && cfg.MaxEpisodesPerSession > 0 {
			k := epKey{"session", ep.Scope.SessionID}
			if epCounts[k] >= cfg.MaxEpisodesPerSession {
				keep = false
			} else {
				epCounts[k]++
			}
		}

		if keep {
			activeEpisodesMap[ep.EpisodeID] = struct{}{}
		} else {
			prunedEpisodes++
		}
	}

	if prunedDigests == 0 && prunedEpisodes == 0 {
		return 0, 0, nil
	}

	activeEpisodes := make([]LocalMemoryEpisodeRecord, 0, len(s.state.Episodes)-prunedEpisodes)
	for _, ep := range s.state.Episodes {
		if _, ok := activeEpisodesMap[ep.EpisodeID]; ok {
			activeEpisodes = append(activeEpisodes, ep)
		}
	}

	s.state.Digests = activeDigests
	s.state.Episodes = activeEpisodes
	s.reindexLocked()
	err := s.saveLocked(now)
	return prunedDigests, prunedEpisodes, err
}

func (s *LocalMemoryStore) ReadPacketView(query LocalMemoryPacketQuery) (LocalMemoryPacketView, error) {
	if s == nil {
		return LocalMemoryPacketView{}, nil
	}
	normalized := normalizeLocalMemoryPacketQuery(query)
	now := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()

	view := LocalMemoryPacketView{
		UpdatedAt: strings.TrimSpace(s.state.UpdatedAt),
	}
	accessedIDs := make([]string, 0, 3)
	var taskQuality localMemoryMatchQuality
	var tensionQuality localMemoryMatchQuality
	var clusterQuality localMemoryMatchQuality

	digestIDs := s.candidateDigestIDsLocked(normalized)
	if len(digestIDs) == 0 && !normalized.hasSelectors() {
		digestIDs = make([]string, 0, len(s.state.Digests))
		for _, digest := range s.state.Digests {
			if id := strings.TrimSpace(digest.DigestID); id != "" {
				digestIDs = append(digestIDs, id)
			}
		}
	}
	for _, digestID := range digestIDs {
		idx, ok := s.digestIndex[digestID]
		if !ok {
			continue
		}
		digest := s.state.Digests[idx]
		quality := normalized.qualityForDigest(digest)
		if !quality.matched {
			continue
		}
		if digest.Stale {
			view.StaleDigestIDs = append(view.StaleDigestIDs, digest.DigestID)
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(digest.Kind)) {
		case "TASK_DIGEST":
			view.TaskDigest, taskQuality = chooseBestMatchedDigestRecord(view.TaskDigest, taskQuality, digest, quality)
		case "TENSION_DIGEST":
			view.TensionDigest, tensionQuality = chooseBestMatchedDigestRecord(view.TensionDigest, tensionQuality, digest, quality)
		case "CLUSTER_DIGEST":
			view.ClusterDigest, clusterQuality = chooseBestMatchedDigestRecord(view.ClusterDigest, clusterQuality, digest, quality)
		}
	}

	for _, digest := range []*LocalMemoryDigestRecord{view.TaskDigest, view.TensionDigest, view.ClusterDigest} {
		if digest == nil {
			continue
		}
		view.Matched = true
		accessedIDs = append(accessedIDs, strings.TrimSpace(digest.DigestID))
	}

	episodeIDs := s.candidateEpisodeIDsLocked(normalized)
	if len(episodeIDs) == 0 && !normalized.hasSelectors() {
		episodeIDs = make([]string, 0, len(s.state.Episodes))
		for _, episode := range s.state.Episodes {
			if id := strings.TrimSpace(episode.EpisodeID); id != "" {
				episodeIDs = append(episodeIDs, id)
			}
		}
	}
	type localMemoryEpisodeCandidate struct {
		record  LocalMemoryEpisodeRecord
		quality localMemoryMatchQuality
	}
	candidates := make([]localMemoryEpisodeCandidate, 0, min(len(episodeIDs), normalized.recentLimit))
	bestQuality := localMemoryMatchQuality{}
	for _, episodeID := range episodeIDs {
		idx, ok := s.episodeIndex[episodeID]
		if !ok {
			continue
		}
		episode := s.state.Episodes[idx]
		quality := normalized.qualityForEpisode(episode)
		if !quality.matched {
			continue
		}
		if strings.TrimSpace(episode.SupersededAt) != "" {
			continue
		}
		if quality.betterThan(bestQuality) {
			bestQuality = quality
		}
		candidates = append(candidates, localMemoryEpisodeCandidate{
			record:  cloneEpisodeRecord(episode),
			quality: quality,
		})
	}
	for _, candidate := range candidates {
		if !sameLocalMemoryMatchBand(candidate.quality, bestQuality) {
			continue
		}
		view.Episodes = append(view.Episodes, candidate.record)
	}
	if len(view.Episodes) > 0 {
		view.Matched = true
		sort.SliceStable(view.Episodes, func(i, j int) bool {
			return compareLocalMemoryTimestamps(view.Episodes[i].CreatedAt, view.Episodes[j].CreatedAt) < 0
		})
		if normalized.recentLimit > 0 && len(view.Episodes) > normalized.recentLimit {
			view.Episodes = append([]LocalMemoryEpisodeRecord(nil), view.Episodes[len(view.Episodes)-normalized.recentLimit:]...)
		}
	}

	if len(accessedIDs) > 0 {
		if err := s.recordDigestAccessesLocked(uniqueTrimmedMemoryStrings(accessedIDs), now); err != nil {
			return LocalMemoryPacketView{}, err
		}
		view.UpdatedAt = strings.TrimSpace(s.state.UpdatedAt)
	}

	view.StaleDigestIDs = uniqueTrimmedMemoryStrings(view.StaleDigestIDs)
	sort.Strings(view.StaleDigestIDs)
	return view, nil
}

type localMemoryPacketQueryNormalized struct {
	scope          LocalMemoryScope
	docKeys        map[string]struct{}
	artifactRefs   map[string]struct{}
	constraintRefs map[string]struct{}
	segmentRefs    map[string]struct{}
	recentLimit    int
}

type localMemoryMatchQuality struct {
	matched      bool
	exactScope   bool
	scopeMatches int
	refMatches   int
	conflicts    int
}

func normalizeLocalMemoryPacketQuery(query LocalMemoryPacketQuery) localMemoryPacketQueryNormalized {
	limit := query.RecentLimit
	if limit <= 0 {
		limit = 8
	}
	return localMemoryPacketQueryNormalized{
		scope:          normalizeLocalMemoryScope(query.Scope),
		docKeys:        localMemorySet(query.DocKeys),
		artifactRefs:   localMemorySet(query.ArtifactRefs),
		constraintRefs: localMemorySet(query.ConstraintRefs),
		segmentRefs:    localMemorySet(query.SegmentRefs),
		recentLimit:    limit,
	}
}

func (q localMemoryPacketQueryNormalized) hasSelectors() bool {
	return q.hasScopeAnchors() || q.hasRefSelectors()
}

func (q localMemoryPacketQueryNormalized) hasScopeAnchors() bool {
	return q.scope.TaskID != "" ||
		q.scope.SessionID != "" ||
		q.scope.TensionID != "" ||
		q.scope.ProtoClusterID != ""
}

func (q localMemoryPacketQueryNormalized) hasRefSelectors() bool {
	return len(q.docKeys) > 0 ||
		len(q.artifactRefs) > 0 ||
		len(q.constraintRefs) > 0 ||
		len(q.segmentRefs) > 0
}

func (q localMemoryPacketQueryNormalized) matchesEpisode(record LocalMemoryEpisodeRecord) bool {
	return q.qualityForEpisode(record).matched
}

func (q localMemoryPacketQueryNormalized) matchesDigest(record LocalMemoryDigestRecord) bool {
	if !q.hasSelectors() {
		return true
	}
	return q.qualityForDigest(record).matched
}

func (q localMemoryPacketQueryNormalized) qualityForEpisode(record LocalMemoryEpisodeRecord) localMemoryMatchQuality {
	if !q.hasSelectors() {
		return localMemoryMatchQuality{matched: true}
	}
	quality := q.scopeMatchQuality(record.Scope)
	quality.refMatches += q.countRefMatches(record.DocKeys, q.docKeys)
	quality.refMatches += q.countRefMatches(record.ArtifactRefs, q.artifactRefs)
	quality.refMatches += q.countRefMatches(record.ConstraintRefs, q.constraintRefs)
	quality.refMatches += q.countRefMatches(record.SegmentRefs, q.segmentRefs)
	if quality.conflicts > 0 && quality.scopeMatches == 0 {
		return localMemoryMatchQuality{}
	}
	if quality.scopeMatches > 0 || quality.refMatches > 0 {
		quality.matched = true
	}
	return quality
}

func (q localMemoryPacketQueryNormalized) qualityForDigest(record LocalMemoryDigestRecord) localMemoryMatchQuality {
	if !q.hasSelectors() {
		return localMemoryMatchQuality{matched: true}
	}
	quality := q.scopeMatchQuality(record.Scope)
	quality.refMatches += q.countRefMatches(record.DocKeys, q.docKeys)
	quality.refMatches += q.countRefMatches(record.ArtifactRefs, q.artifactRefs)
	quality.refMatches += q.countRefMatches(record.ConstraintRefs, q.constraintRefs)
	quality.refMatches += q.countRefMatches(record.SegmentRefs, q.segmentRefs)
	if quality.conflicts > 0 && quality.scopeMatches == 0 {
		return localMemoryMatchQuality{}
	}
	if quality.scopeMatches > 0 || quality.refMatches > 0 {
		quality.matched = true
	}
	return quality
}

func (q localMemoryPacketQueryNormalized) scopeMatchQuality(scope LocalMemoryScope) localMemoryMatchQuality {
	quality := localMemoryMatchQuality{}
	selected := 0
	matchField := func(expected, actual string) {
		expected = strings.TrimSpace(expected)
		actual = strings.TrimSpace(actual)
		if expected == "" {
			return
		}
		selected++
		if actual == "" {
			return
		}
		if actual == expected {
			quality.scopeMatches++
			return
		}
		quality.conflicts++
	}
	matchField(q.scope.TaskID, scope.TaskID)
	matchField(q.scope.SessionID, scope.SessionID)
	matchField(q.scope.TensionID, scope.TensionID)
	matchField(q.scope.ProtoClusterID, scope.ProtoClusterID)
	quality.exactScope = selected > 0 && quality.scopeMatches == selected && quality.conflicts == 0
	return quality
}

func (q localMemoryPacketQueryNormalized) countRefMatches(values []string, selector map[string]struct{}) int {
	if len(selector) == 0 || len(values) == 0 {
		return 0
	}
	matches := 0
	for _, value := range values {
		if _, ok := selector[strings.TrimSpace(value)]; ok {
			matches++
		}
	}
	return matches
}

func (q localMemoryMatchQuality) betterThan(other localMemoryMatchQuality) bool {
	if !q.matched {
		return false
	}
	if !other.matched {
		return true
	}
	switch {
	case q.exactScope != other.exactScope:
		return q.exactScope
	case q.conflicts != other.conflicts:
		return q.conflicts < other.conflicts
	case q.scopeMatches != other.scopeMatches:
		return q.scopeMatches > other.scopeMatches
	case q.refMatches != other.refMatches:
		return q.refMatches > other.refMatches
	default:
		return false
	}
}

func sameLocalMemoryMatchBand(left, right localMemoryMatchQuality) bool {
	if !left.matched || !right.matched {
		return false
	}
	if left.exactScope != right.exactScope || left.conflicts != right.conflicts || left.scopeMatches != right.scopeMatches {
		return false
	}
	if left.scopeMatches == 0 {
		return left.refMatches == right.refMatches
	}
	return true
}

func chooseBestMatchedDigestRecord(current *LocalMemoryDigestRecord, currentQuality localMemoryMatchQuality, candidate LocalMemoryDigestRecord, candidateQuality localMemoryMatchQuality) (*LocalMemoryDigestRecord, localMemoryMatchQuality) {
	if current == nil {
		copy := cloneDigestRecord(candidate)
		return &copy, candidateQuality
	}
	if candidateQuality.betterThan(currentQuality) {
		copy := cloneDigestRecord(candidate)
		return &copy, candidateQuality
	}
	if currentQuality.betterThan(candidateQuality) {
		return current, currentQuality
	}
	return chooseFreshestDigestRecord(current, currentQuality, candidate, candidateQuality)
}

func chooseFreshestDigestRecord(current *LocalMemoryDigestRecord, currentQuality localMemoryMatchQuality, candidate LocalMemoryDigestRecord, candidateQuality localMemoryMatchQuality) (*LocalMemoryDigestRecord, localMemoryMatchQuality) {
	if current == nil {
		copy := cloneDigestRecord(candidate)
		return &copy, candidateQuality
	}
	if compareLocalMemoryTimestamps(candidate.UpdatedAt, current.UpdatedAt) > 0 {
		copy := cloneDigestRecord(candidate)
		return &copy, candidateQuality
	}
	if compareLocalMemoryTimestamps(candidate.UpdatedAt, current.UpdatedAt) == 0 && compareLocalMemoryTimestamps(candidate.CreatedAt, current.CreatedAt) > 0 {
		copy := cloneDigestRecord(candidate)
		return &copy, candidateQuality
	}
	return current, currentQuality
}

func compareLocalMemoryTimestamps(left, right string) int {
	leftTime := parseLocalMemoryTimestamp(left)
	rightTime := parseLocalMemoryTimestamp(right)
	switch {
	case leftTime.After(rightTime):
		return 1
	case leftTime.Before(rightTime):
		return -1
	default:
		return strings.Compare(strings.TrimSpace(left), strings.TrimSpace(right))
	}
}

func parseLocalMemoryTimestamp(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func (s *LocalMemoryStore) reindexLocked() {
	s.episodeIndex = map[string]int{}
	s.digestIndex = map[string]int{}
	s.episodeRefs = newLocalMemoryRecordSelectorIndex()
	s.digestRefs = newLocalMemoryRecordSelectorIndex()
	for idx, episode := range s.state.Episodes {
		id := strings.TrimSpace(episode.EpisodeID)
		if id != "" {
			s.episodeIndex[id] = idx
			s.episodeRefs.add(id, episode.Scope, episode.DocKeys, episode.ArtifactRefs, episode.ConstraintRefs, episode.SegmentRefs)
		}
	}
	for idx, digest := range s.state.Digests {
		id := strings.TrimSpace(digest.DigestID)
		if id != "" {
			s.digestIndex[id] = idx
			s.digestRefs.add(id, digest.Scope, digest.DocKeys, digest.ArtifactRefs, digest.ConstraintRefs, digest.SegmentRefs)
		}
	}
}

func newLocalMemoryRecordSelectorIndex() localMemoryRecordSelectorIndex {
	return localMemoryRecordSelectorIndex{
		taskIDs:         map[string]map[string]struct{}{},
		sessionIDs:      map[string]map[string]struct{}{},
		tensionIDs:      map[string]map[string]struct{}{},
		protoClusterIDs: map[string]map[string]struct{}{},
		docKeys:         map[string]map[string]struct{}{},
		artifactRefs:    map[string]map[string]struct{}{},
		constraintRefs:  map[string]map[string]struct{}{},
		segmentRefs:     map[string]map[string]struct{}{},
	}
}

func (idx *localMemoryRecordSelectorIndex) add(id string, scope LocalMemoryScope, docKeys, artifactRefs, constraintRefs, segmentRefs []string) {
	id = strings.TrimSpace(id)
	if id == "" || idx == nil {
		return
	}
	addLocalMemoryIndexEntry(idx.taskIDs, scope.TaskID, id)
	addLocalMemoryIndexEntry(idx.sessionIDs, scope.SessionID, id)
	addLocalMemoryIndexEntry(idx.tensionIDs, scope.TensionID, id)
	addLocalMemoryIndexEntry(idx.protoClusterIDs, scope.ProtoClusterID, id)
	for _, value := range docKeys {
		addLocalMemoryIndexEntry(idx.docKeys, value, id)
	}
	for _, value := range artifactRefs {
		addLocalMemoryIndexEntry(idx.artifactRefs, value, id)
	}
	for _, value := range constraintRefs {
		addLocalMemoryIndexEntry(idx.constraintRefs, value, id)
	}
	for _, value := range segmentRefs {
		addLocalMemoryIndexEntry(idx.segmentRefs, value, id)
	}
}

func addLocalMemoryIndexEntry(index map[string]map[string]struct{}, key, id string) {
	trimmedKey := strings.TrimSpace(key)
	trimmedID := strings.TrimSpace(id)
	if trimmedKey == "" || trimmedID == "" {
		return
	}
	bucket, ok := index[trimmedKey]
	if !ok {
		bucket = map[string]struct{}{}
		index[trimmedKey] = bucket
	}
	bucket[trimmedID] = struct{}{}
}

func (s *LocalMemoryStore) candidateEpisodeIDsLocked(query localMemoryPacketQueryNormalized) []string {
	if s == nil || !query.hasSelectors() {
		return nil
	}
	if s.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		candidates, err := selectScoredLocalMemoryEpisodeIDs(ctx, s.db, query)
		if err == nil && len(candidates) > 0 {
			ids := make([]string, 0, len(candidates))
			for _, c := range candidates {
				ids = append(ids, c.ID)
			}
			return ids
		}
		if err == nil {
			return nil
		}
	}
	ids := map[string]struct{}{}
	collectIndexedLocalMemoryScopeIDs(ids, s.episodeRefs, query.scope)
	collectIndexedLocalMemoryRefIDs(ids, s.episodeRefs, query)
	return sortedLocalMemoryIDs(ids)
}

func (s *LocalMemoryStore) candidateDigestIDsLocked(query localMemoryPacketQueryNormalized) []string {
	if s == nil || !query.hasSelectors() {
		return nil
	}
	if s.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		candidates, err := selectScoredLocalMemoryDigestIDs(ctx, s.db, query)
		if err == nil && len(candidates) > 0 {
			ids := make([]string, 0, len(candidates))
			for _, c := range candidates {
				ids = append(ids, c.ID)
			}
			return ids
		}
		if err == nil {
			return nil
		}
	}
	ids := map[string]struct{}{}
	collectIndexedLocalMemoryScopeIDs(ids, s.digestRefs, query.scope)
	collectIndexedLocalMemoryRefIDs(ids, s.digestRefs, query)
	return sortedLocalMemoryIDs(ids)
}

func collectIndexedLocalMemoryIDs(index localMemoryRecordSelectorIndex, query localMemoryPacketQueryNormalized) []string {
	ids := map[string]struct{}{}
	collectIndexedLocalMemoryScopeIDs(ids, index, query.scope)
	collectIndexedLocalMemoryRefIDs(ids, index, query)
	return sortedLocalMemoryIDs(ids)
}

func collectIndexedLocalMemoryScopeIDs(target map[string]struct{}, index localMemoryRecordSelectorIndex, scope LocalMemoryScope) {
	collectLocalMemoryBucket(target, index.taskIDs, scope.TaskID)
	collectLocalMemoryBucket(target, index.sessionIDs, scope.SessionID)
	collectLocalMemoryBucket(target, index.tensionIDs, scope.TensionID)
	collectLocalMemoryBucket(target, index.protoClusterIDs, scope.ProtoClusterID)
}

func collectIndexedLocalMemoryRefIDs(target map[string]struct{}, index localMemoryRecordSelectorIndex, query localMemoryPacketQueryNormalized) {
	for key := range query.docKeys {
		collectLocalMemoryBucket(target, index.docKeys, key)
	}
	for key := range query.artifactRefs {
		collectLocalMemoryBucket(target, index.artifactRefs, key)
	}
	for key := range query.constraintRefs {
		collectLocalMemoryBucket(target, index.constraintRefs, key)
	}
	for key := range query.segmentRefs {
		collectLocalMemoryBucket(target, index.segmentRefs, key)
	}
}

func addLocalMemoryIDs(target map[string]struct{}, ids []string) {
	for _, id := range ids {
		trimmed := strings.TrimSpace(id)
		if trimmed != "" {
			target[trimmed] = struct{}{}
		}
	}
}

func sortedLocalMemoryIDs(ids map[string]struct{}) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func collectLocalMemoryBucket(target map[string]struct{}, index map[string]map[string]struct{}, key string) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return
	}
	bucket, ok := index[trimmedKey]
	if !ok {
		return
	}
	for id := range bucket {
		target[id] = struct{}{}
	}
}

func (s *LocalMemoryStore) saveLocked(now time.Time) error {
	if s.state.Version == 0 {
		s.state.Version = localMemoryStateVersion
	}
	if s.state.Episodes == nil {
		s.state.Episodes = []LocalMemoryEpisodeRecord{}
	}
	if s.state.Digests == nil {
		s.state.Digests = []LocalMemoryDigestRecord{}
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	s.state.UpdatedAt = now.UTC().Format(time.RFC3339Nano)
	if err := s.syncSQLiteLocked(); err != nil {
		return err
	}
	s.revision++
	return nil
}

func (s *LocalMemoryStore) recordDigestAccessesLocked(digestIDs []string, now time.Time) error {
	if s == nil || len(digestIDs) == 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	accessedAt := now.UTC().Format(time.RFC3339Nano)
	changedIDs := make([]string, 0, len(digestIDs))
	for _, digestID := range uniqueTrimmedMemoryStrings(digestIDs) {
		idx, ok := s.digestIndex[digestID]
		if !ok {
			continue
		}
		record := s.state.Digests[idx]
		record.LastAccessedAt = accessedAt
		record.UpdatedAt = firstNonEmpty(record.UpdatedAt, accessedAt)
		s.state.Digests[idx] = record
		changedIDs = append(changedIDs, digestID)
	}
	if len(changedIDs) == 0 {
		return nil
	}
	s.state.UpdatedAt = accessedAt
	if s.db != nil {
		if err := updateLocalMemoryDigestAccesses(s.db, changedIDs, accessedAt); err != nil {
			return err
		}
		s.revision++
		return nil
	}
	if err := s.syncSQLiteLocked(); err != nil {
		return err
	}
	s.revision++
	return nil
}

func (s *LocalMemoryStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	db := s.db
	s.db = nil
	s.mu.Unlock()
	if db == nil {
		return nil
	}
	return db.Close()
}

func normalizeLocalMemoryEpisodeRecord(record LocalMemoryEpisodeRecord, now time.Time) LocalMemoryEpisodeRecord {
	record.EpisodeID = strings.TrimSpace(record.EpisodeID)
	record.Scope = normalizeLocalMemoryScope(record.Scope)
	record.ClaimModality = normalizeLocalMemoryClaimModality(record.ClaimModality)
	if record.ClaimModality == "" {
		record.ClaimModality = localMemoryClaimModalityForEpisodeRecord(record)
	}
	record.WriteState = normalizeLocalMemoryWriteState(record.WriteState)
	if record.WriteState == "" {
		if strings.TrimSpace(record.SupersededAt) != "" {
			record.WriteState = localMemoryWriteStateSuperseded
		} else {
			record.WriteState = localMemoryWriteStatePromoted
		}
	}
	record.AnchorsJSON = localMemoryAnchorsJSONFromValues(record.Scope.TaskID, record.Scope.SessionID, record.Scope.TensionID, record.Scope.ProtoClusterID, record.DocKeys, record.ArtifactRefs)
	record.RunID = strings.TrimSpace(record.RunID)
	record.Trigger = strings.TrimSpace(record.Trigger)
	record.Outcome = strings.TrimSpace(record.Outcome)
	record.Summary = strings.TrimSpace(record.Summary)
	record.DigestRefs = uniqueTrimmedMemoryStrings(record.DigestRefs)
	record.DocKeys = uniqueTrimmedMemoryStrings(record.DocKeys)
	record.ArtifactRefs = uniqueTrimmedMemoryStrings(record.ArtifactRefs)
	record.ConstraintRefs = uniqueTrimmedMemoryStrings(record.ConstraintRefs)
	record.SegmentRefs = uniqueTrimmedMemoryStrings(record.SegmentRefs)
	record.Tags = uniqueTrimmedMemoryStrings(record.Tags)
	record.Meta = cloneStringMap(record.Meta)
	record.InvalidationReasons = uniqueTrimmedMemoryStrings(record.InvalidationReasons)
	record.SupersededAt = strings.TrimSpace(record.SupersededAt)
	if strings.TrimSpace(record.CreatedAt) == "" && !now.IsZero() {
		record.CreatedAt = now.UTC().Format(time.RFC3339Nano)
	}
	return record
}

func normalizeLocalMemoryDigestRecord(record LocalMemoryDigestRecord, now time.Time) LocalMemoryDigestRecord {
	record.DigestID = strings.TrimSpace(record.DigestID)
	record.Tier = normalizeLocalMemoryTier(record.Tier)
	record.Kind = strings.TrimSpace(record.Kind)
	if record.Kind == "" {
		record.Kind = "DIGEST"
	}
	record.Scope = normalizeLocalMemoryScope(record.Scope)
	record.ClaimModality = normalizeLocalMemoryClaimModality(record.ClaimModality)
	if record.ClaimModality == "" {
		record.ClaimModality = localMemoryClaimModalityForDigestRecord(record)
	}
	record.WriteState = normalizeLocalMemoryWriteState(record.WriteState)
	if record.WriteState == "" {
		if strings.TrimSpace(record.InvalidatedAt) != "" || record.Stale {
			record.WriteState = localMemoryWriteStateSuperseded
		} else {
			record.WriteState = localMemoryWriteStatePromoted
		}
	}
	record.AnchorsJSON = localMemoryAnchorsJSONFromValues(record.Scope.TaskID, record.Scope.SessionID, record.Scope.TensionID, record.Scope.ProtoClusterID, record.DocKeys, record.ArtifactRefs)
	record.Visibility = strings.ToUpper(strings.TrimSpace(record.Visibility))
	if record.Visibility == "" {
		record.Visibility = "PRIVATE"
	}
	record.Summary = strings.TrimSpace(record.Summary)
	record.Body = strings.TrimSpace(record.Body)
	record.SourceEpisodeID = strings.TrimSpace(record.SourceEpisodeID)
	record.EpisodeDigest = cloneLocalEpisodeDigestPtr(record.EpisodeDigest)
	record.DocKeys = uniqueTrimmedMemoryStrings(record.DocKeys)
	record.ArtifactRefs = uniqueTrimmedMemoryStrings(record.ArtifactRefs)
	record.ConstraintRefs = uniqueTrimmedMemoryStrings(record.ConstraintRefs)
	record.SegmentRefs = uniqueTrimmedMemoryStrings(record.SegmentRefs)
	record.Guards = normalizeLocalMemoryGuards(record.Guards)
	record.Tags = uniqueTrimmedMemoryStrings(record.Tags)
	record.Meta = cloneStringMap(record.Meta)
	record.InvalidatedAt = strings.TrimSpace(record.InvalidatedAt)
	record.InvalidationReasons = uniqueTrimmedMemoryStrings(record.InvalidationReasons)
	if strings.TrimSpace(record.CreatedAt) == "" && !now.IsZero() {
		record.CreatedAt = now.UTC().Format(time.RFC3339Nano)
	}
	if !now.IsZero() {
		record.UpdatedAt = now.UTC().Format(time.RFC3339Nano)
	} else {
		record.UpdatedAt = strings.TrimSpace(record.UpdatedAt)
	}
	return record
}

func normalizeLocalMemoryScope(scope LocalMemoryScope) LocalMemoryScope {
	scope.TaskID = strings.TrimSpace(scope.TaskID)
	scope.SessionID = strings.TrimSpace(scope.SessionID)
	scope.TensionID = strings.TrimSpace(scope.TensionID)
	scope.ProtoClusterID = strings.TrimSpace(scope.ProtoClusterID)
	return scope
}

func normalizeLocalMemoryGuards(guards []LocalMemoryGuard) []LocalMemoryGuard {
	out := make([]LocalMemoryGuard, 0, len(guards))
	seen := map[string]struct{}{}
	for _, guard := range guards {
		normalized := LocalMemoryGuard{
			GuardType: strings.ToLower(strings.TrimSpace(guard.GuardType)),
			Ref:       strings.TrimSpace(guard.Ref),
			Version:   strings.TrimSpace(guard.Version),
		}
		if normalized.GuardType == "" || normalized.Ref == "" {
			continue
		}
		key := normalized.GuardType + "|" + normalized.Ref + "|" + normalized.Version
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func normalizeLocalMemoryTier(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "P1":
		return "P1"
	default:
		return "P2"
	}
}

type localMemoryAnchorSnapshot struct {
	TaskID         string   `json:"task_id,omitempty"`
	SessionID      string   `json:"session_id,omitempty"`
	TensionID      string   `json:"tension_id,omitempty"`
	ProtoClusterID string   `json:"proto_cluster_id,omitempty"`
	DocKeys        []string `json:"doc_keys,omitempty"`
	ArtifactRefs   []string `json:"artifact_refs,omitempty"`
}

func localMemoryAnchorsJSONFromValues(taskID, sessionID, tensionID, protoClusterID string, docKeys, artifactRefs []string) string {
	payload := localMemoryAnchorSnapshot{
		TaskID:         strings.TrimSpace(taskID),
		SessionID:      strings.TrimSpace(sessionID),
		TensionID:      strings.TrimSpace(tensionID),
		ProtoClusterID: strings.TrimSpace(protoClusterID),
		DocKeys:        uniqueTrimmedMemoryStrings(docKeys),
		ArtifactRefs:   uniqueTrimmedMemoryStrings(artifactRefs),
	}
	raw, err := json.Marshal(payload)
	if err != nil || len(raw) == 0 {
		return ""
	}
	return string(raw)
}

func localMemoryClaimModalityForEpisodeRecord(record LocalMemoryEpisodeRecord) string {
	text := strings.ToLower(strings.TrimSpace(strings.Join([]string{record.Trigger, record.Outcome, record.Summary}, " ")))
	switch {
	case len(record.ConstraintRefs) > 0 || containsAnyLocalMemoryText(text, "block", "constraint", "guard"):
		return localMemoryClaimModalityConstrained
	case containsAnyLocalMemoryText(text, "decid", "decision", "resolve", "handoff"):
		return localMemoryClaimModalityDecided
	case containsAnyLocalMemoryText(text, "propos", "candidate", "draft"):
		return localMemoryClaimModalityProposed
	case containsAnyLocalMemoryText(text, "infer", "analysis", "synth", "derive"):
		return localMemoryClaimModalityInferred
	default:
		return localMemoryClaimModalityObserved
	}
}

func localMemoryClaimModalityForDigestRecord(record LocalMemoryDigestRecord) string {
	text := strings.ToLower(strings.TrimSpace(strings.Join([]string{record.Kind, record.Summary, record.Body}, " ")))
	switch {
	case len(record.ConstraintRefs) > 0 || len(record.Guards) > 0 || containsAnyLocalMemoryText(text, "block", "constraint", "guard"):
		return localMemoryClaimModalityConstrained
	case containsAnyLocalMemoryText(text, "decid", "decision"):
		return localMemoryClaimModalityDecided
	case containsAnyLocalMemoryText(text, "propos", "candidate", "draft"):
		return localMemoryClaimModalityProposed
	case containsAnyLocalMemoryText(text, "infer", "analysis", "synth", "derive", "proced", "lesson"):
		return localMemoryClaimModalityInferred
	default:
		return localMemoryClaimModalityObserved
	}
}

func containsAnyLocalMemoryText(text string, fragments ...string) bool {
	for _, fragment := range fragments {
		if strings.Contains(text, fragment) {
			return true
		}
	}
	return false
}

func normalizeLocalMemoryState(state LocalMemoryState, now time.Time) LocalMemoryState {
	source := cloneLocalMemoryState(state)
	source.Version = localMemoryStateVersion
	source.WorkspaceID = strings.TrimSpace(source.WorkspaceID)
	source.AgentID = strings.TrimSpace(source.AgentID)
	source.UpdatedAt = strings.TrimSpace(source.UpdatedAt)
	normalizedEpisodes := make([]LocalMemoryEpisodeRecord, 0, len(source.Episodes))
	for _, episode := range source.Episodes {
		normalizedEpisodes = append(normalizedEpisodes, normalizeLocalMemoryEpisodeRecord(episode, now))
	}
	normalizedDigests := make([]LocalMemoryDigestRecord, 0, len(source.Digests))
	for _, digest := range source.Digests {
		normalizedDigests = append(normalizedDigests, normalizeLocalMemoryDigestRecord(digest, now))
	}
	source.Episodes = normalizedEpisodes
	source.Digests = normalizedDigests
	return source
}

func normalizeLocalMemoryPromotionCandidate(candidate LocalPromotionCandidate) LocalPromotionCandidate {
	candidate.CandidateID = strings.TrimSpace(candidate.CandidateID)
	candidate.CreatedAt = strings.TrimSpace(candidate.CreatedAt)
	candidate.NodeType = localMemoryNodeType(strings.TrimSpace(string(candidate.NodeType)))
	candidate.ClaimModality = normalizeLocalMemoryClaimModality(candidate.ClaimModality)
	if candidate.ClaimModality == "" {
		candidate.ClaimModality = localMemoryClaimModalityForPromotionCandidate(candidate)
	}
	candidate.WriteState = normalizeLocalMemoryWriteState(candidate.WriteState)
	if candidate.WriteState == "" {
		if strings.TrimSpace(candidate.PromotedAt) != "" {
			candidate.WriteState = localMemoryWriteStatePromoted
		} else {
			candidate.WriteState = localMemoryWriteStateCandidate
		}
	}
	candidate.MemoryType = strings.ToUpper(strings.TrimSpace(candidate.MemoryType))
	candidate.SourceID = strings.TrimSpace(candidate.SourceID)
	candidate.Title = strings.TrimSpace(candidate.Title)
	candidate.Body = strings.TrimSpace(candidate.Body)
	candidate.Summary = strings.TrimSpace(candidate.Summary)
	candidate.TaskID = strings.TrimSpace(candidate.TaskID)
	candidate.SessionID = strings.TrimSpace(candidate.SessionID)
	candidate.TensionID = strings.TrimSpace(candidate.TensionID)
	candidate.ProtoClusterID = strings.TrimSpace(candidate.ProtoClusterID)
	candidate.ArtifactRefs = uniqueTrimmedMemoryStrings(candidate.ArtifactRefs)
	candidate.DocKeys = uniqueTrimmedMemoryStrings(candidate.DocKeys)
	candidate.ConstraintRefs = uniqueTrimmedMemoryStrings(candidate.ConstraintRefs)
	candidate.AnchorsJSON = localMemoryAnchorsJSONFromValues(candidate.TaskID, candidate.SessionID, candidate.TensionID, candidate.ProtoClusterID, candidate.DocKeys, candidate.ArtifactRefs)
	candidate.MemoryID = strings.TrimSpace(candidate.MemoryID)
	candidate.ClaimID = strings.TrimSpace(candidate.ClaimID)
	candidate.LastAttemptAt = strings.TrimSpace(candidate.LastAttemptAt)
	candidate.PromotedAt = strings.TrimSpace(candidate.PromotedAt)
	candidate.LastError = strings.TrimSpace(candidate.LastError)
	return candidate
}

func localMemoryClaimModalityForPromotionCandidate(candidate LocalPromotionCandidate) string {
	switch candidate.NodeType {
	case localMemoryNodeDecision:
		return localMemoryClaimModalityDecided
	case localMemoryNodeProcedure, localMemoryNodeAntiProcedure:
		return localMemoryClaimModalityInferred
	case localMemoryNodeConstraint:
		return localMemoryClaimModalityConstrained
	case localMemoryNodeRawEvent, localMemoryNodeRawMessage, localMemoryNodeEpisodePack, localMemoryNodeBlocker, localMemoryNodeDissent, localMemoryNodeHandoff, localMemoryNodeArtifactDelta, localMemoryNodeClusterBrief:
		return localMemoryClaimModalityObserved
	default:
		return localMemoryClaimModalityProposed
	}
}

func cloneLocalMemoryState(state LocalMemoryState) LocalMemoryState {
	copy := state
	copy.Episodes = make([]LocalMemoryEpisodeRecord, 0, len(state.Episodes))
	for _, episode := range state.Episodes {
		copy.Episodes = append(copy.Episodes, cloneEpisodeRecord(episode))
	}
	copy.Digests = make([]LocalMemoryDigestRecord, 0, len(state.Digests))
	for _, digest := range state.Digests {
		copy.Digests = append(copy.Digests, cloneDigestRecord(digest))
	}
	return copy
}

func cloneEpisodeRecord(record LocalMemoryEpisodeRecord) LocalMemoryEpisodeRecord {
	copy := record
	copy.Scope = normalizeLocalMemoryScope(record.Scope)
	copy.DigestRefs = append([]string(nil), record.DigestRefs...)
	copy.DocKeys = append([]string(nil), record.DocKeys...)
	copy.ArtifactRefs = append([]string(nil), record.ArtifactRefs...)
	copy.ConstraintRefs = append([]string(nil), record.ConstraintRefs...)
	copy.SegmentRefs = append([]string(nil), record.SegmentRefs...)
	copy.Tags = append([]string(nil), record.Tags...)
	copy.InvalidationReasons = append([]string(nil), record.InvalidationReasons...)
	copy.Meta = cloneStringMap(record.Meta)
	return copy
}

func cloneDigestRecord(record LocalMemoryDigestRecord) LocalMemoryDigestRecord {
	copy := record
	copy.Scope = normalizeLocalMemoryScope(record.Scope)
	copy.EpisodeDigest = cloneLocalEpisodeDigestPtr(record.EpisodeDigest)
	copy.DocKeys = append([]string(nil), record.DocKeys...)
	copy.ArtifactRefs = append([]string(nil), record.ArtifactRefs...)
	copy.ConstraintRefs = append([]string(nil), record.ConstraintRefs...)
	copy.SegmentRefs = append([]string(nil), record.SegmentRefs...)
	copy.Guards = append([]LocalMemoryGuard(nil), record.Guards...)
	copy.Tags = append([]string(nil), record.Tags...)
	copy.InvalidationReasons = append([]string(nil), record.InvalidationReasons...)
	copy.Meta = cloneStringMap(record.Meta)
	return copy
}

func cloneLocalEpisodeDigestPtr(source *LocalEpisodeDigest) *LocalEpisodeDigest {
	if source == nil {
		return nil
	}
	copy := *source
	copy.RecentSummaries = append([]string(nil), source.RecentSummaries...)
	copy.ArtifactRefs = append([]string(nil), source.ArtifactRefs...)
	copy.DocKeys = append([]string(nil), source.DocKeys...)
	copy.BlockerKinds = append([]string(nil), source.BlockerKinds...)
	copy.OpenLoops = append([]string(nil), source.OpenLoops...)
	copy.DissentSummaries = append([]string(nil), source.DissentSummaries...)
	copy.ConstraintRefs = append([]string(nil), source.ConstraintRefs...)
	return &copy
}

func uniqueTrimmedMemoryStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func mergeUniqueMemoryStrings(chunks ...[]string) []string {
	var merged []string
	for _, chunk := range chunks {
		merged = append(merged, chunk...)
	}
	return uniqueTrimmedMemoryStrings(merged)
}

func cloneStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	copy := make(map[string]string, len(source))
	for key, value := range source {
		trimmedKey := strings.TrimSpace(key)
		if trimmedKey == "" {
			continue
		}
		copy[trimmedKey] = strings.TrimSpace(value)
	}
	if len(copy) == 0 {
		return nil
	}
	return copy
}

func (s *LocalMemoryStore) Search(ctx context.Context, query string, limit int) ([]LocalMemoryDigestRecord, []LocalMemoryEpisodeRecord, error) {
	if s.db == nil {
		return nil, nil, fmt.Errorf("local memory store offline")
	}
	digestIDs, episodeIDs, err := SearchLocalMemoryFTS(ctx, s.db, query, limit)
	if err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var digests []LocalMemoryDigestRecord
	for _, id := range digestIDs {
		if idx, ok := s.digestIndex[id]; ok {
			digests = append(digests, s.state.Digests[idx])
		}
	}

	var episodes []LocalMemoryEpisodeRecord
	for _, id := range episodeIDs {
		if idx, ok := s.episodeIndex[id]; ok {
			episodes = append(episodes, s.state.Episodes[idx])
		}
	}

	return digests, episodes, nil
}

func (s *LocalMemoryStore) PutPromotion(ctx context.Context, candidate LocalPromotionCandidate) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	return upsertLocalMemoryPromotion(ctx, s.db, candidate, "")
}

func (s *LocalMemoryStore) PendingPromotions(ctx context.Context, limit int) ([]LocalPromotionCandidate, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil, nil
	}
	return selectPendingLocalMemoryPromotions(ctx, s.db, limit)
}

func (s *LocalMemoryStore) SetPromotionStatus(ctx context.Context, candidateID, memoryID, claimID, status string, errInfo error, incrementAttempt bool) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	errMsg := ""
	if errInfo != nil {
		errMsg = errInfo.Error()
	}
	return updateLocalMemoryPromotionStatus(ctx, s.db, candidateID, memoryID, claimID, status, errMsg, incrementAttempt)
}
