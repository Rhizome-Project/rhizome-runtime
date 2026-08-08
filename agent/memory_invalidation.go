package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type LocalMemoryGuardChange struct {
	GuardType      string `json:"guard_type"`
	Ref            string `json:"ref"`
	CurrentVersion string `json:"current_version,omitempty"`
}

type LocalMemoryInvalidationInput struct {
	Reasons         []string                 `json:"reasons,omitempty"`
	TaskIDs         []string                 `json:"task_ids,omitempty"`
	SessionIDs      []string                 `json:"session_ids,omitempty"`
	TensionIDs      []string                 `json:"tension_ids,omitempty"`
	ProtoClusterIDs []string                 `json:"proto_cluster_ids,omitempty"`
	DocKeys         []string                 `json:"doc_keys,omitempty"`
	ArtifactRefs    []string                 `json:"artifact_refs,omitempty"`
	ConstraintRefs  []string                 `json:"constraint_refs,omitempty"`
	SegmentRefs     []string                 `json:"segment_refs,omitempty"`
	GuardChanges    []LocalMemoryGuardChange `json:"guard_changes,omitempty"`
}

type LocalMemoryInvalidationResult struct {
	InvalidatedAt      string   `json:"invalidated_at,omitempty"`
	DigestsInvalidated int      `json:"digests_invalidated"`
	EpisodesSuperseded int      `json:"episodes_superseded"`
	DigestIDs          []string `json:"digest_ids,omitempty"`
	EpisodeIDs         []string `json:"episode_ids,omitempty"`
}

func (s *LocalMemoryStore) Invalidate(input LocalMemoryInvalidationInput) (LocalMemoryInvalidationResult, error) {
	if s == nil {
		return LocalMemoryInvalidationResult{}, nil
	}
	normalized := normalizeLocalMemoryInvalidationInput(input)
	if !normalized.hasSelectors() {
		return LocalMemoryInvalidationResult{}, nil
	}
	now := time.Now().UTC()
	at := now.Format(time.RFC3339Nano)

	s.mu.Lock()
	defer s.mu.Unlock()

	var result LocalMemoryInvalidationResult
	result.InvalidatedAt = at

	for idx, episode := range s.state.Episodes {
		reasons := normalized.matchReasonsForEpisode(episode)
		if len(reasons) == 0 {
			continue
		}
		episode.SupersededAt = at
		episode.InvalidationReasons = mergeUniqueMemoryStrings(episode.InvalidationReasons, reasons)
		s.state.Episodes[idx] = episode
		result.EpisodesSuperseded++
		result.EpisodeIDs = append(result.EpisodeIDs, episode.EpisodeID)
	}

	for idx, digest := range s.state.Digests {
		reasons := normalized.matchReasonsForDigest(digest)
		if len(reasons) == 0 {
			continue
		}
		digest.Stale = true
		digest.InvalidatedAt = at
		digest.InvalidationReasons = mergeUniqueMemoryStrings(digest.InvalidationReasons, reasons)
		digest.UpdatedAt = at
		s.state.Digests[idx] = digest
		result.DigestsInvalidated++
		result.DigestIDs = append(result.DigestIDs, digest.DigestID)
	}

	if result.DigestsInvalidated == 0 && result.EpisodesSuperseded == 0 {
		return LocalMemoryInvalidationResult{}, nil
	}
	sort.Strings(result.DigestIDs)
	sort.Strings(result.EpisodeIDs)
	if err := s.saveLocked(now); err != nil {
		return LocalMemoryInvalidationResult{}, err
	}
	return result, nil
}

type localMemoryNormalizedInvalidation struct {
	reasons         []string
	taskIDs         map[string]struct{}
	sessionIDs      map[string]struct{}
	tensionIDs      map[string]struct{}
	protoClusterIDs map[string]struct{}
	docKeys         map[string]struct{}
	artifactRefs    map[string]struct{}
	constraintRefs  map[string]struct{}
	segmentRefs     map[string]struct{}
	guardChanges    map[string]LocalMemoryGuardChange
}

func normalizeLocalMemoryInvalidationInput(input LocalMemoryInvalidationInput) localMemoryNormalizedInvalidation {
	out := localMemoryNormalizedInvalidation{
		reasons:         uniqueTrimmedMemoryStrings(input.Reasons),
		taskIDs:         localMemorySet(input.TaskIDs),
		sessionIDs:      localMemorySet(input.SessionIDs),
		tensionIDs:      localMemorySet(input.TensionIDs),
		protoClusterIDs: localMemorySet(input.ProtoClusterIDs),
		docKeys:         localMemorySet(input.DocKeys),
		artifactRefs:    localMemorySet(input.ArtifactRefs),
		constraintRefs:  localMemorySet(input.ConstraintRefs),
		segmentRefs:     localMemorySet(input.SegmentRefs),
		guardChanges:    map[string]LocalMemoryGuardChange{},
	}
	for _, change := range input.GuardChanges {
		normalized := LocalMemoryGuardChange{
			GuardType:      strings.ToLower(strings.TrimSpace(change.GuardType)),
			Ref:            strings.TrimSpace(change.Ref),
			CurrentVersion: strings.TrimSpace(change.CurrentVersion),
		}
		if normalized.GuardType == "" || normalized.Ref == "" {
			continue
		}
		out.guardChanges[normalized.GuardType+"|"+normalized.Ref] = normalized
	}
	if len(out.reasons) == 0 {
		out.reasons = []string{"invalidate"}
	}
	return out
}

func (n localMemoryNormalizedInvalidation) hasSelectors() bool {
	return len(n.taskIDs) > 0 ||
		len(n.sessionIDs) > 0 ||
		len(n.tensionIDs) > 0 ||
		len(n.protoClusterIDs) > 0 ||
		len(n.docKeys) > 0 ||
		len(n.artifactRefs) > 0 ||
		len(n.constraintRefs) > 0 ||
		len(n.segmentRefs) > 0 ||
		len(n.guardChanges) > 0
}

func (n localMemoryNormalizedInvalidation) matchReasonsForEpisode(record LocalMemoryEpisodeRecord) []string {
	var reasons []string
	reasons = append(reasons, n.matchReasonsForScope(record.Scope)...)
	reasons = append(reasons, n.matchReasonsForRefs(record.DocKeys, n.docKeys, "doc")...)
	reasons = append(reasons, n.matchReasonsForRefs(record.ArtifactRefs, n.artifactRefs, "artifact")...)
	reasons = append(reasons, n.matchReasonsForRefs(record.ConstraintRefs, n.constraintRefs, "constraint")...)
	reasons = append(reasons, n.matchReasonsForRefs(record.SegmentRefs, n.segmentRefs, "segment")...)
	if len(reasons) == 0 {
		return nil
	}
	return mergeUniqueMemoryStrings(reasons, n.reasons)
}

func (n localMemoryNormalizedInvalidation) matchReasonsForDigest(record LocalMemoryDigestRecord) []string {
	reasons := n.matchReasonsForEpisode(LocalMemoryEpisodeRecord{
		Scope:          record.Scope,
		DocKeys:        record.DocKeys,
		ArtifactRefs:   record.ArtifactRefs,
		ConstraintRefs: record.ConstraintRefs,
		SegmentRefs:    record.SegmentRefs,
	})
	for _, guard := range record.Guards {
		key := strings.ToLower(strings.TrimSpace(guard.GuardType)) + "|" + strings.TrimSpace(guard.Ref)
		change, ok := n.guardChanges[key]
		if !ok {
			continue
		}
		currentVersion := strings.TrimSpace(change.CurrentVersion)
		guardVersion := strings.TrimSpace(guard.Version)
		if currentVersion != "" && currentVersion == guardVersion {
			continue
		}
		reasons = append(reasons, fmt.Sprintf("guard:%s:%s", change.GuardType, change.Ref))
	}
	if len(reasons) == 0 {
		return nil
	}
	return mergeUniqueMemoryStrings(reasons, n.reasons)
}

func (n localMemoryNormalizedInvalidation) matchReasonsForScope(scope LocalMemoryScope) []string {
	var reasons []string
	if scope.TaskID != "" {
		if _, ok := n.taskIDs[scope.TaskID]; ok {
			reasons = append(reasons, "task:"+scope.TaskID)
		}
	}
	if scope.SessionID != "" {
		if _, ok := n.sessionIDs[scope.SessionID]; ok {
			reasons = append(reasons, "session:"+scope.SessionID)
		}
	}
	if scope.TensionID != "" {
		if _, ok := n.tensionIDs[scope.TensionID]; ok {
			reasons = append(reasons, "tension:"+scope.TensionID)
		}
	}
	if scope.ProtoClusterID != "" {
		if _, ok := n.protoClusterIDs[scope.ProtoClusterID]; ok {
			reasons = append(reasons, "cluster:"+scope.ProtoClusterID)
		}
	}
	return reasons
}

func (n localMemoryNormalizedInvalidation) matchReasonsForRefs(values []string, selector map[string]struct{}, prefix string) []string {
	if len(selector) == 0 || len(values) == 0 {
		return nil
	}
	var reasons []string
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := selector[trimmed]; ok {
			reasons = append(reasons, prefix+":"+trimmed)
		}
	}
	return reasons
}

func localMemorySet(values []string) map[string]struct{} {
	set := make(map[string]struct{})
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		set[trimmed] = struct{}{}
	}
	return set
}
