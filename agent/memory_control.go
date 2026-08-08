package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

func (s *AgentMemoryService) controlSnapshot() LocalMemoryControlSnapshot {
	if s == nil {
		return LocalMemoryControlSnapshot{}
	}

	// We do DB lookup outside of strict lock to avoid deadlocks, though here it's fast.
	var pendingProms []LocalPromotionCandidate
	if s.store != nil {
		pendingProms, _ = s.store.PendingPromotions(context.Background(), 50)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.state.Stats
	stats.EpisodeDigests = len(s.state.TaskDigests) + len(s.state.TensionDigests) + len(s.state.ClusterDigests)
	stats.Procedures = len(s.state.Procedures)
	stats.AntiProcedures = len(s.state.AntiProcedures)

	snapshot := LocalMemoryControlSnapshot{
		Stats:                stats,
		PacketCacheEntries:   len(s.packetCache),
		LastSequence:         s.state.LastSequence,
		DocVersionCount:      len(s.state.DocVersions),
		ArtifactVersionCount: len(s.state.ArtifactVersions),
		Procedures:           topProcedureMemories(s.state.Procedures, 4),
		AntiProcedures:       topProcedureMemories(s.state.AntiProcedures, 4),
		PendingPromotions:    summarizePendingPromotions(pendingProms, 4),
	}
	if s.store != nil {
		snapshot.Store = s.store.Stats()
		if snapshot.Store.Episodes > snapshot.Stats.TotalEvents {
			snapshot.Stats.TotalEvents = snapshot.Store.Episodes
		}
	}
	return snapshot
}

func (s *AgentMemoryService) forceRebuild() error {
	if s == nil {
		return nil
	}
	s.invalidatePacketCache()
	if s.store == nil {
		return nil
	}
	return s.reconcileShadowFromStore(true)
}

func topProcedureMemories(items map[string]LocalProcedureMemory, limit int) []LocalProcedureMemory {
	if len(items) == 0 {
		return nil
	}
	list := make([]LocalProcedureMemory, 0, len(items))
	for _, item := range items {
		copy := item
		copy.ArtifactRefs = cloneStrings(copy.ArtifactRefs)
		copy.DocKeys = cloneStrings(copy.DocKeys)
		copy.BlockerKinds = cloneStrings(copy.BlockerKinds)
		copy.SourceEventIDs = cloneStrings(copy.SourceEventIDs)
		list = append(list, copy)
	}
	sort.SliceStable(list, func(i, j int) bool {
		switch {
		case list[i].EvidenceCount != list[j].EvidenceCount:
			return list[i].EvidenceCount > list[j].EvidenceCount
		case compareLocalMemoryTimestamps(list[i].LastSeenAt, list[j].LastSeenAt) != 0:
			return compareLocalMemoryTimestamps(list[i].LastSeenAt, list[j].LastSeenAt) > 0
		default:
			return list[i].Summary < list[j].Summary
		}
	})
	if limit > 0 && len(list) > limit {
		list = list[:limit]
	}
	return list
}

func summarizePendingPromotions(items []LocalPromotionCandidate, limit int) []LocalMemoryPromotionSummary {
	if len(items) == 0 {
		return nil
	}
	out := make([]LocalMemoryPromotionSummary, 0, min(limit, len(items)))
	for _, item := range items {
		if strings.TrimSpace(item.PromotedAt) != "" {
			continue
		}
		out = append(out, LocalMemoryPromotionSummary{
			CandidateID:    strings.TrimSpace(item.CandidateID),
			NodeType:       item.NodeType,
			MemoryType:     strings.TrimSpace(item.MemoryType),
			Summary:        firstNonEmpty(strings.TrimSpace(item.Summary), strings.TrimSpace(item.Title)),
			TaskID:         strings.TrimSpace(item.TaskID),
			SessionID:      strings.TrimSpace(item.SessionID),
			TensionID:      strings.TrimSpace(item.TensionID),
			ProtoClusterID: strings.TrimSpace(item.ProtoClusterID),
			CreatedAt:      strings.TrimSpace(item.CreatedAt),
			AttemptCount:   item.AttemptCount,
			LastError:      strings.TrimSpace(item.LastError),
		})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func (r *Runtime) memoryControlSnapshot() LocalMemoryControlSnapshot {
	service := r.memoryService()
	if service == nil {
		return LocalMemoryControlSnapshot{}
	}
	return service.controlSnapshot()
}

func (r *Runtime) memorySummary() string {
	snapshot := r.memoryControlSnapshot()
	parts := []string{}
	if snapshot.Stats.Procedures > 0 {
		parts = append(parts, fmt.Sprintf("proc=%d", snapshot.Stats.Procedures))
	}
	if snapshot.Stats.AntiProcedures > 0 {
		parts = append(parts, fmt.Sprintf("anti=%d", snapshot.Stats.AntiProcedures))
	}
	if snapshot.Stats.PromotionQueue > 0 {
		parts = append(parts, fmt.Sprintf("prom=%d", snapshot.Stats.PromotionQueue))
	}
	if snapshot.Store.StaleDigests > 0 {
		parts = append(parts, fmt.Sprintf("stale=%d", snapshot.Store.StaleDigests))
	}
	if len(parts) == 0 {
		return ""
	}
	return "memory " + strings.Join(parts, " ")
}

func (r *Runtime) currentMemoryPacket(maxChars int) string {
	service := r.memoryService()
	if service == nil {
		return ""
	}
	if maxChars <= 0 {
		maxChars = max(1200, r.cfg.MaxPromptSpecChars/3)
	}
	task := r.currentTaskCopy()
	hydration := r.currentHydrationCopy()
	focus := r.currentFocusCopy()
	r.syncMemoryCanonicalVersions(hydration)
	return service.buildPacket(MemoryPacketInput{
		Task:           task,
		Session:        r.currentSessionCopy(),
		Focus:          focus,
		Hydration:      hydration,
		WorkPacket:     r.currentWorkPacketCopy(),
		CurrentSummary: r.currentSummary(),
	}, maxChars)
}
