package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

func (s *AgentMemoryService) buildPacket(input MemoryPacketInput, maxChars int) string {
	if s == nil {
		return ""
	}
	now := time.Now().UTC()

	s.mu.Lock()
	events := append([]LocalMemoryEvent(nil), s.state.RecentEvents...)
	taskDigests := cloneEpisodeDigestMap(s.state.TaskDigests)
	tensionDigests := cloneEpisodeDigestMap(s.state.TensionDigests)
	clusterDigests := cloneEpisodeDigestMap(s.state.ClusterDigests)
	procedures := cloneProcedureMemoryMap(s.state.Procedures)
	antiProcedures := cloneProcedureMemoryMap(s.state.AntiProcedures)
	constitution := s.constitution
	artifactVersions := cloneStringMap(s.state.ArtifactVersions)
	stats := s.state.Stats
	lastSequence := s.state.LastSequence
	store := s.store
	storeVersion := localMemoryStoreCacheVersion(store)
	key := memoryPacketKey(input, artifactVersions)
	if cached, ok := s.packetCache[key]; ok && cached.Key == key && cached.Sequence == s.state.LastSequence && cached.StoreUpdatedAt == storeVersion.UpdatedAt && cached.StoreRevision == storeVersion.Revision && now.Sub(cached.BuiltAt) < agentMemoryPacketTTL {
		s.state.Stats.P1Hits++
		compiled := cached.Compiled
		s.mu.Unlock()
		s.markDigestAccesses(packetAnchorTaskID(input), packetAnchorClusterID(input), packetAnchorTensionID(input), now)
		return compiled
	}
	s.state.Stats.P1Misses++
	s.mu.Unlock()

	view := LocalMemoryPacketView{}
	if store != nil {
		if packetView, err := store.ReadPacketView(buildLocalMemoryPacketQuery(input)); err == nil {
			view = packetView
		}
	}
	storeVersion = localMemoryStoreCacheVersion(store)
	stalePersistentView := len(view.StaleDigestIDs) > 0
	if view.Matched && !stalePersistentView {
		stats.P2Hits++
		stats.ConsecutiveP2Misses = 0
		stats.LastP2HitAt = now.Format(time.RFC3339Nano)
	} else if store != nil {
		stats.P2Misses++
		stats.ConsecutiveP2Misses++
		stats.LastP2MissAt = now.Format(time.RFC3339Nano)
	}
	if len(view.StaleDigestIDs) > 0 {
		stats.StaleHits += len(view.StaleDigestIDs)
		stats.ConsecutiveStaleReads++
		stats.LastStaleHitAt = now.Format(time.RFC3339Nano)
	} else {
		stats.ConsecutiveStaleReads = 0
	}
	freshness := localMemoryPacketFreshness{
		StaleDigestIDs: cloneStrings(view.StaleDigestIDs),
	}
	if stalePersistentView {
		freshness.PersistentViewDowngraded = true
		view = downgradeStaleLocalMemoryPacketView(view)
	}
	events = mergePacketEventsWithView(events, view)
	taskDigests = mergePacketDigestMaps(taskDigests, view.TaskDigest)
	tensionDigests = mergePacketDigestMaps(tensionDigests, view.TensionDigest)
	clusterDigests = mergePacketDigestMaps(clusterDigests, view.ClusterDigest)

	compiled := compileMemoryPacket(events, taskDigests, tensionDigests, clusterDigests, procedures, antiProcedures, constitution, artifactVersions, stats, freshness, input, maxChars)

	s.mu.Lock()
	if freshness.PersistentViewDowngraded {
		delete(s.packetCache, key)
	} else {
		s.packetCache[key] = cachedMemoryPacket{
			Key:            key,
			Compiled:       compiled,
			BuiltAt:        now,
			Sequence:       lastSequence,
			StoreUpdatedAt: memoryPacketStoreUpdatedAt(storeVersion.UpdatedAt, view),
			StoreRevision:  storeVersion.Revision,
		}
	}
	s.state.Stats.PacketBuilds++
	s.state.Stats.P2Hits = stats.P2Hits
	s.state.Stats.P2Misses = stats.P2Misses
	s.state.Stats.StaleHits = stats.StaleHits
	s.state.Stats.ConsecutiveP2Misses = stats.ConsecutiveP2Misses
	s.state.Stats.ConsecutiveStaleReads = stats.ConsecutiveStaleReads
	s.state.Stats.LastP2HitAt = stats.LastP2HitAt
	s.state.Stats.LastP2MissAt = stats.LastP2MissAt
	s.state.Stats.LastStaleHitAt = stats.LastStaleHitAt
	s.state.Stats.LastPacketBuiltAt = now.Format(time.RFC3339Nano)
	_ = s.saveStateLocked()
	s.mu.Unlock()
	if !view.Matched {
		s.markDigestAccesses(packetAnchorTaskID(input), packetAnchorClusterID(input), packetAnchorTensionID(input), now)
	}

	return compiled
}

type localMemoryStoreCacheSnapshot struct {
	UpdatedAt string
	Revision  int64
}

func localMemoryStoreCacheVersion(store *LocalMemoryStore) localMemoryStoreCacheSnapshot {
	if store == nil {
		return localMemoryStoreCacheSnapshot{}
	}
	stats := store.Stats()
	return localMemoryStoreCacheSnapshot{
		UpdatedAt: strings.TrimSpace(stats.UpdatedAt),
		Revision:  stats.Revision,
	}
}

func memoryPacketStoreUpdatedAt(fallback string, view LocalMemoryPacketView) string {
	if updatedAt := strings.TrimSpace(view.UpdatedAt); updatedAt != "" {
		return updatedAt
	}
	return strings.TrimSpace(fallback)
}

type localMemoryPacketFreshness struct {
	StaleDigestIDs           []string
	PersistentViewDowngraded bool
}

func buildLocalMemoryPacketQuery(input MemoryPacketInput) LocalMemoryPacketQuery {
	query := LocalMemoryPacketQuery{
		Scope: LocalMemoryScope{
			TaskID:         packetAnchorTaskID(input),
			TensionID:      packetAnchorTensionID(input),
			ProtoClusterID: packetAnchorClusterID(input),
		},
		DocKeys:      memoryPacketDocKeys(input),
		ArtifactRefs: memoryPacketArtifactRefs(input),
		RecentLimit:  8,
	}
	if input.Session != nil {
		query.Scope.SessionID = strings.TrimSpace(input.Session.SessionID)
	}
	if input.WorkPacket != nil && len(input.WorkPacket.Blockers) > 0 {
		query.ConstraintRefs = make([]string, 0, len(input.WorkPacket.Blockers))
		for _, blocker := range input.WorkPacket.Blockers {
			if ref := strings.TrimSpace(blocker.Kind); ref != "" {
				query.ConstraintRefs = append(query.ConstraintRefs, ref)
			}
		}
	}
	return query
}

func mergePacketEventsWithView(hot []LocalMemoryEvent, view LocalMemoryPacketView) []LocalMemoryEvent {
	merged := append([]LocalMemoryEvent(nil), hot...)
	for _, episode := range view.Episodes {
		merged = append(merged, episodeRecordToMemoryEvent(episode, 0))
	}
	return dedupePacketEvents(merged)
}

func downgradeStaleLocalMemoryPacketView(view LocalMemoryPacketView) LocalMemoryPacketView {
	return LocalMemoryPacketView{
		StaleDigestIDs: cloneStrings(view.StaleDigestIDs),
		UpdatedAt:      strings.TrimSpace(view.UpdatedAt),
	}
}

func dedupePacketEvents(events []LocalMemoryEvent) []LocalMemoryEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]LocalMemoryEvent, 0, len(events))
	seen := map[string]struct{}{}
	for _, event := range events {
		key := strings.Join([]string{
			strings.TrimSpace(event.OccurredAt),
			strings.TrimSpace(event.EventKind),
			strings.TrimSpace(event.TaskID),
			strings.TrimSpace(event.SessionID),
			strings.TrimSpace(event.TensionID),
			strings.TrimSpace(event.ProtoClusterID),
			strings.TrimSpace(event.Summary),
			strings.TrimSpace(event.Details),
		}, "|")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, event)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return compareLocalMemoryTimestamps(out[i].OccurredAt, out[j].OccurredAt) < 0
	})
	if len(out) > agentMemoryRecentLimit {
		out = out[len(out)-agentMemoryRecentLimit:]
	}
	return out
}

func mergePacketDigestMaps(hot map[string]LocalEpisodeDigest, persistent *LocalMemoryDigestRecord) map[string]LocalEpisodeDigest {
	merged := cloneEpisodeDigestMap(hot)
	if persistent == nil {
		return merged
	}
	digest, ok := digestRecordToEpisodeDigest(*persistent)
	if !ok || strings.TrimSpace(digest.ScopeKey) == "" {
		return merged
	}
	existing, hasExisting := merged[digest.ScopeKey]
	if !hasExisting || compareLocalMemoryTimestamps(digest.UpdatedAt, existing.UpdatedAt) > 0 {
		merged[digest.ScopeKey] = digest
	}
	return merged
}

func packetAnchorTaskID(input MemoryPacketInput) string {
	if input.Task == nil {
		return ""
	}
	return strings.TrimSpace(input.Task.TaskID)
}

func packetAnchorClusterID(input MemoryPacketInput) string {
	if input.Focus != nil && strings.TrimSpace(input.Focus.ProtoClusterID) != "" {
		return strings.TrimSpace(input.Focus.ProtoClusterID)
	}
	if input.WorkPacket != nil && input.WorkPacket.Advisory != nil {
		return strings.TrimSpace(input.WorkPacket.Advisory.ProtoClusterID)
	}
	return ""
}

func packetAnchorTensionID(input MemoryPacketInput) string {
	if input.Focus == nil {
		return ""
	}
	return strings.TrimSpace(input.Focus.FocusTensionID)
}

func cloneEpisodeDigestMap(src map[string]LocalEpisodeDigest) map[string]LocalEpisodeDigest {
	if len(src) == 0 {
		return map[string]LocalEpisodeDigest{}
	}
	out := make(map[string]LocalEpisodeDigest, len(src))
	for key, value := range src {
		copy := value
		copy.RecentSummaries = cloneStrings(copy.RecentSummaries)
		copy.ArtifactRefs = cloneStrings(copy.ArtifactRefs)
		copy.DocKeys = cloneStrings(copy.DocKeys)
		copy.BlockerKinds = cloneStrings(copy.BlockerKinds)
		copy.OpenLoops = cloneStrings(copy.OpenLoops)
		copy.DissentSummaries = cloneStrings(copy.DissentSummaries)
		copy.ConstraintRefs = cloneStrings(copy.ConstraintRefs)
		out[key] = copy
	}
	return out
}

func memoryPacketKey(input MemoryPacketInput, artifactVersions map[string]string) string {
	type memoryPacketSignature struct {
		TaskID         string   `json:"task_id,omitempty"`
		SessionID      string   `json:"session_id,omitempty"`
		TensionID      string   `json:"tension_id,omitempty"`
		ProtoClusterID string   `json:"proto_cluster_id,omitempty"`
		ArtifactRefs   []string `json:"artifact_refs,omitempty"`
		ArtifactVers   []string `json:"artifact_versions,omitempty"`
		DocKeys        []string `json:"doc_keys,omitempty"`
		ConstraintRefs []string `json:"constraint_refs,omitempty"`
		BlockerKinds   []string `json:"blocker_kinds,omitempty"`
	}
	sig := memoryPacketSignature{
		ArtifactRefs: uniqueTrimmedFocusStrings(memoryPacketArtifactRefs(input)),
		DocKeys:      uniqueTrimmedFocusStrings(memoryPacketDocKeys(input)),
	}
	for _, ref := range sig.ArtifactRefs {
		artifactRef := strings.TrimSpace(ref)
		version := strings.TrimSpace(artifactVersions[artifactRef])
		if artifactRef == "" || version == "" {
			continue
		}
		sig.ArtifactVers = append(sig.ArtifactVers, artifactRef+"@"+version)
	}
	if input.Task != nil {
		sig.TaskID = strings.TrimSpace(input.Task.TaskID)
	}
	if input.Session != nil {
		sig.SessionID = strings.TrimSpace(input.Session.SessionID)
	}
	if input.Focus != nil {
		sig.TensionID = strings.TrimSpace(input.Focus.FocusTensionID)
		sig.ProtoClusterID = strings.TrimSpace(input.Focus.ProtoClusterID)
		sig.ConstraintRefs = uniqueTrimmedFocusStrings([]string{input.Focus.FocusTensionAnchorRef})
	}
	if input.WorkPacket != nil {
		blockers := make([]string, 0, len(input.WorkPacket.Blockers))
		for _, blocker := range input.WorkPacket.Blockers {
			blockers = append(blockers, strings.TrimSpace(blocker.Kind))
		}
		sig.BlockerKinds = uniqueTrimmedFocusStrings(blockers)
		if sig.ProtoClusterID == "" && input.WorkPacket.Advisory != nil {
			sig.ProtoClusterID = strings.TrimSpace(input.WorkPacket.Advisory.ProtoClusterID)
		}
	}
	raw, err := json.Marshal(sig)
	if err != nil {
		return fmt.Sprintf("%s|%s|%s|%s", sig.TaskID, sig.SessionID, sig.TensionID, sig.ProtoClusterID)
	}
	return string(raw)
}

func memoryPacketArtifactRefs(input MemoryPacketInput) []string {
	refs := []string{}
	if input.Hydration != nil {
		for _, artifact := range input.Hydration.Artifacts {
			refs = append(refs, strings.TrimSpace(artifact.ArtifactRef))
		}
	}
	if input.Focus != nil {
		refs = append(refs, input.Focus.ClusterArtifactRefs...)
		refs = append(refs, input.Focus.FocusTensionArtifactRefs...)
	}
	if input.WorkPacket != nil {
		refs = append(refs, input.WorkPacket.ContextHints.RelatedArtifactRefs...)
	}
	return uniqueTrimmedFocusStrings(refs)
}

func memoryPacketDocKeys(input MemoryPacketInput) []string {
	keys := []string{}
	if input.Hydration != nil {
		keys = append(keys, hydrationDocKeys(input.Hydration)...)
	}
	if input.Focus != nil {
		keys = append(keys, input.Focus.ClusterDocKeys...)
		keys = append(keys, input.Focus.FocusTensionDocKeys...)
	}
	if input.WorkPacket != nil {
		keys = append(keys, input.WorkPacket.ContextHints.SuggestedDocKeys...)
	}
	return uniqueTrimmedFocusStrings(keys)
}

func compileMemoryPacket(events []LocalMemoryEvent, taskDigests, tensionDigests, clusterDigests map[string]LocalEpisodeDigest, procedures, antiProcedures map[string]LocalProcedureMemory, constitution AgentAnatomyConstitution, artifactVersions map[string]string, stats LocalMemoryStats, freshness localMemoryPacketFreshness, input MemoryPacketInput, maxChars int) string {
	taskID := ""
	sessionID := ""
	tensionID := ""
	clusterID := ""
	if input.Task != nil {
		taskID = strings.TrimSpace(input.Task.TaskID)
	}
	if input.Session != nil {
		sessionID = strings.TrimSpace(input.Session.SessionID)
	}
	if input.Focus != nil {
		tensionID = strings.TrimSpace(input.Focus.FocusTensionID)
		clusterID = strings.TrimSpace(input.Focus.ProtoClusterID)
	}
	if clusterID == "" && input.WorkPacket != nil && input.WorkPacket.Advisory != nil {
		clusterID = strings.TrimSpace(input.WorkPacket.Advisory.ProtoClusterID)
	}

	taskDigest, hasTaskDigest := taskDigests[taskID]
	tensionDigest, hasTensionDigest := tensionDigests[tensionID]
	clusterDigest, hasClusterDigest := clusterDigests[clusterID]
	recent := selectAnchoredMemoryEvents(events, taskID, sessionID, tensionID, clusterID)
	if len(recent) == 0 && len(events) > 0 {
		recent = append([]LocalMemoryEvent(nil), events[max(0, len(events)-4):]...)
	}
	docKeys := memoryPacketDocKeys(input)
	artifactRefs := memoryPacketArtifactRefs(input)
	localProcedures := selectAnchoredProcedureMemories(procedures, taskID, sessionID, tensionID, clusterID, docKeys, artifactRefs)
	localAntiProcedures := selectAnchoredProcedureMemories(antiProcedures, taskID, sessionID, tensionID, clusterID, docKeys, artifactRefs)

	var b strings.Builder
	b.WriteString("## Agent Memory Body\n")
	b.WriteString(fmt.Sprintf("- Local Event Count: %d\n", stats.TotalEvents))
	if taskID != "" || clusterID != "" || tensionID != "" {
		b.WriteString(fmt.Sprintf("- Memory Anchors: task=%s | cluster=%s | tension=%s\n", firstNonEmpty(taskID, "-"), firstNonEmpty(clusterID, "-"), firstNonEmpty(tensionID, "-")))
	}
	writeMemoryFreshnessGuardSection(&b, freshness)
	writeKernelSection(&b, input, taskDigest, tensionDigest, clusterDigest, artifactVersions)
	if constitution.Enabled != nil && *constitution.Enabled && constitution.Promotion != "disabled" {
		maxRules := constitution.MaxRules
		if maxRules <= 0 {
			maxRules = constitutionDefaultMaxRules
		}
		seeds := constitution.SeedRules
		var derivedProcedures, derivedAntiProcedures map[string]LocalProcedureMemory
		if constitution.Promotion != "seed_only" {
			derivedProcedures = procedures
			derivedAntiProcedures = antiProcedures
		}
		writeConstitutionSection(&b, selectConstitutionRules(derivedProcedures, derivedAntiProcedures, seeds, maxRules))
	}
	writeDifferentialShellSection(&b, taskDigest, tensionDigest, clusterDigest, recent, localProcedures, localAntiProcedures)
	writeEpisodicTailSection(&b, taskDigest, tensionDigest, clusterDigest, recent, hasTaskDigest, hasTensionDigest, hasClusterDigest)
	return clipForPrompt(strings.TrimSpace(b.String()), maxChars)
}

func writeMemoryFreshnessGuardSection(b *strings.Builder, freshness localMemoryPacketFreshness) {
	if !freshness.PersistentViewDowngraded && len(freshness.StaleDigestIDs) == 0 {
		return
	}
	b.WriteString("\n### Freshness Guard\n")
	if freshness.PersistentViewDowngraded {
		b.WriteString("- Persistent P2 view: downgraded; stale digests were ignored.\n")
	} else {
		b.WriteString("- Persistent P2 view: watch; stale digests were observed.\n")
	}
	if len(freshness.StaleDigestIDs) > 0 {
		ids := uniqueTrimmedFocusStrings(freshness.StaleDigestIDs)
		b.WriteString(fmt.Sprintf("- Stale Digest IDs: %s\n", strings.Join(ids[:min(len(ids), 6)], ", ")))
	}
	b.WriteString("- Action: rely on live work packet, hot local events, and repaired memory before treating P2 context as authoritative.\n")
}

func writeKernelSection(b *strings.Builder, input MemoryPacketInput, taskDigest, tensionDigest, clusterDigest LocalEpisodeDigest, artifactVersions map[string]string) {
	b.WriteString("\n### Shared Kernel\n")
	if input.Task != nil {
		b.WriteString(fmt.Sprintf("- Active Task: %s\n", firstNonEmpty(strings.TrimSpace(input.Task.Title), strings.TrimSpace(input.Task.TaskID), "unknown")))
		b.WriteString(fmt.Sprintf("- Task Status: %s | Priority: %s\n", firstNonEmpty(strings.TrimSpace(input.Task.Status), "unknown"), firstNonEmpty(strings.TrimSpace(input.Task.Priority), "unknown")))
	}
	if input.WorkPacket != nil {
		b.WriteString(fmt.Sprintf("- Work Type: %s | Why Now: %s\n", firstNonEmpty(strings.TrimSpace(input.WorkPacket.WorkType), "unknown"), firstNonEmpty(oneLine(input.WorkPacket.WhyNow), "unspecified")))
		if input.WorkPacket.Handoff != nil {
			b.WriteString(fmt.Sprintf("- Last Handoff: %s\n", firstNonEmpty(oneLine(input.WorkPacket.Handoff.Summary), oneLine(taskDigest.HandoffSummary), oneLine(tensionDigest.HandoffSummary), oneLine(clusterDigest.HandoffSummary), "none")))
		}
		if len(input.WorkPacket.Blockers) > 0 {
			parts := make([]string, 0, len(input.WorkPacket.Blockers))
			for _, blocker := range input.WorkPacket.Blockers {
				parts = append(parts, firstNonEmpty(oneLine(blocker.Kind+": "+blocker.Detail), oneLine(blocker.Kind), oneLine(blocker.Detail)))
			}
			b.WriteString(fmt.Sprintf("- Active Blockers: %s\n", strings.Join(uniqueTrimmedFocusStrings(parts), " | ")))
		}
	}
	docKeys := memoryPacketDocKeys(input)
	if len(docKeys) > 0 {
		b.WriteString(fmt.Sprintf("- Hot Docs: %s\n", strings.Join(docKeys[:min(len(docKeys), 5)], ", ")))
	}
	artifactRefs := memoryPacketArtifactRefs(input)
	if len(artifactRefs) > 0 {
		b.WriteString(fmt.Sprintf("- Artifact Neighborhood: %s\n", strings.Join(artifactRefs[:min(len(artifactRefs), 4)], ", ")))
		if currentVersions := formatVersionedArtifactRefs(artifactRefs, artifactVersions, 4); len(currentVersions) > 0 {
			b.WriteString(fmt.Sprintf("- Current Artifact Versions: %s\n", strings.Join(currentVersions, " | ")))
		}
	}
	if input.Focus != nil {
		b.WriteString(fmt.Sprintf("- Locus: cluster=%s | corridor=%s | attention=%s\n", firstNonEmpty(strings.TrimSpace(input.Focus.ProtoClusterID), "-"), firstNonEmpty(strings.TrimSpace(input.Focus.CorridorReadiness), "-"), firstNonEmpty(strings.TrimSpace(input.Focus.ControlAttentionBand), "-")))
	}
}

func formatVersionedArtifactRefs(refs []string, versions map[string]string, limit int) []string {
	if len(refs) == 0 || len(versions) == 0 {
		return nil
	}
	out := make([]string, 0, min(limit, len(refs)))
	for _, ref := range refs {
		artifactRef := strings.TrimSpace(ref)
		if artifactRef == "" {
			continue
		}
		version := strings.TrimSpace(versions[artifactRef])
		if version == "" {
			continue
		}
		out = append(out, fmt.Sprintf("%s @ %s", artifactRef, abbreviateMemoryVersion(version)))
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func abbreviateMemoryVersion(version string) string {
	version = strings.TrimSpace(version)
	if len(version) <= 16 {
		return version
	}
	return version[:16]
}

// writeConstitutionSection renders the always-on, anchor-independent rule tier
// (Layer A). It is placed before the Differential Shell so global rules are the
// stable backdrop while the shell stays the task-local foreground. Anti/invariant
// rules carry CRITICAL framing; procedures are rendered as preferences.
func writeConstitutionSection(b *strings.Builder, rules []ConstitutionRule) {
	if len(rules) == 0 {
		return
	}
	b.WriteString("\n### Constitution (Always-On Rules)\n")
	for _, rule := range rules {
		text := oneLine(rule.Text)
		if text == "" {
			continue
		}
		var label string
		switch rule.Kind {
		case "anti_procedure":
			label = "[avoid] " + text
		case "invariant":
			label = "[invariant] " + text
		default:
			label = "[prefer] " + text
		}
		suffix := ""
		if rule.Seed {
			suffix = " (seed)"
		} else if rule.EvidenceCount > 1 {
			suffix = fmt.Sprintf(" (x%d)", rule.EvidenceCount)
		}
		b.WriteString(fmt.Sprintf("- %s%s\n", label, suffix))
	}
}

func writeDifferentialShellSection(b *strings.Builder, taskDigest, tensionDigest, clusterDigest LocalEpisodeDigest, events []LocalMemoryEvent, procedures, antiProcedures []LocalProcedureMemory) {
	b.WriteString("\n### Differential Shell\n")
	if rendered := renderProcedureHints(procedures, 3); len(rendered) > 0 {
		b.WriteString(fmt.Sprintf("- Repeatedly Works Here: %s\n", strings.Join(rendered, " | ")))
	}
	if rendered := renderProcedureHints(antiProcedures, 3); len(rendered) > 0 {
		b.WriteString(fmt.Sprintf("- Avoid Repeating: %s\n", strings.Join(rendered, " | ")))
	}
	cautions := collectCautionSummaries(events, taskDigest, tensionDigest, clusterDigest)
	if len(cautions) > 0 {
		b.WriteString(fmt.Sprintf("- Cautions: %s\n", strings.Join(cautions[:min(len(cautions), 3)], " | ")))
	}
	dissent := uniqueTrimmedFocusStrings(append(append(cloneStrings(taskDigest.DissentSummaries), tensionDigest.DissentSummaries...), clusterDigest.DissentSummaries...))
	if len(dissent) > 0 {
		b.WriteString(fmt.Sprintf("- Preserve Dissent: %s\n", strings.Join(dissent[:min(len(dissent), 3)], " | ")))
	}
	if len(taskDigest.BlockerKinds) > 0 || len(tensionDigest.BlockerKinds) > 0 || len(clusterDigest.BlockerKinds) > 0 {
		blockers := uniqueTrimmedFocusStrings(append(append(cloneStrings(taskDigest.BlockerKinds), tensionDigest.BlockerKinds...), clusterDigest.BlockerKinds...))
		b.WriteString(fmt.Sprintf("- Repeated Blocker Kinds: %s\n", strings.Join(blockers[:min(len(blockers), 4)], ", ")))
	}
	if len(renderProcedureHints(procedures, 1)) == 0 && len(renderProcedureHints(antiProcedures, 1)) == 0 && len(cautions) == 0 && len(dissent) == 0 && len(taskDigest.BlockerKinds) == 0 && len(tensionDigest.BlockerKinds) == 0 && len(clusterDigest.BlockerKinds) == 0 {
		b.WriteString("- No special local caution recorded yet.\n")
	}
}

func writeEpisodicTailSection(b *strings.Builder, taskDigest, tensionDigest, clusterDigest LocalEpisodeDigest, events []LocalMemoryEvent, hasTaskDigest, hasTensionDigest, hasClusterDigest bool) {
	b.WriteString("\n### Episodic Tail\n")
	if hasTaskDigest {
		b.WriteString(fmt.Sprintf("- Task Digest: events=%d | messages=%d | last=%s\n", taskDigest.EventCount, taskDigest.MessageCount, firstNonEmpty(oneLine(taskDigest.LastSummary), "none")))
		if len(taskDigest.OpenLoops) > 0 {
			b.WriteString(fmt.Sprintf("- Open Loops: %s\n", strings.Join(taskDigest.OpenLoops[:min(len(taskDigest.OpenLoops), 3)], " | ")))
		}
	}
	if hasTensionDigest {
		b.WriteString(fmt.Sprintf("- Tension Digest: events=%d | last=%s\n", tensionDigest.EventCount, firstNonEmpty(oneLine(tensionDigest.LastSummary), "none")))
	}
	if hasClusterDigest {
		b.WriteString(fmt.Sprintf("- Cluster Digest: events=%d | last=%s\n", clusterDigest.EventCount, firstNonEmpty(oneLine(clusterDigest.LastSummary), "none")))
	}
	if len(events) == 0 {
		b.WriteString("- No local episodic events captured yet.\n")
		return
	}
	b.WriteString("- Recent Local Events:\n")
	for _, event := range lastMemoryEvents(events, 4) {
		label := firstNonEmpty(event.EventKind, string(event.NodeType), "event")
		b.WriteString(fmt.Sprintf("  - %s | %s\n", label, firstNonEmpty(oneLine(event.Summary), oneLine(event.Details), "no summary")))
	}
}

func lastMemoryEvents(events []LocalMemoryEvent, limit int) []LocalMemoryEvent {
	if len(events) <= limit {
		return append([]LocalMemoryEvent(nil), events...)
	}
	return append([]LocalMemoryEvent(nil), events[len(events)-limit:]...)
}

func collectCautionSummaries(events []LocalMemoryEvent, taskDigest, tensionDigest, clusterDigest LocalEpisodeDigest) []string {
	out := uniqueTrimmedFocusStrings(append(append(cloneStrings(taskDigest.OpenLoops), tensionDigest.OpenLoops...), clusterDigest.OpenLoops...))
	for _, event := range events {
		if strings.EqualFold(strings.TrimSpace(event.Outcome), "blocked") || event.RequiresHuman {
			out = pushLimitedString(out, firstNonEmpty(event.Summary, event.Details), 6)
		}
	}
	sort.Strings(out)
	return out
}

func selectAnchoredMemoryEvents(events []LocalMemoryEvent, taskID, sessionID, tensionID, clusterID string) []LocalMemoryEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]LocalMemoryEvent, 0, len(events))
	for _, event := range events {
		switch {
		case taskID != "" && strings.TrimSpace(event.TaskID) == taskID:
			out = append(out, event)
		case sessionID != "" && strings.TrimSpace(event.SessionID) == sessionID:
			out = append(out, event)
		case tensionID != "" && strings.TrimSpace(event.TensionID) == tensionID:
			out = append(out, event)
		case clusterID != "" && strings.TrimSpace(event.ProtoClusterID) == clusterID:
			out = append(out, event)
		}
	}
	return out
}
