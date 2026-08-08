package main

import (
	"encoding/json"
	"log"
	"strings"
)

func (r *Runtime) memoryService() *AgentMemoryService {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.memory
}

func (r *Runtime) buildMemoryPacket(task *WorkspaceTaskRecord, hydration *TaskHydrationBundle, focus *RuntimeFocusState) string {
	service := r.memoryService()
	if service == nil {
		return ""
	}
	r.syncMemoryCanonicalVersions(hydration)
	return service.buildPacket(MemoryPacketInput{
		Task:           task,
		Session:        r.currentSessionCopy(),
		Focus:          focus,
		Hydration:      hydration,
		WorkPacket:     r.currentWorkPacketCopy(),
		CurrentSummary: r.currentSummary(),
	}, max(1400, r.cfg.MaxPromptSpecChars/3))
}

func (r *Runtime) syncMemoryCanonicalVersions(hydration *TaskHydrationBundle) {
	service := r.memoryService()
	if service == nil {
		return
	}
	r.mu.Lock()
	docVersions := cloneStringMap(r.scratch.DocSHAs)
	r.mu.Unlock()
	artifactVersions := memoryArtifactVersionsFromHydration(hydration)
	prevDocVersions, prevArtifactVersions := service.canonicalVersionSnapshot()
	if err := service.rememberDocVersions(docVersions); err != nil {
		log.Printf("[memory] sync doc versions degraded: %v", err)
	}
	if err := service.rememberArtifactVersions(artifactVersions); err != nil {
		log.Printf("[memory] sync artifact versions degraded: %v", err)
	}
	invalidation := buildCanonicalVersionInvalidation(docVersions, artifactVersions, prevDocVersions, prevArtifactVersions)
	if len(invalidation.GuardChanges) > 0 {
		r.invalidateMemory(invalidation)
	}
}

func (r *Runtime) rememberMemoryDocVersion(docKey, version string) {
	service := r.memoryService()
	if service == nil {
		return
	}
	if err := service.rememberDocVersions(map[string]string{strings.TrimSpace(docKey): strings.TrimSpace(version)}); err != nil {
		log.Printf("[memory] remember doc version degraded: %v", err)
	}
}

func (r *Runtime) rememberMemoryArtifactVersion(artifactRef, version string) {
	service := r.memoryService()
	if service == nil {
		return
	}
	if err := service.rememberArtifactVersions(map[string]string{strings.TrimSpace(artifactRef): strings.TrimSpace(version)}); err != nil {
		log.Printf("[memory] remember artifact version degraded: %v", err)
	}
}

func (r *Runtime) memoryStatsSnapshot() LocalMemoryStats {
	service := r.memoryService()
	if service == nil {
		return LocalMemoryStats{}
	}
	return service.statsSnapshot()
}

func (r *Runtime) memoryStoreStatsSnapshot() LocalMemoryStoreStats {
	service := r.memoryService()
	if service == nil {
		return LocalMemoryStoreStats{}
	}
	return service.storeStatsSnapshot()
}

func (r *Runtime) logMemoryEvent(event LocalMemoryEvent) {
	service := r.memoryService()
	if service == nil {
		return
	}
	r.syncMemoryCanonicalVersions(r.currentHydrationCopy())
	if err := service.appendEvent(event); err != nil {
		log.Printf("[memory] append event degraded: %v", err)
	}
}

func (r *Runtime) queueMemoryPromotion(candidate LocalPromotionCandidate) {
	service := r.memoryService()
	if service == nil {
		return
	}
	if err := service.queuePromotion(candidate); err != nil {
		log.Printf("[memory] queue promotion degraded: %v", err)
	}
}

func (r *Runtime) markMemoryPromotionsPromoted(sourceID string) {
	service := r.memoryService()
	if service == nil {
		return
	}
	if err := service.markPromotionsPromotedBySource(sourceID); err != nil {
		log.Printf("[memory] mark promotions promoted degraded: %v", err)
	}
}

func (r *Runtime) invalidateMemory(input LocalMemoryInvalidationInput) {
	service := r.memoryService()
	if service == nil {
		return
	}
	if err := service.invalidate(input); err != nil {
		log.Printf("[memory] invalidate degraded: %v", err)
	}
}

func memoryArtifactVersionsFromHydration(bundle *TaskHydrationBundle) map[string]string {
	if bundle == nil || len(bundle.Artifacts) == 0 {
		return map[string]string{}
	}
	versions := make(map[string]string, len(bundle.Artifacts))
	for _, artifact := range bundle.Artifacts {
		artifactRef := strings.TrimSpace(artifact.ArtifactRef)
		artifactID := strings.TrimSpace(artifact.ArtifactID)
		if artifactRef == "" || artifactID == "" {
			continue
		}
		versions[artifactRef] = artifactID
	}
	return versions
}

func buildCanonicalVersionInvalidation(docVersions, artifactVersions, prevDocVersions, prevArtifactVersions map[string]string) LocalMemoryInvalidationInput {
	docKeys := []string{}
	artifactRefs := []string{}
	guardChanges := []LocalMemoryGuardChange{}

	for docKey, version := range docVersions {
		docKey = strings.TrimSpace(docKey)
		version = strings.TrimSpace(version)
		prevVersion := strings.TrimSpace(prevDocVersions[docKey])
		if docKey == "" || version == "" || prevVersion == "" || prevVersion == version {
			continue
		}
		docKeys = append(docKeys, docKey)
		guardChanges = append(guardChanges, LocalMemoryGuardChange{
			GuardType:      "doc_sha",
			Ref:            docKey,
			CurrentVersion: version,
		})
	}
	for artifactRef, version := range artifactVersions {
		artifactRef = strings.TrimSpace(artifactRef)
		version = strings.TrimSpace(version)
		prevVersion := strings.TrimSpace(prevArtifactVersions[artifactRef])
		if artifactRef == "" || version == "" || prevVersion == "" || prevVersion == version {
			continue
		}
		artifactRefs = append(artifactRefs, artifactRef)
		guardChanges = append(guardChanges, LocalMemoryGuardChange{
			GuardType:      "artifact_version",
			Ref:            artifactRef,
			CurrentVersion: version,
		})
	}

	return LocalMemoryInvalidationInput{
		Reasons:      []string{"canonical version updated"},
		DocKeys:      uniqueTrimmedFocusStrings(docKeys),
		ArtifactRefs: uniqueTrimmedFocusStrings(artifactRefs),
		GuardChanges: guardChanges,
	}
}

func (r *Runtime) logInboundMessageMemory(message MessageRecord, taskID string) {
	clusterID, tensionID := currentFocusAnchors(r.currentFocusCopy())
	metadata := map[string]any{
		"from_agent_id": strings.TrimSpace(message.FromAgentID),
		"to_agent_id":   strings.TrimSpace(message.ToAgentID),
		"channel":       strings.TrimSpace(message.Channel),
	}
	raw, _ := json.Marshal(metadata)
	r.logMemoryEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawMessage,
		EventKind:      "inbound_message",
		Summary:        firstNonEmpty(oneLine(message.Content), "inbound message"),
		Details:        strings.TrimSpace(message.Content),
		TaskID:         taskID,
		SessionID:      r.currentSessionID(),
		TensionID:      tensionID,
		ProtoClusterID: clusterID,
		SourceID:       strings.TrimSpace(message.MessageID),
		MetadataJSON:   string(raw),
	})
}

func (r *Runtime) logNewsMemory(item NewsRecord, summary string) {
	clusterID, tensionID := currentFocusAnchors(r.currentFocusCopy())
	metadata := map[string]any{
		"title":       strings.TrimSpace(item.Title),
		"author_id":   strings.TrimSpace(item.AuthorID),
		"author_type": strings.TrimSpace(item.AuthorType),
	}
	raw, _ := json.Marshal(metadata)
	r.logMemoryEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      "system_news",
		Summary:        summary,
		Details:        strings.TrimSpace(item.Content),
		TaskID:         r.currentTaskID(),
		SessionID:      r.currentSessionID(),
		TensionID:      tensionID,
		ProtoClusterID: clusterID,
		SourceID:       strings.TrimSpace(item.NewsID),
		MetadataJSON:   string(raw),
	})
}

func (r *Runtime) logRequestMemory(request AgentRequestRecord) {
	clusterID, tensionID := currentFocusAnchors(r.currentFocusCopy())
	r.logMemoryEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      "request",
		Summary:        firstNonEmpty(strings.TrimSpace(request.Method), "request"),
		Details:        strings.TrimSpace(request.Payload),
		TaskID:         r.currentTaskID(),
		SessionID:      r.currentSessionID(),
		TensionID:      tensionID,
		ProtoClusterID: clusterID,
		SourceID:       strings.TrimSpace(request.RequestID),
	})
}

func (r *Runtime) logTaskCycleStartMemory(task WorkspaceTaskRecord, session *AgentSessionStateRecord, runID string) {
	focus := r.currentFocusCopy()
	clusterID, tensionID := currentFocusAnchors(focus)
	packetInput := MemoryPacketInput{
		Task:       &task,
		Session:    session,
		Hydration:  r.currentHydrationCopy(),
		Focus:      focus,
		WorkPacket: r.currentWorkPacketCopy(),
	}
	r.logMemoryEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      "task_cycle_start",
		Summary:        firstNonEmpty(strings.TrimSpace(task.Title), strings.TrimSpace(task.Description), task.TaskID),
		TaskID:         strings.TrimSpace(task.TaskID),
		SessionID:      sessionIDOrCurrent(session, r),
		TensionID:      tensionID,
		ProtoClusterID: clusterID,
		ArtifactRefs:   memoryPacketArtifactRefs(packetInput),
		DocKeys:        memoryPacketDocKeys(packetInput),
		SourceID:       strings.TrimSpace(runID),
	})
}

func (r *Runtime) logTaskResultMemory(task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult) {
	focus := r.currentFocusCopy()
	clusterID, tensionID := currentFocusAnchors(focus)
	nodeType := localMemoryNodeEpisodePack
	switch normalizeOutcome(result.Outcome) {
	case "blocked":
		nodeType = localMemoryNodeBlocker
	case "completed":
		nodeType = localMemoryNodeArtifactDelta
	}
	if result.MemoryType != "" {
		switch strings.ToUpper(strings.TrimSpace(result.MemoryType)) {
		case "DECISION":
			nodeType = localMemoryNodeDecision
		case "PROCEDURE":
			nodeType = localMemoryNodeProcedure
		case "ANTI_PROCEDURE":
			nodeType = localMemoryNodeAntiProcedure
		case "INCIDENT":
			nodeType = localMemoryNodeBlocker
		}
	}
	blockers := make([]string, 0, len(result.BlockedOn))
	for _, blocker := range result.BlockedOn {
		blockers = append(blockers, strings.TrimSpace(blocker.Kind))
	}
	artifactRefs := resultArtifactRefs(result)
	artifactRefStrings := make([]string, 0, len(artifactRefs))
	for _, artifact := range artifactRefs {
		artifactRefStrings = append(artifactRefStrings, strings.TrimSpace(artifact.Ref))
	}
	docKeys := []string{}
	if strings.TrimSpace(result.Materialize.DocKey) != "" {
		docKeys = append(docKeys, strings.TrimSpace(result.Materialize.DocKey))
	}
	metadataJSON := encodeTaskResultMemoryMetadata(result)
	r.logMemoryEvent(LocalMemoryEvent{
		NodeType:       nodeType,
		EventKind:      "task_cycle_result",
		Summary:        strings.TrimSpace(result.Summary),
		Details:        firstNonEmpty(strings.TrimSpace(result.Details), strings.TrimSpace(result.NextAction)),
		TaskID:         strings.TrimSpace(task.TaskID),
		SessionID:      strings.TrimSpace(session.SessionID),
		TensionID:      tensionID,
		ProtoClusterID: clusterID,
		ArtifactRefs:   artifactRefStrings,
		DocKeys:        docKeys,
		BlockerKinds:   blockers,
		Outcome:        normalizeOutcome(result.Outcome),
		SourceID:       strings.TrimSpace(runID),
		RequiresHuman:  result.RequiresHuman,
		MetadataJSON:   metadataJSON,
	})
	if candidate := deriveTaskResultPromotionCandidate(task, session, runID, result, nodeType, tensionID, clusterID, artifactRefStrings, docKeys); candidate != nil {
		r.queueMemoryPromotion(*candidate)
	}
}

func currentFocusAnchors(focus *RuntimeFocusState) (clusterID, tensionID string) {
	if focus == nil {
		return "", ""
	}
	return strings.TrimSpace(focus.ProtoClusterID), strings.TrimSpace(focus.FocusTensionID)
}

func sessionIDOrCurrent(session *AgentSessionStateRecord, runtime *Runtime) string {
	if session != nil {
		return strings.TrimSpace(session.SessionID)
	}
	if runtime == nil {
		return ""
	}
	return runtime.currentSessionID()
}

func (r *Runtime) currentSessionID() string {
	if session := r.currentSessionCopy(); session != nil {
		return strings.TrimSpace(session.SessionID)
	}
	return ""
}

func (r *Runtime) currentTaskID() string {
	if task := r.currentTaskCopy(); task != nil {
		return strings.TrimSpace(task.TaskID)
	}
	return ""
}

func (r *Runtime) logSessionTransitionMemory(eventKind, taskID, sessionID, status, summary, decisionType, handoffTo string, blockedOn []BlockedRef) {
	focus := r.currentFocusCopy()
	clusterID, tensionID := currentFocusAnchors(focus)
	nodeType := localMemoryNodeEpisodePack
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "WAITING_DECISION":
		nodeType = localMemoryNodeDecision
	case "BLOCKED":
		nodeType = localMemoryNodeBlocker
	case "HANDOFF_PENDING":
		nodeType = localMemoryNodeHandoff
	}
	blockerKinds := make([]string, 0, len(blockedOn))
	for _, blocker := range blockedOn {
		blockerKinds = append(blockerKinds, strings.TrimSpace(blocker.Kind))
	}
	details := []string{}
	if decisionType = strings.TrimSpace(decisionType); decisionType != "" {
		details = append(details, "decision_type="+decisionType)
	}
	if handoffTo = strings.TrimSpace(handoffTo); handoffTo != "" {
		details = append(details, "handoff_to="+handoffTo)
	}
	r.logMemoryEvent(LocalMemoryEvent{
		NodeType:       nodeType,
		EventKind:      strings.TrimSpace(eventKind),
		Summary:        firstNonEmpty(strings.TrimSpace(summary), strings.TrimSpace(status), strings.TrimSpace(eventKind)),
		Details:        strings.Join(details, "\n"),
		TaskID:         strings.TrimSpace(taskID),
		SessionID:      strings.TrimSpace(sessionID),
		TensionID:      tensionID,
		ProtoClusterID: clusterID,
		BlockerKinds:   uniqueTrimmedFocusStrings(blockerKinds),
		Outcome:        strings.ToLower(strings.TrimSpace(status)),
		SourceID:       strings.TrimSpace(sessionID),
	})
}

func (r *Runtime) logRecoveryMemory(scope string, cause error) {
	message := ""
	if cause != nil {
		message = strings.TrimSpace(cause.Error())
	}
	focus := r.currentFocusCopy()
	clusterID, tensionID := currentFocusAnchors(focus)
	r.logMemoryEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      "runtime_recovery",
		Summary:        firstNonEmpty(strings.TrimSpace(scope), "runtime recovery"),
		Details:        message,
		TaskID:         r.currentTaskID(),
		SessionID:      r.currentSessionID(),
		TensionID:      tensionID,
		ProtoClusterID: clusterID,
		SourceID:       strings.TrimSpace(scope),
	})
}
