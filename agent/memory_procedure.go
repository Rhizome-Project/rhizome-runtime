package main

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type localTaskResultMemoryMetadata struct {
	MemoryType      string       `json:"memory_type,omitempty"`
	NextAction      string       `json:"next_action,omitempty"`
	OwnerAction     string       `json:"owner_action,omitempty"`
	HumanReason     string       `json:"human_reason,omitempty"`
	DecisionType    string       `json:"decision_type,omitempty"`
	MaterializedDoc string       `json:"materialized_doc,omitempty"`
	BlockedOn       []BlockedRef `json:"blocked_on,omitempty"`
}

type localProcedureEvidenceClass struct {
	kind     localMemoryNodeType
	explicit bool
	meta     localTaskResultMemoryMetadata
}

type localProcedureAccumulator struct {
	kind           localMemoryNodeType
	signature      string
	explicit       bool
	evidenceCount  int
	lastSeenAt     string
	taskID         string
	sessionID      string
	tensionID      string
	protoClusterID string
	artifactRefs   []string
	docKeys        []string
	blockerKinds   []string
	sourceEventIDs []string
	summarySeed    string
	guidanceSeed   string
	// actionBasis is the anchor-independent action key (shared by every event in
	// this signature bucket). It joins per-signature buckets that represent the
	// same action across different loci for constitution generalization.
	actionBasis string
}

// localProcedureActionAggregate accumulates evidence for one (kind, action)
// across ALL anchors. The procedure signature is anchor-specific, so a single
// bucket can never show generalization; this cross-bucket view is what the
// constitution promotion guard (Layer A) uses to decide whether a rule has
// generalized across >=2 distinct loci.
type localProcedureActionAggregate struct {
	explicit       bool
	evidence       int
	taskAnchors    map[string]struct{}
	sessionAnchors map[string]struct{}
}

func (a *localProcedureActionAggregate) record(explicit bool, taskID, sessionID string) {
	a.explicit = a.explicit || explicit
	a.evidence++
	if taskID = strings.TrimSpace(taskID); taskID != "" {
		if a.taskAnchors == nil {
			a.taskAnchors = map[string]struct{}{}
		}
		a.taskAnchors[taskID] = struct{}{}
	}
	if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
		if a.sessionAnchors == nil {
			a.sessionAnchors = map[string]struct{}{}
		}
		a.sessionAnchors[sessionID] = struct{}{}
	}
}

// distinctAnchors reports how many distinct task (or, failing that, session)
// loci contributed evidence for this action.
func (a *localProcedureActionAggregate) distinctAnchors() int {
	tasks := len(a.taskAnchors)
	sessions := len(a.sessionAnchors)
	if tasks >= sessions {
		return tasks
	}
	return sessions
}

type localProcedureSelection struct {
	hint      LocalProcedureMemory
	scopeHits int
	refHits   int
}

func encodeTaskResultMemoryMetadata(result StructuredTaskResult) string {
	payload := localTaskResultMemoryMetadata{
		MemoryType:      strings.ToUpper(strings.TrimSpace(result.MemoryType)),
		NextAction:      strings.TrimSpace(result.NextAction),
		OwnerAction:     strings.TrimSpace(result.OwnerAction),
		HumanReason:     strings.TrimSpace(result.HumanReason),
		DecisionType:    strings.TrimSpace(result.DecisionType),
		MaterializedDoc: strings.TrimSpace(result.Materialize.DocKey),
		BlockedOn:       append([]BlockedRef(nil), result.BlockedOn...),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return string(raw)
}

func decodeTaskResultMemoryMetadata(raw string) localTaskResultMemoryMetadata {
	if strings.TrimSpace(raw) == "" {
		return localTaskResultMemoryMetadata{}
	}
	var payload localTaskResultMemoryMetadata
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &payload); err != nil {
		return localTaskResultMemoryMetadata{}
	}
	payload.MemoryType = strings.ToUpper(strings.TrimSpace(payload.MemoryType))
	payload.NextAction = strings.TrimSpace(payload.NextAction)
	payload.OwnerAction = strings.TrimSpace(payload.OwnerAction)
	payload.HumanReason = strings.TrimSpace(payload.HumanReason)
	payload.DecisionType = strings.TrimSpace(payload.DecisionType)
	payload.MaterializedDoc = strings.TrimSpace(payload.MaterializedDoc)
	for idx := range payload.BlockedOn {
		payload.BlockedOn[idx].Kind = strings.TrimSpace(payload.BlockedOn[idx].Kind)
		payload.BlockedOn[idx].Detail = strings.TrimSpace(payload.BlockedOn[idx].Detail)
	}
	return payload
}

func localMemoryEpisodeMetaFromEvent(event LocalMemoryEvent) map[string]string {
	meta := map[string]string{}
	if event.NodeType != "" {
		meta["node_type"] = string(event.NodeType)
	}
	if event.Details != "" {
		meta["details"] = strings.TrimSpace(event.Details)
	}
	if event.RequiresHuman {
		meta["requires_human"] = "true"
	}
	if event.MetadataJSON != "" {
		meta["metadata_json"] = strings.TrimSpace(event.MetadataJSON)
	}
	if len(event.BlockerKinds) > 0 {
		meta["blocker_kinds"] = encodeLocalMemoryStringList(event.BlockerKinds)
	}
	if len(meta) == 0 {
		return nil
	}
	return meta
}

func hydrateLocalMemoryEventFromRecordMeta(event *LocalMemoryEvent, meta map[string]string) {
	if event == nil || len(meta) == 0 {
		return
	}
	if rawType := strings.TrimSpace(meta["node_type"]); rawType != "" {
		event.NodeType = localMemoryNodeType(rawType)
	}
	if details := strings.TrimSpace(meta["details"]); details != "" {
		event.Details = details
	}
	if raw := strings.TrimSpace(meta["requires_human"]); raw != "" {
		required, err := strconv.ParseBool(raw)
		if err == nil {
			event.RequiresHuman = required
		}
	}
	if metadataJSON := strings.TrimSpace(meta["metadata_json"]); metadataJSON != "" {
		event.MetadataJSON = metadataJSON
	}
	if blockerKinds := decodeLocalMemoryStringList(meta["blocker_kinds"]); len(blockerKinds) > 0 {
		event.BlockerKinds = blockerKinds
	}
	if event.NodeType == "" {
		event.NodeType = localMemoryNodeRawEvent
	}
}

func encodeLocalMemoryStringList(items []string) string {
	if len(items) == 0 {
		return ""
	}
	raw, err := json.Marshal(uniqueTrimmedFocusStrings(items))
	if err != nil {
		return ""
	}
	return string(raw)
}

func decodeLocalMemoryStringList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var items []string
	if err := json.Unmarshal([]byte(raw), &items); err == nil {
		return uniqueTrimmedFocusStrings(items)
	}
	return uniqueTrimmedFocusStrings(strings.Split(raw, "|"))
}

func deriveLocalProcedureMaps(events []LocalMemoryEvent, constitutionMinEvidence int) (map[string]LocalProcedureMemory, map[string]LocalProcedureMemory) {
	procedures := map[string]LocalProcedureMemory{}
	antiProcedures := map[string]LocalProcedureMemory{}
	if len(events) == 0 {
		return procedures, antiProcedures
	}
	if constitutionMinEvidence <= 0 {
		constitutionMinEvidence = constitutionDefaultMinEvidence
	}

	sorted := append([]LocalMemoryEvent(nil), events...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return compareLocalMemoryTimestamps(sorted[i].OccurredAt, sorted[j].OccurredAt) < 0
	})

	sessionOutcomes := map[string]string{}
	taskOutcomes := map[string]string{}

	// Pass 1: Gather downstream effects
	for _, event := range sorted {
		eventKind := strings.ToLower(strings.TrimSpace(event.EventKind))
		outcome := normalizeOutcome(event.Outcome)
		if eventKind == "session_decision_needed" && event.SessionID != "" {
			sessionOutcomes[event.SessionID] = outcome
		}
		if eventKind == "task_cycle_result" && event.TaskID != "" {
			taskOutcomes[event.TaskID] = outcome
		}
	}

	buckets := map[string]*localProcedureAccumulator{}
	actionAgg := map[string]*localProcedureActionAggregate{}
	for _, event := range sorted {
		taskOutcome := taskOutcomes[event.TaskID]
		sessionOutcome := sessionOutcomes[event.SessionID]

		class, ok := classifyLocalProcedureEvent(event, taskOutcome, sessionOutcome)
		if !ok {
			continue
		}
		signature := buildLocalProcedureSignature(event, class)
		if signature == "" {
			continue
		}
		action := normalizedProcedureActionBasis(event, class.meta)
		actionKey := string(class.kind) + "|" + action
		agg := actionAgg[actionKey]
		if agg == nil {
			agg = &localProcedureActionAggregate{}
			actionAgg[actionKey] = agg
		}
		agg.record(class.explicit, event.TaskID, event.SessionID)

		key := string(class.kind) + "|" + signature
		bucket := buckets[key]
		if bucket == nil {
			bucket = &localProcedureAccumulator{
				kind:        class.kind,
				signature:   signature,
				actionBasis: action,
			}
			buckets[key] = bucket
		}
		bucket.explicit = bucket.explicit || class.explicit
		bucket.evidenceCount++
		bucket.lastSeenAt = maxLocalMemoryTimestamp(bucket.lastSeenAt, event.OccurredAt)
		bucket.taskID = firstNonEmpty(bucket.taskID, strings.TrimSpace(event.TaskID))
		bucket.sessionID = firstNonEmpty(bucket.sessionID, strings.TrimSpace(event.SessionID))
		bucket.tensionID = firstNonEmpty(bucket.tensionID, strings.TrimSpace(event.TensionID))
		bucket.protoClusterID = firstNonEmpty(bucket.protoClusterID, strings.TrimSpace(event.ProtoClusterID))
		bucket.artifactRefs = mergeLimitedStrings(bucket.artifactRefs, event.ArtifactRefs, 8)
		bucket.docKeys = mergeLimitedStrings(bucket.docKeys, event.DocKeys, 8)
		bucket.blockerKinds = mergeLimitedStrings(bucket.blockerKinds, event.BlockerKinds, 6)
		bucket.sourceEventIDs = mergeLimitedStrings(bucket.sourceEventIDs, []string{localProcedureSourceEventID(event)}, 6)
		bucket.summarySeed = firstNonEmpty(bucket.summarySeed, localProcedureSummarySeed(event, class.meta))
		bucket.guidanceSeed = firstNonEmpty(bucket.guidanceSeed, localProcedureGuidanceSeed(event, class.meta, class.kind))
	}

	for _, bucket := range buckets {
		threshold := 2
		if bucket.explicit {
			threshold = 1
		}
		if bucket.evidenceCount < threshold {
			continue
		}
		agg := actionAgg[string(bucket.kind)+"|"+bucket.actionBasis]
		distinctAnchors := 0
		actionEvidence := bucket.evidenceCount
		actionExplicit := bucket.explicit
		if agg != nil {
			distinctAnchors = agg.distinctAnchors()
			actionEvidence = agg.evidence
			actionExplicit = agg.explicit
		}
		hint := LocalProcedureMemory{
			HintID:          buildLocalProcedureHintID(bucket.kind, bucket.signature),
			NodeType:        bucket.kind,
			Signature:       bucket.signature,
			Summary:         buildLocalProcedureSummary(*bucket),
			Guidance:        buildLocalProcedureGuidance(*bucket),
			EvidenceCount:   bucket.evidenceCount,
			LastSeenAt:      strings.TrimSpace(bucket.lastSeenAt),
			TaskID:          bucket.taskID,
			SessionID:       bucket.sessionID,
			TensionID:       bucket.tensionID,
			ProtoClusterID:  bucket.protoClusterID,
			ArtifactRefs:    append([]string(nil), bucket.artifactRefs...),
			DocKeys:         append([]string(nil), bucket.docKeys...),
			BlockerKinds:    append([]string(nil), bucket.blockerKinds...),
			SourceEventIDs:  append([]string(nil), bucket.sourceEventIDs...),
			DistinctAnchors: distinctAnchors,
		}
		// Constitution promotion guard (Layer A): a derived rule becomes global
		// only when its action is explicitly typed AND has generalized across >=2
		// distinct loci AND its cross-anchor evidence clears the threshold.
		// Implicit, single-locus evidence can never silently become a global rule.
		if actionExplicit && distinctAnchors >= 2 && actionEvidence >= constitutionMinEvidence {
			hint.Global = true
		}
		switch hint.NodeType {
		case localMemoryNodeProcedure:
			procedures[hint.HintID] = hint
		case localMemoryNodeAntiProcedure:
			antiProcedures[hint.HintID] = hint
		}
	}
	return procedures, antiProcedures
}

func classifyLocalProcedureEvent(event LocalMemoryEvent, taskOutcome, sessionOutcome string) (localProcedureEvidenceClass, bool) {
	meta := decodeTaskResultMemoryMetadata(event.MetadataJSON)

	// 1. Explicitly typed memory nodes are always honored, regardless of EventKind
	switch event.NodeType {
	case localMemoryNodeProcedure:
		return localProcedureEvidenceClass{kind: localMemoryNodeProcedure, explicit: true, meta: meta}, true
	case localMemoryNodeAntiProcedure:
		return localProcedureEvidenceClass{kind: localMemoryNodeAntiProcedure, explicit: true, meta: meta}, true
	}

	// 2. Explicitly tagged memory via metadata (legacy path)
	switch meta.MemoryType {
	case "PROCEDURE":
		return localProcedureEvidenceClass{kind: localMemoryNodeProcedure, explicit: true, meta: meta}, true
	case "ANTI_PROCEDURE":
		return localProcedureEvidenceClass{kind: localMemoryNodeAntiProcedure, explicit: true, meta: meta}, true
	}

	// Check if this is one of our recognized implicit evidence sources
	eventKind := strings.ToLower(strings.TrimSpace(event.EventKind))
	isTaskResult := eventKind == "task_cycle_result"
	isSessionDecision := eventKind == "session_decision_needed"
	isRuntimeRecovery := eventKind == "runtime_recovery"
	isToolResult := eventKind == "tool_call_result"
	isKnowledgeClaimVerified := eventKind == "knowledge_claim.verified" || eventKind == "knowledge_claim.confirmed"
	isKnowledgeClaimDisputed := eventKind == "knowledge_claim.disputed" || eventKind == "knowledge_claim.superseded" || eventKind == "knowledge_claim.rejected"
	isVerifierFail := eventKind == "verifier_fail" || eventKind == "tension.verifier_fail"
	isDiscarded := eventKind == "tension.discarded" || eventKind == "tension_discarded"

	// 3. Implicit evidence from Runtime Recovery (crashes, stalls) -> Anti-Procedure
	if isRuntimeRecovery {
		// A recovery event means we stalled or failed severely in this locus.
		if normalizedProcedureActionBasis(event, meta) != "" {
			return localProcedureEvidenceClass{kind: localMemoryNodeAntiProcedure, meta: meta}, true
		}
		return localProcedureEvidenceClass{}, false
	}

	// 4. Implicit evidence from Tool Results
	if isToolResult {
		rawOutcome := strings.ToLower(strings.TrimSpace(event.Outcome))
		if rawOutcome == "failed" || rawOutcome == "error" || rawOutcome == "blocked" {
			if normalizedProcedureActionBasis(event, meta) != "" {
				return localProcedureEvidenceClass{kind: localMemoryNodeAntiProcedure, meta: meta}, true
			}
		}
		// Downstream-effect scoring:
		// If the tool succeeded, and the downstream task or session completed successfully,
		// we consider it a positive procedure hint.
		if rawOutcome == "completed" || rawOutcome == "success" {
			if strings.ToLower(strings.TrimSpace(taskOutcome)) == "completed" || strings.ToLower(strings.TrimSpace(sessionOutcome)) == "completed" {
				if normalizedProcedureActionBasis(event, meta) != "" {
					return localProcedureEvidenceClass{kind: localMemoryNodeProcedure, meta: meta}, true
				}
			}
		}
		return localProcedureEvidenceClass{}, false
	}

	// 5. Implicit evidence from Knowledge Claims
	if isKnowledgeClaimVerified {
		if normalizedProcedureActionBasis(event, meta) != "" {
			return localProcedureEvidenceClass{kind: localMemoryNodeProcedure, meta: meta}, true
		}
	}
	if isKnowledgeClaimDisputed {
		if normalizedProcedureActionBasis(event, meta) != "" {
			return localProcedureEvidenceClass{kind: localMemoryNodeAntiProcedure, meta: meta}, true
		}
	}

	// 5.5 Implicit evidence from Rhizome Protocol failures -> Anti-Procedure
	if isVerifierFail || isDiscarded {
		if normalizedProcedureActionBasis(event, meta) != "" {
			return localProcedureEvidenceClass{kind: localMemoryNodeAntiProcedure, meta: meta}, true
		}
	}

	if !isTaskResult && !isSessionDecision {
		return localProcedureEvidenceClass{}, false
	}

	// 6. Implicit evidence from Task Cycle Results and Session Decisions
	rawOutcome := strings.ToLower(strings.TrimSpace(event.Outcome))
	switch normalizeOutcome(event.Outcome) {
	case "completed", "continue":
		// If it was explicitly a handoff or escalated, treat it as anti-procedure despite normalized 'continue'
		if rawOutcome == "handoff" || rawOutcome == "escalated" {
			if isExternalHumanGateBlock(event.BlockerKinds, meta.BlockedOn, event.RequiresHuman) {
				return localProcedureEvidenceClass{}, false
			}
			return localProcedureEvidenceClass{kind: localMemoryNodeAntiProcedure, meta: meta}, normalizedProcedureActionBasis(event, meta) != ""
		}
		if event.RequiresHuman {
			return localProcedureEvidenceClass{}, false
		}
		return localProcedureEvidenceClass{kind: localMemoryNodeProcedure, meta: meta}, normalizedProcedureActionBasis(event, meta) != ""
	case "blocked", "failed":
		if isExternalHumanGateBlock(event.BlockerKinds, meta.BlockedOn, event.RequiresHuman) {
			return localProcedureEvidenceClass{}, false
		}
		return localProcedureEvidenceClass{kind: localMemoryNodeAntiProcedure, meta: meta}, normalizedProcedureActionBasis(event, meta) != ""
	default:
		return localProcedureEvidenceClass{}, false
	}
}

func buildLocalProcedureSignature(event LocalMemoryEvent, class localProcedureEvidenceClass) string {
	action := normalizedProcedureActionBasis(event, class.meta)
	if action == "" {
		return ""
	}
	parts := []string{
		string(class.kind),
		"task=" + strings.TrimSpace(event.TaskID),
		"session=" + strings.TrimSpace(event.SessionID),
		"tension=" + strings.TrimSpace(event.TensionID),
		"cluster=" + strings.TrimSpace(event.ProtoClusterID),
		"action=" + action,
	}
	if len(event.BlockerKinds) > 0 {
		parts = append(parts, "blockers="+strings.Join(uniqueTrimmedFocusStrings(event.BlockerKinds), ","))
	}
	if docKey := strings.TrimSpace(class.meta.MaterializedDoc); docKey != "" {
		parts = append(parts, "materialized_doc="+docKey)
	}
	if len(event.DocKeys) > 0 {
		parts = append(parts, "docs="+strings.Join(uniqueTrimmedFocusStrings(event.DocKeys), ","))
	}
	if len(event.ArtifactRefs) > 0 {
		parts = append(parts, "artifacts="+strings.Join(uniqueTrimmedFocusStrings(event.ArtifactRefs), ","))
	}
	return strings.Join(parts, "|")
}

func normalizedProcedureActionBasis(event LocalMemoryEvent, meta localTaskResultMemoryMetadata) string {
	value := firstNonEmpty(
		meta.NextAction,
		meta.OwnerAction,
		meta.MaterializedDoc,
		event.Summary,
		event.Details,
	)
	value = oneLine(strings.ToLower(strings.TrimSpace(value)))
	if value == "" {
		return ""
	}
	return strings.Join(strings.Fields(value), " ")
}

func localProcedureSummarySeed(event LocalMemoryEvent, meta localTaskResultMemoryMetadata) string {
	return firstNonEmpty(
		oneLine(meta.NextAction),
		oneLine(meta.OwnerAction),
		oneLine(event.Summary),
		oneLine(event.Details),
	)
}

func localProcedureGuidanceSeed(event LocalMemoryEvent, meta localTaskResultMemoryMetadata, kind localMemoryNodeType) string {
	switch kind {
	case localMemoryNodeProcedure:
		return firstNonEmpty(
			oneLine(meta.NextAction),
			oneLine(event.Details),
			oneLine(event.Summary),
		)
	case localMemoryNodeAntiProcedure:
		return firstNonEmpty(
			oneLine(meta.OwnerAction),
			oneLine(meta.HumanReason),
			oneLine(event.Details),
			oneLine(event.Summary),
		)
	default:
		return firstNonEmpty(oneLine(event.Summary), oneLine(event.Details))
	}
}

func buildLocalProcedureSummary(bucket localProcedureAccumulator) string {
	base := firstNonEmpty(oneLine(bucket.summarySeed), "local continuity")
	switch bucket.kind {
	case localMemoryNodeProcedure:
		return firstNonEmpty(base, "Repeated successful local path")
	case localMemoryNodeAntiProcedure:
		if len(bucket.blockerKinds) > 0 {
			return fmt.Sprintf("CRITICAL ANTI-PATTERN: Do not attempt this branch. %s (ends in %s)", base, strings.Join(bucket.blockerKinds, ", "))
		}
		prefix := "CRITICAL ANTI-PATTERN: Do not attempt this branch. "
		if base != "local continuity" {
			return prefix + base
		}
		return "CRITICAL ANTI-PATTERN: Repeated failure path"
	default:
		return base
	}
}

func buildLocalProcedureGuidance(bucket localProcedureAccumulator) string {
	seed := firstNonEmpty(oneLine(bucket.guidanceSeed), oneLine(bucket.summarySeed))
	switch bucket.kind {
	case localMemoryNodeProcedure:
		return firstNonEmpty(seed, "Prefer this local path when the same locus recurs.")
	case localMemoryNodeAntiProcedure:
		if len(bucket.blockerKinds) > 0 {
			return fmt.Sprintf("CRITICAL IMMUNITY BLOCK: Avoid this path; it repeatedly ends in %s.", strings.Join(bucket.blockerKinds, ", "))
		}
		prefix := "CRITICAL IMMUNITY BLOCK: Avoid this path. "
		if seed != "" {
			return prefix + seed
		}
		return "CRITICAL IMMUNITY BLOCK: Avoid this path; it repeatedly fails in the same locus."
	default:
		return seed
	}
}

func buildLocalProcedureHintID(kind localMemoryNodeType, signature string) string {
	sum := sha1.Sum([]byte(strings.TrimSpace(signature)))
	prefix := "proc"
	if kind == localMemoryNodeAntiProcedure {
		prefix = "anti"
	}
	return fmt.Sprintf("%s-%x", prefix, sum[:6])
}

func localProcedureSourceEventID(event LocalMemoryEvent) string {
	if sourceID := strings.TrimSpace(event.SourceID); sourceID != "" {
		return sourceID
	}
	if event.Sequence > 0 {
		return fmt.Sprintf("event-%d", event.Sequence)
	}
	return strings.TrimSpace(event.OccurredAt)
}

func isExternalHumanGateBlock(blockerKinds []string, blockedOn []BlockedRef, requiresHuman bool) bool {
	if !requiresHuman && len(blockerKinds) == 0 && len(blockedOn) == 0 {
		return false
	}
	kinds := append([]string(nil), blockerKinds...)
	for _, blocker := range blockedOn {
		if kind := strings.TrimSpace(blocker.Kind); kind != "" {
			kinds = append(kinds, kind)
		}
	}
	if len(kinds) == 0 {
		return requiresHuman
	}
	for _, kind := range kinds {
		switch strings.ToLower(strings.TrimSpace(kind)) {
		case "credential", "auth", "authentication", "oauth", "payment", "billing", "approval", "policy", "operator", "manual", "human":
			continue
		default:
			return false
		}
	}
	return true
}

func maxLocalMemoryTimestamp(left, right string) string {
	if compareLocalMemoryTimestamps(strings.TrimSpace(left), strings.TrimSpace(right)) >= 0 {
		return strings.TrimSpace(left)
	}
	return strings.TrimSpace(right)
}

func cloneProcedureMemoryMap(src map[string]LocalProcedureMemory) map[string]LocalProcedureMemory {
	if len(src) == 0 {
		return map[string]LocalProcedureMemory{}
	}
	out := make(map[string]LocalProcedureMemory, len(src))
	for key, value := range src {
		copy := value
		copy.ArtifactRefs = cloneStrings(copy.ArtifactRefs)
		copy.DocKeys = cloneStrings(copy.DocKeys)
		copy.BlockerKinds = cloneStrings(copy.BlockerKinds)
		copy.SourceEventIDs = cloneStrings(copy.SourceEventIDs)
		out[key] = copy
	}
	return out
}

func selectAnchoredProcedureMemories(hints map[string]LocalProcedureMemory, taskID, sessionID, tensionID, clusterID string, docKeys, artifactRefs []string) []LocalProcedureMemory {
	if len(hints) == 0 {
		return nil
	}
	docKeySet := map[string]struct{}{}
	for _, key := range docKeys {
		if key = strings.TrimSpace(key); key != "" {
			docKeySet[key] = struct{}{}
		}
	}
	artifactSet := map[string]struct{}{}
	for _, ref := range artifactRefs {
		if ref = strings.TrimSpace(ref); ref != "" {
			artifactSet[ref] = struct{}{}
		}
	}

	selected := make([]localProcedureSelection, 0, len(hints))
	for _, hint := range hints {
		scopeHits := 0
		conflicts := 0
		matchScope := func(expected, actual string) {
			expected = strings.TrimSpace(expected)
			actual = strings.TrimSpace(actual)
			if expected == "" || actual == "" {
				return
			}
			if expected == actual {
				scopeHits++
				return
			}
			conflicts++
		}
		matchScope(hint.TaskID, taskID)
		matchScope(hint.SessionID, sessionID)
		matchScope(hint.TensionID, tensionID)
		matchScope(hint.ProtoClusterID, clusterID)
		if conflicts > 0 {
			continue
		}
		refHits := 0
		for _, key := range hint.DocKeys {
			if _, ok := docKeySet[strings.TrimSpace(key)]; ok {
				refHits++
			}
		}
		for _, ref := range hint.ArtifactRefs {
			if _, ok := artifactSet[strings.TrimSpace(ref)]; ok {
				refHits++
			}
		}
		if scopeHits == 0 && refHits == 0 {
			continue
		}
		selected = append(selected, localProcedureSelection{
			hint:      hint,
			scopeHits: scopeHits,
			refHits:   refHits,
		})
	}

	sort.SliceStable(selected, func(i, j int) bool {
		switch {
		case selected[i].scopeHits != selected[j].scopeHits:
			return selected[i].scopeHits > selected[j].scopeHits
		case selected[i].refHits != selected[j].refHits:
			return selected[i].refHits > selected[j].refHits
		case selected[i].hint.EvidenceCount != selected[j].hint.EvidenceCount:
			return selected[i].hint.EvidenceCount > selected[j].hint.EvidenceCount
		case compareLocalMemoryTimestamps(selected[i].hint.LastSeenAt, selected[j].hint.LastSeenAt) != 0:
			return compareLocalMemoryTimestamps(selected[i].hint.LastSeenAt, selected[j].hint.LastSeenAt) > 0
		default:
			return selected[i].hint.Summary < selected[j].hint.Summary
		}
	})

	out := make([]LocalProcedureMemory, 0, len(selected))
	for _, item := range selected {
		out = append(out, item.hint)
	}
	return out
}

// selectConstitutionRules returns the always-on, anchor-independent rule tier
// (Layer A): operator-authored seed rules plus derived rules flagged Global.
// Ranking is total-ordered (seeds first, then priority, evidence, recency, text)
// and, when the cap forces truncation, anti_procedure/invariant rules are
// preferred over procedure rules — a forgotten "do-not" is more dangerous than
// a forgotten "prefer". The current anchor is intentionally NOT consulted.
func selectConstitutionRules(
	procedures, antiProcedures map[string]LocalProcedureMemory,
	seeds []AgentAnatomyConstitutionRule,
	maxRules int,
) []ConstitutionRule {
	if maxRules <= 0 {
		return nil
	}
	candidates := make([]ConstitutionRule, 0, len(seeds)+len(procedures)+len(antiProcedures))
	for _, seed := range seeds {
		text := strings.TrimSpace(seed.Rule)
		if text == "" {
			continue
		}
		kind := firstNonEmpty(normalizeConstitutionRuleKind(seed.Kind), "invariant")
		candidates = append(candidates, ConstitutionRule{
			ID:       strings.TrimSpace(seed.ID),
			Text:     text,
			Kind:     kind,
			Seed:     true,
			Priority: seed.Priority,
		})
	}
	appendDerived := func(hints map[string]LocalProcedureMemory, kind string) {
		for _, hint := range hints {
			if !hint.Global {
				continue
			}
			text := firstNonEmpty(oneLine(hint.Summary), oneLine(hint.Guidance))
			if text == "" {
				continue
			}
			candidates = append(candidates, ConstitutionRule{
				ID:            hint.HintID,
				Text:          text,
				Kind:          kind,
				Seed:          false,
				EvidenceCount: hint.EvidenceCount,
			})
		}
	}
	appendDerived(procedures, "procedure")
	appendDerived(antiProcedures, "anti_procedure")
	if len(candidates) == 0 {
		return nil
	}

	less := func(a, b ConstitutionRule) bool {
		if a.Seed != b.Seed {
			return a.Seed // seeds rank first
		}
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if a.EvidenceCount != b.EvidenceCount {
			return a.EvidenceCount > b.EvidenceCount
		}
		if a.Text != b.Text {
			return a.Text < b.Text
		}
		return a.ID < b.ID
	}

	// Fill order honors two contracts together: (1) seeds rank first and are only
	// dropped if there are more seeds than the cap; (2) among derived rules,
	// anti_procedure/invariant ("do-not") are preferred over procedure ("prefer")
	// on truncation — a forgotten do-not is more dangerous than a forgotten prefer.
	var seedRules, derivedSafety, derivedAdvisory []ConstitutionRule
	for _, rule := range candidates {
		switch {
		case rule.Seed:
			seedRules = append(seedRules, rule)
		case rule.Kind == "procedure":
			derivedAdvisory = append(derivedAdvisory, rule)
		default:
			derivedSafety = append(derivedSafety, rule)
		}
	}
	sort.SliceStable(seedRules, func(i, j int) bool { return less(seedRules[i], seedRules[j]) })
	sort.SliceStable(derivedSafety, func(i, j int) bool { return less(derivedSafety[i], derivedSafety[j]) })
	sort.SliceStable(derivedAdvisory, func(i, j int) bool { return less(derivedAdvisory[i], derivedAdvisory[j]) })

	// Distinct per-anchor buckets can globalize the same action, yielding
	// duplicate-text rules. Dedupe by (kind, normalized text); sorting first
	// means the highest-ranked instance wins.
	seen := map[string]struct{}{}
	out := make([]ConstitutionRule, 0, maxRules)
	fill := func(rules []ConstitutionRule) {
		for _, rule := range rules {
			if len(out) >= maxRules {
				return
			}
			dedupeKey := rule.Kind + "|" + strings.ToLower(strings.TrimSpace(rule.Text))
			if _, ok := seen[dedupeKey]; ok {
				continue
			}
			seen[dedupeKey] = struct{}{}
			out = append(out, rule)
		}
	}
	fill(seedRules)
	fill(derivedSafety)
	fill(derivedAdvisory)
	return out
}

func renderProcedureHints(hints []LocalProcedureMemory, limit int) []string {
	if len(hints) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(hints) {
		limit = len(hints)
	}
	out := make([]string, 0, limit)
	for _, hint := range hints[:limit] {
		label := firstNonEmpty(oneLine(hint.Summary), oneLine(hint.Guidance), "local continuity")
		if hint.EvidenceCount > 1 {
			label = fmt.Sprintf("%s (x%d)", label, hint.EvidenceCount)
		}
		if hint.NodeType == localMemoryNodeAntiProcedure && len(hint.BlockerKinds) > 0 {
			label = fmt.Sprintf("%s; blockers=%s", label, strings.Join(hint.BlockerKinds[:min(len(hint.BlockerKinds), 3)], ", "))
		}
		out = append(out, label)
	}
	return out
}
