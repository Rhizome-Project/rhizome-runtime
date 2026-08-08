package sessionmemory

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

const (
	memoryTypeUpdateDigest = "UPDATE_DIGEST"
	sourceKindSessionEvent = "session_event"
)

const (
	SyncEventActionRecorded = "RECORDED"
	SyncEventActionArchived = "ARCHIVED"
)

type SyncEventResult struct {
	Action               string
	Record               sqlite.WorkspaceMemoryRecord
	Event                sqlite.RuntimeEventRecord
	PromotedClaimEffects *sqlite.PromotedKnowledgeClaimSyncEffects
}

func SyncEvent(ctx context.Context, store *sqlite.Store, workspaceID string, state sqlite.AgentSessionStateRecord) error {
	_, err := SyncEventWithResult(ctx, store, workspaceID, state)
	return err
}

func SyncEventWithResult(ctx context.Context, store *sqlite.Store, workspaceID string, state sqlite.AgentSessionStateRecord) (SyncEventResult, error) {
	if store == nil {
		return SyncEventResult{}, nil
	}
	workspaceID = strings.TrimSpace(firstNonEmptyNonBlank(workspaceID, state.WorkspaceID))
	if workspaceID == "" {
		return SyncEventResult{}, nil
	}
	eventType := model.NormalizeSessionEventType(state.UpdateType)
	switch eventType {
	case model.SessionEventStart, model.SessionEventKeepalive:
		return archiveStateMemoryIfPresentWithResult(ctx, store, workspaceID, state.SessionID, state.AgentID, eventType)
	case model.SessionEventStatus:
		if ShouldPersistStateMemory(state) {
			record, event, effects, err := store.RecordWorkspaceMemoryWithEffects(ctx, WorkspaceMemoryInput(workspaceID, state))
			if err != nil {
				return SyncEventResult{}, err
			}
			return SyncEventResult{Action: SyncEventActionRecorded, Record: record, Event: event, PromotedClaimEffects: effects}, nil
		}
		return archiveStateMemoryIfPresentWithResult(ctx, store, workspaceID, state.SessionID, state.AgentID, eventType)
	case model.SessionEventBlocked, model.SessionEventDecisionNeeded, model.SessionEventEnd:
		record, event, effects, err := store.RecordWorkspaceMemoryWithEffects(ctx, WorkspaceMemoryInput(workspaceID, state))
		if err != nil {
			return SyncEventResult{}, err
		}
		return SyncEventResult{Action: SyncEventActionRecorded, Record: record, Event: event, PromotedClaimEffects: effects}, nil
	default:
		return SyncEventResult{}, nil
	}
}

func StateMemoryID(sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ""
	}
	return "memory-session-state-" + sessionID
}

func WorkspaceMemoryInput(workspaceID string, state sqlite.AgentSessionStateRecord) sqlite.WorkspaceMemoryInput {
	title, body, tags := buildStateMemoryContent(state)
	importance, confidence := memoryScores(memoryTypeUpdateDigest, sourceKindSessionEvent)
	switch model.NormalizeSessionEventType(state.UpdateType) {
	case model.SessionEventDecisionNeeded:
		importance = clampUnitInterval(importance + 0.10)
	case model.SessionEventBlocked:
		importance = clampUnitInterval(importance + 0.06)
	case model.SessionEventEnd:
		confidence = clampUnitInterval(confidence + 0.04)
	}
	return sqlite.WorkspaceMemoryInput{
		MemoryID:    StateMemoryID(state.SessionID),
		WorkspaceID: strings.TrimSpace(workspaceID),
		MemoryType:  memoryTypeUpdateDigest,
		Title:       title,
		Body:        body,
		Summary:     clipSummary(state.Summary, 240),
		AgentID:     strings.TrimSpace(state.AgentID),
		SessionID:   strings.TrimSpace(state.SessionID),
		TaskID:      strings.TrimSpace(state.TaskID),
		SourceKind:  sourceKindSessionEvent,
		SourceID:    strings.TrimSpace(state.SessionID),
		Tags:        tags,
		Importance:  importance,
		Confidence:  confidence,
	}
}

func ShouldPersistStateMemory(state sqlite.AgentSessionStateRecord) bool {
	if model.NormalizeSessionEventType(state.UpdateType) != model.SessionEventStatus {
		return false
	}
	return model.NormalizeStatus(state.Status) == model.SessionStatusHandoffPending || strings.TrimSpace(state.HandoffTo) != ""
}

func archiveStateMemoryIfPresent(ctx context.Context, store *sqlite.Store, workspaceID, sessionID, agentID, eventType string) error {
	_, err := archiveStateMemoryIfPresentWithResult(ctx, store, workspaceID, sessionID, agentID, eventType)
	return err
}

func archiveStateMemoryIfPresentWithResult(ctx context.Context, store *sqlite.Store, workspaceID, sessionID, agentID, eventType string) (SyncEventResult, error) {
	memoryID := StateMemoryID(sessionID)
	if memoryID == "" {
		return SyncEventResult{}, nil
	}
	if _, err := store.GetWorkspaceMemory(ctx, workspaceID, memoryID); err != nil {
		return SyncEventResult{}, nil
	}
	record, event, effects, err := store.ArchiveWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    memoryID,
		ArchivedBy:  firstNonEmptyNonBlank(strings.TrimSpace(agentID), workspaceID),
		Reason:      "session_" + strings.ReplaceAll(model.NormalizeSessionEventType(eventType), ".", "_"),
	})
	if err != nil {
		return SyncEventResult{}, err
	}
	return SyncEventResult{Action: SyncEventActionArchived, Record: record, Event: event, PromotedClaimEffects: effects}, nil
}

func buildStateMemoryContent(state sqlite.AgentSessionStateRecord) (title, body string, tags []string) {
	eventType := model.NormalizeSessionEventType(state.UpdateType)
	taskID := strings.TrimSpace(state.TaskID)
	sessionID := strings.TrimSpace(state.SessionID)
	status := strings.TrimSpace(state.Status)
	title = stateMemoryTitle(eventType, status, state.HandoffTo, taskID, sessionID)

	lines := []string{
		fmt.Sprintf("Session event: %s", eventType),
		fmt.Sprintf("Session ID: %s", sessionID),
	}
	if taskID != "" {
		lines = append(lines, fmt.Sprintf("Task ID: %s", taskID))
	}
	if status != "" {
		lines = append(lines, fmt.Sprintf("Status: %s", status))
	}
	if summary := strings.TrimSpace(state.Summary); summary != "" {
		lines = append(lines, fmt.Sprintf("Summary: %s", summary))
	}
	if state.DecisionNeededFrom != "" {
		lines = append(lines, fmt.Sprintf("Decision needed from: %s", strings.TrimSpace(state.DecisionNeededFrom)))
	}
	if state.DecisionType != "" {
		lines = append(lines, fmt.Sprintf("Decision type: %s", strings.TrimSpace(state.DecisionType)))
	}
	if state.HandoffTo != "" {
		lines = append(lines, fmt.Sprintf("Handoff to: %s", strings.TrimSpace(state.HandoffTo)))
	}
	if len(state.BlockedOn) > 0 {
		lines = append(lines, "Blocked on:")
		for _, item := range state.BlockedOn {
			lines = append(lines, fmt.Sprintf("- %s: %s", strings.TrimSpace(item.Kind), strings.TrimSpace(item.Detail)))
		}
	}
	if len(state.RelatedDocKeys) > 0 {
		lines = append(lines, "Related docs: "+strings.Join(state.RelatedDocKeys, ", "))
	}
	if len(state.RelatedArtifactRefs) > 0 {
		artifactRefs := make([]string, 0, len(state.RelatedArtifactRefs))
		for _, artifact := range state.RelatedArtifactRefs {
			ref := firstNonEmptyNonBlank(strings.TrimSpace(artifact.Ref), strings.TrimSpace(artifact.Kind), strings.TrimSpace(artifact.ContentType))
			if ref == "" {
				continue
			}
			artifactRefs = append(artifactRefs, ref)
		}
		if len(artifactRefs) > 0 {
			lines = append(lines, "Related artifacts: "+strings.Join(artifactRefs, ", "))
		}
	}

	baseTags := buildTags(memoryTypeUpdateDigest, sourceKindSessionEvent, title)
	tags = append(baseTags, "session")
	switch eventType {
	case model.SessionEventBlocked:
		tags = append(tags, "blocked")
	case model.SessionEventDecisionNeeded:
		tags = append(tags, "decision_needed")
	case model.SessionEventEnd:
		tags = append(tags, "ended")
	case model.SessionEventStatus:
		if model.NormalizeStatus(state.Status) == model.SessionStatusHandoffPending || strings.TrimSpace(state.HandoffTo) != "" {
			tags = append(tags, "handoff", "handoff_pending")
		}
	}
	for _, item := range state.BlockedOn {
		if kind := strings.TrimSpace(item.Kind); kind != "" {
			tags = append(tags, kind)
		}
	}
	return title, strings.Join(lines, "\n"), uniqueLowercaseValues(tags)
}

func stateMemoryTitle(eventType, status, handoffTo, taskID, sessionID string) string {
	label := "Session update"
	switch eventType {
	case model.SessionEventBlocked:
		label = "Session blocked"
	case model.SessionEventDecisionNeeded:
		label = "Decision needed"
	case model.SessionEventEnd:
		label = "Session ended"
	case model.SessionEventStatus:
		if model.NormalizeStatus(status) == model.SessionStatusHandoffPending || strings.TrimSpace(handoffTo) != "" {
			label = "Handoff pending"
		}
	}
	if taskID = strings.TrimSpace(taskID); taskID != "" {
		return label + " for task " + taskID
	}
	if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
		return label + " for " + sessionID
	}
	return label
}

func buildTags(memoryType, sourceKind, title string) []string {
	tags := make([]string, 0, 5)
	if normalizedType := strings.ToLower(strings.TrimSpace(memoryType)); normalizedType != "" {
		tags = append(tags, normalizedType)
	}
	if normalizedSource := strings.ToLower(strings.TrimSpace(sourceKind)); normalizedSource != "" {
		tags = append(tags, normalizedSource)
	}
	for _, keyword := range extractKeywords(title, 3) {
		tags = append(tags, keyword)
	}
	return uniqueLowercaseValues(tags)
}

func extractKeywords(raw string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	fields := strings.FieldsFunc(strings.ToLower(raw), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '_' && r != '-'
	})
	out := make([]string, 0, limit)
	for _, field := range fields {
		if len(field) <= 3 {
			continue
		}
		out = append(out, field)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func uniqueLowercaseValues(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func memoryScores(memoryType, sourceKind string) (importance, confidence float64) {
	switch strings.ToUpper(strings.TrimSpace(memoryType)) {
	case "DECISION":
		importance, confidence = 0.95, 0.84
	case "PROCEDURE", "ANTI_PROCEDURE":
		importance, confidence = 0.90, 0.82
	case "INCIDENT":
		importance, confidence = 0.88, 0.78
	case "LESSON":
		importance, confidence = 0.80, 0.76
	case "ENTITY":
		importance, confidence = 0.72, 0.78
	case "EXPERIENCE":
		importance, confidence = 0.68, 0.66
	case memoryTypeUpdateDigest:
		importance, confidence = 0.62, 0.72
	case "SUMMARY":
		importance, confidence = 0.55, 0.60
	default:
		importance, confidence = 0.50, 0.60
	}

	switch strings.ToLower(strings.TrimSpace(sourceKind)) {
	case "manual":
		importance += 0.05
		confidence += 0.08
	case "reflection":
		confidence += 0.04
	case sourceKindSessionEvent:
		importance += 0.04
		confidence += 0.03
	case "compaction":
		confidence -= 0.03
	}
	return clampUnitInterval(importance), clampUnitInterval(confidence)
}

func clampUnitInterval(v float64) float64 {
	switch {
	case v < 0:
		return 0
	case v > 1:
		return 1
	default:
		return v
	}
}

func clipSummary(summary string, limit int) string {
	summary = strings.TrimSpace(summary)
	if summary == "" || limit <= 0 || len(summary) <= limit {
		return summary
	}
	if limit <= 3 {
		return summary[:limit]
	}
	return summary[:limit-3] + "..."
}

func firstNonEmptyNonBlank(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
