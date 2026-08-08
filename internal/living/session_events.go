package living

import (
	"context"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func recordSessionEventIfAvailable(ctx context.Context, rhizome RhizomeClient, input SessionEventInput) {
	sessionClient, ok := rhizome.(SessionAwareRhizomeClient)
	if !ok {
		return
	}
	if strings.TrimSpace(input.SessionID) == "" || strings.TrimSpace(input.AgentID) == "" {
		return
	}
	if err := sessionClient.RecordSessionEvent(ctx, input); err != nil {
		// Best-effort only. Callers already log operational outcomes elsewhere.
	}
}

func taskBlockedRef(kind, detail string) []model.AgentUpdateBlockedRef {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = "external_dependency"
	}
	detail = strings.TrimSpace(detail)
	if detail == "" {
		detail = "blocked"
	}
	return []model.AgentUpdateBlockedRef{{Kind: kind, Detail: detail}}
}

func pausedSessionEvent(task *TaskState, agentID, taskID string, update Update, reason string) SessionEventInput {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = strings.TrimSpace(update.Summary)
	}
	keepFalse := false
	input := SessionEventInput{
		SessionID:         strings.TrimSpace(task.SessionID),
		AgentID:           strings.TrimSpace(agentID),
		TaskID:            strings.TrimSpace(taskID),
		Summary:           clipTaskSessionSummary(reason, 240),
		KeepSessionActive: &keepFalse,
	}
	if update.RequiresHuman || isDecisionUpdateType(update.UpdateType) {
		input.EventType = model.SessionEventDecisionNeeded
		input.DecisionNeededFrom = "human"
		input.DecisionType = firstNonEmptyNonBlank(strings.TrimSpace(update.UpdateType), "human_input")
		return input
	}
	input.EventType = model.SessionEventBlocked
	input.BlockedOn = taskBlockedRef(normalizeBlockerKind(update.UpdateType), reason)
	return input
}

func resumedSessionKeepalive(task *TaskState, agentID, taskID, fromAgentID string) SessionEventInput {
	keepTrue := true
	summary := "Task resumed"
	if trimmed := strings.TrimSpace(fromAgentID); trimmed != "" {
		summary = "Task resumed after message from " + trimmed
	}
	return SessionEventInput{
		EventType:         model.SessionEventKeepalive,
		SessionID:         strings.TrimSpace(task.SessionID),
		AgentID:           strings.TrimSpace(agentID),
		TaskID:            strings.TrimSpace(taskID),
		Summary:           clipTaskSessionSummary(summary, 240),
		KeepSessionActive: &keepTrue,
	}
}

func firstNonEmptyNonBlank(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func normalizeBlockerKind(updateType string) string {
	raw := strings.TrimSpace(strings.ToLower(updateType))
	switch raw {
	case "", "progress", "status":
		return "external_dependency"
	case "blocked", "blocker":
		return "blocked"
	case "needs_input", "decision", "decision_needed":
		return "human_input"
	default:
		return raw
	}
}

func isDecisionUpdateType(updateType string) bool {
	switch strings.TrimSpace(strings.ToLower(updateType)) {
	case "needs_input", "decision", "decision_needed", "approval", "approval_required", "escalation":
		return true
	default:
		return false
	}
}
