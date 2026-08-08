package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type agentSessionEventParams struct {
	WorkspaceID         string                         `json:"workspace_id"`
	SessionID           string                         `json:"session_id"`
	AgentID             string                         `json:"agent_id"`
	TaskID              string                         `json:"task_id,omitempty"`
	Summary             string                         `json:"summary"`
	Status              string                         `json:"status,omitempty"`
	OwnerScope          string                         `json:"owner_scope,omitempty"`
	BlockedOn           []model.AgentUpdateBlockedRef  `json:"blocked_on,omitempty"`
	DecisionNeededFrom  string                         `json:"decision_needed_from,omitempty"`
	DecisionType        string                         `json:"decision_type,omitempty"`
	KeepSessionActive   *bool                          `json:"keep_session_active,omitempty"`
	HandoffTo           string                         `json:"handoff_to,omitempty"`
	RelatedDocKeys      []string                       `json:"related_doc_keys,omitempty"`
	RelatedArtifactRefs []model.AgentUpdateArtifactRef `json:"related_artifact_refs,omitempty"`
	Iterations          int                            `json:"iterations,omitempty"`
	TotalInputTokens    int                            `json:"total_input_tokens,omitempty"`
	TotalOutputTokens   int                            `json:"total_output_tokens,omitempty"`
	ToolCalls           int                            `json:"tool_calls,omitempty"`
	UpdatedAt           string                         `json:"updated_at,omitempty"`
}

type workspaceSessionsListParams struct {
	WorkspaceID string `json:"workspace_id"`
	ActiveOnly  *bool  `json:"active_only,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type agentSessionTakeoverParams struct {
	WorkspaceID        string `json:"workspace_id"`
	SessionID          string `json:"session_id"`
	TakeoverAgentID    string `json:"takeover_agent_id"`
	Summary            string `json:"summary"`
	SuccessorSessionID string `json:"successor_session_id,omitempty"`
	SuccessorSummary   string `json:"successor_summary,omitempty"`
	UpdatedAt          string `json:"updated_at,omitempty"`
}

func (h *Handler) sessionPromptContextEnvelope(ctx context.Context, workspaceID, surface string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	return sqlite.BuildSessionPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
}

func (h *Handler) agentSessionStart(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.agentSessionEvent(ctx, raw, model.SessionEventStart)
}

func (h *Handler) agentSessionStatus(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.agentSessionEvent(ctx, raw, model.SessionEventStatus)
}

func (h *Handler) agentSessionBlocked(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.agentSessionEvent(ctx, raw, model.SessionEventBlocked)
}

func (h *Handler) agentSessionDecisionNeeded(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.agentSessionEvent(ctx, raw, model.SessionEventDecisionNeeded)
}

func (h *Handler) agentSessionKeepalive(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.agentSessionEvent(ctx, raw, model.SessionEventKeepalive)
}

func (h *Handler) agentSessionEnd(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.agentSessionEvent(ctx, raw, model.SessionEventEnd)
}

func (h *Handler) agentSessionTakeover(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentSessionTakeoverParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.SessionID, "session_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.TakeoverAgentID, "takeover_agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Summary, "summary"); rpcErr != nil {
		return nil, rpcErr
	}

	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.TakeoverAgentID, "takeover_agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	record, event, err := h.store.TakeOverAgentSessionWithEvent(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:           p.WorkspaceID,
		SessionID:             p.SessionID,
		SuccessorSessionID:    p.SuccessorSessionID,
		TakeoverAgentID:       p.TakeoverAgentID,
		Summary:               p.Summary,
		SuccessorSummary:      p.SuccessorSummary,
		UpdatedAt:             p.UpdatedAt,
		PromptContextEnvelope: h.sessionPromptContextEnvelope(ctx, p.WorkspaceID, "agent.session.takeover"),
	})
	if err != nil {
		if errors.Is(err, sqlite.ErrSessionNotFound) || isSessionValidationError(err) ||
			errors.Is(err, sqlite.ErrSessionNotActive) ||
			errors.Is(err, sqlite.ErrSessionTakeoverNotAuthorized) ||
			errors.Is(err, sqlite.ErrSessionTakeoverSuccessorExists) ||
			errors.Is(err, sqlite.ErrSessionTakeoverAgentSame) ||
			errors.Is(err, sqlite.ErrSessionTakeoverSuccessorSame) {
			code := errCodeInvalidParams
			if errors.Is(err, sqlite.ErrSessionTakeoverNotAuthorized) {
				code = errCodePermissionDenied
			}
			return nil, &RPCError{Code: code, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "agent.session.takeover")
	}

	actions := []runtimeEventPublishAction{{
		Event: event,
		Publish: func(event sqlite.RuntimeEventRecord) {
			h.publishRuntimeEventRecordAs(event, "agent.session.takeover", record.SuccessorState.Summary, record.SourceState.SessionID)
		},
	}}
	actions = append(actions, h.syncSessionEventMemoryActions(ctx, record.SourceState)...)
	actions = append(actions, h.syncSessionEventMemoryActions(ctx, record.SuccessorState)...)
	actions = append(actions, h.syncSessionOperatorQueueActions(ctx, record.SourceState)...)
	actions = append(actions, h.syncSessionOperatorQueueActions(ctx, record.SuccessorState)...)
	actions = append(actions, h.syncExecutionRunFromSessionActions(ctx, record.SourceState)...)
	actions = append(actions, h.syncExecutionRunFromSessionActions(ctx, record.SuccessorState)...)
	h.touchAgentActivity(ctx, record.SourceState.WorkspaceID, record.SourceState.AgentID)
	h.touchAgentActivity(ctx, record.SuccessorState.WorkspaceID, record.SuccessorState.AgentID)
	h.publishRuntimeEventActionsChronological(actions...)

	return map[string]any{
		"source_state":    record.SourceState,
		"successor_state": record.SuccessorState,
		"status":          "TAKEN_OVER",
	}, nil
}

func (h *Handler) agentSessionEvent(ctx context.Context, raw json.RawMessage, eventType string) (any, *RPCError) {
	var p agentSessionEventParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.SessionID, "session_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Summary, "summary"); rpcErr != nil {
		return nil, rpcErr
	}

	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	state, event, err := h.store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:             eventType,
		WorkspaceID:           p.WorkspaceID,
		SessionID:             p.SessionID,
		AgentID:               p.AgentID,
		TaskID:                p.TaskID,
		Summary:               p.Summary,
		Status:                p.Status,
		OwnerScope:            p.OwnerScope,
		BlockedOn:             p.BlockedOn,
		DecisionNeededFrom:    p.DecisionNeededFrom,
		DecisionType:          p.DecisionType,
		KeepSessionActive:     p.KeepSessionActive,
		HandoffTo:             p.HandoffTo,
		RelatedDocKeys:        p.RelatedDocKeys,
		RelatedArtifactRefs:   p.RelatedArtifactRefs,
		Iterations:            p.Iterations,
		TotalInputTokens:      p.TotalInputTokens,
		TotalOutputTokens:     p.TotalOutputTokens,
		ToolCalls:             p.ToolCalls,
		UpdatedAt:             p.UpdatedAt,
		PromptContextEnvelope: h.sessionPromptContextEnvelope(ctx, p.WorkspaceID, "agent."+canonicalSessionSurface(eventType)),
	})
	if err != nil {
		if errors.Is(err, sqlite.ErrSessionOwnershipMismatch) {
			return nil, &RPCError{Code: errCodePermissionDenied, Message: err.Error()}
		}
		if errors.Is(err, sqlite.ErrSessionNotActive) || errors.Is(err, sqlite.ErrSessionNotFound) ||
			errors.Is(err, sqlite.ErrWorkspaceTaskAbsent) || isSessionValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "agent."+canonicalSessionSurface(eventType))
	}

	canonicalEventType := model.NormalizeSessionEventType(eventType)
	actions := []runtimeEventPublishAction{{
		Event: event,
		Publish: func(event sqlite.RuntimeEventRecord) {
			h.publishRuntimeEventRecordAs(event, "agent."+canonicalEventType, sessionEventSummary(state), state.SessionID)
		},
	}}
	actions = append(actions, h.syncSessionEventMemoryActions(ctx, state)...)
	actions = append(actions, h.syncSessionOperatorQueueActions(ctx, state)...)
	actions = append(actions, h.syncExecutionRunFromSessionActions(ctx, state)...)
	h.touchAgentActivity(ctx, state.WorkspaceID, state.AgentID)
	h.publishRuntimeEventActionsChronological(actions...)

	return map[string]any{
		"session_id": state.SessionID,
		"event_type": canonicalEventType,
		"state":      state,
		"status":     "RECORDED",
	}, nil
}

func (h *Handler) workspaceSessionsList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceSessionsListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	activeOnly := true
	if p.ActiveOnly != nil {
		activeOnly = *p.ActiveOnly
	}
	limit := p.Limit
	if limit <= 0 {
		limit = 20
	}

	sessions, err := h.store.ListWorkspaceSessionStates(ctx, workspaceID, activeOnly, limit)
	if err != nil {
		if isSessionValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	return map[string]any{
		"workspace_id": workspaceID,
		"active_only":  activeOnly,
		"sessions":     sessions,
		"count":        len(sessions),
	}, nil
}

func canonicalSessionSurface(eventType string) string {
	switch model.NormalizeSessionEventType(eventType) {
	case model.SessionEventStart:
		return "session.start"
	case model.SessionEventStatus:
		return "session.status"
	case model.SessionEventBlocked:
		return "session.blocked"
	case model.SessionEventDecisionNeeded:
		return "session.decision_needed"
	case model.SessionEventKeepalive:
		return "session.keepalive"
	case model.SessionEventEnd:
		return "session.end"
	default:
		return model.NormalizeSessionEventType(eventType)
	}
}

func sessionEventSummary(state sqlite.AgentSessionStateRecord) string {
	if strings.TrimSpace(state.Summary) != "" {
		return state.Summary
	}
	if strings.TrimSpace(state.SessionID) != "" {
		return state.SessionID
	}
	return strings.TrimSpace(state.UpdateType)
}

func isSessionValidationError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return false
	}
	return strings.Contains(msg, " is required") ||
		strings.Contains(msg, "invalid session event type") ||
		strings.Contains(msg, "session payload ") ||
		strings.Contains(msg, "session start requires ") ||
		strings.Contains(msg, "task is not attached to workspace")
}

func (h *Handler) syncSessionOperatorQueue(ctx context.Context, state sqlite.AgentSessionStateRecord) {
	h.publishRuntimeEventActionsChronological(h.syncSessionOperatorQueueActions(ctx, state)...)
}

func (h *Handler) syncSessionOperatorQueueActions(ctx context.Context, state sqlite.AgentSessionStateRecord) []runtimeEventPublishAction {
	result, err := h.store.SyncOperatorQueueFromSessionState(ctx, state)
	if err != nil {
		return nil
	}
	actions := make([]runtimeEventPublishAction, 0, len(result.Opened)+len(result.Resolved))
	for _, item := range result.Opened {
		if item.Event.EventID == "" {
			continue
		}
		item := item
		actions = append(actions, runtimeEventPublishAction{
			Event: item.Event,
			Publish: func(event sqlite.RuntimeEventRecord) {
				h.publishOperatorQueueEventRecord(event, "workspace.ops.updated", item.Record)
			},
		})
	}
	for _, item := range result.Resolved {
		if item.Event.EventID == "" {
			continue
		}
		item := item
		actions = append(actions, runtimeEventPublishAction{
			Event: item.Event,
			Publish: func(event sqlite.RuntimeEventRecord) {
				h.publishOperatorQueueEventRecord(event, "workspace.ops.resolved", item.Record)
			},
		})
	}
	return actions
}

func (h *Handler) syncExecutionRunFromSession(ctx context.Context, state sqlite.AgentSessionStateRecord) {
	h.publishRuntimeEventActionsChronological(h.syncExecutionRunFromSessionActions(ctx, state)...)
}

func (h *Handler) syncExecutionRunFromSessionActions(ctx context.Context, state sqlite.AgentSessionStateRecord) []runtimeEventPublishAction {
	result, err := h.store.SyncExecutionRunFromSessionStateWithResult(ctx, state)
	if err != nil {
		return nil
	}
	actions := make([]runtimeEventPublishAction, 0, 2)
	if strings.TrimSpace(result.RunEvent.EventID) != "" {
		run := result.Run
		actions = append(actions, runtimeEventPublishAction{
			Event: result.RunEvent,
			Publish: func(event sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecordEnvelopeAs(event, "workspace.execution.run", string(mustJSON(run)), firstNonEmpty(run.Summary, run.Title, run.RunID))
			},
		})
	}
	if strings.TrimSpace(result.StepEvent.EventID) != "" {
		step := result.Step
		actions = append(actions, runtimeEventPublishAction{
			Event: result.StepEvent,
			Publish: func(event sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecordEnvelopeAs(event, "workspace.execution.step", string(mustJSON(step)), firstNonEmpty(step.Summary, step.Title, step.StepID))
			},
		})
	}
	return actions
}
