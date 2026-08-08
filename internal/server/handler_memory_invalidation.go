package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryInvalidationPollParams struct {
	WorkspaceID   string `json:"workspace_id"`
	AgentID       string `json:"agent_id"`
	SessionID     string `json:"session_id,omitempty"`
	IncludeAcked  bool   `json:"include_acked,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	MarkDelivered bool   `json:"mark_delivered,omitempty"`
}

type workspaceMemoryInvalidationAckParams struct {
	WorkspaceID     string   `json:"workspace_id"`
	AgentID         string   `json:"agent_id"`
	InvalidationIDs []string `json:"invalidation_ids"`
}

type workspaceMemoryInvalidationFailParams struct {
	WorkspaceID     string   `json:"workspace_id"`
	AgentID         string   `json:"agent_id"`
	InvalidationIDs []string `json:"invalidation_ids"`
	FailureReason   string   `json:"failure_reason,omitempty"`
}

type workspaceMemoryInvalidationRequeueParams struct {
	WorkspaceID     string   `json:"workspace_id"`
	AgentID         string   `json:"agent_id"`
	InvalidationIDs []string `json:"invalidation_ids"`
}

type workspaceMemoryInvalidationListParams struct {
	WorkspaceID       string `json:"workspace_id"`
	AgentID           string `json:"agent_id"`
	SessionID         string `json:"session_id,omitempty"`
	IncludeAcked      bool   `json:"include_acked,omitempty"`
	IncludeDeadLetter bool   `json:"include_dead_letter,omitempty"`
	Limit             int    `json:"limit,omitempty"`
}

type workspaceMemoryInvalidationGetParams struct {
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	InvalidationID string `json:"invalidation_id"`
}

type workspaceMemoryInvalidationCursorGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	SessionID   string `json:"session_id,omitempty"`
}

func (h *Handler) workspaceMemoryInvalidationPoll(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryInvalidationPollParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireMemoryInvalidationAgentPrincipal(ctx, p.WorkspaceID, p.AgentID); rpcErr != nil {
		return nil, rpcErr
	}
	items, events, err := h.store.PollMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationPollFilter{
		WorkspaceID:   p.WorkspaceID,
		AgentID:       p.AgentID,
		SessionID:     p.SessionID,
		IncludeAcked:  p.IncludeAcked,
		Limit:         p.Limit,
		MarkDelivered: p.MarkDelivered,
	})
	if err != nil {
		if isMemoryInvalidationValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.memory.invalidation.poll")
	}
	if p.MarkDelivered {
		h.publishRuntimeEventBatchChronological(events, func(event sqlite.RuntimeEventRecord) {
			h.publishRuntimeEventRecord(event, "memory invalidation delivered")
		})
	}
	authority, rpcErr := h.memoryInvalidationTimeAuthority(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   p.WorkspaceID,
		"agent_id":       p.AgentID,
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceMemoryInvalidationAck(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryInvalidationAckParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireMemoryInvalidationAgentPrincipal(ctx, p.WorkspaceID, p.AgentID); rpcErr != nil {
		return nil, rpcErr
	}
	items, events, err := h.store.AckMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationAckInput{
		WorkspaceID:     p.WorkspaceID,
		AgentID:         p.AgentID,
		InvalidationIDs: p.InvalidationIDs,
	})
	if err != nil {
		if isMemoryInvalidationValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.memory.invalidation.ack")
	}
	h.publishRuntimeEventBatchChronological(events, func(event sqlite.RuntimeEventRecord) {
		h.publishRuntimeEventRecord(event, "memory invalidation ack")
	})
	authority, rpcErr := h.memoryInvalidationTimeAuthority(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   p.WorkspaceID,
		"agent_id":       p.AgentID,
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
		"status":         "ACKED",
	}, nil
}

func (h *Handler) workspaceMemoryInvalidationFail(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryInvalidationFailParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireMemoryInvalidationAgentPrincipal(ctx, p.WorkspaceID, p.AgentID); rpcErr != nil {
		return nil, rpcErr
	}
	items, events, err := h.store.FailMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationFailInput{
		WorkspaceID:     p.WorkspaceID,
		AgentID:         p.AgentID,
		InvalidationIDs: p.InvalidationIDs,
		FailureReason:   p.FailureReason,
	})
	if err != nil {
		if isMemoryInvalidationValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.memory.invalidation.fail")
	}
	h.publishRuntimeEventBatchChronological(events, func(event sqlite.RuntimeEventRecord) {
		h.publishRuntimeEventRecord(event, "memory invalidation failure")
	})
	authority, rpcErr := h.memoryInvalidationTimeAuthority(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   p.WorkspaceID,
		"agent_id":       p.AgentID,
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceMemoryInvalidationRequeue(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryInvalidationRequeueParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireMemoryInvalidationAgentPrincipal(ctx, p.WorkspaceID, p.AgentID); rpcErr != nil {
		return nil, rpcErr
	}
	items, events, err := h.store.RequeueMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationRequeueInput{
		WorkspaceID:     p.WorkspaceID,
		AgentID:         p.AgentID,
		InvalidationIDs: p.InvalidationIDs,
	})
	if err != nil {
		if isMemoryInvalidationValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.memory.invalidation.requeue")
	}
	h.publishRuntimeEventBatchChronological(events, func(event sqlite.RuntimeEventRecord) {
		h.publishRuntimeEventRecord(event, "memory invalidation requeue")
	})
	authority, rpcErr := h.memoryInvalidationTimeAuthority(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   p.WorkspaceID,
		"agent_id":       p.AgentID,
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceMemoryInvalidationList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryInvalidationListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireMemoryInvalidationAgentPrincipal(ctx, p.WorkspaceID, p.AgentID); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListMemoryInvalidations(ctx, sqlite.MemoryInvalidationListFilter{
		WorkspaceID:       p.WorkspaceID,
		AgentID:           p.AgentID,
		SessionID:         p.SessionID,
		IncludeAcked:      p.IncludeAcked,
		IncludeDeadLetter: p.IncludeDeadLetter,
		Limit:             p.Limit,
	})
	if err != nil {
		if isMemoryInvalidationValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	authority, rpcErr := h.memoryInvalidationTimeAuthority(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	return map[string]any{
		"workspace_id":   p.WorkspaceID,
		"agent_id":       p.AgentID,
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceMemoryInvalidationGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryInvalidationGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.InvalidationID, "invalidation_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireMemoryInvalidationAgentPrincipal(ctx, p.WorkspaceID, p.AgentID); rpcErr != nil {
		return nil, rpcErr
	}
	item, err := h.store.GetMemoryInvalidation(ctx, p.WorkspaceID, p.AgentID, p.InvalidationID)
	if err != nil {
		if isMemoryInvalidationValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return item, nil
}

func (h *Handler) workspaceMemoryInvalidationCursorGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryInvalidationCursorGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireMemoryInvalidationAgentPrincipal(ctx, p.WorkspaceID, p.AgentID); rpcErr != nil {
		return nil, rpcErr
	}
	cursor, err := h.store.GetMemoryInvalidationCursor(ctx, p.WorkspaceID, p.AgentID, p.SessionID)
	if err != nil {
		if isMemoryInvalidationValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return cursor, nil
}

func (h *Handler) memoryInvalidationTimeAuthority(ctx context.Context, workspaceID string) (sqlite.WorkspaceTimeAuthority, *RPCError) {
	authority, err := h.store.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return sqlite.WorkspaceTimeAuthority{}, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return authority, nil
}

func requireMemoryInvalidationAgentPrincipal(ctx context.Context, workspaceID, agentID string) (AuthPrincipal, *RPCError) {
	return requireAgentPrincipal(ctx, workspaceID, agentID, "agent_id")
}

func isMemoryInvalidationValidationError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return false
	}
	return errors.Is(err, sqlite.ErrWorkspaceNotFound) ||
		errors.Is(err, sqlite.ErrAgentNotFound) ||
		errors.Is(err, sqlite.ErrSessionNotFound) ||
		strings.Contains(msg, "memory invalidation not found") ||
		strings.Contains(msg, "memory invalidation cursor not found") ||
		strings.Contains(msg, " is required")
}
