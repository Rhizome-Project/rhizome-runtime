package server

import (
	"context"
	"encoding/json"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryNodeSearchParams struct {
	WorkspaceID     string `json:"workspace_id"`
	Query           string `json:"query,omitempty"`
	MemoryType      string `json:"memory_type,omitempty"`
	MemoryLayer     string `json:"memory_layer,omitempty"`
	Visibility      string `json:"visibility,omitempty"`
	EpistemicStatus string `json:"epistemic_status,omitempty"`
	LifecycleState  string `json:"lifecycle_state,omitempty"`
	OriginKind      string `json:"origin_kind,omitempty"`
	OriginID        string `json:"origin_id,omitempty"`
	SourceKind      string `json:"source_kind,omitempty"`
	AgentID         string `json:"agent_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	TaskID          string `json:"task_id,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

func (h *Handler) workspaceMemoryNodeSearch(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryNodeSearchParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Query, "query"); rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.SearchMemoryNodes(ctx, sqlite.MemoryNodeSearchFilter{
		WorkspaceID:     p.WorkspaceID,
		Query:           p.Query,
		MemoryType:      p.MemoryType,
		MemoryLayer:     p.MemoryLayer,
		Visibility:      p.Visibility,
		EpistemicStatus: p.EpistemicStatus,
		LifecycleState:  p.LifecycleState,
		OriginKind:      p.OriginKind,
		OriginID:        p.OriginID,
		SourceKind:      p.SourceKind,
		AgentID:         p.AgentID,
		SessionID:       p.SessionID,
		TaskID:          p.TaskID,
		IncludeArchived: p.IncludeArchived,
		Limit:           p.Limit,
	})
	if err != nil {
		if isMemoryGraphValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return result, nil
}
