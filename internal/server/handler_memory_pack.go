package server

import (
	"context"
	"encoding/json"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryPackListParams struct {
	WorkspaceID string `json:"workspace_id"`
	PackType    string `json:"pack_type,omitempty"`
	PackMode    string `json:"pack_mode,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceMemoryPackGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	PackID      string `json:"pack_id"`
}

func (h *Handler) workspaceMemoryPackList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryPackListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListEpisodePacks(ctx, sqlite.EpisodePackFilter{
		WorkspaceID: p.WorkspaceID,
		PackType:    p.PackType,
		PackMode:    p.PackMode,
		SessionID:   p.SessionID,
		AgentID:     p.AgentID,
		TaskID:      p.TaskID,
		Limit:       p.Limit,
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id": p.WorkspaceID,
		"pack_source":  "episode_pack",
		"items":        items,
		"count":        len(items),
	}, nil
}

func (h *Handler) workspaceMemoryPackGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryPackGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.PackID, "pack_id"); rpcErr != nil {
		return nil, rpcErr
	}
	record, err := h.store.GetEpisodePack(ctx, p.WorkspaceID, p.PackID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id": p.WorkspaceID,
		"pack_id":      p.PackID,
		"pack_source":  "episode_pack",
		"pack":         record,
	}, nil
}
