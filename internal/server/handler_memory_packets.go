package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryPacketParams struct {
	WorkspaceID    string                     `json:"workspace_id"`
	TaskID         string                     `json:"task_id,omitempty"`
	SessionID      string                     `json:"session_id,omitempty"`
	AgentID        string                     `json:"agent_id,omitempty"`
	DocKeys        []string                   `json:"doc_keys,omitempty"`
	ArtifactRefs   []string                   `json:"artifact_refs,omitempty"`
	IncludeAllDocs bool                       `json:"include_all_docs,omitempty"`
	Budget         *sqlite.MemoryPacketBudget `json:"budget,omitempty"`
}

func (h *Handler) workspaceMemoryPacketKernel(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryPacketParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if strings.TrimSpace(p.TaskID) == "" && strings.TrimSpace(p.SessionID) == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "task_id or session_id is required"}
	}

	packet, err := h.store.BuildMemoryKernelPacket(ctx, sqlite.MemoryPacketFilter{
		WorkspaceID:    p.WorkspaceID,
		TaskID:         p.TaskID,
		SessionID:      p.SessionID,
		AgentID:        p.AgentID,
		DocKeys:        append([]string(nil), p.DocKeys...),
		ArtifactRefs:   append([]string(nil), p.ArtifactRefs...),
		IncludeAllDocs: p.IncludeAllDocs,
		Budget:         p.Budget,
	})
	if err != nil {
		return nil, memoryPacketRPCError(err)
	}
	return map[string]any{
		"workspace_id": strings.TrimSpace(packet.Meta.WorkspaceID),
		"packet":       packet,
	}, nil
}

func (h *Handler) workspaceMemoryPacketShell(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryPacketParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if strings.TrimSpace(p.TaskID) == "" && strings.TrimSpace(p.SessionID) == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "task_id or session_id is required"}
	}

	packet, err := h.store.BuildMemoryShellPacket(ctx, sqlite.MemoryPacketFilter{
		WorkspaceID:    p.WorkspaceID,
		TaskID:         p.TaskID,
		SessionID:      p.SessionID,
		AgentID:        p.AgentID,
		DocKeys:        append([]string(nil), p.DocKeys...),
		ArtifactRefs:   append([]string(nil), p.ArtifactRefs...),
		IncludeAllDocs: p.IncludeAllDocs,
		Budget:         p.Budget,
	})
	if err != nil {
		return nil, memoryPacketRPCError(err)
	}
	return map[string]any{
		"workspace_id": strings.TrimSpace(packet.Meta.WorkspaceID),
		"packet":       packet,
	}, nil
}

func memoryPacketRPCError(err error) *RPCError {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, sqlite.ErrWorkspaceNotFound),
		errors.Is(err, sqlite.ErrAgentNotFound),
		errors.Is(err, sqlite.ErrTaskNotFound),
		errors.Is(err, sqlite.ErrWorkspaceTaskAbsent),
		errors.Is(err, sqlite.ErrSessionNotFound):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	case strings.Contains(strings.ToLower(err.Error()), "required"),
		strings.Contains(strings.ToLower(err.Error()), "does not belong"),
		strings.Contains(strings.ToLower(err.Error()), "not found"):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	default:
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
}
