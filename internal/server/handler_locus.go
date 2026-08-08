package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceInstrumentationLocusParams struct {
	WorkspaceID    string                     `json:"workspace_id"`
	ProtoClusterID string                     `json:"proto_cluster_id,omitempty"`
	AgentID        string                     `json:"agent_id,omitempty"`
	TaskID         string                     `json:"task_id,omitempty"`
	SessionID      string                     `json:"session_id,omitempty"`
	DocKeys        []string                   `json:"doc_keys,omitempty"`
	ArtifactRefs   []string                   `json:"artifact_refs,omitempty"`
	FrontierLimit  int                        `json:"frontier_limit,omitempty"`
	MemoryBudget   *sqlite.MemoryPacketBudget `json:"memory_budget,omitempty"`
}

func (h *Handler) workspaceInstrumentationLocusBundle(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationLocusParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}

	bundle, err := h.store.BuildInstrumentationLocusBundle(ctx, sqlite.InstrumentationLocusFilter{
		WorkspaceID:    p.WorkspaceID,
		ProtoClusterID: p.ProtoClusterID,
		AgentID:        p.AgentID,
		TaskID:         p.TaskID,
		SessionID:      p.SessionID,
		DocKeys:        append([]string(nil), p.DocKeys...),
		ArtifactRefs:   append([]string(nil), p.ArtifactRefs...),
		FrontierLimit:  p.FrontierLimit,
		MemoryBudget:   p.MemoryBudget,
	})
	if err != nil {
		if isControlPlaneValidationError(err) || strings.Contains(strings.ToLower(err.Error()), "not found") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	if memoryPacket, pktErr := h.store.BuildMemoryKernelPacket(ctx, sqlite.MemoryPacketFilter{
		WorkspaceID:  bundle.WorkspaceID,
		AgentID:      p.AgentID,
		SessionID:    p.SessionID,
		TaskID:       p.TaskID,
		DocKeys:      p.DocKeys,
		ArtifactRefs: p.ArtifactRefs,
		Budget:       p.MemoryBudget,
	}); pktErr == nil {
		bundle.MemoryPacket = &memoryPacket
	}

	return map[string]any{
		"workspace_id": strings.TrimSpace(bundle.WorkspaceID),
		"bundle":       bundle,
	}, nil
}
