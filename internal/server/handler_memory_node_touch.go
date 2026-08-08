package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryNodeTouchParams struct {
	WorkspaceID string `json:"workspace_id"`
	NodeID      string `json:"node_id"`
	Trusted     bool   `json:"trusted"`
	Actor       string `json:"actor"`
}

func (h *Handler) workspaceMemoryNodeTouch(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryNodeTouchParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.NodeID, "node_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Actor, "actor"); rpcErr != nil {
		return nil, rpcErr
	}

	workspaceID := strings.TrimSpace(p.WorkspaceID)
	nodeID := strings.TrimSpace(p.NodeID)
	actor := strings.TrimSpace(p.Actor)

	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actor, "actor")
	if rpcErr != nil {
		return nil, rpcErr
	}

	envelope := sqlite.BuildWorkspaceMemoryPromptContextEnvelope(
		"workspace.memory.node.touch",
		"server_rpc",
		workspaceID,
		principal.PrincipalType,
		principal.PrincipalID,
	)
	result, err := h.store.TouchMemoryNodeWithEvent(ctx, sqlite.MemoryNodeTouchInput{
		WorkspaceID:           workspaceID,
		NodeID:                nodeID,
		Trusted:               p.Trusted,
		RiskAgent:             0.0,
		SalienceConfig:        sqlite.DefaultRMPSalienceConfig(),
		ActorType:             principal.PrincipalType,
		ActorID:               actor,
		PromptContextEnvelope: envelope,
	})
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.memory.node.touch")
	}

	h.publishRuntimeEventRecordAs(result.RuntimeEvent, "workspace.memory.node.touched", "Memory node touched: "+nodeID)
	return map[string]any{
		"status":     "ok",
		"event_id":   result.RuntimeEvent.EventID,
		"ingest_seq": result.RuntimeEvent.IngestSeq,
		"salience":   result.Salience,
	}, nil
}
