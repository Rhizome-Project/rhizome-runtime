package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceNodesListParams struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id,omitempty"`
	Status      string `json:"status,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

func nodeLifecyclePayload(taskID, nodeID, agentID, status, summary, reason string) map[string]string {
	return map[string]string{
		"task_id":  taskID,
		"node_id":  nodeID,
		"agent_id": agentID,
		"status":   status,
		"summary":  summary,
		"reason":   reason,
	}
}

func (h *Handler) workspaceNodesList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceNodesListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	nodes, err := h.store.ListWorkspaceNodes(ctx, sqlite.WorkspaceNodeFilter{
		WorkspaceID: p.WorkspaceID,
		TaskID:      p.TaskID,
		Status:      p.Status,
		Limit:       p.Limit,
	})
	if err != nil {
		if err == sqlite.ErrWorkspaceNotFound {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	return map[string]any{"nodes": nodes}, nil
}

type agentNodeClaimParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	TaskID      string `json:"task_id"`
	NodeID      string `json:"node_id"`
	Summary     string `json:"summary"`
}

func (h *Handler) agentNodeClaim(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentNodeClaimParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	event, err := h.store.ClaimNodeWithEvent(ctx, sqlite.NodeClaimInput{
		WorkspaceID:           p.WorkspaceID,
		TaskID:                p.TaskID,
		NodeID:                p.NodeID,
		AgentID:               p.AgentID,
		Summary:               p.Summary,
		PromptContextEnvelope: h.agentNodePromptContextEnvelope(ctx, p.WorkspaceID, "agent.node.claim", p.TaskID, p.NodeID, p.AgentID),
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "agent.node.claim"); rpcErr != nil {
			return nil, rpcErr
		}
		if err == sqlite.ErrNodeClaimConflict || err == sqlite.ErrNodeNotPending || err == sqlite.ErrNodeNotFound {
			return nil, &RPCError{Code: -32014, Message: err.Error()} // Custom app error code
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	if strings.TrimSpace(event.EventID) != "" {
		h.touchAgentActivity(ctx, p.WorkspaceID, p.AgentID)
		h.publishRuntimeEventRecord(event, "Node "+p.NodeID+" claimed by agent "+p.AgentID)
	}

	return map[string]any{
		"workspace_id": p.WorkspaceID,
		"task_id":      p.TaskID,
		"node_id":      p.NodeID,
		"agent_id":     p.AgentID,
		"status":       "CLAIMED",
	}, nil
}

type agentNodeReleaseParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	TaskID      string `json:"task_id"`
	NodeID      string `json:"node_id"`
	Reason      string `json:"reason"`
}

func (h *Handler) agentNodeRelease(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentNodeReleaseParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	event, err := h.store.ReleaseNodeClaimWithEvent(ctx, sqlite.NodeReleaseInput{
		WorkspaceID:           p.WorkspaceID,
		TaskID:                p.TaskID,
		NodeID:                p.NodeID,
		AgentID:               p.AgentID,
		Reason:                p.Reason,
		PromptContextEnvelope: h.agentNodePromptContextEnvelope(ctx, p.WorkspaceID, "agent.node.release", p.TaskID, p.NodeID, p.AgentID),
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "agent.node.release"); rpcErr != nil {
			return nil, rpcErr
		}
		if err == sqlite.ErrNodeClaimConflict {
			return nil, &RPCError{Code: -32014, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	if strings.TrimSpace(event.EventID) != "" {
		h.touchAgentActivity(ctx, p.WorkspaceID, p.AgentID)
		h.publishRuntimeEventRecord(event, "Node "+p.NodeID+" released by agent "+p.AgentID)
	}

	return map[string]any{
		"workspace_id": p.WorkspaceID,
		"task_id":      p.TaskID,
		"node_id":      p.NodeID,
		"agent_id":     p.AgentID,
		"status":       "RELEASED",
	}, nil
}

type agentNodeCompleteParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	TaskID      string `json:"task_id"`
	NodeID      string `json:"node_id"`
	Summary     string `json:"summary"`
}

func (h *Handler) agentNodeComplete(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentNodeCompleteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	event, err := h.store.CompleteNodeClaimWithEvent(ctx, sqlite.NodeCompleteInput{
		WorkspaceID:           p.WorkspaceID,
		TaskID:                p.TaskID,
		NodeID:                p.NodeID,
		AgentID:               p.AgentID,
		Summary:               p.Summary,
		PromptContextEnvelope: h.agentNodePromptContextEnvelope(ctx, p.WorkspaceID, "agent.node.complete", p.TaskID, p.NodeID, p.AgentID),
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "agent.node.complete"); rpcErr != nil {
			return nil, rpcErr
		}
		if err == sqlite.ErrNodeClaimConflict {
			return nil, &RPCError{Code: -32014, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	if strings.TrimSpace(event.EventID) != "" {
		h.touchAgentActivity(ctx, p.WorkspaceID, p.AgentID)
		h.publishRuntimeEventRecord(event, "Node "+p.NodeID+" completed by agent "+p.AgentID)
	}

	return map[string]any{
		"workspace_id": p.WorkspaceID,
		"task_id":      p.TaskID,
		"node_id":      p.NodeID,
		"agent_id":     p.AgentID,
		"status":       "COMPLETED",
	}, nil
}

func (h *Handler) agentNodePromptContextEnvelope(ctx context.Context, workspaceID, surface, taskID, nodeID, agentID string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildNodeLifecyclePromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
	envelope["actor_agent_id"] = strings.TrimSpace(agentID)
	envelope["agent_id"] = strings.TrimSpace(agentID)
	envelope["task_id"] = strings.TrimSpace(taskID)
	envelope["node_id"] = strings.TrimSpace(nodeID)
	switch strings.TrimSpace(surface) {
	case "agent.node.claim":
		envelope["status"] = "CLAIMED"
		envelope["node_claim_status"] = "CLAIMED"
	case "agent.node.release":
		envelope["status"] = "RELEASED"
		envelope["node_claim_status"] = "RELEASED"
	case "agent.node.complete":
		envelope["status"] = "COMPLETED"
		envelope["node_claim_status"] = "COMPLETED"
	}
	return envelope
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
