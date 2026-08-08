package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceRSPBeliefReportParams struct {
	WorkspaceID string `json:"workspace_id"`
	ClaimType   string `json:"claim_type,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceRSPBeliefClaimParams struct {
	WorkspaceID string `json:"workspace_id"`
	ClaimID     string `json:"claim_id"`
}

func (h *Handler) workspaceRSPBeliefReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceRSPBeliefReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildRSPBeliefReport(ctx, sqlite.RSPBeliefReportFilter{
		WorkspaceID: p.WorkspaceID,
		ClaimType:   p.ClaimType,
		AgentID:     p.AgentID,
		SessionID:   p.SessionID,
		TaskID:      p.TaskID,
		Limit:       p.Limit,
	})
	if err != nil {
		return nil, rspBeliefRPCError(err)
	}
	return report, nil
}

func (h *Handler) workspaceRSPBeliefClaim(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceRSPBeliefClaimParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ClaimID, "claim_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	item, err := h.store.GetRSPBeliefClaim(ctx, p.WorkspaceID, p.ClaimID)
	if err != nil {
		return nil, rspBeliefRPCError(err)
	}
	return item, nil
}

func (h *Handler) workspaceRSPBeliefSnapshot(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceRSPBeliefReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.SnapshotRSPBeliefReport(ctx, sqlite.RSPBeliefReportFilter{
		WorkspaceID: p.WorkspaceID,
		ClaimType:   p.ClaimType,
		AgentID:     p.AgentID,
		SessionID:   p.SessionID,
		TaskID:      p.TaskID,
		Limit:       p.Limit,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.rsp.belief.snapshot"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, rspBeliefRPCError(err)
	}
	h.publishRuntimeEventRecord(result.Event, "rsp belief snapshot")
	return map[string]any{
		"status": "RECORDED",
		"report": result.Report,
		"event":  result.Event,
	}, nil
}

func rspBeliefRPCError(err error) *RPCError {
	if err == nil {
		return nil
	}
	if sqliteErr := sqliteErrorPermission(err); sqliteErr != nil {
		return sqliteErr
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "required"),
		strings.Contains(message, "not found"),
		strings.Contains(message, "not supported"):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	default:
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
}
