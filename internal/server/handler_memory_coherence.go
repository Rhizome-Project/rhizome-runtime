package server

import (
	"context"
	"encoding/json"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryCoherenceReportParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	ReportScope string `json:"report_scope,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceMemoryCoherenceScopeParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	SessionID   string `json:"session_id,omitempty"`
	ReportScope string `json:"report_scope,omitempty"`
}

func (h *Handler) workspaceMemoryCoherenceReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryCoherenceReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildMemoryCoherenceReport(ctx, sqlite.MemoryCoherenceReportFilter{
		WorkspaceID: p.WorkspaceID,
		AgentID:     p.AgentID,
		SessionID:   p.SessionID,
		ReportScope: p.ReportScope,
		Limit:       p.Limit,
	})
	if err != nil {
		if isMemoryCoherenceValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return report, nil
}

func (h *Handler) workspaceMemoryCoherenceScope(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryCoherenceScopeParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.GetMemoryCoherenceScope(ctx, p.WorkspaceID, p.AgentID, p.SessionID, p.ReportScope)
	if err != nil {
		if isMemoryCoherenceValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return report, nil
}

func (h *Handler) workspaceMemoryCoherenceSnapshot(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryCoherenceReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.SnapshotMemoryCoherenceReport(ctx, sqlite.MemoryCoherenceReportFilter{
		WorkspaceID: p.WorkspaceID,
		AgentID:     p.AgentID,
		SessionID:   p.SessionID,
		ReportScope: p.ReportScope,
		Limit:       p.Limit,
	})
	if err != nil {
		if isMemoryCoherenceValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.memory.coherence.snapshot")
	}
	h.publishRuntimeEventRecord(result.Event, "memory coherence snapshot")
	return map[string]any{
		"status": "RECORDED",
		"report": result.Report,
		"event":  result.Event,
	}, nil
}

func isMemoryCoherenceValidationError(err error) bool {
	if err == nil {
		return false
	}
	return isMemoryMetricsValidationError(err) || isMemoryResidencyValidationError(err)
}
