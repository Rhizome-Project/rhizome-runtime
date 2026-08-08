package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryMetricsListParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	ReportScope string `json:"report_scope,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceMemoryMetricsGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	ReportID    string `json:"report_id"`
}

func (h *Handler) workspaceMemoryMetricsReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p sqlite.MemoryMetricsReportInput
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
	result, err := h.store.ReportMemoryMetrics(ctx, p)
	if err != nil {
		if isMemoryMetricsValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.memory.metrics.report")
	}
	h.publishRuntimeEventRecord(result.Event, "memory metrics report")
	return map[string]any{
		"status": "RECORDED",
		"report": result.Report,
		"event":  result.Event,
	}, nil
}

func (h *Handler) workspaceMemoryMetricsList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryMetricsListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListMemoryMetricsReports(ctx, sqlite.MemoryMetricsReportFilter{
		WorkspaceID: p.WorkspaceID,
		AgentID:     p.AgentID,
		SessionID:   p.SessionID,
		ReportScope: p.ReportScope,
		Limit:       p.Limit,
	})
	if err != nil {
		if isMemoryMetricsValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	authority, err := h.store.GetWorkspaceTimeAuthority(ctx, p.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id":   p.WorkspaceID,
		"time_authority": authority,
		"items":          items,
		"count":          len(items),
	}, nil
}

func (h *Handler) workspaceMemoryMetricsGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryMetricsGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ReportID, "report_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	record, err := h.store.GetMemoryMetricsReport(ctx, p.WorkspaceID, p.ReportID)
	if err != nil {
		if isMemoryMetricsValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return record, nil
}

func isMemoryMetricsValidationError(err error) bool {
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
		strings.Contains(msg, "memory metrics report not found") ||
		strings.Contains(msg, " is required") ||
		strings.Contains(msg, "invalid report_scope") ||
		strings.Contains(msg, "invalid metrics timestamp") ||
		strings.Contains(msg, "must be <=") ||
		strings.Contains(msg, "cannot exceed") ||
		strings.Contains(msg, "counts must be >=") ||
		strings.Contains(msg, "belongs to another agent") ||
		strings.Contains(msg, "already belongs to")
}
