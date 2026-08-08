package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceMemoryResidencyListParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	ReportScope string `json:"report_scope,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type workspaceMemoryResidencyGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	ReportID    string `json:"report_id"`
}

func (h *Handler) workspaceMemoryResidencyReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p sqlite.MemoryResidencyReportInput
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
	result, err := h.store.ReportMemoryResidency(ctx, p)
	if err != nil {
		if isMemoryResidencyValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(err, "workspace.memory.residency.report")
	}
	events := append([]sqlite.RuntimeEventRecord{}, result.InvalidationEvents...)
	events = append(events, result.Event)
	h.publishRuntimeEventBatchChronological(events, func(event sqlite.RuntimeEventRecord) {
		switch event.EventType {
		case "memory.invalidation_enqueued":
			h.publishRuntimeEventRecord(event, "memory invalidation enqueue")
		case "memory.invalidation_refreshed":
			h.publishRuntimeEventRecord(event, "memory invalidation refresh")
		default:
			h.publishRuntimeEventRecord(event, "memory residency report")
		}
	})
	return map[string]any{
		"status": "RECORDED",
		"report": result.Report,
		"event":  result.Event,
	}, nil
}

func (h *Handler) workspaceMemoryResidencyList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryResidencyListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListMemoryResidencyReports(ctx, sqlite.MemoryResidencyReportFilter{
		WorkspaceID: p.WorkspaceID,
		AgentID:     p.AgentID,
		SessionID:   p.SessionID,
		ReportScope: p.ReportScope,
		Limit:       p.Limit,
	})
	if err != nil {
		if isMemoryResidencyValidationError(err) {
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

func (h *Handler) workspaceMemoryResidencyGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMemoryResidencyGetParams
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
	detail, err := h.store.GetMemoryResidencyReport(ctx, p.WorkspaceID, p.ReportID)
	if err != nil {
		if isMemoryResidencyValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return detail, nil
}

func isMemoryResidencyValidationError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return false
	}
	return errors.Is(err, sqlite.ErrWorkspaceNotFound) ||
		errors.Is(err, sqlite.ErrAgentNotFound) ||
		strings.Contains(msg, "memory residency report not found") ||
		strings.Contains(msg, " is required") ||
		strings.Contains(msg, "invalid report_scope") ||
		strings.Contains(msg, "invalid residency_tier") ||
		strings.Contains(msg, "invalid coherence_class") ||
		strings.Contains(msg, "invalid state") ||
		strings.Contains(msg, "already belongs to")
}
