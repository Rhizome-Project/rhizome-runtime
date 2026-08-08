package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceInstrumentationCorridorAuthorityParams struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

func (h *Handler) workspaceInstrumentationCorridorAuthorityReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationCorridorAuthorityParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildCorridorAuthorityReport(ctx, sqlite.CorridorAuthorityFilter{
		WorkspaceID: p.WorkspaceID,
		TaskID:      p.TaskID,
		Limit:       p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id": strings.TrimSpace(report.WorkspaceID),
		"report":       report,
	}, nil
}

func (h *Handler) workspaceInstrumentationCorridorAuthorityTask(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationCorridorAuthorityParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.TaskID, "task_id"); rpcErr != nil {
		return nil, rpcErr
	}
	detail, err := h.store.BuildCorridorAuthorityTaskDetail(ctx, p.WorkspaceID, p.TaskID)
	if err != nil {
		if isControlPlaneValidationError(err) || err == sqlite.ErrTaskNotFound {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id": strings.TrimSpace(p.WorkspaceID),
		"task_id":      strings.TrimSpace(p.TaskID),
		"detail":       detail,
	}, nil
}
