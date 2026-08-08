package server

import (
	"context"
	"encoding/json"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func (h *Handler) workspaceRSPTelemetryDump(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var in struct {
		WorkspaceID string `json:"workspace_id"`
		Limit       int    `json:"limit,omitempty"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	result, err := h.store.DumpRSPTelemetry(ctx, sqlite.RSPTelemetryDumpFilter{
		WorkspaceID: in.WorkspaceID,
		Limit:       in.Limit,
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	return result, nil
}
