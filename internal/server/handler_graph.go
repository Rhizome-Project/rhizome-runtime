package server

import (
	"context"
	"encoding/json"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func (h *Handler) workspaceGraphSnapshot(ctx context.Context, params json.RawMessage) (any, *RPCError) {
	var req sqlite.GraphSnapshotRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	if req.WorkspaceID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id is required"}
	}

	if _, rpcErr := requireWorkspacePrincipal(ctx, req.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	if req.Limit <= 0 || req.Limit > 5000 {
		req.Limit = 1000
	}

	// Delegate to the projection layer
	snapshot, err := h.store.GetGraphSnapshot(ctx, req)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	return snapshot, nil
}
