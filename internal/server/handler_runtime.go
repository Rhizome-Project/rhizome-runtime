package server

import (
	"context"
	"encoding/json"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
)

type runtimeBuildInfoParams struct {
	WorkspaceID string `json:"workspace_id"`
}

// runtimeBuildInfo returns the non-secret build/runtime identity of the running
// server binary so a managed-runtime preflight can prove the remote build matches
// the local substrate before admitting a run. It requires an authenticated
// workspace principal; the payload contains no secrets.
func (h *Handler) runtimeBuildInfo(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p runtimeBuildInfoParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	return app.CurrentRuntimeBuildInfo(), nil
}
