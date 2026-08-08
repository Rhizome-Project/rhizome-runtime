package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type toolRemoveParams struct {
	WorkspaceID string `json:"workspace_id"`
	ToolID      string `json:"tool_id"`
	RemovedBy   string `json:"removed_by"`
}

func (h *Handler) toolRemove(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p toolRemoveParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}

	deployed, err := h.toolExec.IsDeployed(p.WorkspaceID, p.ToolID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if deployed {
		return nil, &RPCError{
			Code:    errCodeInvalidParams,
			Message: fmt.Sprintf("tool %s is still deployed; call tool.undeploy before removing the registry entry", strings.TrimSpace(p.ToolID)),
		}
	}

	existed, err := h.store.RemoveWorkspaceTool(ctx, sqlite.WorkspaceToolRemoveInput{
		WorkspaceID: p.WorkspaceID,
		ToolID:      p.ToolID,
		RemovedBy:   p.RemovedBy,
		PromptContextEnvelope: sqlite.BuildToolRegistryPromptContextEnvelope(
			"tool.remove",
			"server_rpc",
			p.WorkspaceID,
			principal.PrincipalType,
			principal.PrincipalID,
		),
		PromptContextSurface:       "tool.remove",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "tool.remove"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	return map[string]any{
		"tool_id":    strings.TrimSpace(p.ToolID),
		"removed_by": strings.TrimSpace(p.RemovedBy),
		"existed":    existed,
		"status":     "REMOVED",
	}, nil
}
