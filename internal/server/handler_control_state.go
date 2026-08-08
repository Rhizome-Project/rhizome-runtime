package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

const (
	controlStateTickSurface     = "workspace.instrumentation.control.state.tick"
	controlStateSnapshotSurface = "workspace.instrumentation.control.state.snapshot"
)

type workspaceInstrumentationControlStateParams struct {
	WorkspaceID    string `json:"workspace_id"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	ActorID        string `json:"actor_id,omitempty"`
	Mode           string `json:"mode,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

func (h *Handler) workspaceInstrumentationControlStateReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationControlStateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildClusterControlStateReport(ctx, sqlite.ClusterControlStateFilter{
		WorkspaceID:    p.WorkspaceID,
		ProtoClusterID: p.ProtoClusterID,
		Mode:           p.Mode,
		Limit:          p.Limit,
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

func (h *Handler) workspaceInstrumentationControlStateCluster(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationControlStateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ProtoClusterID, "proto_cluster_id"); rpcErr != nil {
		return nil, rpcErr
	}
	detail, err := h.store.BuildClusterControlStateDetail(ctx, p.WorkspaceID, p.ProtoClusterID)
	if err != nil {
		if isControlPlaneValidationError(err) || strings.Contains(strings.ToLower(err.Error()), "not found") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id":     strings.TrimSpace(p.WorkspaceID),
		"proto_cluster_id": strings.TrimSpace(p.ProtoClusterID),
		"detail":           detail,
	}, nil
}

func (h *Handler) workspaceInstrumentationControlStateTick(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationControlStateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ActorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principalType, principalID := h.promptContextPrincipal(ctx)
	result, err := h.store.TickClusterControlState(ctx, sqlite.ClusterControlTickInput{
		WorkspaceID:                p.WorkspaceID,
		ProtoClusterID:             p.ProtoClusterID,
		ActorID:                    p.ActorID,
		PromptContextEnvelope:      h.controlStatePromptContextEnvelope(ctx, p.WorkspaceID, controlStateTickSurface, p.ActorID),
		PromptContextSurface:       controlStateTickSurface,
		PromptContextPrincipalType: principalType,
		PromptContextPrincipalID:   principalID,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, controlStateTickSurface); rpcErr != nil {
			return nil, rpcErr
		}
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	h.publishControlStateRuntimeEvents(result.Events)
	return map[string]any{
		"workspace_id": strings.TrimSpace(result.WorkspaceID),
		"result":       result,
	}, nil
}

func (h *Handler) workspaceInstrumentationControlStateSnapshot(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationControlStateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	actorID := controlStateSnapshotActorID(ctx, p.ActorID)
	principalType, principalID := h.promptContextPrincipal(ctx)
	report, err := h.store.BuildClusterControlStateReport(ctx, sqlite.ClusterControlStateFilter{
		WorkspaceID:    p.WorkspaceID,
		ProtoClusterID: p.ProtoClusterID,
		Mode:           p.Mode,
		Limit:          p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	event, err := h.store.RecordClusterControlStateSnapshot(ctx, report, sqlite.ClusterControlSnapshotInput{
		ActorID:                    actorID,
		Limit:                      p.Limit,
		PromptContextEnvelope:      h.controlStatePromptContextEnvelope(ctx, p.WorkspaceID, controlStateSnapshotSurface, actorID),
		PromptContextSurface:       controlStateSnapshotSurface,
		PromptContextPrincipalType: principalType,
		PromptContextPrincipalID:   principalID,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, controlStateSnapshotSurface); rpcErr != nil {
			return nil, rpcErr
		}
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	h.publishControlStateRuntimeEvent(event)
	return map[string]any{
		"workspace_id": strings.TrimSpace(report.WorkspaceID),
		"report":       report,
		"event":        event,
	}, nil
}

func (h *Handler) publishControlStateRuntimeEvents(events []sqlite.RuntimeEventRecord) {
	h.publishRuntimeEventBatchChronological(events, h.publishControlStateRuntimeEvent)
}

func (h *Handler) publishControlStateRuntimeEvent(event sqlite.RuntimeEventRecord) {
	h.publishRuntimeEventRecord(event)
}

func (h *Handler) controlStatePromptContextEnvelope(ctx context.Context, workspaceID, surface, actorID string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildControlStatePromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
	if actorID = strings.TrimSpace(actorID); actorID != "" {
		envelope["actor_id"] = actorID
	}
	return envelope
}

func controlStateSnapshotActorID(ctx context.Context, actorID string) string {
	if trimmed := strings.TrimSpace(actorID); trimmed != "" {
		return trimmed
	}
	if principal, ok := authPrincipalFromContext(ctx); ok {
		if trimmed := strings.TrimSpace(principal.PrincipalID); trimmed != "" {
			return trimmed
		}
	}
	return "control.state.snapshot"
}
