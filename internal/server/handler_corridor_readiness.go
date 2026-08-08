package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceInstrumentationCorridorParams struct {
	WorkspaceID    string `json:"workspace_id"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	ActorID        string `json:"actor_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

func (h *Handler) workspaceInstrumentationCorridorReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationCorridorParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildCorridorReadinessReport(ctx, sqlite.CorridorReadinessFilter{
		WorkspaceID:    p.WorkspaceID,
		ProtoClusterID: p.ProtoClusterID,
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

func (h *Handler) workspaceInstrumentationCorridorCluster(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationCorridorParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ProtoClusterID, "proto_cluster_id"); rpcErr != nil {
		return nil, rpcErr
	}
	detail, err := h.store.BuildCorridorClusterDetail(ctx, p.WorkspaceID, p.ProtoClusterID)
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

func (h *Handler) workspaceInstrumentationCorridorSnapshot(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationCorridorParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildCorridorReadinessReport(ctx, sqlite.CorridorReadinessFilter{
		WorkspaceID:    p.WorkspaceID,
		ProtoClusterID: p.ProtoClusterID,
		Limit:          p.Limit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	event, err := h.store.RecordCorridorReadinessSnapshot(ctx, report, sqlite.CorridorSnapshotInput{
		ActorID: p.ActorID,
		Limit:   p.Limit,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.instrumentation.corridor.snapshot"); rpcErr != nil {
			return nil, rpcErr
		}
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	h.publishCorridorReadinessRuntimeEvent(event)
	return map[string]any{
		"workspace_id": strings.TrimSpace(report.WorkspaceID),
		"report":       report,
		"event":        event,
	}, nil
}

func (h *Handler) publishCorridorReadinessRuntimeEvent(event sqlite.RuntimeEventRecord) {
	h.publishRuntimeEventRecord(event)
}
