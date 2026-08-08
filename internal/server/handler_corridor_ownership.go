package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func (h *Handler) workspaceInstrumentationCorridorOwnershipReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationCorridorParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildCorridorOwnershipReport(ctx, sqlite.CorridorOwnershipFilter{
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

func (h *Handler) workspaceInstrumentationCorridorOwnershipCluster(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
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
	detail, err := h.store.BuildCorridorOwnershipClusterDetail(ctx, p.WorkspaceID, p.ProtoClusterID)
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

func (h *Handler) workspaceInstrumentationCorridorOwnershipSnapshot(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationCorridorParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildCorridorOwnershipReport(ctx, sqlite.CorridorOwnershipFilter{
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
	event, err := h.store.RecordCorridorOwnershipSnapshot(ctx, report, sqlite.CorridorOwnershipSnapshotInput{
		ActorID: p.ActorID,
		Limit:   p.Limit,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.instrumentation.corridor.ownership.snapshot"); rpcErr != nil {
			return nil, rpcErr
		}
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	h.publishCorridorOwnershipRuntimeEvent(event)
	return map[string]any{
		"workspace_id": strings.TrimSpace(report.WorkspaceID),
		"report":       report,
		"event":        event,
	}, nil
}

func (h *Handler) publishCorridorOwnershipRuntimeEvent(event sqlite.RuntimeEventRecord) {
	h.publishRuntimeEventRecord(event)
}
