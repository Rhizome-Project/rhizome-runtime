package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceInstrumentationControlParams struct {
	WorkspaceID    string `json:"workspace_id"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	ActorID        string `json:"actor_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

func (h *Handler) workspaceInstrumentationControlReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationControlParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildControlReport(ctx, sqlite.ControlReportFilter{
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

func (h *Handler) workspaceInstrumentationControlCluster(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationControlParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.ProtoClusterID, "proto_cluster_id"); rpcErr != nil {
		return nil, rpcErr
	}
	detail, err := h.store.BuildControlClusterDetail(ctx, p.WorkspaceID, p.ProtoClusterID)
	if err != nil {
		if isControlPlaneValidationError(err) || strings.Contains(strings.ToLower(err.Error()), "not found") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	forecasts, _ := h.store.ListRSPForecastsByCluster(ctx, p.WorkspaceID, p.ProtoClusterID)

	return map[string]any{
		"workspace_id":     strings.TrimSpace(p.WorkspaceID),
		"proto_cluster_id": strings.TrimSpace(p.ProtoClusterID),
		"detail":           detail,
		"forecasts":        forecasts,
	}, nil
}

func (h *Handler) workspaceInstrumentationControlSnapshot(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationControlParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildControlReport(ctx, sqlite.ControlReportFilter{
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
	event, err := h.store.RecordControlSignalSnapshot(ctx, report, sqlite.ControlSnapshotInput{
		ActorID: p.ActorID,
		Limit:   p.Limit,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.instrumentation.control.snapshot"); rpcErr != nil {
			return nil, rpcErr
		}
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	h.publishControlAdvisoryRuntimeEvent(event)
	return map[string]any{
		"workspace_id": strings.TrimSpace(report.WorkspaceID),
		"report":       report,
		"event":        event,
	}, nil
}

func (h *Handler) publishControlAdvisoryRuntimeEvent(event sqlite.RuntimeEventRecord) {
	h.publishRuntimeEventRecord(event)
}
