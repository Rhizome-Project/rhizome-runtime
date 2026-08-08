package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceInstrumentationParams struct {
	WorkspaceID  string `json:"workspace_id"`
	AgentID      string `json:"agent_id,omitempty"`
	SessionID    string `json:"session_id,omitempty"`
	TaskID       string `json:"task_id,omitempty"`
	ActorID      string `json:"actor_id,omitempty"`
	Limit        int    `json:"limit,omitempty"`
	ClusterLimit int    `json:"cluster_limit,omitempty"`
}

func (h *Handler) workspaceInstrumentationReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  p.WorkspaceID,
		AgentID:      p.AgentID,
		SessionID:    p.SessionID,
		TaskID:       p.TaskID,
		Limit:        p.Limit,
		ClusterLimit: p.ClusterLimit,
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

func (h *Handler) workspaceInstrumentationClusters(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  p.WorkspaceID,
		AgentID:      p.AgentID,
		SessionID:    p.SessionID,
		TaskID:       p.TaskID,
		Limit:        p.Limit,
		ClusterLimit: p.ClusterLimit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id":   strings.TrimSpace(report.WorkspaceID),
		"time_authority": report.TimeAuthority,
		"clusters":       report.Clusters,
		"count":          len(report.Clusters),
	}, nil
}

func (h *Handler) workspaceInstrumentationSnapshot(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  p.WorkspaceID,
		AgentID:      p.AgentID,
		SessionID:    p.SessionID,
		TaskID:       p.TaskID,
		Limit:        p.Limit,
		ClusterLimit: p.ClusterLimit,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	event, err := h.store.RecordInstrumentationMetricSnapshot(ctx, report, sqlite.InstrumentationSnapshotInput{
		ActorID: p.ActorID,
		Limit:   p.ClusterLimit,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.instrumentation.snapshot"); rpcErr != nil {
			return nil, rpcErr
		}
		if isControlPlaneValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	h.publishRuntimeEventRecord(event, fmt.Sprintf("Instrumentation snapshot: %d clusters, %d blocked", report.Workspace.TotalClusters, report.Workspace.BlockedClusterCount))
	return map[string]any{
		"workspace_id": strings.TrimSpace(report.WorkspaceID),
		"report":       report,
		"event":        event,
	}, nil
}
