package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceRSPForecastReportParams struct {
	WorkspaceID    string   `json:"workspace_id"`
	ProtoClusterID string   `json:"proto_cluster_id,omitempty"`
	AgentID        string   `json:"agent_id,omitempty"`
	SessionID      string   `json:"session_id,omitempty"`
	TaskID         string   `json:"task_id,omitempty"`
	DocKeys        []string `json:"doc_keys,omitempty"`
	ArtifactRefs   []string `json:"artifact_refs,omitempty"`
	FrontierLimit  int      `json:"frontier_limit,omitempty"`
}

func (h *Handler) workspaceRSPForecastReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceRSPForecastReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildRSPForecastReport(ctx, sqlite.RSPForecastReportFilter{
		WorkspaceID:    p.WorkspaceID,
		ProtoClusterID: p.ProtoClusterID,
		AgentID:        p.AgentID,
		SessionID:      p.SessionID,
		TaskID:         p.TaskID,
		DocKeys:        p.DocKeys,
		ArtifactRefs:   p.ArtifactRefs,
		FrontierLimit:  p.FrontierLimit,
	})
	if err != nil {
		return nil, rspForecastRPCError(err)
	}
	return report, nil
}

func (h *Handler) workspaceRSPForecastSnapshot(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceRSPForecastReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.SnapshotRSPForecastReport(ctx, sqlite.RSPForecastReportFilter{
		WorkspaceID:    p.WorkspaceID,
		ProtoClusterID: p.ProtoClusterID,
		AgentID:        p.AgentID,
		SessionID:      p.SessionID,
		TaskID:         p.TaskID,
		DocKeys:        p.DocKeys,
		ArtifactRefs:   p.ArtifactRefs,
		FrontierLimit:  p.FrontierLimit,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.rsp.forecast.snapshot"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, rspForecastRPCError(err)
	}
	h.publishRuntimeEventRecord(result.Event, "rsp forecast snapshot")
	return map[string]any{
		"status": "RECORDED",
		"report": result.Report,
		"event":  result.Event,
	}, nil
}

func rspForecastRPCError(err error) *RPCError {
	if err == nil {
		return nil
	}
	if sqliteErr := sqliteErrorPermission(err); sqliteErr != nil {
		return sqliteErr
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "required"),
		strings.Contains(message, "not found"),
		strings.Contains(message, "invalid"):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	default:
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
}
