package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceInstrumentationUnifiedControlParams struct {
	WorkspaceID    string   `json:"workspace_id"`
	ProtoClusterID string   `json:"proto_cluster_id,omitempty"`
	AgentID        string   `json:"agent_id,omitempty"`
	TaskID         string   `json:"task_id,omitempty"`
	SessionID      string   `json:"session_id,omitempty"`
	DocKeys        []string `json:"doc_keys,omitempty"`
	ArtifactRefs   []string `json:"artifact_refs,omitempty"`
	FrontierLimit  int      `json:"frontier_limit,omitempty"`
}

type workspaceInstrumentationUnifiedControlSnapshotParams struct {
	WorkspaceID    string   `json:"workspace_id"`
	ProtoClusterID string   `json:"proto_cluster_id,omitempty"`
	AgentID        string   `json:"agent_id,omitempty"`
	TaskID         string   `json:"task_id,omitempty"`
	SessionID      string   `json:"session_id,omitempty"`
	DocKeys        []string `json:"doc_keys,omitempty"`
	ArtifactRefs   []string `json:"artifact_refs,omitempty"`
	FrontierLimit  int      `json:"frontier_limit,omitempty"`
	ActorID        string   `json:"actor_id,omitempty"`
}

func (h *Handler) workspaceInstrumentationUnifiedControlReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationUnifiedControlParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	report, err := h.store.BuildUnifiedControlReport(ctx, sqlite.UnifiedControlReportFilter{
		WorkspaceID:    p.WorkspaceID,
		ProtoClusterID: p.ProtoClusterID,
		AgentID:        p.AgentID,
		TaskID:         p.TaskID,
		SessionID:      p.SessionID,
		DocKeys:        append([]string(nil), p.DocKeys...),
		ArtifactRefs:   append([]string(nil), p.ArtifactRefs...),
		FrontierLimit:  p.FrontierLimit,
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return boundedUnifiedControlEnvelope(p.WorkspaceID, report), nil
}

func (h *Handler) workspaceInstrumentationUnifiedControlSnapshot(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceInstrumentationUnifiedControlSnapshotParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	filter := sqlite.UnifiedControlReportFilter{
		WorkspaceID:    p.WorkspaceID,
		ProtoClusterID: p.ProtoClusterID,
		AgentID:        p.AgentID,
		TaskID:         p.TaskID,
		SessionID:      p.SessionID,
		DocKeys:        append([]string(nil), p.DocKeys...),
		ArtifactRefs:   append([]string(nil), p.ArtifactRefs...),
		FrontierLimit:  p.FrontierLimit,
	}
	report, err := h.store.BuildUnifiedControlReport(ctx, filter)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	event, err := h.store.RecordUnifiedControlSnapshot(ctx, report, filter, sqlite.UnifiedControlSnapshotInput{
		ActorID: p.ActorID,
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.instrumentation.unified_control.snapshot"); rpcErr != nil {
			return nil, rpcErr
		}
		if isControlPlaneValidationError(err) || strings.Contains(strings.ToLower(err.Error()), "not found") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	payload := boundedUnifiedControlEnvelope(p.WorkspaceID, report)
	payload["event"] = event
	h.publishRuntimeEventRecord(event)
	return payload, nil
}

func boundedUnifiedControlEnvelope(workspaceID string, report sqlite.UnifiedControlReport) map[string]any {
	return map[string]any{
		"workspace_id":                    workspaceID,
		"report":                          report,
		"time_authority":                  report.TimeAuthority,
		"summary":                         report.Summary,
		"advisory_only":                   report.AdvisoryOnly,
		"capability_flags":                report.CapabilityFlags,
		"advisory_controls":               report.AdvisoryControls,
		"candidate_controls":              report.CandidateControls,
		"effective_controls":              report.EffectiveControls,
		"effective_controls_audit":        report.EffectiveControlsAudit,
		"effective_control_basis":         report.EffectiveControlBasis,
		"effective_control_basis_summary": report.EffectiveControlBasisSummary,
		"contradiction_summary":           report.ContradictionSummary,
		"governed_hint_summary":           report.GovernedHintSummary,
		"governed_hint_outcomes":          report.GovernedHintOutcomes,
		"audit_summary":                   report.AuditSummary,
		"audit_coverage":                  report.AuditCoverage,
	}
}
