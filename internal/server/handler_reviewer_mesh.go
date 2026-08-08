package server

import (
	"context"
	"encoding/json"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type reviewerRouteParams struct {
	WorkspaceID           string   `json:"workspace_id"`
	BundleID              string   `json:"bundle_id,omitempty"`
	GeneratorAgentID      string   `json:"generator_agent_id"`
	AvailableReviewers    []string `json:"available_reviewers"`
	IsMultiPatch          *bool    `json:"is_multi_patch"`
	ImpactScore           *float64 `json:"impact_score"`
	ContradictionPressure *float64 `json:"contradiction_pressure"`
	HasActiveDissent      *bool    `json:"has_active_dissent"`
	TouchesHardConstraint *bool    `json:"touches_hard_constraint"`
	ClusterMode           string   `json:"cluster_mode"`
	MergeRisk             *float64 `json:"merge_risk"`
}

func (h *Handler) reviewerRoute(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p reviewerRouteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid params", Details: err.Error()}
	}
	if p.WorkspaceID == "" || p.GeneratorAgentID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id and generator_agent_id are required"}
	}
	if len(p.AvailableReviewers) == 0 {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "available_reviewers is required"}
	}
	if p.IsMultiPatch == nil || p.ImpactScore == nil || p.ContradictionPressure == nil || p.HasActiveDissent == nil || p.TouchesHardConstraint == nil || p.MergeRisk == nil || p.ClusterMode == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "reviewer.route requires explicit routing evidence: is_multi_patch, impact_score, contradiction_pressure, has_active_dissent, touches_hard_constraint, cluster_mode, merge_risk"}
	}
	if *p.ImpactScore < 0 || *p.ImpactScore > 1 || *p.ContradictionPressure < 0 || *p.ContradictionPressure > 1 || *p.MergeRisk < 0 || *p.MergeRisk > 1 {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "impact_score, contradiction_pressure, and merge_risk must be within 0..1"}
	}

	res, err := h.store.RouteVerification(ctx, sqlite.VerifierMeshRouteInput{
		WorkspaceID:           p.WorkspaceID,
		BundleID:              p.BundleID,
		GeneratorAgentID:      p.GeneratorAgentID,
		AvailableReviewers:    p.AvailableReviewers,
		IsMultiPatch:          *p.IsMultiPatch,
		HasActiveDissent:      *p.HasActiveDissent,
		TouchesHardConstraint: *p.TouchesHardConstraint,
		ImpactScore:           *p.ImpactScore,
		ContradictionPressure: *p.ContradictionPressure,
		MergeRisk:             *p.MergeRisk,
		ClusterMode:           p.ClusterMode,
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: "failed to route verification", Details: err.Error()}
	}

	return res, nil
}

type reviewerScarcityParams struct {
	WorkspaceID string `json:"workspace_id"`
}

func (h *Handler) reviewerScarcity(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p reviewerScarcityParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid params", Details: err.Error()}
	}
	if p.WorkspaceID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id is required"}
	}

	snapshot, err := h.store.ReviewerMeshScarcitySnapshot(ctx, p.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: "failed to read reviewer scarcity", Details: err.Error()}
	}
	return snapshot, nil
}
