package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceRSPCapabilityGetParams struct {
	WorkspaceID string `json:"workspace_id"`
}

type workspaceRSPCapabilityPutParams struct {
	WorkspaceID             string `json:"workspace_id"`
	BeliefLive              *bool  `json:"belief_live,omitempty"`
	AnomalyShadow           *bool  `json:"anomaly_shadow,omitempty"`
	StateShadow             *bool  `json:"state_shadow,omitempty"`
	ForecastShadow          *bool  `json:"forecast_shadow,omitempty"`
	SafeLocalAutonomicsLive *bool  `json:"safe_local_autonomics_live,omitempty"`
	GovernedHintsLive       *bool  `json:"governed_hints_live,omitempty"`
	StrongConsequencesLive  *bool  `json:"strong_consequences_live,omitempty"`
	UpdatedBy               string `json:"updated_by"`
	Reason                  string `json:"reason,omitempty"`
}

func (h *Handler) workspaceRSPCapabilityGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceRSPCapabilityGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	return h.store.GetRSPCapabilityFlags(ctx, p.WorkspaceID), nil
}

func (h *Handler) workspaceRSPCapabilityPut(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceRSPCapabilityPutParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.UpdatedBy, "updated_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, p.WorkspaceID, p.UpdatedBy, "updated_by"); rpcErr != nil {
		return nil, rpcErr
	}
	flags, err := h.store.SetRSPCapabilityFlags(ctx, sqlite.SetRSPCapabilityFlagsInput{
		WorkspaceID:             p.WorkspaceID,
		BeliefLive:              p.BeliefLive,
		AnomalyShadow:           p.AnomalyShadow,
		StateShadow:             p.StateShadow,
		ForecastShadow:          p.ForecastShadow,
		SafeLocalAutonomicsLive: p.SafeLocalAutonomicsLive,
		GovernedHintsLive:       p.GovernedHintsLive,
		StrongConsequencesLive:  p.StrongConsequencesLive,
		UpdatedBy:               p.UpdatedBy,
		Reason:                  p.Reason,
		PromptContextEnvelope:   h.capabilityPolicyPromptContextEnvelope(ctx, p.WorkspaceID, "workspace.rsp.capability.put"),
		PromptContextSurface:    "workspace.rsp.capability.put",
	})
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "workspace.rsp.capability.put"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, rspCapabilityRPCError(err)
	}
	return flags, nil
}

func rspCapabilityRPCError(err error) *RPCError {
	if err == nil {
		return nil
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
