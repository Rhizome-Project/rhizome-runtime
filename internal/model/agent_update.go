package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type AgentUpdateArtifactRef struct {
	Ref         string `json:"ref"`
	Kind        string `json:"kind,omitempty"`
	ContentType string `json:"content_type,omitempty"`
}

type AgentUpdateBlockedRef struct {
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

type AgentUpdateSideEffectV1 struct {
	Schema               string   `json:"schema"`
	SideEffectRef        string   `json:"side_effect_ref"`
	Actor                string   `json:"actor"`
	LaneRef              string   `json:"lane_ref"`
	TensionRef           string   `json:"tension_ref"`
	ArtifactRef          string   `json:"artifact_ref"`
	RegionRef            string   `json:"region_ref"`
	Operation            string   `json:"operation"`
	SourceKind           string   `json:"source_kind"`
	BoundaryRef          string   `json:"boundary_ref"`
	BoundaryRelation     string   `json:"boundary_relation"`
	MaterializationState string   `json:"materialization_state"`
	IntegrationIntent    string   `json:"integration_intent"`
	IntegrationStatus    string   `json:"integration_status"`
	Decision             string   `json:"decision"`
	Justification        string   `json:"justification"`
	DerivedRegionRefs    []string `json:"derived_region_refs"`
}

type AgentUpdatePayloadV1 struct {
	Status      string                    `json:"status,omitempty"`
	NextAction  string                    `json:"next_action,omitempty"`
	TaskIDs     []string                  `json:"task_ids,omitempty"`
	DocKeys     []string                  `json:"doc_keys,omitempty"`
	Artifacts   []AgentUpdateArtifactRef  `json:"artifacts,omitempty"`
	Links       []string                  `json:"links,omitempty"`
	BlockedOn   []AgentUpdateBlockedRef   `json:"blocked_on,omitempty"`
	SideEffects []AgentUpdateSideEffectV1 `json:"side_effects,omitempty"`
	OwnerUserID string                    `json:"owner_user_id,omitempty"`
	HumanReason string                    `json:"human_reason,omitempty"`
	OwnerAction string                    `json:"owner_action,omitempty"`
	Notes       string                    `json:"notes,omitempty"`
}

func ParseAgentUpdateSideEffectsV1(raw string) ([]AgentUpdateSideEffectV1, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}

	var probe struct {
		SideEffects json.RawMessage `json:"side_effects"`
	}
	if err := json.Unmarshal([]byte(trimmed), &probe); err != nil {
		return nil, nil
	}
	if len(probe.SideEffects) == 0 || string(probe.SideEffects) == "null" {
		return nil, nil
	}

	var payload AgentUpdatePayloadV1
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return nil, fmt.Errorf("decode side effects payload schema v1: %w", err)
	}
	payload.Normalize()
	if err := validateAgentUpdateSideEffects(payload.SideEffects); err != nil {
		return nil, err
	}
	return append([]AgentUpdateSideEffectV1(nil), payload.SideEffects...), nil
}

func ParseAndNormalizeAgentUpdatePayloadV1(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}

	var payload AgentUpdatePayloadV1
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return "", fmt.Errorf("decode payload schema v1: %w", err)
	}
	payload.Normalize()
	if err := payload.Validate(); err != nil {
		return "", err
	}

	normalized, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode payload schema v1: %w", err)
	}
	return string(normalized), nil
}

func (p *AgentUpdatePayloadV1) Normalize() {
	p.Status = strings.TrimSpace(p.Status)
	p.NextAction = strings.TrimSpace(p.NextAction)
	p.TaskIDs = normalizeStringList(p.TaskIDs)
	p.DocKeys = normalizeStringList(p.DocKeys)
	p.Links = normalizeStringList(p.Links)
	p.OwnerUserID = strings.TrimSpace(p.OwnerUserID)
	p.HumanReason = strings.TrimSpace(p.HumanReason)
	p.OwnerAction = strings.TrimSpace(p.OwnerAction)
	p.Notes = strings.TrimSpace(p.Notes)

	artifacts := make([]AgentUpdateArtifactRef, 0, len(p.Artifacts))
	for _, artifact := range p.Artifacts {
		artifact.Ref = strings.TrimSpace(artifact.Ref)
		artifact.Kind = strings.TrimSpace(artifact.Kind)
		artifact.ContentType = strings.TrimSpace(artifact.ContentType)
		if artifact.Ref == "" {
			continue
		}
		artifacts = append(artifacts, artifact)
	}
	p.Artifacts = artifacts

	blocked := make([]AgentUpdateBlockedRef, 0, len(p.BlockedOn))
	for _, item := range p.BlockedOn {
		item.Kind = strings.TrimSpace(item.Kind)
		item.Detail = strings.TrimSpace(item.Detail)
		if item.Kind == "" && item.Detail == "" {
			continue
		}
		blocked = append(blocked, item)
	}
	p.BlockedOn = blocked

	sideEffects := make([]AgentUpdateSideEffectV1, 0, len(p.SideEffects))
	for _, effect := range p.SideEffects {
		effect.Schema = strings.TrimSpace(effect.Schema)
		effect.SideEffectRef = strings.TrimSpace(effect.SideEffectRef)
		effect.Actor = strings.TrimSpace(effect.Actor)
		effect.LaneRef = strings.TrimSpace(effect.LaneRef)
		effect.TensionRef = strings.TrimSpace(effect.TensionRef)
		effect.ArtifactRef = strings.TrimSpace(effect.ArtifactRef)
		effect.RegionRef = strings.TrimSpace(effect.RegionRef)
		effect.Operation = strings.TrimSpace(effect.Operation)
		effect.SourceKind = strings.TrimSpace(effect.SourceKind)
		effect.BoundaryRef = strings.TrimSpace(effect.BoundaryRef)
		effect.BoundaryRelation = strings.TrimSpace(effect.BoundaryRelation)
		effect.MaterializationState = strings.TrimSpace(effect.MaterializationState)
		effect.IntegrationIntent = strings.TrimSpace(effect.IntegrationIntent)
		effect.IntegrationStatus = strings.TrimSpace(effect.IntegrationStatus)
		effect.Decision = strings.TrimSpace(effect.Decision)
		effect.Justification = strings.TrimSpace(effect.Justification)
		effect.DerivedRegionRefs = normalizeStringList(effect.DerivedRegionRefs)
		sideEffects = append(sideEffects, effect)
	}
	p.SideEffects = sideEffects
}

func (p AgentUpdatePayloadV1) Validate() error {
	if len(p.BlockedOn) == 0 && len(p.Artifacts) == 0 && len(p.SideEffects) == 0 && p.Status == "" && p.NextAction == "" && len(p.TaskIDs) == 0 && len(p.DocKeys) == 0 && len(p.Links) == 0 && p.OwnerUserID == "" && p.HumanReason == "" && p.OwnerAction == "" && p.Notes == "" {
		return errors.New("payload schema v1 requires at least one populated field")
	}
	for _, item := range p.BlockedOn {
		if item.Kind == "" {
			return errors.New("payload schema v1 blocked_on.kind is required when blocked_on is present")
		}
		if item.Detail == "" {
			return errors.New("payload schema v1 blocked_on.detail is required when blocked_on is present")
		}
	}
	if p.HumanReason != "" && p.OwnerAction == "" {
		return errors.New("payload schema v1 owner_action is required when human_reason is set")
	}
	if err := validateAgentUpdateSideEffects(p.SideEffects); err != nil {
		return err
	}
	return nil
}

func validateAgentUpdateSideEffects(items []AgentUpdateSideEffectV1) error {
	for _, item := range items {
		if item.Schema != "artifact_bound_side_effect.v1" {
			return errors.New("payload schema v1 side_effects.schema must be artifact_bound_side_effect.v1")
		}
		if item.SideEffectRef == "" {
			return errors.New("payload schema v1 side_effects.side_effect_ref is required")
		}
		if item.Actor == "" {
			return errors.New("payload schema v1 side_effects.actor is required")
		}
		if item.LaneRef == "" {
			return errors.New("payload schema v1 side_effects.lane_ref is required")
		}
		if item.TensionRef == "" {
			return errors.New("payload schema v1 side_effects.tension_ref is required")
		}
		if item.ArtifactRef == "" {
			return errors.New("payload schema v1 side_effects.artifact_ref is required")
		}
		if item.RegionRef == "" {
			return errors.New("payload schema v1 side_effects.region_ref is required")
		}
		if !agentUpdateAllowedValue(item.Operation, agentUpdateOperationValues) {
			return errors.New("payload schema v1 side_effects.operation is invalid")
		}
		if !validAgentUpdateSourceKind(item.SourceKind) {
			return errors.New("payload schema v1 side_effects.source_kind must start with adapter: or core:")
		}
		if item.BoundaryRef == "" {
			return errors.New("payload schema v1 side_effects.boundary_ref is required")
		}
		if !agentUpdateAllowedValue(item.BoundaryRelation, agentUpdateBoundaryRelationValues) {
			return errors.New("payload schema v1 side_effects.boundary_relation is invalid")
		}
		if !agentUpdateAllowedValue(item.MaterializationState, agentUpdateMaterializationStateValues) {
			return errors.New("payload schema v1 side_effects.materialization_state is invalid")
		}
		if !agentUpdateAllowedValue(item.IntegrationIntent, agentUpdateIntegrationIntentValues) {
			return errors.New("payload schema v1 side_effects.integration_intent is invalid")
		}
		if !agentUpdateAllowedValue(item.IntegrationStatus, agentUpdateIntegrationStatusValues) {
			return errors.New("payload schema v1 side_effects.integration_status is invalid")
		}
		if !agentUpdateAllowedValue(item.Decision, agentUpdateDecisionValues) {
			return errors.New("payload schema v1 side_effects.decision is invalid")
		}
		if item.Justification == "" {
			return errors.New("payload schema v1 side_effects.justification is required")
		}
		if len(item.DerivedRegionRefs) == 0 {
			return errors.New("payload schema v1 side_effects.derived_region_refs requires at least one ref")
		}
	}
	return nil
}

func agentUpdateAllowedValue(value string, allowed map[string]struct{}) bool {
	_, ok := allowed[value]
	return ok
}

func validAgentUpdateSourceKind(value string) bool {
	var suffix string
	switch {
	case strings.HasPrefix(value, "adapter:"):
		suffix = strings.TrimPrefix(value, "adapter:")
	case strings.HasPrefix(value, "core:"):
		suffix = strings.TrimPrefix(value, "core:")
	default:
		return false
	}
	return suffix != "" && !strings.ContainsAny(suffix, " \t\r\n")
}

var agentUpdateOperationValues = map[string]struct{}{
	"create":   {},
	"modify":   {},
	"delete":   {},
	"move":     {},
	"change":   {},
	"generate": {},
	"publish":  {},
	"classify": {},
	"unknown":  {},
}

var agentUpdateBoundaryRelationValues = map[string]struct{}{
	"in_boundary":                   {},
	"generated_cache":               {},
	"adjacent_foundation_candidate": {},
	"out_of_boundary":               {},
	"ambiguous":                     {},
}

var agentUpdateMaterializationStateValues = map[string]struct{}{
	"attempted":            {},
	"present_unintegrated": {},
	"staged":               {},
	"committed":            {},
	"integrated":           {},
}

var agentUpdateIntegrationIntentValues = map[string]struct{}{
	"local_completion":       {},
	"dependency_preparation": {},
	"exploratory_probe":      {},
	"foundation_effect":      {},
	"repair":                 {},
	"refactor_restructure":   {},
	"unknown":                {},
}

var agentUpdateIntegrationStatusValues = map[string]struct{}{
	"pending_classification":       {},
	"accepted":                     {},
	"quarantined":                  {},
	"rejected":                     {},
	"split_requested":              {},
	"boundary_expansion_requested": {},
	"verification_requested":       {},
}

var agentUpdateDecisionValues = map[string]struct{}{
	"none_pending":           {},
	"accept":                 {},
	"quarantine":             {},
	"revert":                 {},
	"split_tension":          {},
	"expand_boundary":        {},
	"reassign":               {},
	"request_verification":   {},
	"reroute_to_active_lane": {},
}

func normalizeStringList(items []string) []string {
	out := make([]string, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}
