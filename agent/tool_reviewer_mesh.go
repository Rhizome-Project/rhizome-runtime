package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	reviewerRouteNearStatusScarcitySaturated = "OMITTED_REVIEWER_SCARCITY_SATURATED"
	reviewerRouteFarStatusScarcitySaturated  = "OMITTED_REVIEWER_SCARCITY_SATURATED"
	reviewerRouteScarcitySaturatedReason     = "reviewer_scarcity_saturated"
)

type ReviewerRouteTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewReviewerRouteTool(client *RhizomeClient, workspaceID, agentID string) *ReviewerRouteTool {
	return &ReviewerRouteTool{
		client:      client,
		workspaceID: workspaceID,
		agentID:     agentID,
	}
}

func (t *ReviewerRouteTool) Name() string { return "reviewer_route" }
func (t *ReviewerRouteTool) Description() string {
	return "Route verifier tier plus near/far reviewers from explicit bundle evidence. Far reviewer is omitted when no evidence-backed distance candidate exists."
}
func (t *ReviewerRouteTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"bundle_id":               map[string]any{"type": "string", "description": "Optional bundle identifier for audit context"},
			"available_reviewers":     map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Candidate reviewer agent IDs"},
			"is_multi_patch":          map[string]any{"type": "boolean", "description": "Whether the bundle spans multiple patches"},
			"impact_score":            map[string]any{"type": "number", "description": "0..1 explicit route evidence"},
			"contradiction_pressure":  map[string]any{"type": "number", "description": "0..1 explicit route evidence"},
			"has_active_dissent":      map[string]any{"type": "boolean", "description": "Whether active dissent already exists"},
			"touches_hard_constraint": map[string]any{"type": "boolean", "description": "Whether the bundle mutates a hard constraint"},
			"cluster_mode":            map[string]any{"type": "string", "description": "Explicit cluster mode, e.g. explore or stabilize"},
			"merge_risk":              map[string]any{"type": "number", "description": "0..1 explicit route evidence"},
		},
		"required": []string{"available_reviewers", "is_multi_patch", "impact_score", "contradiction_pressure", "has_active_dissent", "touches_hard_constraint", "cluster_mode", "merge_risk"},
	}
}
func (t *ReviewerRouteTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "ReviewerRouteTool is disabled: missing client or context", IsError: true}
	}
	availableReviewers, ok := toStringSlice(args["available_reviewers"])
	if !ok || len(availableReviewers) == 0 {
		return &ToolResult{Output: "available_reviewers is required", IsError: true}
	}
	isMultiPatch, ok := args["is_multi_patch"].(bool)
	if !ok {
		return &ToolResult{Output: "is_multi_patch is required", IsError: true}
	}
	impactScore, ok := toFloat64(args["impact_score"])
	if !ok {
		return &ToolResult{Output: "impact_score is required", IsError: true}
	}
	contradictionPressure, ok := toFloat64(args["contradiction_pressure"])
	if !ok {
		return &ToolResult{Output: "contradiction_pressure is required", IsError: true}
	}
	hasActiveDissent, ok := args["has_active_dissent"].(bool)
	if !ok {
		return &ToolResult{Output: "has_active_dissent is required", IsError: true}
	}
	touchesHardConstraint, ok := args["touches_hard_constraint"].(bool)
	if !ok {
		return &ToolResult{Output: "touches_hard_constraint is required", IsError: true}
	}
	clusterMode, _ := args["cluster_mode"].(string)
	if strings.TrimSpace(clusterMode) == "" {
		return &ToolResult{Output: "cluster_mode is required", IsError: true}
	}
	mergeRisk, ok := toFloat64(args["merge_risk"])
	if !ok {
		return &ToolResult{Output: "merge_risk is required", IsError: true}
	}

	rawMsg, err := t.client.RouteReviewer(ctx, ReviewerRouteInput{
		WorkspaceID:           t.workspaceID,
		BundleID:              strings.TrimSpace(stringArg(args, "bundle_id")),
		GeneratorAgentID:      t.agentID,
		AvailableReviewers:    availableReviewers,
		IsMultiPatch:          isMultiPatch,
		ImpactScore:           impactScore,
		ContradictionPressure: contradictionPressure,
		HasActiveDissent:      hasActiveDissent,
		TouchesHardConstraint: touchesHardConstraint,
		ClusterMode:           clusterMode,
		MergeRisk:             mergeRisk,
	})
	if err != nil {
		// OE-1 (static-audit 2026-06-02): enrich the opaque routing error so a verifier/
		// generator agent has a concrete self-correction path instead of a Run25 dead-end.
		return &ToolResult{Output: fmt.Sprintf("reviewer_route failed to route reviewers: %v.\nNo reviewer was assigned. Next: verify available_reviewers lists live non-self agent ids and the route evidence (impact_score, merge_risk, contradiction_pressure) is well-formed in 0..1; if the mesh is transiently unavailable, call reviewer_scarcity then retry once; otherwise record a blocker/tension rather than treating the bundle as reviewed.", err), IsError: true}
	}

	// Convert result nicely.
	respStr := string(rawMsg)
	if respStr == "" || respStr == "null" {
		respStr = "{}"
	}
	var outMap map[string]any
	if json.Unmarshal(rawMsg, &outMap) == nil {
		if b, e := json.MarshalIndent(outMap, "", "  "); e == nil {
			respStr = string(b)
		}
		if reviewerRouteScarcitySaturated(outMap) {
			return &ToolResult{
				Output:  fmt.Sprintf("Reviewer route deferred: reviewer mesh is saturated; do not treat this as a successful reviewer assignment.\n%s", respStr),
				IsError: true,
			}
		}
	}

	// SG-RM-1 (static-audit 2026-06-02): routing hands the bundle to a DIFFERENT agent, so the
	// success result must carry a next-step contract — this is a routing receipt, not a review.
	return &ToolResult{Output: fmt.Sprintf("Reviewer route requested successfully (this is a ROUTING RECEIPT, not a completed review):\n%s\n\nNext: a reviewer was selected; do NOT re-route the same bundle. If the routed reviewer is not auto-woken, send it an agent_request to review this bundle; otherwise leave the bundle for the reviewer and continue your own owned lane. Await the reviewer verdict before treating this bundle as reviewed.", respStr)}
}

func reviewerRouteScarcitySaturated(route map[string]any) bool {
	if strings.EqualFold(strings.TrimSpace(stringArg(route, "near_reviewer_status")), reviewerRouteNearStatusScarcitySaturated) {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(stringArg(route, "far_reviewer_status")), reviewerRouteFarStatusScarcitySaturated) {
		return true
	}
	if values, ok := toStringSlice(route["warnings"]); ok {
		for _, value := range values {
			if strings.EqualFold(strings.TrimSpace(value), reviewerRouteScarcitySaturatedReason) {
				return true
			}
		}
	}
	if values, ok := toStringSlice(route["reasons"]); ok {
		for _, value := range values {
			if strings.EqualFold(strings.TrimSpace(value), reviewerRouteScarcitySaturatedReason) {
				return true
			}
		}
	}
	return false
}

type ReviewerScarcityTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewReviewerScarcityTool(client *RhizomeClient, workspaceID, agentID string) *ReviewerScarcityTool {
	return &ReviewerScarcityTool{
		client:      client,
		workspaceID: workspaceID,
		agentID:     agentID,
	}
}

func (t *ReviewerScarcityTool) Name() string { return "reviewer_scarcity" }
func (t *ReviewerScarcityTool) Description() string {
	return "Read the persisted reviewer-load snapshot for the current workspace. This surface is intentionally partial and does not claim full capacity truth."
}
func (t *ReviewerScarcityTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
		"required":   []string{},
	}
}

func (t *ReviewerScarcityTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "ReviewerScarcityTool is disabled: missing client or context", IsError: true}
	}

	rawMsg, err := t.client.CheckReviewerScarcity(ctx, ReviewerScarcityInput{
		WorkspaceID: t.workspaceID,
	})
	if err != nil {
		// OE-2 (static-audit 2026-06-02): this is the fallback path agents are told to use when
		// routing fails (OE-1); an opaque error here collapses the recovery. Steer conservatively.
		return &ToolResult{Output: fmt.Sprintf("reviewer_scarcity could not read the reviewer-load snapshot: %v.\nTreat reviewer capacity as unknown, not zero: defer opening new reviewer-admission work and retry this read once before escalating; do not assume reviewers are free.", err), IsError: true}
	}

	respStr := string(rawMsg)
	if respStr == "" || respStr == "null" {
		respStr = "{}"
	}
	var outMap map[string]any
	if json.Unmarshal(rawMsg, &outMap) == nil {
		if b, e := json.MarshalIndent(outMap, "", "  "); e == nil {
			respStr = string(b)
		}
		if reviewerScarcitySnapshotSaturated(outMap) {
			return &ToolResult{
				Output:  fmt.Sprintf("Reviewer scarcity is saturated; defer new reviewer admission work.\n%s", respStr),
				IsError: true,
			}
		}
	}

	return &ToolResult{Output: fmt.Sprintf("Reviewer scarcity snapshot:\n%s", respStr)}
}

func reviewerScarcitySnapshotSaturated(snapshot map[string]any) bool {
	return strings.EqualFold(strings.TrimSpace(stringArg(snapshot, "status")), "SATURATED")
}

func toFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func toStringSlice(value any) ([]string, bool) {
	switch v := value.(type) {
	case []string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if trimmed := strings.TrimSpace(item); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out, true
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			text, ok := item.(string)
			if !ok {
				return nil, false
			}
			if trimmed := strings.TrimSpace(text); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out, true
	default:
		return nil, false
	}
}

func stringArg(args map[string]any, key string) string {
	text, _ := args[key].(string)
	return text
}
