package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type CoalitionOfferTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewCoalitionOfferTool(client *RhizomeClient, workspaceID, agentID string) *CoalitionOfferTool {
	return &CoalitionOfferTool{
		client:      client,
		workspaceID: workspaceID,
		agentID:     agentID,
	}
}

func (t *CoalitionOfferTool) Name() string { return "coalition_offer" }
func (t *CoalitionOfferTool) Description() string {
	return "Offer to join an agent coalition for resolving a task or tension. Use this when you are taking responsibility alongside other agents."
}
func (t *CoalitionOfferTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"task_id": map[string]any{"type": "string", "description": "The ID of the task to attach to"},
			"role":    map[string]any{"type": "string", "description": "Your intended role in the coalition (e.g., 'REVIEWER', 'PRIMARY')"},
		},
		"required": []string{"task_id", "role"},
	}
}
func (t *CoalitionOfferTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "CoalitionOfferTool is disabled: missing client or context", IsError: true}
	}
	taskID, _ := args["task_id"].(string)
	role, _ := args["role"].(string)
	taskID = strings.TrimSpace(taskID)
	role = strings.TrimSpace(role)

	if taskID == "" || role == "" {
		return &ToolResult{Output: "task_id and role are required", IsError: true}
	}

	result, err := t.client.OfferCoalition(ctx, CoalitionOfferInput{
		WorkspaceID: t.workspaceID,
		TaskID:      taskID,
		AgentID:     t.agentID,
		ActorID:     t.agentID,
		Role:        role,
	})
	if err != nil {
		if coalitionOfferDurabilityError(err) {
			return &ToolResult{Output: fmt.Sprintf("failed to offer coalition for task %s: %v", taskID, err), IsError: true}
		}
		return &ToolResult{Output: fmt.Sprintf("coalition_offer unavailable for task %s: %v. Treat coalition bookkeeping as advisory; do not retry coalition_offer blindly. Continue coordination through agent_request, task_submit, workspace docs, or one durable tension/blocker.", taskID, err)}
	}
	if !result.Changed {
		return &ToolResult{Output: fmt.Sprintf("already offered or attached to coalition %s for task %s", firstNonEmpty(result.CoalitionID, result.Coalition.CoalitionID), taskID)}
	}
	return &ToolResult{Output: fmt.Sprintf("successfully offered to join coalition %s for task %s as %s (event %s)", firstNonEmpty(result.CoalitionID, result.Coalition.CoalitionID), taskID, role, result.Event.EventID)}
}

func coalitionOfferDurabilityError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "rpc returned success") ||
		strings.Contains(msg, "without runtime event") ||
		strings.Contains(msg, "unexpected event_type")
}

type CoalitionLeaveTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewCoalitionLeaveTool(client *RhizomeClient, workspaceID, agentID string) *CoalitionLeaveTool {
	return &CoalitionLeaveTool{
		client:      client,
		workspaceID: workspaceID,
		agentID:     agentID,
	}
}

func (t *CoalitionLeaveTool) Name() string { return "coalition_leave" }
func (t *CoalitionLeaveTool) Description() string {
	return "Leave an active coalition. Use this if you are abandoning a dead-end approach or your role is complete."
}
func (t *CoalitionLeaveTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"coalition_id": map[string]any{"type": "string", "description": "The ID of the coalition to leave from"},
			"reason":       map[string]any{"type": "string", "description": "Why you are leaving (optional)"},
		},
		"required": []string{"coalition_id"},
	}
}
func (t *CoalitionLeaveTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "CoalitionLeaveTool is disabled: missing client or context", IsError: true}
	}
	coalitionID, _ := args["coalition_id"].(string)
	reason, _ := args["reason"].(string)
	coalitionID = strings.TrimSpace(coalitionID)
	reason = strings.TrimSpace(reason)

	if coalitionID == "" {
		return &ToolResult{Output: "coalition_id is required", IsError: true}
	}

	result, err := t.client.LeaveCoalition(ctx, CoalitionLeaveInput{
		WorkspaceID: t.workspaceID,
		CoalitionID: coalitionID,
		AgentID:     t.agentID,
		ActorID:     t.agentID,
		Reason:      reason,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("failed to leave coalition %s: %v", coalitionID, err), IsError: true}
	}
	if !result.Changed {
		return &ToolResult{Output: fmt.Sprintf("already absent from coalition %s", coalitionID)}
	}
	return &ToolResult{Output: fmt.Sprintf("successfully left coalition %s (event %s)", coalitionID, result.Event.EventID)}
}

type CoalitionSeekTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewCoalitionSeekTool(client *RhizomeClient, workspaceID, agentID string) *CoalitionSeekTool {
	return &CoalitionSeekTool{client: client, workspaceID: workspaceID, agentID: agentID}
}
func (t *CoalitionSeekTool) Name() string { return "coalition_seek" }
func (t *CoalitionSeekTool) Description() string {
	return "Seek help by broadcasting a requirement for certain skills or roles. If no useful coalition is returned, use agent_request to ask a concrete peer directly."
}
func (t *CoalitionSeekTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"task_id":         map[string]any{"type": "string"},
			"required_skills": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"reason":          map[string]any{"type": "string"},
		},
		"required": []string{"task_id"},
	}
}
func (t *CoalitionSeekTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "Disabled", IsError: true}
	}

	taskID, _ := args["task_id"].(string)
	reason, _ := args["reason"].(string)

	reqSkills := []string{}
	if arr, ok := args["required_skills"].([]any); ok {
		for _, v := range arr {
			if s, ok := v.(string); ok {
				reqSkills = append(reqSkills, s)
			}
		}
	}

	raw, err := t.client.SeekCoalition(ctx, CoalitionSeekInput{
		WorkspaceID: t.workspaceID, TaskID: taskID, AgentID: t.agentID, RequiredSkills: reqSkills, Reason: reason,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("error: %v. Coalition discovery did not produce a coordination path; fall back to agent_request with a bounded ask to a suitable peer.", err), IsError: true}
	}
	if hint := coalitionSeekFallbackHint(raw); hint != "" {
		return &ToolResult{Output: hint + "\nRaw coalition response: " + string(raw)}
	}
	return &ToolResult{Output: fmt.Sprintf("Seek broadcasted: %s", string(raw))}
}

func coalitionSeekFallbackHint(raw json.RawMessage) string {
	var payload struct {
		Matches          []json.RawMessage `json:"matches"`
		TargetResolution struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		} `json:"target_resolution"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	if len(payload.Matches) > 0 {
		return ""
	}
	targetStatus := strings.TrimSpace(payload.TargetResolution.Status)
	if targetStatus == "" {
		targetStatus = "not_requested"
	}
	targetDetail := targetStatus
	if errText := strings.TrimSpace(payload.TargetResolution.Error); errText != "" {
		targetDetail += ": " + errText
	}
	return "No coalition matches were available (target_resolution=" + targetDetail + "). Use agent_request for direct peer review, implementation help, or synthesis instead of retrying coalition_seek."
}

type CoalitionInviteTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewCoalitionInviteTool(client *RhizomeClient, workspaceID, agentID string) *CoalitionInviteTool {
	return &CoalitionInviteTool{client: client, workspaceID: workspaceID, agentID: agentID}
}
func (t *CoalitionInviteTool) Name() string { return "coalition_invite" }
func (t *CoalitionInviteTool) Description() string {
	return "Directly invite another agent to a coalition."
}
func (t *CoalitionInviteTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"coalition_id": map[string]any{"type": "string"},
			"target_id":    map[string]any{"type": "string", "description": "Agent ID to invite"},
			"role":         map[string]any{"type": "string", "description": "Suggested role"},
		},
		"required": []string{"coalition_id", "target_id"},
	}
}
func (t *CoalitionInviteTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "Disabled", IsError: true}
	}
	coalitionID, _ := args["coalition_id"].(string)
	targetID, _ := args["target_id"].(string)
	role, _ := args["role"].(string)
	coalitionID = strings.TrimSpace(coalitionID)
	targetID = strings.TrimSpace(targetID)
	role = strings.TrimSpace(role)
	if coalitionID == "" || targetID == "" {
		return &ToolResult{Output: "coalition_id and target_id are required", IsError: true}
	}
	result, err := t.client.InviteCoalition(ctx, CoalitionInviteInput{
		WorkspaceID: t.workspaceID, CoalitionID: coalitionID, ActorID: t.agentID, AgentID: t.agentID, TargetID: targetID, Role: role,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("error: %v", err), IsError: true}
	}
	if !result.Changed {
		return &ToolResult{Output: fmt.Sprintf("%s is already attached to coalition %s", targetID, coalitionID)}
	}
	return &ToolResult{Output: fmt.Sprintf("Invited %s to %s (event %s)", targetID, coalitionID, result.Event.EventID)}
}

type CoalitionKickTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewCoalitionKickTool(client *RhizomeClient, workspaceID, agentID string) *CoalitionKickTool {
	return &CoalitionKickTool{client: client, workspaceID: workspaceID, agentID: agentID}
}
func (t *CoalitionKickTool) Name() string { return "coalition_kick" }
func (t *CoalitionKickTool) Description() string {
	return "Kick an agent from a coalition. Must be primary/steward."
}
func (t *CoalitionKickTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"coalition_id": map[string]any{"type": "string"},
			"target_id":    map[string]any{"type": "string", "description": "Agent ID to kick"},
			"reason":       map[string]any{"type": "string"},
		},
		"required": []string{"coalition_id", "target_id"},
	}
}
func (t *CoalitionKickTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "Disabled", IsError: true}
	}
	coalitionID, _ := args["coalition_id"].(string)
	targetID, _ := args["target_id"].(string)
	reason, _ := args["reason"].(string)
	coalitionID = strings.TrimSpace(coalitionID)
	targetID = strings.TrimSpace(targetID)
	reason = strings.TrimSpace(reason)
	if coalitionID == "" || targetID == "" {
		return &ToolResult{Output: "coalition_id and target_id are required", IsError: true}
	}
	result, err := t.client.KickCoalition(ctx, CoalitionKickInput{
		WorkspaceID: t.workspaceID, CoalitionID: coalitionID, ActorID: t.agentID, AgentID: t.agentID, TargetID: targetID, Reason: reason,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("error: %v", err), IsError: true}
	}
	if !result.Changed {
		return &ToolResult{Output: fmt.Sprintf("%s is already absent from coalition %s", targetID, coalitionID)}
	}
	return &ToolResult{Output: fmt.Sprintf("Kicked %s from %s (event %s)", targetID, coalitionID, result.Event.EventID)}
}

type CoalitionStatusTool struct {
	client      *RhizomeClient
	workspaceID string
}

func NewCoalitionStatusTool(client *RhizomeClient, workspaceID string) *CoalitionStatusTool {
	return &CoalitionStatusTool{client: client, workspaceID: workspaceID}
}
func (t *CoalitionStatusTool) Name() string { return "coalition_status" }
func (t *CoalitionStatusTool) Description() string {
	return "Get detailed status and members of a coalition."
}
func (t *CoalitionStatusTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"coalition_id": map[string]any{"type": "string"},
		},
		"required": []string{"coalition_id"},
	}
}
func (t *CoalitionStatusTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" {
		return &ToolResult{Output: "Disabled", IsError: true}
	}
	coalitionID, _ := args["coalition_id"].(string)
	raw, err := t.client.GetCoalitionStatus(ctx, CoalitionStatusInput{
		WorkspaceID: t.workspaceID, CoalitionID: coalitionID,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("error: %v", err), IsError: true}
	}
	return &ToolResult{Output: string(raw)}
}
