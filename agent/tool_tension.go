package main

import (
	"context"
	"fmt"
	"strings"
)

type TensionAttachTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewTensionAttachTool(client *RhizomeClient, workspaceID, agentID string) *TensionAttachTool {
	return &TensionAttachTool{
		client:      client,
		workspaceID: workspaceID,
		agentID:     agentID,
	}
}

func (t *TensionAttachTool) Name() string { return "tension_attach" }
func (t *TensionAttachTool) Description() string {
	return "Attach yourself directly to an active system tension to monitor or assist. Note: To officially join a coalition to perform a task, use the coalition_offer tool instead."
}
func (t *TensionAttachTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"tension_id": map[string]any{"type": "string", "description": "The ID of the tension to attach to"},
			"role":       map[string]any{"type": "string", "description": "Your intended role in the coalition (e.g., 'lead', 'reviewer', 'contributor')"},
			"reason":     map[string]any{"type": "string", "description": "Why you are attaching (optional)"},
		},
		"required": []string{"tension_id", "role"},
	}
}
func (t *TensionAttachTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "TensionAttachTool is disabled: missing client or context", IsError: true}
	}
	tensionID, _ := args["tension_id"].(string)
	role, _ := args["role"].(string)
	reason, _ := args["reason"].(string)

	if strings.TrimSpace(tensionID) == "" || strings.TrimSpace(role) == "" {
		return &ToolResult{Output: "tension_id and role are required", IsError: true}
	}

	result, err := t.client.AttachTensionAgent(ctx, TensionAgentAttachInput{
		WorkspaceID:      t.workspaceID,
		TensionID:        strings.TrimSpace(tensionID),
		AgentID:          t.agentID,
		ActorID:          t.agentID,
		SuccessCriterion: "Attached as: " + strings.TrimSpace(role),
		Reason:           strings.TrimSpace(reason),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("failed to attach to tension %s: %v", tensionID, err), IsError: true}
	}
	coalitionID := firstNonEmpty(result.CoalitionID, result.Coalition.CoalitionID)
	if !result.Changed {
		return &ToolResult{Output: fmt.Sprintf("already attached to tension %s (coalition: %s); no duplicate coalition event was emitted", strings.TrimSpace(tensionID), coalitionID)}
	}
	return &ToolResult{Output: fmt.Sprintf("successfully attached to tension %s (coalition: %s) as %s%s", strings.TrimSpace(tensionID), coalitionID, strings.TrimSpace(role), formatTensionMutationEventSuffix(result))}
}

type TensionDetachTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewTensionDetachTool(client *RhizomeClient, workspaceID, agentID string) *TensionDetachTool {
	return &TensionDetachTool{
		client:      client,
		workspaceID: workspaceID,
		agentID:     agentID,
	}
}

func (t *TensionDetachTool) Name() string { return "tension_detach" }
func (t *TensionDetachTool) Description() string {
	return "Detach yourself from a tension. Use this if you are leaving the coalition or abandoning a dead-end approach."
}
func (t *TensionDetachTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"tension_id": map[string]any{"type": "string", "description": "The ID of the tension to detach from"},
			"reason":     map[string]any{"type": "string", "description": "Why you are detaching (optional)"},
		},
		"required": []string{"tension_id"},
	}
}
func (t *TensionDetachTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "TensionDetachTool is disabled: missing client or context", IsError: true}
	}
	tensionID, _ := args["tension_id"].(string)
	reason, _ := args["reason"].(string)

	if strings.TrimSpace(tensionID) == "" {
		return &ToolResult{Output: "tension_id is required", IsError: true}
	}

	coalition, err := t.client.ResolveTensionAgentCoalition(ctx, t.workspaceID, tensionID, t.agentID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("failed to resolve live coalition for tension %s: %v", tensionID, err), IsError: true}
	}

	result, err := t.client.DetachTensionAgent(ctx, TensionAgentDetachInput{
		WorkspaceID: t.workspaceID,
		CoalitionID: coalition.CoalitionID,
		AgentID:     t.agentID,
		ActorID:     t.agentID,
		Reason:      strings.TrimSpace(reason),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("failed to detach from tension %s: %v", tensionID, err), IsError: true}
	}
	if !result.Changed {
		return &ToolResult{Output: fmt.Sprintf("already detached from tension %s (coalition: %s); no duplicate coalition event was emitted", strings.TrimSpace(tensionID), coalition.CoalitionID)}
	}
	return &ToolResult{Output: fmt.Sprintf("successfully detached from tension %s (coalition: %s)%s", strings.TrimSpace(tensionID), coalition.CoalitionID, formatTensionMutationEventSuffix(result))}
}

func formatTensionMutationEventSuffix(result TensionAgentMutationResult) string {
	eventID := strings.TrimSpace(result.Event.EventID)
	eventType := strings.TrimSpace(result.Event.EventType)
	if eventID == "" {
		return ""
	}
	if eventType == "" {
		return fmt.Sprintf("; event: %s", eventID)
	}
	return fmt.Sprintf("; event: %s (%s)", eventID, eventType)
}

type TensionLifecycleTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewTensionLifecycleTool(client *RhizomeClient, workspaceID, agentID string) *TensionLifecycleTool {
	return &TensionLifecycleTool{
		client:      client,
		workspaceID: workspaceID,
		agentID:     agentID,
	}
}

func (t *TensionLifecycleTool) Name() string { return "tension_lifecycle_update" }
func (t *TensionLifecycleTool) Description() string {
	return "Update the lifecycle state of a tension. Use this to mark a tension as RESOLVED (if you fixed the underlying issue) or DISCARDED (if it is a false positive or no longer relevant)."
}
func (t *TensionLifecycleTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"tension_id":      map[string]any{"type": "string", "description": "The ID of the tension"},
			"lifecycle_state": map[string]any{"type": "string", "description": "The new state: RESOLVED or DISCARDED"},
			"reason":          map[string]any{"type": "string", "description": "Why you are updating the state"},
		},
		"required": []string{"tension_id", "lifecycle_state", "reason"},
	}
}
func (t *TensionLifecycleTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "TensionLifecycleTool is disabled: missing client or context", IsError: true}
	}
	tensionID, _ := args["tension_id"].(string)
	state, _ := args["lifecycle_state"].(string)
	reason, _ := args["reason"].(string)

	if strings.TrimSpace(tensionID) == "" || strings.TrimSpace(state) == "" || strings.TrimSpace(reason) == "" {
		return &ToolResult{Output: "tension_id, lifecycle_state, and reason are required", IsError: true}
	}

	err := t.client.UpdateTensionLifecycle(ctx, TensionLifecycleUpdateInput{
		WorkspaceID:    t.workspaceID,
		TensionID:      tensionID,
		LifecycleState: state,
		UpdatedBy:      t.agentID,
		Reason:         reason,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("failed to update tension %s lifecycle to %s: %v", tensionID, state, err), IsError: true}
	}
	return &ToolResult{Output: fmt.Sprintf("successfully updated tension %s lifecycle to %s", tensionID, state)}
}
