package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	agenttools "github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// TensionAttachTool attaches the agent to a tension's primary coalition natively via SQLite.
type TensionAttachTool struct {
	store       *sqlite.Store
	workspaceID string
	agentID     string
}

func NewTensionAttachTool(store *sqlite.Store, workspaceID, agentID string) *TensionAttachTool {
	return &TensionAttachTool{
		store:       store,
		workspaceID: workspaceID,
		agentID:     agentID,
	}
}

func (t *TensionAttachTool) Name() string { return "tension_attach" }

func (t *TensionAttachTool) Description() string {
	return "Attach yourself directly to an active system tension to monitor or assist. Any requested role is advisory only; actual coalition membership remains system-normalized. To officially join a coalition to perform a task, use the coalition_offer tool instead."
}

func (t *TensionAttachTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"tension_id": {
				Type:        "string",
				Description: "The ID of the tension to attach to",
			},
			"role": {
				Type:        "string",
				Description: "Your requested role in the coalition (advisory only; actual coalition membership remains system-normalized)",
			},
			"reason": {
				Type:        "string",
				Description: "Why you are attaching (optional)",
			},
		},
		Required: []string{"tension_id", "role"},
	}
}

func (t *TensionAttachTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		TensionID string `json:"tension_id"`
		Role      string `json:"role"`
		Reason    string `json:"reason,omitempty"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("tension_attach: invalid input: %w", err)
	}
	if strings.TrimSpace(params.TensionID) == "" || strings.TrimSpace(params.Role) == "" {
		return "", fmt.Errorf("tension_attach: tension_id and role are required")
	}

	tensionID := strings.TrimSpace(params.TensionID)
	role := strings.TrimSpace(params.Role)
	result, err := t.store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                t.workspaceID,
		TensionID:                  tensionID,
		AgentID:                    t.agentID,
		ActorType:                  "agent",
		ActorID:                    t.agentID,
		SuccessCriterion:           "Attached as: " + role,
		Reason:                     strings.TrimSpace(params.Reason),
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("agent.tension.attach", "agent_tool", t.workspaceID, "agent", t.agentID),
		PromptContextSurface:       "agent.tension.attach",
		PromptContextPrincipalType: "agent",
		PromptContextPrincipalID:   t.agentID,
	})
	if err != nil {
		return "", fmt.Errorf("failed to attach to tension %s coalition: %w", params.TensionID, err)
	}
	if !result.Changed {
		return fmt.Sprintf("already attached to tension %s (coalition: %s); no duplicate coalition event was emitted", tensionID, result.Coalition.CoalitionID), nil
	}

	return fmt.Sprintf("successfully attached to tension %s (coalition: %s) with requested role %s; actual coalition membership remains system-normalized", tensionID, result.Coalition.CoalitionID, role), nil
}

// TensionDetachTool detaches the agent from a tension's primary coalition natively via SQLite.
type TensionDetachTool struct {
	store       *sqlite.Store
	workspaceID string
	agentID     string
}

func NewTensionDetachTool(store *sqlite.Store, workspaceID, agentID string) *TensionDetachTool {
	return &TensionDetachTool{
		store:       store,
		workspaceID: workspaceID,
		agentID:     agentID,
	}
}

func (t *TensionDetachTool) Name() string { return "tension_detach" }

func (t *TensionDetachTool) Description() string {
	return "Detach yourself from a tension. Use this if you are leaving the coalition or abandoning a dead-end approach."
}

func (t *TensionDetachTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"tension_id": {
				Type:        "string",
				Description: "The ID of the tension to detach from",
			},
			"reason": {
				Type:        "string",
				Description: "Why you are detaching (optional)",
			},
		},
		Required: []string{"tension_id"},
	}
}

func (t *TensionDetachTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		TensionID string `json:"tension_id"`
		Reason    string `json:"reason,omitempty"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("tension_detach: invalid input: %w", err)
	}
	if strings.TrimSpace(params.TensionID) == "" {
		return "", fmt.Errorf("tension_detach: tension_id is required")
	}

	coalition, err := t.store.GetTensionCoalition(ctx, t.workspaceID, params.TensionID)
	if err != nil {
		return "", fmt.Errorf("failed to discover active coalition for tension %s: %w", params.TensionID, err)
	}
	if coalition == nil {
		return "", fmt.Errorf("failed to discover active coalition for tension %s: no live coalition", params.TensionID)
	}

	result, err := t.store.DetachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                t.workspaceID,
		CoalitionID:                coalition.CoalitionID,
		AgentID:                    t.agentID,
		ActorType:                  "agent",
		ActorID:                    t.agentID,
		Reason:                     strings.TrimSpace(params.Reason),
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("agent.tension.detach", "agent_tool", t.workspaceID, "agent", t.agentID),
		PromptContextSurface:       "agent.tension.detach",
		PromptContextPrincipalType: "agent",
		PromptContextPrincipalID:   t.agentID,
	})
	if err != nil {
		if errors.Is(err, sqlite.ErrCoalitionActorNotMember) {
			return "", fmt.Errorf("failed to detach from tension %s: agent is not a member of the live coalition", params.TensionID)
		}
		return "", fmt.Errorf("failed to detach from tension %s: %w", params.TensionID, err)
	}
	if !result.Changed {
		return fmt.Sprintf("already detached from tension %s; no duplicate coalition event was emitted", params.TensionID), nil
	}

	return fmt.Sprintf("successfully detached from tension %s", params.TensionID), nil
}

// TensionLifecycleTool resolves or discards tensions natively via SQLite.
type TensionLifecycleTool struct {
	store       *sqlite.Store
	workspaceID string
	agentID     string
}

func NewTensionLifecycleTool(store *sqlite.Store, workspaceID, agentID string) *TensionLifecycleTool {
	return &TensionLifecycleTool{
		store:       store,
		workspaceID: workspaceID,
		agentID:     agentID,
	}
}

func (t *TensionLifecycleTool) Name() string { return "tension_lifecycle_update" }

func (t *TensionLifecycleTool) Description() string {
	return "Update the lifecycle state of a tension. Use this to mark a tension as RESOLVED (if you fixed the underlying issue) or DISCARDED (if it is a false positive or no longer relevant)."
}

func (t *TensionLifecycleTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"tension_id": {
				Type:        "string",
				Description: "The ID of the tension",
			},
			"lifecycle_state": {
				Type:        "string",
				Description: "The new state: RESOLVED or DISCARDED",
			},
			"reason": {
				Type:        "string",
				Description: "Why you are updating the state",
			},
		},
		Required: []string{"tension_id", "lifecycle_state", "reason"},
	}
}

func (t *TensionLifecycleTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		TensionID      string `json:"tension_id"`
		LifecycleState string `json:"lifecycle_state"`
		Reason         string `json:"reason"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("tension_lifecycle_update: invalid input: %w", err)
	}

	tensionID := strings.TrimSpace(params.TensionID)
	state := strings.ToUpper(strings.TrimSpace(params.LifecycleState))
	reason := strings.TrimSpace(params.Reason)

	if tensionID == "" || state == "" || reason == "" {
		return "", fmt.Errorf("tension_id, lifecycle_state, and reason are required")
	}

	mutationInput := sqlite.TensionMutationInput{
		WorkspaceID: t.workspaceID,
		TensionID:   tensionID,
		ActorID:     t.agentID,
		Reason:      reason,
	}

	var err error
	switch state {
	case "RESOLVED":
		_, err = t.store.ResolveTension(ctx, mutationInput)
	case "DISCARDED":
		_, err = t.store.DiscardTension(ctx, mutationInput)
	case "ARCHIVED":
		_, err = t.store.ArchiveTension(ctx, mutationInput)
	default:
		return "", fmt.Errorf("unsupported lifecycle state update: %s", state)
	}

	if err != nil {
		return "", fmt.Errorf("failed to update tension %s to %s: %w", tensionID, state, err)
	}

	return fmt.Sprintf("successfully updated tension %s to state %s", tensionID, state), nil
}

// RegisterTensionTools registers native tension control tools in the agent registry.
func RegisterTensionTools(reg *agenttools.Registry, store *sqlite.Store, workspaceID, agentID string) {
	reg.Register(NewTensionAttachTool(store, workspaceID, agentID))
	reg.Register(NewTensionDetachTool(store, workspaceID, agentID))
	reg.Register(NewTensionLifecycleTool(store, workspaceID, agentID))
}
