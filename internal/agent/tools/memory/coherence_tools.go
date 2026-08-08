package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	agenttools "github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// WorkspaceMemoryPromotionReadTool natively queries promotion and coherence gates via SQLite.
type WorkspaceMemoryPromotionReadTool struct {
	store       *sqlite.Store
	workspaceID string
}

func NewWorkspaceMemoryPromotionReadTool(store *sqlite.Store, workspaceID string) *WorkspaceMemoryPromotionReadTool {
	return &WorkspaceMemoryPromotionReadTool{store: store, workspaceID: workspaceID}
}

func (t *WorkspaceMemoryPromotionReadTool) Name() string { return "memory_promotion_read" }

func (t *WorkspaceMemoryPromotionReadTool) Description() string {
	return "Read advisory memory-promotion candidates and their current review/coherence state from the canonical promotion queue."
}

func (t *WorkspaceMemoryPromotionReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"promotion_id": {
				Type:        "string",
				Description: "Optional promotion id to fetch one advisory candidate directly.",
			},
			"state": {
				Type:        "string",
				Description: "Optional promotion state filter.",
				Enum:        []string{"PENDING", "ACCEPTED", "REJECTED", "SUPERSEDED", "CANCELLED"},
				Default:     "PENDING",
			},
			"candidate_kind": {
				Type:        "string",
				Description: "Optional candidate kind filter.",
				Enum:        []string{"WORKSPACE_MEMORY"},
			},
			"candidate_type": {
				Type:        "string",
				Description: "Optional candidate type filter.",
				Enum:        canonicalMemoryTypes,
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of promotion candidates to return when listing.",
				Default:     10,
			},
		},
	}
}

func (t *WorkspaceMemoryPromotionReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		PromotionID   string `json:"promotion_id"`
		State         string `json:"state"`
		CandidateKind string `json:"candidate_kind"`
		CandidateType string `json:"candidate_type"`
		Limit         int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_promotion_read: invalid input: %w", err)
	}

	if strings.TrimSpace(params.PromotionID) != "" {
		record, err := t.store.GetMemoryPromotion(ctx, t.workspaceID, params.PromotionID)
		if err != nil {
			return "", fmt.Errorf("memory_promotion_read: %w", err)
		}
		out, err := json.Marshal(map[string]any{
			"workspace_id": t.workspaceID,
			"promotion":    record,
			"advisory":     buildWorkspaceMemoryPromotionAdvisory(record),
		})
		if err != nil {
			return "", fmt.Errorf("memory_promotion_read: marshal result: %w", err)
		}
		return string(out), nil
	}

	items, err := t.store.ListMemoryPromotions(ctx, sqlite.MemoryPromotionFilter{
		WorkspaceID:   t.workspaceID,
		State:         params.State,
		CandidateKind: params.CandidateKind,
		CandidateType: params.CandidateType,
		Limit:         params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("memory_promotion_read: %w", err)
	}
	advisoryItems := make([]workspaceMemoryPromotionAdvisory, 0, len(items))
	for _, item := range items {
		advisoryItems = append(advisoryItems, buildWorkspaceMemoryPromotionAdvisory(item))
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":   t.workspaceID,
		"count":          len(items),
		"items":          items,
		"advisory_items": advisoryItems,
	})
	if err != nil {
		return "", fmt.Errorf("memory_promotion_read: marshal result: %w", err)
	}
	return string(out), nil
}

type workspaceMemoryPromotionAdvisory struct {
	PromotionID    string `json:"promotion_id"`
	State          string `json:"state"`
	ReviewAction   string `json:"review_action"`
	Source         string `json:"source"`
	CoherenceBand  string `json:"coherence_band,omitempty"`
	NeedsAttention bool   `json:"needs_attention"`
}

func buildWorkspaceMemoryPromotionAdvisory(record sqlite.MemoryPromotionRecord) workspaceMemoryPromotionAdvisory {
	advisory := workspaceMemoryPromotionAdvisory{
		PromotionID: strings.TrimSpace(record.PromotionID),
		State:       strings.TrimSpace(record.State),
		Source:      "promotion_record",
	}
	if record.CoherenceGate != nil {
		advisory.CoherenceBand = strings.TrimSpace(record.CoherenceGate.CoherenceBand)
		advisory.NeedsAttention = record.CoherenceGate.NeedsAttention
		if action := strings.TrimSpace(record.CoherenceGate.AdvisoryAction); action != "" {
			advisory.ReviewAction = action
		}
	}
	if advisory.ReviewAction == "" {
		if strings.EqualFold(strings.TrimSpace(record.State), "PENDING") {
			advisory.ReviewAction = "REVIEW"
		} else {
			advisory.ReviewAction = "RESOLVED"
		}
	}
	return advisory
}

// WorkspaceMemoryCoherenceReadTool queries the agent memory coherence baseline via SQLite.
type WorkspaceMemoryCoherenceReadTool struct {
	store       *sqlite.Store
	workspaceID string
}

func NewWorkspaceMemoryCoherenceReadTool(store *sqlite.Store, workspaceID string) *WorkspaceMemoryCoherenceReadTool {
	return &WorkspaceMemoryCoherenceReadTool{store: store, workspaceID: workspaceID}
}

func (t *WorkspaceMemoryCoherenceReadTool) Name() string { return "memory_coherence_read" }

func (t *WorkspaceMemoryCoherenceReadTool) Description() string {
	return "Read the current agent or session-scoped memory coherence attention snapshot from the existing canonical read-side."
}

func (t *WorkspaceMemoryCoherenceReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"agent_id": {
				Type:        "string",
				Description: "Agent id whose coherence scope should be read.",
			},
			"session_id": {
				Type:        "string",
				Description: "Optional session id to narrow coherence scope to one live session.",
			},
			"report_scope": {
				Type:        "string",
				Description: "Optional scope selector.",
				Enum:        []string{"AGENT", "SESSION"},
			},
		},
		Required: []string{"agent_id"},
	}
}

func (t *WorkspaceMemoryCoherenceReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		AgentID     string `json:"agent_id"`
		SessionID   string `json:"session_id"`
		ReportScope string `json:"report_scope"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_coherence_read: invalid input: %w", err)
	}
	if strings.TrimSpace(params.AgentID) == "" {
		return "", fmt.Errorf("memory_coherence_read: agent_id is required")
	}

	scope, err := t.store.GetMemoryCoherenceScope(ctx, t.workspaceID, params.AgentID, params.SessionID, params.ReportScope)
	if err != nil {
		return "", fmt.Errorf("memory_coherence_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id": t.workspaceID,
		"scope":        scope,
	})
	if err != nil {
		return "", fmt.Errorf("memory_coherence_read: marshal result: %w", err)
	}
	return string(out), nil
}

// WorkspaceMemoryInvalidationReadTool lists agent invalidations via SQLite.
type WorkspaceMemoryInvalidationReadTool struct {
	store       *sqlite.Store
	workspaceID string
	agentID     string
}

func NewWorkspaceMemoryInvalidationReadTool(store *sqlite.Store, workspaceID, agentID string) *WorkspaceMemoryInvalidationReadTool {
	return &WorkspaceMemoryInvalidationReadTool{store: store, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceMemoryInvalidationReadTool) Name() string { return "memory_invalidation_read" }

func (t *WorkspaceMemoryInvalidationReadTool) Description() string {
	return "Read the current canonical memory invalidation queue rows for one agent from the existing invalidation read-side."
}

func (t *WorkspaceMemoryInvalidationReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id. Defaults to the current agent when omitted.",
			},
			"session_id": {
				Type:        "string",
				Description: "Optional session id to narrow the invalidation queue to one live session.",
			},
			"include_acked": {
				Type:        "boolean",
				Description: "Whether acknowledged invalidations should also be returned.",
			},
			"include_dead_letter": {
				Type:        "boolean",
				Description: "Whether dead-letter invalidations should also be returned.",
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of invalidation rows to return.",
				Default:     50,
			},
		},
	}
}

func (t *WorkspaceMemoryInvalidationReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		AgentID           string `json:"agent_id"`
		SessionID         string `json:"session_id"`
		IncludeAcked      bool   `json:"include_acked"`
		IncludeDeadLetter bool   `json:"include_dead_letter"`
		Limit             int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_invalidation_read: invalid input: %w", err)
	}
	agentID := params.AgentID
	if strings.TrimSpace(agentID) == "" {
		agentID = t.agentID
	}
	if agentID == "" {
		return "", fmt.Errorf("memory_invalidation_read: agent_id is required")
	}

	result, err := t.store.ListMemoryInvalidations(ctx, sqlite.MemoryInvalidationListFilter{
		WorkspaceID:       t.workspaceID,
		AgentID:           agentID,
		SessionID:         params.SessionID,
		IncludeAcked:      params.IncludeAcked,
		IncludeDeadLetter: params.IncludeDeadLetter,
		Limit:             params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("memory_invalidation_read: %w", err)
	}
	out, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("memory_invalidation_read: marshal result: %w", err)
	}
	return string(out), nil
}

// WorkspaceMemoryInvalidationItemReadTool queries a specialized single agent invalidation via SQLite.
type WorkspaceMemoryInvalidationItemReadTool struct {
	store       *sqlite.Store
	workspaceID string
	agentID     string
}

func NewWorkspaceMemoryInvalidationItemReadTool(store *sqlite.Store, workspaceID, agentID string) *WorkspaceMemoryInvalidationItemReadTool {
	return &WorkspaceMemoryInvalidationItemReadTool{store: store, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceMemoryInvalidationItemReadTool) Name() string {
	return "memory_invalidation_item_read"
}

func (t *WorkspaceMemoryInvalidationItemReadTool) Description() string {
	return "Read one canonical memory invalidation record from the existing invalidation ledger."
}

func (t *WorkspaceMemoryInvalidationItemReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"invalidation_id": {
				Type:        "string",
				Description: "Exact ID of the invalidation to read.",
			},
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id. Defaults to the current agent when omitted.",
			},
		},
		Required: []string{"invalidation_id"},
	}
}

func (t *WorkspaceMemoryInvalidationItemReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		InvalidationID string `json:"invalidation_id"`
		AgentID        string `json:"agent_id"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_invalidation_item_read: invalid input: %w", err)
	}
	if strings.TrimSpace(params.InvalidationID) == "" {
		return "", fmt.Errorf("memory_invalidation_item_read: invalidation_id is required")
	}
	agentID := params.AgentID
	if strings.TrimSpace(agentID) == "" {
		agentID = t.agentID
	}
	if agentID == "" {
		return "", fmt.Errorf("memory_invalidation_item_read: agent_id is required")
	}

	record, err := t.store.GetMemoryInvalidation(ctx, t.workspaceID, agentID, params.InvalidationID)
	if err != nil {
		return "", fmt.Errorf("memory_invalidation_item_read: %w", err)
	}
	out, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf("memory_invalidation_item_read: marshal result: %w", err)
	}
	return string(out), nil
}

// RegisterCoherenceTools registers promotion and invalidation visibility tools.
func RegisterCoherenceTools(reg *agenttools.Registry, store *sqlite.Store, workspaceID, agentID string) {
	reg.Register(NewWorkspaceMemoryPromotionReadTool(store, workspaceID))
	reg.Register(NewWorkspaceMemoryCoherenceReadTool(store, workspaceID))
	reg.Register(NewWorkspaceMemoryInvalidationReadTool(store, workspaceID, agentID))
	reg.Register(NewWorkspaceMemoryInvalidationItemReadTool(store, workspaceID, agentID))
}
