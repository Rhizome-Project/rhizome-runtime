package living

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	agenttools "github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

var canonicalMemoryTypes = []string{
	"NOTE",
	"LESSON",
	"DECISION",
	"PROCEDURE",
	"ANTI_PROCEDURE",
	"INCIDENT",
	"ENTITY",
	"EXPERIENCE",
	"UPDATE_DIGEST",
	"SUMMARY",
	"SELF_MODEL",
	"GOAL_COMMITMENT",
	"POLICY_TRACE",
}

func validateWorkspaceMemoryToolType(raw string) error {
	memoryType := strings.ToUpper(strings.TrimSpace(raw))
	if memoryType == "" {
		return nil
	}
	for _, allowed := range canonicalMemoryTypes {
		if memoryType == allowed {
			return nil
		}
	}
	return fmt.Errorf("memory_write: type must be one of: %s", strings.Join(canonicalMemoryTypes, ", "))
}

type WorkspaceMemorySearchTool struct {
	client      WorkspaceMemoryAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceMemorySearchTool(client WorkspaceMemoryAwareRhizomeClient, workspaceID string) *WorkspaceMemorySearchTool {
	return &WorkspaceMemorySearchTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceMemorySearchTool) Name() string { return "memory_search" }

func (t *WorkspaceMemorySearchTool) Description() string {
	return "Search canonical workspace memory using lexical retrieval over the current runtime truth."
}

func (t *WorkspaceMemorySearchTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"query": {
				Type:        "string",
				Description: "The lexical memory query.",
			},
			"type": {
				Type:        "string",
				Description: "Optional memory type filter.",
				Enum:        canonicalMemoryTypes,
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task filter.",
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of results to return.",
				Default:     10,
			},
		},
		Required: []string{"query"},
	}
}

func (t *WorkspaceMemorySearchTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Query  string `json:"query"`
		Type   string `json:"type"`
		TaskID string `json:"task_id"`
		Limit  int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_search: invalid input: %w", err)
	}

	items, err := t.client.SearchWorkspaceMemory(ctx, WorkspaceMemorySearchFilter{
		WorkspaceID: t.workspaceID,
		Query:       params.Query,
		MemoryType:  params.Type,
		TaskID:      params.TaskID,
		Limit:       params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("memory_search: %w", err)
	}

	out, err := json.Marshal(truncateWorkspaceMemoryRecords(items))
	if err != nil {
		return "", fmt.Errorf("memory_search: marshal results: %w", err)
	}
	return string(out), nil
}

type WorkspaceMemoryReadTool struct {
	client      WorkspaceMemoryAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceMemoryReadTool(client WorkspaceMemoryAwareRhizomeClient, workspaceID string) *WorkspaceMemoryReadTool {
	return &WorkspaceMemoryReadTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceMemoryReadTool) Name() string { return "memory_read" }

func (t *WorkspaceMemoryReadTool) Description() string {
	return "Read recent canonical workspace memory entries."
}

func (t *WorkspaceMemoryReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"type": {
				Type:        "string",
				Description: "Optional memory type filter.",
				Enum:        canonicalMemoryTypes,
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task filter.",
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of results to return.",
				Default:     10,
			},
		},
	}
}

func (t *WorkspaceMemoryReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Type   string `json:"type"`
		TaskID string `json:"task_id"`
		Limit  int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_read: invalid input: %w", err)
	}

	items, err := t.client.ListWorkspaceMemory(ctx, WorkspaceMemorySearchFilter{
		WorkspaceID: t.workspaceID,
		MemoryType:  params.Type,
		TaskID:      params.TaskID,
		Limit:       params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("memory_read: %w", err)
	}

	out, err := json.Marshal(items)
	if err != nil {
		return "", fmt.Errorf("memory_read: marshal results: %w", err)
	}
	return string(out), nil
}

type WorkspaceMemoryWriteTool struct {
	client      WorkspaceMemoryAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceMemoryWriteTool(client WorkspaceMemoryAwareRhizomeClient, workspaceID, agentID string) *WorkspaceMemoryWriteTool {
	return &WorkspaceMemoryWriteTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceMemoryWriteTool) Name() string { return "memory_write" }

func (t *WorkspaceMemoryWriteTool) Description() string {
	return "Save durable canonical workspace memory attributed to the current agent."
}

func (t *WorkspaceMemoryWriteTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"type": {
				Type:        "string",
				Description: "Memory type.",
				Enum:        canonicalMemoryTypes,
				Default:     "NOTE",
			},
			"topic": {
				Type:        "string",
				Description: "Short title or topic.",
			},
			"content": {
				Type:        "string",
				Description: "Durable memory content.",
			},
			"summary": {
				Type:        "string",
				Description: "Optional compact summary.",
			},
			"source": {
				Type:        "string",
				Description: "Source kind such as manual, compaction or reflection.",
				Default:     "manual",
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task association.",
			},
			"session_id": {
				Type:        "string",
				Description: "Optional session association.",
			},
			"tags": {
				Type:        "array",
				Description: "Optional tags.",
				Items:       &agenttools.Property{Type: "string"},
			},
			"importance": {
				Type:        "number",
				Description: "0..1 importance weight.",
			},
			"confidence": {
				Type:        "number",
				Description: "0..1 confidence weight.",
			},
		},
		Required: []string{"content"},
	}
}

func (t *WorkspaceMemoryWriteTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Type       string   `json:"type"`
		Topic      string   `json:"topic"`
		Content    string   `json:"content"`
		Summary    string   `json:"summary"`
		Source     string   `json:"source"`
		TaskID     string   `json:"task_id"`
		SessionID  string   `json:"session_id"`
		Tags       []string `json:"tags"`
		Importance float64  `json:"importance"`
		Confidence float64  `json:"confidence"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_write: invalid input: %w", err)
	}
	if err := validateWorkspaceMemoryToolType(params.Type); err != nil {
		return "", err
	}
	if strings.TrimSpace(params.Content) == "" {
		return "", fmt.Errorf("memory_write: content must not be empty")
	}

	writeInput := WorkspaceMemoryInput{
		WorkspaceID: t.workspaceID,
		MemoryType:  params.Type,
		Title:       params.Topic,
		Body:        params.Content,
		Summary:     params.Summary,
		AgentID:     t.agentID,
		SessionID:   params.SessionID,
		TaskID:      params.TaskID,
		SourceKind:  params.Source,
		SourceID:    t.agentID,
		Tags:        params.Tags,
		Importance:  params.Importance,
		Confidence:  params.Confidence,
	}
	outPayload := map[string]any{"status": "saved"}
	if effectsClient, ok := t.client.(WorkspaceMemoryEffectsAwareRhizomeClient); ok {
		result, err := effectsClient.RecordWorkspaceMemoryWithEffects(ctx, writeInput)
		if err != nil {
			return "", fmt.Errorf("memory_write: %w", err)
		}
		outPayload["memory"] = result.Memory
		if result.PromotedClaimEffects != nil {
			outPayload["promoted_claim_effects"] = result.PromotedClaimEffects
		}
	} else {
		record, err := t.client.RecordWorkspaceMemory(ctx, writeInput)
		if err != nil {
			return "", fmt.Errorf("memory_write: %w", err)
		}
		outPayload["memory"] = record
	}

	out, err := json.Marshal(outPayload)
	if err != nil {
		return "", fmt.Errorf("memory_write: marshal result: %w", err)
	}
	return string(out), nil
}

func truncateWorkspaceMemoryRecords(items []WorkspaceMemoryRecord) []WorkspaceMemoryRecord {
	out := make([]WorkspaceMemoryRecord, 0, len(items))
	for _, item := range items {
		copyItem := item
		if len(copyItem.Body) > 500 {
			copyItem.Body = copyItem.Body[:500] + "..."
		}
		out = append(out, copyItem)
	}
	return out
}

type WorkspaceMemoryPacketShellTool struct {
	client      WorkspaceMemoryPacketAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceMemoryPacketShellTool(client WorkspaceMemoryPacketAwareRhizomeClient, workspaceID, agentID string) *WorkspaceMemoryPacketShellTool {
	return &WorkspaceMemoryPacketShellTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceMemoryPacketShellTool) Name() string { return "memory_packet_shell" }

func (t *WorkspaceMemoryPacketShellTool) Description() string {
	return "Build a bounded shell memory packet for the current task or session using the existing canonical packet read-side."
}

func (t *WorkspaceMemoryPacketShellTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"task_id": {
				Type:        "string",
				Description: "Task id for the shell packet scope.",
			},
			"session_id": {
				Type:        "string",
				Description: "Optional session id for the shell packet scope.",
			},
			"doc_keys": {
				Type:        "array",
				Description: "Optional document keys to include in packet scope.",
				Items:       &agenttools.Property{Type: "string"},
			},
			"artifact_refs": {
				Type:        "array",
				Description: "Optional artifact refs to include in packet scope.",
				Items:       &agenttools.Property{Type: "string"},
			},
			"include_all_docs": {
				Type:        "boolean",
				Description: "Whether to include all relevant docs instead of only explicit doc keys.",
				Default:     false,
			},
		},
	}
}

func (t *WorkspaceMemoryPacketShellTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		TaskID         string   `json:"task_id"`
		SessionID      string   `json:"session_id"`
		DocKeys        []string `json:"doc_keys"`
		ArtifactRefs   []string `json:"artifact_refs"`
		IncludeAllDocs bool     `json:"include_all_docs"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_packet_shell: invalid input: %w", err)
	}
	if strings.TrimSpace(params.TaskID) == "" && strings.TrimSpace(params.SessionID) == "" {
		return "", fmt.Errorf("memory_packet_shell: task_id or session_id is required")
	}

	packet, err := t.client.BuildMemoryShellPacket(ctx, WorkspaceMemoryPacketFilter{
		WorkspaceID:    t.workspaceID,
		TaskID:         params.TaskID,
		SessionID:      params.SessionID,
		AgentID:        t.agentID,
		DocKeys:        params.DocKeys,
		ArtifactRefs:   params.ArtifactRefs,
		IncludeAllDocs: params.IncludeAllDocs,
	})
	if err != nil {
		return "", fmt.Errorf("memory_packet_shell: %w", err)
	}

	out, err := json.Marshal(map[string]any{
		"packet":           packet,
		"meta":             packet.Meta,
		"boundary_summary": packet.BoundarySummary,
		"basis_summary":    packet.BasisSummary,
	})
	if err != nil {
		return "", fmt.Errorf("memory_packet_shell: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceMemoryPacketKernelTool struct {
	client      WorkspaceMemoryPacketAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceMemoryPacketKernelTool(client WorkspaceMemoryPacketAwareRhizomeClient, workspaceID string) *WorkspaceMemoryPacketKernelTool {
	return &WorkspaceMemoryPacketKernelTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceMemoryPacketKernelTool) Name() string { return "memory_packet_kernel" }

func (t *WorkspaceMemoryPacketKernelTool) Description() string {
	return "Build a bounded kernel memory packet for the current task or session using the existing canonical packet read-side."
}

func (t *WorkspaceMemoryPacketKernelTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"task_id": {
				Type:        "string",
				Description: "Task id for the kernel packet scope.",
			},
			"session_id": {
				Type:        "string",
				Description: "Optional session id for the kernel packet scope.",
			},
			"doc_keys": {
				Type:        "array",
				Description: "Optional document keys to include in packet scope.",
				Items:       &agenttools.Property{Type: "string"},
			},
			"artifact_refs": {
				Type:        "array",
				Description: "Optional artifact refs to include in packet scope.",
				Items:       &agenttools.Property{Type: "string"},
			},
			"include_all_docs": {
				Type:        "boolean",
				Description: "Whether to include all relevant docs instead of only explicit doc keys.",
				Default:     false,
			},
		},
	}
}

func (t *WorkspaceMemoryPacketKernelTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		TaskID         string   `json:"task_id"`
		SessionID      string   `json:"session_id"`
		DocKeys        []string `json:"doc_keys"`
		ArtifactRefs   []string `json:"artifact_refs"`
		IncludeAllDocs bool     `json:"include_all_docs"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_packet_kernel: invalid input: %w", err)
	}
	if strings.TrimSpace(params.TaskID) == "" && strings.TrimSpace(params.SessionID) == "" {
		return "", fmt.Errorf("memory_packet_kernel: task_id or session_id is required")
	}

	packet, err := t.client.BuildMemoryKernelPacket(ctx, WorkspaceMemoryPacketFilter{
		WorkspaceID:    t.workspaceID,
		TaskID:         params.TaskID,
		SessionID:      params.SessionID,
		DocKeys:        params.DocKeys,
		ArtifactRefs:   params.ArtifactRefs,
		IncludeAllDocs: params.IncludeAllDocs,
	})
	if err != nil {
		return "", fmt.Errorf("memory_packet_kernel: %w", err)
	}

	out, err := json.Marshal(map[string]any{
		"packet":           packet,
		"meta":             packet.Meta,
		"boundary_summary": packet.BoundarySummary,
		"basis_summary":    packet.BasisSummary,
	})
	if err != nil {
		return "", fmt.Errorf("memory_packet_kernel: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceMemoryPromotionReadTool struct {
	client      WorkspaceMemoryPromotionAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceMemoryPromotionReadTool(client WorkspaceMemoryPromotionAwareRhizomeClient, workspaceID string) *WorkspaceMemoryPromotionReadTool {
	return &WorkspaceMemoryPromotionReadTool{client: client, workspaceID: workspaceID}
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
		record, err := t.client.GetMemoryPromotion(ctx, WorkspaceMemoryPromotionFilter{
			WorkspaceID: t.workspaceID,
			PromotionID: params.PromotionID,
		})
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

	items, err := t.client.ListMemoryPromotions(ctx, WorkspaceMemoryPromotionFilter{
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

type WorkspaceMemoryCoherenceReadTool struct {
	client      WorkspaceMemoryCoherenceAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceMemoryCoherenceReadTool(client WorkspaceMemoryCoherenceAwareRhizomeClient, workspaceID string) *WorkspaceMemoryCoherenceReadTool {
	return &WorkspaceMemoryCoherenceReadTool{client: client, workspaceID: workspaceID}
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

	scope, err := t.client.GetMemoryCoherenceScope(ctx, WorkspaceMemoryCoherenceFilter{
		WorkspaceID: t.workspaceID,
		AgentID:     params.AgentID,
		SessionID:   params.SessionID,
		ReportScope: params.ReportScope,
	})
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

type WorkspaceMemoryInvalidationReadTool struct {
	client      WorkspaceMemoryInvalidationAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceMemoryInvalidationReadTool(client WorkspaceMemoryInvalidationAwareRhizomeClient, workspaceID, agentID string) *WorkspaceMemoryInvalidationReadTool {
	return &WorkspaceMemoryInvalidationReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
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
	agentID := firstNonEmptyTrimmed(params.AgentID, t.agentID)
	if agentID == "" {
		return "", fmt.Errorf("memory_invalidation_read: agent_id is required")
	}
	result, err := t.client.ListMemoryInvalidations(ctx, WorkspaceMemoryInvalidationListFilter{
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

type WorkspaceMemoryInvalidationItemReadTool struct {
	client      WorkspaceMemoryInvalidationAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceMemoryInvalidationItemReadTool(client WorkspaceMemoryInvalidationAwareRhizomeClient, workspaceID, agentID string) *WorkspaceMemoryInvalidationItemReadTool {
	return &WorkspaceMemoryInvalidationItemReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
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
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id. Defaults to the current agent when omitted.",
			},
			"invalidation_id": {
				Type:        "string",
				Description: "Invalidation id to fetch.",
			},
		},
		Required: []string{"invalidation_id"},
	}
}

func (t *WorkspaceMemoryInvalidationItemReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		AgentID        string `json:"agent_id"`
		InvalidationID string `json:"invalidation_id"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_invalidation_item_read: invalid input: %w", err)
	}
	agentID := firstNonEmptyTrimmed(params.AgentID, t.agentID)
	if agentID == "" {
		return "", fmt.Errorf("memory_invalidation_item_read: agent_id is required")
	}
	record, err := t.client.GetMemoryInvalidation(ctx, WorkspaceMemoryInvalidationGetFilter{
		WorkspaceID:    t.workspaceID,
		AgentID:        agentID,
		InvalidationID: params.InvalidationID,
	})
	if err != nil {
		return "", fmt.Errorf("memory_invalidation_item_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id": t.workspaceID,
		"agent_id":     agentID,
		"invalidation": record,
	})
	if err != nil {
		return "", fmt.Errorf("memory_invalidation_item_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceMemoryInvalidationCursorReadTool struct {
	client      WorkspaceMemoryInvalidationAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceMemoryInvalidationCursorReadTool(client WorkspaceMemoryInvalidationAwareRhizomeClient, workspaceID, agentID string) *WorkspaceMemoryInvalidationCursorReadTool {
	return &WorkspaceMemoryInvalidationCursorReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceMemoryInvalidationCursorReadTool) Name() string {
	return "memory_invalidation_cursor_read"
}

func (t *WorkspaceMemoryInvalidationCursorReadTool) Description() string {
	return "Read the current canonical memory invalidation cursor for one agent or agent session."
}

func (t *WorkspaceMemoryInvalidationCursorReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id. Defaults to the current agent when omitted.",
			},
			"session_id": {
				Type:        "string",
				Description: "Optional session id to narrow the cursor to one live session.",
			},
		},
	}
}

func (t *WorkspaceMemoryInvalidationCursorReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		AgentID   string `json:"agent_id"`
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_invalidation_cursor_read: invalid input: %w", err)
	}
	agentID := firstNonEmptyTrimmed(params.AgentID, t.agentID)
	if agentID == "" {
		return "", fmt.Errorf("memory_invalidation_cursor_read: agent_id is required")
	}
	cursor, err := t.client.GetMemoryInvalidationCursor(ctx, WorkspaceMemoryInvalidationCursorFilter{
		WorkspaceID: t.workspaceID,
		AgentID:     agentID,
		SessionID:   params.SessionID,
	})
	if err != nil {
		return "", fmt.Errorf("memory_invalidation_cursor_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id": t.workspaceID,
		"agent_id":     agentID,
		"cursor":       cursor,
	})
	if err != nil {
		return "", fmt.Errorf("memory_invalidation_cursor_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceMemoryGraphListReadTool struct {
	client      WorkspaceMemoryGraphAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceMemoryGraphListReadTool(client WorkspaceMemoryGraphAwareRhizomeClient, workspaceID, agentID string) *WorkspaceMemoryGraphListReadTool {
	return &WorkspaceMemoryGraphListReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceMemoryGraphListReadTool) Name() string { return "memory_graph_list_read" }

func (t *WorkspaceMemoryGraphListReadTool) Description() string {
	return "Read the current derived memory-graph node list from the existing compatibility graph read-side."
}

func (t *WorkspaceMemoryGraphListReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"memory_type":      {Type: "string", Description: "Optional derived graph memory type filter."},
			"memory_layer":     {Type: "string", Description: "Optional memory layer filter.", Enum: []string{"EPISODIC", "SEMANTIC", "PROCEDURAL", "IDENTITY", "ARCHIVE"}},
			"visibility":       {Type: "string", Description: "Optional visibility filter.", Enum: []string{"PRIVATE", "COALITION", "CLUSTER", "WORKSPACE"}},
			"epistemic_status": {Type: "string", Description: "Optional epistemic-status filter.", Enum: []string{"ALLEGED", "SUPPORTED", "VERIFIED", "DISPUTED", "RETRACTED"}},
			"lifecycle_state":  {Type: "string", Description: "Optional lifecycle-state filter.", Enum: []string{"ACTIVE", "DORMANT", "SUPERSEDED", "ARCHIVED"}},
			"origin_kind":      {Type: "string", Description: "Optional origin kind filter."},
			"origin_id":        {Type: "string", Description: "Optional origin id filter."},
			"source_kind":      {Type: "string", Description: "Optional source kind filter."},
			"agent_id":         {Type: "string", Description: "Optional agent id filter. Defaults to the current agent when omitted."},
			"session_id":       {Type: "string", Description: "Optional session id filter."},
			"task_id":          {Type: "string", Description: "Optional task id filter."},
			"include_archived": {Type: "boolean", Description: "Whether archived graph nodes should also be returned."},
			"limit":            {Type: "integer", Description: "Maximum number of graph nodes to return.", Default: 50},
		},
	}
}

func (t *WorkspaceMemoryGraphListReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		MemoryType      string `json:"memory_type"`
		MemoryLayer     string `json:"memory_layer"`
		Visibility      string `json:"visibility"`
		EpistemicStatus string `json:"epistemic_status"`
		LifecycleState  string `json:"lifecycle_state"`
		OriginKind      string `json:"origin_kind"`
		OriginID        string `json:"origin_id"`
		SourceKind      string `json:"source_kind"`
		AgentID         string `json:"agent_id"`
		SessionID       string `json:"session_id"`
		TaskID          string `json:"task_id"`
		IncludeArchived bool   `json:"include_archived"`
		Limit           int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_graph_list_read: invalid input: %w", err)
	}
	result, err := t.client.ListMemoryGraphNodes(ctx, WorkspaceMemoryGraphListFilter{
		WorkspaceID:     t.workspaceID,
		MemoryType:      params.MemoryType,
		MemoryLayer:     params.MemoryLayer,
		Visibility:      params.Visibility,
		EpistemicStatus: params.EpistemicStatus,
		LifecycleState:  params.LifecycleState,
		OriginKind:      params.OriginKind,
		OriginID:        params.OriginID,
		SourceKind:      params.SourceKind,
		AgentID:         firstNonEmptyTrimmed(params.AgentID, t.agentID),
		SessionID:       params.SessionID,
		TaskID:          params.TaskID,
		IncludeArchived: params.IncludeArchived,
		Limit:           params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("memory_graph_list_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":      result.WorkspaceID,
		"time_authority":    result.TimeAuthority,
		"boundary_contract": result.BoundaryContract,
		"count":             result.Count,
		"items":             result.Items,
	})
	if err != nil {
		return "", fmt.Errorf("memory_graph_list_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceMemoryGraphGetReadTool struct {
	client      WorkspaceMemoryGraphAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceMemoryGraphGetReadTool(client WorkspaceMemoryGraphAwareRhizomeClient, workspaceID string) *WorkspaceMemoryGraphGetReadTool {
	return &WorkspaceMemoryGraphGetReadTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceMemoryGraphGetReadTool) Name() string { return "memory_graph_get_read" }

func (t *WorkspaceMemoryGraphGetReadTool) Description() string {
	return "Read one derived memory-graph node detail from the existing compatibility graph read-side."
}

func (t *WorkspaceMemoryGraphGetReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"memory_id": {
				Type:        "string",
				Description: "Derived graph memory id to inspect through the existing memory-graph detail path.",
			},
		},
		Required: []string{"memory_id"},
	}
}

func (t *WorkspaceMemoryGraphGetReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		MemoryID string `json:"memory_id"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_graph_get_read: invalid input: %w", err)
	}
	detail, err := t.client.GetMemoryGraphNode(ctx, WorkspaceMemoryGraphGetFilter{
		WorkspaceID: t.workspaceID,
		MemoryID:    params.MemoryID,
	})
	if err != nil {
		return "", fmt.Errorf("memory_graph_get_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":      t.workspaceID,
		"memory_id":         strings.TrimSpace(params.MemoryID),
		"detail":            detail,
		"time_authority":    detail.TimeAuthority,
		"boundary_contract": detail.BoundaryContract,
		"node":              detail.Node,
		"refs":              detail.Refs,
		"versions":          detail.Versions,
		"drift_report":      detail.DriftReport,
		"metrics":           detail.Metrics,
		"outbound_edges":    detail.OutboundEdges,
		"inbound_edges":     detail.InboundEdges,
	})
	if err != nil {
		return "", fmt.Errorf("memory_graph_get_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceMemoryNodeSearchReadTool struct {
	client      WorkspaceMemoryNodeSearchAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceMemoryNodeSearchReadTool(client WorkspaceMemoryNodeSearchAwareRhizomeClient, workspaceID, agentID string) *WorkspaceMemoryNodeSearchReadTool {
	return &WorkspaceMemoryNodeSearchReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceMemoryNodeSearchReadTool) Name() string { return "memory_node_search_read" }

func (t *WorkspaceMemoryNodeSearchReadTool) Description() string {
	return "Search derived memory-graph nodes through the existing bounded compatibility node-search read-side."
}

func (t *WorkspaceMemoryNodeSearchReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"query":            {Type: "string", Description: "Lexical query matched against title, summary, body, and claim fields."},
			"memory_type":      {Type: "string", Description: "Optional derived graph memory type filter."},
			"memory_layer":     {Type: "string", Description: "Optional memory layer filter.", Enum: []string{"EPISODIC", "SEMANTIC", "PROCEDURAL", "IDENTITY", "ARCHIVE"}},
			"visibility":       {Type: "string", Description: "Optional visibility filter.", Enum: []string{"PRIVATE", "COALITION", "CLUSTER", "WORKSPACE"}},
			"epistemic_status": {Type: "string", Description: "Optional epistemic-status filter.", Enum: []string{"ALLEGED", "SUPPORTED", "VERIFIED", "DISPUTED", "RETRACTED"}},
			"lifecycle_state":  {Type: "string", Description: "Optional lifecycle-state filter.", Enum: []string{"ACTIVE", "DORMANT", "SUPERSEDED", "ARCHIVED"}},
			"origin_kind":      {Type: "string", Description: "Optional origin kind filter."},
			"origin_id":        {Type: "string", Description: "Optional origin id filter."},
			"source_kind":      {Type: "string", Description: "Optional source kind filter."},
			"agent_id":         {Type: "string", Description: "Optional agent id filter. Defaults to the current agent when omitted."},
			"session_id":       {Type: "string", Description: "Optional session id filter."},
			"task_id":          {Type: "string", Description: "Optional task id filter."},
			"include_archived": {Type: "boolean", Description: "Whether archived nodes should also be included in the bounded search."},
			"limit":            {Type: "integer", Description: "Maximum number of search hits to return.", Default: 20},
		},
		Required: []string{"query"},
	}
}

func (t *WorkspaceMemoryNodeSearchReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Query           string `json:"query"`
		MemoryType      string `json:"memory_type"`
		MemoryLayer     string `json:"memory_layer"`
		Visibility      string `json:"visibility"`
		EpistemicStatus string `json:"epistemic_status"`
		LifecycleState  string `json:"lifecycle_state"`
		OriginKind      string `json:"origin_kind"`
		OriginID        string `json:"origin_id"`
		SourceKind      string `json:"source_kind"`
		AgentID         string `json:"agent_id"`
		SessionID       string `json:"session_id"`
		TaskID          string `json:"task_id"`
		IncludeArchived bool   `json:"include_archived"`
		Limit           int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_node_search_read: invalid input: %w", err)
	}
	if strings.TrimSpace(params.Query) == "" {
		return "", fmt.Errorf("memory_node_search_read: query is required")
	}
	result, err := t.client.SearchMemoryNodes(ctx, WorkspaceMemoryNodeSearchFilter{
		WorkspaceID:     t.workspaceID,
		Query:           params.Query,
		MemoryType:      params.MemoryType,
		MemoryLayer:     params.MemoryLayer,
		Visibility:      params.Visibility,
		EpistemicStatus: params.EpistemicStatus,
		LifecycleState:  params.LifecycleState,
		OriginKind:      params.OriginKind,
		OriginID:        params.OriginID,
		SourceKind:      params.SourceKind,
		AgentID:         firstNonEmptyTrimmed(params.AgentID, t.agentID),
		SessionID:       params.SessionID,
		TaskID:          params.TaskID,
		IncludeArchived: params.IncludeArchived,
		Limit:           params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("memory_node_search_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":      result.WorkspaceID,
		"time_authority":    result.TimeAuthority,
		"boundary_contract": result.BoundaryContract,
		"query":             result.Query,
		"generated_at":      result.GeneratedAt,
		"count":             result.Count,
		"hits":              result.Hits,
		"result":            result,
	})
	if err != nil {
		return "", fmt.Errorf("memory_node_search_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceTensionListReadTool struct {
	client      WorkspaceTensionAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceTensionListReadTool(client WorkspaceTensionAwareRhizomeClient, workspaceID, agentID string) *WorkspaceTensionListReadTool {
	return &WorkspaceTensionListReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceTensionListReadTool) Name() string { return "tension_list_read" }

func (t *WorkspaceTensionListReadTool) Description() string {
	return "Read the current canonical tension list from the existing persisted tension read-side."
}

func (t *WorkspaceTensionListReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"tension_type":    {Type: "string", Description: "Optional tension type filter."},
			"lifecycle_state": {Type: "string", Description: "Optional lifecycle-state filter."},
			"review_status":   {Type: "string", Description: "Optional review-status filter."},
			"proto_cluster_id": {
				Type:        "string",
				Description: "Optional proto-cluster id filter.",
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task id filter.",
			},
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id filter. Defaults to the current agent when omitted.",
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of tensions to return.",
				Default:     20,
			},
		},
	}
}

func (t *WorkspaceTensionListReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		TensionType    string `json:"tension_type"`
		LifecycleState string `json:"lifecycle_state"`
		ReviewStatus   string `json:"review_status"`
		ProtoClusterID string `json:"proto_cluster_id"`
		TaskID         string `json:"task_id"`
		AgentID        string `json:"agent_id"`
		Limit          int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("tension_list_read: invalid input: %w", err)
	}
	result, err := t.client.ListTensions(ctx, WorkspaceTensionFilter{
		WorkspaceID:    t.workspaceID,
		TensionType:    params.TensionType,
		LifecycleState: params.LifecycleState,
		ReviewStatus:   params.ReviewStatus,
		ProtoClusterID: params.ProtoClusterID,
		TaskID:         params.TaskID,
		AgentID:        firstNonEmptyTrimmed(params.AgentID, t.agentID),
		Limit:          params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("tension_list_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":   result.WorkspaceID,
		"time_authority": result.TimeAuthority,
		"count":          result.Count,
		"items":          result.Items,
	})
	if err != nil {
		return "", fmt.Errorf("tension_list_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceTensionGetReadTool struct {
	client      WorkspaceTensionAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceTensionGetReadTool(client WorkspaceTensionAwareRhizomeClient, workspaceID string) *WorkspaceTensionGetReadTool {
	return &WorkspaceTensionGetReadTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceTensionGetReadTool) Name() string { return "tension_get_read" }

func (t *WorkspaceTensionGetReadTool) Description() string {
	return "Read one canonical tension detail from the existing persisted tension read-side."
}

func (t *WorkspaceTensionGetReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"tension_id": {
				Type:        "string",
				Description: "Canonical tension id to inspect through the existing tension detail path.",
			},
		},
		Required: []string{"tension_id"},
	}
}

func (t *WorkspaceTensionGetReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		TensionID string `json:"tension_id"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("tension_get_read: invalid input: %w", err)
	}
	if strings.TrimSpace(params.TensionID) == "" {
		return "", fmt.Errorf("tension_get_read: tension_id is required")
	}
	detail, err := t.client.GetTension(ctx, WorkspaceTensionGetFilter{
		WorkspaceID: t.workspaceID,
		TensionID:   params.TensionID,
	})
	if err != nil {
		return "", fmt.Errorf("tension_get_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":   t.workspaceID,
		"tension_id":     strings.TrimSpace(params.TensionID),
		"detail":         detail,
		"time_authority": detail.TimeAuthority,
		"tension":        detail.Tension,
		"dependencies":   detail.Dependencies,
		"dependents":     detail.Dependents,
		"evidence":       detail.Evidence,
		"events":         detail.Events,
		"claims":         detail.Claims,
		"queues":         detail.Queues,
		"docs":           detail.Docs,
		"artifacts":      detail.Artifacts,
		"proto_cluster":  detail.ProtoCluster,
	})
	if err != nil {
		return "", fmt.Errorf("tension_get_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceTensionFrontierReadTool struct {
	client      WorkspaceTensionAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceTensionFrontierReadTool(client WorkspaceTensionAwareRhizomeClient, workspaceID, agentID string) *WorkspaceTensionFrontierReadTool {
	return &WorkspaceTensionFrontierReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceTensionFrontierReadTool) Name() string { return "tension_frontier_read" }

func (t *WorkspaceTensionFrontierReadTool) Description() string {
	return "Read the current surfaced tension frontier from the existing canonical frontier read-side."
}

func (t *WorkspaceTensionFrontierReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"tension_type":    {Type: "string", Description: "Optional tension type filter."},
			"lifecycle_state": {Type: "string", Description: "Optional lifecycle-state filter."},
			"review_status":   {Type: "string", Description: "Optional review-status filter."},
			"proto_cluster_id": {
				Type:        "string",
				Description: "Optional proto-cluster id filter.",
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task id filter.",
			},
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id filter. Defaults to the current agent when omitted.",
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of frontier tensions to return.",
				Default:     20,
			},
		},
	}
}

func (t *WorkspaceTensionFrontierReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		TensionType    string `json:"tension_type"`
		LifecycleState string `json:"lifecycle_state"`
		ReviewStatus   string `json:"review_status"`
		ProtoClusterID string `json:"proto_cluster_id"`
		TaskID         string `json:"task_id"`
		AgentID        string `json:"agent_id"`
		Limit          int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("tension_frontier_read: invalid input: %w", err)
	}
	result, err := t.client.ListTensionFrontier(ctx, WorkspaceTensionFilter{
		WorkspaceID:    t.workspaceID,
		TensionType:    params.TensionType,
		LifecycleState: params.LifecycleState,
		ReviewStatus:   params.ReviewStatus,
		ProtoClusterID: params.ProtoClusterID,
		TaskID:         params.TaskID,
		AgentID:        firstNonEmptyTrimmed(params.AgentID, t.agentID),
		Limit:          params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("tension_frontier_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":   result.WorkspaceID,
		"time_authority": result.TimeAuthority,
		"count":          result.Count,
		"items":          result.Items,
	})
	if err != nil {
		return "", fmt.Errorf("tension_frontier_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceTensionAttachableReadTool struct {
	client      WorkspaceTensionAttachableAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceTensionAttachableReadTool(client WorkspaceTensionAttachableAwareRhizomeClient, workspaceID, agentID string) *WorkspaceTensionAttachableReadTool {
	return &WorkspaceTensionAttachableReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceTensionAttachableReadTool) Name() string { return "tension_attachable_read" }

func (t *WorkspaceTensionAttachableReadTool) Description() string {
	return "Read the current attachable tension shortlist from the existing canonical attachability read-side."
}

func (t *WorkspaceTensionAttachableReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id. Defaults to the current agent when omitted.",
			},
		},
	}
}

func (t *WorkspaceTensionAttachableReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		AgentID string `json:"agent_id"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("tension_attachable_read: invalid input: %w", err)
	}
	agentID := firstNonEmptyTrimmed(params.AgentID, t.agentID)
	if agentID == "" {
		return "", fmt.Errorf("tension_attachable_read: agent_id is required")
	}
	result, err := t.client.ListAttachableTensions(ctx, WorkspaceTensionAttachableFilter{
		WorkspaceID: t.workspaceID,
		AgentID:     agentID,
	})
	if err != nil {
		return "", fmt.Errorf("tension_attachable_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id": result.WorkspaceID,
		"agent_id":     result.AgentID,
		"count":        result.Count,
		"items":        result.Items,
	})
	if err != nil {
		return "", fmt.Errorf("tension_attachable_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceRSPStateReadTool struct {
	client      WorkspaceRSPStateAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceRSPStateReadTool(client WorkspaceRSPStateAwareRhizomeClient, workspaceID, agentID string) *WorkspaceRSPStateReadTool {
	return &WorkspaceRSPStateReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceRSPStateReadTool) Name() string { return "rsp_state_read" }

func (t *WorkspaceRSPStateReadTool) Description() string {
	return "Read the current inspectability-only RSP state report for the current agent, task, session, or cluster from the existing canonical read-side."
}

func (t *WorkspaceRSPStateReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"proto_cluster_id": {
				Type:        "string",
				Description: "Optional proto-cluster id to resolve the state report against one cluster.",
			},
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id. Defaults to the current agent when omitted.",
			},
			"session_id": {
				Type:        "string",
				Description: "Optional session id to narrow the state report.",
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task id to narrow the state report.",
			},
			"doc_keys": {
				Type:        "array",
				Description: "Optional document keys to include in the report locus.",
				Items:       &agenttools.Property{Type: "string"},
			},
			"artifact_refs": {
				Type:        "array",
				Description: "Optional artifact refs to include in the report locus.",
				Items:       &agenttools.Property{Type: "string"},
			},
			"frontier_limit": {
				Type:        "integer",
				Description: "Optional frontier limit for the underlying report locus.",
				Default:     3,
			},
		},
	}
}

func (t *WorkspaceRSPStateReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		ProtoClusterID string   `json:"proto_cluster_id"`
		AgentID        string   `json:"agent_id"`
		SessionID      string   `json:"session_id"`
		TaskID         string   `json:"task_id"`
		DocKeys        []string `json:"doc_keys"`
		ArtifactRefs   []string `json:"artifact_refs"`
		FrontierLimit  int      `json:"frontier_limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("rsp_state_read: invalid input: %w", err)
	}

	report, err := t.client.GetRSPStateReport(ctx, WorkspaceRSPStateFilter{
		WorkspaceID:    t.workspaceID,
		ProtoClusterID: params.ProtoClusterID,
		AgentID:        firstNonEmptyTrimmed(params.AgentID, t.agentID),
		SessionID:      params.SessionID,
		TaskID:         params.TaskID,
		DocKeys:        params.DocKeys,
		ArtifactRefs:   params.ArtifactRefs,
		FrontierLimit:  params.FrontierLimit,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_state_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":                t.workspaceID,
		"report":                      report,
		"time_authority":              report.TimeAuthority,
		"summary":                     report.Summary,
		"governed_hint_summary":       report.GovernedHintSummary,
		"governed_hints":              report.GovernedHints,
		"local_autonomics_candidates": report.LocalAutonomicsCandidates,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_state_read: marshal result: %w", err)
	}
	return string(out), nil
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

type WorkspaceRSPForecastReadTool struct {
	client      WorkspaceRSPForecastAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceRSPForecastReadTool(client WorkspaceRSPForecastAwareRhizomeClient, workspaceID, agentID string) *WorkspaceRSPForecastReadTool {
	return &WorkspaceRSPForecastReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceRSPForecastReadTool) Name() string { return "rsp_forecast_read" }

func (t *WorkspaceRSPForecastReadTool) Description() string {
	return "Read the current inspectability-only RSP forecast report for the current agent, task, session, or cluster from the existing canonical read-side."
}

func (t *WorkspaceRSPForecastReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"proto_cluster_id": {
				Type:        "string",
				Description: "Optional proto-cluster id to resolve the forecast report against one cluster.",
			},
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id. Defaults to the current agent when omitted.",
			},
			"session_id": {
				Type:        "string",
				Description: "Optional session id to narrow the forecast report.",
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task id to narrow the forecast report.",
			},
			"doc_keys": {
				Type:        "array",
				Description: "Optional document keys to include in the forecast locus.",
				Items:       &agenttools.Property{Type: "string"},
			},
			"artifact_refs": {
				Type:        "array",
				Description: "Optional artifact refs to include in the forecast locus.",
				Items:       &agenttools.Property{Type: "string"},
			},
			"frontier_limit": {
				Type:        "integer",
				Description: "Optional frontier limit for the underlying forecast locus.",
				Default:     3,
			},
		},
	}
}

func (t *WorkspaceRSPForecastReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		ProtoClusterID string   `json:"proto_cluster_id"`
		AgentID        string   `json:"agent_id"`
		SessionID      string   `json:"session_id"`
		TaskID         string   `json:"task_id"`
		DocKeys        []string `json:"doc_keys"`
		ArtifactRefs   []string `json:"artifact_refs"`
		FrontierLimit  int      `json:"frontier_limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("rsp_forecast_read: invalid input: %w", err)
	}

	report, err := t.client.GetRSPForecastReport(ctx, WorkspaceRSPForecastFilter{
		WorkspaceID:    t.workspaceID,
		ProtoClusterID: params.ProtoClusterID,
		AgentID:        firstNonEmptyTrimmed(params.AgentID, t.agentID),
		SessionID:      params.SessionID,
		TaskID:         params.TaskID,
		DocKeys:        params.DocKeys,
		ArtifactRefs:   params.ArtifactRefs,
		FrontierLimit:  params.FrontierLimit,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_forecast_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":              t.workspaceID,
		"report":                    report,
		"time_authority":            report.TimeAuthority,
		"summary":                   report.Summary,
		"forecast_readiness":        report.ForecastReadiness,
		"forecast_provenance_hints": report.ForecastProvenanceHints,
		"forecast_coverage_summary": report.ForecastCoverageSummary,
		"projections":               report.Projections,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_forecast_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceRSPBeliefReadTool struct {
	client      WorkspaceRSPBeliefAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceRSPBeliefReadTool(client WorkspaceRSPBeliefAwareRhizomeClient, workspaceID, agentID string) *WorkspaceRSPBeliefReadTool {
	return &WorkspaceRSPBeliefReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceRSPBeliefReadTool) Name() string { return "rsp_belief_read" }

func (t *WorkspaceRSPBeliefReadTool) Description() string {
	return "Read the current inspectability-only RSP belief report for the current agent, task, or session from the existing canonical read-side."
}

func (t *WorkspaceRSPBeliefReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"claim_type": {
				Type:        "string",
				Description: "Optional claim type filter for the belief report.",
			},
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id. Defaults to the current agent when omitted.",
			},
			"session_id": {
				Type:        "string",
				Description: "Optional session id to narrow the belief report.",
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task id to narrow the belief report.",
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of belief items to return.",
				Default:     20,
			},
		},
	}
}

func (t *WorkspaceRSPBeliefReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		ClaimType string `json:"claim_type"`
		AgentID   string `json:"agent_id"`
		SessionID string `json:"session_id"`
		TaskID    string `json:"task_id"`
		Limit     int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("rsp_belief_read: invalid input: %w", err)
	}

	report, err := t.client.GetRSPBeliefReport(ctx, WorkspaceRSPBeliefFilter{
		WorkspaceID: t.workspaceID,
		ClaimType:   params.ClaimType,
		AgentID:     firstNonEmptyTrimmed(params.AgentID, t.agentID),
		SessionID:   params.SessionID,
		TaskID:      params.TaskID,
		Limit:       params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_belief_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":             t.workspaceID,
		"report":                   report,
		"time_authority":           report.TimeAuthority,
		"summary":                  report.Summary,
		"count":                    report.Count,
		"items":                    report.Items,
		"low_independence_count":   report.LowIndependenceCount,
		"high_contradiction_count": report.HighContradictionCount,
		"verifier_stale_count":     report.VerifierStaleCount,
		"high_uncertainty_count":   report.HighUncertaintyCount,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_belief_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceRSPBeliefClaimReadTool struct {
	client      WorkspaceRSPBeliefAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceRSPBeliefClaimReadTool(client WorkspaceRSPBeliefAwareRhizomeClient, workspaceID string) *WorkspaceRSPBeliefClaimReadTool {
	return &WorkspaceRSPBeliefClaimReadTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceRSPBeliefClaimReadTool) Name() string { return "rsp_belief_claim_read" }

func (t *WorkspaceRSPBeliefClaimReadTool) Description() string {
	return "Read one canonical shadow-only RSP belief item for a claim from the existing read-side."
}

func (t *WorkspaceRSPBeliefClaimReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"claim_id": {
				Type:        "string",
				Description: "Claim id of the belief item to read.",
			},
		},
		Required: []string{"claim_id"},
	}
}

func (t *WorkspaceRSPBeliefClaimReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		ClaimID string `json:"claim_id"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("rsp_belief_claim_read: invalid input: %w", err)
	}

	item, err := t.client.GetRSPBeliefClaim(ctx, WorkspaceRSPBeliefClaimFilter{
		WorkspaceID: t.workspaceID,
		ClaimID:     params.ClaimID,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_belief_claim_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":                t.workspaceID,
		"claim_id":                    item.ClaimID,
		"item":                        item,
		"time_authority":              item.TimeAuthority,
		"summary":                     item.Summary,
		"status":                      item.Status,
		"suggested_state":             item.SuggestedState,
		"source_diversity":            item.SourceDiversity,
		"independence_discount":       item.IndependenceDiscount,
		"correlated_evidence_count":   item.CorrelatedEvidence,
		"independent_evidence_groups": item.IndependentGroups,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_belief_claim_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceRSPTelemetryReadTool struct {
	client      WorkspaceRSPTelemetryAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceRSPTelemetryReadTool(client WorkspaceRSPTelemetryAwareRhizomeClient, workspaceID string) *WorkspaceRSPTelemetryReadTool {
	return &WorkspaceRSPTelemetryReadTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceRSPTelemetryReadTool) Name() string { return "rsp_telemetry_read" }

func (t *WorkspaceRSPTelemetryReadTool) Description() string {
	return "Read the current inspectability-only RSP telemetry dump from the existing canonical read-side."
}

func (t *WorkspaceRSPTelemetryReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"limit": {
				Type:        "integer",
				Description: "Maximum number of telemetry rows to return per stream.",
				Default:     20,
			},
		},
	}
}

func (t *WorkspaceRSPTelemetryReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Limit int `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("rsp_telemetry_read: invalid input: %w", err)
	}

	dump, err := t.client.GetRSPTelemetryDump(ctx, WorkspaceRSPTelemetryFilter{
		WorkspaceID: t.workspaceID,
		Limit:       params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_telemetry_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":              t.workspaceID,
		"dump":                      dump,
		"summary":                   dump.Summary,
		"readiness_coverage_rollup": dump.Summary.ReadinessCoverageRollup,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_telemetry_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceUnifiedControlReadTool struct {
	client      WorkspaceUnifiedControlAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceUnifiedControlReadTool(client WorkspaceUnifiedControlAwareRhizomeClient, workspaceID, agentID string) *WorkspaceUnifiedControlReadTool {
	return &WorkspaceUnifiedControlReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceUnifiedControlReadTool) Name() string { return "unified_control_read" }

func (t *WorkspaceUnifiedControlReadTool) Description() string {
	return "Read the current inspectability-only unified control report from the existing canonical read-side."
}

func (t *WorkspaceUnifiedControlReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"proto_cluster_id": {
				Type:        "string",
				Description: "Optional proto-cluster id to resolve the unified control report against one cluster.",
			},
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id. Defaults to the current agent when omitted.",
			},
			"session_id": {
				Type:        "string",
				Description: "Optional session id to narrow the unified control report.",
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task id to narrow the unified control report.",
			},
			"doc_keys": {
				Type:        "array",
				Description: "Optional document keys to include in the control locus.",
				Items:       &agenttools.Property{Type: "string"},
			},
			"artifact_refs": {
				Type:        "array",
				Description: "Optional artifact refs to include in the control locus.",
				Items:       &agenttools.Property{Type: "string"},
			},
			"frontier_limit": {
				Type:        "integer",
				Description: "Optional frontier limit for the underlying control locus.",
				Default:     3,
			},
		},
	}
}

func (t *WorkspaceUnifiedControlReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		ProtoClusterID string   `json:"proto_cluster_id"`
		AgentID        string   `json:"agent_id"`
		SessionID      string   `json:"session_id"`
		TaskID         string   `json:"task_id"`
		DocKeys        []string `json:"doc_keys"`
		ArtifactRefs   []string `json:"artifact_refs"`
		FrontierLimit  int      `json:"frontier_limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("unified_control_read: invalid input: %w", err)
	}

	report, err := t.client.GetUnifiedControlReport(ctx, WorkspaceUnifiedControlFilter{
		WorkspaceID:    t.workspaceID,
		ProtoClusterID: params.ProtoClusterID,
		AgentID:        firstNonEmptyTrimmed(params.AgentID, t.agentID),
		SessionID:      params.SessionID,
		TaskID:         params.TaskID,
		DocKeys:        params.DocKeys,
		ArtifactRefs:   params.ArtifactRefs,
		FrontierLimit:  params.FrontierLimit,
	})
	if err != nil {
		return "", fmt.Errorf("unified_control_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":                    t.workspaceID,
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
	})
	if err != nil {
		return "", fmt.Errorf("unified_control_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceControlReportReadTool struct {
	client      WorkspaceControlReportAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceControlReportReadTool(client WorkspaceControlReportAwareRhizomeClient, workspaceID string) *WorkspaceControlReportReadTool {
	return &WorkspaceControlReportReadTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceControlReportReadTool) Name() string { return "control_report_read" }

func (t *WorkspaceControlReportReadTool) Description() string {
	return "Read the current inspectability-only control advisory report from the existing canonical read-side."
}

func (t *WorkspaceControlReportReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"proto_cluster_id": {
				Type:        "string",
				Description: "Optional proto-cluster id to narrow the control report to one cluster.",
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of clusters to return.",
				Default:     20,
			},
		},
	}
}

func (t *WorkspaceControlReportReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		ProtoClusterID string `json:"proto_cluster_id"`
		Limit          int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("control_report_read: invalid input: %w", err)
	}

	report, err := t.client.GetControlReport(ctx, WorkspaceControlReportFilter{
		WorkspaceID:    t.workspaceID,
		ProtoClusterID: params.ProtoClusterID,
		Limit:          params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("control_report_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":   t.workspaceID,
		"report":         report,
		"time_authority": report.TimeAuthority,
		"workspace":      report.Workspace,
		"clusters":       report.Clusters,
	})
	if err != nil {
		return "", fmt.Errorf("control_report_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceControlClusterReadTool struct {
	client      WorkspaceControlClusterAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceControlClusterReadTool(client WorkspaceControlClusterAwareRhizomeClient, workspaceID string) *WorkspaceControlClusterReadTool {
	return &WorkspaceControlClusterReadTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceControlClusterReadTool) Name() string { return "control_cluster_read" }

func (t *WorkspaceControlClusterReadTool) Description() string {
	return "Read the current inspectability-only control cluster detail from the existing canonical read-side."
}

func (t *WorkspaceControlClusterReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"proto_cluster_id": {
				Type:        "string",
				Description: "Proto-cluster id to inspect.",
			},
		},
		Required: []string{"proto_cluster_id"},
	}
}

func (t *WorkspaceControlClusterReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		ProtoClusterID string `json:"proto_cluster_id"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("control_cluster_read: invalid input: %w", err)
	}

	detail, err := t.client.GetControlClusterDetail(ctx, WorkspaceControlClusterFilter{
		WorkspaceID:    t.workspaceID,
		ProtoClusterID: params.ProtoClusterID,
	})
	if err != nil {
		return "", fmt.Errorf("control_cluster_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":     t.workspaceID,
		"proto_cluster_id": strings.TrimSpace(params.ProtoClusterID),
		"detail":           detail,
		"time_authority":   detail.TimeAuthority,
		"cluster":          detail.Cluster,
		"tensions":         detail.Tensions,
	})
	if err != nil {
		return "", fmt.Errorf("control_cluster_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceControlStateReadTool struct {
	client      WorkspaceControlStateAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceControlStateReadTool(client WorkspaceControlStateAwareRhizomeClient, workspaceID string) *WorkspaceControlStateReadTool {
	return &WorkspaceControlStateReadTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceControlStateReadTool) Name() string { return "control_state_read" }

func (t *WorkspaceControlStateReadTool) Description() string {
	return "Read the current inspectability-only cluster control state report from the existing canonical read-side."
}

func (t *WorkspaceControlStateReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"proto_cluster_id": {
				Type:        "string",
				Description: "Optional proto-cluster id to narrow the control-state report to one cluster.",
			},
			"mode": {
				Type:        "string",
				Description: "Optional control-state mode filter.",
				Enum:        []string{"STEADY", "ANTI_COLLAPSE", "COHERENCE", "DECENTRALIZE", "SYNERGY_SEEKING", "UNFREEZE", "STABILIZE"},
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of clusters to return.",
				Default:     20,
			},
		},
	}
}

func (t *WorkspaceControlStateReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		ProtoClusterID string `json:"proto_cluster_id"`
		Mode           string `json:"mode"`
		Limit          int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("control_state_read: invalid input: %w", err)
	}

	report, err := t.client.GetControlStateReport(ctx, WorkspaceControlStateFilter{
		WorkspaceID:    t.workspaceID,
		ProtoClusterID: params.ProtoClusterID,
		Mode:           params.Mode,
		Limit:          params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("control_state_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":   t.workspaceID,
		"report":         report,
		"time_authority": report.TimeAuthority,
		"workspace":      report.Workspace,
		"clusters":       report.Clusters,
	})
	if err != nil {
		return "", fmt.Errorf("control_state_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceControlStateClusterReadTool struct {
	client      WorkspaceControlStateClusterAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceControlStateClusterReadTool(client WorkspaceControlStateClusterAwareRhizomeClient, workspaceID string) *WorkspaceControlStateClusterReadTool {
	return &WorkspaceControlStateClusterReadTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceControlStateClusterReadTool) Name() string { return "control_state_cluster_read" }

func (t *WorkspaceControlStateClusterReadTool) Description() string {
	return "Read the current inspectability-only control-state cluster detail from the existing canonical read-side."
}

func (t *WorkspaceControlStateClusterReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"proto_cluster_id": {
				Type:        "string",
				Description: "Proto-cluster id to inspect.",
			},
		},
		Required: []string{"proto_cluster_id"},
	}
}

func (t *WorkspaceControlStateClusterReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		ProtoClusterID string `json:"proto_cluster_id"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("control_state_cluster_read: invalid input: %w", err)
	}

	detail, err := t.client.GetControlStateClusterDetail(ctx, WorkspaceControlStateClusterFilter{
		WorkspaceID:    t.workspaceID,
		ProtoClusterID: params.ProtoClusterID,
	})
	if err != nil {
		return "", fmt.Errorf("control_state_cluster_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":     t.workspaceID,
		"proto_cluster_id": strings.TrimSpace(params.ProtoClusterID),
		"time_authority":   detail.TimeAuthority,
		"cluster_basis":    detail.Cluster,
		"state":            detail.State,
		"tensions":         detail.Tensions,
	})
	if err != nil {
		return "", fmt.Errorf("control_state_cluster_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceRSPCapabilityReadTool struct {
	client      WorkspaceRSPCapabilityAwareRhizomeClient
	workspaceID string
}

func NewWorkspaceRSPCapabilityReadTool(client WorkspaceRSPCapabilityAwareRhizomeClient, workspaceID string) *WorkspaceRSPCapabilityReadTool {
	return &WorkspaceRSPCapabilityReadTool{client: client, workspaceID: workspaceID}
}

func (t *WorkspaceRSPCapabilityReadTool) Name() string { return "rsp_capability_read" }

func (t *WorkspaceRSPCapabilityReadTool) Description() string {
	return "Read the current bounded RSP rollout capability flags from the existing canonical read-side."
}

func (t *WorkspaceRSPCapabilityReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type:       "object",
		Properties: map[string]agenttools.Property{},
	}
}

func (t *WorkspaceRSPCapabilityReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	if len(strings.TrimSpace(string(input))) > 0 && strings.TrimSpace(string(input)) != "{}" {
		var ignored map[string]any
		if err := json.Unmarshal(input, &ignored); err != nil {
			return "", fmt.Errorf("rsp_capability_read: invalid input: %w", err)
		}
	}

	flags, err := t.client.GetRSPCapabilityFlags(ctx, WorkspaceRSPCapabilityFilter{
		WorkspaceID: t.workspaceID,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_capability_read: %w", err)
	}
	out, err := json.Marshal(map[string]any{
		"workspace_id":     t.workspaceID,
		"capability_flags": flags,
	})
	if err != nil {
		return "", fmt.Errorf("rsp_capability_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceCompactionCandidatesReadTool struct {
	client      CompactionAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceCompactionCandidatesReadTool(client CompactionAwareRhizomeClient, workspaceID, agentID string) *WorkspaceCompactionCandidatesReadTool {
	return &WorkspaceCompactionCandidatesReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceCompactionCandidatesReadTool) Name() string { return "compaction_candidates_read" }

func (t *WorkspaceCompactionCandidatesReadTool) Description() string {
	return "Read the current active session-ledger compaction candidates from the existing canonical read-side."
}

func (t *WorkspaceCompactionCandidatesReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id whose active session compaction candidates should be read. Defaults to the current agent.",
			},
			"min_messages": {
				Type:        "integer",
				Description: "Minimum message threshold for surfacing canonical compaction candidates.",
				Default:     model.DefaultSessionCompactionMinMessages,
			},
			"min_tokens": {
				Type:        "integer",
				Description: "Minimum token threshold for surfacing canonical compaction candidates.",
				Default:     model.DefaultSessionCompactionMinTokens,
			},
		},
	}
}

func (t *WorkspaceCompactionCandidatesReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		AgentID     string `json:"agent_id"`
		MinMessages int    `json:"min_messages"`
		MinTokens   int    `json:"min_tokens"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("compaction_candidates_read: invalid input: %w", err)
	}

	agentID := strings.TrimSpace(params.AgentID)
	if agentID == "" {
		agentID = strings.TrimSpace(t.agentID)
	}
	minMessages := params.MinMessages
	if minMessages <= 0 {
		minMessages = model.DefaultSessionCompactionMinMessages
	}
	minTokens := params.MinTokens
	if minTokens <= 0 {
		minTokens = model.DefaultSessionCompactionMinTokens
	}

	items, err := t.client.ListSessionCompactionCandidates(ctx, agentID, minMessages, minTokens)
	if err != nil {
		return "", fmt.Errorf("compaction_candidates_read: %w", err)
	}

	out, err := json.Marshal(map[string]any{
		"workspace_id": t.workspaceID,
		"agent_id":     agentID,
		"active_only":  true,
		"min_messages": minMessages,
		"min_tokens":   minTokens,
		"count":        len(items),
		"items":        items,
	})
	if err != nil {
		return "", fmt.Errorf("compaction_candidates_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceCompactionSnapshotsReadTool struct {
	client      CompactionSnapshotAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceCompactionSnapshotsReadTool(client CompactionSnapshotAwareRhizomeClient, workspaceID, agentID string) *WorkspaceCompactionSnapshotsReadTool {
	return &WorkspaceCompactionSnapshotsReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceCompactionSnapshotsReadTool) Name() string { return "compaction_snapshots_read" }

func (t *WorkspaceCompactionSnapshotsReadTool) Description() string {
	return "Read the current canonical session compaction snapshots from the existing snapshot ledger."
}

func (t *WorkspaceCompactionSnapshotsReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"session_id": {
				Type:        "string",
				Description: "Optional session id to narrow the canonical compaction snapshot ledger.",
			},
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id whose compaction snapshots should be read. Defaults to the current agent.",
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of canonical compaction snapshots to return.",
				Default:     20,
			},
		},
	}
}

func (t *WorkspaceCompactionSnapshotsReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		SessionID string `json:"session_id"`
		AgentID   string `json:"agent_id"`
		Limit     int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("compaction_snapshots_read: invalid input: %w", err)
	}

	agentID := strings.TrimSpace(params.AgentID)
	if agentID == "" {
		agentID = strings.TrimSpace(t.agentID)
	}

	items, err := t.client.ListSessionCompactionSnapshots(ctx, WorkspaceCompactionSnapshotFilter{
		WorkspaceID: t.workspaceID,
		SessionID:   strings.TrimSpace(params.SessionID),
		AgentID:     agentID,
		Limit:       params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("compaction_snapshots_read: %w", err)
	}

	out, err := json.Marshal(map[string]any{
		"workspace_id": t.workspaceID,
		"session_id":   strings.TrimSpace(params.SessionID),
		"agent_id":     agentID,
		"count":        len(items),
		"items":        items,
	})
	if err != nil {
		return "", fmt.Errorf("compaction_snapshots_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceEventsListReadTool struct {
	client      WorkspaceEventsListAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceEventsListReadTool(client WorkspaceEventsListAwareRhizomeClient, workspaceID, agentID string) *WorkspaceEventsListReadTool {
	return &WorkspaceEventsListReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceEventsListReadTool) Name() string { return "events_list_read" }

func (t *WorkspaceEventsListReadTool) Description() string {
	return "Read the current canonical runtime event ledger rows from the existing workspace events list surface."
}

func (t *WorkspaceEventsListReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"event_type": {
				Type:        "string",
				Description: "Optional runtime event type filter.",
			},
			"entity_type": {
				Type:        "string",
				Description: "Optional runtime entity type filter.",
			},
			"entity_id": {
				Type:        "string",
				Description: "Optional runtime entity id filter.",
			},
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id filter. Defaults to the current agent.",
			},
			"session_id": {
				Type:        "string",
				Description: "Optional session id filter.",
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task id filter.",
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of canonical runtime events to return.",
				Default:     50,
			},
		},
	}
}

func (t *WorkspaceEventsListReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		EventType  string `json:"event_type"`
		EntityType string `json:"entity_type"`
		EntityID   string `json:"entity_id"`
		AgentID    string `json:"agent_id"`
		SessionID  string `json:"session_id"`
		TaskID     string `json:"task_id"`
		Limit      int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("events_list_read: invalid input: %w", err)
	}

	agentID := strings.TrimSpace(params.AgentID)
	if agentID == "" {
		agentID = strings.TrimSpace(t.agentID)
	}
	result, err := t.client.ListWorkspaceEvents(ctx, WorkspaceEventsListFilter{
		WorkspaceID: t.workspaceID,
		EventType:   strings.TrimSpace(params.EventType),
		EntityType:  strings.TrimSpace(params.EntityType),
		EntityID:    strings.TrimSpace(params.EntityID),
		AgentID:     agentID,
		SessionID:   strings.TrimSpace(params.SessionID),
		TaskID:      strings.TrimSpace(params.TaskID),
		Limit:       params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("events_list_read: %w", err)
	}

	out, err := json.Marshal(map[string]any{
		"workspace_id":   result.WorkspaceID,
		"time_authority": result.TimeAuthority,
		"count":          result.Count,
		"items":          result.Items,
	})
	if err != nil {
		return "", fmt.Errorf("events_list_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceEventsReplayReadTool struct {
	client      WorkspaceEventsReplayAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceEventsReplayReadTool(client WorkspaceEventsReplayAwareRhizomeClient, workspaceID, agentID string) *WorkspaceEventsReplayReadTool {
	return &WorkspaceEventsReplayReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceEventsReplayReadTool) Name() string { return "events_replay_read" }

func (t *WorkspaceEventsReplayReadTool) Description() string {
	return "Read the current inspectability-only runtime replay report from the existing canonical read-side."
}

func (t *WorkspaceEventsReplayReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"session_id": {
				Type:        "string",
				Description: "Optional session id to narrow the canonical replay scope.",
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task id to narrow the canonical replay scope.",
			},
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id to narrow the canonical replay scope. Defaults to the current agent.",
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of canonical runtime events to consider for the replay report.",
				Default:     200,
			},
			"include_events": {
				Type:        "boolean",
				Description: "Include current replay event rows in the returned canonical report.",
				Default:     false,
			},
		},
	}
}

func (t *WorkspaceEventsReplayReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		SessionID     string `json:"session_id"`
		TaskID        string `json:"task_id"`
		AgentID       string `json:"agent_id"`
		Limit         int    `json:"limit"`
		IncludeEvents bool   `json:"include_events"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("events_replay_read: invalid input: %w", err)
	}

	agentID := strings.TrimSpace(params.AgentID)
	if agentID == "" {
		agentID = strings.TrimSpace(t.agentID)
	}
	report, err := t.client.ReplayWorkspaceEvents(ctx, WorkspaceEventsReplayFilter{
		WorkspaceID:   t.workspaceID,
		AgentID:       agentID,
		SessionID:     strings.TrimSpace(params.SessionID),
		TaskID:        strings.TrimSpace(params.TaskID),
		Limit:         params.Limit,
		IncludeEvents: params.IncludeEvents,
	})
	if err != nil {
		return "", fmt.Errorf("events_replay_read: %w", err)
	}

	out, err := json.Marshal(map[string]any{
		"workspace_id":   t.workspaceID,
		"report":         report,
		"time_authority": report.TimeAuthority,
		"metrics":        report.Metrics,
		"evaluation":     report.Evaluation,
		"counts":         runtimeReplayCounts(report),
	})
	if err != nil {
		return "", fmt.Errorf("events_replay_read: marshal result: %w", err)
	}
	return string(out), nil
}

type WorkspaceEventsEvaluateReadTool struct {
	client      WorkspaceEventsReplayAwareRhizomeClient
	workspaceID string
	agentID     string
}

func NewWorkspaceEventsEvaluateReadTool(client WorkspaceEventsReplayAwareRhizomeClient, workspaceID, agentID string) *WorkspaceEventsEvaluateReadTool {
	return &WorkspaceEventsEvaluateReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *WorkspaceEventsEvaluateReadTool) Name() string { return "events_evaluate_read" }

func (t *WorkspaceEventsEvaluateReadTool) Description() string {
	return "Read the current inspectability-only runtime replay evaluation from the existing canonical read-side."
}

func (t *WorkspaceEventsEvaluateReadTool) Schema() agenttools.Schema {
	return agenttools.Schema{
		Type: "object",
		Properties: map[string]agenttools.Property{
			"session_id": {
				Type:        "string",
				Description: "Optional session id to narrow the canonical evaluation scope.",
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task id to narrow the canonical evaluation scope.",
			},
			"agent_id": {
				Type:        "string",
				Description: "Optional agent id to narrow the canonical evaluation scope. Defaults to the current agent.",
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of canonical runtime events to consider for the evaluation scope.",
				Default:     200,
			},
		},
	}
}

func (t *WorkspaceEventsEvaluateReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		SessionID string `json:"session_id"`
		TaskID    string `json:"task_id"`
		AgentID   string `json:"agent_id"`
		Limit     int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("events_evaluate_read: invalid input: %w", err)
	}

	agentID := strings.TrimSpace(params.AgentID)
	if agentID == "" {
		agentID = strings.TrimSpace(t.agentID)
	}
	report, err := t.client.ReplayWorkspaceEvents(ctx, WorkspaceEventsReplayFilter{
		WorkspaceID: t.workspaceID,
		AgentID:     agentID,
		SessionID:   strings.TrimSpace(params.SessionID),
		TaskID:      strings.TrimSpace(params.TaskID),
		Limit:       params.Limit,
	})
	if err != nil {
		return "", fmt.Errorf("events_evaluate_read: %w", err)
	}

	out, err := json.Marshal(map[string]any{
		"workspace_id":   t.workspaceID,
		"time_authority": report.TimeAuthority,
		"truncated":      report.Truncated,
		"filter":         report.Filter,
		"metrics":        report.Metrics,
		"evaluation":     report.Evaluation,
		"counts":         runtimeReplayCounts(report),
	})
	if err != nil {
		return "", fmt.Errorf("events_evaluate_read: marshal result: %w", err)
	}
	return string(out), nil
}

func runtimeReplayCounts(report sqlite.RuntimeReplayReport) map[string]int {
	return map[string]int{
		"sessions":       len(report.Sessions),
		"queues":         len(report.Queues),
		"claims":         len(report.Claims),
		"execution_runs": len(report.ExecutionRuns),
		"events":         len(report.Events),
	}
}
