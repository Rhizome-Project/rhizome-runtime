package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	agenttools "github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
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

// WorkspaceMemorySearchTool searches the workspace memory graph context natively via sqlite.
type WorkspaceMemorySearchTool struct {
	store       *sqlite.Store
	workspaceID string
}

func NewWorkspaceMemorySearchTool(store *sqlite.Store, workspaceID string) *WorkspaceMemorySearchTool {
	return &WorkspaceMemorySearchTool{store: store, workspaceID: workspaceID}
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

	items, err := t.store.SearchWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
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

// WorkspaceMemoryReadTool lists recent canonical memory records from sqlite.
type WorkspaceMemoryReadTool struct {
	store       *sqlite.Store
	workspaceID string
}

func NewWorkspaceMemoryReadTool(store *sqlite.Store, workspaceID string) *WorkspaceMemoryReadTool {
	return &WorkspaceMemoryReadTool{store: store, workspaceID: workspaceID}
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

	items, err := t.store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
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

// WorkspaceMemoryWriteTool saves durable canonical memory attributed to the acting agent via sqlite.
type WorkspaceMemoryWriteTool struct {
	store       *sqlite.Store
	workspaceID string
	agentID     string
}

func NewWorkspaceMemoryWriteTool(store *sqlite.Store, workspaceID, agentID string) *WorkspaceMemoryWriteTool {
	return &WorkspaceMemoryWriteTool{store: store, workspaceID: workspaceID, agentID: agentID}
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

	writeInput := sqlite.WorkspaceMemoryInput{
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
	record, err := t.store.RecordWorkspaceMemory(ctx, writeInput)
	if err != nil {
		return "", fmt.Errorf("memory_write: %w", err)
	}
	outPayload["memory"] = record

	out, err := json.Marshal(outPayload)
	if err != nil {
		return "", fmt.Errorf("memory_write: marshal result: %w", err)
	}
	return string(out), nil
}

func truncateWorkspaceMemoryRecords(items []sqlite.WorkspaceMemoryRecord) []sqlite.WorkspaceMemoryRecord {
	out := make([]sqlite.WorkspaceMemoryRecord, 0, len(items))
	for _, item := range items {
		copyItem := item
		if len(copyItem.Body) > 500 {
			copyItem.Body = copyItem.Body[:500] + "..."
		}
		out = append(out, copyItem)
	}
	return out
}

// WorkspaceMemoryPacketShellTool builds a bounded shell memory packet directly via sqlite.
type WorkspaceMemoryPacketShellTool struct {
	store       *sqlite.Store
	workspaceID string
	agentID     string
}

func NewWorkspaceMemoryPacketShellTool(store *sqlite.Store, workspaceID, agentID string) *WorkspaceMemoryPacketShellTool {
	return &WorkspaceMemoryPacketShellTool{store: store, workspaceID: workspaceID, agentID: agentID}
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

	packet, err := t.store.BuildMemoryShellPacket(ctx, sqlite.MemoryPacketFilter{
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

// WorkspaceMemoryPacketKernelTool builds a bounded kernel memory packet natively via sqlite.
type WorkspaceMemoryPacketKernelTool struct {
	store       *sqlite.Store
	workspaceID string
}

func NewWorkspaceMemoryPacketKernelTool(store *sqlite.Store, workspaceID string) *WorkspaceMemoryPacketKernelTool {
	return &WorkspaceMemoryPacketKernelTool{store: store, workspaceID: workspaceID}
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

	packet, err := t.store.BuildMemoryKernelPacket(ctx, sqlite.MemoryPacketFilter{
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

// RegisterTools registers all canonical memory tools into the agent's tool registry.
func RegisterTools(reg *agenttools.Registry, store *sqlite.Store, workspaceID, agentID string) {
	reg.Register(NewWorkspaceMemorySearchTool(store, workspaceID))
	reg.Register(NewWorkspaceMemoryReadTool(store, workspaceID))
	reg.Register(NewWorkspaceMemoryWriteTool(store, workspaceID, agentID))
	reg.Register(NewWorkspaceMemoryPacketShellTool(store, workspaceID, agentID))
	reg.Register(NewWorkspaceMemoryPacketKernelTool(store, workspaceID))
}
