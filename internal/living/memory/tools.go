package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
)

var validTypes = []string{TypeExperience, TypeReflection, TypeProcedure, TypeEntity, TypeError}

// ---- MemorySearchTool ----

// MemorySearchTool implements tools.Tool for BM25 full-text search over agent memory.
type MemorySearchTool struct {
	store *MemoryStore
}

// NewMemorySearchTool creates a MemorySearchTool backed by the given store.
func NewMemorySearchTool(store *MemoryStore) *MemorySearchTool {
	return &MemorySearchTool{store: store}
}

func (t *MemorySearchTool) Name() string { return "memory_search" }

func (t *MemorySearchTool) Description() string {
	return "Search agent memory using BM25 full-text search. Returns ranked results."
}

func (t *MemorySearchTool) Schema() tools.Schema {
	return tools.Schema{
		Type: "object",
		Properties: map[string]tools.Property{
			"query": {
				Type:        "string",
				Description: "The search query string.",
			},
			"type": {
				Type:        "string",
				Description: "Optional type filter.",
				Enum:        validTypes,
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

func (t *MemorySearchTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Query string `json:"query"`
		Type  string `json:"type"`
		Limit int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_search: invalid input: %w", err)
	}

	opts := SearchOpts{
		TypeFilter: params.Type,
		Limit:      params.Limit,
	}

	entries, err := t.store.Search(ctx, params.Query, opts)
	if err != nil {
		return "", fmt.Errorf("memory_search: %w", err)
	}

	type searchResult struct {
		ID        int64   `json:"id"`
		Type      string  `json:"type"`
		Topic     string  `json:"topic"`
		Content   string  `json:"content"`
		Timestamp string  `json:"timestamp"`
		Rank      float64 `json:"rank"`
	}

	results := make([]searchResult, len(entries))
	for i, e := range entries {
		content := e.Content
		if len(content) > 500 {
			content = content[:500] + "..."
		}
		results[i] = searchResult{
			ID:        e.ID,
			Type:      e.Type,
			Topic:     e.Topic,
			Content:   content,
			Timestamp: e.Timestamp.Format("2006-01-02T15:04:05.000"),
			Rank:      e.Rank,
		}
	}

	out, err := json.Marshal(results)
	if err != nil {
		return "", fmt.Errorf("memory_search: marshal results: %w", err)
	}
	return string(out), nil
}

// ---- MemoryReadTool ----

// MemoryReadTool implements tools.Tool for reading recent memory entries.
type MemoryReadTool struct {
	store *MemoryStore
}

// NewMemoryReadTool creates a MemoryReadTool backed by the given store.
func NewMemoryReadTool(store *MemoryStore) *MemoryReadTool {
	return &MemoryReadTool{store: store}
}

func (t *MemoryReadTool) Name() string { return "memory_read" }

func (t *MemoryReadTool) Description() string {
	return "Read recent memory entries, optionally filtered by type."
}

func (t *MemoryReadTool) Schema() tools.Schema {
	return tools.Schema{
		Type: "object",
		Properties: map[string]tools.Property{
			"type": {
				Type:        "string",
				Description: "Optional type filter.",
				Enum:        validTypes,
			},
			"limit": {
				Type:        "integer",
				Description: "Maximum number of results to return.",
				Default:     10,
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task ID filter.",
			},
		},
	}
}

func (t *MemoryReadTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Type   string `json:"type"`
		Limit  int    `json:"limit"`
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_read: invalid input: %w", err)
	}

	opts := RecentOpts{
		TypeFilter: params.Type,
		Limit:      params.Limit,
		TaskID:     params.TaskID,
	}

	entries, err := t.store.GetRecent(ctx, opts)
	if err != nil {
		return "", fmt.Errorf("memory_read: %w", err)
	}

	type readResult struct {
		ID        int64  `json:"id"`
		Type      string `json:"type"`
		Topic     string `json:"topic"`
		Content   string `json:"content"`
		Timestamp string `json:"timestamp"`
	}

	results := make([]readResult, len(entries))
	for i, e := range entries {
		results[i] = readResult{
			ID:        e.ID,
			Type:      e.Type,
			Topic:     e.Topic,
			Content:   e.Content,
			Timestamp: e.Timestamp.Format("2006-01-02T15:04:05.000"),
		}
	}

	out, err := json.Marshal(results)
	if err != nil {
		return "", fmt.Errorf("memory_read: marshal results: %w", err)
	}
	return string(out), nil
}

// ---- MemoryWriteTool ----

// MemoryWriteTool implements tools.Tool for saving new entries to agent memory.
type MemoryWriteTool struct {
	store        *MemoryStore
	memoryMDPath string
}

// NewMemoryWriteTool creates a MemoryWriteTool. If memoryMDPath is non-empty,
// procedure and entity entries will also be appended to that MEMORY.md file.
func NewMemoryWriteTool(store *MemoryStore, memoryMDPath string) *MemoryWriteTool {
	return &MemoryWriteTool{store: store, memoryMDPath: memoryMDPath}
}

func (t *MemoryWriteTool) Name() string { return "memory_write" }

func (t *MemoryWriteTool) Description() string {
	return "Save a new entry to agent memory. Optionally updates MEMORY.md index."
}

func (t *MemoryWriteTool) Schema() tools.Schema {
	return tools.Schema{
		Type: "object",
		Properties: map[string]tools.Property{
			"type": {
				Type:        "string",
				Description: "The type of memory entry.",
				Enum:        validTypes,
			},
			"topic": {
				Type:        "string",
				Description: "Short topic or title for the entry.",
			},
			"content": {
				Type:        "string",
				Description: "The content of the memory entry.",
			},
			"source": {
				Type:        "string",
				Description: "Source of the entry.",
				Default:     "brain",
			},
			"task_id": {
				Type:        "string",
				Description: "Optional task ID to associate with this entry.",
			},
		},
		Required: []string{"type", "topic", "content"},
	}
}

func (t *MemoryWriteTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Type    string `json:"type"`
		Topic   string `json:"topic"`
		Content string `json:"content"`
		Source  string `json:"source"`
		TaskID  string `json:"task_id"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("memory_write: invalid input: %w", err)
	}

	// Validate type.
	if !isValidType(params.Type) {
		return "", fmt.Errorf("memory_write: invalid type %q; must be one of %s", params.Type, strings.Join(validTypes, ", "))
	}

	// Validate content.
	if params.Content == "" {
		return "", fmt.Errorf("memory_write: content must not be empty")
	}

	source := params.Source
	if source == "" {
		source = "brain"
	}

	entry := MemoryEntry{
		Type:    params.Type,
		Topic:   params.Topic,
		Content: params.Content,
		Source:  source,
		TaskID:  params.TaskID,
	}

	id, err := t.store.Save(ctx, entry)
	if err != nil {
		return "", fmt.Errorf("memory_write: %w", err)
	}

	// Append to MEMORY.md for procedure and entity types.
	if (params.Type == TypeProcedure || params.Type == TypeEntity) && t.memoryMDPath != "" {
		contentPreview := params.Content
		if len(contentPreview) > 100 {
			contentPreview = contentPreview[:100]
		}
		line := fmt.Sprintf("- [%s] %s: %s\n", params.Type, params.Topic, contentPreview)
		f, err := os.OpenFile(t.memoryMDPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("memory_write: warning: failed to open MEMORY.md: %v", err)
		} else {
			if _, err := f.WriteString(line); err != nil {
				log.Printf("memory_write: warning: failed to write to MEMORY.md: %v", err)
			}
			f.Close()
		}
	}

	result := fmt.Sprintf(`{"status":"saved","id":%d}`, id)
	return result, nil
}

func isValidType(t string) bool {
	for _, vt := range validTypes {
		if t == vt {
			return true
		}
	}
	return false
}
