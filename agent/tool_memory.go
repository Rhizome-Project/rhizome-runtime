package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// MemoryReadTool reads MEMORY.md from the workspace.
type MemoryReadTool struct {
	workdir string
	policy  workspacePathPolicy
}

func NewMemoryReadTool(workdir string) *MemoryReadTool {
	return &MemoryReadTool{workdir: workdir}
}

func NewMemoryReadToolWithDeniedSubpaths(workdir string, denied []string) *MemoryReadTool {
	return &MemoryReadTool{
		workdir: workdir,
		policy:  newWorkspacePathPolicy(workdir, denied),
	}
}

func (t *MemoryReadTool) Name() string { return "memory_read" }

func (t *MemoryReadTool) Description() string {
	return "Read the agent's long-term memory (MEMORY.md). Use this to recall past context."
}

func (t *MemoryReadTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}
}

func (t *MemoryReadTool) Execute(_ context.Context, _ map[string]any) *ToolResult {
	fullPath, err := safePath(t.workdir, "MEMORY.md")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("read error: %v", err), IsError: true}
	}
	if err := t.policy.Validate(fullPath); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	data, err := os.ReadFile(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &ToolResult{Output: "(no memory yet)"}
		}
		return &ToolResult{Output: fmt.Sprintf("read error: %v", err), IsError: true}
	}
	return &ToolResult{Output: string(data)}
}

// MemoryWriteTool writes to MEMORY.md.
type MemoryWriteTool struct {
	workdir string
}

func NewMemoryWriteTool(workdir string) *MemoryWriteTool {
	return &MemoryWriteTool{workdir: workdir}
}

func (t *MemoryWriteTool) Name() string { return "memory_write" }

func (t *MemoryWriteTool) Description() string {
	return "Write to the agent's long-term memory (MEMORY.md). Overwrites entire content. Use to persist important context between sessions."
}

func (t *MemoryWriteTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"content": map[string]any{
				"type":        "string",
				"description": "Full content for MEMORY.md",
			},
		},
		"required": []string{"content"},
	}
}

func (t *MemoryWriteTool) Execute(_ context.Context, args map[string]any) *ToolResult {
	content, _ := args["content"].(string)
	err := os.WriteFile(filepath.Join(t.workdir, "MEMORY.md"), []byte(content), 0644)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("write error: %v", err), IsError: true}
	}
	return &ToolResult{Output: "memory updated"}
}

// DailyNoteAppendTool appends to today's daily note.
type DailyNoteAppendTool struct {
	workdir string
}

func NewDailyNoteAppendTool(workdir string) *DailyNoteAppendTool {
	return &DailyNoteAppendTool{workdir: workdir}
}

func (t *DailyNoteAppendTool) Name() string { return "daily_note" }

func (t *DailyNoteAppendTool) Description() string {
	return "Append a note to today's daily log. Use for observations, events, task results."
}

func (t *DailyNoteAppendTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"note": map[string]any{
				"type":        "string",
				"description": "Content to append to today's daily note",
			},
		},
		"required": []string{"note"},
	}
}

func (t *DailyNoteAppendTool) Execute(_ context.Context, args map[string]any) *ToolResult {
	note, _ := args["note"].(string)
	if note == "" {
		return &ToolResult{Output: "note is required", IsError: true}
	}

	now := time.Now()
	dir := filepath.Join(t.workdir, "memory", now.Format("200601"))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return &ToolResult{Output: fmt.Sprintf("mkdir error: %v", err), IsError: true}
	}

	path := filepath.Join(dir, now.Format("20060102")+".md")

	// Create with header if new
	if _, err := os.Stat(path); os.IsNotExist(err) {
		header := fmt.Sprintf("# %s\n\n", now.Format("2006-01-02 Monday"))
		os.WriteFile(path, []byte(header), 0644)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("open error: %v", err), IsError: true}
	}
	defer f.Close()

	fmt.Fprintf(f, "- [%s] %s\n", now.Format("15:04"), note)
	return &ToolResult{Output: fmt.Sprintf("appended to %s", now.Format("20060102")+".md")}
}

// MemorySearchTool queries the agent's sqlite episodic/digest memory via FTS.
type MemorySearchTool struct {
	service     *AgentMemoryService
	client      *RhizomeClient
	workspaceID string
}

func NewMemorySearchTool(service *AgentMemoryService, client *RhizomeClient, workspaceID string) *MemorySearchTool {
	return &MemorySearchTool{service: service, client: client, workspaceID: workspaceID}
}

func (t *MemorySearchTool) Name() string { return "memory_search" }

func (t *MemorySearchTool) Description() string {
	return "Search the agent's long-term internal memory (recent tasks, past constraints, procedures) via Full-Text Search. Use this to explicitly search your episodic memory for relevant past experiences when working on something new."
}

func (t *MemorySearchTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type":        "string",
				"description": "The search keywords to look up in the event summaries and bodies.",
			},
		},
		"required": []string{"query"},
	}
}

func (t *MemorySearchTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	query, _ := args["query"].(string)
	if strings.TrimSpace(query) == "" {
		return &ToolResult{Output: "query string is required", IsError: true}
	}

	digests, episodes, err := t.service.SearchMemory(ctx, query, 5)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("search memory error: %v", err), IsError: true}
	}

	if len(digests) == 0 && len(episodes) == 0 {
		return &ToolResult{Output: "No memories found marching query: " + query}
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("# Memory Search Results for '%s'\n\n", query))

	if len(digests) > 0 {
		b.WriteString("## Relevant Digests (Procedures, Decisions, Lessons, Notes)\n")
		for _, d := range digests {
			b.WriteString(fmt.Sprintf("### [%s] %s\n", d.Kind, firstNonEmpty(d.Summary, d.DigestID)))
			if d.Body != "" && d.Body != d.Summary {
				b.WriteString(d.Body + "\n")
			}
			b.WriteString("\n")
		}
	}

	if len(episodes) > 0 {
		b.WriteString("## Relevant Task Episodes\n")
		for _, e := range episodes {
			b.WriteString(fmt.Sprintf("- [%s] %s\n", firstNonEmpty(e.Outcome, "unknown"), firstNonEmpty(e.Summary, e.EpisodeID)))
		}
	}

	if t.client != nil && t.workspaceID != "" {
		go func() {
			for _, d := range digests {
				if d.DigestID != "" {
					_ = t.client.TouchMemoryNode(context.Background(), WorkspaceMemoryNodeTouchInput{
						WorkspaceID: t.workspaceID,
						NodeID:      d.DigestID,
						Trusted:     false,
					})
				}
			}
			for _, e := range episodes {
				if e.EpisodeID != "" {
					_ = t.client.TouchMemoryNode(context.Background(), WorkspaceMemoryNodeTouchInput{
						WorkspaceID: t.workspaceID,
						NodeID:      e.EpisodeID,
						Trusted:     false,
					})
				}
			}
		}()
	}

	return &ToolResult{Output: b.String()}
}

// MemoryReinforceTool reinforces a useful memory node.
type MemoryReinforceTool struct {
	client      *RhizomeClient
	workspaceID string
}

func NewMemoryReinforceTool(client *RhizomeClient, workspaceID string) *MemoryReinforceTool {
	return &MemoryReinforceTool{client: client, workspaceID: workspaceID}
}

func (t *MemoryReinforceTool) Name() string { return "memory_reinforce" }

func (t *MemoryReinforceTool) Description() string {
	return "Reinforce a useful memory node. If a specific memory node (e.g. from memory_search or context) helped you solve a problem, reinforce it to prevent it from being garbage collected."
}

func (t *MemoryReinforceTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"node_id": map[string]any{
				"type":        "string",
				"description": "The ID of the memory node to reinforce.",
			},
		},
		"required": []string{"node_id"},
	}
}

func (t *MemoryReinforceTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	nodeID, _ := args["node_id"].(string)
	if nodeID == "" {
		return &ToolResult{Output: "node_id is required", IsError: true}
	}
	if t.client == nil || t.workspaceID == "" {
		return &ToolResult{Output: "cannot reinforce: client not configured"}
	}
	err := t.client.TouchMemoryNode(ctx, WorkspaceMemoryNodeTouchInput{
		WorkspaceID: t.workspaceID,
		NodeID:      nodeID,
		Trusted:     true,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("failed to reinforce memory: %v", err), IsError: true}
	}
	return &ToolResult{Output: "Memory node reinforced."}
}

// MemoryCoherenceReadTool reads the current agent/session-scoped coherence attention snapshot
type MemoryCoherenceReadTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewMemoryCoherenceReadTool(client *RhizomeClient, workspaceID, agentID string) *MemoryCoherenceReadTool {
	return &MemoryCoherenceReadTool{client: client, workspaceID: workspaceID, agentID: agentID}
}

func (t *MemoryCoherenceReadTool) Name() string { return "memory_coherence_read" }

func (t *MemoryCoherenceReadTool) Description() string {
	return "Read the current memory coherence snapshot for this workspace to understand system attention."
}

func (t *MemoryCoherenceReadTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}
}

func (t *MemoryCoherenceReadTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" {
		return &ToolResult{Output: "Disabled", IsError: true}
	}
	raw, err := t.client.GetMemoryCoherence(ctx, MemoryCoherenceInput{
		WorkspaceID: t.workspaceID,
		AgentID:     t.agentID,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("error reading coherence: %v", err), IsError: true}
	}
	return &ToolResult{Output: string(raw)}
}

// MemoryPromotionReadTool reads current memory promotion candidates
type MemoryPromotionReadTool struct {
	client      *RhizomeClient
	workspaceID string
}

func NewMemoryPromotionReadTool(client *RhizomeClient, workspaceID string) *MemoryPromotionReadTool {
	return &MemoryPromotionReadTool{client: client, workspaceID: workspaceID}
}

func (t *MemoryPromotionReadTool) Name() string { return "memory_promotion_read" }

func (t *MemoryPromotionReadTool) Description() string {
	return "List current memory promotion candidates in the workspace."
}

func (t *MemoryPromotionReadTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}
}

func (t *MemoryPromotionReadTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" {
		return &ToolResult{Output: "Disabled", IsError: true}
	}
	raw, err := t.client.ListPromotionRequests(ctx, MemoryPromotionInput{
		WorkspaceID: t.workspaceID,
		Limit:       50,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("error reading promotions: %v", err), IsError: true}
	}
	return &ToolResult{Output: string(raw)}
}
