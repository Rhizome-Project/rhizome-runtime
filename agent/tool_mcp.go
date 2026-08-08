package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type RhizomeMCPTool struct {
	client       *RhizomeClient
	workspaceID  string
	serverID     string
	toolName     string
	functionName string
	description  string
	parameters   map[string]any
}

func NewRhizomeMCPTool(client *RhizomeClient, workspaceID string, record MCPToolRecord) *RhizomeMCPTool {
	return &RhizomeMCPTool{
		client:       client,
		workspaceID:  strings.TrimSpace(workspaceID),
		serverID:     strings.TrimSpace(record.ServerID),
		toolName:     strings.TrimSpace(record.ToolName),
		functionName: sanitizeMCPFunctionName(record.ServerID, record.ToolName),
		description:  firstNonEmpty(record.Description, fmt.Sprintf("Rhizome MCP tool %s/%s", record.ServerID, record.ToolName)),
		parameters:   parseMCPInputSchema(record.InputSchema),
	}
}

func (t *RhizomeMCPTool) Name() string {
	return t.functionName
}

func (t *RhizomeMCPTool) Description() string {
	return t.description
}

func (t *RhizomeMCPTool) Parameters() map[string]any {
	return t.parameters
}

func (t *RhizomeMCPTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	result, err := t.client.CallMCPTool(ctx, t.serverID, t.toolName, args)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("mcp tool %s/%s failed: %v", t.serverID, t.toolName, err), IsError: true}
	}
	if strings.TrimSpace(result.ServerID) != t.serverID || strings.TrimSpace(result.ToolName) != t.toolName {
		return &ToolResult{
			Output: fmt.Sprintf(
				"mcp tool %s/%s returned mismatched identity %q/%q",
				t.serverID,
				t.toolName,
				strings.TrimSpace(result.ServerID),
				strings.TrimSpace(result.ToolName),
			),
			IsError: true,
		}
	}
	parts := make([]string, 0, len(result.Content))
	for _, item := range result.Content {
		if text := strings.TrimSpace(item.Text); text != "" {
			parts = append(parts, text)
		}
	}
	if len(parts) == 0 {
		raw, _ := json.Marshal(result)
		return &ToolResult{Output: string(raw), IsError: result.IsError}
	}
	return &ToolResult{Output: strings.Join(parts, "\n\n"), IsError: result.IsError}
}

func sanitizeMCPFunctionName(serverID, toolName string) string {
	raw := "mcp__" + strings.TrimSpace(serverID) + "__" + strings.TrimSpace(toolName)
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func parseMCPInputSchema(raw string) map[string]any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	var schema map[string]any
	if err := json.Unmarshal([]byte(raw), &schema); err != nil {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return schema
}
