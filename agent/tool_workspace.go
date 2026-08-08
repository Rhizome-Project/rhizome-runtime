package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const workspaceToolTimeoutArg = "_timeout_sec"

type workspaceToolManifest struct {
	Route       *workspaceToolRouteSpec `json:"route,omitempty"`
	InputSchema map[string]any          `json:"input_schema,omitempty"`
}

type workspaceToolRouteSpec struct {
	Kind     string `json:"kind,omitempty"`
	ServerID string `json:"server_id,omitempty"`
	ToolName string `json:"tool_name,omitempty"`
}

type WorkspaceToolExecutionContextProvider func() (taskID, sessionID, runID string)

type WorkspaceToolOption func(*RhizomeWorkspaceTool)

func WithWorkspaceToolExecutionContextProvider(provider WorkspaceToolExecutionContextProvider) WorkspaceToolOption {
	return func(t *RhizomeWorkspaceTool) {
		t.executionContext = provider
	}
}

type RhizomeWorkspaceTool struct {
	client           *RhizomeClient
	workspaceID      string
	actorID          string
	toolID           string
	functionName     string
	description      string
	parameters       map[string]any
	executionContext WorkspaceToolExecutionContextProvider
}

func NewRhizomeWorkspaceTool(client *RhizomeClient, workspaceID, actorID string, record WorkspaceToolRecord, options ...WorkspaceToolOption) *RhizomeWorkspaceTool {
	tool := &RhizomeWorkspaceTool{
		client:       client,
		workspaceID:  strings.TrimSpace(workspaceID),
		actorID:      strings.TrimSpace(actorID),
		toolID:       strings.TrimSpace(record.ToolID),
		functionName: sanitizeWorkspaceFunctionName(record.ToolID),
		description:  firstNonEmpty(record.Description, record.DisplayName, "Rhizome workspace tool "+record.ToolID),
		parameters:   workspaceToolInputSchema(record),
	}
	for _, option := range options {
		if option != nil {
			option(tool)
		}
	}
	return tool
}

func (t *RhizomeWorkspaceTool) Name() string {
	return t.functionName
}

func (t *RhizomeWorkspaceTool) Description() string {
	return t.description
}

func (t *RhizomeWorkspaceTool) Parameters() map[string]any {
	return t.parameters
}

func (t *RhizomeWorkspaceTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	taskID, sessionID, runID := "", "", ""
	if t.executionContext != nil {
		taskID, sessionID, runID = t.executionContext()
	}
	callArgs, timeoutSec := workspaceToolCallArgsAndTimeout(args)
	result, err := t.client.CallWorkspaceTool(ctx, WorkspaceToolCallInput{
		ToolID:              t.toolID,
		WorkspaceID:         t.workspaceID,
		Arguments:           callArgs,
		TimeoutSec:          timeoutSec,
		ActorType:           "agent",
		ActorID:             t.actorID,
		RequestedCapability: "tool.call",
		TaskID:              strings.TrimSpace(taskID),
		SessionID:           strings.TrimSpace(sessionID),
		RunID:               strings.TrimSpace(runID),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("workspace tool %s failed: %v", t.toolID, err), IsError: true}
	}
	if strings.TrimSpace(result.ToolID) != t.toolID {
		return &ToolResult{
			Output:  fmt.Sprintf("workspace tool %s returned mismatched tool_id %q", t.toolID, strings.TrimSpace(result.ToolID)),
			IsError: true,
		}
	}
	if result.TimedOut {
		return &ToolResult{
			Output:  formatWorkspaceToolFailure(t.toolID, result, fmt.Sprintf("workspace tool %s timed out", t.toolID)),
			IsError: true,
		}
	}
	if result.IsError || result.ExitCode != 0 {
		return &ToolResult{
			Output:  formatWorkspaceToolFailure(t.toolID, result, fmt.Sprintf("workspace tool %s failed", t.toolID)),
			IsError: true,
		}
	}
	return &ToolResult{Output: workspaceToolResultOutput(result, false), IsError: false}
}

func workspaceToolCallArgsAndTimeout(args map[string]any) (map[string]any, int) {
	if args == nil {
		return nil, 0
	}
	timeoutSec := workspaceToolTimeoutSec(args[workspaceToolTimeoutArg])
	if _, ok := args[workspaceToolTimeoutArg]; !ok {
		return args, timeoutSec
	}
	cleaned := make(map[string]any, len(args)-1)
	for key, value := range args {
		if key == workspaceToolTimeoutArg {
			continue
		}
		cleaned[key] = value
	}
	return cleaned, timeoutSec
}

func workspaceToolTimeoutSec(value any) int {
	switch typed := value.(type) {
	case int:
		if typed > 0 {
			return typed
		}
	case int64:
		if typed > 0 {
			return int(typed)
		}
	case float64:
		if typed > 0 {
			return int(typed)
		}
	case json.Number:
		if parsed, err := typed.Int64(); err == nil && parsed > 0 {
			return int(parsed)
		}
	case string:
		if parsed, err := strconv.Atoi(strings.TrimSpace(typed)); err == nil && parsed > 0 {
			return parsed
		}
	}
	return 0
}

func formatWorkspaceToolFailure(_ string, result WorkspaceToolCallResult, summary string) string {
	detail := workspaceToolResultOutput(result, true)
	if detail == "" {
		return summary
	}
	return summary + "\n\n" + detail
}

func workspaceToolResultOutput(result WorkspaceToolCallResult, preferErrorDetail bool) string {
	if len(result.Content) > 0 {
		parts := make([]string, 0, len(result.Content))
		for _, item := range result.Content {
			if text := strings.TrimSpace(item.Text); text != "" {
				parts = append(parts, text)
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "\n\n")
		}
	}
	stdout := strings.TrimSpace(result.Stdout)
	stderr := strings.TrimSpace(result.Stderr)
	if stdout != "" {
		if stderr != "" && (preferErrorDetail || result.RouterKind == "") {
			return stdout + "\n\nstderr:\n" + stderr
		}
		return stdout
	}
	if stderr != "" {
		return "stderr:\n" + stderr
	}
	raw, _ := json.Marshal(result)
	return string(raw)
}

func sanitizeWorkspaceFunctionName(toolID string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(toolID) {
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
	name := strings.Trim(b.String(), "_")
	if name == "" {
		return "workspace_tool"
	}
	return name
}

func workspaceToolInputSchema(record WorkspaceToolRecord) map[string]any {
	manifest := parseWorkspaceToolManifest(record.ManifestJSON)
	if len(manifest.InputSchema) > 0 {
		return manifest.InputSchema
	}
	return map[string]any{"type": "object", "properties": map[string]any{}}
}

func parseWorkspaceToolManifest(raw string) workspaceToolManifest {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return workspaceToolManifest{}
	}
	var manifest workspaceToolManifest
	_ = json.Unmarshal([]byte(raw), &manifest)
	return manifest
}

func workspaceToolHasUsableSchema(record WorkspaceToolRecord) bool {
	manifest := parseWorkspaceToolManifest(record.ManifestJSON)
	return len(manifest.InputSchema) > 0
}
