package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/bridgepolicy"
	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type workspaceToolManifest struct {
	Route          *workspaceToolRouteSpec      `json:"route,omitempty"`
	InputSchema    any                          `json:"input_schema,omitempty"`
	PolicyEnvelope *bridgepolicy.PolicyEnvelope `json:"policy_envelope,omitempty"`
}

type workspaceToolRouteSpec struct {
	Kind     string `json:"kind,omitempty"`
	ServerID string `json:"server_id,omitempty"`
	ToolName string `json:"tool_name,omitempty"`
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

func mcpWorkspaceToolID(serverID, toolName string) string {
	return sanitizeToolFunctionName("mcp__" + strings.TrimSpace(serverID) + "__" + strings.TrimSpace(toolName))
}

func sanitizeToolFunctionName(value string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(value) {
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
	return strings.Trim(b.String(), "_")
}

func mcpWorkspaceToolManifest(server mcp.ServerRecord, tool mcp.Tool) string {
	envelope := mcpWorkspaceToolPolicyEnvelope(server)
	manifest := workspaceToolManifest{
		Route: &workspaceToolRouteSpec{
			Kind:     "mcp",
			ServerID: strings.TrimSpace(server.ServerID),
			ToolName: strings.TrimSpace(tool.Name),
		},
		PolicyEnvelope: &envelope,
	}
	if len(tool.InputSchema) > 0 {
		var schema any
		_ = json.Unmarshal(tool.InputSchema, &schema)
		manifest.InputSchema = schema
	}
	raw, _ := json.Marshal(manifest)
	return string(raw)
}

func mcpWorkspaceToolPolicyEnvelope(server mcp.ServerRecord) bridgepolicy.PolicyEnvelope {
	transport := strings.ToLower(strings.TrimSpace(server.Transport))
	switch transport {
	case "stdio":
		return bridgepolicy.BuildEnvelope(
			"mcp/stdio",
			bridgepolicy.TierCodeExec,
			bridgepolicy.PostureSupported,
			[]bridgepolicy.SurfacePolicy{
				bridgepolicy.NewPolicy(
					"mcp/stdio-server",
					bridgepolicy.TierCodeExec,
					"local MCP stdio transport launches a host process on the shared machine",
				),
				bridgepolicy.NewPolicy(
					"mcp/stdio-tool-execution",
					bridgepolicy.TierHighRisk,
					"discovered stdio MCP tools require explicit operator approval before execution",
				),
			},
			"local stdio MCP transport executes a host binary on the shared VPS",
			"discovered aliases stay capability-gated instead of downgrading into a generic tool.call surface",
		)
	default:
		return bridgepolicy.BuildEnvelope(
			"mcp/streamable-http",
			bridgepolicy.TierNetworked,
			bridgepolicy.PostureSupported,
			[]bridgepolicy.SurfacePolicy{
				bridgepolicy.NewPolicy(
					"mcp/streamable-http",
					bridgepolicy.TierNetworked,
					"remote MCP transport over explicit HTTP client",
				),
			},
			"discovered HTTP MCP aliases remain policy-enveloped and capability-gated",
		)
	}
}

func mcpWorkspaceToolCapabilities(server mcp.ServerRecord) []string {
	envelope := mcpWorkspaceToolPolicyEnvelope(server)
	capabilities := []string{"tool.call", "bridge.policy_enveloped"}
	if envelope.HighRisk {
		capabilities = append(capabilities, "bridge.high_risk")
	}
	if !envelope.SupportedArchitecture {
		capabilities = append(capabilities, "bridge.legacy_unsupported")
	}
	if envelope.OperatorControlRequired {
		capabilities = append(capabilities, "bridge.operator_control_required")
	}
	return normalizeWorkspaceToolCapabilities(capabilities)
}

func mcpWorkspaceToolClassificationStale(record sqlite.WorkspaceToolRecord, server mcp.ServerRecord) bool {
	expectedEnvelope := mcpWorkspaceToolPolicyEnvelope(server)
	if record.PolicyEnvelope == nil {
		return true
	}
	actual := record.PolicyEnvelope
	if actual.Surface != expectedEnvelope.Surface ||
		actual.PrimaryTier != expectedEnvelope.PrimaryTier ||
		actual.HighestTier != expectedEnvelope.HighestTier ||
		actual.SupportedArchitecture != expectedEnvelope.SupportedArchitecture ||
		actual.HighRisk != expectedEnvelope.HighRisk ||
		actual.OperatorControlRequired != expectedEnvelope.OperatorControlRequired {
		return true
	}
	for _, required := range mcpWorkspaceToolCapabilities(server) {
		if !workspaceToolHasCapability(record.Capabilities, required) {
			return true
		}
	}
	return false
}

func normalizeWorkspaceToolCapabilities(raw []string) []string {
	out := make([]string, 0, len(raw))
	seen := map[string]struct{}{}
	for _, capability := range raw {
		trimmed := strings.TrimSpace(capability)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func workspaceToolHasCapability(capabilities []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, capability := range capabilities {
		if strings.EqualFold(strings.TrimSpace(capability), want) {
			return true
		}
	}
	return false
}

func (h *Handler) executeMCPTool(ctx context.Context, workspaceID, serverID, toolName string, arguments map[string]any) (*mcp.ToolCallResult, error) {
	server, err := h.mcpStore.GetServer(ctx, workspaceID, serverID)
	if err != nil {
		return nil, fmt.Errorf("server not found: %w", err)
	}

	switch server.Transport {
	case "streamable-http":
		headers := h.mcpStore.GetServerHeaders(server)
		result, err := h.mcpClient.CallTool(ctx, server.URL, headers, toolName, arguments)
		if err != nil {
			return nil, fmt.Errorf("tool call failed: %w", err)
		}
		return result, nil
	case "stdio":
		command, args, env := h.mcpStore.GetServerStdioConfig(server)
		sc := mcp.NewStdioClient()
		if err := sc.Start(command, args, env); err != nil {
			return nil, fmt.Errorf("start process failed: %w", err)
		}
		defer sc.Stop()
		if _, err := sc.Initialize(ctx); err != nil {
			return nil, fmt.Errorf("initialize failed: %w", err)
		}
		result, err := sc.CallTool(ctx, toolName, arguments)
		if err != nil {
			return nil, fmt.Errorf("tool call failed: %w", err)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported transport: %s", server.Transport)
	}
}

func joinMCPToolContent(result *mcp.ToolCallResult) string {
	if result == nil {
		return ""
	}
	parts := make([]string, 0, len(result.Content))
	for _, item := range result.Content {
		if text := strings.TrimSpace(item.Text); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n"))
}

func (h *Handler) routedToolCall(ctx context.Context, workspaceID, toolID string, arguments map[string]any) (map[string]any, bool, error) {
	record, err := h.store.GetWorkspaceTool(ctx, workspaceID, toolID)
	if err != nil {
		if errors.Is(err, sqlite.ErrToolNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	manifest := parseWorkspaceToolManifest(record.ManifestJSON)
	if manifest.Route == nil || !strings.EqualFold(strings.TrimSpace(manifest.Route.Kind), "mcp") {
		return nil, false, nil
	}

	result, err := h.executeMCPTool(ctx, workspaceID, manifest.Route.ServerID, manifest.Route.ToolName, arguments)
	if err != nil {
		return nil, true, err
	}
	exitCode := 0
	if result.IsError {
		exitCode = 1
	}
	return map[string]any{
		"tool_id":     toolID,
		"stdout":      joinMCPToolContent(result),
		"stderr":      "",
		"exit_code":   exitCode,
		"timed_out":   false,
		"router_kind": "mcp",
		"is_error":    result.IsError,
		"content":     result.Content,
		"server_id":   manifest.Route.ServerID,
		"tool_name":   manifest.Route.ToolName,
	}, true, nil
}
