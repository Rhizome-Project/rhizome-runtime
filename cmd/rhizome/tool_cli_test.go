package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/Rhizome-Project/rhizome-runtime/internal/tools"
)

func TestResolveToolInvokeUpdateType_DefaultsToGenericToolCall(t *testing.T) {
	t.Parallel()

	got := resolveToolInvokeUpdateType("other-tool", "")
	if got != "tool_call_requested" {
		t.Fatalf("expected tool_call_requested, got %q", got)
	}
}

func TestToolInvokePayloadNormalizesGenericPayload(t *testing.T) {
	t.Parallel()

	request := toolInvokePayload{
		ToolID:       "  design-tool  ",
		Prompt:       "  Write code  ",
		DocKeys:      []string{" spec ", "", "notes"},
		ArtifactRefs: []string{" artifact:1 ", "\t"},
	}

	request.Normalize()
	if request.ToolID != "design-tool" || request.Prompt != "Write code" {
		t.Fatalf("request not normalized: %+v", request)
	}
	if got := strings.Join(request.DocKeys, ","); got != "spec,notes" {
		t.Fatalf("doc keys not normalized: %q", got)
	}
	if got := strings.Join(request.ArtifactRefs, ","); got != "artifact:1" {
		t.Fatalf("artifact refs not normalized: %q", got)
	}
}

func TestRunToolInvokeRequiresExplicitToolID(t *testing.T) {
	err := runToolInvoke([]string{
		"--workspace-id", "ws-main",
		"--agent-id", "agent-a",
	})
	if err == nil {
		t.Fatal("expected missing tool id to fail")
	}
	if !strings.Contains(err.Error(), "--tool-id or positional <tool-id> is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunToolInvokeRejectsRemovedLegacyProvider(t *testing.T) {
	err := runToolInvoke([]string{
		"--workspace-id", "ws-main",
		"--agent-id", "agent-a",
		"--tool-id", sqlite.RemovedLegacyProviderToolID,
		"--prompt", "Write code",
	})
	if err == nil {
		t.Fatal("expected removed legacy provider tool invoke to fail")
	}
	if !strings.Contains(err.Error(), "has been removed from Rhizome") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunToolInvokePublishesAgentUpdatePromptContext(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-tool-invoke-prompt-context"
		agentID     = "agent-tool-invoke"
		toolID      = "design-tool"
	)
	createCLITestWorkspace(t, workspaceID)
	if err := runAgentRegister([]string{
		"--workspace-id", workspaceID,
		"--agent-id", agentID,
		"--owner-user-id", "developer",
		"--display-name", "Tool Invoke Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}
	if err := runToolRegister([]string{
		"--workspace-id", workspaceID,
		"--tool-id", toolID,
		"--display-name", "Design Tool",
		"--owner-user-id", "developer",
		"--kind", "OTHER",
		"--status", "ACTIVE",
	}); err != nil {
		t.Fatalf("runToolRegister failed: %v", err)
	}

	if _, err := captureStdout(t, func() error {
		return runToolInvoke([]string{
			"--workspace-id", workspaceID,
			"--agent-id", agentID,
			"--tool-id", toolID,
			"--prompt", "Draft a compact coordination artifact",
			"--summary", "Invoke design tool",
		})
	}); err != nil {
		t.Fatalf("runToolInvoke failed: %v", err)
	}
	requireCLIAgentUpdateRuntimeEvent(t, workspaceID, agentID, "cli.tool.invoke.update", map[string]string{
		"agent_id":       agentID,
		"actor_agent_id": agentID,
		"update_type":    "tool_call_requested",
		"summary":        "Invoke design tool",
		"requires_human": "false",
	})
}

func TestDefaultToolInvokeSummary(t *testing.T) {
	t.Parallel()

	summary := defaultToolInvokeSummary(sqlite.WorkspaceToolRecord{
		ToolID:      "design-tool",
		DisplayName: "Design Tool",
	}, "task-123")
	if summary != "Invoke Design Tool for task-123" {
		t.Fatalf("unexpected summary: %q", summary)
	}
}

func TestRunToolRemove_RemovesRegistryEntryAndIsIdempotent(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-tools",
		"--title", "Tools Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-tools")
	if err := runToolRegister([]string{
		"--workspace-id", "ws-tools",
		"--tool-id", "telegram-bot",
		"--display-name", "Telegram Bot",
		"--owner-user-id", "developer",
		"--kind", "BOT",
		"--status", "ACTIVE",
	}); err != nil {
		t.Fatalf("runToolRegister failed: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runToolRemove([]string{
			"--workspace-id", "ws-tools",
			"--tool-id", "telegram-bot",
			"--removed-by", "developer",
		})
	})
	if err != nil {
		t.Fatalf("runToolRemove failed: %v", err)
	}

	var first struct {
		ToolID  string `json:"tool_id"`
		Status  string `json:"status"`
		Existed bool   `json:"existed"`
	}
	if err := json.Unmarshal([]byte(out), &first); err != nil {
		t.Fatalf("decode first remove output: %v; output=%q", err, out)
	}
	if first.ToolID != "telegram-bot" || first.Status != "REMOVED" || !first.Existed {
		t.Fatalf("unexpected first remove output: %+v", first)
	}

	out, err = captureStdout(t, func() error {
		return runToolRemove([]string{
			"--workspace-id", "ws-tools",
			"--tool-id", "telegram-bot",
			"--removed-by", "developer",
		})
	})
	if err != nil {
		t.Fatalf("second runToolRemove failed: %v", err)
	}

	var second struct {
		Existed bool   `json:"existed"`
		Status  string `json:"status"`
	}
	if err := json.Unmarshal([]byte(out), &second); err != nil {
		t.Fatalf("decode second remove output: %v; output=%q", err, out)
	}
	if second.Existed || second.Status != "REMOVED" {
		t.Fatalf("unexpected second remove output: %+v", second)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := store.GetWorkspaceTool(ctx, "ws-tools", "telegram-bot"); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected tool to be removed, got %v", err)
	}
}

func TestRunToolRemove_RefusesWhenDeploymentExists(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-tools",
		"--title", "Tools Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-tools")
	if err := runToolRegister([]string{
		"--workspace-id", "ws-tools",
		"--tool-id", "telegram-bot",
		"--display-name", "Telegram Bot",
		"--owner-user-id", "developer",
		"--kind", "BOT",
		"--status", "ACTIVE",
	}); err != nil {
		t.Fatalf("runToolRegister failed: %v", err)
	}
	if _, err := workspaceToolExecutor().Deploy(tools.DeployInput{
		WorkspaceID: "ws-tools",
		ToolID:      "telegram-bot",
		Runtime:     "python",
		SourceCode:  "print('ok')\n",
		DeployedBy:  "developer",
	}); err != nil {
		t.Fatalf("deploy tool fixture: %v", err)
	}

	err := runToolRemove([]string{
		"--workspace-id", "ws-tools",
		"--tool-id", "telegram-bot",
		"--removed-by", "developer",
	})
	if err == nil {
		t.Fatal("expected runToolRemove to refuse while tool is deployed")
	}
	if !strings.Contains(err.Error(), "still deployed") {
		t.Fatalf("expected deployed refusal, got %v", err)
	}
}
