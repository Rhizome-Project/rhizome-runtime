package tools

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecutorDeployRequiresWorkspaceID(t *testing.T) {
	t.Parallel()

	executor := NewExecutor(t.TempDir())
	if _, err := executor.Deploy(DeployInput{
		ToolID:     "tool-no-workspace",
		Runtime:    "node",
		SourceCode: `console.log("no workspace")`,
		EntryPoint: "main.js",
	}); err == nil || !strings.Contains(err.Error(), "workspace_id is required") {
		t.Fatalf("expected workspace_id validation error, got %v", err)
	}
}

func TestExecutorUndeployRejectsBlankToolIDWithoutRemovingDefault(t *testing.T) {
	t.Parallel()

	const workspaceID = "ws-executor-undeploy-blank"
	executor := NewExecutor(t.TempDir())
	if _, err := executor.Deploy(DeployInput{
		WorkspaceID: workspaceID,
		ToolID:      "default",
		Runtime:     "node",
		SourceCode:  `console.log("must remain")`,
		EntryPoint:  "main.js",
	}); err != nil {
		t.Fatalf("deploy default tool: %v", err)
	}

	if err := executor.Undeploy(workspaceID, "   "); err == nil || !strings.Contains(err.Error(), "tool_id is required") {
		t.Fatalf("expected blank tool_id validation error, got %v", err)
	}
	deployed, err := executor.IsDeployed(workspaceID, "default")
	if err != nil {
		t.Fatalf("check default deployment: %v", err)
	}
	if !deployed {
		t.Fatalf("blank undeploy removed sanitized default deployment")
	}
}

func TestExecutorDeployMetadataWriteFailureRollsBackScript(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	executor := NewExecutor(root)
	const workspaceID = "ws-metadata-failure"
	const toolID = "tool-metadata-failure"

	toolDir := filepath.Join(root, "tools", sanitize(workspaceID), sanitize(toolID))
	if err := os.MkdirAll(filepath.Join(toolDir, "tool.json"), 0o755); err != nil {
		t.Fatalf("seed metadata directory: %v", err)
	}

	_, err := executor.Deploy(DeployInput{
		WorkspaceID: workspaceID,
		ToolID:      toolID,
		Runtime:     "node",
		EntryPoint:  "main.js",
		SourceCode:  `console.log("should not remain after failed deploy")`,
	})
	if err == nil || !strings.Contains(err.Error(), "write tool metadata") {
		t.Fatalf("expected metadata write failure, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(toolDir, "main.js")); !os.IsNotExist(statErr) {
		t.Fatalf("failed deploy left source script on disk, statErr=%v", statErr)
	}
}
