package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/Rhizome-Project/rhizome-runtime/internal/tools"
)

func runToolRemove(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("tool remove", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	toolID := fs.String("tool-id", "", "Tool identifier")
	removedBy := fs.String("removed-by", "", "Optional actor recorded in the audit trail")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	if err := ensureToolRegistryRemovalAllowed(*workspaceID, *toolID); err != nil {
		return err
	}

	existed, err := store.RemoveWorkspaceTool(ctx, sqlite.WorkspaceToolRemoveInput{
		WorkspaceID: *workspaceID,
		ToolID:      *toolID,
		RemovedBy:   *removedBy,
		PromptContextEnvelope: cliToolRegistryPromptContextEnvelope(
			"cli.tool.remove",
			*workspaceID,
			*removedBy,
		),
		PromptContextSurface:       "cli.tool.remove",
		PromptContextPrincipalType: "operator",
		PromptContextPrincipalID:   cliToolRegistryPrincipalID(*removedBy),
	})
	if err != nil {
		return fmt.Errorf("remove workspace tool: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"tool_id":      strings.TrimSpace(*toolID),
		"removed_by":   strings.TrimSpace(*removedBy),
		"existed":      existed,
		"status":       "REMOVED",
	})
}

func ensureToolRegistryRemovalAllowed(workspaceID, toolID string) error {
	deployed, err := workspaceToolExecutor().IsDeployed(workspaceID, toolID)
	if err != nil {
		return fmt.Errorf("check tool deployment: %w", err)
	}
	if deployed {
		return fmt.Errorf("tool %s is still deployed; call RPC tool.undeploy before removing it from the registry", strings.TrimSpace(toolID))
	}
	return nil
}

func workspaceToolExecutor() *tools.Executor {
	cfg := app.LoadConfig()
	return tools.NewExecutor(cfg.WorkspaceRoot)
}
