package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runConfig(args []string) error {
	if len(args) < 1 {
		printConfigUsage(os.Stderr)
		return errors.New("missing config subcommand")
	}

	switch args[0] {
	case "show":
		return runConfigShow(args[1:])
	case "save":
		return runConfigSave(args[1:])
	case "load":
		return runConfigLoad(args[1:])
	default:
		printConfigUsage(os.Stderr)
		return fmt.Errorf("unknown config subcommand: %s", args[0])
	}
}

func runConfigShow(args []string) error {
	fs := flag.NewFlagSet("config show", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	_ = fs.String("format", "json", "Output format (only json supported)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := app.LoadConfig()

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

	exportCfg, err := buildExportFromStore(ctx, cfg, store)
	if err != nil {
		return err
	}

	data, err := app.MarshalExportConfig(exportCfg)
	if err != nil {
		return fmt.Errorf("marshal export config: %w", err)
	}
	_, err = os.Stdout.Write(append(data, '\n'))
	return err
}

func runConfigSave(args []string) error {
	fs := flag.NewFlagSet("config save", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 1 {
		return errors.New("file path is required")
	}
	filePath := fs.Arg(0)

	cfg := app.LoadConfig()

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

	exportCfg, err := buildExportFromStore(ctx, cfg, store)
	if err != nil {
		return err
	}

	data, err := app.MarshalExportConfig(exportCfg)
	if err != nil {
		return fmt.Errorf("marshal export config: %w", err)
	}

	// Create parent directories if needed.
	dir := filepath.Dir(filePath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("create parent directories: %w", err)
		}
	}

	if err := os.WriteFile(filePath, append(data, '\n'), 0644); err != nil {
		return fmt.Errorf("write config file: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"saved_to": filePath,
	})
}

func runConfigLoad(args []string) error {
	fs := flag.NewFlagSet("config load", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dryRun := fs.Bool("dry-run", false, "Parse and validate without making changes")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 1 {
		return errors.New("file path is required")
	}
	filePath := fs.Arg(0)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read config file %s: %w", filePath, err)
	}

	exportCfg, err := app.UnmarshalExportConfig(data)
	if err != nil {
		return err
	}

	if *dryRun {
		return writeJSON(os.Stdout, map[string]any{
			"loaded":             true,
			"dry_run":            true,
			"agents_created":     0,
			"workspaces_created": 0,
			"skipped":            len(exportCfg.Agents) + len(exportCfg.Workspaces),
		})
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	// Load existing workspaces to detect duplicates.
	existingWorkspaces, err := store.ListWorkspaces(ctx, 1000)
	if err != nil {
		return fmt.Errorf("list workspaces: %w", err)
	}
	wsExists := make(map[string]bool, len(existingWorkspaces))
	for _, ws := range existingWorkspaces {
		wsExists[ws.WorkspaceID] = true
	}

	// Load existing agent definitions to detect duplicates.
	existingAgents, err := store.ListAgentDefinitions(ctx, "")
	if err != nil {
		return fmt.Errorf("list agent definitions: %w", err)
	}
	type agentKey struct {
		name        string
		workspaceID string
	}
	agentExists := make(map[agentKey]bool, len(existingAgents))
	for _, a := range existingAgents {
		agentExists[agentKey{name: a.Name, workspaceID: a.WorkspaceID}] = true
	}

	workspacesCreated := 0
	agentsCreated := 0
	skipped := 0
	workspacePassword := strings.TrimSpace(os.Getenv("RHIZOME_WORKSPACE_PASSWORD"))

	// Create workspaces first.
	for _, ws := range exportCfg.Workspaces {
		if wsExists[ws.WorkspaceID] {
			fmt.Fprintf(os.Stderr, "skipping workspace %s: already exists\n", ws.WorkspaceID)
			skipped++
			continue
		}
		if workspacePassword == "" {
			return errors.New("RHIZOME_WORKSPACE_PASSWORD is required when importing new workspaces")
		}
		err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID:       ws.WorkspaceID,
			Title:             ws.Title,
			Description:       ws.Description,
			CreatedBy:         "config-load",
			Status:            "active",
			WorkspacePassword: workspacePassword,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create workspace %s: %v\n", ws.WorkspaceID, err)
			skipped++
			continue
		}
		workspacesCreated++
	}

	// Create agent definitions.
	for _, agent := range exportCfg.Agents {
		key := agentKey{name: agent.Name, workspaceID: agent.WorkspaceID}
		if agentExists[key] {
			fmt.Fprintf(os.Stderr, "skipping agent %s (workspace %s): already exists\n", agent.Name, agent.WorkspaceID)
			skipped++
			continue
		}
		maxIter := agent.MaxIterations
		if maxIter <= 0 {
			maxIter = 50
		}
		provider := strings.TrimSpace(agent.Provider)
		if provider == "" {
			provider = "claude"
		}
		_, err := store.CreateAgentDefinition(ctx, sqlite.AgentDefinitionCreateInput{
			Name:          agent.Name,
			Provider:      provider,
			Model:         agent.Model,
			SystemPrompt:  agent.SystemPrompt,
			Tools:         agent.Tools,
			MaxIterations: maxIter,
			WorkspaceID:   agent.WorkspaceID,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create agent %s: %v\n", agent.Name, err)
			skipped++
			continue
		}
		agentsCreated++
	}

	return writeJSON(os.Stdout, map[string]any{
		"loaded":             true,
		"agents_created":     agentsCreated,
		"workspaces_created": workspacesCreated,
		"skipped":            skipped,
	})
}

func buildExportFromStore(ctx context.Context, cfg app.Config, store *sqlite.Store) (app.ExportConfig, error) {
	// Load all agent definitions.
	agentRecords, err := store.ListAgentDefinitions(ctx, "")
	if err != nil {
		return app.ExportConfig{}, fmt.Errorf("list agent definitions: %w", err)
	}
	agents := make([]app.AgentExportConfig, 0, len(agentRecords))
	for _, a := range agentRecords {
		agents = append(agents, app.AgentExportConfig{
			Name:          a.Name,
			Provider:      a.Provider,
			Model:         a.Model,
			SystemPrompt:  a.SystemPrompt,
			Tools:         a.Tools,
			MaxIterations: a.MaxIterations,
			WorkspaceID:   a.WorkspaceID,
		})
	}

	// Load all workspaces.
	wsRecords, err := store.ListWorkspaces(ctx, 1000)
	if err != nil {
		return app.ExportConfig{}, fmt.Errorf("list workspaces: %w", err)
	}
	workspaces := make([]app.WorkspaceExportConfig, 0, len(wsRecords))
	for _, ws := range wsRecords {
		workspaces = append(workspaces, app.WorkspaceExportConfig{
			WorkspaceID: ws.WorkspaceID,
			Title:       ws.Title,
			Description: ws.Description,
		})
	}

	return app.BuildExportConfig(cfg, agents, workspaces), nil
}

func printConfigUsage(out *os.File) {
	fmt.Fprintln(out, "Config commands:")
	fmt.Fprintln(out, "  rhizome config show [--format json]")
	fmt.Fprintln(out, "  rhizome config save <file>")
	fmt.Fprintln(out, "  rhizome config load <file> [--dry-run]")
}
