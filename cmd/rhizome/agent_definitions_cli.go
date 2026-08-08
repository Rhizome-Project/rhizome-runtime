package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runAgentCreate(args []string) error {
	fs := flag.NewFlagSet("agent create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	name := fs.String("name", "", "Agent display name (required)")
	provider := fs.String("provider", "claude", "LLM provider: claude or openai")
	model := fs.String("model", "", "Model name")
	prompt := fs.String("prompt", "", "System prompt text")
	promptFile := fs.String("prompt-file", "", "Path to file containing system prompt")
	tools := fs.String("tools", "", "Comma-separated list of tool names")
	maxIterations := fs.Int("max-iterations", 50, "Max LLM iterations")
	workspaceID := fs.String("workspace-id", "", "Workspace ID (required)")
	_ = fs.String("format", "json", "Output format")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*name) == "" {
		return errors.New("name is required")
	}
	if strings.TrimSpace(*workspaceID) == "" {
		return errors.New("workspace-id is required")
	}

	hasPrompt := strings.TrimSpace(*prompt) != ""
	hasFile := strings.TrimSpace(*promptFile) != ""
	if hasPrompt && hasFile {
		return errors.New("specify --prompt or --prompt-file, not both")
	}

	systemPrompt, err := readTextValue(*prompt, *promptFile)
	if err != nil {
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

	var toolList []string
	if strings.TrimSpace(*tools) != "" {
		toolList = parseCSV(*tools)
	}

	rec, err := store.CreateAgentDefinition(ctx, sqlite.AgentDefinitionCreateInput{
		Name:          strings.TrimSpace(*name),
		Provider:      strings.TrimSpace(*provider),
		Model:         strings.TrimSpace(*model),
		SystemPrompt:  systemPrompt,
		Tools:         toolList,
		MaxIterations: *maxIterations,
		WorkspaceID:   strings.TrimSpace(*workspaceID),
	})
	if err != nil {
		return fmt.Errorf("create agent definition: %w", err)
	}

	return writeJSON(os.Stdout, rec)
}

func runAgentList(args []string) error {
	fs := flag.NewFlagSet("agent list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Filter by workspace ID")
	_ = fs.String("format", "json", "Output format")

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

	defs, err := store.ListAgentDefinitions(ctx, strings.TrimSpace(*workspaceID))
	if err != nil {
		return fmt.Errorf("list agent definitions: %w", err)
	}

	return writeJSON(os.Stdout, defs)
}

func runAgentShow(args []string) error {
	fs := flag.NewFlagSet("agent show", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	id := fs.String("id", "", "Agent definition ID")
	_ = fs.String("format", "json", "Output format")

	if err := fs.Parse(args); err != nil {
		return err
	}

	agentID := strings.TrimSpace(*id)
	if agentID == "" {
		// Try positional argument
		if fs.NArg() > 0 {
			agentID = strings.TrimSpace(fs.Arg(0))
		}
	}
	if agentID == "" {
		return errors.New("agent definition ID is required (positional arg or --id)")
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

	rec, err := store.GetAgentDefinition(ctx, agentID)
	if err != nil {
		if errors.Is(err, sqlite.ErrAgentDefinitionNotFound) {
			return errors.New("agent definition not found")
		}
		return fmt.Errorf("get agent definition: %w", err)
	}

	return writeJSON(os.Stdout, rec)
}

func runAgentDelete(args []string) error {
	fs := flag.NewFlagSet("agent delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	id := fs.String("id", "", "Agent definition ID")
	force := fs.Bool("force", false, "Skip confirmation prompt")
	_ = fs.String("format", "json", "Output format")

	if err := fs.Parse(args); err != nil {
		return err
	}

	agentID := strings.TrimSpace(*id)
	if agentID == "" {
		if fs.NArg() > 0 {
			agentID = strings.TrimSpace(fs.Arg(0))
		}
	}
	if agentID == "" {
		return errors.New("agent definition ID is required (positional arg or --id)")
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

	if !*force {
		// Look up the agent to show its name in the prompt
		rec, err := store.GetAgentDefinition(ctx, agentID)
		if err != nil {
			if errors.Is(err, sqlite.ErrAgentDefinitionNotFound) {
				return errors.New("agent definition not found")
			}
			return fmt.Errorf("get agent definition: %w", err)
		}

		fmt.Fprintf(os.Stderr, "Delete agent '%s' (%s)? [y/N]: ", rec.Name, rec.ID)
		var answer string
		fmt.Fscanln(os.Stdin, &answer)
		if strings.ToLower(strings.TrimSpace(answer)) != "y" {
			return writeJSON(os.Stdout, map[string]any{
				"deleted": false,
				"id":      agentID,
			})
		}
	}

	err = store.DeleteAgentDefinition(ctx, agentID)
	if err != nil {
		if errors.Is(err, sqlite.ErrAgentDefinitionNotFound) {
			return errors.New("agent definition not found")
		}
		return fmt.Errorf("delete agent definition: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"deleted": true,
		"id":      agentID,
	})
}
