package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/team"
	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
)

func runAgentRunTeam(args []string) error {
	fs := flag.NewFlagSet("agent run-team", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	configPath := fs.String("config", "", "Path to team JSON config (required)")
	task := fs.String("task", "", "Task description")
	taskFile := fs.String("task-file", "", "Path to file containing task description")
	format := fs.String("format", "json", "Output format: json")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Validate --config is provided (EC-1).
	if *configPath == "" {
		return errors.New("config is required")
	}

	// Resolve task text (EC-2).
	taskText, err := resolveTaskText(*task, *taskFile)
	if err != nil {
		return err
	}

	// Load app config.
	cfg := app.LoadConfig()

	// Open store and apply migrations.
	store, err := openStore()
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() { _ = store.Close() }()

	migCtx, migCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer migCancel()
	if err := store.ApplyMigrations(migCtx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	// Set up signal handling (EC-4).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nInterrupted, stopping team run...")
		cancel()
	}()

	// Run team (EC-3).
	fmt.Fprintf(os.Stderr, "INFO: Running team in Local Utility Mode [Synchronous, Non-Daemon]\n")
	result, err := team.RunTeam(ctx, team.RunTeamOpts{
		ConfigPath: *configPath,
		Task:       taskText,
		Store:      store,
		AppConfig:  cfg,
	})
	if err != nil {
		// R-5: Output error as JSON to stdout.
		_ = writeJSON(os.Stdout, map[string]any{
			"error": err.Error(),
		})
		return err
	}

	_ = *format // reserved for future format support

	// R-4: Output full TeamResult as JSON.
	return writeJSON(os.Stdout, result)
}

// Ensure json import is used.
var _ = json.Marshal
