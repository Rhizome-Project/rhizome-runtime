package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runLiving(args []string) error {
	if len(args) < 1 || args[0] != "run" {
		printLivingUsage(os.Stderr)
		return errors.New("missing living subcommand")
	}

	fs := flag.NewFlagSet("living run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	remaining := fs.Args()
	if len(remaining) < 1 {
		return errors.New("usage: rhizome living run <config.yaml>")
	}
	configPath := remaining[0]

	cfg, err := living.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := living.ValidateRunContract(*cfg); err != nil {
		return err
	}

	appCfg := app.LoadConfig()
	store, err := sqlite.NewStore(appCfg.DBPath)
	if err != nil {
		return fmt.Errorf("open rhizome store: %w", err)
	}
	defer func() { _ = store.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	if _, err := store.EnsureAgentRegistered(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: cfg.WorkspaceID,
		AgentID:     cfg.ID,
		OwnerUserID: "living",
		DisplayName: cfg.ID,
		Role:        cfg.Role,
		Status:      model.AgentStatusRegistered,
	}); err != nil {
		return fmt.Errorf("register living agent: %w", err)
	}

	rhizome := living.NewDirectRhizomeClient(store, cfg.WorkspaceID)
	rhizome.SetAgentID(cfg.ID)
	brain, err := living.NewBrain(*cfg, &living.BrainDeps{
		Rhizome: rhizome,
	})
	if err != nil {
		return fmt.Errorf("create brain: %w", err)
	}

	fmt.Fprintf(os.Stderr, "WARNING: [DEPRECATED] 'rhizome living run' invokes the experimental non-canonical runtime.\n")
	fmt.Fprintf(os.Stderr, "         This command is slated for complete removal in Phase 4.\n")
	fmt.Fprintf(os.Stderr, "         The canonical execution pathway is 'rhizome agent run'.\n\n")
	fmt.Fprintf(os.Stderr, "Living Agent (Experimental) starting...\n")
	fmt.Fprintf(os.Stderr, "  ID:        %s\n", cfg.ID)
	fmt.Fprintf(os.Stderr, "  Role:      %s\n", cfg.Role)
	fmt.Fprintf(os.Stderr, "  Mode:      %s\n", cfg.RuntimeMode())
	fmt.Fprintf(os.Stderr, "  Workspace: %s\n", cfg.WorkspaceID)
	if cfg.RedisURL != "" {
		fmt.Fprintf(os.Stderr, "  Redis:     %s\n", cfg.RedisURL)
	}
	fmt.Fprintf(os.Stderr, "\n")

	runErr := brain.Run(ctx)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	_ = brain.Shutdown(shutdownCtx)

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return nil
}

func printLivingUsage(out *os.File) {
	fmt.Fprintln(out, "Living agent commands (Experimental/Deprecated):")
	fmt.Fprintln(out, "  rhizome living run <config.yaml>   # Warning: Non-canonical runtime. See 'rhizome agent run'.")
}
