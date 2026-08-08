package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/auth"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runAgentRun(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: rhizome agent run <agent-id> [flags]\n\nagent-id is required")
	}

	// First positional arg is agent-id.
	agentID := strings.TrimSpace(args[0])
	if agentID == "" {
		return errors.New("agent-id is required")
	}

	// Parse remaining flags.
	fs := flag.NewFlagSet("agent run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	task := fs.String("task", "", "Task description to execute")
	taskFile := fs.String("task-file", "", "Path to file containing task description")
	maxIterations := fs.Int("max-iterations", 0, "Override max iterations from definition (0 = use definition value)")
	modelOverride := fs.String("model", "", "Override LLM model from definition")
	providerOverride := fs.String("provider", "", "Override LLM provider from definition: claude, openai")
	outputFormat := fs.String("format", "json", "Output format: json")

	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	// Resolve task text.
	taskText, err := resolveTaskText(*task, *taskFile)
	if err != nil {
		return err
	}

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

	// Load agent definition.
	defCtx, defCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer defCancel()
	def, err := store.GetAgentDefinition(defCtx, agentID)
	if err != nil {
		if errors.Is(err, sqlite.ErrAgentDefinitionNotFound) {
			return fmt.Errorf("agent definition not found: %s", agentID)
		}
		return fmt.Errorf("load agent definition: %w", err)
	}

	// Load app config for env-based fallbacks.
	cfg := app.LoadConfig()

	// Resolve provider: CLI flag > definition > env config.
	provider := def.Provider
	if provider == "" {
		provider = cfg.LLMProvider
	}
	if *providerOverride != "" {
		provider = *providerOverride
	}

	// Resolve model: CLI flag > definition > env config.
	model := def.Model
	if model == "" {
		model = cfg.LLMModel
	}
	if *modelOverride != "" {
		model = *modelOverride
	}

	// Resolve API key: env config first, then auth store fallback.
	apiKey := cfg.LLMAPIKey
	if apiKey == "" {
		apiKey, err = resolveAPIKeyFromAuthStore()
		if err != nil {
			log.Printf("[agent run] auth store fallback: %v", err)
		}
	}
	if apiKey == "" {
		switch provider {
		case "openai":
			return errors.New("LLM API key not configured. Set OPENAI_API_KEY, OPENAI_CODEX_API_KEY, or RHIZOME_LLM_API_KEY, or save credentials via rhizome auth.")
		default:
			return errors.New("LLM API key not configured. Set ANTHROPIC_API_KEY or RHIZOME_LLM_API_KEY, or save credentials via rhizome auth.")
		}
	}

	// Resolve max iterations: CLI flag > definition.
	iterations := def.MaxIterations
	if *maxIterations > 0 {
		iterations = *maxIterations
	}

	// Create LLM client.
	llmClient := llm.NewClient(llm.ClientConfig{
		Provider:  llm.ProviderType(provider),
		APIKey:    apiKey,
		Model:     model,
		MaxTokens: cfg.LLMMaxTokens,
		Timeout:   time.Duration(cfg.LLMTimeout) * time.Second,
		BaseURL:   cfg.LLMBaseURL,
		Headers:   cfg.LLMHeaders,
	})

	// Create agent.
	internalAgent, err := agent.NewInternalAgent(store, llmClient, agent.AgentConfig{
		ID:           def.ID,
		WorkspaceID:  def.WorkspaceID,
		StaticPrompt: def.SystemPrompt,
		LoopConfig: agent.LoopConfig{
			MaxIterations: iterations,
		},
	})
	if err != nil {
		return fmt.Errorf("create agent: %w", err)
	}

	// Set up signal handling.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nInterrupted, stopping agent...")
		cancel()
	}()

	// Run agent.
	result, err := internalAgent.Run(ctx, taskText)
	if err != nil {
		return fmt.Errorf("agent run: %w", err)
	}

	// Output result.
	output := map[string]any{
		"session_id":          fmt.Sprintf("sess-%s-%d", def.ID, time.Now().UnixNano()),
		"agent_id":            def.ID,
		"status":              "COMPLETED",
		"iterations":          result.Iterations,
		"tool_calls":          result.ToolCalls,
		"total_input_tokens":  result.TotalInputTokens,
		"total_output_tokens": result.TotalOutputTokens,
		"stop_reason":         string(result.StopReason),
		"final_response":      result.FinalResponse,
		"error":               nil,
	}
	if result.Error != nil {
		output["status"] = "FAILED"
		output["error"] = result.Error.Error()
	}

	_ = *outputFormat // reserved for future jsonl support

	return writeJSON(os.Stdout, output)
}

// resolveAPIKeyFromAuthStore attempts to load an API key from the saved
// credentials file (~/.rhizome/auth.json).
func resolveAPIKeyFromAuthStore() (string, error) {
	path := auth.DefaultAuthFilePath()
	if path == "" {
		return "", errors.New("cannot determine auth file path")
	}
	creds, err := auth.LoadCredentials(path)
	if err != nil {
		return "", err
	}
	if creds.APIKey == "" {
		return "", errors.New("auth credentials file has no api_key")
	}
	log.Printf("[agent run] using API key from %s", path)
	return creds.APIKey, nil
}
