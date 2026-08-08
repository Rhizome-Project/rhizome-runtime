package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
)

func runAgentInternal(args []string) error {
	fs := flag.NewFlagSet("agent run-internal", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	agentID := fs.String("agent-id", "", "Agent identifier (required)")
	workspaceID := fs.String("workspace-id", "", "Workspace to operate in (required)")
	task := fs.String("task", "", "Task description to execute")
	taskFile := fs.String("task-file", "", "Path to file containing task description")
	maxIterations := fs.Int("max-iterations", 50, "Maximum LLM iterations")
	modelOverride := fs.String("model", "", "Override LLM model from config")
	providerOverride := fs.String("provider", "", "Override LLM provider: claude, openai")
	outputFormat := fs.String("format", "json", "Output format: json or jsonl")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Validate agent-id and workspace-id
	if strings.TrimSpace(*agentID) == "" {
		return errors.New("--agent-id is required")
	}
	if strings.TrimSpace(*workspaceID) == "" {
		return errors.New("--workspace-id is required")
	}

	// Validate task source
	taskText, err := resolveTaskText(*task, *taskFile)
	if err != nil {
		return err
	}

	// Load config
	cfg := app.LoadConfig()

	// Apply CLI overrides
	provider := cfg.LLMProvider
	if *providerOverride != "" {
		provider = *providerOverride
	}

	model := cfg.LLMModel
	if *modelOverride != "" {
		model = *modelOverride
	}

	// Check API key
	if cfg.LLMAPIKey == "" {
		switch provider {
		case "openai":
			return errors.New("LLM API key not configured. Set OPENAI_API_KEY, OPENAI_CODEX_API_KEY, or RHIZOME_LLM_API_KEY.")
		default:
			return errors.New("LLM API key not configured. Set ANTHROPIC_API_KEY or RHIZOME_LLM_API_KEY.")
		}
	}

	// Create LLM client
	llmClient := llm.NewClient(llm.ClientConfig{
		Provider:  llm.ProviderType(provider),
		APIKey:    cfg.LLMAPIKey,
		Model:     model,
		MaxTokens: cfg.LLMMaxTokens,
		Timeout:   time.Duration(cfg.LLMTimeout) * time.Second,
		BaseURL:   cfg.LLMBaseURL,
		Headers:   cfg.LLMHeaders,
	})

	// Open store
	store, err := openStore()
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() { _ = store.Close() }()

	// Apply migrations
	migCtx, migCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer migCancel()
	if err := store.ApplyMigrations(migCtx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	// Create agent
	internalAgent, err := agent.NewInternalAgent(store, llmClient, agent.AgentConfig{
		ID:          strings.TrimSpace(*agentID),
		WorkspaceID: strings.TrimSpace(*workspaceID),
		LoopConfig: agent.LoopConfig{
			MaxIterations: *maxIterations,
		},
	})
	if err != nil {
		return fmt.Errorf("create agent: %w", err)
	}

	// Set up signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nInterrupted, stopping agent...")
		cancel()
	}()

	// Run agent
	result, err := internalAgent.Run(ctx, taskText)
	if err != nil {
		return fmt.Errorf("agent run: %w", err)
	}

	// Output result
	output := map[string]any{
		"session_id":          fmt.Sprintf("sess-%s-%d", *agentID, time.Now().UnixNano()),
		"agent_id":            *agentID,
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

func resolveTaskText(task, taskFile string) (string, error) {
	hasTask := strings.TrimSpace(task) != ""
	hasFile := strings.TrimSpace(taskFile) != ""

	if hasTask && hasFile {
		return "", errors.New("specify --task or --task-file, not both")
	}
	if !hasTask && !hasFile {
		return "", errors.New("specify --task or --task-file")
	}

	if hasFile {
		data, err := os.ReadFile(taskFile)
		if err != nil {
			return "", fmt.Errorf("read task file %s: %w", taskFile, err)
		}
		return strings.TrimSpace(string(data)), nil
	}

	return strings.TrimSpace(task), nil
}

// Ensure json import is used (writeJSON uses encoding/json from helpers).
var _ = json.Marshal
