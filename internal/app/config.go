package app

import (
	"os"
	"path/filepath"
	"strconv"
)

type Config struct {
	DBPath               string
	WorkspaceRoot        string
	MetricsPath          string
	ExecutorPython       string
	ExecutorBridgeScript string

	// LLM provider configuration (provider-agnostic)
	LLMProvider  string // "claude" (default), "openai"
	LLMAPIKey    string
	LLMModel     string
	LLMMaxTokens int
	LLMBaseURL   string
	LLMTimeout   int
	LLMHeaders   map[string]string // extra headers (e.g., for OpenRouter)
}

func LoadConfig() Config {
	dbPath := os.Getenv("RHIZOME_DB")
	if dbPath == "" {
		dbPath = "data/rhizome.db"
	}

	workspaceRoot := os.Getenv("RHIZOME_WORKSPACE_ROOT")
	if workspaceRoot == "" {
		workspaceRoot = "data/workspace"
	}
	metricsPath := os.Getenv("RHIZOME_METRICS_PATH")
	if metricsPath == "" {
		metricsPath = filepath.Join(filepath.Dir(workspaceRoot), "metrics.jsonl")
	}

	executorPython := os.Getenv("RHIZOME_EXECUTOR_PYTHON")
	if executorPython == "" {
		executorPython = "python"
	}

	executorBridgeScript := os.Getenv("RHIZOME_EXECUTOR_BRIDGE_SCRIPT")
	if executorBridgeScript == "" {
		executorBridgeScript = "internal/executor/rpc_bridge.py"
	}

	// LLM provider selection
	llmProvider := os.Getenv("RHIZOME_LLM_PROVIDER")
	if llmProvider == "" {
		llmProvider = "claude"
	}

	// API key: try provider-specific env vars, then generic fallback
	llmAPIKey := os.Getenv("RHIZOME_LLM_API_KEY")
	if llmAPIKey == "" {
		switch llmProvider {
		case "openai":
			llmAPIKey = os.Getenv("OPENAI_API_KEY")
			if llmAPIKey == "" {
				// Support Codex CLI auth token
				llmAPIKey = os.Getenv("OPENAI_CODEX_API_KEY")
			}
		default: // claude
			llmAPIKey = os.Getenv("ANTHROPIC_API_KEY")
		}
	}

	// Model: provider-specific defaults applied by the provider itself,
	// but allow override via env var
	llmModel := os.Getenv("RHIZOME_LLM_MODEL")

	// Base URL override
	llmBaseURL := os.Getenv("RHIZOME_LLM_BASE_URL")

	llmMaxTokens := envInt("RHIZOME_LLM_MAX_TOKENS", 8192)
	llmTimeout := envInt("RHIZOME_LLM_TIMEOUT", 120)

	// Extra headers (e.g., for OpenRouter)
	var llmHeaders map[string]string
	if referer := os.Getenv("RHIZOME_LLM_HTTP_REFERER"); referer != "" {
		llmHeaders = map[string]string{"HTTP-Referer": referer}
	}

	return Config{
		DBPath:               dbPath,
		WorkspaceRoot:        workspaceRoot,
		MetricsPath:          metricsPath,
		ExecutorPython:       executorPython,
		ExecutorBridgeScript: executorBridgeScript,
		LLMProvider:          llmProvider,
		LLMAPIKey:            llmAPIKey,
		LLMModel:             llmModel,
		LLMMaxTokens:         llmMaxTokens,
		LLMBaseURL:           llmBaseURL,
		LLMTimeout:           llmTimeout,
		LLMHeaders:           llmHeaders,
	}
}

func envInt(key string, defaultVal int) int {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return defaultVal
	}
	return v
}
