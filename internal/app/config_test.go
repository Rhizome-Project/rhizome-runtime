package app

import (
	"testing"
)

// T-1: Verifies defaults when no env vars set
func TestLoadConfig_LLMDefaults(t *testing.T) {
	t.Setenv("RHIZOME_LLM_PROVIDER", "")
	t.Setenv("RHIZOME_LLM_API_KEY", "")
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("OPENAI_CODEX_API_KEY", "")
	t.Setenv("RHIZOME_LLM_MODEL", "")
	t.Setenv("RHIZOME_LLM_MAX_TOKENS", "")
	t.Setenv("RHIZOME_LLM_BASE_URL", "")
	t.Setenv("RHIZOME_LLM_TIMEOUT", "")
	t.Setenv("RHIZOME_LLM_HTTP_REFERER", "")

	cfg := LoadConfig()

	if cfg.LLMProvider != "claude" {
		t.Fatalf("expected provider %q, got %q", "claude", cfg.LLMProvider)
	}
	if cfg.LLMAPIKey != "" {
		t.Fatalf("expected empty API key, got %q", cfg.LLMAPIKey)
	}
	if cfg.LLMModel != "" {
		t.Fatalf("expected empty model (provider sets default), got %q", cfg.LLMModel)
	}
	if cfg.LLMMaxTokens != 8192 {
		t.Fatalf("expected max tokens 8192, got %d", cfg.LLMMaxTokens)
	}
	if cfg.LLMTimeout != 120 {
		t.Fatalf("expected timeout 120, got %d", cfg.LLMTimeout)
	}
}

// T-2: Claude provider picks up ANTHROPIC_API_KEY
func TestLoadConfig_ClaudeAPIKey(t *testing.T) {
	t.Setenv("RHIZOME_LLM_PROVIDER", "claude")
	t.Setenv("RHIZOME_LLM_API_KEY", "")
	t.Setenv("ANTHROPIC_API_KEY", "sk-anthropic")

	cfg := LoadConfig()

	if cfg.LLMAPIKey != "sk-anthropic" {
		t.Fatalf("expected %q, got %q", "sk-anthropic", cfg.LLMAPIKey)
	}
}

// T-3: OpenAI provider picks up OPENAI_API_KEY
func TestLoadConfig_OpenAIAPIKey(t *testing.T) {
	t.Setenv("RHIZOME_LLM_PROVIDER", "openai")
	t.Setenv("RHIZOME_LLM_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "sk-openai")
	t.Setenv("OPENAI_CODEX_API_KEY", "")

	cfg := LoadConfig()

	if cfg.LLMAPIKey != "sk-openai" {
		t.Fatalf("expected %q, got %q", "sk-openai", cfg.LLMAPIKey)
	}
}

// T-4: OpenAI Codex CLI auth token fallback
func TestLoadConfig_CodexAPIKey(t *testing.T) {
	t.Setenv("RHIZOME_LLM_PROVIDER", "openai")
	t.Setenv("RHIZOME_LLM_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("OPENAI_CODEX_API_KEY", "sk-codex-token")

	cfg := LoadConfig()

	if cfg.LLMAPIKey != "sk-codex-token" {
		t.Fatalf("expected %q, got %q", "sk-codex-token", cfg.LLMAPIKey)
	}
}

// T-5: RHIZOME_LLM_API_KEY takes precedence over provider-specific keys
func TestLoadConfig_GenericKeyPrecedence(t *testing.T) {
	t.Setenv("RHIZOME_LLM_PROVIDER", "openai")
	t.Setenv("RHIZOME_LLM_API_KEY", "sk-generic")
	t.Setenv("OPENAI_API_KEY", "sk-openai")

	cfg := LoadConfig()

	if cfg.LLMAPIKey != "sk-generic" {
		t.Fatalf("expected %q, got %q", "sk-generic", cfg.LLMAPIKey)
	}
}

// T-6: Custom values from env
func TestLoadConfig_LLMCustomValues(t *testing.T) {
	t.Setenv("RHIZOME_LLM_PROVIDER", "openai")
	t.Setenv("RHIZOME_LLM_API_KEY", "sk-custom")
	t.Setenv("RHIZOME_LLM_MODEL", "gpt-4o")
	t.Setenv("RHIZOME_LLM_MAX_TOKENS", "4096")
	t.Setenv("RHIZOME_LLM_BASE_URL", "https://openrouter.ai/api")
	t.Setenv("RHIZOME_LLM_TIMEOUT", "60")
	t.Setenv("RHIZOME_LLM_HTTP_REFERER", "https://myapp.com")

	cfg := LoadConfig()

	if cfg.LLMProvider != "openai" {
		t.Fatalf("provider: expected %q, got %q", "openai", cfg.LLMProvider)
	}
	if cfg.LLMModel != "gpt-4o" {
		t.Fatalf("model: expected %q, got %q", "gpt-4o", cfg.LLMModel)
	}
	if cfg.LLMMaxTokens != 4096 {
		t.Fatalf("max tokens: expected 4096, got %d", cfg.LLMMaxTokens)
	}
	if cfg.LLMBaseURL != "https://openrouter.ai/api" {
		t.Fatalf("base URL: expected %q, got %q", "https://openrouter.ai/api", cfg.LLMBaseURL)
	}
	if cfg.LLMTimeout != 60 {
		t.Fatalf("timeout: expected 60, got %d", cfg.LLMTimeout)
	}
	if cfg.LLMHeaders == nil || cfg.LLMHeaders["HTTP-Referer"] != "https://myapp.com" {
		t.Fatalf("expected HTTP-Referer header, got %v", cfg.LLMHeaders)
	}
}

// T-7: Existing non-LLM fields unchanged
func TestLoadConfig_ExistingFieldsUnchanged(t *testing.T) {
	t.Setenv("RHIZOME_DB", "custom/path.db")
	t.Setenv("RHIZOME_WORKSPACE_ROOT", "/custom/workspace")
	t.Setenv("RHIZOME_EXECUTOR_PYTHON", "python3")
	t.Setenv("RHIZOME_LLM_API_KEY", "")
	t.Setenv("RHIZOME_LLM_PROVIDER", "")
	t.Setenv("ANTHROPIC_API_KEY", "")

	cfg := LoadConfig()

	if cfg.DBPath != "custom/path.db" {
		t.Fatalf("DBPath: expected %q, got %q", "custom/path.db", cfg.DBPath)
	}
	if cfg.WorkspaceRoot != "/custom/workspace" {
		t.Fatalf("WorkspaceRoot: expected %q, got %q", "/custom/workspace", cfg.WorkspaceRoot)
	}
	if cfg.ExecutorPython != "python3" {
		t.Fatalf("ExecutorPython: expected %q, got %q", "python3", cfg.ExecutorPython)
	}
}

// NT-1: Invalid int uses default
func TestLoadConfig_InvalidInt(t *testing.T) {
	t.Setenv("RHIZOME_LLM_PROVIDER", "")
	t.Setenv("RHIZOME_LLM_API_KEY", "")
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("RHIZOME_LLM_MODEL", "")
	t.Setenv("RHIZOME_LLM_MAX_TOKENS", "abc")
	t.Setenv("RHIZOME_LLM_BASE_URL", "")
	t.Setenv("RHIZOME_LLM_TIMEOUT", "not-a-number")

	cfg := LoadConfig()

	if cfg.LLMMaxTokens != 8192 {
		t.Fatalf("expected default max tokens 8192, got %d", cfg.LLMMaxTokens)
	}
	if cfg.LLMTimeout != 120 {
		t.Fatalf("expected default timeout 120, got %d", cfg.LLMTimeout)
	}
}

// NT-2: Negative values use default
func TestLoadConfig_NegativeValues(t *testing.T) {
	t.Setenv("RHIZOME_LLM_PROVIDER", "")
	t.Setenv("RHIZOME_LLM_API_KEY", "")
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("RHIZOME_LLM_MODEL", "")
	t.Setenv("RHIZOME_LLM_MAX_TOKENS", "-100")
	t.Setenv("RHIZOME_LLM_BASE_URL", "")
	t.Setenv("RHIZOME_LLM_TIMEOUT", "-5")

	cfg := LoadConfig()

	if cfg.LLMMaxTokens != 8192 {
		t.Fatalf("expected default max tokens 8192, got %d", cfg.LLMMaxTokens)
	}
	if cfg.LLMTimeout != 120 {
		t.Fatalf("expected default timeout 120, got %d", cfg.LLMTimeout)
	}
}

// NT-3: Zero values accepted
func TestLoadConfig_ZeroIntValues(t *testing.T) {
	t.Setenv("RHIZOME_LLM_PROVIDER", "")
	t.Setenv("RHIZOME_LLM_API_KEY", "")
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("RHIZOME_LLM_MODEL", "")
	t.Setenv("RHIZOME_LLM_MAX_TOKENS", "0")
	t.Setenv("RHIZOME_LLM_BASE_URL", "")
	t.Setenv("RHIZOME_LLM_TIMEOUT", "0")

	cfg := LoadConfig()

	if cfg.LLMMaxTokens != 0 {
		t.Fatalf("expected 0 max tokens, got %d", cfg.LLMMaxTokens)
	}
	if cfg.LLMTimeout != 0 {
		t.Fatalf("expected 0 timeout, got %d", cfg.LLMTimeout)
	}
}
