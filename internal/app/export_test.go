package app

import (
	"encoding/json"
	"strings"
	"testing"
)

// T-1: Round-trip marshal/unmarshal preserves all fields.
func TestMarshalUnmarshalExportConfig_RoundTrip(t *testing.T) {
	original := ExportConfig{
		Version:    "1",
		ExportedAt: "2026-01-15T10:30:00Z",
		LLM: LLMExportConfig{
			Provider:  "claude",
			Model:     "claude-sonnet-4-20250514",
			MaxTokens: 8192,
			BaseURL:   "https://api.anthropic.com",
			Timeout:   120,
		},
		Agents: []AgentExportConfig{
			{
				Name:          "coder",
				Provider:      "claude",
				Model:         "claude-sonnet-4-20250514",
				SystemPrompt:  "You are a coding assistant.",
				Tools:         []string{"file_read", "file_write"},
				MaxIterations: 10,
				WorkspaceID:   "ws-001",
			},
		},
		Workspaces: []WorkspaceExportConfig{
			{
				WorkspaceID: "ws-001",
				Title:       "Main Workspace",
				Description: "Primary development workspace",
			},
		},
	}

	data, err := MarshalExportConfig(original)
	if err != nil {
		t.Fatalf("MarshalExportConfig failed: %v", err)
	}

	restored, err := UnmarshalExportConfig(data)
	if err != nil {
		t.Fatalf("UnmarshalExportConfig failed: %v", err)
	}

	// Verify top-level fields
	if restored.Version != original.Version {
		t.Errorf("Version: got %q, want %q", restored.Version, original.Version)
	}
	if restored.ExportedAt != original.ExportedAt {
		t.Errorf("ExportedAt: got %q, want %q", restored.ExportedAt, original.ExportedAt)
	}

	// Verify LLM config
	if restored.LLM.Provider != original.LLM.Provider {
		t.Errorf("LLM.Provider: got %q, want %q", restored.LLM.Provider, original.LLM.Provider)
	}
	if restored.LLM.Model != original.LLM.Model {
		t.Errorf("LLM.Model: got %q, want %q", restored.LLM.Model, original.LLM.Model)
	}
	if restored.LLM.MaxTokens != original.LLM.MaxTokens {
		t.Errorf("LLM.MaxTokens: got %d, want %d", restored.LLM.MaxTokens, original.LLM.MaxTokens)
	}
	if restored.LLM.BaseURL != original.LLM.BaseURL {
		t.Errorf("LLM.BaseURL: got %q, want %q", restored.LLM.BaseURL, original.LLM.BaseURL)
	}
	if restored.LLM.Timeout != original.LLM.Timeout {
		t.Errorf("LLM.Timeout: got %d, want %d", restored.LLM.Timeout, original.LLM.Timeout)
	}

	// Verify agents
	if len(restored.Agents) != 1 {
		t.Fatalf("Agents: got %d, want 1", len(restored.Agents))
	}
	a := restored.Agents[0]
	if a.Name != "coder" {
		t.Errorf("Agent.Name: got %q, want %q", a.Name, "coder")
	}
	if a.Provider != "claude" {
		t.Errorf("Agent.Provider: got %q, want %q", a.Provider, "claude")
	}
	if a.SystemPrompt != "You are a coding assistant." {
		t.Errorf("Agent.SystemPrompt: got %q", a.SystemPrompt)
	}
	if len(a.Tools) != 2 || a.Tools[0] != "file_read" || a.Tools[1] != "file_write" {
		t.Errorf("Agent.Tools: got %v", a.Tools)
	}
	if a.MaxIterations != 10 {
		t.Errorf("Agent.MaxIterations: got %d, want 10", a.MaxIterations)
	}
	if a.WorkspaceID != "ws-001" {
		t.Errorf("Agent.WorkspaceID: got %q, want %q", a.WorkspaceID, "ws-001")
	}

	// Verify workspaces
	if len(restored.Workspaces) != 1 {
		t.Fatalf("Workspaces: got %d, want 1", len(restored.Workspaces))
	}
	w := restored.Workspaces[0]
	if w.WorkspaceID != "ws-001" {
		t.Errorf("Workspace.WorkspaceID: got %q", w.WorkspaceID)
	}
	if w.Title != "Main Workspace" {
		t.Errorf("Workspace.Title: got %q", w.Title)
	}
	if w.Description != "Primary development workspace" {
		t.Errorf("Workspace.Description: got %q", w.Description)
	}
}

// T-2: Unsupported version returns error.
func TestUnmarshalExportConfig_InvalidVersion(t *testing.T) {
	data := []byte(`{"version":"2","exported_at":"2026-01-15T10:30:00Z","llm":{"provider":"claude","max_tokens":8192,"timeout":120},"agents":[]}`)

	_, err := UnmarshalExportConfig(data)
	if err == nil {
		t.Fatal("expected error for unsupported version, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported config version: 2") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// T-3: Malformed JSON returns descriptive error.
func TestUnmarshalExportConfig_MalformedJSON(t *testing.T) {
	data := []byte(`{not valid json}`)

	_, err := UnmarshalExportConfig(data)
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse export config") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// T-4: BuildExportConfig assembles fields correctly from app Config.
func TestBuildExportConfig(t *testing.T) {
	appCfg := Config{
		LLMProvider:  "openai",
		LLMModel:     "gpt-4",
		LLMMaxTokens: 4096,
		LLMBaseURL:   "https://api.openai.com",
		LLMTimeout:   60,
		LLMAPIKey:    "sk-secret-key-should-not-appear",
	}

	agents := []AgentExportConfig{
		{
			Name:          "reviewer",
			Provider:      "openai",
			Model:         "gpt-4",
			MaxIterations: 5,
			WorkspaceID:   "ws-100",
		},
	}

	workspaces := []WorkspaceExportConfig{
		{
			WorkspaceID: "ws-100",
			Title:       "Review Workspace",
		},
	}

	cfg := BuildExportConfig(appCfg, agents, workspaces)

	if cfg.Version != "1" {
		t.Errorf("Version: got %q, want %q", cfg.Version, "1")
	}
	if cfg.ExportedAt == "" {
		t.Error("ExportedAt should not be empty")
	}
	if cfg.LLM.Provider != "openai" {
		t.Errorf("LLM.Provider: got %q, want %q", cfg.LLM.Provider, "openai")
	}
	if cfg.LLM.Model != "gpt-4" {
		t.Errorf("LLM.Model: got %q, want %q", cfg.LLM.Model, "gpt-4")
	}
	if cfg.LLM.MaxTokens != 4096 {
		t.Errorf("LLM.MaxTokens: got %d, want 4096", cfg.LLM.MaxTokens)
	}
	if cfg.LLM.Timeout != 60 {
		t.Errorf("LLM.Timeout: got %d, want 60", cfg.LLM.Timeout)
	}
	if len(cfg.Agents) != 1 {
		t.Fatalf("Agents: got %d, want 1", len(cfg.Agents))
	}
	if cfg.Agents[0].Name != "reviewer" {
		t.Errorf("Agent.Name: got %q, want %q", cfg.Agents[0].Name, "reviewer")
	}
	if len(cfg.Workspaces) != 1 {
		t.Fatalf("Workspaces: got %d, want 1", len(cfg.Workspaces))
	}
}

// T-5: Marshaled JSON must not contain any API key.
func TestMarshalExportConfig_NoAPIKey(t *testing.T) {
	appCfg := Config{
		LLMProvider:  "claude",
		LLMAPIKey:    "sk-ant-super-secret-key",
		LLMMaxTokens: 8192,
		LLMTimeout:   120,
	}

	cfg := BuildExportConfig(appCfg, nil, nil)

	data, err := MarshalExportConfig(cfg)
	if err != nil {
		t.Fatalf("MarshalExportConfig failed: %v", err)
	}

	output := string(data)
	if strings.Contains(output, "sk-ant-super-secret-key") {
		t.Error("marshaled JSON contains the API key")
	}
	if strings.Contains(output, "api_key") {
		t.Error("marshaled JSON contains an api_key field")
	}

	// Also verify via raw map that no key-like field exists
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("failed to unmarshal to map: %v", err)
	}
	llm, ok := raw["llm"].(map[string]interface{})
	if !ok {
		t.Fatal("llm field missing or wrong type")
	}
	for key := range llm {
		if strings.Contains(strings.ToLower(key), "key") {
			t.Errorf("llm contains key-like field: %q", key)
		}
	}
}

// EC-1: Empty agents list serializes as [], not null.
func TestMarshalExportConfig_EmptyAgentsList(t *testing.T) {
	cfg := ExportConfig{
		Version:    "1",
		ExportedAt: "2026-01-15T10:30:00Z",
		LLM: LLMExportConfig{
			Provider:  "claude",
			MaxTokens: 8192,
			Timeout:   120,
		},
		Agents: nil, // explicitly nil
	}

	data, err := MarshalExportConfig(cfg)
	if err != nil {
		t.Fatalf("MarshalExportConfig failed: %v", err)
	}

	output := string(data)
	if !strings.Contains(output, `"agents": []`) {
		t.Errorf("expected empty agents array [], got: %s", output)
	}
}

// BuildExportConfig with nil agents should produce empty slice, not nil.
func TestBuildExportConfig_NilAgents(t *testing.T) {
	cfg := BuildExportConfig(Config{LLMProvider: "claude", LLMMaxTokens: 8192, LLMTimeout: 120}, nil, nil)

	if cfg.Agents == nil {
		t.Error("Agents should not be nil after BuildExportConfig")
	}
	if len(cfg.Agents) != 0 {
		t.Errorf("Agents: got %d, want 0", len(cfg.Agents))
	}
}
