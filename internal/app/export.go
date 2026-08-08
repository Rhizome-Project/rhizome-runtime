package app

import (
	"encoding/json"
	"fmt"
	"time"
)

// ExportConfig represents the full exportable configuration of a Rhizome instance.
type ExportConfig struct {
	Version    string                  `json:"version"`
	ExportedAt string                  `json:"exported_at"`
	LLM        LLMExportConfig         `json:"llm"`
	Agents     []AgentExportConfig     `json:"agents"`
	Workspaces []WorkspaceExportConfig `json:"workspaces,omitempty"`
}

// LLMExportConfig holds LLM provider settings for export.
// API key is deliberately excluded.
type LLMExportConfig struct {
	Provider  string `json:"provider"`
	Model     string `json:"model,omitempty"`
	MaxTokens int    `json:"max_tokens"`
	BaseURL   string `json:"base_url,omitempty"`
	Timeout   int    `json:"timeout"`
}

// AgentExportConfig holds an agent definition for export.
// ID is not exported; it will be regenerated on import.
type AgentExportConfig struct {
	Name          string   `json:"name"`
	Provider      string   `json:"provider"`
	Model         string   `json:"model,omitempty"`
	SystemPrompt  string   `json:"system_prompt,omitempty"`
	Tools         []string `json:"tools,omitempty"`
	MaxIterations int      `json:"max_iterations"`
	WorkspaceID   string   `json:"workspace_id"`
}

// WorkspaceExportConfig holds workspace metadata for export.
type WorkspaceExportConfig struct {
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
}

// MarshalExportConfig serializes an ExportConfig to indented JSON.
func MarshalExportConfig(cfg ExportConfig) ([]byte, error) {
	// Ensure agents is serialized as [] not null.
	if cfg.Agents == nil {
		cfg.Agents = []AgentExportConfig{}
	}
	return json.MarshalIndent(cfg, "", "  ")
}

// UnmarshalExportConfig deserializes JSON into an ExportConfig and validates
// the version field.
func UnmarshalExportConfig(data []byte) (*ExportConfig, error) {
	var cfg ExportConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse export config: %w", err)
	}
	if cfg.Version != "1" {
		return nil, fmt.Errorf("unsupported config version: %s", cfg.Version)
	}
	return &cfg, nil
}

// BuildExportConfig assembles an ExportConfig from the current app Config
// and lists of agents and workspaces.
func BuildExportConfig(appCfg Config, agents []AgentExportConfig, workspaces []WorkspaceExportConfig) ExportConfig {
	if agents == nil {
		agents = []AgentExportConfig{}
	}
	return ExportConfig{
		Version:    "1",
		ExportedAt: time.Now().UTC().Format(time.RFC3339),
		LLM: LLMExportConfig{
			Provider:  appCfg.LLMProvider,
			Model:     appCfg.LLMModel,
			MaxTokens: appCfg.LLMMaxTokens,
			BaseURL:   appCfg.LLMBaseURL,
			Timeout:   appCfg.LLMTimeout,
		},
		Agents:     agents,
		Workspaces: workspaces,
	}
}
