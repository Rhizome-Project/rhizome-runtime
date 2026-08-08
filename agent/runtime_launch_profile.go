package main

import (
	"path/filepath"
	"strings"
)

type RuntimeLaunchProfile struct {
	Name          string
	AgentID       string
	DisplayName   string
	ProviderID    string
	ModelOverride string
	LLMBackend    string
	Model         string
	GroupID       string
	Role          string
	Capabilities  []string
}

func deterministicLaunchProfileNames() []string {
	return []string{"alpha", "beta", "gamma"}
}

func deterministicLaunchProfileForWorkdir(workdir string) (RuntimeLaunchProfile, bool) {
	return deterministicLaunchProfileForName(launchProfileNameFromPath(workdir))
}

func deterministicLaunchProfileForName(name string) (RuntimeLaunchProfile, bool) {
	switch canonicalLaunchProfileName(name) {
	case "alpha":
		return RuntimeLaunchProfile{
			Name:          "alpha",
			AgentID:       "alpha",
			DisplayName:   "Alpha",
			ProviderID:    "codex",
			ModelOverride: defaultModel,
			LLMBackend:    llmBackendCodex,
			Model:         defaultModel,
			GroupID:       "codex",
			Role:          "generalist",
		}, true
	case "beta":
		return RuntimeLaunchProfile{
			Name:          "beta",
			AgentID:       "beta",
			DisplayName:   "Beta",
			ProviderID:    "codex",
			ModelOverride: defaultModel,
			LLMBackend:    llmBackendCodex,
			Model:         defaultModel,
			GroupID:       "codex",
			Role:          "generalist",
		}, true
	case "gamma":
		return RuntimeLaunchProfile{
			Name:          "gamma",
			AgentID:       "gamma",
			DisplayName:   "Gamma",
			ProviderID:    "codex",
			ModelOverride: defaultModel,
			LLMBackend:    llmBackendCodex,
			Model:         defaultModel,
			GroupID:       "codex",
			Role:          "generalist",
		}, true
	default:
		return RuntimeLaunchProfile{}, false
	}
}

func (p RuntimeLaunchProfile) active() bool {
	return strings.TrimSpace(p.Name) != ""
}

func (p RuntimeLaunchProfile) summary() string {
	if !p.active() {
		return ""
	}
	parts := []string{"profile=" + p.Name}
	if strings.TrimSpace(p.AgentID) != "" {
		parts = append(parts, "agent="+p.AgentID)
	}
	if strings.TrimSpace(p.DisplayName) != "" {
		parts = append(parts, "display="+p.DisplayName)
	}
	if strings.TrimSpace(p.ProviderID) != "" {
		parts = append(parts, "provider="+p.ProviderID)
	}
	if strings.TrimSpace(p.GroupID) != "" {
		parts = append(parts, "group="+p.GroupID)
	}
	if strings.TrimSpace(p.LLMBackend) != "" {
		parts = append(parts, "backend="+p.LLMBackend)
	}
	if strings.TrimSpace(p.Model) != "" {
		parts = append(parts, "model="+p.Model)
	}
	if strings.TrimSpace(p.Role) != "" {
		parts = append(parts, "role="+p.Role)
	}
	return strings.Join(parts, " ")
}

func (p RuntimeLaunchProfile) applyTo(cfg *RuntimeConfig) {
	if cfg == nil || !p.active() {
		return
	}
	cfg.AgentID = firstNonEmpty(cfg.AgentID, p.AgentID)
	cfg.DisplayName = firstNonEmpty(cfg.DisplayName, p.DisplayName)
	cfg.ProviderID = firstNonEmpty(cfg.ProviderID, p.ProviderID)
	cfg.ModelOverride = firstNonEmpty(cfg.ModelOverride, p.ModelOverride)
	cfg.LLMBackend = firstNonEmpty(cfg.LLMBackend, p.LLMBackend)
	cfg.Model = firstNonEmpty(cfg.Model, p.Model)
	cfg.GroupID = firstNonEmpty(cfg.GroupID, p.GroupID)
	cfg.Role = firstNonEmpty(cfg.Role, p.Role)
	if len(cfg.Capabilities) == 0 && len(p.Capabilities) > 0 {
		cfg.Capabilities = append([]string(nil), p.Capabilities...)
	}
}

func launchProfileNameFromPath(workdir string) string {
	return canonicalLaunchProfileName(filepath.Base(strings.TrimSpace(workdir)))
}

func canonicalLaunchProfileName(name string) string {
	normalized := strings.ToLower(strings.TrimSpace(name))
	normalized = strings.ReplaceAll(normalized, "_", "-")
	normalized = strings.ReplaceAll(normalized, ".", "-")
	normalized = strings.TrimSuffix(normalized, "-dir")
	normalized = strings.TrimPrefix(normalized, "rhizome-agent-")
	normalized = strings.TrimPrefix(normalized, "agent-")
	return normalized
}
