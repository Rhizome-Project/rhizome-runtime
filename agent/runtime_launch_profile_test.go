package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDeterministicLaunchProfilesAreStableAndDistinct(t *testing.T) {
	seen := map[string]RuntimeLaunchProfile{}
	for _, name := range deterministicLaunchProfileNames() {
		first, ok := deterministicLaunchProfileForName(name)
		if !ok {
			t.Fatalf("expected launch profile for %q", name)
		}
		second, ok := deterministicLaunchProfileForName(name)
		if !ok {
			t.Fatalf("expected second launch profile lookup for %q", name)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("expected deterministic launch profile for %q to be stable, got %+v vs %+v", name, first, second)
		}
		if first.Name != name || first.AgentID != name || first.DisplayName != strings.ToUpper(name[:1])+name[1:] {
			t.Fatalf("unexpected profile identity for %q: %+v", name, first)
		}
		if first.ProviderID != "codex" || first.ModelOverride != defaultModel || first.LLMBackend != llmBackendCodex || first.Model != defaultModel || first.GroupID != "codex" || first.Role != "generalist" {
			t.Fatalf("unexpected runtime binding for %q: %+v", name, first)
		}
		if summary := first.summary(); !strings.Contains(summary, "profile="+name) || !strings.Contains(summary, "agent="+name) {
			t.Fatalf("expected profile summary to expose identity for %q, got %q", name, summary)
		}
		seen[name] = first
	}
	if len(seen) != 3 {
		t.Fatalf("expected three canonical profiles, got %+v", seen)
	}
	if _, ok := deterministicLaunchProfileForName("delta"); ok {
		t.Fatal("did not expect deterministic launch profile for delta")
	}
}

func TestLoadRuntimeConfigFromArgsUsesDeterministicLaunchProfileForAlpha(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_HOST", "")
	t.Setenv("RHIZOME_RPC", "")
	t.Setenv("RHIZOME_TOKEN", "")
	t.Setenv("RHIZOME_WORKSPACE_ID", "")
	t.Setenv("RHIZOME_AGENT_ID", "")
	t.Setenv("RHIZOME_AGENT_NAME", "")
	t.Setenv("RHIZOME_OWNER_USER_ID", "")
	t.Setenv("RHIZOME_AGENT_ROLE", "")
	t.Setenv("RHIZOME_AGENT_CAPABILITIES", "")
	t.Setenv("RHIZOME_AGENT_PROVIDER_ID", "")
	t.Setenv("RHIZOME_AGENT_MODEL_OVERRIDE", "")
	t.Setenv("RHIZOME_AGENT_LLM_BACKEND", "")
	t.Setenv("RHIZOME_AGENT_MODEL", "")
	t.Setenv("RHIZOME_AGENT_MODE", "")

	workdir := filepath.Join(t.TempDir(), "alpha")
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}

	cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", workdir})
	if err != nil {
		t.Fatalf("loadRuntimeConfigFromArgs() error: %v", err)
	}

	if cfg.AgentID != "alpha" || cfg.DisplayName != "Alpha" {
		t.Fatalf("expected alpha launch identity, got agent=%q display=%q", cfg.AgentID, cfg.DisplayName)
	}
	if cfg.ProviderID != "codex" || cfg.ModelOverride != defaultModel || cfg.GroupID != "codex" {
		t.Fatalf("expected alpha launch provider binding, got provider=%q model_override=%q group=%q", cfg.ProviderID, cfg.ModelOverride, cfg.GroupID)
	}
	if cfg.LLMBackend != llmBackendCodex || cfg.Model != defaultModel || cfg.Role != "generalist" {
		t.Fatalf("expected alpha launch runtime defaults, got backend=%q model=%q role=%q", cfg.LLMBackend, cfg.Model, cfg.Role)
	}
	if !strings.Contains(strings.Join(cfg.Capabilities, ","), "tool.call") {
		t.Fatalf("expected launch config to keep default capabilities, got %+v", cfg.Capabilities)
	}
}

func TestDeterministicLaunchProfilesNormalizeOperatorWorkdirAliases(t *testing.T) {
	cases := []string{
		"alpha",
		"agent-alpha",
		"agent-alpha-dir",
		"rhizome-agent-alpha",
		"agent_alpha",
		"agent.alpha",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			profile, ok := deterministicLaunchProfileForWorkdir(filepath.Join(t.TempDir(), name))
			if !ok {
				t.Fatalf("expected launch profile for workdir alias %q", name)
			}
			if profile.AgentID != "alpha" || profile.DisplayName != "Alpha" {
				t.Fatalf("expected alpha profile for alias %q, got %+v", name, profile)
			}
		})
	}
}
