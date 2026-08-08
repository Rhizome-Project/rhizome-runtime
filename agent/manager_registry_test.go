package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBotRegistryDefaultsFallBackToLegacyProfile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	err := SaveRhizomeProfile(RhizomeConnectionProfile{
		HostURL:           "https://rhizome.test",
		WorkspaceID:       "ws-legacy",
		WorkspacePassword: "pw-legacy",
		OwnerUserID:       "owner-legacy",
	})
	if err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	registry := LoadBotRegistry()
	if registry.Defaults.HostURL != "https://rhizome.test" {
		t.Fatalf("expected host default from legacy profile, got %q", registry.Defaults.HostURL)
	}
	if registry.Defaults.WorkspaceID != "ws-legacy" {
		t.Fatalf("expected workspace default from legacy profile, got %q", registry.Defaults.WorkspaceID)
	}
	if registry.Defaults.OwnerUserID != "owner-legacy" {
		t.Fatalf("expected owner default from legacy profile, got %q", registry.Defaults.OwnerUserID)
	}
}

func TestBotRegistryDefaultsPreferGlobalOwnerIdentityOverStaleRegistryDefault(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := SaveRhizomeProfile(RhizomeConnectionProfile{
		HostURL:           "https://rhizome.test",
		WorkspaceID:       "ws-live",
		WorkspacePassword: "pw-live",
		OwnerUserID:       "user-live",
	}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}
	if err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			HostURL:           "https://rhizome.test",
			WorkspaceID:       "ws-live",
			WorkspacePassword: "pw-live",
			OwnerUserID:       "developer",
		},
	}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	registry := LoadBotRegistry()
	if registry.Defaults.OwnerUserID != "user-live" {
		t.Fatalf("expected global live owner identity to win, got %q", registry.Defaults.OwnerUserID)
	}
}

func TestUpsertManagedAgentRoundTrip(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	record := ManagedAgentRecord{
		AgentID:       "lyrica",
		DisplayName:   "Lyrica",
		Workdir:       t.TempDir(),
		HostURL:       "https://rhizome.test",
		WorkspaceID:   "rhizome-main",
		Role:          "generalist",
		AnatomyPreset: "ui-critic",
		AnatomyDigest: "digest-1",
		ToolBundles:   []string{"browser-visual-probe", "browser_visual_probe"},
		LLMBackend:    llmBackendCodex,
		Model:         defaultModel,
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	got, ok := FindManagedAgent("lyrica")
	if !ok {
		t.Fatal("expected managed agent to be found")
	}
	if got.AgentID != "lyrica" || got.Workdir == "" {
		t.Fatalf("unexpected managed agent record: %+v", got)
	}
	if got.AnatomyPreset != "ui_ux_reality_critic" || got.AnatomyDigest != "digest-1" {
		t.Fatalf("expected anatomy fields to round-trip normalized, got %+v", got)
	}
	if len(got.ToolBundles) != 1 || got.ToolBundles[0] != "browser_visual_probe" {
		t.Fatalf("expected tool bundles to round-trip normalized and deduped, got %+v", got.ToolBundles)
	}
}

func TestBotRegistryIgnoresLocalDevelopmentLegacyProfileForDefaults(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	err := SaveRhizomeProfile(RhizomeConnectionProfile{
		HostURL:           "http://127.0.0.1:52472",
		WorkspaceID:       "ws-1",
		WorkspacePassword: "test-workspace-password",
		OwnerUserID:       "owner-1",
	})
	if err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	registry := LoadBotRegistry()
	if registry.Defaults.HostURL != defaultRhizomeHostURL {
		t.Fatalf("expected local dev host to be ignored in favor of built-in default, got %q", registry.Defaults.HostURL)
	}
	if registry.Defaults.WorkspaceID != defaultWorkspaceID {
		t.Fatalf("expected local dev workspace to be ignored in favor of built-in default, got %q", registry.Defaults.WorkspaceID)
	}
}

func TestUpsertManagedAgentRejectsDuplicateCanonicalWorkdirAcrossAgents(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "agent-a",
		DisplayName: "Agent A",
		Workdir:     workdir,
		WorkspaceID: "rhizome-main",
		OwnerUserID: "owner-a",
	}); err != nil {
		t.Fatalf("UpsertManagedAgent(agent-a) error: %v", err)
	}

	err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "agent-b",
		DisplayName: "Agent B",
		Workdir:     filepath.Join(workdir, "."),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "owner-b",
	})
	if err == nil {
		t.Fatal("expected duplicate canonical workdir to be rejected")
	}
	if !strings.Contains(err.Error(), "cannot share workdir") {
		t.Fatalf("expected duplicate workdir error, got %v", err)
	}
}

func TestUpsertManagedAgentArchivesCorruptRegistryInsteadOfOverwritingIt(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := os.MkdirAll(agentRuntimeConfigRoot(), 0o700); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	if err := os.WriteFile(botRegistryPath(), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("WriteFile(botRegistryPath) error: %v", err)
	}

	err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID: "agent-safe",
		Workdir: t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected corrupt bot registry to fail closed")
	}
	if pathExists(botRegistryPath()) {
		t.Fatalf("expected corrupt registry at %s to be archived", botRegistryPath())
	}
	matches, globErr := filepath.Glob(botRegistryPath() + ".broken-*")
	if globErr != nil {
		t.Fatalf("Glob() error: %v", globErr)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived bot registry, got %v", matches)
	}
}

func TestSetManagerDefaultSupportsAnatomyFields(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	path := filepath.Join(t.TempDir(), "agent.anatomy.json")
	if err := SetManagerDefault("anatomy_preset", "ui-critic"); err != nil {
		t.Fatalf("SetManagerDefault(anatomy_preset) error: %v", err)
	}
	if err := SetManagerDefault("anatomy_path", path); err != nil {
		t.Fatalf("SetManagerDefault(anatomy_path) error: %v", err)
	}
	if err := SetManagerDefault("anatomy_digest", "digest-1"); err != nil {
		t.Fatalf("SetManagerDefault(anatomy_digest) error: %v", err)
	}
	if err := SetManagerDefault("tool_bundles", "browser-visual-probe, ../bad"); err != nil {
		t.Fatalf("SetManagerDefault(tool_bundles) error: %v", err)
	}
	registry := LoadBotRegistry()
	if registry.Defaults.AnatomyPreset != "ui_ux_reality_critic" || registry.Defaults.AnatomyPath != path || registry.Defaults.AnatomyDigest != "digest-1" {
		t.Fatalf("unexpected anatomy defaults: %+v", registry.Defaults)
	}
	if len(registry.Defaults.ToolBundles) != 1 || registry.Defaults.ToolBundles[0] != "browser_visual_probe" {
		t.Fatalf("unexpected tool bundle defaults: %+v", registry.Defaults.ToolBundles)
	}
	if err := SetManagerDefault("anatomy_path", "relative.json"); err == nil {
		t.Fatal("expected relative anatomy_path to be rejected")
	}
}
