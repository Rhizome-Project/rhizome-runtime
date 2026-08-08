package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSyncManagedRosterUpdatesLaunchRolesAndPrunesNonRosterAgents(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	parentDir := filepath.Join(home, "agents")
	alphaWorkdir := filepath.Join(parentDir, "alpha")
	iotaWorkdir := filepath.Join(parentDir, "iota")
	if err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			HostURL:           "https://rhizome.test",
			WorkspaceID:       "ws-1",
			OwnerUserID:       "owner-1",
			DefaultProviderID: "codex",
			ModelOverride:     "gpt-5.4-mini",
			GroupID:           "codex",
			LLMBackend:        "codex",
			Model:             "gpt-5.4-mini",
			CoordinationMode:  "trust_first",
			DefaultParentDir:  parentDir,
		},
		Agents: []ManagedAgentRecord{
			{
				AgentID:          "alpha",
				DisplayName:      "Alpha",
				Workdir:          alphaWorkdir,
				HostURL:          "https://rhizome.test",
				WorkspaceID:      "ws-1",
				OwnerUserID:      "owner-1",
				ProviderID:       "codex",
				ModelOverride:    "gpt-5.4-mini",
				GroupID:          "codex",
				Role:             "rq product coordinator",
				LLMBackend:       "codex",
				Model:            "gpt-5.4-mini",
				AnatomyPath:      filepath.Join(home, "stale-anatomy.json"),
				AnatomyDigest:    "stale-digest",
				ToolBundles:      []string{"browser_visual_probe"},
				CoordinationMode: "trust_first",
			},
			{
				AgentID:     "iota",
				DisplayName: "Iota",
				Workdir:     iotaWorkdir,
				Role:        "rq extra verifier",
			},
		},
	}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}
	if err := SaveAgentProfile(alphaWorkdir, AgentProfile{
		AgentID:               "alpha",
		DisplayName:           "Alpha",
		Role:                  "rq product coordinator",
		PrimarySpecialization: "rq",
	}); err != nil {
		t.Fatalf("SaveAgentProfile() error: %v", err)
	}

	rosterPath := filepath.Join(home, "roster.json")
	roster := `{
  "alpha": {
    "preset_target": "strategist",
    "role": "Lua capability lead",
    "specialization": "Lua project lead"
  },
  "beta": {
    "preset_target": "generalist",
    "role": "Lua lexer/parser frontend implementer",
    "specialization": "Lua lexer and parser"
  },
  "zeta": {
    "preset_target": "integrator",
    "role": "Lua integration steward",
    "specialization": "Lua integration"
  }
}`
	if err := os.WriteFile(rosterPath, []byte(roster), 0o600); err != nil {
		t.Fatalf("WriteFile(roster) error: %v", err)
	}

	result, err := syncManagedRosterFromFile(managedRosterSyncOptions{
		RosterJSON:          rosterPath,
		Prune:               true,
		MaterializeProfiles: true,
	})
	if err != nil {
		t.Fatalf("syncManagedRosterFromFile() error: %v", err)
	}
	if len(result.Added) != 2 || result.Added[0] != "beta" || result.Added[1] != "zeta" {
		t.Fatalf("unexpected added agents: %+v", result.Added)
	}
	if len(result.Updated) != 1 || result.Updated[0] != "alpha" {
		t.Fatalf("unexpected updated agents: %+v", result.Updated)
	}
	if len(result.Removed) != 1 || result.Removed[0] != "iota" {
		t.Fatalf("unexpected removed agents: %+v", result.Removed)
	}

	registry := LoadBotRegistry()
	if len(registry.Agents) != 3 {
		t.Fatalf("expected exactly roster agents, got %+v", registry.Agents)
	}
	alpha, ok := FindManagedAgent("alpha")
	if !ok {
		t.Fatal("expected alpha after sync")
	}
	if alpha.Role != "Lua capability lead" || alpha.AnatomyPreset != "strategist" || alpha.AnatomyPath != "" || alpha.AnatomyDigest != "" {
		t.Fatalf("alpha was not reconciled to roster launch contract: %+v", alpha)
	}
	if alpha.ProviderID != "codex" || alpha.ModelOverride != "gpt-5.4-mini" || alpha.Workdir != alphaWorkdir {
		t.Fatalf("alpha provider/model/workdir were not preserved: %+v", alpha)
	}
	beta, ok := FindManagedAgent("beta")
	if !ok {
		t.Fatal("expected beta after sync")
	}
	if beta.Workdir != filepath.Join(parentDir, "beta") || beta.Role != "Lua lexer/parser frontend implementer" {
		t.Fatalf("beta was not materialized from roster/default parent: %+v", beta)
	}
	if _, ok := FindManagedAgent("iota"); ok {
		t.Fatal("expected non-roster iota to be pruned")
	}

	profile := LoadAgentProfile(alphaWorkdir)
	if profile.Role != "Lua capability lead" || profile.PrimarySpecialization != "Lua project lead" {
		t.Fatalf("expected alpha profile to be materialized from roster, got %+v", profile)
	}
}

func TestSyncManagedRosterRejectsMissingRole(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	rosterPath := filepath.Join(home, "bad-roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{"alpha":{"preset_target":"strategist"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile(roster) error: %v", err)
	}
	if _, err := syncManagedRosterFromFile(managedRosterSyncOptions{RosterJSON: rosterPath}); err == nil {
		t.Fatal("expected missing role to fail closed")
	}
}
