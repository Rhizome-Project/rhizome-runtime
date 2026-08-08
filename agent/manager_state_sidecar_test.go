package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveBotRegistryNormalizesAndDropsIncompleteAgents(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			HostURL: "https://rhizome.test",
		},
		Agents: []ManagedAgentRecord{
			{AgentID: "bravo", Workdir: filepath.Join(home, "bravo")},
			{AgentID: "", Workdir: filepath.Join(home, "missing-id")},
			{AgentID: "alpha", Workdir: filepath.Join(home, "alpha")},
			{AgentID: "charlie"},
		},
	})
	if err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	registry := LoadBotRegistry()
	if len(registry.Agents) != 2 {
		t.Fatalf("expected incomplete agents to be dropped, got %+v", registry.Agents)
	}
	if registry.Agents[0].AgentID != "alpha" || registry.Agents[1].AgentID != "bravo" {
		t.Fatalf("expected deterministic agent ordering, got %+v", registry.Agents)
	}
	if registry.Agents[0].LLMBackend != llmBackendAuto || registry.Agents[0].Model != defaultModel {
		t.Fatalf("expected normalized backend/model defaults, got %+v", registry.Agents[0])
	}
}

func TestSaveLocalRuntimeProfileEmptyWorkdirIsNoop(t *testing.T) {
	if err := SaveLocalRuntimeProfile("", LocalRuntimeProfile{AgentID: "noop"}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
}

func TestProcessExistsReportsCurrentPidAndMissingPid(t *testing.T) {
	ok, err := processExists(os.Getpid())
	if err != nil {
		t.Fatalf("processExists(current) error: %v", err)
	}
	if !ok {
		t.Fatal("expected current pid to exist")
	}

	ok, err = processExists(2147483647)
	if err != nil {
		t.Fatalf("processExists(missing) error: %v", err)
	}
	if ok {
		t.Fatal("expected obviously missing pid to be absent")
	}
}
