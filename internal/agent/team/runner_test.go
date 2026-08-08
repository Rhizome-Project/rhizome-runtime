package team

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// testStore creates a temporary SQLite store for testing.
func testStore(t *testing.T) *sqlite.Store {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	s, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("creating test store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestRunTeam_ConfigNotFound(t *testing.T) {
	store := testStore(t)
	opts := RunTeamOpts{
		ConfigPath: "/nonexistent/path/team.json",
		Task:       "do something",
		Store:      store,
		AppConfig:  app.Config{},
	}

	result, err := RunTeam(context.Background(), opts)
	if err == nil {
		t.Fatal("expected error for nonexistent config, got nil")
	}
	if result != nil {
		t.Fatal("expected nil result on error")
	}
	if !strings.Contains(err.Error(), "loading team config") {
		t.Errorf("error should mention loading: %v", err)
	}
}

func TestRunTeam_InvalidConfig(t *testing.T) {
	store := testStore(t)

	// Write a config with missing required fields.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "team.json")

	cfg := TeamConfig{
		// Missing name, invalid coordination, no agents.
		Coordination: "bogus",
	}
	data, _ := json.Marshal(cfg)
	if err := os.WriteFile(cfgPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	opts := RunTeamOpts{
		ConfigPath: cfgPath,
		Task:       "do something",
		Store:      store,
		AppConfig:  app.Config{},
	}

	result, err := RunTeam(context.Background(), opts)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if result != nil {
		t.Fatal("expected nil result on validation error")
	}
	if !strings.Contains(err.Error(), "validating team config") {
		t.Errorf("error should mention validation: %v", err)
	}
}

func TestRunTeam_MissingAPIKey(t *testing.T) {
	store := testStore(t)

	// Write a valid config that requires an API key we don't provide.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "team.json")

	cfg := TeamConfig{
		Name:         "test-team",
		Coordination: CoordinationSequential,
		Agents: []AgentSpec{
			{
				Name:         "agent1",
				Role:         "assistant",
				Provider:     "claude",
				SystemPrompt: "help",
				WorkspaceID:  "ws1",
			},
		},
	}
	data, _ := json.Marshal(cfg)
	if err := os.WriteFile(cfgPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Clear env vars that might provide keys.
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("RHIZOME_LLM_API_KEY", "")
	// Override HOME so auth.json won't be found.
	t.Setenv("HOME", dir)

	opts := RunTeamOpts{
		ConfigPath: cfgPath,
		Task:       "do something",
		Store:      store,
		AppConfig:  app.Config{LLMProvider: "openai", LLMAPIKey: ""}, // wrong provider, no key
	}

	result, err := RunTeam(context.Background(), opts)
	if err == nil {
		t.Fatal("expected API key error, got nil")
	}
	if result != nil {
		t.Fatal("expected nil result on API key error")
	}
	if !strings.Contains(err.Error(), "no API key found for provider") {
		t.Errorf("error should mention missing API key: %v", err)
	}
	if !strings.Contains(err.Error(), "claude") {
		t.Errorf("error should mention the provider name: %v", err)
	}
}

func TestRunTeam_EmptyTask(t *testing.T) {
	store := testStore(t)
	opts := RunTeamOpts{
		ConfigPath: "irrelevant",
		Task:       "",
		Store:      store,
		AppConfig:  app.Config{},
	}

	result, err := RunTeam(context.Background(), opts)
	if err == nil {
		t.Fatal("expected error for empty task, got nil")
	}
	if result != nil {
		t.Fatal("expected nil result on error")
	}
	if !strings.Contains(err.Error(), "task is required") {
		t.Errorf("error should mention task is required: %v", err)
	}
}

func TestRunTeam_NilStore(t *testing.T) {
	opts := RunTeamOpts{
		ConfigPath: "irrelevant",
		Task:       "do something",
		Store:      nil,
		AppConfig:  app.Config{},
	}

	result, err := RunTeam(context.Background(), opts)
	if err == nil {
		t.Fatal("expected error for nil store, got nil")
	}
	if result != nil {
		t.Fatal("expected nil result on error")
	}
	if !strings.Contains(err.Error(), "store is required") {
		t.Errorf("error should mention store is required: %v", err)
	}
}

func TestResolveAPIKeys_AppConfigMatch(t *testing.T) {
	cfg := &TeamConfig{
		Name:         "test",
		Coordination: CoordinationSequential,
		Agents: []AgentSpec{
			{Name: "a1", Provider: "claude"},
		},
	}

	appCfg := app.Config{
		LLMProvider: "claude",
		LLMAPIKey:   "sk-test-key",
	}

	keys, err := resolveAPIKeys(cfg, appCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keys["claude"] != "sk-test-key" {
		t.Errorf("expected sk-test-key, got %s", keys["claude"])
	}
}

func TestResolveAPIKeys_EnvVar(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "sk-openai-env")

	cfg := &TeamConfig{
		Name:         "test",
		Coordination: CoordinationSequential,
		Agents: []AgentSpec{
			{Name: "a1", Provider: "openai"},
		},
	}

	appCfg := app.Config{
		LLMProvider: "claude",
		LLMAPIKey:   "sk-claude-key",
	}

	keys, err := resolveAPIKeys(cfg, appCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keys["openai"] != "sk-openai-env" {
		t.Errorf("expected sk-openai-env, got %s", keys["openai"])
	}
}

func TestResolveAPIKeys_MultipleProviders(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "sk-openai")
	t.Setenv("ANTHROPIC_API_KEY", "sk-anthropic")

	cfg := &TeamConfig{
		Name:         "test",
		Coordination: CoordinationParallel,
		Agents: []AgentSpec{
			{Name: "a1", Provider: "openai"},
			{Name: "a2", Provider: "claude"},
		},
	}

	appCfg := app.Config{}

	keys, err := resolveAPIKeys(cfg, appCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keys["openai"] != "sk-openai" {
		t.Errorf("expected sk-openai, got %s", keys["openai"])
	}
	if keys["claude"] != "sk-anthropic" {
		t.Errorf("expected sk-anthropic, got %s", keys["claude"])
	}
}
