package living

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validYAML = `
id: agent-1
role: developer
workspace_id: ws-123
task_types:
  - code_review
  - refactor
heartbeat_interval: "5s"
situation_check_every: 10
max_concurrent_tasks: 5
max_retries: 3
reflect_every: 8
context_threshold: 0.7
models:
  primary: claude-opus-4-20250514
  worker: claude-sonnet-4-20250514
  cheap: claude-haiku
worker_tools:
  - bash
  - read
tools:
  - search
plugins:
  - git
skills:
  - deploy
rhizome_url: http://localhost:8080
redis_url: localhost:6380
memory:
  db_path: data/my-memory.db
  max_results: 20
llm:
  api_key: sk-test-key
  provider: claude
  base_url: https://api.example.com
  max_tokens: 4096
  timeout: "60s"
`

const requiredOnlyYAML = `
id: agent-2
role: tester
workspace_id: ws-456
task_types:
  - testing
models:
  primary: gpt-4
  worker: gpt-3.5
  cheap: gpt-3.5-mini
rhizome_url: http://localhost:9090
llm:
  api_key: sk-required-key
`

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

// T-1: Valid file with all fields populated
func TestLoadConfig_ValidFile(t *testing.T) {
	path := writeTemp(t, validYAML)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ID != "agent-1" {
		t.Errorf("ID = %q, want %q", cfg.ID, "agent-1")
	}
	if cfg.Role != "developer" {
		t.Errorf("Role = %q, want %q", cfg.Role, "developer")
	}
	if cfg.WorkspaceID != "ws-123" {
		t.Errorf("WorkspaceID = %q, want %q", cfg.WorkspaceID, "ws-123")
	}
	if len(cfg.TaskTypes) != 2 || cfg.TaskTypes[0] != "code_review" {
		t.Errorf("TaskTypes = %v, want [code_review refactor]", cfg.TaskTypes)
	}
	if cfg.HeartbeatInterval != 5*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 5s", cfg.HeartbeatInterval)
	}
	if cfg.SituationCheckEvery != 10 {
		t.Errorf("SituationCheckEvery = %d, want 10", cfg.SituationCheckEvery)
	}
	if cfg.MaxConcurrentTasks != 5 {
		t.Errorf("MaxConcurrentTasks = %d, want 5", cfg.MaxConcurrentTasks)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", cfg.MaxRetries)
	}
	if cfg.ReflectEvery != 8 {
		t.Errorf("ReflectEvery = %d, want 8", cfg.ReflectEvery)
	}
	if cfg.ContextThreshold != 0.7 {
		t.Errorf("ContextThreshold = %f, want 0.7", cfg.ContextThreshold)
	}
	if cfg.Models.Primary != "claude-opus-4-20250514" {
		t.Errorf("Models.Primary = %q, want %q", cfg.Models.Primary, "claude-opus-4-20250514")
	}
	if cfg.Models.Worker != "claude-sonnet-4-20250514" {
		t.Errorf("Models.Worker = %q, want %q", cfg.Models.Worker, "claude-sonnet-4-20250514")
	}
	if cfg.Models.Cheap != "claude-haiku" {
		t.Errorf("Models.Cheap = %q, want %q", cfg.Models.Cheap, "claude-haiku")
	}
	if len(cfg.WorkerTools) != 2 || cfg.WorkerTools[0] != "bash" {
		t.Errorf("WorkerTools = %v, want [bash read]", cfg.WorkerTools)
	}
	if len(cfg.Tools) != 1 || cfg.Tools[0] != "search" {
		t.Errorf("Tools = %v, want [search]", cfg.Tools)
	}
	if len(cfg.Plugins) != 1 || cfg.Plugins[0] != "git" {
		t.Errorf("Plugins = %v, want [git]", cfg.Plugins)
	}
	if len(cfg.Skills) != 1 || cfg.Skills[0] != "deploy" {
		t.Errorf("Skills = %v, want [deploy]", cfg.Skills)
	}
	if cfg.RhizomeURL != "http://localhost:8080" {
		t.Errorf("RhizomeURL = %q, want %q", cfg.RhizomeURL, "http://localhost:8080")
	}
	if cfg.RedisURL != "localhost:6380" {
		t.Errorf("RedisURL = %q, want %q", cfg.RedisURL, "localhost:6380")
	}
	if cfg.Memory.DBPath != "data/my-memory.db" {
		t.Errorf("Memory.DBPath = %q, want %q", cfg.Memory.DBPath, "data/my-memory.db")
	}
	if cfg.Memory.MaxResults != 20 {
		t.Errorf("Memory.MaxResults = %d, want 20", cfg.Memory.MaxResults)
	}
	if cfg.LLM.APIKey != "sk-test-key" {
		t.Errorf("LLM.APIKey = %q, want %q", cfg.LLM.APIKey, "sk-test-key")
	}
	if cfg.LLM.Provider != "claude" {
		t.Errorf("LLM.Provider = %q, want %q", cfg.LLM.Provider, "claude")
	}
	if cfg.LLM.BaseURL != "https://api.example.com" {
		t.Errorf("LLM.BaseURL = %q, want %q", cfg.LLM.BaseURL, "https://api.example.com")
	}
	if cfg.LLM.MaxTokens != 4096 {
		t.Errorf("LLM.MaxTokens = %d, want 4096", cfg.LLM.MaxTokens)
	}
	if cfg.LLM.Timeout != 60*time.Second {
		t.Errorf("LLM.Timeout = %v, want 60s", cfg.LLM.Timeout)
	}
}

// T-2: Defaults applied when only required fields are present
func TestLoadConfig_Defaults(t *testing.T) {
	path := writeTemp(t, requiredOnlyYAML)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.HeartbeatInterval != 10*time.Second {
		t.Errorf("HeartbeatInterval default = %v, want 10s", cfg.HeartbeatInterval)
	}
	if cfg.SituationCheckEvery != 6 {
		t.Errorf("SituationCheckEvery default = %d, want 6", cfg.SituationCheckEvery)
	}
	if cfg.MaxConcurrentTasks != 3 {
		t.Errorf("MaxConcurrentTasks default = %d, want 3", cfg.MaxConcurrentTasks)
	}
	if cfg.MaxRetries != 2 {
		t.Errorf("MaxRetries default = %d, want 2", cfg.MaxRetries)
	}
	if cfg.ReflectEvery != 5 {
		t.Errorf("ReflectEvery default = %d, want 5", cfg.ReflectEvery)
	}
	if cfg.ContextThreshold != 0.5 {
		t.Errorf("ContextThreshold default = %f, want 0.5", cfg.ContextThreshold)
	}
	expected := []string{"bash", "read", "write", "edit", "glob", "grep"}
	if len(cfg.WorkerTools) != len(expected) {
		t.Errorf("WorkerTools default = %v, want %v", cfg.WorkerTools, expected)
	} else {
		for i, v := range expected {
			if cfg.WorkerTools[i] != v {
				t.Errorf("WorkerTools[%d] = %q, want %q", i, cfg.WorkerTools[i], v)
			}
		}
	}
	if cfg.RedisURL != "localhost:6379" {
		t.Errorf("RedisURL default = %q, want %q", cfg.RedisURL, "localhost:6379")
	}
	if cfg.Memory.DBPath != "data/living-agent-2-memory.db" {
		t.Errorf("Memory.DBPath default = %q, want %q", cfg.Memory.DBPath, "data/living-agent-2-memory.db")
	}
	if cfg.Memory.MaxResults != 10 {
		t.Errorf("Memory.MaxResults default = %d, want 10", cfg.Memory.MaxResults)
	}
	if cfg.LLM.Provider != "claude" {
		t.Errorf("LLM.Provider default = %q, want %q", cfg.LLM.Provider, "claude")
	}
	if cfg.LLM.MaxTokens != 8192 {
		t.Errorf("LLM.MaxTokens default = %d, want 8192", cfg.LLM.MaxTokens)
	}
	if cfg.LLM.Timeout != 120*time.Second {
		t.Errorf("LLM.Timeout default = %v, want 120s", cfg.LLM.Timeout)
	}
}

// T-3: Environment variable expansion
func TestLoadConfig_EnvExpansion(t *testing.T) {
	t.Setenv("TEST_API_KEY", "sk-from-env")
	t.Setenv("TEST_AGENT_ID", "env-agent")

	yaml := `
id: "${TEST_AGENT_ID}"
role: developer
workspace_id: ws-1
task_types: [coding]
models:
  primary: m1
  worker: m2
  cheap: m3
rhizome_url: http://localhost:8080
llm:
  api_key: "${TEST_API_KEY}"
`
	path := writeTemp(t, yaml)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ID != "env-agent" {
		t.Errorf("ID = %q, want %q", cfg.ID, "env-agent")
	}
	if cfg.LLM.APIKey != "sk-from-env" {
		t.Errorf("LLM.APIKey = %q, want %q", cfg.LLM.APIKey, "sk-from-env")
	}
}

// T-4: Validation returns multi-error for all missing required fields
func TestLoadConfig_ValidationErrors(t *testing.T) {
	path := writeTemp(t, "{}")
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	msg := err.Error()
	required := []string{
		"id is required",
		"role is required",
		"workspace_id is required",
		"task_types is required",
		"models.primary is required",
		"models.worker is required",
		"models.cheap is required",
		"rhizome_url is required",
		"llm.api_key is required",
	}
	for _, r := range required {
		if !strings.Contains(msg, r) {
			t.Errorf("error missing %q, got: %s", r, msg)
		}
	}
}

// T-5: File not found
func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "reading config file") {
		t.Errorf("unexpected error: %v", err)
	}
}

// T-6: Invalid duration
func TestLoadConfig_InvalidDuration(t *testing.T) {
	yaml := `
id: a
role: b
workspace_id: ws
task_types: [x]
heartbeat_interval: "xyz"
models: {primary: m1, worker: m2, cheap: m3}
rhizome_url: http://x
llm: {api_key: sk-key}
`
	path := writeTemp(t, yaml)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid heartbeat_interval") {
		t.Errorf("unexpected error: %v", err)
	}
}

// T-7: Negative concurrency clamped to 1
func TestLoadConfig_NegativeConcurrency(t *testing.T) {
	yaml := `
id: a
role: b
workspace_id: ws
task_types: [x]
max_concurrent_tasks: -1
models: {primary: m1, worker: m2, cheap: m3}
rhizome_url: http://x
llm: {api_key: sk-key}
`
	path := writeTemp(t, yaml)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxConcurrentTasks != 1 {
		t.Errorf("MaxConcurrentTasks = %d, want 1", cfg.MaxConcurrentTasks)
	}
}

// T-EC3: Unset env var leaves placeholder, validation catches it
func TestLoadConfig_UnsetEnvVar(t *testing.T) {
	yaml := `
id: a
role: b
workspace_id: ws
task_types: [x]
models: {primary: m1, worker: m2, cheap: m3}
rhizome_url: http://x
llm:
  api_key: "${UNSET_VAR_THAT_DOES_NOT_EXIST}"
`
	path := writeTemp(t, yaml)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for unset env var in api_key")
	}
	if !strings.Contains(err.Error(), "llm.api_key is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

// T-EC2: Empty YAML fails validation for all required fields
func TestLoadConfig_EmptyYAML(t *testing.T) {
	path := writeTemp(t, "")
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "id is required") {
		t.Errorf("missing 'id is required' in: %s", msg)
	}
	if !strings.Contains(msg, "role is required") {
		t.Errorf("missing 'role is required' in: %s", msg)
	}
}
