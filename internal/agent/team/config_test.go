package team

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// helper to write a temp JSON file and return its path.
func writeTempJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	path := filepath.Join(t.TempDir(), "team.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func validConfig() TeamConfig {
	return TeamConfig{
		Name:         "test-team",
		Coordination: CoordinationSequential,
		MaxRounds:    2,
		Agents: []AgentSpec{
			{
				Name:          "agent-a",
				Role:          "researcher",
				Provider:      "claude",
				Model:         "claude-sonnet-4-20250514",
				SystemPrompt:  "Research the topic.",
				Tools:         []string{"search"},
				MaxIterations: 10,
				WorkspaceID:   "ws-1",
			},
			{
				Name:          "agent-b",
				Role:          "coder",
				Provider:      "openai",
				Model:         "gpt-4o",
				SystemPrompt:  "Write code.",
				MaxIterations: 20,
				WorkspaceID:   "ws-2",
			},
		},
	}
}

// T-1: TestLoadTeamConfig_ValidFile verifies R-4.
func TestLoadTeamConfig_ValidFile(t *testing.T) {
	original := validConfig()
	path := writeTempJSON(t, original)

	cfg, err := LoadTeamConfig(path)
	if err != nil {
		t.Fatalf("LoadTeamConfig: %v", err)
	}

	if cfg.Name != "test-team" {
		t.Errorf("Name = %q, want %q", cfg.Name, "test-team")
	}
	if cfg.Coordination != CoordinationSequential {
		t.Errorf("Coordination = %q, want %q", cfg.Coordination, CoordinationSequential)
	}
	if cfg.MaxRounds != 2 {
		t.Errorf("MaxRounds = %d, want 2", cfg.MaxRounds)
	}
	if len(cfg.Agents) != 2 {
		t.Fatalf("len(Agents) = %d, want 2", len(cfg.Agents))
	}

	a := cfg.Agents[0]
	if a.Name != "agent-a" || a.Role != "researcher" || a.Provider != "claude" {
		t.Errorf("agent-a fields mismatch: %+v", a)
	}
	if a.MaxIterations != 10 {
		t.Errorf("agent-a MaxIterations = %d, want 10", a.MaxIterations)
	}

	b := cfg.Agents[1]
	if b.Name != "agent-b" || b.Provider != "openai" || b.WorkspaceID != "ws-2" {
		t.Errorf("agent-b fields mismatch: %+v", b)
	}
}

func TestLoadTeamConfig_FileNotFound(t *testing.T) {
	_, err := LoadTeamConfig("/nonexistent/path/team.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadTeamConfig_InvalidJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	os.WriteFile(path, []byte("{invalid"), 0644)

	_, err := LoadTeamConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestLoadTeamConfig_DefaultsApplied(t *testing.T) {
	// MaxRounds=0 and MaxIterations=0 should get defaults.
	raw := `{"name":"t","coordination":"parallel","agents":[{"name":"a","role":"r","provider":"claude","system_prompt":"p","workspace_id":"w"}]}`
	path := filepath.Join(t.TempDir(), "team.json")
	os.WriteFile(path, []byte(raw), 0644)

	cfg, err := LoadTeamConfig(path)
	if err != nil {
		t.Fatalf("LoadTeamConfig: %v", err)
	}
	if cfg.MaxRounds != 1 {
		t.Errorf("MaxRounds = %d, want 1 (default)", cfg.MaxRounds)
	}
	if cfg.Agents[0].MaxIterations != 50 {
		t.Errorf("MaxIterations = %d, want 50 (default)", cfg.Agents[0].MaxIterations)
	}
	if cfg.MaxParallelAgents != 1 {
		t.Errorf("MaxParallelAgents = %d, want 1 (default)", cfg.MaxParallelAgents)
	}
}

// T-2: TestValidateTeamConfig_Valid verifies R-5.
func TestValidateTeamConfig_Valid(t *testing.T) {
	cfg := validConfig()
	if err := ValidateTeamConfig(&cfg); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

// T-3: TestValidateTeamConfig_EmptyName verifies R-5.
func TestValidateTeamConfig_EmptyName(t *testing.T) {
	cfg := validConfig()
	cfg.Name = ""
	err := ValidateTeamConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for empty name")
	}
	if !strings.Contains(err.Error(), "team name is required") {
		t.Errorf("error = %q, want mention of team name", err)
	}
}

// T-4: TestValidateTeamConfig_InvalidCoordination verifies R-5, EC-5.
func TestValidateTeamConfig_InvalidCoordination(t *testing.T) {
	cfg := validConfig()
	cfg.Coordination = "round-robin"
	err := ValidateTeamConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for invalid coordination")
	}
	if !strings.Contains(err.Error(), "invalid coordination pattern") {
		t.Errorf("error = %q, want mention of coordination pattern", err)
	}
}

// T-5: TestValidateTeamConfig_NoAgents verifies R-5, EC-1.
func TestValidateTeamConfig_NoAgents(t *testing.T) {
	cfg := validConfig()
	cfg.Agents = nil
	err := ValidateTeamConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for no agents")
	}
	if !strings.Contains(err.Error(), "at least one agent") {
		t.Errorf("error = %q, want mention of agents", err)
	}
}

// T-6: TestValidateTeamConfig_HierarchicalNoCoordinator verifies R-5, EC-2.
func TestValidateTeamConfig_HierarchicalNoCoordinator(t *testing.T) {
	cfg := validConfig()
	cfg.Coordination = CoordinationHierarchical
	// No agent has IsCoordinator=true.
	err := ValidateTeamConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for hierarchical without coordinator")
	}
	if !strings.Contains(err.Error(), "coordinator") {
		t.Errorf("error = %q, want mention of coordinator", err)
	}
}

// T-7: TestValidateTeamConfig_HierarchicalMultipleCoordinators verifies EC-3.
func TestValidateTeamConfig_HierarchicalMultipleCoordinators(t *testing.T) {
	cfg := validConfig()
	cfg.Coordination = CoordinationHierarchical
	cfg.Agents[0].IsCoordinator = true
	cfg.Agents[1].IsCoordinator = true
	err := ValidateTeamConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for multiple coordinators")
	}
	if !strings.Contains(err.Error(), "exactly one coordinator") {
		t.Errorf("error = %q, want mention of exactly one coordinator", err)
	}
}

// T-8: TestValidateTeamConfig_DuplicateAgentNames verifies EC-4.
func TestValidateTeamConfig_DuplicateAgentNames(t *testing.T) {
	cfg := validConfig()
	cfg.Agents[1].Name = cfg.Agents[0].Name // duplicate
	err := ValidateTeamConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for duplicate names")
	}
	if !strings.Contains(err.Error(), "duplicate name") {
		t.Errorf("error = %q, want mention of duplicate name", err)
	}
}

// T-9: TestDefaultTeamConfig_IsValid verifies R-6.
func TestDefaultTeamConfig_IsValid(t *testing.T) {
	cfg := DefaultTeamConfig()
	if err := ValidateTeamConfig(&cfg); err != nil {
		t.Fatalf("DefaultTeamConfig should be valid, got: %v", err)
	}
}

func TestValidateTeamConfig_EmptyWorkspaceID(t *testing.T) {
	cfg := validConfig()
	cfg.Agents[0].WorkspaceID = ""
	err := ValidateTeamConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for empty workspace_id")
	}
	if !strings.Contains(err.Error(), "workspace_id") {
		t.Errorf("error = %q, want mention of workspace_id", err)
	}
}

func TestValidateTeamConfig_InvalidProvider(t *testing.T) {
	cfg := validConfig()
	cfg.Agents[0].Provider = "gemini"
	err := ValidateTeamConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for invalid provider")
	}
	if !strings.Contains(err.Error(), "provider") {
		t.Errorf("error = %q, want mention of provider", err)
	}
}

func TestValidateTeamConfig_MultipleErrors(t *testing.T) {
	cfg := TeamConfig{} // everything wrong
	err := ValidateTeamConfig(&cfg)
	if err == nil {
		t.Fatal("expected errors")
	}
	errStr := err.Error()
	// Should report both name and agents and coordination.
	if !strings.Contains(errStr, "team name") {
		t.Errorf("missing 'team name' in: %s", errStr)
	}
	if !strings.Contains(errStr, "at least one agent") {
		t.Errorf("missing 'at least one agent' in: %s", errStr)
	}
	if !strings.Contains(errStr, "coordination pattern") {
		t.Errorf("missing 'coordination pattern' in: %s", errStr)
	}
}

func TestValidateTeamConfig_HierarchicalWithOneCoordinator(t *testing.T) {
	cfg := validConfig()
	cfg.Coordination = CoordinationHierarchical
	cfg.Agents[0].IsCoordinator = true
	if err := ValidateTeamConfig(&cfg); err != nil {
		t.Fatalf("expected valid hierarchical config, got: %v", err)
	}
}

func TestValidateTeamConfig_NegativeMaxParallelAgents(t *testing.T) {
	cfg := validConfig()
	cfg.Coordination = CoordinationParallel
	cfg.MaxParallelAgents = -1
	err := ValidateTeamConfig(&cfg)
	if err == nil {
		t.Fatal("expected error for negative max_parallel_agents")
	}
	if !strings.Contains(err.Error(), "max_parallel_agents") {
		t.Fatalf("error = %q, want mention of max_parallel_agents", err)
	}
}

func TestTeamConfig_EffectiveParallelism_DefaultBudget(t *testing.T) {
	cfg := TeamConfig{
		Coordination: CoordinationParallel,
		Agents:       make([]AgentSpec, 32),
	}
	if got := cfg.effectiveParallelism(); got != defaultParallelAgentBudget {
		t.Fatalf("effectiveParallelism() = %d, want %d", got, defaultParallelAgentBudget)
	}
}

func TestTeamConfig_EffectiveParallelism_ExplicitBudget(t *testing.T) {
	cfg := TeamConfig{
		Coordination:      CoordinationParallel,
		MaxParallelAgents: 3,
		Agents:            make([]AgentSpec, 5),
	}
	if got := cfg.effectiveParallelism(); got != 3 {
		t.Fatalf("effectiveParallelism() = %d, want 3", got)
	}
}
