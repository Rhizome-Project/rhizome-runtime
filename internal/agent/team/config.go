// Package team defines the team configuration model for multi-agent orchestration.
// It is a pure data package with no runtime dependencies.
package team

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

const defaultParallelAgentBudget = 8

// CoordinationPattern defines how agents in a team coordinate.
type CoordinationPattern string

const (
	// CoordinationSequential means agents execute in order, each sees previous output.
	CoordinationSequential CoordinationPattern = "sequential"
	// CoordinationParallel means agents execute concurrently, results collected.
	CoordinationParallel CoordinationPattern = "parallel"
	// CoordinationHierarchical means a coordinator agent dispatches to worker agents.
	CoordinationHierarchical CoordinationPattern = "hierarchical"
)

// AgentSpec defines an agent within a team configuration.
type AgentSpec struct {
	Name          string   `json:"name"`
	Role          string   `json:"role"`
	Provider      string   `json:"provider"`
	Model         string   `json:"model,omitempty"`
	SystemPrompt  string   `json:"system_prompt"`
	Tools         []string `json:"tools,omitempty"`
	MaxIterations int      `json:"max_iterations,omitempty"`
	WorkspaceID   string   `json:"workspace_id"`
	IsCoordinator bool     `json:"is_coordinator,omitempty"`
}

// TeamConfig defines a team of agents and their coordination pattern.
type TeamConfig struct {
	Name              string              `json:"name"`
	Coordination      CoordinationPattern `json:"coordination"`
	Agents            []AgentSpec         `json:"agents"`
	MaxRounds         int                 `json:"max_rounds,omitempty"`
	MaxParallelAgents int                 `json:"max_parallel_agents,omitempty"`
}

// LoadTeamConfig reads and parses a team configuration from a JSON file.
func LoadTeamConfig(path string) (*TeamConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading team config: %w", err)
	}

	var cfg TeamConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing team config: %w", err)
	}

	// Apply defaults.
	applyDefaults(&cfg)

	return &cfg, nil
}

// applyDefaults sets default values for fields that were omitted or zero.
func applyDefaults(cfg *TeamConfig) {
	if cfg.MaxRounds <= 0 {
		cfg.MaxRounds = 1
	}
	if cfg.Coordination == CoordinationParallel && cfg.MaxParallelAgents == 0 {
		cfg.MaxParallelAgents = defaultParallelismBudget(len(cfg.Agents))
	}
	for i := range cfg.Agents {
		if cfg.Agents[i].MaxIterations <= 0 {
			cfg.Agents[i].MaxIterations = 50
		}
	}
}

// ValidateTeamConfig checks a TeamConfig for structural errors.
// It returns a combined error describing all problems found.
func ValidateTeamConfig(cfg *TeamConfig) error {
	var errs []error

	if cfg.Name == "" {
		errs = append(errs, errors.New("team name is required"))
	}

	if !isValidPattern(cfg.Coordination) {
		errs = append(errs, fmt.Errorf("invalid coordination pattern: %q", cfg.Coordination))
	}

	if len(cfg.Agents) == 0 {
		errs = append(errs, errors.New("at least one agent is required"))
	}
	if cfg.MaxParallelAgents < 0 {
		errs = append(errs, fmt.Errorf("max_parallel_agents must be >= 0, got %d", cfg.MaxParallelAgents))
	}

	// Check agent-level constraints.
	names := make(map[string]bool)
	coordinatorCount := 0

	for i, a := range cfg.Agents {
		if a.Name == "" {
			errs = append(errs, fmt.Errorf("agent[%d]: name is required", i))
		} else if names[a.Name] {
			errs = append(errs, fmt.Errorf("agent[%d]: duplicate name %q", i, a.Name))
		} else {
			names[a.Name] = true
		}

		if a.WorkspaceID == "" {
			errs = append(errs, fmt.Errorf("agent[%d] (%s): workspace_id is required", i, a.Name))
		}

		if a.Provider != "claude" && a.Provider != "openai" {
			errs = append(errs, fmt.Errorf("agent[%d] (%s): provider must be \"claude\" or \"openai\", got %q", i, a.Name, a.Provider))
		}

		if a.IsCoordinator {
			coordinatorCount++
		}
	}

	// Hierarchical pattern constraints.
	if cfg.Coordination == CoordinationHierarchical {
		if coordinatorCount == 0 {
			errs = append(errs, errors.New("hierarchical coordination requires exactly one coordinator agent"))
		} else if coordinatorCount > 1 {
			errs = append(errs, fmt.Errorf("hierarchical coordination requires exactly one coordinator agent, found %d", coordinatorCount))
		}
	}

	return errors.Join(errs...)
}

// isValidPattern checks whether a CoordinationPattern is one of the known values.
func isValidPattern(p CoordinationPattern) bool {
	switch p {
	case CoordinationSequential, CoordinationParallel, CoordinationHierarchical:
		return true
	}
	return false
}

func defaultParallelismBudget(agentCount int) int {
	if agentCount <= 0 {
		return 0
	}
	if agentCount < defaultParallelAgentBudget {
		return agentCount
	}
	return defaultParallelAgentBudget
}

func (cfg TeamConfig) effectiveParallelism() int {
	if cfg.Coordination != CoordinationParallel {
		return 0
	}
	if len(cfg.Agents) == 0 {
		return 0
	}
	if cfg.MaxParallelAgents > 0 {
		if cfg.MaxParallelAgents < len(cfg.Agents) {
			return cfg.MaxParallelAgents
		}
		return len(cfg.Agents)
	}
	return defaultParallelismBudget(len(cfg.Agents))
}

// DefaultTeamConfig returns a minimal valid team config for documentation and testing.
func DefaultTeamConfig() TeamConfig {
	return TeamConfig{
		Name:         "default-team",
		Coordination: CoordinationSequential,
		MaxRounds:    1,
		Agents: []AgentSpec{
			{
				Name:          "default-agent",
				Role:          "assistant",
				Provider:      "claude",
				SystemPrompt:  "You are a helpful assistant.",
				MaxIterations: 50,
				WorkspaceID:   "default-workspace",
			},
		},
	}
}
