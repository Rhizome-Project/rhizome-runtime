package team

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
)

type p8CBenchSpawner struct {
	mu     sync.Mutex
	agents map[string]*mockAgent
}

func (s *p8CBenchSpawner) Spawn(cfg agent.AgentConfig, llmCfg llm.ClientConfig) (agent.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[cfg.ID]
	if !ok {
		return nil, fmt.Errorf("unknown agent: %s", cfg.ID)
	}
	return a, nil
}

func BenchmarkP8CCoordinatorOverhead(b *testing.B) {
	for _, agentCount := range []int{4, 16, 32, 64} {
		b.Run(fmt.Sprintf("sequential_%d_agents", agentCount), func(b *testing.B) {
			benchmarkP8CCoordinator(b, CoordinationSequential, agentCount, 0)
		})
		b.Run(fmt.Sprintf("parallel_unbounded_%d_agents", agentCount), func(b *testing.B) {
			benchmarkP8CCoordinator(b, CoordinationParallel, agentCount, agentCount)
		})
		if agentCount > defaultParallelAgentBudget {
			b.Run(fmt.Sprintf("parallel_budgeted_%d_agents", agentCount), func(b *testing.B) {
				benchmarkP8CCoordinator(b, CoordinationParallel, agentCount, defaultParallelAgentBudget)
			})
		}
		if agentCount <= 16 {
			b.Run(fmt.Sprintf("hierarchical_%d_workers", agentCount), func(b *testing.B) {
				benchmarkP8CCoordinator(b, CoordinationHierarchical, agentCount, 0)
			})
		}
	}
}

func benchmarkP8CCoordinator(b *testing.B, pattern CoordinationPattern, agentCount int, maxParallelAgents int) {
	b.Helper()
	cfg, spawner := newP8CBenchmarkFixture(pattern, agentCount, maxParallelAgents)
	coord := NewCoordinator(cfg, spawner, map[string]string{"claude": "test-key"})
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := coord.Run(ctx, "p8c scheduler benchmark")
		if err != nil {
			b.Fatalf("coordinator run failed: %v", err)
		}
		if len(result.AgentResults) == 0 {
			b.Fatalf("expected agent results, got %+v", result)
		}
	}
}

func newP8CBenchmarkFixture(pattern CoordinationPattern, agentCount int, maxParallelAgents int) (TeamConfig, *p8CBenchSpawner) {
	agents := make(map[string]*mockAgent, agentCount+1)
	specs := make([]AgentSpec, 0, agentCount+1)

	switch pattern {
	case CoordinationHierarchical:
		assignments := make([]hierarchicalAssignment, 0, agentCount)
		for i := 0; i < agentCount; i++ {
			name := fmt.Sprintf("worker-%02d", i)
			assignments = append(assignments, hierarchicalAssignment{
				Agent: name,
				Task:  fmt.Sprintf("subtask-%02d", i),
			})
			agents[name] = newMockAgent(name, "worker complete", 1, 1)
			specs = append(specs, AgentSpec{
				Name:         name,
				Role:         "worker",
				Provider:     "claude",
				WorkspaceID:  "ws-bench",
				SystemPrompt: "You are a worker.",
			})
		}
		payload, _ := json.Marshal(hierarchicalAssignments{Assignments: assignments})
		agents["coordinator"] = newMockAgent("coordinator", string(payload), 1, 0)
		specs = append([]AgentSpec{{
			Name:          "coordinator",
			Role:          "coordinator",
			Provider:      "claude",
			WorkspaceID:   "ws-bench",
			IsCoordinator: true,
			SystemPrompt:  "You coordinate.",
		}}, specs...)
	default:
		for i := 0; i < agentCount; i++ {
			name := fmt.Sprintf("agent-%02d", i)
			agents[name] = newMockAgent(name, "done", 1, 1)
			specs = append(specs, AgentSpec{
				Name:         name,
				Role:         "worker",
				Provider:     "claude",
				WorkspaceID:  "ws-bench",
				SystemPrompt: "You work.",
			})
		}
	}

	return TeamConfig{
		Name:              "p8c-bench",
		Coordination:      pattern,
		MaxRounds:         1,
		MaxParallelAgents: maxParallelAgents,
		Agents:            specs,
	}, &p8CBenchSpawner{agents: agents}
}
