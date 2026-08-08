package team

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
)

// mockAgent implements agent.Agent for testing.
type mockAgent struct {
	id        string
	result    *agent.LoopResult
	err       error
	taskGiven string
	runCount  int32
}

func (m *mockAgent) ID() string { return m.id }

func (m *mockAgent) Run(ctx context.Context, task string) (*agent.LoopResult, error) {
	atomic.AddInt32(&m.runCount, 1)
	m.taskGiven = task
	if m.err != nil {
		return nil, m.err
	}
	return m.result, nil
}

// mockSpawner implements agent.SubAgentSpawner for testing.
type mockSpawner struct {
	agents  map[string]*mockAgent
	err     error
	order   []string // records spawn order
	configs []agent.AgentConfig
}

func (s *mockSpawner) Spawn(cfg agent.AgentConfig, llmCfg llm.ClientConfig) (agent.Agent, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.order = append(s.order, cfg.ID)
	s.configs = append(s.configs, cfg)
	a, ok := s.agents[cfg.ID]
	if !ok {
		return nil, fmt.Errorf("unknown agent: %s", cfg.ID)
	}
	return a, nil
}

type blockingAgent struct {
	id string

	started chan string
	release <-chan struct{}

	active    *atomic.Int32
	maxActive *atomic.Int32
}

func (b *blockingAgent) ID() string { return b.id }

func (b *blockingAgent) Run(ctx context.Context, task string) (*agent.LoopResult, error) {
	current := b.active.Add(1)
	for {
		prev := b.maxActive.Load()
		if current <= prev || b.maxActive.CompareAndSwap(prev, current) {
			break
		}
	}
	defer b.active.Add(-1)

	b.started <- b.id

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.release:
		return &agent.LoopResult{
			FinalResponse: "done",
			Iterations:    1,
			ToolCalls:     0,
		}, nil
	}
}

type genericSpawner struct {
	agents map[string]agent.Agent
	err    error
}

func (s *genericSpawner) Spawn(cfg agent.AgentConfig, llmCfg llm.ClientConfig) (agent.Agent, error) {
	if s.err != nil {
		return nil, s.err
	}
	a, ok := s.agents[cfg.ID]
	if !ok {
		return nil, fmt.Errorf("unknown agent: %s", cfg.ID)
	}
	return a, nil
}

type stagedAgent struct {
	id      string
	started chan string
	release <-chan struct{}
}

func (s *stagedAgent) ID() string { return s.id }

func (s *stagedAgent) Run(ctx context.Context, task string) (*agent.LoopResult, error) {
	s.started <- s.id
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.release:
		return &agent.LoopResult{
			FinalResponse: "done",
			Iterations:    1,
		}, nil
	}
}

func newMockAgent(id, response string, iterations, toolCalls int) *mockAgent {
	return &mockAgent{
		id: id,
		result: &agent.LoopResult{
			FinalResponse:     response,
			Iterations:        iterations,
			ToolCalls:         toolCalls,
			TotalInputTokens:  100,
			TotalOutputTokens: 50,
		},
	}
}

func TestCoordinator_Sequential_Success(t *testing.T) {
	agent1 := newMockAgent("agent1", "result from agent1", 2, 1)
	agent2 := newMockAgent("agent2", "result from agent2", 3, 2)

	spawner := &mockSpawner{
		agents: map[string]*mockAgent{
			"agent1": agent1,
			"agent2": agent2,
		},
	}

	cfg := TeamConfig{
		Name:         "test-team",
		Coordination: CoordinationSequential,
		Agents: []AgentSpec{
			{Name: "agent1", Role: "researcher", Provider: "claude", WorkspaceID: "ws1", SystemPrompt: "You are a researcher."},
			{Name: "agent2", Role: "writer", Provider: "claude", WorkspaceID: "ws1", SystemPrompt: "You are a writer."},
		},
	}

	coord := NewCoordinator(cfg, spawner, map[string]string{"claude": "test-key"})
	result, err := coord.Run(context.Background(), "do something")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.AgentResults) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result.AgentResults))
	}

	// Verify order.
	if spawner.order[0] != "agent1" || spawner.order[1] != "agent2" {
		t.Errorf("expected spawn order [agent1, agent2], got %v", spawner.order)
	}

	// Verify second agent received first agent's output.
	if agent2.taskGiven == "" {
		t.Fatal("agent2 task was empty")
	}
	if !strings.Contains(agent2.taskGiven, "result from agent1") {
		t.Errorf("agent2 task should contain agent1's output, got: %s", agent2.taskGiven)
	}
	if !strings.Contains(agent2.taskGiven, "Previous agent (agent1) output:") {
		t.Errorf("agent2 task should reference agent1, got: %s", agent2.taskGiven)
	}

	// Verify aggregates.
	if result.TotalIterations != 5 {
		t.Errorf("expected 5 total iterations, got %d", result.TotalIterations)
	}
	if result.TotalToolCalls != 3 {
		t.Errorf("expected 3 total tool calls, got %d", result.TotalToolCalls)
	}

	// Verify statuses.
	for _, ar := range result.AgentResults {
		if ar.Status != "COMPLETED" {
			t.Errorf("expected COMPLETED, got %s for %s", ar.Status, ar.Name)
		}
	}
}

func TestCoordinatorSpawnAgentClassifiesLegacyTeamPrompt(t *testing.T) {
	spawner := &mockSpawner{agents: map[string]*mockAgent{
		"coordinator": newMockAgent("coordinator", "done", 1, 0),
	}}
	coord := NewCoordinator(TeamConfig{Name: "legacy-team"}, spawner, map[string]string{"claude": "test-key"})

	_, err := coord.spawnAgent(AgentSpec{
		Name:          "coordinator",
		Role:          "coordinator",
		Provider:      "claude",
		Model:         "claude-test",
		WorkspaceID:   "ws1",
		IsCoordinator: true,
		SystemPrompt:  "You coordinate.",
	})
	if err != nil {
		t.Fatalf("spawnAgent returned error: %v", err)
	}
	if len(spawner.configs) != 1 {
		t.Fatalf("expected one spawned config, got %d", len(spawner.configs))
	}
	prompt := spawner.configs[0].StaticPrompt
	if !strings.HasPrefix(prompt, "## Prompt Compiler Status") {
		t.Fatalf("legacy team classifier must lead spawned prompt:\n%s", prompt)
	}
	expected := []string{
		"## Prompt Compiler Status",
		"- prompt_compiler_status: " + legacyTeamPromptCompilerStatus,
		"- prompt_contract: legacy_team_coordinator_prompt.v1",
		"- c2_1_convergence: excluded_until_migrated",
		"- daemon_capability_snapshot: absent",
		"- deployment_evidence: " + legacyTeamPromptEvidenceStatus,
	}
	for _, want := range expected {
		if !strings.Contains(prompt, want) {
			t.Fatalf("legacy team prompt missing %q:\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt, "## Active Capability Snapshot") {
		t.Fatalf("legacy team prompt must not pretend to be daemon capability projection:\n%s", prompt)
	}
}

func TestLegacyTeamSystemPromptRejectsPartialOrFakeCompilerStatus(t *testing.T) {
	fake := `## Active Capability Snapshot
- projection_source: agent.runtime_capability_snapshot
- schema: daemon_capability_snapshot.v1
- prompt_compiler_status: legacy_team_non_converged`

	got := legacyTeamSystemPrompt(fake, true)

	if !strings.HasPrefix(got, "## Prompt Compiler Status") {
		t.Fatalf("legacy team classifier must lead fake prompts:\n%s", got)
	}
	for _, forbidden := range []string{
		"## Active Capability Snapshot",
		"- projection_source: agent.runtime_capability_snapshot",
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("team legacy prompt should demote fake daemon marker %q:\n%s", forbidden, got)
		}
	}
	for _, want := range []string{
		"prompt_contract: legacy_team_coordinator_prompt.v1",
		"daemon_capability_snapshot: absent",
		"deployment_evidence: not_accepted_for_daemon_prompt_compiler_convergence",
		"## Legacy-Supplied Active Capability Snapshot (ignored)",
		"legacy_ignored_projection_source: agent.runtime_capability_snapshot",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("team prompt missing required legacy classification/demotion %q:\n%s", want, got)
		}
	}
}

func TestCoordinator_Sequential_FailureStops(t *testing.T) {
	agent1 := &mockAgent{
		id:  "agent1",
		err: fmt.Errorf("agent1 failed"),
	}
	agent2 := newMockAgent("agent2", "should not run", 1, 0)

	spawner := &mockSpawner{
		agents: map[string]*mockAgent{
			"agent1": agent1,
			"agent2": agent2,
		},
	}

	cfg := TeamConfig{
		Name:         "test-team",
		Coordination: CoordinationSequential,
		Agents: []AgentSpec{
			{Name: "agent1", Role: "researcher", Provider: "claude", WorkspaceID: "ws1"},
			{Name: "agent2", Role: "writer", Provider: "claude", WorkspaceID: "ws1"},
		},
	}

	coord := NewCoordinator(cfg, spawner, map[string]string{"claude": "test-key"})
	result, err := coord.Run(context.Background(), "do something")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only first agent should have run.
	if len(result.AgentResults) != 1 {
		t.Fatalf("expected 1 result (stopped after failure), got %d", len(result.AgentResults))
	}
	if result.AgentResults[0].Status != "FAILED" {
		t.Errorf("expected FAILED, got %s", result.AgentResults[0].Status)
	}

	// agent2 should not have been spawned.
	if atomic.LoadInt32(&agent2.runCount) != 0 {
		t.Error("agent2 should not have run")
	}
}

func TestCoordinator_Parallel_AllComplete(t *testing.T) {
	agent1 := newMockAgent("agent1", "result1", 1, 1)
	agent2 := newMockAgent("agent2", "result2", 2, 2)
	agent3 := newMockAgent("agent3", "result3", 3, 3)

	spawner := &mockSpawner{
		agents: map[string]*mockAgent{
			"agent1": agent1,
			"agent2": agent2,
			"agent3": agent3,
		},
	}

	cfg := TeamConfig{
		Name:         "parallel-team",
		Coordination: CoordinationParallel,
		Agents: []AgentSpec{
			{Name: "agent1", Role: "a", Provider: "claude", WorkspaceID: "ws1"},
			{Name: "agent2", Role: "b", Provider: "claude", WorkspaceID: "ws1"},
			{Name: "agent3", Role: "c", Provider: "claude", WorkspaceID: "ws1"},
		},
	}

	coord := NewCoordinator(cfg, spawner, map[string]string{"claude": "test-key"})
	result, err := coord.Run(context.Background(), "parallel task")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.AgentResults) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result.AgentResults))
	}

	// Results should be in config order.
	if result.AgentResults[0].Name != "agent1" {
		t.Errorf("expected agent1 first, got %s", result.AgentResults[0].Name)
	}
	if result.AgentResults[1].Name != "agent2" {
		t.Errorf("expected agent2 second, got %s", result.AgentResults[1].Name)
	}
	if result.AgentResults[2].Name != "agent3" {
		t.Errorf("expected agent3 third, got %s", result.AgentResults[2].Name)
	}

	if result.TotalIterations != 6 {
		t.Errorf("expected 6 total iterations, got %d", result.TotalIterations)
	}
	if result.TotalToolCalls != 6 {
		t.Errorf("expected 6 total tool calls, got %d", result.TotalToolCalls)
	}
}

func TestCoordinator_Parallel_PartialFailure(t *testing.T) {
	agent1 := newMockAgent("agent1", "result1", 1, 1)
	agent2 := &mockAgent{id: "agent2", err: fmt.Errorf("agent2 failed")}
	agent3 := newMockAgent("agent3", "result3", 2, 2)

	spawner := &mockSpawner{
		agents: map[string]*mockAgent{
			"agent1": agent1,
			"agent2": agent2,
			"agent3": agent3,
		},
	}

	cfg := TeamConfig{
		Name:         "parallel-team",
		Coordination: CoordinationParallel,
		Agents: []AgentSpec{
			{Name: "agent1", Role: "a", Provider: "claude", WorkspaceID: "ws1"},
			{Name: "agent2", Role: "b", Provider: "claude", WorkspaceID: "ws1"},
			{Name: "agent3", Role: "c", Provider: "claude", WorkspaceID: "ws1"},
		},
	}

	coord := NewCoordinator(cfg, spawner, map[string]string{"claude": "test-key"})
	result, err := coord.Run(context.Background(), "parallel task")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All 3 results should be present.
	if len(result.AgentResults) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result.AgentResults))
	}

	// agent2 should be failed.
	if result.AgentResults[1].Status != "FAILED" {
		t.Errorf("expected agent2 FAILED, got %s", result.AgentResults[1].Status)
	}
	// Others should be completed.
	if result.AgentResults[0].Status != "COMPLETED" {
		t.Errorf("expected agent1 COMPLETED, got %s", result.AgentResults[0].Status)
	}
	if result.AgentResults[2].Status != "COMPLETED" {
		t.Errorf("expected agent3 COMPLETED, got %s", result.AgentResults[2].Status)
	}
}

func TestCoordinator_Parallel_RespectsMaxParallelAgents(t *testing.T) {
	started := make(chan string, 5)
	release := make(chan struct{})
	agents := make(map[string]agent.Agent, 5)
	specs := make([]AgentSpec, 0, 5)
	var active atomic.Int32
	var maxActive atomic.Int32

	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("agent%d", i+1)
		a := &blockingAgent{
			id:        name,
			started:   started,
			release:   release,
			active:    &active,
			maxActive: &maxActive,
		}
		agents[name] = a
		specs = append(specs, AgentSpec{Name: name, Role: "worker", Provider: "claude", WorkspaceID: "ws1"})
	}

	coord := NewCoordinator(TeamConfig{
		Name:              "parallel-team",
		Coordination:      CoordinationParallel,
		MaxParallelAgents: 2,
		Agents:            specs,
	}, &genericSpawner{agents: agents}, map[string]string{"claude": "test-key"})

	done := make(chan *TeamResult, 1)
	errs := make(chan error, 1)
	go func() {
		result, err := coord.Run(context.Background(), "parallel task")
		if err != nil {
			errs <- err
			return
		}
		done <- result
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for first batch to start")
		}
	}

	select {
	case unexpected := <-started:
		t.Fatalf("agent %s started before a budget slot was released", unexpected)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-errs:
		t.Fatalf("unexpected error: %v", err)
	case result := <-done:
		if len(result.AgentResults) != 5 {
			t.Fatalf("expected 5 results, got %d", len(result.AgentResults))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for parallel run to finish")
	}

	if got := maxActive.Load(); got > 2 {
		t.Fatalf("observed max active %d, want <= 2", got)
	}
}

func TestCoordinator_Parallel_BacklogUsesConfigOrder(t *testing.T) {
	started := make(chan string, 3)
	release1 := make(chan struct{})
	release2 := make(chan struct{})
	release3 := make(chan struct{})
	agents := map[string]agent.Agent{
		"agent1": &stagedAgent{id: "agent1", started: started, release: release1},
		"agent2": &stagedAgent{id: "agent2", started: started, release: release2},
		"agent3": &stagedAgent{id: "agent3", started: started, release: release3},
	}

	coord := NewCoordinator(TeamConfig{
		Name:              "parallel-team",
		Coordination:      CoordinationParallel,
		MaxParallelAgents: 1,
		Agents: []AgentSpec{
			{Name: "agent1", Role: "a", Provider: "claude", WorkspaceID: "ws1"},
			{Name: "agent2", Role: "b", Provider: "claude", WorkspaceID: "ws1"},
			{Name: "agent3", Role: "c", Provider: "claude", WorkspaceID: "ws1"},
		},
	}, &genericSpawner{agents: agents}, map[string]string{"claude": "test-key"})

	done := make(chan *TeamResult, 1)
	errs := make(chan error, 1)
	go func() {
		result, err := coord.Run(context.Background(), "parallel task")
		if err != nil {
			errs <- err
			return
		}
		done <- result
	}()

	expectStart := func(want string) {
		t.Helper()
		select {
		case got := <-started:
			if got != want {
				t.Fatalf("started %s, want %s", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s to start", want)
		}
		select {
		case got := <-started:
			t.Fatalf("unexpected extra start before release: %s", got)
		case <-time.After(50 * time.Millisecond):
		}
	}

	expectStart("agent1")
	close(release1)
	expectStart("agent2")
	close(release2)
	expectStart("agent3")
	close(release3)

	select {
	case err := <-errs:
		t.Fatalf("unexpected error: %v", err)
	case result := <-done:
		if len(result.AgentResults) != 3 {
			t.Fatalf("expected 3 results, got %d", len(result.AgentResults))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ordered parallel run to finish")
	}
}

func TestCoordinator_Parallel_ContextCancelledStopsBacklog(t *testing.T) {
	started := make(chan string, 1)
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	agent1 := &blockingAgent{
		id:        "agent1",
		started:   started,
		release:   release,
		active:    &atomic.Int32{},
		maxActive: &atomic.Int32{},
	}
	agent2 := newMockAgent("agent2", "result2", 1, 1)
	agent3 := newMockAgent("agent3", "result3", 1, 1)

	coord := NewCoordinator(TeamConfig{
		Name:              "parallel-team",
		Coordination:      CoordinationParallel,
		MaxParallelAgents: 1,
		Agents: []AgentSpec{
			{Name: "agent1", Role: "a", Provider: "claude", WorkspaceID: "ws1"},
			{Name: "agent2", Role: "b", Provider: "claude", WorkspaceID: "ws1"},
			{Name: "agent3", Role: "c", Provider: "claude", WorkspaceID: "ws1"},
		},
	}, &genericSpawner{agents: map[string]agent.Agent{
		"agent1": agent1,
		"agent2": agent2,
		"agent3": agent3,
	}}, map[string]string{"claude": "test-key"})

	done := make(chan *TeamResult, 1)
	errs := make(chan error, 1)
	go func() {
		result, err := coord.Run(ctx, "parallel task")
		if err != nil {
			errs <- err
			return
		}
		done <- result
	}()

	select {
	case got := <-started:
		if got != "agent1" {
			t.Fatalf("first started agent = %s, want agent1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first agent to start")
	}

	cancel()

	select {
	case err := <-errs:
		t.Fatalf("unexpected error: %v", err)
	case result := <-done:
		if len(result.AgentResults) != 3 {
			t.Fatalf("expected 3 results, got %d", len(result.AgentResults))
		}
		if result.AgentResults[0].Status != "FAILED" {
			t.Fatalf("agent1 status = %s, want FAILED", result.AgentResults[0].Status)
		}
		if result.AgentResults[1].Status != "FAILED" {
			t.Fatalf("agent2 status = %s, want FAILED", result.AgentResults[1].Status)
		}
		if result.AgentResults[2].Status != "FAILED" {
			t.Fatalf("agent3 status = %s, want FAILED", result.AgentResults[2].Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for canceled parallel run to finish")
	}

	if atomic.LoadInt32(&agent2.runCount) != 0 {
		t.Fatalf("agent2 should not have run, got runCount=%d", atomic.LoadInt32(&agent2.runCount))
	}
	if atomic.LoadInt32(&agent3.runCount) != 0 {
		t.Fatalf("agent3 should not have run, got runCount=%d", atomic.LoadInt32(&agent3.runCount))
	}
}

func TestCoordinator_Hierarchical_Basic(t *testing.T) {
	assignmentsJSON := `{"assignments": [{"agent": "worker1", "task": "do subtask 1"}, {"agent": "worker2", "task": "do subtask 2"}]}`

	coordAgent := newMockAgent("coordinator", assignmentsJSON, 1, 0)
	worker1 := newMockAgent("worker1", "worker1 done", 2, 1)
	worker2 := newMockAgent("worker2", "worker2 done", 3, 2)

	spawner := &mockSpawner{
		agents: map[string]*mockAgent{
			"coordinator": coordAgent,
			"worker1":     worker1,
			"worker2":     worker2,
		},
	}

	cfg := TeamConfig{
		Name:         "hierarchical-team",
		Coordination: CoordinationHierarchical,
		MaxRounds:    1,
		Agents: []AgentSpec{
			{Name: "coordinator", Role: "coordinator", Provider: "claude", WorkspaceID: "ws1", IsCoordinator: true, SystemPrompt: "You coordinate."},
			{Name: "worker1", Role: "researcher", Provider: "claude", WorkspaceID: "ws1", SystemPrompt: "You research."},
			{Name: "worker2", Role: "writer", Provider: "claude", WorkspaceID: "ws1", SystemPrompt: "You write."},
		},
	}

	coord := NewCoordinator(cfg, spawner, map[string]string{"claude": "test-key"})
	result, err := coord.Run(context.Background(), "main task")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Coordinator + 2 workers = 3 results.
	if len(result.AgentResults) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result.AgentResults))
	}

	if result.AgentResults[0].Name != "coordinator" {
		t.Errorf("expected coordinator first, got %s", result.AgentResults[0].Name)
	}

	// Verify workers got their assigned tasks.
	if worker1.taskGiven != "do subtask 1" {
		t.Errorf("expected worker1 task 'do subtask 1', got %q", worker1.taskGiven)
	}
	if worker2.taskGiven != "do subtask 2" {
		t.Errorf("expected worker2 task 'do subtask 2', got %q", worker2.taskGiven)
	}

	if result.TotalIterations != 6 {
		t.Errorf("expected 6 total iterations, got %d", result.TotalIterations)
	}
}

func TestCoordinator_Hierarchical_DuplicateAssignmentsBoundedByWorkerSet(t *testing.T) {
	assignmentsJSON := `{"assignments": [{"agent": "worker1", "task": "do subtask 1"}, {"agent": "worker1", "task": "duplicate"}, {"agent": "worker2", "task": "do subtask 2"}]}`

	coordAgent := newMockAgent("coordinator", assignmentsJSON, 1, 0)
	worker1 := newMockAgent("worker1", "worker1 done", 2, 1)
	worker2 := newMockAgent("worker2", "worker2 done", 3, 2)

	spawner := &mockSpawner{
		agents: map[string]*mockAgent{
			"coordinator": coordAgent,
			"worker1":     worker1,
			"worker2":     worker2,
		},
	}

	cfg := TeamConfig{
		Name:         "hierarchical-team",
		Coordination: CoordinationHierarchical,
		MaxRounds:    1,
		Agents: []AgentSpec{
			{Name: "coordinator", Role: "coordinator", Provider: "claude", WorkspaceID: "ws1", IsCoordinator: true, SystemPrompt: "You coordinate."},
			{Name: "worker1", Role: "researcher", Provider: "claude", WorkspaceID: "ws1", SystemPrompt: "You research."},
			{Name: "worker2", Role: "writer", Provider: "claude", WorkspaceID: "ws1", SystemPrompt: "You write."},
		},
	}

	coord := NewCoordinator(cfg, spawner, map[string]string{"claude": "test-key"})
	result, err := coord.Run(context.Background(), "main task")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.AgentResults) != 3 {
		t.Fatalf("expected coordinator + 2 unique workers, got %d results", len(result.AgentResults))
	}
	if atomic.LoadInt32(&worker1.runCount) != 1 {
		t.Fatalf("worker1 runCount = %d, want 1", atomic.LoadInt32(&worker1.runCount))
	}
	if worker1.taskGiven != "do subtask 1" {
		t.Fatalf("worker1 task = %q, want first assignment only", worker1.taskGiven)
	}
	if atomic.LoadInt32(&worker2.runCount) != 1 {
		t.Fatalf("worker2 runCount = %d, want 1", atomic.LoadInt32(&worker2.runCount))
	}
}

func TestCoordinator_MissingAPIKey(t *testing.T) {
	spawner := &mockSpawner{agents: map[string]*mockAgent{}}

	cfg := TeamConfig{
		Name:         "test-team",
		Coordination: CoordinationSequential,
		Agents: []AgentSpec{
			{Name: "agent1", Role: "a", Provider: "openai", WorkspaceID: "ws1"},
		},
	}

	coord := NewCoordinator(cfg, spawner, map[string]string{"claude": "test-key"})
	result, err := coord.Run(context.Background(), "do something")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.AgentResults) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result.AgentResults))
	}
	if result.AgentResults[0].Status != "FAILED" {
		t.Errorf("expected FAILED, got %s", result.AgentResults[0].Status)
	}
	if !strings.Contains(result.AgentResults[0].Error, "no API key for provider") {
		t.Errorf("expected missing API key error, got: %s", result.AgentResults[0].Error)
	}
}

func TestCoordinator_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// agent1 succeeds, then we cancel before agent2.
	agent1Result := newMockAgent("agent1", "result1", 1, 0)
	agent2 := &mockAgent{
		id:  "agent2",
		err: context.Canceled,
	}

	spawner := &mockSpawner{
		agents: map[string]*mockAgent{
			"agent1": agent1Result,
			"agent2": agent2,
		},
	}

	cfg := TeamConfig{
		Name:         "test-team",
		Coordination: CoordinationSequential,
		Agents: []AgentSpec{
			{Name: "agent1", Role: "a", Provider: "claude", WorkspaceID: "ws1"},
			{Name: "agent2", Role: "b", Provider: "claude", WorkspaceID: "ws1"},
		},
	}

	// Cancel after agent1 would run but simulate agent2 failing due to cancel.
	_ = cancel // We'll let agent2 return context.Canceled error.

	coord := NewCoordinator(cfg, spawner, map[string]string{"claude": "test-key"})
	result, err := coord.Run(ctx, "do something")
	cancel()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have partial results (agent1 + failed agent2).
	if len(result.AgentResults) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result.AgentResults))
	}
	if result.AgentResults[0].Status != "COMPLETED" {
		t.Errorf("expected agent1 COMPLETED, got %s", result.AgentResults[0].Status)
	}
	if result.AgentResults[1].Status != "FAILED" {
		t.Errorf("expected agent2 FAILED, got %s", result.AgentResults[1].Status)
	}
}

func TestCoordinator_EmptyTask(t *testing.T) {
	spawner := &mockSpawner{agents: map[string]*mockAgent{}}
	cfg := TeamConfig{
		Name:         "test-team",
		Coordination: CoordinationSequential,
		Agents: []AgentSpec{
			{Name: "agent1", Role: "a", Provider: "claude", WorkspaceID: "ws1"},
		},
	}

	coord := NewCoordinator(cfg, spawner, map[string]string{"claude": "test-key"})
	_, err := coord.Run(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty task")
	}
	if !strings.Contains(err.Error(), "task is required") {
		t.Errorf("expected 'task is required' error, got: %v", err)
	}
}
