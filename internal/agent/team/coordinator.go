package team

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
)

// AgentResult holds the result of a single agent's execution within a team.
type AgentResult struct {
	Name          string `json:"name"`
	Role          string `json:"role"`
	Status        string `json:"status"`
	FinalResponse string `json:"final_response"`
	Iterations    int    `json:"iterations"`
	ToolCalls     int    `json:"tool_calls"`
	Error         string `json:"error,omitempty"`
}

// TeamResult holds the aggregated results of a team execution.
type TeamResult struct {
	TeamName          string        `json:"team_name"`
	Coordination      string        `json:"coordination"`
	AgentResults      []AgentResult `json:"agent_results"`
	TotalIterations   int           `json:"total_iterations"`
	TotalToolCalls    int           `json:"total_tool_calls"`
	TotalInputTokens  int           `json:"total_input_tokens"`
	TotalOutputTokens int           `json:"total_output_tokens"`
}

// Coordinator orchestrates a team of agents according to a coordination pattern.
type Coordinator struct {
	config  TeamConfig
	spawner agent.SubAgentSpawner
	apiKeys map[string]string
}

const (
	legacyTeamPromptCompilerStatus = "legacy_team_non_converged"
	legacyTeamPromptEvidenceStatus = "not_accepted_for_daemon_prompt_compiler_convergence"
)

// NewCoordinator creates a new Coordinator.
func NewCoordinator(cfg TeamConfig, spawner agent.SubAgentSpawner, apiKeys map[string]string) *Coordinator {
	return &Coordinator{
		config:  cfg,
		spawner: spawner,
		apiKeys: apiKeys,
	}
}

// Run executes the team according to the coordination pattern.
func (c *Coordinator) Run(ctx context.Context, task string) (*TeamResult, error) {
	if task == "" {
		return nil, fmt.Errorf("task is required")
	}

	switch c.config.Coordination {
	case CoordinationSequential:
		return c.runSequential(ctx, task)
	case CoordinationParallel:
		return c.runParallel(ctx, task)
	case CoordinationHierarchical:
		return c.runHierarchical(ctx, task)
	default:
		return nil, fmt.Errorf("unsupported coordination pattern: %q", c.config.Coordination)
	}
}

func (c *Coordinator) spawnAgent(spec AgentSpec) (agent.Agent, error) {
	apiKey, ok := c.apiKeys[spec.Provider]
	if !ok {
		return nil, fmt.Errorf("no API key for provider: %s", spec.Provider)
	}

	cfg := agent.AgentConfig{
		ID:           spec.Name,
		WorkspaceID:  spec.WorkspaceID,
		DisplayName:  spec.Name,
		Role:         spec.Role,
		StaticPrompt: legacyTeamSystemPrompt(spec.SystemPrompt, spec.IsCoordinator),
		LoopConfig: agent.LoopConfig{
			MaxIterations: spec.MaxIterations,
		},
	}

	llmCfg := llm.ClientConfig{
		Provider: llm.ProviderType(spec.Provider),
		APIKey:   apiKey,
		Model:    spec.Model,
	}

	return c.spawner.Spawn(cfg, llmCfg)
}

func legacyTeamSystemPrompt(prompt string, coordinator bool) string {
	prompt = strings.TrimSpace(prompt)
	notice := legacyTeamPromptCompilerNotice(coordinator)
	if prompt == "" {
		return notice
	}
	return notice + "\n\n## Legacy User-Supplied Team Prompt\n" + agent.DemoteLegacyDaemonProjectionMarkers(prompt)
}

func legacyTeamPromptCompilerNotice(coordinator bool) string {
	contract := "legacy_team_agent_prompt.v1"
	if coordinator {
		contract = "legacy_team_coordinator_prompt.v1"
	}
	return strings.Join([]string{
		"## Prompt Compiler Status",
		"- prompt_compiler_status: " + legacyTeamPromptCompilerStatus,
		"- prompt_contract: " + contract,
		"- c2_1_convergence: excluded_until_migrated",
		"- daemon_capability_snapshot: absent",
		"- deployment_evidence: " + legacyTeamPromptEvidenceStatus,
		"- note: This team prompt is a legacy internal agent/team prompt, not the agent daemon active capability snapshot compiler.",
	}, "\n")
}

func (c *Coordinator) buildResult(results []AgentResult) *TeamResult {
	tr := &TeamResult{
		TeamName:     c.config.Name,
		Coordination: string(c.config.Coordination),
		AgentResults: results,
	}
	for _, r := range results {
		tr.TotalIterations += r.Iterations
		tr.TotalToolCalls += r.ToolCalls
	}
	return tr
}

func runAgent(ctx context.Context, a agent.Agent, spec AgentSpec, task string) AgentResult {
	result, err := a.Run(ctx, task)
	if err != nil {
		return AgentResult{
			Name:   spec.Name,
			Role:   spec.Role,
			Status: "FAILED",
			Error:  err.Error(),
		}
	}

	ar := AgentResult{
		Name:          spec.Name,
		Role:          spec.Role,
		Status:        "COMPLETED",
		FinalResponse: result.FinalResponse,
		Iterations:    result.Iterations,
		ToolCalls:     result.ToolCalls,
	}
	if result.Error != nil {
		ar.Status = "FAILED"
		ar.Error = result.Error.Error()
	}
	return ar
}

func canceledAgentResult(spec AgentSpec, err error) AgentResult {
	return AgentResult{
		Name:   spec.Name,
		Role:   spec.Role,
		Status: "FAILED",
		Error:  err.Error(),
	}
}

func (c *Coordinator) runSequential(ctx context.Context, task string) (*TeamResult, error) {
	var results []AgentResult
	prevResponse := ""

	for i, spec := range c.config.Agents {
		agentTask := task
		if i > 0 && prevResponse != "" {
			agentTask = fmt.Sprintf("Previous agent (%s) output:\n%s\n\nYour task:\n%s",
				c.config.Agents[i-1].Name, prevResponse, task)
		}

		a, err := c.spawnAgent(spec)
		if err != nil {
			results = append(results, AgentResult{
				Name:   spec.Name,
				Role:   spec.Role,
				Status: "FAILED",
				Error:  err.Error(),
			})
			return c.buildResult(results), nil
		}

		ar := runAgent(ctx, a, spec, agentTask)
		results = append(results, ar)

		if ar.Status == "FAILED" {
			return c.buildResult(results), nil
		}

		prevResponse = ar.FinalResponse
	}

	return c.buildResult(results), nil
}

func (c *Coordinator) runParallel(ctx context.Context, task string) (*TeamResult, error) {
	agents := c.config.Agents
	results := make([]AgentResult, len(agents))
	if len(agents) == 0 {
		return c.buildResult(results), nil
	}

	workerCount := c.config.effectiveParallelism()
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(agents) {
		workerCount = len(agents)
	}
	var wg sync.WaitGroup
	jobs := make(chan int)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case idx, ok := <-jobs:
					if !ok {
						return
					}
					if err := ctx.Err(); err != nil {
						results[idx] = canceledAgentResult(agents[idx], err)
						return
					}

					spec := agents[idx]
					a, err := c.spawnAgent(spec)
					if err != nil {
						results[idx] = AgentResult{
							Name:   spec.Name,
							Role:   spec.Role,
							Status: "FAILED",
							Error:  err.Error(),
						}
						continue
					}

					results[idx] = runAgent(ctx, a, spec, task)
				}
			}
		}()
	}

	// Enqueue work in config order so earlier agents retain FIFO backlog priority
	// when the bounded worker pool is saturated.
	for i := range agents {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			for idx, spec := range agents {
				if results[idx].Name == "" {
					results[idx] = canceledAgentResult(spec, ctx.Err())
				}
			}
			return c.buildResult(results), nil
		case jobs <- i:
		}
	}
	close(jobs)

	wg.Wait()
	if err := ctx.Err(); err != nil {
		for idx, spec := range agents {
			if results[idx].Name == "" {
				results[idx] = canceledAgentResult(spec, err)
			}
		}
	}
	return c.buildResult(results), nil
}

// hierarchicalAssignment represents a sub-task assignment from the coordinator agent.
type hierarchicalAssignment struct {
	Agent string `json:"agent"`
	Task  string `json:"task"`
}

type hierarchicalAssignments struct {
	Assignments []hierarchicalAssignment `json:"assignments"`
}

func (c *Coordinator) runHierarchical(ctx context.Context, task string) (*TeamResult, error) {
	// Find coordinator and workers.
	var coordSpec *AgentSpec
	var workers []AgentSpec
	workerMap := make(map[string]AgentSpec)

	for i := range c.config.Agents {
		if c.config.Agents[i].IsCoordinator {
			coordSpec = &c.config.Agents[i]
		} else {
			workers = append(workers, c.config.Agents[i])
			workerMap[c.config.Agents[i].Name] = c.config.Agents[i]
		}
	}

	if coordSpec == nil {
		return nil, fmt.Errorf("no coordinator agent found in hierarchical config")
	}

	// Build coordinator system prompt with worker info.
	var workerDescs []string
	for _, w := range workers {
		workerDescs = append(workerDescs, fmt.Sprintf("- %s (role: %s): %s", w.Name, w.Role, w.SystemPrompt))
	}

	coordSystemPrompt := coordSpec.SystemPrompt + "\n\nAvailable worker agents:\n" + strings.Join(workerDescs, "\n") +
		"\n\nRespond with a JSON object containing task assignments for workers. Format:\n" +
		`{"assignments": [{"agent": "<worker_name>", "task": "<task_description>"}]}`

	// Override coordinator's system prompt.
	modifiedCoordSpec := *coordSpec
	modifiedCoordSpec.SystemPrompt = coordSystemPrompt

	var allResults []AgentResult

	// Spawn and run coordinator.
	coordAgent, err := c.spawnAgent(modifiedCoordSpec)
	if err != nil {
		return nil, fmt.Errorf("failed to spawn coordinator agent: %w", err)
	}

	coordResult := runAgent(ctx, coordAgent, modifiedCoordSpec, task)
	allResults = append(allResults, coordResult)

	if coordResult.Status == "FAILED" {
		return c.buildResult(allResults), nil
	}

	// Parse assignments from coordinator response.
	assignments, err := parseAssignments(coordResult.FinalResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to parse coordinator assignments: %w (raw response: %s)", err, coordResult.FinalResponse)
	}

	// Run assigned workers.
	assignedWorkers := make(map[string]bool, len(workers))
	for _, assignment := range assignments.Assignments {
		workerSpec, ok := workerMap[assignment.Agent]
		if !ok {
			// Skip unknown worker (EC-6).
			continue
		}
		if assignedWorkers[assignment.Agent] {
			// A single round is bounded by the configured worker set.
			continue
		}
		assignedWorkers[assignment.Agent] = true

		workerAgent, err := c.spawnAgent(workerSpec)
		if err != nil {
			allResults = append(allResults, AgentResult{
				Name:   workerSpec.Name,
				Role:   workerSpec.Role,
				Status: "FAILED",
				Error:  err.Error(),
			})
			continue
		}

		ar := runAgent(ctx, workerAgent, workerSpec, assignment.Task)
		allResults = append(allResults, ar)
	}

	return c.buildResult(allResults), nil
}

// parseAssignments extracts assignment JSON from a coordinator response.
// It handles both raw JSON and JSON wrapped in markdown code fences.
func parseAssignments(response string) (*hierarchicalAssignments, error) {
	// Strip markdown code fences if present.
	cleaned := response
	if idx := strings.Index(cleaned, "```json"); idx != -1 {
		cleaned = cleaned[idx+7:]
	} else if idx := strings.Index(cleaned, "```"); idx != -1 {
		cleaned = cleaned[idx+3:]
	}
	if idx := strings.LastIndex(cleaned, "```"); idx != -1 {
		cleaned = cleaned[:idx]
	}
	cleaned = strings.TrimSpace(cleaned)

	var assignments hierarchicalAssignments
	if err := json.Unmarshal([]byte(cleaned), &assignments); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return &assignments, nil
}
