package living

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living/memory"
)

// WorkerRunner abstracts the actual LLM execution for testability.
type WorkerRunner interface {
	RunWorker(ctx context.Context, systemPrompt string, task string) (*WorkerResult, error)
}

// WorkerTask describes a unit of work to be executed by a spawned worker.
type WorkerTask struct {
	Description string
	TaskID      string
	Priority    string
}

// WorkerResult holds the outcome of a worker execution.
type WorkerResult struct {
	FinalResponse string
	InputTokens   int
	OutputTokens  int
	ToolCalls     int
	Error         error
	Duration      time.Duration
}

// WorkerSpawner creates ephemeral workers that execute tasks using cheap models.
// Before spawning, it searches associative memory for relevant procedures,
// decisions, lessons, incidents, and recent context to pack into the worker's prompt.
type WorkerSpawner struct {
	memoryStore MemoryBackend // may be nil
	runner      WorkerRunner
}

// NewWorkerSpawner creates a WorkerSpawner with the given memory store and runner.
// The memoryStore may be nil, in which case workers are spawned without memory context.
func NewWorkerSpawner(memoryStore MemoryBackend, runner WorkerRunner) *WorkerSpawner {
	return &WorkerSpawner{
		memoryStore: memoryStore,
		runner:      runner,
	}
}

// SpawnWorker creates and runs an ephemeral worker for the given task.
func (ws *WorkerSpawner) SpawnWorker(ctx context.Context, task WorkerTask) (*WorkerResult, error) {
	if task.Description == "" {
		return nil, fmt.Errorf("worker task description is required")
	}

	start := time.Now()

	// Build memory context
	memCtx := ws.buildMemoryContext(ctx, task.Description)

	// Build system prompt
	prompt := buildWorkerPrompt(task, memCtx)

	// Run worker
	result, err := ws.runner.RunWorker(ctx, prompt, task.Description)
	if err != nil {
		duration := time.Since(start)
		if duration <= 0 {
			duration = time.Nanosecond
		}
		return &WorkerResult{Error: err, Duration: duration}, nil
	}

	duration := time.Since(start)
	if duration <= 0 {
		duration = time.Nanosecond
	}
	result.Duration = duration
	return result, nil
}

// buildMemoryContext searches the memory store for relevant procedures,
// decisions, lessons, experiences, incidents, and update digests.
func (ws *WorkerSpawner) buildMemoryContext(ctx context.Context, description string) string {
	if ws.memoryStore == nil {
		return ""
	}

	var sections []string
	keywords := extractKeywords(description)
	for _, section := range []struct {
		Title string
		Limit int
		Types []string
	}{
		{Title: "Relevant Procedures", Limit: 4, Types: memorySearchAliases(memory.TypeProcedure)},
		{Title: "Key Decisions", Limit: 3, Types: memorySearchAliases(memory.TypeDecision)},
		{Title: "Relevant Lessons", Limit: 3, Types: memorySearchAliases(memory.TypeLesson)},
		{Title: "Relevant Experiences", Limit: 2, Types: memorySearchAliases(memory.TypeExperience)},
		{Title: "Known Incidents", Limit: 2, Types: memorySearchAliases(memory.TypeIncident)},
		{Title: "Recent Updates", Limit: 2, Types: memorySearchAliases(memory.TypeUpdateDigest)},
	} {
		entries := ws.searchMemorySection(ctx, keywords, section.Limit, section.Types...)
		if len(entries) == 0 {
			continue
		}
		sections = append(sections, formatMemorySection(section.Title, entries))
	}

	return strings.Join(sections, "\n\n")
}

func (ws *WorkerSpawner) searchMemorySection(ctx context.Context, query string, limit int, typeFilters ...string) []memory.MemoryEntry {
	if strings.TrimSpace(query) == "" || limit <= 0 || len(typeFilters) == 0 {
		return nil
	}
	out := make([]memory.MemoryEntry, 0, limit)
	seen := make(map[string]struct{}, limit)
	for _, typeFilter := range typeFilters {
		entries, err := ws.memoryStore.Search(ctx, query, memory.SearchOpts{TypeFilter: typeFilter, Limit: limit})
		if err != nil {
			continue
		}
		for _, entry := range entries {
			key := fmt.Sprintf("%d|%s|%s|%s", entry.ID, entry.Type, entry.Topic, entry.Content)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, entry)
			if len(out) >= limit {
				return out
			}
		}
	}
	return out
}

func formatMemorySection(title string, entries []memory.MemoryEntry) string {
	lines := make([]string, 0, len(entries))
	for _, entry := range entries {
		lines = append(lines, fmt.Sprintf("- [%s] %s", firstNonEmptyNonBlank(entry.Topic, defaultMemoryTopic(entry.Type, entry.Content)), truncate(entry.Content, 300)))
	}
	return "## " + title + "\n" + strings.Join(lines, "\n")
}

// extractKeywords extracts up to 5 keywords (words longer than 3 characters)
// from the input text for use as FTS5 search queries. Keywords are joined
// with OR so that partial matches are returned.
func extractKeywords(text string) string {
	words := strings.Fields(text)
	var keywords []string
	for _, w := range words {
		w = strings.Trim(w, ".,;:!?\"'()[]{}")
		if len(w) > 3 {
			keywords = append(keywords, w)
		}
		if len(keywords) >= 5 {
			break
		}
	}
	return strings.Join(keywords, " OR ")
}

// truncate shortens a string to maxLen characters, appending "..." if truncated.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

const workerPromptTemplate = `You are a worker agent executing a specific task.
Your job is to complete the assigned task efficiently and return the result.

## Constraints
- Focus only on the assigned task
- Do not plan or delegate — just execute
- Use your available tools to accomplish the task
- Return a clear, concise result when done

## Task
%s`

// buildWorkerPrompt constructs the system prompt for a worker, including
// any relevant memory context.
func buildWorkerPrompt(task WorkerTask, memoryContext string) string {
	prompt := legacyLivingPromptCompilerNotice(
		"legacy_living_worker_prompt.v1",
		"legacy_living_worker_non_converged",
		"This living worker prompt is not the agent daemon active capability snapshot compiler.",
	)
	prompt += "\n\n" + fmt.Sprintf(workerPromptTemplate, demoteLegacyDaemonProjectionMarkers(task.Description))
	if memoryContext != "" {
		prompt += "\n\n# Relevant Context from Memory\n" + demoteLegacyDaemonProjectionMarkers(memoryContext)
	}
	return prompt
}
