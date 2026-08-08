package living

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living/memory"
)

// ReflectionLLM abstracts the LLM call for reflection.
type ReflectionLLM interface {
	Reflect(ctx context.Context, prompt string) (string, error)
}

const reflectionPrompt = `## Prompt Compiler Status
- prompt_compiler_status: legacy_living_reflection_non_converged
- prompt_contract: legacy_living_reflection_prompt.v1
- c2_1_convergence: excluded_until_migrated
- daemon_capability_snapshot: absent
- deployment_evidence: not_accepted_for_daemon_prompt_compiler_convergence
- note: This living reflection prompt is not the agent daemon active capability snapshot compiler.

Analyze these worker execution results and extract valuable insights.

## Existing Knowledge
%s

## Worker Results
%s

Output a JSON array of insights. Each entry must have:
- "type": one of "incident", "procedure", "lesson", "decision", "update_digest"
- "topic": short descriptive label
- "content": detailed insight

Focus on:
- Recurring failure patterns and operator-relevant breakages (type: "incident")
- Successful approaches that should be documented (type: "procedure")
- Durable observations and learnings (type: "lesson")
- Explicit choices, policy changes, or defaults that future agents must preserve (type: "decision")
- Short state summaries worth preserving across future sessions (type: "update_digest")
- Avoid duplicating existing knowledge listed above

Output ONLY the JSON array, no other text.`

type workerLog struct {
	TaskDescription string
	Result          *WorkerResult
}

// Reflector periodically reflects on accumulated worker results using an LLM,
// extracting insights and saving them to associative memory.
type Reflector struct {
	llm            ReflectionLLM
	memoryStore    MemoryBackend
	reflectEvery   int
	counter        int
	accumulated    []workerLog
	mu             sync.Mutex
	maxAccumulated int
}

// NewReflector creates a Reflector that triggers reflection every reflectEvery
// worker executions.
func NewReflector(llm ReflectionLLM, memoryStore MemoryBackend, reflectEvery int) *Reflector {
	return &Reflector{
		llm:            llm,
		memoryStore:    memoryStore,
		reflectEvery:   reflectEvery,
		maxAccumulated: 20,
	}
}

// RecordWorkerResult records a worker execution outcome for future reflection.
func (r *Reflector) RecordWorkerResult(result *WorkerResult, taskDescription string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.counter++
	r.accumulated = append(r.accumulated, workerLog{
		TaskDescription: taskDescription,
		Result:          result,
	})
	if len(r.accumulated) > r.maxAccumulated {
		r.accumulated = r.accumulated[len(r.accumulated)-r.maxAccumulated:]
	}
}

// ShouldReflect returns true when enough worker executions have accumulated
// to warrant a reflection cycle.
func (r *Reflector) ShouldReflect() bool {
	return r.counter >= r.reflectEvery && r.reflectEvery > 0
}

// Reflect analyzes accumulated worker logs using the LLM and saves extracted
// insights to the memory store.
func (r *Reflector) Reflect(ctx context.Context) error {
	r.mu.Lock()
	logs := make([]workerLog, len(r.accumulated))
	copy(logs, r.accumulated)
	r.counter = 0
	r.accumulated = nil
	r.mu.Unlock()

	if len(logs) == 0 {
		return nil
	}

	// Format worker logs
	var logLines []string
	for _, wl := range logs {
		entry := fmt.Sprintf("Task: %s\nResult: %s\nTokens: %d/%d",
			wl.TaskDescription,
			truncateReflection(wl.Result.FinalResponse, 1500),
			wl.Result.InputTokens,
			wl.Result.OutputTokens,
		)
		if wl.Result.Error != nil {
			entry += fmt.Sprintf("\nError: %v", wl.Result.Error)
		}
		if len(entry) > 2000 {
			entry = entry[:2000]
		}
		logLines = append(logLines, entry)
	}

	// Search memory for existing relevant context
	existingKnowledge := ""
	keywords := extractKeywords(logs[0].TaskDescription)
	if keywords != "" && r.memoryStore != nil {
		entries, err := r.memoryStore.Search(ctx, keywords, memory.SearchOpts{Limit: 5})
		if err == nil && len(entries) > 0 {
			var lines []string
			for _, e := range entries {
				lines = append(lines, fmt.Sprintf("- [%s/%s] %s", e.Type, e.Topic, truncateReflection(e.Content, 300)))
			}
			existingKnowledge = strings.Join(lines, "\n")
		}
	}
	if existingKnowledge == "" {
		existingKnowledge = "(none)"
	}

	prompt := fmt.Sprintf(
		reflectionPrompt,
		demoteLegacyDaemonProjectionMarkers(existingKnowledge),
		demoteLegacyDaemonProjectionMarkers(strings.Join(logLines, "\n---\n")),
	)

	response, err := r.llm.Reflect(ctx, prompt)
	if err != nil {
		return fmt.Errorf("reflection: llm call failed: %w", err)
	}

	// Parse response as JSON array
	type insightEntry struct {
		Type    string `json:"type"`
		Topic   string `json:"topic"`
		Content string `json:"content"`
	}

	var insights []insightEntry
	if err := json.Unmarshal([]byte(response), &insights); err != nil {
		// Fallback: try each line as individual JSON
		for _, line := range strings.Split(response, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var single insightEntry
			if jsonErr := json.Unmarshal([]byte(line), &single); jsonErr == nil && single.Type != "" {
				insights = append(insights, single)
			}
		}
	}

	// Save valid entries to memory store
	for _, insight := range insights {
		entry, ok := promoteMemoryEntry(memory.MemoryEntry{
			Type:    insight.Type,
			Source:  "reflection",
			Topic:   insight.Topic,
			Content: insight.Content,
		}, "reflection")
		if !ok {
			continue
		}
		_, saveErr := r.memoryStore.Save(ctx, entry)
		if saveErr != nil {
			log.Printf("reflection: failed to save insight: %v", saveErr)
		}
	}

	return nil
}

// mapInsightType maps an insight type string to a memory entry type constant.
func mapInsightType(t string) string {
	return normalizeLivingMemoryType(t)
}

// truncateReflection shortens a string to maxLen characters.
func truncateReflection(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
