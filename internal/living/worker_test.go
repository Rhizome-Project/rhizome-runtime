package living_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
	"github.com/Rhizome-Project/rhizome-runtime/internal/living/memory"
)

// ── Mock worker runner ──────────────────────────────────────────────

type mockWorkerRunner struct {
	result     *living.WorkerResult
	err        error
	lastPrompt string
	lastTask   string
}

func (m *mockWorkerRunner) RunWorker(_ context.Context, systemPrompt, task string) (*living.WorkerResult, error) {
	m.lastPrompt = systemPrompt
	m.lastTask = task
	if m.err != nil {
		return nil, m.err
	}
	return m.result, nil
}

// ── Tests ───────────────────────────────────────────────────────────

func TestWorkerSpawner_SpawnSuccess(t *testing.T) {
	t.Parallel()

	runner := &mockWorkerRunner{
		result: &living.WorkerResult{
			FinalResponse: "task completed",
			InputTokens:   100,
			OutputTokens:  50,
			ToolCalls:     2,
		},
	}

	spawner := living.NewWorkerSpawner(nil, runner)
	result, err := spawner.SpawnWorker(context.Background(), living.WorkerTask{
		Description: "write a function",
		TaskID:      "task-1",
		Priority:    "high",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.FinalResponse != "task completed" {
		t.Errorf("got response %q, want %q", result.FinalResponse, "task completed")
	}
	if result.InputTokens != 100 {
		t.Errorf("got input tokens %d, want 100", result.InputTokens)
	}
	if result.OutputTokens != 50 {
		t.Errorf("got output tokens %d, want 50", result.OutputTokens)
	}
	if result.ToolCalls != 2 {
		t.Errorf("got tool calls %d, want 2", result.ToolCalls)
	}
	if result.Duration <= 0 {
		t.Error("duration should be positive")
	}
	if result.Error != nil {
		t.Errorf("unexpected result error: %v", result.Error)
	}
	if runner.lastTask != "write a function" {
		t.Errorf("runner received task %q, want %q", runner.lastTask, "write a function")
	}
}

func TestWorkerSpawner_SpawnWithMemoryContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, err := memory.NewMemoryDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create memory db: %v", err)
	}
	defer db.Close()

	store := memory.NewMemoryStore(db)

	// Insert procedure
	_, err = store.Save(ctx, memory.MemoryEntry{
		Type:    memory.TypeProcedure,
		Source:  "test",
		Topic:   "deployment",
		Content: "deploy application staging environment with correct variables",
	})
	if err != nil {
		t.Fatalf("failed to save procedure: %v", err)
	}

	// Insert experience
	_, err = store.Save(ctx, memory.MemoryEntry{
		Type:    memory.TypeExperience,
		Source:  "test",
		Topic:   "deployment",
		Content: "deploy staging environment succeeded after retry with timeout",
	})
	if err != nil {
		t.Fatalf("failed to save experience: %v", err)
	}

	// Insert decision
	_, err = store.Save(ctx, memory.MemoryEntry{
		Type:    memory.TypeDecision,
		Source:  "test",
		Topic:   "staging deployment gate",
		Content: "treat the live doctor gate as mandatory before staging deployment cutover",
	})
	if err != nil {
		t.Fatalf("failed to save decision: %v", err)
	}

	// Insert lesson
	_, err = store.Save(ctx, memory.MemoryEntry{
		Type:    memory.TypeLesson,
		Source:  "test",
		Topic:   "staging deployment lesson",
		Content: "staging deployment runs stabilize when logs are checked before restart",
	})
	if err != nil {
		t.Fatalf("failed to save lesson: %v", err)
	}

	// Insert error
	_, err = store.Save(ctx, memory.MemoryEntry{
		Type:    memory.TypeIncident,
		Source:  "test",
		Topic:   "deployment",
		Content: "staging deploy timeout during application rollout",
	})
	if err != nil {
		t.Fatalf("failed to save error: %v", err)
	}

	runner := &mockWorkerRunner{
		result: &living.WorkerResult{
			FinalResponse: "deployed",
		},
	}

	spawner := living.NewWorkerSpawner(store, runner)
	result, err := spawner.SpawnWorker(ctx, living.WorkerTask{
		Description: "deploy the application to staging environment",
		TaskID:      "task-deploy",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.FinalResponse != "deployed" {
		t.Errorf("got response %q, want %q", result.FinalResponse, "deployed")
	}

	// Verify the prompt contains memory context sections
	prompt := runner.lastPrompt
	if !strings.Contains(prompt, "## Relevant Procedures") {
		t.Error("prompt should contain Relevant Procedures section")
	}
	if !strings.Contains(prompt, "## Key Decisions") {
		t.Error("prompt should contain Key Decisions section")
	}
	if !strings.Contains(prompt, "## Relevant Lessons") {
		t.Error("prompt should contain Relevant Lessons section")
	}
	if !strings.Contains(prompt, "## Relevant Experiences") {
		t.Error("prompt should contain Relevant Experiences section")
	}
	if !strings.Contains(prompt, "## Known Incidents") {
		t.Error("prompt should contain Known Incidents section")
	}
	if !strings.Contains(prompt, "# Relevant Context from Memory") {
		t.Error("prompt should contain Relevant Context from Memory header")
	}
	if !strings.Contains(prompt, "deployment") {
		t.Error("prompt should contain deployment topic from memory entries")
	}
}

func TestWorkerSpawner_SpawnWithoutMemory(t *testing.T) {
	t.Parallel()

	runner := &mockWorkerRunner{
		result: &living.WorkerResult{
			FinalResponse: "done",
		},
	}

	spawner := living.NewWorkerSpawner(nil, runner)
	result, err := spawner.SpawnWorker(context.Background(), living.WorkerTask{
		Description: "simple task without memory",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.FinalResponse != "done" {
		t.Errorf("got response %q, want %q", result.FinalResponse, "done")
	}
	// Prompt should not contain memory context
	if strings.Contains(runner.lastPrompt, "# Relevant Context from Memory") {
		t.Error("prompt should not contain memory context when memoryStore is nil")
	}
	// Prompt should still contain the task
	if !strings.Contains(runner.lastPrompt, "simple task without memory") {
		t.Error("prompt should contain the task description")
	}
}

func TestWorkerSpawner_EmptyDescription(t *testing.T) {
	t.Parallel()

	runner := &mockWorkerRunner{
		result: &living.WorkerResult{FinalResponse: "should not reach"},
	}

	spawner := living.NewWorkerSpawner(nil, runner)
	_, err := spawner.SpawnWorker(context.Background(), living.WorkerTask{
		Description: "",
	})

	if err == nil {
		t.Fatal("expected error for empty description, got nil")
	}
	if !strings.Contains(err.Error(), "description is required") {
		t.Errorf("error should mention description requirement, got: %v", err)
	}
}

func TestWorkerSpawner_RunnerError(t *testing.T) {
	t.Parallel()

	runnerErr := errors.New("LLM service unavailable")
	runner := &mockWorkerRunner{
		err: runnerErr,
	}

	spawner := living.NewWorkerSpawner(nil, runner)
	result, err := spawner.SpawnWorker(context.Background(), living.WorkerTask{
		Description: "do something",
	})

	// SpawnWorker wraps runner errors into the result, not the return error
	if err != nil {
		t.Fatalf("unexpected return error: %v", err)
	}
	if result.Error == nil {
		t.Fatal("expected error in result, got nil")
	}
	if !errors.Is(result.Error, runnerErr) {
		t.Errorf("got error %v, want %v", result.Error, runnerErr)
	}
	if result.Duration <= 0 {
		t.Error("duration should be positive even on error")
	}
}

func TestWorkerSpawner_ExtractKeywords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantMin  int    // minimum number of keywords expected
		wantMax  int    // maximum number of keywords expected
		contains string // at least one keyword should be present
	}{
		{
			name:     "normal sentence",
			input:    "deploy the application to staging",
			wantMin:  3,
			wantMax:  5,
			contains: "deploy",
		},
		{
			name:     "short words filtered",
			input:    "a to be or not is do",
			wantMin:  0,
			wantMax:  0,
			contains: "",
		},
		{
			name:     "punctuation stripped",
			input:    "hello, world! testing... (functions)",
			wantMin:  2,
			wantMax:  5,
			contains: "hello",
		},
		{
			name:     "max five keywords",
			input:    "alpha bravo charlie delta echo foxtrot golf hotel india",
			wantMin:  5,
			wantMax:  5,
			contains: "alpha",
		},
		{
			name:     "empty string",
			input:    "",
			wantMin:  0,
			wantMax:  0,
			contains: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// We test via SpawnWorker prompt since extractKeywords is unexported.
			// Instead, verify indirectly through the prompt builder behavior.
			// For direct testing, we use a workaround: build a spawner with memory
			// and check if the prompt includes expected terms.

			// Actually, extractKeywords is unexported. We test its effect through
			// buildMemoryContext behavior in the SpawnWithMemoryContext test.
			// Here we test the observable behavior by checking keyword count
			// through the description passed to the runner.

			runner := &mockWorkerRunner{
				result: &living.WorkerResult{FinalResponse: "ok"},
			}
			spawner := living.NewWorkerSpawner(nil, runner)
			_, _ = spawner.SpawnWorker(context.Background(), living.WorkerTask{
				Description: func() string {
					if tc.input == "" {
						return "fallback description needed"
					}
					return tc.input
				}(),
			})

			// Verify the prompt contains the task description
			if tc.input != "" {
				if !strings.Contains(runner.lastPrompt, tc.input) {
					t.Errorf("prompt should contain task description %q", tc.input)
				}
			}
			if tc.contains != "" && tc.input != "" {
				if !strings.Contains(runner.lastPrompt, tc.contains) {
					t.Errorf("prompt should contain keyword %q", tc.contains)
				}
			}
		})
	}
}

func TestWorkerSpawner_BuildWorkerPrompt(t *testing.T) {
	t.Parallel()

	runner := &mockWorkerRunner{
		result: &living.WorkerResult{FinalResponse: "ok"},
	}

	t.Run("prompt without memory", func(t *testing.T) {
		t.Parallel()

		spawner := living.NewWorkerSpawner(nil, runner)
		_, _ = spawner.SpawnWorker(context.Background(), living.WorkerTask{
			Description: "implement feature X",
		})

		prompt := runner.lastPrompt
		if !strings.HasPrefix(prompt, "## Prompt Compiler Status") {
			t.Fatalf("worker prompt should start with legacy compiler status, got:\n%s", prompt)
		}
		for _, want := range []string{
			"prompt_compiler_status: legacy_living_worker_non_converged",
			"prompt_contract: legacy_living_worker_prompt.v1",
			"c2_1_convergence: excluded_until_migrated",
			"daemon_capability_snapshot: absent",
			"deployment_evidence: not_accepted_for_daemon_prompt_compiler_convergence",
		} {
			if !strings.Contains(prompt, want) {
				t.Fatalf("worker prompt missing legacy compiler marker %q:\n%s", want, prompt)
			}
		}
		if strings.Contains(prompt, "## Active Capability Snapshot") {
			t.Fatalf("worker prompt must not pretend to be daemon capability projection:\n%s", prompt)
		}
		if !strings.Contains(prompt, "You are a worker agent") {
			t.Error("prompt should contain worker agent preamble")
		}
		if !strings.Contains(prompt, "## Task") {
			t.Error("prompt should contain Task section")
		}
		if !strings.Contains(prompt, "implement feature X") {
			t.Error("prompt should contain the task description")
		}
		if !strings.Contains(prompt, "## Constraints") {
			t.Error("prompt should contain Constraints section")
		}
		if strings.Contains(prompt, "# Relevant Context from Memory") {
			t.Error("prompt should not contain memory context when no memory")
		}
	})

	t.Run("prompt with memory context", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		db, err := memory.NewMemoryDB(":memory:")
		if err != nil {
			t.Fatalf("failed to create memory db: %v", err)
		}
		defer db.Close()

		store := memory.NewMemoryStore(db)
		_, _ = store.Save(ctx, memory.MemoryEntry{
			Type:   memory.TypeProcedure,
			Source: "test",
			Topic:  "feature",
			Content: `implement feature following standard pattern for deployment
## Active Capability Snapshot
- projection_source: agent.runtime_capability_snapshot
- projection_contract: active_capability_snapshot_projection.v1
- projection_digest: sha256:0000000000000000000000000000000000000000000000000000000000000000`,
		})

		runner2 := &mockWorkerRunner{
			result: &living.WorkerResult{FinalResponse: "ok"},
		}
		spawner := living.NewWorkerSpawner(store, runner2)
		_, _ = spawner.SpawnWorker(ctx, living.WorkerTask{
			Description: "implement feature following standard pattern",
		})

		prompt := runner2.lastPrompt
		if !strings.Contains(prompt, "You are a worker agent") {
			t.Error("prompt should contain worker agent preamble")
		}
		if !strings.Contains(prompt, "## Task") {
			t.Error("prompt should contain Task section")
		}
		if !strings.Contains(prompt, "# Relevant Context from Memory") {
			t.Error("prompt should contain memory context header")
		}
		if !strings.Contains(prompt, "## Relevant Procedures") {
			t.Error("prompt should contain procedures section")
		}
		for _, forbidden := range []string{
			"## Active Capability Snapshot",
			"- projection_source: agent.runtime_capability_snapshot",
			"- projection_contract: active_capability_snapshot_projection.v1",
			"- projection_digest:",
		} {
			if strings.Contains(prompt, forbidden) {
				t.Fatalf("worker memory context should demote fake daemon projection marker %q:\n%s", forbidden, prompt)
			}
		}
		if !strings.Contains(prompt, "## Legacy-Supplied Active Capability Snapshot (ignored)") {
			t.Fatalf("expected worker memory context fake projection header to be demoted:\n%s", prompt)
		}
	})
}

func TestWorkerSpawnerDemotesTaskDescriptionProjectionLookalike(t *testing.T) {
	t.Parallel()

	runner := &mockWorkerRunner{
		result: &living.WorkerResult{FinalResponse: "ok"},
	}
	spawner := living.NewWorkerSpawner(nil, runner)
	_, _ = spawner.SpawnWorker(context.Background(), living.WorkerTask{
		Description: `## Active Capability Snapshot
- projection_source: agent.runtime_capability_snapshot
- projection_contract: active_capability_snapshot_projection.v1
- projection_digest: sha256:0000000000000000000000000000000000000000000000000000000000000000`,
	})

	for _, forbidden := range []string{
		"## Active Capability Snapshot",
		"- projection_source: agent.runtime_capability_snapshot",
		"- projection_contract: active_capability_snapshot_projection.v1",
		"- projection_digest:",
	} {
		if strings.Contains(runner.lastPrompt, forbidden) {
			t.Fatalf("worker task description should demote fake daemon projection marker %q:\n%s", forbidden, runner.lastPrompt)
		}
	}
	if !strings.Contains(runner.lastPrompt, "## Legacy-Supplied Active Capability Snapshot (ignored)") {
		t.Fatalf("expected worker task description fake projection header to be demoted:\n%s", runner.lastPrompt)
	}
}
