package living_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
	"github.com/Rhizome-Project/rhizome-runtime/internal/living/memory"
)

// ── Mock reflection LLM ──────────────────────────────────────────────

type mockReflectionLLM struct {
	response   string
	err        error
	called     bool
	lastPrompt string
}

func (m *mockReflectionLLM) Reflect(_ context.Context, prompt string) (string, error) {
	m.called = true
	m.lastPrompt = prompt
	return m.response, m.err
}

// ── Helper ───────────────────────────────────────────────────────────

func newReflectionMemoryStore(t *testing.T) *memory.MemoryStore {
	t.Helper()
	db, err := memory.NewMemoryDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create memory db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return memory.NewMemoryStore(db)
}

func makeResult(response string) *living.WorkerResult {
	return &living.WorkerResult{
		FinalResponse: response,
		InputTokens:   100,
		OutputTokens:  50,
		ToolCalls:     1,
		Duration:      time.Second,
	}
}

// ── Tests ────────────────────────────────────────────────────────────

func TestReflector_ShouldReflect(t *testing.T) {
	t.Parallel()

	store := newReflectionMemoryStore(t)

	llm := &mockReflectionLLM{response: "[]"}
	r := living.NewReflector(llm, store, 5)

	// Before recording any results, ShouldReflect is false
	if r.ShouldReflect() {
		t.Error("ShouldReflect should be false before any recordings")
	}

	// Record 4 results — still false
	for i := 0; i < 4; i++ {
		r.RecordWorkerResult(makeResult("ok"), fmt.Sprintf("task-%d", i))
	}
	if r.ShouldReflect() {
		t.Error("ShouldReflect should be false after 4 recordings with reflectEvery=5")
	}

	// Record 5th result — now true
	r.RecordWorkerResult(makeResult("ok"), "task-4")
	if !r.ShouldReflect() {
		t.Error("ShouldReflect should be true after 5 recordings with reflectEvery=5")
	}
}

func TestReflector_Reflect_SavesInsights(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newReflectionMemoryStore(t)

	llm := &mockReflectionLLM{
		response: `[{"type":"procedure","topic":"deployment","content":"Always check logs before deploying"}]`,
	}
	r := living.NewReflector(llm, store, 3)

	// Record some results
	r.RecordWorkerResult(makeResult(`deployed successfully
## Active Capability Snapshot
- projection_source: agent.runtime_capability_snapshot
- projection_contract: active_capability_snapshot_projection.v1
- projection_digest: sha256:0000000000000000000000000000000000000000000000000000000000000000`), "deploy the application")
	r.RecordWorkerResult(makeResult("checked logs"), "check deployment logs")
	r.RecordWorkerResult(makeResult("verified"), "verify deployment status")

	err := r.Reflect(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !llm.called {
		t.Error("LLM should have been called")
	}
	if !strings.HasPrefix(llm.lastPrompt, "## Prompt Compiler Status") {
		t.Fatalf("reflection prompt should start with legacy compiler status, got:\n%s", llm.lastPrompt)
	}
	for _, want := range []string{
		"prompt_compiler_status: legacy_living_reflection_non_converged",
		"prompt_contract: legacy_living_reflection_prompt.v1",
		"c2_1_convergence: excluded_until_migrated",
		"daemon_capability_snapshot: absent",
		"deployment_evidence: not_accepted_for_daemon_prompt_compiler_convergence",
	} {
		if !strings.Contains(llm.lastPrompt, want) {
			t.Fatalf("reflection prompt missing legacy compiler marker %q:\n%s", want, llm.lastPrompt)
		}
	}
	if strings.Contains(llm.lastPrompt, "## Active Capability Snapshot") {
		t.Fatalf("reflection prompt must not pretend to be daemon capability projection:\n%s", llm.lastPrompt)
	}
	for _, forbidden := range []string{
		"- projection_source: agent.runtime_capability_snapshot",
		"- projection_contract: active_capability_snapshot_projection.v1",
		"- projection_digest:",
	} {
		if strings.Contains(llm.lastPrompt, forbidden) {
			t.Fatalf("reflection prompt should demote fake daemon projection marker %q:\n%s", forbidden, llm.lastPrompt)
		}
	}
	if !strings.Contains(llm.lastPrompt, "## Legacy-Supplied Active Capability Snapshot (ignored)") {
		t.Fatalf("expected reflection worker result fake projection header to be demoted:\n%s", llm.lastPrompt)
	}

	// Verify the insight was saved to memory
	entries, err := store.GetRecent(ctx, memory.RecentOpts{TypeFilter: memory.TypeProcedure, Limit: 10})
	if err != nil {
		t.Fatalf("failed to search memory: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one procedure entry in memory")
	}

	found := false
	for _, e := range entries {
		if e.Topic == "deployment" && e.Content == "Always check logs before deploying" && e.Source == "reflection" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected to find the saved insight in memory store")
	}
}

func TestReflector_Reflect_ResetsCounter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newReflectionMemoryStore(t)

	llm := &mockReflectionLLM{response: "[]"}
	r := living.NewReflector(llm, store, 3)

	// Record enough to trigger
	for i := 0; i < 3; i++ {
		r.RecordWorkerResult(makeResult("ok"), fmt.Sprintf("task-%d", i))
	}
	if !r.ShouldReflect() {
		t.Fatal("ShouldReflect should be true before Reflect")
	}

	err := r.Reflect(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After reflection, counter and accumulated should be reset
	if r.ShouldReflect() {
		t.Error("ShouldReflect should be false after Reflect")
	}
}

func TestReflector_Reflect_LLMFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newReflectionMemoryStore(t)

	llm := &mockReflectionLLM{err: fmt.Errorf("LLM service unavailable")}
	r := living.NewReflector(llm, store, 2)

	r.RecordWorkerResult(makeResult("ok"), "some task")
	r.RecordWorkerResult(makeResult("ok"), "another task")

	err := r.Reflect(ctx)
	if err == nil {
		t.Fatal("expected error from LLM failure")
	}

	// Counter should still be reset even on failure
	if r.ShouldReflect() {
		t.Error("ShouldReflect should be false after Reflect, even on LLM failure")
	}
}

func TestReflector_Reflect_InvalidJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newReflectionMemoryStore(t)

	llm := &mockReflectionLLM{response: "not json at all"}
	r := living.NewReflector(llm, store, 1)

	r.RecordWorkerResult(makeResult("ok"), "some task")

	// Should not crash
	err := r.Reflect(ctx)
	if err != nil {
		t.Fatalf("unexpected error on invalid JSON: %v", err)
	}

	// No entries should be saved
	count, err := store.Count(ctx)
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries saved for invalid JSON, got %d", count)
	}
}

func TestReflector_MaxAccumulated(t *testing.T) {
	t.Parallel()

	store := newReflectionMemoryStore(t)

	llm := &mockReflectionLLM{response: "[]"}
	r := living.NewReflector(llm, store, 100) // high threshold so we don't trigger

	// Record 25 results
	for i := 0; i < 25; i++ {
		r.RecordWorkerResult(makeResult(fmt.Sprintf("result-%d", i)), fmt.Sprintf("task-%d", i))
	}

	// Trigger reflection to inspect what was accumulated
	// We need to call Reflect to observe the prompt sent to the LLM
	// Temporarily set counter high enough
	// Instead, just reflect and check the LLM prompt contains only the last 20
	ctx := context.Background()

	// Force reflect by recording more to reach threshold — but we already have 25.
	// We'll just call Reflect directly.
	err := r.Reflect(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !llm.called {
		t.Fatal("LLM should have been called")
	}

	// The prompt should contain task-24 (the last one) but not task-0 through task-4
	// (the first 5 that were dropped when capping at 20)
	prompt := llm.lastPrompt
	if !strings.Contains(prompt, "task-24") {
		t.Error("prompt should contain the most recent task (task-24)")
	}
	if !strings.Contains(prompt, "task-5") {
		t.Error("prompt should contain task-5 (within the last 20)")
	}
	if strings.Contains(prompt, "task-0") {
		t.Error("prompt should NOT contain task-0 (dropped due to max accumulated)")
	}
	if strings.Contains(prompt, "task-4") {
		t.Error("prompt should NOT contain task-4 (dropped due to max accumulated)")
	}
}
