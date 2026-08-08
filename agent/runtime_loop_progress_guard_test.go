package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// progressGuardFixtureLLM is an inline fake ChatLLM that issues one tool call
// per iteration up to a cap, then returns a terminal text answer. Each call
// names the same tool with the same arguments so the executor controls whether
// outputs are novel or identical.
type progressGuardFixtureLLM struct {
	toolName  string
	arguments string
	maxCalls  int
	calls     int
}

func (l *progressGuardFixtureLLM) Chat(_ context.Context, _ []Message, _ []ToolDef) (*LLMResponse, error) {
	l.calls++
	if l.calls > l.maxCalls {
		return &LLMResponse{Content: "natural final answer"}, nil
	}
	args := l.arguments
	if args == "" {
		args = "{}"
	}
	return &LLMResponse{
		Content: fmt.Sprintf("iteration %d", l.calls),
		ToolCalls: []ToolCall{{
			ID:       fmt.Sprintf("call-%d", l.calls),
			Type:     "function",
			Function: FunctionCall{Name: l.toolName, Arguments: args},
		}},
	}, nil
}

func enableProgressGuardForTest(t *testing.T, window int) {
	t.Helper()
	original := CurrentToolLoopProgressGuard()
	t.Cleanup(func() { SetDefaultToolLoopProgressGuard(original) })
	SetDefaultToolLoopProgressGuard(ToolLoopProgressGuardConfig{Enabled: true, Window: window})
}

// TestProgressGuardStopsZeroDiffReadOnlyLoopEarly is the TE-40 acceptance
// fixture: a 25-iteration loop limit that produces identical read-only outcomes
// must stop early (well before the limit), emit the guard marker as a blocked
// result, and preserve the ToolResults trace.
func TestProgressGuardStopsZeroDiffReadOnlyLoopEarly(t *testing.T) {
	enableProgressGuardForTest(t, defaultProgressGuardWindow)

	const iterationLimit = 25
	llm := &progressGuardFixtureLLM{toolName: "read_file", arguments: `{"path":"a.go"}`, maxCalls: iterationLimit}
	// Identical output every call -> no novel outcome after the first.
	executor := func(_ context.Context, _ *ToolRegistry, _ ToolCall) ToolResult {
		return ToolResult{Output: "same read-only output"}
	}
	seed := []Message{
		{Role: "system", Content: "system prompt"},
		{Role: "user", Content: "task packet"},
	}

	run, err := RunToolLoopDetailedWithLimit(context.Background(), llm, NewToolRegistry(), seed, executor, nil, iterationLimit)
	if err != nil {
		t.Fatalf("guard-stopped loop must not error: %v", err)
	}
	if run == nil {
		t.Fatal("expected a ToolLoopRun, got nil")
	}
	// The guard should trigger at exactly window iterations (first is novel,
	// then window-1 more... here window=3 -> first novel at iter 1, no-progress
	// streak builds on iters 2,3,4 -> stop at iter 4). Must be well below cap.
	if llm.calls >= iterationLimit {
		t.Fatalf("guard did not stop early; ran %d/%d iterations", llm.calls, iterationLimit)
	}
	if llm.calls > defaultProgressGuardWindow+2 {
		t.Fatalf("guard stopped later than expected: %d iterations", llm.calls)
	}

	var decoded struct {
		Outcome   string `json:"outcome"`
		Summary   string `json:"summary"`
		BlockedOn []struct {
			Kind   string `json:"kind"`
			Detail string `json:"detail"`
		} `json:"blocked_on"`
	}
	if err := json.Unmarshal([]byte(run.Content), &decoded); err != nil {
		t.Fatalf("guard stop content must be parseable blocked JSON: %v\ncontent=%s", err, run.Content)
	}
	if decoded.Outcome != "blocked" {
		t.Fatalf("expected blocked outcome, got %q", decoded.Outcome)
	}
	if !strings.Contains(run.Content, "tool-loop-progress-guard:") {
		t.Fatalf("guard marker missing from content: %s", run.Content)
	}
	if len(decoded.BlockedOn) == 0 || decoded.BlockedOn[0].Kind != "no_loop_progress" {
		t.Fatalf("expected no_loop_progress blocker, got %+v", decoded.BlockedOn)
	}
	// ToolResults trace preserved (one per executed iteration).
	if len(run.ToolResults) != llm.calls {
		t.Fatalf("expected %d tool results preserved, got %d", llm.calls, len(run.ToolResults))
	}
	if run.ToolResults[0].ToolName != "read_file" {
		t.Fatalf("expected preserved read_file trace, got %q", run.ToolResults[0].ToolName)
	}
}

// TestProgressGuardLetsNovelLoopFinish: a loop whose every iteration produces a
// novel tool output must run to natural completion (no early guard stop).
func TestProgressGuardLetsNovelLoopFinish(t *testing.T) {
	enableProgressGuardForTest(t, defaultProgressGuardWindow)

	const maxCalls = 8
	llm := &progressGuardFixtureLLM{toolName: "read_file", arguments: `{"path":"a.go"}`, maxCalls: maxCalls}
	step := 0
	executor := func(_ context.Context, _ *ToolRegistry, _ ToolCall) ToolResult {
		step++
		return ToolResult{Output: fmt.Sprintf("novel output %d", step)}
	}
	seed := []Message{
		{Role: "system", Content: "system prompt"},
		{Role: "user", Content: "task packet"},
	}

	run, err := RunToolLoopDetailedWithLimit(context.Background(), llm, NewToolRegistry(), seed, executor, nil, 50)
	if err != nil {
		t.Fatalf("novel loop must run to completion: %v", err)
	}
	if run.Content != "natural final answer" {
		t.Fatalf("expected natural completion, got %q", run.Content)
	}
	if llm.calls != maxCalls+1 {
		t.Fatalf("expected %d iterations (incl. terminal), got %d", maxCalls+1, llm.calls)
	}
}

// TestProgressGuardNeverTriggersOnMutatingLoop: even with identical outputs, a
// loop whose calls are in a mutating class (write_file to a durable path) must
// never trigger the guard.
func TestProgressGuardNeverTriggersOnMutatingLoop(t *testing.T) {
	enableProgressGuardForTest(t, defaultProgressGuardWindow)

	const maxCalls = 10
	llm := &progressGuardFixtureLLM{toolName: "write_file", arguments: `{"path":"src/main.go","content":"x"}`, maxCalls: maxCalls}
	executor := func(_ context.Context, _ *ToolRegistry, _ ToolCall) ToolResult {
		return ToolResult{Output: "wrote file ok"} // identical output
	}
	seed := []Message{
		{Role: "system", Content: "system prompt"},
		{Role: "user", Content: "task packet"},
	}

	run, err := RunToolLoopDetailedWithLimit(context.Background(), llm, NewToolRegistry(), seed, executor, nil, 50)
	if err != nil {
		t.Fatalf("mutating loop must run to completion: %v", err)
	}
	if run.Content != "natural final answer" {
		t.Fatalf("mutating loop must not be guard-stopped, got content=%q", run.Content)
	}
	if llm.calls != maxCalls+1 {
		t.Fatalf("expected %d iterations, got %d", maxCalls+1, llm.calls)
	}
}

// TestProgressGuardDisabledByDefault: with the default (off) config, an
// identical read-only loop runs to the iteration cap (error), confirming the
// guard is inert unless explicitly enabled.
func TestProgressGuardDisabledByDefault(t *testing.T) {
	if CurrentToolLoopProgressGuard().Enabled {
		t.Fatal("progress guard must default to disabled")
	}
	llm := &progressGuardFixtureLLM{toolName: "read_file", arguments: `{"path":"a.go"}`, maxCalls: 100}
	executor := func(_ context.Context, _ *ToolRegistry, _ ToolCall) ToolResult {
		return ToolResult{Output: "same output"}
	}
	seed := []Message{
		{Role: "system", Content: "system prompt"},
		{Role: "user", Content: "task packet"},
	}
	_, err := RunToolLoopDetailedWithLimit(context.Background(), llm, NewToolRegistry(), seed, executor, nil, 5)
	if err == nil {
		t.Fatal("with guard disabled, identical loop must hit the iteration cap error")
	}
	if !strings.Contains(err.Error(), "exceeded 5 iterations") {
		t.Fatalf("expected iteration-cap error, got %v", err)
	}
}

// TestProgressGuardArgumentNormalization: reordered-but-equivalent argument
// objects must collapse to one fingerprint so they count as repeated outcomes.
func TestProgressGuardArgumentNormalization(t *testing.T) {
	a := toolCallOutcomeFingerprint(
		ToolCall{Function: FunctionCall{Name: "read_file", Arguments: `{"path":"a.go","limit":10}`}},
		ToolResult{Output: "out"},
	)
	b := toolCallOutcomeFingerprint(
		ToolCall{Function: FunctionCall{Name: "read_file", Arguments: `{"limit":10,"path":"a.go"}`}},
		ToolResult{Output: "out"},
	)
	if a != b {
		t.Fatalf("reordered args must produce identical fingerprint: %s != %s", a, b)
	}
	c := toolCallOutcomeFingerprint(
		ToolCall{Function: FunctionCall{Name: "read_file", Arguments: `{"path":"b.go"}`}},
		ToolResult{Output: "out"},
	)
	if a == c {
		t.Fatal("different args must produce different fingerprints")
	}
}
