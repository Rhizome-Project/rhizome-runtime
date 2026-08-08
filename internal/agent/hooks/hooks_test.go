package hooks

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// testHook is a configurable mock hook for testing.
type testHook struct {
	name   string
	points []Point
	runFn  func(ctx context.Context, hctx Context) (Result, error)
}

func (h *testHook) Name() string    { return h.name }
func (h *testHook) Points() []Point { return h.points }
func (h *testHook) Run(ctx context.Context, hctx Context) (Result, error) {
	return h.runFn(ctx, hctx)
}

// T-1: Verifies R-6 — register hook for BeforeTool, fire it
func TestRunnerRegisterAndRun(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	called := false
	hook := &testHook{
		name:   "test-hook",
		points: []Point{BeforeTool},
		runFn: func(_ context.Context, hctx Context) (Result, error) {
			called = true
			if hctx.Point != BeforeTool {
				t.Errorf("expected point %q, got %q", BeforeTool, hctx.Point)
			}
			return Result{}, nil
		},
	}
	runner.Register(hook)

	_, err := runner.Run(context.Background(), BeforeTool, Context{ToolName: "bash"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !called {
		t.Fatal("hook was not called")
	}
}

// T-2: Verifies R-7 — PreventStop is true if any hook sets it
func TestRunnerMultipleHooks_MergeResult(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	runner.Register(&testHook{
		name:   "no-prevent",
		points: []Point{OnStop},
		runFn:  func(_ context.Context, _ Context) (Result, error) { return Result{PreventStop: false}, nil },
	})
	runner.Register(&testHook{
		name:   "prevent",
		points: []Point{OnStop},
		runFn:  func(_ context.Context, _ Context) (Result, error) { return Result{PreventStop: true}, nil },
	})

	result, err := runner.Run(context.Background(), OnStop, Context{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !result.PreventStop {
		t.Fatal("expected PreventStop to be true")
	}
}

// T-3: Verifies R-7 — InjectSystemMessage values concatenated with newline
func TestRunnerMultipleHooks_InjectMerge(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	runner.Register(&testHook{
		name:   "inject-1",
		points: []Point{AfterTool},
		runFn:  func(_ context.Context, _ Context) (Result, error) { return Result{InjectSystemMessage: "msg1"}, nil },
	})
	runner.Register(&testHook{
		name:   "inject-2",
		points: []Point{AfterTool},
		runFn:  func(_ context.Context, _ Context) (Result, error) { return Result{InjectSystemMessage: "msg2"}, nil },
	})

	result, err := runner.Run(context.Background(), AfterTool, Context{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.InjectSystemMessage != "msg1\nmsg2" {
		t.Fatalf("expected %q, got %q", "msg1\nmsg2", result.InjectSystemMessage)
	}
}

// T-4: Verifies R-7 — ModifiedInput from last hook wins
func TestRunnerModifiedInput_LastWins(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	input1 := json.RawMessage(`{"v":1}`)
	input2 := json.RawMessage(`{"v":2}`)

	runner.Register(&testHook{
		name:   "mod-1",
		points: []Point{BeforeTool},
		runFn:  func(_ context.Context, _ Context) (Result, error) { return Result{ModifiedInput: input1}, nil },
	})
	runner.Register(&testHook{
		name:   "mod-2",
		points: []Point{BeforeTool},
		runFn:  func(_ context.Context, _ Context) (Result, error) { return Result{ModifiedInput: input2}, nil },
	})

	result, err := runner.Run(context.Background(), BeforeTool, Context{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if string(result.ModifiedInput) != `{"v":2}` {
		t.Fatalf("expected last hook input, got %s", result.ModifiedInput)
	}
}

// T-5: Verifies R-7 — hook error is non-fatal, next hook still runs
func TestRunnerHookError_Continues(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	secondCalled := false

	runner.Register(&testHook{
		name:   "error-hook",
		points: []Point{BeforeTool},
		runFn: func(_ context.Context, _ Context) (Result, error) {
			return Result{}, errors.New("hook failed")
		},
	})
	runner.Register(&testHook{
		name:   "ok-hook",
		points: []Point{BeforeTool},
		runFn: func(_ context.Context, _ Context) (Result, error) {
			secondCalled = true
			return Result{InjectSystemMessage: "injected"}, nil
		},
	})

	result, err := runner.Run(context.Background(), BeforeTool, Context{})
	if err != nil {
		t.Fatalf("run should not return error: %v", err)
	}
	if !secondCalled {
		t.Fatal("second hook was not called after first errored")
	}
	if result.InjectSystemMessage != "injected" {
		t.Fatalf("expected injected message from second hook, got %q", result.InjectSystemMessage)
	}
}

// T-6: Verifies EC-3 — no hooks for point returns zero Result
func TestRunnerNoHooksForPoint(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	// Register for a different point
	runner.Register(&testHook{
		name:   "other",
		points: []Point{BeforeTool},
		runFn:  func(_ context.Context, _ Context) (Result, error) { return Result{PreventStop: true}, nil },
	})

	result, err := runner.Run(context.Background(), OnStop, Context{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.PreventStop {
		t.Fatal("expected zero result")
	}
	if result.ModifiedInput != nil {
		t.Fatal("expected nil ModifiedInput")
	}
	if result.InjectSystemMessage != "" {
		t.Fatal("expected empty InjectSystemMessage")
	}
}

// T-7: Verifies EC-2 — hook panic is recovered and next hook runs
func TestRunnerHookPanic_Recovers(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	secondCalled := false

	runner.Register(&testHook{
		name:   "panic-hook",
		points: []Point{AfterTool},
		runFn: func(_ context.Context, _ Context) (Result, error) {
			panic("something went wrong")
		},
	})
	runner.Register(&testHook{
		name:   "ok-hook",
		points: []Point{AfterTool},
		runFn: func(_ context.Context, _ Context) (Result, error) {
			secondCalled = true
			return Result{InjectSystemMessage: "survived"}, nil
		},
	})

	result, err := runner.Run(context.Background(), AfterTool, Context{})
	if err != nil {
		t.Fatalf("run should not return error: %v", err)
	}
	if !secondCalled {
		t.Fatal("second hook was not called after first panicked")
	}
	if result.InjectSystemMessage != "survived" {
		t.Fatalf("expected 'survived', got %q", result.InjectSystemMessage)
	}
}

// T-8: Verifies EC-1 — hook registered for multiple points fires on each
func TestRunnerMultiPoint(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	callCount := 0

	runner.Register(&testHook{
		name:   "multi-point",
		points: []Point{BeforeTool, AfterTool},
		runFn: func(_ context.Context, _ Context) (Result, error) {
			callCount++
			return Result{}, nil
		},
	})

	if _, err := runner.Run(context.Background(), BeforeTool, Context{}); err != nil {
		t.Fatalf("run BeforeTool: %v", err)
	}
	if _, err := runner.Run(context.Background(), AfterTool, Context{}); err != nil {
		t.Fatalf("run AfterTool: %v", err)
	}
	if callCount != 2 {
		t.Fatalf("expected hook called 2 times, got %d", callCount)
	}
}

// NT-1: Negative test — empty runner, no hooks at all
func TestRunnerEmpty(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	result, err := runner.Run(context.Background(), SessionEnd, Context{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.PreventStop || result.ModifiedInput != nil || result.InjectSystemMessage != "" {
		t.Fatal("expected zero Result from empty runner")
	}
}

// NT-2: Negative test — hook that returns error result with ModifiedInput is skipped
func TestRunnerHookError_ResultIgnored(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	runner.Register(&testHook{
		name:   "error-with-result",
		points: []Point{BeforeTool},
		runFn: func(_ context.Context, _ Context) (Result, error) {
			return Result{ModifiedInput: json.RawMessage(`{"bad":true}`)}, errors.New("fail")
		},
	})

	result, err := runner.Run(context.Background(), BeforeTool, Context{})
	if err != nil {
		t.Fatalf("run should not return error: %v", err)
	}
	// Result from errored hook should be ignored (continue skips merge)
	if result.ModifiedInput != nil {
		t.Fatalf("expected nil ModifiedInput from errored hook, got %s", result.ModifiedInput)
	}
}

// NT-3: Verifies Point constants have correct string values
func TestPointConstants(t *testing.T) {
	t.Parallel()
	if BeforeTool != "before_tool" {
		t.Fatalf("BeforeTool = %q", BeforeTool)
	}
	if AfterTool != "after_tool" {
		t.Fatalf("AfterTool = %q", AfterTool)
	}
	if OnStop != "on_stop" {
		t.Fatalf("OnStop = %q", OnStop)
	}
	if BeforeCompact != "before_compact" {
		t.Fatalf("BeforeCompact = %q", BeforeCompact)
	}
	if SessionEnd != "session_end" {
		t.Fatalf("SessionEnd = %q", SessionEnd)
	}
}
