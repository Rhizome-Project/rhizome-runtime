package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockAgent struct {
	id        string
	runFn     func(ctx context.Context, task string) (*LoopResult, error)
	runCalled bool
	mu        sync.Mutex
}

func (m *mockAgent) ID() string { return m.id }

func (m *mockAgent) Run(ctx context.Context, task string) (*LoopResult, error) {
	m.mu.Lock()
	m.runCalled = true
	m.mu.Unlock()
	if m.runFn != nil {
		return m.runFn(ctx, task)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestRuntimeSupervisor_StartAndStop(t *testing.T) {
	t.Parallel()

	events := make(chan struct {
		status LoopStatus
		err    error
	}, 10)

	sup := NewRuntimeSupervisor(func(agentID string, status LoopStatus, err error) {
		events <- struct {
			status LoopStatus
			err    error
		}{status, err}
	})

	agent := &mockAgent{
		id: "agent-1",
		runFn: func(ctx context.Context, task string) (*LoopResult, error) {
			<-ctx.Done() // wait for cancellation
			return nil, ctx.Err()
		},
	}

	if err := sup.Start(context.Background(), agent, "test-task"); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if h := sup.Health("agent-1"); h != StatusStarting && h != StatusRunning {
		t.Fatalf("expected STARTING or RUNNING gracefully, got %s", h)
	}

	if err := sup.Stop("agent-1"); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// wait for runAsync to complete via contexts stopping
	time.Sleep(100 * time.Millisecond)

	if h := sup.Health("agent-1"); h != StatusStopped {
		t.Fatalf("expected STOPPED, got %s", h)
	}
}

func TestRuntimeSupervisor_LifecycleCrash(t *testing.T) {
	t.Parallel()

	sup := NewRuntimeSupervisor(nil)

	agent := &mockAgent{
		id: "agent-crash",
		runFn: func(ctx context.Context, task string) (*LoopResult, error) {
			return nil, errors.New("simulated fatal failure")
		},
	}

	if err := sup.Start(context.Background(), agent, "test-task"); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if h := sup.Health("agent-crash"); h != StatusCrashed {
		t.Fatalf("expected CRASHED, got %s", h)
	}

	if err := sup.Error("agent-crash"); err == nil || err.Error() != "simulated fatal failure" {
		t.Fatalf("missing or incorrect trace error: %v", err)
	}
}

func TestRuntimeSupervisor_PanicContainment(t *testing.T) {
	t.Parallel()

	sup := NewRuntimeSupervisor(nil)

	agent := &mockAgent{
		id: "agent-panic",
		runFn: func(ctx context.Context, task string) (*LoopResult, error) {
			panic("simulated panic")
		},
	}

	if err := sup.Start(context.Background(), agent, "test-task"); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if h := sup.Health("agent-panic"); h != StatusCrashed {
		t.Fatalf("expected CRASHED from wrapped panic, got %s", h)
	}

	err := sup.Error("agent-panic")
	if err == nil {
		t.Fatal("expected panic error to be captured")
	}
	if err.Error() != "agent panicked: simulated panic" {
		t.Fatalf("expected formatted panic msg, got %v", err.Error())
	}
}
