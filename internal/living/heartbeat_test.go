package living

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// ── Mock RhizomeClient ──────────────────────────────────────────────

type mockRhizomeClient struct {
	mu                   sync.Mutex
	heartbeatCount       int
	tasks                []Task
	updates              map[string][]Update
	messages             []Message
	compactionCandidates []SessionCompactionCandidate

	heartbeatErr  error
	fetchTaskErr  error
	compactionErr error
}

func newMockRhizomeClient() *mockRhizomeClient {
	return &mockRhizomeClient{
		updates: make(map[string][]Update),
	}
}

func (m *mockRhizomeClient) Heartbeat(_ context.Context, _, _, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatCount++
	return m.heartbeatErr
}

func (m *mockRhizomeClient) FetchTasks(_ context.Context, _ []string) ([]Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fetchTaskErr != nil {
		return nil, m.fetchTaskErr
	}
	return m.tasks, nil
}

func (m *mockRhizomeClient) GetTaskUpdates(_ context.Context, taskID string, _ time.Time) ([]Update, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.updates[taskID], nil
}

func (m *mockRhizomeClient) FetchMessages(_ context.Context, _ string, _ time.Time) ([]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.messages, nil
}

func (m *mockRhizomeClient) ListSessionCompactionCandidates(_ context.Context, _ string, _, _ int) ([]SessionCompactionCandidate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.compactionErr != nil {
		return nil, m.compactionErr
	}
	out := make([]SessionCompactionCandidate, len(m.compactionCandidates))
	copy(out, m.compactionCandidates)
	return out, nil
}

func (m *mockRhizomeClient) ClaimTask(context.Context, string, string) error           { return nil }
func (m *mockRhizomeClient) ReleaseTask(context.Context, string, string, string) error { return nil }
func (m *mockRhizomeClient) CompleteTask(context.Context, string, string) error        { return nil }
func (m *mockRhizomeClient) FailTask(context.Context, string, string) error            { return nil }
func (m *mockRhizomeClient) SendUpdate(context.Context, string, string, string, string, string) error {
	return nil
}
func (m *mockRhizomeClient) SendMessage(context.Context, string, string, string, string) error {
	return nil
}
func (m *mockRhizomeClient) EscalateTask(context.Context, string, string) error { return nil }

func (m *mockRhizomeClient) getHeartbeatCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heartbeatCount
}

// ── Helpers ─────────────────────────────────────────────────────────

func testStateStore() StateStore {
	return NewMemoryStateStore()
}

func testConfig() Config {
	return Config{
		ID:                  "test-agent",
		HeartbeatInterval:   50 * time.Millisecond,
		SituationCheckEvery: 3,
		MaxRetries:          2,
		TaskTypes:           []string{"code"},
	}
}

// ── Tests ───────────────────────────────────────────────────────────

func TestHeartbeatLoop_TickCallsAllChecks(t *testing.T) {
	client := newMockRhizomeClient()
	client.tasks = []Task{{TaskID: "t1", TaskKind: "code"}}
	client.updates = map[string][]Update{
		"t1": {{UpdateID: "u1"}},
	}
	client.messages = []Message{{MessageID: "m1", CreatedAt: time.Now().Format(time.RFC3339Nano)}}

	var order []string
	var mu sync.Mutex
	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}

	handlers := HeartbeatHandlers{
		OnNewTasks: func(_ context.Context, tasks []Task) error {
			record("new-tasks")
			return nil
		},
		OnTaskUpdates: func(_ context.Context, _ string, _ []Update) error {
			record("task-updates")
			return nil
		},
		OnMessages: func(_ context.Context, _ []Message) error {
			record("messages")
			return nil
		},
		OnCompactionNeeded: func(_ context.Context) error {
			record("compaction")
			return nil
		},
		OnSituationAssessment: func(_ context.Context) error {
			record("situation")
			return nil
		},
	}

	loop := NewHeartbeatLoop(testConfig(), client, testStateStore(), handlers)
	ctx := context.Background()

	// Run 3 ticks so situation assessment fires on tick 3.
	for i := 0; i < 3; i++ {
		_ = loop.Tick(ctx)
	}

	mu.Lock()
	defer mu.Unlock()

	// Ticks 1 and 2: 4 checks each (new-tasks, task-updates, messages, compaction skipped since <100 msgs).
	// Tick 3 adds situation. Let's just verify the key checks were called.
	found := map[string]bool{}
	for _, name := range order {
		found[name] = true
	}

	for _, check := range []string{"new-tasks", "messages", "situation"} {
		if !found[check] {
			t.Errorf("expected check %q to be called, but it was not. order=%v", check, order)
		}
	}

	// task-updates should fire on tick 2+ (after new tasks detected on tick 1).
	if !found["task-updates"] {
		t.Errorf("expected task-updates to be called after new tasks detected")
	}
}

func TestHeartbeatLoop_SituationAssessmentEveryN(t *testing.T) {
	cfg := testConfig()
	cfg.SituationCheckEvery = 3

	client := newMockRhizomeClient()
	var situationCount int

	handlers := HeartbeatHandlers{
		OnSituationAssessment: func(_ context.Context) error {
			situationCount++
			return nil
		},
	}

	loop := NewHeartbeatLoop(cfg, client, testStateStore(), handlers)
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		_ = loop.Tick(ctx)
	}

	if situationCount != 2 {
		t.Errorf("expected situation assessment to fire 2 times (ticks 3 and 6), got %d", situationCount)
	}
}

func TestHeartbeatLoop_RunStopsOnContextCancel(t *testing.T) {
	cfg := testConfig()
	cfg.HeartbeatInterval = 10 * time.Millisecond

	client := newMockRhizomeClient()
	loop := NewHeartbeatLoop(cfg, client, testStateStore(), HeartbeatHandlers{})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- loop.Run(ctx)
	}()

	// Let a few ticks run.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestHeartbeatLoop_NewTasksDetectedAndHandlersCalled(t *testing.T) {
	client := newMockRhizomeClient()
	client.tasks = []Task{
		{TaskID: "t1", TaskKind: "code"},
		{TaskID: "t2", TaskKind: "code"},
	}

	var received []Task
	handlers := HeartbeatHandlers{
		OnNewTasks: func(_ context.Context, tasks []Task) error {
			received = append(received, tasks...)
			return nil
		},
	}

	loop := NewHeartbeatLoop(testConfig(), client, testStateStore(), handlers)
	ctx := context.Background()

	// First tick: both tasks are new.
	_ = loop.Tick(ctx)
	if len(received) != 2 {
		t.Fatalf("expected 2 new tasks on first tick, got %d", len(received))
	}

	// Second tick: same tasks should not trigger handler again.
	received = nil
	_ = loop.Tick(ctx)
	if len(received) != 0 {
		t.Errorf("expected 0 new tasks on second tick (already tracked), got %d", len(received))
	}

	// Verify active tasks map.
	active := loop.ActiveTasks()
	if len(active) != 2 {
		t.Errorf("expected 2 active tasks, got %d", len(active))
	}
}

func TestHeartbeatLoop_HeartbeatSentEachTick(t *testing.T) {
	cfg := testConfig()
	cfg.HeartbeatInterval = 10 * time.Millisecond

	client := newMockRhizomeClient()
	loop := NewHeartbeatLoop(cfg, client, testStateStore(), HeartbeatHandlers{})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- loop.Run(ctx)
	}()

	// Let a few ticks run.
	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done

	count := client.getHeartbeatCount()
	if count < 2 {
		t.Errorf("expected at least 2 heartbeats, got %d", count)
	}
}

func TestHeartbeatLoop_NilHandlers(t *testing.T) {
	client := newMockRhizomeClient()
	client.tasks = []Task{{TaskID: "t1"}}
	client.messages = []Message{{MessageID: "m1", CreatedAt: time.Now().Format(time.RFC3339Nano)}}

	// All handlers nil -- should not panic.
	loop := NewHeartbeatLoop(testConfig(), client, testStateStore(), HeartbeatHandlers{})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_ = loop.Tick(ctx)
	}

	// If we got here without panic, the test passes.
}

func TestHeartbeatLoop_HandlerErrorDoesNotStop(t *testing.T) {
	client := newMockRhizomeClient()
	client.tasks = []Task{{TaskID: "t1"}}
	client.messages = []Message{{MessageID: "m1", CreatedAt: time.Now().Format(time.RFC3339Nano)}}

	callCount := 0
	handlers := HeartbeatHandlers{
		OnNewTasks: func(_ context.Context, _ []Task) error {
			return errors.New("handler error")
		},
		OnMessages: func(_ context.Context, _ []Message) error {
			callCount++
			return nil
		},
	}

	loop := NewHeartbeatLoop(testConfig(), client, testStateStore(), handlers)
	ctx := context.Background()

	// Despite OnNewTasks returning error, OnMessages should still be called.
	_ = loop.Tick(ctx)

	if callCount != 1 {
		t.Errorf("expected OnMessages to be called despite OnNewTasks error, callCount=%d", callCount)
	}
}

func TestHeartbeatLoop_HandlerPanicRecovery(t *testing.T) {
	client := newMockRhizomeClient()
	client.tasks = []Task{{TaskID: "t1"}}
	client.messages = []Message{{MessageID: "m1", CreatedAt: time.Now().Format(time.RFC3339Nano)}}

	messagesCallCount := 0
	handlers := HeartbeatHandlers{
		OnNewTasks: func(_ context.Context, _ []Task) error {
			panic("test panic")
		},
		OnMessages: func(_ context.Context, _ []Message) error {
			messagesCallCount++
			return nil
		},
	}

	loop := NewHeartbeatLoop(testConfig(), client, testStateStore(), handlers)
	ctx := context.Background()

	// Should not crash despite panic in OnNewTasks.
	_ = loop.Tick(ctx)

	if messagesCallCount != 1 {
		t.Errorf("expected OnMessages to be called despite panic in OnNewTasks, count=%d", messagesCallCount)
	}
}

func TestHeartbeatLoop_TickCountIncrementsMonotonically(t *testing.T) {
	client := newMockRhizomeClient()
	loop := NewHeartbeatLoop(testConfig(), client, testStateStore(), HeartbeatHandlers{})
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		_ = loop.Tick(ctx)
		expected := int64(i + 1)
		if loop.TickCount() != expected {
			t.Errorf("tick %d: expected tickCount=%d, got %d", i, expected, loop.TickCount())
		}
	}
}

func TestHeartbeatLoop_CompactionUsesCanonicalSessionLedger(t *testing.T) {
	client := newMockRhizomeClient()
	client.compactionCandidates = []SessionCompactionCandidate{
		{
			SessionID:     "sess-1",
			WorkspaceID:   "ws-test",
			AgentID:       "test-agent",
			MessageCount:  18,
			TotalTokens:   16000,
			LastMessageAt: time.Now().Format(time.RFC3339Nano),
		},
	}

	compactionCount := 0
	loop := NewHeartbeatLoop(testConfig(), client, testStateStore(), HeartbeatHandlers{
		OnCompactionNeeded: func(_ context.Context) error {
			compactionCount++
			return nil
		},
	})

	if err := loop.Tick(context.Background()); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	if compactionCount != 1 {
		t.Fatalf("expected canonical compaction candidate to trigger handler once, got %d", compactionCount)
	}
}
