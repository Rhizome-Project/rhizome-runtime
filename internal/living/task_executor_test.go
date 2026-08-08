package living_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
)

// --- Mock RhizomeClient ---

type mockRhizomeClient struct {
	mu             sync.Mutex
	claimCalls     []string // taskIDs
	completeCalls  []string // taskIDs
	releaseCalls   []string // taskIDs
	failCalls      []string // taskIDs
	sessionEvents  []living.SessionEventInput
	claimErr       error  // if set, ClaimTask returns this error
	claimErrTaskID string // if set, only this taskID returns claimErr
}

func newMockRhizomeClient() *mockRhizomeClient {
	return &mockRhizomeClient{}
}

func (m *mockRhizomeClient) FetchTasks(_ context.Context, _ []string) ([]living.Task, error) {
	return nil, nil
}

func (m *mockRhizomeClient) ClaimTask(_ context.Context, taskID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimErr != nil && (m.claimErrTaskID == "" || m.claimErrTaskID == taskID) {
		return m.claimErr
	}
	m.claimCalls = append(m.claimCalls, taskID)
	return nil
}

func (m *mockRhizomeClient) ReleaseTask(_ context.Context, taskID, _, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseCalls = append(m.releaseCalls, taskID)
	return nil
}

func (m *mockRhizomeClient) CompleteTask(_ context.Context, taskID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completeCalls = append(m.completeCalls, taskID)
	return nil
}

func (m *mockRhizomeClient) FailTask(_ context.Context, taskID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failCalls = append(m.failCalls, taskID)
	return nil
}

func (m *mockRhizomeClient) GetTaskUpdates(_ context.Context, _ string, _ time.Time) ([]living.Update, error) {
	return nil, nil
}

func (m *mockRhizomeClient) SendUpdate(_ context.Context, _, _, _, _, _ string) error {
	return nil
}

func (m *mockRhizomeClient) FetchMessages(_ context.Context, _ string, _ time.Time) ([]living.Message, error) {
	return nil, nil
}

func (m *mockRhizomeClient) SendMessage(_ context.Context, _, _, _, _ string) error {
	return nil
}

func (m *mockRhizomeClient) Heartbeat(_ context.Context, _, _, _ string) error {
	return nil
}

func (m *mockRhizomeClient) EscalateTask(_ context.Context, _, _ string) error {
	return nil
}

func (m *mockRhizomeClient) RecordSessionEvent(_ context.Context, input living.SessionEventInput) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessionEvents = append(m.sessionEvents, input)
	return nil
}

func (m *mockRhizomeClient) getClaimCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.claimCalls))
	copy(result, m.claimCalls)
	return result
}

func (m *mockRhizomeClient) getCompleteCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.completeCalls))
	copy(result, m.completeCalls)
	return result
}

func (m *mockRhizomeClient) getReleaseCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.releaseCalls))
	copy(result, m.releaseCalls)
	return result
}

func (m *mockRhizomeClient) getFailCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.failCalls))
	copy(result, m.failCalls)
	return result
}

func (m *mockRhizomeClient) getSessionEvents() []living.SessionEventInput {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]living.SessionEventInput, len(m.sessionEvents))
	copy(result, m.sessionEvents)
	return result
}

// --- Mock TaskRunner ---

type mockTaskRunner struct {
	mu      sync.Mutex
	result  string
	err     error
	calls   int
	callCh  chan struct{} // signaled on each call
	blockCh chan struct{} // if non-nil, RunTask blocks until closed
	// perTask allows different results per taskID
	perTask map[string]mockTaskResult
}

type mockTaskResult struct {
	result string
	err    error
}

func newMockTaskRunner(result string, err error) *mockTaskRunner {
	return &mockTaskRunner{
		result: result,
		err:    err,
		callCh: make(chan struct{}, 100),
	}
}

func (r *mockTaskRunner) RunTask(_ context.Context, task living.Task) (string, error) {
	if r.blockCh != nil {
		<-r.blockCh
	}

	r.mu.Lock()
	r.calls++
	callCount := r.calls

	var result string
	var err error

	if pt, ok := r.perTask[task.TaskID]; ok {
		result = pt.result
		err = pt.err
	} else {
		result = r.result
		err = r.err
	}
	r.mu.Unlock()

	_ = callCount

	r.callCh <- struct{}{}
	return result, err
}

func (r *mockTaskRunner) getCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// --- Helper ---

func makeConfig(maxConcurrent, maxRetries int) living.Config {
	return living.Config{
		ID:                 "test-agent",
		MaxConcurrentTasks: maxConcurrent,
		MaxRetries:         maxRetries,
		TaskTypes:          []string{"code"},
	}
}

func waitForCh(t *testing.T, ch <-chan struct{}, count int, timeout time.Duration) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-ch:
		case <-time.After(timeout):
			t.Fatalf("timed out waiting for call %d/%d", i+1, count)
		}
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

// T-1: TestTaskExecutor_HandleNewTasks_ClaimsAvailable
func TestTaskExecutor_HandleNewTasks_ClaimsAvailable(t *testing.T) {
	t.Parallel()

	rhizome := newMockRhizomeClient()
	store := living.NewMemoryStateStore()
	runner := newMockTaskRunner("done", nil)
	executor := living.NewTaskExecutor(makeConfig(3, 2), rhizome, store, runner)

	tasks := []living.Task{
		{TaskID: "t1", Description: "task 1"},
		{TaskID: "t2", Description: "task 2"},
	}

	err := executor.HandleNewTasks(context.Background(), tasks)
	if err != nil {
		t.Fatalf("HandleNewTasks error: %v", err)
	}

	// Wait for both goroutines to complete
	waitForCh(t, runner.callCh, 2, 2*time.Second)
	waitForCondition(t, 2*time.Second, func() bool {
		return len(rhizome.getCompleteCalls()) == 2
	}, "both task completions")

	claims := rhizome.getClaimCalls()
	if len(claims) != 2 {
		t.Fatalf("expected 2 claims, got %d", len(claims))
	}

	completes := rhizome.getCompleteCalls()
	if len(completes) != 2 {
		t.Fatalf("expected 2 completes, got %d", len(completes))
	}
}

// T-2: TestTaskExecutor_HandleNewTasks_RespectsMaxConcurrent
func TestTaskExecutor_HandleNewTasks_RespectsMaxConcurrent(t *testing.T) {
	t.Parallel()

	rhizome := newMockRhizomeClient()
	store := living.NewMemoryStateStore()
	runner := newMockTaskRunner("done", nil)
	runner.blockCh = make(chan struct{}) // block all executions
	executor := living.NewTaskExecutor(makeConfig(3, 2), rhizome, store, runner)

	// Fill up 3 slots with blocking tasks
	firstBatch := []living.Task{
		{TaskID: "t1", Description: "task 1"},
		{TaskID: "t2", Description: "task 2"},
		{TaskID: "t3", Description: "task 3"},
	}
	err := executor.HandleNewTasks(context.Background(), firstBatch)
	if err != nil {
		t.Fatalf("HandleNewTasks error: %v", err)
	}

	// Give goroutines time to start
	time.Sleep(50 * time.Millisecond)

	// Try to add more — should be rejected
	moreTasks := []living.Task{
		{TaskID: "t4", Description: "task 4"},
	}
	err = executor.HandleNewTasks(context.Background(), moreTasks)
	if err != nil {
		t.Fatalf("HandleNewTasks error: %v", err)
	}

	claims := rhizome.getClaimCalls()
	if len(claims) != 3 {
		t.Fatalf("expected 3 claims (only first batch), got %d", len(claims))
	}

	// Cleanup: unblock
	close(runner.blockCh)
	waitForCh(t, runner.callCh, 3, 2*time.Second)
}

// T-3: TestTaskExecutor_Execution_Success
func TestTaskExecutor_Execution_Success(t *testing.T) {
	t.Parallel()

	rhizome := newMockRhizomeClient()
	store := living.NewMemoryStateStore()
	runner := newMockTaskRunner("task completed successfully", nil)
	executor := living.NewTaskExecutor(makeConfig(3, 2), rhizome, store, runner)

	tasks := []living.Task{
		{TaskID: "t1", Description: "do something"},
	}

	err := executor.HandleNewTasks(context.Background(), tasks)
	if err != nil {
		t.Fatalf("HandleNewTasks error: %v", err)
	}

	waitForCh(t, runner.callCh, 1, 2*time.Second)
	// Allow completion to propagate
	time.Sleep(50 * time.Millisecond)

	completes := rhizome.getCompleteCalls()
	if len(completes) != 1 {
		t.Fatalf("expected 1 complete call, got %d", len(completes))
	}
	if completes[0] != "t1" {
		t.Fatalf("expected complete for t1, got %s", completes[0])
	}

	// Task should no longer be active
	active := executor.ActiveTasks()
	if len(active) != 0 {
		t.Fatalf("expected 0 active tasks, got %d", len(active))
	}

	// State should be saved as completed
	state, err := store.LoadTaskState(context.Background(), "t1")
	if err != nil {
		t.Fatalf("LoadTaskState error: %v", err)
	}
	if state == nil {
		t.Fatal("expected saved state, got nil")
	}
	if state.Status != living.TaskStatusCompleted {
		t.Fatalf("expected completed status, got %s", state.Status)
	}

	events := rhizome.getSessionEvents()
	if len(events) != 2 {
		t.Fatalf("expected 2 session events, got %+v", events)
	}
	if events[0].EventType != "session.start" || events[1].EventType != "session.end" {
		t.Fatalf("expected start/end session events, got %+v", events)
	}
	if events[0].TaskID != "t1" || events[1].TaskID != "t1" {
		t.Fatalf("expected session events for t1, got %+v", events)
	}
	if events[0].SessionID == "" || events[0].SessionID != events[1].SessionID {
		t.Fatalf("expected matching session IDs, got %+v", events)
	}
}

// T-4: TestTaskExecutor_Execution_FailWithRetry
func TestTaskExecutor_Execution_FailWithRetry(t *testing.T) {
	t.Parallel()

	rhizome := newMockRhizomeClient()
	store := living.NewMemoryStateStore()
	callCount := 0
	var mu sync.Mutex
	runner := &countingRunner{
		callCh: make(chan struct{}, 100),
		fn: func(ctx context.Context, task living.Task) (string, error) {
			mu.Lock()
			callCount++
			c := callCount
			mu.Unlock()
			if c < 3 {
				return "", fmt.Errorf("transient error %d", c)
			}
			return "finally done", nil
		},
	}

	// maxRetries=3 means up to 3 retries (RetryCount < MaxRetries)
	executor := living.NewTaskExecutor(makeConfig(3, 3), rhizome, store, runner)

	tasks := []living.Task{
		{TaskID: "t1", Description: "flaky task"},
	}

	err := executor.HandleNewTasks(context.Background(), tasks)
	if err != nil {
		t.Fatalf("HandleNewTasks error: %v", err)
	}

	// Should be called 3 times: fail, fail, succeed
	waitForCh(t, runner.callCh, 3, 5*time.Second)
	time.Sleep(50 * time.Millisecond)

	completes := rhizome.getCompleteCalls()
	if len(completes) != 1 {
		t.Fatalf("expected 1 complete call after retries, got %d", len(completes))
	}
}

// countingRunner is a TaskRunner backed by a custom function.
type countingRunner struct {
	callCh chan struct{}
	fn     func(ctx context.Context, task living.Task) (string, error)
}

func (r *countingRunner) RunTask(ctx context.Context, task living.Task) (string, error) {
	result, err := r.fn(ctx, task)
	r.callCh <- struct{}{}
	return result, err
}

// T-5: TestTaskExecutor_Execution_FailExhausted
func TestTaskExecutor_Execution_FailExhausted(t *testing.T) {
	t.Parallel()

	rhizome := newMockRhizomeClient()
	store := living.NewMemoryStateStore()
	runner := newMockTaskRunner("", fmt.Errorf("permanent error"))
	// maxRetries=0 means no retries: Fail sets status to failed immediately
	executor := living.NewTaskExecutor(makeConfig(3, 0), rhizome, store, runner)

	tasks := []living.Task{
		{TaskID: "t1", Description: "doomed task"},
	}

	err := executor.HandleNewTasks(context.Background(), tasks)
	if err != nil {
		t.Fatalf("HandleNewTasks error: %v", err)
	}

	waitForCh(t, runner.callCh, 1, 2*time.Second)
	time.Sleep(50 * time.Millisecond)

	// Should have been marked failed, not completed
	completes := rhizome.getCompleteCalls()
	if len(completes) != 0 {
		t.Fatalf("expected 0 complete calls, got %d", len(completes))
	}

	fails := rhizome.getFailCalls()
	if len(fails) != 1 {
		t.Fatalf("expected 1 fail call, got %d", len(fails))
	}

	// Task should no longer be active
	active := executor.ActiveTasks()
	if len(active) != 0 {
		t.Fatalf("expected 0 active tasks, got %d", len(active))
	}
}

// T-6: TestTaskExecutor_ClaimRace
func TestTaskExecutor_ClaimRace(t *testing.T) {
	t.Parallel()

	rhizome := newMockRhizomeClient()
	rhizome.claimErr = fmt.Errorf("already claimed")
	store := living.NewMemoryStateStore()
	runner := newMockTaskRunner("done", nil)
	executor := living.NewTaskExecutor(makeConfig(3, 2), rhizome, store, runner)

	tasks := []living.Task{
		{TaskID: "t1", Description: "contested task"},
	}

	err := executor.HandleNewTasks(context.Background(), tasks)
	if err != nil {
		t.Fatalf("HandleNewTasks error: %v", err)
	}

	// No claims should succeed
	claims := rhizome.getClaimCalls()
	if len(claims) != 0 {
		t.Fatalf("expected 0 successful claims, got %d", len(claims))
	}

	// No active tasks
	active := executor.ActiveTasks()
	if len(active) != 0 {
		t.Fatalf("expected 0 active tasks, got %d", len(active))
	}

	// Runner should not have been called
	if runner.getCalls() != 0 {
		t.Fatalf("expected 0 runner calls, got %d", runner.getCalls())
	}
}

// T-7: TestTaskExecutor_ActiveTasks
func TestTaskExecutor_ActiveTasks(t *testing.T) {
	t.Parallel()

	rhizome := newMockRhizomeClient()
	store := living.NewMemoryStateStore()
	runner := newMockTaskRunner("done", nil)
	runner.blockCh = make(chan struct{}) // block execution
	executor := living.NewTaskExecutor(makeConfig(5, 2), rhizome, store, runner)

	tasks := []living.Task{
		{TaskID: "t1", Description: "task 1"},
		{TaskID: "t2", Description: "task 2"},
	}

	err := executor.HandleNewTasks(context.Background(), tasks)
	if err != nil {
		t.Fatalf("HandleNewTasks error: %v", err)
	}

	// Give goroutines time to start
	time.Sleep(50 * time.Millisecond)

	// Should have 2 active tasks
	active := executor.ActiveTasks()
	if len(active) != 2 {
		t.Fatalf("expected 2 active tasks, got %d", len(active))
	}

	// Should find specific task by ID
	ts := executor.ActiveTaskByID("t1")
	if ts == nil {
		t.Fatal("expected to find task t1")
	}
	if ts.TaskID != "t1" {
		t.Fatalf("expected task ID t1, got %s", ts.TaskID)
	}

	// Non-existent task returns nil
	ts = executor.ActiveTaskByID("nonexistent")
	if ts != nil {
		t.Fatal("expected nil for nonexistent task")
	}

	// Cleanup
	close(runner.blockCh)
	waitForCh(t, runner.callCh, 2, 2*time.Second)
}

// T-8: TestTaskExecutor_RestoreFromStore
func TestTaskExecutor_RestoreFromStore(t *testing.T) {
	t.Parallel()

	rhizome := newMockRhizomeClient()
	store := living.NewMemoryStateStore()
	runner := newMockTaskRunner("restored done", nil)

	ctx := context.Background()

	// Pre-populate store with a pending task and a waiting task
	pendingState := living.NewTaskState("t-pending", "t-pending", 2)
	if err := store.SaveTaskState(ctx, pendingState); err != nil {
		t.Fatalf("save pending state: %v", err)
	}

	waitingState := living.NewTaskState("t-waiting", "t-waiting", 2)
	waitingState.Status = living.TaskStatusWaiting
	waitingState.WaitingForMessage = "msg-123"
	if err := store.SaveTaskState(ctx, waitingState); err != nil {
		t.Fatalf("save waiting state: %v", err)
	}

	executor := living.NewTaskExecutor(makeConfig(5, 2), rhizome, store, runner)

	if err := executor.RestoreFromStore(ctx); err != nil {
		t.Fatalf("RestoreFromStore error: %v", err)
	}

	// The pending task should be executed
	waitForCh(t, runner.callCh, 1, 2*time.Second)
	time.Sleep(50 * time.Millisecond)

	// The waiting task should still be active but not executed
	if runner.getCalls() != 1 {
		t.Fatalf("expected 1 runner call (only pending task), got %d", runner.getCalls())
	}

	// Waiting task should still be in active map
	ts := executor.ActiveTaskByID("t-waiting")
	if ts == nil {
		t.Fatal("expected waiting task to be in active map")
	}
	if ts.Status != living.TaskStatusWaiting {
		t.Fatalf("expected waiting status, got %s", ts.Status)
	}
}
