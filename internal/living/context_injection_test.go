package living_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
)

// ── Mock evaluator ──────────────────────────────────────────────────

type mockEvaluator struct {
	result *living.EvalResult
	err    error
}

func (m *mockEvaluator) Evaluate(_ context.Context, _ living.Update, _ string) (*living.EvalResult, error) {
	return m.result, m.err
}

// ── Mock task lookup for context injection ───────────────────────────

type mockTaskLookupForCI struct {
	tasks map[string]*living.TaskState
}

func (m *mockTaskLookupForCI) ActiveTaskByID(taskID string) *living.TaskState {
	return m.tasks[taskID]
}

// ── Mock RhizomeClient for context injection ────────────────────────

type mockRhizomeClientForCI struct {
	releaseTaskCalled  bool
	lastReleasedTaskID string
	sessionEvents      []living.SessionEventInput
}

func (m *mockRhizomeClientForCI) FetchTasks(_ context.Context, _ []string) ([]living.Task, error) {
	return nil, nil
}
func (m *mockRhizomeClientForCI) ClaimTask(_ context.Context, _, _ string) error { return nil }
func (m *mockRhizomeClientForCI) ReleaseTask(_ context.Context, taskID, _, _ string) error {
	m.releaseTaskCalled = true
	m.lastReleasedTaskID = taskID
	return nil
}
func (m *mockRhizomeClientForCI) CompleteTask(_ context.Context, _, _ string) error { return nil }
func (m *mockRhizomeClientForCI) FailTask(_ context.Context, _, _ string) error     { return nil }
func (m *mockRhizomeClientForCI) GetTaskUpdates(_ context.Context, _ string, _ time.Time) ([]living.Update, error) {
	return nil, nil
}
func (m *mockRhizomeClientForCI) SendUpdate(_ context.Context, _, _, _, _, _ string) error {
	return nil
}
func (m *mockRhizomeClientForCI) FetchMessages(_ context.Context, _ string, _ time.Time) ([]living.Message, error) {
	return nil, nil
}
func (m *mockRhizomeClientForCI) SendMessage(_ context.Context, _, _, _, _ string) error { return nil }
func (m *mockRhizomeClientForCI) Heartbeat(_ context.Context, _, _, _ string) error      { return nil }
func (m *mockRhizomeClientForCI) EscalateTask(_ context.Context, _, _ string) error      { return nil }
func (m *mockRhizomeClientForCI) RecordSessionEvent(_ context.Context, input living.SessionEventInput) error {
	m.sessionEvents = append(m.sessionEvents, input)
	return nil
}

// ── Helpers ─────────────────────────────────────────────────────────

func newRunningTaskForCI(taskID string) *living.TaskState {
	ts := living.NewTaskState(taskID, taskID, 2)
	ts.SessionID = "sess-" + taskID
	_ = ts.Start()
	return ts
}

func newTestCIConfig() living.Config {
	return living.Config{ID: "test-agent"}
}

func oneUpdate() []living.Update {
	return []living.Update{
		{UpdateID: "u1", AgentID: "other-agent", UpdateType: "progress", Summary: "did something"},
	}
}

// ── Tests ───────────────────────────────────────────────────────────

func TestContextInjector_AdjustInjectsMessage(t *testing.T) {
	t.Parallel()

	task := newRunningTaskForCI("task-1")
	lookup := &mockTaskLookupForCI{tasks: map[string]*living.TaskState{"task-1": task}}
	eval := &mockEvaluator{result: &living.EvalResult{Action: living.UpdateAdjust, Reason: "new info available"}}
	rhizome := &mockRhizomeClientForCI{}

	ci := living.NewContextInjector(newTestCIConfig(), rhizome, lookup, eval)
	err := ci.HandleTaskUpdates(context.Background(), "task-1", oneUpdate())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(task.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(task.Messages))
	}

	msg := task.Messages[0]
	if msg.Role != llm.RoleUser {
		t.Errorf("expected role %q, got %q", llm.RoleUser, msg.Role)
	}

	text := msg.TextContent()
	expected := "[SYSTEM UPDATE]: new info available"
	if text != expected {
		t.Errorf("expected message text %q, got %q", expected, text)
	}
}

func TestContextInjector_PauseSetsWaiting(t *testing.T) {
	t.Parallel()

	task := newRunningTaskForCI("task-2")
	lookup := &mockTaskLookupForCI{tasks: map[string]*living.TaskState{"task-2": task}}
	eval := &mockEvaluator{result: &living.EvalResult{Action: living.UpdatePause, Reason: "blocked on dependency"}}
	rhizome := &mockRhizomeClientForCI{}

	ci := living.NewContextInjector(newTestCIConfig(), rhizome, lookup, eval)
	err := ci.HandleTaskUpdates(context.Background(), "task-2", oneUpdate())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if task.Status != living.TaskStatusWaiting {
		t.Errorf("expected status %q, got %q", living.TaskStatusWaiting, task.Status)
	}
	if len(rhizome.sessionEvents) != 1 {
		t.Fatalf("expected 1 session event, got %+v", rhizome.sessionEvents)
	}
	if rhizome.sessionEvents[0].EventType != "session.blocked" {
		t.Fatalf("expected blocked session event, got %+v", rhizome.sessionEvents[0])
	}
	if rhizome.sessionEvents[0].KeepSessionActive == nil || *rhizome.sessionEvents[0].KeepSessionActive {
		t.Fatalf("expected keep_session_active=false, got %+v", rhizome.sessionEvents[0])
	}
}

func TestContextInjector_PauseRequiresHumanRecordsDecisionNeeded(t *testing.T) {
	t.Parallel()

	task := newRunningTaskForCI("task-2b")
	lookup := &mockTaskLookupForCI{tasks: map[string]*living.TaskState{"task-2b": task}}
	eval := &mockEvaluator{result: &living.EvalResult{Action: living.UpdatePause, Reason: "awaiting operator decision"}}
	rhizome := &mockRhizomeClientForCI{}

	ci := living.NewContextInjector(newTestCIConfig(), rhizome, lookup, eval)
	err := ci.HandleTaskUpdates(context.Background(), "task-2b", []living.Update{
		{
			UpdateID:      "u2",
			AgentID:       "human",
			UpdateType:    "needs_input",
			Summary:       "operator input required",
			RequiresHuman: true,
			CreatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rhizome.sessionEvents) != 1 {
		t.Fatalf("expected 1 session event, got %+v", rhizome.sessionEvents)
	}
	if rhizome.sessionEvents[0].EventType != "session.decision_needed" {
		t.Fatalf("expected decision-needed session event, got %+v", rhizome.sessionEvents[0])
	}
	if rhizome.sessionEvents[0].DecisionNeededFrom != "human" {
		t.Fatalf("expected decision_needed_from=human, got %+v", rhizome.sessionEvents[0])
	}
}

func TestContextInjector_AbortReleasesTask(t *testing.T) {
	t.Parallel()

	task := newRunningTaskForCI("task-3")
	lookup := &mockTaskLookupForCI{tasks: map[string]*living.TaskState{"task-3": task}}
	eval := &mockEvaluator{result: &living.EvalResult{Action: living.UpdateAbort, Reason: "task cancelled"}}
	rhizome := &mockRhizomeClientForCI{}

	ci := living.NewContextInjector(newTestCIConfig(), rhizome, lookup, eval)
	err := ci.HandleTaskUpdates(context.Background(), "task-3", oneUpdate())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if task.Status != living.TaskStatusAborted {
		t.Errorf("expected status %q, got %q", living.TaskStatusAborted, task.Status)
	}

	if !rhizome.releaseTaskCalled {
		t.Error("expected ReleaseTask to be called")
	}
	if rhizome.lastReleasedTaskID != "task-3" {
		t.Errorf("expected released task ID %q, got %q", "task-3", rhizome.lastReleasedTaskID)
	}
}

func TestContextInjector_ContinueDoesNothing(t *testing.T) {
	t.Parallel()

	task := newRunningTaskForCI("task-4")
	initialMsgCount := len(task.Messages)
	lookup := &mockTaskLookupForCI{tasks: map[string]*living.TaskState{"task-4": task}}
	eval := &mockEvaluator{result: &living.EvalResult{Action: living.UpdateContinue, Reason: ""}}
	rhizome := &mockRhizomeClientForCI{}

	ci := living.NewContextInjector(newTestCIConfig(), rhizome, lookup, eval)
	err := ci.HandleTaskUpdates(context.Background(), "task-4", oneUpdate())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if task.Status != living.TaskStatusRunning {
		t.Errorf("expected status %q, got %q", living.TaskStatusRunning, task.Status)
	}
	if len(task.Messages) != initialMsgCount {
		t.Errorf("expected %d messages, got %d", initialMsgCount, len(task.Messages))
	}
}

func TestContextInjector_EvaluatorError(t *testing.T) {
	t.Parallel()

	task := newRunningTaskForCI("task-5")
	lookup := &mockTaskLookupForCI{tasks: map[string]*living.TaskState{"task-5": task}}
	eval := &mockEvaluator{result: nil, err: errors.New("llm unavailable")}
	rhizome := &mockRhizomeClientForCI{}

	ci := living.NewContextInjector(newTestCIConfig(), rhizome, lookup, eval)
	err := ci.HandleTaskUpdates(context.Background(), "task-5", oneUpdate())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Task should remain running (default to CONTINUE).
	if task.Status != living.TaskStatusRunning {
		t.Errorf("expected status %q, got %q", living.TaskStatusRunning, task.Status)
	}
	if len(task.Messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(task.Messages))
	}
}

func TestContextInjector_TaskNotFound(t *testing.T) {
	t.Parallel()

	lookup := &mockTaskLookupForCI{tasks: map[string]*living.TaskState{}}
	eval := &mockEvaluator{result: &living.EvalResult{Action: living.UpdateAbort, Reason: "should not matter"}}
	rhizome := &mockRhizomeClientForCI{}

	ci := living.NewContextInjector(newTestCIConfig(), rhizome, lookup, eval)
	err := ci.HandleTaskUpdates(context.Background(), "nonexistent", oneUpdate())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No panic, no error, nothing happened.
	if rhizome.releaseTaskCalled {
		t.Error("ReleaseTask should not be called for a missing task")
	}
}
