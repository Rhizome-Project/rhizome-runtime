package living_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
)

// ── Mocks ────────────────────────────────────────────────────────────

// mockTriager returns a pre-configured TriageResult or error.
type mockTriager struct {
	result *living.TriageResult
	err    error
}

func (m *mockTriager) Triage(_ context.Context, _ living.Message, _ string) (*living.TriageResult, error) {
	return m.result, m.err
}

// mockTaskLookup returns a TaskState by ID from an in-memory map.
type mockTaskLookup struct {
	tasks map[string]*living.TaskState
}

func (m *mockTaskLookup) ActiveTaskByID(taskID string) *living.TaskState {
	return m.tasks[taskID]
}

// sentMessage records a message sent via the mock rhizome client.
type sentMessage struct {
	From    string
	To      string
	Content string
	TaskID  string
}

// mockRhizomeForMessages implements living.RhizomeClient with no-ops for
// everything except SendMessage, which records calls.
type mockRhizomeForMessages struct {
	sent          []sentMessage
	sessionEvents []living.SessionEventInput
}

func (m *mockRhizomeForMessages) FetchTasks(_ context.Context, _ []string) ([]living.Task, error) {
	return nil, nil
}
func (m *mockRhizomeForMessages) ClaimTask(_ context.Context, _, _ string) error { return nil }
func (m *mockRhizomeForMessages) ReleaseTask(_ context.Context, _, _, _ string) error {
	return nil
}
func (m *mockRhizomeForMessages) CompleteTask(_ context.Context, _ string, _ string) error {
	return nil
}
func (m *mockRhizomeForMessages) FailTask(_ context.Context, _ string, _ string) error { return nil }
func (m *mockRhizomeForMessages) GetTaskUpdates(_ context.Context, _ string, _ time.Time) ([]living.Update, error) {
	return nil, nil
}
func (m *mockRhizomeForMessages) SendUpdate(_ context.Context, _, _, _, _, _ string) error {
	return nil
}
func (m *mockRhizomeForMessages) FetchMessages(_ context.Context, _ string, _ time.Time) ([]living.Message, error) {
	return nil, nil
}
func (m *mockRhizomeForMessages) SendMessage(_ context.Context, from, to, content string, taskID string) error {
	m.sent = append(m.sent, sentMessage{From: from, To: to, Content: content, TaskID: taskID})
	return nil
}
func (m *mockRhizomeForMessages) Heartbeat(_ context.Context, _, _, _ string) error { return nil }
func (m *mockRhizomeForMessages) EscalateTask(_ context.Context, _, _ string) error { return nil }
func (m *mockRhizomeForMessages) RecordSessionEvent(_ context.Context, input living.SessionEventInput) error {
	m.sessionEvents = append(m.sessionEvents, input)
	return nil
}

// ── Helpers ──────────────────────────────────────────────────────────

func testConfig() living.Config {
	return living.Config{ID: "test-agent"}
}

func newRunningTask(taskID string) *living.TaskState {
	ts := living.NewTaskState(taskID, taskID, 2)
	ts.SessionID = "sess-" + taskID
	_ = ts.Start()
	return ts
}

func newWaitingTask(taskID, waitingFor string) *living.TaskState {
	ts := newRunningTask(taskID)
	_ = ts.Wait(waitingFor)
	return ts
}

// ── Tests ────────────────────────────────────────────────────────────

func TestMessageHandler_InjectsTaskBoundMessage(t *testing.T) {
	t.Parallel()

	task := newRunningTask("task-1")
	lookup := &mockTaskLookup{tasks: map[string]*living.TaskState{"task-1": task}}
	rhizome := &mockRhizomeForMessages{}
	triager := &mockTriager{result: &living.TriageResult{Action: living.TriageIgnore}}

	handler := living.NewMessageHandler(testConfig(), rhizome, lookup, triager)

	msgs := []living.Message{
		{
			MessageID:   "msg-1",
			FromAgentID: "agent-2",
			ToAgentID:   "test-agent",
			Content:     "[task:task-1] hello",
			CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	if err := handler.HandleMessages(context.Background(), msgs); err != nil {
		t.Fatalf("HandleMessages: %v", err)
	}

	if len(task.Messages) != 1 {
		t.Fatalf("expected 1 injected message, got %d", len(task.Messages))
	}

	text := task.Messages[0].TextContent()
	if text == "" {
		t.Fatal("injected message has no text content")
	}
	if !contains(text, "agent-2") || !contains(text, "[task:task-1] hello") {
		t.Fatalf("unexpected injected text: %s", text)
	}
}

func TestMessageHandler_TriagesGeneralMessage(t *testing.T) {
	t.Parallel()

	lookup := &mockTaskLookup{tasks: map[string]*living.TaskState{}}
	rhizome := &mockRhizomeForMessages{}
	triager := &mockTriager{result: &living.TriageResult{
		Action: living.TriageRespond,
		Reply:  "got it",
	}}

	handler := living.NewMessageHandler(testConfig(), rhizome, lookup, triager)

	msgs := []living.Message{
		{
			MessageID:   "msg-2",
			FromAgentID: "agent-3",
			ToAgentID:   "test-agent",
			Content:     "how are you?",
			CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	if err := handler.HandleMessages(context.Background(), msgs); err != nil {
		t.Fatalf("HandleMessages: %v", err)
	}

	if len(rhizome.sent) != 1 {
		t.Fatalf("expected 1 sent message, got %d", len(rhizome.sent))
	}
	if rhizome.sent[0].To != "agent-3" {
		t.Fatalf("expected reply to agent-3, got %s", rhizome.sent[0].To)
	}
	if rhizome.sent[0].Content != "got it" {
		t.Fatalf("expected reply 'got it', got %s", rhizome.sent[0].Content)
	}
}

func TestMessageHandler_TriageForward(t *testing.T) {
	t.Parallel()

	lookup := &mockTaskLookup{tasks: map[string]*living.TaskState{}}
	rhizome := &mockRhizomeForMessages{}
	triager := &mockTriager{result: &living.TriageResult{
		Action: living.TriageForward,
		Target: "agent-ops",
	}}

	handler := living.NewMessageHandler(testConfig(), rhizome, lookup, triager)

	msgs := []living.Message{
		{
			MessageID:   "msg-3",
			FromAgentID: "agent-4",
			ToAgentID:   "test-agent",
			Content:     "please deploy",
			CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	if err := handler.HandleMessages(context.Background(), msgs); err != nil {
		t.Fatalf("HandleMessages: %v", err)
	}

	if len(rhizome.sent) != 1 {
		t.Fatalf("expected 1 forwarded message, got %d", len(rhizome.sent))
	}
	if rhizome.sent[0].To != "agent-ops" {
		t.Fatalf("expected forward to agent-ops, got %s", rhizome.sent[0].To)
	}
	if rhizome.sent[0].Content != "please deploy" {
		t.Fatalf("expected original content forwarded, got %s", rhizome.sent[0].Content)
	}
}

func TestMessageHandler_ResumesWaitingTask(t *testing.T) {
	t.Parallel()

	task := newWaitingTask("task-2", "agent-5")
	lookup := &mockTaskLookup{tasks: map[string]*living.TaskState{"task-2": task}}
	rhizome := &mockRhizomeForMessages{}
	triager := &mockTriager{result: &living.TriageResult{Action: living.TriageIgnore}}

	handler := living.NewMessageHandler(testConfig(), rhizome, lookup, triager)

	msgs := []living.Message{
		{
			MessageID:   "msg-4",
			FromAgentID: "agent-5",
			ToAgentID:   "test-agent",
			Content:     "[task:task-2] here is the data",
			CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	if err := handler.HandleMessages(context.Background(), msgs); err != nil {
		t.Fatalf("HandleMessages: %v", err)
	}

	if task.Status != living.TaskStatusRunning {
		t.Fatalf("expected task status running after resume, got %s", task.Status)
	}
	if len(rhizome.sessionEvents) != 1 {
		t.Fatalf("expected 1 session keepalive event, got %+v", rhizome.sessionEvents)
	}
	if rhizome.sessionEvents[0].EventType != "session.keepalive" {
		t.Fatalf("expected keepalive event, got %+v", rhizome.sessionEvents[0])
	}
	if rhizome.sessionEvents[0].KeepSessionActive == nil || !*rhizome.sessionEvents[0].KeepSessionActive {
		t.Fatalf("expected keep_session_active=true, got %+v", rhizome.sessionEvents[0])
	}
}

func TestMessageHandler_ResumesWaitingTaskOnAnySenderWhenWaitingForBlank(t *testing.T) {
	t.Parallel()

	task := newWaitingTask("task-blank", "")
	lookup := &mockTaskLookup{tasks: map[string]*living.TaskState{"task-blank": task}}
	rhizome := &mockRhizomeForMessages{}
	triager := &mockTriager{result: &living.TriageResult{Action: living.TriageIgnore}}

	handler := living.NewMessageHandler(testConfig(), rhizome, lookup, triager)
	msgs := []living.Message{
		{
			MessageID:   "msg-blank",
			FromAgentID: "agent-any",
			ToAgentID:   "test-agent",
			Content:     "[task:task-blank] wake up",
			CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		},
	}
	if err := handler.HandleMessages(context.Background(), msgs); err != nil {
		t.Fatalf("HandleMessages: %v", err)
	}
	if task.Status != living.TaskStatusRunning {
		t.Fatalf("expected task status running after blank-wait resume, got %s", task.Status)
	}
}

func TestMessageHandler_TriageError(t *testing.T) {
	t.Parallel()

	lookup := &mockTaskLookup{tasks: map[string]*living.TaskState{}}
	rhizome := &mockRhizomeForMessages{}
	triager := &mockTriager{err: errors.New("llm unavailable")}

	handler := living.NewMessageHandler(testConfig(), rhizome, lookup, triager)

	msgs := []living.Message{
		{
			MessageID:   "msg-5",
			FromAgentID: "agent-6",
			ToAgentID:   "test-agent",
			Content:     "hello?",
			CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	// Should not error out — the handler logs and continues.
	if err := handler.HandleMessages(context.Background(), msgs); err != nil {
		t.Fatalf("HandleMessages should not fail on triage error: %v", err)
	}

	if len(rhizome.sent) != 0 {
		t.Fatalf("expected no sent messages on triage error, got %d", len(rhizome.sent))
	}
}

func TestMessageHandler_NoMessages(t *testing.T) {
	t.Parallel()

	lookup := &mockTaskLookup{tasks: map[string]*living.TaskState{}}
	rhizome := &mockRhizomeForMessages{}
	triager := &mockTriager{result: &living.TriageResult{Action: living.TriageIgnore}}

	handler := living.NewMessageHandler(testConfig(), rhizome, lookup, triager)

	if err := handler.HandleMessages(context.Background(), nil); err != nil {
		t.Fatalf("HandleMessages with nil: %v", err)
	}
	if err := handler.HandleMessages(context.Background(), []living.Message{}); err != nil {
		t.Fatalf("HandleMessages with empty slice: %v", err)
	}
}

func TestMessageHandler_UnknownTaskID(t *testing.T) {
	t.Parallel()

	lookup := &mockTaskLookup{tasks: map[string]*living.TaskState{}}
	rhizome := &mockRhizomeForMessages{}
	triager := &mockTriager{result: &living.TriageResult{Action: living.TriageIgnore}}

	handler := living.NewMessageHandler(testConfig(), rhizome, lookup, triager)

	msgs := []living.Message{
		{
			MessageID:   "msg-6",
			FromAgentID: "agent-7",
			ToAgentID:   "test-agent",
			Content:     "[task:nonexistent-task] data here",
			CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	// Should not error — the handler logs and moves on.
	if err := handler.HandleMessages(context.Background(), msgs); err != nil {
		t.Fatalf("HandleMessages should handle unknown task gracefully: %v", err)
	}

	if len(rhizome.sent) != 0 {
		t.Fatalf("expected no sent messages for unknown task, got %d", len(rhizome.sent))
	}
}

// contains is a small helper to check substring presence.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
