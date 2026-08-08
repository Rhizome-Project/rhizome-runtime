package living_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
)

// --- Mock SituationLLM ---

type mockSituationLLM struct {
	response string
	err      error
	called   bool
}

func (m *mockSituationLLM) Assess(_ context.Context, _ string) (string, error) {
	m.called = true
	return m.response, m.err
}

// --- Mock tasks provider ---

type mockTasksProvider struct {
	tasks map[string]*living.TaskState
}

func (m *mockTasksProvider) ActiveTaskByID(taskID string) *living.TaskState {
	return m.tasks[taskID]
}

func (m *mockTasksProvider) AllTasks() []*living.TaskState {
	result := make([]*living.TaskState, 0, len(m.tasks))
	for _, t := range m.tasks {
		result = append(result, t)
	}
	return result
}

// --- Mock RhizomeClient for situation tests ---

type mockRhizomeForSituation struct {
	mu              sync.Mutex
	sendUpdateCalls []sendUpdateCall
	escalateCalls   []escalateCall
	sessionEvents   []living.SessionEventInput
}

type sendUpdateCall struct {
	agentID     string
	workspaceID string
	updateType  string
	summary     string
	payload     string
}

type escalateCall struct {
	taskID string
	reason string
}

func newMockRhizomeForSituation() *mockRhizomeForSituation {
	return &mockRhizomeForSituation{}
}

func (m *mockRhizomeForSituation) FetchTasks(_ context.Context, _ []string) ([]living.Task, error) {
	return nil, nil
}
func (m *mockRhizomeForSituation) ClaimTask(_ context.Context, _, _ string) error      { return nil }
func (m *mockRhizomeForSituation) ReleaseTask(_ context.Context, _, _, _ string) error { return nil }
func (m *mockRhizomeForSituation) CompleteTask(_ context.Context, _, _ string) error   { return nil }
func (m *mockRhizomeForSituation) FailTask(_ context.Context, _, _ string) error       { return nil }
func (m *mockRhizomeForSituation) GetTaskUpdates(_ context.Context, _ string, _ time.Time) ([]living.Update, error) {
	return nil, nil
}
func (m *mockRhizomeForSituation) SendUpdate(_ context.Context, agentID, workspaceID, updateType, summary, payload string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendUpdateCalls = append(m.sendUpdateCalls, sendUpdateCall{
		agentID:     agentID,
		workspaceID: workspaceID,
		updateType:  updateType,
		summary:     summary,
		payload:     payload,
	})
	return nil
}
func (m *mockRhizomeForSituation) FetchMessages(_ context.Context, _ string, _ time.Time) ([]living.Message, error) {
	return nil, nil
}
func (m *mockRhizomeForSituation) SendMessage(_ context.Context, _, _, _, _ string) error { return nil }
func (m *mockRhizomeForSituation) Heartbeat(_ context.Context, _, _, _ string) error      { return nil }
func (m *mockRhizomeForSituation) EscalateTask(_ context.Context, taskID, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.escalateCalls = append(m.escalateCalls, escalateCall{taskID: taskID, reason: reason})
	return nil
}
func (m *mockRhizomeForSituation) RecordSessionEvent(_ context.Context, input living.SessionEventInput) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessionEvents = append(m.sessionEvents, input)
	return nil
}

func (m *mockRhizomeForSituation) getSendUpdateCalls() []sendUpdateCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]sendUpdateCall, len(m.sendUpdateCalls))
	copy(result, m.sendUpdateCalls)
	return result
}

func (m *mockRhizomeForSituation) getEscalateCalls() []escalateCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]escalateCall, len(m.escalateCalls))
	copy(result, m.escalateCalls)
	return result
}

func (m *mockRhizomeForSituation) getSessionEvents() []living.SessionEventInput {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]living.SessionEventInput, len(m.sessionEvents))
	copy(result, m.sessionEvents)
	return result
}

// --- Helper ---

func makeSituationConfig() living.Config {
	return living.Config{
		ID:        "test-agent",
		TaskTypes: []string{"code"},
	}
}

func makeRunningTask(taskID string) *living.TaskState {
	state := living.NewTaskState(taskID, taskID, 2)
	state.SessionID = "sess-" + taskID
	_ = state.Start()
	return state
}

func mustMarshalResults(results []living.AssessmentResult) string {
	data, err := json.Marshal(results)
	if err != nil {
		panic(fmt.Sprintf("marshal assessment results: %v", err))
	}
	return string(data)
}

// T-1: TestSituationAssessor_NoActiveTasks
func TestSituationAssessor_NoActiveTasks(t *testing.T) {
	t.Parallel()

	llmMock := &mockSituationLLM{response: "[]"}
	provider := &mockTasksProvider{tasks: map[string]*living.TaskState{}}
	rhizome := newMockRhizomeForSituation()

	sa := living.NewSituationAssessor(
		makeSituationConfig(),
		rhizome,
		provider,
		provider.AllTasks,
		llmMock,
	)

	err := sa.Assess(context.Background())
	if err != nil {
		t.Fatalf("Assess returned error: %v", err)
	}

	if llmMock.called {
		t.Fatal("expected LLM not to be called when there are no active tasks")
	}
}

// T-2: TestSituationAssessor_HealthyTasks
func TestSituationAssessor_HealthyTasks(t *testing.T) {
	t.Parallel()

	task := makeRunningTask("t1")
	provider := &mockTasksProvider{tasks: map[string]*living.TaskState{"t1": task}}
	rhizome := newMockRhizomeForSituation()

	results := []living.AssessmentResult{
		{TaskID: "t1", Assessment: living.AssessmentHealthy, Action: "none", Details: "ok"},
	}
	llmMock := &mockSituationLLM{response: mustMarshalResults(results)}

	sa := living.NewSituationAssessor(
		makeSituationConfig(),
		rhizome,
		provider,
		provider.AllTasks,
		llmMock,
	)

	msgCountBefore := len(task.Messages)

	err := sa.Assess(context.Background())
	if err != nil {
		t.Fatalf("Assess returned error: %v", err)
	}

	if !llmMock.called {
		t.Fatal("expected LLM to be called")
	}

	// No messages should be injected for healthy tasks
	if len(task.Messages) != msgCountBefore {
		t.Fatalf("expected no new messages, got %d new", len(task.Messages)-msgCountBefore)
	}

	// No rhizome calls
	if len(rhizome.getSendUpdateCalls()) != 0 {
		t.Fatal("expected no SendUpdate calls for healthy task")
	}
	if len(rhizome.getEscalateCalls()) != 0 {
		t.Fatal("expected no EscalateTask calls for healthy task")
	}
}

// T-3: TestSituationAssessor_StuckTask
func TestSituationAssessor_StuckTask(t *testing.T) {
	t.Parallel()

	task := makeRunningTask("t1")
	provider := &mockTasksProvider{tasks: map[string]*living.TaskState{"t1": task}}
	rhizome := newMockRhizomeForSituation()

	results := []living.AssessmentResult{
		{TaskID: "t1", Assessment: living.AssessmentStuck, Action: "inject", Details: "Try a different approach"},
	}
	llmMock := &mockSituationLLM{response: mustMarshalResults(results)}

	sa := living.NewSituationAssessor(
		makeSituationConfig(),
		rhizome,
		provider,
		provider.AllTasks,
		llmMock,
	)

	err := sa.Assess(context.Background())
	if err != nil {
		t.Fatalf("Assess returned error: %v", err)
	}

	// Should have injected an advice message
	if len(task.Messages) != 1 {
		t.Fatalf("expected 1 injected message, got %d", len(task.Messages))
	}

	// Verify message content contains the details
	msg := task.Messages[0]
	if msg.Role != "user" {
		t.Fatalf("expected user role, got %s", msg.Role)
	}
	found := false
	for _, block := range msg.Content {
		if block.Type == "text" && block.Text != "" {
			if strings.Contains(block.Text, "Try a different approach") {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected injected message to contain assessment details")
	}
}

// T-4: TestSituationAssessor_BlockedTask
func TestSituationAssessor_BlockedTask(t *testing.T) {
	t.Parallel()

	task := makeRunningTask("t1")
	provider := &mockTasksProvider{tasks: map[string]*living.TaskState{"t1": task}}
	rhizome := newMockRhizomeForSituation()

	results := []living.AssessmentResult{
		{TaskID: "t1", Assessment: living.AssessmentBlocked, Action: "notify", Details: "waiting for API access"},
	}
	llmMock := &mockSituationLLM{response: mustMarshalResults(results)}

	sa := living.NewSituationAssessor(
		makeSituationConfig(),
		rhizome,
		provider,
		provider.AllTasks,
		llmMock,
	)

	err := sa.Assess(context.Background())
	if err != nil {
		t.Fatalf("Assess returned error: %v", err)
	}

	calls := rhizome.getSendUpdateCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 SendUpdate call, got %d", len(calls))
	}
	if calls[0].updateType != "blocked" {
		t.Fatalf("expected update type 'blocked', got %q", calls[0].updateType)
	}
	if !strings.Contains(calls[0].summary, "waiting for API access") {
		t.Fatalf("expected summary to contain details, got %q", calls[0].summary)
	}
	sessionEvents := rhizome.getSessionEvents()
	if len(sessionEvents) != 1 {
		t.Fatalf("expected 1 session event, got %+v", sessionEvents)
	}
	if sessionEvents[0].EventType != "session.blocked" {
		t.Fatalf("expected blocked session event, got %+v", sessionEvents[0])
	}
}

// T-5: TestSituationAssessor_EscalateTask
func TestSituationAssessor_EscalateTask(t *testing.T) {
	t.Parallel()

	task := makeRunningTask("t1")
	provider := &mockTasksProvider{tasks: map[string]*living.TaskState{"t1": task}}
	rhizome := newMockRhizomeForSituation()

	results := []living.AssessmentResult{
		{TaskID: "t1", Assessment: living.AssessmentEscalate, Action: "escalate", Details: "requires human decision"},
	}
	llmMock := &mockSituationLLM{response: mustMarshalResults(results)}

	sa := living.NewSituationAssessor(
		makeSituationConfig(),
		rhizome,
		provider,
		provider.AllTasks,
		llmMock,
	)

	err := sa.Assess(context.Background())
	if err != nil {
		t.Fatalf("Assess returned error: %v", err)
	}

	calls := rhizome.getEscalateCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 EscalateTask call, got %d", len(calls))
	}
	if calls[0].taskID != "t1" {
		t.Fatalf("expected escalation for t1, got %s", calls[0].taskID)
	}
	if calls[0].reason != "requires human decision" {
		t.Fatalf("expected reason 'requires human decision', got %q", calls[0].reason)
	}
	sessionEvents := rhizome.getSessionEvents()
	if len(sessionEvents) != 1 {
		t.Fatalf("expected 1 session event, got %+v", sessionEvents)
	}
	if sessionEvents[0].EventType != "session.decision_needed" {
		t.Fatalf("expected decision-needed session event, got %+v", sessionEvents[0])
	}
	if sessionEvents[0].DecisionNeededFrom != "human" {
		t.Fatalf("expected decision_needed_from=human, got %+v", sessionEvents[0])
	}
}

// T-6: TestSituationAssessor_LLMFailure
func TestSituationAssessor_LLMFailure(t *testing.T) {
	t.Parallel()

	task := makeRunningTask("t1")
	provider := &mockTasksProvider{tasks: map[string]*living.TaskState{"t1": task}}
	rhizome := newMockRhizomeForSituation()

	llmMock := &mockSituationLLM{err: fmt.Errorf("LLM service unavailable")}

	sa := living.NewSituationAssessor(
		makeSituationConfig(),
		rhizome,
		provider,
		provider.AllTasks,
		llmMock,
	)

	err := sa.Assess(context.Background())
	if err != nil {
		t.Fatalf("expected nil error on LLM failure, got: %v", err)
	}

	// No rhizome calls should have been made
	if len(rhizome.getSendUpdateCalls()) != 0 {
		t.Fatal("expected no SendUpdate calls on LLM failure")
	}
	if len(rhizome.getEscalateCalls()) != 0 {
		t.Fatal("expected no EscalateTask calls on LLM failure")
	}
}

// T-7: TestSituationAssessor_InvalidJSON
func TestSituationAssessor_InvalidJSON(t *testing.T) {
	t.Parallel()

	task := makeRunningTask("t1")
	provider := &mockTasksProvider{tasks: map[string]*living.TaskState{"t1": task}}
	rhizome := newMockRhizomeForSituation()

	llmMock := &mockSituationLLM{response: "bad json"}

	sa := living.NewSituationAssessor(
		makeSituationConfig(),
		rhizome,
		provider,
		provider.AllTasks,
		llmMock,
	)

	err := sa.Assess(context.Background())
	if err != nil {
		t.Fatalf("expected nil error on invalid JSON, got: %v", err)
	}

	// No rhizome calls should have been made
	if len(rhizome.getSendUpdateCalls()) != 0 {
		t.Fatal("expected no SendUpdate calls on invalid JSON")
	}
	if len(rhizome.getEscalateCalls()) != 0 {
		t.Fatal("expected no EscalateTask calls on invalid JSON")
	}
}
