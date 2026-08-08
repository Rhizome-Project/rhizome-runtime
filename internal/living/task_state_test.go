package living

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
)

func TestTaskState_NewTaskState(t *testing.T) {
	ts := NewTaskState("task-1", "rhizome-1", 3)

	if ts.TaskID != "task-1" {
		t.Errorf("expected TaskID %q, got %q", "task-1", ts.TaskID)
	}
	if ts.RhizomeTaskID != "rhizome-1" {
		t.Errorf("expected RhizomeTaskID %q, got %q", "rhizome-1", ts.RhizomeTaskID)
	}
	if ts.Status != TaskStatusPending {
		t.Errorf("expected status %q, got %q", TaskStatusPending, ts.Status)
	}
	if ts.IterationCount != 0 {
		t.Errorf("expected IterationCount 0, got %d", ts.IterationCount)
	}
	if ts.RetryCount != 0 {
		t.Errorf("expected RetryCount 0, got %d", ts.RetryCount)
	}
	if ts.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", ts.MaxRetries)
	}
	if ts.CreatedAt.IsZero() {
		t.Error("expected CreatedAt to be set")
	}
	if ts.UpdatedAt.IsZero() {
		t.Error("expected UpdatedAt to be set")
	}
}

func TestTaskState_HappyPath(t *testing.T) {
	ts := NewTaskState("task-1", "rhizome-1", 3)

	if err := ts.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	if ts.Status != TaskStatusRunning {
		t.Errorf("expected status %q, got %q", TaskStatusRunning, ts.Status)
	}

	if err := ts.Complete("all done"); err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	if ts.Status != TaskStatusCompleted {
		t.Errorf("expected status %q, got %q", TaskStatusCompleted, ts.Status)
	}
	if ts.ProgressSummary != "all done" {
		t.Errorf("expected ProgressSummary %q, got %q", "all done", ts.ProgressSummary)
	}
}

func TestTaskState_FailWithRetry(t *testing.T) {
	ts := NewTaskState("task-1", "rhizome-1", 2)

	if err := ts.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	// Add a message so we can verify it gets cleared
	ts.Messages = append(ts.Messages, llm.NewUserMessage("hello"))

	retryable, err := ts.Fail("something went wrong")
	if err != nil {
		t.Fatalf("Fail() error: %v", err)
	}
	if !retryable {
		t.Error("expected Fail() to return true (retryable)")
	}
	if ts.Status != TaskStatusPending {
		t.Errorf("expected status %q, got %q", TaskStatusPending, ts.Status)
	}
	if ts.RetryCount != 1 {
		t.Errorf("expected RetryCount 1, got %d", ts.RetryCount)
	}
	if len(ts.Messages) != 0 {
		t.Errorf("expected Messages to be cleared, got %d", len(ts.Messages))
	}
	if ts.Error != "something went wrong" {
		t.Errorf("expected Error %q, got %q", "something went wrong", ts.Error)
	}
}

func TestTaskState_FailExhausted(t *testing.T) {
	ts := NewTaskState("task-1", "rhizome-1", 1)

	if err := ts.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	retryable, err := ts.Fail("final failure")
	if err != nil {
		t.Fatalf("Fail() error: %v", err)
	}
	if retryable {
		t.Error("expected Fail() to return false (not retryable)")
	}
	if ts.Status != TaskStatusFailed {
		t.Errorf("expected status %q, got %q", TaskStatusFailed, ts.Status)
	}
	if ts.RetryCount != 1 {
		t.Errorf("expected RetryCount 1, got %d", ts.RetryCount)
	}
}

func TestTaskState_FailZeroMaxRetries(t *testing.T) {
	// EC-3: Fail with 0 maxRetries -> immediately set to failed, RetryCount = 1
	ts := NewTaskState("task-1", "rhizome-1", 0)

	if err := ts.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	retryable, err := ts.Fail("no retries")
	if err != nil {
		t.Fatalf("Fail() error: %v", err)
	}
	if retryable {
		t.Error("expected Fail() to return false")
	}
	if ts.Status != TaskStatusFailed {
		t.Errorf("expected status %q, got %q", TaskStatusFailed, ts.Status)
	}
	if ts.RetryCount != 1 {
		t.Errorf("expected RetryCount 1, got %d", ts.RetryCount)
	}
}

func TestTaskState_InvalidTransition(t *testing.T) {
	ts := NewTaskState("task-1", "rhizome-1", 3)

	// Start and complete
	_ = ts.Start()
	_ = ts.Complete("done")

	// EC-1 style: Start on completed task
	if err := ts.Start(); err == nil {
		t.Error("expected error when starting completed task")
	}

	// EC-2: Complete on pending task
	ts2 := NewTaskState("task-2", "rhizome-2", 3)
	if err := ts2.Complete("nope"); err == nil {
		t.Error("expected error when completing pending task")
	}

	// Double-start
	ts3 := NewTaskState("task-3", "rhizome-3", 3)
	_ = ts3.Start()
	if err := ts3.Start(); err == nil {
		t.Error("expected error when double-starting")
	}

	// Fail on pending
	ts4 := NewTaskState("task-4", "rhizome-4", 3)
	_, err := ts4.Fail("nope")
	if err == nil {
		t.Error("expected error when failing pending task")
	}

	// Resume on non-waiting
	ts5 := NewTaskState("task-5", "rhizome-5", 3)
	_ = ts5.Start()
	if err := ts5.Resume(); err == nil {
		t.Error("expected error when resuming running task")
	}

	// Wait on pending
	ts6 := NewTaskState("task-6", "rhizome-6", 3)
	if err := ts6.Wait("msg-1"); err == nil {
		t.Error("expected error when waiting on pending task")
	}
}

func TestTaskState_AbortFromAny(t *testing.T) {
	statuses := []struct {
		name  string
		setup func() *TaskState
	}{
		{
			name: "pending",
			setup: func() *TaskState {
				return NewTaskState("t", "r", 3)
			},
		},
		{
			name: "running",
			setup: func() *TaskState {
				ts := NewTaskState("t", "r", 3)
				_ = ts.Start()
				return ts
			},
		},
		{
			name: "waiting",
			setup: func() *TaskState {
				ts := NewTaskState("t", "r", 3)
				_ = ts.Start()
				_ = ts.Wait("msg-1")
				return ts
			},
		},
		{
			name: "completed",
			setup: func() *TaskState {
				ts := NewTaskState("t", "r", 3)
				_ = ts.Start()
				_ = ts.Complete("done")
				return ts
			},
		},
	}

	for _, tc := range statuses {
		t.Run(tc.name, func(t *testing.T) {
			ts := tc.setup()
			ts.Abort("cancelled")
			if ts.Status != TaskStatusAborted {
				t.Errorf("expected status %q, got %q", TaskStatusAborted, ts.Status)
			}
			if ts.Error != "cancelled" {
				t.Errorf("expected Error %q, got %q", "cancelled", ts.Error)
			}
		})
	}
}

func TestTaskState_WaitAndResume(t *testing.T) {
	ts := NewTaskState("task-1", "rhizome-1", 3)

	_ = ts.Start()

	if err := ts.Wait("msg-42"); err != nil {
		t.Fatalf("Wait() error: %v", err)
	}
	if ts.Status != TaskStatusWaiting {
		t.Errorf("expected status %q, got %q", TaskStatusWaiting, ts.Status)
	}
	if ts.WaitingForMessage != "msg-42" {
		t.Errorf("expected WaitingForMessage %q, got %q", "msg-42", ts.WaitingForMessage)
	}

	if err := ts.Resume(); err != nil {
		t.Fatalf("Resume() error: %v", err)
	}
	if ts.Status != TaskStatusRunning {
		t.Errorf("expected status %q, got %q", TaskStatusRunning, ts.Status)
	}
	if ts.WaitingForMessage != "" {
		t.Errorf("expected WaitingForMessage to be cleared, got %q", ts.WaitingForMessage)
	}
}

func TestTaskState_JSONRoundtrip(t *testing.T) {
	ts := NewTaskState("task-1", "rhizome-1", 3)
	_ = ts.Start()
	ts.Messages = []llm.Message{
		llm.NewUserMessage("hello"),
		llm.NewAssistantMessage([]llm.ContentBlock{
			{Type: "text", Text: "hi there"},
		}),
		llm.NewToolResultMessage([]llm.ToolResult{
			{ToolUseID: "tool-1", Content: "result", IsError: false},
		}),
	}
	ts.Context = "some context"
	ts.ProgressSummary = "halfway"
	ts.IterationCount = 5
	ts.Error = "last error"

	data, err := json.Marshal(ts)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	var ts2 TaskState
	if err := json.Unmarshal(data, &ts2); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	// Verify fields
	if ts2.TaskID != ts.TaskID {
		t.Errorf("TaskID: got %q, want %q", ts2.TaskID, ts.TaskID)
	}
	if ts2.RhizomeTaskID != ts.RhizomeTaskID {
		t.Errorf("RhizomeTaskID: got %q, want %q", ts2.RhizomeTaskID, ts.RhizomeTaskID)
	}
	if ts2.Status != ts.Status {
		t.Errorf("Status: got %q, want %q", ts2.Status, ts.Status)
	}
	if len(ts2.Messages) != len(ts.Messages) {
		t.Fatalf("Messages: got %d, want %d", len(ts2.Messages), len(ts.Messages))
	}
	for i, msg := range ts2.Messages {
		if msg.Role != ts.Messages[i].Role {
			t.Errorf("Messages[%d].Role: got %q, want %q", i, msg.Role, ts.Messages[i].Role)
		}
		if len(msg.Content) != len(ts.Messages[i].Content) {
			t.Errorf("Messages[%d].Content length: got %d, want %d", i, len(msg.Content), len(ts.Messages[i].Content))
		}
	}
	if ts2.Context != ts.Context {
		t.Errorf("Context: got %q, want %q", ts2.Context, ts.Context)
	}
	if ts2.ProgressSummary != ts.ProgressSummary {
		t.Errorf("ProgressSummary: got %q, want %q", ts2.ProgressSummary, ts.ProgressSummary)
	}
	if ts2.IterationCount != ts.IterationCount {
		t.Errorf("IterationCount: got %d, want %d", ts2.IterationCount, ts.IterationCount)
	}
	if ts2.MaxRetries != ts.MaxRetries {
		t.Errorf("MaxRetries: got %d, want %d", ts2.MaxRetries, ts.MaxRetries)
	}
	if ts2.Error != ts.Error {
		t.Errorf("Error: got %q, want %q", ts2.Error, ts.Error)
	}
	if !ts2.CreatedAt.Equal(ts.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", ts2.CreatedAt, ts.CreatedAt)
	}
	if !ts2.UpdatedAt.Equal(ts.UpdatedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", ts2.UpdatedAt, ts.UpdatedAt)
	}
}

func TestTaskState_JSONRoundtripEmptyMessages(t *testing.T) {
	// EC-5: JSON roundtrip with empty Messages slice
	ts := NewTaskState("task-1", "rhizome-1", 0)

	data, err := json.Marshal(ts)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	var ts2 TaskState
	if err := json.Unmarshal(data, &ts2); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	if ts2.Status != TaskStatusPending {
		t.Errorf("Status: got %q, want %q", ts2.Status, TaskStatusPending)
	}
	// Both nil and empty are acceptable per EC-5
}

func TestTaskState_UpdatedAtChanges(t *testing.T) {
	// R-7: All transition methods update UpdatedAt
	ts := NewTaskState("task-1", "rhizome-1", 3)
	initialTime := ts.UpdatedAt

	// Small sleep to ensure time difference
	time.Sleep(time.Millisecond)

	_ = ts.Start()
	if !ts.UpdatedAt.After(initialTime) {
		t.Error("Start() should update UpdatedAt")
	}

	prev := ts.UpdatedAt
	time.Sleep(time.Millisecond)

	_ = ts.Wait("msg-1")
	if !ts.UpdatedAt.After(prev) {
		t.Error("Wait() should update UpdatedAt")
	}

	prev = ts.UpdatedAt
	time.Sleep(time.Millisecond)

	_ = ts.Resume()
	if !ts.UpdatedAt.After(prev) {
		t.Error("Resume() should update UpdatedAt")
	}

	prev = ts.UpdatedAt
	time.Sleep(time.Millisecond)

	_ = ts.Complete("done")
	if !ts.UpdatedAt.After(prev) {
		t.Error("Complete() should update UpdatedAt")
	}

	prev = ts.UpdatedAt
	time.Sleep(time.Millisecond)

	ts.Abort("reason")
	if !ts.UpdatedAt.After(prev) {
		t.Error("Abort() should update UpdatedAt")
	}
}
