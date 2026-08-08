package living

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
)

// TaskStatus represents the status of a task.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusAborted   TaskStatus = "aborted"
	TaskStatusWaiting   TaskStatus = "waiting"
)

// TaskState tracks an individual task's execution within the living agent.
// It stores the conversation history, status, progress, and retry state.
// It is JSON-serializable for Redis persistence.
type TaskState struct {
	TaskID            string        `json:"task_id"`
	RhizomeTaskID     string        `json:"rhizome_task_id"`
	SessionID         string        `json:"session_id,omitempty"`
	Status            TaskStatus    `json:"status"`
	Messages          []llm.Message `json:"messages"`
	Context           string        `json:"context"`
	ProgressSummary   string        `json:"progress_summary"`
	IterationCount    int           `json:"iteration_count"`
	RetryCount        int           `json:"retry_count"`
	MaxRetries        int           `json:"max_retries"`
	WaitingForMessage string        `json:"waiting_for_message,omitempty"`
	CreatedAt         time.Time     `json:"created_at"`
	UpdatedAt         time.Time     `json:"updated_at"`
	Error             string        `json:"error,omitempty"`
}

// NewTaskState creates a new TaskState in pending status.
func NewTaskState(taskID, rhizomeTaskID string, maxRetries int) *TaskState {
	now := time.Now()
	return &TaskState{
		TaskID:        taskID,
		RhizomeTaskID: rhizomeTaskID,
		Status:        TaskStatusPending,
		Messages:      nil,
		MaxRetries:    maxRetries,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

// Start transitions the task from pending to running.
func (ts *TaskState) Start() error {
	if ts.Status != TaskStatusPending {
		return fmt.Errorf("cannot start task: current status is %q, expected %q", ts.Status, TaskStatusPending)
	}
	ts.Status = TaskStatusRunning
	ts.UpdatedAt = time.Now()
	return nil
}

// Complete transitions the task from running to completed.
func (ts *TaskState) Complete(summary string) error {
	if ts.Status != TaskStatusRunning {
		return fmt.Errorf("cannot complete task: current status is %q, expected %q", ts.Status, TaskStatusRunning)
	}
	ts.Status = TaskStatusCompleted
	ts.ProgressSummary = summary
	ts.UpdatedAt = time.Now()
	return nil
}

// Fail handles task failure. It increments RetryCount. If retries are not
// exhausted (RetryCount < MaxRetries), it resets the task to pending, clears
// messages, and returns true. If retries are exhausted, it sets status to
// failed and returns false.
func (ts *TaskState) Fail(errMsg string) (bool, error) {
	if ts.Status != TaskStatusRunning {
		return false, fmt.Errorf("cannot fail task: current status is %q, expected %q", ts.Status, TaskStatusRunning)
	}
	ts.RetryCount++
	ts.Error = errMsg
	ts.UpdatedAt = time.Now()

	if ts.RetryCount < ts.MaxRetries {
		ts.Status = TaskStatusPending
		ts.Messages = nil
		return true, nil
	}
	ts.Status = TaskStatusFailed
	return false, nil
}

// Abort transitions the task to aborted from any status.
func (ts *TaskState) Abort(reason string) {
	ts.Status = TaskStatusAborted
	ts.Error = reason
	ts.UpdatedAt = time.Now()
}

// Wait transitions the task from running to waiting.
func (ts *TaskState) Wait(messageID string) error {
	if ts.Status != TaskStatusRunning {
		return fmt.Errorf("cannot wait on task: current status is %q, expected %q", ts.Status, TaskStatusRunning)
	}
	ts.Status = TaskStatusWaiting
	ts.WaitingForMessage = messageID
	ts.UpdatedAt = time.Now()
	return nil
}

// Resume transitions the task from waiting to running.
func (ts *TaskState) Resume() error {
	if ts.Status != TaskStatusWaiting {
		return fmt.Errorf("cannot resume task: current status is %q, expected %q", ts.Status, TaskStatusWaiting)
	}
	ts.Status = TaskStatusRunning
	ts.WaitingForMessage = ""
	ts.UpdatedAt = time.Now()
	return nil
}

// MarshalJSON implements json.Marshaler using standard encoding/json.
func (ts *TaskState) MarshalJSON() ([]byte, error) {
	type Alias TaskState
	return json.Marshal((*Alias)(ts))
}

// UnmarshalJSON implements json.Unmarshaler using standard encoding/json.
func (ts *TaskState) UnmarshalJSON(data []byte) error {
	type Alias TaskState
	return json.Unmarshal(data, (*Alias)(ts))
}
