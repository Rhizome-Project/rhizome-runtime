package living

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/hooks"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// TaskRunner abstracts task execution for testability.
type TaskRunner interface {
	RunTask(ctx context.Context, task Task) (result string, err error)
}

// TaskExecutor manages concurrent execution of tasks via a bounded pool.
//
// Deprecated: TaskExecutor relies on the experimental living runtime. It will be removed in Phase 4.
type TaskExecutor struct {
	config     Config
	rhizome    RhizomeClient
	stateStore StateStore
	taskRunner TaskRunner
	supervisor *agent.RuntimeSupervisor
	mu         sync.Mutex // reserved for generic sync if needed
}

// NewTaskExecutor creates a new TaskExecutor wired with the given dependencies.
func NewTaskExecutor(config Config, rhizome RhizomeClient, stateStore StateStore, taskRunner TaskRunner) *TaskExecutor {
	return &TaskExecutor{
		config:     config,
		rhizome:    rhizome,
		stateStore: stateStore,
		taskRunner: taskRunner,
		supervisor: agent.NewRuntimeSupervisor(nil),
	}
}

// HandleNewTasks claims available tasks up to the concurrency limit and starts execution goroutines.
func (te *TaskExecutor) HandleNewTasks(ctx context.Context, tasks []Task) error {
	activeCount := len(te.ActiveTasks())

	if activeCount >= te.config.MaxConcurrentTasks {
		return nil
	}

	slots := te.config.MaxConcurrentTasks - activeCount
	for i, task := range tasks {
		if i >= slots {
			break
		}

		if err := te.rhizome.ClaimTask(ctx, task.TaskID, te.config.ID); err != nil {
			log.Printf("[task-executor] failed to claim task %s: %v", task.TaskID, err)
			continue
		}

		state := NewTaskState(task.TaskID, task.TaskID, te.config.MaxRetries)
		if err := te.stateStore.SaveTaskState(ctx, state); err != nil {
			log.Printf("[task-executor] failed to save initial state for %s: %v", task.TaskID, err)
		}

		wrapper := &taskAgentWrapper{taskID: task.TaskID, task: task, te: te}
		if err := te.supervisor.Start(ctx, wrapper, task.TaskID); err != nil {
			continue // Already supervised
		}
		// execution now managed natively inside the wrapper startExecutionSync
	}

	return nil
}

// taskAgentWrapper implements agent.Agent to bridge internal/living legacy Tasks into RuntimeSupervisor.
type taskAgentWrapper struct {
	taskID string
	task   Task
	te     *TaskExecutor
}

func (w *taskAgentWrapper) ID() string {
	return w.taskID
}

func (w *taskAgentWrapper) Run(ctx context.Context, _ string) (*agent.LoopResult, error) {
	w.te.startExecutionSync(ctx, w.task)
	return &agent.LoopResult{}, nil
}

// startExecutionSync runs a task to completion. Managed securely by RuntimeSupervisor.
func (te *TaskExecutor) startExecutionSync(ctx context.Context, task Task) {
	state, err := te.stateStore.LoadTaskState(ctx, task.TaskID)
	if err != nil || state == nil {
		if state == nil {
			log.Printf("[task-executor] task %s not found in store", task.TaskID)
		} else {
			log.Printf("[task-executor] error loading state for %s: %v", task.TaskID, err)
		}
		return
	}

	if err := state.Start(); err != nil {
		log.Printf("[task-executor] failed to start task %s: %v", task.TaskID, err)
		return
	}
	sessionID := strings.TrimSpace(state.SessionID)
	isNewSession := sessionID == ""
	if isNewSession {
		sessionID = taskExecutionSessionID(te.config.ID, task.TaskID)
		state.SessionID = sessionID
	}
	if isNewSession {
		te.recordTaskSessionEvent(ctx, SessionEventInput{
			EventType:  model.SessionEventStart,
			SessionID:  sessionID,
			AgentID:    te.config.ID,
			TaskID:     task.TaskID,
			Summary:    taskSessionStartSummary(task),
			OwnerScope: "task/session",
		})
	} else {
		te.recordTaskSessionEvent(ctx, SessionEventInput{
			EventType: model.SessionEventStatus,
			SessionID: sessionID,
			AgentID:   te.config.ID,
			TaskID:    task.TaskID,
			Summary:   taskSessionEndSummary("Retrying task execution", task.TaskID, true),
		})
	}
	if err := te.stateStore.SaveTaskState(ctx, state); err != nil {
		log.Printf("[task-executor] failed to save running state for %s: %v", task.TaskID, err)
	}

	result, err := te.taskRunner.RunTask(ctx, task)
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled)) {
		te.recordTaskSessionEvent(ctx, SessionEventInput{
			EventType: model.SessionEventEnd,
			SessionID: sessionID,
			AgentID:   te.config.ID,
			TaskID:    task.TaskID,
			Summary:   "Task execution canceled",
		})
		if releaseErr := te.rhizome.ReleaseTask(context.Background(), task.TaskID, te.config.ID, "task execution canceled"); releaseErr != nil {
			log.Printf("[task-executor] failed to release canceled task %s: %v", task.TaskID, releaseErr)
		}
		return
	}

	if err == nil {
		if completeErr := state.Complete(result); completeErr != nil {
			log.Printf("[task-executor] failed to complete task state %s: %v", task.TaskID, completeErr)
		}
		if saveErr := te.stateStore.SaveTaskState(ctx, state); saveErr != nil {
			log.Printf("[task-executor] failed to save completed state for %s: %v", task.TaskID, saveErr)
		}
		if rhizErr := te.rhizome.CompleteTask(ctx, task.TaskID, result); rhizErr != nil {
			log.Printf("[task-executor] failed to complete task in rhizome %s: %v", task.TaskID, rhizErr)
		}
		te.recordTaskSessionEvent(ctx, SessionEventInput{
			EventType: model.SessionEventEnd,
			SessionID: sessionID,
			AgentID:   te.config.ID,
			TaskID:    task.TaskID,
			Summary:   taskSessionEndSummary(result, task.TaskID, false),
		})

		return
	}

	errorMsg := err.Error()
	retryable, failErr := state.Fail(errorMsg)
	if failErr != nil {
		log.Printf("[task-executor] failed to mark task state failed %s: %v", task.TaskID, failErr)
		return
	}
	if err := te.stateStore.SaveTaskState(ctx, state); err != nil {
		log.Printf("[task-executor] failed to save failed state for %s: %v", task.TaskID, err)
	}

	if retryable {
		te.recordTaskSessionEvent(ctx, SessionEventInput{
			EventType: model.SessionEventStatus,
			SessionID: sessionID,
			AgentID:   te.config.ID,
			TaskID:    task.TaskID,
			Summary:   taskSessionEndSummary("Retry scheduled after failure: "+errorMsg, task.TaskID, true),
		})
		delay := te.retryDelay(state.RetryCount)
		go func() {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				if releaseErr := te.rhizome.ReleaseTask(context.Background(), task.TaskID, te.config.ID, "task execution canceled during retry backoff"); releaseErr != nil {
					log.Printf("[task-executor] failed to release canceled task %s: %v", task.TaskID, releaseErr)
				}
			case <-timer.C:
				wrapper := &taskAgentWrapper{taskID: task.TaskID, task: task, te: te}
				if err := te.supervisor.Start(ctx, wrapper, task.TaskID); err != nil {
					log.Printf("[task-executor] failed to retry task %s: %v", task.TaskID, err)
				}
			}
		}()
		return
	}

	if rhizErr := te.rhizome.FailTask(ctx, task.TaskID, errorMsg); rhizErr != nil {
		log.Printf("[task-executor] failed to mark task failed %s: %v", task.TaskID, rhizErr)
	}
	te.recordTaskSessionEvent(ctx, SessionEventInput{
		EventType: model.SessionEventEnd,
		SessionID: sessionID,
		AgentID:   te.config.ID,
		TaskID:    task.TaskID,
		Summary:   taskSessionEndSummary("Task failed: "+errorMsg, task.TaskID, true),
	})

}

func (te *TaskExecutor) recordTaskSessionEvent(ctx context.Context, input SessionEventInput) {
	sessionClient, ok := te.rhizome.(SessionAwareRhizomeClient)
	if !ok {
		return
	}
	if err := sessionClient.RecordSessionEvent(ctx, input); err != nil {
		log.Printf("[task-executor] failed to record session event %s for %s: %v", input.EventType, input.TaskID, err)
	}
}

func taskExecutionSessionID(agentID, taskID string) string {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		taskID = "task"
	}
	return fmt.Sprintf("sess-%s-%s-%d", strings.TrimSpace(agentID), taskID, time.Now().UnixNano())
}

func taskSessionStartSummary(task Task) string {
	if trimmed := strings.TrimSpace(task.Description); trimmed != "" {
		return clipTaskSessionSummary(trimmed, 240)
	}
	if trimmed := strings.TrimSpace(task.Title); trimmed != "" {
		return clipTaskSessionSummary(trimmed, 240)
	}
	return "Executing task " + strings.TrimSpace(task.TaskID)
}

func taskSessionEndSummary(summary, taskID string, includeTaskPrefix bool) string {
	summary = clipTaskSessionSummary(summary, 240)
	if !includeTaskPrefix || strings.TrimSpace(taskID) == "" {
		return summary
	}
	return clipTaskSessionSummary("Task "+strings.TrimSpace(taskID)+": "+summary, 240)
}

func clipTaskSessionSummary(summary string, limit int) string {
	summary = strings.TrimSpace(summary)
	if summary == "" || limit <= 0 || len(summary) <= limit {
		return summary
	}
	if limit <= 3 {
		return summary[:limit]
	}
	return summary[:limit-3] + "..."
}

// ActiveTasks queries the underlying SQLite store for truth.
func (te *TaskExecutor) ActiveTasks() []*TaskState {
	states, err := te.stateStore.ListActiveTaskStates(context.Background())
	if err != nil {
		log.Printf("[task-executor] failed to list active tasks: %v", err)
		return nil
	}
	var active []*TaskState
	for _, state := range states {
		if state.Status == TaskStatusWaiting {
			active = append(active, state)
			continue
		}
		if te.supervisor.Health(state.TaskID) == agent.StatusRunning || te.supervisor.Health(state.TaskID) == agent.StatusStarting {
			active = append(active, state)
		}
	}
	return active
}

// ActiveTaskByID retrieves truth from state store.
func (te *TaskExecutor) ActiveTaskByID(taskID string) *TaskState {
	state, err := te.stateStore.LoadTaskState(context.Background(), taskID)
	if err != nil || state == nil {
		return nil
	}
	if state.Status == TaskStatusWaiting {
		return state
	}
	if te.supervisor.Health(taskID) != agent.StatusRunning && te.supervisor.Health(taskID) != agent.StatusStarting {
		return nil
	}
	return state
}

// RestoreFromStore loads active task states from the state store and resumes execution.
func (te *TaskExecutor) RestoreFromStore(ctx context.Context) error {
	states, err := te.stateStore.ListActiveTaskStates(ctx)
	if err != nil {
		return err
	}

	for _, state := range states {
		if strings.TrimSpace(state.SessionID) == "" {
			state.SessionID = taskExecutionSessionID(te.config.ID, state.TaskID)
			if err := te.stateStore.SaveTaskState(ctx, state); err != nil {
				log.Printf("[task-executor] failed to save restored session id for %s: %v", state.TaskID, err)
			}
		}
		switch state.Status {
		case TaskStatusRunning, TaskStatusPending:
			task := Task{
				TaskID:      state.TaskID,
				Description: state.ProgressSummary,
			}
			if state.Status == TaskStatusRunning {
				state.Status = TaskStatusPending
				state.UpdatedAt = time.Now()
			}
			wrapper := &taskAgentWrapper{taskID: state.TaskID, task: task, te: te}
			_ = te.supervisor.Start(ctx, wrapper, task.TaskID)
		case TaskStatusWaiting:
			// Keep waiting tasks in active set without restarting them.
		}
	}

	return nil
}

func (te *TaskExecutor) retryDelay(retryCount int) time.Duration {
	if retryCount <= 0 {
		return 0
	}
	delay := time.Duration(retryCount) * 250 * time.Millisecond
	if delay > 3*time.Second {
		return 3 * time.Second
	}
	return delay
}

// checkpointHook saves task state after each tool execution.
type checkpointHook struct {
	state      *TaskState
	stateStore StateStore
}

func (h *checkpointHook) Name() string { return "checkpoint" }

func (h *checkpointHook) Points() []hooks.Point { return []hooks.Point{hooks.AfterTool} }

func (h *checkpointHook) Run(ctx context.Context, hctx hooks.Context) (hooks.Result, error) {
	h.state.IterationCount++
	h.state.UpdatedAt = time.Now()
	if err := h.stateStore.SaveTaskState(ctx, h.state); err != nil {
		log.Printf("[task-executor] checkpoint save error for %s: %v", h.state.TaskID, err)
	}
	return hooks.Result{}, nil
}
