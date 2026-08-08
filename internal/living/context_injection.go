package living

import (
	"context"
	"fmt"
	"log"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
)

// ── Update evaluation types ─────────────────────────────────────────

// UpdateAction describes the action to take after evaluating an update.
type UpdateAction string

const (
	UpdateContinue UpdateAction = "CONTINUE"
	UpdateAdjust   UpdateAction = "ADJUST"
	UpdatePause    UpdateAction = "PAUSE"
	UpdateAbort    UpdateAction = "ABORT"
)

// EvalResult holds the outcome of evaluating a single update.
type EvalResult struct {
	Action UpdateAction
	Reason string
}

// UpdateEvaluator decides what action to take for a given update in the
// context of a running task.
type UpdateEvaluator interface {
	Evaluate(ctx context.Context, update Update, taskContext string) (*EvalResult, error)
}

// ── ContextInjector ─────────────────────────────────────────────────

// ContextInjector evaluates incoming task updates and decides whether to
// inject context, pause, or abort active tasks.
type ContextInjector struct {
	config     Config
	rhizome    RhizomeClient
	taskLookup TaskLookup
	evaluator  UpdateEvaluator
}

// NewContextInjector creates a ContextInjector with the given dependencies.
func NewContextInjector(config Config, rhizome RhizomeClient, taskLookup TaskLookup, evaluator UpdateEvaluator) *ContextInjector {
	return &ContextInjector{
		config:     config,
		rhizome:    rhizome,
		taskLookup: taskLookup,
		evaluator:  evaluator,
	}
}

// HandleTaskUpdates processes a batch of updates for a task. For each update
// it evaluates the action and applies it to the task state.
func (ci *ContextInjector) HandleTaskUpdates(ctx context.Context, taskID string, updates []Update) error {
	for _, update := range updates {
		task := ci.taskLookup.ActiveTaskByID(taskID)
		if task == nil {
			log.Printf("[context-injection] task %s not found in active tasks, skipping", taskID)
			continue
		}

		if task.Status != TaskStatusRunning {
			continue
		}

		result, err := ci.evaluator.Evaluate(ctx, update, task.ProgressSummary)
		if err != nil {
			log.Printf("[context-injection] evaluator error for task %s: %v, defaulting to CONTINUE", taskID, err)
			continue
		}

		switch result.Action {
		case UpdateContinue:
			// Do nothing.

		case UpdateAdjust:
			msg := llm.NewUserMessage(fmt.Sprintf("[SYSTEM UPDATE]: %s", result.Reason))
			task.Messages = append(task.Messages, msg)

		case UpdatePause:
			if err := task.Wait(""); err != nil {
				log.Printf("[context-injection] failed to pause task %s: %v", taskID, err)
				continue
			}
			recordSessionEventIfAvailable(ctx, ci.rhizome, pausedSessionEvent(task, ci.config.ID, taskID, update, result.Reason))

		case UpdateAbort:
			task.Abort(result.Reason)
			if err := ci.rhizome.ReleaseTask(ctx, taskID, ci.config.ID, result.Reason); err != nil {
				log.Printf("[context-injection] failed to release task %s: %v", taskID, err)
			}
		}
	}

	return nil
}
