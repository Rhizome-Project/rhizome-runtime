package living

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// HeartbeatHandlers holds optional callbacks for each check the heartbeat loop
// performs on every tick. A nil callback means that check is skipped.
type HeartbeatHandlers struct {
	OnNewTasks            func(ctx context.Context, tasks []Task) error
	OnTaskUpdates         func(ctx context.Context, taskID string, updates []Update) error
	OnMessages            func(ctx context.Context, messages []Message) error
	OnCompactionNeeded    func(ctx context.Context) error
	OnSituationAssessment func(ctx context.Context) error
}

// HeartbeatLoop is the living agent's clock. It runs a time.Ticker and on each
// tick performs up to 5 checks in a fixed order. It does not execute tasks
// itself -- it dispatches to handlers.
type HeartbeatLoop struct {
	config     Config
	rhizome    RhizomeClient
	stateStore StateStore
	handlers   HeartbeatHandlers

	activeTasks     map[string]*TaskState
	tickCount       int64
	lastMessageTime time.Time
}

// NewHeartbeatLoop creates a new HeartbeatLoop wired with the given
// dependencies.
func NewHeartbeatLoop(config Config, rhizome RhizomeClient, stateStore StateStore, handlers HeartbeatHandlers) *HeartbeatLoop {
	return &HeartbeatLoop{
		config:      config,
		rhizome:     rhizome,
		stateStore:  stateStore,
		handlers:    handlers,
		activeTasks: make(map[string]*TaskState),
	}
}

// Run starts the heartbeat loop. It blocks until ctx is cancelled.
// Each tick sends a heartbeat to Rhizome and then runs the 5 checks via Tick.
func (h *HeartbeatLoop) Run(ctx context.Context) error {
	interval := h.config.HeartbeatInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Per-tick timeout: 80% of heartbeat interval.
			tickTimeout := interval * 80 / 100
			tickCtx, cancel := context.WithTimeout(ctx, tickTimeout)

			// Send heartbeat; failure is non-fatal.
			if err := h.rhizome.Heartbeat(tickCtx, h.config.ID, "alive", ""); err != nil {
				log.Printf("[living-heartbeat] heartbeat error: %v", err)
			}

			if err := h.Tick(tickCtx); err != nil {
				log.Printf("[living-heartbeat] tick error: %v", err)
			}

			cancel()
		}
	}
}

// Tick executes the 5 checks in order. Errors from individual checks are
// logged but do not stop the loop.
func (h *HeartbeatLoop) Tick(ctx context.Context) error {
	// Check 1: New tasks
	h.safeCall("new-tasks", func() error {
		return h.checkNewTasks(ctx)
	})

	// Check 2: Task updates
	h.safeCall("task-updates", func() error {
		return h.checkTaskUpdates(ctx)
	})

	// Check 3: Messages
	h.safeCall("messages", func() error {
		return h.checkMessages(ctx)
	})

	// Check 4: Compaction
	h.safeCall("compaction", func() error {
		return h.checkCompaction(ctx)
	})

	// Check 5: Situation assessment (every Nth tick)
	h.tickCount++
	if h.config.SituationCheckEvery > 0 && h.tickCount%int64(h.config.SituationCheckEvery) == 0 {
		h.safeCall("situation-assessment", func() error {
			return h.checkSituationAssessment(ctx)
		})
	}

	return nil
}

// safeCall invokes fn with panic recovery. Errors and panics are logged but
// never propagated.
func (h *HeartbeatLoop) safeCall(name string, fn func() error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[living-heartbeat] panic in %s: %v", name, r)
		}
	}()
	if err := fn(); err != nil {
		log.Printf("[living-heartbeat] %s error: %v", name, err)
	}
}

func (h *HeartbeatLoop) checkNewTasks(ctx context.Context) error {
	if h.handlers.OnNewTasks == nil {
		return nil
	}
	tasks, err := h.rhizome.FetchTasks(ctx, h.config.TaskTypes)
	if err != nil {
		return fmt.Errorf("fetch tasks: %w", err)
	}

	// Filter out tasks already tracked.
	var newTasks []Task
	for _, t := range tasks {
		if _, exists := h.activeTasks[t.TaskID]; !exists {
			newTasks = append(newTasks, t)
		}
	}
	if len(newTasks) == 0 {
		return nil
	}

	if err := h.handlers.OnNewTasks(ctx, newTasks); err != nil {
		return fmt.Errorf("on new tasks: %w", err)
	}

	// Track newly discovered tasks.
	for _, t := range newTasks {
		h.activeTasks[t.TaskID] = NewTaskState(t.TaskID, t.TaskID, h.config.MaxRetries)
	}
	return nil
}

func (h *HeartbeatLoop) checkTaskUpdates(ctx context.Context) error {
	if h.handlers.OnTaskUpdates == nil {
		return nil
	}
	for taskID, ts := range h.activeTasks {
		updates, err := h.rhizome.GetTaskUpdates(ctx, taskID, ts.UpdatedAt)
		if err != nil {
			log.Printf("[living-heartbeat] get updates for task %s: %v", taskID, err)
			continue
		}
		if len(updates) == 0 {
			continue
		}
		if err := h.handlers.OnTaskUpdates(ctx, taskID, updates); err != nil {
			log.Printf("[living-heartbeat] on task updates for %s: %v", taskID, err)
		}
	}
	return nil
}

func (h *HeartbeatLoop) checkMessages(ctx context.Context) error {
	if h.handlers.OnMessages == nil {
		return nil
	}
	messages, err := h.rhizome.FetchMessages(ctx, h.config.ID, h.lastMessageTime)
	if err != nil {
		return fmt.Errorf("fetch messages: %w", err)
	}
	if len(messages) == 0 {
		return nil
	}

	if err := h.handlers.OnMessages(ctx, messages); err != nil {
		return fmt.Errorf("on messages: %w", err)
	}

	// Update watermark to latest message time.
	for _, m := range messages {
		if t, err := time.Parse(time.RFC3339Nano, m.CreatedAt); err == nil {
			if t.After(h.lastMessageTime) {
				h.lastMessageTime = t
			}
		}
	}
	return nil
}

func (h *HeartbeatLoop) checkCompaction(ctx context.Context) error {
	if h.handlers.OnCompactionNeeded == nil {
		return nil
	}
	if compactionClient, ok := h.rhizome.(CompactionAwareRhizomeClient); ok {
		candidates, err := compactionClient.ListSessionCompactionCandidates(
			ctx,
			h.config.ID,
			model.DefaultSessionCompactionMinMessages,
			model.DefaultSessionCompactionMinTokens,
		)
		if err != nil {
			return fmt.Errorf("list canonical compaction candidates: %w", err)
		}
		if len(candidates) > 0 {
			return h.handlers.OnCompactionNeeded(ctx)
		}
		return nil
	}

	// Compatibility fallback for clients that do not yet expose the canonical
	// session ledger view.
	for _, ts := range h.activeTasks {
		if len(ts.Messages) > 100 {
			return h.handlers.OnCompactionNeeded(ctx)
		}
	}
	return nil
}

func (h *HeartbeatLoop) checkSituationAssessment(ctx context.Context) error {
	if h.handlers.OnSituationAssessment == nil {
		return nil
	}
	return h.handlers.OnSituationAssessment(ctx)
}

// TickCount returns the current tick count (for testing).
func (h *HeartbeatLoop) TickCount() int64 {
	return h.tickCount
}

// ActiveTasks returns the current active tasks map (for testing).
func (h *HeartbeatLoop) ActiveTasks() map[string]*TaskState {
	return h.activeTasks
}
