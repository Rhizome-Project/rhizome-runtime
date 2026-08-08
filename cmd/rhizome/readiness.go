package main

import (
	"sync"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// LoopState represents the current lifecycle state of a background loop.
// These states reflect actual loop ownership, not synthetic config-based guesses.
type LoopState string

const (
	// LoopNotStarted means the loop has been registered but never started.
	LoopNotStarted LoopState = "not_started"

	// LoopRunning means the loop is actively processing.
	LoopRunning LoopState = "running"

	// LoopRecovering means the loop crashed and is restarting with backoff.
	LoopRecovering LoopState = "recovering"

	// LoopDisabled means the loop was intentionally not started (e.g. --with-daemon=false).
	LoopDisabled LoopState = "disabled"

	// LoopDegraded means the loop is running but experiencing partial failures
	// (e.g. firehose dropping events due to full buffer).
	LoopDegraded LoopState = "degraded"

	// LoopStopped means the loop has been cleanly stopped.
	LoopStopped LoopState = "stopped"
)

// LoopReadiness captures the readiness signal for one background loop.
type LoopReadiness struct {
	Name          string    `json:"name"`
	State         LoopState `json:"state"`
	Since         string    `json:"since"`
	LastSuccess   string    `json:"last_success,omitempty"`
	LastError     string    `json:"last_error,omitempty"`
	LastErrorAt   string    `json:"last_error_at,omitempty"`
	Restarts      int       `json:"restarts"`
	DroppedEvents int64     `json:"dropped_events,omitempty"`
}

// ReadinessRegistry is a minimal thread-safe registry for background loop readiness.
//
// This is NOT a RuntimeSupervisor (that is P3C-001). Loops self-report their state
// through Set() calls. The registry provides a diagnostic snapshot for health/diagnostics
// endpoints and the doctor command.
type ReadinessRegistry struct {
	mu    sync.RWMutex
	loops map[string]*LoopReadiness
}

// NewReadinessRegistry creates a new empty registry.
func NewReadinessRegistry() *ReadinessRegistry {
	return &ReadinessRegistry{
		loops: make(map[string]*LoopReadiness),
	}
}

// Register adds a loop with initial not_started state.
func (r *ReadinessRegistry) Register(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loops[name] = &LoopReadiness{
		Name:  name,
		State: LoopNotStarted,
		Since: time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// SetState updates the state of a registered loop.
func (r *ReadinessRegistry) SetState(name string, state LoopState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	loop, ok := r.loops[name]
	if !ok {
		return
	}
	if loop.State != state {
		loop.State = state
		loop.Since = time.Now().UTC().Format(time.RFC3339Nano)
	}
}

// SetSuccess records a successful loop tick and marks the loop as running.
func (r *ReadinessRegistry) SetSuccess(name string) {
	r.setSuccessAt(name, time.Now().UTC())
}

func (r *ReadinessRegistry) setSuccessAt(name string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	loop, ok := r.loops[name]
	if !ok {
		return
	}
	ts := at.UTC().Format(time.RFC3339Nano)
	if loop.State != LoopRunning {
		loop.State = LoopRunning
		loop.Since = ts
	}
	loop.LastSuccess = ts
}

// SetError updates the state to recovering and records the error.
func (r *ReadinessRegistry) SetError(name string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	loop, ok := r.loops[name]
	if !ok {
		return
	}
	loop.State = LoopRecovering
	loop.Since = time.Now().UTC().Format(time.RFC3339Nano)
	loop.Restarts++
	if err != nil {
		loop.LastError = err.Error()
		loop.LastErrorAt = loop.Since
	}
}

// SetDegraded records a non-fatal loop failure without treating it as a restart.
func (r *ReadinessRegistry) SetDegraded(name string, err error) {
	r.setDegradedAt(name, err, time.Now().UTC())
}

func (r *ReadinessRegistry) setDegradedAt(name string, err error, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	loop, ok := r.loops[name]
	if !ok {
		return
	}
	ts := at.UTC().Format(time.RFC3339Nano)
	if loop.State != LoopDegraded {
		loop.State = LoopDegraded
		loop.Since = ts
	}
	if err != nil {
		loop.LastError = err.Error()
		loop.LastErrorAt = ts
	}
}

// SetDroppedEvents updates the dropped event count (for firehose).
func (r *ReadinessRegistry) SetDroppedEvents(name string, count int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	loop, ok := r.loops[name]
	if !ok {
		return
	}
	loop.DroppedEvents = count
	if count > 0 && loop.State != LoopDisabled && loop.State != LoopStopped {
		loop.State = LoopDegraded
		loop.Since = time.Now().UTC().Format(time.RFC3339Nano)
	}
}

// Get returns a copy of the readiness for one loop, or nil if not registered.
func (r *ReadinessRegistry) Get(name string) *LoopReadiness {
	r.mu.RLock()
	defer r.mu.RUnlock()
	loop, ok := r.loops[name]
	if !ok {
		return nil
	}
	copy := *loop
	return &copy
}

// Snapshot returns a copy of all registered loop readiness states.
func (r *ReadinessRegistry) Snapshot() []LoopReadiness {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]LoopReadiness, 0, len(r.loops))
	for _, loop := range r.loops {
		result = append(result, *loop)
	}
	return result
}

// OverallState returns the worst-case state across all registered loops.
// Priority: recovering > degraded > not_started > stopped > running > disabled.
func (r *ReadinessRegistry) OverallState() LoopState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.loops) == 0 {
		return LoopNotStarted
	}

	worst := LoopDisabled
	for _, loop := range r.loops {
		if statePriority(loop.State) > statePriority(worst) {
			worst = loop.State
		}
	}
	return worst
}

func statePriority(s LoopState) int {
	switch s {
	case LoopDisabled:
		return 0
	case LoopRunning:
		return 1
	case LoopStopped:
		return 2
	case LoopNotStarted:
		return 3
	case LoopDegraded:
		return 4
	case LoopRecovering:
		return 5
	default:
		return 6
	}
}

// syncFirehoseReadiness queries the actual firehose state from the Store
// and updates the readiness registry. This reads honest runtime signals
// (IsRunning, DroppedEvents), not config flags.
func syncFirehoseReadiness(r *ReadinessRegistry, store *sqlite.Store) {
	if r == nil || store == nil {
		return
	}
	status := store.RSPFirehoseReadiness()
	if !status.Running {
		current := r.Get(loopNameFirehose)
		if current != nil {
			switch current.State {
			case LoopRunning, LoopDegraded, LoopRecovering:
				r.SetState(loopNameFirehose, LoopStopped)
				return
			}
		}
		r.SetState(loopNameFirehose, LoopNotStarted)
		return
	}
	r.SetDroppedEvents(loopNameFirehose, status.DroppedEvents)
	if status.DroppedEvents > 0 {
		// Already set to degraded by SetDroppedEvents.
		return
	}
	r.SetState(loopNameFirehose, LoopRunning)
}
