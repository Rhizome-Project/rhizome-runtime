package agent

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// LoopStatus represents the lifecycle state of a supervised agent loop.
type LoopStatus string

const (
	StatusStarting LoopStatus = "STARTING"
	StatusRunning  LoopStatus = "RUNNING"
	StatusCrashed  LoopStatus = "CRASHED"
	StatusStopped  LoopStatus = "STOPPED"
	StatusUnknown  LoopStatus = "UNKNOWN"
)

// LifecycleObserver is a callback for emitting state transitions.
type LifecycleObserver func(agentID string, status LoopStatus, err error)

// SupervisedLoop tracks the runtime evidence of an agent execution.
type SupervisedLoop struct {
	AgentID   string
	Status    LoopStatus
	StartedAt time.Time
	LastError error
	cancel    context.CancelFunc
}

// RuntimeSupervisor manages the lifecycles of long-running canonical agent activities without relying on orphaned background contexts.
type RuntimeSupervisor struct {
	mu       sync.RWMutex
	loops    map[string]*SupervisedLoop
	observer LifecycleObserver
}

// NewRuntimeSupervisor creates a new lifecycle manager.
func NewRuntimeSupervisor(observer LifecycleObserver) *RuntimeSupervisor {
	return &RuntimeSupervisor{
		loops:    make(map[string]*SupervisedLoop),
		observer: observer,
	}
}

// Start spawns the agent execution under bounded supervision.
func (s *RuntimeSupervisor) Start(ctx context.Context, agent Agent, task string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := agent.ID()
	if existing, exists := s.loops[id]; exists && (existing.Status == StatusStarting || existing.Status == StatusRunning) {
		return fmt.Errorf("agent %s is already running", id)
	}

	execCtx, cancel := context.WithCancel(ctx)
	loop := &SupervisedLoop{
		AgentID:   id,
		Status:    StatusStarting,
		StartedAt: time.Now().UTC(),
		cancel:    cancel,
	}
	s.loops[id] = loop
	s.unsafeEmit(id, StatusStarting, nil)

	go s.runAsync(execCtx, agent, task, loop)
	return nil
}

func (s *RuntimeSupervisor) runAsync(ctx context.Context, agent Agent, task string, loop *SupervisedLoop) {
	s.setStatus(loop.AgentID, StatusRunning, nil)

	defer func() {
		// Native panic boundary containment (P3C-003)
		if r := recover(); r != nil {
			err := fmt.Errorf("agent panicked: %v", r)
			s.setStatus(loop.AgentID, StatusCrashed, err)
		}
	}()

	_, err := agent.Run(ctx, task)

	s.mu.Lock()
	defer s.mu.Unlock()

	l, exists := s.loops[loop.AgentID]
	if !exists {
		return
	}

	if ctx.Err() != nil {
		l.Status = StatusStopped
		l.LastError = nil
	} else if err != nil {
		l.Status = StatusCrashed
		l.LastError = err
	} else {
		l.Status = StatusStopped
		l.LastError = nil
	}

	s.unsafeEmit(l.AgentID, l.Status, l.LastError)
}

// Stop cleanly triggers cancellation for an active agent loop without hard-killing unhandled processes.
func (s *RuntimeSupervisor) Stop(agentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	loop, exists := s.loops[agentID]
	if !exists {
		return fmt.Errorf("agent %s not found under supervision", agentID)
	}

	if loop.Status == StatusStopped || loop.Status == StatusCrashed {
		return nil
	}

	if loop.cancel != nil {
		loop.cancel()
		s.unsafeEmit(agentID, StatusStopping, nil)
	}
	return nil
}

const StatusStopping LoopStatus = "STOPPING"

// Health returns the active phase of an agent's run loop.
func (s *RuntimeSupervisor) Health(agentID string) LoopStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	loop, exists := s.loops[agentID]
	if !exists {
		return StatusUnknown
	}
	return loop.Status
}

// Error exposes the evidence trace if the loop crashed.
func (s *RuntimeSupervisor) Error(agentID string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if loop, ok := s.loops[agentID]; ok {
		return loop.LastError
	}
	return nil
}

func (s *RuntimeSupervisor) setStatus(agentID string, status LoopStatus, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if loop, ok := s.loops[agentID]; ok {
		loop.Status = status
		if err != nil {
			loop.LastError = err
		}
		s.unsafeEmit(agentID, status, err)
	}
}

// unsafeEmit must be called while a lock is held, invoking the thread-safe observer cleanly
func (s *RuntimeSupervisor) unsafeEmit(agentID string, status LoopStatus, err error) {
	if s.observer != nil {
		s.observer(agentID, status, err)
	}
}
