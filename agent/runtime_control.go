package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type runtimeControlRequestPayload struct {
	Reason                string `json:"reason,omitempty"`
	TaskID                string `json:"task_id,omitempty"`
	SessionID             string `json:"session_id,omitempty"`
	TensionID             string `json:"tension_id,omitempty"`
	TensionRole           string `json:"role,omitempty"`
	TensionLifecycleState string `json:"lifecycle_state,omitempty"`
	Action                string `json:"action,omitempty"`
	Resume                bool   `json:"resume,omitempty"`
}

type RuntimeControlSnapshot struct {
	Paused              bool   `json:"paused"`
	Mode                string `json:"mode,omitempty"`
	LastAction          string `json:"last_action,omitempty"`
	LastActionReason    string `json:"last_action_reason,omitempty"`
	LastActionAt        string `json:"last_action_at,omitempty"`
	TargetTaskID        string `json:"target_task_id,omitempty"`
	TargetSessionID     string `json:"target_session_id,omitempty"`
	TargetTensionID     string `json:"target_tension_id,omitempty"`
	TargetTensionRole   string `json:"target_tension_role,omitempty"`
	TargetTensionState  string `json:"target_tension_state,omitempty"`
	TargetTensionAction string `json:"target_tension_action,omitempty"`
	Attachable          bool   `json:"attachable"`
}

type RuntimeTensionMutationSnapshot struct {
	Surface        string `json:"surface"`
	TensionID      string `json:"tension_id"`
	AgentID        string `json:"agent_id"`
	CoalitionID    string `json:"coalition_id,omitempty"`
	Changed        bool   `json:"changed"`
	RuntimeEventID string `json:"runtime_event_id,omitempty"`
	EventType      string `json:"event_type,omitempty"`
}

func (r *Runtime) runtimeAttachable() bool {
	if r == nil {
		return false
	}
	r.runMu.Lock()
	defer r.runMu.Unlock()
	return !r.closed
}

func (r *Runtime) runtimePaused() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.scratch.ControlPaused
}

func (r *Runtime) runtimeControlSnapshot(now time.Time) RuntimeControlSnapshot {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	r.mu.Lock()
	state := r.scratch
	r.mu.Unlock()

	mode := strings.TrimSpace(state.ControlMode)
	if mode == "" {
		if state.ControlPaused {
			mode = "paused"
		} else {
			mode = "live"
		}
	}

	return RuntimeControlSnapshot{
		Paused:              state.ControlPaused,
		Mode:                mode,
		LastAction:          strings.TrimSpace(state.ControlAction),
		LastActionReason:    strings.TrimSpace(state.ControlActionReason),
		LastActionAt:        strings.TrimSpace(state.ControlActionAt),
		TargetTaskID:        strings.TrimSpace(state.ControlTargetTaskID),
		TargetSessionID:     strings.TrimSpace(state.ControlTargetSessionID),
		TargetTensionID:     strings.TrimSpace(state.ControlTargetTensionID),
		TargetTensionRole:   strings.TrimSpace(state.ControlTensionRole),
		TargetTensionState:  strings.TrimSpace(state.ControlTensionState),
		TargetTensionAction: strings.TrimSpace(state.ControlTensionAction),
		Attachable:          r.runtimeAttachable(),
	}
}

func (r *Runtime) pauseRuntime(ctx context.Context, payload runtimeControlRequestPayload) (RuntimeControlSnapshot, error) {
	now := time.Now().UTC()
	reason := firstNonEmpty(payload.Reason, "operator pause")
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		taskID := strings.TrimSpace(payload.TaskID)
		if taskID == "" {
			taskID = strings.TrimSpace(state.ControlTargetTaskID)
		}
		sessionID := strings.TrimSpace(payload.SessionID)
		if sessionID == "" {
			sessionID = strings.TrimSpace(state.ControlTargetSessionID)
		}
		tensionID := strings.TrimSpace(payload.TensionID)
		if tensionID == "" {
			tensionID = strings.TrimSpace(state.ControlTargetTensionID)
		}
		state.ControlPaused = true
		state.ControlMode = "paused"
		state.ControlAction = "pause"
		state.ControlActionReason = reason
		state.ControlActionAt = now.Format(time.RFC3339Nano)
		state.ControlTargetTaskID = taskID
		state.ControlTargetSessionID = sessionID
		state.ControlTargetTensionID = tensionID
		if role := strings.TrimSpace(payload.TensionRole); role != "" {
			state.ControlTensionRole = role
		}
		if lifecycle := strings.TrimSpace(payload.TensionLifecycleState); lifecycle != "" {
			state.ControlTensionState = lifecycle
		}
	}); err != nil {
		return RuntimeControlSnapshot{}, err
	}
	r.invalidateFocus()
	return r.runtimeControlSnapshot(now), nil
}

func (r *Runtime) resumeRuntime(ctx context.Context, payload runtimeControlRequestPayload) (RuntimeControlSnapshot, error) {
	now := time.Now().UTC()
	reason := firstNonEmpty(payload.Reason, "runtime.resume")
	taskID := strings.TrimSpace(payload.TaskID)
	sessionID := strings.TrimSpace(payload.SessionID)

	r.mu.Lock()
	if taskID == "" {
		taskID = strings.TrimSpace(r.scratch.ControlTargetTaskID)
	}
	if sessionID == "" {
		sessionID = strings.TrimSpace(r.scratch.ControlTargetSessionID)
	}
	if taskID == "" && r.activeSession != nil {
		taskID = strings.TrimSpace(r.activeSession.TaskID)
	}
	if taskID == "" && r.activeTask != nil {
		taskID = strings.TrimSpace(r.activeTask.TaskID)
	}
	if sessionID == "" && r.activeSession != nil {
		sessionID = strings.TrimSpace(r.activeSession.SessionID)
	}
	r.mu.Unlock()

	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.ControlPaused = false
		state.ControlMode = "live"
		state.ControlAction = "resume"
		state.ControlActionReason = reason
		state.ControlActionAt = now.Format(time.RFC3339Nano)
		if taskID != "" {
			state.ControlTargetTaskID = taskID
		}
		if sessionID != "" {
			state.ControlTargetSessionID = sessionID
		}
	}); err != nil {
		return RuntimeControlSnapshot{}, err
	}

	if taskID != "" || sessionID != "" {
		if err := r.setPendingWorkTrigger(ctx, "runtime_resume", taskID, sessionID); err != nil {
			return RuntimeControlSnapshot{}, err
		}
	}
	r.markBootstrapStale()
	r.invalidateFocus()
	return r.runtimeControlSnapshot(now), nil
}

func (r *Runtime) switchTaskRuntime(ctx context.Context, payload runtimeControlRequestPayload) (RuntimeControlSnapshot, error) {
	taskID := strings.TrimSpace(payload.TaskID)
	if taskID == "" {
		return RuntimeControlSnapshot{}, fmt.Errorf("task_id is required")
	}
	sessionID := strings.TrimSpace(payload.SessionID)
	reason := firstNonEmpty(payload.Reason, "runtime.switch_task")

	r.mu.Lock()
	snapshotTasks := append([]WorkspaceTaskRecord(nil), r.bootstrap.Snapshot.Tasks...)
	snapshotSessions := append([]AgentSessionStateRecord(nil), r.bootstrap.Snapshot.Sessions...)
	currentTask := r.activeTask
	currentSession := r.activeSession
	r.mu.Unlock()

	if _, ok := findBootstrapTask(snapshotTasks, taskID); !ok {
		if currentTask == nil || strings.TrimSpace(currentTask.TaskID) != taskID {
			return RuntimeControlSnapshot{}, fmt.Errorf("task %q not found in current bootstrap snapshot", taskID)
		}
	}
	if sessionID != "" && !findBootstrapSession(snapshotSessions, sessionID, r.cfg.AgentID) {
		if currentSession == nil || strings.TrimSpace(currentSession.SessionID) != sessionID {
			return RuntimeControlSnapshot{}, fmt.Errorf("session %q not found in current bootstrap snapshot", sessionID)
		}
	}

	now := time.Now().UTC()
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.ControlMode = "task"
		state.ControlAction = "switch_task"
		state.ControlActionReason = reason
		state.ControlActionAt = now.Format(time.RFC3339Nano)
		state.ControlTargetTaskID = taskID
		state.ControlTargetSessionID = sessionID
	}); err != nil {
		return RuntimeControlSnapshot{}, err
	}

	if err := r.setPendingWorkTrigger(ctx, "runtime_switch_task", taskID, sessionID); err != nil {
		return RuntimeControlSnapshot{}, err
	}
	r.markBootstrapStale()
	r.invalidateFocus()
	return r.runtimeControlSnapshot(now), nil
}

func (r *Runtime) switchTensionRuntime(ctx context.Context, payload runtimeControlRequestPayload) (RuntimeControlSnapshot, *RuntimeTensionMutationSnapshot, error) {
	tensionID := strings.TrimSpace(payload.TensionID)
	if tensionID == "" {
		return RuntimeControlSnapshot{}, nil, fmt.Errorf("tension_id is required")
	}
	action := strings.ToLower(strings.TrimSpace(payload.Action))
	if action == "" {
		action = "attach"
	}
	role := firstNonEmpty(payload.TensionRole, r.cfg.Role)
	lifecycle := strings.TrimSpace(payload.TensionLifecycleState)
	reason := firstNonEmpty(payload.Reason, "runtime.switch_tension")
	now := time.Now().UTC()

	var err error
	var mutation *RuntimeTensionMutationSnapshot
	switch action {
	case "attach", "focus":
		var result TensionAgentMutationResult
		result, err = r.client.AttachTensionAgent(ctx, TensionAgentAttachInput{
			WorkspaceID:      r.cfg.WorkspaceID,
			TensionID:        tensionID,
			AgentID:          r.cfg.AgentID,
			ActorID:          r.cfg.AgentID,
			SuccessCriterion: "Attached as: " + strings.TrimSpace(role),
			Reason:           reason,
		})
		if err == nil {
			mutation = runtimeTensionMutationSnapshot("workspace.tension.agent.attach", tensionID, r.cfg.AgentID, "", result)
		}
		if err == nil && lifecycle != "" {
			err = r.client.UpdateTensionLifecycle(ctx, TensionLifecycleUpdateInput{
				WorkspaceID:    r.cfg.WorkspaceID,
				TensionID:      tensionID,
				LifecycleState: lifecycle,
				UpdatedBy:      r.cfg.AgentID,
				Reason:         reason,
			})
		}
	case "detach", "release":
		var coalition WorkspaceCoalitionRecord
		coalition, err = r.client.ResolveTensionAgentCoalition(ctx, r.cfg.WorkspaceID, tensionID, r.cfg.AgentID)
		if err != nil {
			break
		}
		var result TensionAgentMutationResult
		result, err = r.client.DetachTensionAgent(ctx, TensionAgentDetachInput{
			WorkspaceID: r.cfg.WorkspaceID,
			CoalitionID: coalition.CoalitionID,
			AgentID:     r.cfg.AgentID,
			ActorID:     r.cfg.AgentID,
			Reason:      reason,
		})
		if err == nil {
			mutation = runtimeTensionMutationSnapshot("workspace.tension.agent.detach", tensionID, r.cfg.AgentID, coalition.CoalitionID, result)
		}
	case "lifecycle", "update":
		if lifecycle == "" {
			lifecycle = "ACTIVE"
		}
		err = r.client.UpdateTensionLifecycle(ctx, TensionLifecycleUpdateInput{
			WorkspaceID:    r.cfg.WorkspaceID,
			TensionID:      tensionID,
			LifecycleState: lifecycle,
			UpdatedBy:      r.cfg.AgentID,
			Reason:         reason,
		})
	default:
		return RuntimeControlSnapshot{}, nil, fmt.Errorf("unsupported tension action %q", action)
	}
	if err != nil {
		return RuntimeControlSnapshot{}, mutation, err
	}

	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.ControlMode = "tension"
		state.ControlAction = "switch_tension"
		state.ControlActionReason = reason
		state.ControlActionAt = now.Format(time.RFC3339Nano)
		state.ControlTargetTaskID = strings.TrimSpace(payload.TaskID)
		state.ControlTargetSessionID = strings.TrimSpace(payload.SessionID)
		state.ControlTargetTensionID = tensionID
		state.ControlTensionAction = action
		state.ControlTensionRole = role
		state.ControlTensionState = lifecycle
	}); err != nil {
		return RuntimeControlSnapshot{}, mutation, err
	}

	r.invalidateFocus()
	r.markBootstrapStale()
	return r.runtimeControlSnapshot(now), mutation, nil
}

func runtimeTensionMutationSnapshot(surface, tensionID, agentID, fallbackCoalitionID string, result TensionAgentMutationResult) *RuntimeTensionMutationSnapshot {
	return &RuntimeTensionMutationSnapshot{
		Surface:        strings.TrimSpace(surface),
		TensionID:      strings.TrimSpace(tensionID),
		AgentID:        strings.TrimSpace(agentID),
		CoalitionID:    firstNonEmpty(result.CoalitionID, result.Coalition.CoalitionID, fallbackCoalitionID),
		Changed:        result.Changed,
		RuntimeEventID: strings.TrimSpace(result.Event.EventID),
		EventType:      strings.TrimSpace(result.Event.EventType),
	}
}
