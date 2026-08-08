package main

import (
	"context"
	"encoding/json"
	"strings"
)

func (r *Runtime) handleTaskProjectFieldsRuntimeEvent(ctx context.Context, evt RhizomeEvent) error {
	if r == nil || r.runtimePaused() {
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(evt.Type), "task.project_fields.updated") {
		return nil
	}
	if evt.WorkspaceID != "" && r.cfg.WorkspaceID != "" && evt.WorkspaceID != r.cfg.WorkspaceID {
		return nil
	}

	taskID := taskProjectFieldsEventTaskID(evt)
	if taskID == "" {
		r.invalidateBootstrap()
		r.wakePlanner()
		return nil
	}

	sessionID, shouldRefreshActive := r.invalidateActiveTaskProjectFieldsContext(taskID)
	r.invalidateBootstrap()
	if !shouldRefreshActive {
		r.wakePlanner()
		return nil
	}
	return r.setPendingWorkTrigger(ctx, "task_project_fields_updated", taskID, sessionID)
}

func taskProjectFieldsEventTaskID(evt RhizomeEvent) string {
	var payload map[string]any
	if raw := strings.TrimSpace(evt.PayloadJSON); raw != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	for _, field := range []string{"task_id", "taskID", "requested_task_id", "requestedTaskID"} {
		if value, ok := payload[field].(string); ok {
			if value = strings.TrimSpace(value); value != "" {
				return value
			}
		}
	}
	return ""
}

func (r *Runtime) invalidateActiveTaskProjectFieldsContext(taskID string) (string, bool) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return "", false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	matched := false
	sessionID := ""
	if r.activeTask != nil && strings.TrimSpace(r.activeTask.TaskID) == taskID {
		matched = true
	}
	if r.activeSession != nil && strings.TrimSpace(r.activeSession.TaskID) == taskID {
		matched = true
		sessionID = strings.TrimSpace(r.activeSession.SessionID)
	}
	if strings.TrimSpace(r.scratch.ActiveTaskID) == taskID {
		matched = true
		sessionID = firstNonEmpty(sessionID, strings.TrimSpace(r.scratch.ActiveSessionID))
	}
	if r.activeHydration != nil && hydrationTaskID(r.activeHydration) == taskID {
		matched = true
	}
	if !matched {
		return "", false
	}

	r.clearHydrationLocked()
	r.activeWorkPacket = nil
	r.clearContinuationHoldLocked()
	r.invalidateFocusLocked()
	return sessionID, true
}
