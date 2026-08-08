package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTaskProjectFieldsUpdatedQueuesActiveTaskRefresh(t *testing.T) {
	var saved RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if got := rpcString(req.Params, "key"); got != runtimeScratchStateKey {
				t.Fatalf("agent.state.set key = %q, want %q", got, runtimeScratchStateKey)
			}
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode saved scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during project fields wake: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-alpha",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs:                   map[string]string{},
			ActiveTaskID:              "task-gated",
			ActiveSessionID:           "session-alpha",
			ContinuationHoldTaskID:    "task-gated",
			ContinuationHoldSessionID: "session-alpha",
			ContinuationHoldRunID:     "run-alpha",
			ContinuationHoldUntil:     time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:   "task-gated",
			Status:   "RUNNING",
			TaskKind: "EXECUTION",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-alpha",
			TaskID:    "task-gated",
			AgentID:   "agent-alpha",
			Status:    "ACTIVE",
		},
		activeHydration:  &TaskHydrationBundle{Task: TaskStatus{TaskID: "task-gated"}},
		activeWorkPacket: &AgentWorkPacket{WorkType: "resume_session"},
		lastBootstrap:    time.Now().UTC(),
	}

	payload, err := json.Marshal(map[string]any{
		"task_id":                         "task-gated",
		"requested_requires_project_gate": true,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := runtime.handleTaskProjectFieldsRuntimeEvent(context.Background(), RhizomeEvent{
		Type:        "task.project_fields.updated",
		WorkspaceID: "ws-1",
		PayloadJSON: string(payload),
	}); err != nil {
		t.Fatalf("handleTaskProjectFieldsRuntimeEvent() error = %v", err)
	}

	if saved.PendingTrigger != "task_project_fields_updated" || saved.PendingTriggerTask != "task-gated" || saved.PendingTriggerSession != "session-alpha" {
		t.Fatalf("unexpected saved pending trigger: %+v", saved)
	}
	if saved.ContinuationHoldTaskID != "" || saved.ContinuationHoldSessionID != "" || saved.ContinuationHoldRunID != "" || saved.ContinuationHoldUntil != "" {
		t.Fatalf("project fields wake should clear continuation hold, got %+v", saved)
	}
	trigger := runtime.currentPendingWorkTrigger()
	if trigger.Trigger != "task_project_fields_updated" || trigger.TaskID != "task-gated" || trigger.SessionID != "session-alpha" {
		t.Fatalf("unexpected runtime pending trigger: %+v", trigger)
	}

	runtime.mu.Lock()
	hydration := runtime.activeHydration
	packet := runtime.activeWorkPacket
	lastBootstrap := runtime.lastBootstrap
	runtime.mu.Unlock()
	if hydration != nil || packet != nil {
		t.Fatalf("expected stale hydration and packet to be cleared, hydration=%+v packet=%+v", hydration, packet)
	}
	if !lastBootstrap.IsZero() {
		t.Fatalf("expected bootstrap cache invalidation, got %s", lastBootstrap.Format(time.RFC3339Nano))
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("expected task project fields wake to notify planner")
	}
}
