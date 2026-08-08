package main

import (
	"testing"
	"time"
)

func TestRuntimeMergeScratchPreservesNewerLocalPendingTrigger(t *testing.T) {
	now := time.Now().UTC()
	runtime := &Runtime{
		scratch: RuntimeScratchState{
			PendingTrigger:        "runtime_switch_task",
			PendingTriggerTask:    "task-delegated",
			PendingTriggerSession: "session-delegated",
			PendingTriggerAt:      now.Format(time.RFC3339Nano),
		},
	}
	stale := RuntimeScratchState{
		LastWakeReason:   "ambient_reflection",
		PendingTriggerAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
	}

	merged := runtime.mergeCurrentPendingTriggerIntoScratchState(stale)

	if merged.PendingTrigger != "runtime_switch_task" || merged.PendingTriggerTask != "task-delegated" || merged.PendingTriggerSession != "session-delegated" {
		t.Fatalf("expected local pending trigger to survive stale scratch save, got trigger=%q task=%q session=%q", merged.PendingTrigger, merged.PendingTriggerTask, merged.PendingTriggerSession)
	}
	if merged.LastWakeReason != "ambient_reflection" {
		t.Fatalf("expected unrelated scratch fields to survive merge, got %q", merged.LastWakeReason)
	}
}

func TestRuntimeMergeScratchDoesNotResurrectClearedPendingTrigger(t *testing.T) {
	runtime := &Runtime{scratch: RuntimeScratchState{}}
	state := RuntimeScratchState{
		LastWakeReason: "trigger_no_work",
	}

	merged := runtime.mergeCurrentPendingTriggerIntoScratchState(state)

	if merged.PendingTrigger != "" || merged.PendingTriggerTask != "" || merged.PendingTriggerSession != "" || merged.PendingTriggerAt != "" {
		t.Fatalf("expected cleared pending trigger to stay cleared, got %+v", merged)
	}
}

func TestRuntimeMergeScratchDoesNotResurrectMaterializedDelegatedSwitch(t *testing.T) {
	runtime := &Runtime{
		scratch: RuntimeScratchState{
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-delegated",
		},
	}
	state := RuntimeScratchState{
		ActiveTaskID:    "task-delegated",
		ActiveSessionID: "session-delegated",
		LastWakeTrigger: "runtime_switch_task",
	}

	merged := runtime.mergeCurrentPendingTriggerIntoScratchState(state)

	if merged.PendingTrigger != "" || merged.PendingTriggerTask != "" || merged.PendingTriggerSession != "" || merged.PendingTriggerAt != "" {
		t.Fatalf("materialized delegated switch must not resurrect local pending trigger, got %+v", merged)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("materialized delegated switch should clear local pending trigger too, got %+v", got)
	}
}

func TestRuntimeMergeScratchKeepsNewerOutgoingPendingTrigger(t *testing.T) {
	now := time.Now().UTC()
	runtime := &Runtime{
		scratch: RuntimeScratchState{
			PendingTrigger:     "request_resume",
			PendingTriggerTask: "task-old",
			PendingTriggerAt:   now.Add(-time.Minute).Format(time.RFC3339Nano),
		},
	}
	state := RuntimeScratchState{
		PendingTrigger:     "runtime_switch_task",
		PendingTriggerTask: "task-new",
		PendingTriggerAt:   now.Format(time.RFC3339Nano),
	}

	merged := runtime.mergeCurrentPendingTriggerIntoScratchState(state)

	if merged.PendingTrigger != "runtime_switch_task" || merged.PendingTriggerTask != "task-new" {
		t.Fatalf("expected newer outgoing pending trigger to win, got trigger=%q task=%q", merged.PendingTrigger, merged.PendingTriggerTask)
	}
}
