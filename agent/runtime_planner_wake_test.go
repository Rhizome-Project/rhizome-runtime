package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFinishPlannerTickReplaysPendingTrigger(t *testing.T) {
	r := &Runtime{
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			PendingTrigger:        "request_resume",
			PendingTriggerTask:    "task-1",
			PendingTriggerSession: "session-1",
		},
		busy: true,
	}

	r.finishPlannerTick()

	r.mu.Lock()
	busy := r.busy
	r.mu.Unlock()
	if busy {
		t.Fatal("expected planner tick finish to clear busy flag")
	}
	select {
	case <-r.eventWakePlanner:
	default:
		t.Fatal("expected pending trigger to replay planner wake")
	}
}

func TestFinishPlannerTickDoesNotWakeWithoutPendingTrigger(t *testing.T) {
	r := &Runtime{
		eventWakePlanner: make(chan struct{}, 1),
		busy:             true,
	}

	r.finishPlannerTick()

	select {
	case <-r.eventWakePlanner:
		t.Fatal("unexpected planner wake without pending trigger")
	default:
	}
}

func TestRuntimePlannerCycleTimeoutDefaultsFinite(t *testing.T) {
	cfg := RuntimeConfig{}
	cfg.ApplyDefaults()

	if cfg.PlannerCycleTimeout <= 0 {
		t.Fatalf("expected finite default planner cycle timeout, got %s", cfg.PlannerCycleTimeout)
	}
	if got := runtimePlannerCycleTimeout(cfg); got != cfg.PlannerCycleTimeout {
		t.Fatalf("runtimePlannerCycleTimeout() = %s, want configured %s", got, cfg.PlannerCycleTimeout)
	}

	cfg.PlannerCycleTimeout = 25 * time.Millisecond
	if got := runtimePlannerCycleTimeout(cfg); got != 25*time.Millisecond {
		t.Fatalf("runtimePlannerCycleTimeout() explicit override = %s", got)
	}
}

func TestPlannerTickTimesOutSlowWorkNextAndClearsBusy(t *testing.T) {
	called := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method == "workspace.tension.refresh" {
			writeRPCResult(w, req, map[string]any{"refreshed": 0})
			return
		}
		if req.Method != "agent.work.next" {
			t.Fatalf("unexpected method during slow planner cycle: %s", req.Method)
		}
		called <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:                RuntimeModeDaemon,
		Workdir:             t.TempDir(),
		RhizomeRPC:          server.URL,
		RhizomeToken:        "token",
		WorkspaceID:         "ws-1",
		AgentID:             "agent-1",
		OwnerUserID:         "owner-1",
		PlannerCycleTimeout: 10 * time.Millisecond,
	}, nil)
	runtime.lastBootstrap = time.Now()
	t.Cleanup(func() { _ = runtime.Close() })

	err := runtime.plannerTick(context.Background())
	if !errors.Is(err, errRuntimePlannerWorkCycleTimeout) {
		t.Fatalf("plannerTick() error = %v, want %v", err, errRuntimePlannerWorkCycleTimeout)
	}
	if !strings.Contains(err.Error(), "ensure runnable task") {
		t.Fatalf("planner timeout error should name phase, got %v", err)
	}
	select {
	case <-called:
	default:
		t.Fatal("expected planner tick to call agent.work.next")
	}

	runtime.mu.Lock()
	busy := runtime.busy
	runtime.mu.Unlock()
	if busy {
		t.Fatal("planner timeout left runtime busy")
	}
}
