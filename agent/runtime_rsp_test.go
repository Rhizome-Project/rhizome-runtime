package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRuntimeConfigDefaultsRSPRolloutPhase(t *testing.T) {
	cfg := RuntimeConfig{Workdir: t.TempDir()}
	cfg.ApplyDefaults()

	if cfg.RSPRolloutPhase != string(RSPRolloutObserveOnly) {
		t.Fatalf("expected default rollout phase observe_only, got %q", cfg.RSPRolloutPhase)
	}
}

func TestDeriveRuntimeRSPStateObserveOnlySuppressesIntervention(t *testing.T) {
	cfg := RuntimeConfig{RSPRolloutPhase: string(RSPRolloutObserveOnly)}
	cfg.ApplyDefaults()
	cfg.RSPRolloutPhase = string(RSPRolloutObserveOnly)

	snapshot := RuntimeWatchdogSnapshot{
		MonitorVerdict:    "stalled",
		Reason:            "active task made no durable progress",
		RecommendedAction: "summarize_and_replan",
		CurrentTaskID:     "task-1",
	}
	state := deriveRuntimeRSPState(cfg, snapshot, time.Date(2026, 3, 26, 8, 30, 0, 0, time.UTC))

	if state.RolloutPhase != RSPRolloutObserveOnly {
		t.Fatalf("expected observe_only phase, got %+v", state)
	}
	if state.LiveActuationAllowed {
		t.Fatalf("expected observe_only phase to forbid live actuation, got %+v", state)
	}
	if !state.AdvisoryOnly {
		t.Fatalf("expected observe_only phase to remain advisory-only, got %+v", state)
	}
	if state.hasIntervention() {
		t.Fatalf("expected observe_only phase to suppress intervention, got %+v", state)
	}
	if state.intervention() != nil {
		t.Fatalf("expected observe_only phase to suppress intervention payload, got %+v", state)
	}
	if !strings.Contains(state.advisorySignal(), "phase=observe_only") {
		t.Fatalf("expected advisory signal to advertise phase, got %q", state.advisorySignal())
	}
}

func TestDeriveRuntimeRSPStateLiveAllowsIntervention(t *testing.T) {
	cfg := RuntimeConfig{RSPRolloutPhase: string(RSPRolloutLive)}
	cfg.ApplyDefaults()
	cfg.RSPRolloutPhase = string(RSPRolloutLive)

	snapshot := RuntimeWatchdogSnapshot{
		MonitorVerdict:    "degraded",
		Reason:            "listener degraded",
		RecommendedAction: "refresh_bootstrap",
		ListenerState:     "degraded",
	}
	state := deriveRuntimeRSPState(cfg, snapshot, time.Date(2026, 3, 26, 8, 35, 0, 0, time.UTC))

	if state.RolloutPhase != RSPRolloutLive {
		t.Fatalf("expected live phase, got %+v", state)
	}
	if !state.LiveActuationAllowed {
		t.Fatalf("expected live phase to allow actuation, got %+v", state)
	}
	if state.InterventionLevel != "FORCE_REPLAN" {
		t.Fatalf("expected force replan intervention, got %+v", state)
	}
	if got := state.intervention(); got == nil || got.Level != "FORCE_REPLAN" {
		t.Fatalf("expected intervention payload, got %+v", got)
	}
}

func TestShadowMonitorObserveOnlyDoesNotEscalate(t *testing.T) {
	cfg := RuntimeConfig{Workdir: t.TempDir(), RSPRolloutPhase: string(RSPRolloutObserveOnly)}
	cfg.ApplyDefaults()
	cfg.RSPRolloutPhase = string(RSPRolloutObserveOnly)

	monitor := &ShadowMonitor{cfg: cfg}
	snapshot := RuntimeWatchdogSnapshot{
		MonitorVerdict:    "stalled",
		Reason:            "active task made no durable progress",
		RecommendedAction: "summarize_and_replan",
		RSP:               deriveRuntimeRSPState(cfg, RuntimeWatchdogSnapshot{MonitorVerdict: "stalled", Reason: "active task made no durable progress", RecommendedAction: "summarize_and_replan"}, time.Date(2026, 3, 26, 8, 40, 0, 0, time.UTC)),
	}

	if err := monitor.handleSnapshot(context.Background(), snapshot, time.Date(2026, 3, 26, 8, 40, 0, 0, time.UTC)); err != nil {
		t.Fatalf("handleSnapshot() error = %v", err)
	}
	if monitor.consecutiveInterventions != 0 {
		t.Fatalf("expected observe_only to suppress interventions, got %d", monitor.consecutiveInterventions)
	}
}
