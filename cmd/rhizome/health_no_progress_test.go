package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestDiagnosticsPayload_ExposesDurableNoProgressHealth(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := createValidMetricsFixture(metricsFile); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	noProgressHealth := &sqlite.ExecutionNoProgressSnapshot{
		State:                    "blocked",
		Message:                  "durable no-progress detected: run=run-123, checkpoint=checkpoint-1",
		ReferenceAt:              "2026-04-21T12:00:00Z",
		NoProgressRunCount:       1,
		TriggeredWorkspaceID:     "ws-no-progress",
		TriggeredRunID:           "run-123",
		TriggeredStepID:          "step-abc",
		TriggeredStepPhase:       "EXECUTE",
		TriggeredStepStatus:      "ACTIVE",
		TriggeredConsecutiveRuns: 3,
		RecommendedAction:        "needs_operator",
	}

	payload := collectServiceHealthPayloadWithAuthorityAndReviewerScarcityHealthAndStuckAgentHealthAndNoProgressHealth(
		app.Config{MetricsPath: metricsFile},
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "ok", Message: "operator queue lag healthy"},
		DiagnosticSignal{State: "ok", Message: "reviewer scarcity healthy"},
		sqlite.ReviewerScarcityHealthSnapshot{},
		DiagnosticSignal{State: "ok", Message: "stuck agent health is stable"},
		nil,
		DiagnosticSignal{State: "blocked", Message: noProgressHealth.Message},
		noProgressHealth,
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy"},
	)

	if payload.NoProgressHealth == nil {
		t.Fatal("expected no-progress health payload to be exposed")
	}
	if payload.NoProgressHealth.TriggeredRunID != "run-123" {
		t.Fatalf("unexpected no-progress health payload: %+v", payload.NoProgressHealth)
	}
	if payload.Extended.NoProgress.State != "blocked" {
		t.Fatalf("expected extended no-progress signal to be blocked, got %+v", payload.Extended.NoProgress)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected no-progress watchdog to degrade top-level status, got %+v", payload)
	}
	if payload.Semantics.Degraded.State != "degraded" {
		t.Fatalf("expected top-level degraded semantics to reflect no-progress, got %+v", payload.Semantics.Degraded)
	}
}

func TestDoctor_ExtendedReadinessCheck_FailsOnBlockedNoProgress(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "ok", "message": "firehose running"},
				"invalidation_lag":   map[string]any{"state": "ok", "message": "index present"},
				"operator_queue_lag": map[string]any{"state": "ok", "message": "operator queue lag healthy"},
				"reviewer_scarcity":  map[string]any{"state": "ok", "message": "reviewer scarcity healthy"},
				"stuck_agents":       map[string]any{"state": "ok", "message": "stuck agent health is stable"},
				"no_progress":        map[string]any{"state": "blocked", "message": "durable no-progress detected"},
				"projection_lag":     map[string]any{"state": "ok", "message": "projection lag healthy"},
				"replay_health":      map[string]any{"state": "ok", "message": "replay view present"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusFail {
		t.Fatalf("expected blocked no-progress to fail doctor extended readiness, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(strings.ToLower(check.Message), "no_progress: blocked") {
		t.Fatalf("expected no-progress failure message, got %s", check.Message)
	}
}

func createValidMetricsFixture(path string) error {
	return os.WriteFile(path, []byte(`{"schema_version":"1.0","timestamp":"2026-04-12T10:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}`), 0o644)
}
