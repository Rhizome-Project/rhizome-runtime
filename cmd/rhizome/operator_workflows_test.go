package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAuditExport_JSONFilter(t *testing.T) {
	dbPath, workspaceRoot := setupFakeBridgeEnv(t)
	_ = workspaceRoot

	createCLITestWorkspace(t, "ws-audit-export")
	if err := runTaskSubmit([]string{
		"--task-id", "task-audit-export",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-audit-export",
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.AddAuditEvent(ctx, sqlite.AuditEventInput{
		EventID:     "audit-1",
		EventType:   "node_execution_started",
		EntityType:  "node",
		EntityID:    "task-audit-export/node-1",
		ActorID:     "daemon",
		PayloadJSON: `{"trace_id":"tr-1"}`,
	}); err != nil {
		t.Fatalf("add audit event 1: %v", err)
	}
	if err := store.AddAuditEvent(ctx, sqlite.AuditEventInput{
		EventID:     "audit-2",
		EventType:   "node_execution_failed",
		EntityType:  "node",
		EntityID:    "task-audit-export/node-1",
		ActorID:     "daemon",
		PayloadJSON: `{"trace_id":"tr-2"}`,
	}); err != nil {
		t.Fatalf("add audit event 2: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runAuditExport([]string{
			"--event-type", "node_execution_failed",
			"--limit", "10",
			"--format", "json",
		})
	})
	if err != nil {
		t.Fatalf("runAuditExport failed: %v", err)
	}

	var payload struct {
		Count   int `json:"count"`
		Entries []struct {
			EventID   string `json:"event_id"`
			EventType string `json:"event_type"`
			EntityID  string `json:"entity_id"`
			ActorID   string `json:"actor_id"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode audit export output: %v; output=%q", err, out)
	}
	if payload.Count != 1 {
		t.Fatalf("expected 1 audit event, got %d", payload.Count)
	}
	if len(payload.Entries) != 1 {
		t.Fatalf("expected 1 audit entry, got %d", len(payload.Entries))
	}
	if payload.Entries[0].EventID != "audit-2" {
		t.Fatalf("expected audit-2, got %q", payload.Entries[0].EventID)
	}
	if payload.Entries[0].EventType != "node_execution_failed" {
		t.Fatalf("expected node_execution_failed, got %q", payload.Entries[0].EventType)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("expected db file to exist: %v", err)
	}
}

func TestDoctor_PassWithHealthyLocalState(t *testing.T) {
	dbPath, workspaceRoot := setupFakeBridgeEnv(t)

	sharedDir := filepath.Join(workspaceRoot, "shared")
	stateDir := filepath.Join(workspaceRoot, "state")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatalf("mkdir shared dir: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}

	createCLITestWorkspace(t, "ws-doctor")
	if err := runTaskSubmit([]string{
		"--task-id", "task-doctor",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-doctor",
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}

	metricsPath := filepath.Join(filepath.Dir(workspaceRoot), "metrics.jsonl")
	if err := os.WriteFile(metricsPath, []byte(validMetricsSnapshotLine()+"\n"), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runDoctor([]string{
			"--format", "json",
		})
	})
	if err != nil {
		t.Fatalf("runDoctor failed: %v", err)
	}

	var report struct {
		Verdict string `json:"verdict"`
		Config  struct {
			DBPath        string `json:"db_path"`
			WorkspaceRoot string `json:"workspace_root"`
			MetricsPath   string `json:"metrics_path"`
		} `json:"config"`
		Checks []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"checks"`
	}
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("decode doctor output: %v; output=%q", err, out)
	}
	if report.Verdict != doctorStatusPass {
		t.Fatalf("expected doctor verdict %q, got %q", doctorStatusPass, report.Verdict)
	}
	if report.Config.DBPath != dbPath {
		t.Fatalf("expected doctor db path %q, got %q", dbPath, report.Config.DBPath)
	}
	if len(report.Checks) == 0 {
		t.Fatalf("expected doctor checks, got none")
	}
}

func TestDoctor_WarnsOnLoopbackServiceDrift(t *testing.T) {
	_, workspaceRoot := setupFakeBridgeEnv(t)

	sharedDir := filepath.Join(workspaceRoot, "shared")
	stateDir := filepath.Join(workspaceRoot, "state")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatalf("mkdir shared dir: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}

	createCLITestWorkspace(t, "ws-doctor-drift")
	if err := runTaskSubmit([]string{
		"--task-id", "task-doctor-drift",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-doctor-drift",
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}

	localCheckout := app.CurrentGitCheckoutInfo()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(serviceHealthPayload{
			Status: "ok",
			TS:     "2026-03-21T00:00:00Z",
			Config: configSnapshot{
				DBPath:               filepath.Join(t.TempDir(), "other.db"),
				WorkspaceRoot:        workspaceRoot,
				MetricsPath:          filepath.Join(filepath.Dir(workspaceRoot), "metrics.jsonl"),
				ExecutorPython:       "python",
				ExecutorBridgeScript: "internal/executor/rpc_bridge.py",
			},
			Checkout: localCheckout,
			Metrics: serviceMetricsSummary{
				Status:              "ok",
				SourcePath:          filepath.Join(filepath.Dir(workspaceRoot), "metrics.jsonl"),
				SnapshotsLoaded:     1,
				SnapshotsTotalValid: 1,
				Health: runtimeMetricsHealth{
					Verdict:              "healthy",
					ThresholdFailureRate: runtimeFailureRateThreshold,
					ProfilesEvaluated:    1,
				},
			},
		})
	}))
	defer server.Close()

	out, err := captureStdout(t, func() error {
		return runDoctor([]string{
			"--format", "json",
			"--health-url", server.URL,
		})
	})
	if err != nil {
		t.Fatalf("runDoctor failed: %v", err)
	}

	var report struct {
		Verdict string `json:"verdict"`
		Checks  []struct {
			Name    string         `json:"name"`
			Status  string         `json:"status"`
			Details map[string]any `json:"details"`
		} `json:"checks"`
	}
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("decode doctor output: %v; output=%q", err, out)
	}
	if report.Verdict != doctorStatusWarn {
		t.Fatalf("expected doctor verdict %q, got %q", doctorStatusWarn, report.Verdict)
	}

	found := false
	for _, check := range report.Checks {
		if check.Name != "serve_health" {
			continue
		}
		found = true
		if check.Status != doctorStatusWarn {
			t.Fatalf("expected serve_health warn, got %q", check.Status)
		}
		rawDiffs, ok := check.Details["config_differences"].([]any)
		if !ok || len(rawDiffs) == 0 {
			t.Fatalf("expected config_differences in serve_health details, got %#v", check.Details)
		}
	}
	if !found {
		t.Fatalf("expected serve_health check in report")
	}
}

func TestDoctor_ParsesTypedLiveHealthPayload(t *testing.T) {
	dbPath, workspaceRoot := setupFakeBridgeEnv(t)

	sharedDir := filepath.Join(workspaceRoot, "shared")
	stateDir := filepath.Join(workspaceRoot, "state")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatalf("mkdir shared dir: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	createCLITestWorkspace(t, "ws-doctor-typed-payload")
	if err := runTaskSubmit([]string{
		"--task-id", "task-doctor-typed-payload",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-doctor-typed-payload",
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}

	cfg := app.LoadConfig()
	if cfg.DBPath != dbPath {
		t.Fatalf("expected doctor config db path %q, got %q", dbPath, cfg.DBPath)
	}
	localCheckout := app.CurrentGitCheckoutInfo()
	localCheckout.Dirty = true
	metricsPath := filepath.Join(filepath.Dir(workspaceRoot), "metrics.jsonl")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(serviceHealthPayload{
			Status:   "ok",
			TS:       "2026-04-08T00:00:00Z",
			Config:   snapshotConfig(cfg),
			Runtime:  app.CurrentRuntimeBuildInfo(),
			Checkout: localCheckout,
			Metrics: serviceMetricsSummary{
				Status:              "ok",
				SourcePath:          metricsPath,
				SnapshotsLoaded:     1,
				SnapshotsTotalValid: 1,
				LatestTimestamp:     "2026-04-08T00:00:00Z",
				Health: runtimeMetricsHealth{
					Verdict:              "healthy",
					ThresholdFailureRate: runtimeFailureRateThreshold,
					ProfilesEvaluated:    1,
				},
			},
			LoopReadiness: []LoopReadiness{
				{Name: loopNameDaemon, State: LoopRunning, Since: "2026-04-08T00:00:00Z"},
				{Name: loopNameFirehose, State: LoopRunning, Since: "2026-04-08T00:00:00Z"},
			},
			Semantics: TopLevelSemantics{
				Liveness:  DiagnosticSignal{State: "ok", Message: "endpoint is reachable"},
				Readiness: DiagnosticSignal{State: "ok", Message: "core dependencies initialized"},
				Degraded:  DiagnosticSignal{State: "degraded", Message: "one or more loops or metrics are degraded"},
			},
			Extended: ExtendedReadiness{
				MotifLifecycle:   DiagnosticSignal{State: "ok", Message: "firehose loop running"},
				InvalidationLag:  DiagnosticSignal{State: "unsupported", Message: "requires global O(1) index, currently missing"},
				OperatorQueueLag: DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
				ReviewerScarcity: DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
				StuckAgents:      DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
				ProjectionLag:    sqlite.MemoryProjectionLagSnapshot{State: "unsupported", Message: "memory projection lacks async tracking metrics"},
				ReplayHealth:     DiagnosticSignal{State: "unsupported", Message: "lacks global cross-workspace aggregation view"},
			},
		})
	}))
	defer server.Close()

	report := collectDoctorReport("trace-typed-live", cfg, "", server.URL, "", false)
	if report.Verdict != doctorStatusFail {
		t.Fatalf("expected critical doctor verdict from typed degraded payload, got %q", report.Verdict)
	}

	checkByName := make(map[string]doctorCheck, len(report.Checks))
	for _, check := range report.Checks {
		checkByName[check.Name] = check
	}

	if got := checkByName["serve_health"]; got.Status != doctorStatusWarn {
		t.Fatalf("expected serve_health warn for typed payload in dirty worktree, got %q: %+v", got.Status, got)
	}
	if got := checkByName["loop_readiness"]; got.Status != doctorStatusPass {
		t.Fatalf("expected loop_readiness pass from typed payload, got %q: %+v", got.Status, got)
	}
	if got := checkByName["extended_readiness"]; got.Status != doctorStatusWarn {
		t.Fatalf("expected extended_readiness warn from typed payload, got %q: %+v", got.Status, got)
	}
	if got := checkByName["service_liveness"]; got.Status != doctorStatusPass {
		t.Fatalf("expected service_liveness pass from typed payload, got %q: %+v", got.Status, got)
	}
	if got := checkByName["service_readiness"]; got.Status != doctorStatusFail {
		t.Fatalf("expected service_readiness fail from typed degraded legacy payload, got %q: %+v", got.Status, got)
	}
	if got := checkByName["service_deployment_readiness"]; got.Status != doctorStatusFail {
		t.Fatalf("expected service_deployment_readiness fail from typed degraded payload, got %q: %+v", got.Status, got)
	}
	if got := checkByName["service_degraded"]; got.Status != doctorStatusWarn {
		t.Fatalf("expected service_degraded warn from typed payload, got %q: %+v", got.Status, got)
	}
	if _, ok := checkByName["serve_health"]; !ok {
		t.Fatal("expected serve_health in doctor checks")
	}
}

func TestDoctor_TypedLiveHealthPayload_PartialDaemonFailureIsolationWarnsWithoutGlobalFailure(t *testing.T) {
	dbPath, workspaceRoot := setupFakeBridgeEnv(t)

	sharedDir := filepath.Join(workspaceRoot, "shared")
	stateDir := filepath.Join(workspaceRoot, "state")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatalf("mkdir shared dir: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	createCLITestWorkspace(t, "ws-doctor-partial-daemon-failure")
	if err := runTaskSubmit([]string{
		"--task-id", "task-doctor-partial-daemon-failure",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-doctor-partial-daemon-failure",
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}

	cfg := app.LoadConfig()
	if cfg.DBPath != dbPath {
		t.Fatalf("expected doctor config db path %q, got %q", dbPath, cfg.DBPath)
	}
	metricsPath := filepath.Join(t.TempDir(), "metrics.jsonl")
	if err := os.WriteFile(metricsPath, []byte(validMetricsSnapshotLine()+"\n"), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}
	cfg.MetricsPath = metricsPath

	registry := NewReadinessRegistry()
	registry.Register(loopNameDaemon)
	registry.Register(loopNameFirehose)
	registry.Register(loopNameTimeoutReaper)
	registry.SetState(loopNameDaemon, LoopRunning)
	registry.SetError(loopNameDaemon, os.ErrDeadlineExceeded)
	registry.SetState(loopNameFirehose, LoopRunning)
	registry.SetState(loopNameTimeoutReaper, LoopRunning)

	payload := collectServiceHealthPayload(cfg, registry, sqlite.MemoryProjectionLagSnapshot{State: "ok"})
	if payload.Status != "degraded" {
		t.Fatalf("expected generated payload degraded, got %q", payload.Status)
	}
	if payload.Semantics.Liveness.State != "ok" {
		t.Fatalf("expected generated payload liveness ok, got %q", payload.Semantics.Liveness.State)
	}
	if payload.Semantics.Readiness.State != "not_ready" {
		t.Fatalf("expected generated payload readiness not_ready, got %q", payload.Semantics.Readiness.State)
	}
	if payload.Semantics.Degraded.State != "degraded" {
		t.Fatalf("expected generated payload degraded semantic, got %q", payload.Semantics.Degraded.State)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	}))
	defer server.Close()

	report := collectDoctorReport("trace-partial-daemon-failure", cfg, "", server.URL, "", false)
	if report.Verdict != doctorStatusWarn {
		t.Fatalf("expected warn doctor verdict for partial daemon failure, got %q", report.Verdict)
	}

	checkByName := make(map[string]doctorCheck, len(report.Checks))
	for _, check := range report.Checks {
		checkByName[check.Name] = check
	}

	if got := checkByName["service_liveness"]; got.Status != doctorStatusPass {
		t.Fatalf("expected service_liveness pass, got %q: %+v", got.Status, got)
	}
	if got := checkByName["service_readiness"]; got.Status != doctorStatusWarn {
		t.Fatalf("expected service_readiness warn, got %q: %+v", got.Status, got)
	}
	if got := checkByName["service_deployment_readiness"]; got.Status != doctorStatusWarn {
		t.Fatalf("expected service_deployment_readiness warn, got %q: %+v", got.Status, got)
	}
	if got := checkByName["service_degraded"]; got.Status != doctorStatusWarn {
		t.Fatalf("expected service_degraded warn, got %q: %+v", got.Status, got)
	}
	if got := checkByName["loop_readiness"]; got.Status != doctorStatusWarn {
		t.Fatalf("expected loop_readiness warn, got %q: %+v", got.Status, got)
	}
}

func TestDoctor_FailOnWarnReturnsError(t *testing.T) {
	dbPath, workspaceRoot := setupFakeBridgeEnv(t)

	sharedDir := filepath.Join(workspaceRoot, "shared")
	stateDir := filepath.Join(workspaceRoot, "state")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatalf("mkdir shared dir: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	createCLITestWorkspace(t, "ws-doctor-warn-gate")
	if err := runTaskSubmit([]string{
		"--task-id", "task-doctor-warn-gate",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-doctor-warn-gate",
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(serviceHealthPayload{
			Status: "degraded",
			TS:     "2026-03-21T00:00:00Z",
			Config: configSnapshot{
				DBPath:               dbPath,
				WorkspaceRoot:        workspaceRoot,
				MetricsPath:          filepath.Join(filepath.Dir(workspaceRoot), "metrics.jsonl"),
				ExecutorPython:       "python",
				ExecutorBridgeScript: "internal/executor/rpc_bridge.py",
			},
			Checkout: app.CurrentGitCheckoutInfo(),
			Metrics: serviceMetricsSummary{
				Status: "degraded",
				Health: runtimeMetricsHealth{
					Verdict:              "degraded",
					ThresholdFailureRate: runtimeFailureRateThreshold,
					ProfilesEvaluated:    1,
					Reasons:              []string{"profile=compute failure_rate=0.5 exceeds threshold=0.20"},
				},
			},
		})
	}))
	defer server.Close()

	out, err := captureStdout(t, func() error {
		return runDoctor([]string{
			"--format", "json",
			"--health-url", server.URL,
			"--fail-on-warn",
		})
	})
	if err == nil {
		t.Fatal("expected runDoctor to fail when --fail-on-warn is set")
	}
	if !strings.Contains(err.Error(), "doctor verdict: warn") {
		t.Fatalf("unexpected fail-on-warn error: %v", err)
	}
	if !strings.Contains(out, `"verdict": "warn"`) {
		t.Fatalf("expected warn verdict in doctor output, got %s", out)
	}
}

func TestBackupCreateRestore_RoundTrip(t *testing.T) {
	_, workspaceRoot := setupFakeBridgeEnv(t)
	sharedDir := filepath.Join(workspaceRoot, "shared")
	stateDir := filepath.Join(workspaceRoot, "state")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatalf("mkdir shared dir: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}

	const taskID = "task-backup"
	createCLITestWorkspace(t, "ws-backup")
	if err := runTaskSubmit([]string{
		"--task-id", taskID,
		"--owner-user-id", "developer",
		"--workspace-id", "ws-backup",
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}

	artifactPath := filepath.Join(sharedDir, "artifact.txt")
	if err := os.WriteFile(artifactPath, []byte("artifact-data"), 0o644); err != nil {
		t.Fatalf("write artifact file: %v", err)
	}

	cfg := app.LoadConfig()
	if err := os.WriteFile(cfg.MetricsPath, []byte(validMetricsSnapshotLine()+"\n"), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	backupPath := filepath.Join(t.TempDir(), "rhizome-backup.zip")
	out, err := captureStdout(t, func() error {
		return runBackupCreate([]string{
			"--output", backupPath,
		})
	})
	if err != nil {
		t.Fatalf("runBackupCreate failed: %v", err)
	}
	if !strings.Contains(out, `"status": "CREATED"`) {
		t.Fatalf("expected CREATED backup output, got %q", out)
	}

	if err := os.Remove(cfg.DBPath); err != nil {
		t.Fatalf("remove db for restore test: %v", err)
	}
	_ = os.Remove(cfg.DBPath + "-wal")
	_ = os.Remove(cfg.DBPath + "-shm")
	if err := os.Remove(cfg.MetricsPath); err != nil {
		t.Fatalf("remove metrics for restore test: %v", err)
	}
	if err := os.RemoveAll(cfg.WorkspaceRoot); err != nil {
		t.Fatalf("remove workspace root for restore test: %v", err)
	}

	restoreOut, err := captureStdout(t, func() error {
		return runBackupRestore([]string{
			"--input", backupPath,
		})
	})
	if err != nil {
		t.Fatalf("runBackupRestore failed: %v", err)
	}
	if !strings.Contains(restoreOut, `"status": "RESTORED"`) {
		t.Fatalf("expected RESTORED backup output, got %q", restoreOut)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("open restored store: %v", err)
	}
	defer func() { _ = store.Close() }()

	status, err := store.GetTaskStatus(context.Background(), "", taskID)
	if err != nil {
		t.Fatalf("get restored task status: %v", err)
	}
	if status.TaskID != taskID {
		t.Fatalf("expected restored task %q, got %q", taskID, status.TaskID)
	}

	artifactRaw, err := os.ReadFile(filepath.Join(cfg.WorkspaceRoot, "shared", "artifact.txt"))
	if err != nil {
		t.Fatalf("read restored artifact: %v", err)
	}
	if string(artifactRaw) != "artifact-data" {
		t.Fatalf("expected restored artifact-data, got %q", string(artifactRaw))
	}

	metricsRaw, err := os.ReadFile(cfg.MetricsPath)
	if err != nil {
		t.Fatalf("read restored metrics: %v", err)
	}
	if !strings.Contains(string(metricsRaw), `"timestamp":"2026-03-13T00:00:00Z"`) {
		t.Fatalf("expected restored metrics snapshot, got %q", string(metricsRaw))
	}
}

func validMetricsSnapshotLine() string {
	return `{"timestamp":"2026-03-13T00:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0,"avg_duration_sec":1.2,"avg_startup_ms":200}},"recovery":{"total_recoveries":0,"successful":0,"failed":0,"avg_recovery_time_sec":0},"orphan_containers_cleaned":0}`
}
