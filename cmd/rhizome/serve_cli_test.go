package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestServeListenAddressDefaultsAndRemoteOptIn(t *testing.T) {
	if got := resolveServeListenAddr("", ""); got != defaultServeListenAddr {
		t.Fatalf("default listen address = %q, want %q", got, defaultServeListenAddr)
	}
	if got := resolveServeListenAddr("127.0.0.1:9000", "127.0.0.1:9001"); got != "127.0.0.1:9000" {
		t.Fatalf("flag listen address did not take precedence: %q", got)
	}

	for _, addr := range []string{"127.0.0.1:8420", "localhost:8420", "[::1]:8420"} {
		if remote, err := validateServeListenAddr(addr, false); err != nil || remote {
			t.Errorf("loopback address %q rejected: remote=%v err=%v", addr, remote, err)
		}
	}
	for _, addr := range []string{":8420", "0.0.0.0:8420", "[::]:8420", "192.0.2.10:8420"} {
		if remote, err := validateServeListenAddr(addr, false); err == nil || !remote || !strings.Contains(err.Error(), "--allow-remote") {
			t.Errorf("remote address %q was not gated: remote=%v err=%v", addr, remote, err)
		}
		if remote, err := validateServeListenAddr(addr, true); err != nil || !remote {
			t.Errorf("explicitly allowed remote address %q rejected: remote=%v err=%v", addr, remote, err)
		}
	}
	if _, err := validateServeListenAddr("not-an-address", true); err == nil {
		t.Fatal("malformed listen address unexpectedly accepted")
	}
}

func TestServeReadinessHTTPStatusContract(t *testing.T) {
	ready := serviceHealthPayload{
		Status: "ok",
		TS:     "2026-04-26T00:00:00Z",
		Semantics: TopLevelSemantics{
			Readiness:           DiagnosticSignal{State: "ok", Message: "core dependencies initialized"},
			DeploymentReadiness: DiagnosticSignal{State: "ok", Message: "deployment diagnostics are ready"},
		},
	}
	if got := serveReadinessHTTPStatus(ready); got != http.StatusOK {
		t.Fatalf("expected ready status %d, got %d", http.StatusOK, got)
	}
	if got := collectPublicReadinessPayload(ready).Status; got != "ready" {
		t.Fatalf("expected public readiness status ready, got %q", got)
	}

	notReady := ready
	notReady.Semantics.Readiness = DiagnosticSignal{State: "not_ready", Message: "one or more required loops have not started"}
	if got := serveReadinessHTTPStatus(notReady); got != http.StatusServiceUnavailable {
		t.Fatalf("expected not-ready status %d, got %d", http.StatusServiceUnavailable, got)
	}
	if got := collectPublicReadinessPayload(notReady).Status; got != "not_ready" {
		t.Fatalf("expected public readiness status not_ready, got %q", got)
	}
	if got := collectPublicReadinessPayload(notReady).ReasonCodes; !containsString(got, "readiness_not_ready") {
		t.Fatalf("expected not-ready public reason code, got %+v", got)
	}

	degraded := ready
	degraded.Status = "degraded"
	degraded.Semantics.DeploymentReadiness = DiagnosticSignal{State: "degraded", Message: "runtime metrics status is missing"}
	if got := serveReadinessHTTPStatus(degraded); got != http.StatusServiceUnavailable {
		t.Fatalf("expected degraded status %d, got %d", http.StatusServiceUnavailable, got)
	}
	if got := collectPublicReadinessPayload(degraded).Status; got != "degraded" {
		t.Fatalf("expected public readiness status degraded, got %q", got)
	}
	if got := collectPublicReadinessPayload(degraded).ReasonCodes; !containsString(got, "deployment_readiness_degraded") || !containsString(got, "service_status_degraded") {
		t.Fatalf("expected degraded public reason codes, got %+v", got)
	}
}

func TestPublicReadinessReasonCodesAreSafeAndStructured(t *testing.T) {
	payload := serviceHealthPayload{
		Status: "degraded",
		TS:     "2026-05-26T00:00:00Z",
		Semantics: TopLevelSemantics{
			Readiness:           DiagnosticSignal{State: "not_ready", Message: "C:/secret/path loop down"},
			DeploymentReadiness: DiagnosticSignal{State: "degraded", Message: "current git checkout has local modifications at C:/secret"},
		},
		Runtime: app.RuntimeBuildInfo{VCSModified: true},
		Metrics: serviceMetricsSummary{Status: "missing"},
		LoopReadiness: []LoopReadiness{{
			Name:  "memory projection",
			State: LoopRecovering,
		}},
	}

	public := collectPublicReadinessPayload(payload)
	for _, want := range []string{
		"readiness_not_ready",
		"deployment_readiness_degraded",
		"service_status_degraded",
		"runtime_built_from_modified_checkout",
		"runtime_metrics_missing",
		"loop_memory_projection_recovering",
	} {
		if !containsString(public.ReasonCodes, want) {
			t.Fatalf("expected reason code %q, got %+v", want, public.ReasonCodes)
		}
	}
	raw, err := json.Marshal(public.ReasonCodes)
	if err != nil {
		t.Fatalf("marshal public readiness reason codes: %v", err)
	}
	if strings.Contains(string(raw), "C:/secret") {
		t.Fatalf("public readiness reason codes leaked message/path data: %s", raw)
	}
}

func TestPublicReadinessPayloadExcludesDiagnosticsSecrets(t *testing.T) {
	payload := serviceHealthPayload{
		Status: "ok",
		TS:     "2026-04-26T00:00:00Z",
		Semantics: TopLevelSemantics{
			Readiness:           DiagnosticSignal{State: "ok"},
			DeploymentReadiness: DiagnosticSignal{State: "ok"},
		},
		Config: configSnapshot{
			DBPath:         "C:/secret/rhizome.db",
			WorkspaceRoot:  "C:/secret/workspace",
			MetricsPath:    "C:/secret/metrics.jsonl",
			ExecutorPython: "C:/secret/python.exe",
		},
	}

	raw, err := json.Marshal(collectPublicReadinessPayload(payload))
	if err != nil {
		t.Fatalf("marshal public readiness payload: %v", err)
	}
	for _, forbidden := range []string{"db_path", "workspace_root", "metrics_path", "executor_python", "C:/secret"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("public readiness payload leaked %q: %s", forbidden, raw)
		}
	}
}

func TestRunServeBindFailureReturnsError(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy test listener: %v", err)
	}
	defer func() { _ = occupied.Close() }()

	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "rhizome.db")
	workspaceRoot := filepath.Join(tmp, "workspace")
	metricsPath := filepath.Join(tmp, "metrics", "runtime_metrics.jsonl")
	t.Setenv("RHIZOME_DB", dbPath)
	t.Setenv("RHIZOME_WORKSPACE_ROOT", workspaceRoot)
	t.Setenv("RHIZOME_METRICS_PATH", metricsPath)

	err = runServe([]string{"--addr", occupied.Addr().String()})
	if err == nil {
		t.Fatal("expected bind failure, got nil")
	}
	if !strings.Contains(err.Error(), "bind http listener") {
		t.Fatalf("expected bind listener error, got %v", err)
	}
	for _, unexpected := range []string{dbPath, workspaceRoot, metricsPath} {
		if _, statErr := os.Stat(unexpected); !os.IsNotExist(statErr) {
			t.Fatalf("bind failure should not create %s; stat err=%v", unexpected, statErr)
		}
	}
}

func TestRunServeWithDaemonInvalidFlagsReturnBeforeBootstrap(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flag    string
		value   string
		wantErr string
	}{
		{
			name:    "poll interval",
			flag:    "--poll-ms",
			value:   "0",
			wantErr: "--poll-ms must be positive",
		},
		{
			name:    "negative poll interval",
			flag:    "--poll-ms",
			value:   "-1",
			wantErr: "--poll-ms must be positive",
		},
		{
			name:    "max nodes",
			flag:    "--max-nodes",
			value:   "0",
			wantErr: "--max-nodes must be positive",
		},
		{
			name:    "negative max nodes",
			flag:    "--max-nodes",
			value:   "-1",
			wantErr: "--max-nodes must be positive",
		},
		{
			name:    "node timeout",
			flag:    "--node-timeout-sec",
			value:   "0",
			wantErr: "--node-timeout-sec must be positive",
		},
		{
			name:    "negative node timeout",
			flag:    "--node-timeout-sec",
			value:   "-1",
			wantErr: "--node-timeout-sec must be positive",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			dbPath := filepath.Join(tmp, "rhizome.db")
			workspaceRoot := filepath.Join(tmp, "workspace")
			metricsPath := filepath.Join(tmp, "metrics", "runtime_metrics.jsonl")
			t.Setenv("RHIZOME_DB", dbPath)
			t.Setenv("RHIZOME_WORKSPACE_ROOT", workspaceRoot)
			t.Setenv("RHIZOME_METRICS_PATH", metricsPath)

			err := runServe([]string{"--addr", "127.0.0.1:0", "--with-daemon", tc.flag, tc.value})
			if err == nil {
				t.Fatal("expected invalid daemon flag error, got nil")
			}
			if got := err.Error(); got != tc.wantErr {
				t.Fatalf("expected error %q, got %q", tc.wantErr, got)
			}
			for _, unexpected := range []string{dbPath, workspaceRoot, metricsPath} {
				if _, statErr := os.Stat(unexpected); !os.IsNotExist(statErr) {
					t.Fatalf("invalid daemon flag should return before bootstrap creates %s; stat err=%v", unexpected, statErr)
				}
			}
		})
	}
}

func TestShutdownServeDrainsHTTPAndLoopsBeforeReturn(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	supervisor := newServeLoopSupervisor()
	serveDone := make(chan struct{})
	serveErr := make(chan error, 1)
	supervisor.Go(func() {
		defer close(serveDone)
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	})

	client := &http.Client{Timeout: 2 * time.Second}
	url := "http://" + listener.Addr().String()
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not start accepting requests: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	loopRelease := make(chan struct{})
	loopDone := make(chan struct{})
	supervisor.Go(func() {
		defer close(loopDone)
		<-loopRelease
	})

	returned := make(chan error, 1)
	go func() {
		returned <- shutdownServe(context.Background(), srv, supervisor)
	}()

	select {
	case err := <-returned:
		t.Fatalf("shutdown returned before supervised loop drained: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(loopRelease)
	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("shutdown serve: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not return after loop drained")
	}

	select {
	case <-serveDone:
	default:
		t.Fatal("shutdown returned before HTTP server goroutine stopped")
	}
	select {
	case <-loopDone:
	default:
		t.Fatal("shutdown returned before background loop stopped")
	}
	select {
	case err := <-serveErr:
		t.Fatalf("serve error: %v", err)
	default:
	}
}

func TestShutdownServeBoundsLoopDrain(t *testing.T) {
	supervisor := newServeLoopSupervisor()
	release := make(chan struct{})
	supervisor.Go(func() {
		<-release
	})
	defer func() {
		close(release)
		if err := supervisor.Wait(context.Background()); err != nil {
			t.Fatalf("cleanup supervisor wait: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := shutdownServe(ctx, nil, supervisor)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected shutdown to return drain deadline error, got %v", err)
	}
}

func TestEnsureServeWorkspaceBootstrapsDefaultWorkspace(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "serve-workspace.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := ensureServeWorkspace(ctx, store, "", "test-bootstrap-password"); err != nil {
		t.Fatalf("ensure serve workspace: %v", err)
	}

	workspace, err := store.GetWorkspace(ctx, "rhizome-main")
	if err != nil {
		t.Fatalf("get default workspace: %v", err)
	}
	if workspace.WorkspaceID != "rhizome-main" || workspace.Title != "rhizome-main" {
		t.Fatalf("unexpected default workspace: %+v", workspace)
	}
	if _, err := store.GetWorkspaceSecuritySettings(ctx, "rhizome-main"); err != nil {
		t.Fatalf("default workspace security settings missing: %v", err)
	}
}

func TestEnsureServeWorkspaceRequiresPasswordForNewWorkspace(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "serve-workspace-password.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := ensureServeWorkspace(ctx, store, "ws-password-required", ""); err == nil || !strings.Contains(err.Error(), "RHIZOME_WORKSPACE_PASSWORD") {
		t.Fatalf("ensureServeWorkspace() error = %v, want missing password error", err)
	}
}

func TestEnsureServeWorkspaceKeepsExistingWorkspace(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "serve-workspace-existing.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-existing",
		Title:       "Existing Workspace",
		Description: "already configured",
		CreatedBy:   "test",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := ensureServeWorkspace(ctx, store, "ws-existing", ""); err != nil {
		t.Fatalf("ensure serve workspace: %v", err)
	}

	workspace, err := store.GetWorkspace(ctx, "ws-existing")
	if err != nil {
		t.Fatalf("get existing workspace: %v", err)
	}
	if workspace.Title != "Existing Workspace" || workspace.CreatedBy != "test" {
		t.Fatalf("existing workspace was overwritten: %+v", workspace)
	}
}

func TestServeMemoryProjectionReconcilerClearsPendingBacklog(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "serve-memory-projection.db")
	t.Setenv("RHIZOME_DB", dbPath)
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	const workspaceID = "ws-serve-projection-reconcile"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Serve Projection Reconcile",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "Serve projection backlog",
		Body:        "The serve process should reconcile memory projection outbox rows without --with-daemon.",
		Summary:     "serve projection backlog",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	nodeID := "memnode:workspace_memory:" + record.MemoryID
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE memory_projection_outbox
		    SET status = ?, attempt_count = 0, last_error = '', available_at = ?, started_at = NULL, completed_at = NULL, updated_at = ?
		  WHERE workspace_id = ? AND projection_kind = ? AND origin_id = ?`,
		"PENDING",
		now,
		now,
		workspaceID,
		"WORKSPACE_MEMORY",
		record.MemoryID,
	); err != nil {
		t.Fatalf("reset memory projection outbox row to pending: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_edges WHERE workspace_id = ? AND (from_memory_id = ? OR to_memory_id = ?)`, workspaceID, nodeID, nodeID); err != nil {
		t.Fatalf("delete projection edges: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, workspaceID, nodeID); err != nil {
		t.Fatalf("delete projection node: %v", err)
	}

	processed, failed, err := runServeMemoryProjectionReconcilerOnce(ctx, store)
	if err != nil {
		t.Fatalf("run serve memory projection reconciler: processed=%d failed=%d err=%v", processed, failed, err)
	}
	if processed == 0 || failed != 0 {
		t.Fatalf("expected serve reconciler to process pending projection without failures, processed=%d failed=%d", processed, failed)
	}
	if _, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID); err != nil {
		t.Fatalf("expected serve reconciler to restore projection node: %v", err)
	}
	lag, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		t.Fatalf("memory projection lag snapshot: %v", err)
	}
	if lag.PendingCount != 0 || lag.ProcessingCount != 0 || lag.FailedCount != 0 {
		t.Fatalf("expected serve reconciler to clear projection lag, got %+v", lag)
	}
}

func TestEnsureServeRuntimeFilesCreatesWorkspaceRootAndMetrics(t *testing.T) {
	tmp := t.TempDir()
	cfg := app.Config{
		WorkspaceRoot: filepath.Join(tmp, "workspace"),
		MetricsPath:   filepath.Join(tmp, "metrics", "runtime_metrics.jsonl"),
	}

	if err := ensureServeRuntimeFiles(cfg, time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("ensure serve runtime files: %v", err)
	}

	for _, dir := range []string{
		filepath.Join(cfg.WorkspaceRoot, "shared"),
		filepath.Join(cfg.WorkspaceRoot, "state"),
	} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("expected runtime dir %s: %v", dir, err)
		}
		if !info.IsDir() {
			t.Fatalf("expected runtime path %s to be a directory", dir)
		}
	}

	check := checkRuntimeMetrics(cfg.MetricsPath)
	if check.Status != doctorStatusPass {
		t.Fatalf("expected serve startup metrics to satisfy doctor, got %+v", check)
	}

	snapshots, totalValid, parseErrors, err := readRuntimeMetricsSnapshots(cfg.MetricsPath, 1)
	if err != nil {
		t.Fatalf("read runtime metrics snapshots: %v", err)
	}
	if totalValid != 1 || parseErrors != 0 || len(snapshots) != 1 {
		t.Fatalf("unexpected startup metrics parse result: total=%d parseErrors=%d snapshots=%d", totalValid, parseErrors, len(snapshots))
	}
	if _, ok := snapshots[0].Profiles["serve_startup"]; !ok {
		t.Fatalf("expected serve_startup profile in metrics snapshot: %+v", snapshots[0].Profiles)
	}
}
