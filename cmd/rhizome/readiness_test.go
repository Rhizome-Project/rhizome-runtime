package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// ---------- ReadinessRegistry unit tests ----------

func TestReadinessRegistry_InitialState(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register("daemon")
	r.Register("firehose")
	r.Register("timeout_reaper")

	snap := r.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 loops registered, got %d", len(snap))
	}
	for _, loop := range snap {
		if loop.State != LoopNotStarted {
			t.Fatalf("expected initial state %q for %s, got %q", LoopNotStarted, loop.Name, loop.State)
		}
	}
}

func TestReadinessRegistry_DaemonEnabled(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.SetState(loopNameDaemon, LoopRunning)

	got := r.Get(loopNameDaemon)
	if got == nil {
		t.Fatal("expected daemon readiness, got nil")
	}
	if got.State != LoopRunning {
		t.Fatalf("expected daemon state %q, got %q", LoopRunning, got.State)
	}
}

func TestReadinessRegistry_DaemonDisabled(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.SetState(loopNameDaemon, LoopDisabled)

	got := r.Get(loopNameDaemon)
	if got == nil {
		t.Fatal("expected daemon readiness, got nil")
	}
	if got.State != LoopDisabled {
		t.Fatalf("expected daemon state %q, got %q", LoopDisabled, got.State)
	}
}

func TestReadinessRegistry_FirehoseHealthy(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameFirehose)
	r.SetState(loopNameFirehose, LoopRunning)
	r.SetDroppedEvents(loopNameFirehose, 0)

	got := r.Get(loopNameFirehose)
	if got == nil {
		t.Fatal("expected firehose readiness, got nil")
	}
	if got.State != LoopRunning {
		t.Fatalf("expected firehose state %q, got %q", LoopRunning, got.State)
	}
	if got.DroppedEvents != 0 {
		t.Fatalf("expected 0 dropped events, got %d", got.DroppedEvents)
	}
}

func TestReadinessRegistry_FirehoseDegraded(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameFirehose)
	r.SetState(loopNameFirehose, LoopRunning)
	r.SetDroppedEvents(loopNameFirehose, 42)

	got := r.Get(loopNameFirehose)
	if got == nil {
		t.Fatal("expected firehose readiness, got nil")
	}
	if got.State != LoopDegraded {
		t.Fatalf("expected firehose state %q after drops, got %q", LoopDegraded, got.State)
	}
	if got.DroppedEvents != 42 {
		t.Fatalf("expected 42 dropped events, got %d", got.DroppedEvents)
	}
}

func TestReadinessRegistry_LoopRecovering(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.SetState(loopNameDaemon, LoopRunning)

	// Simulate error
	r.SetError(loopNameDaemon, fmt.Errorf("database locked"))

	got := r.Get(loopNameDaemon)
	if got == nil {
		t.Fatal("expected daemon readiness, got nil")
	}
	if got.State != LoopRecovering {
		t.Fatalf("expected state %q after error, got %q", LoopRecovering, got.State)
	}
	if got.Restarts != 1 {
		t.Fatalf("expected 1 restart, got %d", got.Restarts)
	}
	if got.LastError != "database locked" {
		t.Fatalf("expected last error 'database locked', got %q", got.LastError)
	}

	// Simulate second error
	r.SetError(loopNameDaemon, fmt.Errorf("context deadline exceeded"))
	got = r.Get(loopNameDaemon)
	if got.Restarts != 2 {
		t.Fatalf("expected 2 restarts after second error, got %d", got.Restarts)
	}
}

func TestReadinessRegistry_LoopSuccessAndDegradedErrorMetadata(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameMemoryProjection)

	successAt := time.Date(2026, 4, 26, 10, 30, 0, 0, time.UTC)
	r.setSuccessAt(loopNameMemoryProjection, successAt)

	got := r.Get(loopNameMemoryProjection)
	if got == nil {
		t.Fatal("expected memory projection readiness, got nil")
	}
	if got.State != LoopRunning {
		t.Fatalf("expected memory projection state %q after success, got %q", LoopRunning, got.State)
	}
	if got.LastSuccess != successAt.Format(time.RFC3339Nano) {
		t.Fatalf("expected last_success %q, got %q", successAt.Format(time.RFC3339Nano), got.LastSuccess)
	}
	if got.Restarts != 0 {
		t.Fatalf("success should not increment restarts, got %d", got.Restarts)
	}

	errorAt := successAt.Add(time.Second)
	r.setDegradedAt(loopNameMemoryProjection, fmt.Errorf("projection outbox locked"), errorAt)

	got = r.Get(loopNameMemoryProjection)
	if got == nil {
		t.Fatal("expected memory projection readiness after degraded error, got nil")
	}
	if got.State != LoopDegraded {
		t.Fatalf("expected memory projection state %q after non-fatal error, got %q", LoopDegraded, got.State)
	}
	if got.LastSuccess != successAt.Format(time.RFC3339Nano) {
		t.Fatalf("expected last_success to be preserved after error, got %q", got.LastSuccess)
	}
	if got.LastError != "projection outbox locked" {
		t.Fatalf("expected last_error to be recorded, got %q", got.LastError)
	}
	if got.LastErrorAt != errorAt.Format(time.RFC3339Nano) {
		t.Fatalf("expected last_error_at %q, got %q", errorAt.Format(time.RFC3339Nano), got.LastErrorAt)
	}
	if got.Restarts != 0 {
		t.Fatalf("non-fatal degraded error should not increment restarts, got %d", got.Restarts)
	}
}

func TestReadinessRegistry_OverallState(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register("a")
	r.Register("b")
	r.Register("c")

	// All not_started
	if got := r.OverallState(); got != LoopNotStarted {
		t.Fatalf("expected overall %q, got %q", LoopNotStarted, got)
	}

	// Mix of running and disabled
	r.SetState("a", LoopRunning)
	r.SetState("b", LoopDisabled)
	r.SetState("c", LoopRunning)
	if got := r.OverallState(); got != LoopRunning {
		t.Fatalf("expected overall %q, got %q", LoopRunning, got)
	}

	// One degraded
	r.SetState("c", LoopDegraded)
	if got := r.OverallState(); got != LoopDegraded {
		t.Fatalf("expected overall %q, got %q", LoopDegraded, got)
	}

	// One recovering
	r.SetError("a", fmt.Errorf("boom"))
	if got := r.OverallState(); got != LoopRecovering {
		t.Fatalf("expected overall %q, got %q", LoopRecovering, got)
	}
}

// ---------- Firehose readiness integration ----------

func TestSyncFirehoseReadiness_Running(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "readiness-fh.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Give the firehose goroutine a moment to start
	time.Sleep(50 * time.Millisecond)

	r := NewReadinessRegistry()
	r.Register(loopNameFirehose)
	syncFirehoseReadiness(r, store)

	got := r.Get(loopNameFirehose)
	if got == nil {
		t.Fatal("expected firehose readiness, got nil")
	}
	if got.State != LoopRunning {
		t.Fatalf("expected firehose state %q (store creates firehose on init), got %q", LoopRunning, got.State)
	}
	if got.DroppedEvents != 0 {
		t.Fatalf("expected 0 dropped events on fresh store, got %d", got.DroppedEvents)
	}
}

func TestSyncFirehoseReadiness_RunningWithDroppedEventsBecomesDegraded(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameFirehose)
	r.SetDroppedEvents(loopNameFirehose, 3)

	got := r.Get(loopNameFirehose)
	if got == nil {
		t.Fatal("expected firehose readiness, got nil")
	}
	if got.State != LoopDegraded {
		t.Fatalf("expected firehose state %q with dropped events, got %q", LoopDegraded, got.State)
	}
	if got.DroppedEvents != 3 {
		t.Fatalf("expected 3 dropped events, got %d", got.DroppedEvents)
	}
}

func TestSyncFirehoseReadiness_Stopped(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "readiness-fh-stopped.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	r := NewReadinessRegistry()
	r.Register(loopNameFirehose)

	// Observe the firehose in a running state first, then verify that losing
	// ownership transitions the loop to stopped rather than back to not_started.
	time.Sleep(50 * time.Millisecond)
	syncFirehoseReadiness(r, store)
	if got := r.Get(loopNameFirehose); got == nil || got.State != LoopRunning {
		t.Fatalf("expected firehose to be running before shutdown, got %+v", got)
	}

	_ = store.Close()
	time.Sleep(50 * time.Millisecond)
	syncFirehoseReadiness(r, store)

	got := r.Get(loopNameFirehose)
	if got == nil {
		t.Fatal("expected firehose readiness, got nil")
	}
	if got.State != LoopStopped {
		t.Fatalf("expected firehose state %q after store close, got %q", LoopStopped, got.State)
	}
}

func TestMemoryProjectionReconcilerReadinessRecordsSuccessAndError(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameMemoryProjection)

	recordMemoryProjectionReconcilerResult(r, nil)
	got := r.Get(loopNameMemoryProjection)
	if got == nil {
		t.Fatal("expected memory projection readiness, got nil")
	}
	if got.State != LoopRunning {
		t.Fatalf("expected memory projection state %q after success, got %q", LoopRunning, got.State)
	}
	if strings.TrimSpace(got.LastSuccess) == "" {
		t.Fatalf("expected last_success after successful sweep, got %+v", got)
	}

	recordMemoryProjectionReconcilerResult(r, fmt.Errorf("reconcile memory projection workspace ws-1: database locked"))
	got = r.Get(loopNameMemoryProjection)
	if got == nil {
		t.Fatal("expected memory projection readiness after error, got nil")
	}
	if got.State != LoopDegraded {
		t.Fatalf("expected memory projection state %q after reconcile error, got %q", LoopDegraded, got.State)
	}
	if !strings.Contains(got.LastError, "database locked") {
		t.Fatalf("expected last_error to include reconcile failure, got %q", got.LastError)
	}
	if strings.TrimSpace(got.LastErrorAt) == "" {
		t.Fatalf("expected last_error_at after reconcile failure, got %+v", got)
	}
	if strings.TrimSpace(got.LastSuccess) == "" {
		t.Fatalf("expected previous last_success to remain visible after error, got %+v", got)
	}
}

func TestMemoryProjectionReadiness_StaleSuccessDegradesReadyPayload(t *testing.T) {
	now := time.Date(2026, 4, 26, 11, 0, 0, 0, time.UTC)
	r := NewReadinessRegistry()
	r.Register(loopNameMemoryProjection)
	r.setSuccessAt(loopNameMemoryProjection, now.Add(-serveMemoryProjectionReadinessStaleAfter-time.Second))

	syncMemoryProjectionReadiness(r, now)
	got := r.Get(loopNameMemoryProjection)
	if got == nil {
		t.Fatal("expected memory projection readiness, got nil")
	}
	if got.State != LoopDegraded {
		t.Fatalf("expected stale memory projection loop to become %q, got %+v", LoopDegraded, got)
	}
	if got.LastSuccess == "" || !strings.Contains(got.LastError, "successful sweep") {
		t.Fatalf("expected stale diagnostics to retain last_success and explain last_error, got %+v", got)
	}

	metricsFile := filepath.Join(t.TempDir(), "runtime_metrics.jsonl")
	if err := createValidMetricsFixture(metricsFile); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}
	payload := collectServiceHealthPayloadFromState(
		app.Config{MetricsPath: metricsFile},
		r,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "ok", Message: "operator queue lag healthy"},
		DiagnosticSignal{State: "ok", Message: "reviewer scarcity healthy"},
		DiagnosticSignal{State: "ok", Message: "stuck agent health is stable"},
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy"},
		app.RuntimeBuildInfo{},
		app.GitCheckoutInfo{},
	)
	if payload.Status != "degraded" {
		t.Fatalf("expected stale memory projection loop to degrade diagnostics, got %q", payload.Status)
	}
	if serveReadinessHTTPStatus(payload) != http.StatusServiceUnavailable {
		t.Fatalf("expected stale memory projection loop to make /ready unavailable, got %d", serveReadinessHTTPStatus(payload))
	}
	public := collectPublicReadinessPayload(payload)
	found := false
	for _, loop := range public.LoopReadiness {
		if loop.Name != loopNameMemoryProjection {
			continue
		}
		found = true
		if loop.LastSuccess == "" || loop.LastError == "" {
			t.Fatalf("expected public readiness to include memory projection last success/error, got %+v", loop)
		}
	}
	if !found {
		t.Fatalf("expected public readiness to include %q loop, got %+v", loopNameMemoryProjection, public.LoopReadiness)
	}
}

// ---------- Diagnostics payload integration ----------

func TestDiagnosticsPayload_IncludesReadiness(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.Register(loopNameFirehose)
	r.Register(loopNameTimeoutReaper)

	r.SetState(loopNameDaemon, LoopDisabled)
	r.SetState(loopNameFirehose, LoopRunning)
	r.SetState(loopNameTimeoutReaper, LoopRunning)

	cfg := app.Config{}
	payload := collectServiceHealthPayload(cfg, r, sqlite.MemoryProjectionLagSnapshot{State: "ok"})

	if payload.LoopReadiness == nil {
		t.Fatal("expected loop_readiness in diagnostics payload")
	}
	if len(payload.LoopReadiness) != 3 {
		t.Fatalf("expected 3 loop readiness entries, got %d", len(payload.LoopReadiness))
	}

	// Check that all loops are present
	found := map[string]bool{}
	for _, loop := range payload.LoopReadiness {
		found[loop.Name] = true
	}
	for _, name := range []string{loopNameDaemon, loopNameFirehose, loopNameTimeoutReaper} {
		if !found[name] {
			t.Fatalf("expected loop %q in readiness, not found", name)
		}
	}
}

func TestDiagnosticsPayload_DegradedWhenRecovering(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.SetError(loopNameDaemon, fmt.Errorf("test crash"))

	cfg := app.Config{}
	payload := collectServiceHealthPayload(cfg, r, sqlite.MemoryProjectionLagSnapshot{State: "ok"})

	if payload.Status != "degraded" {
		t.Fatalf("expected status 'degraded' when loop recovering, got %q", payload.Status)
	}
}

func TestPublicLiveness_ExcludesReadiness(t *testing.T) {
	payload := collectPublicLivenessPayload()

	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal liveness: %v", err)
	}
	if strings.Contains(string(raw), "loop_readiness") {
		t.Fatalf("public liveness payload should not contain loop_readiness: %s", raw)
	}
	if strings.Contains(string(raw), "config") {
		t.Fatalf("public liveness payload should not contain config: %s", raw)
	}
}

func TestDiagnosticsPayload_NilRegistryStillWorks(t *testing.T) {
	cfg := app.Config{}
	payload := collectServiceHealthPayload(cfg, nil, sqlite.MemoryProjectionLagSnapshot{State: "ok"})

	if payload.LoopReadiness != nil {
		t.Fatalf("expected nil loop_readiness when no registry, got %v", payload.LoopReadiness)
	}
}

func TestDiagnosticsPayload_IncludesProjectionLag(t *testing.T) {
	cfg := app.Config{}
	age := int64(120)
	payload := collectServiceHealthPayload(cfg, nil, sqlite.MemoryProjectionLagSnapshot{
		State:                   "degraded",
		Message:                 "2 pending projection(s); 1 failed projection(s)",
		PendingCount:            2,
		FailedCount:             1,
		OldestPendingAt:         "2026-04-08T12:00:00Z",
		OldestPendingAgeSeconds: &age,
	})

	if payload.Extended.ProjectionLag.State != "degraded" {
		t.Fatalf("expected projection lag state degraded, got %q", payload.Extended.ProjectionLag.State)
	}
	if payload.Extended.ProjectionLag.PendingCount != 2 || payload.Extended.ProjectionLag.FailedCount != 1 {
		t.Fatalf("unexpected projection lag snapshot: %+v", payload.Extended.ProjectionLag)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected top-level status degraded with projection backlog, got %q", payload.Status)
	}
}

func TestDiagnosticsPayload_IncludesBudgetLedgerAndDegradesWhenExhausted(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := createValidMetricsFixture(metricsFile); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}
	budget := sqlite.BudgetLedgerHealthSnapshot{
		Contract:              sqlite.BudgetLedgerHealthContract,
		AccountCount:          1,
		ExhaustedAccountCount: 1,
		OpenReservationCount:  1,
		LedgerEntryCount:      1,
		ReferenceAt:           "2026-04-24T12:00:00Z",
		Status:                "exhausted",
		Message:               "budget exhausted for 1 account(s)",
		Reasons:               []string{"exhausted_accounts"},
		ExhaustedAccountExamples: []sqlite.BudgetLedgerAccountHealthExample{
			{
				AccountID:       "budget-account-r4-3",
				WorkspaceID:     "ws-budget",
				LimitMicros:     1000,
				ReservedMicros:  1000,
				AvailableMicros: 0,
				Status:          sqlite.BudgetAccountStatusActive,
			},
		},
	}
	payload := collectServiceHealthPayloadFromStateWithReviewerScarcityHealthAndNoProgressHealthAndBudgetLedger(
		app.Config{MetricsPath: metricsFile},
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "ok", Message: "operator queue lag healthy"},
		DiagnosticSignal{State: "ok", Message: "reviewer scarcity healthy"},
		sqlite.ReviewerScarcityHealthSnapshot{},
		DiagnosticSignal{State: "ok", Message: "stuck agent health is stable"},
		nil,
		DiagnosticSignal{State: "ok", Message: "execution progress healthy"},
		nil,
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy"},
		&budget,
		app.RuntimeBuildInfo{},
		app.GitCheckoutInfo{},
	)

	if payload.BudgetLedger == nil || payload.BudgetLedger.ExhaustedAccountCount != 1 {
		t.Fatalf("expected exhausted budget ledger diagnostics, got %+v", payload.BudgetLedger)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected exhausted budget ledger to degrade diagnostics payload, got %q", payload.Status)
	}
	if !strings.Contains(payload.Semantics.DeploymentReadiness.Message, "budget ledger status is exhausted") {
		t.Fatalf("expected deployment readiness message to include budget exhaustion, got %+v", payload.Semantics.DeploymentReadiness)
	}

	check := checkBudgetLedgerFromDetails(map[string]any{"service": payload})
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected doctor budget ledger warning for exhausted accounts, got %+v", check)
	}
	if got := check.Details["exhausted_account_count"]; got != 1 {
		t.Fatalf("expected exhausted count in doctor details, got %+v", check.Details)
	}
	if got := check.Details["contract"]; got != sqlite.BudgetLedgerHealthContract {
		t.Fatalf("expected budget health contract in doctor details, got %+v", check.Details)
	}
}

func TestDoctor_BudgetLedgerCheckAllowsEmptyConfiguredLedger(t *testing.T) {
	check := checkBudgetLedgerFromDetails(map[string]any{
		"service": serviceHealthPayload{
			Status: "ok",
			Config: configSnapshot{WorkspaceRoot: "workspace"},
			BudgetLedger: &sqlite.BudgetLedgerHealthSnapshot{
				Contract:     sqlite.BudgetLedgerHealthContract,
				AccountCount: 0,
				Status:       "ok",
				Message:      "budget ledger initialized; no budget accounts configured",
			},
		},
	})
	if check.Status != doctorStatusPass {
		t.Fatalf("expected empty configured budget ledger to pass, got %+v", check)
	}
}

func TestDoctor_BudgetLedgerCheckFailsUnsupportedContract(t *testing.T) {
	check := checkBudgetLedgerFromDetails(map[string]any{
		"service": serviceHealthPayload{
			Status: "ok",
			Config: configSnapshot{WorkspaceRoot: "workspace"},
			BudgetLedger: &sqlite.BudgetLedgerHealthSnapshot{
				Contract:     "budget_ledger.health.v0",
				AccountCount: 1,
				Status:       "ok",
				Message:      "legacy budget payload",
			},
		},
	})
	if check.Status != doctorStatusFail {
		t.Fatalf("expected unsupported budget health contract to fail, got %+v", check)
	}
	if got := check.Details["required_contract"]; got != sqlite.BudgetLedgerHealthContract {
		t.Fatalf("expected required budget health contract in details, got %+v", check.Details)
	}
}

func TestHealthPayloadSurfacesRepoMutationActivationFailClosed(t *testing.T) {
	diag := collectRepoMutationActivationDiagnostics()
	if diag.Schema != repoauthority.MutationActivationGateSchemaVersion {
		t.Fatalf("repo mutation activation schema = %q", diag.Schema)
	}
	if diag.Status != repoauthority.MutationActivationStatusBlocked {
		t.Fatalf("repo mutation activation status = %q", diag.Status)
	}
	if diag.MutationAllowed {
		t.Fatalf("repo mutation activation unexpectedly allowed mutation: %+v", diag)
	}
	if mutationActivationGateByNameForReadinessTest(t, diag, "controlled_context_mode").Passed {
		t.Fatalf("controlled_context_mode should block current patch-only repoauthority context")
	}
	dryRun := collectRepoMutationActuatorDryRunDiagnostics(diag)
	if dryRun.Schema != repoauthority.MutationActuatorDryRunSchemaVersion {
		t.Fatalf("repo mutation actuator dry-run schema = %q", dryRun.Schema)
	}
	if dryRun.Status != repoauthority.MutationActuatorDryRunStatusBlocked {
		t.Fatalf("repo mutation actuator dry-run status = %q, want blocked; result=%+v", dryRun.Status, dryRun)
	}
	if dryRun.WouldMutate || dryRun.MutationExecuted {
		t.Fatalf("repo mutation actuator dry-run must not claim mutation, got %+v", dryRun)
	}
	if err := repoauthority.VerifyMutationActuatorDryRunResult(dryRun); err != nil {
		t.Fatalf("VerifyMutationActuatorDryRunResult: %v", err)
	}
}

func TestServeHealthPayloadSurfacesPatchQueueDurabilityAndFeedsMutationGate(t *testing.T) {
	store := newReadinessTestStore(t)
	payload := collectServeHealthPayload(context.Background(), app.Config{}, store, NewReadinessRegistry())

	if payload.PatchQueue == nil {
		t.Fatalf("expected project patch queue durability proof in serve diagnostics")
	}
	if payload.PatchQueue.State != "ok" || !payload.PatchQueue.Durable {
		t.Fatalf("expected durable project patch queue proof, got %+v", payload.PatchQueue)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, payload.RepoMutation, "durable_patch_queue"); !gate.Passed {
		t.Fatalf("expected durable_patch_queue gate to pass from storage proof, got %+v", gate)
	}
	if payload.RepoMutation.Status != repoauthority.MutationActivationStatusBlocked || payload.RepoMutation.MutationAllowed {
		t.Fatalf("expected repo mutation activation to remain fail-closed, got %+v", payload.RepoMutation)
	}
	if payload.RepoMutationDryRun.ActivationDigest != payload.RepoMutation.Digest {
		t.Fatalf("expected actuator dry-run to bind activation digest, got dry_run=%+v activation=%+v", payload.RepoMutationDryRun, payload.RepoMutation)
	}
	if payload.RepoMutationDryRun.WouldMutate || payload.RepoMutationDryRun.MutationExecuted {
		t.Fatalf("actuator dry-run must remain non-mutating in health payload, got %+v", payload.RepoMutationDryRun)
	}
	if err := repoauthority.VerifyMutationActuatorDryRunResult(payload.RepoMutationDryRun); err != nil {
		t.Fatalf("VerifyMutationActuatorDryRunResult: %v", err)
	}
}

func TestServeHealthPayloadSurfacesStoreBackedRepoMutationActuatorDryRunReady(t *testing.T) {
	store := newReadinessTestStore(t)
	ctx := context.Background()
	gitFixture := newReadinessGitFixture(t)
	seedReadinessStoreBackedMutationDryRunCandidate(t, ctx, store, gitFixture)

	t.Setenv("RHIZOME_REPO_MUTATION_LIVE_VERIFIER", "1")
	payload := collectServeHealthPayload(ctx, app.Config{}, store, NewReadinessRegistry())

	if payload.RepoMutation.Source != repoauthority.MutationActivationSourceDurableControlledQueueCandidate {
		t.Fatalf("expected store-backed controlled candidate source, got %+v", payload.RepoMutation)
	}
	if payload.RepoMutation.Candidate == nil ||
		payload.RepoMutation.Candidate.QueueID == "" ||
		payload.RepoMutation.Candidate.State != sqlite.ProjectPatchQueueStateClaimed {
		t.Fatalf("expected candidate summary for claimed patch queue item, got %+v", payload.RepoMutation.Candidate)
	}
	if payload.RepoMutation.Status != repoauthority.MutationActivationStatusBlocked || payload.RepoMutation.MutationAllowed {
		t.Fatalf("store-backed verifier-ready activation must remain blocked before live actuator, got %+v", payload.RepoMutation)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, payload.RepoMutation, "live_mutation_verifier_enabled"); !gate.Passed {
		t.Fatalf("expected live verifier gate to pass from explicit env, got %+v", gate)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, payload.RepoMutation, "materialization_preflight_verified"); gate.Passed {
		t.Fatalf("materialization preflight must stay blocked until durable patch bytes exist, got %+v", gate)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, payload.RepoMutation, "live_mutation_actuator_enabled"); gate.Passed {
		t.Fatalf("live actuator gate must remain disabled in health smoke, got %+v", gate)
	}
	if payload.RepoMutationDryRun.Status != repoauthority.MutationActuatorDryRunStatusBlocked {
		t.Fatalf("expected dry-run actuator boundary to stay blocked until materialization exists, got %+v", payload.RepoMutationDryRun)
	}
	if payload.RepoMutationDryRun.ActivationDigest != payload.RepoMutation.Digest {
		t.Fatalf("expected dry-run to bind the store-backed activation digest, got dry_run=%+v activation=%+v", payload.RepoMutationDryRun, payload.RepoMutation)
	}
	if !payload.RepoMutationDryRun.VerifierReady ||
		payload.RepoMutationDryRun.ActuatorEnabled ||
		payload.RepoMutationDryRun.WouldMutate ||
		payload.RepoMutationDryRun.MutationExecuted {
		t.Fatalf("dry-run actuator must be visible without mutating, got %+v", payload.RepoMutationDryRun)
	}
	if !strings.Contains(strings.Join(payload.RepoMutationDryRun.BlockingReasons, "\n"), "materialization_preflight_verified: patch materialization is required") {
		t.Fatalf("dry-run must expose missing materialization blocker, got %+v", payload.RepoMutationDryRun.BlockingReasons)
	}
	if err := repoauthority.VerifyMutationActuatorDryRunResult(payload.RepoMutationDryRun); err != nil {
		t.Fatalf("VerifyMutationActuatorDryRunResult: %v", err)
	}
}

func TestServeHealthPayloadSurfacesDurableMaterializationWithoutRawContent(t *testing.T) {
	store := newReadinessTestStore(t)
	ctx := context.Background()
	gitFixture := newReadinessGitFixture(t)
	item := seedReadinessStoreBackedMutationDryRunCandidate(t, ctx, store, gitFixture)

	const candidateContent = "head\n"
	materialized, _, err := store.RecordProjectPatchQueueMaterializationWithEvent(ctx, sqlite.ProjectPatchQueueMaterializationRecordInput{
		WorkspaceID: "ws-readiness-mutation-dry-run",
		ProjectID:   "project-readiness-mutation-dry-run",
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ClaimToken:  item.ClaimToken,
		ActorID:     "lead-agent",
		ActorType:   "agent",
		Materialization: repoauthority.PatchMaterialization{
			Files: []repoauthority.PatchMaterializedFile{
				{Path: "web-app.txt", Content: candidateContent},
			},
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.materialization_record", "server_rpc", "ws-readiness-mutation-dry-run", "agent", "lead-agent"),
		PromptContextSurface:  "project.patch_queue.materialization_record",
	})
	if err != nil {
		t.Fatalf("record readiness materialization: %v", err)
	}
	if !sqlite.ProjectPatchQueueMaterializationReady(materialized) {
		t.Fatalf("expected materialization to be ready after storage normalization: %+v", materialized)
	}

	t.Setenv("RHIZOME_REPO_MUTATION_LIVE_VERIFIER", "1")
	payload := collectServeHealthPayload(ctx, app.Config{}, store, NewReadinessRegistry())
	if payload.RepoMutation.MaterializationPreflight == nil {
		t.Fatalf("expected materialization preflight diagnostics in repo mutation activation")
	}
	if payload.RepoMutation.MaterializationPreflight.MaterializationDigest != materialized.MaterializationDigest {
		t.Fatalf("expected materialization digest to surface in activation diagnostics, got %+v", payload.RepoMutation.MaterializationPreflight)
	}
	if payload.RepoMutation.MaterializationPreflight.AuthorityProof == nil ||
		payload.RepoMutation.MaterializationPreflight.AuthorityProof.AuthorityDigest != materialized.MaterializationAuthorityProofDigest {
		t.Fatalf("expected durable materialization authority proof to surface in activation diagnostics, got %+v", payload.RepoMutation.MaterializationPreflight)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, payload.RepoMutation, "materialization_preflight_verified"); !gate.Passed {
		t.Fatalf("durable materialization authority proof should pass preflight gate, got %+v", gate)
	}
	if payload.RepoMutationDryRun.Status != repoauthority.MutationActuatorDryRunStatusReady {
		t.Fatalf("durable materialization authority proof should make dry-run verifier-ready, got %+v", payload.RepoMutationDryRun)
	}
	if payload.RepoMutationDryRun.LiveScope != repoauthority.MutationActuatorLiveScopeAddModify {
		t.Fatalf("dry-run live scope = %q, want %q", payload.RepoMutationDryRun.LiveScope, repoauthority.MutationActuatorLiveScopeAddModify)
	}
	if !reflect.DeepEqual(payload.RepoMutationDryRun.AllowedChangeKinds, []string{repoauthority.CASPatchChangeModify, repoauthority.CASPatchChangeAdd}) {
		t.Fatalf("dry-run allowed change kinds = %+v, want modify/add", payload.RepoMutationDryRun.AllowedChangeKinds)
	}
	if !reflect.DeepEqual(payload.RepoMutationDryRun.ObservedChangeKinds, []string{repoauthority.CASPatchChangeModify}) {
		t.Fatalf("dry-run observed change kinds = %+v, want modify", payload.RepoMutationDryRun.ObservedChangeKinds)
	}
	if len(payload.RepoMutationDryRun.UnsupportedChangeKinds) != 0 {
		t.Fatalf("dry-run unsupported change kinds = %+v, want none", payload.RepoMutationDryRun.UnsupportedChangeKinds)
	}
	raw, err := json.Marshal(payload.RepoMutation.MaterializationPreflight)
	if err != nil {
		t.Fatalf("marshal materialization preflight diagnostics: %v", err)
	}
	if strings.Contains(string(raw), candidateContent) || strings.Contains(string(raw), "head\\n") {
		t.Fatalf("materialization preflight diagnostics must not expose raw candidate content: %s", raw)
	}
	if !strings.Contains(string(raw), repoauthority.PatchMaterializationContentDigest(candidateContent)) {
		t.Fatalf("materialization preflight diagnostics should expose content digest, got %s", raw)
	}
	if err := repoauthority.VerifyMutationActuatorDryRunResult(payload.RepoMutationDryRun); err != nil {
		t.Fatalf("VerifyMutationActuatorDryRunResult: %v", err)
	}
}

func TestPatchQueueDurabilityFailureDegradesServeHealth(t *testing.T) {
	payload := serviceHealthPayload{
		Status: "ok",
		Semantics: TopLevelSemantics{
			DeploymentReadiness: DiagnosticSignal{State: "ok", Message: "deployment diagnostics are ready"},
			Degraded:            DiagnosticSignal{State: "ok", Message: "no known degradation"},
		},
	}

	applyProjectPatchQueueDurabilityHealth(&payload, sqlite.ProjectPatchQueueDurabilityProof{
		Contract: sqlite.ProjectPatchQueueDurabilityProofContract,
		State:    "error",
		Message:  "patch queue item count query failed",
		Error:    "database is locked",
	})

	if payload.Status != "degraded" {
		t.Fatalf("expected patch queue durability error to degrade health, got %q", payload.Status)
	}
	if payload.Semantics.DeploymentReadiness.State != "degraded" || !strings.Contains(payload.Semantics.DeploymentReadiness.Message, "patch queue item count") {
		t.Fatalf("expected deployment readiness degradation from patch queue proof, got %+v", payload.Semantics.DeploymentReadiness)
	}
	if payload.Semantics.Degraded.State != "degraded" || !strings.Contains(payload.Semantics.Degraded.Message, "patch queue item count") {
		t.Fatalf("expected degraded semantics from patch queue proof, got %+v", payload.Semantics.Degraded)
	}
}

func TestServeHealthPayloadDegradesForStaleRepoMutationActuatorStart(t *testing.T) {
	store := newReadinessTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-readiness-actuator-health"
		projectID   = "project-readiness-actuator-health"
		repoID      = "repo-main"
		queueID     = "queue-readiness-actuator-health"
		itemID      = "item-readiness-actuator-health"
	)
	seedReadinessWorkspaceAndAgents(t, ctx, store, workspaceID, []string{"lead-agent"})
	materializationDigest := "sha256:" + strings.Repeat("a", 64)
	startedPayload := fmt.Sprintf(`{
		"schema":"repo_mutation_actuator_started.v1",
		"workspace_id":%q,
		"project_id":%q,
		"repo_id":%q,
		"queue_id":%q,
		"item_id":%q,
		"target_checkout_id":"checkout-integration",
		"target_branch_name":"main",
		"activation_digest":"sha256:%s",
		"materialization_digest":%q,
		"materialization_authority_proof_digest":"sha256:%s"
	}`, workspaceID, projectID, repoID, queueID, itemID, strings.Repeat("b", 64), materializationDigest, strings.Repeat("c", 64))
	if _, err := store.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   sqlite.ProjectPatchQueueActuatorStartedEventType,
		EntityType:  "project_patch_queue_item",
		EntityID:    queueID + "/" + itemID,
		ActorType:   "system",
		ActorID:     sqlite.ProjectPatchQueueActuatorActorID,
		PayloadJSON: startedPayload,
		CreatedAt:   time.Now().UTC().Add(-sqlite.ProjectPatchQueueActuatorStartedStaleAfter - time.Second).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record actuator started event: %v", err)
	}

	payload := collectServeHealthPayload(ctx, app.Config{}, store, NewReadinessRegistry())
	if payload.RepoMutationActuator == nil || payload.RepoMutationActuator.State != "degraded" {
		t.Fatalf("expected degraded actuator health in serve payload, got %+v", payload.RepoMutationActuator)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected stale actuator start to degrade service payload, got %+v", payload)
	}
	if !strings.Contains(payload.Semantics.Degraded.Message, "repo mutation actuator journal degraded") {
		t.Fatalf("expected degraded semantics to mention actuator journal, got %q", payload.Semantics.Degraded.Message)
	}

	check := checkRepoMutationActuatorHealthFromDetails(map[string]any{"service": payload})
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected doctor to warn about stale actuator start, got %+v", check)
	}
	if !strings.Contains(check.Message, "stale_started=1") {
		t.Fatalf("expected doctor message to include stale count, got %q", check.Message)
	}
}

func TestRepoMutationActivationIgnoresTamperedPatchQueueProof(t *testing.T) {
	store := newReadinessTestStore(t)
	proof := store.ProjectPatchQueueDurabilityProof(context.Background())
	proof.Digest = strings.Repeat("0", 64)

	diag := collectRepoMutationActivationDiagnostics(&proof)
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "durable_patch_queue"); gate.Passed {
		t.Fatalf("tampered patch queue proof should not satisfy durable_patch_queue gate: %+v", gate)
	}
}

func TestRepoMutationActivationUsesControlledQueueCandidateEvidence(t *testing.T) {
	store := newReadinessTestStore(t)
	proof := store.ProjectPatchQueueDurabilityProof(context.Background())
	gitFixture := newReadinessGitFixture(t)
	baseSHA := gitFixture.BaseSHA
	headSHA := gitFixture.HeadSHA
	candidate := sqlite.ProjectRepoMutationActivationCandidate{
		QueueItem: sqlite.ProjectPatchQueueItemRecord{
			QueueID:           "patchq-main",
			ItemID:            "patchitem-branch-ready",
			WorkspaceID:       "ws-controlled",
			ProjectID:         "project-controlled",
			RepoID:            "repo-main",
			BranchID:          "branch-ready",
			ReviewDocKey:      "project.project-controlled.branch.branch-ready.review",
			RepoAuthorityMode: sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
			State:             sqlite.ProjectPatchQueueStateProposed,
			Pathset:           []string{"web/app.js"},
			BaseRef:           "main",
			BaseSHA:           baseSHA,
			HeadSHA:           headSHA,
			SubmittedBy:       "worker-agent",
			CreatedAt:         "2026-04-26T00:00:00Z",
			UpdatedAt:         "2026-04-26T00:00:00Z",
		},
		Branch: sqlite.ProjectBranchRecord{
			BranchID:   "branch-ready",
			BranchName: gitFixture.BranchName,
			AgentID:    "worker-agent",
			BaseBranch: "main",
			BaseSHA:    baseSHA,
			HeadSHA:    headSHA,
		},
		Checkout: sqlite.ProjectCheckoutRecord{
			CheckoutID: "checkout-worker",
			RepoID:     "repo-main",
			MachineID:  "developer-desktop",
			AgentID:    "worker-agent",
			LocalPath:  gitFixture.Path,
		},
	}

	diag := collectRepoMutationActivationDiagnosticsFromCandidate(&proof, candidate, true)
	if diag.Status != repoauthority.MutationActivationStatusBlocked || diag.MutationAllowed {
		t.Fatalf("controlled candidate must remain blocked/fail-closed, got %+v", diag)
	}
	if diag.Source != repoauthority.MutationActivationSourceDurableControlledQueueCandidate || diag.SourceError != "" {
		t.Fatalf("unexpected controlled candidate source metadata: %+v", diag)
	}
	if diag.Candidate == nil {
		t.Fatalf("expected controlled candidate summary in activation diagnostics")
	}
	if diag.Candidate.QueueID != "patchq-main" ||
		diag.Candidate.ItemID != "patchitem-branch-ready" ||
		diag.Candidate.BranchID != "branch-ready" ||
		diag.Candidate.BranchName != gitFixture.BranchName ||
		diag.Candidate.CheckoutID != "checkout-worker" ||
		diag.Candidate.BaseSHA != baseSHA ||
		diag.Candidate.HeadSHA != headSHA {
		t.Fatalf("unexpected controlled candidate summary: %+v", diag.Candidate)
	}
	if diag.WorktreeIdentity == nil {
		t.Fatalf("expected worktree identity readback in activation diagnostics")
	}
	if diag.WorktreeIdentity.ReadbackState != "ok" ||
		diag.WorktreeIdentity.ObservedBranchName != gitFixture.BranchName ||
		diag.WorktreeIdentity.ObservedHeadSHA != headSHA ||
		diag.WorktreeIdentity.ObservedDirtyState != "clean" {
		t.Fatalf("unexpected worktree readback: %+v", diag.WorktreeIdentity)
	}
	if diag.MutationBindingEvidence == nil {
		t.Fatalf("expected mutation binding evidence in activation diagnostics")
	}
	if diag.MutationBindingEvidence.WorkspaceID != "ws-controlled" ||
		diag.MutationBindingEvidence.PatchQueueID != "patchq-main" ||
		diag.MutationBindingEvidence.PatchQueueItemID != "patchitem-branch-ready" ||
		diag.MutationBindingEvidence.PatchQueueItemQueueID != "patchq-main" ||
		diag.MutationBindingEvidence.PatchQueueItemItemID != "patchitem-branch-ready" {
		t.Fatalf("unexpected mutation binding evidence identity: %+v", diag.MutationBindingEvidence)
	}
	for _, missing := range []string{"task_id", "session_id", "run_id", "capability_snapshot.id", "repo_lease_id", "operation_id", "patch_queue_item.operation_id"} {
		if !stringSliceContainsForReadinessTest(diag.MutationBindingEvidence.MissingRefs, missing) {
			t.Fatalf("expected mutation binding evidence to call out missing %s, got %+v", missing, diag.MutationBindingEvidence.MissingRefs)
		}
	}
	for _, name := range []string{"controlled_authority_mode", "controlled_context_mode", "durable_patch_queue", "canonical_worktree_identity"} {
		if gate := mutationActivationGateByNameForReadinessTest(t, diag, name); !gate.Passed {
			t.Fatalf("expected %s gate to pass from controlled candidate evidence, got %+v", name, gate)
		}
	}
	for _, name := range []string{"mutation_binding", "merge_admission_conflict_safe", "bounded_retry", "rollback_proven"} {
		if gate := mutationActivationGateByNameForReadinessTest(t, diag, name); gate.Passed {
			t.Fatalf("expected %s gate to remain blocked before live mutation verifier exists, got %+v", name, gate)
		}
	}
}

func TestRepoMutationActivationUsesDurableBindingRefsBeforeOperation(t *testing.T) {
	store := newReadinessTestStore(t)
	proof := store.ProjectPatchQueueDurabilityProof(context.Background())
	gitFixture := newReadinessGitFixture(t)
	baseSHA := gitFixture.BaseSHA
	headSHA := gitFixture.HeadSHA
	baseFileHashes := map[string]string{"web/app.js": "sha256:web"}
	contextDigest, err := repoauthority.Context{
		Mode:        repoauthority.ModeControlledQueue,
		WorkspaceID: "ws-controlled",
		TaskID:      "task-worker",
		SessionID:   "session-worker",
		RunID:       "run-worker",
		AgentID:     "worker-agent",
		Principal: repoauthority.PrincipalRef{
			Type: "agent",
			ID:   "worker-agent",
		},
		CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
			ID:     "cap-worker",
			Schema: "daemon_capability_snapshot.v1",
		},
		RepoRoot: gitFixture.Path,
		Base: repoauthority.BaseIdentity{
			Ref:        "main",
			TreeHash:   baseSHA,
			FileHashes: baseFileHashes,
		},
		Pathset: []string{"web/app.js"},
		Lease: repoauthority.LeaseRef{
			ID:   "lease-worker",
			Term: 11,
		},
		PatchQueue: repoauthority.PatchQueueRef{
			QueueID: "patchq-main",
			ItemID:  "patchitem-branch-ready",
		},
	}.Digest()
	if err != nil {
		t.Fatalf("compute controlled queue context digest: %v", err)
	}
	candidate := sqlite.ProjectRepoMutationActivationCandidate{
		QueueItem: sqlite.ProjectPatchQueueItemRecord{
			QueueID:                  "patchq-main",
			ItemID:                   "patchitem-branch-ready",
			WorkspaceID:              "ws-controlled",
			ProjectID:                "project-controlled",
			RepoID:                   "repo-main",
			BranchID:                 "branch-ready",
			ReviewDocKey:             "project.project-controlled.branch.branch-ready.review",
			RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
			State:                    sqlite.ProjectPatchQueueStateProposed,
			Pathset:                  []string{"web/app.js"},
			BaseRef:                  "main",
			BaseSHA:                  baseSHA,
			HeadSHA:                  headSHA,
			SubmittedBy:              "worker-agent",
			TaskID:                   "task-worker",
			SessionID:                "session-worker",
			RunID:                    "run-worker",
			AgentID:                  "worker-agent",
			PrincipalType:            "agent",
			PrincipalID:              "worker-agent",
			CapabilitySnapshotID:     "cap-worker",
			CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
			RepoRoot:                 gitFixture.Path,
			BaseTreeHash:             baseSHA,
			BaseFileHashes:           baseFileHashes,
			ContextDigest:            contextDigest,
			RepoLeaseID:              "lease-worker",
			LeaseTerm:                11,
			CreatedAt:                "2026-04-26T00:00:00Z",
			UpdatedAt:                "2026-04-26T00:00:00Z",
		},
		Branch: sqlite.ProjectBranchRecord{
			BranchID:   "branch-ready",
			BranchName: gitFixture.BranchName,
			AgentID:    "worker-agent",
			BaseBranch: "main",
			BaseSHA:    baseSHA,
			HeadSHA:    headSHA,
		},
		Checkout: sqlite.ProjectCheckoutRecord{
			CheckoutID: "checkout-worker",
			RepoID:     "repo-main",
			MachineID:  "developer-desktop",
			AgentID:    "worker-agent",
			LocalPath:  gitFixture.Path,
		},
	}

	diag := collectRepoMutationActivationDiagnosticsFromCandidate(&proof, candidate, true)
	if diag.MutationBindingEvidence == nil {
		t.Fatalf("expected mutation binding evidence")
	}
	evidence := diag.MutationBindingEvidence
	if evidence.ContextDigest != contextDigest ||
		evidence.PatchQueueContextDigest != contextDigest ||
		evidence.PatchQueueItemContext != contextDigest ||
		evidence.TaskID != "task-worker" ||
		evidence.SessionID != "session-worker" ||
		evidence.RunID != "run-worker" ||
		evidence.CapabilitySnapshotID != "cap-worker" ||
		evidence.RepoLeaseID != "lease-worker" ||
		evidence.PatchQueueItemRepoLeaseID != "lease-worker" {
		t.Fatalf("durable binding refs were not projected into diagnostics: %+v", evidence)
	}
	for _, noLongerMissing := range []string{"task_id", "session_id", "run_id", "capability_snapshot.id", "repo_lease_id", "lease_term", "patch_queue_item.context_digest", "patch_queue_item.repo_lease_id"} {
		if stringSliceContainsForReadinessTest(evidence.MissingRefs, noLongerMissing) {
			t.Fatalf("durable binding refs should satisfy %s, missing refs: %+v", noLongerMissing, evidence.MissingRefs)
		}
	}
	for _, stillMissing := range []string{"operation_id", "operation_kind", "patch_queue_item.operation_id", "patch_queue_item.operation_kind"} {
		if !stringSliceContainsForReadinessTest(evidence.MissingRefs, stillMissing) {
			t.Fatalf("operation refs must still block mutation binding before CAS evidence, missing refs: %+v", evidence.MissingRefs)
		}
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "mutation_binding"); gate.Passed {
		t.Fatalf("mutation binding must remain blocked until operation refs exist, got %+v", gate)
	}
}

func TestRepoMutationActivationUsesVerifiedOperationBindingBeforeCAS(t *testing.T) {
	store := newReadinessTestStore(t)
	proof := store.ProjectPatchQueueDurabilityProof(context.Background())
	gitFixture := newReadinessGitFixture(t)
	baseSHA := gitFixture.BaseSHA
	headSHA := gitFixture.HeadSHA
	baseFileHashes := map[string]string{"web/app.js": "sha256:web"}
	proposalContext := repoauthority.Context{
		Mode:        repoauthority.ModeControlledQueue,
		WorkspaceID: "ws-controlled",
		TaskID:      "task-worker",
		SessionID:   "session-worker",
		RunID:       "run-worker",
		AgentID:     "worker-agent",
		Principal: repoauthority.PrincipalRef{
			Type: "agent",
			ID:   "worker-agent",
		},
		CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
			ID:     "cap-worker",
			Schema: "daemon_capability_snapshot.v1",
		},
		RepoRoot: gitFixture.Path,
		Base: repoauthority.BaseIdentity{
			Ref:        "main",
			TreeHash:   baseSHA,
			FileHashes: baseFileHashes,
		},
		Pathset: []string{"web/app.js"},
		Lease: repoauthority.LeaseRef{
			ID:   "lease-worker",
			Term: 11,
		},
		PatchQueue: repoauthority.PatchQueueRef{
			QueueID: "patchq-main",
			ItemID:  "patchitem-branch-ready",
		},
	}
	proposalDigest, err := proposalContext.Digest()
	if err != nil {
		t.Fatalf("compute controlled queue proposal digest: %v", err)
	}
	operationContext := proposalContext
	operationContext.Operation = repoauthority.OperationRef{
		ID:   "op-worker-apply",
		Kind: "repo_patch_apply",
	}
	operationDigest, err := operationContext.Digest()
	if err != nil {
		t.Fatalf("compute operation binding digest: %v", err)
	}
	leaseContext := operationContext
	leaseContext.Lease = repoauthority.LeaseRef{}
	leaseContext.PatchQueue = repoauthority.PatchQueueRef{}
	leaseContext.Operation = repoauthority.OperationRef{}
	leaseDigest, err := leaseContext.Digest()
	if err != nil {
		t.Fatalf("compute operation lease digest: %v", err)
	}
	candidate := sqlite.ProjectRepoMutationActivationCandidate{
		QueueItem: sqlite.ProjectPatchQueueItemRecord{
			QueueID:                     "patchq-main",
			ItemID:                      "patchitem-branch-ready",
			WorkspaceID:                 "ws-controlled",
			ProjectID:                   "project-controlled",
			RepoID:                      "repo-main",
			BranchID:                    "branch-ready",
			ReviewDocKey:                "project.project-controlled.branch.branch-ready.review",
			RepoAuthorityMode:           sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
			State:                       sqlite.ProjectPatchQueueStateClaimed,
			Attempt:                     1,
			MaxAttempts:                 1,
			Pathset:                     []string{"web/app.js"},
			PathsetJSON:                 `["web/app.js"]`,
			BaseRef:                     "main",
			BaseSHA:                     baseSHA,
			HeadSHA:                     headSHA,
			SubmittedBy:                 "worker-agent",
			TaskID:                      "task-worker",
			SessionID:                   "session-worker",
			RunID:                       "run-worker",
			AgentID:                     "worker-agent",
			PrincipalType:               "agent",
			PrincipalID:                 "worker-agent",
			CapabilitySnapshotID:        "cap-worker",
			CapabilitySnapshotSchema:    "daemon_capability_snapshot.v1",
			RepoRoot:                    gitFixture.Path,
			BaseTreeHash:                baseSHA,
			BaseFileHashes:              baseFileHashes,
			ContextDigest:               proposalDigest,
			RepoLeaseID:                 "lease-worker",
			LeaseTerm:                   11,
			OperationID:                 "op-worker-apply",
			OperationKind:               "repo_patch_apply",
			OperationBindingSchema:      sqlite.ProjectPatchQueueOperationBindingSchema,
			OperationBindingAccepted:    true,
			OperationContextDigest:      operationDigest,
			OperationLeaseContextDigest: leaseDigest,
			OperationMutationPaths:      []string{"web/app.js"},
			OperationMutationPathsJSON:  `["web/app.js"]`,
			OperationBoundBy:            "integrator-agent",
			OperationBoundAt:            "2026-04-26T00:00:00Z",
			CreatedAt:                   "2026-04-26T00:00:00Z",
			UpdatedAt:                   "2026-04-26T00:00:00Z",
		},
		Branch: sqlite.ProjectBranchRecord{
			BranchID:   "branch-ready",
			BranchName: gitFixture.BranchName,
			AgentID:    "worker-agent",
			BaseBranch: "main",
			BaseSHA:    baseSHA,
			HeadSHA:    headSHA,
		},
		Checkout: sqlite.ProjectCheckoutRecord{
			CheckoutID: "checkout-worker",
			RepoID:     "repo-main",
			MachineID:  "developer-desktop",
			AgentID:    "worker-agent",
			LocalPath:  gitFixture.Path,
		},
	}

	diag := collectRepoMutationActivationDiagnosticsFromCandidate(&proof, candidate, true)
	if diag.Status != repoauthority.MutationActivationStatusBlocked || diag.MutationAllowed {
		t.Fatalf("operation binding plus retry bounds must not enable full mutation before CAS/rollback, got %+v", diag)
	}
	evidence := diag.MutationBindingEvidence
	if evidence == nil || !evidence.Ready || evidence.OperationID != "op-worker-apply" || evidence.PatchQueueItemOperationID != "op-worker-apply" {
		t.Fatalf("expected operation binding refs to satisfy mutation binding evidence, got %+v", evidence)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "mutation_binding"); !gate.Passed {
		t.Fatalf("expected mutation_binding to pass after verified operation binding, got %+v", gate)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "bounded_retry"); !gate.Passed {
		t.Fatalf("expected bounded_retry to pass from durable retry bounds, got %+v", gate)
	}
	corruptRetryCandidate := candidate
	corruptRetryCandidate.QueueItem.Attempt = 2
	corruptRetryCandidate.QueueItem.MaxAttempts = 1
	corruptRetryDiag := collectRepoMutationActivationDiagnosticsFromCandidate(&proof, corruptRetryCandidate, true)
	if gate := mutationActivationGateByNameForReadinessTest(t, corruptRetryDiag, "bounded_retry"); gate.Passed {
		t.Fatalf("expected corrupt retry bounds to block bounded_retry, got %+v", gate)
	}
	for _, name := range []string{"merge_admission_conflict_safe", "rollback_proven"} {
		if gate := mutationActivationGateByNameForReadinessTest(t, diag, name); gate.Passed {
			t.Fatalf("expected %s to remain blocked before live CAS/rollback, got %+v", name, gate)
		}
	}

	casResult := repoauthority.EvaluateCASPatchApply(repoauthority.CASPatchApplyInput{
		Context: operationContext,
		CurrentFileHashes: map[string]string{
			"web/app.js": "sha256:web",
		},
		CandidateFileHashes: map[string]string{
			"web/app.js": "sha256:new-web",
		},
	})
	casResultJSON, err := json.Marshal(casResult)
	if err != nil {
		t.Fatalf("marshal CAS result: %v", err)
	}
	testEvidence := repoauthority.PatchQueueTestEvidence{
		Schema:       repoauthority.PatchQueueTestEvidenceSchemaVersion,
		Name:         "unit",
		Command:      "go test ./...",
		Status:       repoauthority.PatchQueueTestStatusPassed,
		ExitCode:     0,
		OutputDigest: "sha256:" + strings.Repeat("4", 64),
	}
	testEvidenceJSON, err := json.Marshal(testEvidence)
	if err != nil {
		t.Fatalf("marshal test evidence: %v", err)
	}
	candidate.QueueItem.CASEvidenceSchema = sqlite.ProjectPatchQueueCASEvidenceSchema
	candidate.QueueItem.CASEvidenceAccepted = true
	candidate.QueueItem.CASStatus = repoauthority.CASPatchStatusApplied
	candidate.QueueItem.CASPatchDigest = casResult.PatchDigest
	candidate.QueueItem.CASEvaluationDigest = repoauthority.PatchQueueCASEvaluationDigest(casResult)
	candidate.QueueItem.CASResult = casResult
	candidate.QueueItem.CASResultJSON = string(casResultJSON)
	candidate.QueueItem.CASTestEvidence = testEvidence
	candidate.QueueItem.CASTestEvidenceJSON = string(testEvidenceJSON)
	candidate.QueueItem.CASTestEvidenceDigest = repoauthority.PatchQueueTestEvidenceDigest(testEvidence)
	candidate.QueueItem.CASRecordedBy = "integrator-agent"
	candidate.QueueItem.CASRecordedAt = "2026-04-26T00:00:01Z"

	diag = collectRepoMutationActivationDiagnosticsFromCandidate(&proof, candidate, true)
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "merge_admission_conflict_safe"); !gate.Passed {
		t.Fatalf("expected merge admission to pass with verified CAS evidence, got %+v", gate)
	}
	if diag.Status != repoauthority.MutationActivationStatusBlocked || diag.MutationAllowed {
		t.Fatalf("CAS evidence must still leave full mutation blocked before retry/rollback, got %+v", diag)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "bounded_retry"); !gate.Passed {
		t.Fatalf("expected bounded_retry to remain passed after CAS evidence, got %+v", gate)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "rollback_proven"); gate.Passed {
		t.Fatalf("expected rollback_proven to remain blocked after CAS evidence, got %+v", gate)
	}

	appliedItem := repoMutationPatchQueueItemFromCandidate(candidate)
	rollbackEvidence, err := repoauthority.NormalizePatchQueueRollbackEvidence(repoauthority.PatchQueueRollback{
		Reason:                     "prove rollback before mutation activation",
		SourcePatchDigest:          casResult.PatchDigest,
		VerificationCommand:        "go test ./...",
		VerificationStatus:         repoauthority.PatchQueueTestStatusPassed,
		VerificationExitCode:       0,
		VerificationOutputDigest:   "sha256:" + strings.Repeat("5", 64),
		VerificationOutputSummary:  "ok",
		VerificationDurationMillis: 10,
		RollbackPaths: []repoauthority.PatchQueueRollbackPath{
			{Path: "web/app.js", SourceBaseHash: "sha256:web", SourceAppliedHash: "sha256:new-web", RollbackCandidateHash: "sha256:web"},
		},
	}, appliedItem, repoauthority.OperationRef{ID: "op-worker-rollback", Kind: "repo_patch_apply"}, time.Date(2026, 4, 26, 0, 0, 2, 0, time.UTC))
	if err != nil {
		t.Fatalf("normalize rollback evidence: %v", err)
	}
	rollbackJSON, err := json.Marshal(rollbackEvidence)
	if err != nil {
		t.Fatalf("marshal rollback evidence: %v", err)
	}
	candidate.QueueItem.RollbackEvidenceSchema = sqlite.ProjectPatchQueueRollbackEvidenceSchema
	candidate.QueueItem.RollbackEvidenceAccepted = true
	candidate.QueueItem.RollbackEvidence = rollbackEvidence
	candidate.QueueItem.RollbackEvidenceJSON = string(rollbackJSON)
	candidate.QueueItem.RollbackEvidenceDigest = repoauthority.PatchQueueRollbackEvidenceDigest(rollbackEvidence)
	candidate.QueueItem.RollbackRecordedBy = "integrator-agent"
	candidate.QueueItem.RollbackRecordedAt = rollbackEvidence.RecordedAt

	diag = collectRepoMutationActivationDiagnosticsFromCandidate(&proof, candidate, true)
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "rollback_proven"); !gate.Passed {
		t.Fatalf("expected rollback_proven to pass with durable rollback evidence, got %+v", gate)
	}
	if diag.Status != repoauthority.MutationActivationStatusBlocked || diag.MutationAllowed {
		t.Fatalf("durable rollback proof must still leave mutation activation fail-closed until live verifier exists, got %+v", diag)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "live_mutation_verifier_enabled"); gate.Passed {
		t.Fatalf("live verifier gate must remain disabled in health-facing diagnostics, got %+v", gate)
	}
	if diag.RollbackProofEvidence == nil || !diag.RollbackProofEvidence.Ready || diag.RollbackProofEvidence.RollbackEvidenceDigest == "" {
		t.Fatalf("expected digest-backed rollback proof evidence, got %+v", diag.RollbackProofEvidence)
	}

	appliedItem = repoMutationPatchQueueItemFromCandidate(candidate)
	reviewerAdvisory := repoauthority.PatchQueueReviewerAdvisory{
		Schema:                 repoauthority.PatchQueueReviewerAdvisorySchema,
		Mode:                   repoauthority.MutationActivationReviewerMeshAdvisoryOnly,
		Verdict:                repoauthority.PatchQueueReviewerAdvisoryVerdictReviewed,
		ReviewerID:             "reviewer-agent",
		ReviewDocKey:           appliedItem.ReviewDocKey,
		OperationID:            appliedItem.OperationID,
		OperationKind:          appliedItem.OperationKind,
		CASPatchDigest:         appliedItem.CASPatchDigest,
		CASEvaluationDigest:    appliedItem.CASEvaluationDigest,
		RollbackEvidenceDigest: appliedItem.RollbackEvidenceDigest,
		Summary:                "reviewed for live verifier transition",
		RecordedAt:             "2026-04-26T00:00:03Z",
	}
	if err := repoauthority.ValidatePatchQueueReviewerAdvisory(reviewerAdvisory, appliedItem); err != nil {
		t.Fatalf("validate reviewer advisory: %v", err)
	}
	reviewerAdvisoryJSON, err := json.Marshal(reviewerAdvisory)
	if err != nil {
		t.Fatalf("marshal reviewer advisory: %v", err)
	}
	candidate.QueueItem.ReviewerAdvisorySchema = sqlite.ProjectPatchQueueReviewerAdvisorySchema
	candidate.QueueItem.ReviewerAdvisoryAccepted = true
	candidate.QueueItem.ReviewerAdvisory = reviewerAdvisory
	candidate.QueueItem.ReviewerAdvisoryJSON = string(reviewerAdvisoryJSON)
	candidate.QueueItem.ReviewerAdvisoryDigest = repoauthority.PatchQueueReviewerAdvisoryDigest(reviewerAdvisory)
	candidate.QueueItem.ReviewerRecordedBy = "reviewer-agent"
	candidate.QueueItem.ReviewerRecordedAt = reviewerAdvisory.RecordedAt

	appliedItem = repoMutationPatchQueueItemFromCandidate(candidate)
	operatorEnablement := repoauthority.PatchQueueOperatorEnablement{
		Schema:                 repoauthority.PatchQueueOperatorEnablementSchema,
		Scope:                  repoauthority.MutationActivationOperatorEnablementScope,
		Enabled:                true,
		EnabledBy:              "operator-1",
		EnabledAt:              "2026-04-26T00:00:04Z",
		Reason:                 "enable live verifier transition fixture",
		WorkspaceID:            appliedItem.WorkspaceID,
		ProjectID:              appliedItem.ProjectID,
		QueueID:                appliedItem.QueueID,
		ItemID:                 appliedItem.ItemID,
		OperationID:            appliedItem.OperationID,
		CASPatchDigest:         appliedItem.CASPatchDigest,
		RollbackEvidenceDigest: appliedItem.RollbackEvidenceDigest,
		ReviewerAdvisoryDigest: appliedItem.ReviewerAdvisoryDigest,
	}
	if err := repoauthority.ValidatePatchQueueOperatorEnablement(operatorEnablement, appliedItem, reviewerAdvisory); err != nil {
		t.Fatalf("validate operator enablement: %v", err)
	}
	operatorEnablementJSON, err := json.Marshal(operatorEnablement)
	if err != nil {
		t.Fatalf("marshal operator enablement: %v", err)
	}
	candidate.QueueItem.OperatorEnablementSchema = sqlite.ProjectPatchQueueOperatorEnablementSchema
	candidate.QueueItem.OperatorEnablementAccepted = true
	candidate.QueueItem.OperatorEnablement = operatorEnablement
	candidate.QueueItem.OperatorEnablementJSON = string(operatorEnablementJSON)
	candidate.QueueItem.OperatorEnablementDigest = repoauthority.PatchQueueOperatorEnablementDigest(operatorEnablement)
	candidate.QueueItem.OperatorEnabledBy = "operator-1"
	candidate.QueueItem.OperatorEnabledAt = operatorEnablement.EnabledAt

	diag = collectRepoMutationActivationDiagnosticsFromCandidate(&proof, candidate, true)
	if diag.Status != repoauthority.MutationActivationStatusBlocked || diag.MutationAllowed {
		t.Fatalf("complete evidence must remain blocked before explicit live verifier env, got %+v", diag)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "operator_enablement_recorded"); !gate.Passed {
		t.Fatalf("expected operator enablement gate to pass with durable operator evidence, got %+v", gate)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "live_mutation_verifier_enabled"); gate.Passed {
		t.Fatalf("live verifier gate must remain disabled without env, got %+v", gate)
	}

	t.Setenv("RHIZOME_REPO_MUTATION_LIVE_VERIFIER", "1")
	diag = collectRepoMutationActivationDiagnosticsFromCandidate(&proof, candidate, true)
	if diag.Status != repoauthority.MutationActivationStatusBlocked || diag.MutationAllowed {
		t.Fatalf("complete evidence plus explicit live verifier env must remain mutation-blocked before actuator, got %+v", diag)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "live_mutation_verifier_enabled"); !gate.Passed {
		t.Fatalf("expected live verifier gate to pass with explicit env, got %+v", gate)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "materialization_preflight_verified"); gate.Passed {
		t.Fatalf("materialization preflight must stay blocked until durable patch bytes exist, got %+v", gate)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "live_mutation_actuator_enabled"); gate.Passed {
		t.Fatalf("live actuator gate must remain disabled before actuator implementation, got %+v", gate)
	}
	if err := repoauthority.VerifyMutationActivationGateResult(diag); err != nil {
		t.Fatalf("VerifyMutationActivationGateResult: %v", err)
	}
	dryRun := collectRepoMutationActuatorDryRunDiagnostics(diag)
	if dryRun.Status != repoauthority.MutationActuatorDryRunStatusBlocked {
		t.Fatalf("complete evidence plus live verifier should stay blocked until materialization exists, got %+v", dryRun)
	}
	if !dryRun.VerifierReady || dryRun.ActuatorEnabled || dryRun.WouldMutate || dryRun.MutationExecuted {
		t.Fatalf("dry-run actuator boundary must remain verifier-aware without mutation, got %+v", dryRun)
	}
	if !strings.Contains(strings.Join(dryRun.BlockingReasons, "\n"), "materialization_preflight_verified: patch materialization is required") {
		t.Fatalf("dry-run must expose missing materialization blocker, got %+v", dryRun.BlockingReasons)
	}
	if err := repoauthority.VerifyMutationActuatorDryRunResult(dryRun); err != nil {
		t.Fatalf("VerifyMutationActuatorDryRunResult: %v", err)
	}
	actuatorCheck := checkRepoMutationActuatorDryRunFromDetails(map[string]any{
		"service": serviceHealthPayload{
			Status:             "ok",
			RepoMutation:       diag,
			RepoMutationDryRun: dryRun,
		},
	})
	if actuatorCheck.Status != doctorStatusPass {
		t.Fatalf("expected blocked actuator dry-run to pass doctor consistency check, got %+v", actuatorCheck)
	}
	if !strings.Contains(actuatorCheck.Message, "fail-closed") || !strings.Contains(actuatorCheck.Message, "materialization_preflight_verified") {
		t.Fatalf("expected doctor dry-run message to expose materialization fail-closed boundary, got %q", actuatorCheck.Message)
	}
	if got, _ := actuatorCheck.Details["live_scope"].(string); got != repoauthority.MutationActuatorLiveScopeAddModify {
		t.Fatalf("expected doctor dry-run details to expose live scope, got %+v", actuatorCheck.Details)
	}
	if got, _ := actuatorCheck.Details["allowed_change_kinds"].([]string); !reflect.DeepEqual(got, []string{repoauthority.CASPatchChangeModify, repoauthority.CASPatchChangeAdd}) {
		t.Fatalf("expected doctor dry-run details to expose allowed change kinds, got %+v", actuatorCheck.Details)
	}
	if got, _ := actuatorCheck.Details["observed_change_kinds"].([]string); len(got) != 0 {
		t.Fatalf("expected doctor dry-run details to expose empty observed change kinds before materialization, got %+v", actuatorCheck.Details)
	}
	mismatchedDryRun := collectRepoMutationActuatorDryRunDiagnostics(collectRepoMutationActivationDiagnostics())
	mismatchCheck := checkRepoMutationActuatorDryRunFromDetails(map[string]any{
		"service": serviceHealthPayload{
			Status:             "ok",
			RepoMutation:       diag,
			RepoMutationDryRun: mismatchedDryRun,
		},
	})
	if mismatchCheck.Status != doctorStatusFail {
		t.Fatalf("expected mismatched actuator dry-run activation digest to fail doctor, got %+v", mismatchCheck)
	}
	if !strings.Contains(mismatchCheck.Message, "does not match") {
		t.Fatalf("expected dry-run mismatch message, got %q", mismatchCheck.Message)
	}

	corruptRollbackCandidate := candidate
	corruptRollbackCandidate.QueueItem.RollbackEvidence.SourcePatchDigest = "sha256:" + strings.Repeat("6", 64)
	corruptRollbackCandidate.QueueItem.RollbackEvidenceDigest = repoauthority.PatchQueueRollbackEvidenceDigest(corruptRollbackCandidate.QueueItem.RollbackEvidence)
	corruptRollbackDiag := collectRepoMutationActivationDiagnosticsFromCandidate(&proof, corruptRollbackCandidate, true)
	if gate := mutationActivationGateByNameForReadinessTest(t, corruptRollbackDiag, "rollback_proven"); gate.Passed {
		t.Fatalf("expected corrupt rollback source patch digest to block rollback_proven, got %+v", gate)
	}
}

func TestRepoMutationActivationBlocksDirtyWorktreeReadback(t *testing.T) {
	store := newReadinessTestStore(t)
	proof := store.ProjectPatchQueueDurabilityProof(context.Background())
	gitFixture := newReadinessGitFixture(t)
	if err := os.WriteFile(filepath.Join(gitFixture.Path, "dirty.txt"), []byte("uncommitted\n"), 0o644); err != nil {
		t.Fatalf("dirty fixture worktree: %v", err)
	}
	candidate := sqlite.ProjectRepoMutationActivationCandidate{
		QueueItem: sqlite.ProjectPatchQueueItemRecord{
			QueueID:           "patchq-main",
			ItemID:            "patchitem-branch-ready",
			WorkspaceID:       "ws-controlled",
			ProjectID:         "project-controlled",
			RepoID:            "repo-main",
			BranchID:          "branch-ready",
			ReviewDocKey:      "project.project-controlled.branch.branch-ready.review",
			RepoAuthorityMode: sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
			State:             sqlite.ProjectPatchQueueStateProposed,
			Pathset:           []string{"web/app.js"},
			BaseRef:           "main",
			BaseSHA:           gitFixture.BaseSHA,
			HeadSHA:           gitFixture.HeadSHA,
			SubmittedBy:       "worker-agent",
			CreatedAt:         "2026-04-26T00:00:00Z",
			UpdatedAt:         "2026-04-26T00:00:00Z",
		},
		Branch: sqlite.ProjectBranchRecord{
			BranchID:   "branch-ready",
			BranchName: gitFixture.BranchName,
			AgentID:    "worker-agent",
			BaseBranch: "main",
			BaseSHA:    gitFixture.BaseSHA,
			HeadSHA:    gitFixture.HeadSHA,
		},
		Checkout: sqlite.ProjectCheckoutRecord{
			CheckoutID: "checkout-worker",
			RepoID:     "repo-main",
			MachineID:  "developer-desktop",
			AgentID:    "worker-agent",
			LocalPath:  gitFixture.Path,
		},
	}

	diag := collectRepoMutationActivationDiagnosticsFromCandidate(&proof, candidate, true)
	if diag.WorktreeIdentity == nil || diag.WorktreeIdentity.ReadbackState != "dirty" {
		t.Fatalf("expected dirty worktree readback, got %+v", diag.WorktreeIdentity)
	}
	if gate := mutationActivationGateByNameForReadinessTest(t, diag, "canonical_worktree_identity"); gate.Passed {
		t.Fatalf("dirty worktree must block canonical_worktree_identity, got %+v", gate)
	}
}

func TestDoctorProjectPatchQueueDurabilityPassesStorageBackedProof(t *testing.T) {
	store := newReadinessTestStore(t)
	proof := store.ProjectPatchQueueDurabilityProof(context.Background())
	check := checkProjectPatchQueueDurabilityFromDetails(map[string]any{
		"service": serviceHealthPayload{
			Status:     "ok",
			PatchQueue: &proof,
		},
	})
	if check.Status != doctorStatusPass {
		t.Fatalf("expected durable patch queue proof to pass doctor check, got %+v", check)
	}
	if got := check.Details["contract"]; got != sqlite.ProjectPatchQueueDurabilityProofContract {
		t.Fatalf("expected patch queue durability contract in doctor details, got %+v", check.Details)
	}
}

func TestDoctorProjectPatchQueueDurabilityFailsUnsupportedContract(t *testing.T) {
	proof := sqlite.ProjectPatchQueueDurabilityProof{
		Contract: "project_patch_queue_durability_proof.v0",
		State:    "ok",
		Durable:  true,
	}
	check := checkProjectPatchQueueDurabilityFromDetails(map[string]any{
		"service": serviceHealthPayload{
			Status:     "ok",
			PatchQueue: &proof,
		},
	})
	if check.Status != doctorStatusFail {
		t.Fatalf("expected unsupported patch queue durability contract to fail, got %+v", check)
	}
}

func TestDoctorProjectPatchQueueDurabilityFailsTamperedProof(t *testing.T) {
	store := newReadinessTestStore(t)
	proof := store.ProjectPatchQueueDurabilityProof(context.Background())
	proof.Durable = false

	check := checkProjectPatchQueueDurabilityFromDetails(map[string]any{
		"service": serviceHealthPayload{
			Status:     "ok",
			PatchQueue: &proof,
		},
	})
	if check.Status != doctorStatusFail {
		t.Fatalf("expected tampered patch queue durability proof to fail, got %+v", check)
	}
	if !strings.Contains(check.Message, "inconsistent") {
		t.Fatalf("expected consistency failure message, got %q", check.Message)
	}
}

func newReadinessTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	store, err := sqlite.NewStore(filepath.Join(t.TempDir(), "rhizome-readiness-test.db"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.ApplyMigrations(context.Background()); err != nil {
		_ = store.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func seedReadinessBranchClaimForReady(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, agentID, checkoutID, branchID, taskID, scopeJSON string) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate readiness branch task graph: %v", err)
	}
	writeScopeHints := readinessWriteScopeHintPathsForTest(scopeJSON)
	taskRequirementsJSON := ""
	if len(writeScopeHints) > 0 {
		taskRequirementsJSON = `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Readiness controlled queue candidate",
		Description:          "Synthetic task backing the readiness READY_FOR_REVIEW branch.",
		ProjectID:            projectID,
		TaskKind:             "EXECUTION",
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: taskRequirementsJSON,
		WriteScopeHints:      writeScopeHints,
	}, graph); err != nil {
		t.Fatalf("create readiness branch task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach readiness branch task: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO task_claims(
  task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at,
  project_role_id, repo_id, checkout_id, branch_id, write_scope_json
) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, '', ?, ?, ?, ?)`,
		taskID, workspaceID, agentID, model.TaskClaimStatusClaimed, "readiness branch claim", now, now, repoID, checkoutID, branchID, scopeJSON); err != nil {
		t.Fatalf("seed readiness branch claim: %v", err)
	}
}

func readinessWriteScopeHintPathsForTest(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || !json.Valid([]byte(raw)) {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil
	}
	var paths []string
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case string:
			if path := strings.TrimSpace(typed); path != "" {
				paths = append(paths, path)
			}
		case []any:
			for _, item := range typed {
				walk(item)
			}
		}
	}
	if object, ok := decoded.(map[string]any); ok {
		for _, key := range []string{"paths", "files", "path_prefixes", "write_paths", "scopes"} {
			walk(object[key])
		}
		return paths
	}
	walk(decoded)
	return paths
}

func seedReadinessStoreBackedMutationDryRunCandidate(t *testing.T, ctx context.Context, store *sqlite.Store, gitFixture readinessGitFixture) sqlite.ProjectPatchQueueItemRecord {
	t.Helper()
	const (
		workspaceID = "ws-readiness-mutation-dry-run"
		projectID   = "project-readiness-mutation-dry-run"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		branchID    = "branch-ready"
		taskID      = "task-readiness"
		reviewKey   = "project.project-readiness-mutation-dry-run.branch.branch-ready.review"
	)
	pathsetJSON := `{"paths":["web-app.txt"]}`
	baseFileHashes := map[string]string{"web-app.txt": "sha256:base-web"}
	candidateContent := "head\n"
	candidateHash := repoauthority.PatchMaterializationContentDigest(candidateContent)
	candidateFileHashes := map[string]string{"web-app.txt": candidateHash}

	seedReadinessWorkspaceAndAgents(t, ctx, store, workspaceID, []string{leadID, workerID})
	createReadinessProject(t, ctx, store, workspaceID, projectID, leadID)
	claimReadinessProjectLead(t, ctx, store, workspaceID, projectID, leadID)
	upsertReadinessRepository(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerReadinessCheckout(t, ctx, store, workspaceID, projectID, repoID, workerID, gitFixture)
	registerReadinessIntegrationCheckout(t, ctx, store, workspaceID, projectID, repoID, leadID, gitFixture)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Readiness Controlled Branch Review",
		Content:     "# Readiness Controlled Branch Review\n\nStore-backed dry-run actuator smoke.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write readiness review doc: %v", err)
	}
	seedReadinessBranchClaimForReady(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, branchID, taskID, pathsetJSON)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branchID,
		BranchName:            gitFixture.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               gitFixture.BaseSHA,
		HeadSHA:               gitFixture.HeadSHA,
		WriteScopeJSON:        pathsetJSON,
		ReviewDocKey:          reviewKey,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register readiness branch: %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-readiness",
		RunID:                    "run-readiness",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-readiness",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 gitFixture.Path,
		BaseTreeHash:             gitFixture.BaseSHA,
		BaseFileHashes:           baseFileHashes,
		RepoLeaseID:              "lease-readiness",
		LeaseTerm:                5,
		ActorID:                  workerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit readiness controlled patch queue item: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          600,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim readiness controlled patch queue item: %v", err)
	}
	bound, _, err := store.BindProjectPatchQueueMutationOperationWithEvent(ctx, sqlite.ProjectPatchQueueOperationBindInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		OperationID:           "op-readiness-apply",
		OperationKind:         sqlite.ProjectPatchQueueOperationKindRepoPatchApply,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.operation_bind", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.operation_bind",
	})
	if err != nil {
		t.Fatalf("bind readiness operation evidence: %v", err)
	}
	operationContext := repoauthority.Context{
		Mode:        repoauthority.ModeControlledQueue,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		SessionID:   "session-readiness",
		RunID:       "run-readiness",
		AgentID:     workerID,
		Principal:   repoauthority.PrincipalRef{Type: "agent", ID: workerID},
		CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
			ID:     "cap-readiness",
			Schema: "daemon_capability_snapshot.v1",
		},
		RepoRoot: gitFixture.Path,
		Base: repoauthority.BaseIdentity{
			Ref:        "main",
			TreeHash:   gitFixture.BaseSHA,
			FileHashes: baseFileHashes,
		},
		Pathset: []string{"web-app.txt"},
		Lease: repoauthority.LeaseRef{
			ID:   "lease-readiness",
			Term: 5,
		},
		PatchQueue: repoauthority.PatchQueueRef{
			QueueID: item.QueueID,
			ItemID:  item.ItemID,
		},
		Operation: repoauthority.OperationRef{ID: bound.OperationID, Kind: bound.OperationKind},
	}
	appliedCAS := repoauthority.EvaluateCASPatchApply(repoauthority.CASPatchApplyInput{
		Context:             operationContext,
		CurrentFileHashes:   baseFileHashes,
		CandidateFileHashes: candidateFileHashes,
	})
	testEvidence := repoauthority.PatchQueueTestEvidence{
		Schema:       repoauthority.PatchQueueTestEvidenceSchemaVersion,
		Name:         "unit",
		Command:      "go test ./...",
		Status:       repoauthority.PatchQueueTestStatusPassed,
		ExitCode:     0,
		OutputDigest: "sha256:" + strings.Repeat("2", 64),
	}
	casRecorded, _, err := store.RecordProjectPatchQueueCASEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueCASRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		CASResult:             appliedCAS,
		TestEvidence:          testEvidence,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.cas_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.cas_record",
	})
	if err != nil {
		t.Fatalf("record readiness CAS evidence: %v", err)
	}
	rollbackItem := repoauthority.PatchQueueItem{
		Schema:              repoauthority.PatchQueueItemSchemaVersion,
		ID:                  item.QueueID + "/" + item.ItemID,
		QueueID:             item.QueueID,
		ItemID:              item.ItemID,
		State:               repoauthority.PatchQueueStateApplied,
		Attempt:             casRecorded.Attempt,
		MaxAttempts:         casRecorded.MaxAttempts,
		ContextDigest:       casRecorded.ContextDigest,
		RepoLeaseID:         casRecorded.RepoLeaseID,
		LeaseTerm:           casRecorded.LeaseTerm,
		Pathset:             []string{"web-app.txt"},
		CASResult:           appliedCAS,
		CASPatchDigest:      appliedCAS.PatchDigest,
		CASEvaluationDigest: repoauthority.PatchQueueCASEvaluationDigest(appliedCAS),
		TestEvidence:        testEvidence,
		TestEvidenceDigest:  repoauthority.PatchQueueTestEvidenceDigest(testEvidence),
		OperationID:         bound.OperationID,
		OperationKind:       bound.OperationKind,
	}
	rollbackEvidence, err := repoauthority.NormalizePatchQueueRollbackEvidence(repoauthority.PatchQueueRollback{
		Reason:                     "prove rollback before dry-run actuator readiness",
		SourcePatchDigest:          appliedCAS.PatchDigest,
		VerificationCommand:        "go test ./...",
		VerificationStatus:         repoauthority.PatchQueueTestStatusPassed,
		VerificationExitCode:       0,
		VerificationOutputDigest:   "sha256:" + strings.Repeat("4", 64),
		VerificationOutputSummary:  "ok",
		VerificationDurationMillis: 25,
		RollbackPaths: []repoauthority.PatchQueueRollbackPath{
			{Path: "web-app.txt", SourceBaseHash: "sha256:base-web", SourceAppliedHash: candidateHash, RollbackCandidateHash: "sha256:base-web"},
		},
	}, rollbackItem, repoauthority.OperationRef{ID: "op-readiness-rollback", Kind: sqlite.ProjectPatchQueueOperationKindRepoPatchApply}, time.Date(2026, 4, 26, 0, 0, 2, 0, time.UTC))
	if err != nil {
		t.Fatalf("normalize readiness rollback evidence: %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueRollbackEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueRollbackRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		RollbackEvidence:      rollbackEvidence,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.rollback_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.rollback_record",
	}); err != nil {
		t.Fatalf("record readiness rollback evidence: %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueReviewerAdvisoryWithEvent(ctx, sqlite.ProjectPatchQueueReviewerAdvisoryRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ReviewerAdvisory:      repoauthority.PatchQueueReviewerAdvisory{Summary: "reviewed store-backed CAS and rollback evidence"},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.reviewer_advisory_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.reviewer_advisory_record",
	}); err != nil {
		t.Fatalf("record readiness reviewer advisory: %v", err)
	}
	operatorRecorded, _, err := store.RecordProjectPatchQueueOperatorEnablementWithEvent(ctx, sqlite.ProjectPatchQueueOperatorEnablementRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		OperatorEnablement:    repoauthority.PatchQueueOperatorEnablement{Enabled: true, Reason: "operator explicitly enabled dry-run actuator smoke"},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               "operator-human",
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("cli.project.patch_queue.operator_enablement_record", "cli_local", workspaceID, "human", "operator-human"),
		PromptContextSurface:  "cli.project.patch_queue.operator_enablement_record",
	})
	if err != nil {
		t.Fatalf("record readiness operator enablement: %v", err)
	}
	if _, ok, err := store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select seeded readiness activation candidate: %v", err)
	} else if !ok {
		t.Fatalf("expected seeded readiness activation candidate")
	}
	return operatorRecorded
}

func seedReadinessWorkspaceAndAgents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, agentIDs []string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create readiness workspace: %v", err)
	}
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register readiness agent %s: %v", agentID, err)
		}
	}
	claimReadinessWorkspaceAuthority(t, ctx, store, workspaceID)
}

func claimReadinessWorkspaceAuthority(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure readiness authority node: %v", err)
	}
	now := time.Now().UTC()
	referenceAt := now.Format(time.RFC3339Nano)
	registeredAt := strings.TrimSpace(node.RegisteredAt)
	if registeredAt == "" {
		registeredAt = referenceAt
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(authority_node_id) DO UPDATE SET
	node_kind = excluded.node_kind,
	host_label = excluded.host_label,
	boot_instance_id = excluded.boot_instance_id,
	last_seen_at = excluded.last_seen_at,
	status = excluded.status`,
		node.AuthorityNodeID, node.NodeKind, node.HostLabel, node.BootInstanceID, registeredAt, referenceAt, string(sqlite.RuntimeNodeStatusOnline)); err != nil {
		t.Fatalf("seed readiness runtime authority node: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO workspace_authority(
	workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at,
	commit_watermark, applied_watermark, status, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workspace_id, scope) DO UPDATE SET
	holder_authority_node_id = excluded.holder_authority_node_id,
	lease_token = excluded.lease_token,
	term = excluded.term,
	lease_expires_at = excluded.lease_expires_at,
	commit_watermark = excluded.commit_watermark,
	applied_watermark = excluded.applied_watermark,
	status = excluded.status,
	updated_at = excluded.updated_at`,
		workspaceID, "workspace", node.AuthorityNodeID, "lease-readiness-"+workspaceID, int64(1), now.Add(time.Hour).Format(time.RFC3339Nano), int64(1), int64(1), string(sqlite.WorkspaceAuthorityStatusActive), referenceAt); err != nil {
		t.Fatalf("seed readiness workspace authority: %v", err)
	}
	if _, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace"); err != nil {
		t.Fatalf("reload readiness workspace authority: %v", err)
	}
}

func createReadinessProject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, createdBy string) {
	t.Helper()
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       projectID,
		CreatedBy:   createdBy,
	}); err != nil {
		t.Fatalf("create readiness project: %v", err)
	}
}

func claimReadinessProjectLead(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, agentID string) {
	t.Helper()
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		ActorID:               agentID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "readiness mutation dry-run lead",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim readiness project lead: %v", err)
	}
}

func upsertReadinessRepository(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, actorID string) {
	t.Helper()
	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		RemoteURL:             "git@github.com:ExampleOrg/" + projectID + ".git",
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindGitHub,
		DefaultBranch:         "main",
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		IsCanonical:           true,
		CreatedByAgentID:      actorID,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.repository.upsert",
	}); err != nil {
		t.Fatalf("upsert readiness repository: %v", err)
	}
}

func registerReadinessCheckout(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, agentID string, gitFixture readinessGitFixture) sqlite.ProjectCheckoutRecord {
	t.Helper()
	checkout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               agentID,
		LocalPath:             gitFixture.Path,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		BranchName:            gitFixture.BranchName,
		BaseBranch:            "main",
		BaseSHA:               gitFixture.BaseSHA,
		HeadSHA:               gitFixture.HeadSHA,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               agentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register readiness checkout: %v", err)
	}
	return checkout
}

func registerReadinessIntegrationCheckout(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, actorID string, gitFixture readinessGitFixture) sqlite.ProjectCheckoutRecord {
	t.Helper()
	checkout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		LocalPath:             gitFixture.TargetPath,
		CheckoutKind:          sqlite.ProjectCheckoutKindIntegration,
		BranchName:            gitFixture.TargetBranchName,
		BaseBranch:            gitFixture.TargetBranchName,
		BaseSHA:               gitFixture.BaseSHA,
		HeadSHA:               gitFixture.BaseSHA,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register readiness integration checkout: %v", err)
	}
	return checkout
}

type readinessGitFixture struct {
	Path             string
	TargetPath       string
	BranchName       string
	TargetBranchName string
	BaseSHA          string
	HeadSHA          string
}

func newReadinessGitFixture(t *testing.T) readinessGitFixture {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skipf("git executable not available: %v", err)
	}
	dir := t.TempDir()
	branchName := "agent/worker-agent/branch-ready"
	runReadinessGit(t, dir, "init")
	runReadinessGit(t, dir, "config", "user.email", "rhizome-test@example.invalid")
	runReadinessGit(t, dir, "config", "user.name", "Rhizome Test")
	runReadinessGit(t, dir, "checkout", "-b", branchName)
	if err := os.WriteFile(filepath.Join(dir, "web-app.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatalf("write base fixture file: %v", err)
	}
	runReadinessGit(t, dir, "add", "web-app.txt")
	runReadinessGit(t, dir, "commit", "-m", "base")
	baseSHA := runReadinessGit(t, dir, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(dir, "web-app.txt"), []byte("head\n"), 0o644); err != nil {
		t.Fatalf("write head fixture file: %v", err)
	}
	runReadinessGit(t, dir, "add", "web-app.txt")
	runReadinessGit(t, dir, "commit", "-m", "head")
	headSHA := runReadinessGit(t, dir, "rev-parse", "HEAD")
	targetDir := filepath.Join(t.TempDir(), "integration")
	runReadinessGit(t, dir, "branch", "main", strings.TrimSpace(baseSHA))
	runReadinessGit(t, dir, "worktree", "add", targetDir, "main")
	return readinessGitFixture{
		Path:             dir,
		TargetPath:       targetDir,
		BranchName:       branchName,
		TargetBranchName: "main",
		BaseSHA:          strings.TrimSpace(baseSHA),
		HeadSHA:          strings.TrimSpace(headSHA),
	}
}

func runReadinessGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	raw, err := cmd.CombinedOutput()
	output := strings.TrimSpace(string(raw))
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, output)
	}
	return output
}

func TestDoctorRepoMutationActivationPassesFailClosedBlockedState(t *testing.T) {
	store := newReadinessTestStore(t)
	proof := store.ProjectPatchQueueDurabilityProof(context.Background())
	gitFixture := newReadinessGitFixture(t)
	baseSHA := gitFixture.BaseSHA
	headSHA := gitFixture.HeadSHA
	candidate := sqlite.ProjectRepoMutationActivationCandidate{
		QueueItem: sqlite.ProjectPatchQueueItemRecord{
			QueueID:           "patchq-main",
			ItemID:            "patchitem-branch-ready",
			WorkspaceID:       "ws-controlled",
			ProjectID:         "project-controlled",
			RepoID:            "repo-main",
			BranchID:          "branch-ready",
			ReviewDocKey:      "project.project-controlled.branch.branch-ready.review",
			RepoAuthorityMode: sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
			State:             sqlite.ProjectPatchQueueStateProposed,
			Pathset:           []string{"web/app.js"},
			BaseRef:           "main",
			BaseSHA:           baseSHA,
			HeadSHA:           headSHA,
			SubmittedBy:       "worker-agent",
			CreatedAt:         "2026-04-26T00:00:00Z",
			UpdatedAt:         "2026-04-26T00:00:00Z",
		},
		Branch: sqlite.ProjectBranchRecord{
			BranchID:   "branch-ready",
			BranchName: gitFixture.BranchName,
			AgentID:    "worker-agent",
			BaseBranch: "main",
			BaseSHA:    baseSHA,
			HeadSHA:    headSHA,
		},
		Checkout: sqlite.ProjectCheckoutRecord{
			CheckoutID: "checkout-worker",
			RepoID:     "repo-main",
			MachineID:  "developer-desktop",
			AgentID:    "worker-agent",
			LocalPath:  gitFixture.Path,
		},
	}
	diag := collectRepoMutationActivationDiagnosticsFromCandidate(&proof, candidate, true)

	check := checkRepoMutationActivationFromDetails(map[string]any{
		"service": serviceHealthPayload{
			Status:       "ok",
			RepoMutation: diag,
		},
	})
	if check.Status != doctorStatusPass {
		t.Fatalf("expected fail-closed repo mutation activation to pass doctor consistency check, got %+v", check)
	}
	if !strings.Contains(check.Message, "fail-closed") || !strings.Contains(check.Message, "mutation_binding") {
		t.Fatalf("expected fail-closed blocking reason in message, got %q", check.Message)
	}
	gotCandidate, ok := check.Details["candidate"].(*repoauthority.MutationActivationCandidateSummary)
	if !ok || gotCandidate == nil ||
		gotCandidate.QueueID != candidate.QueueItem.QueueID ||
		gotCandidate.ItemID != candidate.QueueItem.ItemID ||
		gotCandidate.BranchID != candidate.QueueItem.BranchID ||
		gotCandidate.CheckoutID != candidate.Checkout.CheckoutID {
		t.Fatalf("expected doctor details to expose candidate summary, got %+v", check.Details)
	}
	gotWorktree, ok := check.Details["worktree_identity"].(*repoauthority.WorktreeIdentityEvidence)
	if !ok || gotWorktree == nil ||
		gotWorktree.ReadbackState != "ok" ||
		gotWorktree.ObservedHeadSHA != headSHA ||
		gotWorktree.ObservedDirtyState != "clean" {
		t.Fatalf("expected doctor details to expose worktree readback, got %+v", check.Details)
	}
	gotBinding, ok := check.Details["mutation_binding_evidence"].(*repoauthority.MutationBindingEvidence)
	if !ok || gotBinding == nil ||
		gotBinding.WorkspaceID != candidate.QueueItem.WorkspaceID ||
		gotBinding.PatchQueueID != candidate.QueueItem.QueueID ||
		!stringSliceContainsForReadinessTest(gotBinding.MissingRefs, "operation_id") {
		t.Fatalf("expected doctor details to expose mutation binding evidence, got %+v", check.Details)
	}
}

func TestDoctorRepoMutationActivationFailsInconsistentAllowedState(t *testing.T) {
	diag := collectRepoMutationActivationDiagnostics()
	diag.Status = repoauthority.MutationActivationStatusReady
	diag.MutationAllowed = true
	check := checkRepoMutationActivationFromDetails(map[string]any{
		"service": serviceHealthPayload{
			Status:       "ok",
			RepoMutation: diag,
		},
	})
	if check.Status != doctorStatusFail {
		t.Fatalf("expected inconsistent repo mutation activation to fail, got %+v", check)
	}
	if !strings.Contains(check.Message, "inconsistent") {
		t.Fatalf("expected consistency failure message, got %q", check.Message)
	}
}

func TestDiagnosticsPayload_IncludesAuthorityNode(t *testing.T) {
	cfg := app.Config{}
	base := collectServiceHealthPayload(cfg, nil, sqlite.MemoryProjectionLagSnapshot{State: "ok"})
	payload := collectServiceHealthPayloadWithAuthority(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{
			State:           "ok",
			AuthorityNodeID: "authnode-1",
			NodeKind:        "sqlite_local_store",
			HostLabel:       "host-a",
			BootInstanceID:  "boot-a",
			Status:          "ONLINE",
		},
		sqlite.AuthorityLeaseDiagnostics{},
	)

	if payload.AuthorityNode.AuthorityNodeID != "authnode-1" || payload.AuthorityNode.State != "ok" {
		t.Fatalf("expected authority node diagnostics in payload, got %+v", payload.AuthorityNode)
	}
	if payload.Status != base.Status {
		t.Fatalf("expected healthy authority node diagnostics not to change top-level status by itself, base=%+v payload=%+v", base, payload)
	}
}

func TestDiagnosticsPayload_DegradedWhenAuthorityNodeMissing(t *testing.T) {
	cfg := app.Config{}
	payload := collectServiceHealthPayloadWithAuthority(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{
			State:   "missing",
			Message: "authority node identity not initialized",
		},
		sqlite.AuthorityLeaseDiagnostics{},
	)

	if payload.AuthorityNode.State != "missing" {
		t.Fatalf("expected missing authority node state, got %+v", payload.AuthorityNode)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected missing authority node diagnostics payload to be degraded, got %+v", payload)
	}
}

func TestDiagnosticsPayload_DegradedWhenAuthorityNodeOffline(t *testing.T) {
	cfg := app.Config{}
	payload := collectServiceHealthPayloadWithAuthority(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{
			State:           "degraded",
			Message:         "authority runtime node status is OFFLINE",
			AuthorityNodeID: "authnode-1",
			NodeKind:        "sqlite_local_store",
			Status:          "OFFLINE",
		},
		sqlite.AuthorityLeaseDiagnostics{},
	)

	if payload.AuthorityNode.State != "degraded" {
		t.Fatalf("expected degraded authority node state, got %+v", payload.AuthorityNode)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected offline authority node diagnostics payload to be degraded, got %+v", payload)
	}
}

func TestDiagnosticsPayload_DegradedWhenAuthorityLeaseGraceDetected(t *testing.T) {
	cfg := app.Config{}
	payload := collectServiceHealthPayloadWithAuthority(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{
			State:       "degraded",
			Message:     "local authority lease diagnostics: grace=1",
			Scope:       "workspace",
			ReferenceAt: "2026-04-12T10:00:00Z",
			TotalHeld:   1,
			Grace:       1,
		},
	)

	if payload.AuthorityLease.State != "degraded" {
		t.Fatalf("expected degraded authority lease state, got %+v", payload.AuthorityLease)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected grace authority lease diagnostics payload to be degraded, got %+v", payload)
	}
}

func TestDiagnosticsPayload_DegradedWhenAuthorityLeaseForeignLiveDetected(t *testing.T) {
	cfg := app.Config{}
	payload := collectServiceHealthPayloadWithAuthority(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{
			State:       "degraded",
			Message:     "workspace authority lease diagnostics: foreign_live=1",
			Scope:       "workspace",
			ReferenceAt: "2026-04-12T10:05:00Z",
			ForeignLive: 1,
		},
	)

	if payload.AuthorityLease.ForeignLive != 1 {
		t.Fatalf("expected foreign_live authority lease signal in payload, got %+v", payload.AuthorityLease)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected foreign_live authority lease diagnostics payload to be degraded, got %+v", payload)
	}
}

func TestDiagnosticsPayload_DegradedWhenOperatorQueueLagDetected(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-12T10:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	cfg := app.Config{MetricsPath: metricsFile}
	payload := collectServiceHealthPayloadWithAuthority(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "degraded", Message: "operator queue lag detected: overdue=2"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy"},
	)

	if payload.Extended.OperatorQueueLag.State != "degraded" {
		t.Fatalf("expected degraded operator queue lag state, got %+v", payload.Extended.OperatorQueueLag)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected diagnostics payload to be degraded when operator queue lag is degraded, got %+v", payload)
	}
	if !strings.Contains(payload.Semantics.Degraded.Message, "operator queue lag state is degraded") {
		t.Fatalf("expected degraded semantics to mention operator queue lag, got %q", payload.Semantics.Degraded.Message)
	}
}

func TestDiagnosticsPayload_DegradedWhenStaleOpenOperatorQueueDetected(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-12T10:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	cfg := app.Config{MetricsPath: metricsFile}
	payload := collectServiceHealthPayloadWithAuthority(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "degraded", Message: "operator queue lag detected: stale_open=1"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy"},
	)

	if payload.Status != "degraded" {
		t.Fatalf("expected diagnostics payload to be degraded when stale open queue signal is degraded, got %+v", payload)
	}
	if !strings.Contains(payload.Extended.OperatorQueueLag.Message, "stale_open=1") {
		t.Fatalf("expected stale_open details in operator queue lag message, got %+v", payload.Extended.OperatorQueueLag)
	}
}

func TestDiagnosticsPayload_DegradedWhenMissingOperatorQueueDetected(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-12T10:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	cfg := app.Config{MetricsPath: metricsFile}
	payload := collectServiceHealthPayloadWithAuthority(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "degraded", Message: "operator queue lag detected: missing_operator_queue=1"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy"},
	)

	if payload.Status != "degraded" {
		t.Fatalf("expected diagnostics payload to be degraded when missing operator queue signal is degraded, got %+v", payload)
	}
	if !strings.Contains(payload.Extended.OperatorQueueLag.Message, "missing_operator_queue=1") {
		t.Fatalf("expected missing_operator_queue details in operator queue lag message, got %+v", payload.Extended.OperatorQueueLag)
	}
}

func TestDiagnosticsPayload_DegradedWhenReviewerScarcityDetected(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-12T10:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	cfg := app.Config{MetricsPath: metricsFile}
	payload := collectServiceHealthPayloadWithAuthority(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "degraded", Message: "reviewer scarcity detected: saturated_workspaces=1"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy"},
	)

	if payload.Status != "degraded" {
		t.Fatalf("expected diagnostics payload to be degraded when reviewer scarcity is degraded, got %+v", payload)
	}
	if payload.Extended.ReviewerScarcity.State != "degraded" {
		t.Fatalf("expected degraded reviewer scarcity state, got %+v", payload.Extended.ReviewerScarcity)
	}
	if !strings.Contains(payload.Semantics.Degraded.Message, "reviewer scarcity state is degraded") {
		t.Fatalf("expected degraded semantics to mention reviewer scarcity, got %q", payload.Semantics.Degraded.Message)
	}
}

func TestDiagnosticsPayload_DegradedWhenReviewerScarcityIsPartialUnknown(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-12T10:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	cfg := app.Config{MetricsPath: metricsFile}
	payload := collectServiceHealthPayloadWithAuthority(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "degraded", Message: "reviewer scarcity health is partial: unknown_workspaces=1"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy"},
	)

	if payload.Status != "degraded" {
		t.Fatalf("expected diagnostics payload to be degraded when reviewer scarcity is partial/unknown, got %+v", payload)
	}
	if payload.Extended.ReviewerScarcity.State != "degraded" {
		t.Fatalf("expected degraded reviewer scarcity state for partial/unknown signal, got %+v", payload.Extended.ReviewerScarcity)
	}
	if !strings.Contains(payload.Extended.ReviewerScarcity.Message, "unknown_workspaces=1") {
		t.Fatalf("expected reviewer scarcity partial/unknown detail in payload, got %+v", payload.Extended.ReviewerScarcity)
	}
	if !strings.Contains(payload.Semantics.Degraded.Message, "reviewer scarcity state is degraded") {
		t.Fatalf("expected degraded semantics to mention reviewer scarcity partial/unknown signal, got %q", payload.Semantics.Degraded.Message)
	}
}

func TestReviewerScarcityDiagnosticSignalKeepsUnknownPartialAdvisory(t *testing.T) {
	signal := reviewerScarcityDiagnosticSignal(sqlite.ReviewerScarcityHealthSnapshot{
		State:                 "degraded",
		Message:               "reviewer scarcity health is partial: unknown_workspaces=1",
		UnknownWorkspaceCount: 1,
	})
	if signal.State != "partial" {
		t.Fatalf("expected unknown-only reviewer scarcity to be partial, got %+v", signal)
	}
	if !strings.Contains(signal.Message, "unknown_workspaces=1") {
		t.Fatalf("expected unknown count in partial message, got %+v", signal)
	}
}

func TestReviewerScarcityDiagnosticSignalKeepsScarceDegraded(t *testing.T) {
	signal := reviewerScarcityDiagnosticSignal(sqlite.ReviewerScarcityHealthSnapshot{
		State:                   "degraded",
		Message:                 "reviewer scarcity detected: scarce_workspaces=1",
		ScarceWorkspaceCount:    1,
		UnknownWorkspaceCount:   1,
		SaturatedWorkspaceCount: 0,
	})
	if signal.State != "degraded" {
		t.Fatalf("expected scarce reviewer scarcity to remain degraded, got %+v", signal)
	}
}

func TestDiagnosticsPayload_IncludesReviewerScarcityHealthWorkspaceExamples(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-12T10:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	cfg := app.Config{MetricsPath: metricsFile}
	reviewerScarcityHealth := sqlite.ReviewerScarcityHealthSnapshot{
		State:                      "degraded",
		Message:                    "reviewer scarcity detected: saturated_workspaces=1, scarce_workspaces=1, unknown_workspaces=1",
		WorkspaceCount:             3,
		SaturatedWorkspaceCount:    1,
		ScarceWorkspaceCount:       1,
		UnknownWorkspaceCount:      1,
		SaturatedWorkspaceExamples: []string{"ws-hot"},
		ScarceWorkspaceExamples:    []string{"ws-tight"},
		UnknownWorkspaceExamples:   []string{"ws-partial"},
	}
	payload := collectServiceHealthPayloadWithAuthorityAndReviewerScarcityHealthAndStuckAgentHealth(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "degraded", Message: reviewerScarcityHealth.Message},
		reviewerScarcityHealth,
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		nil,
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy"},
	)

	if payload.Extended.ReviewerScarcityHealth.WorkspaceCount != 3 {
		t.Fatalf("expected reviewer scarcity health workspace count in payload, got %+v", payload.Extended.ReviewerScarcityHealth)
	}
	if len(payload.Extended.ReviewerScarcityHealth.SaturatedWorkspaceExamples) != 1 || payload.Extended.ReviewerScarcityHealth.SaturatedWorkspaceExamples[0] != "ws-hot" {
		t.Fatalf("expected saturated reviewer scarcity workspace example in payload, got %+v", payload.Extended.ReviewerScarcityHealth)
	}
	if len(payload.Extended.ReviewerScarcityHealth.ScarceWorkspaceExamples) != 1 || payload.Extended.ReviewerScarcityHealth.ScarceWorkspaceExamples[0] != "ws-tight" {
		t.Fatalf("expected scarce reviewer scarcity workspace example in payload, got %+v", payload.Extended.ReviewerScarcityHealth)
	}
	if len(payload.Extended.ReviewerScarcityHealth.UnknownWorkspaceExamples) != 1 || payload.Extended.ReviewerScarcityHealth.UnknownWorkspaceExamples[0] != "ws-partial" {
		t.Fatalf("expected unknown reviewer scarcity workspace example in payload, got %+v", payload.Extended.ReviewerScarcityHealth)
	}
}

func TestDiagnosticsPayload_DegradedWhenStuckAgentsDetected(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-12T10:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	cfg := app.Config{MetricsPath: metricsFile}
	payload := collectServiceHealthPayloadWithAuthority(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "degraded", Message: "stuck agent risk detected: offline_agent_sessions=1, missing_agent_sessions=0"},
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy"},
	)

	if payload.Status != "degraded" {
		t.Fatalf("expected diagnostics payload to be degraded when stuck-agent signal is degraded, got %+v", payload)
	}
	if !strings.Contains(payload.Extended.StuckAgents.Message, "offline_agent_sessions=1") {
		t.Fatalf("expected offline_agent_sessions detail in stuck agent message, got %+v", payload.Extended.StuckAgents)
	}
}

// ---------- Doctor loop_readiness check ----------

func TestDoctor_LoopReadinessCheck_Healthy(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"loop_readiness": []any{
				map[string]any{"name": "daemon", "state": "disabled", "restarts": float64(0)},
				map[string]any{"name": "firehose", "state": "running", "restarts": float64(0)},
				map[string]any{"name": "timeout_reaper", "state": "running", "restarts": float64(0)},
			},
		},
	}

	check := checkLoopReadinessFromDetails(details)
	if check.Status != doctorStatusPass {
		t.Fatalf("expected pass for healthy loops, got %q: %s", check.Status, check.Message)
	}
}

func TestDoctor_LoopReadinessCheck_Degraded(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"loop_readiness": []any{
				map[string]any{"name": "daemon", "state": "running", "restarts": float64(0)},
				map[string]any{"name": "firehose", "state": "degraded", "restarts": float64(0), "dropped_events": float64(10)},
				map[string]any{"name": "timeout_reaper", "state": "running", "restarts": float64(0)},
			},
		},
	}

	check := checkLoopReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for degraded firehose, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "firehose") {
		t.Fatalf("expected firehose mentioned in message: %s", check.Message)
	}
}

func TestDoctor_LoopReadinessCheck_Recovering(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"loop_readiness": []any{
				map[string]any{"name": "daemon", "state": "recovering", "restarts": float64(5), "last_error": "panic: runtime error"},
				map[string]any{"name": "firehose", "state": "running", "restarts": float64(0)},
			},
		},
	}

	check := checkLoopReadinessFromDetails(details)
	if check.Status != doctorStatusFail {
		t.Fatalf("expected critical for recovering with 5 restarts, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "daemon") {
		t.Fatalf("expected daemon mentioned in message: %s", check.Message)
	}
}

func TestDoctor_LoopReadinessCheck_NoData(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{},
	}

	check := checkLoopReadinessFromDetails(details)
	if check.Status != doctorStatusPass {
		t.Fatalf("expected pass when no loop_readiness data, got %q", check.Status)
	}
}

func TestDoctor_LoopReadinessCheck_FirehoseNotStarted(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"loop_readiness": []any{
				map[string]any{"name": "daemon", "state": "disabled", "restarts": float64(0)},
				map[string]any{"name": "firehose", "state": "not_started", "restarts": float64(0)},
				map[string]any{"name": "timeout_reaper", "state": "running", "restarts": float64(0)},
			},
		},
	}

	check := checkLoopReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn when firehose not started, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "firehose: not started") {
		t.Fatalf("expected firehose not started in message: %s", check.Message)
	}
}

// ---------- Doctor integration with live readiness endpoint ----------

func TestDoctor_LoopReadinessFromLiveEndpoint(t *testing.T) {
	_, workspaceRoot := setupFakeBridgeEnv(t)

	createCLITestWorkspace(t, "ws-readiness-doctor")
	if err := runTaskSubmit([]string{
		"--task-id", "task-readiness-doctor",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-readiness-doctor",
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}

	localCheckout := app.CurrentGitCheckoutInfo()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(serviceHealthPayload{
			Status: "ok",
			TS:     time.Now().UTC().Format(time.RFC3339Nano),
			Config: configSnapshot{
				DBPath:               filepath.Join(t.TempDir(), "rhizome.db"),
				WorkspaceRoot:        workspaceRoot,
				ExecutorPython:       "python",
				ExecutorBridgeScript: "internal/executor/rpc_bridge.py",
			},
			Checkout: localCheckout,
			Metrics: serviceMetricsSummary{
				Status: "missing",
				Health: runtimeMetricsHealth{
					Verdict:              "unknown",
					ThresholdFailureRate: runtimeFailureRateThreshold,
				},
			},
			LoopReadiness: []LoopReadiness{
				{Name: "daemon", State: LoopDisabled},
				{Name: "firehose", State: LoopRunning},
				{Name: "timeout_reaper", State: LoopRunning},
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
		Checks []struct {
			Name    string `json:"name"`
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"checks"`
	}
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("decode doctor output: %v; output=%q", err, out)
	}

	found := false
	for _, check := range report.Checks {
		if check.Name == "loop_readiness" {
			found = true
			if check.Status != doctorStatusPass {
				t.Fatalf("expected loop_readiness pass, got %q: %s", check.Status, check.Message)
			}
		}
	}
	if !found {
		t.Fatalf("expected loop_readiness check in doctor output")
	}
}

func TestDoctor_PublicHealthPayloadFailsDeploymentReadiness(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(serviceLivenessPayload{
			Status: "ok",
			TS:     time.Now().UTC().Format(time.RFC3339Nano),
		})
	}))
	defer server.Close()

	var runErr error
	out, err := captureStdout(t, func() error {
		runErr = runDoctor([]string{
			"--format", "json",
			"--health-url", server.URL,
		})
		return nil
	})
	if err != nil {
		t.Fatalf("capture doctor output failed: %v", err)
	}
	if runErr == nil {
		t.Fatalf("expected runDoctor to return a critical verdict for public health payload")
	}

	var report struct {
		Checks []struct {
			Name    string `json:"name"`
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"checks"`
	}
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("decode doctor output: %v; output=%q", err, out)
	}

	var deploymentCheck *struct {
		Name    string `json:"name"`
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	for i := range report.Checks {
		check := &report.Checks[i]
		if check.Name == "service_deployment_readiness" {
			deploymentCheck = check
			break
		}
	}
	if deploymentCheck == nil {
		t.Fatalf("expected service_deployment_readiness check in doctor output: %+v", report.Checks)
	}
	if deploymentCheck.Status != doctorStatusFail {
		t.Fatalf("expected public health payload deployment readiness to fail, got %+v", deploymentCheck)
	}
	if !strings.Contains(strings.ToLower(deploymentCheck.Message), "/api/diagnostics") {
		t.Fatalf("expected deployment readiness failure to point at diagnostics endpoint, got %+v", deploymentCheck)
	}
}

func TestDoctor_StrictPreflightReturnsErrorOnWarnings(t *testing.T) {
	_, workspaceRoot := setupFakeBridgeEnv(t)
	dbPath := seedFirstDeploymentRosterDB(t, []string{"alpha", "beta", "gamma"})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(serviceHealthPayload{
			Status:          "degraded",
			TS:              time.Now().UTC().Format(time.RFC3339Nano),
			PromptAuthority: collectPromptAuthorityScopeDiagnostics(),
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
			"--db-path", dbPath,
			"--health-url", server.URL,
			"--strict-preflight",
		})
	})
	if err == nil {
		t.Fatal("expected runDoctor to fail when --strict-preflight is set")
	}
	if !strings.Contains(err.Error(), "doctor verdict: warn") {
		t.Fatalf("unexpected strict-preflight error: %v", err)
	}
	if !strings.Contains(out, `"verdict": "warn"`) {
		t.Fatalf("expected warn verdict in doctor output, got %s", out)
	}
}

func TestDoctor_PromptAuthorityScopePassesFirstStableBoundary(t *testing.T) {
	check := checkPromptAuthorityScope(collectPromptAuthorityScopeDiagnostics())
	if check.Status != doctorStatusPass {
		t.Fatalf("expected first-stable prompt authority scope to pass, got %+v", check)
	}
	if check.Details["surface_count"] != len(collectPromptAuthorityScopeDiagnostics().Surfaces) {
		t.Fatalf("expected surface count detail, got %+v", check.Details)
	}
}

func TestDoctor_PromptAuthorityScopeFailsMissingRequiredSurface(t *testing.T) {
	scope := collectPromptAuthorityScopeDiagnostics()
	filtered := scope.Surfaces[:0]
	for _, surface := range scope.Surfaces {
		if surface.Surface != "internal_agent.memory_write" {
			filtered = append(filtered, surface)
		}
	}
	scope.Surfaces = filtered

	check := checkPromptAuthorityScope(scope)
	if check.Status != doctorStatusFail {
		t.Fatalf("expected missing memory_write boundary to fail, got %+v", check)
	}
	if !strings.Contains(check.Message, "internal_agent.memory_write") {
		t.Fatalf("expected missing surface in failure message, got %q", check.Message)
	}
}

func TestDoctor_PromptAuthorityScopeFailsFalseDaemonConvergenceClaim(t *testing.T) {
	scope := collectPromptAuthorityScopeDiagnostics()
	scope.Surfaces[0].AcceptedAsDaemonConvergence = true
	scope.Surfaces[0].C21Convergence = durablePromptConvergenceAccepted
	scope.Surfaces[0].DeploymentEvidence = durablePromptDeploymentEvidenceAccepted

	check := checkPromptAuthorityScope(scope)
	if check.Status != doctorStatusFail {
		t.Fatalf("expected false daemon convergence claim to fail, got %+v", check)
	}
	if !strings.Contains(strings.ToLower(check.Message), "daemon") {
		t.Fatalf("expected daemon convergence failure in message, got %q", check.Message)
	}
}

func TestDoctor_PromptAuthorityScopeFailsArbitraryUnsafeClassification(t *testing.T) {
	scope := collectPromptAuthorityScopeDiagnostics()
	for idx := range scope.Surfaces {
		if scope.Surfaces[idx].Surface == "manager.live_attach.runtime_control" {
			scope.Surfaces[idx].Decision = "operator_accepted"
			scope.Surfaces[idx].C21Convergence = "manual_reviewed"
			scope.Surfaces[idx].DeploymentEvidence = "looks_safe"
			scope.Surfaces[idx].FirstDeploymentPreflight = "ok"
			break
		}
	}

	check := checkPromptAuthorityScope(scope)
	if check.Status != doctorStatusFail {
		t.Fatalf("expected arbitrary unsafe classifications to fail, got %+v", check)
	}
	for _, want := range []string{"manager.live_attach.runtime_control", "decision", "operator_accepted"} {
		if !strings.Contains(check.Message, want) {
			t.Fatalf("expected %q in failure message, got %q", want, check.Message)
		}
	}
}

func TestDoctor_StrictPreflightRequiresHealthURLAndValidatesRoster(t *testing.T) {
	dbPath := seedFirstDeploymentRosterDB(t, []string{"alpha", "beta"})

	out, err := captureStdout(t, func() error {
		return runDoctor([]string{
			"--format", "json",
			"--db-path", dbPath,
			"--strict-preflight",
		})
	})
	if err == nil {
		t.Fatal("expected strict preflight without health URL and with incomplete roster to fail")
	}
	if !strings.Contains(err.Error(), "doctor verdict: critical") {
		t.Fatalf("unexpected strict-preflight error: %v", err)
	}
	if !strings.Contains(out, `"name": "strict_preflight_inputs"`) {
		t.Fatalf("expected strict_preflight_inputs check in doctor output, got %s", out)
	}
	if !strings.Contains(out, `"name": "agent_roster"`) || !strings.Contains(out, "gamma") {
		t.Fatalf("expected agent_roster failure to be emitted under strict preflight, got %s", out)
	}
}

func TestDoctor_ExtendedReadinessProjectionLagCheck(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "ok", "message": "firehose running"},
				"invalidation_lag":   map[string]any{"state": "ok", "message": "index present"},
				"operator_queue_lag": map[string]any{"state": "ok", "message": "queue scan healthy"},
				"projection_lag": map[string]any{
					"state":                      "degraded",
					"message":                    "2 pending projection(s); 1 failed projection(s)",
					"pending_count":              float64(2),
					"failed_count":               float64(1),
					"oldest_pending_at":          "2026-04-08T12:00:00Z",
					"oldest_pending_age_seconds": float64(120),
				},
				"replay_health": map[string]any{"state": "ok", "message": "replay view present"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for degraded projection lag, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "projection_lag") {
		t.Fatalf("expected projection lag mentioned in message: %s", check.Message)
	}
}

func TestDoctor_TopologyDriftCheck_HealthyFirstDeploymentTopology(t *testing.T) {
	dbPath := seedFirstDeploymentRosterDB(t, []string{"alpha", "beta", "gamma"})

	check := checkTopologyDriftFromDetails(map[string]any{
		"service": serviceHealthPayload{
			LoopReadiness: []LoopReadiness{
				{Name: loopNameDaemon, State: LoopDisabled, Restarts: 0},
				{Name: loopNameFirehose, State: LoopRunning, Restarts: 0},
				{Name: loopNameTimeoutReaper, State: LoopRunning, Restarts: 0},
			},
			Extended: ExtendedReadiness{
				StuckAgents: DiagnosticSignal{State: "ok", Message: "stuck agent health is stable"},
			},
		},
	}, dbPath)

	if check.Status != doctorStatusPass {
		t.Fatalf("expected healthy first-deployment topology to pass, got %q: %s", check.Status, check.Message)
	}
}

func TestDoctor_TopologyDriftCheck_FailsWhenDaemonLoopIsRunning(t *testing.T) {
	dbPath := seedFirstDeploymentRosterDB(t, []string{"alpha", "beta", "gamma"})

	check := checkTopologyDriftFromDetails(map[string]any{
		"service": serviceHealthPayload{
			LoopReadiness: []LoopReadiness{
				{Name: loopNameDaemon, State: LoopRunning, Restarts: 2},
				{Name: loopNameFirehose, State: LoopRunning, Restarts: 0},
				{Name: loopNameTimeoutReaper, State: LoopRunning, Restarts: 0},
			},
			Extended: ExtendedReadiness{
				StuckAgents: DiagnosticSignal{State: "ok", Message: "stuck agent health is stable"},
			},
		},
	}, dbPath)

	if check.Status != doctorStatusFail {
		t.Fatalf("expected daemon-loop drift to fail, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(strings.ToLower(check.Message), "daemon loop") {
		t.Fatalf("expected daemon loop drift in failure message, got %s", check.Message)
	}
}

func TestDoctor_TopologyDriftCheck_FailsWhenStuckAgentsDegraded(t *testing.T) {
	dbPath := seedFirstDeploymentRosterDB(t, []string{"alpha", "beta", "gamma"})

	check := checkTopologyDriftFromDetails(map[string]any{
		"service": serviceHealthPayload{
			LoopReadiness: []LoopReadiness{
				{Name: loopNameDaemon, State: LoopDisabled, Restarts: 0},
				{Name: loopNameFirehose, State: LoopRunning, Restarts: 0},
				{Name: loopNameTimeoutReaper, State: LoopRunning, Restarts: 0},
			},
			Extended: ExtendedReadiness{
				StuckAgents: DiagnosticSignal{State: "degraded", Message: "stuck agent risk detected"},
			},
		},
	}, dbPath)

	if check.Status != doctorStatusFail {
		t.Fatalf("expected degraded stuck-agents signal to fail, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(strings.ToLower(check.Message), "stuck agent risk detected") {
		t.Fatalf("expected stuck-agent signal in failure message, got %s", check.Message)
	}
}

func TestDoctor_AgentRosterCheck_FailsWhenAgentMissing(t *testing.T) {
	dbPath := seedFirstDeploymentRosterDB(t, []string{"alpha", "beta"})

	check := checkDeploymentAgentRoster(dbPath)
	if check.Status != doctorStatusFail {
		t.Fatalf("expected missing agent roster to fail, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "gamma") {
		t.Fatalf("expected missing agent in message, got %s", check.Message)
	}
}

func TestDoctor_AgentRosterCheck_FailsWhenUnexpectedAgentPresent(t *testing.T) {
	dbPath := seedFirstDeploymentRosterDB(t, []string{"alpha", "beta", "gamma", "delta"})

	check := checkDeploymentAgentRoster(dbPath)
	if check.Status != doctorStatusFail {
		t.Fatalf("expected unexpected agent roster to fail, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "delta") {
		t.Fatalf("expected unexpected agent in message, got %s", check.Message)
	}
}

func TestDoctor_AgentRosterCheck_IgnoresInfrastructureBridgeAgents(t *testing.T) {
	dbPath := seedFirstDeploymentRosterDB(t, []string{"alpha", "beta", "gamma"})
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	if err := store.RegisterAgent(context.Background(), sqlite.AgentRegisterInput{
		WorkspaceID:     "ws-first-deployment",
		AgentID:         "telegram-bridge",
		OwnerUserID:     "telegram",
		DisplayName:     "Telegram Bridge",
		Role:            "bridge",
		Status:          model.AgentStatusActive,
		ProtocolVersion: "telegram-bridge/v1",
		Summary:         "human notification bridge",
	}); err != nil {
		t.Fatalf("register telegram bridge: %v", err)
	}

	check := checkDeploymentAgentRoster(dbPath)
	if check.Status != doctorStatusPass {
		t.Fatalf("expected infrastructure bridge to be ignored, got %q: %s", check.Status, check.Message)
	}
	ignored, _ := check.Details["ignored_infrastructure_agents"].([]string)
	if len(ignored) != 1 || ignored[0] != "telegram-bridge" {
		t.Fatalf("ignored_infrastructure_agents = %#v, want telegram-bridge", check.Details["ignored_infrastructure_agents"])
	}
}

func TestDoctor_AgentRosterCheck_FailsWhenDBCannotBeValidated(t *testing.T) {
	emptyPathCheck := checkDeploymentAgentRoster("")
	if emptyPathCheck.Status != doctorStatusFail {
		t.Fatalf("expected empty db path to fail, got %q: %s", emptyPathCheck.Status, emptyPathCheck.Message)
	}
	if !strings.Contains(strings.ToLower(emptyPathCheck.Message), "db path is empty") {
		t.Fatalf("expected empty db path in failure message, got %s", emptyPathCheck.Message)
	}

	missingDBPath := filepath.Join(t.TempDir(), "missing.sqlite")
	missingPathCheck := checkDeploymentAgentRoster(missingDBPath)
	if missingPathCheck.Status != doctorStatusFail {
		t.Fatalf("expected missing db path to fail, got %q: %s", missingPathCheck.Status, missingPathCheck.Message)
	}
	if !strings.Contains(strings.ToLower(missingPathCheck.Message), "db file is missing") {
		t.Fatalf("expected missing db file in failure message, got %s", missingPathCheck.Message)
	}
}

func TestDoctor_AgentRosterCheck_FailsWhenMultipleActiveWorkspacesExist(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "rhizome.sqlite")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	workspaces := []string{"ws-alpha", "ws-beta"}
	for _, workspaceID := range workspaces {
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       workspaceID,
			Description: "first deployment workspace",
			CreatedBy:   "developer",
			Status:      model.WorkspaceStatusActive,
		}); err != nil {
			t.Fatalf("create workspace %s: %v", workspaceID, err)
		}
	}
	for _, agentID := range canonicalDeploymentAgentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaces[0],
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
			Role:        "agent",
			Status:      model.AgentStatusActive,
			Summary:     "first-deployment agent",
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	check := checkDeploymentAgentRoster(dbPath)
	if check.Status != doctorStatusFail {
		t.Fatalf("expected multiple active workspaces to fail, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "exactly one active workspace") {
		t.Fatalf("expected multiple-active-workspace failure message, got %s", check.Message)
	}
}

func seedFirstDeploymentRosterDB(t *testing.T, agentIDs []string) string {
	t.Helper()

	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "rhizome.sqlite")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-first-deployment",
		Title:       "first deployment",
		Description: "canonical first-deployment workspace",
		CreatedBy:   "developer",
		Status:      model.WorkspaceStatusActive,
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-first-deployment",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
			Role:        "agent",
			Status:      model.AgentStatusActive,
			Summary:     "first-deployment agent",
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	return dbPath
}

// ---------- TopLevelSemantics diagnostics integration ----------

func TestDiagnosticsPayload_TopLevelSemantics_CoreReadyButMetricsMissing(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.SetState(loopNameDaemon, LoopRunning)
	r.Register(loopNameFirehose)
	r.SetState(loopNameFirehose, LoopRunning)
	cfg := app.Config{}
	payload := collectServiceHealthPayload(cfg, r, sqlite.MemoryProjectionLagSnapshot{State: "ok"})

	if payload.Semantics.Liveness.State != "ok" {
		t.Fatalf("expected liveness ok, got %q", payload.Semantics.Liveness.State)
	}
	if payload.Semantics.Readiness.State != "ok" {
		t.Fatalf("expected readiness ok, got %q", payload.Semantics.Readiness.State)
	}
	if payload.Semantics.DeploymentReadiness.State != "degraded" {
		t.Fatalf("expected deployment readiness degraded when metrics are missing, got %q", payload.Semantics.DeploymentReadiness.State)
	}
	// In a missing-config test environment, Metrics will be "missing" resulting in "degraded".
	// The liveness and readiness remain "ok" though.
	if payload.Semantics.Degraded.State != "degraded" {
		t.Fatalf("expected degraded state due to missing metrics, got %q", payload.Semantics.Degraded.State)
	}
}

func TestDiagnosticsPayload_TopLevelSemantics_IdleRuntimeMetricsStayReady(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-11T00:00:00Z","profiles":{"compute":{"total_runs":0,"success_count":0,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.0,"avg_startup_ms":0}}}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.SetState(loopNameDaemon, LoopRunning)
	r.Register(loopNameFirehose)
	r.SetState(loopNameFirehose, LoopRunning)
	cfg := app.Config{MetricsPath: metricsFile}
	payload := collectServiceHealthPayloadFromState(
		cfg,
		r,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{},
		sqlite.AuthorityLeaseDiagnostics{},
		app.RuntimeBuildInfo{
			BinaryPath:       "/tmp/rhizome",
			WorkingDirectory: "/tmp",
			RepoRoot:         "/tmp/repo",
			VCSRevision:      "abc123",
		},
		app.GitCheckoutInfo{
			RepoRoot: "/tmp/repo",
			Branch:   "main",
			Head:     "abc123",
		},
	)

	if payload.Metrics.Status != "ok" {
		t.Fatalf("expected idle metrics status ok, got %q", payload.Metrics.Status)
	}
	if payload.Semantics.Liveness.State != "ok" || payload.Semantics.Readiness.State != "ok" {
		t.Fatalf("expected healthy liveness/readiness, got %+v", payload.Semantics)
	}
	if payload.Semantics.DeploymentReadiness.State != "ok" {
		t.Fatalf("expected healthy deployment readiness, got %+v", payload.Semantics.DeploymentReadiness)
	}
	if payload.Semantics.Degraded.State != "ok" {
		t.Fatalf("expected idle runtime metrics not to degrade service semantics, got %q", payload.Semantics.Degraded.State)
	}
	if payload.Status != "ok" {
		t.Fatalf("expected top-level status ok when runtime metrics are merely idle, got %q", payload.Status)
	}
}

func TestDiagnosticsPayload_DirtyCheckoutDoesNotBlockDeploymentReadiness(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(validMetricsSnapshotLine()+"\n"), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.SetState(loopNameDaemon, LoopRunning)
	r.Register(loopNameFirehose)
	r.SetState(loopNameFirehose, LoopRunning)
	r.Register(loopNameTimeoutReaper)
	r.SetState(loopNameTimeoutReaper, LoopRunning)
	cfg := app.Config{MetricsPath: metricsFile}
	payload := collectServiceHealthPayloadFromState(
		cfg,
		r,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "ok", Message: "operator queue lag healthy"},
		DiagnosticSignal{State: "ok", Message: "reviewer scarcity healthy"},
		DiagnosticSignal{State: "ok", Message: "stuck agent health is stable"},
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy", Healthy: 1},
		app.RuntimeBuildInfo{
			BinaryPath:       "/tmp/rhizome",
			WorkingDirectory: "/tmp",
			RepoRoot:         "/tmp/repo",
			VCSRevision:      "abc123",
		},
		app.GitCheckoutInfo{
			RepoRoot: "/tmp/repo",
			Branch:   "main",
			Head:     "abc123",
			Dirty:    true,
		},
	)

	if !payload.Checkout.Dirty {
		t.Fatal("expected diagnostics to preserve dirty checkout evidence")
	}
	if payload.Status != "ok" {
		t.Fatalf("dirty checkout should not degrade live service health, got status=%q semantics=%+v", payload.Status, payload.Semantics)
	}
	if payload.Semantics.DeploymentReadiness.State != "ok" {
		t.Fatalf("dirty checkout should not block deployment readiness, got %+v", payload.Semantics.DeploymentReadiness)
	}
	if payload.Semantics.Degraded.State != "ok" {
		t.Fatalf("dirty checkout should remain drift evidence, not degraded semantics, got %+v", payload.Semantics.Degraded)
	}
}

func TestDurablePromptCompilerConvergenceDoesNotBlockServeReadinessWhenDaemonDisabled(t *testing.T) {
	mismatch := &durablePromptCompilerSnapshot{
		State:                  "mismatch",
		Message:                "capability snapshot readback projection_digest mismatch",
		CapabilitySnapshotRef:  "capability_snapshot:cap_stale",
		ProjectionDigest:       "sha256:c2ed82752c89c4a5096ee2e9003210695e6a4f7350c6a37e0a87c899c6f8290d",
		SnapshotReadbackDigest: "sha256:60e499add8b4a42606a7220d28674e2dfc1acf68a6f888ada5d69d003e3cec52",
	}

	if durablePromptCompilerConvergenceBlocksServeReadiness([]LoopReadiness{{Name: loopNameDaemon, State: LoopDisabled}}, mismatch) {
		t.Fatal("disabled embedded daemon loop must not turn stale prompt compiler evidence into a serve readiness blocker")
	}
	if !durablePromptCompilerConvergenceBlocksServeReadiness([]LoopReadiness{{Name: loopNameDaemon, State: LoopRunning}}, mismatch) {
		t.Fatal("running embedded daemon loop must still fail closed on prompt compiler evidence mismatch")
	}
	if durablePromptCompilerConvergenceBlocksServeReadiness([]LoopReadiness{{Name: loopNameDaemon, State: LoopRunning}}, &durablePromptCompilerSnapshot{State: "ok"}) {
		t.Fatal("accepted prompt compiler proof must not block serve readiness")
	}
	if durablePromptCompilerConvergenceBlocksServeReadiness([]LoopReadiness{{Name: loopNameDaemon, State: LoopRunning}}, nil) {
		t.Fatal("missing prompt compiler snapshot pointer must stay advisory at the serve readiness layer")
	}
}

func TestDiagnosticsPayload_RuntimeWorkGateDoesNotClaimLiveAgentAggregation(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(validMetricsSnapshotLine()+"\n"), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.SetState(loopNameDaemon, LoopRunning)
	r.Register(loopNameFirehose)
	r.SetState(loopNameFirehose, LoopRunning)
	cfg := app.Config{MetricsPath: metricsFile}
	payload := collectServiceHealthPayloadFromState(
		cfg,
		r,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "ok", Message: "operator queue lag healthy"},
		DiagnosticSignal{State: "ok", Message: "reviewer scarcity healthy"},
		DiagnosticSignal{State: "ok", Message: "stuck agent health is stable"},
		sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
		sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy", Healthy: 1},
		app.RuntimeBuildInfo{
			BinaryPath:       "/tmp/rhizome",
			WorkingDirectory: "/tmp",
			RepoRoot:         "/tmp/repo",
			VCSRevision:      "abc123",
		},
		app.GitCheckoutInfo{
			RepoRoot: "/tmp/repo",
			Branch:   "main",
			Head:     "abc123",
		},
	)

	if payload.RuntimeWorkGate != nil {
		t.Fatalf("root diagnostics must not claim live per-agent runtime work-gate aggregation yet, got %+v", payload.RuntimeWorkGate)
	}
	if payload.Status != "ok" || payload.Semantics.DeploymentReadiness.State != "ok" {
		t.Fatalf("missing live runtime-work-gate aggregation should not invent a degraded or safe gate, got status=%q semantics=%+v", payload.Status, payload.Semantics)
	}
}

func TestDiagnosticsPayload_TopLevelSemantics_Degraded(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.SetError(loopNameDaemon, fmt.Errorf("crash"))
	cfg := app.Config{}
	payload := collectServiceHealthPayload(cfg, r, sqlite.MemoryProjectionLagSnapshot{State: "ok"})

	if payload.Semantics.Liveness.State != "ok" {
		t.Fatalf("expected liveness ok (still responds), got %q", payload.Semantics.Liveness.State)
	}
	if payload.Semantics.Readiness.State != "not_ready" {
		t.Fatalf("expected readiness not_ready when loop is recovering, got %q", payload.Semantics.Readiness.State)
	}
	if payload.Semantics.DeploymentReadiness.State != "not_ready" {
		t.Fatalf("expected deployment readiness not_ready when loop is recovering, got %q", payload.Semantics.DeploymentReadiness.State)
	}
	if payload.Semantics.Degraded.State != "degraded" {
		t.Fatalf("expected degraded state 'degraded' due to loop crash, got %q", payload.Semantics.Degraded.State)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected top-level status degraded when loop is recovering, got %q", payload.Status)
	}
}

func TestDiagnosticsPayload_TopLevelSemantics_NotReadyLoopsDegradeTopLevelStatus(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-12T10:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	cfg := app.Config{MetricsPath: metricsFile}
	cases := []struct {
		name         string
		state        LoopState
		wantMessage  string
		wantDegraded string
	}{
		{
			name:         "loop not started",
			state:        LoopNotStarted,
			wantMessage:  "one or more required loops have not started",
			wantDegraded: "required loops have not started",
		},
		{
			name:         "loop stopped",
			state:        LoopStopped,
			wantMessage:  "one or more required loops stopped",
			wantDegraded: "required loops stopped",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewReadinessRegistry()
			r.Register(loopNameDaemon)
			r.Register(loopNameFirehose)
			r.SetState(loopNameDaemon, LoopRunning)
			r.SetState(loopNameFirehose, tc.state)

			payload := collectServiceHealthPayloadFromState(
				cfg,
				r,
				sqlite.MemoryProjectionLagSnapshot{State: "ok"},
				DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
				DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
				DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
				sqlite.AuthorityNodeDiagnostics{State: "ok", AuthorityNodeID: "authnode-1", Status: "ONLINE"},
				sqlite.AuthorityLeaseDiagnostics{State: "ok", Scope: "workspace", Message: "workspace authority lease diagnostics are healthy"},
				app.RuntimeBuildInfo{},
				app.GitCheckoutInfo{},
			)

			if payload.Status != "degraded" {
				t.Fatalf("expected top-level status degraded when loop is %s, got %+v", tc.state, payload)
			}
			if payload.Semantics.Readiness.State != "not_ready" {
				t.Fatalf("expected readiness not_ready when loop is %s, got %+v", tc.state, payload.Semantics)
			}
			if payload.Semantics.Readiness.Message != tc.wantMessage {
				t.Fatalf("expected readiness message %q when loop is %s, got %+v", tc.wantMessage, tc.state, payload.Semantics.Readiness)
			}
			if payload.Semantics.DeploymentReadiness.State != "not_ready" {
				t.Fatalf("expected deployment readiness not_ready when loop is %s, got %+v", tc.state, payload.Semantics.DeploymentReadiness)
			}
			if payload.Semantics.DeploymentReadiness.Message != tc.wantMessage {
				t.Fatalf("expected deployment readiness message %q when loop is %s, got %+v", tc.wantMessage, tc.state, payload.Semantics.DeploymentReadiness)
			}
			if payload.Semantics.Degraded.State != "degraded" {
				t.Fatalf("expected degraded semantic when loop is %s, got %+v", tc.state, payload.Semantics)
			}
			if !strings.Contains(payload.Semantics.Degraded.Message, tc.wantDegraded) {
				t.Fatalf("expected degraded semantic to mention %q, got %q", tc.wantDegraded, payload.Semantics.Degraded.Message)
			}
		})
	}
}

func TestDiagnosticsPayload_TopLevelSemantics_DegradedOnProjectionLagCollectionError(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.SetState(loopNameDaemon, LoopRunning)
	cfg := app.Config{}
	payload := collectServiceHealthPayload(cfg, r, sqlite.MemoryProjectionLagSnapshot{
		State:   "unsupported",
		Message: "collect failed",
		Error:   "collect failed",
	})

	if payload.Semantics.Liveness.State != "ok" {
		t.Fatalf("expected liveness ok, got %q", payload.Semantics.Liveness.State)
	}
	if payload.Semantics.Readiness.State != "ok" {
		t.Fatalf("expected readiness ok when loops are running, got %q", payload.Semantics.Readiness.State)
	}
	if payload.Semantics.DeploymentReadiness.State != "degraded" {
		t.Fatalf("expected deployment readiness degraded when projection lag collection fails, got %q", payload.Semantics.DeploymentReadiness.State)
	}
	if payload.Semantics.Degraded.State != "degraded" {
		t.Fatalf("expected degraded state when projection lag collection fails, got %q", payload.Semantics.Degraded.State)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected top-level status degraded when projection lag collection fails, got %q", payload.Status)
	}
}

func TestDiagnosticsPayload_DegradedWhenRuntimeRevisionDiffersFromCheckoutHead(t *testing.T) {
	cfg := app.Config{}
	payload := collectServiceHealthPayloadFromState(
		cfg,
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "ok"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{State: "ok"},
		sqlite.AuthorityLeaseDiagnostics{},
		app.RuntimeBuildInfo{
			VCSRevision: "1111111111111111111111111111111111111111",
		},
		app.GitCheckoutInfo{
			Head: "2222222222222222222222222222222222222222",
		},
	)

	if payload.Status != "degraded" {
		t.Fatalf("expected top-level status degraded when runtime vcs revision drifts from checkout head, got %q", payload.Status)
	}
	if payload.Semantics.DeploymentReadiness.State != "degraded" {
		t.Fatalf("expected deployment readiness degraded when runtime vcs revision drifts from checkout head, got %q", payload.Semantics.DeploymentReadiness.State)
	}
	if payload.Semantics.Degraded.State != "degraded" {
		t.Fatalf("expected degraded semantic when runtime vcs revision drifts from checkout head, got %q", payload.Semantics.Degraded.State)
	}
	if !strings.Contains(payload.Semantics.Degraded.Message, "vcs_revision") {
		t.Fatalf("expected degraded message to mention vcs_revision drift, got %q", payload.Semantics.Degraded.Message)
	}
}

func TestDiagnosticsPayload_DegradedWhenFirehoseDropsEvents(t *testing.T) {
	r := NewReadinessRegistry()
	r.Register(loopNameDaemon)
	r.SetState(loopNameDaemon, LoopRunning)
	r.Register(loopNameFirehose)
	r.SetState(loopNameFirehose, LoopRunning)
	r.SetDroppedEvents(loopNameFirehose, 7)

	cfg := app.Config{}
	metricsFile := filepath.Join(t.TempDir(), "runtime_metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-08T12:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}
	cfg.MetricsPath = metricsFile

	payload := collectServiceHealthPayload(cfg, r, sqlite.MemoryProjectionLagSnapshot{State: "ok"})
	if payload.Status != "degraded" {
		t.Fatalf("expected top-level degraded status when firehose drops events, got %q", payload.Status)
	}
	if payload.Semantics.DeploymentReadiness.State != "degraded" {
		t.Fatalf("expected deployment readiness degraded when firehose drops events, got %q", payload.Semantics.DeploymentReadiness.State)
	}
	if payload.Semantics.Degraded.State != "degraded" {
		t.Fatalf("expected degraded top-level semantic when firehose drops events, got %q", payload.Semantics.Degraded.State)
	}
}

// ---------- Doctor TopLevelSemantics parsing ----------

func TestDoctor_TopLevelSemanticsCheck(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"semantics": map[string]any{
				"liveness":             map[string]any{"state": "ok", "message": "live"},
				"readiness":            map[string]any{"state": "ok", "message": "ready"},
				"deployment_readiness": map[string]any{"state": "ok", "message": "deployment ready"},
				"degraded":             map[string]any{"state": "degraded", "message": "partial outage"},
			},
		},
	}

	checks := checkTopLevelSemanticsFromDetails(details)
	if len(checks) != 4 {
		t.Fatalf("expected 4 semantic checks, got %d", len(checks))
	}

	for _, check := range checks {
		switch check.Name {
		case "service_liveness":
			if check.Status != doctorStatusPass {
				t.Errorf("liveness expected pass, got %s", check.Status)
			}
		case "service_readiness":
			if check.Status != doctorStatusPass {
				t.Errorf("readiness expected pass, got %s", check.Status)
			}
		case "service_deployment_readiness":
			if check.Status != doctorStatusPass {
				t.Errorf("deployment readiness expected pass, got %s", check.Status)
			}
		case "service_degraded":
			if check.Status != doctorStatusWarn {
				t.Errorf("degraded expected warn, got %s", check.Status)
			}
			if !strings.Contains(check.Message, "partial outage") {
				t.Errorf("degraded message missing cause: %s", check.Message)
			}
		}
	}
}

func TestDoctor_TopLevelSemanticsLimitedPublicPayloadFailsDeploymentReadiness(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"status": "ok",
			"ts":     "2026-04-10T12:00:00Z",
		},
	}

	checks := checkTopLevelSemanticsFromDetails(details)
	if len(checks) != 3 {
		t.Fatalf("expected 3 semantic checks for public payload, got %d", len(checks))
	}

	var (
		livenessCheck            doctorCheck
		readinessCheck           doctorCheck
		deploymentReadinessCheck doctorCheck
	)
	for _, check := range checks {
		switch check.Name {
		case "service_liveness":
			livenessCheck = check
		case "service_readiness":
			readinessCheck = check
		case "service_deployment_readiness":
			deploymentReadinessCheck = check
		}
	}

	if livenessCheck.Status != doctorStatusPass {
		t.Fatalf("expected public payload liveness to pass, got %+v", livenessCheck)
	}
	if readinessCheck.Status != doctorStatusWarn {
		t.Fatalf("expected public payload readiness to warn, got %+v", readinessCheck)
	}
	if deploymentReadinessCheck.Status != doctorStatusFail {
		t.Fatalf("expected public payload deployment readiness to fail, got %+v", deploymentReadinessCheck)
	}
	if !strings.Contains(strings.ToLower(deploymentReadinessCheck.Message), "/api/diagnostics") {
		t.Fatalf("expected deployment readiness failure to point at diagnostics endpoint, got %+v", deploymentReadinessCheck)
	}
}

func TestDoctor_RuntimeWorkGateWarnsWhenBootstrapFallbackCanConsumeWork(t *testing.T) {
	check := checkRuntimeWorkGateFromDetails(map[string]any{
		"service": serviceHealthPayload{
			RuntimeWorkGate: &RuntimeWorkGateDiagnostics{
				WorkType:      "not_evaluated",
				WorkGateState: "open",
				WorkGateType:  "bootstrap_compatibility_fallback",
				BootstrapWorkFallback: RuntimeBootstrapWorkFallbackDiagnostics{
					Posture:        "compatibility_enabled",
					GateState:      "open",
					Mode:           "tui",
					Selector:       "bootstrap_snapshot",
					Scope:          "non_daemon",
					CanConsumeWork: true,
					Summary:        "bootstrap compatibility fallback can consume work outside daemon mode",
				},
			},
		},
	})

	if check.Status != doctorStatusWarn {
		t.Fatalf("expected doctor warning when compatibility fallback can consume work, got %+v", check)
	}
	if !strings.Contains(check.Message, "can still consume work") {
		t.Fatalf("expected warning message to name fallback work consumption, got %+v", check)
	}
}

func TestDoctor_RuntimeWorkGateProfileGateClosedIsInformational(t *testing.T) {
	check := checkRuntimeWorkGateFromDetails(map[string]any{
		"service": serviceHealthPayload{
			RuntimeWorkGate: &RuntimeWorkGateDiagnostics{
				WorkType:              "profile_gate_closed",
				WorkCoordinationState: "profile_gate_closed",
				WorkGateState:         "closed",
				WorkGateType:          "profile_autonomous_execution",
				WorkGateReason:        "default_work_mode_observer",
				WorkGateNeededFrom:    "agent.profile.update",
				WorkGateSummary:       "Agent profile default_work_mode is observer.",
				BootstrapWorkFallback: RuntimeBootstrapWorkFallbackDiagnostics{
					Posture:        "disabled",
					GateState:      "closed",
					Mode:           "daemon",
					Selector:       "bootstrap_snapshot",
					Scope:          "agent_daemon",
					CanConsumeWork: false,
				},
			},
		},
	})

	if check.Status != doctorStatusPass {
		t.Fatalf("profile_gate_closed should be visible without blocking preflight, got %+v", check)
	}
	if !strings.Contains(check.Message, "profile_gate_closed") {
		t.Fatalf("expected profile gate status in doctor message, got %+v", check)
	}
}

func TestDoctor_ExtendedReadinessCheck_UnsupportedSignalsWarn(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "degraded", "message": "firehose is recovering"},
				"invalidation_lag":   map[string]any{"state": "unsupported", "message": "requires global O(1) index, currently missing"},
				"operator_queue_lag": map[string]any{"state": "unsupported", "message": "operator queue lag not collected"},
				"reviewer_scarcity":  map[string]any{"state": "unsupported", "message": "reviewer scarcity not collected"},
				"stuck_agents":       map[string]any{"state": "unsupported", "message": "stuck agent health not collected"},
				"projection_lag":     map[string]any{"state": "unsupported", "message": "projection visibility unavailable"},
				"replay_health":      map[string]any{"state": "unsupported", "message": "lacks global cross-workspace aggregation view"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for unsupported extended readiness, got %q: %s", check.Status, check.Message)
	}
	for _, want := range []string{"invalidation_lag", "operator_queue_lag", "reviewer_scarcity", "stuck_agents", "projection_lag", "replay_health", "motif_lifecycle"} {
		if !strings.Contains(check.Message, want) {
			t.Fatalf("expected %s in warning message: %s", want, check.Message)
		}
	}
}

func TestDoctor_ExtendedReadinessCheck_AdvisoryUnsupportedSignalsPass(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "ok", "message": "firehose running"},
				"invalidation_lag":   map[string]any{"state": "unsupported", "message": "requires global O(1) index, currently missing"},
				"operator_queue_lag": map[string]any{"state": "ok", "message": "no open operator queues are overdue"},
				"reviewer_scarcity":  map[string]any{"state": "ok", "message": "reviewer mesh healthy"},
				"stuck_agents":       map[string]any{"state": "ok", "message": "no stuck agents"},
				"no_progress":        map[string]any{"state": "ok", "message": "no repeated progress signatures"},
				"projection_lag":     map[string]any{"state": "ok", "message": "projection lag healthy"},
				"replay_health":      map[string]any{"state": "unsupported", "message": "lacks global cross-workspace aggregation view"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusPass {
		t.Fatalf("expected advisory unsupported extended readiness signals to pass, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "advisory unsupported") {
		t.Fatalf("expected advisory unsupported explanation in message: %s", check.Message)
	}
}

func TestDoctor_ExtendedReadinessCheck_DegradedOperatorQueueLagWarns(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "ok", "message": "firehose running"},
				"invalidation_lag":   map[string]any{"state": "ok", "message": "index present"},
				"operator_queue_lag": map[string]any{"state": "degraded", "message": "operator queue lag detected: overdue=2"},
				"projection_lag":     map[string]any{"state": "ok", "message": "projection lag healthy"},
				"replay_health":      map[string]any{"state": "ok", "message": "replay view present"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for degraded operator queue lag, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "operator_queue_lag: degraded") {
		t.Fatalf("expected operator_queue_lag degraded in warning message: %s", check.Message)
	}
}

func TestDoctor_ExtendedReadinessCheck_DegradedStaleOpenQueueWarns(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "ok", "message": "firehose running"},
				"invalidation_lag":   map[string]any{"state": "ok", "message": "index present"},
				"operator_queue_lag": map[string]any{"state": "degraded", "message": "operator queue lag detected: stale_open=1"},
				"projection_lag":     map[string]any{"state": "ok", "message": "projection lag healthy"},
				"replay_health":      map[string]any{"state": "ok", "message": "replay view present"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for degraded stale open queue lag, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "stale_open=1") {
		t.Fatalf("expected stale_open detail in warning message: %s", check.Message)
	}
}

func TestDoctor_ExtendedReadinessCheck_DegradedMissingOperatorQueueWarns(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "ok", "message": "firehose running"},
				"invalidation_lag":   map[string]any{"state": "ok", "message": "index present"},
				"operator_queue_lag": map[string]any{"state": "degraded", "message": "operator queue lag detected: missing_operator_queue=1"},
				"projection_lag":     map[string]any{"state": "ok", "message": "projection lag healthy"},
				"replay_health":      map[string]any{"state": "ok", "message": "replay view present"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for degraded missing operator queue signal, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "missing_operator_queue=1") {
		t.Fatalf("expected missing_operator_queue detail in warning message: %s", check.Message)
	}
}

func TestDoctor_ExtendedReadinessCheck_DegradedStuckAgentsWarns(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "ok", "message": "firehose running"},
				"invalidation_lag":   map[string]any{"state": "ok", "message": "index present"},
				"operator_queue_lag": map[string]any{"state": "ok", "message": "operator queue lag healthy"},
				"stuck_agents":       map[string]any{"state": "degraded", "message": "stuck agent risk detected: offline_agent_sessions=1, missing_agent_sessions=0"},
				"projection_lag":     map[string]any{"state": "ok", "message": "projection lag healthy"},
				"replay_health":      map[string]any{"state": "ok", "message": "replay view present"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for degraded stuck-agent signal, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "stuck_agents: degraded") {
		t.Fatalf("expected stuck_agents degraded in warning message: %s", check.Message)
	}
}

func TestDoctor_ExtendedReadinessCheck_DegradedReviewerScarcityWarns(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "ok", "message": "firehose running"},
				"invalidation_lag":   map[string]any{"state": "ok", "message": "index present"},
				"operator_queue_lag": map[string]any{"state": "ok", "message": "operator queue lag healthy"},
				"reviewer_scarcity":  map[string]any{"state": "degraded", "message": "reviewer scarcity detected: saturated_workspaces=1"},
				"projection_lag":     map[string]any{"state": "ok", "message": "projection lag healthy"},
				"replay_health":      map[string]any{"state": "ok", "message": "replay view present"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for degraded reviewer scarcity signal, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "reviewer_scarcity: degraded") {
		t.Fatalf("expected reviewer_scarcity degraded in warning message: %s", check.Message)
	}
}

func TestDoctor_ExtendedReadinessCheck_DegradedReviewerScarcityShowsWorkspaceExamples(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "ok", "message": "firehose running"},
				"invalidation_lag":   map[string]any{"state": "ok", "message": "index present"},
				"operator_queue_lag": map[string]any{"state": "ok", "message": "operator queue lag healthy"},
				"reviewer_scarcity":  map[string]any{"state": "degraded", "message": "reviewer scarcity detected: saturated_workspaces=1, scarce_workspaces=1"},
				"reviewer_scarcity_health": map[string]any{
					"state":                        "degraded",
					"message":                      "reviewer scarcity detected: saturated_workspaces=1, scarce_workspaces=1",
					"workspace_count":              float64(2),
					"saturated_workspace_count":    float64(1),
					"scarce_workspace_count":       float64(1),
					"saturated_workspace_examples": []any{"ws-hot"},
					"scarce_workspace_examples":    []any{"ws-tight"},
				},
				"projection_lag": map[string]any{"state": "ok", "message": "projection lag healthy"},
				"replay_health":  map[string]any{"state": "ok", "message": "replay view present"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for degraded reviewer scarcity signal with workspace examples, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "saturated=ws-hot") || !strings.Contains(check.Message, "scarce=ws-tight") {
		t.Fatalf("expected reviewer scarcity workspace examples in warning message, got %s", check.Message)
	}
	if _, ok := check.Details["reviewer_scarcity_health"]; !ok {
		t.Fatalf("expected reviewer_scarcity_health in doctor check details, got %+v", check.Details)
	}
}

func TestDoctor_ExtendedReadinessCheck_PartialUnknownReviewerScarcityWarns(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "ok", "message": "firehose running"},
				"invalidation_lag":   map[string]any{"state": "ok", "message": "index present"},
				"operator_queue_lag": map[string]any{"state": "ok", "message": "operator queue lag healthy"},
				"reviewer_scarcity":  map[string]any{"state": "degraded", "message": "reviewer scarcity health is partial: unknown_workspaces=1"},
				"projection_lag":     map[string]any{"state": "ok", "message": "projection lag healthy"},
				"replay_health":      map[string]any{"state": "ok", "message": "replay view present"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for partial unknown reviewer scarcity signal, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "reviewer_scarcity: degraded") {
		t.Fatalf("expected reviewer_scarcity degraded in warning message: %s", check.Message)
	}
	if !strings.Contains(check.Message, "unknown_workspaces=1") {
		t.Fatalf("expected unknown reviewer scarcity detail in warning message: %s", check.Message)
	}
}

func TestDoctor_ExtendedReadinessCheck_PartialReviewerScarcityAdvises(t *testing.T) {
	details := map[string]any{
		"service": map[string]any{
			"extended_readiness": map[string]any{
				"motif_lifecycle":    map[string]any{"state": "ok", "message": "firehose running"},
				"invalidation_lag":   map[string]any{"state": "ok", "message": "index present"},
				"operator_queue_lag": map[string]any{"state": "ok", "message": "operator queue lag healthy"},
				"reviewer_scarcity":  map[string]any{"state": "partial", "message": "reviewer scarcity health is partial: unknown_workspaces=1"},
				"projection_lag":     map[string]any{"state": "ok", "message": "projection lag healthy"},
				"replay_health":      map[string]any{"state": "ok", "message": "replay view present"},
			},
		},
	}

	check := checkExtendedReadinessFromDetails(details)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for partial reviewer scarcity advisory, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "reviewer_scarcity: partial") || !strings.Contains(check.Message, "unknown_workspaces=1") {
		t.Fatalf("expected reviewer_scarcity partial advisory in message: %s", check.Message)
	}
}

func TestDoctor_AuthorityNodeCheck(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		check := checkAuthorityNodeFromDetails(map[string]any{
			"service": map[string]any{
				"authority_node": map[string]any{
					"state":             "ok",
					"authority_node_id": "authnode-1",
					"node_kind":         "process_local",
					"status":            "ONLINE",
				},
			},
		})
		if check.Status != doctorStatusPass {
			t.Fatalf("expected pass for authority node ok, got %+v", check)
		}
	})

	t.Run("missing", func(t *testing.T) {
		check := checkAuthorityNodeFromDetails(map[string]any{
			"service": map[string]any{
				"authority_node": map[string]any{
					"state":   "missing",
					"message": "authority identity file missing",
				},
			},
		})
		if check.Status != doctorStatusWarn {
			t.Fatalf("expected warn for missing authority node, got %+v", check)
		}
		if !strings.Contains(check.Message, "missing") {
			t.Fatalf("expected missing authority message, got %+v", check)
		}
	})

	t.Run("error", func(t *testing.T) {
		check := checkAuthorityNodeFromDetails(map[string]any{
			"service": map[string]any{
				"authority_node": map[string]any{
					"state":   "error",
					"message": "authority identity drift detected",
				},
			},
		})
		if check.Status != doctorStatusFail {
			t.Fatalf("expected fail for authority node error, got %+v", check)
		}
		if !strings.Contains(check.Message, "drift") {
			t.Fatalf("expected authority drift message, got %+v", check)
		}
	})

	t.Run("offline_status", func(t *testing.T) {
		check := checkAuthorityNodeFromDetails(map[string]any{
			"service": map[string]any{
				"authority_node": map[string]any{
					"state":             "degraded",
					"message":           "authority runtime node status is OFFLINE",
					"authority_node_id": "authnode-1",
					"node_kind":         "process_local",
					"status":            "OFFLINE",
				},
			},
		})
		if check.Status != doctorStatusWarn {
			t.Fatalf("expected warn for offline authority node, got %+v", check)
		}
		if !strings.Contains(strings.ToLower(check.Message), "offline") {
			t.Fatalf("expected offline authority message, got %+v", check)
		}
	})

	t.Run("limited_public_payload", func(t *testing.T) {
		check := checkAuthorityNodeFromDetails(map[string]any{
			"service": map[string]any{
				"status": "ok",
				"ts":     "2026-04-10T12:00:00Z",
			},
		})
		if check.Status != doctorStatusWarn {
			t.Fatalf("expected warn for limited payload without authority diagnostics, got %+v", check)
		}
		if !strings.Contains(strings.ToLower(check.Message), "missing") {
			t.Fatalf("expected missing authority diagnostics message, got %+v", check)
		}
	})
}

func TestDoctor_AuthorityLeaseCheck(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		check := checkAuthorityLeaseFromDetails(map[string]any{
			"service": map[string]any{
				"authority_lease": map[string]any{
					"state":        "ok",
					"message":      "local authority lease diagnostics are healthy",
					"scope":        "workspace",
					"reference_at": "2026-04-12T10:00:00Z",
					"total_held":   float64(1),
					"healthy":      float64(1),
				},
			},
		})
		if check.Status != doctorStatusPass {
			t.Fatalf("expected pass for healthy authority lease, got %+v", check)
		}
	})

	t.Run("renew_due", func(t *testing.T) {
		check := checkAuthorityLeaseFromDetails(map[string]any{
			"service": map[string]any{
				"authority_lease": map[string]any{
					"state":      "ok",
					"message":    "local authority lease diagnostics: renew_due=1",
					"total_held": float64(1),
					"renew_due":  float64(1),
				},
			},
		})
		if check.Status != doctorStatusWarn {
			t.Fatalf("expected warn for renew_due authority lease, got %+v", check)
		}
		if !strings.Contains(check.Message, "renew_due") {
			t.Fatalf("expected renew_due message, got %+v", check)
		}
	})

	t.Run("grace", func(t *testing.T) {
		check := checkAuthorityLeaseFromDetails(map[string]any{
			"service": map[string]any{
				"authority_lease": map[string]any{
					"state":      "degraded",
					"message":    "local authority lease diagnostics: grace=1",
					"total_held": float64(1),
					"grace":      float64(1),
				},
			},
		})
		if check.Status != doctorStatusWarn {
			t.Fatalf("expected warn for grace authority lease, got %+v", check)
		}
		if !strings.Contains(strings.ToLower(check.Message), "grace") {
			t.Fatalf("expected grace authority lease message, got %+v", check)
		}
	})

	t.Run("foreign_live", func(t *testing.T) {
		check := checkAuthorityLeaseFromDetails(map[string]any{
			"service": map[string]any{
				"authority_lease": map[string]any{
					"state":        "degraded",
					"message":      "workspace authority lease diagnostics: foreign_live=1",
					"foreign_live": float64(1),
				},
			},
		})
		if check.Status != doctorStatusWarn {
			t.Fatalf("expected warn for foreign_live authority lease, got %+v", check)
		}
		if !strings.Contains(strings.ToLower(check.Message), "foreign_live") {
			t.Fatalf("expected foreign_live authority lease message, got %+v", check)
		}
	})

	t.Run("missing_payload", func(t *testing.T) {
		check := checkAuthorityLeaseFromDetails(map[string]any{
			"service": map[string]any{
				"status": "ok",
			},
		})
		if check.Status != doctorStatusWarn {
			t.Fatalf("expected warn for missing authority lease diagnostics, got %+v", check)
		}
		if !strings.Contains(strings.ToLower(check.Message), "missing") {
			t.Fatalf("expected missing authority lease diagnostics message, got %+v", check)
		}
	})
}

func TestDoctor_RuntimeMetricsCheck_ParseErrorsWarn(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-08T12:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}
{broken_line}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	check := checkRuntimeMetrics(metricsFile)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for parse errors, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(check.Message, "invalid lines") {
		t.Fatalf("expected parse-error message to mention invalid lines, got %s", check.Message)
	}
}

func TestDoctor_RuntimeMetricsCheck_UnknownHealthWarns(t *testing.T) {
	tmp := t.TempDir()
	metricsFile := filepath.Join(tmp, "metrics.jsonl")
	if err := os.WriteFile(metricsFile, []byte(`{"schema_version":"1.0","timestamp":"2026-04-11T00:00:00Z","profiles":{"compute":{"total_runs":0,"success_count":0,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.0,"avg_startup_ms":0}}}`), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	check := checkRuntimeMetrics(metricsFile)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected warn for unknown runtime metrics health, got %q: %s", check.Status, check.Message)
	}
	if !strings.Contains(strings.ToLower(check.Message), "unknown") {
		t.Fatalf("expected unknown-health message, got %s", check.Message)
	}
	if verdict, _ := check.Details["health_verdict"].(string); verdict != "unknown" {
		t.Fatalf("expected health verdict unknown in details, got %+v", check.Details)
	}
}

func mutationActivationGateByNameForReadinessTest(t *testing.T, result repoauthority.MutationActivationGateResult, name string) repoauthority.MutationActivationGate {
	t.Helper()
	for _, gate := range result.Gates {
		if gate.Name == name {
			return gate
		}
	}
	t.Fatalf("gate %q not found in %+v", name, result.Gates)
	return repoauthority.MutationActivationGate{}
}

func stringSliceContainsForReadinessTest(values []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}
