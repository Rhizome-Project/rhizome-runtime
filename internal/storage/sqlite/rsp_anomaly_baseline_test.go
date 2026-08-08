package sqlite

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestProcessRSPAnomalyTelemetryWarmupPersistsExactAndFallbackBaselines(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-anomaly-baseline-warmup"
		agentID     = "agent-a"
		taskID      = "task-rsp-anomaly-baseline-warmup"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, agentID)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-a")
	if _, err := store.PutTaskClassEvidence(ctx, TaskClassEvidencePutInput{
		WorkspaceID:     workspaceID,
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "tests",
	}); err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}

	store.ProcessRSPAnomalyTelemetry(ctx, RuntimeEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "verifier.fail",
		EntityType:  "FACT",
		EntityID:    "claim-warmup",
		AgentID:     agentID,
		TaskID:      taskID,
		CreatedAt:   "2026-03-30T10:00:00Z",
	})

	exact := requireRSPAnomalyBaselineRow(t, ctx, store, rspAnomalyScopeKey{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskClass:   model.TaskClassIncident,
		Mode:        rspAnomalyDefaultMode,
		Phase:       rspAnomalyDefaultPhase,
		MetricName:  "verifier_fail_rate",
	})
	if exact.SampleSize != 1 || exact.LastHealthyWindowAt == "" {
		t.Fatalf("expected exact warmup baseline row, got %+v", exact)
	}
	if exact.MuHat <= 0 || exact.MuHat >= 1 {
		t.Fatalf("expected bounded diversity-discounted warmup mean, got %+v", exact)
	}

	agentDefault := requireRSPAnomalyBaselineRow(t, ctx, store, rspAnomalyScopeKey{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskClass:   rspAnomalyDefaultTaskClass,
		Mode:        rspAnomalyDefaultMode,
		Phase:       rspAnomalyDefaultPhase,
		MetricName:  "verifier_fail_rate",
	})
	if agentDefault.SampleSize != 1 {
		t.Fatalf("expected agent default baseline row, got %+v", agentDefault)
	}

	workspaceDefault := requireRSPAnomalyBaselineRow(t, ctx, store, rspAnomalyScopeKey{
		WorkspaceID: workspaceID,
		AgentID:     "",
		TaskClass:   rspAnomalyDefaultTaskClass,
		Mode:        rspAnomalyDefaultMode,
		Phase:       rspAnomalyDefaultPhase,
		MetricName:  "verifier_fail_rate",
	})
	if workspaceDefault.SampleSize != 1 {
		t.Fatalf("expected workspace default baseline row, got %+v", workspaceDefault)
	}

	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{WorkspaceID: workspaceID, Limit: 20})
	if err != nil {
		t.Fatalf("dump rsp telemetry: %v", err)
	}
	if len(dump.AnomalyBaselines) < 3 {
		t.Fatalf("expected persisted anomaly baselines in dump, got %+v", dump.AnomalyBaselines)
	}
	if len(dump.AnomalyLogs) != 0 {
		t.Fatalf("expected warmup path to update baselines without alerts, got %+v", dump.AnomalyLogs)
	}
}

func TestLoadRSPAnomalyBaselineFallsBackToAgentDefault(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-anomaly-baseline-fallback"
		agentID     = "agent-a"
		taskID      = "task-rsp-anomaly-baseline-fallback"
		healthyAt   = "2026-03-29T09:00:00Z"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, agentID)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-a")
	if _, err := store.PutTaskClassEvidence(ctx, TaskClassEvidencePutInput{
		WorkspaceID:     workspaceID,
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "tests",
	}); err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}
	if err := store.upsertRSPAnomalyBaselineObservation(ctx, rspAnomalyScopeKey{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskClass:   rspAnomalyDefaultTaskClass,
		Mode:        rspAnomalyDefaultMode,
		Phase:       rspAnomalyDefaultPhase,
		MetricName:  "verifier_fail_rate",
	}, 0.1, healthyAt); err != nil {
		t.Fatalf("seed agent default baseline row 1: %v", err)
	}
	if err := store.upsertRSPAnomalyBaselineObservation(ctx, rspAnomalyScopeKey{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskClass:   rspAnomalyDefaultTaskClass,
		Mode:        rspAnomalyDefaultMode,
		Phase:       rspAnomalyDefaultPhase,
		MetricName:  "verifier_fail_rate",
	}, 0.1, healthyAt); err != nil {
		t.Fatalf("seed agent default baseline row 2: %v", err)
	}
	if err := store.upsertRSPAnomalyBaselineObservation(ctx, rspAnomalyScopeKey{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskClass:   rspAnomalyDefaultTaskClass,
		Mode:        rspAnomalyDefaultMode,
		Phase:       rspAnomalyDefaultPhase,
		MetricName:  "verifier_fail_rate",
	}, 0.1, healthyAt); err != nil {
		t.Fatalf("seed agent default baseline row 3: %v", err)
	}

	baseline, scopeName := store.loadRSPAnomalyBaselineWithFallback(ctx, rspAnomalyScopeKey{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskClass:   model.TaskClassIncident,
		Mode:        rspAnomalyDefaultMode,
		Phase:       rspAnomalyDefaultPhase,
		MetricName:  "verifier_fail_rate",
	})
	if scopeName != rspAnomalyScopeAgentDefault {
		t.Fatalf("expected agent-default fallback scope, got %s with %+v", scopeName, baseline)
	}
	if baseline.SampleSize != 3 || baseline.LastHealthyWindowAt != healthyAt {
		t.Fatalf("expected persisted fallback baseline provenance, got %+v", baseline)
	}
	if baseline.TaskClass != rspAnomalyDefaultTaskClass || baseline.AgentID != agentID {
		t.Fatalf("expected fallback baseline to come from agent default scope, got %+v", baseline)
	}
}

func TestProcessRSPAnomalyTelemetryUsesWarmedExactBaselineForAlertMoments(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-anomaly-baseline-exact-moments"
		agentID     = "agent-a"
		taskID      = "task-rsp-anomaly-baseline-exact-moments"
		healthyAt   = "2026-03-29T09:00:00Z"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, agentID)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-a")
	if _, err := store.PutTaskClassEvidence(ctx, TaskClassEvidencePutInput{
		WorkspaceID:     workspaceID,
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "tests",
	}); err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}
	for i := 0; i < rspAnomalyWarmupSampleFloor; i++ {
		if err := store.upsertRSPAnomalyBaselineObservation(ctx, rspAnomalyScopeKey{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			TaskClass:   model.TaskClassIncident,
			Mode:        rspAnomalyDefaultMode,
			Phase:       rspAnomalyDefaultPhase,
			MetricName:  "patch_rate",
		}, 0.8, healthyAt); err != nil {
			t.Fatalf("seed exact baseline row %d: %v", i, err)
		}
	}

	store.ProcessRSPAnomalyTelemetry(ctx, RuntimeEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "task.update",
		EntityType:  "FACT",
		EntityID:    "claim-exact-moments",
		AgentID:     agentID,
		TaskID:      taskID,
		CreatedAt:   "2026-03-30T10:00:00Z",
	})

	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{WorkspaceID: workspaceID, Limit: 20})
	if err != nil {
		t.Fatalf("dump rsp telemetry: %v", err)
	}
	if len(dump.AnomalyLogs) != 0 {
		t.Fatalf("expected warmed exact baseline to keep in-band patch observation below alert threshold, got %+v", dump.AnomalyLogs)
	}
}

func TestProcessRSPAnomalyTelemetryUsesWarmedWorkspaceFallbackForAlertMoments(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-anomaly-baseline-workspace-fallback"
		agentID     = "agent-a"
		taskID      = "task-rsp-anomaly-baseline-workspace-fallback"
		healthyAt   = "2026-03-29T09:00:00Z"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, agentID)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-a")
	if _, err := store.PutTaskClassEvidence(ctx, TaskClassEvidencePutInput{
		WorkspaceID:     workspaceID,
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "tests",
	}); err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}
	for i := 0; i < rspAnomalyWarmupSampleFloor; i++ {
		if err := store.upsertRSPAnomalyBaselineObservation(ctx, rspAnomalyScopeKey{
			WorkspaceID: workspaceID,
			AgentID:     "",
			TaskClass:   rspAnomalyDefaultTaskClass,
			Mode:        rspAnomalyDefaultMode,
			Phase:       rspAnomalyDefaultPhase,
			MetricName:  "verifier_fail_rate",
		}, 0.1, healthyAt); err != nil {
			t.Fatalf("seed workspace fallback baseline row %d: %v", i, err)
		}
	}

	store.ProcessRSPAnomalyTelemetry(ctx, RuntimeEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "verifier.fail",
		EntityType:  "FACT",
		EntityID:    "claim-workspace-fallback",
		AgentID:     agentID,
		TaskID:      taskID,
		CreatedAt:   "2026-03-30T10:00:00Z",
	})

	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{WorkspaceID: workspaceID, Limit: 20})
	if err != nil {
		t.Fatalf("dump rsp telemetry: %v", err)
	}
	if len(dump.AnomalyLogs) == 0 {
		t.Fatalf("expected warmed workspace fallback baseline to drive alert provenance, got %+v", dump)
	}
	latest := dump.AnomalyLogs[0]
	if latest.BaselineScope != rspAnomalyScopeWorkspace || latest.BaselineSampleSize != rspAnomalyWarmupSampleFloor {
		t.Fatalf("expected workspace-default fallback provenance, got %+v", latest)
	}
	if latest.BaselineLastHealthyWindow != healthyAt {
		t.Fatalf("expected workspace fallback healthy-window provenance, got %+v", latest)
	}
	if latest.MuHat != 0.1 || latest.SigmaHat != 0.1 {
		t.Fatalf("expected warmed workspace fallback moments to drive anomaly log, got %+v", latest)
	}
}

func TestProcessRSPAnomalyTelemetryPersistsVersionedCalibrationProvenance(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-anomaly-versioned"
		agentID     = "agent-a"
		taskID      = "task-rsp-anomaly-versioned"
		healthyAt   = "2026-03-29T09:00:00Z"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, agentID)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-a")
	if _, err := store.PutTaskClassEvidence(ctx, TaskClassEvidencePutInput{
		WorkspaceID:     workspaceID,
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "tests",
	}); err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}
	for i := 0; i < rspAnomalyWarmupSampleFloor; i++ {
		if err := store.upsertRSPAnomalyBaselineObservation(ctx, rspAnomalyScopeKey{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			TaskClass:   model.TaskClassIncident,
			Mode:        rspAnomalyDefaultMode,
			Phase:       rspAnomalyDefaultPhase,
			MetricName:  "verifier_fail_rate",
		}, 0.1, healthyAt); err != nil {
			t.Fatalf("seed versioned baseline row %d: %v", i, err)
		}
	}

	store.ProcessRSPAnomalyTelemetry(ctx, RuntimeEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "verifier.fail",
		EntityType:  "FACT",
		EntityID:    "claim-versioned",
		AgentID:     agentID,
		TaskID:      taskID,
		CreatedAt:   "2026-03-30T10:00:00Z",
	})

	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{WorkspaceID: workspaceID, Limit: 20})
	if err != nil {
		t.Fatalf("dump rsp telemetry: %v", err)
	}
	if len(dump.AnomalyLogs) == 0 {
		t.Fatalf("expected anomaly alert log after versioned warm baseline, got %+v", dump)
	}
	if dump.Summary.CalibrationIntegrityBand != "VERSIONED" || len(dump.Summary.CalibrationGaps) != 0 {
		t.Fatalf("expected fully versioned anomaly-only fixture to stay versioned, got %+v", dump.Summary)
	}
	latest := dump.AnomalyLogs[0]
	if latest.CalibrationProfile != rspAnomalyTelemetryCalibrationContract().Basis ||
		latest.CalibrationVersion != rspAnomalyTelemetryCalibrationContract().CalibrationVersion {
		t.Fatalf("expected anomaly log to persist versioned calibration provenance, got %+v", latest)
	}
	foundVersionedBaseline := false
	for _, item := range dump.AnomalyBaselines {
		if item.AgentID == agentID && item.TaskClass == model.TaskClassIncident && item.MetricName == "verifier_fail_rate" {
			foundVersionedBaseline = true
			if item.CalibrationProfile != rspAnomalyTelemetryCalibrationContract().Basis ||
				item.CalibrationVersion != rspAnomalyTelemetryCalibrationContract().CalibrationVersion {
				t.Fatalf("expected anomaly baseline to persist versioned calibration provenance, got %+v", item)
			}
		}
	}
	if !foundVersionedBaseline {
		t.Fatalf("expected versioned exact anomaly baseline row, got %+v", dump.AnomalyBaselines)
	}
}

func TestProcessRSPAnomalyTelemetryShrinksColdExactBaselineTowardWarmedAgentFallback(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-anomaly-baseline-shrink"
		agentID     = "agent-a"
		taskID      = "task-rsp-anomaly-baseline-shrink"
		exactAt     = "2026-03-29T09:00:00Z"
		fallbackAt  = "2026-03-29T09:05:00Z"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, agentID)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-a")
	if _, err := store.PutTaskClassEvidence(ctx, TaskClassEvidencePutInput{
		WorkspaceID:     workspaceID,
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "tests",
	}); err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, agentID, model.TaskClassIncident, rspAnomalyDefaultMode, rspAnomalyDefaultPhase, "verifier_fail_rate", 0.6, 0.05, 1, exactAt,
		workspaceID, agentID, rspAnomalyDefaultTaskClass, rspAnomalyDefaultMode, rspAnomalyDefaultPhase, "verifier_fail_rate", 0.1, 0.1, rspAnomalyWarmupSampleFloor, fallbackAt,
	); err != nil {
		t.Fatalf("seed anomaly baseline rows: %v", err)
	}

	store.ProcessRSPAnomalyTelemetry(ctx, RuntimeEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "verifier.fail",
		EntityType:  "FACT",
		EntityID:    "claim-shrink",
		AgentID:     agentID,
		TaskID:      taskID,
		CreatedAt:   "2026-03-30T10:00:00Z",
	})

	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{WorkspaceID: workspaceID, Limit: 20})
	if err != nil {
		t.Fatalf("dump rsp telemetry: %v", err)
	}
	if len(dump.AnomalyLogs) == 0 {
		t.Fatalf("expected shrunk exact baseline to produce anomaly provenance, got %+v", dump)
	}
	latest := dump.AnomalyLogs[0]
	if latest.BaselineScope != rspAnomalyScopeExactShrunkAgent {
		t.Fatalf("expected shrunk exact-agent fallback provenance, got %+v", latest)
	}
	if latest.BaselineSampleSize != 1+rspAnomalyWarmupSampleFloor {
		t.Fatalf("expected effective shrunk sample size, got %+v", latest)
	}
	if latest.BaselineLastHealthyWindow != fallbackAt {
		t.Fatalf("expected shrunk baseline to preserve later fallback healthy window, got %+v", latest)
	}
	if latest.MuHat <= 0.1 || latest.MuHat >= 0.6 {
		t.Fatalf("expected shrunk baseline mean between exact and fallback moments, got %+v", latest)
	}
	if math.Abs(latest.MuHat-0.2666666667) > 0.001 {
		t.Fatalf("expected bounded exact-to-agent shrinkage moment, got %+v", latest)
	}
}

func TestProcessRSPAnomalyTelemetryShrinksColdExactBaselineTowardWarmedWorkspaceFallback(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-anomaly-baseline-shrink-ws"
		agentID     = "agent-a"
		taskID      = "task-rsp-anomaly-baseline-shrink-ws"
		exactAt     = "2026-03-29T09:00:00Z"
		fallbackAt  = "2026-03-29T09:06:00Z"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, agentID)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-a")
	if _, err := store.PutTaskClassEvidence(ctx, TaskClassEvidencePutInput{
		WorkspaceID:     workspaceID,
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "tests",
	}); err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, agentID, model.TaskClassIncident, rspAnomalyDefaultMode, rspAnomalyDefaultPhase, "verifier_fail_rate", 0.6, 0.05, 1, exactAt,
		workspaceID, "", rspAnomalyDefaultTaskClass, rspAnomalyDefaultMode, rspAnomalyDefaultPhase, "verifier_fail_rate", 0.1, 0.1, rspAnomalyWarmupSampleFloor, fallbackAt,
	); err != nil {
		t.Fatalf("seed anomaly baseline rows: %v", err)
	}

	store.ProcessRSPAnomalyTelemetry(ctx, RuntimeEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "verifier.fail",
		EntityType:  "FACT",
		EntityID:    "claim-shrink-ws",
		AgentID:     agentID,
		TaskID:      taskID,
		CreatedAt:   "2026-03-30T10:00:00Z",
	})

	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{WorkspaceID: workspaceID, Limit: 20})
	if err != nil {
		t.Fatalf("dump rsp telemetry: %v", err)
	}
	if len(dump.AnomalyLogs) == 0 {
		t.Fatalf("expected shrunk exact baseline to workspace fallback provenance, got %+v", dump)
	}
	latest := dump.AnomalyLogs[0]
	if latest.BaselineScope != rspAnomalyScopeExactShrunkWS {
		t.Fatalf("expected shrunk exact-workspace fallback provenance, got %+v", latest)
	}
	if latest.BaselineSampleSize != 1+rspAnomalyWarmupSampleFloor {
		t.Fatalf("expected effective shrunk sample size, got %+v", latest)
	}
	if latest.BaselineLastHealthyWindow != fallbackAt {
		t.Fatalf("expected shrunk baseline to preserve workspace fallback healthy window, got %+v", latest)
	}
	if latest.MuHat <= 0.1 || latest.MuHat >= 0.6 {
		t.Fatalf("expected shrunk baseline mean between exact and workspace fallback moments, got %+v", latest)
	}
	if dump.Summary.ShrinkageFallbackQualityBand != "WORKSPACE_FALLBACK" || dump.Summary.AnomalyReadinessBand != "WARMING" || dump.Summary.ReadinessBand != "WARMING" {
		t.Fatalf("expected workspace fallback shrinkage to downgrade anomaly and overall telemetry readiness, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageFallbackScopeTier != "WORKSPACE_TIER" {
		t.Fatalf("expected workspace-tier shrinkage provenance on summary, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceFallbackMixBand != "EXACT_ONLY" {
		t.Fatalf("expected exact-only workspace fallback mix on summary, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureBand != "ALL_SHRUNK" {
		t.Fatalf("expected exact-only workspace fallback to keep all-shrunk workspace-tier pressure on summary, got %+v", dump.Summary)
	}
	if !reflect.DeepEqual(dump.Summary.WorkspaceFallbackMixCounts, map[string]int{
		"exact_workspace":         1,
		"agent_default_workspace": 0,
	}) {
		t.Fatalf("expected exact-only workspace fallback mix counts on summary, got %+v", dump.Summary.WorkspaceFallbackMixCounts)
	}
	if !reflect.DeepEqual(dump.Summary.WorkspaceTierPressureCounts, map[string]int{
		"workspace_tier": 1,
		"agent_tier":     0,
	}) {
		t.Fatalf("expected exact-only workspace tier pressure counts on summary, got %+v", dump.Summary.WorkspaceTierPressureCounts)
	}
	if dump.Summary.AnomalyWarmingDriver != "ALL_SHRUNK_WORKSPACE_FALLBACK" {
		t.Fatalf("expected workspace-fallback shrinkage warming driver on summary, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil || dump.Summary.ReadinessCoverageRollup.OverallReadinessBand != "WARMING" {
		t.Fatalf("expected readiness rollup to mirror workspace fallback downgrade, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_baseline_cold") {
		t.Fatalf("expected all-shrunk workspace fallback downgrade to avoid cold-baseline gap, got %+v", dump.Summary)
	}
}

func TestDumpRSPTelemetryBuildsCalibrationSummary(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-rsp-telemetry-summary"

	store.appendBeliefTelemetryLog(ctx, workspaceID, "evt-belief-1", "FACT", "claim-1", 0.0, 0.4, 0.7, 1.0, 0.6)
	store.appendBeliefTelemetryLog(ctx, workspaceID, "evt-belief-2", "FACT", "claim-2", 0.0, 0.2, 0.2, 1.0, 0.1)
	contract := rspAnomalyTelemetryCalibrationContract()
	store.appendAnomalyTelemetryLog(ctx, workspaceID, "DEFAULT", "verifier_fail_rate", model.TaskClassIncident, rspAnomalyDefaultPhase, rspAnomalyScopeWorkspace, contract.Basis, contract.CalibrationVersion, rspAnomalyWarmupSampleFloor, "2026-03-29T09:00:00Z", 0.1, 0.1, 0.8, 0.52, 0.8, "THRASHING", "2026-03-30T10:00:00Z")
	store.appendAnomalyTelemetryLog(ctx, workspaceID, "DEFAULT", "patch_rate", model.TaskClassIncident, rspAnomalyDefaultPhase, rspAnomalyScopeExact, contract.Basis, contract.CalibrationVersion, rspAnomalyWarmupSampleFloor+1, "2026-03-29T09:05:00Z", 0.8, 0.1, 0.1, 0.24, 0.8, "STAGNATION", "2026-03-30T10:01:00Z")
	store.appendAnomalyTelemetryLog(ctx, workspaceID, "DEFAULT", "queue_latency", model.TaskClassIncident, rspAnomalyDefaultPhase, rspAnomalyScopeExactShrunkAgent, contract.Basis, contract.CalibrationVersion, rspAnomalyWarmupSampleFloor+1, "2026-03-29T09:10:00Z", 0.45, 0.08, 0.91, 0.71, 0.7, "THRASHING", "2026-03-30T10:01:30Z")
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "", rspAnomalyDefaultTaskClass, rspAnomalyDefaultMode, rspAnomalyDefaultPhase, "verifier_fail_rate", 0.1, 0.1, rspAnomalyWarmupSampleFloor, "2026-03-29T09:00:00Z",
		workspaceID, "agent-a", rspAnomalyDefaultTaskClass, rspAnomalyDefaultMode, rspAnomalyDefaultPhase, "patch_rate", 0.25, 0.12, 2, "2026-03-29T09:01:00Z",
		workspaceID, "agent-a", model.TaskClassIncident, rspAnomalyDefaultMode, rspAnomalyDefaultPhase, "patch_rate", 0.8, 0.1, rspAnomalyWarmupSampleFloor+1, "2026-03-29T09:05:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline rows: %v", err)
	}
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO rsp_agent_state_telemetry(id, workspace_id, agent_id, cache_pressure_i, stale_hit_i, thrashing_risk, ungrounded_risk, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)`,
		"stlog-1", workspaceID, "agent-a", 0.7, 0.4, 0.8, 0.1, "2026-03-30T10:02:00Z",
		"stlog-2", workspaceID, "agent-b", 0.2, 0.1, 0.2, 0.7, "2026-03-30T10:01:30Z",
	); err != nil {
		t.Fatalf("seed state telemetry rows: %v", err)
	}

	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{WorkspaceID: workspaceID, Limit: 20})
	if err != nil {
		t.Fatalf("dump rsp telemetry: %v", err)
	}
	if dump.SchemaVersion != rspCalibrationSchemaVersion {
		t.Fatalf("expected telemetry dump schema version %q, got %+v", rspCalibrationSchemaVersion, dump)
	}
	if dump.CalibrationContracts.Belief.Status != rspCalibrationStatusProvisional ||
		dump.CalibrationContracts.Belief.CalibrationVersion != "belief-heuristic-event-v1" ||
		!containsString(dump.CalibrationContracts.Belief.Unsupported, "historical_priors") {
		t.Fatalf("expected belief telemetry contract to stay provisional and explicit, got %+v", dump.CalibrationContracts.Belief)
	}
	if dump.CalibrationContracts.Anomaly.Status != rspCalibrationStatusShadowOnly ||
		dump.CalibrationContracts.Anomaly.CalibrationVersion != "anomaly-ewma-shrinkage-v1" {
		t.Fatalf("expected anomaly telemetry contract to stay shadow-only, got %+v", dump.CalibrationContracts.Anomaly)
	}
	if dump.CalibrationContracts.State.Status != rspCalibrationStatusShadowOnly ||
		dump.CalibrationContracts.State.CalibrationVersion != "state-shadow-s1-v1" {
		t.Fatalf("expected state telemetry contract to stay shadow-only, got %+v", dump.CalibrationContracts.State)
	}
	if dump.Summary.UnversionedAnomalyAlertCount != 0 || dump.Summary.UnversionedAnomalyBaselineCount != 3 {
		t.Fatalf("expected summary to surface mixed versioned/legacy anomaly coverage honestly, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyCalibrationVersionCounts["anomaly-ewma-shrinkage-v1"] != 3 {
		t.Fatalf("expected summary to count versioned anomaly alerts, got %+v", dump.Summary.AnomalyCalibrationVersionCounts)
	}
	if dump.Summary.BeliefUnversionedLogCount != 2 || dump.Summary.StateUnversionedLogCount != 2 {
		t.Fatalf("expected summary to keep unversioned belief/state row counts explicit, got %+v", dump.Summary)
	}
	if dump.Summary.CalibrationIntegrityBand != "MIXED_LEGACY" ||
		!containsString(dump.Summary.CalibrationGaps, "anomaly_rows_unversioned") ||
		!containsString(dump.Summary.CalibrationGaps, "belief_rows_unversioned") ||
		!containsString(dump.Summary.CalibrationGaps, "state_rows_unversioned") {
		t.Fatalf("expected calibration integrity rollup to stay explicit under mixed legacy rows, got %+v", dump.Summary)
	}
	if dump.Summary.BeliefLogCount != 2 || dump.Summary.BeliefHighUncertaintyCount != 1 || dump.Summary.BeliefHighDriftCount != 1 {
		t.Fatalf("expected belief calibration summary counts, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyAlertCount != 3 || dump.Summary.WarmedAnomalyAlertCount != 3 || dump.Summary.ThrashingAlertCount != 2 || dump.Summary.StagnationAlertCount != 1 {
		t.Fatalf("expected anomaly calibration summary counts, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyBaselineCount != 3 || dump.Summary.WarmAnomalyBaselineCount != 2 || dump.Summary.AnomalyLogsWithBaselineCount != 3 {
		t.Fatalf("expected baseline calibration summary counts, got %+v", dump.Summary)
	}
	if dump.Summary.ShrunkAnomalyAlertCount != 1 || dump.Summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkAgent] != 1 {
		t.Fatalf("expected bounded shrinkage provenance rollup in summary, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageRelianceBand != "PARTIAL" {
		t.Fatalf("expected partial shrinkage reliance band, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageFallbackQualityBand != "AGENT_LOCALIZED" {
		t.Fatalf("expected agent-localized shrinkage fallback quality, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageFallbackScopeTier != "AGENT_TIER" {
		t.Fatalf("expected agent-tier shrinkage provenance on summary, got %+v", dump.Summary)
	}
	if !reflect.DeepEqual(dump.Summary.WorkspaceTierPressureCounts, map[string]int{
		"workspace_tier": 0,
		"agent_tier":     1,
	}) {
		t.Fatalf("expected agent-tier pressure counts on summary, got %+v", dump.Summary.WorkspaceTierPressureCounts)
	}
	if dump.Summary.AnomalyWarmingDriver != "NONE" {
		t.Fatalf("expected observable telemetry summary to avoid warming-driver label, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyBaselineScopeCounts[rspAnomalyScopeWorkspace] != 1 ||
		dump.Summary.AnomalyBaselineScopeCounts[rspAnomalyScopeAgentDefault] != 1 ||
		dump.Summary.AnomalyBaselineScopeCounts[rspAnomalyScopeExact] != 1 {
		t.Fatalf("expected baseline scope provenance counts, got %+v", dump.Summary)
	}
	if dump.Summary.StateLogCount != 2 || dump.Summary.StateHighThrashingCount != 1 || dump.Summary.StateHighUngroundedCount != 1 {
		t.Fatalf("expected state calibration summary counts, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessBand != "OBSERVABLE" {
		t.Fatalf("expected observable telemetry readiness band, got %+v", dump.Summary)
	}
	if dump.Summary.BeliefReadinessBand != "WARMING" || dump.Summary.AnomalyReadinessBand != "OBSERVABLE" || dump.Summary.StateReadinessBand != "WARMING" {
		t.Fatalf("expected per-stream readiness bands to reflect current telemetry coverage, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil {
		t.Fatalf("expected additive readiness/coverage rollup, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup.OverallReadinessBand != "OBSERVABLE" ||
		dump.Summary.ReadinessCoverageRollup.ObservableStreamCount != 1 ||
		dump.Summary.ReadinessCoverageRollup.WarmingStreamCount != 2 ||
		dump.Summary.ReadinessCoverageRollup.InsufficientStreamCount != 0 {
		t.Fatalf("expected readiness rollup to mirror current per-stream coverage bands, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
	if !containsString(dump.Summary.CoverageGaps, "belief_coverage_thin") ||
		!containsString(dump.Summary.CoverageGaps, "state_coverage_thin") ||
		!containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse_agent_tier") {
		t.Fatalf("expected per-stream coverage gaps in summary, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup.CoverageGapCount != 3 || !dump.Summary.ReadinessCoverageRollup.HasCoverageGaps {
		t.Fatalf("expected readiness rollup to mirror coverage-gap presence, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
	if dump.Summary.ReadinessCoverageRollup.CoverageGapCounts["belief_coverage_thin"] != 1 ||
		dump.Summary.ReadinessCoverageRollup.CoverageGapCounts["state_coverage_thin"] != 1 ||
		dump.Summary.ReadinessCoverageRollup.CoverageGapCounts["anomaly_exact_coverage_sparse_agent_tier"] != 1 {
		t.Fatalf("expected readiness rollup to mirror bounded coverage-gap counts, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_baseline_cold") {
		t.Fatalf("expected warmed anomaly coverage to avoid cold-baseline gap, got %+v", dump.Summary)
	}
	if dump.Summary.LatestBeliefAt == "" || dump.Summary.LatestAnomalyAt == "" || dump.Summary.LatestStateAt == "" {
		t.Fatalf("expected telemetry summary recency markers, got %+v", dump.Summary)
	}
}

func TestDumpRSPTelemetryKeepsPartialWorkspaceFallbackShrinkageObservable(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-rsp-telemetry-partial-ws-shrink"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Telemetry Partial Workspace Shrink",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-partial-ws-shrunk", workspaceID, "DEFAULT", "verifier_fail_rate", "INCIDENT", "S1", "EXACT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:06:00Z", 0.4, 0.09, 0.88, 0.68, 0.7, "THRASHING", "2026-03-30T10:01:30Z",
		"rspan-partial-ws-direct", workspaceID, "DEFAULT", "queue_latency", "INCIDENT", "S1", "WORKSPACE_DEFAULT", 4, "2026-03-29T09:04:00Z", 0.3, 0.07, 0.82, 0.64, 0.8, "STAGNATION", "2026-03-30T10:01:00Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry rows: %v", err)
	}
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "", "UNKNOWN", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, 3, "2026-03-29T09:00:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline row: %v", err)
	}

	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("dump rsp telemetry: %v", err)
	}
	if dump.Summary.ShrinkageFallbackQualityBand != "WORKSPACE_FALLBACK" || dump.Summary.ShrinkageRelianceBand != "PARTIAL" {
		t.Fatalf("expected partial workspace fallback provenance, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyWarmingDriver != "NONE" {
		t.Fatalf("expected partial workspace fallback to avoid warming-driver label once anomaly readiness is observable, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse_workspace_tier") || !containsString(dump.Summary.CoverageGaps, "anomaly_workspace_fallback_partial_workspace_tier") {
		t.Fatalf("expected partial workspace fallback to surface workspace-tier sparse coverage gaps, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyReadinessBand != "OBSERVABLE" || dump.Summary.ReadinessBand != "OBSERVABLE" {
		t.Fatalf("expected partial workspace fallback shrinkage to keep anomaly and overall readiness observable, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil || dump.Summary.ReadinessCoverageRollup.OverallReadinessBand != "OBSERVABLE" {
		t.Fatalf("expected readiness rollup to stay observable on partial workspace fallback, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
}

func TestDumpRSPTelemetrySurfacesAgentDefaultWorkspaceShrinkageGap(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-rsp-telemetry-agent-default-ws-shrink"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Telemetry Agent-Default Workspace Shrink",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-agent-default-ws-shrunk", workspaceID, "DEFAULT", "verifier_fail_rate", "INCIDENT", "S1", "AGENT_DEFAULT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:06:00Z", 0.4, 0.09, 0.88, 0.68, 0.7, "THRASHING", "2026-03-30T10:01:30Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry rows: %v", err)
	}
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "", "INCIDENT", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, 4, "2026-03-29T09:00:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline row: %v", err)
	}

	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("dump rsp telemetry: %v", err)
	}
	if dump.Summary.ShrinkageFallbackQualityBand != "AGENT_DEFAULT_WORKSPACE_FALLBACK" || dump.Summary.ShrinkageRelianceBand != "ALL_SHRUNK" {
		t.Fatalf("expected agent-default workspace fallback provenance, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceFallbackMixBand != "AGENT_DEFAULT_ONLY" {
		t.Fatalf("expected agent-default-only workspace fallback mix on summary, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureBand != "ALL_SHRUNK" {
		t.Fatalf("expected agent-default-only workspace fallback to keep all-shrunk workspace-tier pressure on summary, got %+v", dump.Summary)
	}
	if !reflect.DeepEqual(dump.Summary.WorkspaceFallbackMixCounts, map[string]int{
		"exact_workspace":         0,
		"agent_default_workspace": 1,
	}) {
		t.Fatalf("expected agent-default-only workspace fallback mix counts on summary, got %+v", dump.Summary.WorkspaceFallbackMixCounts)
	}
	if !reflect.DeepEqual(dump.Summary.WorkspaceTierPressureCounts, map[string]int{
		"workspace_tier": 1,
		"agent_tier":     0,
	}) {
		t.Fatalf("expected agent-default-only workspace tier pressure counts on summary, got %+v", dump.Summary.WorkspaceTierPressureCounts)
	}
	if dump.Summary.AnomalyWarmingDriver != "ALL_SHRUNK_AGENT_DEFAULT_WORKSPACE_FALLBACK" {
		t.Fatalf("expected agent-default workspace fallback to surface workspace-fallback warming driver, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_agent_default_coverage_sparse_workspace_tier") || !containsString(dump.Summary.CoverageGaps, "anomaly_agent_default_workspace_fallback_all_shrunk_workspace_tier") {
		t.Fatalf("expected agent-default workspace fallback to surface agent-default sparse and workspace-fallback gaps, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse_workspace_tier") {
		t.Fatalf("expected agent-default workspace fallback to avoid exact-tier sparse gap, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil || dump.Summary.ReadinessCoverageRollup.CoverageGapCounts["anomaly_agent_default_coverage_sparse_workspace_tier"] != 1 {
		t.Fatalf("expected readiness rollup to count agent-default workspace sparse gap, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
}

func TestDumpRSPTelemetrySurfacesMixedWorkspaceFallbackQuality(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-rsp-telemetry-mixed-workspace-shrink"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Telemetry Mixed Workspace Shrink",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
		        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-mixed-workspace-exact", workspaceID, "DEFAULT", "verifier_fail_rate", "INCIDENT", "S1", "EXACT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:06:00Z", 0.4, 0.09, 0.88, 0.68, 0.7, "THRASHING", "2026-03-30T10:01:30Z",
		"rspan-mixed-workspace-agent-default", workspaceID, "DEFAULT", "queue_latency", "INCIDENT", "S1", "AGENT_DEFAULT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:07:00Z", 0.38, 0.07, 0.91, 0.7, 0.7, "STAGNATION", "2026-03-30T10:01:45Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry rows: %v", err)
	}
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "", "INCIDENT", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, 4, "2026-03-29T09:00:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline row: %v", err)
	}

	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("dump rsp telemetry: %v", err)
	}
	if dump.Summary.ShrinkageFallbackQualityBand != "MIXED_WORKSPACE_FALLBACK" || dump.Summary.ShrinkageRelianceBand != "ALL_SHRUNK" {
		t.Fatalf("expected mixed workspace fallback provenance on summary, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceFallbackMixBand != "BALANCED" {
		t.Fatalf("expected balanced workspace fallback mix on summary, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureBand != "ALL_SHRUNK" {
		t.Fatalf("expected mixed workspace fallback to keep all-shrunk workspace-tier pressure on summary, got %+v", dump.Summary)
	}
	if !reflect.DeepEqual(dump.Summary.WorkspaceFallbackMixCounts, map[string]int{
		"exact_workspace":         1,
		"agent_default_workspace": 1,
	}) {
		t.Fatalf("expected balanced workspace fallback mix counts on summary, got %+v", dump.Summary.WorkspaceFallbackMixCounts)
	}
	if !reflect.DeepEqual(dump.Summary.WorkspaceTierPressureCounts, map[string]int{
		"workspace_tier": 2,
		"agent_tier":     0,
	}) {
		t.Fatalf("expected mixed workspace tier pressure counts on summary, got %+v", dump.Summary.WorkspaceTierPressureCounts)
	}
	if dump.Summary.ShrinkageFallbackScopeTier != "WORKSPACE_TIER" {
		t.Fatalf("expected workspace-tier provenance on mixed workspace fallback summary, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyWarmingDriver != "ALL_SHRUNK_MIXED_WORKSPACE_FALLBACK" {
		t.Fatalf("expected mixed workspace fallback warming driver on summary, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_mixed_workspace_coverage_sparse_workspace_tier") || !containsString(dump.Summary.CoverageGaps, "anomaly_mixed_workspace_fallback_all_shrunk_workspace_tier") {
		t.Fatalf("expected mixed workspace fallback to use mixed-workspace gap family, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_workspace_fallback_all_shrunk_workspace_tier") || containsString(dump.Summary.CoverageGaps, "anomaly_agent_default_workspace_fallback_all_shrunk_workspace_tier") || containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse_workspace_tier") {
		t.Fatalf("expected mixed workspace fallback to avoid generic/pure agent-default workspace gap families, got %+v", dump.Summary)
	}
}

func TestRSPTelemetryShrinkageFallbackQualityBand(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		summary  RSPTelemetryCalibrationSummary
		expected string
	}{
		{
			name: "none",
			summary: RSPTelemetryCalibrationSummary{
				ShrunkAnomalyAlertCount:  0,
				ShrunkAnomalyScopeCounts: map[string]int{},
			},
			expected: "NONE",
		},
		{
			name: "agent localized",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
				},
			},
			expected: "AGENT_LOCALIZED",
		},
		{
			name: "workspace fallback",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
				},
			},
			expected: "WORKSPACE_FALLBACK",
		},
		{
			name: "agent-default workspace fallback",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeAgentShrunkWS: 1,
				},
			},
			expected: "AGENT_DEFAULT_WORKSPACE_FALLBACK",
		},
		{
			name: "mixed workspace fallback",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
					rspAnomalyScopeAgentShrunkWS: 1,
				},
			},
			expected: "MIXED_WORKSPACE_FALLBACK",
		},
		{
			name: "mixed",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
					rspAnomalyScopeAgentShrunkWS:    1,
				},
			},
			expected: "MIXED",
		},
		{
			name: "cold baseline path suppresses quality provenance",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     0,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
				},
			},
			expected: "NONE",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := rspTelemetryShrinkageFallbackQualityBand(tc.summary); got != tc.expected {
				t.Fatalf("expected shrinkage fallback quality %s, got %s", tc.expected, got)
			}
		})
	}
}

func TestRSPTelemetryWorkspaceFallbackMixBand(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		summary  RSPTelemetryCalibrationSummary
		expected string
	}{
		{
			name: "none",
			summary: RSPTelemetryCalibrationSummary{
				ShrunkAnomalyAlertCount: 0,
			},
			expected: "NONE",
		},
		{
			name: "exact only",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
				},
			},
			expected: "EXACT_ONLY",
		},
		{
			name: "agent-default only",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeAgentShrunkWS: 1,
				},
			},
			expected: "AGENT_DEFAULT_ONLY",
		},
		{
			name: "exact dominant",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 3,
				ShrunkAnomalyAlertCount:      3,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 2,
					rspAnomalyScopeAgentShrunkWS: 1,
				},
			},
			expected: "EXACT_DOMINANT",
		},
		{
			name: "agent-default dominant",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 3,
				ShrunkAnomalyAlertCount:      3,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
					rspAnomalyScopeAgentShrunkWS: 2,
				},
			},
			expected: "AGENT_DEFAULT_DOMINANT",
		},
		{
			name: "balanced",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
					rspAnomalyScopeAgentShrunkWS: 1,
				},
			},
			expected: "BALANCED",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := rspTelemetryWorkspaceFallbackMixBand(tc.summary); got != tc.expected {
				t.Fatalf("expected workspace fallback mix band %s, got %s", tc.expected, got)
			}
		})
	}
}

func TestRSPTelemetryWorkspaceFallbackMixCounts(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		summary  RSPTelemetryCalibrationSummary
		expected map[string]int
	}{
		{
			name: "none",
			summary: RSPTelemetryCalibrationSummary{
				ShrunkAnomalyAlertCount: 0,
			},
			expected: nil,
		},
		{
			name: "exact only",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 2,
				},
			},
			expected: map[string]int{
				"exact_workspace":         2,
				"agent_default_workspace": 0,
			},
		},
		{
			name: "agent-default only",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeAgentShrunkWS: 3,
				},
			},
			expected: map[string]int{
				"exact_workspace":         0,
				"agent_default_workspace": 3,
			},
		},
		{
			name: "balanced",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
					rspAnomalyScopeAgentShrunkWS: 1,
				},
			},
			expected: map[string]int{
				"exact_workspace":         1,
				"agent_default_workspace": 1,
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := rspTelemetryWorkspaceFallbackMixCounts(tc.summary)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Fatalf("expected workspace fallback mix counts %+v, got %+v", tc.expected, got)
			}
		})
	}
}

func TestRSPTelemetryShrinkageRelianceBand(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		summary  RSPTelemetryCalibrationSummary
		expected string
	}{
		{
			name: "none without warmed baseline-backed shrinkage",
			summary: RSPTelemetryCalibrationSummary{
				ShrunkAnomalyAlertCount: 1,
			},
			expected: "NONE",
		},
		{
			name: "all shrunk",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
			},
			expected: "ALL_SHRUNK",
		},
		{
			name: "strict majority dominant",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 3,
				ShrunkAnomalyAlertCount:      2,
			},
			expected: "DOMINANT",
		},
		{
			name: "balanced split stays partial",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      1,
			},
			expected: "PARTIAL",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := rspTelemetryShrinkageRelianceBand(tc.summary); got != tc.expected {
				t.Fatalf("expected shrinkage reliance band %s, got %s", tc.expected, got)
			}
		})
	}
}

func TestRSPTelemetryWorkspaceTierPressureBand(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		summary  RSPTelemetryCalibrationSummary
		expected string
	}{
		{
			name: "none without warmed baseline backed shrinkage",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 0,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
				},
			},
			expected: "NONE",
		},
		{
			name: "none without workspace tier shrinkage",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
				},
			},
			expected: "NONE",
		},
		{
			name: "all shrunk",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
					rspAnomalyScopeAgentShrunkWS: 1,
				},
			},
			expected: "ALL_SHRUNK",
		},
		{
			name: "dominant workspace tier pressure",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 3,
				ShrunkAnomalyAlertCount:      3,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
					rspAnomalyScopeExactShrunkWS:    2,
				},
			},
			expected: "DOMINANT",
		},
		{
			name: "partial workspace tier pressure on balanced mix",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
					rspAnomalyScopeExactShrunkWS:    1,
				},
			},
			expected: "PARTIAL",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := rspTelemetryWorkspaceTierPressureBand(tc.summary); got != tc.expected {
				t.Fatalf("expected workspace tier pressure band %s, got %s", tc.expected, got)
			}
		})
	}
}

func TestRSPTelemetryWorkspaceTierPressureCounts(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		summary  RSPTelemetryCalibrationSummary
		expected map[string]int
	}{
		{
			name: "none without warmed baseline backed shrinkage",
			summary: RSPTelemetryCalibrationSummary{
				ShrunkAnomalyAlertCount: 1,
			},
			expected: nil,
		},
		{
			name: "workspace tier only",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
					rspAnomalyScopeAgentShrunkWS: 1,
				},
			},
			expected: map[string]int{
				"workspace_tier": 2,
				"agent_tier":     0,
			},
		},
		{
			name: "agent tier only",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
				},
			},
			expected: map[string]int{
				"workspace_tier": 0,
				"agent_tier":     1,
			},
		},
		{
			name: "mixed dominant workspace tier pressure",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 3,
				ShrunkAnomalyAlertCount:      3,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
					rspAnomalyScopeExactShrunkWS:    2,
				},
			},
			expected: map[string]int{
				"workspace_tier": 2,
				"agent_tier":     1,
			},
		},
		{
			name: "balanced mixed pressure",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
					rspAnomalyScopeExactShrunkWS:    1,
				},
			},
			expected: map[string]int{
				"workspace_tier": 1,
				"agent_tier":     1,
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := rspTelemetryWorkspaceTierPressureCounts(tc.summary)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Fatalf("expected workspace tier pressure counts %+v, got %+v", tc.expected, got)
			}
		})
	}
}

func TestRSPTelemetryShrinkageFallbackScopeTier(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		summary  RSPTelemetryCalibrationSummary
		expected string
	}{
		{
			name: "none",
			summary: RSPTelemetryCalibrationSummary{
				ShrunkAnomalyAlertCount:  0,
				ShrunkAnomalyScopeCounts: map[string]int{},
			},
			expected: "NONE",
		},
		{
			name: "agent tier",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
				},
			},
			expected: "AGENT_TIER",
		},
		{
			name: "workspace tier",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
					rspAnomalyScopeAgentShrunkWS: 1,
				},
			},
			expected: "WORKSPACE_TIER",
		},
		{
			name: "mixed tiers",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
					rspAnomalyScopeAgentShrunkWS:    1,
				},
			},
			expected: "MIXED_TIERS",
		},
		{
			name: "cold baseline path suppresses scope tier provenance",
			summary: RSPTelemetryCalibrationSummary{
				WarmAnomalyBaselineCount:     0,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
				},
			},
			expected: "NONE",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := rspTelemetryShrinkageFallbackScopeTier(tc.summary); got != tc.expected {
				t.Fatalf("expected shrinkage fallback scope tier %s, got %s", tc.expected, got)
			}
		})
	}
}

func TestRSPTelemetryWorkspaceFallbackGap(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		summary  RSPTelemetryCalibrationSummary
		expected string
	}{
		{
			name: "none",
			summary: RSPTelemetryCalibrationSummary{
				ShrinkageFallbackQualityBand: "AGENT_LOCALIZED",
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrinkageFallbackScopeTier:   "AGENT_TIER",
			},
			expected: "",
		},
		{
			name: "workspace tier all shrunk",
			summary: RSPTelemetryCalibrationSummary{
				ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
			},
			expected: "anomaly_workspace_fallback_all_shrunk_workspace_tier",
		},
		{
			name: "workspace tier dominant",
			summary: RSPTelemetryCalibrationSummary{
				ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
				ShrinkageRelianceBand:        "DOMINANT",
				ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
			},
			expected: "anomaly_workspace_fallback_dominant_workspace_tier",
		},
		{
			name: "workspace tier partial",
			summary: RSPTelemetryCalibrationSummary{
				ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
				ShrinkageRelianceBand:        "PARTIAL",
				ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
			},
			expected: "anomaly_workspace_fallback_partial_workspace_tier",
		},
		{
			name: "mixed tiers all shrunk balanced suppresses workspace dominance",
			summary: RSPTelemetryCalibrationSummary{
				ShrinkageFallbackQualityBand: "MIXED",
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrinkageFallbackScopeTier:   "MIXED_TIERS",
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
					rspAnomalyScopeExactShrunkWS:    1,
				},
			},
			expected: "",
		},
		{
			name: "mixed tiers all shrunk dominant strict majority",
			summary: RSPTelemetryCalibrationSummary{
				ShrinkageFallbackQualityBand: "MIXED",
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrinkageFallbackScopeTier:   "MIXED_TIERS",
				ShrunkAnomalyAlertCount:      3,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
					rspAnomalyScopeExactShrunkWS:    2,
				},
			},
			expected: "anomaly_mixed_fallback_all_shrunk_mixed_tiers",
		},
		{
			name: "mixed tiers balanced suppresses workspace dominance",
			summary: RSPTelemetryCalibrationSummary{
				ShrinkageFallbackQualityBand: "MIXED",
				ShrinkageRelianceBand:        "DOMINANT",
				ShrinkageFallbackScopeTier:   "MIXED_TIERS",
				ShrunkAnomalyAlertCount:      2,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
					rspAnomalyScopeExactShrunkWS:    1,
				},
			},
			expected: "",
		},
		{
			name: "mixed tiers dominant strict majority",
			summary: RSPTelemetryCalibrationSummary{
				ShrinkageFallbackQualityBand: "MIXED",
				ShrinkageRelianceBand:        "DOMINANT",
				ShrinkageFallbackScopeTier:   "MIXED_TIERS",
				ShrunkAnomalyAlertCount:      3,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 1,
					rspAnomalyScopeExactShrunkWS:    2,
				},
			},
			expected: "anomaly_mixed_fallback_dominant_mixed_tiers",
		},
		{
			name: "mixed tiers partial strict majority",
			summary: RSPTelemetryCalibrationSummary{
				ShrinkageFallbackQualityBand: "MIXED",
				ShrinkageRelianceBand:        "PARTIAL",
				ShrinkageFallbackScopeTier:   "MIXED_TIERS",
				ShrunkAnomalyAlertCount:      5,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 2,
					rspAnomalyScopeExactShrunkWS:    3,
				},
			},
			expected: "anomaly_mixed_fallback_partial_mixed_tiers",
		},
		{
			name: "mixed tiers incidental workspace partial",
			summary: RSPTelemetryCalibrationSummary{
				ShrinkageFallbackQualityBand: "MIXED",
				ShrinkageRelianceBand:        "DOMINANT",
				ShrinkageFallbackScopeTier:   "MIXED_TIERS",
				ShrunkAnomalyAlertCount:      3,
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkAgent: 2,
					rspAnomalyScopeExactShrunkWS:    1,
				},
			},
			expected: "",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := rspTelemetryWorkspaceFallbackGap(tc.summary); got != tc.expected {
				t.Fatalf("expected workspace-fallback gap %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestRSPTelemetryAnomalyColdGap(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		summary  RSPTelemetryCalibrationSummary
		expected string
	}{
		{
			name: "warming without persisted baselines uses missing gap",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyReadinessBand: "WARMING",
			},
			expected: "anomaly_baseline_missing",
		},
		{
			name: "workspace all shrunk suppresses cold gap",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyBaselineCount:         1,
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
			},
			expected: "",
		},
		{
			name: "mixed workspace all shrunk suppresses cold gap",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyBaselineCount:         1,
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrinkageFallbackQualityBand: "MIXED_WORKSPACE_FALLBACK",
			},
			expected: "",
		},
		{
			name: "mixed all shrunk suppresses cold gap",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyBaselineCount:         1,
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrinkageFallbackQualityBand: "MIXED",
			},
			expected: "",
		},
		{
			name: "warm baselines without anomaly observability use observability-missing gap",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyBaselineCount:     1,
				WarmAnomalyBaselineCount: 1,
			},
			expected: "anomaly_baseline_observability_missing",
		},
		{
			name: "partial fallback without persisted baselines uses missing gap",
			summary: RSPTelemetryCalibrationSummary{
				ShrunkAnomalyAlertCount:      1,
				ShrinkageRelianceBand:        "PARTIAL",
				ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
			},
			expected: "anomaly_baseline_missing",
		},
		{
			name: "all shrunk cold warmup still keeps cold gap",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyBaselineCount:         1,
				WarmAnomalyBaselineCount:     0,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
			},
			expected: "anomaly_baseline_cold",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := rspTelemetryAnomalyColdGap(tc.summary); got != tc.expected {
				t.Fatalf("expected anomaly cold gap %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestRSPTelemetryAnomalyWarmingDriver(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		summary  RSPTelemetryCalibrationSummary
		expected string
	}{
		{
			name: "non warming path keeps none",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyReadinessBand: "OBSERVABLE",
			},
			expected: "NONE",
		},
		{
			name: "missing baselines",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyReadinessBand: "WARMING",
			},
			expected: "BASELINE_MISSING",
		},
		{
			name: "cold baseline path",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyReadinessBand:     "WARMING",
				AnomalyBaselineCount:     1,
				WarmAnomalyBaselineCount: 0,
			},
			expected: "BASELINE_COLD",
		},
		{
			name: "observability missing",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyReadinessBand:     "WARMING",
				AnomalyBaselineCount:     1,
				WarmAnomalyBaselineCount: 1,
			},
			expected: "OBSERVABILITY_MISSING",
		},
		{
			name: "all shrunk workspace fallback",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyReadinessBand:         "WARMING",
				AnomalyBaselineCount:         1,
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
			},
			expected: "ALL_SHRUNK_WORKSPACE_FALLBACK",
		},
		{
			name: "all shrunk agent-default workspace fallback",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyReadinessBand:         "WARMING",
				AnomalyBaselineCount:         1,
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 1,
				ShrunkAnomalyAlertCount:      1,
				ShrinkageFallbackQualityBand: "AGENT_DEFAULT_WORKSPACE_FALLBACK",
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeAgentShrunkWS: 1,
				},
			},
			expected: "ALL_SHRUNK_AGENT_DEFAULT_WORKSPACE_FALLBACK",
		},
		{
			name: "all shrunk mixed workspace fallback",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyReadinessBand:         "WARMING",
				AnomalyBaselineCount:         1,
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrinkageFallbackQualityBand: "MIXED_WORKSPACE_FALLBACK",
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrunkAnomalyScopeCounts: map[string]int{
					rspAnomalyScopeExactShrunkWS: 1,
					rspAnomalyScopeAgentShrunkWS: 1,
				},
			},
			expected: "ALL_SHRUNK_MIXED_WORKSPACE_FALLBACK",
		},
		{
			name: "all shrunk mixed fallback",
			summary: RSPTelemetryCalibrationSummary{
				AnomalyReadinessBand:         "WARMING",
				AnomalyBaselineCount:         1,
				WarmAnomalyBaselineCount:     1,
				AnomalyLogsWithBaselineCount: 2,
				ShrunkAnomalyAlertCount:      2,
				ShrinkageRelianceBand:        "ALL_SHRUNK",
				ShrinkageFallbackQualityBand: "MIXED",
			},
			expected: "ALL_SHRUNK_MIXED_TIER_FALLBACK",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := rspTelemetryAnomalyWarmingDriver(tc.summary); got != tc.expected {
				t.Fatalf("expected anomaly warming driver %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestRSPTelemetryAnomalyReadinessBandDowngradesWorkspaceFallbackShrinkage(t *testing.T) {
	t.Parallel()

	workspaceFallback := RSPTelemetryCalibrationSummary{
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 1,
		ShrunkAnomalyAlertCount:      1,
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkWS: 1,
		},
	}
	workspaceFallback.ShrinkageFallbackQualityBand = rspTelemetryShrinkageFallbackQualityBand(workspaceFallback)
	workspaceFallback.ShrinkageRelianceBand = rspTelemetryShrinkageRelianceBand(workspaceFallback)
	if got := rspTelemetryAnomalyReadinessBand(workspaceFallback); got != "WARMING" {
		t.Fatalf("expected workspace-fallback shrinkage to remain warming, got %s", got)
	}

	workspacePartial := RSPTelemetryCalibrationSummary{
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 2,
		ShrunkAnomalyAlertCount:      1,
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkWS: 1,
		},
	}
	workspacePartial.ShrinkageFallbackQualityBand = rspTelemetryShrinkageFallbackQualityBand(workspacePartial)
	workspacePartial.ShrinkageRelianceBand = rspTelemetryShrinkageRelianceBand(workspacePartial)
	if got := rspTelemetryAnomalyReadinessBand(workspacePartial); got != "OBSERVABLE" {
		t.Fatalf("expected partial workspace-fallback shrinkage to stay observable, got %s", got)
	}

	agentFallback := RSPTelemetryCalibrationSummary{
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 1,
		ShrunkAnomalyAlertCount:      1,
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkAgent: 1,
		},
	}
	agentFallback.ShrinkageFallbackQualityBand = rspTelemetryShrinkageFallbackQualityBand(agentFallback)
	if got := rspTelemetryAnomalyReadinessBand(agentFallback); got != "OBSERVABLE" {
		t.Fatalf("expected agent-localized shrinkage to stay observable, got %s", got)
	}

	mixedAllShrunk := RSPTelemetryCalibrationSummary{
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 2,
		ShrunkAnomalyAlertCount:      2,
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkAgent: 1,
			rspAnomalyScopeExactShrunkWS:    1,
		},
	}
	mixedAllShrunk.ShrinkageFallbackQualityBand = rspTelemetryShrinkageFallbackQualityBand(mixedAllShrunk)
	mixedAllShrunk.ShrinkageRelianceBand = rspTelemetryShrinkageRelianceBand(mixedAllShrunk)
	if got := rspTelemetryAnomalyReadinessBand(mixedAllShrunk); got != "WARMING" {
		t.Fatalf("expected all-shrunk mixed fallback to remain warming, got %s", got)
	}

	mixedPartial := RSPTelemetryCalibrationSummary{
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 3,
		ShrunkAnomalyAlertCount:      2,
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkAgent: 1,
			rspAnomalyScopeExactShrunkWS:    1,
		},
	}
	mixedPartial.ShrinkageFallbackQualityBand = rspTelemetryShrinkageFallbackQualityBand(mixedPartial)
	mixedPartial.ShrinkageRelianceBand = rspTelemetryShrinkageRelianceBand(mixedPartial)
	if got := rspTelemetryAnomalyReadinessBand(mixedPartial); got != "OBSERVABLE" {
		t.Fatalf("expected mixed partial shrinkage to stay observable, got %s", got)
	}
}

func TestRSPTelemetryReadinessBandDowngradesWorkspaceFallbackShrinkageOverall(t *testing.T) {
	t.Parallel()

	workspaceFallback := RSPTelemetryCalibrationSummary{
		BeliefLogCount:               2,
		AnomalyAlertCount:            1,
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 1,
		StateLogCount:                2,
		ShrunkAnomalyAlertCount:      1,
		ShrinkageRelianceBand:        "ALL_SHRUNK",
		AnomalyReadinessBand:         "WARMING",
		ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
	}
	if got := rspTelemetryReadinessBand(workspaceFallback); got != "WARMING" {
		t.Fatalf("expected workspace fallback shrinkage to downgrade overall readiness, got %s", got)
	}

	workspacePartial := RSPTelemetryCalibrationSummary{
		BeliefLogCount:               2,
		AnomalyAlertCount:            2,
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 2,
		StateLogCount:                2,
		ShrunkAnomalyAlertCount:      1,
		ShrinkageRelianceBand:        "PARTIAL",
		AnomalyReadinessBand:         "OBSERVABLE",
		ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
	}
	if got := rspTelemetryReadinessBand(workspacePartial); got != "OBSERVABLE" {
		t.Fatalf("expected partial workspace fallback shrinkage to keep overall readiness observable, got %s", got)
	}

	agentFallback := RSPTelemetryCalibrationSummary{
		BeliefLogCount:               2,
		AnomalyAlertCount:            1,
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 1,
		StateLogCount:                2,
		ShrunkAnomalyAlertCount:      1,
		AnomalyReadinessBand:         "OBSERVABLE",
		ShrinkageFallbackQualityBand: "AGENT_LOCALIZED",
	}
	if got := rspTelemetryReadinessBand(agentFallback); got != "OBSERVABLE" {
		t.Fatalf("expected agent-localized shrinkage to keep overall readiness observable, got %s", got)
	}

	mixedAllShrunk := RSPTelemetryCalibrationSummary{
		BeliefLogCount:               2,
		AnomalyAlertCount:            2,
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 2,
		StateLogCount:                2,
		ShrunkAnomalyAlertCount:      2,
		ShrinkageFallbackQualityBand: "MIXED",
		ShrinkageRelianceBand:        "ALL_SHRUNK",
		AnomalyReadinessBand:         "WARMING",
	}
	if got := rspTelemetryReadinessBand(mixedAllShrunk); got != "WARMING" {
		t.Fatalf("expected all-shrunk mixed fallback to downgrade overall readiness, got %s", got)
	}

	mixedPartial := RSPTelemetryCalibrationSummary{
		BeliefLogCount:               2,
		AnomalyAlertCount:            3,
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 3,
		StateLogCount:                2,
		ShrunkAnomalyAlertCount:      2,
		ShrinkageFallbackQualityBand: "MIXED",
		ShrinkageRelianceBand:        "DOMINANT",
		AnomalyReadinessBand:         "OBSERVABLE",
		ShrinkageFallbackScopeTier:   "MIXED_TIERS",
	}
	if got := rspTelemetryReadinessBand(mixedPartial); got != "OBSERVABLE" {
		t.Fatalf("expected mixed partial fallback shrinkage to keep overall readiness observable, got %s", got)
	}
}

func TestRSPTelemetryCoverageGapsSurfacesWorkspaceFallbackShrinkage(t *testing.T) {
	t.Parallel()

	coldMissingSummary := RSPTelemetryCalibrationSummary{
		BeliefReadinessBand:          "OBSERVABLE",
		AnomalyReadinessBand:         "WARMING",
		StateReadinessBand:           "OBSERVABLE",
		AnomalyBaselineCount:         0,
		AnomalyAlertCount:            1,
		AnomalyLogsWithBaselineCount: 1,
	}

	coldMissingGaps := rspTelemetryCoverageGaps(coldMissingSummary)
	if !containsString(coldMissingGaps, "anomaly_baseline_missing") {
		t.Fatalf("expected warming path without persisted baselines to surface missing-baseline gap, got %+v", coldMissingGaps)
	}
	if containsString(coldMissingGaps, "anomaly_baseline_cold") {
		t.Fatalf("expected warming path without persisted baselines to avoid cold-baseline gap, got %+v", coldMissingGaps)
	}
	if containsString(coldMissingGaps, "anomaly_exact_coverage_sparse") || containsString(coldMissingGaps, "anomaly_workspace_fallback") {
		t.Fatalf("expected warming path without persisted baselines to avoid shrinkage-derived gaps, got %+v", coldMissingGaps)
	}

	missingWithShrunkSummary := RSPTelemetryCalibrationSummary{
		BeliefReadinessBand:          "OBSERVABLE",
		AnomalyReadinessBand:         "WARMING",
		StateReadinessBand:           "OBSERVABLE",
		AnomalyBaselineCount:         0,
		AnomalyAlertCount:            1,
		AnomalyLogsWithBaselineCount: 1,
		ShrunkAnomalyAlertCount:      1,
		ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
		ShrinkageRelianceBand:        "ALL_SHRUNK",
		ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkWS: 1,
		},
	}

	missingWithShrunkGaps := rspTelemetryCoverageGaps(missingWithShrunkSummary)
	if !containsString(missingWithShrunkGaps, "anomaly_baseline_missing") {
		t.Fatalf("expected missing-baseline path with shrunk provenance to surface missing-baseline gap, got %+v", missingWithShrunkGaps)
	}
	if containsString(missingWithShrunkGaps, "anomaly_exact_coverage_sparse") || containsString(missingWithShrunkGaps, "anomaly_workspace_fallback") {
		t.Fatalf("expected missing-baseline path with shrunk provenance to avoid shrinkage-derived gaps, got %+v", missingWithShrunkGaps)
	}

	coldBaselineSummary := RSPTelemetryCalibrationSummary{
		BeliefReadinessBand:          "OBSERVABLE",
		AnomalyReadinessBand:         "WARMING",
		StateReadinessBand:           "OBSERVABLE",
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     0,
		AnomalyAlertCount:            1,
		AnomalyLogsWithBaselineCount: 1,
		ShrunkAnomalyAlertCount:      1,
		ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
		ShrinkageRelianceBand:        "ALL_SHRUNK",
		ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkWS: 1,
		},
	}

	coldBaselineSummary.ShrinkageRelianceBand = rspTelemetryShrinkageRelianceBand(coldBaselineSummary)
	coldBaselineSummary.ShrinkageFallbackQualityBand = rspTelemetryShrinkageFallbackQualityBand(coldBaselineSummary)
	coldBaselineSummary.ShrinkageFallbackScopeTier = rspTelemetryShrinkageFallbackScopeTier(coldBaselineSummary)
	if coldBaselineSummary.ShrinkageRelianceBand != "NONE" || coldBaselineSummary.ShrinkageFallbackQualityBand != "NONE" || coldBaselineSummary.ShrinkageFallbackScopeTier != "NONE" {
		t.Fatalf("expected cold-baseline path to suppress shrinkage provenance bands, got %+v", coldBaselineSummary)
	}

	coldBaselineGaps := rspTelemetryCoverageGaps(coldBaselineSummary)
	if !containsString(coldBaselineGaps, "anomaly_baseline_cold") {
		t.Fatalf("expected cold-baseline path to keep cold-baseline gap, got %+v", coldBaselineGaps)
	}
	if containsString(coldBaselineGaps, "anomaly_exact_coverage_sparse") || containsString(coldBaselineGaps, "anomaly_workspace_fallback") {
		t.Fatalf("expected cold-baseline path to avoid shrinkage-derived gaps without warmed baseline-backed observability, got %+v", coldBaselineGaps)
	}

	observabilityMissingSummary := RSPTelemetryCalibrationSummary{
		BeliefReadinessBand:      "OBSERVABLE",
		StateReadinessBand:       "OBSERVABLE",
		AnomalyBaselineCount:     1,
		WarmAnomalyBaselineCount: 1,
		AnomalyAlertCount:        0,
	}
	observabilityMissingSummary.AnomalyReadinessBand = rspTelemetryAnomalyReadinessBand(observabilityMissingSummary)
	observabilityMissingSummary.ReadinessBand = rspTelemetryReadinessBand(observabilityMissingSummary)

	observabilityMissingGaps := rspTelemetryCoverageGaps(observabilityMissingSummary)
	observabilityMissingSummary.CoverageGaps = observabilityMissingGaps
	observabilityMissingSummary.ReadinessCoverageRollup = rspTelemetryBuildReadinessCoverageRollup(observabilityMissingSummary)
	if observabilityMissingSummary.AnomalyReadinessBand != "WARMING" || observabilityMissingSummary.ReadinessBand != "WARMING" {
		t.Fatalf("expected warm-baseline path without anomaly observability to keep anomaly and overall readiness warming, got %+v", observabilityMissingSummary)
	}
	if !containsString(observabilityMissingGaps, "anomaly_baseline_observability_missing") {
		t.Fatalf("expected warm-baseline path without anomaly observability to surface observability-missing gap, got %+v", observabilityMissingGaps)
	}
	if containsString(observabilityMissingGaps, "anomaly_baseline_cold") || containsString(observabilityMissingGaps, "anomaly_baseline_missing") {
		t.Fatalf("expected warm-baseline path without anomaly observability to avoid cold/missing baseline gaps, got %+v", observabilityMissingGaps)
	}
	if containsString(observabilityMissingGaps, "anomaly_exact_coverage_sparse") || containsString(observabilityMissingGaps, "anomaly_workspace_fallback") {
		t.Fatalf("expected warm-baseline path without anomaly observability to avoid shrinkage-derived gaps, got %+v", observabilityMissingGaps)
	}
	if observabilityMissingSummary.ReadinessCoverageRollup == nil || observabilityMissingSummary.ReadinessCoverageRollup.OverallReadinessBand != "WARMING" {
		t.Fatalf("expected warm-baseline observability-missing path to keep readiness rollup warming, got %+v", observabilityMissingSummary.ReadinessCoverageRollup)
	}
	if observabilityMissingSummary.ReadinessCoverageRollup.CoverageGapCounts["anomaly_baseline_observability_missing"] != 1 {
		t.Fatalf("expected warm-baseline observability-missing path to count observability gap in rollup, got %+v", observabilityMissingSummary.ReadinessCoverageRollup)
	}
	if observabilityMissingSummary.ReadinessCoverageRollup.CoverageGapCounts["anomaly_baseline_cold"] != 0 {
		t.Fatalf("expected warm-baseline observability-missing path to avoid cold-baseline rollup counts, got %+v", observabilityMissingSummary.ReadinessCoverageRollup)
	}

	summary := RSPTelemetryCalibrationSummary{
		BeliefReadinessBand:          "OBSERVABLE",
		AnomalyReadinessBand:         "OBSERVABLE",
		StateReadinessBand:           "OBSERVABLE",
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 1,
		ShrunkAnomalyAlertCount:      1,
		ShrinkageFallbackQualityBand: "WORKSPACE_FALLBACK",
		ShrinkageRelianceBand:        "ALL_SHRUNK",
		ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkWS: 1,
		},
	}

	gaps := rspTelemetryCoverageGaps(summary)
	if !containsString(gaps, "anomaly_exact_coverage_sparse_workspace_tier") || !containsString(gaps, "anomaly_workspace_fallback_all_shrunk_workspace_tier") {
		t.Fatalf("expected workspace fallback shrinkage coverage gaps, got %+v", gaps)
	}
	if containsString(gaps, "anomaly_baseline_cold") {
		t.Fatalf("expected all-shrunk workspace fallback shrinkage to avoid cold-baseline gap, got %+v", gaps)
	}

	agentDefaultWorkspaceSummary := RSPTelemetryCalibrationSummary{
		BeliefReadinessBand:          "OBSERVABLE",
		AnomalyReadinessBand:         "WARMING",
		StateReadinessBand:           "OBSERVABLE",
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 1,
		ShrunkAnomalyAlertCount:      1,
		ShrinkageFallbackQualityBand: "AGENT_DEFAULT_WORKSPACE_FALLBACK",
		ShrinkageRelianceBand:        "ALL_SHRUNK",
		ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeAgentShrunkWS: 1,
		},
	}

	agentDefaultWorkspaceGaps := rspTelemetryCoverageGaps(agentDefaultWorkspaceSummary)
	if !containsString(agentDefaultWorkspaceGaps, "anomaly_agent_default_coverage_sparse_workspace_tier") || !containsString(agentDefaultWorkspaceGaps, "anomaly_agent_default_workspace_fallback_all_shrunk_workspace_tier") {
		t.Fatalf("expected agent-default workspace fallback shrinkage coverage gaps, got %+v", agentDefaultWorkspaceGaps)
	}
	if containsString(agentDefaultWorkspaceGaps, "anomaly_exact_coverage_sparse_workspace_tier") {
		t.Fatalf("expected agent-default workspace fallback shrinkage to avoid exact-tier sparse gap, got %+v", agentDefaultWorkspaceGaps)
	}

	agentDefaultWorkspaceDominantSummary := RSPTelemetryCalibrationSummary{
		ShrinkageFallbackQualityBand: "AGENT_DEFAULT_WORKSPACE_FALLBACK",
		ShrinkageRelianceBand:        "DOMINANT",
		ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeAgentShrunkWS: 2,
		},
	}
	if got := rspTelemetryWorkspaceFallbackGap(agentDefaultWorkspaceDominantSummary); got != "anomaly_agent_default_workspace_fallback_dominant_workspace_tier" {
		t.Fatalf("expected pure agent-default workspace fallback to use dominant agent-default workspace gap, got %q", got)
	}

	agentDefaultWorkspacePartialSummary := RSPTelemetryCalibrationSummary{
		ShrinkageFallbackQualityBand: "AGENT_DEFAULT_WORKSPACE_FALLBACK",
		ShrinkageRelianceBand:        "PARTIAL",
		ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeAgentShrunkWS: 1,
		},
	}
	if got := rspTelemetryWorkspaceFallbackGap(agentDefaultWorkspacePartialSummary); got != "anomaly_agent_default_workspace_fallback_partial_workspace_tier" {
		t.Fatalf("expected pure agent-default workspace fallback to use partial agent-default workspace gap, got %q", got)
	}

	mixedWorkspaceAllShrunkSummary := RSPTelemetryCalibrationSummary{
		ShrinkageFallbackQualityBand: "MIXED_WORKSPACE_FALLBACK",
		ShrinkageRelianceBand:        "ALL_SHRUNK",
		ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkWS: 1,
			rspAnomalyScopeAgentShrunkWS: 1,
		},
	}
	if got := rspTelemetrySparseExactCoverageGap(mixedWorkspaceAllShrunkSummary); got != "anomaly_mixed_workspace_coverage_sparse_workspace_tier" {
		t.Fatalf("expected mixed workspace fallback to use mixed-workspace sparse gap, got %q", got)
	}
	if got := rspTelemetryWorkspaceFallbackGap(mixedWorkspaceAllShrunkSummary); got != "anomaly_mixed_workspace_fallback_all_shrunk_workspace_tier" {
		t.Fatalf("expected mixed workspace fallback to use all-shrunk mixed workspace gap, got %q", got)
	}

	mixedWorkspaceDominantSummary := RSPTelemetryCalibrationSummary{
		ShrinkageFallbackQualityBand: "MIXED_WORKSPACE_FALLBACK",
		ShrinkageRelianceBand:        "DOMINANT",
		ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkWS: 2,
			rspAnomalyScopeAgentShrunkWS: 1,
		},
	}
	if got := rspTelemetryWorkspaceFallbackGap(mixedWorkspaceDominantSummary); got != "anomaly_mixed_workspace_fallback_dominant_workspace_tier" {
		t.Fatalf("expected mixed workspace fallback to use dominant mixed workspace gap, got %q", got)
	}

	mixedWorkspacePartialSummary := RSPTelemetryCalibrationSummary{
		ShrinkageFallbackQualityBand: "MIXED_WORKSPACE_FALLBACK",
		ShrinkageRelianceBand:        "PARTIAL",
		ShrinkageFallbackScopeTier:   "WORKSPACE_TIER",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkWS: 1,
			rspAnomalyScopeAgentShrunkWS: 1,
		},
	}
	if got := rspTelemetryWorkspaceFallbackGap(mixedWorkspacePartialSummary); got != "anomaly_mixed_workspace_fallback_partial_workspace_tier" {
		t.Fatalf("expected mixed workspace fallback to use partial mixed workspace gap, got %q", got)
	}

	mixedSummary := RSPTelemetryCalibrationSummary{
		BeliefReadinessBand:          "OBSERVABLE",
		AnomalyReadinessBand:         "OBSERVABLE",
		StateReadinessBand:           "OBSERVABLE",
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 4,
		ShrunkAnomalyAlertCount:      3,
		ShrinkageFallbackQualityBand: "MIXED",
		ShrinkageRelianceBand:        "DOMINANT",
		ShrinkageFallbackScopeTier:   "MIXED_TIERS",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkAgent: 1,
			rspAnomalyScopeExactShrunkWS:    2,
		},
	}

	mixedGaps := rspTelemetryCoverageGaps(mixedSummary)
	if !containsString(mixedGaps, "anomaly_mixed_coverage_sparse_mixed_tiers") || !containsString(mixedGaps, "anomaly_mixed_fallback_dominant_mixed_tiers") {
		t.Fatalf("expected mixed dominant fallback shrinkage coverage gaps, got %+v", mixedGaps)
	}
	if containsString(mixedGaps, "anomaly_exact_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected mixed dominant fallback shrinkage to avoid generic exact mixed-tier sparse gap, got %+v", mixedGaps)
	}

	mixedIncidentalWorkspaceSummary := RSPTelemetryCalibrationSummary{
		BeliefReadinessBand:          "OBSERVABLE",
		AnomalyReadinessBand:         "OBSERVABLE",
		StateReadinessBand:           "OBSERVABLE",
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 4,
		ShrunkAnomalyAlertCount:      3,
		ShrinkageFallbackQualityBand: "MIXED",
		ShrinkageRelianceBand:        "DOMINANT",
		ShrinkageFallbackScopeTier:   "MIXED_TIERS",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkAgent: 2,
			rspAnomalyScopeExactShrunkWS:    1,
		},
	}

	mixedIncidentalWorkspaceGaps := rspTelemetryCoverageGaps(mixedIncidentalWorkspaceSummary)
	if !containsString(mixedIncidentalWorkspaceGaps, "anomaly_mixed_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected mixed incidental workspace shrinkage to keep mixed-tier sparse coverage gap, got %+v", mixedIncidentalWorkspaceGaps)
	}
	if containsString(mixedIncidentalWorkspaceGaps, "anomaly_exact_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected mixed incidental workspace shrinkage to avoid generic exact mixed-tier sparse gap, got %+v", mixedIncidentalWorkspaceGaps)
	}
	if containsString(mixedIncidentalWorkspaceGaps, "anomaly_mixed_fallback_partial_mixed_tiers") {
		t.Fatalf("expected mixed incidental workspace shrinkage to suppress mixed-tier workspace-fallback gap, got %+v", mixedIncidentalWorkspaceGaps)
	}

	mixedBalancedWorkspaceSummary := RSPTelemetryCalibrationSummary{
		BeliefReadinessBand:          "OBSERVABLE",
		AnomalyReadinessBand:         "OBSERVABLE",
		StateReadinessBand:           "OBSERVABLE",
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 3,
		ShrunkAnomalyAlertCount:      2,
		ShrinkageFallbackQualityBand: "MIXED",
		ShrinkageRelianceBand:        "DOMINANT",
		ShrinkageFallbackScopeTier:   "MIXED_TIERS",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkAgent: 1,
			rspAnomalyScopeExactShrunkWS:    1,
		},
	}

	mixedBalancedWorkspaceGaps := rspTelemetryCoverageGaps(mixedBalancedWorkspaceSummary)
	if !containsString(mixedBalancedWorkspaceGaps, "anomaly_mixed_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected balanced mixed shrinkage to keep mixed-tier sparse coverage gap, got %+v", mixedBalancedWorkspaceGaps)
	}
	if containsString(mixedBalancedWorkspaceGaps, "anomaly_exact_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected balanced mixed shrinkage to avoid generic exact mixed-tier sparse gap, got %+v", mixedBalancedWorkspaceGaps)
	}
	if containsString(mixedBalancedWorkspaceGaps, "anomaly_mixed_fallback_dominant_mixed_tiers") || containsString(mixedBalancedWorkspaceGaps, "anomaly_mixed_fallback_partial_mixed_tiers") {
		t.Fatalf("expected balanced mixed shrinkage to suppress mixed-tier workspace-fallback gap, got %+v", mixedBalancedWorkspaceGaps)
	}

	mixedAllShrunkSummary := RSPTelemetryCalibrationSummary{
		BeliefReadinessBand:          "OBSERVABLE",
		AnomalyReadinessBand:         "WARMING",
		StateReadinessBand:           "OBSERVABLE",
		AnomalyBaselineCount:         1,
		WarmAnomalyBaselineCount:     1,
		AnomalyLogsWithBaselineCount: 2,
		ShrunkAnomalyAlertCount:      2,
		ShrinkageFallbackQualityBand: "MIXED",
		ShrinkageRelianceBand:        "ALL_SHRUNK",
		ShrinkageFallbackScopeTier:   "MIXED_TIERS",
		ShrunkAnomalyScopeCounts: map[string]int{
			rspAnomalyScopeExactShrunkAgent: 1,
			rspAnomalyScopeExactShrunkWS:    1,
		},
	}

	mixedAllShrunkGaps := rspTelemetryCoverageGaps(mixedAllShrunkSummary)
	if !containsString(mixedAllShrunkGaps, "anomaly_mixed_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected mixed all-shrunk fallback shrinkage to keep mixed-tier sparse coverage gap, got %+v", mixedAllShrunkGaps)
	}
	if containsString(mixedAllShrunkGaps, "anomaly_exact_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected mixed all-shrunk fallback shrinkage to avoid generic exact mixed-tier sparse gap, got %+v", mixedAllShrunkGaps)
	}
	if containsString(mixedAllShrunkGaps, "anomaly_mixed_fallback_all_shrunk_mixed_tiers") {
		t.Fatalf("expected balanced mixed all-shrunk fallback to suppress mixed-fallback gap without workspace-tier majority, got %+v", mixedAllShrunkGaps)
	}
	if containsString(mixedAllShrunkGaps, "anomaly_baseline_cold") {
		t.Fatalf("expected mixed all-shrunk fallback shrinkage to avoid cold-baseline gap, got %+v", mixedAllShrunkGaps)
	}
}

func requireRSPAnomalyBaselineRow(t *testing.T, ctx context.Context, store *Store, scope rspAnomalyScopeKey) rspAnomalyBaselineRecord {
	t.Helper()

	record, ok, err := store.getRSPAnomalyBaseline(ctx, scope)
	if err != nil {
		t.Fatalf("get anomaly baseline %+v: %v", scope, err)
	}
	if !ok {
		t.Fatalf("expected anomaly baseline for %+v", scope)
	}
	return record
}
