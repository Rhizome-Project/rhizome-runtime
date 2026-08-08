package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceRSPTelemetryDumpSurfacesPersistedAnomalyBaselines(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-rsp-telemetry"
		agentID     = "agent-a"
		taskID      = "task-handler-rsp-telemetry"
		healthyAt   = "2026-03-29T09:00:00Z"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")
	if _, err := store.PutTaskClassEvidence(ctx, sqlite.TaskClassEvidencePutInput{
		WorkspaceID:     workspaceID,
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "tests",
	}); err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := store.DB().ExecContext(ctx,
			`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(workspace_id, agent_id, task_class, mode, phase, metric_name) DO UPDATE SET
			    mu_hat = excluded.mu_hat,
			    sigma_hat = excluded.sigma_hat,
			    sample_size = excluded.sample_size,
			    last_healthy_window_at = excluded.last_healthy_window_at`,
			workspaceID, agentID, "UNKNOWN", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, i+1, healthyAt,
		); err != nil {
			t.Fatalf("seed anomaly baseline %d: %v", i, err)
		}
	}

	store.ProcessRSPAnomalyTelemetry(ctx, sqlite.RuntimeEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "verifier.fail",
		EntityType:  "FACT",
		EntityID:    "claim-handler-rsp-telemetry",
		AgentID:     agentID,
		TaskID:      taskID,
		CreatedAt:   "2026-03-30T10:00:00Z",
	})

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.SchemaVersion != "1.0" {
		t.Fatalf("expected handler telemetry dump schema version, got %+v", dump)
	}
	if dump.CalibrationContracts.Belief.Status != "PROVISIONAL" || dump.CalibrationContracts.Anomaly.Status != "SHADOW_ONLY" {
		t.Fatalf("expected handler telemetry dump to expose explicit calibration contracts, got %+v", dump.CalibrationContracts)
	}
	if dump.CalibrationContracts.Belief.HistoricalRowCoverage != "UNVERSIONED_ROWS" ||
		dump.CalibrationContracts.State.HistoricalRowCoverage != "UNVERSIONED_ROWS" {
		t.Fatalf("expected handler telemetry dump to keep belief/state row coverage limits explicit, got %+v", dump.CalibrationContracts)
	}
	if dump.Summary.CalibrationIntegrityBand != "PARTIAL" && dump.Summary.CalibrationIntegrityBand != "MIXED_LEGACY" {
		t.Fatalf("expected handler telemetry dump to expose calibration integrity posture, got %+v", dump.Summary)
	}
	if len(dump.AnomalyBaselines) == 0 {
		t.Fatalf("expected anomaly telemetry dump, got %+v", dump)
	}
	var exact *sqlite.RSPAnomalyBaselineLog
	var agentDefault *sqlite.RSPAnomalyBaselineLog
	for i := range dump.AnomalyBaselines {
		item := &dump.AnomalyBaselines[i]
		if item.TaskClass == model.TaskClassIncident {
			exact = item
		}
		if item.AgentID == agentID && item.TaskClass == "UNKNOWN" {
			agentDefault = item
		}
	}
	if exact == nil || exact.Mode != "DEFAULT" || exact.Phase != "S1" {
		t.Fatalf("expected exact anomaly baseline row through handler dump, got %+v", dump.AnomalyBaselines)
	}
	if agentDefault == nil || agentDefault.SampleSize < 3 || agentDefault.LastHealthyWindowAt == "" {
		t.Fatalf("expected agent-default anomaly baseline row through handler dump, got %+v", dump.AnomalyBaselines)
	}
}

func TestWorkspaceRSPTelemetryDumpMirrorsWorkspaceFallbackAnomalyProvenance(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-rsp-telemetry-fallback"
		agentID     = "agent-a"
		taskID      = "task-handler-rsp-telemetry-fallback"
		healthyAt   = "2026-03-29T09:00:00Z"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")
	if _, err := store.PutTaskClassEvidence(ctx, sqlite.TaskClassEvidencePutInput{
		WorkspaceID:     workspaceID,
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "tests",
	}); err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := store.DB().ExecContext(ctx,
			`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(workspace_id, agent_id, task_class, mode, phase, metric_name) DO UPDATE SET
			    mu_hat = excluded.mu_hat,
			    sigma_hat = excluded.sigma_hat,
			    sample_size = excluded.sample_size,
			    last_healthy_window_at = excluded.last_healthy_window_at`,
			workspaceID, "", "UNKNOWN", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, i+1, healthyAt,
		); err != nil {
			t.Fatalf("seed workspace anomaly fallback baseline %d: %v", i, err)
		}
	}

	store.ProcessRSPAnomalyTelemetry(ctx, sqlite.RuntimeEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "verifier.fail",
		EntityType:  "FACT",
		EntityID:    "claim-handler-rsp-telemetry-fallback",
		AgentID:     agentID,
		TaskID:      taskID,
		CreatedAt:   "2026-03-30T10:00:00Z",
	})

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if len(dump.AnomalyLogs) == 0 {
		t.Fatalf("expected anomaly telemetry dump to expose warmed workspace fallback alert provenance, got %+v", dump)
	}
	latest := dump.AnomalyLogs[0]
	if latest.BaselineScope != "WORKSPACE_DEFAULT" || latest.BaselineSampleSize < 3 {
		t.Fatalf("expected handler dump to mirror workspace fallback provenance, got %+v", latest)
	}
	if latest.BaselineLastHealthyWindow != healthyAt {
		t.Fatalf("expected handler dump to mirror fallback healthy window, got %+v", latest)
	}
	if latest.MuHat != 0.1 || latest.SigmaHat != 0.1 {
		t.Fatalf("expected handler dump to mirror fallback baseline moments, got %+v", latest)
	}
}

func TestWorkspaceRSPTelemetryDumpMirrorsShrunkExactBaselineProvenance(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-rsp-telemetry-shrink"
		agentID     = "agent-a"
		taskID      = "task-handler-rsp-telemetry-shrink"
		exactAt     = "2026-03-29T09:00:00Z"
		fallbackAt  = "2026-03-29T09:05:00Z"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")
	if _, err := store.PutTaskClassEvidence(ctx, sqlite.TaskClassEvidencePutInput{
		WorkspaceID:     workspaceID,
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "tests",
	}); err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, agentID, model.TaskClassIncident, "DEFAULT", "S1", "verifier_fail_rate", 0.6, 0.05, 1, exactAt,
		workspaceID, agentID, "UNKNOWN", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, 3, fallbackAt,
	); err != nil {
		t.Fatalf("seed anomaly baseline rows: %v", err)
	}

	store.ProcessRSPAnomalyTelemetry(ctx, sqlite.RuntimeEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "verifier.fail",
		EntityType:  "FACT",
		EntityID:    "claim-handler-rsp-telemetry-shrink",
		AgentID:     agentID,
		TaskID:      taskID,
		CreatedAt:   "2026-03-30T10:00:00Z",
	})

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if len(dump.AnomalyLogs) == 0 {
		t.Fatalf("expected handler dump to expose shrunk exact baseline provenance, got %+v", dump)
	}
	latest := dump.AnomalyLogs[0]
	if latest.BaselineScope != "EXACT_SHRUNK_AGENT_DEFAULT" || latest.BaselineSampleSize != 4 {
		t.Fatalf("expected handler dump to mirror shrunk exact baseline provenance, got %+v", latest)
	}
	if latest.BaselineLastHealthyWindow != fallbackAt {
		t.Fatalf("expected handler dump to mirror shrunk fallback healthy window, got %+v", latest)
	}
	if latest.MuHat <= 0.1 || latest.MuHat >= 0.6 {
		t.Fatalf("expected handler dump to mirror bounded shrinkage moments, got %+v", latest)
	}
}

func TestWorkspaceRSPTelemetryDumpMirrorsShrunkExactWorkspaceFallbackProvenance(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-rsp-telemetry-shrink-ws"
		agentID     = "agent-a"
		taskID      = "task-handler-rsp-telemetry-shrink-ws"
		exactAt     = "2026-03-29T09:00:00Z"
		fallbackAt  = "2026-03-29T09:06:00Z"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")
	if _, err := store.PutTaskClassEvidence(ctx, sqlite.TaskClassEvidencePutInput{
		WorkspaceID:     workspaceID,
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "tests",
	}); err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, agentID, model.TaskClassIncident, "DEFAULT", "S1", "verifier_fail_rate", 0.6, 0.05, 1, exactAt,
		workspaceID, "", "UNKNOWN", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, 3, fallbackAt,
	); err != nil {
		t.Fatalf("seed anomaly baseline rows: %v", err)
	}

	store.ProcessRSPAnomalyTelemetry(ctx, sqlite.RuntimeEventRecord{
		WorkspaceID: workspaceID,
		EventType:   "verifier.fail",
		EntityType:  "FACT",
		EntityID:    "claim-handler-rsp-telemetry-shrink-ws",
		AgentID:     agentID,
		TaskID:      taskID,
		CreatedAt:   "2026-03-30T10:00:00Z",
	})

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if len(dump.AnomalyLogs) == 0 {
		t.Fatalf("expected handler dump to expose shrunk exact workspace fallback provenance, got %+v", dump)
	}
	latest := dump.AnomalyLogs[0]
	if latest.BaselineScope != "EXACT_SHRUNK_WORKSPACE_DEFAULT" || latest.BaselineSampleSize != 4 {
		t.Fatalf("expected handler dump to mirror shrunk exact workspace fallback provenance, got %+v", latest)
	}
	if latest.BaselineLastHealthyWindow != fallbackAt {
		t.Fatalf("expected handler dump to mirror shrunk workspace fallback healthy window, got %+v", latest)
	}
	if latest.MuHat <= 0.1 || latest.MuHat >= 0.6 {
		t.Fatalf("expected handler dump to mirror bounded workspace shrinkage moments, got %+v", latest)
	}
	if dump.Summary.ShrinkageFallbackQualityBand != "WORKSPACE_FALLBACK" || dump.Summary.AnomalyReadinessBand != "WARMING" || dump.Summary.ReadinessBand != "WARMING" {
		t.Fatalf("expected handler dump to downgrade anomaly and overall readiness on workspace fallback shrinkage, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageFallbackScopeTier != "WORKSPACE_TIER" {
		t.Fatalf("expected handler dump to surface workspace-tier shrinkage provenance, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceFallbackMixBand != "EXACT_ONLY" {
		t.Fatalf("expected handler dump to surface exact-only workspace fallback mix, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureBand != "ALL_SHRUNK" {
		t.Fatalf("expected handler dump to keep all-shrunk workspace-tier pressure on exact workspace fallback, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureCounts["workspace_tier"] != 1 || dump.Summary.WorkspaceTierPressureCounts["agent_tier"] != 0 {
		t.Fatalf("expected handler dump to surface exact workspace tier pressure counts, got %+v", dump.Summary.WorkspaceTierPressureCounts)
	}
	if dump.Summary.WorkspaceFallbackMixCounts["exact_workspace"] != 1 || dump.Summary.WorkspaceFallbackMixCounts["agent_default_workspace"] != 0 {
		t.Fatalf("expected handler dump to surface exact-only workspace fallback mix counts, got %+v", dump.Summary.WorkspaceFallbackMixCounts)
	}
	if dump.Summary.AnomalyWarmingDriver != "ALL_SHRUNK_WORKSPACE_FALLBACK" {
		t.Fatalf("expected handler dump to surface workspace-fallback warming driver, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil || dump.Summary.ReadinessCoverageRollup.OverallReadinessBand != "WARMING" {
		t.Fatalf("expected handler dump to mirror workspace fallback downgrade in readiness rollup, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_baseline_cold") {
		t.Fatalf("expected handler dump to avoid cold-baseline gap on all-shrunk workspace fallback, got %+v", dump.Summary)
	}
}

func TestWorkspaceRSPTelemetryDumpDowngradesAllShrunkMixedFallbackReadiness(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-rsp-telemetry-mixed-shrink"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a", "agent-b"})
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_belief_telemetry(id, workspace_id, event_id, entity_type, entity_id, prior_b_m, posterior_b_m, uncertainty_u_m, evidence_mass, drift_score, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rsptl-mixed-1", workspaceID, "evt-mixed-1", "FACT", "claim-mixed-1", 0.0, 0.4, 0.7, 1.0, 0.6, "2026-03-30T10:00:00Z",
		"rsptl-mixed-2", workspaceID, "evt-mixed-2", "FACT", "claim-mixed-2", 0.0, 0.2, 0.2, 1.0, 0.1, "2026-03-30T09:59:00Z",
	); err != nil {
		t.Fatalf("seed belief telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-mixed-agent", workspaceID, "DEFAULT", "queue_latency", "INCIDENT", "S1", "EXACT_SHRUNK_AGENT_DEFAULT", 4, "2026-03-29T09:05:00Z", 0.45, 0.08, 0.9, 0.7, 0.7, "STAGNATION", "2026-03-30T10:01:30Z",
		"rspan-mixed-ws", workspaceID, "DEFAULT", "verifier_fail_rate", "INCIDENT", "S1", "EXACT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:06:00Z", 0.4, 0.09, 0.88, 0.68, 0.7, "THRASHING", "2026-03-30T10:01:00Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_agent_state_telemetry(id, workspace_id, agent_id, cache_pressure_i, stale_hit_i, thrashing_risk, ungrounded_risk, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)`,
		"stlog-mixed-1", workspaceID, "agent-a", 0.7, 0.3, 0.8, 0.2, "2026-03-30T10:02:00Z",
		"stlog-mixed-2", workspaceID, "agent-b", 0.2, 0.1, 0.2, 0.7, "2026-03-30T10:01:30Z",
	); err != nil {
		t.Fatalf("seed state telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "", "UNKNOWN", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, 3, "2026-03-29T09:00:00Z",
		workspaceID, "agent-a", "INCIDENT", "DEFAULT", "S1", "queue_latency", 0.8, 0.1, 4, "2026-03-29T09:05:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline rows: %v", err)
	}

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.Summary.ShrinkageFallbackQualityBand != "MIXED" || dump.Summary.ShrinkageRelianceBand != "ALL_SHRUNK" {
		t.Fatalf("expected mixed all-shrunk fallback provenance, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageFallbackScopeTier != "MIXED_TIERS" {
		t.Fatalf("expected mixed-tier shrinkage provenance, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureBand != "PARTIAL" {
		t.Fatalf("expected balanced mixed all-shrunk fallback to keep partial workspace-tier pressure, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureCounts["workspace_tier"] != 1 || dump.Summary.WorkspaceTierPressureCounts["agent_tier"] != 1 {
		t.Fatalf("expected handler dump to surface balanced workspace-vs-agent pressure counts, got %+v", dump.Summary.WorkspaceTierPressureCounts)
	}
	if dump.Summary.AnomalyWarmingDriver != "ALL_SHRUNK_MIXED_TIER_FALLBACK" {
		t.Fatalf("expected handler dump to surface mixed all-shrunk warming driver, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_mixed_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected mixed all-shrunk fallback to keep mixed-tier sparse coverage gap, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected mixed all-shrunk fallback to avoid generic exact mixed-tier sparse gap, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_mixed_fallback_all_shrunk_mixed_tiers") {
		t.Fatalf("expected balanced mixed all-shrunk fallback to suppress mixed-fallback gap without workspace-tier majority, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyReadinessBand != "WARMING" || dump.Summary.ReadinessBand != "WARMING" {
		t.Fatalf("expected mixed all-shrunk fallback to keep anomaly and aggregate readiness warming, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil || dump.Summary.ReadinessCoverageRollup.OverallReadinessBand != "WARMING" {
		t.Fatalf("expected readiness rollup to mirror mixed all-shrunk downgrade, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_baseline_cold") {
		t.Fatalf("expected mixed all-shrunk handler dump to avoid cold-baseline gap, got %+v", dump.Summary)
	}
}

func TestWorkspaceRSPTelemetryDumpKeepsPartialWorkspaceFallbackObservable(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-rsp-telemetry-partial-ws-shrink"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-handler-partial-ws-shrunk", workspaceID, "DEFAULT", "verifier_fail_rate", "INCIDENT", "S1", "EXACT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:06:00Z", 0.4, 0.09, 0.88, 0.68, 0.7, "THRASHING", "2026-03-30T10:01:30Z",
		"rspan-handler-partial-ws-direct", workspaceID, "DEFAULT", "queue_latency", "INCIDENT", "S1", "WORKSPACE_DEFAULT", 4, "2026-03-29T09:04:00Z", 0.3, 0.07, 0.82, 0.64, 0.8, "STAGNATION", "2026-03-30T10:01:00Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "", "UNKNOWN", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, 3, "2026-03-29T09:00:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline row: %v", err)
	}

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.Summary.ShrinkageFallbackQualityBand != "WORKSPACE_FALLBACK" || dump.Summary.ShrinkageRelianceBand != "PARTIAL" {
		t.Fatalf("expected handler dump to surface partial workspace fallback provenance, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyWarmingDriver != "NONE" {
		t.Fatalf("expected handler dump to avoid warming-driver label on observable partial workspace fallback, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse_workspace_tier") || !containsString(dump.Summary.CoverageGaps, "anomaly_workspace_fallback_partial_workspace_tier") {
		t.Fatalf("expected handler dump to surface workspace-tier sparse coverage gaps on partial workspace fallback, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyReadinessBand != "OBSERVABLE" || dump.Summary.ReadinessBand != "OBSERVABLE" {
		t.Fatalf("expected handler dump to keep partial workspace fallback shrinkage observable, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil || dump.Summary.ReadinessCoverageRollup.OverallReadinessBand != "OBSERVABLE" {
		t.Fatalf("expected handler dump readiness rollup to stay observable on partial workspace fallback, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
}

func TestWorkspaceRSPTelemetryDumpSurfacesAgentDefaultWorkspaceShrinkageGap(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-rsp-telemetry-agent-default-ws-shrink"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-handler-agent-default-ws-shrunk", workspaceID, "DEFAULT", "verifier_fail_rate", "INCIDENT", "S1", "AGENT_DEFAULT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:06:00Z", 0.4, 0.09, 0.88, 0.68, 0.7, "THRASHING", "2026-03-30T10:01:30Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "", "INCIDENT", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, 4, "2026-03-29T09:00:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline row: %v", err)
	}

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.Summary.ShrinkageFallbackQualityBand != "AGENT_DEFAULT_WORKSPACE_FALLBACK" || dump.Summary.ShrinkageRelianceBand != "ALL_SHRUNK" {
		t.Fatalf("expected handler dump to surface agent-default workspace fallback provenance, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceFallbackMixBand != "AGENT_DEFAULT_ONLY" {
		t.Fatalf("expected handler dump to surface agent-default-only workspace fallback mix, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureBand != "ALL_SHRUNK" {
		t.Fatalf("expected handler dump to keep all-shrunk workspace-tier pressure on agent-default workspace shrinkage, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureCounts["workspace_tier"] != 1 || dump.Summary.WorkspaceTierPressureCounts["agent_tier"] != 0 {
		t.Fatalf("expected handler dump to surface agent-default workspace tier pressure counts, got %+v", dump.Summary.WorkspaceTierPressureCounts)
	}
	if dump.Summary.WorkspaceFallbackMixCounts["exact_workspace"] != 0 || dump.Summary.WorkspaceFallbackMixCounts["agent_default_workspace"] != 1 {
		t.Fatalf("expected handler dump to surface agent-default-only workspace fallback mix counts, got %+v", dump.Summary.WorkspaceFallbackMixCounts)
	}
	if dump.Summary.AnomalyWarmingDriver != "ALL_SHRUNK_AGENT_DEFAULT_WORKSPACE_FALLBACK" {
		t.Fatalf("expected handler dump to surface workspace-fallback warming driver on agent-default workspace shrinkage, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_agent_default_coverage_sparse_workspace_tier") || !containsString(dump.Summary.CoverageGaps, "anomaly_agent_default_workspace_fallback_all_shrunk_workspace_tier") {
		t.Fatalf("expected handler dump to surface agent-default workspace sparse and workspace-fallback gaps, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse_workspace_tier") {
		t.Fatalf("expected handler dump to avoid exact-tier sparse gap on agent-default workspace shrinkage, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil || dump.Summary.ReadinessCoverageRollup.CoverageGapCounts["anomaly_agent_default_coverage_sparse_workspace_tier"] != 1 {
		t.Fatalf("expected handler dump readiness rollup to count agent-default workspace sparse gap, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
}

func TestWorkspaceRSPTelemetryDumpSurfacesMixedWorkspaceFallbackQuality(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-rsp-telemetry-mixed-workspace-shrink"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
		        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-handler-mixed-workspace-exact", workspaceID, "DEFAULT", "verifier_fail_rate", "INCIDENT", "S1", "EXACT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:06:00Z", 0.4, 0.09, 0.88, 0.68, 0.7, "THRASHING", "2026-03-30T10:01:30Z",
		"rspan-handler-mixed-workspace-agent-default", workspaceID, "DEFAULT", "queue_latency", "INCIDENT", "S1", "AGENT_DEFAULT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:07:00Z", 0.38, 0.07, 0.91, 0.7, 0.7, "STAGNATION", "2026-03-30T10:01:45Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "", "INCIDENT", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, 4, "2026-03-29T09:00:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline row: %v", err)
	}

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.Summary.ShrinkageFallbackQualityBand != "MIXED_WORKSPACE_FALLBACK" || dump.Summary.ShrinkageRelianceBand != "ALL_SHRUNK" {
		t.Fatalf("expected handler dump to surface mixed workspace fallback provenance, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceFallbackMixBand != "BALANCED" {
		t.Fatalf("expected handler dump to surface balanced workspace fallback mix, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureBand != "ALL_SHRUNK" {
		t.Fatalf("expected handler dump to keep all-shrunk workspace-tier pressure on mixed workspace fallback, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureCounts["workspace_tier"] != 2 || dump.Summary.WorkspaceTierPressureCounts["agent_tier"] != 0 {
		t.Fatalf("expected handler dump to surface mixed workspace tier pressure counts, got %+v", dump.Summary.WorkspaceTierPressureCounts)
	}
	if dump.Summary.WorkspaceFallbackMixCounts["exact_workspace"] != 1 || dump.Summary.WorkspaceFallbackMixCounts["agent_default_workspace"] != 1 {
		t.Fatalf("expected handler dump to surface balanced workspace fallback mix counts, got %+v", dump.Summary.WorkspaceFallbackMixCounts)
	}
	if dump.Summary.ShrinkageFallbackScopeTier != "WORKSPACE_TIER" {
		t.Fatalf("expected handler dump to surface workspace-tier mixed workspace fallback provenance, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyWarmingDriver != "ALL_SHRUNK_MIXED_WORKSPACE_FALLBACK" {
		t.Fatalf("expected handler dump to surface mixed workspace fallback warming driver, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_mixed_workspace_coverage_sparse_workspace_tier") || !containsString(dump.Summary.CoverageGaps, "anomaly_mixed_workspace_fallback_all_shrunk_workspace_tier") {
		t.Fatalf("expected handler dump to use mixed-workspace gap family on mixed workspace fallback, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_workspace_fallback_all_shrunk_workspace_tier") || containsString(dump.Summary.CoverageGaps, "anomaly_agent_default_workspace_fallback_all_shrunk_workspace_tier") || containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse_workspace_tier") {
		t.Fatalf("expected handler dump to avoid generic/pure agent-default workspace gap families on mixed workspace fallback, got %+v", dump.Summary)
	}
}

func TestWorkspaceRSPTelemetryDumpSurfacesCalibrationSummary(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-rsp-telemetry-summary"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a", "agent-b"})
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_belief_telemetry(id, workspace_id, event_id, entity_type, entity_id, prior_b_m, posterior_b_m, uncertainty_u_m, evidence_mass, drift_score, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rsptl-summary", workspaceID, "evt-summary", "FACT", "claim-summary", 0.0, 0.4, 0.7, 1.0, 0.6, "2026-03-30T10:00:00Z",
	); err != nil {
		t.Fatalf("seed belief telemetry row: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-summary", workspaceID, "DEFAULT", "verifier_fail_rate", "INCIDENT", "S1", "WORKSPACE_DEFAULT", 3, "2026-03-29T09:00:00Z", 0.1, 0.1, 0.8, 0.52, 0.8, "THRASHING", "2026-03-30T10:01:00Z",
		"rspan-summary-shrunk", workspaceID, "DEFAULT", "queue_latency", "INCIDENT", "S1", "EXACT_SHRUNK_AGENT_DEFAULT", 4, "2026-03-29T09:05:00Z", 0.45, 0.08, 0.9, 0.7, 0.7, "STAGNATION", "2026-03-30T10:01:30Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry row: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_agent_state_telemetry(id, workspace_id, agent_id, cache_pressure_i, stale_hit_i, thrashing_risk, ungrounded_risk, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"stlog-summary", workspaceID, "agent-a", 0.7, 0.3, 0.8, 0.2, "2026-03-30T10:02:00Z",
	); err != nil {
		t.Fatalf("seed state telemetry row: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "", "UNKNOWN", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, 3, "2026-03-29T09:00:00Z",
		workspaceID, "agent-a", "INCIDENT", "DEFAULT", "S1", "patch_rate", 0.8, 0.1, 4, "2026-03-29T09:05:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline rows: %v", err)
	}

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.Summary.BeliefLogCount == 0 || dump.Summary.AnomalyAlertCount != 2 || dump.Summary.StateLogCount != 1 {
		t.Fatalf("expected handler dump to surface telemetry summary counts, got %+v", dump.Summary)
	}
	if dump.Summary.WarmedAnomalyAlertCount != 2 || dump.Summary.ThrashingAlertCount != 1 || dump.Summary.StagnationAlertCount != 1 || dump.Summary.StateHighThrashingCount != 1 {
		t.Fatalf("expected handler dump to surface bounded calibration summary tallies, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyBaselineCount != 2 || dump.Summary.WarmAnomalyBaselineCount != 2 || dump.Summary.AnomalyLogsWithBaselineCount != 2 {
		t.Fatalf("expected handler dump to surface baseline readiness counts, got %+v", dump.Summary)
	}
	if dump.Summary.ShrunkAnomalyAlertCount != 1 || dump.Summary.ShrunkAnomalyScopeCounts["EXACT_SHRUNK_AGENT_DEFAULT"] != 1 {
		t.Fatalf("expected handler dump to surface shrinkage provenance rollup, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageRelianceBand != "PARTIAL" {
		t.Fatalf("expected handler dump to surface partial shrinkage reliance, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageFallbackQualityBand != "AGENT_LOCALIZED" {
		t.Fatalf("expected handler dump to surface agent-localized fallback quality, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageFallbackScopeTier != "AGENT_TIER" {
		t.Fatalf("expected handler dump to surface agent-tier shrinkage provenance, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureCounts["workspace_tier"] != 0 || dump.Summary.WorkspaceTierPressureCounts["agent_tier"] != 1 {
		t.Fatalf("expected handler dump to surface agent-tier pressure counts, got %+v", dump.Summary.WorkspaceTierPressureCounts)
	}
	if dump.Summary.AnomalyBaselineScopeCounts["WORKSPACE_DEFAULT"] != 1 || dump.Summary.AnomalyBaselineScopeCounts["EXACT"] != 1 {
		t.Fatalf("expected handler dump to surface baseline scope counts, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessBand != "OBSERVABLE" {
		t.Fatalf("expected handler dump to surface observable readiness, got %+v", dump.Summary)
	}
	if dump.Summary.BeliefReadinessBand != "WARMING" || dump.Summary.AnomalyReadinessBand != "OBSERVABLE" || dump.Summary.StateReadinessBand != "WARMING" {
		t.Fatalf("expected handler dump to surface per-stream readiness bands, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil {
		t.Fatalf("expected handler dump to surface additive readiness/coverage rollup, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup.OverallReadinessBand != "OBSERVABLE" ||
		dump.Summary.ReadinessCoverageRollup.ObservableStreamCount != 1 ||
		dump.Summary.ReadinessCoverageRollup.WarmingStreamCount != 2 ||
		dump.Summary.ReadinessCoverageRollup.InsufficientStreamCount != 0 {
		t.Fatalf("expected handler dump to mirror per-stream readiness rollup, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
	if !containsString(dump.Summary.CoverageGaps, "belief_coverage_thin") ||
		!containsString(dump.Summary.CoverageGaps, "state_coverage_thin") ||
		!containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse_agent_tier") {
		t.Fatalf("expected handler dump to surface coverage gaps, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup.CoverageGapCount != 3 || !dump.Summary.ReadinessCoverageRollup.HasCoverageGaps {
		t.Fatalf("expected handler dump to mirror bounded coverage-gap presence, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
	if dump.Summary.ReadinessCoverageRollup.CoverageGapCounts["belief_coverage_thin"] != 1 ||
		dump.Summary.ReadinessCoverageRollup.CoverageGapCounts["state_coverage_thin"] != 1 ||
		dump.Summary.ReadinessCoverageRollup.CoverageGapCounts["anomaly_exact_coverage_sparse_agent_tier"] != 1 {
		t.Fatalf("expected handler dump to mirror coverage-gap counts, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
}

func TestWorkspaceRSPTelemetryDumpUsesMissingGapWhenNoPersistedAnomalyBaselineExists(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-rsp-telemetry-missing-baseline-gap"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_belief_telemetry(id, workspace_id, event_id, entity_type, entity_id, prior_b_m, posterior_b_m, uncertainty_u_m, evidence_mass, drift_score, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rsptl-missing-baseline-1", workspaceID, "evt-missing-baseline-1", "FACT", "claim-missing-baseline-1", 0.0, 0.4, 0.7, 1.0, 0.6, "2026-03-30T10:00:00Z",
		"rsptl-missing-baseline-2", workspaceID, "evt-missing-baseline-2", "FACT", "claim-missing-baseline-2", 0.0, 0.2, 0.2, 1.0, 0.1, "2026-03-30T09:59:00Z",
	); err != nil {
		t.Fatalf("seed belief telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-missing-baseline", workspaceID, "DEFAULT", "queue_latency", "INCIDENT", "S1", "WORKSPACE_DEFAULT", 1, "2026-03-29T09:04:00Z", 0.3, 0.07, 0.82, 0.64, 0.8, "STAGNATION", "2026-03-30T10:01:00Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry row: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_agent_state_telemetry(id, workspace_id, agent_id, cache_pressure_i, stale_hit_i, thrashing_risk, ungrounded_risk, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)`,
		"stlog-missing-baseline-1", workspaceID, "agent-a", 0.7, 0.3, 0.8, 0.2, "2026-03-30T10:02:00Z",
		"stlog-missing-baseline-2", workspaceID, "agent-a", 0.5, 0.2, 0.4, 0.1, "2026-03-30T10:01:30Z",
	); err != nil {
		t.Fatalf("seed state telemetry rows: %v", err)
	}

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.Summary.AnomalyReadinessBand != "WARMING" {
		t.Fatalf("expected anomaly readiness to stay warming without persisted baselines, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyWarmingDriver != "BASELINE_MISSING" {
		t.Fatalf("expected handler dump to surface baseline-missing warming driver, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_baseline_missing") {
		t.Fatalf("expected handler dump to surface missing-baseline gap, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_baseline_cold") {
		t.Fatalf("expected handler dump to avoid cold-baseline gap when persisted baselines are absent, got %+v", dump.Summary)
	}
}

func TestWorkspaceRSPTelemetryDumpUsesMissingGapWithoutShrinkageCoverageWhenShrunkAlertsHaveNoPersistedBaseline(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-rsp-telemetry-missing-baseline-shrunk-gap"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_belief_telemetry(id, workspace_id, event_id, entity_type, entity_id, prior_b_m, posterior_b_m, uncertainty_u_m, evidence_mass, drift_score, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rsptl-missing-shrunk-1", workspaceID, "evt-missing-shrunk-1", "FACT", "claim-missing-shrunk-1", 0.0, 0.4, 0.7, 1.0, 0.6, "2026-03-30T10:00:00Z",
		"rsptl-missing-shrunk-2", workspaceID, "evt-missing-shrunk-2", "FACT", "claim-missing-shrunk-2", 0.0, 0.2, 0.2, 1.0, 0.1, "2026-03-30T09:59:00Z",
	); err != nil {
		t.Fatalf("seed belief telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-missing-shrunk", workspaceID, "DEFAULT", "queue_latency", "INCIDENT", "S1", "EXACT_SHRUNK_WORKSPACE_DEFAULT", 1, "2026-03-29T09:04:00Z", 0.3, 0.07, 0.82, 0.64, 0.8, "STAGNATION", "2026-03-30T10:01:00Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry row: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_agent_state_telemetry(id, workspace_id, agent_id, cache_pressure_i, stale_hit_i, thrashing_risk, ungrounded_risk, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)`,
		"stlog-missing-shrunk-1", workspaceID, "agent-a", 0.7, 0.3, 0.8, 0.2, "2026-03-30T10:02:00Z",
		"stlog-missing-shrunk-2", workspaceID, "agent-a", 0.5, 0.2, 0.4, 0.1, "2026-03-30T10:01:30Z",
	); err != nil {
		t.Fatalf("seed state telemetry rows: %v", err)
	}

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.Summary.AnomalyReadinessBand != "WARMING" {
		t.Fatalf("expected anomaly readiness to stay warming on missing-baseline shrunk path, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_baseline_missing") {
		t.Fatalf("expected handler dump to surface missing-baseline gap on shrunk path without persisted baselines, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse") || containsString(dump.Summary.CoverageGaps, "anomaly_workspace_fallback") {
		t.Fatalf("expected handler dump to avoid shrinkage-derived gaps when persisted baselines are absent, got %+v", dump.Summary)
	}
}

func TestWorkspaceRSPTelemetryDumpKeepsColdGapWithoutShrinkageProvenanceWhenBaselinesAreNotWarm(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-rsp-telemetry-cold-no-shrink-provenance"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_belief_telemetry(id, workspace_id, event_id, entity_type, entity_id, prior_b_m, posterior_b_m, uncertainty_u_m, evidence_mass, drift_score, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rsptl-cold-no-prov-1", workspaceID, "evt-cold-no-prov-1", "FACT", "claim-cold-no-prov-1", 0.0, 0.4, 0.7, 1.0, 0.6, "2026-03-30T10:00:00Z",
		"rsptl-cold-no-prov-2", workspaceID, "evt-cold-no-prov-2", "FACT", "claim-cold-no-prov-2", 0.0, 0.2, 0.2, 1.0, 0.1, "2026-03-30T09:59:00Z",
	); err != nil {
		t.Fatalf("seed belief telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-cold-no-prov", workspaceID, "DEFAULT", "queue_latency", "INCIDENT", "S1", "EXACT_SHRUNK_WORKSPACE_DEFAULT", 1, "2026-03-29T09:04:00Z", 0.3, 0.07, 0.82, 0.64, 0.8, "STAGNATION", "2026-03-30T10:01:00Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry row: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_agent_state_telemetry(id, workspace_id, agent_id, cache_pressure_i, stale_hit_i, thrashing_risk, ungrounded_risk, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)`,
		"stlog-cold-no-prov-1", workspaceID, "agent-a", 0.7, 0.3, 0.8, 0.2, "2026-03-30T10:02:00Z",
		"stlog-cold-no-prov-2", workspaceID, "agent-a", 0.5, 0.2, 0.4, 0.1, "2026-03-30T10:01:30Z",
	); err != nil {
		t.Fatalf("seed state telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "", "INCIDENT", "DEFAULT", "S1", "queue_latency", 0.3, 0.07, 1, "2026-03-29T09:04:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline row: %v", err)
	}

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.Summary.AnomalyReadinessBand != "WARMING" {
		t.Fatalf("expected anomaly readiness to stay warming on cold-baseline path, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyWarmingDriver != "BASELINE_COLD" {
		t.Fatalf("expected handler dump to keep baseline-cold warming driver on cold-baseline path, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_baseline_cold") {
		t.Fatalf("expected handler dump to keep cold-baseline gap on cold-baseline path, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse") || containsString(dump.Summary.CoverageGaps, "anomaly_workspace_fallback") {
		t.Fatalf("expected handler dump to avoid shrinkage-derived gaps without warmed baseline-backed observability, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageRelianceBand != "NONE" || dump.Summary.ShrinkageFallbackQualityBand != "NONE" || dump.Summary.ShrinkageFallbackScopeTier != "NONE" {
		t.Fatalf("expected handler dump to suppress shrinkage provenance bands on cold-baseline path, got %+v", dump.Summary)
	}
}

func TestWorkspaceRSPTelemetryDumpUsesObservabilityMissingGapWhenWarmBaselinesExistWithoutAnomalyObservability(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-rsp-telemetry-observability-missing"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_belief_telemetry(id, workspace_id, event_id, entity_type, entity_id, prior_b_m, posterior_b_m, uncertainty_u_m, evidence_mass, drift_score, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rsptl-observability-missing-belief", workspaceID, "evt-observability-missing-belief", "FACT", "claim-observability-missing", 0.0, 0.4, 0.7, 1.0, 0.6, "2026-03-30T10:00:00Z",
	); err != nil {
		t.Fatalf("seed belief telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_agent_state_telemetry(id, workspace_id, agent_id, cache_pressure_i, stale_hit_i, thrashing_risk, ungrounded_risk, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)`,
		"stlog-observability-missing-1", workspaceID, "agent-a", 0.7, 0.3, 0.8, 0.2, "2026-03-30T10:02:00Z",
		"stlog-observability-missing-2", workspaceID, "agent-a", 0.2, 0.1, 0.2, 0.7, "2026-03-30T10:01:30Z",
	); err != nil {
		t.Fatalf("seed state telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "", "UNKNOWN", "DEFAULT", "S1", "verifier_fail_rate", 0.1, 0.1, 3, "2026-03-29T09:00:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline row: %v", err)
	}

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.Summary.AnomalyReadinessBand != "WARMING" || dump.Summary.ReadinessBand != "WARMING" {
		t.Fatalf("expected handler dump to keep anomaly readiness warming when no baseline-backed anomaly observability exists yet, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyWarmingDriver != "OBSERVABILITY_MISSING" {
		t.Fatalf("expected handler dump to surface observability-missing warming driver, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_baseline_observability_missing") {
		t.Fatalf("expected handler dump to surface observability-missing gap, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_baseline_cold") || containsString(dump.Summary.CoverageGaps, "anomaly_baseline_missing") {
		t.Fatalf("expected handler dump to avoid cold/missing baseline gaps on warm-baseline observability-missing path, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageRelianceBand != "NONE" || dump.Summary.ShrinkageFallbackQualityBand != "NONE" || dump.Summary.ShrinkageFallbackScopeTier != "NONE" {
		t.Fatalf("expected handler dump to keep shrinkage provenance inactive without baseline-backed anomaly observability, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil || dump.Summary.ReadinessCoverageRollup.OverallReadinessBand != "WARMING" {
		t.Fatalf("expected handler dump to keep readiness rollup warming on observability-missing path, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
	if dump.Summary.ReadinessCoverageRollup.CoverageGapCounts["anomaly_baseline_observability_missing"] != 1 {
		t.Fatalf("expected handler dump to count observability-missing gap in readiness rollup, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
	if dump.Summary.ReadinessCoverageRollup.CoverageGapCounts["anomaly_baseline_cold"] != 0 {
		t.Fatalf("expected handler dump to avoid cold-baseline rollup counts on observability-missing path, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
}

func TestWorkspaceRSPTelemetryDumpKeepsMixedDominantFallbackObservable(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-rsp-telemetry-mixed-dominant-shrink"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a", "agent-b"})
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_belief_telemetry(id, workspace_id, event_id, entity_type, entity_id, prior_b_m, posterior_b_m, uncertainty_u_m, evidence_mass, drift_score, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rsptl-mixed-dominant-1", workspaceID, "evt-mixed-dominant-1", "FACT", "claim-mixed-dominant-1", 0.0, 0.4, 0.7, 1.0, 0.6, "2026-03-30T10:00:00Z",
		"rsptl-mixed-dominant-2", workspaceID, "evt-mixed-dominant-2", "FACT", "claim-mixed-dominant-2", 0.0, 0.2, 0.2, 1.0, 0.1, "2026-03-30T09:59:00Z",
	); err != nil {
		t.Fatalf("seed belief telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-mixed-dominant-agent", workspaceID, "DEFAULT", "queue_latency", "INCIDENT", "S1", "EXACT_SHRUNK_AGENT_DEFAULT", 4, "2026-03-29T09:05:00Z", 0.45, 0.08, 0.9, 0.7, 0.7, "STAGNATION", "2026-03-30T10:01:30Z",
		"rspan-mixed-dominant-ws-1", workspaceID, "DEFAULT", "verifier_fail_rate", "INCIDENT", "S1", "EXACT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:06:00Z", 0.4, 0.09, 0.88, 0.68, 0.7, "THRASHING", "2026-03-30T10:01:00Z",
		"rspan-mixed-dominant-ws-2", workspaceID, "DEFAULT", "verifier_timeout", "INCIDENT", "S1", "EXACT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:06:30Z", 0.39, 0.08, 0.86, 0.67, 0.7, "THRASHING", "2026-03-30T10:00:50Z",
		"rspan-mixed-dominant-direct", workspaceID, "DEFAULT", "patch_rate", "INCIDENT", "S1", "AGENT_DEFAULT", 4, "2026-03-29T09:07:00Z", 0.38, 0.06, 0.74, 0.6, 0.8, "THRASHING", "2026-03-30T10:00:30Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_agent_state_telemetry(id, workspace_id, agent_id, cache_pressure_i, stale_hit_i, thrashing_risk, ungrounded_risk, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)`,
		"stlog-mixed-partial-1", workspaceID, "agent-a", 0.7, 0.3, 0.8, 0.2, "2026-03-30T10:02:00Z",
		"stlog-mixed-partial-2", workspaceID, "agent-b", 0.2, 0.1, 0.2, 0.7, "2026-03-30T10:01:30Z",
	); err != nil {
		t.Fatalf("seed state telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "agent-a", "INCIDENT", "DEFAULT", "S1", "queue_latency", 0.45, 0.08, 4, "2026-03-29T09:05:00Z",
		workspaceID, "", "INCIDENT", "DEFAULT", "S1", "verifier_fail_rate", 0.4, 0.09, 4, "2026-03-29T09:06:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline rows: %v", err)
	}

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.Summary.ShrinkageFallbackQualityBand != "MIXED" || dump.Summary.ShrinkageRelianceBand != "DOMINANT" {
		t.Fatalf("expected mixed dominant fallback provenance, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyWarmingDriver != "NONE" {
		t.Fatalf("expected handler dump to avoid warming-driver label on observable mixed dominant fallback, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageFallbackScopeTier != "MIXED_TIERS" {
		t.Fatalf("expected mixed-tier shrinkage provenance, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureBand != "DOMINANT" {
		t.Fatalf("expected mixed dominant fallback to keep dominant workspace-tier pressure, got %+v", dump.Summary)
	}
	if dump.Summary.WorkspaceTierPressureCounts["workspace_tier"] != 2 || dump.Summary.WorkspaceTierPressureCounts["agent_tier"] != 1 {
		t.Fatalf("expected handler dump to surface dominant workspace-vs-agent pressure counts, got %+v", dump.Summary.WorkspaceTierPressureCounts)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_mixed_coverage_sparse_mixed_tiers") || !containsString(dump.Summary.CoverageGaps, "anomaly_mixed_fallback_dominant_mixed_tiers") {
		t.Fatalf("expected mixed dominant fallback to surface mixed-tier coverage gaps, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected mixed dominant fallback to avoid generic exact mixed-tier sparse gap, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyReadinessBand != "OBSERVABLE" || dump.Summary.ReadinessBand != "OBSERVABLE" {
		t.Fatalf("expected mixed dominant fallback to keep anomaly and aggregate readiness observable, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil || dump.Summary.ReadinessCoverageRollup.OverallReadinessBand != "OBSERVABLE" {
		t.Fatalf("expected readiness rollup to stay observable on mixed dominant fallback, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
}

func TestWorkspaceRSPTelemetryDumpSuppressesIncidentalMixedWorkspaceFallbackGap(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-rsp-telemetry-mixed-incidental-ws-shrink"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a", "agent-b"})
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_belief_telemetry(id, workspace_id, event_id, entity_type, entity_id, prior_b_m, posterior_b_m, uncertainty_u_m, evidence_mass, drift_score, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rsptl-mixed-incidental-1", workspaceID, "evt-mixed-incidental-1", "FACT", "claim-mixed-incidental-1", 0.0, 0.4, 0.7, 1.0, 0.6, "2026-03-30T10:00:00Z",
		"rsptl-mixed-incidental-2", workspaceID, "evt-mixed-incidental-2", "FACT", "claim-mixed-incidental-2", 0.0, 0.2, 0.2, 1.0, 0.1, "2026-03-30T09:59:00Z",
	); err != nil {
		t.Fatalf("seed belief telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"rspan-mixed-incidental-agent-1", workspaceID, "DEFAULT", "queue_latency", "INCIDENT", "S1", "EXACT_SHRUNK_AGENT_DEFAULT", 4, "2026-03-29T09:05:00Z", 0.45, 0.08, 0.9, 0.7, 0.7, "STAGNATION", "2026-03-30T10:01:30Z",
		"rspan-mixed-incidental-agent-2", workspaceID, "DEFAULT", "queue_depth", "INCIDENT", "S1", "EXACT_SHRUNK_AGENT_DEFAULT", 4, "2026-03-29T09:05:30Z", 0.41, 0.07, 0.87, 0.66, 0.7, "THRASHING", "2026-03-30T10:01:20Z",
		"rspan-mixed-incidental-ws", workspaceID, "DEFAULT", "verifier_fail_rate", "INCIDENT", "S1", "EXACT_SHRUNK_WORKSPACE_DEFAULT", 4, "2026-03-29T09:06:00Z", 0.4, 0.09, 0.88, 0.68, 0.7, "THRASHING", "2026-03-30T10:01:00Z",
		"rspan-mixed-incidental-direct", workspaceID, "DEFAULT", "patch_rate", "INCIDENT", "S1", "AGENT_DEFAULT", 4, "2026-03-29T09:07:00Z", 0.38, 0.06, 0.74, 0.6, 0.8, "STAGNATION", "2026-03-30T10:00:30Z",
	); err != nil {
		t.Fatalf("seed anomaly telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_agent_state_telemetry(id, workspace_id, agent_id, cache_pressure_i, stale_hit_i, thrashing_risk, ungrounded_risk, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)`,
		"stlog-mixed-incidental-1", workspaceID, "agent-a", 0.7, 0.3, 0.8, 0.2, "2026-03-30T10:02:00Z",
		"stlog-mixed-incidental-2", workspaceID, "agent-b", 0.2, 0.1, 0.2, 0.7, "2026-03-30T10:01:30Z",
	); err != nil {
		t.Fatalf("seed state telemetry rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID, "agent-a", "INCIDENT", "DEFAULT", "S1", "queue_latency", 0.45, 0.08, 4, "2026-03-29T09:05:00Z",
		workspaceID, "", "INCIDENT", "DEFAULT", "S1", "verifier_fail_rate", 0.4, 0.09, 4, "2026-03-29T09:06:00Z",
	); err != nil {
		t.Fatalf("seed anomaly baseline rows: %v", err)
	}

	result, rpcErr := h.workspaceRSPTelemetryDump(ctx, mustJSONRaw(map[string]any{
		"workspace_id": workspaceID,
		"limit":        10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPTelemetryDump rpc error: %+v", rpcErr)
	}
	dump := result.(sqlite.RSPTelemetryDump)
	if dump.Summary.ShrinkageFallbackQualityBand != "MIXED" || dump.Summary.ShrinkageRelianceBand != "DOMINANT" {
		t.Fatalf("expected mixed incidental workspace fallback provenance, got %+v", dump.Summary)
	}
	if dump.Summary.ShrinkageFallbackScopeTier != "MIXED_TIERS" {
		t.Fatalf("expected mixed-tier shrinkage provenance, got %+v", dump.Summary)
	}
	if !containsString(dump.Summary.CoverageGaps, "anomaly_mixed_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected mixed incidental workspace shrinkage to keep mixed-tier sparse coverage gap, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_exact_coverage_sparse_mixed_tiers") {
		t.Fatalf("expected mixed incidental workspace shrinkage to avoid generic exact mixed-tier sparse gap, got %+v", dump.Summary)
	}
	if containsString(dump.Summary.CoverageGaps, "anomaly_mixed_fallback_partial_mixed_tiers") {
		t.Fatalf("expected mixed incidental workspace shrinkage to suppress mixed-tier workspace-fallback gap, got %+v", dump.Summary)
	}
	if dump.Summary.AnomalyReadinessBand != "OBSERVABLE" || dump.Summary.ReadinessBand != "OBSERVABLE" {
		t.Fatalf("expected mixed incidental workspace shrinkage to keep anomaly and aggregate readiness observable, got %+v", dump.Summary)
	}
	if dump.Summary.ReadinessCoverageRollup == nil || dump.Summary.ReadinessCoverageRollup.OverallReadinessBand != "OBSERVABLE" {
		t.Fatalf("expected readiness rollup to stay observable on mixed incidental workspace shrinkage, got %+v", dump.Summary.ReadinessCoverageRollup)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
