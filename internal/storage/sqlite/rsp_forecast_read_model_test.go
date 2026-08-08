package sqlite

import (
	"context"
	"encoding/json"
	"testing"
)

func seedInstrumentationLocusHealthyScenarioWithAuthority(t *testing.T, ctx context.Context, store *Store, suffix string) locusSidecarScenario {
	t.Helper()

	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, suffix)
	claimTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	return scenario
}

func TestBuildRSPForecastReportExposesWorkspaceTimeAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenarioWithAuthority(t, ctx, store, "rsp-forecast-report")

	report, err := store.BuildRSPForecastReport(ctx, RSPForecastReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build rsp forecast report: %v", err)
	}
	if !report.Resolved || report.SignalType != rspForecastSignalType || !report.ShadowMode {
		t.Fatalf("expected resolved shadow forecast report, got %+v", report)
	}
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp forecast report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp forecast report generated_at %q to mirror authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if report.ShadowPhase != rspForecastShadowPhase {
		t.Fatalf("expected rsp forecast report to stay in shadow phase %s, got %+v", rspForecastShadowPhase, report)
	}
	if len(report.Projections) == 0 {
		t.Fatalf("expected rsp forecast report to expose bounded projections, got %+v", report)
	}
	if report.ForecastReadiness != "PARTIAL" {
		t.Fatalf("expected thin default forecast fixture to stay partial, got %+v", report)
	}
	if report.ScenarioReadiness != "UNAVAILABLE" {
		t.Fatalf("expected default forecast fixture to keep scenario readiness unavailable without explicit plan inputs, got %+v", report)
	}
	assertForecastTemporalContracts(t, report.TimeAuthority, report.Projections)
	if report.Calibration.SchemaVersion != rspCalibrationSchemaVersion ||
		report.Calibration.Status != rspCalibrationStatusProvisional ||
		report.Calibration.CalibrationVersion != "forecast-shadow-s2-v2" {
		t.Fatalf("expected forecast report to expose versioned provisional calibration contract, got %+v", report.Calibration)
	}
	if !containsString(report.Calibration.Unsupported, "root_cause_independence") ||
		!containsString(report.Calibration.Unsupported, "broad_external_non_control_inputs") {
		t.Fatalf("expected forecast report calibration contract to keep unsupported semantics explicit, got %+v", report.Calibration)
	}
	if report.ForecastCoverageSummary == nil {
		t.Fatalf("expected forecast report to surface coverage summary, got %+v", report)
	}
	if report.ForecastCoverageSummary.ProjectionCount != len(report.Projections) || report.ForecastCoverageSummary.MissingInputCount != len(report.MissingInputs) {
		t.Fatalf("expected forecast coverage summary to align with projections and missing inputs, got %+v", report.ForecastCoverageSummary)
	}
	if report.ForecastCoverageSummary.EvidenceBackedProjectionCount == 0 || report.ForecastCoverageSummary.EvidenceRefCount != len(report.EvidenceRefs) {
		t.Fatalf("expected forecast coverage summary to surface evidence-backed coverage, got %+v", report.ForecastCoverageSummary)
	}
	if len(report.ForecastCoverageSummary.BasisCount) == 0 || len(report.ForecastCoverageSummary.ModelCount) == 0 {
		t.Fatalf("expected forecast coverage summary to surface basis/model counts, got %+v", report.ForecastCoverageSummary)
	}
	if report.ForecastCoverageSummary.PlannedInputCount != 0 || report.ForecastCoverageSummary.ScenarioAdjustedProjectionCount != 0 {
		t.Fatalf("expected default forecast coverage summary not to claim scenario conditioning, got %+v", report.ForecastCoverageSummary)
	}
	if !containsLocusString(report.ForecastProvenanceHints, "basis:fresh") ||
		!containsLocusString(report.ForecastProvenanceHints, "missing:metrics_history") {
		t.Fatalf("expected forecast provenance hints to explain bounded readiness, got %+v", report.ForecastProvenanceHints)
	}
	if !containsLocusString(report.ScenarioProvenanceHints, "scenario:none") {
		t.Fatalf("expected default forecast scenario hints to stay explicit about missing plan inputs, got %+v", report.ScenarioProvenanceHints)
	}
}

func TestBuildRSPForecastReportMarksWarmCoverageReady(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenarioWithAuthority(t, ctx, store, "rsp-forecast-ready")

	for i, staleHits := range []int{1, 0} {
		if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
			WorkspaceID:        scenario.workspaceID,
			AgentID:            scenario.agentID,
			SessionID:          scenario.sessionID,
			ReportScope:        "SESSION",
			ReportID:           "memmet-rsp-forecast-ready-" + string(rune('1'+i)),
			LookupCount:        12,
			L1HitCount:         7,
			L2HitCount:         3,
			StaleHitCount:      staleHits,
			PromotionCount:     1,
			FlushCount:         2,
			FlushPositiveCount: 1,
		}); err != nil {
			t.Fatalf("report memory metrics %d: %v", i+1, err)
		}
	}

	report, err := store.BuildRSPForecastReport(ctx, RSPForecastReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build ready rsp forecast report: %v", err)
	}
	if report.ForecastReadiness != "READY" {
		t.Fatalf("expected warmed forecast coverage to become ready, got %+v", report)
	}
	if report.ScenarioReadiness != "UNAVAILABLE" {
		t.Fatalf("expected warmed endogenous forecast not to become scenario-ready without explicit plan inputs, got %+v", report)
	}
	if containsLocusString(report.ForecastProvenanceHints, "missing:metrics_history") {
		t.Fatalf("did not expect warmed forecast readiness to retain metrics-history gap, got %+v", report.ForecastProvenanceHints)
	}
	if !containsLocusString(report.ForecastProvenanceHints, "coverage:ready") {
		t.Fatalf("expected warmed forecast readiness to surface ready coverage hint, got %+v", report.ForecastProvenanceHints)
	}
	if report.ForecastCoverageSummary == nil || report.ForecastCoverageSummary.HistoryBackedProjectionCount == 0 {
		t.Fatalf("expected warmed forecast coverage to surface history-backed projections, got %+v", report.ForecastCoverageSummary)
	}
	if report.ForecastCoverageSummary.AlertProjectionCount != len(report.AlertVariables) {
		t.Fatalf("expected forecast coverage summary to align alert projection count with alert variables, got %+v alerts=%+v", report.ForecastCoverageSummary, report.AlertVariables)
	}
	if report.ForecastCoverageSummary.PlannedInputCount != 0 || report.ForecastCoverageSummary.ScenarioAdjustedProjectionCount != 0 {
		t.Fatalf("expected warmed endogenous forecast not to claim scenario-adjusted coverage, got %+v", report.ForecastCoverageSummary)
	}
}

func TestBuildRSPForecastReportSurfacesSharedLatencyHistory(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenarioWithAuthority(t, ctx, store, "rsp-forecast-latency")

	latencies := []int64{999, 320, 300, 280, 140, 120, 100}
	statuses := []string{"error", "ok", "ok", "ok", "ok", "ok", "ok"}
	createdAt := []string{
		"2026-04-08T10:07:00Z",
		"2026-04-08T10:06:00Z",
		"2026-04-08T10:05:00Z",
		"2026-04-08T10:04:00Z",
		"2026-04-08T10:03:00Z",
		"2026-04-08T10:02:00Z",
		"2026-04-08T10:01:00Z",
	}
	for idx, latency := range latencies {
		if _, err := store.db.ExecContext(ctx, `INSERT INTO rpc_access_log (method, workspace_id, actor, status, error_msg, latency_ms, created_at) VALUES (?,?,?,?,?,?,?)`,
			"workspace.memory.get", scenario.workspaceID, "system", statuses[idx], "", latency, createdAt[idx]); err != nil {
			t.Fatalf("insert rpc access log %d: %v", idx, err)
		}
	}

	report, err := store.BuildRSPForecastReport(ctx, RSPForecastReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build shared-latency rsp forecast report: %v", err)
	}
	if containsLocusString(report.MissingInputs, "shared_latency") {
		t.Fatalf("expected persisted rpc access history to satisfy shared_latency input, got %+v", report)
	}
	if !containsString(report.EvidenceRefs, "rpc_access_log:recent") {
		t.Fatalf("expected shared latency evidence ref to be attached, got %+v", report.EvidenceRefs)
	}
	var latencyProjection *RSPForecastProjection
	for idx := range report.Projections {
		if report.Projections[idx].Variable == "SHARED_LATENCY" {
			latencyProjection = &report.Projections[idx]
			break
		}
	}
	if latencyProjection == nil {
		t.Fatalf("expected shared latency projection, got %+v", report.Projections)
	}
	if latencyProjection.Basis != "latency_ms" || latencyProjection.Unit != "ms" {
		t.Fatalf("expected shared latency projection metadata, got %+v", latencyProjection)
	}
	if latencyProjection.CurrentValue <= 0 {
		t.Fatalf("expected shared latency projection to carry current value, got %+v", latencyProjection)
	}
	if latencyProjection.CurrentValue != 300 {
		t.Fatalf("expected failed rpc rows to be excluded from shared latency average, got %+v", latencyProjection)
	}
	if report.ForecastCoverageSummary == nil || report.ForecastCoverageSummary.HistoryBackedProjectionCount == 0 {
		t.Fatalf("expected shared latency history to contribute history-backed coverage, got %+v", report.ForecastCoverageSummary)
	}
}

func TestBuildRSPForecastReportAppliesPendingControlPlanInputs(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenarioWithAuthority(t, ctx, store, "rsp-forecast-plan-inputs")

	for i, staleHits := range []int{1, 0} {
		if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
			WorkspaceID:        scenario.workspaceID,
			AgentID:            scenario.agentID,
			SessionID:          scenario.sessionID,
			ReportScope:        "SESSION",
			ReportID:           "memmet-rsp-forecast-plan-" + string(rune('1'+i)),
			LookupCount:        12,
			L1HitCount:         7,
			L2HitCount:         3,
			StaleHitCount:      staleHits,
			PromotionCount:     1,
			FlushCount:         2,
			FlushPositiveCount: 1,
		}); err != nil {
			t.Fatalf("report memory metrics %d: %v", i+1, err)
		}
	}

	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		Epoch:          2,
		TTLSeconds:     600,
		ControlMode:    clusterControlModeSteady,
		CandidateMode:  clusterControlModeStabilize,
		CandidateControls: ControlSuggestedControls{
			FanoutCap:      4,
			ReviewDepth:    1,
			ContextCap:     6,
			BridgeQuota:    4,
			MergeThreshold: 0.75,
			PriorityFocus:  "throughput",
		},
		AdvisoryControls: ControlSuggestedControls{
			FanoutCap:      4,
			ReviewDepth:    1,
			ContextCap:     6,
			BridgeQuota:    4,
			MergeThreshold: 0.75,
			PriorityFocus:  "throughput",
		},
		EffectiveControls: ControlSuggestedControls{
			FanoutCap:      2,
			ReviewDepth:    3,
			ContextCap:     4,
			BridgeQuota:    2,
			MergeThreshold: 0.60,
			PriorityFocus:  "review",
		},
		ResolvedFrom: "forecast-plan-test",
		MatchScore:   100,
		BasisSummary: "pending control plan for forecast scenario coverage",
		GeneratedAt:  "2099-01-01T00:00:00Z",
		ActorID:      "tests",
	}); err != nil {
		t.Fatalf("persist pending effective controls: %v", err)
	}

	report, err := store.BuildRSPForecastReport(ctx, RSPForecastReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build planned-input rsp forecast report: %v", err)
	}
	if report.ForecastReadiness != "READY" {
		t.Fatalf("expected baseline warmed coverage to stay ready, got %+v", report)
	}
	if report.ScenarioReadiness != "READY" {
		t.Fatalf("expected pending cluster-scoped effective controls to make scenario readiness ready, got %+v", report)
	}
	if report.ScenarioBasis != "PENDING_EFFECTIVE_CONTROLS" {
		t.Fatalf("expected bounded control-plan scenario basis, got %+v", report)
	}
	if !containsLocusString(report.ScenarioProvenanceHints, "planned:effective_controls_pending") {
		t.Fatalf("expected scenario hints to expose pending effective controls, got %+v", report.ScenarioProvenanceHints)
	}
	if report.ForecastCoverageSummary == nil || report.ForecastCoverageSummary.PlannedInputCount == 0 || report.ForecastCoverageSummary.ScenarioAdjustedProjectionCount == 0 {
		t.Fatalf("expected scenario coverage summary to expose planned inputs and adjusted projections, got %+v", report.ForecastCoverageSummary)
	}
	assertForecastTemporalContracts(t, report.TimeAuthority, report.Projections)
	var adjusted *RSPForecastProjection
	for idx := range report.Projections {
		if report.Projections[idx].Variable == "FANOUT_PRESSURE" {
			adjusted = &report.Projections[idx]
			break
		}
	}
	if adjusted == nil {
		t.Fatalf("expected planned-input forecast to include fanout pressure projection, got %+v", report.Projections)
	}
	if adjusted.ScenarioBasis != "CONTROL_PLAN_ADJUSTED" || len(adjusted.ScenarioAdjustments) == 0 {
		t.Fatalf("expected fanout pressure projection to carry control-plan adjustments, got %+v", adjusted)
	}
	if adjusted.ScenarioAdjustments[0].Delta >= 0 {
		t.Fatalf("expected tighter pending fanout cap to reduce projected fanout pressure, got %+v", adjusted.ScenarioAdjustments)
	}
}

func TestBuildRSPForecastReportKeepsNoOpPendingPlanPartial(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenarioWithAuthority(t, ctx, store, "rsp-forecast-plan-noop")

	for i, staleHits := range []int{1, 0} {
		if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
			WorkspaceID:        scenario.workspaceID,
			AgentID:            scenario.agentID,
			SessionID:          scenario.sessionID,
			ReportScope:        "SESSION",
			ReportID:           "memmet-rsp-forecast-noop-" + string(rune('1'+i)),
			LookupCount:        12,
			L1HitCount:         7,
			L2HitCount:         3,
			StaleHitCount:      staleHits,
			PromotionCount:     1,
			FlushCount:         2,
			FlushPositiveCount: 1,
		}); err != nil {
			t.Fatalf("report memory metrics %d: %v", i+1, err)
		}
	}

	controls := ControlSuggestedControls{
		FanoutCap:      4,
		ReviewDepth:    1,
		ContextCap:     6,
		BridgeQuota:    4,
		MergeThreshold: 0.75,
		PriorityFocus:  "throughput",
	}
	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       scenario.workspaceID,
		ProtoClusterID:    scenario.clusterID,
		Epoch:             2,
		TTLSeconds:        600,
		ControlMode:       clusterControlModeSteady,
		CandidateMode:     clusterControlModeStabilize,
		CandidateControls: controls,
		AdvisoryControls:  controls,
		EffectiveControls: controls,
		ResolvedFrom:      "forecast-plan-noop-test",
		MatchScore:        100,
		BasisSummary:      "pending plan exists but does not change control values",
		GeneratedAt:       "2099-01-01T00:00:00Z",
		ActorID:           "tests",
	}); err != nil {
		t.Fatalf("persist noop pending effective controls: %v", err)
	}

	report, err := store.BuildRSPForecastReport(ctx, RSPForecastReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("build noop planned-input rsp forecast report: %v", err)
	}
	if report.ForecastReadiness != "READY" {
		t.Fatalf("expected baseline warmed coverage to stay ready, got %+v", report)
	}
	if report.ScenarioReadiness != "PARTIAL" {
		t.Fatalf("expected noop pending plan to stay scenario-partial, got %+v", report)
	}
	if !containsLocusString(report.ScenarioProvenanceHints, "adjustment:none") {
		t.Fatalf("expected scenario hints to expose missing effective adjustment, got %+v", report.ScenarioProvenanceHints)
	}
	if report.ForecastCoverageSummary == nil || report.ForecastCoverageSummary.PlannedInputCount == 0 {
		t.Fatalf("expected planned inputs to remain visible even for noop plans, got %+v", report.ForecastCoverageSummary)
	}
	if report.ForecastCoverageSummary.ScenarioAdjustedProjectionCount != 0 {
		t.Fatalf("expected noop pending plan not to claim adjusted projections, got %+v", report.ForecastCoverageSummary)
	}
	for _, projection := range report.Projections {
		if projection.ScenarioBasis == "CONTROL_PLAN_ADJUSTED" {
			t.Fatalf("expected noop pending plan to avoid adjusted projections, got %+v", report.Projections)
		}
	}
}

func TestBuildRSPForecastReportMarksUnresolvedCoverageUnavailable(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	setupInstrumentationInternalWorkspace(t, ctx, store, "ws-rsp-forecast-unresolved", "agent-a")

	report, err := store.BuildRSPForecastReport(ctx, RSPForecastReportFilter{
		WorkspaceID: "ws-rsp-forecast-unresolved",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("build unresolved rsp forecast report: %v", err)
	}
	if report.ForecastReadiness != "UNAVAILABLE" || report.Resolved {
		t.Fatalf("expected unresolved forecast readiness to stay unavailable, got %+v", report)
	}
	if report.ScenarioReadiness != "UNAVAILABLE" {
		t.Fatalf("expected unresolved forecast scenario readiness to stay unavailable, got %+v", report)
	}
	if report.ForecastBand != "UNRESOLVED" || len(report.Projections) != 0 {
		t.Fatalf("expected unresolved forecast report to stay projection-free, got %+v", report)
	}
	if report.ForecastCoverageSummary == nil || report.ForecastCoverageSummary.ProjectionCount != 0 || report.ForecastCoverageSummary.MissingInputCount != len(report.MissingInputs) {
		t.Fatalf("expected unresolved forecast report to keep coverage summary aligned, got %+v", report.ForecastCoverageSummary)
	}
	if !containsLocusString(report.ForecastProvenanceHints, "resolution:unresolved") {
		t.Fatalf("expected unresolved forecast provenance to say unresolved, got %+v", report.ForecastProvenanceHints)
	}
}

func TestSnapshotRSPForecastReportAppendsSyntheticEvent(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenarioWithAuthority(t, ctx, store, "rsp-forecast-snapshot")

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityForecastShadow,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable forecast snapshot for test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable forecast shadow capability: %v", err)
	}

	result, err := store.SnapshotRSPForecastReport(ctx, RSPForecastReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("snapshot rsp forecast report: %v", err)
	}
	if result.Event.EventType != "rsp.forecast_snapshot" || result.Event.EntityType != "rsp_forecast" {
		t.Fatalf("unexpected rsp forecast snapshot event %+v", result.Event)
	}
	if !isSyntheticOperationalEvent(result.Event) {
		t.Fatalf("expected rsp forecast snapshot to stay synthetic %+v", result.Event)
	}
	if result.Report.TimeAuthority.WorkspaceID != scenario.workspaceID || result.Report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp forecast snapshot report to expose workspace time authority, got %+v", result.Report.TimeAuthority)
	}
	if result.Report.GeneratedAt != result.Report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp forecast snapshot report generated_at %q to mirror authority reference_at %q", result.Report.GeneratedAt, result.Report.TimeAuthority.ReferenceAt)
	}
	if result.Report.SignalType != rspForecastSignalType || result.Report.ShadowPhase != rspForecastShadowPhase {
		t.Fatalf("unexpected rsp forecast snapshot report %+v", result.Report)
	}
	if result.Report.Calibration.SchemaVersion != rspCalibrationSchemaVersion ||
		result.Report.Calibration.CalibrationVersion != "forecast-shadow-s2-v2" {
		t.Fatalf("expected forecast snapshot report to carry versioned calibration contract, got %+v", result.Report.Calibration)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(result.Event.PayloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal forecast snapshot payload: %v", err)
	}
	calibration, ok := payload["calibration"].(map[string]any)
	if !ok {
		t.Fatalf("expected forecast snapshot payload to carry calibration contract, got %+v", payload)
	}
	if calibration["schema_version"] != rspCalibrationSchemaVersion ||
		calibration["calibration_version"] != "forecast-shadow-s2-v2" ||
		calibration["status"] != rspCalibrationStatusProvisional {
		t.Fatalf("expected forecast snapshot payload calibration contract, got %+v", calibration)
	}
	if result.Report.ForecastReadiness == "" || len(result.Report.ForecastProvenanceHints) == 0 {
		t.Fatalf("expected rsp forecast snapshot report to surface readiness/provenance, got %+v", result.Report)
	}
	if result.Report.ScenarioReadiness == "" {
		t.Fatalf("expected rsp forecast snapshot report to surface scenario readiness, got %+v", result.Report)
	}
	if result.Report.ForecastCoverageSummary == nil {
		t.Fatalf("expected rsp forecast snapshot report to surface coverage summary, got %+v", result.Report)
	}
	assertForecastTemporalContracts(t, result.Report.TimeAuthority, result.Report.Projections)
}

func assertForecastTemporalContracts(t *testing.T, authority WorkspaceTimeAuthority, projections []RSPForecastProjection) {
	t.Helper()
	if len(projections) == 0 {
		t.Fatalf("expected forecast projections, got %+v", projections)
	}
	for _, projection := range projections {
		for _, horizon := range projection.Forecasts {
			if horizon.TemporalContract == nil {
				t.Fatalf("expected forecast temporal contract on %+v", horizon)
			}
			contract := horizon.TemporalContract
			if contract.SchemaVersion != temporalContractSchemaVersion ||
				contract.Domain != "forecast" ||
				contract.HorizonKind != "projection_horizon" ||
				contract.Basis != temporalBasisControlEpoch ||
				contract.Mapping != temporalMappingExplicitPhi ||
				contract.WallClockComparable {
				t.Fatalf("expected epoch-relative forecast temporal contract, got %+v", contract)
			}
			if contract.CurrentEpoch != authority.CurrentEpoch || contract.TargetEpoch != authority.CurrentEpoch+horizon.Epochs {
				t.Fatalf("expected forecast temporal epochs to follow time authority, got %+v authority=%+v horizon=%+v", contract, authority, horizon)
			}
			if contract.ReferenceAt == "" {
				t.Fatalf("expected forecast temporal contract to carry reference_at, got %+v", contract)
			}
		}
	}
}
