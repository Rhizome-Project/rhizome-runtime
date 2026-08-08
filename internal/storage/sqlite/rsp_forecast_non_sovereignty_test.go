package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestRSPForecastSnapshotScenarioReadyStaysNonSovereign(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenarioWithAuthority(t, ctx, store, "rsp-forecast-non-sovereign")

	seedWarmForecastMetrics(t, ctx, store, scenario.workspaceID, scenario.agentID, scenario.sessionID, "rsp-forecast-non-sovereign")
	seedPendingForecastControls(t, ctx, store, scenario.workspaceID, scenario.clusterID, "rsp-forecast-non-sovereign")

	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityForecastShadow,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable forecast snapshot for non-sovereignty regression",
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
	if result.Report.ScenarioReadiness != "READY" {
		t.Fatalf("expected regression fixture to keep scenario readiness ready, got %+v", result.Report)
	}
	if result.Report.Calibration.Status != rspCalibrationStatusProvisional {
		t.Fatalf("expected forecast snapshot to remain provisional even when scenario-ready, got %+v", result.Report.Calibration)
	}
	if !containsString(result.Report.Calibration.Unsupported, "causal_intervention_counterfactuals") {
		t.Fatalf("expected forecast snapshot contract to keep counterfactual semantics unsupported, got %+v", result.Report.Calibration)
	}

	requested, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "control.command.requested",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list control.command.requested: %v", err)
	}
	if len(requested) != 0 {
		t.Fatalf("expected scenario-ready forecast snapshot to avoid control command requests, got %+v", requested)
	}
}

func TestRSPForecastLiveControlContextStaysScenarioUnavailable(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenarioWithAuthority(t, ctx, store, "rsp-forecast-live-context")

	seedWarmForecastMetrics(t, ctx, store, scenario.workspaceID, scenario.agentID, scenario.sessionID, "rsp-forecast-live-context")
	liveControls := ControlSuggestedControls{
		FanoutCap:      2,
		ReviewDepth:    2,
		ContextCap:     4,
		BridgeQuota:    2,
		MergeThreshold: 0.60,
		PriorityFocus:  "review",
	}
	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       scenario.workspaceID,
		ProtoClusterID:    scenario.clusterID,
		Epoch:             1,
		TTLSeconds:        600,
		ControlMode:       clusterControlModeStabilize,
		CandidateMode:     clusterControlModeStabilize,
		CandidateControls: liveControls,
		AdvisoryControls:  liveControls,
		EffectiveControls: liveControls,
		ResolvedFrom:      "forecast-live-context",
		MatchScore:        100,
		BasisSummary:      "live controls should stay observational for scenario semantics",
		GeneratedAt:       time.Now().UTC().Add(-1 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "tester",
	}); err != nil {
		t.Fatalf("persist live effective controls: %v", err)
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
		t.Fatalf("build live-context rsp forecast report: %v", err)
	}
	if report.ForecastReadiness != "READY" {
		t.Fatalf("expected warmed baseline coverage to stay ready, got %+v", report)
	}
	if report.ScenarioReadiness != "UNAVAILABLE" {
		t.Fatalf("expected live control context without pending plan to stay scenario-unavailable, got %+v", report)
	}
	if len(report.PlannedInputs) != 0 {
		t.Fatalf("expected live control context not to fabricate planned inputs, got %+v", report.PlannedInputs)
	}
	foundLiveControl := false
	for _, item := range report.InterventionInputs {
		if item.Source == "effective_controls_live" {
			foundLiveControl = true
			break
		}
	}
	if !foundLiveControl {
		t.Fatalf("expected live controls to remain visible as intervention context, got %+v", report.InterventionInputs)
	}
}

func seedWarmForecastMetrics(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID, sessionID, prefix string) {
	t.Helper()

	for i, staleHits := range []int{1, 0} {
		if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
			WorkspaceID:        workspaceID,
			AgentID:            agentID,
			SessionID:          sessionID,
			ReportScope:        "SESSION",
			ReportID:           prefix + "-memmet-" + string(rune('1'+i)),
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
}

func seedPendingForecastControls(t *testing.T, ctx context.Context, store *Store, workspaceID, protoClusterID, basis string) {
	t.Helper()

	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:    workspaceID,
		ProtoClusterID: protoClusterID,
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
		ResolvedFrom: basis,
		MatchScore:   100,
		BasisSummary: "pending control plan for forecast scenario coverage",
		GeneratedAt:  "2099-01-01T00:00:00Z",
		ActorID:      "tests",
	}); err != nil {
		t.Fatalf("persist pending effective controls: %v", err)
	}
}
