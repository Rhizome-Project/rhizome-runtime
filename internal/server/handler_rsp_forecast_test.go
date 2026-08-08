package server

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceRSPForecastReportAndSnapshot(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerLocusSidecarScenario(t, ctx, store, "rsp-forecast")
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	reportAny, rpcErr := callWorkspaceRSPForecastReportRaw(t, h, ctx, mustJSONRaw(workspaceRSPForecastReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPForecastReport rpc error: %+v", rpcErr)
	}
	report := reportAny.(sqlite.RSPForecastReport)
	if !report.Resolved || report.SignalType != "LOAD_FORECAST" {
		t.Fatalf("unexpected rsp forecast report %+v", report)
	}
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp forecast rpc to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	assertHandlerTimeAuthorityTemporalContract(t, report.TimeAuthority)
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp forecast rpc generated_at %q to mirror authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if report.ShadowPhase != "S2" || len(report.Projections) == 0 {
		t.Fatalf("expected rsp forecast rpc to expose bounded forecast projections, got %+v", report)
	}
	if !report.ShadowMode || report.Calibration.Status != "PROVISIONAL" {
		t.Fatalf("expected rsp forecast rpc to stay shadow/provisional, got %+v", report)
	}
	for _, needle := range []string{"authority", "eligible", "actuat"} {
		if strings.Contains(strings.ToLower(report.Summary), needle) || strings.Contains(strings.ToLower(report.Calibration.Basis), needle) {
			t.Fatalf("expected rsp forecast rpc payload to stay inspectability-only, got summary=%q calibration=%+v", report.Summary, report.Calibration)
		}
	}
	if report.ForecastReadiness == "" || len(report.ForecastProvenanceHints) == 0 {
		t.Fatalf("expected rsp forecast rpc to expose readiness/provenance hints, got %+v", report)
	}
	if report.ForecastCoverageSummary == nil || report.ForecastCoverageSummary.ProjectionCount != len(report.Projections) {
		t.Fatalf("expected rsp forecast rpc to expose coverage summary aligned with projections, got %+v", report.ForecastCoverageSummary)
	}
	assertHandlerForecastTemporalContracts(t, report.TimeAuthority, report.Projections)

	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  "rsp.forecast.shadow",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable forecast snapshot for handler test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable forecast shadow capability: %v", err)
	}

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	snapshotAny, rpcErr := callWorkspaceRSPForecastSnapshotRaw(t, h, ctx, mustJSONRaw(workspaceRSPForecastReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPForecastSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload := snapshotAny.(map[string]any)
	snapshotReport := snapshotPayload["report"].(sqlite.RSPForecastReport)
	if snapshotReport.TimeAuthority.WorkspaceID != scenario.workspaceID || snapshotReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp forecast snapshot rpc to expose workspace time authority, got %+v", snapshotReport.TimeAuthority)
	}
	assertHandlerTimeAuthorityTemporalContract(t, snapshotReport.TimeAuthority)
	if snapshotReport.GeneratedAt != snapshotReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp forecast snapshot rpc generated_at %q to mirror authority reference_at %q", snapshotReport.GeneratedAt, snapshotReport.TimeAuthority.ReferenceAt)
	}
	if snapshotReport.ShadowPhase != "S2" || len(snapshotReport.Projections) == 0 {
		t.Fatalf("expected rsp forecast snapshot rpc to keep bounded forecast projections, got %+v", snapshotReport)
	}
	if !snapshotReport.ShadowMode || snapshotReport.Calibration.Status != "PROVISIONAL" {
		t.Fatalf("expected rsp forecast snapshot rpc to stay shadow/provisional, got %+v", snapshotReport)
	}
	for _, needle := range []string{"authority", "eligible", "actuat"} {
		if strings.Contains(strings.ToLower(snapshotReport.Summary), needle) || strings.Contains(strings.ToLower(snapshotReport.Calibration.Basis), needle) {
			t.Fatalf("expected rsp forecast snapshot rpc payload to stay inspectability-only, got summary=%q calibration=%+v", snapshotReport.Summary, snapshotReport.Calibration)
		}
	}
	if snapshotReport.ForecastReadiness != report.ForecastReadiness || len(snapshotReport.ForecastProvenanceHints) == 0 {
		t.Fatalf("expected rsp forecast snapshot rpc to preserve readiness/provenance hints, got %+v", snapshotReport)
	}
	if snapshotReport.ForecastCoverageSummary == nil || snapshotReport.ForecastCoverageSummary.ProjectionCount != len(snapshotReport.Projections) {
		t.Fatalf("expected rsp forecast snapshot rpc to preserve coverage summary, got %+v", snapshotReport.ForecastCoverageSummary)
	}
	assertHandlerForecastTemporalContracts(t, snapshotReport.TimeAuthority, snapshotReport.Projections)
	event := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if event.EventType != "rsp.forecast_snapshot" || event.EntityType != "rsp_forecast" {
		t.Fatalf("unexpected rsp forecast snapshot event %+v", event)
	}
	expectMemoryInvalidationEvent(t, ch, "rsp.forecast_snapshot")
}

func TestWorkspaceRSPForecastReportRequiresWorkspaceID(t *testing.T) {
	t.Parallel()

	h := NewHandler(newServerTestStore(t))
	if _, rpcErr := h.workspaceRSPForecastReport(context.Background(), mustJSONRaw(workspaceRSPForecastReportParams{
		AgentID: "agent-a",
	})); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected missing workspace_id invalid params error, got %+v", rpcErr)
	}
}

func assertHandlerForecastTemporalContracts(t *testing.T, authority sqlite.WorkspaceTimeAuthority, projections []sqlite.RSPForecastProjection) {
	t.Helper()
	if len(projections) == 0 {
		t.Fatalf("expected forecast projections, got %+v", projections)
	}
	for _, projection := range projections {
		for _, horizon := range projection.Forecasts {
			if horizon.TemporalContract == nil {
				t.Fatalf("expected handler forecast temporal contract on %+v", horizon)
			}
			contract := horizon.TemporalContract
			if contract.Domain != "forecast" ||
				contract.Basis != "control_epoch" ||
				contract.Mapping != "explicit_phi_required" ||
				contract.WallClockComparable {
				t.Fatalf("expected handler epoch-relative forecast temporal contract, got %+v", contract)
			}
			if contract.CurrentEpoch != authority.CurrentEpoch || contract.TargetEpoch != authority.CurrentEpoch+horizon.Epochs {
				t.Fatalf("expected handler forecast epochs to follow time authority, got %+v authority=%+v horizon=%+v", contract, authority, horizon)
			}
		}
	}
}

func assertHandlerTimeAuthorityTemporalContract(t *testing.T, authority sqlite.WorkspaceTimeAuthority) {
	t.Helper()
	if authority.TemporalContract == nil {
		t.Fatalf("expected handler time authority temporal contract, got %+v", authority)
	}
	contract := authority.TemporalContract
	if contract.Domain != "control_epoch" ||
		contract.HorizonKind != "current_epoch" ||
		contract.Basis != "control_epoch" ||
		contract.Mapping != "explicit_phi_required" ||
		contract.WallClockComparable ||
		contract.State != "LIVE" {
		t.Fatalf("unexpected handler time authority temporal contract %+v", contract)
	}
}
