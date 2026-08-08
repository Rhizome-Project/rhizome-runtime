package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	rspForecastSignalType    = "LOAD_FORECAST"
	rspForecastShadowPhase   = "S2"
	rspForecastAlertCutoff   = 0.70
	rspForecastDefaultH3     = 3
	rspForecastDefaultH5     = 5
	rspForecastProbabilityEP = 0.001
)

type RSPForecastReportFilter struct {
	WorkspaceID    string
	ProtoClusterID string
	AgentID        string
	TaskID         string
	SessionID      string
	DocKeys        []string
	ArtifactRefs   []string
	FrontierLimit  int
}

type RSPForecastHorizon struct {
	Epochs                     int                      `json:"epochs"`
	Mean                       float64                  `json:"mean"`
	Median                     float64                  `json:"median"`
	P90                        float64                  `json:"p90"`
	ProbabilityExceedThreshold float64                  `json:"probability_exceed_threshold"`
	Uncertainty                float64                  `json:"uncertainty"`
	TemporalContract           *TemporalHorizonContract `json:"temporal_contract,omitempty"`
}

type RSPForecastProjection struct {
	Variable            string                          `json:"variable"`
	Basis               string                          `json:"basis"`
	Model               string                          `json:"model"`
	Unit                string                          `json:"unit,omitempty"`
	CurrentValue        float64                         `json:"current_value"`
	Threshold           float64                         `json:"threshold"`
	Dispersion          float64                         `json:"dispersion"`
	EvidenceRefs        []string                        `json:"evidence_refs,omitempty"`
	ScenarioBasis       string                          `json:"scenario_basis,omitempty"`
	ScenarioAdjustments []RSPForecastScenarioAdjustment `json:"scenario_adjustments,omitempty"`
	Forecasts           []RSPForecastHorizon            `json:"forecasts,omitempty"`
	Summary             string                          `json:"summary,omitempty"`
}

type RSPForecastCoverageSummary struct {
	ProjectionCount                 int            `json:"projection_count"`
	AlertProjectionCount            int            `json:"alert_projection_count"`
	HistoryBackedProjectionCount    int            `json:"history_backed_projection_count"`
	EvidenceBackedProjectionCount   int            `json:"evidence_backed_projection_count"`
	ScenarioAdjustedProjectionCount int            `json:"scenario_adjusted_projection_count"`
	MissingInputCount               int            `json:"missing_input_count"`
	EvidenceRefCount                int            `json:"evidence_ref_count"`
	InterventionInputCount          int            `json:"intervention_input_count"`
	PlannedInputCount               int            `json:"planned_input_count"`
	BasisCount                      map[string]int `json:"basis_count,omitempty"`
	ModelCount                      map[string]int `json:"model_count,omitempty"`
}

type RSPForecastScenarioInput struct {
	Source      string                    `json:"source"`
	ScopeSource string                    `json:"scope_source,omitempty"`
	Mode        string                    `json:"mode,omitempty"`
	Controls    *ControlSuggestedControls `json:"controls,omitempty"`
	Planned     bool                      `json:"planned,omitempty"`
	Live        bool                      `json:"live,omitempty"`
	Pending     bool                      `json:"pending,omitempty"`
	Summary     string                    `json:"summary,omitempty"`
}

type RSPForecastScenarioAdjustment struct {
	Source  string  `json:"source"`
	Delta   float64 `json:"delta"`
	Summary string  `json:"summary,omitempty"`
}

type RSPForecastReport struct {
	WorkspaceID             string                      `json:"workspace_id"`
	TimeAuthority           WorkspaceTimeAuthority      `json:"time_authority"`
	ProtoClusterID          string                      `json:"proto_cluster_id,omitempty"`
	AgentID                 string                      `json:"agent_id,omitempty"`
	SessionID               string                      `json:"session_id,omitempty"`
	TaskID                  string                      `json:"task_id,omitempty"`
	Resolved                bool                        `json:"resolved"`
	ResolvedFrom            string                      `json:"resolved_from,omitempty"`
	MatchScore              int                         `json:"match_score,omitempty"`
	SignalType              string                      `json:"signal_type"`
	ShadowMode              bool                        `json:"shadow_mode"`
	ShadowPhase             string                      `json:"shadow_phase"`
	Calibration             RSPCalibrationContract      `json:"calibration"`
	BasisState              string                      `json:"basis_state"`
	MissingInputs           []string                    `json:"missing_inputs,omitempty"`
	HiddenState             string                      `json:"hidden_state,omitempty"`
	StateConfidence         float64                     `json:"state_confidence"`
	RiskScore               float64                     `json:"risk_score"`
	RiskBand                string                      `json:"risk_band"`
	AnomalyScore            float64                     `json:"anomaly_score"`
	PersistenceScore        float64                     `json:"persistence_score"`
	PersistenceEpochs       int                         `json:"persistence_epochs"`
	PressureScore           int                         `json:"pressure_score"`
	AttentionBand           string                      `json:"attention_band,omitempty"`
	CoherenceBand           string                      `json:"coherence_band,omitempty"`
	SupportedVariables      int                         `json:"supported_variables"`
	AlertVariables          []string                    `json:"alert_variables,omitempty"`
	MaxAlertProbability     float64                     `json:"max_alert_probability"`
	ForecastBand            string                      `json:"forecast_band"`
	ForecastReadiness       string                      `json:"forecast_readiness"`
	ForecastProvenanceHints []string                    `json:"forecast_provenance_hints,omitempty"`
	ScenarioBasis           string                      `json:"scenario_basis,omitempty"`
	ScenarioReadiness       string                      `json:"scenario_readiness,omitempty"`
	ScenarioProvenanceHints []string                    `json:"scenario_provenance_hints,omitempty"`
	InterventionInputs      []RSPForecastScenarioInput  `json:"intervention_inputs,omitempty"`
	PlannedInputs           []RSPForecastScenarioInput  `json:"planned_inputs,omitempty"`
	ForecastCoverageSummary *RSPForecastCoverageSummary `json:"forecast_coverage_summary,omitempty"`
	Projections             []RSPForecastProjection     `json:"projections,omitempty"`
	EvidenceRefs            []string                    `json:"evidence_refs,omitempty"`
	GeneratedAt             string                      `json:"generated_at"`
	Summary                 string                      `json:"summary"`
}

type RSPForecastSnapshotResult struct {
	Report RSPForecastReport  `json:"report"`
	Event  RuntimeEventRecord `json:"event"`
}

type rspForecastVariableInput struct {
	name          string
	basis         string
	unit          string
	countLike     bool
	maxValue      float64
	currentValue  float64
	previousValue *float64
	threshold     float64
	evidenceRefs  []string
}

type rspForecastPacketResult struct {
	report         *RSPForecastReport
	metricsHistory []MemoryMetricsReportRecord
}

type rspForecastSharedLatencyInput struct {
	currentValue  float64
	previousValue *float64
	evidenceRefs  []string
}

type rspForecastScenarioContext struct {
	basis              string
	interventionInputs []RSPForecastScenarioInput
	plannedInputs      []RSPForecastScenarioInput
	pendingControls    *EffectiveControlsScopeResolution
}

func (s *Store) BuildRSPForecastReport(ctx context.Context, filter RSPForecastReportFilter) (RSPForecastReport, error) {
	filter = normalizeRSPForecastReportFilter(filter)
	if filter.WorkspaceID == "" {
		return RSPForecastReport{}, errors.New("workspace_id is required")
	}
	bundle, err := s.BuildInstrumentationLocusBundle(ctx, InstrumentationLocusFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		AgentID:        filter.AgentID,
		TaskID:         filter.TaskID,
		SessionID:      filter.SessionID,
		DocKeys:        append([]string(nil), filter.DocKeys...),
		ArtifactRefs:   append([]string(nil), filter.ArtifactRefs...),
		FrontierLimit:  filter.FrontierLimit,
	})
	if err != nil {
		return RSPForecastReport{}, err
	}
	state := buildRSPStateReportFromBundle(s, ctx, RSPStateReportFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		AgentID:        filter.AgentID,
		TaskID:         filter.TaskID,
		SessionID:      filter.SessionID,
		DocKeys:        append([]string(nil), filter.DocKeys...),
		ArtifactRefs:   append([]string(nil), filter.ArtifactRefs...),
		FrontierLimit:  filter.FrontierLimit,
	}, bundle)
	metricsHistory, err := s.listRSPForecastMetricsHistory(ctx, filter, bundle)
	if err != nil {
		return RSPForecastReport{}, err
	}
	sharedLatency, err := s.listRSPForecastSharedLatencyHistory(ctx, filter.WorkspaceID)
	if err != nil {
		return RSPForecastReport{}, err
	}
	scenario := s.buildRSPForecastScenarioContext(ctx, filter, bundle, state)
	return buildRSPForecastReportFromBundle(filter, bundle, state, metricsHistory, sharedLatency, scenario), nil
}

func (s *Store) SnapshotRSPForecastReport(ctx context.Context, filter RSPForecastReportFilter) (RSPForecastSnapshotResult, error) {
	if err := s.ensureRSPCapabilityEnabled(ctx, filter.WorkspaceID, rspCapabilityForecastShadow); err != nil {
		return RSPForecastSnapshotResult{}, err
	}
	report, err := s.BuildRSPForecastReport(ctx, filter)
	if err != nil {
		return RSPForecastSnapshotResult{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, report.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RSPForecastSnapshotResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RSPForecastSnapshotResult{}, fmt.Errorf("begin rsp forecast snapshot tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		event, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: report.WorkspaceID,
			EventType:   "rsp.forecast_snapshot",
			EntityType:  "rsp_forecast",
			EntityID:    rspForecastSnapshotEntityID(report),
			ActorType:   "system",
			ActorID:     "rsp_forecast",
			PayloadJSON: mustJSON(rspForecastSnapshotPayload(report)),
			CreatedAt:   now,
		})
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RSPForecastSnapshotResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RSPForecastSnapshotResult{}, fmt.Errorf("commit rsp forecast snapshot tx: %w", err)
	}
	return RSPForecastSnapshotResult{Report: report, Event: event}, nil
}

func buildRSPForecastReportFromBundle(
	filter RSPForecastReportFilter,
	bundle InstrumentationLocusBundle,
	state RSPStateReport,
	metricsHistory []MemoryMetricsReportRecord,
	sharedLatency rspForecastSharedLatencyInput,
	scenario rspForecastScenarioContext,
) RSPForecastReport {
	report := RSPForecastReport{
		WorkspaceID:        filter.WorkspaceID,
		TimeAuthority:      bundle.TimeAuthority,
		ProtoClusterID:     strings.TrimSpace(bundle.ProtoClusterID),
		AgentID:            rspStateResolvedAgentID(RSPStateReportFilter{AgentID: filter.AgentID}, bundle),
		SessionID:          strings.TrimSpace(firstNonEmpty(filter.SessionID, bundleMemoryCoherenceSessionID(bundle.MemoryCoherence))),
		TaskID:             rspStateResolvedTaskID(RSPStateReportFilter{TaskID: filter.TaskID}, bundle),
		Resolved:           bundle.Resolved,
		ResolvedFrom:       strings.TrimSpace(bundle.ResolvedFrom),
		MatchScore:         bundle.MatchScore,
		SignalType:         rspForecastSignalType,
		ShadowMode:         true,
		ShadowPhase:        rspForecastShadowPhase,
		Calibration:        rspForecastCalibrationContract(),
		BasisState:         state.BasisState,
		MissingInputs:      append([]string(nil), state.MissingInputs...),
		HiddenState:        state.HiddenState,
		StateConfidence:    state.StateConfidence,
		RiskScore:          state.RiskScore,
		RiskBand:           state.RiskBand,
		AnomalyScore:       state.AnomalyScore,
		PersistenceScore:   state.PersistenceScore,
		PersistenceEpochs:  state.PersistenceEpochs,
		PressureScore:      state.PressureScore,
		AttentionBand:      state.AttentionBand,
		CoherenceBand:      state.CoherenceBand,
		ScenarioBasis:      strings.TrimSpace(scenario.basis),
		InterventionInputs: append([]RSPForecastScenarioInput(nil), scenario.interventionInputs...),
		PlannedInputs:      append([]RSPForecastScenarioInput(nil), scenario.plannedInputs...),
		EvidenceRefs:       append([]string(nil), state.EvidenceRefs...),
		GeneratedAt:        generatedAtFromWorkspaceTimeAuthority(bundle.TimeAuthority),
	}
	if len(metricsHistory) < 2 {
		report.MissingInputs = uniqueTrimmedLocusStrings(append(report.MissingInputs, "metrics_history"))
	}

	if !report.Resolved {
		report.ForecastBand = "UNRESOLVED"
		report.ForecastReadiness, report.ForecastProvenanceHints = rspForecastReadiness(report)
		report.ScenarioReadiness, report.ScenarioProvenanceHints = rspForecastScenarioReadiness(report)
		report.ForecastCoverageSummary = rspForecastCoverageSummary(report, nil)
		report.Summary = fmt.Sprintf("rsp forecast shadow report for %s unresolved: locus could not be attached", report.WorkspaceID)
		return report
	}

	var previousMetrics *MemoryMetricsReportRecord
	if len(metricsHistory) > 1 {
		previousMetrics = &metricsHistory[1]
	}
	metricsRefs := rspForecastMetricsEvidenceRefs(metricsHistory)
	if len(metricsRefs) > 0 {
		report.EvidenceRefs = uniqueTrimmedLocusStrings(append(report.EvidenceRefs, metricsRefs...))
	}
	if len(sharedLatency.evidenceRefs) > 0 {
		report.EvidenceRefs = uniqueTrimmedLocusStrings(append(report.EvidenceRefs, sharedLatency.evidenceRefs...))
	}

	variables := make([]rspForecastVariableInput, 0, 8)
	if current, ok := rspForecastOpenTensions(bundle); ok {
		variables = append(variables, rspForecastVariableInput{
			name:         "OPEN_TENSIONS",
			basis:        "count",
			unit:         "count",
			countLike:    true,
			maxValue:     0,
			currentValue: current,
			threshold:    2.5,
			evidenceRefs: append([]string(nil), report.EvidenceRefs...),
		})
	} else {
		report.MissingInputs = uniqueTrimmedLocusStrings(append(report.MissingInputs, "open_tensions"))
	}
	if current, ok := rspForecastBlockerMass(bundle); ok {
		variables = append(variables, rspForecastVariableInput{
			name:         "BLOCKER_MASS",
			basis:        "ratio",
			unit:         "ratio",
			maxValue:     1,
			currentValue: current,
			threshold:    0.25,
			evidenceRefs: append([]string(nil), report.EvidenceRefs...),
		})
	} else {
		report.MissingInputs = uniqueTrimmedLocusStrings(append(report.MissingInputs, "blocker_mass"))
	}
	if current, ok := rspForecastVerifierQueueDepth(bundle); ok {
		variables = append(variables, rspForecastVariableInput{
			name:         "VERIFIER_QUEUE_DEPTH",
			basis:        "count",
			unit:         "count",
			countLike:    true,
			maxValue:     0,
			currentValue: current,
			threshold:    1.5,
			evidenceRefs: append([]string(nil), report.EvidenceRefs...),
		})
	} else {
		report.MissingInputs = uniqueTrimmedLocusStrings(append(report.MissingInputs, "verifier_queue_depth"))
	}
	if current, ok := rspForecastFanoutPressure(bundle); ok {
		variables = append(variables, rspForecastVariableInput{
			name:         "FANOUT_PRESSURE",
			basis:        "score",
			unit:         "score",
			maxValue:     100,
			currentValue: current,
			threshold:    65,
			evidenceRefs: append([]string(nil), report.EvidenceRefs...),
		})
	} else {
		report.MissingInputs = uniqueTrimmedLocusStrings(append(report.MissingInputs, "fanout_pressure"))
	}
	if sharedLatency.currentValue > 0 {
		variables = append(variables, rspForecastVariableInput{
			name:          "SHARED_LATENCY",
			basis:         "latency_ms",
			unit:          "ms",
			maxValue:      0,
			currentValue:  sharedLatency.currentValue,
			previousValue: sharedLatency.previousValue,
			threshold:     250,
			evidenceRefs:  append([]string(nil), sharedLatency.evidenceRefs...),
		})
	} else {
		report.MissingInputs = uniqueTrimmedLocusStrings(append(report.MissingInputs, "shared_latency"))
	}
	if current, previous, ok := rspForecastStaleHitRate(bundle, metricsHistory, previousMetrics); ok {
		variables = append(variables, rspForecastVariableInput{
			name:          "STALE_HIT_RATE",
			basis:         "ratio",
			unit:          "ratio",
			maxValue:      1,
			currentValue:  current,
			previousValue: previous,
			threshold:     0.20,
			evidenceRefs:  append([]string(nil), metricsRefs...),
		})
	} else {
		report.MissingInputs = uniqueTrimmedLocusStrings(append(report.MissingInputs, "stale_hit_rate"))
	}
	if current, previous, ok := rspForecastOffloadRatio(bundle, metricsHistory, previousMetrics); ok {
		variables = append(variables, rspForecastVariableInput{
			name:          "OFFLOAD_RATIO",
			basis:         "ratio",
			unit:          "ratio",
			maxValue:      1,
			currentValue:  current,
			previousValue: previous,
			threshold:     0.70,
			evidenceRefs:  append([]string(nil), metricsRefs...),
		})
	} else {
		report.MissingInputs = uniqueTrimmedLocusStrings(append(report.MissingInputs, "offload_ratio"))
	}
	if current, previous, ok := rspForecastPollutionRate(metricsHistory, previousMetrics); ok {
		variables = append(variables, rspForecastVariableInput{
			name:          "POLLUTION_RATE",
			basis:         "ratio",
			unit:          "ratio",
			maxValue:      1,
			currentValue:  current,
			previousValue: previous,
			threshold:     0.20,
			evidenceRefs:  append([]string(nil), metricsRefs...),
		})
	} else {
		report.MissingInputs = uniqueTrimmedLocusStrings(append(report.MissingInputs, "pollution_rate"))
	}

	projections := make([]RSPForecastProjection, 0, len(variables))
	historyBackedVariables := make(map[string]bool, len(variables))
	for _, variable := range variables {
		projection := rspForecastBuildProjection(variable, state, report.BasisState, scenario, report.TimeAuthority)
		if len(projection.Forecasts) == 0 {
			continue
		}
		projections = append(projections, projection)
		report.SupportedVariables++
		if variable.previousValue != nil {
			historyBackedVariables[projection.Variable] = true
		}
		for _, horizon := range projection.Forecasts {
			if horizon.ProbabilityExceedThreshold >= rspForecastAlertCutoff {
				report.AlertVariables = append(report.AlertVariables, projection.Variable)
				break
			}
		}
		report.MaxAlertProbability = maxFloat(report.MaxAlertProbability, rspForecastMaxProbability(projection.Forecasts))
	}
	report.Projections = projections
	report.AlertVariables = uniqueTrimmedLocusStrings(report.AlertVariables)
	report.ForecastBand = rspForecastBand(report.MaxAlertProbability, state)
	report.ForecastReadiness, report.ForecastProvenanceHints = rspForecastReadiness(report)
	report.ScenarioReadiness, report.ScenarioProvenanceHints = rspForecastScenarioReadiness(report)
	report.ForecastCoverageSummary = rspForecastCoverageSummary(report, historyBackedVariables)
	report.Summary = fmt.Sprintf(
		"rsp forecast shadow report for %s/%s: %s %d alert vars max_p=%.2f basis=%s",
		firstNonEmpty(report.WorkspaceID, "workspace"),
		firstNonEmpty(report.AgentID, report.ProtoClusterID, "scope"),
		strings.ToLower(report.ForecastBand),
		len(report.AlertVariables),
		report.MaxAlertProbability,
		strings.ToLower(report.BasisState),
	)
	if report.ScenarioReadiness != "" && report.ScenarioReadiness != "UNAVAILABLE" {
		report.Summary += fmt.Sprintf(" scenario=%s", strings.ToLower(report.ScenarioReadiness))
	}
	return report
}

func (s *Store) buildRSPForecastForPacket(ctx context.Context, packetCtx memoryPacketBuildContext) (*rspForecastPacketResult, error) {
	if strings.TrimSpace(packetCtx.agentID) == "" {
		return nil, nil
	}
	filter := RSPForecastReportFilter{
		WorkspaceID:    packetCtx.workspaceID,
		ProtoClusterID: packetCtx.locus.ProtoClusterID,
		AgentID:        packetCtx.agentID,
		TaskID:         packetCtx.taskID,
		SessionID:      packetCtx.sessionID,
		DocKeys:        append([]string(nil), packetCtx.docKeys...),
		ArtifactRefs:   append([]string(nil), packetCtx.artifactRefs...),
		FrontierLimit:  packetCtx.filter.Budget.Lanes[MemoryRetrievalLaneCluster].ItemLimit,
	}
	state := buildRSPStateReportFromBundle(s, ctx, RSPStateReportFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		AgentID:        filter.AgentID,
		TaskID:         filter.TaskID,
		SessionID:      filter.SessionID,
		DocKeys:        append([]string(nil), filter.DocKeys...),
		ArtifactRefs:   append([]string(nil), filter.ArtifactRefs...),
		FrontierLimit:  filter.FrontierLimit,
	}, packetCtx.locus)
	metricsHistory, err := s.listRSPForecastMetricsHistory(ctx, filter, packetCtx.locus)
	if err != nil {
		return nil, err
	}
	sharedLatency, err := s.listRSPForecastSharedLatencyHistory(ctx, filter.WorkspaceID)
	if err != nil {
		return nil, err
	}
	scenario := s.buildRSPForecastScenarioContext(ctx, filter, packetCtx.locus, state)
	report := buildRSPForecastReportFromBundle(filter, packetCtx.locus, state, metricsHistory, sharedLatency, scenario)
	return &rspForecastPacketResult{report: &report, metricsHistory: metricsHistory}, nil
}

func (s *Store) listRSPForecastMetricsHistory(ctx context.Context, filter RSPForecastReportFilter, bundle InstrumentationLocusBundle) ([]MemoryMetricsReportRecord, error) {
	agentID := rspStateResolvedAgentID(RSPStateReportFilter{AgentID: filter.AgentID}, bundle)
	if agentID == "" {
		return nil, nil
	}
	sessionID := strings.TrimSpace(firstNonEmpty(filter.SessionID, bundleMemoryCoherenceSessionID(bundle.MemoryCoherence)))
	reportScope := "AGENT"
	if sessionID != "" {
		reportScope = "SESSION"
	}
	items, err := s.ListMemoryMetricsReports(ctx, MemoryMetricsReportFilter{
		WorkspaceID: filter.WorkspaceID,
		AgentID:     agentID,
		SessionID:   sessionID,
		ReportScope: reportScope,
		Limit:       2,
	})
	if err != nil {
		return nil, err
	}
	if len(items) == 0 && reportScope == "SESSION" {
		items, err = s.ListMemoryMetricsReports(ctx, MemoryMetricsReportFilter{
			WorkspaceID: filter.WorkspaceID,
			AgentID:     agentID,
			ReportScope: "AGENT",
			Limit:       2,
		})
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}

func (s *Store) listRSPForecastSharedLatencyHistory(ctx context.Context, workspaceID string) (rspForecastSharedLatencyInput, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return rspForecastSharedLatencyInput{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT latency_ms FROM rpc_access_log WHERE workspace_id = ? AND status = ? ORDER BY created_at DESC, id DESC LIMIT 6`, workspaceID, "ok")
	if err != nil {
		return rspForecastSharedLatencyInput{}, err
	}
	defer rows.Close()

	samples := make([]float64, 0, 6)
	for rows.Next() {
		var latency int64
		if err := rows.Scan(&latency); err != nil {
			return rspForecastSharedLatencyInput{}, err
		}
		samples = append(samples, float64(latency))
	}
	if err := rows.Err(); err != nil {
		return rspForecastSharedLatencyInput{}, err
	}
	if len(samples) == 0 {
		return rspForecastSharedLatencyInput{}, nil
	}

	currentWindow := rspForecastWindowAverage(samples[:minInt(len(samples), 3)])
	var previous *float64
	if len(samples) > 3 {
		value := rspForecastWindowAverage(samples[3:minInt(len(samples), 6)])
		previous = &value
	}
	return rspForecastSharedLatencyInput{
		currentValue:  currentWindow,
		previousValue: previous,
		evidenceRefs:  []string{"rpc_access_log:recent"},
	}, nil
}

func rspForecastWindowAverage(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func (s *Store) buildRSPForecastScenarioContext(
	ctx context.Context,
	filter RSPForecastReportFilter,
	bundle InstrumentationLocusBundle,
	state RSPStateReport,
) rspForecastScenarioContext {
	ctxInfo := rspForecastScenarioContext{}
	currentMode := normalizeClusterControlMode(state.ControlMode)

	if currentMode != "" && currentMode != clusterControlModeSteady {
		ctxInfo.interventionInputs = append(ctxInfo.interventionInputs, RSPForecastScenarioInput{
			Source:  "control_mode_effective",
			Mode:    currentMode,
			Live:    true,
			Summary: fmt.Sprintf("active control mode %s is part of current shadow-state context", currentMode),
		})
	}
	if !bundle.Resolved {
		ctxInfo.basis = rspForecastScenarioBasis(ctxInfo)
		return ctxInfo
	}
	resolution, err := s.ResolveEffectiveControlsScope(ctx, filter.WorkspaceID, bundle.ProtoClusterID, bundle.GeneratedAt)
	if err != nil || !resolution.Found {
		ctxInfo.basis = rspForecastScenarioBasis(ctxInfo)
		return ctxInfo
	}
	record := resolution.Record
	controlsCopy := record.EffectiveControls
	input := RSPForecastScenarioInput{
		ScopeSource: resolution.ScopeSource,
		Mode:        normalizeClusterControlMode(firstNonEmpty(record.CandidateMode, record.ControlMode)),
		Controls:    &controlsCopy,
		Live:        resolution.Live,
		Pending:     record.Pending,
		Planned:     !resolution.Live,
	}
	if resolution.Live {
		input.Source = "effective_controls_live"
		input.Summary = fmt.Sprintf("live effective controls (%s) are part of current shadow-state context", firstNonEmpty(strings.TrimSpace(resolution.ScopeSource), "unknown_scope"))
		ctxInfo.interventionInputs = append(ctxInfo.interventionInputs, input)
	} else {
		input.Source = "effective_controls_pending"
		input.Summary = fmt.Sprintf("pending effective controls (%s) condition plan-side forecast deltas", firstNonEmpty(strings.TrimSpace(resolution.ScopeSource), "unknown_scope"))
		ctxInfo.plannedInputs = append(ctxInfo.plannedInputs, input)
		ctxInfo.pendingControls = &resolution
	}
	ctxInfo.basis = rspForecastScenarioBasis(ctxInfo)
	return ctxInfo
}

func rspForecastScenarioBasis(ctx rspForecastScenarioContext) string {
	if len(ctx.plannedInputs) == 0 {
		return "NONE"
	}
	hasPendingControls := false
	for _, item := range ctx.plannedInputs {
		switch strings.TrimSpace(item.Source) {
		case "effective_controls_pending":
			hasPendingControls = true
		}
	}
	switch {
	case hasPendingControls:
		return "PENDING_EFFECTIVE_CONTROLS"
	default:
		return "NONE"
	}
}

func rspForecastBuildProjection(
	variable rspForecastVariableInput,
	state RSPStateReport,
	basisState string,
	scenario rspForecastScenarioContext,
	authority WorkspaceTimeAuthority,
) RSPForecastProjection {
	dispersion := rspForecastDispersion(variable)
	model := rspForecastModel(variable.countLike, dispersion)
	growth := rspForecastEscalationFactor(state)
	historySlope := rspForecastHistorySlope(variable)
	uncertainty := rspForecastBaseUncertainty(state, basisState, variable.previousValue != nil)
	adjustments := rspForecastScenarioAdjustments(variable, scenario)
	scenarioBasis := "BASELINE_ONLY"
	if len(adjustments) > 0 {
		scenarioBasis = "CONTROL_PLAN_ADJUSTED"
	} else if len(scenario.plannedInputs) > 0 {
		scenarioBasis = "CONTROL_PLAN_OBSERVED_ONLY"
	}
	forecasts := make([]RSPForecastHorizon, 0, 2)
	for _, horizonEpochs := range []int{rspForecastDefaultH3, rspForecastDefaultH5} {
		horizon := rspForecastProjectHorizon(variable, model, dispersion, growth, historySlope, uncertainty, adjustments, authority, horizonEpochs)
		forecasts = append(forecasts, horizon)
	}
	summary := fmt.Sprintf(
		"%s %s forecast max_p=%.2f threshold=%.2f",
		strings.ToLower(variable.name),
		strings.ToLower(model),
		rspForecastMaxProbability(forecasts),
		variable.threshold,
	)
	return RSPForecastProjection{
		Variable:            variable.name,
		Basis:               variable.basis,
		Model:               model,
		Unit:                variable.unit,
		CurrentValue:        variable.currentValue,
		Threshold:           variable.threshold,
		Dispersion:          rspStateClamp(0, dispersion, 8),
		EvidenceRefs:        uniqueTrimmedLocusStrings(variable.evidenceRefs),
		ScenarioBasis:       scenarioBasis,
		ScenarioAdjustments: append([]RSPForecastScenarioAdjustment(nil), adjustments...),
		Forecasts:           forecasts,
		Summary:             summary,
	}
}

func rspForecastScenarioAdjustments(variable rspForecastVariableInput, scenario rspForecastScenarioContext) []RSPForecastScenarioAdjustment {
	adjustments := make([]RSPForecastScenarioAdjustment, 0, 4)
	for _, item := range scenario.plannedInputs {
		switch strings.TrimSpace(item.Source) {
		case "effective_controls_pending":
			if scenario.pendingControls == nil {
				continue
			}
			adjustments = append(adjustments, rspForecastPendingControlsAdjustments(variable, scenario.pendingControls.Record)...)
		}
	}
	if len(adjustments) == 0 {
		return nil
	}
	return adjustments
}

func rspForecastPendingControlsAdjustments(variable rspForecastVariableInput, record EffectiveControlsRecord) []RSPForecastScenarioAdjustment {
	deltas := make([]RSPForecastScenarioAdjustment, 0, 4)
	switch variable.name {
	case "FANOUT_PRESSURE":
		delta := rspStateClamp(-12, float64(record.EffectiveControls.FanoutCap-record.CandidateControls.FanoutCap)*3, 12)
		if delta != 0 {
			deltas = append(deltas, RSPForecastScenarioAdjustment{
				Source:  "effective_controls_pending",
				Delta:   delta,
				Summary: "pending effective fanout cap shifts projected coordination pressure",
			})
		}
	case "VERIFIER_QUEUE_DEPTH":
		delta := rspStateClamp(-4, float64(record.EffectiveControls.ReviewDepth-record.CandidateControls.ReviewDepth)*0.75, 4)
		if delta != 0 {
			deltas = append(deltas, RSPForecastScenarioAdjustment{
				Source:  "effective_controls_pending",
				Delta:   delta,
				Summary: "pending effective review depth shifts projected verifier queue depth",
			})
		}
	case "POLLUTION_RATE":
		delta := rspStateClamp(-0.12, float64(record.EffectiveControls.ContextCap-record.CandidateControls.ContextCap)*0.03, 0.12)
		if delta != 0 {
			deltas = append(deltas, RSPForecastScenarioAdjustment{
				Source:  "effective_controls_pending",
				Delta:   delta,
				Summary: "pending effective context cap shifts projected pollution rate",
			})
		}
	case "OFFLOAD_RATIO":
		delta := rspStateClamp(-0.15, float64(record.EffectiveControls.BridgeQuota-record.CandidateControls.BridgeQuota)*0.04, 0.15)
		if delta != 0 {
			deltas = append(deltas, RSPForecastScenarioAdjustment{
				Source:  "effective_controls_pending",
				Delta:   delta,
				Summary: "pending effective bridge quota shifts projected offload ratio",
			})
		}
	case "BLOCKER_MASS":
		delta := rspStateClamp(-0.10, (record.EffectiveControls.MergeThreshold-record.CandidateControls.MergeThreshold)*0.20, 0.10)
		if delta != 0 {
			deltas = append(deltas, RSPForecastScenarioAdjustment{
				Source:  "effective_controls_pending",
				Delta:   delta,
				Summary: "pending effective merge threshold shifts projected blocker mass",
			})
		}
	}
	return deltas
}

func rspForecastScenarioDelta(adjustments []RSPForecastScenarioAdjustment) float64 {
	total := 0.0
	for _, item := range adjustments {
		total += item.Delta
	}
	return total
}

func rspForecastProjectHorizon(
	variable rspForecastVariableInput,
	model string,
	dispersion, growth, historySlope, uncertainty float64,
	adjustments []RSPForecastScenarioAdjustment,
	authority WorkspaceTimeAuthority,
	horizonEpochs int,
) RSPForecastHorizon {
	h := float64(horizonEpochs)

	// RSP-1.2 Appendix 5.3.2: Damped Holt with Exogenous Interventions
	// \hat y_{k+h\mid k} = \ell_k + \phi\frac{1-\phi^h}{1-\phi}b_k + \Gamma^\top u_{k+h}^{plan}
	phi := 0.85
	gammaU := rspForecastScenarioDelta(adjustments)

	// To preserve existing behavior until DB state is fully populated, we emulate \ell_k and b_k
	// from currentValue and historySlope:
	ell_k := variable.currentValue
	b_k := historySlope * 0.5

	holtTerm := 0.0
	if math.Abs(1.0-phi) > 0.001 {
		holtTerm = phi * (1.0 - math.Pow(phi, h)) / (1.0 - phi) * b_k
	} else {
		holtTerm = h * b_k
	}

	// Calculate growth pressure compounding
	growthTerm := 0.0
	switch variable.basis {
	case "count":
		growthTerm = h * growth * maxFloat(variable.currentValue, variable.threshold*0.35)
	case "score":
		growthTerm = h * growth * 14
	default:
		growthTerm = h * growth * 0.08
	}

	mean := ell_k + holtTerm + growthTerm + gammaU
	if variable.basis == "count" && mean < 0 {
		mean = 0
	}
	mean = rspForecastClampByVariable(variable, mean)
	median := mean
	p90 := mean
	if variable.countLike {
		median = math.Round(mean)
		p90 = mean + 1.2816*math.Sqrt(maxFloat(mean, 1))*(1+dispersion*0.25+uncertainty)
	} else {
		margin := rspForecastContinuousMargin(variable, uncertainty)
		p90 = mean + margin
	}
	p90 = rspForecastClampByVariable(variable, p90)
	probability := rspForecastProbability(variable, mean, uncertainty, dispersion)
	return RSPForecastHorizon{
		Epochs:                     horizonEpochs,
		Mean:                       rspForecastRound(mean),
		Median:                     rspForecastRound(median),
		P90:                        rspForecastRound(p90),
		ProbabilityExceedThreshold: rspForecastRound(rspStateClamp(0, probability, 1)),
		Uncertainty:                rspForecastRound(rspStateClamp(0, uncertainty, 1)),
		TemporalContract:           rspForecastTemporalContract(authority, horizonEpochs),
	}
}

func rspForecastProbability(variable rspForecastVariableInput, mean, uncertainty, dispersion float64) float64 {
	var spread float64
	switch variable.basis {
	case "count":
		spread = maxFloat(0.85, math.Sqrt(maxFloat(mean, 1))*(1+dispersion*0.20+uncertainty))
	case "score":
		spread = maxFloat(6, 8+uncertainty*20)
	default:
		spread = maxFloat(0.05, 0.06+uncertainty*0.28)
	}
	return 1 / (1 + math.Exp(-(mean-variable.threshold)/(spread+rspForecastProbabilityEP)))
}

func rspForecastContinuousMargin(variable rspForecastVariableInput, uncertainty float64) float64 {
	switch variable.basis {
	case "score":
		return 8 + uncertainty*15
	default:
		return 0.08 + uncertainty*0.20
	}
}

func rspForecastDispersion(variable rspForecastVariableInput) float64 {
	if variable.previousValue == nil {
		if variable.countLike {
			return 1
		}
		return 0.20
	}
	previous := maxFloat(*variable.previousValue, 0)
	mean := (previous + variable.currentValue) / 2
	if mean <= 0 {
		if variable.countLike {
			return 1
		}
		return 0.20
	}
	variance := math.Pow(variable.currentValue-mean, 2) + math.Pow(previous-mean, 2)
	if variable.countLike {
		return variance / (mean + rspForecastProbabilityEP)
	}
	return math.Sqrt(variance / 2)
}

func rspForecastModel(countLike bool, dispersion float64) string {
	if !countLike {
		return "Holt-damped"
	}
	if dispersion > 1.15 {
		return "NegBin-ETS"
	}
	return "Poisson-ETS"
}

func rspForecastEscalationFactor(state RSPStateReport) float64 {
	growth := 0.02
	growth += state.RiskScore * 0.35
	growth += state.AnomalyScore * 0.20
	growth += state.PersistenceScore * 0.10
	growth += float64(state.PressureScore) / 100 * 0.20
	switch strings.ToUpper(strings.TrimSpace(state.AttentionBand)) {
	case "HOT":
		growth += 0.10
	case "WATCH":
		growth += 0.05
	}
	switch strings.ToUpper(strings.TrimSpace(state.CoherenceBand)) {
	case "CRITICAL":
		growth += 0.10
	case "WATCH", "DEGRADED":
		growth += 0.05
	}
	if state.ControlMode != clusterControlModeSteady {
		growth += 0.07
	}
	if state.CandidateMode != clusterControlModeSteady {
		growth += 0.04
	}
	switch state.HiddenState {
	case "FOCUSED":
		growth -= 0.08
	case "RECOVERING":
		growth -= 0.04
	case "SATURATED", "THRASHING", "UNGROUNDED":
		growth += 0.10
	}
	return rspStateClamp(-0.10, growth, 0.65)
}

func rspForecastHistorySlope(variable rspForecastVariableInput) float64 {
	if variable.previousValue == nil {
		return 0
	}
	return variable.currentValue - *variable.previousValue
}

func rspForecastBaseUncertainty(state RSPStateReport, basisState string, hasHistory bool) float64 {
	uncertainty := 0.16
	if !hasHistory {
		uncertainty += 0.10
	}
	switch basisState {
	case "PARTIAL":
		uncertainty += 0.10
	case "STALE":
		uncertainty += 0.16
	case "UNRESOLVED":
		uncertainty += 0.22
	}
	switch {
	case state.StateConfidence >= 0.70:
		uncertainty -= 0.04
	case state.StateConfidence <= 0.35:
		uncertainty += 0.10
	}
	if state.HiddenState == "UNGROUNDED" {
		uncertainty += 0.08
	}
	return rspStateClamp(0.10, uncertainty, 0.60)
}

func rspForecastBand(maxProbability float64, state RSPStateReport) string {
	switch {
	case maxProbability >= 0.85:
		return "HIGH"
	case maxProbability >= rspForecastAlertCutoff:
		return "WATCH"
	case maxProbability >= 0.55 || state.RiskBand == "HIGH" || state.RiskBand == "ELEVATED":
		return "EMERGING"
	default:
		return "LOW"
	}
}

func rspForecastReadiness(report RSPForecastReport) (string, []string) {
	hints := make([]string, 0, len(report.MissingInputs)+4)
	if basisState := strings.ToLower(strings.TrimSpace(report.BasisState)); basisState != "" {
		hints = append(hints, "basis:"+basisState)
	}
	for _, item := range uniqueTrimmedLocusStrings(report.MissingInputs) {
		hints = append(hints, "missing:"+strings.ToLower(strings.TrimSpace(item)))
	}
	switch {
	case len(report.Projections) > 0:
		hints = append(hints, "projections:present")
	default:
		hints = append(hints, "projections:none")
	}
	if len(report.EvidenceRefs) > 0 {
		hints = append(hints, "evidence:attached")
	}
	if !report.Resolved {
		return "UNAVAILABLE", uniqueTrimmedLocusStrings(append(hints, "resolution:unresolved"))
	}
	if report.SupportedVariables == 0 || len(report.Projections) == 0 {
		return "UNAVAILABLE", uniqueTrimmedLocusStrings(append(hints, "coverage:insufficient"))
	}
	if strings.EqualFold(report.BasisState, "FRESH") && !containsLocusString(report.MissingInputs, "metrics_history") && len(report.MissingInputs) <= 2 {
		return "READY", uniqueTrimmedLocusStrings(append(hints, "coverage:ready"))
	}
	return "PARTIAL", uniqueTrimmedLocusStrings(append(hints, "coverage:partial"))
}

func rspForecastScenarioReadiness(report RSPForecastReport) (string, []string) {
	hints := make([]string, 0, len(report.PlannedInputs)+4)
	if basis := strings.ToLower(strings.TrimSpace(report.ScenarioBasis)); basis != "" && basis != "none" {
		hints = append(hints, "scenario_basis:"+basis)
	}
	if len(report.PlannedInputs) == 0 {
		return "UNAVAILABLE", []string{"scenario:none", "coverage:unavailable"}
	}
	hasPendingControls := false
	clusterScopedPending := false
	adjustedProjectionCount := 0
	for _, item := range report.PlannedInputs {
		source := strings.TrimSpace(item.Source)
		if source != "" {
			hints = append(hints, "planned:"+strings.ToLower(source))
		}
		if scope := strings.TrimSpace(item.ScopeSource); scope != "" {
			hints = append(hints, "scope:"+strings.ToLower(scope))
		}
		if source == "effective_controls_pending" {
			hasPendingControls = true
			if item.ScopeSource == "proto_cluster" {
				clusterScopedPending = true
			}
		}
	}
	for _, projection := range report.Projections {
		if strings.TrimSpace(projection.ScenarioBasis) == "CONTROL_PLAN_ADJUSTED" {
			adjustedProjectionCount++
		}
	}
	switch {
	case hasPendingControls && clusterScopedPending && adjustedProjectionCount > 0:
		return "READY", uniqueTrimmedLocusStrings(append(hints, "coverage:ready"))
	case hasPendingControls && clusterScopedPending:
		return "PARTIAL", uniqueTrimmedLocusStrings(append(hints, "coverage:partial", "adjustment:none"))
	default:
		return "PARTIAL", uniqueTrimmedLocusStrings(append(hints, "coverage:partial"))
	}
}

func rspForecastCoverageSummary(report RSPForecastReport, historyBackedVariables map[string]bool) *RSPForecastCoverageSummary {
	summary := &RSPForecastCoverageSummary{
		ProjectionCount:        len(report.Projections),
		MissingInputCount:      len(uniqueTrimmedLocusStrings(report.MissingInputs)),
		EvidenceRefCount:       len(uniqueTrimmedLocusStrings(report.EvidenceRefs)),
		InterventionInputCount: len(report.InterventionInputs),
		PlannedInputCount:      len(report.PlannedInputs),
		BasisCount:             make(map[string]int),
		ModelCount:             make(map[string]int),
	}
	alertVariables := make(map[string]struct{}, len(report.AlertVariables))
	for _, variable := range uniqueTrimmedLocusStrings(report.AlertVariables) {
		alertVariables[variable] = struct{}{}
	}
	for _, projection := range report.Projections {
		if basis := strings.TrimSpace(projection.Basis); basis != "" {
			summary.BasisCount[basis]++
		}
		if model := strings.TrimSpace(projection.Model); model != "" {
			summary.ModelCount[model]++
		}
		if len(uniqueTrimmedLocusStrings(projection.EvidenceRefs)) > 0 {
			summary.EvidenceBackedProjectionCount++
		}
		if historyBackedVariables[strings.TrimSpace(projection.Variable)] {
			summary.HistoryBackedProjectionCount++
		}
		if strings.TrimSpace(projection.ScenarioBasis) == "CONTROL_PLAN_ADJUSTED" {
			summary.ScenarioAdjustedProjectionCount++
		}
		if _, ok := alertVariables[strings.TrimSpace(projection.Variable)]; ok {
			summary.AlertProjectionCount++
		}
	}
	if len(summary.BasisCount) == 0 {
		summary.BasisCount = nil
	}
	if len(summary.ModelCount) == 0 {
		summary.ModelCount = nil
	}
	return summary
}

func rspForecastMaxProbability(forecasts []RSPForecastHorizon) float64 {
	maxProbability := 0.0
	for _, item := range forecasts {
		maxProbability = maxFloat(maxProbability, item.ProbabilityExceedThreshold)
	}
	return maxProbability
}

func rspForecastOpenTensions(bundle InstrumentationLocusBundle) (float64, bool) {
	if bundle.ControlState != nil {
		return float64(bundle.ControlState.State.State.ConfirmedTensionCount + bundle.ControlState.State.State.PendingTensionCount), true
	}
	if bundle.Control != nil {
		return float64(bundle.Control.Cluster.ConfirmedTensionCount + bundle.Control.Cluster.PendingTensionCount), true
	}
	return 0, false
}

func rspForecastBlockerMass(bundle InstrumentationLocusBundle) (float64, bool) {
	if bundle.ControlState != nil {
		return rspStateClamp(0, bundle.ControlState.State.Metrics.BlockerDensity, 1), true
	}
	if bundle.Control != nil {
		return rspStateClamp(0, bundle.Control.Cluster.Metrics.BlockerDensity, 1), true
	}
	return 0, false
}

func rspForecastVerifierQueueDepth(bundle InstrumentationLocusBundle) (float64, bool) {
	if bundle.ControlState != nil {
		return float64(bundle.ControlState.State.Metrics.OpenQueueCount), true
	}
	if bundle.Control != nil {
		return float64(bundle.Control.Cluster.Metrics.OpenQueueCount), true
	}
	return 0, false
}

func rspForecastFanoutPressure(bundle InstrumentationLocusBundle) (float64, bool) {
	if bundle.ControlState != nil {
		return float64(bundle.ControlState.State.Signals.CoordinationPressure), true
	}
	if bundle.Control != nil {
		return float64(bundle.Control.Cluster.Signals.CoordinationPressure), true
	}
	return 0, false
}

func rspForecastStaleHitRate(
	bundle InstrumentationLocusBundle,
	metricsHistory []MemoryMetricsReportRecord,
	previousMetrics *MemoryMetricsReportRecord,
) (float64, *float64, bool) {
	if len(metricsHistory) > 0 {
		current := rspStateClamp(0, metricsHistory[0].StaleHitRate, 1)
		var previous *float64
		if previousMetrics != nil {
			value := rspStateClamp(0, previousMetrics.StaleHitRate, 1)
			previous = &value
		}
		return current, previous, true
	}
	if bundle.MemoryCoherence != nil {
		current := rspStateClamp(0, bundle.MemoryCoherence.StaleHitRate, 1)
		return current, nil, true
	}
	return 0, nil, false
}

func rspForecastOffloadRatio(
	bundle InstrumentationLocusBundle,
	metricsHistory []MemoryMetricsReportRecord,
	previousMetrics *MemoryMetricsReportRecord,
) (float64, *float64, bool) {
	if len(metricsHistory) > 0 {
		current := rspStateClamp(0, metricsHistory[0].OffloadRatio, 1)
		var previous *float64
		if previousMetrics != nil {
			value := rspStateClamp(0, previousMetrics.OffloadRatio, 1)
			previous = &value
		}
		return current, previous, true
	}
	if bundle.MemoryCoherence != nil {
		current := rspStateClamp(0, bundle.MemoryCoherence.OffloadRatio, 1)
		return current, nil, true
	}
	return 0, nil, false
}

func rspForecastPollutionRate(metricsHistory []MemoryMetricsReportRecord, previousMetrics *MemoryMetricsReportRecord) (float64, *float64, bool) {
	if len(metricsHistory) == 0 {
		return 0, nil, false
	}
	current := rspStateClamp(0, metricsHistory[0].PollutionRate, 1)
	var previous *float64
	if previousMetrics != nil {
		value := rspStateClamp(0, previousMetrics.PollutionRate, 1)
		previous = &value
	}
	return current, previous, true
}

func rspForecastMetricsEvidenceRefs(metricsHistory []MemoryMetricsReportRecord) []string {
	refs := make([]string, 0, len(metricsHistory))
	for _, item := range metricsHistory {
		if reportID := strings.TrimSpace(item.ReportID); reportID != "" {
			refs = append(refs, "memory_metrics:"+reportID)
		}
	}
	return uniqueTrimmedLocusStrings(refs)
}

func normalizeRSPForecastReportFilter(filter RSPForecastReportFilter) RSPForecastReportFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	filter.TaskID = strings.TrimSpace(filter.TaskID)
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	filter.DocKeys = uniqueTrimmedLocusStrings(filter.DocKeys)
	filter.ArtifactRefs = uniqueTrimmedLocusStrings(filter.ArtifactRefs)
	if filter.FrontierLimit <= 0 {
		filter.FrontierLimit = 3
	}
	return filter
}

func rspForecastSnapshotEntityID(report RSPForecastReport) string {
	switch {
	case strings.TrimSpace(report.ProtoClusterID) != "":
		return strings.TrimSpace(report.ProtoClusterID)
	case strings.TrimSpace(report.AgentID) != "" && strings.TrimSpace(report.SessionID) != "":
		return strings.TrimSpace(report.AgentID) + "/" + strings.TrimSpace(report.SessionID)
	case strings.TrimSpace(report.AgentID) != "":
		return strings.TrimSpace(report.AgentID)
	default:
		return strings.TrimSpace(report.WorkspaceID)
	}
}

func rspForecastSnapshotPayload(report RSPForecastReport) map[string]any {
	return map[string]any{
		"workspace_id":              report.WorkspaceID,
		"time_authority":            report.TimeAuthority,
		"proto_cluster_id":          report.ProtoClusterID,
		"agent_id":                  report.AgentID,
		"session_id":                report.SessionID,
		"task_id":                   report.TaskID,
		"resolved":                  report.Resolved,
		"resolved_from":             report.ResolvedFrom,
		"signal_type":               report.SignalType,
		"shadow_phase":              report.ShadowPhase,
		"calibration":               report.Calibration,
		"basis_state":               report.BasisState,
		"missing_inputs":            report.MissingInputs,
		"hidden_state":              report.HiddenState,
		"state_confidence":          report.StateConfidence,
		"risk_score":                report.RiskScore,
		"risk_band":                 report.RiskBand,
		"anomaly_score":             report.AnomalyScore,
		"persistence_score":         report.PersistenceScore,
		"persistence_epochs":        report.PersistenceEpochs,
		"pressure_score":            report.PressureScore,
		"attention_band":            report.AttentionBand,
		"coherence_band":            report.CoherenceBand,
		"supported_variables":       report.SupportedVariables,
		"alert_variables":           report.AlertVariables,
		"max_alert_probability":     report.MaxAlertProbability,
		"forecast_band":             report.ForecastBand,
		"forecast_readiness":        report.ForecastReadiness,
		"forecast_provenance_hints": report.ForecastProvenanceHints,
		"scenario_basis":            report.ScenarioBasis,
		"scenario_readiness":        report.ScenarioReadiness,
		"scenario_provenance_hints": report.ScenarioProvenanceHints,
		"intervention_inputs":       report.InterventionInputs,
		"planned_inputs":            report.PlannedInputs,
		"forecast_coverage_summary": report.ForecastCoverageSummary,
		"projections":               report.Projections,
		"evidence_refs":             report.EvidenceRefs,
		"typed_event_type":          "RSP_FORECAST_SNAPSHOT",
		"summary":                   report.Summary,
	}
}

func rspForecastClampByVariable(variable rspForecastVariableInput, value float64) float64 {
	if variable.countLike {
		return maxFloat(0, value)
	}
	if variable.maxValue > 0 {
		return rspStateClamp(0, value, variable.maxValue)
	}
	return maxFloat(0, value)
}

func rspForecastRound(value float64) float64 {
	return math.Round(value*1000) / 1000
}

func appendMemoryPacketRSPForecastBasisRefs(refs []MemoryPacketBasisRef, metricsHistory []MemoryMetricsReportRecord) []MemoryPacketBasisRef {
	for _, item := range metricsHistory {
		if reportID := strings.TrimSpace(item.ReportID); reportID != "" {
			refs = appendMemoryPacketBasisRef(refs, "memory_metrics_report", reportID, "rsp_forecast_memory_metrics", item.UpdatedAt)
		}
	}
	return refs
}
