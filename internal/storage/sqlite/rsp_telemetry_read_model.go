package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type RSPTelemetryDumpFilter struct {
	WorkspaceID string
	Limit       int
}

type RSPTelemetryDump struct {
	SchemaVersion        string                           `json:"schema_version"`
	CalibrationContracts RSPTelemetryCalibrationContracts `json:"calibration_contracts"`
	Summary              RSPTelemetryCalibrationSummary   `json:"summary"`
	BeliefLogs           []RSPBeliefLog                   `json:"belief_logs"`
	AnomalyLogs          []RSPAnomalyLog                  `json:"anomaly_logs"`
	AnomalyBaselines     []RSPAnomalyBaselineLog          `json:"anomaly_baselines"`
	StateLogs            []RSPStateLog                    `json:"state_logs"`
}

type RSPTelemetryCalibrationSummary struct {
	BeliefLogCount                  int                                  `json:"belief_log_count"`
	BeliefUnversionedLogCount       int                                  `json:"belief_unversioned_log_count"`
	BeliefHighUncertaintyCount      int                                  `json:"belief_high_uncertainty_count"`
	BeliefHighDriftCount            int                                  `json:"belief_high_drift_count"`
	BeliefReadinessBand             string                               `json:"belief_readiness_band,omitempty"`
	LatestBeliefAt                  string                               `json:"latest_belief_at,omitempty"`
	AnomalyAlertCount               int                                  `json:"anomaly_alert_count"`
	AnomalyLogsWithBaselineCount    int                                  `json:"anomaly_logs_with_baseline_count"`
	ShrunkAnomalyAlertCount         int                                  `json:"shrunk_anomaly_alert_count"`
	AnomalyBaselineCount            int                                  `json:"anomaly_baseline_count"`
	WarmAnomalyBaselineCount        int                                  `json:"warm_anomaly_baseline_count"`
	WarmedAnomalyAlertCount         int                                  `json:"warmed_anomaly_alert_count"`
	AnomalyReadinessBand            string                               `json:"anomaly_readiness_band,omitempty"`
	AnomalyWarmingDriver            string                               `json:"anomaly_warming_driver,omitempty"`
	ShrinkageRelianceBand           string                               `json:"shrinkage_reliance_band,omitempty"`
	ShrinkageFallbackQualityBand    string                               `json:"shrinkage_fallback_quality_band,omitempty"`
	WorkspaceFallbackMixBand        string                               `json:"workspace_fallback_mix_band,omitempty"`
	WorkspaceFallbackMixCounts      map[string]int                       `json:"workspace_fallback_mix_counts,omitempty"`
	WorkspaceTierPressureCounts     map[string]int                       `json:"workspace_tier_pressure_counts,omitempty"`
	WorkspaceTierPressureBand       string                               `json:"workspace_tier_pressure_band,omitempty"`
	ShrinkageFallbackScopeTier      string                               `json:"shrinkage_fallback_scope_tier,omitempty"`
	AnomalyBaselineScopeCounts      map[string]int                       `json:"anomaly_baseline_scope_counts,omitempty"`
	ShrunkAnomalyScopeCounts        map[string]int                       `json:"shrunk_anomaly_scope_counts,omitempty"`
	ThrashingAlertCount             int                                  `json:"thrashing_alert_count"`
	StagnationAlertCount            int                                  `json:"stagnation_alert_count"`
	UnversionedAnomalyAlertCount    int                                  `json:"unversioned_anomaly_alert_count"`
	UnversionedAnomalyBaselineCount int                                  `json:"unversioned_anomaly_baseline_count"`
	AnomalyCalibrationVersionCounts map[string]int                       `json:"anomaly_calibration_version_counts,omitempty"`
	ReadinessBand                   string                               `json:"readiness_band,omitempty"`
	CoverageGaps                    []string                             `json:"coverage_gaps,omitempty"`
	CalibrationIntegrityBand        string                               `json:"calibration_integrity_band,omitempty"`
	CalibrationGaps                 []string                             `json:"calibration_gaps,omitempty"`
	ReadinessCoverageRollup         *RSPTelemetryReadinessCoverageRollup `json:"readiness_coverage_rollup,omitempty"`
	LatestAnomalyAt                 string                               `json:"latest_anomaly_at,omitempty"`
	StateLogCount                   int                                  `json:"state_log_count"`
	StateUnversionedLogCount        int                                  `json:"state_unversioned_log_count"`
	StateHighThrashingCount         int                                  `json:"state_high_thrashing_count"`
	StateHighUngroundedCount        int                                  `json:"state_high_ungrounded_count"`
	StateReadinessBand              string                               `json:"state_readiness_band,omitempty"`
	LatestStateAt                   string                               `json:"latest_state_at,omitempty"`
}

type RSPTelemetryReadinessCoverageRollup struct {
	OverallReadinessBand    string         `json:"overall_readiness_band,omitempty"`
	ObservableStreamCount   int            `json:"observable_stream_count"`
	WarmingStreamCount      int            `json:"warming_stream_count"`
	InsufficientStreamCount int            `json:"insufficient_stream_count"`
	CoverageGapCount        int            `json:"coverage_gap_count"`
	HasCoverageGaps         bool           `json:"has_coverage_gaps"`
	CoverageGapCounts       map[string]int `json:"coverage_gap_counts,omitempty"`
}

type RSPBeliefLog struct {
	ID            string  `json:"id"`
	EventID       string  `json:"event_id"`
	EntityType    string  `json:"entity_type"`
	EntityID      string  `json:"entity_id"`
	PriorBM       float64 `json:"prior_b_m"`
	PosteriorBM   float64 `json:"posterior_b_m"`
	UncertaintyUM float64 `json:"uncertainty_u_m"`
	EvidenceMass  float64 `json:"evidence_mass"`
	DriftScore    float64 `json:"drift_score"`
	MeasuredAt    string  `json:"measured_at"`
}

type RSPAnomalyLog struct {
	AlertID                   string  `json:"alert_id"`
	ClusterMode               string  `json:"cluster_mode"`
	MetricName                string  `json:"metric_name"`
	TaskClass                 string  `json:"task_class,omitempty"`
	ShadowPhase               string  `json:"shadow_phase,omitempty"`
	BaselineScope             string  `json:"baseline_scope,omitempty"`
	CalibrationProfile        string  `json:"calibration_profile,omitempty"`
	CalibrationVersion        string  `json:"calibration_version,omitempty"`
	BaselineSampleSize        int     `json:"baseline_sample_size"`
	BaselineLastHealthyWindow string  `json:"baseline_last_healthy_window_at,omitempty"`
	MuHat                     float64 `json:"mu_hat"`
	SigmaHat                  float64 `json:"sigma_hat"`
	CurrentValue              float64 `json:"current_value"`
	EwmaValue                 float64 `json:"ewma_value"`
	SourceDiversity           float64 `json:"source_diversity_discount"`
	AlertType                 string  `json:"alert_type"`
	DetectedAt                string  `json:"detected_at"`
}

type RSPAnomalyBaselineLog struct {
	AgentID             string  `json:"agent_id,omitempty"`
	TaskClass           string  `json:"task_class"`
	Mode                string  `json:"mode"`
	Phase               string  `json:"phase"`
	MetricName          string  `json:"metric_name"`
	CalibrationProfile  string  `json:"calibration_profile,omitempty"`
	CalibrationVersion  string  `json:"calibration_version,omitempty"`
	MuHat               float64 `json:"mu_hat"`
	SigmaHat            float64 `json:"sigma_hat"`
	SampleSize          int     `json:"sample_size"`
	LastHealthyWindowAt string  `json:"last_healthy_window_at,omitempty"`
}

type RSPStateLog struct {
	ID             string  `json:"id"`
	AgentID        string  `json:"agent_id"`
	CachePressureI float64 `json:"cache_pressure_i"`
	StaleHitI      float64 `json:"stale_hit_i"`
	ThrashingRisk  float64 `json:"thrashing_risk"`
	UngroundedRisk float64 `json:"ungrounded_risk"`
	MeasuredAt     string  `json:"measured_at"`
}

// DumpRSPTelemetry reads the shadow mode telemetry tables for analysis and threshold tuning.
func (s *Store) DumpRSPTelemetry(ctx context.Context, filter RSPTelemetryDumpFilter) (RSPTelemetryDump, error) {
	wsID := strings.TrimSpace(filter.WorkspaceID)
	if wsID == "" {
		return RSPTelemetryDump{}, fmt.Errorf("workspace_id cannot be empty")
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 500
	}

	dump := RSPTelemetryDump{
		SchemaVersion:        rspCalibrationSchemaVersion,
		CalibrationContracts: rspTelemetryCalibrationContracts(),
		BeliefLogs:           []RSPBeliefLog{},
		AnomalyLogs:          []RSPAnomalyLog{},
		AnomalyBaselines:     []RSPAnomalyBaselineLog{},
		StateLogs:            []RSPStateLog{},
	}

	// Fast sequential read from recent items
	bRows, err := s.db.QueryContext(ctx, "SELECT id, event_id, entity_type, entity_id, prior_b_m, posterior_b_m, uncertainty_u_m, evidence_mass, drift_score, measured_at FROM rsp_belief_telemetry WHERE workspace_id = ? ORDER BY measured_at DESC LIMIT ?", wsID, limit)
	if err == nil {
		defer bRows.Close()
		for bRows.Next() {
			var l RSPBeliefLog
			if err := bRows.Scan(&l.ID, &l.EventID, &l.EntityType, &l.EntityID, &l.PriorBM, &l.PosteriorBM, &l.UncertaintyUM, &l.EvidenceMass, &l.DriftScore, &l.MeasuredAt); err == nil {
				dump.BeliefLogs = append(dump.BeliefLogs, l)
			}
		}
	} else if err != sql.ErrNoRows {
		return dump, fmt.Errorf("query belief logs: %w", err)
	}

	aRows, err := s.db.QueryContext(ctx, "SELECT alert_id, cluster_mode, metric_name, COALESCE(task_class, ''), COALESCE(shadow_phase, ''), COALESCE(baseline_scope, ''), COALESCE(calibration_profile, ''), COALESCE(calibration_version, ''), COALESCE(baseline_sample_size, 0), COALESCE(baseline_last_healthy_window_at, ''), mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at FROM rsp_anomaly_telemetry WHERE workspace_id = ? ORDER BY detected_at DESC LIMIT ?", wsID, limit)
	if err == nil {
		defer aRows.Close()
		for aRows.Next() {
			var l RSPAnomalyLog
			if err := aRows.Scan(&l.AlertID, &l.ClusterMode, &l.MetricName, &l.TaskClass, &l.ShadowPhase, &l.BaselineScope, &l.CalibrationProfile, &l.CalibrationVersion, &l.BaselineSampleSize, &l.BaselineLastHealthyWindow, &l.MuHat, &l.SigmaHat, &l.CurrentValue, &l.EwmaValue, &l.SourceDiversity, &l.AlertType, &l.DetectedAt); err == nil {
				dump.AnomalyLogs = append(dump.AnomalyLogs, l)
			}
		}
	} else if err != sql.ErrNoRows {
		return dump, fmt.Errorf("query anomaly logs: %w", err)
	}

	abRows, err := s.db.QueryContext(ctx, "SELECT COALESCE(agent_id, ''), COALESCE(task_class, ''), COALESCE(mode, ''), COALESCE(phase, ''), metric_name, COALESCE(calibration_profile, ''), COALESCE(calibration_version, ''), mu_hat, sigma_hat, sample_size, COALESCE(last_healthy_window_at, '') FROM rsp_anomaly_baseline WHERE workspace_id = ? ORDER BY metric_name ASC, agent_id ASC, task_class ASC, mode ASC, phase ASC LIMIT ?", wsID, limit)
	if err == nil {
		defer abRows.Close()
		for abRows.Next() {
			var l RSPAnomalyBaselineLog
			if err := abRows.Scan(&l.AgentID, &l.TaskClass, &l.Mode, &l.Phase, &l.MetricName, &l.CalibrationProfile, &l.CalibrationVersion, &l.MuHat, &l.SigmaHat, &l.SampleSize, &l.LastHealthyWindowAt); err == nil {
				dump.AnomalyBaselines = append(dump.AnomalyBaselines, l)
			}
		}
	} else if err != sql.ErrNoRows {
		return dump, fmt.Errorf("query anomaly baselines: %w", err)
	}

	sRows, err := s.db.QueryContext(ctx, "SELECT id, agent_id, cache_pressure_i, stale_hit_i, thrashing_risk, ungrounded_risk, measured_at FROM rsp_agent_state_telemetry WHERE workspace_id = ? ORDER BY measured_at DESC LIMIT ?", wsID, limit)
	if err == nil {
		defer sRows.Close()
		for sRows.Next() {
			var l RSPStateLog
			if err := sRows.Scan(&l.ID, &l.AgentID, &l.CachePressureI, &l.StaleHitI, &l.ThrashingRisk, &l.UngroundedRisk, &l.MeasuredAt); err == nil {
				dump.StateLogs = append(dump.StateLogs, l)
			}
		}
	} else if err != sql.ErrNoRows {
		return dump, fmt.Errorf("query state logs: %w", err)
	}

	dump.Summary = buildRSPTelemetryCalibrationSummary(dump)
	return dump, nil
}

func buildRSPTelemetryCalibrationSummary(dump RSPTelemetryDump) RSPTelemetryCalibrationSummary {
	summary := RSPTelemetryCalibrationSummary{
		BeliefLogCount:                  len(dump.BeliefLogs),
		AnomalyAlertCount:               len(dump.AnomalyLogs),
		AnomalyBaselineCount:            len(dump.AnomalyBaselines),
		AnomalyBaselineScopeCounts:      map[string]int{},
		ShrunkAnomalyScopeCounts:        map[string]int{},
		AnomalyCalibrationVersionCounts: map[string]int{},
		StateLogCount:                   len(dump.StateLogs),
	}
	if len(dump.BeliefLogs) > 0 {
		summary.LatestBeliefAt = dump.BeliefLogs[0].MeasuredAt
	}
	for _, item := range dump.BeliefLogs {
		summary.BeliefUnversionedLogCount++
		if item.UncertaintyUM >= 0.50 {
			summary.BeliefHighUncertaintyCount++
		}
		if item.DriftScore >= 0.50 {
			summary.BeliefHighDriftCount++
		}
	}
	if len(dump.AnomalyLogs) > 0 {
		summary.LatestAnomalyAt = dump.AnomalyLogs[0].DetectedAt
	}
	for _, item := range dump.AnomalyLogs {
		if strings.TrimSpace(item.BaselineScope) != "" {
			summary.AnomalyLogsWithBaselineCount++
			if rspTelemetryIsShrunkAnomalyScope(item.BaselineScope) {
				summary.ShrunkAnomalyAlertCount++
				summary.ShrunkAnomalyScopeCounts[strings.TrimSpace(item.BaselineScope)]++
			}
		}
		if item.BaselineSampleSize >= rspAnomalyWarmupSampleFloor {
			summary.WarmedAnomalyAlertCount++
		}
		switch strings.ToUpper(strings.TrimSpace(item.AlertType)) {
		case "THRASHING":
			summary.ThrashingAlertCount++
		case "STAGNATION":
			summary.StagnationAlertCount++
		}
		if version := strings.TrimSpace(item.CalibrationVersion); version != "" {
			summary.AnomalyCalibrationVersionCounts[version]++
		} else {
			summary.UnversionedAnomalyAlertCount++
		}
	}
	for _, item := range dump.AnomalyBaselines {
		scope := rspTelemetryBaselineScopeLabel(item)
		summary.AnomalyBaselineScopeCounts[scope]++
		if item.SampleSize >= rspAnomalyWarmupSampleFloor {
			summary.WarmAnomalyBaselineCount++
		}
		if version := strings.TrimSpace(item.CalibrationVersion); version != "" {
			summary.AnomalyCalibrationVersionCounts[version]++
		} else {
			summary.UnversionedAnomalyBaselineCount++
		}
	}
	if len(summary.AnomalyCalibrationVersionCounts) == 0 {
		summary.AnomalyCalibrationVersionCounts = nil
	}
	if len(summary.ShrunkAnomalyScopeCounts) == 0 {
		summary.ShrunkAnomalyScopeCounts = nil
	}
	if len(dump.StateLogs) > 0 {
		summary.LatestStateAt = dump.StateLogs[0].MeasuredAt
	}
	for _, item := range dump.StateLogs {
		summary.StateUnversionedLogCount++
		if item.ThrashingRisk >= 0.65 {
			summary.StateHighThrashingCount++
		}
		if item.UngroundedRisk >= 0.65 {
			summary.StateHighUngroundedCount++
		}
	}
	summary.BeliefReadinessBand = rspTelemetryBeliefReadinessBand(summary)
	summary.ShrinkageRelianceBand = rspTelemetryShrinkageRelianceBand(summary)
	summary.ShrinkageFallbackQualityBand = rspTelemetryShrinkageFallbackQualityBand(summary)
	summary.WorkspaceFallbackMixBand = rspTelemetryWorkspaceFallbackMixBand(summary)
	summary.WorkspaceFallbackMixCounts = rspTelemetryWorkspaceFallbackMixCounts(summary)
	summary.WorkspaceTierPressureCounts = rspTelemetryWorkspaceTierPressureCounts(summary)
	summary.WorkspaceTierPressureBand = rspTelemetryWorkspaceTierPressureBand(summary)
	summary.ShrinkageFallbackScopeTier = rspTelemetryShrinkageFallbackScopeTier(summary)
	summary.AnomalyReadinessBand = rspTelemetryAnomalyReadinessBand(summary)
	summary.AnomalyWarmingDriver = rspTelemetryAnomalyWarmingDriver(summary)
	summary.StateReadinessBand = rspTelemetryStateReadinessBand(summary)
	summary.CoverageGaps = rspTelemetryCoverageGaps(summary)
	summary.ReadinessBand = rspTelemetryReadinessBand(summary)
	summary.CalibrationIntegrityBand = rspTelemetryCalibrationIntegrityBand(summary)
	summary.CalibrationGaps = rspTelemetryCalibrationGaps(summary)
	summary.ReadinessCoverageRollup = rspTelemetryBuildReadinessCoverageRollup(summary)
	return summary
}

func rspTelemetryBaselineScopeLabel(item RSPAnomalyBaselineLog) string {
	if strings.TrimSpace(item.AgentID) == "" {
		return rspAnomalyScopeWorkspace
	}
	if strings.TrimSpace(item.TaskClass) == rspAnomalyDefaultTaskClass && strings.TrimSpace(item.Mode) == rspAnomalyDefaultMode {
		return rspAnomalyScopeAgentDefault
	}
	return rspAnomalyScopeExact
}

func rspTelemetryIsShrunkAnomalyScope(scope string) bool {
	switch strings.TrimSpace(scope) {
	case rspAnomalyScopeExactShrunkAgent, rspAnomalyScopeExactShrunkWS, rspAnomalyScopeAgentShrunkWS:
		return true
	default:
		return false
	}
}

func rspTelemetryReadinessBand(summary RSPTelemetryCalibrationSummary) string {
	switch {
	case summary.BeliefLogCount == 0 && summary.AnomalyAlertCount == 0 && summary.AnomalyBaselineCount == 0 && summary.StateLogCount == 0:
		return "INSUFFICIENT"
	case !rspTelemetryHasWarmedBaselineBackedAnomalyObservability(summary):
		return "WARMING"
	case summary.ShrunkAnomalyAlertCount > 0 && summary.AnomalyReadinessBand == "WARMING":
		return "WARMING"
	default:
		return "OBSERVABLE"
	}
}

func rspTelemetryBeliefReadinessBand(summary RSPTelemetryCalibrationSummary) string {
	switch {
	case summary.BeliefLogCount == 0:
		return "INSUFFICIENT"
	case summary.BeliefLogCount < 3:
		return "WARMING"
	default:
		return "OBSERVABLE"
	}
}

func rspTelemetryAnomalyReadinessBand(summary RSPTelemetryCalibrationSummary) string {
	switch {
	case summary.AnomalyBaselineCount == 0 && summary.AnomalyAlertCount == 0:
		return "INSUFFICIENT"
	case !rspTelemetryHasWarmedBaselineBackedAnomalyObservability(summary):
		return "WARMING"
	case summary.ShrunkAnomalyAlertCount > 0 && summary.ShrinkageFallbackQualityBand == "AGENT_DEFAULT_WORKSPACE_FALLBACK" && summary.ShrinkageRelianceBand == "ALL_SHRUNK":
		return "WARMING"
	case summary.ShrunkAnomalyAlertCount > 0 && summary.ShrinkageFallbackQualityBand == "MIXED_WORKSPACE_FALLBACK" && summary.ShrinkageRelianceBand == "ALL_SHRUNK":
		return "WARMING"
	case summary.ShrunkAnomalyAlertCount > 0 && summary.ShrinkageFallbackQualityBand == "WORKSPACE_FALLBACK" && summary.ShrinkageRelianceBand == "ALL_SHRUNK":
		return "WARMING"
	case summary.ShrunkAnomalyAlertCount > 0 && summary.ShrinkageFallbackQualityBand == "MIXED" && summary.ShrinkageRelianceBand == "ALL_SHRUNK":
		return "WARMING"
	default:
		return "OBSERVABLE"
	}
}

func rspTelemetryAnomalyWarmingDriver(summary RSPTelemetryCalibrationSummary) string {
	if strings.TrimSpace(summary.AnomalyReadinessBand) != "WARMING" {
		return "NONE"
	}
	switch {
	case summary.AnomalyBaselineCount == 0:
		return "BASELINE_MISSING"
	case rspTelemetryHasWarmAnomalyBaselines(summary) && !rspTelemetryHasBaselineBackedAnomalyObservability(summary):
		return "OBSERVABILITY_MISSING"
	case rspTelemetryHasWarmedBaselineBackedShrinkage(summary) && strings.TrimSpace(summary.ShrinkageRelianceBand) == "ALL_SHRUNK":
		exactScopedShrinkCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkAgent] + summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkWS]
		switch strings.TrimSpace(summary.ShrinkageFallbackQualityBand) {
		case "AGENT_DEFAULT_WORKSPACE_FALLBACK":
			return "ALL_SHRUNK_AGENT_DEFAULT_WORKSPACE_FALLBACK"
		case "MIXED_WORKSPACE_FALLBACK":
			return "ALL_SHRUNK_MIXED_WORKSPACE_FALLBACK"
		case "WORKSPACE_FALLBACK":
			if exactScopedShrinkCount == 0 && summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeAgentShrunkWS] > 0 {
				return "ALL_SHRUNK_AGENT_DEFAULT_WORKSPACE_FALLBACK"
			}
			return "ALL_SHRUNK_WORKSPACE_FALLBACK"
		case "MIXED":
			return "ALL_SHRUNK_MIXED_TIER_FALLBACK"
		}
	}
	return "BASELINE_COLD"
}

func rspTelemetryStateReadinessBand(summary RSPTelemetryCalibrationSummary) string {
	switch {
	case summary.StateLogCount == 0:
		return "INSUFFICIENT"
	case summary.StateLogCount < 3:
		return "WARMING"
	default:
		return "OBSERVABLE"
	}
}

func rspTelemetryShrinkageRelianceBand(summary RSPTelemetryCalibrationSummary) string {
	switch {
	case !rspTelemetryHasWarmedBaselineBackedShrinkage(summary):
		return "NONE"
	case summary.ShrunkAnomalyAlertCount >= summary.AnomalyLogsWithBaselineCount:
		return "ALL_SHRUNK"
	case summary.ShrunkAnomalyAlertCount*2 > summary.AnomalyLogsWithBaselineCount:
		return "DOMINANT"
	default:
		return "PARTIAL"
	}
}

func rspTelemetryShrinkageFallbackQualityBand(summary RSPTelemetryCalibrationSummary) string {
	if !rspTelemetryHasWarmedBaselineBackedShrinkage(summary) {
		return "NONE"
	}
	agentLocalizedCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkAgent]
	exactScopedWorkspaceCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkWS]
	agentDefaultWorkspaceCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeAgentShrunkWS]
	workspaceFallbackCount := exactScopedWorkspaceCount + agentDefaultWorkspaceCount
	switch {
	case agentLocalizedCount > 0 && workspaceFallbackCount > 0:
		return "MIXED"
	case agentLocalizedCount > 0:
		return "AGENT_LOCALIZED"
	case exactScopedWorkspaceCount > 0 && agentDefaultWorkspaceCount > 0:
		return "MIXED_WORKSPACE_FALLBACK"
	case exactScopedWorkspaceCount == 0 && agentDefaultWorkspaceCount > 0:
		return "AGENT_DEFAULT_WORKSPACE_FALLBACK"
	case workspaceFallbackCount > 0:
		return "WORKSPACE_FALLBACK"
	default:
		return "UNKNOWN"
	}
}

func rspTelemetryWorkspaceFallbackMixBand(summary RSPTelemetryCalibrationSummary) string {
	if !rspTelemetryHasWarmedBaselineBackedShrinkage(summary) {
		return "NONE"
	}
	exactScopedWorkspaceCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkWS]
	agentDefaultWorkspaceCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeAgentShrunkWS]
	switch {
	case exactScopedWorkspaceCount == 0 && agentDefaultWorkspaceCount == 0:
		return "NONE"
	case exactScopedWorkspaceCount > 0 && agentDefaultWorkspaceCount == 0:
		return "EXACT_ONLY"
	case exactScopedWorkspaceCount == 0 && agentDefaultWorkspaceCount > 0:
		return "AGENT_DEFAULT_ONLY"
	case exactScopedWorkspaceCount > agentDefaultWorkspaceCount:
		return "EXACT_DOMINANT"
	case agentDefaultWorkspaceCount > exactScopedWorkspaceCount:
		return "AGENT_DEFAULT_DOMINANT"
	default:
		return "BALANCED"
	}
}

func rspTelemetryWorkspaceFallbackMixCounts(summary RSPTelemetryCalibrationSummary) map[string]int {
	if !rspTelemetryHasWarmedBaselineBackedShrinkage(summary) {
		return nil
	}
	exactScopedWorkspaceCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkWS]
	agentDefaultWorkspaceCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeAgentShrunkWS]
	if exactScopedWorkspaceCount == 0 && agentDefaultWorkspaceCount == 0 {
		return nil
	}
	return map[string]int{
		"exact_workspace":         exactScopedWorkspaceCount,
		"agent_default_workspace": agentDefaultWorkspaceCount,
	}
}

func rspTelemetryWorkspaceTierPressureCounts(summary RSPTelemetryCalibrationSummary) map[string]int {
	if !rspTelemetryHasWarmedBaselineBackedShrinkage(summary) || summary.ShrunkAnomalyAlertCount <= 0 {
		return nil
	}
	workspaceTierCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkWS] + summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeAgentShrunkWS]
	agentTierCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkAgent]
	if workspaceTierCount == 0 && agentTierCount == 0 {
		return nil
	}
	return map[string]int{
		"workspace_tier": workspaceTierCount,
		"agent_tier":     agentTierCount,
	}
}

func rspTelemetryWorkspaceTierPressureBand(summary RSPTelemetryCalibrationSummary) string {
	pressureCounts := rspTelemetryWorkspaceTierPressureCounts(summary)
	if len(pressureCounts) == 0 || summary.ShrunkAnomalyAlertCount <= 0 {
		return "NONE"
	}
	workspaceTierCount := pressureCounts["workspace_tier"]
	switch {
	case workspaceTierCount <= 0:
		return "NONE"
	case workspaceTierCount >= summary.ShrunkAnomalyAlertCount:
		return "ALL_SHRUNK"
	case workspaceTierCount*2 > summary.ShrunkAnomalyAlertCount:
		return "DOMINANT"
	default:
		return "PARTIAL"
	}
}

func rspTelemetryShrinkageFallbackScopeTier(summary RSPTelemetryCalibrationSummary) string {
	if !rspTelemetryHasWarmedBaselineBackedShrinkage(summary) || len(summary.ShrunkAnomalyScopeCounts) == 0 {
		return "NONE"
	}
	agentTierCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkAgent]
	workspaceTierCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkWS] + summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeAgentShrunkWS]
	switch {
	case agentTierCount > 0 && workspaceTierCount > 0:
		return "MIXED_TIERS"
	case agentTierCount > 0:
		return "AGENT_TIER"
	case workspaceTierCount > 0:
		return "WORKSPACE_TIER"
	default:
		return "NONE"
	}
}

func rspTelemetryCoverageGaps(summary RSPTelemetryCalibrationSummary) []string {
	var gaps []string
	switch summary.BeliefReadinessBand {
	case "INSUFFICIENT":
		gaps = append(gaps, "belief_missing")
	case "WARMING":
		gaps = append(gaps, "belief_coverage_thin")
	}
	switch summary.AnomalyReadinessBand {
	case "INSUFFICIENT":
		gaps = append(gaps, "anomaly_baseline_missing")
	case "WARMING":
		if coldGap := rspTelemetryAnomalyColdGap(summary); coldGap != "" {
			gaps = append(gaps, coldGap)
		}
	}
	if rspTelemetryHasWarmedBaselineBackedShrinkage(summary) {
		gaps = append(gaps, rspTelemetrySparseExactCoverageGap(summary))
	}
	if rspTelemetryHasWarmedBaselineBackedShrinkage(summary) {
		if workspaceFallbackGap := rspTelemetryWorkspaceFallbackGap(summary); workspaceFallbackGap != "" {
			gaps = append(gaps, workspaceFallbackGap)
		}
	}
	switch summary.StateReadinessBand {
	case "INSUFFICIENT":
		gaps = append(gaps, "state_missing")
	case "WARMING":
		gaps = append(gaps, "state_coverage_thin")
	}
	return gaps
}

func rspTelemetryCalibrationIntegrityBand(summary RSPTelemetryCalibrationSummary) string {
	switch {
	case summary.BeliefLogCount == 0 && summary.AnomalyAlertCount == 0 && summary.AnomalyBaselineCount == 0 && summary.StateLogCount == 0:
		return "INSUFFICIENT"
	case summary.UnversionedAnomalyAlertCount > 0 || summary.UnversionedAnomalyBaselineCount > 0:
		return "MIXED_LEGACY"
	case summary.BeliefUnversionedLogCount > 0 || summary.StateUnversionedLogCount > 0:
		return "PARTIAL"
	default:
		return "VERSIONED"
	}
}

func rspTelemetryCalibrationGaps(summary RSPTelemetryCalibrationSummary) []string {
	var gaps []string
	if summary.UnversionedAnomalyAlertCount > 0 || summary.UnversionedAnomalyBaselineCount > 0 {
		gaps = append(gaps, "anomaly_rows_unversioned")
	}
	if summary.BeliefUnversionedLogCount > 0 {
		gaps = append(gaps, "belief_rows_unversioned")
	}
	if summary.StateUnversionedLogCount > 0 {
		gaps = append(gaps, "state_rows_unversioned")
	}
	return gaps
}

func rspTelemetrySparseExactCoverageGap(summary RSPTelemetryCalibrationSummary) string {
	exactScopedShrinkCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkAgent] + summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkWS]
	if exactScopedShrinkCount == 0 && summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeAgentShrunkWS] > 0 {
		switch strings.TrimSpace(summary.ShrinkageFallbackScopeTier) {
		case "WORKSPACE_TIER":
			return "anomaly_agent_default_coverage_sparse_workspace_tier"
		default:
			return "anomaly_agent_default_coverage_sparse"
		}
	}
	if strings.TrimSpace(summary.ShrinkageFallbackQualityBand) == "MIXED_WORKSPACE_FALLBACK" {
		switch strings.TrimSpace(summary.ShrinkageFallbackScopeTier) {
		case "WORKSPACE_TIER":
			return "anomaly_mixed_workspace_coverage_sparse_workspace_tier"
		}
	}
	if strings.TrimSpace(summary.ShrinkageFallbackQualityBand) == "MIXED" {
		switch strings.TrimSpace(summary.ShrinkageFallbackScopeTier) {
		case "MIXED_TIERS":
			return "anomaly_mixed_coverage_sparse_mixed_tiers"
		}
	}
	switch strings.TrimSpace(summary.ShrinkageFallbackScopeTier) {
	case "AGENT_TIER":
		return "anomaly_exact_coverage_sparse_agent_tier"
	case "WORKSPACE_TIER":
		return "anomaly_exact_coverage_sparse_workspace_tier"
	case "MIXED_TIERS":
		return "anomaly_exact_coverage_sparse_mixed_tiers"
	default:
		return "anomaly_exact_coverage_sparse"
	}
}

func rspTelemetryWorkspaceFallbackGap(summary RSPTelemetryCalibrationSummary) string {
	qualityBand := strings.TrimSpace(summary.ShrinkageFallbackQualityBand)
	switch qualityBand {
	case "AGENT_DEFAULT_WORKSPACE_FALLBACK", "MIXED_WORKSPACE_FALLBACK", "WORKSPACE_FALLBACK", "MIXED":
	default:
		return ""
	}
	exactScopedShrinkCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkAgent] + summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkWS]
	agentDefaultWorkspaceOnly := exactScopedShrinkCount == 0 && summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeAgentShrunkWS] > 0
	suffix := ""
	switch strings.TrimSpace(summary.ShrinkageFallbackScopeTier) {
	case "WORKSPACE_TIER":
		suffix = "_workspace_tier"
	case "MIXED_TIERS":
		suffix = "_mixed_tiers"
	}
	prefix := "anomaly_workspace_fallback"
	switch qualityBand {
	case "AGENT_DEFAULT_WORKSPACE_FALLBACK":
		prefix = "anomaly_agent_default_workspace_fallback"
	case "MIXED_WORKSPACE_FALLBACK":
		prefix = "anomaly_mixed_workspace_fallback"
	case "MIXED":
		prefix = "anomaly_mixed_fallback"
	default:
		if agentDefaultWorkspaceOnly {
			prefix = "anomaly_agent_default_workspace_fallback"
		}
	}
	if strings.TrimSpace(summary.ShrinkageRelianceBand) == "ALL_SHRUNK" {
		if qualityBand == "MIXED" && !rspTelemetryHasDominantWorkspaceTierShrinkage(summary) {
			return ""
		}
		return prefix + "_all_shrunk" + suffix
	}
	if qualityBand == "MIXED" && !rspTelemetryHasDominantWorkspaceTierShrinkage(summary) {
		return ""
	}
	if strings.TrimSpace(summary.ShrinkageRelianceBand) == "DOMINANT" {
		return prefix + "_dominant" + suffix
	}
	return prefix + "_partial" + suffix
}

func rspTelemetryAnomalyColdGap(summary RSPTelemetryCalibrationSummary) string {
	if summary.AnomalyBaselineCount == 0 {
		return "anomaly_baseline_missing"
	}
	if rspTelemetryHasWarmAnomalyBaselines(summary) && !rspTelemetryHasBaselineBackedAnomalyObservability(summary) {
		return "anomaly_baseline_observability_missing"
	}
	if rspTelemetryHasWarmedBaselineBackedShrinkage(summary) && strings.TrimSpace(summary.ShrinkageRelianceBand) == "ALL_SHRUNK" {
		switch strings.TrimSpace(summary.ShrinkageFallbackQualityBand) {
		case "AGENT_DEFAULT_WORKSPACE_FALLBACK", "MIXED_WORKSPACE_FALLBACK", "WORKSPACE_FALLBACK", "MIXED":
			return ""
		}
	}
	return "anomaly_baseline_cold"
}

func rspTelemetryHasWarmAnomalyBaselines(summary RSPTelemetryCalibrationSummary) bool {
	return summary.WarmAnomalyBaselineCount > 0
}

func rspTelemetryHasBaselineBackedAnomalyObservability(summary RSPTelemetryCalibrationSummary) bool {
	return summary.AnomalyLogsWithBaselineCount > 0
}

func rspTelemetryHasWarmedBaselineBackedAnomalyObservability(summary RSPTelemetryCalibrationSummary) bool {
	return rspTelemetryHasWarmAnomalyBaselines(summary) &&
		rspTelemetryHasBaselineBackedAnomalyObservability(summary)
}

func rspTelemetryHasWarmedBaselineBackedShrinkage(summary RSPTelemetryCalibrationSummary) bool {
	return summary.ShrunkAnomalyAlertCount > 0 &&
		rspTelemetryHasWarmedBaselineBackedAnomalyObservability(summary)
}

func rspTelemetryHasDominantWorkspaceTierShrinkage(summary RSPTelemetryCalibrationSummary) bool {
	workspaceTierCount := summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeExactShrunkWS] + summary.ShrunkAnomalyScopeCounts[rspAnomalyScopeAgentShrunkWS]
	return workspaceTierCount > 0 && workspaceTierCount*2 > summary.ShrunkAnomalyAlertCount
}

func rspTelemetryBuildReadinessCoverageRollup(summary RSPTelemetryCalibrationSummary) *RSPTelemetryReadinessCoverageRollup {
	rollup := &RSPTelemetryReadinessCoverageRollup{
		OverallReadinessBand: summary.ReadinessBand,
		CoverageGapCount:     len(summary.CoverageGaps),
		HasCoverageGaps:      len(summary.CoverageGaps) > 0,
		CoverageGapCounts:    map[string]int{},
	}
	for _, band := range []string{summary.BeliefReadinessBand, summary.AnomalyReadinessBand, summary.StateReadinessBand} {
		switch band {
		case "OBSERVABLE":
			rollup.ObservableStreamCount++
		case "WARMING":
			rollup.WarmingStreamCount++
		case "INSUFFICIENT":
			rollup.InsufficientStreamCount++
		}
	}
	for _, gap := range summary.CoverageGaps {
		rollup.CoverageGapCounts[gap]++
	}
	return rollup
}
