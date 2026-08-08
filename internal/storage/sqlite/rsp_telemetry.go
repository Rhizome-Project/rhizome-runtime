package sqlite

import (
	"context"
	"database/sql"
	"log"
	"math"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	rspAnomalyDefaultMode           = "DEFAULT"
	rspAnomalyDefaultTaskClass      = "UNKNOWN"
	rspAnomalyDefaultPhase          = rspStateShadowPhase
	rspAnomalyWarmupSampleFloor     = 3
	rspAnomalyEWMAAlpha             = 0.60
	rspAnomalyUpperSigmaThreshold   = 2.0
	rspAnomalyLowerSigmaThreshold   = 2.0
	rspAnomalySigmaFloor            = 0.10
	rspAnomalyScopeExact            = "EXACT"
	rspAnomalyScopeAgentDefault     = "AGENT_DEFAULT"
	rspAnomalyScopeWorkspace        = "WORKSPACE_DEFAULT"
	rspAnomalyScopeColdStart        = "COLD_START"
	rspAnomalyScopeExactShrunkAgent = "EXACT_SHRUNK_AGENT_DEFAULT"
	rspAnomalyScopeExactShrunkWS    = "EXACT_SHRUNK_WORKSPACE_DEFAULT"
	rspAnomalyScopeAgentShrunkWS    = "AGENT_DEFAULT_SHRUNK_WORKSPACE_DEFAULT"
)

type rspAnomalyScopeKey struct {
	WorkspaceID string
	AgentID     string
	TaskClass   string
	Mode        string
	Phase       string
	MetricName  string
}

type rspAnomalyBaselineRecord struct {
	WorkspaceID         string
	AgentID             string
	TaskClass           string
	Mode                string
	Phase               string
	MetricName          string
	CalibrationProfile  string
	CalibrationVersion  string
	MuHat               float64
	SigmaHat            float64
	SampleSize          int
	LastHealthyWindowAt string
}

type rspAnomalyEffectiveBaseline struct {
	ScopeName           string
	CalibrationProfile  string
	CalibrationVersion  string
	SampleSize          int
	LastHealthyWindowAt string
	MuHat               float64
	SigmaHat            float64
	Warmed              bool
}

type rspBeliefTelemetryEstimate struct {
	PriorBM       float64
	PosteriorBM   float64
	UncertaintyUM float64
	EvidenceMass  float64
}

// ProcessRSPBeliefTelemetry computes Belief (b_m) and logs it for retrospective Brier scores.
// It maps standard fact/decision/blocker events into the log-odds space.
func (s *Store) ProcessRSPBeliefTelemetry(ctx context.Context, event RuntimeEventRecord) {
	flags := s.GetRSPCapabilityFlags(ctx, event.WorkspaceID)
	if !flags.BeliefLive {
		return
	}
	// Shadow Mode: We match on specific event types that indicate evidence shifts.
	eventType := strings.ToLower(event.EventType)
	entityType := strings.ToUpper(event.EntityType)

	var domain string
	switch entityType {
	case "FACT", "ENTITY", "LESSON":
		domain = "FACT"
	case "DECISION":
		domain = "DECISION"
	case "BLOCKER", "INCIDENT":
		domain = "BLOCKER"
	default:
		// Also catch task events acting as blockers or verifier passes
		if strings.HasPrefix(eventType, "task.critique") || strings.HasPrefix(eventType, "tension.") {
			domain = "FACT" // Fallback broad domain for telemetry
		} else {
			return
		}
	}

	// Fast-path lookup to check if we can calculate a meaningful update
	if event.EntityID == "" {
		return
	}

	estimate := rspBeliefTelemetryEstimateForEvent(eventType)

	// Fetch drift score placeholder (for user constraint 2)
	driftScore := 0.0
	// Try fetching real drift if it's a known memory graph node
	if detail, err := s.GetMemoryGraphNode(ctx, event.WorkspaceID, memoryGraphNodeID(entityType, event.EntityID)); err == nil {
		driftScore = detail.Node.Drift
	}

	// Insert into telemetry table
	s.appendBeliefTelemetryLog(ctx, event.WorkspaceID, event.EventID, domain, event.EntityID, estimate.PriorBM, estimate.PosteriorBM, estimate.UncertaintyUM, estimate.EvidenceMass, driftScore)
}

func rspBeliefTelemetryEstimateForEvent(eventType string) rspBeliefTelemetryEstimate {
	estimate := rspBeliefTelemetryEstimate{
		PriorBM:       0.0,
		PosteriorBM:   0.0,
		UncertaintyUM: 0.5,
		EvidenceMass:  1.0,
	}

	switch {
	case strings.Contains(eventType, "verify"), strings.Contains(eventType, "pass"), strings.Contains(eventType, "resolve"):
		estimate.PosteriorBM = estimate.PriorBM + 1.2
		estimate.UncertaintyUM = 0.3
	case strings.Contains(eventType, "fail"), strings.Contains(eventType, "reject"), strings.Contains(eventType, "dispute"):
		estimate.PosteriorBM = estimate.PriorBM - 1.5
		estimate.UncertaintyUM = 0.6
	default:
		estimate.PosteriorBM = estimate.PriorBM + 0.1
	}

	return estimate
}

func (s *Store) appendBeliefTelemetryLog(ctx context.Context, workspaceID, eventID, entityType, entityID string, prior, posterior, uncertainty, evidence, drift float64) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	id := nextID("rsptl")

	// Use background context just in case original context is canceled
	_, err := s.writeDB.ExecContext(context.Background(),
		`INSERT INTO rsp_belief_telemetry(id, workspace_id, event_id, entity_type, entity_id, prior_b_m, posterior_b_m, uncertainty_u_m, evidence_mass, drift_score, measured_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, workspaceID, eventID, entityType, entityID, prior, posterior, uncertainty, evidence, drift, now,
	)
	if err != nil {
		log.Printf("[RSP-1.2 Telemetry] Failed to append belief log: %v", err)
	}
}

// ProcessRSPAnomalyTelemetry processes events sequentially to maintain EWMA anomaly parameters
func (s *Store) ProcessRSPAnomalyTelemetry(ctx context.Context, event RuntimeEventRecord) {
	flags := s.GetRSPCapabilityFlags(ctx, event.WorkspaceID)
	if !flags.AnomalyShadow {
		return
	}
	eventType := strings.ToLower(event.EventType)

	// We specifically look at verifier fails and patching rates for anomaly metrics
	metricName := ""
	val := 1.0

	if strings.Contains(eventType, "verifier") && strings.Contains(eventType, "fail") {
		metricName = "verifier_fail_rate"
	} else if strings.Contains(eventType, "patch") || strings.Contains(eventType, "update") {
		metricName = "patch_rate"
	} else {
		return
	}

	// Diversity discount implementation (user constraint 3)
	diversityDiscount := 1.0
	if event.AgentID != "" {
		// Example: discount spammy agents by a factor
		diversityDiscount = 0.8
	}
	scope := s.rspAnomalyScopeForEvent(ctx, event, metricName)
	baseline := s.loadRSPAnomalyEffectiveBaseline(ctx, scope)
	currentValue := val * diversityDiscount
	detectedAt := firstNonEmpty(strings.TrimSpace(event.CreatedAt), time.Now().UTC().Format(time.RFC3339Nano))

	muHat, sigmaHat, ewma := rspAnomalyAlertMoments(metricName, baseline, currentValue)

	for _, candidate := range rspAnomalyBaselineUpdateScopes(scope) {
		if err := s.upsertRSPAnomalyBaselineObservation(ctx, candidate, currentValue, detectedAt); err != nil {
			log.Printf("[RSP-1.2 Telemetry] Failed to update anomaly baseline %s/%s/%s: %v", candidate.AgentID, candidate.TaskClass, candidate.Mode, err)
		}
	}

	clusterMode := scope.Mode

	alertType := "NONE"
	if baseline.Warmed {
		if ewma > muHat+(rspAnomalyUpperSigmaThreshold*sigmaHat) {
			alertType = "THRASHING"
		} else if ewma < muHat-(rspAnomalyLowerSigmaThreshold*sigmaHat) {
			alertType = "STAGNATION"
		}
	}

	if alertType != "NONE" {
		s.appendAnomalyTelemetryLog(ctx, event.WorkspaceID, clusterMode, metricName, scope.TaskClass, scope.Phase, baseline.ScopeName, baseline.CalibrationProfile, baseline.CalibrationVersion, baseline.SampleSize, baseline.LastHealthyWindowAt, muHat, sigmaHat, currentValue, ewma, diversityDiscount, alertType, detectedAt)
	}
}

func (s *Store) appendAnomalyTelemetryLog(ctx context.Context, workspaceID, clusterMode, metricName, taskClass, shadowPhase, baselineScope, calibrationProfile, calibrationVersion string, baselineSampleSize int, baselineLastHealthyWindowAt string, mu_hat, sigma_hat, current, ewma, discount float64, alertType, detectedAt string) {
	alertID := nextID("rspan")

	_, err := s.writeDB.ExecContext(context.Background(),
		`INSERT INTO rsp_anomaly_telemetry(alert_id, workspace_id, cluster_mode, metric_name, task_class, shadow_phase, baseline_scope, calibration_profile, calibration_version, baseline_sample_size, baseline_last_healthy_window_at, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		alertID, workspaceID, clusterMode, metricName, taskClass, shadowPhase, baselineScope, calibrationProfile, calibrationVersion, baselineSampleSize, baselineLastHealthyWindowAt, mu_hat, sigma_hat, current, ewma, discount, alertType, detectedAt,
	)
	if err != nil {
		log.Printf("[RSP-1.2 Telemetry] Failed to append anomaly log: %v", err)
	}
}

func (s *Store) rspAnomalyScopeForEvent(ctx context.Context, event RuntimeEventRecord, metricName string) rspAnomalyScopeKey {
	scope := rspAnomalyScopeKey{
		WorkspaceID: strings.TrimSpace(event.WorkspaceID),
		AgentID:     strings.TrimSpace(event.AgentID),
		TaskClass:   rspAnomalyDefaultTaskClass,
		Mode:        rspAnomalyDefaultMode,
		Phase:       rspAnomalyDefaultPhase,
		MetricName:  strings.TrimSpace(metricName),
	}
	if taskID := strings.TrimSpace(event.TaskID); taskID != "" {
		if status, err := s.GetTaskStatus(ctx, scope.WorkspaceID, taskID); err == nil {
			if taskClass := model.NormalizeTaskClass(strings.TrimSpace(status.TaskClass)); taskClass != "" && taskClass != model.TaskClassUnknown {
				scope.TaskClass = taskClass
			}
		}
		if mode, ok := s.rspAnomalyControlModeForCluster(ctx, scope.WorkspaceID, rspAnomalyTaskProtoClusterID(scope.WorkspaceID, taskID)); ok {
			scope.Mode = mode
		}
	}
	if strings.EqualFold(strings.TrimSpace(event.EntityType), "proto_cluster") {
		if mode, ok := s.rspAnomalyControlModeForCluster(ctx, scope.WorkspaceID, strings.TrimSpace(event.EntityID)); ok {
			scope.Mode = mode
		}
	}
	return scope
}

func (s *Store) rspAnomalyControlModeForCluster(ctx context.Context, workspaceID, protoClusterID string) (string, bool) {
	workspaceID = strings.TrimSpace(workspaceID)
	protoClusterID = strings.TrimSpace(protoClusterID)
	if workspaceID == "" || protoClusterID == "" {
		return "", false
	}
	rows, err := s.listClusterControlStateRows(ctx, workspaceID, protoClusterID)
	if err != nil || len(rows) == 0 {
		return "", false
	}
	return normalizeClusterControlMode(rows[0].CurrentMode), true
}

func rspAnomalyTaskProtoClusterID(workspaceID, taskID string) string {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	if workspaceID == "" || taskID == "" {
		return ""
	}
	return "task:" + workspaceID + "/" + taskID
}

func (s *Store) loadRSPAnomalyBaselineWithFallback(ctx context.Context, scope rspAnomalyScopeKey) (rspAnomalyBaselineRecord, string) {
	for _, candidate := range rspAnomalyBaselineFallbackScopes(scope) {
		record, ok, err := s.getRSPAnomalyBaseline(ctx, candidate.scope)
		if err != nil {
			log.Printf("[RSP-1.2 Telemetry] Failed to load anomaly baseline %s/%s/%s: %v", candidate.scope.AgentID, candidate.scope.TaskClass, candidate.scope.Mode, err)
			continue
		}
		if ok {
			return record, candidate.scopeName
		}
	}
	return rspAnomalyDefaultBaseline(scope), rspAnomalyScopeColdStart
}

func (s *Store) loadRSPAnomalyEffectiveBaseline(ctx context.Context, scope rspAnomalyScopeKey) rspAnomalyEffectiveBaseline {
	scope = normalizeRSPAnomalyScope(scope)
	candidates := rspAnomalyBaselineFallbackScopes(scope)
	type resolvedBaseline struct {
		record    rspAnomalyBaselineRecord
		scopeName string
		ok        bool
	}
	resolved := make([]resolvedBaseline, 0, len(candidates))
	for _, candidate := range candidates {
		record, ok, err := s.getRSPAnomalyBaseline(ctx, candidate.scope)
		if err != nil {
			log.Printf("[RSP-1.2 Telemetry] Failed to load anomaly baseline %s/%s/%s: %v", candidate.scope.AgentID, candidate.scope.TaskClass, candidate.scope.Mode, err)
			resolved = append(resolved, resolvedBaseline{scopeName: candidate.scopeName})
			continue
		}
		resolved = append(resolved, resolvedBaseline{record: record, scopeName: candidate.scopeName, ok: ok})
	}
	if len(resolved) > 0 && resolved[0].ok {
		exact := resolved[0]
		if exact.record.SampleSize >= rspAnomalyWarmupSampleFloor {
			return rspAnomalyEffectiveBaselineFromRecord(exact.record, exact.scopeName)
		}
		if len(resolved) > 1 && resolved[1].ok && resolved[1].record.SampleSize >= rspAnomalyWarmupSampleFloor {
			return rspAnomalyShrinkBaseline(exact.record, resolved[1].record, rspAnomalyScopeExactShrunkAgent)
		}
		if len(resolved) > 2 && resolved[2].ok && resolved[2].record.SampleSize >= rspAnomalyWarmupSampleFloor {
			return rspAnomalyShrinkBaseline(exact.record, resolved[2].record, rspAnomalyScopeExactShrunkWS)
		}
		return rspAnomalyEffectiveBaselineFromRecord(exact.record, exact.scopeName)
	}
	if len(resolved) > 1 && resolved[1].ok {
		agent := resolved[1]
		if agent.record.SampleSize >= rspAnomalyWarmupSampleFloor {
			return rspAnomalyEffectiveBaselineFromRecord(agent.record, agent.scopeName)
		}
		if len(resolved) > 2 && resolved[2].ok && resolved[2].record.SampleSize >= rspAnomalyWarmupSampleFloor {
			return rspAnomalyShrinkBaseline(agent.record, resolved[2].record, rspAnomalyScopeAgentShrunkWS)
		}
		return rspAnomalyEffectiveBaselineFromRecord(agent.record, agent.scopeName)
	}
	if len(resolved) > 2 && resolved[2].ok {
		return rspAnomalyEffectiveBaselineFromRecord(resolved[2].record, resolved[2].scopeName)
	}
	record, scopeName := s.loadRSPAnomalyBaselineWithFallback(ctx, scope)
	return rspAnomalyEffectiveBaselineFromRecord(record, scopeName)
}

func rspAnomalyEffectiveBaselineFromRecord(record rspAnomalyBaselineRecord, scopeName string) rspAnomalyEffectiveBaseline {
	return rspAnomalyEffectiveBaseline{
		ScopeName:           scopeName,
		CalibrationProfile:  rspAnomalyNormalizedCalibrationProfile(record.CalibrationProfile, record.CalibrationVersion),
		CalibrationVersion:  strings.TrimSpace(record.CalibrationVersion),
		SampleSize:          record.SampleSize,
		LastHealthyWindowAt: record.LastHealthyWindowAt,
		MuHat:               record.MuHat,
		SigmaHat:            record.SigmaHat,
		Warmed:              record.SampleSize >= rspAnomalyWarmupSampleFloor,
	}
}

func rspAnomalyShrinkBaseline(primary, fallback rspAnomalyBaselineRecord, scopeName string) rspAnomalyEffectiveBaseline {
	weight := float64(primary.SampleSize) / float64(rspAnomalyWarmupSampleFloor)
	if weight < 0 {
		weight = 0
	}
	if weight > 1 {
		weight = 1
	}
	fallbackWeight := 1 - weight
	calibrationProfile, calibrationVersion := rspAnomalyResolvedCalibration(primary, fallback)
	return rspAnomalyEffectiveBaseline{
		ScopeName:           scopeName,
		CalibrationProfile:  calibrationProfile,
		CalibrationVersion:  calibrationVersion,
		SampleSize:          primary.SampleSize + fallback.SampleSize,
		LastHealthyWindowAt: rspAnomalyLaterTimestamp(primary.LastHealthyWindowAt, fallback.LastHealthyWindowAt),
		MuHat:               (primary.MuHat * weight) + (fallback.MuHat * fallbackWeight),
		SigmaHat:            (primary.SigmaHat * weight) + (fallback.SigmaHat * fallbackWeight),
		Warmed:              true,
	}
}

func rspAnomalyLaterTimestamp(a, b string) string {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	switch {
	case a == "":
		return b
	case b == "":
		return a
	case a >= b:
		return a
	default:
		return b
	}
}

func rspAnomalyNormalizedCalibrationProfile(profile, version string) string {
	profile = strings.TrimSpace(profile)
	version = strings.TrimSpace(version)
	switch {
	case profile != "":
		return profile
	case version != "":
		return "legacy_version_without_profile"
	default:
		return "legacy_unversioned"
	}
}

func rspAnomalyResolvedCalibration(primary, fallback rspAnomalyBaselineRecord) (string, string) {
	primaryProfile := rspAnomalyNormalizedCalibrationProfile(primary.CalibrationProfile, primary.CalibrationVersion)
	fallbackProfile := rspAnomalyNormalizedCalibrationProfile(fallback.CalibrationProfile, fallback.CalibrationVersion)
	primaryVersion := strings.TrimSpace(primary.CalibrationVersion)
	fallbackVersion := strings.TrimSpace(fallback.CalibrationVersion)

	switch {
	case primaryVersion != "" && primaryVersion == fallbackVersion && primaryProfile == fallbackProfile:
		return primaryProfile, primaryVersion
	case primaryVersion == "" && fallbackVersion == "":
		return "legacy_unversioned", ""
	case primaryVersion == "" || fallbackVersion == "":
		return "mixed_or_legacy_fallback", ""
	default:
		return "mixed_version_fallback", ""
	}
}

func (s *Store) getRSPAnomalyBaseline(ctx context.Context, scope rspAnomalyScopeKey) (rspAnomalyBaselineRecord, bool, error) {
	scope = normalizeRSPAnomalyScope(scope)
	var record rspAnomalyBaselineRecord
	err := s.db.QueryRowContext(
		ctx,
		`SELECT workspace_id, agent_id, task_class, mode, phase, metric_name, calibration_profile, calibration_version, mu_hat, sigma_hat, sample_size, last_healthy_window_at
		 FROM rsp_anomaly_baseline
		 WHERE workspace_id = ? AND agent_id = ? AND task_class = ? AND mode = ? AND phase = ? AND metric_name = ?`,
		scope.WorkspaceID, scope.AgentID, scope.TaskClass, scope.Mode, scope.Phase, scope.MetricName,
	).Scan(
		&record.WorkspaceID,
		&record.AgentID,
		&record.TaskClass,
		&record.Mode,
		&record.Phase,
		&record.MetricName,
		&record.CalibrationProfile,
		&record.CalibrationVersion,
		&record.MuHat,
		&record.SigmaHat,
		&record.SampleSize,
		&record.LastHealthyWindowAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return rspAnomalyBaselineRecord{}, false, nil
		}
		return rspAnomalyBaselineRecord{}, false, err
	}
	return record, true, nil
}

func (s *Store) upsertRSPAnomalyBaselineObservation(ctx context.Context, scope rspAnomalyScopeKey, value float64, healthyWindowAt string) error {
	scope = normalizeRSPAnomalyScope(scope)
	current, ok, err := s.getRSPAnomalyBaseline(ctx, scope)
	if err != nil {
		return err
	}
	if !ok {
		current = rspAnomalyDefaultBaseline(scope)
		current.MuHat = value
		current.SigmaHat = 0
		current.SampleSize = 1
		current.LastHealthyWindowAt = healthyWindowAt
	} else {
		nextMu, nextSigma, nextCount := rspAnomalyUpdateMoments(current.MuHat, current.SigmaHat, current.SampleSize, value)
		current.MuHat = nextMu
		current.SigmaHat = nextSigma
		current.SampleSize = nextCount
		current.LastHealthyWindowAt = healthyWindowAt
	}
	_, err = s.writeDB.ExecContext(
		ctx,
		`INSERT INTO rsp_anomaly_baseline(workspace_id, agent_id, task_class, mode, phase, metric_name, calibration_profile, calibration_version, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(workspace_id, agent_id, task_class, mode, phase, metric_name) DO UPDATE SET
		    calibration_profile = excluded.calibration_profile,
		    calibration_version = excluded.calibration_version,
		    mu_hat = excluded.mu_hat,
		    sigma_hat = excluded.sigma_hat,
		    sample_size = excluded.sample_size,
		    last_healthy_window_at = excluded.last_healthy_window_at`,
		current.WorkspaceID,
		current.AgentID,
		current.TaskClass,
		current.Mode,
		current.Phase,
		current.MetricName,
		current.CalibrationProfile,
		current.CalibrationVersion,
		current.MuHat,
		current.SigmaHat,
		current.SampleSize,
		current.LastHealthyWindowAt,
	)
	return err
}

func normalizeRSPAnomalyScope(scope rspAnomalyScopeKey) rspAnomalyScopeKey {
	scope.WorkspaceID = strings.TrimSpace(scope.WorkspaceID)
	scope.AgentID = strings.TrimSpace(scope.AgentID)
	scope.TaskClass = firstNonEmpty(model.NormalizeTaskClass(strings.TrimSpace(scope.TaskClass)), rspAnomalyDefaultTaskClass)
	scope.Mode = firstNonEmpty(strings.TrimSpace(scope.Mode), rspAnomalyDefaultMode)
	scope.Phase = firstNonEmpty(strings.TrimSpace(scope.Phase), rspAnomalyDefaultPhase)
	scope.MetricName = strings.TrimSpace(scope.MetricName)
	return scope
}

func rspAnomalyDefaultBaseline(scope rspAnomalyScopeKey) rspAnomalyBaselineRecord {
	scope = normalizeRSPAnomalyScope(scope)
	mu, sigma := rspAnomalyDefaultMoments(scope.MetricName)
	return rspAnomalyBaselineRecord{
		WorkspaceID:         scope.WorkspaceID,
		AgentID:             scope.AgentID,
		TaskClass:           scope.TaskClass,
		Mode:                scope.Mode,
		Phase:               scope.Phase,
		MetricName:          scope.MetricName,
		CalibrationProfile:  rspAnomalyTelemetryCalibrationContract().Basis,
		CalibrationVersion:  rspAnomalyTelemetryCalibrationContract().CalibrationVersion,
		MuHat:               mu,
		SigmaHat:            sigma,
		SampleSize:          0,
		LastHealthyWindowAt: "",
	}
}

func rspAnomalyDefaultMoments(metricName string) (float64, float64) {
	switch strings.TrimSpace(metricName) {
	case "verifier_fail_rate":
		return 0.15, 0.10
	case "patch_rate":
		return 0.25, 0.12
	default:
		return 0.20, 0.10
	}
}

func rspAnomalyDefaultSigma(metricName string) float64 {
	_, sigma := rspAnomalyDefaultMoments(metricName)
	return maxFloat(sigma, rspAnomalySigmaFloor)
}

func rspAnomalyUpdateMoments(mu, sigma float64, sampleSize int, value float64) (float64, float64, int) {
	if sampleSize <= 0 {
		return value, 0, 1
	}
	prevCount := float64(sampleSize)
	prevM2 := sigma * sigma * prevCount
	nextCount := sampleSize + 1
	delta := value - mu
	nextMu := mu + delta/float64(nextCount)
	delta2 := value - nextMu
	nextM2 := prevM2 + delta*delta2
	nextSigma := 0.0
	if nextCount > 0 {
		nextSigma = math.Sqrt(maxFloat(0, nextM2/float64(nextCount)))
	}
	return nextMu, nextSigma, nextCount
}

func rspAnomalyEWMA(muHat, currentValue float64) float64 {
	return ((1 - rspAnomalyEWMAAlpha) * muHat) + (rspAnomalyEWMAAlpha * currentValue)
}

func rspAnomalyAlertMoments(metricName string, baseline rspAnomalyEffectiveBaseline, currentValue float64) (float64, float64, float64) {
	if baseline.Warmed {
		muHat := baseline.MuHat
		sigmaHat := maxFloat(baseline.SigmaHat, rspAnomalyDefaultSigma(metricName))
		return muHat, sigmaHat, rspAnomalyEWMA(muHat, currentValue)
	}
	muHat, sigmaHat := rspAnomalyDefaultMoments(metricName)
	return muHat, sigmaHat, rspAnomalyEWMA(muHat, currentValue)
}

type rspAnomalyFallbackCandidate struct {
	scope     rspAnomalyScopeKey
	scopeName string
}

func rspAnomalyBaselineFallbackScopes(scope rspAnomalyScopeKey) []rspAnomalyFallbackCandidate {
	scope = normalizeRSPAnomalyScope(scope)
	candidates := []rspAnomalyFallbackCandidate{
		{scope: scope, scopeName: rspAnomalyScopeExact},
	}
	agentDefault := scope
	agentDefault.TaskClass = rspAnomalyDefaultTaskClass
	agentDefault.Mode = rspAnomalyDefaultMode
	candidates = appendUniqueRSPAnomalyFallbackScope(candidates, agentDefault, rspAnomalyScopeAgentDefault)
	workspaceDefault := agentDefault
	workspaceDefault.AgentID = ""
	candidates = appendUniqueRSPAnomalyFallbackScope(candidates, workspaceDefault, rspAnomalyScopeWorkspace)
	return candidates
}

func appendUniqueRSPAnomalyFallbackScope(items []rspAnomalyFallbackCandidate, scope rspAnomalyScopeKey, scopeName string) []rspAnomalyFallbackCandidate {
	scope = normalizeRSPAnomalyScope(scope)
	for _, item := range items {
		if item.scope == scope {
			return items
		}
	}
	return append(items, rspAnomalyFallbackCandidate{scope: scope, scopeName: scopeName})
}

func rspAnomalyBaselineUpdateScopes(scope rspAnomalyScopeKey) []rspAnomalyScopeKey {
	scope = normalizeRSPAnomalyScope(scope)
	scopes := []rspAnomalyScopeKey{scope}
	agentDefault := scope
	agentDefault.TaskClass = rspAnomalyDefaultTaskClass
	agentDefault.Mode = rspAnomalyDefaultMode
	scopes = appendUniqueRSPAnomalyUpdateScope(scopes, agentDefault)
	workspaceDefault := agentDefault
	workspaceDefault.AgentID = ""
	scopes = appendUniqueRSPAnomalyUpdateScope(scopes, workspaceDefault)
	return scopes
}

func appendUniqueRSPAnomalyUpdateScope(items []rspAnomalyScopeKey, scope rspAnomalyScopeKey) []rspAnomalyScopeKey {
	scope = normalizeRSPAnomalyScope(scope)
	for _, item := range items {
		if item == scope {
			return items
		}
	}
	return append(items, scope)
}
