package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	rspStateSignalType        = "AGENT_STATE_POSTERIOR"
	rspStateAnomalySignalType = "ANOMALY_ALERT"
	rspStateShadowPhase       = "S1"
)

var rspStateKnownStates = []string{
	"FOCUSED",
	"EXPLORING",
	"SATURATED",
	"THRASHING",
	"UNGROUNDED",
	"IDLE",
	"RECOVERING",
}

type RSPStateReportFilter struct {
	WorkspaceID    string
	ProtoClusterID string
	AgentID        string
	TaskID         string
	SessionID      string
	DocKeys        []string
	ArtifactRefs   []string
	FrontierLimit  int
}

type RSPStatePosterior struct {
	State     string  `json:"state"`
	Posterior float64 `json:"posterior"`
}

type RSPStateAnomaly struct {
	Family       string   `json:"family"`
	Score        float64  `json:"score"`
	Severity     string   `json:"severity"`
	HardGuard    bool     `json:"hard_guard,omitempty"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
}

type RSPStateDriverHint struct {
	Factor string  `json:"factor"`
	Value  float64 `json:"value"`
}

type RSPStateLocalAutonomicsCandidate struct {
	Command             string   `json:"command"`
	Trigger             string   `json:"trigger"`
	Severity            float64  `json:"severity"`
	PersistenceEpochs   int      `json:"persistence_epochs"`
	Uncertainty         float64  `json:"uncertainty"`
	EffectiveThreshold  float64  `json:"effective_threshold"`
	GateOpen            bool     `json:"gate_open"`
	CapabilityEnabled   bool     `json:"capability_enabled"`
	BoundedLocal        bool     `json:"bounded_local"`
	Reversible          bool     `json:"reversible"`
	SharedTruthMutation bool     `json:"shared_truth_mutation"`
	ObserveOnlyReason   string   `json:"observe_only_reason,omitempty"`
	EvidenceRefs        []string `json:"evidence_refs,omitempty"`
	Summary             string   `json:"summary,omitempty"`
}

type RSPStateReport struct {
	WorkspaceID                   string                             `json:"workspace_id"`
	TimeAuthority                 WorkspaceTimeAuthority             `json:"time_authority"`
	ProtoClusterID                string                             `json:"proto_cluster_id,omitempty"`
	AgentID                       string                             `json:"agent_id,omitempty"`
	SessionID                     string                             `json:"session_id,omitempty"`
	TaskID                        string                             `json:"task_id,omitempty"`
	Resolved                      bool                               `json:"resolved"`
	ResolvedFrom                  string                             `json:"resolved_from,omitempty"`
	MatchScore                    int                                `json:"match_score,omitempty"`
	SignalType                    string                             `json:"signal_type"`
	AnomalySignalType             string                             `json:"anomaly_signal_type"`
	ShadowMode                    bool                               `json:"shadow_mode"`
	ShadowPhase                   string                             `json:"shadow_phase"`
	Calibration                   RSPCalibrationContract             `json:"calibration"`
	BasisState                    string                             `json:"basis_state"`
	MissingInputs                 []string                           `json:"missing_inputs,omitempty"`
	HiddenState                   string                             `json:"hidden_state"`
	StateConfidence               float64                            `json:"state_confidence"`
	RiskScore                     float64                            `json:"risk_score"`
	RiskBand                      string                             `json:"risk_band"`
	AnomalyScore                  float64                            `json:"anomaly_score"`
	ExplorationViability          string                             `json:"exploration_viability,omitempty"`
	ExplorationSuppressionReasons []string                           `json:"exploration_suppression_reasons,omitempty"`
	PersistenceScore              float64                            `json:"persistence_score"`
	PersistenceEpochs             int                                `json:"persistence_epochs"`
	PersistenceBand               string                             `json:"persistence_band"`
	ControlMode                   string                             `json:"control_mode,omitempty"`
	CandidateMode                 string                             `json:"candidate_mode,omitempty"`
	PressureScore                 int                                `json:"pressure_score"`
	AttentionBand                 string                             `json:"attention_band,omitempty"`
	CoherenceBand                 string                             `json:"coherence_band,omitempty"`
	StateRationale                string                             `json:"state_rationale,omitempty"`
	StateDriverHints              []RSPStateDriverHint               `json:"state_driver_hints,omitempty"`
	LocalAutonomicsCandidates     []RSPStateLocalAutonomicsCandidate `json:"local_autonomics_candidates,omitempty"`
	HardGuards                    []string                           `json:"hard_guards,omitempty"`
	Anomalies                     []RSPStateAnomaly                  `json:"anomalies,omitempty"`
	StatePosterior                []RSPStatePosterior                `json:"state_posterior,omitempty"`
	EvidenceRefs                  []string                           `json:"evidence_refs,omitempty"`
	CapabilityFlags               RSPCapabilityFlags                 `json:"capability_flags"`
	GovernedHints                 []RSPGovernedHint                  `json:"governed_hints,omitempty"`
	GovernedHintSummary           *RSPGovernedHintSummary            `json:"governed_hint_summary,omitempty"`
	GeneratedAt                   string                             `json:"generated_at"`
	Summary                       string                             `json:"summary"`
}

type RSPStateSnapshotResult struct {
	Report RSPStateReport     `json:"report"`
	Event  RuntimeEventRecord `json:"event"`
}

type rspStateDerivedSignals struct {
	controlMode                   string
	candidateMode                 string
	pressureScore                 int
	attentionBand                 string
	coherenceBand                 string
	basisState                    string
	missingInputs                 []string
	persistenceEpochs             int
	persistenceScore              float64
	cacheDrift                    float64
	saturation                    float64
	thrashing                     float64
	ungrounded                    float64
	exploration                   float64
	explorationViability          string
	explorationSuppressionReasons []string
	hardGuards                    []string
	evidenceRefs                  []string
}

func (s *Store) BuildRSPStateReport(ctx context.Context, filter RSPStateReportFilter) (RSPStateReport, error) {
	filter = normalizeRSPStateReportFilter(filter)
	if filter.WorkspaceID == "" {
		return RSPStateReport{}, errors.New("workspace_id is required")
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
		return RSPStateReport{}, err
	}
	return buildRSPStateReportFromBundle(s, ctx, filter, bundle), nil
}

func (s *Store) SnapshotRSPStateReport(ctx context.Context, filter RSPStateReportFilter) (RSPStateSnapshotResult, error) {
	if err := s.ensureRSPCapabilityEnabled(ctx, filter.WorkspaceID, rspCapabilityStateShadow); err != nil {
		return RSPStateSnapshotResult{}, err
	}
	report, err := s.BuildRSPStateReport(ctx, filter)
	if err != nil {
		return RSPStateSnapshotResult{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, report.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RSPStateSnapshotResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RSPStateSnapshotResult{}, fmt.Errorf("begin rsp state snapshot tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		event, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: report.WorkspaceID,
			EventType:   "rsp.state_snapshot",
			EntityType:  "rsp_state",
			EntityID:    rspStateSnapshotEntityID(report),
			ActorType:   "system",
			ActorID:     "rsp_state",
			PayloadJSON: mustJSON(rspStateSnapshotPayload(report)),
			CreatedAt:   now,
		})
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RSPStateSnapshotResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RSPStateSnapshotResult{}, fmt.Errorf("commit rsp state snapshot tx: %w", err)
	}
	return RSPStateSnapshotResult{Report: report, Event: event}, nil
}

func buildRSPStateReportFromBundle(store *Store, ctx context.Context, filter RSPStateReportFilter, bundle InstrumentationLocusBundle) RSPStateReport {
	report := RSPStateReport{
		WorkspaceID:       filter.WorkspaceID,
		TimeAuthority:     bundle.TimeAuthority,
		ProtoClusterID:    strings.TrimSpace(bundle.ProtoClusterID),
		AgentID:           rspStateResolvedAgentID(filter, bundle),
		SessionID:         strings.TrimSpace(firstNonEmpty(filter.SessionID, bundleMemoryCoherenceSessionID(bundle.MemoryCoherence))),
		TaskID:            rspStateResolvedTaskID(filter, bundle),
		Resolved:          bundle.Resolved,
		ResolvedFrom:      strings.TrimSpace(bundle.ResolvedFrom),
		MatchScore:        bundle.MatchScore,
		SignalType:        rspStateSignalType,
		AnomalySignalType: rspStateAnomalySignalType,
		ShadowMode:        true,
		ShadowPhase:       rspStateShadowPhase,
		Calibration:       rspStateReadModelCalibrationContract(),
		GeneratedAt:       generatedAtFromWorkspaceTimeAuthority(bundle.TimeAuthority),
	}
	if store != nil {
		report.CapabilityFlags = store.GetRSPCapabilityFlags(ctx, filter.WorkspaceID)
	}
	derived := deriveRSPStateSignals(bundle)
	report.BasisState = derived.basisState
	report.MissingInputs = derived.missingInputs
	report.ControlMode = derived.controlMode
	report.CandidateMode = derived.candidateMode
	report.PressureScore = derived.pressureScore
	report.AttentionBand = derived.attentionBand
	report.CoherenceBand = derived.coherenceBand
	report.PersistenceEpochs = derived.persistenceEpochs
	report.PersistenceScore = derived.persistenceScore
	report.PersistenceBand = rspStatePersistenceBand(report.PersistenceScore)
	report.HardGuards = append([]string(nil), derived.hardGuards...)
	report.EvidenceRefs = append([]string(nil), derived.evidenceRefs...)

	if !report.Resolved {
		report.BasisState = "UNRESOLVED"
		report.HiddenState = "IDLE"
		report.StatePosterior = rspStateNormalizePosterior(map[string]float64{
			"FOCUSED":    0.10,
			"EXPLORING":  0.08,
			"SATURATED":  0.04,
			"THRASHING":  0.03,
			"UNGROUNDED": 0.03,
			"IDLE":       0.64,
			"RECOVERING": 0.08,
		})
		report.StateConfidence = rspStateConfidence(report.StatePosterior) * 0.35
		report.RiskScore = rspStateRiskScore(report.StatePosterior)
		report.RiskBand = rspStateRiskBand(report.RiskScore)
		report.AnomalyScore = 0
		report.StateDriverHints = []RSPStateDriverHint{{Factor: "unresolved_locus", Value: 1}}
		report.StateRationale = "unresolved locus"
		report.Summary = fmt.Sprintf("rsp state shadow report for %s unresolved: locus could not be attached", report.WorkspaceID)
		return report
	}

	report.Anomalies = rspStateBuildAnomalyFamilies(derived)
	report.AnomalyScore = rspStateMaxAnomalyScore(report.Anomalies)
	report.ExplorationViability = derived.explorationViability
	report.ExplorationSuppressionReasons = append([]string(nil), derived.explorationSuppressionReasons...)
	report.StatePosterior = rspStateNormalizePosterior(rspStateBuildPosteriorWeights(report, derived))
	report.HiddenState = rspStateTopPosteriorState(report.StatePosterior)
	report.StateConfidence = rspStateConfidence(report.StatePosterior)
	switch report.BasisState {
	case "STALE":
		report.StateConfidence *= 0.65
	case "PARTIAL":
		report.StateConfidence *= 0.75
	}
	report.StateConfidence = rspStateClamp(0, report.StateConfidence, 1)
	report.RiskScore = rspStateRiskScore(report.StatePosterior)
	report.RiskBand = rspStateRiskBand(report.RiskScore)
	report.StateDriverHints = rspStateBuildDriverHints(report, derived)
	report.StateRationale = rspStateBuildRationale(report.HiddenState, report.StateDriverHints)
	report.LocalAutonomicsCandidates = rspStateBuildLocalAutonomicsCandidates(report, derived)
	report.GovernedHints = rspStateBuildGovernedHints(store, ctx, report, derived)
	report.GovernedHintSummary = buildRSPGovernedHintSummary(report.GovernedHints, nil)
	report.Summary = fmt.Sprintf(
		"rsp state shadow report for %s/%s: %s risk=%s anomaly=%.2f basis=%s",
		firstNonEmpty(report.WorkspaceID, "workspace"),
		firstNonEmpty(report.AgentID, report.ProtoClusterID, "scope"),
		strings.ToLower(report.HiddenState),
		strings.ToLower(report.RiskBand),
		report.AnomalyScore,
		strings.ToLower(report.BasisState),
	)
	return report
}

func deriveRSPStateSignals(bundle InstrumentationLocusBundle) rspStateDerivedSignals {
	derived := rspStateDerivedSignals{
		controlMode:   clusterControlModeSteady,
		candidateMode: clusterControlModeSteady,
		coherenceBand: "STABLE",
		basisState:    "FRESH",
	}
	evidenceRefs := rspStateCollectEvidenceRefs(bundle)
	missingInputs := []string{}

	var (
		pressureScore    int
		attentionBand    string
		candidateStreak  int
		persistenceEpoch int
		basisStale       bool
		metricsMissing   bool
		openQueues       int
		blockerDensity   float64
	)
	if bundle.ControlState != nil {
		derived.controlMode = normalizeClusterControlMode(bundle.ControlState.State.State.CurrentMode)
		derived.candidateMode = normalizeClusterControlMode(bundle.ControlState.State.State.CandidateMode)
		pressureScore = maxInt(bundle.ControlState.State.Signals.PressureScore, bundle.ControlState.State.State.PressureScore)
		attentionBand = firstNonEmpty(bundle.ControlState.State.Signals.AttentionBand, bundle.ControlState.State.State.AttentionBand)
		candidateStreak = maxInt(bundle.ControlState.State.State.CandidateStreak, 0)
		persistenceEpoch = candidateStreak
		if derived.controlMode != clusterControlModeSteady {
			persistenceEpoch = maxInt(persistenceEpoch, 2)
		}
		basisStale = bundle.ControlState.State.BasisStale
		metricsMissing = bundle.ControlState.State.MetricsMissing
		openQueues = bundle.ControlState.State.Metrics.OpenQueueCount
		blockerDensity = bundle.ControlState.State.Metrics.BlockerDensity
		if strings.TrimSpace(bundle.ControlState.State.State.LastTickEventID) != "" {
			evidenceRefs = append(evidenceRefs, "runtime_event:"+strings.TrimSpace(bundle.ControlState.State.State.LastTickEventID))
		}
	} else {
		missingInputs = append(missingInputs, "control_state")
	}
	if bundle.Control != nil {
		basisStale = basisStale || bundle.Control.Cluster.BasisStale
		metricsMissing = metricsMissing || bundle.Control.Cluster.MetricsMissing
		if openQueues == 0 {
			openQueues = bundle.Control.Cluster.Metrics.OpenQueueCount
		}
		if blockerDensity == 0 {
			blockerDensity = bundle.Control.Cluster.Metrics.BlockerDensity
		}
	}

	derived.pressureScore = pressureScore
	derived.attentionBand = attentionBand

	coherenceSeverity := 0.0
	if bundle.MemoryCoherence != nil {
		derived.coherenceBand = firstNonEmpty(strings.TrimSpace(bundle.MemoryCoherence.CoherenceBandHint), "STABLE")
		coherenceSeverity = rspStateCoherenceSeverity(*bundle.MemoryCoherence)
		if bundle.MemoryCoherence.DeadLetterCount > 0 {
			persistenceEpoch = maxInt(persistenceEpoch, 2)
		}
		if bundle.MemoryCoherence.ReadyInvalidationCount > 0 || bundle.MemoryCoherence.BackoffInvalidationCount > 0 || bundle.MemoryCoherence.InvalidatedReplicaCount > 0 {
			persistenceEpoch = maxInt(persistenceEpoch, 1)
		}
	} else {
		missingInputs = append(missingInputs, "memory_coherence")
	}
	if bundle.SegmentReport == nil || len(bundle.SegmentReport.Segments) == 0 {
		missingInputs = append(missingInputs, "segment_report")
	}
	if !bundle.Resolved {
		derived.basisState = "UNRESOLVED"
	} else if basisStale {
		derived.basisState = "STALE"
	} else if len(missingInputs) > 0 || metricsMissing {
		derived.basisState = "PARTIAL"
	}
	derived.missingInputs = missingInputs

	contradictions, ambiguities, bottlenecks, bridges := rspStateCountTensions(bundle)
	segmentCount := 0
	if bundle.SegmentReport != nil {
		segmentCount = len(bundle.SegmentReport.Segments)
	}
	derived.cacheDrift = coherenceSeverity

	pressureContribution := 0.0
	if derived.controlMode != clusterControlModeSteady || !strings.EqualFold(strings.TrimSpace(attentionBand), "STEADY") || openQueues > 0 || blockerDensity > 0 {
		pressureContribution = rspStateClamp(0, float64(pressureScore)/20.0, 1)
	}
	saturation := maxFloat(
		maxFloat(pressureContribution,
			rspStateClamp(0, float64(openQueues)/3.0, 1)),
		rspStateClamp(0, blockerDensity*2.0, 1),
	)
	switch derived.controlMode {
	case clusterControlModeStabilize:
		saturation = maxFloat(saturation, 0.72)
	case clusterControlModeAntiCollapse:
		saturation = maxFloat(saturation, 0.78)
	case clusterControlModeDecentralize:
		saturation = maxFloat(saturation, 0.58)
	}
	if bottlenecks > 0 {
		saturation = maxFloat(saturation, 0.45+0.12*float64(minInt(bottlenecks, 3)))
	}
	if coherenceSeverity >= 0.7 {
		saturation = maxFloat(saturation, 0.50+0.25*coherenceSeverity)
	}
	derived.saturation = rspStateClamp(0, saturation, 1)

	thrashing := 0.0
	if contradictions > 0 {
		thrashing = maxFloat(thrashing, 0.35+0.12*float64(minInt(contradictions, 3)))
	}
	if derived.controlMode == clusterControlModeCoherence {
		thrashing = maxFloat(thrashing, 0.58)
	}
	if derived.candidateMode == clusterControlModeCoherence && candidateStreak > 0 {
		thrashing = maxFloat(thrashing, 0.48)
	}
	if contradictions > 0 && pressureScore >= 2 {
		thrashing = maxFloat(thrashing, 0.70)
	}
	if contradictions > 0 && coherenceSeverity >= 0.7 {
		thrashing = maxFloat(thrashing, 0.76)
	}
	derived.thrashing = rspStateClamp(0, thrashing, 1)

	ungrounded := 0.0
	if contradictions > 0 && (basisStale || metricsMissing) {
		ungrounded = maxFloat(ungrounded, 0.82)
	}
	if contradictions > 0 && segmentCount == 0 {
		ungrounded = maxFloat(ungrounded, 0.62)
	}
	if derived.controlMode == clusterControlModeCoherence && basisStale {
		ungrounded = maxFloat(ungrounded, 0.74)
	}
	if len(bundle.RelatedSegmentRefs) == 0 && basisStale {
		ungrounded = maxFloat(ungrounded, 0.56)
	}
	derived.ungrounded = rspStateClamp(0, ungrounded, 1)

	exploration := 0.05
	switch derived.controlMode {
	case clusterControlModeUnfreeze, clusterControlModeSynergySeeking:
		exploration = maxFloat(exploration, 0.74)
	case clusterControlModeDecentralize:
		exploration = maxFloat(exploration, 0.45)
	}
	if ambiguities > 0 || bridges > 0 {
		exploration = maxFloat(exploration, 0.32+0.10*float64(minInt(ambiguities+bridges, 3)))
	}
	if derived.thrashing > 0.65 || derived.ungrounded > 0.65 {
		exploration *= 0.65
	}
	derived.exploration = rspStateClamp(0, exploration, 1)

	hardGuards := []string{}
	if bundle.MemoryCoherence != nil && (bundle.MemoryCoherence.CoherenceBandHint == "CRITICAL" || bundle.MemoryCoherence.StaleHitRate >= 0.25 || bundle.MemoryCoherence.StaleReadRate >= 0.25 || bundle.MemoryCoherence.DeadLetterCount > 0) {
		hardGuards = append(hardGuards, "CACHE")
	}
	if derived.thrashing >= 0.75 && pressureScore >= 2 {
		hardGuards = append(hardGuards, "LOOP")
	}
	if derived.ungrounded >= 0.75 {
		hardGuards = append(hardGuards, "UNGROUNDED")
	}
	derived.hardGuards = hardGuards
	derived.explorationViability, derived.explorationSuppressionReasons = rspStateExplorationViability(derived, ambiguities, bridges)
	derived.persistenceEpochs = persistenceEpoch
	derived.persistenceScore = rspStateClamp(0, maxFloat(float64(persistenceEpoch)/3.0, coherenceSeverity*0.8), 1)
	derived.evidenceRefs = uniqueTrimmedLocusStrings(evidenceRefs)
	return derived
}

func rspStateBuildAnomalyFamilies(derived rspStateDerivedSignals) []RSPStateAnomaly {
	type familySpec struct {
		name      string
		score     float64
		hardGuard string
	}
	specs := []familySpec{
		{name: "cache_drift", score: derived.cacheDrift, hardGuard: "CACHE"},
		{name: "saturation", score: derived.saturation},
		{name: "thrashing", score: derived.thrashing, hardGuard: "LOOP"},
		{name: "ungrounded", score: derived.ungrounded, hardGuard: "UNGROUNDED"},
	}
	out := make([]RSPStateAnomaly, 0, len(specs))
	for _, spec := range specs {
		if spec.score < 0.20 {
			continue
		}
		out = append(out, RSPStateAnomaly{
			Family:       spec.name,
			Score:        rspStateClamp(0, spec.score, 1),
			Severity:     rspStateAnomalySeverity(spec.score),
			HardGuard:    spec.hardGuard != "" && containsLocusString(derived.hardGuards, spec.hardGuard),
			EvidenceRefs: append([]string(nil), derived.evidenceRefs...),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].Family < out[j].Family
	})
	return out
}

func rspStateBuildPosteriorWeights(report RSPStateReport, derived rspStateDerivedSignals) map[string]float64 {
	weights := map[string]float64{
		"FOCUSED":    0.08,
		"EXPLORING":  0.06,
		"SATURATED":  0.05,
		"THRASHING":  0.03,
		"UNGROUNDED": 0.03,
		"IDLE":       0.04,
		"RECOVERING": 0.05,
	}
	stableScope := derived.controlMode == clusterControlModeSteady && derived.coherenceBand == "STABLE" && report.AnomalyScore < 0.35
	weights["FOCUSED"] += 0.55
	if stableScope {
		weights["FOCUSED"] += 0.35
	}
	weights["FOCUSED"] -= derived.saturation*0.35 + derived.thrashing*0.45 + derived.ungrounded*0.50

	weights["EXPLORING"] += derived.exploration * 1.10
	weights["EXPLORING"] -= derived.saturation*0.20 + derived.ungrounded*0.20

	weights["SATURATED"] += derived.saturation * 1.20
	weights["THRASHING"] += derived.thrashing * 1.25
	weights["UNGROUNDED"] += derived.ungrounded * 1.25

	if containsLocusString(derived.hardGuards, "LOOP") {
		weights["THRASHING"] += 0.35
	}
	if containsLocusString(derived.hardGuards, "UNGROUNDED") {
		weights["UNGROUNDED"] += 0.35
	}
	if derived.coherenceBand != "STABLE" || derived.controlMode == clusterControlModeCoherence {
		weights["RECOVERING"] += 0.35 + derived.cacheDrift*0.35
	}
	if containsLocusString(derived.hardGuards, "CACHE") {
		weights["RECOVERING"] += 0.20
	}
	if report.SessionID == "" && report.TaskID == "" && report.AnomalyScore < 0.20 {
		weights["IDLE"] += 0.25
	}
	switch report.BasisState {
	case "STALE":
		weights["UNGROUNDED"] += 0.10
		weights["RECOVERING"] += 0.10
		weights["FOCUSED"] -= 0.10
	case "PARTIAL":
		weights["RECOVERING"] += 0.10
		weights["IDLE"] += 0.10
	}
	for state, weight := range weights {
		weights[state] = maxFloat(weight, 0.001)
	}
	return weights
}

func rspStateNormalizePosterior(weights map[string]float64) []RSPStatePosterior {
	total := 0.0
	for _, state := range rspStateKnownStates {
		total += maxFloat(0, weights[state])
	}
	if total <= 0 {
		total = float64(len(rspStateKnownStates))
		for _, state := range rspStateKnownStates {
			weights[state] = 1
		}
	}
	out := make([]RSPStatePosterior, 0, len(rspStateKnownStates))
	for _, state := range rspStateKnownStates {
		out = append(out, RSPStatePosterior{
			State:     state,
			Posterior: rspStateClamp(0, maxFloat(0, weights[state])/total, 1),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Posterior != out[j].Posterior {
			return out[i].Posterior > out[j].Posterior
		}
		return out[i].State < out[j].State
	})
	return out
}

func rspStateConfidence(posterior []RSPStatePosterior) float64 {
	if len(posterior) == 0 {
		return 0
	}
	entropy := 0.0
	for _, item := range posterior {
		if item.Posterior <= 0 {
			continue
		}
		entropy -= item.Posterior * math.Log(item.Posterior)
	}
	return rspStateClamp(0, 1-entropy/math.Log(float64(len(rspStateKnownStates))), 1)
}

func rspStateRiskScore(posterior []RSPStatePosterior) float64 {
	risk := 0.0
	for _, item := range posterior {
		switch item.State {
		case "SATURATED", "THRASHING", "UNGROUNDED":
			risk += item.Posterior
		}
	}
	return rspStateClamp(0, risk, 1)
}

func rspStateRiskBand(risk float64) string {
	switch {
	case risk >= 0.80:
		return "HIGH"
	case risk >= 0.60:
		return "ELEVATED"
	case risk >= 0.30:
		return "WATCH"
	default:
		return "LOW"
	}
}

func rspStateExplorationViability(derived rspStateDerivedSignals, ambiguities, bridges int) (string, []string) {
	exploratoryMode := derived.controlMode == clusterControlModeUnfreeze ||
		derived.controlMode == clusterControlModeSynergySeeking ||
		derived.controlMode == clusterControlModeDecentralize
	if !(exploratoryMode || ambiguities+bridges > 0 || derived.exploration >= 0.45) {
		return "", nil
	}
	reasons := []string{}
	if derived.thrashing >= 0.65 {
		reasons = append(reasons, "thrashing")
	}
	if derived.ungrounded >= 0.65 {
		reasons = append(reasons, "ungrounded")
	}
	for _, hardGuard := range derived.hardGuards {
		if trimmed := strings.ToLower(strings.TrimSpace(hardGuard)); trimmed != "" {
			reasons = append(reasons, "hard_guard:"+trimmed)
		}
	}
	reasons = uniqueTrimmedLocusStrings(reasons)
	if len(reasons) > 0 {
		return "SUPPRESSED", reasons
	}
	return "HEALTHY", nil
}

func rspStatePersistenceBand(score float64) string {
	switch {
	case score >= 0.75:
		return "PERSISTENT"
	case score >= 0.40:
		return "EMERGING"
	default:
		return "TRANSIENT"
	}
}

func rspStateAnomalySeverity(score float64) string {
	switch {
	case score >= 0.85:
		return "HARD"
	case score >= 0.70:
		return "SOFT"
	case score >= 0.40:
		return "WATCH"
	default:
		return "INFO"
	}
}

func rspStateMaxAnomalyScore(items []RSPStateAnomaly) float64 {
	score := 0.0
	for _, item := range items {
		score = maxFloat(score, item.Score)
	}
	return rspStateClamp(0, score, 1)
}

func rspStateTopPosteriorState(items []RSPStatePosterior) string {
	if len(items) == 0 {
		return ""
	}
	return items[0].State
}

func rspStateBuildDriverHints(report RSPStateReport, derived rspStateDerivedSignals) []RSPStateDriverHint {
	hints := make([]RSPStateDriverHint, 0, 4)
	add := func(factor string, value float64) {
		if strings.TrimSpace(factor) == "" || value <= 0 {
			return
		}
		hints = append(hints, RSPStateDriverHint{
			Factor: factor,
			Value:  rspStateClamp(0, value, 1),
		})
	}

	switch report.HiddenState {
	case "FOCUSED":
		if derived.controlMode == clusterControlModeSteady && derived.coherenceBand == "STABLE" && report.AnomalyScore < 0.35 {
			add("stable_scope", 1)
		}
		add("low_anomaly", 1-report.AnomalyScore)
	case "EXPLORING":
		add("exploration_signal", derived.exploration)
		switch derived.controlMode {
		case clusterControlModeUnfreeze, clusterControlModeSynergySeeking, clusterControlModeDecentralize:
			add("exploratory_mode", 1)
		}
		if report.ExplorationViability == "HEALTHY" {
			add("healthy_exploration", 1)
		}
	case "SATURATED":
		add("saturation", derived.saturation)
		add("pressure", float64(derived.pressureScore)/20.0)
		if !strings.EqualFold(strings.TrimSpace(derived.attentionBand), "STEADY") {
			add("attention_pressure", 1)
		}
	case "THRASHING":
		add("thrashing", derived.thrashing)
		if containsLocusString(derived.hardGuards, "LOOP") {
			add("loop_guard", 1)
		}
	case "UNGROUNDED":
		add("ungrounded", derived.ungrounded)
		if report.BasisState == "STALE" {
			add("stale_basis", 1)
		}
		if containsLocusString(derived.hardGuards, "UNGROUNDED") {
			add("ungrounded_guard", 1)
		}
	case "RECOVERING":
		add("cache_drift", derived.cacheDrift)
		if derived.coherenceBand != "STABLE" {
			add("coherence_recovery", 1)
		}
		if containsLocusString(derived.hardGuards, "CACHE") {
			add("cache_guard", 1)
		}
	case "IDLE":
		if report.SessionID == "" && report.TaskID == "" {
			add("no_active_scope", 1)
		}
		add("low_anomaly", 1-report.AnomalyScore)
	}

	sort.Slice(hints, func(i, j int) bool {
		if hints[i].Value != hints[j].Value {
			return hints[i].Value > hints[j].Value
		}
		return hints[i].Factor < hints[j].Factor
	})
	if len(hints) > 4 {
		hints = hints[:4]
	}
	return hints
}

func rspStateBuildRationale(hiddenState string, hints []RSPStateDriverHint) string {
	if strings.TrimSpace(hiddenState) == "" || len(hints) == 0 {
		return ""
	}
	labels := make([]string, 0, minInt(len(hints), 3))
	for _, hint := range hints {
		if len(labels) >= 3 {
			break
		}
		label := strings.ReplaceAll(strings.TrimSpace(hint.Factor), "_", " ")
		if label != "" {
			labels = append(labels, label)
		}
	}
	if len(labels) == 0 {
		return ""
	}
	return fmt.Sprintf("%s driven by %s", strings.ToLower(hiddenState), strings.Join(labels, ", "))
}

func normalizeRSPStateReportFilter(filter RSPStateReportFilter) RSPStateReportFilter {
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

func rspStateResolvedAgentID(filter RSPStateReportFilter, bundle InstrumentationLocusBundle) string {
	if agentID := strings.TrimSpace(filter.AgentID); agentID != "" {
		return agentID
	}
	if bundle.MemoryCoherence != nil && strings.TrimSpace(bundle.MemoryCoherence.AgentID) != "" {
		return strings.TrimSpace(bundle.MemoryCoherence.AgentID)
	}
	if bundle.Control != nil && len(bundle.Control.Cluster.AgentIDs) == 1 {
		return strings.TrimSpace(bundle.Control.Cluster.AgentIDs[0])
	}
	return ""
}

func rspStateResolvedTaskID(filter RSPStateReportFilter, bundle InstrumentationLocusBundle) string {
	if taskID := strings.TrimSpace(filter.TaskID); taskID != "" {
		return taskID
	}
	if bundle.CorridorAuthority != nil && strings.TrimSpace(bundle.CorridorAuthority.Task.TaskID) != "" {
		return strings.TrimSpace(bundle.CorridorAuthority.Task.TaskID)
	}
	if bundle.Control != nil && len(bundle.Control.Cluster.TaskIDs) == 1 {
		return strings.TrimSpace(bundle.Control.Cluster.TaskIDs[0])
	}
	return ""
}

func bundleMemoryCoherenceSessionID(report *MemoryCoherenceScopeReport) string {
	if report == nil {
		return ""
	}
	return strings.TrimSpace(report.SessionID)
}

func rspStateCountTensions(bundle InstrumentationLocusBundle) (int, int, int, int) {
	contradictions := 0
	ambiguities := 0
	bottlenecks := 0
	bridges := 0
	if bundle.Control != nil && len(bundle.Control.Tensions) > 0 {
		for _, tension := range bundle.Control.Tensions {
			switch strings.ToLower(strings.TrimSpace(tension.TensionType)) {
			case "contradiction":
				contradictions++
			case "ambiguity", "gap":
				ambiguities++
			case "bottleneck":
				bottlenecks++
			case "bridge":
				bridges++
			}
		}
		return contradictions, ambiguities, bottlenecks, bridges
	}
	for _, item := range bundle.Frontier {
		switch strings.ToLower(strings.TrimSpace(item.TensionType)) {
		case "contradiction":
			contradictions++
		case "ambiguity", "gap":
			ambiguities++
		case "bottleneck":
			bottlenecks++
		case "bridge":
			bridges++
		}
	}
	return contradictions, ambiguities, bottlenecks, bridges
}

func rspStateCollectEvidenceRefs(bundle InstrumentationLocusBundle) []string {
	refs := []string{}
	if clusterID := strings.TrimSpace(bundle.ProtoClusterID); clusterID != "" {
		refs = append(refs, "cluster:"+clusterID)
	}
	if bundle.DominantTension != nil && strings.TrimSpace(bundle.DominantTension.Tension.TensionID) != "" {
		refs = append(refs, "tension:"+strings.TrimSpace(bundle.DominantTension.Tension.TensionID))
	}
	if bundle.MemoryCoherence != nil {
		if reportID := strings.TrimSpace(bundle.MemoryCoherence.MetricsReportID); reportID != "" {
			refs = append(refs, "memory_metrics:"+reportID)
		}
		if reportID := strings.TrimSpace(bundle.MemoryCoherence.ResidencyReportID); reportID != "" {
			refs = append(refs, "memory_residency:"+reportID)
		}
	}
	for _, ref := range bundle.RelatedSegmentRefs {
		if len(refs) >= 8 {
			break
		}
		if trimmed := strings.TrimSpace(ref); trimmed != "" {
			refs = append(refs, "segment:"+trimmed)
		}
	}
	return uniqueTrimmedLocusStrings(refs)
}

func rspStateCoherenceSeverity(report MemoryCoherenceScopeReport) float64 {
	severity := maxFloat(report.StaleHitRate*3.0, report.StaleReadRate*3.0)
	switch strings.ToUpper(strings.TrimSpace(report.CoherenceBandHint)) {
	case "CRITICAL":
		severity = maxFloat(severity, 0.95)
	case "DEGRADED":
		severity = maxFloat(severity, 0.72)
	case "WATCH":
		severity = maxFloat(severity, 0.38)
	}
	if report.ReadyInvalidationCount > 0 {
		severity = maxFloat(severity, 0.68)
	}
	if report.DeadLetterCount > 0 {
		severity = 1
	}
	return rspStateClamp(0, severity, 1)
}

func rspStateSnapshotEntityID(report RSPStateReport) string {
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

func rspStateSnapshotPayload(report RSPStateReport) map[string]any {
	return map[string]any{
		"workspace_id":                report.WorkspaceID,
		"time_authority":              report.TimeAuthority,
		"proto_cluster_id":            report.ProtoClusterID,
		"agent_id":                    report.AgentID,
		"session_id":                  report.SessionID,
		"task_id":                     report.TaskID,
		"resolved":                    report.Resolved,
		"resolved_from":               report.ResolvedFrom,
		"signal_type":                 report.SignalType,
		"anomaly_signal_type":         report.AnomalySignalType,
		"shadow_phase":                report.ShadowPhase,
		"calibration":                 report.Calibration,
		"basis_state":                 report.BasisState,
		"missing_inputs":              report.MissingInputs,
		"hidden_state":                report.HiddenState,
		"state_confidence":            report.StateConfidence,
		"risk_score":                  report.RiskScore,
		"risk_band":                   report.RiskBand,
		"anomaly_score":               report.AnomalyScore,
		"persistence_score":           report.PersistenceScore,
		"persistence_epochs":          report.PersistenceEpochs,
		"persistence_band":            report.PersistenceBand,
		"control_mode":                report.ControlMode,
		"candidate_mode":              report.CandidateMode,
		"pressure_score":              report.PressureScore,
		"attention_band":              report.AttentionBand,
		"coherence_band":              report.CoherenceBand,
		"local_autonomics_candidates": report.LocalAutonomicsCandidates,
		"hard_guards":                 report.HardGuards,
		"anomalies":                   report.Anomalies,
		"state_posterior":             report.StatePosterior,
		"evidence_refs":               report.EvidenceRefs,
		"capability_flags":            report.CapabilityFlags,
		"governed_hints":              report.GovernedHints,
		"governed_hint_summary":       report.GovernedHintSummary,
		"typed_event_type":            rspStateSignalType,
		"shadow_mode":                 true,
		"summary":                     report.Summary,
	}
}

func rspStateBuildLocalAutonomicsCandidates(report RSPStateReport, derived rspStateDerivedSignals) []RSPStateLocalAutonomicsCandidate {
	if !report.Resolved {
		return nil
	}
	const actuatorUncertaintyProxy = 0.20
	persistenceEpochs := maxInt(report.PersistenceEpochs, 0)
	build := func(command, trigger string, severity float64) RSPStateLocalAutonomicsCandidate {
		gate := rspLocalActuationGate(severity, float64(persistenceEpochs), actuatorUncertaintyProxy)
		candidate := RSPStateLocalAutonomicsCandidate{
			Command:             command,
			Trigger:             trigger,
			Severity:            rspStateClamp(0, severity, 1),
			PersistenceEpochs:   persistenceEpochs,
			Uncertainty:         actuatorUncertaintyProxy,
			EffectiveThreshold:  rspStateClamp(0.2, gate.EffectiveThreshold, 1),
			GateOpen:            gate.GateOpen,
			CapabilityEnabled:   report.CapabilityFlags.SafeLocalAutonomicsLive,
			BoundedLocal:        true,
			Reversible:          true,
			SharedTruthMutation: false,
			EvidenceRefs:        append([]string(nil), report.EvidenceRefs...),
		}
		status := "observe_only"
		switch {
		case !candidate.GateOpen:
			status = "below_gate"
		case !candidate.CapabilityEnabled:
			candidate.ObserveOnlyReason = "capability_disabled"
		default:
			candidate.ObserveOnlyReason = "canonical_command_path_pending"
		}
		candidate.Summary = fmt.Sprintf(
			"%s local autonomics %s: state-derived severity %.2f vs threshold %.2f using bounded persistence proxy %d",
			strings.ReplaceAll(strings.ToLower(trigger), "_", " "),
			status,
			candidate.Severity,
			candidate.EffectiveThreshold,
			candidate.PersistenceEpochs,
		)
		return candidate
	}
	candidates := []RSPStateLocalAutonomicsCandidate{}
	thrashingSeverity := maxFloat(derived.thrashing, report.AnomalyScore)
	ungroundedSeverity := maxFloat(derived.ungrounded, report.RiskScore)
	if thrashingSeverity > 0 || persistenceEpochs > 0 {
		candidates = append(candidates, build("agent.control.flush_cache", "thrashing_risk", thrashingSeverity))
	}
	if ungroundedSeverity > 0 || persistenceEpochs > 0 {
		candidates = append(candidates, build("agent.control.refresh_kernel", "ungrounded_risk", ungroundedSeverity))
	}
	return candidates
}

func rspStateBuildGovernedHints(store *Store, ctx context.Context, report RSPStateReport, derived rspStateDerivedSignals) []RSPGovernedHint {
	if !report.CapabilityFlags.GovernedHintsLive {
		return nil
	}
	scope := "workspace"
	entityID := strings.TrimSpace(report.WorkspaceID)
	if clusterID := strings.TrimSpace(report.ProtoClusterID); clusterID != "" {
		scope = "cluster"
		entityID = clusterID
	} else if agentID := strings.TrimSpace(report.AgentID); agentID != "" {
		scope = "agent"
		entityID = agentID
	}
	evidenceRefs := append([]string(nil), report.EvidenceRefs...)
	if len(evidenceRefs) > 8 {
		evidenceRefs = evidenceRefs[:8]
	}
	evidenceSourceKinds := rspGovernedHintEvidenceSourceKinds(evidenceRefs)
	runtimeEventRefCount := rspGovernedHintRuntimeEventRefCount(evidenceRefs)
	evidenceSourceMix := rspGovernedHintEvidenceSourceMix(evidenceSourceKinds, runtimeEventRefCount)
	evidenceSupportGroups, rootCauseGroups, runtimeLineageBasis := rspGovernedHintEvidenceSupportLineage(store, ctx, report.WorkspaceID, entityID, evidenceRefs)
	evidenceDiversity := rspGovernedHintEvidenceDiversity(evidenceSupportGroups)
	evidenceDiversityBand := rspGovernedHintEvidenceDiversityBand(evidenceDiversity)
	ttlWindowState := rspGovernedHintTTLWindowState(report.PersistenceEpochs, 2)
	uncertainty := rspStateClamp(0, 1-report.StateConfidence, 1)
	build := func(hintType string, severity float64, actions []string) RSPGovernedHint {
		hint := RSPGovernedHint{
			Type:                  hintType,
			Scope:                 scope,
			EntityID:              entityID,
			Severity:              rspStateClamp(0, severity, 1),
			Uncertainty:           uncertainty,
			PersistenceEpochs:     report.PersistenceEpochs,
			EvidenceDiversity:     evidenceDiversity,
			EvidenceDiversityBand: evidenceDiversityBand,
			EvidenceSourceMix:     evidenceSourceMix,
			RuntimeEventRefCount:  runtimeEventRefCount,
			EvidenceRefs:          append([]string(nil), evidenceRefs...),
			EvidenceSourceKinds:   append([]string(nil), evidenceSourceKinds...),
			RootCauseGroups:       append([]string(nil), rootCauseGroups...),
			RuntimeLineageBasis:   runtimeLineageBasis,
			TTLWindowState:        ttlWindowState,
			RecommendedActions:    append([]string(nil), actions...),
			RecommendationClass:   rspGovernedHintRecommendationClass(hintType, actions),
			ActuationClass:        "governed_hint",
			TTLEpochs:             2,
		}
		hint.Summary = summarizeRSPGovernedHint(hint)
		return hint
	}
	hints := []RSPGovernedHint{}
	if report.AnomalyScore >= 0.70 || report.HiddenState == "THRASHING" {
		hint := build(
			"routing_risk",
			maxFloat(report.AnomalyScore, report.RiskScore),
			[]string{"require_far_reviewer", "raise_reviewer_diversity", "reduce_solver_fanout"},
		)
		hint.HintID = strings.ToLower(strings.Join([]string{"rsphint", entityID, "routing", report.HiddenState}, ":"))
		hints = append(hints, hint)
	}
	if report.CoherenceBand != "" && report.CoherenceBand != "STABLE" {
		hint := build(
			"memory_coherence",
			maxFloat(derived.cacheDrift, report.RiskScore),
			[]string{"tighten_context_cap", "prefer_kernel_refresh"},
		)
		hint.HintID = strings.ToLower(strings.Join([]string{"rsphint", entityID, "memory", report.CoherenceBand}, ":"))
		hints = append(hints, hint)
	}
	if report.HiddenState == "UNGROUNDED" || report.RiskBand == "HIGH" {
		hint := build(
			"grounding_risk",
			maxFloat(report.RiskScore, derived.ungrounded),
			[]string{"require_far_reviewer", "tighten_context_cap", "prefer_kernel_refresh"},
		)
		hint.HintID = strings.ToLower(strings.Join([]string{"rsphint", entityID, "grounding", report.HiddenState}, ":"))
		hints = append(hints, hint)
	}
	return hints
}

func rspGovernedHintEvidenceSourceKinds(evidenceRefs []string) []string {
	kinds := make([]string, 0, len(evidenceRefs))
	for _, ref := range evidenceRefs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		kind := ref
		if idx := strings.Index(ref, ":"); idx > 0 {
			kind = ref[:idx]
		}
		kinds = append(kinds, kind)
	}
	return uniqueTrimmedLocusStrings(kinds)
}

func rspGovernedHintEvidenceDiversity(sourceKinds []string) float64 {
	const governedHintEvidenceKindCeiling = 4.0
	if len(sourceKinds) == 0 {
		return 0
	}
	return rspStateClamp(0, float64(len(sourceKinds))/governedHintEvidenceKindCeiling, 1)
}

func rspGovernedHintEvidenceDiversityBand(diversity float64) string {
	switch {
	case diversity >= 0.75:
		return "HIGH"
	case diversity >= 0.35:
		return "MEDIUM"
	case diversity > 0:
		return "LOW"
	default:
		return "UNKNOWN"
	}
}

func rspGovernedHintRuntimeEventRefCount(evidenceRefs []string) int {
	count := 0
	for _, ref := range evidenceRefs {
		if strings.HasPrefix(strings.TrimSpace(ref), "runtime_event:") {
			count++
		}
	}
	return count
}

func rspGovernedHintEvidenceSourceMix(sourceKinds []string, runtimeEventRefCount int) string {
	hasContext := false
	for _, kind := range sourceKinds {
		if !strings.EqualFold(strings.TrimSpace(kind), "runtime_event") {
			hasContext = true
			break
		}
	}
	switch {
	case runtimeEventRefCount > 0 && hasContext:
		return "MIXED"
	case runtimeEventRefCount > 0:
		return "RUNTIME_ONLY"
	case len(sourceKinds) > 0:
		return "CONTEXT_ONLY"
	default:
		return "UNKNOWN"
	}
}

func rspGovernedHintTTLWindowState(persistenceEpochs, ttlEpochs int) string {
	persistenceEpochs = maxInt(persistenceEpochs, 0)
	ttlEpochs = maxInt(ttlEpochs, 0)
	switch {
	case ttlEpochs <= 0:
		return "UNBOUNDED"
	case persistenceEpochs <= 0:
		return "EARLY"
	case persistenceEpochs < ttlEpochs:
		return "ACTIVE"
	default:
		return "EDGE"
	}
}

func rspGovernedHintRecommendationClass(hintType string, actions []string) string {
	switch strings.TrimSpace(hintType) {
	case "routing_risk":
		return "coordination_review"
	case "memory_coherence":
		return "memory_freshness"
	case "grounding_risk":
		return "grounding_safety"
	}
	hasReview := false
	hasMemory := false
	for _, action := range normalizeUnifiedHintActions(actions) {
		switch action {
		case "require_far_reviewer", "raise_reviewer_diversity", "reduce_solver_fanout":
			hasReview = true
		case "tighten_context_cap", "prefer_kernel_refresh":
			hasMemory = true
		}
	}
	switch {
	case hasReview && hasMemory:
		return "mixed_guard"
	case hasReview:
		return "coordination_review"
	case hasMemory:
		return "memory_freshness"
	default:
		return ""
	}
}

func rspGovernedHintRootCauseGroups(store *Store, ctx context.Context, workspaceID, entityID string, evidenceRefs []string) []string {
	_, groups, _ := rspGovernedHintEvidenceSupportLineage(store, ctx, workspaceID, entityID, evidenceRefs)
	return groups
}

func rspGovernedHintRootCauseLineage(store *Store, ctx context.Context, workspaceID, entityID string, evidenceRefs []string) ([]string, string) {
	_, groups, basis := rspGovernedHintEvidenceSupportLineage(store, ctx, workspaceID, entityID, evidenceRefs)
	return groups, basis
}

func rspGovernedHintEvidenceSupportLineage(store *Store, ctx context.Context, workspaceID, entityID string, evidenceRefs []string) ([]string, []string, string) {
	workspaceID = strings.TrimSpace(workspaceID)
	entityID = strings.TrimSpace(entityID)
	if store == nil || workspaceID == "" {
		return nil, nil, "NONE"
	}
	supportGroups := []string{}
	rootCauseGroups := []string{}
	directRuntimeRef := false
	evidenceRefFallback := false
	for _, ref := range evidenceRefs {
		resolvedGroups, basis := store.rspResolveRootCauseGroupsForRef(ctx, workspaceID, ref)
		if len(resolvedGroups) > 0 {
			for _, group := range resolvedGroups {
				supportGroups = append(supportGroups, "root_cause:"+strings.TrimSpace(group))
				rootCauseGroups = append(rootCauseGroups, group)
			}
			switch strings.TrimSpace(basis) {
			case "DIRECT_RUNTIME_EVENT_REF":
				directRuntimeRef = true
			case "EVIDENCE_REF_ENTITY_FALLBACK":
				evidenceRefFallback = true
			}
			continue
		}
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		kind := ref
		if idx := strings.Index(ref, ":"); idx > 0 {
			kind = ref[:idx]
		}
		kind = strings.ToLower(strings.TrimSpace(kind))
		if kind != "" {
			supportGroups = append(supportGroups, "kind:"+kind)
		}
	}
	if len(rootCauseGroups) > 0 {
		switch {
		case directRuntimeRef && evidenceRefFallback:
			return uniqueTrimmedLocusStrings(supportGroups), uniqueTrimmedLocusStrings(rootCauseGroups), "MIXED_EVIDENCE_LINEAGE"
		case directRuntimeRef:
			return uniqueTrimmedLocusStrings(supportGroups), uniqueTrimmedLocusStrings(rootCauseGroups), "DIRECT_RUNTIME_EVENT_REFS"
		default:
			return uniqueTrimmedLocusStrings(supportGroups), uniqueTrimmedLocusStrings(rootCauseGroups), "EVIDENCE_REF_ENTITY_FALLBACK"
		}
	}
	if entityID != "" {
		rootCauseGroups = store.rspResolveEntityRuntimeRootCauseGroups(ctx, workspaceID, entityID, rspRootCauseEntityFallbackLimit)
	}
	if len(rootCauseGroups) > 0 {
		for _, group := range rootCauseGroups {
			supportGroups = append(supportGroups, "root_cause:"+strings.TrimSpace(group))
		}
		return uniqueTrimmedLocusStrings(supportGroups), uniqueTrimmedLocusStrings(rootCauseGroups), "ENTITY_SCOPE_FALLBACK"
	}
	return uniqueTrimmedLocusStrings(supportGroups), nil, "NONE"
}

func summarizeRSPGovernedHint(hint RSPGovernedHint) string {
	parts := []string{firstNonEmpty(strings.TrimSpace(hint.Type), "governed_hint")}
	if recommendationClass := strings.TrimSpace(hint.RecommendationClass); recommendationClass != "" {
		parts = append(parts, "class "+recommendationClass)
	}
	parts = append(parts, fmt.Sprintf("severity %.2f", rspStateClamp(0, hint.Severity, 1)))
	parts = append(parts, fmt.Sprintf("uncertainty %.2f", rspStateClamp(0, hint.Uncertainty, 1)))
	parts = append(parts, fmt.Sprintf("ttl %d", maxInt(hint.TTLEpochs, 0)))
	if hint.EvidenceDiversity > 0 {
		parts = append(parts, fmt.Sprintf("evidence diversity %.2f", rspStateClamp(0, hint.EvidenceDiversity, 1)))
	}
	if diversityBand := strings.TrimSpace(hint.EvidenceDiversityBand); diversityBand != "" {
		parts = append(parts, "diversity band "+diversityBand)
	}
	if sourceMix := strings.TrimSpace(hint.EvidenceSourceMix); sourceMix != "" {
		parts = append(parts, "source mix "+sourceMix)
	}
	if hint.RuntimeEventRefCount > 0 {
		parts = append(parts, fmt.Sprintf("runtime refs %d", hint.RuntimeEventRefCount))
	}
	if len(hint.EvidenceSourceKinds) > 0 {
		parts = append(parts, "sources "+strings.Join(hint.EvidenceSourceKinds, ", "))
	}
	if len(hint.RootCauseGroups) > 0 {
		parts = append(parts, "root causes "+strings.Join(hint.RootCauseGroups, ", "))
	}
	if lineageBasis := strings.TrimSpace(hint.RuntimeLineageBasis); lineageBasis != "" {
		parts = append(parts, "lineage basis "+lineageBasis)
	}
	if ttlWindowState := strings.TrimSpace(hint.TTLWindowState); ttlWindowState != "" {
		parts = append(parts, "ttl window "+ttlWindowState)
	}
	return strings.Join(parts, " | ")
}

func rspStateClamp(minValue, value, maxValue float64) float64 {
	switch {
	case value < minValue:
		return minValue
	case value > maxValue:
		return maxValue
	default:
		return value
	}
}
