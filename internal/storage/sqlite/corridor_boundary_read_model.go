package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	corridorBoundaryStateInRange      = "IN_RANGE"
	corridorBoundaryStateNearBoundary = "NEAR_BOUNDARY"
	corridorBoundaryStateViolated     = "VIOLATED"

	corridorBoundaryBasisStateReady          = "READY"
	corridorBoundaryBasisStateUnderEvidenced = "UNDER_EVIDENCED"
	corridorBoundaryBasisStateStaleBasis     = "STALE_BASIS"

	corridorBoundarySignalInRange       = "IN_RANGE"
	corridorBoundarySignalNearLower     = "NEAR_LOWER"
	corridorBoundarySignalNearUpper     = "NEAR_UPPER"
	corridorBoundarySignalLowViolation  = "LOW_VIOLATION"
	corridorBoundarySignalHighViolation = "HIGH_VIOLATION"

	corridorBoundarySeverityInfo     = "INFO"
	corridorBoundarySeverityWatch    = "WATCH"
	corridorBoundarySeverityWarning  = "WARNING"
	corridorBoundarySeverityCritical = "CRITICAL"

	corridorBoundaryNearThreshold     = 0.12
	corridorBoundaryWarningThreshold  = 0.05
	corridorBoundaryCriticalThreshold = 0.20

	corridorBoundarySourceFitDerived = "FIT_DERIVED"
)

type CorridorBoundaryFilter struct {
	WorkspaceID    string `json:"workspace_id"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type CorridorBoundaryMetricSignal struct {
	Metric          string   `json:"metric"`
	Value           float64  `json:"value"`
	LowerBound      *float64 `json:"lower_bound,omitempty"`
	UpperBound      *float64 `json:"upper_bound,omitempty"`
	Signal          string   `json:"signal"`
	Severity        string   `json:"severity"`
	Delta           float64  `json:"delta"`
	NormalizedDelta float64  `json:"normalized_delta"`
	Summary         string   `json:"summary,omitempty"`
}

type CorridorBoundaryClusterReport struct {
	ProtoClusterID              string                         `json:"proto_cluster_id"`
	ResolutionKind              string                         `json:"resolution_kind"`
	TaskIDs                     []string                       `json:"task_ids,omitempty"`
	SessionIDs                  []string                       `json:"session_ids,omitempty"`
	DocKeys                     []string                       `json:"doc_keys,omitempty"`
	ArtifactRefs                []string                       `json:"artifact_refs,omitempty"`
	AgentIDs                    []string                       `json:"agent_ids,omitempty"`
	TaskClass                   string                         `json:"task_class,omitempty"`
	TaskClassSource             string                         `json:"task_class_source,omitempty"`
	TaskClassUpdatedAt          string                         `json:"task_class_updated_at,omitempty"`
	TaskClassHint               string                         `json:"task_class_hint"`
	CorridorCatalogHint         string                         `json:"corridor_catalog_hint,omitempty"`
	CorridorLookup              CorridorLookupRecord           `json:"corridor_lookup,omitempty"`
	CorridorOwnership           CorridorOwnershipDigest        `json:"corridor_ownership,omitempty"`
	CorridorReadiness           string                         `json:"corridor_readiness"`
	ReadinessConfidence         float64                        `json:"readiness_confidence"`
	BasisStale                  bool                           `json:"basis_stale,omitempty"`
	LastBasisEventAt            string                         `json:"last_basis_event_at,omitempty"`
	MetricsMissing              bool                           `json:"metrics_missing,omitempty"`
	Metrics                     ProtoClusterMetrics            `json:"metrics"`
	CatalogRangeCheck           CorridorFitCatalogRangeCheck   `json:"catalog_range_check,omitempty"`
	MetricVector                CorridorFitMetricVector        `json:"metric_vector"`
	FitStatus                   string                         `json:"fit_status"`
	FitConfidence               float64                        `json:"fit_confidence"`
	FitScore                    int                            `json:"fit_score"`
	BoundarySource              string                         `json:"boundary_source"`
	BasisState                  string                         `json:"basis_state"`
	BoundaryState               string                         `json:"boundary_state"`
	BoundaryConfidence          float64                        `json:"boundary_confidence"`
	MinNormalizedMargin         float64                        `json:"min_normalized_margin"`
	MaxViolationNormalizedDelta float64                        `json:"max_violation_normalized_delta"`
	NearestMetric               string                         `json:"nearest_metric,omitempty"`
	NearestBound                string                         `json:"nearest_bound,omitempty"`
	NearestDistance             float64                        `json:"nearest_distance"`
	NearBoundaryMetricCount     int                            `json:"near_boundary_metric_count"`
	OutsideMetricCount          int                            `json:"outside_metric_count"`
	CriticalViolationCount      int                            `json:"critical_violation_count"`
	DominantViolationMetric     string                         `json:"dominant_violation_metric,omitempty"`
	DominantViolationDirection  string                         `json:"dominant_violation_direction,omitempty"`
	NormalizedSeverity          float64                        `json:"normalized_severity"`
	BoundarySignals             []CorridorBoundaryMetricSignal `json:"boundary_signals,omitempty"`
	ConfirmedTensionCount       int                            `json:"confirmed_tension_count"`
	ConfirmedCountsByType       map[string]int                 `json:"confirmed_counts_by_type,omitempty"`
	ConfirmedTensionIDs         []string                       `json:"confirmed_tension_ids,omitempty"`
	Summary                     string                         `json:"summary,omitempty"`
}

type CorridorBoundaryWorkspaceMetrics struct {
	TotalClusters               int            `json:"total_clusters"`
	InRangeCount                int            `json:"in_range_count"`
	NearBoundaryCount           int            `json:"near_boundary_count"`
	ViolatedCount               int            `json:"violated_count"`
	ReadyBasisCount             int            `json:"ready_basis_count"`
	UnderEvidencedCount         int            `json:"under_evidenced_count"`
	StaleBasisCount             int            `json:"stale_basis_count"`
	TotalViolationSignals       int            `json:"total_violation_signals"`
	CriticalViolationSignals    int            `json:"critical_violation_signals"`
	BoundaryStateCounts         map[string]int `json:"boundary_state_counts,omitempty"`
	BasisStateCounts            map[string]int `json:"basis_state_counts,omitempty"`
	ViolationMetricCounts       map[string]int `json:"violation_metric_counts,omitempty"`
	DominantViolationMetric     string         `json:"dominant_violation_metric,omitempty"`
	DominantViolationMetricHits int            `json:"dominant_violation_metric_hits"`
}

type CorridorBoundaryReport struct {
	WorkspaceID   string                           `json:"workspace_id"`
	TimeAuthority WorkspaceTimeAuthority           `json:"time_authority"`
	GeneratedAt   string                           `json:"generated_at"`
	Filter        CorridorBoundaryFilter           `json:"filter"`
	Workspace     CorridorBoundaryWorkspaceMetrics `json:"workspace"`
	Clusters      []CorridorBoundaryClusterReport  `json:"clusters,omitempty"`
}

type CorridorBoundaryClusterDetail struct {
	TimeAuthority     WorkspaceTimeAuthority        `json:"time_authority"`
	Cluster           CorridorBoundaryClusterReport `json:"cluster"`
	Fit               CorridorFitClusterReport      `json:"fit"`
	ConfirmedTensions []TensionRecord               `json:"confirmed_tensions,omitempty"`
}

func normalizeCorridorBoundaryFilter(filter CorridorBoundaryFilter) CorridorBoundaryFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	if filter.Limit <= 0 {
		filter.Limit = 20
	}
	return filter
}

func (s *Store) BuildCorridorBoundaryReport(ctx context.Context, filter CorridorBoundaryFilter) (CorridorBoundaryReport, error) {
	filter = normalizeCorridorBoundaryFilter(filter)
	if filter.WorkspaceID == "" {
		return CorridorBoundaryReport{}, errors.New("workspace_id is required")
	}
	fullLimit := corridorReadClusterWindow
	if filter.Limit > fullLimit {
		fullLimit = filter.Limit
	}
	if filter.ProtoClusterID != "" {
		fullLimit = 1
	}
	fitReport, err := s.BuildCorridorFitReport(ctx, CorridorFitFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		Limit:          fullLimit,
	})
	if err != nil {
		return CorridorBoundaryReport{}, err
	}
	report := CorridorBoundaryReport{
		WorkspaceID:   filter.WorkspaceID,
		TimeAuthority: fitReport.TimeAuthority,
		GeneratedAt:   generatedAtFromWorkspaceTimeAuthority(fitReport.TimeAuthority),
		Filter:        filter,
		Workspace: CorridorBoundaryWorkspaceMetrics{
			BoundaryStateCounts:   map[string]int{},
			BasisStateCounts:      map[string]int{},
			ViolationMetricCounts: map[string]int{},
		},
	}
	for _, cluster := range fitReport.Clusters {
		item := buildCorridorBoundaryClusterReport(cluster)
		report.Clusters = append(report.Clusters, item)
		if item.BoundaryState != "" {
			report.Workspace.BoundaryStateCounts[item.BoundaryState]++
		}
		report.Workspace.BasisStateCounts[item.BasisState]++
		switch item.BoundaryState {
		case corridorBoundaryStateInRange:
			report.Workspace.InRangeCount++
		case corridorBoundaryStateNearBoundary:
			report.Workspace.NearBoundaryCount++
		case corridorBoundaryStateViolated:
			report.Workspace.ViolatedCount++
		}
		switch item.BasisState {
		case corridorBoundaryBasisStateReady:
			report.Workspace.ReadyBasisCount++
		case corridorBoundaryBasisStateStaleBasis:
			report.Workspace.StaleBasisCount++
		default:
			report.Workspace.UnderEvidencedCount++
		}
		report.Workspace.TotalViolationSignals += item.OutsideMetricCount
		report.Workspace.CriticalViolationSignals += item.CriticalViolationCount
		for _, signal := range item.BoundarySignals {
			if !corridorBoundaryIsViolationSignal(signal.Signal) {
				continue
			}
			report.Workspace.ViolationMetricCounts[signal.Metric]++
		}
	}
	report.Workspace.TotalClusters = len(report.Clusters)
	report.Workspace.DominantViolationMetric, report.Workspace.DominantViolationMetricHits = corridorBoundaryDominantViolationMetric(report.Workspace.ViolationMetricCounts)
	sort.Slice(report.Clusters, func(i, j int) bool {
		left := report.Clusters[i]
		right := report.Clusters[j]
		if corridorBoundaryStateRank(left.BoundaryState) != corridorBoundaryStateRank(right.BoundaryState) {
			return corridorBoundaryStateRank(left.BoundaryState) > corridorBoundaryStateRank(right.BoundaryState)
		}
		if corridorBoundaryBasisStateRank(left.BasisState) != corridorBoundaryBasisStateRank(right.BasisState) {
			return corridorBoundaryBasisStateRank(left.BasisState) > corridorBoundaryBasisStateRank(right.BasisState)
		}
		if left.CriticalViolationCount != right.CriticalViolationCount {
			return left.CriticalViolationCount > right.CriticalViolationCount
		}
		if left.MaxViolationNormalizedDelta != right.MaxViolationNormalizedDelta {
			return left.MaxViolationNormalizedDelta > right.MaxViolationNormalizedDelta
		}
		if left.MinNormalizedMargin != right.MinNormalizedMargin {
			return left.MinNormalizedMargin < right.MinNormalizedMargin
		}
		if left.BoundaryConfidence != right.BoundaryConfidence {
			return left.BoundaryConfidence > right.BoundaryConfidence
		}
		return left.ProtoClusterID < right.ProtoClusterID
	})
	if filter.Limit > 0 && len(report.Clusters) > filter.Limit {
		report.Clusters = append([]CorridorBoundaryClusterReport(nil), report.Clusters[:filter.Limit]...)
	}
	return report, nil
}

func (s *Store) BuildCorridorBoundaryClusterDetail(ctx context.Context, workspaceID, protoClusterID string) (CorridorBoundaryClusterDetail, error) {
	fitDetail, err := s.BuildCorridorFitClusterDetail(ctx, workspaceID, protoClusterID)
	if err != nil {
		return CorridorBoundaryClusterDetail{}, err
	}
	return CorridorBoundaryClusterDetail{
		TimeAuthority:     fitDetail.TimeAuthority,
		Cluster:           buildCorridorBoundaryClusterReport(fitDetail.Cluster),
		Fit:               fitDetail.Cluster,
		ConfirmedTensions: append([]TensionRecord(nil), fitDetail.ConfirmedTensions...),
	}, nil
}

func buildCorridorBoundaryClusterReport(fit CorridorFitClusterReport) CorridorBoundaryClusterReport {
	signals, minMargin, maxViolation, nearCount, violationCount, criticalCount := corridorBoundarySignalsForCluster(fit)
	basisState := corridorBoundaryBasisStateForCluster(fit)
	boundaryState := corridorBoundaryStateForCluster(fit, nearCount, violationCount)
	nearestMetric, nearestBound, nearestDistance := corridorBoundaryNearestSignal(signals)
	dominantViolationMetric, dominantViolationDirection := corridorBoundaryDominantViolation(signals)
	return CorridorBoundaryClusterReport{
		ProtoClusterID:              fit.ProtoClusterID,
		ResolutionKind:              fit.ResolutionKind,
		TaskIDs:                     append([]string{}, fit.TaskIDs...),
		SessionIDs:                  append([]string{}, fit.SessionIDs...),
		DocKeys:                     append([]string{}, fit.DocKeys...),
		ArtifactRefs:                append([]string{}, fit.ArtifactRefs...),
		AgentIDs:                    append([]string{}, fit.AgentIDs...),
		TaskClass:                   fit.TaskClass,
		TaskClassSource:             fit.TaskClassSource,
		TaskClassUpdatedAt:          fit.TaskClassUpdatedAt,
		TaskClassHint:               fit.TaskClassHint,
		CorridorCatalogHint:         fit.CorridorCatalogHint,
		CorridorLookup:              fit.CorridorLookup,
		CorridorOwnership:           fit.CorridorOwnership,
		CorridorReadiness:           fit.CorridorReadiness,
		ReadinessConfidence:         fit.ReadinessConfidence,
		BasisStale:                  fit.BasisStale,
		LastBasisEventAt:            fit.LastBasisEventAt,
		MetricsMissing:              fit.MetricsMissing,
		Metrics:                     fit.Metrics,
		CatalogRangeCheck:           fit.CatalogRangeCheck,
		MetricVector:                fit.MetricVector,
		FitStatus:                   fit.FitStatus,
		FitConfidence:               fit.FitConfidence,
		FitScore:                    fit.FitScore,
		BoundarySource:              corridorBoundarySourceFitDerived,
		BasisState:                  basisState,
		BoundaryState:               boundaryState,
		BoundaryConfidence:          fit.FitConfidence,
		MinNormalizedMargin:         minMargin,
		MaxViolationNormalizedDelta: maxViolation,
		NearestMetric:               nearestMetric,
		NearestBound:                nearestBound,
		NearestDistance:             nearestDistance,
		NearBoundaryMetricCount:     nearCount,
		OutsideMetricCount:          violationCount,
		CriticalViolationCount:      criticalCount,
		DominantViolationMetric:     dominantViolationMetric,
		DominantViolationDirection:  dominantViolationDirection,
		NormalizedSeverity:          corridorBoundaryNormalizedSeverity(boundaryState, minMargin, maxViolation),
		BoundarySignals:             signals,
		ConfirmedTensionCount:       fit.ConfirmedTensionCount,
		ConfirmedCountsByType:       cloneIntMap(fit.ConfirmedCountsByType),
		ConfirmedTensionIDs:         uniqueSortedStrings(fit.ConfirmedTensionIDs),
		Summary:                     corridorBoundarySummary(fit, basisState, boundaryState, signals, nearCount, violationCount),
	}
}

func corridorBoundarySignalsForCluster(fit CorridorFitClusterReport) ([]CorridorBoundaryMetricSignal, float64, float64, int, int, int) {
	if fit.FitStatus == corridorFitStatusUnderEvidenced || fit.FitStatus == corridorFitStatusStaleBasis {
		return nil, 0, 0, 0, 0, 0
	}
	gapByMetric := map[string]CorridorFitMetricGap{}
	for _, gap := range fit.MetricGapBreakdown {
		gapByMetric[strings.TrimSpace(gap.Metric)] = gap
	}
	signals := []CorridorBoundaryMetricSignal{}
	minMargin := 1.0
	maxViolation := 0.0
	nearCount := 0
	violationCount := 0
	criticalCount := 0
	for _, rule := range fit.CatalogRangeCheck.Ranges {
		gap, ok := gapByMetric[strings.TrimSpace(rule.Metric)]
		if !ok {
			gap = CorridorFitMetricGap{
				Metric:     rule.Metric,
				Value:      corridorFitMetricValue(fit.MetricVector, rule.Metric),
				LowerBound: corridorFitCloneBound(rule.LowerBound),
				UpperBound: corridorFitCloneBound(rule.UpperBound),
				Status:     corridorBoundarySignalInRange,
			}
		}
		if signal, ok := corridorBoundaryViolationSignal(gap); ok {
			signals = append(signals, signal)
			violationCount++
			if signal.Severity == corridorBoundarySeverityCritical {
				criticalCount++
			}
			maxViolation = maxFloat(maxViolation, signal.NormalizedDelta)
			minMargin = 0
			continue
		}
		if signal, ok := corridorBoundaryNearSignal(gap); ok {
			signals = append(signals, signal)
			nearCount++
			minMargin = minFloat(minMargin, signal.NormalizedDelta)
			continue
		}
		margin := corridorBoundaryNormalizedMargin(gap)
		if margin >= 0 {
			minMargin = minFloat(minMargin, margin)
		}
	}
	if minMargin == 1.0 && len(fit.CatalogRangeCheck.Ranges) == 0 {
		minMargin = 0
	}
	sort.Slice(signals, func(i, j int) bool {
		if corridorBoundarySignalRank(signals[i]) != corridorBoundarySignalRank(signals[j]) {
			return corridorBoundarySignalRank(signals[i]) > corridorBoundarySignalRank(signals[j])
		}
		if signals[i].NormalizedDelta != signals[j].NormalizedDelta {
			return signals[i].NormalizedDelta > signals[j].NormalizedDelta
		}
		return signals[i].Metric < signals[j].Metric
	})
	return signals, minMargin, maxViolation, nearCount, violationCount, criticalCount
}

func corridorBoundaryViolationSignal(gap CorridorFitMetricGap) (CorridorBoundaryMetricSignal, bool) {
	if gap.Status == "IN_RANGE" {
		return CorridorBoundaryMetricSignal{}, false
	}
	width := corridorBoundaryRangeWidth(gap.LowerBound, gap.UpperBound)
	normalized := mathAbs(gap.Delta) / width
	signal := corridorBoundarySignalLowViolation
	if gap.Status == "HIGH" {
		signal = corridorBoundarySignalHighViolation
	}
	severity := corridorBoundarySeverityWatch
	switch {
	case normalized >= corridorBoundaryCriticalThreshold:
		severity = corridorBoundarySeverityCritical
	case normalized >= corridorBoundaryWarningThreshold:
		severity = corridorBoundarySeverityWarning
	}
	direction := "below lower bound"
	if signal == corridorBoundarySignalHighViolation {
		direction = "above upper bound"
	}
	return CorridorBoundaryMetricSignal{
		Metric:          gap.Metric,
		Value:           gap.Value,
		LowerBound:      corridorFitCloneBound(gap.LowerBound),
		UpperBound:      corridorFitCloneBound(gap.UpperBound),
		Signal:          signal,
		Severity:        severity,
		Delta:           gap.Delta,
		NormalizedDelta: normalized,
		Summary:         fmt.Sprintf("%s sits %.2f %s", gap.Metric, mathAbs(gap.Delta), direction),
	}, true
}

func corridorBoundaryNearSignal(gap CorridorFitMetricGap) (CorridorBoundaryMetricSignal, bool) {
	if gap.Status != "IN_RANGE" {
		return CorridorBoundaryMetricSignal{}, false
	}
	signal, margin, ok := corridorBoundaryNearSignalKind(gap)
	if !ok {
		return CorridorBoundaryMetricSignal{}, false
	}
	severity := corridorBoundarySeverityWatch
	if margin <= corridorBoundaryWarningThreshold {
		severity = corridorBoundarySeverityWarning
	}
	direction := "lower"
	if signal == corridorBoundarySignalNearUpper {
		direction = "upper"
	}
	return CorridorBoundaryMetricSignal{
		Metric:          gap.Metric,
		Value:           gap.Value,
		LowerBound:      corridorFitCloneBound(gap.LowerBound),
		UpperBound:      corridorFitCloneBound(gap.UpperBound),
		Signal:          signal,
		Severity:        severity,
		Delta:           0,
		NormalizedDelta: margin,
		Summary:         fmt.Sprintf("%s sits near the %s boundary", gap.Metric, direction),
	}, true
}

func corridorBoundaryNearSignalKind(gap CorridorFitMetricGap) (string, float64, bool) {
	width := corridorBoundaryRangeWidth(gap.LowerBound, gap.UpperBound)
	bestSignal := ""
	bestMargin := 0.0
	if gap.LowerBound != nil {
		margin := (gap.Value - *gap.LowerBound) / width
		if margin >= 0 && margin <= corridorBoundaryNearThreshold {
			bestSignal = corridorBoundarySignalNearLower
			bestMargin = margin
		}
	}
	if gap.UpperBound != nil {
		margin := (*gap.UpperBound - gap.Value) / width
		if margin >= 0 && margin <= corridorBoundaryNearThreshold && (bestSignal == "" || margin < bestMargin) {
			bestSignal = corridorBoundarySignalNearUpper
			bestMargin = margin
		}
	}
	if bestSignal == "" {
		return "", 0, false
	}
	return bestSignal, bestMargin, true
}

func corridorBoundaryNormalizedMargin(gap CorridorFitMetricGap) float64 {
	if gap.Status != "IN_RANGE" {
		return -1
	}
	width := corridorBoundaryRangeWidth(gap.LowerBound, gap.UpperBound)
	values := []float64{}
	if gap.LowerBound != nil {
		values = append(values, (gap.Value-*gap.LowerBound)/width)
	}
	if gap.UpperBound != nil {
		values = append(values, (*gap.UpperBound-gap.Value)/width)
	}
	if len(values) == 0 {
		return -1
	}
	best := values[0]
	for _, value := range values[1:] {
		if value < best {
			best = value
		}
	}
	return best
}

func corridorBoundaryRangeWidth(lower, upper *float64) float64 {
	switch {
	case lower != nil && upper != nil && *upper > *lower:
		return *upper - *lower
	case lower != nil || upper != nil:
		return 1.0
	default:
		return 1.0
	}
}

func corridorBoundaryBasisStateForCluster(fit CorridorFitClusterReport) string {
	switch fit.FitStatus {
	case corridorFitStatusStaleBasis:
		return corridorBoundaryBasisStateStaleBasis
	case corridorFitStatusUnderEvidenced:
		return corridorBoundaryBasisStateUnderEvidenced
	default:
		return corridorBoundaryBasisStateReady
	}
}

func corridorBoundaryStateForCluster(fit CorridorFitClusterReport, nearCount, violationCount int) string {
	switch fit.FitStatus {
	case corridorFitStatusStaleBasis, corridorFitStatusUnderEvidenced:
		return ""
	}
	switch {
	case violationCount > 0 || fit.FitStatus == corridorFitStatusOutOfCorridor:
		return corridorBoundaryStateViolated
	case nearCount > 0 || fit.FitStatus == corridorFitStatusNearBoundary:
		return corridorBoundaryStateNearBoundary
	default:
		return corridorBoundaryStateInRange
	}
}

func corridorBoundarySummary(fit CorridorFitClusterReport, basisState, boundaryState string, signals []CorridorBoundaryMetricSignal, nearCount, violationCount int) string {
	subject := firstNonEmpty(strings.TrimSpace(fit.ProtoClusterID), "cluster")
	switch basisState {
	case corridorBoundaryBasisStateStaleBasis:
		return fmt.Sprintf("%s keeps the current corridor-boundary approximation, but the task-class basis is stale and should be refreshed first", subject)
	case corridorBoundaryBasisStateUnderEvidenced:
		return fmt.Sprintf("%s does not yet have enough stable task-class evidence for corridor-boundary diagnosis", subject)
	}
	switch boundaryState {
	case corridorBoundaryStateInRange:
		return fmt.Sprintf("%s stays within the current corridor-boundary approximation", subject)
	case corridorBoundaryStateNearBoundary:
		names := corridorBoundarySignalMetricNames(signals, false)
		if len(names) == 0 {
			return fmt.Sprintf("%s sits near one or more current corridor boundaries", subject)
		}
		return fmt.Sprintf("%s sits near the current corridor boundary on %s", subject, strings.Join(names, ", "))
	default:
		names := corridorBoundarySignalMetricNames(signals, true)
		if len(names) == 0 {
			return fmt.Sprintf("%s approximates a corridor-boundary violation on one or more metrics", subject)
		}
		if violationCount == 1 {
			return fmt.Sprintf("%s approximates a corridor-boundary violation on %s", subject, names[0])
		}
		return fmt.Sprintf("%s approximates corridor-boundary violations on %s", subject, strings.Join(names, ", "))
	}
}

func corridorBoundaryStateRank(status string) int {
	switch strings.TrimSpace(status) {
	case corridorBoundaryStateViolated:
		return 5
	case corridorBoundaryStateNearBoundary:
		return 4
	case corridorBoundaryStateInRange:
		return 1
	default:
		return 0
	}
}

func corridorBoundaryBasisStateRank(status string) int {
	switch strings.TrimSpace(status) {
	case corridorBoundaryBasisStateStaleBasis:
		return 3
	case corridorBoundaryBasisStateUnderEvidenced:
		return 2
	case corridorBoundaryBasisStateReady:
		return 1
	default:
		return 0
	}
}

func corridorBoundarySignalRank(signal CorridorBoundaryMetricSignal) int {
	switch signal.Signal {
	case corridorBoundarySignalHighViolation, corridorBoundarySignalLowViolation:
		if signal.Severity == corridorBoundarySeverityCritical {
			return 5
		}
		if signal.Severity == corridorBoundarySeverityWarning {
			return 4
		}
		return 3
	case corridorBoundarySignalNearLower, corridorBoundarySignalNearUpper:
		if signal.Severity == corridorBoundarySeverityWarning {
			return 2
		}
		return 1
	default:
		return 0
	}
}

func corridorBoundaryIsViolationSignal(signal string) bool {
	switch strings.TrimSpace(signal) {
	case corridorBoundarySignalLowViolation, corridorBoundarySignalHighViolation:
		return true
	default:
		return false
	}
}

func corridorBoundarySignalMetricNames(signals []CorridorBoundaryMetricSignal, onlyViolations bool) []string {
	names := []string{}
	for _, signal := range signals {
		if onlyViolations && !corridorBoundaryIsViolationSignal(signal.Signal) {
			continue
		}
		if !onlyViolations && corridorBoundaryIsViolationSignal(signal.Signal) {
			continue
		}
		if metric := strings.TrimSpace(signal.Metric); metric != "" {
			names = append(names, metric)
		}
	}
	return uniqueSortedStrings(names)
}

func corridorBoundaryDominantViolationMetric(counts map[string]int) (string, int) {
	best := ""
	bestCount := 0
	for _, key := range []string{"alignment", "differentiation", "synergy", "centralization", "metastability", "progress"} {
		count := counts[key]
		if count > bestCount {
			best = key
			bestCount = count
		}
	}
	return best, bestCount
}

func corridorBoundaryNearestSignal(signals []CorridorBoundaryMetricSignal) (string, string, float64) {
	bestMetric := ""
	bestBound := ""
	bestDistance := 0.0
	found := false
	for _, signal := range signals {
		distance := signal.NormalizedDelta
		bound := "LOWER"
		switch signal.Signal {
		case corridorBoundarySignalNearUpper, corridorBoundarySignalHighViolation:
			bound = "UPPER"
		}
		if !found || distance < bestDistance {
			bestMetric = strings.TrimSpace(signal.Metric)
			bestBound = bound
			bestDistance = distance
			found = true
		}
	}
	return bestMetric, bestBound, bestDistance
}

func corridorBoundaryDominantViolation(signals []CorridorBoundaryMetricSignal) (string, string) {
	bestMetric := ""
	bestDirection := ""
	bestSeverity := 0.0
	for _, signal := range signals {
		if !corridorBoundaryIsViolationSignal(signal.Signal) {
			continue
		}
		if signal.NormalizedDelta > bestSeverity {
			bestMetric = strings.TrimSpace(signal.Metric)
			if signal.Signal == corridorBoundarySignalHighViolation {
				bestDirection = "UPPER"
			} else {
				bestDirection = "LOWER"
			}
			bestSeverity = signal.NormalizedDelta
		}
	}
	return bestMetric, bestDirection
}

func corridorBoundaryNormalizedSeverity(boundaryState string, minMargin, maxViolation float64) float64 {
	switch strings.TrimSpace(boundaryState) {
	case corridorBoundaryStateViolated:
		return maxViolation
	case corridorBoundaryStateNearBoundary:
		if minMargin < 0 {
			return 0
		}
		return corridorClampUnit(1 - minFloat(minMargin/corridorBoundaryNearThreshold, 1))
	default:
		return 0
	}
}

func mathAbs(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}
