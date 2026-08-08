package sqlite

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	corridorFitTensionWindow = 1000

	corridorFitStatusInCorridor     = "IN_CORRIDOR"
	corridorFitStatusNearBoundary   = "NEAR_BOUNDARY"
	corridorFitStatusOutOfCorridor  = "OUT_OF_CORRIDOR"
	corridorFitStatusUnderEvidenced = "UNDER_EVIDENCED"
	corridorFitStatusStaleBasis     = "STALE_BASIS"
)

type CorridorFitFilter struct {
	WorkspaceID    string `json:"workspace_id"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type CorridorFitMetricVector struct {
	Alignment       float64 `json:"alignment"`
	Differentiation float64 `json:"differentiation"`
	Synergy         float64 `json:"synergy"`
	Centralization  float64 `json:"centralization"`
	Metastability   float64 `json:"metastability"`
	Progress        float64 `json:"progress"`
}

type CorridorFitMetricRange struct {
	Metric     string   `json:"metric"`
	LowerBound *float64 `json:"lower_bound,omitempty"`
	UpperBound *float64 `json:"upper_bound,omitempty"`
}

type CorridorFitCatalogRangeCheck struct {
	CatalogKey   string                   `json:"catalog_key,omitempty"`
	DisplayName  string                   `json:"display_name,omitempty"`
	TaskClass    string                   `json:"task_class,omitempty"`
	MatchSource  string                   `json:"match_source,omitempty"`
	Ranges       []CorridorFitMetricRange `json:"ranges,omitempty"`
	BasisFresh   bool                     `json:"basis_fresh"`
	BasisSummary string                   `json:"basis_summary,omitempty"`
}

type CorridorFitMetricGap struct {
	Metric     string   `json:"metric"`
	Value      float64  `json:"value"`
	LowerBound *float64 `json:"lower_bound,omitempty"`
	UpperBound *float64 `json:"upper_bound,omitempty"`
	Delta      float64  `json:"delta"`
	Status     string   `json:"status"`
}

type CorridorFitClusterReport struct {
	ProtoClusterID        string                       `json:"proto_cluster_id"`
	ResolutionKind        string                       `json:"resolution_kind"`
	TaskIDs               []string                     `json:"task_ids,omitempty"`
	SessionIDs            []string                     `json:"session_ids,omitempty"`
	DocKeys               []string                     `json:"doc_keys,omitempty"`
	ArtifactRefs          []string                     `json:"artifact_refs,omitempty"`
	AgentIDs              []string                     `json:"agent_ids,omitempty"`
	TaskClass             string                       `json:"task_class,omitempty"`
	TaskClassSource       string                       `json:"task_class_source,omitempty"`
	TaskClassUpdatedAt    string                       `json:"task_class_updated_at,omitempty"`
	TaskClassHint         string                       `json:"task_class_hint"`
	CorridorCatalogHint   string                       `json:"corridor_catalog_hint,omitempty"`
	CorridorLookup        CorridorLookupRecord         `json:"corridor_lookup,omitempty"`
	CorridorOwnership     CorridorOwnershipDigest      `json:"corridor_ownership,omitempty"`
	CorridorReadiness     string                       `json:"corridor_readiness"`
	ReadinessConfidence   float64                      `json:"readiness_confidence"`
	BasisStale            bool                         `json:"basis_stale,omitempty"`
	LastBasisEventAt      string                       `json:"last_basis_event_at,omitempty"`
	MetricsMissing        bool                         `json:"metrics_missing,omitempty"`
	Metrics               ProtoClusterMetrics          `json:"metrics"`
	CatalogRangeCheck     CorridorFitCatalogRangeCheck `json:"catalog_range_check,omitempty"`
	MetricVector          CorridorFitMetricVector      `json:"metric_vector"`
	MetricGapBreakdown    []CorridorFitMetricGap       `json:"metric_gap_breakdown,omitempty"`
	FitStatus             string                       `json:"fit_status"`
	FitConfidence         float64                      `json:"fit_confidence"`
	FitScore              int                          `json:"fit_score"`
	ConfirmedTensionCount int                          `json:"confirmed_tension_count"`
	ConfirmedCountsByType map[string]int               `json:"confirmed_counts_by_type,omitempty"`
	ConfirmedTensionIDs   []string                     `json:"confirmed_tension_ids,omitempty"`
	Summary               string                       `json:"summary,omitempty"`
}

type CorridorFitWorkspaceMetrics struct {
	TotalClusters          int            `json:"total_clusters"`
	InCorridorCount        int            `json:"in_corridor_count"`
	NearBoundaryCount      int            `json:"near_boundary_count"`
	OutOfCorridorCount     int            `json:"out_of_corridor_count"`
	UnderEvidencedCount    int            `json:"under_evidenced_count"`
	StaleBasisCount        int            `json:"stale_basis_count"`
	DominantCatalogKey     string         `json:"dominant_catalog_key,omitempty"`
	DominantCatalogKeyHits int            `json:"dominant_catalog_key_hits"`
	FitStatusCounts        map[string]int `json:"fit_status_counts,omitempty"`
	CatalogKeyCounts       map[string]int `json:"catalog_key_counts,omitempty"`
}

type CorridorFitReport struct {
	WorkspaceID   string                      `json:"workspace_id"`
	TimeAuthority WorkspaceTimeAuthority      `json:"time_authority"`
	GeneratedAt   string                      `json:"generated_at"`
	Filter        CorridorFitFilter           `json:"filter"`
	Workspace     CorridorFitWorkspaceMetrics `json:"workspace"`
	Clusters      []CorridorFitClusterReport  `json:"clusters,omitempty"`
}

type CorridorFitClusterDetail struct {
	TimeAuthority     WorkspaceTimeAuthority   `json:"time_authority"`
	Cluster           CorridorFitClusterReport `json:"cluster"`
	ConfirmedTensions []TensionRecord          `json:"confirmed_tensions,omitempty"`
}

type CorridorFitSnapshotInput struct {
	ActorID string
	Limit   int
}

type corridorFitCatalogRule struct {
	Metric string
	Lower  *float64
	Upper  *float64
}

type corridorFitCatalogProfile struct {
	TaskClass   string
	CatalogKey  string
	DisplayName string
	Rules       []corridorFitCatalogRule
}

var corridorFitCatalogProfiles = []corridorFitCatalogProfile{
	{
		TaskClass:   taskClassHintProof,
		CatalogKey:  "proof",
		DisplayName: "Proof Corridor",
		Rules: []corridorFitCatalogRule{
			{Metric: "alignment", Lower: corridorFitBound(0.55), Upper: corridorFitBound(0.80)},
			{Metric: "differentiation", Lower: corridorFitBound(0.35), Upper: corridorFitBound(0.65)},
			{Metric: "synergy", Lower: corridorFitBound(0.05)},
			{Metric: "centralization", Upper: corridorFitBound(0.45)},
			{Metric: "metastability", Lower: corridorFitBound(0.10), Upper: corridorFitBound(0.35)},
			{Metric: "progress", Lower: corridorFitBound(0.01)},
		},
	},
	{
		TaskClass:   taskClassHintExploration,
		CatalogKey:  "exploration",
		DisplayName: "Exploration Corridor",
		Rules: []corridorFitCatalogRule{
			{Metric: "alignment", Lower: corridorFitBound(0.25), Upper: corridorFitBound(0.55)},
			{Metric: "differentiation", Lower: corridorFitBound(0.60), Upper: corridorFitBound(0.90)},
			{Metric: "synergy", Lower: corridorFitBound(0.03)},
			{Metric: "centralization", Upper: corridorFitBound(0.40)},
			{Metric: "metastability", Lower: corridorFitBound(0.35), Upper: corridorFitBound(0.70)},
			{Metric: "progress", Lower: corridorFitBound(0.01)},
		},
	},
	{
		TaskClass:   taskClassHintIntegration,
		CatalogKey:  "integration",
		DisplayName: "Integration Corridor",
		Rules: []corridorFitCatalogRule{
			{Metric: "alignment", Lower: corridorFitBound(0.50), Upper: corridorFitBound(0.75)},
			{Metric: "differentiation", Lower: corridorFitBound(0.40), Upper: corridorFitBound(0.70)},
			{Metric: "synergy", Lower: corridorFitBound(0.04)},
			{Metric: "centralization", Upper: corridorFitBound(0.45)},
			{Metric: "metastability", Lower: corridorFitBound(0.20), Upper: corridorFitBound(0.45)},
			{Metric: "progress", Lower: corridorFitBound(0.01)},
		},
	},
	{
		TaskClass:   taskClassHintIncident,
		CatalogKey:  "incident",
		DisplayName: "Incident Corridor",
		Rules: []corridorFitCatalogRule{
			{Metric: "alignment", Lower: corridorFitBound(0.65), Upper: corridorFitBound(0.85)},
			{Metric: "differentiation", Lower: corridorFitBound(0.20), Upper: corridorFitBound(0.50)},
			{Metric: "synergy", Lower: corridorFitBound(0.02)},
			{Metric: "centralization", Upper: corridorFitBound(0.50)},
			{Metric: "metastability", Lower: corridorFitBound(0.05), Upper: corridorFitBound(0.25)},
			{Metric: "progress", Lower: corridorFitBound(0.01)},
		},
	},
}

func normalizeCorridorFitFilter(filter CorridorFitFilter) CorridorFitFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	if filter.Limit <= 0 {
		filter.Limit = 20
	}
	return filter
}

func (s *Store) BuildCorridorFitReport(ctx context.Context, filter CorridorFitFilter) (CorridorFitReport, error) {
	filter = normalizeCorridorFitFilter(filter)
	if filter.WorkspaceID == "" {
		return CorridorFitReport{}, errors.New("workspace_id is required")
	}
	fullLimit := corridorReadClusterWindow
	if filter.Limit > fullLimit {
		fullLimit = filter.Limit
	}
	if filter.ProtoClusterID != "" {
		fullLimit = 1
	}
	readiness, err := s.BuildCorridorReadinessReport(ctx, CorridorReadinessFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		Limit:          fullLimit,
	})
	if err != nil {
		return CorridorFitReport{}, err
	}
	ownershipReport, err := s.BuildCorridorOwnershipReport(ctx, CorridorOwnershipFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		Limit:          fullLimit,
	})
	if err != nil {
		return CorridorFitReport{}, err
	}
	ownershipByCluster := map[string]CorridorOwnershipDigest{}
	for _, cluster := range ownershipReport.Clusters {
		ownershipByCluster[strings.TrimSpace(cluster.ProtoClusterID)] = cluster.Ownership
	}
	tensions, err := s.listAllTensions(ctx, TensionFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		Limit:          corridorFitTensionWindow,
	})
	if err != nil {
		return CorridorFitReport{}, err
	}
	confirmedByCluster := map[string][]TensionRecord{}
	for _, tension := range tensions {
		clusterID := strings.TrimSpace(tension.ProtoClusterID)
		if clusterID == "" {
			continue
		}
		if strings.TrimSpace(tension.LifecycleState) == tensionLifecycleArchived || strings.TrimSpace(tension.ReviewStatus) != tensionReviewConfirmed {
			continue
		}
		confirmedByCluster[clusterID] = append(confirmedByCluster[clusterID], tension)
	}
	report := CorridorFitReport{
		WorkspaceID:   filter.WorkspaceID,
		TimeAuthority: ownershipReport.TimeAuthority,
		GeneratedAt:   generatedAtFromWorkspaceTimeAuthority(ownershipReport.TimeAuthority),
		Filter:        filter,
		Workspace: CorridorFitWorkspaceMetrics{
			FitStatusCounts:  map[string]int{},
			CatalogKeyCounts: map[string]int{},
		},
	}
	for _, cluster := range readiness.Clusters {
		report.Clusters = append(report.Clusters, buildCorridorFitClusterReport(cluster, confirmedByCluster[strings.TrimSpace(cluster.ProtoClusterID)], ownershipByCluster[strings.TrimSpace(cluster.ProtoClusterID)]))
	}
	sort.Slice(report.Clusters, func(i, j int) bool {
		left := report.Clusters[i]
		right := report.Clusters[j]
		if corridorFitStatusRank(left.FitStatus) != corridorFitStatusRank(right.FitStatus) {
			return corridorFitStatusRank(left.FitStatus) > corridorFitStatusRank(right.FitStatus)
		}
		if left.FitScore != right.FitScore {
			return left.FitScore < right.FitScore
		}
		if left.FitConfidence != right.FitConfidence {
			return left.FitConfidence > right.FitConfidence
		}
		return left.ProtoClusterID < right.ProtoClusterID
	})
	report.Workspace.TotalClusters = len(report.Clusters)
	for _, cluster := range report.Clusters {
		report.Workspace.FitStatusCounts[cluster.FitStatus]++
		switch cluster.FitStatus {
		case corridorFitStatusInCorridor:
			report.Workspace.InCorridorCount++
		case corridorFitStatusNearBoundary:
			report.Workspace.NearBoundaryCount++
		case corridorFitStatusOutOfCorridor:
			report.Workspace.OutOfCorridorCount++
		case corridorFitStatusStaleBasis:
			report.Workspace.StaleBasisCount++
		default:
			report.Workspace.UnderEvidencedCount++
		}
		if key := strings.TrimSpace(cluster.CatalogRangeCheck.CatalogKey); key != "" {
			report.Workspace.CatalogKeyCounts[key]++
		}
	}
	report.Workspace.DominantCatalogKey, report.Workspace.DominantCatalogKeyHits = corridorFitDominantCatalogKey(report.Workspace.CatalogKeyCounts)
	if filter.Limit > 0 && len(report.Clusters) > filter.Limit {
		report.Clusters = append([]CorridorFitClusterReport(nil), report.Clusters[:filter.Limit]...)
	}
	return report, nil
}

func (s *Store) BuildCorridorFitClusterDetail(ctx context.Context, workspaceID, protoClusterID string) (CorridorFitClusterDetail, error) {
	report, err := s.BuildCorridorFitReport(ctx, CorridorFitFilter{
		WorkspaceID:    strings.TrimSpace(workspaceID),
		ProtoClusterID: strings.TrimSpace(protoClusterID),
		Limit:          1,
	})
	if err != nil {
		return CorridorFitClusterDetail{}, err
	}
	if len(report.Clusters) == 0 {
		return CorridorFitClusterDetail{}, fmt.Errorf("corridor fit cluster not found: %s/%s", strings.TrimSpace(workspaceID), strings.TrimSpace(protoClusterID))
	}
	tensions, err := s.listAllTensions(ctx, TensionFilter{
		WorkspaceID:    strings.TrimSpace(workspaceID),
		ProtoClusterID: strings.TrimSpace(protoClusterID),
		Limit:          corridorFitTensionWindow,
	})
	if err != nil {
		return CorridorFitClusterDetail{}, err
	}
	confirmed := make([]TensionRecord, 0, len(tensions))
	for _, tension := range tensions {
		if strings.TrimSpace(tension.LifecycleState) == tensionLifecycleArchived || strings.TrimSpace(tension.ReviewStatus) != tensionReviewConfirmed {
			continue
		}
		confirmed = append(confirmed, tension)
	}
	sort.Slice(confirmed, func(i, j int) bool {
		left := confirmed[i]
		right := confirmed[j]
		if left.SurfaceScore != right.SurfaceScore {
			return left.SurfaceScore > right.SurfaceScore
		}
		return left.TensionID < right.TensionID
	})
	return CorridorFitClusterDetail{
		TimeAuthority:     report.TimeAuthority,
		Cluster:           report.Clusters[0],
		ConfirmedTensions: confirmed,
	}, nil
}

func (s *Store) RecordCorridorFitSnapshot(ctx context.Context, report CorridorFitReport, input CorridorFitSnapshotInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(report.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		actorID = "corridor.fit.snapshot"
	}
	clusters := append([]CorridorFitClusterReport(nil), report.Clusters...)
	if input.Limit > 0 && len(clusters) > input.Limit {
		clusters = append([]CorridorFitClusterReport(nil), clusters[:input.Limit]...)
	}
	payload := map[string]any{
		"generated_at":     report.GeneratedAt,
		"workspace_id":     report.WorkspaceID,
		"filter":           report.Filter,
		"workspace":        report.Workspace,
		"clusters":         clusters,
		"summary":          corridorFitSnapshotSummary(report, clusters),
		"typed_event_type": "CORRIDOR_FIT_SNAPSHOT",
	}
	return s.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "cluster.corridor_fit_snapshot",
		EntityType:  "instrumentation_corridor_fit",
		EntityID:    corridorFitSnapshotEntityID(report.Filter),
		ActorType:   "operator",
		ActorID:     actorID,
		SessionID:   corridorFitSnapshotSessionID(report),
		TaskID:      corridorFitSnapshotTaskID(report),
		PayloadJSON: mustJSON(payload),
		CreatedAt:   referenceAt,
	})
}

func buildCorridorFitClusterReport(cluster CorridorClusterReport, tensions []TensionRecord, ownership CorridorOwnershipDigest) CorridorFitClusterReport {
	confirmedCounts := map[string]int{}
	confirmedIDs := []string{}
	for _, tension := range tensions {
		confirmedCounts[tension.TensionType]++
		confirmedIDs = append(confirmedIDs, tension.TensionID)
	}
	vector := corridorFitMetricVectorForCluster(cluster, confirmedCounts)
	rangeCheck, ok := corridorFitCatalogRangeCheckForCluster(cluster, ownership)
	gaps := []CorridorFitMetricGap{}
	fitStatus := corridorFitStatusUnderEvidenced
	fitConfidence := corridorFitConfidenceForCluster(cluster, ok)
	fitScore := 0
	metricsMissing := cluster.Metrics.EventCount == 0 && strings.TrimSpace(cluster.Metrics.LastEventAt) == ""
	switch {
	case cluster.BasisStale:
		fitStatus = corridorFitStatusStaleBasis
	case !ok || cluster.CorridorReadiness == corridorReadinessMixed || cluster.CorridorLookup.LookupStatus == corridorLookupStatusAmbiguous:
		fitStatus = corridorFitStatusUnderEvidenced
	case metricsMissing || cluster.CorridorReadiness == corridorReadinessUnderEvidenced:
		fitStatus = corridorFitStatusUnderEvidenced
	default:
		gaps = corridorFitGapBreakdown(vector, rangeCheck.Ranges)
		fitStatus = corridorFitStatusForGaps(gaps)
		fitScore = corridorFitScoreForCluster(gaps, fitConfidence)
	}
	if fitStatus == corridorFitStatusStaleBasis || fitStatus == corridorFitStatusUnderEvidenced {
		fitScore = clampInt(int(fitConfidence*55), 0, 55)
	}
	return CorridorFitClusterReport{
		ProtoClusterID:        cluster.ProtoClusterID,
		ResolutionKind:        cluster.ResolutionKind,
		TaskIDs:               append([]string{}, cluster.TaskIDs...),
		SessionIDs:            append([]string{}, cluster.SessionIDs...),
		DocKeys:               append([]string{}, cluster.DocKeys...),
		ArtifactRefs:          append([]string{}, cluster.ArtifactRefs...),
		AgentIDs:              append([]string{}, cluster.AgentIDs...),
		TaskClass:             cluster.TaskClass,
		TaskClassSource:       cluster.TaskClassSource,
		TaskClassUpdatedAt:    cluster.TaskClassUpdatedAt,
		TaskClassHint:         cluster.TaskClassHint,
		CorridorCatalogHint:   cluster.CorridorCatalogHint,
		CorridorLookup:        cluster.CorridorLookup,
		CorridorOwnership:     ownership,
		CorridorReadiness:     cluster.CorridorReadiness,
		ReadinessConfidence:   cluster.ReadinessConfidence,
		BasisStale:            cluster.BasisStale,
		LastBasisEventAt:      cluster.LastBasisEventAt,
		MetricsMissing:        metricsMissing,
		Metrics:               cluster.Metrics,
		CatalogRangeCheck:     rangeCheck,
		MetricVector:          vector,
		MetricGapBreakdown:    gaps,
		FitStatus:             fitStatus,
		FitConfidence:         fitConfidence,
		FitScore:              fitScore,
		ConfirmedTensionCount: len(confirmedIDs),
		ConfirmedCountsByType: cloneIntMap(confirmedCounts),
		ConfirmedTensionIDs:   uniqueSortedStrings(confirmedIDs),
		Summary:               corridorFitSummary(cluster, rangeCheck, fitStatus, gaps, confirmedCounts),
	}
}

func corridorFitCatalogRangeCheckForCluster(cluster CorridorClusterReport, ownership CorridorOwnershipDigest) (CorridorFitCatalogRangeCheck, bool) {
	taskClass := normalizeTaskClassHint(firstNonEmpty(ownership.BasisTaskClass, cluster.TaskClass, cluster.TaskClassHint))
	matchSource := strings.TrimSpace(cluster.CorridorLookup.MatchSource)
	if strings.TrimSpace(ownership.OwnershipState) != "" {
		matchSource = "corridor_ownership:" + strings.ToLower(strings.TrimSpace(ownership.OwnershipState))
	}
	if cluster.MixedTaskClasses || strings.TrimSpace(cluster.CorridorLookup.LookupStatus) == corridorLookupStatusAmbiguous {
		return CorridorFitCatalogRangeCheck{
			BasisFresh:   false,
			BasisSummary: "mixed task-class evidence prevents a stable corridor fit profile",
		}, false
	}
	profile, ok := corridorFitCatalogProfileForTaskClass(taskClass)
	if !ok {
		return CorridorFitCatalogRangeCheck{
			BasisFresh:   !cluster.BasisStale,
			BasisSummary: "no corridor catalog profile is available for the current task-class basis",
		}, false
	}
	ranges := make([]CorridorFitMetricRange, 0, len(profile.Rules))
	for _, rule := range profile.Rules {
		ranges = append(ranges, CorridorFitMetricRange{
			Metric:     rule.Metric,
			LowerBound: corridorFitCloneBound(rule.Lower),
			UpperBound: corridorFitCloneBound(rule.Upper),
		})
	}
	basisSummary := "catalog lookup is backed by the current corridor ownership basis"
	if strings.TrimSpace(ownership.OwnershipState) == corridorOwnershipStateUnresolved {
		basisSummary = "corridor ownership is unresolved, so corridor fit remains a weak approximation"
	}
	if cluster.BasisStale || strings.TrimSpace(ownership.OwnershipState) == corridorOwnershipStateOwnedExplicitStale {
		basisSummary = "catalog lookup is currently stale relative to the latest task-class basis"
	}
	if matchSource == "" {
		matchSource = "corridor_lookup"
	}
	return CorridorFitCatalogRangeCheck{
		CatalogKey:   profile.CatalogKey,
		DisplayName:  profile.DisplayName,
		TaskClass:    profile.TaskClass,
		MatchSource:  matchSource,
		Ranges:       ranges,
		BasisFresh:   !cluster.BasisStale,
		BasisSummary: basisSummary,
	}, true
}

func corridorFitMetricVectorForCluster(cluster CorridorClusterReport, confirmedCounts map[string]int) CorridorFitMetricVector {
	metrics := cluster.Metrics
	entropy := corridorFitNormalizedEntropy(metrics.ActivityShareByAgent)
	z := corridorClampUnit(maxFloat(metrics.CommunicationCentralization, metrics.MaxAgentActivityShare))
	d := corridorClampUnit((entropy * 0.70) + ((1 - corridorClampUnit(metrics.DuplicationIndex)) * 0.30))
	p := corridorFitProgress(metrics.EventTypeCounts)
	alignmentPenalty := 0.0
	alignmentPenalty += minFloat(float64(confirmedCounts["contradiction"])*0.20, 0.40)
	alignmentPenalty += minFloat(float64(confirmedCounts["ambiguity"])*0.12, 0.24)
	alignmentPenalty += minFloat(float64(confirmedCounts["gap"])*0.10, 0.18)
	alignmentPenalty += corridorClampUnit(metrics.DuplicationIndex) * 0.25
	alignmentPenalty += corridorClampUnit(metrics.BlockerDensity) * 0.20
	alignmentPenalty += z * 0.10
	a := corridorClampUnit(1 - alignmentPenalty)
	s := corridorClampUnit(
		0.05 +
			(minFloat(float64(confirmedCounts["bridge"])*0.12, 0.24)) +
			((1 - z) * 0.20) +
			(d * 0.20) +
			(maxFloat(p, 0) * 0.20) -
			(minFloat(float64(confirmedCounts["contradiction"])*0.15, 0.30)) -
			(minFloat(float64(confirmedCounts["gap"])*0.08, 0.12)),
	)
	m := corridorClampUnit(
		(corridorClampUnit(metrics.BlockerDensity) * 0.35) +
			(corridorClampUnit(metrics.DuplicationIndex) * 0.30) +
			((1 - a) * 0.15) +
			(z * 0.10) +
			(minFloat(math.Abs(p), 1.0) * 0.10),
	)
	return CorridorFitMetricVector{
		Alignment:       a,
		Differentiation: d,
		Synergy:         s,
		Centralization:  z,
		Metastability:   m,
		Progress:        p,
	}
}

func corridorFitGapBreakdown(vector CorridorFitMetricVector, ranges []CorridorFitMetricRange) []CorridorFitMetricGap {
	out := make([]CorridorFitMetricGap, 0, len(ranges))
	for _, rule := range ranges {
		value := corridorFitMetricValue(vector, rule.Metric)
		status := "IN_RANGE"
		delta := 0.0
		switch {
		case rule.LowerBound != nil && value < *rule.LowerBound:
			status = "LOW"
			delta = value - *rule.LowerBound
		case rule.UpperBound != nil && value > *rule.UpperBound:
			status = "HIGH"
			delta = value - *rule.UpperBound
		}
		out = append(out, CorridorFitMetricGap{
			Metric:     rule.Metric,
			Value:      value,
			LowerBound: corridorFitCloneBound(rule.LowerBound),
			UpperBound: corridorFitCloneBound(rule.UpperBound),
			Delta:      delta,
			Status:     status,
		})
	}
	return out
}

func corridorFitStatusForGaps(gaps []CorridorFitMetricGap) string {
	if len(gaps) == 0 {
		return corridorFitStatusUnderEvidenced
	}
	outsideCount := 0
	maxGap := 0.0
	minBoundaryDistance := math.MaxFloat64
	for _, gap := range gaps {
		if gap.Status != "IN_RANGE" {
			outsideCount++
			maxGap = maxFloat(maxGap, math.Abs(gap.Delta))
			continue
		}
		if gap.LowerBound != nil {
			minBoundaryDistance = minFloat(minBoundaryDistance, math.Abs(gap.Value-*gap.LowerBound))
		}
		if gap.UpperBound != nil {
			minBoundaryDistance = minFloat(minBoundaryDistance, math.Abs(*gap.UpperBound-gap.Value))
		}
	}
	switch {
	case outsideCount == 0 && minBoundaryDistance <= 0.05:
		return corridorFitStatusNearBoundary
	case outsideCount == 0:
		return corridorFitStatusInCorridor
	case outsideCount == 1 && maxGap <= 0.08:
		return corridorFitStatusNearBoundary
	default:
		return corridorFitStatusOutOfCorridor
	}
}

func corridorFitConfidenceForCluster(cluster CorridorClusterReport, hasCatalog bool) float64 {
	confidence := corridorClampUnit(cluster.ReadinessConfidence)
	switch strings.TrimSpace(cluster.CorridorLookup.LookupStatus) {
	case corridorLookupStatusTemplateMatch:
		confidence = corridorMaxFloat(confidence, 0.85)
	case corridorLookupStatusClassMatch:
		confidence = corridorMaxFloat(confidence, 0.78)
	}
	if normalizeAuthoredTaskClassSource(cluster.TaskClassSource) == model.TaskClassSourceExplicit {
		confidence = corridorMaxFloat(confidence, 0.95)
	}
	if normalizeAuthoredTaskClassSource(cluster.TaskClassSource) == model.TaskClassSourceTemplateDefault {
		confidence = corridorClampUnit(confidence - 0.05)
	}
	if !hasCatalog {
		confidence = corridorClampUnit(confidence - 0.35)
	}
	if cluster.BasisStale {
		confidence = corridorClampUnit(confidence - 0.25)
	}
	if cluster.Metrics.EventCount < 4 {
		confidence = corridorClampUnit(confidence - 0.15)
	}
	if len(cluster.TaskIDs) > 1 && cluster.MixedTaskClasses {
		confidence = corridorClampUnit(confidence - 0.25)
	}
	return confidence
}

func corridorFitScoreForCluster(gaps []CorridorFitMetricGap, confidence float64) int {
	totalGap := 0.0
	for _, gap := range gaps {
		totalGap += math.Abs(gap.Delta)
	}
	score := int(math.Round((confidence * 100) - (totalGap * 100)))
	return clampInt(score, 0, 100)
}

func corridorFitSummary(cluster CorridorClusterReport, rangeCheck CorridorFitCatalogRangeCheck, fitStatus string, gaps []CorridorFitMetricGap, confirmedCounts map[string]int) string {
	subject := firstNonEmpty(strings.TrimSpace(cluster.ProtoClusterID), "cluster")
	switch fitStatus {
	case corridorFitStatusStaleBasis:
		return fmt.Sprintf("%s still maps to the %s approximation, but the fit read-side is stale until the task-class basis is refreshed", subject, firstNonEmpty(rangeCheck.DisplayName, "selected corridor"))
	case corridorFitStatusUnderEvidenced:
		return fmt.Sprintf("%s does not yet have enough stable task-class evidence to evaluate the corridor-fit approximation", subject)
	}
	violations := []string{}
	for _, gap := range gaps {
		if gap.Status == "IN_RANGE" {
			continue
		}
		violations = append(violations, fmt.Sprintf("%s %s %.2f", gap.Metric, strings.ToLower(gap.Status), math.Abs(gap.Delta)))
	}
	if len(violations) == 0 {
		if fitStatus == corridorFitStatusNearBoundary {
			return fmt.Sprintf("%s currently sits near the %s approximation boundary on one or more metrics", subject, firstNonEmpty(rangeCheck.DisplayName, "selected corridor"))
		}
		return fmt.Sprintf("%s currently stays within the %s approximation range, with %d confirmed tensions carried only as corroborating context", subject, firstNonEmpty(rangeCheck.DisplayName, "selected corridor"), sumControlCountMap(confirmedCounts))
	}
	if fitStatus == corridorFitStatusNearBoundary {
		return fmt.Sprintf("%s currently sits near the %s approximation boundary with slight drift on %s", subject, firstNonEmpty(rangeCheck.DisplayName, "selected corridor"), strings.Join(violations, ", "))
	}
	return fmt.Sprintf("%s currently falls outside the %s approximation range on %s", subject, firstNonEmpty(rangeCheck.DisplayName, "selected corridor"), strings.Join(violations, ", "))
}

func corridorFitSnapshotEntityID(filter CorridorFitFilter) string {
	if clusterID := strings.TrimSpace(filter.ProtoClusterID); clusterID != "" {
		return clusterID
	}
	return strings.TrimSpace(filter.WorkspaceID)
}

func corridorFitSnapshotTaskID(report CorridorFitReport) string {
	if strings.TrimSpace(report.Filter.ProtoClusterID) == "" {
		return ""
	}
	for _, cluster := range report.Clusters {
		if len(cluster.TaskIDs) == 1 {
			return strings.TrimSpace(cluster.TaskIDs[0])
		}
	}
	return ""
}

func corridorFitSnapshotSessionID(report CorridorFitReport) string {
	if strings.TrimSpace(report.Filter.ProtoClusterID) == "" {
		return ""
	}
	for _, cluster := range report.Clusters {
		if len(cluster.SessionIDs) == 1 {
			return strings.TrimSpace(cluster.SessionIDs[0])
		}
	}
	return ""
}

func corridorFitSnapshotSummary(report CorridorFitReport, clusters []CorridorFitClusterReport) string {
	if clusterID := strings.TrimSpace(report.Filter.ProtoClusterID); clusterID != "" {
		if len(clusters) > 0 {
			return fmt.Sprintf("Corridor fit approximation snapshot: %s status=%s score=%d", clusterID, strings.ToLower(clusters[0].FitStatus), clusters[0].FitScore)
		}
		return "Corridor fit approximation snapshot: " + clusterID
	}
	return fmt.Sprintf(
		"Corridor fit approximation snapshot: %d in-corridor, %d near-boundary, %d out-of-corridor",
		report.Workspace.InCorridorCount,
		report.Workspace.NearBoundaryCount,
		report.Workspace.OutOfCorridorCount,
	)
}

func corridorFitCatalogProfileForTaskClass(taskClass string) (corridorFitCatalogProfile, bool) {
	taskClass = normalizeTaskClassHint(taskClass)
	for _, profile := range corridorFitCatalogProfiles {
		if profile.TaskClass == taskClass {
			return profile, true
		}
	}
	return corridorFitCatalogProfile{}, false
}

func corridorFitMetricValue(vector CorridorFitMetricVector, metric string) float64 {
	switch strings.TrimSpace(metric) {
	case "alignment":
		return vector.Alignment
	case "differentiation":
		return vector.Differentiation
	case "synergy":
		return vector.Synergy
	case "centralization":
		return vector.Centralization
	case "metastability":
		return vector.Metastability
	case "progress":
		return vector.Progress
	default:
		return 0
	}
}

func corridorFitStatusRank(status string) int {
	switch strings.TrimSpace(status) {
	case corridorFitStatusOutOfCorridor:
		return 5
	case corridorFitStatusNearBoundary:
		return 4
	case corridorFitStatusStaleBasis:
		return 3
	case corridorFitStatusUnderEvidenced:
		return 2
	case corridorFitStatusInCorridor:
		return 1
	default:
		return 0
	}
}

func corridorFitDominantCatalogKey(counts map[string]int) (string, int) {
	best := ""
	bestCount := 0
	for _, key := range []string{"proof", "exploration", "integration", "incident"} {
		count := counts[key]
		if count > bestCount {
			best = key
			bestCount = count
		}
	}
	return best, bestCount
}

func corridorFitProgress(eventCounts map[string]int) float64 {
	positive := 0
	negative := 0
	for eventType, count := range eventCounts {
		lower := strings.ToLower(strings.TrimSpace(eventType))
		switch {
		case strings.Contains(lower, "completed"),
			strings.Contains(lower, "confirmed"),
			strings.Contains(lower, "accepted"),
			strings.Contains(lower, "resolved"),
			strings.Contains(lower, "created"),
			strings.Contains(lower, "published"),
			strings.Contains(lower, "restored"):
			positive += count
		case strings.Contains(lower, "blocked"),
			strings.Contains(lower, "disputed"),
			strings.Contains(lower, "superseded"),
			strings.Contains(lower, "escalated"),
			strings.Contains(lower, "failed"),
			strings.Contains(lower, "rollback"),
			strings.Contains(lower, "reopened"):
			negative += count
		}
	}
	total := positive + negative
	if total == 0 {
		return 0
	}
	return math.Max(-1, math.Min(1, float64(positive-negative)/float64(total)))
}

func corridorFitNormalizedEntropy(shares map[string]float64) float64 {
	if len(shares) <= 1 {
		return 0
	}
	entropy := 0.0
	for _, share := range shares {
		if share <= 0 {
			continue
		}
		entropy -= share * math.Log(share)
	}
	denom := math.Log(float64(len(shares)))
	if denom <= 0 {
		return 0
	}
	return corridorClampUnit(entropy / denom)
}

func corridorFitBound(value float64) *float64 {
	bound := value
	return &bound
}

func corridorFitCloneBound(value *float64) *float64 {
	if value == nil {
		return nil
	}
	return corridorFitBound(*value)
}

func minFloat(left, right float64) float64 {
	if left < right {
		return left
	}
	return right
}
