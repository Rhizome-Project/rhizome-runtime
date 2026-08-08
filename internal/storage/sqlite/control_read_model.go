package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	controlReadRuntimeEventWindow = 500
	controlReadClusterWindow      = 128
	controlReadTensionWindow      = 1000
	controlReadBasisStaleAfter    = 72 * time.Hour
)

type ControlReportFilter struct {
	WorkspaceID    string `json:"workspace_id"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type ControlSignalVector struct {
	ThroughputPressure   int     `json:"throughput_pressure"`
	ReviewPressure       int     `json:"review_pressure"`
	CoordinationPressure int     `json:"coordination_pressure"`
	PressureScore        int     `json:"pressure_score"`
	AttentionBand        string  `json:"attention_band"`
	RSPRiskScore         float64 `json:"rsp_risk_score,omitempty"`
	RSPDominantState     string  `json:"rsp_dominant_state,omitempty"`
}

type ControlSuggestedControls struct {
	FanoutCap      int     `json:"fanout_cap"`
	ReviewDepth    int     `json:"review_depth"`
	ContextCap     int     `json:"context_cap"`
	BridgeQuota    int     `json:"bridge_quota"`
	MergeThreshold float64 `json:"merge_threshold"`
	PriorityFocus  string  `json:"priority_focus"`
}

type ControlClusterReport struct {
	ProtoClusterID        string                   `json:"proto_cluster_id"`
	ResolutionKind        string                   `json:"resolution_kind"`
	TaskIDs               []string                 `json:"task_ids,omitempty"`
	SessionIDs            []string                 `json:"session_ids,omitempty"`
	DocKeys               []string                 `json:"doc_keys,omitempty"`
	ArtifactRefs          []string                 `json:"artifact_refs,omitempty"`
	AgentIDs              []string                 `json:"agent_ids,omitempty"`
	MetricsMissing        bool                     `json:"metrics_missing,omitempty"`
	BasisStale            bool                     `json:"basis_stale,omitempty"`
	LastTensionBasisAt    string                   `json:"last_tension_basis_at,omitempty"`
	Metrics               ProtoClusterMetrics      `json:"metrics"`
	Signals               ControlSignalVector      `json:"signals"`
	SuggestedControls     ControlSuggestedControls `json:"suggested_controls"`
	ConfirmedTensionCount int                      `json:"confirmed_tension_count"`
	PendingTensionCount   int                      `json:"pending_tension_count"`
	ConfirmedCountsByType map[string]int           `json:"confirmed_counts_by_type,omitempty"`
	PendingCountsByType   map[string]int           `json:"pending_counts_by_type,omitempty"`
	ConfirmedTensionIDs   []string                 `json:"confirmed_tension_ids,omitempty"`
	PendingTensionIDs     []string                 `json:"pending_tension_ids,omitempty"`
	Summary               string                   `json:"summary,omitempty"`
}

type ControlWorkspaceMetrics struct {
	TotalClusters            int    `json:"total_clusters"`
	HotClusterCount          int    `json:"hot_cluster_count"`
	AttentionClusterCount    int    `json:"attention_cluster_count"`
	ConfirmedTensionCount    int    `json:"confirmed_tension_count"`
	PendingTensionCount      int    `json:"pending_tension_count"`
	HighestPressureClusterID string `json:"highest_pressure_cluster_id,omitempty"`
	HighestPressureScore     int    `json:"highest_pressure_score"`
}

type ControlReport struct {
	WorkspaceID       string                  `json:"workspace_id"`
	TimeAuthority     WorkspaceTimeAuthority  `json:"time_authority"`
	GeneratedAt       string                  `json:"generated_at"`
	Filter            ControlReportFilter     `json:"filter"`
	Workspace         ControlWorkspaceMetrics `json:"workspace"`
	Clusters          []ControlClusterReport  `json:"clusters,omitempty"`
	readSurfacePolicy ReadSurfacePolicy       `json:"-"`
}

type ControlClusterDetail struct {
	TimeAuthority WorkspaceTimeAuthority `json:"time_authority"`
	Cluster       ControlClusterReport   `json:"cluster"`
	Tensions      []TensionRecord        `json:"tensions,omitempty"`
}

type ControlSnapshotInput struct {
	ActorID string
	Limit   int
}

func normalizeControlReportFilter(filter ControlReportFilter) ControlReportFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	filter.Limit = clampReadSurfaceLimit(filter.Limit, readSurfaceReportLimitDefault, readSurfaceReportLimitMax)
	return filter
}

func (s *Store) BuildControlReport(ctx context.Context, filter ControlReportFilter) (ControlReport, error) {
	filter = normalizeControlReportFilter(filter)
	if filter.WorkspaceID == "" {
		return ControlReport{}, errors.New("workspace_id is required")
	}
	report := ControlReport{
		WorkspaceID: filter.WorkspaceID,
		Filter:      filter,
	}
	var err error
	report.TimeAuthority, err = s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return ControlReport{}, err
	}
	report.GeneratedAt = generatedAtFromWorkspaceTimeAuthority(report.TimeAuthority)

	clusterLimit := controlReadClusterWindow
	if filter.ProtoClusterID != "" {
		clusterLimit = controlReadRuntimeEventWindow
	}
	report.readSurfacePolicy = controlReadSurfacePolicy(filter, clusterLimit)
	instrumentation, err := s.BuildInstrumentationReport(ctx, InstrumentationReportFilter{
		WorkspaceID:  filter.WorkspaceID,
		Limit:        controlReadRuntimeEventWindow,
		ClusterLimit: clusterLimit,
	})
	if err != nil {
		return ControlReport{}, err
	}
	epochAnchorAt, err := s.persistedControlEpochAnchor(ctx, filter.WorkspaceID)
	if err != nil {
		return ControlReport{}, err
	}
	tensions, err := s.listAllTensions(ctx, TensionFilter{
		WorkspaceID:    filter.WorkspaceID,
		ProtoClusterID: filter.ProtoClusterID,
		Limit:          controlReadTensionWindow,
	})
	if err != nil {
		return ControlReport{}, err
	}

	clusters := map[string]ProtoClusterReport{}
	for _, cluster := range instrumentation.Clusters {
		if filter.ProtoClusterID != "" && strings.TrimSpace(cluster.ProtoClusterID) != filter.ProtoClusterID {
			continue
		}
		clusters[strings.TrimSpace(cluster.ProtoClusterID)] = cluster
	}
	activeTensionsByCluster := map[string][]TensionRecord{}
	for _, tension := range tensions {
		clusterID := strings.TrimSpace(tension.ProtoClusterID)
		if clusterID == "" {
			continue
		}
		if strings.TrimSpace(tension.LifecycleState) == tensionLifecycleArchived || strings.TrimSpace(tension.ReviewStatus) == tensionReviewDiscarded {
			continue
		}
		if filter.ProtoClusterID != "" && clusterID != filter.ProtoClusterID {
			continue
		}
		activeTensionsByCluster[clusterID] = append(activeTensionsByCluster[clusterID], tension)
		if _, ok := clusters[clusterID]; !ok {
			clusters[clusterID] = controlProtoClusterFromTension(tension)
		}
	}

	clusterReports := make([]ControlClusterReport, 0, len(clusters))
	for clusterID, cluster := range clusters {
		clusterReport := buildControlClusterReport(cluster, activeTensionsByCluster[clusterID], epochAnchorAt)
		snapshot := s.getLatestClusterRSPSnapshot(ctx, filter.WorkspaceID, clusterID)
		clusterReport.Signals.RSPRiskScore = snapshot.RiskScore
		clusterReport.Signals.RSPDominantState = snapshot.HiddenState
		clusterReport.SuggestedControls = applyUnifiedHintOverlay(
			clusterReport.SuggestedControls,
			snapshot.CapabilityFlags,
			snapshot.GovernedHints,
			snapshot.CoherenceBand,
		).Controls
		clusterReport.Summary = controlSummaryForCluster(cluster, clusterReport.Signals, clusterReport.SuggestedControls, clusterReport.ConfirmedCountsByType, clusterReport.PendingCountsByType)
		clusterReports = append(clusterReports, clusterReport)
	}
	sort.Slice(clusterReports, func(i, j int) bool {
		left := clusterReports[i]
		right := clusterReports[j]
		if left.Signals.PressureScore != right.Signals.PressureScore {
			return left.Signals.PressureScore > right.Signals.PressureScore
		}
		if left.Signals.ReviewPressure != right.Signals.ReviewPressure {
			return left.Signals.ReviewPressure > right.Signals.ReviewPressure
		}
		if left.Signals.ThroughputPressure != right.Signals.ThroughputPressure {
			return left.Signals.ThroughputPressure > right.Signals.ThroughputPressure
		}
		return left.ProtoClusterID < right.ProtoClusterID
	})

	report.Workspace.TotalClusters = len(clusterReports)
	for _, cluster := range clusterReports {
		report.Workspace.ConfirmedTensionCount += cluster.ConfirmedTensionCount
		report.Workspace.PendingTensionCount += cluster.PendingTensionCount
		switch cluster.Signals.AttentionBand {
		case "HOT":
			report.Workspace.HotClusterCount++
			report.Workspace.AttentionClusterCount++
		case "WATCH":
			report.Workspace.AttentionClusterCount++
		}
		if cluster.Signals.PressureScore > report.Workspace.HighestPressureScore {
			report.Workspace.HighestPressureScore = cluster.Signals.PressureScore
			report.Workspace.HighestPressureClusterID = cluster.ProtoClusterID
		}
	}
	if filter.Limit > 0 && len(clusterReports) > filter.Limit {
		clusterReports = append([]ControlClusterReport(nil), clusterReports[:filter.Limit]...)
	}
	report.Clusters = clusterReports
	return report, nil
}

func (s *Store) BuildControlClusterDetail(ctx context.Context, workspaceID, protoClusterID string) (ControlClusterDetail, error) {
	report, err := s.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: protoClusterID,
		Limit:          1,
	})
	if err != nil {
		return ControlClusterDetail{}, err
	}
	if len(report.Clusters) == 0 {
		return ControlClusterDetail{}, fmt.Errorf("control cluster not found: %s/%s", strings.TrimSpace(workspaceID), strings.TrimSpace(protoClusterID))
	}
	tensions, err := s.listAllTensions(ctx, TensionFilter{
		WorkspaceID:    strings.TrimSpace(workspaceID),
		ProtoClusterID: strings.TrimSpace(protoClusterID),
		Limit:          controlReadTensionWindow,
	})
	if err != nil {
		return ControlClusterDetail{}, err
	}
	active := make([]TensionRecord, 0, len(tensions))
	for _, tension := range tensions {
		if strings.TrimSpace(tension.LifecycleState) == tensionLifecycleArchived || strings.TrimSpace(tension.ReviewStatus) == tensionReviewDiscarded {
			continue
		}
		active = append(active, tension)
	}
	sort.Slice(active, func(i, j int) bool {
		left := active[i]
		right := active[j]
		if left.SurfaceScore != right.SurfaceScore {
			return left.SurfaceScore > right.SurfaceScore
		}
		return left.TensionID < right.TensionID
	})
	return ControlClusterDetail{
		TimeAuthority: report.TimeAuthority,
		Cluster:       report.Clusters[0],
		Tensions:      active,
	}, nil
}

func (s *Store) RecordControlSignalSnapshot(ctx context.Context, report ControlReport, input ControlSnapshotInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(report.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		actorID = "control.snapshot"
	}
	clusters := append([]ControlClusterReport(nil), report.Clusters...)
	if input.Limit > 0 && len(clusters) > input.Limit {
		clusters = append([]ControlClusterReport(nil), clusters[:input.Limit]...)
	}
	if strings.TrimSpace(report.Filter.ProtoClusterID) != "" && len(clusters) == 0 {
		return RuntimeEventRecord{}, fmt.Errorf("control cluster not found: %s/%s", workspaceID, strings.TrimSpace(report.Filter.ProtoClusterID))
	}
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, referenceAt)
	if err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	summary := controlSnapshotSummary(report, clusters)
	payload := map[string]any{
		"generated_at":           report.GeneratedAt,
		"workspace_id":           report.WorkspaceID,
		"filter":                 report.Filter,
		"workspace":              report.Workspace,
		"clusters":               clusters,
		"summary":                summary,
		"source_cluster_count":   len(report.Clusters),
		"captured_cluster_count": len(clusters),
		"snapshot_limit":         input.Limit,
		"snapshot_truncated":     input.Limit > 0 && len(report.Clusters) > input.Limit,
		"typed_event_type":       "CONTROL_ADVISORY_SNAPSHOT",
		"event_kind":             "cluster.control_advisory_snapshot",
		"hot_cluster_count":      report.Workspace.HotClusterCount,
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin control snapshot tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		record, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: workspaceID,
			EventType:   "cluster.control_advisory_snapshot",
			EntityType:  "instrumentation_control",
			EntityID:    controlSnapshotEntityID(report.Filter),
			ActorType:   "operator",
			ActorID:     actorID,
			SessionID:   controlSnapshotSessionID(report),
			TaskID:      controlSnapshotTaskID(report),
			PayloadJSON: mustJSON(payload),
			CreatedAt:   referenceAt,
		})
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit control snapshot tx: %w", err)
	}
	return record, nil
}

func controlSnapshotEntityID(filter ControlReportFilter) string {
	if clusterID := strings.TrimSpace(filter.ProtoClusterID); clusterID != "" {
		return clusterID
	}
	return strings.TrimSpace(filter.WorkspaceID)
}

func controlSnapshotSummary(report ControlReport, clusters []ControlClusterReport) string {
	if clusterID := strings.TrimSpace(report.Filter.ProtoClusterID); clusterID != "" {
		if len(clusters) > 0 {
			return fmt.Sprintf("Control advisory snapshot: %s pressure=%d", clusterID, clusters[0].Signals.PressureScore)
		}
		return "Control advisory snapshot: " + clusterID
	}
	if len(clusters) == 0 {
		return fmt.Sprintf("Control advisory snapshot for %s captured with no eligible clusters", firstNonEmpty(strings.TrimSpace(report.WorkspaceID), "workspace"))
	}
	if sourceCount := len(report.Clusters); sourceCount > len(clusters) {
		return fmt.Sprintf(
			"Control advisory snapshot: captured %d/%d clusters; %d hot total",
			len(clusters),
			sourceCount,
			report.Workspace.HotClusterCount,
		)
	}
	return fmt.Sprintf("Control advisory snapshot: %d hot / %d clusters", report.Workspace.HotClusterCount, report.Workspace.TotalClusters)
}

func controlSnapshotTaskID(report ControlReport) string {
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

func controlSnapshotSessionID(report ControlReport) string {
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

func controlProtoClusterFromTension(tension TensionRecord) ProtoClusterReport {
	clusterID := strings.TrimSpace(tension.ProtoClusterID)
	return ProtoClusterReport{
		ProtoClusterID: clusterID,
		ResolutionKind: controlResolutionKindFromProtoClusterID(clusterID, tension.AnchorKind),
		TaskIDs:        append([]string{}, tension.TaskIDs...),
		SessionIDs:     append([]string{}, tension.SessionIDs...),
		DocKeys:        append([]string{}, tension.DocKeys...),
		ArtifactRefs:   append([]string{}, tension.ArtifactRefs...),
		AgentIDs:       append([]string{}, tension.AgentIDs...),
	}
}

func controlResolutionKindFromProtoClusterID(clusterID, fallback string) string {
	if head, _, ok := strings.Cut(strings.TrimSpace(clusterID), ":"); ok && strings.TrimSpace(head) != "" {
		return strings.TrimSpace(head)
	}
	return firstNonEmpty(strings.TrimSpace(fallback), "proto_cluster")
}

func buildControlClusterReport(cluster ProtoClusterReport, tensions []TensionRecord, epochAnchorAt string) ControlClusterReport {
	confirmedCounts := map[string]int{}
	pendingCounts := map[string]int{}
	confirmedIDs := []string{}
	pendingIDs := []string{}
	lastTensionBasisAt := ""
	for _, tension := range tensions {
		switch strings.TrimSpace(tension.ReviewStatus) {
		case tensionReviewConfirmed:
			confirmedCounts[tension.TensionType]++
			confirmedIDs = append(confirmedIDs, tension.TensionID)
			lastTensionBasisAt = controlLaterTimestamp(lastTensionBasisAt, controlTensionBasisAt(tension))
		case tensionReviewPending:
			pendingCounts[tension.TensionType]++
			pendingIDs = append(pendingIDs, tension.TensionID)
		}
	}
	metricsMissing := cluster.Metrics.EventCount == 0 && strings.TrimSpace(cluster.Metrics.LastEventAt) == "" && len(tensions) > 0
	basisStale := metricsMissing
	if !basisStale && len(confirmedIDs) > 0 {
		latestMetricEventAt := strings.TrimSpace(cluster.Metrics.LastEventAt)
		referenceAt := controlLaterTimestamp(latestMetricEventAt, epochAnchorAt)
		switch {
		case latestMetricEventAt != "" && lastTensionBasisAt == "":
			basisStale = true
		case controlTimestampAfter(latestMetricEventAt, lastTensionBasisAt):
			basisStale = true
		case controlTimestampStale(lastTensionBasisAt, referenceAt, controlReadBasisStaleAfter):
			basisStale = true
		}
	}
	effectiveConfirmedCounts := cloneIntMap(confirmedCounts)
	if metricsMissing {
		effectiveConfirmedCounts = map[string]int{}
	}
	signals := controlSignalsForCluster(cluster, effectiveConfirmedCounts)
	controls := controlSuggestedControlsForSignals(cluster.Metrics, effectiveConfirmedCounts, signals)
	return ControlClusterReport{
		ProtoClusterID:        cluster.ProtoClusterID,
		ResolutionKind:        cluster.ResolutionKind,
		TaskIDs:               append([]string{}, cluster.TaskIDs...),
		SessionIDs:            append([]string{}, cluster.SessionIDs...),
		DocKeys:               append([]string{}, cluster.DocKeys...),
		ArtifactRefs:          append([]string{}, cluster.ArtifactRefs...),
		AgentIDs:              append([]string{}, cluster.AgentIDs...),
		MetricsMissing:        metricsMissing,
		BasisStale:            basisStale,
		LastTensionBasisAt:    lastTensionBasisAt,
		Metrics:               cluster.Metrics,
		Signals:               signals,
		SuggestedControls:     controls,
		ConfirmedTensionCount: len(confirmedIDs),
		PendingTensionCount:   len(pendingIDs),
		ConfirmedCountsByType: confirmedCounts,
		PendingCountsByType:   pendingCounts,
		ConfirmedTensionIDs:   uniqueSortedStrings(confirmedIDs),
		PendingTensionIDs:     uniqueSortedStrings(pendingIDs),
		Summary:               controlSummaryForCluster(cluster, signals, controls, confirmedCounts, pendingCounts),
	}
}

func controlTensionBasisAt(tension TensionRecord) string {
	return firstNonEmpty(
		strings.TrimSpace(tension.LastDetectedAt),
		strings.TrimSpace(tension.LastSeenAt),
		strings.TrimSpace(tension.LastRefreshedAt),
		strings.TrimSpace(tension.UpdatedAt),
		strings.TrimSpace(tension.CreatedAt),
	)
}

func controlLaterTimestamp(current, candidate string) string {
	current = strings.TrimSpace(current)
	candidate = strings.TrimSpace(candidate)
	switch {
	case candidate == "":
		return current
	case current == "":
		return candidate
	case controlTimestampAfter(candidate, current):
		return candidate
	default:
		return current
	}
}

func controlTimestampAfter(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	leftTime, leftOK := controlParseTimestamp(left)
	rightTime, rightOK := controlParseTimestamp(right)
	switch {
	case leftOK && rightOK:
		return leftTime.After(rightTime)
	case leftOK && !rightOK:
		return true
	case !leftOK && rightOK:
		return false
	default:
		return left > right
	}
}

func controlParseTimestamp(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func controlTimestampStale(value, reference string, threshold time.Duration) bool {
	parsedValue, valueOK := controlParseTimestamp(value)
	parsedReference, referenceOK := controlParseTimestamp(reference)
	if !valueOK || !referenceOK || parsedReference.Before(parsedValue) {
		return false
	}
	return parsedReference.Sub(parsedValue) > threshold
}

func controlSignalsForCluster(cluster ProtoClusterReport, confirmedCounts map[string]int) ControlSignalVector {
	metrics := cluster.Metrics
	throughput := 0
	throughput += minInt(metrics.OpenQueueCount*10, 25)
	throughput += minInt(metrics.BlockerSignalCount*6, 24)
	throughput += minInt(confirmedCounts["bottleneck"]*25, 50)
	if metrics.BlockerDensity >= 0.30 {
		throughput += 10
	} else if metrics.BlockerDensity >= 0.15 {
		throughput += 5
	}
	throughput = clampInt(throughput, 0, 100)

	review := 0
	review += minInt(confirmedCounts["contradiction"]*30, 60)
	review += minInt(confirmedCounts["ambiguity"]*20, 40)
	review += clampInt(int(metrics.DuplicationIndex*35), 0, 20)
	review = clampInt(review, 0, 100)

	coordination := 0
	coordination += minInt(confirmedCounts["bridge"]*20, 40)
	coordination += clampInt(int(metrics.CommunicationCentralization*40), 0, 25)
	coordination += clampInt(int(metrics.MaxAgentActivityShare*20), 0, 15)
	coordination += minInt(len(cluster.AgentIDs)*2, 10)
	coordination = clampInt(coordination, 0, 100)

	pressure := clampInt((throughput*4+review*4+coordination*2)/10, 0, 100)
	pressure = maxInt(pressure, throughput)
	pressure = maxInt(pressure, review)
	pressure = maxInt(pressure, coordination)
	pressure = maxInt(pressure, clampInt(len(confirmedCounts)*10+sumControlCountMap(confirmedCounts)*6, 0, 100))

	band := "STEADY"
	switch {
	case pressure >= 70 || sumControlCountMap(confirmedCounts) > 0:
		band = "HOT"
	case pressure >= 40:
		band = "WATCH"
	}
	return ControlSignalVector{
		ThroughputPressure:   throughput,
		ReviewPressure:       review,
		CoordinationPressure: coordination,
		PressureScore:        pressure,
		AttentionBand:        band,
	}
}

func controlSuggestedControlsForSignals(metrics ProtoClusterMetrics, confirmedCounts map[string]int, signals ControlSignalVector) ControlSuggestedControls {
	focus := "throughput"
	top := signals.ThroughputPressure
	if signals.ReviewPressure > top {
		focus = "review"
		top = signals.ReviewPressure
	}
	if signals.CoordinationPressure > top {
		focus = "coordination"
	}

	fanoutCap := 4
	switch {
	case signals.ThroughputPressure >= 85 || signals.PressureScore >= 90:
		fanoutCap = 1
	case signals.ThroughputPressure >= 60 || signals.PressureScore >= 70:
		fanoutCap = 2
	case signals.ThroughputPressure >= 35:
		fanoutCap = 3
	}
	if confirmedCounts["bottleneck"] > 0 && fanoutCap > 2 {
		fanoutCap = 2
	}

	reviewDepth := 1
	switch {
	case signals.ReviewPressure >= 80 || confirmedCounts["contradiction"] > 0:
		reviewDepth = 3
	case signals.ReviewPressure >= 45 || confirmedCounts["ambiguity"] > 0:
		reviewDepth = 2
	}

	contextCap := 8
	switch {
	case signals.CoordinationPressure >= 80 || metrics.MaxAgentActivityShare >= 0.75:
		contextCap = 4
	case signals.CoordinationPressure >= 50 || metrics.MaxAgentActivityShare >= 0.60:
		contextCap = 6
	}

	bridgeQuota := 1
	if signals.CoordinationPressure >= 65 || confirmedCounts["bridge"] > 0 {
		bridgeQuota = 2
	}

	mergeThreshold := 0.60
	switch {
	case signals.ReviewPressure >= 80 || confirmedCounts["contradiction"] > 0:
		mergeThreshold = 0.90
	case signals.ReviewPressure >= 55 || confirmedCounts["ambiguity"] > 0:
		mergeThreshold = 0.80
	case signals.PressureScore >= 70:
		mergeThreshold = 0.75
	}

	return ControlSuggestedControls{
		FanoutCap:      fanoutCap,
		ReviewDepth:    reviewDepth,
		ContextCap:     contextCap,
		BridgeQuota:    bridgeQuota,
		MergeThreshold: mergeThreshold,
		PriorityFocus:  focus,
	}
}

func controlSummaryForCluster(cluster ProtoClusterReport, signals ControlSignalVector, controls ControlSuggestedControls, confirmedCounts, pendingCounts map[string]int) string {
	subject := firstNonEmpty(strings.TrimSpace(cluster.ProtoClusterID), strings.TrimSpace(cluster.ResolutionKind), "cluster")
	parts := []string{
		fmt.Sprintf("focus=%s", controls.PriorityFocus),
		fmt.Sprintf("pressure=%d", signals.PressureScore),
	}
	if count := sumControlCountMap(confirmedCounts); count > 0 {
		parts = append(parts, fmt.Sprintf("%d confirmed tensions", count))
	}
	if count := sumControlCountMap(pendingCounts); count > 0 {
		parts = append(parts, fmt.Sprintf("tracking %d pending tensions", count))
	}
	return "Control signals for " + clipSummary(subject, 96) + ": " + strings.Join(parts, ", ")
}

func sumControlCountMap(values map[string]int) int {
	total := 0
	for _, count := range values {
		total += count
	}
	return total
}
