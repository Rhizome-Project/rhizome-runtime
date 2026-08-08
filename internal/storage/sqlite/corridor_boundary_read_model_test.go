package sqlite

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestBuildCorridorBoundaryClusterReportMapsStatusMatrix(t *testing.T) {
	t.Parallel()

	within := buildCorridorBoundaryClusterReport(corridorBoundaryBaseFitCluster("task:ws-boundary/within", 0.70, corridorFitStatusInCorridor))
	if within.BoundarySource != corridorBoundarySourceFitDerived {
		t.Fatalf("expected fit-derived boundary source, got %+v", within)
	}
	if within.BasisState != corridorBoundaryBasisStateReady || within.BoundaryState != corridorBoundaryStateInRange {
		t.Fatalf("expected ready/in-range boundary cluster, got %+v", within)
	}
	if within.NearBoundaryMetricCount != 0 || within.OutsideMetricCount != 0 || len(within.BoundarySignals) != 0 {
		t.Fatalf("expected within cluster to stay signal-free, got %+v", within)
	}
	if within.NearestMetric != "" || within.NearestBound != "" || within.NearestDistance != 0 {
		t.Fatalf("expected within cluster not to expose nearest boundary signal, got %+v", within)
	}
	if within.NormalizedSeverity != 0 || !strings.Contains(within.Summary, "stays within the current corridor-boundary approximation") {
		t.Fatalf("expected within summary to stay in-range, got %+v", within)
	}

	near := buildCorridorBoundaryClusterReport(corridorBoundaryBaseFitCluster("task:ws-boundary/near", 0.62, corridorFitStatusNearBoundary))
	if near.BasisState != corridorBoundaryBasisStateReady || near.BoundaryState != corridorBoundaryStateNearBoundary {
		t.Fatalf("expected ready/near boundary cluster, got %+v", near)
	}
	if near.NearBoundaryMetricCount != 1 || near.OutsideMetricCount != 0 || len(near.BoundarySignals) != 1 {
		t.Fatalf("expected one near-boundary signal, got %+v", near)
	}
	nearSignal := requireCorridorBoundarySignalByMetric(t, near.BoundarySignals, "alignment")
	if nearSignal.Signal != corridorBoundarySignalNearLower || nearSignal.Severity != corridorBoundarySeverityWatch {
		t.Fatalf("expected near-lower watch signal, got %+v", nearSignal)
	}
	if mathAbs(nearSignal.NormalizedDelta-0.10) > 1e-9 {
		t.Fatalf("expected normalized near margin 0.10, got %+v", nearSignal)
	}
	if near.NearestMetric != "alignment" || near.NearestBound != "LOWER" || mathAbs(near.NearestDistance-0.10) > 1e-9 {
		t.Fatalf("expected nearest boundary signal on lower alignment bound, got %+v", near)
	}
	if near.MinNormalizedMargin <= 0 || near.NormalizedSeverity <= 0 || !strings.Contains(near.Summary, "sits near the current corridor boundary on alignment") {
		t.Fatalf("expected near-boundary summary semantics, got %+v", near)
	}

	violated := buildCorridorBoundaryClusterReport(corridorBoundaryBaseFitCluster("task:ws-boundary/violated", 0.90, corridorFitStatusOutOfCorridor))
	if violated.BasisState != corridorBoundaryBasisStateReady || violated.BoundaryState != corridorBoundaryStateViolated {
		t.Fatalf("expected ready/violated boundary cluster, got %+v", violated)
	}
	if violated.NearBoundaryMetricCount != 0 || violated.OutsideMetricCount != 1 || len(violated.BoundarySignals) != 1 {
		t.Fatalf("expected one violation signal, got %+v", violated)
	}
	violationSignal := requireCorridorBoundarySignalByMetric(t, violated.BoundarySignals, "alignment")
	if violationSignal.Signal != corridorBoundarySignalHighViolation || violationSignal.Severity != corridorBoundarySeverityCritical {
		t.Fatalf("expected critical upper-bound violation signal, got %+v", violationSignal)
	}
	if mathAbs(violationSignal.NormalizedDelta-0.50) > 1e-9 {
		t.Fatalf("expected normalized violation delta 0.50, got %+v", violationSignal)
	}
	if violated.CriticalViolationCount != 1 || violated.DominantViolationMetric != "alignment" || violated.DominantViolationDirection != "UPPER" {
		t.Fatalf("expected dominant upper alignment violation, got %+v", violated)
	}
	if violated.MaxViolationNormalizedDelta < corridorBoundaryCriticalThreshold || violated.NormalizedSeverity != violated.MaxViolationNormalizedDelta {
		t.Fatalf("expected violation severity to mirror max normalized delta, got %+v", violated)
	}
	if !strings.Contains(violated.Summary, "approximates a corridor-boundary violation on alignment") {
		t.Fatalf("expected violation summary semantics, got %+v", violated)
	}

	under := buildCorridorBoundaryClusterReport(corridorBoundaryBaseFitCluster("task:ws-boundary/under", 0.70, corridorFitStatusUnderEvidenced))
	if under.BasisState != corridorBoundaryBasisStateUnderEvidenced || under.BoundaryState != "" {
		t.Fatalf("expected under-evidenced basis-only state, got %+v", under)
	}
	if len(under.BoundarySignals) != 0 || under.NearBoundaryMetricCount != 0 || under.OutsideMetricCount != 0 {
		t.Fatalf("expected under-evidenced cluster to avoid boundary signals, got %+v", under)
	}
	if !strings.Contains(under.Summary, "does not yet have enough stable task-class evidence") {
		t.Fatalf("expected under-evidenced summary semantics, got %+v", under)
	}

	stale := buildCorridorBoundaryClusterReport(corridorBoundaryBaseFitCluster("task:ws-boundary/stale", 0.70, corridorFitStatusStaleBasis))
	if stale.BasisState != corridorBoundaryBasisStateStaleBasis || stale.BoundaryState != "" {
		t.Fatalf("expected stale-basis state, got %+v", stale)
	}
	if len(stale.BoundarySignals) != 0 || stale.NearBoundaryMetricCount != 0 || stale.OutsideMetricCount != 0 {
		t.Fatalf("expected stale cluster to avoid boundary signals, got %+v", stale)
	}
	if !strings.Contains(stale.Summary, "task-class basis is stale") {
		t.Fatalf("expected stale summary semantics, got %+v", stale)
	}
}

func TestBuildCorridorBoundaryReportDetailParity(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := prepareCorridorBoundaryScenario(t, ctx, store, "detail-parity")

	report, err := store.BuildCorridorBoundaryReport(ctx, CorridorBoundaryFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("build corridor boundary report: %v", err)
	}
	authority, err := store.GetWorkspaceTimeAuthority(ctx, scenario.workspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFields(t, report.TimeAuthority, authority)
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor boundary report generated_at %q to anchor to report time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if report.Filter.ProtoClusterID != scenario.clusterID {
		t.Fatalf("expected scoped corridor boundary report for %s, got %+v", scenario.clusterID, report.Filter)
	}
	if report.Workspace.TotalClusters != 1 {
		t.Fatalf("expected one corridor boundary cluster, got %+v", report.Workspace)
	}
	cluster := requireCorridorBoundaryClusterByID(t, report.Clusters, scenario.clusterID)
	if cluster.BoundarySource != corridorBoundarySourceFitDerived || cluster.BasisState != corridorBoundaryBasisStateReady {
		t.Fatalf("expected ready fit-derived boundary cluster, got %+v", cluster)
	}
	if cluster.CorridorLookup.CatalogKey != "incident" || cluster.CatalogRangeCheck.CatalogKey != "incident" {
		t.Fatalf("expected incident corridor boundary contract to preserve lookup/catalog parity, got %+v", cluster)
	}
	if cluster.BoundaryState == "" {
		t.Fatalf("expected evaluable boundary state for confirmed incident cluster, got %+v", cluster)
	}
	if report.Workspace.BasisStateCounts[cluster.BasisState] != 1 {
		t.Fatalf("expected workspace basis-state counts to include %+v, got %+v", cluster, report.Workspace)
	}
	if report.Workspace.BoundaryStateCounts[cluster.BoundaryState] != 1 {
		t.Fatalf("expected workspace boundary-state counts to include %+v, got %+v", cluster, report.Workspace)
	}
	if report.Workspace.TotalViolationSignals != cluster.OutsideMetricCount {
		t.Fatalf("expected violation counts to aggregate from the only cluster, got report=%+v cluster=%+v", report.Workspace, cluster)
	}
	if cluster.DominantViolationMetric != "" && report.Workspace.ViolationMetricCounts[cluster.DominantViolationMetric] == 0 {
		t.Fatalf("expected dominant violation metric to survive into workspace rollup, got report=%+v cluster=%+v", report.Workspace, cluster)
	}

	detail, err := store.BuildCorridorBoundaryClusterDetail(ctx, scenario.workspaceID, scenario.clusterID)
	if err != nil {
		t.Fatalf("build corridor boundary detail: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFields(t, detail.TimeAuthority, authority)
	if detail.Cluster.ProtoClusterID != scenario.clusterID || detail.Fit.ProtoClusterID != scenario.clusterID {
		t.Fatalf("expected scoped corridor boundary detail, got %+v", detail)
	}
	if detail.Cluster.BasisState != cluster.BasisState || detail.Cluster.BoundaryState != cluster.BoundaryState || detail.Cluster.Summary != cluster.Summary {
		t.Fatalf("expected report/detail boundary parity, report=%+v detail=%+v", cluster, detail.Cluster)
	}
	if !reflect.DeepEqual(detail.Cluster.BoundarySignals, cluster.BoundarySignals) {
		t.Fatalf("expected report/detail signal parity, report=%+v detail=%+v", cluster.BoundarySignals, detail.Cluster.BoundarySignals)
	}
	if !reflect.DeepEqual(detail.Cluster.CatalogRangeCheck.Ranges, detail.Fit.CatalogRangeCheck.Ranges) {
		t.Fatalf("expected boundary detail to preserve fit range payloads, cluster=%+v fit=%+v", detail.Cluster.CatalogRangeCheck, detail.Fit.CatalogRangeCheck)
	}
	if !reflect.DeepEqual(detail.Cluster.MetricVector, detail.Fit.MetricVector) {
		t.Fatalf("expected boundary detail to preserve fit metric vector parity, cluster=%+v fit=%+v", detail.Cluster.MetricVector, detail.Fit.MetricVector)
	}
	if len(detail.ConfirmedTensions) != 1 || detail.ConfirmedTensions[0].ReviewStatus != tensionReviewConfirmed {
		t.Fatalf("expected confirmed tension context in boundary detail, got %+v", detail.ConfirmedTensions)
	}
	if !reflect.DeepEqual(detail.Cluster.ConfirmedTensionIDs, cluster.ConfirmedTensionIDs) {
		t.Fatalf("expected confirmed tension id parity between report and detail, report=%+v detail=%+v", cluster.ConfirmedTensionIDs, detail.Cluster.ConfirmedTensionIDs)
	}
}

func TestCorridorBoundarySnapshotPayloadParityAndSyntheticExclusion(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := prepareCorridorBoundaryScenario(t, ctx, store, "snapshot")

	report, err := store.BuildCorridorBoundaryReport(ctx, CorridorBoundaryFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: scenario.clusterID,
		Limit:          1,
	})
	if err != nil {
		t.Fatalf("build scoped corridor boundary report: %v", err)
	}
	if report.Filter.ProtoClusterID != scenario.clusterID || len(report.Clusters) != 1 {
		t.Fatalf("expected scoped boundary report for %s, got %+v", scenario.clusterID, report)
	}
	cluster := requireCorridorBoundaryClusterByID(t, report.Clusters, scenario.clusterID)

	beforeInstrumentation, err := store.BuildInstrumentationReport(ctx, InstrumentationReportFilter{
		WorkspaceID:  scenario.workspaceID,
		Limit:        200,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("build instrumentation before boundary snapshot: %v", err)
	}
	beforeTensions, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions before boundary snapshot: %v", err)
	}

	event, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.corridor_boundary_snapshot",
		EntityType:  "instrumentation_corridor_boundary",
		EntityID:    scenario.clusterID,
		ActorType:   "operator",
		ActorID:     "dashboard",
		TaskID:      scenario.taskID,
		SessionID:   scenario.sessionID,
		PayloadJSON: mustJSONString(t, map[string]any{
			"workspace_id":     scenario.workspaceID,
			"proto_cluster_id": scenario.clusterID,
			"cluster":          cluster,
			"summary":          cluster.Summary,
			"typed_event_type": "CORRIDOR_BOUNDARY_SNAPSHOT",
			"event_kind":       "cluster.corridor_boundary_snapshot",
		}),
	})
	if err != nil {
		t.Fatalf("record corridor boundary snapshot: %v", err)
	}
	if event.EventType != "cluster.corridor_boundary_snapshot" || event.EntityType != "instrumentation_corridor_boundary" || event.EntityID != scenario.clusterID {
		t.Fatalf("unexpected corridor boundary snapshot event %+v", event)
	}

	var payload struct {
		WorkspaceID    string                        `json:"workspace_id"`
		ProtoClusterID string                        `json:"proto_cluster_id"`
		Cluster        CorridorBoundaryClusterReport `json:"cluster"`
		Summary        string                        `json:"summary"`
		TypedEventType string                        `json:"typed_event_type"`
		EventKind      string                        `json:"event_kind"`
	}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode corridor boundary snapshot payload: %v", err)
	}
	if payload.WorkspaceID != scenario.workspaceID || payload.ProtoClusterID != scenario.clusterID {
		t.Fatalf("expected scoped boundary snapshot payload, got %+v", payload)
	}
	if payload.TypedEventType != "CORRIDOR_BOUNDARY_SNAPSHOT" || payload.EventKind != event.EventType {
		t.Fatalf("expected typed/event-kind parity in boundary snapshot payload, got %+v", payload)
	}
	if payload.Summary != cluster.Summary || !reflect.DeepEqual(payload.Cluster, cluster) {
		t.Fatalf("expected boundary snapshot payload to preserve cluster parity, payload=%+v cluster=%+v", payload.Cluster, cluster)
	}

	afterInstrumentation, err := store.BuildInstrumentationReport(ctx, InstrumentationReportFilter{
		WorkspaceID:  scenario.workspaceID,
		Limit:        200,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("build instrumentation after boundary snapshot: %v", err)
	}
	beforeCluster := requireCorridorBoundaryProtoCluster(t, beforeInstrumentation.Clusters, scenario.clusterID)
	afterCluster := requireCorridorBoundaryProtoCluster(t, afterInstrumentation.Clusters, scenario.clusterID)
	if beforeCluster.Metrics.EventCount != afterCluster.Metrics.EventCount || beforeCluster.Metrics.BlockerSignalCount != afterCluster.Metrics.BlockerSignalCount {
		t.Fatalf("expected boundary snapshot exclusion to keep instrumentation metrics stable, before=%+v after=%+v", beforeCluster.Metrics, afterCluster.Metrics)
	}

	if _, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions after boundary snapshot: %v", err)
	}
	afterTensions, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after boundary snapshot: %v", err)
	}
	if !reflect.DeepEqual(corridorBoundaryTensionIDs(beforeTensions), corridorBoundaryTensionIDs(afterTensions)) {
		t.Fatalf("expected boundary snapshot exclusion to keep tension ids stable, before=%v after=%v", corridorBoundaryTensionIDs(beforeTensions), corridorBoundaryTensionIDs(afterTensions))
	}
}

type corridorBoundaryScenario struct {
	workspaceID string
	taskID      string
	sessionID   string
	clusterID   string
}

func corridorBoundaryProofRangeCheck() CorridorFitCatalogRangeCheck {
	return CorridorFitCatalogRangeCheck{
		CatalogKey:   "incident",
		DisplayName:  "Incident Corridor",
		TaskClass:    taskClassHintIncident,
		MatchSource:  model.TaskClassSourceExplicit,
		BasisFresh:   true,
		BasisSummary: "catalog lookup is backed by current task-class evidence",
		Ranges: []CorridorFitMetricRange{
			{Metric: "alignment", LowerBound: corridorFitBound(0.60), UpperBound: corridorFitBound(0.80)},
			{Metric: "progress", LowerBound: corridorFitBound(0.10), UpperBound: corridorFitBound(0.50)},
		},
	}
}

func corridorBoundaryBaseFitCluster(clusterID string, alignment float64, fitStatus string) CorridorFitClusterReport {
	rangeCheck := corridorBoundaryProofRangeCheck()
	vector := CorridorFitMetricVector{
		Alignment:       alignment,
		Differentiation: 0.42,
		Synergy:         0.09,
		Centralization:  0.25,
		Metastability:   0.18,
		Progress:        0.30,
	}
	gaps := corridorFitGapBreakdown(vector, rangeCheck.Ranges)
	if strings.TrimSpace(fitStatus) == "" {
		fitStatus = corridorFitStatusForGaps(gaps)
	}
	return CorridorFitClusterReport{
		ProtoClusterID:      clusterID,
		ResolutionKind:      "task",
		TaskIDs:             []string{strings.TrimPrefix(clusterID, "task:ws-boundary/")},
		TaskClass:           model.TaskClassIncident,
		TaskClassSource:     model.TaskClassSourceExplicit,
		TaskClassHint:       taskClassHintIncident,
		CorridorCatalogHint: "incident",
		CorridorLookup: CorridorLookupRecord{
			LookupStatus: corridorLookupStatusTemplateMatch,
			CatalogKey:   "incident",
			MatchSource:  model.TaskClassSourceExplicit,
		},
		CorridorReadiness:     corridorReadinessReady,
		ReadinessConfidence:   0.95,
		BasisStale:            fitStatus == corridorFitStatusStaleBasis,
		LastBasisEventAt:      "2026-03-23T00:00:00Z",
		Metrics:               ProtoClusterMetrics{EventCount: 6, LastEventAt: "2026-03-23T00:00:00Z"},
		CatalogRangeCheck:     rangeCheck,
		MetricVector:          vector,
		MetricGapBreakdown:    gaps,
		FitStatus:             fitStatus,
		FitConfidence:         0.91,
		FitScore:              84,
		ConfirmedTensionCount: 1,
		ConfirmedCountsByType: map[string]int{"bottleneck": 1},
		ConfirmedTensionIDs:   []string{"tension:" + clusterID},
		Summary:               "fit summary placeholder",
	}
}

func prepareCorridorBoundaryScenario(t *testing.T, ctx context.Context, store *Store, suffix string) corridorBoundaryScenario {
	t.Helper()

	base := seedBlockedTensionScenario(t, ctx, store, suffix)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE tasks
		 SET task_kind = ?, task_template = ?, task_class = ?, task_class_source = ?, task_class_updated_at = ?, title = ?, description = ?, tags_json = ?
		 WHERE task_id = ?`,
		model.TaskKindExecution,
		model.TaskTemplateBugfix,
		model.TaskClassIncident,
		model.TaskClassSourceExplicit,
		now,
		"Repair failing rollout",
		"Fix the deploy regression and restore the operator path.",
		`["incident","ops"]`,
		base.taskID,
	); err != nil {
		t.Fatalf("update task metadata for corridor boundary scenario: %v", err)
	}

	if _, err := store.SendMessage(ctx, MessageSendInput{
		WorkspaceID: base.workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Channel:     "ops",
		Content:     "Need operator confirmation for corridor boundary coverage",
	}); err != nil {
		t.Fatalf("send message for corridor boundary scenario: %v", err)
	}

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  base.workspaceID,
		ActorID:      "tests",
		Limit:        100,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("refresh tensions for corridor boundary scenario: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected corridor boundary scenario to create tensions, got %+v", refresh)
	}

	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: base.workspaceID,
		TaskID:      base.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions for corridor boundary scenario: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: base.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for corridor boundary coverage",
	}); err != nil {
		t.Fatalf("confirm tension for corridor boundary scenario: %v", err)
	}

	return corridorBoundaryScenario{
		workspaceID: base.workspaceID,
		taskID:      base.taskID,
		sessionID:   base.sessionID,
		clusterID:   "task:" + base.workspaceID + "/" + base.taskID,
	}
}

func requireCorridorBoundaryClusterByID(t *testing.T, items []CorridorBoundaryClusterReport, clusterID string) CorridorBoundaryClusterReport {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == clusterID {
			return item
		}
	}
	t.Fatalf("corridor boundary cluster %s not found in %+v", clusterID, items)
	return CorridorBoundaryClusterReport{}
}

func requireCorridorBoundaryProtoCluster(t *testing.T, items []ProtoClusterReport, clusterID string) ProtoClusterReport {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == clusterID {
			return item
		}
	}
	t.Fatalf("proto cluster %s not found in %+v", clusterID, items)
	return ProtoClusterReport{}
}

func requireCorridorBoundarySignalByMetric(t *testing.T, items []CorridorBoundaryMetricSignal, metric string) CorridorBoundaryMetricSignal {
	t.Helper()
	for _, item := range items {
		if item.Metric == metric {
			return item
		}
	}
	t.Fatalf("corridor boundary signal for %s not found in %+v", metric, items)
	return CorridorBoundaryMetricSignal{}
}

func corridorBoundaryTensionIDs(items []TensionRecord) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.TensionID)
	}
	return out
}
