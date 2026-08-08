package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestBuildCorridorFitClusterReportMapsProofLikeClusterIntoCorridor(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	cluster := CorridorClusterReport{
		ProtoClusterID:      "task:ws-corridor-fit/proof-cluster",
		ResolutionKind:      "task",
		TaskIDs:             []string{"task-proof"},
		SessionIDs:          []string{"sess-proof"},
		AgentIDs:            []string{"agent-a", "agent-b"},
		TaskClass:           taskClassHintProof,
		TaskClassSource:     model.TaskClassSourceExplicit,
		TaskClassUpdatedAt:  now,
		TaskClassHint:       taskClassHintProof,
		CorridorCatalogHint: "proof",
		CorridorLookup: CorridorLookupRecord{
			LookupStatus: corridorLookupStatusClassMatch,
			CatalogKey:   "proof",
			DisplayName:  "Proof Corridor",
			MatchSource:  "authored_task_class_dominant",
		},
		CorridorReadiness:   corridorReadinessReady,
		ReadinessConfidence: 0.95,
		LastBasisEventAt:    now,
		Metrics: ProtoClusterMetrics{
			EventCount:                  10,
			OpenQueueCount:              1,
			BlockerSignalCount:          3,
			BlockerDensity:              0.30,
			ActivityShareByAgent:        map[string]float64{"agent-a": 0.9, "agent-b": 0.1},
			MaxAgentActivityShare:       0.39,
			CommunicationCentralization: 0.34,
			DuplicationIndex:            0.20,
			EventTypeCounts:             map[string]int{"task.completed": 3, "agent.session.blocked": 2, "agent.update.posted": 4},
			LastEventAt:                 now,
		},
	}
	item := buildCorridorFitClusterReport(cluster, []TensionRecord{{
		TensionID:      "tension-proof-ambiguity",
		ProtoClusterID: cluster.ProtoClusterID,
		TensionType:    "ambiguity",
		ReviewStatus:   tensionReviewConfirmed,
	}}, CorridorOwnershipDigest{
		OwnershipState:       corridorOwnershipStateOwnedExplicit,
		BasisTaskClass:       taskClassHintProof,
		BasisTaskClassSource: model.TaskClassSourceExplicit,
		BasisFresh:           true,
		BasisAuthoritative:   true,
		OwnerTaskID:          "task-proof",
		OwnerTaskIDs:         []string{"task-proof"},
	})
	if item.CatalogRangeCheck.CatalogKey != "proof" || !item.CatalogRangeCheck.BasisFresh {
		t.Fatalf("expected proof catalog range check with fresh basis, got %+v", item.CatalogRangeCheck)
	}
	if item.FitStatus != corridorFitStatusInCorridor {
		t.Fatalf("expected proof-like cluster to fit inside corridor, got %+v", item)
	}
	if item.FitScore <= 0 || item.FitConfidence < 0.8 {
		t.Fatalf("expected positive fit score/confidence, got %+v", item)
	}
	if len(item.MetricGapBreakdown) != 6 {
		t.Fatalf("expected complete gap breakdown, got %+v", item.MetricGapBreakdown)
	}
	for _, gap := range item.MetricGapBreakdown {
		if gap.Status != "IN_RANGE" {
			t.Fatalf("expected proof-like cluster to stay in range, got %+v", item.MetricGapBreakdown)
		}
	}
}

func TestBuildCorridorFitClusterReportMarksStaleBasisWithoutPromotingPolicyAuthority(t *testing.T) {
	t.Parallel()

	cluster := CorridorClusterReport{
		ProtoClusterID:      "task:ws-corridor-fit/stale-cluster",
		ResolutionKind:      "task",
		TaskIDs:             []string{"task-stale"},
		TaskClass:           taskClassHintIncident,
		TaskClassSource:     model.TaskClassSourceExplicit,
		TaskClassHint:       taskClassHintIncident,
		CorridorCatalogHint: "incident",
		CorridorLookup: CorridorLookupRecord{
			LookupStatus: corridorLookupStatusClassMatch,
			CatalogKey:   "incident",
			DisplayName:  "Incident Corridor",
			MatchSource:  "authored_task_class_dominant",
		},
		CorridorReadiness:   corridorReadinessStaleBasis,
		ReadinessConfidence: 0.88,
		BasisStale:          true,
		Metrics: ProtoClusterMetrics{
			EventCount:                  6,
			BlockerDensity:              0.35,
			ActivityShareByAgent:        map[string]float64{"agent-a": 1},
			MaxAgentActivityShare:       1,
			CommunicationCentralization: 1,
			DuplicationIndex:            0.20,
			EventTypeCounts:             map[string]int{"task.blocked": 1, "task.completed": 1},
			LastEventAt:                 time.Now().UTC().Format(time.RFC3339Nano),
		},
	}
	item := buildCorridorFitClusterReport(cluster, []TensionRecord{{
		TensionID:      "tension-stale-bottleneck",
		ProtoClusterID: cluster.ProtoClusterID,
		TensionType:    "bottleneck",
		ReviewStatus:   tensionReviewConfirmed,
	}}, CorridorOwnershipDigest{
		OwnershipState:       corridorOwnershipStateOwnedExplicitStale,
		BasisTaskClass:       taskClassHintIncident,
		BasisTaskClassSource: model.TaskClassSourceExplicit,
		BasisFresh:           false,
		BasisAuthoritative:   true,
		OwnerTaskID:          "task-stale",
		OwnerTaskIDs:         []string{"task-stale"},
	})
	if item.FitStatus != corridorFitStatusStaleBasis {
		t.Fatalf("expected stale-basis fit status, got %+v", item)
	}
	if item.CatalogRangeCheck.BasisFresh {
		t.Fatalf("expected stale basis to be reflected in catalog range check, got %+v", item.CatalogRangeCheck)
	}
	if len(item.MetricGapBreakdown) != 0 {
		t.Fatalf("expected stale basis to avoid authoritative gap breakdown, got %+v", item.MetricGapBreakdown)
	}
}

func TestCorridorFitSummaryKeepsNearBoundaryLanguageForSlightSingleMetricDrift(t *testing.T) {
	t.Parallel()

	gaps := []CorridorFitMetricGap{
		{
			Metric: "alignment",
			Delta:  -0.02,
			Status: "LOW",
		},
	}
	fitStatus := corridorFitStatusForGaps(gaps)
	if fitStatus != corridorFitStatusNearBoundary {
		t.Fatalf("expected slight single-metric drift to stay near boundary, got %s", fitStatus)
	}

	summary := corridorFitSummary(
		CorridorClusterReport{ProtoClusterID: "task:ws-corridor-fit/near-boundary"},
		CorridorFitCatalogRangeCheck{DisplayName: "Incident Corridor"},
		fitStatus,
		gaps,
		map[string]int{},
	)
	if !strings.Contains(summary, "near the Incident Corridor approximation boundary") {
		t.Fatalf("expected near-boundary summary language, got %q", summary)
	}
	if strings.Contains(summary, "falls outside") {
		t.Fatalf("expected near-boundary summary not to describe the cluster as outside, got %q", summary)
	}
}

func TestBuildCorridorFitReportAndSnapshot(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "corridor-fit-report")
	now := time.Now().UTC().Format(time.RFC3339Nano)

	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE tasks
		 SET task_template = ?, task_class = ?, task_class_source = ?, task_class_updated_at = ?, title = ?, description = ?
		 WHERE task_id = ?`,
		model.TaskTemplateBugfix,
		model.TaskClassIncident,
		model.TaskClassSourceExplicit,
		now,
		"Repair failing rollout",
		"Fix the outage and restore the operator path.",
		scenario.taskID,
	); err != nil {
		t.Fatalf("update task metadata for corridor fit: %v", err)
	}

	refresh, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected at least one detected tension before corridor fit, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for corridor fit",
	}); err != nil {
		t.Fatalf("confirm corridor fit tension: %v", err)
	}

	clusterID := "task:" + scenario.workspaceID + "/" + scenario.taskID
	report, err := store.BuildCorridorFitReport(ctx, CorridorFitFilter{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("build corridor fit report: %v", err)
	}
	authority, err := store.GetWorkspaceTimeAuthority(ctx, scenario.workspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFields(t, report.TimeAuthority, authority)
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor fit report generated_at %q to anchor to report time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	cluster := requireCorridorFitClusterByID(t, report.Clusters, clusterID)
	if cluster.CatalogRangeCheck.CatalogKey != "incident" {
		t.Fatalf("expected incident catalog key, got %+v", cluster)
	}
	if cluster.ConfirmedCountsByType["bottleneck"] != 1 || cluster.ConfirmedTensionCount != 1 {
		t.Fatalf("expected confirmed bottleneck to stay visible in fit report, got %+v", cluster)
	}
	if cluster.FitStatus == corridorFitStatusUnderEvidenced || cluster.FitStatus == corridorFitStatusStaleBasis {
		t.Fatalf("expected fit report to be evaluable once authored basis and metrics exist, got %+v", cluster)
	}
	detail, err := store.BuildCorridorFitClusterDetail(ctx, scenario.workspaceID, clusterID)
	if err != nil {
		t.Fatalf("build corridor fit detail: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFields(t, detail.TimeAuthority, authority)
	if detail.Cluster.ProtoClusterID != clusterID {
		t.Fatalf("expected corridor fit detail scoped to %s, got %+v", clusterID, detail)
	}
	if detail.Cluster.FitStatus != cluster.FitStatus || detail.Cluster.FitScore != cluster.FitScore {
		t.Fatalf("expected corridor fit detail/report parity, report=%+v detail=%+v", cluster, detail.Cluster)
	}

	event, err := store.RecordCorridorFitSnapshot(ctx, report, CorridorFitSnapshotInput{
		ActorID: "dashboard",
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("record corridor fit snapshot: %v", err)
	}
	if event.EventType != "cluster.corridor_fit_snapshot" || event.EntityType != "instrumentation_corridor_fit" {
		t.Fatalf("unexpected corridor fit snapshot event %+v", event)
	}
	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.corridor_fit_snapshot",
		EntityType:  "instrumentation_corridor_fit",
		EntityID:    clusterID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list persisted corridor fit snapshots: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one persisted corridor fit snapshot, got %+v", events)
	}
	if !strings.Contains(event.PayloadJSON, "\"generated_at\":\""+report.GeneratedAt+"\"") {
		t.Fatalf("expected corridor fit snapshot payload to mirror report generated_at %q, got %s", report.GeneratedAt, event.PayloadJSON)
	}
	if !strings.Contains(event.PayloadJSON, "\"typed_event_type\":\"CORRIDOR_FIT_SNAPSHOT\"") {
		t.Fatalf("expected corridor fit snapshot payload to carry typed_event_type, got %s", event.PayloadJSON)
	}
}

func requireCorridorFitClusterByID(t *testing.T, items []CorridorFitClusterReport, clusterID string) CorridorFitClusterReport {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == clusterID {
			return item
		}
	}
	t.Fatalf("corridor fit cluster %s not found in %+v", clusterID, items)
	return CorridorFitClusterReport{}
}
