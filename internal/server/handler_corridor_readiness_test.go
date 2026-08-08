package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationCorridorRPCSurface(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE tasks SET task_kind = ?, task_template = ?, title = ?, description = ?, tags_json = ? WHERE task_id = ?`,
		model.TaskKindCoordination,
		model.TaskTemplateResearch,
		"Explore instrumentation rollout",
		"Research the corridor basis for this cluster.",
		`["discovery"]`,
		scenario.primaryTaskID,
	); err != nil {
		t.Fatalf("update primary task metadata: %v", err)
	}

	rawReport, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal corridor report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationCorridorReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorReport rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected report result type %T", result)
	}
	report, ok := reportPayload["report"].(sqlite.CorridorReadinessReport)
	if !ok {
		t.Fatalf("unexpected corridor report payload type %T", reportPayload["report"])
	}
	if report.WorkspaceID != scenario.workspaceID {
		t.Fatalf("expected workspace %s, got %+v", scenario.workspaceID, report)
	}
	if len(report.Catalog) != 4 {
		t.Fatalf("expected explicit corridor catalog in report, got %+v", report.Catalog)
	}
	clusterID := "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID
	cluster := requireServerCorridorCluster(t, report.Clusters, clusterID)
	if cluster.TaskClassHint != "EXPLORATION" || cluster.CorridorReadiness != "READY" {
		t.Fatalf("expected exploration task class hint, got %+v", cluster)
	}
	if cluster.CorridorCatalogHint != "exploration" || len(cluster.TaskClassBasis) == 0 {
		t.Fatalf("expected report cluster to expose corridor catalog hint and basis, got %+v", cluster)
	}
	if cluster.CorridorLookup.LookupStatus != "TEMPLATE_MATCH" || cluster.CorridorLookup.CatalogKey != "exploration" {
		t.Fatalf("expected report cluster to expose template-backed lookup, got %+v", cluster.CorridorLookup)
	}
	if report.Workspace.ReadyCount == 0 || report.Workspace.TaskClassCounts["EXPLORATION"] == 0 {
		t.Fatalf("expected workspace readiness summary to include exploration-ready cluster, got %+v", report.Workspace)
	}
	if report.Workspace.LookupStatusCounts["TEMPLATE_MATCH"] == 0 {
		t.Fatalf("expected workspace lookup summary to include template matches, got %+v", report.Workspace)
	}
	if report.GeneratedAt == "" || report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor report generated_at to mirror time authority reference_at, report=%+v", report)
	}

	rawCluster, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
	})
	if err != nil {
		t.Fatalf("marshal corridor cluster params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationCorridorCluster(ctx, rawCluster)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorCluster rpc error: %+v", rpcErr)
	}
	clusterPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected cluster result type %T", result)
	}
	detail, ok := clusterPayload["detail"].(sqlite.CorridorClusterDetail)
	if !ok {
		t.Fatalf("unexpected cluster detail type %T", clusterPayload["detail"])
	}
	if detail.Cluster.ProtoClusterID != clusterID || len(detail.Tasks) == 0 {
		t.Fatalf("unexpected corridor detail %+v", detail)
	}
	if detail.Cluster.CorridorCatalogHint != "exploration" || len(detail.Cluster.TaskClassBasis) == 0 {
		t.Fatalf("expected corridor cluster detail to expose catalog lookup basis, got %+v", detail.Cluster)
	}
	if detail.Tasks[0].TaskClassHint != "EXPLORATION" || detail.Tasks[0].TaskKind != model.TaskKindCoordination || detail.Tasks[0].TaskTemplate != model.TaskTemplateResearch {
		t.Fatalf("expected corridor detail to preserve task metadata and hint, got %+v", detail.Tasks[0])
	}
	if detail.Tasks[0].CorridorHint != "exploration" || len(detail.Tasks[0].TaskClassBasis) == 0 {
		t.Fatalf("expected corridor detail basis and catalog hint, got %+v", detail.Tasks[0])
	}
	if detail.Tasks[0].CorridorLookup.LookupStatus != "TEMPLATE_MATCH" || detail.Tasks[0].BasisUpdatedAt == "" {
		t.Fatalf("expected corridor detail to surface lookup status and basis freshness, got %+v", detail.Tasks[0])
	}

	rawSnapshot, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		ActorID:        "dashboard",
		Limit:          5,
	})
	if err != nil {
		t.Fatalf("marshal corridor snapshot params: %v", err)
	}
	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)
	result, rpcErr = h.workspaceInstrumentationCorridorSnapshot(ctx, rawSnapshot)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected snapshot result type %T", result)
	}
	snapshotReport, ok := snapshotPayload["report"].(sqlite.CorridorReadinessReport)
	if !ok {
		t.Fatalf("unexpected corridor snapshot report type %T", snapshotPayload["report"])
	}
	if snapshotReport.Filter.ProtoClusterID != clusterID || len(snapshotReport.Clusters) != 1 {
		t.Fatalf("expected scoped corridor snapshot report for %s, got %+v", clusterID, snapshotReport)
	}
	if len(snapshotReport.Catalog) != 4 {
		t.Fatalf("expected scoped corridor snapshot to carry catalog entries, got %+v", snapshotReport.Catalog)
	}
	if snapshotReport.GeneratedAt == "" || snapshotReport.GeneratedAt != snapshotReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor snapshot report generated_at to mirror time authority reference_at, report=%+v", snapshotReport)
	}
	event, ok := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected corridor snapshot event type %T", snapshotPayload["event"])
	}
	if event.EventType != "cluster.corridor_readiness_snapshot" || event.EntityType != "instrumentation_corridor" || event.EntityID != clusterID {
		t.Fatalf("unexpected corridor snapshot event %+v", event)
	}
	if event.ActorID != "dashboard" {
		t.Fatalf("expected snapshot actor dashboard, got %+v", event)
	}
	liveEvent := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, liveEvent, event, "cluster.corridor_readiness_snapshot")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), event.PayloadJSON)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.corridor_readiness_snapshot",
		EntityType:  "instrumentation_corridor",
		EntityID:    clusterID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list corridor readiness snapshot runtime events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one persisted corridor snapshot event, got %+v", events)
	}
}

func TestWorkspaceInstrumentationCorridorRPCRejectsInvalidParams(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	tests := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params workspaceInstrumentationCorridorParams
	}{
		{name: "report", call: h.workspaceInstrumentationCorridorReport, params: workspaceInstrumentationCorridorParams{Limit: 10}},
		{name: "cluster", call: h.workspaceInstrumentationCorridorCluster, params: workspaceInstrumentationCorridorParams{WorkspaceID: "ws-only"}},
		{name: "snapshot", call: h.workspaceInstrumentationCorridorSnapshot, params: workspaceInstrumentationCorridorParams{ActorID: "dashboard", Limit: 5}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			if _, rpcErr := tc.call(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params error, got %+v", rpcErr)
			}
		})
	}
}

func TestWorkspaceInstrumentationCorridorClusterRejectsUnknownProtoCluster(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	raw, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: "task:" + scenario.workspaceID + "/missing",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	if _, rpcErr := h.workspaceInstrumentationCorridorCluster(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for unknown corridor proto cluster, got %+v", rpcErr)
	}
}

func requireServerCorridorCluster(t *testing.T, items []sqlite.CorridorClusterReport, clusterID string) sqlite.CorridorClusterReport {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == clusterID {
			return item
		}
	}
	t.Fatalf("corridor cluster %s not found in %+v", clusterID, items)
	return sqlite.CorridorClusterReport{}
}
