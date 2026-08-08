package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationCorridorFitRPCSurface(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	if _, err := store.DB().ExecContext(
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
		scenario.primaryTaskID,
	); err != nil {
		t.Fatalf("update task metadata: %v", err)
	}

	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected corridor fit rpc scenario to create tensions, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.primaryTaskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	primary := requireTensionRecordByType(t, items, "bottleneck")
	if _, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "operator",
		Reason:      "confirm for corridor fit rpc",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	clusterID := "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID
	rawReport, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal corridor fit report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationCorridorFitReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorFitReport rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected report result type %T", result)
	}
	report, ok := reportPayload["report"].(sqlite.CorridorFitReport)
	if !ok {
		t.Fatalf("unexpected corridor fit report payload type %T", reportPayload["report"])
	}
	if report.GeneratedAt == "" || report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor fit report generated_at to mirror time authority reference_at, report=%+v", report)
	}
	cluster := requireServerCorridorFitCluster(t, report.Clusters, clusterID)
	if cluster.CatalogRangeCheck.CatalogKey != "incident" {
		t.Fatalf("expected incident corridor-fit catalog key, got %+v", cluster)
	}
	if cluster.ConfirmedCountsByType["bottleneck"] != 1 || cluster.ConfirmedTensionCount != 1 {
		t.Fatalf("expected confirmed bottleneck evidence in corridor fit cluster, got %+v", cluster)
	}
	if cluster.FitStatus == "UNDER_EVIDENCED" || cluster.FitStatus == "STALE_BASIS" {
		t.Fatalf("expected evaluable corridor fit cluster, got %+v", cluster)
	}

	rawCluster, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
	})
	if err != nil {
		t.Fatalf("marshal corridor fit cluster params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationCorridorFitCluster(ctx, rawCluster)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorFitCluster rpc error: %+v", rpcErr)
	}
	clusterPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected cluster result type %T", result)
	}
	detail, ok := clusterPayload["detail"].(sqlite.CorridorFitClusterDetail)
	if !ok {
		t.Fatalf("unexpected corridor fit detail type %T", clusterPayload["detail"])
	}
	if detail.Cluster.ProtoClusterID != clusterID || len(detail.ConfirmedTensions) == 0 {
		t.Fatalf("expected confirmed tensions in corridor fit detail, got %+v", detail)
	}

	rawSnapshot, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		ActorID:        "dashboard",
		Limit:          5,
	})
	if err != nil {
		t.Fatalf("marshal corridor fit snapshot params: %v", err)
	}
	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)
	result, rpcErr = h.workspaceInstrumentationCorridorFitSnapshot(ctx, rawSnapshot)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorFitSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected snapshot result type %T", result)
	}
	snapshotReport, ok := snapshotPayload["report"].(sqlite.CorridorFitReport)
	if !ok {
		t.Fatalf("unexpected corridor fit snapshot report type %T", snapshotPayload["report"])
	}
	if snapshotReport.Filter.ProtoClusterID != clusterID || len(snapshotReport.Clusters) != 1 {
		t.Fatalf("expected scoped corridor fit snapshot report for %s, got %+v", clusterID, snapshotReport)
	}
	if snapshotReport.GeneratedAt == "" || snapshotReport.GeneratedAt != snapshotReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor fit snapshot generated_at to mirror time authority reference_at, report=%+v", snapshotReport)
	}
	event, ok := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected corridor fit snapshot event type %T", snapshotPayload["event"])
	}
	if event.EventType != "cluster.corridor_fit_snapshot" || event.EntityType != "instrumentation_corridor_fit" || event.EntityID != clusterID {
		t.Fatalf("unexpected corridor fit snapshot event %+v", event)
	}
	liveEvent := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, liveEvent, event, "cluster.corridor_fit_snapshot")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), event.PayloadJSON)
}

func TestWorkspaceInstrumentationCorridorFitRPCRejectsInvalidParams(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	tests := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params workspaceInstrumentationCorridorParams
	}{
		{name: "report", call: h.workspaceInstrumentationCorridorFitReport, params: workspaceInstrumentationCorridorParams{Limit: 10}},
		{name: "cluster", call: h.workspaceInstrumentationCorridorFitCluster, params: workspaceInstrumentationCorridorParams{WorkspaceID: "ws-only"}},
		{name: "snapshot", call: h.workspaceInstrumentationCorridorFitSnapshot, params: workspaceInstrumentationCorridorParams{ActorID: "dashboard", Limit: 5}},
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

func requireServerCorridorFitCluster(t *testing.T, items []sqlite.CorridorFitClusterReport, clusterID string) sqlite.CorridorFitClusterReport {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == clusterID {
			return item
		}
	}
	t.Fatalf("corridor fit cluster %s not found in %+v", clusterID, items)
	return sqlite.CorridorFitClusterReport{}
}
