package server

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationCorridorOwnershipRPCSurface(t *testing.T) {
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
		t.Fatalf("expected corridor ownership rpc scenario to create tensions, got %+v", refresh)
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
		Reason:      "confirm for corridor ownership rpc",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	clusterID := "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID
	if _, err := store.ElectClusterSteward(ctx, sqlite.ElectStewardInput{
		ClusterID:   clusterID,
		EpochID:     "epoch-handler-corridor-ownership",
		CandidateID: "agent-a",
		TTLSeconds:  300,
	}); err != nil {
		t.Fatalf("elect corridor ownership steward: %v", err)
	}
	rawReport, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal corridor ownership report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationCorridorOwnershipReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorOwnershipReport rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected report result type %T", result)
	}
	report, ok := reportPayload["report"].(sqlite.CorridorOwnershipReport)
	if !ok {
		t.Fatalf("unexpected corridor ownership report payload type %T", reportPayload["report"])
	}
	if report.GeneratedAt == "" || report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor ownership report generated_at to mirror time authority reference_at, report=%+v", report)
	}
	cluster := requireServerCorridorOwnershipCluster(t, report.Clusters, clusterID)
	if cluster.Ownership.OwnershipState != "OWNED_EXPLICIT" || cluster.Ownership.OwnerTaskID != scenario.primaryTaskID {
		t.Fatalf("expected owned explicit cluster, got %+v", cluster)
	}
	if cluster.Ownership.BasisTaskClass != model.TaskClassIncident || cluster.Ownership.BasisTaskClassSource != model.TaskClassSourceExplicit || !cluster.Ownership.BasisAuthoritative {
		t.Fatalf("expected explicit incident ownership basis in report cluster, got %+v", cluster)
	}
	if cluster.Steward == nil || cluster.Steward.StewardAgentID != "agent-a" || cluster.Steward.EpochID != "epoch-handler-corridor-ownership" || cluster.Steward.Status != "ACTIVE" {
		t.Fatalf("expected active steward lease in report cluster, got %+v", cluster.Steward)
	}

	rawCluster, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
	})
	if err != nil {
		t.Fatalf("marshal corridor ownership cluster params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationCorridorOwnershipCluster(ctx, rawCluster)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorOwnershipCluster rpc error: %+v", rpcErr)
	}
	clusterPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected cluster result type %T", result)
	}
	detail, ok := clusterPayload["detail"].(sqlite.CorridorOwnershipClusterDetail)
	if !ok {
		t.Fatalf("unexpected corridor ownership detail type %T", clusterPayload["detail"])
	}
	if detail.Cluster.ProtoClusterID != clusterID || len(detail.Tasks) == 0 {
		t.Fatalf("expected scoped corridor ownership detail, got %+v", detail)
	}
	if !reflect.DeepEqual(detail.Cluster.Ownership, cluster.Ownership) {
		t.Fatalf("expected ownership detail parity, report=%+v detail=%+v", cluster.Ownership, detail.Cluster.Ownership)
	}
	if detail.Cluster.TaskClassHint != cluster.TaskClassHint || !reflect.DeepEqual(detail.Cluster.CorridorLookup, cluster.CorridorLookup) {
		t.Fatalf("expected ownership detail to preserve upstream corridor hint/lookup parity, report=%+v detail=%+v", cluster, detail.Cluster)
	}
	if detail.Cluster.Steward == nil || !reflect.DeepEqual(detail.Cluster.Steward, cluster.Steward) {
		t.Fatalf("expected steward lease detail/report parity, report=%+v detail=%+v", cluster.Steward, detail.Cluster.Steward)
	}

	rawSnapshot, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		ActorID:        "dashboard",
		Limit:          5,
	})
	if err != nil {
		t.Fatalf("marshal corridor ownership snapshot params: %v", err)
	}
	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)
	result, rpcErr = h.workspaceInstrumentationCorridorOwnershipSnapshot(ctx, rawSnapshot)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorOwnershipSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected snapshot result type %T", result)
	}
	snapshotReport, ok := snapshotPayload["report"].(sqlite.CorridorOwnershipReport)
	if !ok {
		t.Fatalf("unexpected corridor ownership snapshot report type %T", snapshotPayload["report"])
	}
	if snapshotReport.Filter.ProtoClusterID != clusterID || len(snapshotReport.Clusters) != 1 {
		t.Fatalf("expected scoped corridor ownership snapshot report for %s, got %+v", clusterID, snapshotReport)
	}
	if snapshotReport.GeneratedAt == "" || snapshotReport.GeneratedAt != snapshotReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor ownership snapshot generated_at to mirror time authority reference_at, report=%+v", snapshotReport)
	}
	if snapshotReport.Workspace.ActiveStewardCount != 1 || snapshotReport.Clusters[0].Steward == nil {
		t.Fatalf("expected steward lease to persist in corridor ownership snapshot report, got %+v", snapshotReport)
	}
	event, ok := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected corridor ownership snapshot event type %T", snapshotPayload["event"])
	}
	if event.EventType != "cluster.corridor_ownership_snapshot" || event.EntityType != "instrumentation_corridor_ownership" || event.EntityID != clusterID {
		t.Fatalf("unexpected corridor ownership snapshot event %+v", event)
	}
	liveEvent := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, liveEvent, event, "cluster.corridor_ownership_snapshot")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, liveEvent.PayloadJSON), event.PayloadJSON)
}

func TestWorkspaceInstrumentationCorridorOwnershipRPCRejectsInvalidParams(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	tests := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params workspaceInstrumentationCorridorParams
	}{
		{name: "report", call: h.workspaceInstrumentationCorridorOwnershipReport, params: workspaceInstrumentationCorridorParams{Limit: 10}},
		{name: "cluster missing workspace", call: h.workspaceInstrumentationCorridorOwnershipCluster, params: workspaceInstrumentationCorridorParams{ProtoClusterID: "task:ws/task"}},
		{name: "cluster missing proto cluster", call: h.workspaceInstrumentationCorridorOwnershipCluster, params: workspaceInstrumentationCorridorParams{WorkspaceID: "ws-only"}},
		{name: "snapshot", call: h.workspaceInstrumentationCorridorOwnershipSnapshot, params: workspaceInstrumentationCorridorParams{ActorID: "dashboard", Limit: 5}},
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

func requireServerCorridorOwnershipCluster(t *testing.T, items []sqlite.CorridorOwnershipClusterReport, clusterID string) sqlite.CorridorOwnershipClusterReport {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == clusterID {
			return item
		}
	}
	t.Fatalf("corridor ownership cluster %s not found in %+v", clusterID, items)
	return sqlite.CorridorOwnershipClusterReport{}
}
