package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationCorridorBoundaryRPCSurface(t *testing.T) {
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
		t.Fatalf("expected corridor boundary rpc scenario to create tensions, got %+v", refresh)
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
		Reason:      "confirm for corridor boundary rpc",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	clusterID := "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID
	rawReport, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal corridor boundary report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationCorridorBoundaryReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorBoundaryReport rpc error: %+v", rpcErr)
	}
	reportPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected report result type %T", result)
	}
	report, ok := reportPayload["report"].(sqlite.CorridorBoundaryReport)
	if !ok {
		t.Fatalf("unexpected corridor boundary report payload type %T", reportPayload["report"])
	}
	if report.GeneratedAt == "" || report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor boundary report generated_at to mirror time authority reference_at, report=%+v", report)
	}
	cluster := requireServerCorridorBoundaryCluster(t, report.Clusters, clusterID)
	if cluster.BasisState != "READY" || cluster.BoundarySource != "FIT_DERIVED" {
		t.Fatalf("expected fit-derived ready boundary cluster, got %+v", cluster)
	}
	if cluster.CorridorLookup.CatalogKey != "incident" {
		t.Fatalf("expected incident corridor lookup to survive into boundary report, got %+v", cluster)
	}

	rawCluster, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
	})
	if err != nil {
		t.Fatalf("marshal corridor boundary cluster params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationCorridorBoundaryCluster(ctx, rawCluster)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorBoundaryCluster rpc error: %+v", rpcErr)
	}
	clusterPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected cluster result type %T", result)
	}
	detail, ok := clusterPayload["detail"].(sqlite.CorridorBoundaryClusterDetail)
	if !ok {
		t.Fatalf("unexpected corridor boundary detail type %T", clusterPayload["detail"])
	}
	if detail.Cluster.ProtoClusterID != clusterID || detail.Fit.ProtoClusterID != clusterID {
		t.Fatalf("expected scoped corridor boundary detail, got %+v", detail)
	}
	if detail.Cluster.BasisState != cluster.BasisState || detail.Cluster.BoundaryState != cluster.BoundaryState {
		t.Fatalf("expected corridor boundary detail parity, report=%+v detail=%+v", cluster, detail.Cluster)
	}
}

func TestWorkspaceInstrumentationCorridorBoundaryRPCRejectsInvalidParams(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	tests := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params workspaceInstrumentationCorridorParams
	}{
		{name: "report", call: h.workspaceInstrumentationCorridorBoundaryReport, params: workspaceInstrumentationCorridorParams{Limit: 10}},
		{name: "cluster missing workspace", call: h.workspaceInstrumentationCorridorBoundaryCluster, params: workspaceInstrumentationCorridorParams{ProtoClusterID: "task:ws/task"}},
		{name: "cluster missing proto cluster", call: h.workspaceInstrumentationCorridorBoundaryCluster, params: workspaceInstrumentationCorridorParams{WorkspaceID: "ws-only"}},
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

func requireServerCorridorBoundaryCluster(t *testing.T, items []sqlite.CorridorBoundaryClusterReport, clusterID string) sqlite.CorridorBoundaryClusterReport {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == clusterID {
			return item
		}
	}
	t.Fatalf("corridor boundary cluster %s not found in %+v", clusterID, items)
	return sqlite.CorridorBoundaryClusterReport{}
}
