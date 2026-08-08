package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationCorridorMixedClusterDoesNotExposeAuthoritativeTaskClass(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	for _, update := range []struct {
		taskID      string
		title       string
		description string
		taskClass   string
		template    string
	}{
		{
			taskID:      scenario.primaryTaskID,
			title:       "Refactor adapter boundary",
			description: "Connect the integration transport path.",
			taskClass:   model.TaskClassIntegration,
			template:    model.TaskTemplateIntegration,
		},
		{
			taskID:      scenario.secondaryTaskID,
			title:       "Repair rollout regression",
			description: "Fix the deploy incident and restore service.",
			taskClass:   model.TaskClassIncident,
			template:    model.TaskTemplateBugfix,
		},
	} {
		if _, err := store.DB().ExecContext(
			ctx,
			`UPDATE tasks SET task_kind = ?, task_template = ?, title = ?, description = ? WHERE task_id = ?`,
			model.TaskKindExecution,
			update.template,
			update.title,
			update.description,
			update.taskID,
		); err != nil {
			t.Fatalf("update task metadata for %s: %v", update.taskID, err)
		}
		if _, err := store.PutTaskClassEvidence(ctx, sqlite.TaskClassEvidencePutInput{
			TaskID:          update.taskID,
			TaskClass:       update.taskClass,
			TaskClassSource: model.TaskClassSourceExplicit,
			ActorID:         "operator",
		}); err != nil {
			t.Fatalf("put task class evidence for %s: %v", update.taskID, err)
		}
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: scenario.workspaceID,
		AgentID:     "agent-a",
		UpdateType:  "status",
		Summary:     "Mixed authored task classes linked into one proto-cluster",
		PayloadJSON: `{"task_ids":["` + scenario.primaryTaskID + `","` + scenario.secondaryTaskID + `"]}`,
	}); err != nil {
		t.Fatalf("record mixed authored class update: %v", err)
	}

	clusterID := "task:" + scenario.workspaceID + "/" + scenario.primaryTaskID
	rawReport, err := json.Marshal(workspaceInstrumentationCorridorParams{
		WorkspaceID:    scenario.workspaceID,
		ProtoClusterID: clusterID,
		Limit:          10,
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
	cluster := requireServerCorridorCluster(t, report.Clusters, clusterID)
	if cluster.CorridorReadiness != "MIXED" || !cluster.MixedTaskClasses {
		t.Fatalf("expected mixed corridor cluster, got %+v", cluster)
	}
	if cluster.CorridorLookup.LookupStatus != "AMBIGUOUS" {
		t.Fatalf("expected ambiguous corridor lookup, got %+v", cluster.CorridorLookup)
	}
	if cluster.TaskClass != "" || cluster.TaskClassSource != "" || cluster.TaskClassUpdatedAt != "" {
		t.Fatalf("expected mixed corridor cluster not to expose authoritative task_class evidence, got %+v", cluster)
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
		t.Fatalf("unexpected corridor detail type %T", clusterPayload["detail"])
	}
	if detail.Cluster.TaskClass != "" || detail.Cluster.TaskClassSource != "" || detail.Cluster.TaskClassUpdatedAt != "" {
		t.Fatalf("expected mixed corridor detail not to expose authoritative task_class evidence, got %+v", detail.Cluster)
	}
	if len(detail.Tasks) < 2 {
		t.Fatalf("expected linked mixed cluster to surface both tasks, got %+v", detail.Tasks)
	}
}
