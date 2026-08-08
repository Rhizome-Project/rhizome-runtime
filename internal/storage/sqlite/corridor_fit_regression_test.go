package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestCorridorMixedExplicitClassesDoNotExposeAuthoritativeTaskClass(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID     = "ws-corridor-mixed-authored"
		primaryTaskID   = "task-corridor-mixed-authored-primary"
		secondaryTaskID = "task-corridor-mixed-authored-secondary"
		expectedCluster = "task:" + workspaceID + "/" + primaryTaskID
	)

	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          primaryTaskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Refactor adapter boundary",
		Description:     "Connect the transport integration path.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateIntegration,
		TaskClass:       model.TaskClassIntegration,
		TaskClassSource: model.TaskClassSourceExplicit,
	})
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          secondaryTaskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Repair failing rollout",
		Description:     "Fix the deploy regression and restore the path.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateBugfix,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
	})
	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    primaryTaskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		TaskID:      primaryTaskID,
		PayloadJSON: mustJSONForTest(t, map[string]any{
			"task_id":  primaryTaskID,
			"task_ids": []string{primaryTaskID, secondaryTaskID},
		}),
	})

	report, err := store.BuildCorridorReadinessReport(ctx, sqlite.CorridorReadinessFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build corridor readiness report: %v", err)
	}
	cluster := requireCorridorCluster(t, report.Clusters, expectedCluster)
	if cluster.CorridorReadiness != "MIXED" || !cluster.MixedTaskClasses {
		t.Fatalf("expected mixed corridor cluster, got %+v", cluster)
	}
	if cluster.CorridorLookup.LookupStatus != "AMBIGUOUS" {
		t.Fatalf("expected ambiguous corridor lookup, got %+v", cluster.CorridorLookup)
	}
	if cluster.TaskClass != "" || cluster.TaskClassSource != "" || cluster.TaskClassUpdatedAt != "" {
		t.Fatalf("expected mixed cluster not to expose authoritative task_class evidence, got %+v", cluster)
	}
	if cluster.CorridorCatalogHint != "" {
		t.Fatalf("expected mixed cluster not to surface a single corridor catalog hint, got %+v", cluster)
	}

	detail, err := store.BuildCorridorClusterDetail(ctx, workspaceID, expectedCluster)
	if err != nil {
		t.Fatalf("build corridor cluster detail: %v", err)
	}
	if detail.Cluster.TaskClass != "" || detail.Cluster.TaskClassSource != "" || detail.Cluster.TaskClassUpdatedAt != "" {
		t.Fatalf("expected mixed cluster detail not to expose authoritative task_class evidence, got %+v", detail.Cluster)
	}
	if len(detail.Tasks) != 2 {
		t.Fatalf("expected two task records in mixed cluster detail, got %+v", detail.Tasks)
	}
	for _, task := range detail.Tasks {
		if task.TaskClass == "" || task.TaskClassSource != model.TaskClassSourceExplicit {
			t.Fatalf("expected task detail to preserve per-task authored class evidence, got %+v", task)
		}
	}
}

func TestCorridorUnderEvidencedBasisDoesNotAppearFresh(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID     = "ws-corridor-under-freshness"
		taskID          = "task-corridor-under-freshness"
		expectedCluster = "task:" + workspaceID + "/" + taskID
	)

	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Follow-up task",
		Description:  "Continue work.",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateGeneric,
	})
	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    taskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		TaskID:      taskID,
		PayloadJSON: `{"task_id":"` + taskID + `"}`,
	})

	report, err := store.BuildCorridorReadinessReport(ctx, sqlite.CorridorReadinessFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build corridor readiness report: %v", err)
	}
	cluster := requireCorridorCluster(t, report.Clusters, expectedCluster)
	if cluster.CorridorReadiness != "UNDER_EVIDENCED" || cluster.TaskClassHint != "UNKNOWN" {
		t.Fatalf("expected under-evidenced unknown cluster, got %+v", cluster)
	}
	if cluster.LastBasisEventAt != "" || cluster.BasisStale {
		t.Fatalf("expected under-evidenced cluster not to claim fresh or stale basis timestamps, got %+v", cluster)
	}
	if len(cluster.TaskClassBasis) != 0 {
		t.Fatalf("expected under-evidenced cluster to keep empty basis, got %+v", cluster)
	}

	detail, err := store.BuildCorridorClusterDetail(ctx, workspaceID, expectedCluster)
	if err != nil {
		t.Fatalf("build corridor cluster detail: %v", err)
	}
	if len(detail.Tasks) != 1 {
		t.Fatalf("expected one task in under-evidenced detail, got %+v", detail.Tasks)
	}
	if detail.Tasks[0].BasisUpdatedAt != "" || detail.Tasks[0].TaskClassUpdatedAt != "" {
		t.Fatalf("expected under-evidenced task not to expose basis freshness timestamps, got %+v", detail.Tasks[0])
	}
	if detail.Tasks[0].CorridorLookup.LookupStatus != "NO_MATCH" {
		t.Fatalf("expected under-evidenced task to stay unmatched, got %+v", detail.Tasks[0].CorridorLookup)
	}
}

func TestCorridorFitSyntheticSnapshotDoesNotContaminateEvidenceWindows(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-corridor-fit-snapshot"
		taskID      = "task-corridor-fit-snapshot"
		sessionID   = "sess-corridor-fit-snapshot"
		clusterID   = "task:" + workspaceID + "/" + taskID
	)

	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Repair failing rollout",
		Description:  "Fix the incident and unblock the operator.",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateBugfix,
	})

	keepTrue := true
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, "agent-a")
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:         model.SessionEventStart,
		WorkspaceID:       workspaceID,
		SessionID:         sessionID,
		AgentID:           "agent-a",
		TaskID:            taskID,
		Summary:           "Starting fit evidence scenario",
		OwnerScope:        "task/session",
		KeepSessionActive: &keepTrue,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "Blocked on operator confirmation",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "approve fix"},
		},
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue: %v", err)
	}

	beforeInstrumentation, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  workspaceID,
		Limit:        200,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("build instrumentation before fit snapshot: %v", err)
	}
	if _, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions before fit snapshot: %v", err)
	}
	beforeTensions, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions before fit snapshot: %v", err)
	}

	event, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "cluster.corridor_fit_snapshot",
		EntityType:  "instrumentation_corridor_fit",
		EntityID:    clusterID,
		ActorType:   "operator",
		ActorID:     "dashboard",
		TaskID:      taskID,
		PayloadJSON: mustJSONForTest(t, map[string]any{
			"workspace_id":     workspaceID,
			"proto_cluster_id": clusterID,
			"typed_event_type": "CORRIDOR_FIT_SNAPSHOT",
			"event_kind":       "cluster.corridor_fit_snapshot",
			"summary":          "synthetic fit snapshot",
		}),
	})
	if err != nil {
		t.Fatalf("record synthetic corridor fit snapshot: %v", err)
	}
	if event.EventType != "cluster.corridor_fit_snapshot" || event.EntityType != "instrumentation_corridor_fit" {
		t.Fatalf("unexpected corridor fit snapshot event %+v", event)
	}

	afterInstrumentation, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  workspaceID,
		Limit:        200,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("build instrumentation after fit snapshot: %v", err)
	}
	beforeCluster := requireCorridorProtoCluster(t, beforeInstrumentation.Clusters, clusterID)
	afterCluster := requireCorridorProtoCluster(t, afterInstrumentation.Clusters, clusterID)
	if beforeCluster.Metrics.EventCount != afterCluster.Metrics.EventCount || beforeCluster.Metrics.BlockerSignalCount != afterCluster.Metrics.BlockerSignalCount {
		t.Fatalf("expected synthetic corridor fit snapshot to stay out of instrumentation metrics, before=%+v after=%+v", beforeCluster.Metrics, afterCluster.Metrics)
	}

	if _, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions after fit snapshot: %v", err)
	}
	afterTensions, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after fit snapshot: %v", err)
	}
	if len(beforeTensions) != len(afterTensions) {
		t.Fatalf("expected fit snapshot exclusion to keep tension count stable, before=%+v after=%+v", beforeTensions, afterTensions)
	}
	if got, want := corridorTensionIDs(afterTensions), corridorTensionIDs(beforeTensions); len(got) != len(want) {
		t.Fatalf("expected same tension IDs after fit snapshot exclusion, before=%v after=%v", want, got)
	} else {
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("expected same tension IDs after fit snapshot exclusion, before=%v after=%v", want, got)
			}
		}
	}
}
