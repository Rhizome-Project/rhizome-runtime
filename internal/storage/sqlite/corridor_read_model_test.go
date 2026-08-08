package sqlite_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestBuildCorridorReadinessReportDerivesTaskClassHintsAndReadiness(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-corridor-readiness"
	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")

	readyTaskID := "task-corridor-ready"
	borderlineTaskID := "task-corridor-borderline"
	underTaskID := "task-corridor-under"
	mixedPrimaryTaskID := "task-corridor-mixed-primary"
	mixedSecondaryTaskID := "task-corridor-mixed-secondary"
	staleTaskID := "task-corridor-stale"

	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       readyTaskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Explore rollout options",
		Description:  "Research the best deployment path before execution.",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateResearch,
		Tags:         []string{"discovery"},
	})
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       borderlineTaskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Validate rollout package",
		Description:  "Review the release evidence and compare the verification steps.",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateGeneric,
	})
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       underTaskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Follow-up task",
		Description:  "Continue work.",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateGeneric,
	})
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       mixedPrimaryTaskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Refactor transport bridge",
		Description:  "Align the adapter and integration path.",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateIntegration,
	})
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       mixedSecondaryTaskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Repair deploy regression",
		Description:  "Fix the incident and restore the failing path.",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateBugfix,
	})
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       staleTaskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Ops maintenance pass",
		Description:  "Maintenance task for runtime health and repair.",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateOps,
	})

	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    readyTaskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		TaskID:      readyTaskID,
		PayloadJSON: `{"task_id":"` + readyTaskID + `"}`,
	})
	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    borderlineTaskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		TaskID:      borderlineTaskID,
		PayloadJSON: `{"task_id":"` + borderlineTaskID + `"}`,
	})
	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    underTaskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		TaskID:      underTaskID,
		PayloadJSON: `{"task_id":"` + underTaskID + `"}`,
	})
	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    mixedPrimaryTaskID,
		ActorType:   "agent",
		ActorID:     "agent-b",
		AgentID:     "agent-b",
		TaskID:      mixedPrimaryTaskID,
		PayloadJSON: mustJSONForTest(t, map[string]any{
			"task_id":  mixedPrimaryTaskID,
			"task_ids": []string{mixedPrimaryTaskID, mixedSecondaryTaskID},
		}),
	})
	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    staleTaskID,
		ActorType:   "agent",
		ActorID:     "agent-b",
		AgentID:     "agent-b",
		TaskID:      staleTaskID,
		PayloadJSON: `{"task_id":"` + staleTaskID + `"}`,
	})
	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE tasks SET updated_at = ? WHERE task_id = ?`,
		time.Now().UTC().Add(-96*time.Hour).Format(time.RFC3339Nano),
		staleTaskID,
	); err != nil {
		t.Fatalf("stale task metadata updated_at: %v", err)
	}

	report, err := store.BuildCorridorReadinessReport(ctx, sqlite.CorridorReadinessFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build corridor readiness report: %v", err)
	}
	if report.Workspace.TotalClusters != 5 {
		t.Fatalf("expected 5 clusters, got %+v", report.Workspace)
	}
	if report.Workspace.ReadyCount != 1 || report.Workspace.BorderlineCount != 1 || report.Workspace.UnderEvidencedCount != 1 || report.Workspace.MixedCount != 1 || report.Workspace.StaleBasisCount != 1 {
		t.Fatalf("unexpected workspace readiness counts: %+v", report.Workspace)
	}
	if len(report.Catalog) != 4 {
		t.Fatalf("expected explicit corridor catalog in report, got %+v", report.Catalog)
	}

	readyCluster := requireCorridorCluster(t, report.Clusters, "task:"+workspaceID+"/"+readyTaskID)
	if readyCluster.TaskClassHint != "EXPLORATION" || readyCluster.CorridorReadiness != "READY" {
		t.Fatalf("expected ready exploration cluster, got %+v", readyCluster)
	}
	if readyCluster.CorridorCatalogHint != "exploration" || readyCluster.TaskClassConfidence < 0.8 {
		t.Fatalf("expected strong exploration confidence, got %+v", readyCluster)
	}
	if readyCluster.CorridorLookup.LookupStatus != "TEMPLATE_MATCH" || readyCluster.CorridorLookup.CatalogKey != "exploration" {
		t.Fatalf("expected explicit template-backed lookup, got %+v", readyCluster.CorridorLookup)
	}

	borderlineCluster := requireCorridorCluster(t, report.Clusters, "task:"+workspaceID+"/"+borderlineTaskID)
	if borderlineCluster.TaskClassHint != "PROOF" || borderlineCluster.CorridorReadiness != "BORDERLINE" {
		t.Fatalf("expected borderline proof cluster, got %+v", borderlineCluster)
	}
	if borderlineCluster.CorridorLookup.LookupStatus != "CLASS_MATCH" || borderlineCluster.CorridorLookup.CatalogKey != "proof" {
		t.Fatalf("expected class-backed proof lookup, got %+v", borderlineCluster.CorridorLookup)
	}

	underCluster := requireCorridorCluster(t, report.Clusters, "task:"+workspaceID+"/"+underTaskID)
	if underCluster.TaskClassHint != "UNKNOWN" || underCluster.CorridorReadiness != "UNDER_EVIDENCED" {
		t.Fatalf("expected under-evidenced cluster, got %+v", underCluster)
	}
	if underCluster.CorridorLookup.LookupStatus != "NO_MATCH" {
		t.Fatalf("expected no-match lookup for under-evidenced cluster, got %+v", underCluster.CorridorLookup)
	}

	mixedCluster := requireCorridorCluster(t, report.Clusters, "task:"+workspaceID+"/"+mixedPrimaryTaskID)
	if mixedCluster.CorridorReadiness != "MIXED" || !mixedCluster.MixedTaskClasses {
		t.Fatalf("expected mixed corridor readiness, got %+v", mixedCluster)
	}
	if mixedCluster.TaskClassHint != "UNKNOWN" || mixedCluster.TaskClassConfidence != 0 {
		t.Fatalf("expected mixed cluster not to expose a concrete authoritative class hint, got %+v", mixedCluster)
	}
	if mixedCluster.TaskClassCounts["INTEGRATION"] != 1 || mixedCluster.TaskClassCounts["INCIDENT"] != 1 {
		t.Fatalf("expected mixed class counts, got %+v", mixedCluster.TaskClassCounts)
	}
	if mixedCluster.CorridorLookup.LookupStatus != "AMBIGUOUS" {
		t.Fatalf("expected ambiguous lookup for mixed cluster, got %+v", mixedCluster.CorridorLookup)
	}
	if report.Workspace.TaskClassCounts["INTEGRATION"] != 0 {
		t.Fatalf("expected mixed cluster not to leak integration into workspace task-class counts, got %+v", report.Workspace.TaskClassCounts)
	}

	staleCluster := requireCorridorCluster(t, report.Clusters, "task:"+workspaceID+"/"+staleTaskID)
	if staleCluster.CorridorReadiness != "STALE_BASIS" || !staleCluster.BasisStale {
		t.Fatalf("expected stale-basis cluster, got %+v", staleCluster)
	}
	if staleCluster.LastBasisEventAt == "" {
		t.Fatalf("expected stale cluster to retain basis freshness timestamp, got %+v", staleCluster)
	}
	if report.Workspace.TaskClassCounts["EXPLORATION"] != 1 || report.Workspace.TaskClassCounts["PROOF"] != 1 || report.Workspace.TaskClassCounts["INCIDENT"] != 1 {
		t.Fatalf("expected workspace task-class counts to ignore mixed cluster dominance leakage, got %+v", report.Workspace.TaskClassCounts)
	}
	if report.Workspace.DominantTaskClass == "INTEGRATION" {
		t.Fatalf("expected mixed cluster not to leak integration dominance into workspace counts, got %+v", report.Workspace)
	}

	detail, err := store.BuildCorridorClusterDetail(ctx, workspaceID, "task:"+workspaceID+"/"+borderlineTaskID)
	if err != nil {
		t.Fatalf("build corridor cluster detail: %v", err)
	}
	if len(detail.Tasks) != 1 {
		t.Fatalf("expected one task in detail, got %+v", detail.Tasks)
	}
	if detail.Tasks[0].TaskClassHint != "PROOF" || detail.Tasks[0].HintConfidence <= 0 || len(detail.Tasks[0].TaskClassBasis) == 0 {
		t.Fatalf("expected proof detail basis, got %+v", detail.Tasks[0])
	}
	if detail.Tasks[0].CorridorLookup.LookupStatus != "CLASS_MATCH" || detail.Tasks[0].BasisUpdatedAt == "" {
		t.Fatalf("expected detail task to expose lookup status and basis freshness, got %+v", detail.Tasks[0])
	}
}

func TestCorridorReadinessSnapshotDoesNotContaminateInstrumentationOrTensionRefresh(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-corridor-snapshot"
		taskID      = "task-corridor-snapshot"
		sessionID   = "sess-corridor-snapshot"
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
		Summary:           "Starting repair",
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
		t.Fatalf("build instrumentation before snapshot: %v", err)
	}

	firstRefresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions before snapshot: %v", err)
	}
	if firstRefresh.CreatedCount == 0 {
		t.Fatalf("expected refresh to create tensions, got %+v", firstRefresh)
	}
	beforeTensions, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions before snapshot: %v", err)
	}

	report, err := store.BuildCorridorReadinessReport(ctx, sqlite.CorridorReadinessFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build corridor readiness report: %v", err)
	}
	event, err := store.RecordCorridorReadinessSnapshot(ctx, report, sqlite.CorridorSnapshotInput{
		ActorID: "dashboard",
		Limit:   5,
	})
	if err != nil {
		t.Fatalf("record corridor snapshot: %v", err)
	}
	if event.EventType != "cluster.corridor_readiness_snapshot" || event.EntityType != "instrumentation_corridor" {
		t.Fatalf("unexpected corridor snapshot event %+v", event)
	}
	payload := decodeJSONMap(t, event.PayloadJSON)
	if payload["typed_event_type"] != "CORRIDOR_READINESS_SNAPSHOT" || payload["event_kind"] != "cluster.corridor_readiness_snapshot" {
		t.Fatalf("unexpected corridor snapshot payload %+v", payload)
	}
	if payload["generated_at"] != report.GeneratedAt {
		t.Fatalf("expected corridor snapshot generated_at %q to mirror report %q", payload["generated_at"], report.GeneratedAt)
	}
	if payload["captured_cluster_count"] != float64(len(report.Clusters)) || payload["source_cluster_count"] != float64(len(report.Clusters)) {
		t.Fatalf("expected corridor snapshot payload to expose captured/source cluster counts, got %+v", payload)
	}
	if payload["snapshot_limit"] != float64(5) || payload["snapshot_truncated"] != false {
		t.Fatalf("expected corridor snapshot payload to preserve limit/truncation metadata, got %+v", payload)
	}

	afterInstrumentation, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  workspaceID,
		Limit:        200,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("build instrumentation after snapshot: %v", err)
	}
	beforeCluster := requireCorridorProtoCluster(t, beforeInstrumentation.Clusters, "task:"+workspaceID+"/"+taskID)
	afterCluster := requireCorridorProtoCluster(t, afterInstrumentation.Clusters, "task:"+workspaceID+"/"+taskID)
	if beforeCluster.Metrics.EventCount != afterCluster.Metrics.EventCount || beforeCluster.Metrics.BlockerSignalCount != afterCluster.Metrics.BlockerSignalCount {
		t.Fatalf("expected synthetic corridor snapshot to stay out of instrumentation metrics, before=%+v after=%+v", beforeCluster.Metrics, afterCluster.Metrics)
	}

	if _, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions after snapshot: %v", err)
	}
	afterTensions, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after snapshot: %v", err)
	}
	if !equalStringSlices(corridorTensionIDs(beforeTensions), corridorTensionIDs(afterTensions)) {
		t.Fatalf("expected corridor snapshot to stay out of tension extraction, before=%v after=%v", corridorTensionIDs(beforeTensions), corridorTensionIDs(afterTensions))
	}
}

func TestRecordCorridorReadinessSnapshotUsesScopedProtoClusterEntityID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-corridor-scoped-snapshot"
		taskID      = "task-corridor-scoped-snapshot"
	)

	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Explore rollout evidence",
		Description:  "Research the corridor basis before execution.",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateResearch,
		Tags:         []string{"discovery"},
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

	clusterID := "task:" + workspaceID + "/" + taskID
	report, err := store.BuildCorridorReadinessReport(ctx, sqlite.CorridorReadinessFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: clusterID,
		Limit:          1,
	})
	if err != nil {
		t.Fatalf("build scoped corridor readiness report: %v", err)
	}
	if report.Filter.ProtoClusterID != clusterID || len(report.Clusters) != 1 {
		t.Fatalf("expected scoped report to keep one cluster %s, got %+v", clusterID, report)
	}

	event, err := store.RecordCorridorReadinessSnapshot(ctx, report, sqlite.CorridorSnapshotInput{
		ActorID: "dashboard",
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("record scoped corridor snapshot: %v", err)
	}
	if event.EventType != "cluster.corridor_readiness_snapshot" || event.EntityType != "instrumentation_corridor" || event.EntityID != clusterID {
		t.Fatalf("expected scoped corridor snapshot event for %s, got %+v", clusterID, event)
	}

	payload := decodeJSONMap(t, event.PayloadJSON)
	if payload["workspace_id"] != workspaceID {
		t.Fatalf("expected workspace payload %s, got %+v", workspaceID, payload)
	}
	if payload["generated_at"] != report.GeneratedAt {
		t.Fatalf("expected scoped corridor snapshot generated_at %q to mirror report %q", payload["generated_at"], report.GeneratedAt)
	}
	filter, ok := payload["filter"].(map[string]any)
	if !ok {
		t.Fatalf("expected filter map in payload, got %+v", payload["filter"])
	}
	if filter["proto_cluster_id"] != clusterID {
		t.Fatalf("expected payload proto_cluster_id %s, got %+v", clusterID, filter)
	}
	clusters, ok := payload["clusters"].([]any)
	if !ok || len(clusters) != 1 {
		t.Fatalf("expected one scoped cluster in payload, got %+v", payload["clusters"])
	}
	if payload["captured_cluster_count"] != float64(1) || payload["source_cluster_count"] != float64(1) {
		t.Fatalf("expected scoped corridor snapshot to expose captured/source counts, got %+v", payload)
	}
	if payload["snapshot_limit"] != float64(1) || payload["snapshot_truncated"] != false {
		t.Fatalf("expected scoped corridor snapshot limit/truncation metadata, got %+v", payload)
	}
	if payload["captured_cluster_count"] != float64(1) || payload["source_cluster_count"] != float64(1) {
		t.Fatalf("expected scoped corridor snapshot to expose captured/source cluster counts, got %+v", payload)
	}
	if payload["snapshot_limit"] != float64(1) || payload["snapshot_truncated"] != false {
		t.Fatalf("expected scoped corridor snapshot limit/truncation metadata, got %+v", payload)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "cluster.corridor_readiness_snapshot",
		EntityType:  "instrumentation_corridor",
		EntityID:    clusterID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list scoped corridor snapshot runtime events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one persisted scoped corridor snapshot event, got %+v", events)
	}
}

func TestBuildCorridorReadinessReportKeepsWorkspaceTimeAuthorityInspectable(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-corridor-time-authority"
	const taskID = "task-corridor-time-authority"

	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Inspect corridor authority",
		Description:  "Validate the workspace time authority pair is still inspectable.",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateResearch,
	})
	if err := store.SetPolicyMode(ctx, workspaceID, "active"); err != nil {
		t.Fatalf("set active policy mode: %v", err)
	}
	if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
		t.Fatalf("increment control epoch: %v", err)
	}

	referenceAt := time.Now().UTC().Add(30 * time.Minute).Round(0).Format(time.RFC3339Nano)
	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "corridor.time_authority_probe",
		EntityType:  "task",
		EntityID:    taskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		TaskID:      taskID,
		PayloadJSON: `{"probe":"corridor"}`,
		CreatedAt:   referenceAt,
	})

	report, err := store.BuildCorridorReadinessReport(ctx, sqlite.CorridorReadinessFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build corridor readiness report for authority inspection: %v", err)
	}
	if len(report.Clusters) == 0 {
		t.Fatalf("expected corridor readiness report to remain populated, got %+v", report)
	}

	authority, err := store.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	if authority.WorkspaceID != workspaceID || authority.CurrentEpoch != 1 || authority.ReferenceAt != referenceAt {
		t.Fatalf("expected corridor surface to keep authority pair inspectable, got %+v", authority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor report generated_at %q to anchor to report time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
}

func setupCorridorWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, agentIDs ...string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Corridor Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
}

func createCorridorTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, input sqlite.TaskCreateInput) {
	t.Helper()
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + input.TaskID, Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph for %s: %v", input.TaskID, err)
	}
	if err := store.CreateTaskWithGraph(ctx, input, graph); err != nil {
		t.Fatalf("create task %s: %v", input.TaskID, err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      input.TaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task %s: %v", input.TaskID, err)
	}
}

func recordCorridorRuntimeEvent(t *testing.T, ctx context.Context, store *sqlite.Store, input sqlite.RuntimeEventInput) {
	t.Helper()
	if _, err := store.RecordRuntimeEvent(ctx, input); err != nil {
		t.Fatalf("record runtime event %+v: %v", input, err)
	}
}

func requireCorridorCluster(t *testing.T, items []sqlite.CorridorClusterReport, clusterID string) sqlite.CorridorClusterReport {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == clusterID {
			return item
		}
	}
	t.Fatalf("corridor cluster %s not found in %+v", clusterID, items)
	return sqlite.CorridorClusterReport{}
}

func requireCorridorProtoCluster(t *testing.T, items []sqlite.ProtoClusterReport, clusterID string) sqlite.ProtoClusterReport {
	t.Helper()
	for _, item := range items {
		if item.ProtoClusterID == clusterID {
			return item
		}
	}
	t.Fatalf("proto cluster %s not found in %+v", clusterID, items)
	return sqlite.ProtoClusterReport{}
}

func corridorTensionIDs(items []sqlite.TensionRecord) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.TensionID)
	}
	return out
}

func mustJSONForTest(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json payload: %v", err)
	}
	return string(raw)
}

func decodeJSONMap(t *testing.T, raw string) map[string]any {
	t.Helper()
	out := map[string]any{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("decode json payload: %v", err)
	}
	return out
}
