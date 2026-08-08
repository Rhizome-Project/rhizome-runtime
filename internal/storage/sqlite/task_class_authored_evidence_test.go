package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestCreateTaskWithGraphPersistsExplicitTaskClassEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const taskID = "task-class-create"
	requireTaskClassEvidenceColumns(t, ctx, store)

	createTaskWithSingleNode(t, ctx, store, sqlite.TaskCreateInput{
		TaskID:          taskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Template-backed task class",
		Description:     "Task starts with explicit authored class evidence.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateIntegration,
		TaskClass:       model.TaskClassIntegration,
		TaskClassSource: model.TaskClassSourceTemplateDefault,
		Tags:            []string{"integration"},
	})

	status, err := store.GetTaskStatus(ctx, "", taskID)
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.TaskClass != model.TaskClassIntegration {
		t.Fatalf("expected persisted task_class %s, got %+v", model.TaskClassIntegration, status)
	}
	if status.TaskClassSource != model.TaskClassSourceTemplateDefault {
		t.Fatalf("expected persisted task_class_source %s, got %+v", model.TaskClassSourceTemplateDefault, status)
	}
	if status.TaskClassUpdatedAt == "" {
		t.Fatalf("expected task_class_updated_at to be populated, got %+v", status)
	}
}

func TestPutTaskClassEvidenceUpdatesAndClearsEvidenceAndAudit(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const taskID = "task-class-put"
	createTaskWithSingleNode(t, ctx, store, sqlite.TaskCreateInput{
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Fix deployment regression",
		Description:  "Task class evidence should be mutable.",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateBugfix,
	})

	updated, err := store.PutTaskClassEvidence(ctx, sqlite.TaskClassEvidencePutInput{
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		ActorID:         "operator-a",
	})
	if err != nil {
		t.Fatalf("put task class evidence: %v", err)
	}
	if updated.TaskClass != model.TaskClassIncident || updated.TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected updated task class evidence, got %+v", updated)
	}
	if updated.TaskClassUpdatedAt == "" {
		t.Fatalf("expected updated task_class_updated_at, got %+v", updated)
	}

	cleared, err := store.PutTaskClassEvidence(ctx, sqlite.TaskClassEvidencePutInput{
		TaskID:          taskID,
		TaskClass:       "",
		TaskClassSource: model.TaskClassSourceUnset,
		ActorID:         "operator-a",
	})
	if err != nil {
		t.Fatalf("clear task class evidence: %v", err)
	}
	if cleared.TaskClass != "" || cleared.TaskClassSource != model.TaskClassSourceUnset || cleared.TaskClassUpdatedAt != "" {
		t.Fatalf("expected cleared task class evidence, got %+v", cleared)
	}

	events, err := store.ListAuditEvents(ctx, sqlite.AuditEventFilter{
		EventType: "task_class_evidence_put",
		EntityID:  taskID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list task_class_evidence_put audit events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected two task_class_evidence_put audit events, got %+v", events)
	}
	latestPayload := decodeJSONMap(t, events[0].PayloadJSON)
	if latestPayload["task_class"] != "" || latestPayload["task_class_source"] != model.TaskClassSourceUnset {
		t.Fatalf("expected latest audit payload to reflect cleared evidence, got %+v", latestPayload)
	}
	previousPayload := decodeJSONMap(t, events[1].PayloadJSON)
	if previousPayload["task_class"] != model.TaskClassIncident || previousPayload["task_class_source"] != model.TaskClassSourceExplicit {
		t.Fatalf("expected previous audit payload to reflect explicit incident evidence, got %+v", previousPayload)
	}
}

func TestPutTaskClassEvidenceRejectsDerivedOnlySource(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const taskID = "task-class-invalid-source"
	createTaskWithSingleNode(t, ctx, store, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
		Title:       "Invalid task class source",
	})

	if _, err := store.PutTaskClassEvidence(ctx, sqlite.TaskClassEvidencePutInput{
		TaskID:          taskID,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceHeuristicFallback,
		ActorID:         "operator-a",
	}); err == nil {
		t.Fatal("expected HEURISTIC_FALLBACK source to be rejected for authored task_class evidence")
	}
}

func TestTaskStatusAndCorridorPreferExplicitTaskClassEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-class-authored"
		taskID      = "task-class-authored"
	)

	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          taskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Explore rollout options",
		Description:     "Research the best deployment path before execution.",
		TaskKind:        model.TaskKindCoordination,
		TaskTemplate:    model.TaskTemplateResearch,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		Tags:            []string{"discovery"},
	})
	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    taskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		TaskID:      taskID,
		PayloadJSON: `{"task_id":"` + taskID + `"}`,
	})

	status, err := store.GetTaskStatus(ctx, "", taskID)
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	if status.TaskClass != model.TaskClassIncident || status.TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected task status to expose authored class evidence, got %+v", status)
	}
	workspaceTasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	if len(workspaceTasks) != 1 || workspaceTasks[0].TaskClass != model.TaskClassIncident || workspaceTasks[0].TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected workspace task list to surface authored class evidence, got %+v", workspaceTasks)
	}

	clusterID := "task:" + workspaceID + "/" + taskID
	detail, err := store.BuildCorridorClusterDetail(ctx, workspaceID, clusterID)
	if err != nil {
		t.Fatalf("build corridor cluster detail: %v", err)
	}
	detailMap := jsonObjectMap(t, detail)
	cluster, ok := detailMap["cluster"].(map[string]any)
	if !ok {
		t.Fatalf("expected cluster map in corridor detail, got %+v", detailMap["cluster"])
	}
	if cluster["task_class"] != model.TaskClassIncident || cluster["task_class_source"] != model.TaskClassSourceExplicit {
		t.Fatalf("expected corridor cluster to surface authored task_class evidence, got %+v", cluster)
	}
	if cluster["task_class_hint"] != model.TaskClassExploration {
		t.Fatalf("expected heuristic task_class_hint to remain visible for comparison, got %+v", cluster)
	}
	if cluster["corridor_catalog_hint"] != "incident" {
		t.Fatalf("expected authored task_class to drive incident corridor lookup, got %+v", cluster)
	}
	clusterLookup, ok := cluster["corridor_lookup"].(map[string]any)
	if !ok {
		t.Fatalf("expected corridor_lookup map on cluster, got %+v", cluster["corridor_lookup"])
	}
	if clusterLookup["catalog_key"] != "incident" {
		t.Fatalf("expected cluster corridor lookup to prefer explicit task_class INCIDENT, got %+v", clusterLookup)
	}
	if clusterLookup["lookup_status"] != "CLASS_MATCH" {
		t.Fatalf("expected explicit task_class to outrank template support at cluster scope, got %+v", clusterLookup)
	}
	if clusterLookup["match_source"] == "task_class_hint" || clusterLookup["match_source"] == "dominant_task_class" {
		t.Fatalf("expected authored task_class to outrank heuristic sources, got %+v", clusterLookup)
	}

	tasks, ok := detailMap["tasks"].([]any)
	if !ok || len(tasks) != 1 {
		t.Fatalf("expected one task in corridor detail, got %+v", detailMap["tasks"])
	}
	task, ok := tasks[0].(map[string]any)
	if !ok {
		t.Fatalf("expected task map in corridor detail, got %+v", tasks[0])
	}
	if task["task_class"] != model.TaskClassIncident || task["task_class_source"] != model.TaskClassSourceExplicit {
		t.Fatalf("expected corridor task detail to surface explicit authored class evidence, got %+v", task)
	}
	if task["task_class_hint"] != model.TaskClassExploration {
		t.Fatalf("expected corridor task detail to preserve heuristic comparison hint, got %+v", task)
	}
	taskLookup, ok := task["corridor_lookup"].(map[string]any)
	if !ok {
		t.Fatalf("expected task corridor_lookup map, got %+v", task["corridor_lookup"])
	}
	if taskLookup["catalog_key"] != "incident" {
		t.Fatalf("expected task corridor lookup to follow explicit task_class INCIDENT, got %+v", taskLookup)
	}
	if taskLookup["lookup_status"] != "CLASS_MATCH" {
		t.Fatalf("expected explicit task_class to outrank template support at task scope, got %+v", taskLookup)
	}
	bundle, err := store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
	})
	if err != nil {
		t.Fatalf("get task hydration bundle: %v", err)
	}
	if bundle.WorkspaceTask == nil || bundle.WorkspaceTask.TaskClass != "INCIDENT" || bundle.WorkspaceTask.TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected workspace hydration task to preserve explicit authored class evidence, got %+v", bundle.WorkspaceTask)
	}
}

func TestCorridorExplicitTaskClassDoesNotBorrowGenericTaskUpdatedAtForBasisFreshness(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-class-freshness"
		taskID      = "task-class-freshness"
	)

	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          taskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Repair rollout",
		Description:     "Fix the deploy regression and restore the service.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateBugfix,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
	})
	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE tasks SET task_class_updated_at = '', updated_at = ? WHERE task_id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano),
		taskID,
	); err != nil {
		t.Fatalf("clear task_class_updated_at while keeping task updated_at fresh: %v", err)
	}
	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    taskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		TaskID:      taskID,
		PayloadJSON: `{"task_id":"` + taskID + `"}`,
	})

	detail, err := store.BuildCorridorClusterDetail(ctx, workspaceID, "task:"+workspaceID+"/"+taskID)
	if err != nil {
		t.Fatalf("build corridor cluster detail: %v", err)
	}
	if detail.Cluster.TaskClass != model.TaskClassIncident || detail.Cluster.TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected explicit task_class to remain authoritative, got %+v", detail.Cluster)
	}
	if detail.Cluster.TaskClassUpdatedAt != "" {
		t.Fatalf("expected cluster task_class_updated_at to stay empty without authored timestamp, got %+v", detail.Cluster)
	}
	if detail.Cluster.LastBasisEventAt != "" {
		t.Fatalf("expected cluster last_basis_event_at to stay empty instead of borrowing task updated_at, got %+v", detail.Cluster)
	}
	if !detail.Cluster.BasisStale || detail.Cluster.CorridorReadiness != "STALE_BASIS" {
		t.Fatalf("expected authored task_class without timestamp to remain authoritative but stale, got %+v", detail.Cluster)
	}
	if len(detail.Tasks) != 1 {
		t.Fatalf("expected one task in corridor detail, got %+v", detail.Tasks)
	}
	if detail.Tasks[0].TaskClass != model.TaskClassIncident || detail.Tasks[0].TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected explicit task_class to remain authoritative at task scope, got %+v", detail.Tasks[0])
	}
	if detail.Tasks[0].TaskClassUpdatedAt != "" || detail.Tasks[0].BasisUpdatedAt != "" {
		t.Fatalf("expected task basis freshness to remain empty without task_class_updated_at, got %+v", detail.Tasks[0])
	}
	if detail.Tasks[0].CorridorLookup.LookupStatus != "CLASS_MATCH" || detail.Tasks[0].CorridorLookup.CatalogKey != "incident" {
		t.Fatalf("expected explicit task_class to continue driving corridor lookup, got %+v", detail.Tasks[0].CorridorLookup)
	}
}

func createTaskWithSingleNode(t *testing.T, ctx context.Context, store *sqlite.Store, input sqlite.TaskCreateInput) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{
			{NodeID: "node-" + input.TaskID, Type: "generic"},
		},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph for %s: %v", input.TaskID, err)
	}
	if err := store.CreateTaskWithGraph(ctx, input, graph); err != nil {
		t.Fatalf("create task %s: %v", input.TaskID, err)
	}
}

func TestCorridorHeuristicFallbackRemainsWithoutExplicitTaskClassEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-class-heuristic"
		taskID      = "task-class-heuristic"
	)

	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Explore the rollout basis",
		Description:  "Research and compare options before execution.",
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
		TaskID:      taskID,
		PayloadJSON: `{"task_id":"` + taskID + `"}`,
	})

	detail, err := store.BuildCorridorClusterDetail(ctx, workspaceID, "task:"+workspaceID+"/"+taskID)
	if err != nil {
		t.Fatalf("build corridor cluster detail: %v", err)
	}
	if detail.Cluster.TaskClassHint != model.TaskClassExploration || detail.Cluster.CorridorCatalogHint != "exploration" {
		t.Fatalf("expected heuristic exploration fallback to remain active, got %+v", detail.Cluster)
	}
	if len(detail.Tasks) != 1 {
		t.Fatalf("expected one task in corridor detail, got %+v", detail.Tasks)
	}
	if detail.Tasks[0].TaskClassHint != model.TaskClassExploration {
		t.Fatalf("expected task heuristic fallback hint, got %+v", detail.Tasks[0])
	}
	if detail.Tasks[0].CorridorLookup.LookupStatus == "NO_MATCH" {
		t.Fatalf("expected heuristic/template fallback lookup to remain available, got %+v", detail.Tasks[0].CorridorLookup)
	}
}

func requireTaskClassEvidenceColumns(t *testing.T, ctx context.Context, store *sqlite.Store) {
	t.Helper()

	rows, err := store.DB().QueryContext(ctx, `PRAGMA table_info(tasks)`)
	if err != nil {
		t.Fatalf("pragma table_info(tasks): %v", err)
	}
	defer rows.Close()

	columns := map[string]bool{}
	for rows.Next() {
		var (
			cid        int
			name       string
			typ        string
			notNull    int
			defaultV   sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultV, &primaryKey); err != nil {
			t.Fatalf("scan table_info(tasks): %v", err)
		}
		columns[name] = true
	}
	for _, required := range []string{"task_class", "task_class_source"} {
		if !columns[required] {
			t.Fatalf("tasks table is missing explicit authored class column %q; columns=%v", required, columns)
		}
	}
}

func jsonObjectMap(t *testing.T, value any) map[string]any {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal value to json: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode value json: %v", err)
	}
	return decoded
}
