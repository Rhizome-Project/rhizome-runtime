package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestCorridorAuthorityKeepsExplicitTaskClassVisibleWithoutRecentRuntimeActivity(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-corridor-task-first-visibility"
		taskID      = "task-corridor-task-first-visibility"
		clusterID   = "task:" + workspaceID + "/" + taskID
	)

	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          taskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Repair rollout authority basis",
		Description:     "Explicit task-authored class evidence should stay visible before new runtime activity arrives.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateResearch,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
		Tags:            []string{"incident", "authority"},
	})

	report, err := store.BuildCorridorAuthorityReport(ctx, sqlite.CorridorAuthorityFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build corridor authority report without runtime activity: %v", err)
	}
	task := requireCorridorAuthorityTask(t, report.Tasks, taskID)
	if task.TaskClass != model.TaskClassIncident || task.TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected explicit task-authored class evidence to stay visible at authority scope, got %+v", task)
	}
	if task.CorridorLookup.LookupStatus != "CLASS_MATCH" || task.CorridorLookup.CatalogKey != "incident" {
		t.Fatalf("expected explicit task-authored class evidence to keep driving corridor lookup, got %+v", task.CorridorLookup)
	}
	if task.TaskClassHint == "" {
		t.Fatalf("expected heuristic comparison surface to remain available, got %+v", task)
	}
	if task.BasisState != "AUTHORED_FRESH" || !task.BasisAuthoritative {
		t.Fatalf("expected explicit task-authored class evidence to remain authoritative while fresh, got %+v", task)
	}
	if task.VisibleInInstrumentation {
		t.Fatalf("expected task-authored class evidence to stay visible before instrumentation activity appears, got %+v", task)
	}
	if len(task.ActiveProtoClusterIDs) != 0 || task.LastActivityAt != "" {
		t.Fatalf("expected inactive task-authored corridor authority surface to stay decoupled from instrumentation activity, got %+v", task)
	}
	if report.Workspace.InactiveAuthoredTaskCount != 1 || report.Workspace.AuthoredFreshCount != 1 {
		t.Fatalf("expected workspace authority metrics to count inactive authored task basis, got %+v", report.Workspace)
	}

	detail, err := store.BuildCorridorAuthorityTaskDetail(ctx, workspaceID, taskID)
	if err != nil {
		t.Fatalf("build corridor authority task detail without runtime activity: %v", err)
	}
	if detail.Task.TaskID != taskID || detail.Task.TaskClass != model.TaskClassIncident || detail.Task.TaskClassSource != model.TaskClassSourceExplicit {
		t.Fatalf("expected corridor authority task detail to preserve explicit task-authored class evidence, got %+v", detail.Task)
	}
	if len(detail.Clusters) != 0 {
		t.Fatalf("expected inactive authored task to stay visible even without active proto-clusters, got %+v", detail.Clusters)
	}
}

func TestCorridorAuthorityDoesNotTreatTemplateDefaultAsAuthoritativeBasis(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-corridor-template-default-authority"
		taskID      = "task-corridor-template-default-authority"
	)

	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          taskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Template-seeded corridor evidence",
		Description:     "Template defaults may seed lookup, but must not become authoritative corridor authority.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateDeploy,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceTemplateDefault,
	})

	report, err := store.BuildCorridorAuthorityReport(ctx, sqlite.CorridorAuthorityFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build corridor authority report with template default: %v", err)
	}
	task := requireCorridorAuthorityTask(t, report.Tasks, taskID)
	if task.TaskClass != model.TaskClassIncident || task.TaskClassSource != model.TaskClassSourceTemplateDefault {
		t.Fatalf("expected task to preserve template-seeded evidence, got %+v", task)
	}
	if task.BasisAuthoritative {
		t.Fatalf("expected template default not to count as authoritative basis, got %+v", task)
	}
	if task.AuthorityClass != "" {
		t.Fatalf("expected no authoritative class from template default evidence, got %+v", task)
	}
	if task.BasisState != "DERIVED_ONLY" {
		t.Fatalf("expected template default to remain derived-only at authority scope, got %+v", task)
	}
	if report.Workspace.AuthoredFreshCount != 0 || report.Workspace.AuthoredStaleCount != 0 {
		t.Fatalf("expected workspace authored counts to ignore template-default basis, got %+v", report.Workspace)
	}
}

func TestCorridorAuthorityLimitDoesNotHideInactiveExplicitBasisBehindVisibleDerivedTasks(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID    = "ws-corridor-authority-limit"
		explicitTaskID = "task-corridor-authority-limit-explicit"
		visibleTaskID  = "task-corridor-authority-limit-visible"
		sessionID      = "sess-corridor-authority-limit-visible"
	)

	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          explicitTaskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Inactive explicit basis",
		Description:     "Should stay at the top of authority ordering even without instrumentation visibility.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateBugfix,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
	})
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       visibleTaskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Visible derived task",
		Description:  "This task becomes visible via runtime activity, but should not outrank explicit corridor authority.",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateResearch,
	})
	recordCorridorVisibilityActivity(t, ctx, store, workspaceID, visibleTaskID, sessionID)

	report, err := store.BuildCorridorAuthorityReport(ctx, sqlite.CorridorAuthorityFilter{
		WorkspaceID: workspaceID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("build corridor authority report with competing visible task: %v", err)
	}
	if len(report.Tasks) != 1 {
		t.Fatalf("expected one limited authority record, got %+v", report.Tasks)
	}
	if report.Tasks[0].TaskID != explicitTaskID {
		t.Fatalf("expected explicit authority task to win limit ordering, got %+v", report.Tasks)
	}
	if report.Tasks[0].VisibleInInstrumentation {
		t.Fatalf("expected limited top authority task to stay inactive in instrumentation, got %+v", report.Tasks[0])
	}
}

func recordCorridorVisibilityActivity(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, sessionID string) {
	t.Helper()

	keepTrue := true
	claimExternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, "agent-a")
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:         model.SessionEventStart,
		WorkspaceID:       workspaceID,
		SessionID:         sessionID,
		AgentID:           "agent-a",
		TaskID:            taskID,
		Summary:           "start visible authority task",
		OwnerScope:        "task/session",
		KeepSessionActive: &keepTrue,
	}); err != nil {
		t.Fatalf("record visible authority session start: %v", err)
	}
}

func requireCorridorAuthorityTask(t *testing.T, items []sqlite.CorridorAuthorityTaskRecord, taskID string) sqlite.CorridorAuthorityTaskRecord {
	t.Helper()
	for _, item := range items {
		if item.TaskID == taskID {
			return item
		}
	}
	t.Fatalf("corridor authority task %s not found in %+v", taskID, items)
	return sqlite.CorridorAuthorityTaskRecord{}
}
