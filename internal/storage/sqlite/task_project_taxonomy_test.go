package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestTaskProjectCreateRejectsOrphanProjectID(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-pcaw-create-orphan"

	seedPCAWWorkspace(t, ctx, store, workspaceID)

	err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		WorkspaceID: workspaceID,
		TaskID:      "task-pcaw-create-orphan",
		OwnerUserID: "developer",
		Priority:    "normal",
		ProjectID:   "project-pcaw-missing",
	}, singleNodePCAWGraph(t))
	if !errors.Is(err, ErrTaskProjectNotFound) {
		t.Fatalf("expected ErrTaskProjectNotFound, got %v", err)
	}
}

func TestTaskProjectFieldsPreservedAndUpdateValidated(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-pcaw-project-fields"
		projectID   = "project-pcaw-fields"
		taskID      = "task-pcaw-fields"
	)

	seedPCAWWorkspace(t, ctx, store, workspaceID)
	seedPCAWProject(t, ctx, store, workspaceID, projectID)

	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "normal",
		ProjectID:           projectID,
		TaskKind:            " execution ",
		ProjectLane:         " BackEnd ",
		RequiresProjectGate: true,
	}, singleNodePCAWGraph(t)); err != nil {
		t.Fatalf("create project task: %v", err)
	}
	attachPCAWTask(t, ctx, store, workspaceID, taskID)

	status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
	if err != nil {
		t.Fatalf("get task status: %v", err)
	}
	assertPCAWTaskFields(t, status.ProjectID, status.TaskKind, status.ProjectLane, status.RequiresProjectGate, projectID, "EXECUTION", "backend", true)

	missingProjectID := "project-pcaw-update-missing"
	if _, err := store.UpdateTaskProjectFields(ctx, TaskProjectFieldsUpdateInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ProjectID:   &missingProjectID,
	}); !errors.Is(err, ErrTaskProjectNotFound) {
		t.Fatalf("expected update to reject orphan project_id, got %v", err)
	}

	taskKind := "COORDINATION"
	projectLane := " Review "
	requiresGate := false
	validProjectID := projectID
	claimPCAWProjectLead(t, ctx, store, workspaceID, projectID, "pcaw-test")
	updated, err := store.UpdateTaskProjectFields(ctx, TaskProjectFieldsUpdateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		ProjectID:           &validProjectID,
		TaskKind:            &taskKind,
		ProjectLane:         &projectLane,
		RequiresProjectGate: &requiresGate,
		ActorID:             "pcaw-test",
	})
	if err != nil {
		t.Fatalf("update task project fields: %v", err)
	}
	assertPCAWTaskFields(t, updated.ProjectID, updated.TaskKind, updated.ProjectLane, updated.RequiresProjectGate, projectID, "COORDINATION", "review", false)

	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("workspace task count = %d, want 1", len(tasks))
	}
	assertPCAWTaskFields(t, tasks[0].ProjectID, tasks[0].TaskKind, tasks[0].ProjectLane, tasks[0].RequiresProjectGate, projectID, "COORDINATION", "review", false)
}

func TestTaskProjectFieldsLaneGateGuardRequiresLeadAndUnboundTask(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-pcaw-lane-gate-guard"
		projectID   = "project-pcaw-lane-gate-guard"
		taskID      = "task-pcaw-lane-gate-guard"
		leadID      = "agent-pcaw-lead"
		otherID     = "agent-pcaw-other"
	)

	seedPCAWWorkspace(t, ctx, store, workspaceID)
	seedPCAWProject(t, ctx, store, workspaceID, projectID)
	registerPCAWAgent(t, ctx, store, workspaceID, otherID)
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "normal",
		ProjectID:           projectID,
		TaskKind:            "COORDINATION",
		ProjectLane:         "coordination",
		RequiresProjectGate: false,
	}, singleNodePCAWGraph(t)); err != nil {
		t.Fatalf("create lane guard task: %v", err)
	}
	attachPCAWTask(t, ctx, store, workspaceID, taskID)

	implementationLane := "implementation"
	if _, err := store.UpdateTaskProjectFields(ctx, TaskProjectFieldsUpdateInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ProjectLane: &implementationLane,
		ActorID:     otherID,
	}); !errors.Is(err, ErrProjectLeadRequired) {
		t.Fatalf("expected lane flip without strategic lead to fail, got %v", err)
	}

	claimPCAWProjectLead(t, ctx, store, workspaceID, projectID, leadID)
	if _, err := store.UpdateTaskProjectFields(ctx, TaskProjectFieldsUpdateInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ProjectLane: &implementationLane,
		ActorID:     otherID,
	}); !errors.Is(err, ErrProjectLeadMismatch) {
		t.Fatalf("expected lane flip by non-lead to fail, got %v", err)
	}

	requiresGate := true
	updated, err := store.UpdateTaskProjectFields(ctx, TaskProjectFieldsUpdateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		ProjectLane:         &implementationLane,
		RequiresProjectGate: &requiresGate,
		ActorID:             leadID,
	})
	if err != nil {
		t.Fatalf("strategic lead should be allowed to update unbound lane/gate: %v", err)
	}
	assertPCAWTaskFields(t, updated.ProjectID, updated.TaskKind, updated.ProjectLane, updated.RequiresProjectGate, projectID, "COORDINATION", "implementation", true)
}

func TestTaskProjectFieldsLaneGateGuardRejectsBoundTask(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID       = "ws-pcaw-lane-gate-bound"
		projectID         = "project-pcaw-lane-gate-bound"
		leadID            = "agent-pcaw-bound-lead"
		workerID          = "agent-pcaw-bound-worker"
		claimedTaskID     = "task-pcaw-lane-gate-claimed"
		branchBoundTaskID = "task-pcaw-lane-gate-branch"
	)

	seedPCAWWorkspace(t, ctx, store, workspaceID)
	seedPCAWProject(t, ctx, store, workspaceID, projectID)
	claimPCAWProjectLead(t, ctx, store, workspaceID, projectID, leadID)
	registerPCAWAgent(t, ctx, store, workspaceID, workerID)
	for _, taskID := range []string{claimedTaskID, branchBoundTaskID} {
		if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
			WorkspaceID:         workspaceID,
			TaskID:              taskID,
			OwnerUserID:         "developer",
			Priority:            "normal",
			ProjectID:           projectID,
			TaskKind:            "COORDINATION",
			ProjectLane:         "coordination",
			RequiresProjectGate: false,
		}, singleNodePCAWGraph(t)); err != nil {
			t.Fatalf("create bound guard task %s: %v", taskID, err)
		}
		attachPCAWTask(t, ctx, store, workspaceID, taskID)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                claimedTaskID,
		AgentID:               workerID,
		Summary:               "claim should make lane/gate flip illegal",
		PromptContextEnvelope: boundPCAWTaskPromptContextEnvelope("agent.task.claim", workspaceID, claimedTaskID, workerID),
	}); err != nil {
		t.Fatalf("claim task for lane guard: %v", err)
	}
	seedPCAWBranchBinding(t, ctx, store, workspaceID, projectID, "repo-pcaw-lane-gate-bound", "branch-pcaw-lane-gate-bound", branchBoundTaskID, workerID)

	implementationLane := "implementation"
	for _, taskID := range []string{claimedTaskID, branchBoundTaskID} {
		if _, err := store.UpdateTaskProjectFields(ctx, TaskProjectFieldsUpdateInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			ProjectLane: &implementationLane,
			ActorID:     leadID,
		}); !errors.Is(err, ErrTaskProjectFieldsBindingActive) {
			t.Fatalf("expected bound task %s lane flip to fail, got %v", taskID, err)
		}
		status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
		if err != nil {
			t.Fatalf("read bound task %s after rejected lane flip: %v", taskID, err)
		}
		assertPCAWTaskFields(t, status.ProjectID, status.TaskKind, status.ProjectLane, status.RequiresProjectGate, projectID, "COORDINATION", "coordination", false)
	}
}

func TestTaskProjectFieldsRejectMultiWorkspaceProjectScope(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceA = "ws-pcaw-project-scope-a"
		workspaceB = "ws-pcaw-project-scope-b"
		projectID  = "project-pcaw-single-scope"
		taskID     = "task-pcaw-single-scope"
	)

	seedPCAWWorkspace(t, ctx, store, workspaceA)
	seedPCAWWorkspace(t, ctx, store, workspaceB)
	seedPCAWProject(t, ctx, store, workspaceA, projectID)
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		WorkspaceID: workspaceA,
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, singleNodePCAWGraph(t)); err != nil {
		t.Fatalf("create multi-workspace task: %v", err)
	}
	attachPCAWTask(t, ctx, store, workspaceA, taskID)
	attachPCAWTask(t, ctx, store, workspaceB, taskID)

	projectLane := "review"
	if _, err := store.UpdateTaskProjectFields(ctx, TaskProjectFieldsUpdateInput{
		WorkspaceID: workspaceA,
		TaskID:      taskID,
		ProjectLane: &projectLane,
	}); !errors.Is(err, ErrTaskWorkspaceAmbiguous) {
		t.Fatalf("expected multi-workspace lane update to fail with ErrTaskWorkspaceAmbiguous, got %v", err)
	}

	if _, err := store.UpdateTaskProjectFields(ctx, TaskProjectFieldsUpdateInput{
		WorkspaceID: workspaceA,
		TaskID:      taskID,
		ProjectID:   stringPtrForPCAW(projectID),
	}); !errors.Is(err, ErrTaskWorkspaceAmbiguous) {
		t.Fatalf("expected multi-workspace project update to fail with ErrTaskWorkspaceAmbiguous, got %v", err)
	}
}

func TestProjectLinkedTaskCannotAttachToSecondWorkspace(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceA = "ws-pcaw-project-attach-a"
		workspaceB = "ws-pcaw-project-attach-b"
		projectID  = "project-pcaw-attach"
		taskID     = "task-pcaw-project-attach"
	)

	seedPCAWWorkspace(t, ctx, store, workspaceA)
	seedPCAWWorkspace(t, ctx, store, workspaceB)
	seedPCAWProject(t, ctx, store, workspaceA, projectID)
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		WorkspaceID: workspaceA,
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
		ProjectID:   projectID,
	}, singleNodePCAWGraph(t)); err != nil {
		t.Fatalf("create project-linked task: %v", err)
	}
	attachPCAWTask(t, ctx, store, workspaceA, taskID)

	err := store.AttachTaskToWorkspace(ctx, TaskAttachmentInput{
		WorkspaceID: workspaceB,
		TaskID:      taskID,
		LinkedBy:    "developer",
	})
	if !errors.Is(err, ErrTaskProjectNotFound) && !errors.Is(err, ErrTaskWorkspaceAmbiguous) {
		t.Fatalf("expected project-linked second workspace attach to fail, got %v", err)
	}
}

func TestTaskProjectFieldsWithRuntimeEventRollsBackOnEventFailure(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-pcaw-project-event-rollback"
		taskID      = "task-pcaw-project-event-rollback"
	)

	seedPCAWWorkspace(t, ctx, store, workspaceID)
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, singleNodePCAWGraph(t)); err != nil {
		t.Fatalf("create rollback task: %v", err)
	}
	attachPCAWTask(t, ctx, store, workspaceID, taskID)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	taskKind := "COORDINATION"
	if _, _, err := store.UpdateTaskProjectFieldsWithRuntimeEvent(ctx, TaskProjectFieldsUpdateInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		TaskKind:    &taskKind,
		ActorID:     "pcaw-test",
	}, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EntityType:  "task",
		EntityID:    taskID,
		TaskID:      taskID,
		PayloadJSON: "{}",
	}); err == nil || !strings.Contains(err.Error(), "event_type is required") {
		t.Fatalf("expected runtime event failure, got %v", err)
	}

	status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
	if err != nil {
		t.Fatalf("read task after rollback: %v", err)
	}
	if status.TaskKind != "EXECUTION" {
		t.Fatalf("task kind should roll back on event failure, got %+v", status)
	}
}

func TestTaskProjectTaxonomyDefaultsAndNormalization(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-pcaw-taxonomy-defaults"
		defaultTask = "task-pcaw-default-taxonomy"
		laneTask    = "task-pcaw-lane-taxonomy"
	)

	seedPCAWWorkspace(t, ctx, store, workspaceID)
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		WorkspaceID: workspaceID,
		TaskID:      defaultTask,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, singleNodePCAWGraph(t)); err != nil {
		t.Fatalf("create default taxonomy task: %v", err)
	}
	attachPCAWTask(t, ctx, store, workspaceID, defaultTask)

	status, err := store.GetTaskStatus(ctx, workspaceID, defaultTask)
	if err != nil {
		t.Fatalf("get default task status: %v", err)
	}
	assertPCAWTaskFields(t, status.ProjectID, status.TaskKind, status.ProjectLane, status.RequiresProjectGate, "", "EXECUTION", "", false)

	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		WorkspaceID: workspaceID,
		TaskID:      laneTask,
		OwnerUserID: "developer",
		Priority:    "normal",
		TaskKind:    " execution ",
		ProjectLane: " Design ",
	}, singleNodePCAWGraph(t)); err != nil {
		t.Fatalf("create normalized taxonomy task: %v", err)
	}
	attachPCAWTask(t, ctx, store, workspaceID, laneTask)

	status, err = store.GetTaskStatus(ctx, workspaceID, laneTask)
	if err != nil {
		t.Fatalf("get normalized task status: %v", err)
	}
	assertPCAWTaskFields(t, status.ProjectID, status.TaskKind, status.ProjectLane, status.RequiresProjectGate, "", "EXECUTION", "design", false)
}

func TestTaskHydrationAndAgentWorkReturnProjectFields(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-pcaw-hydration"
		projectID   = "project-pcaw-hydration"
		taskID      = "task-pcaw-hydration"
		agentID     = "agent-pcaw-hydration"
	)

	seedPCAWWorkspace(t, ctx, store, workspaceID)
	seedPCAWProject(t, ctx, store, workspaceID, projectID)
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "PCAW Hydration Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	designDocID := "doc.pcaw.hydration.design"
	implementationPlanDocID := "doc.pcaw.hydration.plan"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 agentID,
		ActorType:               "agent",
		PromptContextEnvelope:   BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("mark hydration project ready: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		ActorID:               agentID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim hydration project lead: %v", err)
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               ProjectPhaseImplementation,
		Reason:                "Hydration test project is ready for implementation.",
		ActorID:               agentID,
		ActorType:             "agent",
		PromptContextEnvelope: BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition hydration project: %v", err)
	}

	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		ProjectID:            projectID,
		TaskKind:             "EXECUTION",
		ProjectLane:          "frontend",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","write_scope_hints":["src/ui/**"],"preferred_tools":["browser"]}`,
		WriteScopeHints:      []string{"src/ui/**"},
	}, singleNodePCAWGraph(t)); err != nil {
		t.Fatalf("create hydration task: %v", err)
	}
	attachPCAWTask(t, ctx, store, workspaceID, taskID)

	bundle, err := store.GetTaskHydrationBundle(ctx, TaskHydrationFilter{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		RelatedTaskLimit: 1,
	})
	if err != nil {
		t.Fatalf("get hydration bundle: %v", err)
	}
	assertPCAWTaskFields(t, bundle.Task.ProjectID, bundle.Task.TaskKind, bundle.Task.ProjectLane, bundle.Task.RequiresProjectGate, projectID, "EXECUTION", "frontend", true)
	if !strings.Contains(bundle.Task.TaskRequirementsJSON, "preferred_tools") || strings.Join(bundle.Task.WriteScopeHints, ",") != "src/ui/**" {
		t.Fatalf("expected task status hydration to include requirements/write scope, got %+v", bundle.Task)
	}
	if bundle.WorkspaceTask == nil {
		t.Fatal("expected workspace task in hydration bundle")
	}
	assertPCAWTaskFields(t, bundle.WorkspaceTask.ProjectID, bundle.WorkspaceTask.TaskKind, bundle.WorkspaceTask.ProjectLane, bundle.WorkspaceTask.RequiresProjectGate, projectID, "EXECUTION", "frontend", true)
	if !strings.Contains(bundle.WorkspaceTask.TaskRequirementsJSON, "preferred_tools") || strings.Join(bundle.WorkspaceTask.WriteScopeHints, ",") != "src/ui/**" {
		t.Fatalf("expected workspace task hydration to include requirements/write scope, got %+v", bundle.WorkspaceTask)
	}

	work, err := store.GetAgentWorkNext(ctx, AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          agentID,
		IncludeHydration: true,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !work.HasWork || work.Task == nil || work.Packet == nil {
		t.Fatalf("expected task and packet, got %+v", work)
	}
	assertPCAWTaskFields(t, work.ProjectID, work.TaskKind, work.ProjectLane, work.RequiresProjectGate, projectID, "EXECUTION", "frontend", true)
	assertPCAWTaskFields(t, work.Task.ProjectID, work.Task.TaskKind, work.Task.ProjectLane, work.Task.RequiresProjectGate, projectID, "EXECUTION", "frontend", true)
	assertPCAWTaskFields(t, work.Packet.ProjectID, work.Packet.TaskKind, work.Packet.ProjectLane, work.Packet.RequiresProjectGate, projectID, "EXECUTION", "frontend", true)
}

func TestTaskProjectTaxonomyMigrationReportsAndCleansOrphanProjectIDs(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", sqliteDSNWithPragmas(filepath.Join(t.TempDir(), "pcaw-legacy.db"), "foreign_keys=ON"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`
CREATE TABLE tasks (
  task_id TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  priority TEXT NOT NULL,
  status TEXT NOT NULL,
  task_kind TEXT NOT NULL DEFAULT 'EXECUTION',
  project_id TEXT DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE workspace_tasks (
  workspace_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  linked_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (workspace_id, task_id)
);
CREATE TABLE projects (
  project_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  title TEXT NOT NULL,
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
INSERT INTO projects(project_id, workspace_id, title, created_by, created_at, updated_at)
VALUES ('project-valid', 'ws-valid', 'Valid Project', 'developer', '2026-04-28T00:00:00Z', '2026-04-28T00:00:00Z');
INSERT INTO tasks(task_id, owner_user_id, priority, status, task_kind, project_id, created_at, updated_at)
VALUES
  ('task-valid', 'developer', 'normal', 'PENDING', 'execution', 'project-valid', '2026-04-28T00:00:00Z', '2026-04-28T00:00:00Z'),
  ('task-orphan', 'developer', 'normal', 'PENDING', 'review', 'project-missing', '2026-04-28T00:00:00Z', '2026-04-28T00:00:00Z'),
  ('task-detached', 'developer', 'normal', 'PENDING', 'spec', 'project-detached', '2026-04-28T00:00:00Z', '2026-04-28T00:00:00Z'),
  ('task-multi', 'developer', 'normal', 'PENDING', 'implementation', 'project-valid', '2026-04-28T00:00:00Z', '2026-04-28T00:00:00Z');
INSERT INTO workspace_tasks(workspace_id, task_id, linked_by, created_at)
VALUES
  ('ws-valid', 'task-valid', 'developer', '2026-04-28T00:00:00Z'),
  ('ws-orphan', 'task-orphan', 'developer', '2026-04-28T00:00:00Z'),
  ('ws-valid', 'task-multi', 'developer', '2026-04-28T00:00:00Z'),
  ('ws-other', 'task-multi', 'developer', '2026-04-28T00:00:00Z');
`); err != nil {
		t.Fatalf("seed legacy schema: %v", err)
	}

	sqlText, err := migrationSQL("0087_task_project_taxonomy.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := db.Exec(sqlText); err != nil {
		t.Fatalf("execute migration: %v", err)
	}

	var validProjectID, orphanProjectID, detachedProjectID, multiProjectID string
	if err := db.QueryRow(`SELECT project_id FROM tasks WHERE task_id = 'task-valid'`).Scan(&validProjectID); err != nil {
		t.Fatalf("query valid project_id: %v", err)
	}
	if err := db.QueryRow(`SELECT project_id FROM tasks WHERE task_id = 'task-orphan'`).Scan(&orphanProjectID); err != nil {
		t.Fatalf("query orphan project_id: %v", err)
	}
	if err := db.QueryRow(`SELECT project_id FROM tasks WHERE task_id = 'task-detached'`).Scan(&detachedProjectID); err != nil {
		t.Fatalf("query detached project_id: %v", err)
	}
	if err := db.QueryRow(`SELECT project_id FROM tasks WHERE task_id = 'task-multi'`).Scan(&multiProjectID); err != nil {
		t.Fatalf("query multi-workspace project_id: %v", err)
	}
	if validProjectID != "project-valid" || orphanProjectID != "" || detachedProjectID != "" || multiProjectID != "" {
		t.Fatalf("migration project cleanup got valid=%q orphan=%q detached=%q multi=%q", validProjectID, orphanProjectID, detachedProjectID, multiProjectID)
	}

	var reportCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM task_project_orphan_report`).Scan(&reportCount); err != nil {
		t.Fatalf("count orphan report: %v", err)
	}
	if reportCount != 4 {
		t.Fatalf("orphan report count = %d, want 4", reportCount)
	}

	var taskKind, projectLane string
	var requiresProjectGate int
	if err := db.QueryRow(`SELECT task_kind, project_lane, requires_project_gate FROM tasks WHERE task_id = 'task-orphan'`).Scan(&taskKind, &projectLane, &requiresProjectGate); err != nil {
		t.Fatalf("query migrated taxonomy: %v", err)
	}
	if taskKind != "EXECUTION" || projectLane != "review" || requiresProjectGate != 0 {
		t.Fatalf("migrated taxonomy = kind %q lane %q gate %d", taskKind, projectLane, requiresProjectGate)
	}
}

func stringPtrForPCAW(value string) *string {
	return &value
}

func seedPCAWWorkspace(t *testing.T, ctx context.Context, store *Store, workspaceID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
}

func seedPCAWProject(t *testing.T, ctx context.Context, store *Store, workspaceID, projectID string) {
	t.Helper()
	if err := store.CreateProject(ctx, ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       projectID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create project %s: %v", projectID, err)
	}
}

func attachPCAWTask(t *testing.T, ctx context.Context, store *Store, workspaceID, taskID string) {
	t.Helper()
	if err := store.AttachTaskToWorkspace(ctx, TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task %s to workspace %s: %v", taskID, workspaceID, err)
	}
}

func registerPCAWAgent(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID string) {
	t.Helper()
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent %s: %v", agentID, err)
	}
}

func claimPCAWProjectLead(t *testing.T, ctx context.Context, store *Store, workspaceID, projectID, agentID string) {
	t.Helper()
	registerPCAWAgent(t, ctx, store, workspaceID, agentID)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		ActorID:               agentID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "test strategic lead for task project field lane/gate guard",
		PromptContextEnvelope: BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim project strategic lead %s: %v", agentID, err)
	}
}

func seedPCAWBranchBinding(t *testing.T, ctx context.Context, store *Store, workspaceID, projectID, repoID, branchID, taskID, agentID string) {
	t.Helper()
	now := "2026-06-22T00:00:00Z"
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO project_repositories(repo_id, workspace_id, project_id, remote_url, remote_kind, name, default_branch, repo_status, is_canonical, created_by_agent_id, updated_by, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)`,
		repoID, workspaceID, projectID, "file:///tmp/"+repoID+".git", ProjectRepositoryRemoteKindLocal, repoID, "main", ProjectRepositoryStatusReady, agentID, agentID, now, now); err != nil {
		t.Fatalf("seed project repository for branch binding: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO project_branch_registry(
  branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id,
  active_task_id, active_claim_id, branch_name, branch_kind, base_branch,
  head_sha, base_sha, write_scope_json, status, updated_by, created_at, updated_at
) VALUES (?, ?, ?, ?, '', ?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		branchID, workspaceID, projectID, repoID, agentID, taskID, "agent/"+agentID+"/lane-gate-bound", ProjectBranchKindFeature, "main", strings.Repeat("2", 40), strings.Repeat("1", 40), `{"paths":["internal/eval/**"]}`, ProjectBranchStatusActive, agentID, now, now); err != nil {
		t.Fatalf("seed project branch binding: %v", err)
	}
}

func boundPCAWTaskPromptContextEnvelope(surface, workspaceID, taskID, agentID string) map[string]any {
	envelope := BuildTaskPromptContextEnvelope(surface, "server_rpc", workspaceID, "agent", agentID)
	envelope["actor_agent_id"] = agentID
	envelope["agent_id"] = agentID
	envelope["task_id"] = taskID
	envelope["claim_status"] = model.TaskClaimStatusClaimed
	return envelope
}

func singleNodePCAWGraph(t *testing.T) dag.Graph {
	t.Helper()
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "node-pcaw", Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	return graph
}

func assertPCAWTaskFields(t *testing.T, gotProjectID, gotTaskKind, gotProjectLane string, gotRequiresProjectGate bool, wantProjectID, wantTaskKind, wantProjectLane string, wantRequiresProjectGate bool) {
	t.Helper()
	if gotProjectID != wantProjectID || gotTaskKind != wantTaskKind || gotProjectLane != wantProjectLane || gotRequiresProjectGate != wantRequiresProjectGate {
		t.Fatalf("project taxonomy = project_id %q task_kind %q project_lane %q requires_project_gate %v, want %q %q %q %v",
			gotProjectID, gotTaskKind, gotProjectLane, gotRequiresProjectGate,
			wantProjectID, wantTaskKind, wantProjectLane, wantRequiresProjectGate)
	}
}
