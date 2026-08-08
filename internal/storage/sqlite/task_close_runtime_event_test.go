package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestCloseTaskWithRuntimeEventSkipsEventForSameResolutionNoOp(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-close-runtime-noop"
		taskID      = "task-close-runtime-noop"
	)
	createCloseableRuntimeTask(t, ctx, store, workspaceID, taskID)

	first, changed, err := store.CloseTaskWithRuntimeEvent(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusResolved,
		Reason:      "initial reason",
	}, taskClosedRuntimeInput(t, workspaceID, taskID, model.TaskStatusResolved, "initial reason"))
	if err != nil {
		t.Fatalf("first close task with runtime event: %v", err)
	}
	if !changed || strings.TrimSpace(first.EventID) == "" {
		t.Fatalf("expected first close to mutate and append runtime event, changed=%v event=%+v", changed, first)
	}
	if first.AuthorityHolderNodeID == "" || first.AuthorityTerm <= 0 || first.AuthorityLeaseTokenFingerprint == "" {
		t.Fatalf("task close runtime event must be authority-backed, got %+v", first)
	}

	second, changed, err := store.CloseTaskWithRuntimeEvent(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusResolved,
		Reason:      "replayed reason must not overwrite close truth",
	}, taskClosedRuntimeInput(t, workspaceID, taskID, model.TaskStatusResolved, "replayed reason"))
	if err != nil {
		t.Fatalf("same-resolution close should be idempotent: %v", err)
	}
	if changed {
		t.Fatalf("expected same-resolution close to be a no-op, got changed=true event=%+v", second)
	}
	if strings.TrimSpace(second.EventID) != "" {
		t.Fatalf("expected no runtime event for same-resolution close, got %+v", second)
	}

	status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
	if err != nil {
		t.Fatalf("get task status after no-op close: %v", err)
	}
	if status.Status != model.TaskStatusResolved {
		t.Fatalf("expected status to remain RESOLVED, got %+v", status)
	}
	assertTaskCloseReason(t, ctx, store, taskID, "initial reason")
	assertRuntimeEventCount(t, ctx, store, workspaceID, taskID, 1)
}

func TestCloseTaskWithRuntimeEventEmitsEventForSameResolutionClaimRepair(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-close-runtime-repair"
		taskID      = "task-close-runtime-repair"
		agentID     = "agent-close-runtime-repair"
	)
	createCloseableRuntimeTask(t, ctx, store, workspaceID, taskID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Close Repair Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, NULL, ?)`,
		taskID,
		workspaceID,
		agentID,
		model.TaskClaimStatusClaimed,
		"out of sync claim",
		"2026-04-22T00:00:00Z",
		"2026-04-22T00:00:00Z",
	); err != nil {
		t.Fatalf("insert out-of-sync task claim: %v", err)
	}

	if _, changed, err := store.CloseTaskWithRuntimeEvent(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusResolved,
		Reason:      "initial close",
	}, taskClosedRuntimeInput(t, workspaceID, taskID, model.TaskStatusResolved, "initial close")); err != nil || !changed {
		t.Fatalf("initial close changed=%v err=%v", changed, err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE task_claims SET claim_status = ?, updated_at = ? WHERE workspace_id = ? AND task_id = ?`,
		model.TaskClaimStatusClaimed,
		"2026-04-22T00:01:00Z",
		workspaceID,
		taskID,
	); err != nil {
		t.Fatalf("force claim status drift: %v", err)
	}

	repair, changed, err := store.CloseTaskWithRuntimeEvent(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusResolved,
		Reason:      "claim repair close",
	}, taskClosedRuntimeInput(t, workspaceID, taskID, model.TaskStatusResolved, "claim repair close"))
	if err != nil {
		t.Fatalf("same-resolution claim repair close: %v", err)
	}
	if !changed || strings.TrimSpace(repair.EventID) == "" {
		t.Fatalf("expected claim repair to mutate with runtime event, changed=%v event=%+v", changed, repair)
	}
	assertTaskCloseClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusCompleted)
	assertRuntimeEventCount(t, ctx, store, workspaceID, taskID, 2)
}

func TestCloseTaskWrapperRequiresWorkspaceAndWritesAuthorityBackedReceipt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-close-wrapper"
		taskID      = "task-close-wrapper"
	)
	createCloseableRuntimeTask(t, ctx, store, workspaceID, taskID)

	if err := store.CloseTask(ctx, sqlite.TaskCloseInput{
		TaskID:     taskID,
		ActorID:    "developer",
		Resolution: model.TaskStatusResolved,
		Reason:     "missing workspace must fail closed",
	}); err == nil || !strings.Contains(err.Error(), "workspace_id is required") {
		t.Fatalf("expected workspace-required close error, got %v", err)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
	if err != nil {
		t.Fatalf("get task status after rejected close: %v", err)
	}
	if status.Status != model.TaskStatusPending {
		t.Fatalf("missing-workspace close should not mutate task, got %+v", status)
	}
	assertRuntimeEventCount(t, ctx, store, workspaceID, taskID, 0)

	if err := store.CloseTask(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusResolved,
		Reason:      "wrapper close records durable receipt",
	}); err != nil {
		t.Fatalf("close task wrapper: %v", err)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.closed",
		EntityType:  "task",
		EntityID:    taskID,
		TaskID:      taskID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list close runtime events: %v", err)
	}
	if len(events) != 1 || events[0].AuthorityHolderNodeID == "" || events[0].AuthorityTerm <= 0 || events[0].AuthorityLeaseTokenFingerprint == "" {
		t.Fatalf("expected authority-backed close receipt, got %+v", events)
	}
}

func TestCloseTaskWithRuntimeEventRejectsTerminalRewrite(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		from string
		to   string
	}{
		{name: "resolved to cancelled", from: model.TaskStatusResolved, to: model.TaskStatusCancelled},
		{name: "resolved to failed", from: model.TaskStatusResolved, to: model.TaskStatusFailed},
		{name: "cancelled to resolved", from: model.TaskStatusCancelled, to: model.TaskStatusResolved},
		{name: "failed to resolved", from: model.TaskStatusFailed, to: model.TaskStatusResolved},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			workspaceID := "ws-terminal-rewrite-" + strings.ReplaceAll(tc.name, " ", "-")
			taskID := "task-terminal-rewrite-" + strings.ReplaceAll(tc.name, " ", "-")
			createCloseableRuntimeTask(t, ctx, store, workspaceID, taskID)

			if _, changed, err := store.CloseTaskWithRuntimeEvent(ctx, sqlite.TaskCloseInput{
				WorkspaceID: workspaceID,
				TaskID:      taskID,
				ActorID:     "developer",
				Resolution:  tc.from,
				Reason:      "original terminal reason",
			}, taskClosedRuntimeInput(t, workspaceID, taskID, tc.from, "original terminal reason")); err != nil || !changed {
				t.Fatalf("initial terminal close changed=%v err=%v", changed, err)
			}

			event, changed, err := store.CloseTaskWithRuntimeEvent(ctx, sqlite.TaskCloseInput{
				WorkspaceID: workspaceID,
				TaskID:      taskID,
				ActorID:     "developer",
				Resolution:  tc.to,
				Reason:      "rewrite attempt",
			}, taskClosedRuntimeInput(t, workspaceID, taskID, tc.to, "rewrite attempt"))
			if err == nil {
				t.Fatalf("expected terminal rewrite %s -> %s to fail", tc.from, tc.to)
			}
			if changed {
				t.Fatalf("terminal rewrite reported changed=true with event %+v", event)
			}
			if strings.TrimSpace(event.EventID) != "" {
				t.Fatalf("terminal rewrite returned runtime event %+v", event)
			}

			status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
			if err != nil {
				t.Fatalf("get task status after terminal rewrite: %v", err)
			}
			if status.Status != tc.from {
				t.Fatalf("expected terminal status to remain %s, got %+v", tc.from, status)
			}
			assertTaskCloseReason(t, ctx, store, taskID, "original terminal reason")
			assertRuntimeEventCount(t, ctx, store, workspaceID, taskID, 1)
		})
	}
}

func TestCloseTaskWithRuntimeEventClearsProjectRunQueuesAndRoles(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-task-close-project-cleanup"
		projectID   = "project-task-close-cleanup"
		taskID      = "task-close-project-cleanup"
		agentID     = "agent-project-cleanup"
		queueKey    = "external_gate:payment_billing:rnar.task.task-close-project-cleanup"
	)
	createCloseableProjectTask(t, ctx, store, workspaceID, projectID, taskID, agentID)
	role, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		ActorID:               agentID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "lead for bounded cleanup test",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.lead.claim",
	})
	if err != nil {
		t.Fatalf("claim project lead: %v", err)
	}
	if _, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:           workspaceID,
		QueueKey:              queueKey,
		QueueType:             "BLOCKER",
		Title:                 "Stale payment blocker",
		Summary:               "This blocker belongs to the task being closed.",
		Details:               "stale",
		AssignedTo:            "developer",
		Urgency:               "HIGH",
		SourceKind:            "session",
		SourceID:              "session-stale",
		TaskID:                taskID,
		AgentID:               agentID,
		PromptContextEnvelope: sqlite.BuildOperatorQueuePromptContextEnvelope("workspace.ops.upsert", "server_rpc", workspaceID, "agent", agentID),
	}); err != nil {
		t.Fatalf("upsert operator queue: %v", err)
	}

	if _, changed, err := store.CloseTaskWithRuntimeEvent(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusFailed,
		Reason:      "bounded run failed",
	}, taskClosedRuntimeInput(t, workspaceID, taskID, model.TaskStatusFailed, "bounded run failed")); err != nil || !changed {
		t.Fatalf("close project task changed=%v err=%v", changed, err)
	}

	var queueStatus, queueResolution string
	if err := store.DB().QueryRowContext(ctx, `SELECT status, resolution FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`, workspaceID, queueKey).Scan(&queueStatus, &queueResolution); err != nil {
		t.Fatalf("query operator queue after close: %v", err)
	}
	if queueStatus != "RESOLVED" || !strings.Contains(queueResolution, "cleared_by_task_close:FAILED") {
		t.Fatalf("expected task close to resolve stale operator queue, got status=%q resolution=%q", queueStatus, queueResolution)
	}
	var roleStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM project_agent_roles WHERE role_id = ?`, role.RoleID).Scan(&roleStatus); err != nil {
		t.Fatalf("query project role after close: %v", err)
	}
	if roleStatus != sqlite.ProjectRoleStatusReleased {
		t.Fatalf("expected final project task close to release active roles, got %q", roleStatus)
	}
}

func TestCloseTaskRejectsEmptyFrontierIdleReflectionHeartbeat(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-close-empty-frontier-reflection"
		projectID    = "project-close-empty-frontier-reflection"
		taskID       = "task-idle-reflection-close-empty-frontier"
		agentID      = "agent-close-empty-frontier"
		heartbeatMsg = "recorded a heartbeat because no safe unclaimed lane emerged"
	)
	createCloseableProjectTask(t, ctx, store, workspaceID, projectID, taskID, agentID)
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "fixture enters delivery phase",
		ActorID:               agentID,
		ActorType:             "agent",
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project to implementation: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE tasks
   SET title = 'Project metacognition pass: inspect empty frontier',
       description = 'Idle reflection with no durable empty-frontier requirements.',
       tags_json = '["meta-reflection","anti-idle","product-quality"]',
       task_requirements_json = '{}'
 WHERE task_id = ?`, taskID); err != nil {
		t.Fatalf("mark close task as idle reflection: %v", err)
	}

	err := store.CloseTask(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     agentID,
		Resolution:  model.TaskStatusResolved,
		Reason:      heartbeatMsg,
	})
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) || !strings.Contains(err.Error(), "empty product frontier") {
		t.Fatalf("expected empty-frontier completion contract error, got %v", err)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
	if err != nil {
		t.Fatalf("get task status after rejected close: %v", err)
	}
	if status.Status != model.TaskStatusPending {
		t.Fatalf("rejected close should leave task pending, got %+v", status)
	}
	var closeReason string
	if err := store.DB().QueryRowContext(ctx, `SELECT COALESCE(close_reason, '') FROM tasks WHERE task_id = ?`, taskID).Scan(&closeReason); err != nil {
		t.Fatalf("query close reason after rejected close: %v", err)
	}
	if closeReason == heartbeatMsg {
		t.Fatalf("rejected close should not persist heartbeat close reason")
	}
}

func TestCloseTaskWithRuntimeEventClearsProjectAdmissionOnCancellation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-task-close-project-admission"
		projectID   = "project-task-close-admission"
		leadID      = "lead-task-close-admission"
		workerID    = "worker-task-close-admission"
		repoID      = "repo-task-close-admission"
		taskID      = "task-close-project-admission"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["src/**","tests/**"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	checkout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-task-close-admission\project`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		BranchName:            "agent/worker-task-close-admission/project",
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register checkout: %v", err)
	}
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-task-close-admission/project",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["src/**","tests/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        `{"paths":["src/**","tests/**"]}`,
		Summary:               "claim interrupted implementation lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, workerID),
	}); err != nil {
		t.Fatalf("claim task with project admission: %v", err)
	}
	const (
		runID  = "run-task-close-project-admission"
		stepID = "step-task-close-project-admission"
	)
	insertActiveExecutionRunForTaskCloseTest(t, ctx, store, workspaceID, taskID, workerID, runID, stepID)

	if _, changed, err := store.CloseTaskWithRuntimeEvent(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusCancelled,
		Reason:      "operator interrupted a stuck implementation lane",
	}, taskClosedRuntimeInput(t, workspaceID, taskID, model.TaskStatusCancelled, "operator interrupted a stuck implementation lane")); err != nil || !changed {
		t.Fatalf("cancel project implementation task changed=%v err=%v", changed, err)
	}

	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		RepoID:          repoID,
		AgentID:         workerID,
		IncludeInactive: true,
	})
	if err != nil {
		t.Fatalf("list branches after cancellation: %v", err)
	}
	if len(branches) != 1 || branches[0].BranchID != branch.BranchID || branches[0].ActiveTaskID != "" || branches[0].ActiveClaimID != "" || branches[0].Status != sqlite.ProjectBranchStatusAbandoned {
		t.Fatalf("expected cancellation to abandon branch and clear active refs, got %+v", branches)
	}
	checkouts, err := store.ListProjectCheckouts(ctx, sqlite.ProjectCheckoutListFilter{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		RepoID:          repoID,
		AgentID:         workerID,
		IncludeInactive: true,
	})
	if err != nil {
		t.Fatalf("list checkouts after cancellation: %v", err)
	}
	if len(checkouts) != 1 || checkouts[0].CheckoutID != checkout.CheckoutID || checkouts[0].ActiveTaskID != "" || checkouts[0].ActiveClaimID != "" {
		t.Fatalf("expected cancellation to clear checkout active refs, got %+v", checkouts)
	}
	var claimStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus); err != nil {
		t.Fatalf("query cancelled claim status: %v", err)
	}
	if claimStatus != model.TaskClaimStatusCancelled {
		t.Fatalf("expected cancelled claim status, got %q", claimStatus)
	}
	var runStatus, runOutcome, runClosedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT status, outcome, COALESCE(closed_at, '') FROM execution_runs WHERE workspace_id = ? AND run_id = ?`, workspaceID, runID).Scan(&runStatus, &runOutcome, &runClosedAt); err != nil {
		t.Fatalf("query cancelled execution run: %v", err)
	}
	if runStatus != "CANCELLED" || runOutcome != "CANCELLED" || runClosedAt == "" {
		t.Fatalf("expected task cancellation to terminalize execution run, got status=%q outcome=%q closed_at=%q", runStatus, runOutcome, runClosedAt)
	}
	var stepStatus, stepCompletedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT status, COALESCE(completed_at, '') FROM execution_steps WHERE workspace_id = ? AND step_id = ?`, workspaceID, stepID).Scan(&stepStatus, &stepCompletedAt); err != nil {
		t.Fatalf("query cancelled execution step: %v", err)
	}
	if stepStatus != "CANCELLED" || stepCompletedAt == "" {
		t.Fatalf("expected task cancellation to terminalize execution step, got status=%q completed_at=%q", stepStatus, stepCompletedAt)
	}
}

func createCloseableRuntimeTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Closeable Runtime Task",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Closeable runtime task",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateGeneric,
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task to workspace: %v", err)
	}
}

func createCloseableProjectTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID, agentID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Closeable Project Task",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Project Cleanup Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Cleanup",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "critical",
		Title:        "Closeable project task",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateGeneric,
		ProjectID:    projectID,
		ProjectLane:  "strategy",
	}, graph); err != nil {
		t.Fatalf("create project task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task to workspace: %v", err)
	}
}

func insertActiveExecutionRunForTaskCloseTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID, runID, stepID string) {
	t.Helper()
	now := "2026-05-05T08:00:00Z"
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO execution_runs(
  run_id, workspace_id, task_id, session_id, agent_id, title, summary,
  status, outcome, verification_json, created_at, updated_at, closed_at
) VALUES (?, ?, ?, NULL, ?, ?, ?, 'ACTIVE', '', '{}', ?, ?, NULL)`,
		runID, workspaceID, taskID, agentID, "Interrupted implementation run", "active run should close with task", now, now); err != nil {
		t.Fatalf("insert active execution run: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO execution_steps(
  step_id, run_id, workspace_id, parent_step_id, phase, title, summary,
  status, sort_order, evidence_json, verification_json, created_at, updated_at, completed_at
) VALUES (?, ?, ?, NULL, 'EXECUTE', ?, ?, 'ACTIVE', 1, '[]', '{}', ?, ?, NULL)`,
		stepID, runID, workspaceID, "Implement interrupted slice", "active step should close with task", now, now); err != nil {
		t.Fatalf("insert active execution step: %v", err)
	}
}

func taskClosedRuntimeInput(t *testing.T, workspaceID, taskID, resolution, reason string) sqlite.RuntimeEventInput {
	t.Helper()

	payload, err := sqlite.AttachTaskPromptContextEnvelope(map[string]any{
		"workspace_id": workspaceID,
		"task_id":      taskID,
		"resolution":   resolution,
		"reason":       reason,
		"status":       resolution,
	}, sqlite.BuildTaskPromptContextEnvelope("task.close", "server_rpc", workspaceID, "human", "developer"))
	if err != nil {
		t.Fatalf("attach task prompt context envelope: %v", err)
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal runtime event payload: %v", err)
	}
	return sqlite.RuntimeEventInput{
		DedupKey:    "task:" + taskID + ":closed:" + resolution,
		WorkspaceID: workspaceID,
		EventType:   "task.closed",
		EntityType:  "task",
		EntityID:    taskID,
		ActorType:   "human",
		ActorID:     "developer",
		TaskID:      taskID,
		PayloadJSON: string(payloadJSON),
	}
}

func assertRuntimeEventCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID string, want int) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.closed",
		EntityType:  "task",
		EntityID:    taskID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != want {
		t.Fatalf("expected %d task.closed runtime events, got %+v", want, events)
	}
}

func assertTaskCloseReason(t *testing.T, ctx context.Context, store *sqlite.Store, taskID, want string) {
	t.Helper()

	var got string
	if err := store.DB().QueryRowContext(ctx, `SELECT COALESCE(close_reason, '') FROM tasks WHERE task_id = ?`, taskID).Scan(&got); err != nil {
		t.Fatalf("query close_reason: %v", err)
	}
	if got != want {
		t.Fatalf("expected close_reason %q, got %q", want, got)
	}
}

func assertTaskCloseClaimStatus(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, want string) {
	t.Helper()

	var got string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&got); err != nil {
		t.Fatalf("query task claim status: %v", err)
	}
	if got != want {
		t.Fatalf("expected claim_status %q, got %q", want, got)
	}
}
