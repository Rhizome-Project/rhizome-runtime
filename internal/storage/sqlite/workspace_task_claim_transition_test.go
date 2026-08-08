package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestTaskClaimTransitionsFailClosedOnDuplicateAndTerminalReopen(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID    = "ws-task-claim-transition-guard"
		agentID        = "agent-task-claim-transition-guard"
		claimTaskID    = "task-task-claim-transition-guard-claim"
		releaseTaskID  = "task-task-claim-transition-guard-release"
		completeTaskID = "task-task-claim-transition-guard-complete"
		terminalTaskID = "task-task-claim-transition-guard-terminal"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Claim Transition Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Task Claim Guard Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	for _, taskID := range []string{claimTaskID, releaseTaskID, completeTaskID, terminalTaskID} {
		createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      claimTaskID,
		AgentID:     agentID,
		Summary:     "initial claim",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, claimTaskID, model.TaskClaimStatusClaimed)
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      claimTaskID,
		AgentID:     agentID,
		Summary:     "duplicate claim",
	}); !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) {
		t.Fatalf("expected duplicate claim to fail closed with stale transition, got %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, claimTaskID, model.TaskClaimStatusClaimed)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      releaseTaskID,
		AgentID:     agentID,
		Summary:     "claim before release",
	}); err != nil {
		t.Fatalf("claim release task: %v", err)
	}
	insertActiveExecutionRunForTaskCloseTest(t, ctx, store, workspaceID, releaseTaskID, agentID, "run-task-claim-release-active", "step-task-claim-release-active")
	if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      releaseTaskID,
		AgentID:     agentID,
		Reason:      "waiting",
	}); err != nil {
		t.Fatalf("release task claim: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, releaseTaskID, model.TaskClaimStatusReleased)
	var releaseRunStatus, releaseRunOutcome, releaseRunClosedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT status, outcome, COALESCE(closed_at, '') FROM execution_runs WHERE workspace_id = ? AND run_id = ?`, workspaceID, "run-task-claim-release-active").Scan(&releaseRunStatus, &releaseRunOutcome, &releaseRunClosedAt); err != nil {
		t.Fatalf("query released execution run: %v", err)
	}
	if releaseRunStatus != "CANCELLED" || releaseRunOutcome != "RELEASED" || releaseRunClosedAt == "" {
		t.Fatalf("expected release to cancel active execution run, got status=%q outcome=%q closed_at=%q", releaseRunStatus, releaseRunOutcome, releaseRunClosedAt)
	}
	var releaseStepStatus, releaseStepCompletedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT status, COALESCE(completed_at, '') FROM execution_steps WHERE workspace_id = ? AND step_id = ?`, workspaceID, "step-task-claim-release-active").Scan(&releaseStepStatus, &releaseStepCompletedAt); err != nil {
		t.Fatalf("query released execution step: %v", err)
	}
	if releaseStepStatus != "CANCELLED" || releaseStepCompletedAt == "" {
		t.Fatalf("expected release to cancel active execution step, got status=%q completed_at=%q", releaseStepStatus, releaseStepCompletedAt)
	}
	if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      releaseTaskID,
		AgentID:     agentID,
		Reason:      "duplicate release",
	}); !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) {
		t.Fatalf("expected duplicate release to fail closed with stale transition, got %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, releaseTaskID, model.TaskClaimStatusReleased)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      completeTaskID,
		AgentID:     agentID,
		Summary:     "claim before completion",
	}); err != nil {
		t.Fatalf("claim completion task: %v", err)
	}
	insertActiveExecutionRunForTaskCloseTest(t, ctx, store, workspaceID, completeTaskID, agentID, "run-task-claim-complete-active", "step-task-claim-complete-active")
	if err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      completeTaskID,
		AgentID:     agentID,
		Summary:     "complete once",
	}); err != nil {
		t.Fatalf("complete task: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, completeTaskID, model.TaskClaimStatusCompleted)
	var completeRunStatus, completeRunOutcome, completeRunClosedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT status, outcome, COALESCE(closed_at, '') FROM execution_runs WHERE workspace_id = ? AND run_id = ?`, workspaceID, "run-task-claim-complete-active").Scan(&completeRunStatus, &completeRunOutcome, &completeRunClosedAt); err != nil {
		t.Fatalf("query completed execution run: %v", err)
	}
	if completeRunStatus != "COMPLETED" || completeRunOutcome != "COMPLETED" || completeRunClosedAt == "" {
		t.Fatalf("expected completion to close active execution run, got status=%q outcome=%q closed_at=%q", completeRunStatus, completeRunOutcome, completeRunClosedAt)
	}
	var completeStepStatus, completeStepCompletedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT status, COALESCE(completed_at, '') FROM execution_steps WHERE workspace_id = ? AND step_id = ?`, workspaceID, "step-task-claim-complete-active").Scan(&completeStepStatus, &completeStepCompletedAt); err != nil {
		t.Fatalf("query completed execution step: %v", err)
	}
	if completeStepStatus != "COMPLETED" || completeStepCompletedAt == "" {
		t.Fatalf("expected completion to close active execution step, got status=%q completed_at=%q", completeStepStatus, completeStepCompletedAt)
	}
	if err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      completeTaskID,
		AgentID:     agentID,
		Summary:     "duplicate complete",
	}); !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) {
		t.Fatalf("expected duplicate complete to fail closed with stale transition, got %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, completeTaskID, model.TaskClaimStatusCompleted)
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      completeTaskID,
		AgentID:     agentID,
		Summary:     "terminal reopen",
	}); !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) {
		t.Fatalf("expected terminal reopen claim to fail closed with stale transition, got %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, completeTaskID, model.TaskClaimStatusCompleted)

	if err := store.CloseTask(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      terminalTaskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusCancelled,
		Reason:      "terminal before first claim",
	}); err != nil {
		t.Fatalf("close never-claimed task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      terminalTaskID,
		AgentID:     agentID,
		Summary:     "claim terminal task",
	}); !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) {
		t.Fatalf("expected terminal never-claimed task claim to fail closed, got %v", err)
	}
	assertWorkspaceTaskClaimStatusNil(t, ctx, store, workspaceID, terminalTaskID)
}

func TestAgentTaskPromptContextEnvelopeCarriesLifecycleEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID    = "ws-agent-task-prompt-context"
		agentID        = "agent-task-prompt-context"
		claimTaskID    = "task-agent-task-prompt-context-claim"
		releaseTaskID  = "task-agent-task-prompt-context-release"
		completeTaskID = "task-agent-task-prompt-context-complete"
		blockTaskID    = "task-agent-task-prompt-context-block"
	)
	createAgentTaskPromptContextFixture(t, ctx, store, workspaceID, agentID, claimTaskID, releaseTaskID, completeTaskID, blockTaskID)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: releaseTaskID, AgentID: agentID}); err != nil {
		t.Fatalf("seed release claim: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: completeTaskID, AgentID: agentID}); err != nil {
		t.Fatalf("seed complete claim: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: blockTaskID, AgentID: agentID}); err != nil {
		t.Fatalf("seed block claim: %v", err)
	}

	claimEvent, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                claimTaskID,
		AgentID:               agentID,
		Summary:               "claim with context",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.claim", workspaceID, claimTaskID, agentID),
	})
	if err != nil {
		t.Fatalf("claim task with prompt context: %v", err)
	}
	assertAgentTaskRuntimePromptContext(t, claimEvent, "agent.task.claim", workspaceID, claimTaskID, agentID)

	releaseEvent, err := store.ReleaseTaskClaimWithEvent(ctx, sqlite.TaskReleaseInput{
		WorkspaceID:           workspaceID,
		TaskID:                releaseTaskID,
		AgentID:               agentID,
		Reason:                "release with context",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.release", workspaceID, releaseTaskID, agentID),
	})
	if err != nil {
		t.Fatalf("release task with prompt context: %v", err)
	}
	assertAgentTaskRuntimePromptContext(t, releaseEvent, "agent.task.release", workspaceID, releaseTaskID, agentID)

	completeEvent, err := store.CompleteTaskWithEvent(ctx, sqlite.TaskCompleteInput{
		WorkspaceID:           workspaceID,
		TaskID:                completeTaskID,
		AgentID:               agentID,
		Summary:               "complete with context",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.complete", workspaceID, completeTaskID, agentID),
	})
	if err != nil {
		t.Fatalf("complete task with prompt context: %v", err)
	}
	assertAgentTaskRuntimePromptContext(t, completeEvent, "agent.task.complete", workspaceID, completeTaskID, agentID)

	blockEvent, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID:           workspaceID,
		TaskID:                blockTaskID,
		AgentID:               agentID,
		Reason:                "block with context",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.block", workspaceID, blockTaskID, agentID),
	})
	if err != nil {
		t.Fatalf("block task with prompt context: %v", err)
	}
	assertAgentTaskRuntimePromptContext(t, blockEvent, "agent.task.block", workspaceID, blockTaskID, agentID)
}

func TestCompleteTaskWithEventClearsProjectQueuesAndFinalRoles(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-complete-project-cleanup"
		projectID   = "project-agent-complete-cleanup"
		agentID     = "agent-agent-complete-cleanup"
		rootTaskID  = "task-agent-complete-cleanup-root"
		implTaskID  = "task-agent-complete-cleanup-impl"
		queueKey    = "external_gate:peer_review:rnar.task.agent-complete-cleanup"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Complete Project Cleanup",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Agent Complete Cleanup",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, rootTaskID)
	createWorkspaceTask(t, ctx, store, workspaceID, implTaskID)
	for _, taskID := range []string{rootTaskID, implTaskID} {
		projectIDValue := projectID
		if _, err := store.UpdateTaskProjectFields(ctx, sqlite.TaskProjectFieldsUpdateInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			ProjectID:   &projectIDValue,
			ActorID:     agentID,
		}); err != nil {
			t.Fatalf("link task %s to project: %v", taskID, err)
		}
	}
	role, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		ActorID:               agentID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "lead for agent completion cleanup test",
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
		Title:                 "Stale peer review blocker",
		Summary:               "This blocker should clear when the implementation task completes.",
		Details:               "stale implementation blocker",
		SourceKind:            "session_event",
		SourceID:              "session-agent-complete-cleanup",
		TaskID:                implTaskID,
		AgentID:               agentID,
		PromptContextEnvelope: sqlite.BuildOperatorQueuePromptContextEnvelope("workspace.ops.upsert", "server_rpc", workspaceID, "agent", agentID),
	}); err != nil {
		t.Fatalf("upsert operator queue: %v", err)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: implTaskID, AgentID: agentID}); err != nil {
		t.Fatalf("claim implementation task: %v", err)
	}
	if _, err := store.CompleteTaskWithEvent(ctx, sqlite.TaskCompleteInput{
		WorkspaceID:           workspaceID,
		TaskID:                implTaskID,
		AgentID:               agentID,
		Summary:               "implementation complete",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.complete", workspaceID, implTaskID, agentID),
	}); err != nil {
		t.Fatalf("complete implementation task: %v", err)
	}
	var queueStatus, queueResolution string
	if err := store.DB().QueryRowContext(ctx, `SELECT status, resolution FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`, workspaceID, queueKey).Scan(&queueStatus, &queueResolution); err != nil {
		t.Fatalf("query queue after implementation completion: %v", err)
	}
	if queueStatus != "RESOLVED" || !strings.Contains(queueResolution, "cleared_by_task_close:RESOLVED") {
		t.Fatalf("expected agent task completion to resolve stale queue, got status=%q resolution=%q", queueStatus, queueResolution)
	}
	assertProjectRoleStatus(t, ctx, store, role.RoleID, sqlite.ProjectRoleStatusActive)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{WorkspaceID: workspaceID, TaskID: rootTaskID, AgentID: agentID}); err != nil {
		t.Fatalf("claim root task: %v", err)
	}
	if _, err := store.CompleteTaskWithEvent(ctx, sqlite.TaskCompleteInput{
		WorkspaceID:           workspaceID,
		TaskID:                rootTaskID,
		AgentID:               agentID,
		Summary:               "project complete",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.complete", workspaceID, rootTaskID, agentID),
	}); err != nil {
		t.Fatalf("complete root task: %v", err)
	}
	assertProjectRoleStatus(t, ctx, store, role.RoleID, sqlite.ProjectRoleStatusReleased)
}

func TestCompleteTaskRejectsEmptyFrontierIdleReflectionHeartbeat(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-empty-frontier-reflection-complete"
		projectID    = "project-empty-frontier-reflection-complete"
		agentID      = "epsilon"
		reflectionID = "task-idle-reflection-project-empty-frontier"
	)
	setupEmptyFrontierReflectionCompletionFixture(t, ctx, store, workspaceID, projectID, agentID, reflectionID)

	err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      reflectionID,
		AgentID:     agentID,
		Summary:     "no safe unclaimed lane emerged, so record a heartbeat and stop",
	})
	if !errors.Is(err, sqlite.ErrTaskCompletionContract) || !strings.Contains(err.Error(), "empty product frontier") {
		t.Fatalf("expected empty-frontier completion contract error, got %v", err)
	}
	assertTaskStatus(t, ctx, store, reflectionID, model.TaskStatusRunning)
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, reflectionID, model.TaskClaimStatusClaimed)

	createEmptyFrontierGuardProjectTask(t, ctx, store, workspaceID, projectID, "task-empty-frontier-implementation-followup", "implementation", nil)
	if err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      reflectionID,
		AgentID:     agentID,
		Summary:     "opened a bounded implementation task for the uncovered acceptance gap",
	}); err != nil {
		t.Fatalf("completion should pass once a non-reflection project task exists: %v", err)
	}
	assertTaskStatus(t, ctx, store, reflectionID, model.TaskStatusResolved)
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, reflectionID, model.TaskClaimStatusCompleted)
}

func TestCompleteTaskAllowsEmptyFrontierIdleReflectionAfterProjectDone(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-empty-frontier-reflection-done"
		projectID    = "project-empty-frontier-reflection-done"
		agentID      = "zeta"
		reflectionID = "task-idle-reflection-project-done"
	)
	setupEmptyFrontierReflectionCompletionFixture(t, ctx, store, workspaceID, projectID, agentID, reflectionID)

	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseDone,
		Reason:                "durable verification found no remaining substantial product gap",
		ActorID:               agentID,
		ActorType:             "agent",
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project to DONE: %v", err)
	}
	if err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      reflectionID,
		AgentID:     agentID,
		Summary:     "project is DONE with durable convergence evidence",
	}); err != nil {
		t.Fatalf("completion should pass after project DONE: %v", err)
	}
	assertTaskStatus(t, ctx, store, reflectionID, model.TaskStatusResolved)
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, reflectionID, model.TaskClaimStatusCompleted)
}

func TestAgentTaskPromptContextEnvelopeRejectsForgedClaimBinding(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{
			name: "wrong surface",
			mutate: func(envelope map[string]any) {
				envelope["surface"] = "task.close"
			},
			want: "expected \"agent.task.claim\"",
		},
		{
			name: "wrong workspace",
			mutate: func(envelope map[string]any) {
				envelope["workspace_id"] = "ws-other"
			},
			want: "workspace_id",
		},
		{
			name: "wrong principal",
			mutate: func(envelope map[string]any) {
				envelope["principal_id"] = "agent-forged"
			},
			want: "principal_id",
		},
		{
			name: "wrong task",
			mutate: func(envelope map[string]any) {
				envelope["task_id"] = "task-forged"
			},
			want: "task_id",
		},
		{
			name: "nested wrong workspace",
			mutate: func(envelope map[string]any) {
				nested := make(map[string]any, len(envelope))
				for key, value := range envelope {
					nested[key] = value
				}
				nested["workspace_id"] = "ws-other"
				envelope["nested_false_context"] = nested
			},
			want: "workspace_id",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			suffix := strings.ReplaceAll(tc.name, " ", "-")
			workspaceID := "ws-agent-task-forged-" + suffix
			agentID := "agent-task-forged-" + suffix
			taskID := "task-agent-task-forged-" + suffix
			createAgentTaskPromptContextFixture(t, ctx, store, workspaceID, agentID, taskID)
			envelope := boundAgentTaskPromptContextEnvelope("agent.task.claim", workspaceID, taskID, agentID)
			tc.mutate(envelope)

			_, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
				WorkspaceID:           workspaceID,
				TaskID:                taskID,
				AgentID:               agentID,
				Summary:               "forged context should fail",
				PromptContextEnvelope: envelope,
			})
			if err == nil {
				t.Fatal("expected forged task prompt context to fail closed")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unexpected forged prompt context error: %v", err)
			}
			assertWorkspaceTaskClaimStatusNil(t, ctx, store, workspaceID, taskID)
			if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "task.claimed",
				EntityType:  "task",
				EntityID:    taskID,
				Limit:       10,
			}); got != 0 {
				t.Fatalf("forged prompt context appended task.claimed events: %d", got)
			}
		})
	}
}

func TestTaskClaimTerminalTransitionFailsClosedWhenTaskStateDrifts(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-claim-terminal-task-guard"
		agentID     = "agent-task-claim-terminal-task-guard"
		taskID      = "task-task-claim-terminal-task-guard"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Claim Terminal Task Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Terminal Task Guard Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "claim before external task drift",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)

	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE tasks SET status = ?, close_reason = ?, updated_at = ? WHERE task_id = ?`,
		model.TaskStatusCancelled,
		"external cancellation",
		"2099-01-01T00:00:00Z",
		taskID,
	); err != nil {
		t.Fatalf("force external task state drift: %v", err)
	}
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusCancelled)

	err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "stale completion should not clobber cancellation",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) {
		t.Fatalf("expected terminal transition over drifted task to fail closed, got %v", err)
	}
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusCancelled)
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusClaimed)
}

func createAgentTaskPromptContextFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string, taskIDs ...string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Task Prompt Context",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	for _, taskID := range taskIDs {
		createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	}
}

func setupEmptyFrontierReflectionCompletionFixture(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, agentID, reflectionID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Empty Frontier Reflection Completion",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       projectID,
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
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
	createEmptyFrontierGuardProjectTask(t, ctx, store, workspaceID, projectID, reflectionID, "qa", []string{"meta-reflection", "anti-idle", "product-quality", "metacognition-scope-global"})
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE tasks
   SET title = 'Project metacognition pass: inspect plan, blockers, and next improvements',
       description = 'Profile-scoped product-quality iteration task with no durable empty-frontier requirements.',
       task_requirements_json = '{}'
 WHERE task_id = ?`, reflectionID); err != nil {
		t.Fatalf("mark reflection task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      reflectionID,
		AgentID:     agentID,
		Summary:     "claim empty-frontier reflection",
	}); err != nil {
		t.Fatalf("claim reflection task: %v", err)
	}
}

func createEmptyFrontierGuardProjectTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID, lane string, tags []string) {
	t.Helper()
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	tagsJSON := "[]"
	if len(tags) > 0 {
		raw, err := json.Marshal(tags)
		if err != nil {
			t.Fatalf("marshal task tags: %v", err)
		}
		tagsJSON = string(raw)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE tasks
   SET project_id = ?,
       project_lane = ?,
       task_kind = ?,
       task_template = 'generic',
       tags_json = ?
 WHERE task_id = ?`,
		projectID, lane, model.TaskKindExecution, tagsJSON, taskID); err != nil {
		t.Fatalf("link project task %s: %v", taskID, err)
	}
}

func boundAgentTaskPromptContextEnvelope(surface, workspaceID, taskID, agentID string) map[string]any {
	envelope := sqlite.BuildTaskPromptContextEnvelope(surface, "server_rpc", workspaceID, "agent", agentID)
	envelope["actor_agent_id"] = agentID
	envelope["agent_id"] = agentID
	envelope["task_id"] = taskID
	envelope["claim_status"] = agentTaskClaimStatusForTestSurface(surface)
	return envelope
}

func assertAgentTaskRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantTaskID, wantAgentID string) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode task runtime payload: %v", err)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected task prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	required := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_task_write",
		"surface":                            wantSurface,
		"origin":                             "server_rpc",
		"workspace_id":                       wantWorkspaceID,
		"principal_type":                     "agent",
		"principal_id":                       wantAgentID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
		"actor_agent_id":                     wantAgentID,
		"agent_id":                           wantAgentID,
		"task_id":                            wantTaskID,
	}
	if wantClaimStatus := agentTaskClaimStatusForTestSurface(wantSurface); wantClaimStatus != "" {
		required["claim_status"] = wantClaimStatus
		if got := payload["claim_status"]; got != wantClaimStatus {
			t.Fatalf("task runtime payload claim_status = %v, want %s in %+v", got, wantClaimStatus, payload)
		}
	}
	for key, want := range required {
		got, ok := envelope[key].(string)
		if !ok {
			t.Fatalf("task prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
		}
		if got != want {
			t.Fatalf("task prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
		}
	}
}

func assertProjectRoleStatus(t *testing.T, ctx context.Context, store *sqlite.Store, roleID, want string) {
	t.Helper()

	var got string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM project_agent_roles WHERE role_id = ?`, roleID).Scan(&got); err != nil {
		t.Fatalf("query project role %s: %v", roleID, err)
	}
	if got != want {
		t.Fatalf("project role %s status = %q, want %q", roleID, got, want)
	}
}

func agentTaskClaimStatusForTestSurface(surface string) string {
	switch surface {
	case "agent.task.claim":
		return model.TaskClaimStatusClaimed
	case "agent.task.release":
		return model.TaskClaimStatusReleased
	case "agent.task.complete":
		return model.TaskClaimStatusCompleted
	case "agent.task.block":
		return model.TaskClaimStatusBlocked
	default:
		return ""
	}
}
