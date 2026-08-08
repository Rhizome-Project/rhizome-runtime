package sqlite_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestTaskClaimLegalTransitionsCoverClaimReleaseReclaimComplete(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-claim-legal-path"
		agentID     = "agent-task-claim-legal-path"
		taskID      = "task-task-claim-legal-path"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Claim Legal Path",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Legal Path Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "claim available task",
	}); err != nil {
		t.Fatalf("claim available task: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusClaimed)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)

	if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "pause for reassessment",
	}); err != nil {
		t.Fatalf("release claimed task: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusReleased)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusPending)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "reclaim released task",
	}); err != nil {
		t.Fatalf("reclaim released task: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusClaimed)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)

	if err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "complete running task",
	}); err != nil {
		t.Fatalf("complete running task: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusCompleted)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusResolved)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "terminal reopen",
	}); !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) {
		t.Fatalf("expected terminal reopen claim to fail closed with stale transition, got %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusCompleted)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusResolved)
}

func TestTaskClaimRejectsRunningTaskWithoutLiveClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID     = "ws-task-claim-running-without-live-claim"
		agentID         = "agent-task-claim-running-without-live-claim"
		unclaimedTaskID = "task-running-unclaimed"
		releasedTaskID  = "task-running-released"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Claim Running Without Live Claim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Running Claim Guard Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	for _, taskID := range []string{unclaimedTaskID, releasedTaskID} {
		createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE tasks SET status = ? WHERE task_id = ?`, model.TaskStatusRunning, unclaimedTaskID); err != nil {
		t.Fatalf("mark unclaimed task running: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      unclaimedTaskID,
		AgentID:     agentID,
		Summary:     "must not create fresh claim on running task",
	}); !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) {
		t.Fatalf("expected unclaimed RUNNING task claim to fail closed, got %v", err)
	}
	assertWorkspaceTaskClaimStatusNil(t, ctx, store, workspaceID, unclaimedTaskID)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      releasedTaskID,
		AgentID:     agentID,
		Summary:     "claim before release",
	}); err != nil {
		t.Fatalf("claim released task setup: %v", err)
	}
	if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      releasedTaskID,
		AgentID:     agentID,
		Reason:      "release before inconsistent running status",
	}); err != nil {
		t.Fatalf("release task setup: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE tasks SET status = ? WHERE task_id = ?`, model.TaskStatusRunning, releasedTaskID); err != nil {
		t.Fatalf("mark released task running: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      releasedTaskID,
		AgentID:     agentID,
		Summary:     "must not reclaim inconsistent running task",
	}); !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) {
		t.Fatalf("expected released RUNNING task claim to fail closed, got %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, releasedTaskID, model.TaskClaimStatusReleased)
}

func TestTaskClaimBlockedClaimCanOnlyBeReclaimedByOwner(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-claim-blocked-owner"
		ownerAgent  = "agent-task-claim-blocked-owner"
		peerAgent   = "agent-task-claim-blocked-peer"
		taskID      = "task-task-claim-blocked-owner"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Claim Blocked Owner",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{ownerAgent, peerAgent} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     ownerAgent,
		Summary:     "owner has local implementation state",
	}); err != nil {
		t.Fatalf("owner claim task: %v", err)
	}
	if err := store.BlockTaskClaim(ctx, taskID, workspaceID, ownerAgent); err != nil {
		t.Fatalf("block task claim: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusBlocked)
	assertWorkspaceTaskClaimAgent(t, ctx, store, workspaceID, taskID, ownerAgent)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     peerAgent,
		Summary:     "peer should not steal blocked work",
	}); !errors.Is(err, sqlite.ErrTaskClaimConflict) {
		t.Fatalf("expected peer claim of blocked work to conflict, got %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusBlocked)
	assertWorkspaceTaskClaimAgent(t, ctx, store, workspaceID, taskID, ownerAgent)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     ownerAgent,
		Summary:     "owner explicitly resumes blocked work",
	}); err != nil {
		t.Fatalf("owner reclaim blocked task: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusClaimed)
	assertWorkspaceTaskClaimAgent(t, ctx, store, workspaceID, taskID, ownerAgent)
}

func assertWorkspaceTaskClaimAgent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, want string) {
	t.Helper()

	var got string
	if err := store.DB().QueryRowContext(ctx, `SELECT agent_id FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&got); err != nil {
		t.Fatalf("query task claim agent: %v", err)
	}
	if got != want {
		t.Fatalf("task %s claim agent = %s, want %s", taskID, got, want)
	}
}

func TestTaskClaimTerminalTransitionFailsClosedAfterSessionTakeover(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID     = "ws-task-claim-session-takeover-guard"
		sourceAgent     = "agent-task-claim-source"
		takeoverAgent   = "agent-task-claim-takeover"
		taskID          = "task-task-claim-session-takeover-guard"
		sourceSessionID = "sess-task-claim-source"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Claim Session Takeover Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{sourceAgent, takeoverAgent} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     sourceAgent,
		Summary:     "source agent claims task",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusClaimed)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)

	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sourceSessionID,
		AgentID:     sourceAgent,
		TaskID:      taskID,
		Summary:     "source session starts on task",
		HandoffTo:   takeoverAgent,
	}); err != nil {
		t.Fatalf("start source session: %v", err)
	}

	takeover, err := store.TakeOverAgentSession(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:      workspaceID,
		SessionID:        sourceSessionID,
		TakeoverAgentID:  takeoverAgent,
		Summary:          "take over stale source session",
		SuccessorSummary: "successor owns the task now",
	})
	if err != nil {
		t.Fatalf("take over agent session: %v", err)
	}
	if takeover.SourceState.Status != model.SessionStatusEnded {
		t.Fatalf("expected source session to end during takeover, got %+v", takeover.SourceState)
	}
	if takeover.SuccessorState.Status != model.SessionStatusActive {
		t.Fatalf("expected successor session to be active after takeover, got %+v", takeover.SuccessorState)
	}

	err = store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     sourceAgent,
		Summary:     "stale source holder should not complete",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) && !errors.Is(err, sqlite.ErrTaskClaimConflict) {
		t.Fatalf("expected stale holder completion to fail closed, got %v", err)
	}

	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusClaimed)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)

	sourceState, err := store.GetAgentSessionState(ctx, workspaceID, sourceSessionID)
	if err != nil {
		t.Fatalf("get source session state: %v", err)
	}
	if sourceState.Status != model.SessionStatusEnded {
		t.Fatalf("expected source session to remain ended after stale task completion attempt, got %+v", sourceState)
	}
	if sourceState.TaskID != taskID {
		t.Fatalf("expected source session to stay attached to task %s, got %+v", taskID, sourceState)
	}

	successorState, err := store.GetAgentSessionState(ctx, workspaceID, takeover.SuccessorState.SessionID)
	if err != nil {
		t.Fatalf("get successor session state: %v", err)
	}
	if successorState.Status != model.SessionStatusActive || successorState.AgentID != takeoverAgent {
		t.Fatalf("expected successor session to remain active for takeover agent, got %+v", successorState)
	}
}

func TestTaskClaimStaleHolderMutationsFailClosedAfterSessionTakeover(t *testing.T) {
	t.Parallel()

	type staleMutationCase struct {
		name         string
		runtimeEvent string
		mutate       func(context.Context, *sqlite.Store, string, string, string) error
	}
	cases := []staleMutationCase{
		{
			name:         "release",
			runtimeEvent: "task.released",
			mutate: func(ctx context.Context, store *sqlite.Store, workspaceID, taskID, sourceAgent string) error {
				return store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
					WorkspaceID: workspaceID,
					TaskID:      taskID,
					AgentID:     sourceAgent,
					Reason:      "stale source holder should not release",
				})
			},
		},
		{
			name:         "complete",
			runtimeEvent: "task.completed",
			mutate: func(ctx context.Context, store *sqlite.Store, workspaceID, taskID, sourceAgent string) error {
				return store.CompleteTask(ctx, sqlite.TaskCompleteInput{
					WorkspaceID: workspaceID,
					TaskID:      taskID,
					AgentID:     sourceAgent,
					Summary:     "stale source holder should not complete",
				})
			},
		},
		{
			name:         "block",
			runtimeEvent: "task.blocked",
			mutate: func(ctx context.Context, store *sqlite.Store, workspaceID, taskID, sourceAgent string) error {
				return store.BlockTask(ctx, sqlite.TaskBlockInput{
					WorkspaceID: workspaceID,
					TaskID:      taskID,
					AgentID:     sourceAgent,
					Reason:      "stale source holder should not block",
				})
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()

			workspaceID := "ws-task-claim-stale-holder-" + tc.name
			sourceAgent := "agent-task-claim-stale-source-" + tc.name
			takeoverAgent := "agent-task-claim-stale-takeover-" + tc.name
			taskID := "task-task-claim-stale-holder-" + tc.name
			sourceSessionID := "sess-task-claim-stale-source-" + tc.name

			if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
				WorkspaceID: workspaceID,
				Title:       "Task Claim Stale Holder " + tc.name,
				CreatedBy:   "developer",
			}); err != nil {
				t.Fatalf("create workspace: %v", err)
			}
			claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
			for _, agentID := range []string{sourceAgent, takeoverAgent} {
				if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
					WorkspaceID: workspaceID,
					AgentID:     agentID,
					OwnerUserID: "developer",
					DisplayName: agentID,
				}); err != nil {
					t.Fatalf("register agent %s: %v", agentID, err)
				}
			}
			createWorkspaceTask(t, ctx, store, workspaceID, taskID)

			if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
				WorkspaceID: workspaceID,
				TaskID:      taskID,
				AgentID:     sourceAgent,
				Summary:     "source agent claims task",
			}); err != nil {
				t.Fatalf("claim task: %v", err)
			}
			if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
				EventType:   model.SessionEventStart,
				WorkspaceID: workspaceID,
				SessionID:   sourceSessionID,
				AgentID:     sourceAgent,
				TaskID:      taskID,
				Summary:     "source session starts on task",
				HandoffTo:   takeoverAgent,
			}); err != nil {
				t.Fatalf("start source session: %v", err)
			}
			takeover, err := store.TakeOverAgentSession(ctx, sqlite.AgentSessionTakeoverInput{
				WorkspaceID:      workspaceID,
				SessionID:        sourceSessionID,
				TakeoverAgentID:  takeoverAgent,
				Summary:          "take over stale source session",
				SuccessorSummary: "successor owns the task now",
			})
			if err != nil {
				t.Fatalf("take over agent session: %v", err)
			}
			if takeover.SourceState.Status != model.SessionStatusEnded || takeover.SuccessorState.Status != model.SessionStatusActive {
				t.Fatalf("unexpected takeover states: %+v", takeover)
			}

			beforeEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   tc.runtimeEvent,
				EntityType:  "task",
				EntityID:    taskID,
				TaskID:      taskID,
				Limit:       10,
			})

			err = tc.mutate(ctx, store, workspaceID, taskID, sourceAgent)
			if !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) && !errors.Is(err, sqlite.ErrTaskClaimConflict) {
				t.Fatalf("expected stale holder %s to fail closed, got %v", tc.name, err)
			}

			assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusClaimed)
			assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)
			if afterEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   tc.runtimeEvent,
				EntityType:  "task",
				EntityID:    taskID,
				TaskID:      taskID,
				Limit:       10,
			}); afterEvents != beforeEvents {
				t.Fatalf("expected no %s runtime event after stale holder reject, before=%d after=%d", tc.runtimeEvent, beforeEvents, afterEvents)
			}

			sourceState, err := store.GetAgentSessionState(ctx, workspaceID, sourceSessionID)
			if err != nil {
				t.Fatalf("get source session state: %v", err)
			}
			if sourceState.Status != model.SessionStatusEnded {
				t.Fatalf("expected source session to remain ended, got %+v", sourceState)
			}
			activeSessions, err := store.ListWorkspaceSessionStates(ctx, workspaceID, true, 10)
			if err != nil {
				t.Fatalf("list active session states: %v", err)
			}
			if len(activeSessions) != 1 || activeSessions[0].SessionID != takeover.SuccessorState.SessionID {
				t.Fatalf("expected only successor session to remain active, got %+v", activeSessions)
			}
		})
	}
}

func TestTaskClaimReleaseFailsClosedAfterSessionEnds(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-claim-ended-session-release-guard"
		agentID     = "agent-task-claim-ended-session"
		taskID      = "task-task-claim-ended-session-release-guard"
		sessionID   = "sess-task-claim-ended-session"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Claim Ended Session Release Guard",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Ended Session Release Guard Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "claim before session end",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "start task session",
	}); err != nil {
		t.Fatalf("start session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventEnd,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "end task session",
	}); err != nil {
		t.Fatalf("end session: %v", err)
	}

	err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "stale owner should not release after session ended",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) {
		t.Fatalf("expected ended-session release to fail closed, got %v", err)
	}

	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusClaimed)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)
}

func TestTaskClaimBlockParksClaimAfterSessionEnds(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-claim-ended-session-block-park"
		agentID     = "agent-task-claim-ended-session-block"
		taskID      = "task-task-claim-ended-session-block-park"
		sessionID   = "sess-task-claim-ended-session-block"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Claim Ended Session Block Park",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Ended Session Block Park Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "claim before session end",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "start task session",
	}); err != nil {
		t.Fatalf("start session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventEnd,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "end task session before parking block",
	}); err != nil {
		t.Fatalf("end session: %v", err)
	}

	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "park stale execution blocker after session ended",
	}); err != nil {
		t.Fatalf("block after ended session should park claim: %v", err)
	}

	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusBlocked)
	assertTaskStatus(t, ctx, store, taskID, model.TaskStatusRunning)
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "task.blocked",
		EntityType:  "task",
		EntityID:    taskID,
		TaskID:      taskID,
		Limit:       10,
	}); got != 1 {
		t.Fatalf("expected one task.blocked runtime event after parking block, got %d", got)
	}
}
