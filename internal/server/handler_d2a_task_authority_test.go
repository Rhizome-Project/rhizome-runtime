package server

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentTaskClaimRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-d2a-agent-task-claim-missing-authority-rpc"
		taskID      = "task-d2a-agent-task-claim-missing-authority-rpc"
		agentID     = "agent-d2a-agent-task-claim-missing-authority-rpc"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createAgentTaskLifecycleFixture(t, ctx, store, workspaceID, agentID, taskID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID)

	raw, err := json.Marshal(agentTaskClaimParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "should fail closed before claim side effects",
	})
	if err != nil {
		t.Fatalf("marshal agent.task.claim params: %v", err)
	}

	result, rpcErr := h.agentTaskClaim(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "agent.task.claim")

	assertNoServerTaskClaimRowForAuthorityReject(t, ctx, store, taskID)
	if got := mustServerTaskStatusForAuthorityReject(t, ctx, store, taskID); got != model.TaskStatusPending {
		t.Fatalf("expected pending task after authority reject, got %q", got)
	}
	if got := countServerTaskClaimAuditEventsForAuthorityReject(t, ctx, store, "task_claimed", taskID); got != 0 {
		t.Fatalf("expected no task_claimed audit events after authority reject, got %d", got)
	}
	assertNoServerTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, "task.claimed")
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to return before authority journaling and keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
	if afterLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID); afterLastSeenAt != beforeLastSeenAt {
		t.Fatalf("expected agent last_seen_at to remain %q, got %q", beforeLastSeenAt, afterLastSeenAt)
	}
}

func TestAgentTaskReleaseRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-d2a-agent-task-release-stale-authority-rpc"
		taskID      = "task-d2a-agent-task-release-stale-authority-rpc"
		agentID     = "agent-d2a-agent-task-release-stale-authority-rpc"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createAgentTaskLifecycleFixture(t, ctx, store, workspaceID, agentID, taskID)
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "seed claimed task before stale authority release",
	}); err != nil {
		t.Fatalf("seed claimed task: %v", err)
	}
	beforeClaim := mustServerTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-711")

	raw, err := json.Marshal(agentTaskReleaseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "should fail closed under stale authority",
	})
	if err != nil {
		t.Fatalf("marshal agent.task.release params: %v", err)
	}

	result, rpcErr := h.agentTaskRelease(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "agent.task.release")

	afterClaim := mustServerTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	if afterClaim != beforeClaim {
		t.Fatalf("expected stale authority reject not to mutate task claim, before=%+v after=%+v", beforeClaim, afterClaim)
	}
	if got := mustServerTaskStatusForAuthorityReject(t, ctx, store, taskID); got != model.TaskStatusRunning {
		t.Fatalf("expected running task after authority reject, got %q", got)
	}
	if got := countServerTaskClaimAuditEventsForAuthorityReject(t, ctx, store, "task_released", taskID); got != 0 {
		t.Fatalf("expected no task_released audit events after authority reject, got %d", got)
	}
	assertNoServerTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, "task.released")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
	if afterLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID); afterLastSeenAt != beforeLastSeenAt {
		t.Fatalf("expected agent last_seen_at to remain %q, got %q", beforeLastSeenAt, afterLastSeenAt)
	}
}

func TestAgentTaskCompleteRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-d2a-agent-task-complete-stale-authority-rpc"
		taskID      = "task-d2a-agent-task-complete-stale-authority-rpc"
		agentID     = "agent-d2a-agent-task-complete-stale-authority-rpc"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createAgentTaskLifecycleFixture(t, ctx, store, workspaceID, agentID, taskID)
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "seed claimed task before stale authority completion",
	}); err != nil {
		t.Fatalf("seed claimed task: %v", err)
	}
	beforeClaim := mustServerTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-712")

	raw, err := json.Marshal(taskCompleteParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "should fail closed under stale authority",
	})
	if err != nil {
		t.Fatalf("marshal agent.task.complete params: %v", err)
	}

	result, rpcErr := h.agentTaskComplete(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "agent.task.complete")

	afterClaim := mustServerTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	if afterClaim != beforeClaim {
		t.Fatalf("expected stale authority reject not to mutate task claim, before=%+v after=%+v", beforeClaim, afterClaim)
	}
	if got := mustServerTaskStatusForAuthorityReject(t, ctx, store, taskID); got != model.TaskStatusRunning {
		t.Fatalf("expected running task after authority reject, got %q", got)
	}
	if got := countServerTaskClaimAuditEventsForAuthorityReject(t, ctx, store, "task_completed", taskID); got != 0 {
		t.Fatalf("expected no task_completed audit events after authority reject, got %d", got)
	}
	assertNoServerTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, "task.completed")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
	if afterLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID); afterLastSeenAt != beforeLastSeenAt {
		t.Fatalf("expected agent last_seen_at to remain %q, got %q", beforeLastSeenAt, afterLastSeenAt)
	}
}

func TestAgentTaskBlockRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-d2a-agent-task-block-stale-authority-rpc"
		taskID      = "task-d2a-agent-task-block-stale-authority-rpc"
		agentID     = "agent-d2a-agent-task-block-stale-authority-rpc"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createAgentTaskLifecycleFixture(t, ctx, store, workspaceID, agentID, taskID)
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "seed claimed task before stale authority block",
	}); err != nil {
		t.Fatalf("seed claimed task: %v", err)
	}
	beforeClaim := mustServerTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-713")

	raw, err := json.Marshal(taskBlockParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "should fail closed under stale authority",
	})
	if err != nil {
		t.Fatalf("marshal agent.task.block params: %v", err)
	}

	result, rpcErr := h.agentTaskBlock(ctx, raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "agent.task.block")

	afterClaim := mustServerTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	if afterClaim != beforeClaim {
		t.Fatalf("expected stale authority reject not to mutate task claim, before=%+v after=%+v", beforeClaim, afterClaim)
	}
	if got := mustServerTaskStatusForAuthorityReject(t, ctx, store, taskID); got != model.TaskStatusRunning {
		t.Fatalf("expected running task after authority reject, got %q", got)
	}
	if got := countServerTaskClaimAuditEventsForAuthorityReject(t, ctx, store, "task_blocked", taskID); got != 0 {
		t.Fatalf("expected no task_blocked audit events after authority reject, got %d", got)
	}
	assertNoServerTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, "task.blocked")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
	if afterLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID); afterLastSeenAt != beforeLastSeenAt {
		t.Fatalf("expected agent last_seen_at to remain %q, got %q", beforeLastSeenAt, afterLastSeenAt)
	}
}

func TestAgentTaskStaleHolderMutationsRejectAfterSessionTakeover(t *testing.T) {
	type staleMutationCase struct {
		name         string
		runtimeEvent string
		auditEvent   string
		call         func(*Handler, context.Context, json.RawMessage) (any, *RPCError)
		params       func(workspaceID, taskID, sourceAgent string) any
	}
	cases := []staleMutationCase{
		{
			name:         "release",
			runtimeEvent: "task.released",
			auditEvent:   "task_released",
			call:         (*Handler).agentTaskRelease,
			params: func(workspaceID, taskID, sourceAgent string) any {
				return agentTaskReleaseParams{
					WorkspaceID: workspaceID,
					TaskID:      taskID,
					AgentID:     sourceAgent,
					Reason:      "stale source holder should not release",
				}
			},
		},
		{
			name:         "complete",
			runtimeEvent: "task.completed",
			auditEvent:   "task_completed",
			call:         (*Handler).agentTaskComplete,
			params: func(workspaceID, taskID, sourceAgent string) any {
				return taskCompleteParams{
					WorkspaceID: workspaceID,
					TaskID:      taskID,
					AgentID:     sourceAgent,
					Summary:     "stale source holder should not complete",
				}
			},
		},
		{
			name:         "block",
			runtimeEvent: "task.blocked",
			auditEvent:   "task_blocked",
			call:         (*Handler).agentTaskBlock,
			params: func(workspaceID, taskID, sourceAgent string) any {
				return taskBlockParams{
					WorkspaceID: workspaceID,
					TaskID:      taskID,
					AgentID:     sourceAgent,
					Reason:      "stale source holder should not block",
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			store := newServerTestStore(t)
			h := NewHandler(store)

			workspaceID := "ws-d2a-agent-task-stale-holder-" + tc.name
			taskID := "task-d2a-agent-task-stale-holder-" + tc.name
			sourceAgent := "agent-d2a-agent-task-stale-source-" + tc.name
			takeoverAgent := "agent-d2a-agent-task-stale-takeover-" + tc.name
			sourceSessionID := "sess-d2a-agent-task-stale-source-" + tc.name
			ctx := testAuthContext(workspaceID, "agent", sourceAgent)

			createAgentTaskLifecycleFixture(t, ctx, store, workspaceID, sourceAgent, taskID)
			if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
				WorkspaceID: workspaceID,
				AgentID:     takeoverAgent,
				OwnerUserID: "developer",
				DisplayName: takeoverAgent,
			}); err != nil {
				t.Fatalf("register takeover agent: %v", err)
			}
			if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
				WorkspaceID: workspaceID,
				TaskID:      taskID,
				AgentID:     sourceAgent,
				Summary:     "source agent claims task before takeover",
			}); err != nil {
				t.Fatalf("seed claimed task: %v", err)
			}
			if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
				EventType:   model.SessionEventStart,
				WorkspaceID: workspaceID,
				SessionID:   sourceSessionID,
				AgentID:     sourceAgent,
				TaskID:      taskID,
				Summary:     "source session starts on claimed task",
				HandoffTo:   takeoverAgent,
			}); err != nil {
				t.Fatalf("start source session: %v", err)
			}
			takeover, err := store.TakeOverAgentSession(ctx, sqlite.AgentSessionTakeoverInput{
				WorkspaceID:      workspaceID,
				SessionID:        sourceSessionID,
				TakeoverAgentID:  takeoverAgent,
				Summary:          "take over stale source task holder",
				SuccessorSummary: "successor owns the task now",
			})
			if err != nil {
				t.Fatalf("take over source session: %v", err)
			}
			if takeover.SourceState.Status != model.SessionStatusEnded || takeover.SuccessorState.Status != model.SessionStatusActive {
				t.Fatalf("unexpected takeover states: %+v", takeover)
			}

			beforeClaim := mustServerTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
			beforeRuntimeEvents := countServerTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, tc.runtimeEvent)
			beforeAuditEvents := countServerTaskClaimAuditEventsForAuthorityReject(t, ctx, store, tc.auditEvent, taskID)

			result, rpcErr := tc.call(h, ctx, mustMarshalJSON(t, tc.params(workspaceID, taskID, sourceAgent)))
			if rpcErr == nil {
				t.Fatalf("expected stale holder %s to reject after takeover", tc.name)
			}
			if result != nil {
				t.Fatalf("expected no result on stale holder %s reject, got %+v", tc.name, result)
			}

			afterClaim := mustServerTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
			if afterClaim != beforeClaim {
				t.Fatalf("expected stale holder reject not to mutate task claim, before=%+v after=%+v", beforeClaim, afterClaim)
			}
			if got := mustServerTaskStatusForAuthorityReject(t, ctx, store, taskID); got != model.TaskStatusRunning {
				t.Fatalf("expected running task after stale holder reject, got %q", got)
			}
			if afterRuntimeEvents := countServerTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, tc.runtimeEvent); afterRuntimeEvents != beforeRuntimeEvents {
				t.Fatalf("expected no %s runtime event after stale holder reject, before=%d after=%d", tc.runtimeEvent, beforeRuntimeEvents, afterRuntimeEvents)
			}
			if afterAuditEvents := countServerTaskClaimAuditEventsForAuthorityReject(t, ctx, store, tc.auditEvent, taskID); afterAuditEvents != beforeAuditEvents {
				t.Fatalf("expected no %s audit event after stale holder reject, before=%d after=%d", tc.auditEvent, beforeAuditEvents, afterAuditEvents)
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

func TestAgentTaskReleaseIdempotentNoOpDoesNotTouchAgentActivity(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-d2a-agent-task-release-idempotent-rpc"
		taskID      = "task-d2a-agent-task-release-idempotent-rpc"
		agentID     = "agent-d2a-agent-task-release-idempotent-rpc"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createAgentTaskLifecycleFixture(t, ctx, store, workspaceID, agentID, taskID)
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "seed claimed task before idempotent release",
	}); err != nil {
		t.Fatalf("seed claimed task: %v", err)
	}

	firstRaw, err := json.Marshal(agentTaskReleaseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "first release",
	})
	if err != nil {
		t.Fatalf("marshal first release params: %v", err)
	}
	if result, rpcErr := h.agentTaskRelease(ctx, firstRaw); rpcErr != nil {
		t.Fatalf("first agent.task.release rpc error: %+v", rpcErr)
	} else if result == nil {
		t.Fatal("expected release result on first release")
	}

	beforeLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeReleasedEvents := countServerTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, "task.released")

	secondRaw, err := json.Marshal(agentTaskReleaseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "idempotent release should be a no-op",
	})
	if err != nil {
		t.Fatalf("marshal second release params: %v", err)
	}
	if result, rpcErr := h.agentTaskRelease(ctx, secondRaw); rpcErr != nil {
		t.Fatalf("second agent.task.release rpc error: %+v", rpcErr)
	} else if result == nil {
		t.Fatal("expected release result on idempotent second release")
	}

	if afterReleasedEvents := countServerTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, "task.released"); afterReleasedEvents != beforeReleasedEvents {
		t.Fatalf("expected idempotent release not to append task.released again, before=%d after=%d", beforeReleasedEvents, afterReleasedEvents)
	}
	if afterLastSeenAt := mustAgentLastSeenAtForTaskAuthority(t, ctx, store, workspaceID, agentID); afterLastSeenAt != beforeLastSeenAt {
		t.Fatalf("expected idempotent release not to touch agent last_seen_at, before=%q after=%q", beforeLastSeenAt, afterLastSeenAt)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected idempotent release not to touch workspace updated_at, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

// TestAgentTaskReleaseAfterTerminalCompletionIsNoOp locks C1: when an agent has
// already driven its own claim to a terminal status (COMPLETED here) in-process,
// a later operator-stop cleanup re-release for that same claim must be tolerated
// as a benign RELEASED no-op instead of surfacing the recurring
// "task claim transition is stale or duplicate" warning. Before C1 the handler
// only tolerated an already-RELEASED claim, so a terminal claim leaked the error
// on every stop (round-39/40/42 recurrence for the agents that finished work).
func TestAgentTaskReleaseAfterTerminalCompletionIsNoOp(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-c1-agent-task-release-terminal-noop"
		taskID      = "task-c1-agent-task-release-terminal-noop"
		agentID     = "agent-c1-agent-task-release-terminal-noop"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createAgentTaskLifecycleFixture(t, ctx, store, workspaceID, agentID, taskID)
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "seed claimed task before terminal completion",
	}); err != nil {
		t.Fatalf("seed claimed task: %v", err)
	}
	if _, err := store.CompleteTaskWithEvent(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "agent finished work in-process before stop sweep",
	}); err != nil {
		t.Fatalf("complete task to terminal claim: %v", err)
	}

	// Sanity: a raw store re-release on the terminal claim is rejected as stale,
	// which is the exact error the handler must now absorb at the RPC boundary.
	if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "raw store re-release of terminal claim",
	}); !errors.Is(err, sqlite.ErrTaskClaimStaleTransition) {
		t.Fatalf("expected store re-release of terminal claim to be stale, got %v", err)
	}

	raw, err := json.Marshal(agentTaskReleaseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "stop cleanup re-release of completed claim",
	})
	if err != nil {
		t.Fatalf("marshal terminal release params: %v", err)
	}
	result, rpcErr := h.agentTaskRelease(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("expected terminal-claim re-release to be a tolerated no-op, got rpc error %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result from tolerated release, got %T", result)
	}
	if status, _ := resultMap["status"].(string); status != "RELEASED" {
		t.Fatalf("expected RELEASED status on tolerated terminal re-release, got %q", status)
	}
}

func TestAgentTaskReleaseReclaimReleaseAfterEndedSession(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-rpf32-agent-task-release-reclaim-ended-session"
		taskID      = "task-rpf32-agent-task-release-reclaim-ended-session"
		agentID     = "agent-rpf32-agent-task-release-reclaim-ended-session"
		sessionID   = "session-rpf32-agent-task-release-reclaim-ended-session"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	createAgentTaskLifecycleFixture(t, ctx, store, workspaceID, agentID, taskID)
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "seed claimed task before transient timeout session",
	}); err != nil {
		t.Fatalf("seed claimed task: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "session starts on claimed task",
	}); err != nil {
		t.Fatalf("start session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventEnd,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "provider timeout ended session before release retry",
	}); err != nil {
		t.Fatalf("end session before release: %v", err)
	}

	rawDefault, err := json.Marshal(agentTaskReleaseParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "default release after ended session remains fail-closed",
	})
	if err != nil {
		t.Fatalf("marshal default release params: %v", err)
	}
	if result, rpcErr := h.agentTaskRelease(ctx, rawDefault); rpcErr == nil || result != nil {
		t.Fatalf("default release after ended session must remain rejected, result=%+v rpcErr=%+v", result, rpcErr)
	}

	rawReclaim, err := json.Marshal(agentTaskReleaseParams{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               agentID,
		Reason:                "transient timeout retry release after ended session",
		SessionTransitionKind: "reclaim_release",
	})
	if err != nil {
		t.Fatalf("marshal reclaim release params: %v", err)
	}
	result, rpcErr := h.agentTaskRelease(ctx, rawReclaim)
	if rpcErr != nil {
		t.Fatalf("expected reclaim_release to release same-agent ended-session claim, got rpc error %+v", rpcErr)
	}
	resultMap, ok := result.(map[string]any)
	if !ok || resultMap["status"] != "RELEASED" {
		t.Fatalf("expected RELEASED result from reclaim release, got %+v", result)
	}
	afterClaim := mustServerTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	if afterClaim.AgentID != agentID || afterClaim.ClaimStatus != model.TaskClaimStatusReleased {
		t.Fatalf("reclaim release should release same-agent ended-session claim, got %+v", afterClaim)
	}
	if got := mustServerTaskStatusForAuthorityReject(t, ctx, store, taskID); got != model.TaskStatusPending {
		t.Fatalf("reclaim release should return running task to pending, got %q", got)
	}
}

type serverTaskClaimRecordForAuthorityReject struct {
	AgentID     string
	ClaimStatus string
	Summary     string
}

func assertTaskAuthorityRejectDetails(t *testing.T, rpcErr *RPCError, rejectCode sqlite.AuthorityRejectCode, surface string) {
	t.Helper()

	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(rejectCode) || details["surface"] != surface {
		t.Fatalf("unexpected authority reject details %+v", details)
	}
}

func mustAgentLastSeenAtForTaskAuthority(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID string) string {
	t.Helper()

	var lastSeenAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT COALESCE(last_seen_at, '') FROM agents WHERE workspace_id = ? AND agent_id = ?`, workspaceID, agentID).Scan(&lastSeenAt); err != nil {
		t.Fatalf("load agent last_seen_at for %s/%s: %v", workspaceID, agentID, err)
	}
	return lastSeenAt
}

func mustServerTaskStatusForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, taskID string) string {
	t.Helper()

	var status string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&status); err != nil {
		t.Fatalf("load task status for %s: %v", taskID, err)
	}
	return status
}

func assertNoServerTaskClaimRowForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, taskID string) {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM task_claims WHERE task_id = ?`, taskID).Scan(&count); err != nil {
		t.Fatalf("count task_claim rows for %s: %v", taskID, err)
	}
	if count != 0 {
		t.Fatalf("expected no task_claim rows for %s, got %d", taskID, count)
	}
}

func mustServerTaskClaimRecordForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID string) serverTaskClaimRecordForAuthorityReject {
	t.Helper()

	var record serverTaskClaimRecordForAuthorityReject
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COALESCE(agent_id, ''), COALESCE(claim_status, ''), COALESCE(summary, '') FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&record.AgentID, &record.ClaimStatus, &record.Summary); err != nil {
		t.Fatalf("load task claim for %s/%s: %v", workspaceID, taskID, err)
	}
	return record
}

func countServerTaskClaimAuditEventsForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, eventType, taskID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_events WHERE event_type = ? AND entity_type = 'task_claim' AND entity_id = ?`, eventType, taskID).Scan(&count); err != nil {
		t.Fatalf("count %s audit events for %s: %v", eventType, taskID, err)
	}
	return count
}

func assertNoServerTaskRuntimeEventsForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, eventType string) {
	t.Helper()

	if got := countServerTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, eventType); got != 0 {
		t.Fatalf("expected no %s runtime events for %s, got %d", eventType, taskID, got)
	}
}

func countServerTaskRuntimeEventsForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, eventType string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "task",
		EntityID:    taskID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events for %s: %v", eventType, taskID, err)
	}
	return len(events)
}

func assertServerTaskAuthorityRejectEvent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, wantRejectCode string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   sqlite.AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list authority rejected events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected authority.rejected runtime event")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority reject payload: %v", err)
	}
	if payload["reject_code"] != wantRejectCode {
		t.Fatalf("expected authority reject code %q, got %+v", wantRejectCode, payload)
	}
}
