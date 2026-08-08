package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// openQueueCountForSession counts OPEN session-derived operator queues for a session.
func openQueueCountForSession(t *testing.T, ctx context.Context, store *Store, workspaceID, sessionID string) int {
	t.Helper()
	var n int
	if err := store.db.QueryRowContext(ctx, `
SELECT COUNT(1) FROM operator_queue_items
 WHERE workspace_id = ? AND session_id = ? AND status = 'OPEN' AND source_kind = 'session_event'`,
		workspaceID, sessionID,
	).Scan(&n); err != nil {
		t.Fatalf("count open queues: %v", err)
	}
	return n
}

// TestReconcileMissingSessionOperatorQueuesRepairsBlocked: a BLOCKED session whose
// operator queue is missing is given its BLOCKER by one reconcile pass.
func TestReconcileMissingSessionOperatorQueuesRepairsBlocked(t *testing.T) {
	t.Parallel()
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-oqr-repair"
		sessionID   = "sess-oqr-repair"
		agentID     = "agent-oqr-repair"
		taskID      = "task-oqr-repair"
	)
	seedBlockingSessionWithoutQueue(t, ctx, store, workspaceID, sessionID, agentID, taskID, model.SessionStatusBlocked)

	if got := openQueueCountForSession(t, ctx, store, workspaceID, sessionID); got != 0 {
		t.Fatalf("precondition: expected no queue, got %d", got)
	}
	if snap := store.CurrentOperatorQueueLagSnapshot(ctx); snap.MissingOperatorQueueCount != 1 {
		t.Fatalf("precondition: expected 1 missing queue, got %+v", snap)
	}

	result, err := store.ReconcileMissingSessionOperatorQueues(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Repaired != 1 || result.Missing != 1 {
		t.Fatalf("expected 1 repaired of 1 missing, got %+v", result)
	}
	if got := openQueueCountForSession(t, ctx, store, workspaceID, sessionID); got != 1 {
		t.Fatalf("expected one OPEN BLOCKER after reconcile, got %d", got)
	}
	if snap := store.CurrentOperatorQueueLagSnapshot(ctx); snap.MissingOperatorQueueCount != 0 {
		t.Fatalf("expected missing count to clear, got %+v", snap)
	}
}

// TestReconcileMissingSessionOperatorQueuesRepairsKeepaliveLatest is the P1
// regression: a session that went BLOCKED and whose LATEST coordination update is
// a keepalive `session.status` (so a naive reconstruct would carry the wrong event
// type and open nothing) must still get its BLOCKER recreated by the sweep.
func TestReconcileMissingSessionOperatorQueuesRepairsKeepaliveLatest(t *testing.T) {
	t.Parallel()
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-oqr-keepalive"
		sessionID   = "sess-oqr-keepalive"
		agentID     = "agent-oqr-keepalive"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID, Title: workspaceID, CreatedBy: "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID, AgentID: agentID, OwnerUserID: "developer", DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	// 1. Real session.blocked (creates the BLOCKER queue).
	blockedState, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, AgentSessionCoordinationInput{
		EventType: model.SessionEventBlocked, WorkspaceID: workspaceID, SessionID: sessionID,
		AgentID: agentID, Summary: "blocked", Status: model.SessionStatusBlocked,
		BlockedOn: []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve"}},
	})
	if err != nil {
		t.Fatalf("record blocked: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync blocked queue: %v", err)
	}
	// 2. A keepalive status event that PRESERVES the BLOCKED status (the latest
	//    coordination update is now session.status, not session.blocked).
	if _, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, AgentSessionCoordinationInput{
		EventType: model.SessionEventStatus, WorkspaceID: workspaceID, SessionID: sessionID,
		AgentID: agentID, Summary: "still blocked, keepalive", Status: model.SessionStatusBlocked,
		KeepSessionActive: localBoolPtr(true),
	}); err != nil {
		t.Fatalf("record keepalive: %v", err)
	}
	// 3. Simulate the projection loss: delete the operator queue out from under it.
	if _, err := store.db.ExecContext(ctx, `DELETE FROM operator_queue_items WHERE workspace_id = ? AND session_id = ?`, workspaceID, sessionID); err != nil {
		t.Fatalf("delete queue: %v", err)
	}
	if got := openQueueCountForSession(t, ctx, store, workspaceID, sessionID); got != 0 {
		t.Fatalf("precondition: expected queue deleted, got %d", got)
	}

	// 4. The sweep must repair it despite the latest update being a keepalive.
	result, err := store.ReconcileMissingSessionOperatorQueues(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Repaired != 1 {
		t.Fatalf("keepalive-latest BLOCKED session must be repaired, got %+v", result)
	}
	if got := openQueueCountForSession(t, ctx, store, workspaceID, sessionID); got != 1 {
		t.Fatalf("expected BLOCKER recreated, got %d", got)
	}
}

// recordBlockingCoordinationWithQueue records a blocking coordination event and
// projects its operator queue, returning nothing (helper for the e2e repair tests).
func recordBlockingCoordinationWithQueue(t *testing.T, ctx context.Context, store *Store, in AgentSessionCoordinationInput) {
	t.Helper()
	state, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, in)
	if err != nil {
		t.Fatalf("record %s coordination: %v", in.EventType, err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, state); err != nil {
		t.Fatalf("sync queue for %s: %v", in.EventType, err)
	}
}

// queueAssigneeForSession returns the assigned_to of the OPEN session-derived queue.
func queueAssigneeForSession(t *testing.T, ctx context.Context, store *Store, workspaceID, sessionID string) string {
	t.Helper()
	var assignee string
	if err := store.db.QueryRowContext(ctx, `
SELECT COALESCE(assigned_to,'') FROM operator_queue_items
 WHERE workspace_id = ? AND session_id = ? AND status = 'OPEN' AND source_kind = 'session_event' LIMIT 1`,
		workspaceID, sessionID,
	).Scan(&assignee); err != nil {
		t.Fatalf("read queue assignee: %v", err)
	}
	return assignee
}

// TestReconcileMissingSessionOperatorQueuesRepairsHandoffWithAssignee: a
// HANDOFF_PENDING session whose queue was lost must be repaired AND retain its
// handoff assignee (the P2 fidelity fix — the reconciler loads full canonical state).
func TestReconcileMissingSessionOperatorQueuesRepairsHandoffWithAssignee(t *testing.T) {
	t.Parallel()
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-oqr-handoff"
		sessionID   = "sess-oqr-handoff"
		agentID     = "agent-oqr-handoff"
		targetAgent = "agent-oqr-handoff-target"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{WorkspaceID: workspaceID, Title: workspaceID, CreatedBy: "developer"}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, a := range []string{agentID, targetAgent} {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{WorkspaceID: workspaceID, AgentID: a, OwnerUserID: "developer", DisplayName: a}); err != nil {
			t.Fatalf("register agent %s: %v", a, err)
		}
	}
	recordBlockingCoordinationWithQueue(t, ctx, store, AgentSessionCoordinationInput{
		EventType: model.SessionEventStatus, WorkspaceID: workspaceID, SessionID: sessionID,
		AgentID: agentID, Summary: "handing off", Status: model.SessionStatusHandoffPending, HandoffTo: targetAgent,
	})
	if got := queueAssigneeForSession(t, ctx, store, workspaceID, sessionID); got != targetAgent {
		t.Fatalf("precondition: expected HANDOFF queue assigned to %s, got %q", targetAgent, got)
	}
	// Lose the queue.
	if _, err := store.db.ExecContext(ctx, `DELETE FROM operator_queue_items WHERE workspace_id = ? AND session_id = ?`, workspaceID, sessionID); err != nil {
		t.Fatalf("delete queue: %v", err)
	}

	result, err := store.ReconcileMissingSessionOperatorQueues(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Repaired != 1 {
		t.Fatalf("expected HANDOFF repair, got %+v", result)
	}
	if got := queueAssigneeForSession(t, ctx, store, workspaceID, sessionID); got != targetAgent {
		t.Fatalf("repaired HANDOFF queue must retain assignee %s, got %q", targetAgent, got)
	}
}

// TestReconcileMissingSessionOperatorQueuesRepairsDecisionWithAssignee mirrors the
// HANDOFF case for WAITING_DECISION (decision_needed_from assignee).
func TestReconcileMissingSessionOperatorQueuesRepairsDecisionWithAssignee(t *testing.T) {
	t.Parallel()
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-oqr-decision"
		sessionID   = "sess-oqr-decision"
		agentID     = "agent-oqr-decision"
		decider     = "agent-oqr-decider"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{WorkspaceID: workspaceID, Title: workspaceID, CreatedBy: "developer"}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, a := range []string{agentID, decider} {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{WorkspaceID: workspaceID, AgentID: a, OwnerUserID: "developer", DisplayName: a}); err != nil {
			t.Fatalf("register agent %s: %v", a, err)
		}
	}
	recordBlockingCoordinationWithQueue(t, ctx, store, AgentSessionCoordinationInput{
		EventType: model.SessionEventDecisionNeeded, WorkspaceID: workspaceID, SessionID: sessionID,
		AgentID: agentID, Summary: "need a decision", Status: model.SessionStatusWaitingDecision,
		DecisionNeededFrom: decider, DecisionType: "approval",
	})
	if got := queueAssigneeForSession(t, ctx, store, workspaceID, sessionID); got != decider {
		t.Fatalf("precondition: expected DECISION queue assigned to %s, got %q", decider, got)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM operator_queue_items WHERE workspace_id = ? AND session_id = ?`, workspaceID, sessionID); err != nil {
		t.Fatalf("delete queue: %v", err)
	}

	result, err := store.ReconcileMissingSessionOperatorQueues(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Repaired != 1 {
		t.Fatalf("expected DECISION repair, got %+v", result)
	}
	if got := queueAssigneeForSession(t, ctx, store, workspaceID, sessionID); got != decider {
		t.Fatalf("repaired DECISION queue must retain assignee %s, got %q", decider, got)
	}
}

// TestReconcileMissingSessionOperatorQueuesIsIdempotent: a second pass over an
// already-repaired session creates no duplicate and reports nothing missing.
func TestReconcileMissingSessionOperatorQueuesIsIdempotent(t *testing.T) {
	t.Parallel()
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-oqr-idem"
		sessionID   = "sess-oqr-idem"
		agentID     = "agent-oqr-idem"
		taskID      = "task-oqr-idem"
	)
	seedBlockingSessionWithoutQueue(t, ctx, store, workspaceID, sessionID, agentID, taskID, model.SessionStatusBlocked)

	if _, err := store.ReconcileMissingSessionOperatorQueues(ctx, 0); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	second, err := store.ReconcileMissingSessionOperatorQueues(ctx, 0)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if second.Missing != 0 || second.Repaired != 0 {
		t.Fatalf("second pass must be a no-op, got %+v", second)
	}
	if got := openQueueCountForSession(t, ctx, store, workspaceID, sessionID); got != 1 {
		t.Fatalf("idempotent reconcile must keep exactly one queue, got %d", got)
	}
}

// TestMissingSessionOperatorQueuesLimitBounds: limit>0 bounds the result; limit<=0
// returns all.
func TestMissingSessionOperatorQueuesLimitBounds(t *testing.T) {
	t.Parallel()
	store := NewTestStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		ws := "ws-oqr-limit-" + string(rune('a'+i))
		seedBlockingSessionWithoutQueue(t, ctx, store, ws, "sess-"+ws, "agent-"+ws, "", model.SessionStatusBlocked)
	}
	all, err := store.MissingSessionOperatorQueues(ctx, 0)
	if err != nil {
		t.Fatalf("unlimited: %v", err)
	}
	if len(all) < 3 {
		t.Fatalf("expected at least 3 missing rows unlimited, got %d", len(all))
	}
	bounded, err := store.MissingSessionOperatorQueues(ctx, 2)
	if err != nil {
		t.Fatalf("bounded: %v", err)
	}
	if len(bounded) != 2 {
		t.Fatalf("limit=2 must bound to 2 rows, got %d", len(bounded))
	}
}

// TestReconcileMissingSessionOperatorQueuesSkipsEndedSession: an ended/terminal
// session must NOT get a resurrected queue (the CA-06 guard holds under the sweep).
func TestReconcileMissingSessionOperatorQueuesSkipsEndedSession(t *testing.T) {
	t.Parallel()
	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-oqr-ended"
		sessionID   = "sess-oqr-ended"
		agentID     = "agent-oqr-ended"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID, Title: workspaceID, CreatedBy: "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID: sessionID, AgentID: agentID, WorkspaceID: workspaceID,
		StartedAt: time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	// Session is ENDED, so MissingSessionOperatorQueues should not even list it.
	if err := store.UpdateAgentSession(ctx, AgentSessionUpdateInput{
		SessionID: sessionID, Status: model.SessionStatusEnded,
		CompletedAt: time.Now().UTC().Format(time.RFC3339Nano), StopReason: "completed",
	}); err != nil {
		t.Fatalf("end session: %v", err)
	}

	rows, err := store.MissingSessionOperatorQueues(ctx, 0)
	if err != nil {
		t.Fatalf("missing query: %v", err)
	}
	for _, r := range rows {
		if r.SessionID == sessionID {
			t.Fatalf("ended session must not be listed as missing-queue: %+v", r)
		}
	}
	result, err := store.ReconcileMissingSessionOperatorQueues(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := openQueueCountForSession(t, ctx, store, workspaceID, sessionID); got != 0 {
		t.Fatalf("ended session must not get a resurrected queue, got %d (result=%+v)", got, result)
	}
}

func TestReconcileTerminalSessionOperatorQueuesResolvesSessionSourcedExternalGate(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-oqr-terminal-session-external-gate"
		sessionID   = "sess-oqr-terminal-session-external-gate"
		agentID     = "agent-oqr-terminal-session-external-gate"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "session starts before external gate opens",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	keepInactive := false
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:         model.SessionEventEnd,
		WorkspaceID:       workspaceID,
		SessionID:         sessionID,
		AgentID:           agentID,
		Summary:           "session ended before terminal-session queue reconcile",
		KeepSessionActive: &keepInactive,
	}); err != nil {
		t.Fatalf("record session end: %v", err)
	}

	externalGate, err := store.UpsertOperatorQueueItem(ctx, OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "external_gate:credential_auth:terminal-session-reconcile",
		QueueType:         "BLOCKER",
		Title:             "Complete credential authorization",
		Summary:           "session-owned external gate left open after stop",
		Urgency:           "NORMAL",
		SourceKind:        "session",
		SourceID:          sessionID,
		SessionID:         sessionID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("upsert external gate queue: %v", err)
	}
	manualQueue, err := store.UpsertOperatorQueueItem(ctx, OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "manual:terminal-session-reconcile",
		QueueType:         "FOLLOW_UP",
		Title:             "Manual follow-up",
		Summary:           "manual same-session queue must remain open",
		Urgency:           "NORMAL",
		SourceKind:        "manual",
		SourceID:          "operator",
		SessionID:         sessionID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("upsert manual queue: %v", err)
	}

	result, err := store.ReconcileTerminalSessionOperatorQueues(ctx, TerminalSessionOperatorQueueReconcileInput{
		ActorType: "system",
		ActorID:   "terminal_session_queue_reconciler",
		Limit:     32,
	})
	if err != nil {
		t.Fatalf("reconcile terminal session queues: %v", err)
	}
	if result.Scanned != 1 || result.Resolved != 1 || result.Problems != 0 {
		t.Fatalf("expected one resolved terminal-session queue, got %+v", result)
	}

	resolved, err := store.GetOperatorQueueItem(ctx, workspaceID, externalGate.QueueID, "")
	if err != nil {
		t.Fatalf("get resolved external gate: %v", err)
	}
	if resolved.Status != "RESOLVED" || resolved.Resolution != "cleared_by_terminal_session_reconcile" || resolved.ResolvedBy == nil || *resolved.ResolvedBy != "terminal_session_queue_reconciler" {
		t.Fatalf("unexpected resolved external gate: %+v", resolved)
	}
	stillOpen, err := store.GetOperatorQueueItem(ctx, workspaceID, manualQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get manual queue: %v", err)
	}
	if stillOpen.Status != "OPEN" {
		t.Fatalf("manual queue should remain open, got %+v", stillOpen)
	}

	var eventActor, eventSession string
	if err := store.db.QueryRowContext(ctx, `
SELECT actor_id, COALESCE(session_id,'')
  FROM runtime_events
 WHERE workspace_id = ? AND event_type = 'operator_queue.resolved' AND entity_id = ?
 ORDER BY COALESCE(ingest_seq,0) DESC, created_at DESC, event_id DESC
 LIMIT 1`,
		workspaceID,
		externalGate.QueueID,
	).Scan(&eventActor, &eventSession); err != nil {
		t.Fatalf("query resolve runtime event: %v", err)
	}
	if eventActor != "terminal_session_queue_reconciler" || eventSession != "" {
		t.Fatalf("unexpected resolve runtime event actor/session: actor=%q session=%q", eventActor, eventSession)
	}
}

func TestReconcileTerminalSessionOperatorQueuesSkipsActiveSession(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-oqr-terminal-session-skip-active"
		sessionID   = "sess-oqr-terminal-session-skip-active"
		agentID     = "agent-oqr-terminal-session-skip-active"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "active session with external gate",
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	gate, err := store.UpsertOperatorQueueItem(ctx, OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "external_gate:credential_auth:active-session-skip",
		QueueType:         "BLOCKER",
		Title:             "Complete credential authorization",
		Summary:           "active session-owned gate",
		Urgency:           "NORMAL",
		SourceKind:        "session",
		SourceID:          sessionID,
		SessionID:         sessionID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("upsert active external gate: %v", err)
	}

	result, err := store.ReconcileTerminalSessionOperatorQueues(ctx, TerminalSessionOperatorQueueReconcileInput{
		ActorType: "system",
		ActorID:   "terminal_session_queue_reconciler",
	})
	if err != nil {
		t.Fatalf("reconcile terminal session queues: %v", err)
	}
	if result.Scanned != 0 || result.Resolved != 0 || result.Problems != 0 {
		t.Fatalf("active session should not be scanned, got %+v", result)
	}
	stillOpen, err := store.GetOperatorQueueItem(ctx, workspaceID, gate.QueueID, "")
	if err != nil {
		t.Fatalf("get active gate: %v", err)
	}
	if stillOpen.Status != "OPEN" {
		t.Fatalf("active session gate should remain open, got %+v", stillOpen)
	}
}
