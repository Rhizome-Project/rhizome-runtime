package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// seedBlockingSessionWithoutQueue creates a workspace + a session driven into a
// blocking phase via the base-table update path (CreateAgentSession +
// UpdateAgentSession), which does NOT create an operator queue -- the exact
// "missing queue" condition MissingSessionOperatorQueues must detect.
func seedBlockingSessionWithoutQueue(t *testing.T, ctx context.Context, store *Store, workspaceID, sessionID, agentID, taskID, status string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent %s: %v", agentID, err)
	}
	if taskID != "" {
		graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "n1", Type: "generic"}}})
		if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
			TaskID: taskID, OwnerUserID: "developer", Priority: "normal", Title: taskID, TaskKind: "EXECUTION",
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, TaskAttachmentInput{
			WorkspaceID: workspaceID, TaskID: taskID, LinkedBy: "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", taskID, err)
		}
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Add(-30 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create agent session %s: %v", sessionID, err)
	}
	if err := store.UpdateAgentSession(ctx, AgentSessionUpdateInput{
		SessionID:    sessionID,
		Status:       status,
		Iterations:   1,
		ToolCalls:    1,
		ErrorMessage: "waiting on operator",
	}); err != nil {
		t.Fatalf("mark agent session %s status %s: %v", sessionID, status, err)
	}
}

func TestMissingSessionOperatorQueuesReturnsAllBlockingPhases(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	cases := []struct {
		ws, sess, agent, task, status, wantQueue string
	}{
		{"ws-msoq-blocked", "sess-msoq-blocked", "agent-msoq-1", "task-msoq-1", model.SessionStatusBlocked, "BLOCKER"},
		{"ws-msoq-decision", "sess-msoq-decision", "agent-msoq-2", "task-msoq-2", model.SessionStatusWaitingDecision, "DECISION"},
		{"ws-msoq-handoff", "sess-msoq-handoff", "agent-msoq-3", "task-msoq-3", model.SessionStatusHandoffPending, "HANDOFF"},
	}
	for _, c := range cases {
		seedBlockingSessionWithoutQueue(t, ctx, store, c.ws, c.sess, c.agent, c.task, c.status)
	}

	rows, err := store.MissingSessionOperatorQueues(ctx, 0)
	if err != nil {
		t.Fatalf("MissingSessionOperatorQueues: %v", err)
	}
	bySession := make(map[string]MissingSessionOperatorQueue, len(rows))
	for _, r := range rows {
		bySession[r.SessionID] = r
	}
	for _, c := range cases {
		got, ok := bySession[c.sess]
		if !ok {
			t.Fatalf("expected %s (%s) in missing set, got rows=%+v", c.sess, c.status, rows)
		}
		if got.RequiredQueueType != c.wantQueue {
			t.Fatalf("%s: required queue type = %q, want %q", c.sess, got.RequiredQueueType, c.wantQueue)
		}
		if got.WorkspaceID != c.ws || got.AgentID != c.agent || got.TaskID != c.task || got.Status != c.status {
			t.Fatalf("%s: row carries wrong identity fields: %+v", c.sess, got)
		}
	}
}

// TestMissingSessionOperatorQueuesOmitsSessionsWithQueue proves the anti-join:
// a session that DOES have its OPEN session-derived queue (created via the real
// session.blocked coordination sync) is not reported as missing.
func TestMissingSessionOperatorQueuesOmitsSessionsWithQueue(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-msoq-haz-queue"
		sessionID   = "sess-msoq-haz-queue"
		agentID     = "agent-msoq-haz-queue"
		taskID      = "task-msoq-haz-queue"
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

	// Record a real session.blocked event; the coordination sync creates the
	// BLOCKER operator queue, so this session must NOT appear as missing.
	blockedState, _, err := store.RecordAgentSessionCoordinationWithEvent(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "blocked with a real queue",
		Status:      model.SessionStatusBlocked,
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "human_input", Detail: "approve"}},
	})
	if err != nil {
		t.Fatalf("record session.blocked: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue: %v", err)
	}

	rows, err := store.MissingSessionOperatorQueues(ctx, 0)
	if err != nil {
		t.Fatalf("MissingSessionOperatorQueues: %v", err)
	}
	for _, r := range rows {
		if r.SessionID == sessionID {
			t.Fatalf("session with an OPEN BLOCKER queue must not be reported missing, got %+v", r)
		}
	}
}

func TestSessionEventForBlockingStatusInverseMapping(t *testing.T) {
	cases := map[string]string{
		model.SessionStatusBlocked:         model.SessionEventBlocked,
		model.SessionStatusWaitingDecision: model.SessionEventDecisionNeeded,
		model.SessionStatusHandoffPending:  model.SessionEventStatus,
		model.SessionStatusActive:          "",
		"ENDED":                            "",
	}
	for status, want := range cases {
		if got := sessionEventForBlockingStatus(status); got != want {
			t.Fatalf("sessionEventForBlockingStatus(%q) = %q, want %q", status, got, want)
		}
	}
}
