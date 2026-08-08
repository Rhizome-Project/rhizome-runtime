package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// T-1: Verifies R-3, R-9 — create a session, get it back, all fields match.
func TestCreateAndGetAgentSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	input := sqlite.AgentSessionCreateInput{
		SessionID:   "sess-test-1",
		AgentID:     "agent-1",
		WorkspaceID: "ws-1",
		TaskID:      "task-1",
		StartedAt:   now,
	}

	if err := store.CreateAgentSession(ctx, input); err != nil {
		t.Fatalf("create session: %v", err)
	}

	got, err := store.GetAgentSession(ctx, "sess-test-1")
	if err != nil {
		t.Fatalf("get session: %v", err)
	}

	if got.SessionID != "sess-test-1" {
		t.Fatalf("expected session_id %q, got %q", "sess-test-1", got.SessionID)
	}
	if got.AgentID != "agent-1" {
		t.Fatalf("expected agent_id %q, got %q", "agent-1", got.AgentID)
	}
	if got.WorkspaceID != "ws-1" {
		t.Fatalf("expected workspace_id %q, got %q", "ws-1", got.WorkspaceID)
	}
	if got.TaskID != "task-1" {
		t.Fatalf("expected task_id %q, got %q", "task-1", got.TaskID)
	}
	if got.Status != "RUNNING" {
		t.Fatalf("expected status %q, got %q", "RUNNING", got.Status)
	}
	if got.StartedAt != now {
		t.Fatalf("expected started_at %q, got %q", now, got.StartedAt)
	}
	if got.CreatedAt == "" {
		t.Fatal("created_at should not be empty")
	}
}

// T-2: Verifies R-5 — create session, update it, verify status/iterations changed.
func TestUpdateAgentSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-update-1",
		AgentID:     "agent-1",
		WorkspaceID: "ws-1",
		StartedAt:   now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	completedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.UpdateAgentSession(ctx, sqlite.AgentSessionUpdateInput{
		SessionID:         "sess-update-1",
		Status:            "COMPLETED",
		Iterations:        5,
		TotalInputTokens:  1000,
		TotalOutputTokens: 500,
		ToolCalls:         3,
		StopReason:        "end_turn",
		CompletedAt:       completedAt,
	}); err != nil {
		t.Fatalf("update session: %v", err)
	}

	got, err := store.GetAgentSession(ctx, "sess-update-1")
	if err != nil {
		t.Fatalf("get session: %v", err)
	}

	if got.Status != "COMPLETED" {
		t.Fatalf("expected status %q, got %q", "COMPLETED", got.Status)
	}
	if got.Iterations != 5 {
		t.Fatalf("expected iterations 5, got %d", got.Iterations)
	}
	if got.TotalInputTokens != 1000 {
		t.Fatalf("expected input tokens 1000, got %d", got.TotalInputTokens)
	}
	if got.TotalOutputTokens != 500 {
		t.Fatalf("expected output tokens 500, got %d", got.TotalOutputTokens)
	}
	if got.ToolCalls != 3 {
		t.Fatalf("expected tool calls 3, got %d", got.ToolCalls)
	}
	if got.StopReason != "end_turn" {
		t.Fatalf("expected stop_reason %q, got %q", "end_turn", got.StopReason)
	}
}

// T-3: Verifies R-7, R-11 — append 3 messages, list returns them in sequence order.
func TestAppendAndListMessages(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-msg-1",
		AgentID:     "agent-1",
		WorkspaceID: "ws-1",
		StartedAt:   now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	messages := []sqlite.AgentSessionMessageInput{
		{SessionID: "sess-msg-1", Sequence: 0, Role: "user", ContentJSON: `[{"type":"text","text":"hello"}]`, TokenCount: 10},
		{SessionID: "sess-msg-1", Sequence: 1, Role: "assistant", ContentJSON: `[{"type":"text","text":"hi"}]`, TokenCount: 5},
		{SessionID: "sess-msg-1", Sequence: 2, Role: "user", ContentJSON: `[{"type":"text","text":"bye"}]`, TokenCount: 8},
	}

	for _, msg := range messages {
		if err := store.AppendAgentSessionMessage(ctx, msg); err != nil {
			t.Fatalf("append message seq=%d: %v", msg.Sequence, err)
		}
	}

	got, err := store.ListAgentSessionMessages(ctx, "sess-msg-1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(got))
	}
	for i, msg := range got {
		if msg.Sequence != i {
			t.Fatalf("msg[%d]: expected sequence %d, got %d", i, i, msg.Sequence)
		}
		if msg.Role != messages[i].Role {
			t.Fatalf("msg[%d]: expected role %q, got %q", i, messages[i].Role, msg.Role)
		}
		if msg.ContentJSON != messages[i].ContentJSON {
			t.Fatalf("msg[%d]: content mismatch", i)
		}
	}
}

// T-4: Verifies EC-1 — get non-existent session returns ErrSessionNotFound.
func TestGetAgentSession_NotFound(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	_, err := store.GetAgentSession(ctx, "nonexistent-session")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sqlite.ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound, got: %v", err)
	}
}

// T-5: Verifies EC-2 — appending same sequence twice returns error.
func TestAppendMessage_DuplicateSequence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-dup-1",
		AgentID:     "agent-1",
		WorkspaceID: "ws-1",
		StartedAt:   now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	msg := sqlite.AgentSessionMessageInput{
		SessionID:   "sess-dup-1",
		Sequence:    0,
		Role:        "user",
		ContentJSON: `[{"type":"text","text":"hello"}]`,
	}

	if err := store.AppendAgentSessionMessage(ctx, msg); err != nil {
		t.Fatalf("first append: %v", err)
	}

	err := store.AppendAgentSessionMessage(ctx, msg)
	if err == nil {
		t.Fatal("expected error on duplicate sequence, got nil")
	}
}

func TestReplaceAgentSessionMessages(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-replace-1",
		AgentID:     "agent-1",
		WorkspaceID: "ws-1",
		StartedAt:   now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	for _, msg := range []sqlite.AgentSessionMessageInput{
		{SessionID: "sess-replace-1", Sequence: 0, Role: "user", ContentJSON: `[{"type":"text","text":"first"}]`, TokenCount: 10},
		{SessionID: "sess-replace-1", Sequence: 1, Role: "assistant", ContentJSON: `[{"type":"text","text":"second"}]`, TokenCount: 12},
	} {
		if err := store.AppendAgentSessionMessage(ctx, msg); err != nil {
			t.Fatalf("append message: %v", err)
		}
	}

	replacement := []sqlite.AgentSessionMessageInput{
		{SessionID: "sess-replace-1", Sequence: 0, Role: "user", ContentJSON: `[{"type":"text","text":"summary"}]`, TokenCount: 7},
	}
	if err := store.ReplaceAgentSessionMessages(ctx, "sess-replace-1", replacement); err != nil {
		t.Fatalf("replace session messages: %v", err)
	}

	got, err := store.ListAgentSessionMessages(ctx, "sess-replace-1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 message after replace, got %+v", got)
	}
	if got[0].ContentJSON != replacement[0].ContentJSON || got[0].TokenCount != replacement[0].TokenCount {
		t.Fatalf("unexpected replaced message: %+v", got[0])
	}
}

// T-6: Verifies R-12 — create 3 sessions for same agent, list returns them newest first.
func TestListAgentSessions(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		startedAt := time.Date(2025, 1, 1+i, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
		if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
			SessionID:   fmt.Sprintf("sess-list-%d", i),
			AgentID:     "agent-list",
			WorkspaceID: "ws-1",
			StartedAt:   startedAt,
		}); err != nil {
			t.Fatalf("create session %d: %v", i, err)
		}
	}

	got, err := store.ListAgentSessions(ctx, "ws-1", "agent-list", 10)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 sessions, got %d", len(got))
	}

	// Newest first (session 2 before session 1 before session 0)
	if got[0].SessionID != "sess-list-2" {
		t.Fatalf("expected newest first, got %q", got[0].SessionID)
	}
	if got[2].SessionID != "sess-list-0" {
		t.Fatalf("expected oldest last, got %q", got[2].SessionID)
	}
}

// T-7: Verifies EC-3 — list for unknown agent returns empty slice.
func TestListAgentSessions_Empty(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	got, err := store.ListAgentSessions(ctx, "ws-1", "nonexistent-agent", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 sessions, got %d", len(got))
	}
}

// Verifies output contract — ListAgentSessionMessages returns empty non-nil slice when no messages.
func TestListAgentSessionMessages_Empty(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-empty-msgs",
		AgentID:     "agent-1",
		WorkspaceID: "ws-1",
		StartedAt:   now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	got, err := store.ListAgentSessionMessages(ctx, "sess-empty-msgs")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(got))
	}
}

// Verifies R-12 — default limit when <= 0.
func TestListAgentSessions_DefaultLimit(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	// Create 25 sessions
	for i := 0; i < 25; i++ {
		startedAt := time.Date(2025, 1, 1, i, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
		if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
			SessionID:   fmt.Sprintf("sess-limit-%02d", i),
			AgentID:     "agent-limit",
			WorkspaceID: "ws-1",
			StartedAt:   startedAt,
		}); err != nil {
			t.Fatalf("create session %d: %v", i, err)
		}
	}

	// Call with limit 0, should default to 20
	got, err := store.ListAgentSessions(ctx, "ws-1", "agent-limit", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 20 {
		t.Fatalf("expected 20 sessions (default limit), got %d", len(got))
	}
}

// Verifies R-5 — UpdateAgentSession with error message.
func TestUpdateAgentSession_WithError(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-err-1",
		AgentID:     "agent-1",
		WorkspaceID: "ws-1",
		StartedAt:   now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	if err := store.UpdateAgentSession(ctx, sqlite.AgentSessionUpdateInput{
		SessionID:    "sess-err-1",
		Status:       "FAILED",
		ErrorMessage: "context cancelled",
		CompletedAt:  now,
	}); err != nil {
		t.Fatalf("update session: %v", err)
	}

	got, err := store.GetAgentSession(ctx, "sess-err-1")
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != "FAILED" {
		t.Fatalf("expected status FAILED, got %q", got.Status)
	}
	if got.ErrorMessage != "context cancelled" {
		t.Fatalf("expected error_message %q, got %q", "context cancelled", got.ErrorMessage)
	}
}

func TestAgentSessionLifecycleRuntimeEventMethods(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-session-runtime-events"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Runtime Events",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-runtime",
		OwnerUserID: "developer",
		DisplayName: "Runtime Agent",
		Role:        "worker",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	startEvent, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-runtime-event-1",
		AgentID:     "agent-runtime",
		WorkspaceID: workspaceID,
		StartedAt:   startedAt,
	})
	if err != nil {
		t.Fatalf("create session with runtime event: %v", err)
	}
	if startEvent.EventType != "agent.session.started" || startEvent.EntityID != "sess-runtime-event-1" || startEvent.SessionID != "sess-runtime-event-1" {
		t.Fatalf("unexpected start event: %+v", startEvent)
	}

	completedAt := time.Now().UTC().Format(time.RFC3339Nano)
	doneEvent, err := store.UpdateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionUpdateInput{
		SessionID:         "sess-runtime-event-1",
		Status:            "COMPLETED",
		Iterations:        2,
		TotalInputTokens:  100,
		TotalOutputTokens: 50,
		ToolCalls:         3,
		StopReason:        "end_turn",
		CompletedAt:       completedAt,
	})
	if err != nil {
		t.Fatalf("update session with runtime event: %v", err)
	}
	if doneEvent.EventType != "agent.session.completed" || doneEvent.EntityID != "sess-runtime-event-1" || doneEvent.SessionID != "sess-runtime-event-1" {
		t.Fatalf("unexpected completion event: %+v", doneEvent)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "agent_session",
		EntityID:    "sess-runtime-event-1",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 2 || events[0].EventType != "agent.session.completed" || events[1].EventType != "agent.session.started" {
		t.Fatalf("expected start and completion runtime receipts, got %+v", events)
	}
}

func TestUpdateAgentSessionMetricsDoesNotReviveTerminalLifecycle(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-session-metrics-terminal"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Session Metrics Terminal",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-metrics-terminal",
		OwnerUserID: "developer",
		DisplayName: "Metrics Terminal Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-metrics-terminal",
		AgentID:     "agent-metrics-terminal",
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session with runtime event: %v", err)
	}
	completedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.UpdateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionUpdateInput{
		SessionID:   "sess-metrics-terminal",
		Status:      "COMPLETED",
		Iterations:  3,
		ToolCalls:   1,
		CompletedAt: completedAt,
	}); err != nil {
		t.Fatalf("complete session with runtime event: %v", err)
	}
	if err := store.UpdateAgentSession(ctx, sqlite.AgentSessionUpdateInput{
		SessionID:        "sess-metrics-terminal",
		Status:           "RUNNING",
		Iterations:       9,
		TotalInputTokens: 123,
		ToolCalls:        4,
	}); err != nil {
		t.Fatalf("late metric update: %v", err)
	}
	got, err := store.GetAgentSession(ctx, "sess-metrics-terminal")
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != "COMPLETED" || got.CompletedAt != completedAt {
		t.Fatalf("late metric update revived terminal lifecycle: %+v", got)
	}
	if got.Iterations != 9 || got.TotalInputTokens != 123 || got.ToolCalls != 4 {
		t.Fatalf("late metric update should still record metrics, got %+v", got)
	}
}
