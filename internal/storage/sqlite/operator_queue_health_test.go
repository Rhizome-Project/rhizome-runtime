package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestCurrentOperatorQueueLagSnapshotMarksOverdueQueuesDegraded(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-lag"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Lag",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	overdueAt := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	if _, err := store.UpsertOperatorQueueItem(ctx, OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "manual:follow-up:overdue",
		QueueType:   "FOLLOW_UP",
		Title:       "Investigate queue lag",
		Summary:     "manual follow-up",
		AssignedTo:  "developer",
		Urgency:     "HIGH",
		SourceKind:  "manual",
		SourceID:    "operator",
		DueAt:       overdueAt,
	}); err != nil {
		t.Fatalf("create overdue operator queue: %v", err)
	}

	if _, err := store.UpsertOperatorQueueItem(ctx, OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "knowledge_claim:claim-1:review",
		QueueType:   "FOLLOW_UP",
		Title:       "Review claim claim-1",
		Summary:     "review claim",
		AssignedTo:  "developer",
		Urgency:     "NORMAL",
		SourceKind:  "knowledge_claim",
		SourceID:    "claim-1",
		DueAt:       overdueAt,
	}); err != nil {
		t.Fatalf("create overdue knowledge claim review queue: %v", err)
	}

	snapshot := store.CurrentOperatorQueueLagSnapshot(ctx)
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded operator queue lag snapshot, got %+v", snapshot)
	}
	if snapshot.OpenQueueCount != 2 {
		t.Fatalf("expected two open queues, got %+v", snapshot)
	}
	if snapshot.OverdueQueueCount != 2 {
		t.Fatalf("expected two overdue queues, got %+v", snapshot)
	}
	if snapshot.OverdueClaimReviewUnescalatedCount != 1 {
		t.Fatalf("expected one overdue unescalated claim review queue, got %+v", snapshot)
	}
	if snapshot.OldestOverdueDueAt == "" {
		t.Fatalf("expected oldest overdue due_at to be populated, got %+v", snapshot)
	}
}

func TestCurrentOperatorQueueLagSnapshotMarksStaleOpenQueueDegraded(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-stale-open"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Stale Open",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   "sess-stale-open",
		AgentID:     "agent-stale-open",
		WorkspaceID: workspaceID,
		TaskID:      "task-stale-open",
		StartedAt:   time.Now().UTC().Add(-4 * time.Hour).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if err := store.UpdateAgentSession(ctx, AgentSessionUpdateInput{
		SessionID:    "sess-stale-open",
		Status:       model.SessionStatusEnded,
		CompletedAt:  time.Now().UTC().Add(-3 * time.Hour).Format(time.RFC3339Nano),
		Iterations:   1,
		ToolCalls:    1,
		StopReason:   "completed",
		ErrorMessage: "",
	}); err != nil {
		t.Fatalf("end agent session: %v", err)
	}

	if _, err := store.UpsertOperatorQueueItem(ctx, OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "manual:stale-open:blocker",
		QueueType:         "BLOCKER",
		Title:             "Stale blocker queue",
		Summary:           "terminal session should not retain open blocker",
		AssignedTo:        "developer",
		Urgency:           "HIGH",
		SourceKind:        "manual",
		SourceID:          "sess-stale-open",
		SessionID:         "sess-stale-open",
		KeepSessionActive: true,
	}); err != nil {
		t.Fatalf("create stale open operator queue: %v", err)
	}

	snapshot := store.CurrentOperatorQueueLagSnapshot(ctx)
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded operator queue lag snapshot, got %+v", snapshot)
	}
	if snapshot.StaleOpenQueueCount != 1 {
		t.Fatalf("expected one stale open queue, got %+v", snapshot)
	}
	if snapshot.OldestStaleOpenUpdatedAt == "" {
		t.Fatalf("expected oldest stale open updated_at to be populated, got %+v", snapshot)
	}
}

func TestCurrentOperatorQueueLagSnapshotMarksMissingOperatorQueueDegraded(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-operator-queue-missing"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Operator Queue Missing",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimInternalWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   "sess-missing-queue",
		AgentID:     "agent-missing-queue",
		WorkspaceID: workspaceID,
		TaskID:      "task-missing-queue",
		StartedAt:   time.Now().UTC().Add(-30 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if err := store.UpdateAgentSession(ctx, AgentSessionUpdateInput{
		SessionID:    "sess-missing-queue",
		Status:       model.SessionStatusBlocked,
		Iterations:   1,
		ToolCalls:    1,
		StopReason:   "",
		ErrorMessage: "waiting on operator",
	}); err != nil {
		t.Fatalf("mark agent session blocked: %v", err)
	}

	snapshot := store.CurrentOperatorQueueLagSnapshot(ctx)
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded operator queue lag snapshot, got %+v", snapshot)
	}
	if snapshot.MissingOperatorQueueCount != 1 {
		t.Fatalf("expected one missing operator queue, got %+v", snapshot)
	}
}
