package sqlite_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestSessionCompactionSnapshotLifecycle(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-compaction-snap",
		Title:       "Compaction Snapshots",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, "ws-compaction-snap")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-compaction-snap",
		AgentID:     "agent-snap",
		OwnerUserID: "developer",
		DisplayName: "Snapshot Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-snap",
		AgentID:     "agent-snap",
		WorkspaceID: "ws-compaction-snap",
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-compaction-snap",
		MemoryType:  "SUMMARY",
		Title:       "Compaction summary",
		Body:        "Conversation summary after compaction.",
		AgentID:     "agent-snap",
		SessionID:   "sess-snap",
		SourceKind:  "compaction",
		SourceID:    "sess-snap",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	snapshot, err := store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
		SessionID:              "sess-snap",
		WorkspaceID:            "ws-compaction-snap",
		AgentID:                "agent-snap",
		TriggerKind:            "token_budget_exceeded",
		TokenBudget:            1000,
		MessageCountBefore:     10,
		MessageCountAfter:      4,
		MessageTokensBefore:    1800,
		MessageTokensAfter:     720,
		TotalInputTokens:       2400,
		TotalOutputTokens:      600,
		SummaryText:            "Conversation summary after compaction.",
		SummaryWorkspaceMemory: record.MemoryID,
	})
	if err != nil {
		t.Fatalf("record session compaction snapshot: %v", err)
	}
	if snapshot.SummaryWorkspaceMemory != record.MemoryID {
		t.Fatalf("expected summary memory link %q, got %+v", record.MemoryID, snapshot)
	}

	items, err := store.ListSessionCompactionSnapshots(ctx, sqlite.SessionCompactionSnapshotFilter{
		WorkspaceID: "ws-compaction-snap",
		SessionID:   "sess-snap",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list session compaction snapshots: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 snapshot, got %+v", items)
	}
	if items[0].MessageTokensBefore != 1800 || items[0].MessageTokensAfter != 720 || items[0].TotalTokens != 3000 {
		t.Fatalf("unexpected snapshot token counts: %+v", items[0])
	}
}

func TestSessionCompactionSnapshotRejectsMismatchedAgentOwnership(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-compaction-snap-mismatch"
		agentID     = "agent-snap-owner"
		otherAgent  = "agent-snap-other"
		sessionID   = "sess-snap-mismatch"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Compaction Snapshot Ownership",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, id := range []string{agentID, otherAgent} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     id,
			OwnerUserID: "developer",
			DisplayName: id,
		}); err != nil {
			t.Fatalf("register agent %s: %v", id, err)
		}
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	_, err := store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     otherAgent,
	})
	if err == nil || !strings.Contains(err.Error(), "session_id does not belong to agent_id") {
		t.Fatalf("expected session/agent ownership rejection, got %v", err)
	}
}

func TestSessionCompactionSnapshotRejectsNonCompactionSummaryWorkspaceMemory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-compaction-snap-summary"
		agentID     = "agent-snap-summary"
		sessionID   = "sess-snap-summary"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Compaction Snapshot Summary Validation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Snapshot Summary Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "SUMMARY",
		Title:       "Manual summary",
		Body:        "This memory is not compaction-backed.",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "manual",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	_, err = store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
		SessionID:              sessionID,
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		SummaryWorkspaceMemory: record.MemoryID,
	})
	if err == nil || !strings.Contains(err.Error(), "must reference compaction workspace memory") {
		t.Fatalf("expected compaction summary validation error, got %v", err)
	}
}
