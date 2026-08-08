package sqlite_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestMemoryPackWriteWritesThroughCompactionSnapshot(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-pack-write"
		agentID     = "agent-memory-pack"
		sessionID   = "sess-memory-pack"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Pack Write",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Pack Agent",
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
	summaryMemory, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "SUMMARY",
		Title:       "Compaction summary",
		Body:        "Canonical summary memory for pack write.",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "compaction",
		SourceID:    sessionID,
	})
	if err != nil {
		t.Fatalf("record summary workspace memory: %v", err)
	}

	result, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		WorkspaceID:            workspaceID,
		SessionID:              sessionID,
		PackMode:               "DETERMINISTIC_FALLBACK",
		SourceWindowDigest:     "digest-memory-pack",
		TokenBudget:            4096,
		MessageCountBefore:     12,
		MessageCountAfter:      4,
		MessageTokensBefore:    3200,
		MessageTokensAfter:     1200,
		TotalInputTokens:       5000,
		TotalOutputTokens:      1800,
		SummaryText:            "[Previous conversation history was truncated due to length. 8 messages were removed.]",
		SummaryWorkspaceMemory: summaryMemory.MemoryID,
	})
	if err != nil {
		t.Fatalf("write memory pack: %v", err)
	}

	if result.Status != "RECORDED" || result.PackSource != "episode_pack" {
		t.Fatalf("unexpected memory pack write result: %+v", result)
	}
	if result.Snapshot.AgentID != agentID || result.Snapshot.EpisodePackID == "" {
		t.Fatalf("expected derived snapshot metadata, got %+v", result.Snapshot)
	}
	if result.Pack.PackID != result.Snapshot.EpisodePackID || result.Pack.CanonicalMemoryID == "" {
		t.Fatalf("expected canonical episode pack projection, got %+v", result.Pack)
	}
}

func TestMemoryPackWriteRejectsMismatchedAgentAndDuplicateSnapshotOwnership(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceA = "ws-memory-pack-a"
		workspaceB = "ws-memory-pack-b"
		agentA     = "agent-memory-pack-a"
		agentB     = "agent-memory-pack-b"
		sessionA   = "sess-memory-pack-a"
		sessionB   = "sess-memory-pack-b"
	)

	for _, workspaceID := range []string{workspaceA, workspaceB} {
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       workspaceID,
			CreatedBy:   "developer",
		}); err != nil {
			t.Fatalf("create workspace %s: %v", workspaceID, err)
		}
	}
	for _, item := range []struct {
		workspaceID string
		agentID     string
	}{
		{workspaceA, agentA},
		{workspaceA, agentB},
		{workspaceB, agentB},
	} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: item.workspaceID,
			AgentID:     item.agentID,
			OwnerUserID: "developer",
			DisplayName: item.agentID,
		}); err != nil {
			t.Fatalf("register agent %+v: %v", item, err)
		}
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionA,
		AgentID:     agentA,
		WorkspaceID: workspaceA,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session A: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionB,
		AgentID:     agentB,
		WorkspaceID: workspaceB,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session B: %v", err)
	}

	first, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		SnapshotID:  "snapshot-memory-pack",
		WorkspaceID: workspaceA,
		SessionID:   sessionA,
	})
	if err != nil {
		t.Fatalf("seed memory pack snapshot: %v", err)
	}
	if first.Snapshot.SnapshotID == "" {
		t.Fatalf("expected canonical snapshot id, got %+v", first.Snapshot)
	}
	if _, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		WorkspaceID: workspaceA,
		SessionID:   sessionA,
		AgentID:     agentB,
	}); err == nil || !strings.Contains(err.Error(), "does not belong to agent_id") {
		t.Fatalf("expected session/agent mismatch rejection, got %v", err)
	}
	if _, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		SnapshotID:  first.Snapshot.SnapshotID,
		WorkspaceID: workspaceB,
		SessionID:   sessionB,
		AgentID:     agentB,
	}); err == nil || !strings.Contains(err.Error(), "already belongs to workspace") {
		t.Fatalf("expected cross-workspace snapshot ownership rejection, got %v", err)
	}
}

func TestMemoryPackWriteReusesExistingSnapshotInWorkspace(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-pack-reuse"
		agentID     = "agent-memory-pack-reuse"
		sessionID   = "sess-memory-pack-reuse"
		snapshotID  = "snapshot-memory-pack-reuse"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Pack Reuse",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Pack Reuse Agent",
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

	first, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		SnapshotID:  snapshotID,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
	})
	if err != nil {
		t.Fatalf("seed memory pack snapshot: %v", err)
	}
	second, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		SnapshotID:  snapshotID,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("reuse memory pack snapshot: %v", err)
	}
	if second.Snapshot.SnapshotID != first.Snapshot.SnapshotID || second.Pack.PackID != first.Pack.PackID {
		t.Fatalf("expected existing snapshot and pack to be reused, got first=%+v second=%+v", first, second)
	}
}

func TestMemoryPackWriteRejectsMissingSummaryWorkspaceMemory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-pack-summary"
		agentID     = "agent-memory-pack-summary"
		sessionID   = "sess-memory-pack-summary"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Pack Summary Validation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Pack Summary Agent",
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

	if _, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		WorkspaceID:            workspaceID,
		SessionID:              sessionID,
		SummaryWorkspaceMemory: "mem-missing",
	}); err == nil || !strings.Contains(err.Error(), "summary_workspace_memory") {
		t.Fatalf("expected summary workspace memory validation error, got %v", err)
	}
}

func TestMemoryPackWriteReusesDuplicateSnapshotIDAndKeepsSingleAliasProjection(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-pack-duplicate"
		agentID     = "agent-memory-pack-duplicate"
		sessionID   = "sess-memory-pack-duplicate"
		snapshotID  = "snapshot-memory-pack-duplicate"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Pack Duplicate Snapshot",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Pack Duplicate Agent",
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

	first, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		SnapshotID:         snapshotID,
		WorkspaceID:        workspaceID,
		SessionID:          sessionID,
		PackMode:           "COMPLETE",
		SourceWindowDigest: "digest-duplicate-snapshot",
		SummaryText:        "First canonical compaction snapshot.",
	})
	if err != nil {
		t.Fatalf("first write memory pack: %v", err)
	}
	second, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		SnapshotID:         snapshotID,
		WorkspaceID:        workspaceID,
		SessionID:          sessionID,
		PackMode:           "COMPLETE",
		SourceWindowDigest: "digest-duplicate-snapshot",
		SummaryText:        "First canonical compaction snapshot.",
	})
	if err != nil {
		t.Fatalf("expected same-workspace duplicate snapshot reuse, got %v", err)
	}
	if second.Snapshot.SnapshotID != first.Snapshot.SnapshotID || second.Pack.PackID != first.Pack.PackID {
		t.Fatalf("expected same-workspace duplicate snapshot to reuse canonical objects, got first=%+v second=%+v", first, second)
	}
	if second.Pack.PackMode != first.Pack.PackMode || second.Pack.SourceWindowDigest != first.Pack.SourceWindowDigest {
		t.Fatalf("expected duplicate snapshot reuse to preserve original pack payload, got first=%+v second=%+v", first.Pack, second.Pack)
	}

	snapshots, err := store.ListSessionCompactionSnapshots(ctx, sqlite.SessionCompactionSnapshotFilter{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list compaction snapshots: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected one canonical snapshot after duplicate reuse, got %+v", snapshots)
	}
	if snapshots[0].SnapshotID != snapshotID || snapshots[0].EpisodePackID != first.Pack.PackID || snapshots[0].PackMode != "COMPLETE" {
		t.Fatalf("unexpected canonical snapshot after duplicate reuse: %+v", snapshots[0])
	}

	packs, err := store.ListEpisodePacks(ctx, sqlite.EpisodePackFilter{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list episode packs: %v", err)
	}
	if len(packs) != 1 {
		t.Fatalf("expected one derived episode pack after duplicate reuse, got %+v", packs)
	}
	if packs[0].CompactionSnapshotID != snapshotID || packs[0].PackID != first.Pack.PackID {
		t.Fatalf("unexpected derived episode pack after duplicate reuse: %+v", packs[0])
	}
}

func TestMemoryPackWriteRejectsReplayPayloadMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-pack-replay-mismatch"
		agentID     = "agent-memory-pack-replay-mismatch"
		sessionID   = "sess-memory-pack-replay-mismatch"
		snapshotID  = "snapshot-memory-pack-replay-mismatch"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Pack Replay Mismatch",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Pack Replay Mismatch Agent",
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

	if _, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		SnapshotID:         snapshotID,
		WorkspaceID:        workspaceID,
		SessionID:          sessionID,
		PackMode:           "COMPLETE",
		SourceWindowDigest: "digest-replay-mismatch",
	}); err != nil {
		t.Fatalf("seed memory pack snapshot: %v", err)
	}
	if _, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		SnapshotID:         snapshotID,
		WorkspaceID:        workspaceID,
		SessionID:          sessionID,
		PackMode:           "DETERMINISTIC_FALLBACK",
		SourceWindowDigest: "digest-replay-mismatch-second",
	}); err == nil || !strings.Contains(err.Error(), "snapshot_id replay payload does not match existing snapshot") {
		t.Fatalf("expected replay payload mismatch rejection, got %v", err)
	}
}

func TestMemoryPackWriteRejectsInvalidTriggerKind(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-pack-trigger"
		agentID     = "agent-memory-pack-trigger"
		sessionID   = "sess-memory-pack-trigger"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Pack Trigger Validation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Pack Trigger Agent",
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

	if _, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		TriggerKind: "policy_override",
	}); err == nil || !strings.Contains(err.Error(), "trigger_kind must be one of") {
		t.Fatalf("expected trigger_kind validation error, got %v", err)
	}
}

func TestMemoryPackWriteRejectsForeignSummaryWorkspaceMemory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceA = "ws-memory-pack-summary-a"
		workspaceB = "ws-memory-pack-summary-b"
		agentA     = "agent-memory-pack-summary-a"
		agentB     = "agent-memory-pack-summary-b"
		sessionA   = "sess-memory-pack-summary-a"
		sessionB   = "sess-memory-pack-summary-b"
	)

	for _, workspaceID := range []string{workspaceA, workspaceB} {
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       workspaceID,
			CreatedBy:   "developer",
		}); err != nil {
			t.Fatalf("create workspace %s: %v", workspaceID, err)
		}
	}
	for _, item := range []struct {
		workspaceID string
		agentID     string
		sessionID   string
	}{
		{workspaceA, agentA, sessionA},
		{workspaceB, agentB, sessionB},
	} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: item.workspaceID,
			AgentID:     item.agentID,
			OwnerUserID: "developer",
			DisplayName: item.agentID,
		}); err != nil {
			t.Fatalf("register agent %+v: %v", item, err)
		}
		if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
			SessionID:   item.sessionID,
			AgentID:     item.agentID,
			WorkspaceID: item.workspaceID,
			StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("create session %+v: %v", item, err)
		}
	}
	foreignSummary, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceB,
		MemoryType:  "SUMMARY",
		Title:       "Foreign summary",
		Body:        "Should not be accepted from another workspace.",
		AgentID:     agentB,
		SessionID:   sessionB,
		SourceKind:  "compaction",
		SourceID:    sessionB,
	})
	if err != nil {
		t.Fatalf("record foreign summary workspace memory: %v", err)
	}

	if _, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		WorkspaceID:            workspaceA,
		SessionID:              sessionA,
		SummaryWorkspaceMemory: foreignSummary.MemoryID,
	}); err == nil || !strings.Contains(err.Error(), "summary_workspace_memory must reference an existing workspace memory in workspace_id") {
		t.Fatalf("expected foreign summary workspace memory validation error, got %v", err)
	}
}

func TestMemoryPackWriteRejectsArchivedSummaryWorkspaceMemory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-pack-summary-archived"
		agentID     = "agent-memory-pack-summary-archived"
		sessionID   = "sess-memory-pack-summary-archived"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Pack Archived Summary Validation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Pack Archived Summary Agent",
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
	summaryMemory, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "SUMMARY",
		Title:       "Archived compaction summary",
		Body:        "Old compaction summary.",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "compaction",
		SourceID:    sessionID,
	})
	if err != nil {
		t.Fatalf("record summary workspace memory: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    summaryMemory.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "stale summary",
	}); err != nil {
		t.Fatalf("archive summary workspace memory: %v", err)
	}

	if _, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		WorkspaceID:            workspaceID,
		SessionID:              sessionID,
		SummaryWorkspaceMemory: summaryMemory.MemoryID,
	}); err == nil || !strings.Contains(err.Error(), "summary_workspace_memory is archived") {
		t.Fatalf("expected archived summary workspace memory rejection, got %v", err)
	}
}

func TestMemoryPackWriteRejectsNonCompactionSummaryWorkspaceMemory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-pack-summary-manual"
		agentID     = "agent-memory-pack-summary-manual"
		sessionID   = "sess-memory-pack-summary-manual"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Pack Manual Summary Validation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Pack Manual Summary Agent",
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
	summaryMemory, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "SUMMARY",
		Title:       "Manual summary",
		Body:        "Not a compaction summary.",
		AgentID:     agentID,
		SessionID:   sessionID,
	})
	if err != nil {
		t.Fatalf("record manual summary workspace memory: %v", err)
	}

	if _, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		WorkspaceID:            workspaceID,
		SessionID:              sessionID,
		SummaryWorkspaceMemory: summaryMemory.MemoryID,
	}); err == nil || !strings.Contains(err.Error(), "summary_workspace_memory must reference compaction workspace memory") {
		t.Fatalf("expected non-compaction summary workspace memory rejection, got %v", err)
	}
}

func TestMemoryPackWriteRejectsSnapshotIDCollisionWithExistingEpisodePack(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-pack-collision"
		agentID     = "agent-memory-pack-collision"
		sessionID   = "sess-memory-pack-collision"
		snapshotID  = "snapshot-pack-collision"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Pack Collision",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Pack Collision Agent",
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

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO episode_packs(
		pack_id, pack_key, workspace_id, pack_type, session_id, lineage_session_id, agent_id, trigger_kind
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshotID,
		"lifecycle:"+snapshotID,
		workspaceID,
		"SESSION_END",
		sessionID,
		sessionID,
		agentID,
		"status",
	); err != nil {
		t.Fatalf("seed conflicting episode pack: %v", err)
	}

	if _, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		SnapshotID:  snapshotID,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
	}); err == nil || !strings.Contains(err.Error(), "snapshot_id collides with canonical episode pack") {
		t.Fatalf("expected episode-pack collision rejection, got %v", err)
	}
}

func TestMemoryPackWriteRejectsCompleteModeForFallbackSummaryText(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-pack-fallback"
		agentID     = "agent-memory-pack-fallback"
		sessionID   = "sess-memory-pack-fallback"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Pack Fallback Validation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Pack Fallback Agent",
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

	if _, err := store.WriteMemoryPack(ctx, sqlite.MemoryPackWriteInput{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		PackMode:    "COMPLETE",
		SummaryText: "[Previous conversation history was truncated due to length. 6 messages were removed.]",
	}); err == nil || !strings.Contains(err.Error(), "pack_mode COMPLETE is incompatible with fallback summary_text") {
		t.Fatalf("expected canonical pack_mode rejection, got %v", err)
	}
}
