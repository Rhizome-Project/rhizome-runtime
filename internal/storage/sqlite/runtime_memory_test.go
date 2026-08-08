package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryLifecycleSearchAndSnapshot(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory",
		Title:       "Runtime Memory",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-memory",
		AgentID:     "agent-memory",
		OwnerUserID: "developer",
		DisplayName: "Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	createSingleNodeTask(t, ctx, store, "task-memory", "node-memory")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-memory",
		TaskID:      "task-memory",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-memory",
		AgentID:     "agent-memory",
		WorkspaceID: "ws-memory",
		TaskID:      "task-memory",
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory",
		MemoryType:  "lesson",
		Title:       "Bridge wake lessons",
		Body:        "Wake path must ignore duplicate delivery ids after workspace reset.",
		Summary:     "Deduping needs delivery id parity after reset.",
		AgentID:     "agent-memory",
		SessionID:   "sess-memory",
		TaskID:      "task-memory",
		SourceKind:  "compaction",
		SourceID:    "sess-memory",
		Tags:        []string{"transport", "dedupe"},
		Importance:  0.9,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if record.MemoryType != "LESSON" {
		t.Fatalf("expected normalized memory type LESSON, got %+v", record)
	}

	items, err := store.SearchWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: "ws-memory",
		Query:       "delivery ids reset",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search workspace memory: %v", err)
	}
	if len(items) != 1 || items[0].MemoryID != record.MemoryID {
		t.Fatalf("expected stored memory in search results, got %+v", items)
	}

	results, err := store.SearchWorkspace(ctx, sqlite.WorkspaceSearchFilter{
		WorkspaceID: "ws-memory",
		Query:       "delivery reset duplicate",
		EntityType:  "memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search workspace: %v", err)
	}
	if len(results) != 1 || results[0].EntityType != "memory" {
		t.Fatalf("expected memory workspace search result, got %+v", results)
	}

	snapshot, err := store.GetWorkspaceSnapshot(ctx, "ws-memory", 10)
	if err != nil {
		t.Fatalf("get workspace snapshot: %v", err)
	}
	if len(snapshot.RecentMemory) != 1 || snapshot.RecentMemory[0].MemoryID != record.MemoryID {
		t.Fatalf("expected recent memory in snapshot, got %+v", snapshot.RecentMemory)
	}
}

func TestRecordWorkspaceMemoryRebuildsCorruptFTSIndex(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-fts-repair",
		Title:       "Runtime Memory FTS Repair",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-memory-fts-repair",
		AgentID:     "agent-memory",
		OwnerUserID: "developer",
		DisplayName: "Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-fts-repair",
		MemoryType:  "lesson",
		Title:       "First memory",
		Body:        "Seed the FTS index before corruption.",
		AgentID:     "agent-memory",
		SourceKind:  "reflection",
		SourceID:    "agent-memory",
	}); err != nil {
		t.Fatalf("record seed workspace memory: %v", err)
	}

	result, err := store.WriteDB().ExecContext(ctx, `
UPDATE workspace_memory_fts_data
   SET block = zeroblob(length(block))
 WHERE id = (SELECT id FROM workspace_memory_fts_data ORDER BY id LIMIT 1)`)
	if err != nil {
		t.Skipf("cannot corrupt workspace_memory_fts_data in this sqlite build: %v", err)
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		t.Skip("workspace_memory_fts_data had no rows to corrupt")
	}

	repaired, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-fts-repair",
		MemoryType:  "lesson",
		Title:       "Second memory after FTS repair",
		Body:        "The write path should rebuild the workspace memory FTS index and retry once.",
		AgentID:     "agent-memory",
		SourceKind:  "reflection",
		SourceID:    "agent-memory",
	})
	if err != nil {
		t.Fatalf("record workspace memory after FTS corruption: %v", err)
	}
	items, err := store.SearchWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: "ws-memory-fts-repair",
		Query:       "rebuild retry once",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search workspace memory after FTS repair: %v", err)
	}
	if len(items) == 0 || items[0].MemoryID != repaired.MemoryID {
		t.Fatalf("expected repaired memory to be searchable, repaired=%s items=%+v", repaired.MemoryID, items)
	}
}

func TestWorkspaceMemorySearchPrioritizesDurableTypedRecords(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-priority",
		Title:       "Runtime Memory Priority",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-memory-priority",
		AgentID:     "agent-memory",
		OwnerUserID: "developer",
		DisplayName: "Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	commonBody := "Use the live doctor gate to verify rollout health before cutover."
	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-priority",
		MemoryType:  "summary",
		Title:       "Deploy status summary",
		Body:        commonBody,
		AgentID:     "agent-memory",
		SourceKind:  "compaction",
		SourceID:    "sess-1",
		Importance:  1.0,
	}); err != nil {
		t.Fatalf("record summary workspace memory: %v", err)
	}
	decision, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-priority",
		MemoryType:  "decision",
		Title:       "Deploy gate decision",
		Body:        commonBody,
		AgentID:     "agent-memory",
		SourceKind:  "reflection",
		SourceID:    "agent-memory",
		Importance:  0.2,
	})
	if err != nil {
		t.Fatalf("record decision workspace memory: %v", err)
	}

	items, err := store.SearchWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: "ws-memory-priority",
		Query:       "live doctor gate rollout health cutover",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search workspace memory: %v", err)
	}
	if len(items) < 2 {
		t.Fatalf("expected both priority records in search results, got %+v", items)
	}
	if items[0].MemoryID != decision.MemoryID || items[0].MemoryType != "DECISION" {
		t.Fatalf("expected decision to outrank summary for the same query, got %+v", items)
	}
}

func TestWorkspaceMemorySearchTreatsAntiProcedureAsDurableTypedPriority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-anti-procedure-priority",
		Title:       "Runtime Memory Anti Procedure Priority",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-memory-anti-procedure-priority",
		AgentID:     "agent-memory",
		OwnerUserID: "developer",
		DisplayName: "Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	commonBody := "Never bypass the live doctor gate or rollback checks during degraded rollout telemetry."
	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-anti-procedure-priority",
		MemoryType:  "summary",
		Title:       "Rollback status summary",
		Body:        commonBody,
		AgentID:     "agent-memory",
		SourceKind:  "compaction",
		SourceID:    "sess-1",
		Importance:  1.0,
	}); err != nil {
		t.Fatalf("record summary workspace memory: %v", err)
	}
	antiProcedure, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-anti-procedure-priority",
		MemoryType:  "anti_procedure",
		Title:       "Rollback bypass stays forbidden",
		Body:        commonBody,
		AgentID:     "agent-memory",
		SourceKind:  "reflection",
		SourceID:    "agent-memory",
		Importance:  0.2,
	})
	if err != nil {
		t.Fatalf("record anti procedure workspace memory: %v", err)
	}

	items, err := store.SearchWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: "ws-memory-anti-procedure-priority",
		Query:       "bypass live doctor gate rollback checks degraded rollout telemetry",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search workspace memory: %v", err)
	}
	if len(items) < 2 {
		t.Fatalf("expected both priority records in search results, got %+v", items)
	}
	if items[0].MemoryID != antiProcedure.MemoryID || items[0].MemoryType != "ANTI_PROCEDURE" {
		t.Fatalf("expected anti procedure to outrank summary for the same query, got %+v", items)
	}
}

func TestWorkspaceMemoryDedupesCanonicalDuplicates(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-dedupe",
		Title:       "Runtime Memory Dedupe",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-memory-dedupe",
		AgentID:     "agent-memory",
		OwnerUserID: "developer",
		DisplayName: "Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	first, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-dedupe",
		MemoryType:  "procedure",
		Title:       "Deploy gate",
		Body:        "Run doctor with fail-on-warn after rollout.",
		AgentID:     "agent-memory",
		SourceKind:  "reflection",
		SourceID:    "agent-memory",
		Tags:        []string{"deploy"},
		Importance:  0.3,
	})
	if err != nil {
		t.Fatalf("record first workspace memory: %v", err)
	}
	second, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-dedupe",
		MemoryType:  "procedure",
		Title:       "Deploy gate",
		Body:        "Run doctor with fail-on-warn after rollout.",
		Summary:     "Re-assert the deploy gate",
		AgentID:     "agent-memory",
		SourceKind:  "reflection",
		SourceID:    "agent-memory",
		Tags:        []string{"deploy", "gate"},
		Importance:  0.9,
	})
	if err != nil {
		t.Fatalf("record duplicate workspace memory: %v", err)
	}
	if first.MemoryID != second.MemoryID {
		t.Fatalf("expected duplicate writes to reuse memory_id, got %q vs %q", first.MemoryID, second.MemoryID)
	}

	items, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: "ws-memory-dedupe",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace memory: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 deduped memory record, got %+v", items)
	}
	if items[0].Importance != 0.9 || items[0].Summary != "Re-assert the deploy gate" {
		t.Fatalf("expected latest duplicate write to update fields, got %+v", items[0])
	}
}

func TestWorkspaceMemoryArchiveHidesFromActiveRetrieval(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-archive",
		Title:       "Runtime Memory Archive",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-memory-archive",
		AgentID:     "agent-memory",
		OwnerUserID: "developer",
		DisplayName: "Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-archive",
		MemoryType:  "decision",
		Title:       "Use live doctor gate",
		Body:        "Treat live health drift as a deploy blocker.",
		AgentID:     "agent-memory",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	archived, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: "ws-memory-archive",
		MemoryID:    record.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "superseded",
	})
	if err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}
	if archived.ArchivedAt == nil || archived.ArchivedBy == nil || *archived.ArchivedBy != "developer" || archived.ArchivedReason != "superseded" {
		t.Fatalf("expected archive tombstone fields, got %+v", archived)
	}

	items, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: "ws-memory-archive",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list active workspace memory: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected archived memory to be hidden from active list, got %+v", items)
	}

	archivedItems, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID:     "ws-memory-archive",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("list archived workspace memory: %v", err)
	}
	if len(archivedItems) != 1 || archivedItems[0].MemoryID != record.MemoryID {
		t.Fatalf("expected archived memory in include-archived listing, got %+v", archivedItems)
	}

	searchItems, err := store.SearchWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: "ws-memory-archive",
		Query:       "deploy blocker drift",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search active workspace memory: %v", err)
	}
	if len(searchItems) != 0 {
		t.Fatalf("expected archived memory to be hidden from active search, got %+v", searchItems)
	}

	snapshot, err := store.GetWorkspaceSnapshot(ctx, "ws-memory-archive", 10)
	if err != nil {
		t.Fatalf("get workspace snapshot: %v", err)
	}
	if len(snapshot.RecentMemory) != 0 {
		t.Fatalf("expected archived memory to be hidden from workspace snapshot, got %+v", snapshot.RecentMemory)
	}
}

func TestWorkspaceMemoryLifecycleAppendsRuntimeEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-runtime-events",
		Title:       "Workspace Memory Runtime Events",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-memory-runtime-events",
		AgentID:     "agent-memory-runtime",
		OwnerUserID: "developer",
		DisplayName: "Memory Runtime Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, "task-memory-runtime", "node-memory-runtime")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-memory-runtime-events",
		TaskID:      "task-memory-runtime",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-memory-runtime",
		AgentID:     "agent-memory-runtime",
		WorkspaceID: "ws-memory-runtime-events",
		TaskID:      "task-memory-runtime",
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-runtime-events",
		MemoryType:  "decision",
		Title:       "Deploy gate required",
		Body:        "Treat live doctor drift as a blocker.",
		AgentID:     "agent-memory-runtime",
		SessionID:   "sess-memory-runtime",
		TaskID:      "task-memory-runtime",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	recordedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-memory-runtime-events",
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    record.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list recorded runtime events: %v", err)
	}
	if len(recordedEvents) != 1 {
		t.Fatalf("expected 1 recorded runtime event, got %+v", recordedEvents)
	}
	if recordedEvents[0].AgentID != "agent-memory-runtime" || recordedEvents[0].ActorID != "agent-memory-runtime" {
		t.Fatalf("expected recorded event agent/actor ids, got %+v", recordedEvents[0])
	}

	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: "ws-memory-runtime-events",
		MemoryID:    record.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "superseded",
	}); err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}
	archivedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-memory-runtime-events",
		EventType:   "workspace_memory.archived",
		EntityType:  "workspace_memory",
		EntityID:    record.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list archived runtime events: %v", err)
	}
	if len(archivedEvents) != 1 || archivedEvents[0].ActorID != "developer" {
		t.Fatalf("expected archived runtime event by developer, got %+v", archivedEvents)
	}

	if _, err := store.RestoreWorkspaceMemory(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID: "ws-memory-runtime-events",
		MemoryID:    record.MemoryID,
		RestoredBy:  "developer",
	}); err != nil {
		t.Fatalf("restore workspace memory: %v", err)
	}
	restoredEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-memory-runtime-events",
		EventType:   "workspace_memory.restored",
		EntityType:  "workspace_memory",
		EntityID:    record.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list restored runtime events: %v", err)
	}
	if len(restoredEvents) != 1 || restoredEvents[0].ActorID != "developer" {
		t.Fatalf("expected restored runtime event by developer, got %+v", restoredEvents)
	}
}

func TestWorkspaceMemoryArchivedDuplicateCanBeRecordedAgain(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-archive-dedupe",
		Title:       "Runtime Memory Archive Dedupe",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-memory-archive-dedupe",
		AgentID:     "agent-memory",
		OwnerUserID: "developer",
		DisplayName: "Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	first, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-archive-dedupe",
		MemoryType:  "procedure",
		Title:       "Rollout gate",
		Body:        "Run the live doctor gate before declaring deploy success.",
		AgentID:     "agent-memory",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record initial workspace memory: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: "ws-memory-archive-dedupe",
		MemoryID:    first.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "stale",
	}); err != nil {
		t.Fatalf("archive initial workspace memory: %v", err)
	}

	second, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-archive-dedupe",
		MemoryType:  "procedure",
		Title:       "Rollout gate",
		Body:        "Run the live doctor gate before declaring deploy success.",
		AgentID:     "agent-memory",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record replacement workspace memory: %v", err)
	}
	if first.MemoryID == second.MemoryID {
		t.Fatalf("expected archived duplicate to allow a new memory_id, got %q", second.MemoryID)
	}

	items, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: "ws-memory-archive-dedupe",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list active workspace memory: %v", err)
	}
	if len(items) != 1 || items[0].MemoryID != second.MemoryID {
		t.Fatalf("expected only replacement active memory, got %+v", items)
	}
}

func TestWorkspaceMemoryExpiredArchivedDuplicateReactivatesOnEquivalentRecord(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-expired-archive-reactivation",
		Title:       "Runtime Memory Expired Archive Reactivation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	first, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-expired-archive-reactivation",
		MemoryType:  "procedure",
		Title:       "Re-run live doctor after restart",
		Body:        "Equivalent write should reactivate the expired archived memory instead of forking lineage.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
	})
	if err != nil {
		t.Fatalf("record initial workspace memory: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: "ws-memory-expired-archive-reactivation",
		MemoryID:    first.MemoryID,
		ArchivedBy:  "rmp_pruner",
		Reason:      "rmp_gc_expired",
	}); err != nil {
		t.Fatalf("archive initial workspace memory as expired: %v", err)
	}

	second, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-expired-archive-reactivation",
		MemoryType:  "procedure",
		Title:       "Re-run live doctor after restart",
		Body:        "Equivalent write should reactivate the expired archived memory instead of forking lineage.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
	})
	if err != nil {
		t.Fatalf("record equivalent workspace memory after expiry archive: %v", err)
	}
	if second.MemoryID != first.MemoryID {
		t.Fatalf("expected expired archived duplicate to reactivate original memory_id %q, got %+v", first.MemoryID, second)
	}
	if second.ArchivedAt != nil || second.RecoveryReason != "rmp_gc_reactivated" {
		t.Fatalf("expected reactivated memory to clear archive state and keep recovery trace, got %+v", second)
	}

	activeItems, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: "ws-memory-expired-archive-reactivation",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list active workspace memory: %v", err)
	}
	if len(activeItems) != 1 || activeItems[0].MemoryID != first.MemoryID {
		t.Fatalf("expected exactly one reactivated active memory row, got %+v", activeItems)
	}

	restoredEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-memory-expired-archive-reactivation",
		EventType:   "workspace_memory.restored",
		EntityType:  "workspace_memory",
		EntityID:    first.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list restored runtime events: %v", err)
	}
	if len(restoredEvents) != 1 {
		t.Fatalf("expected one restore event for reactivated memory, got %+v", restoredEvents)
	}

	recordedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-memory-expired-archive-reactivation",
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    first.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list recorded runtime events: %v", err)
	}
	if len(recordedEvents) != 2 {
		t.Fatalf("expected initial and reactivation recorded events, got %+v", recordedEvents)
	}
	if !strings.Contains(recordedEvents[0].ParentRefsJSON, restoredEvents[0].EventID) {
		t.Fatalf("expected reactivation recorded event to point at restore event, got %+v vs %+v", recordedEvents[0], restoredEvents[0])
	}
}

func TestWorkspaceMemoryAntiProcedureExpiredArchivedDuplicateReactivatesOnEquivalentRecord(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-anti-procedure-reactivation",
		Title:       "Runtime Memory Anti Procedure Reactivation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	first, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-anti-procedure-reactivation",
		MemoryType:  "anti_procedure",
		Title:       "Rollback bypass stays forbidden",
		Body:        "Equivalent anti-procedure write should reactivate expired archive instead of forking lineage.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
	})
	if err != nil {
		t.Fatalf("record initial anti procedure workspace memory: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: "ws-memory-anti-procedure-reactivation",
		MemoryID:    first.MemoryID,
		ArchivedBy:  "rmp_pruner",
		Reason:      "rmp_gc_expired",
	}); err != nil {
		t.Fatalf("archive initial anti procedure workspace memory as expired: %v", err)
	}

	second, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-anti-procedure-reactivation",
		MemoryType:  "anti_procedure",
		Title:       "Rollback bypass stays forbidden",
		Body:        "Equivalent anti-procedure write should reactivate expired archive instead of forking lineage.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
	})
	if err != nil {
		t.Fatalf("record equivalent anti procedure workspace memory after expiry archive: %v", err)
	}
	if second.MemoryID != first.MemoryID {
		t.Fatalf("expected expired archived anti procedure to reactivate original memory_id %q, got %+v", first.MemoryID, second)
	}
	if second.MemoryType != "ANTI_PROCEDURE" || second.ArchivedAt != nil || second.RecoveryReason != "rmp_gc_reactivated" {
		t.Fatalf("expected reactivated anti procedure to preserve type and recovery trace, got %+v", second)
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: "ws-memory-anti-procedure-reactivation",
		MemoryID:    first.MemoryID,
		ClaimType:   "ANTI_PROCEDURE",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list anti procedure promoted claims after reactivation: %v", err)
	}
	if len(claims) != 1 || claims[0].ClaimType != "ANTI_PROCEDURE" || claims[0].Status != "ACTIVE" {
		t.Fatalf("expected reactivated anti procedure promoted claim, got %+v", claims)
	}

	restoredEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-memory-anti-procedure-reactivation",
		EventType:   "workspace_memory.restored",
		EntityType:  "workspace_memory",
		EntityID:    first.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list anti procedure restored runtime events: %v", err)
	}
	recordedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-memory-anti-procedure-reactivation",
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + first.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list anti procedure promoted claim write events: %v", err)
	}
	if len(restoredEvents) != 1 || len(recordedEvents) != 2 {
		t.Fatalf("expected one restore event and two anti procedure claim write events, got restored=%+v claimWrites=%+v", restoredEvents, recordedEvents)
	}
}

func TestWorkspaceMemoryRestoreReactivatesArchivedRecord(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-restore",
		Title:       "Runtime Memory Restore",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-restore",
		MemoryType:  "decision",
		Title:       "Keep health gate",
		Body:        "Doctor gate remains mandatory after restart.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: "ws-memory-restore",
		MemoryID:    record.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "stale",
	}); err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}

	restored, err := store.RestoreWorkspaceMemory(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID: "ws-memory-restore",
		MemoryID:    record.MemoryID,
		RestoredBy:  "developer",
	})
	if err != nil {
		t.Fatalf("restore workspace memory: %v", err)
	}
	if restored.ArchivedAt != nil || restored.ArchivedBy != nil || restored.ArchivedReason != "" {
		t.Fatalf("expected restored memory to clear archive fields, got %+v", restored)
	}

	activeItems, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID: "ws-memory-restore",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list active workspace memory: %v", err)
	}
	if len(activeItems) != 1 || activeItems[0].MemoryID != record.MemoryID {
		t.Fatalf("expected restored memory to reappear in active listing, got %+v", activeItems)
	}

	idempotent, err := store.RestoreWorkspaceMemory(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID: "ws-memory-restore",
		MemoryID:    record.MemoryID,
		RestoredBy:  "developer",
	})
	if err != nil {
		t.Fatalf("idempotent restore workspace memory: %v", err)
	}
	if idempotent.ArchivedAt != nil {
		t.Fatalf("expected idempotent restore to remain active, got %+v", idempotent)
	}
}

func TestWorkspaceMemoryRestoreRejectsActiveDuplicateConflict(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-restore-conflict",
		Title:       "Runtime Memory Restore Conflict",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	original, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-restore-conflict",
		MemoryType:  "procedure",
		Title:       "Rollout gate",
		Body:        "Run doctor after restart.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
	})
	if err != nil {
		t.Fatalf("record original workspace memory: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: "ws-memory-restore-conflict",
		MemoryID:    original.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "stale",
	}); err != nil {
		t.Fatalf("archive original workspace memory: %v", err)
	}
	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-restore-conflict",
		MemoryType:  "procedure",
		Title:       "Rollout gate",
		Body:        "Run doctor after restart.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
	}); err != nil {
		t.Fatalf("record replacement workspace memory: %v", err)
	}

	if _, err := store.RestoreWorkspaceMemory(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID: "ws-memory-restore-conflict",
		MemoryID:    original.MemoryID,
		RestoredBy:  "developer",
	}); err == nil {
		t.Fatalf("expected restore conflict when active duplicate exists")
	}
}

func TestWorkspaceMemoryPromotesTypedClaimsAcrossLifecycle(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-memory-claims",
		Title:       "Runtime Memory Claims",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-memory-claims",
		AgentID:     "agent-memory-claims",
		OwnerUserID: "developer",
		DisplayName: "Memory Claims Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: "ws-memory-claims",
		MemoryType:  "decision",
		Title:       "Trust runtime truth",
		Body:        "Use the current runtime state as the canonical operational truth.",
		Summary:     "Trust runtime truth over archived traces.",
		AgentID:     "agent-memory-claims",
		SourceKind:  "manual",
		SourceID:    "dashboard",
		Tags:        []string{"runtime", "truth"},
		Confidence:  0.92,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: "ws-memory-claims",
		MemoryID:    record.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted knowledge claims: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("expected one promoted claim, got %+v", claims)
	}
	if claims[0].ClaimID != "claim:memory:"+record.MemoryID || claims[0].ClaimType != "DECISION" || claims[0].Status != "ACTIVE" {
		t.Fatalf("unexpected promoted claim %+v", claims[0])
	}

	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: "ws-memory-claims",
		MemoryID:    record.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "no longer current",
	}); err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}

	activeClaims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: "ws-memory-claims",
		MemoryID:    record.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list active promoted claims after archive: %v", err)
	}
	if len(activeClaims) != 0 {
		t.Fatalf("expected archived memory claim to disappear from active listing, got %+v", activeClaims)
	}

	allClaims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID:     "ws-memory-claims",
		MemoryID:        record.MemoryID,
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("list archived promoted claims: %v", err)
	}
	if len(allClaims) != 1 || allClaims[0].ArchivedAt == nil || allClaims[0].Status != "ARCHIVED" {
		t.Fatalf("expected archived promoted claim metadata, got %+v", allClaims)
	}

	if _, err := store.RestoreWorkspaceMemory(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID: "ws-memory-claims",
		MemoryID:    record.MemoryID,
		RestoredBy:  "developer",
	}); err != nil {
		t.Fatalf("restore workspace memory: %v", err)
	}

	restoredClaims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: "ws-memory-claims",
		MemoryID:    record.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list restored promoted claims: %v", err)
	}
	if len(restoredClaims) != 1 || restoredClaims[0].Status != "ACTIVE" || restoredClaims[0].ArchivedAt != nil {
		t.Fatalf("expected restored promoted claim to reactivate, got %+v", restoredClaims)
	}
}

func TestWorkspaceMemoryAlternativeBranchPromotesRecoverableClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-alt-branch"
		agentID     = "agent-memory-alt-branch"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Memory Alternative Branch",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Alternative Branch Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "alternative_branch",
		Title:       "Delayed rollout branch",
		Body:        "Preserve a delayed rollout path as a recoverable contrastive branch.",
		Summary:     "Recoverable rollout branch.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "dashboard",
		Tags:        []string{"branch", "contrastive"},
		Confidence:  0.74,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted knowledge claims: %v", err)
	}
	if len(claims) != 1 || claims[0].ClaimType != "ALTERNATIVE_BRANCH" || claims[0].Status != "ACTIVE" {
		t.Fatalf("expected alternative branch promoted claim, got %+v", claims)
	}

	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "rmp_pruner",
		Reason:      "rmp_gc_expired",
	}); err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}

	archivedClaims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID:     workspaceID,
		MemoryID:        record.MemoryID,
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("list archived promoted claims: %v", err)
	}
	if len(archivedClaims) != 1 || archivedClaims[0].Status != "ARCHIVED" || archivedClaims[0].ArchivedAt == nil || archivedClaims[0].LifecycleReason != "rmp_gc_expired" {
		t.Fatalf("expected archived alternative branch promoted claim, got %+v", archivedClaims)
	}
}

func TestWorkspaceMemoryConvenienceLifecycleAppendsPromotedClaimRuntimeEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-claim-effects"
		agentID     = "agent-memory-claim-effects"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Memory Claim Effects",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Claim Effects Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Promoted claim convenience lifecycle",
		Body:        "Plain workspace-memory helpers should still append promoted-claim runtime side effects.",
		Summary:     "Convenience wrappers must keep promoted claim effects.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "dashboard",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	claimID := "claim:memory:" + record.MemoryID

	claimWrittenEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted claim write events after record: %v", err)
	}
	if len(claimWrittenEvents) != 1 {
		t.Fatalf("expected one promoted claim written runtime event after record, got %+v", claimWrittenEvents)
	}

	claim, err := store.GetKnowledgeClaim(ctx, workspaceID, claimID)
	if err != nil {
		t.Fatalf("get promoted knowledge claim: %v", err)
	}
	claim, err = store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ActorID:     agentID,
		Reason:      "seed review queue before convenience archive",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-memory-effects",
	})
	if err != nil {
		t.Fatalf("request review for promoted claim: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-memory-claim-effects",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:memory-claim-effects",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "knowledge_claim", RefID: claimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed promoted claim residency: %v", err)
	}
	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted claim queues: %v", err)
	}
	if len(queues) != 1 {
		t.Fatalf("expected one promoted claim review queue, got %+v", queues)
	}
	queueID := queues[0].QueueID

	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "convenience archive",
	}); err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}

	claimArchivedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted claim archive events: %v", err)
	}
	if len(claimArchivedEvents) != 1 {
		t.Fatalf("expected one promoted claim archived runtime event, got %+v", claimArchivedEvents)
	}
	queueResolvedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted claim queue resolution events: %v", err)
	}
	if len(queueResolvedEvents) != 1 {
		t.Fatalf("expected one promoted claim queue resolution runtime event, got %+v", queueResolvedEvents)
	}
	invalidationEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted claim invalidation events: %v", err)
	}
	if len(invalidationEvents) != 1 {
		t.Fatalf("expected one promoted claim invalidation runtime event, got %+v", invalidationEvents)
	}
	var invalidationPayload map[string]any
	if err := json.Unmarshal([]byte(invalidationEvents[0].PayloadJSON), &invalidationPayload); err != nil {
		t.Fatalf("decode promoted claim invalidation payload: %v", err)
	}
	if invalidationPayload["trigger_cause"] != "knowledge_claim.archived" || invalidationPayload["ref_kind"] != "knowledge_claim" || invalidationPayload["ref_id"] != claimID {
		t.Fatalf("expected promoted claim invalidation payload, got %+v", invalidationPayload)
	}

	preRestoreClaimEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted claim write events before restore: %v", err)
	}
	preRestoreClaimEventIDs := make(map[string]struct{}, len(preRestoreClaimEvents))
	for _, event := range preRestoreClaimEvents {
		preRestoreClaimEventIDs[event.EventID] = struct{}{}
	}

	if _, err := store.RestoreWorkspaceMemory(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		RestoredBy:  "developer",
	}); err != nil {
		t.Fatalf("restore workspace memory: %v", err)
	}

	claimWrittenEvents, err = store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted claim write events after restore: %v", err)
	}
	if len(claimWrittenEvents) != len(preRestoreClaimEventIDs)+1 {
		t.Fatalf("expected restore to append exactly one promoted claim written runtime event, got %+v", claimWrittenEvents)
	}
	if _, seen := preRestoreClaimEventIDs[claimWrittenEvents[0].EventID]; seen {
		t.Fatalf("expected restore to prepend a new promoted claim written runtime event, got %+v", claimWrittenEvents)
	}
}

func TestListSessionCompactionCandidatesUsesCanonicalLedger(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-compaction",
		Title:       "Compaction Candidates",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-compaction",
		AgentID:     "agent-compaction",
		OwnerUserID: "developer",
		DisplayName: "Compaction Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-candidate",
		AgentID:     "agent-compaction",
		WorkspaceID: "ws-compaction",
		StartedAt:   startedAt,
	}); err != nil {
		t.Fatalf("create candidate session: %v", err)
	}
	for i := 0; i < 14; i++ {
		if err := store.AppendAgentSessionMessage(ctx, sqlite.AgentSessionMessageInput{
			SessionID:   "sess-candidate",
			Sequence:    i,
			Role:        "user",
			ContentJSON: `[{"type":"text","text":"hello"}]`,
			TokenCount:  250,
		}); err != nil {
			t.Fatalf("append candidate message %d: %v", i, err)
		}
	}
	if err := store.UpdateAgentSession(ctx, sqlite.AgentSessionUpdateInput{
		SessionID:         "sess-candidate",
		Status:            "RUNNING",
		Iterations:        4,
		TotalInputTokens:  0,
		TotalOutputTokens: 0,
		ToolCalls:         2,
	}); err != nil {
		t.Fatalf("update candidate session: %v", err)
	}

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-small",
		AgentID:     "agent-compaction",
		WorkspaceID: "ws-compaction",
		StartedAt:   startedAt,
	}); err != nil {
		t.Fatalf("create small session: %v", err)
	}
	if err := store.AppendAgentSessionMessage(ctx, sqlite.AgentSessionMessageInput{
		SessionID:   "sess-small",
		Sequence:    0,
		Role:        "user",
		ContentJSON: `[{"type":"text","text":"short"}]`,
		TokenCount:  10,
	}); err != nil {
		t.Fatalf("append small session message: %v", err)
	}
	if err := store.UpdateAgentSession(ctx, sqlite.AgentSessionUpdateInput{
		SessionID:         "sess-small",
		Status:            "COMPLETED",
		Iterations:        1,
		TotalInputTokens:  100,
		TotalOutputTokens: 90,
		CompletedAt:       time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("update small session: %v", err)
	}

	items, err := store.ListSessionCompactionCandidates(ctx, sqlite.SessionCompactionFilter{
		WorkspaceID: "ws-compaction",
		AgentID:     "agent-compaction",
		ActiveOnly:  true,
		MinMessages: 12,
		MinTokens:   3000,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list session compaction candidates: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one candidate, got %+v", items)
	}
	if items[0].SessionID != "sess-candidate" {
		t.Fatalf("expected sess-candidate, got %+v", items[0])
	}
	if items[0].MessageCount != 14 || items[0].MessageTokens != 3500 || items[0].TotalTokens != 0 {
		t.Fatalf("expected canonical message/token totals, got %+v", items[0])
	}
}
