package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryCLIAndSnapshot(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-memory-cli",
		"--title", "Workspace Memory CLI",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-memory-cli")
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-memory-cli",
		"--agent-id", "agent-memory-cli",
		"--owner-user-id", "developer",
		"--display-name", "Memory CLI Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	writeOut, err := captureStdout(t, func() error {
		return runWorkspaceMemory([]string{
			"write",
			"--workspace-id", "ws-memory-cli",
			"--memory-type", "lesson",
			"--title", "Delivery dedupe",
			"--body", "Bridge delivery ids must survive reset to avoid duplicate wake handling.",
			"--summary", "Persist delivery ids across reset.",
			"--agent-id", "agent-memory-cli",
			"--source-kind", "manual",
			"--source-id", "developer",
			"--tags", "transport,dedupe",
			"--importance", "0.8",
			"--confidence", "0.9",
		})
	})
	if err != nil {
		t.Fatalf("runWorkspaceMemory write failed: %v", err)
	}

	var writePayload struct {
		Memory struct {
			MemoryID string `json:"memory_id"`
		} `json:"memory"`
	}
	if err := json.Unmarshal([]byte(writeOut), &writePayload); err != nil {
		t.Fatalf("decode workspace memory write output: %v; output=%q", err, writeOut)
	}
	if writePayload.Memory.MemoryID == "" {
		t.Fatalf("expected memory_id in workspace memory write output, got %q", writeOut)
	}

	searchOut, err := captureStdout(t, func() error {
		return runWorkspaceMemory([]string{
			"search",
			"--workspace-id", "ws-memory-cli",
			"--query", "duplicate wake reset",
		})
	})
	if err != nil {
		t.Fatalf("workspace memory search failed: %v", err)
	}

	var searchPayload struct {
		Count int `json:"count"`
		Items []struct {
			MemoryType string `json:"memory_type"`
			Title      string `json:"title"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(searchOut), &searchPayload); err != nil {
		t.Fatalf("decode workspace memory search output: %v; output=%q", err, searchOut)
	}
	if searchPayload.Count != 1 || len(searchPayload.Items) != 1 {
		t.Fatalf("expected one memory search result, got %+v", searchPayload)
	}
	if searchPayload.Items[0].MemoryType != "LESSON" || searchPayload.Items[0].Title != "Delivery dedupe" {
		t.Fatalf("unexpected memory search item: %+v", searchPayload.Items[0])
	}

	statusOut, err := captureStdout(t, func() error {
		return runWorkspaceStatus([]string{
			"--workspace-id", "ws-memory-cli",
			"--updates-limit", "10",
		})
	})
	if err != nil {
		t.Fatalf("workspace status failed: %v", err)
	}

	var statusPayload struct {
		Snapshot struct {
			RecentMemory []struct {
				MemoryType string `json:"memory_type"`
				Title      string `json:"title"`
			} `json:"recent_memory"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal([]byte(statusOut), &statusPayload); err != nil {
		t.Fatalf("decode workspace status: %v; output=%q", err, statusOut)
	}
	if len(statusPayload.Snapshot.RecentMemory) != 1 {
		t.Fatalf("expected recent memory in snapshot, got %+v", statusPayload.Snapshot.RecentMemory)
	}

	removeOut, err := captureStdout(t, func() error {
		return runWorkspaceMemory([]string{
			"remove",
			"--workspace-id", "ws-memory-cli",
			"--memory-id", writePayload.Memory.MemoryID,
			"--removed-by", "developer",
			"--reason", "stale",
		})
	})
	if err != nil {
		t.Fatalf("workspace memory remove failed: %v", err)
	}

	var removePayload struct {
		Status string `json:"status"`
		Memory struct {
			ArchivedReason string `json:"archived_reason"`
		} `json:"memory"`
	}
	if err := json.Unmarshal([]byte(removeOut), &removePayload); err != nil {
		t.Fatalf("decode workspace memory remove output: %v; output=%q", err, removeOut)
	}
	if removePayload.Status != "ARCHIVED" || removePayload.Memory.ArchivedReason != "stale" {
		t.Fatalf("unexpected workspace memory remove payload: %+v", removePayload)
	}

	activeListOut, err := captureStdout(t, func() error {
		return runWorkspaceMemory([]string{
			"list",
			"--workspace-id", "ws-memory-cli",
		})
	})
	if err != nil {
		t.Fatalf("workspace memory list failed: %v", err)
	}

	var activeListPayload struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal([]byte(activeListOut), &activeListPayload); err != nil {
		t.Fatalf("decode workspace memory list output: %v; output=%q", err, activeListOut)
	}
	if activeListPayload.Count != 0 {
		t.Fatalf("expected archived memory to be hidden from active list, got %+v", activeListPayload)
	}

	archivedListOut, err := captureStdout(t, func() error {
		return runWorkspaceMemory([]string{
			"list",
			"--workspace-id", "ws-memory-cli",
			"--include-archived",
		})
	})
	if err != nil {
		t.Fatalf("workspace memory include-archived list failed: %v", err)
	}

	var archivedListPayload struct {
		Count int `json:"count"`
		Items []struct {
			MemoryID       string `json:"memory_id"`
			ArchivedReason string `json:"archived_reason"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(archivedListOut), &archivedListPayload); err != nil {
		t.Fatalf("decode workspace memory include-archived list output: %v; output=%q", err, archivedListOut)
	}
	if archivedListPayload.Count != 1 || len(archivedListPayload.Items) != 1 {
		t.Fatalf("expected archived memory in include-archived list, got %+v", archivedListPayload)
	}
	if archivedListPayload.Items[0].MemoryID != writePayload.Memory.MemoryID || archivedListPayload.Items[0].ArchivedReason != "stale" {
		t.Fatalf("unexpected archived memory item: %+v", archivedListPayload.Items[0])
	}
}

func TestWorkspaceMemoryWriteCLIRejectsMissingWorkspaceAuthority(t *testing.T) {
	setupFakeBridgeEnv(t)

	const workspaceID = "ws-memory-cli-missing-authority"
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "Workspace Memory CLI Missing Authority",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}

	err := runWorkspaceMemory([]string{
		"write",
		"--workspace-id", workspaceID,
		"--memory-type", "note",
		"--title", "CLI write missing authority",
		"--body", "workspace memory CLI should fail closed before any memory/event side effect",
		"--source-kind", "manual",
		"--source-id", "developer",
	})
	if err == nil {
		t.Fatal("expected workspace memory write CLI to fail without workspace authority")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected authority_missing reject, got %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	var memoryCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_memory WHERE workspace_id = ?`, workspaceID).Scan(&memoryCount); err != nil {
		t.Fatalf("count workspace memory rows: %v", err)
	}
	if memoryCount != 0 {
		t.Fatalf("expected no workspace memory rows after authority reject, got %d", memoryCount)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no workspace_memory.recorded events after authority reject, got %+v", events)
	}
}

func TestWorkspaceCompactionCandidatesCLI(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-compaction-cli",
		"--title", "Workspace Compaction CLI",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-compaction-cli",
		"--agent-id", "agent-compaction-cli",
		"--owner-user-id", "developer",
		"--display-name", "Compaction CLI Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-compaction-cli",
		AgentID:     "agent-compaction-cli",
		WorkspaceID: "ws-compaction-cli",
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	for i := 0; i < 13; i++ {
		if err := store.AppendAgentSessionMessage(ctx, sqlite.AgentSessionMessageInput{
			SessionID:   "sess-compaction-cli",
			Sequence:    i,
			Role:        "user",
			ContentJSON: `[{"type":"text","text":"candidate"}]`,
			TokenCount:  1000,
		}); err != nil {
			t.Fatalf("append session message %d: %v", i, err)
		}
	}
	if err := store.UpdateAgentSession(ctx, sqlite.AgentSessionUpdateInput{
		SessionID:         "sess-compaction-cli",
		Status:            "RUNNING",
		Iterations:        3,
		TotalInputTokens:  8000,
		TotalOutputTokens: 5000,
	}); err != nil {
		t.Fatalf("update agent session: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runWorkspaceCompaction([]string{
			"candidates",
			"--workspace-id", "ws-compaction-cli",
		})
	})
	if err != nil {
		t.Fatalf("workspace compaction candidates failed: %v", err)
	}

	var payload struct {
		Count int `json:"count"`
		Items []struct {
			SessionID    string `json:"session_id"`
			MessageCount int    `json:"message_count"`
			TotalTokens  int    `json:"total_tokens"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode compaction candidates output: %v; output=%q", err, out)
	}
	if payload.Count != 1 || len(payload.Items) != 1 {
		t.Fatalf("expected one compaction candidate, got %+v", payload)
	}
	if payload.Items[0].SessionID != "sess-compaction-cli" || payload.Items[0].MessageCount != 13 || payload.Items[0].TotalTokens != 13000 {
		t.Fatalf("unexpected compaction candidate payload: %+v", payload.Items[0])
	}

	if _, err := store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
		SessionID:           "sess-compaction-cli",
		WorkspaceID:         "ws-compaction-cli",
		AgentID:             "agent-compaction-cli",
		TriggerKind:         "token_budget_exceeded",
		TokenBudget:         12000,
		MessageCountBefore:  13,
		MessageCountAfter:   4,
		MessageTokensBefore: 13000,
		MessageTokensAfter:  900,
		TotalInputTokens:    8000,
		TotalOutputTokens:   5000,
		SummaryText:         "Compacted runtime summary.",
	}); err != nil {
		t.Fatalf("record compaction snapshot: %v", err)
	}

	snapshotOut, err := captureStdout(t, func() error {
		return runWorkspaceCompaction([]string{
			"snapshots",
			"--workspace-id", "ws-compaction-cli",
			"--session-id", "sess-compaction-cli",
		})
	})
	if err != nil {
		t.Fatalf("workspace compaction snapshots failed: %v", err)
	}

	var snapshotPayload struct {
		Count int `json:"count"`
		Items []struct {
			SessionID           string `json:"session_id"`
			MessageTokensBefore int    `json:"message_tokens_before"`
			MessageTokensAfter  int    `json:"message_tokens_after"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(snapshotOut), &snapshotPayload); err != nil {
		t.Fatalf("decode compaction snapshots output: %v; output=%q", err, snapshotOut)
	}
	if snapshotPayload.Count != 1 || len(snapshotPayload.Items) != 1 {
		t.Fatalf("expected one compaction snapshot, got %+v", snapshotPayload)
	}
	if snapshotPayload.Items[0].SessionID != "sess-compaction-cli" || snapshotPayload.Items[0].MessageTokensBefore != 13000 || snapshotPayload.Items[0].MessageTokensAfter != 900 {
		t.Fatalf("unexpected compaction snapshot payload: %+v", snapshotPayload.Items[0])
	}
}
