package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceEpisodePackListAndGet(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-episode-pack"
		agentID     = "agent-handler-episode-pack"
		sessionID   = "sess-handler-episode-pack"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Episode Packs",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Episode Pack Agent",
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
	snapshot, err := store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
		SessionID:           sessionID,
		WorkspaceID:         workspaceID,
		AgentID:             agentID,
		TriggerKind:         "token_budget_exceeded",
		PackMode:            "DETERMINISTIC_FALLBACK",
		SourceWindowDigest:  "digest-handler",
		MessageCountBefore:  6,
		MessageCountAfter:   3,
		MessageTokensBefore: 1200,
		MessageTokensAfter:  480,
		TotalInputTokens:    1800,
		TotalOutputTokens:   500,
		SummaryText:         "[Previous conversation history was truncated due to length. 3 messages were removed.]",
	})
	if err != nil {
		t.Fatalf("record session compaction snapshot: %v", err)
	}

	listRaw, err := json.Marshal(workspaceEpisodePackListParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	result, rpcErr := h.workspaceEpisodePackList(ctx, listRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEpisodePackList rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	items, ok := payload["items"].([]sqlite.EpisodePackRecord)
	if !ok {
		t.Fatalf("unexpected items type %T", payload["items"])
	}
	if len(items) != 1 || items[0].PackID != snapshot.EpisodePackID {
		t.Fatalf("unexpected episode pack list %+v", items)
	}

	getRaw, err := json.Marshal(workspaceEpisodePackGetParams{
		WorkspaceID: workspaceID,
		PackID:      snapshot.EpisodePackID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	getResult, rpcErr := h.workspaceEpisodePackGet(ctx, getRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEpisodePackGet rpc error: %+v", rpcErr)
	}
	record := getResult.(sqlite.EpisodePackRecord)
	if record.PackMode != "DETERMINISTIC_FALLBACK" || record.CanonicalMemoryID == "" {
		t.Fatalf("unexpected episode pack payload %+v", record)
	}
}

func TestWorkspaceEpisodePackSyncBackfillsLegacySnapshot(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-handler-episode-pack-sync"
		agentID     = "agent-handler-episode-pack-sync"
		sessionID   = "sess-handler-episode-pack-sync"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Episode Pack Sync",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Episode Pack Sync Agent",
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
	if _, err := store.DB().ExecContext(
		ctx,
		`INSERT INTO session_compaction_snapshots(
		    snapshot_id, session_id, workspace_id, agent_id, trigger_kind, token_budget,
		    message_count_before, message_count_after, message_tokens_before, message_tokens_after,
		    total_input_tokens, total_output_tokens, summary_text, summary_workspace_memory
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"legacy-handler-snapshot",
		sessionID,
		workspaceID,
		agentID,
		"token_budget_exceeded",
		900,
		5,
		3,
		1000,
		450,
		1500,
		450,
		"Legacy handler compaction summary.",
	); err != nil {
		t.Fatalf("insert legacy compaction snapshot: %v", err)
	}

	syncRaw, err := json.Marshal(workspaceEpisodePackSyncParams{WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("marshal sync params: %v", err)
	}
	result, rpcErr := h.workspaceEpisodePackSync(ctx, syncRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceEpisodePackSync rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	if payload["status"] != "SYNCED" {
		t.Fatalf("unexpected sync payload %+v", payload)
	}

	snapshotResp, rpcErr := h.workspaceCompactionSnapshots(ctx, mustMarshalJSON(t, workspaceCompactionSnapshotsParams{WorkspaceID: workspaceID}))
	if rpcErr != nil {
		t.Fatalf("workspaceCompactionSnapshots rpc error: %+v", rpcErr)
	}
	snapshotPayload := snapshotResp.(map[string]any)
	items, ok := snapshotPayload["items"].([]sqlite.SessionCompactionSnapshotRecord)
	if !ok || len(items) != 1 {
		t.Fatalf("unexpected compaction snapshot payload %+v", snapshotPayload)
	}
	if items[0].EpisodePackID == "" || items[0].CanonicalMemoryID == "" {
		t.Fatalf("expected backfilled episode pack link, got %+v", items[0])
	}
}

func TestWorkspaceEpisodePackListRejectsInvalidPackMode(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	raw, err := json.Marshal(workspaceEpisodePackListParams{
		WorkspaceID: "ws-invalid-pack-mode",
		PackMode:    "NOT_A_MODE",
	})
	if err != nil {
		t.Fatalf("marshal invalid pack mode params: %v", err)
	}
	if _, rpcErr := h.workspaceEpisodePackList(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for invalid pack_mode, got %+v", rpcErr)
	}
}

func mustMarshalJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	return raw
}
