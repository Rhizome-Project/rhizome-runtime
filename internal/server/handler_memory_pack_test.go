package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryPackListAndGetAliasEpisodePacks(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-pack"
		agentID     = "agent-handler-memory-pack"
		sessionID   = "sess-handler-memory-pack"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Packs",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Pack Agent",
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
		SourceWindowDigest:  "digest-handler-pack",
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

	listRaw, err := json.Marshal(workspaceMemoryPackListParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryPackList(ctx, listRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPackList rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	items, ok := payload["items"].([]sqlite.EpisodePackRecord)
	if !ok {
		t.Fatalf("unexpected items type %T", payload["items"])
	}
	if payload["pack_source"] != "episode_pack" || len(items) != 1 || items[0].PackID != snapshot.EpisodePackID {
		t.Fatalf("unexpected memory pack list payload %+v", payload)
	}

	getRaw, err := json.Marshal(workspaceMemoryPackGetParams{
		WorkspaceID: workspaceID,
		PackID:      snapshot.EpisodePackID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	getResult, rpcErr := h.workspaceMemoryPackGet(ctx, getRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPackGet rpc error: %+v", rpcErr)
	}
	getPayload := getResult.(map[string]any)
	record, ok := getPayload["pack"].(sqlite.EpisodePackRecord)
	if !ok {
		t.Fatalf("unexpected pack type %T", getPayload["pack"])
	}
	if record.PackID != snapshot.EpisodePackID || record.CanonicalMemoryID == "" {
		t.Fatalf("unexpected memory pack detail payload %+v", getPayload)
	}
}

func TestWorkspaceMemoryPackListRejectsInvalidPackMode(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	raw, err := json.Marshal(workspaceMemoryPackListParams{
		WorkspaceID: "ws-invalid-memory-pack-mode",
		PackMode:    "NOT_A_MODE",
	})
	if err != nil {
		t.Fatalf("marshal invalid pack mode params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryPackList(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for invalid pack_mode, got %+v", rpcErr)
	}
}
