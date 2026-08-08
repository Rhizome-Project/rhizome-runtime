package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestNewsPollReturnsCompositeCursorForSameTimestampItems(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-news-poll"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a")
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	firstRaw, err := json.Marshal(newsPublishParams{
		WorkspaceID: workspaceID,
		Title:       "First",
		Content:     "One",
		AuthorID:    "agent-a",
		AuthorType:  "agent",
	})
	if err != nil {
		t.Fatalf("marshal first publish: %v", err)
	}
	firstAny, rpcErr := h.newsPublish(ctx, firstRaw)
	if rpcErr != nil {
		t.Fatalf("newsPublish(first) rpc error: %+v", rpcErr)
	}
	first, ok := firstAny.(*sqlite.NewsRecord)
	if !ok {
		t.Fatalf("unexpected first result type %T", firstAny)
	}

	secondRaw, err := json.Marshal(newsPublishParams{
		WorkspaceID: workspaceID,
		Title:       "Second",
		Content:     "Two",
		AuthorID:    "agent-a",
		AuthorType:  "agent",
	})
	if err != nil {
		t.Fatalf("marshal second publish: %v", err)
	}
	secondAny, rpcErr := h.newsPublish(ctx, secondRaw)
	if rpcErr != nil {
		t.Fatalf("newsPublish(second) rpc error: %+v", rpcErr)
	}
	second, ok := secondAny.(*sqlite.NewsRecord)
	if !ok {
		t.Fatalf("unexpected second result type %T", secondAny)
	}

	sameTimestamp := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE news SET created_at = ?, updated_at = ? WHERE news_id IN (?, ?)`,
		sameTimestamp, sameTimestamp, first.NewsID, second.NewsID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE news
		 SET news_id = CASE news_id
			WHEN ? THEN 'news-shared-2'
			WHEN ? THEN 'news-shared-10'
		 END
		 WHERE news_id IN (?, ?)`,
		first.NewsID, second.NewsID, first.NewsID, second.NewsID,
	); err != nil {
		t.Fatalf("rewrite news ids: %v", err)
	}

	pollRaw, err := json.Marshal(newsPollParams{
		WorkspaceID:   workspaceID,
		Limit:         1,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal first poll: %v", err)
	}
	result, rpcErr := h.newsPoll(ctx, pollRaw)
	if rpcErr != nil {
		t.Fatalf("newsPoll(first) rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected first poll type %T", result)
	}
	items, ok := payload["items"].([]sqlite.NewsRecord)
	if !ok {
		t.Fatalf("unexpected first items type %T", payload["items"])
	}
	if len(items) != 1 || items[0].NewsID != "news-shared-2" {
		t.Fatalf("expected first rowid-ordered item, got %+v", items)
	}
	nextCreatedAt, _ := payload["next_cursor_created_at"].(string)
	nextNewsID, _ := payload["next_cursor_news_id"].(string)
	if nextCreatedAt != sameTimestamp || nextNewsID != "news-shared-2" {
		t.Fatalf("unexpected next cursor created_at=%q news_id=%q", nextCreatedAt, nextNewsID)
	}

	pollRaw, err = json.Marshal(newsPollParams{
		WorkspaceID:    workspaceID,
		AfterCreatedAt: nextCreatedAt,
		AfterNewsID:    nextNewsID,
		Limit:          10,
		LookbackHours:  24,
	})
	if err != nil {
		t.Fatalf("marshal second poll: %v", err)
	}
	result, rpcErr = h.newsPoll(ctx, pollRaw)
	if rpcErr != nil {
		t.Fatalf("newsPoll(second) rpc error: %+v", rpcErr)
	}
	payload, ok = result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second poll type %T", result)
	}
	items, ok = payload["items"].([]sqlite.NewsRecord)
	if !ok {
		t.Fatalf("unexpected second items type %T", payload["items"])
	}
	if len(items) != 1 || items[0].NewsID != "news-shared-10" {
		t.Fatalf("expected cursor follower, got %+v", items)
	}
}

func TestNewsPublishBroadcastIncludesNewsMetadata(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-news-broadcast"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a", "agent-b")
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	timeoutSec := 0

	raw, err := json.Marshal(newsPublishParams{
		WorkspaceID: workspaceID,
		Title:       "Bridge upgrade",
		Content:     "The listener now reacts to system news.",
		AuthorID:    "agent-a",
		AuthorType:  "agent",
	})
	if err != nil {
		t.Fatalf("marshal publish: %v", err)
	}
	if _, rpcErr := h.newsPublish(ctx, raw); rpcErr != nil {
		t.Fatalf("newsPublish() rpc error: %+v", rpcErr)
	}

	pollRaw, err := json.Marshal(agentMessagePollParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-b",
		Limit:         10,
		TimeoutSec:    &timeoutSec,
		LookbackHours: 24,
	})
	if err != nil {
		t.Fatalf("marshal message poll: %v", err)
	}

	ctx = context.WithValue(context.Background(), authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   workspaceID,
		PrincipalType: "agent",
		PrincipalID:   "agent-b",
	})
	result, rpcErr := h.agentMessagePoll(ctx, pollRaw)
	if rpcErr != nil {
		t.Fatalf("agentMessagePoll() rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected poll result type %T", result)
	}
	messages, ok := payload["messages"].([]agentPollMessage)
	if !ok {
		t.Fatalf("unexpected messages type %T", payload["messages"])
	}
	if len(messages) != 1 {
		t.Fatalf("expected one broadcast message, got %+v", messages)
	}
	if messages[0].Channel != "news" {
		t.Fatalf("expected news channel, got %+v", messages[0])
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(messages[0].MetadataJSON), &metadata); err != nil {
		t.Fatalf("decode news metadata: %v", err)
	}
	if metadata["news_id"] == "" || metadata["title"] != "Bridge upgrade" {
		t.Fatalf("unexpected metadata: %+v", metadata)
	}
}
