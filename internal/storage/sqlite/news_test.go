package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestListNewsAfterUsesCompositeCursorForSameTimestampItems(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-news-cursor"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "News Cursor",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	first, err := store.CreateNews(ctx, sqlite.NewsCreateInput{
		WorkspaceID: workspaceID,
		Title:       "First",
		Content:     "One",
		AuthorID:    "agent-a",
		AuthorType:  "agent",
	})
	if err != nil {
		t.Fatalf("CreateNews(first): %v", err)
	}
	second, err := store.CreateNews(ctx, sqlite.NewsCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Second",
		Content:     "Two",
		AuthorID:    "agent-a",
		AuthorType:  "agent",
	})
	if err != nil {
		t.Fatalf("CreateNews(second): %v", err)
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

	items, err := store.ListNewsAfter(ctx, workspaceID, "", "", 1, 24)
	if err != nil {
		t.Fatalf("ListNewsAfter(first page): %v", err)
	}
	if len(items) != 1 || items[0].NewsID != "news-shared-2" {
		t.Fatalf("expected first rowid-ordered news item, got %+v", items)
	}

	items, err = store.ListNewsAfter(ctx, workspaceID, sameTimestamp, "news-shared-2", 10, 24)
	if err != nil {
		t.Fatalf("ListNewsAfter(cursor page): %v", err)
	}
	if len(items) != 1 || items[0].NewsID != "news-shared-10" {
		t.Fatalf("expected cursor follower, got %+v", items)
	}
}
