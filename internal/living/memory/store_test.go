package memory

import (
	"context"
	"testing"
)

func newTestStore(t *testing.T) *MemoryStore {
	t.Helper()
	mdb, err := NewMemoryDB(":memory:")
	if err != nil {
		t.Fatalf("NewMemoryDB: %v", err)
	}
	t.Cleanup(func() { mdb.Close() })
	return NewMemoryStore(mdb)
}

func TestMemoryStore_SaveAndSearch(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entries := []MemoryEntry{
		{Type: TypeExperience, Source: "agent", Topic: "golang", Content: "learned about goroutines and channels"},
		{Type: TypeReflection, Source: "agent", Topic: "python", Content: "python asyncio is different from goroutines"},
		{Type: TypeProcedure, Source: "agent", Topic: "deployment", Content: "deploy the service using kubernetes"},
	}
	for i, e := range entries {
		id, err := store.Save(ctx, e)
		if err != nil {
			t.Fatalf("Save entry %d: %v", i, err)
		}
		if id <= 0 {
			t.Fatalf("expected positive ID, got %d", id)
		}
	}

	results, err := store.Search(ctx, "goroutines", SearchOpts{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one result for 'goroutines'")
	}

	found := false
	for _, r := range results {
		if r.Topic == "golang" {
			found = true
		}
	}
	if !found {
		t.Error("expected to find entry with topic 'golang'")
	}
}

func TestMemoryStore_SearchWithTypeFilter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	store.Save(ctx, MemoryEntry{Type: TypeExperience, Topic: "testing", Content: "wrote unit tests for the database layer"})
	store.Save(ctx, MemoryEntry{Type: TypeReflection, Topic: "testing", Content: "unit tests improve database confidence"})
	store.Save(ctx, MemoryEntry{Type: TypeProcedure, Topic: "ci", Content: "run database tests in CI pipeline"})

	results, err := store.Search(ctx, "database", SearchOpts{TypeFilter: TypeExperience})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	for _, r := range results {
		if r.Type != TypeExperience {
			t.Errorf("expected type %q, got %q", TypeExperience, r.Type)
		}
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result with type filter, got %d", len(results))
	}
}

func TestMemoryStore_SearchBM25Ranking(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Entry with "concurrency" mentioned multiple times should rank higher.
	store.Save(ctx, MemoryEntry{Type: TypeExperience, Topic: "basics", Content: "go is a programming language"})
	store.Save(ctx, MemoryEntry{Type: TypeExperience, Topic: "concurrency", Content: "concurrency in go uses goroutines for concurrency patterns and concurrency models"})
	store.Save(ctx, MemoryEntry{Type: TypeExperience, Topic: "misc", Content: "concurrency is a topic"})

	results, err := store.Search(ctx, "concurrency", SearchOpts{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) < 2 {
		t.Fatalf("expected at least 2 results, got %d", len(results))
	}

	// The entry with more "concurrency" mentions should come first.
	if results[0].Topic != "concurrency" {
		t.Errorf("expected most relevant result first (topic 'concurrency'), got %q", results[0].Topic)
	}
}

func TestMemoryStore_GetRecent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		store.Save(ctx, MemoryEntry{
			Type:    TypeExperience,
			Topic:   "topic",
			Content: "entry content",
		})
	}

	results, err := store.GetRecent(ctx, RecentOpts{Limit: 3})
	if err != nil {
		t.Fatalf("GetRecent: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Should be newest first — IDs should be descending.
	for i := 1; i < len(results); i++ {
		if results[i].ID > results[i-1].ID {
			t.Errorf("results not in descending ID order: id[%d]=%d > id[%d]=%d",
				i, results[i].ID, i-1, results[i-1].ID)
		}
	}
}

func TestMemoryStore_GetRecentWithTypeFilter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	store.Save(ctx, MemoryEntry{Type: TypeExperience, Content: "exp 1"})
	store.Save(ctx, MemoryEntry{Type: TypeReflection, Content: "ref 1"})
	store.Save(ctx, MemoryEntry{Type: TypeExperience, Content: "exp 2"})
	store.Save(ctx, MemoryEntry{Type: TypeProcedure, Content: "proc 1"})

	results, err := store.GetRecent(ctx, RecentOpts{TypeFilter: TypeExperience})
	if err != nil {
		t.Fatalf("GetRecent: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 experience entries, got %d", len(results))
	}
	for _, r := range results {
		if r.Type != TypeExperience {
			t.Errorf("expected type %q, got %q", TypeExperience, r.Type)
		}
	}
}

func TestMemoryStore_GetRecentWithTaskIDFilter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	store.Save(ctx, MemoryEntry{Type: TypeExperience, Content: "task a work", TaskID: "task-a"})
	store.Save(ctx, MemoryEntry{Type: TypeExperience, Content: "task b work", TaskID: "task-b"})
	store.Save(ctx, MemoryEntry{Type: TypeExperience, Content: "more task a", TaskID: "task-a"})

	results, err := store.GetRecent(ctx, RecentOpts{TaskID: "task-a"})
	if err != nil {
		t.Fatalf("GetRecent: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 entries for task-a, got %d", len(results))
	}
	for _, r := range results {
		if r.TaskID != "task-a" {
			t.Errorf("expected task_id 'task-a', got %q", r.TaskID)
		}
	}
}

func TestMemoryStore_Delete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	id, err := store.Save(ctx, MemoryEntry{
		Type:    TypeExperience,
		Topic:   "delete_test",
		Content: "uniqueword_xyzzy content to find",
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify it's searchable.
	results, err := store.Search(ctx, "uniqueword_xyzzy", SearchOpts{})
	if err != nil {
		t.Fatalf("Search before delete: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result before delete, got %d", len(results))
	}

	// Delete it.
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify it's gone from search.
	results, err = store.Search(ctx, "uniqueword_xyzzy", SearchOpts{})
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results after delete, got %d", len(results))
	}

	// Verify count decreased.
	count, err := store.Count(ctx)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0 after delete, got %d", count)
	}
}

func TestMemoryStore_EmptySearchReturnsEmptySlice(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	results, err := store.Search(ctx, "", SearchOpts{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if results == nil {
		t.Error("expected empty slice, got nil")
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for empty query, got %d", len(results))
	}
}

func TestMemoryStore_SearchNoResults(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	store.Save(ctx, MemoryEntry{Type: TypeExperience, Content: "hello world"})

	results, err := store.Search(ctx, "zzzznotfound", SearchOpts{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if results == nil {
		t.Error("expected empty slice, got nil")
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}
