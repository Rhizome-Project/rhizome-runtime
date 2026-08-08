package living_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
	"github.com/Rhizome-Project/rhizome-runtime/internal/living/memory"
)

func TestCanonicalMemoryBackend_SaveAndSearch(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{}
	backend := living.NewCanonicalMemoryBackend(client, "ws-1", "brain-1")
	if backend == nil {
		t.Fatal("expected canonical backend")
	}

	if _, err := backend.Save(context.Background(), memory.MemoryEntry{
		Type:    memory.TypeProcedure,
		Source:  "reflection",
		Topic:   "Deploy gate",
		Content: "Run doctor with fail-on-warn after rollout.",
		TaskID:  "task-1",
	}); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	items, err := backend.Search(context.Background(), "doctor rollout", memory.SearchOpts{
		TypeFilter: memory.TypeProcedure,
		Limit:      5,
	})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 search hit, got %+v", items)
	}
	if items[0].Type != memory.TypeProcedure {
		t.Fatalf("expected procedure type, got %+v", items[0])
	}
	if items[0].TaskID != "task-1" {
		t.Fatalf("expected task-1 provenance, got %+v", items[0])
	}
}

func TestCanonicalMemoryBackend_PreservesTypedCategories(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{}
	backend := living.NewCanonicalMemoryBackend(client, "ws-typed", "brain-1")
	if backend == nil {
		t.Fatal("expected canonical backend")
	}

	if _, err := backend.Save(context.Background(), memory.MemoryEntry{
		Type:    memory.TypeDecision,
		Source:  "reflection",
		Topic:   "Deploy gate",
		Content: "Treat live doctor drift as a release blocker.",
		TaskID:  "task-typed",
	}); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	items, err := backend.Search(context.Background(), "release blocker doctor drift", memory.SearchOpts{
		TypeFilter: memory.TypeDecision,
		Limit:      5,
	})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 search hit, got %+v", items)
	}
	if items[0].Type != memory.TypeDecision {
		t.Fatalf("expected decision type, got %+v", items[0])
	}
}
