package memory

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newToolTestStore(t *testing.T) *MemoryStore {
	t.Helper()
	db, err := NewMemoryDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return NewMemoryStore(db)
}

func TestMemorySearchTool_Execute(t *testing.T) {
	store := newToolTestStore(t)
	ctx := context.Background()

	// Save some entries.
	store.Save(ctx, MemoryEntry{Type: TypeExperience, Source: "agent", Topic: "golang", Content: "learned about goroutines and channels"})
	store.Save(ctx, MemoryEntry{Type: TypeReflection, Source: "agent", Topic: "python", Content: "python asyncio is different"})

	tool := NewMemorySearchTool(store)

	// Verify interface compliance.
	if tool.Name() != "memory_search" {
		t.Errorf("expected name memory_search, got %s", tool.Name())
	}

	input := json.RawMessage(`{"query": "goroutines"}`)
	result, err := tool.Execute(ctx, input)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var entries []map[string]any
	if err := json.Unmarshal([]byte(result), &entries); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one result")
	}

	// Verify expected fields.
	entry := entries[0]
	for _, field := range []string{"id", "type", "topic", "content", "timestamp", "rank"} {
		if _, ok := entry[field]; !ok {
			t.Errorf("missing field %q in result", field)
		}
	}
	if entry["topic"] != "golang" {
		t.Errorf("expected topic 'golang', got %v", entry["topic"])
	}
}

func TestMemorySearchTool_EmptyResults(t *testing.T) {
	store := newToolTestStore(t)
	ctx := context.Background()

	store.Save(ctx, MemoryEntry{Type: TypeExperience, Content: "hello world"})

	tool := NewMemorySearchTool(store)
	input := json.RawMessage(`{"query": "zzzznotfound"}`)
	result, err := tool.Execute(ctx, input)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result != "[]" {
		t.Errorf("expected '[]', got %q", result)
	}
}

func TestMemorySearchTool_ContentTruncation(t *testing.T) {
	store := newToolTestStore(t)
	ctx := context.Background()

	// Create content longer than 500 chars.
	longContent := "truncationtest " + strings.Repeat("a", 1000)
	store.Save(ctx, MemoryEntry{Type: TypeExperience, Topic: "long", Content: longContent})

	tool := NewMemorySearchTool(store)
	input := json.RawMessage(`{"query": "truncationtest"}`)
	result, err := tool.Execute(ctx, input)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var entries []map[string]any
	if err := json.Unmarshal([]byte(result), &entries); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one result")
	}

	content := entries[0]["content"].(string)
	if !strings.HasSuffix(content, "...") {
		t.Error("expected content to end with '...'")
	}
	// 500 chars + "..." = 503
	if len(content) != 503 {
		t.Errorf("expected truncated content length 503, got %d", len(content))
	}
}

func TestMemoryReadTool_Execute(t *testing.T) {
	store := newToolTestStore(t)
	ctx := context.Background()

	store.Save(ctx, MemoryEntry{Type: TypeExperience, Topic: "first", Content: "entry one"})
	store.Save(ctx, MemoryEntry{Type: TypeReflection, Topic: "second", Content: "entry two"})
	store.Save(ctx, MemoryEntry{Type: TypeProcedure, Topic: "third", Content: "entry three"})

	tool := NewMemoryReadTool(store)

	if tool.Name() != "memory_read" {
		t.Errorf("expected name memory_read, got %s", tool.Name())
	}

	input := json.RawMessage(`{"limit": 2}`)
	result, err := tool.Execute(ctx, input)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var entries []map[string]any
	if err := json.Unmarshal([]byte(result), &entries); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 results, got %d", len(entries))
	}

	// Verify expected fields (no rank field for read).
	for _, field := range []string{"id", "type", "topic", "content", "timestamp"} {
		if _, ok := entries[0][field]; !ok {
			t.Errorf("missing field %q in result", field)
		}
	}
	if _, ok := entries[0]["rank"]; ok {
		t.Error("read results should not have rank field")
	}
}

func TestMemoryWriteTool_Execute(t *testing.T) {
	store := newToolTestStore(t)
	ctx := context.Background()

	tool := NewMemoryWriteTool(store, "")

	if tool.Name() != "memory_write" {
		t.Errorf("expected name memory_write, got %s", tool.Name())
	}

	input := json.RawMessage(`{"type": "experience", "topic": "test", "content": "test content"}`)
	result, err := tool.Execute(ctx, input)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var resp map[string]any
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["status"] != "saved" {
		t.Errorf("expected status 'saved', got %v", resp["status"])
	}
	if resp["id"] == nil || resp["id"].(float64) <= 0 {
		t.Error("expected positive id")
	}

	// Verify entry was actually saved.
	entries, err := store.GetRecent(ctx, RecentOpts{Limit: 10})
	if err != nil {
		t.Fatalf("GetRecent: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Content != "test content" {
		t.Errorf("expected content 'test content', got %q", entries[0].Content)
	}
}

func TestMemoryWriteTool_UpdatesMemoryMD(t *testing.T) {
	store := newToolTestStore(t)
	ctx := context.Background()

	tmpDir := t.TempDir()
	mdPath := filepath.Join(tmpDir, "MEMORY.md")

	tool := NewMemoryWriteTool(store, mdPath)

	input := json.RawMessage(`{"type": "procedure", "topic": "deploy", "content": "deploy using kubernetes"}`)
	_, err := tool.Execute(ctx, input)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	data, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	expected := "- [procedure] deploy: deploy using kubernetes\n"
	if string(data) != expected {
		t.Errorf("expected MEMORY.md content %q, got %q", expected, string(data))
	}
}

func TestMemoryWriteTool_InvalidType(t *testing.T) {
	store := newToolTestStore(t)
	ctx := context.Background()

	tool := NewMemoryWriteTool(store, "")

	input := json.RawMessage(`{"type": "invalid", "topic": "test", "content": "test content"}`)
	_, err := tool.Execute(ctx, input)
	if err == nil {
		t.Fatal("expected error for invalid type")
	}
	if !strings.Contains(err.Error(), "invalid type") {
		t.Errorf("expected error to mention 'invalid type', got: %v", err)
	}
}

func TestMemoryWriteTool_EmptyContent(t *testing.T) {
	store := newToolTestStore(t)
	ctx := context.Background()

	tool := NewMemoryWriteTool(store, "")

	input := json.RawMessage(`{"type": "experience", "topic": "test", "content": ""}`)
	_, err := tool.Execute(ctx, input)
	if err == nil {
		t.Fatal("expected error for empty content")
	}
	if !strings.Contains(err.Error(), "content must not be empty") {
		t.Errorf("expected error to mention 'content must not be empty', got: %v", err)
	}
}
