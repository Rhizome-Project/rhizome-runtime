package living_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
	"github.com/Rhizome-Project/rhizome-runtime/internal/living/memory"
)

type mockCompactLLM struct {
	extractEntries  []living.ExtractionEntry
	extractErr      error
	compressSummary string
	compressErr     error
	extractCalled   bool
	compressCalled  bool
}

func (m *mockCompactLLM) Extract(ctx context.Context, formatted string) ([]living.ExtractionEntry, error) {
	m.extractCalled = true
	return m.extractEntries, m.extractErr
}

func (m *mockCompactLLM) Compress(ctx context.Context, formatted string) (string, error) {
	m.compressCalled = true
	return m.compressSummary, m.compressErr
}

func newTestMemoryStore(t *testing.T) *memory.MemoryStore {
	t.Helper()
	db, err := memory.NewMemoryDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create memory db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return memory.NewMemoryStore(db)
}

func TestBrainCompaction_ExtractsAndCompresses(t *testing.T) {
	t.Parallel()

	store := newTestMemoryStore(t)
	mock := &mockCompactLLM{
		extractEntries: []living.ExtractionEntry{
			{Type: "experience", Topic: "deployment", Content: "learned about k8s rollouts"},
			{Type: "entity", Topic: "redis", Content: "redis is used for caching"},
		},
		compressSummary: "User discussed deployment and caching strategies.",
	}

	fn := living.NewBrainCompactFunc(mock, store)
	messages := []llm.Message{
		llm.NewUserMessage("tell me about deployments"),
	}

	result, err := fn(context.Background(), messages, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !mock.extractCalled {
		t.Error("expected Extract to be called")
	}
	if !mock.compressCalled {
		t.Error("expected Compress to be called")
	}

	// Verify entries were saved to memory
	count, err := store.Count(context.Background())
	if err != nil {
		t.Fatalf("failed to count entries: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 memory entries, got %d", count)
	}

	// Verify compressed result
	if len(result) != 1 {
		t.Fatalf("expected 1 message, got %d", len(result))
	}
	if result[0].Role != llm.RoleUser {
		t.Errorf("expected user role, got %s", result[0].Role)
	}
}

func TestBrainCompaction_ExtractionFailure(t *testing.T) {
	t.Parallel()

	store := newTestMemoryStore(t)
	mock := &mockCompactLLM{
		extractErr:      errors.New("extraction broke"),
		compressSummary: "compressed summary",
	}

	fn := living.NewBrainCompactFunc(mock, store)
	messages := []llm.Message{
		llm.NewUserMessage("hello"),
	}

	result, err := fn(context.Background(), messages, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !mock.extractCalled {
		t.Error("expected Extract to be called")
	}
	if !mock.compressCalled {
		t.Error("expected Compress to be called")
	}

	// Compression should still succeed
	if len(result) != 1 {
		t.Fatalf("expected 1 message, got %d", len(result))
	}
	if result[0].TextContent() != "[COMPRESSED CONTEXT]\ncompressed summary" {
		t.Errorf("unexpected content: %s", result[0].TextContent())
	}

	// No entries should be saved
	count, err := store.Count(context.Background())
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries, got %d", count)
	}
}

func TestBrainCompaction_CompressionFailure(t *testing.T) {
	t.Parallel()

	store := newTestMemoryStore(t)
	mock := &mockCompactLLM{
		extractEntries: []living.ExtractionEntry{},
		compressErr:    errors.New("compress broke"),
	}

	fn := living.NewBrainCompactFunc(mock, store)
	messages := []llm.Message{
		llm.NewUserMessage("hello"),
		llm.NewUserMessage("world"),
	}

	result, err := fn(context.Background(), messages, "", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, mock.compressErr) {
		t.Errorf("expected wrapped compress error, got: %v", err)
	}

	// Original messages should be returned
	if len(result) != len(messages) {
		t.Errorf("expected %d messages returned, got %d", len(messages), len(result))
	}
}

func TestBrainCompaction_NilMemoryStore(t *testing.T) {
	t.Parallel()

	mock := &mockCompactLLM{
		compressSummary: "compressed without memory",
	}

	fn := living.NewBrainCompactFunc(mock, nil)
	messages := []llm.Message{
		llm.NewUserMessage("hello"),
	}

	result, err := fn(context.Background(), messages, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Extract should NOT be called when memory store is nil
	if mock.extractCalled {
		t.Error("expected Extract NOT to be called with nil memory store")
	}
	if !mock.compressCalled {
		t.Error("expected Compress to be called")
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 message, got %d", len(result))
	}
}

func TestBrainCompaction_CompressedFormat(t *testing.T) {
	t.Parallel()

	mock := &mockCompactLLM{
		compressSummary: "the summary content",
	}

	fn := living.NewBrainCompactFunc(mock, nil)
	messages := []llm.Message{
		llm.NewUserMessage("some conversation"),
	}

	result, err := fn(context.Background(), messages, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 message, got %d", len(result))
	}

	msg := result[0]
	if msg.Role != llm.RoleUser {
		t.Errorf("expected role %q, got %q", llm.RoleUser, msg.Role)
	}

	text := msg.TextContent()
	expected := "[COMPRESSED CONTEXT]\nthe summary content"
	if text != expected {
		t.Errorf("expected %q, got %q", expected, text)
	}
}

func TestBrainCompaction_FormatMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		messages []llm.Message
		contains []string
	}{
		{
			name: "text message",
			messages: []llm.Message{
				llm.NewUserMessage("hello world"),
			},
			contains: []string{"[user]:", "hello world"},
		},
		{
			name: "tool_use message",
			messages: []llm.Message{
				{
					Role: llm.RoleAssistant,
					Content: []llm.ContentBlock{
						{Type: "tool_use", Name: "bash"},
					},
				},
			},
			contains: []string{"[assistant]:", "[used tool: bash]"},
		},
		{
			name: "tool_result message",
			messages: []llm.Message{
				{
					Role: llm.RoleUser,
					Content: []llm.ContentBlock{
						{Type: "tool_result", ToolUseID: "123", Content: "output"},
					},
				},
			},
			contains: []string{"[user]:", "[tool result]"},
		},
		{
			name: "mixed content",
			messages: []llm.Message{
				llm.NewUserMessage("first"),
				{
					Role: llm.RoleAssistant,
					Content: []llm.ContentBlock{
						{Type: "text", Text: "thinking..."},
						{Type: "tool_use", Name: "read"},
					},
				},
			},
			contains: []string{"first", "thinking...", "[used tool: read]"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := living.FormatMessages(tc.messages)
			for _, s := range tc.contains {
				if !strings.Contains(result, s) {
					t.Errorf("expected output to contain %q, got:\n%s", s, result)
				}
			}
		})
	}
}
