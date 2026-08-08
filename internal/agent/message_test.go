package agent

import (
	"encoding/json"
	"testing"
)

// T-1: Verifies R-5 — NewUserMessage creates user message with single text block
func TestNewUserMessage(t *testing.T) {
	t.Parallel()
	msg := NewUserMessage("hello")

	if msg.Role != RoleUser {
		t.Fatalf("expected role %q, got %q", RoleUser, msg.Role)
	}
	if len(msg.Content) != 1 {
		t.Fatalf("expected 1 content block, got %d", len(msg.Content))
	}
	if msg.Content[0].Type != "text" {
		t.Fatalf("expected type %q, got %q", "text", msg.Content[0].Type)
	}
	if msg.Content[0].Text != "hello" {
		t.Fatalf("expected text %q, got %q", "hello", msg.Content[0].Text)
	}
}

// T-2: Verifies R-5 — NewToolResultMessage creates user message with tool_result blocks
func TestNewToolResultMessage(t *testing.T) {
	t.Parallel()
	results := []ToolResult{
		{ToolUseID: "id-1", Content: "output-1", IsError: false},
		{ToolUseID: "id-2", Content: "error-output", IsError: true},
	}
	msg := NewToolResultMessage(results)

	if msg.Role != RoleUser {
		t.Fatalf("expected role %q, got %q", RoleUser, msg.Role)
	}
	if len(msg.Content) != 2 {
		t.Fatalf("expected 2 content blocks, got %d", len(msg.Content))
	}
	for i, block := range msg.Content {
		if block.Type != "tool_result" {
			t.Fatalf("block[%d]: expected type %q, got %q", i, "tool_result", block.Type)
		}
		if block.ToolUseID != results[i].ToolUseID {
			t.Fatalf("block[%d]: expected tool_use_id %q, got %q", i, results[i].ToolUseID, block.ToolUseID)
		}
		if block.Content != results[i].Content {
			t.Fatalf("block[%d]: expected content %q, got %q", i, results[i].Content, block.Content)
		}
		if block.IsError != results[i].IsError {
			t.Fatalf("block[%d]: expected is_error %v, got %v", i, results[i].IsError, block.IsError)
		}
	}
}

// Verifies R-5 — NewAssistantMessage creates assistant message
func TestNewAssistantMessage(t *testing.T) {
	t.Parallel()
	blocks := []ContentBlock{
		{Type: "text", Text: "I will help you"},
		{Type: "tool_use", ID: "tu-1", Name: "bash", Input: json.RawMessage(`{"command":"ls"}`)},
	}
	msg := NewAssistantMessage(blocks)

	if msg.Role != RoleAssistant {
		t.Fatalf("expected role %q, got %q", RoleAssistant, msg.Role)
	}
	if len(msg.Content) != 2 {
		t.Fatalf("expected 2 content blocks, got %d", len(msg.Content))
	}
}

// T-3: Verifies R-7 — HasToolUse returns true when tool_use block present
func TestHasToolUse_True(t *testing.T) {
	t.Parallel()
	msg := Message{
		Role: RoleAssistant,
		Content: []ContentBlock{
			{Type: "text", Text: "thinking..."},
			{Type: "tool_use", ID: "tu-1", Name: "bash", Input: json.RawMessage(`{}`)},
		},
	}
	if !msg.HasToolUse() {
		t.Fatal("expected HasToolUse() to return true")
	}
}

// T-4: Verifies R-7 — HasToolUse returns false when no tool_use blocks
func TestHasToolUse_False(t *testing.T) {
	t.Parallel()
	msg := Message{
		Role:    RoleAssistant,
		Content: []ContentBlock{{Type: "text", Text: "done"}},
	}
	if msg.HasToolUse() {
		t.Fatal("expected HasToolUse() to return false")
	}
}

// T-5: Verifies R-8 — ToolUseBlocks returns only tool_use blocks
func TestToolUseBlocks(t *testing.T) {
	t.Parallel()
	msg := Message{
		Role: RoleAssistant,
		Content: []ContentBlock{
			{Type: "text", Text: "a"},
			{Type: "tool_use", ID: "tu-1", Name: "bash"},
			{Type: "text", Text: "b"},
			{Type: "tool_use", ID: "tu-2", Name: "read"},
			{Type: "tool_use", ID: "tu-3", Name: "edit"},
		},
	}
	blocks := msg.ToolUseBlocks()
	if len(blocks) != 3 {
		t.Fatalf("expected 3 tool_use blocks, got %d", len(blocks))
	}
	expected := []string{"tu-1", "tu-2", "tu-3"}
	for i, b := range blocks {
		if b.ID != expected[i] {
			t.Fatalf("block[%d]: expected ID %q, got %q", i, expected[i], b.ID)
		}
	}
}

// T-6: Verifies EC-1 — TextContent skips empty text blocks
func TestTextContent_SkipsEmpty(t *testing.T) {
	t.Parallel()
	msg := Message{
		Role: RoleAssistant,
		Content: []ContentBlock{
			{Type: "text", Text: "hello"},
			{Type: "text", Text: ""},
			{Type: "text", Text: "world"},
		},
	}
	got := msg.TextContent()
	if got != "hello\nworld" {
		t.Fatalf("expected %q, got %q", "hello\nworld", got)
	}
}

// T-7: Verifies R-10 — JSON round-trip preserves all fields
func TestMessageJSONRoundTrip(t *testing.T) {
	t.Parallel()
	original := Message{
		Role: RoleAssistant,
		Content: []ContentBlock{
			{Type: "text", Text: "let me help"},
			{Type: "tool_use", ID: "tu-1", Name: "bash", Input: json.RawMessage(`{"command":"ls -la"}`)},
			{Type: "tool_result", ToolUseID: "tu-1", Content: "file1.txt\nfile2.txt", IsError: false},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var restored Message
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if restored.Role != original.Role {
		t.Fatalf("role mismatch: %q vs %q", original.Role, restored.Role)
	}
	if len(restored.Content) != len(original.Content) {
		t.Fatalf("content length mismatch: %d vs %d", len(original.Content), len(restored.Content))
	}
	for i := range original.Content {
		o, r := original.Content[i], restored.Content[i]
		if o.Type != r.Type {
			t.Fatalf("block[%d] type: %q vs %q", i, o.Type, r.Type)
		}
		if o.Text != r.Text {
			t.Fatalf("block[%d] text: %q vs %q", i, o.Text, r.Text)
		}
		if o.ID != r.ID {
			t.Fatalf("block[%d] id: %q vs %q", i, o.ID, r.ID)
		}
		if o.Name != r.Name {
			t.Fatalf("block[%d] name: %q vs %q", i, o.Name, r.Name)
		}
		if o.ToolUseID != r.ToolUseID {
			t.Fatalf("block[%d] tool_use_id: %q vs %q", i, o.ToolUseID, r.ToolUseID)
		}
		if o.Content != r.Content {
			t.Fatalf("block[%d] content: %q vs %q", i, o.Content, r.Content)
		}
		if o.IsError != r.IsError {
			t.Fatalf("block[%d] is_error: %v vs %v", i, o.IsError, r.IsError)
		}
		if string(o.Input) != string(r.Input) {
			t.Fatalf("block[%d] input: %s vs %s", i, o.Input, r.Input)
		}
	}
}

// T-8: Verifies EC-2 — empty message returns zero values
func TestEmptyMessage(t *testing.T) {
	t.Parallel()
	msg := Message{}

	if msg.HasToolUse() {
		t.Fatal("empty message: HasToolUse should be false")
	}
	if blocks := msg.ToolUseBlocks(); blocks != nil {
		t.Fatalf("empty message: ToolUseBlocks should be nil, got %v", blocks)
	}
	if text := msg.TextContent(); text != "" {
		t.Fatalf("empty message: TextContent should be empty, got %q", text)
	}
}

// NT-1: Negative test — TextContent ignores non-text block types
func TestTextContent_IgnoresNonTextBlocks(t *testing.T) {
	t.Parallel()
	msg := Message{
		Role: RoleAssistant,
		Content: []ContentBlock{
			{Type: "thinking", Text: "secret thoughts"},
			{Type: "tool_use", ID: "tu-1", Name: "bash"},
		},
	}
	if got := msg.TextContent(); got != "" {
		t.Fatalf("expected empty string for non-text blocks, got %q", got)
	}
}

// NT-2: Negative test — ToolUseBlocks returns nil when no tool_use
func TestToolUseBlocks_NoToolUse(t *testing.T) {
	t.Parallel()
	msg := NewUserMessage("hello")
	if blocks := msg.ToolUseBlocks(); blocks != nil {
		t.Fatalf("expected nil, got %v", blocks)
	}
}

// NT-3: Negative test — NewToolResultMessage with empty slice
func TestNewToolResultMessage_Empty(t *testing.T) {
	t.Parallel()
	msg := NewToolResultMessage([]ToolResult{})
	if msg.Role != RoleUser {
		t.Fatalf("expected role %q, got %q", RoleUser, msg.Role)
	}
	if len(msg.Content) != 0 {
		t.Fatalf("expected 0 blocks, got %d", len(msg.Content))
	}
}

// Verifies R-12, R-13 — Response JSON round-trip
func TestResponseJSONRoundTrip(t *testing.T) {
	t.Parallel()
	original := Response{
		ID:    "msg_123",
		Model: "claude-sonnet-4-20250514",
		Role:  RoleAssistant,
		Content: []ContentBlock{
			{Type: "text", Text: "hello"},
		},
		StopReason: StopReasonEndTurn,
		Usage: Usage{
			InputTokens:              100,
			OutputTokens:             50,
			CacheCreationInputTokens: 10,
			CacheReadInputTokens:     5,
		},
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var restored Response
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if restored.ID != original.ID {
		t.Fatalf("id mismatch")
	}
	if restored.StopReason != original.StopReason {
		t.Fatalf("stop_reason mismatch: %q vs %q", original.StopReason, restored.StopReason)
	}
	if restored.Usage.InputTokens != original.Usage.InputTokens {
		t.Fatalf("input_tokens mismatch")
	}
	if restored.Usage.CacheCreationInputTokens != original.Usage.CacheCreationInputTokens {
		t.Fatalf("cache_creation_input_tokens mismatch")
	}
}

// Verifies R-2, R-11 — Role and StopReason constants have correct values
func TestConstants(t *testing.T) {
	t.Parallel()
	if RoleUser != "user" {
		t.Fatalf("RoleUser = %q", RoleUser)
	}
	if RoleAssistant != "assistant" {
		t.Fatalf("RoleAssistant = %q", RoleAssistant)
	}
	if StopReasonEndTurn != "end_turn" {
		t.Fatalf("StopReasonEndTurn = %q", StopReasonEndTurn)
	}
	if StopReasonToolUse != "tool_use" {
		t.Fatalf("StopReasonToolUse = %q", StopReasonToolUse)
	}
	if StopReasonMaxTokens != "max_tokens" {
		t.Fatalf("StopReasonMaxTokens = %q", StopReasonMaxTokens)
	}
	if StopReasonStopSequence != "stop_sequence" {
		t.Fatalf("StopReasonStopSequence = %q", StopReasonStopSequence)
	}
}
