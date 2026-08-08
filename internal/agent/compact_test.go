package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
)

// --- Helpers ---

func makeMessages(n int) []Message {
	msgs := make([]Message, n)
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			msgs[i] = NewUserMessage(fmt.Sprintf("User message %d", i))
		} else {
			msgs[i] = NewAssistantMessage([]ContentBlock{
				{Type: "text", Text: fmt.Sprintf("Assistant response %d", i)},
			})
		}
	}
	return msgs
}

func mockSummaryServer(t *testing.T, summaryText string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := llm.Response{
			ID:    "msg_summary",
			Model: "claude-sonnet-4-20250514",
			Role:  llm.RoleAssistant,
			Content: []llm.ContentBlock{
				{Type: "text", Text: summaryText},
			},
			StopReason: llm.StopReasonEndTurn,
			Usage:      llm.Usage{InputTokens: 50, OutputTokens: 30},
		}
		data, _ := json.Marshal(resp)
		w.Write(data)
	}))
}

func failingLLMServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte(`{"type":"error","error":{"type":"api_error","message":"server error"}}`))
	}))
}

func emptySummaryServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := llm.Response{
			ID:         "msg_empty",
			Model:      "claude-sonnet-4-20250514",
			Role:       llm.RoleAssistant,
			Content:    []llm.ContentBlock{},
			StopReason: llm.StopReasonEndTurn,
		}
		data, _ := json.Marshal(resp)
		w.Write(data)
	}))
}

// --- Tests ---

// T-1: Verifies R-3 — given 20 messages, compaction produces fewer messages with a summary.
func TestCompactMessages_Basic(t *testing.T) {
	t.Parallel()

	srv := mockSummaryServer(t, "This is a summary of the conversation.")
	defer srv.Close()

	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	msgs := makeMessages(20)

	result, err := CompactMessages(context.Background(), msgs, "system prompt", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) >= len(msgs) {
		t.Fatalf("expected fewer messages after compaction, got %d (original %d)", len(result), len(msgs))
	}
	// First message should contain the summary
	if result[0].Role != RoleUser {
		t.Fatalf("first message should be user, got %s", result[0].Role)
	}
	firstText := result[0].TextContent()
	if !strings.Contains(firstText, "summary") || !strings.Contains(firstText, "conversation") {
		t.Fatalf("first message should contain summary text, got %q", firstText)
	}
}

// T-2: Verifies R-6/EC-2 — given 4 messages, returns unchanged.
func TestCompactMessages_TooFewMessages(t *testing.T) {
	t.Parallel()

	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})
	msgs := makeMessages(4)

	result, err := CompactMessages(context.Background(), msgs, "system prompt", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != len(msgs) {
		t.Fatalf("expected unchanged messages (len %d), got %d", len(msgs), len(result))
	}
}

// T-3: Verifies EC-1 — given empty messages, returns empty.
func TestCompactMessages_Empty(t *testing.T) {
	t.Parallel()

	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})

	result, err := CompactMessages(context.Background(), []Message{}, "system prompt", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(result))
	}
}

// T-4: Verifies R-7 — LLM returns error, fallback truncation is used.
func TestCompactMessages_LLMFailure_Fallback(t *testing.T) {
	t.Parallel()

	srv := failingLLMServer(t)
	defer srv.Close()

	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	msgs := makeMessages(20)

	result, err := CompactMessages(context.Background(), msgs, "system prompt", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) >= len(msgs) {
		t.Fatalf("expected fewer messages after fallback truncation, got %d", len(result))
	}
	// First message should contain truncation notice
	firstText := result[0].TextContent()
	if !strings.Contains(firstText, "truncated") {
		t.Fatalf("expected truncation notice, got %q", firstText)
	}
	if !strings.Contains(firstText, "messages were removed") {
		t.Fatalf("expected 'messages were removed' in truncation notice, got %q", firstText)
	}
}

// T-5: Verifies R-3 — after compaction, the last N messages are identical to the original last N.
func TestCompactMessages_PreservesRecentMessages(t *testing.T) {
	t.Parallel()

	srv := mockSummaryServer(t, "Summary text.")
	defer srv.Close()

	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	msgs := makeMessages(20)

	result, err := CompactMessages(context.Background(), msgs, "system prompt", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// With 20 messages, keepCount = min(10, 20/2) = 10
	keepCount := 10
	recentOriginal := msgs[len(msgs)-keepCount:]
	recentResult := result[len(result)-keepCount:]

	if len(recentResult) != keepCount {
		t.Fatalf("expected %d recent messages preserved, got %d", keepCount, len(recentResult))
	}

	for i := 0; i < keepCount; i++ {
		if recentResult[i].Role != recentOriginal[i].Role {
			t.Fatalf("recent msg[%d]: role mismatch %s vs %s", i, recentOriginal[i].Role, recentResult[i].Role)
		}
		if recentResult[i].TextContent() != recentOriginal[i].TextContent() {
			t.Fatalf("recent msg[%d]: content mismatch", i)
		}
	}
}

// T-6: Verifies R-8 — given messages with known text lengths, estimate matches expected heuristic.
func TestEstimateTokens(t *testing.T) {
	t.Parallel()

	msgs := []Message{
		NewUserMessage("hello world"), // 11 chars
		NewAssistantMessage([]ContentBlock{
			{Type: "text", Text: strings.Repeat("a", 400)}, // 400 chars
		}),
	}

	estimate := EstimateTokens(msgs)
	// Total chars = 11 + 400 = 411, divided by 4 = 102
	expected := 411 / 4
	if estimate != expected {
		t.Fatalf("expected estimate %d, got %d", expected, estimate)
	}
}

// Verifies R-8 — EstimateTokens handles tool_use and tool_result blocks.
func TestEstimateTokens_WithTools(t *testing.T) {
	t.Parallel()

	msgs := []Message{
		{
			Role: RoleAssistant,
			Content: []ContentBlock{
				{Type: "tool_use", Name: "bash", Input: json.RawMessage(`{"command":"ls"}`)}, // "bash" (4) + `{"command":"ls"}` (16) = 20
			},
		},
		{
			Role: RoleUser,
			Content: []ContentBlock{
				{Type: "tool_result", Content: "file.txt"}, // 8 chars
			},
		},
	}

	estimate := EstimateTokens(msgs)
	// 20 + 8 = 28, / 4 = 7
	expected := 28 / 4
	if estimate != expected {
		t.Fatalf("expected estimate %d, got %d", expected, estimate)
	}
}

// T-7: Verifies R-5 — messages with tool_use and tool_result are formatted correctly.
func TestFormatMessagesForSummary(t *testing.T) {
	t.Parallel()

	msgs := []Message{
		NewUserMessage("Please run ls"),
		{
			Role: RoleAssistant,
			Content: []ContentBlock{
				{Type: "tool_use", Name: "bash", Input: json.RawMessage(`{"command":"ls"}`)},
			},
		},
		{
			Role: RoleUser,
			Content: []ContentBlock{
				{Type: "tool_result", ToolUseID: "tu-1", Content: "file1.txt\nfile2.txt"},
			},
		},
	}

	result := FormatMessagesForSummary(msgs)

	if !strings.Contains(result, "[user]") {
		t.Fatalf("expected [user] prefix, got %q", result)
	}
	if !strings.Contains(result, "[assistant]") {
		t.Fatalf("expected [assistant] prefix, got %q", result)
	}
	if !strings.Contains(result, "[tool_use: bash") {
		t.Fatalf("expected tool_use formatting, got %q", result)
	}
	if !strings.Contains(result, "[tool_result:") {
		t.Fatalf("expected tool_result formatting, got %q", result)
	}
}

// Verifies EC-3 — LLM returns empty summary, uses truncation fallback.
func TestCompactMessages_EmptySummary_Fallback(t *testing.T) {
	t.Parallel()

	srv := emptySummaryServer(t)
	defer srv.Close()

	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	msgs := makeMessages(20)

	result, err := CompactMessages(context.Background(), msgs, "system prompt", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	firstText := result[0].TextContent()
	if !strings.Contains(firstText, "truncated") {
		t.Fatalf("expected truncation fallback, got %q", firstText)
	}
}

// Verifies EC-4 — long messages are truncated in summary formatting.
func TestFormatMessagesForSummary_LongMessages(t *testing.T) {
	t.Parallel()

	longText := strings.Repeat("x", 10000)
	msgs := []Message{
		NewUserMessage(longText),
	}

	result := FormatMessagesForSummary(msgs)

	// maxMessageReprLen = 5000, so should be truncated with "..."
	if len(result) > 6000 { // some overhead for prefix
		t.Fatalf("expected truncated output, got length %d", len(result))
	}
	if !strings.Contains(result, "...") {
		t.Fatalf("expected truncation marker '...' in long message formatting")
	}
}

// NT-1: Exactly 6 messages (minimum threshold) — should compact.
func TestCompactMessages_ExactMinimum(t *testing.T) {
	t.Parallel()

	srv := mockSummaryServer(t, "Brief summary.")
	defer srv.Close()

	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	msgs := makeMessages(6)

	result, err := CompactMessages(context.Background(), msgs, "system prompt", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) >= 6 {
		t.Fatalf("expected compaction with 6 messages, got %d messages back", len(result))
	}
}

// NT-2: 5 messages (below minimum) — should not compact.
func TestCompactMessages_BelowMinimum(t *testing.T) {
	t.Parallel()

	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})
	msgs := makeMessages(5)

	result, err := CompactMessages(context.Background(), msgs, "system prompt", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 5 {
		t.Fatalf("expected 5 messages unchanged, got %d", len(result))
	}
}

// NT-3: nil messages — should not panic.
func TestCompactMessages_Nil(t *testing.T) {
	t.Parallel()

	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test"})

	result, err := CompactMessages(context.Background(), nil, "system prompt", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty result for nil messages, got %d", len(result))
	}
}
