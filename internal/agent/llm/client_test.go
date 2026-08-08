package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// --- Helpers ---

func validClaudeResponse() Response {
	return Response{
		ID:    "msg_test_123",
		Model: "claude-sonnet-4-20250514",
		Role:  RoleAssistant,
		Content: []ContentBlock{
			{Type: "text", Text: "Hello, world!"},
		},
		StopReason: StopReasonEndTurn,
		Usage: Usage{
			InputTokens:  100,
			OutputTokens: 50,
		},
	}
}

func validOpenAIResponse() string {
	return `{
		"id": "chatcmpl-test",
		"model": "gpt-4o",
		"choices": [{
			"message": {"role": "assistant", "content": "Hello from OpenAI!"},
			"finish_reason": "stop"
		}],
		"usage": {"prompt_tokens": 80, "completion_tokens": 40}
	}`
}

func mustJSON(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}

// --- Claude Provider Tests ---

func TestNewClient_ClaudeDefault(t *testing.T) {
	t.Parallel()
	c := NewClient(ClientConfig{APIKey: "sk-test"})
	if c.ProviderName() != "claude" {
		t.Fatalf("expected provider %q, got %q", "claude", c.ProviderName())
	}
	if !c.IsConfigured() {
		t.Fatal("expected IsConfigured=true")
	}
}

func TestNewClient_OpenAI(t *testing.T) {
	t.Parallel()
	c := NewClient(ClientConfig{Provider: ProviderOpenAI, APIKey: "sk-test"})
	if c.ProviderName() != "openai" {
		t.Fatalf("expected provider %q, got %q", "openai", c.ProviderName())
	}
}

func TestNewClient_NotConfigured(t *testing.T) {
	t.Parallel()
	c := NewClient(ClientConfig{})
	if c.IsConfigured() {
		t.Fatal("expected IsConfigured=false without API key")
	}
}

// TestSend_Claude_Success — Claude provider with mock server
func TestSend_Claude_Success(t *testing.T) {
	t.Parallel()

	resp := validClaudeResponse()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify Claude-specific headers
		if got := r.Header.Get("x-api-key"); got != "sk-test-key" {
			t.Errorf("x-api-key: expected %q, got %q", "sk-test-key", got)
		}
		if got := r.Header.Get("anthropic-version"); got != "2023-06-01" {
			t.Errorf("anthropic-version: expected %q, got %q", "2023-06-01", got)
		}
		if !strings.HasSuffix(r.URL.Path, "/v1/messages") {
			t.Errorf("path: expected suffix /v1/messages, got %s", r.URL.Path)
		}
		w.WriteHeader(200)
		w.Write(mustJSON(resp))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-test-key", BaseURL: srv.URL})
	result, err := c.Send(context.Background(), SendRequest{
		System:   "You are helpful.",
		Messages: []Message{NewUserMessage("hello")},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ID != "msg_test_123" {
		t.Fatalf("expected ID %q, got %q", "msg_test_123", result.ID)
	}
	if result.StopReason != StopReasonEndTurn {
		t.Fatalf("expected stop_reason %q, got %q", StopReasonEndTurn, result.StopReason)
	}
	if len(result.Content) != 1 || result.Content[0].Text != "Hello, world!" {
		t.Fatalf("unexpected content: %+v", result.Content)
	}
	if result.Usage.InputTokens != 100 || result.Usage.OutputTokens != 50 {
		t.Fatalf("unexpected usage: %+v", result.Usage)
	}
}

// TestSend_OpenAI_Success — OpenAI provider with mock server
func TestSend_OpenAI_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify OpenAI-specific headers
		if got := r.Header.Get("Authorization"); got != "Bearer sk-openai" {
			t.Errorf("Authorization: expected %q, got %q", "Bearer sk-openai", got)
		}
		if !strings.HasSuffix(r.URL.Path, "/v1/chat/completions") {
			t.Errorf("path: expected suffix /v1/chat/completions, got %s", r.URL.Path)
		}
		w.WriteHeader(200)
		w.Write([]byte(validOpenAIResponse()))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{Provider: ProviderOpenAI, APIKey: "sk-openai", BaseURL: srv.URL})
	result, err := c.Send(context.Background(), SendRequest{
		System:   "You are helpful.",
		Messages: []Message{NewUserMessage("hello")},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ID != "chatcmpl-test" {
		t.Fatalf("expected ID %q, got %q", "chatcmpl-test", result.ID)
	}
	if result.StopReason != StopReasonEndTurn {
		t.Fatalf("expected stop_reason %q, got %q", StopReasonEndTurn, result.StopReason)
	}
	if len(result.Content) != 1 || result.Content[0].Text != "Hello from OpenAI!" {
		t.Fatalf("unexpected content: %+v", result.Content)
	}
	if result.Usage.InputTokens != 80 || result.Usage.OutputTokens != 40 {
		t.Fatalf("unexpected usage: %+v", result.Usage)
	}
}

// TestSend_OpenAI_ToolCalls — OpenAI tool_calls response
func TestSend_OpenAI_ToolCalls(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{
			"id": "chatcmpl-tools",
			"model": "gpt-4o",
			"choices": [{
				"message": {
					"role": "assistant",
					"content": "Let me run that.",
					"tool_calls": [{
						"id": "call_123",
						"type": "function",
						"function": {"name": "bash", "arguments": "{\"command\":\"ls\"}"}
					}]
				},
				"finish_reason": "tool_calls"
			}],
			"usage": {"prompt_tokens": 50, "completion_tokens": 30}
		}`))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{Provider: ProviderOpenAI, APIKey: "sk-test", BaseURL: srv.URL})
	result, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("run ls")},
		Tools:    []map[string]any{{"name": "bash", "description": "Run bash", "input_schema": map[string]any{"type": "object"}}},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.StopReason != StopReasonToolUse {
		t.Fatalf("expected stop_reason %q, got %q", StopReasonToolUse, result.StopReason)
	}
	if len(result.Content) != 2 {
		t.Fatalf("expected 2 content blocks (text + tool_use), got %d", len(result.Content))
	}
	if result.Content[0].Type != "text" || result.Content[0].Text != "Let me run that." {
		t.Fatalf("unexpected text block: %+v", result.Content[0])
	}
	if result.Content[1].Type != "tool_use" || result.Content[1].Name != "bash" {
		t.Fatalf("unexpected tool_use block: %+v", result.Content[1])
	}
	if result.Content[1].ID != "call_123" {
		t.Fatalf("expected tool call ID %q, got %q", "call_123", result.Content[1].ID)
	}
}

// TestSend_OpenAI_ExtraHeaders — custom headers (e.g. OpenRouter)
func TestSend_OpenAI_ExtraHeaders(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("HTTP-Referer"); got != "https://myapp.com" {
			t.Errorf("HTTP-Referer: expected %q, got %q", "https://myapp.com", got)
		}
		w.WriteHeader(200)
		w.Write([]byte(validOpenAIResponse()))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{
		Provider: ProviderOpenAI,
		APIKey:   "sk-test",
		BaseURL:  srv.URL,
		Headers:  map[string]string{"HTTP-Referer": "https://myapp.com"},
	})
	_, err := c.Send(context.Background(), SendRequest{Messages: []Message{NewUserMessage("hi")}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestSend_Claude_RateLimit_Retry — 429 retries for Claude
func TestSend_Claude_RateLimit_Retry(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n <= 2 {
			w.WriteHeader(429)
			w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"rate limited"}}`))
			return
		}
		w.WriteHeader(200)
		w.Write(mustJSON(validClaudeResponse()))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	result, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}
	if result.ID != "msg_test_123" {
		t.Fatalf("expected ID %q, got %q", "msg_test_123", result.ID)
	}
	if got := callCount.Load(); got != 3 {
		t.Fatalf("expected 3 calls, got %d", got)
	}
}

// TestSend_OpenAI_RateLimit_Retry — 429 retries for OpenAI
func TestSend_OpenAI_RateLimit_Retry(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n <= 2 {
			w.WriteHeader(429)
			w.Write([]byte(`{"error":{"message":"rate limited","type":"rate_limit"}}`))
			return
		}
		w.WriteHeader(200)
		w.Write([]byte(validOpenAIResponse()))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{Provider: ProviderOpenAI, APIKey: "sk-test", BaseURL: srv.URL})
	result, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}
	if result.ID != "chatcmpl-test" {
		t.Fatalf("expected ID %q, got %q", "chatcmpl-test", result.ID)
	}
}

// TestSend_MaxRetries — exhausts retries
func TestSend_MaxRetries(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(429)
		w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"rate limited"}}`))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	_, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrMaxRetries) {
		t.Fatalf("expected ErrMaxRetries, got: %v", err)
	}
	if got := callCount.Load(); got != 4 {
		t.Fatalf("expected 4 calls (1 + 3 retries), got %d", got)
	}
}

// TestSend_BadRequest
func TestSend_BadRequest(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"invalid model"}}`))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	_, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected ErrBadRequest, got: %v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got: %T", err)
	}
	if apiErr.StatusCode != 400 {
		t.Fatalf("expected status 400, got %d", apiErr.StatusCode)
	}
}

// TestSend_Unauthorized
func TestSend_Unauthorized(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte(`{"type":"error","error":{"type":"authentication_error","message":"invalid api key"}}`))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-bad", BaseURL: srv.URL})
	_, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got: %v", err)
	}
}

// TestSend_ServerError
func TestSend_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte(`{"type":"error","error":{"type":"api_error","message":"internal server error"}}`))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	_, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if !errors.Is(err, ErrAPIError) {
		t.Fatalf("expected ErrAPIError, got: %v", err)
	}
}

// TestSend_ContextCancelled
func TestSend_ContextCancelled(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"rate limited"}}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	c := NewClient(ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	_, err := c.Send(ctx, SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

// TestSend_EmptyAPIKey
func TestSend_EmptyAPIKey(t *testing.T) {
	t.Parallel()

	var called atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "", BaseURL: srv.URL})
	_, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got: %v", err)
	}
	if got := called.Load(); got != 0 {
		t.Fatalf("expected no HTTP calls, got %d", got)
	}
}

// TestSend_Claude_ErrorInBody — 200 response with error type
func TestSend_Claude_ErrorInBody(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{"type":"error","error":{"type":"overloaded_error","message":"the server is overloaded"}}`))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	_, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got: %T — %v", err, err)
	}
}

// TestSend_Claude_RequestBody — system and tools in request body
func TestSend_Claude_RequestBody(t *testing.T) {
	t.Parallel()

	var receivedBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&receivedBody)
		w.WriteHeader(200)
		w.Write(mustJSON(validClaudeResponse()))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})

	// Without system and tools
	_, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := receivedBody["system"]; ok {
		t.Fatal("system field should be omitted when empty")
	}
	if _, ok := receivedBody["tools"]; ok {
		t.Fatal("tools field should be omitted when nil/empty")
	}

	// With system and tools
	_, err = c.Send(context.Background(), SendRequest{
		System:   "Be helpful",
		Messages: []Message{NewUserMessage("hello")},
		Tools:    []map[string]any{{"name": "bash"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedBody["system"] != "Be helpful" {
		t.Fatalf("expected system field, got %v", receivedBody["system"])
	}
	if receivedBody["tools"] == nil {
		t.Fatal("expected tools field when tools provided")
	}
}

// TestSend_Claude_ToolUseResponse
func TestSend_Claude_ToolUseResponse(t *testing.T) {
	t.Parallel()

	resp := Response{
		ID:    "msg_tools",
		Model: "claude-sonnet-4-20250514",
		Role:  RoleAssistant,
		Content: []ContentBlock{
			{Type: "text", Text: "I'll run a command."},
			{Type: "tool_use", ID: "toolu_1", Name: "bash", Input: json.RawMessage(`{"command":"ls"}`)},
		},
		StopReason: StopReasonToolUse,
		Usage:      Usage{InputTokens: 200, OutputTokens: 100},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write(mustJSON(resp))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	result, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.StopReason != StopReasonToolUse {
		t.Fatalf("expected stop_reason tool_use, got %q", result.StopReason)
	}
	if len(result.Content) != 2 {
		t.Fatalf("expected 2 content blocks, got %d", len(result.Content))
	}
	if result.Content[1].Name != "bash" {
		t.Fatalf("expected tool name %q, got %q", "bash", result.Content[1].Name)
	}
}

// TestSend_MalformedResponse
func TestSend_MalformedResponse(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{invalid json}`))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	_, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

// TestSend_Claude_529Retry — overloaded retries
func TestSend_Claude_529Retry(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n <= 1 {
			w.WriteHeader(529)
			w.Write([]byte(`{"type":"error","error":{"type":"overloaded_error","message":"overloaded"}}`))
			return
		}
		w.WriteHeader(200)
		w.Write(mustJSON(validClaudeResponse()))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	result, err := c.Send(context.Background(), SendRequest{
		Messages: []Message{NewUserMessage("hello")},
	})

	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if result.ID != "msg_test_123" {
		t.Fatalf("unexpected response ID: %q", result.ID)
	}
}

// TestAPIError_ErrorAndUnwrap
func TestAPIError_ErrorAndUnwrap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		statusCode int
		wantErr    error
	}{
		{400, ErrBadRequest},
		{401, ErrUnauthorized},
		{500, ErrAPIError},
		{403, ErrAPIError},
	}
	for _, tt := range tests {
		apiErr := &APIError{StatusCode: tt.statusCode, ErrorMsg: "test", Type: "test_error"}
		if !errors.Is(apiErr, tt.wantErr) {
			t.Errorf("APIError{StatusCode: %d}: expected to wrap %v", tt.statusCode, tt.wantErr)
		}
		errStr := apiErr.Error()
		if !strings.Contains(errStr, fmt.Sprintf("%d", tt.statusCode)) {
			t.Errorf("Error() should contain status code %d, got %q", tt.statusCode, errStr)
		}
	}
}

// TestRetryDelay
func TestRetryDelay(t *testing.T) {
	t.Parallel()

	// 429 attempt 0 = 1s
	d429 := retryDelay(429, 0, time.Second, 2*time.Second)
	if d429 != time.Second {
		t.Fatalf("429 attempt 0: expected 1s, got %v", d429)
	}

	// 529 attempt 0 = 2s
	d529 := retryDelay(529, 0, time.Second, 2*time.Second)
	if d529 != 2*time.Second {
		t.Fatalf("529 attempt 0: expected 2s, got %v", d529)
	}

	// 429 attempt 1 = 2s
	d429a1 := retryDelay(429, 1, time.Second, 2*time.Second)
	if d429a1 != 2*time.Second {
		t.Fatalf("429 attempt 1: expected 2s, got %v", d429a1)
	}

	// 529 attempt 1 = 4s
	d529a1 := retryDelay(529, 1, time.Second, 2*time.Second)
	if d529a1 != 4*time.Second {
		t.Fatalf("529 attempt 1: expected 4s, got %v", d529a1)
	}
}

// TestNewClientWithProvider — custom provider injection
func TestNewClientWithProvider(t *testing.T) {
	t.Parallel()

	p := NewClaudeProvider(ClaudeConfig{APIKey: "sk-test"})
	c := NewClientWithProvider(p)
	if c.ProviderName() != "claude" {
		t.Fatalf("expected provider %q, got %q", "claude", c.ProviderName())
	}
	if !c.IsConfigured() {
		t.Fatal("expected IsConfigured=true")
	}
}

// TestSend_NilMessages — no validation, still works
func TestSend_NilMessages(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write(mustJSON(validClaudeResponse()))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	result, err := c.Send(context.Background(), SendRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// --- OpenAI conversion tests ---

func TestConvertToOpenAIMessages_System(t *testing.T) {
	t.Parallel()

	msgs := convertToOpenAIMessages("Be helpful", []Message{NewUserMessage("hi")})
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].Role != "system" || msgs[0].Content != "Be helpful" {
		t.Fatalf("unexpected system message: %+v", msgs[0])
	}
	if msgs[1].Role != "user" {
		t.Fatalf("unexpected user message role: %q", msgs[1].Role)
	}
}

func TestConvertToOpenAIMessages_ToolResults(t *testing.T) {
	t.Parallel()

	toolMsg := NewToolResultMessage([]ToolResult{
		{ToolUseID: "call_1", Content: "output"},
	})
	msgs := convertToOpenAIMessages("", []Message{toolMsg})
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Role != "tool" {
		t.Fatalf("expected role %q, got %q", "tool", msgs[0].Role)
	}
	if msgs[0].ToolCallID != "call_1" {
		t.Fatalf("expected tool_call_id %q, got %q", "call_1", msgs[0].ToolCallID)
	}
}

func TestConvertToOpenAIMessages_AssistantWithTools(t *testing.T) {
	t.Parallel()

	assistantMsg := NewAssistantMessage([]ContentBlock{
		{Type: "text", Text: "Let me check."},
		{Type: "tool_use", ID: "call_1", Name: "bash", Input: json.RawMessage(`{"cmd":"ls"}`)},
	})
	msgs := convertToOpenAIMessages("", []Message{assistantMsg})
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Content != "Let me check." {
		t.Fatalf("unexpected content: %v", msgs[0].Content)
	}
	if len(msgs[0].ToolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(msgs[0].ToolCalls))
	}
	if msgs[0].ToolCalls[0].Function.Name != "bash" {
		t.Fatalf("expected tool name %q, got %q", "bash", msgs[0].ToolCalls[0].Function.Name)
	}
}

func TestConvertToOpenAITools(t *testing.T) {
	t.Parallel()

	tools := []map[string]any{
		{
			"name":         "bash",
			"description":  "Run bash commands",
			"input_schema": map[string]any{"type": "object"},
		},
	}
	oaiTools := convertToOpenAITools(tools)
	if len(oaiTools) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(oaiTools))
	}
	if oaiTools[0].Type != "function" {
		t.Fatalf("expected type %q, got %q", "function", oaiTools[0].Type)
	}
	if oaiTools[0].Function.Name != "bash" {
		t.Fatalf("expected name %q, got %q", "bash", oaiTools[0].Function.Name)
	}
}

func TestMapOpenAIFinishReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reason string
		want   StopReason
	}{
		{"stop", StopReasonEndTurn},
		{"tool_calls", StopReasonToolUse},
		{"length", StopReasonMaxTokens},
		{"unknown", StopReasonEndTurn},
	}
	for _, tt := range tests {
		got := mapOpenAIFinishReason(tt.reason)
		if got != tt.want {
			t.Errorf("mapOpenAIFinishReason(%q) = %q, want %q", tt.reason, got, tt.want)
		}
	}
}

func TestParseOpenAIResponse_NoChoices(t *testing.T) {
	t.Parallel()

	_, err := parseOpenAIResponse([]byte(`{"id":"test","choices":[]}`))
	if err == nil {
		t.Fatal("expected error for empty choices")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got: %T", err)
	}
}
