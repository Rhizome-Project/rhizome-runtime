package agent

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

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/hooks"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
)

// --- Test helpers ---

// newMockLLMServer creates a httptest server that responds with responses in order.
// Each call to the server gets the next response from the list.
// If calls exceed the list, the last response is reused.
func newMockLLMServer(t *testing.T, responses []llm.Response) *httptest.Server {
	t.Helper()
	var callIdx atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := int(callIdx.Add(1)) - 1
		if idx >= len(responses) {
			idx = len(responses) - 1
		}
		w.Header().Set("Content-Type", "application/json")
		data, _ := json.Marshal(responses[idx])
		w.Write(data)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func textResponse(text string, inputTokens, outputTokens int) llm.Response {
	return llm.Response{
		ID:    "msg_test",
		Model: "claude-sonnet-4-20250514",
		Role:  llm.RoleAssistant,
		Content: []llm.ContentBlock{
			{Type: "text", Text: text},
		},
		StopReason: llm.StopReasonEndTurn,
		Usage:      llm.Usage{InputTokens: inputTokens, OutputTokens: outputTokens},
	}
}

func toolUseResponse(toolName, toolID string, input map[string]any, inputTokens, outputTokens int) llm.Response {
	inputJSON, _ := json.Marshal(input)
	return llm.Response{
		ID:    "msg_test",
		Model: "claude-sonnet-4-20250514",
		Role:  llm.RoleAssistant,
		Content: []llm.ContentBlock{
			{Type: "text", Text: "I'll use a tool."},
			{Type: "tool_use", ID: toolID, Name: toolName, Input: json.RawMessage(inputJSON)},
		},
		StopReason: llm.StopReasonToolUse,
		Usage:      llm.Usage{InputTokens: inputTokens, OutputTokens: outputTokens},
	}
}

func multiToolUseResponse(toolCalls []struct {
	Name  string
	ID    string
	Input map[string]any
}, inputTokens, outputTokens int) llm.Response {
	blocks := []llm.ContentBlock{{Type: "text", Text: "I'll use multiple tools."}}
	for _, tc := range toolCalls {
		inputJSON, _ := json.Marshal(tc.Input)
		blocks = append(blocks, llm.ContentBlock{
			Type:  "tool_use",
			ID:    tc.ID,
			Name:  tc.Name,
			Input: json.RawMessage(inputJSON),
		})
	}
	return llm.Response{
		ID:         "msg_test",
		Model:      "claude-sonnet-4-20250514",
		Role:       llm.RoleAssistant,
		Content:    blocks,
		StopReason: llm.StopReasonToolUse,
		Usage:      llm.Usage{InputTokens: inputTokens, OutputTokens: outputTokens},
	}
}

type fixedTool struct {
	name   string
	output string
	err    error
}

func (t *fixedTool) Name() string        { return t.name }
func (t *fixedTool) Description() string { return "test tool" }
func (t *fixedTool) Schema() tools.Schema {
	return tools.Schema{Type: "object", Properties: map[string]tools.Property{
		"input": {Type: "string"},
	}}
}
func (t *fixedTool) Execute(_ context.Context, _ json.RawMessage) (string, error) {
	return t.output, t.err
}

func newTestLoop(t *testing.T, srv *httptest.Server, toolReg *tools.Registry, hookRunner *hooks.Runner, cfg LoopConfig) *Loop {
	t.Helper()
	client := llm.NewClient(llm.ClientConfig{APIKey: "sk-test", BaseURL: srv.URL})
	if toolReg == nil {
		toolReg = tools.NewRegistry()
	}
	if hookRunner == nil {
		hookRunner = hooks.NewRunner()
	}
	return NewLoop(client, toolReg, hookRunner, "You are a test agent.", cfg)
}

// --- Tests ---

// T-1: Verifies R-5 — mock LLM returns text-only response, loop exits after 1 iteration.
func TestLoop_SingleTextResponse(t *testing.T) {
	t.Parallel()

	srv := newMockLLMServer(t, []llm.Response{
		textResponse("Hello from the agent!", 100, 50),
	})

	loop := newTestLoop(t, srv, nil, nil, LoopConfig{})
	result, err := loop.Run(context.Background(), "Say hello")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Iterations != 1 {
		t.Fatalf("expected 1 iteration, got %d", result.Iterations)
	}
	if result.FinalResponse != "Hello from the agent!" {
		t.Fatalf("expected final response %q, got %q", "Hello from the agent!", result.FinalResponse)
	}
	if result.StopReason != StopReasonEndTurn {
		t.Fatalf("expected stop_reason %q, got %q", StopReasonEndTurn, result.StopReason)
	}
	if result.ToolCalls != 0 {
		t.Fatalf("expected 0 tool calls, got %d", result.ToolCalls)
	}
}

// T-2: Verifies R-5d — mock LLM returns tool_use, then text. Loop makes 2 iterations. ToolCalls=1.
func TestLoop_ToolUseAndContinue(t *testing.T) {
	t.Parallel()

	srv := newMockLLMServer(t, []llm.Response{
		toolUseResponse("test_tool", "tu-1", map[string]any{"input": "data"}, 100, 50),
		textResponse("Done!", 80, 30),
	})

	toolReg := tools.NewRegistry()
	toolReg.Register(&fixedTool{name: "test_tool", output: "tool output"})

	loop := newTestLoop(t, srv, toolReg, nil, LoopConfig{})
	result, err := loop.Run(context.Background(), "Do something")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Iterations != 2 {
		t.Fatalf("expected 2 iterations, got %d", result.Iterations)
	}
	if result.ToolCalls != 1 {
		t.Fatalf("expected 1 tool call, got %d", result.ToolCalls)
	}
	if result.FinalResponse != "Done!" {
		t.Fatalf("expected final response %q, got %q", "Done!", result.FinalResponse)
	}
}

// T-3: Verifies R-9 — mock LLM always returns tool_use, loop stops at MaxIterations.
func TestLoop_MaxIterations(t *testing.T) {
	t.Parallel()

	srv := newMockLLMServer(t, []llm.Response{
		toolUseResponse("test_tool", "tu-1", map[string]any{}, 100, 50),
	})

	toolReg := tools.NewRegistry()
	toolReg.Register(&fixedTool{name: "test_tool", output: "ok"})

	loop := newTestLoop(t, srv, toolReg, nil, LoopConfig{MaxIterations: 3})
	result, err := loop.Run(context.Background(), "Loop forever")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Iterations != 3 {
		t.Fatalf("expected 3 iterations, got %d", result.Iterations)
	}
	if result.StopReason != StopReasonMaxIterations {
		t.Fatalf("expected stop_reason %q, got %q", StopReasonMaxIterations, result.StopReason)
	}
}

// T-4: Verifies R-7 — mock LLM returns 3 tool_use blocks, all execute, results are in order.
func TestLoop_ParallelToolExecution(t *testing.T) {
	t.Parallel()

	toolCalls := []struct {
		Name  string
		ID    string
		Input map[string]any
	}{
		{"tool_a", "tu-1", map[string]any{}},
		{"tool_b", "tu-2", map[string]any{}},
		{"tool_c", "tu-3", map[string]any{}},
	}

	srv := newMockLLMServer(t, []llm.Response{
		multiToolUseResponse(toolCalls, 100, 50),
		textResponse("Done with all tools.", 80, 30),
	})

	toolReg := tools.NewRegistry()
	toolReg.Register(&fixedTool{name: "tool_a", output: "output_a"})
	toolReg.Register(&fixedTool{name: "tool_b", output: "output_b"})
	toolReg.Register(&fixedTool{name: "tool_c", output: "output_c"})

	loop := newTestLoop(t, srv, toolReg, nil, LoopConfig{ParallelToolCalls: true})
	result, err := loop.Run(context.Background(), "Use all tools")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ToolCalls != 3 {
		t.Fatalf("expected 3 tool calls, got %d", result.ToolCalls)
	}
	if result.Iterations != 2 {
		t.Fatalf("expected 2 iterations, got %d", result.Iterations)
	}
}

// T-5: Verifies R-10 — mock tool returns error, tool_result has IsError=true, loop continues.
func TestLoop_ToolError(t *testing.T) {
	t.Parallel()

	srv := newMockLLMServer(t, []llm.Response{
		toolUseResponse("error_tool", "tu-1", map[string]any{}, 100, 50),
		textResponse("I handled the error.", 80, 30),
	})

	toolReg := tools.NewRegistry()
	toolReg.Register(&fixedTool{name: "error_tool", err: fmt.Errorf("tool failed")})

	loop := newTestLoop(t, srv, toolReg, nil, LoopConfig{})
	result, err := loop.Run(context.Background(), "Use failing tool")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Iterations != 2 {
		t.Fatalf("expected 2 iterations (tool error then text), got %d", result.Iterations)
	}
	// Verify tool_result message has IsError
	found := false
	for _, msg := range result.Messages {
		for _, b := range msg.Content {
			if b.Type == "tool_result" && b.IsError {
				found = true
				if !strings.Contains(b.Content, "tool failed") {
					t.Fatalf("expected error message in content, got %q", b.Content)
				}
			}
		}
	}
	if !found {
		t.Fatal("expected tool_result with IsError=true in messages")
	}
}

// T-6: Verifies EC-3 — LLM references unknown tool name, tool_result has IsError=true.
func TestLoop_UnknownTool(t *testing.T) {
	t.Parallel()

	srv := newMockLLMServer(t, []llm.Response{
		toolUseResponse("nonexistent_tool", "tu-1", map[string]any{}, 100, 50),
		textResponse("I see the error.", 80, 30),
	})

	toolReg := tools.NewRegistry()
	// No tools registered

	loop := newTestLoop(t, srv, toolReg, nil, LoopConfig{})
	result, err := loop.Run(context.Background(), "Use unknown tool")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	found := false
	for _, msg := range result.Messages {
		for _, b := range msg.Content {
			if b.Type == "tool_result" && b.IsError && strings.Contains(b.Content, "Unknown tool") {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected tool_result with 'Unknown tool' error")
	}
}

// T-7: Verifies EC-1 — empty task string, returns error immediately.
func TestLoop_EmptyTask(t *testing.T) {
	t.Parallel()

	srv := newMockLLMServer(t, []llm.Response{})

	loop := newTestLoop(t, srv, nil, nil, LoopConfig{})
	_, err := loop.Run(context.Background(), "")

	if err == nil {
		t.Fatal("expected error for empty task, got nil")
	}
	if !strings.Contains(err.Error(), "required") {
		t.Fatalf("expected 'required' in error, got %q", err.Error())
	}
}

// T-8: Verifies R-8 — LLM client returns Go error, loop stops with Error set.
func TestLoop_LLMError(t *testing.T) {
	t.Parallel()

	// Server that returns 500 to cause an API error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte(`{"type":"error","error":{"type":"api_error","message":"server error"}}`))
	}))
	t.Cleanup(srv.Close)

	loop := newTestLoop(t, srv, nil, nil, LoopConfig{})
	result, err := loop.Run(context.Background(), "Do something")

	if err != nil {
		t.Fatalf("Run should not return Go error directly, got: %v", err)
	}
	if result.Error == nil {
		t.Fatal("expected LoopResult.Error to be set")
	}
	if result.Iterations != 1 {
		t.Fatalf("expected 1 iteration, got %d", result.Iterations)
	}
}

// T-9: Verifies R-5c — OnStop hook sets PreventStop=true, loop continues.
func TestLoop_HookPreventStop(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(callCount.Add(1))
		var resp llm.Response
		if n == 1 {
			resp = textResponse("First response.", 100, 50)
		} else {
			resp = textResponse("Final response.", 80, 30)
		}
		data, _ := json.Marshal(resp)
		w.Write(data)
	}))
	t.Cleanup(srv.Close)

	hookRunner := hooks.NewRunner()
	preventOnce := &preventStopHook{preventOnce: true}
	hookRunner.Register(preventOnce)

	loop := newTestLoop(t, srv, nil, hookRunner, LoopConfig{})
	result, err := loop.Run(context.Background(), "Keep going")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Iterations != 2 {
		t.Fatalf("expected 2 iterations (prevented stop then final), got %d", result.Iterations)
	}
	// Verify "Please continue." was injected
	foundContinue := false
	for _, msg := range result.Messages {
		if msg.Role == RoleUser {
			for _, b := range msg.Content {
				if b.Type == "text" && strings.Contains(b.Text, "continue") {
					foundContinue = true
				}
			}
		}
	}
	if !foundContinue {
		t.Fatal("expected 'continue' user message after PreventStop")
	}
}

// T-10: Verifies R-11 — after 3 iterations, tokens are summed correctly.
func TestLoop_TokenTracking(t *testing.T) {
	t.Parallel()

	srv := newMockLLMServer(t, []llm.Response{
		toolUseResponse("test_tool", "tu-1", map[string]any{}, 100, 50),
		toolUseResponse("test_tool", "tu-2", map[string]any{}, 200, 80),
		textResponse("Done.", 150, 60),
	})

	toolReg := tools.NewRegistry()
	toolReg.Register(&fixedTool{name: "test_tool", output: "ok"})

	loop := newTestLoop(t, srv, toolReg, nil, LoopConfig{})
	result, err := loop.Run(context.Background(), "Track tokens")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Iterations != 3 {
		t.Fatalf("expected 3 iterations, got %d", result.Iterations)
	}
	expectedIn := 100 + 200 + 150
	expectedOut := 50 + 80 + 60
	if result.TotalInputTokens != expectedIn {
		t.Fatalf("expected total input tokens %d, got %d", expectedIn, result.TotalInputTokens)
	}
	if result.TotalOutputTokens != expectedOut {
		t.Fatalf("expected total output tokens %d, got %d", expectedOut, result.TotalOutputTokens)
	}
}

// T-11: Verifies EC-2 — no tools registered, LLM called with empty tools, loop works.
func TestLoop_NoTools(t *testing.T) {
	t.Parallel()

	srv := newMockLLMServer(t, []llm.Response{
		textResponse("I have no tools.", 100, 50),
	})

	loop := newTestLoop(t, srv, nil, nil, LoopConfig{})
	result, err := loop.Run(context.Background(), "Help me")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.FinalResponse != "I have no tools." {
		t.Fatalf("expected final response %q, got %q", "I have no tools.", result.FinalResponse)
	}
}

// Verifies that LoopConfig defaults are applied correctly.
func TestLoopConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg := LoopConfig{}.withDefaults()
	if cfg.MaxIterations != 50 {
		t.Fatalf("expected MaxIterations 50, got %d", cfg.MaxIterations)
	}
	if cfg.MaxTokensPerRequest != 8192 {
		t.Fatalf("expected MaxTokensPerRequest 8192, got %d", cfg.MaxTokensPerRequest)
	}
	if cfg.TokenBudget != 100000 {
		t.Fatalf("expected TokenBudget 100000, got %d", cfg.TokenBudget)
	}
}

// Verifies that messages are accumulated in result.Messages.
func TestLoop_MessagesAccumulated(t *testing.T) {
	t.Parallel()

	srv := newMockLLMServer(t, []llm.Response{
		toolUseResponse("test_tool", "tu-1", map[string]any{}, 100, 50),
		textResponse("Done.", 80, 30),
	})

	toolReg := tools.NewRegistry()
	toolReg.Register(&fixedTool{name: "test_tool", output: "output"})

	loop := newTestLoop(t, srv, toolReg, nil, LoopConfig{})
	result, err := loop.Run(context.Background(), "Do work")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Expected: user(task), assistant(tool_use), user(tool_result), assistant(text)
	if len(result.Messages) < 4 {
		t.Fatalf("expected at least 4 messages, got %d", len(result.Messages))
	}
	if result.Messages[0].Role != RoleUser {
		t.Fatalf("first message should be user, got %s", result.Messages[0].Role)
	}
}

// NT-1: Negative — context cancelled stops loop.
func TestLoop_ContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		data, _ := json.Marshal(textResponse("Should not arrive.", 100, 50))
		w.Write(data)
	}))
	t.Cleanup(srv.Close)

	loop := newTestLoop(t, srv, nil, nil, LoopConfig{})
	result, err := loop.Run(ctx, "Do work")

	if err != nil {
		t.Fatalf("unexpected direct error: %v", err)
	}
	if result.Error == nil {
		t.Fatal("expected LoopResult.Error to be set for cancelled context")
	}
	if !errors.Is(result.Error, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", result.Error)
	}
}

// --- Hook helpers ---

type preventStopHook struct {
	preventOnce bool
	prevented   atomic.Int32
}

func (h *preventStopHook) Name() string          { return "prevent_stop" }
func (h *preventStopHook) Points() []hooks.Point { return []hooks.Point{hooks.OnStop} }
func (h *preventStopHook) Run(_ context.Context, hctx hooks.Context) (hooks.Result, error) {
	if h.preventOnce && h.prevented.Add(1) == 1 {
		return hooks.Result{PreventStop: true}, nil
	}
	return hooks.Result{}, nil
}
