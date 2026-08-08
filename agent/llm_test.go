package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type captureTool struct {
	mu     sync.Mutex
	inputs []map[string]any
}

func (t *captureTool) Name() string { return "capture" }

func (t *captureTool) Description() string { return "capture tool inputs" }

func (t *captureTool) Parameters() map[string]any { return map[string]any{"type": "object"} }

func (t *captureTool) Execute(_ context.Context, args map[string]any) *ToolResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inputs = append(t.inputs, args)
	return &ToolResult{Output: fmt.Sprintf("captured:%v", args["value"])}
}

func TestLLMChatBuildsOpenAICompatibleRequest(t *testing.T) {
	var seen chatRequest

	llm := &OpenAILLM{
		apiKey: "test-key",
		model:  "gpt-test",
		client: &http.Client{
			Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodPost {
					t.Fatalf("method = %s", req.Method)
				}
				if req.URL.String() != "https://api.openai.com/v1/chat/completions" {
					t.Fatalf("unexpected URL: %s", req.URL.String())
				}
				if got := req.Header.Get("Authorization"); got != "Bearer test-key" {
					t.Fatalf("authorization header = %q", got)
				}

				body, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}
				if err := json.Unmarshal(body, &seen); err != nil {
					t.Fatalf("decode request body: %v", err)
				}

				respBody := `{"choices":[{"message":{"role":"assistant","content":"final answer"},"finish_reason":"stop"}]}`
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader(respBody)),
					Header:     make(http.Header),
				}, nil
			}),
		},
	}

	got, err := llm.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, []ToolDef{{Type: "function", Function: ToolFunctionDef{Name: "capture"}}})
	if err != nil {
		t.Fatalf("LLM.Chat() error = %v", err)
	}
	if got.Content != "final answer" {
		t.Fatalf("LLM.Chat() content = %q", got.Content)
	}
	if seen.Model != "gpt-test" {
		t.Fatalf("request model = %q", seen.Model)
	}
	if len(seen.Messages) != 1 || seen.Messages[0].Role != "user" || seen.Messages[0].Content != "hello" {
		t.Fatalf("request messages = %+v", seen.Messages)
	}
	if len(seen.Tools) != 1 || seen.Tools[0].Function.Name != "capture" {
		t.Fatalf("request tools = %+v", seen.Tools)
	}
}

func TestRunToolLoopExecutesToolCallAndReturnsFinalResponse(t *testing.T) {
	var calls int
	var sawToolResult bool
	capture := &captureTool{}

	llm := &OpenAILLM{
		apiKey: "test-key",
		model:  "gpt-test",
		client: &http.Client{
			Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}

				calls++
				switch calls {
				case 1:
					var decoded chatRequest
					if err := json.Unmarshal(body, &decoded); err != nil {
						t.Fatalf("decode first request: %v", err)
					}
					if decoded.Messages[0].Content != "start" {
						t.Fatalf("unexpected initial message: %+v", decoded.Messages)
					}
					respBody := `{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"call-1","type":"function","function":{"name":"capture","arguments":"{\"value\":\"abc\"}"}}]},"finish_reason":"tool_calls"}]}`
					return &http.Response{
						StatusCode: 200,
						Body:       io.NopCloser(strings.NewReader(respBody)),
						Header:     make(http.Header),
					}, nil
				case 2:
					if !bytes.Contains(body, []byte(`"role":"tool"`)) {
						t.Fatalf("second request did not include tool result: %s", string(body))
					}
					sawToolResult = true
					respBody := `{"choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}`
					return &http.Response{
						StatusCode: 200,
						Body:       io.NopCloser(strings.NewReader(respBody)),
						Header:     make(http.Header),
					}, nil
				default:
					t.Fatalf("unexpected request %d", calls)
					return nil, nil
				}
			}),
		},
	}

	reg := NewToolRegistry()
	reg.Register(capture)

	got, err := RunToolLoop(context.Background(), llm, reg, []Message{{Role: "user", Content: "start"}})
	if err != nil {
		t.Fatalf("RunToolLoop() error = %v", err)
	}
	if got != "done" {
		t.Fatalf("RunToolLoop() = %q", got)
	}
	if calls != 2 {
		t.Fatalf("expected 2 LLM calls, got %d", calls)
	}
	if !sawToolResult {
		t.Fatal("second request did not observe tool result payload")
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.inputs) != 1 || capture.inputs[0]["value"] != "abc" {
		t.Fatalf("tool inputs = %+v", capture.inputs)
	}
}

func TestCodeChallengeAndExtractEmail(t *testing.T) {
	if got := codeChallenge("verifier"); got != "iMnq5o6zALKXGivsnlom_0F5_WYda32GHkxlV7mq7hQ" {
		t.Fatalf("codeChallenge() = %q", got)
	}

	tokenPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"email":"agent@example.com"}`))
	idToken := "header." + tokenPayload + ".sig"
	if got := extractEmail(idToken); got != "agent@example.com" {
		t.Fatalf("extractEmail() = %q", got)
	}
}
