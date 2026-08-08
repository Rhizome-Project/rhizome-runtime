package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type ChatLLM interface {
	Chat(ctx context.Context, messages []Message, tools []ToolDef) (*LLMResponse, error)
}

type OpenAILLM struct {
	apiKey        string
	model         string
	client        *http.Client
	endpointURL   string
	publicHeaders map[string]string
}

func NewOpenAILLM(apiKey, model string) *OpenAILLM {
	return NewOpenAILLMWithConfig(apiKey, model, "", nil)
}

func NewOpenAILLMWithConfig(apiKey, model, baseURL string, publicHeaders map[string]string) *OpenAILLM {
	return &OpenAILLM{
		apiKey:        apiKey,
		model:         model,
		client:        &http.Client{Timeout: 2 * time.Minute},
		endpointURL:   openAIChatCompletionsURL(baseURL),
		publicHeaders: cloneHeaderMap(publicHeaders),
	}
}

func openAIChatCompletionsURL(baseURL string) string {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return defaultOpenAIBaseURL + "/chat/completions"
	}
	return strings.TrimRight(baseURL, "/") + "/chat/completions"
}

func cloneHeaderMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Message types for OpenAI chat completions with function calling.

type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function FunctionCall `json:"function"`
}

type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type ToolDef struct {
	Type     string          `json:"type"`
	Function ToolFunctionDef `json:"function"`
}

type ToolFunctionDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

// Request/response types.

type chatRequest struct {
	Model               string    `json:"model"`
	Messages            []Message `json:"messages"`
	Tools               []ToolDef `json:"tools,omitempty"`
	MaxCompletionTokens int       `json:"max_completion_tokens,omitempty"`
	Temperature         float64   `json:"temperature"`
}

type TokenUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
	// CachedPromptTokens is the provider-reported cached-input portion of
	// PromptTokens (codex exec emits cached_input_tokens). Telemetry only
	// (TE-20 finding): budget/ledger math still bills full PromptTokens until
	// a priced cache policy is pinned.
	CachedPromptTokens int `json:"cached_prompt_tokens,omitempty"`
}

type chatResponse struct {
	Choices []chatChoice `json:"choices"`
	Usage   *TokenUsage  `json:"usage,omitempty"`
	Error   *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

type chatChoice struct {
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

// LLMResponse is what Chat returns.
type LLMResponse struct {
	Content   string
	ToolCalls []ToolCall
	Usage     TokenUsage
}

// Chat sends messages with optional tool definitions and returns the response.
func (l *OpenAILLM) Chat(ctx context.Context, messages []Message, tools []ToolDef) (*LLMResponse, error) {
	log.Printf("[llm] calling %s (%d messages, %d tools)", l.model, len(messages), len(tools))

	reqBody, _ := json.Marshal(chatRequest{
		Model:               l.model,
		Messages:            messages,
		Tools:               tools,
		MaxCompletionTokens: 16384,
		Temperature:         0.3,
	})

	// TE-02 prompt telemetry: emit per-call section bytes. The OpenAI backend
	// injects no static preamble (static_preamble=0); total is the byte length
	// of the marshaled request messages+tools actually sent on the wire.
	openAIMessagesTools, _ := json.Marshal(struct {
		Messages []Message `json:"messages"`
		Tools    []ToolDef `json:"tools,omitempty"`
	}{Messages: messages, Tools: tools})
	logPromptSectionBytes("openai", computePromptSectionBytes(messages, marshalToolsForTelemetry(tools), 0, len(openAIMessagesTools)))

	endpointURL := strings.TrimSpace(l.endpointURL)
	if endpointURL == "" {
		endpointURL = openAIChatCompletionsURL("")
	}
	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+l.apiKey)
	for key, value := range l.publicHeaders {
		if strings.EqualFold(strings.TrimSpace(key), "Authorization") || strings.EqualFold(strings.TrimSpace(key), "Content-Type") {
			continue
		}
		req.Header.Set(key, value)
	}

	start := time.Now()
	resp, err := l.client.Do(req)
	if err != nil {
		log.Printf("[llm] failed after %v: %v", time.Since(start), err)
		return nil, fmt.Errorf("openai: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	log.Printf("[llm] status=%d latency=%v", resp.StatusCode, time.Since(start))

	var result chatResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse: %w (body: %s)", err, body)
	}
	if result.Error != nil {
		log.Printf("[llm] error: %s", result.Error.Message)
		return nil, fmt.Errorf("openai: %s", result.Error.Message)
	}
	if len(result.Choices) == 0 {
		return nil, fmt.Errorf("no choices")
	}

	msg := result.Choices[0].Message
	if msg.Content != "" {
		log.Printf("[llm] response: %s", msg.Content)
	}
	if len(msg.ToolCalls) > 0 {
		for _, tc := range msg.ToolCalls {
			log.Printf("[llm] tool_call: %s(%s)", tc.Function.Name, tc.Function.Arguments)
		}
	}

	var usage TokenUsage
	if result.Usage != nil {
		usage = *result.Usage
		log.Printf("[llm] usage: prompt=%d completion=%d total=%d", usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens)
	}

	return &LLMResponse{
		Content:   msg.Content,
		ToolCalls: msg.ToolCalls,
		Usage:     usage,
	}, nil
}
