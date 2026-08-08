package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// OpenAIProvider implements the OpenAI-compatible Chat Completions API.
// Works with OpenAI, OpenRouter, Codex CLI, and any OpenAI-compatible endpoint.
type OpenAIProvider struct {
	apiKey     string
	model      string
	maxTokens  int
	baseURL    string
	httpClient *http.Client
	headers    map[string]string // extra headers (e.g., for OpenRouter)
}

// OpenAIConfig configures the OpenAI-compatible provider.
type OpenAIConfig struct {
	APIKey    string
	Model     string
	MaxTokens int
	Timeout   time.Duration
	BaseURL   string
	Headers   map[string]string // extra headers (e.g., "HTTP-Referer" for OpenRouter)
}

// NewOpenAIProvider creates a new OpenAI-compatible API provider.
func NewOpenAIProvider(cfg OpenAIConfig) *OpenAIProvider {
	if cfg.Model == "" {
		cfg.Model = "gpt-4o"
	}
	if cfg.MaxTokens <= 0 {
		cfg.MaxTokens = 8192
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 120 * time.Second
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.openai.com"
	}
	return &OpenAIProvider{
		apiKey:    cfg.APIKey,
		model:     cfg.Model,
		maxTokens: cfg.MaxTokens,
		baseURL:   cfg.BaseURL,
		headers:   cfg.Headers,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
	}
}

func (p *OpenAIProvider) Name() string       { return "openai" }
func (p *OpenAIProvider) IsConfigured() bool { return p.apiKey != "" }

func (p *OpenAIProvider) Send(ctx context.Context, req SendRequest) (*Response, error) {
	if p.apiKey == "" {
		return nil, &APIError{StatusCode: 401, ErrorMsg: "API key not configured", Type: "authentication_error"}
	}

	// Convert internal messages to OpenAI format
	oaiMessages := convertToOpenAIMessages(req.System, req.Messages)
	oaiTools := convertToOpenAITools(req.Tools)

	body := map[string]any{
		"model":      p.model,
		"max_tokens": p.maxTokens,
		"messages":   oaiMessages,
	}
	if len(oaiTools) > 0 {
		body["tools"] = oaiTools
	}

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	maxRetries := 3
	for attempt := 0; attempt <= maxRetries; attempt++ {
		resp, err := p.doRequest(ctx, bodyJSON)
		if err != nil {
			return nil, err
		}

		respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}

		if resp.StatusCode == 429 {
			if attempt >= maxRetries {
				return nil, fmt.Errorf("%w: after %d attempts, last status %d", ErrMaxRetries, attempt+1, resp.StatusCode)
			}
			backoff := retryDelay(resp.StatusCode, attempt, time.Second, time.Second)
			log.Printf("[agent-llm] retrying after %s (attempt %d/%d, status %d)", backoff, attempt+1, maxRetries, resp.StatusCode)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				continue
			}
		}

		if resp.StatusCode != 200 {
			return nil, parseOpenAIError(resp.StatusCode, respBody)
		}

		return parseOpenAIResponse(respBody)
	}

	return nil, fmt.Errorf("%w: exhausted retries", ErrMaxRetries)
}

func (p *OpenAIProvider) doRequest(ctx context.Context, bodyJSON []byte) (*http.Response, error) {
	url := p.baseURL + "/v1/chat/completions"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)
	for k, v := range p.headers {
		httpReq.Header.Set(k, v)
	}
	return p.httpClient.Do(httpReq)
}

// --- OpenAI message format conversion ---

type oaiMessage struct {
	Role       string        `json:"role"`
	Content    any           `json:"content,omitempty"`      // string or null
	ToolCalls  []oaiToolCall `json:"tool_calls,omitempty"`   // assistant only
	ToolCallID string        `json:"tool_call_id,omitempty"` // tool role only
}

type oaiToolCall struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Function oaiToolFunction `json:"function"`
}

type oaiToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type oaiTool struct {
	Type     string         `json:"type"`
	Function oaiToolFuncDef `json:"function"`
}

type oaiToolFuncDef struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"`
}

// convertToOpenAIMessages converts internal content-block messages to OpenAI format.
func convertToOpenAIMessages(system string, messages []Message) []oaiMessage {
	var out []oaiMessage

	// System message
	if system != "" {
		out = append(out, oaiMessage{Role: "system", Content: system})
	}

	for _, msg := range messages {
		switch msg.Role {
		case RoleUser:
			// Check if it's tool results
			if len(msg.Content) > 0 && msg.Content[0].Type == "tool_result" {
				for _, b := range msg.Content {
					out = append(out, oaiMessage{
						Role:       "tool",
						Content:    b.Content,
						ToolCallID: b.ToolUseID,
					})
				}
			} else {
				out = append(out, oaiMessage{
					Role:    "user",
					Content: msg.TextContent(),
				})
			}

		case RoleAssistant:
			oaiMsg := oaiMessage{Role: "assistant"}
			text := msg.TextContent()
			if text != "" {
				oaiMsg.Content = text
			}
			for _, b := range msg.ToolUseBlocks() {
				oaiMsg.ToolCalls = append(oaiMsg.ToolCalls, oaiToolCall{
					ID:   b.ID,
					Type: "function",
					Function: oaiToolFunction{
						Name:      b.Name,
						Arguments: string(b.Input),
					},
				})
			}
			out = append(out, oaiMsg)
		}
	}

	return out
}

// convertToOpenAITools converts Claude-format tool schemas to OpenAI format.
func convertToOpenAITools(tools []map[string]any) []oaiTool {
	if len(tools) == 0 {
		return nil
	}
	out := make([]oaiTool, len(tools))
	for i, t := range tools {
		name, _ := t["name"].(string)
		desc, _ := t["description"].(string)
		params := t["input_schema"]
		out[i] = oaiTool{
			Type: "function",
			Function: oaiToolFuncDef{
				Name:        name,
				Description: desc,
				Parameters:  params,
			},
		}
	}
	return out
}

// --- OpenAI response parsing ---

type oaiResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Role      string        `json:"role"`
			Content   *string       `json:"content"`
			ToolCalls []oaiToolCall `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// parseOpenAIResponse converts an OpenAI response to internal Response format.
func parseOpenAIResponse(body []byte) (*Response, error) {
	var oai oaiResponse
	if err := json.Unmarshal(body, &oai); err != nil {
		return nil, fmt.Errorf("unmarshal openai response: %w", err)
	}

	if len(oai.Choices) == 0 {
		return nil, &APIError{StatusCode: 200, ErrorMsg: "no choices in response", Type: "empty_response"}
	}

	choice := oai.Choices[0]
	var blocks []ContentBlock

	// Text content
	if choice.Message.Content != nil && *choice.Message.Content != "" {
		blocks = append(blocks, ContentBlock{
			Type: "text",
			Text: *choice.Message.Content,
		})
	}

	// Tool calls → tool_use blocks
	for _, tc := range choice.Message.ToolCalls {
		blocks = append(blocks, ContentBlock{
			Type:  "tool_use",
			ID:    tc.ID,
			Name:  tc.Function.Name,
			Input: json.RawMessage(tc.Function.Arguments),
		})
	}

	// Map finish_reason to StopReason
	stopReason := mapOpenAIFinishReason(choice.FinishReason)

	return &Response{
		ID:         oai.ID,
		Model:      oai.Model,
		Role:       RoleAssistant,
		Content:    blocks,
		StopReason: stopReason,
		Usage: Usage{
			InputTokens:  oai.Usage.PromptTokens,
			OutputTokens: oai.Usage.CompletionTokens,
		},
	}, nil
}

func mapOpenAIFinishReason(reason string) StopReason {
	switch reason {
	case "stop":
		return StopReasonEndTurn
	case "tool_calls":
		return StopReasonToolUse
	case "length":
		return StopReasonMaxTokens
	default:
		return StopReasonEndTurn
	}
}

func parseOpenAIError(statusCode int, body []byte) error {
	var errResp struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error.Message != "" {
		return &APIError{
			StatusCode: statusCode,
			ErrorMsg:   errResp.Error.Message,
			Type:       errResp.Error.Type,
		}
	}
	return &APIError{
		StatusCode: statusCode,
		ErrorMsg:   string(body),
		Type:       "unknown_error",
	}
}
