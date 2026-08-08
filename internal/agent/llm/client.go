package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrBadRequest   = errors.New("bad request")
	ErrUnauthorized = errors.New("unauthorized")
	ErrAPIError     = errors.New("api error")
	ErrMaxRetries   = errors.New("max retries exceeded")
)

// Role represents the role of a message sender.
type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

// StopReason indicates why the model stopped generating.
type StopReason string

const (
	StopReasonEndTurn      StopReason = "end_turn"
	StopReasonToolUse      StopReason = "tool_use"
	StopReasonMaxTokens    StopReason = "max_tokens"
	StopReasonStopSequence StopReason = "stop_sequence"
)

// ContentBlock represents a single block in a message's content array.
type ContentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   string          `json:"content,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
}

// Message represents a single message in the conversation.
type Message struct {
	Role    Role           `json:"role"`
	Content []ContentBlock `json:"content"`
}

// HasToolUse returns true if any content block has type "tool_use".
func (m Message) HasToolUse() bool {
	for _, b := range m.Content {
		if b.Type == "tool_use" {
			return true
		}
	}
	return false
}

// ToolUseBlocks returns only the content blocks with type "tool_use".
func (m Message) ToolUseBlocks() []ContentBlock {
	var blocks []ContentBlock
	for _, b := range m.Content {
		if b.Type == "tool_use" {
			blocks = append(blocks, b)
		}
	}
	return blocks
}

// TextContent concatenates all text blocks' Text fields, separated by newlines.
func (m Message) TextContent() string {
	var parts []string
	for _, b := range m.Content {
		if b.Type == "text" && b.Text != "" {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// ToolResult represents the result of a tool execution.
type ToolResult struct {
	ToolUseID string `json:"tool_use_id"`
	Content   string `json:"content"`
	IsError   bool   `json:"is_error,omitempty"`
}

// Usage represents token usage from an LLM response.
type Usage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens,omitempty"`
}

// Response represents a parsed LLM response (provider-agnostic).
type Response struct {
	ID         string         `json:"id"`
	Model      string         `json:"model"`
	Role       Role           `json:"role"`
	Content    []ContentBlock `json:"content"`
	StopReason StopReason     `json:"stop_reason"`
	Usage      Usage          `json:"usage"`
}

// NewUserMessage creates a user message with a single text block.
func NewUserMessage(text string) Message {
	return Message{
		Role: RoleUser,
		Content: []ContentBlock{
			{Type: "text", Text: text},
		},
	}
}

// NewToolResultMessage creates a user message with tool_result blocks.
func NewToolResultMessage(results []ToolResult) Message {
	blocks := make([]ContentBlock, len(results))
	for i, r := range results {
		blocks[i] = ContentBlock{
			Type:      "tool_result",
			ToolUseID: r.ToolUseID,
			Content:   r.Content,
			IsError:   r.IsError,
		}
	}
	return Message{
		Role:    RoleUser,
		Content: blocks,
	}
}

// NewAssistantMessage creates an assistant message with the given content blocks.
func NewAssistantMessage(blocks []ContentBlock) Message {
	return Message{
		Role:    RoleAssistant,
		Content: blocks,
	}
}

// SendRequest holds the parameters for an LLM API call.
type SendRequest struct {
	System   string
	Messages []Message
	Tools    []map[string]any
}

// APIError represents an error from an LLM API.
type APIError struct {
	StatusCode int    `json:"status_code"`
	ErrorMsg   string `json:"message"`
	Type       string `json:"type"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("llm api error %d (%s): %s", e.StatusCode, e.Type, e.ErrorMsg)
}

func (e *APIError) Unwrap() error {
	switch e.StatusCode {
	case 400:
		return ErrBadRequest
	case 401:
		return ErrUnauthorized
	default:
		return ErrAPIError
	}
}

// Client wraps an LLM Provider with a unified interface.
// This is the type that the rest of the agent code uses.
type Client struct {
	provider Provider
}

// ClientConfig configures the LLM client.
type ClientConfig struct {
	Provider  ProviderType
	APIKey    string
	Model     string
	MaxTokens int
	Timeout   time.Duration
	BaseURL   string
	Headers   map[string]string // extra headers (e.g., for OpenRouter)
}

// NewClient creates a new LLM client with the appropriate provider.
func NewClient(cfg ClientConfig) *Client {
	var provider Provider

	switch cfg.Provider {
	case ProviderOpenAI:
		provider = NewOpenAIProvider(OpenAIConfig{
			APIKey:    cfg.APIKey,
			Model:     cfg.Model,
			MaxTokens: cfg.MaxTokens,
			Timeout:   cfg.Timeout,
			BaseURL:   cfg.BaseURL,
			Headers:   cfg.Headers,
		})
	default: // ProviderClaude or empty
		provider = NewClaudeProvider(ClaudeConfig{
			APIKey:    cfg.APIKey,
			Model:     cfg.Model,
			MaxTokens: cfg.MaxTokens,
			Timeout:   cfg.Timeout,
			BaseURL:   cfg.BaseURL,
		})
	}

	return &Client{provider: provider}
}

// NewClientWithProvider creates a client from an existing provider.
func NewClientWithProvider(provider Provider) *Client {
	return &Client{provider: provider}
}

// Send delegates to the underlying provider.
func (c *Client) Send(ctx context.Context, req SendRequest) (*Response, error) {
	return c.provider.Send(ctx, req)
}

// IsConfigured delegates to the underlying provider.
func (c *Client) IsConfigured() bool {
	return c.provider.IsConfigured()
}

// ProviderName returns the name of the underlying provider.
func (c *Client) ProviderName() string {
	return c.provider.Name()
}

// retryDelay calculates exponential backoff for retries.
func retryDelay(statusCode, attempt int, base429, base529 time.Duration) time.Duration {
	base := base429
	if statusCode == 529 {
		base = base529
	}
	return base * (1 << attempt)
}
