package llm

import "context"

// Provider is the interface for LLM API backends.
// Internal message format uses content blocks (tool_use, tool_result).
// Each provider converts to/from its wire format.
type Provider interface {
	// Send sends a conversation to the LLM and returns a parsed response.
	Send(ctx context.Context, req SendRequest) (*Response, error)
	// IsConfigured returns true if the provider has valid credentials.
	IsConfigured() bool
	// Name returns the provider identifier (e.g., "claude", "openai").
	Name() string
}

// ProviderType identifies the LLM provider backend.
type ProviderType string

const (
	ProviderClaude ProviderType = "claude"
	ProviderOpenAI ProviderType = "openai"
)
