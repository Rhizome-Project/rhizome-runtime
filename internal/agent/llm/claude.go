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

const anthropicVersion = "2023-06-01"

// ClaudeProvider implements the Anthropic Claude Messages API.
type ClaudeProvider struct {
	apiKey     string
	model      string
	maxTokens  int
	baseURL    string
	httpClient *http.Client
}

// ClaudeConfig configures the Claude provider.
type ClaudeConfig struct {
	APIKey    string
	Model     string
	MaxTokens int
	Timeout   time.Duration
	BaseURL   string
}

// NewClaudeProvider creates a new Claude API provider.
func NewClaudeProvider(cfg ClaudeConfig) *ClaudeProvider {
	if cfg.Model == "" {
		cfg.Model = "claude-sonnet-4-20250514"
	}
	if cfg.MaxTokens <= 0 {
		cfg.MaxTokens = 8192
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 120 * time.Second
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.anthropic.com"
	}
	return &ClaudeProvider{
		apiKey:    cfg.APIKey,
		model:     cfg.Model,
		maxTokens: cfg.MaxTokens,
		baseURL:   cfg.BaseURL,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
	}
}

func (p *ClaudeProvider) Name() string       { return "claude" }
func (p *ClaudeProvider) IsConfigured() bool { return p.apiKey != "" }

func (p *ClaudeProvider) Send(ctx context.Context, req SendRequest) (*Response, error) {
	if p.apiKey == "" {
		return nil, &APIError{StatusCode: 401, ErrorMsg: "API key not configured", Type: "authentication_error"}
	}

	body := p.buildRequestBody(req)
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

		if resp.StatusCode == 429 || resp.StatusCode == 529 {
			if attempt >= maxRetries {
				return nil, fmt.Errorf("%w: after %d attempts, last status %d", ErrMaxRetries, attempt+1, resp.StatusCode)
			}
			backoff := retryDelay(resp.StatusCode, attempt, time.Second, 2*time.Second)
			log.Printf("[agent-llm] retrying after %s (attempt %d/%d, status %d)", backoff, attempt+1, maxRetries, resp.StatusCode)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				continue
			}
		}

		if resp.StatusCode != 200 {
			return nil, parseClaudeError(resp.StatusCode, respBody)
		}

		var rawResp struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(respBody, &rawResp); err == nil && rawResp.Type == "error" {
			return nil, parseClaudeError(resp.StatusCode, respBody)
		}

		var apiResp Response
		if err := json.Unmarshal(respBody, &apiResp); err != nil {
			return nil, fmt.Errorf("unmarshal response: %w", err)
		}
		return &apiResp, nil
	}

	return nil, fmt.Errorf("%w: exhausted retries", ErrMaxRetries)
}

func (p *ClaudeProvider) buildRequestBody(req SendRequest) map[string]any {
	body := map[string]any{
		"model":      p.model,
		"max_tokens": p.maxTokens,
		"messages":   req.Messages,
	}
	if req.System != "" {
		body["system"] = req.System
	}
	if len(req.Tools) > 0 {
		body["tools"] = req.Tools
	}
	return body
}

func (p *ClaudeProvider) doRequest(ctx context.Context, bodyJSON []byte) (*http.Response, error) {
	url := p.baseURL + "/v1/messages"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", p.apiKey)
	httpReq.Header.Set("anthropic-version", anthropicVersion)
	return p.httpClient.Do(httpReq)
}

func parseClaudeError(statusCode int, body []byte) error {
	var errResp struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
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
