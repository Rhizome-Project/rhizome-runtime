package tools

import (
	"context"
	"encoding/json"
	"fmt"
)

type webSearchTool struct{}

func (t *webSearchTool) Name() string        { return "web_search" }
func (t *webSearchTool) Description() string { return "Search the web (stub implementation)" }

func (t *webSearchTool) Schema() Schema {
	return Schema{
		Type: "object",
		Properties: map[string]Property{
			"query":       {Type: "string", Description: "Search query"},
			"max_results": {Type: "integer", Description: "Maximum results (default: 5)"},
		},
		Required: []string{"query"},
	}
}

func (t *webSearchTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Query      string `json:"query"`
		MaxResults int    `json:"max_results"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("parse input: %w", err)
	}

	return fmt.Sprintf("WebSearch is not yet implemented. Query: %s", params.Query), nil
}

// RegisterBuiltins registers all 7 built-in tools with the given registry.
func RegisterBuiltins(reg *Registry, cfg BuiltinConfig) {
	reg.Register(WithCapabilityTier(&bashTool{cfg: cfg}, TierHighRisk, cfg))
	reg.Register(WithCapabilityTier(&readTool{cfg: cfg}, TierSafeLocal, cfg))
	reg.Register(WithCapabilityTier(&editTool{cfg: cfg}, TierHighRisk, cfg))
	reg.Register(WithCapabilityTier(&writeTool{cfg: cfg}, TierHighRisk, cfg))
	reg.Register(WithCapabilityTier(&globTool{cfg: cfg}, TierSafeLocal, cfg))
	reg.Register(WithCapabilityTier(&grepTool{cfg: cfg}, TierSafeLocal, cfg))
	reg.Register(WithCapabilityTier(&webSearchTool{}, TierSafeLocal, cfg))
}
