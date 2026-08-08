package main

import "strings"

type SupportedProviderOption struct {
	ID                  string `json:"id"`
	Label               string `json:"label"`
	Description         string `json:"description,omitempty"`
	ChannelType         string `json:"channel_type"`
	Driver              string `json:"driver"`
	SuggestedProviderID string `json:"suggested_provider_id,omitempty"`
	SuggestedTitle      string `json:"suggested_title,omitempty"`
}

func managerSupportedProviderCatalog() []SupportedProviderOption {
	return []SupportedProviderOption{
		{
			ID:                  "codex_bridge",
			Label:               "Codex Bridge",
			Description:         "Use the local Codex CLI bridge for agent runtime execution.",
			ChannelType:         providerChannelBridge,
			Driver:              llmBackendCodex,
			SuggestedProviderID: "codex-bridge",
			SuggestedTitle:      "Codex Bridge",
		},
		{
			ID:                  "openrouter_api",
			Label:               "OpenRouter API",
			Description:         "Use OpenRouter through its OpenAI-compatible chat completions API. Set OPENROUTER_API_KEY in the runtime environment.",
			ChannelType:         providerChannelAPI,
			Driver:              providerDriverOpenRouter,
			SuggestedProviderID: "openrouter",
			SuggestedTitle:      "OpenRouter",
		},
		{
			ID:                  "qwen_code_bridge",
			Label:               "Qwen Code Bridge",
			Description:         "Use the local Qwen Code CLI in headless mode while routing shell and file actions through the outer runtime tools.",
			ChannelType:         providerChannelBridge,
			Driver:              llmBackendQwen,
			SuggestedProviderID: "qwen-code",
			SuggestedTitle:      "Qwen Code",
		},
	}
}

func findSupportedProviderOption(id string) (SupportedProviderOption, bool) {
	id = strings.TrimSpace(id)
	if id == "" {
		return SupportedProviderOption{}, false
	}
	for _, option := range managerSupportedProviderCatalog() {
		if strings.EqualFold(strings.TrimSpace(option.ID), id) {
			return option, true
		}
	}
	return SupportedProviderOption{}, false
}

func supportedProviderOptionForRecord(channelType, driver string) (SupportedProviderOption, bool) {
	channelType = strings.TrimSpace(strings.ToLower(channelType))
	driver = strings.TrimSpace(strings.ToLower(driver))
	if channelType == "" || driver == "" {
		return SupportedProviderOption{}, false
	}
	for _, option := range managerSupportedProviderCatalog() {
		if strings.EqualFold(strings.TrimSpace(option.ChannelType), channelType) &&
			strings.EqualFold(strings.TrimSpace(option.Driver), driver) {
			return option, true
		}
	}
	return SupportedProviderOption{}, false
}
