package main

import "strings"

func applyProviderBinding(providerID, modelOverride, fallbackGroup, fallbackBackend, fallbackModel string) (string, string, string) {
	providerID = strings.TrimSpace(providerID)
	modelOverride = strings.TrimSpace(modelOverride)
	groupID := strings.TrimSpace(fallbackGroup)
	backend := strings.TrimSpace(fallbackBackend)
	model := firstNonEmpty(modelOverride, fallbackModel)

	if provider, ok := FindProviderRecord(providerID); ok {
		if strings.TrimSpace(provider.GroupID) != "" {
			groupID = strings.TrimSpace(provider.GroupID)
		}
		model = firstNonEmpty(modelOverride, provider.DefaultModel, fallbackModel)
		if legacyBackend := legacyRuntimeBackendForProvider(provider); legacyBackend != "" {
			backend = legacyBackend
		}
	}

	return strings.TrimSpace(groupID), strings.TrimSpace(backend), strings.TrimSpace(model)
}

func legacyRuntimeBackendForProvider(provider ProviderRecord) string {
	provider = normalizeProviderRecord(provider)
	switch {
	case provider.ChannelType == providerChannelBridge && provider.Driver == llmBackendCodex:
		return llmBackendCodex
	case provider.ChannelType == providerChannelBridge && provider.Driver == llmBackendQwen:
		return llmBackendQwen
	case provider.ChannelType == providerChannelAPI && (provider.Driver == llmBackendOpenAI || provider.Driver == providerDriverOpenAICompatible || provider.Driver == providerDriverOpenRouter):
		return llmBackendOpenAI
	default:
		return ""
	}
}

func providerDisplayName(providerID string) string {
	if provider, ok := FindProviderRecord(providerID); ok {
		return firstNonEmpty(strings.TrimSpace(provider.GroupID), strings.TrimSpace(provider.Title), strings.TrimSpace(provider.ProviderID))
	}
	return strings.TrimSpace(providerID)
}
