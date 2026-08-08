package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
)

func explicitOrEnvAPIKey(explicit string) string {
	apiKey := strings.TrimSpace(explicit)
	if apiKey == "" {
		apiKey = strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	}
	return apiKey
}

func explicitOrProviderEnvAPIKey(explicit string, provider ProviderRecord) string {
	apiKey := strings.TrimSpace(explicit)
	if apiKey != "" {
		return apiKey
	}
	for _, name := range providerAPIKeyEnvNames(provider) {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	if providerUsesOpenAIKeyFallback(provider) {
		return strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	}
	return ""
}

func providerAPIKeyEnvNames(provider ProviderRecord) []string {
	provider = normalizeProviderRecord(provider)
	names := make([]string, 0, 4)
	if provider.ProviderID != "" {
		names = append(names, "RHIZOME_AGENT_PROVIDER_"+providerEnvNameComponent(provider.ProviderID)+"_API_KEY")
	}
	if provider.Driver == providerDriverOpenRouter || strings.Contains(strings.ToLower(provider.API.BaseURL), "openrouter.ai") {
		names = append(names, "OPENROUTER_API_KEY")
	}
	names = append(names, "RHIZOME_AGENT_API_KEY")
	return uniqueTrimmedCSVStrings(names)
}

func providerEnvNameComponent(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	value = regexp.MustCompile(`[^A-Z0-9]+`).ReplaceAllString(value, "_")
	return strings.Trim(value, "_")
}

func providerUsesOpenAIKeyFallback(provider ProviderRecord) bool {
	provider = normalizeProviderRecord(provider)
	return provider.ProviderID == "" || provider.Driver == "" || provider.Driver == llmBackendOpenAI
}

func configuredAPIKey(explicit string) string {
	apiKey := explicitOrEnvAPIKey(explicit)
	if apiKey == "" {
		apiKey = strings.TrimSpace(LoadSavedKey())
	}
	return apiKey
}

func configuredAPIKeyForProvider(explicit string, provider ProviderRecord) string {
	apiKey := explicitOrProviderEnvAPIKey(explicit, provider)
	if apiKey == "" && providerUsesOpenAIKeyFallback(provider) {
		apiKey = strings.TrimSpace(LoadSavedKey())
	}
	return apiKey
}

func loadAPIKey(explicit string, provider ProviderRecord) (string, error) {
	apiKey := configuredAPIKeyForProvider(explicit, provider)
	if apiKey != "" {
		return apiKey, nil
	}
	if isPartnerManagedRuntime() {
		return "", fmt.Errorf("managed partner runtime requires explicit local %s credential", providerCredentialLabel(provider))
	}
	if !providerUsesOpenAIKeyFallback(provider) {
		return "", fmt.Errorf("%s credential not configured; set %s", providerCredentialLabel(provider), strings.Join(providerAPIKeyEnvNames(provider), " or "))
	}

	log.Println("[auth] No OpenAI key found, starting browser auth...")
	fetched, err := RunBrowserAuth()
	if err != nil {
		return "", fmt.Errorf("openai auth failed: %w", err)
	}
	if err := SaveKey(fetched); err != nil {
		log.Printf("[auth] warning: could not save key: %v", err)
	}
	log.Println("[auth] OpenAI key obtained and saved")
	return fetched, nil
}

func providerCredentialLabel(provider ProviderRecord) string {
	provider = normalizeProviderRecord(provider)
	switch provider.Driver {
	case providerDriverOpenRouter:
		return "OpenRouter"
	case providerDriverOpenAICompatible:
		return "OpenAI-compatible provider"
	default:
		return "OpenAI"
	}
}
