package main

import "testing"

func TestManagerSupportedProviderCatalogIncludesCodexBridge(t *testing.T) {
	catalog := managerSupportedProviderCatalog()
	if len(catalog) != 3 {
		t.Fatalf("expected three supported provider options, got %+v", catalog)
	}
	option := catalog[0]
	if option.ID != "codex_bridge" {
		t.Fatalf("expected codex_bridge option id, got %+v", option)
	}
	if option.ChannelType != providerChannelBridge || option.Driver != llmBackendCodex {
		t.Fatalf("expected codex bridge implementation, got %+v", option)
	}
	if option.SuggestedProviderID != "codex-bridge" {
		t.Fatalf("expected suggested provider id to be codex-bridge, got %+v", option)
	}

	openrouter := catalog[1]
	if openrouter.ID != "openrouter_api" {
		t.Fatalf("expected openrouter_api option id, got %+v", openrouter)
	}
	if openrouter.ChannelType != providerChannelAPI || openrouter.Driver != providerDriverOpenRouter {
		t.Fatalf("expected openrouter API implementation, got %+v", openrouter)
	}
	if openrouter.SuggestedProviderID != "openrouter" {
		t.Fatalf("expected suggested provider id to be openrouter, got %+v", openrouter)
	}

	qwen := catalog[2]
	if qwen.ID != "qwen_code_bridge" {
		t.Fatalf("expected qwen_code_bridge option id, got %+v", qwen)
	}
	if qwen.ChannelType != providerChannelBridge || qwen.Driver != llmBackendQwen {
		t.Fatalf("expected qwen bridge implementation, got %+v", qwen)
	}
	if qwen.SuggestedProviderID != "qwen-code" {
		t.Fatalf("expected suggested provider id to be qwen-code, got %+v", qwen)
	}
}

func TestFindSupportedProviderOptionFindsKnownID(t *testing.T) {
	option, ok := findSupportedProviderOption("codex_bridge")
	if !ok {
		t.Fatal("expected codex_bridge to be found")
	}
	if option.Driver != llmBackendCodex || option.ChannelType != providerChannelBridge {
		t.Fatalf("unexpected supported provider option: %+v", option)
	}
	option, ok = findSupportedProviderOption("qwen_code_bridge")
	if !ok {
		t.Fatal("expected qwen_code_bridge to be found")
	}
	if option.Driver != llmBackendQwen || option.ChannelType != providerChannelBridge {
		t.Fatalf("unexpected qwen provider option: %+v", option)
	}
}

func TestSupportedProviderOptionForRecordMatchesCodexBridge(t *testing.T) {
	option, ok := supportedProviderOptionForRecord(providerChannelBridge, llmBackendCodex)
	if !ok {
		t.Fatal("expected codex bridge implementation to resolve")
	}
	if option.ID != "codex_bridge" {
		t.Fatalf("expected codex_bridge match, got %+v", option)
	}
	if _, ok := supportedProviderOptionForRecord(providerChannelAPI, llmBackendOpenAI); ok {
		t.Fatal("did not expect openai api to appear in the supported provider catalog yet")
	}
	if option, ok := supportedProviderOptionForRecord(providerChannelAPI, providerDriverOpenRouter); !ok || option.ID != "openrouter_api" {
		t.Fatalf("expected openrouter API implementation to resolve, got %+v ok=%v", option, ok)
	}
	if option, ok := supportedProviderOptionForRecord(providerChannelBridge, llmBackendQwen); !ok || option.ID != "qwen_code_bridge" {
		t.Fatalf("expected qwen bridge implementation to resolve, got %+v ok=%v", option, ok)
	}
}
