package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestNewLLMUsesProviderConfiguredOpenAIEndpointAndHeaders(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:   "openai-main",
		ChannelType:  providerChannelAPI,
		Driver:       llmBackendOpenAI,
		GroupID:      "group-openai-main",
		DefaultModel: "gpt-5.4",
		Enabled:      true,
		API: ProviderAPIConfig{
			BaseURL: "https://example.test/v1",
			PublicHeaders: map[string]string{
				"X-Env": "prod",
			},
		},
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	llm, err := NewLLM(RuntimeConfig{
		ProviderID: "openai-main",
		LLMBackend: llmBackendOpenAI,
		Model:      "gpt-5.4-mini",
		OpenAIKey:  "test-key",
		Workdir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewLLM() error: %v", err)
	}

	openaiLLM, ok := llm.(*OpenAILLM)
	if !ok {
		t.Fatalf("expected OpenAILLM, got %T", llm)
	}
	if openaiLLM.endpointURL != "https://example.test/v1/chat/completions" {
		t.Fatalf("expected provider endpoint, got %q", openaiLLM.endpointURL)
	}
	if openaiLLM.publicHeaders["X-Env"] != "prod" {
		t.Fatalf("expected provider headers, got %+v", openaiLLM.publicHeaders)
	}
}

func TestNewLLMUsesOpenRouterProviderDefaultsAndEnvKey(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("OPENAI_API_KEY", "wrong-openai-key")
	t.Setenv("OPENROUTER_API_KEY", "openrouter-key")

	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:   "openrouter",
		ChannelType:  providerChannelAPI,
		Driver:       providerDriverOpenRouter,
		GroupID:      "openrouter",
		DefaultModel: "qwen/qwen3-coder",
		Enabled:      true,
		API: ProviderAPIConfig{
			PublicHeaders: map[string]string{
				"HTTP-Referer": "https://rhizome.local",
				"X-Title":      "Rhizome",
			},
		},
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	cfg := RuntimeConfig{
		ProviderID: "openrouter",
		Workdir:    t.TempDir(),
	}
	cfg.ApplyDefaults()
	llm, err := NewLLM(cfg)
	if err != nil {
		t.Fatalf("NewLLM() error: %v", err)
	}

	openaiLLM, ok := llm.(*OpenAILLM)
	if !ok {
		t.Fatalf("expected OpenAILLM, got %T", llm)
	}
	if openaiLLM.apiKey != "openrouter-key" {
		t.Fatalf("expected OPENROUTER_API_KEY to win, got %q", openaiLLM.apiKey)
	}
	if openaiLLM.endpointURL != "https://openrouter.ai/api/v1/chat/completions" {
		t.Fatalf("expected openrouter endpoint, got %q", openaiLLM.endpointURL)
	}
	if openaiLLM.model != "qwen/qwen3-coder" {
		t.Fatalf("expected provider default model, got %q", openaiLLM.model)
	}
	if openaiLLM.publicHeaders["HTTP-Referer"] != "https://rhizome.local" || openaiLLM.publicHeaders["X-Title"] != "Rhizome" {
		t.Fatalf("expected openrouter public headers, got %+v", openaiLLM.publicHeaders)
	}
}

func TestNewLLMRejectsOpenRouterWithoutProviderKey(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("OPENAI_API_KEY", "wrong-openai-key")
	t.Setenv("OPENROUTER_API_KEY", "")

	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "openrouter",
		ChannelType: providerChannelAPI,
		Driver:      providerDriverOpenRouter,
		GroupID:     "openrouter",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	cfg := RuntimeConfig{
		ProviderID: "openrouter",
		Model:      "qwen/qwen3-coder",
		Workdir:    t.TempDir(),
	}
	cfg.ApplyDefaults()
	_, err := NewLLM(cfg)
	if err == nil {
		t.Fatal("expected missing OpenRouter credential to fail")
	}
	if !strings.Contains(err.Error(), "OPENROUTER_API_KEY") {
		t.Fatalf("expected OpenRouter env hint, got %v", err)
	}
}

func TestNewLLMUsesProviderConfiguredCodexExecutable(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "codex-bridge",
		ChannelType: providerChannelBridge,
		Driver:      llmBackendCodex,
		GroupID:     "group-codex",
		Enabled:     true,
		Bridge: ProviderBridgeConfig{
			Executable: "custom-codex",
		},
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	llm, err := NewLLM(RuntimeConfig{
		ProviderID: "codex-bridge",
		LLMBackend: llmBackendCodex,
		Model:      "gpt-5.4",
		Workdir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewLLM() error: %v", err)
	}

	codexLLM, ok := llm.(*CodexExecLLM)
	if !ok {
		t.Fatalf("expected CodexExecLLM, got %T", llm)
	}
	if codexLLM.executablePath != "custom-codex" {
		t.Fatalf("expected provider executable, got %q", codexLLM.executablePath)
	}
}

func TestNewLLMRejectsDisabledProvider(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "openai-disabled",
			ChannelType: providerChannelAPI,
			Driver:      llmBackendOpenAI,
			GroupID:     "group-openai-disabled",
			Enabled:     false,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	_, err := NewLLM(RuntimeConfig{
		ProviderID: "openai-disabled",
		LLMBackend: llmBackendOpenAI,
		Model:      "gpt-5.4-mini",
		OpenAIKey:  "test-key",
		Workdir:    t.TempDir(),
	})
	if !errors.Is(err, errProviderDisabled) {
		t.Fatalf("expected disabled provider error, got %v", err)
	}
}

func TestNewLLMCarriesProviderBridgeCommandArgs(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "codex-bridge",
		ChannelType: providerChannelBridge,
		Driver:      llmBackendCodex,
		GroupID:     "group-codex",
		Enabled:     true,
		Bridge: ProviderBridgeConfig{
			Command: "custom-codex --profile prod --json",
		},
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	llm, err := NewLLM(RuntimeConfig{
		ProviderID: "codex-bridge",
		LLMBackend: llmBackendCodex,
		Model:      "gpt-5.4",
		Workdir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewLLM() error: %v", err)
	}

	codexLLM, ok := llm.(*CodexExecLLM)
	if !ok {
		t.Fatalf("expected CodexExecLLM, got %T", llm)
	}
	if codexLLM.executablePath != "custom-codex" {
		t.Fatalf("expected bridge executable from command, got %q", codexLLM.executablePath)
	}
	if !reflect.DeepEqual(codexLLM.baseArgs, []string{"--profile", "prod", "--json"}) {
		t.Fatalf("expected bridge args to survive, got %+v", codexLLM.baseArgs)
	}
}

func TestNewLLMUsesProviderConfiguredQwenCommand(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "qwen-code",
		ChannelType: providerChannelBridge,
		Driver:      llmBackendQwen,
		GroupID:     "group-qwen",
		Enabled:     true,
		Bridge: ProviderBridgeConfig{
			Command: "custom-qwen --profile prod",
		},
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	llm, err := NewLLM(RuntimeConfig{
		ProviderID: "qwen-code",
		LLMBackend: llmBackendQwen,
		Model:      "qwen3-coder-plus",
		Workdir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewLLM() error: %v", err)
	}

	qwenLLM, ok := llm.(*QwenExecLLM)
	if !ok {
		t.Fatalf("expected QwenExecLLM, got %T", llm)
	}
	if qwenLLM.executablePath != "custom-qwen" {
		t.Fatalf("expected bridge executable from command, got %q", qwenLLM.executablePath)
	}
	if !reflect.DeepEqual(qwenLLM.baseArgs, []string{"--profile", "prod"}) {
		t.Fatalf("expected bridge args to survive, got %+v", qwenLLM.baseArgs)
	}
}

func TestNewLLMUsesScriptedFakeBackendWithoutProviderRegistry(t *testing.T) {
	llm, err := NewLLM(RuntimeConfig{
		ProviderID: "fake",
		LLMBackend: llmBackendFake,
		Model:      "normal_complete",
	})
	if err != nil {
		t.Fatalf("NewLLM() error: %v", err)
	}

	resp, err := llm.Chat(context.Background(), []Message{{Role: "user", Content: "run"}}, nil)
	if err != nil {
		t.Fatalf("Chat() error: %v", err)
	}
	if resp.Content == "" || len(resp.ToolCalls) != 0 {
		t.Fatalf("expected self-contained scripted fake response, got %+v", resp)
	}
}
