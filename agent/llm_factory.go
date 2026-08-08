package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

const (
	llmBackendAuto   = "auto"
	llmBackendOpenAI = "openai"
	llmBackendCodex  = "codex"
	llmBackendQwen   = "qwen"
	llmBackendFake   = "fake"
)

func normalizeLLMBackend(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", llmBackendAuto:
		return llmBackendAuto
	case llmBackendOpenAI, "api", "openai-api":
		return llmBackendOpenAI
	case llmBackendCodex, "chatgpt", "subscription", "codex-cli":
		return llmBackendCodex
	case llmBackendQwen, "qwen-code", "qwen-cli":
		return llmBackendQwen
	case llmBackendFake, "scripted", "scripted-fake":
		return llmBackendFake
	default:
		return ""
	}
}

func resolveLLMBackend(cfg RuntimeConfig) (string, error) {
	backend := normalizeLLMBackend(cfg.LLMBackend)
	if backend == "" {
		return "", fmt.Errorf("unsupported llm backend: %q", cfg.LLMBackend)
	}
	if backend != llmBackendAuto {
		return backend, nil
	}

	if explicit := explicitOrEnvAPIKey(cfg.OpenAIKey); explicit != "" {
		return llmBackendOpenAI, nil
	}
	if hasChatGPTCodexSession() && findCodexExecutable() != "" {
		return llmBackendCodex, nil
	}
	if strings.TrimSpace(LoadSavedKey()) != "" {
		return llmBackendOpenAI, nil
	}
	return llmBackendOpenAI, nil
}

func NewLLM(cfg RuntimeConfig) (ChatLLM, error) {
	backend, err := resolveLLMBackend(cfg)
	if err != nil {
		return nil, err
	}
	if backend == llmBackendFake {
		log.Printf("[llm] backend=fake scenario=%s", cfg.Model)
		return NewScriptedFakeLLM(cfg.Model), nil
	}
	providerRecord, err := runtimeProviderRecord(cfg)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.ProviderID) != "" && strings.TrimSpace(providerRecord.ProviderID) == "" {
		return nil, fmt.Errorf("unknown provider %q", strings.TrimSpace(cfg.ProviderID))
	}

	switch backend {
	case llmBackendCodex:
		if managedRuntimeRequiresIsolatedCodexHome() && strings.TrimSpace(os.Getenv(managedAgentCodexHomeFlag)) == "" {
			return nil, fmt.Errorf("managed partner runtime requires RHIZOME_AGENT_CODEX_HOME for codex backend")
		}
		launchSpec, err := providerCodexLaunchSpec(providerRecord, findCodexExecutable())
		if err != nil {
			return nil, fmt.Errorf("codex bridge command is invalid: %w", err)
		}
		if launchSpec.Executable == "" {
			return nil, fmt.Errorf("codex backend requested but no codex executable was found")
		}
		if err := verifyCodexExecVersionPin(launchSpec.Executable); err != nil {
			return nil, err
		}
		log.Printf("[llm] backend=codex executable=%s extra_args=%d model=%s", launchSpec.Executable, len(launchSpec.Args), cfg.Model)
		return NewCodexExecLLM(launchSpec.Executable, cfg.Workdir, cfg.Model, launchSpec.Args...), nil
	case llmBackendQwen:
		launchSpec, err := providerQwenLaunchSpec(providerRecord, findQwenExecutable())
		if err != nil {
			return nil, fmt.Errorf("qwen bridge command is invalid: %w", err)
		}
		if launchSpec.Executable == "" {
			return nil, fmt.Errorf("qwen backend requested but no qwen executable was found")
		}
		log.Printf("[llm] backend=qwen executable=%s extra_args=%d model=%s", launchSpec.Executable, len(launchSpec.Args), cfg.Model)
		return NewQwenExecLLM(launchSpec.Executable, cfg.Workdir, cfg.Model, launchSpec.Args...), nil
	case llmBackendOpenAI:
		apiKey, err := loadAPIKey(cfg.OpenAIKey, providerRecord)
		if err != nil {
			return nil, err
		}
		if hasChatGPTCodexSession() && explicitOrProviderEnvAPIKey(cfg.OpenAIKey, providerRecord) == "" && providerUsesOpenAIKeyFallback(providerRecord) && strings.TrimSpace(os.Getenv("OPENAI_API_KEY")) == "" {
			log.Printf("[llm] backend=openai selected even though ChatGPT Codex session exists; set --llm-backend codex to force subscription-backed execution")
		}
		baseURL := providerOpenAIBaseURL(providerRecord)
		log.Printf("[llm] backend=openai-api model=%s base_url=%s", cfg.Model, firstNonEmpty(baseURL, defaultOpenAIBaseURL))
		return NewOpenAILLMWithConfig(apiKey, cfg.Model, baseURL, providerOpenAIPublicHeaders(providerRecord)), nil
	default:
		return nil, fmt.Errorf("unsupported llm backend: %q", backend)
	}
}
