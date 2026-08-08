package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	realLLMPilotProfileSchema             = "real_llm_pilot_profile.v1"
	realLLMPilotMaxToolLoopIterations     = 30
	realLLMPilotMaxProviderRetryAttempts  = 1
	realLLMPilotMaxProviderCallTimeout    = 18 * time.Minute
	realLLMPilotStatusDisabled            = "disabled"
	realLLMPilotStatusReady               = "ready"
	realLLMPilotStatusInvalid             = "invalid"
	realLLMPilotContractBudgetLedgerScope = "runtime_llm_budget_ledger"
)

func validateRealLLMPilotConfig(cfg RuntimeConfig) error {
	reasons := realLLMPilotProfileReasons(cfg)
	if len(reasons) == 0 {
		return nil
	}
	return fmt.Errorf("real LLM pilot profile invalid: %s", strings.Join(reasons, "; "))
}

func realLLMPilotProfileReasons(cfg RuntimeConfig) []string {
	if runtimeTrustFirst(cfg) {
		return realLLMPilotTrustFirstProfileReasons(cfg)
	}
	if cfg.RealLLMPilot {
		return realLLMPilotValidationReasons(cfg)
	}
	return realProviderWithoutPilotReasons(cfg)
}

func realLLMPilotTrustFirstProfileReasons(cfg RuntimeConfig) []string {
	if !cfg.RealLLMPilot {
		return nil
	}
	cfg.ProviderID = strings.TrimSpace(cfg.ProviderID)
	cfg.GroupID = strings.TrimSpace(cfg.GroupID)
	cfg.LLMBackend = normalizeLLMBackend(cfg.LLMBackend)
	cfg.Model = strings.TrimSpace(cfg.Model)
	cfg.ModelOverride = strings.TrimSpace(cfg.ModelOverride)

	reasons := make([]string, 0, 6)
	if cfg.Mode != RuntimeModeDaemon {
		reasons = append(reasons, "real LLM pilot requires daemon mode")
	}
	if cfg.ProviderID == "" || strings.EqualFold(cfg.ProviderID, "fake") {
		reasons = append(reasons, "real LLM pilot requires an explicit non-fake provider id")
	}
	if cfg.GroupID == "" || strings.EqualFold(cfg.GroupID, "fake") {
		reasons = append(reasons, "real LLM pilot requires an explicit non-fake provider group id")
	}
	if !realLLMPilotBackendAllowed(cfg.LLMBackend) {
		reasons = append(reasons, "real LLM pilot requires llm backend openai, codex, or qwen")
	}
	if cfg.Model == "" || realLLMPilotModelLooksFake(cfg.Model) {
		reasons = append(reasons, "real LLM pilot requires an explicit non-fake model")
	}
	if cfg.ModelOverride != "" && realLLMPilotModelLooksFake(cfg.ModelOverride) {
		reasons = append(reasons, "real LLM pilot model override cannot be a fake scenario")
	}
	return reasons
}

func realProviderWithoutPilotReasons(cfg RuntimeConfig) []string {
	if cfg.Mode != RuntimeModeDaemon {
		return nil
	}
	backend := normalizeLLMBackend(cfg.LLMBackend)
	if backend == "" || backend == llmBackendFake {
		return nil
	}
	providerID := strings.TrimSpace(cfg.ProviderID)
	if backend == llmBackendAuto || backend == llmBackendOpenAI || backend == llmBackendCodex || backend == llmBackendQwen || (providerID != "" && !strings.EqualFold(providerID, "fake")) {
		return []string{"daemon real-capable provider/backend requires --real-llm-pilot or explicit --llm-backend fake"}
	}
	return nil
}

func realLLMPilotValidationReasons(cfg RuntimeConfig) []string {
	if !cfg.RealLLMPilot {
		return nil
	}
	cfg.ProviderID = strings.TrimSpace(cfg.ProviderID)
	cfg.GroupID = strings.TrimSpace(cfg.GroupID)
	cfg.LLMBackend = normalizeLLMBackend(cfg.LLMBackend)
	cfg.Model = strings.TrimSpace(cfg.Model)
	cfg.ModelOverride = strings.TrimSpace(cfg.ModelOverride)
	cfg.BudgetAccountID = strings.TrimSpace(cfg.BudgetAccountID)

	reasons := make([]string, 0, 8)
	if cfg.Mode != RuntimeModeDaemon {
		reasons = append(reasons, "real LLM pilot requires daemon mode")
	}
	if cfg.ProviderID == "" || strings.EqualFold(cfg.ProviderID, "fake") {
		reasons = append(reasons, "real LLM pilot requires an explicit non-fake provider id")
	}
	if cfg.GroupID == "" || strings.EqualFold(cfg.GroupID, "fake") {
		reasons = append(reasons, "real LLM pilot requires an explicit non-fake provider group id")
	}
	if !realLLMPilotBackendAllowed(cfg.LLMBackend) {
		reasons = append(reasons, "real LLM pilot requires llm backend openai, codex, or qwen")
	}
	if cfg.Model == "" || realLLMPilotModelLooksFake(cfg.Model) {
		reasons = append(reasons, "real LLM pilot requires an explicit non-fake model")
	}
	if cfg.ModelOverride != "" && realLLMPilotModelLooksFake(cfg.ModelOverride) {
		reasons = append(reasons, "real LLM pilot model override cannot be a fake scenario")
	}
	if !runtimeBudgetLedgerConfigured(cfg) || cfg.BudgetAccountID == "" || cfg.BudgetHardLimitMicros <= 0 || cfg.BudgetReserveMicros <= 0 || cfg.BudgetMicrosPerToken <= 0 {
		reasons = append(reasons, "real LLM pilot requires budget account id, hard limit, reserve, and micros-per-token")
	}
	if cfg.BudgetHardLimitMicros > 0 && cfg.BudgetReserveMicros > cfg.BudgetHardLimitMicros {
		reasons = append(reasons, "real LLM pilot budget reserve must be less than or equal to hard limit")
	}
	if cfg.MaxToolLoopIterations <= 0 || cfg.MaxToolLoopIterations > realLLMPilotMaxToolLoopIterations {
		reasons = append(reasons, fmt.Sprintf("real LLM pilot requires max tool loop iterations between 1 and %d", realLLMPilotMaxToolLoopIterations))
	}
	if cfg.MaxProviderRetryAttempts <= 0 || cfg.MaxProviderRetryAttempts > realLLMPilotMaxProviderRetryAttempts {
		reasons = append(reasons, fmt.Sprintf("real LLM pilot requires max provider retry attempts between 1 and %d", realLLMPilotMaxProviderRetryAttempts))
	}
	if cfg.ProviderCallTimeout <= 0 || cfg.ProviderCallTimeout > realLLMPilotMaxProviderCallTimeout {
		reasons = append(reasons, fmt.Sprintf("real LLM pilot requires provider call timeout between 1s and %s", realLLMPilotMaxProviderCallTimeout))
	}
	return reasons
}

func realLLMPilotBackendAllowed(backend string) bool {
	switch normalizeLLMBackend(backend) {
	case llmBackendOpenAI, llmBackendCodex, llmBackendQwen:
		return true
	default:
		return false
	}
}

func realLLMPilotModelLooksFake(model string) bool {
	normalized := strings.ToLower(strings.TrimSpace(model))
	if normalized == "" {
		return true
	}
	if strings.HasPrefix(normalized, "fake:") || strings.HasPrefix(normalized, "scenario.") {
		return true
	}
	_, fakeScenario := scriptedFakeScenarios[normalized]
	return fakeScenario
}

func buildRealLLMPilotProfileEvidence(cfg RuntimeConfig) map[string]any {
	reasons := realLLMPilotProfileReasons(cfg)
	status := realLLMPilotStatusDisabled
	if cfg.RealLLMPilot && len(reasons) == 0 {
		status = realLLMPilotStatusReady
	} else if cfg.RealLLMPilot {
		status = realLLMPilotStatusInvalid
	} else if len(reasons) > 0 {
		status = realLLMPilotStatusInvalid
	}
	return map[string]any{
		"schema":                                realLLMPilotProfileSchema,
		"enabled":                               cfg.RealLLMPilot,
		"status":                                status,
		"reasons":                               reasons,
		"provider_id":                           strings.TrimSpace(cfg.ProviderID),
		"group_id":                              strings.TrimSpace(cfg.GroupID),
		"llm_backend":                           normalizeLLMBackend(cfg.LLMBackend),
		"model":                                 strings.TrimSpace(cfg.Model),
		"model_override":                        strings.TrimSpace(cfg.ModelOverride),
		"budget_scope":                          realLLMPilotContractBudgetLedgerScope,
		"budget_ledger_configured":              runtimeBudgetLedgerConfigured(cfg),
		"budget_account_id":                     strings.TrimSpace(cfg.BudgetAccountID),
		"budget_hard_limit_micros":              cfg.BudgetHardLimitMicros,
		"budget_reserve_micros":                 cfg.BudgetReserveMicros,
		"budget_micros_per_token":               cfg.BudgetMicrosPerToken,
		"max_tool_loop_iterations":              cfg.MaxToolLoopIterations,
		"max_provider_retry_attempts":           cfg.MaxProviderRetryAttempts,
		"provider_call_timeout_sec":             int(cfg.ProviderCallTimeout / time.Second),
		"allowed_max_tool_iterations":           realLLMPilotMaxToolLoopIterations,
		"allowed_max_provider_retries":          realLLMPilotMaxProviderRetryAttempts,
		"allowed_max_provider_call_timeout_sec": int(realLLMPilotMaxProviderCallTimeout / time.Second),
	}
}

func requireRealLLMPilotReady(cfg RuntimeConfig) error {
	if !cfg.RealLLMPilot {
		return errors.New("real LLM pilot is not enabled")
	}
	return validateRealLLMPilotConfig(cfg)
}
