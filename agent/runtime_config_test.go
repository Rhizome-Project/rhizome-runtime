package main

import (
	"strings"
	"testing"
	"time"
)

func TestRuntimeConfigApplyDefaults(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   "http://example.test/rpc",
		RhizomeToken: "token",
		WorkspaceID:  "ws",
		AgentID:      "agent",
		OwnerUserID:  "owner",
	}
	cfg.ApplyDefaults()

	if cfg.Model != defaultModel {
		t.Fatalf("expected default model %q, got %q", defaultModel, cfg.Model)
	}
	if cfg.LLMBackend != llmBackendAuto {
		t.Fatalf("expected llm backend to default to %q, got %q", llmBackendAuto, cfg.LLMBackend)
	}
	if cfg.DisplayName != "agent" {
		t.Fatalf("expected display name to default to agent id, got %q", cfg.DisplayName)
	}
	if cfg.WorkspacePassword != "" {
		t.Fatalf("expected workspace password to remain unset, got %q", cfg.WorkspacePassword)
	}
	if cfg.RhizomeHost != "http://example.test" {
		t.Fatalf("expected rhizome host to derive from rpc endpoint, got %q", cfg.RhizomeHost)
	}
	if cfg.MaxPromptDocChars <= 0 || cfg.MaxPromptSpecChars <= 0 || cfg.MaxResultMemoryBody <= 0 {
		t.Fatalf("expected prompt limits to be defaulted, got %+v", cfg)
	}
	if cfg.MaxSmokeCyclesPerAgent <= 0 || cfg.MaxSmokeCyclesPerTask <= 0 {
		t.Fatalf("expected smoke cycle ceilings to be defaulted, got %+v", cfg)
	}
	if cfg.MaxToolLoopIterations <= 0 || cfg.MaxProviderRetryAttempts <= 0 {
		t.Fatalf("expected retry ceilings to be defaulted, got %+v", cfg)
	}
	if cfg.MemoryRepairCooldown <= 0 || cfg.MemoryPromotionStaleAfter <= 0 || cfg.MemoryStalePacketThreshold <= 0 {
		t.Fatalf("expected memory control thresholds to be defaulted, got %+v", cfg)
	}
	if cfg.CoordinationMode != CoordinationModeStrict {
		t.Fatalf("expected coordination mode to default to strict, got %q", cfg.CoordinationMode)
	}
	if cfg.PlannerCycleTimeout != defaultPlannerCycleTimeout {
		t.Fatalf("expected planner cycle timeout default %s, got %s", defaultPlannerCycleTimeout, cfg.PlannerCycleTimeout)
	}
}

func TestRuntimeConfigPlannerCycleTimeoutDefaultsFromProviderCallTimeout(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:                RuntimeModeDaemon,
		Workdir:             t.TempDir(),
		RhizomeRPC:          "http://example.test/rpc",
		RhizomeToken:        "token",
		WorkspaceID:         "ws",
		AgentID:             "agent",
		OwnerUserID:         "owner",
		ProviderCallTimeout: 37 * time.Second,
	}
	cfg.ApplyDefaults()

	if cfg.PlannerCycleTimeout != 37*time.Second {
		t.Fatalf("expected planner cycle timeout to inherit provider timeout, got %s", cfg.PlannerCycleTimeout)
	}

	cfg.PlannerCycleTimeout = 42 * time.Second
	cfg.ApplyDefaults()
	if cfg.PlannerCycleTimeout != 42*time.Second {
		t.Fatalf("expected explicit planner cycle timeout to remain, got %s", cfg.PlannerCycleTimeout)
	}
}

func TestRuntimeConfigClampsCycleTimeoutBelowStallWindow(t *testing.T) {
	base := func() RuntimeConfig {
		return RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			Workdir:      t.TempDir(),
			RhizomeRPC:   "http://example.test/rpc",
			RhizomeToken: "token",
			WorkspaceID:  "ws",
			AgentID:      "agent",
			OwnerUserID:  "owner",
		}
	}
	// WR-03: a cycle/provider timeout at/above the stall window is clamped strictly below it,
	// so one legitimately-long cycle cannot self-trigger the stall watchdog.
	cfg := base()
	cfg.TaskStallAfter = 20 * time.Minute
	cfg.PlannerCycleTimeout = 30 * time.Minute
	cfg.ProviderCallTimeout = 25 * time.Minute
	cfg.ApplyDefaults()
	if cfg.PlannerCycleTimeout >= cfg.TaskStallAfter {
		t.Fatalf("expected planner cycle timeout clamped below stall window, got cycle=%s stall=%s", cfg.PlannerCycleTimeout, cfg.TaskStallAfter)
	}
	if cfg.ProviderCallTimeout >= cfg.TaskStallAfter {
		t.Fatalf("expected provider call timeout clamped below stall window, got provider=%s stall=%s", cfg.ProviderCallTimeout, cfg.TaskStallAfter)
	}
	// Defaults (10m cycle < 20m stall) are unaffected; provider stays 0 (unbounded).
	def := base()
	def.ApplyDefaults()
	if def.PlannerCycleTimeout != defaultPlannerCycleTimeout {
		t.Fatalf("expected default planner cycle timeout unchanged, got %s", def.PlannerCycleTimeout)
	}
	if def.ProviderCallTimeout != 0 {
		t.Fatalf("expected default provider call timeout to stay 0 (unbounded), got %s", def.ProviderCallTimeout)
	}
}

func TestRuntimeConfigApplyDefaultsNormalizesTrustFirstCoordinationMode(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:             RuntimeModeDaemon,
		Workdir:          t.TempDir(),
		RhizomeRPC:       "http://example.test/rpc",
		WorkspaceID:      "ws",
		AgentID:          "agent",
		OwnerUserID:      "owner",
		LLMBackend:       llmBackendFake,
		CoordinationMode: "TRUST-FIRST",
	}
	cfg.ApplyDefaults()

	if cfg.CoordinationMode != CoordinationModeTrustFirst {
		t.Fatalf("expected trust-first coordination mode, got %q", cfg.CoordinationMode)
	}
	if !runtimeTrustFirst(cfg) {
		t.Fatalf("expected runtimeTrustFirst to recognize %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected trust-first config to validate, got %v", err)
	}
}

func TestRuntimeConfigValidateRejectsUnknownCoordinationMode(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:             RuntimeModeDaemon,
		Workdir:          t.TempDir(),
		RhizomeRPC:       "http://example.test/rpc",
		WorkspaceID:      "ws",
		AgentID:          "agent",
		OwnerUserID:      "owner",
		LLMBackend:       llmBackendFake,
		CoordinationMode: "committee",
	}
	cfg.ApplyDefaults()

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "coordination mode") {
		t.Fatalf("expected invalid coordination mode error, got %v", err)
	}
}

func TestRuntimeConfigApplyDefaultsDropsPlaceholderCapabilities(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   "http://example.test/rpc",
		RhizomeToken: "token",
		WorkspaceID:  "ws",
		AgentID:      "agent",
		OwnerUserID:  "owner",
		Capabilities: []string{"\u2014", "\u00e2\u20ac\u201d", "\u00c3\u00a2\u00e2\u201a\u00ac\u00e2\u20ac\u009d"},
	}
	cfg.ApplyDefaults()

	if !containsString(cfg.Capabilities, "tool.call") || containsString(cfg.Capabilities, "\u2014") {
		t.Fatalf("expected placeholder-only capabilities to fall back to defaults, got %+v", cfg.Capabilities)
	}
}

func TestFirstCapabilitiesSkipsPlaceholderGroup(t *testing.T) {
	got := firstCapabilities(
		[]string{"\u2014", "\u00e2\u20ac\u201d"},
		[]string{"tool.call", "project.git"},
	)
	if strings.Join(got, ",") != "tool.call,project.git" {
		t.Fatalf("expected first real capability group, got %+v", got)
	}
}

func TestRuntimeConfigApplyDefaultsDerivesRPCFromHost(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		RhizomeHost: "https://rhizome.test",
	}
	cfg.ApplyDefaults()

	if cfg.RhizomeRPC != "https://rhizome.test/rpc" {
		t.Fatalf("expected rhizome rpc to derive from host, got %q", cfg.RhizomeRPC)
	}
}

func TestRuntimeConfigValidateDaemonRequiresRhizomeFields(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:       RuntimeModeDaemon,
		Workdir:    t.TempDir(),
		Model:      defaultModel,
		LLMBackend: llmBackendFake,
	}
	cfg.ApplyDefaults()

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for missing rhizome settings")
	}
}

func TestRuntimeModeFromInputsAllowsDaemonWithoutPresetToken(t *testing.T) {
	got := runtimeModeFromInputs("auto", "https://rhizome.test/rpc", "", "", "ws-1", "agent-1", "owner-1")
	if got != RuntimeModeDaemon {
		t.Fatalf("expected daemon mode without preset token, got %q", got)
	}
}

func TestRuntimeConfigValidateDaemonDoesNotRequireToken(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		RhizomeRPC:  "https://rhizome.test/rpc",
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		OwnerUserID: "owner-1",
		LLMBackend:  llmBackendFake,
	}
	cfg.ApplyDefaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected daemon config without token to validate, got %v", err)
	}
}

func TestRuntimeConfigValidateDaemonAllowsSavedTokenWithoutOwner(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   "https://rhizome.test/rpc",
		RhizomeToken: "saved-token",
		WorkspaceID:  "ws-1",
		AgentID:      "agent-1",
		LLMBackend:   llmBackendFake,
	}
	cfg.ApplyDefaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected daemon config with saved token to validate, got %v", err)
	}
}

func TestRuntimeConfigValidateDaemonRejectsHeartbeatBeyondLeaseSafetyWindow(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:           RuntimeModeDaemon,
		Workdir:        t.TempDir(),
		RhizomeRPC:     "https://rhizome.test/rpc",
		WorkspaceID:    "ws-1",
		AgentID:        "agent-1",
		OwnerUserID:    "owner-1",
		LLMBackend:     llmBackendFake,
		HeartbeatEvery: runtimeIdentityLeaseMaxHeartbeat + time.Second,
	}
	cfg.ApplyDefaults()

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "HEARTBEAT_SEC") {
		t.Fatalf("expected heartbeat lease safety validation error, got %v", err)
	}
}

func TestRuntimeModeFromInputsUsesResolvedConfigWithoutToken(t *testing.T) {
	mode := runtimeModeFromInputs("auto", "https://rhizome.test/rpc", "https://rhizome.test", "", "ws-1", "agent-1", "owner-1")
	if mode != RuntimeModeDaemon {
		t.Fatalf("expected daemon mode, got %q", mode)
	}
}

func TestRuntimeModeFromInputsAllowsSavedTokenWithoutOwner(t *testing.T) {
	mode := runtimeModeFromInputs("auto", "https://rhizome.test/rpc", "https://rhizome.test", "saved-token", "ws-1", "agent-1", "")
	if mode != RuntimeModeDaemon {
		t.Fatalf("expected daemon mode with saved token, got %q", mode)
	}
}

func TestRuntimeModeFromInputsSupportsTUI(t *testing.T) {
	mode := runtimeModeFromInputs("tui", "", "", "", "", "", "")
	if mode != RuntimeModeTUI {
		t.Fatalf("expected tui mode, got %q", mode)
	}
}

func TestRuntimeConfigValidateTUIRequiresOnlyWorkdir(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:    RuntimeModeTUI,
		Workdir: t.TempDir(),
	}
	cfg.ApplyDefaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected tui config to validate, got %v", err)
	}
}

func TestRuntimeConfigValidateRealLLMPilotRequiresExplicitBoundedProfile(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:                  RuntimeModeDaemon,
		Workdir:               t.TempDir(),
		RhizomeRPC:            "https://rhizome.test/rpc",
		WorkspaceID:           "ws-1",
		AgentID:               "agent-1",
		OwnerUserID:           "owner-1",
		ProviderID:            "fake",
		GroupID:               "fake",
		LLMBackend:            llmBackendFake,
		Model:                 "normal_complete",
		RealLLMPilot:          true,
		BudgetHardLimitMicros: 100,
		MaxToolLoopIterations: realLLMPilotMaxToolLoopIterations + 1,
	}
	cfg.ApplyDefaults()

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected real pilot validation error")
	}
	for _, want := range []string{"non-fake provider id", "openai, codex, or qwen", "non-fake model", "max tool loop iterations"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error to mention %q, got %v", want, err)
		}
	}
}

func TestRuntimeConfigValidateRejectsRealCapableDaemonWithoutPilotGate(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		RhizomeRPC:  "https://rhizome.test/rpc",
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		OwnerUserID: "owner-1",
		ProviderID:  "codex-bridge",
		GroupID:     "codex",
		LLMBackend:  llmBackendCodex,
		Model:       "gpt-5.4",
	}
	cfg.ApplyDefaults()

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "--real-llm-pilot") {
		t.Fatalf("expected daemon real provider to require pilot gate, got %v", err)
	}
}

func TestRuntimeConfigValidateTrustFirstAllowsRealCapableDaemonWithoutPilotGate(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:             RuntimeModeDaemon,
		Workdir:          t.TempDir(),
		RhizomeRPC:       "https://rhizome.test/rpc",
		WorkspaceID:      "ws-1",
		AgentID:          "agent-1",
		OwnerUserID:      "owner-1",
		ProviderID:       "codex-bridge",
		GroupID:          "codex",
		LLMBackend:       llmBackendCodex,
		Model:            "gpt-5.4",
		CoordinationMode: CoordinationModeTrustFirst,
	}
	cfg.ApplyDefaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected trust-first daemon real provider to validate without pilot gate, got %v", err)
	}
}

func TestRuntimeConfigValidateRealLLMPilotRejectsReserveAboveHardLimit(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:                     RuntimeModeDaemon,
		Workdir:                  t.TempDir(),
		RhizomeRPC:               "https://rhizome.test/rpc",
		WorkspaceID:              "ws-1",
		AgentID:                  "agent-1",
		OwnerUserID:              "owner-1",
		ProviderID:               "codex-bridge",
		GroupID:                  "codex",
		LLMBackend:               llmBackendCodex,
		Model:                    "gpt-5.4",
		RealLLMPilot:             true,
		BudgetAccountID:          "pilot-budget",
		BudgetHardLimitMicros:    100,
		BudgetReserveMicros:      101,
		BudgetMicrosPerToken:     1,
		MaxToolLoopIterations:    1,
		MaxProviderRetryAttempts: 1,
		ProviderCallTimeout:      10 * time.Second,
	}
	cfg.ApplyDefaults()

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "reserve must be less than or equal to hard limit") {
		t.Fatalf("expected reserve above hard limit rejection, got %v", err)
	}
}

func TestRuntimeConfigValidateRealLLMPilotAcceptsBoundedProfile(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:                     RuntimeModeDaemon,
		Workdir:                  t.TempDir(),
		RhizomeRPC:               "https://rhizome.test/rpc",
		WorkspaceID:              "ws-1",
		AgentID:                  "agent-1",
		OwnerUserID:              "owner-1",
		ProviderID:               "codex-bridge",
		GroupID:                  "codex",
		LLMBackend:               llmBackendCodex,
		Model:                    "gpt-5.4",
		RealLLMPilot:             true,
		BudgetAccountID:          "pilot-budget",
		BudgetHardLimitMicros:    5000,
		BudgetReserveMicros:      500,
		BudgetMicrosPerToken:     2,
		MaxToolLoopIterations:    realLLMPilotMaxToolLoopIterations,
		MaxProviderRetryAttempts: realLLMPilotMaxProviderRetryAttempts,
		ProviderCallTimeout:      realLLMPilotMaxProviderCallTimeout,
	}
	cfg.ApplyDefaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected bounded real pilot profile to validate, got %v", err)
	}
	if !cfg.DaemonWorkspaceTools {
		t.Fatal("expected real LLM daemon pilot to expose workspace tools by default")
	}
	evidence := buildRealLLMPilotProfileEvidence(cfg)
	if evidence["schema"] != realLLMPilotProfileSchema || evidence["status"] != realLLMPilotStatusReady {
		t.Fatalf("expected ready real pilot evidence, got %+v", evidence)
	}
	if evidence["provider_call_timeout_sec"] != int(realLLMPilotMaxProviderCallTimeout/time.Second) ||
		evidence["allowed_max_provider_call_timeout_sec"] != int(realLLMPilotMaxProviderCallTimeout/time.Second) {
		t.Fatalf("expected real pilot timeout evidence to expose raised ceiling, got %+v", evidence)
	}
}

func TestRuntimeConfigValidateRealLLMSoakGuardRequiresCompletePilotProfile(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   "https://rhizome.test/rpc",
		WorkspaceID:  "ws-1",
		AgentID:      "agent-1",
		OwnerUserID:  "owner-1",
		ProviderID:   "codex-bridge",
		GroupID:      "codex",
		LLMBackend:   llmBackendCodex,
		Model:        "gpt-5.4",
		SoakStopFile: t.TempDir(),
	}
	cfg.ApplyDefaults()

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected incomplete soak guard validation error")
	}
	for _, want := range []string{"--real-llm-pilot", "--soak-runtime-limit-sec", "--soak-max-provider-calls"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error to mention %q, got %v", want, err)
		}
	}
}

func TestRuntimeConfigValidateRealLLMSoakGuardAcceptsCompleteProfile(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:                     RuntimeModeDaemon,
		Workdir:                  t.TempDir(),
		RhizomeRPC:               "https://rhizome.test/rpc",
		WorkspaceID:              "ws-1",
		AgentID:                  "agent-1",
		OwnerUserID:              "owner-1",
		ProviderID:               "codex-bridge",
		GroupID:                  "codex",
		LLMBackend:               llmBackendCodex,
		Model:                    "gpt-5.4",
		RealLLMPilot:             true,
		BudgetAccountID:          "pilot-budget",
		BudgetHardLimitMicros:    5000,
		BudgetReserveMicros:      500,
		BudgetMicrosPerToken:     2,
		MaxToolLoopIterations:    realLLMPilotMaxToolLoopIterations,
		MaxProviderRetryAttempts: realLLMPilotMaxProviderRetryAttempts,
		ProviderCallTimeout:      10 * time.Second,
		SoakStopFile:             t.TempDir(),
		SoakRuntimeLimit:         time.Hour,
		SoakMaxProviderCalls:     10,
	}
	cfg.ApplyDefaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected complete real LLM soak guard profile to validate, got %v", err)
	}
}

func TestLoadRuntimeConfigFromArgsBudgetEnvOverridesLocalProfile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_TOKEN", "token")
	t.Setenv("RHIZOME_BUDGET_ACCOUNT_ID", "acct-env")
	t.Setenv("RHIZOME_BUDGET_HARD_LIMIT_MICROS", "10")
	t.Setenv("RHIZOME_BUDGET_RESERVE_MICROS", "5")
	t.Setenv("RHIZOME_BUDGET_MICROS_PER_TOKEN", "2")

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		BudgetAccountID:       "acct-profile",
		BudgetHardLimitMicros: 100,
		BudgetReserveMicros:   50,
		BudgetMicrosPerToken:  9,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error = %v", err)
	}

	cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", workdir, "--mode", string(RuntimeModeTUI)})
	if err != nil {
		t.Fatalf("loadRuntimeConfigFromArgs() error = %v", err)
	}
	if cfg.BudgetAccountID != "acct-env" {
		t.Fatalf("budget account id = %q, want acct-env", cfg.BudgetAccountID)
	}
	if cfg.BudgetHardLimitMicros != 10 || cfg.BudgetReserveMicros != 5 || cfg.BudgetMicrosPerToken != 2 {
		t.Fatalf("expected budget env values to override profile, got hard=%d reserve=%d micros_per_token=%d", cfg.BudgetHardLimitMicros, cfg.BudgetReserveMicros, cfg.BudgetMicrosPerToken)
	}
}

func TestLoadRuntimeConfigFromArgsToolLoopCompactionResolution(t *testing.T) {
	isolateRuntimeConfigHome := func(t *testing.T) {
		t.Helper()
		home := t.TempDir()
		t.Setenv("HOME", home)
		t.Setenv("USERPROFILE", home)
		t.Setenv("HOMEDRIVE", "")
		t.Setenv("HOMEPATH", "")
	}

	t.Run("default off", func(t *testing.T) {
		isolateRuntimeConfigHome(t)
		t.Setenv("RHIZOME_TOOL_LOOP_COMPACTION", "")
		cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", t.TempDir(), "--mode", string(RuntimeModeTUI)})
		if err != nil {
			t.Fatalf("loadRuntimeConfigFromArgs() error = %v", err)
		}
		if cfg.ToolLoopCompaction {
			t.Fatal("expected tool-loop compaction to default off")
		}
	})

	t.Run("env on", func(t *testing.T) {
		isolateRuntimeConfigHome(t)
		t.Setenv("RHIZOME_TOOL_LOOP_COMPACTION", "true")
		cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", t.TempDir(), "--mode", string(RuntimeModeTUI)})
		if err != nil {
			t.Fatalf("loadRuntimeConfigFromArgs() error = %v", err)
		}
		if !cfg.ToolLoopCompaction {
			t.Fatal("expected RHIZOME_TOOL_LOOP_COMPACTION=true to enable tool-loop compaction")
		}
	})

	t.Run("flag overrides env", func(t *testing.T) {
		isolateRuntimeConfigHome(t)
		t.Setenv("RHIZOME_TOOL_LOOP_COMPACTION", "false")
		cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", t.TempDir(), "--mode", string(RuntimeModeTUI), "--tool-loop-compaction"})
		if err != nil {
			t.Fatalf("loadRuntimeConfigFromArgs() error = %v", err)
		}
		if !cfg.ToolLoopCompaction {
			t.Fatal("expected --tool-loop-compaction flag to override env=false")
		}
	})

	t.Run("profile on", func(t *testing.T) {
		isolateRuntimeConfigHome(t)
		t.Setenv("RHIZOME_TOOL_LOOP_COMPACTION", "")
		workdir := t.TempDir()
		if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{ToolLoopCompaction: true}); err != nil {
			t.Fatalf("SaveLocalRuntimeProfile() error = %v", err)
		}
		cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", workdir, "--mode", string(RuntimeModeTUI)})
		if err != nil {
			t.Fatalf("loadRuntimeConfigFromArgs() error = %v", err)
		}
		if !cfg.ToolLoopCompaction {
			t.Fatal("expected local profile tool_loop_compaction=true to enable tool-loop compaction")
		}
	})
}

func TestApplyToolLoopCompactionSettingFlipsProcessDefault(t *testing.T) {
	original := CurrentToolLoopCompaction()
	defer SetDefaultToolLoopCompaction(original)

	SetDefaultToolLoopCompaction(DefaultToolLoopCompactionConfig())
	applyToolLoopCompactionSetting(RuntimeConfig{ToolLoopCompaction: true})
	if !CurrentToolLoopCompaction().Enabled {
		t.Fatal("expected applyToolLoopCompactionSetting(enabled) to flip process default on")
	}

	applyToolLoopCompactionSetting(RuntimeConfig{ToolLoopCompaction: false})
	if CurrentToolLoopCompaction().Enabled {
		t.Fatal("expected applyToolLoopCompactionSetting(disabled) to restore default off")
	}
}

func TestLoadRuntimeConfigFromArgsRealLLMPilotEnvOverridesProfile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_REAL_LLM_PILOT", "true")
	t.Setenv("RHIZOME_MAX_TOOL_LOOP_ITERATIONS", "3")
	t.Setenv("RHIZOME_MAX_PROVIDER_RETRY_ATTEMPTS", "1")
	t.Setenv("RHIZOME_PROVIDER_CALL_TIMEOUT_SEC", "9")
	t.Setenv("RHIZOME_SOAK_STOP_FILE", "C:\\tmp\\STOP_REAL_LLM_SOAK")
	t.Setenv("RHIZOME_SOAK_RUNTIME_LIMIT_SEC", "3600")
	t.Setenv("RHIZOME_SOAK_MAX_PROVIDER_CALLS", "10")

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RealLLMPilot:             false,
		MaxToolLoopIterations:    17,
		MaxProviderRetryAttempts: 3,
		ProviderCallTimeoutSec:   45,
		SoakStopFile:             "profile-stop",
		SoakRuntimeLimitSec:      7200,
		SoakMaxProviderCalls:     20,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error = %v", err)
	}

	cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", workdir, "--mode", string(RuntimeModeTUI)})
	if err != nil {
		t.Fatalf("loadRuntimeConfigFromArgs() error = %v", err)
	}
	if !cfg.RealLLMPilot {
		t.Fatal("expected RHIZOME_REAL_LLM_PILOT to enable real pilot profile")
	}
	if cfg.MaxToolLoopIterations != 3 || cfg.MaxProviderRetryAttempts != 1 {
		t.Fatalf("expected real pilot bounds from env, got tool=%d provider=%d", cfg.MaxToolLoopIterations, cfg.MaxProviderRetryAttempts)
	}
	if cfg.ProviderCallTimeout != 9*time.Second {
		t.Fatalf("expected provider timeout from env, got %s", cfg.ProviderCallTimeout)
	}
	if cfg.SoakStopFile != "C:\\tmp\\STOP_REAL_LLM_SOAK" || cfg.SoakRuntimeLimit != time.Hour || cfg.SoakMaxProviderCalls != 10 {
		t.Fatalf("expected real soak guard bounds from env, got stop=%q limit=%s calls=%d", cfg.SoakStopFile, cfg.SoakRuntimeLimit, cfg.SoakMaxProviderCalls)
	}
}

func TestLoadRuntimeConfigFromArgsHarnessPinAndDisabledTools(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	cfg, err := loadRuntimeConfigFromArgs([]string{
		"--mode", string(RuntimeModeDaemon),
		"--workdir", t.TempDir(),
		"--rhizome-rpc", "http://127.0.0.1:1/rpc",
		"--workspace-id", "ws-harness",
		"--agent-id", "delta",
		"--disabled-tools", "agent_request, project_checkout_materialize",
		"--pinned-task-id", "task-eval",
		"--telic-loop",
		"--telic-require-frozen-authority",
	})
	if err != nil {
		t.Fatalf("loadRuntimeConfigFromArgs() error = %v", err)
	}
	if cfg.PinnedTaskID != "task-eval" {
		t.Fatalf("expected pinned task id, got %q", cfg.PinnedTaskID)
	}
	if strings.Join(cfg.DisabledToolNames, ",") != "agent_request,project_checkout_materialize" {
		t.Fatalf("expected normalized disabled tools, got %#v", cfg.DisabledToolNames)
	}
	if !cfg.TelicLoopEnabled || !cfg.TelicRequireFrozenAuthority {
		t.Fatalf("expected telic frozen-authority harness flags to be enabled, got loop=%v frozen=%v", cfg.TelicLoopEnabled, cfg.TelicRequireFrozenAuthority)
	}
}
