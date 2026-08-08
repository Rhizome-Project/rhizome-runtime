package main

import (
	"bytes"
	"errors"
	"flag"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRuntimeHelpDoesNotExposeLoadedSecretsOrLocalDefaults(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_TOKEN", "secret-token-from-env")
	t.Setenv("RHIZOME_WORKSPACE_PASSWORD", "secret-password-from-env")
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{
		HostURL:           "https://private-host.invalid",
		WorkspacePassword: "secret-password-from-profile",
		AgentToken:        "secret-token-from-profile",
		OwnerUserID:       "private-owner",
	}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	var output bytes.Buffer
	previousOutput := runtimeFlagOutput
	runtimeFlagOutput = &output
	t.Cleanup(func() { runtimeFlagOutput = previousOutput })

	_, err := loadRuntimeConfigFromArgs([]string{"--help"})
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("loadRuntimeConfigFromArgs(--help) error = %v, want flag.ErrHelp", err)
	}
	got := output.String()
	for _, forbidden := range []string{
		"secret-token-from-env",
		"secret-password-from-env",
		"secret-password-from-profile",
		"secret-token-from-profile",
		"private-host.invalid",
		"private-owner",
		home,
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("help output exposed loaded configuration %q", forbidden)
		}
	}
}

func TestRunAgentCLIHelpExitsSuccessfully(t *testing.T) {
	for _, arg := range []string{"help", "-h", "--help"} {
		t.Run(arg, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if code := runAgentCLI([]string{arg}, &stdout, &stderr); code != 0 {
				t.Fatalf("runAgentCLI(%q) exit code = %d, want 0; stderr=%q", arg, code, stderr.String())
			}
			if !strings.Contains(stdout.String(), "rhizome-bot onboard") || !strings.Contains(stdout.String(), "rhizome-bot daemon [runtime flags]") {
				t.Fatalf("runAgentCLI(%q) output did not contain root usage: %q", arg, stdout.String())
			}
		})
	}
}

func TestRunAgentCLIDaemonHelpExitsSuccessfully(t *testing.T) {
	var help bytes.Buffer
	previousOutput := runtimeFlagOutput
	runtimeFlagOutput = &help
	t.Cleanup(func() { runtimeFlagOutput = previousOutput })

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := runAgentCLI([]string{"daemon", "--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("runAgentCLI(daemon --help) exit code = %d, want 0; stderr=%q", code, stderr.String())
	}
	for _, want := range []string{"fake (deterministic local evaluation)", "oracle-driven progress gate"} {
		if !strings.Contains(help.String(), want) {
			t.Fatalf("daemon help missing %q: %s", want, help.String())
		}
	}
	if !strings.Contains(help.String(), "Usage of rhizome-bot daemon:") {
		t.Fatalf("daemon help has the wrong command heading: %s", help.String())
	}
	for _, forbidden := range []string{"TE-", "A/B", "experiment harness", "operator-authored harness"} {
		if strings.Contains(help.String(), forbidden) {
			t.Fatalf("daemon help retained internal jargon %q: %s", forbidden, help.String())
		}
	}
}

func TestRuntimeRejectsSecretCommandLineFlags(t *testing.T) {
	for _, args := range [][]string{
		{"--openai-key", "secret"},
		{"--rhizome-token=secret"},
		{"--workspace-password", "secret"},
	} {
		if _, err := loadRuntimeConfigFromArgs(args); err == nil || !strings.Contains(err.Error(), "command-line secrets") {
			t.Fatalf("loadRuntimeConfigFromArgs(%v) error = %v, want secret-argv rejection", args, err)
		}
	}
}

func TestRuntimeHostFlagDerivesRPCUnlessRPCIsExplicit(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_HOST", "")
	t.Setenv("RHIZOME_RPC", "")

	cfg, err := loadRuntimeConfigFromArgs([]string{
		"--workdir", t.TempDir(),
		"--rhizome-host", "http://127.0.0.1:9137",
	})
	if err != nil {
		t.Fatalf("load runtime config with host override: %v", err)
	}
	if cfg.RhizomeHost != "http://127.0.0.1:9137" || cfg.RhizomeRPC != "http://127.0.0.1:9137/rpc" {
		t.Fatalf("host override did not derive RPC endpoint: host=%q rpc=%q", cfg.RhizomeHost, cfg.RhizomeRPC)
	}

	cfg, err = loadRuntimeConfigFromArgs([]string{
		"--workdir", t.TempDir(),
		"--rhizome-host", "http://127.0.0.1:9137",
		"--rhizome-rpc", "http://127.0.0.1:9248/custom-rpc",
	})
	if err != nil {
		t.Fatalf("load runtime config with explicit RPC override: %v", err)
	}
	if cfg.RhizomeRPC != "http://127.0.0.1:9248/custom-rpc" {
		t.Fatalf("explicit RPC endpoint was overwritten: %q", cfg.RhizomeRPC)
	}
}

func TestDefaultOwnerUserIDPrefersEnvThenProfile(t *testing.T) {
	t.Setenv("RHIZOME_OWNER_USER_ID", "env-owner")

	profile := RhizomeConnectionProfile{OwnerUserID: "profile-owner"}
	if got := defaultOwnerUserID(LocalRuntimeProfile{}, profile); got != "env-owner" {
		t.Fatalf("expected env owner id to win, got %q", got)
	}

	t.Setenv("RHIZOME_OWNER_USER_ID", "")
	if got := defaultOwnerUserID(LocalRuntimeProfile{}, profile); got != "profile-owner" {
		t.Fatalf("expected profile owner id to be used when env is empty, got %q", got)
	}
}

func TestDefaultOwnerUserIDPrefersRegisteredExecutorIdentity(t *testing.T) {
	t.Setenv("RHIZOME_OWNER_USER_ID", "")

	local := LocalRuntimeProfile{
		OwnerUserID: "owner-requested",
		RegisteredExecutor: RegisteredExecutorIdentity{
			OwnerUserID: "owner-registered",
		},
	}
	if got := defaultOwnerUserID(local, RhizomeConnectionProfile{}); got != "owner-registered" {
		t.Fatalf("expected registered owner id to win over loose local owner id, got %q", got)
	}
}

func TestLoadRuntimeConfigFromArgsPrefersLocalRuntimeTuning(t *testing.T) {
	workdir := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_HOST", "")
	t.Setenv("RHIZOME_RPC", "")
	t.Setenv("RHIZOME_TOKEN", "")
	t.Setenv("RHIZOME_WORKSPACE_ID", "")
	t.Setenv("RHIZOME_AGENT_ID", "")
	t.Setenv("RHIZOME_AGENT_NAME", "")
	t.Setenv("RHIZOME_OWNER_USER_ID", "")
	t.Setenv("RHIZOME_AGENT_LLM_BACKEND", "")
	t.Setenv("RHIZOME_AGENT_MODEL", "")
	t.Setenv("RHIZOME_AGENT_MODE", "")

	err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		Mode:                    string(RuntimeModeDaemon),
		ProtocolVersion:         "rnar/v99",
		ProviderID:              "codex",
		ModelOverride:           "gpt-5.4",
		LLMBackend:              llmBackendCodex,
		Model:                   "gpt-5.4",
		RPCEndpoint:             "https://rhizome.test/rpc",
		HostURL:                 "https://rhizome.test",
		WorkspaceID:             "ws-local",
		WorkspacePassword:       "pw-local",
		AgentID:                 "agent-local",
		DisplayName:             "Agent Local",
		OwnerUserID:             "owner-local",
		GroupID:                 "codex",
		HeartbeatSec:            11,
		PlannerSec:              22,
		BootstrapSec:            33,
		WatchdogSec:             44,
		ListenerTimeoutSec:      55,
		ListenerBatch:           66,
		ListenerLookbackHours:   77,
		UpdatesLimit:            88,
		MemorySyncSec:           99,
		PromotionSyncBatch:      5,
		RSPRolloutPhase:         string(RSPRolloutObserveOnly),
		RequestStaleAfterSec:    123,
		MemoryRepairCooldownSec: 456,
		MaxPromptDocChars:       7890,
	})
	if err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", workdir})
	if err != nil {
		t.Fatalf("loadRuntimeConfigFromArgs() error: %v", err)
	}

	if cfg.ProtocolVersion != "rnar/v99" {
		t.Fatalf("expected protocol version from local profile, got %q", cfg.ProtocolVersion)
	}
	if cfg.ProviderID != "codex" || cfg.ModelOverride != "gpt-5.4" || cfg.GroupID != "codex" {
		t.Fatalf("expected provider binding from local profile, got provider=%q model_override=%q group=%q", cfg.ProviderID, cfg.ModelOverride, cfg.GroupID)
	}
	if cfg.HeartbeatEvery != 11*time.Second || cfg.PlannerEvery != 22*time.Second || cfg.BootstrapEvery != 33*time.Second || cfg.WatchdogEvery != 44*time.Second {
		t.Fatalf("expected timing defaults from local profile, got heartbeat=%s planner=%s bootstrap=%s watchdog=%s", cfg.HeartbeatEvery, cfg.PlannerEvery, cfg.BootstrapEvery, cfg.WatchdogEvery)
	}
	if cfg.PollTimeout != 55*time.Second || cfg.PollLimit != 66 || cfg.LookbackHours != 77 || cfg.UpdatesLimit != 88 {
		t.Fatalf("expected listener defaults from local profile, got timeout=%s batch=%d lookback=%d updates=%d", cfg.PollTimeout, cfg.PollLimit, cfg.LookbackHours, cfg.UpdatesLimit)
	}
	if cfg.MemorySyncEvery != 99*time.Second || cfg.MaxPromotionSyncBatch != 5 || cfg.RequestStaleAfter != 123*time.Second || cfg.MemoryRepairCooldown != 456*time.Second {
		t.Fatalf("expected background tuning from local profile, got memory_sync=%s promotion_batch=%d request_stale=%s repair=%s", cfg.MemorySyncEvery, cfg.MaxPromotionSyncBatch, cfg.RequestStaleAfter, cfg.MemoryRepairCooldown)
	}
	if cfg.MaxPromptDocChars != 7890 {
		t.Fatalf("expected prompt tuning from local profile, got %d", cfg.MaxPromptDocChars)
	}
}

func TestLoadRuntimeConfigFromArgsUsesManagerDefaultsWhenNoLocalProfileExists(t *testing.T) {
	workdir := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_HOST", "")
	t.Setenv("RHIZOME_RPC", "")
	t.Setenv("RHIZOME_TOKEN", "")
	t.Setenv("RHIZOME_WORKSPACE_ID", "")
	t.Setenv("RHIZOME_OWNER_USER_ID", "")
	t.Setenv("RHIZOME_AGENT_LLM_BACKEND", "")
	t.Setenv("RHIZOME_AGENT_MODEL", "")
	t.Setenv("RHIZOME_AGENT_MODE", "")

	err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			HostURL:           "https://rhizome.defaults.test",
			WorkspaceID:       "ws-defaults",
			WorkspacePassword: "pw-defaults",
			OwnerUserID:       "owner-defaults",
			LLMBackend:        llmBackendCodex,
			Model:             "gpt-5.4-mini",
			Role:              "reviewer",
			Capabilities:      []string{"tool.call", "local.fs.read"},
		},
	})
	if err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", workdir})
	if err != nil {
		t.Fatalf("loadRuntimeConfigFromArgs() error: %v", err)
	}

	if cfg.RhizomeHost != "https://rhizome.defaults.test" {
		t.Fatalf("expected host from manager defaults, got %q", cfg.RhizomeHost)
	}
	if cfg.WorkspaceID != "ws-defaults" || cfg.WorkspacePassword != "pw-defaults" {
		t.Fatalf("expected workspace defaults from manager registry, got workspace=%q password=%q", cfg.WorkspaceID, cfg.WorkspacePassword)
	}
	if cfg.OwnerUserID != "owner-defaults" {
		t.Fatalf("expected owner from manager defaults, got %q", cfg.OwnerUserID)
	}
	if cfg.ProviderID != "" || cfg.ModelOverride != "" || cfg.GroupID != "" {
		t.Fatalf("expected manager defaults test to keep provider binding empty without explicit profile, got provider=%q model_override=%q group=%q", cfg.ProviderID, cfg.ModelOverride, cfg.GroupID)
	}
	if cfg.LLMBackend != llmBackendCodex || cfg.Model != "gpt-5.4-mini" {
		t.Fatalf("expected llm defaults from manager registry, got backend=%q model=%q", cfg.LLMBackend, cfg.Model)
	}
	if cfg.Role != "reviewer" {
		t.Fatalf("expected role from manager defaults, got %q", cfg.Role)
	}
}

func TestLoadRuntimeConfigFromArgsPrefersRegisteredExecutorDefaultsOverLooseLocalProfile(t *testing.T) {
	workdir := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_HOST", "")
	t.Setenv("RHIZOME_RPC", "")
	t.Setenv("RHIZOME_TOKEN", "")
	t.Setenv("RHIZOME_WORKSPACE_ID", "")
	t.Setenv("RHIZOME_AGENT_ID", "")
	t.Setenv("RHIZOME_AGENT_NAME", "")
	t.Setenv("RHIZOME_OWNER_USER_ID", "")
	t.Setenv("RHIZOME_AGENT_LLM_BACKEND", "")
	t.Setenv("RHIZOME_AGENT_MODEL", "")
	t.Setenv("RHIZOME_AGENT_MODE", "")

	err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		Mode:            string(RuntimeModeDaemon),
		ProtocolVersion: "rnar/v0-stale",
		RPCEndpoint:     "https://rhizome.test/rpc",
		HostURL:         "https://rhizome.test",
		WorkspaceID:     "ws-stale",
		AgentID:         "agent-stale",
		DisplayName:     "Agent Stale",
		OwnerUserID:     "owner-requested",
		Role:            "generalist",
		Capabilities:    []string{"tool.call", "local.shell"},
		RegisteredExecutor: RegisteredExecutorIdentity{
			AgentID:         "agent-registered",
			WorkspaceID:     "ws-registered",
			DisplayName:     "Agent Registered",
			OwnerUserID:     "owner-registered",
			Role:            "reviewer",
			ProtocolVersion: "rnar/v1",
			Capabilities:    []string{"tool.call"},
		},
	})
	if err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", workdir})
	if err != nil {
		t.Fatalf("loadRuntimeConfigFromArgs() error: %v", err)
	}
	if cfg.OwnerUserID != "owner-registered" {
		t.Fatalf("expected registered owner to drive defaults, got %q", cfg.OwnerUserID)
	}
	if cfg.WorkspaceID != "ws-registered" {
		t.Fatalf("expected registered workspace id to drive defaults, got %q", cfg.WorkspaceID)
	}
	if cfg.AgentID != "agent-registered" {
		t.Fatalf("expected registered agent id to drive defaults, got %q", cfg.AgentID)
	}
	if cfg.DisplayName != "Agent Registered" {
		t.Fatalf("expected registered display name to drive defaults, got %q", cfg.DisplayName)
	}
	if cfg.Role != "reviewer" {
		t.Fatalf("expected registered role to drive defaults, got %q", cfg.Role)
	}
	if cfg.ProtocolVersion != "rnar/v1" {
		t.Fatalf("expected registered protocol version to drive defaults, got %q", cfg.ProtocolVersion)
	}
	if len(cfg.Capabilities) != 1 || cfg.Capabilities[0] != "tool.call" {
		t.Fatalf("expected registered capabilities to drive defaults, got %+v", cfg.Capabilities)
	}
}

func TestLoadRuntimeConfigFromArgsManagedProcessIgnoresSharedBotRegistryDefaults(t *testing.T) {
	workdir := t.TempDir()
	home := t.TempDir()
	configRoot := filepath.Join(t.TempDir(), ".runtime-config")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_HOST", "")
	t.Setenv("RHIZOME_RPC", "")
	t.Setenv("RHIZOME_TOKEN", "")
	t.Setenv("RHIZOME_WORKSPACE_ID", "")
	t.Setenv("RHIZOME_AGENT_ID", "")
	t.Setenv("RHIZOME_AGENT_NAME", "")
	t.Setenv("RHIZOME_OWNER_USER_ID", "")
	t.Setenv("RHIZOME_AGENT_LLM_BACKEND", "")
	t.Setenv("RHIZOME_AGENT_MODEL", "")
	t.Setenv("RHIZOME_AGENT_MODE", "")
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentConfigRootFlag, configRoot)

	if err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			HostURL:           "https://shared-manager.test",
			WorkspaceID:       "ws-shared",
			WorkspacePassword: "pw-shared",
			OwnerUserID:       "owner-shared",
			LLMBackend:        llmBackendCodex,
			Model:             "gpt-shared",
			Role:              "reviewer",
			Capabilities:      []string{"tool.call"},
		},
	}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:           "https://local-managed.test",
		RPCEndpoint:       "https://local-managed.test/rpc",
		WorkspaceID:       "ws-local-managed",
		WorkspacePassword: "pw-local-managed",
		AgentID:           "agent-local-managed",
		DisplayName:       "Agent Local Managed",
		OwnerUserID:       "owner-local-managed",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", workdir})
	if err != nil {
		t.Fatalf("loadRuntimeConfigFromArgs() error: %v", err)
	}
	if cfg.RhizomeHost != "https://local-managed.test" {
		t.Fatalf("expected managed runtime to use local profile host, got %q", cfg.RhizomeHost)
	}
	if cfg.WorkspaceID != "ws-local-managed" || cfg.WorkspacePassword != "pw-local-managed" {
		t.Fatalf("expected managed runtime to ignore shared bot registry workspace defaults, got workspace=%q password=%q", cfg.WorkspaceID, cfg.WorkspacePassword)
	}
	if cfg.OwnerUserID != "owner-local-managed" {
		t.Fatalf("expected managed runtime to ignore shared bot registry owner default, got %q", cfg.OwnerUserID)
	}
}

func TestLoadRuntimeConfigFromArgsManagedRealPilotDefaultsBudgetAndTimeouts(t *testing.T) {
	workdir := t.TempDir()
	home := t.TempDir()
	configRoot := filepath.Join(t.TempDir(), ".runtime-config")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv("RHIZOME_HOST", "")
	t.Setenv("RHIZOME_RPC", "")
	t.Setenv("RHIZOME_TOKEN", "")
	t.Setenv("RHIZOME_WORKSPACE_ID", "")
	t.Setenv("RHIZOME_AGENT_ID", "")
	t.Setenv("RHIZOME_AGENT_NAME", "")
	t.Setenv("RHIZOME_OWNER_USER_ID", "")
	t.Setenv("RHIZOME_REAL_LLM_PILOT", "")
	t.Setenv("RHIZOME_AGENT_LLM_BACKEND", "")
	t.Setenv("RHIZOME_AGENT_MODEL", "")
	t.Setenv("RHIZOME_AGENT_MODE", "")

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		Mode:                     string(RuntimeModeDaemon),
		ProviderID:               "codex",
		LLMBackend:               llmBackendCodex,
		Model:                    "gpt-5.4",
		RealLLMPilot:             true,
		CoordinationMode:         CoordinationModeTrustFirst,
		RPCEndpoint:              "https://rhizome.test/rpc",
		HostURL:                  "https://rhizome.test",
		WorkspaceID:              "rhizome-main",
		AgentID:                  "beta",
		OwnerUserID:              "owner-1",
		ProviderCallTimeoutSec:   600,
		MaxProviderRetryAttempts: 2,
		MaxToolLoopIterations:    20,
		BudgetAccountID:          "",
		BudgetHardLimitMicros:    0,
		BudgetReserveMicros:      0,
		BudgetMicrosPerToken:     0,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentConfigRootFlag, configRoot)

	cfg, err := loadRuntimeConfigFromArgs([]string{"--workdir", workdir})
	if err != nil {
		t.Fatalf("loadRuntimeConfigFromArgs() error: %v", err)
	}
	if !cfg.RealLLMPilot {
		t.Fatal("expected managed real provider config to enable real LLM pilot")
	}
	if cfg.BudgetAccountID != "pilot-agent-beta" || cfg.BudgetHardLimitMicros != managedRealPilotDefaultBudgetHardLimitMicros || cfg.BudgetReserveMicros != managedRealPilotDefaultBudgetReserveMicros || cfg.BudgetMicrosPerToken != managedRealPilotDefaultBudgetMicrosPerToken {
		t.Fatalf("expected managed budget defaults, got account=%q hard=%d reserve=%d price=%d", cfg.BudgetAccountID, cfg.BudgetHardLimitMicros, cfg.BudgetReserveMicros, cfg.BudgetMicrosPerToken)
	}
	if cfg.ProviderCallTimeout != managedRealPilotDefaultProviderCallTimeout || cfg.PlannerCycleTimeout != managedRealPilotDefaultProviderCallTimeout {
		t.Fatalf("expected managed provider/planner timeout defaults, got provider=%s planner=%s", cfg.ProviderCallTimeout, cfg.PlannerCycleTimeout)
	}
	if cfg.MaxProviderRetryAttempts != realLLMPilotMaxProviderRetryAttempts || cfg.MaxToolLoopIterations != defaultToolLoopIterations {
		t.Fatalf("expected managed loop bounds, got retries=%d iterations=%d", cfg.MaxProviderRetryAttempts, cfg.MaxToolLoopIterations)
	}
}
