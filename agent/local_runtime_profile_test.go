package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalRuntimeProfileToolLoopCompactionRoundTrip(t *testing.T) {
	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		WorkspaceID:        "rhizome-main",
		AgentID:            "lyrica",
		ToolLoopCompaction: true,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error = %v", err)
	}
	got := LoadLocalRuntimeProfile(workdir)
	if !got.ToolLoopCompaction {
		t.Fatalf("expected tool_loop_compaction to round-trip true, got %+v", got)
	}

	cfg := runtimeConfigFromLocalRuntimeProfile(got)
	if !cfg.ToolLoopCompaction {
		t.Fatal("expected runtime config from profile to carry tool-loop compaction")
	}
	if back := localRuntimeProfileFromConfig(cfg); !back.ToolLoopCompaction {
		t.Fatal("expected config-to-profile conversion to preserve tool-loop compaction")
	}
}

func TestLocalRuntimeProfileRoundTrip(t *testing.T) {
	workdir := t.TempDir()
	profile := LocalRuntimeProfile{
		Mode:              string(RuntimeModeDaemon),
		ProtocolVersion:   "rnar/v1",
		LLMBackend:        llmBackendCodex,
		Model:             "gpt-5.4",
		RealLLMPilot:      true,
		CoordinationMode:  CoordinationModeTrustFirst,
		RPCEndpoint:       "https://rhizome.test/rpc",
		WorkspaceID:       "ws-1",
		WorkspaceName:     "Workspace One",
		WorkspacePassword: "test-workspace-password",
		AgentID:           "agent-1",
		DisplayName:       "Agent One",
		AgentToken:        "token-1",
		OwnerUserID:       "owner-1",
		Role:              "reviewer",
		Capabilities:      []string{"tool.call", "local.shell"},
		RegisteredExecutor: RegisteredExecutorIdentity{
			AgentID:         "agent-1",
			WorkspaceID:     "ws-1",
			DisplayName:     "Agent One",
			OwnerUserID:     "owner-registered",
			Role:            "reviewer",
			Status:          "REGISTERED",
			ProtocolVersion: "rnar/v1",
			Capabilities:    []string{"tool.call"},
			ConfirmedAt:     "2026-04-11T00:00:00Z",
		},
		HeartbeatSec:             120,
		PlannerSec:               45,
		BootstrapSec:             300,
		WatchdogSec:              90,
		ListenerTimeoutSec:       30,
		ListenerBatch:            12,
		ListenerLookbackHours:    48,
		UpdatesLimit:             33,
		MemorySyncSec:            60,
		PromotionSyncBatch:       7,
		RSPRolloutPhase:          string(RSPRolloutLive),
		TaskStallAfterSec:        1800,
		MaxPromptDocChars:        9000,
		MaxPromptSpecChars:       8000,
		MaxResultMemoryBodyChars: 2500,
		MaxToolLoopIterations:    4,
		MaxProviderRetryAttempts: 1,
		ProviderCallTimeoutSec:   10,
		SoakStopFile:             filepath.Join(workdir, "STOP_REAL_LLM_SOAK"),
		SoakRuntimeLimitSec:      3600,
		SoakMaxProviderCalls:     10,
	}
	if err := SaveLocalRuntimeProfile(workdir, profile); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	path := filepath.Join(workdir, localRuntimeProfileFilename)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("expected local profile to exist: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected local profile file to be non-empty")
	}

	got := LoadLocalRuntimeProfile(workdir)
	if got.AgentID != profile.AgentID || got.AgentToken != profile.AgentToken || got.WorkspaceID != profile.WorkspaceID {
		t.Fatalf("unexpected round-trip profile: %+v", got)
	}
	if got.HostURL != "https://rhizome.test" {
		t.Fatalf("expected host to derive from rpc endpoint, got %q", got.HostURL)
	}
	if got.HeartbeatSec != 120 || got.ListenerBatch != 12 || got.MemorySyncSec != 60 {
		t.Fatalf("expected operational tuning to round-trip, got %+v", got)
	}
	if got.RegisteredExecutor.AgentID != "agent-1" || got.RegisteredExecutor.OwnerUserID != "owner-registered" || len(got.RegisteredExecutor.Capabilities) != 1 {
		t.Fatalf("expected registered executor identity to round-trip, got %+v", got.RegisteredExecutor)
	}
	if got.MaxPromptDocChars != 9000 || got.MaxPromptSpecChars != 8000 || got.MaxResultMemoryBodyChars != 2500 {
		t.Fatalf("expected prompt tuning to round-trip, got %+v", got)
	}
	if !got.RealLLMPilot || got.MaxToolLoopIterations != 4 || got.MaxProviderRetryAttempts != 1 || got.ProviderCallTimeoutSec != 10 {
		t.Fatalf("expected real pilot tuning to round-trip, got %+v", got)
	}
	if got.CoordinationMode != CoordinationModeTrustFirst {
		t.Fatalf("expected coordination mode to round-trip, got %+v", got)
	}
	if got.SoakStopFile != profile.SoakStopFile || got.SoakRuntimeLimitSec != 3600 || got.SoakMaxProviderCalls != 10 {
		t.Fatalf("expected real soak guard tuning to round-trip, got %+v", got)
	}
}

func TestSaveLocalRuntimeProfileUsesManagedWriterForRegisteredWorkdir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID: "agent-managed",
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		AgentID:     "agent-managed",
		WorkspaceID: "ws-managed",
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	got := LoadLocalRuntimeProfile(workdir)
	if got.AgentID != "agent-managed" || got.WorkspaceID != "ws-managed" {
		t.Fatalf("expected managed local runtime write to succeed, got %+v", got)
	}
	if pathExists(managerStateMaterializationPath(agentRuntimeConfigRoot())) {
		t.Fatalf("expected managed local runtime journal to be cleared")
	}
	if pathExists(localRuntimeMaterializationMarkerPath(localRuntimeProfilePath(workdir))) {
		t.Fatalf("expected managed local runtime marker to be cleaned up")
	}
}

func TestSaveLocalRuntimeProfileUsesManagedWriterWhenRegistryIsCorrupt(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID: "agent-managed",
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := os.WriteFile(botRegistryPath(), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("WriteFile(botRegistryPath) error: %v", err)
	}

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		AgentID:     "agent-managed",
		WorkspaceID: "ws-managed",
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	got := LoadLocalRuntimeProfile(workdir)
	if got.AgentID != "agent-managed" || got.WorkspaceID != "ws-managed" {
		t.Fatalf("expected managed local runtime write to succeed with corrupt registry, got %+v", got)
	}
	if pathExists(managerStateMaterializationPath(agentRuntimeConfigRoot())) {
		t.Fatalf("expected managed local runtime journal to be cleared")
	}
	if pathExists(localRuntimeMaterializationMarkerPath(localRuntimeProfilePath(workdir))) {
		t.Fatalf("expected managed local runtime marker to be cleaned up")
	}
}

func TestRuntimeConfigFromLocalRuntimeProfilePrefersRegisteredExecutorIdentity(t *testing.T) {
	profile := LocalRuntimeProfile{
		ProtocolVersion:  "rnar/v0-stale",
		CoordinationMode: CoordinationModeTrustFirst,
		WorkspaceID:      "ws-stale",
		AgentID:          "agent-stale",
		DisplayName:      "Agent Stale",
		OwnerUserID:      "owner-requested",
		Role:             "generalist",
		Capabilities:     []string{"tool.call", "local.shell"},
		RegisteredExecutor: RegisteredExecutorIdentity{
			WorkspaceID:     "ws-registered",
			AgentID:         "agent-registered",
			DisplayName:     "Agent Registered",
			OwnerUserID:     "owner-registered",
			Role:            "reviewer",
			ProtocolVersion: "rnar/v1",
			Capabilities:    []string{"tool.call"},
		},
	}

	cfg := runtimeConfigFromLocalRuntimeProfile(profile)
	if cfg.ProtocolVersion != "rnar/v1" {
		t.Fatalf("expected registered protocol version to win, got %q", cfg.ProtocolVersion)
	}
	if cfg.WorkspaceID != "ws-registered" {
		t.Fatalf("expected registered workspace id to win, got %q", cfg.WorkspaceID)
	}
	if cfg.AgentID != "agent-registered" {
		t.Fatalf("expected registered agent id to win, got %q", cfg.AgentID)
	}
	if cfg.DisplayName != "Agent Registered" {
		t.Fatalf("expected registered display name to win, got %q", cfg.DisplayName)
	}
	if cfg.OwnerUserID != "owner-registered" {
		t.Fatalf("expected registered owner to win, got %q", cfg.OwnerUserID)
	}
	if cfg.Role != "reviewer" {
		t.Fatalf("expected registered role to win, got %q", cfg.Role)
	}
	if len(cfg.Capabilities) != 1 || cfg.Capabilities[0] != "tool.call" {
		t.Fatalf("expected registered capabilities to win, got %+v", cfg.Capabilities)
	}
	if cfg.CoordinationMode != CoordinationModeTrustFirst {
		t.Fatalf("expected coordination mode from local profile, got %q", cfg.CoordinationMode)
	}
}

func TestPersistRuntimeProfilesWritesLocalProfile(t *testing.T) {
	workdir := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	cfg := RuntimeConfig{
		Mode:              RuntimeModeDaemon,
		Workdir:           workdir,
		LLMBackend:        llmBackendCodex,
		Model:             defaultModel,
		RhizomeRPC:        "https://rhizome.test/rpc",
		RhizomeHost:       "https://rhizome.test",
		WorkspaceID:       "ws-1",
		WorkspaceName:     "Workspace One",
		WorkspacePassword: "test-workspace-password",
		AgentID:           "agent-1",
		DisplayName:       "Agent One",
		RhizomeToken:      "token-1",
		OwnerUserID:       "owner-1",
		Role:              "generalist",
	}
	cfg.ApplyDefaults()

	if err := persistRuntimeProfiles(workdir, cfg, AgentRegisterResult{}, nil); err != nil {
		t.Fatalf("persistRuntimeProfiles() error: %v", err)
	}
	got := LoadLocalRuntimeProfile(workdir)
	if got.AgentID != "agent-1" || got.AgentToken != "token-1" || got.LLMBackend != llmBackendCodex {
		t.Fatalf("unexpected persisted local profile: %+v", got)
	}
	if got.HeartbeatSec != durationSeconds(defaultHeartbeatInterval) || got.UpdatesLimit != defaultUpdatesLimit {
		t.Fatalf("expected persisted runtime defaults in local profile, got %+v", got)
	}
	if strings.TrimSpace(got.RegisteredExecutor.AgentID) != "" {
		t.Fatalf("expected partial persistence to avoid synthesizing registered executor identity, got %+v", got.RegisteredExecutor)
	}
	global := LoadRhizomeProfile()
	if global.AgentID != "agent-1" || global.AgentToken != "token-1" || global.WorkspaceID != "ws-1" {
		t.Fatalf("expected shared rhizome profile to stay in sync, got %+v", global)
	}
	record, ok := FindManagedAgent("agent-1")
	if !ok {
		t.Fatal("expected managed agent registry record to be created")
	}
	if record.Workdir != workdir || record.HostURL != "https://rhizome.test" {
		t.Fatalf("unexpected managed agent record: %+v", record)
	}
}

func TestPersistRuntimeProfilesStoresExplicitRegisteredExecutorIdentity(t *testing.T) {
	workdir := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	cfg := RuntimeConfig{
		Mode:              RuntimeModeDaemon,
		Workdir:           workdir,
		LLMBackend:        llmBackendCodex,
		Model:             defaultModel,
		RhizomeRPC:        "https://rhizome.test/rpc",
		RhizomeHost:       "https://rhizome.test",
		WorkspaceID:       "ws-1",
		WorkspaceName:     "Workspace One",
		WorkspacePassword: "test-workspace-password",
		AgentID:           "agent-1",
		DisplayName:       "Agent One",
		RhizomeToken:      "token-1",
		OwnerUserID:       "owner-requested",
		Role:              "generalist",
		Capabilities:      []string{"tool.call", "local.shell"},
	}
	cfg.ApplyDefaults()

	registered := AgentRegisterResult{
		AgentID:       "agent-1",
		DisplayName:   "Agent One",
		Token:         "token-1",
		WorkspaceID:   "ws-1",
		WorkspaceName: "Workspace One",
		HostURL:       "https://rhizome.test",
		Agent: AgentRecord{
			AgentID:         "agent-1",
			WorkspaceID:     "ws-1",
			OwnerUserID:     "owner-registered",
			DisplayName:     "Agent One",
			Role:            "reviewer",
			Status:          "REGISTERED",
			ProtocolVersion: "rnar/v1",
			Capabilities:    []string{"tool.call"},
			UpdatedAt:       "2026-04-11T00:00:00Z",
		},
	}

	if err := persistRuntimeProfiles(workdir, cfg, registered, nil); err != nil {
		t.Fatalf("persistRuntimeProfiles() error: %v", err)
	}
	got := LoadLocalRuntimeProfile(workdir)
	if got.OwnerUserID != "owner-requested" {
		t.Fatalf("expected runtime profile to preserve requested owner, got %+v", got)
	}
	if got.RegisteredExecutor.OwnerUserID != "owner-registered" || got.RegisteredExecutor.Role != "reviewer" {
		t.Fatalf("expected explicit registered executor identity to persist, got %+v", got.RegisteredExecutor)
	}
	if len(got.RegisteredExecutor.Capabilities) != 1 || got.RegisteredExecutor.Capabilities[0] != "tool.call" {
		t.Fatalf("expected registered capabilities to persist, got %+v", got.RegisteredExecutor)
	}

	profile := LoadRhizomeProfile()
	if profile.OwnerUserID != "owner-registered" {
		t.Fatalf("expected rhizome connection profile to prefer registered owner, got %+v", profile)
	}
	record, ok := FindManagedAgent("agent-1")
	if !ok {
		t.Fatal("expected managed agent registry record to be created")
	}
	if record.Workdir != workdir || record.Model != defaultModel || record.LLMBackend != llmBackendCodex {
		t.Fatalf("unexpected managed agent record: %+v", record)
	}
}
