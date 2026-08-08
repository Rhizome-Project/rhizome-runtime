package main

import (
	"strings"
	"testing"
)

func setShellContainmentTestHome(t *testing.T, ownerUserID string) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	if ownerUserID != "" {
		if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: ownerUserID}); err != nil {
			t.Fatalf("SaveRhizomeProfile() error: %v", err)
		}
	}
}

func TestNewRuntimeManagedPartnerDisablesGenericShell(t *testing.T) {
	t.Setenv(managedAgentEnvFlag, "1")

	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeTUI,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-partner",
		OwnerUserID: "partner-owner",
	}, nil)

	if runtime == nil || runtime.agent == nil || runtime.agent.registry == nil {
		t.Fatal("expected runtime agent registry to be initialized")
	}
	if _, ok := runtime.agent.registry.Get("shell"); ok {
		t.Fatalf("expected managed partner runtime to omit generic shell")
	}
	if _, ok := runtime.agent.registry.Get("read_file"); !ok {
		t.Fatalf("expected managed partner runtime to retain bounded file tools")
	}
}

func TestNewRuntimeManagedPartnerDisablesLocalMutationTools(t *testing.T) {
	t.Setenv(managedAgentEnvFlag, "1")

	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeTUI,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-partner",
		OwnerUserID: "partner-owner",
	}, nil)

	if runtime == nil || runtime.agent == nil || runtime.agent.registry == nil {
		t.Fatal("expected runtime agent registry to be initialized")
	}
	if _, ok := runtime.agent.registry.Get("write_file"); ok {
		t.Fatalf("expected managed partner runtime to omit local write_file")
	}
	if _, ok := runtime.agent.registry.Get("memory_write"); ok {
		t.Fatalf("expected managed partner runtime to omit local memory_write")
	}
	if _, ok := runtime.agent.registry.Get("daily_note"); ok {
		t.Fatalf("expected managed partner runtime to omit local daily_note")
	}
	if _, ok := runtime.agent.registry.Get("memory_read"); !ok {
		t.Fatalf("expected managed partner runtime to retain local memory_read")
	}
}

func TestNewRuntimeManagedRequiresExplicitManagerShellFlag(t *testing.T) {
	t.Setenv(managedAgentEnvFlag, "1")

	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeTUI,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-local",
		OwnerUserID: "developer",
	}, nil)

	if runtime == nil || runtime.agent == nil || runtime.agent.registry == nil {
		t.Fatal("expected runtime agent registry to be initialized")
	}
	if _, ok := runtime.agent.registry.Get("shell"); ok {
		t.Fatalf("expected managed runtime to require explicit manager shell flag")
	}
}

func TestNewRuntimeManagedRequiresExplicitManagerMutationFlag(t *testing.T) {
	t.Setenv(managedAgentEnvFlag, "1")

	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeTUI,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-local",
		OwnerUserID: "developer",
	}, nil)

	if runtime == nil || runtime.agent == nil || runtime.agent.registry == nil {
		t.Fatal("expected runtime agent registry to be initialized")
	}
	if _, ok := runtime.agent.registry.Get("write_file"); ok {
		t.Fatalf("expected managed runtime to require explicit manager mutation flag")
	}
	if _, ok := runtime.agent.registry.Get("memory_write"); ok {
		t.Fatalf("expected managed runtime to require explicit manager mutation flag")
	}
	if _, ok := runtime.agent.registry.Get("daily_note"); ok {
		t.Fatalf("expected managed runtime to require explicit manager mutation flag")
	}
}

func TestNewRuntimeManagedTrustedOwnerKeepsGenericShell(t *testing.T) {
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalShellFlag, "1")

	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeTUI,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-local",
		OwnerUserID: "developer",
	}, nil)

	if runtime == nil || runtime.agent == nil || runtime.agent.registry == nil {
		t.Fatal("expected runtime agent registry to be initialized")
	}
	if _, ok := runtime.agent.registry.Get("shell"); !ok {
		t.Fatalf("expected trusted managed runtime to keep generic shell")
	}
}

func TestNewRuntimeManagedTrustedOwnerKeepsLocalMutationTools(t *testing.T) {
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalMutationFlag, "1")

	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeTUI,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-local",
		OwnerUserID: "developer",
	}, nil)

	if runtime == nil || runtime.agent == nil || runtime.agent.registry == nil {
		t.Fatal("expected runtime agent registry to be initialized")
	}
	if _, ok := runtime.agent.registry.Get("write_file"); !ok {
		t.Fatalf("expected trusted managed runtime to keep local write_file")
	}
	if _, ok := runtime.agent.registry.Get("memory_write"); !ok {
		t.Fatalf("expected trusted managed runtime to keep local memory_write")
	}
	if _, ok := runtime.agent.registry.Get("daily_note"); !ok {
		t.Fatalf("expected trusted managed runtime to keep local daily_note")
	}
}

func TestNewRuntimeManagedTrustedAliasKeepsLocalMutationTools(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentConfigRootFlag, t.TempDir())
	t.Setenv(managedAgentAllowLocalMutationFlag, "1")
	t.Setenv("RHIZOME_OWNER_USER_ID", "user-live")
	t.Setenv(managedAgentTrustedOwnerUserIDsFlag, "user-live,developer")

	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeTUI,
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		AgentID:     "beta",
		OwnerUserID: "developer",
	}, nil)

	if runtime == nil || runtime.agent == nil || runtime.agent.registry == nil {
		t.Fatal("expected runtime agent registry to be initialized")
	}
	if _, ok := runtime.agent.registry.Get("write_file"); !ok {
		t.Fatalf("expected trusted local alias runtime to keep local write_file")
	}
}

func TestAgentSetDynamicToolsKeepsShellDisabledAcrossRefresh(t *testing.T) {
	agent := &Agent{
		Workdir:           t.TempDir(),
		DisableLocalShell: true,
	}

	agent.SetDynamicTools(nil)
	if _, ok := agent.registry.Get("shell"); ok {
		t.Fatalf("expected first tool registration pass to omit shell")
	}

	agent.SetDynamicTools(nil)
	if _, ok := agent.registry.Get("shell"); ok {
		t.Fatalf("expected repeated tool registration to keep shell disabled")
	}
}

func TestAgentSetDynamicToolsKeepsLocalMutationDisabledAcrossRefresh(t *testing.T) {
	agent := &Agent{
		Workdir:              t.TempDir(),
		DisableLocalMutation: true,
	}

	agent.SetDynamicTools(nil)
	if _, ok := agent.registry.Get("write_file"); ok {
		t.Fatalf("expected first tool registration pass to omit write_file")
	}
	if _, ok := agent.registry.Get("memory_write"); ok {
		t.Fatalf("expected first tool registration pass to omit memory_write")
	}
	if _, ok := agent.registry.Get("daily_note"); ok {
		t.Fatalf("expected first tool registration pass to omit daily_note")
	}

	agent.SetDynamicTools(nil)
	if _, ok := agent.registry.Get("write_file"); ok {
		t.Fatalf("expected repeated tool registration to keep local mutation disabled")
	}
	if _, ok := agent.registry.Get("memory_write"); ok {
		t.Fatalf("expected repeated tool registration to keep local mutation disabled")
	}
	if _, ok := agent.registry.Get("daily_note"); ok {
		t.Fatalf("expected repeated tool registration to keep local mutation disabled")
	}
}

func TestBuildLocalChatToolRegistryDisablesShellForPartnerOwner(t *testing.T) {
	setShellContainmentTestHome(t, "developer")

	registry := buildLocalChatToolRegistry(ManagedAgentRecord{
		AgentID:     "partner-agent",
		Workdir:     t.TempDir(),
		OwnerUserID: "partner-owner",
	})

	if _, ok := registry.Get("shell"); ok {
		t.Fatalf("expected partner-owned local chat registry to omit generic shell")
	}
	if _, ok := registry.Get("read_file"); !ok {
		t.Fatalf("expected partner-owned local chat registry to retain bounded file tools")
	}
}

func TestBuildLocalChatToolRegistryDisablesLocalMutationForPartnerOwner(t *testing.T) {
	setShellContainmentTestHome(t, "developer")

	registry := buildLocalChatToolRegistry(ManagedAgentRecord{
		AgentID:     "partner-agent",
		Workdir:     t.TempDir(),
		OwnerUserID: "partner-owner",
	})

	if _, ok := registry.Get("write_file"); ok {
		t.Fatalf("expected partner-owned local chat registry to omit local write_file")
	}
	if _, ok := registry.Get("memory_write"); ok {
		t.Fatalf("expected partner-owned local chat registry to omit local memory_write")
	}
	if _, ok := registry.Get("daily_note"); ok {
		t.Fatalf("expected partner-owned local chat registry to omit local daily_note")
	}
	if _, ok := registry.Get("memory_read"); !ok {
		t.Fatalf("expected partner-owned local chat registry to retain local memory_read")
	}
}

func TestBuildLocalChatToolRegistryUsesManagerOwnedOwnerPolicy(t *testing.T) {
	setShellContainmentTestHome(t, "developer")

	registry := buildLocalChatToolRegistry(ManagedAgentRecord{
		AgentID:     "lyrica",
		Workdir:     t.TempDir(),
		OwnerUserID: "partner-owner",
	})

	if _, ok := registry.Get("shell"); ok {
		t.Fatalf("expected manager-owned partner owner policy to deny generic shell")
	}
}

func TestBuildLocalChatToolRegistryKeepsTrustedOwnerReadOnlyByDefault(t *testing.T) {
	setShellContainmentTestHome(t, "developer")

	registry := buildLocalChatToolRegistry(ManagedAgentRecord{
		AgentID:     "lyrica",
		Workdir:     t.TempDir(),
		OwnerUserID: "developer",
	})

	if _, ok := registry.Get("write_file"); ok {
		t.Fatalf("expected trusted owner local inspect registry to omit write_file by default")
	}
	if _, ok := registry.Get("memory_write"); ok {
		t.Fatalf("expected trusted owner local inspect registry to omit memory_write by default")
	}
	if _, ok := registry.Get("daily_note"); ok {
		t.Fatalf("expected trusted owner local inspect registry to omit daily_note by default")
	}
	if _, ok := registry.Get("memory_read"); !ok {
		t.Fatalf("expected trusted owner local inspect registry to retain read tools")
	}
}

func TestBuildLocalChatToolRegistryOmitsShellForTrustedOwnerByDefault(t *testing.T) {
	setShellContainmentTestHome(t, "developer")

	registry := buildLocalChatToolRegistry(ManagedAgentRecord{
		AgentID:     "lyrica",
		Workdir:     t.TempDir(),
		OwnerUserID: "developer",
	})

	if _, ok := registry.Get("shell"); ok {
		t.Fatalf("expected trusted owner local inspect registry to omit shell by default")
	}
}

func TestEnsureManagedRecordAllowsInlineLocalChatRejectsPartnerOwner(t *testing.T) {
	setShellContainmentTestHome(t, "developer")

	err := ensureManagedRecordAllowsInlineLocalChat(ManagedAgentRecord{
		AgentID:     "partner-agent",
		Workdir:     t.TempDir(),
		OwnerUserID: "partner-owner",
	})
	if err == nil {
		t.Fatal("expected partner-owned inline local chat to be rejected")
	}
	if !strings.Contains(err.Error(), "inline local chat is disabled") {
		t.Fatalf("expected inline local chat rejection message, got %v", err)
	}
}

func TestEnsureManagedRecordAllowsInlineLocalChatAllowsTrustedOwner(t *testing.T) {
	setShellContainmentTestHome(t, "developer")

	if err := ensureManagedRecordAllowsInlineLocalChat(ManagedAgentRecord{
		AgentID:     "lyrica",
		Workdir:     t.TempDir(),
		OwnerUserID: "developer",
	}); err != nil {
		t.Fatalf("expected trusted owner inline local chat to remain allowed, got %v", err)
	}
}

func TestLoadInlineLocalChatConfigRejectsPartnerOwner(t *testing.T) {
	setShellContainmentTestHome(t, "developer")

	_, err := loadInlineLocalChatConfig(ManagedAgentRecord{
		AgentID:     "partner-agent",
		Workdir:     t.TempDir(),
		OwnerUserID: "partner-owner",
	})
	if err == nil {
		t.Fatal("expected partner-owned inline chat config load to be rejected")
	}
	if !strings.Contains(err.Error(), "inline local chat is disabled") {
		t.Fatalf("expected inline local chat rejection, got %v", err)
	}
}

func TestRunChatAgentRejectsPartnerManagedInlineChat(t *testing.T) {
	setShellContainmentTestHome(t, "developer")

	record := ManagedAgentRecord{
		AgentID:     "partner-agent",
		DisplayName: "Partner Agent",
		Workdir:     t.TempDir(),
		OwnerUserID: "partner-owner",
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	err := runChatAgent([]string{"partner-agent"})
	if err == nil {
		t.Fatal("expected partner-managed inline chat command to be rejected")
	}
	if !strings.Contains(err.Error(), "inline local chat is disabled") {
		t.Fatalf("expected inline local chat rejection, got %v", err)
	}
}

func TestManagerUIOpenAgentChatRejectsPartnerManagedInlineChat(t *testing.T) {
	setShellContainmentTestHome(t, "developer")

	ui := &ManagerUI{}
	err := ui.openAgentChat(ManagedAgentRecord{
		AgentID:     "partner-agent",
		Workdir:     t.TempDir(),
		OwnerUserID: "partner-owner",
	})
	if err == nil {
		t.Fatal("expected manager UI inline chat to reject partner-managed agent")
	}
	if !strings.Contains(err.Error(), "inline local chat is disabled") {
		t.Fatalf("expected inline local chat rejection, got %v", err)
	}
}
