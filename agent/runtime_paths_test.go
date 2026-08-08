package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestAgentRuntimeConfigRootUsesManagedOverride(t *testing.T) {
	root := filepath.Join(t.TempDir(), ".runtime-config")
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentConfigRootFlag, root)

	if got := agentRuntimeConfigRoot(); got != root {
		t.Fatalf("agentRuntimeConfigRoot() = %q, want %q", got, root)
	}
	if got := keyPath(); got != filepath.Join(root, "openai_key") {
		t.Fatalf("keyPath() = %q", got)
	}
	if got := rhizomeProfilePath(); got != filepath.Join(root, "rhizome_profile.json") {
		t.Fatalf("rhizomeProfilePath() = %q", got)
	}
	if got := messageInboxPath("ws", "agent"); got != filepath.Join(root, "inbox", "ws", "agent.json") {
		t.Fatalf("messageInboxPath() = %q", got)
	}
	if got := localMemoryStorePath("ws", "agent"); got != filepath.Join(root, "memory", "ws", "agent", "state.db") {
		t.Fatalf("localMemoryStorePath() = %q", got)
	}
}

func TestManagedAgentHomeRootPathUsesWorkdir(t *testing.T) {
	workdir := t.TempDir()
	if got := managedAgentHomeRootPath(workdir); got != filepath.Join(workdir, ".home") {
		t.Fatalf("managedAgentHomeRootPath() = %q", got)
	}
}

func TestAgentRuntimeConfigRootManagedMissingEnvFailsClosed(t *testing.T) {
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentConfigRootFlag, "")

	if got := agentRuntimeConfigRoot(); got != "" {
		t.Fatalf("agentRuntimeConfigRoot() = %q, want empty", got)
	}
	if _, err := OpenMessageInbox("ws", "agent"); err == nil {
		t.Fatal("expected OpenMessageInbox to fail when managed config root is missing")
	}
	if _, err := OpenLocalMemoryStore("ws", "agent"); err == nil {
		t.Fatal("expected OpenLocalMemoryStore to fail when managed config root is missing")
	}
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{HostURL: "https://example.com"}); err == nil {
		t.Fatal("expected SaveRhizomeProfile to fail when managed config root is missing")
	}
}

func TestAgentRuntimeCodexHomeUsesOverrideAndExecutableDiscovery(t *testing.T) {
	root := filepath.Join(t.TempDir(), ".codex-home")
	sandboxBin := filepath.Join(root, ".sandbox-bin")
	if err := os.MkdirAll(sandboxBin, 0o755); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	name := "codex"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	executable := filepath.Join(sandboxBin, name)
	if err := os.WriteFile(executable, []byte("stub"), 0o755); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	authJSON := `{"auth_mode":"chatgpt","tokens":{"access_token":"token"}}`
	if err := os.WriteFile(filepath.Join(root, "auth.json"), []byte(authJSON), 0o600); err != nil {
		t.Fatalf("WriteFile(auth.json) error: %v", err)
	}

	t.Setenv(managedAgentCodexHomeFlag, root)

	if got := agentRuntimeCodexHome(); got != root {
		t.Fatalf("agentRuntimeCodexHome() = %q, want %q", got, root)
	}
	if !hasChatGPTCodexSession() {
		t.Fatal("expected hasChatGPTCodexSession() to use RHIZOME_AGENT_CODEX_HOME")
	}
	if got := findCodexExecutable(); got != executable {
		t.Fatalf("findCodexExecutable() = %q, want %q", got, executable)
	}
}

func TestCodexExecEnvMapsManagedCodexHomeToCodexHome(t *testing.T) {
	root := filepath.Join(t.TempDir(), ".codex-home")
	t.Setenv(managedAgentCodexHomeFlag, root)
	t.Setenv("CODEX_HOME", "C:\\shared-codex-home")

	env := codexExecEnv()
	found := ""
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok && strings.EqualFold(key, "CODEX_HOME") {
			found = value
			break
		}
	}
	if found != root {
		t.Fatalf("CODEX_HOME env = %q, want %q", found, root)
	}
}

func TestAgentRuntimeCodexHomePartnerManagedMissingEnvFailsClosed(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalShellFlag, "")
	t.Setenv(managedAgentCodexHomeFlag, "")

	if got := agentRuntimeCodexHome(); got != "" {
		t.Fatalf("agentRuntimeCodexHome() = %q, want empty", got)
	}
}

func TestFindCodexExecutablePartnerManagedIgnoresSharedEnvAndPath(t *testing.T) {
	sharedDir := t.TempDir()
	name := "codex"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	sharedExecutable := filepath.Join(sharedDir, name)
	if err := os.WriteFile(sharedExecutable, []byte("stub"), 0o755); err != nil {
		t.Fatalf("WriteFile(shared executable) error: %v", err)
	}

	isolatedHome := filepath.Join(t.TempDir(), ".codex-home")
	if err := os.MkdirAll(isolatedHome, 0o755); err != nil {
		t.Fatalf("MkdirAll(isolated home) error: %v", err)
	}

	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalShellFlag, "")
	t.Setenv(managedAgentCodexHomeFlag, isolatedHome)
	t.Setenv("CODEX_CLI_PATH", sharedExecutable)
	t.Setenv("CUSTOM_CLI_PATH", sharedExecutable)
	t.Setenv("PATH", sharedDir)

	if got := findCodexExecutable(); got != "" {
		t.Fatalf("findCodexExecutable() = %q, want empty for partner-managed runtime without isolated executable", got)
	}
}

func TestFindCodexExecutablePartnerManagedPrefersIsolatedHomeOverSharedOverrides(t *testing.T) {
	sharedDir := t.TempDir()
	name := "codex"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	sharedExecutable := filepath.Join(sharedDir, name)
	if err := os.WriteFile(sharedExecutable, []byte("shared"), 0o755); err != nil {
		t.Fatalf("WriteFile(shared executable) error: %v", err)
	}

	isolatedHome := filepath.Join(t.TempDir(), ".codex-home")
	sandboxBin := filepath.Join(isolatedHome, ".sandbox-bin")
	if err := os.MkdirAll(sandboxBin, 0o755); err != nil {
		t.Fatalf("MkdirAll(sandboxBin) error: %v", err)
	}
	isolatedExecutable := filepath.Join(sandboxBin, name)
	if err := os.WriteFile(isolatedExecutable, []byte("isolated"), 0o755); err != nil {
		t.Fatalf("WriteFile(isolated executable) error: %v", err)
	}

	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalShellFlag, "")
	t.Setenv(managedAgentCodexHomeFlag, isolatedHome)
	t.Setenv("CODEX_CLI_PATH", sharedExecutable)
	t.Setenv("CUSTOM_CLI_PATH", sharedExecutable)
	t.Setenv("PATH", sharedDir)

	if got := findCodexExecutable(); got != isolatedExecutable {
		t.Fatalf("findCodexExecutable() = %q, want isolated partner executable %q", got, isolatedExecutable)
	}
}

func TestNewLLMPartnerManagedCodexBackendRequiresManagedCodexHome(t *testing.T) {
	workdir := t.TempDir()
	executable := filepath.Join(t.TempDir(), "codex")
	if runtime.GOOS == "windows" {
		executable += ".exe"
	}
	if err := os.WriteFile(executable, []byte("stub"), 0o755); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalShellFlag, "")
	t.Setenv(managedAgentCodexHomeFlag, "")
	t.Setenv("CODEX_CLI_PATH", executable)

	_, err := NewLLM(RuntimeConfig{
		LLMBackend: llmBackendCodex,
		Model:      defaultModel,
		Workdir:    workdir,
	})
	if err == nil || !strings.Contains(err.Error(), "RHIZOME_AGENT_CODEX_HOME") {
		t.Fatalf("expected managed codex backend to fail closed on missing RHIZOME_AGENT_CODEX_HOME, got %v", err)
	}
}

func TestNewLLMPartnerManagedCodexBackendRejectsSharedExecutableFallback(t *testing.T) {
	workdir := t.TempDir()
	sharedDir := t.TempDir()
	name := "codex"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	sharedExecutable := filepath.Join(sharedDir, name)
	if err := os.WriteFile(sharedExecutable, []byte("stub"), 0o755); err != nil {
		t.Fatalf("WriteFile(shared executable) error: %v", err)
	}

	isolatedHome := filepath.Join(t.TempDir(), ".codex-home")
	if err := os.MkdirAll(isolatedHome, 0o755); err != nil {
		t.Fatalf("MkdirAll(isolated home) error: %v", err)
	}

	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalShellFlag, "")
	t.Setenv(managedAgentCodexHomeFlag, isolatedHome)
	t.Setenv("CODEX_CLI_PATH", sharedExecutable)
	t.Setenv("CUSTOM_CLI_PATH", sharedExecutable)
	t.Setenv("PATH", sharedDir)

	_, err := NewLLM(RuntimeConfig{
		LLMBackend: llmBackendCodex,
		Model:      defaultModel,
		Workdir:    workdir,
	})
	if err == nil || !strings.Contains(err.Error(), "no codex executable") {
		t.Fatalf("expected partner-managed codex backend to reject shared executable fallback, got %v", err)
	}
}

func TestLoadAPIKeyPartnerManagedRequiresExplicitLocalCredential(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalShellFlag, "")
	t.Setenv("OPENAI_API_KEY", "")

	_, err := loadAPIKey("", ProviderRecord{})
	if err == nil || !strings.Contains(err.Error(), "explicit local OpenAI credential") {
		t.Fatalf("expected partner managed runtime to fail closed without local OpenAI key, got %v", err)
	}
}
