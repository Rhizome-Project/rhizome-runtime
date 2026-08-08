package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractFlagValueSupportsSplitAndEqualsSyntax(t *testing.T) {
	args := []string{"--workdir", "C:\\agents\\beta"}
	if got := extractFlagValue(args, "workdir"); got != "C:\\agents\\beta" {
		t.Fatalf("expected split flag value, got %q", got)
	}

	args = []string{"--workdir=C:\\agents\\alpha"}
	if got := extractFlagValue(args, "workdir"); got != "C:\\agents\\alpha" {
		t.Fatalf("expected equals flag value, got %q", got)
	}
}

func TestSuggestAgentIDUsesFolderName(t *testing.T) {
	workdir := filepath.Join("C:\\agents", "beta-main")
	if got := suggestAgentID(workdir); got != "beta-main" {
		t.Fatalf("expected sanitized folder name, got %q", got)
	}
}

func TestDefaultFolderAgentNameUsesRawFolderName(t *testing.T) {
	workdir := filepath.Join("C:\\agents", "beta main")
	if got := defaultFolderAgentName(workdir); got != "beta main" {
		t.Fatalf("expected raw folder name, got %q", got)
	}
}

func TestHumanizeAgentID(t *testing.T) {
	if got := humanizeAgentID("beta-main"); got != "Beta Main" {
		t.Fatalf("unexpected humanized agent id: %q", got)
	}
}

func TestBuildOnboardStateDefaultsToRemoteRhizomeHost(t *testing.T) {
	t.Setenv("RHIZOME_HOST", "")
	t.Setenv("RHIZOME_RPC", "")
	state := buildOnboardState(filepath.Join("C:\\agents", "beta-01"), BotManagerDefaults{}, RhizomeConnectionProfile{}, LocalRuntimeProfile{}, AgentProfile{})
	if state.Runtime.RhizomeHost != defaultRhizomeHostURL {
		t.Fatalf("expected default host %q, got %q", defaultRhizomeHostURL, state.Runtime.RhizomeHost)
	}
	if state.Runtime.RhizomeRPC != defaultRhizomeHostURL+"/rpc" {
		t.Fatalf("expected default rpc to derive from host, got %q", state.Runtime.RhizomeRPC)
	}
	if state.Runtime.WorkspaceID != defaultWorkspaceID {
		t.Fatalf("expected default workspace id %q, got %q", defaultWorkspaceID, state.Runtime.WorkspaceID)
	}
}

func TestBuildOnboardStatePrefersFolderNameOverGlobalAgentProfile(t *testing.T) {
	global := RhizomeConnectionProfile{
		AgentID:     "global-agent",
		WorkspaceID: "global-ws",
	}
	state := buildOnboardState(filepath.Join("C:\\agents", "lyrica"), BotManagerDefaults{}, global, LocalRuntimeProfile{}, AgentProfile{})
	if state.Runtime.AgentID != "lyrica" {
		t.Fatalf("expected folder-based agent id, got %q", state.Runtime.AgentID)
	}
	if state.Runtime.DisplayName != "lyrica" {
		t.Fatalf("expected folder-based display name, got %q", state.Runtime.DisplayName)
	}
}

func TestBuildOnboardStatePrefersFolderNameOverStaleLocalRuntime(t *testing.T) {
	local := LocalRuntimeProfile{
		AgentID:     "agent-1",
		DisplayName: "Agent One",
		WorkspaceID: "ws-1",
	}
	state := buildOnboardState(filepath.Join("C:\\agents", "lyrica"), BotManagerDefaults{}, RhizomeConnectionProfile{}, local, AgentProfile{})
	if state.Runtime.AgentID != "lyrica" {
		t.Fatalf("expected folder-based agent id, got %q", state.Runtime.AgentID)
	}
	if state.Runtime.DisplayName != "lyrica" {
		t.Fatalf("expected folder-based display name, got %q", state.Runtime.DisplayName)
	}
	if state.Runtime.WorkspaceID != defaultWorkspaceID {
		t.Fatalf("expected default workspace id %q, got %q", defaultWorkspaceID, state.Runtime.WorkspaceID)
	}
}

func TestBuildOnboardStateUsesManagerDefaultsForNewAgents(t *testing.T) {
	t.Setenv("RHIZOME_COORDINATION_MODE", CoordinationModeStrict)
	defaults := BotManagerDefaults{
		HostURL:           "https://rhizome.defaults.test",
		WorkspaceID:       "ws-defaults",
		WorkspacePassword: "pw-defaults",
		OwnerUserID:       "owner-defaults",
		LLMBackend:        llmBackendCodex,
		Model:             "gpt-5.4-mini",
		CoordinationMode:  CoordinationModeTrustFirst,
		Role:              "reviewer",
		Capabilities:      []string{"tool.call", "local.fs.read"},
	}
	state := buildOnboardState(filepath.Join("C:\\agents", "new-agent"), defaults, RhizomeConnectionProfile{}, LocalRuntimeProfile{}, AgentProfile{})
	if state.Runtime.RhizomeHost != defaults.HostURL {
		t.Fatalf("expected manager default host %q, got %q", defaults.HostURL, state.Runtime.RhizomeHost)
	}
	if state.Runtime.WorkspaceID != defaults.WorkspaceID {
		t.Fatalf("expected manager default workspace id %q, got %q", defaults.WorkspaceID, state.Runtime.WorkspaceID)
	}
	if state.Runtime.OwnerUserID != defaults.OwnerUserID {
		t.Fatalf("expected manager default owner %q, got %q", defaults.OwnerUserID, state.Runtime.OwnerUserID)
	}
	if state.Runtime.LLMBackend != defaults.LLMBackend || state.Runtime.Model != defaults.Model {
		t.Fatalf("expected manager default llm settings, got backend=%q model=%q", state.Runtime.LLMBackend, state.Runtime.Model)
	}
	if state.Runtime.CoordinationMode != defaults.CoordinationMode {
		t.Fatalf("expected manager default coordination mode %q, got %q", defaults.CoordinationMode, state.Runtime.CoordinationMode)
	}
	if state.Runtime.Role != defaults.Role {
		t.Fatalf("expected manager default role %q, got %q", defaults.Role, state.Runtime.Role)
	}
	if len(state.Runtime.Capabilities) != len(defaults.Capabilities) {
		t.Fatalf("expected manager default capabilities, got %+v", state.Runtime.Capabilities)
	}
}

func TestBuildOnboardStatePrefersManagerDefaultsOverStaleLocalConnectionSettings(t *testing.T) {
	defaults := BotManagerDefaults{
		HostURL:           "https://rhizome.defaults.test",
		WorkspaceID:       "rhizome-main",
		WorkspacePassword: "pw-defaults",
		OwnerUserID:       "developer",
		LLMBackend:        llmBackendCodex,
		Model:             "gpt-5.4",
		CoordinationMode:  CoordinationModeTrustFirst,
		Capabilities:      []string{"tool.call", "workspace.docs"},
	}
	local := LocalRuntimeProfile{
		HostURL:           "http://127.0.0.1:52472",
		RPCEndpoint:       "http://127.0.0.1:52472/rpc",
		WorkspaceID:       "ws-local",
		WorkspacePassword: "pw-local",
		OwnerUserID:       "owner-local",
		LLMBackend:        llmBackendOpenAI,
		Model:             "gpt-local",
		CoordinationMode:  CoordinationModeStrict,
		Capabilities:      []string{"local.shell"},
	}

	state := buildOnboardState(filepath.Join("C:\\agents", "lyrica"), defaults, RhizomeConnectionProfile{}, local, AgentProfile{})
	if state.Runtime.RhizomeHost != defaults.HostURL {
		t.Fatalf("expected manager default host %q, got %q", defaults.HostURL, state.Runtime.RhizomeHost)
	}
	if state.Runtime.RhizomeRPC != defaults.HostURL+"/rpc" {
		t.Fatalf("expected rpc to derive from manager default host, got %q", state.Runtime.RhizomeRPC)
	}
	if state.Runtime.WorkspacePassword != defaults.WorkspacePassword {
		t.Fatalf("expected manager default workspace password %q, got %q", defaults.WorkspacePassword, state.Runtime.WorkspacePassword)
	}
	if state.Runtime.OwnerUserID != defaults.OwnerUserID {
		t.Fatalf("expected manager default owner %q, got %q", defaults.OwnerUserID, state.Runtime.OwnerUserID)
	}
	if state.Runtime.LLMBackend != defaults.LLMBackend || state.Runtime.Model != defaults.Model {
		t.Fatalf("expected manager default llm settings, got backend=%q model=%q", state.Runtime.LLMBackend, state.Runtime.Model)
	}
	if state.Runtime.CoordinationMode != defaults.CoordinationMode {
		t.Fatalf("expected manager default coordination mode %q, got %q", defaults.CoordinationMode, state.Runtime.CoordinationMode)
	}
	if len(state.Runtime.Capabilities) != len(defaults.Capabilities) {
		t.Fatalf("expected manager default capabilities, got %+v", state.Runtime.Capabilities)
	}
}

func TestBuildOnboardStatePrefersRegisteredExecutorIdentityOverLooseLocalRuntime(t *testing.T) {
	local := LocalRuntimeProfile{
		ProtocolVersion: "rnar/v0-stale",
		WorkspaceID:     "ws-local",
		AgentID:         "agent-bootstrap",
		DisplayName:     "Bootstrap Name",
		OwnerUserID:     "owner-requested",
		Role:            "generalist",
		Capabilities:    []string{"tool.call", "local.shell"},
		RegisteredExecutor: RegisteredExecutorIdentity{
			AgentID:         "lyrica-registered",
			WorkspaceID:     "ws-local",
			DisplayName:     "Lyrica Registered",
			OwnerUserID:     "owner-registered",
			Role:            "reviewer",
			ProtocolVersion: "rnar/v1",
			Capabilities:    []string{"tool.call"},
		},
	}

	state := buildOnboardState(filepath.Join("C:\\agents", "lyrica"), BotManagerDefaults{}, RhizomeConnectionProfile{}, local, AgentProfile{})
	if state.Runtime.AgentID != "lyrica-registered" {
		t.Fatalf("expected registered agent id to win over loose local runtime and folder suggestion, got %q", state.Runtime.AgentID)
	}
	if state.Runtime.DisplayName != "Lyrica Registered" {
		t.Fatalf("expected registered display name to win over loose local runtime and folder name, got %q", state.Runtime.DisplayName)
	}
	if state.Runtime.ProtocolVersion != "rnar/v1" {
		t.Fatalf("expected registered protocol version to win over loose local runtime, got %q", state.Runtime.ProtocolVersion)
	}
	if state.Runtime.OwnerUserID != "owner-registered" {
		t.Fatalf("expected registered owner to win over loose local owner, got %q", state.Runtime.OwnerUserID)
	}
	if state.Runtime.Role != "reviewer" {
		t.Fatalf("expected registered role to win over loose local role, got %q", state.Runtime.Role)
	}
	if len(state.Runtime.Capabilities) != 1 || state.Runtime.Capabilities[0] != "tool.call" {
		t.Fatalf("expected registered capabilities to win over loose local capabilities, got %+v", state.Runtime.Capabilities)
	}
}

func TestRegisterOnboardAgentRejectsPartialRegistrationTruthWithoutPersistingProfiles(t *testing.T) {
	workdir := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"agent_id":     "agent-1",
				"display_name": "Agent One",
				"token":        "token-1",
				"workspace_id": "ws-1",
			},
		})
	}))
	defer server.Close()

	cfg := RuntimeConfig{
		Mode:              RuntimeModeDaemon,
		Workdir:           workdir,
		RhizomeRPC:        server.URL,
		RhizomeHost:       server.URL,
		WorkspaceID:       "ws-1",
		WorkspacePassword: testWorkspacePassword,
		AgentID:           "agent-1",
		DisplayName:       "Agent One",
		OwnerUserID:       "owner-1",
	}

	_, err := registerOnboardAgent(cfg)
	if err == nil {
		t.Fatal("expected partial registration truth to fail closed")
	}
	if _, statErr := os.Stat(localRuntimeProfilePath(workdir)); !os.IsNotExist(statErr) {
		t.Fatalf("expected local runtime profile to remain absent, got stat err %v", statErr)
	}
	if _, statErr := os.Stat(rhizomeProfilePath()); !os.IsNotExist(statErr) {
		t.Fatalf("expected rhizome connection profile to remain absent, got stat err %v", statErr)
	}
	if _, ok := FindManagedAgent("agent-1"); ok {
		t.Fatal("expected managed agent registry to remain absent")
	}
}
