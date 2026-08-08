package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestRefreshManagedAgentCredentialPersistsFreshToken(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var gotAuth string
	var gotMethod string
	var gotParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if r.URL.Path == "/api/auth/agent/register" {
			gotMethod = "http.auth.agent.register"
			gotParams = req
			_ = json.NewEncoder(w).Encode(map[string]any{
				"agent_id":     "alpha",
				"display_name": "Alpha",
				"access_token": "fresh-token-1",
				"workspace_id": "ws-test",
				"agent": map[string]any{
					"agent_id":         "alpha",
					"workspace_id":     "ws-test",
					"owner_user_id":    "owner-1",
					"display_name":     "Alpha",
					"role":             "implementer",
					"status":           "REGISTERED",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"created_at":       "2026-06-02T00:00:00Z",
					"updated_at":       "2026-06-02T00:00:00Z",
				},
			})
			return
		}

		gotMethod, _ = req["method"].(string)
		gotParams, _ = req["params"].(map[string]any)

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"agent_id":     "alpha",
				"display_name": "Alpha",
				"token":        "fresh-token-1",
				"workspace_id": "ws-test",
				"agent": map[string]any{
					"agent_id":         "alpha",
					"workspace_id":     "ws-test",
					"owner_user_id":    "owner-1",
					"display_name":     "Alpha",
					"role":             "implementer",
					"status":           "REGISTERED",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"created_at":       "2026-06-02T00:00:00Z",
					"updated_at":       "2026-06-02T00:00:00Z",
				},
			},
		})
	}))
	defer server.Close()

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "alpha",
		DisplayName: "Alpha",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-test",
		OwnerUserID: "owner-1",
		Role:        "implementer",
	}
	if err := SaveBotRegistry(BotRegistry{
		Defaults: BotManagerDefaults{
			HostURL:           server.URL,
			WorkspaceID:       "ws-test",
			WorkspacePassword: "workspace-password",
			OwnerUserID:       "owner-1",
		},
		Agents: []ManagedAgentRecord{record},
	}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint:       server.URL,
		HostURL:           server.URL,
		WorkspaceID:       "ws-test",
		WorkspacePassword: "workspace-password",
		AgentID:           "alpha",
		DisplayName:       "Alpha",
		AgentToken:        "stale-token",
		OwnerUserID:       "owner-1",
		Role:              "implementer",
		Capabilities:      []string{"tool.call"},
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	result, err := refreshManagedAgentCredential(context.Background(), record)
	if err != nil {
		t.Fatalf("refreshManagedAgentCredential() error: %v", err)
	}

	if gotMethod != "http.auth.agent.register" {
		t.Fatalf("expected HTTP auth registration method, got %q", gotMethod)
	}
	if gotAuth != "" {
		t.Fatalf("expected credential refresh to omit stale bearer auth, got %q", gotAuth)
	}
	if gotParams["workspace_id"] != "ws-test" || gotParams["agent_id"] != "alpha" {
		t.Fatalf("unexpected auth registration params: %+v", gotParams)
	}
	if result.AgentID != "alpha" || result.WorkspaceID != "ws-test" || result.TokenPrefix != "fresh-to" {
		t.Fatalf("unexpected refresh result: %+v", result)
	}

	profile := LoadLocalRuntimeProfile(workdir)
	if profile.AgentToken != "fresh-token-1" {
		t.Fatalf("expected local runtime token to be refreshed, got %q", profile.AgentToken)
	}
	if profile.RegisteredExecutor.AgentID != "alpha" || profile.RegisteredExecutor.WorkspaceID != "ws-test" {
		t.Fatalf("expected registered executor identity to be refreshed, got %+v", profile.RegisteredExecutor)
	}
	if got := LoadBotRegistry().Agents; len(got) != 1 || got[0].AgentID != "alpha" || got[0].Workdir == "" {
		t.Fatalf("expected managed registry to retain alpha, got %+v", got)
	}
}

func TestRefreshManagedAgentCredentialUsesRosterPresetForRegistrationOnly(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var gotParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotParams = req
		_ = json.NewEncoder(w).Encode(map[string]any{
			"agent_id":     "alpha",
			"display_name": "Alpha",
			"access_token": "fresh-token-1",
			"workspace_id": "ws-test",
			"agent": map[string]any{
				"agent_id":         "alpha",
				"workspace_id":     "ws-test",
				"owner_user_id":    "owner-1",
				"display_name":     "Alpha",
				"role":             "strategist",
				"status":           "REGISTERED",
				"protocol_version": "rnar/v1",
				"capabilities":     []string{"tool.call"},
				"created_at":       "2026-06-02T00:00:00Z",
				"updated_at":       "2026-06-02T00:00:00Z",
			},
		})
	}))
	defer server.Close()

	longRuntimeRole := "rq product coordinator and strategist. Writes the product contract and opens semantic deliverable tasks."
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "alpha",
		DisplayName: "Alpha",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-test",
		OwnerUserID: "owner-1",
		Role:        longRuntimeRole,
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		RPCEndpoint:       server.URL,
		HostURL:           server.URL,
		WorkspaceID:       "ws-test",
		WorkspacePassword: "workspace-password",
		AgentID:           "alpha",
		DisplayName:       "Alpha",
		AgentToken:        "stale-token",
		OwnerUserID:       "owner-1",
		Role:              longRuntimeRole,
		Capabilities:      []string{"tool.call"},
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
	rosterPath := filepath.Join(t.TempDir(), "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{"alpha":{"preset_target":"strategist"}}`), 0o600); err != nil {
		t.Fatalf("write roster: %v", err)
	}
	registrationRoles, err := loadManagedCredentialRefreshRoleMap(rosterPath)
	if err != nil {
		t.Fatalf("loadManagedCredentialRefreshRoleMap() error: %v", err)
	}

	if _, err := refreshManagedAgentCredentialWithOptions(context.Background(), record, managedAgentCredentialRefreshOptions{RegistrationRoles: registrationRoles}); err != nil {
		t.Fatalf("refreshManagedAgentCredentialWithOptions() error: %v", err)
	}

	if gotParams["role"] != "strategist" {
		t.Fatalf("expected canonical registration role from roster preset, got %+v", gotParams)
	}
	profile := LoadLocalRuntimeProfile(workdir)
	if profile.Role != longRuntimeRole {
		t.Fatalf("expected local runtime role to be preserved for prompts, got %q", profile.Role)
	}
	if profile.RegisteredExecutor.Role != "strategist" {
		t.Fatalf("expected registered executor identity to record canonical role, got %+v", profile.RegisteredExecutor)
	}
}

func TestSafeTokenPrefixRedactsShortTokens(t *testing.T) {
	if got := safeTokenPrefix("short"); got != "<redacted>" {
		t.Fatalf("expected short token to be redacted, got %q", got)
	}
	if got := safeTokenPrefix("fresh-token-1"); got != "fresh-to" {
		t.Fatalf("expected long token prefix, got %q", got)
	}
}
