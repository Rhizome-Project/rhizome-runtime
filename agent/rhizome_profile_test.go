package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRhizomeProfileMissingReturnsEmptyProfile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if got := LoadRhizomeProfile(); got != (RhizomeConnectionProfile{}) {
		t.Fatalf("expected empty profile when missing file, got %+v", got)
	}
}

func TestSaveAndLoadRhizomeProfileRoundTripDefaults(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	want := RhizomeConnectionProfile{
		RPCEndpoint:   "https://rhizome.test/rpc",
		WorkspaceID:   "ws-123",
		WorkspaceName: "Workspace 123",
		AgentID:       "agent-123",
		AgentToken:    "token-123",
		OwnerUserID:   "owner-123",
	}

	if err := SaveRhizomeProfile(want); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	path := rhizomeProfilePath()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected profile file to exist at %s: %v", path, err)
	}
	if got := filepath.Dir(path); got != filepath.Join(home, configDir) {
		t.Fatalf("unexpected profile directory %q", got)
	}

	got := LoadRhizomeProfile()
	if got.RPCEndpoint != want.RPCEndpoint {
		t.Fatalf("expected rpc endpoint %q, got %q", want.RPCEndpoint, got.RPCEndpoint)
	}
	if got.HostURL != "https://rhizome.test" {
		t.Fatalf("expected host url to derive from rpc endpoint, got %q", got.HostURL)
	}
	if got.WorkspaceID != want.WorkspaceID || got.WorkspaceName != want.WorkspaceName {
		t.Fatalf("unexpected workspace fields: %+v", got)
	}
	if got.WorkspacePassword != "" {
		t.Fatalf("expected an unset workspace password to remain unset, got %q", got.WorkspacePassword)
	}
	if got.AgentID != want.AgentID || got.AgentToken != want.AgentToken {
		t.Fatalf("unexpected agent fields: %+v", got)
	}
	if got.OwnerUserID != want.OwnerUserID {
		t.Fatalf("unexpected owner user id: %+v", got)
	}
	if got.UpdatedAt == "" {
		t.Fatal("expected updated_at to be populated")
	}
}
