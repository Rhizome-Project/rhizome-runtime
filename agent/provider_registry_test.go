package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestUpsertProviderRecordRoundTrip(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:   "openai-main",
		Title:        "OpenAI Main",
		ChannelType:  providerChannelAPI,
		Driver:       "openai",
		GroupID:      "group-openai-main",
		DefaultModel: "gpt-5.4",
		Models:       []string{"gpt-5.4", " gpt-5.4-mini ", "gpt-5.4"},
		Enabled:      true,
		API: ProviderAPIConfig{
			BaseURL: " https://api.openai.com/v1 ",
			PublicHeaders: map[string]string{
				"X-Env": " prod ",
			},
		},
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	record, ok := FindProviderRecord("openai-main")
	if !ok {
		t.Fatal("expected provider to be found")
	}
	if record.ChannelType != providerChannelAPI || record.Driver != "openai" || record.GroupID != "group-openai-main" {
		t.Fatalf("unexpected provider core fields: %+v", record)
	}
	if !record.Enabled {
		t.Fatalf("expected new provider to default enabled, got %+v", record)
	}
	if len(record.Models) != 2 || record.Models[1] != "gpt-5.4-mini" {
		t.Fatalf("expected models to normalize and dedupe, got %+v", record.Models)
	}
	if record.API.BaseURL != "https://api.openai.com/v1" || record.API.PublicHeaders["X-Env"] != "prod" {
		t.Fatalf("expected API config to normalize, got %+v", record.API)
	}
}

func TestUpsertProviderRecordDefaultsProviderIDToGroupID(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := UpsertProviderRecord(ProviderRecord{
		ChannelType: providerChannelAPI,
		Driver:      llmBackendOpenAI,
		GroupID:     "group-openai-main",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	record, ok := FindProviderRecord("group-openai-main")
	if !ok {
		t.Fatal("expected provider to be found by synthesized provider id")
	}
	if record.ProviderID != "group-openai-main" {
		t.Fatalf("expected provider id to default to group id, got %+v", record)
	}
}

func TestValidateProviderReferenceRejectsDisabledProvider(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "openai-disabled",
			ChannelType: providerChannelAPI,
			Driver:      llmBackendOpenAI,
			GroupID:     "group-openai-disabled",
			Enabled:     false,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	err := validateProviderReference("openai-disabled")
	if !errors.Is(err, errProviderDisabled) {
		t.Fatalf("expected disabled provider error, got %v", err)
	}
}

func TestSaveProviderRegistryPreservesDisabledProviders(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "openai-disabled",
			ChannelType: providerChannelAPI,
			Driver:      llmBackendOpenAI,
			GroupID:     "group-openai-disabled",
			Enabled:     false,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	record, ok := FindProviderRecord("openai-disabled")
	if !ok {
		t.Fatal("expected disabled provider to survive round trip")
	}
	if record.Enabled {
		t.Fatalf("expected disabled flag to survive round trip, got %+v", record)
	}
}

func TestMarshalProviderRegistryForWriteKeepsFalseBooleans(t *testing.T) {
	raw, _, err := marshalProviderRegistryForWrite(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "codex-bridge",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			GroupID:     "codex-bridge",
			Enabled:     false,
			Bridge: ProviderBridgeConfig{
				UseManagedHome: false,
			},
			CreatedAt: "2026-04-12T00:00:00Z",
		}},
	})
	if err != nil {
		t.Fatalf("marshalProviderRegistryForWrite() error: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"enabled": false`)) {
		t.Fatalf("expected enabled=false to be serialized, got %s", raw)
	}
	if !bytes.Contains(raw, []byte(`"use_managed_home": false`)) {
		t.Fatalf("expected use_managed_home=false to be serialized, got %s", raw)
	}
}

func TestProviderRegistryRejectsInvalidChannelType(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "bad-provider",
			ChannelType: "desktop",
			Driver:      "openai",
			GroupID:     "group-bad",
		}},
	})
	if err == nil {
		t.Fatal("expected invalid channel type to be rejected")
	}
}

func TestProviderRegistryPreservesUnknownLegacyDriver(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "anthropic-main",
			ChannelType: providerChannelAPI,
			Driver:      "anthropic",
			GroupID:     "group-anthropic",
		}},
	})
	if err != nil {
		t.Fatalf("expected unknown legacy driver to round-trip, got %v", err)
	}
	record, ok := FindProviderRecord("anthropic-main")
	if !ok {
		t.Fatal("expected legacy provider to be loadable")
	}
	if record.Driver != "anthropic" || record.ChannelType != providerChannelAPI {
		t.Fatalf("unexpected legacy provider after round-trip: %+v", record)
	}
}

func TestRemoveProviderRecordDeletesEntry(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "codex-bridge",
		ChannelType: providerChannelBridge,
		Driver:      "codex",
		GroupID:     "group-codex",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}
	if err := RemoveProviderRecord("codex-bridge"); err != nil {
		t.Fatalf("RemoveProviderRecord() error: %v", err)
	}
	if _, ok := FindProviderRecord("codex-bridge"); ok {
		t.Fatal("expected provider to be removed")
	}
}

func TestRemoveProviderRecordRejectsReferencedProvider(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "codex-bridge",
		ChannelType: providerChannelBridge,
		Driver:      "codex",
		GroupID:     "group-codex",
		Enabled:     true,
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}
	if err := SetManagerDefault("default_provider_id", "codex-bridge"); err != nil {
		t.Fatalf("SetManagerDefault(default_provider_id) error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:    "agent-provider",
		Workdir:    workdir,
		ProviderID: "codex-bridge",
	}); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	err := RemoveProviderRecord("codex-bridge")
	if err == nil {
		t.Fatal("expected referenced provider removal to fail")
	}
	if !pathExists(providerRegistryPath()) {
		t.Fatalf("expected provider registry to remain after failed removal")
	}
}

func TestUpsertProviderRecordArchivesCorruptRegistryInsteadOfOverwritingIt(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	if err := os.MkdirAll(agentRuntimeConfigRoot(), 0o700); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	if err := os.WriteFile(providerRegistryPath(), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("WriteFile(providerRegistryPath) error: %v", err)
	}

	err := UpsertProviderRecord(ProviderRecord{
		ProviderID:  "openai-main",
		ChannelType: providerChannelAPI,
		Driver:      "openai",
		GroupID:     "group-openai-main",
	})
	if err == nil {
		t.Fatal("expected corrupt provider registry to fail closed")
	}
	if pathExists(providerRegistryPath()) {
		t.Fatalf("expected corrupt provider registry at %s to be archived", providerRegistryPath())
	}
	matches, globErr := filepath.Glob(providerRegistryPath() + ".broken-*")
	if globErr != nil {
		t.Fatalf("Glob() error: %v", globErr)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived provider registry, got %v", matches)
	}
}
