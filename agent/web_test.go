package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockingInspectLLM struct {
	started  chan struct{}
	release  chan struct{}
	response *LLMResponse
}

func (l *blockingInspectLLM) Chat(ctx context.Context, _ []Message, _ []ToolDef) (*LLMResponse, error) {
	if l.started != nil {
		select {
		case l.started <- struct{}{}:
		default:
		}
	}
	select {
	case <-l.release:
		if l.response != nil {
			return l.response, nil
		}
		return &LLMResponse{Content: "inspect ok"}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func requireProviderCatalogIncludes(t *testing.T, label string, catalog []SupportedProviderOption, id string) {
	t.Helper()
	for _, option := range catalog {
		if option.ID == id {
			return
		}
	}
	t.Fatalf("%s, got %+v", label, catalog)
}

func TestManagerWebDashboardRootContainsTitle(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "rhizome-bot web") {
		t.Fatalf("expected dashboard html to contain web title, got %q", body)
	}
	if !strings.Contains(body, "/api/overview") {
		t.Fatalf("expected dashboard html to reference overview api, got %q", body)
	}
}

func TestManagerWebOverviewAndDefaultsHandlersRedactSecrets(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	if err := SetManagerDefault("host_url", "https://rhizome.test"); err != nil {
		t.Fatalf("SetManagerDefault(host_url) error: %v", err)
	}
	if err := SetManagerDefault("workspace_id", "rhizome-main"); err != nil {
		t.Fatalf("SetManagerDefault(workspace_id) error: %v", err)
	}
	registry := LoadBotRegistry()
	registry.Defaults.WorkspacePassword = "secret-pw"
	if err := SaveBotRegistry(registry); err != nil {
		t.Fatalf("SaveBotRegistry(workspace password fixture) error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		WorkspaceID: "workspace-bootstrap",
		AgentID:     "lyrica-bootstrap",
		DisplayName: "Lyrica Bootstrap",
		Role:        "generalist",
		RegisteredExecutor: RegisteredExecutorIdentity{
			WorkspaceID: "rhizome-main",
			AgentID:     "lyrica",
			DisplayName: "Lyrica Registered",
			Role:        "reviewer",
		},
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	routes := newManagerWebServer().routes()

	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/overview", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("overview status = %d", rec.Code)
	}
	var overview managerWebOverview
	if err := json.Unmarshal(rec.Body.Bytes(), &overview); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if overview.Defaults.HostURL != "https://rhizome.test" {
		t.Fatalf("expected host default, got %+v", overview.Defaults)
	}
	if len(overview.Agents) != 1 {
		t.Fatalf("expected one overview agent row, got %+v", overview.Agents)
	}
	if overview.Agents[0].Record.DisplayName != "Lyrica" || overview.Agents[0].Record.Role != "generalist" {
		t.Fatalf("expected overview record to keep registry truth separate, got %+v", overview.Agents[0].Record)
	}
	if overview.Agents[0].EffectiveIdentity.Source != "registered_executor" {
		t.Fatalf("expected overview effective identity source to be registered_executor, got %+v", overview.Agents[0].EffectiveIdentity)
	}
	if overview.Agents[0].EffectiveIdentity.DisplayName != "Lyrica Registered" || overview.Agents[0].EffectiveIdentity.Role != "reviewer" {
		t.Fatalf("expected overview to surface confirmed executor truth, got %+v", overview.Agents[0].EffectiveIdentity)
	}

	rec = httptest.NewRecorder()
	routes.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/defaults", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("defaults get status = %d body=%s", rec.Code, rec.Body.String())
	}
	var defaultsResp struct {
		HostURL         string                    `json:"host_url"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &defaultsResp); err != nil {
		t.Fatalf("decode defaults get response: %v", err)
	}
	if defaultsResp.HostURL != "https://rhizome.test" {
		t.Fatalf("expected defaults get to preserve backward-compatible top-level defaults, got %+v", defaultsResp)
	}
	if defaultsResp.Defaults.HostURL != "https://rhizome.test" {
		t.Fatalf("expected defaults get to include nested defaults context, got %+v", defaultsResp.Defaults)
	}
	if len(defaultsResp.Agents) != 1 || defaultsResp.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected defaults get response to preserve current agents, got %+v", defaultsResp.Agents)
	}
	if len(defaultsResp.Providers) != 0 {
		t.Fatalf("expected defaults get response to preserve current providers, got %+v", defaultsResp.Providers)
	}
	requireProviderCatalogIncludes(t, "expected defaults get response to include provider catalog", defaultsResp.ProviderCatalog, "codex_bridge")
	if defaultsResp.CreateDefault.FolderName == "" || defaultsResp.CreateDefault.Workdir == "" {
		t.Fatalf("expected defaults get response to include create-default context, got %+v", defaultsResp.CreateDefault)
	}

	updateBody := strings.NewReader(`{"field":"model","value":"gpt-5.5"}`)
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/defaults", updateBody)
	req.Header.Set("Content-Type", "application/json")
	routes.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("defaults update status = %d body=%s", rec.Code, rec.Body.String())
	}
	var updateResp struct {
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &updateResp); err != nil {
		t.Fatalf("decode defaults update response: %v", err)
	}

	if updateResp.Defaults.Model != "gpt-5.5" {
		t.Fatalf("expected updated model, got %+v", updateResp.Defaults)
	}
	if len(updateResp.Agents) != 1 || updateResp.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected defaults update response to preserve current agents, got %+v", updateResp.Agents)
	}
	if len(updateResp.Providers) != 0 {
		t.Fatalf("expected defaults update response to preserve current providers, got %+v", updateResp.Providers)
	}
	requireProviderCatalogIncludes(t, "expected defaults update response to include provider catalog", updateResp.ProviderCatalog, "codex_bridge")
	if updateResp.CreateDefault.ParentDir == "" || updateResp.CreateDefault.FolderName == "" {
		t.Fatalf("expected defaults update response to include create-default context, got %+v", updateResp.CreateDefault)
	}
}

func TestManagerWebOverviewIncludesSubstrateReadiness(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	executable := filepath.Join(workdir, "rhizome-bot-test.exe")
	if err := os.WriteFile(executable, []byte("manager binary"), 0o755); err != nil {
		t.Fatalf("WriteFile(executable) error: %v", err)
	}
	providerExecutable := writeProviderSmokeScript(t, workdir, "codex-test")

	origExecutable := managerSubstrateExecutableFunc
	origInstalled := managerSubstrateInstalledExecutableFunc
	origRepoRoot := managerSubstrateRepoRootFunc
	origCommand := managerSubstrateCommandOutputFunc
	origBuild := managerSubstrateRuntimeBuildFunc
	defer func() {
		managerSubstrateExecutableFunc = origExecutable
		managerSubstrateInstalledExecutableFunc = origInstalled
		managerSubstrateRepoRootFunc = origRepoRoot
		managerSubstrateCommandOutputFunc = origCommand
		managerSubstrateRuntimeBuildFunc = origBuild
	}()
	managerSubstrateExecutableFunc = func() (string, error) { return executable, nil }
	managerSubstrateInstalledExecutableFunc = func() string { return executable }
	managerSubstrateRepoRootFunc = func() string { return workdir }
	managerSubstrateCommandOutputFunc = func(_ string, _ string, args ...string) (string, error) {
		joined := strings.Join(args, " ")
		switch {
		case strings.Contains(joined, "rev-parse HEAD"):
			return "abc123\n", nil
		case strings.Contains(joined, "rev-parse --abbrev-ref HEAD"):
			return "main\n", nil
		case strings.Contains(joined, "status --porcelain"):
			return "", nil
		default:
			return "", nil
		}
	}
	managerSubstrateRuntimeBuildFunc = func() managerRuntimeBuildIdentity {
		return managerRuntimeBuildIdentity{BuildInfoOK: true, VCSRevision: "abc123"}
	}

	if err := SaveProviderRegistry(ProviderRegistry{Providers: []ProviderRecord{{
		ProviderID:  "codex-bridge",
		ChannelType: providerChannelBridge,
		Driver:      llmBackendCodex,
		GroupID:     "codex-bridge",
		Enabled:     true,
		Bridge: ProviderBridgeConfig{
			Executable: providerExecutable,
		},
	}}}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "lyrica",
		Workdir:     workdir,
		ProviderID:  "codex-bridge",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
		DisplayName: "Lyrica",
		Role:        "generalist",
	}); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	rec := httptest.NewRecorder()
	newManagerWebServer().routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/overview", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("overview status = %d body=%s", rec.Code, rec.Body.String())
	}
	var overview managerWebOverview
	if err := json.Unmarshal(rec.Body.Bytes(), &overview); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if overview.Substrate.CurrentExecutable.SHA256 == "" || overview.Substrate.InstalledExecutable.SHA256 == "" {
		t.Fatalf("expected overview substrate executable hashes, got %+v", overview.Substrate)
	}
	if overview.Substrate.Repository.Head != "abc123" || overview.Substrate.Repository.Dirty {
		t.Fatalf("expected clean repo identity in substrate, got %+v", overview.Substrate.Repository)
	}
	if len(overview.Substrate.Providers) != 1 || overview.Substrate.Providers[0].Status != "ready" {
		t.Fatalf("expected ready provider route in substrate, got %+v", overview.Substrate.Providers)
	}
	if len(overview.Substrate.ToolBundles) != 1 || overview.Substrate.ToolBundles[0].Status == "" {
		t.Fatalf("expected tool-bundle readiness row in substrate, got %+v", overview.Substrate.ToolBundles)
	}
}

func TestManagerWebOverviewCleansStaleProcessState(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: 4321, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	defer func() { managedAgentProcessExistsFunc = origExists }()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return false, nil
	}

	rec := httptest.NewRecorder()
	newManagerWebServer().routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/overview", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("overview status = %d body=%s", rec.Code, rec.Body.String())
	}
	var overview managerWebOverview
	if err := json.Unmarshal(rec.Body.Bytes(), &overview); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if len(overview.Agents) != 1 {
		t.Fatalf("expected one overview agent row, got %+v", overview.Agents)
	}
	if overview.Agents[0].Process.State != "stopped" || overview.Agents[0].Process.Stale || overview.Agents[0].Process.Running {
		t.Fatalf("expected stale process state to be cleaned in overview, got %+v", overview.Agents[0].Process)
	}
	if state := LoadAgentProcessState(workdir); state.PID != 0 {
		t.Fatalf("expected stale process state file removed, got %+v", state)
	}
}

func TestManagerWebOverviewUsesBatchedProcessSnapshot(t *testing.T) {
	setManagerWebTestHome(t)

	records := []ManagedAgentRecord{
		{AgentID: "alpha", DisplayName: "Alpha", Workdir: filepath.Join(t.TempDir(), "alpha"), HostURL: "https://rhizome.test", WorkspaceID: "ws-1"},
		{AgentID: "beta", DisplayName: "Beta", Workdir: filepath.Join(t.TempDir(), "beta"), HostURL: "https://rhizome.test", WorkspaceID: "ws-1"},
		{AgentID: "gamma", DisplayName: "Gamma", Workdir: filepath.Join(t.TempDir(), "gamma"), HostURL: "https://rhizome.test", WorkspaceID: "ws-1"},
	}
	snapshot := map[int]managedAgentProcessProbe{}
	for idx, record := range records {
		if err := os.MkdirAll(record.Workdir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := UpsertManagedAgent(record); err != nil {
			t.Fatalf("UpsertManagedAgent(%s) error: %v", record.AgentID, err)
		}
		pid := 4200 + idx
		executable := filepath.Join(t.TempDir(), "rhizome-bot.exe")
		if err := os.WriteFile(executable, []byte("test executable"), 0o755); err != nil {
			t.Fatalf("WriteFile(executable) error: %v", err)
		}
		executableHash, err := managerFileSHA256(executable)
		if err != nil {
			t.Fatalf("managerFileSHA256(%s) error: %v", record.AgentID, err)
		}
		args := managedAgentDaemonArgs(record)
		if err := SaveAgentProcessState(record.Workdir, AgentProcessState{
			PID:                 pid,
			Executable:          executable,
			ExecutableSHA256:    executableHash,
			Workdir:             record.Workdir,
			Args:                args,
			ArgsDigest:          managedAgentArgsDigest(args),
			RuntimeConfigDigest: managedAgentRuntimeConfigDigest(managedAgentStartRuntimeConfig(record)),
		}); err != nil {
			t.Fatalf("SaveAgentProcessState(%s) error: %v", record.AgentID, err)
		}
		snapshot[pid] = managedAgentProcessProbe{
			PID:            pid,
			Exists:         true,
			ExecutablePath: executable,
			CommandLine:    executable + " " + strings.Join(args, " "),
		}
	}

	origSnapshot := managedAgentProcessSnapshotFunc
	origExists := managedAgentProcessExistsFunc
	defer func() {
		managedAgentProcessSnapshotFunc = origSnapshot
		managedAgentProcessExistsFunc = origExists
	}()
	snapshotCalls := 0
	existsCalls := 0
	managedAgentProcessSnapshotFunc = func(got []ManagedAgentRecord) map[int]managedAgentProcessProbe {
		snapshotCalls++
		if len(got) != len(records) {
			t.Fatalf("snapshot should receive all records at once, got %d want %d", len(got), len(records))
		}
		return snapshot
	}
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		existsCalls++
		return false, errors.New("per-agent processExists should not be called when overview has a snapshot")
	}

	rec := httptest.NewRecorder()
	newManagerWebServer().routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/overview", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("overview status = %d body=%s", rec.Code, rec.Body.String())
	}
	var overview managerWebOverview
	if err := json.Unmarshal(rec.Body.Bytes(), &overview); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if snapshotCalls != 1 {
		t.Fatalf("expected one batched snapshot call, got %d", snapshotCalls)
	}
	if existsCalls != 0 {
		t.Fatalf("expected no per-agent process exists calls, got %d", existsCalls)
	}
	if len(overview.Agents) != len(records) {
		t.Fatalf("expected %d agent rows, got %+v", len(records), overview.Agents)
	}
	for _, row := range overview.Agents {
		if !row.Process.Running || row.Process.State != "running" {
			t.Fatalf("expected running process from snapshot, got %+v", row)
		}
	}
}

func TestManagerWebDefaultsRejectDisabledProvider(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	existingWorkdir := filepath.Join(root, "existing-agent")
	if err := os.MkdirAll(existingWorkdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(existingWorkdir) error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "existing",
		DisplayName: "Existing Agent",
		Workdir:     existingWorkdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("UpsertManagedAgent(existing) error: %v", err)
	}
	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "codex-disabled",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			GroupID:     "group-codex-disabled",
			Enabled:     false,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	routes := newManagerWebServer().routes()
	body := strings.NewReader(`{"field":"default_provider_id","value":"codex-disabled"}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/defaults", body)
	req.Header.Set("Content-Type", "application/json")
	routes.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected disabled provider default reject, got %d body=%s", rec.Code, rec.Body.String())
	}
	var response managerWebOverviewErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode defaults reject response: %v", err)
	}
	if !strings.Contains(response.Error, "disabled") {
		t.Fatalf("expected disabled provider error, got %+v", response)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected defaults reject response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "existing" {
		t.Fatalf("expected defaults reject response to preserve current registry, got %+v", response.Agents)
	}
	if len(response.Providers) != 1 || response.Providers[0].ProviderID != "codex-disabled" {
		t.Fatalf("expected defaults reject response to preserve providers, got %+v", response.Providers)
	}
	if len(response.ProviderCatalog) == 0 {
		t.Fatalf("expected defaults reject response to preserve provider catalog, got %+v", response.ProviderCatalog)
	}
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected defaults reject response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
	if got := LoadBotRegistry().Defaults.DefaultProviderID; got != "" {
		t.Fatalf("expected defaults to remain unchanged, got default_provider_id=%q", got)
	}
}

func TestManagerWebDefaultsMalformedBodyReturnsOverviewContext(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	existingWorkdir := filepath.Join(root, "existing-agent")
	if err := os.MkdirAll(existingWorkdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(existingWorkdir) error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "existing",
		DisplayName: "Existing Agent",
		Workdir:     existingWorkdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("UpsertManagedAgent(existing) error: %v", err)
	}
	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "codex-bridge",
			Title:       "Codex Bridge",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			GroupID:     "group-codex",
			Enabled:     true,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	routes := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/defaults", strings.NewReader(`{"field":`))
	req.Header.Set("Content-Type", "application/json")
	routes.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected malformed defaults body to fail, got %d body=%s", rec.Code, rec.Body.String())
	}
	var response managerWebOverviewErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode malformed defaults response: %v", err)
	}
	if !strings.Contains(strings.ToLower(response.Error), "invalid") && !strings.Contains(strings.ToLower(response.Error), "decode") {
		t.Fatalf("expected malformed defaults decode error, got %+v", response)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected malformed defaults response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "existing" {
		t.Fatalf("expected malformed defaults response to preserve current registry, got %+v", response.Agents)
	}
	if len(response.Providers) != 1 || response.Providers[0].ProviderID != "codex-bridge" {
		t.Fatalf("expected malformed defaults response to preserve providers, got %+v", response.Providers)
	}
	if len(response.ProviderCatalog) == 0 {
		t.Fatalf("expected malformed defaults response to preserve provider catalog, got %+v", response.ProviderCatalog)
	}
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected malformed defaults response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
}

func TestManagerWebProvidersHandlersRoundTrip(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	existingWorkdir := filepath.Join(root, "existing-agent")
	if err := os.MkdirAll(existingWorkdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(existingWorkdir) error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "existing",
		DisplayName: "Existing Agent",
		Workdir:     existingWorkdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("UpsertManagedAgent(existing) error: %v", err)
	}

	server := newManagerWebServer().routes()

	body := strings.NewReader(`{
		"provider_id":"codex-bridge",
		"title":"Codex Bridge",
		"channel_type":"bridge",
		"driver":"codex",
		"group_id":"codex-bridge",
		"default_model":"gpt-5.4",
		"models":["gpt-5.4","gpt-5.4-mini"],
		"bridge":{"executable":"codex","command":"codex --mode bridge","use_managed_home":true}
	}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/providers", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("providers create status = %d body=%s", rec.Code, rec.Body.String())
	}
	var createResp struct {
		OK              bool                      `json:"ok"`
		Message         string                    `json:"message"`
		Provider        ProviderRecord            `json:"provider"`
		Providers       []ProviderRecord          `json:"providers"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("decode providers create response: %v", err)
	}
	if !createResp.OK || createResp.Provider.ProviderID != "codex-bridge" {
		t.Fatalf("expected create response to preserve provider payload, got %+v", createResp)
	}
	if createResp.Defaults.DefaultParentDir != root {
		t.Fatalf("expected create response to include current defaults, got %+v", createResp.Defaults)
	}
	if len(createResp.Agents) != 1 || createResp.Agents[0].Record.AgentID != "existing" {
		t.Fatalf("expected create response to include current agents, got %+v", createResp.Agents)
	}
	requireProviderCatalogIncludes(t, "expected create response to include provider catalog", createResp.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(createResp.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected create response to preserve create default parent dir, got %+v", createResp.CreateDefault)
	}

	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/overview", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("overview status = %d body=%s", rec.Code, rec.Body.String())
	}
	var overview managerWebOverview
	if err := json.Unmarshal(rec.Body.Bytes(), &overview); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if len(overview.Providers) != 1 {
		t.Fatalf("expected overview to include one provider, got %+v", overview.Providers)
	}
	if overview.Providers[0].ProviderID != "codex-bridge" || overview.Providers[0].Bridge.Command != "codex --mode bridge" {
		t.Fatalf("unexpected provider payload in overview: %+v", overview.Providers[0])
	}
	requireProviderCatalogIncludes(t, "expected overview to include supported provider catalog", overview.ProviderCatalog, "codex_bridge")

	body = strings.NewReader(`{
		"provider_id":"codex-bridge",
		"title":"Codex Bridge",
		"channel_type":"bridge",
		"driver":"codex",
		"group_id":"codex-bridge",
		"default_model":"gpt-5.4",
		"models":["gpt-5.4","gpt-5.4-mini"],
		"enabled":false,
		"bridge":{"executable":"codex","command":"codex --mode bridge","use_managed_home":false}
	}`)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/providers", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("providers update status = %d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/providers/codex-bridge", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("provider detail status = %d body=%s", rec.Code, rec.Body.String())
	}
	var providerResp struct {
		Provider        ProviderRecord            `json:"provider"`
		Providers       []ProviderRecord          `json:"providers"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &providerResp); err != nil {
		t.Fatalf("decode provider detail: %v", err)
	}
	if providerResp.Provider.Enabled {
		t.Fatalf("expected provider disable toggle to persist, got %+v", providerResp.Provider)
	}
	if providerResp.Provider.Bridge.UseManagedHome {
		t.Fatalf("expected bridge managed-home toggle to persist false, got %+v", providerResp.Provider)
	}
	if len(providerResp.Providers) != 1 || providerResp.Providers[0].ProviderID != "codex-bridge" {
		t.Fatalf("expected provider detail to preserve overview providers, got %+v", providerResp.Providers)
	}
	if providerResp.Defaults.DefaultParentDir != root {
		t.Fatalf("expected provider detail to preserve current defaults, got %+v", providerResp.Defaults)
	}
	if len(providerResp.Agents) != 1 || providerResp.Agents[0].Record.AgentID != "existing" {
		t.Fatalf("expected provider detail to preserve current agents, got %+v", providerResp.Agents)
	}
	requireProviderCatalogIncludes(t, "expected provider detail to preserve provider catalog", providerResp.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(providerResp.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected provider detail to preserve create default parent dir, got %+v", providerResp.CreateDefault)
	}

	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/providers/codex-bridge", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("providers delete status = %d body=%s", rec.Code, rec.Body.String())
	}
	var deleteResp struct {
		OK              bool                      `json:"ok"`
		Message         string                    `json:"message"`
		Providers       []ProviderRecord          `json:"providers"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &deleteResp); err != nil {
		t.Fatalf("decode providers delete response: %v", err)
	}
	if !deleteResp.OK || !strings.Contains(deleteResp.Message, "removed provider codex-bridge") {
		t.Fatalf("expected delete response to preserve success payload, got %+v", deleteResp)
	}
	if len(deleteResp.Providers) != 0 {
		t.Fatalf("expected delete response to show empty provider list, got %+v", deleteResp.Providers)
	}
	if deleteResp.Defaults.DefaultParentDir != root {
		t.Fatalf("expected delete response to include current defaults, got %+v", deleteResp.Defaults)
	}
	if len(deleteResp.Agents) != 1 || deleteResp.Agents[0].Record.AgentID != "existing" {
		t.Fatalf("expected delete response to include current agents, got %+v", deleteResp.Agents)
	}
	requireProviderCatalogIncludes(t, "expected delete response to include provider catalog", deleteResp.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(deleteResp.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected delete response to preserve create default parent dir, got %+v", deleteResp.CreateDefault)
	}

	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/providers", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("providers list status = %d body=%s", rec.Code, rec.Body.String())
	}
	var listResp struct {
		Providers       []ProviderRecord          `json:"providers"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("decode providers list: %v", err)
	}
	if len(listResp.Providers) != 0 {
		t.Fatalf("expected provider to be removed, got %+v", listResp.Providers)
	}
	if listResp.Defaults.DefaultParentDir != root {
		t.Fatalf("expected providers list to preserve current defaults, got %+v", listResp.Defaults)
	}
	if len(listResp.Agents) != 1 || listResp.Agents[0].Record.AgentID != "existing" {
		t.Fatalf("expected providers list to preserve current agents, got %+v", listResp.Agents)
	}
	requireProviderCatalogIncludes(t, "expected providers list to preserve provider catalog", listResp.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(listResp.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected providers list to preserve create default parent dir, got %+v", listResp.CreateDefault)
	}
}

func TestManagerWebProvidersRejectUnsupportedCatalogImplementationForNewProvider(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	existingWorkdir := filepath.Join(root, "existing-agent")
	if err := os.MkdirAll(existingWorkdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(existingWorkdir) error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "existing",
		DisplayName: "Existing Agent",
		Workdir:     existingWorkdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("UpsertManagedAgent(existing) error: %v", err)
	}

	server := newManagerWebServer().routes()
	body := strings.NewReader(`{
		"provider_id":"anthropic-main",
		"title":"Anthropic Main",
		"channel_type":"api",
		"driver":"anthropic",
		"group_id":"anthropic-main"
	}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/providers", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected unsupported catalog implementation reject, got %d body=%s", rec.Code, rec.Body.String())
	}
	var response managerWebOverviewErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode providers reject response: %v", err)
	}
	if !strings.Contains(strings.ToLower(response.Error), "unsupported") {
		t.Fatalf("expected unsupported provider error, got %+v", response)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected providers reject response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "existing" {
		t.Fatalf("expected providers reject response to preserve current registry, got %+v", response.Agents)
	}
	if len(response.ProviderCatalog) == 0 {
		t.Fatalf("expected providers reject response to preserve provider catalog, got %+v", response.ProviderCatalog)
	}
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected providers reject response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
	if _, ok := FindProviderRecord("anthropic-main"); ok {
		t.Fatal("did not expect unsupported provider to be created")
	}
}

func TestManagerWebProvidersMalformedBodyReturnsOverviewContext(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	existingWorkdir := filepath.Join(root, "existing-agent")
	if err := os.MkdirAll(existingWorkdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(existingWorkdir) error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "existing",
		DisplayName: "Existing Agent",
		Workdir:     existingWorkdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("UpsertManagedAgent(existing) error: %v", err)
	}
	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "codex-bridge",
			Title:       "Codex Bridge",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			GroupID:     "group-codex",
			Enabled:     true,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	server := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/providers", strings.NewReader(`{"provider_id":`))
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected malformed providers body to fail, got %d body=%s", rec.Code, rec.Body.String())
	}
	var response managerWebOverviewErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode malformed providers response: %v", err)
	}
	if !strings.Contains(strings.ToLower(response.Error), "invalid") && !strings.Contains(strings.ToLower(response.Error), "decode") {
		t.Fatalf("expected malformed providers decode error, got %+v", response)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected malformed providers response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "existing" {
		t.Fatalf("expected malformed providers response to preserve current registry, got %+v", response.Agents)
	}
	if len(response.Providers) != 1 || response.Providers[0].ProviderID != "codex-bridge" {
		t.Fatalf("expected malformed providers response to preserve current providers, got %+v", response.Providers)
	}
	if len(response.ProviderCatalog) == 0 {
		t.Fatalf("expected malformed providers response to preserve provider catalog, got %+v", response.ProviderCatalog)
	}
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected malformed providers response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
}

func TestManagerWebProvidersAllowsLegacyImplementationUpdate(t *testing.T) {
	setManagerWebTestHome(t)

	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "anthropic-main",
			Title:       "Anthropic Main",
			ChannelType: providerChannelAPI,
			Driver:      "anthropic",
			GroupID:     "anthropic-main",
			Enabled:     true,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}

	server := newManagerWebServer().routes()
	body := strings.NewReader(`{
		"provider_id":"anthropic-main",
		"title":"Anthropic Legacy",
		"channel_type":"api",
		"driver":"anthropic",
		"group_id":"anthropic-main",
		"enabled":false
	}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/providers", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected legacy implementation update to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}

	record, ok := FindProviderRecord("anthropic-main")
	if !ok {
		t.Fatal("expected legacy provider to remain after update")
	}
	if record.Title != "Anthropic Legacy" || record.Enabled {
		t.Fatalf("expected legacy provider update to persist without implementation rewrite, got %+v", record)
	}
}

func TestManagerWebProviderDeleteFailureReturnsOverviewContext(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	existingWorkdir := filepath.Join(root, "existing-agent")
	if err := os.MkdirAll(existingWorkdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(existingWorkdir) error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "existing",
		DisplayName: "Existing Agent",
		Workdir:     existingWorkdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		ProviderID:  "codex-bridge",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("UpsertManagedAgent(existing) error: %v", err)
	}
	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "codex-bridge",
			Title:       "Codex Bridge",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			GroupID:     "group-codex",
			Enabled:     true,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}
	if err := SetManagerDefault("default_provider_id", "codex-bridge"); err != nil {
		t.Fatalf("SetManagerDefault(default_provider_id) error: %v", err)
	}

	server := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/providers/codex-bridge", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected referenced provider delete to fail, got %d body=%s", rec.Code, rec.Body.String())
	}
	var response managerWebOverviewErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode provider delete failure response: %v", err)
	}
	if !strings.Contains(response.Error, "still referenced") {
		t.Fatalf("expected referenced provider error, got %+v", response)
	}
	if response.Defaults.DefaultParentDir != root || response.Defaults.DefaultProviderID != "codex-bridge" {
		t.Fatalf("expected provider delete failure response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "existing" {
		t.Fatalf("expected provider delete failure response to preserve current registry, got %+v", response.Agents)
	}
	if len(response.Providers) != 1 || response.Providers[0].ProviderID != "codex-bridge" {
		t.Fatalf("expected provider delete failure response to preserve current providers, got %+v", response.Providers)
	}
	if len(response.ProviderCatalog) == 0 {
		t.Fatalf("expected provider delete failure response to preserve provider catalog, got %+v", response.ProviderCatalog)
	}
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected provider delete failure response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
}

func TestManagerWebDashboardScriptRenderersIncludeProviderAwareFields(t *testing.T) {
	script := managerWebDashboardScriptRenderers()
	for _, token := range []string{
		"default-default_provider_id",
		"onboard-provider_id",
		"edit-runtime-provider",
		"Default Provider",
		"Provider ID",
		"provider: <strong>",
		"renderProvidersTab",
		"showProviderModal()",
		"provider-card-name",
		"provider-implementation",
		"providerCatalogRows()",
		`var applied=await applyOverviewPayload(result,true,false);`,
		`if(!applied){await refreshOverview(true)}`,
		`catch(err){await hydrateDashboardError(err,false);setMessage(err.message,true)}`,
		`var appliedOverview=await applyOverviewPayload(res,true,true);`,
		`var appliedDetail=applyDetailPayload(res);`,
		`if(!appliedOverview){await refreshOverview(true)}else if(shouldRefreshDetail()&&!appliedDetail){await refreshDetail(true)}`,
		`catch(err){await hydrateDashboardError(err,true);setMessage(err.message,true)}`,
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("expected dashboard script to contain %q", token)
		}
	}
	for _, legacy := range []string{
		`setMessage(result&&result.message?result.message:"saved provider");
closeProviderModal(true);
await refreshOverview(true);`,
		`setMessage(result&&result.message?result.message:"removed provider");
await refreshOverview(true);`,
		`setMessage(res.message);refreshDetail(true);`,
	} {
		if strings.Contains(script, legacy) {
			t.Fatalf("expected dashboard script renderers to stop using legacy refresh-only flow %q", legacy)
		}
	}
}

func TestManagerWebDashboardScriptCoreHydratesSelfContainedOverviewContext(t *testing.T) {
	script := managerWebDashboardScriptCore()
	for _, token := range []string{
		"function extractOverviewFromPayload(payload)",
		"function shouldRefreshDetail(){return !!(state.selectedAgentId&&(state.agentPageOpen||state.activeTab!==\"overview\"))}",
		`var err=new Error(data&&data.error?data.error:"http "+resp.status);err.payload=data;err.status=resp.status;throw err`,
		"function applyDetailPayload(payload)",
		"async function hydrateDashboardError(err,refreshDetailAfter){",
		"async function handleDashboardReadError(err,refreshDetailAfter){",
		"async function refreshOverview(keepMessage,skipDetailRefresh){",
		"function renderLocalChatSessionMessages(session){",
		"function applyLocalChatPayloadFallback(payload,renderMessages){",
		"async function applyLocalChatPayload(payload){",
		`}).catch(function(hydrationErr){console.error("localChat error hydrate error",hydrationErr);return applyLocalChatPayloadFallback(err.payload,renderMessages)})`,
		"async function applyOverviewPayload(payload,keepMessage,skipDetailRefresh)",
		`var detailPayload=await api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"?lines=80");`,
		`await applyOverviewPayload(detailPayload,true,true);`,
		`var activityPayload=await api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/activity");`,
		`await applyOverviewPayload(activityPayload,true,true);`,
		`window.setInboxChannel=function(ch){state.inbox.channel=ch;renderInboxPanel()};`,
		`if(subTabName==="controls"){fetchLocalChats();if(state.agentPageOpen&&state.detail){return}}`,
		`if(state.agentPageOpen&&state.detail&&(subTabName==="info"||subTabName==="settings"||subTabName==="runtime"||subTabName==="logs")){return}`,
		`if(state.agentPageOpen&&subTabName==="activity"&&state.activity&&state.activity.agent_id===state.selectedAgentId){renderActivityPanel();return}`,
		`if(state.agentPageOpen&&subTabName==="inbox"&&state.inbox&&state.inbox.agent_id===state.selectedAgentId){renderInboxPanel();return}`,
		`api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/activity").then(async function(activityPayload){`,
		`state.activity=activityPayload;`,
		`api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/messages").then(async function(fetched){`,
		`state.inbox=fetched;`,
		`state.overviewTimer=setInterval(function(){refreshOverview(true,shouldRefreshDetail()).catch(function(err){handleDashboardReadError(err,false)})},5000);`,
		`state.detailTimer=setInterval(function(){if(state.selectedAgentId&&(state.agentPageOpen||state.activeTab!=="overview")){refreshDetail(true).catch(function(err){handleDashboardReadError(err,true)})}},3000)`,
		`try{await refreshOverview(false,false);setMessage("dashboard ready")}catch(err){await handleDashboardReadError(err,false)}`,
		`switchAgentSubTab("info");
  renderOverview();`,
		"var applied=lastResult?await applyOverviewPayload(lastResult,true,true):false;",
		"var appliedOverview=await applyOverviewPayload(result,true,true);var appliedDetail=applyDetailPayload(result);",
		"else{var appliedOverview=await applyOverviewPayload(result,true,true);var appliedDetail=applyDetailPayload(result);if(!appliedOverview){await refreshOverview(true)}else if(refreshDetailAfter&&shouldRefreshDetail()&&!appliedDetail){await refreshDetail(true)}}",
		`}catch(err){await hydrateDashboardError(err,refreshDetailAfter);handleError(err)}finally{state.busy=false}}`,
		`postJSON("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/control",{method:method,payload:payload},message||"control request sent",true,async function(result){window.__lastControlResponse=result;var appliedOverview=await applyOverviewPayload(result,true,true);var appliedDetail=applyDetailPayload(result);if(!appliedOverview){if(shouldRefreshDetail()){await refreshDetail(true)}}else if(!appliedDetail&&shouldRefreshDetail()){await refreshDetail(true)}})`,
		`postJSON("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/edit",{remove:true},"agent removed",false,async function(result){state.selectedAgentId="";state.detail=null;closeAgentPage();var applied=await applyOverviewPayload(result,true,true);if(!applied){await refreshOverview(true)}})`,
		`}catch(err){await hydrateDashboardError(err,false);handleError(err)}finally{state.busy=false}});`,
		`if(!state.selectedAgentId)return;api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats').then(function(res){applyLocalChatPayload(res).catch(function(err){console.error("fetchChats error",err);if(!applyLocalChatPayloadFallback(res,false)){setMessage("Inspect chats refresh partially failed: "+err.message,true)}})}).catch(function(err){hydrateLocalChatError(err,false).finally(function(){setMessage("Inspect chats refresh failed: "+err.message,true)})});`,
		`state.activeLocalChatId=session.chat_id}applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)})`,
		`inp.value='';inp.disabled=false;btn.disabled=false;state.localChatSending=false;inp.focus();applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)})`,
		`if(state.activeLocalChatId===id){ state.activeLocalChatId=null; state.activeLocalChatSession=null; }`,
		`applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)}).catch(function(err){console.error("localChat delete hydrate error",err);if(!applyLocalChatPayloadFallback(res,true)){var container=document.getElementById('local-chat-messages');if(container)container.innerHTML='<div class="empty">Select or create an inspect chat</div>';setMessage("Inspect chat delete partially failed: "+err.message,true)}});`,
		`applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)}).catch(function(err){console.error("localChat archive hydrate error",err);if(!applyLocalChatPayloadFallback(res,true)){setMessage("Inspect chat archive partially failed: "+err.message,true)}})`,
		`applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)}).catch(function(err){if(!applyLocalChatPayloadFallback(res,true)){container.innerHTML='<div class="empty" style="color:#ff6a6a">Error: '+esc(err.message)+'</div>';setMessage("Inspect chat load partially failed: "+err.message,true)}});`,
		`if(!state.selectedAgentId)return;api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats',{method:'POST'}).then(function(res){var session=res&&res.session?res.session:res;if(session&&session.chat_id){state.activeLocalChatId=session.chat_id}applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)}).catch(function(err){console.error("localChat create hydrate error",err);if(!applyLocalChatPayloadFallback(res,true)){setMessage("Inspect chat create partially failed: "+err.message,true)}})}).catch(function(err){hydrateLocalChatError(err,false).finally(function(){setMessage(err.message,true)})});`,
		`applyLocalChatPayload(res).catch(function(err){console.error("localChat create-before-send hydrate error",err);if(!applyLocalChatPayloadFallback(res,false)){setMessage("Inspect chat bootstrap partially failed: "+err.message,true)}});`,
		`inp.value='';inp.disabled=false;btn.disabled=false;state.localChatSending=false;inp.focus();applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)}).catch(function(err){console.error("localChat send hydrate error",err);if(!applyLocalChatPayloadFallback(res,true)){setMessage("Inspect chat send partially failed: "+err.message,true)}});`,
		`}).catch(function(err){hydrateLocalChatError(err,true).finally(function(){setMessage("Delete failed: "+err.message,true)})});`,
		`}).catch(function(err){hydrateLocalChatError(err,true).finally(function(){setMessage("Archive failed: "+err.message,true)})});`,
		`}).catch(function(err){hydrateLocalChatError(err,false).finally(function(){setMessage(err.message,true)})});`,
		`}).catch(function(err){hydrateLocalChatError(err,false).finally(function(){setMessage(err.message,true);state.localChatSending=false;inp.disabled=false;btn.disabled=false;});});return;`,
		`}).catch(function(err){typing.remove();uidiv.remove();state.localChatSending=false;hydrateLocalChatError(err,true).then(function(applied){if(!applied&&state.activeLocalChatId){fetchLocalChats();loadLocalChat(state.activeLocalChatId)}}).finally(function(){setMessage(err.message,true);if(inp){inp.focus();}});});`,
		`Promise.resolve(applyOverviewPayload(res,true,true)).catch(function(err){console.error("fs overview hydrate error",err)});`,
		`}).catch(function(err){hydrateDashboardError(err,false).catch(function(hydrationErr){console.error("fs error hydrate error",hydrationErr)}).finally(function(){list.innerHTML='<div class="empty" style="color:#ff6a6a">Error: '+esc(err.message)+'</div>'})})`,
		"if(!applied){await refreshOverview(true)}",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("expected dashboard core script to contain %q", token)
		}
	}
	if strings.Contains(script, `closeOnboardModal();await refreshOverview(true);`) {
		t.Fatalf("expected onboard submit path to stop forcing direct overview refetch, got %q", script)
	}
	if strings.Contains(script, `markDirty("defaults",false);await refreshOverview(true);`) {
		t.Fatalf("expected defaults submit path to stop forcing direct overview refetch, got %q", script)
	}
	if strings.Contains(script, `else{await refreshOverview(true);if(refreshDetailAfter&&state.selectedAgentId){await refreshDetail(true)}}`) {
		t.Fatalf("expected generic dashboard postJSON path to stop forcing direct overview refetch, got %q", script)
	}
	if strings.Contains(script, `}catch(err){handleError(err)}finally{state.busy=false}}`) {
		t.Fatalf("expected dashboard mutation paths to hydrate self-contained error payloads before surfacing bare errors, got %q", script)
	}
	if strings.Contains(script, `window.__lastControlResponse=result;if(state.selectedAgentId){await refreshDetail(true)}})`) {
		t.Fatalf("expected control custom refresh to stop skipping self-contained overview hydration, got %q", script)
	}
	if strings.Contains(script, `var applied=lastResult?await applyOverviewPayload(lastResult,true):false;`) {
		t.Fatalf("expected defaults submit path to use skipDetailRefresh-aware overview hydration, got %q", script)
	}
	if strings.Contains(script, `var applied=await applyOverviewPayload(result,true);`) {
		t.Fatalf("expected self-contained dashboard paths to stop using detail-refreshing overview hydration directly, got %q", script)
	}
	if strings.Contains(script, `async function(){state.selectedAgentId="";state.detail=null;closeAgentPage();await refreshOverview(true)})`) {
		t.Fatalf("expected remove-agent custom refresh to stop forcing direct overview refetch, got %q", script)
	}
	if strings.Contains(script, `}catch(err){handleError(err)}finally{state.busy=false}});`) {
		t.Fatalf("expected defaults/onboard form handlers to hydrate self-contained error payloads before surfacing bare errors, got %q", script)
	}
	if strings.Contains(script, `state.activeLocalChatId=session.chat_id;fetchLocalChats();loadLocalChat(session.chat_id);`) {
		t.Fatalf("expected local chat create happy path to stop forcing list+detail refetch when response already carries self-contained state, got %q", script)
	}
	if strings.Contains(script, `inp.value='';inp.disabled=false;btn.disabled=false;state.localChatSending=false;inp.focus();fetchLocalChats();loadLocalChat(state.activeLocalChatId);`) {
		t.Fatalf("expected local chat send happy path to stop forcing list+detail refetch when response already carries self-contained state, got %q", script)
	}
	if strings.Contains(script, `if(state.activeLocalChatId===id){ state.activeLocalChatId=null; state.activeLocalChatSession=null; var container=document.getElementById('local-chat-messages'); if(container)container.innerHTML='<div class="empty">Select or create an inspect chat</div>'; }fetchLocalChats();`) {
		t.Fatalf("expected local chat delete happy path to stop forcing list refetch when response already carries self-contained state, got %q", script)
	}
	if strings.Contains(script, `renderLocalChatContractBanner();renderLocalChatsList();if(state.activeLocalChatId===id){loadLocalChat(id)}else{fetchLocalChats()}`) {
		t.Fatalf("expected local chat archive happy path to stop forcing list/detail refetch when response already carries self-contained state, got %q", script)
	}
	if strings.Contains(script, `}).catch(function(err){setMessage("Delete failed: "+err.message,true)});`) {
		t.Fatalf("expected local chat delete error path to preserve self-contained error payload before degrading to a toast-only failure, got %q", script)
	}
	if strings.Contains(script, `}).catch(function(err){setMessage("Archive failed: "+err.message,true)});`) {
		t.Fatalf("expected local chat archive error path to preserve self-contained error payload before degrading to a toast-only failure, got %q", script)
	}
	if strings.Contains(script, `if(!state.selectedAgentId)return;api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats',{method:'POST'}).then(function(res){var session=res&&res.session?res.session:res;if(session&&session.chat_id){state.activeLocalChatId=session.chat_id}applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)}).catch(function(err){console.error("localChat create hydrate error",err);fetchLocalChats();loadLocalChat(state.activeLocalChatId)})}).catch(function(err){setMessage(err.message,true)});`) {
		t.Fatalf("expected local chat create error path to preserve self-contained error payload before degrading to a toast-only failure, got %q", script)
	}
	if strings.Contains(script, `}).catch(function(err){typing.remove();uidiv.remove();setMessage(err.message,true);state.localChatSending=false;renderLocalChatContractBanner();if(state.activeLocalChatId){fetchLocalChats();loadLocalChat(state.activeLocalChatId)}if(inp){inp.focus();}});`) {
		t.Fatalf("expected local chat send error path to prefer self-contained error payload hydration before falling back to fetch-based recovery, got %q", script)
	}
	if strings.Contains(script, `if(!state.selectedAgentId)return;api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats').then(function(res){applyLocalChatPayload(res).catch(function(err){console.error("fetchChats error",err)})}).catch(function(err){console.error("fetchChats error",err)});`) {
		t.Fatalf("expected local chat list read path to stop degrading to a console-only failure when self-contained error payload is available, got %q", script)
	}
	if strings.Contains(script, `}).catch(function(hydrationErr){console.error("localChat error hydrate error",hydrationErr);return false})`) {
		t.Fatalf("expected local chat error hydration to fall back to the original payload before reporting failure, got %q", script)
	}
	if strings.Contains(script, `console.error("localChat delete hydrate error",err);fetchLocalChats();`) {
		t.Fatalf("expected local chat delete success fallback to stop forcing a list refetch when the original payload can still recover local chat state, got %q", script)
	}
	if strings.Contains(script, `console.error("localChat archive hydrate error",err);if(state.activeLocalChatId===id){loadLocalChat(id)}else{fetchLocalChats()}`) {
		t.Fatalf("expected local chat archive success fallback to stop forcing list/detail refetch when the original payload can still recover local chat state, got %q", script)
	}
	if strings.Contains(script, `console.error("localChat create hydrate error",err);fetchLocalChats();loadLocalChat(state.activeLocalChatId)`) {
		t.Fatalf("expected local chat create success fallback to stop forcing list/detail refetch when the original payload can still recover local chat state, got %q", script)
	}
	if strings.Contains(script, `console.error("localChat create-before-send hydrate error",err);fetchLocalChats()`) {
		t.Fatalf("expected local chat create-before-send fallback to stop forcing a list refetch when the original payload can still recover local chat state, got %q", script)
	}
	if strings.Contains(script, `console.error("localChat send hydrate error",err);fetchLocalChats();loadLocalChat(state.activeLocalChatId)`) {
		t.Fatalf("expected local chat send success fallback to stop forcing list/detail refetch when the original payload can still recover local chat state, got %q", script)
	}
	if strings.Contains(script, `}).catch(function(err){list.innerHTML='<div class="empty" style="color:#ff6a6a">Error: '+esc(err.message)+'</div>'})`) {
		t.Fatalf("expected fs list error path to preserve self-contained dashboard error payload before degrading to a bare error panel, got %q", script)
	}
	if strings.Contains(script, `state.detail=await api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"?lines=80");`) {
		t.Fatalf("expected detail read path to hydrate self-contained overview payload before replacing selected-agent detail state, got %q", script)
	}
	if strings.Contains(script, `try{state.activity=await api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/activity")}catch(e){console.warn(e)}`) {
		t.Fatalf("expected activity read path to hydrate self-contained overview/detail payload before updating activity state, got %q", script)
	}
	if strings.Contains(script, `var fetched=await api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/messages");
      state.inbox=fetched;`) {
		t.Fatalf("expected messages read path to hydrate self-contained overview/detail payload before updating inbox state, got %q", script)
	}
	if strings.Contains(script, `window.setInboxChannel=function(ch){state.inbox.channel=ch;refreshDetail(true)};`) {
		t.Fatalf("expected inbox channel switching to stay local to the loaded inbox payload instead of forcing another detail fetch, got %q", script)
	}
	if strings.Contains(script, `fetchLocalChats();
  switchAgentSubTab("info");
  renderOverview();`) {
		t.Fatalf("expected openAgentPage to stop eagerly fetching local chats before the controls tab is opened, got %q", script)
	}
	if strings.Contains(script, `if(subTabName==="controls"){fetchLocalChats()}
  if(state.agentPageOpen){refreshDetail(true).catch(handleError)}`) {
		t.Fatalf("expected controls-tab switching to stop forcing a second immediate detail fetch when selected-agent detail is already loaded, got %q", script)
	}
	if strings.Contains(script, `var tabs=document.querySelectorAll(".sub-tab");
  for(var i=0;i<tabs.length;i++){tabs[i].classList.toggle("active",tabs[i].getAttribute("data-subtab")===subTabName)}
  if(state.agentPageOpen){refreshDetail(true).catch(handleError)}`) {
		t.Fatalf("expected static agent subtabs to stop forcing a detail refetch when selected-agent detail is already loaded, got %q", script)
	}
	if strings.Contains(script, `if(state.agentPageOpen&&state.detail&&(subTabName==="info"||subTabName==="settings"||subTabName==="runtime"||subTabName==="logs")){return}
  if(state.agentPageOpen){refreshDetail(true).catch(handleError)}`) {
		t.Fatalf("expected cached activity/inbox subtabs to stop forcing another detail fetch when their payload for the selected agent is already loaded, got %q", script)
	}
	if strings.Contains(script, `if(state.agentPageOpen&&subTabName==="activity"&&state.activity&&state.activity.agent_id===state.selectedAgentId){renderActivityPanel();return}
  if(state.agentPageOpen&&subTabName==="inbox"&&state.inbox&&state.inbox.agent_id===state.selectedAgentId){renderInboxPanel();return}
  if(state.agentPageOpen){refreshDetail(true).catch(handleError)}`) {
		t.Fatalf("expected uncached activity/inbox subtabs to fetch only their missing payload instead of falling through to a full detail refresh, got %q", script)
	}
	if strings.Contains(script, `fetchLocalChats();
  switchAgentSubTab("info");
  renderOverview();
  refreshDetail(true).catch(handleError);`) {
		t.Fatalf("expected openAgentPage to stop forcing a second immediate detail fetch after switchAgentSubTab already triggers one, got %q", script)
	}
	if strings.Contains(script, `api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/activity").then(async function(activityPayload){
      await applyOverviewPayload(activityPayload,true,true);
      applyDetailPayload(activityPayload);
      state.activity=activityPayload;
      renderActivityPanel();
    }).catch(handleError);`) {
		t.Fatalf("expected activity subtab reads to hydrate self-contained errors before surfacing them, got %q", script)
	}
	if strings.Contains(script, `api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/messages").then(async function(fetched){
      await applyOverviewPayload(fetched,true,true);
      applyDetailPayload(fetched);
      state.inbox=fetched;
      state.inbox.channel=currentCh;
      renderInboxPanel();
    }).catch(handleError);`) {
		t.Fatalf("expected inbox subtab reads to hydrate self-contained errors before surfacing them, got %q", script)
	}
	if strings.Contains(script, `if(state.agentPageOpen){refreshDetail(true).catch(handleError)}`) {
		t.Fatalf("expected generic selected-agent reads to hydrate self-contained errors before surfacing them, got %q", script)
	}
	if strings.Contains(script, `function startPolling(){if(state.overviewTimer){clearInterval(state.overviewTimer)}if(state.detailTimer){clearInterval(state.detailTimer)}state.overviewTimer=setInterval(function(){refreshOverview(true).catch(handleError)},5000);state.detailTimer=setInterval(function(){if(state.selectedAgentId&&(state.agentPageOpen||state.activeTab!=="overview")){refreshDetail(true).catch(handleError)}},3000)}`) {
		t.Fatalf("expected dashboard polling to stop using redundant bare refresh catches and to skip duplicate detail refresh from overview polling, got %q", script)
	}
	if strings.Contains(script, `try{await refreshOverview(false);setMessage("dashboard ready")}catch(err){handleError(err)}`) {
		t.Fatalf("expected initial dashboard boot to hydrate self-contained read errors before surfacing them, got %q", script)
	}
}

func TestManagerWebDashboardHTMLIncludesProvidersTabAndModal(t *testing.T) {
	html := managerWebDashboardHTML()
	for _, token := range []string{
		"data-tab=\"providers\"",
		"id=\"panel-providers\"",
		"id=\"provider-modal\"",
		"id=\"provider-modal-title\"",
	} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected dashboard html to contain %q", token)
		}
	}
}

func TestManagerWebAgentSettingsPersistsProviderBinding(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:   "codex-bridge",
		Title:        "Codex Bridge",
		ChannelType:  providerChannelBridge,
		Driver:       "codex",
		GroupID:      "group-codex",
		DefaultModel: "gpt-5.4",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendAuto,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		AgentID:     "lyrica",
		WorkspaceID: "rhizome-main",
		LLMBackend:  llmBackendAuto,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	server := newManagerWebServer().routes()
	body := strings.NewReader(`{"provider_id":"codex-bridge","model":"gpt-5.4-mini","planner_sec":"12","watchdog_sec":"34"}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/settings", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("settings status = %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Record       ManagedAgentRecord  `json:"record"`
		LocalRuntime LocalRuntimeProfile `json:"local_runtime"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode settings response: %v", err)
	}
	if response.Record.ProviderID != "codex-bridge" || response.Record.GroupID != "group-codex" {
		t.Fatalf("expected record provider binding to persist, got %+v", response.Record)
	}
	if response.Record.LLMBackend != llmBackendCodex || response.Record.Model != "gpt-5.4-mini" {
		t.Fatalf("expected record legacy runtime fields to derive from provider binding, got %+v", response.Record)
	}
	if response.LocalRuntime.ProviderID != "codex-bridge" || response.LocalRuntime.GroupID != "group-codex" {
		t.Fatalf("expected local runtime provider binding to persist, got %+v", response.LocalRuntime)
	}
}

func TestManagerWebAgentSettingsRejectsDisabledProvider(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	SetManagerDefault("default_parent_dir", root)
	workdir := t.TempDir()
	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "codex-disabled",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			GroupID:     "group-codex-disabled",
			Enabled:     false,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
		Role:        "generalist",
		LLMBackend:  llmBackendAuto,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		AgentID:     "lyrica",
		WorkspaceID: "ws-1",
		AgentToken:  "local-token",
		LLMBackend:  llmBackendAuto,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	routes := newManagerWebServer().routes()
	body := strings.NewReader(`{"provider_id":"codex-disabled","model":"gpt-5.4-mini"}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/settings", body)
	req.Header.Set("Content-Type", "application/json")
	routes.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected disabled provider to be rejected, got %d body=%s", rec.Code, rec.Body.String())
	}
	var response struct {
		Error           string                    `json:"error"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode disabled-provider response: %v", err)
	}
	if !strings.Contains(response.Error, "disabled") {
		t.Fatalf("expected disabled provider error, got %+v", response)
	}
	if response.Process.State != "stopped" || response.Process.Running {
		t.Fatalf("expected stopped process in disabled-provider response, got %+v", response.Process)
	}
	if response.Live.ProcessState != "stopped" || response.Live.Error != "" {
		t.Fatalf("expected stopped live status in disabled-provider response, got %+v", response.Live)
	}
	if len(response.Catalog.Tasks) != 1 || response.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed task catalog in disabled-provider response, got %+v", response.Catalog.Tasks)
	}
	if len(response.Catalog.Tensions) != 1 || response.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed tension catalog in disabled-provider response, got %+v", response.Catalog.Tensions)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected disabled-provider response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected disabled-provider response to preserve current agent overview, got %+v", response.Agents)
	}
	if len(response.Providers) != 1 || response.Providers[0].ProviderID != "codex-disabled" {
		t.Fatalf("expected disabled-provider response to preserve providers, got %+v", response.Providers)
	}
	if len(response.ProviderCatalog) == 0 || response.ProviderCatalog[0].ID == "" {
		t.Fatalf("expected disabled-provider response to include provider catalog, got %+v", response.ProviderCatalog)
	}
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected disabled-provider response to preserve create default parent dir, got %+v", response.CreateDefault)
	}

	runtimeProfile := LoadLocalRuntimeProfile(workdir)
	if runtimeProfile.ProviderID != "" {
		t.Fatalf("expected runtime profile to remain unchanged after disabled provider reject, got %+v", runtimeProfile)
	}
}

func TestManagerWebAgentEditMalformedBodyReturnsCurrentStatusAndCatalog(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	SetManagerDefault("default_parent_dir", root)
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
		Role:        "generalist",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/edit", strings.NewReader(`{"display_name":`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected malformed edit body to fail, got %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Error           string                    `json:"error"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode malformed-edit response: %v", err)
	}
	if !strings.Contains(strings.ToLower(response.Error), "decode json body") {
		t.Fatalf("expected decode error in malformed-edit response, got %+v", response)
	}
	if response.Process.State != "stopped" || response.Process.Running {
		t.Fatalf("expected stopped process in malformed-edit response, got %+v", response.Process)
	}
	if response.Live.ProcessState != "stopped" || response.Live.Error != "" {
		t.Fatalf("expected stopped live status in malformed-edit response, got %+v", response.Live)
	}
	if len(response.Catalog.Tasks) != 1 || response.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed task catalog in malformed-edit response, got %+v", response.Catalog.Tasks)
	}
	if len(response.Catalog.Tensions) != 1 || response.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed tension catalog in malformed-edit response, got %+v", response.Catalog.Tensions)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected malformed-edit response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected malformed-edit response to preserve current agent overview, got %+v", response.Agents)
	}
	if len(response.Providers) != 0 {
		t.Fatalf("expected malformed-edit response to preserve current providers, got %+v", response.Providers)
	}
	if len(response.ProviderCatalog) == 0 || response.ProviderCatalog[0].ID == "" {
		t.Fatalf("expected malformed-edit response to include provider catalog, got %+v", response.ProviderCatalog)
	}
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected malformed-edit response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
}

func TestManagerWebAgentProcessUnsupportedActionReturnsCurrentStatusAndCatalog(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
		Role:        "generalist",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/process", strings.NewReader(`{"action":"noop"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected unsupported process action to fail, got %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Error   string                    `json:"error"`
		Live    managerLiveRuntimeStatus  `json:"live"`
		Catalog managerWorkspaceCatalog   `json:"catalog"`
		Process ManagedAgentProcessStatus `json:"process"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode unsupported-process response: %v", err)
	}
	if !strings.Contains(response.Error, "unsupported process action") {
		t.Fatalf("expected unsupported process action error, got %+v", response)
	}
	if response.Process.State != "stopped" || response.Process.Running {
		t.Fatalf("expected stopped process in unsupported-process response, got %+v", response.Process)
	}
	if response.Live.ProcessState != "stopped" || response.Live.Error != "" {
		t.Fatalf("expected stopped live status in unsupported-process response, got %+v", response.Live)
	}
	if len(response.Catalog.Tasks) != 1 || response.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed task catalog in unsupported-process response, got %+v", response.Catalog.Tasks)
	}
	if len(response.Catalog.Tensions) != 1 || response.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed tension catalog in unsupported-process response, got %+v", response.Catalog.Tensions)
	}
}

func TestManagerWebAgentProcessStartPreflightFailureReturnsConflict(t *testing.T) {
	setManagerWebTestHome(t)

	current := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot-current", "current binary")
	installed := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot-installed", "installed binary")
	installCleanManagerSubstrateStubsForTest(t, current, installed)
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		WorkspaceID: "ws-1",
		Role:        "generalist",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	origStart := managedAgentStartProcessFunc
	defer func() { managedAgentStartProcessFunc = origStart }()
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		t.Fatalf("managed start must not launch when substrate preflight is blocked")
		return 0, nil
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/process", strings.NewReader(`{"action":"start"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected preflight conflict status 409, got %d body=%s", rec.Code, rec.Body.String())
	}
	var response struct {
		Error   string                    `json:"error"`
		Process ManagedAgentProcessStatus `json:"process"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode start-preflight response: %v", err)
	}
	if !strings.Contains(response.Error, "managed run substrate admission blocked") ||
		!strings.Contains(response.Error, "installed_executable_hash_mismatch") {
		t.Fatalf("expected substrate blocker in response, got %+v", response)
	}
	if response.Process.State != "stopped" || response.Process.Running {
		t.Fatalf("blocked start must leave process stopped, got %+v", response.Process)
	}
	if _, err := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(err) {
		t.Fatalf("blocked start must not persist process state, stat err=%v", err)
	}
	if _, err := os.Stat(managedRunSubstrateAdmissionReceiptPath(workdir)); err != nil {
		t.Fatalf("blocked start must persist substrate receipt, stat err=%v", err)
	}
}

func TestManagerWebBulkProcessStartsAgentsInOneRequest(t *testing.T) {
	setManagerWebTestHome(t)

	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	installCleanManagerSubstrateStubsForTest(t, bin, bin)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY", "1")
	root := t.TempDir()
	records := []ManagedAgentRecord{
		{AgentID: "alpha", DisplayName: "Alpha", Workdir: filepath.Join(root, "alpha"), WorkspaceID: "ws-1", Role: "generalist"},
		{AgentID: "beta", DisplayName: "Beta", Workdir: filepath.Join(root, "beta"), WorkspaceID: "ws-1", Role: "generalist"},
	}
	for _, record := range records {
		if err := os.MkdirAll(record.Workdir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := UpsertManagedAgent(record); err != nil {
			t.Fatalf("UpsertManagedAgent(%s) error: %v", record.AgentID, err)
		}
	}

	origStart := managedAgentStartProcessFunc
	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		managedAgentStartProcessFunc = origStart
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentSaveStateFunc = origSave
	}()

	nextPID := 5000
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		nextPID++
		return nextPID, nil
	}
	managedAgentProcessExistsFunc = func(pid int) (bool, error) { return pid >= 5001 && pid <= 5002, nil }
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	managedAgentSaveStateFunc = SaveAgentProcessState

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/bulk_process", strings.NewReader(`{"action":"start","agent_ids":["alpha","beta"],"parallelism":1}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("bulk start status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK      bool                               `json:"ok"`
		Action  string                             `json:"action"`
		Total   int                                `json:"total"`
		OKCount int                                `json:"ok_count"`
		Results []managerWebBulkProcessAgentResult `json:"results"`
		Agents  []managerWebAgentRow               `json:"agents"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode bulk start response: %v", err)
	}
	if !resp.OK || resp.Action != "start" || resp.Total != 2 || resp.OKCount != 2 || len(resp.Results) != 2 {
		t.Fatalf("unexpected bulk start response: %+v", resp)
	}
	for _, result := range resp.Results {
		if !result.OK || result.Process.State != "running" || !result.Process.Running {
			t.Fatalf("expected running bulk start result, got %+v", result)
		}
	}
	if len(resp.Agents) != 2 {
		t.Fatalf("expected refreshed overview agents, got %+v", resp.Agents)
	}
}

func TestManagerWebBulkProcessSkipOverviewReturnsBoundedProcessProof(t *testing.T) {
	setManagerWebTestHome(t)

	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	installCleanManagerSubstrateStubsForTest(t, bin, bin)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY", "1")
	root := t.TempDir()
	records := []ManagedAgentRecord{
		{AgentID: "alpha", DisplayName: "Alpha", Workdir: filepath.Join(root, "alpha"), WorkspaceID: "ws-1", Role: "generalist"},
		{AgentID: "beta", DisplayName: "Beta", Workdir: filepath.Join(root, "beta"), WorkspaceID: "ws-1", Role: "generalist"},
	}
	for _, record := range records {
		if err := os.MkdirAll(record.Workdir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := UpsertManagedAgent(record); err != nil {
			t.Fatalf("UpsertManagedAgent(%s) error: %v", record.AgentID, err)
		}
	}

	origStart := managedAgentStartProcessFunc
	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		managedAgentStartProcessFunc = origStart
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentSaveStateFunc = origSave
	}()

	nextPID := 7000
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		nextPID++
		return nextPID, nil
	}
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid >= 7001 && pid <= 7002, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	managedAgentSaveStateFunc = SaveAgentProcessState

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/bulk_process", strings.NewReader(`{"action":"start","agent_ids":["alpha","beta"],"parallelism":2,"skip_overview":true}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("bulk start status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK      bool                               `json:"ok"`
		OKCount int                                `json:"ok_count"`
		Results []managerWebBulkProcessAgentResult `json:"results"`
		Agents  []managerWebAgentRow               `json:"agents"`
		Command string                             `json:"command"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode bounded bulk start response: %v", err)
	}
	if !resp.OK || resp.OKCount != 2 || len(resp.Results) != 2 {
		t.Fatalf("unexpected bounded bulk start response: %+v", resp)
	}
	for _, result := range resp.Results {
		if !result.OK || result.Process.State != "running" || !result.Process.Running || result.Process.PID <= 0 {
			t.Fatalf("expected running process proof, got %+v", result)
		}
		if result.Process.ExecutableSHA256 == "" || result.Process.ArgsDigest == "" || result.Process.RuntimeConfigDigest == "" {
			t.Fatalf("expected bounded start proof to carry process provenance, got %+v", result.Process)
		}
	}
	if len(resp.Agents) != 0 || resp.Command != "" {
		t.Fatalf("skip_overview response must not include full overview payload, got agents=%d command=%q", len(resp.Agents), resp.Command)
	}
}

func TestManagerWebRosterStartIsAtomicWhenOneAdmissionBlocked(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	records := []ManagedAgentRecord{
		{AgentID: "alpha", DisplayName: "Alpha", Workdir: filepath.Join(root, "alpha"), WorkspaceID: "ws-1", Role: "generalist"},
		{AgentID: "beta", DisplayName: "Beta", Workdir: filepath.Join(root, "beta"), WorkspaceID: "ws-1", Role: "generalist"},
		{AgentID: "gamma", DisplayName: "Gamma", Workdir: filepath.Join(root, "gamma"), WorkspaceID: "ws-1", Role: "generalist"},
	}
	for _, record := range records {
		if err := os.MkdirAll(record.Workdir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := UpsertManagedAgent(record); err != nil {
			t.Fatalf("UpsertManagedAgent(%s) error: %v", record.AgentID, err)
		}
	}

	origAdmit := admitManagedRunStartFunc
	origStart := managedAgentStartProcessFunc
	defer func() {
		admitManagedRunStartFunc = origAdmit
		managedAgentStartProcessFunc = origStart
	}()

	// beta fails admission; the atomic invariant requires that NO agent process
	// is spawned when any roster member is blocked.
	admitManagedRunStartFunc = func(record ManagedAgentRecord) (managedRunPreflightResult, error) {
		if record.AgentID == "beta" {
			return managedRunPreflightResult{}, &managedRunPreflightBlockedError{Reasons: []string{"provider:beta:blocked"}}
		}
		return managedRunPreflightResult{ChildExecutablePath: "/stub/rhizome-bot"}, nil
	}

	var startMu sync.Mutex
	var started []string
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		startMu.Lock()
		started = append(started, workdir)
		startMu.Unlock()
		return 9000, nil
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/bulk_process", strings.NewReader(`{"action":"start","agent_ids":["alpha","beta","gamma"]}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("bulk start status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK      bool                               `json:"ok"`
		OKCount int                                `json:"ok_count"`
		Results []managerWebBulkProcessAgentResult `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode bulk start response: %v", err)
	}

	startMu.Lock()
	startedCount := len(started)
	startMu.Unlock()
	if startedCount != 0 {
		t.Fatalf("atomic roster start must spawn no process when one member is blocked, but started %d", startedCount)
	}
	if resp.OK || resp.OKCount != 0 {
		t.Fatalf("expected blocked roster to report no successes, got %+v", resp)
	}
	byAgent := map[string]managerWebBulkProcessAgentResult{}
	for _, result := range resp.Results {
		byAgent[result.AgentID] = result
	}
	if !strings.Contains(byAgent["beta"].Error, "provider:beta:blocked") {
		t.Fatalf("expected beta to carry its admission error, got %+v", byAgent["beta"])
	}
	for _, id := range []string{"alpha", "gamma"} {
		if byAgent[id].OK || !strings.Contains(byAgent[id].Error, "roster admission aborted") {
			t.Fatalf("expected %s to be aborted by atomic roster start, got %+v", id, byAgent[id])
		}
	}
}

func TestManagerWebBulkProcessStartPassesResumeContinuationWaivers(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	records := []ManagedAgentRecord{
		{AgentID: "alpha", DisplayName: "Alpha", Workdir: filepath.Join(root, "alpha"), WorkspaceID: "ws-1", Role: "generalist"},
		{AgentID: "beta", DisplayName: "Beta", Workdir: filepath.Join(root, "beta"), WorkspaceID: "ws-1", Role: "generalist"},
	}
	for _, record := range records {
		if err := os.MkdirAll(record.Workdir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := UpsertManagedAgent(record); err != nil {
			t.Fatalf("UpsertManagedAgent(%s) error: %v", record.AgentID, err)
		}
	}

	origAdmitWithOptions := admitManagedRunStartWithOptionsFunc
	origStart := managedAgentStartProcessFunc
	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origSave := managedAgentSaveStateFunc
	defer func() {
		admitManagedRunStartWithOptionsFunc = origAdmitWithOptions
		managedAgentStartProcessFunc = origStart
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentSaveStateFunc = origSave
	}()

	var admitMu sync.Mutex
	admitted := map[string]managedRunPreflightOptions{}
	childExecutable := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	admitManagedRunStartWithOptionsFunc = func(record ManagedAgentRecord, options managedRunPreflightOptions) (managedRunPreflightResult, error) {
		waiver := options.resumeContinuationWaiver()
		if !waiver.AllowDirtyProjectCheckout || !waiver.AllowLivePatchQueue || !waiver.AllowAgentRequests || !waiver.AllowLiveProjectBranches || !waiver.AllowPendingResumeTriggers {
			return managedRunPreflightResult{}, &managedRunPreflightBlockedError{Reasons: []string{"clean_stop:blocked"}}
		}
		admitMu.Lock()
		admitted[record.AgentID] = options
		admitMu.Unlock()
		return managedRunPreflightResult{ChildExecutablePath: childExecutable}, nil
	}
	nextPID := 8000
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		nextPID++
		return nextPID, nil
	}
	managedAgentProcessExistsFunc = func(pid int) (bool, error) { return pid >= 8001 && pid <= 8002, nil }
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	managedAgentSaveStateFunc = SaveAgentProcessState

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/bulk_process", strings.NewReader(`{"action":"start","agent_ids":["alpha","beta"],"resume_continuation_waiver":{"allow_dirty_project_checkout":true,"allow_live_patch_queue":true,"allow_agent_requests":true,"allow_live_project_branches":true,"allow_pending_resume_triggers":true}}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("bulk start status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK      bool                               `json:"ok"`
		OKCount int                                `json:"ok_count"`
		Results []managerWebBulkProcessAgentResult `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode bulk start response: %v", err)
	}
	if !resp.OK || resp.OKCount != 2 {
		t.Fatalf("expected waived roster start to admit both agents, got %+v", resp)
	}
	admitMu.Lock()
	defer admitMu.Unlock()
	for _, id := range []string{"alpha", "beta"} {
		options, ok := admitted[id]
		waiver := options.resumeContinuationWaiver()
		if !ok || !waiver.AllowDirtyProjectCheckout || !waiver.AllowLivePatchQueue || !waiver.AllowAgentRequests || !waiver.AllowLiveProjectBranches || !waiver.AllowPendingResumeTriggers {
			t.Fatalf("expected %s admission to receive resume continuation waivers, got admitted=%+v", id, admitted)
		}
	}
}

func TestManagerWebSingleProcessPassesResumeContinuationWaivers(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	record := ManagedAgentRecord{AgentID: "alpha", DisplayName: "Alpha", Workdir: filepath.Join(root, "alpha"), WorkspaceID: "ws-1", Role: "generalist"}
	if err := os.MkdirAll(record.Workdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent(%s) error: %v", record.AgentID, err)
	}

	origAdmit := admitManagedRunStartFunc
	origAdmitWithOptions := admitManagedRunStartWithOptionsFunc
	origStart := managedAgentStartProcessFunc
	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origSave := managedAgentSaveStateFunc
	origTimeout := managedAgentStopExitTimeout
	defer func() {
		admitManagedRunStartFunc = origAdmit
		admitManagedRunStartWithOptionsFunc = origAdmitWithOptions
		managedAgentStartProcessFunc = origStart
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentSaveStateFunc = origSave
		managedAgentStopExitTimeout = origTimeout
	}()

	admitManagedRunStartFunc = func(record ManagedAgentRecord) (managedRunPreflightResult, error) {
		return managedRunPreflightResult{}, &managedRunPreflightBlockedError{Reasons: []string{"missing_resume_waiver"}}
	}
	var admitted []managedRunPreflightOptions
	childExecutable := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	admitManagedRunStartWithOptionsFunc = func(record ManagedAgentRecord, options managedRunPreflightOptions) (managedRunPreflightResult, error) {
		waiver := options.resumeContinuationWaiver()
		if !waiver.AllowDirtyProjectCheckout || !waiver.AllowLivePatchQueue || !waiver.AllowAgentRequests || !waiver.AllowLiveProjectBranches || !waiver.AllowPendingResumeTriggers {
			return managedRunPreflightResult{}, &managedRunPreflightBlockedError{Reasons: []string{"clean_stop:blocked"}}
		}
		admitted = append(admitted, options)
		return managedRunPreflightResult{ChildExecutablePath: childExecutable}, nil
	}
	nextPID := 8100
	livePIDs := map[int]bool{}
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		nextPID++
		livePIDs[nextPID] = true
		return nextPID, nil
	}
	managedAgentProcessExistsFunc = func(pid int) (bool, error) { return livePIDs[pid], nil }
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) { return true, nil }
	managedAgentKillProcessFunc = func(pid int) error {
		delete(livePIDs, pid)
		return nil
	}
	managedAgentSaveStateFunc = SaveAgentProcessState
	managedAgentStopExitTimeout = 5 * time.Millisecond

	for _, action := range []string{"start", "restart"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/agents/alpha/process", strings.NewReader(`{"action":"`+action+`","resume_continuation_waiver":{"allow_dirty_project_checkout":true,"allow_live_patch_queue":true,"allow_agent_requests":true,"allow_live_project_branches":true,"allow_pending_resume_triggers":true}}`))
		req.Header.Set("Content-Type", "application/json")
		newManagerWebServer().routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", action, rec.Code, rec.Body.String())
		}
		var resp struct {
			OK bool `json:"ok"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode %s response: %v", action, err)
		}
		if !resp.OK {
			t.Fatalf("expected %s response ok, body=%s", action, rec.Body.String())
		}
	}
	if len(admitted) != 2 {
		t.Fatalf("expected start and restart admissions to receive options, got %+v", admitted)
	}
	for _, options := range admitted {
		waiver := options.resumeContinuationWaiver()
		if !waiver.AllowDirtyProjectCheckout || !waiver.AllowLivePatchQueue || !waiver.AllowAgentRequests || !waiver.AllowLiveProjectBranches || !waiver.AllowPendingResumeTriggers {
			t.Fatalf("expected resume continuation waivers, got %+v", admitted)
		}
	}
}

func TestManagerWebBulkProcessStopReturnsPerAgentResults(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	records := []ManagedAgentRecord{
		{AgentID: "alpha", DisplayName: "Alpha", Workdir: filepath.Join(root, "alpha"), WorkspaceID: "ws-1", Role: "generalist"},
		{AgentID: "beta", DisplayName: "Beta", Workdir: filepath.Join(root, "beta"), WorkspaceID: "ws-1", Role: "generalist"},
	}
	for idx, record := range records {
		if err := os.MkdirAll(record.Workdir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := UpsertManagedAgent(record); err != nil {
			t.Fatalf("UpsertManagedAgent(%s) error: %v", record.AgentID, err)
		}
		if err := SaveAgentProcessState(record.Workdir, AgentProcessState{PID: 6100 + idx, Workdir: record.Workdir, Mode: string(RuntimeModeDaemon)}); err != nil {
			t.Fatalf("SaveAgentProcessState(%s) error: %v", record.AgentID, err)
		}
	}

	origExists := managedAgentProcessExistsFunc
	defer func() { managedAgentProcessExistsFunc = origExists }()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) { return false, nil }

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/bulk_process", strings.NewReader(`{"action":"stop","agent_ids":["alpha","missing","beta"],"parallelism":2}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("bulk stop status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK         bool                               `json:"ok"`
		Action     string                             `json:"action"`
		Total      int                                `json:"total"`
		OKCount    int                                `json:"ok_count"`
		ErrorCount int                                `json:"error_count"`
		Results    []managerWebBulkProcessAgentResult `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode bulk stop response: %v", err)
	}
	if resp.OK || resp.Action != "stop" || resp.Total != 3 || resp.OKCount != 2 || resp.ErrorCount != 1 || len(resp.Results) != 3 {
		t.Fatalf("unexpected bulk stop response: %+v", resp)
	}
	byAgent := map[string]managerWebBulkProcessAgentResult{}
	for _, result := range resp.Results {
		byAgent[result.AgentID] = result
	}
	if !byAgent["alpha"].OK || !byAgent["beta"].OK || byAgent["missing"].OK || !strings.Contains(byAgent["missing"].Error, "unknown agent") {
		t.Fatalf("expected two stop successes and one missing-agent error, got %+v", byAgent)
	}
	for _, record := range records {
		if state := LoadAgentProcessState(record.Workdir); state.PID != 0 {
			t.Fatalf("expected %s process state removed, got %+v", record.AgentID, state)
		}
	}
}

func TestManagerWebAgentControlUnsupportedMethodReturnsCurrentStatusAndCatalog(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/control", strings.NewReader(`{"method":"runtime.explode","payload":{}}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected unsupported control method to fail, got %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Error   string                    `json:"error"`
		Live    managerLiveRuntimeStatus  `json:"live"`
		Catalog managerWorkspaceCatalog   `json:"catalog"`
		Process ManagedAgentProcessStatus `json:"process"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode unsupported-control response: %v", err)
	}
	if !strings.Contains(response.Error, "unsupported web control method") {
		t.Fatalf("expected unsupported control method error, got %+v", response)
	}
	if response.Process.State != "stopped" || response.Process.Running {
		t.Fatalf("expected stopped process in unsupported-control response, got %+v", response.Process)
	}
	if response.Live.ProcessState != "stopped" || response.Live.Error != "" {
		t.Fatalf("expected stopped live status in unsupported-control response, got %+v", response.Live)
	}
	if len(response.Catalog.Tasks) != 1 || response.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed task catalog in unsupported-control response, got %+v", response.Catalog.Tasks)
	}
	if len(response.Catalog.Tensions) != 1 || response.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed tension catalog in unsupported-control response, got %+v", response.Catalog.Tensions)
	}
}

func TestManagerWebAgentMessagesMissingControlClientReturnsCurrentStatusAndCatalog(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/agents/lyrica/messages", nil)
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected missing control client to fail, got %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Error           string                    `json:"error"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode missing-client messages response: %v", err)
	}
	if !strings.Contains(response.Error, "rhizome host url is required for live control") {
		t.Fatalf("expected missing control client error, got %+v", response)
	}
	if response.Process.State != "stopped" || response.Process.Running {
		t.Fatalf("expected stopped process in missing-client messages response, got %+v", response.Process)
	}
	if response.Live.ProcessState != "stopped" || !strings.Contains(response.Catalog.Error, "rhizome host url is required for live control") {
		t.Fatalf("expected refreshed live/catalog failure context, got live=%+v catalog=%+v", response.Live, response.Catalog)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected missing-client response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected missing-client response to include refreshed agent overview, got %+v", response.Agents)
	}
	if len(response.Providers) != 0 {
		t.Fatalf("expected missing-client response to preserve current providers, got %+v", response.Providers)
	}
	requireProviderCatalogIncludes(t, "expected missing-client response to include provider catalog", response.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected missing-client response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
}

func TestManagerWebAgentMessagesListFailureReturnsCurrentStatusAndCatalog(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.messages.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32000,
					"message": "message bus unavailable",
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/agents/lyrica/messages", nil)
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected messages list failure to fail, got %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Error           string                    `json:"error"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode messages-list-failure response: %v", err)
	}
	if !strings.Contains(response.Error, "message bus unavailable") {
		t.Fatalf("expected messages list failure error, got %+v", response)
	}
	if response.Process.State != "stopped" || response.Process.Running {
		t.Fatalf("expected stopped process in messages-list-failure response, got %+v", response.Process)
	}
	if response.Live.ProcessState != "stopped" || response.Live.Error != "" {
		t.Fatalf("expected stopped live status in messages-list-failure response, got %+v", response.Live)
	}
	if len(response.Catalog.Tasks) != 1 || response.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed task catalog in messages-list-failure response, got %+v", response.Catalog.Tasks)
	}
	if len(response.Catalog.Tensions) != 1 || response.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed tension catalog in messages-list-failure response, got %+v", response.Catalog.Tensions)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected messages-list-failure response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected messages-list-failure response to include refreshed agent overview, got %+v", response.Agents)
	}
	if len(response.Providers) != 0 {
		t.Fatalf("expected messages-list-failure response to preserve current providers, got %+v", response.Providers)
	}
	requireProviderCatalogIncludes(t, "expected messages-list-failure response to include provider catalog", response.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected messages-list-failure response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
}

func TestManagerWebAgentMessagesSuccessReturnsCurrentStatusAndCatalog(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.messages.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"messages": []map[string]any{
						{
							"message_id":    "msg-1",
							"workspace_id":  "ws-1",
							"from_agent_id": "lyrica",
							"to_agent_id":   "partner-agent",
							"channel":       "ops",
							"content_type":  "text/plain",
							"content":       "hello from lyrica",
							"created_at":    "2026-04-14T12:00:00Z",
						},
					},
				},
			})
		case "workspace.agents.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"agents": []map[string]any{
						{"agent_id": "lyrica", "display_name": "Lyrica"},
						{"agent_id": "partner-agent", "display_name": "Partner Agent"},
					},
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/agents/lyrica/messages?channel=ops&limit=10", nil)
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected messages success response, got %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Messages        []MessageRecord           `json:"messages"`
		AgentMap        map[string]string         `json:"agent_map"`
		AgentID         string                    `json:"agent_id"`
		Count           int                       `json:"count"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode messages success response: %v", err)
	}
	if response.AgentID != "lyrica" || response.Count != 1 || len(response.Messages) != 1 {
		t.Fatalf("expected one returned message for lyrica, got %+v", response)
	}
	if response.Messages[0].MessageID != "msg-1" || response.Messages[0].Channel != "ops" {
		t.Fatalf("expected returned message payload to be preserved, got %+v", response.Messages[0])
	}
	if response.AgentMap["lyrica"] != "Lyrica" || response.AgentMap["partner-agent"] != "Partner Agent" {
		t.Fatalf("expected agent display-name map to be preserved, got %+v", response.AgentMap)
	}
	if response.Process.State != "stopped" || response.Process.Running {
		t.Fatalf("expected stopped process in messages success response, got %+v", response.Process)
	}
	if response.Live.ProcessState != "stopped" || response.Live.Error != "" {
		t.Fatalf("expected stopped live status in messages success response, got %+v", response.Live)
	}
	if len(response.Catalog.Tasks) != 1 || response.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed task catalog in messages success response, got %+v", response.Catalog.Tasks)
	}
	if len(response.Catalog.Tensions) != 1 || response.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed tension catalog in messages success response, got %+v", response.Catalog.Tensions)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected messages success response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected messages success response to include refreshed agent overview, got %+v", response.Agents)
	}
	if len(response.Providers) != 0 {
		t.Fatalf("expected messages success response to preserve current providers, got %+v", response.Providers)
	}
	requireProviderCatalogIncludes(t, "expected messages success response to include provider catalog", response.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected messages success response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
}

func TestManagerWebAgentActivityReturnsCurrentStatusAndOverview(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/agents/lyrica/activity", nil)
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected activity success response, got %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		AgentID         string                    `json:"agent_id"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode activity response: %v", err)
	}
	if response.AgentID != "lyrica" {
		t.Fatalf("expected activity response for lyrica, got %+v", response)
	}
	if response.Process.State != "stopped" || response.Process.Running {
		t.Fatalf("expected stopped process in activity response, got %+v", response.Process)
	}
	if response.Live.ProcessState != "stopped" || response.Live.Error != "" {
		t.Fatalf("expected stopped live status in activity response, got %+v", response.Live)
	}
	if len(response.Catalog.Tasks) != 1 || response.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed task catalog in activity response, got %+v", response.Catalog.Tasks)
	}
	if len(response.Catalog.Tensions) != 1 || response.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed tension catalog in activity response, got %+v", response.Catalog.Tensions)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected activity response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected activity response to include refreshed agent overview, got %+v", response.Agents)
	}
	if len(response.Providers) != 0 {
		t.Fatalf("expected activity response to preserve current providers, got %+v", response.Providers)
	}
	requireProviderCatalogIncludes(t, "expected activity response to include provider catalog", response.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected activity response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
}

func TestManagerWebAgentSettingsDoesNotWriteRuntimeWhenRegistryIsCorrupt(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		AgentID:     "lyrica",
		WorkspaceID: "ws-1",
		AgentToken:  "local-token",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
	before := LoadLocalRuntimeProfile(workdir)
	if err := os.WriteFile(botRegistryPath(), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("WriteFile(botRegistryPath) error: %v", err)
	}

	body := strings.NewReader(`{"llm_backend":"codex","model":"gpt-5.4-mini","planner_sec":"45","watchdog_sec":"90"}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/settings", body)
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().handleAgentSettings(rec, req, record)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected settings save to fail when registry is corrupt, got %d body=%s", rec.Code, rec.Body.String())
	}
	var response struct {
		Error   string                    `json:"error"`
		Live    managerLiveRuntimeStatus  `json:"live"`
		Catalog managerWorkspaceCatalog   `json:"catalog"`
		Process ManagedAgentProcessStatus `json:"process"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode settings failure response: %v", err)
	}
	if !strings.Contains(response.Error, "failed to persist agent state") {
		t.Fatalf("expected persist failure in response, got %+v", response)
	}
	if response.Process.State != "stopped" || response.Process.Running {
		t.Fatalf("expected stopped process in failure response, got %+v", response.Process)
	}
	if response.Live.ProcessState != "stopped" || response.Live.Error != "" {
		t.Fatalf("expected stopped live status in failure response, got %+v", response.Live)
	}
	if len(response.Catalog.Tasks) != 1 || response.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed catalog tasks in failure response, got %+v", response.Catalog.Tasks)
	}
	if len(response.Catalog.Tensions) != 1 || response.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed catalog tensions in failure response, got %+v", response.Catalog.Tensions)
	}

	runtimeProfile := LoadLocalRuntimeProfile(workdir)
	if runtimeProfile.Model != before.Model || runtimeProfile.PlannerSec != before.PlannerSec || runtimeProfile.WatchdogSec != before.WatchdogSec || runtimeProfile.ModelOverride != before.ModelOverride {
		t.Fatalf("expected runtime profile to remain unchanged on failed settings save, got before=%+v after=%+v", before, runtimeProfile)
	}
}

func TestManagerWebAgentEditWorkdirFailureReturnsCurrentStatusAndCatalog(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir := filepath.Join(root, "agent-home")
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		t.Fatalf("mkdir workdir: %v", err)
	}
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
		Role:        "generalist",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	outside := filepath.Join(t.TempDir(), "escape-home")
	body := strings.NewReader(fmt.Sprintf(`{"workdir":%q}`, outside))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/edit", body)
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected workdir move to fail, got %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Error   string                    `json:"error"`
		Live    managerLiveRuntimeStatus  `json:"live"`
		Catalog managerWorkspaceCatalog   `json:"catalog"`
		Process ManagedAgentProcessStatus `json:"process"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode edit failure response: %v", err)
	}
	if !strings.Contains(response.Error, "managed root") {
		t.Fatalf("expected managed root error, got %+v", response)
	}
	if response.Process.State != "stopped" || response.Process.Running {
		t.Fatalf("expected stopped process in failure response, got %+v", response.Process)
	}
	if response.Live.ProcessState != "stopped" || response.Live.Error != "" {
		t.Fatalf("expected stopped live status in failure response, got %+v", response.Live)
	}
	if len(response.Catalog.Tasks) != 1 || response.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed catalog tasks in failure response, got %+v", response.Catalog.Tasks)
	}
	if len(response.Catalog.Tensions) != 1 || response.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed catalog tensions in failure response, got %+v", response.Catalog.Tensions)
	}
}

func TestManagerWebAgentProcessRestartFailureReturnsCurrentProcessState(t *testing.T) {
	setManagerWebTestHome(t)
	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	installCleanManagerSubstrateStubsForTest(t, bin, bin)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	saveTrustedAgentProcessStateForTest(t, record, 1234)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "rpc.methods.list":
			methods := []map[string]string{}
			for _, method := range managerRPCContractRequiredMethods() {
				methods = append(methods, map[string]string{"method": method, "description": "available"})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"count": len(methods), "methods": methods},
			})
		case "rpc.describe":
			params, _ := req["params"].(map[string]any)
			described, _ := params["method"].(string)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"method": described, "description": "schema", "params": managerRPCContractTestParamsSchema(described)},
			})
		case "runtime.build.info":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"vcs_revision": "abc123", "vcs_modified": false, "go_version": "go1.26.1", "binary_sha256": "deadbeef"},
			})
		case "agent.request.open.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"requests": []map[string]any{}},
			})
		case "agent.request":
			params, _ := req["params"].(map[string]any)
			requestID := "req-control"
			if params["method"] == "runtime.status" {
				requestID = "req-status"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   requestID,
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			params, _ := req["params"].(map[string]any)
			requestID, _ := params["request_id"].(string)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   requestID,
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "COMPLETED",
					"response": mustJSON(t, map[string]any{
						"summary":       "steady",
						"paused":        false,
						"attachable":    true,
						"process_state": "running",
					}),
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"sessions": []map[string]any{}},
			})
		case "agent.state.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"value": "{}"},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	callCount := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		callCount++
		switch callCount {
		case 1, 2:
			return true, nil
		default:
			return false, nil
		}
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	killedPID := 0
	managedAgentKillProcessFunc = func(pid int) error {
		killedPID = pid
		return nil
	}
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		return 0, errors.New("spawn failed")
	}
	managedAgentSaveStateFunc = SaveAgentProcessState
	managedAgentStopExitTimeout = 5 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/process", strings.NewReader(`{"action":"restart"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected restart failure status 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK              bool                      `json:"ok"`
		Error           string                    `json:"error"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode restart failure response: %v", err)
	}
	if resp.OK {
		t.Fatalf("expected restart failure response, got %+v", resp)
	}
	if !strings.Contains(resp.Error, "restart agent lyrica stopped current process but failed to start replacement") || !strings.Contains(resp.Error, "spawn failed") {
		t.Fatalf("expected explicit restart failure error, got %+v", resp)
	}
	if resp.Process.State != "stopped" || resp.Process.Running || resp.Process.PID != 0 {
		t.Fatalf("expected current process snapshot to show stopped state, got %+v", resp.Process)
	}
	if resp.Live.ProcessState != "stopped" {
		t.Fatalf("expected restart failure to include refreshed live snapshot, got %+v", resp.Live)
	}
	if len(resp.Catalog.Tasks) != 1 || resp.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected restart failure to include refreshed task catalog, got %+v", resp.Catalog)
	}
	if len(resp.Catalog.Tensions) != 1 || resp.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected restart failure to include refreshed tension catalog, got %+v", resp.Catalog)
	}
	if resp.Defaults.DefaultParentDir != root {
		t.Fatalf("expected restart failure to preserve current defaults, got %+v", resp.Defaults)
	}
	if len(resp.Agents) != 1 || resp.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected restart failure to include refreshed agent overview, got %+v", resp.Agents)
	}
	if len(resp.Providers) != 0 {
		t.Fatalf("expected restart failure to preserve current providers, got %+v", resp.Providers)
	}
	requireProviderCatalogIncludes(t, "expected restart failure to include provider catalog", resp.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(resp.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected restart failure to preserve create default parent dir, got %+v", resp.CreateDefault)
	}
	if killedPID != 0 {
		t.Fatalf("expected restart to stop existing pid gracefully without force kill, got %d", killedPID)
	}
	if _, err := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(err) {
		t.Fatalf("expected failed replacement restart to leave no process state file, stat err=%v", err)
	}
}

func TestManagerWebAgentControlFailureReturnsRequestContext(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "agent.request":
			requestID := "req-failed"
			if params, _ := req["params"].(map[string]any); params["method"] == "runtime.status" {
				requestID = "req-status"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   requestID,
					"workspace_id": "ws-1",
					"to_agent_id":  "lyrica",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			params, _ := req["params"].(map[string]any)
			requestID, _ := params["request_id"].(string)
			result := map[string]any{
				"request_id":   requestID,
				"workspace_id": "ws-1",
				"to_agent_id":  "lyrica",
			}
			switch requestID {
			case "req-failed":
				result["status"] = "FAILED"
				result["response"] = `{"error":"runtime paused and cannot switch task"}`
			case "req-status":
				result["status"] = "COMPLETED"
				result["response"] = mustJSON(t, map[string]any{
					"summary":    "steady",
					"paused":     false,
					"attachable": true,
					"control":    map[string]any{"mode": "live", "last_action": "status"},
				})
			default:
				t.Fatalf("unexpected request_id %q", requestID)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  result,
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/control", strings.NewReader(`{"method":"runtime.switch_task","payload":{"task_id":"task-2"}}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected control failure status 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	if authHeader != "Bearer local-token" {
		t.Fatalf("expected local token auth header, got %q", authHeader)
	}

	var resp struct {
		OK              bool                             `json:"ok"`
		Error           string                           `json:"error"`
		Result          managedAgentControlRequestResult `json:"result"`
		Live            managerLiveRuntimeStatus         `json:"live"`
		Catalog         managerWorkspaceCatalog          `json:"catalog"`
		Process         ManagedAgentProcessStatus        `json:"process"`
		Defaults        BotManagerDefaults               `json:"defaults"`
		Agents          []managerWebAgentRow             `json:"agents"`
		Providers       []ProviderRecord                 `json:"providers"`
		ProviderCatalog []SupportedProviderOption        `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault          `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode control failure response: %v", err)
	}
	if resp.OK {
		t.Fatalf("expected failure response, got %+v", resp)
	}
	if !strings.Contains(resp.Error, "request req-failed finished with status FAILED") {
		t.Fatalf("expected explicit failed request error, got %+v", resp)
	}
	if resp.Result.RequestID != "req-failed" || resp.Result.Status != "FAILED" {
		t.Fatalf("expected failed request context in response, got %+v", resp.Result)
	}
	if !strings.Contains(resp.Result.Response, "runtime paused and cannot switch task") {
		t.Fatalf("expected failed response payload to be preserved, got %+v", resp.Result)
	}
	if resp.Process.State != "stopped" || resp.Process.Running {
		t.Fatalf("expected current process snapshot to be returned, got %+v", resp.Process)
	}
	if resp.Live.ProcessState != "stopped" {
		t.Fatalf("expected failed control response to include refreshed live snapshot, got %+v", resp.Live)
	}
	if len(resp.Catalog.Tasks) != 1 || resp.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected failed control response to include refreshed task catalog, got %+v", resp.Catalog)
	}
	if len(resp.Catalog.Tensions) != 1 || resp.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected failed control response to include refreshed tension catalog, got %+v", resp.Catalog)
	}
	if resp.Defaults.DefaultParentDir != root {
		t.Fatalf("expected failed control response to preserve current defaults, got %+v", resp.Defaults)
	}
	if len(resp.Agents) != 1 || resp.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected failed control response to include refreshed agent overview, got %+v", resp.Agents)
	}
	if len(resp.Providers) != 0 {
		t.Fatalf("expected failed control response to preserve current providers, got %+v", resp.Providers)
	}
	requireProviderCatalogIncludes(t, "expected failed control response to include provider catalog", resp.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(resp.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected failed control response to preserve create default parent dir, got %+v", resp.CreateDefault)
	}
}

func TestManagerWebAgentProcessSuccessReturnsRefreshedStatusAndCatalog(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{
		PID:     1234,
		Workdir: workdir,
	}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"sessions": []map[string]any{},
					"count":    0,
				},
			})
		case "workspace.execution.agent_runs.cancel":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"result": map[string]any{
						"workspace_id":              "ws-1",
						"agent_id":                  "lyrica",
						"execution_runs_cancelled":  0,
						"execution_steps_cancelled": 0,
					},
					"status": "RECORDED",
				},
			})
		case "agent.state.get":
			// Stop cleanup probes runtime scratch; report none (no rows -> ok=false, clean skip).
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origKill := managedAgentKillProcessFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentKillProcessFunc = origKill
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return false, nil
	}
	killedPID := 0
	managedAgentKillProcessFunc = func(pid int) error {
		killedPID = pid
		return nil
	}
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/process", strings.NewReader(`{"action":"stop"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected stop success status 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK              bool                      `json:"ok"`
		Message         string                    `json:"message"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode stop success response: %v", err)
	}
	if !resp.OK || !strings.Contains(resp.Message, "stopped lyrica") {
		t.Fatalf("expected successful stop response, got %+v", resp)
	}
	if resp.Live.ProcessState != "stopped" {
		t.Fatalf("expected refreshed live snapshot to show stopped state, got %+v", resp.Live)
	}
	if len(resp.Catalog.Tasks) != 1 || resp.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed task catalog in stop response, got %+v", resp.Catalog)
	}
	if len(resp.Catalog.Tensions) != 1 || resp.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed tension catalog in stop response, got %+v", resp.Catalog)
	}
	if resp.Process.State != "stopped" || resp.Process.Running || resp.Process.PID != 0 {
		t.Fatalf("expected refreshed process snapshot to show stopped state, got %+v", resp.Process)
	}
	if resp.Defaults.DefaultParentDir != root {
		t.Fatalf("expected stop success response to preserve current defaults, got %+v", resp.Defaults)
	}
	if len(resp.Agents) != 1 || resp.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected stop success response to include refreshed agent overview, got %+v", resp.Agents)
	}
	if len(resp.Providers) != 0 {
		t.Fatalf("expected stop success response to preserve current providers, got %+v", resp.Providers)
	}
	requireProviderCatalogIncludes(t, "expected stop success response to include provider catalog", resp.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(resp.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected stop success response to preserve create default parent dir, got %+v", resp.CreateDefault)
	}
	if killedPID != 0 {
		t.Fatalf("expected stop to use graceful request without force kill, got %d", killedPID)
	}
}

func TestManagerWebAgentProcessStopEndsActiveAgentSessions(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: 4321, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	var ended []map[string]any
	var released []map[string]any
	var budgetReleased []map[string]any
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		methods = append(methods, method)
		switch method {
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"sessions": []map[string]any{
						{"session_id": "sess-lyrica", "workspace_id": "ws-1", "agent_id": "lyrica", "task_id": "task-1", "status": "ACTIVE", "summary": "in progress"},
						{"session_id": "sess-other", "workspace_id": "ws-1", "agent_id": "other", "task_id": "task-2", "status": "ACTIVE", "summary": "do not touch"},
					},
					"count": 2,
				},
			})
		case "agent.task.release":
			params, _ := req["params"].(map[string]any)
			released = append(released, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"workspace_id": params["workspace_id"],
					"task_id":      params["task_id"],
					"agent_id":     params["agent_id"],
					"status":       "RELEASED",
				},
			})
		case "agent.session.end":
			params, _ := req["params"].(map[string]any)
			ended = append(ended, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"status":     "RECORDED",
					"event_type": "session.end",
					"state": map[string]any{
						"session_id": params["session_id"],
						"agent_id":   params["agent_id"],
						"status":     "ENDED",
					},
				},
			})
		case "budget.reservations.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"reservations": []map[string]any{
						{
							"reservation_id":   "res-open-lyrica",
							"account_id":       "pilot-agent-lyrica",
							"amount_micros":    500,
							"spent_micros":     125,
							"released_micros":  0,
							"remaining_micros": 375,
							"status":           "OPEN",
							"workspace_id":     "ws-1",
							"agent_id":         "lyrica",
							"task_id":          "task-budget",
							"run_id":           "run-budget",
							"provider_id":      "codex",
							"model":            "gpt-test",
						},
						{
							"reservation_id":   "res-open-other",
							"account_id":       "pilot-agent-other",
							"amount_micros":    500,
							"remaining_micros": 500,
							"status":           "OPEN",
							"workspace_id":     "ws-1",
							"agent_id":         "other",
							"task_id":          "task-other",
							"run_id":           "run-other",
							"provider_id":      "codex",
							"model":            "gpt-test",
						},
					},
					"count": 2,
				},
			})
		case "budget.release":
			params, _ := req["params"].(map[string]any)
			budgetReleased = append(budgetReleased, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"account": map[string]any{
						"account_id":      params["account_id"],
						"status":          "ACTIVE",
						"reserved_micros": 0,
					},
				},
			})
		case "workspace.execution.agent_runs.cancel":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"result": map[string]any{
					"workspace_id":     "ws-1",
					"agent_id":         "lyrica",
					"runs_cancelled":   0,
					"steps_cancelled":  0,
					"outcome":          "STOPPED_BY_MANAGER",
					"transition_state": "terminalized",
				}},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"tasks": []map[string]any{}},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"items": []map[string]any{}},
			})
		case "agent.state.get":
			// Stop cleanup probes runtime scratch; report none (no rows -> ok=false, clean skip).
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:             server.URL,
		RPCEndpoint:         server.URL,
		WorkspaceID:         "ws-1",
		AgentID:             "manager",
		AgentToken:          "local-token",
		BudgetAccountID:     "pilot-agent-lyrica",
		BudgetReserveMicros: 500,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origKill := managedAgentKillProcessFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentKillProcessFunc = origKill
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return false, nil
	}
	killedPID := 0
	managedAgentKillProcessFunc = func(pid int) error {
		killedPID = pid
		return nil
	}
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/process", strings.NewReader(`{"action":"stop"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected stop success status 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK       bool     `json:"ok"`
		Message  string   `json:"message"`
		Warnings []string `json:"warnings,omitempty"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode stop response: %v", err)
	}
	if !resp.OK || !strings.Contains(resp.Message, "ended 1 active session") || !strings.Contains(resp.Message, "released 1 task claim") || !strings.Contains(resp.Message, "released 1 budget reservation") {
		t.Fatalf("expected stop response to report session end and claim release, got %+v", resp)
	}
	if len(resp.Warnings) != 0 {
		t.Fatalf("expected no cleanup warnings, got %+v", resp.Warnings)
	}
	if killedPID != 0 {
		t.Fatalf("expected stop to use graceful request without force kill, got %d", killedPID)
	}
	if len(ended) != 1 {
		t.Fatalf("expected exactly one session end call, got %+v", ended)
	}
	if len(released) != 1 {
		t.Fatalf("expected exactly one task release call, got %+v", released)
	}
	if len(budgetReleased) != 1 {
		t.Fatalf("expected exactly one budget release call, got %+v", budgetReleased)
	}
	if budgetReleased[0]["reservation_id"] != "res-open-lyrica" || budgetReleased[0]["amount_micros"] != float64(375) || budgetReleased[0]["reason"] != "rhizome_bot_web_stop_cleanup" {
		t.Fatalf("unexpected budget release params: %+v", budgetReleased[0])
	}
	if released[0]["workspace_id"] != "ws-1" || released[0]["agent_id"] != "lyrica" || released[0]["task_id"] != "task-1" {
		t.Fatalf("unexpected task release params: %+v", released[0])
	}
	if ended[0]["session_id"] != "sess-lyrica" || ended[0]["agent_id"] != "lyrica" || ended[0]["task_id"] != "task-1" || ended[0]["status"] != "ENDED" {
		t.Fatalf("unexpected session end params: %+v", ended[0])
	}
	if keep, ok := ended[0]["keep_session_active"].(bool); !ok || keep {
		t.Fatalf("expected keep_session_active=false, got %+v", ended[0]["keep_session_active"])
	}
	releaseIdx := -1
	endIdx := -1
	for idx, method := range methods {
		if method == "agent.task.release" && releaseIdx == -1 {
			releaseIdx = idx
		}
		if method == "agent.session.end" && endIdx == -1 {
			endIdx = idx
		}
	}
	if releaseIdx == -1 || endIdx == -1 || releaseIdx > endIdx {
		t.Fatalf("expected task release before session end, got methods %+v", methods)
	}
}

func TestManagerWebAgentProcessStopForceKillsAfterGraceTimeout(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: 4321, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origCleanup := managedAgentCleanupWorkdirFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentCleanupWorkdirFunc = origCleanup
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	killedPID := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return killedPID == 0, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	managedAgentKillProcessFunc = func(pid int) error {
		killedPID = pid
		return nil
	}
	managedAgentCleanupWorkdirFunc = func(gotWorkdir string) (string, error) {
		return "", nil
	}
	managedAgentStopExitTimeout = 5 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/process", strings.NewReader(`{"action":"stop"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected stop pending status 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK       bool                      `json:"ok"`
		Message  string                    `json:"message"`
		Warnings []string                  `json:"warnings,omitempty"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
		Agents   []managerWebAgentRow      `json:"agents"`
		Defaults BotManagerDefaults        `json:"defaults"`
		Create   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode stop pending response: %v", err)
	}
	if !resp.OK || !strings.Contains(resp.Message, "stopped lyrica") {
		t.Fatalf("expected stopped response, got %+v", resp)
	}
	if resp.Process.Running || resp.Process.State != "stopped" {
		t.Fatalf("expected process snapshot to show stopped after force stop, got %+v", resp.Process)
	}
	if killedPID != 4321 {
		t.Fatalf("expected dashboard stop to force-kill pid 4321 after grace timeout, got %d", killedPID)
	}
}

func TestRequestManagedAgentGracefulStopWaitsForPostRequestExit(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: 4321, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origCleanup := managedAgentCleanupWorkdirFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentCleanupWorkdirFunc = origCleanup
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	existsCalls := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		existsCalls++
		return existsCalls == 1, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	cleanupCalled := false
	managedAgentCleanupWorkdirFunc = func(gotWorkdir string) (string, error) {
		cleanupCalled = true
		if gotWorkdir != workdir {
			t.Fatalf("cleanup workdir=%q, want %q", gotWorkdir, workdir)
		}
		return "", nil
	}
	managedAgentStopExitTimeout = 50 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	stopped, err := RequestManagedAgentGracefulStop(record)
	if err != nil {
		t.Fatalf("RequestManagedAgentGracefulStop() error: %v", err)
	}
	if !stopped {
		t.Fatal("expected graceful stop to observe process exit after writing stop request")
	}
	if existsCalls < 2 {
		t.Fatalf("expected process existence to be checked again after stop request, got %d", existsCalls)
	}
	if !cleanupCalled {
		t.Fatal("expected post-exit cleanup to run")
	}
	if state := LoadAgentProcessState(workdir); state.PID != 0 {
		t.Fatalf("expected process state removed, got %+v", state)
	}
	if managedAgentGracefulStopRequested(workdir) {
		t.Fatal("expected graceful stop marker to be cleared after observed exit")
	}
}

func TestManagerWebAgentProcessStopReleasesProjectImplementationClaim(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: 4321, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	var ended []map[string]any
	var released []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"sessions": []map[string]any{{
						"session_id":   "sess-lyrica",
						"workspace_id": "ws-1",
						"agent_id":     "lyrica",
						"task_id":      "task-project-impl",
						"status":       "ACTIVE",
						"summary":      "publishing project branch",
					}},
					"count": 1,
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"tasks": []map[string]any{{
					"task_id":                "task-project-impl",
					"title":                  "Implement project slice",
					"owner_user_id":          "developer",
					"priority":               "HIGH",
					"status":                 "RUNNING",
					"task_kind":              "EXECUTION",
					"task_template":          "generic",
					"project_id":             "project-subpixel",
					"project_lane":           "implementation",
					"requires_project_gate":  true,
					"linked_by":              "lead",
					"claim_agent_id":         "lyrica",
					"claim_status":           "CLAIMED",
					"claim_branch_id":        "branch-impl",
					"claim_checkout_id":      "checkout-impl",
					"claim_write_scope_json": `{"paths":["src/**","package.json"]}`,
				}}},
			})
		case "agent.task.release":
			params, _ := req["params"].(map[string]any)
			released = append(released, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{},
			})
		case "agent.session.end":
			params, _ := req["params"].(map[string]any)
			ended = append(ended, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"state": map[string]any{
						"session_id": params["session_id"],
						"agent_id":   params["agent_id"],
						"status":     "ENDED",
					},
				},
			})
		case "workspace.execution.agent_runs.cancel":
			params, _ := req["params"].(map[string]any)
			if params["agent_id"] != "lyrica" {
				t.Fatalf("unexpected execution cleanup params: %+v", params)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"result": map[string]any{
					"workspace_id":     "ws-1",
					"agent_id":         "lyrica",
					"runs_cancelled":   1,
					"steps_cancelled":  2,
					"outcome":          "STOPPED_BY_MANAGER",
					"transition_state": "terminalized",
					"runtime_event_id": "rtev-stop",
				}},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"items": []map[string]any{}},
			})
		case "agent.state.get":
			// Stop cleanup probes runtime scratch; report none (no rows -> ok=false, clean skip).
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origKill := managedAgentKillProcessFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentKillProcessFunc = origKill
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return false, nil
	}
	managedAgentKillProcessFunc = func(pid int) error { return nil }
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/process", strings.NewReader(`{"action":"stop"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected stop success status 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK       bool     `json:"ok"`
		Message  string   `json:"message"`
		Warnings []string `json:"warnings,omitempty"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode stop response: %v", err)
	}
	if !resp.OK || !strings.Contains(resp.Message, "ended 1 active session") || !strings.Contains(resp.Message, "released 1 task claim") || !strings.Contains(resp.Message, "cancelled 1 execution run") || !strings.Contains(resp.Message, "cancelled 2 execution step") {
		t.Fatalf("expected stop response to release project claim ownership, got %+v", resp)
	}
	if len(resp.Warnings) != 0 {
		t.Fatalf("expected no cleanup warnings, got %+v", resp.Warnings)
	}
	if len(released) != 1 || released[0]["task_id"] != "task-project-impl" || released[0]["agent_id"] != "lyrica" {
		t.Fatalf("expected project implementation claim to be released, got release calls %+v", released)
	}
	if len(ended) != 1 || ended[0]["task_id"] != "task-project-impl" {
		t.Fatalf("expected active session to end after releasing claim, got %+v", ended)
	}
}

func TestCloseManagedAgentStopCleanupPreservesBlockedSessionClaim(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}

	var released []map[string]any
	var ended []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"sessions": []map[string]any{{
					"session_id":   "sess-blocked",
					"agent_id":     "lyrica",
					"workspace_id": "ws-1",
					"task_id":      "task-blocked",
					"status":       "BLOCKED",
					"summary":      "durable blocker already published",
				}}, "count": 1},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"tasks": []map[string]any{{
					"task_id":          "task-blocked",
					"title":            "Blocked submit handoff",
					"owner_user_id":    "developer",
					"priority":         "HIGH",
					"status":           "RUNNING",
					"task_kind":        "EXECUTION",
					"task_template":    "generic",
					"linked_by":        "lead",
					"claim_agent_id":   "lyrica",
					"claim_status":     "BLOCKED",
					"claim_summary":    "blocked before stop",
					"claim_updated_at": "2026-06-09T11:00:00Z",
				}}},
			})
		case "agent.task.release":
			params, _ := req["params"].(map[string]any)
			released = append(released, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{},
			})
		case "agent.session.end":
			params, _ := req["params"].(map[string]any)
			ended = append(ended, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"state": map[string]any{"session_id": params["session_id"], "status": "ENDED"}},
			})
		case "workspace.execution.agent_runs.cancel":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"result": map[string]any{
					"workspace_id":     "ws-1",
					"agent_id":         "lyrica",
					"runs_cancelled":   0,
					"steps_cancelled":  0,
					"outcome":          "STOPPED_BY_MANAGER",
					"transition_state": "terminalized",
				}},
			})
		case "agent.state.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	cleanup, err := closeManagedAgentActiveSessionsAfterStop(context.Background(), record)
	if err != nil {
		t.Fatalf("closeManagedAgentActiveSessionsAfterStop() error: %v", err)
	}
	if cleanup.SessionsEnded != 1 || cleanup.TaskClaimsReleased != 0 || len(cleanup.Warnings) != 0 {
		t.Fatalf("expected blocked session to end without releasing claim, got %+v", cleanup)
	}
	if len(released) != 0 {
		t.Fatalf("expected blocked session claim to stay parked, got release calls %+v", released)
	}
	if len(ended) != 1 || ended[0]["task_id"] != "task-blocked" {
		t.Fatalf("expected blocked session to be ended, got %+v", ended)
	}
}

func TestCloseManagedAgentStopCleanupPreservesBlockedClaimOnlyTask(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}

	var released []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"sessions": []map[string]any{}, "count": 0},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"tasks": []map[string]any{{
					"task_id":          "task-claim-only",
					"title":            "Residual blocked claim",
					"owner_user_id":    "developer",
					"priority":         "HIGH",
					"status":           "RUNNING",
					"task_kind":        "EXECUTION",
					"task_template":    "generic",
					"linked_by":        "lead",
					"claim_agent_id":   "lyrica",
					"claim_status":     "BLOCKED",
					"claim_summary":    "session failed before durable end",
					"claim_updated_at": "2026-05-28T10:00:00Z",
				}}},
			})
		case "agent.task.release":
			params, _ := req["params"].(map[string]any)
			released = append(released, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{},
			})
		case "workspace.execution.agent_runs.cancel":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"result": map[string]any{
					"workspace_id":     "ws-1",
					"agent_id":         "lyrica",
					"runs_cancelled":   0,
					"steps_cancelled":  0,
					"outcome":          "STOPPED_BY_MANAGER",
					"transition_state": "terminalized",
				}},
			})
		case "agent.state.get":
			// Stop cleanup probes runtime scratch; report none (no rows -> ok=false, clean skip).
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	cleanup, err := closeManagedAgentActiveSessionsAfterStop(context.Background(), record)
	if err != nil {
		t.Fatalf("closeManagedAgentActiveSessionsAfterStop() error: %v", err)
	}
	if cleanup.SessionsEnded != 0 || cleanup.TaskClaimsReleased != 0 || len(cleanup.Warnings) != 0 {
		t.Fatalf("unexpected cleanup result: %+v", cleanup)
	}
	if len(released) != 0 {
		t.Fatalf("expected blocked claim-only task to stay parked, got release calls %+v", released)
	}
}

func TestCloseManagedAgentStopCleanupToleratesClaimOnlyReleaseStaleNoOp(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}

	var released []map[string]any
	tasksListCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"sessions": []map[string]any{}, "count": 0},
			})
		case "workspace.tasks.list":
			tasksListCalls++
			task := map[string]any{
				"task_id":          "task-claim-only",
				"title":            "Residual claim-only task",
				"owner_user_id":    "developer",
				"priority":         "HIGH",
				"status":           "RUNNING",
				"task_kind":        "EXECUTION",
				"task_template":    "generic",
				"linked_by":        "lead",
				"claim_agent_id":   "lyrica",
				"claim_status":     "CLAIMED",
				"claim_summary":    "session already stopped",
				"claim_updated_at": "2026-06-05T20:52:33Z",
			}
			if tasksListCalls > 1 {
				task["status"] = "PENDING"
				task["claim_status"] = "RELEASED"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"tasks": []map[string]any{task}},
			})
		case "agent.task.release":
			params, _ := req["params"].(map[string]any)
			released = append(released, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32000,
					"message": "task claim transition is stale or duplicate",
				},
			})
		case "workspace.execution.agent_runs.cancel":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"result": map[string]any{
					"workspace_id":     "ws-1",
					"agent_id":         "lyrica",
					"runs_cancelled":   0,
					"steps_cancelled":  0,
					"outcome":          "STOPPED_BY_MANAGER",
					"transition_state": "terminalized",
				}},
			})
		case "agent.state.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	cleanup, err := closeManagedAgentActiveSessionsAfterStop(context.Background(), record)
	if err != nil {
		t.Fatalf("closeManagedAgentActiveSessionsAfterStop() error = %v", err)
	}
	if cleanup.SessionsEnded != 0 || cleanup.TaskClaimsReleased != 1 || len(cleanup.Warnings) != 0 {
		t.Fatalf("expected stale claim-only release to be a benign no-op success, got %+v", cleanup)
	}
	if len(released) != 1 || released[0]["task_id"] != "task-claim-only" || released[0]["agent_id"] != "lyrica" {
		t.Fatalf("expected one claim-only release attempt, got %+v", released)
	}
	if tasksListCalls != 1 {
		t.Fatalf("expected no readback after benign stale release, calls=%d", tasksListCalls)
	}
}

func TestCloseManagedAgentStopCleanupReleasesClaimOnlyProjectClaims(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}

	var released []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"sessions": []map[string]any{}, "count": 0},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"tasks": []map[string]any{
					{
						"task_id":                "task-patchq-submit-branch-ready",
						"title":                  "Owner-only project_patch_queue_submit for branch branch-ready",
						"owner_user_id":          "developer",
						"priority":               "HIGH",
						"status":                 "RUNNING",
						"task_kind":              "EXECUTION",
						"task_template":          "integration",
						"project_id":             "project-subpixel",
						"project_lane":           "integration",
						"requires_project_gate":  true,
						"tags":                   []string{"project", "patch-queue", "integration", "owner-bound", "owner-bound-kind:patch_queue_submit", "required-agent:lyrica"},
						"claim_agent_id":         "lyrica",
						"claim_status":           "CLAIMED",
						"claim_branch_id":        "branch-ready",
						"claim_checkout_id":      "checkout-ready",
						"claim_write_scope_json": `{"paths":["src/**"]}`,
					},
					{
						"task_id":                "task-project-impl",
						"title":                  "Implement product slice",
						"description":            "After implementation, create a project_patch_queue_submit handoff.",
						"owner_user_id":          "developer",
						"priority":               "HIGH",
						"status":                 "RUNNING",
						"task_kind":              "EXECUTION",
						"task_template":          "generic",
						"project_id":             "project-subpixel",
						"project_lane":           "implementation",
						"requires_project_gate":  true,
						"claim_agent_id":         "lyrica",
						"claim_status":           "CLAIMED",
						"claim_branch_id":        "branch-impl",
						"claim_checkout_id":      "checkout-impl",
						"claim_write_scope_json": `{"paths":["src/product/**"]}`,
					},
				}},
			})
		case "agent.task.release":
			params, _ := req["params"].(map[string]any)
			released = append(released, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{},
			})
		case "workspace.execution.agent_runs.cancel":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"result": map[string]any{
					"workspace_id":     "ws-1",
					"agent_id":         "lyrica",
					"runs_cancelled":   0,
					"steps_cancelled":  0,
					"outcome":          "STOPPED_BY_MANAGER",
					"transition_state": "terminalized",
				}},
			})
		case "agent.state.get":
			// Stop cleanup probes runtime scratch; report none (no rows -> ok=false, clean skip).
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	cleanup, err := closeManagedAgentActiveSessionsAfterStop(context.Background(), record)
	if err != nil {
		t.Fatalf("closeManagedAgentActiveSessionsAfterStop() error: %v", err)
	}
	if cleanup.SessionsEnded != 0 || cleanup.TaskClaimsReleased != 2 || len(cleanup.Warnings) != 0 {
		t.Fatalf("unexpected cleanup result: %+v", cleanup)
	}
	if len(released) != 2 || released[0]["agent_id"] != "lyrica" || released[1]["agent_id"] != "lyrica" {
		t.Fatalf("expected both project claims to be released, got %+v", released)
	}
	releasedTasks := map[string]bool{}
	for _, call := range released {
		if taskID, _ := call["task_id"].(string); taskID != "" {
			releasedTasks[taskID] = true
		}
	}
	if !releasedTasks["task-patchq-submit-branch-ready"] || !releasedTasks["task-project-impl"] {
		t.Fatalf("expected owner-bound and implementation project claim releases, got %+v", released)
	}
}

func TestCleanupManagedAgentProjectCheckoutsAfterStopPreservesLocalDirtyState(t *testing.T) {
	checkoutPath := t.TempDir()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skipf("git not available: %v", err)
	}
	if out, err := exec.Command("git", "-C", checkoutPath, "init").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	if err := os.MkdirAll(filepath.Join(checkoutPath, "internal", "token"), 0o755); err != nil {
		t.Fatalf("mkdir checkout file dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "internal", "token", "token.go"), []byte("package token\n"), 0o644); err != nil {
		t.Fatalf("write checkout file: %v", err)
	}

	var registered map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.list":
			writeRPCResult(w, req, map[string]any{"projects": []map[string]any{{
				"project_id":   "project-lua",
				"workspace_id": "ws-1",
				"title":        "Lua capability",
				"status":       "ACTIVE",
			}}})
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"project_id":   "project-lua",
						"workspace_id": "ws-1",
						"title":        "Lua capability",
						"status":       "ACTIVE",
					},
					"checkouts": []map[string]any{{
						"checkout_id":     "projcheckout-r23-delta",
						"workspace_id":    "ws-1",
						"project_id":      "project-lua",
						"repo_id":         "repo-lua",
						"machine_id":      "machine-local",
						"machine_label":   "local",
						"owner_user_id":   "developer",
						"agent_id":        "delta",
						"local_path":      checkoutPath,
						"checkout_kind":   "agent",
						"branch_name":     "agent-delta-p-0df6e988e5-t-a1deaa6c17",
						"base_branch":     "main",
						"head_sha":        "919f1560a034ba277c2fc71d316087b31b535e4b",
						"base_sha":        "04fff399",
						"dirty_state":     "clean",
						"active_task_id":  "task-signal01-lua-lexer-parser-front",
						"active_claim_id": "task-signal01-lua-lexer-parser-front",
						"status":          "ACTIVE",
					}},
				},
			})
		case "project.checkout.register":
			registered = req.Params
			writeRPCResult(w, req, map[string]any{"checkout": registered})
		default:
			t.Fatalf("unexpected rpc method %q", req.Method)
		}
	}))
	defer server.Close()

	result := managedAgentStopCleanupResult{}
	cleanupManagedAgentProjectCheckoutsAfterStop(context.Background(), NewRhizomeClient(server.URL, "token"), "ws-1", "delta", &result)
	if result.ProjectCheckoutsAbandoned != 1 || len(result.Warnings) != 0 {
		t.Fatalf("unexpected cleanup result: %+v", result)
	}
	if registered == nil {
		t.Fatal("expected checkout register call")
	}
	if registered["dirty_state"] != "dirty" {
		t.Fatalf("expected dirty_state to reflect local git dirtiness, got %+v", registered)
	}
	if registered["status"] != "ABANDONED" || strings.TrimSpace(rpcString(registered, "active_task_id")) != "" || strings.TrimSpace(rpcString(registered, "active_claim_id")) != "" {
		t.Fatalf("expected abandoned checkout with cleared active refs, got %+v", registered)
	}
}

func TestManagerWebAgentProcessStopReleasesClaimOnlyProjectImplementationClaim(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: 4321, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	var released []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"sessions": []map[string]any{}, "count": 0},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"tasks": []map[string]any{{
					"task_id":                "task-project-claim-only",
					"title":                  "Implement project slice",
					"owner_user_id":          "developer",
					"priority":               "HIGH",
					"status":                 "RUNNING",
					"task_kind":              "EXECUTION",
					"task_template":          "generic",
					"project_id":             "project-subpixel",
					"project_lane":           "implementation",
					"requires_project_gate":  true,
					"linked_by":              "lead",
					"claim_agent_id":         "lyrica",
					"claim_status":           "CLAIMED",
					"claim_branch_id":        "branch-impl",
					"claim_checkout_id":      "checkout-impl",
					"claim_write_scope_json": `{"paths":["src/**","package.json"]}`,
				}}},
			})
		case "agent.task.release":
			params, _ := req["params"].(map[string]any)
			released = append(released, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{},
			})
		case "workspace.execution.agent_runs.cancel":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"result": map[string]any{
					"workspace_id":     "ws-1",
					"agent_id":         "lyrica",
					"runs_cancelled":   1,
					"steps_cancelled":  1,
					"outcome":          "STOPPED_BY_MANAGER",
					"transition_state": "terminalized",
				}},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"items": []map[string]any{}},
			})
		case "agent.state.get":
			// Stop cleanup probes runtime scratch; report none (no rows -> ok=false, clean skip).
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origKill := managedAgentKillProcessFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentKillProcessFunc = origKill
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return false, nil
	}
	managedAgentKillProcessFunc = func(pid int) error { return nil }
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/process", strings.NewReader(`{"action":"stop"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected stop success status 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK       bool     `json:"ok"`
		Message  string   `json:"message"`
		Warnings []string `json:"warnings,omitempty"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode stop response: %v", err)
	}
	if !resp.OK || !strings.Contains(resp.Message, "released 1 task claim") {
		t.Fatalf("expected stop response to report released claim-only project claim, got %+v", resp)
	}
	if len(resp.Warnings) != 0 {
		t.Fatalf("expected no cleanup warnings, got %+v", resp.Warnings)
	}
	if len(released) != 1 || released[0]["task_id"] != "task-project-claim-only" || released[0]["agent_id"] != "lyrica" {
		t.Fatalf("expected project implementation claim to be released, got release calls %+v", released)
	}
}

func TestCloseManagedAgentStopCleanupUsesScratchWhenSessionListFails(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}

	scratchRaw, err := json.Marshal(RuntimeScratchState{
		ActiveSessionID: "session-scratch",
		ActiveTaskID:    "task-scratch",
		ActiveRunID:     "run-scratch",
		PendingTrigger:  "request_resume",
	})
	if err != nil {
		t.Fatalf("marshal scratch: %v", err)
	}
	stateGetCalls := 0
	var released []map[string]any
	var ended []map[string]any
	var stateSets []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32000,
					"message": "context deadline exceeded",
				},
			})
		case "agent.state.get":
			stateGetCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"value": string(scratchRaw)},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"tasks": []map[string]any{{
					"task_id":                "task-scratch",
					"title":                  "Scratch-backed active task",
					"owner_user_id":          "developer",
					"priority":               "HIGH",
					"status":                 "RUNNING",
					"task_kind":              "EXECUTION",
					"task_template":          "generic",
					"project_id":             "project-subpixel",
					"project_lane":           "implementation",
					"requires_project_gate":  true,
					"claim_agent_id":         "lyrica",
					"claim_status":           "CLAIMED",
					"claim_branch_id":        "branch-scratch",
					"claim_checkout_id":      "checkout-scratch",
					"claim_write_scope_json": `{"paths":["src/**"]}`,
				}}},
			})
		case "agent.task.release":
			params, _ := req["params"].(map[string]any)
			released = append(released, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{},
			})
		case "agent.session.end":
			params, _ := req["params"].(map[string]any)
			ended = append(ended, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"status":     "RECORDED",
					"event_type": "session.end",
					"state": map[string]any{
						"session_id": params["session_id"],
						"agent_id":   params["agent_id"],
						"status":     "ENDED",
					},
				},
			})
		case "workspace.execution.agent_runs.cancel":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"result": map[string]any{
					"workspace_id":     "ws-1",
					"agent_id":         "lyrica",
					"runs_cancelled":   0,
					"steps_cancelled":  0,
					"outcome":          "STOPPED_BY_MANAGER",
					"transition_state": "terminalized",
				}},
			})
		case "agent.state.set":
			params, _ := req["params"].(map[string]any)
			stateSets = append(stateSets, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	cleanup, err := closeManagedAgentActiveSessionsAfterStop(context.Background(), record)
	if err != nil {
		t.Fatalf("closeManagedAgentActiveSessionsAfterStop() error: %v", err)
	}
	if cleanup.SessionsEnded != 1 || cleanup.TaskClaimsReleased != 1 || cleanup.ScratchStatesCleared != 1 {
		t.Fatalf("expected scratch fallback to end session, release claim, and clear scratch, got %+v", cleanup)
	}
	if len(cleanup.Warnings) != 1 || !strings.Contains(cleanup.Warnings[0], "active session listing skipped") {
		t.Fatalf("expected one session-list warning, got %+v", cleanup.Warnings)
	}
	if stateGetCalls < 2 {
		t.Fatalf("expected scratch read for fallback and clearing, got %d calls", stateGetCalls)
	}
	if len(released) != 1 || released[0]["task_id"] != "task-scratch" || released[0]["agent_id"] != "lyrica" {
		t.Fatalf("expected scratch task release, got %+v", released)
	}
	if len(ended) != 1 || ended[0]["session_id"] != "session-scratch" || ended[0]["task_id"] != "task-scratch" {
		t.Fatalf("expected scratch session end, got %+v", ended)
	}
	if len(stateSets) != 1 {
		t.Fatalf("expected scratch clear write, got %+v", stateSets)
	}
	var cleared RuntimeScratchState
	value, _ := stateSets[0]["value"].(string)
	if err := json.Unmarshal([]byte(value), &cleared); err != nil {
		t.Fatalf("decode cleared scratch: %v", err)
	}
	if scratchHasActivePresence(cleared) || cleared.PendingTrigger != "" {
		t.Fatalf("expected active scratch residue cleared, got %+v", cleared)
	}
}

func TestManagerWebStopBudgetCleanupFallsBackToLedgerList(t *testing.T) {
	setManagerWebTestHome(t)

	var budgetReleased []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "budget.reservations.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32601,
					"message": "method not found: budget.reservations.list",
				},
			})
		case "budget.ledger.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"entries": []map[string]any{
						{
							"entry_id":        "reserve-open",
							"idempotency_key": "reserve-open",
							"account_id":      "pilot-agent-lyrica",
							"reservation_id":  "res-open-legacy",
							"entry_type":      "RESERVATION",
							"amount_micros":   500,
							"workspace_id":    "ws-1",
							"agent_id":        "lyrica",
							"task_id":         "task-budget",
							"run_id":          "run-budget",
							"provider_id":     "codex",
							"model":           "gpt-test",
							"reason":          "runtime_llm_provider_call",
							"created_at":      "2026-05-07T00:00:00Z",
						},
						{
							"entry_id":        "spend-open",
							"idempotency_key": "spend-open",
							"account_id":      "pilot-agent-lyrica",
							"reservation_id":  "res-open-legacy",
							"entry_type":      "SPEND",
							"amount_micros":   125,
							"workspace_id":    "ws-1",
							"agent_id":        "lyrica",
							"task_id":         "task-budget",
							"run_id":          "run-budget",
							"provider_id":     "codex",
							"model":           "gpt-test",
							"created_at":      "2026-05-07T00:00:01Z",
						},
						{
							"entry_id":        "reserve-other",
							"idempotency_key": "reserve-other",
							"account_id":      "pilot-agent-lyrica",
							"reservation_id":  "res-open-other",
							"entry_type":      "RESERVATION",
							"amount_micros":   500,
							"workspace_id":    "ws-1",
							"agent_id":        "other",
							"task_id":         "task-other",
							"run_id":          "run-other",
							"provider_id":     "codex",
							"model":           "gpt-test",
							"created_at":      "2026-05-07T00:00:02Z",
						},
					},
					"count": 3,
				},
			})
		case "budget.release":
			params, _ := req["params"].(map[string]any)
			budgetReleased = append(budgetReleased, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"account": map[string]any{
						"account_id":      params["account_id"],
						"status":          "ACTIVE",
						"reserved_micros": 0,
					},
				},
			})
		case "agent.state.get":
			// Stop cleanup probes runtime scratch; report none (no rows -> ok=false, clean skip).
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "local-token")
	cleanup := managedAgentStopCleanupResult{}
	cleanupManagedAgentOpenBudgetReservationsAfterStop(context.Background(), client, LocalRuntimeProfile{
		BudgetAccountID:     "pilot-agent-lyrica",
		BudgetReserveMicros: 500,
	}, "ws-1", "lyrica", &cleanup)

	if len(cleanup.Warnings) != 0 {
		t.Fatalf("expected no fallback cleanup warnings, got %+v", cleanup.Warnings)
	}
	if cleanup.BudgetReservationsReleased != 1 || len(budgetReleased) != 1 {
		t.Fatalf("expected one fallback budget release, cleanup=%+v params=%+v", cleanup, budgetReleased)
	}
	if budgetReleased[0]["reservation_id"] != "res-open-legacy" || budgetReleased[0]["amount_micros"] != float64(375) {
		t.Fatalf("unexpected fallback budget release params: %+v", budgetReleased[0])
	}
}

func TestManagerWebAgentProcessStopEndsSessionWhenTaskReleaseFails(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: 4321, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	var ended []map[string]any
	var released []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"sessions": []map[string]any{
						{"session_id": "sess-lyrica", "workspace_id": "ws-1", "agent_id": "lyrica", "task_id": "task-1", "status": "ACTIVE", "summary": "in progress"},
					},
					"count": 1,
				},
			})
		case "agent.task.release":
			params, _ := req["params"].(map[string]any)
			released = append(released, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32000,
					"message": "storage unavailable while releasing task claim",
				},
			})
		case "agent.session.end":
			params, _ := req["params"].(map[string]any)
			ended = append(ended, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"status":     "RECORDED",
					"event_type": "session.end",
					"state": map[string]any{
						"session_id": params["session_id"],
						"agent_id":   params["agent_id"],
						"status":     "ENDED",
					},
				},
			})
		case "workspace.execution.agent_runs.cancel":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"result": map[string]any{
					"workspace_id":     "ws-1",
					"agent_id":         "lyrica",
					"runs_cancelled":   0,
					"steps_cancelled":  0,
					"outcome":          "STOPPED_BY_MANAGER",
					"transition_state": "terminalized",
				}},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"tasks": []map[string]any{}},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"items": []map[string]any{}},
			})
		case "agent.state.get":
			// Stop cleanup probes runtime scratch; report none (no rows -> ok=false, clean skip).
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origKill := managedAgentKillProcessFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentKillProcessFunc = origKill
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return false, nil
	}
	managedAgentKillProcessFunc = func(pid int) error { return nil }
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/process", strings.NewReader(`{"action":"stop"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected stop success status 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK       bool     `json:"ok"`
		Message  string   `json:"message"`
		Warnings []string `json:"warnings,omitempty"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode stop response: %v", err)
	}
	if !resp.OK || !strings.Contains(resp.Message, "ended 1 active session") {
		t.Fatalf("expected stop response to still report ended active session, got %+v", resp)
	}
	if len(resp.Warnings) != 1 || !strings.Contains(resp.Warnings[0], "task claim cleanup skipped for task-1") {
		t.Fatalf("expected task release cleanup warning, got %+v", resp.Warnings)
	}
	if len(released) != 1 {
		t.Fatalf("expected exactly one task release attempt, got %+v", released)
	}
	if len(ended) != 1 {
		t.Fatalf("expected session end despite release failure, got %+v", ended)
	}
}

func TestManagerWebAgentProcessStopToleratesStaleTaskReleaseNoOp(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: 4321, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	var ended []map[string]any
	var released []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"sessions": []map[string]any{
						{"session_id": "sess-lyrica", "workspace_id": "ws-1", "agent_id": "lyrica", "task_id": "task-1", "status": "ACTIVE", "summary": "in progress"},
					},
					"count": 1,
				},
			})
		case "agent.task.release":
			params, _ := req["params"].(map[string]any)
			released = append(released, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32000,
					"message": "task claim transition is stale or duplicate",
				},
			})
		case "agent.session.end":
			params, _ := req["params"].(map[string]any)
			ended = append(ended, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"status":     "RECORDED",
					"event_type": "session.end",
					"state": map[string]any{
						"session_id": params["session_id"],
						"agent_id":   params["agent_id"],
						"status":     "ENDED",
					},
				},
			})
		case "workspace.execution.agent_runs.cancel":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"result": map[string]any{
					"workspace_id":     "ws-1",
					"agent_id":         "lyrica",
					"runs_cancelled":   0,
					"steps_cancelled":  0,
					"outcome":          "STOPPED_BY_MANAGER",
					"transition_state": "terminalized",
				}},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"tasks": []map[string]any{}},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"items": []map[string]any{}},
			})
		case "agent.state.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origKill := managedAgentKillProcessFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentKillProcessFunc = origKill
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return false, nil
	}
	managedAgentKillProcessFunc = func(pid int) error { return nil }
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/process", strings.NewReader(`{"action":"stop"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected stop success status 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK       bool     `json:"ok"`
		Message  string   `json:"message"`
		Warnings []string `json:"warnings,omitempty"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode stop response: %v", err)
	}
	if !resp.OK || !strings.Contains(resp.Message, "ended 1 active session") || !strings.Contains(resp.Message, "released 1 task claim") {
		t.Fatalf("expected stop response to report benign stale release success, got %+v", resp)
	}
	if len(resp.Warnings) != 0 {
		t.Fatalf("expected no cleanup warnings, got %+v", resp.Warnings)
	}
	if len(released) != 1 {
		t.Fatalf("expected exactly one task release attempt, got %+v", released)
	}
	if len(ended) != 1 {
		t.Fatalf("expected session end despite stale release no-op, got %+v", ended)
	}
}

func TestManagerWebBulkStopCleanupUsesBackgroundAndFailsIncompleteCleanup(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveAgentProcessState(workdir, AgentProcessState{PID: 4321, Workdir: workdir}); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}

	var ended []map[string]any
	var released []map[string]any
	cancelRunsCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"sessions": []map[string]any{
						{"session_id": "sess-lyrica", "workspace_id": "ws-1", "agent_id": "lyrica", "task_id": "task-1", "status": "ACTIVE", "summary": "in progress"},
					},
					"count": 1,
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"tasks": []map[string]any{}},
			})
		case "agent.task.release":
			params, _ := req["params"].(map[string]any)
			released = append(released, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32000,
					"message": "storage unavailable while releasing task claim",
				},
			})
		case "agent.session.end":
			params, _ := req["params"].(map[string]any)
			ended = append(ended, params)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"status": "RECORDED"},
			})
		case "workspace.execution.agent_runs.cancel":
			cancelRunsCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{"result": map[string]any{
					"workspace_id":     "ws-1",
					"agent_id":         "lyrica",
					"runs_cancelled":   1,
					"steps_cancelled":  1,
					"outcome":          "STOPPED_BY_MANAGER",
					"transition_state": "terminalized",
				}},
			})
		case "agent.state.get":
			// Stop cleanup probes runtime scratch; report none (no rows -> ok=false, clean skip).
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error":   map[string]any{"code": -32004, "message": "no rows"},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origKill := managedAgentKillProcessFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentKillProcessFunc = origKill
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return false, nil
	}
	managedAgentKillProcessFunc = func(pid int) error { return nil }
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	result := runManagerWebProcessAction(cancelledCtx, record, "stop", true)
	if result.OK || !strings.Contains(result.Error, "session cleanup was incomplete") {
		t.Fatalf("expected incomplete cleanup to fail the bulk result, got %+v", result)
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "task claim cleanup skipped for task-1") {
		t.Fatalf("expected release cleanup warning, got %+v", result.Warnings)
	}
	if len(released) != 1 || len(ended) != 1 || cancelRunsCalls != 1 {
		t.Fatalf("expected cleanup to ignore canceled request context and finish durable calls, released=%+v ended=%+v cancelRuns=%d", released, ended, cancelRunsCalls)
	}
}

func TestManagerWebAgentDetailControlAndEditRoutes(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendAuto,
		Model:       defaultModel,
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveAgentProfile(workdir, AgentProfile{
		AgentID:               "lyrica",
		DisplayName:           "Lyrica",
		Role:                  "generalist",
		PrimarySpecialization: "generalist",
		Mission:               "Ship useful work.",
	}); err != nil {
		t.Fatalf("SaveAgentProfile() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:           "https://rhizome.test",
		RPCEndpoint:       "https://rhizome.test/rpc",
		WorkspaceID:       "rhizome-main",
		WorkspacePassword: "local-secret",
		AgentID:           "lyrica",
		DisplayName:       "Lyrica",
		AgentToken:        "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, "agent.out.log"), []byte("one\ntwo\nthree\n"), 0o644); err != nil {
		t.Fatalf("write stdout log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, "agent.err.log"), []byte("err-one\nerr-two\n"), 0o644); err != nil {
		t.Fatalf("write stderr log: %v", err)
	}

	var authHeader string
	var requestParams map[string]any
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		authHeader = r.Header.Get("Authorization")

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "agent.request":
			requestParams, _ = req["params"].(map[string]any)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-1",
					"workspace_id": "rhizome-main",
					"to_agent_id":  "lyrica",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-1",
					"workspace_id": "rhizome-main",
					"to_agent_id":  "lyrica",
					"status":       "COMPLETED",
					"response":     `{"status":"ok","answer":"hi"}`,
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []any{
						map[string]any{
							"task_id": "task-2",
							"status":  "PENDING",
							"title":   "Task Two",
						},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []any{
						map[string]any{
							"tension_id":   "tension-1",
							"tension_type": "BLOCKER",
							"summary":      "Need decision",
							"status":       "ACTIVE",
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer rpc.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:           rpc.URL,
		RPCEndpoint:       rpc.URL,
		WorkspaceID:       "workspace-bootstrap",
		WorkspacePassword: "local-secret",
		AgentID:           "lyrica-bootstrap",
		DisplayName:       "Lyrica Bootstrap",
		OwnerUserID:       "owner-bootstrap",
		Role:              "generalist",
		Capabilities:      []string{"tool.call", "local.shell"},
		AgentToken:        "local-token",
		RegisteredExecutor: RegisteredExecutorIdentity{
			AgentID:         "lyrica",
			WorkspaceID:     "rhizome-main",
			DisplayName:     "Lyrica Registered",
			OwnerUserID:     "owner-registered",
			Role:            "reviewer",
			ProtocolVersion: "rnar/v1",
			Capabilities:    []string{"tool.call"},
		},
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() override error: %v", err)
	}

	server := newManagerWebServer().routes()

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/agents/lyrica?lines=2", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("detail status = %d body=%s", rec.Code, rec.Body.String())
	}
	var detail managerWebAgentDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	if detail.Profile.Mission != "Ship useful work." {
		t.Fatalf("expected profile mission, got %+v", detail.Profile)
	}
	if got := strings.Join(detail.Logs.Stdout, ","); got != "two,three" {
		t.Fatalf("unexpected stdout tail: %+v", detail.Logs.Stdout)
	}
	if detail.LocalRuntime.WorkspacePassword != "" || detail.LocalRuntime.AgentToken != "" {
		t.Fatalf("expected redacted local runtime secrets, got %+v", detail.LocalRuntime)
	}
	if detail.LocalRuntime.DisplayName != "Lyrica Bootstrap" || detail.LocalRuntime.WorkspaceID != "workspace-bootstrap" {
		t.Fatalf("expected local runtime payload to preserve bootstrap identity fields, got %+v", detail.LocalRuntime)
	}
	if detail.EffectiveIdentity.Source != "registered_executor" {
		t.Fatalf("expected effective identity source to be registered_executor, got %+v", detail.EffectiveIdentity)
	}
	if detail.EffectiveIdentity.AgentID != "lyrica" || detail.EffectiveIdentity.WorkspaceID != "rhizome-main" {
		t.Fatalf("expected effective identity to use confirmed registration truth, got %+v", detail.EffectiveIdentity)
	}
	if detail.EffectiveIdentity.DisplayName != "Lyrica Registered" || detail.EffectiveIdentity.Role != "reviewer" {
		t.Fatalf("expected effective identity to carry confirmed display/role, got %+v", detail.EffectiveIdentity)
	}
	if len(detail.EffectiveIdentity.Capabilities) != 1 || detail.EffectiveIdentity.Capabilities[0] != "tool.call" {
		t.Fatalf("expected effective identity capabilities to stay confirmed, got %+v", detail.EffectiveIdentity)
	}
	if detail.LocalChatContract.ChannelMode != "manager_mediated_inspect" {
		t.Fatalf("expected detail to expose manager-mediated local chat contract, got %+v", detail.LocalChatContract)
	}
	if detail.LocalChatContract.ExecutionIdentity != "manager_process" || detail.LocalChatContract.ServiceIdentityMode != "shared_manager_process_identity" || detail.LocalChatContract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected detail to expose honest inspect-chat execution/runtime contract, got %+v", detail.LocalChatContract)
	}
	if detail.LocalChatContract.Availability != "unavailable" || detail.LocalChatContract.UnavailableReason != "isolated_local_auth_missing" {
		t.Fatalf("expected detail to expose unavailable inspect readiness for partner-managed record, got %+v", detail.LocalChatContract)
	}
	if detail.Defaults.DefaultParentDir != root {
		t.Fatalf("expected detail response to preserve current defaults, got %+v", detail.Defaults)
	}
	if len(detail.Agents) != 1 || detail.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected detail response to include refreshed agent overview, got %+v", detail.Agents)
	}
	if len(detail.Providers) != 0 {
		t.Fatalf("expected detail response to preserve current providers, got %+v", detail.Providers)
	}
	requireProviderCatalogIncludes(t, "expected detail response to include provider catalog", detail.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(detail.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected detail response to preserve create default parent dir, got %+v", detail.CreateDefault)
	}

	body := strings.NewReader(`{"method":"model.ask","payload":"what model are you running?"}`)
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/control", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("control status = %d body=%s", rec.Code, rec.Body.String())
	}
	if authHeader != "Bearer local-token" {
		t.Fatalf("expected local token auth header, got %q", authHeader)
	}
	var controlResp struct {
		Message         string                           `json:"message"`
		Result          managedAgentControlRequestResult `json:"result"`
		Live            managerLiveRuntimeStatus         `json:"live"`
		Catalog         managerWorkspaceCatalog          `json:"catalog"`
		Process         ManagedAgentProcessStatus        `json:"process"`
		Defaults        BotManagerDefaults               `json:"defaults"`
		Agents          []managerWebAgentRow             `json:"agents"`
		Providers       []ProviderRecord                 `json:"providers"`
		ProviderCatalog []SupportedProviderOption        `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault          `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &controlResp); err != nil {
		t.Fatalf("decode control response: %v", err)
	}
	payloadJSON, _ := requestParams["payload_json"].(string)
	if !strings.Contains(payloadJSON, "what model are you running?") {
		t.Fatalf("expected string payload in request params, got %+v", requestParams)
	}
	if controlResp.Result.RequestID != "req-1" || controlResp.Result.Status != "COMPLETED" {
		t.Fatalf("expected control response to preserve completed request context, got %+v", controlResp.Result)
	}
	if controlResp.Process.State != "stopped" || controlResp.Process.Running {
		t.Fatalf("expected control response to include current stopped process snapshot, got %+v", controlResp.Process)
	}
	if controlResp.Live.ProcessState != "stopped" || strings.TrimSpace(controlResp.Live.Error) != "" {
		t.Fatalf("expected control response to include refreshed stopped live status, got %+v", controlResp.Live)
	}
	if len(controlResp.Catalog.Tasks) != 1 || controlResp.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected control response to include refreshed task catalog, got %+v", controlResp.Catalog)
	}
	if len(controlResp.Catalog.Tensions) != 1 || controlResp.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected control response to include refreshed tension catalog, got %+v", controlResp.Catalog)
	}
	if controlResp.Defaults.DefaultParentDir != root {
		t.Fatalf("expected control response to preserve current defaults, got %+v", controlResp.Defaults)
	}
	if len(controlResp.Agents) != 1 || controlResp.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected control response to include refreshed agent overview, got %+v", controlResp.Agents)
	}
	if len(controlResp.Providers) != 0 {
		t.Fatalf("expected control response to preserve current providers, got %+v", controlResp.Providers)
	}
	requireProviderCatalogIncludes(t, "expected control response to include provider catalog", controlResp.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(controlResp.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected control response to preserve create default parent dir, got %+v", controlResp.CreateDefault)
	}

	body = strings.NewReader(`{"method":"runtime.delete_everything","payload":{}}`)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/control", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected unsupported control method to fail, got %d body=%s", rec.Code, rec.Body.String())
	}

	body = strings.NewReader(`{"llm_backend":"codex","model":"gpt-5.4-mini","planner_sec":"45","watchdog_sec":"90"}`)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/settings", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("settings status = %d body=%s", rec.Code, rec.Body.String())
	}
	var settingsResult struct {
		Message           string                      `json:"message"`
		LocalRuntime      LocalRuntimeProfile         `json:"local_runtime"`
		EffectiveIdentity managerWebEffectiveIdentity `json:"effective_identity"`
		Live              managerLiveRuntimeStatus    `json:"live"`
		Catalog           managerWorkspaceCatalog     `json:"catalog"`
		Process           ManagedAgentProcessStatus   `json:"process"`
		Defaults          BotManagerDefaults          `json:"defaults"`
		Agents            []managerWebAgentRow        `json:"agents"`
		Providers         []ProviderRecord            `json:"providers"`
		ProviderCatalog   []SupportedProviderOption   `json:"provider_catalog"`
		CreateDefault     managerWebCreateDefault     `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &settingsResult); err != nil {
		t.Fatalf("decode settings result: %v", err)
	}
	if !strings.Contains(settingsResult.Message, "restart the agent") {
		t.Fatalf("expected settings response to preserve confirmed identity contract, got %+v", settingsResult)
	}
	if settingsResult.LocalRuntime.Model != "gpt-5.4-mini" || settingsResult.LocalRuntime.PlannerSec != 45 || settingsResult.LocalRuntime.WatchdogSec != 90 {
		t.Fatalf("expected settings response to reflect bootstrap runtime edits, got %+v", settingsResult.LocalRuntime)
	}
	if settingsResult.EffectiveIdentity.DisplayName != "Lyrica Registered" || settingsResult.EffectiveIdentity.Role != "reviewer" {
		t.Fatalf("expected settings response to preserve confirmed effective identity, got %+v", settingsResult.EffectiveIdentity)
	}
	if settingsResult.Process.State != "stopped" || settingsResult.Process.Running {
		t.Fatalf("expected settings response to include current stopped process snapshot, got %+v", settingsResult.Process)
	}
	if settingsResult.Live.ProcessState != "stopped" || strings.TrimSpace(settingsResult.Live.Error) != "" {
		t.Fatalf("expected settings response to include refreshed stopped live status, got %+v", settingsResult.Live)
	}
	if len(settingsResult.Catalog.Tasks) != 1 || settingsResult.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected settings response to include refreshed task catalog, got %+v", settingsResult.Catalog)
	}
	if len(settingsResult.Catalog.Tensions) != 1 || settingsResult.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected settings response to include refreshed tension catalog, got %+v", settingsResult.Catalog)
	}
	if settingsResult.Defaults.DefaultParentDir != root {
		t.Fatalf("expected settings response to preserve current defaults, got %+v", settingsResult.Defaults)
	}
	if len(settingsResult.Agents) != 1 || settingsResult.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected settings response to include refreshed agent overview, got %+v", settingsResult.Agents)
	}
	if len(settingsResult.Providers) != 0 {
		t.Fatalf("expected settings response to preserve current providers, got %+v", settingsResult.Providers)
	}
	requireProviderCatalogIncludes(t, "expected settings response to include provider catalog", settingsResult.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(settingsResult.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected settings response to preserve create default parent dir, got %+v", settingsResult.CreateDefault)
	}
	runtimeProfile := LoadLocalRuntimeProfile(workdir)
	if runtimeProfile.Model != "gpt-5.4-mini" || runtimeProfile.PlannerSec != 45 || runtimeProfile.WatchdogSec != 90 {
		t.Fatalf("expected local runtime settings to persist, got %+v", runtimeProfile)
	}
	if runtimeProfile.RegisteredExecutor.DisplayName != "Lyrica Registered" {
		t.Fatalf("expected confirmed registered executor identity to survive settings save, got %+v", runtimeProfile.RegisteredExecutor)
	}

	body = strings.NewReader(`{"display_name":"Lyrica Prime"}`)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/edit", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("edit status = %d body=%s", rec.Code, rec.Body.String())
	}
	var editResult struct {
		Message           string                      `json:"message"`
		LocalRuntime      LocalRuntimeProfile         `json:"local_runtime"`
		EffectiveIdentity managerWebEffectiveIdentity `json:"effective_identity"`
		Live              managerLiveRuntimeStatus    `json:"live"`
		Catalog           managerWorkspaceCatalog     `json:"catalog"`
		Process           ManagedAgentProcessStatus   `json:"process"`
		Defaults          BotManagerDefaults          `json:"defaults"`
		Agents            []managerWebAgentRow        `json:"agents"`
		Providers         []ProviderRecord            `json:"providers"`
		ProviderCatalog   []SupportedProviderOption   `json:"provider_catalog"`
		CreateDefault     managerWebCreateDefault     `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &editResult); err != nil {
		t.Fatalf("decode edit result: %v", err)
	}
	if !strings.Contains(editResult.Message, "confirmed executor identity remains unchanged") {
		t.Fatalf("expected explicit registered-vs-bootstrap edit message, got %+v", editResult)
	}
	if editResult.LocalRuntime.DisplayName != "Lyrica Prime" {
		t.Fatalf("expected edit response to show updated bootstrap display name, got %+v", editResult.LocalRuntime)
	}
	if editResult.EffectiveIdentity.DisplayName != "Lyrica Registered" {
		t.Fatalf("expected edit response to preserve confirmed effective identity, got %+v", editResult.EffectiveIdentity)
	}
	if editResult.Process.State != "stopped" || editResult.Process.Running {
		t.Fatalf("expected edit response to include current stopped process snapshot, got %+v", editResult.Process)
	}
	if editResult.Live.ProcessState != "stopped" || strings.TrimSpace(editResult.Live.Error) != "" {
		t.Fatalf("expected edit response to include refreshed stopped live status, got %+v", editResult.Live)
	}
	if len(editResult.Catalog.Tasks) != 1 || editResult.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected edit response to include refreshed task catalog, got %+v", editResult.Catalog)
	}
	if len(editResult.Catalog.Tensions) != 1 || editResult.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected edit response to include refreshed tension catalog, got %+v", editResult.Catalog)
	}
	if editResult.Defaults.DefaultParentDir != root {
		t.Fatalf("expected edit response to preserve current defaults, got %+v", editResult.Defaults)
	}
	if len(editResult.Agents) != 1 || editResult.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected edit response to include refreshed agent overview, got %+v", editResult.Agents)
	}
	if len(editResult.Providers) != 0 {
		t.Fatalf("expected edit response to preserve current providers, got %+v", editResult.Providers)
	}
	requireProviderCatalogIncludes(t, "expected edit response to include provider catalog", editResult.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(editResult.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected edit response to preserve create default parent dir, got %+v", editResult.CreateDefault)
	}

	runtimeProfile = LoadLocalRuntimeProfile(workdir)
	if runtimeProfile.DisplayName != "Lyrica Prime" {
		t.Fatalf("expected local runtime profile display name to update, got %+v", runtimeProfile)
	}
	if runtimeProfile.RegisteredExecutor.DisplayName != "Lyrica Registered" {
		t.Fatalf("expected confirmed registered executor identity to survive local edit, got %+v", runtimeProfile.RegisteredExecutor)
	}
	profile := LoadAgentProfile(workdir)
	if profile.DisplayName != "Lyrica Prime" {
		t.Fatalf("expected agent profile display name to update, got %+v", profile)
	}

	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/agents/lyrica?lines=2", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("detail-after-edit status = %d body=%s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode detail-after-edit: %v", err)
	}
	if detail.LocalRuntime.DisplayName != "Lyrica Prime" {
		t.Fatalf("expected bootstrap display name edit to remain visible in local runtime payload, got %+v", detail.LocalRuntime)
	}
	if detail.EffectiveIdentity.DisplayName != "Lyrica Registered" {
		t.Fatalf("expected effective identity to remain confirmed until fresh registration, got %+v", detail.EffectiveIdentity)
	}

	body = strings.NewReader(`{"remove":true}`)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/edit", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("remove status = %d body=%s", rec.Code, rec.Body.String())
	}
	var removeResult struct {
		Removed         bool                      `json:"removed"`
		Message         string                    `json:"message"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &removeResult); err != nil {
		t.Fatalf("decode remove result: %v", err)
	}
	if !removeResult.Removed || !strings.Contains(removeResult.Message, "removed lyrica from registry") {
		t.Fatalf("expected remove response to confirm registry removal, got %+v", removeResult)
	}
	if removeResult.Defaults.DefaultParentDir != root {
		t.Fatalf("expected remove response to preserve current defaults, got %+v", removeResult.Defaults)
	}
	if len(removeResult.Agents) != 0 {
		t.Fatalf("expected remove response to include refreshed empty agent overview, got %+v", removeResult.Agents)
	}
	if len(removeResult.Providers) != 0 {
		t.Fatalf("expected remove response to preserve current providers, got %+v", removeResult.Providers)
	}
	requireProviderCatalogIncludes(t, "expected remove response to include provider catalog", removeResult.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(removeResult.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected remove response to preserve create default parent dir, got %+v", removeResult.CreateDefault)
	}
	if _, err := ResolveManagedAgentReference("lyrica"); err == nil {
		t.Fatal("expected removed agent to disappear from registry")
	}
}

func TestManagerWebAgentDetailSurfacesLogTailError(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := os.Mkdir(filepath.Join(workdir, "agent.out.log"), 0o755); err != nil {
		t.Fatalf("mkdir fake stdout log dir: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/agents/lyrica?lines=2", nil)
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("detail-with-log-error status = %d body=%s", rec.Code, rec.Body.String())
	}

	var detail managerWebAgentDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode detail-with-log-error: %v", err)
	}
	if !strings.Contains(strings.ToLower(detail.Logs.Error), "agent.out.log") {
		t.Fatalf("expected detail logs error to surface failing path, got %+v", detail.Logs)
	}
}

func TestApplyWebOnboardRequestAppliesOverrides(t *testing.T) {
	state := onboardState{
		Runtime: RuntimeConfig{
			Workdir:           t.TempDir(),
			AgentID:           "agent-01",
			DisplayName:       "Agent 01",
			RhizomeHost:       "https://example.test",
			WorkspaceID:       "ws-1",
			WorkspacePassword: "pw-1",
			Role:              "generalist",
			LLMBackend:        llmBackendAuto,
			Model:             defaultModel,
		},
		AgentProfile: AgentProfile{
			AgentID:               "agent-01",
			DisplayName:           "Agent 01",
			Role:                  "generalist",
			PrimarySpecialization: "generalist",
		},
	}

	applyWebOnboardRequest(&state, managerWebOnboardRequest{
		AgentID:                  "lyrica",
		DisplayName:              "Lyrica",
		OwnerUserID:              "developer",
		HostURL:                  "https://rhizome.example.test",
		WorkspaceID:              "rhizome-main",
		WorkspacePassword:        "test-workspace-password",
		Role:                     "reviewer",
		PrimarySpecialization:    "reviewer",
		SecondarySpecializations: "repair, qa",
		DomainScope:              "tasks, docs",
		Mission:                  "Keep the workspace clean.",
		LLMBackend:               llmBackendCodex,
		Model:                    "gpt-5.4",
	})

	if state.Runtime.AgentID != "lyrica" || state.Runtime.DisplayName != "Lyrica" {
		t.Fatalf("runtime identity override failed: %+v", state.Runtime)
	}
	if state.Runtime.RhizomeHost != "https://rhizome.example.test" || state.Runtime.RhizomeRPC != "https://rhizome.example.test/rpc" {
		t.Fatalf("runtime host override failed: %+v", state.Runtime)
	}
	if state.AgentProfile.PrimarySpecialization != "reviewer" {
		t.Fatalf("profile specialization override failed: %+v", state.AgentProfile)
	}
	if got := strings.Join(state.AgentProfile.SecondarySpecializations, ","); got != "repair,qa" {
		t.Fatalf("profile secondary specializations override failed: %+v", state.AgentProfile.SecondarySpecializations)
	}
	if got := strings.Join(state.AgentProfile.DomainScope, ","); got != "tasks,docs" {
		t.Fatalf("profile domain scope override failed: %+v", state.AgentProfile.DomainScope)
	}
}

func TestManagerWebAgentEditRunningReturnsCurrentStatusAndCatalog(t *testing.T) {
	setManagerWebTestHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "agent.request":
			params, _ := req["params"].(map[string]any)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-" + fmt.Sprint(params["method"]),
					"workspace_id": "ws-1",
					"to_agent_id":  "lyrica",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-runtime.status",
					"workspace_id": "ws-1",
					"to_agent_id":  "lyrica",
					"status":       "COMPLETED",
					"response": mustJSON(t, map[string]any{
						"status":     "ok",
						"summary":    "steady",
						"paused":     false,
						"attachable": true,
						"task_id":    "task-2",
						"session_id": "session-2",
					}),
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "RUNNING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "manager",
		AgentToken:  "local-token",
		DisplayName: "Lyrica",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
	saveTrustedAgentProcessStateForTest(t, record, 4321)

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
	}()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 4321, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/lyrica/edit", strings.NewReader(`{"display_name":"Lyrica Prime"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected blocked edit status 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK      bool                      `json:"ok"`
		Error   string                    `json:"error"`
		Live    managerLiveRuntimeStatus  `json:"live"`
		Catalog managerWorkspaceCatalog   `json:"catalog"`
		Process ManagedAgentProcessStatus `json:"process"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode blocked edit response: %v", err)
	}
	if resp.OK {
		t.Fatalf("expected blocked edit response, got %+v", resp)
	}
	if !strings.Contains(resp.Error, "stop lyrica before editing registry fields") {
		t.Fatalf("expected explicit running-edit guard error, got %+v", resp)
	}
	if resp.Process.State != "running" || !resp.Process.Running || resp.Process.PID != 4321 {
		t.Fatalf("expected current process snapshot in blocked edit response, got %+v", resp.Process)
	}
	if resp.Live.ProcessState != "running" || resp.Live.ActiveTaskID != "task-2" {
		t.Fatalf("expected refreshed live status in blocked edit response, got %+v", resp.Live)
	}
	if len(resp.Catalog.Tasks) != 1 || resp.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed task catalog in blocked edit response, got %+v", resp.Catalog)
	}
	if len(resp.Catalog.Tensions) != 1 || resp.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed tension catalog in blocked edit response, got %+v", resp.Catalog)
	}
	if got := LoadLocalRuntimeProfile(workdir).DisplayName; got != "Lyrica" {
		t.Fatalf("expected blocked edit to preserve local runtime display name, got %q", got)
	}
}

func TestManagerWebOnboardReturnsConfirmedEffectiveIdentity(t *testing.T) {
	setManagerWebTestHome(t)
	t.Setenv("RHIZOME_COORDINATION_MODE", CoordinationModeStrict)

	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/agent/register":
			defer r.Body.Close()
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode auth register request: %v", err)
			}
			if req["agent_id"] != "lyrica" {
				t.Fatalf("expected onboard register request for lyrica, got %+v", req)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"agent_id":       "lyrica",
				"display_name":   "Lyrica Registered",
				"workspace_id":   "rhizome-main",
				"workspace_name": "Rhizome Main",
				"access_token":   "token-registered",
				"agent": map[string]any{
					"agent_id":         "lyrica",
					"display_name":     "Lyrica Registered",
					"workspace_id":     "rhizome-main",
					"owner_user_id":    "owner-registered",
					"role":             "reviewer",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"status":           "REGISTERED",
				},
			})
		case "/rpc":
			defer r.Body.Close()
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode rpc request: %v", err)
			}
			method, _ := req["method"].(string)
			switch method {
			case "workspace.tasks.list":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req["id"],
					"result": map[string]any{
						"tasks": []map[string]any{
							{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
						},
					},
				})
			case "workspace.tension.frontier":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req["id"],
					"result": map[string]any{
						"items": []map[string]any{
							{"tension_id": "tension-1", "tension_type": "BLOCKER", "summary": "Need decision", "status": "ACTIVE"},
						},
					},
				})
			default:
				t.Fatalf("unexpected rpc method %q", method)
			}
		default:
			t.Fatalf("unexpected auth path %q", r.URL.Path)
		}
	}))
	defer auth.Close()

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	if err := SetManagerDefault("coordination_mode", CoordinationModeTrustFirst); err != nil {
		t.Fatalf("SetManagerDefault(coordination_mode) error: %v", err)
	}
	workdir := filepath.Join(root, "lyrica")
	server := newManagerWebServer().routes()
	body := strings.NewReader(`{
		"workdir":"` + strings.ReplaceAll(workdir, `\`, `\\`) + `",
		"agent_id":"lyrica",
		"display_name":"Lyrica Bootstrap",
		"owner_user_id":"owner-bootstrap",
		"host_url":"` + auth.URL + `",
		"workspace_id":"rhizome-main",
		"workspace_password":"secret-pw",
		"role":"generalist",
		"primary_specialization":"generalist",
		"llm_backend":"codex",
		"model":"gpt-5.4"
	}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/onboard", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("onboard status = %d body=%s", rec.Code, rec.Body.String())
	}

	var result struct {
		Message           string                      `json:"message"`
		Record            ManagedAgentRecord          `json:"record"`
		LocalRuntime      LocalRuntimeProfile         `json:"local_runtime"`
		EffectiveIdentity managerWebEffectiveIdentity `json:"effective_identity"`
		Live              managerLiveRuntimeStatus    `json:"live"`
		Catalog           managerWorkspaceCatalog     `json:"catalog"`
		Process           ManagedAgentProcessStatus   `json:"process"`
		Defaults          BotManagerDefaults          `json:"defaults"`
		Agents            []managerWebAgentRow        `json:"agents"`
		Providers         []ProviderRecord            `json:"providers"`
		ProviderCatalog   []SupportedProviderOption   `json:"provider_catalog"`
		CreateDefault     managerWebCreateDefault     `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode onboard result: %v", err)
	}
	if !strings.Contains(result.Message, "confirmed executor identity is now current") {
		t.Fatalf("expected onboard response to explicitly confirm current executor identity, got %+v", result)
	}
	if result.Record.AgentID != "lyrica" {
		t.Fatalf("expected onboard record to use canonical agent id, got %+v", result.Record)
	}
	if result.Record.CoordinationMode != CoordinationModeTrustFirst || result.LocalRuntime.CoordinationMode != CoordinationModeTrustFirst {
		t.Fatalf("expected onboard response to preserve manager coordination mode despite ambient env, got record=%q local=%q", result.Record.CoordinationMode, result.LocalRuntime.CoordinationMode)
	}
	if result.LocalRuntime.AgentToken != "" || result.LocalRuntime.WorkspacePassword != "" {
		t.Fatalf("expected onboard local runtime secrets to be redacted, got %+v", result.LocalRuntime)
	}
	if result.EffectiveIdentity.Source != "registered_executor" {
		t.Fatalf("expected onboard effective identity source to be registered_executor, got %+v", result.EffectiveIdentity)
	}
	if result.EffectiveIdentity.DisplayName != "Lyrica Registered" || result.EffectiveIdentity.Role != "reviewer" {
		t.Fatalf("expected onboard response to surface confirmed executor identity, got %+v", result.EffectiveIdentity)
	}
	if result.Process.State != "stopped" || result.Process.Running {
		t.Fatalf("expected onboard response to include current stopped process snapshot, got %+v", result.Process)
	}
	if result.Live.ProcessState != "stopped" || strings.TrimSpace(result.Live.Error) != "" {
		t.Fatalf("expected onboard response to include refreshed stopped live status, got %+v", result.Live)
	}
	if len(result.Catalog.Tasks) != 1 || result.Catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected onboard response to include refreshed task catalog, got %+v", result.Catalog)
	}
	if len(result.Catalog.Tensions) != 1 || result.Catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected onboard response to include refreshed tension catalog, got %+v", result.Catalog)
	}
	if result.Defaults.DefaultParentDir != root {
		t.Fatalf("expected onboard response to preserve current defaults, got %+v", result.Defaults)
	}
	if len(result.Agents) != 1 || result.Agents[0].Record.AgentID != "lyrica" {
		t.Fatalf("expected onboard response to include refreshed agent overview, got %+v", result.Agents)
	}
	if len(result.Providers) != 0 {
		t.Fatalf("expected onboard response to preserve current providers, got %+v", result.Providers)
	}
	requireProviderCatalogIncludes(t, "expected onboard response to include provider catalog", result.ProviderCatalog, "codex_bridge")
	if !strings.EqualFold(filepath.Clean(result.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected onboard response to preserve create default parent dir, got %+v", result.CreateDefault)
	}

	runtimeProfile := LoadLocalRuntimeProfile(workdir)
	if runtimeProfile.DisplayName != "Lyrica Bootstrap" {
		t.Fatalf("expected local runtime bootstrap display to preserve requested onboarding display, got %+v", runtimeProfile)
	}
	if runtimeProfile.CoordinationMode != CoordinationModeTrustFirst {
		t.Fatalf("expected local runtime to persist manager coordination mode, got %+v", runtimeProfile)
	}
	if runtimeProfile.RegisteredExecutor.DisplayName != "Lyrica Registered" || runtimeProfile.RegisteredExecutor.Role != "reviewer" {
		t.Fatalf("expected onboard persistence to store confirmed executor identity, got %+v", runtimeProfile.RegisteredExecutor)
	}
}

func TestManagerWebOnboardAppliesProviderBinding(t *testing.T) {
	setManagerWebTestHome(t)

	if err := UpsertProviderRecord(ProviderRecord{
		ProviderID:   "codex-bridge",
		ChannelType:  providerChannelBridge,
		Driver:       llmBackendCodex,
		GroupID:      "group-codex",
		DefaultModel: "gpt-5.4",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("UpsertProviderRecord() error: %v", err)
	}

	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/agent/register":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"agent_id":       "lyrica",
				"display_name":   "Lyrica Registered",
				"workspace_id":   "rhizome-main",
				"workspace_name": "Rhizome Main",
				"access_token":   "token-registered",
				"agent": map[string]any{
					"agent_id":         "lyrica",
					"display_name":     "Lyrica Registered",
					"workspace_id":     "rhizome-main",
					"owner_user_id":    "owner-registered",
					"role":             "reviewer",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"status":           "REGISTERED",
				},
			})
		case "/rpc":
			defer r.Body.Close()
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode rpc request: %v", err)
			}
			method, _ := req["method"].(string)
			switch method {
			case "workspace.tasks.list":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req["id"],
					"result":  map[string]any{"tasks": []any{}},
				})
			case "workspace.tension.frontier":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req["id"],
					"result":  map[string]any{"items": []any{}},
				})
			default:
				t.Fatalf("unexpected rpc method %q", method)
			}
		default:
			t.Fatalf("unexpected auth path %q", r.URL.Path)
		}
	}))
	defer auth.Close()

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir := filepath.Join(root, "lyrica")
	server := newManagerWebServer().routes()
	body := strings.NewReader(`{
		"workdir":"` + strings.ReplaceAll(workdir, `\`, `\\`) + `",
		"agent_id":"lyrica",
		"display_name":"Lyrica Bootstrap",
		"owner_user_id":"owner-bootstrap",
		"host_url":"` + auth.URL + `",
		"workspace_id":"rhizome-main",
		"workspace_password":"secret-pw",
		"role":"generalist",
		"primary_specialization":"generalist",
		"provider_id":"codex-bridge",
		"group_id":"wrong-group",
		"llm_backend":"openai",
		"model":"gpt-5.4-mini"
	}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/onboard", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("onboard status = %d body=%s", rec.Code, rec.Body.String())
	}

	var result struct {
		Record       ManagedAgentRecord  `json:"record"`
		LocalRuntime LocalRuntimeProfile `json:"local_runtime"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode onboard result: %v", err)
	}
	if result.Record.ProviderID != "codex-bridge" || result.Record.GroupID != "group-codex" {
		t.Fatalf("expected onboard record to derive provider binding, got %+v", result.Record)
	}
	if result.Record.LLMBackend != llmBackendCodex || result.Record.Model != "gpt-5.4-mini" {
		t.Fatalf("expected onboard record runtime fields to derive from provider binding, got %+v", result.Record)
	}
	if result.LocalRuntime.ProviderID != "codex-bridge" || result.LocalRuntime.GroupID != "group-codex" {
		t.Fatalf("expected onboard local runtime to derive provider binding, got %+v", result.LocalRuntime)
	}
}

func TestManagerWebOnboardRejectsExplicitWorkdirOutsideManagedRoot(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "codex-bridge",
			Title:       "Codex Bridge",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			GroupID:     "group-codex",
			Enabled:     true,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}
	existingWorkdir := filepath.Join(root, "existing-agent")
	if err := os.MkdirAll(existingWorkdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(existingWorkdir) error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "existing",
		DisplayName: "Existing Agent",
		Workdir:     existingWorkdir,
		HostURL:     "https://rhizome.example",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       "gpt-5.4",
	}); err != nil {
		t.Fatalf("UpsertManagedAgent(existing) error: %v", err)
	}

	outside := filepath.Join(t.TempDir(), "outside-agent")
	server := newManagerWebServer().routes()
	body := strings.NewReader(`{
		"workdir":"` + strings.ReplaceAll(outside, `\`, `\\`) + `",
		"agent_id":"lyrica",
		"display_name":"Lyrica Bootstrap",
		"owner_user_id":"owner-bootstrap",
		"host_url":"https://rhizome.example",
		"workspace_id":"rhizome-main",
		"workspace_password":"secret-pw",
		"role":"generalist",
		"llm_backend":"codex",
		"model":"gpt-5.4"
	}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/onboard", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected outside workdir rejection, got %d body=%s", rec.Code, rec.Body.String())
	}
	var response managerWebOverviewErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode onboard reject response: %v", err)
	}
	if !strings.Contains(response.Error, "managed root") {
		t.Fatalf("expected managed root error, got %+v", response)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected onboard reject response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "existing" {
		t.Fatalf("expected onboard reject response to preserve current agent registry, got %+v", response.Agents)
	}
	if len(response.Providers) != 1 || response.Providers[0].ProviderID != "codex-bridge" {
		t.Fatalf("expected onboard reject response to preserve providers, got %+v", response.Providers)
	}
	if len(response.ProviderCatalog) == 0 {
		t.Fatalf("expected onboard reject response to preserve provider catalog, got %+v", response.ProviderCatalog)
	}
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected onboard reject response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
}

func TestManagerWebOnboardCreateWorkdirFailureReturnsOverviewContext(t *testing.T) {
	setManagerWebTestHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	if err := SaveProviderRegistry(ProviderRegistry{
		Providers: []ProviderRecord{{
			ProviderID:  "codex-bridge",
			Title:       "Codex Bridge",
			ChannelType: providerChannelBridge,
			Driver:      llmBackendCodex,
			GroupID:     "group-codex",
			Enabled:     true,
			CreatedAt:   "2026-04-12T00:00:00Z",
		}},
	}); err != nil {
		t.Fatalf("SaveProviderRegistry() error: %v", err)
	}
	existingWorkdir := filepath.Join(root, "existing-agent")
	if err := os.MkdirAll(existingWorkdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(existingWorkdir) error: %v", err)
	}
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "existing",
		DisplayName: "Existing Agent",
		Workdir:     existingWorkdir,
		HostURL:     "https://rhizome.example",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       "gpt-5.4",
	}); err != nil {
		t.Fatalf("UpsertManagedAgent(existing) error: %v", err)
	}

	blocked := filepath.Join(root, "blocked-agent")
	if err := os.WriteFile(blocked, []byte("not-a-directory"), 0o600); err != nil {
		t.Fatalf("WriteFile(blocked) error: %v", err)
	}

	server := newManagerWebServer().routes()
	body := strings.NewReader(`{
		"workdir":"` + strings.ReplaceAll(blocked, `\`, `\\`) + `",
		"agent_id":"lyrica",
		"display_name":"Lyrica Bootstrap",
		"owner_user_id":"owner-bootstrap",
		"host_url":"https://rhizome.example",
		"workspace_id":"rhizome-main",
		"workspace_password":"secret-pw",
		"role":"generalist",
		"llm_backend":"codex",
		"model":"gpt-5.4"
	}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/onboard", body)
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected create-workdir failure, got %d body=%s", rec.Code, rec.Body.String())
	}
	var response managerWebOverviewErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode onboard create-workdir failure response: %v", err)
	}
	if !strings.Contains(response.Error, "create workdir") {
		t.Fatalf("expected create workdir error, got %+v", response)
	}
	if response.Defaults.DefaultParentDir != root {
		t.Fatalf("expected create-workdir failure response to preserve current defaults, got %+v", response.Defaults)
	}
	if len(response.Agents) != 1 || response.Agents[0].Record.AgentID != "existing" {
		t.Fatalf("expected create-workdir failure response to preserve current agent registry, got %+v", response.Agents)
	}
	if len(response.Providers) != 1 || response.Providers[0].ProviderID != "codex-bridge" {
		t.Fatalf("expected create-workdir failure response to preserve providers, got %+v", response.Providers)
	}
	if len(response.ProviderCatalog) == 0 {
		t.Fatalf("expected create-workdir failure response to preserve provider catalog, got %+v", response.ProviderCatalog)
	}
	if !strings.EqualFold(filepath.Clean(response.CreateDefault.ParentDir), filepath.Clean(root)) {
		t.Fatalf("expected create-workdir failure response to preserve create default parent dir, got %+v", response.CreateDefault)
	}
}

func TestRunWebRejectsRemoteHostUnconditionally(t *testing.T) {
	err := runWeb([]string{"--host", "0.0.0.0", "--port", "0", "--no-open"})
	if err == nil || !strings.Contains(err.Error(), "local-only") {
		t.Fatalf("expected remote host rejection, got %v", err)
	}
	if err := runWeb([]string{"--host", "0.0.0.0", "--port", "0", "--no-open", "--allow-remote"}); err == nil {
		t.Fatal("deprecated --allow-remote unexpectedly enabled a remote manager dashboard")
	}
}

func TestManagerWebListenAddressSupportsIPv6Loopback(t *testing.T) {
	for _, host := range []string{"::1", "[::1]"} {
		if got := managerWebListenAddress(host, 8420); got != "[::1]:8420" {
			t.Errorf("manager web address for %q = %q, want [::1]:8420", host, got)
		}
	}
}

func TestManagerWebRoutesSetSecurityHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	newManagerWebServer().routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected dashboard status 200, got %d", rec.Code)
	}
	for name, want := range map[string]string{
		"Content-Security-Policy": "default-src 'none'",
		"Permissions-Policy":      "camera=()",
		"Referrer-Policy":         "no-referrer",
		"X-Content-Type-Options":  "nosniff",
		"X-Frame-Options":         "DENY",
	} {
		if got := rec.Header().Get(name); !strings.Contains(got, want) {
			t.Errorf("%s = %q, want it to contain %q", name, got, want)
		}
	}
	if html := rec.Body.String(); strings.Contains(html, "fonts.googleapis.com") || strings.Contains(html, "fonts.gstatic.com") {
		t.Fatal("manager dashboard must not load remote font resources")
	}
}

func TestManagerWebDashboardHasNoDynamicInlineEventHandlers(t *testing.T) {
	page := managerWebDashboardHTML()
	inlineEvent := regexp.MustCompile(`(?i)\bon[a-z]+\s*=\s*"([^"]*)"`)
	for _, match := range inlineEvent.FindAllStringSubmatch(page, -1) {
		body := match[1]
		if strings.Contains(body, "'+") || strings.Contains(body, "+'") || strings.Contains(body, "${") {
			t.Fatalf("dynamic data is still concatenated into a manager inline event handler: %s", match[0])
		}
	}
	if !strings.Contains(page, "data-dashboard-action") || !strings.Contains(page, "dashboardActions") {
		t.Fatal("manager dashboard is missing delegated dynamic-action binding")
	}
}

func TestManagerWebDashboardInlineScriptParses(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skipf("node executable not available: %v", err)
	}
	page := managerWebDashboardHTML()
	start := strings.Index(page, "<script>")
	if start < 0 {
		t.Fatal("manager dashboard inline script opening tag not found")
	}
	start += len("<script>")
	end := strings.Index(page[start:], "</script>")
	if end < 0 {
		t.Fatal("manager dashboard inline script closing tag not found")
	}
	scriptPath := filepath.Join(t.TempDir(), "manager-dashboard-inline.js")
	if err := os.WriteFile(scriptPath, []byte(page[start:start+end]), 0o600); err != nil {
		t.Fatalf("write manager dashboard inline script: %v", err)
	}
	if output, err := exec.Command("node", "--check", scriptPath).CombinedOutput(); err != nil {
		t.Fatalf("manager dashboard inline script has invalid syntax: %v\n%s", err, strings.TrimSpace(string(output)))
	}
}

func TestManagerWebDashboardIncludesConfigurableLegalSourceNotice(t *testing.T) {
	t.Setenv("RHIZOME_SOURCE_URL", "https://example.invalid/source/revision-abc?one=1&two=2")
	rec := httptest.NewRecorder()
	newManagerWebServer().routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	page := rec.Body.String()
	for _, want := range []string{
		"Legal &amp; source",
		"Rhizome Project contributors",
		"No warranty",
		"AGPL-3.0-only",
		"https://example.invalid/source/revision-abc?one=1&amp;two=2",
		"Corresponding source for this build",
	} {
		if !strings.Contains(page, want) {
			t.Fatalf("manager dashboard legal notice missing %q", want)
		}
	}
}

func TestManagerWebDashboardRejectsUnsafeLegalSourceURL(t *testing.T) {
	t.Setenv("RHIZOME_SOURCE_URL", "javascript:alert(1)")
	rec := httptest.NewRecorder()
	newManagerWebServer().routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if !strings.Contains(rec.Body.String(), `href="`+defaultManagerWebSourceURL+`"`) {
		t.Fatal("manager dashboard did not fall back to the canonical source URL")
	}
}

func TestManagerWebLocalChatUsesManagerOwnedStorageForPartnerManagedAgent(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "partner-agent",
		DisplayName: "Partner Agent",
		Workdir:     workdir,
		OwnerUserID: "partner-owner",
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendAuto,
		Model:       defaultModel,
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	configRoot := managedAgentConfigRootPath(workdir)
	if err := os.MkdirAll(configRoot, 0o700); err != nil {
		t.Fatalf("MkdirAll(configRoot) error: %v", err)
	}
	if err := os.WriteFile(keyPathForRoot(configRoot), []byte("partner-openai-key"), 0o600); err != nil {
		t.Fatalf("WriteFile(openai_key) error: %v", err)
	}

	server := newManagerWebServer().routes()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected create local chat to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Contract        LocalChatContract         `json:"contract"`
		Session         LocalChatSession          `json:"session"`
		Sessions        []LocalChatSession        `json:"sessions"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Command         string                    `json:"command"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create local chat response: %v", err)
	}
	if created.Contract.ChannelMode != "manager_mediated_inspect" || created.Contract.ExecutionIdentity != "manager_process" || created.Contract.ServiceIdentityMode != "shared_manager_process_identity" {
		t.Fatalf("expected create local chat response to carry inspect-chat contract, got %+v", created.Contract)
	}
	if created.Contract.RuntimeRelation != "not_live_managed_runtime" || created.Contract.TranscriptScope != "manager_owned" {
		t.Fatalf("expected create local chat response to carry honest runtime/transcript contract, got %+v", created.Contract)
	}
	if created.Contract.Availability != "available" || created.Contract.AuthBackend != llmBackendOpenAI {
		t.Fatalf("expected create local chat response to carry available inspect readiness, got %+v", created.Contract)
	}
	if created.Contract.ShellAllowed || created.Contract.MutationAllowed {
		t.Fatalf("expected partner-managed local chat contract to stay bounded, got %+v", created.Contract)
	}
	if len(created.Sessions) != 1 || created.Sessions[0].ChatID != created.Session.ChatID {
		t.Fatalf("expected create local chat response to include refreshed session inventory, got %+v", created.Sessions)
	}
	if created.Process.Running {
		t.Fatalf("expected create local chat response to preserve current stopped process state, got %+v", created.Process)
	}
	if created.Live.Error == "" && created.Live.Status == "" && created.Live.ProcessState == "" {
		t.Fatalf("expected create local chat response to preserve current live runtime snapshot, got %+v", created.Live)
	}
	if created.Catalog.Error == "" && len(created.Catalog.Tasks) == 0 && len(created.Catalog.Tensions) == 0 {
		t.Fatalf("expected create local chat response to preserve current catalog snapshot, got %+v", created.Catalog)
	}
	assertCurrentOverview := func(name, command string, defaults BotManagerDefaults, agents []managerWebAgentRow, providers []ProviderRecord, providerCatalog []SupportedProviderOption, createDefault managerWebCreateDefault) {
		t.Helper()
		if command != appCommandName {
			t.Fatalf("expected %s to preserve command name, got %q", name, command)
		}
		if len(agents) != 1 || agents[0].Record.AgentID != record.AgentID {
			t.Fatalf("expected %s to preserve current agent overview, got %+v", name, agents)
		}
		if len(providers) != 0 {
			t.Fatalf("expected %s to preserve empty provider inventory, got %+v", name, providers)
		}
		if len(providerCatalog) == 0 {
			t.Fatalf("expected %s to preserve provider catalog, got %+v", name, providerCatalog)
		}
		if strings.TrimSpace(createDefault.ParentDir) == "" || strings.TrimSpace(createDefault.FolderName) == "" || strings.TrimSpace(createDefault.Workdir) == "" {
			t.Fatalf("expected %s to preserve create-default context, got %+v", name, createDefault)
		}
		_ = defaults
	}
	assertCurrentOverview("create local chat response", created.Command, created.Defaults, created.Agents, created.Providers, created.ProviderCatalog, created.CreateDefault)
	if _, err := os.Stat(getLocalChatsDir(workdir)); !os.IsNotExist(err) {
		t.Fatalf("expected manager-owned local chat create to avoid workdir local_chats dir, got err=%v", err)
	}
	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(managerDir, created.Session.ChatID+".json")); err != nil {
		t.Fatalf("expected manager-owned local chat transcript to exist, got %v", err)
	}
	stored, err := getLocalChat(managerDir, created.Session.ChatID)
	if err != nil {
		t.Fatalf("getLocalChat(stored create) error: %v", err)
	}
	if stored.Contract.ChannelMode != "manager_mediated_inspect" || stored.Contract.ExecutionIdentity != "manager_process" || stored.Contract.ServiceIdentityMode != "shared_manager_process_identity" {
		t.Fatalf("expected stored transcript to persist inspect contract, got %+v", stored.Contract)
	}
	if stored.OwnerUserID != "partner-owner" || stored.AgentID != "partner-agent" || stored.WorkspaceID != "rhizome-main" {
		t.Fatalf("expected stored transcript to persist owner/agent/workspace snapshot, got %+v", stored)
	}

	replySession := &LocalChatSession{
		ChatID: "reply-provenance",
		Title:  "Reply Provenance",
		Messages: []LocalChatMessage{
			{Role: "user", Content: "hello"},
			{Role: "agent", Content: "inspect reply"},
		},
	}
	if err := saveLocalChatForRecord(record, managerDir, replySession); err != nil {
		t.Fatalf("saveLocalChatForRecord(reply provenance) error: %v", err)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/agents/partner-agent/local_chats", nil)
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected list local chats to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var listed struct {
		Contract        LocalChatContract         `json:"contract"`
		Sessions        []LocalChatSession        `json:"sessions"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Command         string                    `json:"command"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode local chat list: %v", err)
	}
	if listed.Contract.ChannelMode != "manager_mediated_inspect" || listed.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected list local chats response to carry inspect contract, got %+v", listed.Contract)
	}
	if listed.Contract.Availability != "available" || listed.Contract.AuthBackend != llmBackendOpenAI {
		t.Fatalf("expected list local chats response to carry available inspect readiness, got %+v", listed.Contract)
	}
	if len(listed.Sessions) != 2 {
		t.Fatalf("expected manager-owned local chats to appear in list, got %+v", listed)
	}
	var foundCreated bool
	for _, session := range listed.Sessions {
		if session.ChatID == created.Session.ChatID {
			foundCreated = true
			break
		}
	}
	if !foundCreated {
		t.Fatalf("expected created manager-owned local chat to appear in list, got %+v", listed)
	}
	if listed.Process.Running {
		t.Fatalf("expected local chat list to preserve current stopped process state, got %+v", listed.Process)
	}
	if listed.Live.Error == "" && listed.Live.Status == "" && listed.Live.ProcessState == "" {
		t.Fatalf("expected local chat list to preserve current live runtime snapshot, got %+v", listed.Live)
	}
	if listed.Catalog.Error == "" && len(listed.Catalog.Tasks) == 0 && len(listed.Catalog.Tensions) == 0 {
		t.Fatalf("expected local chat list to preserve current catalog snapshot, got %+v", listed.Catalog)
	}
	assertCurrentOverview("local chat list", listed.Command, listed.Defaults, listed.Agents, listed.Providers, listed.ProviderCatalog, listed.CreateDefault)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/agents/partner-agent/local_chats/reply-provenance", nil)
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected inspect chat get to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var fetched struct {
		Contract        LocalChatContract         `json:"contract"`
		Session         LocalChatSession          `json:"session"`
		Sessions        []LocalChatSession        `json:"sessions"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Command         string                    `json:"command"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("decode inspect chat get response: %v", err)
	}
	if len(fetched.Session.Messages) != 2 {
		t.Fatalf("expected fetched inspect chat to include two messages, got %+v", fetched.Session.Messages)
	}
	if fetched.Session.Messages[0].Origin != "operator" || fetched.Session.Messages[1].Origin != "manager_inspect" {
		t.Fatalf("expected inspect chat API to surface operator vs manager_inspect provenance, got %+v", fetched.Session.Messages)
	}
	if fetched.Session.Messages[1].Execution == nil {
		t.Fatalf("expected fetched legacy inspect reply to surface partial execution snapshot, got %+v", fetched.Session.Messages[1])
	}
	if fetched.Session.Messages[1].Execution.SnapshotStatus != "legacy_partial" || fetched.Session.Messages[1].Execution.ToolScope != "legacy_unknown" {
		t.Fatalf("expected fetched legacy inspect reply to surface partial execution snapshot without overclaim, got %+v", fetched.Session.Messages[1].Execution)
	}
	if fetched.Session.Messages[1].Execution.ExecutionIdentity != "manager_process" || fetched.Session.Messages[1].Execution.ServiceIdentityMode != "shared_manager_process_identity" || fetched.Session.Messages[1].Execution.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected fetched legacy inspect reply to surface manager inspect identity, got %+v", fetched.Session.Messages[1].Execution)
	}
	if len(fetched.Session.Messages[1].Execution.ToolsUsed) != 0 {
		t.Fatalf("expected legacy inspect reply to avoid invented tool-use evidence, got %+v", fetched.Session.Messages[1].Execution.ToolsUsed)
	}
	if len(fetched.Sessions) != 2 {
		t.Fatalf("expected inspect chat get response to include refreshed session inventory, got %+v", fetched.Sessions)
	}
	assertCurrentRuntimeSnapshot := func(name string, process ManagedAgentProcessStatus, live managerLiveRuntimeStatus, catalog managerWorkspaceCatalog) {
		if process.Running {
			t.Fatalf("expected %s to preserve stopped process state, got %+v", name, process)
		}
		if live.Error == "" && live.Status == "" && live.ProcessState == "" {
			t.Fatalf("expected %s to include current live snapshot, got %+v", name, live)
		}
		if catalog.Error == "" && len(catalog.Tasks) == 0 && len(catalog.Tensions) == 0 {
			t.Fatalf("expected %s to include current catalog snapshot, got %+v", name, catalog)
		}
	}
	assertCurrentOverview("inspect chat get response", fetched.Command, fetched.Defaults, fetched.Agents, fetched.Providers, fetched.ProviderCatalog, fetched.CreateDefault)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/agents/partner-agent/local_chats/..\\..\\trusted-agent\\local_chats\\chat-1", nil)
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected traversal chat get to be rejected, got %d body=%s", rec.Code, rec.Body.String())
	}
	var invalidGet struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &invalidGet); err != nil {
		t.Fatalf("decode invalid get response: %v", err)
	}
	if !strings.Contains(invalidGet.Error, "invalid chat ID") {
		t.Fatalf("expected invalid chat id error in get response, got %+v", invalidGet)
	}
	if invalidGet.Contract.ChannelMode != "manager_mediated_inspect" || invalidGet.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected invalid get response to preserve inspect contract, got %+v", invalidGet.Contract)
	}
	if len(invalidGet.Sessions) != 2 {
		t.Fatalf("expected invalid get response to include current session inventory, got %+v", invalidGet.Sessions)
	}
	assertCurrentRuntimeSnapshot("invalid get response", invalidGet.Process, invalidGet.Live, invalidGet.Catalog)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/agents/partner-agent/local_chats/missing-chat", nil)
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected missing chat get to be rejected, got %d body=%s", rec.Code, rec.Body.String())
	}
	var missingGet struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &missingGet); err != nil {
		t.Fatalf("decode missing get response: %v", err)
	}
	if missingGet.Error != "chat not found" {
		t.Fatalf("expected missing get response to explain not found, got %+v", missingGet)
	}
	if missingGet.Contract.ChannelMode != "manager_mediated_inspect" || missingGet.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected missing get response to preserve inspect contract, got %+v", missingGet.Contract)
	}
	if len(missingGet.Sessions) != 2 {
		t.Fatalf("expected missing get response to include current session inventory, got %+v", missingGet.Sessions)
	}
	assertCurrentRuntimeSnapshot("missing get response", missingGet.Process, missingGet.Live, missingGet.Catalog)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/..\\..\\trusted-agent\\local_chats\\chat-1/archive", nil)
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected traversal archive to be rejected, got %d body=%s", rec.Code, rec.Body.String())
	}
	var invalidArchive struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &invalidArchive); err != nil {
		t.Fatalf("decode invalid archive response: %v", err)
	}
	if !strings.Contains(invalidArchive.Error, "invalid chat ID") {
		t.Fatalf("expected invalid chat id error in archive response, got %+v", invalidArchive)
	}
	if invalidArchive.Contract.ChannelMode != "manager_mediated_inspect" || invalidArchive.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected invalid archive response to preserve inspect contract, got %+v", invalidArchive.Contract)
	}
	if len(invalidArchive.Sessions) != 2 {
		t.Fatalf("expected invalid archive response to include current session inventory, got %+v", invalidArchive.Sessions)
	}
	assertCurrentRuntimeSnapshot("invalid archive response", invalidArchive.Process, invalidArchive.Live, invalidArchive.Catalog)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/missing-chat/archive", nil)
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected missing archive to be rejected, got %d body=%s", rec.Code, rec.Body.String())
	}
	var missingArchive struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &missingArchive); err != nil {
		t.Fatalf("decode missing archive response: %v", err)
	}
	if missingArchive.Error != "chat not found" {
		t.Fatalf("expected missing archive response to explain not found, got %+v", missingArchive)
	}
	if missingArchive.Contract.ChannelMode != "manager_mediated_inspect" || missingArchive.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected missing archive response to preserve inspect contract, got %+v", missingArchive.Contract)
	}
	if len(missingArchive.Sessions) != 2 {
		t.Fatalf("expected missing archive response to include current session inventory, got %+v", missingArchive.Sessions)
	}
	assertCurrentRuntimeSnapshot("missing archive response", missingArchive.Process, missingArchive.Live, missingArchive.Catalog)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/agents/partner-agent/local_chats/..\\..\\trusted-agent\\local_chats\\chat-1", nil)
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected traversal delete to be rejected, got %d body=%s", rec.Code, rec.Body.String())
	}
	var invalidDelete struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &invalidDelete); err != nil {
		t.Fatalf("decode invalid delete response: %v", err)
	}
	if !strings.Contains(invalidDelete.Error, "invalid chat ID") {
		t.Fatalf("expected invalid chat id error in delete response, got %+v", invalidDelete)
	}
	if invalidDelete.Contract.ChannelMode != "manager_mediated_inspect" || invalidDelete.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected invalid delete response to preserve inspect contract, got %+v", invalidDelete.Contract)
	}
	if len(invalidDelete.Sessions) != 2 {
		t.Fatalf("expected invalid delete response to include current session inventory, got %+v", invalidDelete.Sessions)
	}
	assertCurrentRuntimeSnapshot("invalid delete response", invalidDelete.Process, invalidDelete.Live, invalidDelete.Catalog)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/agents/partner-agent/local_chats/missing-chat", nil)
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected missing delete to be rejected, got %d body=%s", rec.Code, rec.Body.String())
	}
	var missingDelete struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &missingDelete); err != nil {
		t.Fatalf("decode missing delete response: %v", err)
	}
	if missingDelete.Error != "chat not found" {
		t.Fatalf("expected missing delete response to explain not found, got %+v", missingDelete)
	}
	if missingDelete.Contract.ChannelMode != "manager_mediated_inspect" || missingDelete.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected missing delete response to preserve inspect contract, got %+v", missingDelete.Contract)
	}
	if len(missingDelete.Sessions) != 2 {
		t.Fatalf("expected missing delete response to include current session inventory, got %+v", missingDelete.Sessions)
	}
	assertCurrentRuntimeSnapshot("missing delete response", missingDelete.Process, missingDelete.Live, missingDelete.Catalog)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/..\\..\\trusted-agent\\local_chats\\chat-1/message", strings.NewReader(`{"content":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid chat id send to be rejected, got %d body=%s", rec.Code, rec.Body.String())
	}
	var invalidSend struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &invalidSend); err != nil {
		t.Fatalf("decode invalid send response: %v", err)
	}
	if !strings.Contains(invalidSend.Error, "invalid chat ID") {
		t.Fatalf("expected invalid chat id error in send response, got %+v", invalidSend)
	}
	if invalidSend.Contract.ChannelMode != "manager_mediated_inspect" || invalidSend.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected invalid send response to preserve inspect contract, got %+v", invalidSend.Contract)
	}
	if len(invalidSend.Sessions) != 2 {
		t.Fatalf("expected invalid send response to include current session inventory, got %+v", invalidSend.Sessions)
	}
	assertCurrentRuntimeSnapshot("invalid send response", invalidSend.Process, invalidSend.Live, invalidSend.Catalog)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/missing-chat/message", strings.NewReader(`{"content":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected missing send target to be rejected, got %d body=%s", rec.Code, rec.Body.String())
	}
	var missingSend struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &missingSend); err != nil {
		t.Fatalf("decode missing send response: %v", err)
	}
	if missingSend.Error != "chat not found" {
		t.Fatalf("expected missing send response to explain not found, got %+v", missingSend)
	}
	if missingSend.Contract.ChannelMode != "manager_mediated_inspect" || missingSend.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected missing send response to preserve inspect contract, got %+v", missingSend.Contract)
	}
	if len(missingSend.Sessions) != 2 {
		t.Fatalf("expected missing send response to include current session inventory, got %+v", missingSend.Sessions)
	}
	assertCurrentRuntimeSnapshot("missing send response", missingSend.Process, missingSend.Live, missingSend.Catalog)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(`{"content":`))
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid json send to be rejected, got %d body=%s", rec.Code, rec.Body.String())
	}
	var invalidJSONSend struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &invalidJSONSend); err != nil {
		t.Fatalf("decode invalid json send response: %v", err)
	}
	if invalidJSONSend.Error != "invalid json" {
		t.Fatalf("expected invalid json send response, got %+v", invalidJSONSend)
	}
	if invalidJSONSend.Contract.ChannelMode != "manager_mediated_inspect" || invalidJSONSend.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected invalid json send response to preserve inspect contract, got %+v", invalidJSONSend.Contract)
	}
	if len(invalidJSONSend.Sessions) != 2 {
		t.Fatalf("expected invalid json send response to include current session inventory, got %+v", invalidJSONSend.Sessions)
	}
	assertCurrentRuntimeSnapshot("invalid json send response", invalidJSONSend.Process, invalidJSONSend.Live, invalidJSONSend.Catalog)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(`{"content":"   "}`))
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected empty content send to be rejected, got %d body=%s", rec.Code, rec.Body.String())
	}
	var emptyContentSend struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &emptyContentSend); err != nil {
		t.Fatalf("decode empty content send response: %v", err)
	}
	if emptyContentSend.Error != "empty content" {
		t.Fatalf("expected empty content send response, got %+v", emptyContentSend)
	}
	if emptyContentSend.Contract.ChannelMode != "manager_mediated_inspect" || emptyContentSend.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected empty content send response to preserve inspect contract, got %+v", emptyContentSend.Contract)
	}
	if len(emptyContentSend.Sessions) != 2 {
		t.Fatalf("expected empty content send response to include current session inventory, got %+v", emptyContentSend.Sessions)
	}
	assertCurrentRuntimeSnapshot("empty content send response", emptyContentSend.Process, emptyContentSend.Live, emptyContentSend.Catalog)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID, nil)
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected delete local chat to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var deleted struct {
		OK              bool                      `json:"ok"`
		Deleted         string                    `json:"deleted"`
		Contract        LocalChatContract         `json:"contract"`
		Sessions        []LocalChatSession        `json:"sessions"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Command         string                    `json:"command"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &deleted); err != nil {
		t.Fatalf("decode delete local chat response: %v", err)
	}
	if !deleted.OK || deleted.Deleted != created.Session.ChatID {
		t.Fatalf("expected delete response to confirm removed chat id, got %+v", deleted)
	}
	if deleted.Contract.ChannelMode != "manager_mediated_inspect" || deleted.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected delete local chat response to preserve inspect contract, got %+v", deleted.Contract)
	}
	if len(deleted.Sessions) != 1 || deleted.Sessions[0].ChatID != "reply-provenance" {
		t.Fatalf("expected delete local chat response to include remaining sessions, got %+v", deleted.Sessions)
	}
	if deleted.Process.Running {
		t.Fatalf("expected delete local chat response to preserve stopped process state, got %+v", deleted.Process)
	}
	if deleted.Live.Error == "" && deleted.Live.Status == "" && deleted.Live.ProcessState == "" {
		t.Fatalf("expected delete local chat response to include current live snapshot, got %+v", deleted.Live)
	}
	if deleted.Catalog.Error == "" && len(deleted.Catalog.Tasks) == 0 && len(deleted.Catalog.Tensions) == 0 {
		t.Fatalf("expected delete local chat response to include current catalog snapshot, got %+v", deleted.Catalog)
	}
	assertCurrentOverview("delete local chat response", deleted.Command, deleted.Defaults, deleted.Agents, deleted.Providers, deleted.ProviderCatalog, deleted.CreateDefault)
	if _, err := os.Stat(filepath.Join(managerDir, created.Session.ChatID+".json")); !os.IsNotExist(err) {
		t.Fatalf("expected delete local chat to remove manager-owned transcript, got err=%v", err)
	}
}

func TestManagerWebPartnerManagedLocalChatRejectsSharedManagerAuthFallback(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}
	t.Setenv("OPENAI_API_KEY", "shared-manager-openai")
	if err := SaveKey("shared-manager-saved-key"); err != nil {
		t.Fatalf("SaveKey() error: %v", err)
	}

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "partner-agent",
		DisplayName: "Partner Agent",
		Workdir:     workdir,
		OwnerUserID: "partner-owner",
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendOpenAI,
		Model:       defaultModel,
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		LLMBackend: llmBackendOpenAI,
		Model:      defaultModel,
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	server := newManagerWebServer().routes()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected partner-managed inspect chat create to fail closed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var rejected struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &rejected); err != nil {
		t.Fatalf("decode unavailable create response: %v", err)
	}
	if !strings.Contains(rejected.Error, "inspect chat unavailable: isolated_openai_credential_missing") {
		t.Fatalf("expected isolated credential failure, got %+v", rejected)
	}
	if rejected.Contract.Availability != "unavailable" || rejected.Contract.UnavailableReason != "isolated_openai_credential_missing" {
		t.Fatalf("expected unavailable create response to preserve current inspect contract, got %+v", rejected.Contract)
	}
	if len(rejected.Sessions) != 0 {
		t.Fatalf("expected unavailable create response to preserve empty local-chat inventory, got %+v", rejected.Sessions)
	}
	if rejected.Process.Running {
		t.Fatalf("expected unavailable create response to preserve stopped process state, got %+v", rejected.Process)
	}
	if rejected.Live.Error == "" && rejected.Live.Status == "" && rejected.Live.ProcessState == "" {
		t.Fatalf("expected unavailable create response to include current live snapshot, got %+v", rejected.Live)
	}
	if rejected.Catalog.Error == "" && len(rejected.Catalog.Tasks) == 0 && len(rejected.Catalog.Tensions) == 0 {
		t.Fatalf("expected unavailable create response to include current catalog snapshot, got %+v", rejected.Catalog)
	}
	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	sessions, err := listLocalChats(record, managerDir)
	if err != nil {
		t.Fatalf("listLocalChats() error: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("expected unavailable inspect chat create to leave transcript root empty, got %+v", sessions)
	}
}

func TestManagerWebLocalChatCreatePersistenceFailureReturnsCurrentState(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	existing := &LocalChatSession{
		ChatID: "existing-chat",
		Title:  "Existing Inspect Chat",
		Messages: []LocalChatMessage{
			{Role: "user", Origin: "operator", Content: "hello"},
		},
	}
	if err := saveLocalChatForRecord(record, managerDir, existing); err != nil {
		t.Fatalf("saveLocalChatForRecord(existing) error: %v", err)
	}

	origSaveFn := localChatSaveFn
	localChatSaveFn = func(_ ManagedAgentRecord, _ string, _ *LocalChatSession) error {
		return errors.New("disk full")
	}
	t.Cleanup(func() {
		localChatSaveFn = origSaveFn
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats", nil)
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected create persistence failure, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Error    string             `json:"error"`
		Contract LocalChatContract  `json:"contract"`
		Session  *LocalChatSession  `json:"session"`
		Sessions []LocalChatSession `json:"sessions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode create persistence failure response: %v", err)
	}
	if !strings.Contains(resp.Error, "disk full") {
		t.Fatalf("expected create persistence failure error, got %+v", resp)
	}
	if resp.Contract.ChannelMode != "manager_mediated_inspect" || resp.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected create persistence failure response to preserve inspect contract, got %+v", resp.Contract)
	}
	if resp.Session != nil {
		t.Fatalf("expected create persistence failure to omit unsaved session payload, got %+v", resp.Session)
	}
	if len(resp.Sessions) != 1 || resp.Sessions[0].ChatID != "existing-chat" {
		t.Fatalf("expected create persistence failure to preserve existing session inventory, got %+v", resp.Sessions)
	}
}

func TestManagerWebLocalChatSendCorruptTranscriptReturnsCurrentState(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	existing := &LocalChatSession{
		ChatID: "existing-chat",
		Title:  "Existing Inspect Chat",
		Messages: []LocalChatMessage{
			{Role: "user", Origin: "operator", Content: "hello"},
		},
	}
	if err := saveLocalChatForRecord(record, managerDir, existing); err != nil {
		t.Fatalf("saveLocalChatForRecord(existing) error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(managerDir, "broken-chat.json"), []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("WriteFile(broken-chat) error: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/broken-chat/message", strings.NewReader(`{"content":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected corrupt local chat send to fail closed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Error    string             `json:"error"`
		Contract LocalChatContract  `json:"contract"`
		Sessions []LocalChatSession `json:"sessions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode corrupt local chat send response: %v", err)
	}
	if !strings.Contains(resp.Error, "failed to inspect chat before send") {
		t.Fatalf("expected corrupt local chat send to explain transcript read failure, got %+v", resp)
	}
	if resp.Contract.ChannelMode != "manager_mediated_inspect" || resp.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected corrupt local chat send response to preserve inspect contract, got %+v", resp.Contract)
	}
	if len(resp.Sessions) != 1 || resp.Sessions[0].ChatID != "existing-chat" {
		t.Fatalf("expected corrupt local chat send to preserve readable inventory, got %+v", resp.Sessions)
	}
}

func TestManagerWebLocalChatRouteErrorsReturnCurrentState(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	existing := &LocalChatSession{
		ChatID: "existing-chat",
		Title:  "Existing Inspect Chat",
		Messages: []LocalChatMessage{
			{Role: "user", Origin: "operator", Content: "hello"},
		},
	}
	if err := saveLocalChatForRecord(record, managerDir, existing); err != nil {
		t.Fatalf("saveLocalChatForRecord(existing) error: %v", err)
	}

	server := newManagerWebServer().routes()

	methodRec := httptest.NewRecorder()
	methodReq := httptest.NewRequest(http.MethodPut, "/api/agents/trusted-agent/local_chats", nil)
	server.ServeHTTP(methodRec, methodReq)
	if methodRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected local_chat route method reject, got %d body=%s", methodRec.Code, methodRec.Body.String())
	}
	var methodResp struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(methodRec.Body.Bytes(), &methodResp); err != nil {
		t.Fatalf("decode local_chat method reject response: %v", err)
	}
	if methodResp.Error != "method not allowed" {
		t.Fatalf("expected method reject to explain method not allowed, got %+v", methodResp)
	}
	if methodResp.Contract.ChannelMode != "manager_mediated_inspect" || methodResp.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected local_chat method reject to preserve inspect contract, got %+v", methodResp.Contract)
	}
	if len(methodResp.Sessions) != 1 || methodResp.Sessions[0].ChatID != "existing-chat" {
		t.Fatalf("expected local_chat method reject to preserve session inventory, got %+v", methodResp.Sessions)
	}
	if methodResp.Process.Running {
		t.Fatalf("expected local_chat method reject to preserve stopped process state, got %+v", methodResp.Process)
	}
	if methodResp.Live.Error == "" && methodResp.Live.Status == "" && methodResp.Live.ProcessState == "" {
		t.Fatalf("expected local_chat method reject to include current live snapshot, got %+v", methodResp.Live)
	}
	if methodResp.Catalog.Error == "" && len(methodResp.Catalog.Tasks) == 0 && len(methodResp.Catalog.Tensions) == 0 {
		t.Fatalf("expected local_chat method reject to include current catalog snapshot, got %+v", methodResp.Catalog)
	}

	routeRec := httptest.NewRecorder()
	routeReq := httptest.NewRequest(http.MethodGet, "/api/agents/trusted-agent/local_chats/existing-chat/unknown", nil)
	server.ServeHTTP(routeRec, routeReq)
	if routeRec.Code != http.StatusNotFound {
		t.Fatalf("expected local_chat route reject, got %d body=%s", routeRec.Code, routeRec.Body.String())
	}
	var routeResp struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(routeRec.Body.Bytes(), &routeResp); err != nil {
		t.Fatalf("decode local_chat route reject response: %v", err)
	}
	if routeResp.Error != "unknown local_chat route" {
		t.Fatalf("expected route reject to explain unknown local_chat route, got %+v", routeResp)
	}
	if routeResp.Contract.ChannelMode != "manager_mediated_inspect" || routeResp.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected local_chat route reject to preserve inspect contract, got %+v", routeResp.Contract)
	}
	if len(routeResp.Sessions) != 1 || routeResp.Sessions[0].ChatID != "existing-chat" {
		t.Fatalf("expected local_chat route reject to preserve session inventory, got %+v", routeResp.Sessions)
	}
	if routeResp.Process.Running {
		t.Fatalf("expected local_chat route reject to preserve stopped process state, got %+v", routeResp.Process)
	}
	if routeResp.Live.Error == "" && routeResp.Live.Status == "" && routeResp.Live.ProcessState == "" {
		t.Fatalf("expected local_chat route reject to include current live snapshot, got %+v", routeResp.Live)
	}
	if routeResp.Catalog.Error == "" && len(routeResp.Catalog.Tasks) == 0 && len(routeResp.Catalog.Tensions) == 0 {
		t.Fatalf("expected local_chat route reject to include current catalog snapshot, got %+v", routeResp.Catalog)
	}
}

func TestManagerWebAgentRouteErrorsReturnCurrentContext(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	server := newManagerWebServer().routes()

	missingIDRec := httptest.NewRecorder()
	missingIDReq := httptest.NewRequest(http.MethodGet, "/api/agents/", nil)
	server.ServeHTTP(missingIDRec, missingIDReq)
	if missingIDRec.Code != http.StatusNotFound {
		t.Fatalf("expected missing agent id to be rejected, got %d body=%s", missingIDRec.Code, missingIDRec.Body.String())
	}
	var missingIDResp struct {
		Error  string               `json:"error"`
		Agents []managerWebAgentRow `json:"agents"`
	}
	if err := json.Unmarshal(missingIDRec.Body.Bytes(), &missingIDResp); err != nil {
		t.Fatalf("decode missing agent id response: %v", err)
	}
	if missingIDResp.Error != "agent id is required" {
		t.Fatalf("expected missing agent id error, got %+v", missingIDResp)
	}
	if len(missingIDResp.Agents) != 1 || missingIDResp.Agents[0].Record.AgentID != "trusted-agent" {
		t.Fatalf("expected missing agent id response to preserve overview inventory, got %+v", missingIDResp.Agents)
	}

	unknownAgentRec := httptest.NewRecorder()
	unknownAgentReq := httptest.NewRequest(http.MethodGet, "/api/agents/missing-agent", nil)
	server.ServeHTTP(unknownAgentRec, unknownAgentReq)
	if unknownAgentRec.Code != http.StatusNotFound {
		t.Fatalf("expected unknown agent to be rejected, got %d body=%s", unknownAgentRec.Code, unknownAgentRec.Body.String())
	}
	var unknownAgentResp struct {
		Error  string               `json:"error"`
		Agents []managerWebAgentRow `json:"agents"`
	}
	if err := json.Unmarshal(unknownAgentRec.Body.Bytes(), &unknownAgentResp); err != nil {
		t.Fatalf("decode unknown agent response: %v", err)
	}
	if !strings.Contains(unknownAgentResp.Error, "missing-agent") {
		t.Fatalf("expected unknown agent response to explain missing agent, got %+v", unknownAgentResp)
	}
	if len(unknownAgentResp.Agents) != 1 || unknownAgentResp.Agents[0].Record.AgentID != "trusted-agent" {
		t.Fatalf("expected unknown agent response to preserve overview inventory, got %+v", unknownAgentResp.Agents)
	}

	methodRec := httptest.NewRecorder()
	methodReq := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent", nil)
	server.ServeHTTP(methodRec, methodReq)
	if methodRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected agent detail method reject, got %d body=%s", methodRec.Code, methodRec.Body.String())
	}
	var methodResp struct {
		Error   string                    `json:"error"`
		Process ManagedAgentProcessStatus `json:"process"`
		Agents  []managerWebAgentRow      `json:"agents"`
	}
	if err := json.Unmarshal(methodRec.Body.Bytes(), &methodResp); err != nil {
		t.Fatalf("decode agent detail method reject response: %v", err)
	}
	if methodResp.Error != "method not allowed" {
		t.Fatalf("expected agent detail method reject to explain method not allowed, got %+v", methodResp)
	}
	if methodResp.Process.Workdir != record.Workdir {
		t.Fatalf("expected agent detail method reject to preserve selected-agent process context, got %+v", methodResp.Process)
	}
	if len(methodResp.Agents) != 1 || methodResp.Agents[0].Record.AgentID != "trusted-agent" {
		t.Fatalf("expected agent detail method reject to preserve overview inventory, got %+v", methodResp.Agents)
	}

	routeRec := httptest.NewRecorder()
	routeReq := httptest.NewRequest(http.MethodGet, "/api/agents/trusted-agent/unknown", nil)
	server.ServeHTTP(routeRec, routeReq)
	if routeRec.Code != http.StatusNotFound {
		t.Fatalf("expected unknown agent subroute to be rejected, got %d body=%s", routeRec.Code, routeRec.Body.String())
	}
	var routeResp struct {
		Error   string                    `json:"error"`
		Process ManagedAgentProcessStatus `json:"process"`
		Agents  []managerWebAgentRow      `json:"agents"`
	}
	if err := json.Unmarshal(routeRec.Body.Bytes(), &routeResp); err != nil {
		t.Fatalf("decode unknown agent subroute response: %v", err)
	}
	if routeResp.Error != "unknown agent route" {
		t.Fatalf("expected unknown agent subroute response to explain route error, got %+v", routeResp)
	}
	if routeResp.Process.Workdir != record.Workdir {
		t.Fatalf("expected unknown agent subroute response to preserve selected-agent process context, got %+v", routeResp.Process)
	}
	if len(routeResp.Agents) != 1 || routeResp.Agents[0].Record.AgentID != "trusted-agent" {
		t.Fatalf("expected unknown agent subroute response to preserve overview inventory, got %+v", routeResp.Agents)
	}
}

func TestManagerWebDashboardRouteErrorsReturnOverviewContext(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	server := newManagerWebServer().routes()

	assertOverviewError := func(name string, rec *httptest.ResponseRecorder, expectedStatus int, expectedError string) {
		t.Helper()
		if rec.Code != expectedStatus {
			t.Fatalf("%s: expected status %d, got %d body=%s", name, expectedStatus, rec.Code, rec.Body.String())
		}
		var resp struct {
			Error  string               `json:"error"`
			Agents []managerWebAgentRow `json:"agents"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("%s: decode error response: %v", name, err)
		}
		if resp.Error != expectedError {
			t.Fatalf("%s: expected error %q, got %+v", name, expectedError, resp)
		}
		if len(resp.Agents) != 1 || resp.Agents[0].Record.AgentID != "trusted-agent" {
			t.Fatalf("%s: expected overview inventory in error response, got %+v", name, resp.Agents)
		}
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/overview", nil)
	server.ServeHTTP(rec, req)
	assertOverviewError("overview method", rec, http.StatusMethodNotAllowed, "method not allowed")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/defaults", nil)
	server.ServeHTTP(rec, req)
	assertOverviewError("defaults method", rec, http.StatusMethodNotAllowed, "method not allowed")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPatch, "/api/providers", nil)
	server.ServeHTTP(rec, req)
	assertOverviewError("providers method", rec, http.StatusMethodNotAllowed, "method not allowed")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/providers/", nil)
	server.ServeHTTP(rec, req)
	assertOverviewError("provider id missing", rec, http.StatusNotFound, "provider id is required")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/providers/missing-provider", nil)
	server.ServeHTTP(rec, req)
	assertOverviewError("provider missing", rec, http.StatusNotFound, `unknown provider "missing-provider"`)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/onboard", nil)
	server.ServeHTTP(rec, req)
	assertOverviewError("onboard method", rec, http.StatusMethodNotAllowed, "method not allowed")
}

func TestManagerWebAgentSubrouteMethodErrorsReturnCurrentContext(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	server := newManagerWebServer().routes()
	cases := []struct {
		name   string
		method string
		path   string
	}{
		{name: "process", method: http.MethodGet, path: "/api/agents/lyrica/process"},
		{name: "control", method: http.MethodGet, path: "/api/agents/lyrica/control"},
		{name: "settings", method: http.MethodGet, path: "/api/agents/lyrica/settings"},
		{name: "edit", method: http.MethodGet, path: "/api/agents/lyrica/edit"},
		{name: "messages", method: http.MethodPost, path: "/api/agents/lyrica/messages"},
		{name: "activity", method: http.MethodPost, path: "/api/agents/lyrica/activity"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			server.ServeHTTP(rec, req)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("expected %s method reject, got %d body=%s", tc.name, rec.Code, rec.Body.String())
			}
			var resp struct {
				Error   string                    `json:"error"`
				Process ManagedAgentProcessStatus `json:"process"`
				Agents  []managerWebAgentRow      `json:"agents"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode %s method reject response: %v", tc.name, err)
			}
			if resp.Error != "method not allowed" {
				t.Fatalf("expected %s method reject to explain method not allowed, got %+v", tc.name, resp)
			}
			if resp.Process.Workdir != record.Workdir {
				t.Fatalf("expected %s method reject to preserve selected-agent process context, got %+v", tc.name, resp.Process)
			}
			if len(resp.Agents) != 1 || resp.Agents[0].Record.AgentID != "lyrica" {
				t.Fatalf("expected %s method reject to preserve overview inventory, got %+v", tc.name, resp.Agents)
			}
		})
	}
}

func TestManagerWebFSListReturnsOverviewContext(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	server := newManagerWebServer().routes()
	missingDir := filepath.Join(t.TempDir(), "missing-dir")
	listDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(listDir, "notes.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatalf("WriteFile(notes.txt) error: %v", err)
	}

	successRec := httptest.NewRecorder()
	successReq := httptest.NewRequest(http.MethodGet, "/api/fs/list?dir="+url.QueryEscape(listDir), nil)
	server.ServeHTTP(successRec, successReq)
	if successRec.Code != http.StatusOK {
		t.Fatalf("expected fs list success, got %d body=%s", successRec.Code, successRec.Body.String())
	}
	var successResp struct {
		Path    string               `json:"path"`
		Parent  string               `json:"parent"`
		Entries []webFSEntry         `json:"entries"`
		Agents  []managerWebAgentRow `json:"agents"`
	}
	if err := json.Unmarshal(successRec.Body.Bytes(), &successResp); err != nil {
		t.Fatalf("decode fs list success response: %v", err)
	}
	if filepath.Clean(successResp.Path) != filepath.Clean(listDir) {
		t.Fatalf("expected fs list success to preserve requested path, got %+v", successResp)
	}
	if filepath.Clean(successResp.Parent) != filepath.Clean(filepath.Dir(listDir)) {
		t.Fatalf("expected fs list success to preserve requested parent, got %+v", successResp)
	}
	if len(successResp.Entries) != 1 || successResp.Entries[0].Name != "notes.txt" {
		t.Fatalf("expected fs list success to preserve directory entries, got %+v", successResp.Entries)
	}
	if len(successResp.Agents) != 1 || successResp.Agents[0].Record.AgentID != "trusted-agent" {
		t.Fatalf("expected fs list success to preserve overview inventory, got %+v", successResp.Agents)
	}

	methodRec := httptest.NewRecorder()
	methodReq := httptest.NewRequest(http.MethodPost, "/api/fs/list?dir="+url.QueryEscape(missingDir), nil)
	server.ServeHTTP(methodRec, methodReq)
	if methodRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected fs list method reject, got %d body=%s", methodRec.Code, methodRec.Body.String())
	}
	var methodResp struct {
		Error  string               `json:"error"`
		Path   string               `json:"path"`
		Parent string               `json:"parent"`
		Agents []managerWebAgentRow `json:"agents"`
	}
	if err := json.Unmarshal(methodRec.Body.Bytes(), &methodResp); err != nil {
		t.Fatalf("decode fs list method reject response: %v", err)
	}
	if methodResp.Error != "method not allowed" {
		t.Fatalf("expected fs list method reject to explain method not allowed, got %+v", methodResp)
	}
	if filepath.Clean(methodResp.Path) != filepath.Clean(missingDir) {
		t.Fatalf("expected fs list method reject to preserve requested path, got %+v", methodResp)
	}
	if filepath.Clean(methodResp.Parent) != filepath.Clean(filepath.Dir(missingDir)) {
		t.Fatalf("expected fs list method reject to preserve requested parent, got %+v", methodResp)
	}
	if len(methodResp.Agents) != 1 || methodResp.Agents[0].Record.AgentID != "trusted-agent" {
		t.Fatalf("expected fs list method reject to preserve overview inventory, got %+v", methodResp.Agents)
	}

	readRec := httptest.NewRecorder()
	readReq := httptest.NewRequest(http.MethodGet, "/api/fs/list?dir="+url.QueryEscape(missingDir), nil)
	server.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusInternalServerError {
		t.Fatalf("expected fs list read failure, got %d body=%s", readRec.Code, readRec.Body.String())
	}
	var readResp struct {
		Error  string               `json:"error"`
		Path   string               `json:"path"`
		Parent string               `json:"parent"`
		Agents []managerWebAgentRow `json:"agents"`
	}
	if err := json.Unmarshal(readRec.Body.Bytes(), &readResp); err != nil {
		t.Fatalf("decode fs list read failure response: %v", err)
	}
	if readResp.Error == "" {
		t.Fatalf("expected fs list read failure to include error, got %+v", readResp)
	}
	if filepath.Clean(readResp.Path) != filepath.Clean(missingDir) {
		t.Fatalf("expected fs list read failure to preserve requested path, got %+v", readResp)
	}
	if filepath.Clean(readResp.Parent) != filepath.Clean(filepath.Dir(missingDir)) {
		t.Fatalf("expected fs list read failure to preserve requested parent, got %+v", readResp)
	}
	if len(readResp.Agents) != 1 || readResp.Agents[0].Record.AgentID != "trusted-agent" {
		t.Fatalf("expected fs list read failure to preserve overview inventory, got %+v", readResp.Agents)
	}
}

func TestManagerWebLocalChatListFailureReturnsCurrentState(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	origListFn := localChatListFn
	localChatListFn = func(_ ManagedAgentRecord, _ string) ([]LocalChatSession, error) {
		return nil, errors.New("list failed")
	}
	t.Cleanup(func() {
		localChatListFn = origListFn
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/agents/trusted-agent/local_chats", nil)
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected local chat list failure, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Error           string                    `json:"error"`
		Contract        LocalChatContract         `json:"contract"`
		Sessions        []LocalChatSession        `json:"sessions"`
		ListError       string                    `json:"list_error"`
		Process         ManagedAgentProcessStatus `json:"process"`
		Live            managerLiveRuntimeStatus  `json:"live"`
		Catalog         managerWorkspaceCatalog   `json:"catalog"`
		Command         string                    `json:"command"`
		Defaults        BotManagerDefaults        `json:"defaults"`
		Agents          []managerWebAgentRow      `json:"agents"`
		Providers       []ProviderRecord          `json:"providers"`
		ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
		CreateDefault   managerWebCreateDefault   `json:"create_default"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode local chat list failure response: %v", err)
	}
	if !strings.Contains(resp.Error, "list failed") {
		t.Fatalf("expected list failure error body, got %+v", resp)
	}
	if resp.Contract.ChannelMode != "manager_mediated_inspect" || resp.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected list failure response to preserve inspect contract, got %+v", resp.Contract)
	}
	if len(resp.Sessions) != 0 {
		t.Fatalf("expected list failure response to omit unreadable inventory, got %+v", resp.Sessions)
	}
	if !strings.Contains(resp.ListError, "list failed") {
		t.Fatalf("expected list failure response to preserve list_error, got %+v", resp)
	}
	if resp.Process.Running {
		t.Fatalf("expected list failure response to preserve stopped process state, got %+v", resp.Process)
	}
	if resp.Live.Error == "" && resp.Live.Status == "" && resp.Live.ProcessState == "" {
		t.Fatalf("expected list failure response to include current live snapshot, got %+v", resp.Live)
	}
	if resp.Catalog.Error == "" && len(resp.Catalog.Tasks) == 0 && len(resp.Catalog.Tensions) == 0 {
		t.Fatalf("expected list failure response to include current catalog snapshot, got %+v", resp.Catalog)
	}
	if resp.Command != appCommandName {
		t.Fatalf("expected list failure response to preserve command name, got %+v", resp)
	}
	if len(resp.Agents) != 1 || resp.Agents[0].Record.AgentID != record.AgentID {
		t.Fatalf("expected list failure response to preserve current agent overview, got %+v", resp.Agents)
	}
	if len(resp.Providers) != 0 {
		t.Fatalf("expected list failure response to preserve empty provider inventory, got %+v", resp.Providers)
	}
	if len(resp.ProviderCatalog) == 0 {
		t.Fatalf("expected list failure response to preserve provider catalog, got %+v", resp.ProviderCatalog)
	}
	if strings.TrimSpace(resp.CreateDefault.ParentDir) == "" || strings.TrimSpace(resp.CreateDefault.Workdir) == "" {
		t.Fatalf("expected list failure response to preserve create-default context, got %+v", resp.CreateDefault)
	}
}

func TestManagerWebLocalChatSendDirFailureReturnsCurrentState(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	origDirFn := localChatDirFn
	localChatDirFn = func(_ ManagedAgentRecord) (string, error) {
		return "", errors.New("manager local chat root unavailable")
	}
	t.Cleanup(func() {
		localChatDirFn = origDirFn
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/chat-1/message", strings.NewReader(`{"content":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected local chat send dir failure, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode local chat send dir failure response: %v", err)
	}
	if !strings.Contains(resp.Error, "manager local chat root unavailable") {
		t.Fatalf("expected dir failure error body, got %+v", resp)
	}
	if resp.Contract.ChannelMode != "manager_mediated_inspect" || resp.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected send dir failure response to preserve inspect contract, got %+v", resp.Contract)
	}
	if len(resp.Sessions) != 0 {
		t.Fatalf("expected send dir failure response to omit session inventory, got %+v", resp.Sessions)
	}
	if resp.Process.Running {
		t.Fatalf("expected send dir failure response to preserve stopped process state, got %+v", resp.Process)
	}
	if resp.Live.Error == "" && resp.Live.Status == "" && resp.Live.ProcessState == "" {
		t.Fatalf("expected send dir failure response to include current live snapshot, got %+v", resp.Live)
	}
	if resp.Catalog.Error == "" && len(resp.Catalog.Tasks) == 0 && len(resp.Catalog.Tensions) == 0 {
		t.Fatalf("expected send dir failure response to include current catalog snapshot, got %+v", resp.Catalog)
	}
}

func TestManagerWebLocalChatSessionContractRefreshesWhenReadinessChanges(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "partner-agent",
		Workdir:     workdir,
		WorkspaceID: "rhizome-main",
		OwnerUserID: "partner-owner",
	}
	configRoot := managedAgentConfigRootPath(workdir)
	if err := os.MkdirAll(configRoot, 0o700); err != nil {
		t.Fatalf("MkdirAll(configRoot) error: %v", err)
	}
	if err := os.WriteFile(keyPathForRoot(configRoot), []byte("partner-openai-key"), 0o600); err != nil {
		t.Fatalf("WriteFile(openai_key) error: %v", err)
	}

	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}
	mux := newManagerWebServer().routes()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected create local chat to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Contract LocalChatContract `json:"contract"`
		Session  LocalChatSession  `json:"session"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create local chat response: %v", err)
	}
	if created.Contract.Availability != "available" || created.Contract.AuthBackend != llmBackendOpenAI {
		t.Fatalf("expected initial inspect contract to be available via isolated OpenAI, got %+v", created.Contract)
	}

	if err := os.Remove(keyPathForRoot(configRoot)); err != nil {
		t.Fatalf("Remove(openai_key) error: %v", err)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID, nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected get local chat to succeed after readiness change, got %d body=%s", rec.Code, rec.Body.String())
	}
	var fetched struct {
		Contract LocalChatContract         `json:"contract"`
		Session  LocalChatSession          `json:"session"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("decode get local chat response: %v", err)
	}
	if fetched.Contract.Availability != "unavailable" || fetched.Contract.UnavailableReason != "isolated_local_auth_missing" {
		t.Fatalf("expected refreshed inspect contract to become unavailable, got %+v", fetched.Contract)
	}
	if fetched.Session.Contract.Availability != "unavailable" || fetched.Session.Contract.UnavailableReason != "isolated_local_auth_missing" {
		t.Fatalf("expected session contract to refresh with current readiness, got %+v", fetched.Session.Contract)
	}
	if len(fetched.Sessions) != 1 || fetched.Sessions[0].ChatID != created.Session.ChatID {
		t.Fatalf("expected get local chat response to include refreshed session inventory, got %+v", fetched.Sessions)
	}
	if fetched.Process.Running {
		t.Fatalf("expected get local chat response to preserve current stopped process state, got %+v", fetched.Process)
	}
	if fetched.Live.Error == "" && fetched.Live.Status == "" && fetched.Live.ProcessState == "" {
		t.Fatalf("expected get local chat response to preserve current live runtime snapshot, got %+v", fetched.Live)
	}
	if fetched.Catalog.Error == "" && len(fetched.Catalog.Tasks) == 0 && len(fetched.Catalog.Tensions) == 0 {
		t.Fatalf("expected get local chat response to preserve current catalog snapshot, got %+v", fetched.Catalog)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(`{"content":"hello again"}`))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected inspect send to fail closed after readiness change, got %d body=%s", rec.Code, rec.Body.String())
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	stored, err := getLocalChat(managerDir, created.Session.ChatID)
	if err != nil {
		t.Fatalf("getLocalChat() error: %v", err)
	}
	if len(stored.Messages) != 0 {
		t.Fatalf("expected readiness-change reject to leave transcript unchanged, got %+v", stored.Messages)
	}
}

func TestManagerWebLocalChatOwnerChangeDoesNotReusePreviousOwnerTranscriptRoot(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	workdir := t.TempDir()
	server := newManagerWebServer().routes()

	record := ManagedAgentRecord{
		AgentID:     "shared-agent",
		DisplayName: "Shared Agent",
		Workdir:     workdir,
		OwnerUserID: "developer",
		HostURL:     "https://rhizome.test",
		WorkspaceID: "rhizome-main",
		Role:        "generalist",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent(trusted) error: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/shared-agent/local_chats", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected trusted local chat create to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Session LocalChatSession `json:"session"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode trusted create response: %v", err)
	}

	record.OwnerUserID = "partner-owner"
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent(partner) error: %v", err)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/agents/shared-agent/local_chats", nil)
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected partner list local chats to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var listed struct {
		Sessions []LocalChatSession `json:"sessions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode partner local chat list: %v", err)
	}
	if len(listed.Sessions) != 0 {
		t.Fatalf("expected owner-changed record to see no prior-owner transcripts, got %+v", listed.Sessions)
	}

	partnerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord(partner) error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(partnerDir, created.Session.ChatID+".json")); !os.IsNotExist(err) {
		t.Fatalf("expected prior-owner transcript to stay out of new owner root, got err=%v", err)
	}
}

func TestManagerWebLocalChatRejectsConcurrentInspectSendWithoutTranscriptMutation(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "partner-agent",
		Workdir:     workdir,
		WorkspaceID: "rhizome-main",
		OwnerUserID: "partner-owner",
	}
	configRoot := managedAgentConfigRootPath(workdir)
	if err := os.MkdirAll(configRoot, 0o700); err != nil {
		t.Fatalf("MkdirAll(configRoot) error: %v", err)
	}
	if err := os.WriteFile(keyPathForRoot(configRoot), []byte("partner-openai-key"), 0o600); err != nil {
		t.Fatalf("WriteFile(openai_key) error: %v", err)
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	llm := &blockingInspectLLM{
		started:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		response: &LLMResponse{Content: "first reply"},
	}
	origFactory := localInspectLLMFactory
	origBudget := localInspectSendTimeoutBudget
	localInspectLLMFactory = func(_ ManagedAgentRecord, _ RuntimeConfig) (ChatLLM, error) { return llm, nil }
	localInspectSendTimeoutBudget = 2 * time.Second
	t.Cleanup(func() {
		localInspectLLMFactory = origFactory
		localInspectSendTimeoutBudget = origBudget
		select {
		case <-llm.release:
		default:
			close(llm.release)
		}
	})

	mux := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected create local chat to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Session LocalChatSession `json:"session"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create local chat response: %v", err)
	}

	firstDone := make(chan struct{})
	firstRec := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(`{"content":"hello first"}`))
	firstReq.Header.Set("Content-Type", "application/json")
	go func() {
		mux.ServeHTTP(firstRec, firstReq)
		close(firstDone)
	}()

	select {
	case <-llm.started:
	case <-time.After(2 * time.Second):
		t.Fatal("expected first inspect send to reach llm")
	}

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID, nil)
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected get local chat while first send busy to succeed, got %d body=%s", getRec.Code, getRec.Body.String())
	}
	var fetched struct {
		Contract LocalChatContract `json:"contract"`
		Session  LocalChatSession  `json:"session"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("decode get while busy response: %v", err)
	}
	if fetched.Contract.ExecutionState != "busy" || fetched.Session.Contract.ExecutionState != "busy" {
		t.Fatalf("expected contract to surface busy execution state while first send in flight, got contract=%+v session=%+v", fetched.Contract, fetched.Session.Contract)
	}
	if fetched.Contract.ExecutionStateReason != "workdir_inspect_in_flight" || fetched.Session.Contract.ExecutionStateReason != "workdir_inspect_in_flight" {
		t.Fatalf("expected contract to surface busy execution state reason while first send in flight, got contract=%+v session=%+v", fetched.Contract, fetched.Session.Contract)
	}

	secondRec := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(`{"content":"hello second"}`))
	secondReq.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(secondRec, secondReq)
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("expected second inspect send to fail busy, got %d body=%s", secondRec.Code, secondRec.Body.String())
	}
	if !strings.Contains(secondRec.Body.String(), "manager_inspect_busy") {
		t.Fatalf("expected busy reject body, got %s", secondRec.Body.String())
	}

	close(llm.release)
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("expected first inspect send to complete after release")
	}
	if firstRec.Code != http.StatusOK {
		t.Fatalf("expected first inspect send to succeed, got %d body=%s", firstRec.Code, firstRec.Body.String())
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	stored, err := getLocalChat(managerDir, created.Session.ChatID)
	if err != nil {
		t.Fatalf("getLocalChat() error: %v", err)
	}
	if len(stored.Messages) != 2 {
		t.Fatalf("expected only successful inspect send to mutate transcript, got %+v", stored.Messages)
	}
	if stored.Messages[0].Content != "hello first" || stored.Messages[1].Content != "first reply" {
		t.Fatalf("expected transcript to contain only first request pair, got %+v", stored.Messages)
	}
}

func TestManagerWebLocalChatSaturatesAcrossDifferentWorkdirs(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	recordA := ManagedAgentRecord{
		AgentID:     "partner-agent-a",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "partner-owner-a",
	}
	recordB := ManagedAgentRecord{
		AgentID:     "partner-agent-b",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "partner-owner-b",
	}
	for _, record := range []ManagedAgentRecord{recordA, recordB} {
		configRoot := managedAgentConfigRootPath(record.Workdir)
		if err := os.MkdirAll(configRoot, 0o700); err != nil {
			t.Fatalf("MkdirAll(configRoot) error: %v", err)
		}
		if err := os.WriteFile(keyPathForRoot(configRoot), []byte("partner-openai-key"), 0o600); err != nil {
			t.Fatalf("WriteFile(openai_key) error: %v", err)
		}
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{recordA, recordB}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	llm := &blockingInspectLLM{
		started:  make(chan struct{}, 2),
		release:  make(chan struct{}),
		response: &LLMResponse{Content: "first reply"},
	}
	origFactory := localInspectLLMFactory
	origBudget := localInspectSendTimeoutBudget
	origGlobalBudget := localInspectGlobalBudget
	localInspectLLMFactory = func(_ ManagedAgentRecord, _ RuntimeConfig) (ChatLLM, error) { return llm, nil }
	localInspectSendTimeoutBudget = 2 * time.Second
	localInspectGlobalBudget = 1
	t.Cleanup(func() {
		localInspectLLMFactory = origFactory
		localInspectSendTimeoutBudget = origBudget
		localInspectGlobalBudget = origGlobalBudget
		select {
		case <-llm.release:
		default:
			close(llm.release)
		}
	})

	mux := newManagerWebServer().routes()
	createChat := func(agentID string) string {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/agents/"+agentID+"/local_chats", nil)
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected create local chat to succeed for %s, got %d body=%s", agentID, rec.Code, rec.Body.String())
		}
		var created struct {
			Session LocalChatSession `json:"session"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
			t.Fatalf("decode create local chat response: %v", err)
		}
		return created.Session.ChatID
	}

	chatA := createChat("partner-agent-a")
	chatB := createChat("partner-agent-b")

	firstDone := make(chan struct{})
	firstRec := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent-a/local_chats/"+chatA+"/message", strings.NewReader(`{"content":"hello first"}`))
	firstReq.Header.Set("Content-Type", "application/json")
	go func() {
		mux.ServeHTTP(firstRec, firstReq)
		close(firstDone)
	}()

	select {
	case <-llm.started:
	case <-time.After(2 * time.Second):
		t.Fatal("expected first inspect send to reach llm")
	}

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/api/agents/partner-agent-b/local_chats/"+chatB, nil)
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected get local chat on second workdir while saturated to succeed, got %d body=%s", getRec.Code, getRec.Body.String())
	}
	var fetched struct {
		Contract LocalChatContract `json:"contract"`
		Session  LocalChatSession  `json:"session"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("decode get while saturated response: %v", err)
	}
	if fetched.Contract.ExecutionState != "saturated" || fetched.Session.Contract.ExecutionState != "saturated" {
		t.Fatalf("expected second workdir contract to surface saturated execution state, got contract=%+v session=%+v", fetched.Contract, fetched.Session.Contract)
	}
	if fetched.Contract.ExecutionStateReason != "shared_manager_inspect_budget_exhausted" || fetched.Session.Contract.ExecutionStateReason != "shared_manager_inspect_budget_exhausted" {
		t.Fatalf("expected second workdir contract to surface saturation reason, got contract=%+v session=%+v", fetched.Contract, fetched.Session.Contract)
	}

	secondRec := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent-b/local_chats/"+chatB+"/message", strings.NewReader(`{"content":"hello second"}`))
	secondReq.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(secondRec, secondReq)
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("expected second workdir inspect send to fail saturated, got %d body=%s", secondRec.Code, secondRec.Body.String())
	}
	if !strings.Contains(secondRec.Body.String(), "manager_inspect_saturated") {
		t.Fatalf("expected saturation reject body, got %s", secondRec.Body.String())
	}
	select {
	case <-llm.started:
		t.Fatal("expected saturated second workdir send to reject before llm start")
	default:
	}

	managerDirB, err := localChatsDirForRecord(recordB)
	if err != nil {
		t.Fatalf("localChatsDirForRecord(recordB) error: %v", err)
	}
	storedB, err := getLocalChat(managerDirB, chatB)
	if err != nil {
		t.Fatalf("getLocalChat(recordB) error: %v", err)
	}
	if len(storedB.Messages) != 0 {
		t.Fatalf("expected saturated second workdir send to leave transcript unchanged, got %+v", storedB.Messages)
	}

	close(llm.release)
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("expected first inspect send to complete after release")
	}
	if firstRec.Code != http.StatusOK {
		t.Fatalf("expected first inspect send to succeed, got %d body=%s", firstRec.Code, firstRec.Body.String())
	}
}

func TestManagerWebLocalChatDeleteRejectsInFlightInspectRun(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "partner-agent",
		Workdir:     workdir,
		WorkspaceID: "rhizome-main",
		OwnerUserID: "partner-owner",
	}
	configRoot := managedAgentConfigRootPath(workdir)
	if err := os.MkdirAll(configRoot, 0o700); err != nil {
		t.Fatalf("MkdirAll(configRoot) error: %v", err)
	}
	if err := os.WriteFile(keyPathForRoot(configRoot), []byte("partner-openai-key"), 0o600); err != nil {
		t.Fatalf("WriteFile(openai_key) error: %v", err)
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	llm := &blockingInspectLLM{
		started:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		response: &LLMResponse{Content: "first reply"},
	}
	origFactory := localInspectLLMFactory
	origBudget := localInspectSendTimeoutBudget
	localInspectLLMFactory = func(_ ManagedAgentRecord, _ RuntimeConfig) (ChatLLM, error) { return llm, nil }
	localInspectSendTimeoutBudget = 2 * time.Second
	t.Cleanup(func() {
		localInspectLLMFactory = origFactory
		localInspectSendTimeoutBudget = origBudget
		select {
		case <-llm.release:
		default:
			close(llm.release)
		}
	})

	mux := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected create local chat to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Session LocalChatSession `json:"session"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create local chat response: %v", err)
	}

	firstDone := make(chan struct{})
	firstRec := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(`{"content":"hello first"}`))
	firstReq.Header.Set("Content-Type", "application/json")
	go func() {
		mux.ServeHTTP(firstRec, firstReq)
		close(firstDone)
	}()

	select {
	case <-llm.started:
	case <-time.After(2 * time.Second):
		t.Fatal("expected first inspect send to reach llm")
	}

	deleteRec := httptest.NewRecorder()
	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID, nil)
	mux.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusConflict {
		t.Fatalf("expected delete to reject while inspect run active, got %d body=%s", deleteRec.Code, deleteRec.Body.String())
	}
	if !strings.Contains(deleteRec.Body.String(), "delete blocked") {
		t.Fatalf("expected delete-blocked error body, got %s", deleteRec.Body.String())
	}

	close(llm.release)
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("expected first inspect send to complete after release")
	}
}

func TestManagerWebLocalChatTimeoutLeavesTranscriptUnchanged(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "partner-agent",
		Workdir:     workdir,
		WorkspaceID: "rhizome-main",
		OwnerUserID: "partner-owner",
	}
	configRoot := managedAgentConfigRootPath(workdir)
	if err := os.MkdirAll(configRoot, 0o700); err != nil {
		t.Fatalf("MkdirAll(configRoot) error: %v", err)
	}
	if err := os.WriteFile(keyPathForRoot(configRoot), []byte("partner-openai-key"), 0o600); err != nil {
		t.Fatalf("WriteFile(openai_key) error: %v", err)
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	llm := &blockingInspectLLM{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	origFactory := localInspectLLMFactory
	origBudget := localInspectSendTimeoutBudget
	localInspectLLMFactory = func(_ ManagedAgentRecord, _ RuntimeConfig) (ChatLLM, error) { return llm, nil }
	localInspectSendTimeoutBudget = 25 * time.Millisecond
	t.Cleanup(func() {
		localInspectLLMFactory = origFactory
		localInspectSendTimeoutBudget = origBudget
		select {
		case <-llm.release:
		default:
			close(llm.release)
		}
	})

	mux := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected create local chat to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Session LocalChatSession `json:"session"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create local chat response: %v", err)
	}

	sendRec := httptest.NewRecorder()
	sendReq := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(`{"content":"hello timeout"}`))
	sendReq.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(sendRec, sendReq)
	if sendRec.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected inspect send timeout, got %d body=%s", sendRec.Code, sendRec.Body.String())
	}
	var timeoutResp struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Session  LocalChatSession          `json:"session"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(sendRec.Body.Bytes(), &timeoutResp); err != nil {
		t.Fatalf("decode timeout response: %v", err)
	}
	if !strings.Contains(timeoutResp.Error, "timed out") {
		t.Fatalf("expected timeout error body, got %+v", timeoutResp)
	}
	if timeoutResp.Contract.ChannelMode != "manager_mediated_inspect" || timeoutResp.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected timeout response to preserve inspect contract, got %+v", timeoutResp.Contract)
	}
	if timeoutResp.Session.ChatID != created.Session.ChatID || len(timeoutResp.Session.Messages) != 0 {
		t.Fatalf("expected timeout response to preserve unchanged session state, got %+v", timeoutResp.Session)
	}
	if len(timeoutResp.Sessions) != 1 || timeoutResp.Sessions[0].ChatID != created.Session.ChatID {
		t.Fatalf("expected timeout response to include current session inventory, got %+v", timeoutResp.Sessions)
	}
	if timeoutResp.Process.Running {
		t.Fatalf("expected timeout response to preserve stopped process state, got %+v", timeoutResp.Process)
	}
	if timeoutResp.Live.Error == "" && timeoutResp.Live.Status == "" && timeoutResp.Live.ProcessState == "" {
		t.Fatalf("expected timeout response to include current live snapshot, got %+v", timeoutResp.Live)
	}
	if timeoutResp.Catalog.Error == "" && len(timeoutResp.Catalog.Tasks) == 0 && len(timeoutResp.Catalog.Tensions) == 0 {
		t.Fatalf("expected timeout response to include current catalog snapshot, got %+v", timeoutResp.Catalog)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	stored, err := getLocalChat(managerDir, created.Session.ChatID)
	if err != nil {
		t.Fatalf("getLocalChat() error: %v", err)
	}
	if len(stored.Messages) != 0 {
		t.Fatalf("expected timed-out inspect send to leave transcript unchanged, got %+v", stored.Messages)
	}

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID, nil)
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected get local chat after timeout to succeed, got %d body=%s", getRec.Code, getRec.Body.String())
	}
	var fetched struct {
		Contract LocalChatContract `json:"contract"`
		Session  LocalChatSession  `json:"session"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("decode get after timeout response: %v", err)
	}
	if fetched.Contract.ExecutionState != "idle" || fetched.Session.Contract.ExecutionState != "idle" {
		t.Fatalf("expected timeout cleanup to restore idle execution state, got contract=%+v session=%+v", fetched.Contract, fetched.Session.Contract)
	}
}

func TestManagerWebLocalChatRejectsSaveWhenTranscriptDeletedDuringExecution(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     workdir,
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	mux := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected trusted local chat create to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Session LocalChatSession `json:"session"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create local chat response: %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	chatPath := filepath.Join(managerDir, created.Session.ChatID+".json")

	origFactory := localInspectLLMFactory
	origRunner := localInspectToolLoopRunner
	origBudget := localInspectSendTimeoutBudget
	localInspectLLMFactory = func(_ ManagedAgentRecord, _ RuntimeConfig) (ChatLLM, error) { return &sequenceLLM{}, nil }
	localInspectToolLoopRunner = func(_ context.Context, _ ChatLLM, _ *ToolRegistry, _ []Message, _ ToolLoopExecutor, _ ToolLoopObserver) (*ToolLoopRun, error) {
		if err := os.Remove(chatPath); err != nil {
			t.Fatalf("Remove(chatPath) error: %v", err)
		}
		return &ToolLoopRun{Content: "reply that should not be saved"}, nil
	}
	localInspectSendTimeoutBudget = 2 * time.Second
	t.Cleanup(func() {
		localInspectLLMFactory = origFactory
		localInspectToolLoopRunner = origRunner
		localInspectSendTimeoutBudget = origBudget
	})

	sendRec := httptest.NewRecorder()
	sendReq := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(`{"content":"hello"}`))
	sendReq.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(sendRec, sendReq)
	if sendRec.Code != http.StatusConflict {
		t.Fatalf("expected save conflict after out-of-band chat delete, got %d body=%s", sendRec.Code, sendRec.Body.String())
	}
	var changedResp struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Session  *LocalChatSession         `json:"session"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(sendRec.Body.Bytes(), &changedResp); err != nil {
		t.Fatalf("decode changed-during-execution response: %v", err)
	}
	if !strings.Contains(changedResp.Error, "changed during execution") {
		t.Fatalf("expected changed-during-execution error, got %+v", changedResp)
	}
	if changedResp.Contract.ChannelMode != "manager_mediated_inspect" || changedResp.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected changed-during-execution response to preserve inspect contract, got %+v", changedResp.Contract)
	}
	if changedResp.Session != nil {
		t.Fatalf("expected deleted transcript conflict to omit stale session payload, got %+v", changedResp.Session)
	}
	if len(changedResp.Sessions) != 0 {
		t.Fatalf("expected deleted transcript conflict to show empty current inventory, got %+v", changedResp.Sessions)
	}
	if changedResp.Process.Running {
		t.Fatalf("expected changed-during-execution response to preserve stopped process state, got %+v", changedResp.Process)
	}
	if changedResp.Live.Error == "" && changedResp.Live.Status == "" && changedResp.Live.ProcessState == "" {
		t.Fatalf("expected changed-during-execution response to include current live snapshot, got %+v", changedResp.Live)
	}
	if changedResp.Catalog.Error == "" && len(changedResp.Catalog.Tasks) == 0 && len(changedResp.Catalog.Tensions) == 0 {
		t.Fatalf("expected changed-during-execution response to include current catalog snapshot, got %+v", changedResp.Catalog)
	}
	if _, err := os.Stat(chatPath); !os.IsNotExist(err) {
		t.Fatalf("expected deleted transcript to stay deleted after failed final save, got err=%v", err)
	}
}

func TestManagerWebLocalChatTimeoutAfterToolUsePersistsFailureAudit(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     workdir,
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	origFactory := localInspectLLMFactory
	origRunner := localInspectToolLoopRunner
	origBudget := localInspectSendTimeoutBudget
	localInspectLLMFactory = func(_ ManagedAgentRecord, _ RuntimeConfig) (ChatLLM, error) { return &sequenceLLM{}, nil }
	localInspectToolLoopRunner = func(ctx context.Context, _ ChatLLM, registry *ToolRegistry, _ []Message, _ ToolLoopExecutor, observer ToolLoopObserver) (*ToolLoopRun, error) {
		call := ToolCall{
			ID:   "call-1",
			Type: "function",
			Function: FunctionCall{
				Name:      "list_directory",
				Arguments: `{"path":"."}`,
			},
		}
		result := defaultToolLoopExecutor(ctx, registry, call)
		if observer != nil {
			observer.OnToolResult(0, call, result)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	localInspectSendTimeoutBudget = 25 * time.Millisecond
	t.Cleanup(func() {
		localInspectLLMFactory = origFactory
		localInspectToolLoopRunner = origRunner
		localInspectSendTimeoutBudget = origBudget
	})

	mux := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected trusted local chat create to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Session LocalChatSession `json:"session"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create local chat response: %v", err)
	}

	sendRec := httptest.NewRecorder()
	sendReq := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(`{"content":"inspect the directory and report back"}`))
	sendReq.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(sendRec, sendReq)
	if sendRec.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected inspect send timeout after tool use, got %d body=%s", sendRec.Code, sendRec.Body.String())
	}
	var timeoutAfterToolResp struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Session  LocalChatSession          `json:"session"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(sendRec.Body.Bytes(), &timeoutAfterToolResp); err != nil {
		t.Fatalf("decode timeout after tool use response: %v", err)
	}
	if !strings.Contains(timeoutAfterToolResp.Error, "timed out") {
		t.Fatalf("expected timeout after tool use error, got %+v", timeoutAfterToolResp)
	}
	if timeoutAfterToolResp.Session.ChatID != created.Session.ChatID || len(timeoutAfterToolResp.Session.Messages) != 2 {
		t.Fatalf("expected timeout after tool use response to preserve persisted failure session, got %+v", timeoutAfterToolResp.Session)
	}
	if len(timeoutAfterToolResp.Sessions) != 1 || timeoutAfterToolResp.Sessions[0].ChatID != created.Session.ChatID {
		t.Fatalf("expected timeout after tool use response to include current session inventory, got %+v", timeoutAfterToolResp.Sessions)
	}
	if timeoutAfterToolResp.Session.Messages[1].Execution == nil || len(timeoutAfterToolResp.Session.Messages[1].Execution.ToolsUsed) != 1 {
		t.Fatalf("expected timeout after tool use response to include failure audit tool evidence, got %+v", timeoutAfterToolResp.Session.Messages[1].Execution)
	}
	if timeoutAfterToolResp.Process.Running {
		t.Fatalf("expected timeout after tool use response to preserve stopped process state, got %+v", timeoutAfterToolResp.Process)
	}
	if timeoutAfterToolResp.Live.Error == "" && timeoutAfterToolResp.Live.Status == "" && timeoutAfterToolResp.Live.ProcessState == "" {
		t.Fatalf("expected timeout after tool use response to include current live snapshot, got %+v", timeoutAfterToolResp.Live)
	}
	if timeoutAfterToolResp.Catalog.Error == "" && len(timeoutAfterToolResp.Catalog.Tasks) == 0 && len(timeoutAfterToolResp.Catalog.Tensions) == 0 {
		t.Fatalf("expected timeout after tool use response to include current catalog snapshot, got %+v", timeoutAfterToolResp.Catalog)
	}

	if _, err := os.Stat(filepath.Join(workdir, "inspect-side-effect.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected read-only inspect timeout not to create side effect file, got %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	stored, err := getLocalChat(managerDir, created.Session.ChatID)
	if err != nil {
		t.Fatalf("getLocalChat() error: %v", err)
	}
	if len(stored.Messages) != 2 {
		t.Fatalf("expected timeout after tool use to persist user turn plus failure audit, got %+v", stored.Messages)
	}
	if stored.Messages[0].Content != "inspect the directory and report back" {
		t.Fatalf("expected first stored message to preserve operator request, got %+v", stored.Messages[0])
	}
	if stored.Messages[1].Origin != "manager_inspect" || !strings.Contains(stored.Messages[1].Content, "timed out") {
		t.Fatalf("expected second stored message to persist inspect timeout audit, got %+v", stored.Messages[1])
	}
	if stored.Messages[1].Execution == nil || len(stored.Messages[1].Execution.ToolsUsed) != 1 {
		t.Fatalf("expected failure audit to persist tools_used evidence, got %+v", stored.Messages[1].Execution)
	}
	if stored.Messages[1].Execution.OverrideReason != "" {
		t.Fatalf("expected failure audit to omit override reason for default trusted inspect, got %+v", stored.Messages[1].Execution)
	}
	if stored.Messages[1].Execution.ToolsUsed[0].Name != "list_directory" || stored.Messages[1].Execution.ToolsUsed[0].Status != "ok" {
		t.Fatalf("expected persisted tool-use evidence for timed-out inspect, got %+v", stored.Messages[1].Execution.ToolsUsed)
	}
}

func TestManagerWebLocalChatFailureAuditSaveErrorFailsClosedAfterToolUse(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     workdir,
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	origFactory := localInspectLLMFactory
	origRunner := localInspectToolLoopRunner
	origBudget := localInspectSendTimeoutBudget
	localInspectLLMFactory = func(_ ManagedAgentRecord, _ RuntimeConfig) (ChatLLM, error) { return &sequenceLLM{}, nil }
	localInspectToolLoopRunner = func(ctx context.Context, _ ChatLLM, registry *ToolRegistry, _ []Message, _ ToolLoopExecutor, observer ToolLoopObserver) (*ToolLoopRun, error) {
		call := ToolCall{
			ID:   "call-1",
			Type: "function",
			Function: FunctionCall{
				Name:      "list_directory",
				Arguments: `{"path":"."}`,
			},
		}
		result := defaultToolLoopExecutor(ctx, registry, call)
		if observer != nil {
			observer.OnToolResult(0, call, result)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	localInspectSendTimeoutBudget = 25 * time.Millisecond
	t.Cleanup(func() {
		localInspectLLMFactory = origFactory
		localInspectToolLoopRunner = origRunner
		localInspectSendTimeoutBudget = origBudget
	})

	mux := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected trusted local chat create to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Session LocalChatSession `json:"session"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create local chat response: %v", err)
	}

	origSaveCurrent := localChatSaveCurrentFn
	localChatSaveCurrentFn = func(_ ManagedAgentRecord, _ string, _ *LocalChatSession, _ string) error {
		return errors.New("disk full")
	}
	t.Cleanup(func() {
		localChatSaveCurrentFn = origSaveCurrent
	})

	sendRec := httptest.NewRecorder()
	sendReq := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(`{"content":"inspect the directory and report back"}`))
	sendReq.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(sendRec, sendReq)
	if sendRec.Code != http.StatusInternalServerError {
		t.Fatalf("expected inspect audit persistence failure after tool use, got %d body=%s", sendRec.Code, sendRec.Body.String())
	}
	var auditSaveFailureResp struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Session  LocalChatSession          `json:"session"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(sendRec.Body.Bytes(), &auditSaveFailureResp); err != nil {
		t.Fatalf("decode audit persistence failure response: %v", err)
	}
	if !strings.Contains(auditSaveFailureResp.Error, "inspect audit persistence failed after tool execution") {
		t.Fatalf("expected audit persistence failure body, got %+v", auditSaveFailureResp)
	}
	if auditSaveFailureResp.Contract.ChannelMode != "manager_mediated_inspect" || auditSaveFailureResp.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected audit persistence failure response to preserve inspect contract, got %+v", auditSaveFailureResp.Contract)
	}
	if auditSaveFailureResp.Session.ChatID != created.Session.ChatID || len(auditSaveFailureResp.Session.Messages) != 0 {
		t.Fatalf("expected audit persistence failure response to preserve unchanged persisted session, got %+v", auditSaveFailureResp.Session)
	}
	if len(auditSaveFailureResp.Sessions) != 1 || auditSaveFailureResp.Sessions[0].ChatID != created.Session.ChatID {
		t.Fatalf("expected audit persistence failure response to include current session inventory, got %+v", auditSaveFailureResp.Sessions)
	}
	if auditSaveFailureResp.Process.Running {
		t.Fatalf("expected audit persistence failure response to preserve stopped process state, got %+v", auditSaveFailureResp.Process)
	}
	if auditSaveFailureResp.Live.Error == "" && auditSaveFailureResp.Live.Status == "" && auditSaveFailureResp.Live.ProcessState == "" {
		t.Fatalf("expected audit persistence failure response to include current live snapshot, got %+v", auditSaveFailureResp.Live)
	}
	if auditSaveFailureResp.Catalog.Error == "" && len(auditSaveFailureResp.Catalog.Tasks) == 0 && len(auditSaveFailureResp.Catalog.Tensions) == 0 {
		t.Fatalf("expected audit persistence failure response to include current catalog snapshot, got %+v", auditSaveFailureResp.Catalog)
	}

	if _, err := os.Stat(filepath.Join(workdir, "inspect-side-effect.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected read-only inspect audit failure not to create side effect file, got %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	stored, err := getLocalChat(managerDir, created.Session.ChatID)
	if err != nil {
		t.Fatalf("getLocalChat() error: %v", err)
	}
	if len(stored.Messages) != 0 {
		t.Fatalf("expected failed audit persistence to leave transcript unchanged, got %+v", stored.Messages)
	}
}

func TestManagerWebTrustedInspectStaysReadOnlyWithoutQuarantine(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	origFactory := localInspectLLMFactory
	origRunner := localInspectToolLoopRunner
	localInspectLLMFactory = func(_ ManagedAgentRecord, _ RuntimeConfig) (ChatLLM, error) { return &sequenceLLM{}, nil }
	step := 0
	localInspectToolLoopRunner = func(_ context.Context, _ ChatLLM, registry *ToolRegistry, _ []Message, _ ToolLoopExecutor, _ ToolLoopObserver) (*ToolLoopRun, error) {
		step++
		switch step {
		case 1:
			if _, ok := registry.Get("write_file"); ok {
				t.Fatalf("expected trusted inspect send to omit write_file")
			}
			if _, ok := registry.Get("shell"); ok {
				t.Fatalf("expected trusted inspect send to omit shell")
			}
			return &ToolLoopRun{Content: "trusted reply 1"}, nil
		case 2:
			if _, ok := registry.Get("write_file"); ok {
				t.Fatalf("expected trusted follow-up send to omit write_file")
			}
			if _, ok := registry.Get("shell"); ok {
				t.Fatalf("expected trusted follow-up send to omit shell")
			}
			return &ToolLoopRun{Content: "trusted reply 2"}, nil
		default:
			return &ToolLoopRun{Content: "unexpected extra reply"}, nil
		}
	}
	t.Cleanup(func() {
		localInspectLLMFactory = origFactory
		localInspectToolLoopRunner = origRunner
	})

	mux := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected trusted local chat create to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Contract LocalChatContract `json:"contract"`
		Session  LocalChatSession  `json:"session"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create local chat response: %v", err)
	}
	if created.Contract.ShellAllowed || created.Contract.MutationAllowed {
		t.Fatalf("expected trusted inspect contract to stay read-only, got %+v", created.Contract)
	}
	if created.Contract.OverridePolicy != "" || created.Contract.OverrideCanMutation || created.Contract.OverrideCanShell {
		t.Fatalf("expected trusted inspect contract to hide explicit per-send override controls, got %+v", created.Contract)
	}
	if created.Contract.FirstDeploymentPreflight != "excluded_read_only_non_daemon" || created.Contract.DeploymentAuthority != "not_daemon_deployment_authority" {
		t.Fatalf("expected trusted inspect contract to report non-deployment read-only boundary, got %+v", created.Contract)
	}

	for _, rejectCase := range []struct {
		body        string
		wantSnippet string
	}{
		{body: `{"content":"edit this","allow_mutation":true,"override_reason":"manual escalation"}`, wantSnippet: "mutation override not allowed"},
		{body: `{"content":"open shell","allow_shell":true,"allow_mutation":true,"override_reason":"manual escalation"}`, wantSnippet: "shell override not allowed"},
		{body: `{"content":"status?","override_reason":"because"}`, wantSnippet: "override reason requires mutation or shell override"},
	} {
		rejectRec := httptest.NewRecorder()
		rejectReq := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(rejectCase.body))
		rejectReq.Header.Set("Content-Type", "application/json")
		mux.ServeHTTP(rejectRec, rejectReq)
		if rejectRec.Code != http.StatusBadRequest && rejectRec.Code != http.StatusForbidden {
			t.Fatalf("expected trusted inspect explicit override reject for body %s, got %d body=%s", rejectCase.body, rejectRec.Code, rejectRec.Body.String())
		}
		if !strings.Contains(rejectRec.Body.String(), rejectCase.wantSnippet) {
			t.Fatalf("expected reject body for %s to mention %q, got %s", rejectCase.body, rejectCase.wantSnippet, rejectRec.Body.String())
		}
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	unchanged, err := getLocalChat(managerDir, created.Session.ChatID)
	if err != nil {
		t.Fatalf("getLocalChat() error: %v", err)
	}
	if len(unchanged.Messages) != 0 {
		t.Fatalf("expected rejected trusted inspect override sends to leave transcript unchanged, got %+v", unchanged.Messages)
	}

	sendAndDecode := func(chatID, body string) struct {
		Contract LocalChatContract         `json:"contract"`
		Session  LocalChatSession          `json:"session"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	} {
		sendRec := httptest.NewRecorder()
		sendReq := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/"+chatID+"/message", strings.NewReader(body))
		sendReq.Header.Set("Content-Type", "application/json")
		mux.ServeHTTP(sendRec, sendReq)
		if sendRec.Code != http.StatusOK {
			t.Fatalf("inspect send failed for chat %s body %s: %d body=%s", chatID, body, sendRec.Code, sendRec.Body.String())
		}
		var sent struct {
			Contract LocalChatContract         `json:"contract"`
			Session  LocalChatSession          `json:"session"`
			Sessions []LocalChatSession        `json:"sessions"`
			Process  ManagedAgentProcessStatus `json:"process"`
			Live     managerLiveRuntimeStatus  `json:"live"`
			Catalog  managerWorkspaceCatalog   `json:"catalog"`
		}
		if err := json.Unmarshal(sendRec.Body.Bytes(), &sent); err != nil {
			t.Fatalf("decode inspect send response: %v", err)
		}
		return sent
	}

	firstSent := sendAndDecode(created.Session.ChatID, `{"content":"status?"}`)
	if firstSent.Contract.ShellAllowed || firstSent.Contract.MutationAllowed {
		t.Fatalf("expected trusted chat contract to remain read-only after send, got %+v", firstSent.Contract)
	}
	firstLast := firstSent.Session.Messages[len(firstSent.Session.Messages)-1]
	if firstLast.Content != "trusted reply 1" || firstLast.Execution == nil {
		t.Fatalf("expected trusted inspect reply with execution snapshot, got %+v", firstLast)
	}
	if firstLast.Execution.OverrideMode != "default_read_only" || firstLast.Execution.ToolScope != "read_only_inspect_no_shell" {
		t.Fatalf("expected trusted inspect snapshot to reflect read-only execution, got %+v", firstLast.Execution)
	}
	if firstLast.Execution.OverrideReason != "" {
		t.Fatalf("expected default trusted inspect snapshot to omit override reason, got %+v", firstLast.Execution)
	}
	if firstSent.Session.HasPrivilegedTurns || firstSent.Session.SessionMode != "read_only_inspect" || firstSent.Session.SendPolicy != "default_read_only" {
		t.Fatalf("expected trusted chat to stay read-only without privileged-history quarantine, got %+v", firstSent.Session)
	}
	if firstSent.Session.RetentionMode != "" || firstSent.Session.DeletePolicy != "normal_delete_allowed" || firstSent.Session.DeleteBlockedReason != "" {
		t.Fatalf("expected trusted chat to stay normally deletable, got %+v", firstSent.Session)
	}
	if len(firstSent.Sessions) != 1 || firstSent.Sessions[0].ChatID != created.Session.ChatID || firstSent.Sessions[0].SessionMode != "read_only_inspect" {
		t.Fatalf("expected trusted send response to include refreshed read-only session inventory, got %+v", firstSent.Sessions)
	}
	if firstSent.Process.Running {
		t.Fatalf("expected send local chat response to preserve stopped process state, got %+v", firstSent.Process)
	}
	if firstSent.Live.Error == "" && firstSent.Live.Status == "" && firstSent.Live.ProcessState == "" {
		t.Fatalf("expected send local chat response to include current live snapshot, got %+v", firstSent.Live)
	}
	if firstSent.Catalog.Error == "" && len(firstSent.Catalog.Tasks) == 0 && len(firstSent.Catalog.Tensions) == 0 {
		t.Fatalf("expected send local chat response to include current catalog snapshot, got %+v", firstSent.Catalog)
	}

	secondSent := sendAndDecode(created.Session.ChatID, `{"content":"follow up"}`)
	secondLast := secondSent.Session.Messages[len(secondSent.Session.Messages)-1]
	if secondLast.Content != "trusted reply 2" || secondLast.Execution == nil {
		t.Fatalf("expected trusted follow-up reply with execution snapshot, got %+v", secondLast)
	}
	if secondSent.Session.HasPrivilegedTurns || secondSent.Session.SessionMode != "read_only_inspect" || secondSent.Session.SendPolicy != "default_read_only" {
		t.Fatalf("expected trusted follow-up to stay read-only on the same chat, got %+v", secondSent.Session)
	}
	if len(secondSent.Sessions) != 1 || secondSent.Sessions[0].ChatID != created.Session.ChatID {
		t.Fatalf("expected trusted follow-up to keep a single active chat inventory, got %+v", secondSent.Sessions)
	}
	stored, err := getLocalChat(managerDir, created.Session.ChatID)
	if err != nil {
		t.Fatalf("getLocalChat(trusted) error: %v", err)
	}
	if len(stored.Messages) != len(secondSent.Session.Messages) {
		t.Fatalf("expected trusted transcript to persist both active sends, got %+v", stored.Messages)
	}

	deleteRec := httptest.NewRecorder()
	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/agents/trusted-agent/local_chats/"+created.Session.ChatID, nil)
	mux.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("expected trusted inspect delete to succeed for a normal active chat, got %d body=%s", deleteRec.Code, deleteRec.Body.String())
	}
	if _, err := getLocalChat(managerDir, created.Session.ChatID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected trusted chat transcript to be deleted cleanly, got %v", err)
	}

	listRec := httptest.NewRecorder()
	listReq := httptest.NewRequest(http.MethodGet, "/api/agents/trusted-agent/local_chats", nil)
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected local chat list to succeed, got %d body=%s", listRec.Code, listRec.Body.String())
	}
	var listed struct {
		Sessions []LocalChatSession `json:"sessions"`
	}
	if err := json.Unmarshal(listRec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode local chat list response: %v", err)
	}
	if len(listed.Sessions) != 0 {
		t.Fatalf("expected trusted chat list to be empty after normal delete, got %+v", listed.Sessions)
	}
}

func TestManagerWebDeleteRetainsLegacyManagerInspectTranscriptWithoutExecutionSnapshot(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	session := &LocalChatSession{
		ChatID: "legacy-chat",
		Title:  "Legacy Inspect Chat",
		Messages: []LocalChatMessage{
			{
				Role:    "agent",
				Content: "legacy inspect reply without execution snapshot",
			},
		},
	}
	if err := saveLocalChatForRecord(record, managerDir, session); err != nil {
		t.Fatalf("saveLocalChatForRecord() error: %v", err)
	}

	mux := newManagerWebServer().routes()
	deleteRec := httptest.NewRecorder()
	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/agents/trusted-agent/local_chats/legacy-chat", nil)
	mux.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusConflict {
		t.Fatalf("expected legacy manager inspect transcript delete to require retention, got %d body=%s", deleteRec.Code, deleteRec.Body.String())
	}
	var deleteRejected struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Session  LocalChatSession          `json:"session"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(deleteRec.Body.Bytes(), &deleteRejected); err != nil {
		t.Fatalf("decode legacy delete reject response: %v", err)
	}
	if !strings.Contains(deleteRejected.Error, "legacy manager-inspect history requires audit retention") {
		t.Fatalf("expected legacy audit-retention error, got %+v", deleteRejected)
	}
	if deleteRejected.Session.ChatID != "legacy-chat" || deleteRejected.Session.DeleteBlockedReason != "legacy_manager_inspect_history_requires_retention" {
		t.Fatalf("expected legacy delete reject to preserve current session state, got %+v", deleteRejected.Session)
	}
	if len(deleteRejected.Sessions) != 1 || deleteRejected.Sessions[0].ChatID != "legacy-chat" {
		t.Fatalf("expected legacy delete reject to include refreshed session inventory, got %+v", deleteRejected.Sessions)
	}
	if deleteRejected.Process.Running {
		t.Fatalf("expected legacy delete reject to preserve stopped process state, got %+v", deleteRejected.Process)
	}
	if deleteRejected.Live.Error == "" && deleteRejected.Live.Status == "" && deleteRejected.Live.ProcessState == "" {
		t.Fatalf("expected legacy delete reject to include current live snapshot, got %+v", deleteRejected.Live)
	}
	if deleteRejected.Catalog.Error == "" && len(deleteRejected.Catalog.Tasks) == 0 && len(deleteRejected.Catalog.Tensions) == 0 {
		t.Fatalf("expected legacy delete reject to include current catalog snapshot, got %+v", deleteRejected.Catalog)
	}

	stored, err := getLocalChat(managerDir, "legacy-chat")
	if err != nil {
		t.Fatalf("expected legacy transcript to remain after delete reject, got %v", err)
	}
	normalizeLocalChatSessionForRecord(record, stored)
	if stored.RetentionMode != "audit_retained_legacy_manager_inspect_history" || stored.DeletePolicy != "delete_blocked_legacy_audit_retention" || stored.DeleteBlockedReason != "legacy_manager_inspect_history_requires_retention" {
		t.Fatalf("expected legacy transcript to remain audit-retained, got %+v", stored)
	}
}

func TestManagerWebArchiveRetainedInspectChatKeepsTranscriptVisibleAndRejectsFollowUpAndDelete(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	session := &LocalChatSession{
		ChatID: "retained-chat",
		Title:  "Retained Inspect Chat",
		Messages: []LocalChatMessage{
			{Role: "user", Origin: "operator", Content: "edit this"},
			{
				Role:    "agent",
				Origin:  "manager_inspect",
				Content: "mutation reply",
				Execution: &LocalChatExecutionSnapshot{
					OverrideMode:    "operator_override_mutation",
					OverrideReason:  "Need bounded local repair",
					ToolScope:       "bounded_mutation_no_shell",
					MutationAllowed: boolPtr(true),
					ShellAllowed:    boolPtr(false),
				},
			},
		},
	}
	if err := saveLocalChatForRecord(record, managerDir, session); err != nil {
		t.Fatalf("saveLocalChatForRecord() error: %v", err)
	}

	mux := newManagerWebServer().routes()

	archiveRec := httptest.NewRecorder()
	archiveReq := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/retained-chat/archive", nil)
	mux.ServeHTTP(archiveRec, archiveReq)
	if archiveRec.Code != http.StatusOK {
		t.Fatalf("expected archive retained inspect chat to succeed, got %d body=%s", archiveRec.Code, archiveRec.Body.String())
	}
	var archived struct {
		Contract LocalChatContract         `json:"contract"`
		Session  LocalChatSession          `json:"session"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(archiveRec.Body.Bytes(), &archived); err != nil {
		t.Fatalf("decode archive response: %v", err)
	}
	if archived.Contract.ChannelMode != "manager_mediated_inspect" || archived.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected archive response to preserve inspect contract, got %+v", archived.Contract)
	}
	if archived.Session.ArchiveState != "retained_archived" || strings.TrimSpace(archived.Session.ArchivedAt) == "" {
		t.Fatalf("expected archived retained chat state, got %+v", archived.Session)
	}
	if archived.Session.RetentionMode != "audit_retained_privileged_history" || archived.Session.DeletePolicy != "delete_blocked_audit_retention" {
		t.Fatalf("expected archive to preserve retention/delete truth, got %+v", archived.Session)
	}
	if archived.Session.SessionMode != "archived_retained_inspect" || archived.Session.SendPolicy != "archived_retained_history_only" {
		t.Fatalf("expected archive to freeze chat into archived retained mode, got %+v", archived.Session)
	}
	if len(archived.Sessions) != 1 || archived.Sessions[0].ChatID != "retained-chat" || archived.Sessions[0].ArchiveState != "retained_archived" {
		t.Fatalf("expected archive response to include refreshed retained session inventory, got %+v", archived.Sessions)
	}
	if archived.Process.Running {
		t.Fatalf("expected archive response to preserve stopped process state, got %+v", archived.Process)
	}
	if archived.Live.Error == "" && archived.Live.Status == "" && archived.Live.ProcessState == "" {
		t.Fatalf("expected archive response to include current live snapshot, got %+v", archived.Live)
	}
	if archived.Catalog.Error == "" && len(archived.Catalog.Tasks) == 0 && len(archived.Catalog.Tensions) == 0 {
		t.Fatalf("expected archive response to include current catalog snapshot, got %+v", archived.Catalog)
	}

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/api/agents/trusted-agent/local_chats/retained-chat", nil)
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected archived retained get to stay visible, got %d body=%s", getRec.Code, getRec.Body.String())
	}
	var fetched struct {
		Session  LocalChatSession   `json:"session"`
		Sessions []LocalChatSession `json:"sessions"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("decode archived get response: %v", err)
	}
	if fetched.Session.ArchiveState != "retained_archived" {
		t.Fatalf("expected archived retained chat to stay visible on get, got %+v", fetched.Session)
	}
	if len(fetched.Sessions) != 1 || fetched.Sessions[0].ChatID != "retained-chat" || fetched.Sessions[0].ArchiveState != "retained_archived" {
		t.Fatalf("expected archived get response to include refreshed retained session inventory, got %+v", fetched.Sessions)
	}

	listRec := httptest.NewRecorder()
	listReq := httptest.NewRequest(http.MethodGet, "/api/agents/trusted-agent/local_chats", nil)
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected archived retained list to succeed, got %d body=%s", listRec.Code, listRec.Body.String())
	}
	var listed struct {
		Sessions []LocalChatSession `json:"sessions"`
	}
	if err := json.Unmarshal(listRec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listed.Sessions) != 1 || listed.Sessions[0].ArchiveState != "retained_archived" {
		t.Fatalf("expected archived retained chat to remain list-visible, got %+v", listed.Sessions)
	}

	sendReadOnlyRec := httptest.NewRecorder()
	sendReadOnlyReq := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/retained-chat/message", strings.NewReader(`{"content":"status?"}`))
	sendReadOnlyReq.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(sendReadOnlyRec, sendReadOnlyReq)
	if sendReadOnlyRec.Code != http.StatusConflict {
		t.Fatalf("expected archived retained chat to reject read-only follow-up, got %d body=%s", sendReadOnlyRec.Code, sendReadOnlyRec.Body.String())
	}
	if !strings.Contains(sendReadOnlyRec.Body.String(), "archived for retained audit") {
		t.Fatalf("expected archived retained send error, got %s", sendReadOnlyRec.Body.String())
	}

	sendPrivilegedRec := httptest.NewRecorder()
	sendPrivilegedReq := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/retained-chat/message", strings.NewReader(`{"content":"open shell","allow_shell":true,"override_reason":"Need shell"}`))
	sendPrivilegedReq.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(sendPrivilegedRec, sendPrivilegedReq)
	if sendPrivilegedRec.Code != http.StatusConflict {
		t.Fatalf("expected archived retained chat to reject privileged follow-up, got %d body=%s", sendPrivilegedRec.Code, sendPrivilegedRec.Body.String())
	}

	deleteRec := httptest.NewRecorder()
	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/agents/trusted-agent/local_chats/retained-chat", nil)
	mux.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusConflict {
		t.Fatalf("expected archived retained delete to stay blocked, got %d body=%s", deleteRec.Code, deleteRec.Body.String())
	}
	var archivedDeleteRejected struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Session  LocalChatSession          `json:"session"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(deleteRec.Body.Bytes(), &archivedDeleteRejected); err != nil {
		t.Fatalf("decode archived delete reject response: %v", err)
	}
	if !strings.Contains(archivedDeleteRejected.Error, "privileged history requires audit retention") {
		t.Fatalf("expected archived delete reject to preserve audit-retention reason, got %+v", archivedDeleteRejected)
	}
	if archivedDeleteRejected.Session.ChatID != "retained-chat" || archivedDeleteRejected.Session.ArchiveState != "retained_archived" {
		t.Fatalf("expected archived delete reject to preserve current retained session state, got %+v", archivedDeleteRejected.Session)
	}
	if len(archivedDeleteRejected.Sessions) != 1 || archivedDeleteRejected.Sessions[0].ChatID != "retained-chat" {
		t.Fatalf("expected archived delete reject to include refreshed session inventory, got %+v", archivedDeleteRejected.Sessions)
	}
	if archivedDeleteRejected.Process.Running {
		t.Fatalf("expected archived delete reject to preserve stopped process state, got %+v", archivedDeleteRejected.Process)
	}
	if archivedDeleteRejected.Live.Error == "" && archivedDeleteRejected.Live.Status == "" && archivedDeleteRejected.Live.ProcessState == "" {
		t.Fatalf("expected archived delete reject to include current live snapshot, got %+v", archivedDeleteRejected.Live)
	}
	if archivedDeleteRejected.Catalog.Error == "" && len(archivedDeleteRejected.Catalog.Tasks) == 0 && len(archivedDeleteRejected.Catalog.Tensions) == 0 {
		t.Fatalf("expected archived delete reject to include current catalog snapshot, got %+v", archivedDeleteRejected.Catalog)
	}

	stored, err := getLocalChat(managerDir, "retained-chat")
	if err != nil {
		t.Fatalf("expected archived retained transcript to remain stored, got %v", err)
	}
	normalizeLocalChatSessionForRecord(record, stored)
	if len(stored.Messages) != 2 || stored.ArchiveState != "retained_archived" {
		t.Fatalf("expected archived retained transcript to remain unchanged and archived, got %+v", stored)
	}
}

func TestManagerWebArchiveAlreadyArchivedInspectChatReturnsListErrorContext(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	session := &LocalChatSession{
		ChatID:       "retained-chat",
		Title:        "Retained Inspect Chat",
		ArchiveState: "retained_archived",
		ArchivedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Messages: []LocalChatMessage{
			{Role: "user", Origin: "operator", Content: "edit this"},
			{
				Role:    "agent",
				Origin:  "manager_inspect",
				Content: "mutation reply",
				Execution: &LocalChatExecutionSnapshot{
					OverrideMode:    "operator_override_mutation",
					OverrideReason:  "Need bounded local repair",
					ToolScope:       "bounded_mutation_no_shell",
					MutationAllowed: boolPtr(true),
					ShellAllowed:    boolPtr(false),
				},
			},
		},
	}
	if err := saveLocalChatForRecord(record, managerDir, session); err != nil {
		t.Fatalf("saveLocalChatForRecord() error: %v", err)
	}

	origListFn := localChatListFn
	localChatListFn = func(_ ManagedAgentRecord, dir string) ([]LocalChatSession, error) {
		sessions, _ := listLocalChats(record, dir)
		return sessions, errors.New("inventory warning")
	}
	t.Cleanup(func() {
		localChatListFn = origListFn
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/retained-chat/archive", nil)
	newManagerWebServer().routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected already-archived archive replay to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Contract  LocalChatContract  `json:"contract"`
		Session   LocalChatSession   `json:"session"`
		Sessions  []LocalChatSession `json:"sessions"`
		ListError string             `json:"list_error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode already-archived archive response: %v", err)
	}
	if resp.Session.ArchiveState != "retained_archived" {
		t.Fatalf("expected already-archived archive response to preserve retained state, got %+v", resp.Session)
	}
	if len(resp.Sessions) != 1 || resp.Sessions[0].ChatID != "retained-chat" {
		t.Fatalf("expected already-archived archive response to preserve session inventory, got %+v", resp.Sessions)
	}
	if !strings.Contains(resp.ListError, "inventory warning") {
		t.Fatalf("expected already-archived archive response to preserve list_error, got %+v", resp)
	}
}

func TestManagerWebArchiveRetainedInspectRejectsWhenTranscriptDeletedDuringArchive(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	session := &LocalChatSession{
		ChatID: "retained-chat",
		Title:  "Retained Inspect Chat",
		Messages: []LocalChatMessage{
			{Role: "user", Origin: "operator", Content: "edit this"},
			{
				Role:    "agent",
				Origin:  "manager_inspect",
				Content: "mutation reply",
				Execution: &LocalChatExecutionSnapshot{
					OverrideMode:    "operator_override_mutation",
					OverrideReason:  "Need bounded local repair",
					ToolScope:       "bounded_mutation_no_shell",
					MutationAllowed: boolPtr(true),
					ShellAllowed:    boolPtr(false),
				},
			},
		},
	}
	if err := saveLocalChatForRecord(record, managerDir, session); err != nil {
		t.Fatalf("saveLocalChatForRecord() error: %v", err)
	}
	chatPath := filepath.Join(managerDir, "retained-chat.json")

	origSaveCurrent := localChatSaveCurrentFn
	localChatSaveCurrentFn = func(_ ManagedAgentRecord, _ string, _ *LocalChatSession, _ string) error {
		if err := os.Remove(chatPath); err != nil {
			t.Fatalf("Remove(chatPath) error: %v", err)
		}
		return errLocalChatStateChanged
	}
	t.Cleanup(func() {
		localChatSaveCurrentFn = origSaveCurrent
	})

	mux := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/retained-chat/archive", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected archive conflict after out-of-band delete, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Error    string             `json:"error"`
		Contract LocalChatContract  `json:"contract"`
		Session  *LocalChatSession  `json:"session"`
		Sessions []LocalChatSession `json:"sessions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode archive changed response: %v", err)
	}
	if !strings.Contains(resp.Error, "changed during archive") {
		t.Fatalf("expected changed-during-archive error, got %+v", resp)
	}
	if resp.Contract.ChannelMode != "manager_mediated_inspect" || resp.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected archive changed response to preserve inspect contract, got %+v", resp.Contract)
	}
	if resp.Session != nil {
		t.Fatalf("expected deleted archive conflict to omit stale session payload, got %+v", resp.Session)
	}
	if len(resp.Sessions) != 0 {
		t.Fatalf("expected deleted archive conflict to show empty current inventory, got %+v", resp.Sessions)
	}
	if _, err := os.Stat(chatPath); !os.IsNotExist(err) {
		t.Fatalf("expected deleted transcript to stay deleted after archive conflict, got err=%v", err)
	}
}

func TestManagerWebArchiveRetainedInspectPersistenceFailureReturnsCurrentState(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	session := &LocalChatSession{
		ChatID: "retained-chat",
		Title:  "Retained Inspect Chat",
		Messages: []LocalChatMessage{
			{Role: "user", Origin: "operator", Content: "edit this"},
			{
				Role:    "agent",
				Origin:  "manager_inspect",
				Content: "mutation reply",
				Execution: &LocalChatExecutionSnapshot{
					OverrideMode:    "operator_override_mutation",
					OverrideReason:  "Need bounded local repair",
					ToolScope:       "bounded_mutation_no_shell",
					MutationAllowed: boolPtr(true),
					ShellAllowed:    boolPtr(false),
				},
			},
		},
	}
	if err := saveLocalChatForRecord(record, managerDir, session); err != nil {
		t.Fatalf("saveLocalChatForRecord() error: %v", err)
	}

	origSaveCurrent := localChatSaveCurrentFn
	localChatSaveCurrentFn = func(_ ManagedAgentRecord, _ string, _ *LocalChatSession, _ string) error {
		return errors.New("disk full")
	}
	t.Cleanup(func() {
		localChatSaveCurrentFn = origSaveCurrent
	})

	mux := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/retained-chat/archive", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected archive persistence failure, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Error    string             `json:"error"`
		Contract LocalChatContract  `json:"contract"`
		Session  LocalChatSession   `json:"session"`
		Sessions []LocalChatSession `json:"sessions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode archive persistence failure response: %v", err)
	}
	if !strings.Contains(resp.Error, "failed to archive inspect chat: disk full") {
		t.Fatalf("expected archive persistence failure error, got %+v", resp)
	}
	if resp.Contract.ChannelMode != "manager_mediated_inspect" || resp.Contract.RuntimeRelation != "not_live_managed_runtime" {
		t.Fatalf("expected archive persistence failure response to preserve inspect contract, got %+v", resp.Contract)
	}
	if resp.Session.ChatID != "retained-chat" || resp.Session.ArchiveState != "retained_active" {
		t.Fatalf("expected archive persistence failure response to preserve current retained-active session state, got %+v", resp.Session)
	}
	if len(resp.Sessions) != 1 || resp.Sessions[0].ChatID != "retained-chat" || resp.Sessions[0].ArchiveState != "retained_active" {
		t.Fatalf("expected archive persistence failure response to include current retained-active inventory, got %+v", resp.Sessions)
	}
}

func TestManagerWebArchiveRetainedInspectRejectsWhileBusy(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	session := &LocalChatSession{
		ChatID: "retained-chat",
		Title:  "Retained Inspect Chat",
		Messages: []LocalChatMessage{
			{Role: "user", Origin: "operator", Content: "edit this"},
			{
				Role:    "agent",
				Origin:  "manager_inspect",
				Content: "mutation reply",
				Execution: &LocalChatExecutionSnapshot{
					OverrideMode:    "operator_override_mutation",
					OverrideReason:  "Need bounded local repair",
					ToolScope:       "bounded_mutation_no_shell",
					MutationAllowed: boolPtr(true),
					ShellAllowed:    boolPtr(false),
				},
			},
		},
	}
	if err := saveLocalChatForRecord(record, managerDir, session); err != nil {
		t.Fatalf("saveLocalChatForRecord() error: %v", err)
	}

	localInspectSendInFlight.Store(localInspectExecutionKey(record), localInspectExecutionLease{ChatID: "retained-chat"})
	defer localInspectSendInFlight.Delete(localInspectExecutionKey(record))

	mux := newManagerWebServer().routes()
	archiveRec := httptest.NewRecorder()
	archiveReq := httptest.NewRequest(http.MethodPost, "/api/agents/trusted-agent/local_chats/retained-chat/archive", nil)
	mux.ServeHTTP(archiveRec, archiveReq)
	if archiveRec.Code != http.StatusConflict {
		t.Fatalf("expected archive to reject while inspect run is active, got %d body=%s", archiveRec.Code, archiveRec.Body.String())
	}
	var archiveRejected struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Session  LocalChatSession          `json:"session"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(archiveRec.Body.Bytes(), &archiveRejected); err != nil {
		t.Fatalf("decode archive busy reject response: %v", err)
	}
	if !strings.Contains(archiveRejected.Error, "archive blocked") {
		t.Fatalf("expected archive-blocked error body, got %+v", archiveRejected)
	}
	if archiveRejected.Session.ChatID != "retained-chat" || archiveRejected.Session.ArchiveState != "retained_active" {
		t.Fatalf("expected archive busy reject to preserve current retained session state, got %+v", archiveRejected.Session)
	}
	if len(archiveRejected.Sessions) != 1 || archiveRejected.Sessions[0].ChatID != "retained-chat" {
		t.Fatalf("expected archive busy reject to include refreshed session inventory, got %+v", archiveRejected.Sessions)
	}
	if archiveRejected.Process.Running {
		t.Fatalf("expected archive busy reject to preserve stopped process state, got %+v", archiveRejected.Process)
	}
	if archiveRejected.Live.Error == "" && archiveRejected.Live.Status == "" && archiveRejected.Live.ProcessState == "" {
		t.Fatalf("expected archive busy reject to include current live snapshot, got %+v", archiveRejected.Live)
	}
	if archiveRejected.Catalog.Error == "" && len(archiveRejected.Catalog.Tasks) == 0 && len(archiveRejected.Catalog.Tensions) == 0 {
		t.Fatalf("expected archive busy reject to include current catalog snapshot, got %+v", archiveRejected.Catalog)
	}

	stored, err := getLocalChat(managerDir, "retained-chat")
	if err != nil {
		t.Fatalf("expected retained transcript to remain after archive reject, got %v", err)
	}
	normalizeLocalChatSessionForRecord(record, stored)
	if stored.ArchiveState != "retained_active" || strings.TrimSpace(stored.ArchivedAt) != "" {
		t.Fatalf("expected archive reject to leave retained chat unarchived, got %+v", stored)
	}
}

func TestManagerWebDeleteInspectRejectsWhileBusy(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	session := &LocalChatSession{
		ChatID: "busy-delete-chat",
		Title:  "Busy Delete Chat",
		Messages: []LocalChatMessage{
			{Role: "user", Origin: "operator", Content: "status?"},
			{Role: "agent", Origin: "manager_inspect", Content: "still here"},
		},
	}
	if err := saveLocalChatForRecord(record, managerDir, session); err != nil {
		t.Fatalf("saveLocalChatForRecord() error: %v", err)
	}

	localInspectSendInFlight.Store(localInspectExecutionKey(record), localInspectExecutionLease{ChatID: "busy-delete-chat"})
	defer localInspectSendInFlight.Delete(localInspectExecutionKey(record))

	mux := newManagerWebServer().routes()
	deleteRec := httptest.NewRecorder()
	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/agents/trusted-agent/local_chats/busy-delete-chat", nil)
	mux.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusConflict {
		t.Fatalf("expected delete to reject while inspect run is active, got %d body=%s", deleteRec.Code, deleteRec.Body.String())
	}
	var deleteRejected struct {
		Error    string                    `json:"error"`
		Contract LocalChatContract         `json:"contract"`
		Session  LocalChatSession          `json:"session"`
		Sessions []LocalChatSession        `json:"sessions"`
		Process  ManagedAgentProcessStatus `json:"process"`
		Live     managerLiveRuntimeStatus  `json:"live"`
		Catalog  managerWorkspaceCatalog   `json:"catalog"`
	}
	if err := json.Unmarshal(deleteRec.Body.Bytes(), &deleteRejected); err != nil {
		t.Fatalf("decode delete busy reject response: %v", err)
	}
	if !strings.Contains(deleteRejected.Error, "delete blocked") {
		t.Fatalf("expected delete-blocked error body, got %+v", deleteRejected)
	}
	if deleteRejected.Session.ChatID != "busy-delete-chat" || deleteRejected.Session.SessionMode != "read_only_inspect" {
		t.Fatalf("expected delete busy reject to preserve current session state, got %+v", deleteRejected.Session)
	}
	if len(deleteRejected.Sessions) != 1 || deleteRejected.Sessions[0].ChatID != "busy-delete-chat" {
		t.Fatalf("expected delete busy reject to include refreshed session inventory, got %+v", deleteRejected.Sessions)
	}
	if deleteRejected.Process.Running {
		t.Fatalf("expected delete busy reject to preserve stopped process state, got %+v", deleteRejected.Process)
	}
	if deleteRejected.Live.Error == "" && deleteRejected.Live.Status == "" && deleteRejected.Live.ProcessState == "" {
		t.Fatalf("expected delete busy reject to include current live snapshot, got %+v", deleteRejected.Live)
	}
	if deleteRejected.Catalog.Error == "" && len(deleteRejected.Catalog.Tasks) == 0 && len(deleteRejected.Catalog.Tensions) == 0 {
		t.Fatalf("expected delete busy reject to include current catalog snapshot, got %+v", deleteRejected.Catalog)
	}

	stored, err := getLocalChat(managerDir, "busy-delete-chat")
	if err != nil {
		t.Fatalf("expected busy delete transcript to remain stored, got %v", err)
	}
	if len(stored.Messages) != 2 {
		t.Fatalf("expected delete reject to leave chat unchanged, got %+v", stored.Messages)
	}
}

func TestManagerWebLocalChatHistoryUsesLatestPrivilegedTurnAcrossMixedLegacyAndReadOnlyReplies(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "trusted-agent",
		DisplayName: "Trusted Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "developer",
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}
	dir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	session := &LocalChatSession{
		ChatID: "chat-history",
		Title:  "History Test",
		Messages: []LocalChatMessage{
			{Role: "user", Origin: "operator", Content: "start"},
			{
				Role:    "agent",
				Origin:  "manager_inspect",
				Content: "legacy partial",
				Execution: &LocalChatExecutionSnapshot{
					SnapshotStatus: "legacy_partial",
					ToolScope:      "legacy_unknown",
				},
			},
			{
				Role:    "agent",
				Origin:  "manager_inspect",
				Content: "legacy privileged",
				Execution: &LocalChatExecutionSnapshot{
					SnapshotStatus:  "captured",
					ToolScope:       "bounded_mutation_no_shell",
					MutationAllowed: boolRef(true),
				},
			},
			{
				Role:    "agent",
				Origin:  "manager_inspect",
				Content: "explicit override",
				Execution: &LocalChatExecutionSnapshot{
					SnapshotStatus:  "captured",
					OverrideMode:    "operator_override_mutation",
					ToolScope:       "bounded_mutation_no_shell",
					MutationAllowed: boolRef(true),
				},
			},
			{
				Role:    "agent",
				Origin:  "manager_inspect",
				Content: "read only later",
				Execution: &LocalChatExecutionSnapshot{
					SnapshotStatus:  "captured",
					OverrideMode:    "default_read_only",
					ToolScope:       "read_only_inspect_no_shell",
					ShellAllowed:    boolRef(false),
					MutationAllowed: boolRef(false),
				},
			},
		},
	}
	if err := saveLocalChatForRecord(record, dir, session); err != nil {
		t.Fatalf("saveLocalChatForRecord() error: %v", err)
	}

	mux := newManagerWebServer().routes()
	listRec := httptest.NewRecorder()
	listReq := httptest.NewRequest(http.MethodGet, "/api/agents/trusted-agent/local_chats", nil)
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected local chat list to succeed, got %d body=%s", listRec.Code, listRec.Body.String())
	}
	var listed struct {
		Sessions []LocalChatSession `json:"sessions"`
	}
	if err := json.Unmarshal(listRec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode local chat list response: %v", err)
	}
	if len(listed.Sessions) != 1 {
		t.Fatalf("expected one listed local chat session, got %+v", listed.Sessions)
	}
	if !listed.Sessions[0].HasPrivilegedTurns || listed.Sessions[0].LastOverrideMode != "operator_override_mutation" {
		t.Fatalf("expected list to use latest real privileged turn, got %+v", listed.Sessions[0])
	}
	if listed.Sessions[0].LastPrivilegedToolScope != "bounded_mutation_no_shell" {
		t.Fatalf("expected list to surface latest privileged tool scope, got %+v", listed.Sessions[0])
	}

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/api/agents/trusted-agent/local_chats/chat-history", nil)
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected local chat get to succeed, got %d body=%s", getRec.Code, getRec.Body.String())
	}
	var fetched struct {
		Session LocalChatSession `json:"session"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("decode local chat get response: %v", err)
	}
	if !fetched.Session.HasPrivilegedTurns || fetched.Session.LastOverrideMode != "operator_override_mutation" {
		t.Fatalf("expected get to use latest real privileged turn, got %+v", fetched.Session)
	}
	if fetched.Session.LastPrivilegedToolScope != "bounded_mutation_no_shell" {
		t.Fatalf("expected get to surface latest privileged tool scope, got %+v", fetched.Session)
	}
}

func TestManagerWebPartnerManagedInspectRejectsOverrideAndLeavesTranscriptUnchanged(t *testing.T) {
	setManagerWebTestHome(t)
	if err := SaveRhizomeProfile(RhizomeConnectionProfile{OwnerUserID: "developer"}); err != nil {
		t.Fatalf("SaveRhizomeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "partner-agent",
		DisplayName: "Partner Agent",
		Workdir:     t.TempDir(),
		WorkspaceID: "rhizome-main",
		OwnerUserID: "partner-owner",
	}
	configRoot := managedAgentConfigRootPath(record.Workdir)
	if err := os.MkdirAll(configRoot, 0o700); err != nil {
		t.Fatalf("MkdirAll(configRoot) error: %v", err)
	}
	if err := os.WriteFile(keyPathForRoot(configRoot), []byte("partner-openai-key"), 0o600); err != nil {
		t.Fatalf("WriteFile(openai_key) error: %v", err)
	}
	if err := SaveBotRegistry(BotRegistry{Agents: []ManagedAgentRecord{record}}); err != nil {
		t.Fatalf("SaveBotRegistry() error: %v", err)
	}

	mux := newManagerWebServer().routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected partner-managed inspect chat create to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Session LocalChatSession `json:"session"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create local chat response: %v", err)
	}

	for _, body := range []string{
		`{"content":"edit this","allow_mutation":true}`,
		`{"content":"shell this","allow_shell":true}`,
	} {
		sendRec := httptest.NewRecorder()
		sendReq := httptest.NewRequest(http.MethodPost, "/api/agents/partner-agent/local_chats/"+created.Session.ChatID+"/message", strings.NewReader(body))
		sendReq.Header.Set("Content-Type", "application/json")
		mux.ServeHTTP(sendRec, sendReq)
		if sendRec.Code != http.StatusForbidden {
			t.Fatalf("expected partner-managed inspect override to fail closed, got %d body=%s", sendRec.Code, sendRec.Body.String())
		}
		if !strings.Contains(sendRec.Body.String(), "override not allowed") {
			t.Fatalf("expected explicit override reject, got %s", sendRec.Body.String())
		}
	}

	managerDir, err := localChatsDirForRecord(record)
	if err != nil {
		t.Fatalf("localChatsDirForRecord() error: %v", err)
	}
	stored, err := getLocalChat(managerDir, created.Session.ChatID)
	if err != nil {
		t.Fatalf("getLocalChat() error: %v", err)
	}
	if len(stored.Messages) != 0 {
		t.Fatalf("expected rejected partner-managed override sends to leave transcript unchanged, got %+v", stored.Messages)
	}
}

func TestManagerWebDashboardUsesInspectChatWording(t *testing.T) {
	html := managerWebDashboardHTML()
	if !strings.Contains(html, ">Inspect Chat<") {
		t.Fatalf("expected dashboard html to surface inspect-chat wording, got %q", html)
	}
	if !strings.Contains(html, "New Inspect Chat") {
		t.Fatalf("expected dashboard html to surface new inspect chat wording, got %q", html)
	}
	if !strings.Contains(html, "Manager Inspect is thinking...") {
		t.Fatalf("expected dashboard script to use manager inspect typing copy, got %q", html)
	}
	if strings.Contains(html, "Allow mutation for this send") || strings.Contains(html, "Allow shell (+ mutation) for this send") {
		t.Fatalf("expected dashboard html to hide per-send inspect override controls, got %q", html)
	}
	if strings.Contains(html, "Required when enabling mutation or shell (+ mutation) override") {
		t.Fatalf("expected dashboard html to hide explicit inspect override reason field, got %q", html)
	}
	if !strings.Contains(html, "Delete blocked: privileged history requires audit retention") || !strings.Contains(html, "retained for privileged-history audit") {
		t.Fatalf("expected dashboard script to surface audit-retained inspect chat wording, got %q", html)
	}
	if !strings.Contains(html, "Archived Retained Chats") || !strings.Contains(html, "/archive', {method:'POST'}") {
		t.Fatalf("expected dashboard script to surface retained archive lifecycle, got %q", html)
	}
	if !strings.Contains(html, "chat-item-tag-retained") || !strings.Contains(html, "chat-item-action-archive") {
		t.Fatalf("expected dashboard script to render retained inspect chat pills and archive action buttons, got %q", html)
	}
	if strings.Contains(html, `<div class="chat-item-time">Retention: `) {
		t.Fatalf("expected retained inspect chat list rows to stop rendering inline retention copy, got %q", html)
	}
	if !strings.Contains(html, "api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats/'+encodeURIComponent(id)).then(function(res)") {
		t.Fatalf("expected dashboard delete path to refresh latest session truth before delete, got %q", html)
	}
	if strings.Contains(html, ">Local Chat<") {
		t.Fatalf("expected dashboard html to stop using live-like local chat tab copy, got %q", html)
	}
	if strings.Contains(html, "Agent is typing...") {
		t.Fatalf("expected dashboard script to stop using live-like typing copy, got %q", html)
	}
}

func TestManagerWebDashboardRollsBackOptimisticInspectTurnOnSendFailure(t *testing.T) {
	html := managerWebDashboardHTML()
	if !strings.Contains(html, ".catch(function(err){typing.remove();uidiv.remove();") {
		t.Fatalf("expected inspect send failure path to remove optimistic operator turn, got %q", html)
	}
	if !strings.Contains(html, "fetchLocalChats();loadLocalChat(state.activeLocalChatId)") {
		t.Fatalf("expected inspect send failure path to refresh transcript from server truth, got %q", html)
	}
}

func setManagerWebTestHome(t *testing.T) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
}
