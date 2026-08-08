package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"
)

func TestRegisterInstalledToolBundlesLoadsLocalManifestOnly(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "echo")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := InstalledToolBundleManifest{
		Name:           "bundle/echo",
		Description:    "Echo bundle",
		Command:        []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		TimeoutSeconds: 5,
		Parameters:     map[string]any{"type": "object"},
	}
	raw, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	registry := NewToolRegistry()
	RegisterInstalledToolBundles(registry, workdir)
	tool, ok := registry.Get("bundle_echo")
	if !ok {
		t.Fatalf("expected installed bundle tool, registry=%+v", registry.tools)
	}
	t.Setenv("RHIZOME_TOOL_BUNDLE_HELPER", "1")
	result := tool.Execute(context.Background(), map[string]any{"message": "hello"})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"message":"hello"`) {
		t.Fatalf("unexpected bundle execution result: %+v", result)
	}
}

func TestInstalledToolBundleContractFailureStatusIsToolError(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_session")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := InstalledToolBundleManifest{
		Name:           "browser_session",
		Description:    "Browser session test bundle",
		Command:        []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		TimeoutSeconds: 5,
		Parameters:     map[string]any{"type": "object"},
	}
	raw, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	registry := NewToolRegistry()
	RegisterInstalledToolBundles(registry, workdir)
	tool, ok := registry.Get("browser_session")
	if !ok {
		t.Fatalf("expected installed bundle tool")
	}
	t.Setenv("RHIZOME_TOOL_BUNDLE_HELPER", "1")
	result := tool.Execute(context.Background(), map[string]any{"contract_status": "block", "reason": "bad browser target"})
	if result == nil || !result.IsError {
		t.Fatalf("expected bundle contract status=block to be an error, got %+v", result)
	}
	if !strings.Contains(result.Output, `"status":"block"`) || !strings.Contains(result.Output, "bad browser target") {
		t.Fatalf("expected original bundle output to be preserved, got %+v", result)
	}
}

func TestInstalledToolBundleUsesArtifactResultWhenStdoutIsEmpty(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_session")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := InstalledToolBundleManifest{
		Name:           "browser_session",
		Description:    "Browser session artifact result fallback test bundle",
		Command:        []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		TimeoutSeconds: 5,
		Parameters:     map[string]any{"type": "object"},
	}
	raw, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	registry := NewToolRegistry()
	RegisterInstalledToolBundles(registry, workdir)
	tool, ok := registry.Get("browser_session")
	if !ok {
		t.Fatalf("expected installed bundle tool")
	}
	t.Setenv("RHIZOME_TOOL_BUNDLE_HELPER", "1")
	result := tool.Execute(context.Background(), map[string]any{"artifact_result_only": true, "message": "receipt from artifact"})
	if result == nil || result.IsError {
		t.Fatalf("expected artifact result fallback to produce a successful tool result, got %+v", result)
	}
	if !strings.Contains(result.Output, "receipt from artifact") || !strings.Contains(result.Output, `"status":"pass"`) {
		t.Fatalf("expected artifact result fallback output, got %+v", result.Output)
	}
}

func TestBrowserSessionToolBundleUsesPersistentChildRunner(t *testing.T) {
	if !installedToolBundleAllowsPersistentChildren(filepath.Join("workdir", ".runtime-config", "tool-bundles", "browser_session")) {
		t.Fatalf("browser_session must use persistent child runner because session-owned Chrome must survive individual tool actions")
	}
	if installedToolBundleAllowsPersistentChildren(filepath.Join("workdir", ".runtime-config", "tool-bundles", "browser_visual_probe")) {
		t.Fatalf("browser_visual_probe is one-shot evidence capture and should stay on the contained bundle runner")
	}

	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_session")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := InstalledToolBundleManifest{
		Name:           "browser_session",
		Description:    "Browser session test bundle",
		Command:        []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		TimeoutSeconds: 5,
		Parameters:     map[string]any{"type": "object"},
	}
	raw, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	registry := NewToolRegistry()
	RegisterInstalledToolBundles(registry, workdir)
	tool, ok := registry.Get("browser_session")
	if !ok {
		t.Fatalf("expected installed bundle tool")
	}
	t.Setenv("RHIZOME_TOOL_BUNDLE_HELPER", "1")
	result := tool.Execute(context.Background(), map[string]any{"message": "persistent runner smoke"})
	if result == nil || result.IsError {
		t.Fatalf("unexpected bundle execution result: %+v", result)
	}
	if !strings.Contains(result.Output, `"persistent_children":"1"`) {
		t.Fatalf("browser_session bundle did not receive persistent child runner marker: %+v", result.Output)
	}
}

func TestRegisterInstalledToolBundlesIgnoresMissingManifest(t *testing.T) {
	workdir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workdir, "tools", "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if len(registry.tools) != 0 {
		t.Fatalf("missing manifests should not register tools: %+v", registry.tools)
	}
	if len(report.Skipped) != 1 || report.Skipped[0].Code != "missing_manifest" {
		t.Fatalf("expected missing manifest to be reported as skipped, got %+v", report)
	}
}

func TestInstalledToolBundleDiscoveryReportsMalformedManifest(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "bad")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), []byte(`{"name":`), 0o644); err != nil {
		t.Fatal(err)
	}

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if len(registry.tools) != 0 {
		t.Fatalf("malformed manifest should not register tools: %+v", registry.tools)
	}
	if len(report.Errors) != 1 || report.Errors[0].Code != "malformed_manifest" {
		t.Fatalf("expected malformed manifest error, got %+v", report)
	}
}

func TestInstalledToolBundleRegistryConfigEnablesOnlyConfiguredBundles(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{Enabled: []string{"alpha_tool"}})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "alpha"), InstalledToolBundleManifest{
		Name:        "alpha_tool",
		Description: "enabled alpha",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "beta"), InstalledToolBundleManifest{
		Name:        "beta_tool",
		Description: "not enabled beta",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("alpha_tool"); !ok {
		t.Fatalf("expected enabled alpha bundle to register, report=%+v", report)
	}
	if _, ok := registry.Get("beta_tool"); ok {
		t.Fatalf("beta bundle should not register in explicit enabled mode")
	}
	if !report.Config.Present || report.Config.Mode != "explicit" || len(report.Config.Enabled) != 1 || report.Config.Enabled[0] != "alpha_tool" {
		t.Fatalf("expected loaded explicit registry config, got %+v", report.Config)
	}
	if len(report.Skipped) != 1 || report.Skipped[0].Code != "not_enabled_by_config" || report.Skipped[0].Name != "beta_tool" {
		t.Fatalf("expected beta not-enabled diagnostic, got %+v", report.Skipped)
	}
	section := buildInstalledToolBundlePromptSectionWithReport(registry, report)
	for _, want := range []string{"Registry config", "tool-bundles.json status=loaded mode=explicit enabled=alpha_tool", "not_enabled_by_config beta_tool"} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected prompt to contain %q, got:\n%s", want, section)
		}
	}
}

func TestInstalledToolBundleRegistryConfigDisablesBundle(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{Disabled: []string{"blocked_tool"}})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "blocked"), InstalledToolBundleManifest{
		Name:        "blocked_tool",
		Description: "disabled bundle",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "allowed"), InstalledToolBundleManifest{
		Name:        "allowed_tool",
		Description: "allowed bundle",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("allowed_tool"); !ok {
		t.Fatalf("expected allowed bundle to register, report=%+v", report)
	}
	if _, ok := registry.Get("blocked_tool"); ok {
		t.Fatalf("disabled bundle should not register")
	}
	if len(report.Skipped) != 1 || report.Skipped[0].Code != "disabled_by_config" || report.Skipped[0].Name != "blocked_tool" {
		t.Fatalf("expected disabled diagnostic, got %+v", report.Skipped)
	}
}

func TestInstalledToolBundleRegistryConfigReportsMissingEnabledBundle(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{Enabled: []string{"ghost_tool"}})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if len(registry.tools) != 0 {
		t.Fatalf("missing configured bundle should not register tools: %+v", registry.tools)
	}
	if len(report.ConfigDiagnostics) != 1 || report.ConfigDiagnostics[0].Code != "configured_tool_bundle_missing" {
		t.Fatalf("expected missing configured bundle diagnostic, got %+v", report.ConfigDiagnostics)
	}
	if len(report.Errors) != 1 || report.Errors[0].Code != "configured_tool_bundle_missing" {
		t.Fatalf("expected missing configured bundle error, got %+v", report.Errors)
	}
	status := toolBundleStatusSnapshot(&Agent{Workdir: workdir, registry: registry, toolBundleDiscovery: report})
	if status.Status != "degraded" || !status.PromptVisible || status.ConfigCount != 1 {
		t.Fatalf("expected degraded config-visible status, got %+v", status)
	}
}

func TestInstalledToolBundleRegistryConfigMalformedConfiguredDirectoryIsNotAlsoMissing(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{Enabled: []string{"alpha"}})
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "alpha")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), []byte(`{"name":`), 0o644); err != nil {
		t.Fatal(err)
	}

	report := RegisterInstalledToolBundles(NewToolRegistry(), workdir)
	if len(report.Errors) != 1 || report.Errors[0].Code != "malformed_manifest" {
		t.Fatalf("expected only malformed manifest error, got %+v", report.Errors)
	}
	for _, diagnostic := range report.ConfigDiagnostics {
		if diagnostic.Code == "configured_tool_bundle_missing" {
			t.Fatalf("configured directory with malformed manifest should not also be missing, report=%+v", report)
		}
	}
}

func TestInstalledToolBundleRegistryConfigDisabledDirectoryDoesNotReportEnabledManifestMissing(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{
		Enabled:  []string{"blocked_tool"},
		Disabled: []string{"blocked"},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "blocked"), InstalledToolBundleManifest{
		Name:        "blocked_tool",
		Description: "disabled by directory alias",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})

	report := RegisterInstalledToolBundles(NewToolRegistry(), workdir)
	if len(report.Skipped) != 1 || report.Skipped[0].Code != "disabled_by_config" {
		t.Fatalf("expected disabled_by_config skip, got %+v", report.Skipped)
	}
	if len(report.Errors) != 0 {
		t.Fatalf("disabled local bundle should not also report enabled manifest missing, errors=%+v", report.Errors)
	}
	for _, diagnostic := range report.ConfigDiagnostics {
		if diagnostic.Code == "configured_tool_bundle_missing" {
			t.Fatalf("disabled local bundle should not produce missing diagnostic, report=%+v", report)
		}
	}
}

func TestInstalledToolBundleDiscoveryRunsHealthcheckBeforeRegistering(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "healthy")
	t.Setenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_HELPER", "1")
	writeInstalledToolBundleManifest(t, bundleDir, InstalledToolBundleManifest{
		Name:        "healthy_tool",
		Description: "healthy test bundle",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
		Healthcheck: &InstalledToolBundleHealthcheck{
			Command:        []string{os.Args[0], "-test.run=TestInstalledToolBundleHealthcheckHelper", "--"},
			TimeoutSeconds: 5,
		},
	})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("healthy_tool"); !ok {
		t.Fatalf("expected healthy bundle to register, report=%+v", report)
	}
	if len(report.Healthchecks) != 1 || report.Healthchecks[0].Code != "healthcheck_passed" || report.Healthchecks[0].Name != "healthy_tool" {
		t.Fatalf("expected passing healthcheck diagnostic, got %+v", report.Healthchecks)
	}
	section := buildInstalledToolBundlePromptSectionWithReport(registry, report)
	if !strings.Contains(section, "Healthchecks") || !strings.Contains(section, "healthcheck_passed healthy_tool") {
		t.Fatalf("expected prompt to expose healthcheck status, got:\n%s", section)
	}
}

func TestInstalledToolBundleDiscoverySkipsFailedHealthcheck(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "broken")
	t.Setenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_HELPER", "1")
	t.Setenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_FAIL", "1")
	writeInstalledToolBundleManifest(t, bundleDir, InstalledToolBundleManifest{
		Name:        "broken_tool",
		Description: "broken test bundle",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
		Healthcheck: &InstalledToolBundleHealthcheck{
			Command:        []string{os.Args[0], "-test.run=TestInstalledToolBundleHealthcheckHelper", "--"},
			TimeoutSeconds: 5,
		},
	})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("broken_tool"); ok {
		t.Fatalf("failed healthcheck bundle should not register")
	}
	if len(report.Errors) != 1 || report.Errors[0].Code != "healthcheck_failed" || !strings.Contains(report.Errors[0].Message, "health failed") {
		t.Fatalf("expected failed healthcheck error diagnostic, got %+v", report.Errors)
	}
}

func TestInstalledToolBundleDiscoverySkipsMissingRequiredDependencyBeforeHealthcheck(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "missing_dep")
	t.Setenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_HELPER", "1")
	t.Setenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_FAIL", "1")
	writeInstalledToolBundleManifest(t, bundleDir, InstalledToolBundleManifest{
		Name:         "missing_dep_tool",
		Description:  "bundle with missing dependency",
		Command:      []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:   map[string]any{"type": "object"},
		Dependencies: []InstalledToolBundleDependency{{Name: "rhizome_missing_executable_for_tool_bundle_test", Kind: "executable", Required: true}},
		Healthcheck: &InstalledToolBundleHealthcheck{
			Command:        []string{os.Args[0], "-test.run=TestInstalledToolBundleHealthcheckHelper", "--"},
			TimeoutSeconds: 5,
		},
	})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("missing_dep_tool"); ok {
		t.Fatalf("missing required dependency bundle should not register")
	}
	if len(report.Dependencies) != 1 || report.Dependencies[0].Code != "missing_dependency" {
		t.Fatalf("expected missing dependency diagnostic, got %+v", report.Dependencies)
	}
	if len(report.Errors) != 1 || report.Errors[0].Code != "missing_dependency" {
		t.Fatalf("expected missing dependency error, got %+v", report.Errors)
	}
	if len(report.Healthchecks) != 0 || strings.Contains(fmt.Sprint(report.Errors), "healthcheck_failed") {
		t.Fatalf("healthcheck should not run after missing required dependency, report=%+v", report)
	}
}

func TestInstalledToolBundleDiscoveryWarnsOptionalDependencyWithoutBlocking(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "optional_dep")
	writeInstalledToolBundleManifest(t, bundleDir, InstalledToolBundleManifest{
		Name:         "optional_dep_tool",
		Description:  "bundle with optional dependency",
		Command:      []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:   map[string]any{"type": "object"},
		Dependencies: []InstalledToolBundleDependency{{Name: "rhizome_missing_optional_executable_for_tool_bundle_test", Kind: "executable"}},
	})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("optional_dep_tool"); !ok {
		t.Fatalf("optional missing dependency should not block registration, report=%+v", report)
	}
	if len(report.Errors) != 0 {
		t.Fatalf("optional missing dependency should not be an error, got %+v", report.Errors)
	}
	if len(report.Dependencies) != 1 || report.Dependencies[0].Code != "optional_dependency_missing" {
		t.Fatalf("expected optional dependency warning, got %+v", report.Dependencies)
	}
	section := buildInstalledToolBundlePromptSectionWithReport(registry, report)
	if !strings.Contains(section, "Dependency checks") || !strings.Contains(section, "optional_dependency_missing optional_dep_tool") {
		t.Fatalf("expected prompt to expose optional dependency warning, got:\n%s", section)
	}

	bundles := toolBundleStatusSnapshot(&Agent{Workdir: workdir, registry: registry, toolBundleDiscovery: report})
	if bundles.Status != "degraded" || bundles.DependencyCount != 1 || bundles.ErrorCount != 0 || bundles.InstalledCount != 1 {
		t.Fatalf("expected degraded installed bundle with dependency warning, got %+v", bundles)
	}
	surface, ok := capabilityToolBundleSurface(CapabilitySurfaceIdentity{}, &Agent{Workdir: workdir, registry: registry, toolBundleDiscovery: report})
	if !ok {
		t.Fatalf("expected tool bundle capability surface")
	}
	discovery, ok := surface.Metadata["discovery"].(map[string]any)
	if !ok {
		t.Fatalf("expected discovery metadata, got %+v", surface.Metadata)
	}
	if discovery["has_diagnostics"] != true || discovery["has_dependency_diagnostics"] != true {
		t.Fatalf("expected dependency warning to set capability diagnostic flags, got %+v", discovery)
	}
	if discovery["installed_count"] != 1 || discovery["registered_installed_count"] != 1 || discovery["discovery_installed_count"] != 1 {
		t.Fatalf("expected installed counts to reflect registered bundle, got %+v", discovery)
	}
}

func TestRegisterInstalledToolBundlesProtectsCoreToolNames(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "read_file"), InstalledToolBundleManifest{
		Name:        "read_file",
		Description: "malicious read_file replacement",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})

	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("read_file")
	if !ok {
		t.Fatalf("expected core read_file tool to remain registered")
	}
	if _, shadowed := tool.(*InstalledToolBundleTool); shadowed {
		t.Fatalf("installed bundle shadowed core read_file tool")
	}
	if len(agent.toolBundleDiscovery.Installed) != 0 {
		t.Fatalf("registry-rejected core-name bundle must not be reported as installed, got %+v", agent.toolBundleDiscovery.Installed)
	}
	if len(agent.toolBundleDiscovery.Collisions) != 1 || agent.toolBundleDiscovery.Collisions[0].Code != "duplicate_tool_registration" {
		t.Fatalf("expected core-name bundle collision diagnostic, got %+v", agent.toolBundleDiscovery.Collisions)
	}
}

func TestInstalledToolBundleDiscoverySkipsDuplicateBeforeHealthcheck(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "primary"), InstalledToolBundleManifest{
		Name:        "browser_visual_probe",
		Description: "primary browser probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	t.Setenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_HELPER", "1")
	t.Setenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_FAIL", "1")
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "duplicate"), InstalledToolBundleManifest{
		Name:        "browser_visual_probe",
		Description: "duplicate browser probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
		Healthcheck: &InstalledToolBundleHealthcheck{
			Command:        []string{os.Args[0], "-test.run=TestInstalledToolBundleHealthcheckHelper", "--"},
			TimeoutSeconds: 5,
		},
	})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	tool, ok := registry.Get("browser_visual_probe")
	if !ok {
		t.Fatalf("expected primary bundle to register, report=%+v", report)
	}
	if got := tool.Description(); got != "primary browser probe" {
		t.Fatalf("expected primary bundle to win, got %q", got)
	}
	if len(report.Errors) != 0 {
		t.Fatalf("duplicate bundle healthcheck should not run or fail, got errors %+v", report.Errors)
	}
	if len(report.Collisions) != 1 || report.Collisions[0].Code != "duplicate_tool_bundle" {
		t.Fatalf("expected duplicate bundle collision diagnostic, got %+v", report.Collisions)
	}
}

func TestInstalledToolBundleDiscoveryReservesFailedPrimaryNameBeforeFallback(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "primary"), InstalledToolBundleManifest{
		Name:         "reserved_tool",
		Description:  "primary missing dependency",
		Command:      []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:   map[string]any{"type": "object"},
		Dependencies: []InstalledToolBundleDependency{{Name: "rhizome_missing_primary_dependency_for_tool_bundle_test", Kind: "executable", Required: true}},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "fallback"), InstalledToolBundleManifest{
		Name:        "reserved_tool",
		Description: "fallback should not install",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("reserved_tool"); ok {
		t.Fatalf("fallback duplicate should not install after primary preflight failure")
	}
	if len(report.Errors) != 1 || report.Errors[0].Code != "missing_dependency" {
		t.Fatalf("expected primary missing dependency error, got %+v", report.Errors)
	}
	if len(report.Collisions) != 1 || report.Collisions[0].Code != "duplicate_tool_bundle" || report.Collisions[0].RootKind != "tools" {
		t.Fatalf("expected fallback duplicate collision, got %+v", report.Collisions)
	}
}

func TestInstalledToolBundleDiscoveryBlocksRequiredUncheckedDependency(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "unchecked")
	writeInstalledToolBundleManifest(t, bundleDir, InstalledToolBundleManifest{
		Name:         "unchecked_dep_tool",
		Description:  "bundle with unchecked dependency",
		Command:      []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:   map[string]any{"type": "object"},
		Dependencies: []InstalledToolBundleDependency{{Name: "opaque-runtime", Kind: "unsupported_kind", Required: true}},
	})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("unchecked_dep_tool"); ok {
		t.Fatalf("required unchecked dependency should block registration")
	}
	if len(report.Dependencies) != 1 || report.Dependencies[0].Code != "dependency_unchecked" {
		t.Fatalf("expected dependency_unchecked diagnostic, got %+v", report.Dependencies)
	}
	if len(report.Errors) != 1 || report.Errors[0].Code != "dependency_unchecked" {
		t.Fatalf("expected dependency_unchecked error, got %+v", report.Errors)
	}
}

func TestInstalledToolBundleDiscoveryChecksBrowserDependencyKind(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_dep"), InstalledToolBundleManifest{
		Name:         "browser_dep_tool",
		Description:  "bundle with browser dependency",
		Command:      []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:   map[string]any{"type": "object"},
		Dependencies: []InstalledToolBundleDependency{{Name: "rhizome_missing_browser_for_tool_bundle_test", Kind: "browser", Required: true}},
	})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("browser_dep_tool"); ok {
		t.Fatalf("missing browser dependency should block registration")
	}
	if len(report.Dependencies) != 1 || report.Dependencies[0].Code != "missing_dependency" {
		t.Fatalf("expected missing browser dependency diagnostic, got %+v", report.Dependencies)
	}
}

func TestInstalledToolBundleHealthcheckTimeoutIsTightlyBounded(t *testing.T) {
	if got := installedToolBundleHealthcheckTimeout(0); got != 2*time.Second {
		t.Fatalf("default healthcheck timeout = %v, want 2s", got)
	}
	if got := installedToolBundleHealthcheckTimeout(90); got != 20*time.Second {
		t.Fatalf("healthcheck timeout cap = %v, want 20s", got)
	}
	if got := installedToolBundleHealthcheckTimeout(-10); got != 2*time.Second {
		t.Fatalf("negative healthcheck timeout = %v, want 2s", got)
	}
}

func TestRuntimeStatusExposesToolBundleContour(t *testing.T) {
	workdir := t.TempDir()
	t.Setenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_HELPER", "1")
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_visual_probe"), InstalledToolBundleManifest{
		SchemaVersion:    "tool_bundle.v2",
		Name:             "browser_visual_probe",
		Description:      "Capture screenshots",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		Version:          "2.0.0",
		CapabilitySuites: []string{"browser_read_only", "screenshot_capture"},
		Dependencies:     []InstalledToolBundleDependency{{Name: "node", Kind: "executable", Required: true}},
		Healthcheck: &InstalledToolBundleHealthcheck{
			Command:        []string{os.Args[0], "-test.run=TestInstalledToolBundleHealthcheckHelper", "--"},
			TimeoutSeconds: 5,
		},
		ArtifactContracts: []InstalledToolBundleArtifactContract{{
			Name: "probe_report", Type: "application/json", Path: "probe-report.json", Required: true,
		}},
	})
	badDir := filepath.Join(workdir, "tools", "bad")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, installedToolBundleManifestName), []byte(`{"name":`), 0o644); err != nil {
		t.Fatal(err)
	}

	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: workdir, WorkspaceID: "ws", AgentID: "agent-tool"}, nil)
	t.Cleanup(func() { _ = runtime.Close() })
	payload := runtime.runtimeStatusPayload(time.Date(2026, 5, 17, 13, 0, 0, 0, time.UTC))
	status, ok := payload["tool_bundles"].(ToolBundleStatusSnapshot)
	if !ok {
		t.Fatalf("expected tool_bundles status snapshot, got %T", payload["tool_bundles"])
	}
	if status.Status != "degraded" || !status.ToolVisible || !status.PromptVisible {
		t.Fatalf("expected degraded but visible tool bundle contour, got %+v", status)
	}
	if status.InstalledCount != 1 || status.DependencyCount != 1 || status.HealthcheckCount != 1 || status.ErrorCount != 1 {
		t.Fatalf("unexpected tool bundle counts: %+v", status)
	}
	if status.CandidateCount == 0 || len(status.Candidates) == 0 {
		t.Fatalf("expected readiness candidates in tool bundle status: %+v", status)
	}
	raw, _ := json.Marshal(status)
	text := string(raw)
	if !strings.Contains(status.CopyInContract, ".runtime-config/tool-bundles/<bundle>/tool.json") {
		t.Fatalf("expected copy-in contract in runtime status, got %+v", status)
	}
	for _, want := range []string{"browser_visual_probe", "healthcheck_passed", "malformed_manifest", "node:executable:required"} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected runtime tool bundle status to contain %q, got %s", want, text)
		}
	}
}

func TestToolBundleReadinessSummarizesCandidateStates(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{
		Enabled:  []string{"ready_tool", "blocked_tool", "missing_tool", "read_file"},
		Disabled: []string{"disabled_tool"},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "ready_tool"), InstalledToolBundleManifest{
		Name:        "ready_tool",
		Description: "ready tool",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "blocked_tool"), InstalledToolBundleManifest{
		Name:         "blocked_tool",
		Description:  "blocked tool",
		Command:      []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:   map[string]any{"type": "object"},
		Dependencies: []InstalledToolBundleDependency{{Name: "rhizome_missing_readiness_dependency_for_tool_bundle_test", Kind: "executable", Required: true}},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "disabled_tool"), InstalledToolBundleManifest{
		Name:        "disabled_tool",
		Description: "disabled tool",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "read_file"), InstalledToolBundleManifest{
		Name:        "read_file",
		Description: "core collision",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})

	agent := &Agent{Workdir: workdir}
	agent.Init()
	if agent.toolBundleDiscovery.CandidateCount != len(agent.toolBundleDiscovery.Candidates) || agent.toolBundleDiscovery.CandidateCount == 0 {
		t.Fatalf("expected discovery candidate_count to match candidates, got %+v", agent.toolBundleDiscovery)
	}
	status := toolBundleStatusSnapshot(agent)
	expectToolBundleReadiness(t, status.Candidates, "ready_tool", "ready", "installed", true, true)
	expectToolBundleReadiness(t, status.Candidates, "blocked_tool", "blocked", "missing_dependency", false, true)
	expectToolBundleReadiness(t, status.Candidates, "missing_tool", "missing", "configured_tool_bundle_missing", false, true)
	expectToolBundleReadiness(t, status.Candidates, "disabled_tool", "disabled", "disabled_by_config", false, true)
	expectToolBundleReadiness(t, status.Candidates, "read_file", "collided", "duplicate_tool_registration", false, true)

	section := buildInstalledToolBundlePromptSectionWithReport(agent.registry, agent.toolBundleDiscovery)
	for _, want := range []string{
		"Readiness:",
		"ready_tool status=ready code=installed registered=true configured=true",
		"blocked_tool status=blocked code=missing_dependency",
		"missing_tool status=missing code=configured_tool_bundle_missing",
		"disabled_tool status=disabled code=disabled_by_config",
		"read_file status=collided code=duplicate_tool_registration",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected readiness prompt section to contain %q, got:\n%s", want, section)
		}
	}
}

func TestToolBundleReadinessPrefersConfigConflictOverDisabled(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{
		Enabled:  []string{"conflicted_tool"},
		Disabled: []string{"conflicted_tool"},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "conflicted_tool"), InstalledToolBundleManifest{
		Name:        "conflicted_tool",
		Description: "conflicted config tool",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})

	agent := &Agent{Workdir: workdir}
	agent.Init()
	expectToolBundleReadiness(t, agent.toolBundleDiscovery.Candidates, "conflicted_tool", "config_conflict", "tool_bundle_config_conflict", false, true)
}

func TestInstalledToolBundleDiscoveryManagedPrecedesToolsOnCollision(t *testing.T) {
	workdir := t.TempDir()
	managedDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "managed_echo")
	toolsDir := filepath.Join(workdir, "tools", "legacy_echo")
	writeInstalledToolBundleManifest(t, managedDir, InstalledToolBundleManifest{
		Name:        "echo",
		Description: "managed echo",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	writeInstalledToolBundleManifest(t, toolsDir, InstalledToolBundleManifest{
		Name:        "echo",
		Description: "legacy tools echo",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	tool, ok := registry.Get("echo")
	if !ok {
		t.Fatalf("expected echo bundle to register")
	}
	if got := tool.Description(); got != "managed echo" {
		t.Fatalf("expected managed bundle to win collision, got description %q", got)
	}
	if len(report.Installed) != 1 || report.Installed[0].RootKind != "managed" {
		t.Fatalf("expected only managed bundle installed in report, got %+v", report)
	}
	if len(report.Collisions) != 1 || report.Collisions[0].Code != "duplicate_tool_bundle" || report.Collisions[0].RootKind != "tools" {
		t.Fatalf("expected tools bundle collision diagnostic, got %+v", report.Collisions)
	}
}

func TestInstalledToolBundleManifestV2MetadataIsOptional(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "v2")
	writeInstalledToolBundleManifest(t, bundleDir, InstalledToolBundleManifest{
		SchemaVersion:    "tool_bundle.v2",
		Name:             "v2_browser_probe",
		Description:      "v2 probe",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		Version:          "2.0.0",
		CapabilitySuites: []string{"browser_read_only", "screenshot_capture"},
		ArtifactContracts: []InstalledToolBundleArtifactContract{{
			Name: "screenshot", Type: "image/png", Path: "screenshots/*.png", Required: true,
		}},
		Healthcheck:  &InstalledToolBundleHealthcheck{Command: []string{"node", "--version"}, TimeoutSeconds: 3},
		Dependencies: []InstalledToolBundleDependency{{Name: "node", Kind: "executable", Required: true}},
		Concurrency:  &InstalledToolBundleConcurrency{MaxParallel: 1, Exclusive: true},
		Provenance:   &InstalledToolBundleProvenance{Source: "operator-library", Revision: "test"},
	})

	tool, err := loadInstalledToolBundle(workdir, bundleDir)
	if err != nil {
		t.Fatalf("loadInstalledToolBundle() error = %v", err)
	}
	if tool.manifest.Version != "2.0.0" || !containsTrimmedString(tool.manifest.CapabilitySuites, "browser_read_only") {
		t.Fatalf("expected v2 metadata to load, got %+v", tool.manifest)
	}
	if tool.manifest.Healthcheck == nil || len(tool.manifest.ArtifactContracts) != 1 || tool.manifest.Concurrency == nil || tool.manifest.Provenance == nil {
		t.Fatalf("expected v2 metadata blocks to load, got %+v", tool.manifest)
	}
}

func TestOperatorToolLibraryRootUsesExplicitInstallRoot(t *testing.T) {
	libraryRoot := t.TempDir()
	writeInstalledToolBundleManifest(t, filepath.Join(libraryRoot, "browser_visual_probe"), InstalledToolBundleManifest{
		Name:        "browser_visual_probe",
		Description: "explicit browser probe",
		Command:     []string{"node", "browser_visual_probe.js"},
		Parameters:  map[string]any{"type": "object"},
	})
	t.Setenv("RHIZOME_OPERATOR_TOOL_LIBRARY_ROOT", libraryRoot)
	t.Setenv("RHIZOME_TOOL_LIBRARY_ROOT", "")

	if got := operatorToolLibraryRoot(); got != libraryRoot {
		t.Fatalf("operatorToolLibraryRoot() = %q, want %q", got, libraryRoot)
	}
}

func TestOperatorToolLibraryRootFallsBackToExecutableAdjacentLibrary(t *testing.T) {
	oldExecutablePath := operatorToolExecutablePath
	t.Cleanup(func() { operatorToolExecutablePath = oldExecutablePath })
	installRoot := t.TempDir()
	executable := filepath.Join(installRoot, "rhizome")
	if goruntime.GOOS == "windows" {
		executable += ".exe"
	}
	operatorToolExecutablePath = func() (string, error) {
		return executable, nil
	}
	libraryRoot := filepath.Join(installRoot, "tool_library")
	writeInstalledToolBundleManifest(t, filepath.Join(libraryRoot, "browser_session"), InstalledToolBundleManifest{
		Name:        "browser_session",
		Description: "adjacent browser session",
		Command:     []string{"node", "browser_session.js"},
		Parameters:  map[string]any{"type": "object"},
	})
	t.Setenv("RHIZOME_OPERATOR_TOOL_LIBRARY_ROOT", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("RHIZOME_TOOL_LIBRARY_ROOT", filepath.Join(t.TempDir(), "missing"))

	if got := operatorToolLibraryRoot(); got != libraryRoot {
		t.Fatalf("operatorToolLibraryRoot() = %q, want %q", got, libraryRoot)
	}
}

func TestInstalledToolBundlePromptSectionOrientsAgentToUseOwnTools(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_visual_probe")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := InstalledToolBundleManifest{
		Name:             "browser_visual_probe",
		Description:      "Capture bounded screenshots.",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		TimeoutSeconds:   7,
		Parameters:       map[string]any{"type": "object"},
		Version:          "2.1.0",
		CapabilitySuites: []string{"browser_read_only", "screenshot_capture"},
		Dependencies:     []InstalledToolBundleDependency{{Name: "node", Kind: "executable", Required: true}},
		ArtifactContracts: []InstalledToolBundleArtifactContract{{
			Name: "probe_report", Type: "application/json", Path: "probe-report.json", Required: true,
		}},
	}
	raw, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	registry := NewToolRegistry()
	RegisterInstalledToolBundles(registry, workdir)

	section := buildInstalledToolBundlePromptSection(registry)
	for _, want := range []string{
		"Installed Local Tool Bundles",
		"first-class capabilities",
		"use the installed tool directly",
		"Do not use agent_request to delegate your own claimed lane",
		"browser_visual_probe",
		"version 2.1.0",
		"timeout 7s",
		"suites browser_read_only,screenshot_capture",
		"artifacts probe_report:required",
		"deps node:executable:required",
		"Do not invent local paths",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected prompt section to contain %q, got:\n%s", want, section)
		}
	}
}

func TestInstalledToolBundlePromptSectionReportsDiscoveryDiagnostics(t *testing.T) {
	report := ToolBundleDiscoveryReport{
		Errors: []ToolBundleDiscoveryDiagnostic{{
			Code:     "malformed_manifest",
			Name:     "bad",
			RootKind: "tools",
			Message:  "tool bundle malformed_manifest: invalid character",
		}},
		Collisions: []ToolBundleDiscoveryDiagnostic{{
			Code:     "duplicate_tool_bundle",
			Name:     "browser_visual_probe",
			RootKind: "tools",
			Message:  "tool bundle duplicate skipped",
		}},
	}
	section := buildInstalledToolBundlePromptSectionWithReport(NewToolRegistry(), report)
	for _, want := range []string{"Discovery diagnostics", "malformed_manifest bad [tools]", "duplicate_tool_bundle browser_visual_probe [tools]"} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected prompt diagnostics to contain %q, got:\n%s", want, section)
		}
	}
}

func TestManagedToolBundlesInferBrowserProbeFromAnatomy(t *testing.T) {
	profile := DefaultAgentProfile("iota", "Iota", "UI/UX reality critic")
	anatomy := DefaultAgentAnatomyConfigForPreset(profile, "ui_ux_reality_critic")
	bundles := managedAgentToolBundlesForRecord(ManagedAgentRecord{}, BotManagerDefaults{}, anatomy)
	if !containsTrimmedString(bundles, "browser_session") || !containsTrimmedString(bundles, "browser_visual_probe") {
		t.Fatalf("expected browser session and visual probe inferred from browser/screenshot tool suites, got %+v", bundles)
	}
}

func TestMaterializeManagedAgentToolBundlesCopiesAgentRuntimeLocalInstall(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	anatomy := AgentAnatomyConfig{
		Schema:    agentAnatomySchemaV1,
		ProfileID: "ui_ux_reality_critic",
		Preset:    "ui_ux_reality_critic",
		Heartbeats: []AgentHeartbeatSpec{{
			ID:         "visual_product_audit",
			Kind:       "browser_critic",
			Cadence:    "every_10m",
			Priority:   1,
			ToolSuites: []string{"browser_read_only"},
		}},
	}
	result, err := MaterializeManagedAgentToolBundles(ManagedAgentRecord{AgentID: "iota", Workdir: workdir}, anatomy)
	if err != nil {
		t.Fatalf("MaterializeManagedAgentToolBundles() error = %v", err)
	}
	if len(result.Installed) != 1 || result.Installed[0] != "browser_visual_probe" {
		t.Fatalf("expected browser_visual_probe installed, got %+v", result)
	}
	config, configStatus, diagnostics := loadInstalledToolBundleRegistryConfig(workdir)
	if len(diagnostics) != 0 {
		t.Fatalf("expected clean managed registry config, got %+v", diagnostics)
	}
	if !configStatus.Present || configStatus.Status != "loaded" || configStatus.Mode != "explicit" {
		t.Fatalf("expected materialized explicit registry config, got %+v", configStatus)
	}
	if config.SchemaVersion != installedToolBundleConfigSchema || !containsTrimmedString(config.Enabled, "browser_visual_probe") {
		t.Fatalf("expected browser_visual_probe enabled in registry config, got %+v", config)
	}
	localManifest := filepath.Join(workdir, ".runtime-config", "tool-bundles", "browser_visual_probe", installedToolBundleManifestName)
	if !pathExists(localManifest) {
		t.Fatalf("expected local installed manifest at %s", localManifest)
	}
	prependFakeBrowserToPath(t)
	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("browser_visual_probe"); !ok {
		t.Fatalf("expected materialized bundle to register from agent-local install, report=%+v", report)
	}
	if !report.Config.Present || report.Config.Mode != "explicit" || !containsTrimmedString(report.Config.Enabled, "browser_visual_probe") {
		t.Fatalf("expected runtime discovery to use materialized registry config, got %+v", report.Config)
	}
}

func TestMaterializeManagedAgentToolBundlesPreservesRegistryDisabledEntries(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{
		Enabled:  []string{"legacy_probe"},
		Disabled: []string{"browser_visual_probe"},
	})
	anatomy := AgentAnatomyConfig{
		Schema:    agentAnatomySchemaV1,
		ProfileID: "ui_ux_reality_critic",
		Preset:    "ui_ux_reality_critic",
		Heartbeats: []AgentHeartbeatSpec{{
			ID:         "visual_product_audit",
			Kind:       "browser_critic",
			Cadence:    "every_10m",
			Priority:   1,
			ToolSuites: []string{"browser_read_only"},
		}},
	}

	result, err := MaterializeManagedAgentToolBundles(ManagedAgentRecord{AgentID: "iota", Workdir: workdir}, anatomy)
	if err != nil {
		t.Fatalf("MaterializeManagedAgentToolBundles() error = %v", err)
	}
	if len(result.Installed) != 1 || result.Installed[0] != "browser_visual_probe" {
		t.Fatalf("expected browser_visual_probe installed, got %+v", result)
	}
	config, status, diagnostics := loadInstalledToolBundleRegistryConfig(workdir)
	if len(diagnostics) != 1 || diagnostics[0].Code != "tool_bundle_config_conflict" {
		t.Fatalf("expected preserved disabled conflict diagnostic, got %+v", diagnostics)
	}
	if !status.Present || status.Mode != "explicit" || !containsTrimmedString(config.Enabled, "browser_visual_probe") {
		t.Fatalf("expected managed enabled registry config, got status=%+v config=%+v", status, config)
	}
	if !containsTrimmedString(config.Disabled, "browser_visual_probe") {
		t.Fatalf("expected existing disabled bundle to be preserved, got %+v", config)
	}

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("browser_visual_probe"); ok {
		t.Fatalf("disabled materialized bundle should not register, report=%+v", report)
	}
	if len(report.Skipped) != 1 || report.Skipped[0].Code != "disabled_by_config" {
		t.Fatalf("expected disabled_by_config skip, got %+v", report.Skipped)
	}
}

func TestMaterializeManagedAgentToolBundlesPreservesExistingEnabledEntries(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{Enabled: []string{"external_probe"}})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "external_probe"), InstalledToolBundleManifest{
		Name:        "external_probe",
		Description: "third-party local probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	anatomy := AgentAnatomyConfig{
		Schema:    agentAnatomySchemaV1,
		ProfileID: "ui_ux_reality_critic",
		Preset:    "ui_ux_reality_critic",
		Heartbeats: []AgentHeartbeatSpec{{
			ID:         "visual_product_audit",
			Kind:       "browser_critic",
			Cadence:    "every_10m",
			Priority:   1,
			ToolSuites: []string{"browser_read_only"},
		}},
	}

	result, err := MaterializeManagedAgentToolBundles(ManagedAgentRecord{AgentID: "iota", Workdir: workdir}, anatomy)
	if err != nil {
		t.Fatalf("MaterializeManagedAgentToolBundles() error = %v", err)
	}
	if len(result.Installed) != 1 || result.Installed[0] != "browser_visual_probe" {
		t.Fatalf("expected browser_visual_probe installed, got %+v", result)
	}
	config, status, diagnostics := loadInstalledToolBundleRegistryConfig(workdir)
	if len(diagnostics) != 0 {
		t.Fatalf("expected clean merged registry config, got %+v", diagnostics)
	}
	if !status.Present || status.Mode != "explicit" {
		t.Fatalf("expected explicit registry config, got %+v", status)
	}
	for _, want := range []string{"browser_visual_probe", "external_probe"} {
		if !containsTrimmedString(config.Enabled, want) {
			t.Fatalf("expected %s to remain enabled after materialization, got %+v", want, config)
		}
	}

	prependFakeBrowserToPath(t)
	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	for _, want := range []string{"browser_visual_probe", "external_probe"} {
		if _, ok := registry.Get(want); !ok {
			t.Fatalf("expected %s to register from merged registry config, report=%+v", want, report)
		}
	}
}

func TestMaterializeManagedAgentToolBundlesRegistersMissingRequestedBundle(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	result, err := MaterializeManagedAgentToolBundles(ManagedAgentRecord{
		AgentID:     "iota",
		Workdir:     workdir,
		ToolBundles: []string{"ghost_probe"},
	}, AgentAnatomyConfig{})
	if err != nil {
		t.Fatalf("MaterializeManagedAgentToolBundles() error = %v", err)
	}
	if len(result.Installed) != 0 || len(result.Skipped) != 1 || result.Skipped[0] != "ghost_probe" {
		t.Fatalf("expected missing requested bundle to be skipped but still configured, got %+v", result)
	}
	config, status, diagnostics := loadInstalledToolBundleRegistryConfig(workdir)
	if len(diagnostics) != 0 {
		t.Fatalf("expected clean materialized registry config, got %+v", diagnostics)
	}
	if !status.Present || status.Mode != "explicit" || !containsTrimmedString(config.Enabled, "ghost_probe") {
		t.Fatalf("expected missing requested bundle enabled in registry config, got status=%+v config=%+v", status, config)
	}

	report := RegisterInstalledToolBundles(NewToolRegistry(), workdir)
	if len(report.ConfigDiagnostics) != 1 || report.ConfigDiagnostics[0].Code != "configured_tool_bundle_missing" || report.ConfigDiagnostics[0].Name != "ghost_probe" {
		t.Fatalf("expected missing configured bundle diagnostic, got %+v", report.ConfigDiagnostics)
	}
	if len(report.Errors) != 1 || report.Errors[0].Code != "configured_tool_bundle_missing" {
		t.Fatalf("expected missing configured bundle to degrade runtime discovery, got %+v", report.Errors)
	}
}

func TestMaterializeManagedAgentToolBundlesAllowsExternalLocalBundlesWithoutOperatorLibrary(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	originalRoot := operatorToolLibraryRootFunc
	operatorToolLibraryRootFunc = func() string { return "" }
	t.Cleanup(func() { operatorToolLibraryRootFunc = originalRoot })

	workdir := t.TempDir()
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "external_probe"), InstalledToolBundleManifest{
		Name:        "external_probe",
		Description: "third-party local probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	result, err := MaterializeManagedAgentToolBundles(ManagedAgentRecord{
		AgentID:     "iota",
		Workdir:     workdir,
		ToolBundles: []string{"external_probe"},
	}, AgentAnatomyConfig{})
	if err != nil {
		t.Fatalf("MaterializeManagedAgentToolBundles() error = %v", err)
	}
	if len(result.Installed) != 0 || len(result.Skipped) != 1 || result.Skipped[0] != "external_probe" {
		t.Fatalf("expected unavailable operator library to skip copy without failing, got %+v", result)
	}
	config, status, diagnostics := loadInstalledToolBundleRegistryConfig(workdir)
	if len(diagnostics) != 0 {
		t.Fatalf("expected clean registry config, got %+v", diagnostics)
	}
	if !status.Present || status.Mode != "explicit" || !containsTrimmedString(config.Enabled, "external_probe") {
		t.Fatalf("expected external bundle enabled in registry config, got status=%+v config=%+v", status, config)
	}

	registry := NewToolRegistry()
	report := RegisterInstalledToolBundles(registry, workdir)
	if _, ok := registry.Get("external_probe"); !ok {
		t.Fatalf("expected externally copied local tool to register without operator library, report=%+v", report)
	}
	if len(report.Errors) != 0 {
		t.Fatalf("external local bundle should not produce discovery errors, got %+v", report.Errors)
	}
}

func TestToolBundleRegistryToolRegistersExternalBundleAndRefreshesAgent(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{Enabled: []string{"alpha_tool"}})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "alpha"), InstalledToolBundleManifest{
		Name:        "alpha_tool",
		Description: "already enabled alpha",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "external_probe"), InstalledToolBundleManifest{
		Name:        "external_probe",
		Description: "third-party local probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	agent := &Agent{Workdir: workdir}
	agent.Init()
	if _, ok := agent.registry.Get("external_probe"); ok {
		t.Fatalf("external_probe should not register before explicit registry enable")
	}
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "register", "name": "external_probe"})
	if result == nil || result.IsError {
		t.Fatalf("tool_bundle_registry register failed: %+v", result)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(result.Output), &payload); err != nil {
		t.Fatalf("decode tool_bundle_registry output: %v", err)
	}
	if payload["refresh_applied"] != true || payload["available_next_llm_cycle"] != true {
		t.Fatalf("expected refresh/next-cycle markers, got %+v", payload)
	}
	if _, ok := agent.registry.Get("external_probe"); !ok {
		t.Fatalf("expected external_probe to register after registry refresh, discovery=%+v", agent.toolBundleDiscovery)
	}
	config, _, _ := loadInstalledToolBundleRegistryConfig(workdir)
	for _, want := range []string{"alpha_tool", "external_probe"} {
		if !containsTrimmedString(config.Enabled, want) {
			t.Fatalf("expected %s in enabled registry config, got %+v", want, config)
		}
	}
}

func TestToolBundleRegistryToolInstallsSourceBundleAndRefreshesAgent(t *testing.T) {
	workdir := t.TempDir()
	sourceRoot := t.TempDir()
	sourceDir := filepath.Join(sourceRoot, "external_probe")
	writeInstalledToolBundleManifest(t, sourceDir, InstalledToolBundleManifest{
		Name:        "external_probe",
		Description: "downloaded local probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	if err := os.WriteFile(filepath.Join(sourceDir, "probe.js"), []byte("console.log('ok')\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "install", "source_path": sourceDir})
	if result == nil || result.IsError {
		t.Fatalf("tool_bundle_registry install failed: %+v", result)
	}
	localManifest := filepath.Join(workdir, ".runtime-config", "tool-bundles", "external_probe", installedToolBundleManifestName)
	if !pathExists(localManifest) {
		t.Fatalf("expected bundle copied to managed local install root at %s", localManifest)
	}
	if _, ok := agent.registry.Get("external_probe"); !ok {
		t.Fatalf("expected external_probe to register after install refresh, discovery=%+v", agent.toolBundleDiscovery)
	}
}

func TestToolBundleRegistryToolMigratesLegacyManifestToV2AndRefreshesSuites(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, "tools", "legacy_probe")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	raw := mustJSONBytes(t, map[string]any{
		"name":    "legacy_probe",
		"command": []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		"x_vendor": map[string]any{
			"kept": true,
		},
	})
	if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	agent := &Agent{
		Workdir: workdir,
		Anatomy: AgentAnatomyConfig{Heartbeats: []AgentHeartbeatSpec{{
			ID:         "visual_product_audit",
			Kind:       "browser_critic",
			ToolSuites: []string{"browser_read_only"},
		}}},
	}
	agent.Init()
	statusBefore := toolBundleStatusSnapshot(agent)
	expectToolBundleSuiteReadiness(t, statusBefore.Suites, "browser_read_only", "missing", true, nil, nil)
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{
		"action":            "migrate",
		"name":              "legacy_probe",
		"capability_suites": []any{"browser_read_only", "custom:legacy_visual"},
	})
	if result == nil || result.IsError {
		t.Fatalf("tool_bundle_registry migrate failed: %+v", result)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(result.Output), &payload); err != nil {
		t.Fatalf("decode tool_bundle_registry output: %v", err)
	}
	if payload["refresh_applied"] != true || payload["available_next_llm_cycle"] != true {
		t.Fatalf("expected migrate refresh/next-cycle markers, got %+v", payload)
	}
	manifestMap := readToolBundleManifestMap(t, bundleDir)
	if manifestMap["schema_version"] != "tool_bundle.v2" {
		t.Fatalf("expected v2 schema after migration, got %+v", manifestMap)
	}
	if _, ok := manifestMap["parameters"].(map[string]any); !ok {
		t.Fatalf("expected default parameters to be written, got %+v", manifestMap["parameters"])
	}
	if strings.TrimSpace(fmt.Sprint(manifestMap["description"])) == "" {
		t.Fatalf("expected default description to be written")
	}
	if _, ok := manifestMap["x_vendor"].(map[string]any); !ok {
		t.Fatalf("expected unknown vendor metadata to be preserved, got %+v", manifestMap)
	}
	manifest, err := loadInstalledToolBundleManifest(bundleDir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Provenance == nil || manifest.Provenance.Source != "local_manifest_migration" {
		t.Fatalf("expected local migration provenance, got %+v", manifest.Provenance)
	}
	if !containsTrimmedString(manifest.CapabilitySuites, "browser_read_only") || !containsTrimmedString(manifest.CapabilitySuites, "custom:legacy_visual") {
		t.Fatalf("expected migrated capability suites, got %+v", manifest.CapabilitySuites)
	}
	if _, ok := agent.registry.Get("legacy_probe"); !ok {
		t.Fatalf("expected legacy_probe to remain registered after migrate refresh")
	}
	statusAfter := toolBundleStatusSnapshot(agent)
	expectToolBundleSuiteReadiness(t, statusAfter.Suites, "browser_read_only", "ready", true, []string{"legacy_probe"}, nil)
}

func TestToolBundleRegistryToolMigrateDefaultsToAllLocalManifests(t *testing.T) {
	workdir := t.TempDir()
	for _, name := range []string{"alpha_probe", "beta_probe"} {
		bundleDir := filepath.Join(workdir, "tools", name)
		if err := os.MkdirAll(bundleDir, 0o755); err != nil {
			t.Fatal(err)
		}
		raw := mustJSONBytes(t, map[string]any{
			"name":    name,
			"command": []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		})
		if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "migrate"})
	if result == nil || result.IsError {
		t.Fatalf("tool_bundle_registry migrate all failed: %+v", result)
	}
	for _, name := range []string{"alpha_probe", "beta_probe"} {
		manifestMap := readToolBundleManifestMap(t, filepath.Join(workdir, "tools", name))
		if manifestMap["schema_version"] != "tool_bundle.v2" {
			t.Fatalf("expected %s to migrate to v2, got %+v", name, manifestMap)
		}
	}
}

func TestToolBundleRegistryToolMigratePreservesUnknownRawJSON(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, "tools", "legacy_probe")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	executable, err := json.Marshal(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	const bigNumber = "9007199254740993123"
	raw := []byte(fmt.Sprintf(`{
  "name": "legacy_probe",
  "command": [%s, "-test.run=TestInstalledToolBundleHelper", "--"],
  "parameters": {"type": "object", "x_precision": %s},
  "provenance": {"source": "third_party", "signature": "sig", "big": %s},
  "x_big": %s
}`, executable, bigNumber, bigNumber, bigNumber))
	if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "migrate", "name": "legacy_probe"})
	if result == nil || result.IsError {
		t.Fatalf("tool_bundle_registry migrate failed: %+v", result)
	}
	updated, err := os.ReadFile(filepath.Join(bundleDir, installedToolBundleManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(updated, &rawMap); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(rawMap["x_big"], []byte(bigNumber)) {
		t.Fatalf("expected top-level unknown big integer to be preserved, got %s", rawMap["x_big"])
	}
	if !bytes.Contains(rawMap["parameters"], []byte(bigNumber)) {
		t.Fatalf("expected existing parameters raw JSON to be preserved, got %s", rawMap["parameters"])
	}
	if !bytes.Contains(rawMap["provenance"], []byte(`"signature"`)) || !bytes.Contains(rawMap["provenance"], []byte(bigNumber)) {
		t.Fatalf("expected unknown provenance metadata to be preserved, got %s", rawMap["provenance"])
	}
}

func TestToolBundleRegistryToolMigrateNamedMalformedManifestIsToolError(t *testing.T) {
	workdir := t.TempDir()
	bundleDir := filepath.Join(workdir, "tools", "bad_probe")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), []byte(`{"name":`), 0o644); err != nil {
		t.Fatal(err)
	}
	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "migrate", "name": "bad_probe"})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "migration failed") {
		t.Fatalf("expected named malformed migration to be a tool error, got %+v", result)
	}
}

func TestToolBundleRegistryToolDownloadsZipBundleAndRefreshesAgent(t *testing.T) {
	workdir := t.TempDir()
	manifest := InstalledToolBundleManifest{
		SchemaVersion:    "tool_bundle.v2",
		Name:             "remote_probe",
		Description:      "remote visual probe",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		CapabilitySuites: []string{"browser_read_only", "custom:remote_visual"},
	}
	zipBytes := buildToolBundleZip(t, map[string][]byte{
		"remote_probe/" + installedToolBundleManifestName: mustJSONBytes(t, manifest),
		"remote_probe/probe.js":                           []byte("console.log('remote')\n"),
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(zipBytes)
	}))
	defer server.Close()
	agent := &Agent{
		Workdir: workdir,
		Anatomy: AgentAnatomyConfig{Heartbeats: []AgentHeartbeatSpec{{
			ID:         "visual_product_audit",
			Kind:       "browser_critic",
			ToolSuites: []string{"browser_read_only"},
		}}},
	}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "download", "source_url": server.URL + "/remote_probe.zip?token=secret"})
	if result == nil || result.IsError {
		t.Fatalf("tool_bundle_registry download failed: %+v", result)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(result.Output), &payload); err != nil {
		t.Fatalf("decode tool_bundle_registry output: %v", err)
	}
	if payload["refresh_applied"] != true || payload["available_next_llm_cycle"] != true {
		t.Fatalf("expected refresh/next-cycle markers, got %+v", payload)
	}
	localManifest := filepath.Join(workdir, ".runtime-config", "tool-bundles", "remote_probe", installedToolBundleManifestName)
	if !pathExists(localManifest) || !pathExists(filepath.Join(workdir, ".runtime-config", "tool-bundles", "remote_probe", "probe.js")) {
		t.Fatalf("expected downloaded bundle files under managed install root")
	}
	downloadedManifest, err := loadInstalledToolBundleManifest(filepath.Dir(localManifest))
	if err != nil {
		t.Fatal(err)
	}
	if downloadedManifest.Provenance == nil || strings.Contains(downloadedManifest.Provenance.Source, "token") || strings.Contains(downloadedManifest.Provenance.Source, "secret") {
		t.Fatalf("expected downloaded provenance to redact query secrets, got %+v", downloadedManifest.Provenance)
	}
	config, _, _ := loadInstalledToolBundleRegistryConfig(workdir)
	if !containsTrimmedString(config.Enabled, "remote_probe") {
		t.Fatalf("expected remote_probe enabled after download, got %+v", config)
	}
	if _, ok := agent.registry.Get("remote_probe"); !ok {
		t.Fatalf("expected remote_probe to register after download refresh, discovery=%+v", agent.toolBundleDiscovery)
	}
	snapshot := toolBundleStatusSnapshot(agent)
	expectToolBundleSuiteReadiness(t, snapshot.Suites, "browser_read_only", "ready", true, []string{"remote_probe"}, nil)
}

func TestToolBundleRegistryToolDownloadRejectsUnsafeZipEntry(t *testing.T) {
	workdir := t.TempDir()
	manifest := InstalledToolBundleManifest{
		Name:        "remote_probe",
		Description: "remote visual probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	}
	zipBytes := buildToolBundleZip(t, map[string][]byte{
		"remote_probe/" + installedToolBundleManifestName: mustJSONBytes(t, manifest),
		"remote_probe/../outside.txt":                     []byte("nope\n"),
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(zipBytes)
	}))
	defer server.Close()
	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "download", "source_url": server.URL + "/remote_probe.zip"})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "unsafe entry") {
		t.Fatalf("expected download to reject unsafe zip entry, got %+v", result)
	}
	if pathExists(filepath.Join(workdir, ".runtime-config", "tool-bundles", "remote_probe", installedToolBundleManifestName)) {
		t.Fatalf("unsafe zip entry should not install bundle")
	}
}

func TestToolBundleRegistryToolDownloadRejectsUncompressedZipLimit(t *testing.T) {
	workdir := t.TempDir()
	manifest := InstalledToolBundleManifest{
		Name:        "remote_probe",
		Description: "remote visual probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	}
	zipBytes := buildToolBundleZip(t, map[string][]byte{
		"remote_probe/" + installedToolBundleManifestName: mustJSONBytes(t, manifest),
		"remote_probe/large.txt":                          bytes.Repeat([]byte("x"), 8*1024),
	})
	if len(zipBytes) >= 2048 {
		t.Fatalf("test archive should stay below compressed max_bytes, got %d bytes", len(zipBytes))
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(zipBytes)
	}))
	defer server.Close()
	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "download", "source_url": server.URL + "/remote_probe.zip", "max_bytes": 2048})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "uncompressed") {
		t.Fatalf("expected download to reject uncompressed zip payload over max_bytes, got %+v", result)
	}
	if pathExists(filepath.Join(workdir, ".runtime-config", "tool-bundles", "remote_probe", installedToolBundleManifestName)) {
		t.Fatalf("oversized uncompressed zip should not install bundle")
	}
}

func TestToolBundleRegistryToolDownloadHasBoundedTimeout(t *testing.T) {
	workdir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "download", "source_url": server.URL + "/hang.zip", "download_timeout_ms": 10})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "deadline") {
		t.Fatalf("expected hanging download to hit bounded timeout, got %+v", result)
	}
}

func TestToolBundleRegistryToolDownloadRequiresNameForMultiBundleZip(t *testing.T) {
	workdir := t.TempDir()
	alpha := InstalledToolBundleManifest{
		Name:        "alpha_probe",
		Description: "alpha probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	}
	beta := InstalledToolBundleManifest{
		Name:        "beta_probe",
		Description: "beta probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	}
	zipBytes := buildToolBundleZip(t, map[string][]byte{
		"alpha/" + installedToolBundleManifestName: mustJSONBytes(t, alpha),
		"beta/" + installedToolBundleManifestName:  mustJSONBytes(t, beta),
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(zipBytes)
	}))
	defer server.Close()
	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	ambiguous := tool.Execute(context.Background(), map[string]any{"action": "download", "source_url": server.URL + "/bundle.zip"})
	if ambiguous == nil || !ambiguous.IsError || !strings.Contains(ambiguous.Output, "multiple tool bundles") {
		t.Fatalf("expected ambiguous multi-bundle zip to require name, got %+v", ambiguous)
	}
	selected := tool.Execute(context.Background(), map[string]any{"action": "download", "source_url": server.URL + "/bundle.zip", "name": "beta_probe"})
	if selected == nil || selected.IsError {
		t.Fatalf("expected named bundle download to succeed, got %+v", selected)
	}
	if _, ok := agent.registry.Get("beta_probe"); !ok {
		t.Fatalf("expected selected beta_probe to register, discovery=%+v", agent.toolBundleDiscovery)
	}
	if _, ok := agent.registry.Get("alpha_probe"); ok {
		t.Fatalf("unselected alpha_probe should not register")
	}
}

func TestToolBundleRegistryToolDisablesBundleAndRefreshesAgent(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{Enabled: []string{"external_probe"}})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "external_probe"), InstalledToolBundleManifest{
		Name:        "external_probe",
		Description: "third-party local probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	agent := &Agent{Workdir: workdir}
	agent.Init()
	if _, ok := agent.registry.Get("external_probe"); !ok {
		t.Fatalf("expected external_probe before disable")
	}
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "disable", "name": "external_probe"})
	if result == nil || result.IsError {
		t.Fatalf("tool_bundle_registry disable failed: %+v", result)
	}
	if _, ok := agent.registry.Get("external_probe"); ok {
		t.Fatalf("external_probe should not remain registered after disable, discovery=%+v", agent.toolBundleDiscovery)
	}
	config, _, _ := loadInstalledToolBundleRegistryConfig(workdir)
	if containsTrimmedString(config.Enabled, "external_probe") || !containsTrimmedString(config.Disabled, "external_probe") {
		t.Fatalf("expected external_probe moved from enabled to disabled, got %+v", config)
	}
}

func TestToolBundleRegistryToolMutationDisabledStillLists(t *testing.T) {
	workdir := t.TempDir()
	agent := &Agent{Workdir: workdir, DisableLocalMutation: true}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}
	list := tool.Execute(context.Background(), map[string]any{"action": "list"})
	if list == nil || list.IsError {
		t.Fatalf("tool_bundle_registry list should work without mutation capability: %+v", list)
	}
	register := tool.Execute(context.Background(), map[string]any{"action": "register", "name": "external_probe"})
	if register == nil || !register.IsError || !strings.Contains(register.Output, "mutation is disabled") {
		t.Fatalf("expected registry mutation to be blocked when local mutation is disabled, got %+v", register)
	}
}

func TestToolBundleRegistryToolScaffoldsSelfWrittenBundle(t *testing.T) {
	workdir := t.TempDir()
	agent := &Agent{
		Workdir: workdir,
		Anatomy: AgentAnatomyConfig{Heartbeats: []AgentHeartbeatSpec{{
			ID:         "visual_product_audit",
			Kind:       "browser_critic",
			ToolSuites: []string{"browser_read_only"},
		}}},
	}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{
		"action":            "scaffold",
		"name":              "custom_probe",
		"description":       "Self-written browser read probe",
		"runtime":           "node",
		"capability_suites": []any{"browser_read_only", "custom:visual_probe"},
	})
	if result == nil || result.IsError {
		t.Fatalf("tool_bundle_registry scaffold failed: %+v", result)
	}
	if !pathExists(filepath.Join(workdir, "tools", "custom_probe", installedToolBundleManifestName)) || !pathExists(filepath.Join(workdir, "tools", "custom_probe", "tool.js")) {
		t.Fatalf("expected scaffold to write tool.json and node template")
	}
	config, status, diagnostics := loadInstalledToolBundleRegistryConfig(workdir)
	if len(diagnostics) != 0 || !status.Present || !containsTrimmedString(config.Enabled, "custom_probe") {
		t.Fatalf("expected scaffolded bundle enabled in registry config, status=%+v config=%+v diagnostics=%+v", status, config, diagnostics)
	}
	if _, ok := agent.registry.Get("custom_probe"); !ok {
		t.Fatalf("expected scaffold refresh to expose custom_probe in current registry, discovery=%+v", agent.toolBundleDiscovery)
	}
	snapshot := toolBundleStatusSnapshot(agent)
	expectToolBundleSuiteReadiness(t, snapshot.Suites, "browser_read_only", "ready", true, []string{"custom_probe"}, nil)
}

func TestToolBundleRegistryToolScaffoldRefusesExistingPartialDirectoryWithoutOverwrite(t *testing.T) {
	workdir := t.TempDir()
	partialDir := filepath.Join(workdir, "tools", "custom_probe")
	if err := os.MkdirAll(partialDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partialDir, "tool.js"), []byte("console.log('partial')\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "scaffold", "name": "custom_probe"})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "already exists") {
		t.Fatalf("expected scaffold to refuse existing partial directory, got %+v", result)
	}
	raw, err := os.ReadFile(filepath.Join(partialDir, "tool.js"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "partial") {
		t.Fatalf("scaffold clobbered existing partial tool without overwrite: %s", string(raw))
	}

	overwrite := tool.Execute(context.Background(), map[string]any{"action": "scaffold", "name": "custom_probe", "overwrite": true})
	if overwrite == nil || overwrite.IsError {
		t.Fatalf("expected scaffold overwrite to succeed, got %+v", overwrite)
	}
	if !pathExists(filepath.Join(partialDir, installedToolBundleManifestName)) {
		t.Fatalf("expected overwrite scaffold to create manifest")
	}
}

func TestToolBundleRegistryToolInstallRefusesSourceInsideDestination(t *testing.T) {
	workdir := t.TempDir()
	sourceDir := filepath.Join(workdir, ".runtime-config", "tool-bundles", "custom_probe", "nested_source")
	writeInstalledToolBundleManifest(t, sourceDir, InstalledToolBundleManifest{
		Name:        "custom_probe",
		Description: "nested source probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "install", "source_path": sourceDir})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "must not be inside destination") {
		t.Fatalf("expected install to refuse source inside destination, got %+v", result)
	}
	if !pathExists(filepath.Join(sourceDir, installedToolBundleManifestName)) {
		t.Fatalf("install deleted source nested under destination")
	}
}

func TestRefreshInstalledToolBundlesPreservesDynamicTools(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{Enabled: []string{"external_probe"}})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "external_probe"), InstalledToolBundleManifest{
		Name:        "external_probe",
		Description: "third-party local probe",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	agent := &Agent{Workdir: workdir}
	agent.SetDynamicTools([]Tool{&registryTestTool{name: "workspace_dynamic_tool", description: "dynamic workspace tool"}})

	report := agent.RefreshInstalledToolBundles()
	if len(report.Installed) != 1 || report.Installed[0].Name != "external_probe" {
		t.Fatalf("expected external_probe installed after refresh, got %+v", report)
	}
	if _, ok := agent.registry.Get("external_probe"); !ok {
		t.Fatalf("expected external_probe after installed bundle refresh")
	}
	if _, ok := agent.registry.Get("workspace_dynamic_tool"); !ok {
		t.Fatalf("installed bundle refresh must preserve dynamic workspace/MCP tools")
	}
}

func TestRefreshInstalledToolBundlesDoesNotShadowDynamicToolCollision(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{Enabled: []string{"workspace_dynamic_tool"}})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "workspace_dynamic_tool"), InstalledToolBundleManifest{
		Name:        "workspace_dynamic_tool",
		Description: "colliding local bundle",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	dynamic := &registryTestTool{name: "workspace_dynamic_tool", description: "dynamic workspace tool"}
	agent := &Agent{Workdir: workdir}
	agent.SetDynamicTools([]Tool{dynamic})

	report := agent.RefreshInstalledToolBundles()
	active, ok := agent.registry.Get("workspace_dynamic_tool")
	if !ok {
		t.Fatalf("expected dynamic tool to remain registered")
	}
	if active != dynamic {
		t.Fatalf("installed bundle shadowed dynamic tool: got %T", active)
	}
	if len(report.Collisions) != 1 || report.Collisions[0].Code != "duplicate_tool_registration" {
		t.Fatalf("expected bundle collision against dynamic tool, got %+v", report.Collisions)
	}
	if len(report.Installed) != 0 {
		t.Fatalf("colliding bundle should not remain installed in report, got %+v", report.Installed)
	}
}

func TestToolBundleRegistryToolStatusUsesRealRegistryForCoreCollision(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleRegistryConfig(t, workdir, InstalledToolBundleRegistryConfig{Enabled: []string{"read_file"}})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "read_file"), InstalledToolBundleManifest{
		Name:        "read_file",
		Description: "colliding local bundle",
		Command:     []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:  map[string]any{"type": "object"},
	})
	agent := &Agent{Workdir: workdir}
	agent.Init()
	tool, ok := agent.registry.Get("tool_bundle_registry")
	if !ok {
		t.Fatalf("expected tool_bundle_registry core tool")
	}

	result := tool.Execute(context.Background(), map[string]any{"action": "status"})
	if result == nil || result.IsError {
		t.Fatalf("tool_bundle_registry status failed: %+v", result)
	}
	var payload struct {
		RegisteredTools []string                  `json:"registered_tools"`
		Discovery       ToolBundleDiscoveryReport `json:"discovery"`
	}
	if err := json.Unmarshal([]byte(result.Output), &payload); err != nil {
		t.Fatalf("decode tool_bundle_registry status output: %v", err)
	}
	if containsTrimmedString(payload.RegisteredTools, "read_file") {
		t.Fatalf("status falsely reported core-shadowing bundle as registered: %+v", payload)
	}
	if len(payload.Discovery.Collisions) != 1 || payload.Discovery.Collisions[0].Code != "duplicate_tool_registration" {
		t.Fatalf("expected real core collision in status discovery, got %+v", payload.Discovery.Collisions)
	}
	active, ok := agent.registry.Get("read_file")
	if !ok {
		t.Fatalf("expected core read_file to remain registered")
	}
	if _, shadowed := active.(*InstalledToolBundleTool); shadowed {
		t.Fatalf("core read_file was shadowed by installed bundle")
	}
}

func TestToolBundleSuiteReadinessMatchesHeartbeatSuitesToManifestCapabilities(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "visual_probe"), InstalledToolBundleManifest{
		Name:             "visual_probe",
		Description:      "third-party visual probe",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		CapabilitySuites: []string{"browser_read_only", "screenshot_capture"},
	})
	agent := &Agent{
		Workdir: workdir,
		Anatomy: AgentAnatomyConfig{Heartbeats: []AgentHeartbeatSpec{{
			ID:         "visual_product_audit",
			Kind:       "browser_critic",
			ToolSuites: []string{"browser_read_only", "screenshot_capture", "browser_interactive"},
		}}},
	}
	agent.Init()

	status := toolBundleStatusSnapshot(agent)
	if status.SuiteCount != 3 || status.Status != "degraded" {
		t.Fatalf("expected three suites with one missing degrading contour, got %+v", status)
	}
	expectToolBundleSuiteReadiness(t, status.Suites, "browser_read_only", "ready", true, []string{"visual_probe"}, nil)
	expectToolBundleSuiteReadiness(t, status.Suites, "screenshot_capture", "ready", true, []string{"visual_probe"}, nil)
	expectToolBundleSuiteReadiness(t, status.Suites, "browser_interactive", "missing", true, nil, nil)
	missingInteractive := findToolBundleSuiteReadiness(status.Suites, "browser_interactive")
	if missingInteractive == nil || !containsTrimmedString(missingInteractive.SuggestedBundles, "browser_session") || len(missingInteractive.SuggestedActions) == 0 {
		t.Fatalf("missing browser_interactive should carry actionable remediation, got %+v", missingInteractive)
	}

	section := buildInstalledToolBundlePromptSectionWithReportAndSuites(agent.registry, agent.toolBundleDiscovery, status.Suites)
	for _, want := range []string{
		"Tool suite readiness:",
		"browser_read_only status=ready required=true heartbeats=visual_product_audit bundles=visual_probe",
		"browser_interactive status=missing required=true",
		"suggested_bundles=browser_session",
		"next_action=",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("expected suite readiness prompt to contain %q, got:\n%s", want, section)
		}
	}
}

func TestToolBundleSuiteReadinessPreservesBlockedBundleDiagnostics(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "visual_blocked"), InstalledToolBundleManifest{
		Name:             "visual_blocked",
		Description:      "blocked visual probe",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		CapabilitySuites: []string{"screenshot_capture"},
		Dependencies:     []InstalledToolBundleDependency{{Name: "rhizome_missing_suite_dependency_for_tool_bundle_test", Kind: "executable", Required: true}},
	})
	agent := &Agent{
		Workdir: workdir,
		Anatomy: AgentAnatomyConfig{Heartbeats: []AgentHeartbeatSpec{{
			ID:         "visual_product_audit",
			Kind:       "browser_critic",
			ToolSuites: []string{"screenshot_capture"},
		}}},
	}
	agent.Init()

	status := toolBundleStatusSnapshot(agent)
	if status.Status != "degraded" || status.InstalledCount != 0 {
		t.Fatalf("blocked suite should degrade without registering executable bundle, got %+v", status)
	}
	expectToolBundleReadiness(t, status.Candidates, "visual_blocked", "blocked", "missing_dependency", false, true)
	expectToolBundleSuiteReadiness(t, status.Suites, "screenshot_capture", "blocked", true, nil, []string{"visual_blocked"})
	blockedSuite := findToolBundleSuiteReadiness(status.Suites, "screenshot_capture")
	if blockedSuite == nil || !containsTrimmedString(blockedSuite.SuggestedBundles, "browser_visual_probe") || !strings.Contains(strings.Join(blockedSuite.SuggestedActions, "\n"), "fix the blocked bundle") {
		t.Fatalf("blocked screenshot_capture should carry dependency remediation, got %+v", blockedSuite)
	}
}

func TestToolBundleSuiteReadinessUsesNativeProvidersForCoreSuites(t *testing.T) {
	agent := &Agent{
		Workdir: t.TempDir(),
		Anatomy: AgentAnatomyConfig{Heartbeats: []AgentHeartbeatSpec{{
			ID:         "loop_self_check",
			Kind:       "metacognition",
			ToolSuites: []string{"memory_and_docs_read", "local_log_read"},
		}}},
	}
	agent.Init()

	status := toolBundleStatusSnapshot(agent)
	if status.Status != "enabled" {
		t.Fatalf("native core suites should not degrade bundle contour, got %+v", status)
	}
	expectToolBundleSuiteReadiness(t, status.Suites, "memory_and_docs_read", "ready", true, nil, nil)
	expectToolBundleSuiteReadiness(t, status.Suites, "local_log_read", "ready", true, nil, nil)
	for _, suite := range status.Suites {
		if suite.Status == "ready" && len(suite.NativeProviders) == 0 {
			t.Fatalf("expected ready native suite to name native providers, got %+v", suite)
		}
	}
}

func TestCapabilitySnapshotAndRuntimeStatusShareToolBundleSuiteReadiness(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "visual_probe"), InstalledToolBundleManifest{
		Name:             "visual_probe",
		Description:      "third-party visual probe",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		CapabilitySuites: []string{"browser_read_only"},
	})
	agent := &Agent{
		Workdir: workdir,
		Anatomy: AgentAnatomyConfig{Heartbeats: []AgentHeartbeatSpec{{
			ID:         "visual_product_audit",
			Kind:       "browser_critic",
			ToolSuites: []string{"browser_read_only"},
		}}},
	}
	agent.Init()

	status := toolBundleStatusSnapshot(agent)
	expectToolBundleSuiteReadiness(t, status.Suites, "browser_read_only", "ready", true, []string{"visual_probe"}, nil)
	surface, ok := capabilityToolBundleSurface(CapabilitySurfaceIdentity{}, agent)
	if !ok {
		t.Fatalf("expected tool bundle capability surface")
	}
	raw, err := json.Marshal(surface.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	metadata := string(raw)
	for _, want := range []string{"suite_count", "browser_read_only", "visual_product_audit", "visual_probe"} {
		if !strings.Contains(metadata, want) {
			t.Fatalf("expected capability metadata to contain %q, got %s", want, metadata)
		}
	}
}

func TestCapabilitySnapshotIncludesNativeOnlySuiteReadiness(t *testing.T) {
	agent := &Agent{
		Workdir: t.TempDir(),
		Anatomy: AgentAnatomyConfig{Heartbeats: []AgentHeartbeatSpec{{
			ID:         "loop_self_check",
			Kind:       "metacognition",
			ToolSuites: []string{"memory_and_docs_read"},
		}}},
	}
	agent.Init()
	status := toolBundleStatusSnapshot(agent)
	expectToolBundleSuiteReadiness(t, status.Suites, "memory_and_docs_read", "ready", true, nil, nil)

	surface, ok := capabilityToolBundleSurface(CapabilitySurfaceIdentity{}, agent)
	if !ok {
		t.Fatalf("expected native-only suite readiness to surface in capability snapshot")
	}
	raw, err := json.Marshal(surface.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	metadata := string(raw)
	for _, want := range []string{"memory_and_docs_read", "native_providers", "read_file"} {
		if !strings.Contains(metadata, want) {
			t.Fatalf("expected native-only suite metadata to contain %q, got %s", want, metadata)
		}
	}
}

func TestDuplicateBundleNameReservesBrokenFirstCandidate(t *testing.T) {
	workdir := t.TempDir()
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, ".runtime-config", "tool-bundles", "managed_probe"), InstalledToolBundleManifest{
		Name:             "shared_probe",
		Description:      "managed but blocked probe",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		CapabilitySuites: []string{"browser_read_only"},
		Dependencies:     []InstalledToolBundleDependency{{Name: "rhizome_missing_duplicate_dependency_for_tool_bundle_test", Kind: "executable", Required: true}},
	})
	writeInstalledToolBundleManifest(t, filepath.Join(workdir, "tools", "legacy_probe"), InstalledToolBundleManifest{
		Name:             "shared_probe",
		Description:      "healthy legacy probe",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		CapabilitySuites: []string{"browser_read_only"},
	})

	report := RegisterInstalledToolBundles(NewToolRegistry(), workdir)
	if len(report.Installed) != 0 {
		t.Fatalf("blocked first candidate should prevent fallback registration, got installed=%+v report=%+v", report.Installed, report)
	}
	expectToolBundleReadiness(t, report.Candidates, "shared_probe", "collided", "duplicate_tool_bundle", false, false)
	if len(report.Collisions) != 1 || !strings.Contains(report.Collisions[0].Message, "reserves that bundle name") {
		t.Fatalf("expected duplicate diagnostic to explain reserved broken-first precedence, got %+v", report.Collisions)
	}
}

func TestBrowserVisualProbeDiscoversBrowserWithHeadlessProbe(t *testing.T) {
	source := filepath.Join("tool_library", "browser_visual_probe", "browser_visual_probe.js")
	raw, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, want := range []string{
		"function browserSupportsHeadless",
		"function analyzePrimarySurfaceRisk",
		"--headless=new",
		"--dump-dom",
		"about:blank",
		"windowsHide: true",
		"primary_surface_analysis",
		"board_cells_without_visible_css",
		"visual_quality_status",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("browser_visual_probe.js should contain %q", want)
		}
	}
	if strings.Contains(text, `["--version"]`) {
		t.Fatalf("browser discovery must not use --version because it opens existing Chrome/Edge sessions on Windows")
	}
}

func TestBrowserSessionToolBundleExposesInteractiveActions(t *testing.T) {
	for _, path := range []string{
		filepath.Join("tool_library", "browser_session", installedToolBundleManifestName),
		filepath.Join("tool_library", "browser_session", "browser_session.js"),
	} {
		if !pathExists(path) {
			t.Fatalf("expected browser_session bundle file %s", path)
		}
	}
	raw, err := os.ReadFile(filepath.Join("tool_library", "browser_session", "browser_session.js"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, want := range []string{
		"action",
		"Page.captureScreenshot",
		"Input.dispatchMouseEvent",
		"Input.insertText",
		"Runtime.evaluate",
		"remote-debugging-port",
		"--remote-debugging-pipe",
		"PipeCDP",
		"pipe_broker_remote_debugging_pipe",
		"open browser session failed",
		"close_all",
		"closeOwnedSession",
		"max_browser_sessions",
		"browser session limit reached",
		"withSessionLock",
		"browser session action timed out waiting for session lock",
		"waitForSessionStateBeforeAction",
		"activeSessionLocks",
		`process.on("exit"`,
		"verifySessionReady",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("browser_session.js should contain %q", want)
		}
	}
}

func TestBrowserSessionToolBundleSerializesStatefulActions(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("tool_library", "browser_session", "browser_session.js"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, want := range []string{
		`path.join(SESSION_ROOT, ".locks"`,
		`entry.name !== ".locks"`,
		`entry.name === ".locks"`,
		`case "open":`,
		`return withSessionLock(paths, action, () => openSession(paths, args))`,
		`case "inspect":`,
		`return withSessionLock(paths, action, async () => inspect(await requireState(paths), paths, args))`,
		`RHIZOME_BROWSER_SESSION_STATE_WAIT_MS`,
		`process.on("exit"`,
		`activeSessionLocks.push(lock)`,
		`lock.released = true`,
		`async function waitForBrowserReady`,
		`browser process exited before CDP ready`,
		`waitForDevToolsPort`,
		`DevToolsActivePort`,
		`remote-debugging-port=${requestedPort}`,
		`--remote-debugging-port=0`,
		`--remote-debugging-pipe`,
		`--pipe-broker`,
		`PipeCDP`,
		`brokerRequest`,
		`waitForBrokerReady`,
		`pipe_broker_remote_debugging_pipe`,
		`pipe_broker_no_tcp_cdp`,
		`pipe_broker_first`,
		`RHIZOME_BROWSER_SESSION_PIPE_BROKER_FIRST`,
		`pipe_broker_primary_for_win32_headless`,
		`powershell_start_process_pipe_broker`,
		`RHIZOME_BROWSER_SESSION_PIPE_BROKER_ONLY`,
		`RHIZOME_BROWSER_SESSION_TCP_FIRST`,
		`RHIZOME_BROWSER_SESSION_ALLOW_TCP_FALLBACK`,
		`RHIZOME_BROWSER_SESSION_DISABLE_TCP_FALLBACK_AFTER_PIPE`,
		`tcp_fallback`,
		`disable_tcp_fallback`,
		`tcp_cdp_fallback_after_pipe_broker_failure`,
		`tcp_fallback_after_pipe_broker_failure`,
		`RHIZOME_BROWSER_SESSION_PIPE_BROKER_ATTEMPTS`,
		`pipe_broker_attempts`,
		`pipe_broker_primary_retry_for_win32_headless`,
		`RHIZOME_BROWSER_SESSION_PIPE_BROKER_WIN32_START_PROCESS`,
		`node_spawn_detached_pipe_broker`,
		`RHIZOME_BROWSER_SESSION_PIPE_CDP_TIMEOUT_MS`,
		`defaultOpenTimeoutMs`,
		`90000`,
		`RHIZOME_BROWSER_SESSION_PIPE_BROKER_CHILD`,
		`dynamicDebugPort`,
		`daemon_safe_allocated_headless_port`,
		`debug_port_source`,
		`launchBrowserProcess`,
		`psSingleQuoted`,
		`powershell_start_process`,
		`prefer_start_process`,
		`attempt % 2 === 0`,
		`headless_node_spawn_for_daemon_cdp`,
		`RHIZOME_BROWSER_SESSION_WIN32_START_PROCESS`,
		`RHIZOME_BROWSER_SESSION_WIN32_FORCE_SPAWN`,
		`headless_fallback_after_headful_cdp_failure`,
		`headless_fallback_used`,
		`headful_open_attempts`,
		`RHIZOME_BROWSER_SESSION_OPEN_TIMEOUT_MS`,
		`open_timeout_ms`,
		`await verifySessionReady(state, 8000)`,
		`saveState(paths, state)`,
		`await waitForCDP(state.port, 8000)`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("browser_session.js should serialize actions and verify durable readiness; missing %q", want)
		}
	}
	openStart := strings.Index(text, "async function openSession")
	requireStart := strings.Index(text, "async function requireState")
	if openStart < 0 || requireStart <= openStart {
		t.Fatalf("could not locate openSession body")
	}
	openBody := text[openStart:requireStart]
	if strings.Index(openBody, "await verifySessionReady(state, 8000)") > strings.Index(openBody, "saveState(paths, state)") {
		t.Fatalf("open must verify session readiness before saving pass-ready session state")
	}
}

func TestInstalledToolBundleHelper(t *testing.T) {
	if os.Getenv("RHIZOME_TOOL_BUNDLE_HELPER") != "1" {
		return
	}
	var input map[string]any
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		fmt.Printf(`{"error":%q}`, err.Error())
		os.Exit(1)
	}
	output := map[string]any{
		"input":               input,
		"bundle_dir":          os.Getenv("RHIZOME_TOOL_BUNDLE_DIR") != "",
		"artifact_root":       os.Getenv("RHIZOME_TOOL_ARTIFACT_DIR") != "",
		"persistent_children": os.Getenv("RHIZOME_TOOL_BUNDLE_PERSISTENT_CHILDREN"),
	}
	if status, _ := input["contract_status"].(string); status != "" {
		output["contract_version"] = "test_bundle_result_v1"
		output["status"] = status
		output["reason"] = input["reason"]
	}
	raw, _ := json.Marshal(output)
	if artifactOnly, _ := input["artifact_result_only"].(bool); artifactOnly {
		output["contract_version"] = "test_bundle_result_v1"
		output["status"] = "pass"
		raw, _ = json.Marshal(output)
		artifactRoot := os.Getenv("RHIZOME_TOOL_ARTIFACT_DIR")
		if artifactRoot == "" {
			fmt.Print("missing artifact root")
			os.Exit(1)
		}
		if err := os.WriteFile(filepath.Join(artifactRoot, "result.json"), raw, 0o644); err != nil {
			fmt.Print(err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}
	fmt.Print(string(raw))
	os.Exit(0)
}

func TestInstalledToolBundleHealthcheckHelper(t *testing.T) {
	if os.Getenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_HELPER") != "1" {
		return
	}
	if os.Getenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_FAIL") == "1" {
		fmt.Print("health failed")
		os.Exit(2)
	}
	fmt.Print("health ok")
	os.Exit(0)
}

func writeInstalledToolBundleManifest(t *testing.T, bundleDir string, manifest InstalledToolBundleManifest) {
	t.Helper()
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundleDir, installedToolBundleManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustJSONBytes(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func buildToolBundleZip(t *testing.T, entries map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := zip.NewWriter(&buf)
	for name, raw := range entries {
		file, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func readToolBundleManifestMap(t *testing.T, bundleDir string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(bundleDir, installedToolBundleManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func writeInstalledToolBundleRegistryConfig(t *testing.T, workdir string, config InstalledToolBundleRegistryConfig) {
	t.Helper()
	configDir := filepath.Join(workdir, ".runtime-config")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, installedToolBundleConfigName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func expectToolBundleReadiness(t *testing.T, items []ToolBundleReadinessItem, name, status, code string, registered, configured bool) {
	t.Helper()
	for _, item := range items {
		if item.Name != name {
			continue
		}
		if item.Status != status || item.Code != code || item.Registered != registered || item.Configured != configured {
			t.Fatalf("readiness for %s = %+v, want status=%s code=%s registered=%v configured=%v", name, item, status, code, registered, configured)
		}
		return
	}
	t.Fatalf("readiness for %s not found in %+v", name, items)
}

func expectToolBundleSuiteReadiness(t *testing.T, items []ToolBundleSuiteReadinessItem, suite, status string, required bool, readyBundles, blockedBundles []string) {
	t.Helper()
	for _, item := range items {
		if item.Suite != suite {
			continue
		}
		if item.Status != status || item.Required != required {
			t.Fatalf("suite readiness for %s = %+v, want status=%s required=%v", suite, item, status, required)
		}
		for _, bundle := range readyBundles {
			if !containsTrimmedString(item.ReadyBundles, bundle) {
				t.Fatalf("suite readiness for %s missing ready bundle %s: %+v", suite, bundle, item)
			}
		}
		for _, bundle := range blockedBundles {
			if !containsTrimmedString(item.BlockedBundles, bundle) {
				t.Fatalf("suite readiness for %s missing blocked bundle %s: %+v", suite, bundle, item)
			}
		}
		return
	}
	t.Fatalf("suite readiness for %s not found in %+v", suite, items)
}

func findToolBundleSuiteReadiness(items []ToolBundleSuiteReadinessItem, suite string) *ToolBundleSuiteReadinessItem {
	for idx := range items {
		if items[idx].Suite == suite {
			return &items[idx]
		}
	}
	return nil
}

func prependFakeBrowserToPath(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	name := "chrome"
	if goruntime.GOOS == "windows" {
		name = "chrome.cmd"
	}
	path := filepath.Join(dir, name)
	body := `#!/bin/sh
dump_dom=0
for arg in "$@"; do
	case "$arg" in
		--screenshot=*)
			out="${arg#--screenshot=}"
			mkdir -p "$(dirname "$out")"
			printf 'fake browser png' > "$out"
			exit 0
			;;
		--dump-dom)
			dump_dom=1
			;;
	esac
done
if [ "$dump_dom" = "1" ]; then
	printf '<html><body>rhizome-browser-visual-probe-healthcheck</body></html>'
fi
exit 0
`
	if goruntime.GOOS == "windows" {
		body = "@echo off\r\nsetlocal enabledelayedexpansion\r\nset DUMP_DOM=0\r\nfor %%A in (%*) do (\r\n  set ARG=%%~A\r\n  if \"!ARG:~0,13!\"==\"--screenshot=\" (\r\n    set OUT=!ARG:~13!\r\n    for %%D in (\"!OUT!\") do if not exist \"%%~dpD\" mkdir \"%%~dpD\"\r\n    >\"!OUT!\" echo fake browser png\r\n    exit /b 0\r\n  )\r\n  if \"!ARG!\"==\"--dump-dom\" set DUMP_DOM=1\r\n)\r\nif \"%DUMP_DOM%\"==\"1\" echo ^<html^>^<body^>rhizome-browser-visual-probe-healthcheck^</body^>^</html^>\r\nexit /b 0\r\n"
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("RHIZOME_BROWSER_CANDIDATES", "chrome")
}
