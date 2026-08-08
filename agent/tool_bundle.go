package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"sort"
	"strings"
	"time"
)

const (
	installedToolBundleManifestName = "tool.json"
	installedToolBundleConfigName   = "tool-bundles.json"
	installedToolBundleConfigSchema = "tool_bundle_registry.v1"
	installedToolBundleDefaultLimit = 180 * time.Second
	installedToolBundleMaxLimit     = 300 * time.Second
)

var installedToolBundleRunner = runInstalledToolBundleCommand
var operatorToolExecutablePath = os.Executable
var operatorToolLibraryRootFunc = operatorToolLibraryRoot

type InstalledToolBundleManifest struct {
	SchemaVersion     string                                `json:"schema_version,omitempty"`
	Name              string                                `json:"name"`
	Description       string                                `json:"description"`
	Command           []string                              `json:"command"`
	TimeoutSeconds    int                                   `json:"timeout_seconds,omitempty"`
	Parameters        map[string]any                        `json:"parameters"`
	Version           string                                `json:"version,omitempty"`
	CapabilitySuites  []string                              `json:"capability_suites,omitempty"`
	ArtifactContracts []InstalledToolBundleArtifactContract `json:"artifact_contracts,omitempty"`
	Healthcheck       *InstalledToolBundleHealthcheck       `json:"healthcheck,omitempty"`
	Dependencies      []InstalledToolBundleDependency       `json:"dependencies,omitempty"`
	Concurrency       *InstalledToolBundleConcurrency       `json:"concurrency,omitempty"`
	Provenance        *InstalledToolBundleProvenance        `json:"provenance,omitempty"`
}

type InstalledToolBundleArtifactContract struct {
	Name        string `json:"name,omitempty"`
	Type        string `json:"type,omitempty"`
	Path        string `json:"path,omitempty"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
}

type InstalledToolBundleHealthcheck struct {
	Command        []string `json:"command,omitempty"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty"`
}

type InstalledToolBundleDependency struct {
	Name     string `json:"name,omitempty"`
	Kind     string `json:"kind,omitempty"`
	Version  string `json:"version,omitempty"`
	Required bool   `json:"required,omitempty"`
}

type InstalledToolBundleConcurrency struct {
	MaxParallel int  `json:"max_parallel,omitempty"`
	Exclusive   bool `json:"exclusive,omitempty"`
}

type InstalledToolBundleProvenance struct {
	Source    string `json:"source,omitempty"`
	Revision  string `json:"revision,omitempty"`
	Installed string `json:"installed,omitempty"`
}

type InstalledToolBundleRegistryConfig struct {
	SchemaVersion string   `json:"schema_version,omitempty"`
	Mode          string   `json:"mode,omitempty"`
	Enabled       []string `json:"enabled,omitempty"`
	Disabled      []string `json:"disabled,omitempty"`
}

type InstalledToolBundleTool struct {
	workdir  string
	dir      string
	manifest InstalledToolBundleManifest
}

type InstalledToolBundlePromptItem struct {
	Name              string
	Description       string
	Version           string
	CapabilitySuites  []string
	ArtifactContracts []string
	Dependencies      []string
	TimeoutSeconds    int
}

type ToolBundleStatusSnapshot struct {
	Schema            string                          `json:"schema,omitempty"`
	Status            string                          `json:"status"`
	Workdir           string                          `json:"workdir,omitempty"`
	PromptVisible     bool                            `json:"prompt_visible"`
	ToolVisible       bool                            `json:"tool_visible"`
	CopyInContract    string                          `json:"copy_in_contract,omitempty"`
	CandidateCount    int                             `json:"candidate_count"`
	SuiteCount        int                             `json:"suite_count"`
	InstalledCount    int                             `json:"installed_count"`
	DependencyCount   int                             `json:"dependency_count"`
	HealthcheckCount  int                             `json:"healthcheck_count"`
	ConfigCount       int                             `json:"config_count"`
	SkippedCount      int                             `json:"skipped_count"`
	ErrorCount        int                             `json:"error_count"`
	CollisionCount    int                             `json:"collision_count"`
	Roots             []ToolBundleDiscoveryRoot       `json:"roots,omitempty"`
	Config            ToolBundleRegistryConfigStatus  `json:"config,omitempty"`
	Candidates        []ToolBundleReadinessItem       `json:"candidates,omitempty"`
	Suites            []ToolBundleSuiteReadinessItem  `json:"suites,omitempty"`
	Installed         []ToolBundleStatusItem          `json:"installed,omitempty"`
	Dependencies      []ToolBundleDiscoveryDiagnostic `json:"dependencies,omitempty"`
	Healthchecks      []ToolBundleDiscoveryDiagnostic `json:"healthchecks,omitempty"`
	ConfigDiagnostics []ToolBundleDiscoveryDiagnostic `json:"config_diagnostics,omitempty"`
	Skipped           []ToolBundleDiscoveryDiagnostic `json:"skipped,omitempty"`
	Errors            []ToolBundleDiscoveryDiagnostic `json:"errors,omitempty"`
	Collisions        []ToolBundleDiscoveryDiagnostic `json:"collisions,omitempty"`
}

type ToolBundleStatusItem struct {
	Name              string   `json:"name"`
	Description       string   `json:"description,omitempty"`
	Version           string   `json:"version,omitempty"`
	CapabilitySuites  []string `json:"capability_suites,omitempty"`
	ArtifactContracts []string `json:"artifact_contracts,omitempty"`
	Dependencies      []string `json:"dependencies,omitempty"`
	TimeoutSeconds    int      `json:"timeout_seconds,omitempty"`
	SourceDir         string   `json:"source_dir,omitempty"`
}

type ManagedAgentToolBundleMaterialization struct {
	Installed []string `json:"installed,omitempty"`
	Skipped   []string `json:"skipped,omitempty"`
	SourceDir string   `json:"source_dir,omitempty"`
}

type ToolBundleDiscoveryReport struct {
	Workdir           string                          `json:"workdir,omitempty"`
	Roots             []ToolBundleDiscoveryRoot       `json:"roots,omitempty"`
	Config            ToolBundleRegistryConfigStatus  `json:"config,omitempty"`
	CandidateCount    int                             `json:"candidate_count"`
	Candidates        []ToolBundleReadinessItem       `json:"candidates,omitempty"`
	Installed         []ToolBundleDiscoveryDiagnostic `json:"installed,omitempty"`
	Dependencies      []ToolBundleDiscoveryDiagnostic `json:"dependencies,omitempty"`
	Healthchecks      []ToolBundleDiscoveryDiagnostic `json:"healthchecks,omitempty"`
	ConfigDiagnostics []ToolBundleDiscoveryDiagnostic `json:"config_diagnostics,omitempty"`
	Skipped           []ToolBundleDiscoveryDiagnostic `json:"skipped,omitempty"`
	Errors            []ToolBundleDiscoveryDiagnostic `json:"errors,omitempty"`
	Collisions        []ToolBundleDiscoveryDiagnostic `json:"collisions,omitempty"`
}

type ToolBundleDiscoveryRoot struct {
	Kind string `json:"kind,omitempty"`
	Path string `json:"path,omitempty"`
}

type ToolBundleDiscoveryDiagnostic struct {
	Code             string   `json:"code,omitempty"`
	Name             string   `json:"name,omitempty"`
	RootKind         string   `json:"root_kind,omitempty"`
	Root             string   `json:"root,omitempty"`
	Dir              string   `json:"dir,omitempty"`
	ExistingDir      string   `json:"existing_dir,omitempty"`
	CapabilitySuites []string `json:"capability_suites,omitempty"`
	Message          string   `json:"message,omitempty"`
}

type ToolBundleReadinessItem struct {
	Name             string   `json:"name"`
	Status           string   `json:"status"`
	Code             string   `json:"code,omitempty"`
	RootKind         string   `json:"root_kind,omitempty"`
	Root             string   `json:"root,omitempty"`
	Dir              string   `json:"dir,omitempty"`
	ExistingDir      string   `json:"existing_dir,omitempty"`
	CapabilitySuites []string `json:"capability_suites,omitempty"`
	Message          string   `json:"message,omitempty"`
	Registered       bool     `json:"registered"`
	Configured       bool     `json:"configured,omitempty"`
}

type ToolBundleSuiteReadinessItem struct {
	Suite            string   `json:"suite"`
	Status           string   `json:"status"`
	Required         bool     `json:"required"`
	Heartbeats       []string `json:"heartbeats,omitempty"`
	ReadyBundles     []string `json:"ready_bundles,omitempty"`
	WarningBundles   []string `json:"warning_bundles,omitempty"`
	BlockedBundles   []string `json:"blocked_bundles,omitempty"`
	CandidateBundles []string `json:"candidate_bundles,omitempty"`
	NativeProviders  []string `json:"native_providers,omitempty"`
	SuggestedBundles []string `json:"suggested_bundles,omitempty"`
	SuggestedActions []string `json:"suggested_actions,omitempty"`
	Message          string   `json:"message,omitempty"`
}

type ToolBundleRegistryConfigStatus struct {
	Path     string   `json:"path,omitempty"`
	Present  bool     `json:"present"`
	Status   string   `json:"status,omitempty"`
	Mode     string   `json:"mode,omitempty"`
	Enabled  []string `json:"enabled,omitempty"`
	Disabled []string `json:"disabled,omitempty"`
	Message  string   `json:"message,omitempty"`
}

type installedToolBundleRoot struct {
	kind string
	path string
}

func normalizeManagedToolBundleList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			name := sanitizeManagedToolBundleName(part)
			if name == "" {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func parseManagedToolBundlesCSV(raw string) []string {
	return normalizeManagedToolBundleList([]string{raw})
}

func sanitizeManagedToolBundleName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, "\\", "/")
	if strings.Contains(value, "/") || strings.Contains(value, "..") {
		return ""
	}
	name := sanitizeWorkspaceFunctionName(value)
	if name == "" || !containsAlphaNumeric(name) {
		return ""
	}
	return name
}

func inferManagedToolBundlesFromAnatomy(anatomy AgentAnatomyConfig) []string {
	bundles := []string{}
	for _, heartbeat := range anatomy.Heartbeats {
		if !heartbeatEnabled(heartbeat) {
			continue
		}
		for _, suite := range heartbeat.ToolSuites {
			switch strings.ToLower(strings.TrimSpace(suite)) {
			case "browser_unrestricted", "browser_interactive":
				bundles = append(bundles, "browser_session", "browser_visual_probe")
			case "browser_read_only", "screenshot_capture", "console_read":
				bundles = append(bundles, "browser_visual_probe")
			}
		}
	}
	return normalizeManagedToolBundleList(bundles)
}

func managedAgentToolBundlesForRecord(record ManagedAgentRecord, defaults BotManagerDefaults, anatomy AgentAnatomyConfig) []string {
	values := []string{}
	values = append(values, defaults.ToolBundles...)
	values = append(values, record.ToolBundles...)
	values = append(values, inferManagedToolBundlesFromAnatomy(anatomy)...)
	return normalizeManagedToolBundleList(values)
}

func MaterializeManagedAgentToolBundles(record ManagedAgentRecord, anatomy AgentAnatomyConfig) (ManagedAgentToolBundleMaterialization, error) {
	record = normalizeManagedAgentRecord(record)
	defaults := LoadBotRegistry().Defaults
	bundles := managedAgentToolBundlesForRecord(record, defaults, anatomy)
	if len(bundles) == 0 {
		return ManagedAgentToolBundleMaterialization{}, nil
	}
	if strings.TrimSpace(record.Workdir) == "" {
		return ManagedAgentToolBundleMaterialization{}, fmt.Errorf("managed agent workdir is required")
	}
	destRoot := filepath.Join(record.Workdir, ".runtime-config", "tool-bundles")
	sourceRoot := operatorToolLibraryRootFunc()
	result := ManagedAgentToolBundleMaterialization{SourceDir: sourceRoot}
	if sourceRoot == "" {
		result.Skipped = append(result.Skipped, bundles...)
		if err := saveManagedToolBundleRegistryConfig(record.Workdir, bundles); err != nil {
			return result, fmt.Errorf("write managed tool bundle registry config: %w", err)
		}
		return result, nil
	}
	for _, name := range bundles {
		sourceDir := filepath.Join(sourceRoot, name)
		if !pathIsInside(sourceRoot, sourceDir) {
			return result, fmt.Errorf("tool bundle %q escapes operator library", name)
		}
		if !pathExists(filepath.Join(sourceDir, installedToolBundleManifestName)) {
			result.Skipped = append(result.Skipped, name)
			continue
		}
		destDir := filepath.Join(destRoot, name)
		if err := copyManagedToolBundleDir(record.Workdir, sourceDir, destDir); err != nil {
			return result, fmt.Errorf("materialize tool bundle %s: %w", name, err)
		}
		result.Installed = append(result.Installed, name)
	}
	if err := saveManagedToolBundleRegistryConfig(record.Workdir, bundles); err != nil {
		return result, fmt.Errorf("write managed tool bundle registry config: %w", err)
	}
	return result, nil
}

func operatorToolLibraryRoot() string {
	for _, candidate := range operatorToolLibraryRootCandidates() {
		if directoryExists(candidate) {
			if abs, err := filepath.Abs(candidate); err == nil {
				return abs
			}
			return candidate
		}
	}
	return ""
}

func operatorToolLibraryRootCandidates() []string {
	candidates := []string{}
	for _, key := range []string{"RHIZOME_OPERATOR_TOOL_LIBRARY_ROOT", "RHIZOME_TOOL_LIBRARY_ROOT"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			candidates = append(candidates, value)
		}
	}
	if executable, err := operatorToolExecutablePath(); err == nil {
		exeDir := filepath.Dir(executable)
		candidates = append(candidates,
			filepath.Join(exeDir, "tool_library"),
			filepath.Join(exeDir, "agent", "tool_library"),
			filepath.Clean(filepath.Join(exeDir, "..", "tool_library")),
		)
	}
	if _, file, _, ok := goruntime.Caller(0); ok {
		candidates = append(candidates, filepath.Join(filepath.Dir(file), "tool_library"))
	}
	return candidates
}

func directoryExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func copyManagedToolBundleDir(workdir, sourceDir, destDir string) error {
	if !pathIsInside(workdir, destDir) {
		return fmt.Errorf("destination escapes workdir")
	}
	if err := os.MkdirAll(filepath.Dir(destDir), 0o755); err != nil {
		return err
	}
	if err := os.RemoveAll(destDir); err != nil {
		return err
	}
	return filepath.WalkDir(sourceDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destDir, rel)
		if !pathIsInside(destDir, target) {
			return fmt.Errorf("copy target escapes bundle destination")
		}
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, raw, info.Mode().Perm())
	})
}

func RegisterInstalledToolBundles(registry *ToolRegistry, workdir string) ToolBundleDiscoveryReport {
	report := ToolBundleDiscoveryReport{Workdir: strings.TrimSpace(workdir)}
	if registry == nil || strings.TrimSpace(workdir) == "" {
		report = withToolBundleReadiness(report)
		return report
	}
	bundles, report := discoverInstalledToolBundlesWithReport(workdir)
	for _, bundle := range bundles {
		diagnostics := registry.RegisterWithDiagnostics(bundle)
		if len(diagnostics) > 0 {
			report.Installed = removeToolBundleDiscoveryDiagnostics(report.Installed, bundle.Name(), bundle.dir)
			report.Dependencies = removeToolBundleDiscoveryDiagnostics(report.Dependencies, bundle.Name(), bundle.dir)
			report.Healthchecks = removeToolBundleDiscoveryDiagnostics(report.Healthchecks, bundle.Name(), bundle.dir)
		}
		for _, diagnostic := range diagnostics {
			report.Collisions = append(report.Collisions, ToolBundleDiscoveryDiagnostic{
				Code:             diagnostic.Code,
				Name:             diagnostic.ToolName,
				Dir:              bundle.dir,
				CapabilitySuites: uniqueTrimmedCSVStrings(bundle.manifest.CapabilitySuites),
				Message:          diagnostic.Message,
				RootKind:         installedToolBundleRootKind(workdir, bundle.dir),
			})
		}
	}
	report = withToolBundleReadiness(report)
	return report
}

func discoverInstalledToolBundles(workdir string) []*InstalledToolBundleTool {
	bundles, _ := discoverInstalledToolBundlesWithReport(workdir)
	return bundles
}

func discoverInstalledToolBundlesWithReport(workdir string) ([]*InstalledToolBundleTool, ToolBundleDiscoveryReport) {
	roots := installedToolBundleDiscoveryRoots(workdir)
	report := ToolBundleDiscoveryReport{Workdir: strings.TrimSpace(workdir)}
	registryConfig, configStatus, configDiagnostics := loadInstalledToolBundleRegistryConfig(workdir)
	report.Config = configStatus
	report.ConfigDiagnostics = append(report.ConfigDiagnostics, configDiagnostics...)
	report.Errors = append(report.Errors, blockingToolBundleConfigDiagnostics(configDiagnostics)...)
	for _, root := range roots {
		report.Roots = append(report.Roots, ToolBundleDiscoveryRoot{Kind: root.kind, Path: root.path})
	}
	out := []*InstalledToolBundleTool{}
	seen := map[string]*InstalledToolBundleTool{}
	matchedConfigured := map[string]struct{}{}
	for _, root := range roots {
		entries, err := os.ReadDir(root.path)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			dir := filepath.Join(root.path, entry.Name())
			entryName := sanitizeWorkspaceFunctionName(entry.Name())
			if entryName != "" && toolBundleRegistryConfigNameConfigured(registryConfig, entryName) && pathExists(filepath.Join(dir, installedToolBundleManifestName)) {
				matchedConfigured[entryName] = struct{}{}
			}
			tool, err := loadInstalledToolBundle(workdir, dir)
			if err != nil {
				diagnostic := installedToolBundleLoadDiagnostic(err, root, dir, entry.Name())
				if diagnostic.Code == "missing_manifest" {
					report.Skipped = append(report.Skipped, diagnostic)
				} else {
					report.Errors = append(report.Errors, diagnostic)
				}
				continue
			}
			if tool == nil {
				report.Skipped = append(report.Skipped, ToolBundleDiscoveryDiagnostic{
					Code:     "nil_tool_bundle",
					Name:     entry.Name(),
					RootKind: root.kind,
					Root:     root.path,
					Dir:      dir,
					Message:  "tool bundle loader returned no bundle",
				})
				continue
			}
			name := strings.TrimSpace(tool.Name())
			for _, candidate := range []string{entryName, name} {
				if toolBundleRegistryConfigNameConfigured(registryConfig, candidate) {
					matchedConfigured[candidate] = struct{}{}
				}
			}
			if skipDiagnostic, skip := toolBundleRegistryConfigSkipForTool(registryConfig, root, dir, entryName, name); skip {
				skipDiagnostic.CapabilitySuites = uniqueTrimmedCSVStrings(tool.manifest.CapabilitySuites)
				report.Skipped = append(report.Skipped, skipDiagnostic)
				continue
			}
			if len(tool.manifest.Command) == 0 {
				report.Errors = append(report.Errors, ToolBundleDiscoveryDiagnostic{
					Code:             "missing_command",
					Name:             name,
					RootKind:         root.kind,
					Root:             root.path,
					Dir:              dir,
					CapabilitySuites: uniqueTrimmedCSVStrings(tool.manifest.CapabilitySuites),
					Message:          fmt.Sprintf("tool bundle %q has no command", name),
				})
				continue
			}
			if existing, ok := seen[name]; ok {
				report.Collisions = append(report.Collisions, ToolBundleDiscoveryDiagnostic{
					Code:             "duplicate_tool_bundle",
					Name:             name,
					RootKind:         root.kind,
					Root:             root.path,
					Dir:              dir,
					ExistingDir:      existing.dir,
					CapabilitySuites: uniqueTrimmedCSVStrings(tool.manifest.CapabilitySuites),
					Message:          fmt.Sprintf("tool bundle %q from %s skipped because %s was discovered first and reserves that bundle name even if it later reports dependency or health diagnostics", name, dir, existing.dir),
				})
				continue
			}
			seen[name] = tool
			dependencyDiagnostics := validateInstalledToolBundleDependencies(tool, root, dir)
			report.Dependencies = append(report.Dependencies, dependencyDiagnostics...)
			if installedToolBundleDependenciesHaveBlockingFailure(dependencyDiagnostics) {
				report.Errors = append(report.Errors, blockingInstalledToolBundleDependencyDiagnostics(dependencyDiagnostics)...)
				continue
			}
			if diagnostic, ok := validateInstalledToolBundleHealthcheck(workdir, tool, root, dir); ok {
				if diagnostic.Code == "healthcheck_passed" {
					report.Healthchecks = append(report.Healthchecks, diagnostic)
				} else {
					report.Errors = append(report.Errors, diagnostic)
					continue
				}
			}
			out = append(out, tool)
			report.Installed = append(report.Installed, ToolBundleDiscoveryDiagnostic{
				Code:             "installed",
				Name:             name,
				RootKind:         root.kind,
				Root:             root.path,
				Dir:              dir,
				CapabilitySuites: uniqueTrimmedCSVStrings(tool.manifest.CapabilitySuites),
				Message:          fmt.Sprintf("tool bundle %q discovered from %s", name, dir),
			})
		}
	}
	for _, missingName := range missingConfiguredToolBundles(registryConfig, matchedConfigured) {
		diagnostic := ToolBundleDiscoveryDiagnostic{
			Code:    "configured_tool_bundle_missing",
			Name:    missingName,
			Message: fmt.Sprintf("tool bundle %q is enabled in %s but no matching local manifest was discovered", missingName, installedToolBundleConfigName),
		}
		report.ConfigDiagnostics = append(report.ConfigDiagnostics, diagnostic)
		report.Errors = append(report.Errors, diagnostic)
	}
	report = withToolBundleReadiness(report)
	return out, report
}

func removeToolBundleDiscoveryDiagnostics(diagnostics []ToolBundleDiscoveryDiagnostic, name, dir string) []ToolBundleDiscoveryDiagnostic {
	if len(diagnostics) == 0 {
		return diagnostics
	}
	name = strings.TrimSpace(name)
	dir = strings.TrimSpace(dir)
	out := diagnostics[:0]
	for _, diagnostic := range diagnostics {
		if toolBundleDiscoveryDiagnosticMatches(diagnostic, name, dir) {
			continue
		}
		out = append(out, diagnostic)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func toolBundleDiscoveryDiagnosticMatches(diagnostic ToolBundleDiscoveryDiagnostic, name, dir string) bool {
	if name != "" && strings.TrimSpace(diagnostic.Name) != name {
		return false
	}
	if dir != "" && strings.TrimSpace(diagnostic.Dir) != dir {
		return false
	}
	return true
}

func loadInstalledToolBundleRegistryConfig(workdir string) (InstalledToolBundleRegistryConfig, ToolBundleRegistryConfigStatus, []ToolBundleDiscoveryDiagnostic) {
	path := installedToolBundleRegistryConfigPath(workdir)
	status := ToolBundleRegistryConfigStatus{
		Path:   path,
		Status: "absent",
		Mode:   "auto",
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return InstalledToolBundleRegistryConfig{Mode: "auto"}, status, nil
		}
		status.Present = true
		status.Status = "error_auto_fallback"
		status.Message = err.Error()
		diagnostic := ToolBundleDiscoveryDiagnostic{
			Code:    "tool_bundle_config_error",
			Message: fmt.Sprintf("read %s: %v", installedToolBundleConfigName, err),
		}
		return InstalledToolBundleRegistryConfig{Mode: "auto"}, status, []ToolBundleDiscoveryDiagnostic{diagnostic}
	}
	status.Present = true
	var config InstalledToolBundleRegistryConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		status.Status = "malformed_auto_fallback"
		status.Message = err.Error()
		diagnostic := ToolBundleDiscoveryDiagnostic{
			Code:    "malformed_tool_bundle_config",
			Message: fmt.Sprintf("parse %s: %v", installedToolBundleConfigName, err),
		}
		return InstalledToolBundleRegistryConfig{Mode: "auto"}, status, []ToolBundleDiscoveryDiagnostic{diagnostic}
	}
	config.Mode = normalizeInstalledToolBundleRegistryMode(config.Mode, config.Enabled)
	config.Enabled = normalizeManagedToolBundleList(config.Enabled)
	config.Disabled = normalizeManagedToolBundleList(config.Disabled)
	status.Status = "loaded"
	status.Mode = config.Mode
	status.Enabled = append([]string(nil), config.Enabled...)
	status.Disabled = append([]string(nil), config.Disabled...)
	diagnostics := installedToolBundleRegistryConfigConflictDiagnostics(config)
	return config, status, diagnostics
}

func saveManagedToolBundleRegistryConfig(workdir string, enabled []string) error {
	enabled = normalizeManagedToolBundleList(enabled)
	if len(enabled) == 0 {
		return nil
	}
	existing, status, diagnostics := loadInstalledToolBundleRegistryConfig(workdir)
	disabled := []string{}
	if status.Status == "loaded" && len(blockingToolBundleConfigDiagnostics(diagnostics)) == 0 {
		enabled = normalizeManagedToolBundleList(append(enabled, existing.Enabled...))
		disabled = append(disabled, existing.Disabled...)
	}
	return saveInstalledToolBundleRegistryConfig(workdir, InstalledToolBundleRegistryConfig{
		SchemaVersion: installedToolBundleConfigSchema,
		Mode:          "explicit",
		Enabled:       enabled,
		Disabled:      disabled,
	})
}

func saveInstalledToolBundleRegistryConfig(workdir string, config InstalledToolBundleRegistryConfig) error {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return fmt.Errorf("managed agent workdir is required")
	}
	config.SchemaVersion = firstNonEmpty(config.SchemaVersion, installedToolBundleConfigSchema)
	config.Enabled = normalizeManagedToolBundleList(config.Enabled)
	config.Disabled = normalizeManagedToolBundleList(config.Disabled)
	config.Mode = normalizeInstalledToolBundleRegistryMode(config.Mode, config.Enabled)
	path := installedToolBundleRegistryConfigPath(workdir)
	if !pathIsInside(workdir, path) {
		return fmt.Errorf("tool bundle registry config path escapes workdir")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func installedToolBundleRegistryConfigPath(workdir string) string {
	return filepath.Join(workdir, ".runtime-config", installedToolBundleConfigName)
}

func normalizeInstalledToolBundleRegistryMode(mode string, enabled []string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "explicit", "allowlist", "enabled_only":
		return "explicit"
	case "auto", "":
		if len(normalizeManagedToolBundleList(enabled)) > 0 {
			return "explicit"
		}
		return "auto"
	default:
		if len(normalizeManagedToolBundleList(enabled)) > 0 {
			return "explicit"
		}
		return "auto"
	}
}

func installedToolBundleRegistryConfigConflictDiagnostics(config InstalledToolBundleRegistryConfig) []ToolBundleDiscoveryDiagnostic {
	if len(config.Enabled) == 0 || len(config.Disabled) == 0 {
		return nil
	}
	disabled := map[string]struct{}{}
	for _, name := range config.Disabled {
		disabled[name] = struct{}{}
	}
	out := []ToolBundleDiscoveryDiagnostic{}
	for _, name := range config.Enabled {
		if _, ok := disabled[name]; ok {
			out = append(out, ToolBundleDiscoveryDiagnostic{
				Code:    "tool_bundle_config_conflict",
				Name:    name,
				Message: fmt.Sprintf("tool bundle %q is both enabled and disabled in %s; disabled wins", name, installedToolBundleConfigName),
			})
		}
	}
	return out
}

func blockingToolBundleConfigDiagnostics(diagnostics []ToolBundleDiscoveryDiagnostic) []ToolBundleDiscoveryDiagnostic {
	out := []ToolBundleDiscoveryDiagnostic{}
	for _, diagnostic := range diagnostics {
		switch strings.TrimSpace(diagnostic.Code) {
		case "tool_bundle_config_error", "malformed_tool_bundle_config":
			out = append(out, diagnostic)
		}
	}
	return out
}

func toolBundleRegistryConfigNameConfigured(config InstalledToolBundleRegistryConfig, name string) bool {
	name = sanitizeWorkspaceFunctionName(name)
	if name == "" {
		return false
	}
	return containsTrimmedString(config.Enabled, name) || containsTrimmedString(config.Disabled, name)
}

func toolBundleRegistryConfigSkipsName(config InstalledToolBundleRegistryConfig, name string) bool {
	name = sanitizeWorkspaceFunctionName(name)
	if name == "" {
		return false
	}
	if containsTrimmedString(config.Disabled, name) {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(config.Mode), "explicit") && len(config.Enabled) > 0 && !containsTrimmedString(config.Enabled, name)
}

func toolBundleRegistryConfigSkipForTool(config InstalledToolBundleRegistryConfig, root installedToolBundleRoot, dir, entryName, toolName string) (ToolBundleDiscoveryDiagnostic, bool) {
	names := uniqueNonEmptyStrings([]string{sanitizeWorkspaceFunctionName(entryName), sanitizeWorkspaceFunctionName(toolName)})
	for _, name := range names {
		if containsTrimmedString(config.Disabled, name) {
			return toolBundleRegistryConfigSkipDiagnostic(config, root, dir, name), true
		}
	}
	if strings.EqualFold(strings.TrimSpace(config.Mode), "explicit") && len(config.Enabled) > 0 {
		for _, name := range names {
			if containsTrimmedString(config.Enabled, name) {
				return ToolBundleDiscoveryDiagnostic{}, false
			}
		}
		name := firstNonEmpty(toolName, entryName)
		return toolBundleRegistryConfigSkipDiagnostic(config, root, dir, sanitizeWorkspaceFunctionName(name)), true
	}
	return ToolBundleDiscoveryDiagnostic{}, false
}

func toolBundleRegistryConfigSkipDiagnostic(config InstalledToolBundleRegistryConfig, root installedToolBundleRoot, dir, name string) ToolBundleDiscoveryDiagnostic {
	name = sanitizeWorkspaceFunctionName(name)
	code := "not_enabled_by_config"
	message := fmt.Sprintf("tool bundle %q skipped because %s is in explicit mode and does not enable it", name, installedToolBundleConfigName)
	if containsTrimmedString(config.Disabled, name) {
		code = "disabled_by_config"
		message = fmt.Sprintf("tool bundle %q skipped because it is disabled in %s", name, installedToolBundleConfigName)
	}
	return ToolBundleDiscoveryDiagnostic{
		Code:     code,
		Name:     name,
		RootKind: root.kind,
		Root:     root.path,
		Dir:      dir,
		Message:  message,
	}
}

func missingConfiguredToolBundles(config InstalledToolBundleRegistryConfig, matched map[string]struct{}) []string {
	if len(config.Enabled) == 0 {
		return nil
	}
	disabled := map[string]struct{}{}
	for _, name := range config.Disabled {
		disabled[name] = struct{}{}
	}
	out := []string{}
	for _, name := range config.Enabled {
		if _, skip := disabled[name]; skip {
			continue
		}
		if _, ok := matched[name]; !ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func validateInstalledToolBundleDependencies(tool *InstalledToolBundleTool, root installedToolBundleRoot, dir string) []ToolBundleDiscoveryDiagnostic {
	if tool == nil || len(tool.manifest.Dependencies) == 0 {
		return nil
	}
	name := strings.TrimSpace(tool.Name())
	diagnostics := make([]ToolBundleDiscoveryDiagnostic, 0, len(tool.manifest.Dependencies))
	for _, dependency := range tool.manifest.Dependencies {
		dependencyName := strings.TrimSpace(dependency.Name)
		if dependencyName == "" {
			continue
		}
		kind := strings.ToLower(strings.TrimSpace(dependency.Kind))
		if kind == "" {
			kind = "executable"
		}
		diagnostic := ToolBundleDiscoveryDiagnostic{
			Code:             "dependency_available",
			Name:             name,
			RootKind:         root.kind,
			Root:             root.path,
			Dir:              dir,
			CapabilitySuites: uniqueTrimmedCSVStrings(tool.manifest.CapabilitySuites),
			Message:          fmt.Sprintf("tool bundle %q dependency %s is declared", name, installedToolBundleDependencyLabel(dependency)),
		}
		switch kind {
		case "executable", "binary", "command":
			if _, err := exec.LookPath(dependencyName); err != nil {
				if dependency.Required {
					diagnostic.Code = "missing_dependency"
				} else {
					diagnostic.Code = "optional_dependency_missing"
				}
				diagnostic.Message = fmt.Sprintf("tool bundle %q dependency %s is not available on PATH", name, installedToolBundleDependencyLabel(dependency))
			} else {
				diagnostic.Code = "dependency_available"
				diagnostic.Message = fmt.Sprintf("tool bundle %q dependency %s is available", name, installedToolBundleDependencyLabel(dependency))
			}
		case "browser":
			if installedToolBundleBrowserDependencyAvailable(dependencyName) {
				diagnostic.Code = "dependency_available"
				diagnostic.Message = fmt.Sprintf("tool bundle %q dependency %s is available", name, installedToolBundleDependencyLabel(dependency))
			} else if dependency.Required {
				diagnostic.Code = "missing_dependency"
				diagnostic.Message = fmt.Sprintf("tool bundle %q dependency %s is not available", name, installedToolBundleDependencyLabel(dependency))
			} else {
				diagnostic.Code = "optional_dependency_missing"
				diagnostic.Message = fmt.Sprintf("tool bundle %q dependency %s is not available", name, installedToolBundleDependencyLabel(dependency))
			}
		default:
			if dependency.Required {
				diagnostic.Code = "dependency_unchecked"
			} else {
				diagnostic.Code = "optional_dependency_unchecked"
			}
			diagnostic.Message = fmt.Sprintf("tool bundle %q dependency %s has unsupported kind %q and was not checked", name, installedToolBundleDependencyLabel(dependency), kind)
		}
		diagnostics = append(diagnostics, diagnostic)
	}
	return diagnostics
}

func installedToolBundleBrowserDependencyAvailable(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	candidates := []string{name}
	useKnownBrowserCandidates := false
	if name == "" || name == "browser" || name == "chrome_or_edge" || name == "chrome_or_chromium" {
		useKnownBrowserCandidates = true
		if goruntime.GOOS == "windows" {
			candidates = append(candidates, "chrome.exe", "msedge.exe", "chromium.exe")
		} else {
			candidates = append(candidates, "chrome", "chromium-browser", "chromium", "google-chrome")
		}
	}
	if useKnownBrowserCandidates {
		for _, candidate := range knownBrowserExecutableCandidates(goruntime.GOOS) {
			if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
				return true
			}
		}
	}
	for _, candidate := range uniqueNonEmptyStrings(candidates) {
		if _, err := exec.LookPath(candidate); err == nil {
			return true
		}
	}
	return false
}

func installedToolBundleDependenciesHaveBlockingFailure(diagnostics []ToolBundleDiscoveryDiagnostic) bool {
	for _, diagnostic := range diagnostics {
		switch strings.TrimSpace(diagnostic.Code) {
		case "missing_dependency", "dependency_unchecked":
			return true
		}
	}
	return false
}

func blockingInstalledToolBundleDependencyDiagnostics(diagnostics []ToolBundleDiscoveryDiagnostic) []ToolBundleDiscoveryDiagnostic {
	out := []ToolBundleDiscoveryDiagnostic{}
	for _, diagnostic := range diagnostics {
		switch strings.TrimSpace(diagnostic.Code) {
		case "missing_dependency", "dependency_unchecked":
			out = append(out, diagnostic)
		}
	}
	return out
}

func installedToolBundleDependencyLabel(dependency InstalledToolBundleDependency) string {
	name := strings.TrimSpace(dependency.Name)
	if name == "" {
		name = "unnamed"
	}
	kind := strings.TrimSpace(dependency.Kind)
	if kind == "" {
		kind = "executable"
	}
	parts := []string{name, kind}
	if version := strings.TrimSpace(dependency.Version); version != "" {
		parts = append(parts, version)
	}
	if dependency.Required {
		parts = append(parts, "required")
	} else {
		parts = append(parts, "optional")
	}
	return strings.Join(parts, ":")
}

func installedToolBundleDiscoveryRoots(workdir string) []installedToolBundleRoot {
	return []installedToolBundleRoot{
		{kind: "managed", path: filepath.Join(workdir, ".runtime-config", "tool-bundles")},
		{kind: "tools", path: filepath.Join(workdir, "tools")},
	}
}

func installedToolBundleLoadDiagnostic(err error, root installedToolBundleRoot, dir, fallbackName string) ToolBundleDiscoveryDiagnostic {
	code := "load_error"
	if os.IsNotExist(err) {
		code = "missing_manifest"
	} else if _, ok := err.(*json.SyntaxError); ok {
		code = "malformed_manifest"
	} else if _, ok := err.(*json.UnmarshalTypeError); ok {
		code = "malformed_manifest"
	} else if strings.Contains(err.Error(), "invalid character") {
		code = "malformed_manifest"
	} else if strings.Contains(err.Error(), "invalid name") {
		code = "invalid_name"
	}
	return ToolBundleDiscoveryDiagnostic{
		Code:     code,
		Name:     fallbackName,
		RootKind: root.kind,
		Root:     root.path,
		Dir:      dir,
		Message:  fmt.Sprintf("tool bundle %s: %v", code, err),
	}
}

func validateInstalledToolBundleHealthcheck(workdir string, tool *InstalledToolBundleTool, root installedToolBundleRoot, dir string) (ToolBundleDiscoveryDiagnostic, bool) {
	if tool == nil || tool.manifest.Healthcheck == nil || len(tool.manifest.Healthcheck.Command) == 0 {
		return ToolBundleDiscoveryDiagnostic{}, false
	}
	name := strings.TrimSpace(tool.Name())
	timeout := installedToolBundleHealthcheckTimeout(tool.manifest.Healthcheck.TimeoutSeconds)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	output, err := runInstalledToolBundleHealthcheckCommand(ctx, tool.manifest.Healthcheck.Command, dir, workdir)
	diagnostic := ToolBundleDiscoveryDiagnostic{
		Code:             "healthcheck_passed",
		Name:             name,
		RootKind:         root.kind,
		Root:             root.path,
		Dir:              dir,
		CapabilitySuites: uniqueTrimmedCSVStrings(tool.manifest.CapabilitySuites),
		Message:          fmt.Sprintf("tool bundle %q healthcheck passed", name),
	}
	if err == nil && ctx.Err() == nil {
		return diagnostic, true
	}
	code := "healthcheck_failed"
	if ctx.Err() != nil {
		code = "healthcheck_timeout"
		err = ctx.Err()
	}
	diagnostic.Code = code
	diagnostic.Message = fmt.Sprintf("tool bundle %q %s: %v", name, code, err)
	if trimmed := strings.TrimSpace(string(output)); trimmed != "" {
		diagnostic.Message += ": " + truncate(trimmed, 512)
	}
	return diagnostic, true
}

func runInstalledToolBundleHealthcheckCommand(ctx context.Context, command []string, bundleDir, workdir string) ([]byte, error) {
	if len(command) == 0 {
		return nil, fmt.Errorf("missing healthcheck command")
	}
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Dir = bundleDir
	cmd.Env = appendKnownBrowserDirsToEnvPath(os.Environ())
	cmd.Env = append(cmd.Env,
		"RHIZOME_TOOL_WORKDIR="+workdir,
		"RHIZOME_TOOL_BUNDLE_DIR="+bundleDir,
	)
	out, err := runShellToolCommand(ctx, cmd)
	if ctx.Err() != nil {
		return out, ctx.Err()
	}
	return out, err
}

func installedToolBundleHealthcheckTimeout(seconds int) time.Duration {
	if seconds <= 0 {
		return 2 * time.Second
	}
	value := time.Duration(seconds) * time.Second
	if value < time.Second {
		return time.Second
	}
	if value > 20*time.Second {
		return 20 * time.Second
	}
	return value
}

func installedToolBundleRootKind(workdir, dir string) string {
	for _, root := range installedToolBundleDiscoveryRoots(workdir) {
		if pathIsInside(root.path, dir) {
			return root.kind
		}
	}
	return ""
}

func loadInstalledToolBundle(workdir, dir string) (*InstalledToolBundleTool, error) {
	if !pathIsInside(workdir, dir) {
		return nil, fmt.Errorf("tool bundle escapes workdir: %s", dir)
	}
	manifest, err := loadInstalledToolBundleManifest(dir)
	if err != nil {
		return nil, err
	}
	return &InstalledToolBundleTool{
		workdir:  workdir,
		dir:      dir,
		manifest: manifest,
	}, nil
}

func loadInstalledToolBundleManifest(dir string) (InstalledToolBundleManifest, error) {
	raw, err := os.ReadFile(filepath.Join(dir, installedToolBundleManifestName))
	if err != nil {
		return InstalledToolBundleManifest{}, err
	}
	var manifest InstalledToolBundleManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return InstalledToolBundleManifest{}, err
	}
	return normalizeInstalledToolBundleManifest(manifest)
}

func normalizeInstalledToolBundleManifest(manifest InstalledToolBundleManifest) (InstalledToolBundleManifest, error) {
	manifest.Name = sanitizeWorkspaceFunctionName(manifest.Name)
	if manifest.Name == "" || !containsAlphaNumeric(manifest.Name) {
		return InstalledToolBundleManifest{}, fmt.Errorf("tool bundle has invalid name")
	}
	if strings.TrimSpace(manifest.Description) == "" {
		manifest.Description = "Installed local tool bundle " + manifest.Name
	}
	if len(manifest.Parameters) == 0 {
		manifest.Parameters = map[string]any{"type": "object"}
	}
	manifest.CapabilitySuites = uniqueTrimmedCSVStrings(manifest.CapabilitySuites)
	return manifest, nil
}

func (t *InstalledToolBundleTool) Name() string {
	return strings.TrimSpace(t.manifest.Name)
}

func (t *InstalledToolBundleTool) Description() string {
	return strings.TrimSpace(t.manifest.Description)
}

func (t *InstalledToolBundleTool) Parameters() map[string]any {
	if len(t.manifest.Parameters) == 0 {
		return map[string]any{"type": "object"}
	}
	return t.manifest.Parameters
}

func installedToolBundlePromptInventory(registry *ToolRegistry) []InstalledToolBundlePromptItem {
	if registry == nil {
		return nil
	}
	items := []InstalledToolBundlePromptItem{}
	for _, tool := range registry.tools {
		bundle, ok := tool.(*InstalledToolBundleTool)
		if !ok || bundle == nil {
			continue
		}
		items = append(items, InstalledToolBundlePromptItem{
			Name:              strings.TrimSpace(bundle.Name()),
			Description:       strings.TrimSpace(bundle.Description()),
			Version:           strings.TrimSpace(bundle.manifest.Version),
			CapabilitySuites:  uniqueTrimmedCSVStrings(bundle.manifest.CapabilitySuites),
			ArtifactContracts: installedToolBundlePromptArtifactContracts(bundle.manifest.ArtifactContracts),
			Dependencies:      installedToolBundlePromptDependencies(bundle.manifest.Dependencies),
			TimeoutSeconds:    int(installedToolBundleTimeout(bundle.manifest.TimeoutSeconds) / time.Second),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Name < items[j].Name
	})
	return items
}

func installedToolBundlePromptArtifactContracts(contracts []InstalledToolBundleArtifactContract) []string {
	out := make([]string, 0, len(contracts))
	for _, contract := range contracts {
		name := strings.TrimSpace(contract.Name)
		if name == "" {
			name = strings.TrimSpace(contract.Path)
		}
		if name == "" {
			name = strings.TrimSpace(contract.Type)
		}
		if name == "" {
			continue
		}
		if contract.Required {
			name += ":required"
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return uniqueTrimmedCSVStrings(out)
}

func installedToolBundlePromptDependencies(dependencies []InstalledToolBundleDependency) []string {
	out := make([]string, 0, len(dependencies))
	for _, dependency := range dependencies {
		name := strings.TrimSpace(dependency.Name)
		if name == "" {
			continue
		}
		parts := []string{name}
		if kind := strings.TrimSpace(dependency.Kind); kind != "" {
			parts = append(parts, kind)
		}
		if version := strings.TrimSpace(dependency.Version); version != "" {
			parts = append(parts, version)
		}
		if dependency.Required {
			parts = append(parts, "required")
		}
		out = append(out, strings.Join(parts, ":"))
	}
	sort.Strings(out)
	return uniqueTrimmedCSVStrings(out)
}

func hasInstalledToolBundles(registry *ToolRegistry) bool {
	return len(installedToolBundlePromptInventory(registry)) > 0
}

func toolBundleStatusSnapshot(agent *Agent) ToolBundleStatusSnapshot {
	snapshot := ToolBundleStatusSnapshot{
		Schema:         "tool_bundle_status/v1",
		Status:         "unavailable",
		CopyInContract: ".runtime-config/tool-bundles/<bundle>/tool.json or tools/<bundle>/tool.json",
	}
	if agent == nil {
		return snapshot
	}
	report := agent.toolBundleDiscovery
	snapshot.Workdir = strings.TrimSpace(firstNonEmpty(report.Workdir, agent.Workdir))
	snapshot.Roots = append([]ToolBundleDiscoveryRoot(nil), report.Roots...)
	snapshot.Config = report.Config
	snapshot.Candidates = append([]ToolBundleReadinessItem(nil), report.Candidates...)
	snapshot.Suites = toolBundleSuiteReadinessForAgent(agent)
	snapshot.Installed = installedToolBundleStatusInventory(agent.registry)
	snapshot.Dependencies = append([]ToolBundleDiscoveryDiagnostic(nil), report.Dependencies...)
	snapshot.Healthchecks = append([]ToolBundleDiscoveryDiagnostic(nil), report.Healthchecks...)
	snapshot.ConfigDiagnostics = append([]ToolBundleDiscoveryDiagnostic(nil), report.ConfigDiagnostics...)
	snapshot.Skipped = append([]ToolBundleDiscoveryDiagnostic(nil), report.Skipped...)
	snapshot.Errors = append([]ToolBundleDiscoveryDiagnostic(nil), report.Errors...)
	snapshot.Collisions = append([]ToolBundleDiscoveryDiagnostic(nil), report.Collisions...)
	if len(snapshot.Candidates) == 0 {
		snapshot.Candidates = toolBundleReadinessItems(report)
	}
	snapshot.CandidateCount = len(snapshot.Candidates)
	snapshot.SuiteCount = len(snapshot.Suites)
	snapshot.InstalledCount = len(snapshot.Installed)
	snapshot.DependencyCount = len(snapshot.Dependencies)
	snapshot.HealthcheckCount = len(snapshot.Healthchecks)
	snapshot.ConfigCount = len(snapshot.ConfigDiagnostics)
	snapshot.SkippedCount = len(snapshot.Skipped)
	snapshot.ErrorCount = len(snapshot.Errors)
	snapshot.CollisionCount = len(snapshot.Collisions)
	snapshot.ToolVisible = snapshot.InstalledCount > 0
	snapshot.PromptVisible = snapshot.ToolVisible || snapshot.SuiteCount > 0 || snapshot.Config.Present || snapshot.DependencyCount > 0 || snapshot.ConfigCount > 0 || snapshot.SkippedCount > 0 || snapshot.ErrorCount > 0 || snapshot.CollisionCount > 0
	switch {
	case snapshot.ErrorCount > 0 || snapshot.CollisionCount > 0:
		snapshot.Status = "degraded"
	case len(snapshot.ConfigDiagnostics) > 0:
		snapshot.Status = "degraded"
	case toolBundleSuitesHaveBlockingRequired(snapshot.Suites):
		snapshot.Status = "degraded"
	case toolBundleDependencyDiagnosticsHaveWarnings(snapshot.Dependencies):
		snapshot.Status = "degraded"
	case toolBundleSuitesHaveRequiredWarnings(snapshot.Suites):
		snapshot.Status = "degraded"
	case snapshot.InstalledCount > 0:
		snapshot.Status = "enabled"
	case snapshot.SuiteCount > 0:
		snapshot.Status = "enabled"
	case snapshot.SkippedCount > 0 || len(report.Roots) > 0:
		snapshot.Status = "empty"
	default:
		snapshot.Status = "unavailable"
	}
	return snapshot
}

func toolBundleDependencyDiagnosticsHaveWarnings(diagnostics []ToolBundleDiscoveryDiagnostic) bool {
	for _, diagnostic := range diagnostics {
		switch strings.TrimSpace(diagnostic.Code) {
		case "missing_dependency", "optional_dependency_missing", "dependency_unchecked", "optional_dependency_unchecked":
			return true
		}
	}
	return false
}

func toolBundleSuitesHaveBlockingRequired(items []ToolBundleSuiteReadinessItem) bool {
	for _, item := range items {
		if !item.Required {
			continue
		}
		switch strings.TrimSpace(item.Status) {
		case "missing", "blocked", "config_conflict", "collided":
			return true
		}
	}
	return false
}

func toolBundleSuitesHaveRequiredWarnings(items []ToolBundleSuiteReadinessItem) bool {
	for _, item := range items {
		if item.Required && strings.TrimSpace(item.Status) == "ready_with_warnings" {
			return true
		}
	}
	return false
}

func installedToolBundleStatusInventory(registry *ToolRegistry) []ToolBundleStatusItem {
	if registry == nil {
		return nil
	}
	items := []ToolBundleStatusItem{}
	names := make([]string, 0, len(registry.tools))
	byName := map[string]*InstalledToolBundleTool{}
	for name, tool := range registry.tools {
		bundle, ok := tool.(*InstalledToolBundleTool)
		if !ok || bundle == nil {
			continue
		}
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			trimmed = strings.TrimSpace(bundle.Name())
		}
		if trimmed == "" {
			continue
		}
		names = append(names, trimmed)
		byName[trimmed] = bundle
	}
	sort.Strings(names)
	for _, name := range names {
		bundle := byName[name]
		items = append(items, ToolBundleStatusItem{
			Name:              strings.TrimSpace(bundle.Name()),
			Description:       strings.TrimSpace(bundle.Description()),
			Version:           strings.TrimSpace(bundle.manifest.Version),
			CapabilitySuites:  uniqueTrimmedCSVStrings(bundle.manifest.CapabilitySuites),
			ArtifactContracts: installedToolBundlePromptArtifactContracts(bundle.manifest.ArtifactContracts),
			Dependencies:      installedToolBundlePromptDependencies(bundle.manifest.Dependencies),
			TimeoutSeconds:    int(installedToolBundleTimeout(bundle.manifest.TimeoutSeconds) / time.Second),
			SourceDir:         strings.TrimSpace(bundle.dir),
		})
	}
	return items
}

func buildInstalledToolBundlePromptSection(registry *ToolRegistry) string {
	return buildInstalledToolBundlePromptSectionWithReport(registry, ToolBundleDiscoveryReport{})
}

func buildInstalledToolBundlePromptSectionWithReport(registry *ToolRegistry, report ToolBundleDiscoveryReport) string {
	return buildInstalledToolBundlePromptSectionWithReportAndSuites(registry, report, nil)
}

func buildInstalledToolBundlePromptSectionWithReportAndSuites(registry *ToolRegistry, report ToolBundleDiscoveryReport, suites []ToolBundleSuiteReadinessItem) string {
	items := installedToolBundlePromptInventory(registry)
	hasDiagnostics := toolBundleDiscoveryReportHasDiagnostics(report)
	if len(items) == 0 && !hasDiagnostics && len(suites) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("## Installed Local Tool Bundles And Suite Readiness\n")
	b.WriteString("These are agent-local tools installed inside this exact workdir and exposed in the current tool loop. Treat them as first-class capabilities you can execute yourself when they match your claimed task.\n")
	b.WriteString("- If your active/claimed task asks for evidence, validation, inspection, conversion, or another capability covered by an installed bundle, use the installed tool directly before asking a peer.\n")
	b.WriteString("- Do not use agent_request to delegate your own claimed lane merely because a local tool call is required; agent_request is for peer-owned work, independent review, or a separate non-overlapping task.\n")
	b.WriteString("- Follow each tool schema literally. Do not invent local paths; first verify paths with read_file/list_directory or use a proven URL/artifact reference. If a bundle call fails from bad arguments, correct the arguments and retry once before publishing a blocker.\n")
	if len(suites) > 0 {
		b.WriteString("- Tool suite readiness:\n")
		limit := len(suites)
		if limit > 16 {
			limit = 16
		}
		for _, suite := range suites[:limit] {
			b.WriteString(fmt.Sprintf("  - %s status=%s", suite.Suite, suite.Status))
			if suite.Required {
				b.WriteString(" required=true")
			}
			if len(suite.Heartbeats) > 0 {
				b.WriteString(" heartbeats=" + strings.Join(suite.Heartbeats, ","))
			}
			if len(suite.ReadyBundles) > 0 {
				b.WriteString(" bundles=" + strings.Join(suite.ReadyBundles, ","))
			}
			if len(suite.WarningBundles) > 0 {
				b.WriteString(" warning_bundles=" + strings.Join(suite.WarningBundles, ","))
			}
			if len(suite.BlockedBundles) > 0 {
				b.WriteString(" blocked_bundles=" + strings.Join(suite.BlockedBundles, ","))
			}
			if len(suite.NativeProviders) > 0 {
				b.WriteString(" native=" + strings.Join(suite.NativeProviders, ","))
			}
			if len(suite.SuggestedBundles) > 0 {
				b.WriteString(" suggested_bundles=" + strings.Join(suite.SuggestedBundles, ","))
			}
			if len(suite.SuggestedActions) > 0 {
				b.WriteString(" next_action=" + suite.SuggestedActions[0])
			}
			if suite.Message != "" {
				b.WriteString(": " + suite.Message)
			}
			b.WriteString("\n")
		}
		if len(suites) > limit {
			b.WriteString(fmt.Sprintf("  - ... %d more suite readiness item(s) omitted\n", len(suites)-limit))
		}
	}
	if len(items) > 0 {
		b.WriteString("- Installed bundles:\n")
		limit := len(items)
		if limit > 12 {
			limit = 12
		}
		for _, item := range items[:limit] {
			line := fmt.Sprintf("  - %s", item.Name)
			attrs := []string{}
			if item.Version != "" {
				attrs = append(attrs, "version "+item.Version)
			}
			if item.TimeoutSeconds > 0 {
				attrs = append(attrs, fmt.Sprintf("timeout %ds", item.TimeoutSeconds))
			}
			if len(item.CapabilitySuites) > 0 {
				attrs = append(attrs, "suites "+strings.Join(item.CapabilitySuites, ","))
			}
			if len(item.ArtifactContracts) > 0 {
				attrs = append(attrs, "artifacts "+strings.Join(item.ArtifactContracts, ","))
			}
			if len(item.Dependencies) > 0 {
				attrs = append(attrs, "deps "+strings.Join(item.Dependencies, ","))
			}
			if len(attrs) > 0 {
				line += " (" + strings.Join(attrs, "; ") + ")"
			}
			if item.Description != "" {
				line += ": " + item.Description
			}
			b.WriteString(line + "\n")
		}
		if len(items) > limit {
			b.WriteString(fmt.Sprintf("  - ... %d more installed bundle(s) omitted\n", len(items)-limit))
		}
	}
	if len(report.Candidates) > 0 {
		b.WriteString("- Readiness:\n")
		limit := len(report.Candidates)
		if limit > 12 {
			limit = 12
		}
		for _, item := range report.Candidates[:limit] {
			b.WriteString(fmt.Sprintf("  - %s status=%s", item.Name, item.Status))
			if item.Code != "" {
				b.WriteString(" code=" + item.Code)
			}
			if item.Registered {
				b.WriteString(" registered=true")
			}
			if item.Configured {
				b.WriteString(" configured=true")
			}
			if item.RootKind != "" {
				b.WriteString(" [" + item.RootKind + "]")
			}
			if item.Message != "" {
				b.WriteString(": " + item.Message)
			}
			b.WriteString("\n")
		}
		if len(report.Candidates) > limit {
			b.WriteString(fmt.Sprintf("  - ... %d more readiness candidate(s) omitted\n", len(report.Candidates)-limit))
		}
	}
	if hasDiagnostics {
		b.WriteString("- Discovery diagnostics:\n")
		for _, diagnostic := range toolBundleDiscoveryPromptDiagnostics(report, 12) {
			b.WriteString(fmt.Sprintf("  - %s", diagnostic.Code))
			if diagnostic.Name != "" {
				b.WriteString(" " + diagnostic.Name)
			}
			if diagnostic.RootKind != "" {
				b.WriteString(" [" + diagnostic.RootKind + "]")
			}
			if diagnostic.Message != "" {
				b.WriteString(": " + diagnostic.Message)
			}
			b.WriteString("\n")
		}
	}
	if report.Config.Present || len(report.ConfigDiagnostics) > 0 {
		b.WriteString("- Registry config:\n")
		if report.Config.Present {
			b.WriteString(fmt.Sprintf("  - %s status=%s mode=%s", installedToolBundleConfigName, firstNonEmpty(report.Config.Status, "loaded"), firstNonEmpty(report.Config.Mode, "auto")))
			if len(report.Config.Enabled) > 0 {
				b.WriteString(" enabled=" + strings.Join(report.Config.Enabled, ","))
			}
			if len(report.Config.Disabled) > 0 {
				b.WriteString(" disabled=" + strings.Join(report.Config.Disabled, ","))
			}
			if report.Config.Message != "" {
				b.WriteString(": " + report.Config.Message)
			}
			b.WriteString("\n")
		}
		for _, diagnostic := range report.ConfigDiagnostics[:min(len(report.ConfigDiagnostics), 12)] {
			b.WriteString(fmt.Sprintf("  - %s", diagnostic.Code))
			if diagnostic.Name != "" {
				b.WriteString(" " + diagnostic.Name)
			}
			if diagnostic.Message != "" {
				b.WriteString(": " + diagnostic.Message)
			}
			b.WriteString("\n")
		}
	}
	if len(report.Dependencies) > 0 {
		b.WriteString("- Dependency checks:\n")
		for _, diagnostic := range report.Dependencies[:min(len(report.Dependencies), 12)] {
			b.WriteString(fmt.Sprintf("  - %s", diagnostic.Code))
			if diagnostic.Name != "" {
				b.WriteString(" " + diagnostic.Name)
			}
			if diagnostic.RootKind != "" {
				b.WriteString(" [" + diagnostic.RootKind + "]")
			}
			if diagnostic.Message != "" {
				b.WriteString(": " + diagnostic.Message)
			}
			b.WriteString("\n")
		}
	}
	if len(report.Healthchecks) > 0 {
		b.WriteString("- Healthchecks:\n")
		for _, diagnostic := range report.Healthchecks[:min(len(report.Healthchecks), 12)] {
			b.WriteString(fmt.Sprintf("  - %s", diagnostic.Code))
			if diagnostic.Name != "" {
				b.WriteString(" " + diagnostic.Name)
			}
			if diagnostic.RootKind != "" {
				b.WriteString(" [" + diagnostic.RootKind + "]")
			}
			if diagnostic.Message != "" {
				b.WriteString(": " + diagnostic.Message)
			}
			b.WriteString("\n")
		}
	}
	return strings.TrimSpace(b.String())
}

func toolBundleDiscoveryReportHasDiagnostics(report ToolBundleDiscoveryReport) bool {
	return len(report.ConfigDiagnostics) > 0 || len(report.Skipped) > 0 || len(report.Errors) > 0 || len(report.Collisions) > 0
}

func withToolBundleReadiness(report ToolBundleDiscoveryReport) ToolBundleDiscoveryReport {
	report.Candidates = toolBundleReadinessItems(report)
	report.CandidateCount = len(report.Candidates)
	return report
}

func toolBundleReadinessItems(report ToolBundleDiscoveryReport) []ToolBundleReadinessItem {
	type candidateState struct {
		item     ToolBundleReadinessItem
		priority int
	}
	candidates := map[string]candidateState{}
	add := func(diagnostic ToolBundleDiscoveryDiagnostic, status string, registered, configured bool, priority int) {
		name := strings.TrimSpace(diagnostic.Name)
		if name == "" {
			name = sanitizeManagedToolBundleName(filepath.Base(strings.TrimSpace(diagnostic.Dir)))
		}
		if name == "" {
			return
		}
		item := ToolBundleReadinessItem{
			Name:             name,
			Status:           status,
			Code:             strings.TrimSpace(diagnostic.Code),
			RootKind:         strings.TrimSpace(diagnostic.RootKind),
			Root:             strings.TrimSpace(diagnostic.Root),
			Dir:              strings.TrimSpace(diagnostic.Dir),
			ExistingDir:      strings.TrimSpace(diagnostic.ExistingDir),
			CapabilitySuites: uniqueTrimmedCSVStrings(diagnostic.CapabilitySuites),
			Message:          strings.TrimSpace(diagnostic.Message),
			Registered:       registered,
			Configured:       configured || containsTrimmedString(report.Config.Enabled, name) || containsTrimmedString(report.Config.Disabled, name),
		}
		if item.Status == "" {
			item.Status = "unknown"
		}
		current, ok := candidates[name]
		if ok && current.priority > priority {
			return
		}
		if ok && current.priority == priority && current.item.Status == item.Status {
			if current.item.Message != "" && item.Message == "" {
				item.Message = current.item.Message
			}
			if current.item.Dir != "" && item.Dir == "" {
				item.Dir = current.item.Dir
			}
		}
		candidates[name] = candidateState{item: item, priority: priority}
	}
	for _, diagnostic := range report.Installed {
		add(diagnostic, "ready", true, false, 10)
	}
	for _, diagnostic := range report.Dependencies {
		switch strings.TrimSpace(diagnostic.Code) {
		case "optional_dependency_missing", "optional_dependency_unchecked":
			add(diagnostic, "ready_with_warnings", true, false, 20)
		case "missing_dependency", "dependency_unchecked":
			add(diagnostic, "blocked", false, false, 80)
		}
	}
	for _, diagnostic := range report.Healthchecks {
		if strings.TrimSpace(diagnostic.Code) != "healthcheck_passed" {
			add(diagnostic, "blocked", false, false, 80)
		}
	}
	for _, diagnostic := range report.Skipped {
		status := "skipped"
		priority := 50
		configured := false
		switch strings.TrimSpace(diagnostic.Code) {
		case "disabled_by_config":
			status = "disabled"
			priority = 70
			configured = true
		case "not_enabled_by_config":
			status = "skipped"
			configured = true
		}
		add(diagnostic, status, false, configured, priority)
	}
	for _, diagnostic := range report.ConfigDiagnostics {
		status := "configured"
		priority := 40
		switch strings.TrimSpace(diagnostic.Code) {
		case "configured_tool_bundle_missing":
			status = "missing"
			priority = 90
		case "tool_bundle_config_conflict":
			status = "config_conflict"
			priority = 95
		}
		add(diagnostic, status, false, true, priority)
	}
	for _, diagnostic := range report.Errors {
		status := "blocked"
		priority := 85
		if strings.TrimSpace(diagnostic.Code) == "configured_tool_bundle_missing" {
			status = "missing"
			priority = 90
		}
		add(diagnostic, status, false, true, priority)
	}
	for _, diagnostic := range report.Collisions {
		add(diagnostic, "collided", false, false, 88)
	}
	names := make([]string, 0, len(candidates))
	for name := range candidates {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]ToolBundleReadinessItem, 0, len(names))
	for _, name := range names {
		out = append(out, candidates[name].item)
	}
	return out
}

func toolBundleSuiteReadinessForAgent(agent *Agent) []ToolBundleSuiteReadinessItem {
	if agent == nil {
		return nil
	}
	candidates := append([]ToolBundleReadinessItem(nil), agent.toolBundleDiscovery.Candidates...)
	if len(candidates) == 0 {
		candidates = toolBundleReadinessItems(agent.toolBundleDiscovery)
	}
	return toolBundleSuiteReadiness(agent.Anatomy, agent.registry, candidates)
}

func toolBundleSuiteReadiness(anatomy AgentAnatomyConfig, registry *ToolRegistry, candidates []ToolBundleReadinessItem) []ToolBundleSuiteReadinessItem {
	required := requiredToolSuitesFromAnatomy(anatomy)
	suites := map[string]*ToolBundleSuiteReadinessItem{}
	ensure := func(suite string) *ToolBundleSuiteReadinessItem {
		suite = strings.TrimSpace(suite)
		if suite == "" {
			return nil
		}
		item, ok := suites[suite]
		if ok {
			return item
		}
		item = &ToolBundleSuiteReadinessItem{
			Suite:      suite,
			Status:     "available",
			Required:   len(required[suite]) > 0,
			Heartbeats: sortedStringSet(required[suite]),
		}
		suites[suite] = item
		return item
	}
	for suite := range required {
		ensure(suite)
	}
	for _, candidate := range candidates {
		name := strings.TrimSpace(candidate.Name)
		if name == "" {
			continue
		}
		for _, suite := range uniqueTrimmedCSVStrings(candidate.CapabilitySuites) {
			item := ensure(suite)
			if item == nil {
				continue
			}
			item.CandidateBundles = append(item.CandidateBundles, name)
			switch strings.TrimSpace(candidate.Status) {
			case "ready":
				item.ReadyBundles = append(item.ReadyBundles, name)
			case "ready_with_warnings":
				item.WarningBundles = append(item.WarningBundles, name)
			case "blocked", "missing", "disabled", "skipped", "config_conflict", "collided":
				item.BlockedBundles = append(item.BlockedBundles, name)
			default:
				item.BlockedBundles = append(item.BlockedBundles, name)
			}
		}
	}
	for _, item := range suites {
		item.NativeProviders = nativeToolSuiteProviders(registry, item.Suite)
		item.ReadyBundles = normalizeManagedToolBundleList(item.ReadyBundles)
		item.WarningBundles = normalizeManagedToolBundleList(item.WarningBundles)
		item.BlockedBundles = normalizeManagedToolBundleList(item.BlockedBundles)
		item.CandidateBundles = normalizeManagedToolBundleList(item.CandidateBundles)
		item.NativeProviders = uniqueTrimmedCSVStrings(item.NativeProviders)
		sort.Strings(item.NativeProviders)
		item.Status = toolBundleSuiteReadinessStatus(*item)
		item.SuggestedBundles = suggestedToolBundlesForSuite(item.Suite)
		item.SuggestedActions = suggestedToolBundleActionsForSuite(*item)
		item.Message = toolBundleSuiteReadinessMessage(*item)
	}
	names := make([]string, 0, len(suites))
	for suite := range suites {
		names = append(names, suite)
	}
	sort.Slice(names, func(i, j int) bool {
		left := suites[names[i]]
		right := suites[names[j]]
		if left.Required != right.Required {
			return left.Required
		}
		return left.Suite < right.Suite
	})
	out := make([]ToolBundleSuiteReadinessItem, 0, len(names))
	for _, suite := range names {
		out = append(out, *suites[suite])
	}
	return out
}

func requiredToolSuitesFromAnatomy(anatomy AgentAnatomyConfig) map[string]map[string]struct{} {
	required := map[string]map[string]struct{}{}
	for _, heartbeat := range anatomy.Heartbeats {
		heartbeat = normalizeAgentHeartbeatSpec(heartbeat)
		if !heartbeatEnabled(heartbeat) {
			continue
		}
		label := firstNonEmpty(strings.TrimSpace(heartbeat.ID), strings.TrimSpace(heartbeat.Kind))
		for _, suite := range uniqueTrimmedCSVStrings(heartbeat.ToolSuites) {
			if suite == "" {
				continue
			}
			if _, ok := required[suite]; !ok {
				required[suite] = map[string]struct{}{}
			}
			if label != "" {
				required[suite][label] = struct{}{}
			}
		}
	}
	return required
}

func sortedStringSet(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func nativeToolSuiteProviders(registry *ToolRegistry, suite string) []string {
	if registry == nil {
		return nil
	}
	suite = strings.TrimSpace(suite)
	providers := []string{}
	addIfPresent := func(names ...string) {
		for _, name := range names {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			if _, ok := registry.Get(name); ok {
				providers = append(providers, name)
			}
		}
	}
	switch suite {
	case "local_execution":
		addIfPresent("shell")
	case "memory_and_docs_read":
		addIfPresent("read_file", "list_directory", "memory_read", "memory_search", "workspace_doc_get")
	case "local_log_read":
		addIfPresent("read_file", "list_directory", "shell")
	case "task_authority":
		addIfPresent(
			"task_submit",
			"project_bootstrap",
			"project_phase_transition",
			"project_checkout_materialize",
			"project_checkout_register",
			"project_branch_commit",
			"project_patch_queue_submit",
			"project_patch_queue_lifecycle",
			"project_patch_queue_cas_record",
			"project_patch_queue_integrate",
			"project_patch_queue_followup",
		)
	case "workspace_tools":
		addIfPresent("workspace_doc_get", "workspace_doc_put")
		for name, tool := range registry.tools {
			switch tool.(type) {
			case *RhizomeWorkspaceTool, *RhizomeMCPTool:
				providers = append(providers, name)
			}
		}
	case "rhizome_read":
		addIfPresent(
			"workspace_doc_get",
			"project_patch_queue_list",
			"coalition_status",
			"reviewer_route",
			"reviewer_scarcity",
			"memory_coherence_read",
			"memory_promotion_read",
		)
	case "workspace_docs_read":
		addIfPresent("workspace_doc_get")
	case "bounded_task_submit":
		addIfPresent("task_submit", "agent_request")
	case "patch_queue_read":
		addIfPresent("project_patch_queue_list")
	case "local_tests_read":
		addIfPresent("shell", "read_file", "list_directory")
	}
	providers = uniqueTrimmedCSVStrings(providers)
	sort.Strings(providers)
	return providers
}

func toolBundleSuiteReadinessStatus(item ToolBundleSuiteReadinessItem) string {
	if len(item.NativeProviders) > 0 || len(item.ReadyBundles) > 0 {
		if len(item.WarningBundles) > 0 && len(item.ReadyBundles) == 0 && len(item.NativeProviders) == 0 {
			return "ready_with_warnings"
		}
		return "ready"
	}
	if len(item.WarningBundles) > 0 {
		return "ready_with_warnings"
	}
	if len(item.BlockedBundles) > 0 {
		return "blocked"
	}
	if item.Required {
		return "missing"
	}
	return "available"
}

func toolBundleSuiteReadinessMessage(item ToolBundleSuiteReadinessItem) string {
	switch strings.TrimSpace(item.Status) {
	case "ready":
		providers := append([]string{}, item.ReadyBundles...)
		providers = append(providers, item.NativeProviders...)
		return fmt.Sprintf("tool suite %q is available via %s", item.Suite, strings.Join(uniqueTrimmedCSVStrings(providers), ","))
	case "ready_with_warnings":
		return fmt.Sprintf("tool suite %q is available via %s with diagnostics", item.Suite, strings.Join(uniqueTrimmedCSVStrings(item.WarningBundles), ","))
	case "blocked":
		return fmt.Sprintf("tool suite %q is declared only by blocked bundles: %s", item.Suite, strings.Join(uniqueTrimmedCSVStrings(item.BlockedBundles), ","))
	case "missing":
		return fmt.Sprintf("required heartbeat tool suite %q has no installed bundle or native provider", item.Suite)
	default:
		if len(item.CandidateBundles) > 0 {
			return fmt.Sprintf("tool suite %q is declared by candidate bundles: %s", item.Suite, strings.Join(uniqueTrimmedCSVStrings(item.CandidateBundles), ","))
		}
		return ""
	}
}

func suggestedToolBundlesForSuite(suite string) []string {
	switch strings.TrimSpace(suite) {
	case "browser_read_only", "screenshot_capture":
		return []string{"browser_visual_probe", "browser_session"}
	case "console_read", "browser_interactive", "browser_unrestricted":
		return []string{"browser_session"}
	default:
		return nil
	}
}

func suggestedToolBundleActionsForSuite(item ToolBundleSuiteReadinessItem) []string {
	switch strings.TrimSpace(item.Status) {
	case "missing", "blocked", "ready_with_warnings":
	default:
		return nil
	}
	actions := []string{}
	if len(item.SuggestedBundles) > 0 {
		actions = append(actions, "if a suggested bundle is already copied into tools/<bundle> or .runtime-config/tool-bundles/<bundle>, call tool_bundle_registry action=enable name=<bundle>")
	}
	if strings.TrimSpace(item.Status) == "blocked" && len(item.BlockedBundles) > 0 {
		actions = append(actions, "inspect tool_bundle_registry action=status and fix the blocked bundle dependency, healthcheck, config, or collision before refreshing")
	}
	actions = append(actions, fmt.Sprintf("to author a new local tool, call tool_bundle_registry action=scaffold name=<bundle_name> capability_suites=%q, edit the generated tool, then refresh", item.Suite))
	actions = append(actions, fmt.Sprintf("write, download, or copy a bundle with tool.json capability_suites including %q, then call tool_bundle_registry action=install source_path=<bundle_dir>", item.Suite))
	actions = append(actions, "call tool_bundle_registry action=refresh after filesystem changes so the next LLM cycle sees the updated tool plane")
	return actions
}

func toolBundleDiscoveryPromptDiagnostics(report ToolBundleDiscoveryReport, limit int) []ToolBundleDiscoveryDiagnostic {
	diagnostics := make([]ToolBundleDiscoveryDiagnostic, 0, len(report.Errors)+len(report.Collisions)+len(report.Skipped))
	diagnostics = append(diagnostics, report.Errors...)
	diagnostics = append(diagnostics, report.Collisions...)
	diagnostics = append(diagnostics, report.Skipped...)
	if limit > 0 && len(diagnostics) > limit {
		return append([]ToolBundleDiscoveryDiagnostic(nil), diagnostics[:limit]...)
	}
	return diagnostics
}

func (t *InstalledToolBundleTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t == nil || strings.TrimSpace(t.dir) == "" || len(t.manifest.Command) == 0 {
		return &ToolResult{Output: "installed tool bundle is not configured", IsError: true}
	}
	timeout := installedToolBundleTimeout(t.manifest.TimeoutSeconds)
	artifactRoot := filepath.Join(t.workdir, ".runtime-config", "tool-artifacts", t.Name(), time.Now().UTC().Format("20060102T150405.000000000Z"))
	if err := os.MkdirAll(artifactRoot, 0o755); err != nil {
		return &ToolResult{Output: fmt.Sprintf("create artifact root: %v", err), IsError: true}
	}
	payload, _ := json.Marshal(args)
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	output, err := installedToolBundleRunner(runCtx, t.manifest.Command, t.dir, t.workdir, artifactRoot, payload)
	if len(bytes.TrimSpace(output)) == 0 {
		if fallbackOutput, ok := readInstalledToolBundleArtifactResult(artifactRoot); ok {
			output = fallbackOutput
		}
	}
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("%s\n%s", err.Error(), truncate(string(output), 8192)), IsError: true}
	}
	if len(output) == 0 {
		return &ToolResult{Output: "(no output)"}
	}
	outputText := truncate(string(output), 64*1024)
	if _, ok := installedToolBundleFailureStatus(output); ok {
		return &ToolResult{Output: outputText, IsError: true}
	}
	return &ToolResult{Output: outputText}
}

func readInstalledToolBundleArtifactResult(artifactRoot string) ([]byte, bool) {
	if strings.TrimSpace(artifactRoot) == "" {
		return nil, false
	}
	raw, err := os.ReadFile(filepath.Join(artifactRoot, "result.json"))
	if err != nil || len(bytes.TrimSpace(raw)) == 0 {
		return nil, false
	}
	return raw, true
}

func installedToolBundleFailureStatus(output []byte) (string, bool) {
	trimmed := bytes.TrimSpace(output)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return "", false
	}
	var envelope struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(trimmed, &envelope); err != nil {
		return "", false
	}
	status := strings.ToLower(strings.TrimSpace(envelope.Status))
	switch status {
	case "fail", "failed", "failure", "error", "block", "blocked":
		return status, true
	default:
		return status, false
	}
}

func runInstalledToolBundleCommand(ctx context.Context, command []string, bundleDir, workdir, artifactRoot string, stdin []byte) ([]byte, error) {
	if len(command) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	if installedToolBundleAllowsPersistentChildren(bundleDir) {
		return runInstalledPersistentToolBundleCommand(ctx, command, bundleDir, workdir, artifactRoot, stdin)
	}
	name := command[0]
	args := append([]string(nil), command[1:]...)
	cmd := exec.Command(name, args...)
	cmd.Dir = bundleDir
	cmd.Stdin = bytes.NewReader(stdin)
	cmd.Env = installedToolBundleCommandEnv(bundleDir, workdir, artifactRoot, false)
	out, err := runShellToolCommand(ctx, cmd)
	if ctx.Err() != nil {
		return out, ctx.Err()
	}
	if err != nil {
		return out, err
	}
	return out, nil
}

func installedToolBundleAllowsPersistentChildren(bundleDir string) bool {
	name := strings.ToLower(strings.TrimSpace(filepath.Base(filepath.Clean(bundleDir))))
	return name == "browser_session"
}

func installedToolBundleCommandEnv(bundleDir, workdir, artifactRoot string, persistentChildren bool) []string {
	env := appendKnownBrowserDirsToEnvPath(os.Environ())
	env = append(env,
		"RHIZOME_TOOL_BUNDLE_DIR="+bundleDir,
		"RHIZOME_TOOL_WORKDIR="+workdir,
		"RHIZOME_TOOL_ARTIFACT_DIR="+artifactRoot,
	)
	if persistentChildren {
		env = append(env, "RHIZOME_TOOL_BUNDLE_PERSISTENT_CHILDREN=1")
	}
	return env
}

func runInstalledPersistentToolBundleCommand(ctx context.Context, command []string, bundleDir, workdir, artifactRoot string, stdin []byte) ([]byte, error) {
	if len(command) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	name := command[0]
	args := append([]string(nil), command[1:]...)
	cmd := exec.Command(name, args...)
	cmd.Dir = bundleDir
	cmd.Stdin = bytes.NewReader(stdin)
	cmd.Env = installedToolBundleCommandEnv(bundleDir, workdir, artifactRoot, true)
	output := newBoundedShellOutputBuffer(shellToolOutputLimitBytes)
	cmd.Stdout = output
	cmd.Stderr = output

	if err := ctx.Err(); err != nil {
		return output.Bytes(), err
	}
	if err := cmd.Start(); err != nil {
		return output.Bytes(), err
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	select {
	case err := <-waitCh:
		return output.Bytes(), err
	case <-ctx.Done():
		output.WriteString(fmt.Sprintf("\n[tool bundle cleanup] context ended; persistent bundle primary process kill attempted; wait grace %s\n", shellToolKillWaitBudget))
		var killErr error
		if cmd.Process != nil {
			killErr = killShellCommandProcessTreeByPID(cmd.Process.Pid)
		}
		select {
		case waitErr := <-waitCh:
			if killErr != nil {
				output.WriteString(fmt.Sprintf("[tool bundle cleanup] process tree kill error: %v\n", killErr))
			}
			if waitErr != nil {
				return output.Bytes(), waitErr
			}
			return output.Bytes(), ctx.Err()
		case <-time.After(shellToolKillWaitBudget):
			if killErr != nil {
				output.WriteString(fmt.Sprintf("[tool bundle cleanup] process tree kill error: %v\n", killErr))
			}
			output.WriteString(fmt.Sprintf("[tool bundle cleanup] process wait grace expired after %s\n", shellToolKillWaitBudget))
			return output.Bytes(), ctx.Err()
		}
	}
}

func installedToolBundleTimeout(seconds int) time.Duration {
	if seconds <= 0 {
		return installedToolBundleDefaultLimit
	}
	value := time.Duration(seconds) * time.Second
	if value > installedToolBundleMaxLimit {
		return installedToolBundleMaxLimit
	}
	return value
}

func pathIsInside(root, candidate string) bool {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	candidateAbs, err := filepath.Abs(candidate)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(rootAbs, candidateAbs)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}
