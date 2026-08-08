package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"time"
)

const (
	toolBundleRegistryDefaultDownloadLimit = 50 * 1024 * 1024
	toolBundleRegistryMaxDownloadLimit     = 200 * 1024 * 1024
	toolBundleRegistryDefaultDownloadMS    = 120_000
	toolBundleRegistryMaxDownloadMS        = 300_000
)

type ToolBundleRegistryTool struct {
	workdir string
	mutable bool
	inspect func() ToolBundleDiscoveryReport
	refresh func() ToolBundleDiscoveryReport
}

type ToolBundleManifestMigration struct {
	Name             string   `json:"name,omitempty"`
	Status           string   `json:"status"`
	Changed          bool     `json:"changed"`
	RootKind         string   `json:"root_kind,omitempty"`
	Dir              string   `json:"dir,omitempty"`
	SchemaBefore     string   `json:"schema_before,omitempty"`
	SchemaAfter      string   `json:"schema_after,omitempty"`
	CapabilitySuites []string `json:"capability_suites,omitempty"`
	Message          string   `json:"message,omitempty"`
}

func NewToolBundleRegistryTool(workdir string, mutable bool, inspect, refresh func() ToolBundleDiscoveryReport) *ToolBundleRegistryTool {
	return &ToolBundleRegistryTool{
		workdir: strings.TrimSpace(workdir),
		mutable: mutable,
		inspect: inspect,
		refresh: refresh,
	}
}

func (t *ToolBundleRegistryTool) Name() string { return "tool_bundle_registry" }

func (t *ToolBundleRegistryTool) Description() string {
	return "List, enable, disable, scaffold, install, download, migrate, and refresh agent-local tool bundles in this workdir. Use this after copying or writing a bundle with tool.json under tools/<bundle>/, before/after copying a third-party bundle into .runtime-config/tool-bundles/<bundle>, to download a zip bundle from source_url, or to migrate legacy manifests to the v2 metadata contract."
}

func (t *ToolBundleRegistryTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"description": "One of: list, status, register, enable, disable, scaffold, install, download, install_url, migrate, migrate_manifest, refresh. register is an alias for enable; install_url is an alias for download.",
				"enum":        []string{"list", "status", "register", "enable", "disable", "scaffold", "install", "download", "install_url", "migrate", "migrate_manifest", "refresh"},
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Bundle name to register/enable/disable/install. Names are sanitized to tool-safe identifiers.",
			},
			"names": map[string]any{
				"type":        "array",
				"description": "Optional bundle names for register/enable/disable.",
				"items":       map[string]any{"type": "string"},
			},
			"source_path": map[string]any{
				"type":        "string",
				"description": "Directory containing tool.json for install. Relative paths resolve under the agent workdir; absolute paths are allowed in this trusted runtime.",
			},
			"source_url": map[string]any{
				"type":        "string",
				"description": "HTTP(S) URL to a zip archive containing tool.json at the archive root or inside a single bundle directory. Used by action=download/install_url.",
			},
			"max_bytes": map[string]any{
				"type":        "integer",
				"description": "Optional maximum compressed download size and uncompressed extraction size for source_url zip archives. Defaults to 50MiB and is capped at 200MiB.",
			},
			"download_timeout_ms": map[string]any{
				"type":        "integer",
				"description": "Optional bounded HTTP download timeout in milliseconds. Defaults to 120000 and is capped at 300000.",
			},
			"capability_suites": map[string]any{
				"type":        "array",
				"description": "Capability suites declared by scaffolded or migrated bundles, for example browser_read_only or custom:my_suite.",
				"items":       map[string]any{"type": "string"},
			},
			"all": map[string]any{
				"type":        "boolean",
				"description": "For action=migrate, migrate all discovered local manifests when true. If name/names is omitted, migrate all manifests by default.",
			},
			"description": map[string]any{
				"type":        "string",
				"description": "Description for a scaffolded bundle manifest.",
			},
			"runtime": map[string]any{
				"type":        "string",
				"description": "Scaffold runtime template. Defaults to node.",
				"enum":        []string{"node", "javascript", "python", "custom"},
			},
			"command": map[string]any{
				"type":        "array",
				"description": "Optional explicit command for scaffold. Required for runtime=custom. If omitted, a tiny node/python JSON-stdin template is written.",
				"items":       map[string]any{"type": "string"},
			},
			"parameters": map[string]any{
				"type":        "object",
				"description": "Optional JSON schema for a scaffolded bundle. Defaults to an object that accepts additional properties.",
			},
			"overwrite": map[string]any{
				"type":        "boolean",
				"description": "Allow scaffold to overwrite an existing tools/<name> bundle. Defaults to false.",
			},
			"refresh": map[string]any{
				"type":        "boolean",
				"description": "Refresh installed bundle discovery after mutation. Defaults to true.",
			},
		},
	}
}

func (t *ToolBundleRegistryTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	action := strings.ToLower(strings.TrimSpace(stringArg(args, "action")))
	if action == "" {
		action = "list"
	}
	refreshRequested := boolArg(args, "refresh", true)
	mutated := false
	configMutated := false
	installed := []string{}
	downloaded := []string{}
	scaffolded := []string{}
	migrated := []string{}
	manifestMigrations := []ToolBundleManifestMigration{}
	repairedConfig := false
	var err error

	switch action {
	case "list", "status":
	case "refresh":
	default:
		if !t.mutable {
			return toolBundleRegistryToolError("tool bundle registry mutation is disabled for this agent")
		}
	}

	config, configStatus, _ := loadInstalledToolBundleRegistryConfig(t.workdir)
	if strings.TrimSpace(configStatus.Status) != "loaded" {
		config = InstalledToolBundleRegistryConfig{SchemaVersion: installedToolBundleConfigSchema, Mode: "auto"}
	}

	switch action {
	case "list", "status", "refresh":
	case "enable", "register":
		names := toolBundleRegistryToolNames(args)
		if len(names) == 0 {
			return toolBundleRegistryToolError("name or names is required")
		}
		config.Enabled = normalizeManagedToolBundleList(append(config.Enabled, names...))
		config.Disabled = removeToolBundleRegistryNames(config.Disabled, names)
		mutated = true
		configMutated = true
	case "disable":
		names := toolBundleRegistryToolNames(args)
		if len(names) == 0 {
			return toolBundleRegistryToolError("name or names is required")
		}
		config.Disabled = normalizeManagedToolBundleList(append(config.Disabled, names...))
		config.Enabled = removeToolBundleRegistryNames(config.Enabled, names)
		mutated = true
		configMutated = true
	case "install":
		var name string
		name, err = t.installFromSource(args)
		if err != nil {
			return toolBundleRegistryToolError(err.Error())
		}
		installed = append(installed, name)
		config.Enabled = normalizeManagedToolBundleList(append(config.Enabled, name))
		config.Disabled = removeToolBundleRegistryNames(config.Disabled, []string{name})
		mutated = true
		configMutated = true
	case "download", "install_url":
		var name string
		name, err = t.installFromURL(ctx, args)
		if err != nil {
			return toolBundleRegistryToolError(err.Error())
		}
		downloaded = append(downloaded, name)
		config.Enabled = normalizeManagedToolBundleList(append(config.Enabled, name))
		config.Disabled = removeToolBundleRegistryNames(config.Disabled, []string{name})
		mutated = true
		configMutated = true
	case "migrate", "migrate_manifest":
		manifestMigrations, err = t.migrateManifests(args)
		if err != nil {
			return toolBundleRegistryToolError(err.Error())
		}
		for _, migration := range manifestMigrations {
			if migration.Changed {
				migrated = append(migrated, migration.Name)
				mutated = true
			}
		}
		migrated = normalizeManagedToolBundleList(migrated)
	case "scaffold":
		var name string
		name, err = t.scaffoldBundle(args)
		if err != nil {
			return toolBundleRegistryToolError(err.Error())
		}
		scaffolded = append(scaffolded, name)
		config.Enabled = normalizeManagedToolBundleList(append(config.Enabled, name))
		config.Disabled = removeToolBundleRegistryNames(config.Disabled, []string{name})
		mutated = true
		configMutated = true
	default:
		return toolBundleRegistryToolError(fmt.Sprintf("unknown action %q", action))
	}

	if configMutated {
		config.SchemaVersion = installedToolBundleConfigSchema
		config.Mode = "explicit"
		if err := saveInstalledToolBundleRegistryConfig(t.workdir, config); err != nil {
			return toolBundleRegistryToolError(err.Error())
		}
		repairedConfig = configStatus.Present && strings.TrimSpace(configStatus.Status) != "loaded"
	}

	refreshApplied := false
	report := ToolBundleDiscoveryReport{}
	if action == "refresh" || (mutated && refreshRequested) {
		report = t.refreshDiscovery()
		refreshApplied = true
	} else {
		report = t.inspectDiscovery()
	}
	_, status, diagnostics := loadInstalledToolBundleRegistryConfig(t.workdir)
	payload := map[string]any{
		"contract_version":         "tool_bundle_registry_tool.v1",
		"status":                   "ok",
		"action":                   action,
		"mutated":                  mutated,
		"installed":                installed,
		"downloaded":               downloaded,
		"scaffolded":               scaffolded,
		"migrated":                 migrated,
		"manifest_migrations":      manifestMigrations,
		"refresh_applied":          refreshApplied,
		"available_next_llm_cycle": refreshApplied,
		"config_path":              installedToolBundleRegistryConfigPath(t.workdir),
		"config":                   status,
		"config_diagnostics":       diagnostics,
		"repaired_config":          repairedConfig,
		"discovery":                report,
		"registered_tools":         installedToolBundleDiscoveryNames(report.Installed),
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return toolBundleRegistryToolError(err.Error())
	}
	return &ToolResult{Output: string(raw)}
}

func (t *ToolBundleRegistryTool) inspectDiscovery() ToolBundleDiscoveryReport {
	if t.inspect != nil {
		return t.inspect()
	}
	return RegisterInstalledToolBundles(NewToolRegistry(), t.workdir)
}

func (t *ToolBundleRegistryTool) refreshDiscovery() ToolBundleDiscoveryReport {
	if t.refresh != nil {
		return t.refresh()
	}
	return RegisterInstalledToolBundles(NewToolRegistry(), t.workdir)
}

func (t *ToolBundleRegistryTool) installFromSource(args map[string]any) (string, error) {
	source, err := resolveToolBundleRegistrySourcePath(t.workdir, stringArg(args, "source_path"))
	if err != nil {
		return "", err
	}
	info, err := os.Stat(source)
	if err != nil {
		return "", fmt.Errorf("stat source_path: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("source_path must be a directory containing %s", installedToolBundleManifestName)
	}
	manifest, err := loadInstalledToolBundleManifest(source)
	if err != nil {
		return "", fmt.Errorf("load source manifest: %w", err)
	}
	names := toolBundleRegistryToolNames(args)
	name := strings.TrimSpace(manifest.Name)
	if len(names) > 0 {
		name = names[0]
	}
	name = sanitizeManagedToolBundleName(name)
	if name == "" {
		return "", fmt.Errorf("installed bundle name is empty after sanitization")
	}
	dest := filepath.Join(t.workdir, ".runtime-config", "tool-bundles", name)
	if !pathIsInside(t.workdir, dest) {
		return "", fmt.Errorf("tool bundle destination escapes workdir")
	}
	if sameFilesystemPath(source, dest) {
		return name, nil
	}
	if pathIsInside(dest, source) {
		return "", fmt.Errorf("source_path must not be inside destination bundle directory %s", dest)
	}
	if err := copyManagedToolBundleDir(t.workdir, source, dest); err != nil {
		return "", fmt.Errorf("copy tool bundle: %w", err)
	}
	return name, nil
}

func (t *ToolBundleRegistryTool) installFromURL(ctx context.Context, args map[string]any) (string, error) {
	sourceURL := strings.TrimSpace(stringArg(args, "source_url"))
	if sourceURL == "" {
		sourceURL = strings.TrimSpace(stringArg(args, "url"))
	}
	if sourceURL == "" {
		return "", fmt.Errorf("source_url is required")
	}
	parsed, err := url.Parse(sourceURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("source_url must be an absolute HTTP(S) URL")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
	default:
		return "", fmt.Errorf("source_url must use http or https")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	downloadCtx, cancel := context.WithTimeout(ctx, toolBundleRegistryDownloadTimeout(args))
	defer cancel()
	req, err := http.NewRequestWithContext(downloadCtx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "rhizome-tool-bundle-registry/1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("download source_url: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("download source_url returned HTTP %d", resp.StatusCode)
	}
	limit := toolBundleRegistryDownloadLimit(args)
	raw, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
	if err != nil {
		return "", fmt.Errorf("read source_url body: %w", err)
	}
	if len(raw) > limit {
		return "", fmt.Errorf("source_url archive exceeds max_bytes=%d", limit)
	}
	return t.installFromZipBytes(args, raw, sourceURL, limit)
}

func (t *ToolBundleRegistryTool) installFromZipBytes(args map[string]any, raw []byte, sourceURL string, extractLimit int) (string, error) {
	reader, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return "", fmt.Errorf("source_url must return a zip archive containing %s: %w", installedToolBundleManifestName, err)
	}
	root, manifest, err := selectToolBundleZipRoot(reader, toolBundleRegistryToolNames(args))
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(manifest.Name)
	if names := toolBundleRegistryToolNames(args); len(names) > 0 {
		name = names[0]
	}
	name = sanitizeManagedToolBundleName(name)
	if name == "" {
		return "", fmt.Errorf("downloaded bundle name is empty after sanitization")
	}
	tempParent := filepath.Join(t.workdir, ".runtime-config")
	if err := os.MkdirAll(tempParent, 0o755); err != nil {
		return "", err
	}
	tempDir, err := os.MkdirTemp(tempParent, ".tool-bundle-download-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tempDir)
	if !pathIsInside(t.workdir, tempDir) {
		return "", fmt.Errorf("temporary tool bundle directory escapes workdir")
	}
	if err := extractToolBundleZipRoot(reader, root, tempDir, int64(extractLimit)); err != nil {
		return "", fmt.Errorf("extract tool bundle zip: %w", err)
	}
	manifest, err = loadInstalledToolBundleManifest(tempDir)
	if err != nil {
		return "", fmt.Errorf("load downloaded bundle manifest: %w", err)
	}
	manifest.Provenance = downloadedToolBundleProvenance(manifest.Provenance, sourceURL)
	if err := writeInstalledToolBundleManifestFile(tempDir, manifest); err != nil {
		return "", err
	}
	dest := filepath.Join(t.workdir, ".runtime-config", "tool-bundles", name)
	if !pathIsInside(t.workdir, dest) {
		return "", fmt.Errorf("tool bundle destination escapes workdir")
	}
	if err := copyManagedToolBundleDir(t.workdir, tempDir, dest); err != nil {
		return "", fmt.Errorf("copy downloaded tool bundle: %w", err)
	}
	return name, nil
}

func (t *ToolBundleRegistryTool) migrateManifests(args map[string]any) ([]ToolBundleManifestMigration, error) {
	requested := toolBundleRegistryToolNames(args)
	requestedSet := map[string]struct{}{}
	for _, name := range requested {
		requestedSet[name] = struct{}{}
	}
	migrateAll := boolArg(args, "all", len(requested) == 0)
	addSuites := uniqueTrimmedCSVStrings(stringSliceArg(args, "capability_suites"))
	results := []ToolBundleManifestMigration{}
	matched := map[string]struct{}{}
	for _, root := range installedToolBundleDiscoveryRoots(t.workdir) {
		entries, err := os.ReadDir(root.path)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			dir := filepath.Join(root.path, entry.Name())
			if !pathIsInside(t.workdir, dir) {
				continue
			}
			manifestPath := filepath.Join(dir, installedToolBundleManifestName)
			if !pathExists(manifestPath) {
				continue
			}
			raw, err := os.ReadFile(manifestPath)
			if err != nil {
				return nil, fmt.Errorf("read manifest %s: %w", manifestPath, err)
			}
			migration, updated, err := migrateToolBundleManifestBytes(raw, addSuites)
			migration.RootKind = root.kind
			migration.Dir = dir
			if migration.Name == "" {
				migration.Name = sanitizeManagedToolBundleName(entry.Name())
			}
			entryName := sanitizeManagedToolBundleName(entry.Name())
			matchesRequested := migrateAll
			for _, candidate := range []string{migration.Name, entryName} {
				if candidate == "" {
					continue
				}
				if _, ok := requestedSet[candidate]; ok {
					matchesRequested = true
					matched[candidate] = struct{}{}
				}
			}
			if !matchesRequested {
				continue
			}
			if err != nil {
				migration.Status = "error"
				migration.Message = err.Error()
				results = append(results, migration)
				continue
			}
			if migration.Changed {
				if err := os.WriteFile(manifestPath, updated, 0o644); err != nil {
					return nil, fmt.Errorf("write migrated manifest %s: %w", manifestPath, err)
				}
			}
			results = append(results, migration)
		}
	}
	if len(requestedSet) > 0 {
		missing := []string{}
		for _, name := range requested {
			if _, ok := matched[name]; !ok {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			return results, fmt.Errorf("no local tool bundle manifest matched %s", strings.Join(missing, ","))
		}
		successes := 0
		failures := []string{}
		for _, result := range results {
			if strings.TrimSpace(result.Status) == "error" {
				failures = append(failures, firstNonEmpty(result.Name, result.Dir))
				continue
			}
			successes++
		}
		if successes == 0 && len(failures) > 0 {
			return results, fmt.Errorf("selected tool bundle manifest migration failed for %s", strings.Join(uniqueTrimmedCSVStrings(failures), ","))
		}
	}
	if len(results) == 0 {
		return results, fmt.Errorf("no local tool bundle manifests found to migrate")
	}
	return results, nil
}

func migrateToolBundleManifestBytes(raw []byte, addSuites []string) (ToolBundleManifestMigration, []byte, error) {
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawMap); err != nil {
		return ToolBundleManifestMigration{Status: "error"}, nil, err
	}
	var manifest InstalledToolBundleManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return ToolBundleManifestMigration{Status: "error"}, nil, err
	}
	normalized, err := normalizeInstalledToolBundleManifest(manifest)
	if err != nil {
		return ToolBundleManifestMigration{Status: "error"}, nil, err
	}
	before := ""
	if schemaRaw, ok := rawMap["schema_version"]; ok {
		_ = json.Unmarshal(schemaRaw, &before)
	}
	normalized.SchemaVersion = "tool_bundle.v2"
	normalized.CapabilitySuites = uniqueTrimmedCSVStrings(normalized.CapabilitySuites, addSuites)
	setRaw := func(key string, value any) error {
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		rawMap[key] = encoded
		return nil
	}
	if err := setRaw("schema_version", normalized.SchemaVersion); err != nil {
		return ToolBundleManifestMigration{Status: "error", Name: normalized.Name}, nil, err
	}
	if err := setRaw("name", normalized.Name); err != nil {
		return ToolBundleManifestMigration{Status: "error", Name: normalized.Name}, nil, err
	}
	if err := setRaw("description", normalized.Description); err != nil {
		return ToolBundleManifestMigration{Status: "error", Name: normalized.Name}, nil, err
	}
	if err := setRaw("command", normalized.Command); err != nil {
		return ToolBundleManifestMigration{Status: "error", Name: normalized.Name}, nil, err
	}
	if _, hasParameters := rawMap["parameters"]; !hasParameters || len(manifest.Parameters) == 0 {
		if err := setRaw("parameters", normalized.Parameters); err != nil {
			return ToolBundleManifestMigration{Status: "error", Name: normalized.Name}, nil, err
		}
	}
	if len(addSuites) > 0 || len(normalized.CapabilitySuites) > 0 {
		if err := setRaw("capability_suites", normalized.CapabilitySuites); err != nil {
			return ToolBundleManifestMigration{Status: "error", Name: normalized.Name}, nil, err
		}
	}
	if _, hasProvenance := rawMap["provenance"]; !hasProvenance {
		normalized.Provenance = &InstalledToolBundleProvenance{
			Source:    "local_manifest_migration",
			Installed: time.Now().UTC().Format(time.RFC3339Nano),
		}
		if err := setRaw("provenance", normalized.Provenance); err != nil {
			return ToolBundleManifestMigration{Status: "error", Name: normalized.Name}, nil, err
		}
	}
	updated, err := json.MarshalIndent(rawMap, "", "  ")
	if err != nil {
		return ToolBundleManifestMigration{Status: "error", Name: normalized.Name}, nil, err
	}
	updated = append(updated, '\n')
	changed := !bytes.Equal(bytes.TrimSpace(raw), bytes.TrimSpace(updated))
	status := "up_to_date"
	message := "manifest already matched the v2 contract"
	if changed {
		status = "migrated"
		message = "manifest migrated to tool_bundle.v2"
	}
	return ToolBundleManifestMigration{
		Name:             normalized.Name,
		Status:           status,
		Changed:          changed,
		SchemaBefore:     strings.TrimSpace(before),
		SchemaAfter:      normalized.SchemaVersion,
		CapabilitySuites: uniqueTrimmedCSVStrings(normalized.CapabilitySuites),
		Message:          message,
	}, updated, nil
}

func toolBundleRegistryDownloadLimit(args map[string]any) int {
	limit := intArg(args["max_bytes"], toolBundleRegistryDefaultDownloadLimit)
	if limit <= 0 {
		return toolBundleRegistryDefaultDownloadLimit
	}
	if limit > toolBundleRegistryMaxDownloadLimit {
		return toolBundleRegistryMaxDownloadLimit
	}
	return limit
}

func toolBundleRegistryDownloadTimeout(args map[string]any) time.Duration {
	milliseconds := intArg(args["download_timeout_ms"], toolBundleRegistryDefaultDownloadMS)
	if milliseconds <= 0 {
		milliseconds = toolBundleRegistryDefaultDownloadMS
	}
	if milliseconds > toolBundleRegistryMaxDownloadMS {
		milliseconds = toolBundleRegistryMaxDownloadMS
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func selectToolBundleZipRoot(reader *zip.Reader, requestedNames []string) (string, InstalledToolBundleManifest, error) {
	manifests := map[string]InstalledToolBundleManifest{}
	for _, file := range reader.File {
		if file == nil || file.FileInfo().IsDir() {
			continue
		}
		clean, ok := cleanToolBundleZipEntryName(file.Name)
		if !ok {
			return "", InstalledToolBundleManifest{}, fmt.Errorf("zip archive contains unsafe entry %q", file.Name)
		}
		if path.Base(clean) != installedToolBundleManifestName {
			continue
		}
		manifest, err := readToolBundleZipManifest(file)
		if err != nil {
			return "", InstalledToolBundleManifest{}, fmt.Errorf("load zipped manifest %s: %w", clean, err)
		}
		root := path.Dir(clean)
		if root == "." {
			root = ""
		}
		manifests[root] = manifest
	}
	if len(manifests) == 0 {
		return "", InstalledToolBundleManifest{}, fmt.Errorf("zip archive does not contain %s", installedToolBundleManifestName)
	}
	if len(requestedNames) > 0 {
		want := sanitizeManagedToolBundleName(requestedNames[0])
		for root, manifest := range manifests {
			if want == manifest.Name || want == sanitizeManagedToolBundleName(path.Base(root)) {
				return root, manifest, nil
			}
		}
		return "", InstalledToolBundleManifest{}, fmt.Errorf("zip archive does not contain requested bundle %q", want)
	}
	if len(manifests) > 1 {
		names := []string{}
		for root, manifest := range manifests {
			label := manifest.Name
			if root != "" {
				label = fmt.Sprintf("%s at %s", manifest.Name, root)
			}
			names = append(names, label)
		}
		return "", InstalledToolBundleManifest{}, fmt.Errorf("zip archive contains multiple tool bundles (%s); pass name=<bundle>", strings.Join(uniqueTrimmedCSVStrings(names), ", "))
	}
	for root, manifest := range manifests {
		return root, manifest, nil
	}
	return "", InstalledToolBundleManifest{}, fmt.Errorf("zip archive does not contain %s", installedToolBundleManifestName)
}

func readToolBundleZipManifest(file *zip.File) (InstalledToolBundleManifest, error) {
	handle, err := file.Open()
	if err != nil {
		return InstalledToolBundleManifest{}, err
	}
	defer handle.Close()
	raw, err := io.ReadAll(io.LimitReader(handle, 1024*1024+1))
	if err != nil {
		return InstalledToolBundleManifest{}, err
	}
	if len(raw) > 1024*1024 {
		return InstalledToolBundleManifest{}, fmt.Errorf("%s exceeds 1MiB", installedToolBundleManifestName)
	}
	var manifest InstalledToolBundleManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return InstalledToolBundleManifest{}, err
	}
	return normalizeInstalledToolBundleManifest(manifest)
}

func extractToolBundleZipRoot(reader *zip.Reader, root, dest string, maxUncompressedBytes int64) error {
	if maxUncompressedBytes <= 0 {
		maxUncompressedBytes = toolBundleRegistryDefaultDownloadLimit
	}
	var extracted int64
	for _, file := range reader.File {
		if file == nil {
			continue
		}
		clean, ok := cleanToolBundleZipEntryName(file.Name)
		if !ok {
			return fmt.Errorf("zip archive contains unsafe entry %q", file.Name)
		}
		rel := clean
		if root != "" {
			if clean == root {
				continue
			}
			prefix := root + "/"
			if !strings.HasPrefix(clean, prefix) {
				continue
			}
			rel = strings.TrimPrefix(clean, prefix)
		}
		if rel == "" {
			continue
		}
		target := filepath.Join(dest, filepath.FromSlash(rel))
		if !pathIsInside(dest, target) {
			return fmt.Errorf("zip entry %q escapes extraction root", file.Name)
		}
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		mode := file.FileInfo().Mode()
		if mode&os.ModeSymlink != 0 {
			return fmt.Errorf("zip entry %q is a symlink", file.Name)
		}
		if !mode.IsRegular() {
			return fmt.Errorf("zip entry %q is not a regular file", file.Name)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		remaining := maxUncompressedBytes - extracted
		if remaining <= 0 {
			return fmt.Errorf("zip uncompressed content exceeds max_bytes=%d", maxUncompressedBytes)
		}
		handle, err := file.Open()
		if err != nil {
			return err
		}
		perm := mode.Perm()
		if perm == 0 {
			perm = 0o644
		}
		out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
		if err != nil {
			handle.Close()
			return err
		}
		limited := &io.LimitedReader{R: handle, N: remaining + 1}
		written, copyErr := io.Copy(out, limited)
		extracted += written
		closeErr := out.Close()
		handleErr := handle.Close()
		if copyErr != nil {
			return copyErr
		}
		if written > remaining || extracted > maxUncompressedBytes {
			return fmt.Errorf("zip uncompressed content exceeds max_bytes=%d", maxUncompressedBytes)
		}
		if closeErr != nil {
			return closeErr
		}
		if handleErr != nil {
			return handleErr
		}
	}
	return nil
}

func cleanToolBundleZipEntryName(name string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", false
	}
	name = strings.ReplaceAll(name, "\\", "/")
	if strings.HasPrefix(name, "/") {
		return "", false
	}
	parts := strings.Split(name, "/")
	for index, part := range parts {
		switch part {
		case "", ".":
			continue
		case "..":
			return "", false
		}
		if index == 0 && strings.Contains(part, ":") {
			return "", false
		}
	}
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || path.IsAbs(clean) {
		return "", false
	}
	return clean, true
}

func downloadedToolBundleProvenance(existing *InstalledToolBundleProvenance, sourceURL string) *InstalledToolBundleProvenance {
	provenance := &InstalledToolBundleProvenance{}
	if existing != nil {
		*provenance = *existing
	}
	provenance.Source = redactedToolBundleSourceURL(sourceURL)
	provenance.Installed = time.Now().UTC().Format(time.RFC3339Nano)
	return provenance
}

func redactedToolBundleSourceURL(sourceURL string) string {
	sourceURL = strings.TrimSpace(sourceURL)
	parsed, err := url.Parse(sourceURL)
	if err != nil || parsed.Scheme == "" {
		return sourceURL
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func writeInstalledToolBundleManifestFile(dir string, manifest InstalledToolBundleManifest) error {
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(filepath.Join(dir, installedToolBundleManifestName), raw, 0o644)
}

func (t *ToolBundleRegistryTool) scaffoldBundle(args map[string]any) (string, error) {
	names := toolBundleRegistryToolNames(args)
	if len(names) == 0 {
		return "", fmt.Errorf("name is required")
	}
	name := sanitizeManagedToolBundleName(names[0])
	if name == "" {
		return "", fmt.Errorf("scaffolded bundle name is empty after sanitization")
	}
	dest := filepath.Join(t.workdir, "tools", name)
	if !pathIsInside(t.workdir, dest) {
		return "", fmt.Errorf("tool bundle destination escapes workdir")
	}
	overwrite := boolArg(args, "overwrite", false)
	if directoryExists(dest) && !overwrite {
		return "", fmt.Errorf("tool bundle %q already exists; pass overwrite=true to replace its scaffold", name)
	}
	if overwrite {
		if err := os.RemoveAll(dest); err != nil {
			return "", err
		}
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return "", err
	}
	command := stringSliceArg(args, "command")
	runtimeName := strings.ToLower(strings.TrimSpace(stringArg(args, "runtime")))
	if runtimeName == "" {
		runtimeName = "node"
	}
	if len(command) == 0 {
		var script string
		var raw []byte
		switch runtimeName {
		case "node", "javascript":
			script = "tool.js"
			command = []string{"node", script}
			raw = []byte(toolBundleRegistryNodeTemplate(name))
		case "python", "py":
			script = "main.py"
			command = []string{"python", script}
			raw = []byte(toolBundleRegistryPythonTemplate(name))
		default:
			return "", fmt.Errorf("command is required when runtime=%q", runtimeName)
		}
		if err := os.WriteFile(filepath.Join(dest, script), raw, 0o755); err != nil {
			return "", err
		}
	}
	parameters := map[string]any{"type": "object", "additionalProperties": true}
	if rawParameters, ok := args["parameters"].(map[string]any); ok && len(rawParameters) > 0 {
		parameters = rawParameters
	}
	description := strings.TrimSpace(stringArg(args, "description"))
	if description == "" {
		description = "Self-written local tool bundle " + name
	}
	manifest := InstalledToolBundleManifest{
		SchemaVersion:    "tool_bundle.v2",
		Name:             name,
		Description:      description,
		Command:          command,
		Parameters:       parameters,
		CapabilitySuites: uniqueTrimmedCSVStrings(stringSliceArg(args, "capability_suites")),
		Provenance: &InstalledToolBundleProvenance{
			Source:    "agent_scaffold",
			Installed: time.Now().UTC().Format(time.RFC3339Nano),
		},
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", err
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(filepath.Join(dest, installedToolBundleManifestName), raw, 0o644); err != nil {
		return "", err
	}
	return name, nil
}

func toolBundleRegistryNodeTemplate(name string) string {
	escaped, _ := json.Marshal(name)
	return `const fs = require("fs");

let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => { input += chunk; });
process.stdin.on("end", () => {
  let args = {};
  try {
    args = input.trim() ? JSON.parse(input) : {};
  } catch (error) {
    console.log(JSON.stringify({ contract_version: "tool_bundle_result.v1", status: "fail", error: String(error && error.message || error) }));
    return;
  }
  console.log(JSON.stringify({
    contract_version: "tool_bundle_result.v1",
    status: "ok",
    tool: ` + string(escaped) + `,
    args
  }));
});
`
}

func toolBundleRegistryPythonTemplate(name string) string {
	escaped, _ := json.Marshal(name)
	return `import json
import sys

try:
    raw = sys.stdin.read().strip()
    args = json.loads(raw) if raw else {}
except Exception as exc:
    print(json.dumps({"contract_version": "tool_bundle_result.v1", "status": "fail", "error": str(exc)}))
    raise SystemExit(0)

print(json.dumps({
    "contract_version": "tool_bundle_result.v1",
    "status": "ok",
    "tool": ` + string(escaped) + `,
    "args": args,
}))
`
}

func resolveToolBundleRegistrySourcePath(workdir, source string) (string, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return "", fmt.Errorf("source_path is required")
	}
	if !filepath.IsAbs(source) {
		source = filepath.Join(workdir, source)
	}
	abs, err := filepath.Abs(source)
	if err != nil {
		return "", err
	}
	return abs, nil
}

func toolBundleRegistryToolNames(args map[string]any) []string {
	values := []string{}
	values = append(values, stringArg(args, "name"))
	values = append(values, stringArg(args, "bundle"))
	values = append(values, stringSliceArg(args, "names")...)
	return normalizeManagedToolBundleList(values)
}

func removeToolBundleRegistryNames(values, names []string) []string {
	if len(values) == 0 || len(names) == 0 {
		return normalizeManagedToolBundleList(values)
	}
	remove := map[string]struct{}{}
	for _, name := range normalizeManagedToolBundleList(names) {
		remove[name] = struct{}{}
	}
	out := []string{}
	for _, value := range normalizeManagedToolBundleList(values) {
		if _, ok := remove[value]; ok {
			continue
		}
		out = append(out, value)
	}
	return out
}

func installedToolBundleDiscoveryNames(diagnostics []ToolBundleDiscoveryDiagnostic) []string {
	names := []string{}
	for _, diagnostic := range diagnostics {
		if name := strings.TrimSpace(diagnostic.Name); name != "" {
			names = append(names, name)
		}
	}
	return normalizeManagedToolBundleList(names)
}

func sameFilesystemPath(left, right string) bool {
	leftAbs, leftErr := filepath.Abs(left)
	rightAbs, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	if goruntime.GOOS == "windows" {
		return strings.EqualFold(filepath.Clean(leftAbs), filepath.Clean(rightAbs))
	}
	return filepath.Clean(leftAbs) == filepath.Clean(rightAbs)
}

func toolBundleRegistryToolError(message string) *ToolResult {
	payload := map[string]any{
		"contract_version": "tool_bundle_registry_tool.v1",
		"status":           "error",
		"error":            strings.TrimSpace(message),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return &ToolResult{Output: message, IsError: true}
	}
	return &ToolResult{Output: string(raw), IsError: true}
}
