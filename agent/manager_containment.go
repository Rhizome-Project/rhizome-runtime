package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const managedAgentEnvFlag = "RHIZOME_AGENT_MANAGED"
const defaultManagedCodexCLIVersionPin = "codex-cli 0.139.0"

func managedAgentWorkdirRoot(registry BotRegistry) (string, error) {
	root := strings.TrimSpace(registry.Defaults.DefaultParentDir)
	if root == "" {
		root = defaultManagerCreateParentDir(registry, 0)
	}
	if root == "" {
		return "", fmt.Errorf("managed agent root is required")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve managed agent root: %w", err)
	}
	return filepath.Clean(rootAbs), nil
}

func validateManagedAgentFolderName(folderName string) (string, error) {
	folderName = strings.TrimSpace(folderName)
	if folderName == "" {
		return "", fmt.Errorf("folder_name is required")
	}
	if filepath.VolumeName(folderName) != "" {
		return "", fmt.Errorf("folder_name must be a single path component")
	}
	if strings.ContainsAny(folderName, `/\`) {
		return "", fmt.Errorf("folder_name must be a single path component")
	}
	clean := filepath.Clean(folderName)
	if clean == "." || clean == ".." || clean != folderName {
		return "", fmt.Errorf("folder_name must be a single path component")
	}
	return clean, nil
}

func validateManagedAgentPathWithinRoot(rootDir, candidate string, allowRoot bool) (string, error) {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return "", fmt.Errorf("managed agent root is required")
	}
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return "", fmt.Errorf("workdir cannot be empty")
	}

	rootAbs, err := filepath.Abs(rootDir)
	if err != nil {
		return "", fmt.Errorf("resolve managed agent root: %w", err)
	}
	candidateAbs, err := filepath.Abs(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve workdir: %w", err)
	}
	rootAbs = filepath.Clean(rootAbs)
	candidateAbs = filepath.Clean(candidateAbs)

	rel, err := filepath.Rel(rootAbs, candidateAbs)
	if err != nil {
		return "", fmt.Errorf("compare workdir against managed root: %w", err)
	}
	if rel == "." || rel == "" {
		if allowRoot {
			return candidateAbs, nil
		}
		return "", fmt.Errorf("workdir must be inside managed root: %s", rootAbs)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("workdir must stay inside managed root: %s", rootAbs)
	}
	return candidateAbs, nil
}

func validateManagedAgentWorkdirWithinRoot(rootDir, candidate string) (string, error) {
	return validateManagedAgentPathWithinRoot(rootDir, candidate, false)
}

func validateManagedAgentWorkdirTarget(candidate string) (string, error) {
	registry := LoadBotRegistry()
	root, err := managedAgentWorkdirRoot(registry)
	if err != nil {
		return "", err
	}
	return validateManagedAgentWorkdirWithinRoot(root, candidate)
}

func buildManagedAgentProcessEnv(record ManagedAgentRecord) ([]string, error) {
	record = normalizeManagedAgentRecord(record)
	if record.Workdir == "" {
		return nil, fmt.Errorf("managed agent requires workdir")
	}

	startCfg := managedAgentStartRuntimeConfig(record)
	allowLocalShell := managedRecordAllowsLocalShell(record) || runtimeTrustFirst(startCfg)
	allowLocalMutation := managedRecordAllowsLocalMutation(record) || runtimeTrustFirst(startCfg)
	useManagedCodexHome := !allowLocalShell || managedAgentUsesManagedCodexHome(record)
	tmpDir := filepath.Join(record.Workdir, ".tmp")
	cacheDir := filepath.Join(record.Workdir, ".cache")
	stateDir := filepath.Join(record.Workdir, ".state")
	configDir := filepath.Join(record.Workdir, ".local-config")
	dataDir := filepath.Join(record.Workdir, ".local-data")
	appDataDir := filepath.Join(dataDir, "AppData", "Roaming")
	localAppDataDir := filepath.Join(dataDir, "LocalAppData")
	homeDir := managedAgentHomeRootPath(record.Workdir)
	runtimeConfigRoot := managedAgentConfigRootPath(record.Workdir)
	identityLeaseRoot := managedAgentIdentityLeaseRootPath(record.Workdir)
	goCacheDir := filepath.Join(cacheDir, "go-build")
	// TA-01 (static-audit 2026-06-02): the Codex provider repoints HOME to a fresh empty managed
	// home, so Go would derive GOMODCACHE/GOPATH from the empty HOME (cold, possibly unwritable),
	// re-downloading the whole module graph on every build and hard-failing in any restricted
	// child. Give them explicit per-agent writable locations (parent env can override below).
	goModCacheDir := filepath.Join(cacheDir, "go-mod")
	goPathDir := filepath.Join(cacheDir, "go-path")
	partnerCodexHome := ""
	if useManagedCodexHome {
		partnerCodexHome = managedAgentCodexHomePath(record.Workdir)
	}
	for _, dir := range []string{tmpDir, cacheDir, stateDir, configDir, dataDir, appDataDir, localAppDataDir, homeDir, runtimeConfigRoot, identityLeaseRoot, goCacheDir, goModCacheDir, goPathDir, partnerCodexHome} {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("prepare managed runtime directory %s: %w", dir, err)
		}
	}
	if err := materializeManagedAgentProviderRegistry(runtimeConfigRoot); err != nil {
		return nil, fmt.Errorf("materialize managed provider registry: %w", err)
	}
	if useManagedCodexHome {
		if err := materializeManagedAgentCodexAuth(partnerCodexHome); err != nil {
			return nil, fmt.Errorf("materialize managed codex auth: %w", err)
		}
	}

	env := map[string]string{
		managedAgentEnvFlag:               "1",
		managedAgentConfigRootFlag:        runtimeConfigRoot,
		managedAgentIdentityLeaseRootFlag: identityLeaseRoot,
		"TMP":                             tmpDir,
		"TEMP":                            tmpDir,
		"TMPDIR":                          tmpDir,
		"XDG_CACHE_HOME":                  cacheDir,
		"XDG_STATE_HOME":                  stateDir,
		"XDG_CONFIG_HOME":                 configDir,
		"XDG_DATA_HOME":                   dataDir,
		"APPDATA":                         appDataDir,
		"LOCALAPPDATA":                    localAppDataDir,
		"LocalAppData":                    localAppDataDir,
		"GOCACHE":                         goCacheDir,
		"GOMODCACHE":                      goModCacheDir,
		"GOPATH":                          goPathDir,
	}
	trustedOwners := trustedManagedShellOwnerUserIDs()
	if trustedOwner := strings.TrimSpace(trustedManagedShellOwnerUserID()); trustedOwner != "" {
		env["RHIZOME_OWNER_USER_ID"] = trustedOwner
	}
	if len(trustedOwners) > 0 {
		env[managedAgentTrustedOwnerUserIDsFlag] = strings.Join(trustedOwners, ",")
	}
	if allowLocalShell {
		env[managedAgentAllowLocalShellFlag] = "1"
	}
	if allowLocalMutation {
		env[managedAgentAllowLocalMutationFlag] = "1"
	}
	if runtimeTrustFirst(startCfg) {
		env[codexExecSandboxEnvFlag] = codexExecSandboxDanger
		env["RHIZOME_COORDINATION_MODE"] = CoordinationModeTrustFirst
	}
	if pin := strings.TrimSpace(os.Getenv(codexExecVersionPinEnvFlag)); pin != "" {
		env[codexExecVersionPinEnvFlag] = pin
	} else {
		env[codexExecVersionPinEnvFlag] = defaultManagedCodexCLIVersionPin
	}
	if useManagedCodexHome && partnerCodexHome != "" {
		env["HOME"] = homeDir
		env["USERPROFILE"] = homeDir
		if volume := filepath.VolumeName(homeDir); volume != "" {
			env["HOMEDRIVE"] = volume
			homePath := strings.TrimPrefix(homeDir, volume)
			if homePath == "" {
				homePath = string(filepath.Separator)
			}
			env["HOMEPATH"] = homePath
		}
		env[managedAgentCodexHomeFlag] = partnerCodexHome
		env["CODEX_HOME"] = partnerCodexHome
	}

	copyManagedAgentEnv(env,
		"PATH",
		"SYSTEMROOT",
		"SystemRoot",
		"WINDIR",
		"COMSPEC",
		"ComSpec",
		"PATHEXT",
		"OS",
		"SHELL",
		"LANG",
		"LC_ALL",
		"LC_CTYPE",
		"TERM",
	)
	if allowLocalShell {
		addKnownBrowserDirsToEnvMap(env)
	}
	if allowLocalShell {
		keys := []string{
			"OPENAI_API_KEY",
			"OPENAI_BASE_URL",
			"OPENAI_MODEL",
			"OPENROUTER_API_KEY",
			"RHIZOME_AGENT_API_KEY",
			"BAILIAN_API_KEY",
			"BAILIAN_TOKEN_PLAN_API_KEY",
			"BAILIAN_CODING_PLAN_API_KEY",
			"DASHSCOPE_API_KEY",
			"QWEN_API_KEY",
		}
		if !useManagedCodexHome {
			keys = append(keys,
				"HOME",
				"USERPROFILE",
				"HOMEDRIVE",
				"HOMEPATH",
				"CODEX_CLI_PATH",
				"CUSTOM_CLI_PATH",
				"QWEN_CLI_PATH",
			)
		}
		keys = append(keys,
			"HTTP_PROXY",
			"HTTPS_PROXY",
			"NO_PROXY",
			"ALL_PROXY",
			"SSL_CERT_FILE",
			"SSL_CERT_DIR",
			// TA-01: propagate the host Go module-fetch CONFIG (not the cache/path, which stay
			// per-agent and deterministic) so GOPROXY/GOFLAGS/toolchain settings survive into a
			// restricted child instead of silently failing the first module download.
			"GOPROXY",
			"GOFLAGS",
			"GOPRIVATE",
			"GONOSUMCHECK",
			"GOSUMDB",
			"GONOSUMDB",
			"GOTOOLCHAIN",
		)
		copyManagedAgentEnv(env, keys...)
		copyManagedAgentEnvByPrefix(env, "RHIZOME_AGENT_PROVIDER_")
		if codexExecutable := strings.TrimSpace(sharedCodexExecutablePath()); codexExecutable != "" {
			if strings.TrimSpace(env["CODEX_CLI_PATH"]) == "" {
				env["CODEX_CLI_PATH"] = codexExecutable
			}
			if strings.TrimSpace(env["CUSTOM_CLI_PATH"]) == "" {
				env["CUSTOM_CLI_PATH"] = codexExecutable
			}
		}
		if qwenExecutable := strings.TrimSpace(sharedQwenExecutablePath()); qwenExecutable != "" && strings.TrimSpace(env["QWEN_CLI_PATH"]) == "" {
			env["QWEN_CLI_PATH"] = qwenExecutable
		}
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+env[key])
	}
	return result, nil
}

func materializeManagedAgentProviderRegistry(runtimeConfigRoot string) error {
	runtimeConfigRoot = strings.TrimSpace(runtimeConfigRoot)
	if runtimeConfigRoot == "" {
		return fmt.Errorf("managed runtime config root is required")
	}
	registry := LoadProviderRegistry()
	raw, _, err := marshalProviderRegistryForWrite(registry)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(runtimeConfigRoot, 0o755); err != nil {
		return err
	}
	return atomicWriteFile(filepath.Join(runtimeConfigRoot, providerRegistryFilename), raw, 0o600)
}

func managedAgentUsesManagedCodexHome(record ManagedAgentRecord) bool {
	record = normalizeManagedAgentRecord(record)
	if record.ProviderID == "" {
		return false
	}
	provider, ok := FindProviderRecord(record.ProviderID)
	if !ok {
		return false
	}
	return providerUsesManagedCodexHome(provider)
}

func materializeManagedAgentCodexAuth(targetHome string) error {
	targetHome = cleanManagedRuntimeRoot(targetHome)
	if targetHome == "" {
		return nil
	}
	sourceAuth := sharedManagedAgentCodexAuthPath(targetHome)
	if sourceAuth == "" {
		return nil
	}
	data, err := os.ReadFile(sourceAuth)
	if err != nil {
		return fmt.Errorf("read shared auth: %w", err)
	}
	if err := os.MkdirAll(targetHome, 0o755); err != nil {
		return err
	}
	targetAuth := codexAuthPathForHome(targetHome)
	current, err := os.ReadFile(targetAuth)
	if err == nil && string(current) == string(data) {
		return nil
	}
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read managed auth: %w", err)
	}
	return atomicWriteFile(targetAuth, data, 0o600)
}

func sharedManagedAgentCodexAuthPath(targetHome string) string {
	targetHome = cleanManagedRuntimeRoot(targetHome)
	candidates := []string{}
	if env := cleanManagedRuntimeRoot(os.Getenv("CODEX_HOME")); env != "" {
		candidates = append(candidates, env)
	}
	if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
		candidates = append(candidates, filepath.Join(home, codexConfigDir))
	}

	for _, candidate := range candidates {
		candidate = cleanManagedRuntimeRoot(candidate)
		if candidate == "" || strings.EqualFold(candidate, targetHome) {
			continue
		}
		if hasChatGPTCodexSessionInHome(candidate) {
			return codexAuthPathForHome(candidate)
		}
	}
	return ""
}

func copyManagedAgentEnv(dst map[string]string, keys ...string) {
	if dst == nil {
		return
	}
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
			dst[key] = value
		}
	}
}

func copyManagedAgentEnvByPrefix(dst map[string]string, prefix string) {
	if dst == nil {
		return
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return
	}
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || strings.TrimSpace(value) == "" {
			continue
		}
		if strings.HasPrefix(key, prefix) {
			dst[key] = value
		}
	}
}
