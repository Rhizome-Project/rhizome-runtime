package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const botRegistryFilename = "bot_registry.json"

type BotManagerDefaults struct {
	ProtocolVersion   string   `json:"protocol_version,omitempty"`
	HostURL           string   `json:"host_url,omitempty"`
	WorkspaceID       string   `json:"workspace_id,omitempty"`
	WorkspacePassword string   `json:"workspace_password,omitempty"`
	OwnerUserID       string   `json:"owner_user_id,omitempty"`
	DefaultProviderID string   `json:"default_provider_id,omitempty"`
	ModelOverride     string   `json:"model_override,omitempty"`
	GroupID           string   `json:"group_id,omitempty"`
	LLMBackend        string   `json:"llm_backend,omitempty"`
	Model             string   `json:"model,omitempty"`
	CoordinationMode  string   `json:"coordination_mode,omitempty"`
	Role              string   `json:"role,omitempty"`
	AnatomyPreset     string   `json:"anatomy_preset,omitempty"`
	AnatomyPath       string   `json:"anatomy_path,omitempty"`
	AnatomyDigest     string   `json:"anatomy_digest,omitempty"`
	ToolBundles       []string `json:"tool_bundles,omitempty"`
	Capabilities      []string `json:"capabilities,omitempty"`
	DefaultParentDir  string   `json:"default_parent_dir,omitempty"`
	UpdatedAt         string   `json:"updated_at,omitempty"`
}

type ManagedAgentRecord struct {
	AgentID          string   `json:"agent_id,omitempty"`
	DisplayName      string   `json:"display_name,omitempty"`
	Workdir          string   `json:"workdir,omitempty"`
	HostURL          string   `json:"host_url,omitempty"`
	WorkspaceID      string   `json:"workspace_id,omitempty"`
	OwnerUserID      string   `json:"owner_user_id,omitempty"`
	ProviderID       string   `json:"provider_id,omitempty"`
	ModelOverride    string   `json:"model_override,omitempty"`
	GroupID          string   `json:"group_id,omitempty"`
	Role             string   `json:"role,omitempty"`
	LLMBackend       string   `json:"llm_backend,omitempty"`
	Model            string   `json:"model,omitempty"`
	CoordinationMode string   `json:"coordination_mode,omitempty"`
	AnatomyPreset    string   `json:"anatomy_preset,omitempty"`
	AnatomyPath      string   `json:"anatomy_path,omitempty"`
	AnatomyDigest    string   `json:"anatomy_digest,omitempty"`
	ToolBundles      []string `json:"tool_bundles,omitempty"`
	AddedAt          string   `json:"added_at,omitempty"`
	UpdatedAt        string   `json:"updated_at,omitempty"`
}

type BotRegistry struct {
	Defaults  BotManagerDefaults   `json:"defaults,omitempty"`
	Agents    []ManagedAgentRecord `json:"agents,omitempty"`
	UpdatedAt string               `json:"updated_at,omitempty"`
}

func botRegistryPath() string {
	return agentRuntimeConfigPath(botRegistryFilename)
}

func LoadBotRegistry() BotRegistry {
	data, err := os.ReadFile(botRegistryPath())
	if err != nil {
		return normalizeBotRegistry(BotRegistry{})
	}
	var registry BotRegistry
	if err := json.Unmarshal(data, &registry); err != nil {
		return normalizeBotRegistry(BotRegistry{})
	}
	return normalizeBotRegistry(registry)
}

func SaveBotRegistry(registry BotRegistry) error {
	root := agentRuntimeConfigRoot()
	path := botRegistryPath()
	if root == "" || path == "" {
		return fmt.Errorf("agent config root is unavailable")
	}
	return withManagerStateLock(root, true, func() error {
		return saveBotRegistryLocked(root, path, registry)
	})
}

func validateBotRegistry(registry BotRegistry) error {
	seenWorkdirs := make(map[string]string, len(registry.Agents))
	for _, record := range registry.Agents {
		record = normalizeManagedAgentRecord(record)
		workdirKey := canonicalManagedAgentWorkdirKey(record.Workdir)
		if workdirKey == "" {
			continue
		}
		if existingAgentID, ok := seenWorkdirs[workdirKey]; ok && existingAgentID != record.AgentID {
			return fmt.Errorf("managed agents %q and %q cannot share workdir %q", existingAgentID, record.AgentID, record.Workdir)
		}
		seenWorkdirs[workdirKey] = record.AgentID
	}
	return nil
}

func normalizeBotRegistry(registry BotRegistry) BotRegistry {
	global := LoadRhizomeProfile()
	registry.Defaults = normalizeBotManagerDefaults(registry.Defaults, global)

	seen := map[string]struct{}{}
	normalized := make([]ManagedAgentRecord, 0, len(registry.Agents))
	for _, record := range registry.Agents {
		record = normalizeManagedAgentRecord(record)
		if strings.TrimSpace(record.AgentID) == "" || strings.TrimSpace(record.Workdir) == "" {
			continue
		}
		if _, ok := seen[record.AgentID]; ok {
			continue
		}
		seen[record.AgentID] = struct{}{}
		normalized = append(normalized, record)
	}
	sort.SliceStable(normalized, func(i, j int) bool {
		return normalized[i].AgentID < normalized[j].AgentID
	})
	registry.Agents = normalized
	return registry
}

func normalizeBotManagerDefaults(defaults BotManagerDefaults, global RhizomeConnectionProfile) BotManagerDefaults {
	global = sanitizeGlobalDefaultsProfile(global)
	defaults.DefaultProviderID = strings.TrimSpace(defaults.DefaultProviderID)
	defaults.ModelOverride = strings.TrimSpace(defaults.ModelOverride)
	defaults.ProtocolVersion = firstNonEmpty(defaults.ProtocolVersion, defaultProtocolVersion)
	defaults.HostURL = firstNonEmpty(defaults.HostURL, global.HostURL, hostURLForRPC(global.RPCEndpoint), defaultRhizomeHostURL)
	defaults.WorkspaceID = firstNonEmpty(defaults.WorkspaceID, global.WorkspaceID, defaultWorkspaceID)
	defaults.WorkspacePassword = firstNonEmpty(defaults.WorkspacePassword, global.WorkspacePassword)
	defaults.OwnerUserID = firstNonEmpty(global.OwnerUserID, defaults.OwnerUserID)
	defaults.GroupID, defaults.LLMBackend, defaults.Model = applyProviderBinding(defaults.DefaultProviderID, defaults.ModelOverride, defaults.GroupID, defaults.LLMBackend, defaults.Model)
	defaults.LLMBackend = normalizeLLMBackend(firstNonEmpty(defaults.LLMBackend, defaultOnboardLLMBackend()))
	if defaults.LLMBackend == "" {
		defaults.LLMBackend = llmBackendAuto
	}
	defaults.Model = firstNonEmpty(defaults.Model, defaultModel)
	defaults.CoordinationMode = normalizeCoordinationModeOptional(defaults.CoordinationMode)
	defaults.Role = firstNonEmpty(defaults.Role, "generalist")
	defaults.AnatomyPreset = normalizeAgentAnatomyPreset(defaults.AnatomyPreset)
	defaults.AnatomyPath = cleanOptionalManagedPath(defaults.AnatomyPath)
	defaults.AnatomyDigest = strings.TrimSpace(defaults.AnatomyDigest)
	defaults.ToolBundles = normalizeManagedToolBundleList(defaults.ToolBundles)
	defaults.Capabilities = normalizeCapabilityList(defaults.Capabilities)
	return defaults
}

func sanitizeGlobalDefaultsProfile(global RhizomeConnectionProfile) RhizomeConnectionProfile {
	host := firstNonEmpty(global.HostURL, hostURLForRPC(global.RPCEndpoint))
	if !looksLikeLocalDevelopmentHost(host) {
		return global
	}
	return RhizomeConnectionProfile{}
}

func looksLikeLocalDevelopmentHost(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return strings.Contains(strings.ToLower(raw), "localhost") || strings.Contains(raw, "127.0.0.1") || strings.Contains(raw, "[::1]")
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

func normalizeManagedAgentRecord(record ManagedAgentRecord) ManagedAgentRecord {
	record.AgentID = strings.TrimSpace(record.AgentID)
	record.DisplayName = firstNonEmpty(record.DisplayName, record.AgentID)
	record.Workdir = strings.TrimSpace(record.Workdir)
	if record.Workdir != "" {
		if abs, err := filepath.Abs(record.Workdir); err == nil {
			record.Workdir = abs
		}
	}
	record.HostURL = strings.TrimSpace(record.HostURL)
	record.WorkspaceID = strings.TrimSpace(record.WorkspaceID)
	record.OwnerUserID = strings.TrimSpace(record.OwnerUserID)
	record.ProviderID = strings.TrimSpace(record.ProviderID)
	record.ModelOverride = strings.TrimSpace(record.ModelOverride)
	record.GroupID, record.LLMBackend, record.Model = applyProviderBinding(record.ProviderID, record.ModelOverride, record.GroupID, record.LLMBackend, record.Model)
	record.Role = firstNonEmpty(record.Role, "generalist")
	record.LLMBackend = normalizeLLMBackend(record.LLMBackend)
	if record.LLMBackend == "" {
		record.LLMBackend = llmBackendAuto
	}
	record.Model = firstNonEmpty(record.Model, defaultModel)
	record.CoordinationMode = normalizeCoordinationModeOptional(record.CoordinationMode)
	record.AnatomyPreset = normalizeAgentAnatomyPreset(record.AnatomyPreset)
	record.AnatomyPath = cleanOptionalManagedPath(record.AnatomyPath)
	record.AnatomyDigest = strings.TrimSpace(record.AnatomyDigest)
	record.ToolBundles = normalizeManagedToolBundleList(record.ToolBundles)
	return record
}

func cleanOptionalManagedPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	return filepath.Clean(path)
}

func canonicalManagedAgentWorkdirKey(workdir string) string {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return ""
	}
	if abs, err := filepath.Abs(workdir); err == nil {
		workdir = abs
	}
	workdir = filepath.Clean(workdir)
	if realPath, err := filepath.EvalSymlinks(workdir); err == nil && strings.TrimSpace(realPath) != "" {
		workdir = filepath.Clean(realPath)
	}
	if runtime.GOOS == "windows" {
		workdir = strings.ToLower(workdir)
	}
	return workdir
}

func UpsertManagedAgent(record ManagedAgentRecord) error {
	record = normalizeManagedAgentRecord(record)
	if strings.TrimSpace(record.AgentID) == "" || strings.TrimSpace(record.Workdir) == "" {
		return fmt.Errorf("managed agent requires agent_id and workdir")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	record.UpdatedAt = now
	if strings.TrimSpace(record.AddedAt) == "" {
		record.AddedAt = now
	}

	return updateBotRegistry(func(registry *BotRegistry) error {
		upsertManagedAgentInRegistry(registry, record)
		return nil
	})
}

func RemoveManagedAgent(agentID string) error {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return fmt.Errorf("agent id is required")
	}
	return updateBotRegistry(func(registry *BotRegistry) error {
		filtered := make([]ManagedAgentRecord, 0, len(registry.Agents))
		for _, record := range registry.Agents {
			if record.AgentID == agentID {
				continue
			}
			filtered = append(filtered, record)
		}
		registry.Agents = filtered
		return nil
	})
}

func ResolveManagedAgentReference(ref string) (ManagedAgentRecord, error) {
	registry := LoadBotRegistry()
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ManagedAgentRecord{}, fmt.Errorf("agent reference is required")
	}
	if idx, err := strconv.Atoi(ref); err == nil {
		if idx < 1 || idx > len(registry.Agents) {
			return ManagedAgentRecord{}, fmt.Errorf("agent index %d is out of range", idx)
		}
		return normalizeManagedAgentRecord(registry.Agents[idx-1]), nil
	}
	for _, record := range registry.Agents {
		if record.AgentID == ref {
			return normalizeManagedAgentRecord(record), nil
		}
	}
	for _, record := range registry.Agents {
		if strings.EqualFold(record.DisplayName, ref) {
			return normalizeManagedAgentRecord(record), nil
		}
	}
	for _, record := range registry.Agents {
		if strings.EqualFold(filepath.Base(record.Workdir), ref) {
			return normalizeManagedAgentRecord(record), nil
		}
	}
	return ManagedAgentRecord{}, fmt.Errorf("unknown agent %q", ref)
}

func FindManagedAgent(ref string) (ManagedAgentRecord, bool) {
	record, err := ResolveManagedAgentReference(ref)
	return record, err == nil
}

func managedAgentRecordFromConfig(cfg RuntimeConfig) ManagedAgentRecord {
	cfg.ApplyDefaults()
	return normalizeManagedAgentRecord(ManagedAgentRecord{
		AgentID:          cfg.AgentID,
		DisplayName:      cfg.DisplayName,
		Workdir:          cfg.Workdir,
		HostURL:          cfg.RhizomeHost,
		WorkspaceID:      cfg.WorkspaceID,
		OwnerUserID:      cfg.OwnerUserID,
		ProviderID:       cfg.ProviderID,
		ModelOverride:    cfg.ModelOverride,
		GroupID:          cfg.GroupID,
		Role:             cfg.Role,
		LLMBackend:       cfg.LLMBackend,
		Model:            cfg.Model,
		CoordinationMode: cfg.CoordinationMode,
	})
}

func SetManagerDefault(field, value string) error {
	field = strings.ToLower(strings.TrimSpace(field))
	value = strings.TrimSpace(value)

	return updateBotRegistry(func(registry *BotRegistry) error {
		switch field {
		case "host", "host_url", "url":
			registry.Defaults.HostURL = value
		case "workspace", "workspace_id":
			registry.Defaults.WorkspaceID = value
		case "workspace_password", "password":
			return fmt.Errorf("workspace passwords cannot be set on the command line; use onboarding or RHIZOME_WORKSPACE_PASSWORD")
		case "owner", "owner_user_id":
			registry.Defaults.OwnerUserID = value
		case "default_provider", "default_provider_id", "provider":
			if err := validateProviderReference(value); err != nil {
				return err
			}
			registry.Defaults.DefaultProviderID = value
			registry.Defaults.GroupID = ""
		case "group", "group_id":
			registry.Defaults.GroupID = value
		case "llm_backend", "backend":
			registry.Defaults.LLMBackend = value
		case "model_override":
			registry.Defaults.ModelOverride = value
		case "model":
			registry.Defaults.Model = value
		case "coordination_mode", "coordination", "mode":
			if value == "" {
				registry.Defaults.CoordinationMode = ""
				return nil
			}
			if err := validateCoordinationMode(value); err != nil {
				return err
			}
			registry.Defaults.CoordinationMode = normalizeCoordinationMode(value)
		case "role":
			registry.Defaults.Role = value
		case "anatomy_preset", "anatomy", "runtime_anatomy":
			registry.Defaults.AnatomyPreset = normalizeAgentAnatomyPreset(value)
		case "anatomy_path":
			if value != "" && !filepath.IsAbs(value) {
				return fmt.Errorf("anatomy_path must be absolute")
			}
			registry.Defaults.AnatomyPath = value
		case "anatomy_digest":
			registry.Defaults.AnatomyDigest = value
		case "tool_bundles", "tool_bundle", "bundles":
			registry.Defaults.ToolBundles = parseManagedToolBundlesCSV(value)
		case "protocol", "protocol_version":
			registry.Defaults.ProtocolVersion = value
		case "capabilities":
			registry.Defaults.Capabilities = parseCapabilitiesCSV(value)
		case "default_parent_dir", "parent_dir":
			registry.Defaults.DefaultParentDir = value
		default:
			return fmt.Errorf("unknown default field %q", field)
		}
		return nil
	})
}

func ClearManagerDefault(field string) error {
	return SetManagerDefault(field, "")
}

func updateBotRegistry(mutator func(*BotRegistry) error) error {
	root := agentRuntimeConfigRoot()
	path := botRegistryPath()
	if root == "" || path == "" {
		return fmt.Errorf("agent config root is unavailable")
	}
	return withManagerStateLock(root, true, func() error {
		registry, err := loadBotRegistryFromDisk(path)
		if err != nil {
			return err
		}
		registry = normalizeBotRegistry(registry)
		if err := mutator(&registry); err != nil {
			return err
		}
		return saveBotRegistryLocked(root, path, registry)
	})
}

func loadBotRegistryFromDisk(path string) (BotRegistry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return BotRegistry{}, nil
		}
		return BotRegistry{}, err
	}
	var registry BotRegistry
	if err := json.Unmarshal(data, &registry); err != nil {
		if archiveErr := archiveBrokenManagerStateJournalPath(path, "corrupt bot registry"); archiveErr != nil {
			return BotRegistry{}, archiveErr
		}
		return BotRegistry{}, fmt.Errorf("corrupt bot registry %q was archived: %w", path, err)
	}
	return registry, nil
}

func saveBotRegistryLocked(root, path string, registry BotRegistry) error {
	raw, _, err := marshalBotRegistryForWrite(registry)
	if err != nil {
		return err
	}
	return writeManagerStateBytesLocked(root, path, "save_bot_registry", raw, 0o600)
}

func upsertManagedAgentInRegistry(registry *BotRegistry, record ManagedAgentRecord) {
	if registry == nil {
		return
	}
	record = normalizeManagedAgentRecord(record)
	replaced := false
	for i, existing := range registry.Agents {
		if existing.AgentID != record.AgentID {
			continue
		}
		record.AddedAt = firstNonEmpty(existing.AddedAt, record.AddedAt)
		registry.Agents[i] = record
		replaced = true
		break
	}
	if !replaced {
		registry.Agents = append(registry.Agents, record)
	}
}

func botRegistryContainsManagedWorkdir(registry BotRegistry, workdir string) bool {
	want := canonicalManagedAgentWorkdirKey(workdir)
	if want == "" {
		return false
	}
	for _, record := range registry.Agents {
		if canonicalManagedAgentWorkdirKey(record.Workdir) == want {
			return true
		}
	}
	return false
}

func marshalBotRegistryForWrite(registry BotRegistry) ([]byte, BotRegistry, error) {
	registry = normalizeBotRegistry(registry)
	if err := validateBotRegistry(registry); err != nil {
		return nil, BotRegistry{}, err
	}
	registry.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	registry.Defaults.UpdatedAt = registry.UpdatedAt
	raw, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		return nil, BotRegistry{}, err
	}
	return raw, registry, nil
}
