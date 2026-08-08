package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

const providerRegistryFilename = "provider_registry.json"

const (
	providerChannelAPI    = "api"
	providerChannelBridge = "bridge"
)

var (
	errProviderNotFound = errors.New("provider not found")
	errProviderDisabled = errors.New("provider disabled")
)

type ProviderRegistry struct {
	Providers []ProviderRecord `json:"providers,omitempty"`
	UpdatedAt string           `json:"updated_at,omitempty"`
}

type ProviderRecord struct {
	ProviderID   string               `json:"provider_id,omitempty"`
	Title        string               `json:"title,omitempty"`
	ChannelType  string               `json:"channel_type,omitempty"`
	Driver       string               `json:"driver,omitempty"`
	GroupID      string               `json:"group_id,omitempty"`
	DefaultModel string               `json:"default_model,omitempty"`
	Models       []string             `json:"models,omitempty"`
	Enabled      bool                 `json:"enabled"`
	System       bool                 `json:"system,omitempty"`
	API          ProviderAPIConfig    `json:"api,omitempty"`
	Bridge       ProviderBridgeConfig `json:"bridge,omitempty"`
	CreatedAt    string               `json:"created_at,omitempty"`
	UpdatedAt    string               `json:"updated_at,omitempty"`
}

type ProviderAPIConfig struct {
	BaseURL       string            `json:"base_url,omitempty"`
	PublicHeaders map[string]string `json:"public_headers,omitempty"`
}

type ProviderBridgeConfig struct {
	Executable     string `json:"executable,omitempty"`
	Command        string `json:"command,omitempty"`
	UseManagedHome bool   `json:"use_managed_home"`
}

func providerRegistryPath() string {
	return agentRuntimeConfigPath(providerRegistryFilename)
}

func LoadProviderRegistry() ProviderRegistry {
	registry, err := LoadProviderRegistryWithError()
	if err != nil {
		return normalizeProviderRegistry(ProviderRegistry{})
	}
	return registry
}

func LoadProviderRegistryWithError() (ProviderRegistry, error) {
	registry, err := loadProviderRegistryFromDisk(providerRegistryPath())
	if err != nil {
		return ProviderRegistry{}, err
	}
	return normalizeProviderRegistry(registry), nil
}

func SaveProviderRegistry(registry ProviderRegistry) error {
	root := agentRuntimeConfigRoot()
	path := providerRegistryPath()
	if root == "" || path == "" {
		return fmt.Errorf("agent config root is unavailable")
	}
	return withManagerStateLock(root, true, func() error {
		return saveProviderRegistryLocked(root, path, registry)
	})
}

func UpsertProviderRecord(record ProviderRecord) error {
	record = normalizeProviderRecord(record)
	if strings.TrimSpace(record.ProviderID) == "" {
		return fmt.Errorf("provider requires provider_id")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	record.UpdatedAt = now
	if strings.TrimSpace(record.CreatedAt) == "" {
		record.CreatedAt = now
	}
	return updateProviderRegistry(func(registry *ProviderRegistry) error {
		replaced := false
		for i, existing := range registry.Providers {
			if existing.ProviderID != record.ProviderID {
				continue
			}
			record.CreatedAt = firstNonEmpty(existing.CreatedAt, record.CreatedAt)
			registry.Providers[i] = record
			replaced = true
			break
		}
		if !replaced {
			registry.Providers = append(registry.Providers, record)
		}
		return nil
	})
}

func RemoveProviderRecord(providerID string) error {
	providerID = strings.TrimSpace(providerID)
	if providerID == "" {
		return fmt.Errorf("provider id is required")
	}
	return updateProviderRegistry(func(registry *ProviderRegistry) error {
		labels, err := providerReferenceLabels(providerID)
		if err != nil {
			return err
		}
		if len(labels) > 0 {
			return fmt.Errorf("provider %q is still referenced by %s", providerID, strings.Join(labels, ", "))
		}
		filtered := make([]ProviderRecord, 0, len(registry.Providers))
		for _, record := range registry.Providers {
			if record.ProviderID == providerID {
				continue
			}
			filtered = append(filtered, record)
		}
		registry.Providers = filtered
		return nil
	})
}

func FindProviderRecord(providerID string) (ProviderRecord, bool) {
	providerID = strings.TrimSpace(providerID)
	if providerID == "" {
		return ProviderRecord{}, false
	}
	return findProviderRecordInRegistry(LoadProviderRegistry(), providerID)
}

func validateProviderReference(providerID string) error {
	providerID = strings.TrimSpace(providerID)
	if providerID == "" {
		return nil
	}
	_, err := loadEnabledProviderRecord(providerID)
	return err
}

func updateProviderRegistry(mutator func(*ProviderRegistry) error) error {
	root := agentRuntimeConfigRoot()
	path := providerRegistryPath()
	if root == "" || path == "" {
		return fmt.Errorf("agent config root is unavailable")
	}
	return withManagerStateLock(root, true, func() error {
		registry, err := loadProviderRegistryFromDisk(path)
		if err != nil {
			return err
		}
		registry = normalizeProviderRegistry(registry)
		if err := mutator(&registry); err != nil {
			return err
		}
		return saveProviderRegistryLocked(root, path, registry)
	})
}

func loadProviderRegistryFromDisk(path string) (ProviderRegistry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ProviderRegistry{}, nil
		}
		return ProviderRegistry{}, err
	}
	var registry ProviderRegistry
	if err := json.Unmarshal(data, &registry); err != nil {
		if archiveErr := archiveBrokenManagerStateJournalPath(path, "corrupt provider registry"); archiveErr != nil {
			return ProviderRegistry{}, archiveErr
		}
		return ProviderRegistry{}, fmt.Errorf("corrupt provider registry %q was archived: %w", path, err)
	}
	return registry, nil
}

func saveProviderRegistryLocked(root, path string, registry ProviderRegistry) error {
	raw, _, err := marshalProviderRegistryForWrite(registry)
	if err != nil {
		return err
	}
	return writeManagerStateBytesLocked(root, path, "save_provider_registry", raw, 0o600)
}

func marshalProviderRegistryForWrite(registry ProviderRegistry) ([]byte, ProviderRegistry, error) {
	registry = normalizeProviderRegistry(registry)
	if err := validateProviderRegistry(registry); err != nil {
		return nil, ProviderRegistry{}, err
	}
	registry.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		return nil, ProviderRegistry{}, err
	}
	return raw, registry, nil
}

func validateProviderRegistry(registry ProviderRegistry) error {
	seen := make(map[string]struct{}, len(registry.Providers))
	for _, record := range registry.Providers {
		record = normalizeProviderRecord(record)
		if record.ProviderID == "" {
			return fmt.Errorf("provider requires provider_id")
		}
		if record.ChannelType != providerChannelAPI && record.ChannelType != providerChannelBridge {
			return fmt.Errorf("provider %q has unsupported channel_type %q", record.ProviderID, record.ChannelType)
		}
		if strings.TrimSpace(record.Driver) == "" {
			return fmt.Errorf("provider %q requires driver", record.ProviderID)
		}
		if strings.TrimSpace(record.GroupID) == "" {
			return fmt.Errorf("provider %q requires group_id", record.ProviderID)
		}
		if _, ok := seen[record.ProviderID]; ok {
			return fmt.Errorf("duplicate provider_id %q", record.ProviderID)
		}
		seen[record.ProviderID] = struct{}{}
	}
	return nil
}

func validateProviderCatalogSelection(record ProviderRecord) error {
	record = normalizeProviderRecord(record)
	if record.ChannelType == "" || record.Driver == "" {
		return nil
	}
	if _, ok := supportedProviderOptionForRecord(record.ChannelType, record.Driver); ok {
		return nil
	}
	if existing, ok := FindProviderRecord(record.ProviderID); ok {
		existing = normalizeProviderRecord(existing)
		if strings.EqualFold(existing.ChannelType, record.ChannelType) && strings.EqualFold(existing.Driver, record.Driver) {
			return nil
		}
	}
	return fmt.Errorf("provider %q uses unsupported implementation %s/%s; choose one from the supported catalog", firstNonEmpty(record.ProviderID, record.GroupID, "provider"), record.ChannelType, record.Driver)
}

func normalizeProviderRegistry(registry ProviderRegistry) ProviderRegistry {
	seen := make(map[string]struct{}, len(registry.Providers))
	normalized := make([]ProviderRecord, 0, len(registry.Providers))
	for _, record := range registry.Providers {
		record = normalizeProviderRecord(record)
		if record.ProviderID == "" {
			continue
		}
		if _, ok := seen[record.ProviderID]; ok {
			continue
		}
		seen[record.ProviderID] = struct{}{}
		normalized = append(normalized, record)
	}
	sort.SliceStable(normalized, func(i, j int) bool {
		left := firstNonEmpty(strings.TrimSpace(normalized[i].GroupID), strings.TrimSpace(normalized[i].ProviderID))
		right := firstNonEmpty(strings.TrimSpace(normalized[j].GroupID), strings.TrimSpace(normalized[j].ProviderID))
		if left == right {
			return normalized[i].ProviderID < normalized[j].ProviderID
		}
		return left < right
	})
	registry.Providers = normalized
	return registry
}

func normalizeProviderRecord(record ProviderRecord) ProviderRecord {
	record.GroupID = strings.TrimSpace(record.GroupID)
	record.ProviderID = firstNonEmpty(strings.TrimSpace(record.ProviderID), record.GroupID)
	record.Title = firstNonEmpty(strings.TrimSpace(record.Title), record.GroupID, record.ProviderID)
	record.ChannelType = strings.ToLower(strings.TrimSpace(record.ChannelType))
	record.Driver = strings.ToLower(strings.TrimSpace(record.Driver))
	record.DefaultModel = strings.TrimSpace(record.DefaultModel)
	record.Models = uniqueTrimmedCSVStrings(record.Models)
	record.API.BaseURL = strings.TrimSpace(record.API.BaseURL)
	record.API.PublicHeaders = normalizeProviderPublicHeaders(record.API.PublicHeaders)
	record.Bridge.Executable = strings.TrimSpace(record.Bridge.Executable)
	record.Bridge.Command = strings.TrimSpace(record.Bridge.Command)
	record.CreatedAt = strings.TrimSpace(record.CreatedAt)
	record.UpdatedAt = strings.TrimSpace(record.UpdatedAt)
	return record
}

func normalizeProviderPublicHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	normalized := make(map[string]string, len(headers))
	for key, value := range headers {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		normalized[key] = value
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func findProviderRecordInRegistry(registry ProviderRegistry, providerID string) (ProviderRecord, bool) {
	providerID = strings.TrimSpace(providerID)
	if providerID == "" {
		return ProviderRecord{}, false
	}
	for _, record := range normalizeProviderRegistry(registry).Providers {
		if record.ProviderID == providerID {
			return normalizeProviderRecord(record), true
		}
	}
	return ProviderRecord{}, false
}

func loadEnabledProviderRecord(providerID string) (ProviderRecord, error) {
	providerID = strings.TrimSpace(providerID)
	if providerID == "" {
		return ProviderRecord{}, nil
	}
	registry, err := loadProviderRegistryFromDisk(providerRegistryPath())
	if err != nil {
		return ProviderRecord{}, err
	}
	record, ok := findProviderRecordInRegistry(registry, providerID)
	if !ok {
		return ProviderRecord{}, fmt.Errorf("unknown provider %q: %w", providerID, errProviderNotFound)
	}
	if !record.Enabled {
		return ProviderRecord{}, fmt.Errorf("provider %q is disabled: %w", providerID, errProviderDisabled)
	}
	return record, nil
}

func providerReferenceLabels(providerID string) ([]string, error) {
	providerID = strings.TrimSpace(providerID)
	if providerID == "" {
		return nil, nil
	}
	registry, err := loadBotRegistryFromDisk(botRegistryPath())
	if err != nil {
		return nil, err
	}
	registry = normalizeBotRegistry(registry)
	seen := map[string]struct{}{}
	labels := make([]string, 0, 1+len(registry.Agents))
	push := func(label string) {
		label = strings.TrimSpace(label)
		if label == "" {
			return
		}
		if _, ok := seen[label]; ok {
			return
		}
		seen[label] = struct{}{}
		labels = append(labels, label)
	}
	if strings.TrimSpace(registry.Defaults.DefaultProviderID) == providerID {
		push("manager defaults")
	}
	for _, agent := range registry.Agents {
		if strings.TrimSpace(agent.ProviderID) == providerID {
			push(fmt.Sprintf("agent %s", agent.AgentID))
			continue
		}
		if strings.TrimSpace(LoadLocalRuntimeProfile(agent.Workdir).ProviderID) == providerID {
			push(fmt.Sprintf("local runtime %s", agent.AgentID))
		}
	}
	sort.Strings(labels)
	return labels, nil
}
