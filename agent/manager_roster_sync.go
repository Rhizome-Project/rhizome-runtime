package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type managedRosterSyncEntry struct {
	PresetTarget   string `json:"preset_target"`
	Role           string `json:"role"`
	Specialization string `json:"specialization"`
}

type managedRosterSyncOptions struct {
	RosterJSON          string
	ParentDir           string
	Prune               bool
	MaterializeProfiles bool
}

type managedRosterSyncResult struct {
	Added   []string
	Updated []string
	Removed []string
}

func runSyncRoster(args []string) error {
	flags := flag.NewFlagSet(appCommandName+" sync-roster", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	rosterJSON := flags.String("roster-json", "", "Run roster JSON whose roles become the local managed launch registry")
	parentDir := flags.String("parent-dir", "", "Parent directory for agents that are present in the roster but not yet in the local registry")
	prune := flags.Bool("prune", false, "Remove local managed agents that are not present in the roster")
	noProfiles := flags.Bool("no-profiles", false, "Do not materialize agent_profile.json files from roster roles")
	if err := flags.Parse(args); err != nil {
		return err
	}
	result, err := syncManagedRosterFromFile(managedRosterSyncOptions{
		RosterJSON:          *rosterJSON,
		ParentDir:           *parentDir,
		Prune:               *prune,
		MaterializeProfiles: !*noProfiles,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "sync-roster\tpass\tadded=%s\tupdated=%s\tremoved=%s\n",
		strings.Join(result.Added, ","),
		strings.Join(result.Updated, ","),
		strings.Join(result.Removed, ","),
	)
	return nil
}

func syncManagedRosterFromFile(opts managedRosterSyncOptions) (managedRosterSyncResult, error) {
	roster, err := loadManagedRosterSyncEntries(opts.RosterJSON)
	if err != nil {
		return managedRosterSyncResult{}, err
	}

	var synced []ManagedAgentRecord
	var profileEntries map[string]managedRosterSyncEntry
	var result managedRosterSyncResult
	now := time.Now().UTC().Format(time.RFC3339Nano)

	if err := updateBotRegistry(func(registry *BotRegistry) error {
		*registry = normalizeBotRegistry(*registry)
		existingByID := make(map[string]ManagedAgentRecord, len(registry.Agents))
		for _, record := range registry.Agents {
			record = normalizeManagedAgentRecord(record)
			if strings.TrimSpace(record.AgentID) == "" {
				continue
			}
			existingByID[record.AgentID] = record
		}

		parentDir := managedRosterSyncParentDir(opts.ParentDir, *registry)
		ids := sortedManagedRosterSyncIDs(roster)
		next := make([]ManagedAgentRecord, 0, len(ids))
		profileEntries = make(map[string]managedRosterSyncEntry, len(ids))
		for _, agentID := range ids {
			entry := roster[agentID]
			record, existed := existingByID[agentID]
			updated, err := managedRosterSyncRecord(agentID, entry, record, registry.Defaults, parentDir, now)
			if err != nil {
				return err
			}
			if existed {
				if managedRosterRecordChanged(record, updated) {
					result.Updated = append(result.Updated, agentID)
				}
			} else {
				result.Added = append(result.Added, agentID)
			}
			next = append(next, updated)
			synced = append(synced, updated)
			profileEntries[agentID] = entry
		}

		if opts.Prune {
			for _, record := range registry.Agents {
				record = normalizeManagedAgentRecord(record)
				if strings.TrimSpace(record.AgentID) == "" {
					continue
				}
				if _, ok := roster[record.AgentID]; !ok {
					result.Removed = append(result.Removed, record.AgentID)
				}
			}
			registry.Agents = next
		} else {
			seen := make(map[string]struct{}, len(next))
			for _, record := range next {
				seen[record.AgentID] = struct{}{}
			}
			kept := make([]ManagedAgentRecord, 0, len(registry.Agents)+len(next))
			for _, record := range registry.Agents {
				record = normalizeManagedAgentRecord(record)
				if _, ok := seen[record.AgentID]; ok {
					continue
				}
				kept = append(kept, record)
			}
			registry.Agents = append(kept, next...)
		}
		return nil
	}); err != nil {
		return managedRosterSyncResult{}, err
	}

	if opts.MaterializeProfiles {
		for _, record := range synced {
			entry := profileEntries[record.AgentID]
			if err := materializeManagedRosterSyncProfile(record, entry); err != nil {
				return managedRosterSyncResult{}, err
			}
		}
	}

	sort.Strings(result.Added)
	sort.Strings(result.Updated)
	sort.Strings(result.Removed)
	return result, nil
}

func loadManagedRosterSyncEntries(path string) (map[string]managedRosterSyncEntry, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("roster json path is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read roster json: %w", err)
	}
	var decoded map[string]managedRosterSyncEntry
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("decode roster json: %w", err)
	}
	if len(decoded) == 0 {
		return nil, fmt.Errorf("roster json has no agents")
	}
	roster := make(map[string]managedRosterSyncEntry, len(decoded))
	for agentID, entry := range decoded {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			return nil, fmt.Errorf("roster json contains an empty agent id")
		}
		entry.PresetTarget = normalizeAgentAnatomyPreset(entry.PresetTarget)
		entry.Role = strings.TrimSpace(entry.Role)
		entry.Specialization = strings.TrimSpace(entry.Specialization)
		if entry.PresetTarget == "" {
			return nil, fmt.Errorf("roster json agent %s is missing preset_target", agentID)
		}
		if entry.Role == "" {
			return nil, fmt.Errorf("roster json agent %s is missing role", agentID)
		}
		roster[agentID] = entry
	}
	return roster, nil
}

func sortedManagedRosterSyncIDs(roster map[string]managedRosterSyncEntry) []string {
	ids := make([]string, 0, len(roster))
	for id := range roster {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func managedRosterSyncParentDir(explicit string, registry BotRegistry) string {
	if parent := strings.TrimSpace(explicit); parent != "" {
		return cleanOptionalManagedPath(parent)
	}
	if parent := strings.TrimSpace(registry.Defaults.DefaultParentDir); parent != "" {
		return cleanOptionalManagedPath(parent)
	}
	for _, record := range registry.Agents {
		record = normalizeManagedAgentRecord(record)
		if strings.TrimSpace(record.Workdir) != "" {
			return filepath.Dir(record.Workdir)
		}
	}
	return ""
}

func managedRosterSyncRecord(agentID string, entry managedRosterSyncEntry, existing ManagedAgentRecord, defaults BotManagerDefaults, parentDir, now string) (ManagedAgentRecord, error) {
	record := normalizeManagedAgentRecord(existing)
	record.AgentID = agentID
	record.DisplayName = firstNonEmpty(record.DisplayName, humanizeAgentID(agentID))
	record.Workdir = strings.TrimSpace(record.Workdir)
	if record.Workdir == "" {
		if strings.TrimSpace(parentDir) == "" {
			return ManagedAgentRecord{}, fmt.Errorf("managed roster sync cannot infer workdir for %s: set bot_registry.defaults.default_parent_dir or pass -parent-dir", agentID)
		}
		record.Workdir = filepath.Join(parentDir, agentID)
	}
	record.HostURL = firstNonEmpty(record.HostURL, defaults.HostURL)
	record.WorkspaceID = firstNonEmpty(record.WorkspaceID, defaults.WorkspaceID)
	record.OwnerUserID = firstNonEmpty(record.OwnerUserID, defaults.OwnerUserID)
	record.ProviderID = firstNonEmpty(record.ProviderID, defaults.DefaultProviderID)
	record.ModelOverride = firstNonEmpty(record.ModelOverride, defaults.ModelOverride)
	record.GroupID = firstNonEmpty(record.GroupID, defaults.GroupID)
	record.LLMBackend = firstNonEmpty(record.LLMBackend, defaults.LLMBackend)
	record.Model = firstNonEmpty(record.Model, defaults.Model)
	record.CoordinationMode = firstNonEmpty(record.CoordinationMode, defaults.CoordinationMode)
	record.Role = entry.Role
	record.AnatomyPreset = entry.PresetTarget
	record.AnatomyPath = ""
	record.AnatomyDigest = ""
	if len(record.ToolBundles) == 0 && len(defaults.ToolBundles) > 0 {
		record.ToolBundles = append([]string(nil), defaults.ToolBundles...)
	}
	if strings.TrimSpace(record.AddedAt) == "" {
		record.AddedAt = now
	}
	record.UpdatedAt = now
	return normalizeManagedAgentRecord(record), nil
}

func managedRosterRecordChanged(before, after ManagedAgentRecord) bool {
	before = normalizeManagedAgentRecord(before)
	after = normalizeManagedAgentRecord(after)
	return before.AgentID != after.AgentID ||
		before.DisplayName != after.DisplayName ||
		before.Workdir != after.Workdir ||
		before.HostURL != after.HostURL ||
		before.WorkspaceID != after.WorkspaceID ||
		before.OwnerUserID != after.OwnerUserID ||
		before.ProviderID != after.ProviderID ||
		before.ModelOverride != after.ModelOverride ||
		before.GroupID != after.GroupID ||
		before.Role != after.Role ||
		before.LLMBackend != after.LLMBackend ||
		before.Model != after.Model ||
		before.CoordinationMode != after.CoordinationMode ||
		before.AnatomyPreset != after.AnatomyPreset ||
		before.AnatomyPath != after.AnatomyPath ||
		before.AnatomyDigest != after.AnatomyDigest ||
		strings.Join(before.ToolBundles, "\x00") != strings.Join(after.ToolBundles, "\x00")
}

func materializeManagedRosterSyncProfile(record ManagedAgentRecord, entry managedRosterSyncEntry) error {
	record = normalizeManagedAgentRecord(record)
	if strings.TrimSpace(record.Workdir) == "" {
		return nil
	}
	if err := os.MkdirAll(record.Workdir, 0o755); err != nil {
		return fmt.Errorf("create managed agent workdir %s: %w", record.Workdir, err)
	}
	profile := LoadAgentProfile(record.Workdir)
	if strings.TrimSpace(profile.AgentID) == "" && strings.TrimSpace(profile.DisplayName) == "" {
		profile = DefaultAgentProfile(record.AgentID, record.DisplayName, entry.Role)
	}
	profile.AgentID = record.AgentID
	profile.DisplayName = firstNonEmpty(record.DisplayName, profile.DisplayName, humanizeAgentID(record.AgentID))
	profile.GroupID = firstNonEmpty(record.GroupID, profile.GroupID)
	profile.Role = entry.Role
	if entry.Specialization != "" {
		profile.PrimarySpecialization = entry.Specialization
		profile.DefaultWorkMode = entry.Specialization
	}
	return SaveAgentProfile(record.Workdir, profile)
}
