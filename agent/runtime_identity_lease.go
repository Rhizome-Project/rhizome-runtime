package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	runtimeIdentityLeaseDirname      = "runtime_identity"
	runtimeIdentityLeaseFilename     = ".lease.json"
	runtimeIdentityQuarantineDirname = "runtime_identity_quarantine"
	runtimeIdentityLeaseStaleAfter   = 5 * time.Minute
	runtimeIdentityLeaseMaxHeartbeat = runtimeIdentityLeaseStaleAfter / 2
)

type runtimeIdentityLeaseInfo struct {
	PID           int    `json:"pid,omitempty"`
	StartedAt     string `json:"started_at,omitempty"`
	LastSeenAt    string `json:"last_seen_at,omitempty"`
	Mode          string `json:"mode,omitempty"`
	Workdir       string `json:"workdir,omitempty"`
	WorkspaceID   string `json:"workspace_id,omitempty"`
	AgentID       string `json:"agent_id,omitempty"`
	LaunchProfile string `json:"launch_profile,omitempty"`
	DisplayName   string `json:"display_name,omitempty"`
	Reason        string `json:"reason,omitempty"`
}

type runtimeIdentityLease struct {
	path     string
	info     runtimeIdentityLeaseInfo
	acquired bool
}

var runtimeIdentityLeaseProcessMatchesFunc = runtimeIdentityLeaseProcessMatches

func (r *Runtime) acquireStartupIdentityLease() error {
	if r == nil {
		return nil
	}
	if r.cfg.Mode != RuntimeModeDaemon {
		return nil
	}
	if r.hasStartupIdentityLease() {
		return nil
	}

	lease, err := acquireRuntimeIdentityLease(r.cfg)
	if err != nil {
		return err
	}
	if lease == nil {
		return nil
	}

	r.mu.Lock()
	r.startupIdentityLease = lease
	r.mu.Unlock()
	return nil
}

func (r *Runtime) releaseStartupIdentityLease() {
	if r == nil {
		return
	}
	r.mu.Lock()
	lease := r.startupIdentityLease
	r.startupIdentityLease = nil
	r.mu.Unlock()
	if lease == nil || !lease.acquired {
		return
	}
	if runtimeIdentityLeaseIsOwnedBy(lease.path, lease.info) {
		_ = os.Remove(lease.path)
	}
}

func (r *Runtime) refreshStartupIdentityLease() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	lease := r.startupIdentityLease
	r.mu.Unlock()
	if lease == nil || !lease.acquired {
		return nil
	}

	current, err := loadRuntimeIdentityLease(lease.path)
	if err != nil {
		return err
	}
	if current.PID != lease.info.PID || strings.TrimSpace(current.WorkspaceID) != strings.TrimSpace(lease.info.WorkspaceID) || strings.TrimSpace(current.AgentID) != strings.TrimSpace(lease.info.AgentID) {
		return fmt.Errorf("runtime identity lease ownership changed for %s/%s", lease.info.WorkspaceID, lease.info.AgentID)
	}

	lease.info.LastSeenAt = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.MarshalIndent(lease.info, "", "  ")
	if err != nil {
		return err
	}
	if err := atomicWriteFile(lease.path, raw, 0o600); err != nil {
		return err
	}

	r.mu.Lock()
	if r.startupIdentityLease == lease {
		r.startupIdentityLease.info = lease.info
	}
	r.mu.Unlock()
	return nil
}

func (r *Runtime) hasStartupIdentityLease() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.startupIdentityLease != nil && r.startupIdentityLease.acquired
}

func acquireRuntimeIdentityLease(cfg RuntimeConfig) (*runtimeIdentityLease, error) {
	cfg.ApplyDefaults()
	if cfg.Mode != RuntimeModeDaemon {
		return nil, nil
	}

	workspaceID := strings.TrimSpace(cfg.WorkspaceID)
	agentID := strings.TrimSpace(cfg.AgentID)
	if workspaceID == "" || agentID == "" {
		return nil, fmt.Errorf("runtime identity lease requires workspace and agent identity")
	}

	path := runtimeIdentityLeasePath(workspaceID, agentID)
	if path == "" {
		return nil, fmt.Errorf("agent config root is unavailable")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	lease := runtimeIdentityLease{
		path: path,
		info: runtimeIdentityLeaseInfo{
			PID:           os.Getpid(),
			StartedAt:     now,
			LastSeenAt:    now,
			Mode:          string(cfg.Mode),
			Workdir:       strings.TrimSpace(cfg.Workdir),
			WorkspaceID:   workspaceID,
			AgentID:       agentID,
			LaunchProfile: runtimeLaunchProfileName(cfg.Workdir),
			DisplayName:   strings.TrimSpace(cfg.DisplayName),
		},
	}

	for {
		acquired, err := tryCreateRuntimeIdentityLease(path, lease.info)
		if err == nil {
			lease.acquired = acquired
			return &lease, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}

		existing, readErr := loadRuntimeIdentityLease(path)
		if readErr != nil {
			if archiveErr := archiveBrokenRuntimeIdentityLease(path, "corrupt runtime identity lease"); archiveErr != nil {
				return nil, fmt.Errorf("read runtime identity lease: %w", readErr)
			}
			continue
		}

		if existing.PID > 0 {
			if runtimeIdentityLeaseIsStale(existing, time.Now().UTC()) {
				if err := quarantineRuntimeIdentityLease(path); err != nil {
					return nil, err
				}
				continue
			}
			live, liveErr := processExists(existing.PID)
			if liveErr != nil {
				return nil, liveErr
			}
			if live {
				matchesLease, matchErr := runtimeIdentityLeaseProcessMatchesFunc(existing)
				if matchErr != nil {
					return nil, matchErr
				}
				if !matchesLease {
					if err := quarantineRuntimeIdentityLease(path); err != nil {
						return nil, err
					}
					continue
				}
				quarantinePath, quarantineErr := writeRuntimeIdentityQuarantine(path, lease.info, existing, "duplicate runtime identity lease already owns this agent/workspace")
				if quarantineErr != nil {
					return nil, quarantineErr
				}
				return nil, fmt.Errorf("runtime identity %s/%s already has an active consumer (pid=%d); quarantined at %s", workspaceID, agentID, existing.PID, quarantinePath)
			}
		}

		if err := quarantineRuntimeIdentityLease(path); err != nil {
			return nil, err
		}
	}
}

func runtimeIdentityLeaseProcessMatches(info runtimeIdentityLeaseInfo) (bool, error) {
	commandLine, err := processCommandLine(info.PID)
	if err != nil {
		return false, err
	}
	return runtimeIdentityLeaseCommandLineMatches(info, commandLine), nil
}

func runtimeIdentityLeaseCommandLineMatches(info runtimeIdentityLeaseInfo, commandLine string) bool {
	commandLine = strings.TrimSpace(commandLine)
	if commandLine == "" {
		return false
	}
	lower := strings.ToLower(commandLine)
	if !strings.Contains(" "+lower+" ", " daemon ") {
		return false
	}
	for _, pair := range []struct {
		flag  string
		value string
	}{
		{flag: "--workspace-id", value: info.WorkspaceID},
		{flag: "--agent-id", value: info.AgentID},
		{flag: "--workdir", value: info.Workdir},
	} {
		if !runtimeIdentityCommandLineContainsFlagValue(commandLine, pair.flag, pair.value) {
			return false
		}
	}
	return true
}

func runtimeIdentityCommandLineContainsFlagValue(commandLine, flag, value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return true
	}
	normalizedCommand := runtimeIdentityNormalizeCommandLine(commandLine)
	normalizedFlag := strings.ToLower(strings.TrimSpace(flag))
	normalizedValue := runtimeIdentityNormalizeCommandLine(value)
	return strings.Contains(normalizedCommand, normalizedFlag) && strings.Contains(normalizedCommand, normalizedValue)
}

func runtimeIdentityNormalizeCommandLine(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, `"'`)
	value = strings.ReplaceAll(value, "\\", "/")
	value = strings.ToLower(value)
	return value
}

func runtimeIdentityLeasePath(workspaceID, agentID string) string {
	return runtimeIdentityLeasePathUnderRoot(runtimeIdentityLeaseRoot(), workspaceID, agentID)
}

func runtimeIdentityLeasePathUnderRoot(root, workspaceID, agentID string) string {
	workspacePart := sanitizePathComponent(firstNonEmpty(workspaceID, "workspace"))
	agentPart := sanitizePathComponent(firstNonEmpty(agentID, "agent"))
	if root == "" {
		return ""
	}
	return filepath.Join(root, workspacePart, agentPart+runtimeIdentityLeaseFilename)
}

func runtimeIdentityQuarantinePath(workspaceID, agentID string) string {
	workspacePart := sanitizePathComponent(firstNonEmpty(workspaceID, "workspace"))
	agentPart := sanitizePathComponent(firstNonEmpty(agentID, "agent"))
	name := fmt.Sprintf("%s-%s-%d.json", time.Now().UTC().Format("20060102T150405.000000000Z"), workspacePart+"-"+agentPart, os.Getpid())
	root := runtimeIdentityQuarantineRoot()
	if root == "" {
		return ""
	}
	return filepath.Join(root, workspacePart, agentPart, name)
}

func runtimeIdentityLeaseRoot() string {
	if root := strings.TrimSpace(os.Getenv(managedAgentIdentityLeaseRootFlag)); root != "" {
		return filepath.Join(cleanManagedRuntimeRoot(root), runtimeIdentityLeaseDirname)
	}
	return agentRuntimeConfigPath(runtimeIdentityLeaseDirname)
}

func runtimeIdentityQuarantineRoot() string {
	if root := strings.TrimSpace(os.Getenv(managedAgentIdentityLeaseRootFlag)); root != "" {
		return filepath.Join(cleanManagedRuntimeRoot(root), runtimeIdentityQuarantineDirname)
	}
	return agentRuntimeConfigPath(runtimeIdentityQuarantineDirname)
}

func runtimeIdentityLeaseIsStale(info runtimeIdentityLeaseInfo, now time.Time) bool {
	reference := strings.TrimSpace(info.LastSeenAt)
	if reference == "" {
		reference = strings.TrimSpace(info.StartedAt)
	}
	if reference == "" {
		return true
	}
	seenAt, err := time.Parse(time.RFC3339Nano, reference)
	if err != nil {
		return true
	}
	return now.Sub(seenAt) > runtimeIdentityLeaseStaleAfter
}

func runtimeIdentityLeaseIsOwnedBy(path string, owner runtimeIdentityLeaseInfo) bool {
	current, err := loadRuntimeIdentityLease(path)
	if err != nil {
		return false
	}
	return current.PID == owner.PID &&
		strings.TrimSpace(current.WorkspaceID) == strings.TrimSpace(owner.WorkspaceID) &&
		strings.TrimSpace(current.AgentID) == strings.TrimSpace(owner.AgentID)
}

func tryCreateRuntimeIdentityLease(path string, info runtimeIdentityLeaseInfo) (bool, error) {
	if strings.TrimSpace(path) == "" {
		return false, fmt.Errorf("runtime identity lease path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, err
	}
	raw, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return false, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return false, err
	}
	if _, err := file.Write(raw); err != nil {
		file.Close()
		_ = os.Remove(path)
		return false, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		_ = os.Remove(path)
		return false, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return false, err
	}
	return true, nil
}

func loadRuntimeIdentityLease(path string) (runtimeIdentityLeaseInfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return runtimeIdentityLeaseInfo{}, err
	}
	var info runtimeIdentityLeaseInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return runtimeIdentityLeaseInfo{}, err
	}
	return info, nil
}

func quarantineRuntimeIdentityLease(path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("runtime identity lease path is empty")
	}
	backupPath := path + ".stale-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	if err := replaceFileAtomic(path, backupPath); err != nil {
		return err
	}
	return nil
}

func writeRuntimeIdentityQuarantine(path string, current, existing runtimeIdentityLeaseInfo, reason string) (string, error) {
	quarantinePath := runtimeIdentityQuarantinePath(current.WorkspaceID, current.AgentID)
	if err := os.MkdirAll(filepath.Dir(quarantinePath), 0o700); err != nil {
		return "", err
	}
	record := map[string]any{
		"reason":     strings.TrimSpace(reason),
		"current":    current,
		"existing":   existing,
		"lease":      path,
		"created_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	raw, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(quarantinePath, raw, 0o600); err != nil {
		return "", err
	}
	return quarantinePath, nil
}

func archiveBrokenRuntimeIdentityLease(path, reason string) error {
	archived := path + ".broken-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	if err := replaceFileAtomic(path, archived); err != nil {
		return fmt.Errorf("%s: %w", reason, err)
	}
	return nil
}

func runtimeLaunchProfileName(workdir string) string {
	if profile, ok := deterministicLaunchProfileForWorkdir(workdir); ok {
		return profile.Name
	}
	return ""
}
