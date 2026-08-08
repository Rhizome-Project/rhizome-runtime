package main

import (
	"os"
	"path/filepath"
	"strings"
)

const (
	managedAgentConfigRootFlag        = "RHIZOME_AGENT_CONFIG_ROOT"
	managedAgentCodexHomeFlag         = "RHIZOME_AGENT_CODEX_HOME"
	managedAgentIdentityLeaseRootFlag = "RHIZOME_AGENT_IDENTITY_LEASE_ROOT"
)

func managedAgentHomeRootPath(workdir string) string {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return ""
	}
	return filepath.Join(workdir, ".home")
}

func managedAgentConfigRootPath(workdir string) string {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return ""
	}
	return filepath.Join(workdir, ".runtime-config")
}

func managedAgentIdentityLeaseRootPath(workdir string) string {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return ""
	}
	parent := filepath.Dir(cleanManagedRuntimeRoot(workdir))
	if strings.TrimSpace(parent) == "" || parent == "." {
		return ""
	}
	return filepath.Join(parent, ".runtime-identity")
}

func managedAgentCodexHomePath(workdir string) string {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return ""
	}
	return filepath.Join(workdir, ".codex-home")
}

func agentRuntimeConfigRoot() string {
	if root := strings.TrimSpace(os.Getenv(managedAgentConfigRootFlag)); root != "" {
		return cleanManagedRuntimeRoot(root)
	}
	if isManagedAgentProcess() {
		return ""
	}
	home, _ := os.UserHomeDir()
	if strings.TrimSpace(home) == "" {
		return ""
	}
	return filepath.Join(home, configDir)
}

func agentRuntimeConfigPath(parts ...string) string {
	root := agentRuntimeConfigRoot()
	if root == "" {
		return ""
	}
	return filepath.Join(append([]string{root}, parts...)...)
}

func agentRuntimeCodexHome() string {
	if root := strings.TrimSpace(os.Getenv(managedAgentCodexHomeFlag)); root != "" {
		return cleanManagedRuntimeRoot(root)
	}
	if managedRuntimeRequiresIsolatedCodexHome() {
		return ""
	}
	home, _ := os.UserHomeDir()
	if strings.TrimSpace(home) == "" {
		return ""
	}
	return filepath.Join(home, codexConfigDir)
}

func agentRuntimeCodexPath(parts ...string) string {
	root := agentRuntimeCodexHome()
	if root == "" {
		return ""
	}
	return filepath.Join(append([]string{root}, parts...)...)
}

func cleanManagedRuntimeRoot(root string) string {
	root = strings.TrimSpace(root)
	if root == "" {
		return ""
	}
	if abs, err := filepath.Abs(root); err == nil {
		root = abs
	}
	return filepath.Clean(root)
}

func managedRuntimeRequiresIsolatedCodexHome() bool {
	return isPartnerManagedRuntime()
}

func isPartnerManagedRuntime() bool {
	return isManagedAgentProcess() && !runtimeAllowsLocalShell(RuntimeConfig{})
}
