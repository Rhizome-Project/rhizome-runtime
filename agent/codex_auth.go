package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

const codexConfigDir = ".codex"

type codexAuthState struct {
	AuthMode string `json:"auth_mode"`
	Tokens   struct {
		AccessToken string `json:"access_token"`
	} `json:"tokens"`
}

func codexAuthPath() string {
	return agentRuntimeCodexPath("auth.json")
}

func codexAuthPathForHome(root string) string {
	root = cleanManagedRuntimeRoot(root)
	if root == "" {
		return ""
	}
	return filepath.Join(root, "auth.json")
}

func hasChatGPTCodexSession() bool {
	return hasChatGPTCodexSessionInHome(agentRuntimeCodexHome())
}

func hasChatGPTCodexSessionInHome(root string) bool {
	data, err := os.ReadFile(codexAuthPathForHome(root))
	if err != nil {
		return false
	}
	var state codexAuthState
	if err := json.Unmarshal(data, &state); err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(state.AuthMode), "chatgpt") &&
		strings.TrimSpace(state.Tokens.AccessToken) != ""
}

func findCodexExecutable() string {
	return findCodexExecutableInHome(agentRuntimeCodexHome())
}

func sharedCodexExecutablePath() string {
	name := "codex"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}

	if env := strings.TrimSpace(firstNonEmpty(os.Getenv("CODEX_CLI_PATH"), os.Getenv("CUSTOM_CLI_PATH"))); env != "" {
		if info, err := os.Stat(env); err == nil && !info.IsDir() {
			return env
		}
	}

	if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
		candidates := []string{
			filepath.Join(home, ".local", "bin", name),
			filepath.Join(home, "go", "bin", name),
			filepath.Join(home, "Desktop", "agents", name),
		}
		for _, candidate := range candidates {
			if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
				return candidate
			}
		}
	}

	if path, err := exec.LookPath(name); err == nil {
		return path
	}
	return ""
}

func findCodexExecutableInHome(root string) string {
	name := "codex"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}

	candidates := []string{
		filepath.Join(cleanManagedRuntimeRoot(root), ".sandbox-bin", name),
		filepath.Join(cleanManagedRuntimeRoot(root), "bin", name),
	}
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}

	if !managedRuntimeRequiresIsolatedCodexHome() && strings.TrimSpace(root) == strings.TrimSpace(agentRuntimeCodexHome()) {
		if env := strings.TrimSpace(firstNonEmpty(os.Getenv("CODEX_CLI_PATH"), os.Getenv("CUSTOM_CLI_PATH"))); env != "" {
			if info, err := os.Stat(env); err == nil && !info.IsDir() {
				return env
			}
		}
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	return ""
}
