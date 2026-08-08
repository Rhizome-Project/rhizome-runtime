package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func appendKnownBrowserDirsToEnvPath(env []string) []string {
	return appendExistingExecutableDirsToEnvPath(env, knownBrowserExecutableCandidates(runtime.GOOS))
}

func addKnownBrowserDirsToEnvMap(env map[string]string) {
	if env == nil {
		return
	}
	pathValue, pathKey := envPathValueFromMap(env)
	updated := appendExistingExecutableDirsToPathValue(pathValue, knownBrowserExecutableCandidates(runtime.GOOS))
	if updated == pathValue {
		return
	}
	if pathKey == "" {
		pathKey = "PATH"
	}
	env[pathKey] = updated
}

func appendExistingExecutableDirsToEnvPath(env []string, executablePaths []string) []string {
	pathValue, pathKey := envPathValueFromSlice(env)
	updated := appendExistingExecutableDirsToPathValue(pathValue, executablePaths)
	if updated == pathValue {
		return env
	}
	if pathKey == "" {
		pathKey = "PATH"
	}
	return upsertEnvValue(env, pathKey, updated)
}

func appendExistingExecutableDirsToPathValue(pathValue string, executablePaths []string) string {
	dirs := existingExecutableDirs(executablePaths)
	if len(dirs) == 0 {
		return pathValue
	}
	parts := splitPathList(pathValue)
	for _, dir := range dirs {
		if pathListContains(parts, dir) {
			continue
		}
		parts = append(parts, dir)
	}
	return strings.Join(parts, string(os.PathListSeparator))
}

func existingExecutableDirs(executablePaths []string) []string {
	out := []string{}
	for _, candidate := range executablePaths {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			dir := filepath.Clean(filepath.Dir(candidate))
			if dir != "." && !pathListContains(out, dir) {
				out = append(out, dir)
			}
		}
	}
	return out
}

func knownBrowserExecutableCandidates(goos string) []string {
	switch goos {
	case "windows":
		return knownWindowsBrowserExecutableCandidates()
	default:
		return nil
	}
}

func knownWindowsBrowserExecutableCandidates() []string {
	bases := uniqueNonEmptyStrings([]string{
		os.Getenv("ProgramFiles"),
		os.Getenv("ProgramW6432"),
		os.Getenv("ProgramFiles(x86)"),
		`C:\Program Files`,
		`C:\Program Files (x86)`,
	})
	localAppData := strings.TrimSpace(os.Getenv("LOCALAPPDATA"))
	candidates := []string{}
	for _, base := range bases {
		candidates = append(candidates,
			filepath.Join(base, "Google", "Chrome", "Application", "chrome.exe"),
			filepath.Join(base, "Microsoft", "Edge", "Application", "msedge.exe"),
			filepath.Join(base, "Mozilla Firefox", "firefox.exe"),
		)
	}
	if localAppData != "" {
		candidates = append(candidates,
			filepath.Join(localAppData, "Google", "Chrome", "Application", "chrome.exe"),
			filepath.Join(localAppData, "Microsoft", "Edge", "Application", "msedge.exe"),
		)
	}
	return uniqueNonEmptyStrings(candidates)
}

func envPathValueFromSlice(env []string) (string, string) {
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok && strings.EqualFold(strings.TrimSpace(key), "PATH") {
			return value, key
		}
	}
	return "", ""
}

func envPathValueFromMap(env map[string]string) (string, string) {
	for key, value := range env {
		if strings.EqualFold(strings.TrimSpace(key), "PATH") {
			return value, key
		}
	}
	return "", ""
}

func splitPathList(pathValue string) []string {
	parts := []string{}
	for _, part := range filepath.SplitList(pathValue) {
		part = strings.TrimSpace(part)
		if part != "" {
			parts = append(parts, filepath.Clean(part))
		}
	}
	return parts
}

func pathListContains(parts []string, candidate string) bool {
	candidate = filepath.Clean(strings.TrimSpace(candidate))
	if candidate == "" {
		return true
	}
	for _, part := range parts {
		part = filepath.Clean(strings.TrimSpace(part))
		if runtime.GOOS == "windows" {
			if strings.EqualFold(part, candidate) {
				return true
			}
			continue
		}
		if part == candidate {
			return true
		}
	}
	return false
}

func uniqueNonEmptyStrings(values []string) []string {
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || stringSliceContains(out, value) {
			continue
		}
		out = append(out, value)
	}
	return out
}

func stringSliceContains(values []string, candidate string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(candidate)) {
			return true
		}
	}
	return false
}
