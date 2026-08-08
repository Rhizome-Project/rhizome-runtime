//go:build windows

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

func ensureInstallDirInPath(installDir string) (bool, error) {
	installDir, err := filepath.Abs(strings.TrimSpace(installDir))
	if err != nil {
		return false, err
	}

	current := strings.TrimSpace(os.Getenv("PATH"))
	currentPath := prioritizePathEntry(current, installDir)
	_ = os.Setenv("PATH", currentPath)
	if shouldSkipUserPathMutation() {
		return currentPath != current, nil
	}

	key, _, err := registry.CreateKey(registry.CURRENT_USER, `Environment`, registry.QUERY_VALUE|registry.SET_VALUE)
	if err != nil {
		if isRegistryAccessDenied(err) {
			return false, nil
		}
		return false, fmt.Errorf("open user environment: %w", err)
	}
	defer key.Close()

	value, _, err := key.GetStringValue("Path")
	if err != nil && err != registry.ErrNotExist {
		if isRegistryAccessDenied(err) {
			return false, nil
		}
		return false, fmt.Errorf("read user PATH: %w", err)
	}

	updatedUserPath := prioritizePathEntry(value, installDir)
	changed := updatedUserPath != value || currentPath != current
	if updatedUserPath != value {
		if err := key.SetStringValue("Path", updatedUserPath); err != nil {
			if isRegistryAccessDenied(err) {
				return false, nil
			}
			return false, fmt.Errorf("write user PATH: %w", err)
		}
	}
	return changed, nil
}

func pathContainsDir(pathValue, dir string) bool {
	for _, entry := range strings.Split(pathValue, ";") {
		if strings.EqualFold(strings.TrimSpace(entry), dir) {
			return true
		}
	}
	return false
}

func appendPathEntry(pathValue, dir string) string {
	pathValue = strings.TrimSpace(pathValue)
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return pathValue
	}
	if pathContainsDir(pathValue, dir) {
		return pathValue
	}
	if pathValue == "" {
		return dir
	}
	return pathValue + ";" + dir
}

func prioritizePathEntry(pathValue, dir string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return strings.TrimSpace(pathValue)
	}
	entries := make([]string, 0, 8)
	entries = append(entries, dir)
	for _, entry := range strings.Split(pathValue, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" || strings.EqualFold(entry, dir) {
			continue
		}
		entries = append(entries, entry)
	}
	return strings.Join(entries, ";")
}

func isRegistryAccessDenied(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED)
}

func shouldSkipUserPathMutation() bool {
	if truthyEnv("RHIZOME_BOT_INSTALL_SKIP_USER_PATH") {
		return true
	}
	exeName := strings.ToLower(filepath.Base(os.Args[0]))
	return strings.HasSuffix(exeName, ".test.exe") || strings.HasSuffix(exeName, ".test")
}

func truthyEnv(name string) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	switch value {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
