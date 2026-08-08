//go:build !windows

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const unixPathMarker = "# rhizome-bot PATH"

func ensureInstallDirInPath(installDir string) (bool, error) {
	installDir, err := filepath.Abs(strings.TrimSpace(installDir))
	if err != nil {
		return false, err
	}
	current := strings.TrimSpace(os.Getenv("PATH"))
	_ = os.Setenv("PATH", appendPathEntry(current, installDir))
	if pathContainsDir(current, installDir) {
		return false, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return false, fmt.Errorf("resolve home directory: %w", err)
	}
	profilePath := filepath.Join(home, ".profile")
	existing, err := os.ReadFile(profilePath)
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("read %s: %w", profilePath, err)
	}
	if strings.Contains(string(existing), unixPathMarker) || strings.Contains(string(existing), installDir) {
		return false, nil
	}

	block := fmt.Sprintf("\n%s\ncase \":$PATH:\" in\n  *:%s:*) ;;\n  *) export PATH=\"$PATH:%s\" ;;\nesac\n",
		unixPathMarker,
		installDir,
		installDir,
	)
	file, err := os.OpenFile(profilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return false, fmt.Errorf("open %s: %w", profilePath, err)
	}
	defer file.Close()
	if _, err := file.WriteString(block); err != nil {
		return false, fmt.Errorf("append PATH block to %s: %w", profilePath, err)
	}
	return true, nil
}

func pathContainsDir(pathValue, dir string) bool {
	for _, entry := range strings.Split(pathValue, ":") {
		if strings.TrimSpace(entry) == dir {
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
	return pathValue + ":" + dir
}
