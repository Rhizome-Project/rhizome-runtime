package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendExistingExecutableDirsToEnvPathAddsDiscoveredDirs(t *testing.T) {
	existingPathDir := t.TempDir()
	browserDir := t.TempDir()
	browserExe := filepath.Join(browserDir, "chrome.exe")
	if err := os.WriteFile(browserExe, []byte("stub"), 0o755); err != nil {
		t.Fatalf("write browser stub: %v", err)
	}
	missingExe := filepath.Join(t.TempDir(), "missing-browser.exe")

	env := []string{"Path=" + existingPathDir}
	got := appendExistingExecutableDirsToEnvPath(env, []string{browserExe, browserExe, missingExe})
	pathValue, pathKey := envPathValueFromSlice(got)
	if pathKey != "Path" {
		t.Fatalf("expected original path key casing to be preserved, got %q in %+v", pathKey, got)
	}
	parts := filepath.SplitList(pathValue)
	if !pathListContains(parts, existingPathDir) {
		t.Fatalf("expected existing PATH dir %q to remain in %q", existingPathDir, pathValue)
	}
	if !pathListContains(parts, browserDir) {
		t.Fatalf("expected discovered browser dir %q in PATH %q", browserDir, pathValue)
	}
	if countPathPart(parts, browserDir) != 1 {
		t.Fatalf("expected browser dir to be added once, got parts %+v", parts)
	}
}

func TestAppendExistingExecutableDirsToEnvPathLeavesEnvWhenNoCandidatesExist(t *testing.T) {
	env := []string{"PATH=" + t.TempDir()}
	got := appendExistingExecutableDirsToEnvPath(env, []string{filepath.Join(t.TempDir(), "missing.exe")})
	if len(got) != len(env) || got[0] != env[0] {
		t.Fatalf("expected env to remain unchanged, got %+v want %+v", got, env)
	}
}

func countPathPart(parts []string, candidate string) int {
	count := 0
	for _, part := range parts {
		if pathListContains([]string{part}, candidate) {
			count++
		}
	}
	return count
}
