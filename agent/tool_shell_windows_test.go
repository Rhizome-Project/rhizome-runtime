package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCleanupWindowsCommandLineProcessesParsesJSONRootArray(t *testing.T) {
	rootA := filepath.Join(t.TempDir(), "alpha")
	rootB := filepath.Join(t.TempDir(), "beta")
	if err := os.MkdirAll(rootA, 0o755); err != nil {
		t.Fatalf("mkdir rootA: %v", err)
	}
	if err := os.MkdirAll(rootB, 0o755); err != nil {
		t.Fatalf("mkdir rootB: %v", err)
	}

	note, err := cleanupWindowsCommandLineProcessesForRoots([]string{rootA, rootB})
	if err != nil {
		t.Fatalf("cleanupWindowsCommandLineProcessesForRoots() error = %v note=%s", err, note)
	}
	if strings.Contains(strings.ToLower(note), "getfullpath") || strings.Contains(strings.ToLower(note), "invalid cleanup root") {
		t.Fatalf("cleanup parsed a JSON root array incorrectly: %s", note)
	}
}
