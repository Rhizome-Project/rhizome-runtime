package app

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
)

func TestApplyBuildSettings(t *testing.T) {
	info := RuntimeBuildInfo{}
	ApplyBuildSettings(&info, []debug.BuildSetting{
		{Key: "vcs.revision", Value: "abc123"},
		{Key: "vcs.time", Value: "2026-03-21T00:00:00Z"},
		{Key: "vcs.modified", Value: "true"},
	})

	if info.VCSRevision != "abc123" {
		t.Fatalf("expected vcs revision abc123, got %q", info.VCSRevision)
	}
	if info.VCSTime != "2026-03-21T00:00:00Z" {
		t.Fatalf("expected vcs time to be set, got %q", info.VCSTime)
	}
	if !info.VCSModified {
		t.Fatalf("expected vcs modified to be true")
	}
}

func TestCurrentRuntimeBuildInfoIncludesBinaryIdentity(t *testing.T) {
	info := CurrentRuntimeBuildInfo()
	if strings.TrimSpace(info.BinaryPath) == "" {
		t.Fatalf("expected binary path in runtime build info, got %+v", info)
	}
	if len(info.BinarySHA256) != 64 {
		t.Fatalf("expected sha256 binary digest, got %+v", info)
	}
	if strings.TrimSpace(info.BinaryModTime) == "" {
		t.Fatalf("expected binary modtime in runtime build info, got %+v", info)
	}
	if info.BinarySizeBytes <= 0 {
		t.Fatalf("expected positive binary size in runtime build info, got %+v", info)
	}
}

func TestFindGitRepoRoot(t *testing.T) {
	root := t.TempDir()
	gitMarker := filepath.Join(root, ".git")
	if err := os.WriteFile(gitMarker, []byte("gitdir: ./.git/worktrees/test"), 0o644); err != nil {
		t.Fatalf("write git marker: %v", err)
	}

	nested := filepath.Join(root, "a", "b", "c")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	got, err := FindGitRepoRoot(nested)
	if err != nil {
		t.Fatalf("FindGitRepoRoot failed: %v", err)
	}
	if got != root {
		t.Fatalf("expected repo root %q, got %q", root, got)
	}
}
