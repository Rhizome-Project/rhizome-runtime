package repoauthority

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestPathResolverBindsToCanonicalRepoRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "internal", "repoauthority"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	target := filepath.Join(root, "internal", "repoauthority", "path_resolver.go")
	if err := os.WriteFile(target, []byte("package repoauthority\n"), 0o644); err != nil {
		t.Fatalf("write target: %v", err)
	}

	resolver, err := NewPathResolver(root)
	if err != nil {
		t.Fatalf("NewPathResolver: %v", err)
	}
	wantRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("EvalSymlinks(root): %v", err)
	}
	if resolver.RepoRoot() != filepath.Clean(wantRoot) {
		t.Fatalf("RepoRoot = %q, want %q", resolver.RepoRoot(), filepath.Clean(wantRoot))
	}

	resolved, err := resolver.Resolve("internal/repoauthority/path_resolver.go")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.RepoRoot != resolver.RepoRoot() {
		t.Fatalf("resolved root = %q, want %q", resolved.RepoRoot, resolver.RepoRoot())
	}
	if resolved.RelPath != "internal/repoauthority/path_resolver.go" {
		t.Fatalf("RelPath = %q", resolved.RelPath)
	}
	if resolved.AbsPath != filepath.Clean(target) {
		t.Fatalf("AbsPath = %q, want %q", resolved.AbsPath, filepath.Clean(target))
	}
	if !resolved.Existing {
		t.Fatalf("expected existing path: %+v", resolved)
	}
	if resolved.ExistingAncestor != filepath.Clean(target) {
		t.Fatalf("ExistingAncestor = %q, want %q", resolved.ExistingAncestor, filepath.Clean(target))
	}
}

func TestPathResolverNormalizesSingleRepoRelativePath(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "a", "b"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	resolver := newTestPathResolver(t, root)

	resolved, err := resolver.Resolve("./a//b\\new.go")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.RelPath != "a/b/new.go" {
		t.Fatalf("RelPath = %q, want a/b/new.go", resolved.RelPath)
	}
	if resolved.Existing {
		t.Fatalf("new file should not be marked existing: %+v", resolved)
	}
	if resolved.ExistingAncestor != filepath.Join(resolver.RepoRoot(), "a", "b") {
		t.Fatalf("ExistingAncestor = %q", resolved.ExistingAncestor)
	}
}

func TestPathResolverRejectsBypassInputs(t *testing.T) {
	resolver := newTestPathResolver(t, t.TempDir())
	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "empty", path: "", want: "path is required"},
		{name: "spaces", path: "   ", want: "path is required"},
		{name: "dot", path: ".", want: "path is required"},
		{name: "parent traversal", path: "../outside.go", want: "path traversal is not allowed"},
		{name: "nested traversal", path: "a/../outside.go", want: "path traversal is not allowed"},
		{name: "absolute slash", path: "/outside.go", want: "absolute paths are not allowed"},
		{name: "leading backslash", path: "\\outside.go", want: "absolute paths are not allowed"},
		{name: "windows drive", path: "C:\\outside.go", want: "absolute paths are not allowed"},
		{name: "windows drive relative", path: "C:outside.go", want: "absolute paths are not allowed"},
		{name: "unc", path: "\\\\server\\share\\outside.go", want: "absolute paths are not allowed"},
		{name: "nul", path: "a\x00b.go", want: "path contains NUL byte"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resolver.Resolve(tt.path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Resolve(%q) error = %v, want containing %q", tt.path, err, tt.want)
			}
		})
	}
}

func TestPathResolverRejectsDuplicateAndUnstablePathsets(t *testing.T) {
	resolver := newTestPathResolver(t, t.TempDir())
	tests := []struct {
		name  string
		paths []string
		want  string
	}{
		{name: "empty", paths: nil, want: "pathset is required"},
		{name: "unsorted", paths: []string{"b.go", "a.go"}, want: "pathset is not sorted"},
		{name: "duplicate", paths: []string{"a.go", "a.go"}, want: "duplicate"},
		{name: "dot unstable", paths: []string{"./a.go"}, want: "not normalized"},
		{name: "slash unstable", paths: []string{"a//b.go"}, want: "not normalized"},
		{name: "backslash unstable", paths: []string{"a\\b.go"}, want: "not normalized"},
		{name: "traversal", paths: []string{"a/../b.go"}, want: "path traversal is not allowed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resolver.ResolvePathSet(tt.paths)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ResolvePathSet(%#v) error = %v, want containing %q", tt.paths, err, tt.want)
			}
		})
	}
}

func TestPathResolverResolvesStablePathset(t *testing.T) {
	resolver := newTestPathResolver(t, t.TempDir())
	resolved, err := resolver.ResolvePathSet([]string{"a.go", "dir/b.go"})
	if err != nil {
		t.Fatalf("ResolvePathSet: %v", err)
	}
	if len(resolved) != 2 {
		t.Fatalf("resolved len = %d", len(resolved))
	}
	if resolved[0].RelPath != "a.go" || resolved[1].RelPath != "dir/b.go" {
		t.Fatalf("unexpected resolved pathset: %+v", resolved)
	}
}

func TestPathResolverRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "outside.go"), []byte("package outside\n"), 0o644); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	linkPath := filepath.Join(root, "link")
	if err := os.Symlink(outside, linkPath); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("creating symlink requires privileges on this Windows host: %v", err)
		}
		t.Fatalf("symlink: %v", err)
	}

	resolver := newTestPathResolver(t, root)
	_, err := resolver.Resolve("link/outside.go")
	if err == nil || !strings.Contains(err.Error(), "symlink path component is not allowed") {
		t.Fatalf("Resolve symlink error = %v, want symlink rejection", err)
	}
}

func TestPathResolverRejectsNestedGitBoundary(t *testing.T) {
	root := t.TempDir()
	submodule := filepath.Join(root, "vendor", "module")
	if err := os.MkdirAll(submodule, 0o755); err != nil {
		t.Fatalf("mkdir submodule: %v", err)
	}
	if err := os.WriteFile(filepath.Join(submodule, ".git"), []byte("gitdir: ../.git/modules/vendor/module\n"), 0o644); err != nil {
		t.Fatalf("write .git marker: %v", err)
	}

	resolver := newTestPathResolver(t, root)
	_, err := resolver.Resolve("vendor/module/file.go")
	if err == nil || !strings.Contains(err.Error(), "nested git repository boundary") {
		t.Fatalf("Resolve nested git boundary error = %v, want boundary rejection", err)
	}
}

func TestPathResolverRejectsFileAsParent(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.go"), []byte("package main\n"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	resolver := newTestPathResolver(t, root)
	_, err := resolver.Resolve("file.go/nested.go")
	if err == nil || !strings.Contains(err.Error(), "path component is not a directory") {
		t.Fatalf("Resolve file parent error = %v, want non-directory rejection", err)
	}
}

func newTestPathResolver(t *testing.T, root string) PathResolver {
	t.Helper()
	resolver, err := NewPathResolver(root)
	if err != nil {
		t.Fatalf("NewPathResolver(%q): %v", root, err)
	}
	return resolver
}
