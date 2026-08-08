package fssecure

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPrivatePermissionsOnPOSIX(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not enforced on Windows")
	}

	root := t.TempDir()
	dir := filepath.Join(root, "private")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("seed directory: %v", err)
	}
	if err := EnsurePrivateDir(dir); err != nil {
		t.Fatalf("secure directory: %v", err)
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode = %v, err=%v; want 0700", infoMode(info), err)
	}

	path := filepath.Join(dir, "secret.log")
	if err := os.WriteFile(path, []byte("secret"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	file, err := OpenPrivateFile(path, os.O_WRONLY|os.O_APPEND)
	if err != nil {
		t.Fatalf("open private file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close private file: %v", err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %v, err=%v; want 0600", infoMode(info), err)
	}
}

func TestEnsurePrivateParentDirPreservesExistingDirectoryModeOnPOSIX(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not enforced on Windows")
	}

	dir := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("create shared directory: %v", err)
	}
	if err := EnsurePrivateParentDir(dir); err != nil {
		t.Fatalf("ensure existing parent directory: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat shared directory: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Fatalf("existing parent mode = %o, want 755", got)
	}

	created := filepath.Join(t.TempDir(), "private")
	if err := EnsurePrivateParentDir(created); err != nil {
		t.Fatalf("create private parent directory: %v", err)
	}
	info, err = os.Stat(created)
	if err != nil {
		t.Fatalf("stat created directory: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("created parent mode = %o, want 700", got)
	}
}

func infoMode(info os.FileInfo) os.FileMode {
	if info == nil {
		return 0
	}
	return info.Mode().Perm()
}
