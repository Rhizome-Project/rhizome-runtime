package sqlite

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestNewStoreHardensFileURIWithoutChangingExistingParent(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatalf("create shared parent: %v", err)
	}
	dbPath := filepath.Join(parent, "rhizome.db")
	if err := os.WriteFile(dbPath, nil, 0o644); err != nil {
		t.Fatalf("seed database file: %v", err)
	}

	store, err := NewStore("file:" + filepath.ToSlash(dbPath) + "?cache=shared")
	if err != nil {
		t.Fatalf("open file URI database: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close file URI database: %v", err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("stat file URI database: %v", err)
	}

	if runtime.GOOS == "windows" {
		return
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat database permissions: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("database mode = %o, want 600", got)
	}
	info, err = os.Stat(parent)
	if err != nil {
		t.Fatalf("stat shared parent permissions: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Fatalf("existing parent mode = %o, want 755", got)
	}
}

func TestSQLiteFilesystemPathClassifiesMemoryURIs(t *testing.T) {
	for _, dsn := range []string{
		":memory:",
		"file::memory:?cache=shared",
		"file:named-memory?mode=memory&cache=shared",
	} {
		path, fileBacked, err := sqliteFilesystemPath(dsn)
		if err != nil {
			t.Errorf("sqliteFilesystemPath(%q): %v", dsn, err)
			continue
		}
		if fileBacked || path != "" {
			t.Errorf("sqliteFilesystemPath(%q) = (%q, %v), want memory", dsn, path, fileBacked)
		}
	}
}
