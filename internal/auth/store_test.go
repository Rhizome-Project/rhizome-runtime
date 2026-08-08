package auth

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func sampleCredentials() Credentials {
	return Credentials{
		APIKey:     "sk-test-key-123",
		Provider:   "openai",
		Email:      "user@example.com",
		ObtainedAt: "2026-03-15T10:00:00Z",
	}
}

// T-1: Save credentials, load them back, assert equality.
func TestSaveAndLoadCredentials(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")

	creds := sampleCredentials()
	if err := SaveCredentials(path, creds); err != nil {
		t.Fatalf("SaveCredentials failed: %v", err)
	}

	loaded, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials failed: %v", err)
	}

	if loaded.APIKey != creds.APIKey {
		t.Errorf("APIKey: got %q, want %q", loaded.APIKey, creds.APIKey)
	}
	if loaded.Provider != creds.Provider {
		t.Errorf("Provider: got %q, want %q", loaded.Provider, creds.Provider)
	}
	if loaded.Email != creds.Email {
		t.Errorf("Email: got %q, want %q", loaded.Email, creds.Email)
	}
	if loaded.ObtainedAt != creds.ObtainedAt {
		t.Errorf("ObtainedAt: got %q, want %q", loaded.ObtainedAt, creds.ObtainedAt)
	}
}

// T-2: Load from non-existent file returns ErrNoCredentials.
func TestLoadCredentials_FileNotExists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no-such-file.json")

	creds, err := LoadCredentials(path)
	if creds != nil {
		t.Errorf("expected nil credentials, got %+v", creds)
	}
	if !errors.Is(err, ErrNoCredentials) {
		t.Errorf("expected ErrNoCredentials, got %v", err)
	}
}

// T-3: Save to a deeply nested path creates parent directories.
func TestSaveCredentials_CreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a", "b", "c", "auth.json")

	if err := SaveCredentials(path, sampleCredentials()); err != nil {
		t.Fatalf("SaveCredentials failed: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file not created: %v", err)
	}
}

// T-4: File is created with mode 0600.
func TestSaveCredentials_FilePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file permission checks not reliable on Windows")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")

	if err := SaveCredentials(path, sampleCredentials()); err != nil {
		t.Fatalf("SaveCredentials failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	mode := info.Mode().Perm()
	if mode != 0600 {
		t.Errorf("file mode: got %o, want 0600", mode)
	}
}

// T-5: Clear removes an existing file.
func TestClearCredentials_Exists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")

	if err := SaveCredentials(path, sampleCredentials()); err != nil {
		t.Fatalf("SaveCredentials failed: %v", err)
	}

	if err := ClearCredentials(path); err != nil {
		t.Fatalf("ClearCredentials failed: %v", err)
	}

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file still exists after ClearCredentials")
	}
}

// T-6: Clear on non-existent file returns nil.
func TestClearCredentials_NotExists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no-such-file.json")

	if err := ClearCredentials(path); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

// T-7: Load from file with invalid JSON returns a descriptive error.
func TestLoadCredentials_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")

	if err := os.WriteFile(path, []byte("{not valid json"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	creds, err := LoadCredentials(path)
	if creds != nil {
		t.Errorf("expected nil credentials, got %+v", creds)
	}
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if errors.Is(err, ErrNoCredentials) {
		t.Error("should not return ErrNoCredentials for invalid JSON")
	}
}

// Verify DefaultAuthFilePath returns a non-empty string ending with auth.json.
func TestDefaultAuthFilePath(t *testing.T) {
	path := DefaultAuthFilePath()
	if path == "" {
		t.Skip("could not determine home directory")
	}
	if filepath.Base(path) != "auth.json" {
		t.Errorf("expected path to end with auth.json, got %q", path)
	}
}
