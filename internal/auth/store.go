package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrNoCredentials is returned when no credentials file exists.
var ErrNoCredentials = errors.New("no stored credentials")

// Credentials holds API authentication data persisted to disk.
type Credentials struct {
	APIKey     string `json:"api_key"`
	Provider   string `json:"provider"`
	Email      string `json:"email,omitempty"`
	ObtainedAt string `json:"obtained_at"`
}

// DefaultAuthFilePath returns the default path for the credentials file:
// ~/.rhizome/auth.json. If the home directory cannot be determined, it
// returns an empty string.
func DefaultAuthFilePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".rhizome", "auth.json")
}

// SaveCredentials writes creds to path as indented JSON. The parent directory
// is created with mode 0700 if it does not exist, and the file is written
// with mode 0600.
func SaveCredentials(path string, creds Credentials) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("creating auth directory: %w", err)
	}

	data, err := json.MarshalIndent(creds, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling credentials: %w", err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("writing credentials file: %w", err)
	}

	return nil
}

// LoadCredentials reads and deserializes credentials from path. If the file
// does not exist, it returns ErrNoCredentials.
func LoadCredentials(path string) (*Credentials, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNoCredentials
		}
		return nil, fmt.Errorf("reading credentials file: %w", err)
	}

	var creds Credentials
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, fmt.Errorf("parsing credentials file: %w", err)
	}

	return &creds, nil
}

// ClearCredentials removes the credentials file at path. If the file does not
// exist, it returns nil (idempotent).
func ClearCredentials(path string) error {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing credentials file: %w", err)
	}
	return nil
}
