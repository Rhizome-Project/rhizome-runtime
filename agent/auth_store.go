package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const configDir = ".config/rhizome-bot"

func keyPath() string {
	return agentRuntimeConfigPath("openai_key")
}

func keyPathForRoot(root string) string {
	root = cleanManagedRuntimeRoot(root)
	if root == "" {
		return ""
	}
	return filepath.Join(root, "openai_key")
}

func SaveKey(key string) error {
	path := keyPath()
	if path == "" {
		return fmt.Errorf("agent config root is unavailable")
	}
	return writeManagerStateBytes("save_openai_key", path, []byte(key), 0o600)
}

func LoadSavedKey() string {
	path := keyPath()
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func LoadSavedKeyFromRoot(root string) string {
	path := keyPathForRoot(root)
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}
