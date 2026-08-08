package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type writeTool struct {
	cfg BuiltinConfig
}

func (t *writeTool) Name() string        { return "write" }
func (t *writeTool) Description() string { return "Create or overwrite a file with given content" }

func (t *writeTool) Schema() Schema {
	return Schema{
		Type: "object",
		Properties: map[string]Property{
			"file_path": {Type: "string", Description: "Path to the file to write"},
			"content":   {Type: "string", Description: "Content to write to the file"},
		},
		Required: []string{"file_path", "content"},
	}
}

func (t *writeTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		FilePath string `json:"file_path"`
		Content  string `json:"content"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("parse input: %w", err)
	}

	if params.FilePath == "" {
		return "file_path is required", nil
	}
	if err := t.cfg.EnsureDirectMutationAllowed(t.Name(), params.FilePath); err != nil {
		return fmt.Sprintf("Permission Denied: %v", err), nil
	}

	// Scope boundary constraint
	resolvedPath, err := t.cfg.ResolveAndVerifyPath(params.FilePath)
	if err != nil {
		return fmt.Sprintf("Permission Denied: %v", err), nil
	}

	dir := filepath.Dir(resolvedPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create dirs: %w", err)
	}

	if err := os.WriteFile(resolvedPath, []byte(params.Content), 0o644); err != nil {
		return "", fmt.Errorf("write file: %w", err)
	}

	return fmt.Sprintf("File written: %s (%d bytes)", params.FilePath, len(params.Content)), nil
}
