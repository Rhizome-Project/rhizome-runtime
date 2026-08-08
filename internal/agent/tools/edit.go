package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type editTool struct {
	cfg BuiltinConfig
}

func (t *editTool) Name() string        { return "edit" }
func (t *editTool) Description() string { return "Perform exact string replacement in a file" }

func (t *editTool) Schema() Schema {
	return Schema{
		Type: "object",
		Properties: map[string]Property{
			"file_path":   {Type: "string", Description: "Path to the file to edit"},
			"old_string":  {Type: "string", Description: "The exact string to find and replace"},
			"new_string":  {Type: "string", Description: "The replacement string"},
			"replace_all": {Type: "boolean", Description: "Replace all occurrences (default false)"},
		},
		Required: []string{"file_path", "old_string", "new_string"},
	}
}

func (t *editTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		FilePath   string `json:"file_path"`
		OldString  string `json:"old_string"`
		NewString  string `json:"new_string"`
		ReplaceAll bool   `json:"replace_all"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("parse input: %w", err)
	}

	if params.OldString == params.NewString {
		return "old_string and new_string are identical", nil
	}
	if err := t.cfg.EnsureDirectMutationAllowed(t.Name(), params.FilePath); err != nil {
		return fmt.Sprintf("Permission Denied: %v", err), nil
	}

	// Scope boundary constraint
	resolvedPath, err := t.cfg.ResolveAndVerifyPath(params.FilePath)
	if err != nil {
		return fmt.Sprintf("Permission Denied: %v", err), nil
	}

	data, err := os.ReadFile(resolvedPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Sprintf("File not found: %s", params.FilePath), nil
		}
		return "", fmt.Errorf("read file: %w", err)
	}

	content := string(data)
	count := strings.Count(content, params.OldString)

	if count == 0 {
		return fmt.Sprintf("old_string not found in %s", params.FilePath), nil
	}

	if count > 1 && !params.ReplaceAll {
		return fmt.Sprintf("old_string is not unique in %s. Found %d occurrences. Use replace_all or provide more context.", params.FilePath, count), nil
	}

	var newContent string
	if params.ReplaceAll {
		newContent = strings.ReplaceAll(content, params.OldString, params.NewString)
	} else {
		newContent = strings.Replace(content, params.OldString, params.NewString, 1)
	}

	if err := os.WriteFile(resolvedPath, []byte(newContent), 0o644); err != nil {
		return "", fmt.Errorf("write file: %w", err)
	}

	if params.ReplaceAll && count > 1 {
		return fmt.Sprintf("File edited: %s (%d replacements)", params.FilePath, count), nil
	}
	return fmt.Sprintf("File edited: %s", params.FilePath), nil
}
