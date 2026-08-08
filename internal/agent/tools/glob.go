package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const maxGlobResults = 1000

type globTool struct {
	cfg BuiltinConfig
}

func (t *globTool) Name() string        { return "glob" }
func (t *globTool) Description() string { return "Find files matching a glob pattern" }

func (t *globTool) Schema() Schema {
	return Schema{
		Type: "object",
		Properties: map[string]Property{
			"pattern": {Type: "string", Description: "Glob pattern (supports **)"},
			"path":    {Type: "string", Description: "Base directory (default: current directory)"},
		},
		Required: []string{"pattern"},
	}
}

func (t *globTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Pattern string `json:"pattern"`
		Path    string `json:"path"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("parse input: %w", err)
	}

	basePath := params.Path
	if basePath == "" {
		basePath = "."
	}

	// Base boundary constraint
	resolvedBase, err := t.cfg.ResolveAndVerifyPath(basePath)
	if err != nil {
		return fmt.Sprintf("Permission Denied: %v", err), nil
	}

	var matches []string

	if strings.Contains(params.Pattern, "**") {
		matches = recursiveGlob(resolvedBase, params.Pattern)
	} else {
		pattern := filepath.Join(resolvedBase, params.Pattern)
		m, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Sprintf("Invalid pattern: %s", err), nil
		}
		matches = m
	}

	// Enforce constraint on all results
	var safeMatches []string
	for _, m := range matches {
		if _, err := t.cfg.ResolveAndVerifyPath(m); err == nil {
			safeMatches = append(safeMatches, m)
		}
	}
	matches = safeMatches

	if len(matches) == 0 {
		return fmt.Sprintf("No files matched pattern (or files were outside workspace boundary): %s", params.Pattern), nil
	}

	total := len(matches)
	if total > maxGlobResults {
		matches = matches[:maxGlobResults]
	}

	result := strings.Join(matches, "\n")
	if total > maxGlobResults {
		result += fmt.Sprintf("\n... and %d more files", total-maxGlobResults)
	}
	return result, nil
}

func recursiveGlob(basePath, pattern string) []string {
	// Split pattern on ** to get prefix and suffix
	parts := strings.SplitN(pattern, "**", 2)
	prefix := parts[0]
	suffix := ""
	if len(parts) > 1 {
		suffix = strings.TrimPrefix(parts[1], "/")
	}

	var matches []string
	root := filepath.Join(basePath, prefix)

	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if suffix == "" {
			matches = append(matches, path)
			return nil
		}
		matched, _ := filepath.Match(suffix, filepath.Base(path))
		if matched {
			matches = append(matches, path)
		}
		return nil
	})
	return matches
}
