package tools

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
)

type readTool struct {
	cfg BuiltinConfig
}

func (t *readTool) Name() string { return "read" }
func (t *readTool) Description() string {
	return "Read a file and return its contents with line numbers"
}

func (t *readTool) Schema() Schema {
	return Schema{
		Type: "object",
		Properties: map[string]Property{
			"file_path": {Type: "string", Description: "Absolute path to the file to read"},
			"offset":    {Type: "integer", Description: "1-based line number to start from"},
			"limit":     {Type: "integer", Description: "Maximum lines to read (default 2000)"},
		},
		Required: []string{"file_path"},
	}
}

func (t *readTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		FilePath string `json:"file_path"`
		Offset   int    `json:"offset"`
		Limit    int    `json:"limit"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("parse input: %w", err)
	}

	if params.FilePath == "" {
		return "file_path is required", nil
	}

	// Scope boundary constraint
	resolvedPath, err := t.cfg.ResolveAndVerifyPath(params.FilePath)
	if err != nil {
		return fmt.Sprintf("Permission Denied: %v", err), nil
	}

	info, err := os.Stat(resolvedPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Sprintf("File not found: %s", params.FilePath), nil
		}
		return "", fmt.Errorf("stat: %w", err)
	}
	if info.IsDir() {
		return fmt.Sprintf("Path is a directory: %s", params.FilePath), nil
	}

	f, err := os.Open(resolvedPath)
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	offset := params.Offset
	if offset < 1 {
		offset = 1
	}
	limit := params.Limit
	if limit <= 0 {
		limit = 2000
	}

	var result []byte
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNum := 0
	linesRead := 0

	for scanner.Scan() {
		lineNum++
		if lineNum < offset {
			continue
		}
		if linesRead >= limit {
			break
		}

		line := scanner.Text()
		if len(line) > 2000 {
			line = line[:2000] + "... (truncated)"
		}

		result = append(result, fmt.Appendf(nil, "%6d\t%s\n", lineNum, line)...)
		linesRead++
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan: %w", err)
	}

	return string(result), nil
}
