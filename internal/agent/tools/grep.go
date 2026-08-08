package tools

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const defaultMaxGrepResults = 100

type grepTool struct {
	cfg BuiltinConfig
}

func (t *grepTool) Name() string        { return "grep" }
func (t *grepTool) Description() string { return "Search file contents using regular expressions" }

func (t *grepTool) Schema() Schema {
	return Schema{
		Type: "object",
		Properties: map[string]Property{
			"pattern":     {Type: "string", Description: "Regular expression pattern to search for"},
			"path":        {Type: "string", Description: "File or directory to search (default: current directory)"},
			"glob":        {Type: "string", Description: "File filter glob pattern (e.g., \"*.go\")"},
			"context":     {Type: "integer", Description: "Lines of context around matches (default: 0)"},
			"max_results": {Type: "integer", Description: "Maximum number of matches (default: 100)"},
		},
		Required: []string{"pattern"},
	}
}

func (t *grepTool) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	var params struct {
		Pattern    string `json:"pattern"`
		Path       string `json:"path"`
		Glob       string `json:"glob"`
		Context    int    `json:"context"`
		MaxResults int    `json:"max_results"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return "", fmt.Errorf("parse input: %w", err)
	}

	re, err := regexp.Compile(params.Pattern)
	if err != nil {
		return fmt.Sprintf("Invalid regex: %s", err), nil
	}

	searchPath := params.Path
	if searchPath == "" {
		searchPath = "."
	}

	// Base boundary constraint
	resolvedPath, err := t.cfg.ResolveAndVerifyPath(searchPath)
	if err != nil {
		return fmt.Sprintf("Permission Denied: %v", err), nil
	}

	info, err := os.Stat(resolvedPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Sprintf("Path not found: %s", params.Path), nil
		}
		return "", fmt.Errorf("stat: %w", err)
	}

	maxResults := params.MaxResults
	if maxResults <= 0 {
		maxResults = defaultMaxGrepResults
	}

	var results []string
	matchCount := 0

	if !info.IsDir() {
		results, matchCount = grepFile(re, resolvedPath, params.Context, maxResults)
	} else {
		_ = filepath.WalkDir(resolvedPath, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil || d.IsDir() {
				return nil
			}
			if matchCount >= maxResults {
				return filepath.SkipAll
			}
			if params.Glob != "" {
				matched, _ := filepath.Match(params.Glob, filepath.Base(path))
				if !matched {
					return nil
				}
			}
			fileResults, count := grepFile(re, path, params.Context, maxResults-matchCount)
			results = append(results, fileResults...)
			matchCount += count
			return nil
		})
	}

	if len(results) == 0 {
		return fmt.Sprintf("No matches found for pattern: %s", params.Pattern), nil
	}

	return strings.Join(results, "\n"), nil
}

func grepFile(re *regexp.Regexp, path string, contextLines, maxResults int) ([]string, int) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var allLines []string
	for scanner.Scan() {
		allLines = append(allLines, scanner.Text())
	}

	var results []string
	matchCount := 0

	for i, line := range allLines {
		if matchCount >= maxResults {
			break
		}
		if re.MatchString(line) {
			// Context lines before
			for c := contextLines; c > 0; c-- {
				idx := i - c
				if idx >= 0 {
					results = append(results, fmt.Sprintf("%s-%d-%s", path, idx+1, allLines[idx]))
				}
			}
			results = append(results, fmt.Sprintf("%s:%d:%s", path, i+1, line))
			// Context lines after
			for c := 1; c <= contextLines; c++ {
				idx := i + c
				if idx < len(allLines) {
					results = append(results, fmt.Sprintf("%s-%d-%s", path, idx+1, allLines[idx]))
				}
			}
			matchCount++
		}
	}

	return results, matchCount
}
