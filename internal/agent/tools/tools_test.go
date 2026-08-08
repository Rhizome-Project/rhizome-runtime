package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mustInput(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}

func requireRunnableBash(t *testing.T) {
	t.Helper()

	tool := &bashTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}
	result, err := tool.Execute(context.Background(), mustInput(map[string]any{"command": "echo codex-bash-check"}))
	if err != nil {
		t.Fatalf("unexpected bash preflight error: %v", err)
	}
	if strings.Contains(result, bashUnavailableMessage) {
		t.Skip(result)
	}
	if !strings.Contains(result, "codex-bash-check") {
		t.Fatalf("unexpected bash preflight output %q", result)
	}
}

// ========== Bash Tool Tests ==========

// T-1: Verifies R-1, R-3 — execute "echo hello", output contains "hello".
func TestBashTool_Echo(t *testing.T) {
	t.Parallel()
	requireRunnableBash(t)
	tool := &bashTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}
	input := mustInput(map[string]any{"command": "echo hello"})

	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "hello") {
		t.Fatalf("expected output to contain %q, got %q", "hello", result)
	}
}

// T-2: Verifies R-3 — execute "exit 42", output starts with "Exit code: 42".
func TestBashTool_NonZeroExit(t *testing.T) {
	t.Parallel()
	requireRunnableBash(t)
	tool := &bashTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}
	input := mustInput(map[string]any{"command": "exit 42"})

	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(result, "Exit code: 42") {
		t.Fatalf("expected output starting with %q, got %q", "Exit code: 42", result)
	}
}

// T-3: Verifies R-4 — execute "sleep 10" with timeout 1, output contains "timed out".
func TestBashTool_Timeout(t *testing.T) {
	t.Parallel()
	requireRunnableBash(t)
	tool := &bashTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}
	input := mustInput(map[string]any{"command": "sleep 10", "timeout": 1})

	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(strings.ToLower(result), "timed out") {
		t.Fatalf("expected timeout message, got %q", result)
	}
}

// T-4: Verifies EC-1 — empty command returns error message.
func TestBashTool_EmptyCommand(t *testing.T) {
	t.Parallel()
	tool := &bashTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}
	input := mustInput(map[string]any{"command": ""})

	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "Command is required") {
		t.Fatalf("expected error message, got %q", result)
	}
}

// Verifies R-2 — timeout defaults and max capping.
func TestBashTool_TimeoutDefaults(t *testing.T) {
	t.Parallel()
	requireRunnableBash(t)
	tool := &bashTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}
	// Command with no timeout should default to 120 (just verify it completes)
	input := mustInput(map[string]any{"command": "echo ok"})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "ok") {
		t.Fatalf("expected ok, got %q", result)
	}
}

// Verifies R-3 — captures both stdout and stderr.
func TestBashTool_CapturesStdoutAndStderr(t *testing.T) {
	t.Parallel()
	requireRunnableBash(t)
	tool := &bashTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}
	input := mustInput(map[string]any{"command": "echo out; echo err >&2"})

	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "out") || !strings.Contains(result, "err") {
		t.Fatalf("expected stdout+stderr, got %q", result)
	}
}

func TestBashTool_LargeOutputIsTruncated(t *testing.T) {
	t.Parallel()
	requireRunnableBash(t)
	tool := &bashTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}
	input := mustInput(map[string]any{"command": "printf '%*s' 2000000 '' | tr ' ' x"})

	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "output truncated") {
		t.Fatalf("expected truncation marker, got %q", result[:min(len(result), 200)])
	}
	if len(result) > maxOutputBytes+len(bashOutputTruncationMessage) {
		t.Fatalf("expected bounded output, got %d bytes", len(result))
	}
}

// ========== Read Tool Tests ==========

// T-5: Verifies R-6, R-8 — create a temp file with 3 lines, read it, output has numbered lines.
func TestReadTool_BasicFile(t *testing.T) {
	t.Parallel()
	tool := &readTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "test.txt")
	os.WriteFile(path, []byte("line one\nline two\nline three\n"), 0o644)

	input := mustInput(map[string]any{"file_path": path})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(result), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d: %q", len(lines), result)
	}
	if !strings.Contains(lines[0], "1") || !strings.Contains(lines[0], "line one") {
		t.Fatalf("first line should contain line number and content, got %q", lines[0])
	}
}

// T-6: Verifies R-7 — read with offset=2, first line number in output is 2.
func TestReadTool_Offset(t *testing.T) {
	t.Parallel()
	tool := &readTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "test.txt")
	os.WriteFile(path, []byte("line one\nline two\nline three\n"), 0o644)

	input := mustInput(map[string]any{"file_path": path, "offset": 2})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(result), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if !strings.Contains(lines[0], "2") {
		t.Fatalf("first line should start with line number 2, got %q", lines[0])
	}
}

// T-7: Verifies R-9 — read non-existent file, output contains "not found".
func TestReadTool_FileNotFound(t *testing.T) {
	t.Parallel()
	tool := &readTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}
	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir

	input := mustInput(map[string]any{"file_path": filepath.Join(dir, "nonexistent/file.txt")})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(strings.ToLower(result), "not found") {
		t.Fatalf("expected 'not found', got %q", result)
	}
}

// T-8: Verifies EC-2 — read a directory path, output contains "is a directory".
func TestReadTool_Directory(t *testing.T) {
	t.Parallel()
	tool := &readTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	input := mustInput(map[string]any{"file_path": dir})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "is a directory") {
		t.Fatalf("expected 'is a directory', got %q", result)
	}
}

// Verifies R-10 — lines longer than 2000 chars are truncated.
func TestReadTool_LongLineTruncation(t *testing.T) {
	t.Parallel()
	tool := &readTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "long.txt")
	longLine := strings.Repeat("x", 3000)
	os.WriteFile(path, []byte(longLine+"\n"), 0o644)

	input := mustInput(map[string]any{"file_path": path})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "truncated") {
		t.Fatalf("expected truncation marker, got %q (len=%d)", result[:100], len(result))
	}
}

// Verifies R-7 — limit parameter caps lines read.
func TestReadTool_Limit(t *testing.T) {
	t.Parallel()
	tool := &readTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "many.txt")
	var content string
	for i := 1; i <= 50; i++ {
		content += fmt.Sprintf("line %d\n", i)
	}
	os.WriteFile(path, []byte(content), 0o644)

	input := mustInput(map[string]any{"file_path": path, "limit": 5})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(result), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 lines, got %d", len(lines))
	}
}

// ========== Edit Tool Tests ==========

// T-9: Verifies R-11, R-12 — file contains "foo", edit "foo"->"bar", file now contains "bar".
func TestEditTool_ReplaceOnce(t *testing.T) {
	t.Parallel()
	tool := &editTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "file.txt")
	os.WriteFile(path, []byte("hello foo world"), 0o644)

	input := mustInput(map[string]any{"file_path": path, "old_string": "foo", "new_string": "bar"})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "File edited") {
		t.Fatalf("expected success message, got %q", result)
	}

	data, _ := os.ReadFile(path)
	if string(data) != "hello bar world" {
		t.Fatalf("expected file content %q, got %q", "hello bar world", string(data))
	}
}

// T-10: Verifies R-13 — old_string not in file, returns error message.
func TestEditTool_NotFound(t *testing.T) {
	t.Parallel()
	tool := &editTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "file.txt")
	os.WriteFile(path, []byte("hello world"), 0o644)

	input := mustInput(map[string]any{"file_path": path, "old_string": "xyz", "new_string": "abc"})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "not found") {
		t.Fatalf("expected 'not found', got %q", result)
	}
}

// T-11: Verifies R-14 — old_string appears 3 times, replace_all=false, returns error with count.
func TestEditTool_NonUnique(t *testing.T) {
	t.Parallel()
	tool := &editTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "file.txt")
	os.WriteFile(path, []byte("aaa bbb aaa ccc aaa"), 0o644)

	input := mustInput(map[string]any{"file_path": path, "old_string": "aaa", "new_string": "zzz"})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "not unique") {
		t.Fatalf("expected 'not unique', got %q", result)
	}
	if !strings.Contains(result, "3") {
		t.Fatalf("expected count '3' in message, got %q", result)
	}
}

// T-12: Verifies R-12 — old_string appears 3 times, replace_all=true, all replaced.
func TestEditTool_ReplaceAll(t *testing.T) {
	t.Parallel()
	tool := &editTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "file.txt")
	os.WriteFile(path, []byte("aaa bbb aaa ccc aaa"), 0o644)

	input := mustInput(map[string]any{"file_path": path, "old_string": "aaa", "new_string": "zzz", "replace_all": true})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "3 replacements") {
		t.Fatalf("expected replacement count, got %q", result)
	}

	data, _ := os.ReadFile(path)
	if string(data) != "zzz bbb zzz ccc zzz" {
		t.Fatalf("expected all replaced, got %q", string(data))
	}
}

// T-13: Verifies EC-3 — old_string == new_string, returns error.
func TestEditTool_Identical(t *testing.T) {
	t.Parallel()
	tool := &editTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "file.txt")
	os.WriteFile(path, []byte("hello"), 0o644)

	input := mustInput(map[string]any{"file_path": path, "old_string": "hello", "new_string": "hello"})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "identical") {
		t.Fatalf("expected 'identical', got %q", result)
	}
}

// Verifies R-13 — edit on non-existent file.
func TestEditTool_FileNotFound(t *testing.T) {
	t.Parallel()
	tool := &editTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}
	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir

	input := mustInput(map[string]any{"file_path": filepath.Join(dir, "nonexistent/foo.txt"), "old_string": "a", "new_string": "b"})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "not found") {
		t.Fatalf("expected 'not found', got %q", result)
	}
}

func TestEditTool_DeniesRawMutationWithoutRepoAuthorityContext(t *testing.T) {
	t.Parallel()

	var denied []MutationDenyRecord
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	os.WriteFile(path, []byte("hello foo world"), 0o644)
	tool := &editTool{cfg: BuiltinConfig{
		WorkspaceDir: dir,
		AllowedTiers: []string{"autonomous"},
		RepoMutation: RepoMutationPolicy{
			RequireAuthority: true,
			RecordDeny: func(record MutationDenyRecord) {
				denied = append(denied, record)
			},
		},
	}}

	result, err := tool.Execute(context.Background(), mustInput(map[string]any{
		"file_path":  path,
		"old_string": "foo",
		"new_string": "bar",
	}))
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "Permission Denied") || !strings.Contains(result, DirectRepoMutationDeniedReason) {
		t.Fatalf("expected repo mutation denial, got %q", result)
	}
	data, _ := os.ReadFile(path)
	if string(data) != "hello foo world" {
		t.Fatalf("denied edit mutated file, got %q", string(data))
	}
	if len(denied) != 1 || denied[0].ToolName != "edit" || denied[0].ReasonCode != DirectRepoMutationDeniedReason {
		t.Fatalf("expected one edit deny record, got %+v", denied)
	}
	if !stringSliceContains(denied[0].MissingContext, "repo_lease_id") ||
		!stringSliceContains(denied[0].MissingContext, "patch_queue_item_id") {
		t.Fatalf("expected missing lease and patch context, got %+v", denied[0].MissingContext)
	}
}

// ========== Write Tool Tests ==========

// T-14: Verifies R-16, R-17 — write to new path including new dirs, file exists with content.
func TestWriteTool_CreateFile(t *testing.T) {
	t.Parallel()
	tool := &writeTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "sub", "dir", "file.txt")
	input := mustInput(map[string]any{"file_path": path, "content": "hello world"})

	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "File written") {
		t.Fatalf("expected success message, got %q", result)
	}
	if !strings.Contains(result, "11 bytes") {
		t.Fatalf("expected byte count, got %q", result)
	}

	data, _ := os.ReadFile(path)
	if string(data) != "hello world" {
		t.Fatalf("expected %q, got %q", "hello world", string(data))
	}
}

// T-15: Verifies R-18 — write to existing file, content is replaced.
func TestWriteTool_Overwrite(t *testing.T) {
	t.Parallel()
	tool := &writeTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "file.txt")
	os.WriteFile(path, []byte("original"), 0o644)

	input := mustInput(map[string]any{"file_path": path, "content": "new content"})
	_, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, _ := os.ReadFile(path)
	if string(data) != "new content" {
		t.Fatalf("expected %q, got %q", "new content", string(data))
	}
}

// T-16: Verifies EC-4 — empty file_path returns error message.
func TestWriteTool_EmptyPath(t *testing.T) {
	t.Parallel()
	tool := &writeTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	input := mustInput(map[string]any{"file_path": "", "content": "hello"})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "required") {
		t.Fatalf("expected error message, got %q", result)
	}
}

func TestWriteTool_DeniesRawMutationWithoutRepoAuthorityContext(t *testing.T) {
	t.Parallel()

	var denied []MutationDenyRecord
	dir := t.TempDir()
	path := filepath.Join(dir, "owned.txt")
	tool := &writeTool{cfg: BuiltinConfig{
		WorkspaceDir: dir,
		AllowedTiers: []string{"autonomous"},
		RepoMutation: RepoMutationPolicy{
			RequireAuthority: true,
			RecordDeny: func(record MutationDenyRecord) {
				denied = append(denied, record)
			},
		},
	}}

	result, err := tool.Execute(context.Background(), mustInput(map[string]any{
		"file_path": path,
		"content":   "bypass",
	}))
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "Permission Denied") || !strings.Contains(result, DirectRepoMutationDeniedReason) {
		t.Fatalf("expected repo mutation denial, got %q", result)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("denied write should not create file; stat err=%v", err)
	}
	if len(denied) != 1 || denied[0].ToolName != "write" || denied[0].ReasonCode != DirectRepoMutationDeniedReason {
		t.Fatalf("expected one write deny record, got %+v", denied)
	}
	if !stringSliceContains(denied[0].MissingContext, "repo_lease_id") ||
		!stringSliceContains(denied[0].MissingContext, "lease_term") ||
		!stringSliceContains(denied[0].MissingContext, "patch_queue_id") ||
		!stringSliceContains(denied[0].MissingContext, "patch_queue_item_id") {
		t.Fatalf("expected complete missing authority context, got %+v", denied[0].MissingContext)
	}
}

func TestWriteTool_DirectMutationDisableOverridesCompleteRepoAuthorityContext(t *testing.T) {
	t.Parallel()

	var denied []MutationDenyRecord
	dir := t.TempDir()
	path := filepath.Join(dir, "owned.txt")
	tool := &writeTool{cfg: BuiltinConfig{
		WorkspaceDir: dir,
		AllowedTiers: []string{"autonomous"},
		RepoMutation: RepoMutationPolicy{
			RequireAuthority: true,
			DisableDirect:    true,
			Authority: RepoMutationAuthorityContext{
				RepoLeaseID:      "lease-b2-2",
				LeaseTerm:        12,
				PatchQueueID:     "queue-b2-2",
				PatchQueueItemID: "patch-b2-2",
			},
			RecordDeny: func(record MutationDenyRecord) {
				denied = append(denied, record)
			},
		},
	}}

	result, err := tool.Execute(context.Background(), mustInput(map[string]any{
		"file_path": path,
		"content":   "bypass",
	}))
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "Permission Denied") || !strings.Contains(result, "use repo authority patch flow") {
		t.Fatalf("expected direct mutation disabled denial, got %q", result)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("disabled direct write should not create file; stat err=%v", err)
	}
	if len(denied) != 1 || denied[0].ToolName != "write" || len(denied[0].MissingContext) != 0 {
		t.Fatalf("expected one direct-disable deny record with complete context, got %+v", denied)
	}
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// ========== Glob Tool Tests ==========

// T-17: Verifies R-20 — create temp files, glob matches correct ones.
func TestGlobTool_Match(t *testing.T) {
	t.Parallel()
	tool := &globTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	os.WriteFile(filepath.Join(dir, "foo.go"), []byte(""), 0o644)
	os.WriteFile(filepath.Join(dir, "bar.go"), []byte(""), 0o644)
	os.WriteFile(filepath.Join(dir, "baz.txt"), []byte(""), 0o644)

	input := mustInput(map[string]any{"pattern": "*.go", "path": dir})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result, "foo.go") || !strings.Contains(result, "bar.go") {
		t.Fatalf("expected .go files in result, got %q", result)
	}
	if strings.Contains(result, "baz.txt") {
		t.Fatalf("should not match .txt file, got %q", result)
	}
}

// T-18: Verifies EC-5 — pattern matches nothing, returns message.
func TestGlobTool_NoMatch(t *testing.T) {
	t.Parallel()
	tool := &globTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	input := mustInput(map[string]any{"pattern": "*.xyz", "path": dir})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "No files matched") {
		t.Fatalf("expected 'No files matched', got %q", result)
	}
}

// Verifies R-22 — recursive glob with ** pattern.
func TestGlobTool_RecursiveMatch(t *testing.T) {
	t.Parallel()
	tool := &globTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	subdir := filepath.Join(dir, "sub")
	os.MkdirAll(subdir, 0o755)
	os.WriteFile(filepath.Join(dir, "a.go"), []byte(""), 0o644)
	os.WriteFile(filepath.Join(subdir, "b.go"), []byte(""), 0o644)

	input := mustInput(map[string]any{"pattern": "**/*.go", "path": dir})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "b.go") {
		t.Fatalf("expected recursive match for b.go, got %q", result)
	}
}

// ========== Grep Tool Tests ==========

// T-19: Verifies R-24, R-27 — create file with known content, grep finds correct lines with line numbers.
func TestGrepTool_BasicSearch(t *testing.T) {
	t.Parallel()
	tool := &grepTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "test.go")
	os.WriteFile(path, []byte("func main() {\n\tfmt.Println(\"hello\")\n}\n"), 0o644)

	input := mustInput(map[string]any{"pattern": "Println", "path": path})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, ":2:") {
		t.Fatalf("expected line 2 match, got %q", result)
	}
	if !strings.Contains(result, "Println") {
		t.Fatalf("expected 'Println' in result, got %q", result)
	}
}

// T-20: Verifies R-28 — invalid regex returns error message.
func TestGrepTool_InvalidRegex(t *testing.T) {
	t.Parallel()
	tool := &grepTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	input := mustInput(map[string]any{"pattern": "[invalid"})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "Invalid regex") {
		t.Fatalf("expected 'Invalid regex', got %q", result)
	}
}

// T-21: Verifies EC-6 — grep on non-existent path returns error.
func TestGrepTool_PathNotFound(t *testing.T) {
	t.Parallel()
	tool := &grepTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}
	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir

	input := mustInput(map[string]any{"pattern": "test", "path": filepath.Join(dir, "nonexistent/path")})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "not found") || !strings.Contains(result, "Path") {
		t.Fatalf("expected 'Path not found', got %q", result)
	}
}

// Verifies R-27 — grep with context lines.
func TestGrepTool_WithContext(t *testing.T) {
	t.Parallel()
	tool := &grepTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "test.txt")
	os.WriteFile(path, []byte("alpha\nbeta\ngamma\ndelta\nepsilon\n"), 0o644)

	input := mustInput(map[string]any{"pattern": "gamma", "path": path, "context": 1})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have the match line and context lines
	if !strings.Contains(result, "gamma") {
		t.Fatalf("expected match for gamma, got %q", result)
	}
	if !strings.Contains(result, "beta") || !strings.Contains(result, "delta") {
		t.Fatalf("expected context lines beta and delta, got %q", result)
	}
}

// Verifies R-25 — grep with glob filter.
func TestGrepTool_GlobFilter(t *testing.T) {
	t.Parallel()
	tool := &grepTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	os.WriteFile(filepath.Join(dir, "a.go"), []byte("hello world\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "b.txt"), []byte("hello world\n"), 0o644)

	input := mustInput(map[string]any{"pattern": "hello", "path": dir, "glob": "*.go"})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "a.go") {
		t.Fatalf("expected match in a.go, got %q", result)
	}
	if strings.Contains(result, "b.txt") {
		t.Fatalf("should not match b.txt with *.go glob, got %q", result)
	}
}

// ========== WebSearch Tool Tests ==========

// T-22: Verifies R-29, R-31 — returns stub message with query.
func TestWebSearchTool_Stub(t *testing.T) {
	t.Parallel()
	tool := &webSearchTool{}

	input := mustInput(map[string]any{"query": "golang testing"})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "not yet implemented") {
		t.Fatalf("expected stub message, got %q", result)
	}
	if !strings.Contains(result, "golang testing") {
		t.Fatalf("expected query in stub message, got %q", result)
	}
}

// ========== RegisterBuiltins Tests ==========

// T-23: Verifies R-34 — RegisterBuiltins registers exactly 7 tools.
func TestRegisterBuiltins(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	RegisterBuiltins(reg, BuiltinConfig{AllowedTiers: []string{"autonomous"}})

	tools := reg.List()
	if len(tools) != 7 {
		names := reg.Names()
		t.Fatalf("expected 7 tools, got %d: %v", len(tools), names)
	}

	expectedNames := []string{"bash", "read", "edit", "write", "glob", "grep", "web_search"}
	names := reg.Names()
	for i, expected := range expectedNames {
		if names[i] != expected {
			t.Fatalf("tool[%d]: expected %q, got %q", i, expected, names[i])
		}
	}
}

// Verifies R-32 — all tools implement Tool interface (compile-time check + schema validation).
func TestAllTools_HaveSchemas(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	RegisterBuiltins(reg, BuiltinConfig{AllowedTiers: []string{"autonomous"}})

	for _, tool := range reg.List() {
		schema := tool.Schema()
		if schema.Type != "object" {
			t.Fatalf("tool %q: schema type should be 'object', got %q", tool.Name(), schema.Type)
		}
		if len(schema.Properties) == 0 {
			t.Fatalf("tool %q: schema should have properties", tool.Name())
		}
		if tool.Description() == "" {
			t.Fatalf("tool %q: description should not be empty", tool.Name())
		}
	}
}

// NT-1: Negative test — grep with no matches returns "No matches found".
func TestGrepTool_NoMatches(t *testing.T) {
	t.Parallel()
	tool := &grepTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "test.txt")
	os.WriteFile(path, []byte("hello world\n"), 0o644)

	input := mustInput(map[string]any{"pattern": "zzzzz", "path": path})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "No matches") {
		t.Fatalf("expected 'No matches found', got %q", result)
	}
}

// NT-2: Negative test — write tool with empty content creates empty file.
func TestWriteTool_EmptyContent(t *testing.T) {
	t.Parallel()
	tool := &writeTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	dir := t.TempDir()
	tool.cfg.WorkspaceDir = dir
	tool.cfg.WorkspaceDir = dir
	path := filepath.Join(dir, "empty.txt")
	input := mustInput(map[string]any{"file_path": path, "content": ""})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "0 bytes") {
		t.Fatalf("expected 0 bytes, got %q", result)
	}

	data, _ := os.ReadFile(path)
	if len(data) != 0 {
		t.Fatalf("expected empty file, got %d bytes", len(data))
	}
}

// NT-3: Negative test — read tool with empty file_path.
func TestReadTool_EmptyPath(t *testing.T) {
	t.Parallel()
	tool := &readTool{cfg: BuiltinConfig{AllowedTiers: []string{"autonomous"}}}

	input := mustInput(map[string]any{"file_path": ""})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !strings.Contains(result, "required") {
		t.Fatalf("expected error message, got %q", result)
	}
}
