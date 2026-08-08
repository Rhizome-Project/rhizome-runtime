package main

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSafePathRejectsTraversalAndAllowsWorkspacePaths(t *testing.T) {
	workdir := t.TempDir()

	got, err := safePath(workdir, filepath.Join("nested", "file.txt"))
	if err != nil {
		t.Fatalf("safePath() unexpected error: %v", err)
	}
	rel, err := filepath.Rel(workdir, got)
	if err != nil {
		t.Fatalf("filepath.Rel: %v", err)
	}
	if strings.HasPrefix(rel, "..") {
		t.Fatalf("safePath() escaped workspace: %q", got)
	}

	if _, err := safePath(workdir, ".."+string(os.PathSeparator)+"escape.txt"); err == nil {
		t.Fatal("safePath() accepted path traversal")
	}

	sibling := filepath.Join("..", filepath.Base(workdir)+"-other", "file.txt")
	if _, err := safePath(workdir, sibling); err == nil {
		t.Fatal("safePath() accepted sibling-prefix path outside the workspace")
	}
}

func TestSafePathRejectsSymlinkEscape(t *testing.T) {
	workdir := t.TempDir()
	outside := t.TempDir()

	linkPath := filepath.Join(workdir, "link")
	if err := os.Symlink(outside, linkPath); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}

	if _, err := safePath(workdir, filepath.Join("link", "escape.txt")); err == nil {
		t.Fatal("safePath() accepted a symlink escape")
	}
}

func TestFileToolsReadWriteAndList(t *testing.T) {
	workdir := t.TempDir()

	writeTool := NewWriteFileTool(workdir)
	readTool := NewReadFileTool(workdir)
	listTool := NewListDirectoryTool(workdir)

	if got := writeTool.Execute(context.Background(), map[string]any{"path": "dir/file.txt", "content": "hello"}); got == nil || got.IsError {
		t.Fatalf("WriteFileTool.Execute() failed: %+v", got)
	}

	if got := readTool.Execute(context.Background(), map[string]any{"path": "dir/file.txt"}); got == nil || got.IsError || got.Output != "hello" {
		t.Fatalf("ReadFileTool.Execute() = %+v", got)
	}

	if got := listTool.Execute(context.Background(), map[string]any{"path": "dir"}); got == nil || got.IsError || !strings.Contains(got.Output, "file.txt") {
		t.Fatalf("ListDirectoryTool.Execute() = %+v", got)
	}
}

func TestWriteFileToolAcceptsBase64Content(t *testing.T) {
	workdir := t.TempDir()
	writeTool := NewWriteFileTool(workdir)
	readTool := NewReadFileTool(workdir)
	content := "package lexer\n\nconst sample = `quoted \"source\" with \\\\ escapes`\n"
	encoded := base64.StdEncoding.EncodeToString([]byte(content))

	if got := writeTool.Execute(context.Background(), map[string]any{"path": "src/lexer.go", "content_base64": encoded}); got == nil || got.IsError {
		t.Fatalf("WriteFileTool.Execute(content_base64) failed: %+v", got)
	}
	if got := readTool.Execute(context.Background(), map[string]any{"path": "src/lexer.go"}); got == nil || got.IsError || got.Output != content {
		t.Fatalf("ReadFileTool.Execute() = %+v", got)
	}
	if got := writeTool.Execute(context.Background(), map[string]any{"path": "src/bad.go", "content": "plain", "content_base64": encoded}); got == nil || !got.IsError || !strings.Contains(got.Output, "either content or content_base64") {
		t.Fatalf("expected content/content_base64 ambiguity to fail, got %+v", got)
	}
	if got := writeTool.Execute(context.Background(), map[string]any{"path": "src/missing.go"}); got == nil || !got.IsError || !strings.Contains(got.Output, "content or content_base64 is required") {
		t.Fatalf("expected missing content to fail, got %+v", got)
	}
	if got := writeTool.Execute(context.Background(), map[string]any{"path": "src/invalid.go", "content_base64": "not base64"}); got == nil || !got.IsError || !strings.Contains(got.Output, "content_base64 decode error") {
		t.Fatalf("expected invalid content_base64 to fail, got %+v", got)
	}
}

func TestToolRegistryDefinitionsAreSortedByName(t *testing.T) {
	registry := NewToolRegistry()
	for _, tool := range []*registryTestTool{
		{name: "charlie", description: "third"},
		{name: "alpha", description: "first"},
		{name: "bravo", description: "second"},
	} {
		if diagnostics := registry.RegisterWithDiagnostics(tool); len(diagnostics) != 0 {
			t.Fatalf("RegisterWithDiagnostics(%q) emitted diagnostics: %+v", tool.Name(), diagnostics)
		}
	}

	want := []string{"alpha", "bravo", "charlie"}
	for i := 0; i < 5; i++ {
		got := toolDefinitionNames(registry.Definitions())
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("Definitions() order = %v, want %v", got, want)
		}
	}
}

func TestToolRegistryDuplicateRegistrationIsDiagnosedAndQuarantined(t *testing.T) {
	registry := NewToolRegistry()
	first := &registryTestTool{name: "shared_tool", description: "first registered tool", output: "first"}
	duplicate := &registryTestTool{name: "shared_tool", description: "duplicate tool", output: "duplicate"}

	registry.Register(first)
	diagnostics := registry.RegisterWithDiagnostics(duplicate)
	if len(diagnostics) != 1 {
		t.Fatalf("RegisterWithDiagnostics(duplicate) diagnostics = %+v, want one diagnostic", diagnostics)
	}
	diagnostic := diagnostics[0]
	if diagnostic.Code != "duplicate_tool_registration" ||
		diagnostic.ToolName != "shared_tool" ||
		diagnostic.ExistingType == "" ||
		diagnostic.DuplicateType == "" ||
		!strings.Contains(diagnostic.Message, "quarantined") {
		t.Fatalf("unexpected duplicate diagnostic: %+v", diagnostic)
	}

	active, ok := registry.Get("shared_tool")
	if !ok {
		t.Fatal("Get(shared_tool) did not find the original tool")
	}
	if active != first {
		t.Fatalf("duplicate registration shadowed the original tool: got %T", active)
	}
	defs := registry.Definitions()
	if len(defs) != 1 || defs[0].Function.Name != "shared_tool" || defs[0].Function.Description != first.Description() {
		t.Fatalf("Definitions() exposed duplicate or wrong definition: %+v", defs)
	}
	result := executeToolCall(context.Background(), registry, ToolCall{
		ID:   "call-1",
		Type: "function",
		Function: FunctionCall{
			Name:      "shared_tool",
			Arguments: "{}",
		},
	})
	if result.IsError || result.Output != "first" {
		t.Fatalf("executeToolCall used quarantined duplicate or failed: %+v", result)
	}

	registry.Register(&registryTestTool{name: "shared_tool", description: "second duplicate", output: "second duplicate"})
	allDiagnostics := registry.Diagnostics()
	if len(allDiagnostics) != 2 {
		t.Fatalf("Diagnostics() = %+v, want two retained duplicate diagnostics", allDiagnostics)
	}
	allDiagnostics[0].Message = "mutated"
	if registry.Diagnostics()[0].Message == "mutated" {
		t.Fatal("Diagnostics() returned mutable registry storage")
	}
}

func TestToolRegistryNilRegistrationIsDiagnosedAndQuarantined(t *testing.T) {
	registry := NewToolRegistry()

	diagnostics := registry.RegisterWithDiagnostics(nil)
	if len(diagnostics) != 1 {
		t.Fatalf("RegisterWithDiagnostics(nil) diagnostics = %+v, want one", diagnostics)
	}
	if diagnostics[0].Code != "nil_tool_registration" || !strings.Contains(diagnostics[0].Message, "nil tool") {
		t.Fatalf("unexpected nil diagnostic: %+v", diagnostics[0])
	}
	if len(registry.Definitions()) != 0 {
		t.Fatalf("nil registration exposed definitions: %+v", registry.Definitions())
	}

	var typedNil *registryTestTool
	diagnostics = registry.RegisterWithDiagnostics(typedNil)
	if len(diagnostics) != 1 || diagnostics[0].Code != "nil_tool_registration" {
		t.Fatalf("RegisterWithDiagnostics(typed nil) diagnostics = %+v, want nil diagnostic", diagnostics)
	}
	if len(registry.Diagnostics()) != 2 {
		t.Fatalf("Diagnostics() = %+v, want retained nil diagnostics", registry.Diagnostics())
	}
}

func TestToolRegistryNilReceiverRegistrationReturnsDiagnostic(t *testing.T) {
	var registry *ToolRegistry
	diagnostics := registry.RegisterWithDiagnostics(&registryTestTool{name: "lonely", description: "no registry"})
	if len(diagnostics) != 1 || diagnostics[0].Code != "nil_tool_registry" {
		t.Fatalf("nil receiver diagnostics = %+v, want nil registry diagnostic", diagnostics)
	}
}

func TestAgentSetDynamicToolsSkipsNilAndKeepsBaseTools(t *testing.T) {
	agent := &Agent{Workdir: t.TempDir()}
	var typedNil *registryTestTool

	agent.SetDynamicTools([]Tool{nil, typedNil})

	if _, ok := agent.registry.Get("read_file"); !ok {
		t.Fatal("expected read_file base tool to remain registered")
	}
	if _, ok := agent.registry.Get("list_directory"); !ok {
		t.Fatal("expected list_directory base tool to remain registered")
	}
	diagnostics := agent.registry.Diagnostics()
	if len(diagnostics) != 2 {
		t.Fatalf("Diagnostics() = %+v, want nil dynamic tool diagnostics", diagnostics)
	}
}

func TestWriteFileToolBlocksReviewCheckoutMutation(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepoWithBranch(t, "agent-gamma-feature", "src/App.tsx", "export default function App() { return null }\n")
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "review-candidate")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "agent-gamma-feature")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      remote,
			CurrentPhase: "IMPLEMENTATION",
			Tasks: []map[string]any{{
				"task_id":      "task-review",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "validation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-review",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-iota",
				"local_path":     checkoutPath,
				"checkout_kind":  "review",
				"branch_name":    "agent-gamma-feature",
				"active_task_id": "task-review",
				"status":         "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	tool := NewWriteFileTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-iota",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-subpixel", TaskID: "task-review"}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{
		"path":    filepath.Join("project-checkouts", "review-candidate", "src", "App.tsx"),
		"content": "mutated\n",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected review checkout write to be blocked, got %+v", result)
	}
	if !strings.Contains(result.Output, "Review/validation checkouts are read-only") {
		t.Fatalf("unexpected error output: %s", result.Output)
	}
}

func TestWriteFileToolAllowsOwnedImplementationCheckoutMutation(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "owned-branch")
	branchName := "agent-iota-owned-task"
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-b", branchName)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        remote,
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["src/**"]}`,
			OverallState:   "PARTIAL",
			Tasks: []map[string]any{{
				"task_id":      "task-impl",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-owned",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-iota",
				"local_path":     checkoutPath,
				"checkout_kind":  "clone",
				"branch_name":    branchName,
				"active_task_id": "task-impl",
				"status":         "ACTIVE",
			}},
			Branches: []map[string]any{{
				"branch_id":        "branch-owned",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-owned",
				"agent_id":         "agent-iota",
				"active_task_id":   "task-impl",
				"branch_name":      branchName,
				"branch_kind":      "feature",
				"write_scope_json": `{"paths":["src/**"]}`,
				"status":           "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	tool := NewWriteFileTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-iota",
		func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-subpixel", TaskID: "task-impl"}
		},
	)
	result := tool.Execute(context.Background(), map[string]any{
		"path":    filepath.Join("project-checkouts", "owned-branch", "src", "App.tsx"),
		"content": "export default function App() { return 'ok' }\n",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected owned checkout write to pass, got %+v", result)
	}
	data, err := os.ReadFile(filepath.Join(checkoutPath, "src", "App.tsx"))
	if err != nil {
		t.Fatalf("read written file: %v", err)
	}
	if !strings.Contains(string(data), "return 'ok'") {
		t.Fatalf("unexpected file contents: %q", string(data))
	}
}

func TestFileToolsWithDeniedSubpathsHideSensitiveRoots(t *testing.T) {
	workdir := t.TempDir()
	if err := os.WriteFile(filepath.Join(workdir, "visible.txt"), []byte("ok"), 0o600); err != nil {
		t.Fatalf("write visible file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, ".env.example"), []byte("EXAMPLE=1"), 0o600); err != nil {
		t.Fatalf("write env template: %v", err)
	}
	for _, rel := range []string{
		".env",
		".git-credentials",
		".terraformrc",
		"terraform.tfvars",
		"terraform.tfstate",
		"agent.runtime.json",
		"tls.pem",
		"id_rsa",
		"service-account-prod.json",
	} {
		if err := os.WriteFile(filepath.Join(workdir, rel), []byte("secret"), 0o600); err != nil {
			t.Fatalf("write %q: %v", rel, err)
		}
	}
	if err := os.MkdirAll(filepath.Join(workdir, ".runtime-config"), 0o700); err != nil {
		t.Fatalf("mkdir runtime config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, ".runtime-config", "token.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatalf("write runtime secret: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(workdir, ".local-data"), 0o700); err != nil {
		t.Fatalf("mkdir local data: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, ".local-data", "session.json"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("write local data: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(workdir, ".aws"), 0o700); err != nil {
		t.Fatalf("mkdir aws: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, ".aws", "credentials"), []byte("aws-secret"), 0o600); err != nil {
		t.Fatalf("write aws credentials: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(workdir, ".git"), 0o700); err != nil {
		t.Fatalf("mkdir git: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, ".git", "config"), []byte("[remote \"origin\"]\n\turl = ssh://secret"), 0o600); err != nil {
		t.Fatalf("write git config: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(workdir, ".terraform.d"), 0o700); err != nil {
		t.Fatalf("mkdir terraform.d: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, ".terraform.d", "credentials.tfrc.json"), []byte("{\"credentials\":\"secret\"}"), 0o600); err != nil {
		t.Fatalf("write terraform credentials: %v", err)
	}

	readTool := NewReadFileToolWithDeniedSubpaths(workdir, []string{".runtime-config", ".local-data"})
	listTool := NewListDirectoryToolWithDeniedSubpaths(workdir, []string{".runtime-config", ".local-data"})

	if got := readTool.Execute(context.Background(), map[string]any{"path": "visible.txt"}); got == nil || got.IsError || got.Output != "ok" {
		t.Fatalf("ReadFileToolWithDeniedSubpaths(visible) = %+v", got)
	}
	if got := readTool.Execute(context.Background(), map[string]any{"path": ".env.example"}); got == nil || got.IsError || got.Output != "EXAMPLE=1" {
		t.Fatalf("ReadFileToolWithDeniedSubpaths(env template) = %+v", got)
	}

	for _, blocked := range []string{
		".runtime-config/token.txt",
		filepath.Join(".", ".runtime-config", "..", ".runtime-config", "token.txt"),
		".env",
		".git-credentials",
		".terraformrc",
		"terraform.tfvars",
		"terraform.tfstate",
		"agent.runtime.json",
		"tls.pem",
		"id_rsa",
		"service-account-prod.json",
		filepath.Join(".git", "config"),
		filepath.Join(".terraform.d", "credentials.tfrc.json"),
		filepath.Join(".aws", "credentials"),
	} {
		if got := readTool.Execute(context.Background(), map[string]any{"path": blocked}); got == nil || !got.IsError || !strings.Contains(got.Output, "inspect path unavailable") {
			t.Fatalf("ReadFileToolWithDeniedSubpaths(%q) = %+v", blocked, got)
		}
	}

	if got := listTool.Execute(context.Background(), map[string]any{"path": "."}); got == nil || got.IsError {
		t.Fatalf("ListDirectoryToolWithDeniedSubpaths(root) = %+v", got)
	} else if directoryListingContainsEntry(got.Output, ".runtime-config/") ||
		directoryListingContainsEntry(got.Output, ".local-data/") ||
		directoryListingContainsEntry(got.Output, ".aws/") ||
		directoryListingContainsEntry(got.Output, ".git/") ||
		directoryListingContainsEntry(got.Output, ".terraform.d/") ||
		directoryListingContainsEntry(got.Output, ".env") ||
		directoryListingContainsEntry(got.Output, ".git-credentials") ||
		directoryListingContainsEntry(got.Output, ".terraformrc") ||
		directoryListingContainsEntry(got.Output, "terraform.tfvars") ||
		directoryListingContainsEntry(got.Output, "terraform.tfstate") ||
		directoryListingContainsEntry(got.Output, "agent.runtime.json") ||
		directoryListingContainsEntry(got.Output, "tls.pem") ||
		directoryListingContainsEntry(got.Output, "id_rsa") ||
		directoryListingContainsEntry(got.Output, "service-account-prod.json") ||
		!directoryListingContainsEntry(got.Output, "visible.txt") ||
		!directoryListingContainsEntry(got.Output, ".env.example") {
		t.Fatalf("expected root listing to hide denied roots and obvious secret files while keeping visible entries, got %q", got.Output)
	}

	if got := listTool.Execute(context.Background(), map[string]any{"path": ".runtime-config"}); got == nil || !got.IsError || !strings.Contains(got.Output, "inspect path unavailable") {
		t.Fatalf("ListDirectoryToolWithDeniedSubpaths(secret dir) = %+v", got)
	}
	if got := listTool.Execute(context.Background(), map[string]any{"path": filepath.Join(".", ".runtime-config", "..", ".runtime-config")}); got == nil || !got.IsError || !strings.Contains(got.Output, "inspect path unavailable") {
		t.Fatalf("ListDirectoryToolWithDeniedSubpaths(normalized secret dir) = %+v", got)
	}
	if got := listTool.Execute(context.Background(), map[string]any{"path": ".aws"}); got == nil || !got.IsError || !strings.Contains(got.Output, "inspect path unavailable") {
		t.Fatalf("ListDirectoryToolWithDeniedSubpaths(secret-like dir) = %+v", got)
	}
}

func TestReadFileToolBoundsOutput(t *testing.T) {
	workdir := t.TempDir()
	path := filepath.Join(workdir, "big.txt")
	large := strings.Repeat("0123456789", maxReadSize/10+32)
	if err := os.WriteFile(path, []byte(large), 0o600); err != nil {
		t.Fatalf("write big file: %v", err)
	}

	got := NewReadFileTool(workdir).Execute(context.Background(), map[string]any{"path": "big.txt"})
	if got == nil || got.IsError {
		t.Fatalf("ReadFileTool.Execute() = %+v", got)
	}
	if !strings.Contains(got.Output, "truncated at 64KB") {
		t.Fatalf("expected truncated output marker, got %q", got.Output)
	}
	if len(got.Output) > maxReadSize+64 {
		t.Fatalf("expected bounded output, got length %d", len(got.Output))
	}
}

func TestAtomicWriteFileReplacesExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "state.txt")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	if err := atomicWriteFile(path, []byte("new"), 0o600); err != nil {
		t.Fatalf("atomicWriteFile: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read replaced file: %v", err)
	}
	if string(data) != "new" {
		t.Fatalf("expected replaced content, got %q", string(data))
	}
}

func TestMemoryToolsPersistWorkspaceState(t *testing.T) {
	workdir := t.TempDir()

	readTool := NewMemoryReadTool(workdir)
	_ = NewMemorySearchTool(nil, nil, "") // Added this line based on the instruction
	writeTool := NewMemoryWriteTool(workdir)
	noteTool := NewDailyNoteAppendTool(workdir)

	if got := readTool.Execute(context.Background(), nil); got == nil || got.IsError || !strings.Contains(got.Output, "no memory yet") {
		t.Fatalf("MemoryReadTool on empty workspace = %+v", got)
	}

	if got := writeTool.Execute(context.Background(), map[string]any{"content": "alpha"}); got == nil || got.IsError {
		t.Fatalf("MemoryWriteTool.Execute() failed: %+v", got)
	}

	if got := readTool.Execute(context.Background(), nil); got == nil || got.IsError || !strings.Contains(got.Output, "alpha") {
		t.Fatalf("MemoryReadTool.Execute() after write = %+v", got)
	}

	if got := noteTool.Execute(context.Background(), map[string]any{"note": "daily note"}); got == nil || got.IsError {
		t.Fatalf("DailyNoteAppendTool.Execute() failed: %+v", got)
	}

	now := time.Now()
	path := filepath.Join(workdir, "memory", now.Format("200601"), now.Format("20060102")+".md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read daily note: %v", err)
	}
	if !strings.Contains(string(data), "daily note") {
		t.Fatalf("daily note file missing appended content: %q", string(data))
	}
}

func TestMemoryReadToolWithDeniedSubpathsRejectsSymlinkIntoSensitiveRoot(t *testing.T) {
	workdir := t.TempDir()
	secretPath := filepath.Join(workdir, ".runtime-config", "memory-secret.md")
	if err := os.MkdirAll(filepath.Dir(secretPath), 0o700); err != nil {
		t.Fatalf("mkdir secret root: %v", err)
	}
	if err := os.WriteFile(secretPath, []byte("secret-memory"), 0o600); err != nil {
		t.Fatalf("write secret memory: %v", err)
	}
	if err := os.Symlink(secretPath, filepath.Join(workdir, "MEMORY.md")); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}

	got := NewMemoryReadToolWithDeniedSubpaths(workdir, []string{".runtime-config"}).Execute(context.Background(), nil)
	if got == nil || !got.IsError || !strings.Contains(got.Output, "inspect path unavailable") {
		t.Fatalf("MemoryReadToolWithDeniedSubpaths(symlink secret) = %+v", got)
	}
}

func TestRunToolLoopDetailedWithLimitStopsAfterCeiling(t *testing.T) {
	registry := NewToolRegistry()
	tool := &ceilingFailureTool{}
	registry.Register(tool)

	llm := &loopingToolLLM{}
	run, err := RunToolLoopDetailedWithLimit(context.Background(), llm, registry, []Message{{Role: "user", Content: "start"}}, nil, nil, 2)
	if err == nil {
		t.Fatal("expected tool loop ceiling error")
	}
	if run != nil {
		t.Fatalf("expected no completed run, got %+v", run)
	}
	if !strings.Contains(err.Error(), "tool loop exceeded 2 iterations") {
		t.Fatalf("expected ceiling error, got %v", err)
	}
	if llm.calls != 2 {
		t.Fatalf("expected two llm calls before ceiling, got %d", llm.calls)
	}
	if tool.calls != 2 {
		t.Fatalf("expected two tool executions before ceiling, got %d", tool.calls)
	}
}

func TestRunToolLoopDetailedCapturesStructuredToolResultMetadata(t *testing.T) {
	registry := NewToolRegistry()
	registry.Register(&registryTestTool{
		name:        "browser_visual_probe",
		description: "returns browser visual probe contract output",
		output: strings.Join([]string{
			`{`,
			`  "contract_version": "browser_visual_probe_result_v1",`,
			`  "status": "warn",`,
			`  "artifact_root": "/tmp/rhizome-artifacts",`,
			`  "scenario_id": "initial-load",`,
			`  "state_id": "initial_state",`,
			`  "viewports": [`,
			`    {"id": "desktop", "screenshot_path": "screenshots/desktop.png", "screenshot_sha256": "sha256:desktop"},`,
			`    {"viewport_id": "mobile", "artifact_ref": "artifact://mobile-shot", "artifact_hash": "sha256:mobile"}`,
			`  ]`,
			`}`,
		}, "\n"),
	})
	llm := &sequenceLLM{responses: []*LLMResponse{
		{
			ToolCalls: []ToolCall{{
				ID:   "call-probe",
				Type: "function",
				Function: FunctionCall{
					Name:      "browser_visual_probe",
					Arguments: `{"url":"http://127.0.0.1:3000"}`,
				},
			}},
		},
		{Content: "done"},
	}}

	run, err := RunToolLoopDetailed(context.Background(), llm, registry, []Message{{Role: "user", Content: "inspect"}}, nil, nil)
	if err != nil {
		t.Fatalf("RunToolLoopDetailed() error = %v", err)
	}
	if run.Content != "done" || len(run.Messages) < 4 {
		t.Fatalf("unexpected completed run: %+v", run)
	}
	if len(run.ToolResults) != 1 {
		t.Fatalf("ToolResults = %+v, want one structured result", run.ToolResults)
	}
	got := run.ToolResults[0]
	if got.Iteration != 0 || got.ToolCallID != "call-probe" || got.ToolName != "browser_visual_probe" || got.IsError {
		t.Fatalf("unexpected base tool result trace: %+v", got)
	}
	if got.ContractVersion != "browser_visual_probe_result_v1" || got.Status != "warn" || got.ArtifactRoot != "/tmp/rhizome-artifacts" {
		t.Fatalf("missing contract/status/root metadata: %+v", got)
	}
	if got.ScenarioID != "initial-load" || got.StateID != "initial_state" || got.ViewportID != "desktop" {
		t.Fatalf("missing state labels in metadata: %+v", got)
	}
	if !containsAnySignal(strings.Join(got.ArtifactPaths, "\n"), []string{"screenshots/desktop.png"}) {
		t.Fatalf("missing screenshot path in metadata: %+v", got)
	}
	if !containsAnySignal(strings.Join(got.ArtifactRefs, "\n"), []string{"artifact://mobile-shot"}) {
		t.Fatalf("missing artifact ref in metadata: %+v", got)
	}
	if !containsAnySignal(strings.Join(got.ArtifactHashes, "\n"), []string{"sha256:desktop"}) ||
		!containsAnySignal(strings.Join(got.ArtifactHashes, "\n"), []string{"sha256:mobile"}) {
		t.Fatalf("missing artifact hashes in metadata: %+v", got)
	}
}

func TestExecuteToolCallRejectsRecoverableInvalidCodexArguments(t *testing.T) {
	registry := NewToolRegistry()
	tool := &ceilingFailureTool{}
	registry.Register(tool)

	payload := codexInvalidArgumentsPayload(`{"value":"bad\+escape"}`, os.ErrInvalid)
	result := executeToolCall(context.Background(), registry, ToolCall{
		ID:   "bad-args",
		Type: "function",
		Function: FunctionCall{
			Name:      tool.Name(),
			Arguments: payload,
		},
	})
	if !result.IsError || !strings.Contains(result.Output, "invalid arguments_json") || !strings.Contains(result.Output, "not executed") {
		t.Fatalf("expected recoverable invalid arguments error, got %+v", result)
	}
	if tool.calls != 0 {
		t.Fatalf("tool should not execute on invalid codex arguments, calls=%d", tool.calls)
	}
}

type loopingToolLLM struct {
	calls int
}

func (l *loopingToolLLM) Chat(ctx context.Context, messages []Message, tools []ToolDef) (*LLMResponse, error) {
	l.calls++
	return &LLMResponse{
		ToolCalls: []ToolCall{{
			ID:   "retry-loop-call",
			Type: "function",
			Function: FunctionCall{
				Name:      "ceiling_failure_tool",
				Arguments: "{}",
			},
		}},
	}, nil
}

type ceilingFailureTool struct {
	calls int
}

func (t *ceilingFailureTool) Name() string {
	return "ceiling_failure_tool"
}

func (t *ceilingFailureTool) Description() string {
	return "returns a deterministic error so the loop ceiling can be verified"
}

func (t *ceilingFailureTool) Parameters() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{}}
}

func (t *ceilingFailureTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	t.calls++
	return &ToolResult{Output: "deterministic tool failure", IsError: true}
}

type registryTestTool struct {
	name        string
	description string
	output      string
}

func (t *registryTestTool) Name() string {
	return t.name
}

func (t *registryTestTool) Description() string {
	return t.description
}

func (t *registryTestTool) Parameters() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{}}
}

func (t *registryTestTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	return &ToolResult{Output: t.output}
}

func toolDefinitionNames(defs []ToolDef) []string {
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Function.Name)
	}
	return names
}

func directoryListingContainsEntry(listing, entry string) bool {
	for _, line := range strings.Split(listing, "\n") {
		if strings.TrimSpace(line) == entry {
			return true
		}
	}
	return false
}
