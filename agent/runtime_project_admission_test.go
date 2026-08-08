package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProjectClaimBranchNameCompactsLongRefs(t *testing.T) {
	projectID := "project-task-subpixel-art-processor-20260505t171700z"
	taskID := "task-1778001950357476900-32456111"
	branchName := projectClaimBranchName(
		"delta",
		projectID,
		taskID,
	)
	if strings.Contains(branchName, projectID) {
		t.Fatalf("expected long project id to be compacted, got %q", branchName)
	}
	if strings.Contains(branchName, taskID) {
		t.Fatalf("expected long task id to be compacted, got %q", branchName)
	}
	want := "agent-delta-p-" + shortRefHash(projectID) + "-t-" + shortRefHash(taskID)
	if branchName != want {
		t.Fatalf("branch name = %q, want %q", branchName, want)
	}
	if strings.Contains(branchName, "/") {
		t.Fatalf("default branch name should avoid nested ref directories on Windows, got %q", branchName)
	}
	if len(branchName) > 64 {
		t.Fatalf("branch name too long for Windows ref paths: len=%d branch=%q", len(branchName), branchName)
	}
}

func TestCheckoutProjectClaimBranchResetsAncestorLocalBranchToOriginBase(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "checkout")
	runGitNoDir(t, "clone", remote, checkoutPath)
	staleMain := gitOutput(t, checkoutPath, "rev-parse", "main")
	branchName := "agent-beta-p-282e12381b-t-354cd8bdc1"
	runGit(t, checkoutPath, "checkout", "-B", branchName, "main")

	source := filepath.Join(t.TempDir(), "source")
	runGitNoDir(t, "clone", remote, source)
	runGit(t, source, "config", "user.name", "Rhizome Test")
	runGit(t, source, "config", "user.email", "rhizome-test@example.invalid")
	acceptedPath := filepath.Join(source, "internal", "cli", "acceptance_matrix_test.go")
	if err := os.MkdirAll(filepath.Dir(acceptedPath), 0o755); err != nil {
		t.Fatalf("mkdir accepted parent: %v", err)
	}
	if err := os.WriteFile(acceptedPath, []byte("package cli\n"), 0o644); err != nil {
		t.Fatalf("write accepted test: %v", err)
	}
	runGit(t, source, "add", "internal/cli/acceptance_matrix_test.go")
	runGit(t, source, "commit", "-m", "Advance integrated main")
	runGit(t, source, "push", "origin", "main")
	currentMain := gitOutput(t, source, "rev-parse", "HEAD")
	if currentMain == staleMain {
		t.Fatalf("test setup failed: remote main did not advance")
	}

	repo := ProjectRepositoryRecord{RepoID: "repo-1", RemoteURL: remote, DefaultBranch: "main", RepoStatus: "READY"}
	if _, _, err := materializeGitCheckout(context.Background(), checkoutPath, repo, "main", false); err != nil {
		t.Fatalf("materialize existing checkout: %v", err)
	}
	if err := checkoutProjectClaimBranch(context.Background(), checkoutPath, branchName, "main"); err != nil {
		t.Fatalf("checkout project claim branch: %v", err)
	}
	if got := gitOutput(t, checkoutPath, "branch", "--show-current"); got != branchName {
		t.Fatalf("current branch = %q, want %q", got, branchName)
	}
	if got := gitOutput(t, checkoutPath, "rev-parse", "HEAD"); got != currentMain {
		t.Fatalf("claim branch HEAD = %s, want current origin/main %s", got, currentMain)
	}
	if _, err := os.Stat(filepath.Join(checkoutPath, "internal", "cli", "acceptance_matrix_test.go")); err != nil {
		t.Fatalf("claim branch should include integrated main artifact: %v", err)
	}
}

func TestCheckoutProjectClaimBranchQuarantinesCleanDivergentBranch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "checkout")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "config", "user.name", "Rhizome Test")
	runGit(t, checkoutPath, "config", "user.email", "rhizome-test@example.invalid")
	branchName := "agent-beta-p-282e12381b-t-354cd8bdc1"
	runGit(t, checkoutPath, "checkout", "-B", branchName, "main")
	stalePath := filepath.Join(checkoutPath, "internal", "eval", "eval.go")
	if err := os.MkdirAll(filepath.Dir(stalePath), 0o755); err != nil {
		t.Fatalf("mkdir stale parent: %v", err)
	}
	if err := os.WriteFile(stalePath, []byte("package eval\n\nconst stale = true\n"), 0o644); err != nil {
		t.Fatalf("write stale branch file: %v", err)
	}
	runGit(t, checkoutPath, "add", "internal/eval/eval.go")
	runGit(t, checkoutPath, "commit", "-m", "stale candidate")
	staleHead := gitOutput(t, checkoutPath, "rev-parse", "HEAD")
	runGit(t, checkoutPath, "push", "-u", "origin", branchName)

	source := filepath.Join(t.TempDir(), "source")
	runGitNoDir(t, "clone", remote, source)
	runGit(t, source, "config", "user.name", "Rhizome Test")
	runGit(t, source, "config", "user.email", "rhizome-test@example.invalid")
	acceptedPath := filepath.Join(source, "internal", "cli", "acceptance_matrix_test.go")
	if err := os.MkdirAll(filepath.Dir(acceptedPath), 0o755); err != nil {
		t.Fatalf("mkdir accepted parent: %v", err)
	}
	if err := os.WriteFile(acceptedPath, []byte("package cli\n"), 0o644); err != nil {
		t.Fatalf("write accepted test: %v", err)
	}
	runGit(t, source, "add", "internal/cli/acceptance_matrix_test.go")
	runGit(t, source, "commit", "-m", "Advance integrated main")
	runGit(t, source, "push", "origin", "main")
	currentMain := gitOutput(t, source, "rev-parse", "HEAD")
	if currentMain == staleHead {
		t.Fatalf("test setup failed: remote main did not diverge from stale branch")
	}

	repo := ProjectRepositoryRecord{RepoID: "repo-1", RemoteURL: remote, DefaultBranch: "main", RepoStatus: "READY"}
	if _, _, err := materializeGitCheckout(context.Background(), checkoutPath, repo, "main", false); err != nil {
		t.Fatalf("materialize existing checkout: %v", err)
	}
	if err := checkoutProjectClaimBranch(context.Background(), checkoutPath, branchName, "main"); err != nil {
		t.Fatalf("checkout project claim branch: %v", err)
	}
	if got := gitOutput(t, checkoutPath, "branch", "--show-current"); got != branchName {
		t.Fatalf("current branch = %q, want %q", got, branchName)
	}
	if got := gitOutput(t, checkoutPath, "rev-parse", "HEAD"); got != currentMain {
		t.Fatalf("claim branch HEAD = %s, want current origin/main %s", got, currentMain)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale branch file should not survive fresh claim branch, stat err=%v", err)
	}
	archiveBranch := projectClaimStaleBranchArchiveName(branchName, staleHead)
	if got := gitOutput(t, checkoutPath, "rev-parse", archiveBranch); got != staleHead {
		t.Fatalf("archive branch HEAD = %s, want stale head %s", got, staleHead)
	}
	if got := strings.TrimSpace(mustProjectAdmissionGitOutput(t, checkoutPath, "ls-remote", "--heads", "origin", archiveBranch)); got == "" || !strings.Contains(got, staleHead) {
		t.Fatalf("expected stale remote archive %s at %s, got %q", archiveBranch, staleHead, got)
	}
	if got := strings.TrimSpace(mustProjectAdmissionGitOutput(t, checkoutPath, "ls-remote", "--heads", "origin", branchName)); got != "" {
		t.Fatalf("stale remote implementation branch should be deleted before fresh publish, got %q", got)
	}
}

func TestProjectTaskWriteScopeHintsForAdmissionPrefersScaffoldBoundary(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-clearpress-scaffold",
		Title:       "Scaffold Clearpress app shell with mock auth and shared config ownership",
		Description: "Create the Vite baseline, shared config, package scripts, and app shell before feature lanes start.",
		Tags:        []string{"implementation", "frontend"},
	}
	got := projectTaskWriteScopeHintsForAdmission(task, []string{"package.json", "public/**", "src/**", "tests/**"})
	for _, want := range []string{"package*.json", "vite.config.*", "index.html", "src/App.*", "src/ui/**"} {
		if !stringSliceContainsFold(got, want) {
			t.Fatalf("expected scaffold scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"src/auth/**", "src/profile/**", "src/settings/**"} {
		if stringSliceContainsFold(got, forbidden) {
			t.Fatalf("scaffold scope should not be narrowed to feature path %q, got %+v", forbidden, got)
		}
	}
}

func TestProjectTaskWriteScopeHintsForAdmissionDoesNotLetViteOverbroadenFeatureScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-auth-settings",
		Title:       "Scaffold auth settings in existing Vite app",
		Description: "Wire login, profile avatar, and quote preferences inside the current frontend app.",
		Tags:        []string{"implementation", "frontend"},
	}
	got := projectTaskWriteScopeHintsForAdmission(task, []string{"package.json", "public/**", "src/**", "tests/**"})
	for _, want := range []string{"src/auth/**", "src/profile/**", "src/settings/**"} {
		if !stringSliceContainsFold(got, want) {
			t.Fatalf("expected feature scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"package*.json", "vite.config.*", "src/App.*", "src/ui/**"} {
		if stringSliceContainsFold(got, forbidden) {
			t.Fatalf("feature scope should not be broadened to scaffold path %q, got %+v", forbidden, got)
		}
	}
}

func TestProjectTaskWriteScopeHintsForAdmissionDoesNotRouteFrontendParserToGoInternalScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:          "task-clearpress-markdown-parser",
		Title:           "Implement markdown parser in existing Vite app",
		Description:     "Build the React frontend editor parser for browser markdown shortcuts.",
		Tags:            []string{"implementation", "frontend", "react", "vite"},
		ProjectLane:     "implementation",
		WriteScopeHints: []string{"src/**", "tests/**", "package.json"},
	}
	got := projectTaskWriteScopeHintsForAdmission(task, task.WriteScopeHints)
	for _, want := range []string{"src/editor/**", "src/lib/editor/**", "tests/editor/**"} {
		if !stringSliceContainsFold(got, want) {
			t.Fatalf("expected frontend parser scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"internal/parser/**", "internal/ast/**"} {
		if stringSliceContainsFold(got, forbidden) {
			t.Fatalf("frontend parser scope must not route to Go internal path %q, got %+v", forbidden, got)
		}
	}
}

func TestProjectTaskWriteScopeHintsForAdmissionNarrowsGoRQInterpreterLanes(t *testing.T) {
	tests := []struct {
		name      string
		task      WorkspaceTaskRecord
		want      []string
		forbidden []string
	}{
		{
			name: "lexer",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-lexer",
				Title:           "Implement rq lexer with positioned diagnostics",
				Description:     "Build tokenizer and token stream support for the rq interpreter.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"internal/lexer/**", "internal/token/**"},
			forbidden: []string{"cmd/**", "internal/ast/**", "internal/diagnostics/**", "**/*test.go", "go.mod", "README.md"},
		},
		{
			name: "parser",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-parser",
				Title:           "Implement rq parser and AST for full grammar",
				Description:     "Parse tokens into the rq AST.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"internal/parser/**", "internal/ast/**"},
			forbidden: []string{"cmd/**", "**/*test.go", "go.mod", "README.md"},
		},
		{
			name: "evaluator",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-evaluator",
				Title:           "Implement rq evaluator core and JSON path semantics",
				Description:     "Evaluate rq AST nodes against JSON input values.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"internal/evaluator/**", "internal/runtime/**", "internal/value/**"},
			forbidden: []string{"cmd/**", "internal/parser/**", "**/*test.go", "go.mod", "README.md"},
		},
		{
			name: "builtins",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-builtins",
				Title:           "Implement rq builtins including map/filter lambdas",
				Description:     "Add built-in function library and lambda helpers.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"internal/builtins/**", "internal/lambda/**"},
			forbidden: []string{"cmd/**", "internal/evaluator/**", "**/*test.go", "go.mod", "README.md"},
		},
		{
			name: "cli-repl",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-cli-repl",
				Title:           "Implement rq CLI file mode and REPL",
				Description:     "Wire command line entrypoints and read-eval-print loop.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"cmd/**", "internal/cli/**", "internal/repl/**"},
			forbidden: []string{"internal/lexer/**", "internal/parser/**", "**/*test.go", "go.mod", "README.md"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := projectTaskWriteScopeHintsForAdmission(tt.task, tt.task.WriteScopeHints)
			for _, want := range tt.want {
				if !stringSliceContainsFold(got, want) {
					t.Fatalf("expected rq scope to include %q, got %+v", want, got)
				}
			}
			for _, forbidden := range tt.forbidden {
				if stringSliceContainsFold(got, forbidden) {
					t.Fatalf("rq scope should not keep broad/conflicting path %q, got %+v", forbidden, got)
				}
			}
		})
	}
}

func TestProjectTaskWriteScopeHintsForAdmissionNarrowsGoRQScaffold(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-signal01-rq-run26-scaffold",
		Title:       "Run26 Scaffold: seed shared Go module and repo skeleton for rq",
		Description: "Create the Go module, README, cmd/rq entrypoint, package skeletons for lexer/parser/evaluator/builtins/CLI lanes, and a scaffold smoke test so later lanes can compile incrementally.",
		ProjectLane: "implementation",
		WriteScopeHints: []string{
			"cmd/**",
			"internal/**",
			"tests/**",
			"go.mod",
			"README.md",
		},
	}
	got := projectTaskWriteScopeHintsForAdmission(task, task.WriteScopeHints)
	for _, want := range []string{"go.mod", "README.md", "tests/scaffold_test.go", "cmd/README.md", "internal/README.md"} {
		if !stringSliceContainsFold(got, want) {
			t.Fatalf("expected scaffold rq scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{
		"cmd/**",
		"internal/**",
		"tests/**",
		"internal/lexer/**",
		"internal/parser/**",
		"internal/evaluator/**",
		"internal/builtins/**",
		"internal/cli/**",
		"internal/repl/**",
	} {
		if stringSliceContainsFold(got, forbidden) {
			t.Fatalf("rq scaffold scope must not claim product lane path %q, got %+v", forbidden, got)
		}
	}
}

func TestProjectTaskWriteScopeHintsForAdmissionNarrowsLuaAcceptanceLexer(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-1781622429496831800-2dab1f83",
		ProjectID:   "project-signal01-lua-capability",
		ProjectLane: "implementation",
		Title:       "Implement AC-LUA-LEX-01: Lua lexer subset",
		Description: "Lex Lua 5.1 subset tokens and source positions.",
		WriteScopeHints: []string{
			"cmd/**",
			"internal/**",
			"testdata/**",
			"tools/oracle/**",
			"scripts/**",
			"README.md",
		},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","acceptance_criteria_refs":["AC-LUA-LEX-01"]}`,
	}

	got := projectTaskWriteScopeHintsForAdmission(task, task.WriteScopeHints)
	for _, want := range []string{"internal/lexer/**", "internal/token/**", "internal/tokens/**"} {
		if !stringSliceContainsFold(got, want) {
			t.Fatalf("expected Lua lexer scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"cmd/**", "cmd/glua/**", "internal/**", "internal/runner/**", "internal/parser/**", "testdata/**", "testdata/smoke/**", "tools/oracle/**", "scripts/**", "README.md"} {
		if stringSliceContainsFold(got, forbidden) {
			t.Fatalf("Lua lexer scope must not keep broad/harness path %q, got %+v", forbidden, got)
		}
	}
}

func TestProjectTaskWriteScopeHintsForAdmissionKeepsLuaCLIHarness(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-lua-cli-harness",
		ProjectID:   "project-signal01-lua-capability",
		ProjectLane: "implementation",
		Title:       "Implement AC-LUA-CLI-01 / AC-LUA-ERR-01 harness",
		Description: "Provide file-mode CLI, basic REPL, useful errors, oracle harness, and differential smoke corpus.",
		WriteScopeHints: []string{
			"cmd/**",
			"internal/**",
			"scripts/**",
			"testdata/**",
			"tools/oracle/**",
			"README.md",
		},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","acceptance_criteria_refs":["AC-LUA-CLI-01","AC-LUA-ERR-01"]}`,
	}

	got := projectTaskWriteScopeHintsForAdmission(task, task.WriteScopeHints)
	for _, want := range []string{"cmd/glua/**", "internal/runner/**", "scripts/**", "testdata/smoke/**", "tools/oracle/**", "README.md", "internal/errors/**"} {
		if !stringSliceContainsFold(got, want) {
			t.Fatalf("expected Lua CLI harness scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"internal/lexer/**", "internal/parser/**", "internal/eval/**", "testdata/**", "cmd/**", "internal/**"} {
		if stringSliceContainsFold(got, forbidden) {
			t.Fatalf("Lua CLI harness scope should not keep unrelated broad path %q, got %+v", forbidden, got)
		}
	}
}

func TestProjectTaskWriteScopeHintsForAdmissionKeepsRun10RQLanesDisjoint(t *testing.T) {
	tests := []struct {
		name      string
		task      WorkspaceTaskRecord
		want      []string
		forbidden []string
	}{
		{
			name: "lexer-diagnostics-lane-stays-lexer-owned",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-lexer-diag",
				Title:           "Signal-01 Lane A: implement lexer and diagnostic primitives for rq",
				Description:     "Implement position-aware lexer/tokenization and diagnostic primitives for the rq interpreter.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/rq/**", "internal/lexer/**", "internal/diag/**", "internal/token/**", "internal/ast/**", "testdata/**", "go.mod", "go.sum"},
			},
			want:      []string{"internal/lexer/**", "internal/token/**", "internal/tokens/**"},
			forbidden: []string{"cmd/rq/**", "internal/parser/**", "internal/ast/**", "internal/diag/**", "testdata/**", "go.mod", "go.sum"},
		},
		{
			name: "parser-ast-lane-does-not-overlap-lexer",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-parser-ast",
				Title:           "Signal-01 Lane B: implement parser and AST for rq expressions",
				Description:     "Parse rq tokens into AST nodes. Do not implement evaluator or runtime semantics in this lane.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"internal/parser/**", "internal/ast/**", "internal/diag/**", "testdata/**", "go.mod", "go.sum"},
			},
			want:      []string{"internal/parser/**", "internal/ast/**"},
			forbidden: []string{"internal/lexer/**", "internal/token/**", "internal/eval/**", "internal/evaluator/**", "internal/diag/**", "testdata/**", "go.mod", "go.sum"},
		},
		{
			name: "evaluator-coalition-lane-does-not-overlap-parser-or-lexer",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-evaluator-coalition",
				Title:           "Signal-01 Lane C: implement evaluator, built-ins, and lambda semantics for rq",
				Description:     "Evaluate rq AST nodes against JSON input values and add runtime builtins.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"internal/eval/**", "internal/jsonctx/**", "internal/diag/**", "testdata/**", "go.mod", "go.sum"},
			},
			want:      []string{"internal/eval/**", "internal/jsonctx/**", "internal/evaluator/**", "internal/builtins/**", "internal/lambda/**"},
			forbidden: []string{"internal/lexer/**", "internal/token/**", "internal/parser/**", "internal/ast/**", "internal/diag/**", "testdata/**", "go.mod", "go.sum"},
		},
		{
			name: "cli-repl-readme-lane-does-not-own-evaluator",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-cli-repl-readme",
				Title:           "Signal-01 Lane D: implement CLI, REPL, and reviewer-facing README for rq",
				Description:     "Wire command line entrypoints and REPL to the actual evaluator without changing evaluator semantics.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/rq/**", "internal/jsonctx/**", "README.md", "testdata/**", "go.mod", "go.sum"},
			},
			want:      []string{"cmd/rq/**", "README.md"},
			forbidden: []string{"internal/eval/**", "internal/evaluator/**", "internal/runtime/**", "internal/value/**", "internal/path/**", "internal/jsonpath/**", "internal/jsonctx/**", "testdata/**", "go.mod", "go.sum"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := projectTaskWriteScopeHintsForAdmission(tt.task, tt.task.WriteScopeHints)
			for _, want := range tt.want {
				if !stringSliceContainsFold(got, want) {
					t.Fatalf("expected Run10 rq scope to include %q, got %+v", want, got)
				}
			}
			for _, forbidden := range tt.forbidden {
				if stringSliceContainsFold(got, forbidden) {
					t.Fatalf("Run10 rq scope should not contain conflicting/shared path %q, got %+v", forbidden, got)
				}
			}
		})
	}
}

func stringSliceContainsFold(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(want)) {
			return true
		}
	}
	return false
}

func TestProjectWriteScopeJSONRejectsProseHints(t *testing.T) {
	if got := projectWriteScopeJSONFromPaths([]string{
		"existing Clearpress shell/workspace checkout",
		"app shell",
		"routing",
		"review-ready evidence for the article workspace slice",
	}); got != "" {
		t.Fatalf("prose write scope hints should not produce write_scope_json, got %s", got)
	}
	valid := `{"paths":["package.json","vite.config.*","index.html","public/**","src/**"]}`
	if err := validateProjectWriteScopeJSON(valid); err != nil {
		t.Fatalf("valid write scope rejected: %v", err)
	}
	if err := validateProjectWriteScopeJSON(`{"paths":["app shell","routing"]}`); err == nil || !strings.Contains(err.Error(), "not prose") {
		t.Fatalf("expected prose write_scope_json rejection, got %v", err)
	}
}

func TestProjectClaimExistingBranchUsesFollowupBranchHint(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-revision",
		Title:       "Unblock integration candidate branch-ready",
		Description: "Patch queue decision follow-up.\n\n- queue_id: patchq-1\n- branch_id: branch-ready\n- state: BLOCKED",
	}
	branch, ok := selectProjectClaimExistingBranch([]ProjectBranchRecord{
		{
			BranchID:       "branch-ready",
			RepoID:         "repo-main",
			AgentID:        "gamma",
			BranchName:     "agent/gamma/project-subpixel/task-original",
			WriteScopeJSON: `{"paths":["**"]}`,
			Status:         "READY_FOR_REVIEW",
		},
	}, task, "repo-main", "gamma")
	if !ok {
		t.Fatalf("expected follow-up branch hint to select existing branch")
	}
	if branch.BranchName != "agent/gamma/project-subpixel/task-original" {
		t.Fatalf("selected branch name = %q", branch.BranchName)
	}
	if got := projectClaimTaskBranchHint(task); got != "branch-ready" {
		t.Fatalf("branch hint = %q, want branch-ready", got)
	}
}

func TestRuntimeClaimTaskUsesHintedBlockedBranchWithoutPreClaimReregister(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	branchName := "agent/gamma/project-alpha/task-original"
	repo := ProjectRepositoryRecord{
		RepoID:        "repo-main",
		RemoteURL:     remoteURL,
		DefaultBranch: "main",
		RepoStatus:    "READY",
		IsCanonical:   true,
	}
	if _, _, err := materializeGitCheckout(context.Background(), workdir, repo, "main", true); err != nil {
		t.Fatalf("materialize checkout: %v", err)
	}
	if err := checkoutProjectClaimBranch(context.Background(), workdir, branchName, "main"); err != nil {
		t.Fatalf("checkout hinted branch: %v", err)
	}

	var methods []string
	var claimParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-worker",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "gamma",
							"role_type":        "implementer",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["src/**"]}`,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
					"checkouts": []any{
						map[string]any{
							"checkout_id":   "checkout-1",
							"workspace_id":  "ws",
							"project_id":    "project-alpha",
							"repo_id":       "repo-main",
							"machine_id":    "machine-a",
							"agent_id":      "gamma",
							"local_path":    workdir,
							"checkout_kind": "clone",
							"branch_name":   branchName,
							"dirty_state":   "CLEAN",
							"status":        "ACTIVE",
						},
					},
					"branches": []any{
						map[string]any{
							"branch_id":        "branch-ready",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"repo_id":          "repo-main",
							"checkout_id":      "checkout-1",
							"agent_id":         "gamma",
							"active_task_id":   "task-original",
							"active_claim_id":  "task-original",
							"branch_name":      branchName,
							"branch_kind":      "feature",
							"base_branch":      "main",
							"write_scope_json": `{"paths":["src/**"]}`,
							"status":           "READY_FOR_REVIEW",
						},
					},
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		case "project.checkout.register", "project.branch.register":
			t.Fatalf("hinted blocked branch claim must not pre-register checkout/branch before agent.task.claim: %+v", req.Params)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
			Workdir:     filepath.Dir(workdir),
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-patchq-revision",
		Title:               "Unblock integration candidate branch-ready",
		Description:         "Patch queue decision follow-up.\n\n- queue_id: patchq-1\n- item_id: patchitem-1\n- branch_id: branch-ready\n- state: BLOCKED",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim patch queue revision", nil)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,agent.task.claim,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(claimParams, "branch_id") != "branch-ready" || rpcString(claimParams, "checkout_id") != "checkout-1" {
		t.Fatalf("expected claim to reuse hinted branch/checkout, got %+v", claimParams)
	}
	assertProjectAdmissionGitCheckout(t, workdir, remoteURL, branchName)
}

func TestRuntimeClaimTaskForksReadyBranchHintForFreshImplementation(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	oldBranch := ProjectBranchRecord{
		BranchID:       "branch-ready",
		RepoID:         "repo-main",
		AgentID:        "delta",
		BranchName:     projectClaimBranchName("delta", "project-alpha", "task-eval"),
		WriteScopeJSON: `{"paths":["internal/eval/**"]}`,
		HeadSHA:        strings.Repeat("a", 40),
		ReviewDocKey:   "project.project-alpha.branch.branch-ready.review",
		Status:         "READY_FOR_REVIEW",
	}
	retryBranchID := "branch-abandoned-retry"
	successorBranchName := projectClaimSuccessorBranchName("delta", "project-alpha", "task-eval", oldBranch)
	repo := ProjectRepositoryRecord{
		RepoID:        "repo-main",
		Name:          "project-alpha",
		RemoteURL:     remoteURL,
		DefaultBranch: "main",
		RepoStatus:    "READY",
		IsCanonical:   true,
	}
	expectedCheckoutPath := projectCheckoutMaterializeDefaultPath(workdir, "project-alpha", repo)

	var methods []string
	var claimParams map[string]any
	var checkoutRegisterParams map[string]any
	var branchRegisterParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-worker",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "delta",
							"role_type":        "implementer",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["internal/eval/**"]}`,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"name":           "project-alpha",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
					"checkouts": []any{
						map[string]any{
							"checkout_id":   "checkout-old-ready",
							"workspace_id":  "ws",
							"project_id":    "project-alpha",
							"repo_id":       "repo-main",
							"machine_id":    "machine-a",
							"agent_id":      "delta",
							"local_path":    filepath.Join(workdir, "old-ready-checkout"),
							"checkout_kind": "clone",
							"branch_name":   oldBranch.BranchName,
							"dirty_state":   "CLEAN",
							"status":        "ACTIVE",
						},
					},
					"branches": []any{
						map[string]any{
							"branch_id":        oldBranch.BranchID,
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"repo_id":          oldBranch.RepoID,
							"checkout_id":      "checkout-old-ready",
							"agent_id":         oldBranch.AgentID,
							"branch_name":      oldBranch.BranchName,
							"branch_kind":      "feature",
							"base_branch":      "main",
							"head_sha":         oldBranch.HeadSHA,
							"review_doc_key":   oldBranch.ReviewDocKey,
							"write_scope_json": oldBranch.WriteScopeJSON,
							"status":           oldBranch.Status,
						},
						map[string]any{
							"branch_id":        retryBranchID,
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"repo_id":          oldBranch.RepoID,
							"checkout_id":      "checkout-abandoned-retry",
							"agent_id":         oldBranch.AgentID,
							"branch_name":      oldBranch.BranchName,
							"branch_kind":      "feature",
							"base_branch":      "main",
							"write_scope_json": oldBranch.WriteScopeJSON,
							"status":           "ABANDONED",
						},
					},
				},
			})
		case "project.checkout.register":
			checkoutRegisterParams = req.Params
			if rpcString(req.Params, "local_path") != expectedCheckoutPath {
				t.Fatalf("fresh successor must materialize default implementation checkout, got %+v", req.Params)
			}
			if rpcString(req.Params, "branch_name") != successorBranchName || rpcString(req.Params, "base_branch") != "main" {
				t.Fatalf("expected fresh successor from main, got %+v", req.Params)
			}
			assertProjectAdmissionGitCheckout(t, expectedCheckoutPath, remoteURL, successorBranchName)
			writeRPCResult(w, req, map[string]any{
				"checkout": map[string]any{
					"checkout_id":   "checkout-new",
					"workspace_id":  "ws",
					"project_id":    "project-alpha",
					"repo_id":       "repo-main",
					"machine_id":    rpcString(req.Params, "machine_id"),
					"agent_id":      "delta",
					"local_path":    expectedCheckoutPath,
					"checkout_kind": "clone",
					"branch_name":   successorBranchName,
					"base_branch":   "main",
					"dirty_state":   rpcString(req.Params, "dirty_state"),
					"status":        "ACTIVE",
				},
			})
		case "project.branch.register":
			branchRegisterParams = req.Params
			if rpcString(req.Params, "branch_id") != "" {
				t.Fatalf("READY_FOR_REVIEW predecessor must not be mutated for fresh implementation: %+v", req.Params)
			}
			if rpcString(req.Params, "checkout_id") != "checkout-new" || rpcString(req.Params, "branch_name") != successorBranchName || rpcString(req.Params, "base_branch") != "main" {
				t.Fatalf("unexpected fresh successor branch register params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"branch": map[string]any{
					"branch_id":        "branch-successor",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"repo_id":          "repo-main",
					"checkout_id":      "checkout-new",
					"agent_id":         "delta",
					"branch_name":      successorBranchName,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"write_scope_json": oldBranch.WriteScopeJSON,
					"status":           "RESERVED",
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "delta",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-eval",
		Title:               "Implement roster-built Lua eval/runtime for 0-of-34 conformance gap",
		Description:         "Build the first real evaluator/runtime increment in internal/eval. Do not use third-party interpreter/runtime dependencies.",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		ClaimBranchID:       stringPtr(retryBranchID),
		RequiresProjectGate: &requiresProjectGate,
	}, "claim eval runtime gap", nil)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.checkout.register,project.branch.register,agent.task.claim,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if checkoutRegisterParams == nil || branchRegisterParams == nil {
		t.Fatalf("expected fresh checkout and successor branch registration")
	}
	if rpcString(claimParams, "branch_id") != "branch-successor" || rpcString(claimParams, "checkout_id") != "checkout-new" {
		t.Fatalf("expected claim to bind successor branch/checkout, got %+v", claimParams)
	}
	if rpcString(claimParams, "branch_id") == oldBranch.BranchID {
		t.Fatalf("claim reused stale READY_FOR_REVIEW branch: %+v", claimParams)
	}
}

func TestRuntimeClaimTaskForksBlockedPatchQueueBranchHintForImplementation(t *testing.T) {
	workdir := t.TempDir()
	oldCheckoutPath := filepath.Join(workdir, "old-review-checkout")
	remoteURL := initProjectAdmissionRemote(t)
	oldBranchName := "agent/gamma/project-alpha/task-original"
	newBranchName := projectClaimBranchName("gamma", "project-alpha", "task-patchq-revision")
	repo := ProjectRepositoryRecord{
		RepoID:        "repo-main",
		Name:          "project-alpha",
		RemoteURL:     remoteURL,
		DefaultBranch: "main",
		RepoStatus:    "READY",
		IsCanonical:   true,
	}
	expectedCheckoutPath := projectCheckoutMaterializeDefaultPath(workdir, "project-alpha", repo)
	if _, _, err := materializeGitCheckout(context.Background(), oldCheckoutPath, repo, "main", true); err != nil {
		t.Fatalf("materialize old checkout: %v", err)
	}
	if err := checkoutProjectClaimBranch(context.Background(), oldCheckoutPath, oldBranchName, "main"); err != nil {
		t.Fatalf("checkout old branch: %v", err)
	}
	runProjectAdmissionGit(t, oldCheckoutPath, "config", "user.email", "test@example.invalid")
	runProjectAdmissionGit(t, oldCheckoutPath, "config", "user.name", "Rhizome Test")
	if err := os.MkdirAll(filepath.Join(oldCheckoutPath, "src"), 0o755); err != nil {
		t.Fatalf("create src: %v", err)
	}
	if err := os.WriteFile(filepath.Join(oldCheckoutPath, "src", "App.tsx"), []byte("export const app = 'old candidate'\n"), 0o644); err != nil {
		t.Fatalf("write old candidate: %v", err)
	}
	runProjectAdmissionGit(t, oldCheckoutPath, "add", "src/App.tsx")
	runProjectAdmissionGit(t, oldCheckoutPath, "commit", "-m", "old candidate")
	runProjectAdmissionGit(t, oldCheckoutPath, "push", "-u", "origin", oldBranchName)
	oldHead := strings.TrimSpace(mustProjectAdmissionGitOutput(t, oldCheckoutPath, "rev-parse", "HEAD"))

	var methods []string
	var claimParams map[string]any
	var checkoutRegisterParams map[string]any
	var branchRegisterParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-worker",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "gamma",
							"role_type":        "implementer",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["src/**","tests/**"]}`,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"name":           "project-alpha",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
					"checkouts": []any{
						map[string]any{
							"checkout_id":    "checkout-old-review",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"repo_id":        "repo-main",
							"machine_id":     "machine-a",
							"agent_id":       "gamma",
							"local_path":     oldCheckoutPath,
							"checkout_kind":  "review",
							"branch_name":    oldBranchName,
							"active_task_id": "task-original",
							"dirty_state":    "CLEAN",
							"status":         "ACTIVE",
						},
					},
					"branches": []any{
						map[string]any{
							"branch_id":        "branch-blocked",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"repo_id":          "repo-main",
							"checkout_id":      "checkout-old-review",
							"agent_id":         "gamma",
							"active_task_id":   "task-original",
							"active_claim_id":  "task-original",
							"branch_name":      oldBranchName,
							"branch_kind":      "feature",
							"base_branch":      "main",
							"head_sha":         oldHead,
							"write_scope_json": `{"paths":["src/**","tests/**"]}`,
							"status":           "READY_FOR_REVIEW",
						},
					},
					"tasks": []any{
						map[string]any{
							"task_id":                "task-original",
							"title":                  "Original candidate implementation",
							"owner_user_id":          "owner-1",
							"priority":               "high",
							"status":                 "IN_PROGRESS",
							"task_kind":              "EXECUTION",
							"project_id":             "project-alpha",
							"project_lane":           "implementation",
							"claim_agent_id":         "gamma",
							"claim_status":           "CLAIMED",
							"claim_repo_id":          "repo-main",
							"claim_branch_id":        "branch-blocked",
							"claim_write_scope_json": `{"paths":["src/**","tests/**"]}`,
						},
					},
					"patch_queue_items": []any{
						map[string]any{
							"queue_id":         "patchq-1",
							"item_id":          "patchitem-1",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"repo_id":          "repo-main",
							"branch_id":        "branch-blocked",
							"state":            "BLOCKED",
							"head_sha":         oldHead,
							"decision_summary": "Missing automated test evidence; revise candidate.",
						},
					},
				},
			})
		case "project.checkout.register":
			checkoutRegisterParams = req.Params
			if rpcString(req.Params, "local_path") != expectedCheckoutPath {
				t.Fatalf("blocked branch revision must materialize a fresh implementation checkout, got %+v", req.Params)
			}
			if rpcString(req.Params, "branch_name") != newBranchName || rpcString(req.Params, "base_branch") != oldBranchName {
				t.Fatalf("expected fresh task branch from blocked candidate base, got %+v", req.Params)
			}
			assertProjectAdmissionGitCheckout(t, expectedCheckoutPath, remoteURL, newBranchName)
			writeRPCResult(w, req, map[string]any{
				"checkout": map[string]any{
					"checkout_id":   "checkout-new",
					"workspace_id":  "ws",
					"project_id":    "project-alpha",
					"repo_id":       "repo-main",
					"machine_id":    rpcString(req.Params, "machine_id"),
					"agent_id":      "gamma",
					"local_path":    expectedCheckoutPath,
					"checkout_kind": "clone",
					"branch_name":   newBranchName,
					"base_branch":   oldBranchName,
					"dirty_state":   rpcString(req.Params, "dirty_state"),
					"status":        "ACTIVE",
				},
			})
		case "project.branch.register":
			branchRegisterParams = req.Params
			if rpcString(req.Params, "branch_id") != "" {
				t.Fatalf("blocked patch queue branch_id must not be mutated for a revision claim: %+v", req.Params)
			}
			if rpcString(req.Params, "checkout_id") != "checkout-new" || rpcString(req.Params, "branch_name") != newBranchName || rpcString(req.Params, "base_branch") != oldBranchName {
				t.Fatalf("unexpected fresh branch register params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"branch": map[string]any{
					"branch_id":        "branch-new",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"repo_id":          "repo-main",
					"checkout_id":      "checkout-new",
					"agent_id":         "gamma",
					"branch_name":      newBranchName,
					"branch_kind":      "feature",
					"base_branch":      oldBranchName,
					"write_scope_json": `{"paths":["src/**","tests/**"]}`,
					"status":           "RESERVED",
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-patchq-revision",
		Title:               "Revise blocked candidate for missing evidence",
		Description:         "Create a revision on branch provenance `branch-blocked` at head `" + oldHead + "` for blocked patch queue item `patchitem-1` in queue `patchq-1`. Produce the smallest product revision on an owned implementation branch, publish review-ready evidence for the new head, and request fresh validation.",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		Tags:                []string{"project", "patch-queue", "revision"},
		RequiresProjectGate: &requiresProjectGate,
	}, "claim patch queue revision", nil)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.checkout.register,project.branch.register,agent.task.claim,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if checkoutRegisterParams == nil || branchRegisterParams == nil {
		t.Fatalf("expected fresh checkout and branch registration")
	}
	if rpcString(claimParams, "branch_id") != "branch-new" || rpcString(claimParams, "checkout_id") != "checkout-new" {
		t.Fatalf("expected claim to bind fresh revision branch/checkout, got %+v", claimParams)
	}
	assertProjectAdmissionGitCheckout(t, oldCheckoutPath, remoteURL, oldBranchName)
}

func TestRuntimeClaimTaskRegistersProjectCheckoutBranchAndScope(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	expectedCheckoutPath := projectCheckoutMaterializeDefaultPath(workdir, "project-alpha", ProjectRepositoryRecord{
		RepoID: "repo-main",
		Name:   "project-alpha",
	})
	expectedBranchName := projectClaimBranchName("agent-1", "project-alpha", "task-build")
	var methods []string
	var claimParams map[string]any
	var claimUpdateParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			if rpcString(req.Params, "workspace_id") != "ws" || rpcString(req.Params, "project_id") != "project-alpha" {
				t.Fatalf("unexpected project.coordination.get params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"description":  "Build the thing",
						"status":       "ACTIVE",
						"created_by":   "owner",
						"created_at":   "2026-04-28T00:00:00Z",
						"updated_at":   "2026-04-28T00:00:00Z",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"goal":                "Build the thing",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
						"created_at":          "2026-04-28T00:00:00Z",
						"updated_at":          "2026-04-28T00:00:00Z",
						"implementation_plan": "plan",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-worker",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "agent-1",
							"role_type":        "implementer",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["agent/**"]}`,
							"created_at":       "2026-04-28T00:00:00Z",
							"updated_at":       "2026-04-28T00:00:00Z",
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"remote_kind":    "github",
							"name":           "project-alpha",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
							"created_at":     "2026-04-28T00:00:00Z",
							"updated_at":     "2026-04-28T00:00:00Z",
						},
					},
					"tasks": []any{
						map[string]any{
							"task_id":      "task-build",
							"title":        "Build shared implementation",
							"status":       "PENDING",
							"priority":     "high",
							"project_id":   "project-alpha",
							"project_lane": "implementation",
							"updated_at":   "2026-04-28T00:00:00Z",
						},
					},
				},
			})
		case "project.checkout.register":
			if rpcString(req.Params, "repo_id") != "repo-main" || rpcString(req.Params, "agent_id") != "agent-1" || rpcString(req.Params, "local_path") != expectedCheckoutPath {
				t.Fatalf("unexpected project.checkout.register params: %+v", req.Params)
			}
			if rpcString(req.Params, "active_task_id") != "" || rpcString(req.Params, "active_claim_id") != "" {
				t.Fatalf("pre-claim checkout registration must not carry active refs before agent.task.claim binds them: %+v", req.Params)
			}
			if rpcString(req.Params, "branch_name") != expectedBranchName {
				t.Fatalf("expected deterministic agent task branch name, got %+v", req.Params)
			}
			assertProjectAdmissionGitCheckout(t, expectedCheckoutPath, remoteURL, rpcString(req.Params, "branch_name"))
			writeRPCResult(w, req, map[string]any{
				"checkout": map[string]any{
					"checkout_id":   "checkout-1",
					"workspace_id":  "ws",
					"project_id":    "project-alpha",
					"repo_id":       "repo-main",
					"machine_id":    rpcString(req.Params, "machine_id"),
					"agent_id":      "agent-1",
					"local_path":    expectedCheckoutPath,
					"checkout_kind": "clone",
					"branch_name":   rpcString(req.Params, "branch_name"),
					"dirty_state":   rpcString(req.Params, "dirty_state"),
					"status":        "ACTIVE",
					"last_seen_at":  "2026-04-28T00:00:00Z",
					"created_at":    "2026-04-28T00:00:00Z",
					"updated_at":    "2026-04-28T00:00:00Z",
				},
			})
		case "project.branch.register":
			if rpcString(req.Params, "checkout_id") != "checkout-1" || rpcString(req.Params, "write_scope_json") != `{"paths":["agent/**"]}` {
				t.Fatalf("unexpected project.branch.register params: %+v", req.Params)
			}
			if rpcString(req.Params, "active_task_id") != "" || rpcString(req.Params, "active_claim_id") != "" {
				t.Fatalf("pre-claim branch registration must not carry active refs before agent.task.claim binds them: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"branch": map[string]any{
					"branch_id":        "branch-1",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"repo_id":          "repo-main",
					"checkout_id":      "checkout-1",
					"agent_id":         "agent-1",
					"branch_name":      rpcString(req.Params, "branch_name"),
					"branch_kind":      "feature",
					"base_branch":      "main",
					"write_scope_json": `{"paths":["agent/**"]}`,
					"status":           "RESERVED",
					"created_at":       "2026-04-28T00:00:00Z",
					"updated_at":       "2026-04-28T00:00:00Z",
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			claimUpdateParams = req.Params
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	work := &AgentWorkNextResult{Packet: &AgentWorkPacket{}}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-build",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", work)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if !strings.Contains(string(work.ProjectCoordination), "task-build") || !strings.Contains(string(work.Packet.ProjectCoordination), "task-build") {
		t.Fatalf("expected fetched coordination to be attached to work and packet, top=%s packet=%s", work.ProjectCoordination, work.Packet.ProjectCoordination)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.checkout.register,project.branch.register,agent.task.claim,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if !strings.Contains(rpcString(claimUpdateParams, "payload_json"), `"delegation_state":"claim_admitted"`) ||
		!strings.Contains(rpcString(claimUpdateParams, "payload_json"), `"branch_bound":true`) {
		t.Fatalf("expected claim-admitted evidence update, got %+v", claimUpdateParams)
	}
	for key, want := range map[string]string{
		"workspace_id":     "ws",
		"agent_id":         "agent-1",
		"task_id":          "task-build",
		"project_role_id":  "role-worker",
		"repo_id":          "repo-main",
		"checkout_id":      "checkout-1",
		"branch_id":        "branch-1",
		"write_scope_json": `{"paths":["agent/**"]}`,
		"summary":          "claim project task",
	} {
		if got := rpcString(claimParams, key); got != want {
			encoded, _ := json.Marshal(claimParams)
			t.Fatalf("%s = %q, want %q; claim=%s", key, got, want, encoded)
		}
	}
}

func TestRuntimeClaimTaskNarrowsBroadRoleScopeFromTaskDocHints(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	expectedCheckoutPath := projectCheckoutMaterializeDefaultPath(workdir, "project-alpha", ProjectRepositoryRecord{
		RepoID: "repo-main",
		Name:   "project-alpha",
	})
	expectedBranchName := projectClaimBranchName("gamma", "project-alpha", "task-pipeline")
	var methods []string
	var claimParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-broad-worker",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "gamma",
							"role_type":        "implementer",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["**"]}`,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"name":           "project-alpha",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
				},
			})
		case "project.checkout.register":
			if rpcString(req.Params, "local_path") != expectedCheckoutPath || rpcString(req.Params, "branch_name") != expectedBranchName {
				t.Fatalf("unexpected checkout register params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"checkout": map[string]any{
					"checkout_id":   "checkout-gamma",
					"workspace_id":  "ws",
					"project_id":    "project-alpha",
					"repo_id":       "repo-main",
					"machine_id":    rpcString(req.Params, "machine_id"),
					"agent_id":      "gamma",
					"local_path":    expectedCheckoutPath,
					"checkout_kind": "clone",
					"branch_name":   expectedBranchName,
					"dirty_state":   "CLEAN",
					"status":        "ACTIVE",
					"last_seen_at":  "2026-05-07T00:00:00Z",
					"created_at":    "2026-05-07T00:00:00Z",
					"updated_at":    "2026-05-07T00:00:00Z",
				},
			})
		case "project.branch.register":
			if got := rpcString(req.Params, "write_scope_json"); got != `{"paths":["src/lib/**","tests/**"]}` {
				t.Fatalf("expected task-doc scope to narrow broad role scope, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"branch": map[string]any{
					"branch_id":        "branch-gamma",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"repo_id":          "repo-main",
					"checkout_id":      "checkout-gamma",
					"agent_id":         "gamma",
					"branch_name":      expectedBranchName,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"write_scope_json": `{"paths":["src/lib/**","tests/**"]}`,
					"status":           "RESERVED",
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		case "workspace.doc.get":
			t.Fatalf("hydrated task doc should avoid extra task doc read")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	work := &AgentWorkNextResult{
		Packet: &AgentWorkPacket{},
		Hydration: &TaskHydrationBundle{
			Docs: []WorkspaceDocRecord{
				{
					DocKey:  "task.task-pipeline",
					Title:   "Task Brief - Pipeline",
					Content: "# Task Brief\n\n- task_id: task-pipeline\n- write_scope_hints: src/lib/**, tests/**\n\nImplement the algorithm lane.",
				},
			},
		},
	}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-pipeline",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", work)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.checkout.register,project.branch.register,agent.task.claim,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if got := rpcString(claimParams, "write_scope_json"); got != `{"paths":["src/lib/**","tests/**"]}` {
		t.Fatalf("claim write scope = %q", got)
	}
	if got := rpcString(claimParams, "project_role_id"); got != "role-broad-worker" {
		t.Fatalf("claim project_role_id = %q", got)
	}
}

func TestRuntimeTrustFirstClaimMaterializesBranchWithoutProjectRole(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	expectedBranchName := projectClaimBranchName("gamma", "project-alpha", "task-pipeline")
	var methods []string
	var claimParams map[string]any
	var branchParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"strategic_lead": map[string]any{
						"role_id":          "role-lead",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "alpha",
						"role_type":        "STRATEGIC_LEAD",
						"status":           "ACTIVE",
						"write_scope_json": `{}`,
					},
					"roles": []any{},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"name":           "project-alpha",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
				},
			})
		case "project.checkout.register":
			if rpcString(req.Params, "branch_name") != expectedBranchName {
				t.Fatalf("checkout branch_name = %q", rpcString(req.Params, "branch_name"))
			}
			writeRPCResult(w, req, map[string]any{
				"checkout": map[string]any{
					"checkout_id":   "checkout-gamma",
					"workspace_id":  "ws",
					"project_id":    "project-alpha",
					"repo_id":       "repo-main",
					"machine_id":    rpcString(req.Params, "machine_id"),
					"agent_id":      "gamma",
					"local_path":    rpcString(req.Params, "local_path"),
					"checkout_kind": "clone",
					"branch_name":   expectedBranchName,
					"dirty_state":   "CLEAN",
					"status":        "ACTIVE",
				},
			})
		case "project.branch.register":
			branchParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"branch": map[string]any{
					"branch_id":        "branch-gamma",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"repo_id":          "repo-main",
					"checkout_id":      "checkout-gamma",
					"agent_id":         "gamma",
					"branch_name":      expectedBranchName,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"write_scope_json": `{"paths":["src/lib/**","tests/**"]}`,
					"status":           "RESERVED",
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			OwnerUserID:      "owner-1",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	work := &AgentWorkNextResult{
		Packet: &AgentWorkPacket{
			Frontier: &AgentWorkTaskFrontier{
				GenerationID:   "frontier-1",
				SelectedTaskID: "task-pipeline",
				SelfFitSummary: "gamma has the pipeline/test profile and no active owned work",
			},
		},
		Hydration: &TaskHydrationBundle{
			Docs: []WorkspaceDocRecord{
				{
					DocKey:  "task.task-pipeline",
					Content: "# Task Brief\n\n- task_id: task-pipeline\n- write_scope_hints: src/lib/**, tests/**\n",
				},
			},
		},
	}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:      "task-pipeline",
		ProjectID:   "project-alpha",
		ProjectLane: "implementation",
	}, "claim project task", work)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.checkout.register,project.branch.register,agent.task.claim,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if got := rpcString(branchParams, "write_scope_json"); got != `{"paths":["src/lib/**","tests/**"]}` {
		t.Fatalf("branch write scope = %q", got)
	}
	if got := rpcString(claimParams, "project_role_id"); got != "" {
		t.Fatalf("trust-first branch-backed claim should not require project_role_id, got %q", got)
	}
	if got := rpcString(claimParams, "write_scope_json"); got != `{"paths":["src/lib/**","tests/**"]}` {
		t.Fatalf("claim write scope = %q", got)
	}
	if selected, _ := claimParams["selected_from_frontier"].(bool); !selected {
		t.Fatalf("expected selected_from_frontier claim evidence, got %+v", claimParams)
	}
}

func TestRuntimeTrustFirstClaimFailsClosedWhenCheckoutRegistrationFails(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	expectedBranchName := projectClaimBranchName("gamma", "project-alpha", "task-pipeline")
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-gamma-impl",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "gamma",
							"role_type":        "IMPLEMENTER",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["src/**","tests/**"]}`,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"name":           "project-alpha",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
				},
			})
		case "project.checkout.register":
			if rpcString(req.Params, "branch_name") != expectedBranchName {
				t.Fatalf("checkout branch_name = %q", rpcString(req.Params, "branch_name"))
			}
			writeRPCError(w, req, -32603, "checkout registry unavailable")
		case "agent.task.claim":
			t.Fatalf("agent.task.claim must not run after checkout materialization failure")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			OwnerUserID:      "owner-1",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-pipeline",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", nil)
	if err == nil || !strings.Contains(err.Error(), "checkout registry unavailable") {
		t.Fatalf("expected checkout materialization failure, got %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.checkout.register" {
		t.Fatalf("unexpected method order: %s", got)
	}
}

func TestRuntimeTrustFirstClaimFailsClosedWhenBranchRegistrationFails(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	expectedBranchName := projectClaimBranchName("gamma", "project-alpha", "task-pipeline")
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-gamma-impl",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "gamma",
							"role_type":        "IMPLEMENTER",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["src/**","tests/**"]}`,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"name":           "project-alpha",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
				},
			})
		case "project.checkout.register":
			if rpcString(req.Params, "branch_name") != expectedBranchName {
				t.Fatalf("checkout branch_name = %q", rpcString(req.Params, "branch_name"))
			}
			writeRPCResult(w, req, map[string]any{
				"checkout": map[string]any{
					"checkout_id":   "checkout-gamma",
					"workspace_id":  "ws",
					"project_id":    "project-alpha",
					"repo_id":       "repo-main",
					"machine_id":    rpcString(req.Params, "machine_id"),
					"agent_id":      "gamma",
					"local_path":    rpcString(req.Params, "local_path"),
					"checkout_kind": "clone",
					"branch_name":   expectedBranchName,
					"dirty_state":   "CLEAN",
					"status":        "ACTIVE",
				},
			})
		case "project.branch.register":
			if rpcString(req.Params, "branch_name") != expectedBranchName {
				t.Fatalf("branch_name = %q", rpcString(req.Params, "branch_name"))
			}
			writeRPCError(w, req, -32603, "branch registry unavailable")
		case "agent.task.claim":
			t.Fatalf("agent.task.claim must not run after branch materialization failure")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			OwnerUserID:      "owner-1",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-pipeline",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", nil)
	if err == nil || !strings.Contains(err.Error(), "branch registry unavailable") {
		t.Fatalf("expected branch materialization failure, got %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.checkout.register,project.branch.register" {
		t.Fatalf("unexpected method order: %s", got)
	}
}

func TestRuntimeTrustFirstClaimPrefersTaskScopeOverRepairRoleScope(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	expectedBranchName := projectClaimBranchName("gamma", "project-alpha", "task-dashboard")
	var claimParams map[string]any
	var branchParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"strategic_lead": map[string]any{
						"role_id":          "role-lead",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "alpha",
						"role_type":        "STRATEGIC_LEAD",
						"status":           "ACTIVE",
						"write_scope_json": `{}`,
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-gamma-config",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "gamma",
							"role_type":        "IMPLEMENTER",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["package*.json","vite.config.*","index.html"]}`,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"name":           "project-alpha",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
				},
			})
		case "project.checkout.register":
			writeRPCResult(w, req, map[string]any{
				"checkout": map[string]any{
					"checkout_id":   "checkout-gamma",
					"workspace_id":  "ws",
					"project_id":    "project-alpha",
					"repo_id":       "repo-main",
					"machine_id":    rpcString(req.Params, "machine_id"),
					"agent_id":      "gamma",
					"local_path":    rpcString(req.Params, "local_path"),
					"checkout_kind": "clone",
					"branch_name":   expectedBranchName,
					"dirty_state":   "CLEAN",
					"status":        "ACTIVE",
				},
			})
		case "project.branch.register":
			branchParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"branch": map[string]any{
					"branch_id":        "branch-gamma",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"repo_id":          "repo-main",
					"checkout_id":      "checkout-gamma",
					"agent_id":         "gamma",
					"branch_name":      expectedBranchName,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"write_scope_json": rpcString(req.Params, "write_scope_json"),
					"status":           "RESERVED",
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			OwnerUserID:      "owner-1",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	work := &AgentWorkNextResult{
		Hydration: &TaskHydrationBundle{
			Docs: []WorkspaceDocRecord{{
				DocKey:  "task.task-dashboard",
				Content: "# Task Brief\n\n- task_id: task-dashboard\n- write_scope_hints: src/**, public/**\n",
			}},
		},
	}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:      "task-dashboard",
		ProjectID:   "project-alpha",
		ProjectLane: "implementation",
	}, "claim dashboard task", work)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := rpcString(branchParams, "write_scope_json"); got != `{"paths":["src/**","public/**"]}` {
		t.Fatalf("trust-first branch scope should come from task hints, got %q", got)
	}
	if got := rpcString(claimParams, "write_scope_json"); got != `{"paths":["src/**","public/**"]}` {
		t.Fatalf("trust-first claim scope should come from task hints, got %q", got)
	}
	if got := rpcString(claimParams, "project_role_id"); got != "" {
		t.Fatalf("trust-first task-scope claim should not bind stale repair role, got %q", got)
	}
}

func TestRuntimeTrustFirstClaimKeepsActiveClaimScopeOverTaskHints(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	expectedBranchName := projectClaimBranchName("gamma", "project-alpha", "task-dashboard")
	const expandedScope = `{"paths":["package.json","index.html","public/**","src/**"]}`
	var claimParams map[string]any
	var branchParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"name":           "project-alpha",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
					"tasks": []any{
						map[string]any{
							"task_id":                "task-dashboard",
							"workspace_id":           "ws",
							"project_id":             "project-alpha",
							"project_lane":           "implementation",
							"status":                 "RUNNING",
							"claim_status":           "CLAIMED",
							"claim_agent_id":         "gamma",
							"claim_project_role_id":  "role-gamma-expanded",
							"claim_write_scope_json": expandedScope,
						},
					},
				},
			})
		case "project.checkout.register":
			writeRPCResult(w, req, map[string]any{
				"checkout": map[string]any{
					"checkout_id":   "checkout-gamma",
					"workspace_id":  "ws",
					"project_id":    "project-alpha",
					"repo_id":       "repo-main",
					"machine_id":    rpcString(req.Params, "machine_id"),
					"agent_id":      "gamma",
					"local_path":    rpcString(req.Params, "local_path"),
					"checkout_kind": "clone",
					"branch_name":   expectedBranchName,
					"dirty_state":   "CLEAN",
					"status":        "ACTIVE",
				},
			})
		case "project.branch.register":
			branchParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"branch": map[string]any{
					"branch_id":        "branch-gamma",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"repo_id":          "repo-main",
					"checkout_id":      "checkout-gamma",
					"agent_id":         "gamma",
					"branch_name":      expectedBranchName,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"write_scope_json": rpcString(req.Params, "write_scope_json"),
					"status":           "RESERVED",
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			OwnerUserID:      "owner-1",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	work := &AgentWorkNextResult{
		Hydration: &TaskHydrationBundle{
			Docs: []WorkspaceDocRecord{{
				DocKey:  "task.task-dashboard",
				Content: "# Task Brief\n\n- task_id: task-dashboard\n- write_scope_hints: src/auth/**\n",
			}},
		},
	}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:      "task-dashboard",
		ProjectID:   "project-alpha",
		ProjectLane: "implementation",
	}, "resume dashboard task", work)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := rpcString(branchParams, "write_scope_json"); got != expandedScope {
		t.Fatalf("branch scope should keep active claim boundary, got %q", got)
	}
	if got := rpcString(claimParams, "write_scope_json"); got != expandedScope {
		t.Fatalf("claim scope should keep active claim boundary, got %q", got)
	}
	if got := rpcString(claimParams, "project_role_id"); got != "role-gamma-expanded" {
		t.Fatalf("claim should keep active claim role binding, got %q", got)
	}
}

func TestRuntimeTrustFirstRevisionBroadTaskScopeCanUseNarrowRepairRoleScope(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	expectedBranchName := projectClaimBranchName("beta", "project-alpha", "task-revise-blocked-candidate")
	const uiScope = `{"paths":["package*.json","vite.config.*","tsconfig*.json","index.html","public/**","src/main.*","src/App.*","src/components/**","src/styles/**","src/ui/**"]}`
	var branchParams map[string]any
	var claimParams map[string]any
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-beta-ui",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "beta",
							"role_type":        "IMPLEMENTER",
							"status":           "ACTIVE",
							"write_scope_json": uiScope,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"name":           "project-alpha",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
					"tasks": []any{
						map[string]any{
							"task_id":                "task-data-model",
							"workspace_id":           "ws",
							"project_id":             "project-alpha",
							"project_lane":           "implementation",
							"status":                 "RUNNING",
							"task_kind":              "EXECUTION",
							"claim_status":           "CLAIMED",
							"claim_agent_id":         "delta",
							"claim_repo_id":          "repo-main",
							"claim_branch_id":        "branch-delta",
							"claim_write_scope_json": `{"paths":["src/data/**","src/types/**","src/lib/**"]}`,
						},
					},
					"branches": []any{
						map[string]any{
							"branch_id":        "branch-delta",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"repo_id":          "repo-main",
							"agent_id":         "delta",
							"active_task_id":   "task-data-model",
							"active_claim_id":  "task-data-model",
							"branch_name":      "agent-delta-data",
							"branch_kind":      "feature",
							"base_branch":      "main",
							"head_sha":         "head-delta",
							"review_doc_key":   "project.project-alpha.branch.branch-delta.review",
							"write_scope_json": `{"paths":["src/data/**","src/types/**","src/lib/**"]}`,
							"status":           "READY_FOR_REVIEW",
						},
					},
				},
			})
		case "project.checkout.register":
			writeRPCResult(w, req, map[string]any{
				"checkout": map[string]any{
					"checkout_id":   "checkout-beta",
					"workspace_id":  "ws",
					"project_id":    "project-alpha",
					"repo_id":       "repo-main",
					"machine_id":    rpcString(req.Params, "machine_id"),
					"agent_id":      "beta",
					"local_path":    rpcString(req.Params, "local_path"),
					"checkout_kind": "clone",
					"branch_name":   expectedBranchName,
					"dirty_state":   "CLEAN",
					"status":        "ACTIVE",
				},
			})
		case "project.branch.register":
			branchParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"branch": map[string]any{
					"branch_id":        "branch-beta",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"repo_id":          "repo-main",
					"checkout_id":      "checkout-beta",
					"agent_id":         "beta",
					"branch_name":      expectedBranchName,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"write_scope_json": rpcString(req.Params, "write_scope_json"),
					"status":           "RESERVED",
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "beta",
			OwnerUserID:      "owner-1",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:          "task-revise-blocked-candidate",
		Title:           "Revise blocked candidate branch-delta into a runnable frontend",
		Description:     "Patch queue follow-up.\n\n- queue_id: patchq-1\n- item_id: patchitem-1\n- branch_id: branch-delta\n- head_sha: head-delta\n- state: BLOCKED\n- candidate_pathset: src/**, package.json, index.html\n",
		ProjectID:       "project-alpha",
		ProjectLane:     "implementation",
		WriteScopeHints: []string{"src/**", "package.json", "package-lock.json", "vite.config.*", "tsconfig*.json", "index.html"},
		Tags:            []string{"project", "revision", "frontend", "validation-followup"},
	}, "claim revised frontend lane", nil)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.checkout.register,project.branch.register,agent.task.claim,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if got := rpcString(branchParams, "write_scope_json"); got != uiScope {
		t.Fatalf("revision repair should bind narrowed role scope, got %q", got)
	}
	if got := rpcString(claimParams, "write_scope_json"); got != uiScope {
		t.Fatalf("claim should use narrowed role scope, got %q", got)
	}
	if got := rpcString(claimParams, "project_role_id"); got != "role-beta-ui" {
		t.Fatalf("claim should bind the repaired UI role, got %q", got)
	}
}

func TestRuntimeTrustFirstClaimPreflightsOverlapBeforeBranchMaterialization(t *testing.T) {
	workdir := t.TempDir()
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"strategic_lead": map[string]any{
						"role_id":          "role-lead",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "alpha",
						"role_type":        "STRATEGIC_LEAD",
						"status":           "ACTIVE",
						"write_scope_json": `{}`,
					},
					"roles": []any{},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     "file:///tmp/project-alpha.git",
							"name":           "project-alpha",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
					"tasks": []any{
						map[string]any{
							"task_id":                "task-risk-timeline",
							"workspace_id":           "ws",
							"project_id":             "project-alpha",
							"project_lane":           "implementation",
							"status":                 "RUNNING",
							"task_kind":              "EXECUTION",
							"claim_status":           "CLAIMED",
							"claim_agent_id":         "delta",
							"claim_repo_id":          "repo-main",
							"claim_branch_id":        "branch-delta",
							"claim_write_scope_json": `{"paths":["src/components/risks/**","src/components/timeline/**","src/components/shared/**","src/styles/**","src/data/**","src/types/**"]}`,
						},
					},
					"branches": []any{
						map[string]any{
							"branch_id":        "branch-delta",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"repo_id":          "repo-main",
							"agent_id":         "delta",
							"active_task_id":   "task-risk-timeline",
							"branch_name":      "agent/delta/project-alpha/task-risk-timeline",
							"branch_kind":      "feature",
							"base_branch":      "main",
							"write_scope_json": `{"paths":["src/components/risks/**","src/components/timeline/**","src/components/shared/**","src/styles/**","src/data/**","src/types/**"]}`,
							"status":           "ACTIVE",
						},
					},
				},
			})
		default:
			t.Fatalf("preflight should fail before side effects, got method %q with params %+v", req.Method, req.Params)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			OwnerUserID:      "owner-1",
			Workdir:          workdir,
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:          "task-workboard",
		ProjectID:       "project-alpha",
		ProjectLane:     "implementation",
		WriteScopeHints: []string{"src/components/workboard/**", "src/components/filters/**", "src/hooks/**", "src/state/**", "src/data/**", "src/types/**"},
	}, "claim project task", nil)
	if err == nil || !strings.Contains(err.Error(), "task claim project admission invalid") || !strings.Contains(err.Error(), "overlaps active claim") {
		t.Fatalf("expected preflight overlap admission error, got %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get" {
		t.Fatalf("unexpected method order: %s", got)
	}
}

func TestPreflightProjectClaimKeepsClaimScopeAuthoritativeWhenBranchIsNarrower(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID: "task-scaffold",
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-main"}
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:              "task-dashboard",
				ClaimStatus:         stringPtr("CLAIMED"),
				ClaimAgentID:        stringPtr("eta"),
				ClaimRepoID:         stringPtr("repo-main"),
				ClaimBranchID:       stringPtr("branch-eta"),
				ClaimWriteScopeJSON: stringPtr(`{"paths":["src/**","package.json","index.html"]}`),
			},
		},
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-eta",
				RepoID:         "repo-main",
				AgentID:        "eta",
				ActiveTaskID:   "task-dashboard",
				ActiveClaimID:  "task-dashboard",
				WriteScopeJSON: `{"paths":["src/components/sector-board/**"]}`,
				Status:         "ACTIVE",
			},
		},
	}
	err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["package.json","index.html"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{})
	if err == nil || !strings.Contains(err.Error(), "overlaps active claim") {
		t.Fatalf("expected persisted claim scope to remain authoritative when branch is narrower, got %v", err)
	}
}

func TestPreflightProjectClaimFallsBackToClaimScopeWhenBranchDoesNotMatchClaim(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID: "task-scaffold",
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-main"}
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:              "task-dashboard",
				ClaimStatus:         stringPtr("CLAIMED"),
				ClaimAgentID:        stringPtr("eta"),
				ClaimRepoID:         stringPtr("repo-main"),
				ClaimBranchID:       stringPtr("branch-eta"),
				ClaimWriteScopeJSON: stringPtr(`{"paths":["src/**","package.json","index.html"]}`),
			},
		},
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-eta",
				RepoID:         "repo-main",
				AgentID:        "eta",
				ActiveTaskID:   "task-something-else",
				ActiveClaimID:  "task-something-else",
				WriteScopeJSON: `{"paths":["src/components/sector-board/**"]}`,
				Status:         "ACTIVE",
			},
		},
	}
	err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["package.json","index.html"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{})
	if err == nil || !strings.Contains(err.Error(), "overlaps active claim") {
		t.Fatalf("expected stale claim scope to remain authoritative when branch binding mismatches, got %v", err)
	}
}

func TestPreflightProjectClaimFallsBackToClaimScopeWhenBranchHasNoActiveRefs(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID: "task-scaffold",
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-main"}
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:              "task-dashboard",
				ClaimStatus:         stringPtr("CLAIMED"),
				ClaimAgentID:        stringPtr("eta"),
				ClaimRepoID:         stringPtr("repo-main"),
				ClaimBranchID:       stringPtr("branch-eta"),
				ClaimWriteScopeJSON: stringPtr(`{"paths":["src/**","package.json","index.html"]}`),
			},
		},
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-eta",
				RepoID:         "repo-main",
				AgentID:        "eta",
				HeadSHA:        strings.Repeat("a", 40),
				ReviewDocKey:   "project.project-alpha.branch.branch-eta.review",
				WriteScopeJSON: `{"paths":["src/components/sector-board/**"]}`,
				Status:         "READY_FOR_REVIEW",
			},
		},
	}
	err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["package.json","index.html"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{})
	if err == nil || !strings.Contains(err.Error(), "overlaps active claim") {
		t.Fatalf("expected branch without active refs to fall back to broad claim scope, got %v", err)
	}
}

func TestPreflightProjectClaimIgnoresReservedIntegrationBranchScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-parser",
		ProjectLane: "implementation",
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-main"}
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:              "task-integrate",
				ProjectLane:         "integration",
				RequiresProjectGate: boolPtr(true),
			},
		},
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-zeta-integration",
				RepoID:         "repo-main",
				AgentID:        "zeta",
				ActiveTaskID:   "task-integrate",
				ActiveClaimID:  "task-integrate",
				WriteScopeJSON: `{"paths":["README.md","cmd/**","internal/**","testdata/**","go.mod","go.sum"]}`,
				Status:         "RESERVED",
			},
		},
	}
	err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["internal/parser/**","internal/ast/**"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{})
	if err != nil {
		t.Fatalf("reserved non-implementation integration branch should not block implementation preflight: %v", err)
	}
}

func TestPreflightProjectClaimReservedIntegrationBranchWithEvidenceBlocks(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-parser",
		ProjectLane: "implementation",
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-main"}
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:              "task-integrate",
				ProjectLane:         "integration",
				RequiresProjectGate: boolPtr(true),
			},
		},
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-zeta-integration",
				RepoID:         "repo-main",
				AgentID:        "zeta",
				ActiveTaskID:   "task-integrate",
				ActiveClaimID:  "task-integrate",
				HeadSHA:        strings.Repeat("a", 40),
				ReviewDocKey:   "project.project-rq.branch.branch-zeta-integration.review",
				WriteScopeJSON: `{"paths":["README.md","cmd/**","internal/**","testdata/**","go.mod","go.sum"]}`,
				Status:         "RESERVED",
			},
		},
	}
	err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["internal/parser/**","internal/ast/**"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{})
	if err == nil || !strings.Contains(err.Error(), "overlaps live branch") {
		t.Fatalf("evidence-bearing reserved integration branch must still block implementation preflight, got %v", err)
	}
}

func TestPreflightProjectClaimReviewGateParityIgnoresReservedReviewBranchScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-parser",
		ProjectLane: "implementation",
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-main"}
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:              "task-review",
				ProjectID:           "project-rq",
				ProjectLane:         "review",
				RequiresProjectGate: boolPtr(true),
			},
		},
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-epsilon-review",
				RepoID:         "repo-main",
				AgentID:        "epsilon",
				ActiveTaskID:   "task-review",
				ActiveClaimID:  "task-review",
				WriteScopeJSON: `{"paths":["internal/**","go.mod"]}`,
				Status:         "RESERVED",
			},
		},
	}
	err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["internal/parser/**","internal/ast/**"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{})
	if err != nil {
		t.Fatalf("reserved review branch without evidence must match server lane bypass and not block implementation preflight: %v", err)
	}
}

func TestPreflightProjectClaimStillBlocksReservedImplementationBranchScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-parser",
		ProjectLane: "implementation",
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-main"}
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:              "task-active-parser",
				ProjectID:           "project-rq",
				ProjectLane:         "implementation",
				RequiresProjectGate: boolPtr(true),
			},
		},
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-gamma-parser",
				RepoID:         "repo-main",
				AgentID:        "gamma",
				ActiveTaskID:   "task-active-parser",
				ActiveClaimID:  "task-active-parser",
				WriteScopeJSON: `{"paths":["internal/parser/**","internal/ast/**"]}`,
				Status:         "RESERVED",
			},
		},
	}
	err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["internal/parser/**"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{})
	if err == nil || !strings.Contains(err.Error(), "overlaps live branch") {
		t.Fatalf("reserved implementation branch must still block overlapping implementation claim, got %v", err)
	}
}

func TestPreflightProjectClaimAllowsRevisionPastOlderSameOwnerBlockedBranch(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-owner-revision",
		Title:       "Unblock integration candidate branch-source",
		Description: "Patch queue decision follow-up.\n\n- queue_id: queue-main\n- item_id: item-source\n- branch_id: branch-source\n- head_sha: " + strings.Repeat("b", 40) + "\n- state: BLOCKED\n",
		ProjectLane: "implementation",
		Tags:        []string{"project", "patch-queue", "revision", "blocked", "owner-bound", "owner-bound-kind:patch_queue_revision", "owner-branch:branch-source", "owner-agent:beta", "required-agent:beta"},
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-main"}
	sourceBranch := ProjectBranchRecord{
		BranchID:       "branch-source",
		RepoID:         "repo-main",
		AgentID:        "beta",
		HeadSHA:        strings.Repeat("b", 40),
		ReviewDocKey:   "project.project-alpha.branch.branch-source.review",
		WriteScopeJSON: `{"paths":["src/**","package.json"]}`,
		Status:         "READY_FOR_REVIEW",
	}
	coordination := ProjectCoordinationRecord{
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-predecessor",
				RepoID:         "repo-main",
				AgentID:        "beta",
				HeadSHA:        strings.Repeat("a", 40),
				ReviewDocKey:   "project.project-alpha.branch.branch-predecessor.review",
				WriteScopeJSON: `{"paths":["src/**","package.json"]}`,
				Status:         "READY_FOR_REVIEW",
			},
			sourceBranch,
		},
		PatchQueueItems: []ProjectPatchQueueItemRecord{
			{
				QueueID:   "queue-main",
				ItemID:    "item-predecessor",
				RepoID:    "repo-main",
				BranchID:  "branch-predecessor",
				State:     "BLOCKED",
				HeadSHA:   strings.Repeat("a", 40),
				DecidedAt: "2026-05-17T00:00:00Z",
				UpdatedAt: "2026-05-17T00:00:00Z",
			},
			{
				QueueID:   "queue-main",
				ItemID:    "item-source",
				RepoID:    "repo-main",
				BranchID:  "branch-source",
				State:     "BLOCKED",
				HeadSHA:   strings.Repeat("b", 40),
				DecidedAt: "2026-05-17T00:01:00Z",
				UpdatedAt: "2026-05-17T00:01:00Z",
			},
		},
	}
	err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["src/**","package.json"]}`, ProjectBranchRecord{}, sourceBranch, ProjectBranchRecord{})
	if err != nil {
		t.Fatalf("older same-owner blocked predecessor branch must not block revision preflight: %v", err)
	}
}

func TestPreflightProjectClaimBlocksRevisionWhenPredecessorStillActive(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-owner-revision",
		Title:       "Unblock integration candidate branch-source",
		Description: "Patch queue decision follow-up.\n\n- queue_id: queue-main\n- item_id: item-source\n- branch_id: branch-source\n- head_sha: " + strings.Repeat("b", 40) + "\n- state: BLOCKED\n",
		ProjectLane: "implementation",
		Tags:        []string{"project", "patch-queue", "revision", "blocked", "owner-bound", "owner-bound-kind:patch_queue_revision", "owner-branch:branch-source", "owner-agent:beta", "required-agent:beta"},
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-main"}
	sourceBranch := ProjectBranchRecord{
		BranchID:       "branch-source",
		RepoID:         "repo-main",
		AgentID:        "beta",
		HeadSHA:        strings.Repeat("b", 40),
		ReviewDocKey:   "project.project-alpha.branch.branch-source.review",
		WriteScopeJSON: `{"paths":["src/**","package.json"]}`,
		Status:         "READY_FOR_REVIEW",
	}
	coordination := ProjectCoordinationRecord{
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-predecessor",
				RepoID:         "repo-main",
				AgentID:        "beta",
				ActiveTaskID:   "task-predecessor-active",
				ActiveClaimID:  "claim-predecessor-active",
				HeadSHA:        strings.Repeat("a", 40),
				ReviewDocKey:   "project.project-alpha.branch.branch-predecessor.review",
				WriteScopeJSON: `{"paths":["src/**","package.json"]}`,
				Status:         "READY_FOR_REVIEW",
			},
			sourceBranch,
		},
		PatchQueueItems: []ProjectPatchQueueItemRecord{
			{
				QueueID:   "queue-main",
				ItemID:    "item-predecessor",
				RepoID:    "repo-main",
				BranchID:  "branch-predecessor",
				State:     "BLOCKED",
				HeadSHA:   strings.Repeat("a", 40),
				DecidedAt: "2026-05-17T00:00:00Z",
				UpdatedAt: "2026-05-17T00:00:00Z",
			},
			{
				QueueID:   "queue-main",
				ItemID:    "item-source",
				RepoID:    "repo-main",
				BranchID:  "branch-source",
				State:     "BLOCKED",
				HeadSHA:   strings.Repeat("b", 40),
				DecidedAt: "2026-05-17T00:01:00Z",
				UpdatedAt: "2026-05-17T00:01:00Z",
			},
		},
	}
	err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["src/**","package.json"]}`, ProjectBranchRecord{}, sourceBranch, ProjectBranchRecord{})
	if err == nil || !strings.Contains(err.Error(), "overlaps live branch") {
		t.Fatalf("active predecessor branch must still block revision preflight, got %v", err)
	}
}

func TestPreflightProjectClaimIgnoresInactiveTerminalPatchQueueBranchScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-eval-builtins",
		ProjectLane: "implementation",
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-rq"}
	branch := ProjectBranchRecord{
		BranchID:       "branch-blocked-eval-tests",
		RepoID:         "repo-rq",
		AgentID:        "beta",
		HeadSHA:        strings.Repeat("b", 40),
		ReviewDocKey:   "project.project-rq.branch.branch-blocked-eval-tests.review",
		WriteScopeJSON: `{"paths":["internal/eval/**"]}`,
		Status:         "READY_FOR_REVIEW",
	}
	for _, state := range []string{"BLOCKED", "REJECTED", "CANCELED", "INTEGRATED"} {
		coordination := ProjectCoordinationRecord{
			Branches: []ProjectBranchRecord{branch},
			PatchQueueItems: []ProjectPatchQueueItemRecord{{
				QueueID:  "queue-rq",
				ItemID:   "item-" + strings.ToLower(state),
				RepoID:   "repo-rq",
				BranchID: branch.BranchID,
				State:    state,
				HeadSHA:  branch.HeadSHA,
			}},
		}
		if err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["internal/eval/**"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{}); err != nil {
			t.Fatalf("%s inactive terminal patch queue branch scope should not block fresh product claim: %v", state, err)
		}
	}
}

func TestPreflightProjectClaimIgnoresIntegratedReadyBranchWithTerminalRefs(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-eval-gap",
		ProjectLane: "implementation",
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-rq"}
	ownerTaskID := "task-control-functions"
	completedClaim := "COMPLETED"
	branch := ProjectBranchRecord{
		BranchID:       "branch-integrated-eval",
		RepoID:         "repo-rq",
		AgentID:        "delta",
		ActiveTaskID:   ownerTaskID,
		ActiveClaimID:  ownerTaskID,
		HeadSHA:        strings.Repeat("d", 40),
		ReviewDocKey:   "project.project-rq.branch.branch-integrated-eval.review",
		WriteScopeJSON: `{"paths":["go.mod","go.sum","internal/eval/**","internal/runtime/**","internal/value/**","internal/runner/**"]}`,
		Status:         "READY_FOR_REVIEW",
	}
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:      ownerTaskID,
				Status:      "RESOLVED",
				ClaimStatus: &completedClaim,
			},
		},
		Branches: []ProjectBranchRecord{branch},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:  "queue-rq",
			ItemID:   "item-integrated",
			RepoID:   "repo-rq",
			BranchID: branch.BranchID,
			State:    "INTEGRATED",
			HeadSHA:  branch.HeadSHA,
		}},
	}
	if err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["go.mod","go.sum","internal/eval/**","internal/runtime/**","internal/value/**","internal/runner/**"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{}); err != nil {
		t.Fatalf("integrated ready branch with terminal refs should not block fresh product claim: %v", err)
	}

	coordination.Tasks[0].Status = "PENDING"
	if err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["internal/eval/**"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{}); err == nil || !strings.Contains(err.Error(), "overlaps live branch") {
		t.Fatalf("nonterminal active refs must still preserve ready branch scope, got %v", err)
	}
}

func TestPreflightProjectClaimKeepsAcceptedAndActiveBranchScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-eval-builtins",
		ProjectLane: "implementation",
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-rq"}
	branch := ProjectBranchRecord{
		BranchID:       "branch-accepted-eval",
		RepoID:         "repo-rq",
		AgentID:        "beta",
		HeadSHA:        strings.Repeat("c", 40),
		ReviewDocKey:   "project.project-rq.branch.branch-accepted-eval.review",
		WriteScopeJSON: `{"paths":["internal/eval/**"]}`,
		Status:         "READY_FOR_REVIEW",
	}
	coordination := ProjectCoordinationRecord{
		Branches: []ProjectBranchRecord{branch},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:  "queue-rq",
			ItemID:   "item-accepted",
			RepoID:   "repo-rq",
			BranchID: branch.BranchID,
			State:    "ACCEPTED",
			HeadSHA:  branch.HeadSHA,
		}},
	}
	if err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["internal/eval/**"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{}); err == nil || !strings.Contains(err.Error(), "overlaps live branch") {
		t.Fatalf("accepted pre-integration branch scope must still block overlapping product claim, got %v", err)
	}
	branch.ActiveTaskID = "task-live-owner"
	coordination = ProjectCoordinationRecord{
		Branches: []ProjectBranchRecord{branch},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:  "queue-rq",
			ItemID:   "item-blocked-active",
			RepoID:   "repo-rq",
			BranchID: branch.BranchID,
			State:    "BLOCKED",
			HeadSHA:  branch.HeadSHA,
		}},
	}
	if err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["internal/eval/**"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{}); err == nil || !strings.Contains(err.Error(), "overlaps live branch") {
		t.Fatalf("active branch binding must preserve scope even with terminal patch queue item, got %v", err)
	}
}

func TestProjectClaimAdmissionRevisionRetryKeepsSourceBranchAllowance(t *testing.T) {
	targetBranchID := "branch-target-retry"
	sourceBranchID := "branch-source"
	task := WorkspaceTaskRecord{
		TaskID:        "task-owner-revision",
		Title:         "Unblock integration candidate branch-source",
		Description:   "Patch queue decision follow-up.\n\n- queue_id: queue-main\n- item_id: item-source\n- branch_id: branch-source\n- head_sha: " + strings.Repeat("b", 40) + "\n- state: BLOCKED\n",
		ProjectLane:   "implementation",
		ClaimBranchID: stringPtr(targetBranchID),
		Tags:          []string{"project", "patch-queue", "revision", "blocked", "owner-bound", "owner-bound-kind:patch_queue_revision", "owner-branch:branch-source", "owner-agent:beta", "required-agent:beta"},
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-main"}
	sourceBranch := ProjectBranchRecord{
		BranchID:       sourceBranchID,
		RepoID:         "repo-main",
		AgentID:        "beta",
		BranchName:     "agent-beta-source",
		HeadSHA:        strings.Repeat("b", 40),
		ReviewDocKey:   "project.project-alpha.branch.branch-source.review",
		WriteScopeJSON: `{"paths":["src/**","package.json"]}`,
		Status:         "READY_FOR_REVIEW",
	}
	targetBranch := ProjectBranchRecord{
		BranchID:       targetBranchID,
		RepoID:         "repo-main",
		AgentID:        "beta",
		BranchName:     "agent-beta-target",
		WriteScopeJSON: `{"paths":["src/**","package.json"]}`,
		Status:         "ACTIVE",
	}
	coordination := ProjectCoordinationRecord{
		Branches: []ProjectBranchRecord{targetBranch, sourceBranch},
		PatchQueueItems: []ProjectPatchQueueItemRecord{
			{
				QueueID:   "queue-main",
				ItemID:    "item-source",
				RepoID:    "repo-main",
				BranchID:  sourceBranchID,
				State:     "BLOCKED",
				HeadSHA:   strings.Repeat("b", 40),
				DecidedAt: "2026-05-17T00:01:00Z",
				UpdatedAt: "2026-05-17T00:01:00Z",
			},
		},
	}
	existing, ok := selectProjectClaimExistingBranch(coordination.Branches, task, repo.RepoID, "beta")
	if !ok || strings.TrimSpace(existing.BranchID) != targetBranchID {
		t.Fatalf("expected retry target branch hint, got %#v ok=%v", existing, ok)
	}
	source, ok := selectProjectClaimRevisionSourceBranchExcluding(coordination.PatchQueueItems, coordination.Branches, task, repo.RepoID, existing.BranchID)
	if !ok || strings.TrimSpace(source.BranchID) != sourceBranchID {
		t.Fatalf("expected durable patch queue source branch, got %#v ok=%v", source, ok)
	}
	err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["src/**","package.json"]}`, existing, source, ProjectBranchRecord{})
	if err != nil {
		t.Fatalf("source branch should remain allowable when retry uses target branch hint: %v", err)
	}
}

func TestPreflightProjectClaimDetectsWildcardOverlap(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID: "task-config",
	}
	repo := ProjectRepositoryRecord{RepoID: "repo-main"}
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:              "task-scaffold",
				ClaimStatus:         stringPtr("CLAIMED"),
				ClaimAgentID:        stringPtr("beta"),
				ClaimRepoID:         stringPtr("repo-main"),
				ClaimWriteScopeJSON: stringPtr(`{"paths":["package*.json","vite.config.*"]}`),
			},
		},
	}
	err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, `{"paths":["package.json","vite.config.ts"]}`, ProjectBranchRecord{}, ProjectBranchRecord{}, ProjectBranchRecord{})
	if err == nil || !strings.Contains(err.Error(), "overlaps active claim") {
		t.Fatalf("expected wildcard scope overlap to fail preflight, got %v", err)
	}
}

func TestProjectClaimAdmissionTrustFirstRepairRoleRequiresScopeAnchor(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:          "task-ui-shell",
		ProjectLane:     "implementation",
		WriteScopeHints: []string{"src/**", "tests/**"},
	}
	role := ProjectRoleRecord{
		RoleID:         "role-beta-scope-repair",
		RoleType:       "IMPLEMENTER",
		Status:         "ACTIVE",
		WriteScopeJSON: `{"paths":["package.json","vite.config.*","index.html"]}`,
		Summary:        "Repair stale broad claim scope by narrowing beta scope ownership to scaffold/config files.",
	}
	if projectClaimAdmissionShouldPreferRoleScopeForTrustFirstTask(task, `{"paths":["src/**","tests/**"]}`, role) {
		t.Fatal("repair-role text must not override a task scope without coverage or overlap")
	}
}

func TestProjectClaimAdmissionLuaScopeAnchorRejectsStaleArticleRoleScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-1781635821912583300-cf33d54d",
		Title:       "CLI conformance publication repair follow-up",
		Description: "Repair Lua CLI conformance publication evidence.",
		ProjectID:   "project-signal01-lua-capability",
		ProjectLane: "implementation",
		WriteScopeHints: []string{
			"README.md",
			"internal/runner/**",
			"scripts/**",
			"testdata/smoke/**",
		},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","write_scope_hints":["README.md","internal/runner/**","scripts/**","testdata/smoke/**"]}`,
	}
	taskScope := (&Runtime{}).inferProjectTaskWriteScopeJSON(context.Background(), task, nil)
	role := ProjectRoleRecord{
		RoleID:         "projrole-1781635837098879571-16787",
		RoleType:       "IMPLEMENTER",
		Status:         "ACTIVE",
		WriteScopeJSON: `{"paths":["src/articles/**","tests/articles/**"]}`,
		Summary:        "Auto-provisioned by project task claim task-1781635821912583300-cf33d54d scope repair",
	}
	if taskScope != `{"paths":["README.md","internal/runner/**","scripts/**","testdata/smoke/**"]}` {
		t.Fatalf("Lua task scope = %s", taskScope)
	}
	if projectClaimAdmissionScopeOverrideAnchored(taskScope, role.WriteScopeJSON) {
		t.Fatalf("stale article role scope must not be anchored to Lua CLI task scope")
	}
	if projectClaimAdmissionShouldPreferRoleScopeForTrustFirstTask(task, taskScope, role) {
		t.Fatalf("Lua task scope must reject stale article role override")
	}
}

func TestProjectClaimAdmissionTrustFirstBoundaryExpansionRoleSticksOverTaskHints(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-clearpress-app-shell-auth-inventory",
		ProjectLane: "implementation",
	}
	taskScope := `{"paths":["package.json","package-lock.json","tsconfig*.json","vite.config.*","index.html","public/**","src/app/**","src/routes/**","src/auth/**","src/features/profile/**","src/features/articles/**"]}`
	role := ProjectRoleRecord{
		RoleID:         "role-beta-expanded",
		RoleType:       "IMPLEMENTER",
		Status:         "ACTIVE",
		WriteScopeJSON: `{"paths":[".gitignore","README.md","eslint.config.js","index.html","package-lock.json","package.json","public/**","src/**","tsconfig*.json","vite.config.*"]}`,
		Summary:        "ABPC side-effect resolution expand_boundary: lead-approved boundary expansion for scaffold side effect.",
	}
	if !projectClaimAdmissionShouldPreferRoleScopeForTrustFirstTask(task, taskScope, role) {
		t.Fatal("expected boundary expansion role to remain authoritative over original task write_scope_hints")
	}
}

func TestProjectClaimAdmissionTrustFirstBoundaryExpansionRoleDoesNotNeedCandidateWideTaskHints(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-clearpress-article-list",
		ProjectLane: "implementation",
	}
	taskScope := `{"paths":["src/features/articles/**"]}`
	role := ProjectRoleRecord{
		RoleID:         "role-beta-expanded",
		RoleType:       "IMPLEMENTER",
		Status:         "ACTIVE",
		WriteScopeJSON: `{"paths":["package.json","src/**"]}`,
		Summary:        "ABPC side-effect resolution expand_boundary: lead-approved boundary expansion for scaffold side effect.",
	}
	if !projectClaimAdmissionShouldPreferRoleScopeForTrustFirstTask(task, taskScope, role) {
		t.Fatal("expected explicit boundary expansion role to override non-candidate-wide task hints when scopes overlap")
	}
}

func TestProjectClaimAdmissionProductFirstRootRejectsStaleFrontendRoleScope(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-signal01-rqs1-root-extend-baseline",
		Title:       "S1 product-first root: extend the green rq baseline with real product increments",
		Description: "Start from the canonical green baseline repo. Make one concrete rq product extension, add/adjust tests, keep go build ./... and go test ./... green, then submit runnable patch-queue evidence.",
		ProjectLane: "implementation",
		WriteScopeHints: []string{
			"**",
		},
		TaskRequirementsJSON: `{"schema":"product_first_task_requirements.v1","product_first_root":true,"repo_id":"repo-signal01-rq-core"}`,
	}
	staleRole := ProjectRoleRecord{
		RoleID:         "projrole-r20-stale-frontend",
		RoleType:       "IMPLEMENTER",
		Status:         "ACTIVE",
		WriteScopeJSON: `{"paths":[".gitignore","package*.json","tsconfig*.json","vite.config.*","index.html","public/**","src/main.*","src/App.*","src/styles.*","src/styles/**","src/ui/**"]}`,
		Summary:        "Auto-provisioned by project task claim task-signal01-rqs1-root-extend-baseline",
		UpdatedBy:      "zeta",
	}

	taskScope := runtime.inferProjectTaskWriteScopeJSON(context.Background(), task, nil)
	if taskScope != `{"paths":["**"]}` {
		t.Fatalf("product-first root task scope = %s, want broad authoritative scope", taskScope)
	}
	if projectClaimAdmissionShouldPreferRoleScopeForTrustFirstTask(task, taskScope, staleRole) {
		t.Fatalf("product-first root must reject stale frontend role scope %s", staleRole.WriteScopeJSON)
	}
}

func TestRuntimeProjectClaimSemanticScopeNarrowsBroadClearpressTaskHints(t *testing.T) {
	runtime := &Runtime{}
	for _, tc := range []struct {
		name        string
		task        WorkspaceTaskRecord
		wantPaths   []string
		rejectPaths []string
	}{
		{
			name: "editor-settings",
			task: WorkspaceTaskRecord{
				TaskID:          "task-clearpress-editor-core",
				Title:           "Build editor core with shortcuts, settings, and autosave",
				Description:     "Implement rich-text editor markdown-like shortcuts, blockquote/divider transforms, quote style settings, auto dash replacement, autosave, and focused editor tests.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"src/**", "tests/**", "package.json"},
			},
			wantPaths:   []string{"src/editor", "src/lib/editor", "tests/editor", "src/settings", "tests/settings"},
			rejectPaths: []string{"src", "tests", "package.json"},
		},
		{
			name: "auth-profile-articles",
			task: WorkspaceTaskRecord{
				TaskID:          "task-clearpress-auth-profile-articles",
				Title:           "Implement mock auth, profile, and article management lanes",
				Description:     "Mock Google sign-in, profile avatar editing, my articles list, drafts, archive, delete, and article search.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"src/**", "tests/**", "package.json"},
			},
			wantPaths:   []string{"src/auth", "tests/auth", "src/profile", "tests/profile", "src/articles", "tests/articles"},
			rejectPaths: []string{"src", "tests", "package.json"},
		},
		{
			name: "public-import-export",
			task: WorkspaceTaskRecord{
				TaskID:          "task-clearpress-public-import-export",
				Title:           "Implement public article route and import/export",
				Description:     "Build read-only public article viewer route with slug/share URL plus import article JSON and export article JSON.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"src/**", "tests/**", "package.json"},
			},
			wantPaths:   []string{"src/public", "src/routes", "tests/public", "src/import-export", "src/lib/import-export", "tests/import-export"},
			rejectPaths: []string{"src", "tests", "package.json"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runtime.inferProjectTaskWriteScopeJSON(context.Background(), tc.task, nil)
			paths := projectRoleAssignWriteScopePaths(got)
			for _, want := range tc.wantPaths {
				if !projectTestPathSetContains(paths, want) {
					t.Fatalf("scope %s missing %s; got json=%s paths=%v", tc.name, want, got, paths)
				}
			}
			for _, reject := range tc.rejectPaths {
				if projectTestPathSetContains(paths, reject) {
					t.Fatalf("scope %s should not keep broad path %s; got json=%s paths=%v", tc.name, reject, got, paths)
				}
			}
		})
	}
}

func TestRuntimeProjectClaimScopePreservesAuthoritativeAcceptanceHints(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-signal01-rqs1-acceptance-tests",
		Title:       "Add full acceptance test matrix",
		Description: "Add or extend Go tests so the product contract is executable: happy paths, edge cases, CLI file mode, and REPL non-crash behavior. Do not weaken existing keystone tests.",
		ProjectLane: "implementation",
		WriteScopeHints: []string{
			"internal/**",
			"cmd/**",
			"README.md",
		},
		TaskRequirementsJSON: `{"schema":"product_first_task_requirements.v1","preserve_write_scope_hints":true,"product_slice":"acceptance_tests","must_add_tests":true}`,
	}

	got := runtime.inferProjectTaskWriteScopeJSON(context.Background(), task, nil)
	want := `{"paths":["internal/**","cmd/**","README.md"]}`
	if got != want {
		t.Fatalf("authoritative acceptance scope = %s, want %s", got, want)
	}
}

func TestRuntimeProjectClaimScopePreservesABPCRecoveryDirtyPaths(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-side-effect-lua-foundation",
		Title:       "Create foundation lane for CLI conformance publication",
		Description: "The owned CLI conformance branch needs publication repair for README.md, internal/runner/**, scripts/**, and testdata/smoke/**.",
		ProjectLane: "implementation",
		WriteScopeHints: []string{
			"README.md",
			"cmd/glua/**",
			"internal/runner/**",
			"scripts/run_conformance.ps1",
			"testdata/smoke/**",
		},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","abpc_task_class":"side_effect_foundation","admission_kind":"abpc_recovery_action","action_kind":"split_foundation_bucket","write_scope_hints":["README.md","cmd/glua/**","internal/runner/**","scripts/run_conformance.ps1","testdata/smoke/**"]}`,
	}

	got := runtime.inferProjectTaskWriteScopeJSON(context.Background(), task, nil)
	paths := projectRoleAssignWriteScopePaths(got)
	for _, want := range []string{"README.md", "cmd/glua/**", "internal/runner/**", "scripts/run_conformance.ps1", "testdata/smoke/**"} {
		if !projectTestPathSetContains(paths, want) {
			t.Fatalf("ABPC recovery scope missing %s; got json=%s paths=%v", want, got, paths)
		}
	}
	for _, stale := range []string{"package*.json", "vite.config.*", "src/main.*", "src/App.*", "public/**"} {
		if projectTestPathSetContains(paths, stale) {
			t.Fatalf("ABPC recovery scope must not infer frontend path %s; got json=%s paths=%v", stale, got, paths)
		}
	}
}

func TestRuntimeProjectClaimScopePreservesProductFirstRootBroadScope(t *testing.T) {
	runtime := &Runtime{}
	task := WorkspaceTaskRecord{
		TaskID:      "task-signal01-rqs1-root-extend-baseline",
		Title:       "S1 product-first root: extend the green rq baseline with real product increments",
		Description: "Start from the canonical green baseline repo. Make one concrete rq product extension, add/adjust tests, keep go build ./... and go test ./... green, then submit runnable patch-queue evidence. Coordination receipts, role receipts, or bootstrap artifacts are not acceptance.",
		ProjectLane: "implementation",
		WriteScopeHints: []string{
			"**",
		},
		TaskRequirementsJSON: `{"schema":"product_first_task_requirements.v1","product_first_root":true,"repo_id":"repo-signal01-rq-core","baseline_source":"seeds/rq-core","baseline_target":"runs/signal01-rq-s1/state/rq-core-baseline"}`,
	}

	got := runtime.inferProjectTaskWriteScopeJSON(context.Background(), task, nil)
	want := `{"paths":["**"]}`
	if got != want {
		t.Fatalf("product-first root scope = %s, want %s", got, want)
	}
	for _, stale := range []string{"vite.config", "src/main", "src/App"} {
		if strings.Contains(got, stale) {
			t.Fatalf("product-first root scope kept stale frontend path %q in %s", stale, got)
		}
	}
}

func TestRuntimeProjectClaimMissingRoleRequestUsesTaskDocScopeHints(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"current_phase": "IMPLEMENTATION",
						"repo_required": true,
						"repo_status":   "READY",
					},
					"strategic_lead": map[string]any{
						"role_id":          "role-lead",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "alpha",
						"role_type":        "STRATEGIC_LEAD",
						"status":           "ACTIVE",
						"write_scope_json": `{}`,
					},
					"repositories": []any{
						map[string]any{
							"repo_id":      "repo-main",
							"workspace_id": "ws",
							"project_id":   "project-alpha",
							"remote_url":   "file:///tmp/project-alpha.git",
							"repo_status":  "READY",
							"is_canonical": true,
						},
					},
				},
			})
		case "workspace.doc.get":
			t.Fatalf("hydrated task doc should avoid extra task doc read")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	work := &AgentWorkNextResult{
		Hydration: &TaskHydrationBundle{
			Docs: []WorkspaceDocRecord{
				{
					DocKey:  "task.task-pipeline",
					Content: "# Task Brief\n\n- task_id: task-pipeline\n- write_scope_hints: src/lib/**, tests/**\n",
				},
			},
		},
	}
	_, err := runtime.ensureProjectClaimAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-pipeline",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, work)
	var missing *projectClaimRoleMissingError
	if !errors.As(err, &missing) {
		t.Fatalf("expected missing role error, got %v", err)
	}
	if missing.WriteScopeJSON != `{"paths":["src/lib/**","tests/**"]}` {
		t.Fatalf("missing role write scope = %q", missing.WriteScopeJSON)
	}
	if missing.LeadAgentID != "alpha" {
		t.Fatalf("lead agent = %q", missing.LeadAgentID)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get" {
		t.Fatalf("unexpected method order: %s", got)
	}
}

func projectTestPathSetContains(paths []string, want string) bool {
	want = runtimeProjectNormalizeWriteScopePath(want)
	for _, path := range paths {
		if runtimeProjectNormalizeWriteScopePath(path) == want {
			return true
		}
	}
	return false
}

func TestRuntimeClaimTaskBypassesGateForStructuredPlanningEvidenceTask(t *testing.T) {
	var methods []string
	var claimParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("structured planning evidence task should not enter project admission, got method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "epsilon",
			OwnerUserID: "owner-1",
			Workdir:     t.TempDir(),
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	work := &AgentWorkNextResult{Packet: &AgentWorkPacket{}}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-ambient-project-signal01-rq-s1-84dc75ae6d755732",
		ProjectID:           "project-signal01-rq-s1",
		ProjectLane:         "implementation",
		RequiresProjectGate: boolPtr(true),
		Tags:                []string{"docs", "review", "spec-fidelity"},
		TaskRequirementsJSON: `{
			"schema": "task_requirements.v1",
			"preferred_tools": ["workspace_doc_get", "project_patch_queue_list"],
			"required_work_modes": ["implementation", "review", "synthesis"]
		}`,
		WriteScopeHints: []string{},
	}, "claim structured planning evidence task", work)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "agent.task.claim" {
		t.Fatalf("unexpected method order: %s", got)
	}
	for _, key := range []string{"project_role_id", "repo_id", "checkout_id", "branch_id", "write_scope_json"} {
		if got := rpcString(claimParams, key); got != "" {
			t.Fatalf("structured planning evidence claim should not include %s=%q; params=%+v", key, got, claimParams)
		}
	}
	if got := rpcString(claimParams, "task_id"); got != "task-ambient-project-signal01-rq-s1-84dc75ae6d755732" {
		t.Fatalf("task_id = %q; params=%+v", got, claimParams)
	}
}

func TestRuntimeStructuredPlanningEvidenceBypassRequiresEmptyWriteScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:              "task-real-implementation-docs-review-flavored",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: boolPtr(true),
		Tags:                []string{"docs", "review", "spec-fidelity"},
		TaskRequirementsJSON: `{
			"schema": "task_requirements.v1",
			"required_work_modes": ["implementation", "review", "synthesis"]
		}`,
		WriteScopeHints: []string{"internal/eval/**"},
	}
	if !runtimeProjectTaskRequiresImplementationGate(task) {
		t.Fatalf("docs/review-flavored implementation task with write_scope_hints must still require implementation admission")
	}
}

func TestRuntimeClaimTaskTreatsReviewProjectGateAsContextOnly(t *testing.T) {
	workdir := t.TempDir()
	var methods []string
	var claimParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-reviewer",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "agent-1",
							"role_type":        "reviewer",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["tests/**"]}`,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     "https://github.com/ExampleOrg/project-alpha.git",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("review project gate should not require implementation branch admission, got method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	work := &AgentWorkNextResult{Packet: &AgentWorkPacket{}}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-review",
		ProjectID:           "project-alpha",
		ProjectLane:         "review",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim review task", work)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,agent.task.claim" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if got := rpcString(claimParams, "project_role_id"); got != "role-reviewer" {
		t.Fatalf("review claim should include reviewer project_role_id, got %q; params=%+v", got, claimParams)
	}
	for _, key := range []string{"repo_id", "checkout_id", "branch_id", "write_scope_json"} {
		if got := rpcString(claimParams, key); got != "" {
			t.Fatalf("review claim should not include %s=%q; params=%+v", key, got, claimParams)
		}
	}
	if len(work.ProjectCoordination) == 0 || len(work.Packet.ProjectCoordination) == 0 {
		t.Fatalf("review lane role admission should attach project coordination context")
	}
}

func TestRuntimeClaimTaskTreatsValidationGateStatusAsContextOnly(t *testing.T) {
	workdir := t.TempDir()
	var methods []string
	var claimParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"gate_status": map[string]any{
						"workspace_id":           "ws",
						"project_id":             "project-alpha",
						"current_phase":          "IMPLEMENTATION",
						"overall_state":          "BLOCKED",
						"implementation_ready":   false,
						"gate_count":             1,
						"blocked_required_count": 1,
						"gates": []any{
							map[string]any{
								"gate_key": "strategic_lead_active",
								"state":    "BLOCKED",
								"required": true,
								"summary":  "Active strategic lead lease is required before implementation work",
							},
						},
					},
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("validation project gate should not require implementation branch admission, got method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	work := &AgentWorkNextResult{Packet: &AgentWorkPacket{}}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-visual-validation",
		ProjectID:           "project-alpha",
		ProjectLane:         "validation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim validation task", work)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "agent.task.claim" {
		t.Fatalf("unexpected method order: %s", got)
	}
	for _, key := range []string{"project_role_id", "repo_id", "checkout_id", "branch_id", "write_scope_json"} {
		if got := rpcString(claimParams, key); got != "" {
			t.Fatalf("validation claim should not include %s=%q; params=%+v", key, got, claimParams)
		}
	}
	if len(work.ProjectCoordination) != 0 || len(work.Packet.ProjectCoordination) != 0 {
		t.Fatalf("validation lane should bypass implementation admission context, top=%s packet=%s", work.ProjectCoordination, work.Packet.ProjectCoordination)
	}
}

func TestRuntimeClaimTaskTreatsStrategyProjectGateAsContextOnly(t *testing.T) {
	workdir := t.TempDir()
	var methods []string
	var claimParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     "https://github.com/ExampleOrg/project-alpha.git",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("strategy project gate should not require branch admission, got method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	work := &AgentWorkNextResult{Packet: &AgentWorkPacket{}}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-strategy",
		ProjectID:           "project-alpha",
		ProjectLane:         "strategy",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim strategy task", work)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "agent.task.claim" {
		t.Fatalf("unexpected method order: %s", got)
	}
	for _, key := range []string{"project_role_id", "repo_id", "checkout_id", "branch_id", "write_scope_json"} {
		if got := rpcString(claimParams, key); got != "" {
			t.Fatalf("strategy claim should not include %s=%q; params=%+v", key, got, claimParams)
		}
	}
	if len(work.ProjectCoordination) != 0 || len(work.Packet.ProjectCoordination) != 0 {
		t.Fatalf("strategy lane should bypass implementation admission context, top=%s packet=%s", work.ProjectCoordination, work.Packet.ProjectCoordination)
	}
}

func TestRuntimeClaimTaskTreatsIntegrationProjectGateAsContextOnly(t *testing.T) {
	workdir := t.TempDir()
	var methods []string
	var claimParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "INTEGRATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-integrator",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "agent-1",
							"role_type":        "integrator",
							"status":           "ACTIVE",
							"write_scope_json": `{}`,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     "https://github.com/ExampleOrg/project-alpha.git",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("integration project gate should not require implementation branch admission, got method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	work := &AgentWorkNextResult{Packet: &AgentWorkPacket{}}
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-integration",
		ProjectID:           "project-alpha",
		ProjectLane:         "integration",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim integration task", work)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,agent.task.claim" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if got := rpcString(claimParams, "project_role_id"); got != "role-integrator" {
		t.Fatalf("integration claim should include integrator project_role_id, got %q; params=%+v", got, claimParams)
	}
	for _, key := range []string{"repo_id", "checkout_id", "branch_id", "write_scope_json"} {
		if got := rpcString(claimParams, key); got != "" {
			t.Fatalf("integration claim should not include %s=%q; params=%+v", key, got, claimParams)
		}
	}
	if len(work.ProjectCoordination) == 0 || len(work.Packet.ProjectCoordination) == 0 {
		t.Fatalf("integration lane role admission should attach project coordination context")
	}
}

func TestRuntimeMissingProjectRoleAdmissionRequestsStrategicLead(t *testing.T) {
	var methods []string
	var requestParams map[string]any
	var savedScratch string
	var updateParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.request":
			requestParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-role-1",
				"workspace_id": "ws",
				"to_agent_id":  "gamma",
				"status":       "PENDING",
			})
		case "agent.state.set":
			savedScratch = rpcString(req.Params, "value")
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	handled, err := runtime.handleProjectClaimAdmissionError(context.Background(), WorkspaceTaskRecord{
		TaskID:      "task-build",
		ProjectID:   "project-alpha",
		ProjectLane: "implementation",
	}, &projectClaimRoleMissingError{
		ProjectID:      "project-alpha",
		TaskID:         "task-build",
		AgentID:        "beta",
		LeadAgentID:    "gamma",
		RoleType:       "IMPLEMENTER",
		WriteScopeJSON: `{"paths":["app/**"]}`,
	})
	if err != nil {
		t.Fatalf("handleProjectClaimAdmissionError() error = %v", err)
	}
	if !handled {
		t.Fatal("expected missing project role admission to be handled")
	}
	if got := strings.Join(methods, ","); got != "agent.request,agent.state.set,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(requestParams, "from_agent_id") != "beta" || rpcString(requestParams, "to_agent_id") != "gamma" || rpcString(requestParams, "method") != projectRoleRequestMethod {
		t.Fatalf("unexpected role request params: %+v", requestParams)
	}
	var payload projectRoleRequestPayload
	if err := json.Unmarshal([]byte(rpcString(requestParams, "payload_json")), &payload); err != nil {
		t.Fatalf("decode role request payload: %v", err)
	}
	if payload.ProjectID != "project-alpha" || payload.TaskID != "task-build" || payload.RoleType != "IMPLEMENTER" || payload.WriteScopeJSON != `{"paths":["app/**"]}` {
		t.Fatalf("unexpected role request payload: %+v", payload)
	}
	if !strings.Contains(savedScratch, "areq-role-1") || !strings.Contains(savedScratch, "project_role_request") {
		t.Fatalf("scratch did not record role request: %s", savedScratch)
	}
	if rpcString(updateParams, "update_type") != "coordination" || !strings.Contains(rpcString(updateParams, "summary"), "Requested IMPLEMENTER role") {
		t.Fatalf("unexpected update params: %+v", updateParams)
	}
}

func TestRuntimeProjectRoleLaneRequiredWorkRequestsStrategicLead(t *testing.T) {
	var methods []string
	var requestParams map[string]any
	var updateParams map[string]any
	var savedScratch string

	coordinationRaw, _ := json.Marshal(ProjectCoordinationRecord{
		Project: ProjectRecord{
			WorkspaceID: "ws",
			ProjectID:   "project-alpha",
			Title:       "Project Alpha",
			Status:      "ACTIVE",
		},
		Profile: ProjectProfileRecord{
			WorkspaceID:  "ws",
			ProjectID:    "project-alpha",
			CurrentPhase: "IMPLEMENTATION",
			RepoRequired: true,
			RepoStatus:   "READY",
		},
		StrategicLead: &ProjectRoleRecord{
			RoleID:    "role-gamma-lead",
			ProjectID: "project-alpha",
			AgentID:   "gamma",
			RoleType:  "STRATEGIC_LEAD",
			Status:    "ACTIVE",
		},
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:          "task-build",
				ProjectID:       "project-alpha",
				ProjectLane:     "implementation",
				TaskKind:        "EXECUTION",
				WriteScopeHints: []string{"src/editor/**"},
			},
		},
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.request":
			requestParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-role-worknext",
				"workspace_id": "ws",
				"to_agent_id":  "gamma",
				"status":       "PENDING",
			})
		case "agent.state.set":
			savedScratch = rpcString(req.Params, "value")
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, nil)
		case "project.coordination.get":
			t.Fatalf("work packet already carries project coordination; unexpected project.coordination.get")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	handled, err := runtime.maybeRequestProjectRoleScopeFromWork(context.Background(), AgentWorkNextResult{
		WorkspaceID: "ws",
		AgentID:     "beta",
		HasWork:     false,
		Reason:      "project_role_lane_required",
		ProjectID:   "project-alpha",
		TaskKind:    "EXECUTION",
		ProjectLane: "implementation",
		Packet: &AgentWorkPacket{
			WorkType:            "project_role_lane_required",
			ProjectID:           "project-alpha",
			TaskKind:            "EXECUTION",
			ProjectLane:         "implementation",
			ProjectCoordination: coordinationRaw,
			ContextHints: AgentWorkContextHints{
				AnchorTaskIDs: []string{"task-build"},
			},
		},
	})
	if err != nil {
		t.Fatalf("maybeRequestProjectRoleScopeFromWork() error = %v", err)
	}
	if !handled {
		t.Fatal("expected role-lane no-work packet to materialize role request")
	}
	if got := strings.Join(methods, ","); got != "agent.request,agent.state.set,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(requestParams, "from_agent_id") != "beta" || rpcString(requestParams, "to_agent_id") != "gamma" || rpcString(requestParams, "method") != projectRoleRequestMethod {
		t.Fatalf("unexpected role request params: %+v", requestParams)
	}
	var payload projectRoleRequestPayload
	if err := json.Unmarshal([]byte(rpcString(requestParams, "payload_json")), &payload); err != nil {
		t.Fatalf("decode role request payload: %v", err)
	}
	if payload.ProjectID != "project-alpha" || payload.TaskID != "task-build" || payload.RoleType != "IMPLEMENTER" || payload.WriteScopeJSON != `{"paths":["src/editor/**"]}` {
		t.Fatalf("unexpected role request payload: %+v", payload)
	}
	if !strings.Contains(savedScratch, "areq-role-worknext") || !strings.Contains(savedScratch, "project_role_request") {
		t.Fatalf("scratch did not record role request: %s", savedScratch)
	}
	if rpcString(updateParams, "update_type") != "coordination" || !strings.Contains(rpcString(updateParams, "summary"), "Requested IMPLEMENTER role") {
		t.Fatalf("unexpected update params: %+v", updateParams)
	}
}

func TestRuntimeProjectClaimAdmissionUsesIntegratorRoleForGatedIntegrationTask(t *testing.T) {
	coordinationRaw, _ := json.Marshal(ProjectCoordinationRecord{
		Profile: ProjectProfileRecord{
			WorkspaceID:  "ws",
			ProjectID:    "project-alpha",
			CurrentPhase: "INTEGRATION",
			RepoRequired: true,
			RepoStatus:   "READY",
		},
		Roles: []ProjectRoleRecord{
			{
				RoleID:    "role-zeta-integrator",
				ProjectID: "project-alpha",
				AgentID:   "zeta",
				RoleType:  "INTEGRATOR",
				Status:    "ACTIVE",
			},
		},
	})
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "zeta",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient("http://127.0.0.1:1", "token"),
	}
	admission, err := runtime.ensureProjectClaimAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-integrate-accepted",
		ProjectID:           "project-alpha",
		ProjectLane:         "integration",
		RequiresProjectGate: boolPtr(true),
	}, &AgentWorkNextResult{ProjectCoordination: coordinationRaw})
	if err != nil {
		t.Fatalf("ensureProjectClaimAdmission() error = %v", err)
	}
	if admission.ProjectRoleID != "role-zeta-integrator" || admission.ProjectID != "project-alpha" {
		t.Fatalf("expected role-only INTEGRATOR admission, got %+v", admission)
	}
	if admission.RepoID != "" || admission.CheckoutID != "" || admission.BranchID != "" || admission.WriteScopeJSON != "" {
		t.Fatalf("integration admission should not prepare implementation branch bindings, got %+v", admission)
	}
}

func TestRuntimeTrustFirstIntegrationAdmissionLetsServerAutoProvisionMissingIntegratorRole(t *testing.T) {
	coordinationRaw, _ := json.Marshal(ProjectCoordinationRecord{
		Profile: ProjectProfileRecord{
			WorkspaceID:  "ws",
			ProjectID:    "project-alpha",
			CurrentPhase: "INTEGRATION",
			RepoRequired: true,
			RepoStatus:   "READY",
		},
		StrategicLead: &ProjectRoleRecord{
			RoleID:    "role-alpha-lead",
			ProjectID: "project-alpha",
			AgentID:   "alpha",
			RoleType:  "STRATEGIC_LEAD",
			Status:    "ACTIVE",
		},
	})
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "zeta",
			OwnerUserID:      "owner-1",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient("http://127.0.0.1:1", "token"),
	}
	admission, err := runtime.ensureProjectClaimAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-integrate-accepted",
		ProjectID:           "project-alpha",
		ProjectLane:         "integration",
		RequiresProjectGate: boolPtr(true),
	}, &AgentWorkNextResult{ProjectCoordination: coordinationRaw})
	if err != nil {
		t.Fatalf("ensureProjectClaimAdmission() error = %v", err)
	}
	if admission.ProjectID != "project-alpha" || admission.ProjectRoleID != "" {
		t.Fatalf("trust-first integration admission should leave role auto-provision to server claim, got %+v", admission)
	}
}

func TestRuntimeStrictIntegrationAdmissionRequestsIntegratorRole(t *testing.T) {
	coordinationRaw, _ := json.Marshal(ProjectCoordinationRecord{
		Profile: ProjectProfileRecord{
			WorkspaceID:  "ws",
			ProjectID:    "project-alpha",
			CurrentPhase: "INTEGRATION",
			RepoRequired: true,
			RepoStatus:   "READY",
		},
		StrategicLead: &ProjectRoleRecord{
			RoleID:    "role-alpha-lead",
			ProjectID: "project-alpha",
			AgentID:   "alpha",
			RoleType:  "STRATEGIC_LEAD",
			Status:    "ACTIVE",
		},
	})
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "zeta",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient("http://127.0.0.1:1", "token"),
	}
	_, err := runtime.ensureProjectClaimAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-integrate-accepted",
		ProjectID:           "project-alpha",
		ProjectLane:         "integration",
		RequiresProjectGate: boolPtr(true),
	}, &AgentWorkNextResult{ProjectCoordination: coordinationRaw})
	var missing *projectClaimRoleMissingError
	if !errors.As(err, &missing) {
		t.Fatalf("expected projectClaimRoleMissingError, got %v", err)
	}
	if missing.RoleType != "INTEGRATOR" || missing.WriteScopeJSON != "{}" || missing.LeadAgentID != "alpha" {
		t.Fatalf("unexpected missing role payload: %+v", missing)
	}
}

func TestRuntimeTrustFirstLeadMissingRoleAdmissionAutoAssignsBoundedTaskScope(t *testing.T) {
	var methods []string
	var assignParams map[string]any
	var updateParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.role.assign":
			assignParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"role": map[string]any{
					"role_id":          "role-beta",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"agent_id":         "beta",
					"role_type":        "IMPLEMENTER",
					"status":           "ACTIVE",
					"write_scope_json": `{"paths":["src/editor/**"]}`,
				},
			})
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			OwnerUserID:      "owner-1",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	handled, err := runtime.handleProjectClaimAdmissionError(context.Background(), WorkspaceTaskRecord{
		TaskID:          "task-build",
		ProjectID:       "project-alpha",
		ProjectLane:     "implementation",
		WriteScopeHints: []string{"src/editor/**"},
	}, &projectClaimRoleMissingError{
		ProjectID:      "project-alpha",
		TaskID:         "task-build",
		AgentID:        "beta",
		LeadAgentID:    "gamma",
		RoleType:       "IMPLEMENTER",
		WriteScopeJSON: `{"paths":["src/editor/**"]}`,
	})
	if err != nil {
		t.Fatalf("handleProjectClaimAdmissionError() error = %v", err)
	}
	if !handled {
		t.Fatal("expected missing role admission to be handled")
	}
	if got := strings.Join(methods, ","); got != "project.role.assign,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(assignParams, "actor_id") != "gamma" || rpcString(assignParams, "agent_id") != "beta" || rpcString(assignParams, "role_type") != "IMPLEMENTER" || rpcString(assignParams, "write_scope_json") != `{"paths":["src/editor/**"]}` {
		t.Fatalf("unexpected assign params: %+v", assignParams)
	}
	if rpcString(updateParams, "update_type") != "coordination" || !strings.Contains(rpcString(updateParams, "summary"), "Trust-first assigned bounded IMPLEMENTER role") {
		t.Fatalf("unexpected update params: %+v", updateParams)
	}
}

func TestRuntimeMissingProjectRoleAdmissionDedupeNoStrategicLeadIssue(t *testing.T) {
	var methods []string
	var updateParams map[string]any
	var savedScratch string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"current_phase": "IMPLEMENTATION",
						"repo_required": true,
						"repo_status":   "READY",
					},
				},
			})
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			savedScratch = rpcString(req.Params, "value")
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	missing := &projectClaimRoleMissingError{
		ProjectID:      "project-alpha",
		TaskID:         "task-build",
		AgentID:        "beta",
		RoleType:       "IMPLEMENTER",
		WriteScopeJSON: `{"paths":["app/**"]}`,
	}
	for i := 0; i < 2; i++ {
		handled, err := runtime.handleProjectClaimAdmissionError(context.Background(), WorkspaceTaskRecord{
			TaskID:      "task-build",
			ProjectID:   "project-alpha",
			ProjectLane: "implementation",
		}, missing)
		if err != nil {
			t.Fatalf("handleProjectClaimAdmissionError(%d) error = %v", i, err)
		}
		if !handled {
			t.Fatalf("expected missing project role admission to be handled on iteration %d", i)
		}
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,agent.update.post,agent.state.set,project.coordination.get" {
		t.Fatalf("unexpected method order: %s", got)
	}
	requiresHuman, _ := updateParams["requires_human"].(bool)
	if rpcString(updateParams, "update_type") != "issue" || !requiresHuman {
		t.Fatalf("unexpected update params: %+v", updateParams)
	}
	if !strings.Contains(savedScratch, "project_role_request") || !strings.Contains(savedScratch, "task-build") {
		t.Fatalf("scratch did not record no-lead issue dedupe: %s", savedScratch)
	}
}

func TestRuntimeProjectClaimOverlapAdmissionSetsHold(t *testing.T) {
	var methods []string
	var savedScratchValues []string
	var updateParams []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.state.set":
			savedScratchValues = append(savedScratchValues, rpcString(req.Params, "value"))
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			updateParams = append(updateParams, req.Params)
			writeRPCResult(w, req, nil)
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-subpixel",
						"title":        "Subpixel",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":  "ws",
						"project_id":    "project-subpixel",
						"current_phase": "IMPLEMENTATION",
						"repo_required": true,
						"repo_status":   "READY",
					},
					"strategic_lead": map[string]any{
						"role_id":          "role-alpha",
						"workspace_id":     "ws",
						"project_id":       "project-subpixel",
						"agent_id":         "alpha",
						"role_type":        "STRATEGIC_LEAD",
						"status":           "ACTIVE",
						"write_scope_json": `{}`,
						"created_at":       "2026-04-28T00:00:00Z",
						"updated_at":       "2026-04-28T00:00:00Z",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-gamma",
							"workspace_id":     "ws",
							"project_id":       "project-subpixel",
							"agent_id":         "gamma",
							"role_type":        "IMPLEMENTER",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["src/app/**"]}`,
							"created_at":       "2026-04-28T00:00:00Z",
							"updated_at":       "2026-04-28T00:00:00Z",
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-subpixel",
							"remote_url":     "https://example.invalid/repo.git",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
					"branches": []any{
						map[string]any{
							"branch_id":        "branch-1",
							"workspace_id":     "ws",
							"project_id":       "project-subpixel",
							"repo_id":          "repo-main",
							"agent_id":         "beta",
							"active_task_id":   "task-active",
							"branch_name":      "agent/beta/scaffold",
							"branch_kind":      "feature",
							"write_scope_json": `{"paths":["src/app/bootstrap/**"]}`,
							"status":           "ACTIVE",
							"created_at":       "2026-04-28T00:00:00Z",
							"updated_at":       "2026-04-28T00:00:00Z",
						},
					},
				},
			})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case "task.submit":
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "agent.request":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-repair-1",
				"workspace_id": "ws",
				"to_agent_id":  "alpha",
				"status":       "PENDING",
			})
		case "agent.task.hydrate":
			t.Fatalf("cross-agent conflict should not hydrate local same-agent target")
		case "agent.respond":
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-build",
		ProjectID:   "project-subpixel",
		ProjectLane: "implementation",
	}
	overlapErr := errors.New("rpc agent.task.claim: task claim project admission invalid: write_scope_json overlaps active claim task_id=task-active branch_id=branch-1")

	handled, err := runtime.handleProjectClaimAdmissionError(context.Background(), task, overlapErr)
	if err != nil {
		t.Fatalf("handleProjectClaimAdmissionError() error = %v", err)
	}
	if !handled {
		t.Fatal("expected project claim overlap admission to be handled")
	}
	if !containsAll(methods, []string{"agent.state.set", "agent.update.post", "project.coordination.get", "workspace.tasks.list", "task.submit", "agent.request"}) {
		t.Fatalf("expected overlap handling to queue strategic repair, got methods %s", strings.Join(methods, ","))
	}
	if len(savedScratchValues) == 0 || !strings.Contains(strings.Join(savedScratchValues, "\n"), `"project_claim_hold_task_id":"task-build"`) || !strings.Contains(strings.Join(savedScratchValues, "\n"), `"project_claim_hold_project_id":"project-subpixel"`) {
		t.Fatalf("scratch did not record project claim hold: %s", strings.Join(savedScratchValues, "\n"))
	}
	if !projectClaimHoldActive(runtime.scratch, task, time.Now().UTC()) {
		t.Fatalf("expected in-memory project claim hold to be active: %+v", runtime.scratch)
	}
	if len(updateParams) < 2 || rpcString(updateParams[0], "update_type") != "coordination" || !strings.Contains(rpcString(updateParams[0], "summary"), "Deferring project claim") {
		t.Fatalf("unexpected update params: %+v", updateParams)
	}
	var holdPayload map[string]any
	if err := json.Unmarshal([]byte(rpcString(updateParams[0], "payload_json")), &holdPayload); err != nil {
		t.Fatalf("decode overlap hold payload: %v", err)
	}
	if holdPayload["delegation_state"] != "delegation_project_claim_overlap" ||
		holdPayload["hold_kind"] != "project_claim_overlap" ||
		holdPayload["task_id"] != "task-build" ||
		holdPayload["blocked_task_id"] != "task-build" ||
		holdPayload["to_agent_id"] != "gamma" ||
		holdPayload["coverage_state"] != "covered_by_active_overlapping_claim" ||
		holdPayload["conflict_task_id"] != "task-active" ||
		holdPayload["conflict_branch_id"] != "branch-1" {
		t.Fatalf("unexpected overlap hold payload: %+v", holdPayload)
	}
	holdUntil, _ := holdPayload["hold_until"].(string)
	expiresAt, _ := holdPayload["expires_at"].(string)
	if strings.TrimSpace(holdUntil) == "" || strings.TrimSpace(expiresAt) == "" {
		t.Fatalf("expected expiring overlap hold payload, got %+v", holdPayload)
	}
	if !strings.Contains(rpcString(updateParams[len(updateParams)-1], "summary"), "project claim repair task") {
		t.Fatalf("expected final update to report repair task, got %+v", updateParams)
	}

	handled, err = runtime.handleProjectClaimAdmissionError(context.Background(), task, overlapErr)
	if err != nil {
		t.Fatalf("second handleProjectClaimAdmissionError() error = %v", err)
	}
	if !handled {
		t.Fatal("expected second overlap admission to be handled")
	}
	if got := countString(methods, "task.submit"); got != 1 {
		t.Fatalf("expected active hold to suppress duplicate repair submit, got methods %s", strings.Join(methods, ","))
	}
}

func TestProjectClaimOverlapAdmissionErrorIsNotGenericClaimConflict(t *testing.T) {
	overlapErr := errors.New("rpc agent.task.claim: task claim project admission invalid: write_scope_json overlaps active claim task_id=task-active branch_id=branch-1")
	if !isProjectClaimOverlapAdmissionError(overlapErr) {
		t.Fatalf("expected project overlap admission error to be recognized")
	}
	if isClaimConflictError(overlapErr) {
		t.Fatalf("project overlap admission error must route to project claim repair, not generic claim-conflict retry")
	}
	if !isClaimConflictError(errors.New("rpc agent.task.claim: task claim conflict: already claimed by gamma")) {
		t.Fatalf("ordinary already-claimed conflict should still use generic claim-conflict retry")
	}
}

func TestRuntimeTrustFirstProjectClaimOverlapSetsHoldAndRepairsWithTaskScope(t *testing.T) {
	var methods []string
	var savedScratchValues []string
	var updateParams []map[string]any
	var submitParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.state.set":
			savedScratchValues = append(savedScratchValues, rpcString(req.Params, "value"))
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			updateParams = append(updateParams, req.Params)
			writeRPCResult(w, req, nil)
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"current_phase": "IMPLEMENTATION",
						"repo_required": true,
						"repo_status":   "READY",
					},
					"strategic_lead": map[string]any{
						"role_id":          "role-alpha",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "alpha",
						"role_type":        "STRATEGIC_LEAD",
						"status":           "ACTIVE",
						"write_scope_json": `{}`,
					},
					"roles": []any{},
					"repositories": []any{
						map[string]any{
							"repo_id":      "repo-main",
							"workspace_id": "ws",
							"project_id":   "project-alpha",
							"remote_url":   "https://example.invalid/repo.git",
							"repo_status":  "READY",
							"is_canonical": true,
						},
					},
					"branches": []any{
						map[string]any{
							"branch_id":        "branch-delta",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"repo_id":          "repo-main",
							"agent_id":         "delta",
							"active_task_id":   "task-risk-timeline",
							"branch_name":      "agent/delta/risk-timeline",
							"branch_kind":      "feature",
							"write_scope_json": `{"paths":["src/components/risks/**","src/components/timeline/**","src/components/shared/**","src/styles/**","src/data/**","src/types/**"]}`,
							"status":           "ACTIVE",
						},
					},
				},
			})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case "task.submit":
			submitParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "agent.request":
			writeRPCResult(w, req, map[string]any{
				"request_id":   "areq-repair-1",
				"workspace_id": "ws",
				"to_agent_id":  "alpha",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			OwnerUserID:      "owner-1",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:          "task-workboard",
		ProjectID:       "project-alpha",
		ProjectLane:     "implementation",
		WriteScopeHints: []string{"src/components/workboard/**", "src/components/filters/**", "src/hooks/**", "src/state/**", "src/data/**", "src/types/**"},
	}
	overlapErr := errors.New("rpc agent.task.claim: task claim project admission invalid: write_scope_json overlaps active claim task_id=task-risk-timeline branch_id=branch-delta")

	handled, err := runtime.handleProjectClaimAdmissionError(context.Background(), task, overlapErr)
	if err != nil {
		t.Fatalf("handleProjectClaimAdmissionError() error = %v", err)
	}
	if !handled {
		t.Fatal("expected trust-first overlap admission to be handled")
	}
	if !projectClaimHoldActive(runtime.scratch, task, time.Now().UTC()) {
		t.Fatalf("expected trust-first overlap to record active project claim hold: %+v", runtime.scratch)
	}
	if !containsAll(methods, []string{"agent.state.set", "agent.update.post", "project.coordination.get", "workspace.tasks.list", "task.submit", "agent.request"}) {
		t.Fatalf("expected trust-first overlap to queue repair, got methods %s", strings.Join(methods, ","))
	}
	if len(updateParams) == 0 || !strings.Contains(rpcString(updateParams[0], "payload_json"), `"coordination_mode":"trust_first"`) || !strings.Contains(rpcString(updateParams[0], "payload_json"), `"delegation_state":"delegation_project_claim_overlap"`) {
		t.Fatalf("expected hold update to keep trust-first mode evidence, got %+v", updateParams)
	}
	description := rpcString(submitParams, "description")
	if !strings.Contains(description, `blocked_write_scope_json: {"paths":["src/components/workboard/**","src/components/filters/**","src/hooks/**","src/state/**","src/data/**","src/types/**"]}`) {
		t.Fatalf("expected repair task to use task-local trust-first scope, got %+v", submitParams)
	}
	if len(savedScratchValues) == 0 || !strings.Contains(strings.Join(savedScratchValues, "\n"), `"project_claim_hold_task_id":"task-workboard"`) {
		t.Fatalf("scratch did not record trust-first project claim hold: %s", strings.Join(savedScratchValues, "\n"))
	}

	handled, err = runtime.handleProjectClaimAdmissionError(context.Background(), task, overlapErr)
	if err != nil {
		t.Fatalf("second handleProjectClaimAdmissionError() error = %v", err)
	}
	if !handled {
		t.Fatal("expected second trust-first overlap admission to be handled")
	}
	if got := countString(methods, "task.submit"); got != 1 {
		t.Fatalf("expected trust-first hold to suppress duplicate repair submit, got methods %s", strings.Join(methods, ","))
	}
}

func countString(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}

func TestRuntimeProjectRoleRequestAssignsRoleWhenStrategicLead(t *testing.T) {
	var methods []string
	var assignParams map[string]any
	var responseParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"current_phase": "IMPLEMENTATION",
						"repo_required": true,
						"repo_status":   "READY",
					},
					"strategic_lead": map[string]any{
						"role_id":          "role-lead",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "gamma",
						"role_type":        "STRATEGIC_LEAD",
						"status":           "ACTIVE",
						"write_scope_json": `{}`,
						"created_at":       "2026-04-28T00:00:00Z",
						"updated_at":       "2026-04-28T00:00:00Z",
					},
				},
			})
		case "project.role.assign":
			assignParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"role": map[string]any{
					"role_id":          "role-beta",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"agent_id":         "beta",
					"role_type":        "IMPLEMENTER",
					"status":           "ACTIVE",
					"write_scope_json": `{"paths":["app/**"]}`,
					"created_at":       "2026-04-28T00:00:00Z",
					"updated_at":       "2026-04-28T00:00:00Z",
				},
			})
		case "agent.update.post":
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responseParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	rawPayload, _ := json.Marshal(projectRoleRequestPayload{
		ProjectID:      "project-alpha",
		TaskID:         "task-build",
		RoleType:       "IMPLEMENTER",
		WriteScopeJSON: `{"paths":["app/**"]}`,
		Reason:         "beta cannot claim until role exists",
	})
	err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-role-1",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "gamma",
		Method:      projectRoleRequestMethod,
		Payload:     string(rawPayload),
		Status:      "PENDING",
	})
	if err != nil {
		t.Fatalf("handleRequest(project.role.request) error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.role.assign,agent.update.post,agent.respond" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(assignParams, "actor_id") != "gamma" || rpcString(assignParams, "agent_id") != "beta" || rpcString(assignParams, "role_type") != "IMPLEMENTER" || rpcString(assignParams, "write_scope_json") != `{"paths":["app/**"]}` {
		t.Fatalf("unexpected assign params: %+v", assignParams)
	}
	var response map[string]any
	if err := json.Unmarshal([]byte(rpcString(responseParams, "response")), &response); err != nil {
		t.Fatalf("decode role request response: %v", err)
	}
	if response["status"] != "assigned" || response["role_id"] != "role-beta" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestRuntimeProjectRoleRequestTrustFirstAutoAssignsBoundedTaskScope(t *testing.T) {
	var methods []string
	var assignParams map[string]any
	var responseParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"current_phase": "IMPLEMENTATION",
						"repo_required": true,
						"repo_status":   "READY",
					},
					"strategic_lead": map[string]any{
						"role_id":          "role-lead",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "gamma",
						"role_type":        "STRATEGIC_LEAD",
						"status":           "ACTIVE",
						"write_scope_json": `{}`,
					},
				},
			})
		case "agent.task.hydrate":
			if rpcString(req.Params, "task_id") != "task-build" {
				t.Fatalf("unexpected hydrate params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"workspace_task": map[string]any{
						"task_id":           "task-build",
						"title":             "Implement markdown editor shortcuts",
						"project_id":        "project-alpha",
						"project_lane":      "implementation",
						"write_scope_hints": []string{"src/editor/**", "tests/editor/**"},
					},
					"task": map[string]any{
						"task_id": "task-build",
					},
				},
			})
		case "project.role.assign":
			assignParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"role": map[string]any{
					"role_id":          "role-beta",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"agent_id":         "beta",
					"role_type":        "IMPLEMENTER",
					"status":           "ACTIVE",
					"write_scope_json": `{"paths":["src/editor/**"]}`,
				},
			})
		case "agent.update.post":
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responseParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			OwnerUserID:      "owner-1",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	rawPayload, _ := json.Marshal(projectRoleRequestPayload{
		ProjectID:      "project-alpha",
		TaskID:         "task-build",
		RoleType:       "IMPLEMENTER",
		WriteScopeJSON: `{"paths":["src/editor/**"]}`,
		Reason:         "beta cannot claim until role exists",
	})
	err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-role-1",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "gamma",
		Method:      projectRoleRequestMethod,
		Payload:     string(rawPayload),
		Status:      "PENDING",
	})
	if err != nil {
		t.Fatalf("handleRequest(project.role.request) error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,agent.task.hydrate,project.role.assign,agent.update.post,agent.respond" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(assignParams, "actor_id") != "gamma" || rpcString(assignParams, "agent_id") != "beta" || rpcString(assignParams, "role_type") != "IMPLEMENTER" || rpcString(assignParams, "write_scope_json") != `{"paths":["src/editor/**"]}` {
		t.Fatalf("unexpected assign params: %+v", assignParams)
	}
	var response map[string]any
	if err := json.Unmarshal([]byte(rpcString(responseParams, "response")), &response); err != nil {
		t.Fatalf("decode role request response: %v", err)
	}
	if response["status"] != "assigned" || response["trust_first_auto_assign"] != "bounded_task_scope" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestRuntimeProjectRoleRequestTrustFirstRecordsAdvisoryWithoutAssigning(t *testing.T) {
	var methods []string
	var responseParams map[string]any
	var updateParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"current_phase": "IMPLEMENTATION",
						"repo_required": true,
						"repo_status":   "READY",
					},
					"strategic_lead": map[string]any{
						"role_id":          "role-lead",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "gamma",
						"role_type":        "STRATEGIC_LEAD",
						"status":           "ACTIVE",
						"write_scope_json": `{}`,
					},
				},
			})
		case "project.role.assign":
			t.Fatalf("trust-first advisory role request must not assign role automatically: %+v", req.Params)
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responseParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:      "ws",
			AgentID:          "gamma",
			OwnerUserID:      "owner-1",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	rawPayload, _ := json.Marshal(projectRoleRequestPayload{
		ProjectID:      "project-alpha",
		TaskID:         "task-build",
		RoleType:       "IMPLEMENTER",
		WriteScopeJSON: `{"paths":["**"]}`,
		Reason:         "beta cannot claim until role exists",
	})
	err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-role-1",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "gamma",
		Method:      projectRoleRequestMethod,
		Payload:     string(rawPayload),
		Status:      "PENDING",
	})
	if err != nil {
		t.Fatalf("handleRequest(project.role.request) error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,agent.update.post,agent.respond" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(updateParams, "update_type") != "coordination" || !strings.Contains(rpcString(updateParams, "summary"), "Trust-first recorded advisory") {
		t.Fatalf("unexpected update params: %+v", updateParams)
	}
	var response map[string]any
	if err := json.Unmarshal([]byte(rpcString(responseParams, "response")), &response); err != nil {
		t.Fatalf("decode role request response: %v", err)
	}
	if response["status"] != "advisory_recorded" || response["role_type"] != "IMPLEMENTER" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestRuntimeProjectRoleRequestRefusesAutoPromotionFromNonImplementerRole(t *testing.T) {
	var methods []string
	var responseParams map[string]any
	var updateParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"current_phase": "IMPLEMENTATION",
						"repo_required": true,
						"repo_status":   "READY",
					},
					"strategic_lead": map[string]any{
						"role_id":          "role-lead",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "gamma",
						"role_type":        "STRATEGIC_LEAD",
						"status":           "ACTIVE",
						"write_scope_json": `{}`,
						"created_at":       "2026-04-28T00:00:00Z",
						"updated_at":       "2026-04-28T00:00:00Z",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-zeta-integrator",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "zeta",
							"role_type":        "INTEGRATOR",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["src/**","README.md"]}`,
							"created_at":       "2026-04-28T00:00:00Z",
							"updated_at":       "2026-04-28T00:00:00Z",
						},
					},
				},
			})
		case "project.role.assign":
			t.Fatalf("automatic role request must not promote existing non-IMPLEMENTER role: %+v", req.Params)
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.respond":
			responseParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	rawPayload, _ := json.Marshal(projectRoleRequestPayload{
		ProjectID:      "project-alpha",
		TaskID:         "task-core",
		RoleType:       "IMPLEMENTER",
		WriteScopeJSON: `{"paths":["**"]}`,
		Reason:         "zeta cannot claim until role exists",
	})
	err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-role-zeta",
		WorkspaceID: "ws",
		FromAgentID: "zeta",
		ToAgentID:   "gamma",
		Method:      projectRoleRequestMethod,
		Payload:     string(rawPayload),
		Status:      "PENDING",
	})
	if err != nil {
		t.Fatalf("handleRequest(project.role.request) error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,agent.update.post,agent.respond" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(updateParams, "update_type") != "issue" || !strings.Contains(rpcString(updateParams, "summary"), "Refused automatic IMPLEMENTER role request") {
		t.Fatalf("unexpected update params: %+v", updateParams)
	}
	var response map[string]any
	if err := json.Unmarshal([]byte(rpcString(responseParams, "response")), &response); err != nil {
		t.Fatalf("decode role request response: %v", err)
	}
	if response["status"] != "role_mismatch" || response["existing_role_type"] != "INTEGRATOR" || response["blocked_role_type"] != "IMPLEMENTER" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestRuntimeClaimTaskRepairsStaleProjectBranchCheckout(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	initProjectAdmissionGitCheckout(t, workdir, remoteURL)
	var methods []string
	var branchRegisterParams map[string]any
	var claimParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"description":  "Build the thing",
						"status":       "ACTIVE",
						"created_by":   "owner",
						"created_at":   "2026-04-28T00:00:00Z",
						"updated_at":   "2026-04-28T00:00:00Z",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"goal":                "Build the thing",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
						"created_at":          "2026-04-28T00:00:00Z",
						"updated_at":          "2026-04-28T00:00:00Z",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-worker",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "agent-1",
							"role_type":        "implementer",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["agent/**"]}`,
							"created_at":       "2026-04-28T00:00:00Z",
							"updated_at":       "2026-04-28T00:00:00Z",
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     remoteURL,
							"remote_kind":    "github",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
							"created_at":     "2026-04-28T00:00:00Z",
							"updated_at":     "2026-04-28T00:00:00Z",
						},
					},
					"checkouts": []any{
						map[string]any{
							"checkout_id":   "checkout-2",
							"workspace_id":  "ws",
							"project_id":    "project-alpha",
							"repo_id":       "repo-main",
							"machine_id":    "machine-a",
							"agent_id":      "agent-1",
							"local_path":    workdir,
							"checkout_kind": "clone",
							"branch_name":   "agent/agent-1/project-alpha/task-build",
							"dirty_state":   "UNKNOWN",
							"status":        "ACTIVE",
							"last_seen_at":  "2026-04-28T00:00:00Z",
							"created_at":    "2026-04-28T00:00:00Z",
							"updated_at":    "2026-04-28T00:00:00Z",
						},
					},
					"branches": []any{
						map[string]any{
							"branch_id":        "branch-1",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"repo_id":          "repo-main",
							"checkout_id":      "checkout-1",
							"agent_id":         "agent-1",
							"branch_name":      "agent/agent-1/project-alpha/task-build",
							"branch_kind":      "feature",
							"base_branch":      "main",
							"write_scope_json": `{"paths":["agent/**"]}`,
							"status":           "RESERVED",
							"created_at":       "2026-04-28T00:00:00Z",
							"updated_at":       "2026-04-28T00:00:00Z",
						},
					},
				},
			})
		case "project.branch.register":
			branchRegisterParams = req.Params
			assertProjectAdmissionGitCheckout(t, workdir, remoteURL, rpcString(req.Params, "branch_name"))
			writeRPCResult(w, req, map[string]any{
				"branch": map[string]any{
					"branch_id":        "branch-1",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"repo_id":          "repo-main",
					"checkout_id":      "checkout-2",
					"agent_id":         "agent-1",
					"branch_name":      rpcString(req.Params, "branch_name"),
					"branch_kind":      "feature",
					"base_branch":      "main",
					"write_scope_json": `{"paths":["agent/**"]}`,
					"status":           "RESERVED",
					"created_at":       "2026-04-28T00:00:00Z",
					"updated_at":       "2026-04-28T00:00:00Z",
				},
			})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-build",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", nil)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.branch.register,agent.task.claim,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(branchRegisterParams, "branch_id") != "branch-1" || rpcString(branchRegisterParams, "checkout_id") != "checkout-2" {
		t.Fatalf("expected stale branch checkout repair, got %+v", branchRegisterParams)
	}
	if rpcString(claimParams, "branch_id") != "branch-1" || rpcString(claimParams, "checkout_id") != "checkout-2" {
		t.Fatalf("expected claim to use repaired branch checkout, got %+v", claimParams)
	}
}

func TestRuntimeClaimTaskReactivatesAbandonedDirtySameBranchCheckout(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	repo := ProjectRepositoryRecord{
		RepoID:        "repo-main",
		ProjectID:     "project-alpha",
		RemoteURL:     remoteURL,
		Name:          "project-alpha",
		DefaultBranch: "main",
	}
	expectedCheckoutPath := projectCheckoutMaterializeDefaultPath(workdir, "project-alpha", repo)
	if _, _, err := materializeGitCheckout(context.Background(), expectedCheckoutPath, repo, "main", false); err != nil {
		t.Fatalf("materialize abandoned checkout: %v", err)
	}
	branchName := projectClaimBranchName("agent-1", "project-alpha", "task-build")
	if err := checkoutProjectClaimBranch(context.Background(), expectedCheckoutPath, branchName, "main"); err != nil {
		t.Fatalf("checkout branch: %v", err)
	}
	if err := os.WriteFile(filepath.Join(expectedCheckoutPath, "dirty.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("dirty checkout: %v", err)
	}

	var methods []string
	var checkoutRegisterParams map[string]any
	var branchRegisterParams map[string]any
	var claimParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"project": map[string]any{
					"workspace_id": "ws",
					"project_id":   "project-alpha",
					"title":        "Project Alpha",
					"status":       "ACTIVE",
				},
				"profile": map[string]any{
					"workspace_id":        "ws",
					"project_id":          "project-alpha",
					"current_phase":       "IMPLEMENTATION",
					"repo_required":       true,
					"repo_status":         "READY",
					"repo_default_branch": "main",
				},
				"roles": []any{
					map[string]any{
						"role_id":          "role-worker",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "agent-1",
						"role_type":        "implementer",
						"status":           "ACTIVE",
						"write_scope_json": `{"paths":["agent/**"]}`,
					},
				},
				"repositories": []any{
					map[string]any{
						"repo_id":        "repo-main",
						"workspace_id":   "ws",
						"project_id":     "project-alpha",
						"remote_url":     remoteURL,
						"remote_kind":    "github",
						"name":           "project-alpha",
						"default_branch": "main",
						"repo_status":    "READY",
						"is_canonical":   true,
					},
				},
				"checkouts": []any{
					map[string]any{
						"checkout_id":   "checkout-abandoned",
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"repo_id":       "repo-main",
						"machine_id":    runtimeMachineID(),
						"agent_id":      "agent-1",
						"local_path":    expectedCheckoutPath,
						"checkout_kind": "clone",
						"branch_name":   branchName,
						"dirty_state":   "dirty",
						"status":        "ABANDONED",
					},
				},
				"branches": []any{
					map[string]any{
						"branch_id":        "branch-1",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"repo_id":          "repo-main",
						"checkout_id":      "checkout-abandoned",
						"agent_id":         "agent-1",
						"branch_name":      branchName,
						"branch_kind":      "feature",
						"base_branch":      "main",
						"write_scope_json": `{"paths":["agent/**"]}`,
						"status":           "ABANDONED",
					},
				},
			}})
		case "project.checkout.register":
			checkoutRegisterParams = req.Params
			if rpcString(req.Params, "checkout_id") != "checkout-abandoned" || rpcString(req.Params, "status") != "ACTIVE" {
				t.Fatalf("expected abandoned checkout reactivation, got %+v", req.Params)
			}
			if rpcString(req.Params, "local_path") != expectedCheckoutPath || !strings.EqualFold(rpcString(req.Params, "dirty_state"), "DIRTY") {
				t.Fatalf("expected dirty same-branch checkout preservation, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-abandoned",
				"workspace_id":  "ws",
				"project_id":    "project-alpha",
				"repo_id":       "repo-main",
				"machine_id":    runtimeMachineID(),
				"agent_id":      "agent-1",
				"local_path":    expectedCheckoutPath,
				"checkout_kind": "clone",
				"branch_name":   branchName,
				"dirty_state":   "DIRTY",
				"status":        "ACTIVE",
			}})
		case "project.branch.register":
			branchRegisterParams = req.Params
			if rpcString(req.Params, "branch_id") != "branch-1" || rpcString(req.Params, "status") != "RESERVED" {
				t.Fatalf("expected abandoned branch to reopen for claim, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-alpha",
				"repo_id":          "repo-main",
				"checkout_id":      "checkout-abandoned",
				"agent_id":         "agent-1",
				"branch_name":      branchName,
				"branch_kind":      "feature",
				"base_branch":      "main",
				"write_scope_json": `{"paths":["agent/**"]}`,
				"status":           "RESERVED",
			}})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-build",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", nil)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.checkout.register,project.branch.register,agent.task.claim,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(checkoutRegisterParams, "checkout_id") != "checkout-abandoned" || rpcString(branchRegisterParams, "checkout_id") != "checkout-abandoned" {
		t.Fatalf("expected recovered checkout binding, checkout=%+v branch=%+v", checkoutRegisterParams, branchRegisterParams)
	}
	if rpcString(claimParams, "branch_id") != "branch-1" || rpcString(claimParams, "checkout_id") != "checkout-abandoned" {
		t.Fatalf("expected claim to use recovered checkout/branch, got %+v", claimParams)
	}
	assertProjectAdmissionGitCheckout(t, expectedCheckoutPath, remoteURL, branchName)
	if _, err := os.Stat(filepath.Join(expectedCheckoutPath, "dirty.txt")); err != nil {
		t.Fatalf("expected dirty same-branch work to be preserved: %v", err)
	}
}

func TestRuntimeClaimTaskMaterializesFreshPathForAbandonedHintedCheckoutDirtyForeignDefault(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	repo := ProjectRepositoryRecord{
		RepoID:        "repo-main",
		ProjectID:     "project-alpha",
		RemoteURL:     remoteURL,
		Name:          "project-alpha",
		DefaultBranch: "main",
	}
	defaultCheckoutPath := projectCheckoutMaterializeDefaultPath(workdir, "project-alpha", repo)
	if _, _, err := materializeGitCheckout(context.Background(), defaultCheckoutPath, repo, "main", false); err != nil {
		t.Fatalf("materialize default checkout: %v", err)
	}
	runProjectAdmissionGit(t, defaultCheckoutPath, "config", "user.email", "test@example.invalid")
	runProjectAdmissionGit(t, defaultCheckoutPath, "config", "user.name", "Rhizome Test")
	hintedBranchName := projectClaimBranchName("agent-1", "project-alpha", "task-readme")
	readyBranch := ProjectBranchRecord{
		BranchID:   "branch-ready",
		BranchName: hintedBranchName,
		HeadSHA:    strings.Repeat("b", 40),
	}
	successorBranchName := projectClaimSuccessorBranchName("agent-1", "project-alpha", "task-readme", readyBranch)
	if err := checkoutProjectClaimBranch(context.Background(), defaultCheckoutPath, hintedBranchName, "main"); err != nil {
		t.Fatalf("checkout hinted branch: %v", err)
	}
	if err := os.WriteFile(filepath.Join(defaultCheckoutPath, "README.md"), []byte("# Project Alpha\n\nReady for review.\n"), 0o644); err != nil {
		t.Fatalf("write readme: %v", err)
	}
	runProjectAdmissionGit(t, defaultCheckoutPath, "add", "README.md")
	runProjectAdmissionGit(t, defaultCheckoutPath, "commit", "-m", "readme")
	headSHA := strings.TrimSpace(mustProjectAdmissionGitOutput(t, defaultCheckoutPath, "rev-parse", "HEAD"))
	runProjectAdmissionGit(t, defaultCheckoutPath, "push", "origin", hintedBranchName)
	foreignBranchName := projectClaimBranchName("agent-1", "project-alpha", "task-parser")
	runProjectAdmissionGit(t, defaultCheckoutPath, "checkout", "-B", foreignBranchName, "main")
	if err := os.WriteFile(filepath.Join(defaultCheckoutPath, "dirty-parser.txt"), []byte("dirty parser work\n"), 0o644); err != nil {
		t.Fatalf("dirty foreign checkout: %v", err)
	}
	recoveredCheckoutPath := projectCheckoutMaterializeBranchPath(workdir, "project-alpha", repo, successorBranchName, "")

	var methods []string
	var checkoutRegisterParams map[string]any
	var branchRegisterParams map[string]any
	var claimParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"project": map[string]any{
					"workspace_id": "ws",
					"project_id":   "project-alpha",
					"title":        "Project Alpha",
					"status":       "ACTIVE",
				},
				"profile": map[string]any{
					"workspace_id":        "ws",
					"project_id":          "project-alpha",
					"current_phase":       "IMPLEMENTATION",
					"repo_required":       true,
					"repo_status":         "READY",
					"repo_default_branch": "main",
				},
				"roles": []any{
					map[string]any{
						"role_id":          "role-worker",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "agent-1",
						"role_type":        "implementer",
						"status":           "ACTIVE",
						"write_scope_json": `{"paths":["docs/**"]}`,
					},
				},
				"repositories": []any{
					map[string]any{
						"repo_id":        "repo-main",
						"workspace_id":   "ws",
						"project_id":     "project-alpha",
						"remote_url":     remoteURL,
						"remote_kind":    "github",
						"name":           "project-alpha",
						"default_branch": "main",
						"repo_status":    "READY",
						"is_canonical":   true,
					},
				},
				"checkouts": []any{
					map[string]any{
						"checkout_id":   "checkout-stale",
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"repo_id":       "repo-main",
						"machine_id":    runtimeMachineID(),
						"agent_id":      "agent-1",
						"local_path":    defaultCheckoutPath,
						"checkout_kind": "clone",
						"branch_name":   hintedBranchName,
						"base_sha":      "main",
						"head_sha":      headSHA,
						"dirty_state":   "clean",
						"status":        "ABANDONED",
					},
				},
				"branches": []any{
					map[string]any{
						"branch_id":        "branch-ready",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"repo_id":          "repo-main",
						"checkout_id":      "checkout-stale",
						"agent_id":         "agent-1",
						"branch_name":      hintedBranchName,
						"branch_kind":      "feature",
						"base_branch":      "main",
						"base_sha":         "main",
						"head_sha":         headSHA,
						"write_scope_json": `{"paths":["docs/**"]}`,
						"status":           "READY_FOR_REVIEW",
						"review_doc_key":   "review/readme",
					},
				},
			}})
		case "project.checkout.register":
			checkoutRegisterParams = req.Params
			if rpcString(req.Params, "local_path") != recoveredCheckoutPath {
				t.Fatalf("expected fresh branch-specific checkout path %s, got %+v", recoveredCheckoutPath, req.Params)
			}
			if rpcString(req.Params, "status") != "ACTIVE" {
				t.Fatalf("expected active recovered checkout, got %+v", req.Params)
			}
			if rpcString(req.Params, "branch_name") != successorBranchName {
				t.Fatalf("expected recovered checkout on successor branch %s, got %+v", successorBranchName, req.Params)
			}
			assertProjectAdmissionGitCheckout(t, recoveredCheckoutPath, remoteURL, successorBranchName)
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-recovered",
				"workspace_id":  "ws",
				"project_id":    "project-alpha",
				"repo_id":       "repo-main",
				"machine_id":    runtimeMachineID(),
				"agent_id":      "agent-1",
				"local_path":    recoveredCheckoutPath,
				"checkout_kind": "clone",
				"branch_name":   successorBranchName,
				"dirty_state":   "CLEAN",
				"status":        "ACTIVE",
			}})
		case "project.branch.register":
			branchRegisterParams = req.Params
			if rpcString(req.Params, "branch_id") != "" || rpcString(req.Params, "checkout_id") != "checkout-recovered" || rpcString(req.Params, "branch_name") != successorBranchName || rpcString(req.Params, "status") != "RESERVED" {
				t.Fatalf("expected successor branch to be registered on recovered checkout, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-successor",
				"workspace_id":     "ws",
				"project_id":       "project-alpha",
				"repo_id":          "repo-main",
				"checkout_id":      "checkout-recovered",
				"agent_id":         "agent-1",
				"branch_name":      successorBranchName,
				"branch_kind":      "feature",
				"base_branch":      "main",
				"write_scope_json": `{"paths":["docs/**"]}`,
				"status":           "RESERVED",
			}})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-readme",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
		ClaimBranchID:       stringPtr("branch-ready"),
	}, "claim project task", nil)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.checkout.register,project.branch.register,agent.task.claim,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(checkoutRegisterParams, "local_path") == defaultCheckoutPath {
		t.Fatalf("expected dirty default checkout to be left alone, got %+v", checkoutRegisterParams)
	}
	if rpcString(branchRegisterParams, "checkout_id") != "checkout-recovered" {
		t.Fatalf("expected successor branch to use recovered checkout, got %+v", branchRegisterParams)
	}
	if rpcString(claimParams, "branch_id") != "branch-successor" || rpcString(claimParams, "checkout_id") != "checkout-recovered" {
		t.Fatalf("expected claim to use successor branch checkout, got %+v", claimParams)
	}
	if current := strings.TrimSpace(mustProjectAdmissionGitOutput(t, defaultCheckoutPath, "branch", "--show-current")); current != foreignBranchName {
		t.Fatalf("dirty default checkout should remain on foreign branch %q, got %q", foreignBranchName, current)
	}
	if _, err := os.Stat(filepath.Join(defaultCheckoutPath, "dirty-parser.txt")); err != nil {
		t.Fatalf("expected dirty default work to be preserved: %v", err)
	}
}

func TestRuntimeClaimTaskAbandonsProvisionalBranchAfterClaimAdmissionFailure(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	expectedCheckoutPath := projectCheckoutMaterializeDefaultPath(workdir, "project-alpha", ProjectRepositoryRecord{
		RepoID: "repo-main",
		Name:   "project-alpha",
	})
	var methods []string
	coordinationGets := 0
	branchRegisters := 0
	checkoutRegisters := 0
	cleanupStatus := ""
	cleanupCheckoutStatus := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			coordinationGets++
			coordination := map[string]any{
				"project": map[string]any{
					"workspace_id": "ws",
					"project_id":   "project-alpha",
					"title":        "Project Alpha",
					"status":       "ACTIVE",
				},
				"profile": map[string]any{
					"workspace_id":        "ws",
					"project_id":          "project-alpha",
					"current_phase":       "IMPLEMENTATION",
					"repo_required":       true,
					"repo_status":         "READY",
					"repo_default_branch": "main",
				},
				"roles": []any{
					map[string]any{
						"role_id":          "role-worker",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "agent-1",
						"role_type":        "implementer",
						"status":           "ACTIVE",
						"write_scope_json": `{"paths":["agent/**"]}`,
					},
				},
				"repositories": []any{
					map[string]any{
						"repo_id":        "repo-main",
						"workspace_id":   "ws",
						"project_id":     "project-alpha",
						"remote_url":     remoteURL,
						"remote_kind":    "github",
						"name":           "project-alpha",
						"default_branch": "main",
						"repo_status":    "READY",
						"is_canonical":   true,
					},
				},
			}
			if coordinationGets > 1 {
				coordination["checkouts"] = []any{
					map[string]any{
						"checkout_id":   "checkout-1",
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"repo_id":       "repo-main",
						"machine_id":    runtimeMachineID(),
						"agent_id":      "agent-1",
						"local_path":    expectedCheckoutPath,
						"checkout_kind": "clone",
						"branch_name":   "agent/agent-1/project-alpha/task-build",
						"dirty_state":   "clean",
						"status":        "ACTIVE",
					},
				}
				coordination["branches"] = []any{
					map[string]any{
						"branch_id":        "branch-1",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"repo_id":          "repo-main",
						"checkout_id":      "checkout-1",
						"agent_id":         "agent-1",
						"branch_name":      "agent/agent-1/project-alpha/task-build",
						"branch_kind":      "feature",
						"base_branch":      "main",
						"write_scope_json": `{"paths":["agent/**"]}`,
						"status":           "RESERVED",
					},
				}
			}
			writeRPCResult(w, req, map[string]any{"coordination": coordination})
		case "project.checkout.register":
			checkoutRegisters++
			if rpcString(req.Params, "local_path") != expectedCheckoutPath {
				t.Fatalf("unexpected checkout register params: %+v", req.Params)
			}
			status := firstNonEmpty(rpcString(req.Params, "status"), "ACTIVE")
			if checkoutRegisters == 2 {
				cleanupCheckoutStatus = status
				if status != "ABANDONED" || rpcString(req.Params, "checkout_id") != "checkout-1" {
					t.Fatalf("expected cleanup to abandon provisional checkout, got %+v", req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"checkout": map[string]any{
					"checkout_id":   "checkout-1",
					"workspace_id":  "ws",
					"project_id":    "project-alpha",
					"repo_id":       "repo-main",
					"machine_id":    rpcString(req.Params, "machine_id"),
					"agent_id":      "agent-1",
					"local_path":    expectedCheckoutPath,
					"checkout_kind": "clone",
					"branch_name":   rpcString(req.Params, "branch_name"),
					"dirty_state":   rpcString(req.Params, "dirty_state"),
					"status":        status,
				},
			})
		case "project.branch.register":
			branchRegisters++
			status := firstNonEmpty(rpcString(req.Params, "status"), "RESERVED")
			if branchRegisters == 1 && status != "RESERVED" {
				t.Fatalf("expected first branch registration to reserve branch, got %+v", req.Params)
			}
			if branchRegisters == 2 {
				cleanupStatus = status
				if status != "ABANDONED" || rpcString(req.Params, "branch_id") != "branch-1" {
					t.Fatalf("expected cleanup to abandon provisional branch, got %+v", req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"branch": map[string]any{
					"branch_id":        "branch-1",
					"workspace_id":     "ws",
					"project_id":       "project-alpha",
					"repo_id":          "repo-main",
					"checkout_id":      "checkout-1",
					"agent_id":         "agent-1",
					"branch_name":      rpcString(req.Params, "branch_name"),
					"branch_kind":      "feature",
					"base_branch":      "main",
					"write_scope_json": `{"paths":["agent/**"]}`,
					"status":           status,
				},
			})
		case "agent.task.claim":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error": map[string]any{
					"code":    -32000,
					"message": "task claim project admission invalid: write_scope_json overlaps active claim task_id=task-other branch_id=branch-other",
				},
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-build",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", nil)
	if err == nil || !strings.Contains(err.Error(), "write_scope_json overlaps") {
		t.Fatalf("expected original claim admission error, got %v", err)
	}
	if cleanupStatus != "ABANDONED" {
		t.Fatalf("expected cleanup branch registration, status=%q methods=%s", cleanupStatus, strings.Join(methods, ","))
	}
	if cleanupCheckoutStatus != "ABANDONED" {
		t.Fatalf("expected cleanup checkout registration, status=%q methods=%s", cleanupCheckoutStatus, strings.Join(methods, ","))
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,project.checkout.register,project.branch.register,agent.task.claim,project.coordination.get,project.branch.register,project.checkout.register" {
		t.Fatalf("unexpected method order: %s", got)
	}
}

func TestRuntimeClaimTaskRejectsDirtyUnboundCheckoutBeforeClaim(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	repo := ProjectRepositoryRecord{
		RepoID:        "repo-main",
		ProjectID:     "project-alpha",
		RemoteURL:     remoteURL,
		Name:          "project-alpha",
		DefaultBranch: "main",
	}
	expectedCheckoutPath := projectCheckoutMaterializeDefaultPath(workdir, "project-alpha", repo)
	if _, _, err := materializeGitCheckout(context.Background(), expectedCheckoutPath, repo, "main", false); err != nil {
		t.Fatalf("materialize existing checkout: %v", err)
	}
	if err := os.WriteFile(filepath.Join(expectedCheckoutPath, "dirty.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("dirty checkout: %v", err)
	}
	var methods []string
	branchName := projectClaimBranchName("agent-1", "project-alpha", "task-build")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"project": map[string]any{
					"workspace_id": "ws",
					"project_id":   "project-alpha",
					"title":        "Project Alpha",
					"status":       "ACTIVE",
				},
				"profile": map[string]any{
					"workspace_id":        "ws",
					"project_id":          "project-alpha",
					"current_phase":       "IMPLEMENTATION",
					"repo_required":       true,
					"repo_status":         "READY",
					"repo_default_branch": "main",
				},
				"roles": []any{
					map[string]any{
						"role_id":          "role-worker",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "agent-1",
						"role_type":        "implementer",
						"status":           "ACTIVE",
						"write_scope_json": `{"paths":["agent/**"]}`,
					},
				},
				"repositories": []any{
					map[string]any{
						"repo_id":        "repo-main",
						"workspace_id":   "ws",
						"project_id":     "project-alpha",
						"remote_url":     remoteURL,
						"remote_kind":    "github",
						"name":           "project-alpha",
						"default_branch": "main",
						"repo_status":    "READY",
						"is_canonical":   true,
					},
				},
				"checkouts": []any{
					map[string]any{
						"checkout_id":   "checkout-dirty",
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"repo_id":       "repo-main",
						"machine_id":    runtimeMachineID(),
						"agent_id":      "agent-1",
						"local_path":    expectedCheckoutPath,
						"checkout_kind": "clone",
						"branch_name":   branchName,
						"dirty_state":   "dirty",
						"status":        "ACTIVE",
					},
				},
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-build",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", nil)
	if err == nil || !strings.Contains(err.Error(), "existing checkout") || !strings.Contains(err.Error(), "dirty") {
		t.Fatalf("expected dirty checkout rejection before claim, got %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get" {
		t.Fatalf("dirty checkout should fail before claim/register calls, methods=%s", got)
	}
}

func TestRuntimeClaimTaskReusesDirtyBranchBoundCheckoutAfterReclaimRelease(t *testing.T) {
	workdir := t.TempDir()
	remoteURL := initProjectAdmissionRemote(t)
	repo := ProjectRepositoryRecord{
		RepoID:        "repo-main",
		ProjectID:     "project-alpha",
		RemoteURL:     remoteURL,
		Name:          "project-alpha",
		DefaultBranch: "main",
	}
	expectedCheckoutPath := projectCheckoutMaterializeDefaultPath(workdir, "project-alpha", repo)
	if _, _, err := materializeGitCheckout(context.Background(), expectedCheckoutPath, repo, "main", false); err != nil {
		t.Fatalf("materialize existing checkout: %v", err)
	}
	branchName := projectClaimBranchName("agent-1", "project-alpha", "task-build")
	if err := checkoutProjectClaimBranch(context.Background(), expectedCheckoutPath, branchName, "main"); err != nil {
		t.Fatalf("checkout branch: %v", err)
	}
	if err := os.WriteFile(filepath.Join(expectedCheckoutPath, "dirty.txt"), []byte("retry-owned dirty work\n"), 0o644); err != nil {
		t.Fatalf("dirty checkout: %v", err)
	}

	var methods []string
	var claimParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"project": map[string]any{
					"workspace_id": "ws",
					"project_id":   "project-alpha",
					"title":        "Project Alpha",
					"status":       "ACTIVE",
				},
				"profile": map[string]any{
					"workspace_id":        "ws",
					"project_id":          "project-alpha",
					"current_phase":       "IMPLEMENTATION",
					"repo_required":       true,
					"repo_status":         "READY",
					"repo_default_branch": "main",
				},
				"roles": []any{
					map[string]any{
						"role_id":          "role-worker",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"agent_id":         "agent-1",
						"role_type":        "implementer",
						"status":           "ACTIVE",
						"write_scope_json": `{"paths":["agent/**"]}`,
					},
				},
				"repositories": []any{
					map[string]any{
						"repo_id":        "repo-main",
						"workspace_id":   "ws",
						"project_id":     "project-alpha",
						"remote_url":     remoteURL,
						"remote_kind":    "github",
						"name":           "project-alpha",
						"default_branch": "main",
						"repo_status":    "READY",
						"is_canonical":   true,
					},
				},
				"checkouts": []any{
					map[string]any{
						"checkout_id":   "checkout-released",
						"workspace_id":  "ws",
						"project_id":    "project-alpha",
						"repo_id":       "repo-main",
						"machine_id":    runtimeMachineID(),
						"agent_id":      "agent-1",
						"local_path":    expectedCheckoutPath,
						"checkout_kind": "clone",
						"branch_name":   branchName,
						"dirty_state":   "dirty",
						"status":        "ACTIVE",
					},
				},
				"branches": []any{
					map[string]any{
						"branch_id":        "branch-released",
						"workspace_id":     "ws",
						"project_id":       "project-alpha",
						"repo_id":          "repo-main",
						"checkout_id":      "checkout-released",
						"agent_id":         "agent-1",
						"branch_name":      branchName,
						"branch_kind":      "feature",
						"base_branch":      "main",
						"write_scope_json": `{"paths":["agent/**"]}`,
						"status":           "ACTIVE",
					},
				},
			}})
		case "agent.task.claim":
			claimParams = req.Params
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-claim-admitted"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-build",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", nil)
	if err != nil {
		t.Fatalf("claimTaskWithAdmission() error = %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get,agent.task.claim,agent.update.post" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(claimParams, "branch_id") != "branch-released" || rpcString(claimParams, "checkout_id") != "checkout-released" {
		t.Fatalf("expected claim to reuse released branch/checkout binding, got %+v", claimParams)
	}
	assertProjectAdmissionGitCheckout(t, expectedCheckoutPath, remoteURL, branchName)
	if _, err := os.Stat(filepath.Join(expectedCheckoutPath, "dirty.txt")); err != nil {
		t.Fatalf("expected retry-owned dirty work to be preserved: %v", err)
	}
}

func TestRuntimeClaimTaskFailsClosedWithoutProjectWriteScope(t *testing.T) {
	workdir := t.TempDir()
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"description":  "Build the thing",
						"status":       "ACTIVE",
						"created_by":   "owner",
						"created_at":   "2026-04-28T00:00:00Z",
						"updated_at":   "2026-04-28T00:00:00Z",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"goal":                "Build the thing",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
						"created_at":          "2026-04-28T00:00:00Z",
						"updated_at":          "2026-04-28T00:00:00Z",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "role-lead-empty-scope",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "agent-1",
							"role_type":        "STRATEGIC_LEAD",
							"status":           "ACTIVE",
							"write_scope_json": `{}`,
							"created_at":       "2026-04-28T00:00:00Z",
							"updated_at":       "2026-04-28T00:00:00Z",
						},
						map[string]any{
							"role_id":          "role-reviewer-scope",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "agent-1",
							"role_type":        "REVIEWER",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["tests/**"]}`,
							"created_at":       "2026-04-28T00:00:00Z",
							"updated_at":       "2026-04-28T00:00:00Z",
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     "https://github.com/ExampleOrg/project-alpha.git",
							"remote_kind":    "github",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
							"created_at":     "2026-04-28T00:00:00Z",
							"updated_at":     "2026-04-28T00:00:00Z",
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q after missing write scope", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-build",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", nil)
	if err == nil || !strings.Contains(err.Error(), "write_scope_json") {
		t.Fatalf("expected missing write scope to fail closed, got %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get" {
		t.Fatalf("unexpected method order: %s", got)
	}

	methods = nil
	runtime.cfg.CoordinationMode = CoordinationModeTrustFirst
	err = runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-opaque",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", nil)
	if err == nil || !strings.Contains(err.Error(), "write_scope_json") {
		t.Fatalf("expected trust-first missing write scope to fail closed, got %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get" {
		t.Fatalf("unexpected trust-first method order: %s", got)
	}
}

func TestRuntimeClaimTaskFailsClosedForSpecPhaseImplementationLane(t *testing.T) {
	workdir := t.TempDir()
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":               "ws",
						"project_id":                 "project-alpha",
						"current_phase":              "SPEC",
						"design_doc_id":              "doc.design",
						"implementation_plan_doc_id": "doc.plan",
						"repo_required":              true,
						"repo_status":                "READY",
						"repo_default_branch":        "main",
					},
					"gate_status": map[string]any{
						"workspace_id":         "ws",
						"project_id":           "project-alpha",
						"current_phase":        "SPEC",
						"overall_state":        "BLOCKED",
						"implementation_ready": false,
						"gates": []any{
							map[string]any{
								"gate_key": "implementation_phase_open",
								"state":    "BLOCKED",
								"required": true,
								"summary":  "Project phase must be IMPLEMENTATION before implementation work",
								"source":   "derived",
							},
						},
					},
					"roles": []any{
						map[string]any{
							"role_id":          "projrole-agent",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "agent-1",
							"role_type":        "IMPLEMENTER",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["agent/**"]}`,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_url":     "file:///tmp/project-alpha.git",
							"remote_kind":    "local",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q after spec-phase gate closure", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := false
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-build",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", nil)
	if err == nil || !strings.Contains(err.Error(), "implementation_phase_open") {
		t.Fatalf("expected spec-phase project gate closure, got %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get" {
		t.Fatalf("unexpected method order: %s", got)
	}
}

func TestRuntimeClaimTaskFailsClosedForReadyRepoWithoutRemote(t *testing.T) {
	workdir := t.TempDir()
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{
						"workspace_id": "ws",
						"project_id":   "project-alpha",
						"title":        "Project Alpha",
						"status":       "ACTIVE",
					},
					"profile": map[string]any{
						"workspace_id":        "ws",
						"project_id":          "project-alpha",
						"current_phase":       "IMPLEMENTATION",
						"repo_required":       true,
						"repo_status":         "READY",
						"repo_default_branch": "main",
					},
					"roles": []any{
						map[string]any{
							"role_id":          "projrole-agent",
							"workspace_id":     "ws",
							"project_id":       "project-alpha",
							"agent_id":         "agent-1",
							"role_type":        "IMPLEMENTER",
							"status":           "ACTIVE",
							"write_scope_json": `{"paths":["agent/**"]}`,
						},
					},
					"repositories": []any{
						map[string]any{
							"repo_id":        "repo-main",
							"workspace_id":   "ws",
							"project_id":     "project-alpha",
							"remote_kind":    "github",
							"default_branch": "main",
							"repo_status":    "READY",
							"is_canonical":   true,
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q after repo without remote", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
			Workdir:     workdir,
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	requiresProjectGate := true
	err := runtime.claimTaskWithAdmission(context.Background(), WorkspaceTaskRecord{
		TaskID:              "task-build",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		RequiresProjectGate: &requiresProjectGate,
	}, "claim project task", nil)
	if err == nil || !strings.Contains(err.Error(), "READY repository") {
		t.Fatalf("expected READY repository with remote_url failure, got %v", err)
	}
	if got := strings.Join(methods, ","); got != "project.coordination.get" {
		t.Fatalf("unexpected method order: %s", got)
	}
}

func initProjectAdmissionGitCheckout(t *testing.T, dir, remoteURL string) {
	t.Helper()
	for _, args := range [][]string{
		{"init"},
		{"remote", "add", "origin", remoteURL},
	} {
		runProjectAdmissionGit(t, dir, args...)
	}
}

func initProjectAdmissionRemote(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	src := filepath.Join(root, "src")
	remote := filepath.Join(root, "remote.git")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatalf("create source repo dir: %v", err)
	}
	runProjectAdmissionGit(t, src, "init")
	runProjectAdmissionGit(t, src, "checkout", "-B", "main")
	runProjectAdmissionGit(t, src, "config", "user.email", "test@example.invalid")
	runProjectAdmissionGit(t, src, "config", "user.name", "Rhizome Test")
	if err := os.WriteFile(filepath.Join(src, "README.md"), []byte("# Project Alpha\n"), 0o644); err != nil {
		t.Fatalf("write seed readme: %v", err)
	}
	runProjectAdmissionGit(t, src, "add", "README.md")
	runProjectAdmissionGit(t, src, "commit", "-m", "seed")
	cmd := exec.Command("git", "clone", "--bare", src, remote)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git clone --bare failed: %v\n%s", err, string(out))
	}
	return filepath.ToSlash(remote)
}

func assertProjectAdmissionGitCheckout(t *testing.T, dir, remoteURL, branchName string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, ".git")); err != nil {
		t.Fatalf("expected materialized git checkout at %s: %v", dir, err)
	}
	out, err := exec.Command("git", "-C", dir, "remote", "get-url", "origin").CombinedOutput()
	if err != nil {
		t.Fatalf("git remote get-url failed: %v\n%s", err, string(out))
	}
	if normalizeGitRemoteForCompare(string(out)) != normalizeGitRemoteForCompare(remoteURL) {
		t.Fatalf("unexpected checkout remote: got %q want %q", strings.TrimSpace(string(out)), remoteURL)
	}
	out, err = exec.Command("git", "-C", dir, "branch", "--show-current").CombinedOutput()
	if err != nil {
		t.Fatalf("git branch --show-current failed: %v\n%s", err, string(out))
	}
	if strings.TrimSpace(string(out)) != strings.TrimSpace(branchName) {
		t.Fatalf("unexpected checkout branch: got %q want %q", strings.TrimSpace(string(out)), branchName)
	}
}

func runProjectAdmissionGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, string(out))
	}
}

func mustProjectAdmissionGitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, string(out))
	}
	return string(out)
}
