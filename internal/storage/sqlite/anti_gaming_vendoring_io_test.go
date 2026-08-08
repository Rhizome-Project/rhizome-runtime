package sqlite

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func vendoringFixtureRepoForTest(t *testing.T) (gitDir, cleanSHA, vendoredSHA, parserSHA string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is required for repository-backed vendoring tests")
	}
	repo := filepath.Join(t.TempDir(), "interpreter-fixture")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatalf("create fixture repo: %v", err)
	}
	runGitFixtureForTest(t, repo, "init")
	runGitFixtureForTest(t, repo, "config", "user.name", "Rhizome Test")
	runGitFixtureForTest(t, repo, "config", "user.email", "test@example.invalid")
	writeFixtureGoModForTest(t, repo, "module example.invalid/interpreter\n\ngo 1.24\n")
	runGitFixtureForTest(t, repo, "add", "go.mod")
	runGitFixtureForTest(t, repo, "commit", "-m", "clean interpreter module")
	cleanSHA = strings.TrimSpace(runGitFixtureForTest(t, repo, "rev-parse", "HEAD"))
	writeFixtureGoModForTest(t, repo, "module example.invalid/interpreter\n\ngo 1.24\n\nrequire github.com/yuin/gopher-lua v1.1.1\n")
	if err := os.MkdirAll(filepath.Join(repo, "internal", "runner"), 0o755); err != nil {
		t.Fatalf("create runner fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, "internal", "runner", "runner.go"), []byte("package runner\n\nfunc Run() {}\n"), 0o600); err != nil {
		t.Fatalf("write runner fixture: %v", err)
	}
	runGitFixtureForTest(t, repo, "add", "go.mod", "internal/runner/runner.go")
	runGitFixtureForTest(t, repo, "commit", "-m", "add prohibited interpreter dependency")
	vendoredSHA = strings.TrimSpace(runGitFixtureForTest(t, repo, "rev-parse", "HEAD"))
	if err := os.MkdirAll(filepath.Join(repo, "internal", "parser"), 0o755); err != nil {
		t.Fatalf("create parser fixture: %v", err)
	}
	parserSource := "package parser\n\ntype Node struct {\n\tKind string\n\tValue string\n}\n\nfunc Parse(input string) Node {\n\treturn Node{Kind: \"literal\", Value: input}\n}\n"
	if err := os.WriteFile(filepath.Join(repo, "internal", "parser", "parser.go"), []byte(parserSource), 0o600); err != nil {
		t.Fatalf("write parser fixture: %v", err)
	}
	runGitFixtureForTest(t, repo, "add", "internal/parser/parser.go")
	runGitFixtureForTest(t, repo, "commit", "-m", "implement parser")
	parserSHA = strings.TrimSpace(runGitFixtureForTest(t, repo, "rev-parse", "HEAD"))
	return filepath.Join(repo, ".git"), cleanSHA, vendoredSHA, parserSHA
}

func runGitFixtureForTest(t *testing.T, repo string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = repo
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func writeFixtureGoModForTest(t *testing.T, repo, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, "go.mod"), []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture go.mod: %v", err)
	}
}

// TestProjectInterpreterVendoringVerdictAgainstGitRepo proves that the
// NO-VENDORING gate reads immutable go.mod objects from Git history.
func TestProjectInterpreterVendoringVerdictAgainstGitRepo(t *testing.T) {
	gitDir, cleanSHA, vendoredSHA, _ := vendoringFixtureRepoForTest(t)
	ctx := context.Background()

	vendoredGoMod, err := readGoModAtGitHead(ctx, gitDir, vendoredSHA)
	if err != nil {
		t.Fatalf("read vendored go.mod: %v", err)
	}
	if v := interpreterVendoringViolations(vendoredGoMod); len(v) == 0 {
		t.Fatalf("expected vendored commit to be blocked; go.mod=\n%s", vendoredGoMod)
	}
	cleanGoMod, err := readGoModAtGitHead(ctx, gitDir, cleanSHA)
	if err != nil {
		t.Fatalf("read clean go.mod: %v", err)
	}
	if v := interpreterVendoringViolations(cleanGoMod); len(v) != 0 {
		t.Fatalf("expected clean commit to pass, got violations %v; go.mod=\n%s", v, cleanGoMod)
	}
}

func TestProjectInterpreterVendoringVerdictReadsLocalFileURI(t *testing.T) {
	gitDir, cleanSHA, vendoredSHA, _ := vendoringFixtureRepoForTest(t)
	ctx := context.Background()
	remoteURL := "file:///" + filepath.ToSlash(gitDir)
	if v := projectInterpreterVendoringVerdict(ctx, remoteURL, cleanSHA); len(v) != 0 {
		t.Fatalf("expected clean commit to pass via file URI, got %v", v)
	}
	if v := projectInterpreterVendoringVerdict(ctx, remoteURL, vendoredSHA); len(v) == 0 {
		t.Fatal("expected vendored commit to be blocked via file URI")
	}
	candidates, ok := localGitDirCandidatesFromRemoteURL(remoteURL)
	if !ok || len(candidates) != 1 || candidates[0] != gitDir {
		t.Fatalf("local candidates = %v, ok=%v; want [%s]", candidates, ok, gitDir)
	}
}

func TestProjectInterpreterVendoringVerdictReadStillFailsClosed(t *testing.T) {
	gitDir, _, _, _ := vendoringFixtureRepoForTest(t)
	remoteURL := "file:///" + filepath.ToSlash(gitDir)
	v := projectInterpreterVendoringVerdict(context.Background(), remoteURL, strings.Repeat("f", 40))
	if len(v) == 0 {
		t.Fatal("missing candidate object must BLOCK; read failure cannot become an allow")
	}
	if !strings.Contains(v[0], "no_vendoring_unproven") || !strings.Contains(v[0], "cannot read go.mod at head") {
		t.Fatalf("unexpected fail-closed reason: %v", v)
	}
}

// TestLocalGitDirFromRemoteURL locks the file:// -> local path resolution (incl. the Windows
// drive-letter leading-slash case) and the fail-closed non-local-remote behaviour.
func TestLocalGitDirFromRemoteURL(t *testing.T) {
	cases := []struct {
		url     string
		wantOK  bool
		wantHas string // substring the resolved path must contain (when ok)
	}{
		{"file:///C:/Users/x/repo.git", true, "Users"},
		{"file:///home/u/repo.git", true, "home"},
		{"/var/repos/repo.git", true, "repos"},
		{"https://github.com/x/y.git", false, ""},
		{"ssh://git@host/x.git", false, ""},
		{"", false, ""},
	}
	for _, tc := range cases {
		got, ok := localGitDirFromRemoteURL(tc.url)
		if ok != tc.wantOK {
			t.Fatalf("localGitDirFromRemoteURL(%q) ok=%v want %v (got %q)", tc.url, ok, tc.wantOK, got)
		}
		if ok && tc.wantHas != "" && !strings.Contains(got, tc.wantHas) {
			t.Fatalf("localGitDirFromRemoteURL(%q)=%q, expected to contain %q", tc.url, got, tc.wantHas)
		}
	}
}
