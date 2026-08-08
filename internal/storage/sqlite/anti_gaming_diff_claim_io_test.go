package sqlite

import (
	"context"
	"path/filepath"
	"testing"
)

// TestProjectDiffClaimVerdictAgainstGitRepo proves that a dependency and
// runner-only commit cannot satisfy a parser implementation claim.
func TestProjectDiffClaimVerdictAgainstGitRepo(t *testing.T) {
	gitDir, cleanSHA, vendoredSHA, _ := vendoringFixtureRepoForTest(t)
	ctx := context.Background()

	added, err := readDiffNumstatAtGitHead(ctx, gitDir, cleanSHA, vendoredSHA)
	if err != nil {
		t.Fatalf("read fixture numstat: %v", err)
	}
	if added["internal/runner/runner.go"] == 0 {
		t.Fatalf("expected internal/runner/runner.go in fixture numstat, got %v", added)
	}
	claim := capabilityClaim{
		Present:             true,
		ACIDs:               []string{"AC-LUA-PARSE-01"},
		ImplementationPaths: []string{"internal/ast/**", "internal/parser/**", "internal/eval/**"},
	}
	if v := diffImplementsClaimViolations(claim, added); len(v) == 0 {
		t.Fatalf("runner-only commit claims AST/parser/eval but implements none -> must BLOCK; numstat=%v", added)
	}
}

func TestProjectDiffClaimVerdictReadsLocalFileURI(t *testing.T) {
	gitDir, _, vendoredSHA, parserSHA := vendoringFixtureRepoForTest(t)
	ctx := context.Background()

	claim := capabilityClaim{
		Present:             true,
		ACIDs:               []string{"AC-LUA-PARSE-01"},
		ImplementationPaths: []string{"internal/parser/**"},
	}
	remoteURL := "file:///" + filepath.ToSlash(gitDir)
	if v := projectDiffClaimVerdict(ctx, remoteURL, vendoredSHA, parserSHA, claim); len(v) != 0 {
		t.Fatalf("expected parser claim to pass via local file URI, got %v", v)
	}
}
