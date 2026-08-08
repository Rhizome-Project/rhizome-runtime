package sqlite

import "testing"

// TestParseCapabilityClaim locks the structured-claim parser.
func TestParseCapabilityClaim(t *testing.T) {
	doc := "# Decision\n\nAccepted.\n\n```implements_capability_v1\n" +
		"ac: AC-LUA-EVAL-01, AC-LUA-PARSE-01\n" +
		"implementation_paths: internal/eval/**, internal/parser/**\n" +
		"```\n"
	claim := parseCapabilityClaim(doc)
	if !claim.Present {
		t.Fatal("claim must be Present")
	}
	if len(claim.ACIDs) != 2 || claim.ACIDs[0] != "AC-LUA-EVAL-01" {
		t.Fatalf("ACIDs = %v", claim.ACIDs)
	}
	if len(claim.ImplementationPaths) != 2 || claim.ImplementationPaths[0] != "internal/eval/**" {
		t.Fatalf("ImplementationPaths = %v", claim.ImplementationPaths)
	}
	if c := parseCapabilityClaim("# Decision\n\nNo structured claim.\n"); c.Present {
		t.Fatal("a doc without the fenced block must yield Present=false")
	}
}

// TestDiffImplementsClaimGoldenCases locks the analyzer against 919f156's real diff and inverses.
// 919f156 "Implement AST, control flow, and function forms" numstat:
//
//	go.mod 2, go.sum 2, internal/runner/runner.go 32, internal/runner/runner_test.go 37
//
// It touches NO ast/parser/eval file - so an honest claim of those paths is NOT backed -> BLOCK.
func TestDiffImplementsClaimGoldenCases(t *testing.T) {
	realDiff919f156 := map[string]int{
		"go.mod":                         2,
		"go.sum":                         2,
		"internal/runner/runner.go":      32,
		"internal/runner/runner_test.go": 37,
	}

	// (reds-without-fix) Claim "implement AST/parser/eval" but the diff adds none there -> BLOCK.
	claimAST := capabilityClaim{
		Present:             true,
		ACIDs:               []string{"AC-LUA-PARSE-01"},
		ImplementationPaths: []string{"internal/ast/**", "internal/parser/**", "internal/eval/**"},
	}
	if v := diffImplementsClaimViolations(claimAST, realDiff919f156); len(v) == 0 {
		t.Fatal("919f156 claims AST/parser/eval but its diff implements none of them - must BLOCK")
	}

	// go.mod/go.sum and _test.go do not count as implementation, so a claim that ONLY those
	// paths changed is not backed.
	claimDeps := capabilityClaim{Present: true, ACIDs: []string{"AC-X"}, ImplementationPaths: []string{"go.mod"}}
	if v := diffImplementsClaimViolations(claimDeps, realDiff919f156); len(v) == 0 {
		t.Fatal("a dependency-add (go.mod) is not an implementation - must BLOCK")
	}

	// A faithful claim whose path receives substantive net-new implementation -> PASS.
	honest := map[string]int{
		"internal/eval/eval.go":      140,
		"internal/eval/eval_test.go": 60,
		"go.sum":                     4,
	}
	claimEval := capabilityClaim{Present: true, ACIDs: []string{"AC-LUA-EVAL-01"}, ImplementationPaths: []string{"internal/eval/**"}}
	if v := diffImplementsClaimViolations(claimEval, honest); len(v) != 0 {
		t.Fatalf("a real evaluator implementation in the claimed path must PASS, got %v", v)
	}

	// A stub (below the LOC floor) in the claimed path -> BLOCK.
	stub := map[string]int{"internal/eval/eval.go": 2}
	if v := diffImplementsClaimViolations(claimEval, stub); len(v) == 0 {
		t.Fatal("a sub-threshold stub in the claimed path must BLOCK")
	}

	// No structured claim -> nothing to verify (gate is scoped to explicit claims).
	if v := diffImplementsClaimViolations(capabilityClaim{Present: false}, realDiff919f156); v != nil {
		t.Fatalf("absent claim must be a no-op, got %v", v)
	}

	// Fail-closed: a present claim with an empty diff cannot be backed -> BLOCK.
	if v := diffImplementsClaimViolations(claimEval, map[string]int{}); len(v) == 0 {
		t.Fatal("a present claim with no backing diff must BLOCK (fail-closed)")
	}

	// Fail-closed: a present claim that declares no implementation_paths is unverifiable -> BLOCK.
	if v := diffImplementsClaimViolations(capabilityClaim{Present: true, ACIDs: []string{"AC-X"}}, honest); len(v) == 0 {
		t.Fatal("a claim with no implementation_paths must BLOCK (unverifiable)")
	}
}

// TestPathMatchesClaimedPath locks glob/prefix matching.
func TestPathMatchesClaimedPath(t *testing.T) {
	cases := []struct {
		file, claimed string
		want          bool
	}{
		{"internal/eval/eval.go", "internal/eval/**", true},
		{"internal/eval/sub/x.go", "internal/eval", true},
		{"internal/runner/runner.go", "internal/eval/**", false},
		{"internal/evaluator/x.go", "internal/eval", false}, // sibling prefix must not match
		{"cmd/glua/main.go", "cmd/glua", true},
	}
	for _, tc := range cases {
		if got := pathMatchesClaimedPath(tc.file, tc.claimed); got != tc.want {
			t.Fatalf("pathMatchesClaimedPath(%q,%q)=%v want %v", tc.file, tc.claimed, got, tc.want)
		}
	}
}
