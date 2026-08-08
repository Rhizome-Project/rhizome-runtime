package sqlite

import (
	"bufio"
	"strings"
)

// Anti-gaming diff-implements-claim gate (Phase 1b, ground-truth, fail-closed).
//
// A commit/branch that CLAIMS to implement an acceptance-criteria capability must actually
// contain that implementation - not a dependency-add, a stub, or a delegation. This closes the
// second half of the spec-gaming failure mode: commit 919f156 "Implement AST, control flow, and
// function forms" added zero AST/parser/eval code - its diff was a gopher-lua dep-add plus a
// runner rewire. NO-VENDORING blocks that via the dependency; diff-implements-claim blocks the
// broader class (a stub or a mislabeled commit) by requiring the claimed CAPABILITY to land
// substantive net-new implementation in the claimed paths.
//
// The claim is a structured, fenced block in the decision/review doc (the AC-mapping field is
// empty by construction, so the claim must be explicit and machine-checkable):
//
//	```implements_capability_v1
//	ac: AC-LUA-EVAL-01, AC-LUA-PARSE-01
//	implementation_paths: internal/eval/**, internal/parser/**
//	```
//
// Polarity is fail-closed: a present claim that the base..head diff does not back with
// substantive implementation in EACH claimed path BLOCKS accept.

const implementsCapabilityClaimSchema = "implements_capability_v1"

// minClaimImplementationLOC is the minimum net-new non-test implementation lines a claimed path
// must receive for the claim to be considered implemented (a stub/dep-add/delegation falls short).
const minClaimImplementationLOC = 5

type capabilityClaim struct {
	Present             bool
	ACIDs               []string
	ImplementationPaths []string
}

// parseCapabilityClaim extracts the implements_capability_v1 fenced block from a decision/review
// doc. Returns Present=false when no claim is declared (the gate then has nothing to verify).
func parseCapabilityClaim(docContent string) capabilityClaim {
	claim := capabilityClaim{}
	sc := bufio.NewScanner(strings.NewReader(docContent))
	inBlock := false
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !inBlock {
			if strings.HasPrefix(line, "```") && strings.Contains(line, implementsCapabilityClaimSchema) {
				inBlock = true
				claim.Present = true
			}
			continue
		}
		if strings.HasPrefix(line, "```") {
			break
		}
		lower := strings.ToLower(line)
		switch {
		case strings.HasPrefix(lower, "ac:"), strings.HasPrefix(lower, "acceptance_criteria:"):
			claim.ACIDs = append(claim.ACIDs, splitClaimList(line[strings.Index(line, ":")+1:])...)
		case strings.HasPrefix(lower, "implementation_paths:"), strings.HasPrefix(lower, "paths:"):
			claim.ImplementationPaths = append(claim.ImplementationPaths, splitClaimList(line[strings.Index(line, ":")+1:])...)
		}
	}
	return claim
}

func splitClaimList(s string) []string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "[]")
	var out []string
	for _, part := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' }) {
		p := strings.TrimSpace(strings.Trim(part, "\"'`"))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// diffImplementsClaimViolations verifies that each claimed implementation path received
// substantive net-new non-test implementation in the base..head diff. addedLOCByFile maps a
// changed file path to its added line count (from git --numstat). Empty result = claim backed.
// No claim => nil (nothing to verify). Fail-closed: a present claim with no backing diff blocks.
func diffImplementsClaimViolations(claim capabilityClaim, addedLOCByFile map[string]int) []string {
	if !claim.Present {
		return nil
	}
	if len(claim.ImplementationPaths) == 0 {
		return []string{"claim_unverifiable: implements_capability_v1 declares no implementation_paths to verify against the diff"}
	}
	var reasons []string
	for _, p := range claim.ImplementationPaths {
		if claimedPathImplementationLOC(addedLOCByFile, p) < minClaimImplementationLOC {
			ac := strings.Join(claim.ACIDs, ",")
			if ac == "" {
				ac = "(unspecified AC)"
			}
			reasons = append(reasons, "claim_not_implemented_in_path: "+strings.TrimSpace(p)+" (claimed "+ac+" but the diff adds no substantive implementation there - dep-add/stub/delegation does not count)")
		}
	}
	return reasons
}

func claimedPathImplementationLOC(addedLOCByFile map[string]int, claimedPath string) int {
	total := 0
	for file, loc := range addedLOCByFile {
		if fileIsImplementationGo(file) && pathMatchesClaimedPath(file, claimedPath) {
			total += loc
		}
	}
	return total
}

// fileIsImplementationGo reports whether a changed file is roster-authored implementation code
// (a .go file that is not a test, not go.mod/go.sum, not vendored, not generated boilerplate).
func fileIsImplementationGo(file string) bool {
	f := strings.ToLower(strings.TrimSpace(file))
	if !strings.HasSuffix(f, ".go") {
		return false
	}
	if strings.HasSuffix(f, "_test.go") {
		return false
	}
	base := f
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	if base == "go.mod" || base == "go.sum" {
		return false
	}
	if strings.HasPrefix(f, "vendor/") || strings.Contains(f, "/vendor/") {
		return false
	}
	return true
}

func pathMatchesClaimedPath(file, claimed string) bool {
	file = strings.TrimSpace(file)
	claimed = strings.TrimSpace(claimed)
	claimed = strings.TrimSuffix(claimed, "/**")
	claimed = strings.TrimSuffix(claimed, "/*")
	claimed = strings.TrimSuffix(claimed, "**")
	claimed = strings.TrimSuffix(claimed, "/")
	if claimed == "" {
		return false
	}
	return file == claimed || strings.HasPrefix(file, claimed+"/")
}
