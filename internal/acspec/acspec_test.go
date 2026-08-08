package acspec

import (
	"reflect"
	"testing"
)

// Pin the tokenizer against the EXACT acceptance_criteria_refs the live signal01 seed
// writes into project.<id>.source_requirements_trace. The historical
// `PREFIX-\d+...` pattern did not match AC-LEX-01 (a letter, not a digit, follows the
// prefix dash), which would have silently produced an EMPTY required-set -> the
// forbidden "frontier-empty == DONE" oracle. This locks the widened shape.
func TestNormalizeMatchesSeedAcceptanceRefs(t *testing.T) {
	// As emitted by sourceFidelityFieldValues over the trace block: lower-cased list items.
	raw := []string{"ac-lex-01", "ac-parse-01", "ac-eval-01", "ac-cli-01"}
	got := Normalize(raw)
	want := []string{"AC-CLI-01", "AC-EVAL-01", "AC-LEX-01", "AC-PARSE-01"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Normalize(seed refs) = %v, want %v", got, want)
	}
}

func TestFindIDsExtractsFromMixedText(t *testing.T) {
	got := FindIDs("- AC-LEX-01: lexer produces positioned tokens for rq expressions")
	want := []string{"AC-LEX-01"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FindIDs(mixed) = %v, want %v", got, want)
	}
}

// Exact-token discipline: AC-1 must NEVER be conflated with AC-10 (a substring/prefix
// match would silently lower the bar). Canonical() and Normalize() keep them distinct.
func TestExactTokenDistinctness(t *testing.T) {
	if c := Canonical("AC-1"); c != "AC-1" {
		t.Fatalf("Canonical(AC-1) = %q, want AC-1", c)
	}
	if c := Canonical("AC-10"); c != "AC-10" {
		t.Fatalf("Canonical(AC-10) = %q, want AC-10", c)
	}
	got := Normalize([]string{"ac-1", "ac-10", "ac-1"})
	want := []string{"AC-1", "AC-10"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Normalize(AC-1,AC-10) = %v, want %v", got, want)
	}
}

func TestNonAcceptanceTextYieldsNothing(t *testing.T) {
	for _, s := range []string{"acceptance criteria", "macos", "a stock scaffold", "fraction", ""} {
		if got := FindIDs(s); len(got) != 0 {
			t.Fatalf("FindIDs(%q) = %v, want empty", s, got)
		}
	}
}

func TestBarePrefixNumberShapeStillMatches(t *testing.T) {
	got := FindIDs("covers FR-3 and cr-12b")
	want := []string{"CR-12B", "FR-3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FindIDs(bare) = %v, want %v", got, want)
	}
}
