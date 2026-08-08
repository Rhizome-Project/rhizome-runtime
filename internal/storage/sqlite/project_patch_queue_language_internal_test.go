package sqlite

import "testing"

func TestProjectPatchQueueEnglishEvidenceMatchersRespectNegation(t *testing.T) {
	positive := "browser smoke passed; no required evidence is missing; the candidate is not currently blocked"
	if projectPatchQueueSupersessionEvidenceRejectsProgress(positive) {
		t.Fatalf("positive evidence must not be rejected: %q", positive)
	}

	for _, negated := range []string{
		"browser check: no validation passed on this head",
		"browser: no unit tests passed on this head",
		"browser check: this does not mean validation passed",
		"browser validation passed: false",
		"browser: validation passed? no",
		"browser: validation passed is false",
		"browser: zero unit tests passed",
		"browser: none of the unit tests passed",
		"browser: 0 unit tests passed",
		"browser: unit tests passed, but that claim is false",
		"browser: unit tests passed = false",
		"browser: unit tests passed: 1/100 successful",
		"browser: 1/100 unit tests passed",
		"browser: unit tests passed: 1 of 100 successful",
	} {
		if projectPatchQueueSupersessionEvidenceHasPositiveValidation(negated) {
			t.Fatalf("negated verification must not count as positive validation: %q", negated)
		}
	}

	for _, affirmative := range []string{
		"browser: no failures were observed and unit tests passed",
		"browser: no failures, unit tests passed",
		"browser: not only unit tests passed",
		"browser: no failures — unit tests passed",
		"browser: unit tests passed with 0 failures",
		"browser: unit tests passed, none failed",
		"browser: unit tests passed, no tests failed",
		"browser: unit tests passed? yes",
	} {
		if !projectPatchQueueSupersessionEvidenceHasPositiveValidation(affirmative) {
			t.Fatalf("an unrelated earlier negation must not negate later positive validation: %q", affirmative)
		}
	}
}
