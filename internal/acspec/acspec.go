// Package acspec is the canonical tokenizer for external acceptance-criteria IDs
// (AC-IDs) of the operator/spec shape PREFIX-<WORD>-<NUM> (e.g. AC-LEX-01,
// AC-PARSE-01) as well as the bare PREFIX-<NUM> shape (e.g. FR-3).
//
// It is the single source of truth for the storage-side external-spec convergence
// gate (internal/storage/sqlite/project_external_spec_coverage.go), which derives the
// authoritative required-set from the operator-sealed source_requirements_trace's
// acceptance_criteria_refs list. The pattern is used ONLY as a tokenizer of that
// already-authoritative refs list; the authority for the required-set is the list,
// never a free grep over arbitrary text.
//
// NOTE on the agent module boundary: agent is a SEPARATE Go module (its own
// go.mod, no replace directive onto the rhizome root module), so it cannot import this
// package directly without coupling the lean bot binary to the root module's full
// dependency graph. agent/visual_acceptance.go therefore keeps a byte-identical
// copy of IDPattern and MUST be kept in sync with the pattern defined here.
package acspec

import (
	"regexp"
	"sort"
	"strings"
)

// IDPattern matches an acceptance-criteria ID token. Unlike the historical
// `PREFIX-\d+...` shape (which fails to match AC-LEX-01 because a letter, not a digit,
// follows the prefix dash), it matches one or more dash-separated alphanumeric
// segments after the prefix, covering both AC-<WORD>-<NUM> and PREFIX-<NUM>.
var IDPattern = regexp.MustCompile(`(?i)\b(?:ac|ev|ux|ui|qa|mvp|fr|cr)-[a-z0-9]+(?:-[a-z0-9]+)*\b`)

// FindIDs returns the canonical (upper-cased, de-duplicated, sorted) AC-ID tokens
// found anywhere in s.
func FindIDs(s string) []string {
	return Normalize(IDPattern.FindAllString(s, -1))
}

// Normalize canonicalises a list of raw values to sorted, de-duplicated, upper-cased
// AC-IDs, dropping any value (or fragment of a value) that is not AC-ID-shaped. A
// value carrying several AC-ID tokens contributes each of them; a value with none
// contributes nothing. This is what makes the refs list authoritative: a malformed or
// prose ref line simply yields no AC-ID rather than silently widening the set.
func Normalize(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		for _, token := range IDPattern.FindAllString(value, -1) {
			token = strings.ToUpper(strings.TrimSpace(token))
			if token != "" {
				seen[token] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for token := range seen {
		out = append(out, token)
	}
	sort.Strings(out)
	return out
}

// Canonical returns the canonical AC-ID form of token when token is exactly one AC-ID,
// otherwise the empty string. Used for exact-token comparison (AC-1 must never be
// treated as covering AC-10).
func Canonical(token string) string {
	ids := FindIDs(token)
	if len(ids) == 1 {
		return ids[0]
	}
	return ""
}
