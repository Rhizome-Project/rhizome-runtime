package main

import (
	"errors"
	"strings"
	"testing"
)

// TestBuildParityRunLedgerDriftTolerance locks FF-R09-1: doc-only run-ledger commits ahead of
// the deployed revision are NOT build drift (the R09 auto-advance killer), while any
// build-relevant path, classification error, or unknown ancestry stays fail-closed.
func TestBuildParityRunLedgerDriftTolerance(t *testing.T) {
	old := managerBuildParityDiffFunc
	t.Cleanup(func() { managerBuildParityDiffFunc = old })

	cases := []struct {
		name   string
		paths  []string
		err    error
		ok     bool
		detail string
	}{
		{
			name:  "ledger only",
			paths: []string{"runs/signal01-rq-s1/postmortems/round-09.md", "plans/01-token-economy-plan-2026-06-11.md", "runs/signal01-rq-s1/README.md"},
			ok:    true,
		},
		{
			name:  "markdown and gitignore",
			paths: []string{"README.md", ".gitignore", "docs/notes.md"},
			ok:    true,
		},
		{
			name:   "go code is drift",
			paths:  []string{"runs/x.md", "internal/storage/sqlite/agent_work.go"},
			ok:     false,
			detail: "internal/storage/sqlite/agent_work.go",
		},
		{
			name:   "submodule pointer is drift",
			paths:  []string{"agent"},
			ok:     false,
			detail: "agent",
		},
		{
			name:   "scripts are drift",
			paths:  []string{"scripts/run/rhizome-run.ps1"},
			ok:     false,
			detail: "scripts/run/rhizome-run.ps1",
		},
		{
			name:   "classification error fails closed",
			err:    errors.New("remote revision deadbeef is not a known ancestor"),
			ok:     false,
			detail: "drift classification failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			managerBuildParityDiffFunc = func(remoteRev, localRev string) ([]string, error) {
				return tc.paths, tc.err
			}
			ok, detail := managerBuildParityDriftIsRunLedgerOnly("aaaa", "bbbb")
			if ok != tc.ok {
				t.Fatalf("ok=%v want %v (detail %q)", ok, tc.ok, detail)
			}
			if tc.detail != "" && !strings.Contains(detail, tc.detail) {
				t.Fatalf("detail %q does not contain %q", detail, tc.detail)
			}
		})
	}
}
