package sqlite

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

// convergenceBlockingDecision has a second, byte-identical copy in agent/convergence_blocking.go
// because agent is a SEPARATE Go module and cannot import internal/storage/sqlite across the
// module boundary (same constraint that forced the internal/acspec AC-ID regex copy). If the two
// copies drift, the agent's convergence trigger and the server's DONE gate would disagree about which
// tasks are blocking - the agent could fire the convergence protocol on a frontier the gate still
// rejects (livelock), or stay silent on a frontier the gate would admit (no convergence). This
// import-free guard extracts the function source from both files as text and fails the build if they
// diverge, permanently closing that "two sources of truth" mine. It does NOT link the modules - it
// only reads the files as text, so a root-module test can police a copy that lives in another module.
func TestConvergenceBlockingDecisionCopiesDoNotDrift(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve test file path")
	}
	dir := filepath.Dir(thisFile)
	canonical := extractConvergenceBlockingDecision(t, filepath.Join(dir, "convergence_blocking.go"))
	copyBody := extractConvergenceBlockingDecision(t, filepath.Join(dir, "..", "..", "..", "agent", "convergence_blocking.go"))
	if canonical != copyBody {
		t.Fatalf("convergenceBlockingDecision drift between modules:\n  internal/storage/sqlite/convergence_blocking.go and agent/convergence_blocking.go differ.\nKeep the function body byte-identical (agent cannot import internal/storage/sqlite across the module boundary).\n\n--- internal/storage/sqlite ---\n%s\n--- agent ---\n%s", canonical, copyBody)
	}
}

var convergenceBlockingDecisionFuncRE = regexp.MustCompile(`(?s)func convergenceBlockingDecision\(.*?\n\}\n`)

func extractConvergenceBlockingDecision(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	m := convergenceBlockingDecisionFuncRE.Find(data)
	if m == nil {
		t.Fatalf("could not find func convergenceBlockingDecision(...) {...} in %s", path)
	}
	return string(m)
}
