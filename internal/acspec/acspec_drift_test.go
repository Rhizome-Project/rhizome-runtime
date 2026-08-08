package acspec

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

// The AC-ID tokenizer has a second, byte-identical copy in agent/visual_acceptance.go because
// agent is a SEPARATE Go module and cannot import internal/acspec without coupling the lean
// bot binary to the root module's dependency graph. This import-free guard extracts both raw
// regexp literals straight from source and fails if they drift, permanently closing the
// "two sources of truth" mine. It does NOT link the modules - it only reads the files as text, so a
// root-module test can police a copy that lives in a different module.
//
// The convergence DONE gate imports internal/acspec directly and does NOT depend on the copy, so a
// drift would only ever affect the visual-acceptance feature; this guard keeps even that closed.
func TestACIDPatternCopiesDoNotDrift(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve test file path")
	}
	dir := filepath.Dir(thisFile)
	canonical := extractMustCompileLiteral(t, filepath.Join(dir, "acspec.go"), "IDPattern")
	copyLit := extractMustCompileLiteral(t, filepath.Join(dir, "..", "..", "agent", "visual_acceptance.go"), "visualAcceptanceCriteriaIDPattern")
	if canonical != copyLit {
		t.Fatalf("AC-ID pattern drift between modules:\n  internal/acspec.IDPattern                     = %q\n  agent.visualAcceptanceCriteriaIDPattern  = %q\nkeep them byte-identical (agent cannot import internal/acspec across the module boundary)", canonical, copyLit)
	}
}

func extractMustCompileLiteral(t *testing.T, path, varName string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	re := regexp.MustCompile(regexp.QuoteMeta(varName) + "\\s*=\\s*regexp\\.MustCompile\\(`([^`]*)`")
	m := re.FindSubmatch(data)
	if m == nil {
		t.Fatalf("could not find %s = regexp.MustCompile(`...`) in %s", varName, path)
	}
	return string(m[1])
}
