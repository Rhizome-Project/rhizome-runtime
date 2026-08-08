package main

import (
	"strings"
	"testing"
)

func TestNormalizeRuntimeAdvisorySignalsReplacesLegacyShellRestrictionGuidance(t *testing.T) {
	old := "SYSTEM TOOL GUIDANCE: shell failed. Do not retry shell blindly in this cycle. Use list_directory/read_file for inspection and write_file for local artifact creation; if execution is impossible, materialize a blocker or tension instead."

	got := normalizeRuntimeAdvisorySignals([]string{old})
	if len(got) != 1 {
		t.Fatalf("expected one normalized signal, got %+v", got)
	}
	if !strings.Contains(got[0], "Shell is trusted local execution") || !strings.Contains(got[0], "retry when useful") {
		t.Fatalf("expected trusted shell retry guidance, got %q", got[0])
	}
	if strings.Contains(got[0], "Do not retry shell blindly") || strings.Contains(got[0], "Corrective retry is allowed") {
		t.Fatalf("expected legacy shell restriction guidance to be removed, got %q", got[0])
	}

	again := normalizeRuntimeAdvisorySignals(got)
	if len(again) != 1 || again[0] != got[0] {
		t.Fatalf("expected normalization to be idempotent, got %+v then %+v", got, again)
	}
}
