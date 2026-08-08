package main

import (
	"fmt"
	"strings"
	"testing"
)

// PC-01: a decisive next-action directive must survive a flood of low-value telemetry in the
// capped advisory ring instead of being FIFO-evicted.
func TestAppendCappedAdvisorySignalKeepsDecisiveOverTelemetry(t *testing.T) {
	var signals []string
	for i := 0; i < 5; i++ {
		signals = appendCappedAdvisorySignal(signals, fmt.Sprintf("SYSTEM INBOX DRAIN: pending peer requests %d", i))
	}
	if len(signals) != advisorySignalCap {
		t.Fatalf("expected %d telemetry signals, got %d", advisorySignalCap, len(signals))
	}
	decisive := "UNCOMMITTED WORK: your next action must be project_branch_commit (push=true)."
	signals = appendCappedAdvisorySignal(signals, decisive)
	if len(signals) != advisorySignalCap {
		t.Fatalf("expected the cap to hold at %d, got %d", advisorySignalCap, len(signals))
	}
	found := false
	for _, s := range signals {
		if s == decisive {
			found = true
		}
	}
	if !found {
		t.Fatalf("decisive directive was evicted by telemetry flood; got %v", signals)
	}
}

// PC-02: a re-emitted directive must not consume multiple slots.
func TestAppendCappedAdvisorySignalDedupsWholeSlice(t *testing.T) {
	var signals []string
	d := "READY-FOR-REVIEW NOT SUBMITTED: your next action must be project_patch_queue_submit."
	signals = appendCappedAdvisorySignal(signals, d)
	signals = appendCappedAdvisorySignal(signals, "SYSTEM INBOX DRAIN: x")
	signals = appendCappedAdvisorySignal(signals, d) // re-emit interleaved
	count := 0
	for _, s := range signals {
		if s == d {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one copy of the directive after re-emit, got %d (%v)", count, signals)
	}
}

func TestAppendCappedAdvisorySignalKeepsMultipleDecisiveDirectives(t *testing.T) {
	signals := []string{
		"COMMITTED BUT NOT REVIEW-READY: call project_branch_review_ready.",
		"READY-FOR-REVIEW NOT SUBMITTED: call project_patch_queue_submit.",
	}
	for i := 0; i < 6; i++ {
		signals = appendCappedAdvisorySignal(signals, fmt.Sprintf("SYSTEM COLLABORATION FANOUT note %d", i))
	}
	if len(signals) != advisorySignalCap {
		t.Fatalf("expected cap %d, got %d", advisorySignalCap, len(signals))
	}
	for _, d := range []string{"COMMITTED BUT NOT REVIEW-READY", "NOT SUBMITTED"} {
		found := false
		for _, s := range signals {
			if strings.Contains(s, d) {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected decisive directive %q to survive the telemetry flood, got %v", d, signals)
		}
	}
}
