package main

import (
	"strings"
	"testing"
	"time"
)

// withUpdatedAt stamps a deterministic UpdatedAt so eviction tie-breaks are testable.
func reflectionAt(state *RuntimeScratchState, idx int, ts string) {
	state.ReflectionSignals[idx].UpdatedAt = ts
}

func TestAppendReflectionSignalDedupeBumpsCount(t *testing.T) {
	state := &RuntimeScratchState{}
	appendReflectionSignal(state, ReflectionSignal{Source: "system_sensor", Kind: "observation", Text: "queue is shallow"}, 5)
	appendReflectionSignal(state, ReflectionSignal{Source: "system_sensor", Kind: "observation", Text: "Queue Is Shallow"}, 5)
	if len(state.ReflectionSignals) != 1 {
		t.Fatalf("dedupe-by-(source,text) should keep one entry, got %d", len(state.ReflectionSignals))
	}
	if got := state.ReflectionSignals[0].Count; got != 2 {
		t.Fatalf("dedupe hit must bump Count to 2, got %d", got)
	}
}

func TestAppendReflectionSignalDistinctSourceNotDeduped(t *testing.T) {
	state := &RuntimeScratchState{}
	appendReflectionSignal(state, ReflectionSignal{Source: "system_sensor", Text: "same text"}, 5)
	appendReflectionSignal(state, ReflectionSignal{Source: "strategy_synth", Text: "same text"}, 5)
	if len(state.ReflectionSignals) != 2 {
		t.Fatalf("same text from distinct sources must not dedupe, got %d", len(state.ReflectionSignals))
	}
}

func TestAppendReflectionSignalDedupeRaisesPriority(t *testing.T) {
	state := &RuntimeScratchState{}
	appendReflectionSignal(state, ReflectionSignal{Source: "s", Kind: "observation", Text: "watch this"}, 5)
	appendReflectionSignal(state, ReflectionSignal{Source: "s", Kind: "risk", Text: "watch this"}, 5)
	sig := state.ReflectionSignals[0]
	if sig.Kind != "risk" || sig.Priority != reflectionKindPriority("risk") {
		t.Fatalf("a higher-priority dedupe hit must promote kind/priority to risk, got kind=%q pri=%d", sig.Kind, sig.Priority)
	}
}

func TestAppendReflectionSignalRiskNeverEvictedByObservation(t *testing.T) {
	state := &RuntimeScratchState{}
	cap := 2
	appendReflectionSignal(state, ReflectionSignal{Source: "s", Kind: "risk", Text: "critical risk"}, cap)
	appendReflectionSignal(state, ReflectionSignal{Source: "s", Kind: "observation", Text: "obs one"}, cap)
	// Third entry overflows; the lowest-priority observation must be evicted, not the risk.
	appendReflectionSignal(state, ReflectionSignal{Source: "s", Kind: "observation", Text: "obs two"}, cap)
	if len(state.ReflectionSignals) != cap {
		t.Fatalf("buffer must respect cap=%d, got %d", cap, len(state.ReflectionSignals))
	}
	foundRisk := false
	for _, sig := range state.ReflectionSignals {
		if sig.Kind == "risk" {
			foundRisk = true
		}
	}
	if !foundRisk {
		t.Fatalf("a risk entry must survive eviction by routine observations: %+v", state.ReflectionSignals)
	}
}

func TestAppendReflectionSignalEvictsOldestOnPriorityTie(t *testing.T) {
	state := &RuntimeScratchState{}
	cap := 2
	appendReflectionSignal(state, ReflectionSignal{Source: "s", Kind: "observation", Text: "old obs"}, cap)
	reflectionAt(state, 0, "2020-01-01T00:00:00Z")
	appendReflectionSignal(state, ReflectionSignal{Source: "s", Kind: "observation", Text: "newer obs"}, cap)
	reflectionAt(state, 1, "2020-06-01T00:00:00Z")
	appendReflectionSignal(state, ReflectionSignal{Source: "s", Kind: "observation", Text: "newest obs"}, cap)
	for _, sig := range state.ReflectionSignals {
		if sig.Text == "old obs" {
			t.Fatalf("on priority tie the oldest UpdatedAt must be evicted, but 'old obs' survived: %+v", state.ReflectionSignals)
		}
	}
}

func TestAppendReflectionSignalCapZeroIsNoOp(t *testing.T) {
	state := &RuntimeScratchState{}
	appendReflectionSignal(state, ReflectionSignal{Source: "s", Text: "ignored"}, 0)
	if len(state.ReflectionSignals) != 0 {
		t.Fatalf("cap<=0 (disabled channel) must be a no-op so the caller can fall back to the advisory ring, got %d", len(state.ReflectionSignals))
	}
}

func TestHeartbeatKindUsesReflectionChannel(t *testing.T) {
	for _, kind := range []string{"metacognition", "system_sensing", "strategy_synthesis", "Metacognition", "SYSTEM_SENSING"} {
		if !heartbeatKindUsesReflectionChannel(kind) {
			t.Fatalf("kind %q must route to the reflection channel", kind)
		}
	}
	for _, kind := range []string{"task_execution", "budget_watch", "collaboration", "", "portfolio_scout"} {
		if heartbeatKindUsesReflectionChannel(kind) {
			t.Fatalf("non-reflection kind %q must NOT route to the reflection channel (stays in advisory ring)", kind)
		}
	}
}

func TestClassifyReflectionKindInfersPriorityClass(t *testing.T) {
	cases := map[string]string{
		"there is a risk of starvation here": "risk",
		"do not retry blindly":               "risk",
		"always close the receipt first":     "rule_candidate",
		"we should prefer batching":          "opportunity",
		"queue depth is currently three":     "observation",
	}
	for text, want := range cases {
		if got := classifyReflectionKind(text); got != want {
			t.Fatalf("classifyReflectionKind(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestWriteReflectionChannelSectionRendersDistinctSection(t *testing.T) {
	var b strings.Builder
	signals := []ReflectionSignal{
		{Source: "system_sensor", Kind: "risk", Text: "queue starving", Count: 3},
		{Source: "strategy_synth", Kind: "opportunity", Text: "batch the QA tasks", Count: 1},
	}
	writeReflectionChannelSection(&b, signals)
	out := b.String()
	if !strings.Contains(out, "## Reflection Channel (self-authored insight)") {
		t.Fatalf("reflection section header missing:\n%s", out)
	}
	if !strings.Contains(out, "[risk] system_sensor: queue starving (x3)") {
		t.Fatalf("risk line with count missing:\n%s", out)
	}
	if !strings.Contains(out, "[opportunity] strategy_synth: batch the QA tasks") {
		t.Fatalf("opportunity line missing:\n%s", out)
	}
}

func TestWriteReflectionChannelSectionEmptyIsAbsent(t *testing.T) {
	var b strings.Builder
	writeReflectionChannelSection(&b, nil)
	if b.Len() != 0 {
		t.Fatalf("empty reflection channel must render nothing (byte-identical when disabled), got:\n%s", b.String())
	}
}

func TestNormalizeReflectionSignalsTrimsAndDefaults(t *testing.T) {
	in := []ReflectionSignal{
		{Source: "  s  ", Kind: "", Text: "  keep me  "},
		{Source: "s", Text: "   "}, // blank text dropped
		{Source: "s", Kind: "risk", Text: "danger", Count: 0, Priority: 0},
	}
	out := normalizeReflectionSignals(in)
	if len(out) != 2 {
		t.Fatalf("blank-text entries must be dropped, got %d", len(out))
	}
	if out[0].Source != "s" || out[0].Text != "keep me" {
		t.Fatalf("source/text must be trimmed, got %+v", out[0])
	}
	if out[0].Kind != "observation" || out[0].Count != 1 {
		t.Fatalf("kind must default to observation and Count to 1, got %+v", out[0])
	}
	if out[1].Priority != reflectionKindPriority("risk") {
		t.Fatalf("priority must derive from kind, got %d", out[1].Priority)
	}
}

func TestReflectionSignalsRoundTripStable(t *testing.T) {
	// normalizeReflectionSignals is applied on both load and persist, so a normalized
	// slice must be a fixed point (no churn across save/load cycles).
	now := time.Now().UTC().Format(time.RFC3339)
	in := []ReflectionSignal{{Source: "s", Kind: "risk", Text: "x", Priority: 4, Count: 2, CreatedAt: now, UpdatedAt: now}}
	first := normalizeReflectionSignals(in)
	second := normalizeReflectionSignals(first)
	if len(first) != len(second) || first[0] != second[0] {
		t.Fatalf("normalization must be idempotent, got %+v then %+v", first, second)
	}
}
