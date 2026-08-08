package main

import (
	"context"
	"strings"
	"testing"
)

func telicTestRuntime() *Runtime {
	return &Runtime{cfg: RuntimeConfig{
		TelicLoopEnabled: true,
		PinnedTaskID:     "T1",
		TelicTargetPath:  "internal/eval/eval.go",
		TelicCheckoutDir: "/tmp/checkout",
	}}
}

func TestParseTelicMeasurement(t *testing.T) {
	m := parseTelicMeasurement("noise\nTELIC_BUILD=ok TELIC_PASS=2 TELIC_COVERED=1 TELIC_TOTAL=4 TELIC_FROZEN=red\nTELIC_REDDEST=arithmetic print((3+4)) want=7\nTELIC_FROZEN_NOTE=smoke red\n")
	if !m.BuildOK || m.Pass != 2 || !m.CoveredKnown || m.Covered != 1 || m.creditedPass() != 1 || m.Total != 4 {
		t.Fatalf("parse mismatch: %+v", m)
	}
	if !m.FrozenKnown || m.FrozenGreen {
		t.Fatalf("frozen red not parsed: %+v", m)
	}
	if !strings.Contains(m.FrozenNote, "smoke") {
		t.Fatalf("frozen note not parsed: %+v", m)
	}
	if !strings.Contains(m.Reddest, "arithmetic") {
		t.Fatalf("reddest not parsed: %q", m.Reddest)
	}
	bad := parseTelicMeasurement("TELIC_BUILD=failed TELIC_PASS=0 TELIC_TOTAL=4")
	if bad.BuildOK || bad.Pass != 0 {
		t.Fatalf("build-failed parse mismatch: %+v", bad)
	}
}

// The binding R41 attractor: a pure read/describe cycle must NOT be allowed to close. The gate coerces it
// back to "continue, you must write_file now".
func TestTelicGateRefusesDescriptionOnlyCycle(t *testing.T) {
	r := telicTestRuntime()
	r.scratch.TelicBaselinePass = 0
	r.scratch.TelicLastPass = 0 // no movement
	r.scratch.TelicLastReddest = "arithmetic print((2+9)) want=11"
	res := &StructuredTaskResult{Outcome: "completed", Summary: "internal/eval is a stub; next: implement the evaluator"}
	trace := &TaskRunTrace{SuccessfulToolCalls: []string{"read_file", "list_directory", "shell"}}

	handled := r.telicGate(WorkspaceTaskRecord{TaskID: "T1"}, res, trace)
	if !handled {
		t.Fatal("telic lane must be handled by the gate")
	}
	if normalizeOutcome(res.Outcome) != "continue" {
		t.Fatalf("description-only cycle must NOT close; got outcome=%q", res.Outcome)
	}
	if !strings.Contains(res.NextAction, "write_file") {
		t.Fatalf("gate must demand a write_file edit; NextAction=%q", res.NextAction)
	}
	if !strings.Contains(res.NextAction, "arithmetic") {
		t.Fatalf("gate must carry the held reddest probe; NextAction=%q", res.NextAction)
	}
}

// Even outcome=blocked (the relabel-at-exit escape) must be caught when the agent has write scope and made
// no edit -- there is no admissible non-moving exit.
func TestTelicGateCatchesBlockedEscape(t *testing.T) {
	r := telicTestRuntime()
	r.scratch.TelicBaselinePass = 0
	r.scratch.TelicLastPass = 0
	res := &StructuredTaskResult{Outcome: "blocked", Summary: "blocked on dependency"}
	trace := &TaskRunTrace{SuccessfulToolCalls: []string{"read_file"}}
	r.telicGate(WorkspaceTaskRecord{TaskID: "T1"}, res, trace)
	if normalizeOutcome(res.Outcome) != "continue" {
		t.Fatalf("blocked escape on a no-edit cycle must be coerced to continue; got %q", res.Outcome)
	}
}

// Moved the oracle but not all probes pass: do NOT let the agent complete at 1/N -- re-target the next probe.
func TestTelicGateMovedNotConverged(t *testing.T) {
	r := telicTestRuntime()
	r.scratch.TelicBaselinePass = 0
	r.scratch.TelicLastPass = 1
	r.scratch.TelicLastTotal = 4 // 1/4: progress, not done
	res := &StructuredTaskResult{Outcome: "completed", Summary: "flipped arithmetic, done"}
	trace := &TaskRunTrace{SuccessfulToolCalls: []string{"write_file"}}
	r.telicGate(WorkspaceTaskRecord{TaskID: "T1"}, res, trace)
	if normalizeOutcome(res.Outcome) != "continue" {
		t.Fatalf("moved-but-not-converged must be coerced to continue (not completed at 1/4); got %q", res.Outcome)
	}
	if !strings.Contains(res.NextAction, "ALL probes") {
		t.Fatalf("must re-target toward full convergence; NextAction=%q", res.NextAction)
	}
}

// Fully converged (every probe passes): completion is allowed to stand.
func TestTelicGateConvergedAllowsCompletion(t *testing.T) {
	r := telicTestRuntime()
	r.scratch.TelicBaselinePass = 3
	r.scratch.TelicLastPass = 4
	r.scratch.TelicLastTotal = 4 // 4/4
	res := &StructuredTaskResult{Outcome: "completed", Summary: "all green"}
	trace := &TaskRunTrace{SuccessfulToolCalls: []string{"write_file"}}
	r.telicGate(WorkspaceTaskRecord{TaskID: "T1"}, res, trace)
	if normalizeOutcome(res.Outcome) != "completed" {
		t.Fatalf("fully converged must allow completed; got %q", res.Outcome)
	}
}

func TestTelicGateFrozenAuthorityOverridesDifferentialConvergence(t *testing.T) {
	r := telicTestRuntime()
	r.cfg.TelicRequireFrozenAuthority = true
	r.scratch.TelicBaselinePass = 3
	r.scratch.TelicLastPass = 4
	r.scratch.TelicLastTotal = 4
	r.scratch.TelicLastFrozenKnown = true
	r.scratch.TelicLastFrozenGreen = false
	res := &StructuredTaskResult{Outcome: "completed", Summary: "differential green"}
	trace := &TaskRunTrace{SuccessfulToolCalls: []string{"write_file"}}
	r.telicGate(WorkspaceTaskRecord{TaskID: "T1"}, res, trace)
	if normalizeOutcome(res.Outcome) != "continue" {
		t.Fatalf("frozen-red authority must prevent completion; got %q", res.Outcome)
	}

	r.scratch.TelicLastPass = 3
	r.scratch.TelicLastFrozenGreen = true
	res = &StructuredTaskResult{Outcome: "completed", Summary: "forged frozen green"}
	r.telicGate(WorkspaceTaskRecord{TaskID: "T1"}, res, trace)
	if normalizeOutcome(res.Outcome) != "continue" {
		t.Fatalf("frozen-green without covered convergence must prevent completion; got %q", res.Outcome)
	}

	r.scratch.TelicLastPass = 4
	r.scratch.TelicLastFrozenGreen = true
	res = &StructuredTaskResult{Outcome: "completed", Summary: "frozen green"}
	r.telicGate(WorkspaceTaskRecord{TaskID: "T1"}, res, trace)
	if normalizeOutcome(res.Outcome) != "completed" {
		t.Fatalf("frozen-green authority must allow completion; got %q", res.Outcome)
	}
}

// A real edit that did not (yet) move the oracle is normal iteration, not a parked failure: keep moving,
// distinct from the pure-read nudge.
func TestTelicGateAllowsAttemptWithoutMovement(t *testing.T) {
	r := telicTestRuntime()
	r.scratch.TelicBaselinePass = 0
	r.scratch.TelicLastPass = 0
	res := &StructuredTaskResult{Outcome: "continue", Summary: "wrote lexer scaffold"}
	trace := &TaskRunTrace{SuccessfulToolCalls: []string{"write_file"}}
	r.telicGate(WorkspaceTaskRecord{TaskID: "T1"}, res, trace)
	if normalizeOutcome(res.Outcome) != "continue" {
		t.Fatalf("attempt-without-movement should stay continue; got %q", res.Outcome)
	}
	if strings.Contains(res.NextAction, "MUST be write_file") {
		t.Fatalf("a real edit attempt should not get the read-loop nudge; NextAction=%q", res.NextAction)
	}
}

// Zero blast radius outside the experiment: disabled config or a non-pinned task -> the gate is inert.
func TestTelicGateInertOffLane(t *testing.T) {
	r := telicTestRuntime()
	res := &StructuredTaskResult{Outcome: "completed"}
	if r.telicGate(WorkspaceTaskRecord{TaskID: "OTHER"}, res, &TaskRunTrace{}) {
		t.Fatal("non-pinned task must not be handled by the telic gate")
	}
	r.cfg.TelicLoopEnabled = false
	if r.telicGate(WorkspaceTaskRecord{TaskID: "T1"}, res, &TaskRunTrace{}) {
		t.Fatal("disabled telic loop must be inert")
	}
	if normalizeOutcome(res.Outcome) != "completed" {
		t.Fatalf("inert gate must not mutate the result; got %q", res.Outcome)
	}
}

// Cold-start: when no probe passes the objective is the builder imperative, not the reviewer contract.
func TestCycleObjectiveColdStartSwap(t *testing.T) {
	r := telicTestRuntime()
	r.telicMeasureFn = func(context.Context) telicMeasurement {
		return telicMeasurement{BuildOK: false, Pass: 0, Total: 4, Reddest: "arithmetic print((2+3*4-1)) want=13"}
	}
	task := WorkspaceTaskRecord{TaskID: "T1", Title: "build eval"}
	sess := AgentSessionStateRecord{SessionID: "S1"}
	r.telicSnapshotBaseline(context.Background(), task)
	obj := r.cycleObjective(task, sess)
	if !strings.Contains(obj, "COLD START") || !strings.Contains(obj, "Write code") {
		t.Fatalf("cold-start objective expected; got:\n%s", obj)
	}
	if !strings.Contains(obj, "arithmetic") {
		t.Fatalf("cold-start objective must carry the held probe; got:\n%s", obj)
	}
	if strings.Contains(obj, "Work one useful trust-first autonomy cycle") {
		t.Fatalf("cold-start must REPLACE the standard reviewer-shaped objective")
	}

	// Fall back to the standard objective ONLY when ALL probes pass.
	r.telicMeasureFn = func(context.Context) telicMeasurement {
		return telicMeasurement{BuildOK: true, Pass: 4, Total: 4}
	}
	r.telicSnapshotBaseline(context.Background(), task)
	obj2 := r.cycleObjective(task, sess)
	if strings.Contains(obj2, "COLD START") {
		t.Fatalf("once ALL probes pass, cold-start must not fire; got:\n%s", obj2)
	}
	// A partial pass (1/4) STAYS in builder mode -- never reverts to the reviewer objective mid-build.
	r.telicMeasureFn = func(context.Context) telicMeasurement {
		return telicMeasurement{BuildOK: true, Pass: 1, Total: 4}
	}
	r.telicSnapshotBaseline(context.Background(), task)
	obj3 := r.cycleObjective(task, sess)
	if !strings.Contains(obj3, "COLD START") {
		t.Fatalf("a partial pass (1/4) must STAY in builder mode; got:\n%s", obj3)
	}
	// Frozen red keeps the builder objective even if the differential proxy claims green.
	r.telicMeasureFn = func(context.Context) telicMeasurement {
		return telicMeasurement{BuildOK: true, Pass: 4, Covered: 4, CoveredKnown: true, Total: 4, FrozenKnown: true, FrozenGreen: false}
	}
	r.telicSnapshotBaseline(context.Background(), task)
	obj4 := r.cycleObjective(task, sess)
	if !strings.Contains(obj4, "COLD START") {
		t.Fatalf("frozen-red proxy green must STAY in builder mode; got:\n%s", obj4)
	}
}

// The in-loop sensor appends the differential delta to a write result and into the advisory ring.
func TestTelicSensorAppendsDelta(t *testing.T) {
	r := telicTestRuntime()
	r.activeTask = &WorkspaceTaskRecord{TaskID: "T1"}
	r.scratch.TelicLastPass = 0
	r.telicMeasureFn = func(context.Context) telicMeasurement {
		return telicMeasurement{BuildOK: true, Pass: 3, Covered: 1, CoveredKnown: true, Total: 4, Reddest: "comparison ..."}
	}
	call := ToolCall{Function: FunctionCall{Name: "write_file"}}
	out, ok := r.telicSensor(call, ToolResult{Output: "wrote eval.go"})
	if !ok {
		t.Fatal("sensor must fire on a write in the pinned lane")
	}
	if !strings.Contains(out.Output, "DIFFERENTIAL-DELTA") || !strings.Contains(out.Output, "delta=+1") {
		t.Fatalf("sensor must append the oracle delta; got:\n%s", out.Output)
	}
	if len(r.scratch.AdvisorySignals) == 0 {
		t.Fatal("sensor must push the delta into the advisory ring")
	}
	if r.scratch.TelicLastPass != 1 {
		t.Fatalf("sensor must update last pass; got %d", r.scratch.TelicLastPass)
	}

	// Inert for a read_file (not a checkout write).
	_, ok2 := r.telicSensor(ToolCall{Function: FunctionCall{Name: "read_file"}}, ToolResult{Output: "x"})
	if ok2 {
		t.Fatal("sensor must not fire on read_file")
	}
}
