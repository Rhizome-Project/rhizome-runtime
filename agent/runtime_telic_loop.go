package main

// Telic Oracle Loop is an experimental differential-oracle harness.
//
// It closes the agent's per-iteration loop on a DIFFERENTIAL oracle so the cycle cannot be completed by
// description alone (Root A) and state-conditions the objective on deliverable maturity (Root B):
//   - cold-start objective: when no probe passes yet, replace the reviewer-shaped objective with a single
//     builder imperative carrying a concrete still-red probe as the held end-state.
//   - in-loop oracle sensor: after a committed write/commit, measure the working tree with the oracle and
//     append the build/pass delta to the tool result so it re-enters the monotonic transcript.
//   - telic exit gate: a cycle that neither moved the oracle nor wrote to the checkout is coerced back to
//     "continue, you must edit now" instead of closing as a successful epistemic report.
//
// Scope is intentionally narrow and fail-safe: every path is a no-op unless RuntimeConfig.TelicLoopEnabled
// is set AND the active task is the pinned build lane (PinnedTaskID). The L4 hunger/standing depletion
// ledger is deliberately OUT of scope for the first discriminator -- the gate alone forces the first edit
// at N=1; standing is the N>1 anti-idle layer and is only justified after this experiment moves.

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// telicMeasurement is one observation of the product tree by the differential oracle.
type telicMeasurement struct {
	BuildOK      bool   // did the product binary compile?
	Pass         int    // number of raw differential features the binary passed
	Covered      int    // number of coverage-gated features credited, when TELIC_COVERED is present
	CoveredKnown bool   // whether the oracle emitted TELIC_COVERED
	Total        int    // number of features measured
	Reddest      string // a human-readable still-red probe to hold as the next end-state
	FrozenKnown  bool   // whether the oracle emitted TELIC_FROZEN
	FrozenGreen  bool   // true only for TELIC_FROZEN=green
	FrozenNote   string // optional frozen-authority summary
	Raw          string // raw oracle stdout (for the transcript / debugging)
}

func (m telicMeasurement) creditedPass() int {
	if m.CoveredKnown {
		return m.Covered
	}
	return m.Pass
}

// telicLoopActiveForTask reports whether the telic loop governs this task. Fail-closed: disabled config
// or a non-pinned task => false, so all existing lanes are completely untouched.
func telicLoopActiveForTask(cfg RuntimeConfig, task WorkspaceTaskRecord) bool {
	if !cfg.TelicLoopEnabled {
		return false
	}
	pinned := strings.TrimSpace(cfg.PinnedTaskID)
	return pinned != "" && strings.TrimSpace(task.TaskID) == pinned
}

// telicColdStart reports whether the deliverable is still at the bootstrap stage (nothing the oracle can
// grade yet): the build fails OR zero probes pass. This is fully oracle-derived (no magic size threshold),
// so it is domain-general: "no probe passes yet -> builder mode".
// telicColdStart reports whether the deliverable is still incomplete (builder mode). It stays in builder
// mode until the build compiles AND every measured probe passes -- so the agent is never flipped back to
// the reviewer-shaped objective until the product is fully green, not merely off-zero.
func telicColdStart(baselineBuildOK bool, baselinePass, baselineTotal int, frozenKnown, frozenGreen bool) bool {
	if frozenKnown {
		return !frozenGreen
	}
	if !baselineBuildOK {
		return true
	}
	if baselineTotal <= 0 {
		return baselinePass <= 0
	}
	return baselinePass < baselineTotal
}

// telicMeasure runs the configured oracle against the checkout. It uses an injectable function field so
// tests can supply a deterministic measurement without shelling out a real build+oracle.
func (r *Runtime) telicMeasure(ctx context.Context) telicMeasurement {
	if r != nil && r.telicMeasureFn != nil {
		return r.telicMeasureFn(ctx)
	}
	return r.telicRealMeasure(ctx)
}

// telicRealMeasure executes RuntimeConfig.TelicOracleCmd in the checkout directory and parses the
// machine-readable tokens the command must print on stdout:
//
//	TELIC_BUILD=ok|failed   TELIC_PASS=<int>   TELIC_TOTAL=<int>   TELIC_REDDEST=<text up to end of line>
//
// The command is an operator-side ground-truth grader (e.g. "build glua, run the differential oracle");
// it is never agent-authored, so the relabel regress cannot reach it.
func (r *Runtime) telicRealMeasure(ctx context.Context) telicMeasurement {
	cmdline := strings.TrimSpace(r.cfg.TelicOracleCmd)
	if cmdline == "" {
		return telicMeasurement{Raw: "telic: no oracle command configured"}
	}
	runCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(runCtx, "cmd", "/C", cmdline)
	} else {
		cmd = exec.CommandContext(runCtx, "sh", "-c", cmdline)
	}
	if dir := strings.TrimSpace(r.cfg.TelicCheckoutDir); dir != "" {
		cmd.Dir = dir
	}
	out, _ := cmd.CombinedOutput()
	return parseTelicMeasurement(string(out))
}

func parseTelicMeasurement(out string) telicMeasurement {
	m := telicMeasurement{Raw: strings.TrimSpace(out)}
	for _, line := range strings.Split(out, "\n") {
		for _, tok := range strings.Fields(line) {
			switch {
			case strings.HasPrefix(tok, "TELIC_BUILD="):
				m.BuildOK = strings.EqualFold(strings.TrimPrefix(tok, "TELIC_BUILD="), "ok")
			case strings.HasPrefix(tok, "TELIC_PASS="):
				m.Pass, _ = strconv.Atoi(strings.TrimPrefix(tok, "TELIC_PASS="))
			case strings.HasPrefix(tok, "TELIC_COVERED="):
				m.Covered, _ = strconv.Atoi(strings.TrimPrefix(tok, "TELIC_COVERED="))
				m.CoveredKnown = true
			case strings.HasPrefix(tok, "TELIC_TOTAL="):
				m.Total, _ = strconv.Atoi(strings.TrimPrefix(tok, "TELIC_TOTAL="))
			case strings.HasPrefix(tok, "TELIC_FROZEN="):
				m.FrozenKnown = true
				m.FrozenGreen = strings.EqualFold(strings.TrimPrefix(tok, "TELIC_FROZEN="), "green")
			}
		}
		if idx := strings.Index(line, "TELIC_REDDEST="); idx >= 0 {
			m.Reddest = strings.TrimSpace(line[idx+len("TELIC_REDDEST="):])
		}
		if idx := strings.Index(line, "TELIC_FROZEN_NOTE="); idx >= 0 {
			m.FrozenNote = strings.TrimSpace(line[idx+len("TELIC_FROZEN_NOTE="):])
		}
	}
	return m
}

// telicSnapshotBaseline measures the tree at cycle entry and records it as the baseline the exit gate
// compares against. No-op outside the telic lane.
func (r *Runtime) telicSnapshotBaseline(ctx context.Context, task WorkspaceTaskRecord) {
	if !telicLoopActiveForTask(r.cfg, task) {
		return
	}
	m := r.telicMeasure(ctx)
	credited := m.creditedPass()
	r.mu.Lock()
	r.scratch.TelicBaselineBuildOK = m.BuildOK
	r.scratch.TelicBaselinePass = credited
	r.scratch.TelicLastBuildOK = m.BuildOK
	r.scratch.TelicLastPass = credited
	r.scratch.TelicLastReddest = m.Reddest
	r.scratch.TelicLastTotal = m.Total
	r.scratch.TelicLastFrozenKnown = m.FrozenKnown
	r.scratch.TelicLastFrozenGreen = m.FrozenGreen
	r.mu.Unlock()
}

// telicSensor is the in-loop oracle sensor. On a committed write/commit in the telic lane it re-measures
// the tree and appends a deterministic BUILD/DELTA block to the tool result, which re-enters the
// monotonic transcript next iteration. It updates the "last" snapshot the exit gate reads.
func (r *Runtime) telicSensor(call ToolCall, result ToolResult) (ToolResult, bool) {
	if r == nil || !r.cfg.TelicLoopEnabled || result.IsError {
		return result, false
	}
	name := strings.TrimSpace(call.Function.Name)
	if name != "write_file" && name != "project_branch_commit" {
		return result, false
	}
	task := r.currentTaskCopy()
	if task == nil || !telicLoopActiveForTask(r.cfg, *task) {
		return result, false
	}
	r.mu.Lock()
	prevPass, prevBuild := r.scratch.TelicLastPass, r.scratch.TelicLastBuildOK
	r.mu.Unlock()

	m := r.telicMeasure(context.Background())
	credited := m.creditedPass()
	delta := credited - prevPass
	buildImproved := m.BuildOK && !prevBuild
	passLabel := "pass"
	if m.CoveredKnown {
		passLabel = "covered"
	}
	frozenLabel := ""
	if m.FrozenKnown {
		if m.FrozenGreen {
			frozenLabel = " frozen=green"
		} else {
			frozenLabel = " frozen=red"
		}
	}
	var block string
	switch {
	case !m.BuildOK:
		block = fmt.Sprintf("DIFFERENTIAL-DELTA: build=FAILED (antigen-0). %s=%d/%d%s. The product does not compile; make it compile. Oracle: %s",
			passLabel, credited, m.Total, frozenLabel, oneLine(m.Raw))
	case delta > 0 || buildImproved:
		block = fmt.Sprintf("DIFFERENTIAL-DELTA: build=ok %s %d->%d (delta=+%d)%s. Real oracle movement. Reddest still-red: %s",
			passLabel, prevPass, credited, delta, frozenLabel, firstNonEmpty(m.Reddest, "(none -- all measured features pass)"))
	default:
		block = fmt.Sprintf("DIFFERENTIAL-DELTA: build=ok %s=%d/%d%s (no change). This edit did not move the oracle. Reddest still-red: %s. Keep editing toward it; a lookup-table or constant print fails the randomized siblings.",
			passLabel, credited, m.Total, frozenLabel, firstNonEmpty(m.Reddest, "(none)"))
	}
	result.Output = strings.TrimRight(result.Output, "\r\n") + "\n" + block

	r.mu.Lock()
	r.scratch.TelicLastPass = credited
	r.scratch.TelicLastBuildOK = m.BuildOK
	r.scratch.TelicLastReddest = m.Reddest
	r.scratch.TelicLastFrozenKnown = m.FrozenKnown
	r.scratch.TelicLastFrozenGreen = m.FrozenGreen
	if m.Total > 0 {
		r.scratch.TelicLastTotal = m.Total
	}
	r.appendAdvisorySignalLocked(block)
	r.mu.Unlock()
	return result, true
}

// appendAdvisorySignalLocked pushes a signal into the 5-slot advisory ring (caller holds r.mu).
func (r *Runtime) appendAdvisorySignalLocked(sig string) {
	if len(r.scratch.AdvisorySignals) == 0 || r.scratch.AdvisorySignals[len(r.scratch.AdvisorySignals)-1] != sig {
		r.scratch.AdvisorySignals = append(r.scratch.AdvisorySignals, sig)
		if len(r.scratch.AdvisorySignals) > 5 {
			r.scratch.AdvisorySignals = r.scratch.AdvisorySignals[len(r.scratch.AdvisorySignals)-5:]
		}
	}
}

// telicGate is the telic exit. It runs at cycle end before the generic noop guard and, for the telic
// lane, takes full ownership of the outcome (returns true). A cycle that neither moved the oracle nor
// wrote to the checkout is coerced from a successful epistemic close back to "continue, edit now".
// Returns false for non-telic lanes so the generic guard runs unchanged.
func (r *Runtime) telicGate(task WorkspaceTaskRecord, result *StructuredTaskResult, trace *TaskRunTrace) bool {
	if result == nil || !telicLoopActiveForTask(r.cfg, task) {
		return false
	}
	r.mu.Lock()
	basePass, baseBuild := r.scratch.TelicBaselinePass, r.scratch.TelicBaselineBuildOK
	lastPass, lastBuild := r.scratch.TelicLastPass, r.scratch.TelicLastBuildOK
	lastTotal := r.scratch.TelicLastTotal
	reddest := r.scratch.TelicLastReddest
	frozenKnown := r.scratch.TelicLastFrozenKnown
	frozenGreen := r.scratch.TelicLastFrozenGreen
	r.mu.Unlock()

	movedOracle := lastPass > basePass || (lastBuild && !baseBuild)
	wroteCheckout := traceHasTelicCheckoutWrite(trace)
	converged := lastTotal > 0 && lastPass >= lastTotal
	if r.cfg.TelicRequireFrozenAuthority {
		converged = frozenKnown && frozenGreen && converged
	} else if frozenKnown {
		converged = frozenGreen
	}

	if converged {
		return true
	}

	if movedOracle {
		// Moved the oracle but NOT all probes pass yet. Credit the progress, but do NOT let the agent
		// declare the task complete on a single probe -- re-target the next reddest and keep building.
		// (Without this, the agent flips one probe, returns outcome=completed, and the task RESOLVES at 1/N.)
		result.Outcome = "continue"
		result.Summary = fmt.Sprintf("Oracle moved to %d/%d; not yet converged, continuing", lastPass, lastTotal)
		result.Details = appendAutonomyNote(result.Details, "Telic exit gate: the differential oracle rose but not every probe passes. The task is not complete until all probes are green.")
		result.NextAction = fmt.Sprintf("Oracle pass count rose to %d/%d -- real progress. The task is NOT done until ALL probes pass. Target the next reddest still-red probe and keep editing: %s",
			lastPass, lastTotal, firstNonEmpty(reddest, "(the next still-red probe)"))
		result.RequiresHuman = false
		result.OwnerAction = ""
		result.HumanReason = ""
		result.DecisionType = ""
		result.BlockedOn = nil
		return true
	}
	if wroteCheckout {
		// A genuine attempt that did not (yet) move the oracle (e.g. wrote scaffolding that does not pass).
		// This is normal iteration toward the goal -- keep moving, do not park, but keep the gradient visible.
		result.Outcome = "continue"
		result.NextAction = "Your last edit did not move the differential oracle yet. Continue implementing toward the reddest still-red probe; only an oracle re-measure showing a higher pass count completes this work."
		return true
	}
	// Pure read/describe cycle: the binding R41 attractor. Refuse to let description close the cycle.
	result.Outcome = "continue"
	result.Summary = "No oracle movement and no checkout edit this cycle"
	result.Details = appendAutonomyNote(result.Details, "Telic exit gate: a faithful description of an unbuilt product is NOT a completed cycle. The differential oracle is unchanged and no write touched the checkout. Inspection does not count toward this deliverable.")
	result.NextAction = fmt.Sprintf("Your next action MUST be write_file on the deliverable at %s. Make a bounded first edit toward: %s. Do not review, inspect provenance, or report status -- write code now.",
		firstNonEmpty(strings.TrimSpace(r.cfg.TelicTargetPath), "the target file"),
		firstNonEmpty(reddest, "the reddest still-red probe"))
	result.RequiresHuman = false
	result.OwnerAction = ""
	result.HumanReason = ""
	result.DecisionType = ""
	result.BlockedOn = nil
	return true
}

// traceHasTelicCheckoutWrite reports whether this cycle made a durable edit to the product checkout.
func traceHasTelicCheckoutWrite(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	for _, toolName := range trace.SuccessfulToolCalls {
		switch strings.ToLower(strings.TrimSpace(toolName)) {
		case "write_file", "project_branch_commit":
			return true
		}
	}
	return false
}

// cycleObjective selects the objective for this cycle: the cold-start builder imperative when the telic
// lane is at the bootstrap stage, otherwise the standard objective. telicSnapshotBaseline must have run
// first (it populates the baseline snapshot this reads).
func (r *Runtime) cycleObjective(task WorkspaceTaskRecord, session AgentSessionStateRecord) string {
	if !telicLoopActiveForTask(r.cfg, task) {
		return buildTaskCycleObjective(r.cfg, task, session)
	}
	r.mu.Lock()
	m := telicMeasurement{
		BuildOK:     r.scratch.TelicBaselineBuildOK,
		Pass:        r.scratch.TelicBaselinePass,
		Total:       r.scratch.TelicLastTotal,
		Reddest:     r.scratch.TelicLastReddest,
		FrozenKnown: r.scratch.TelicLastFrozenKnown,
		FrozenGreen: r.scratch.TelicLastFrozenGreen,
	}
	r.mu.Unlock()
	if telicColdStart(m.BuildOK, m.Pass, m.Total, m.FrozenKnown, m.FrozenGreen) {
		return telicColdStartObjective(r.cfg, task, session, m)
	}
	return buildTaskCycleObjective(r.cfg, task, session)
}

// telicColdStartObjective builds the builder-only objective for the cold-start stage, replacing the
// reviewer-shaped contract with a single imperative carrying a concrete still-red probe as the held goal.
func telicColdStartObjective(cfg RuntimeConfig, task WorkspaceTaskRecord, session AgentSessionStateRecord, m telicMeasurement) string {
	var b strings.Builder
	target := firstNonEmpty(strings.TrimSpace(cfg.TelicTargetPath), "the deliverable file")
	b.WriteString("COLD START: the deliverable does not work yet (no differential probe passes). Your ONLY job this cycle is to make it exist and move toward passing one probe.\n")
	b.WriteString("This is a build cycle, not a review cycle. Do NOT review, inspect provenance, audit, gate, or report status. Write code.\n\n")
	b.WriteString(fmt.Sprintf("task_id=%s\n", task.TaskID))
	b.WriteString(fmt.Sprintf("session_id=%s\n", session.SessionID))
	if strings.TrimSpace(task.Title) != "" {
		b.WriteString(fmt.Sprintf("title=%s\n", strings.TrimSpace(task.Title)))
	}
	b.WriteString(fmt.Sprintf("target_file=%s\n", target))
	if checkout := strings.TrimSpace(cfg.TelicCheckoutDir); checkout != "" {
		b.WriteString(fmt.Sprintf("checkout_dir=%s\n", checkout))
	}
	if !m.BuildOK {
		b.WriteString("oracle: the product does not compile yet (antigen-0). First make it build; then make one probe pass.\n")
	} else {
		b.WriteString(fmt.Sprintf("oracle: build=ok, %d/%d probes pass.\n", m.Pass, m.Total))
	}
	if strings.TrimSpace(m.Reddest) != "" {
		b.WriteString("HELD GOAL (reddest still-red probe): " + strings.TrimSpace(m.Reddest) + "\n")
	}
	b.WriteString("\nYou may edit the target file AND any other source files in the checkout needed to make the product COMPILE and pass the probe (e.g. the caller/interface that references your code). When the build fails, the oracle feedback (DIFFERENTIAL-DELTA / BUILD_ERROR) names the exact compiler error -- fix exactly that, in whatever file it points to, before reaching for the probe.\n")
	b.WriteString("Progress requires a committed write to the checkout that makes the differential oracle re-measure a higher pass count; a status report or plan is NOT progress. After each probe goes green, target the NEXT reddest still-red probe. Do NOT declare the task completed until EVERY probe passes -- one passing probe is not done. The next action you take should be a write_file edit toward the held goal.\n\n")
	b.WriteString(strings.TrimSpace(runtimeStructuredTaskResultContract(cfg)))
	return b.String()
}
