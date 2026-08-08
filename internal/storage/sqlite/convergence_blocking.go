package sqlite

import "strings"

// taskIsConvergenceBlocking reports whether an OPEN project task must be resolved before the project
// may converge to phase DONE. It is the single classifier shared by the server DONE-admission gate
// (evaluateProjectPhaseTerminalAdmissionTx) and the agent idle-reflection convergence trigger
// (idleReflectionProjectConvergenceFrontierIsClear).
//
// FIXED POINT OF THE REGRESSION (R24..R65): "blocking" must be anchored on the EXTERNAL SPEC, not on
// any agent-controlled signal. Each prior anchor was an agent-controlled signal and was defeated by
// agents manufacturing blocking-shaped work against it: frontier-emptiness (R24: agents always have a
// task), then lane-label (R64: agents mint ambient lane=implementation tasks). The acceptance-criteria
// set is fixed by the operator in the sealed source_requirements_trace; agents cannot fabricate or
// erase it, so a spec anchor is the stable terminus. A task is convergence-blocking iff it is
// spec-rooted (carries an acceptance binding, is gated implementation work, or is spec-seeded);
// agent-minted proactive (ambient) work is NOT spec-rooted and does not block.
//
// This is NOT bar-lowering: the external-spec coverage gate (evaluateProjectExternalSpecCoverageTx)
// remains an untouched hard floor. If an ambient task really covered a required AC, that AC would have
// to be attested against an integrated artifact; if it isn't, coverage rejects DONE (no false DONE).
// Spec-anchoring the frontier can therefore only discard NON-spec-required work.
//
// The substantive policy is convergenceBlockingDecision, duplicated byte-for-byte into
// agent/convergence_blocking.go (separate Go module, same boundary that forced the acspec copy)
// and policed by convergence_blocking_drift_test.go.
func taskIsConvergenceBlocking(task WorkspaceTaskRecord) bool {
	// Anything the existing phase gate already skipped (proactive metacognition / idle reflection)
	// stays non-blocking - guarantees this change is strictly weaker as a blocker than the prior gate.
	if agentWorkTaskIsProactiveMetacognition(task) {
		return false
	}
	return convergenceBlockingDecision(task.Status, task.TaskID, task.ProjectLane, task.RequiresProjectGate, task.TaskRequirementsJSON, task.Tags)
}

// convergenceBlockingDecision is the SHARED convergence-blocking policy in pure-primitive form. KEEP
// BYTE-IDENTICAL with agent/convergence_blocking.go (enforced by convergence_blocking_drift_test.go).
//
// Policy (spec-anchored; blocking is the DEFAULT, discretionary requires positive proof):
//
//	(A) An EXPLICIT external-spec anchor (acceptance-criteria binding) ALWAYS blocks and wins over every
//	    signal below, including provenance. The operator fixes the AC set; agents cannot fake it away.
//	(B) Ambient / proactive PROVENANCE (task-ambient-*) is not spec-rooted -> non-blocking. This is the
//	    anti-overproduction anchor: proactively minted work cannot manufacture a spec requirement, and
//	    the coverage gate backstops any real AC it touches.
//	(C) Spec-rooted product work -> BLOCKING: gated implementation work (implementation lanes auto-force
//	    the project gate), an implementation lane, or explicit spec-seed linkage (source_doc_keys). A
//	    non-ambient implementation task is either seeded product or the lead's coverage-gap task -> must
//	    block until covered (this is the wedge-guard: an uncovered AC keeps a blocking task present).
//	(D) Positively-recognized discretionary housekeeping / reflection -> non-blocking.
//	(E) DEFAULT: anything not positively discretionary and not ambient-provenance -> BLOCKING (err
//	    toward blocking on ambiguity; the external-spec coverage gate is the independent backstop).
func convergenceBlockingDecision(status, taskID, projectLane string, requiresProjectGate bool, requirementsJSON string, tags []string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "RESOLVED", "FAILED", "CANCELLED":
		return false
	}

	id := strings.ToLower(strings.TrimSpace(taskID))
	lane := strings.ToLower(strings.TrimSpace(projectLane))
	reqs := strings.ToLower(requirementsJSON)
	hasTag := func(want ...string) bool {
		for _, raw := range tags {
			tag := strings.ToLower(strings.TrimSpace(raw))
			for _, w := range want {
				if tag == w {
					return true
				}
			}
		}
		return false
	}
	hasTagPrefix := func(prefixes ...string) bool {
		for _, raw := range tags {
			tag := strings.ToLower(strings.TrimSpace(raw))
			for _, p := range prefixes {
				if p != "" && strings.HasPrefix(tag, p) {
					return true
				}
			}
		}
		return false
	}

	// (1) SPEC-ROOTED signals -> BLOCKING. These win over provenance: agents cannot anchor-away the
	// operator spec, and the ambient PROVENANCE id is agent-forgeable (a normal task_submit can set any
	// task_id), so any task carrying a real spec signal must block regardless of its id. The signals are
	// an explicit acceptance-criteria binding, an implementation lane (a spec-rooted product lane - the
	// ambient creation-side coercion keeps genuine ambient work OFF implementation lanes, so an
	// implementation-lane task-ambient-* did NOT pass that coercion and is pre-coercion or forged), an
	// explicit project gate, or spec-seed linkage (source_doc_keys).
	if strings.Contains(reqs, "acceptance_criteria_refs") ||
		strings.Contains(reqs, "acceptance_criteria_mapping") ||
		strings.Contains(reqs, "acceptance_critical_anchors") {
		return true
	}
	switch lane {
	case "implementation", "implement", "coding", "code", "frontend", "front-end", "ui", "backend", "back-end", "api", "fullstack", "full-stack":
		return true
	}
	if requiresProjectGate {
		return true
	}
	if strings.Contains(reqs, "source_doc_keys") {
		return true
	}

	// (2) Ambient / proactive PROVENANCE on an otherwise fully-unanchored task -> non-blocking. This is
	// the anti-overproduction anchor, positioned AFTER every spec-rooted signal so it is the WEAKEST
	// signal: a forged task-ambient-* id cannot dodge blocking unless the task also carries no spec
	// signal at all, in which case it is indistinguishable from genuine proactive work and the external-
	// spec coverage gate independently backstops any real AC it touches.
	if strings.HasPrefix(id, "task-ambient-") {
		return false
	}

	// (3) Positively-recognized discretionary housekeeping / reflection -> non-blocking.
	if hasTag("idle-reflection", "meta-reflection", "anti-idle") ||
		hasTagPrefix("metacognition-scope-", "idle-policy-") {
		return false
	}
	if hasTag(
		"side-effect-classification",
		"project-claim-repair",
		"project-role-scope",
		"project-repo-repair",
		"project-repository-repair",
		"repo-repair",
		"repository-repair",
	) {
		return false
	}
	if strings.Contains(reqs, "side_effect_classification") ||
		strings.Contains(reqs, "empty_product_frontier") {
		return false
	}

	// (E) Default: not positively discretionary, not ambient-provenance -> BLOCKING.
	return true
}
