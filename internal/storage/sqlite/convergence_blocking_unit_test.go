package sqlite

import "testing"

// Unit coverage for the convergence-blocking classifier (the shared trigger/gate predicate). The
// policy: spec-bearing product work always blocks and wins on overlap; positively-recognized
// discretionary housekeeping / reflection is non-blocking; anything unrecognized defaults to blocking.
func TestTaskIsConvergenceBlocking(t *testing.T) {
	cases := []struct {
		name string
		task WorkspaceTaskRecord
		want bool
	}{
		// --- BLOCKING: spec-bearing product work ---
		{
			name: "implementation lane execution blocks",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "EXECUTION", ProjectLane: "implementation"},
			want: true,
		},
		{
			name: "requires project gate blocks regardless of lane",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "EXECUTION", ProjectLane: "qa", RequiresProjectGate: true},
			want: true,
		},
		{
			name: "acceptance criteria refs in requirements blocks",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "EXECUTION", ProjectLane: "qa", TaskRequirementsJSON: `{"acceptance_criteria_refs":["AC-CLI-01"]}`},
			want: true,
		},
		{
			name: "backend lane blocks (alias)",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "EXECUTION", ProjectLane: "backend"},
			want: true,
		},
		{
			name: "unknown coordination task defaults to blocking",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "coordination", Tags: []string{"unfamiliar-thing"}},
			want: true,
		},
		{
			name: "spec-bearing wins over discretionary tag (implementation + side-effect tag)",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "implementation", Tags: []string{"side-effect-classification"}},
			want: true,
		},
		{
			// Red-team HIGH: an ABPC side-effect RESOLUTION follow-up (verify/revert/quarantine) is NOT
			// the discretionary classification task - it can carry an unintegrated product-affecting
			// decision. It is not impl-lane, not gated, names no acceptance criterion, and carries the
			// CLASSIFICATION value nowhere -> it must default to BLOCKING (we dropped the broad
			// abpc_task_class substring so only the classification task stays discretionary).
			name: "abpc side-effect resolution follow-up (non-classification) defaults to blocking",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "verification", TaskRequirementsJSON: `{"abpc_task_class":"abpc_recovery_action","decision":"request_verification","classification_task_id":"task-x"}`},
			want: true,
		},
		{
			// FIXED-POINT (R65): genuine ambient work is coerced OFF implementation lanes; an ambient task
			// on a non-impl lane with no spec signal is proactive overproduction -> non-blocking (the
			// convergence-unblocking case; lane-label was the defeated R64 anchor).
			name: "ambient qa task with no spec signal is non-blocking (legit coerced ambient)",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskID: "task-ambient-project-rq-2ec89f651c-abc", TaskKind: "EXECUTION", ProjectLane: "qa", Tags: []string{"product-quality"}},
			want: false,
		},
		{
			// Red-team forge-guard: a forged task-ambient-* id on an IMPLEMENTATION lane did not pass the
			// ambient creation-side coercion (genuine ambient is non-impl) -> the spec-rooted impl signal
			// wins over provenance -> BLOCKING. Provenance is not a spoofable bypass for real impl work.
			name: "ambient-id implementation task blocks (forge-guard; provenance does not override impl-lane)",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskID: "task-ambient-project-rq-2ec89f651c-abc", TaskKind: "EXECUTION", ProjectLane: "implementation", RequiresProjectGate: true, Tags: []string{"backend", "cli", "implementation"}},
			want: true,
		},
		{
			// Tripwire: an explicit AC binding wins over ambient provenance (the spec anchor is decisive).
			name: "ambient task with explicit acceptance refs still blocks",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskID: "task-ambient-project-rq-abc", ProjectLane: "qa", TaskRequirementsJSON: `{"acceptance_criteria_refs":["AC-CLI-01"]}`},
			want: true,
		},
		{
			// Spec-seed linkage (source_doc_keys) is a spec anchor -> blocking.
			name: "spec-seed source_doc_keys task blocks",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskID: "task-rq-lexer", ProjectLane: "planning", TaskRequirementsJSON: `{"source_doc_keys":["operator.signal01.rq.spec.v1"]}`},
			want: true,
		},
		{
			// Wedge-guard: the lead's coverage-gap task is a NORMAL task_submit (task-*, not ambient),
			// lane=implementation (auto-gated) -> blocks until the AC is covered.
			name: "non-ambient implementation gap task blocks (wedge-guard)",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskID: "task-1781503044147428619-1302", TaskKind: "EXECUTION", ProjectLane: "implementation", RequiresProjectGate: true},
			want: true,
		},

		// --- NON-BLOCKING: discretionary housekeeping / reflection ---
		{
			name: "side-effect classification (coordination) is discretionary",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "coordination", Tags: []string{"side-effect-classification", "abpc"}},
			want: false,
		},
		{
			name: "side-effect classification via requirement schema is discretionary",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "coordination", TaskRequirementsJSON: `{"abpc_task_class":"side_effect_classification"}`},
			want: false,
		},
		{
			name: "project claim repair (strategy) is discretionary",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "strategy", Tags: []string{"project-claim-repair", "strategic-lead"}},
			want: false,
		},
		{
			name: "project role scope is discretionary",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "strategy", Tags: []string{"project-role-scope"}},
			want: false,
		},
		{
			name: "meta-reflection is discretionary",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "EXECUTION", ProjectLane: "qa", Tags: []string{"meta-reflection", "anti-idle", "product-quality"}},
			want: false,
		},
		{
			name: "empty product frontier reflection contract is discretionary",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "EXECUTION", ProjectLane: "qa", TaskRequirementsJSON: `{"runtime_contract":"empty_product_frontier.v1"}`},
			want: false,
		},

		// --- NON-BLOCKING: terminal tasks never block ---
		{
			name: "resolved implementation task is terminal, non-blocking",
			task: WorkspaceTaskRecord{Status: "RESOLVED", TaskKind: "EXECUTION", ProjectLane: "implementation"},
			want: false,
		},
		{
			name: "cancelled blocking task is terminal, non-blocking",
			task: WorkspaceTaskRecord{Status: "CANCELLED", TaskKind: "EXECUTION", ProjectLane: "implementation", RequiresProjectGate: true},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := taskIsConvergenceBlocking(tc.task); got != tc.want {
				t.Fatalf("taskIsConvergenceBlocking(%+v) = %v, want %v", tc.task, got, tc.want)
			}
		})
	}
}
