package main

import "testing"

// agent half of the convergence-blocking classifier. Must agree with the server copy (the body
// is drift-guarded byte-for-byte); this test pins the agent-visible behavior directly.
func TestTaskIsConvergenceBlockingAgentRuntime(t *testing.T) {
	cases := []struct {
		name string
		task WorkspaceTaskRecord
		want bool
	}{
		{
			name: "implementation lane blocks",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "EXECUTION", ProjectLane: "implementation"},
			want: true,
		},
		{
			name: "requires project gate blocks",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "EXECUTION", ProjectLane: "qa", RequiresProjectGate: boolPtr(true)},
			want: true,
		},
		{
			name: "acceptance criteria refs block",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "EXECUTION", ProjectLane: "qa", TaskRequirementsJSON: `{"acceptance_criteria_refs":["AC-CLI-01"]}`},
			want: true,
		},
		{
			name: "unknown coordination defaults to blocking",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "coordination", Tags: []string{"mystery"}},
			want: true,
		},
		{
			name: "spec-bearing wins over discretionary tag",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "implementation", Tags: []string{"side-effect-classification"}},
			want: true,
		},
		{
			name: "abpc side-effect resolution follow-up (non-classification) defaults to blocking",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "verification", TaskRequirementsJSON: `{"abpc_task_class":"abpc_recovery_action","decision":"request_verification"}`},
			want: true,
		},
		{
			name: "ambient qa task with no spec signal is non-blocking (legit coerced ambient)",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskID: "task-ambient-project-rq-2ec89f651c-abc", TaskKind: "EXECUTION", ProjectLane: "qa", Tags: []string{"product-quality"}},
			want: false,
		},
		{
			name: "ambient-id implementation task blocks (forge-guard; provenance does not override impl-lane)",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskID: "task-ambient-project-rq-2ec89f651c-abc", TaskKind: "EXECUTION", ProjectLane: "implementation", RequiresProjectGate: boolPtr(true), Tags: []string{"backend", "cli", "implementation"}},
			want: true,
		},
		{
			name: "ambient task with explicit acceptance refs still blocks",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskID: "task-ambient-project-rq-abc", ProjectLane: "qa", TaskRequirementsJSON: `{"acceptance_criteria_refs":["AC-CLI-01"]}`},
			want: true,
		},
		{
			name: "spec-seed source_doc_keys task blocks",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskID: "task-rq-lexer", ProjectLane: "planning", TaskRequirementsJSON: `{"source_doc_keys":["operator.signal01.rq.spec.v1"]}`},
			want: true,
		},
		{
			name: "non-ambient implementation gap task blocks (wedge-guard)",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskID: "task-1781503044147428619-1302", TaskKind: "EXECUTION", ProjectLane: "implementation", RequiresProjectGate: boolPtr(true)},
			want: true,
		},
		{
			name: "side-effect classification is discretionary",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "coordination", Tags: []string{"side-effect-classification", "abpc"}},
			want: false,
		},
		{
			name: "claim repair is discretionary",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "strategy", Tags: []string{"project-claim-repair"}},
			want: false,
		},
		{
			name: "meta-reflection is discretionary",
			task: WorkspaceTaskRecord{Status: "PENDING", TaskKind: "EXECUTION", ProjectLane: "qa", Tags: []string{"meta-reflection", "anti-idle"}},
			want: false,
		},
		{
			name: "resolved task is terminal, non-blocking",
			task: WorkspaceTaskRecord{Status: "RESOLVED", TaskKind: "EXECUTION", ProjectLane: "implementation"},
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
