package main

import "testing"

// R68 keystone (agent half): a patch-queue revision followup for a REJECTED candidate whose every pathset
// path is covered by an INTEGRATED sibling is redundant over-production and must NOT keep the convergence
// frontier open. Scope is read from the item's own pathset (not the status-filtered branch list). The
// tripwire errs toward BLOCKING on every unestablished input.

func r68RevisionTask() WorkspaceTaskRecord {
	return WorkspaceTaskRecord{
		TaskID:      "task-patchq-revision-project-rq-item-16139",
		ProjectID:   "project-rq",
		Status:      "PENDING",
		TaskKind:    "EXECUTION",
		ProjectLane: "implementation",
		Title:       "Revise rejected integration candidate projbranch-16139",
		Description: "state: rejected",
		Tags:        []string{"queue:queue-rq", "item:item-16139", "branch:branch-16139"},
	}
}

// r68Items builds the candidate (state + pathset) plus two broad INTEGRATED siblings, with scope carried
// on the ITEM pathset_json (a bare JSON array, the real shape).
func r68Items(candidateState, candidatePathsetJSON string) []ProjectPatchQueueItemRecord {
	return []ProjectPatchQueueItemRecord{
		{QueueID: "queue-rq", ItemID: "item-16139", BranchID: "branch-16139", State: candidateState, PathsetJSON: candidatePathsetJSON},
		{QueueID: "queue-rq", ItemID: "item-cli", BranchID: "branch-cli", State: "INTEGRATED", PathsetJSON: `["cmd/**","internal/cli/**","internal/repl/**"]`},
		{QueueID: "queue-rq", ItemID: "item-eval", BranchID: "branch-eval", State: "INTEGRATED", PathsetJSON: `["internal/evaluator/**"]`},
	}
}

func TestIdleReflectionTaskIsRedundantRejectedRevision(t *testing.T) {
	const fullyCovered = `["cmd/rq/main.go","internal/evaluator/evaluator.go"]`
	const partlyUncovered = `["cmd/rq/main.go","internal/newfeature/feature.go"]`

	cases := []struct {
		name  string
		task  WorkspaceTaskRecord
		items []ProjectPatchQueueItemRecord
		want  bool
	}{
		{
			name:  "rejected revision, every path covered by integrated sibling -> redundant",
			task:  r68RevisionTask(),
			items: r68Items("REJECTED", fullyCovered),
			want:  true,
		},
		{
			name:  "rejected revision but one path is NOT integrated -> keep blocking (tripwire)",
			task:  r68RevisionTask(),
			items: r68Items("REJECTED", partlyUncovered),
			want:  false,
		},
		{
			name:  "referenced item is INTEGRATED (not rejected) -> keep blocking",
			task:  r68RevisionTask(),
			items: r68Items("INTEGRATED", fullyCovered),
			want:  false,
		},
		{
			name:  "referenced item is still in-flight (PROPOSED) -> keep blocking",
			task:  r68RevisionTask(),
			items: r68Items("PROPOSED", fullyCovered),
			want:  false,
		},
		{
			name: "not a revision followup (plain implementation task) -> keep blocking",
			task: WorkspaceTaskRecord{
				TaskID:      "task-impl-cli",
				ProjectID:   "project-rq",
				Status:      "PENDING",
				TaskKind:    "EXECUTION",
				ProjectLane: "implementation",
				Title:       "Implement CLI entrypoint",
			},
			items: r68Items("REJECTED", fullyCovered),
			want:  false,
		},
		{
			name: "broad candidate dir NOT covered by a narrower integrated file beneath it -> keep blocking (directional)",
			task: r68RevisionTask(),
			items: []ProjectPatchQueueItemRecord{
				{QueueID: "queue-rq", ItemID: "item-16139", BranchID: "branch-16139", State: "REJECTED", PathsetJSON: `["cmd/rq"]`},
				{QueueID: "queue-rq", ItemID: "item-cli", BranchID: "branch-cli", State: "INTEGRATED", PathsetJSON: `["cmd/rq/version.go"]`},
			},
			want: false,
		},
		{
			name:  "no patch-queue items available -> keep blocking",
			task:  r68RevisionTask(),
			items: nil,
			want:  false,
		},
		{
			name: "candidate scope unknown (empty pathset) -> keep blocking",
			task: r68RevisionTask(),
			items: []ProjectPatchQueueItemRecord{
				{QueueID: "queue-rq", ItemID: "item-16139", BranchID: "branch-16139", State: "REJECTED", PathsetJSON: ""},
				{QueueID: "queue-rq", ItemID: "item-cli", BranchID: "branch-cli", State: "INTEGRATED", PathsetJSON: `["cmd/**"]`},
			},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := idleReflectionTaskIsRedundantRejectedRevision(tc.task, tc.items); got != tc.want {
				t.Fatalf("idleReflectionTaskIsRedundantRejectedRevision = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestR68ScopePathsAcceptsBareArrayAndWrapped(t *testing.T) {
	// Bare array (patch-queue pathset_json shape).
	if got := r68ScopePaths(`["cmd/rq/main.go","internal/lexer/lexer.go"]`); len(got) != 2 {
		t.Fatalf("bare array must parse to 2 paths, got %v", got)
	}
	// Wrapped object (branch write_scope_json shape).
	if got := r68ScopePaths(`{"paths":["cmd/**"]}`); len(got) != 1 || got[0] != "cmd/**" {
		t.Fatalf("wrapped object must parse to its paths, got %v", got)
	}
	// Malformed -> nil (fail closed -> keep blocking).
	if got := r68ScopePaths(`["cmd/rq/main.go"`); got != nil {
		t.Fatalf("malformed scope must parse to nil, got %v", got)
	}
	if got := r68ScopePaths(""); got != nil {
		t.Fatalf("empty scope must parse to nil, got %v", got)
	}
}

func TestR68PathCoveredByIntegratedIsDirectional(t *testing.T) {
	// A concrete file UNDER an integrated glob/dir is covered.
	if !r68PathCoveredByIntegrated("cmd/rq/main.go", []string{"cmd/**"}) {
		t.Fatal("cmd/rq/main.go must be covered by cmd/**")
	}
	if !r68PathCoveredByIntegrated("internal/evaluator/evaluator.go", []string{"internal/evaluator/**"}) {
		t.Fatal("a file under internal/evaluator/** must be covered")
	}
	// DIRECTIONAL (the red-team HIGH defect): a broad candidate directory is NOT covered by a narrower
	// integrated file beneath it.
	if r68PathCoveredByIntegrated("cmd/rq", []string{"cmd/rq/version.go"}) {
		t.Fatal("broad candidate cmd/rq must NOT be covered by the narrower integrated file cmd/rq/version.go")
	}
	// A sibling prefix must NOT false-positive.
	if r68PathCoveredByIntegrated("internal/lexerx/x.go", []string{"internal/lexer"}) {
		t.Fatal("internal/lexerx must NOT be covered by internal/lexer")
	}
	// A global/empty integrated scope never covers; a global candidate path is never covered.
	if r68PathCoveredByIntegrated("cmd/rq/main.go", []string{"**"}) {
		t.Fatal("a global integrated scope must not vacuously cover")
	}
	if r68PathCoveredByIntegrated("**", []string{"cmd/**"}) {
		t.Fatal("a global candidate path must never be covered")
	}
}

func TestFrontierBlockedOnlyByRevisionFollowupsGatesCoordinationRead(t *testing.T) {
	revision := r68RevisionTask()
	realImpl := WorkspaceTaskRecord{
		TaskID:      "task-impl-real",
		ProjectID:   "project-rq",
		Status:      "PENDING",
		TaskKind:    "EXECUTION",
		ProjectLane: "implementation",
		Title:       "Implement a genuinely new acceptance criterion",
	}
	// Only a revision followup blocks -> worth a coordination read.
	if !idleReflectionFrontierBlockedOnlyByRevisionFollowups([]WorkspaceTaskRecord{revision}, "project-rq") {
		t.Fatal("a frontier blocked solely by a revision followup must gate-in the coordination read")
	}
	// A genuine implementation task also blocks -> NOT worth a coordination read.
	if idleReflectionFrontierBlockedOnlyByRevisionFollowups([]WorkspaceTaskRecord{revision, realImpl}, "project-rq") {
		t.Fatal("a non-revision blocking task must gate-out the coordination read")
	}
	// No blocking task at all -> nothing to upgrade.
	if idleReflectionFrontierBlockedOnlyByRevisionFollowups(nil, "project-rq") {
		t.Fatal("an empty frontier must not gate-in the coordination read")
	}
}

func TestFrontierIsClearWithPatchQueueDiscountsRedundantRevision(t *testing.T) {
	const fullyCovered = `["cmd/rq/main.go","internal/evaluator/evaluator.go"]`
	revision := r68RevisionTask()
	items := r68Items("REJECTED", fullyCovered)

	// Strict predicate (no patch-queue context): the impl-lane revision still blocks.
	if idleReflectionProjectConvergenceFrontierIsClear([]WorkspaceTaskRecord{revision}, "project-rq") {
		t.Fatal("strict frontier predicate must treat the impl-lane revision as blocking")
	}
	// Patch-queue-aware: the redundant rejected revision is discounted -> frontier clear.
	if !idleReflectionProjectConvergenceFrontierIsClearWithPatchQueue([]WorkspaceTaskRecord{revision}, items, "project-rq") {
		t.Fatal("patch-queue-aware predicate must discount the redundant rejected revision")
	}
	// A genuine implementation task alongside it must still hold the frontier open.
	realImpl := WorkspaceTaskRecord{
		TaskID:      "task-impl-real",
		ProjectID:   "project-rq",
		Status:      "PENDING",
		TaskKind:    "EXECUTION",
		ProjectLane: "implementation",
		Title:       "Implement a genuinely new acceptance criterion",
	}
	if idleReflectionProjectConvergenceFrontierIsClearWithPatchQueue([]WorkspaceTaskRecord{revision, realImpl}, items, "project-rq") {
		t.Fatal("a genuine implementation task must keep the frontier open")
	}
}
