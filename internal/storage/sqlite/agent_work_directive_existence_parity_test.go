package sqlite

import (
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// TestPatchQueueSupersedeExistenceHonorsTerminalDeterministicID locks the work.next frontier-preemption
// fix. A pre-frontier directive route (here: supersede-available, dispatched in GetAgentWorkNext BEFORE
// buildAgentWorkTaskFrontier) self-suppresses via this existence-check. The check MUST agree with the
// agent materializer's idempotency key: maybeMaterializePatchQueueSupersedeTask's SubmitTask dedups on a
// deterministic task ID regardless of status, so once that task terminalizes (RESOLVED/FAILED/CANCELLED)
// the agent can never recreate it. Before the fix the check skipped terminal tasks and kept returning
// false -> the route re-emitted the directive every tick -> work.next never reached the frontier and the
// owner's integration carrier was never surfaced (the live zeta livelock). The exact-deterministic-ID
// match is now checked ABOVE the terminal-skip. RED WITHOUT THE FIX: the first assertion fails because the
// RESOLVED exact-ID task is skipped and the check returns false.
func TestPatchQueueSupersedeExistenceHonorsTerminalDeterministicID(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		ProjectID: "project-p", QueueID: "q1", ItemID: "i1", BranchID: "b1", HeadSHA: "deadbeef",
	}
	branch := ProjectBranchRecord{
		ProjectID: "project-p", BranchID: "b1", BranchName: "feature/b1", HeadSHA: "deadbeef",
	}
	const evid = "project.p.evidence.v1"
	detID := agentWorkPatchQueueSupersedeTaskID(item.ProjectID, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA, evid)

	// (#4 reds-without-fix) A RESOLVED task carrying the exact deterministic ID is the idempotency key
	// SubmitTask dedups to. It MUST count as existing, else the directive re-emits and can never progress.
	resolvedExact := []WorkspaceTaskRecord{{TaskID: detID, ProjectID: item.ProjectID, Status: model.TaskStatusResolved}}
	if !agentWorkOpenPatchQueueSupersedeTaskExists(resolvedExact, nil, item, branch, evid) {
		t.Fatal("RESOLVED supersede task with the deterministic ID must count as existing (frontier-preemption guard)")
	}
	// Same for the other terminal statuses the materializer also cannot recreate.
	for _, st := range []string{model.TaskStatusFailed, model.TaskStatusCancelled} {
		terminal := []WorkspaceTaskRecord{{TaskID: detID, ProjectID: item.ProjectID, Status: st}}
		if !agentWorkOpenPatchQueueSupersedeTaskExists(terminal, nil, item, branch, evid) {
			t.Fatalf("terminal (%s) supersede task with the deterministic ID must count as existing", st)
		}
	}

	// (#2 re-supersede preserved) Fresh evidence -> new doc key -> new deterministic ID. The old RESOLVED
	// task must NOT suppress the genuinely new directive.
	if agentWorkOpenPatchQueueSupersedeTaskExists(resolvedExact, nil, item, branch, "project.p.evidence.v2") {
		t.Fatal("fresh-evidence directive (new docKey -> new deterministic ID) must still fire")
	}

	// (#3 no over-suppression) A terminal task that only FUZZY-matches (different TaskID, no det-ID) must
	// NOT suppress: the terminal-skip stays above the fuzzy text-match, so unrelated terminal tasks are
	// never matched loosely.
	fuzzyTerminal := []WorkspaceTaskRecord{{
		TaskID: "task-unrelated-x", ProjectID: item.ProjectID, Status: model.TaskStatusResolved,
		Title: "patch_queue supersede steward i1 b1 " + evid + " q1",
	}}
	if agentWorkOpenPatchQueueSupersedeTaskExists(fuzzyTerminal, nil, item, branch, evid) {
		t.Fatal("a terminal task matched only by fuzzy text must not suppress (no over-suppression)")
	}

	// (#3 still self-suppresses while open) A non-terminal exact-ID match still counts -- the route must
	// keep yielding to an in-flight stewardship task.
	openExact := []WorkspaceTaskRecord{{TaskID: detID, ProjectID: item.ProjectID, Status: model.TaskStatusPending}}
	if !agentWorkOpenPatchQueueSupersedeTaskExists(openExact, nil, item, branch, evid) {
		t.Fatal("non-terminal supersede task with the deterministic ID must still count as existing")
	}
}

// TestPatchQueueSubmitHandoffExistenceHonorsTerminalDeterministicID locks the same invariant for the
// sibling pre-frontier route (submit-handoff-available). Byte-identical terminal-skip bug, same fix.
func TestPatchQueueSubmitHandoffExistenceHonorsTerminalDeterministicID(t *testing.T) {
	branch := ProjectBranchRecord{
		ProjectID: "project-p", BranchID: "b1", HeadSHA: "deadbeef", ReviewDocKey: "project.p.review.v1",
	}
	detID := agentWorkPatchQueueSubmitHandoffTaskID(branch.ProjectID, branch.BranchID, branch.HeadSHA, branch.ReviewDocKey)

	// (#4 reds-without-fix) Every terminal status with the exact deterministic ID must count as existing.
	for _, st := range []string{model.TaskStatusResolved, model.TaskStatusFailed, model.TaskStatusCancelled} {
		terminal := []WorkspaceTaskRecord{{TaskID: detID, ProjectID: branch.ProjectID, Status: st}}
		if !agentWorkOpenPatchQueueSubmitHandoffTaskExists(terminal, nil, branch) {
			t.Fatalf("terminal (%s) submit-handoff task with the deterministic ID must count as existing (frontier-preemption guard)", st)
		}
	}
	resolvedExact := []WorkspaceTaskRecord{{TaskID: detID, ProjectID: branch.ProjectID, Status: model.TaskStatusResolved}}

	// (#2 re-fire preserved) A new head advances the deterministic ID, so the old RESOLVED task must not
	// suppress the new submit-handoff directive.
	advanced := branch
	advanced.HeadSHA = "feedface"
	if agentWorkOpenPatchQueueSubmitHandoffTaskExists(resolvedExact, nil, advanced) {
		t.Fatal("advanced-head submit-handoff directive (new deterministic ID) must still fire")
	}

	// (#3 no over-suppression) A terminal task that only fuzzy-matches the owner-submit markers (different
	// TaskID, no det-ID) must NOT suppress -- terminal-skip stays above the fuzzy text-match.
	fuzzyTerminal := []WorkspaceTaskRecord{{
		TaskID: "task-unrelated-y", ProjectID: branch.ProjectID, Status: model.TaskStatusResolved,
		Title: "owner-bound-kind:patch_queue_submit b1 deadbeef project.p.review.v1",
	}}
	if agentWorkOpenPatchQueueSubmitHandoffTaskExists(fuzzyTerminal, nil, branch) {
		t.Fatal("a terminal task matched only by fuzzy text must not suppress (no over-suppression)")
	}

	// (#3 still self-suppresses while open) Non-terminal exact-ID still counts.
	openExact := []WorkspaceTaskRecord{{TaskID: detID, ProjectID: branch.ProjectID, Status: model.TaskStatusPending}}
	if !agentWorkOpenPatchQueueSubmitHandoffTaskExists(openExact, nil, branch) {
		t.Fatal("non-terminal submit-handoff task with the deterministic ID must still count as existing")
	}
}
