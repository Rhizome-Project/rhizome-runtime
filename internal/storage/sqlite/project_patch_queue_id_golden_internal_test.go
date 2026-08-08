package sqlite

import "testing"

func TestProjectPatchQueueDecisionContinuationIDGolden(t *testing.T) {
	t.Parallel()

	item := ProjectPatchQueueItemRecord{
		WorkspaceID: "ws-golden",
		ProjectID:   "project-signal01-checkpoint-golden",
		QueueID:     "queue-signal01-checkpoint-golden",
		ItemID:      "item-golden-lexer",
		BranchID:    "branch-golden-lexer",
		HeadSHA:     "abcdef1234567890",
	}
	if got, want := projectPatchQueueDecisionContinuationOutboxID(item, ProjectPatchQueueStateAccepted), "patchq-cont-b21f9c0e15d80fd0b6483bfc"; got != want {
		t.Fatalf("outbox id drift: got %q, want %q", got, want)
	}
	if got, want := ProjectPatchQueueDecisionContinuationTaskID(item.ProjectID, item, "integration"), "task-patchq-integration-project-signal01-checkpoint-golden-queue-signal01-checkpoint-golden-item-golden-lexer-eb3d1cc02961"; got != want {
		t.Fatalf("continuation task id drift: got %q, want %q", got, want)
	}
}
