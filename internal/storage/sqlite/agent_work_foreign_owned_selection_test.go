package sqlite_test

import (
	"context"
	"testing"

	dag "github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	sqlite "github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// R25 claimability-parity regression. A patch-queue decision continuation (validation successor) is
// hard-claim-routed by owner_user_id: the write claim path rejects a non-owner via
// rejectForeignAgentOwnedTaskClaimTx. Before the read-frontier fix, GetAgentWorkNext still advertised that
// delta-assigned validation task to non-owners (beta/gamma/...), who burned cycles on authoritatively
// rejected claims while the owner stayed wedged (round-25.md). This locks read==write parity: the non-owner
// must NOT receive the continuation as claimable work, while the owner still does.
func TestGetAgentWorkNextOmitsForeignOwnedPatchQueueContinuation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-r25-foreign-owned-validation"
		projectID   = "project-r25-foreign-owned-validation"
		leadID      = "alpha"
		ownerID     = "delta"
		nonOwnerID  = "beta"
		taskID      = "task-patchq-validation-r25-foreign-owned"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, nonOwnerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "validation", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	// A validation decision-continuation assigned to delta via owner_user_id. patch_queue_task_kind=validation
	// makes agentWorkPatchQueueDecisionContinuationTask true, so taskOwnerUserIDIsHardClaimRoute is true.
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         ownerID,
		Priority:            "high",
		Title:               "Validate blocked integration candidate (delta-owned)",
		Description:         "Validate the blocked patch queue candidate; same-head evidence or bounded revision.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "research",
		ProjectID:           projectID,
		ProjectLane:         "validation",
		RequiresProjectGate: false,
		Tags:                []string{"project", "patch_queue", "validation", "decision_continuation"},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1",` +
			`"patch_queue_task_kind":"validation","state":"BLOCKED"}`,
	}, graph); err != nil {
		t.Fatalf("create delta-owned validation continuation: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach validation continuation: %v", err)
	}

	// Non-owner must NOT be offered the delta-owned continuation as claimable work (read frontier honours the
	// same owner_user_id hard-claim route the write admission enforces).
	nonOwnerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          nonOwnerID,
		CandidateTaskID:  taskID,
		CoordinationMode: "trust_first",
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get non-owner work next: %v", err)
	}
	if nonOwnerWork.HasWork && nonOwnerWork.Task != nil && nonOwnerWork.Task.TaskID == taskID {
		t.Fatalf("non-owner %s must not receive delta-owned validation continuation as claimable work, got %+v", nonOwnerID, nonOwnerWork)
	}

	// The non-owner's claim attempt must also be authoritatively rejected (proves the read frontier omission
	// matches the write admission, not that the task is simply absent).
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     nonOwnerID,
		Summary:     "non-owner attempts delta-owned validation continuation",
	}); err == nil {
		t.Fatalf("expected non-owner claim of delta-owned validation continuation to be rejected, got nil error")
	}

	// The owner is still able to claim its own continuation (the fence narrows claimability, it does not strand
	// the task).
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     ownerID,
		Summary:     "owner claims its validation continuation",
	}); err != nil {
		t.Fatalf("owner %s must be able to claim its own validation continuation, got %v", ownerID, err)
	}
}
