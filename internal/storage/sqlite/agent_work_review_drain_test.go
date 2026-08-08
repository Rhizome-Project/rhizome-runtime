package sqlite_test

import (
	"context"
	"strings"
	"testing"

	dag "github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	model "github.com/Rhizome-Project/rhizome-runtime/internal/model"
	sqlite "github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// R25-F2 regression: a patch-queue review task whose reviewer self-BLOCKED its claim after a
// structured-output repair failure, while the queue decision is already durably recorded, must still
// terminalize. Before the fix a BLOCKED review task was not a required-receipt sweep candidate
// (agentWorkRequiredReceiptTerminalizationCandidate) and the review terminal receipt did not allow
// blocked-claim terminalization, so the task stayed RUNNING and wedged the rightful owner - its
// validation/integration successor could never drain (round-25.md). This proves the sweep now reaps it.
func TestGetAgentWorkNextDrainsBlockedReviewTaskAfterDecision(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-r25-review-drain"
		projectID    = "project-r25-review-drain"
		leadID       = "alpha"
		workerID     = "delta"
		reviewerID   = "epsilon"
		repoID       = "repo-main"
		reviewDocKey = "project.r25-review-drain.branch.ready.review"
		reviewTaskID = "task-r25-review-drain-review"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               reviewerID,
		RoleType:              sqlite.ProjectRoleReviewer,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign reviewer role: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\delta\r25-review-drain`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewDocKey,
		Title:       "R25 Review Drain Packet",
		Content:     "# Review Packet\n\nCandidate is ready except for missing browser-smoke evidence.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, "branch-r25-review-drain", "agent/delta/r25-review-drain", `{"paths":["src/app.ts"]}`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-r25-review-drain",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/delta/r25-review-drain",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["src/app.ts"]}`,
		ReviewDocKey:          reviewDocKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ready branch: %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit patch queue item: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		LeaseSeconds:          900,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim patch queue item: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "review", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := createAgentWorkTaskBypassingSubmitGate(t, ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       reviewTaskID,
		OwnerUserID:  reviewerID,
		Priority:     "critical",
		Title:        "Review patch queue candidate " + item.QueueID + "/" + item.ItemID,
		Description:  "Patch queue: " + item.QueueID + "/" + item.ItemID + "\nBranch ID: " + branch.BranchID,
		TaskKind:     "COORDINATION",
		TaskTemplate: "integration",
		Tags:         []string{"review", "reviewer", "patch_queue", "project"},
		ProjectID:    projectID,
		ProjectLane:  "review",
	}, graph); err != nil {
		t.Fatalf("create review task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      reviewTaskID,
		LinkedBy:    reviewerID,
	}); err != nil {
		t.Fatalf("attach review task: %v", err)
	}
	// The reviewer claims its own review task, then self-BLOCKS the claim (structured-output repair failure
	// leaves the review task RUNNING with a BLOCKED claim while the decision is already recorded).
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      reviewTaskID,
		AgentID:     reviewerID,
		Summary:     "review claimed",
	}); err != nil {
		t.Fatalf("claim review task: %v", err)
	}
	// Durably record the BLOCKED queue decision (the review's contract is satisfied) while the reviewer still
	// holds the review task claim.
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Missing browser-smoke evidence; route follow-up validation instead of re-reviewing the same terminal item.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block patch queue item: %v", err)
	}
	// Structured-output repair failure leaves the review task RUNNING with a self-BLOCKED claim.
	if err := store.BlockTaskClaim(ctx, reviewTaskID, workspaceID, reviewerID); err != nil {
		t.Fatalf("block review task claim: %v", err)
	}

	// GetAgentWorkNext drives the required-receipt suppression sweep. It must now reap the blocked review
	// task because the decision is durably recorded.
	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     reviewerID,
	}); err != nil {
		t.Fatalf("get reviewer work next (drives sweep): %v", err)
	}

	status, err := store.GetTaskStatus(ctx, workspaceID, reviewTaskID)
	if err != nil {
		t.Fatalf("get review task status: %v", err)
	}
	if !strings.EqualFold(strings.TrimSpace(status.Status), model.TaskStatusResolved) {
		t.Fatalf("blocked review task must terminalize (RESOLVED) once the queue decision is recorded, got status %q", status.Status)
	}
}
