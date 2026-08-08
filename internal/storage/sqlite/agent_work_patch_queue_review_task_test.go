package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestGetAgentWorkNextSurfacesMissingPatchQueueReviewTaskRepair(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-missing-review-task"
		projectID   = "project-patchq-missing-review-task"
		repoID      = "repo-patchq-missing-review-task"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "kappa"
		branchID    = "branch-patchq-missing-review-task"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)

	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branchID,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit patch queue item: %v", err)
	}
	reviewTaskID := sqlite.ProjectPatchQueueReviewTaskID(item)
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM workspace_tasks WHERE workspace_id = ? AND task_id = ?`, workspaceID, reviewTaskID); err != nil {
		t.Fatalf("detach review task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM tasks WHERE task_id = ?`, reviewTaskID); err != nil {
		t.Fatalf("delete review task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_patch_queue_review_task_missing" || result.Packet == nil || result.Packet.WorkType != "project_patch_queue_review_task_missing" {
		t.Fatalf("expected missing review task repair packet, got %+v", result)
	}
	if result.Packet.ProjectID != projectID || result.Packet.PreferredTransition != "reconcile_patch_queue_review_task_receipt" {
		t.Fatalf("unexpected repair packet: %+v", result.Packet)
	}
}

func TestGetAgentWorkNextPatchQueueReviewPreemptsStaleReviewerSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-review-preempts-session"
		projectID   = "project-patchq-review-preempts-session"
		repoID      = "repo-patchq-review-preempts-session"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "epsilon"
		branchID    = "branch-patchq-review-preempts-session"
		staleTaskID = "task-idle-reflection-stale-reviewer"
		staleSessID = "sess-idle-reflection-stale-reviewer"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, staleTaskID, "Stale reviewer reflection", "Reviewer should not keep reflecting while a proposed patch waits for review.", "research", "strategy", "high", []string{"idle-reflection"})
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      staleTaskID,
		AgentID:     reviewerID,
		Summary:     "stale sidecar session",
	}); err != nil {
		t.Fatalf("claim stale reviewer task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   staleSessID,
		AgentID:     reviewerID,
		WorkspaceID: workspaceID,
		TaskID:      staleTaskID,
		StartedAt:   "2026-06-08T19:00:00Z",
	}); err != nil {
		t.Fatalf("create stale reviewer session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.start",
		WorkspaceID: workspaceID,
		SessionID:   staleSessID,
		AgentID:     reviewerID,
		TaskID:      staleTaskID,
		Summary:     "stale reviewer sidecar is running",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("record stale reviewer session: %v", err)
	}

	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branchID,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit patch queue item: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Reason != "project_patch_queue_review_available" {
		t.Fatalf("expected pending patch queue review to preempt stale session, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != sqlite.ProjectPatchQueueReviewTaskID(item) {
		t.Fatalf("expected review receipt task, got %+v", result.Task)
	}
	if result.Session != nil {
		t.Fatalf("expected review receipt to start fresh instead of resuming stale session, got %+v", result.Session)
	}
}

func TestGetAgentWorkNextPatchQueueReviewPreemptsStaleReviewerClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-review-preempts-claim"
		projectID   = "project-patchq-review-preempts-claim"
		repoID      = "repo-patchq-review-preempts-claim"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "epsilon"
		branchID    = "branch-patchq-review-preempts-claim"
		staleTaskID = "task-idle-reflection-stale-claim"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, staleTaskID, "Stale reviewer claim", "Reviewer should not resume stale claim-only sidecars while a proposed patch waits for review.", "research", "strategy", "high", []string{"idle-reflection"})
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      staleTaskID,
		AgentID:     reviewerID,
		Summary:     "stale claim-only sidecar",
	}); err != nil {
		t.Fatalf("claim stale reviewer task: %v", err)
	}

	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branchID,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit patch queue item: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Reason != "project_patch_queue_review_available" {
		t.Fatalf("expected pending patch queue review to preempt stale claim, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != sqlite.ProjectPatchQueueReviewTaskID(item) {
		t.Fatalf("expected review receipt task, got %+v", result.Task)
	}
}
