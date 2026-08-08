package sqlite_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestGetAgentWorkNextPrefersActiveSessionAndHydrates(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-work", []string{"agent-a", "agent-b"})
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-agent-work",
		DocKey:      "current_context",
		Title:       "Current Context",
		Content:     "Hydration should ship the current task context.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	createAgentWorkTask(t, ctx, store, "ws-agent-work", "task-session", "normal")
	createAgentWorkTask(t, ctx, store, "ws-agent-work", "task-free", "low")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-work",
		TaskID:      "task-session",
		AgentID:     "agent-a",
		Summary:     "already working",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-agent-a",
		AgentID:     "agent-a",
		WorkspaceID: "ws-agent-work",
		TaskID:      "task-session",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.start",
		WorkspaceID: "ws-agent-work",
		SessionID:   "sess-agent-a",
		AgentID:     "agent-a",
		TaskID:      "task-session",
		Summary:     "Resume task-session",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record session coordination: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      "ws-agent-work",
		AgentID:          "agent-a",
		IncludeHydration: true,
		IncludeAllDocs:   true,
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Reason != "resume_session" {
		t.Fatalf("expected resume_session work result, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != "task-session" {
		t.Fatalf("expected task-session, got %+v", result.Task)
	}
	if result.Session == nil || result.Session.SessionID != "sess-agent-a" {
		t.Fatalf("expected sess-agent-a, got %+v", result.Session)
	}
	if result.TimeAuthority.WorkspaceID != "ws-agent-work" || result.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected workspace time authority on work.next result, got %+v", result.TimeAuthority)
	}
	if result.GeneratedAt != result.TimeAuthority.ReferenceAt {
		t.Fatalf("expected work.next generated_at %q to mirror time authority reference_at %q", result.GeneratedAt, result.TimeAuthority.ReferenceAt)
	}
	if result.Hydration == nil || result.Hydration.WorkspaceTask == nil || result.Hydration.WorkspaceTask.TaskID != "task-session" {
		t.Fatalf("expected hydration bundle for task-session, got %+v", result.Hydration)
	}
	if len(result.Hydration.Docs) != 1 || result.Hydration.Docs[0].DocKey != "current_context" {
		t.Fatalf("expected hydrated current_context doc, got %+v", result.Hydration.Docs)
	}
}

func TestGetAgentWorkNextHonorsWorkspaceTaskDependencies(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-work-deps"
		agentID     = "agent-a"
		blockerID   = "task-implementation"
		blockedID   = "task-review"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createAgentWorkTaskWithDetails(t, ctx, store, workspaceID, blockerID, "Implement converter", "", "high")
	createAgentWorkTaskWithDetails(t, ctx, store, workspaceID, blockedID, "Review converter", "", "critical")
	if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
		WorkspaceID: workspaceID,
		FromTaskID:  blockerID,
		ToTaskID:    blockedID,
		LinkType:    model.TaskLinkBlocks,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("add dependency link: %v", err)
	}
	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != blockerID {
		t.Fatalf("expected unresolved dependency to be selected before blocked task, got %+v", result)
	}
	triggerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		Trigger:         "runtime_switch_task",
		CandidateTaskID: blockedID,
		IncludePacket:   true,
	})
	if err != nil {
		t.Fatalf("get trigger dependency block: %v", err)
	}
	if triggerResult.HasWork || triggerResult.Reason != "task_dependency_blocked" || triggerResult.Trigger != "runtime_switch_task" {
		t.Fatalf("expected triggered blocked task to remain gated by dependency, got %+v", triggerResult)
	}
	if triggerResult.Packet == nil || triggerResult.Packet.Gate == nil || triggerResult.Packet.Gate.GateType != "task_dependency" {
		t.Fatalf("expected task dependency packet for triggered block, got %+v", triggerResult.Packet)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-blocked-review",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      blockedID,
		StartedAt:   "2026-05-11T00:00:00Z",
	}); err != nil {
		t.Fatalf("create blocked session: %v", err)
	}
	if err := store.UpdateAgentSession(ctx, sqlite.AgentSessionUpdateInput{
		SessionID: "sess-blocked-review",
		Status:    model.SessionStatusActive,
	}); err != nil {
		t.Fatalf("activate blocked session: %v", err)
	}
	sessionResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		Trigger:            "runtime_resume",
		CandidateSessionID: "sess-blocked-review",
		IncludePacket:      true,
	})
	if err != nil {
		t.Fatalf("get session dependency block: %v", err)
	}
	if sessionResult.Reason == "resume_session" || sessionResult.Session != nil {
		t.Fatalf("receiptless dependency-gated session must not be treated as resumable work, got %+v", sessionResult)
	}

	err = store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      blockedID,
		AgentID:     agentID,
		Summary:     "try early review",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "unresolved dependency") {
		t.Fatalf("expected blocked dependency claim rejection, got %v", err)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      blockerID,
		AgentID:     agentID,
		Summary:     "implement first",
	}); err != nil {
		t.Fatalf("claim blocker: %v", err)
	}
	if err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      blockerID,
		AgentID:     agentID,
		Summary:     "implementation complete",
	}); err != nil {
		t.Fatalf("complete blocker: %v", err)
	}

	unblocked, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("get unblocked work next: %v", err)
	}
	if !unblocked.HasWork || unblocked.Task == nil || unblocked.Task.TaskID != blockedID {
		t.Fatalf("expected blocked task after dependency resolution, got %+v", unblocked)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      blockedID,
		AgentID:     agentID,
		Summary:     "review after implementation",
	}); err != nil {
		t.Fatalf("claim unblocked task: %v", err)
	}
}

func TestGetAgentWorkNextAllowsPatchQueueValidationAfterReleasedImplementationArtifact(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-agent-work-patchq-artifact-dep"
		projectID    = "project-patchq-artifact-dep"
		repoID       = "repo-patchq-artifact-dep"
		leadID       = "alpha"
		builderID    = "beta"
		reviewerID   = "kappa"
		upstreamID   = "task-frontend-workbench"
		downstreamID = "task-patchq-visual-validation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	createAgentWorkProjectExecutionTask(t, ctx, store, workspaceID, projectID, upstreamID, "Build frontend workbench", "implementation", []string{"frontend"}, true)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-patchq-artifact-dep", builderID, reviewerID, `{"paths":["src/**","tests/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	markAgentWorkTaskReleasedWithBranch(t, ctx, store, workspaceID, upstreamID, builderID, repoID, item.BranchID)

	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, downstreamID, "Materialize missing visual acceptance evidence", strings.Join([]string{
		"Publish rhizome_visual_acceptance_v1 for the already-published blocked patch queue candidate.",
		"queue_id: " + item.QueueID,
		"item_id: " + item.ItemID,
		"branch_id: " + item.BranchID,
		"head_sha: " + item.HeadSHA,
	}, "\n"), "review", []string{"patch-queue", "validation", "visual-qa"}, false)
	if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
		WorkspaceID: workspaceID,
		FromTaskID:  upstreamID,
		ToTaskID:    downstreamID,
		LinkType:    model.TaskLinkBlocks,
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("add dependency link: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  downstreamID,
		CoordinationMode: "trust_first",
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get patch queue validation work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != downstreamID {
		t.Fatalf("expected published patch queue artifact to satisfy stale implementation dependency, got %+v", result)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      downstreamID,
		AgentID:     reviewerID,
		Summary:     "validate published blocked patch queue candidate",
	}); err != nil {
		t.Fatalf("claim downstream patch queue validation task: %v", err)
	}
}

func TestGetAgentWorkNextAllowsPatchQueueValidationWhenTargetNamesPublishedArtifact(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-agent-work-patchq-target-artifact"
		projectID    = "project-patchq-target-artifact"
		repoID       = "repo-patchq-target-artifact"
		leadID       = "alpha"
		builderID    = "beta"
		reviewerID   = "kappa"
		upstreamID   = "task-frontend-workbench"
		downstreamID = "task-materialize-beta-visual-evidence"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	createAgentWorkProjectExecutionTask(t, ctx, store, workspaceID, projectID, upstreamID, "Build frontend workbench", "implementation", []string{"frontend"}, true)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-beta-artifact", builderID, reviewerID, `{"paths":["src/**","tests/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	markAgentWorkTaskReleasedWithBranch(t, ctx, store, workspaceID, upstreamID, builderID, repoID, "branch-unrelated-later-reclaim")

	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, downstreamID, "Materialize missing visual acceptance for blocked beta patch item", strings.Join([]string{
		"Existing implementation dependency was re-claimed on a later unrelated branch, but this validation task is bound to the already-published patch queue artifact.",
		"Fresh coordination evidence shows " + item.ItemID + " on head " + item.HeadSHA + " remains BLOCKED.",
		"Publish rhizome_visual_acceptance_v1 or explicitly confirm the candidate still lacks it.",
	}, "\n"), "review", []string{"patch-queue", "validation", "visual-qa"}, false)
	if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
		WorkspaceID: workspaceID,
		FromTaskID:  upstreamID,
		ToTaskID:    downstreamID,
		LinkType:    model.TaskLinkBlocks,
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("add dependency link: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  downstreamID,
		CoordinationMode: "trust_first",
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get patch queue validation work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != downstreamID {
		t.Fatalf("expected target-bound patch queue artifact to satisfy stale implementation dependency, got %+v", result)
	}
}

func TestPatchQueueValidationDependencyStillBlocksWithoutMatchingArtifact(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-agent-work-patchq-artifact-mismatch"
		projectID    = "project-patchq-artifact-mismatch"
		repoID       = "repo-patchq-artifact-mismatch"
		leadID       = "alpha"
		builderID    = "beta"
		reviewerID   = "kappa"
		upstreamID   = "task-frontend-workbench"
		downstreamID = "task-patchq-visual-validation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	createAgentWorkProjectExecutionTask(t, ctx, store, workspaceID, projectID, upstreamID, "Build frontend workbench", "implementation", []string{"frontend"}, true)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-patchq-artifact-mismatch", builderID, reviewerID, `{"paths":["src/**","tests/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	markAgentWorkTaskReleasedWithBranch(t, ctx, store, workspaceID, upstreamID, builderID, repoID, item.BranchID)

	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, downstreamID, "Materialize missing visual acceptance evidence", strings.Join([]string{
		"Publish rhizome_visual_acceptance_v1 for a different blocked patch queue candidate.",
		"queue_id: " + item.QueueID,
		"item_id: patchitem-other",
		"branch_id: branch-other",
	}, "\n"), "review", []string{"patch-queue", "validation", "visual-qa"}, false)
	if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
		WorkspaceID: workspaceID,
		FromTaskID:  upstreamID,
		ToTaskID:    downstreamID,
		LinkType:    model.TaskLinkBlocks,
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("add dependency link: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     workspaceID,
		AgentID:         reviewerID,
		Trigger:         "runtime_switch_task",
		CandidateTaskID: downstreamID,
		IncludePacket:   true,
	})
	if err != nil {
		t.Fatalf("get mismatched patch queue validation work next: %v", err)
	}
	if result.HasWork || result.Reason != "task_dependency_blocked" {
		t.Fatalf("expected mismatched patch queue artifact to remain dependency-blocked, got %+v", result)
	}
	err = store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      downstreamID,
		AgentID:     reviewerID,
		Summary:     "try mismatched validation",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "unresolved dependency") {
		t.Fatalf("expected unresolved dependency claim rejection, got %v", err)
	}
}

func TestGenericValidationDoesNotBypassImplementationDependency(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-agent-work-generic-validation-dep"
		projectID    = "project-generic-validation-dep"
		repoID       = "repo-generic-validation-dep"
		leadID       = "alpha"
		builderID    = "beta"
		reviewerID   = "kappa"
		upstreamID   = "task-frontend-workbench"
		downstreamID = "task-generic-validation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	createAgentWorkProjectExecutionTask(t, ctx, store, workspaceID, projectID, upstreamID, "Build frontend workbench", "implementation", []string{"frontend"}, true)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-generic-validation-dep", builderID, reviewerID, `{"paths":["src/**","tests/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	markAgentWorkTaskReleasedWithBranch(t, ctx, store, workspaceID, upstreamID, builderID, repoID, item.BranchID)

	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, downstreamID, "Run generic validation", "Validate the current project after implementation is done.", "validation", []string{"validation"}, false)
	if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
		WorkspaceID: workspaceID,
		FromTaskID:  upstreamID,
		ToTaskID:    downstreamID,
		LinkType:    model.TaskLinkBlocks,
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("add dependency link: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     workspaceID,
		AgentID:         reviewerID,
		Trigger:         "runtime_switch_task",
		CandidateTaskID: downstreamID,
		IncludePacket:   true,
	})
	if err != nil {
		t.Fatalf("get generic validation work next: %v", err)
	}
	if result.HasWork || result.Reason != "task_dependency_blocked" {
		t.Fatalf("expected generic validation to remain dependency-blocked, got %+v", result)
	}
}

func TestGetAgentWorkNextTreatsRunningProjectRootDependencyAsContextAnchor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-work-root-anchor"
		projectID   = "project-anchor"
		alphaID     = "alpha"
		betaID      = "beta"
		rootID      = "root-project-anchor"
		followupID  = "task-project-followup"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{alphaID, betaID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Anchor",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, alphaID)
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, rootID, "Project root coordination", "strategy", "critical")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, followupID, "Implement next slice", "implementation", "high")
	if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
		WorkspaceID: workspaceID,
		FromTaskID:  rootID,
		ToTaskID:    followupID,
		LinkType:    model.TaskLinkBlocks,
		CreatedBy:   alphaID,
	}); err != nil {
		t.Fatalf("add root dependency link: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      rootID,
		AgentID:     alphaID,
		Summary:     "root umbrella coordination is running",
	}); err != nil {
		t.Fatalf("claim root task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          betaID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  followupID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != followupID {
		t.Fatalf("expected running project root dependency to be nonblocking context, got %+v", result)
	}
	if result.Reason == "task_dependency_blocked" {
		t.Fatalf("project root context dependency should not block follow-up execution: %+v", result)
	}
}

func TestGetAgentWorkNextReportsTerminalRuntimeSwitchTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-work-terminal-switch"
		agentID     = "agent-a"
		taskID      = "task-terminal-switch"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createAgentWorkTaskWithDetails(t, ctx, store, workspaceID, taskID, "Already done", "", "high")
	if err := store.CloseTask(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		ActorID:     "developer",
		Resolution:  model.TaskStatusResolved,
		Reason:      "task completed before delegated switch replay",
	}); err != nil {
		t.Fatalf("close task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		Trigger:         "runtime_switch_task",
		CandidateTaskID: taskID,
		IncludePacket:   true,
	})
	if err != nil {
		t.Fatalf("get terminal runtime switch work next: %v", err)
	}
	if result.HasWork || result.Reason != "trigger_task_terminal" || result.Trigger != "runtime_switch_task" {
		t.Fatalf("expected terminal runtime switch diagnostic, got %+v", result)
	}
}

func TestClaimTaskKeepsFailedAndCancelledDependenciesBlocking(t *testing.T) {
	t.Parallel()

	for _, resolution := range []string{model.TaskStatusFailed, model.TaskStatusCancelled} {
		resolution := resolution
		t.Run(resolution, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			workspaceID := "ws-agent-work-deps-" + strings.ToLower(resolution)
			const (
				agentID   = "agent-a"
				blockerID = "task-terminal-dependency"
				blockedID = "task-dependent"
			)
			seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
			createAgentWorkTaskWithDetails(t, ctx, store, workspaceID, blockerID, "Terminal dependency", "", "high")
			createAgentWorkTaskWithDetails(t, ctx, store, workspaceID, blockedID, "Dependent task", "", "critical")
			if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
				WorkspaceID: workspaceID,
				FromTaskID:  blockerID,
				ToTaskID:    blockedID,
				LinkType:    model.TaskLinkBlocks,
				CreatedBy:   "developer",
			}); err != nil {
				t.Fatalf("add dependency link: %v", err)
			}
			if err := store.CloseTask(ctx, sqlite.TaskCloseInput{
				WorkspaceID: workspaceID,
				TaskID:      blockerID,
				ActorID:     "developer",
				Resolution:  resolution,
				Reason:      "terminal dependency should not unblock dependent work",
			}); err != nil {
				t.Fatalf("close blocker as %s: %v", resolution, err)
			}

			err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
				WorkspaceID: workspaceID,
				TaskID:      blockedID,
				AgentID:     agentID,
				Summary:     "try dependent task",
			})
			if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "unresolved dependency") {
				t.Fatalf("expected %s dependency to keep claim blocked, got %v", resolution, err)
			}
		})
	}
}

func TestGetAgentWorkNextHydrationAutoIncludesTaskScopedDoc(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-hydration-shape", []string{"agent-a"})
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-agent-hydration-shape",
		DocKey:      "task.task-shaped",
		Title:       "Task Shaped",
		Content:     "This task-scoped doc should ride inside work.next hydration.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert task doc: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-agent-hydration-shape",
		DocKey:      "task.task-shaped.artifact_reality_check",
		Title:       "Artifact Reality Check",
		Content:     "The checkout is stock scaffold and not review-ready; smallest repair direction is implementation.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert artifact reality doc: %v", err)
	}
	createAgentWorkTask(t, ctx, store, "ws-agent-hydration-shape", "task-shaped", "high")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-hydration-shape",
		TaskID:      "task-shaped",
		AgentID:     "agent-a",
		Summary:     "resume shaped hydration",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      "ws-agent-hydration-shape",
		AgentID:          "agent-a",
		IncludeHydration: true,
		DocKeys:          []string{"current_context"},
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if result.Hydration == nil {
		t.Fatalf("expected hydration bundle, got %+v", result)
	}
	gotDocs := map[string]bool{}
	for _, doc := range result.Hydration.Docs {
		gotDocs[doc.DocKey] = true
	}
	for _, want := range []string{"task.task-shaped", "task.task-shaped.artifact_reality_check"} {
		if !gotDocs[want] {
			t.Fatalf("expected task-scoped doc %s to be auto-included, got %+v", want, result.Hydration.Docs)
		}
	}
}

func TestGetAgentWorkNextHydrationAutoIncludesProjectPlanningDocs(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-project-planning-hydration"
		agentID     = "agent-a"
		projectID   = "project-current-hydration"
		taskID      = "task-project-planning-hydration"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Current Hydration Project",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create current project: %v", err)
	}
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   "project-foreign-hydration",
		Title:       "Foreign Hydration Project",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create foreign project: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Hydrate project planning docs", "strategy", "high")
	for _, doc := range []sqlite.WorkspaceDocInput{
		{WorkspaceID: workspaceID, DocKey: "project." + projectID + ".product_contract", Title: "Product Contract", Content: "current product contract", UpdatedBy: "developer"},
		{WorkspaceID: workspaceID, DocKey: "project." + projectID + ".acceptance_criteria", Title: "Acceptance Criteria", Content: "current acceptance criteria", UpdatedBy: "developer"},
		{WorkspaceID: workspaceID, DocKey: "project." + projectID + ".plan_review", Title: "Plan Review", Content: "current plan review", UpdatedBy: "developer"},
		{WorkspaceID: workspaceID, DocKey: "project." + projectID + ".reflection_board", Title: "Reflection Board", Content: "current reflection board", UpdatedBy: "developer"},
		{WorkspaceID: workspaceID, DocKey: "project.project-foreign-hydration.product_contract", Title: "Foreign Contract", Content: "foreign product contract", UpdatedBy: "developer"},
	} {
		if err := store.UpsertWorkspaceDoc(ctx, doc); err != nil {
			t.Fatalf("upsert doc %s: %v", doc.DocKey, err)
		}
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "hydrate project planning docs",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          agentID,
		IncludeHydration: true,
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if result.Hydration == nil {
		t.Fatalf("expected hydration bundle, got %+v", result)
	}
	got := map[string]bool{}
	for _, doc := range result.Hydration.Docs {
		got[doc.DocKey] = true
		if strings.Contains(doc.Content, "foreign product contract") {
			t.Fatalf("foreign project planning doc leaked into hydration: %+v", result.Hydration.Docs)
		}
	}
	for _, want := range []string{
		"project." + projectID + ".product_contract",
		"project." + projectID + ".acceptance_criteria",
		"project." + projectID + ".plan_review",
		"project." + projectID + ".reflection_board",
	} {
		if !got[want] {
			t.Fatalf("expected hydrated project planning doc %s, got %+v", want, result.Hydration.Docs)
		}
	}
}

func TestGetAgentWorkNextReturnsClaimedTaskWithoutSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-claim", []string{"agent-a"})
	createAgentWorkTask(t, ctx, store, "ws-agent-claim", "task-claimed", "high")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-claim",
		TaskID:      "task-claimed",
		AgentID:     "agent-a",
		Summary:     "picked up before runtime restart",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: "ws-agent-claim",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Reason != "resume_claim" {
		t.Fatalf("expected resume_claim work result, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != "task-claimed" {
		t.Fatalf("expected task-claimed, got %+v", result.Task)
	}
	if result.Session != nil {
		t.Fatalf("expected no session for claimed-task resume, got %+v", result.Session)
	}
}

func TestGetAgentWorkNextResumesClaimAfterEndedSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-ended-session", []string{"agent-a"})
	createAgentWorkTask(t, ctx, store, "ws-agent-ended-session", "task-claimed", "high")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-ended-session",
		TaskID:      "task-claimed",
		AgentID:     "agent-a",
		Summary:     "implementation work is in the agent checkout",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-ended",
		AgentID:     "agent-a",
		WorkspaceID: "ws-agent-ended-session",
		TaskID:      "task-claimed",
		StartedAt:   "2026-05-06T00:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	keepActive := false
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:         "session.end",
		WorkspaceID:       "ws-agent-ended-session",
		SessionID:         "sess-ended",
		AgentID:           "agent-a",
		TaskID:            "task-claimed",
		Summary:           "System intervention - Tension Anti-Stall Reboot",
		Status:            model.SessionStatusEnded,
		KeepSessionActive: &keepActive,
	}); err != nil {
		t.Fatalf("record ended coordination: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: "ws-agent-ended-session",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Reason != "resume_claim" {
		t.Fatalf("expected ended session not to park claimed work, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != "task-claimed" {
		t.Fatalf("expected claimed task to resume, got %+v", result.Task)
	}
	if result.Session != nil {
		t.Fatalf("expected fresh session path after ended session, got %+v", result.Session)
	}
	if result.SessionAction != "start_new" || result.ClaimAction != "reuse_claim" {
		t.Fatalf("expected reuse_claim/start_new actions after ended session, got %+v", result)
	}
}

func TestGetAgentWorkNextSkipsBusyForeignSessionAndSelectsPendingTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-pending", []string{"agent-a", "agent-b"})
	createAgentWorkTask(t, ctx, store, "ws-agent-pending", "task-busy", "critical")
	createAgentWorkTask(t, ctx, store, "ws-agent-pending", "task-free", "high")

	claimExternalTaskForSessionStart(t, ctx, store, "ws-agent-pending", "task-busy", "agent-b")
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-agent-b",
		AgentID:     "agent-b",
		WorkspaceID: "ws-agent-pending",
		TaskID:      "task-busy",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.start",
		WorkspaceID: "ws-agent-pending",
		SessionID:   "sess-agent-b",
		AgentID:     "agent-b",
		TaskID:      "task-busy",
		Summary:     "Busy on task-busy",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record session coordination: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: "ws-agent-pending",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Reason != "next_pending" {
		t.Fatalf("expected next_pending work result, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != "task-free" {
		t.Fatalf("expected task-free, got %+v", result.Task)
	}
}

func TestGetAgentWorkNextNormalizesPriorityCaseForFreshSelection(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-priority-case", []string{"agent-a"})
	createAgentWorkTask(t, ctx, store, "ws-agent-priority-case", "task-uppercase-critical", "CRITICAL")
	createAgentWorkTask(t, ctx, store, "ws-agent-priority-case", "task-lower-high", "high")

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: "ws-agent-priority-case",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Reason != "next_pending" {
		t.Fatalf("expected next_pending work result, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != "task-uppercase-critical" {
		t.Fatalf("expected uppercase CRITICAL task to outrank lowercase high, got %+v", result.Task)
	}
}

func TestGetAgentWorkNextClaimsOperatorSpecRootBeforeAmbientRepairTasks(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-operator-root"
		agentID     = "agent-alpha"
		rootID      = "task-clearpress-root"
		repairID    = "task-ambient-context-repair"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		Bio:            "Coordinate autonomous product work, decompose root requests, and route builders/reviewers.",
		Specialization: "strategy",
		Tags:           []string{"strategist", "planner"},
		Metadata: map[string]any{
			"default_work_mode": "strategy",
		},
	}); err != nil {
		t.Fatalf("upsert strategy profile: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      rootID,
		OwnerUserID: "developer",
		Priority:    "HIGH",
		Title:       "Clearpress autonomous MVP deployment run",
		Description: strings.Join([]string{
			"Use the operator spec doc to create project coordination, decompose product work, and route autonomous agents.",
			"Do not treat this root task as implementation-only work.",
		}, "\n"),
		TaskKind:             model.TaskKindCoordination,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectLane:          "strategy",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","operator_spec_doc":"clearpress-abpc54-switch-durable-rerun17-operator-spec"}`,
	}, graph); err != nil {
		t.Fatalf("create operator root task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      rootID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach operator root task: %v", err)
	}
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, repairID, "Restore missing canonical context", "Publish task docs/current_context for visible root work.", model.TaskTemplateIntegration, "coordination", "high", []string{"metacognition", "context-repair"})

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		EnableTaskFrontier: true,
		IncludePacket:      true,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Reason != "next_pending" {
		t.Fatalf("expected operator root to be directly claimable before frontier/ambient repair, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != rootID {
		t.Fatalf("expected operator root task, got %+v", result.Task)
	}
	if result.ClaimAction != "claim_required" || result.SessionAction != "start_new" {
		t.Fatalf("expected durable claim/session transition hints for root work, got %+v", result)
	}
}

func TestGetAgentWorkNextClaimsOperatorSpecRootWithReviewContaminatedStrategistProfile(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-operator-root-review-contaminated-profile"
		agentID     = "alpha"
		rootID      = "task-signal01-rq-root"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	longRole := "rq product coordinator and strategist. Bootstraps the root task into one project with a single shared Go repo, writes the product contract from the spec doc, and opens only semantic deliverable tasks (lexer, parser, evaluator, built-ins with map/filter lambdas, REPL, error-model, test-suite, integration). Drives the work to review-ready branches, verifier review, integration, and runnable evidence without operator intervention."
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		Bio:            "Improve artifacts and move Rhizome work forward safely within the scope of " + longRole,
		Specialization: longRole,
		Tags:           []string{longRole},
		Metadata: map[string]any{
			"default_work_mode":      longRole,
			"primary_specialization": longRole,
			"reflection_scope":       "project",
		},
	}); err != nil {
		t.Fatalf("upsert strategist profile: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:       rootID,
		OwnerUserID:  "developer",
		Priority:     "critical",
		Title:        "Signal-01: build rq query interpreter from operator spec",
		Description:  "Autonomously bootstrap one shared Go project for the rq query interpreter. Read workspace doc operator.signal01.rq.spec.v1, publish product contract/design/acceptance docs, create semantic deliverable tasks, open implementation only when the shared repo and project gates are ready, and drive the work to review-ready branches, verifier review, integration, and runnable evidence without operator intervention.",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateGeneric,
	}, graph); err != nil {
		t.Fatalf("create operator root task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      rootID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach operator root task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		EnableTaskFrontier: true,
		IncludePacket:      true,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Reason != "next_pending" {
		t.Fatalf("review-contaminated strategist profile should still receive operator root, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != rootID {
		t.Fatalf("expected operator root task, got %+v", result.Task)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           rootID,
		AgentID:          agentID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Summary:          "claim review-contaminated strategist root",
	}); err != nil {
		t.Fatalf("claim operator root with review-contaminated strategist profile: %v", err)
	}
}

func TestGetAgentWorkNextSkipsPendingExecutionForObserveOnlyProfile(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-profile-gate", []string{"observer", "worker-neo"})
	createAgentWorkTask(t, ctx, store, "ws-agent-profile-gate", "task-free", "high")

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    "ws-agent-profile-gate",
		AgentID:        "observer",
		Bio:            "Analyze global system dynamics without direct participation. Provide insights, not decisions.",
		Specialization: "meta-analysis",
		Tags:           []string{"generalist", "observer"},
		Metadata: map[string]any{
			"default_work_mode": "observer",
		},
	}); err != nil {
		t.Fatalf("upsert observer profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    "ws-agent-profile-gate",
		AgentID:        "worker-neo",
		Bio:            "Execute tasks.",
		Specialization: "worker",
		Tags:           []string{"generalist", "worker"},
		Metadata: map[string]any{
			"default_work_mode": "generalist",
		},
	}); err != nil {
		t.Fatalf("upsert worker profile: %v", err)
	}

	observerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   "ws-agent-profile-gate",
		AgentID:       "observer",
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get observer work next: %v", err)
	}
	if observerResult.HasWork {
		t.Fatalf("expected observe-only profile to skip pending execution task, got %+v", observerResult)
	}
	if observerResult.Reason != "profile_gate_closed" || observerResult.AutonomousExecutionAllowed {
		t.Fatalf("expected explicit closed profile gate, got %+v", observerResult)
	}
	if !observerResult.ProfileGateBlockedWork || observerResult.ProfileGateReason == "" || observerResult.ProfileGateSummary == "" {
		t.Fatalf("expected profile gate blocking evidence, got %+v", observerResult)
	}
	if observerResult.Packet == nil || observerResult.Packet.Gate == nil || observerResult.Packet.Gate.GateState != "closed" {
		t.Fatalf("expected closed profile gate packet, got %+v", observerResult.Packet)
	}

	workerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: "ws-agent-profile-gate",
		AgentID:     "worker-neo",
	})
	if err != nil {
		t.Fatalf("get worker work next: %v", err)
	}
	if !workerResult.HasWork || workerResult.Reason != "next_pending" {
		t.Fatalf("expected worker to receive pending execution task, got %+v", workerResult)
	}
	if !workerResult.AutonomousExecutionAllowed || workerResult.ProfileGateReason != "profile_allows_autonomous_execution" {
		t.Fatalf("expected explicit open profile gate, got %+v", workerResult)
	}
	if workerResult.Task == nil || workerResult.Task.TaskID != "task-free" {
		t.Fatalf("expected task-free, got %+v", workerResult.Task)
	}
}

func TestGetAgentWorkNextRoutesFreshWorkBySpecializedProfile(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-role-routing"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"worker-neo", "reviewer-epsilon", "synth-zeta"})
	createAgentWorkTask(t, ctx, store, workspaceID, "task-build-dashboard", "critical")
	createAgentWorkTask(t, ctx, store, workspaceID, "task-review-evidence", "high")
	createAgentWorkTask(t, ctx, store, workspaceID, "task-final-report", "normal")

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "worker-neo",
		Bio:            "Build runnable UI surfaces.",
		Specialization: "frontend",
		Tags:           []string{"frontend", "generalist"},
		Metadata: map[string]any{
			"default_work_mode": "generalist",
		},
	}); err != nil {
		t.Fatalf("upsert worker profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "reviewer-epsilon",
		Bio:            "Challenge implementation quality and verify completion evidence.",
		Specialization: "review",
		Tags:           []string{"reviewer", "generalist", "test design"},
		Metadata: map[string]any{
			"default_work_mode": "generalist",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "synth-zeta",
		Bio:            "Condense distributed work into final operator-facing summaries.",
		Specialization: "synthesis",
		Tags:           []string{"synthesizer", "generalist", "final report"},
		Metadata: map[string]any{
			"default_work_mode": "generalist",
		},
	}); err != nil {
		t.Fatalf("upsert synthesis profile: %v", err)
	}

	workerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     "worker-neo",
	})
	if err != nil {
		t.Fatalf("get worker work next: %v", err)
	}
	if !workerResult.HasWork || workerResult.Task == nil || workerResult.Task.TaskID != "task-build-dashboard" {
		t.Fatalf("expected worker to receive generic build work, got %+v", workerResult)
	}

	reviewerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     "reviewer-epsilon",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if !reviewerResult.HasWork || reviewerResult.Task == nil || reviewerResult.Task.TaskID != "task-review-evidence" {
		t.Fatalf("expected reviewer to skip generic build work and receive review work, got %+v", reviewerResult)
	}

	synthResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     "synth-zeta",
	})
	if err != nil {
		t.Fatalf("get synthesis work next: %v", err)
	}
	if !synthResult.HasWork || synthResult.Task == nil || synthResult.Task.TaskID != "task-final-report" {
		t.Fatalf("expected synthesizer to skip generic/review work and receive final report work, got %+v", synthResult)
	}
}

func TestGetAgentWorkNextRoutesByTaskDescriptionAndTags(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-description-routing"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"reviewer-epsilon"})
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, "task-opaque-quality-pass", "Neutral follow-up", "Audit browser smoke, acceptance evidence, and UX regressions before release.", model.TaskTemplateIntegration, "", "high", []string{"quality"})

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "reviewer-epsilon",
		Bio:            "Challenge implementation quality and verify completion evidence.",
		Specialization: "review",
		Tags:           []string{"reviewer", "test design"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     "reviewer-epsilon",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != "task-opaque-quality-pass" {
		t.Fatalf("expected reviewer routing to inspect description/tags, got %+v", result)
	}
}

func TestTrustFirstFreshSelectionKeepsProactiveMetacognitionProfileScoped(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-metacognition-routing"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"worker-beta", "reviewer-epsilon"})
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, "task-idle-reflection-project", "Project metacognition pass: inspect plan and blockers", "meta-reflection follow-up", model.TaskTemplateIntegration, "qa", "critical", []string{"meta-reflection", "metacognition-scope-project"})
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, "task-idle-reflection-artifact", "Artifact quality iteration: review current UI evidence", "meta-reflection follow-up", model.TaskTemplateIntegration, "qa", "high", []string{"meta-reflection", "metacognition-scope-artifact"})
	createAgentWorkTaskWithTemplate(t, ctx, store, workspaceID, "task-build-slice", "Build runnable product slice", "implementation follow-up", model.TaskTemplateIntegration, "implementation", "normal")

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "worker-beta",
		Specialization: "frontend",
		Tags:           []string{"worker", "implementation"},
		Metadata: map[string]any{
			"default_work_mode":            "implementation",
			"reflection_scope":             "local",
			"can_open_reflection_tasks":    false,
			"max_new_tasks_per_idle_cycle": 0,
		},
	}); err != nil {
		t.Fatalf("upsert worker profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "reviewer-epsilon",
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode":         "review",
			"reflection_scope":          "artifact",
			"can_open_reflection_tasks": true,
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	workerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          "worker-beta",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get worker trust-first work next: %v", err)
	}
	if !workerResult.HasWork || workerResult.Task == nil || workerResult.Task.TaskID != "task-build-slice" {
		t.Fatalf("trust-first worker should skip broad metacognition task and claim implementation work, got %+v", workerResult)
	}

	reviewerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          "reviewer-epsilon",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get reviewer trust-first work next: %v", err)
	}
	if !reviewerResult.HasWork || reviewerResult.Task == nil || reviewerResult.Task.TaskID != "task-idle-reflection-artifact" {
		t.Fatalf("trust-first reviewer should skip project metacognition and claim artifact-scope reflection, got %+v", reviewerResult)
	}
}

func TestTrustFirstFreshSelectionReviewerSkipsPureImplementationLane(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-trust-first-reviewer-implementation-skip"
		projectID   = "project-reviewer-implementation-skip"
		reviewerID  = "reviewer-epsilon"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{reviewerID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Reviewer implementation skip",
		CreatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-build-mvp", "Build runnable MVP", "implementation", "critical")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode":         "review",
			"reflection_scope":          "artifact",
			"can_open_reflection_tasks": true,
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get reviewer trust-first work next: %v", err)
	}
	if result.HasWork {
		t.Fatalf("trust-first reviewer should not autoselect a pure implementation lane, got %+v", result)
	}
}

func TestTrustFirstFreshSelectionReviewerTakesReviewLaneOverImplementationLane(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-trust-first-reviewer-review-over-implementation"
		projectID   = "project-reviewer-review-over-implementation"
		repoID      = "repo-reviewer-review-over-implementation"
		reviewerID  = "reviewer-epsilon"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, reviewerID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, reviewerID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, reviewerID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, reviewerID, "branch-reviewer-review-over-implementation")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-build-mvp", "Build runnable MVP", "implementation", "critical")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-review-mvp", "Review runnable MVP against acceptance criteria", "review", "high")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode":         "review",
			"reflection_scope":          "artifact",
			"can_open_reflection_tasks": true,
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get reviewer trust-first work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != "task-review-mvp" {
		t.Fatalf("trust-first reviewer should skip implementation and claim review lane, got %+v", result)
	}
}

func TestTrustFirstTriggeredReviewerRejectsPureImplementationCandidate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-trust-first-triggered-reviewer-implementation"
		projectID   = "project-triggered-reviewer-implementation"
		reviewerID  = "reviewer-epsilon"
		builderID   = "builder-beta"
		taskID      = "task-build-mvp"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{reviewerID, builderID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Triggered reviewer implementation routing",
		CreatedBy:   builderID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, builderID)
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Build runnable MVP", "implementation", "critical")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        builderID,
		Specialization: "frontend",
		Tags:           []string{"builder", "implementation"},
		Metadata: map[string]any{
			"default_work_mode": "implementation",
		},
	}); err != nil {
		t.Fatalf("upsert builder profile: %v", err)
	}

	reviewerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get reviewer triggered work next: %v", err)
	}
	if reviewerResult.HasWork || reviewerResult.Reason != "trigger_no_work" || reviewerResult.ProfileGateReason != "profile_task_mode_mismatch" {
		t.Fatalf("trust-first reviewer should reject triggered pure implementation candidate, got %+v", reviewerResult)
	}

	builderResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
	})
	if err != nil {
		t.Fatalf("get builder triggered work next: %v", err)
	}
	if !builderResult.HasWork || builderResult.Task == nil || builderResult.Task.TaskID != taskID {
		t.Fatalf("trust-first builder should accept triggered implementation candidate, got %+v", builderResult)
	}
}

func TestTrustFirstReviewerAcceptsReviewCodedCoordinationTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-trust-first-reviewer-coordination-review"
		projectID   = "project-reviewer-coordination-review"
		reviewerID  = "iota"
		leadID      = "alpha"
		taskID      = "task-review-rq-doc-pack"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{reviewerID, leadID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "rq coordination review routing",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, leadID)
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Review rq coordination-doc pack for AC/spec gaps", "coordination", "high")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	fresh, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get reviewer fresh work next: %v", err)
	}
	if !fresh.HasWork || fresh.Task == nil || fresh.Task.TaskID != taskID {
		t.Fatalf("trust-first reviewer should see review-coded coordination work, got %+v", fresh)
	}

	triggered, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get reviewer triggered work next: %v", err)
	}
	if !triggered.HasWork || triggered.Task == nil || triggered.Task.TaskID != taskID {
		t.Fatalf("triggered reviewer delegation should accept review-coded coordination work, got %+v", triggered)
	}
	if triggered.ProfileGateReason == "profile_task_mode_mismatch" {
		t.Fatalf("review-coded coordination task must not trip profile mode mismatch, got %+v", triggered)
	}
}

func TestTrustFirstTriggeredImplementationRoleBypassesStaleStrategyProfile(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-trust-first-triggered-role-bypasses-stale-profile"
		projectID   = "project-triggered-role-bypasses-stale-profile"
		leadID      = "alpha"
		builderID   = "beta"
		otherID     = "eta"
		taskID      = "task-launchboard-beta-shell"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, otherID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Triggered role bypasses stale profile",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, leadID)
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Build LaunchBoard shell, layout, and styling surface", "implementation", "critical")
	for _, agentID := range []string{builderID, otherID} {
		if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
			WorkspaceID:    workspaceID,
			AgentID:        agentID,
			Specialization: "project strategy",
			Tags:           []string{"strategy"},
			Metadata: map[string]any{
				"default_work_mode": "strategy",
			},
		}); err != nil {
			t.Fatalf("upsert stale strategy profile for %s: %v", agentID, err)
		}
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               builderID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/**","package.json"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign implementer role: %v", err)
	}

	otherResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          otherID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get unassigned strategy agent triggered work next: %v", err)
	}
	if otherResult.HasWork || otherResult.Reason != "trigger_no_work" || otherResult.ProfileGateReason != "profile_task_mode_mismatch" {
		t.Fatalf("unassigned stale strategy profile must still reject pure implementation candidate, got %+v", otherResult)
	}

	builderResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get assigned implementer triggered work next: %v", err)
	}
	if !builderResult.HasWork || builderResult.Task == nil || builderResult.Task.TaskID != taskID {
		t.Fatalf("assigned implementer should accept targeted implementation task despite stale strategy profile, got %+v", builderResult)
	}
	if builderResult.ProfileGateReason == "profile_task_mode_mismatch" {
		t.Fatalf("explicit project role must bypass stale profile mismatch for targeted switch task, got %+v", builderResult)
	}
}

func TestTrustFirstTriggeredImplementerWithSharedCoordinationTagKeepsImplementationMode(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-triggered-implementer-shared-coordination-tag"
		projectID   = "project-triggered-implementer-shared-coordination-tag"
		leadID      = "alpha"
		builderID   = "beta"
		taskID      = "task-launchboard-beta-shell"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Shared coordination tag classifier regression",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, leadID)
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Build LaunchBoard Studio dashboard shell and responsive surface", "implementation", "critical")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        builderID,
		Specialization: "frontend shell, responsive layout, visual surface",
		Tags: []string{
			"LaunchBoard Studio frontend shell implementer; owns app scaffold",
			"responsive layout",
			"dashboard composition",
			"autonomous heartbeat cycles",
			"shared-memory coordination",
			"tool/plugin selection",
		},
		Metadata: map[string]any{
			"default_work_mode": "Minesweeper frontend shell implementer; owns React or vanilla app scaffold, responsive layout, board presentation, controls, and polished visual surface",
			"domain_scope":      []string{"web application UI", "local-first runtime", "multi-agent coordination"},
		},
	}); err != nil {
		t.Fatalf("upsert implementer profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get shared-coordination implementer triggered work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != taskID {
		t.Fatalf("implementer profile with shared coordination/tooling anatomy tags should accept targeted implementation work, got %+v", result)
	}
	if result.ProfileGateReason == "profile_task_mode_mismatch" {
		t.Fatalf("shared coordination anatomy tag must not reclassify implementer as strategy, got %+v", result)
	}
}

func TestTrustFirstTriggeredExactCoordinationProfileRemainsStrategyScoped(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-triggered-exact-coordination-profile"
		projectID   = "project-triggered-exact-coordination-profile"
		leadID      = "alpha"
		coordID     = "eta"
		taskID      = "task-launchboard-beta-shell"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, coordID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Exact coordination profile classifier regression",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, leadID)
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Build LaunchBoard Studio dashboard shell and responsive surface", "implementation", "critical")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        coordID,
		Specialization: "coordination",
		Tags:           []string{"coordination"},
		Metadata: map[string]any{
			"default_work_mode": "coordination",
		},
	}); err != nil {
		t.Fatalf("upsert coordination profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          coordID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get exact coordination triggered work next: %v", err)
	}
	if result.HasWork || result.Reason != "trigger_no_work" || result.ProfileGateReason != "profile_task_mode_mismatch" {
		t.Fatalf("exact coordination profile should remain strategy-scoped and reject pure implementation work, got %+v", result)
	}
}

func TestTrustFirstTriggeredVisualReviewerAcceptsRoutedBrowserActionTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-trust-first-visual-routed-action"
		projectID   = "project-visual-routed-action"
		reviewerID  = "kappa-visual"
		taskID      = "task-agent-backlog-kappa-browser-shot"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{reviewerID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Visual routed action task",
		CreatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, reviewerID)
	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, taskID,
		"Address: Resolve routed agent action request: browser_screenshot",
		"Private backlog route needs screenshot_capture evidence and a visual probe against the runnable product.",
		"implementation",
		[]string{"agent-backlog", "action-request", "tool-suite-screenshot_capture"},
		false,
	)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Specialization: "visual acceptance reviewer",
		Tags:           []string{"reviewer", "qa", "visual-qa"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert visual reviewer profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get visual reviewer routed action work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != taskID {
		t.Fatalf("visual reviewer should accept routed browser action task despite legacy implementation lane, got %+v", result)
	}
	if result.ProfileGateReason == "profile_task_mode_mismatch" {
		t.Fatalf("routed browser action task must not be profile-gated as pure implementation: %+v", result)
	}
}

func TestTrustFirstTriggeredReviewerRejectsClaimedImplementationWithoutSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-trust-first-triggered-reviewer-claimed-implementation"
		projectID   = "project-triggered-reviewer-claimed-implementation"
		reviewerID  = "reviewer-epsilon"
		taskID      = "task-build-mvp"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{reviewerID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Triggered claimed reviewer implementation routing",
		CreatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, reviewerID)
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Build runnable MVP", "implementation", "critical")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          reviewerID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Summary:          "legacy claimed implementation without a session",
	}); err != nil {
		t.Fatalf("claim implementation task in trust-first setup: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get reviewer triggered claimed work next: %v", err)
	}
	if result.HasWork || result.Reason != "trigger_no_work" || result.ProfileGateReason != "profile_task_mode_mismatch" {
		t.Fatalf("trust-first reviewer should reject claimed pure implementation candidate without a session, got %+v", result)
	}
}

func TestTrustFirstFreshSelectionBuilderSkipsPureStrategyLane(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-trust-first-builder-strategy-skip"
		projectID    = "project-builder-strategy-skip"
		builderID    = "builder-beta"
		strategistID = "strategist-alpha"
		taskID       = "task-frame-project"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{builderID, strategistID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Builder strategy skip",
		CreatedBy:   strategistID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Frame project strategy and acceptance criteria", "strategy", "critical")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        builderID,
		Specialization: "frontend",
		Tags:           []string{"builder", "implementation"},
		Metadata: map[string]any{
			"default_work_mode": "implementation",
		},
	}); err != nil {
		t.Fatalf("upsert builder profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        strategistID,
		Specialization: "strategy",
		Tags:           []string{"strategist", "planning"},
		Metadata: map[string]any{
			"default_work_mode": "strategy",
		},
	}); err != nil {
		t.Fatalf("upsert strategist profile: %v", err)
	}

	builderResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get builder trust-first work next: %v", err)
	}
	if builderResult.HasWork {
		t.Fatalf("trust-first builder should not autoselect a pure strategy lane, got %+v", builderResult)
	}

	strategistResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          strategistID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get strategist trust-first work next: %v", err)
	}
	if !strategistResult.HasWork || strategistResult.Task == nil || strategistResult.Task.TaskID != taskID {
		t.Fatalf("trust-first strategist should accept pure strategy lane, got %+v", strategistResult)
	}
}

func TestTrustFirstOwnerBoundRequiredOwnerBypassesFreshSelectionProfileGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-trust-first-owner-bound-profile-bypass"
		projectID     = "project-owner-bound-profile-bypass"
		leadID        = "strategist-alpha"
		branchOwnerID = "builder-beta"
		otherAgentID  = "builder-eta"
		repoID        = "repo-main"
		strategyTask  = "task-ordinary-strategy"
		pendingTask   = "task-owner-submit-pending"
		claimedTask   = "task-owner-submit-claimed"
		triggeredTask = "task-owner-submit-triggered"
		pendingBranch = "projbranch-owner-submit-pending"
		claimedBranch = "projbranch-owner-submit-claimed"
		triggerBranch = "projbranch-owner-submit-triggered"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, otherAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, pendingBranch)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, claimedBranch)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, triggerBranch)
	for _, agentID := range []string{branchOwnerID, otherAgentID} {
		if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
			WorkspaceID:    workspaceID,
			AgentID:        agentID,
			Specialization: "frontend implementation",
			Tags:           []string{"builder", "implementation"},
			Metadata: map[string]any{
				"default_work_mode": "implementation",
			},
		}); err != nil {
			t.Fatalf("upsert implementation profile for %s: %v", agentID, err)
		}
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, strategyTask, "Frame project strategy and acceptance criteria", "strategy", "critical")
	createOwnerSubmitCoordinationTaskForAgentWorkTest(t, ctx, store, workspaceID, projectID, pendingTask, pendingBranch, branchOwnerID)
	createOwnerSubmitCoordinationTaskForAgentWorkTest(t, ctx, store, workspaceID, projectID, claimedTask, claimedBranch, branchOwnerID)
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           claimedTask,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
		Summary:          "owner will resume owner-submit coordination",
	}); err != nil {
		t.Fatalf("claim owner-submit task: %v", err)
	}

	claimedResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get owner claimed work next: %v", err)
	}
	if !claimedResult.HasWork || claimedResult.Task == nil || claimedResult.Task.TaskID != claimedTask {
		t.Fatalf("owner should resume owner-bound coordination task before ordinary strategy, got %+v", claimedResult)
	}
	if claimedResult.ClaimAction != "reuse_claim" || claimedResult.SessionAction != "start_new" {
		t.Fatalf("expected resumable owner claim, got claim_action=%q session_action=%q", claimedResult.ClaimAction, claimedResult.SessionAction)
	}

	if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      claimedTask,
		AgentID:     branchOwnerID,
		Reason:      "test moves from resumable owner task to pending owner task",
	}); err != nil {
		t.Fatalf("release claimed owner-submit task: %v", err)
	}
	if err := store.CloseTask(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      claimedTask,
		ActorID:     leadID,
		Resolution:  model.TaskStatusCancelled,
		Reason:      "resumable-path assertion complete",
	}); err != nil {
		t.Fatalf("close claimed owner-submit task after resume assertion: %v", err)
	}
	pendingResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get owner pending work next: %v", err)
	}
	if !pendingResult.HasWork || pendingResult.Task == nil || pendingResult.Task.TaskID != pendingTask {
		t.Fatalf("owner should select pending owner-bound coordination task before ordinary strategy, got %+v", pendingResult)
	}
	if pendingResult.ClaimAction != "claim_required" || pendingResult.SessionAction != "start_new" {
		t.Fatalf("expected fresh pending owner task, got claim_action=%q session_action=%q", pendingResult.ClaimAction, pendingResult.SessionAction)
	}

	createOwnerSubmitCoordinationTaskForAgentWorkTest(t, ctx, store, workspaceID, projectID, triggeredTask, triggerBranch, branchOwnerID)
	triggeredResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  triggeredTask,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get owner triggered work next: %v", err)
	}
	if !triggeredResult.HasWork || triggeredResult.Task == nil || triggeredResult.Task.TaskID != triggeredTask {
		t.Fatalf("owner should accept triggered owner-bound coordination task despite implementation profile, got %+v", triggeredResult)
	}

	otherResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          otherAgentID,
		CoordinationMode: "trust_first",
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get non-owner work next: %v", err)
	}
	if otherResult.HasWork {
		t.Fatalf("implementation-profile non-owner must not bypass into ordinary strategy or owner-bound coordination, got %+v", otherResult)
	}
}

func TestReadyBranchWithoutPatchQueueItemSurfacesOwnerSubmitHandoff(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-ready-branch-submit-handoff"
		projectID   = "project-ready-branch-submit-handoff"
		leadID      = "alpha"
		ownerID     = "beta"
		otherID     = "delta"
		repoID      = "repo-main"
		branchID    = "projbranch-ready-submit-orphan"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, otherID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)

	ownerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          ownerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get owner work next: %v", err)
	}
	if ownerResult.HasWork || ownerResult.Reason != "project_patch_queue_submit_handoff_available" {
		t.Fatalf("expected owner submit handoff packet without task, got %+v", ownerResult)
	}
	if ownerResult.Packet == nil || ownerResult.Packet.OwnerBound == nil {
		t.Fatalf("expected owner-bound submit packet, got %+v", ownerResult.Packet)
	}
	if got := ownerResult.Packet.PreferredTransition; got != "create_or_claim_owner_bound_patch_queue_submit" {
		t.Fatalf("preferred transition = %q", got)
	}
	if ownerResult.Packet.OwnerBound.RequiredAgentID != ownerID ||
		ownerResult.Packet.OwnerBound.BranchID != branch.BranchID ||
		ownerResult.Packet.OwnerBound.HeadSHA != branch.HeadSHA ||
		ownerResult.Packet.OwnerBound.ReviewDocKey != branch.ReviewDocKey {
		t.Fatalf("unexpected owner-bound payload: %+v branch=%+v", ownerResult.Packet.OwnerBound, branch)
	}

	otherResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          otherID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get non-owner work next: %v", err)
	}
	if otherResult.Reason == "project_patch_queue_submit_handoff_available" {
		t.Fatalf("non-owner must not receive branch owner submit handoff, got %+v", otherResult)
	}
}

func TestReadyBranchPatchQueueSubmitHandoffPreemptsOrdinaryPendingImplementation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-ready-branch-submit-handoff-preempts-pending"
		projectID   = "project-ready-branch-submit-handoff-preempts-pending"
		leadID      = "alpha"
		ownerID     = "beta"
		repoID      = "repo-main"
		branchID    = "projbranch-ready-submit-preempt"
		taskID      = "task-ordinary-implementation-reclaim"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Continue ordinary implementation lane", "implementation", "critical")

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          ownerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get owner work next: %v", err)
	}
	if result.HasWork || result.Task != nil || result.Reason != "project_patch_queue_submit_handoff_available" {
		t.Fatalf("READY branch missing queue item must preempt ordinary pending implementation reclaim, got %+v", result)
	}
	if result.Packet == nil || result.Packet.OwnerBound == nil || result.Packet.OwnerBound.BranchID != branch.BranchID {
		t.Fatalf("expected owner-bound submit handoff for branch %s, got %+v", branch.BranchID, result.Packet)
	}
}

func TestReadyBranchPatchQueueSubmitHandoffSuppressesWhenItemOrTaskExists(t *testing.T) {
	t.Parallel()

	t.Run("patch queue item exists", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		const (
			workspaceID = "ws-ready-branch-submit-handoff-item"
			projectID   = "project-ready-branch-submit-handoff-item"
			leadID      = "alpha"
			ownerID     = "beta"
			repoID      = "repo-main"
			branchID    = "projbranch-ready-submit-with-item"
		)
		seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
		createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
		claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
		upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
		branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
		if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			RepoID:                repoID,
			BranchID:              branch.BranchID,
			ActorID:               ownerID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", ownerID),
			PromptContextSurface:  "project.patch_queue.submit",
		}); err != nil {
			t.Fatalf("submit patch queue item: %v", err)
		}

		result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
			WorkspaceID:      workspaceID,
			AgentID:          ownerID,
			IncludePacket:    true,
			CoordinationMode: "trust_first",
		})
		if err != nil {
			t.Fatalf("get owner work next: %v", err)
		}
		if result.Reason == "project_patch_queue_submit_handoff_available" {
			t.Fatalf("existing same branch/head queue item should suppress handoff, got %+v", result)
		}
	})

	t.Run("open owner submit task exists", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		const (
			workspaceID = "ws-ready-branch-submit-handoff-task"
			projectID   = "project-ready-branch-submit-handoff-task"
			leadID      = "alpha"
			ownerID     = "beta"
			repoID      = "repo-main"
			branchID    = "projbranch-ready-submit-with-task"
			taskID      = "task-existing-owner-submit"
		)
		seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
		createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
		claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
		upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
		registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
		createOwnerBoundPatchQueueSubmitTask(t, ctx, store, workspaceID, projectID, taskID, branchID, ownerID)

		result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
			WorkspaceID:      workspaceID,
			AgentID:          ownerID,
			IncludePacket:    true,
			CoordinationMode: "trust_first",
		})
		if err != nil {
			t.Fatalf("get owner work next: %v", err)
		}
		if !result.HasWork || result.Task == nil || result.Task.TaskID != taskID {
			t.Fatalf("existing owner-submit task should be selected instead of packet materialization, got %+v", result)
		}
	})

	t.Run("released blocked owner submit task does not starve fresh work", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		ctx := context.Background()
		const (
			workspaceID = "ws-ready-branch-submit-handoff-released-blocked"
			projectID   = "project-ready-branch-submit-handoff-released-blocked"
			leadID      = "alpha"
			ownerID     = "beta"
			repoID      = "repo-main"
			branchID    = "projbranch-ready-submit-released-blocked"
			taskID      = "task-released-blocked-owner-submit"
			freshTaskID = "task-fresh-product-revision"
		)
		seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
		createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
		claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
		upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
		registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
		createOwnerBoundPatchQueueSubmitTask(t, ctx, store, workspaceID, projectID, taskID, branchID, ownerID)

		if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
			WorkspaceID:      workspaceID,
			TaskID:           taskID,
			AgentID:          ownerID,
			CoordinationMode: "trust_first",
			Summary:          "claim owner-submit before blocker",
		}); err != nil {
			t.Fatalf("claim owner-submit task: %v", err)
		}
		if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			AgentID:     ownerID,
			Reason:      "durable owner-submit blocker before manager stop",
		}); err != nil {
			t.Fatalf("block owner-submit task: %v", err)
		}
		if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			AgentID:     ownerID,
			Reason:      "legacy manager stop erased the blocked claim",
		}); err != nil {
			t.Fatalf("release blocked owner-submit task: %v", err)
		}
		createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, freshTaskID, "Continue product revision after stale handoff", "implementation", "critical")

		result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
			WorkspaceID:      workspaceID,
			AgentID:          ownerID,
			Trigger:          "request_resume",
			CandidateTaskID:  taskID,
			IncludePacket:    true,
			CoordinationMode: "trust_first",
		})
		if err != nil {
			t.Fatalf("get triggered owner-submit work next: %v", err)
		}
		// Post-A1 contract: the receipt sweep cancels the stale handoff BEFORE selection,
		// so the dead request_resume returns the superseded packet (the runtime clears
		// the trigger on consumption). The stale task must never be reselected.
		if result.HasWork && result.Task != nil && result.Task.TaskID == taskID {
			t.Fatalf("stale owner-submit task must not be reselected: %+v", result)
		}
		if !result.HasWork && result.Reason != "trigger_task_superseded" {
			t.Fatalf("expected trigger_task_superseded for swept owner-submit handoff, got %+v", result)
		}
		// No-starvation invariant across the poll boundary: the NEXT plain poll must
		// hand out the fresh product task (and must NOT re-offer the submit handoff,
		// because a durable owner-submit block receipt exists for that branch).
		followUp, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
			WorkspaceID:      workspaceID,
			AgentID:          ownerID,
			IncludePacket:    true,
			CoordinationMode: "trust_first",
		})
		if err != nil {
			t.Fatalf("get follow-up work next: %v", err)
		}
		if !followUp.HasWork || followUp.Task == nil || followUp.Task.TaskID != freshTaskID {
			t.Fatalf("released blocked owner-submit task should not starve fresh work on the next poll, got %+v", followUp)
		}
	})
}

func TestTrustFirstFreshSelectionLeavesProductQualityImplementationOpen(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-product-quality-routing"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"worker-beta"})
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, "task-post-mvp-polish", "Polish product UX controls", "implementation follow-up", model.TaskTemplateIntegration, "implementation", "critical", []string{"product-quality", "post-mvp"})
	createAgentWorkTaskWithTemplate(t, ctx, store, workspaceID, "task-build-slice", "Build runnable product slice", "implementation follow-up", model.TaskTemplateIntegration, "implementation", "normal")

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "worker-beta",
		Specialization: "frontend",
		Tags:           []string{"worker", "implementation"},
		Metadata: map[string]any{
			"default_work_mode":         "implementation",
			"reflection_scope":          "local",
			"can_open_reflection_tasks": false,
		},
	}); err != nil {
		t.Fatalf("upsert worker profile: %v", err)
	}

	workerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          "worker-beta",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get worker trust-first work next: %v", err)
	}
	if !workerResult.HasWork || workerResult.Task == nil || workerResult.Task.TaskID != "task-post-mvp-polish" {
		t.Fatalf("trust-first worker should keep ordinary product-quality implementation work open, got %+v", workerResult)
	}
}

func TestTrustFirstFreshSelectionLeavesMetacognitionImplementationOpen(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-metacognition-implementation-routing"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"worker-beta"})
	createAgentWorkTaskWithTemplate(t, ctx, store, workspaceID, "task-implement-metacognition-ui", "Implement metacognition profile gradient UI", "implementation follow-up", model.TaskTemplateIntegration, "implementation", "critical")

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "worker-beta",
		Specialization: "frontend",
		Tags:           []string{"worker", "implementation"},
		Metadata: map[string]any{
			"default_work_mode":         "implementation",
			"reflection_scope":          "local",
			"can_open_reflection_tasks": false,
		},
	}); err != nil {
		t.Fatalf("upsert worker profile: %v", err)
	}

	workerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          "worker-beta",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get worker trust-first work next: %v", err)
	}
	if !workerResult.HasWork || workerResult.Task == nil || workerResult.Task.TaskID != "task-implement-metacognition-ui" {
		t.Fatalf("trust-first worker should keep ordinary metacognition implementation work open, got %+v", workerResult)
	}
}

func TestTrustFirstTriggeredMetacognitionRespectsReflectionScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-triggered-metacognition-routing"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"worker-beta", "reviewer-epsilon"})
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, "task-idle-reflection-project", "Project metacognition pass: inspect plan and blockers", "meta-reflection follow-up", model.TaskTemplateIntegration, "qa", "critical", []string{"meta-reflection", "metacognition-scope-project"})
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, "task-idle-reflection-artifact", "Artifact quality iteration: review current UI evidence", "meta-reflection follow-up", model.TaskTemplateIntegration, "qa", "high", []string{"meta-reflection", "metacognition-scope-artifact"})

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "worker-beta",
		Specialization: "frontend",
		Tags:           []string{"worker", "implementation"},
		Metadata: map[string]any{
			"default_work_mode":         "implementation",
			"reflection_scope":          "local",
			"can_open_reflection_tasks": false,
		},
	}); err != nil {
		t.Fatalf("upsert worker profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "reviewer-epsilon",
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode":         "review",
			"reflection_scope":          "artifact",
			"can_open_reflection_tasks": true,
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	workerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          "worker-beta",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  "task-idle-reflection-project",
	})
	if err != nil {
		t.Fatalf("get worker triggered metacognition work next: %v", err)
	}
	if workerResult.HasWork || workerResult.Reason != "trigger_no_work" || workerResult.ProfileGateReason != "metacognition_scope_mismatch" {
		t.Fatalf("trust-first worker should softly reject triggered project metacognition, got %+v", workerResult)
	}

	reviewerProjectResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          "reviewer-epsilon",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  "task-idle-reflection-project",
	})
	if err != nil {
		t.Fatalf("get reviewer triggered project metacognition work next: %v", err)
	}
	if reviewerProjectResult.HasWork || reviewerProjectResult.Reason != "trigger_no_work" || reviewerProjectResult.ProfileGateReason != "metacognition_scope_mismatch" {
		t.Fatalf("artifact reviewer should softly reject triggered project metacognition, got %+v", reviewerProjectResult)
	}

	reviewerArtifactResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          "reviewer-epsilon",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  "task-idle-reflection-artifact",
	})
	if err != nil {
		t.Fatalf("get reviewer triggered artifact metacognition work next: %v", err)
	}
	if !reviewerArtifactResult.HasWork || reviewerArtifactResult.Task == nil || reviewerArtifactResult.Task.TaskID != "task-idle-reflection-artifact" {
		t.Fatalf("artifact reviewer should accept triggered artifact metacognition, got %+v", reviewerArtifactResult)
	}
}

func TestMixedPlanningEvidenceTaskOutranksIdleReflectionAndClaimsWithoutImplementationGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-r45-planning-evidence"
		projectID    = "project-r45-planning-evidence"
		reviewerID   = "zeta"
		ambientID    = "task-ambient-project-r45-84dc75ae6d755732"
		reflectionID = "task-idle-reflection-project-r45-20260613-1000"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{reviewerID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "R45 planning evidence",
		CreatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, ambientID,
		"Materialize product contract and plan review",
		"Create project product_contract and plan_review docs, then compare shipped evidence against them.",
		"implementation",
		[]string{"docs", "review", "spec-fidelity"},
		false)
	requirementsJSON := string(mustTestJSON(t, map[string]any{
		"schema":              "task_requirements.v1",
		"preferred_tools":     []string{"workspace_doc_get", "project_patch_queue_list"},
		"required_work_modes": []string{"implementation", "review", "synthesis"},
	}))
	if _, err := store.DB().ExecContext(ctx, `
UPDATE tasks
   SET task_requirements_json = ?,
       write_scope_hints_json = '[]',
       requires_project_gate = 1
 WHERE task_id = ?`, requirementsJSON, ambientID); err != nil {
		t.Fatalf("shape ambient planning evidence task: %v", err)
	}
	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, reflectionID,
		"Project reflection pass: join active quality work or fill a gap",
		"Rhizome profile-scoped product-quality iteration task.",
		"qa",
		[]string{"meta-reflection", "anti-idle", "product-quality", "metacognition-scope-global"},
		false)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode":         "review",
			"reflection_scope":          "global",
			"can_open_reflection_tasks": true,
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	triggered, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  ambientID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get triggered ambient planning evidence work: %v", err)
	}
	if !triggered.HasWork || triggered.Task == nil || triggered.Task.TaskID != ambientID {
		t.Fatalf("reviewer runtime_switch_task should admit mixed planning evidence task, got %+v", triggered)
	}
	if triggered.ProfileGateReason == "profile_task_mode_mismatch" || triggered.Reason == "project_gate_closed" {
		t.Fatalf("mixed planning evidence task must not collapse to implementation/profile gate, got %+v", triggered)
	}

	frontierResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            reviewerID,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
		EnableTaskFrontier: true,
		IncludePacket:      true,
	})
	if err != nil {
		t.Fatalf("get frontier work: %v", err)
	}
	if !frontierResult.HasWork || frontierResult.Packet == nil || frontierResult.Packet.Frontier == nil {
		t.Fatalf("expected task frontier, got %+v", frontierResult)
	}
	var ambientCandidate, reflectionCandidate *sqlite.AgentWorkTaskFrontierCandidate
	for i := range frontierResult.Packet.Frontier.Candidates {
		candidate := &frontierResult.Packet.Frontier.Candidates[i]
		switch candidate.Task.TaskID {
		case ambientID:
			ambientCandidate = candidate
		case reflectionID:
			reflectionCandidate = candidate
		}
	}
	if ambientCandidate == nil || ambientCandidate.Blocked {
		t.Fatalf("ambient planning evidence task should be an unblocked frontier candidate, got ambient=%+v frontier=%+v", ambientCandidate, frontierResult.Packet.Frontier.Candidates)
	}
	if reflectionCandidate != nil && !reflectionCandidate.Blocked && reflectionCandidate.Fit.Score >= ambientCandidate.Fit.Score {
		t.Fatalf("idle reflection must not outrank actionable planning evidence work, ambient=%+v reflection=%+v", ambientCandidate, reflectionCandidate)
	}

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           ambientID,
		AgentID:          reviewerID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Summary:          "claim mixed planning evidence task",
	}); err != nil {
		t.Fatalf("claim mixed planning evidence task without implementation branch bindings: %v", err)
	}
}

func TestMixedImplementationReviewTaskWithoutPlanningEvidenceKeepsImplementationGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-mixed-implementation-review-gated"
		projectID   = "project-mixed-implementation-review-gated"
		reviewerID  = "zeta"
		taskID      = "task-build-and-review-feature"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{reviewerID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Mixed implementation review gated",
		CreatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, taskID,
		"Build and review feature slice",
		"Implement the feature and review the changed behavior.",
		"implementation",
		[]string{"feature", "backend"},
		false)
	requirementsJSON := string(mustTestJSON(t, map[string]any{
		"schema":              "task_requirements.v1",
		"required_work_modes": []string{"implementation", "review"},
	}))
	if _, err := store.DB().ExecContext(ctx, `UPDATE tasks SET task_requirements_json = ? WHERE task_id = ?`, requirementsJSON, taskID); err != nil {
		t.Fatalf("shape mixed implementation task: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	triggered, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get triggered mixed implementation work: %v", err)
	}
	if triggered.HasWork {
		t.Fatalf("ordinary mixed implementation/review task must not bypass implementation admission, got %+v", triggered)
	}
	if triggered.ProfileGateReason != "profile_task_mode_mismatch" && triggered.Reason != "project_gate_closed" {
		t.Fatalf("expected profile or project gate to remain active, got %+v", triggered)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          reviewerID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Summary:          "claim should fail",
	}); err == nil {
		t.Fatalf("ordinary mixed implementation/review task claim should still require implementation gate")
	}
}

func TestTrustFirstTriggeredMetacognitionLiftsStaleStrategistProfile(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-stale-strategist-metacognition"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"strategist-alpha", "ux-iota"})
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, "task-project-reflection", "Project metacognition pass: inspect coordination gaps", "meta-reflection follow-up", model.TaskTemplateIntegration, "coordination", "critical", []string{"meta-reflection", "metacognition-scope-project"})

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "strategist-alpha",
		Bio:            "High global meta-cognition. Steward one shared web-app project and project architecture.",
		Specialization: "global strategy and project architecture",
		Tags:           []string{"strategist", "planning", "plan-review"},
		Metadata: map[string]any{
			"default_work_mode":         "strategy",
			"domain_scope":              []any{"workspace docs", "review", "repair", "coordination work"},
			"reflection_scope":          "artifact",
			"can_open_reflection_tasks": false,
		},
	}); err != nil {
		t.Fatalf("upsert stale strategist profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          "strategist-alpha",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  "task-project-reflection",
	})
	if err != nil {
		t.Fatalf("get strategist triggered metacognition work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != "task-project-reflection" {
		t.Fatalf("stale artifact metadata should not trap strategist out of project reflection, got %+v", result)
	}

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "ux-iota",
		Bio:            "Mentions project strategy in feedback but remains a harsh real-user UI/UX critic.",
		Specialization: "ui/ux review and usability",
		Tags:           []string{"ui/ux", "reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode":         "review",
			"reflection_scope":          "artifact",
			"can_open_reflection_tasks": true,
		},
	}); err != nil {
		t.Fatalf("upsert ux profile: %v", err)
	}
	uxResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          "ux-iota",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  "task-project-reflection",
	})
	if err != nil {
		t.Fatalf("get ux triggered project metacognition work next: %v", err)
	}
	if uxResult.HasWork || uxResult.ProfileGateReason != "metacognition_scope_mismatch" {
		t.Fatalf("UX/reviewer artifact profile should not be lifted by project-strategy wording, got %+v", uxResult)
	}
}

func TestTrustFirstTriggeredLegacyIdleReflectionInfersProjectScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-legacy-idle-reflection-routing"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"reviewer-epsilon", "strategist-alpha"})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   "project-subpixel",
		Title:       "Subpixel Art",
		CreatedBy:   "strategist-alpha",
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, "project-subpixel", "task-idle-reflection-project-legacy", "Product quality iteration: inspect and improve Subpixel Art", "qa", "critical")

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "reviewer-epsilon",
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode":         "review",
			"reflection_scope":          "artifact",
			"can_open_reflection_tasks": true,
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "strategist-alpha",
		Specialization: "strategy",
		Tags:           []string{"strategist", "planning"},
		Metadata: map[string]any{
			"default_work_mode":         "strategy",
			"reflection_scope":          "project",
			"can_open_reflection_tasks": true,
		},
	}); err != nil {
		t.Fatalf("upsert strategist profile: %v", err)
	}

	reviewerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          "reviewer-epsilon",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  "task-idle-reflection-project-legacy",
	})
	if err != nil {
		t.Fatalf("get reviewer triggered legacy project reflection: %v", err)
	}
	if reviewerResult.HasWork || reviewerResult.ProfileGateReason != "metacognition_scope_mismatch" {
		t.Fatalf("artifact reviewer should reject legacy project idle reflection, got %+v", reviewerResult)
	}

	strategistResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          "strategist-alpha",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  "task-idle-reflection-project-legacy",
	})
	if err != nil {
		t.Fatalf("get strategist triggered legacy project reflection: %v", err)
	}
	if !strategistResult.HasWork || strategistResult.Task == nil || strategistResult.Task.TaskID != "task-idle-reflection-project-legacy" {
		t.Fatalf("strategist should accept legacy project idle reflection, got %+v", strategistResult)
	}
}

func TestTrustFirstProjectScopeProfilesCanClaimGlobalMetacognition(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-global-metacognition-routing"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"strategist-alpha", "integrator-zeta"})
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, "task-global-metacognition", "Global metacognition pass: inspect workspace coordination", "meta-reflection follow-up", model.TaskTemplateIntegration, "coordination", "critical", []string{"meta-reflection", "metacognition-scope-global"})

	for _, agent := range []struct {
		id             string
		specialization string
		mode           string
	}{
		{id: "strategist-alpha", specialization: "strategy", mode: "strategy"},
		{id: "integrator-zeta", specialization: "integration", mode: "synthesis"},
	} {
		if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
			WorkspaceID:    workspaceID,
			AgentID:        agent.id,
			Specialization: agent.specialization,
			Tags:           []string{agent.specialization},
			Metadata: map[string]any{
				"default_work_mode":         agent.mode,
				"reflection_scope":          "project",
				"can_open_reflection_tasks": true,
			},
		}); err != nil {
			t.Fatalf("upsert %s profile: %v", agent.id, err)
		}
		result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
			WorkspaceID:      workspaceID,
			AgentID:          agent.id,
			CoordinationMode: sqlite.CoordinationModeTrustFirst,
			Trigger:          "runtime_switch_task",
			CandidateTaskID:  "task-global-metacognition",
		})
		if err != nil {
			t.Fatalf("get %s global metacognition work next: %v", agent.id, err)
		}
		if !result.HasWork || result.Task == nil || result.Task.TaskID != "task-global-metacognition" {
			t.Fatalf("%s should accept explicit global metacognition, got %+v", agent.id, result)
		}
	}
}

func TestGetAgentWorkNextRoutesValidationLaneToImplementationAndSynthesisProfiles(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-validation-routing"
		projectID   = "project-validation-routing"
		leadID      = "alpha"
		workerID    = "gamma"
		synthID     = "zeta"
		taskID      = "task-browser-smoke-validation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, synthID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, "repo-validation-routing", leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, "repo-validation-routing", workerID, "branch-validation-routing")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Validate local launch and browser smoke", "validation", "high")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        workerID,
		Bio:            "Build and repair runnable product slices.",
		Specialization: "frontend builder",
		Tags:           []string{"builder", "frontend"},
		Metadata: map[string]any{
			"default_work_mode": "generalist",
		},
	}); err != nil {
		t.Fatalf("upsert worker profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        synthID,
		Bio:            "Synthesize project evidence and operator-facing reports.",
		Specialization: "synthesis",
		Tags:           []string{"synthesizer", "final report"},
		Metadata: map[string]any{
			"default_work_mode": "generalist",
		},
	}); err != nil {
		t.Fatalf("upsert synthesis profile: %v", err)
	}

	for _, tc := range []struct {
		agentID string
		label   string
	}{
		{agentID: workerID, label: "implementation"},
		{agentID: synthID, label: "synthesis"},
	} {
		result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
			WorkspaceID: workspaceID,
			AgentID:     tc.agentID,
		})
		if err != nil {
			t.Fatalf("get %s work next: %v", tc.label, err)
		}
		if !result.HasWork || result.Task == nil || result.Task.TaskID != taskID {
			t.Fatalf("expected %s profile to see validation task %s, got %+v", tc.label, taskID, result)
		}
	}
}

func TestGetAgentWorkNextBlocksVisualValidationBeforeReviewableArtifact(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-validation-artifact-gate"
		projectID   = "project-validation-artifact-gate"
		leadID      = "alpha"
		validatorID = "kappa"
		taskID      = "task-visual-validation-before-artifact"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, validatorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t,
		ctx,
		store,
		workspaceID,
		projectID,
		taskID,
		"Produce browser smoke and visual acceptance evidence",
		"Use the browser to capture screenshot evidence for the active web app once a review-ready branch or patch queue candidate exists.",
		"validation",
		[]string{"validation", "visual-qa", "browser-smoke"},
		false,
	)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, validatorID, sqlite.ProjectRoleReviewer, leadID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        validatorID,
		Bio:            "Harsh UI/UX verifier with browser and visual acceptance skills.",
		Specialization: "visual acceptance verifier",
		Tags:           []string{"reviewer", "visual-qa", "browser-smoke"},
		ToolsAccess:    []string{"browser", "chrome-devtools"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert validator profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          validatorID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get validation work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_validation_artifact_missing" {
		t.Fatalf("expected validation to wait for a reviewable artifact, got %+v", result)
	}
	if result.Packet == nil || result.Packet.Gate == nil || result.Packet.Gate.GateType != "project_validation_artifact" {
		t.Fatalf("expected validation artifact gate packet, got %+v", result.Packet)
	}

	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, "repo-validation-artifact-gate", leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, "repo-validation-artifact-gate", leadID, "branch-validation-artifact-gate")

	readyResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          validatorID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get validation work next after artifact: %v", err)
	}
	if !readyResult.HasWork || readyResult.Task == nil || readyResult.Task.TaskID != taskID {
		t.Fatalf("expected validation task after review-ready branch appears, got %+v", readyResult)
	}
}

func TestGetAgentWorkNextRequiresMatchingArtifactForTargetedValidation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID      = "ws-validation-targeted-artifact-gate"
		projectID        = "project-validation-targeted-artifact-gate"
		repoID           = "repo-validation-targeted-artifact-gate"
		leadID           = "alpha"
		validatorID      = "kappa"
		taskID           = "task-targeted-branch-validation"
		targetBranchID   = "branch-targeted-validation"
		unrelatedBranch  = "branch-unrelated-ready"
		validationPrompt = "Validate browser smoke for branch_id: " + targetBranchID
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, validatorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, leadID, unrelatedBranch)
	createAgentWorkProjectExecutionTaskWithDescription(
		t,
		ctx,
		store,
		workspaceID,
		projectID,
		taskID,
		"Validate targeted branch visual smoke",
		validationPrompt,
		"validation",
		[]string{"validation", "browser-smoke"},
		false,
	)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, validatorID, sqlite.ProjectRoleReviewer, leadID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        validatorID,
		Specialization: "browser validation reviewer",
		Tags:           []string{"reviewer", "browser-smoke"},
		ToolsAccess:    []string{"browser"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert validator profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          validatorID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get targeted validation before matching artifact: %v", err)
	}
	if result.HasWork || result.Reason != "project_validation_artifact_missing" {
		t.Fatalf("unrelated ready branch must not unlock targeted validation, got %+v", result)
	}

	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, leadID, targetBranchID)
	readyResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          validatorID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get targeted validation after matching artifact: %v", err)
	}
	if !readyResult.HasWork || readyResult.Task == nil || readyResult.Task.TaskID != taskID {
		t.Fatalf("matching branch should unlock targeted validation, got %+v", readyResult)
	}
}

func TestGetAgentWorkNextRequiresPatchQueueArtifactForQueueTargetedValidation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID    = "ws-validation-targeted-patch-queue-gate"
		projectID      = "project-validation-targeted-patch-queue-gate"
		repoID         = "repo-validation-targeted-patch-queue-gate"
		leadID         = "alpha"
		validatorID    = "kappa"
		taskID         = "task-targeted-patch-queue-validation"
		targetBranchID = "branch-patch-queue-validation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, validatorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, leadID, targetBranchID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t,
		ctx,
		store,
		workspaceID,
		projectID,
		taskID,
		"Validate targeted patch queue browser smoke",
		"Patch queue decision follow-up.\n\n- queue_id: queue-missing\n- item_id: item-missing\n- branch_id: "+targetBranchID,
		"validation",
		[]string{"validation", "browser-smoke"},
		false,
	)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, validatorID, sqlite.ProjectRoleReviewer, leadID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        validatorID,
		Specialization: "browser validation reviewer",
		Tags:           []string{"reviewer", "browser-smoke"},
		ToolsAccess:    []string{"browser"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert validator profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          validatorID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get queue-targeted validation before item: %v", err)
	}
	if result.HasWork || result.Reason != "project_validation_artifact_missing" {
		t.Fatalf("matching ready branch alone must not unlock queue/item-targeted validation, got %+v", result)
	}
}

func TestGetAgentWorkNextRunnableSurfaceDoesNotBypassTargetedPatchQueueValidation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID    = "ws-validation-targeted-patch-queue-url-gate"
		projectID      = "project-validation-targeted-patch-queue-url-gate"
		repoID         = "repo-validation-targeted-patch-queue-url-gate"
		leadID         = "alpha"
		validatorID    = "kappa"
		taskID         = "task-targeted-patch-queue-validation-url"
		targetBranchID = "branch-patch-queue-validation-url"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, validatorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, leadID, targetBranchID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t,
		ctx,
		store,
		workspaceID,
		projectID,
		taskID,
		"Validate targeted patch queue browser smoke with surface",
		"Patch queue decision follow-up.\n\n- queue_id: queue-missing\n- item_id: item-missing\n- branch_id: "+targetBranchID+"\n- runnable_url: http://127.0.0.1:5173",
		"validation",
		[]string{"validation", "browser-smoke"},
		false,
	)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, validatorID, sqlite.ProjectRoleReviewer, leadID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        validatorID,
		Specialization: "browser validation reviewer",
		Tags:           []string{"reviewer", "browser-smoke"},
		ToolsAccess:    []string{"browser"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert validator profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          validatorID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get queue-targeted validation with runnable URL: %v", err)
	}
	if result.HasWork || result.Reason != "project_validation_artifact_missing" {
		t.Fatalf("runnable URL must not bypass missing queue/item artifact, got %+v", result)
	}
}

func TestGetAgentWorkNextRejectsPlaceholderRunnableSurface(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-validation-placeholder-surface"
		projectID   = "project-validation-placeholder-surface"
		leadID      = "alpha"
		validatorID = "kappa"
		taskID      = "task-placeholder-surface-validation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, validatorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t,
		ctx,
		store,
		workspaceID,
		projectID,
		taskID,
		"Validate placeholder runnable surface",
		"Use browser_visual_probe after the surface is ready. runnable_url: TBD",
		"validation",
		[]string{"validation", "browser-smoke"},
		false,
	)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, validatorID, sqlite.ProjectRoleReviewer, leadID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        validatorID,
		Specialization: "browser validation reviewer",
		Tags:           []string{"reviewer", "browser-smoke"},
		ToolsAccess:    []string{"browser"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert validator profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          validatorID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get placeholder runnable surface validation: %v", err)
	}
	if result.HasWork || result.Reason != "project_validation_artifact_missing" {
		t.Fatalf("placeholder runnable URL must not unlock browser validation, got %+v", result)
	}
}

func TestGetAgentWorkNextAllowsRunnableSurfaceValidationWithoutBranchArtifact(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-validation-runnable-surface"
		projectID   = "project-validation-runnable-surface"
		leadID      = "alpha"
		validatorID = "kappa"
		taskID      = "task-runnable-surface-validation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, validatorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t,
		ctx,
		store,
		workspaceID,
		projectID,
		taskID,
		"Validate provisional runnable surface",
		"Use browser_visual_probe against runnable_url: http://127.0.0.1:5173 to capture provisional UI evidence.",
		"validation",
		[]string{"validation", "browser-smoke"},
		false,
	)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, validatorID, sqlite.ProjectRoleReviewer, leadID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        validatorID,
		Specialization: "browser validation reviewer",
		Tags:           []string{"reviewer", "browser-smoke"},
		ToolsAccess:    []string{"browser"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert validator profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          validatorID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get runnable surface validation: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != taskID {
		t.Fatalf("explicit runnable URL should be enough for provisional browser validation, got %+v", result)
	}
}

func TestGetAgentWorkNextUsesPromotionDocSurfaceEvidenceForValidation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-validation-promotion-doc-surface"
		projectID   = "project-validation-promotion-doc-surface"
		leadID      = "alpha"
		validatorID = "kappa"
		taskID      = "task-promoted-visual-action-request"
		docKey      = "task.task-promoted-visual-action-request"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, validatorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Autonomous Backlog Promotion - verified UI surface",
		Content:     "# Autonomous Backlog Promotion\n\n## Evidence\n- selector:runnable_surface\n- status:surface_preflight_verified\n- url:http://127.0.0.1:5173\n- browser_probe:verified_product_marker\n",
		UpdatedBy:   leadID,
	}); err != nil {
		t.Fatalf("upsert promotion doc: %v", err)
	}
	createAgentWorkProjectExecutionTaskWithDescription(
		t,
		ctx,
		store,
		workspaceID,
		projectID,
		taskID,
		"Capture missing visual acceptance screenshots",
		"Autonomous initiative promoted from agent personal backlog.\n\n- promotion_doc: "+docKey+"\n\nUse browser_visual_probe to capture visual evidence for the verified runnable surface.",
		"validation",
		[]string{"validation", "visual-qa", "browser-smoke"},
		false,
	)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, validatorID, sqlite.ProjectRoleReviewer, leadID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        validatorID,
		Specialization: "visual acceptance reviewer",
		Tags:           []string{"reviewer", "visual-qa", "browser-smoke"},
		ToolsAccess:    []string{"browser", "chrome-devtools"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert validator profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          validatorID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get promoted visual validation: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != taskID {
		t.Fatalf("promotion doc surface evidence should unlock promoted visual validation, got %+v", result)
	}
}

func TestGetAgentWorkNextPromotionDocPatchQueueEvidenceRemainsTargeted(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID    = "ws-validation-promotion-doc-patch-queue"
		projectID      = "project-validation-promotion-doc-patch-queue"
		repoID         = "repo-validation-promotion-doc-patch-queue"
		leadID         = "alpha"
		validatorID    = "kappa"
		taskID         = "task-promoted-patch-queue-visual-validation"
		docKey         = "task.task-promoted-patch-queue-visual-validation"
		targetBranchID = "branch-promoted-patch-queue"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, validatorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, leadID, targetBranchID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Autonomous Backlog Promotion - patch queue visual follow-up",
		Content: strings.Join([]string{
			"# Autonomous Backlog Promotion",
			"",
			"## Evidence",
			"- patch_queue:queue-missing",
			"- patch_item:item-missing",
			"- branch:" + targetBranchID,
			"- url:http://127.0.0.1:5173",
			"- status:surface_preflight_verified",
		}, "\n"),
		UpdatedBy: leadID,
	}); err != nil {
		t.Fatalf("upsert promotion doc: %v", err)
	}
	createAgentWorkProjectExecutionTaskWithDescription(
		t,
		ctx,
		store,
		workspaceID,
		projectID,
		taskID,
		"Validate promoted patch queue visual evidence",
		"Autonomous initiative promoted from agent personal backlog.\n\n- promotion_doc: "+docKey+"\n\nUse browser_visual_probe only for the targeted patch queue item.",
		"validation",
		[]string{"validation", "visual-qa", "browser-smoke"},
		false,
	)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, validatorID, sqlite.ProjectRoleReviewer, leadID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        validatorID,
		Specialization: "visual acceptance reviewer",
		Tags:           []string{"reviewer", "visual-qa", "browser-smoke"},
		ToolsAccess:    []string{"browser", "chrome-devtools"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert validator profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          validatorID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get promoted patch queue visual validation: %v", err)
	}
	if result.HasWork || result.Reason != "project_validation_artifact_missing" {
		t.Fatalf("promotion doc patch queue refs must stay targeted despite URL/ready branch, got %+v", result)
	}
}

func TestGetAgentWorkNextSkipsTerminalPatchQueueReviewTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-agent-terminal-patchq-review"
		projectID      = "project-terminal-patchq-review"
		leadID         = "alpha"
		workerID       = "delta"
		reviewerID     = "epsilon"
		repoID         = "repo-main"
		reviewDocKey   = "project.terminal-patchq.branch.ready.review"
		reviewTaskID   = "task-terminal-patchq-review"
		followupTaskID = "task-patchq-validation-followup"
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
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Bio:            "Review candidates, block incomplete evidence, and validate follow-ups.",
		Specialization: "review",
		Tags:           []string{"reviewer", "verification"},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\delta\terminal-patchq-review`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewDocKey,
		Title:       "Terminal Patch Queue Review Packet",
		Content:     "# Review Packet\n\nCandidate is ready except for missing browser-smoke evidence.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	_, branchTaskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, "branch-terminal-patchq-review", "agent/delta/terminal-patchq-review", `{"paths":["src/app.ts"]}`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-terminal-patchq-review",
		ActiveTaskID:          branchTaskID,
		ActiveClaimID:         branchTaskID,
		BranchName:            "agent/delta/terminal-patchq-review",
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

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "review", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              followupTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Validate blocked patch queue candidate with browser smoke",
		Description:         "Patch queue: " + item.QueueID + "/" + item.ItemID + "\nBranch ID: " + branch.BranchID,
		TaskKind:            "COORDINATION",
		TaskTemplate:        "integration",
		Tags:                []string{"project", "patch-queue", "validation", "blocked"},
		ProjectID:           projectID,
		ProjectLane:         "validation",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create validation follow-up task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      followupTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach validation follow-up task: %v", err)
	}
	if err := createAgentWorkTaskBypassingSubmitGate(t, ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       reviewTaskID,
		OwnerUserID:  "developer",
		Priority:     "critical",
		Title:        "Review patch queue candidate " + item.QueueID + "/" + item.ItemID,
		Description:  "Patch queue: " + item.QueueID + "/" + item.ItemID + "\nBranch ID: " + branch.BranchID,
		TaskKind:     "COORDINATION",
		TaskTemplate: "integration",
		Tags:         []string{"review", "reviewer", "patch_queue", "project"},
		ProjectID:    projectID,
		ProjectLane:  "review",
	}, graph); err != nil {
		t.Fatalf("create terminal patch queue review task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      reviewTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach terminal patch queue review task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     reviewerID,
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != followupTaskID {
		t.Fatalf("expected terminal patch review to be skipped in favor of validation follow-up, got %+v", result)
	}
}

func TestGetAgentWorkNextSurfacesExpiredClaimedPatchQueueStewardship(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-patch-queue-claim-stewardship"
		projectID    = "project-patch-queue-claim-stewardship"
		repoID       = "repo-patch-queue-claim-stewardship"
		leadID       = "alpha"
		ownerID      = "beta"
		integratorID = "zeta"
		reviewerID   = "epsilon"
		builderID    = "gamma"
		branchID     = "branch-claimed-patch-queue"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, integratorID, reviewerID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		RemoteURL:             "file:///tmp/" + repoID,
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindLocal,
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		DefaultBranch:         "main",
		IntegrationBranch:     "main",
		IsCanonical:           true,
		CreatedByAgentID:      leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.repository.upsert",
	}); err != nil {
		t.Fatalf("upsert project repository: %v", err)
	}
	for _, agentID := range []string{integratorID, reviewerID} {
		roleType := sqlite.ProjectRoleIntegrator
		if agentID == reviewerID {
			roleType = sqlite.ProjectRoleReviewer
		}
		if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			AgentID:               agentID,
			RoleType:              roleType,
			ActorID:               leadID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
			PromptContextSurface:  "project.role.assign",
		}); err != nil {
			t.Fatalf("assign project role to %s: %v", agentID, err)
		}
	}
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   "task-" + branchID,
		SessionID:                "session-" + branchID,
		RunID:                    "run-" + branchID,
		AgentID:                  ownerID,
		CapabilitySnapshotID:     "cap-" + branchID,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 `C:\fixtures\agents\` + ownerID + `\` + branchID,
		BaseTreeHash:             strings.Repeat("a", 40),
		BaseFileHashes: map[string]string{
			"src/app.go": "sha256:src",
		},
		RepoLeaseID:           "lease-" + branchID,
		LeaseTerm:             7,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", ownerID),
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
		LeaseSeconds:          3600,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim patch queue item: %v", err)
	}

	activeForeign, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get active foreign work next: %v", err)
	}
	if activeForeign.HasWork || activeForeign.Packet != nil || activeForeign.Reason != "none_available" {
		t.Fatalf("active foreign claim should remain invisible, got %+v", activeForeign)
	}

	// Expire the lease RECENTLY (within the RPF-58C reap grace) so the agent-driven claim
	// stewardship frontier surfaces it; the system reaper only force-releases claims expired
	// beyond the grace (covered separately by TestAgentWorkNextReapsExpiredPatchQueueClaim).
	recentlyExpired := time.Now().UTC().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE project_patch_queue_items SET claim_expires_at = ? WHERE queue_id = ? AND item_id = ?`, recentlyExpired, claimed.QueueID, claimed.ItemID); err != nil {
		t.Fatalf("expire patch queue claim: %v", err)
	}

	expiredForeign, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get expired foreign work next: %v", err)
	}
	if expiredForeign.HasWork || expiredForeign.Packet == nil || expiredForeign.Packet.PatchQueueClaim == nil {
		t.Fatalf("expected expired claim stewardship packet for eligible reviewer, got %+v", expiredForeign)
	}
	packet := expiredForeign.Packet.PatchQueueClaim
	if expiredForeign.Reason != "project_patch_queue_claim_stewardship_available" ||
		expiredForeign.ProjectLane != "review" ||
		expiredForeign.TaskKind != "COORDINATION" ||
		expiredForeign.Packet.ProjectLane != "review" ||
		expiredForeign.Packet.TaskKind != "COORDINATION" ||
		expiredForeign.Packet.PreferredTransition != "project_patch_queue_lifecycle" ||
		packet.QueueID != claimed.QueueID ||
		packet.ItemID != claimed.ItemID ||
		packet.ClaimedBy != integratorID ||
		packet.ClaimActive {
		t.Fatalf("unexpected expired claim packet: result=%+v packet=%+v", expiredForeign, packet)
	}
	if !containsString(packet.AllowedActions, "claim") || !containsString(packet.AllowedActions, "release") {
		t.Fatalf("expected reclaim/release actions for unbound expired claim, got %+v", packet.AllowedActions)
	}

	nonReviewer, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get builder work next: %v", err)
	}
	if nonReviewer.HasWork || nonReviewer.Packet != nil {
		t.Fatalf("builder should not receive patch queue claim stewardship, got %+v", nonReviewer)
	}

	selfExpired, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          integratorID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get self expired work next: %v", err)
	}
	if selfExpired.Packet == nil || selfExpired.Packet.PatchQueueClaim == nil || selfExpired.Packet.PatchQueueClaim.ClaimedBy != integratorID {
		t.Fatalf("expected original claimant to receive expired claim stewardship too, got %+v", selfExpired)
	}
}

func TestGetAgentWorkNextRoutesActiveClaimStewardshipTaskToClaimant(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID        = "ws-active-claim-stewardship-task"
		projectID          = "project-active-claim-stewardship-task"
		repoID             = "repo-active-claim-stewardship-task"
		leadID             = "alpha"
		ownerID            = "beta"
		claimantID         = "zeta"
		otherReviewerID    = "epsilon"
		branchID           = "branch-active-claim-stewardship"
		stewardshipTaskID  = "task-active-claim-stewardship"
		stewardshipTaskDoc = "Patch queue claim stewardship task created from agent.work.next frontier."
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, claimantID, otherReviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		RemoteURL:             "file:///tmp/" + repoID,
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindLocal,
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		DefaultBranch:         "main",
		IntegrationBranch:     "main",
		IsCanonical:           true,
		CreatedByAgentID:      leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.repository.upsert",
	}); err != nil {
		t.Fatalf("upsert project repository: %v", err)
	}
	for _, agentID := range []string{claimantID, otherReviewerID} {
		roleType := sqlite.ProjectRoleIntegrator
		if agentID == otherReviewerID {
			roleType = sqlite.ProjectRoleReviewer
		}
		if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			AgentID:               agentID,
			RoleType:              roleType,
			ActorID:               leadID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
			PromptContextSurface:  "project.role.assign",
		}); err != nil {
			t.Fatalf("assign project role to %s: %v", agentID, err)
		}
	}
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   "task-" + branchID,
		SessionID:                "session-" + branchID,
		RunID:                    "run-" + branchID,
		AgentID:                  ownerID,
		CapabilitySnapshotID:     "cap-" + branchID,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 `C:\fixtures\agents\` + ownerID + `\` + branchID,
		BaseTreeHash:             strings.Repeat("a", 40),
		BaseFileHashes: map[string]string{
			"src/app.go": "sha256:src",
		},
		RepoLeaseID:           "lease-" + branchID,
		LeaseTerm:             7,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", ownerID),
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
		LeaseSeconds:          3600,
		ActorID:               claimantID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", claimantID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim patch queue item: %v", err)
	}
	requiresGate := false
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "stewardship", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              stewardshipTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Resolve claimed patch queue item lifecycle",
		Description:         stewardshipTaskDoc + "\n\nqueue_id: " + claimed.QueueID + "\nitem_id: " + claimed.ItemID + "\nbranch_id: " + branch.BranchID + "\nclaimed_by: " + claimantID,
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "integration",
		RequiresProjectGate: requiresGate,
		Tags:                []string{"project", "patch-queue", "integration", "queue-stewardship", "claim-stewardship", "claimed-decision"},
	}, graph); err != nil {
		t.Fatalf("create claim stewardship task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      stewardshipTaskID,
		LinkedBy:    claimantID,
	}); err != nil {
		t.Fatalf("attach claim stewardship task: %v", err)
	}

	otherWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          otherReviewerID,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get other reviewer work next: %v", err)
	}
	if otherWork.HasWork || otherWork.Task != nil {
		t.Fatalf("active claim stewardship task should stay hidden from non-claimant reviewer, got %+v", otherWork)
	}

	claimantWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          claimantID,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get claimant work next: %v", err)
	}
	if !claimantWork.HasWork || claimantWork.Task == nil || claimantWork.Task.TaskID != stewardshipTaskID {
		t.Fatalf("claimant should receive active claim stewardship task, got %+v", claimantWork)
	}
}

func TestProjectRoleScopeTaskRoutesToStrategicLead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-role-scope-lead-routing"
		projectID   = "project-role-scope-routing"
		leadID      = "alpha"
		builderID   = "beta"
		taskID      = "task-role-scope-builder"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         builderID,
		Priority:            "high",
		Title:               "Resolve project role/scope request for beta",
		Description:         "# Strategic Lead Role/Scope Request\n\nProject role/scope correction requested by beta.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "integration",
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: false,
		Tags:                []string{"project-role-scope", "strategic-lead", "coordination"},
	}, graph); err != nil {
		t.Fatalf("create role/scope task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    builderID,
	}); err != nil {
		t.Fatalf("attach role/scope task: %v", err)
	}

	builderWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get builder work next: %v", err)
	}
	if builderWork.HasWork || builderWork.Reason != "none_available" || builderWork.Packet != nil {
		t.Fatalf("expected builder autonomous selection to skip lead-owned role/scope task, got %+v", builderWork)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     builderID,
		Summary:     "builder should not claim lead-owned role/scope task",
	}); err == nil || !strings.Contains(err.Error(), "role/scope coordination task") {
		t.Fatalf("expected builder role/scope claim to be rejected, got %v", err)
	}

	leadWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get lead work next: %v", err)
	}
	if !leadWork.HasWork || leadWork.Task == nil || leadWork.Task.TaskID != taskID {
		t.Fatalf("expected strategic lead to receive role/scope task, got %+v", leadWork)
	}
	if leadWork.Packet == nil || leadWork.Packet.PreferredTransition != "project_role_assign" {
		t.Fatalf("expected project_role_assign packet, got %+v", leadWork.Packet)
	}
	if leadWork.Packet.Gate == nil || leadWork.Packet.Gate.GateType != "project_role_scope_authority_transition" || leadWork.Packet.Gate.NeededFrom != "project_role_assign" {
		t.Fatalf("expected authority transition gate, got %+v", leadWork.Packet)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     leadID,
		Summary:     "lead resolves role/scope request",
	}); err != nil {
		t.Fatalf("claim role/scope task by lead: %v", err)
	}
}

func TestProjectRoleScopeTaskPreemptsActiveLeadRootSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-role-scope-preempts-root"
		projectID   = "project-role-scope-preempts-root"
		leadID      = "alpha"
		builderID   = "gamma"
		rootTaskID  = "task-project-root"
		scopeTaskID = "task-role-scope-gamma"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              rootTaskID,
		OwnerUserID:         leadID,
		Priority:            "high",
		Title:               "Autonomous coordinator root task",
		Description:         "Coordinate builders, task decomposition, and project strategy.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "project",
		ProjectID:           projectID,
		ProjectLane:         "strategy",
		RequiresProjectGate: false,
		Tags:                []string{"project-root", "strategy"},
	}, graph); err != nil {
		t.Fatalf("create root task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      rootTaskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach root task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      rootTaskID,
		AgentID:     leadID,
		Summary:     "active project root coordination",
	}); err != nil {
		t.Fatalf("claim root task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-alpha-root",
		AgentID:     leadID,
		WorkspaceID: workspaceID,
		TaskID:      rootTaskID,
		StartedAt:   "2026-05-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create root session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.start",
		WorkspaceID: workspaceID,
		SessionID:   "sess-alpha-root",
		AgentID:     leadID,
		TaskID:      rootTaskID,
		Summary:     "Active root lane",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record root session coordination: %v", err)
	}

	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              scopeTaskID,
		OwnerUserID:         builderID,
		Priority:            "high",
		Title:               "Resolve project role/scope request for gamma",
		Description:         "# Strategic Lead Role/Scope Request\n\n- requested_role_type: IMPLEMENTER\n- requested_write_scope_json: {\"paths\":[\"src/**\",\"tests/**\",\"package.json\"]}\n\n## Required Lead Action\nRun project_role_assign if valid.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: false,
		Tags:                []string{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
	}, graph); err != nil {
		t.Fatalf("create role/scope task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      scopeTaskID,
		LinkedBy:    builderID,
	}); err != nil {
		t.Fatalf("attach role/scope task: %v", err)
	}

	leadWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get lead work next: %v", err)
	}
	if !leadWork.HasWork || leadWork.Task == nil || leadWork.Task.TaskID != scopeTaskID {
		t.Fatalf("expected role/scope authority task to preempt active root session, got %+v", leadWork)
	}
	if leadWork.Reason != "project_role_scope_authority_transition" {
		t.Fatalf("reason=%q, want project_role_scope_authority_transition", leadWork.Reason)
	}
	if leadWork.Packet == nil || leadWork.Packet.PreferredTransition != "project_role_assign" {
		t.Fatalf("expected project_role_assign packet, got %+v", leadWork.Packet)
	}
	if leadWork.Packet.Gate == nil || leadWork.Packet.Gate.GateType != "project_role_scope_authority_transition" || leadWork.Packet.Gate.NeededFrom != "project_role_assign" {
		t.Fatalf("expected authority transition gate, got %+v", leadWork.Packet)
	}
	if leadWork.Session != nil || leadWork.SessionAction != "start_new" {
		t.Fatalf("expected controlled authority task start, session=%+v action=%q", leadWork.Session, leadWork.SessionAction)
	}
}

func TestProjectClaimRepairTaskPreemptsActiveLeadRootSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-claim-repair-preempts-root"
		projectID    = "project-claim-repair-preempts-root"
		leadID       = "alpha"
		builderID    = "beta"
		rootTaskID   = "task-project-root"
		repairTaskID = "task-project-claim-repair-beta"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              rootTaskID,
		OwnerUserID:         leadID,
		Priority:            "high",
		Title:               "Autonomous coordinator root task",
		Description:         "Coordinate builders, task decomposition, and project strategy.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "project",
		ProjectID:           projectID,
		ProjectLane:         "strategy",
		RequiresProjectGate: false,
		Tags:                []string{"project-root", "strategy"},
	}, graph); err != nil {
		t.Fatalf("create root task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      rootTaskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach root task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      rootTaskID,
		AgentID:     leadID,
		Summary:     "active project root coordination",
	}); err != nil {
		t.Fatalf("claim root task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-alpha-root",
		AgentID:     leadID,
		WorkspaceID: workspaceID,
		TaskID:      rootTaskID,
		StartedAt:   "2026-05-24T00:00:00Z",
	}); err != nil {
		t.Fatalf("create root session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.start",
		WorkspaceID: workspaceID,
		SessionID:   "sess-alpha-root",
		AgentID:     leadID,
		TaskID:      rootTaskID,
		Summary:     "Active root lane",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record root session coordination: %v", err)
	}

	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              repairTaskID,
		OwnerUserID:         builderID,
		Priority:            "high",
		Title:               "Repair project claim scope conflict",
		Description:         "Claim repair for a write-scope conflict blocking an implementation lane. Strategic lead must inspect project/claim/branch evidence and mutate durable state or sequence dependencies.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "strategy",
		RequiresProjectGate: false,
		Tags:                []string{"project-claim-repair", "strategic-lead", "coordination", "blocker-unblock"},
	}, graph); err != nil {
		t.Fatalf("create claim repair task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		LinkedBy:    builderID,
	}); err != nil {
		t.Fatalf("attach claim repair task: %v", err)
	}

	leadWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get lead work next: %v", err)
	}
	if !leadWork.HasWork || leadWork.Task == nil || leadWork.Task.TaskID != repairTaskID {
		t.Fatalf("expected claim-repair authority task to preempt active root session, got %+v", leadWork)
	}
	if leadWork.Reason != "project_claim_repair_authority_transition" {
		t.Fatalf("reason=%q, want project_claim_repair_authority_transition", leadWork.Reason)
	}
	if leadWork.Packet == nil || leadWork.Packet.Gate == nil || leadWork.Packet.Gate.GateType != "project_claim_repair_authority_transition" {
		t.Fatalf("expected claim-repair authority packet, got %+v", leadWork.Packet)
	}
	if leadWork.Packet.PreferredTransition != "project_claim_repair_receipt" ||
		leadWork.Packet.Gate == nil ||
		leadWork.Packet.Gate.NeededFrom != "project_claim_repair_receipt" ||
		leadWork.Session != nil ||
		leadWork.SessionAction != "start_new" {
		t.Fatalf("expected controlled claim-repair task start, packet=%+v session=%+v action=%q", leadWork.Packet, leadWork.Session, leadWork.SessionAction)
	}
}

func TestProjectClaimRepairTaskPreemptsPendingLeadRootSelection(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-claim-repair-preempts-pending-root"
		projectID    = "project-claim-repair-preempts-pending-root"
		leadID       = "alpha"
		builderID    = "beta"
		rootTaskID   = "task-project-root"
		repairTaskID = "task-project-claim-repair-beta"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, input := range []sqlite.TaskCreateInput{
		{
			WorkspaceID:         workspaceID,
			TaskID:              rootTaskID,
			OwnerUserID:         leadID,
			Priority:            "high",
			Title:               "Autonomous coordinator root task",
			Description:         "Coordinate builders, task decomposition, and project strategy.",
			TaskKind:            "COORDINATION",
			TaskTemplate:        "project",
			ProjectID:           projectID,
			ProjectLane:         "strategy",
			RequiresProjectGate: false,
			Tags:                []string{"project-root", "strategy"},
		},
		{
			WorkspaceID:         workspaceID,
			TaskID:              repairTaskID,
			OwnerUserID:         builderID,
			Priority:            "high",
			Title:               "Repair project claim scope conflict",
			Description:         "Claim repair for a write-scope conflict blocking an implementation lane. Strategic lead must inspect project/claim/branch evidence and mutate durable state or sequence dependencies.",
			TaskKind:            "COORDINATION",
			TaskTemplate:        "generic",
			ProjectID:           projectID,
			ProjectLane:         "strategy",
			RequiresProjectGate: false,
			Tags:                []string{"project-claim-repair", "strategic-lead", "coordination", "blocker-unblock"},
		},
	} {
		if err := store.CreateTaskWithGraph(ctx, input, graph); err != nil {
			t.Fatalf("create task %s: %v", input.TaskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      input.TaskID,
			LinkedBy:    input.OwnerUserID,
		}); err != nil {
			t.Fatalf("attach task %s: %v", input.TaskID, err)
		}
	}

	leadWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get lead work next: %v", err)
	}
	if !leadWork.HasWork || leadWork.Task == nil || leadWork.Task.TaskID != repairTaskID {
		t.Fatalf("expected claim-repair authority task to preempt pending root selection, got %+v", leadWork)
	}
	if leadWork.Reason != "project_claim_repair_authority_transition" {
		t.Fatalf("reason=%q, want project_claim_repair_authority_transition", leadWork.Reason)
	}
	if leadWork.Packet == nil || leadWork.Packet.Gate == nil || leadWork.Packet.Gate.GateType != "project_claim_repair_authority_transition" {
		t.Fatalf("expected claim-repair authority packet, got %+v", leadWork.Packet)
	}
	if leadWork.Packet.PreferredTransition != "project_claim_repair_receipt" || leadWork.Packet.Gate.NeededFrom != "project_claim_repair_receipt" {
		t.Fatalf("expected claim-repair receipt requirement, got %+v", leadWork.Packet)
	}
}

func TestProjectClaimRepairTaskPreemptsTriggeredLeadRootSelection(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-claim-repair-preempts-triggered-root"
		projectID    = "project-claim-repair-preempts-triggered-root"
		leadID       = "alpha"
		builderID    = "beta"
		rootTaskID   = "task-project-root"
		repairTaskID = "task-project-claim-repair-beta"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, input := range []sqlite.TaskCreateInput{
		{
			WorkspaceID:         workspaceID,
			TaskID:              rootTaskID,
			OwnerUserID:         leadID,
			Priority:            "high",
			Title:               "Autonomous coordinator root task",
			Description:         "Coordinate builders, task decomposition, and project strategy.",
			TaskKind:            "COORDINATION",
			TaskTemplate:        "project",
			ProjectID:           projectID,
			ProjectLane:         "strategy",
			RequiresProjectGate: false,
			Tags:                []string{"project-root", "strategy"},
		},
		{
			WorkspaceID:         workspaceID,
			TaskID:              repairTaskID,
			OwnerUserID:         builderID,
			Priority:            "high",
			Title:               "Repair project claim scope conflict",
			Description:         "Claim repair for a write-scope conflict blocking an implementation lane. Strategic lead must inspect project/claim/branch evidence and mutate durable state or sequence dependencies.",
			TaskKind:            "COORDINATION",
			TaskTemplate:        "generic",
			ProjectID:           projectID,
			ProjectLane:         "strategy",
			RequiresProjectGate: false,
			Tags:                []string{"project-claim-repair", "strategic-lead", "coordination", "blocker-unblock"},
		},
	} {
		if err := store.CreateTaskWithGraph(ctx, input, graph); err != nil {
			t.Fatalf("create task %s: %v", input.TaskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      input.TaskID,
			LinkedBy:    input.OwnerUserID,
		}); err != nil {
			t.Fatalf("attach task %s: %v", input.TaskID, err)
		}
	}

	leadWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  rootTaskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get triggered lead work next: %v", err)
	}
	if !leadWork.HasWork || leadWork.Task == nil || leadWork.Task.TaskID != repairTaskID {
		t.Fatalf("expected claim-repair authority task to preempt triggered root selection, got %+v", leadWork)
	}
	if leadWork.Reason != "project_claim_repair_authority_transition" {
		t.Fatalf("reason=%q, want project_claim_repair_authority_transition", leadWork.Reason)
	}
	if leadWork.Packet == nil || leadWork.Packet.Gate == nil ||
		leadWork.Packet.PreferredTransition != "project_claim_repair_receipt" ||
		leadWork.Packet.Gate.NeededFrom != "project_claim_repair_receipt" {
		t.Fatalf("expected claim-repair receipt requirement, got %+v", leadWork.Packet)
	}
}

func TestTriggeredProjectClaimRepairTaskClaimAllowsRecoverableExpiredLead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-claim-repair-expired-lead-claim"
		projectID    = "project-claim-repair-expired-lead-claim"
		leadID       = "alpha"
		builderID    = "beta"
		repairTaskID = "task-project-claim-repair-expired-lead"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	updated, err := store.DB().ExecContext(ctx, `
UPDATE project_agent_roles
   SET status = ?, lease_expires_at = ?, updated_at = ?
 WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND role_type = ?`,
		sqlite.ProjectRoleStatusExpired,
		"2000-01-01T00:00:00Z",
		"2026-06-05T00:00:00Z",
		workspaceID,
		projectID,
		leadID,
		sqlite.ProjectRoleStrategicLead)
	if err != nil {
		t.Fatalf("expire strategic lead role: %v", err)
	}
	if rows, err := updated.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("expected one expired strategic lead row, rows=%d err=%v", rows, err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              repairTaskID,
		OwnerUserID:         builderID,
		Priority:            "high",
		Title:               "Project claim repair for downstream role scope",
		Description:         "Dedicated authority-bearing project claim repair task.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: false,
		Tags:                []string{"project-claim-repair", "strategic-lead", "coordination", "blocker-unblock"},
	}, graph); err != nil {
		t.Fatalf("create claim repair task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		LinkedBy:    builderID,
	}); err != nil {
		t.Fatalf("attach claim repair task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  repairTaskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get triggered claim repair work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != repairTaskID {
		t.Fatalf("recoverable expired lead should receive claim-repair carrier, got %+v", result)
	}
	if result.Reason != "project_claim_repair_authority_transition" {
		t.Fatalf("reason=%q, want project_claim_repair_authority_transition", result.Reason)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		AgentID:     leadID,
		Summary:     "claim recoverable claim-repair authority carrier",
	}); err != nil {
		t.Fatalf("claim repair task selected by work.next must be claimable by the same authority guard: %v", err)
	}
}

func TestProjectRoleScopeTaskRequirementsInferProjectRoleAssignPacket(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-role-scope-requirements-routing"
		projectID   = "project-role-scope-requirements-routing"
		leadID      = "alpha"
		builderID   = "beta"
		taskID      = "task-authority-transition-beta"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		OwnerUserID:          builderID,
		Priority:             "high",
		Title:                "Apply beta authority transition",
		Description:          "Boundary transition executor task. Required transition: project_role_assign.",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		RequiresProjectGate:  false,
		Tags:                 []string{"project-role-scope", "authority-transition", "strategic-lead", "coordination"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign","target_agent_id":"beta"}`,
	}, graph); err != nil {
		t.Fatalf("create role/scope requirements task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    builderID,
	}); err != nil {
		t.Fatalf("attach role/scope requirements task: %v", err)
	}

	leadWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get lead work next: %v", err)
	}
	if !leadWork.HasWork || leadWork.Task == nil || leadWork.Task.TaskID != taskID {
		t.Fatalf("expected requirements-marked authority task, got %+v", leadWork)
	}
	if leadWork.Packet == nil || leadWork.Packet.PreferredTransition != "project_role_assign" {
		t.Fatalf("expected project_role_assign packet from task_requirements_json, got %+v", leadWork.Packet)
	}
	if leadWork.Packet.Gate == nil || leadWork.Packet.Gate.NeededFrom != "project_role_assign" {
		t.Fatalf("expected project_role_assign authority gate, got %+v", leadWork.Packet)
	}
}

func TestAgentWorkNextTerminalizesReleasedRoleScopeCarrierWithExistingReceipt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-role-scope-receipt-terminalization"
		projectID   = "project-role-scope-receipt-terminalization"
		leadID      = "alpha"
		builderID   = "beta"
		taskID      = "task-authority-transition-beta-terminalize"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, builderID, sqlite.ProjectRoleIntegrator, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		OwnerUserID:          builderID,
		Priority:             "high",
		Title:                "Apply beta authority transition",
		Description:          "Boundary transition executor task.",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		RequiresProjectGate:  false,
		Tags:                 []string{"project-role-scope", "authority-transition", "strategic-lead", "coordination"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign","target_agent_id":"beta","role_type":"INTEGRATOR"}`,
	}, graph); err != nil {
		t.Fatalf("create stale role/scope carrier: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    builderID,
	}); err != nil {
		t.Fatalf("attach stale role/scope carrier: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		taskID,
		workspaceID,
		leadID,
		model.TaskClaimStatusReleased,
		"released stale carrier",
		"2026-06-10T00:00:00Z",
		"2026-06-10T00:05:00Z",
		"2026-06-10T00:05:00Z",
	); err != nil {
		t.Fatalf("seed released stale carrier claim: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
		Trigger:          "request_resume",
		CandidateTaskID:  taskID,
	})
	if err != nil {
		t.Fatalf("get work after receipt terminalization: %v", err)
	}
	if work.HasWork && work.Task != nil && work.Task.TaskID == taskID {
		t.Fatalf("stale carrier was still selected after receipt terminalization: %+v", work)
	}

	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	var carrier sqlite.WorkspaceTaskRecord
	for _, task := range tasks {
		if task.TaskID == taskID {
			carrier = task
			break
		}
	}
	if carrier.TaskID == "" {
		t.Fatalf("carrier not found after terminalization")
	}
	if carrier.Status != model.TaskStatusResolved {
		t.Fatalf("carrier status=%s, want RESOLVED", carrier.Status)
	}
	if carrier.ClaimStatus == nil || *carrier.ClaimStatus != model.TaskClaimStatusCompleted {
		t.Fatalf("carrier claim status=%v, want COMPLETED", carrier.ClaimStatus)
	}
	var eventCount int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM runtime_events
 WHERE workspace_id = ?
   AND task_id = ?
   AND event_type = 'task.completed'
   AND payload_json LIKE '%required_receipt_sweep%'
   AND payload_json LIKE '%project_role_assign_receipt%'`,
		workspaceID,
		taskID,
	).Scan(&eventCount); err != nil {
		t.Fatalf("count receipt terminalization event: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("receipt terminalization event count=%d, want 1", eventCount)
	}
}

func TestAgentWorkNextTerminalizesBlockedSideEffectClassifierWithCompletedSuccessor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-side-effect-classifier-receipt-terminalization"
		projectID   = "project-side-effect-classifier-receipt-terminalization"
		leadID      = "epsilon"
		verifierID  = "zeta"
		parentID    = "task-side-effect-classify-receipt-terminalize"
		successorID = "task-side-effect-verify-receipt-terminalize"
		sideEffect  = "side-effect:rhizome-main:project-side-effect-classifier-receipt-terminalization:branch-readme:readme.md"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, verifierID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               parentID,
		OwnerUserID:          leadID,
		Priority:             "high",
		Title:                "Classify side effect",
		Description:          "Classify README side effect.",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		TaskClass:            "INCIDENT",
		TaskClassSource:      "EXPLICIT",
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		RequiresProjectGate:  false,
		Tags:                 []string{"side-effect-classification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification","active_task_id":"task-implementation","branch_id":"branch-readme","side_effect_refs":["` + sideEffect + `"],"dirty_paths":["README.md"]}`,
	}, graph); err != nil {
		t.Fatalf("create parent classifier task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      parentID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach parent classifier task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      parentID,
		AgentID:     leadID,
		Summary:     "classifying side effect",
	}); err != nil {
		t.Fatalf("claim parent classifier task: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID: workspaceID,
		TaskID:      parentID,
		AgentID:     leadID,
		Reason:      "waiting_on_side_effect_resolution_successors:" + successorID,
	}); err != nil {
		t.Fatalf("block parent classifier task: %v", err)
	}

	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               successorID,
		OwnerUserID:          verifierID,
		Priority:             "high",
		Title:                "Verify side effect bucket",
		Description:          "ABPC recovery action: verify bucket and call side_effect_resolve.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		TaskClass:            "PROOF",
		TaskClassSource:      "EXPLICIT",
		ProjectID:            projectID,
		ProjectLane:          "verification",
		RequiresProjectGate:  false,
		Tags:                 []string{"side-effect-resolution", "verification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_verification","action_kind":"verify_bucket","decision":"request_verification","active_task_id":"` + parentID + `","branch_id":"branch-readme","side_effect_refs":["` + sideEffect + `"],"path_bucket":["README.md"]}`,
	}, graph); err != nil {
		t.Fatalf("create completed ABPC successor task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      successorID,
		LinkedBy:    verifierID,
	}); err != nil {
		t.Fatalf("attach completed ABPC successor task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      successorID,
		AgentID:     verifierID,
		Summary:     "verifying side-effect bucket",
	}); err != nil {
		t.Fatalf("claim completed ABPC successor task: %v", err)
	}
	if err := store.CompleteTask(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      successorID,
		AgentID:     verifierID,
		Summary:     "Recorded the side-effect resolution as request_verification.",
	}); err != nil {
		t.Fatalf("complete ABPC successor task: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
		Trigger:          "request_resume",
		CandidateTaskID:  parentID,
	})
	if err != nil {
		t.Fatalf("get work after ABPC successor receipt terminalization: %v", err)
	}
	if work.HasWork && work.Task != nil && work.Task.TaskID == parentID {
		t.Fatalf("stale side-effect classifier was still selected after successor receipt terminalization: %+v", work)
	}

	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	var parent sqlite.WorkspaceTaskRecord
	for _, task := range tasks {
		if task.TaskID == parentID {
			parent = task
			break
		}
	}
	if parent.TaskID == "" {
		t.Fatalf("parent classifier not found after terminalization")
	}
	if parent.Status != model.TaskStatusResolved {
		t.Fatalf("parent classifier status=%s, want RESOLVED", parent.Status)
	}
	if parent.ClaimStatus == nil || *parent.ClaimStatus != model.TaskClaimStatusCompleted {
		t.Fatalf("parent classifier claim status=%v, want COMPLETED", parent.ClaimStatus)
	}
	var eventCount int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM runtime_events
 WHERE workspace_id = ?
   AND task_id = ?
   AND event_type = 'task.completed'
   AND payload_json LIKE '%required_receipt_sweep%'
   AND payload_json LIKE '%abpc_side_effect_successor_terminal_receipt%'
   AND payload_json LIKE ?`,
		workspaceID,
		parentID,
		"%"+successorID+"%",
	).Scan(&eventCount); err != nil {
		t.Fatalf("count ABPC receipt terminalization event: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("ABPC receipt terminalization event count=%d, want 1", eventCount)
	}
}

func TestAgentWorkNextKeepsSideEffectClassifierOpenWithPendingSuccessor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-side-effect-classifier-pending-successor"
		projectID   = "project-side-effect-classifier-pending-successor"
		leadID      = "epsilon"
		verifierID  = "zeta"
		parentID    = "task-side-effect-classify-pending-successor"
		successorID = "task-side-effect-verify-pending-successor"
		sideEffect  = "side-effect:rhizome-main:project-side-effect-classifier-pending-successor:branch-readme:readme.md"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, verifierID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               parentID,
		OwnerUserID:          leadID,
		Priority:             "high",
		Title:                "Classify side effect",
		Description:          "Classify README side effect.",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		TaskClass:            "INCIDENT",
		TaskClassSource:      "EXPLICIT",
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		RequiresProjectGate:  false,
		Tags:                 []string{"side-effect-classification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_classification_task.v1","abpc_task_class":"side_effect_classification","active_task_id":"task-implementation","branch_id":"branch-readme","side_effect_refs":["` + sideEffect + `"],"dirty_paths":["README.md"]}`,
	}, graph); err != nil {
		t.Fatalf("create parent classifier task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      parentID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach parent classifier task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      parentID,
		AgentID:     leadID,
		Summary:     "classifying side effect",
	}); err != nil {
		t.Fatalf("claim parent classifier task: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID: workspaceID,
		TaskID:      parentID,
		AgentID:     leadID,
		Reason:      "waiting_on_side_effect_resolution_successors:" + successorID,
	}); err != nil {
		t.Fatalf("block parent classifier task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               successorID,
		OwnerUserID:          verifierID,
		Priority:             "high",
		Title:                "Verify side effect bucket",
		Description:          "ABPC recovery action still pending.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		TaskClass:            "PROOF",
		TaskClassSource:      "EXPLICIT",
		ProjectID:            projectID,
		ProjectLane:          "verification",
		RequiresProjectGate:  false,
		Tags:                 []string{"side-effect-resolution", "verification", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_verification","action_kind":"verify_bucket","decision":"request_verification","active_task_id":"` + parentID + `","branch_id":"branch-readme","side_effect_refs":["` + sideEffect + `"],"path_bucket":["README.md"]}`,
	}, graph); err != nil {
		t.Fatalf("create pending ABPC successor task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      successorID,
		LinkedBy:    verifierID,
	}); err != nil {
		t.Fatalf("attach pending ABPC successor task: %v", err)
	}

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
		Trigger:          "request_resume",
		CandidateTaskID:  parentID,
	}); err != nil {
		t.Fatalf("get work with pending ABPC successor: %v", err)
	}

	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	for _, task := range tasks {
		if task.TaskID == parentID && task.Status == model.TaskStatusResolved {
			t.Fatalf("parent classifier should remain open while successor is pending: %+v", task)
		}
	}
}

func TestAgentWorkNextTerminalizesReleasedTaskWithIntegratedPatchQueueReceipt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-integrated-patchq-task-terminalization"
		projectID    = "project-integrated-patchq-task-terminalization"
		repoID       = "repo-integrated-patchq-task-terminalization"
		leadID       = "alpha"
		builderID    = "beta"
		reviewerID   = "epsilon"
		integratorID = "zeta"
		taskID       = "task-product-lane-already-integrated"
		branchID     = "branch-product-lane-already-integrated"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               builderID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign builder implementation role: %v", err)
	}
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t,
		ctx,
		store,
		workspaceID,
		projectID,
		taskID,
		"Add acceptance matrix",
		"Lane-owned implementation task whose patch queue candidate has already been integrated.",
		"implementation",
		[]string{"rq", "implementation"},
		true,
	)
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, builderID, branchID)
	branch = rebindReadyBranchActiveTaskForTest(t, ctx, store, branch, taskID, builderID, `{"paths":["src/**"]}`)

	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-" + taskID,
		RunID:                    "run-" + taskID,
		AgentID:                  builderID,
		CapabilitySnapshotID:     "cap-" + taskID,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 `C:\fixtures\agents\beta\` + branchID,
		BaseTreeHash:             strings.Repeat("a", 40),
		BaseFileHashes:           agentWorkTestBaseFileHashesForScope(`{"paths":["src/**"]}`),
		RepoLeaseID:              "lease-" + taskID,
		LeaseTerm:                7,
		ActorID:                  builderID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", builderID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit linked patch queue item: %v", err)
	}
	markAgentWorkTaskReleasedWithBranch(t, ctx, store, workspaceID, taskID, builderID, repoID, branch.BranchID)
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
		t.Fatalf("claim linked patch queue item: %v", err)
	}
	accepted, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateAccepted,
		DecisionSummary:       "Accepted lane-scoped candidate.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("accept linked patch queue item: %v", err)
	}
	integrated, _, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               accepted.QueueID,
		ItemID:                accepted.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetBranch:          "main",
		TargetHeadBefore:      strings.Repeat("c", 40),
		TargetHeadAfter:       strings.Repeat("d", 40),
		RemoteTargetHeadAfter: strings.Repeat("d", 40),
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		PushAttempted:         true,
		PushSucceeded:         true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.integration_record", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.integration_record",
	})
	if err != nil {
		t.Fatalf("record integrated receipt: %v", err)
	}
	if integrated.State != sqlite.ProjectPatchQueueStateIntegrated || integrated.TaskID != taskID {
		t.Fatalf("unexpected integrated receipt item: %+v", integrated)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
		Trigger:          "request_resume",
		CandidateTaskID:  taskID,
	})
	if err != nil {
		t.Fatalf("get work after integrated receipt terminalization: %v", err)
	}
	if work.HasWork && work.Task != nil && work.Task.TaskID == taskID {
		t.Fatalf("integrated task was still selected after receipt terminalization: %+v", work)
	}

	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	var got sqlite.WorkspaceTaskRecord
	for _, task := range tasks {
		if task.TaskID == taskID {
			got = task
			break
		}
	}
	if got.TaskID == "" {
		t.Fatalf("task not found after terminalization")
	}
	if got.Status != model.TaskStatusResolved {
		t.Fatalf("task status=%s, want RESOLVED", got.Status)
	}
	if got.ClaimStatus == nil || *got.ClaimStatus != model.TaskClaimStatusCompleted {
		t.Fatalf("task claim status=%v, want COMPLETED", got.ClaimStatus)
	}
	var eventCount int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM runtime_events
 WHERE workspace_id = ?
   AND task_id = ?
   AND event_type = 'task.completed'
   AND payload_json LIKE '%required_receipt_sweep%'
   AND payload_json LIKE '%integrated_patch_queue_task_receipt%'`,
		workspaceID,
		taskID,
	).Scan(&eventCount); err != nil {
		t.Fatalf("count integrated receipt terminalization event: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("receipt terminalization event count=%d, want 1", eventCount)
	}
}

func TestAgentWorkNextTerminalizesClaimedImplementationTaskWithAcceptedPatchQueueReceipt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-accepted-patchq-claimed-terminalization"
		projectID   = "project-accepted-patchq-claimed-terminalization"
		repoID      = "repo-accepted-patchq-claimed-terminalization"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "epsilon"
		taskID      = "task-product-lane-already-accepted"
		branchID    = "branch-product-lane-already-accepted"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               builderID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign builder implementation role: %v", err)
	}
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t,
		ctx,
		store,
		workspaceID,
		projectID,
		taskID,
		"Add acceptance matrix",
		"Lane-owned implementation task whose patch queue candidate has already been accepted.",
		"implementation",
		[]string{"rq", "implementation"},
		true,
	)
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, builderID, branchID)
	branch = rebindReadyBranchActiveTaskForTest(t, ctx, store, branch, taskID, builderID, `{"paths":["src/**"]}`)
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT OR REPLACE INTO task_claims(
  task_id, workspace_id, agent_id, claim_status, summary, claimed_at, updated_at,
  project_role_id, repo_id, checkout_id, branch_id, write_scope_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		taskID,
		workspaceID,
		builderID,
		model.TaskClaimStatusClaimed,
		"builder is still live after publishing the review-ready candidate",
		"2026-06-12T23:52:00Z",
		"2026-06-12T23:52:00Z",
		"",
		repoID,
		"",
		branch.BranchID,
		`{"paths":["src/**"]}`,
	); err != nil {
		t.Fatalf("seed claimed implementation residue: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ?`,
		model.TaskStatusRunning,
		"2026-06-12T23:52:00Z",
		taskID,
	); err != nil {
		t.Fatalf("mark implementation residue running: %v", err)
	}

	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-" + taskID,
		RunID:                    "run-" + taskID,
		AgentID:                  builderID,
		CapabilitySnapshotID:     "cap-" + taskID,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 `C:\fixtures\agents\beta\` + branchID,
		BaseTreeHash:             strings.Repeat("a", 40),
		BaseFileHashes:           agentWorkTestBaseFileHashesForScope(`{"paths":["src/**"]}`),
		RepoLeaseID:              "lease-" + taskID,
		LeaseTerm:                7,
		ActorID:                  builderID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", builderID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit linked patch queue item: %v", err)
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
		t.Fatalf("claim linked patch queue item: %v", err)
	}
	accepted, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateAccepted,
		DecisionSummary:       "Accepted lane-scoped candidate.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("accept linked patch queue item: %v", err)
	}
	if accepted.State != sqlite.ProjectPatchQueueStateAccepted || accepted.TaskID != taskID {
		t.Fatalf("unexpected accepted receipt item: %+v", accepted)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
		Trigger:          "request_resume",
		CandidateTaskID:  taskID,
	})
	if err != nil {
		t.Fatalf("get work after accepted receipt terminalization: %v", err)
	}
	if work.HasWork && work.Task != nil && work.Task.TaskID == taskID {
		t.Fatalf("accepted implementation task was still selected after receipt terminalization: %+v", work)
	}

	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	var got sqlite.WorkspaceTaskRecord
	for _, task := range tasks {
		if task.TaskID == taskID {
			got = task
			break
		}
	}
	if got.TaskID == "" {
		t.Fatalf("task not found after terminalization")
	}
	if got.Status != model.TaskStatusResolved {
		t.Fatalf("task status=%s, want RESOLVED", got.Status)
	}
	if got.ClaimStatus == nil || *got.ClaimStatus != model.TaskClaimStatusCompleted {
		t.Fatalf("task claim status=%v, want COMPLETED", got.ClaimStatus)
	}
	var eventCount int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM runtime_events
 WHERE workspace_id = ?
   AND task_id = ?
   AND event_type = 'task.completed'
   AND payload_json LIKE '%required_receipt_sweep%'
   AND payload_json LIKE '%accepted_patch_queue_task_receipt%'`,
		workspaceID,
		taskID,
	).Scan(&eventCount); err != nil {
		t.Fatalf("count accepted receipt terminalization event: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("receipt terminalization event count=%d, want 1", eventCount)
	}
}

func TestTriggeredProjectRoleScopeTaskBypassesReviewProfileForExpiredLead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-triggered-role-scope-expired-lead"
		projectID   = "project-triggered-role-scope-expired-lead"
		leadID      = "alpha"
		requesterID = "iota"
		taskID      = "task-role-scope-iota"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, requesterID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	updated, err := store.DB().ExecContext(ctx, `
UPDATE project_agent_roles
   SET status = ?, lease_expires_at = ?, updated_at = ?
 WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND role_type = ?`,
		sqlite.ProjectRoleStatusExpired,
		"2000-01-01T00:00:00Z",
		"2026-06-04T00:00:00Z",
		workspaceID,
		projectID,
		leadID,
		sqlite.ProjectRoleStrategicLead)
	if err != nil {
		t.Fatalf("expire strategic lead role: %v", err)
	}
	if rows, err := updated.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("expected one expired strategic lead row, rows=%d err=%v", rows, err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        leadID,
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert review-profile lead: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		OwnerUserID:          requesterID,
		Priority:             "high",
		Title:                "Apply iota role/scope authority transition",
		Description:          "# Strategic Lead Role/Scope Request\n\nIota needs project_role_assign for the rq parser lane.",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		RequiresProjectGate:  false,
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign","target_agent_id":"iota"}`,
	}, graph); err != nil {
		t.Fatalf("create role/scope authority task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    requesterID,
	}); err != nil {
		t.Fatalf("attach role/scope authority task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get triggered role/scope work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != taskID {
		t.Fatalf("expected expired lead review profile to receive role/scope authority task, got %+v", result)
	}
	if result.Reason != "project_role_scope_authority_transition" {
		t.Fatalf("reason=%q, want project_role_scope_authority_transition", result.Reason)
	}
	if result.ProfileGateReason == "profile_task_mode_mismatch" {
		t.Fatalf("authority role/scope task should bypass ordinary review profile gate, got %+v", result)
	}
	if result.Packet == nil || result.Packet.PreferredTransition != "project_role_assign" {
		t.Fatalf("expected project_role_assign packet, got %+v", result.Packet)
	}
	if result.Packet.Gate == nil || result.Packet.Gate.NeededFrom != "project_role_assign" {
		t.Fatalf("expected project_role_assign authority gate, got %+v", result.Packet)
	}
}

func TestTriggeredProjectRoleScopeTaskDoesNotBypassReviewProfileForNonLead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-triggered-role-scope-nonlead"
		projectID   = "project-triggered-role-scope-nonlead"
		leadID      = "alpha"
		reviewerID  = "iota"
		taskID      = "task-role-scope-iota-nonlead"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Specialization: "review",
		Tags:           []string{"reviewer", "qa"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		OwnerUserID:          reviewerID,
		Priority:             "high",
		Title:                "Apply iota role/scope authority transition",
		Description:          "# Strategic Lead Role/Scope Request\n\nIota needs project_role_assign for the rq parser lane.",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		RequiresProjectGate:  false,
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign","target_agent_id":"iota"}`,
	}, graph); err != nil {
		t.Fatalf("create role/scope authority task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    reviewerID,
	}); err != nil {
		t.Fatalf("attach role/scope authority task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get triggered nonlead role/scope work next: %v", err)
	}
	if result.HasWork {
		t.Fatalf("non-lead review profile should not receive strategic role/scope authority task, got %+v", result)
	}
	if result.Reason != "trigger_no_work" || result.ProfileGateReason != "profile_task_mode_mismatch" {
		t.Fatalf("expected ordinary profile gate for non-lead, got %+v", result)
	}
}

func TestGetAgentWorkNextRoutesABPCVerificationSuccessorWithoutReviewReadyArtifact(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-abpc-verification-successor-routing"
		projectID   = "project-abpc-verification-successor-routing"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "zeta"
		taskID      = "task-side-effect-verify-tooling"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, builderID, sqlite.ProjectRoleIntegrator, leadID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Specialization: "reviewer",
		Tags:           []string{"reviewer", "qa", "verifier"},
		Metadata: map[string]any{
			"default_work_mode": "review",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "verify", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		OwnerUserID:          leadID,
		Priority:             "high",
		Title:                "Verify side effect classification for Gamma tooling bucket",
		Description:          "ABPC recovery action: verify tooling bucket, then run side_effect_resolve with the final decision.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		TaskClass:            "PROOF",
		TaskClassSource:      "EXPLICIT",
		ProjectID:            projectID,
		ProjectLane:          "verification",
		RequiresProjectGate:  false,
		Tags:                 []string{"side-effect-resolution", "verification", "abpc"},
		WriteScopeHints:      []string{"package.json"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_verification","action_kind":"verify_bucket","decision":"request_verification","parent_classifier_task_id":"task-side-effect-classify-gamma","side_effect_refs":["side-effect:gamma"],"dirty_paths":["package.json"],"path_bucket":["package.json"],"next_transition":"route_to_verifier"}`,
	}, graph); err != nil {
		t.Fatalf("create ABPC verification successor task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach ABPC verification successor task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       reviewerID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != taskID {
		t.Fatalf("expected ABPC verification successor to be claimable without review-ready artifact, got %+v", result)
	}
	if result.Reason == "project_validation_artifact_missing" {
		t.Fatalf("ABPC verification successor must not use product review-ready validation gate: %+v", result.Packet)
	}
	if result.Packet == nil || result.Packet.WorkType != "abpc_side_effect_recovery_action" || result.Packet.PreferredTransition != "side_effect_resolve" {
		t.Fatalf("expected ABPC recovery packet with side_effect_resolve transition, got %+v", result.Packet)
	}
	if result.Packet.Gate == nil || result.Packet.Gate.GateType != "abpc_recovery_action" || result.Packet.Gate.NeededFrom != "side_effect_resolve" {
		t.Fatalf("expected ABPC recovery action gate, got %+v", result.Packet)
	}
}

func TestGetAgentWorkNextYieldsParentClassifierToABPCRecoverySuccessor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-abpc-successor-yield"
		projectID   = "project-abpc-successor-yield"
		leadID      = "alpha"
		parentID    = "task-side-effect-classify-parent"
		successorID = "task-side-effect-verify-bucket"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        leadID,
		Specialization: "strategy",
		Tags:           []string{"strategist", "coordination"},
		Metadata: map[string]any{
			"default_work_mode": "strategy",
		},
	}); err != nil {
		t.Fatalf("upsert lead profile: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "classify", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              parentID,
		OwnerUserID:         leadID,
		Priority:            "high",
		Title:               "Classify side effect",
		Description:         "Parent classifier is waiting_on_side_effect_resolution_successors:task-side-effect-verify-bucket.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		TaskClass:           "INCIDENT",
		TaskClassSource:     "EXPLICIT",
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: false,
		Tags:                []string{"side-effect-classification", "abpc"},
	}, graph); err != nil {
		t.Fatalf("create parent classifier task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      parentID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach parent classifier task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      parentID,
		AgentID:     leadID,
		Summary:     "waiting_on_side_effect_resolution_successors:" + successorID,
	}); err != nil {
		t.Fatalf("claim parent classifier task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-parent-classifier",
		WorkspaceID: workspaceID,
		AgentID:     leadID,
		TaskID:      parentID,
		StartedAt:   "2026-05-23T00:00:00Z",
	}); err != nil {
		t.Fatalf("create parent classifier session: %v", err)
	}

	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               successorID,
		OwnerUserID:          leadID,
		Priority:             "high",
		Title:                "Verify side effect bucket",
		Description:          "ABPC recovery action: verify bucket and call side_effect_resolve.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		TaskClass:            "PROOF",
		TaskClassSource:      "EXPLICIT",
		ProjectID:            projectID,
		ProjectLane:          "verification",
		RequiresProjectGate:  false,
		Tags:                 []string{"side-effect-resolution", "verification", "abpc"},
		WriteScopeHints:      []string{"package.json"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_verification","action_kind":"verify_bucket","decision":"request_verification","parent_classifier_task_id":"task-side-effect-classify-parent","side_effect_refs":["side-effect:parent"],"dirty_paths":["package.json"],"path_bucket":["package.json"]}`,
	}, graph); err != nil {
		t.Fatalf("create ABPC successor task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      successorID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach ABPC successor task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get lead work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != successorID {
		t.Fatalf("expected active parent classifier to yield to ABPC successor, got %+v", result)
	}
	if result.Reason != "abpc_recovery_successor_available" {
		t.Fatalf("expected ABPC successor reason, got %q", result.Reason)
	}
	if result.Packet == nil || result.Packet.WorkType != "abpc_side_effect_recovery_action" || result.Packet.PreferredTransition != "side_effect_resolve" {
		t.Fatalf("expected ABPC successor packet requiring side_effect_resolve, got %+v", result.Packet)
	}
}

func TestOrdinaryStrategyTaskDoesNotInferProjectRoleAssignPacket(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-role-scope-negative"
		projectID   = "project-role-scope-negative"
		leadID      = "alpha"
		taskID      = "task-project-root"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         leadID,
		Priority:            "high",
		Title:               "Autonomous coordinator root task",
		Description:         "Coordinate the project and task fanout.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "project",
		ProjectID:           projectID,
		ProjectLane:         "strategy",
		RequiresProjectGate: false,
		Tags:                []string{"project-root", "strategy"},
	}, graph); err != nil {
		t.Fatalf("create root task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach root task: %v", err)
	}

	leadWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get lead work next: %v", err)
	}
	if !leadWork.HasWork || leadWork.Task == nil || leadWork.Task.TaskID != taskID {
		t.Fatalf("expected ordinary strategy task, got %+v", leadWork)
	}
	if leadWork.Packet == nil {
		t.Fatalf("expected packet")
	}
	if leadWork.Packet.PreferredTransition == "project_role_assign" {
		t.Fatalf("ordinary strategy task must not infer project_role_assign packet: %+v", leadWork.Packet)
	}
}

func TestPatchQueueReviewTaskRequiresReviewActor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patch-review-routing"
		projectID   = "project-patch-review-routing"
		leadID      = "alpha"
		builderID   = "delta"
		reviewerID  = "epsilon"
		taskID      = "task-patch-queue-review"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     reviewerID,
		OwnerUserID: "developer",
		DisplayName: reviewerID,
		Role:        "ux qa reviewer",
	}); err != nil {
		t.Fatalf("mark reviewer with semantic review role: %v", err)
	}
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     builderID,
		OwnerUserID: "developer",
		DisplayName: builderID,
		Role:        "builder-reviewer",
	}); err != nil {
		t.Fatalf("mark builder with mixed registered role: %v", err)
	}
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

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "review", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  leadID,
		Priority:     "critical",
		Title:        "Review patch queue candidate",
		Description:  "Patch queue candidate is ready for independent review.",
		TaskKind:     "COORDINATION",
		TaskTemplate: "integration",
		ProjectID:    projectID,
		ProjectLane:  "review",
		Tags:         []string{"review", "reviewer", "patch_queue", "project"},
	}, graph); err != nil {
		t.Fatalf("create patch review task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach patch review task: %v", err)
	}

	builderWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get builder work next: %v", err)
	}
	if builderWork.HasWork || builderWork.Reason != "project_patch_queue_review_role_required" || builderWork.Packet == nil {
		t.Fatalf("expected builder to be routed away from patch queue review task, got %+v", builderWork)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          builderID,
		CoordinationMode: "trust_first",
		Summary:          "builder should not claim patch queue review task",
	}); err == nil || (!strings.Contains(err.Error(), "active REVIEWER role") && !strings.Contains(err.Error(), "active INTEGRATOR/REVIEWER")) {
		t.Fatalf("expected builder patch review claim to be rejected, got %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if !reviewerWork.HasWork || reviewerWork.Task == nil || reviewerWork.Task.TaskID != taskID {
		t.Fatalf("expected reviewer to receive patch queue review task, got %+v", reviewerWork)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     reviewerID,
		Summary:     "reviewer claims patch queue review task",
	}); err != nil {
		t.Fatalf("claim patch review task by reviewer: %v", err)
	}
}

func TestPatchQueueSupersedeTaskRequiresReviewActor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patch-supersede-role-required"
		projectID   = "project-patch-supersede-role-required"
		repoID      = "repo-main"
		leadID      = "alpha"
		builderID   = "eta"
		reviewerID  = "epsilon"
		taskID      = "task-patchq-supersede-role-required"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "supersede", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-supersede-role-required", builderID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		OwnerUserID: leadID,
		Priority:    "high",
		Title:       "Supersede blocked patch queue item after fresh evidence",
		Description: "Call project_patch_queue_lifecycle action=supersede for the exact blocked patch queue item.\n\n" +
			"- queue_id: " + item.QueueID + "\n" +
			"- item_id: " + item.ItemID + "\n" +
			"- branch_id: " + item.BranchID + "\n" +
			"- head_sha: " + item.HeadSHA,
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: "generic",
		ProjectID:    projectID,
		ProjectLane:  "coordination",
		Tags:         []string{"project", "patch-queue", "supersede", "coordination"},
	}, graph); err != nil {
		t.Fatalf("create patch supersede task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach patch supersede task: %v", err)
	}

	builderWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get builder work next: %v", err)
	}
	if builderWork.HasWork || builderWork.Reason != "project_patch_queue_review_role_required" || builderWork.Packet == nil {
		t.Fatalf("expected builder to be routed away from patch queue supersede task, got %+v", builderWork)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          builderID,
		CoordinationMode: "trust_first",
		Summary:          "builder should not claim patch queue supersede task",
	}); err == nil || (!strings.Contains(err.Error(), "active REVIEWER role") && !strings.Contains(err.Error(), "active INTEGRATOR/REVIEWER")) {
		t.Fatalf("expected builder patch supersede claim to be rejected, got %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if !reviewerWork.HasWork || reviewerWork.Task == nil || reviewerWork.Task.TaskID != taskID {
		t.Fatalf("expected reviewer to receive patch queue supersede task, got %+v", reviewerWork)
	}

	triggeredReviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get triggered reviewer work next: %v", err)
	}
	if !triggeredReviewerWork.HasWork || triggeredReviewerWork.Task == nil || triggeredReviewerWork.Task.TaskID != taskID {
		t.Fatalf("expected triggered reviewer to receive blocked-item supersede task, got %+v", triggeredReviewerWork)
	}
}

func TestPatchQueueSupersedeTextOnlyTaskClaimRequiresReviewActor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patch-supersede-text-only-claim"
		projectID   = "project-patch-supersede-text-only-claim"
		leadID      = "alpha"
		builderID   = "eta"
		reviewerID  = "epsilon"
		taskID      = "task-patchq-supersede-text-only-claim"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
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

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "supersede", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  leadID,
		Priority:     "high",
		Title:        "Advance same-head supersede for validated branch",
		Description:  "Call project_patch_queue_lifecycle for the blocked item using action=\"supersede\".\n\n- queue_id: queue-1\n- item_id: item-1\n- branch_id: branch-1",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: "generic",
		ProjectID:    projectID,
		ProjectLane:  "coordination",
		Tags:         []string{"project", "patch-queue", "integration", "coordination"},
	}, graph); err != nil {
		t.Fatalf("create text-only patch supersede task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach text-only patch supersede task: %v", err)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          builderID,
		CoordinationMode: "trust_first",
		Summary:          "builder should not claim text-only patch queue supersede task",
	}); err == nil || (!strings.Contains(err.Error(), "active REVIEWER role") && !strings.Contains(err.Error(), "active INTEGRATOR/REVIEWER")) {
		t.Fatalf("expected builder text-only patch supersede claim to be rejected, got %v", err)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          reviewerID,
		CoordinationMode: "trust_first",
		Summary:          "reviewer claims text-only patch queue supersede task",
	}); err != nil {
		t.Fatalf("claim text-only patch supersede task by reviewer: %v", err)
	}
}

func TestGetAgentWorkNextSurfacesBlockedPatchQueueSupersedeFrontier(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-supersede-frontier"
		projectID   = "project-patchq-supersede-frontier"
		leadID      = "alpha"
		builderID   = "delta"
		reviewerID  = "epsilon"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-supersede-frontier", builderID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "project.patchq-supersede-frontier.browser_smoke_recheck"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Browser Smoke Recheck",
		Content: "The previous blocker claiming missing fresh browser-smoke evidence is stale. Browser-smoke provenance records a PASS and the validation gap is closed for queue_id " + item.QueueID +
			" item_id " + item.ItemID +
			" branch_id " + item.BranchID +
			" head_sha " + item.HeadSHA +
			" branch_name agent/" + builderID + "/" + item.BranchID + ".",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write fresh evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force evidence timestamp: %v", err)
	}
	agentResponseKey := "task.task-patchq-validation-supersede-frontier.agent_response.areq-test"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      agentResponseKey,
		Title:       "Agent response - zeta to theta",
		Content: fmt.Sprintf(`# Agent Request Response Evidence

evidence_scope: public workspace coordination

This response names the queue but is only a coordination acknowledgement, not a validation artifact.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser-smoke evidence: PASS`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write coordination response doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-02T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, agentResponseKey); err != nil {
		t.Fatalf("force coordination response timestamp: %v", err)
	}
	agentStateKey := "agent." + reviewerID + ".claimed_work"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      agentStateKey,
		Title:       "Claimed Work Ledger",
		Content: fmt.Sprintf(`# Claimed Work Ledger

active_claimed_work: none
last_summary: repeated patch queue selectors and browser smoke status for bookkeeping only.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser-smoke evidence: PASS`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write agent state doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-03T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, agentStateKey); err != nil {
		t.Fatalf("force agent state timestamp: %v", err)
	}
	staleContextKey := "agent." + reviewerID + ".current_context_stale"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      staleContextKey,
		Title:       "Current Context Snapshot",
		Content: fmt.Sprintf(`Current context for agent %s in workspace %s.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser-smoke evidence: PASS`, reviewerID, workspaceID, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write suffixed current context doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-04T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, staleContextKey); err != nil {
		t.Fatalf("force suffixed current context timestamp: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-idle-reflection-shadow", "Product quality iteration: inspect project", "qa", "normal")
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET tags_json = ? WHERE task_id = ?`,
		`["meta-reflection","anti-idle","product-quality"]`,
		"task-idle-reflection-shadow"); err != nil {
		t.Fatalf("mark idle task: %v", err)
	}
	builderWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get builder work next: %v", err)
	}
	if builderWork.Reason == "project_patch_queue_supersede_available" {
		t.Fatalf("builder without reviewer/integrator authority should not receive supersede frontier: %+v", builderWork)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.HasWork || reviewerWork.Reason != "project_patch_queue_supersede_available" {
		t.Fatalf("expected no-work supersede frontier for reviewer, got %+v", reviewerWork)
	}
	if reviewerWork.Packet == nil || reviewerWork.Packet.PatchQueueSupersede == nil {
		t.Fatalf("expected patch_queue_supersede packet, got %+v", reviewerWork.Packet)
	}
	got := reviewerWork.Packet.PatchQueueSupersede
	if got.ProjectID != projectID || got.QueueID != item.QueueID || got.ItemID != item.ItemID || got.BranchID != item.BranchID || got.HeadSHA != item.HeadSHA || got.EvidenceDocKey != evidenceDocKey {
		t.Fatalf("supersede packet mismatch: got %+v item=%+v", got, item)
	}
	if got.NewItemID == "" || got.NewItemID == item.ItemID {
		t.Fatalf("expected deterministic new item id distinct from old item, got %+v", got)
	}
	if reviewerWork.Packet.PreferredTransition != "create_or_claim_patch_queue_supersede_stewardship" {
		t.Fatalf("unexpected preferred transition: %+v", reviewerWork.Packet)
	}
}

func TestGetAgentWorkNextTerminalizesPatchQueueValidationTaskAfterBackendEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID      = "ws-patchq-backend-validation-receipt"
		projectID        = "project-patchq-backend-validation-receipt"
		leadID           = "alpha"
		branchOwnerID    = "beta"
		validatorID      = "epsilon"
		repoID           = "repo-main"
		validationTaskID = "task-patchq-validation-backend-evidence"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, validatorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["internal/eval/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, validatorID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-backend-validation", branchOwnerID, validatorID, `{"paths":["internal/eval/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "validate", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	requirements := fmt.Sprintf(`{"schema":"task_requirements.v1","patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1","patch_queue_task_kind":"validation","queue_id":%q,"item_id":%q,"branch_id":%q,"head_sha":%q}`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA)
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               validationTaskID,
		OwnerUserID:          validatorID,
		Priority:             "high",
		Title:                "Validate blocked backend candidate " + item.BranchID,
		Description:          fmt.Sprintf("Queue-bound backend validation for queue_id: %s, item_id: %s, branch_id: %s, head_sha: %s.", item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		Tags:                 []string{"patch-queue", "validation"},
		ProjectID:            projectID,
		ProjectLane:          "validation",
		TaskRequirementsJSON: requirements,
		RequiresProjectGate:  true,
	}, graph); err != nil {
		t.Fatalf("create validation task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      validationTaskID,
		LinkedBy:    "system",
	}); err != nil {
		t.Fatalf("attach validation task: %v", err)
	}

	evidenceDocKey := "task." + validationTaskID + ".validation"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Backend Validation Evidence",
		Content: fmt.Sprintf(`schema: rhizome_validation_evidence_v1
validation_verdict: pass
command: go test ./...
exit_code: 0
tests passed

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		UpdatedBy: validatorID,
	}); err != nil {
		t.Fatalf("write backend validation evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force evidence timestamp: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          validatorID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  validationTaskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get validation task after evidence: %v", err)
	}
	if result.HasWork && result.Task != nil && result.Task.TaskID == validationTaskID {
		t.Fatalf("backend validation evidence should terminalize validation task, got %+v", result)
	}
	if result.Reason != "trigger_task_superseded" {
		t.Fatalf("expected trigger_task_superseded for backend validation receipt, got %+v", result)
	}
}

func TestGetAgentWorkNextSurfacesVisualAcceptanceSupersedeFrontier(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-visual-supersede-frontier"
		projectID   = "project-patchq-visual-supersede-frontier"
		leadID      = "alpha"
		builderID   = "delta"
		reviewerID  = "epsilon"
		repoID      = "repo-main"
		branchID    = "branch-visual-frontier"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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
	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, builderID, reviewerID, `{"paths":["web/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "task.task-visual-frontier.visual_acceptance_attempt"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Visual Acceptance Attempt",
		Content: fmt.Sprintf(`schema: rhizome_visual_acceptance_v1
status: complete
visual_verdict: pass
queue_id: %s
item_id: %s
branch_name: agent/%s/%s
head_sha: %s
state_evidence:
  initial_state: screenshot_path C:/tmp/clearpress-initial-desktop.png
  primary_flow: screenshot_path C:/tmp/clearpress-primary-flow-desktop.png
  result_state: screenshot_path C:/tmp/clearpress-result-mobile.png
viewport_matrix: desktop 1365x768 and mobile narrow 390x844
product_intent: primary user path and core user promise exercised
checks: overlap none; clipping none; contrast/readability pass; responsive typography hierarchy spacing usability pass
screenshot_provenance: browser screenshot passed real user scenario checks`, item.QueueID, item.ItemID, builderID, branchID, item.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write fresh visual evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-02-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force visual evidence timestamp: %v", err)
	}
	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.HasWork || reviewerWork.Reason != "project_patch_queue_supersede_available" {
		t.Fatalf("expected visual acceptance supersede frontier, got %+v", reviewerWork)
	}
	if reviewerWork.Packet == nil || reviewerWork.Packet.PatchQueueSupersede == nil {
		t.Fatalf("expected visual patch queue supersede packet, got %+v", reviewerWork.Packet)
	}
	if got := reviewerWork.Packet.PatchQueueSupersede; got.EvidenceDocKey != evidenceDocKey || got.ItemID != item.ItemID || got.HeadSHA != item.HeadSHA {
		t.Fatalf("unexpected visual supersede packet: got %+v item=%+v", got, item)
	}
}

func TestGetAgentWorkNextSuppressesBlockedVisualAcceptanceSupersedeFrontier(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-blocked-visual-supersede-frontier"
		projectID   = "project-patchq-blocked-visual-supersede-frontier"
		leadID      = "alpha"
		builderID   = "delta"
		reviewerID  = "epsilon"
		repoID      = "repo-main"
		branchID    = "branch-blocked-visual-frontier"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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
	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, builderID, reviewerID, `{"paths":["web/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "task.task-visual-frontier.visual_acceptance_block"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Visual Acceptance Attempt",
		Content: fmt.Sprintf(`{"schema":"rhizome_visual_acceptance_v1","status":"complete","visual_verdict":"block","branch_name":"agent/%s/%s","head_sha":"%s"}

The previous blocker is stale and screenshot capture result: pass, but the real-user visual judgment is blocked.`, builderID, branchID, item.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write blocked visual evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-02-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force blocked visual evidence timestamp: %v", err)
	}
	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.Reason == "project_patch_queue_supersede_available" {
		t.Fatalf("blocked visual evidence must not surface supersede frontier: %+v", reviewerWork)
	}
}

func TestGetAgentWorkNextSuppressesSupersedeFrontierFromNonPassVisualAcceptance(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-non-pass-visual-frontier"
		projectID   = "project-patchq-non-pass-visual-frontier"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "kappa"
		repoID      = "repo-main"
		branchID    = "branch-non-pass-visual-frontier"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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
	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, builderID, reviewerID, `{"paths":["web/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "task.task-clearpress-app-shell.visual_acceptance_provisional"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Clearpress App Shell Provisional Visual Check",
		Content: fmt.Sprintf(`schema: rhizome_visual_acceptance_v1
status: provisional_non_pass
visual_verdict: under_evidenced
pass_for_acceptance: false
acceptance_status: not_accepted
queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
state_evidence:
  initial_state: screenshot_path C:/tmp/clearpress-initial-desktop.png
  primary_flow: screenshot_path C:/tmp/clearpress-primary-flow-desktop.png
  result_state: screenshot_path C:/tmp/clearpress-result-mobile.png
viewport_matrix: desktop 1440x900 and mobile narrow 390x844
product_intent: primary user path and core user promise exercised
checks: overlap checked; clipping checked; contrast/readability checked; responsive typography hierarchy spacing usability checked
screenshot_provenance: browser screenshot captured
browser smoke: passed
tests passed
required_next_packet: visual_verdict: pass`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write non-pass visual evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-02-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force non-pass visual evidence timestamp: %v", err)
	}
	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.Reason == "project_patch_queue_supersede_available" {
		t.Fatalf("non-pass visual evidence must not surface supersede frontier: %+v", reviewerWork)
	}
}

func TestGetAgentWorkNextSuppressesSupersedeFrontierFromTaskBrief(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-task-brief-supersede-frontier"
		projectID   = "project-patchq-task-brief-supersede-frontier"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "kappa"
		repoID      = "repo-main"
		branchID    = "branch-task-brief-frontier"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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
	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, builderID, reviewerID, `{"paths":["web/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	taskBriefKey := "task.task-clearpress-visual-acceptance-review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      taskBriefKey,
		Title:       "Task Brief - Review Clearpress app shell visual acceptance on exact owned HEAD",
		Content: fmt.Sprintf(`# Task Brief - Review Clearpress app shell visual acceptance on exact owned HEAD

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s

Required output: publish durable evidence with either visual_verdict: pass or concrete blocking findings.
This canonical task document was created by task_submit at task.task-clearpress-visual-acceptance-review.`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		UpdatedBy: builderID,
	}); err != nil {
		t.Fatalf("write task brief doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-02-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, taskBriefKey); err != nil {
		t.Fatalf("force task brief timestamp: %v", err)
	}
	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.Reason == "project_patch_queue_supersede_available" {
		t.Fatalf("task brief must not surface supersede frontier: %+v", reviewerWork)
	}
}

func TestGetAgentWorkNextSuppressesSupersedeFrontierAfterAcceptedSuccessor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-supersede-accepted-successor"
		projectID   = "project-patchq-supersede-accepted-successor"
		leadID      = "alpha"
		builderID   = "delta"
		reviewerID  = "epsilon"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-supersede-accepted-successor", builderID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "project.patchq-supersede-accepted-successor.browser_smoke_recheck"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Browser Smoke Recheck",
		Content: "Fresh validation passed. browser smoke: passed for queue_id " + item.QueueID +
			" item_id " + item.ItemID +
			" branch_id " + item.BranchID +
			" head_sha " + item.HeadSHA + ".",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write fresh evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force evidence timestamp: %v", err)
	}

	successor, _, alreadyQueued, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		NewItemID:             item.ItemID + "-accepted-successor",
		EvidenceDocKey:        evidenceDocKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	})
	if err != nil {
		t.Fatalf("create accepted successor: %v", err)
	}
	if alreadyQueued || successor.SupersedesItemID != item.ItemID {
		t.Fatalf("expected fresh supersede successor, already=%v successor=%+v item=%+v", alreadyQueued, successor, item)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               successor.QueueID,
		ItemID:                successor.ItemID,
		LeaseSeconds:          900,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim successor: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateAccepted,
		DecisionSummary:       "Fresh successor accepted; old blocked item no longer needs supersede work.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("accept successor: %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.Reason == "project_patch_queue_supersede_available" {
		t.Fatalf("accepted successor should suppress stale supersede frontier, got %+v", reviewerWork)
	}

	staleTaskID := "task-stale-supersede-after-accepted-successor"
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "stewardship", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              staleTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Supersede blocked patch queue item after fresh evidence",
		Description:         "Call project_patch_queue_lifecycle action=supersede for the exact blocked patch queue item.\n\nqueue_id: " + item.QueueID + "\nitem_id: " + item.ItemID + "\nbranch_id: " + item.BranchID + "\nhead_sha: " + item.HeadSHA + "\nevidence_doc_key: " + evidenceDocKey,
		TaskKind:            model.TaskKindExecution,
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "integration",
		RequiresProjectGate: true,
		Tags:                []string{"project", "patch-queue", "supersede", "queue-stewardship"},
	}, graph); err != nil {
		t.Fatalf("create stale supersede task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: staleTaskID, LinkedBy: reviewerID}); err != nil {
		t.Fatalf("attach stale supersede task: %v", err)
	}
	triggered, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  staleTaskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get triggered stale supersede work: %v", err)
	}
	if triggered.HasWork || triggered.Reason != "trigger_task_superseded" {
		t.Fatalf("targeted stale supersede task should be superseded after accepted successor, got %+v", triggered)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           staleTaskID,
		AgentID:          reviewerID,
		CoordinationMode: "trust_first",
		Summary:          "stale supersede task should not be claimable",
	}); err == nil || !strings.Contains(err.Error(), "superseded by newer project evidence") {
		t.Fatalf("expected stale supersede claim rejection, got %v", err)
	}
}

func TestGetAgentWorkNextSuppressesOldSupersedeFrontierAfterBlockedSuccessor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-supersede-blocked-successor"
		projectID   = "project-patchq-supersede-blocked-successor"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "kappa"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-supersede-blocked-successor", builderID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "project.patchq-supersede-blocked-successor.browser_smoke_recheck"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Browser Smoke Recheck",
		Content: "Fresh validation passed. browser smoke: passed for queue_id " + item.QueueID +
			" item_id " + item.ItemID +
			" branch_id " + item.BranchID +
			" head_sha " + item.HeadSHA + ".",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write fresh evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force evidence timestamp: %v", err)
	}

	successor, _, alreadyQueued, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		NewItemID:             item.ItemID + "-blocked-successor",
		EvidenceDocKey:        evidenceDocKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	})
	if err != nil {
		t.Fatalf("create blocked successor: %v", err)
	}
	if alreadyQueued || successor.SupersedesItemID != item.ItemID {
		t.Fatalf("expected fresh supersede successor, already=%v successor=%+v item=%+v", alreadyQueued, successor, item)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               successor.QueueID,
		ItemID:                successor.ItemID,
		LeaseSeconds:          900,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim successor: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Successor still has gaps; the old blocked item must not be superseded again.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block successor: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE project_patch_queue_items SET decided_at = '2100-01-01T00:00:00Z', updated_at = '2100-01-01T00:00:00Z' WHERE workspace_id = ? AND queue_id = ? AND item_id = ?`, workspaceID, claimed.QueueID, claimed.ItemID); err != nil {
		t.Fatalf("force successor timestamp: %v", err)
	}
	heartbeatKey := "project." + projectID + ".heartbeat.theta.2100-01-02T00-00-00Z"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      heartbeatKey,
		Title:       "Theta Heartbeat",
		Content: "Agent heartbeat observation says the queue might be ready again. browser smoke evidence: PASS for queue_id " + successor.QueueID +
			" item_id " + successor.ItemID +
			" branch_id " + successor.BranchID +
			" head_sha " + successor.HeadSHA + ".",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write heartbeat supersede doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2100-01-02T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, heartbeatKey); err != nil {
		t.Fatalf("force heartbeat timestamp: %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.Reason == "project_patch_queue_supersede_available" {
		t.Fatalf("blocked successor should suppress stale old-item supersede frontier, got %+v", reviewerWork)
	}
}

func TestGetAgentWorkNextSurfacesFreshSupersedeFrontierForLatestBlockedSuccessor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-supersede-latest-blocked-successor"
		projectID   = "project-patchq-supersede-latest-blocked-successor"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "kappa"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-supersede-latest-blocked-successor", builderID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	firstEvidenceKey := "project.patchq-latest-blocked-successor.validation_run_1"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      firstEvidenceKey,
		Title:       "Validation Run 1",
		Content: "Validation passed. browser smoke: passed. validation_run_id: run-1 for queue_id " + item.QueueID +
			" item_id " + item.ItemID +
			" branch_id " + item.BranchID +
			" head_sha " + item.HeadSHA + ".",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write first evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, firstEvidenceKey); err != nil {
		t.Fatalf("force first evidence timestamp: %v", err)
	}
	successor1, _, alreadyQueued, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		NewItemID:             item.ItemID + "-successor-1",
		EvidenceDocKey:        firstEvidenceKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	})
	if err != nil {
		t.Fatalf("create successor1: %v", err)
	}
	if alreadyQueued {
		t.Fatalf("unexpected alreadyQueued for successor1")
	}
	claimed1, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               successor1.QueueID,
		ItemID:                successor1.ItemID,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim successor1: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed1.QueueID,
		ItemID:                claimed1.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "First successor remained blocked.",
		ClaimToken:            claimed1.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block successor1: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE project_patch_queue_items SET decided_at = '2100-01-01T00:00:00Z', updated_at = '2100-01-01T00:00:00Z' WHERE workspace_id = ? AND queue_id = ? AND item_id = ?`, workspaceID, claimed1.QueueID, claimed1.ItemID); err != nil {
		t.Fatalf("force successor1 timestamp: %v", err)
	}

	secondEvidenceKey := "project.patchq-latest-blocked-successor.validation_run_2"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      secondEvidenceKey,
		Title:       "Validation Run 2",
		Content: "Validation passed. browser smoke: passed. validation_run_id: run-2 for queue_id " + successor1.QueueID +
			" item_id " + successor1.ItemID +
			" branch_id " + successor1.BranchID +
			" head_sha " + successor1.HeadSHA + ".",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write second evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2100-01-02T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, secondEvidenceKey); err != nil {
		t.Fatalf("force second evidence timestamp: %v", err)
	}
	successor2, _, alreadyQueued, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               successor1.QueueID,
		ItemID:                successor1.ItemID,
		NewItemID:             successor1.ItemID + "-successor-2",
		EvidenceDocKey:        secondEvidenceKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	})
	if err != nil {
		t.Fatalf("create successor2: %v", err)
	}
	if alreadyQueued {
		t.Fatalf("unexpected alreadyQueued for successor2")
	}
	claimed2, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               successor2.QueueID,
		ItemID:                successor2.ItemID,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim successor2: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed2.QueueID,
		ItemID:                claimed2.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Second successor remained blocked.",
		ClaimToken:            claimed2.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block successor2: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE project_patch_queue_items SET decided_at = '2101-01-01T00:00:00Z', updated_at = '2101-01-01T00:00:00Z' WHERE workspace_id = ? AND queue_id = ? AND item_id = ?`, workspaceID, claimed2.QueueID, claimed2.ItemID); err != nil {
		t.Fatalf("force successor2 timestamp: %v", err)
	}

	thirdEvidenceKey := "project.patchq-latest-blocked-successor.validation_run_3"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      thirdEvidenceKey,
		Title:       "Validation Run 3",
		Content: "Validation passed. browser smoke: passed. validation_run_id: run-3 for queue_id " + successor2.QueueID +
			" item_id " + successor2.ItemID +
			" branch_id " + successor2.BranchID +
			" head_sha " + successor2.HeadSHA + ".",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write third evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2101-01-02T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, thirdEvidenceKey); err != nil {
		t.Fatalf("force third evidence timestamp: %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.HasWork || reviewerWork.Reason != "project_patch_queue_supersede_available" {
		t.Fatalf("expected fresh latest-successor supersede frontier, got %+v", reviewerWork)
	}
	if reviewerWork.Packet == nil || reviewerWork.Packet.PatchQueueSupersede == nil {
		t.Fatalf("expected patch_queue_supersede packet, got %+v", reviewerWork.Packet)
	}
	got := reviewerWork.Packet.PatchQueueSupersede
	if got.ItemID != successor2.ItemID || got.EvidenceDocKey != thirdEvidenceKey {
		t.Fatalf("expected latest successor frontier for evidence %s, got %+v", thirdEvidenceKey, got)
	}
}

func TestGetAgentWorkNextSuppressesClaimStewardshipTaskAfterDecision(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID       = "ws-claim-stewardship-after-decision"
		projectID         = "project-claim-stewardship-after-decision"
		repoID            = "repo-claim-stewardship-after-decision"
		leadID            = "alpha"
		ownerID           = "beta"
		integratorID      = "zeta"
		branchID          = "branch-claim-stewardship-after-decision"
		stewardshipTaskID = "task-claim-stewardship-after-decision"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               integratorID,
		RoleType:              sqlite.ProjectRoleIntegrator,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign integrator role: %v", err)
	}
	accepted := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, ownerID, integratorID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateAccepted)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "stewardship", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              stewardshipTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Resolve claimed patch queue item lifecycle",
		Description:         "Patch queue claim stewardship task created from agent.work.next frontier.\n\nqueue_id: " + accepted.QueueID + "\nitem_id: " + accepted.ItemID + "\nbranch_id: " + accepted.BranchID,
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "integration",
		RequiresProjectGate: true,
		Tags:                []string{"project", "patch-queue", "integration", "queue-stewardship", "claim-stewardship", "claimed-decision"},
	}, graph); err != nil {
		t.Fatalf("create claim stewardship task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      stewardshipTaskID,
		LinkedBy:    integratorID,
	}); err != nil {
		t.Fatalf("attach claim stewardship task: %v", err)
	}

	triggered, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          integratorID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  stewardshipTaskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get triggered claim stewardship work: %v", err)
	}
	if triggered.HasWork || triggered.Reason != "trigger_task_superseded" {
		t.Fatalf("terminal item should supersede stale claim stewardship task, got %+v", triggered)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           stewardshipTaskID,
		AgentID:          integratorID,
		CoordinationMode: "trust_first",
		Summary:          "claim stewardship task should not be claimable after decision",
	}); err == nil || !strings.Contains(err.Error(), "superseded by newer project evidence") {
		t.Fatalf("expected stale claim stewardship claim rejection, got %v", err)
	}
}

func TestGetAgentWorkNextSuppressesSupersedeFrontierWhenStewardshipTaskIsDependencyBlocked(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-supersede-blocked-task"
		projectID   = "project-patchq-supersede-blocked-task"
		leadID      = "alpha"
		builderID   = "delta"
		reviewerID  = "epsilon"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-supersede-blocked-task", builderID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "project.patchq-supersede-blocked-task.browser_smoke_recheck"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Browser Smoke Recheck",
		Content: "Fresh validation passed. browser smoke: passed for queue_id " + item.QueueID +
			" item_id " + item.ItemID +
			" branch_id " + item.BranchID +
			" head_sha " + item.HeadSHA + ".",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write fresh evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force evidence timestamp: %v", err)
	}

	blockerID := "task-unresolved-validation-evidence"
	stewardshipTaskID := patchQueueSupersedeTaskIDForAgentWorkTest(projectID, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA, evidenceDocKey)
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, blockerID, "Collect missing validation evidence", "qa", "high")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, stewardshipTaskID, "Supersede patch queue "+item.QueueID+" "+item.ItemID+" "+item.BranchID+" "+item.HeadSHA, "integration", "critical")
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET tags_json = ? WHERE task_id = ?`,
		`["patch-queue","supersede","queue-stewardship"]`,
		stewardshipTaskID); err != nil {
		t.Fatalf("mark supersede stewardship task: %v", err)
	}
	if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
		WorkspaceID: workspaceID,
		FromTaskID:  blockerID,
		ToTaskID:    stewardshipTaskID,
		LinkType:    model.TaskLinkBlocks,
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("add dependency link: %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.Reason == "project_patch_queue_supersede_available" {
		t.Fatalf("existing dependency-blocked supersede stewardship task should suppress duplicate frontier, got %+v", reviewerWork)
	}
}

func TestGetAgentWorkNextDoesNotSuppressSupersedeFrontierWithOldEvidenceTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-supersede-old-evidence"
		projectID   = "project-patchq-supersede-old-evidence"
		leadID      = "alpha"
		builderID   = "delta"
		reviewerID  = "epsilon"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-supersede-old-evidence", builderID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "project.patchq-supersede-old-evidence.browser_smoke_recheck"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Browser Smoke Recheck",
		Content: "Fresh validation passed. browser smoke: passed for queue_id " + item.QueueID +
			" item_id " + item.ItemID +
			" branch_id " + item.BranchID +
			" head_sha " + item.HeadSHA + ".",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write fresh evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force evidence timestamp: %v", err)
	}

	stewardshipTaskID := "task-patchq-supersede-old-evidence"
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, stewardshipTaskID, "Supersede patch queue "+item.QueueID+" "+item.ItemID+" "+item.BranchID+" "+item.HeadSHA+" project.old.evidence.doc", "integration", "critical")
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET tags_json = ? WHERE task_id = ?`,
		`["patch-queue","supersede","queue-stewardship"]`,
		stewardshipTaskID); err != nil {
		t.Fatalf("mark supersede stewardship task: %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.HasWork || reviewerWork.Reason != "project_patch_queue_supersede_available" {
		t.Fatalf("old evidence stewardship task must not suppress fresh evidence frontier, got %+v", reviewerWork)
	}
}

func TestGetAgentWorkNextDoesNotSurfaceSupersedeFrontierFromBlockingReviewResult(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-supersede-negative-frontier"
		projectID   = "project-patchq-supersede-negative-frontier"
		leadID      = "alpha"
		builderID   = "delta"
		reviewerID  = "epsilon"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-supersede-negative-frontier", builderID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	negativeDocKey := "project.patchq-supersede-negative-frontier.review_result"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      negativeDocKey,
		Title:       "Task Result - Review patch queue candidate",
		Content: "BLOCKED: fresh same-head browser-smoke evidence is still missing for queue_id " + item.QueueID +
			" item_id " + item.ItemID +
			" branch_id " + item.BranchID +
			" head_sha " + item.HeadSHA +
			"; not ready for acceptance.",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write negative review doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, negativeDocKey); err != nil {
		t.Fatalf("force negative evidence timestamp: %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.Reason == "project_patch_queue_supersede_available" {
		t.Fatalf("blocking review result should not surface supersede frontier: %+v", reviewerWork)
	}
}

func TestGetAgentWorkNextDoesNotSurfaceSupersedeFrontierFromFailedValidationVerdict(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-supersede-failed-validation-frontier"
		projectID   = "project-patchq-supersede-failed-validation-frontier"
		leadID      = "alpha"
		builderID   = "delta"
		reviewerID  = "epsilon"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-supersede-failed-validation", builderID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	validationDocKey := "project.patchq-supersede-failed-validation.zeta_validation"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      validationDocKey,
		Title:       "Validation Evidence",
		Content: "build_check: npm run build succeeded\n" +
			"schema: rhizome_visual_acceptance_v1\n" +
			"verdict: fail\n" +
			"recommendation: treat this as a real revision need, not as a same-head requeue candidate\n" +
			"queue_id: " + item.QueueID + "\n" +
			"item_id: " + item.ItemID + "\n" +
			"branch_id: " + item.BranchID + "\n" +
			"head_sha: " + item.HeadSHA + "\n" +
			"browser smoke: passed only for startup; result/export flow missing.",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write failed validation doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, validationDocKey); err != nil {
		t.Fatalf("force failed validation timestamp: %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.Reason == "project_patch_queue_supersede_available" {
		t.Fatalf("failed validation verdict should not surface supersede frontier: %+v", reviewerWork)
	}
}

func TestGetAgentWorkNextDoesNotSurfaceSupersedeFrontierFromPendingVisualAcceptance(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-supersede-pending-visual-frontier"
		projectID   = "project-patchq-supersede-pending-visual-frontier"
		leadID      = "alpha"
		builderID   = "delta"
		reviewerID  = "epsilon"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-supersede-pending-visual", builderID, reviewerID, `{"paths":["src/**","tests/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	pendingDocKey := "project.patchq-supersede-pending-visual.implementation_evidence"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      pendingDocKey,
		Title:       "Implementation Evidence",
		Content: "build_check: passed\n" +
			"test_result: passed\n" +
			"tests passed, but visual acceptance evidence is still pending for this same-head candidate.\n" +
			"queue_id: " + item.QueueID + "\n" +
			"item_id: " + item.ItemID + "\n" +
			"branch_id: " + item.BranchID + "\n" +
			"head_sha: " + item.HeadSHA + "\n" +
			"primary_flow: not evidenced\n" +
			"result_state: not evidenced\n",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write pending visual doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, pendingDocKey); err != nil {
		t.Fatalf("force pending visual timestamp: %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.Reason == "project_patch_queue_supersede_available" {
		t.Fatalf("pending visual acceptance must not surface supersede frontier: %+v", reviewerWork)
	}
}

func TestGetAgentWorkNextDoesNotSurfaceSupersedeFrontierFromSkippedBrowserSmoke(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-supersede-skipped-smoke-frontier"
		projectID   = "project-patchq-supersede-skipped-smoke-frontier"
		leadID      = "alpha"
		builderID   = "delta"
		reviewerID  = "epsilon"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-supersede-skipped-smoke", builderID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	skippedDocKey := "project.patchq-supersede-skipped-smoke.validation"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      skippedDocKey,
		Title:       "Validation Evidence",
		Content: "tests passed, but browser smoke not run and visual check not exercised.\n" +
			"queue_id: " + item.QueueID + "\n" +
			"item_id: " + item.ItemID + "\n" +
			"branch_id: " + item.BranchID + "\n" +
			"head_sha: " + item.HeadSHA,
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write skipped browser smoke doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, skippedDocKey); err != nil {
		t.Fatalf("force skipped evidence timestamp: %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerWork.Reason == "project_patch_queue_supersede_available" {
		t.Fatalf("skipped browser smoke should not surface supersede frontier: %+v", reviewerWork)
	}
}

func TestPatchQueueReviewTaskAllowsSemanticRegisteredReviewerWithoutActiveProjectRole(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patch-review-semantic-role"
		projectID   = "project-patch-review-semantic-role"
		leadID      = "alpha"
		criticID    = "epsilon"
		taskID      = "task-patch-queue-review-semantic-role"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, criticID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     criticID,
		OwnerUserID: "developer",
		DisplayName: criticID,
		Role:        "harsh real-user ux critic",
	}); err != nil {
		t.Fatalf("register semantic reviewer role: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "review", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  leadID,
		Priority:     "critical",
		Title:        "Review patch queue candidate",
		Description:  "Patch queue candidate is ready for independent review.\n\n- queue_id: queue-1\n- item_id: item-1\n- branch_id: branch-1",
		TaskKind:     "COORDINATION",
		TaskTemplate: "integration",
		ProjectID:    projectID,
		ProjectLane:  "review",
		Tags:         []string{"review", "patch_queue", "project"},
	}, graph); err != nil {
		t.Fatalf("create patch review task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach patch review task: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          criticID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get semantic reviewer work next: %v", err)
	}
	if !work.HasWork || work.Task == nil || work.Task.TaskID != taskID {
		t.Fatalf("expected semantic reviewer role to receive patch queue review task, got %+v", work)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          criticID,
		CoordinationMode: "trust_first",
		Summary:          "semantic UX critic claims patch queue review task",
	}); err != nil {
		t.Fatalf("claim patch review task by semantic reviewer: %v", err)
	}
}

func TestTrustFirstPatchQueueReviewAllowsIntegratorActor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-patch-review-integrator-routing"
		projectID    = "project-patch-review-integrator-routing"
		leadID       = "alpha"
		integratorID = "zeta"
		taskID       = "task-patch-queue-review-integrator"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               integratorID,
		RoleType:              sqlite.ProjectRoleIntegrator,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign integrator role: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "review", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  leadID,
		Priority:     "critical",
		Title:        "Review patch queue candidate",
		Description:  "Patch queue candidate is ready for independent review.",
		TaskKind:     "COORDINATION",
		TaskTemplate: "integration",
		ProjectID:    projectID,
		ProjectLane:  "review",
		Tags:         []string{"review", "patch_queue", "project"},
	}, graph); err != nil {
		t.Fatalf("create patch review task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach patch review task: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          integratorID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get integrator work next: %v", err)
	}
	if !work.HasWork || work.Task == nil || work.Task.TaskID != taskID {
		t.Fatalf("expected integrator to receive patch queue review task under trust-first, got %+v", work)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          integratorID,
		CoordinationMode: "trust_first",
		Summary:          "integrator claims patch queue review task",
	}); err != nil {
		t.Fatalf("claim patch review task by integrator: %v", err)
	}
}

func TestPatchQueueReviewTaskAllowsSemanticRegisteredIntegratorWithoutActiveProjectRole(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-patch-review-semantic-integrator"
		projectID    = "project-patch-review-semantic-integrator"
		leadID       = "alpha"
		integratorID = "zeta"
		taskID       = "task-patch-queue-review-semantic-integrator"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     integratorID,
		OwnerUserID: "developer",
		DisplayName: integratorID,
		Role:        "patch queue integrator and release captain",
	}); err != nil {
		t.Fatalf("register semantic integrator role: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "review", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  leadID,
		Priority:     "critical",
		Title:        "Review patch queue candidate",
		Description:  "Patch queue candidate is ready for independent review.\n\n- queue_id: queue-1\n- item_id: item-1\n- branch_id: branch-1",
		TaskKind:     "COORDINATION",
		TaskTemplate: "integration",
		ProjectID:    projectID,
		ProjectLane:  "review",
		Tags:         []string{"review", "patch_queue", "project"},
	}, graph); err != nil {
		t.Fatalf("create patch review task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach patch review task: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          integratorID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  taskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get semantic integrator work next: %v", err)
	}
	if !work.HasWork || work.Task == nil || work.Task.TaskID != taskID {
		t.Fatalf("expected semantic integrator role to receive patch queue review task, got %+v", work)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          integratorID,
		CoordinationMode: "trust_first",
		Summary:          "semantic integrator claims patch queue review task",
	}); err != nil {
		t.Fatalf("claim patch review task by semantic integrator: %v", err)
	}
}

func TestProjectRoleLaneTasksRequireMatchingProjectRole(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID     = "ws-project-role-lane-routing"
		projectID       = "project-role-lane-routing"
		leadID          = "alpha"
		builderID       = "beta"
		reviewerID      = "epsilon"
		integratorID    = "zeta"
		reviewTaskID    = "task-role-lane-review"
		integrateTaskID = "task-role-lane-integration"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	for _, role := range []struct {
		agentID string
		role    string
		scope   string
	}{
		{builderID, sqlite.ProjectRoleImplementer, `{"paths":["src/**","package.json"]}`},
		{reviewerID, sqlite.ProjectRoleReviewer, `{}`},
		{integratorID, sqlite.ProjectRoleIntegrator, `{"paths":["src/**","package.json"]}`},
	} {
		if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			AgentID:               role.agentID,
			RoleType:              role.role,
			WriteScopeJSON:        role.scope,
			ActorID:               leadID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
			PromptContextSurface:  "project.role.assign",
		}); err != nil {
			t.Fatalf("assign %s role to %s: %v", role.role, role.agentID, err)
		}
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, task := range []struct {
		id    string
		title string
		lane  string
	}{
		{reviewTaskID, "Review runnable evidence and product risks", "review"},
		{integrateTaskID, "Integrate implementation lanes into the canonical product branch", "integration"},
	} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:  workspaceID,
			TaskID:       task.id,
			OwnerUserID:  leadID,
			Priority:     "high",
			Title:        task.title,
			TaskKind:     "COORDINATION",
			TaskTemplate: "integration",
			ProjectID:    projectID,
			ProjectLane:  task.lane,
			Tags:         []string{"project", task.lane},
		}, graph); err != nil {
			t.Fatalf("create %s task: %v", task.lane, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      task.id,
			LinkedBy:    leadID,
		}); err != nil {
			t.Fatalf("attach %s task: %v", task.lane, err)
		}
	}

	builderWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       builderID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get builder work next: %v", err)
	}
	if builderWork.HasWork || builderWork.Reason != "project_role_lane_required" || builderWork.Packet == nil {
		t.Fatalf("expected builder to be routed away from review/integration lanes, got %+v", builderWork)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      reviewTaskID,
		AgentID:     builderID,
		Summary:     "builder should not claim review lane",
	}); err == nil || !strings.Contains(err.Error(), "active REVIEWER role") {
		t.Fatalf("expected builder review claim rejection, got %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      integrateTaskID,
		AgentID:     reviewerID,
		Summary:     "reviewer should not claim integration lane",
	}); err == nil || !strings.Contains(err.Error(), "active INTEGRATOR role") {
		t.Fatalf("expected reviewer integration claim rejection, got %v", err)
	}

	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     reviewerID,
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if !reviewerWork.HasWork || reviewerWork.Task == nil || reviewerWork.Task.TaskID != reviewTaskID {
		t.Fatalf("expected reviewer to receive review task, got %+v", reviewerWork)
	}
	integratorWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     integratorID,
	})
	if err != nil {
		t.Fatalf("get integrator work next: %v", err)
	}
	if !integratorWork.HasWork || integratorWork.Task == nil || integratorWork.Task.TaskID != integrateTaskID {
		t.Fatalf("expected integrator to receive integration task, got %+v", integratorWork)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      reviewTaskID,
		AgentID:     reviewerID,
		Summary:     "reviewer claims review lane",
	}); err != nil {
		t.Fatalf("claim review task by reviewer: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      integrateTaskID,
		AgentID:     integratorID,
		Summary:     "integrator claims integration lane",
	}); err != nil {
		t.Fatalf("claim integration task by integrator: %v", err)
	}
}

func TestReviewIntegrationLanesDoNotBootstrapWithoutRoleMatrix(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID     = "ws-project-role-lane-no-bootstrap"
		projectID       = "project-role-lane-no-bootstrap"
		agentID         = "beta"
		reviewTaskID    = "task-no-bootstrap-review"
		integrateTaskID = "task-no-bootstrap-integration"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "No review bootstrap",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, task := range []struct {
		id    string
		title string
		lane  string
	}{
		{reviewTaskID, "Review product evidence", "review"},
		{integrateTaskID, "Integrate accepted work", "integration"},
	} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:  workspaceID,
			TaskID:       task.id,
			OwnerUserID:  agentID,
			Priority:     "high",
			Title:        task.title,
			TaskKind:     "COORDINATION",
			TaskTemplate: "integration",
			ProjectID:    projectID,
			ProjectLane:  task.lane,
			Tags:         []string{"project", task.lane},
		}, graph); err != nil {
			t.Fatalf("create %s task: %v", task.lane, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      task.id,
			LinkedBy:    agentID,
		}); err != nil {
			t.Fatalf("attach %s task: %v", task.lane, err)
		}
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get strict work next: %v", err)
	}
	if work.HasWork || work.Reason != "project_role_lane_required" || work.Packet == nil {
		t.Fatalf("expected review/integration lanes to avoid generic bootstrap, got %+v", work)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      reviewTaskID,
		AgentID:     agentID,
		Summary:     "review still needs reviewer capability",
	}); err == nil || !strings.Contains(err.Error(), "active REVIEWER role") {
		t.Fatalf("expected strict review claim to require reviewer capability, got %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      integrateTaskID,
		AgentID:     agentID,
		Summary:     "integration still needs integrator capability",
	}); err == nil || !strings.Contains(err.Error(), "active INTEGRATOR role") {
		t.Fatalf("expected strict integration claim to require integrator capability, got %v", err)
	}
}

func TestTrustFirstReviewIntegrationRoleLanesAreAdvisory(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		lane string
	}{
		{name: "review", lane: "review"},
		{name: "integration", lane: "integration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			workspaceID := "ws-trust-first-advisory-" + tc.name
			projectID := "project-trust-first-advisory-" + tc.name
			taskID := "task-trust-first-advisory-" + tc.name
			agentID := "beta"

			seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
			if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
				WorkspaceID: workspaceID,
				ProjectID:   projectID,
				Title:       "Trust-first advisory role lanes",
				CreatedBy:   agentID,
			}); err != nil {
				t.Fatalf("create project: %v", err)
			}
			createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Trust-first "+tc.lane+" lane", tc.lane, "high")

			work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
				WorkspaceID:      workspaceID,
				AgentID:          agentID,
				IncludePacket:    true,
				CoordinationMode: "trust_first",
			})
			if err != nil {
				t.Fatalf("get trust-first work next: %v", err)
			}
			if !work.HasWork || work.Task == nil || work.Task.TaskID != taskID {
				t.Fatalf("expected trust-first to treat %s role lane as advisory, got %+v", tc.lane, work)
			}
			if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
				WorkspaceID:      workspaceID,
				TaskID:           taskID,
				AgentID:          agentID,
				CoordinationMode: "trust_first",
				Summary:          "trust-first role lane is advisory",
			}); err != nil {
				t.Fatalf("expected trust-first %s claim to treat role as advisory: %v", tc.lane, err)
			}
		})
	}
}

func TestGetAgentWorkNextRoutesStrategyProfileToStrategyLane(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-agent-project-strategy-routing"
		projectID    = "project-strategy-routing"
		strategistID = "alpha"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{strategistID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Strategy Routing",
		CreatedBy:   strategistID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-root-strategy", "Frame project strategy and task decomposition", "strategy", "critical")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-build-slice", "Implement runnable product slice", "implementation", "high")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        strategistID,
		Bio:            "Lead project framing through shared design docs and task decomposition.",
		Specialization: "autonomous project strategy",
		Tags:           []string{"strategist", "generalist", "coordination", "shared design docs", "task decomposition"},
		Metadata: map[string]any{
			"default_work_mode":      "generalist",
			"primary_specialization": "autonomous project strategy",
		},
	}); err != nil {
		t.Fatalf("upsert strategist profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     strategistID,
	})
	if err != nil {
		t.Fatalf("get strategist work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != "task-root-strategy" {
		t.Fatalf("expected strategist to receive strategy lane root task, got %+v", result)
	}
}

func TestGetAgentWorkNextRoutesBuilderAwayFromClaimedStrategyRoot(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-project-builder-routing"
		projectID   = "project-builder-routing"
		leadID      = "alpha"
		builderID   = "beta"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Builder Routing",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	designDocID := "doc.builder-routing.design"
	implementationPlanDocID := "doc.builder-routing.plan"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 leadID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("open project profile gates: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "Lead owns implementation gate opening.",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim strategic lead: %v", err)
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "Design accepted; implementation lane may start.",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-root-strategy", "Frame project strategy and task decomposition", "strategy", "critical")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-build-slice", "Implement runnable product slice", "implementation", "high")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        builderID,
		Bio:            "Build only after checking the shared project record and current blockers.",
		Specialization: "image processing pipeline",
		Tags:           []string{"builder", "generalist", "typed arrays", "export", "algorithm verification"},
		Metadata: map[string]any{
			"default_work_mode":      "generalist",
			"primary_specialization": "image processing pipeline",
		},
	}); err != nil {
		t.Fatalf("upsert builder profile: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      "task-root-strategy",
		AgentID:     leadID,
		Summary:     "active strategic lead owns root-level strategy lane",
	}); err != nil {
		t.Fatalf("claim root strategy task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     builderID,
	})
	if err != nil {
		t.Fatalf("get builder work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != "task-build-slice" {
		t.Fatalf("expected builder to skip claimed strategy root and receive implementation lane task, got %+v", result)
	}
	if result.ProjectLane != "implementation" {
		t.Fatalf("expected implementation lane digest, got %+v", result)
	}
}

func TestGetAgentWorkNextReviewerSkipsStrategyLaneSmokeRoot(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-project-reviewer-lane-routing"
		projectID   = "project-reviewer-lane-routing"
		repoID      = "repo-reviewer-lane-routing"
		reviewerID  = "delta"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, reviewerID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, reviewerID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, reviewerID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, reviewerID, "branch-reviewer-lane-routing")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-root-smoke", "Signal Loom autonomous coordination smoke", "strategy", "critical")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-review-evidence", "Review branch evidence and acceptance notes", "review", "high")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewerID,
		Bio:            "Challenge implementation quality and verify completion evidence.",
		Specialization: "adversarial review and integration quality",
		Tags:           []string{"generalist", "code", "bug finding", "acceptance criteria", "coordination evidence", "merge readiness"},
		Metadata: map[string]any{
			"default_work_mode":      "generalist",
			"primary_specialization": "adversarial review and integration quality",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     reviewerID,
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != "task-review-evidence" {
		t.Fatalf("expected reviewer to skip strategy-lane smoke root and receive review lane task, got %+v", result)
	}
	if result.ProjectLane != "review" {
		t.Fatalf("expected review lane digest, got %+v", result)
	}
}

func TestGetAgentWorkNextReviewerRoleFallbackSkipsStrategyLaneRoot(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-reviewer-role-fallback"
		projectID   = "project-reviewer-role-fallback"
		repoID      = "repo-reviewer-role-fallback"
		reviewerID  = "delta"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     reviewerID,
		OwnerUserID: "developer",
		DisplayName: "Delta",
		Role:        "reviewer",
	}); err != nil {
		t.Fatalf("register reviewer: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, reviewerID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, reviewerID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, reviewerID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, reviewerID, "branch-reviewer-role-fallback")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-root-smoke", "Causal Board autonomous coordination smoke", "strategy", "critical")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-review-evidence", "Review branch evidence and acceptance notes", "review", "high")

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     reviewerID,
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != "task-review-evidence" {
		t.Fatalf("expected reviewer role fallback to receive review lane task, got %+v", result)
	}
}

func TestGetAgentWorkNextIntegratorSkipsStrategyLaneRoot(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID  = "ws-agent-integrator-lane-routing"
		projectID    = "project-integrator-lane-routing"
		repoID       = "repo-integrator-lane-routing"
		integratorID = "zeta"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, integratorID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, integratorID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, integratorID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, integratorID, "branch-integrator-lane-routing")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-root-strategy", "Frame project strategy and task decomposition", "strategy", "critical")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-browser-validation", "Validate browser smoke evidence for blocked branch", "validation", "high")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        integratorID,
		Bio:            "Integrate reviewed work and assemble release evidence.",
		Specialization: "integrator",
		Tags:           []string{"integrator", "release evidence"},
		Metadata: map[string]any{
			"default_work_mode":      "integrator",
			"primary_specialization": "integrator",
		},
	}); err != nil {
		t.Fatalf("upsert integrator profile: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     integratorID,
	})
	if err != nil {
		t.Fatalf("get integrator work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != "task-browser-validation" {
		t.Fatalf("expected integrator to skip strategy-lane root and receive validation lane task, got %+v", result)
	}
}

func TestGetAgentWorkNextRoutesUnlanedAutonomousCoordinationRootToStrategist(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-unlaned-autonomous-root"
		strategist  = "alpha"
		reviewer    = "delta"
		rootTaskID  = "task-causal-board-root"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agent := range []struct {
		id   string
		role string
	}{
		{id: strategist, role: "strategist"},
		{id: reviewer, role: "reviewer"},
	} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agent.id,
			OwnerUserID: "developer",
			DisplayName: agent.id,
			Role:        agent.role,
		}); err != nil {
			t.Fatalf("register %s: %v", agent.id, err)
		}
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	createAgentWorkTaskWithTemplate(t, ctx, store, workspaceID, rootTaskID, "Sub-pixel art web app", "There is intentionally only one root task. A strategic agent should establish the project, write the design and test approach, create any needed subtasks, and coordinate builders, reviewer, and tester through Rhizome.", model.TaskTemplateProject, "", "critical")

	reviewerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     reviewer,
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerResult.HasWork {
		t.Fatalf("expected reviewer to skip unlaned autonomous coordination root, got %+v", reviewerResult)
	}

	strategistResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     strategist,
	})
	if err != nil {
		t.Fatalf("get strategist work next: %v", err)
	}
	if !strategistResult.HasWork || strategistResult.Task == nil || strategistResult.Task.TaskID != rootTaskID {
		t.Fatalf("expected strategist to receive unlaned autonomous coordination root, got %+v", strategistResult)
	}
}

func TestGetAgentWorkNextTrustFirstReviewerSkipsMixedStrategyRoot(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-mixed-strategy-root"
		projectID   = "project-mixed-strategy-root"
		strategist  = "alpha"
		reviewer    = "theta"
		rootTaskID  = "task-clearpress-root"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{strategist, reviewer})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Clearpress",
		CreatedBy:   strategist,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	rootDescription := "Autonomous Clearpress MVP run. Create one project, decompose into semantic deliverables, self-select work by profile and current load, and converge on a runnable local web app with editor core, persistence, tests, browser and visual evidence."
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       rootTaskID,
		OwnerUserID:  "developer",
		Priority:     "critical",
		Title:        "Autonomous Clearpress MVP run",
		Description:  rootDescription,
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateIntegration,
		ProjectID:    projectID,
		ProjectLane:  "strategy",
	}, graph); err != nil {
		t.Fatalf("create mixed strategy root: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      rootTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach mixed strategy root: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewer,
		Specialization: "Clearpress browser smoke and accessibility verifier; strong in local dev-server probing, E2E happy path, console/network checks, keyboard shortcuts, reload persistence, and viewport checks",
		Tags:           []string{"browser smoke and accessibility verifier", "E2E happy path", "console/network checks", "viewport checks"},
		Metadata: map[string]any{
			"default_work_mode":      "Clearpress browser smoke and accessibility verifier; strong in local dev-server probing, E2E happy path, console/network checks, keyboard shortcuts, reload persistence, and viewport checks",
			"primary_specialization": "Clearpress browser smoke and accessibility verifier; strong in local dev-server probing, E2E happy path, console/network checks, keyboard shortcuts, reload persistence, and viewport checks",
			"reflection_scope":       "artifact",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        strategist,
		Specialization: "Clearpress autonomous product coordinator; bootstraps the root task into one project and opens semantic deliverable tasks",
		Tags:           []string{"strategy", "coordination", "task decomposition"},
		Metadata: map[string]any{
			"default_work_mode":      "strategy",
			"primary_specialization": "autonomous project strategy",
			"reflection_scope":       "project",
		},
	}); err != nil {
		t.Fatalf("upsert strategist profile: %v", err)
	}

	reviewerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            reviewer,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerResult.HasWork || reviewerResult.Task != nil || reviewerResult.Reason == "task_frontier_available" {
		if reviewerResult.Packet != nil && reviewerResult.Packet.Frontier != nil {
			t.Fatalf("reviewer must not claim or see a frontier for mixed strategy root, got result=%+v frontier=%+v", reviewerResult, reviewerResult.Packet.Frontier.Candidates)
		}
		t.Fatalf("reviewer must not claim or see a frontier for mixed strategy root, got %+v", reviewerResult)
	}

	strategistResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          strategist,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get strategist work next: %v", err)
	}
	if !strategistResult.HasWork || strategistResult.Task == nil || strategistResult.Task.TaskID != rootTaskID {
		t.Fatalf("strategist should receive mixed strategy root, got %+v", strategistResult)
	}
}

func TestClaimTaskRejectsReviewerForActiveStrategyRoot(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-strategy-root-claim"
		projectID   = "project-strategy-root-claim"
		strategist  = "alpha"
		reviewer    = "theta"
		taskID      = "task-strategy-root-claim"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{strategist, reviewer})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Strategy Root Claim",
		CreatedBy:   strategist,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, strategist)
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Autonomous product strategy root", "strategy", "critical")
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        reviewer,
		Specialization: "Browser smoke and accessibility verifier",
		Tags:           []string{"reviewer", "browser smoke", "visual QA"},
		Metadata: map[string]any{
			"default_work_mode":      "review",
			"primary_specialization": "browser smoke",
			"reflection_scope":       "artifact",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     reviewer,
		Summary:     "reviewer should not claim strategy root",
	})
	if err == nil || !strings.Contains(err.Error(), "strategy/root task") {
		t.Fatalf("expected reviewer strategy/root claim rejection, got %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     strategist,
		Summary:     "active strategic lead claims strategy root",
	}); err != nil {
		t.Fatalf("expected strategic lead to claim strategy root: %v", err)
	}
}

func TestGetAgentWorkNextRoutesCoordinationLaneToStrategist(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-coordination-lane-routing"
		strategist  = "alpha"
		builder     = "beta"
		reviewer    = "epsilon"
		integrator  = "zeta"
		taskID      = "task-meta-reflection"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agent := range []struct {
		id   string
		role string
	}{
		{id: strategist, role: "strategist"},
		{id: builder, role: "builder"},
		{id: reviewer, role: "reviewer"},
		{id: integrator, role: "integrator"},
	} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agent.id,
			OwnerUserID: "developer",
			DisplayName: agent.id,
			Role:        agent.role,
		}); err != nil {
			t.Fatalf("register %s: %v", agent.id, err)
		}
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	createAgentWorkTaskWithTemplate(t, ctx, store, workspaceID, taskID, "Meta-reflection: inspect idle workspace", "Decide whether idle state is real and create coordination follow-ups when useful.", model.TaskTemplateResearch, "coordination", "normal")

	for _, agentID := range []string{builder, reviewer, integrator} {
		result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
		})
		if err != nil {
			t.Fatalf("get %s work next: %v", agentID, err)
		}
		if result.HasWork {
			t.Fatalf("expected %s to skip coordination lane task, got %+v", agentID, result)
		}
	}

	strategistResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     strategist,
	})
	if err != nil {
		t.Fatalf("get strategist work next: %v", err)
	}
	if !strategistResult.HasWork || strategistResult.Task == nil || strategistResult.Task.TaskID != taskID {
		t.Fatalf("expected strategist to receive coordination lane task, got %+v", strategistResult)
	}
}

func TestGetAgentWorkNextReviewerIgnoresGenericTaskWithReviewMentionOnlyInDescription(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-role-routing-description"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"reviewer-epsilon"})
	createAgentWorkTaskWithDetails(t, ctx, store, workspaceID, "task-build-dashboard", "Build dashboard", "Include tests and reviewer acceptance notes before closing.", "critical")

	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        "reviewer-epsilon",
		Bio:            "Challenge implementation quality and verify completion evidence.",
		Specialization: "review",
		Tags:           []string{"reviewer", "generalist", "test design"},
		Metadata: map[string]any{
			"default_work_mode": "generalist",
		},
	}); err != nil {
		t.Fatalf("upsert reviewer profile: %v", err)
	}

	reviewerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     "reviewer-epsilon",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerResult.HasWork {
		t.Fatalf("expected reviewer to ignore generic build task with review wording only in description, got %+v", reviewerResult)
	}
}

func TestGetAgentWorkNextSkipsWaitingDecisionSessionAndBlockedClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-runnable", []string{"agent-a"})
	createAgentWorkTask(t, ctx, store, "ws-agent-runnable", "task-waiting", "critical")
	createAgentWorkTask(t, ctx, store, "ws-agent-runnable", "task-blocked-claim", "high")
	createAgentWorkTask(t, ctx, store, "ws-agent-runnable", "task-free", "normal")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-runnable",
		TaskID:      "task-waiting",
		AgentID:     "agent-a",
		Summary:     "waiting on approval",
	}); err != nil {
		t.Fatalf("claim waiting task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-agent-waiting",
		AgentID:     "agent-a",
		WorkspaceID: "ws-agent-runnable",
		TaskID:      "task-waiting",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create waiting session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.status",
		WorkspaceID: "ws-agent-runnable",
		SessionID:   "sess-agent-waiting",
		AgentID:     "agent-a",
		TaskID:      "task-waiting",
		Summary:     "need human decision",
		Status:      "WAITING_DECISION",
	}); err != nil {
		t.Fatalf("record waiting session coordination: %v", err)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-runnable",
		TaskID:      "task-blocked-claim",
		AgentID:     "agent-a",
		Summary:     "claim before block",
	}); err != nil {
		t.Fatalf("claim blocked task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-agent-blocked-active",
		AgentID:     "agent-a",
		WorkspaceID: "ws-agent-runnable",
		TaskID:      "task-blocked-claim",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create active session with blocked claim: %v", err)
	}
	if err := store.BlockTaskClaim(ctx, "task-blocked-claim", "ws-agent-runnable", "agent-a"); err != nil {
		t.Fatalf("block claimed task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: "ws-agent-runnable",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Reason != "next_pending" {
		t.Fatalf("expected next_pending work result, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != "task-free" {
		t.Fatalf("expected task-free after skipping non-runnable resumptions, got %+v", result.Task)
	}
	if result.Session != nil {
		t.Fatalf("expected no session when selecting fresh pending work, got %+v", result.Session)
	}
}

func TestGetAgentWorkNextExplicitWakeReclaimsBlockedTaskAfterEndedSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-ended-wake", []string{"agent-a"})
	createAgentWorkTask(t, ctx, store, "ws-agent-ended-wake", "task-blocked-ended", "critical")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-ended-wake",
		TaskID:      "task-blocked-ended",
		AgentID:     "agent-a",
		Summary:     "claim before inactive-session block",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID: "ws-agent-ended-wake",
		TaskID:      "task-blocked-ended",
		AgentID:     "agent-a",
		Reason:      "parked after inactive session",
	}); err != nil {
		t.Fatalf("block task claim: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-ended",
		AgentID:     "agent-a",
		WorkspaceID: "ws-agent-ended-wake",
		TaskID:      "task-blocked-ended",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create ended session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.end",
		WorkspaceID: "ws-agent-ended-wake",
		SessionID:   "sess-ended",
		AgentID:     "agent-a",
		TaskID:      "task-blocked-ended",
		Summary:     "inactive session parking completed",
		Status:      "ENDED",
	}); err != nil {
		t.Fatalf("record ended session: %v", err)
	}

	idle, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: "ws-agent-ended-wake",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("get idle work next: %v", err)
	}
	if idle.HasWork {
		t.Fatalf("blocked claim should stay parked without explicit wake, got %+v", idle)
	}

	systemNews, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        "ws-agent-ended-wake",
		AgentID:            "agent-a",
		Trigger:            "system_news",
		CandidateTaskID:    "task-blocked-ended",
		CandidateSessionID: "sess-ended",
	})
	if err != nil {
		t.Fatalf("get system-news work next: %v", err)
	}
	if systemNews.HasWork {
		t.Fatalf("system news must not reclaim blocked task without explicit wake, got %+v", systemNews)
	}

	for _, trigger := range []string{"task_project_fields_updated", "runtime_switch_task", "control_switch_task"} {
		wake, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
			WorkspaceID:        "ws-agent-ended-wake",
			AgentID:            "agent-a",
			IncludePacket:      true,
			Trigger:            trigger,
			CandidateTaskID:    "task-blocked-ended",
			CandidateSessionID: "sess-ended",
		})
		if err != nil {
			t.Fatalf("get explicit wake work next for %s: %v", trigger, err)
		}
		if !wake.HasWork || wake.Reason != "resume_claim" {
			t.Fatalf("expected %s to reclaim task without ended session reuse, got %+v", trigger, wake)
		}
		if wake.Trigger != trigger {
			t.Fatalf("expected trigger %q to round-trip, got %+v", trigger, wake)
		}
		if wake.Session != nil {
			t.Fatalf("ended session must not be reused for %s, got %+v", trigger, wake.Session)
		}
		if wake.Task == nil || wake.Task.TaskID != "task-blocked-ended" {
			t.Fatalf("expected blocked task to be selected for %s, got %+v", trigger, wake.Task)
		}
		if wake.ClaimAction != "claim_required" || wake.SessionAction != "start_new" {
			t.Fatalf("expected blocked claim to require canonical reclaim and fresh session for %s, got %+v", trigger, wake)
		}
		if wake.Packet == nil || wake.Packet.WorkType != "resume_claim" || wake.Packet.CoordinationState != "claimed_without_session" {
			t.Fatalf("expected resume-claim packet for fresh session binding for %s, got %+v", trigger, wake.Packet)
		}
	}
}

func TestGetAgentWorkNextDoesNotSelfWakeBlockedProjectClaimRepairCarrier(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-claim-repair-blocked-self-wake"
		projectID   = "project-claim-repair-blocked-self-wake"
		agentID     = "theta"
		taskID      = "task-project-claim-repair-236b5f086a"
		sessionID   = "sess-claim-repair-ended"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, agentID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, agentID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         agentID,
		Priority:            "high",
		Title:               "Repair project claim scope conflict",
		Description:         "Dedicated project-claim-repair carrier for a blocked product lane.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        model.TaskTemplateGeneric,
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: false,
		Tags:                []string{"project-claim-repair", "strategic-lead", "coordination", "blocker-unblock"},
	}, graph); err != nil {
		t.Fatalf("create claim repair carrier: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    agentID,
	}); err != nil {
		t.Fatalf("attach claim repair carrier: %v", err)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "claim repair carrier before terminal blocker",
	}); err != nil {
		t.Fatalf("claim repair carrier: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "repair carrier published terminal blocker evidence",
	}); err != nil {
		t.Fatalf("block repair carrier claim: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-06-07T04:27:46Z",
	}); err != nil {
		t.Fatalf("create ended repair session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.end",
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "repair carrier completed blocker publication",
		Status:      "ENDED",
	}); err != nil {
		t.Fatalf("record ended repair session: %v", err)
	}

	for _, trigger := range []string{"runtime_switch_task", "control_switch_task", "task_project_fields_updated", "request_resume"} {
		got, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
			WorkspaceID:        workspaceID,
			AgentID:            agentID,
			IncludePacket:      true,
			Trigger:            trigger,
			CandidateTaskID:    taskID,
			CandidateSessionID: sessionID,
		})
		if err != nil {
			t.Fatalf("get explicit repair wake work next for %s: %v", trigger, err)
		}
		if got.HasWork {
			t.Fatalf("blocked project-claim-repair carrier must not self-wake for %s, got %+v", trigger, got)
		}
	}
}

func TestGetAgentWorkNextDoesNotWakeStaleSupersedeAgentStateEvidenceTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-stale-supersede-wake"
		agentID     = "agent-a"
		taskID      = "task-patchq-supersede-project-demo-agent-state"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, taskID,
		"Supersede blocked patch queue item after fresh evidence",
		`Patch queue stewardship task created from agent.work.next frontier project_patch_queue_supersede_available.

project_patch_queue_lifecycle args:
- action: supersede
- evidence_doc_key: agent.beta.current_context
- branch_id: branch-1`,
		model.TaskTemplateIntegration,
		"integration",
		"high",
		[]string{"patch-queue", "supersede", "queue-stewardship"},
	)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "claim before supersede preflight parking",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "preflight parked stale agent-state evidence",
	}); err != nil {
		t.Fatalf("block stale supersede task claim: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-stale-supersede-ended",
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create ended session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.end",
		WorkspaceID: workspaceID,
		SessionID:   "sess-stale-supersede-ended",
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "stale supersede parking completed",
		Status:      "ENDED",
	}); err != nil {
		t.Fatalf("record ended session: %v", err)
	}

	for _, trigger := range []string{"runtime_switch_task", "control_switch_task", "task_project_fields_updated", "request_resume"} {
		got, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
			WorkspaceID:        workspaceID,
			AgentID:            agentID,
			IncludePacket:      true,
			Trigger:            trigger,
			CandidateTaskID:    taskID,
			CandidateSessionID: "sess-stale-supersede-ended",
		})
		if err != nil {
			t.Fatalf("get explicit wake work next for %s: %v", trigger, err)
		}
		if got.HasWork {
			t.Fatalf("stale supersede agent-state task must stay parked for %s, got %+v", trigger, got)
		}
	}
}

func TestGetAgentWorkNextSkipsBlockedTaskTransitionAndSelectsOtherWork(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-blocked-transition", []string{"agent-a", "agent-b"})
	createAgentWorkTask(t, ctx, store, "ws-agent-blocked-transition", "task-blocked-transition", "critical")
	createAgentWorkTask(t, ctx, store, "ws-agent-blocked-transition", "task-free-after-block", "normal")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-blocked-transition",
		TaskID:      "task-blocked-transition",
		AgentID:     "agent-a",
		Summary:     "claim before explicit blocked transition",
	}); err != nil {
		t.Fatalf("claim task before block: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID: "ws-agent-blocked-transition",
		TaskID:      "task-blocked-transition",
		AgentID:     "agent-a",
		Reason:      "needs operator input",
	}); err != nil {
		t.Fatalf("block task with event: %v", err)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, "ws-agent-blocked-transition", "task-blocked-transition", model.TaskClaimStatusBlocked)
	assertTaskStatus(t, ctx, store, "task-blocked-transition", model.TaskStatusRunning)

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: "ws-agent-blocked-transition",
		AgentID:     "agent-b",
	})
	if err != nil {
		t.Fatalf("get agent-b work next: %v", err)
	}
	if !result.HasWork || result.Reason != "next_pending" {
		t.Fatalf("expected other agent to receive pending work after block, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != "task-free-after-block" {
		t.Fatalf("expected task-free-after-block after blocked transition, got %+v", result.Task)
	}
}

func TestGetAgentWorkNextUsesWakeTriggerForWaitingDecisionSession(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-trigger", []string{"agent-a"})
	createAgentWorkTask(t, ctx, store, "ws-agent-trigger", "task-waiting", "critical")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-trigger",
		TaskID:      "task-waiting",
		AgentID:     "agent-a",
		Summary:     "waiting on decision",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-agent-waiting",
		AgentID:     "agent-a",
		WorkspaceID: "ws-agent-trigger",
		TaskID:      "task-waiting",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create waiting session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.status",
		WorkspaceID: "ws-agent-trigger",
		SessionID:   "sess-agent-waiting",
		AgentID:     "agent-a",
		TaskID:      "task-waiting",
		Summary:     "need human decision",
		Status:      "WAITING_DECISION",
	}); err != nil {
		t.Fatalf("record waiting session coordination: %v", err)
	}

	idle, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: "ws-agent-trigger",
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("get agent work next without trigger: %v", err)
	}
	if idle.HasWork {
		t.Fatalf("expected no default work for waiting session, got %+v", idle)
	}
	if idle.TimeAuthority.WorkspaceID != "ws-agent-trigger" || idle.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected workspace time authority on idle work.next result, got %+v", idle.TimeAuthority)
	}
	if idle.GeneratedAt != idle.TimeAuthority.ReferenceAt {
		t.Fatalf("expected idle work.next generated_at %q to mirror time authority reference_at %q", idle.GeneratedAt, idle.TimeAuthority.ReferenceAt)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     "ws-agent-trigger",
		AgentID:         "agent-a",
		Trigger:         "inbound_message",
		CandidateTaskID: "task-waiting",
	})
	if err != nil {
		t.Fatalf("get agent work next with trigger: %v", err)
	}
	if !result.HasWork || result.Reason != "resume_session" {
		t.Fatalf("expected resume_session work result, got %+v", result)
	}
	if result.Trigger != "inbound_message" {
		t.Fatalf("expected trigger to round-trip, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != "task-waiting" {
		t.Fatalf("expected task-waiting, got %+v", result.Task)
	}
	if result.Session == nil || result.Session.SessionID != "sess-agent-waiting" || result.Session.Status != "WAITING_DECISION" {
		t.Fatalf("expected waiting session, got %+v", result.Session)
	}
	if result.ClaimAction != "reuse_claim" || result.SessionAction != "resume_inactive" {
		t.Fatalf("expected explicit reuse/resume actions, got %+v", result)
	}
	if result.ResumeSummary == "" {
		t.Fatalf("expected resume summary, got %+v", result)
	}

	projectFieldsResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     "ws-agent-trigger",
		AgentID:         "agent-a",
		Trigger:         "task_project_fields_updated",
		CandidateTaskID: "task-waiting",
	})
	if err != nil {
		t.Fatalf("get agent work next with project fields trigger: %v", err)
	}
	if !projectFieldsResult.HasWork || projectFieldsResult.Trigger != "task_project_fields_updated" {
		t.Fatalf("expected project fields trigger to resume waiting session, got %+v", projectFieldsResult)
	}
	if !strings.Contains(projectFieldsResult.ResumeSummary, "Task project fields changed") {
		t.Fatalf("expected project-fields resume summary, got %+v", projectFieldsResult)
	}

	switchTaskResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     "ws-agent-trigger",
		AgentID:         "agent-a",
		Trigger:         "runtime_switch_task",
		CandidateTaskID: "task-waiting",
	})
	if err != nil {
		t.Fatalf("get agent work next with runtime switch-task trigger: %v", err)
	}
	if !switchTaskResult.HasWork || switchTaskResult.Trigger != "runtime_switch_task" || switchTaskResult.SessionAction != "resume_inactive" {
		t.Fatalf("expected runtime switch-task trigger to resume waiting session, got %+v", switchTaskResult)
	}

	systemNewsResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     "ws-agent-trigger",
		AgentID:         "agent-a",
		Trigger:         "system_news",
		CandidateTaskID: "task-waiting",
	})
	if err != nil {
		t.Fatalf("get agent work next with system news trigger: %v", err)
	}
	if systemNewsResult.HasWork {
		t.Fatalf("generic system news must not resume waiting decision sessions, got %+v", systemNewsResult)
	}
}

func TestGetAgentWorkNextRuntimeSwitchPinsUnclaimedCandidateTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-switch-pins-candidate", []string{"agent-a"})
	createAgentWorkTask(t, ctx, store, "ws-agent-switch-pins-candidate", "task-other-high", "critical")
	createAgentWorkTask(t, ctx, store, "ws-agent-switch-pins-candidate", "task-delegated-low", "low")

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     "ws-agent-switch-pins-candidate",
		AgentID:         "agent-a",
		Trigger:         "runtime_switch_task",
		CandidateTaskID: "task-delegated-low",
	})
	if err != nil {
		t.Fatalf("get agent work next with runtime switch-task trigger: %v", err)
	}
	if !result.HasWork || result.Trigger != "runtime_switch_task" {
		t.Fatalf("expected runtime switch-task to select work, got %+v", result)
	}
	if result.Task == nil || result.Task.TaskID != "task-delegated-low" {
		t.Fatalf("runtime switch-task must pin the delegated candidate, got %+v", result.Task)
	}
	if result.ClaimAction != "claim_required" || result.SessionAction != "start_new" {
		t.Fatalf("expected fresh delegated task claim/start actions, got %+v", result)
	}

	systemNewsResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     "ws-agent-switch-pins-candidate",
		AgentID:         "agent-a",
		Trigger:         "system_news",
		CandidateTaskID: "task-delegated-low",
	})
	if err != nil {
		t.Fatalf("get agent work next with system news trigger: %v", err)
	}
	if systemNewsResult.HasWork {
		t.Fatalf("system news must not pin an unclaimed candidate task, got %+v", systemNewsResult)
	}
}

func TestGetAgentWorkNextRuntimeSwitchCandidateMissDoesNotFallbackToOtherPendingTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-agent-switch-candidate-miss"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a", "agent-b"})
	createAgentWorkTask(t, ctx, store, workspaceID, "task-delegated", "critical")
	createAgentWorkTask(t, ctx, store, workspaceID, "task-other", "critical")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      "task-delegated",
		AgentID:     "agent-b",
		Summary:     "peer already claimed delegated candidate",
	}); err != nil {
		t.Fatalf("claim delegated candidate by peer: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     workspaceID,
		AgentID:         "agent-a",
		Trigger:         "runtime_switch_task",
		CandidateTaskID: "task-delegated",
	})
	if err != nil {
		t.Fatalf("get agent work next with missed switch-task candidate: %v", err)
	}
	if result.HasWork || result.Reason != "trigger_task_claimed_by_other" || result.Trigger != "runtime_switch_task" {
		t.Fatalf("runtime switch-task candidate miss must not fall back to other pending task, got %+v", result)
	}
}

func TestGetAgentWorkNextIncludesTypedPacketAndScopedAdvisory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-packet", []string{"agent-a"})
	createAgentWorkTask(t, ctx, store, "ws-agent-packet", "task-proof", "high")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-packet",
		TaskID:      "task-proof",
		AgentID:     "agent-a",
		Summary:     "waiting on proof review",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-proof",
		AgentID:     "agent-a",
		WorkspaceID: "ws-agent-packet",
		TaskID:      "task-proof",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create waiting session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:          "session.decision_needed",
		WorkspaceID:        "ws-agent-packet",
		SessionID:          "sess-proof",
		AgentID:            "agent-a",
		TaskID:             "task-proof",
		Summary:            "need formal approval",
		Status:             "WAITING_DECISION",
		DecisionNeededFrom: "human",
		DecisionType:       "approval",
		RelatedDocKeys:     []string{"task.task-proof"},
	}); err != nil {
		t.Fatalf("record waiting session coordination: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     "ws-agent-packet",
		AgentID:         "agent-a",
		IncludePacket:   true,
		IncludeAdvisory: true,
		FrontierLimit:   2,
		Trigger:         "inbound_message",
		CandidateTaskID: "task-proof",
	})
	if err != nil {
		t.Fatalf("get agent work next with packet: %v", err)
	}
	if result.Packet == nil {
		t.Fatalf("expected packet, got %+v", result)
	}
	if result.Packet.WorkType != "resume_session" || result.Packet.CoordinationState != "waiting_decision" {
		t.Fatalf("unexpected packet core: %+v", result.Packet)
	}
	if result.Packet.PreferredTransition != "await_decision" || result.Packet.WhyNow != "inbound_message" {
		t.Fatalf("unexpected packet transition hints: %+v", result.Packet)
	}
	if result.Packet.Resume == nil || result.Packet.Resume.SessionID != "sess-proof" {
		t.Fatalf("expected resume packet, got %+v", result.Packet)
	}
	if result.Packet.Decision == nil || result.Packet.Decision.NeededFrom != "human" || result.Packet.Decision.DecisionType != "approval" {
		t.Fatalf("expected decision packet, got %+v", result.Packet)
	}
	if result.Packet.Gate == nil || result.Packet.Gate.GateState != "open" || result.Packet.Gate.GateType != "approval" {
		t.Fatalf("expected gate packet, got %+v", result.Packet)
	}
	if len(result.Packet.ContextHints.AnchorTaskIDs) == 0 || result.Packet.ContextHints.AnchorTaskIDs[0] != "task-proof" {
		t.Fatalf("expected task anchor hints, got %+v", result.Packet.ContextHints)
	}
	if result.Packet.Advisory == nil || strings.TrimSpace(result.Packet.Advisory.ProtoClusterID) == "" {
		t.Fatalf("expected scoped advisory packet, got %+v", result.Packet)
	}
	if result.Packet.Advisory.Control == nil || result.Packet.Advisory.Corridor == nil {
		t.Fatalf("expected control and corridor advisory, got %+v", result.Packet.Advisory)
	}
}

func TestGetAgentWorkNextTaskFrontierIncludesRosterFitAndRoleAdvisory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-task-frontier"
		projectID   = "project-agent-task-frontier"
		agentID     = "agent-ui"
		peerID      = "agent-peer"
		uiTaskID    = "task-ui-visual-check"
		peerTaskID  = "task-peer-active"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID, peerID})
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		Specialization: "UI/UX frontend builder",
		Tags:           []string{"ui", "ux", "frontend", "visual-qa"},
		ToolsAccess:    []string{"browser", "chrome-devtools"},
	}); err != nil {
		t.Fatalf("upsert agent profile: %v", err)
	}
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, agentID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, agentID)
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "frontier can expose implementation candidates",
		ActorID:               agentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate frontier task graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               uiTaskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Inspect responsive shell",
		Description:          "Validate the active web app candidate and materialize findings.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","required_work_modes":["implementation"],"preferred_skills":["ui","visual-qa"],"preferred_tools":["browser","chrome-devtools"]}`,
		WriteScopeHints:      []string{"web/**"},
	}, graph); err != nil {
		t.Fatalf("create frontier task with requirements: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      uiTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach frontier task: %v", err)
	}
	createAgentWorkTaskWithDetails(t, ctx, store, workspaceID, peerTaskID, "Peer active task", "Already owned by another agent", "normal")
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      peerTaskID,
		AgentID:     peerID,
		Summary:     "peer is active",
	}); err != nil {
		t.Fatalf("claim peer task: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      3,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get task frontier: %v", err)
	}
	if !work.HasWork || work.Task != nil || work.Reason != "task_frontier_available" || work.Packet == nil || work.Packet.Frontier == nil {
		t.Fatalf("expected task_frontier_available packet without preselected task, got %+v", work)
	}
	frontier := work.Packet.Frontier
	if strings.TrimSpace(frontier.GenerationID) == "" || len(frontier.Candidates) == 0 {
		t.Fatalf("expected generated frontier candidates, got %+v", frontier)
	}
	var uiCandidate *sqlite.AgentWorkTaskFrontierCandidate
	for i := range frontier.Candidates {
		if frontier.Candidates[i].Task.TaskID == uiTaskID {
			uiCandidate = &frontier.Candidates[i]
			break
		}
	}
	if uiCandidate == nil {
		t.Fatalf("expected UI task candidate in frontier, got %+v", frontier.Candidates)
	}
	if uiCandidate.Blocked {
		t.Fatalf("trust-first role/lane mismatch should be advisory in frontier, got %+v", uiCandidate)
	}
	if uiCandidate.Fit.Level == "blocked" || uiCandidate.Fit.Score <= 0 {
		t.Fatalf("expected positive fit evidence, got %+v", uiCandidate.Fit)
	}
	if !containsString(uiCandidate.Fit.RequiredWorkModes, "implementation") || !containsString(uiCandidate.Fit.PreferredTools, "browser") || !containsString(uiCandidate.Fit.PreferredSkills, "visual-qa") {
		t.Fatalf("expected task_requirements to feed fit evidence, got %+v", uiCandidate.Fit)
	}
	if strings.Join(uiCandidate.Task.WriteScopeHints, ",") != "web/**" {
		t.Fatalf("expected write scope hints in frontier candidate task, got %+v", uiCandidate.Task)
	}
	if len(uiCandidate.Fit.AdvisoryRoleTypes) == 0 {
		t.Fatalf("expected project lane roles as advisory fit evidence, got %+v", uiCandidate.Fit)
	}
	var peerSeen bool
	for _, rosterAgent := range frontier.Roster {
		if rosterAgent.AgentID != peerID {
			continue
		}
		peerSeen = true
		hasPeerTask := false
		for _, taskID := range rosterAgent.CurrentTaskIDs {
			if taskID == peerTaskID {
				hasPeerTask = true
				break
			}
		}
		if rosterAgent.ActiveTaskCount == 0 || !hasPeerTask {
			t.Fatalf("expected peer roster busyness/current task evidence, got %+v", rosterAgent)
		}
	}
	if !peerSeen {
		t.Fatalf("expected peer in frontier roster, got %+v", frontier.Roster)
	}
}

func TestGetAgentWorkNextBlocksProjectlessClaimBindingResidueTaskInFrontier(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-frontier-projectless-claim-binding-residue"
		agentID     = "delta"
		blockedID   = "task-side-effect-d015891503"
		runnableID  = "task-workspace-doc-cleanup"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		Specialization: "Implementation engineer for source and test work",
		Tags:           []string{"implementation", "foundation", "docs"},
		ToolsAccess:    []string{"shell"},
	}); err != nil {
		t.Fatalf("upsert agent profile: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               blockedID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Resolve foundation side effect for artifact region",
		Description:          "ABPC recovery action: resolve foundation side effect for artifact region.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		ProjectLane:          "implementation",
		Tags:                 []string{"side-effect-resolution", "foundation-effect", "operational-boundary", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_foundation","action_kind":"split_foundation_bucket","decision":"split_tension","side_effect_refs":["side-effect:r10"],"path_bucket":["artifact region"]}`,
	}, graph); err != nil {
		t.Fatalf("create projectless authoritative-scope task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      blockedID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach projectless authoritative-scope task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT OR REPLACE INTO task_claims(
  task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at,
  project_role_id, repo_id, checkout_id, branch_id, write_scope_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		blockedID,
		workspaceID,
		agentID,
		model.TaskClaimStatusReleased,
		"stale project claim binding residue from a failed admission path",
		"2026-06-17T06:28:00Z",
		"2026-06-17T06:28:30Z",
		"2026-06-17T06:28:30Z",
		"",
		"repo-stale",
		"checkout-stale",
		"branch-stale",
		`{"paths":["cmd/**","internal/cli/**","internal/repl/**"]}`,
	); err != nil {
		t.Fatalf("seed stale claim binding residue: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               runnableID,
		OwnerUserID:          "developer",
		Priority:             "normal",
		Title:                "Update workspace implementation notes",
		Description:          "Record implementation notes for the current workspace.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","required_work_modes":["implementation"],"preferred_tools":["shell"]}`,
	}, graph); err != nil {
		t.Fatalf("create runnable workspace task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      runnableID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach runnable workspace task: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      5,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get task frontier: %v", err)
	}
	if !work.HasWork || work.Reason != "task_frontier_available" || work.Packet == nil || work.Packet.Frontier == nil {
		t.Fatalf("expected task frontier with diagnostic blocker, got %+v", work)
	}
	var blockedCandidate, runnableCandidate *sqlite.AgentWorkTaskFrontierCandidate
	for i := range work.Packet.Frontier.Candidates {
		candidate := &work.Packet.Frontier.Candidates[i]
		switch candidate.Task.TaskID {
		case blockedID:
			blockedCandidate = candidate
		case runnableID:
			runnableCandidate = candidate
		}
	}
	if blockedCandidate == nil {
		t.Fatalf("expected R10-shaped projectless task in frontier diagnostics, got %+v", work.Packet.Frontier.Candidates)
	}
	if !blockedCandidate.Blocked || blockedCandidate.BlockReason != "project_claim_admission_unclaimable" || blockedCandidate.Fit.Level != "blocked" {
		t.Fatalf("expected projectless claim-binding residue task to be hard-blocked, got %+v", blockedCandidate)
	}
	if !strings.Contains(blockedCandidate.BlockSummary, "project_id is empty") {
		t.Fatalf("expected admission parity summary, got %q", blockedCandidate.BlockSummary)
	}
	if runnableCandidate == nil || runnableCandidate.Blocked || runnableCandidate.Fit.Score <= 0 {
		t.Fatalf("expected ordinary workspace task to remain selectable, got %+v", runnableCandidate)
	}
	if _, err := store.RecordAgentTaskFrontierDecision(ctx, sqlite.AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: work.Packet.Frontier.GenerationID,
		DecisionState:        "selected",
		SelectedTaskID:       blockedID,
		Summary:              "trying to select a diagnostic claim-admission blocker should fail",
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "was not in frontier generation") {
		t.Fatalf("expected diagnostic frontier candidate to be unselectable, got %v", err)
	}
}

func TestGetAgentWorkNextDirectSelectionBlocksProjectlessClaimBindingResidueTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-direct-projectless-claim-binding-residue"
		agentID     = "delta"
		blockedID   = "task-side-effect-d015891503"
		ordinaryID  = "task-ordinary-workspace"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		Specialization: "Implementation engineer for source and test work",
		Tags:           []string{"implementation", "foundation"},
		ToolsAccess:    []string{"shell"},
	}); err != nil {
		t.Fatalf("upsert agent profile: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               blockedID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Resolve foundation side effect for artifact region",
		Description:          "ABPC recovery action: resolve foundation side effect for artifact region.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		ProjectLane:          "implementation",
		Tags:                 []string{"side-effect-resolution", "foundation-effect", "operational-boundary", "abpc"},
		TaskRequirementsJSON: `{"schema":"artifact_bound_side_effect_resolution_followup.v1","admission_kind":"abpc_recovery_action","abpc_task_class":"side_effect_foundation","action_kind":"split_foundation_bucket","decision":"split_tension","side_effect_refs":["side-effect:r10"],"path_bucket":["artifact region"]}`,
	}, graph); err != nil {
		t.Fatalf("create projectless authoritative-scope task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      blockedID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach projectless authoritative-scope task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT OR REPLACE INTO task_claims(
  task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at,
  project_role_id, repo_id, checkout_id, branch_id, write_scope_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		blockedID,
		workspaceID,
		agentID,
		model.TaskClaimStatusReleased,
		"stale project claim binding residue from a failed admission path",
		"2026-06-17T06:28:00Z",
		"2026-06-17T06:28:30Z",
		"2026-06-17T06:28:30Z",
		"",
		"repo-stale",
		"checkout-stale",
		"branch-stale",
		`{"paths":["cmd/**","internal/cli/**","internal/repl/**"]}`,
	); err != nil {
		t.Fatalf("seed stale claim binding residue: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		IncludePacket:      true,
		EnableTaskFrontier: false,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get direct work: %v", err)
	}
	if work.HasWork || work.Task != nil || work.Reason != "project_claim_admission_unclaimable" || work.Packet == nil || work.Packet.Gate == nil {
		t.Fatalf("expected typed no-work admission blocker, got %+v", work)
	}
	if work.Packet.WorkType != "project_claim_admission_unclaimable" || work.Packet.Gate.GateType != "project_claim_admission_parity" {
		t.Fatalf("expected project claim admission parity packet, got %+v", work.Packet)
	}

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:    workspaceID,
		TaskID:         blockedID,
		AgentID:        agentID,
		RepoID:         "repo-stale",
		CheckoutID:     "checkout-stale",
		BranchID:       "branch-stale",
		WriteScopeJSON: `{"paths":["cmd/**","internal/cli/**","internal/repl/**"]}`,
		Summary:        "bound claim should still be rejected by write admission",
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "project claim bindings require a project task") {
		t.Fatalf("expected write admission to reject projectless bound claim, got %v", err)
	}

	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               ordinaryID,
		OwnerUserID:          "developer",
		Priority:             "normal",
		Title:                "Collect workspace notes",
		Description:          "Collect ordinary workspace notes without project claim scope.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","required_work_modes":["implementation"],"preferred_tools":["shell"]}`,
	}, graph); err != nil {
		t.Fatalf("create ordinary workspace task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      ordinaryID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach ordinary workspace task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      ordinaryID,
		AgentID:     agentID,
		Summary:     "ordinary workspace task remains claimable",
	}); err != nil {
		t.Fatalf("ordinary projectless workspace task should remain claimable: %v", err)
	}
}

func TestGetAgentWorkNextFrontierDelegatesUnknownClaimRuleToAdmission(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-frontier-admission-dirty-checkout"
		projectID   = "project-frontier-admission-dirty-checkout"
		leadID      = "alpha"
		agentID     = "delta"
		peerID      = "theta"
		repoID      = "repo-dirty-checkout"
		taskID      = "task-future-family-scope-conflict"
		peerTaskID  = "task-peer-runtime-lane"
		runnableID  = "task-frontier-runnable"
		checkoutID  = "checkout-delta-future-family"
		branchID    = "branch-delta-future-family"
		peerBranch  = "branch-theta-runtime-lane"
		scopeJSON   = `{"paths":["internal/runtime/**"]}`
	)

	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, agentID, peerID})
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		Specialization: "Implementation engineer for runtime work",
		Tags:           []string{"implementation", "runtime"},
		ToolsAccess:    []string{"shell"},
	}); err != nil {
		t.Fatalf("upsert agent profile: %v", err)
	}
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, agentID, leadID, scopeJSON)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, peerID, leadID, scopeJSON)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              peerTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Hold runtime lane",
		Description:         "Peer implementation task that owns the runtime write scope.",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"internal/runtime/**"},
	}, graph); err != nil {
		t.Fatalf("create peer runtime task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: peerTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach peer runtime task: %v", err)
	}
	peerCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, peerID, `C:\fixtures\agents\theta\runtime-lane`)
	peerBranchRecord, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		BranchID:              peerBranch,
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            peerCheckout.CheckoutID,
		AgentID:               peerID,
		BranchName:            "agent/theta/runtime-lane",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               peerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", peerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register peer runtime branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                peerTaskID,
		AgentID:               peerID,
		RepoID:                repoID,
		CheckoutID:            peerCheckout.CheckoutID,
		BranchID:              peerBranchRecord.BranchID,
		WriteScopeJSON:        scopeJSON,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim peer runtime lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, peerTaskID, peerID),
	}); err != nil {
		t.Fatalf("claim peer runtime task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Continue runtime implementation for future family",
		Description:         "Synthetic admission-rule fixture: no special task tag should be needed for delegated claimability.",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"internal/runtime/**"},
	}, graph); err != nil {
		t.Fatalf("create dirty checkout task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: taskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach future-family task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       runnableID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Collect runtime notes",
		Description:  "Ordinary implementation task without project claim bindings.",
		TaskKind:     "EXECUTION",
		TaskTemplate: "generic",
	}, graph); err != nil {
		t.Fatalf("create runnable task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: runnableID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach runnable task: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      5,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get task frontier: %v", err)
	}
	if !work.HasWork || work.Reason != "task_frontier_available" || work.Packet == nil || work.Packet.Frontier == nil {
		t.Fatalf("expected task frontier with diagnostic blocker, got %+v", work)
	}
	var blockedCandidate *sqlite.AgentWorkTaskFrontierCandidate
	for i := range work.Packet.Frontier.Candidates {
		if work.Packet.Frontier.Candidates[i].Task.TaskID == taskID {
			blockedCandidate = &work.Packet.Frontier.Candidates[i]
			break
		}
	}
	if blockedCandidate == nil || !blockedCandidate.Blocked || blockedCandidate.BlockReason != "project_claim_scope_busy" {
		t.Fatalf("expected future-family task to be blocked by delegated admission, got %+v", blockedCandidate)
	}
	if !strings.Contains(blockedCandidate.BlockSummary, "write_scope_json overlaps") {
		t.Fatalf("expected delegated admission scope-conflict reason, got %q", blockedCandidate.BlockSummary)
	}
	checkout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		CheckoutID:            checkoutID,
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               agentID,
		LocalPath:             `C:\fixtures\agents\delta\future-family`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		DirtyState:            "clean",
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               agentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register candidate checkout: %v", err)
	}
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		BranchID:              branchID,
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               agentID,
		BranchName:            "agent/delta/future-family",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               agentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register candidate branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          agentID,
		RepoID:           repoID,
		CheckoutID:       checkout.CheckoutID,
		BranchID:         branch.BranchID,
		WriteScopeJSON:   scopeJSON,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Summary:          "direct claim must match read-model admission dry-run",
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "write_scope_json overlaps") {
		t.Fatalf("expected write admission to reject scope-conflicting future-family task, got %v", err)
	}
}

func TestGetAgentWorkNextTrustFirstCriticCannotFrontierOrFreshSelectPureImplementation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-task-frontier-critic-implementation"
		projectID   = "project-agent-task-frontier-critic-implementation"
		leadID      = "alpha"
		criticID    = "iota"
		builderID   = "beta"
		taskID      = "task-wire-incident-workflows"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, criticID, builderID})
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        criticID,
		Specialization: "Harsh real-user UI/UX critic; strong in visual QA, usability defects, contrast, clipping, hierarchy, and interaction affordance critique.",
		Tags:           []string{"visual QA", "usability defects", "interaction affordance critique"},
		ToolsAccess:    []string{"shell", "browser", "chrome-devtools"},
	}); err != nil {
		t.Fatalf("upsert critic profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        builderID,
		Specialization: "Frontend application implementer; strong in React/Vite app scaffolds, responsive layout, component architecture, and styling systems.",
		Tags:           []string{"frontend", "implementer", "react"},
		ToolsAccess:    []string{"shell", "browser"},
	}); err != nil {
		t.Fatalf("upsert builder profile: %v", err)
	}
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "profile routing test needs open implementation gate",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate implementation task graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Wire Incident Atlas command workflows across console surfaces",
		Description:          "Implement the command workflow interactions and local UI state for the active web app.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","required_work_modes":["implementation"],"preferred_skills":["frontend","interaction-design","ui-state","react"],"preferred_tools":["shell"]}`,
		WriteScopeHints:      []string{"src/components/**", "src/features/**", "src/app/**", "src/styles/**"},
	}, graph); err != nil {
		t.Fatalf("create pure implementation task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach pure implementation task: %v", err)
	}

	criticWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            criticID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      3,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get critic work next: %v", err)
	}
	if criticWork.HasWork || criticWork.Task != nil || criticWork.Reason == "task_frontier_available" {
		t.Fatalf("critic profile must not receive pure implementation work or frontier, got %+v", criticWork)
	}
	if criticWork.Packet != nil && criticWork.Packet.Frontier != nil {
		t.Fatalf("critic frontier should be suppressed when every candidate is profile-blocked, got %+v", criticWork.Packet.Frontier)
	}

	builderWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            builderID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      3,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get builder work next: %v", err)
	}
	if !builderWork.HasWork || builderWork.Reason != "task_frontier_available" || builderWork.Packet == nil || builderWork.Packet.Frontier == nil {
		t.Fatalf("builder profile should still see implementation frontier, got %+v", builderWork)
	}
	var found bool
	for _, candidate := range builderWork.Packet.Frontier.Candidates {
		if candidate.Task.TaskID != taskID {
			continue
		}
		found = true
		if candidate.Blocked || candidate.Fit.Level == "blocked" {
			t.Fatalf("builder implementation candidate should be claimable, got %+v", candidate)
		}
	}
	if !found {
		t.Fatalf("expected builder frontier to include implementation task, got %+v", builderWork.Packet.Frontier.Candidates)
	}
}

func TestGetAgentWorkNextTaskFrontierRanksUnblockedFitBeforeEarlierBlockedCandidate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-task-frontier-ranking"
		projectID   = "project-agent-task-frontier-ranking"
		agentID     = "agent-ui"
		blockedID   = "task-closed-gate-critical"
		runnableID  = "task-runnable-browser-low"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		Specialization: "UI/UX browser verification",
		Tags:           []string{"ui", "visual-qa", "browser"},
		ToolsAccess:    []string{"browser", "chrome-devtools"},
	}); err != nil {
		t.Fatalf("upsert agent profile: %v", err)
	}
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       projectID,
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, blockedID, "Closed gate implementation task", "This task is intentionally blocked by a closed implementation gate.", "implementation", []string{"implementation"}, true)
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, runnableID, "Run browser visual QA", "Use browser and chrome-devtools to inspect the UI.", model.TaskTemplateIntegration, "qa", "low", []string{"ui", "browser", "visual-qa"})

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      1,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get ranked task frontier: %v", err)
	}
	if work.Packet == nil || work.Packet.Frontier == nil || len(work.Packet.Frontier.Candidates) != 1 {
		t.Fatalf("expected single ranked frontier candidate, got %+v", work)
	}
	candidate := work.Packet.Frontier.Candidates[0]
	if candidate.Task.TaskID != runnableID || candidate.Blocked {
		t.Fatalf("expected runnable high-fit task to survive frontier limit before blocked critical task, got %+v", candidate)
	}
}

func TestGetAgentWorkNextTaskFrontierAllBlockedFallsThroughToDiagnostic(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-task-frontier-all-blocked"
		projectID   = "project-agent-task-frontier-all-blocked"
		agentID     = "agent-ui"
		blockedID   = "task-all-blocked-closed-gate"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       projectID,
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, blockedID, "implementation", false)

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      3,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get all-blocked task frontier: %v", err)
	}
	if work.HasWork || work.Reason != "project_gate_closed" || work.Packet == nil || work.Packet.Frontier != nil {
		t.Fatalf("all-blocked frontier should fall through to closed-gate diagnostic, got %+v", work)
	}
}

func TestGetAgentWorkNextTaskFrontierDoesNotPreemptOwnedClaimResume(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-task-frontier-owned-claim"
		agentID     = "agent-ui"
		ownedID     = "task-owned-claim"
		freshID     = "task-fresh-ui"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		Specialization: "UI/UX browser verification",
		Tags:           []string{"ui", "browser"},
		ToolsAccess:    []string{"browser"},
	}); err != nil {
		t.Fatalf("upsert agent profile: %v", err)
	}
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, ownedID, "Implement claimed browser UI continuation", "Already claimed by this agent; continue the implementation work.", model.TaskTemplateIntegration, "implementation", "normal", []string{"ui", "browser"})
	createAgentWorkTaskWithDetails(t, ctx, store, workspaceID, freshID, "Fresh browser QA task", "Use browser to inspect the UI.", "critical")
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      ownedID,
		AgentID:     agentID,
		Summary:     "durable owned claim without a live session",
	}); err != nil {
		t.Fatalf("claim owned task: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      3,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get work next: %v", err)
	}
	if !work.HasWork || work.Task == nil || work.Task.TaskID != ownedID || work.Reason != "resume_claim" {
		t.Fatalf("frontier must not preempt owned durable claim resume, got %+v", work)
	}
}

func TestTaskClaimSelectedFromFrontierRequiresAndPersistsSelfFitEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-frontier-claim-evidence"
		agentID     = "agent-alpha"
		taskID      = "task-frontier-claim"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createAgentWorkTaskWithDetails(t, ctx, store, workspaceID, taskID, "Claim from frontier", "Claim should carry self-fit evidence.", "high")

	err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		AgentID:              agentID,
		SelectedFromFrontier: true,
		FrontierGenerationID: "frontier-test",
		Summary:              "missing self fit",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) {
		t.Fatalf("expected missing self-fit claim to be rejected, got %v", err)
	}

	err = store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		AgentID:              agentID,
		SelectedFromFrontier: true,
		FrontierGenerationID: "frontier-missing",
		SelfFitSummary:       "claim has self-fit text but no recorded frontier provenance",
		Summary:              "missing frontier provenance",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "was not recorded") {
		t.Fatalf("expected unrecorded frontier claim to be rejected, got %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      3,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("generate task frontier: %v", err)
	}
	if work.Packet == nil || work.Packet.Frontier == nil || strings.TrimSpace(work.Packet.Frontier.GenerationID) == "" {
		t.Fatalf("expected generated frontier to persist claim provenance, got %+v", work)
	}
	frontierGenerationID := work.Packet.Frontier.GenerationID

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		AgentID:              agentID,
		SelectedFromFrontier: true,
		FrontierGenerationID: frontierGenerationID,
		SelfFitSummary:       "UI/browser profile matches the visual QA task and no peer owns it.",
		Summary:              "frontier self-selected before decision receipt",
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "requires selected decision receipt") {
		t.Fatalf("expected offered-only frontier claim to be rejected, got %v", err)
	}

	if _, err := store.RecordAgentTaskFrontierDecision(ctx, sqlite.AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: frontierGenerationID,
		DecisionState:        "selected",
		SelectedTaskID:       taskID,
		Summary:              "UI/browser profile matches the visual QA task and no peer owns it.",
	}); err != nil {
		t.Fatalf("record selected frontier decision: %v", err)
	}

	event, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		AgentID:              agentID,
		SelectedFromFrontier: true,
		FrontierGenerationID: frontierGenerationID,
		SelfFitSummary:       "UI/browser profile matches the visual QA task and no peer owns it.",
		Summary:              "frontier self-selected",
	})
	if err != nil {
		t.Fatalf("expected frontier claim with self-fit evidence to succeed: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime event payload: %v", err)
	}
	if payload["selected_from_frontier"] != true || payload["frontier_generation_id"] != frontierGenerationID {
		t.Fatalf("expected frontier evidence in runtime event payload, got %+v", payload)
	}
	if !strings.Contains(fmt.Sprint(payload["self_fit_summary"]), "visual QA") {
		t.Fatalf("expected self-fit summary in runtime event payload, got %+v", payload)
	}
}

func TestTaskClaimSelectedFromFrontierRejectsBlockedCandidateContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-frontier-blocked-candidate"
		projectID   = "project-frontier-blocked-candidate"
		agentID     = "agent-ui"
		blockedID   = "task-frontier-blocked-gate"
		runnableID  = "task-frontier-runnable-qa"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		Specialization: "UI/UX browser verification",
		Tags:           []string{"ui", "visual-qa", "browser"},
		ToolsAccess:    []string{"browser", "chrome-devtools"},
	}); err != nil {
		t.Fatalf("upsert agent profile: %v", err)
	}
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       projectID,
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, blockedID, "Closed gate implementation task", "This task is visible in frontier context but must not be claimable from frontier evidence.", "implementation", []string{"implementation"}, true)
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, runnableID, "Run browser visual QA", "Use browser and chrome-devtools to inspect the UI.", model.TaskTemplateIntegration, "qa", "low", []string{"ui", "browser", "visual-qa"})

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            agentID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      3,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("generate frontier: %v", err)
	}
	if work.Packet == nil || work.Packet.Frontier == nil || strings.TrimSpace(work.Packet.Frontier.GenerationID) == "" {
		t.Fatalf("expected frontier packet, got %+v", work)
	}
	var sawBlocked bool
	for _, candidate := range work.Packet.Frontier.Candidates {
		if candidate.Task.TaskID == blockedID && candidate.Blocked {
			sawBlocked = true
			break
		}
	}
	if !sawBlocked {
		t.Fatalf("expected blocked candidate to remain visible as context, got %+v", work.Packet.Frontier.Candidates)
	}
	if _, err := store.RecordAgentTaskFrontierDecision(ctx, sqlite.AgentTaskFrontierDecisionInput{
		WorkspaceID:          workspaceID,
		AgentID:              agentID,
		FrontierGenerationID: work.Packet.Frontier.GenerationID,
		DecisionState:        "selected",
		SelectedTaskID:       blockedID,
		Summary:              "trying to select a blocked context candidate should fail",
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "was not in frontier generation") {
		t.Fatalf("expected blocked frontier candidate decision to be rejected by provenance, got %v", err)
	}
	err = store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:          workspaceID,
		TaskID:               blockedID,
		AgentID:              agentID,
		SelectedFromFrontier: true,
		FrontierGenerationID: work.Packet.Frontier.GenerationID,
		SelfFitSummary:       "trying to claim a blocked context candidate should fail",
		Summary:              "blocked frontier candidate",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "requires selected decision receipt") {
		t.Fatalf("expected blocked frontier candidate claim to require a selected decision receipt, got %v", err)
	}
}

func TestGetAgentWorkNextIncludesBlockedAndHandoffTransitionPackets(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	seedAgentWorkWorkspace(t, ctx, store, "ws-agent-transition", []string{"agent-a", "agent-b"})
	createAgentWorkTask(t, ctx, store, "ws-agent-transition", "task-blocked", "high")
	createAgentWorkTask(t, ctx, store, "ws-agent-transition", "task-handoff", "normal")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-transition",
		TaskID:      "task-blocked",
		AgentID:     "agent-a",
		Summary:     "waiting on credentials",
	}); err != nil {
		t.Fatalf("claim blocked task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-blocked",
		AgentID:     "agent-a",
		WorkspaceID: "ws-agent-transition",
		TaskID:      "task-blocked",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create blocked session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.blocked",
		WorkspaceID: "ws-agent-transition",
		SessionID:   "sess-blocked",
		AgentID:     "agent-a",
		TaskID:      "task-blocked",
		Summary:     "need credential refresh",
		Status:      "BLOCKED",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "credential", Detail: "reauth required"}},
	}); err != nil {
		t.Fatalf("record blocked session: %v", err)
	}

	blockedResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     "ws-agent-transition",
		AgentID:         "agent-a",
		IncludePacket:   true,
		Trigger:         "request_resume",
		CandidateTaskID: "task-blocked",
	})
	if err != nil {
		t.Fatalf("get blocked work next: %v", err)
	}
	if blockedResult.Packet == nil || blockedResult.Packet.Unblock == nil {
		t.Fatalf("expected unblock packet, got %+v", blockedResult.Packet)
	}
	if blockedResult.Packet.Unblock.UnblockState != "wake_selected" || len(blockedResult.Packet.Unblock.BlockerKinds) != 1 || blockedResult.Packet.Unblock.BlockerKinds[0] != "credential" {
		t.Fatalf("unexpected unblock packet: %+v", blockedResult.Packet.Unblock)
	}

	blockedSystemNews, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     "ws-agent-transition",
		AgentID:         "agent-a",
		IncludePacket:   true,
		Trigger:         "system_news",
		CandidateTaskID: "task-blocked",
	})
	if err != nil {
		t.Fatalf("get blocked work next with system news trigger: %v", err)
	}
	if blockedSystemNews.HasWork {
		t.Fatalf("generic system news must not resume blocked sessions, got %+v", blockedSystemNews)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-agent-transition",
		TaskID:      "task-handoff",
		AgentID:     "agent-a",
		Summary:     "handoff to specialist",
	}); err != nil {
		t.Fatalf("claim handoff task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-handoff",
		AgentID:     "agent-a",
		WorkspaceID: "ws-agent-transition",
		TaskID:      "task-handoff",
		StartedAt:   "2026-03-22T00:00:00Z",
	}); err != nil {
		t.Fatalf("create handoff session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.status",
		WorkspaceID: "ws-agent-transition",
		SessionID:   "sess-handoff",
		AgentID:     "agent-a",
		TaskID:      "task-handoff",
		Summary:     "waiting for specialist takeover",
		Status:      "HANDOFF_PENDING",
		HandoffTo:   "agent-b",
	}); err != nil {
		t.Fatalf("record handoff session: %v", err)
	}

	handoffResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     "ws-agent-transition",
		AgentID:         "agent-a",
		IncludePacket:   true,
		Trigger:         "runtime_resume",
		CandidateTaskID: "task-handoff",
	})
	if err != nil {
		t.Fatalf("get handoff work next: %v", err)
	}
	if handoffResult.Packet == nil || handoffResult.Packet.Handoff == nil {
		t.Fatalf("expected handoff packet, got %+v", handoffResult.Packet)
	}
	if handoffResult.Packet.Handoff.HandoffState != "wake_selected" || handoffResult.Packet.Handoff.ToAgentID != "agent-b" {
		t.Fatalf("unexpected handoff packet: %+v", handoffResult.Packet.Handoff)
	}
}

func TestGetAgentWorkNextPacketIncludesProjectCoordinationTasks(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-agent-project-coordination-packet"
	const projectID = "project-agent-work-packet"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Agent Work Packet Project",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-project-main", "Plan shared frontend", "planning", "high")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, "task-project-support", "Prepare generator notes", "research", "normal")

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-a",
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if !result.HasWork || result.Packet == nil {
		t.Fatalf("expected packetized project work, got %+v", result)
	}
	if len(result.ProjectCoordination) == 0 || len(result.Packet.ProjectCoordination) == 0 {
		t.Fatalf("expected project coordination on result and packet, top=%s packet=%s", result.ProjectCoordination, result.Packet.ProjectCoordination)
	}
	var coordination sqlite.ProjectCoordinationRecord
	if err := json.Unmarshal(result.Packet.ProjectCoordination, &coordination); err != nil {
		t.Fatalf("decode project coordination packet: %v", err)
	}
	if coordination.Project.ProjectID != projectID || len(coordination.Tasks) != 2 {
		t.Fatalf("expected project coordination tasks for %s, got %+v", projectID, coordination)
	}
	if !agentWorkCoordinationHasTask(coordination.Tasks, "task-project-main") || !agentWorkCoordinationHasTask(coordination.Tasks, "task-project-support") {
		t.Fatalf("expected both project tasks in packet coordination, got %+v", coordination.Tasks)
	}
}

func TestGetAgentWorkNextAllowsBranchOwnerToTakeRevisionFollowup(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-agent-branch-owner-followup"
		projectID     = "project-branch-owner-followup"
		leadID        = "alpha"
		branchOwnerID = "gamma"
		otherBuilder  = "delta"
		repoID        = "repo-main"
		taskID        = "task-revision-followup"
		reviewKey     = "project.project-branch-owner-followup.branch.branch-ready.review"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, otherBuilder})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	designDocID := "doc.branch-owner-followup.design"
	implementationPlanDocID := "doc.branch-owner-followup.plan"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 leadID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("open project gates: %v", err)
	}
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "Design accepted; implementation lane may start.",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase: %v", err)
	}
	for _, agentID := range []string{branchOwnerID, otherBuilder} {
		if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			AgentID:               agentID,
			RoleType:              sqlite.ProjectRoleImplementer,
			WriteScopeJSON:        `{"paths":["**"]}`,
			ActorID:               leadID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
			PromptContextSurface:  "project.role.assign",
		}); err != nil {
			t.Fatalf("assign implementer role to %s: %v", agentID, err)
		}
	}
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, `C:\fixtures\agents\gamma\branch-owner-followup`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nrevision follow-up evidence",
		UpdatedBy:   branchOwnerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	_, branchTaskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, branchOwnerID, "branch-ready", "agent/gamma/branch-owner-followup", `{"paths":["**"]}`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchOwnerID,
		BranchID:              "branch-ready",
		ActiveTaskID:          branchTaskID,
		ActiveClaimID:         branchTaskID,
		BranchName:            "agent/gamma/branch-owner-followup",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["**"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register ready branch: %v", err)
	}
	graph := dag.NormalizeGraph(dag.DefaultGraph())
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Add missing evidence to ready branch branch_id: branch-ready",
		Description:         "Exact follow-up for branch_id: branch-ready.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "integration",
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"**"},
	}, graph); err != nil {
		t.Fatalf("create revision follow-up task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach revision follow-up task: %v", err)
	}
	vagueTaskID := "task-ready-branch-vague-followup"
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              vagueTaskID,
		OwnerUserID:         "developer",
		Priority:            "normal",
		Title:               "Add missing evidence to ready branch",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "integration",
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"**"},
	}, graph); err != nil {
		t.Fatalf("create vague revision follow-up task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      vagueTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach vague revision follow-up task: %v", err)
	}

	vagueOwnerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     workspaceID,
		AgentID:         branchOwnerID,
		Trigger:         "runtime_switch_task",
		CandidateTaskID: vagueTaskID,
		IncludePacket:   true,
	})
	if err != nil {
		t.Fatalf("get vague branch owner work next: %v", err)
	}
	if vagueOwnerResult.HasWork || vagueOwnerResult.Task != nil || vagueOwnerResult.Reason != "project_claim_scope_busy" {
		t.Fatalf("vague ready-branch follow-up must be blocked by the live ready branch scope, got %+v", vagueOwnerResult)
	}
	if vagueOwnerResult.Packet == nil ||
		len(vagueOwnerResult.Packet.ContextHints.AnchorBranchIDs) != 1 ||
		vagueOwnerResult.Packet.ContextHints.AnchorBranchIDs[0] != "branch-ready" {
		t.Fatalf("expected vague busy packet to anchor ready branch, got %+v", vagueOwnerResult.Packet)
	}

	otherResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       otherBuilder,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get other builder work next: %v", err)
	}
	if otherResult.HasWork || otherResult.Reason != "project_claim_scope_busy" {
		t.Fatalf("expected non-owner builder to be blocked by live branch scope, got %+v", otherResult)
	}
	if otherResult.Task != nil {
		t.Fatalf("busy no-work packet must not carry a runnable task, got %+v", otherResult.Task)
	}
	if otherResult.Packet == nil ||
		otherResult.Packet.PreferredTransition != "delegate_to_branch_owner" ||
		otherResult.Packet.Gate == nil ||
		otherResult.Packet.Gate.NeededFrom != branchOwnerID ||
		otherResult.Packet.HandoffToAgentID != branchOwnerID ||
		otherResult.Packet.Handoff == nil ||
		otherResult.Packet.Handoff.ToAgentID != branchOwnerID {
		t.Fatalf("expected busy packet to hand off to branch owner, got %+v", otherResult.Packet)
	}
	if len(otherResult.Packet.ContextHints.AnchorTaskIDs) != 1 || otherResult.Packet.ContextHints.AnchorTaskIDs[0] != taskID {
		t.Fatalf("expected busy packet to anchor blocked task, got %+v", otherResult.Packet.ContextHints)
	}
	if len(otherResult.Packet.ContextHints.AnchorBranchIDs) != 1 || otherResult.Packet.ContextHints.AnchorBranchIDs[0] != "branch-ready" {
		t.Fatalf("expected busy packet to anchor conflict branch, got %+v", otherResult.Packet.ContextHints)
	}

	otherTrustFirstResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          otherBuilder,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get trust-first other builder work next: %v", err)
	}
	if otherTrustFirstResult.HasWork || otherTrustFirstResult.Reason != "project_claim_scope_busy" {
		t.Fatalf("expected trust-first non-owner builder to remain blocked by live branch scope, got %+v", otherTrustFirstResult)
	}
	if otherTrustFirstResult.Task != nil {
		t.Fatalf("trust-first busy no-work packet must not carry a runnable task, got %+v", otherTrustFirstResult.Task)
	}

	ownerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     branchOwnerID,
	})
	if err != nil {
		t.Fatalf("get branch owner work next: %v", err)
	}
	if ownerResult.HasWork || ownerResult.Task != nil || ownerResult.Reason != "project_patch_queue_submit_handoff_available" {
		t.Fatalf("ready branch owner should use durable patch queue submit handoff instead of prose-linked follow-up, got %+v", ownerResult)
	}
}

func TestBlockedPatchQueueBranchDoesNotHoldFreshImplementationScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-blocked-patchq-scope-release"
		projectID     = "project-blocked-patchq-scope-release"
		leadID        = "alpha"
		branchOwnerID = "delta"
		builderID     = "beta"
		reviewerID    = "theta"
		repoID        = "repo-main"
		taskID        = "task-fresh-ui-after-blocked-patchq"
		reviewKey     = "project.project-blocked-patchq-scope-release.branch.branch-blocked.review"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "Design accepted; implementation lane may start.",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase: %v", err)
	}
	builderRole, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               builderID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/ui/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign builder role: %v", err)
	}
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
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, `C:\fixtures\agents\delta\blocked-patchq`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Blocked Branch Review Packet",
		Content:     "# Branch Review Packet\n\nBlocked because the candidate is missing visual evidence.",
		UpdatedBy:   branchOwnerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	_, branchTaskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, branchOwnerID, "branch-blocked", "agent/delta/blocked-patchq", `{"paths":["src/**","tests/**"]}`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchOwnerID,
		BranchID:              "branch-blocked",
		ActiveTaskID:          branchTaskID,
		ActiveClaimID:         branchTaskID,
		BranchName:            "agent/delta/blocked-patchq",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["src/**","tests/**"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
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
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", branchOwnerID),
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
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Missing browser and visual acceptance evidence; release write-scope mutex so a UI follow-up can be built.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block patch queue item: %v", err)
	}
	releaseProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, branch.BranchID, branchTaskID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Implement UI follow-up after blocked patch queue candidate",
		Description:          "Build the missing UI surface and visual evidence after the previous broad branch was blocked.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: `{"required_work_modes":["implementation"],"preferred_skills":["frontend","ui"],"preferred_tools":["shell"]}`,
		WriteScopeHints:      []string{"src/ui/**"},
	}, graph); err != nil {
		t.Fatalf("create fresh UI task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach fresh UI task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get builder work after blocked patch queue: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != taskID || result.Reason == "project_claim_scope_busy" {
		t.Fatalf("blocked patch queue branch should not hold fresh implementation scope, got %+v", result)
	}
	freshCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, builderID, `C:\fixtures\agents\beta\fresh-ui-after-blocked-patch-queue`)
	freshBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            freshCheckout.CheckoutID,
		AgentID:               builderID,
		BranchName:            "agent/beta/fresh-ui-after-blocked-patch-queue",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["src/ui/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               builderID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", builderID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register fresh UI branch: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          builderID,
		ProjectRoleID:    builderRole.RoleID,
		RepoID:           repoID,
		CheckoutID:       freshCheckout.CheckoutID,
		BranchID:         freshBranch.BranchID,
		WriteScopeJSON:   `{"paths":["src/ui/**"]}`,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Summary:          "claim UI follow-up after blocked patch queue scope release",
	}); err != nil {
		t.Fatalf("claim fresh UI task after blocked patch queue: %v", err)
	}
}

func TestOwnerBoundPatchQueueSubmitRoutesOnlyBranchOwner(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-owner-bound-submit"
		projectID     = "project-owner-bound-submit"
		leadID        = "alpha"
		branchOwnerID = "gamma"
		otherAgentID  = "iota"
		repoID        = "repo-main"
		taskID        = "task-owner-bound-patch-queue-submit"
		branchID      = "branch-owner-bound-submit"
		reviewKey     = "project.owner-bound-submit.branch.review"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, otherAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, `C:\fixtures\agents\gamma\owner-bound-submit`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Review packet",
		Content:     "# Review\n\nReady.",
		UpdatedBy:   branchOwnerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	_, branchTaskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, branchOwnerID, branchID, "agent/gamma/owner-bound-submit", `{"paths":["src/**"]}`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchOwnerID,
		BranchID:              branchID,
		ActiveTaskID:          branchTaskID,
		ActiveClaimID:         branchTaskID,
		BranchName:            "agent/gamma/owner-bound-submit",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register ready branch: %v", err)
	}
	createOwnerBoundPatchQueueSubmitTask(t, ctx, store, workspaceID, projectID, taskID, branchID, branchOwnerID)

	otherResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          otherAgentID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get non-owner work next: %v", err)
	}
	if otherResult.HasWork || otherResult.Reason != "project_owner_bound_agent_required" {
		t.Fatalf("expected non-owner owner-bound handoff, got %+v", otherResult)
	}
	if otherResult.Packet == nil || otherResult.Packet.OwnerBound == nil || otherResult.Packet.OwnerBound.RequiredAgentID != branchOwnerID || otherResult.Packet.HandoffToAgentID != branchOwnerID {
		t.Fatalf("expected structured owner-bound packet to gamma, got %+v", otherResult.Packet)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          otherAgentID,
		CoordinationMode: "trust_first",
		Summary:          "wrong owner should fail",
	}); err == nil || !strings.Contains(err.Error(), "owner-bound task") {
		t.Fatalf("expected non-owner trust-first claim rejection, got %v", err)
	}

	ownerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get owner work next: %v", err)
	}
	if !ownerResult.HasWork || ownerResult.Task == nil || ownerResult.Task.TaskID != taskID {
		t.Fatalf("expected branch owner to receive owner-bound submit task, got %+v", ownerResult)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
		Summary:          "owner may claim submit task",
	}); err != nil {
		t.Fatalf("expected branch owner trust-first claim to succeed: %v", err)
	}
}

func TestOwnerBoundPatchQueueRevisionRoutesOnlyBranchOwner(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-owner-bound-revision"
		projectID     = "project-owner-bound-revision"
		leadID        = "alpha"
		branchOwnerID = "gamma"
		otherAgentID  = "iota"
		reviewerID    = "theta"
		repoID        = "repo-main"
		taskID        = "task-owner-bound-patch-queue-revision"
		branchID      = "branch-owner-bound-revision"
		queueID       = "patchq-owner-bound-revision"
		itemID        = "patchitem-owner-bound-revision"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, otherAgentID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "owner-bound revision task should route to the existing branch owner",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase: %v", err)
	}
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	ownerBranch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, branchID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		QueueID:                  queueID,
		ItemID:                   itemID,
		RepoID:                   repoID,
		BranchID:                 ownerBranch.BranchID,
		ReviewDocKey:             ownerBranch.ReviewDocKey,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		PathsetJSON:              ownerBranch.WriteScopeJSON,
		BaseSHA:                  ownerBranch.BaseSHA,
		HeadSHA:                  ownerBranch.HeadSHA,
		TaskID:                   ownerBranch.ActiveTaskID,
		SessionID:                "session-" + branchID,
		RunID:                    "run-" + branchID,
		AgentID:                  branchOwnerID,
		CapabilitySnapshotID:     "cap-" + branchID,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 `C:\fixtures\agents\` + branchOwnerID + `\` + branchID,
		BaseTreeHash:             ownerBranch.BaseSHA,
		BaseFileHashes:           agentWorkTestBaseFileHashesForScope(ownerBranch.WriteScopeJSON),
		RepoLeaseID:              "lease-" + branchID,
		LeaseTerm:                7,
		ActorID:                  branchOwnerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit owner-bound revision source item: %v", err)
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
		t.Fatalf("claim owner-bound revision source item: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Patch queue decision BLOCKED for " + branchID + ": source drift requires revision.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block owner-bound revision source item: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        branchOwnerID,
		Specialization: "UI/UX artifact reviewer; strong in visual QA, browser checks, and interaction critique",
		Tags:           []string{"reviewer", "visual QA", "browser"},
		Metadata: map[string]any{
			"default_work_mode": "review",
			"reflection_scope":  "artifact",
		},
	}); err != nil {
		t.Fatalf("upsert review-mode branch owner profile: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "revise", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	requiresProjectGate := true
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "critical",
		Title:               "Unblock integration candidate " + branchID,
		Description:         "Patch queue decision follow-up.\n\n- queue_id: " + queueID + "\n- item_id: " + itemID + "\n- branch_id: " + branchID + "\n- head_sha: " + ownerBranch.HeadSHA + "\n- state: BLOCKED",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "patch-queue", "revision", "blocked", "owner-bound", "owner-bound-kind:patch_queue_revision", "owner-branch:" + branchID, "required-agent:" + branchOwnerID},
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: requiresProjectGate,
		WriteScopeHints:     []string{"src/**"},
	}, graph); err != nil {
		t.Fatalf("create owner-bound revision task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach owner-bound revision task: %v", err)
	}

	otherResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          otherAgentID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get non-owner work next: %v", err)
	}
	if otherResult.HasWork || otherResult.Reason != "project_owner_bound_agent_required" {
		t.Fatalf("expected non-owner owner-bound revision handoff, got %+v", otherResult)
	}
	if otherResult.Packet == nil || otherResult.Packet.OwnerBound == nil || otherResult.Packet.OwnerBound.RequiredAgentID != branchOwnerID || otherResult.Packet.HandoffToAgentID != branchOwnerID {
		t.Fatalf("expected structured owner-bound revision packet to gamma, got %+v", otherResult.Packet)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          otherAgentID,
		CoordinationMode: "trust_first",
		Summary:          "wrong owner should fail",
	}); err == nil || !strings.Contains(err.Error(), "owner-bound task") {
		t.Fatalf("expected non-owner trust-first revision claim rejection, got %v", err)
	}

	ownerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get owner work next: %v", err)
	}
	if !ownerResult.HasWork || ownerResult.Task == nil || ownerResult.Task.TaskID != taskID {
		t.Fatalf("expected branch owner to receive owner-bound revision task, got %+v", ownerResult)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          branchOwnerID,
		RepoID:           repoID,
		CheckoutID:       ownerBranch.CheckoutID,
		BranchID:         ownerBranch.BranchID,
		WriteScopeJSON:   ownerBranch.WriteScopeJSON,
		CoordinationMode: "trust_first",
		Summary:          "owner may claim revision task",
	}); err != nil {
		t.Fatalf("expected branch owner trust-first revision claim to succeed: %v", err)
	}
}

func TestPatchQueueDecisionContinuationRoutesToRequiredOwnerDespiteStaleOwnerUserID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-patchq-continuation-stale-owner"
		projectID     = "project-patchq-continuation-stale-owner"
		leadID        = "alpha"
		branchOwnerID = "gamma"
		staleOwnerID  = "iota"
		reviewerID    = "theta"
		repoID        = "repo-main"
		branchID      = "branch-patchq-continuation-stale-owner"
		queueID       = "patchq-continuation-stale-owner"
		itemID        = "patchitem-continuation-stale-owner"
		distractor    = "task-plain-implementation-distractor"
		headSHA       = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, staleOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["src/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	ownerBranch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, branchID)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        branchOwnerID,
		Specialization: "implementation owner",
		Metadata: map[string]any{
			"default_work_mode": "implementation",
		},
	}); err != nil {
		t.Fatalf("upsert branch owner profile: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              distractor,
		OwnerUserID:         branchOwnerID,
		Priority:            "critical",
		Title:               "Plain implementation distractor",
		Description:         "Ordinary implementation work that must not outrank a patch queue decision continuation.",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"src/**"},
	}, graph); err != nil {
		t.Fatalf("create distractor task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: distractor, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach distractor task: %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		QueueID:                  queueID,
		ItemID:                   itemID,
		RepoID:                   repoID,
		BranchID:                 ownerBranch.BranchID,
		ReviewDocKey:             ownerBranch.ReviewDocKey,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		PathsetJSON:              ownerBranch.WriteScopeJSON,
		BaseSHA:                  ownerBranch.BaseSHA,
		HeadSHA:                  headSHA,
		TaskID:                   ownerBranch.ActiveTaskID,
		SessionID:                "session-" + branchID,
		RunID:                    "run-" + branchID,
		AgentID:                  branchOwnerID,
		CapabilitySnapshotID:     "cap-" + branchID,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 `C:\fixtures\agents\` + branchOwnerID + `\` + branchID,
		BaseTreeHash:             ownerBranch.BaseSHA,
		BaseFileHashes:           agentWorkTestBaseFileHashesForScope(ownerBranch.WriteScopeJSON),
		RepoLeaseID:              "lease-" + branchID,
		LeaseTerm:                7,
		ActorID:                  branchOwnerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit stale-owner patch queue item: %v", err)
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
		t.Fatalf("claim stale-owner patch queue item: %v", err)
	}
	decided, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Patch queue decision BLOCKED for " + branchID + ": source drift requires revision.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("block stale-owner patch queue item: %v", err)
	}
	continuation := sqlite.ProjectPatchQueueDecisionContinuationTaskID(projectID, decided, "revision")
	if _, err := store.DB().ExecContext(ctx, `
UPDATE tasks
   SET owner_user_id = ?,
       tags_json = ?
 WHERE task_id = ?`,
		staleOwnerID,
		`["project","patch_queue","revision","decision_continuation","owner-bound","owner-bound-kind:patch_queue_revision","owner-branch:`+branchID+`","owner-agent:`+branchOwnerID+`","required-agent:`+branchOwnerID+`"]`,
		continuation); err != nil {
		t.Fatalf("stale continuation owner_user_id: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get branch owner work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != continuation {
		t.Fatalf("expected stale owner_user_id decision continuation to preempt distractor, got %+v", result)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           continuation,
		AgentID:          branchOwnerID,
		RepoID:           repoID,
		CheckoutID:       ownerBranch.CheckoutID,
		BranchID:         ownerBranch.BranchID,
		WriteScopeJSON:   ownerBranch.WriteScopeJSON,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		Summary:          "branch owner claims stale owner continuation",
	}); err != nil {
		t.Fatalf("expected branch owner claim to succeed despite stale owner_user_id: %v", err)
	}
}

func TestGetAgentWorkNextPrefersBlockedPatchQueueRevisionOverStaleSidecars(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID       = "ws-patchq-revision-preempts-sidecars"
		projectID         = "project-patchq-revision-preempts-sidecars"
		leadID            = "alpha"
		branchOwnerID     = "beta"
		reviewerID        = "theta"
		otherAgentID      = "iota"
		integratorID      = "eta"
		repoID            = "repo-main"
		staleProvenanceID = "task-clearpress-beta-provenance"
		staleClassifierID = "task-side-effect-classify-beta-stale"
		staleBacklogID    = "task-agent-backlog-epsilon-patchq"
		sessionID         = "session-beta-stale-provenance"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, reviewerID, otherAgentID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["src/**","public/**","package*.json"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, staleProvenanceID, "Publish beta Clearpress lane candidate or blocker provenance", "Publish current beta branch provenance for the Clearpress patch queue.", "coordination", []string{"publication", "branch-provenance", "clearpress", "beta"}, false)
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                staleProvenanceID,
		AgentID:               branchOwnerID,
		Summary:               "claim stale provenance sidecar",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, staleProvenanceID, branchOwnerID),
	}); err != nil {
		t.Fatalf("claim stale provenance: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     branchOwnerID,
		WorkspaceID: workspaceID,
		TaskID:      staleProvenanceID,
		StartedAt:   "2026-05-23T20:00:00Z",
	}); err != nil {
		t.Fatalf("create stale provenance session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.start",
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     branchOwnerID,
		TaskID:      staleProvenanceID,
		Summary:     "stale provenance sidecar active",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("record stale provenance session: %v", err)
	}

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-beta-ready", branchOwnerID, reviewerID, `{"paths":["src/**","public/**","package.json"]}`, sqlite.ProjectPatchQueueStateBlocked)

	// The BLOCKED decision eagerly materializes the queue-facing revision continuation owned by the branch owner
	// (beta holds a claimable IMPLEMENTER role): it IS the system's own revision follow-up, so no manual seed is
	// needed. Work-next must prefer this deterministic continuation task over the stale sidecars below.
	revisionTaskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(projectID, item, "revision")
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "revise", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate revision graph: %v", err)
	}

	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               staleClassifierID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Classify side effects for agent-beta-p-clearpress",
		Description:          "Old side-effect classifier for the same beta branch should not outrank the queue-facing revision.",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		Tags:                 []string{"side-effect-classification", "operational-boundary", "abpc", "project-coordination"},
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		TaskRequirementsJSON: fmt.Sprintf(`{"schema":"task_requirements.v1","abpc_task_class":"side_effect_classification","branch_id":%q,"branch_name":"agent/beta/branch-beta-ready"}`, item.BranchID),
		RequiresProjectGate:  false,
	}, graph); err != nil {
		t.Fatalf("create stale side-effect classifier: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      staleClassifierID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach stale side-effect classifier: %v", err)
	}
	staleBacklogDescription := fmt.Sprintf(`Autonomous initiative promoted from agent personal backlog.

- agent_id: epsilon
- backlog_item_id: ais-stale-patchq
- dedup_key: patch-queue-vigilance:patch_queue_convergence_gap:%s
- source_heartbeat: patch_queue_vigilance
- reason: patch queue item is blocked or stale-claimed without visible stewardship

## Finding
Patch queue item %s/%s is BLOCKED and has no visible active follow-up; create one bounded stewardship task instead of mutating the queue directly from heartbeat.

## Evidence
- patch_queue:%s
- patch_item:%s
- project:%s
- branch:%s
- head:%s
- state:BLOCKED
- missing:queue_stewardship`, item.ItemID, item.QueueID, item.ItemID, item.QueueID, item.ItemID, projectID, item.BranchID, item.HeadSHA[:12])
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               staleBacklogID,
		OwnerUserID:          "developer",
		Priority:             "critical",
		Title:                "Address: Patch queue candidate needs queue stewardship",
		Description:          staleBacklogDescription,
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		Tags:                 []string{"internal-heartbeat", "patch_queue_vigilance", "patch_queue_convergence_gap", "agent-backlog", "epsilon"},
		ProjectID:            projectID,
		ProjectLane:          "integration",
		TaskRequirementsJSON: fmt.Sprintf(`{"schema":"task_requirements.v1","write_scope_hints":["patch_queue:%s","patch_item:%s","branch:%s","head:%s"]}`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA[:12]),
		RequiresProjectGate:  false,
	}, graph); err != nil {
		t.Fatalf("create stale patch queue backlog sidecar: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      staleBacklogID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach stale patch queue backlog sidecar: %v", err)
	}

	ownerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get branch owner work next: %v", err)
	}
	if !ownerResult.HasWork || ownerResult.Task == nil || ownerResult.Task.TaskID != revisionTaskID {
		t.Fatalf("expected branch owner to receive queue-facing revision follow-up over stale sidecars, got %+v", ownerResult)
	}

	otherResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          otherAgentID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get non-owner work next: %v", err)
	}
	if otherResult.HasWork && otherResult.Task != nil && otherResult.Task.TaskID == staleClassifierID {
		t.Fatalf("expected stale classifier to be suppressed while branch revision follow-up is live, got %+v", otherResult)
	}
	reviewerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reviewer work next: %v", err)
	}
	if reviewerResult.HasWork && reviewerResult.Task != nil && reviewerResult.Task.TaskID == staleProvenanceID {
		t.Fatalf("expected stale provenance sidecar to be suppressed for reviewers while branch revision follow-up is live, got %+v", reviewerResult)
	}
	integratorResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          integratorID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get integrator work next: %v", err)
	}
	if integratorResult.HasWork && integratorResult.Task != nil && integratorResult.Task.TaskID == staleBacklogID {
		t.Fatalf("expected stale heartbeat patch queue backlog sidecar to be suppressed behind queue-facing revision follow-up, got %+v", integratorResult)
	}
}

func TestGetAgentWorkNextPrefersProductExecutionUnderPatchQueuePressureOverFreshCoordination(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-r50-product-pressure"
		projectID      = "project-r50-product-pressure"
		leadID         = "alpha"
		builderID      = "beta"
		branchOwnerID  = "theta"
		reviewerID     = "eta"
		repoID         = "repo-main"
		executionTask  = "task-rq-eval-import-repair"
		coordinationID = "task-rq-eval-cli-split"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, builderID, leadID, `{"paths":["internal/eval/**","pkg/**"]}`)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["cmd/rq/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-rq-cli-ready", branchOwnerID, reviewerID, `{"paths":["cmd/rq/**"]}`, sqlite.ProjectPatchQueueStateBlocked)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               executionTask,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Repair rq internal/eval import resolution for cli and repl",
		Description:          "Concrete rq product repair: make internal/eval imports resolve from CLI and REPL paths before more split coordination.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		Tags:                 []string{"rq", "implementation", "eval", "cli", "repl"},
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: `{"required_work_modes":["implementation"],"preferred_skills":["go","cli"],"preferred_tools":["shell"],"preserve_write_scope_hints":true}`,
		WriteScopeHints:      []string{"internal/eval/**", "pkg/**"},
	}, graph); err != nil {
		t.Fatalf("create execution repair task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: executionTask, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach execution repair task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              coordinationID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Split rq parser and CLI/repl ownership",
		Description:         "Fresh coordination split that should not outrank a claimable product repair while the patch queue is blocked.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		Tags:                []string{"rq", "coordination", "role-scope"},
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create fresh coordination split: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: coordinationID, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach fresh coordination split: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET updated_at = ? WHERE task_id = ?`, "2026-06-05T21:51:34Z", executionTask); err != nil {
		t.Fatalf("backdate execution task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET updated_at = ? WHERE task_id = ?`, "2026-06-05T23:49:56Z", coordinationID); err != nil {
		t.Fatalf("freshen coordination task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get builder work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != executionTask {
		t.Fatalf("expected product execution to preempt fresh coordination under patch queue pressure, got %+v", result)
	}
	if result.Reason != "product_lane_pressure" {
		t.Fatalf("expected product_lane_pressure reason, got %q", result.Reason)
	}
}

func TestProductLanePressureReadyReviewProposedCandidatePreemptsCoordination(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-r39-ready-review-pressure"
		projectID      = "project-r39-ready-review-pressure"
		leadID         = "alpha"
		builderID      = "delta"
		branchOwnerID  = "beta"
		reviewerID     = "eta"
		repoID         = "repo-lua"
		branchID       = "branch-lua-parser-ready"
		executionTask  = "task-lua-eval-after-parser"
		coordinationID = "task-lua-role-scope-repeat"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, builderID, leadID, `{"paths":["internal/eval/**","internal/runtime/**"]}`)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["internal/parser/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, branchID)
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branchID,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); err != nil {
		t.Fatalf("submit ready parser patch queue item: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        builderID,
		Specialization: "stale coordination profile",
		Tags:           []string{"strategy", "coordination"},
		Metadata: map[string]any{
			"default_work_mode": "strategy",
		},
	}); err != nil {
		t.Fatalf("upsert stale builder profile: %v", err)
	}
	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, executionTask,
		"Build eval now that parser branch is ready for review",
		"Concrete Lua eval implementation should pull ahead of coordination while a ready parser candidate awaits review.",
		"implementation", []string{"signal01", "implementation", "eval"}, true,
	)
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET task_requirements_json = ?, write_scope_hints_json = ?, updated_at = ? WHERE task_id = ?`,
		`{"required_work_modes":["implementation"],"preferred_skills":["go","lua"],"preferred_tools":["shell"]}`,
		`["internal/eval/**","internal/runtime/**"]`,
		"2026-06-21T01:00:00Z",
		executionTask,
	); err != nil {
		t.Fatalf("set execution task requirements: %v", err)
	}
	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, coordinationID,
		"Re-split Lua parser/eval ownership",
		"Fresh coordination work must not outrank ready product review pressure.",
		"coordination", []string{"signal01", "coordination", "role-scope"}, true,
	)
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET task_kind = ?, updated_at = ? WHERE task_id = ?`, model.TaskKindCoordination, "2026-06-21T01:10:00Z", coordinationID); err != nil {
		t.Fatalf("freshen coordination task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get builder work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != executionTask {
		t.Fatalf("ready PROPOSED review candidate should pull product execution ahead of coordination, got %+v", result)
	}
	if result.Reason != "product_lane_pressure" {
		t.Fatalf("expected product_lane_pressure reason, got %q", result.Reason)
	}
}

func TestProductLanePressureActiveProjectRoleBypassesStaleFreshProfile(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-r58-product-pressure-role-profile-bypass"
		projectID      = "project-r58-product-pressure-role-profile-bypass"
		leadID         = "alpha"
		builderID      = "delta"
		branchOwnerID  = "theta"
		reviewerID     = "eta"
		repoID         = "repo-main"
		executionTask  = "task-rq-eval-import-repair-role-bypass"
		coordinationID = "task-rq-eval-role-scope-repeat"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, builderID, leadID, `{"paths":["internal/eval/**","pkg/**"]}`)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["cmd/rq/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-rq-cli-ready", branchOwnerID, reviewerID, `{"paths":["cmd/rq/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        builderID,
		Specialization: "stale project strategy profile",
		Tags:           []string{"strategy", "coordination"},
		Metadata: map[string]any{
			"default_work_mode": "strategy",
		},
	}); err != nil {
		t.Fatalf("upsert stale builder profile: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               executionTask,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Repair rq internal/eval import resolution for cli and repl",
		Description:          "Concrete rq product repair that must stay claimable by the assigned implementer even if the fresh profile is stale.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		Tags:                 []string{"rq", "implementation", "eval", "cli", "repl"},
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: `{"required_work_modes":["implementation"],"preferred_skills":["go","cli"],"preferred_tools":["shell"],"preserve_write_scope_hints":true}`,
		WriteScopeHints:      []string{"internal/eval/**", "pkg/**"},
	}, graph); err != nil {
		t.Fatalf("create execution repair task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: executionTask, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach execution repair task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              coordinationID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Repeat rq role-scope split",
		Description:         "Fresh coordination escape that must not outrank an assigned product execution claimant.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		Tags:                []string{"rq", "coordination", "role-scope"},
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create fresh coordination task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: coordinationID, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach coordination task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET updated_at = ? WHERE task_id = ?`, "2026-06-06T06:40:00Z", executionTask); err != nil {
		t.Fatalf("date execution task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET updated_at = ? WHERE task_id = ?`, "2026-06-06T06:55:00Z", coordinationID); err != nil {
		t.Fatalf("freshen coordination task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get assigned stale-profile builder work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != executionTask {
		t.Fatalf("assigned implementer must claim product execution under pressure despite stale fresh profile, got %+v", result)
	}
	if result.Reason != "product_lane_pressure" {
		t.Fatalf("expected product_lane_pressure reason, got %q", result.Reason)
	}
	if result.ProfileGateReason == "profile_task_mode_mismatch" {
		t.Fatalf("active project role must bypass stale profile mismatch, got %+v", result)
	}
}

func TestProductLanePressureCriticalLuaGapTaskUsesBoundaryRoleScope(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		roleSummary string
		wantTaskID  string
	}{
		{
			name:        "ordinary-role-keeps-umbrella-targeted",
			roleSummary: "ordinary runtime implementer for the eval slice",
			wantTaskID:  "task-lua-runtime-tables",
		},
		{
			name:        "boundary-role-selects-critical-gap",
			roleSummary: "Boundary transition for the Lua runtime slice; keep eval/runtime/value/runner as the narrowed implementation claim while the umbrella conformance gap remains visible.",
			wantTaskID:  "task-lua-step3-conformance-gap",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			workspaceID := "ws-r61-lua-critical-gap-" + tc.name
			projectID := "project-r61-lua-critical-gap-" + tc.name
			const (
				leadID        = "alpha"
				builderID     = "delta"
				branchOwnerID = "theta"
				reviewerID    = "eta"
				repoID        = "repo-lua"
				runtimeTaskID = "task-lua-runtime-tables"
				gapTaskID     = "task-lua-step3-conformance-gap"
			)
			seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, branchOwnerID, reviewerID})
			createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
			claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
			if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
				WorkspaceID:           workspaceID,
				ProjectID:             projectID,
				AgentID:               builderID,
				RoleType:              sqlite.ProjectRoleImplementer,
				WriteScopeJSON:        `{"paths":["go.mod","go.sum","internal/eval/**","internal/runtime/**","internal/value/**","internal/runner/**"]}`,
				Summary:               tc.roleSummary,
				ActorID:               leadID,
				ActorType:             "agent",
				PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
				PromptContextSurface:  "project.role.assign",
			}); err != nil {
				t.Fatalf("assign builder role: %v", err)
			}
			assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["cmd/glua/**"]}`)
			assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
			upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
			createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-lua-cli-blocked", branchOwnerID, reviewerID, `{"paths":["cmd/glua/**"]}`, sqlite.ProjectPatchQueueStateBlocked)

			graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
			if err := dag.ValidateGraph(graph); err != nil {
				t.Fatalf("validate graph: %v", err)
			}
			if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
				WorkspaceID:          workspaceID,
				TaskID:               runtimeTaskID,
				OwnerUserID:          "operator",
				Priority:             "high",
				Title:                "Implement runtime values, tables, closures, and metatables",
				Description:          "Concrete Lua runtime product task for values, evaluator, environments, closures, and tables.",
				TaskKind:             model.TaskKindExecution,
				TaskTemplate:         "generic",
				Tags:                 []string{"signal01", "implementation", "runtime"},
				ProjectID:            projectID,
				ProjectLane:          "implementation",
				RequiresProjectGate:  true,
				TaskRequirementsJSON: `{"required_work_modes":["implementation"],"preferred_skills":["go","lua"],"preferred_tools":["shell"],"preserve_write_scope_hints":true}`,
				WriteScopeHints:      []string{"internal/eval/**", "internal/runtime/**", "internal/value/**"},
			}, graph); err != nil {
				t.Fatalf("create runtime task: %v", err)
			}
			if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: runtimeTaskID, LinkedBy: leadID}); err != nil {
				t.Fatalf("attach runtime task: %v", err)
			}
			if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
				WorkspaceID:          workspaceID,
				TaskID:               gapTaskID,
				OwnerUserID:          leadID,
				Priority:             "critical",
				Title:                "Implement roster-built Lua eval/runtime for 0-of-34 conformance gap",
				Description:          "Close the per-feature luaperfeature 0/34 conformance gap through roster-built product code. No vendoring, shell-out, or stub-only claim is acceptable.",
				TaskKind:             model.TaskKindExecution,
				TaskTemplate:         "generic",
				Tags:                 []string{"signal01", "implementation", "conformance-gap"},
				ProjectID:            projectID,
				ProjectLane:          "implementation",
				RequiresProjectGate:  true,
				TaskRequirementsJSON: `{"schema":"signal01_lua_step3_conformance_gap_task.v1","acceptance_criteria_refs":["AC-LUA-PARSE-01","AC-LUA-SEM-01","AC-LUA-FUNC-01","AC-LUA-TABLE-01","AC-LUA-STDLIB-01","AC-LUA-ERR-01","AC-LUA-CLI-01"],"baseline_per_feature":{"harness":"runs/signal01-lua-capability/tools/luaperfeature.go","pass":0,"total":34},"forbidden_substitutes":["github.com/yuin/gopher-lua","third_party_interpreter_dependency","shell_out_to_lua","stub_only_claim"],"target_packages":["internal/eval","internal/value","internal/runtime","internal/stdlib","internal/parser","internal/ast","internal/runner","cmd/glua"],"success_signal":"At least one luaperfeature probe passes through roster-built product code."}`,
				WriteScopeHints: []string{
					"internal/parser/**",
					"internal/ast/**",
					"internal/eval/**",
					"internal/evaluator/**",
					"internal/runtime/**",
					"internal/value/**",
					"internal/functions/**",
					"internal/table/**",
					"internal/metatable/**",
					"internal/stdlib/**",
					"internal/builtins/**",
					"internal/builtin/**",
					"internal/errors/**",
					"internal/diagnostics/**",
					"internal/runner/**",
					"cmd/glua/**",
					"internal/cli/**",
					"internal/repl/**",
					"scripts/**",
					"testdata/smoke/**",
					"tools/oracle/**",
					"README.md",
				},
			}, graph); err != nil {
				t.Fatalf("create conformance gap task: %v", err)
			}
			if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: gapTaskID, LinkedBy: leadID}); err != nil {
				t.Fatalf("attach conformance gap task: %v", err)
			}

			result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
				WorkspaceID:      workspaceID,
				AgentID:          builderID,
				IncludePacket:    true,
				CoordinationMode: sqlite.CoordinationModeTrustFirst,
			})
			if err != nil {
				t.Fatalf("get builder work next: %v", err)
			}
			if !result.HasWork || result.Task == nil || result.Task.TaskID != tc.wantTaskID {
				t.Fatalf("expected %s to be selected under product pressure, got %+v", tc.wantTaskID, result)
			}
			if result.Reason != "product_lane_pressure" {
				t.Fatalf("expected product_lane_pressure reason, got %q", result.Reason)
			}
		})
	}
}

func requireBlockedProductPressureFrontier(t *testing.T, result sqlite.AgentWorkNextResult, productTaskID, coordinationTaskID string) {
	t.Helper()
	if !result.HasWork || result.Task != nil || result.Reason != "task_frontier_available" || result.Packet == nil || result.Packet.Frontier == nil {
		t.Fatalf("expected task_frontier_available packet with blocked product candidate, got %+v", result)
	}
	productFound := false
	coordinationFound := coordinationTaskID == ""
	for _, candidate := range result.Packet.Frontier.Candidates {
		taskID := strings.TrimSpace(candidate.Task.TaskID)
		if taskID == strings.TrimSpace(productTaskID) {
			productFound = true
			if !candidate.Blocked || strings.TrimSpace(candidate.BlockReason) == "" {
				t.Fatalf("expected pressured product candidate %s to remain visible but blocked, got %+v", productTaskID, candidate)
			}
		}
		if coordinationTaskID != "" && taskID == strings.TrimSpace(coordinationTaskID) {
			coordinationFound = true
			if !candidate.Blocked || !strings.EqualFold(strings.TrimSpace(candidate.BlockReason), "product_lane_pressure") {
				t.Fatalf("coordination candidate %s must be blocked by product_lane_pressure, got %+v", coordinationTaskID, candidate)
			}
		}
	}
	if !productFound {
		t.Fatalf("expected pressured product candidate %s in frontier, got %+v", productTaskID, result.Packet.Frontier.Candidates)
	}
	if !coordinationFound {
		t.Fatalf("expected coordination candidate %s in frontier as blocked evidence, got %+v", coordinationTaskID, result.Packet.Frontier.Candidates)
	}
}

func TestGetAgentWorkNextSuppressesCoordinationFrontierUnderProductLanePressure(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-r51-product-pressure-frontier"
		projectID      = "project-r51-product-pressure-frontier"
		leadID         = "alpha"
		coordinatorID  = "iota"
		builderID      = "beta"
		branchOwnerID  = "theta"
		reviewerID     = "eta"
		repoID         = "repo-main"
		executionTask  = "task-rq-product-impl-pending"
		coordinationID = "task-rq-fresh-coordination-churn"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, coordinatorID, builderID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, builderID, leadID, `{"paths":["internal/eval/**","cmd/rq/**"]}`)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["cmd/rq/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-rq-cli-blocked", branchOwnerID, reviewerID, `{"paths":["cmd/rq/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        coordinatorID,
		Specialization: "rq project strategy and coordination",
		Tags:           []string{"strategy", "coordination", "planner"},
		Metadata: map[string]any{
			"default_work_mode": "strategy",
		},
	}); err != nil {
		t.Fatalf("upsert coordinator profile: %v", err)
	}

	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, executionTask, "Repair rq evaluator import path", "Concrete product implementation task that should create product-lane pressure but is not a strategy/coordinator task.", "implementation", []string{"rq", "implementation"}, true)
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET task_requirements_json = ?, write_scope_hints_json = ? WHERE task_id = ?`,
		`{"required_work_modes":["implementation"],"preferred_skills":["go","cli"],"preferred_tools":["shell"]}`,
		`["internal/eval/**","cmd/rq/**"]`,
		executionTask,
	); err != nil {
		t.Fatalf("add product task requirements: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordinate", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              coordinationID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Re-split rq implementation ownership",
		Description:         "Fresh coordination task that must not be offered while terminal patch queue pressure already has product work pending.",
		TaskKind:            model.TaskKindCoordination,
		TaskTemplate:        "generic",
		Tags:                []string{"rq", "coordination", "strategy"},
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create coordination task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: coordinationID, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach coordination task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET updated_at = ? WHERE task_id = ?`, "2026-06-06T05:08:00Z", coordinationID); err != nil {
		t.Fatalf("freshen coordination task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            coordinatorID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      5,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get coordinator work next: %v", err)
	}
	requireBlockedProductPressureFrontier(t, result, executionTask, coordinationID)
}

func TestGetAgentWorkNextSuppressesPatchQueueRepairCoordinationFrontierUnderProductLanePressure(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-r52-product-pressure-pq-repair"
		projectID      = "project-r52-product-pressure-pq-repair"
		leadID         = "alpha"
		coordinatorID  = "iota"
		builderID      = "beta"
		branchOwnerID  = "theta"
		reviewerID     = "eta"
		repoID         = "repo-main"
		executionTask  = "task-rq-product-impl-still-pending"
		coordinationID = "task-rq-patch-queue-repair-churn"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, coordinatorID, builderID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, builderID, leadID, `{"paths":["internal/eval/**","cmd/rq/**"]}`)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["cmd/rq/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-rq-cli-blocked", branchOwnerID, reviewerID, `{"paths":["cmd/rq/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        coordinatorID,
		Specialization: "rq project strategy and coordination",
		Tags:           []string{"strategy", "coordination", "planner"},
		Metadata: map[string]any{
			"default_work_mode": "strategy",
		},
	}); err != nil {
		t.Fatalf("upsert coordinator profile: %v", err)
	}

	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, executionTask, "Repair rq evaluator import path", "Concrete product implementation task that should create product-lane pressure but is not a strategy/coordinator task.", "implementation", []string{"rq", "implementation"}, true)
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET task_requirements_json = ?, write_scope_hints_json = ? WHERE task_id = ?`,
		`{"required_work_modes":["implementation"],"preferred_skills":["go","cli"],"preferred_tools":["shell"]}`,
		`["internal/eval/**","cmd/rq/**"]`,
		executionTask,
	); err != nil {
		t.Fatalf("add product task requirements: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordinate", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              coordinationID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Patch queue repair coordination split",
		Description:         "Fresh coordination task that says patch queue repair and unblock, but is not a typed product handoff.",
		TaskKind:            model.TaskKindCoordination,
		TaskTemplate:        "generic",
		Tags:                []string{"rq", "coordination", "strategy", "patch_queue", "repair", "unblock"},
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create patch queue repair coordination task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: coordinationID, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach coordination task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET updated_at = ? WHERE task_id = ?`, "2026-06-06T05:22:00Z", coordinationID); err != nil {
		t.Fatalf("freshen coordination task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            coordinatorID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      5,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get coordinator work next: %v", err)
	}
	requireBlockedProductPressureFrontier(t, result, executionTask, coordinationID)
}

func TestGetAgentWorkNextSuppressesProjectlessCoordinationReferencingProductUnderPressure(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-r52-product-pressure-projectless"
		projectID      = "project-r52-product-pressure-projectless"
		leadID         = "alpha"
		coordinatorID  = "zeta"
		builderID      = "beta"
		branchOwnerID  = "theta"
		reviewerID     = "eta"
		repoID         = "repo-main"
		executionTask  = "task-rq-product-impl-projectless-ref"
		coordinationID = "task-rq-projectless-claim-scope-split"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, coordinatorID, builderID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, builderID, leadID, `{"paths":["internal/eval/**","cmd/rq/**"]}`)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["cmd/rq/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-rq-cli-blocked", branchOwnerID, reviewerID, `{"paths":["cmd/rq/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        coordinatorID,
		Specialization: "rq project strategy and coordination",
		Tags:           []string{"strategy", "coordination", "planner"},
		Metadata: map[string]any{
			"default_work_mode": "strategy",
		},
	}); err != nil {
		t.Fatalf("upsert coordinator profile: %v", err)
	}

	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, executionTask, "Repair rq evaluator import path", "Concrete product implementation task that should create product-lane pressure but is not a strategy/coordinator task.", "implementation", []string{"rq", "implementation"}, true)
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET task_requirements_json = ?, write_scope_hints_json = ? WHERE task_id = ?`,
		`{"required_work_modes":["implementation"],"preferred_skills":["go","cli"],"preferred_tools":["shell"]}`,
		`["internal/eval/**","cmd/rq/**"]`,
		executionTask,
	); err != nil {
		t.Fatalf("add product task requirements: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordinate", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              coordinationID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Split rq claim scopes around pending product work",
		Description:         "Projectless coordination task mirroring R52: it references a blocked product task, but should not count as product progress.",
		TaskKind:            model.TaskKindCoordination,
		TaskTemplate:        "generic",
		Tags:                []string{"rq", "coordination", "ownership"},
		ProjectLane:         "coordination",
		RequiresProjectGate: false,
	}, graph); err != nil {
		t.Fatalf("create projectless coordination task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: coordinationID, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach coordination task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET task_requirements_json = ?, updated_at = ? WHERE task_id = ?`,
		`{"schema":"task_requirements.v1","blocked_task_id":"`+executionTask+`","blocked_write_scope_json":{"paths":["cmd/**","internal/cli/**","internal/repl/**"]},"observed_reason":"task claim project admission invalid"}`,
		"2026-06-06T05:24:00Z",
		coordinationID,
	); err != nil {
		t.Fatalf("add projectless coordination requirements: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            coordinatorID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      5,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get coordinator work next: %v", err)
	}
	requireBlockedProductPressureFrontier(t, result, executionTask, coordinationID)
}

func TestGetAgentWorkNextSuppressesProjectlessCoordinationTextRefUnderPressure(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-r53-product-pressure-projectless-text-ref"
		projectID      = "project-r53-product-pressure-projectless-text-ref"
		leadID         = "alpha"
		coordinatorID  = "zeta"
		builderID      = "beta"
		branchOwnerID  = "theta"
		reviewerID     = "eta"
		repoID         = "repo-main"
		executionTask  = "task-signal01-rq-evaluator-builtins"
		coordinationID = "task-rq-projectless-claim-scope-text-ref"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, coordinatorID, builderID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, builderID, leadID, `{"paths":["internal/eval/**","cmd/rq/**"]}`)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["README.md","tests/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-rq-cli-blocked", branchOwnerID, reviewerID, `{"paths":["cmd/rq/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        coordinatorID,
		Specialization: "rq project strategy and coordination",
		Tags:           []string{"strategy", "coordination", "planner"},
		Metadata: map[string]any{
			"default_work_mode": "strategy",
		},
	}); err != nil {
		t.Fatalf("upsert coordinator profile: %v", err)
	}

	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, executionTask, "Implement rq evaluator and built-ins", "Concrete product implementation task that should create product-lane pressure.", "implementation", []string{"rq", "implementation"}, true)
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET task_requirements_json = ?, write_scope_hints_json = ? WHERE task_id = ?`,
		`{"required_work_modes":["implementation"],"preferred_tools":["shell"]}`,
		`["internal/eval/**","cmd/rq/**"]`,
		executionTask,
	); err != nil {
		t.Fatalf("add product task requirements: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordinate", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              coordinationID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Repair rq claim scope overlap",
		Description:         "Resolve overlapping write scope between " + executionTask + " and task-signal01-rq-tests-readme without widening product implementation work.",
		TaskKind:            model.TaskKindCoordination,
		TaskTemplate:        "generic",
		Tags:                []string{"rq", "coordination", "project-claim"},
		ProjectLane:         "coordination",
		RequiresProjectGate: false,
	}, graph); err != nil {
		t.Fatalf("create projectless coordination task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: coordinationID, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach coordination task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET task_requirements_json = ?, updated_at = ? WHERE task_id = ?`,
		`{"schema":"task_requirements.v1","preferred_tools":["project_role_assign","project_patch_queue_followup"],"required_work_modes":["coordination","strategy"]}`,
		"2026-06-06T06:21:00Z",
		coordinationID,
	); err != nil {
		t.Fatalf("add projectless coordination requirements: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            coordinatorID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      5,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get coordinator work next: %v", err)
	}
	requireBlockedProductPressureFrontier(t, result, executionTask, coordinationID)

	triggered, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:        workspaceID,
		AgentID:            coordinatorID,
		IncludePacket:      true,
		EnableTaskFrontier: true,
		FrontierLimit:      5,
		Trigger:            "runtime_switch_task",
		CandidateTaskID:    coordinationID,
		CoordinationMode:   sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get triggered coordinator work next: %v", err)
	}
	if triggered.HasWork || triggered.Task != nil || triggered.Reason != "product_lane_pressure" {
		t.Fatalf("triggered projectless text-referenced coordination must not bypass product pressure, got %+v", triggered)
	}
}

func TestProductLanePressurePreservesReadyBranchPatchQueueSubmitHandoff(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-r50-pressure-preserves-submit"
		projectID     = "project-r50-pressure-preserves-submit"
		leadID        = "alpha"
		ownerID       = "beta"
		oldOwnerID    = "theta"
		reviewerID    = "eta"
		repoID        = "repo-main"
		readyBranchID = "branch-beta-ready-unsubmitted"
		taskID        = "task-rq-followup-under-pressure"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, oldOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["cmd/rq/**"]}`)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, oldOwnerID, leadID, `{"paths":["internal/eval/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-theta-blocked", oldOwnerID, reviewerID, `{"paths":["internal/eval/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	readyBranch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, readyBranchID)

	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, taskID, "Continue rq CLI follow-up under patch queue pressure", "Claimable product task that must wait until the owner submits the ready branch to patch queue.", "implementation", []string{"rq", "implementation", "patch-queue"}, true)
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET write_scope_hints_json = ? WHERE task_id = ?`, `["cmd/rq/**"]`, taskID); err != nil {
		t.Fatalf("add write scope hints: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          ownerID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get owner work next under pressure: %v", err)
	}
	if result.HasWork || result.Task != nil || result.Reason != "project_patch_queue_submit_handoff_available" {
		t.Fatalf("READY branch submit handoff must preempt product pressure, got %+v", result)
	}
	if result.Packet == nil || result.Packet.OwnerBound == nil || result.Packet.OwnerBound.BranchID != readyBranch.BranchID {
		t.Fatalf("expected owner-bound submit handoff for branch %s, got %+v", readyBranch.BranchID, result.Packet)
	}
}

func TestGetAgentWorkNextSuppressesGenericValidationSidecarBehindQueueBoundValidation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID        = "ws-patchq-validation-sidecar-suppressed"
		projectID          = "project-patchq-validation-sidecar-suppressed"
		leadID             = "alpha"
		branchOwnerID      = "beta"
		reviewerID         = "theta"
		repoID             = "repo-main"
		queueBoundTaskID   = "task-patchq-validation-clearpress-candidate"
		genericSidecarID   = "task-clearpress-visual-validation-head"
		genericReviewID    = "task-clearpress-candidate-review-head-only"
		genericFreshTaskID = "task-clearpress-visual-validation-new-head"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["src/**","public/**","package*.json"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-beta-ready", branchOwnerID, reviewerID, `{"paths":["src/**","public/**","package.json"]}`, sqlite.ProjectPatchQueueStateBlocked)
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "validate", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	queueBoundRequirements := fmt.Sprintf(`{"schema":"task_requirements.v1","patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1","patch_queue_task_kind":"validation","queue_id":%q,"item_id":%q,"branch_id":%q,"head_sha":%q}`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA)
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               queueBoundTaskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Validate blocked integration candidate " + item.BranchID,
		Description:          fmt.Sprintf("Queue-bound validation for queue_id: %s, item_id: %s, branch_id: %s, head_sha: %s.", item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		Tags:                 []string{"patch-queue", "validation", "visual"},
		ProjectID:            projectID,
		ProjectLane:          "validation",
		TaskRequirementsJSON: queueBoundRequirements,
		RequiresProjectGate:  true,
	}, graph); err != nil {
		t.Fatalf("create queue-bound validation task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      queueBoundTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach queue-bound validation task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ?`, model.TaskStatusResolved, "2026-05-24T00:00:00Z", queueBoundTaskID); err != nil {
		t.Fatalf("mark queue-bound validation task terminal: %v", err)
	}

	olderItem := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-beta-older", branchOwnerID, reviewerID, `{"paths":["src/**","public/**","package.json"]}`, sqlite.ProjectPatchQueueStateBlocked)
	olderHead := strings.Repeat("9", 40)
	if _, err := store.DB().ExecContext(ctx, `UPDATE project_branch_registry SET head_sha = ?, updated_at = ? WHERE workspace_id = ? AND branch_id = ?`, olderHead, "2026-05-23T23:00:00Z", workspaceID, olderItem.BranchID); err != nil {
		t.Fatalf("set older branch head: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE project_patch_queue_items SET head_sha = ?, updated_at = ? WHERE workspace_id = ? AND item_id = ?`, olderHead, "2026-05-23T23:00:00Z", workspaceID, olderItem.ItemID); err != nil {
		t.Fatalf("set older patch queue head: %v", err)
	}
	olderRevisionTaskID := "task-patchq-revision-older-beta-item"
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       olderRevisionTaskID,
		OwnerUserID:  "developer",
		Priority:     "high",
		Title:        "Unblock older beta integration candidate",
		Description:  fmt.Sprintf("Patch queue decision follow-up for queue_id: %s, item_id: %s, branch_id: %s, head_sha: %s.", olderItem.QueueID, olderItem.ItemID, olderItem.BranchID, olderHead),
		TaskKind:     "EXECUTION",
		TaskTemplate: "generic",
		Tags: []string{
			"patch-queue",
			"revision",
			"blocked",
			"owner-bound",
			"owner-agent:" + branchOwnerID,
			"owner-branch:" + olderItem.BranchID,
		},
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create older revision follow-up: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      olderRevisionTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach older revision follow-up: %v", err)
	}

	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              genericSidecarID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Clearpress visual validation for head " + item.HeadSHA[:7],
		Description:         fmt.Sprintf("Generic visual/source validation sidecar for branch_id: %s, head_sha: %s.", item.BranchID, item.HeadSHA[:7]),
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		Tags:                []string{"validation", "visual"},
		ProjectID:           projectID,
		ProjectLane:         "validation",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create generic validation sidecar: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      genericSidecarID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach generic validation sidecar: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  genericSidecarID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get triggered generic sidecar work next: %v", err)
	}
	if result.HasWork && result.Task != nil && result.Task.TaskID == genericSidecarID {
		t.Fatalf("expected generic validation sidecar to be suppressed behind terminal queue-bound validation, got %+v", result)
	}
	if result.Reason != "trigger_task_superseded" {
		t.Fatalf("expected trigger_task_superseded for generic validation sidecar, got %+v", result)
	}

	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              genericReviewID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Clearpress candidate " + item.HeadSHA[:7] + " visual and source-fidelity review",
		Description:         "Generic exact-head review sidecar without queue, item, or branch refs.",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		Tags:                []string{"review", "visual"},
		ProjectID:           projectID,
		ProjectLane:         "review",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create generic review sidecar: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      genericReviewID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach generic review sidecar: %v", err)
	}
	reviewResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  genericReviewID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get triggered generic review sidecar work next: %v", err)
	}
	if reviewResult.HasWork && reviewResult.Task != nil && reviewResult.Task.TaskID == genericReviewID {
		t.Fatalf("expected generic review sidecar to be suppressed behind terminal queue-bound validation, got %+v", reviewResult)
	}
	if reviewResult.Reason != "trigger_task_superseded" {
		t.Fatalf("expected trigger_task_superseded for generic review sidecar, got %+v", reviewResult)
	}

	evidenceRefreshTaskID := "task-clearpress-visual-acceptance-head-refresh"
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               evidenceRefreshTaskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Refresh Clearpress exact-head visual acceptance for beta candidate",
		Description:          fmt.Sprintf("Produce rhizome_visual_acceptance_v1 evidence for beta branch %s at head %s after the terminal queue-bound validation reported missing visual evidence.", item.BranchID, item.HeadSHA),
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		Tags:                 []string{"validation", "visual-acceptance"},
		ProjectID:            projectID,
		ProjectLane:          "validation",
		TaskRequirementsJSON: fmt.Sprintf(`{"schema":"task_requirements.v1","candidate_branch_id":%q,"candidate_head_sha":%q,"required_doc_schema":"rhizome_visual_acceptance_v1","required_work_modes":["validation"],"preferred_tools":["browser"]}`, item.BranchID, item.HeadSHA),
		RequiresProjectGate:  true,
	}, graph); err != nil {
		t.Fatalf("create evidence refresh task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      evidenceRefreshTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach evidence refresh task: %v", err)
	}
	refreshResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  evidenceRefreshTaskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get evidence refresh work next: %v", err)
	}
	if !refreshResult.HasWork || refreshResult.Task == nil || refreshResult.Task.TaskID != evidenceRefreshTaskID {
		t.Fatalf("structured evidence refresh task must stay claimable after terminal negative validation, got %+v", refreshResult)
	}
	if refreshResult.Reason == "trigger_task_superseded" {
		t.Fatalf("terminal negative validation must not supersede structured evidence refresh, got %+v", refreshResult)
	}

	freshHead := strings.Repeat("c", 40)
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              genericFreshTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Clearpress visual validation for head " + freshHead[:7],
		Description:         fmt.Sprintf("Generic visual/source validation sidecar for branch_id: %s, head_sha: %s.", item.BranchID, freshHead[:7]),
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		Tags:                []string{"validation", "visual"},
		ProjectID:           projectID,
		ProjectLane:         "validation",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create fresh-head validation sidecar: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      genericFreshTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach fresh-head validation sidecar: %v", err)
	}
	freshResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  genericFreshTaskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get fresh-head sidecar work next: %v", err)
	}
	if freshResult.Reason == "trigger_task_superseded" {
		t.Fatalf("fresh-head validation sidecar should not reuse stale terminal queue-bound evidence, got %+v", freshResult)
	}
}

func TestGetAgentWorkNextSuppressesEvidenceRefreshBehindExactVisualEvidenceDoc(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-patchq-visual-evidence-doc-suppressed"
		projectID     = "project-patchq-visual-evidence-doc-suppressed"
		leadID        = "alpha"
		branchOwnerID = "beta"
		reviewerID    = "theta"
		repoID        = "repo-main"
		refreshTaskID = "task-clearpress-visual-acceptance-head-refresh"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["src/**","public/**","package*.json"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-beta-ready", branchOwnerID, reviewerID, `{"paths":["src/**","public/**","package.json"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "task.task-clearpress-visual-acceptance-beta-head.visual_acceptance"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Clearpress Visual Acceptance - beta head " + item.HeadSHA[:8] + " fail",
		Content: fmt.Sprintf(`schema: rhizome_visual_acceptance_v1
queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
visual_verdict: fail
severity: blocking
screenshot_ref: screenshots/desktop.png
viewport_matrix: desktop and narrow`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write exact visual evidence doc: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "validate", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := createAgentWorkTaskBypassingSubmitGate(t, ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               refreshTaskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Refresh Clearpress exact-head visual acceptance for beta candidate",
		Description:          fmt.Sprintf("Produce rhizome_visual_acceptance_v1 evidence for beta branch %s at head %s.", item.BranchID, item.HeadSHA[:8]),
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		Tags:                 []string{"validation", "visual-acceptance"},
		ProjectID:            projectID,
		ProjectLane:          "validation",
		TaskRequirementsJSON: fmt.Sprintf(`{"schema":"task_requirements.v1","candidate_branch_id":%q,"candidate_head_sha":%q,"required_doc_schema":"rhizome_visual_acceptance_v1","required_work_modes":["validation"],"preferred_tools":["browser"]}`, item.BranchID, item.HeadSHA),
		RequiresProjectGate:  true,
	}, graph); err != nil {
		t.Fatalf("create evidence refresh task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      refreshTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach evidence refresh task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  refreshTaskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get evidence refresh work next: %v", err)
	}
	if result.HasWork && result.Task != nil && result.Task.TaskID == refreshTaskID {
		t.Fatalf("expected exact-head visual evidence doc to suppress refresh task, got %+v", result)
	}
	if result.Reason != "trigger_task_superseded" {
		t.Fatalf("expected trigger_task_superseded for evidence refresh behind exact visual doc, got %+v", result)
	}
}

func TestTaskClaimAdmissionAllowsOwnerRevisionForkFromBlockedPatchQueueSourceBranch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-owner-revision-fork-admission"
		projectID     = "project-owner-revision-fork-admission"
		leadID        = "alpha"
		branchOwnerID = "gamma"
		reviewerID    = "epsilon"
		repoID        = "repo-main"
		sourceTaskID  = "task-source-candidate"
		followupID    = "task-owner-revision-fork"
		sourceBranch  = "branch-source-ready-for-review"
		forkBranch    = "branch-owner-revision-fork"
		reviewDocKey  = "project.owner-revision-fork.branch.review"
	)
	baseSHA := strings.Repeat("c", 40)
	headSHA := strings.Repeat("d", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["app/**"]}`)
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
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	transitionProjectPhaseForBranchTest(t, ctx, store, workspaceID, projectID, leadID, sqlite.ProjectPhaseImplementation)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, sourceTaskID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, followupID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, `C:\fixtures\agents\gamma\owner-revision-fork`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewDocKey,
		Title:       "Revision Fork Review Packet",
		Content:     "# Review Packet\n\nCandidate needs an owner revision.",
		UpdatedBy:   branchOwnerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, checkout.CheckoutID, sourceBranch, sourceTaskID, `{"paths":["app/**"]}`)
	source, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchOwnerID,
		BranchID:              sourceBranch,
		ActiveTaskID:          sourceTaskID,
		ActiveClaimID:         sourceTaskID,
		BranchName:            "agent/gamma/source-ready-for-review",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		ReviewDocKey:          reviewDocKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register source branch: %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              source.BranchID,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit source patch queue item: %v", err)
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
		t.Fatalf("claim source patch queue item: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Needs an owner revision before integration.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block source patch queue item: %v", err)
	}
	fork, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchOwnerID,
		BranchID:              forkBranch,
		BranchName:            "agent/gamma/revision-fork",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            source.BranchName,
		BaseSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register fork branch: %v", err)
	}
	writeFollowupMetadata := func(description string) {
		t.Helper()
		if _, err := store.DB().ExecContext(ctx,
			`UPDATE tasks SET title = ?, description = ?, tags_json = ? WHERE task_id = ?`,
			"Unblock integration candidate "+source.BranchID,
			description,
			`["project","patch-queue","revision","blocked","owner-bound","owner-bound-kind:patch_queue_revision","owner-branch:`+source.BranchID+`","owner-agent:`+branchOwnerID+`","required-agent:`+branchOwnerID+`"]`,
			followupID,
		); err != nil {
			t.Fatalf("write owner revision fork metadata: %v", err)
		}
	}
	claimFollowup := func(summary string) error {
		t.Helper()
		_, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
			WorkspaceID:           workspaceID,
			TaskID:                followupID,
			AgentID:               branchOwnerID,
			BranchID:              fork.BranchID,
			CheckoutID:            checkout.CheckoutID,
			WriteScopeJSON:        `{"paths":["app/**"]}`,
			CoordinationMode:      sqlite.CoordinationModeTrustFirst,
			Summary:               summary,
			PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, followupID, branchOwnerID),
		})
		return err
	}
	for _, tc := range []struct {
		name        string
		description string
	}{
		{
			name:        "missing head",
			description: "Patch queue decision follow-up.\n\n- queue_id: " + item.QueueID + "\n- item_id: " + item.ItemID + "\n- branch_id: " + source.BranchID + "\n- state: BLOCKED",
		},
		{
			name:        "wrong head",
			description: "Patch queue decision follow-up.\n\n- queue_id: " + item.QueueID + "\n- item_id: " + item.ItemID + "\n- branch_id: " + source.BranchID + "\n- head_sha: " + strings.Repeat("e", 40) + "\n- state: BLOCKED",
		},
		{
			name:        "missing queue item",
			description: "Patch queue decision follow-up.\n\n- branch_id: " + source.BranchID + "\n- head_sha: " + headSHA + "\n- state: BLOCKED",
		},
	} {
		writeFollowupMetadata(tc.description)
		err := claimFollowup("claim owner revision fork with " + tc.name)
		if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) ||
			(!strings.Contains(err.Error(), "overlaps live branch_id="+source.BranchID) &&
				!strings.Contains(err.Error(), "overlaps active claim") &&
				!strings.Contains(err.Error(), "branch_id="+source.BranchID)) {
			t.Fatalf("expected %s metadata to preserve source branch overlap, got %v", tc.name, err)
		}
	}
	writeFollowupMetadata("Create a revision on branch provenance `" + source.BranchID + "` at head `" + headSHA + "` for blocked patch queue item `" + item.ItemID + "` in queue `" + item.QueueID + "`. Produce the smallest product revision on an owned implementation branch, publish review-ready evidence for the new head, and request fresh validation.")
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE tasks SET tags_json = ? WHERE task_id = ?`,
		`["clearpress","revision","frontend","ac-15","patch-queue"]`,
		followupID,
	); err != nil {
		t.Fatalf("write prose-shaped revision follow-up tags: %v", err)
	}
	now := "2026-05-17T00:00:00Z"
	if _, err := store.DB().ExecContext(ctx, `
UPDATE task_claims
   SET agent_id = ?,
       claim_status = ?,
       summary = ?,
       claimed_at = ?,
       updated_at = ?,
       project_role_id = '',
       repo_id = ?,
       checkout_id = '',
       branch_id = ?,
       write_scope_json = ?
 WHERE workspace_id = ?
   AND task_id = ?`,
		reviewerID,
		model.TaskClaimStatusClaimed,
		"non-owner active source claim must still block revision fork overlap",
		now,
		now,
		repoID,
		source.BranchID,
		`{"paths":["app/**"]}`,
		workspaceID,
		sourceTaskID,
	); err != nil {
		t.Fatalf("write non-owner source claim: %v", err)
	}
	if err := claimFollowup("claim owner revision fork while non-owner source claim active"); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "overlaps active claim") {
		t.Fatalf("expected non-owner active source claim to block revision fork, got %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`DELETE FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		sourceTaskID,
	); err != nil {
		t.Fatalf("delete non-owner source claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE project_branch_registry SET agent_id = ? WHERE workspace_id = ? AND branch_id = ?`,
		"stale-recorded-owner",
		workspaceID,
		source.BranchID,
	); err != nil {
		t.Fatalf("write stale source branch owner: %v", err)
	}
	predecessor := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-owner-revision-predecessor", branchOwnerID, reviewerID, `{"paths":["app/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET updated_at = CASE item_id WHEN ? THEN ? WHEN ? THEN ? ELSE updated_at END,
       decided_at = CASE item_id WHEN ? THEN ? WHEN ? THEN ? ELSE decided_at END
 WHERE workspace_id = ? AND project_id = ? AND item_id IN (?, ?)`,
		predecessor.ItemID, "2026-05-17T00:00:00Z",
		item.ItemID, "2026-05-17T00:01:00Z",
		predecessor.ItemID, "2026-05-17T00:00:00Z",
		item.ItemID, "2026-05-17T00:01:00Z",
		workspaceID, projectID, predecessor.ItemID, item.ItemID,
	); err != nil {
		t.Fatalf("backdate predecessor patch queue item: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE project_branch_registry SET active_task_id = ?, active_claim_id = ? WHERE workspace_id = ? AND branch_id = ?`,
		"task-live-predecessor",
		"claim-live-predecessor",
		workspaceID,
		predecessor.BranchID,
	); err != nil {
		t.Fatalf("mark predecessor branch active: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                followupID,
		AgentID:               branchOwnerID,
		BranchID:              fork.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim owner revision fork while predecessor branch still has active refs",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, followupID, branchOwnerID),
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !(strings.Contains(err.Error(), "overlaps live branch_id="+predecessor.BranchID) || (strings.Contains(err.Error(), "overlaps active claim") && strings.Contains(err.Error(), "branch_id="+predecessor.BranchID))) {
		t.Fatalf("expected active predecessor branch refs to block older-predecessor allowance, got %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE project_branch_registry SET active_task_id = '', active_claim_id = '' WHERE workspace_id = ? AND branch_id = ?`,
		workspaceID,
		predecessor.BranchID,
	); err != nil {
		t.Fatalf("clear predecessor branch active refs: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                followupID,
		AgentID:               branchOwnerID,
		BranchID:              fork.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["app/**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim owner revision fork",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, followupID, branchOwnerID),
	}); err != nil {
		t.Fatalf("expected owner revision fork claim to ignore exact source branch overlap: %v", err)
	}
}

func TestOwnerBoundProseBranchMentionInfersBranchOwner(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-owner-bound-prose-branch"
		projectID     = "project-owner-bound-prose-branch"
		leadID        = "alpha"
		branchOwnerID = "beta"
		otherAgentID  = "eta"
		repoID        = "repo-main"
		taskID        = "task-requeue-projbranch-1778629299299060243-10986"
		branchID      = "projbranch-1778629299299060243-10986"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, otherAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, branchID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "submit", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Beta owner requeue submit for validated branch",
		Description:         "Create a precise branch-bound owner lane for beta to requeue the existing validated candidate. Scope: branch `" + branchID + "` / `agent-beta-p-656e957c8a-t-83d0d5cd28`. Required action: run project_patch_queue_submit as the branch owner only.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "patch-queue", "requeue", "coordination", "owner-only", "beta"},
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create prose owner-bound task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach prose owner-bound task: %v", err)
	}

	otherResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          otherAgentID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get non-owner work next: %v", err)
	}
	if otherResult.HasWork || otherResult.Reason != "project_owner_bound_agent_required" {
		t.Fatalf("expected prose owner-bound task to route away from non-owner, got %+v", otherResult)
	}
	if otherResult.Packet == nil || otherResult.Packet.OwnerBound == nil || otherResult.Packet.OwnerBound.RequiredAgentID != branchOwnerID || otherResult.Packet.OwnerBound.BranchID != branchID {
		t.Fatalf("expected inferred branch owner packet, got %+v", otherResult.Packet)
	}

	ownerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get owner work next: %v", err)
	}
	if !ownerResult.HasWork || ownerResult.Task == nil || ownerResult.Task.TaskID != taskID {
		t.Fatalf("expected branch owner to receive prose owner-bound task, got %+v", ownerResult)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
		Summary:          "owner may claim prose branch-bound submit task",
	}); err != nil {
		t.Fatalf("expected branch owner claim to infer prose branch mention: %v", err)
	}
}

func TestOwnerSubmitTagInfersOwnerBoundBranchOwner(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-owner-submit-tag"
		projectID     = "project-owner-submit-tag"
		leadID        = "alpha"
		branchOwnerID = "beta"
		otherAgentID  = "eta"
		repoID        = "repo-main"
		taskID        = "task-requeue-owner-submit-icon-sprite-forge-dc954850-20260513"
		branchID      = "projbranch-1778648866113702379-5892"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, otherAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, branchID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "submit", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Owner requeue submit for same-head patch queue item on projbranch-1778648866113702379-5892",
		Description:         "Create a fresh live COORDINATION task for branch owner beta to perform the owner-only requeue submission.\n\nbranch_id: " + branchID + "\nbranch_name: agent-beta-p-656e957c8a-t-83d0d5cd28",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "patch-queue", "requeue", "coordination", "owner-submit"},
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create owner-submit tag task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach owner-submit tag task: %v", err)
	}

	otherResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          otherAgentID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get non-owner work next: %v", err)
	}
	if otherResult.HasWork || otherResult.Reason != "project_owner_bound_agent_required" {
		t.Fatalf("expected owner-submit tag task to route away from non-owner, got %+v", otherResult)
	}
	if otherResult.Packet == nil || otherResult.Packet.OwnerBound == nil || otherResult.Packet.OwnerBound.RequiredAgentID != branchOwnerID || otherResult.Packet.OwnerBound.BranchID != branchID {
		t.Fatalf("expected owner-submit packet for branch owner, got %+v", otherResult.Packet)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          leadID,
		CoordinationMode: "trust_first",
		Summary:          "lead should not claim owner-submit task",
	}); err == nil || !strings.Contains(err.Error(), "owner-bound task") {
		t.Fatalf("expected non-owner owner-submit claim rejection, got %v", err)
	}

	ownerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get owner work next: %v", err)
	}
	if !ownerResult.HasWork || ownerResult.Task == nil || ownerResult.Task.TaskID != taskID {
		t.Fatalf("expected branch owner to receive owner-submit task, got %+v", ownerResult)
	}
}

func TestOwnerSubmitTitleBranchIDWinsOverDescriptionBranchNoise(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-owner-submit-title-branch"
		projectID     = "project-owner-submit-title-branch"
		leadID        = "alpha"
		branchOwnerID = "beta"
		staleOwnerID  = "gamma"
		repoID        = "repo-main"
		taskID        = "task-owner-submit-title-branch"
		branchID      = "projbranch-1778648866113702379-5892"
		staleBranchID = "projbranch-1778570052239818287-105568"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, staleOwnerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, branchID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, staleOwnerID, staleBranchID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "submit", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Beta owner-side same-head requeue submit for " + branchID,
		Description:         "Context includes stale coordination notes about `" + staleBranchID + "`, but the branch named in the task title is the owner-submit target.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "patch-queue", "requeue", "coordination", "owner-submit"},
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create owner-submit title branch task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    leadID,
	}); err != nil {
		t.Fatalf("attach owner-submit title branch task: %v", err)
	}

	staleOwnerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          staleOwnerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get stale owner work next: %v", err)
	}
	if staleOwnerResult.HasWork || staleOwnerResult.Reason != "project_owner_bound_agent_required" {
		t.Fatalf("expected stale mentioned branch owner to route away from title-targeted task, got %+v", staleOwnerResult)
	}
	if staleOwnerResult.Packet == nil || staleOwnerResult.Packet.OwnerBound == nil || staleOwnerResult.Packet.OwnerBound.RequiredAgentID != branchOwnerID || staleOwnerResult.Packet.OwnerBound.BranchID != branchID {
		t.Fatalf("expected title branch owner packet, got %+v", staleOwnerResult.Packet)
	}
	ownerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get owner work next: %v", err)
	}
	if !ownerResult.HasWork || ownerResult.Task == nil || ownerResult.Task.TaskID != taskID {
		t.Fatalf("expected title branch owner to receive owner-submit task, got %+v", ownerResult)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
		Summary:          "owner may claim title-targeted submit task",
	}); err != nil {
		t.Fatalf("expected title branch owner claim to pass despite description noise: %v", err)
	}
}

func TestOwnerSubmitTerminalAcceptedBranchDoesNotBlockCurrentOwnerSubmit(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-owner-submit-terminal-accepted"
		projectID     = "project-owner-submit-terminal-accepted"
		leadID        = "alpha"
		branchOwnerID = "beta"
		reviewerID    = "zeta"
		repoID        = "repo-main"
		currentTaskID = "task-current-owner-submit"
		staleTaskID   = "task-stale-owner-submit"
		currentBranch = "projbranch-current-ready"
		staleBranch   = "projbranch-stale-merged"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, reviewerID})
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
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, currentBranch)
	accepted := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, staleBranch, branchOwnerID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateAccepted)
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		IncludeInactive: true,
	})
	if err != nil {
		t.Fatalf("list stale branch: %v", err)
	}
	var staleBranchRecord sqlite.ProjectBranchRecord
	for _, branch := range branches {
		if branch.BranchID == staleBranch {
			staleBranchRecord = branch
			break
		}
	}
	if staleBranchRecord.BranchID == "" {
		t.Fatalf("expected stale branch in %+v", branches)
	}
	recordIntegratedReceiptForAcceptedBranchForMergeTest(t, ctx, store, workspaceID, projectID, staleBranchRecord, leadID, "main", strings.Repeat("8", 40), strings.Repeat("9", 40))
	if _, _, err := closeBranchAsMergedForTest(ctx, store, workspaceID, projectID, repoID, staleBranchRecord, branchOwnerID, leadID); err != nil {
		t.Fatalf("integration-authority close of stale branch as merged: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "submit", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              currentTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Beta owner-side same-head requeue submit for " + currentBranch,
		Description:         "Submit the current READY_FOR_REVIEW branch `" + currentBranch + "`.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "patch-queue", "requeue", "coordination", "owner-submit"},
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create current owner-submit task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: currentTaskID, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach current owner-submit task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              staleTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Beta owner-submit implementation-lane requeue for " + staleBranch,
		Description:         "Required action: beta submits already accepted candidate `" + accepted.ItemID + "` on branch `" + staleBranch + "`.",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "patch-queue", "requeue", "owner-only", "beta", "implementation", "branch-bound", staleBranch},
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create stale owner-submit task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: staleTaskID, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach stale owner-submit task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           staleTaskID,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
		Summary:          "stale owner-submit should not be claimable",
	}); err == nil || (!strings.Contains(err.Error(), "already has an ACCEPTED same-head patch queue decision") && !strings.Contains(err.Error(), "superseded by newer project evidence")) {
		t.Fatalf("expected stale owner-submit claim admission rejection, got %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get owner work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != currentTaskID {
		t.Fatalf("expected current READY owner-submit task, got %+v", result)
	}
}

func TestOwnerBoundRequiredAgentConflictRequiresRepair(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-owner-bound-conflict"
		projectID     = "project-owner-bound-conflict"
		leadID        = "alpha"
		branchOwnerID = "gamma"
		staleTagAgent = "iota"
		repoID        = "repo-main"
		taskID        = "task-owner-bound-conflicting-required-agent"
		branchID      = "branch-owner-bound-conflict"
		reviewKey     = "project.owner-bound-conflict.branch.review"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, staleTagAgent})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, `C:\fixtures\agents\gamma\owner-bound-conflict`)
	_, branchTaskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, branchOwnerID, branchID, "agent/gamma/owner-bound-conflict", `{"paths":["src/**"]}`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Review packet",
		Content:     "# Review\n\nReady.",
		UpdatedBy:   branchOwnerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchOwnerID,
		BranchID:              branchID,
		ActiveTaskID:          branchTaskID,
		ActiveClaimID:         branchTaskID,
		BranchName:            "agent/gamma/owner-bound-conflict",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register ready branch: %v", err)
	}
	createOwnerBoundPatchQueueSubmitTask(t, ctx, store, workspaceID, projectID, taskID, branchID, staleTagAgent)

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          staleTagAgent,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get stale tag owner work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_owner_bound_repair_required" {
		t.Fatalf("expected conflicting required-agent tag to require repair, got %+v", result)
	}
	if result.Packet == nil || result.Packet.OwnerBound == nil || !result.Packet.OwnerBound.RepairNeeded || result.Packet.OwnerBound.RequiredAgentID != branchOwnerID {
		t.Fatalf("expected repair packet to preserve registry branch owner, got %+v", result.Packet)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          staleTagAgent,
		CoordinationMode: "trust_first",
		Summary:          "stale tag should not win",
	}); err == nil || !strings.Contains(err.Error(), "requires strategic repair") {
		t.Fatalf("expected conflicting owner-bound claim to be rejected as repair-needed, got %v", err)
	}
}

func TestOwnerBoundPatchQueueSubmitWithoutBranchRequiresRepair(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-owner-bound-missing-branch"
		projectID   = "project-owner-bound-missing-branch"
		agentID     = "iota"
		taskID      = "task-owner-bound-missing-branch"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Missing branch",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "submit", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "high",
		Title:        "Owner-only project_patch_queue_submit missing branch",
		Description:  "Owner-only submit without branch evidence.",
		TaskKind:     "EXECUTION",
		TaskTemplate: "integration",
		Tags:         []string{"project", "patch-queue", "integration", "owner-bound", "owner-bound-kind:patch_queue_submit", "required-agent:" + agentID},
		ProjectID:    projectID,
		ProjectLane:  "integration",
	}, graph); err != nil {
		t.Fatalf("create owner-bound missing branch task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: taskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach owner-bound missing branch task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          agentID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get missing branch work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_owner_bound_repair_required" {
		t.Fatalf("expected missing branch owner-bound task to require repair, got %+v", result)
	}
	if result.Packet == nil || result.Packet.OwnerBound == nil || !result.Packet.OwnerBound.RepairNeeded {
		t.Fatalf("expected structured repair packet, got %+v", result.Packet)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          agentID,
		CoordinationMode: "trust_first",
		Summary:          "missing branch should not claim",
	}); err == nil || !strings.Contains(err.Error(), "requires strategic repair") {
		t.Fatalf("expected missing branch owner-bound claim to be rejected as repair-needed, got %v", err)
	}
}

func TestOwnerBoundPatchQueueSubmitInfersUniqueRequiredAgentBranch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-owner-bound-unique-branch"
		projectID     = "project-owner-bound-unique-branch"
		leadID        = "alpha"
		branchOwnerID = "gamma"
		repoID        = "repo-main"
		taskID        = "task-owner-bound-unique-branch"
		branchID      = "branch-owner-bound-unique"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, branchID)
	createOwnerBoundPatchQueueSubmitTaskWithoutBranch(t, ctx, store, workspaceID, projectID, taskID, branchOwnerID)

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get owner work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != taskID {
		t.Fatalf("expected owner-bound task to infer the owner's unique branch, got %+v", result)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
		Summary:          "owner claims inferred branch submit",
	}); err != nil {
		t.Fatalf("expected owner claim with inferred branch to succeed: %v", err)
	}
}

func TestOwnerBoundPatchQueueSubmitAmbiguousRequiredAgentBranchesRequireRepair(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-owner-bound-ambiguous-branch"
		projectID     = "project-owner-bound-ambiguous-branch"
		leadID        = "alpha"
		branchOwnerID = "gamma"
		repoID        = "repo-main"
		taskID        = "task-owner-bound-ambiguous-branch"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, "branch-owner-bound-ambiguous-a")
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, "branch-owner-bound-ambiguous-b")
	createOwnerBoundPatchQueueSubmitTaskWithoutBranch(t, ctx, store, workspaceID, projectID, taskID, branchOwnerID)

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get owner work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_owner_bound_repair_required" {
		t.Fatalf("expected ambiguous owner branches to require repair, got %+v", result)
	}
	if result.Packet == nil || result.Packet.OwnerBound == nil || !strings.Contains(result.Packet.OwnerBound.Reason, "multiple open branches") {
		t.Fatalf("expected ambiguous branch repair packet, got %+v", result.Packet)
	}
	err = store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
		Summary:          "ambiguous branch should not claim",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "multiple open branches") {
		t.Fatalf("expected ambiguous owner-bound claim rejection, got %v", err)
	}
}

func TestGetAgentWorkNextReturnsEligibleCoordinationBeforeOwnerBoundRepairDiagnostic(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-owner-bound-diagnostic-ordering"
		projectID   = "project-owner-bound-diagnostic-ordering"
		agentID     = "alpha"
		ownerID     = "gamma"
		blockedID   = "task-owner-bound-missing-branch-ordering"
		eligibleID  = "task-coordination-after-owner-bound-diagnostic"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID, ownerID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Owner Bound Diagnostic Ordering",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createOwnerBoundPatchQueueSubmitTaskWithoutBranch(t, ctx, store, workspaceID, projectID, blockedID, ownerID)
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, eligibleID, "Clarify canonical coordination path", "coordination", "high")

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          agentID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != eligibleID {
		t.Fatalf("eligible coordination task must win over owner-bound diagnostic, got %+v", result)
	}
}

func TestOwnerBoundDetectionLeavesValidationFollowupClaimable(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-owner-bound-validation-negative"
		projectID   = "project-owner-bound-validation-negative"
		repoID      = "repo-owner-bound-validation-negative"
		agentID     = "iota"
		branchID    = "branch-validation-negative"
		taskID      = "task-patchq-validation-negative"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, agentID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, agentID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, agentID)
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, agentID, branchID)
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		ActorID:               agentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit patch queue item: %v", err)
	}
	description := fmt.Sprintf("Patch queue decision follow-up.\n\n- queue_id: %s\n- item_id: %s\n- branch_id: %s\n\nIf evidence passes, the branch owner should call project_patch_queue_submit; this validation task itself is not owner-only.", item.QueueID, item.ItemID, branch.BranchID)
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "validation", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "high",
		Title:        "Validate owner-submit candidate without claiming owner-submit",
		Description:  description,
		TaskKind:     "EXECUTION",
		TaskTemplate: "integration",
		Tags:         []string{"project", "patch-queue", "validation", "blocked"},
		ProjectID:    projectID,
		ProjectLane:  "validation",
	}, graph); err != nil {
		t.Fatalf("create validation follow-up: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: taskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach validation follow-up: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          agentID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get validation follow-up work next: %v", err)
	}
	if !work.HasWork || work.Task == nil || work.Task.TaskID != taskID {
		t.Fatalf("expected validation follow-up to remain claimable, got %+v", work)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          agentID,
		CoordinationMode: "trust_first",
		Summary:          "validation follow-up is not owner-bound",
	}); err != nil {
		t.Fatalf("expected validation follow-up claim to succeed: %v", err)
	}
}

func TestOwnerBoundDetectionIgnoresHistoricalHintsInReflectionDescription(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-owner-bound-reflection-hints"
		projectID   = "project-owner-bound-reflection-hints"
		agentID     = "alpha"
		taskID      = "task-idle-reflection-owner-bound-hints"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Owner Bound Reflection Hints",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "reflection", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Artifact quality iteration: inspect evidence and concrete gaps",
		Description:  "Reflection context copied from an earlier snapshot.\n\nOpen task hints:\n- task-old-owner-submit [PENDING]: Провести owner-only patch-queue submit для gamma branch\n\nThe current task is the artifact review itself.",
		TaskKind:     "EXECUTION",
		TaskTemplate: "generic",
		Tags: []string{
			"meta-reflection",
			"anti-idle",
			"product-quality",
			"post-mvp",
			"qa",
			"planning",
			"metacognition-scope-artifact",
		},
		ProjectID:   projectID,
		ProjectLane: "qa",
	}, graph); err != nil {
		t.Fatalf("create reflection task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: taskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach reflection task: %v", err)
	}

	work, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          agentID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get reflection work next: %v", err)
	}
	if !work.HasWork || work.Task == nil || work.Task.TaskID != taskID {
		t.Fatalf("reflection task must remain claimable despite quoted owner-only hint, got %+v", work)
	}
	if work.Packet != nil && work.Packet.OwnerBound != nil {
		t.Fatalf("reflection task must not carry owner-bound packet from historical hints, got %+v", work.Packet.OwnerBound)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          agentID,
		CoordinationMode: "trust_first",
		Summary:          "artifact reflection, not owner-bound submit",
	}); err != nil {
		t.Fatalf("expected reflection task claim to succeed: %v", err)
	}
}

func TestGetAgentWorkNextSkipsStalePatchQueueRevisionWhenAcceptedReplacementExists(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-agent-stale-patchq-replacement"
		projectID     = "project-stale-patchq-replacement"
		leadID        = "alpha"
		branchOwnerID = "gamma"
		reviewerID    = "kappa"
		repoID        = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	designDocID := "doc.stale-patchq-replacement.design"
	implementationPlanDocID := "doc.stale-patchq-replacement.plan"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 leadID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("open project gates: %v", err)
	}
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "implementation lane may start",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               branchOwnerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign implementer role: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               reviewerID,
		RoleType:              sqlite.ProjectRoleIntegrator,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign integrator role: %v", err)
	}
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	staleSrc := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-stale-src", branchOwnerID, reviewerID, `{"paths":["src/**","package.json"]}`, sqlite.ProjectPatchQueueStateBlocked)
	staleDocs := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-stale-docs", branchOwnerID, reviewerID, `{"paths":["docs/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	_ = createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-accepted-src", branchOwnerID, reviewerID, `{"paths":["src/**","package.json"]}`, sqlite.ProjectPatchQueueStateAccepted)

	// The BLOCKED decisions eagerly materialize each branch owner's revision continuation (gamma holds a claimable
	// IMPLEMENTER role) - the system's own revision follow-ups, no manual seed needed. The stale-source revision
	// must be skipped because an ACCEPTED src/** replacement supersedes it; the unrelated docs revision stays
	// claimable. This asserts the staleness/supersession selection covers the materialized continuation carriers.
	staleContinuationID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(projectID, staleSrc, "revision")
	docsContinuationID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(projectID, staleDocs, "revision")

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get branch owner work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != docsContinuationID {
		t.Fatalf("expected overlapping stale source follow-up to be skipped while unrelated docs follow-up remains claimable, got %+v", result)
	}
	if result.Task.TaskID == staleContinuationID {
		t.Fatalf("stale source revision continuation must be superseded by the accepted src replacement, got %+v", result)
	}
}

func TestGetAgentWorkNextSkipsStalePatchQueueRevisionWhenNewerBlockedCandidateHasVisualEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-agent-stale-patchq-blocked-visual"
		projectID     = "project-stale-patchq-blocked-visual"
		leadID        = "alpha"
		branchOwnerID = "beta"
		reviewerID    = "kappa"
		repoID        = "repo-main"
		staleTaskID   = "task-patchq-revision-old-beta-head"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, `{"paths":["src/**","package.json"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	staleItem := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-old-beta-head", branchOwnerID, reviewerID, `{"paths":["src/**","package.json"]}`, sqlite.ProjectPatchQueueStateBlocked)
	newerItem := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-new-beta-head", branchOwnerID, reviewerID, `{"paths":["src/**","package.json"]}`, sqlite.ProjectPatchQueueStateBlocked)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              staleTaskID,
		OwnerUserID:         "developer",
		Priority:            "critical",
		Title:               "Unblock integration candidate " + staleItem.BranchID,
		Description:         "Patch queue: " + staleItem.QueueID + "/" + staleItem.ItemID + "\nBranch ID: " + staleItem.BranchID + "\nhead_sha: " + staleItem.HeadSHA,
		TaskKind:            "EXECUTION",
		TaskTemplate:        "integration",
		Tags:                []string{"project", "patch-queue", "revision", "blocked"},
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"**"},
	}, graph); err != nil {
		t.Fatalf("create stale revision task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      staleTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach stale revision task: %v", err)
	}

	beforeEvidence, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  staleTaskID,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get stale revision before evidence: %v", err)
	}
	if beforeEvidence.HasWork || beforeEvidence.Reason != "project_claim_scope_busy" {
		t.Fatalf("newer overlapping review-ready branch should block stale revision claim before exact evidence receipt, got %+v", beforeEvidence)
	}

	evidenceDocKey := "task.visual.new-beta-head.visual_acceptance"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Visual acceptance for newer beta head " + newerItem.HeadSHA[:8],
		Content: fmt.Sprintf(`schema: rhizome_visual_acceptance_v1
queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
visual_verdict: fail
severity: blocking
screenshot_ref: screenshots/new-beta-head.png
viewport_matrix: desktop and narrow`, newerItem.QueueID, newerItem.ItemID, newerItem.BranchID, newerItem.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write visual evidence doc: %v", err)
	}

	afterEvidence, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  staleTaskID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get stale revision after evidence: %v", err)
	}
	if afterEvidence.HasWork || afterEvidence.Reason != "trigger_task_superseded" {
		t.Fatalf("expected stale revision follow-up to be superseded behind newer visual-evidenced blocker, got %+v", afterEvidence)
	}
}

func TestGetAgentWorkNextIgnoresSupersededTaskAsDependencyBlocker(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-agent-superseded-dependency"
		projectID     = "project-superseded-dependency"
		leadID        = "alpha"
		branchOwnerID = "gamma"
		reviewerID    = "kappa"
		repoID        = "repo-main"
		staleTaskID   = "task-stale-beta-revision"
		repairTaskID  = "task-ambient-repair-stale-candidate"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               branchOwnerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign implementer role: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               reviewerID,
		RoleType:              sqlite.ProjectRoleIntegrator,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign integrator role: %v", err)
	}
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	staleItem := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-stale-src", branchOwnerID, reviewerID, `{"paths":["src/**","package.json"]}`, sqlite.ProjectPatchQueueStateBlocked)
	_ = createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-accepted-src", branchOwnerID, reviewerID, `{"paths":["src/**","package.json"]}`, sqlite.ProjectPatchQueueStateAccepted)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              staleTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Repair stale source candidate",
		Description:         "Patch queue: " + staleItem.QueueID + "/" + staleItem.ItemID + "\nBranch ID: " + staleItem.BranchID,
		TaskKind:            "EXECUTION",
		TaskTemplate:        "integration",
		Tags:                []string{"project", "patch-queue", "revision", "blocked"},
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create stale task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      staleTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach stale task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		OwnerUserID: "developer",
		Priority:    "critical",
		Title:       "Disposition stale candidate and unblock integration",
		Description: "Determine whether the stale branch still matters now that an accepted overlapping candidate exists, then record the integration path.\nPatch queue: " + staleItem.QueueID + "/" + staleItem.ItemID + "\nBranch ID: " + staleItem.BranchID,
		TaskKind:    "COORDINATION",
		Tags:        []string{"project", "ambient-repair", "patch-queue", "revision"},
		ProjectID:   projectID,
		ProjectLane: "coordination",
	}, graph); err != nil {
		t.Fatalf("create repair task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach repair task: %v", err)
	}
	if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
		WorkspaceID: workspaceID,
		FromTaskID:  staleTaskID,
		ToTaskID:    repairTaskID,
		LinkType:    model.TaskLinkBlocks,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("add dependency link: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		CoordinationMode: "trust_first",
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get lead work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != repairTaskID {
		t.Fatalf("expected superseded dependency to stop blocking repair task, got %+v", result)
	}
	targetedRepair, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		CoordinationMode: "trust_first",
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  repairTaskID,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get targeted repair work next: %v", err)
	}
	if !targetedRepair.HasWork || targetedRepair.Task == nil || targetedRepair.Task.TaskID != repairTaskID {
		t.Fatalf("coordination repair task with patch queue refs must remain selectable, got %+v", targetedRepair)
	}
	staleTarget, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		CoordinationMode: "trust_first",
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  staleTaskID,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get targeted stale work next: %v", err)
	}
	if staleTarget.HasWork || staleTarget.Reason != "trigger_task_superseded" || staleTarget.Packet == nil || staleTarget.Packet.WorkType != "trigger_task_superseded" {
		t.Fatalf("targeted stale revision should report explicit superseded diagnostic, got %+v", staleTarget)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           repairTaskID,
		AgentID:          leadID,
		CoordinationMode: "trust_first",
		Summary:          "claim repair task after filtering superseded dependency",
	}); err != nil {
		t.Fatalf("expected superseded dependency not to block claim admission: %v", err)
	}
}

func TestGetAgentWorkNextBlocksGenericScaffoldAndOverlappingImprovementAfterAcceptedCandidate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-stale-scaffold-after-accepted"
		projectID   = "project-stale-scaffold-after-accepted"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "zeta"
		repoID      = "repo-main"
		scaffoldID  = "task-scaffold-ui-shell"
		improveID   = "task-improve-export-ux"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               builderID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign implementer role: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               reviewerID,
		RoleType:              sqlite.ProjectRoleIntegrator,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign integrator role: %v", err)
	}
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	createImplementationTask := func(id, title string) {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:          workspaceID,
			TaskID:               id,
			OwnerUserID:          "developer",
			Priority:             "high",
			Title:                title,
			Description:          "Create the initial upload -> convert -> preview -> export shell with tests placeholders. Reject drift toward manual cell-art or editor-style product shape.",
			TaskKind:             "EXECUTION",
			TaskTemplate:         "integration",
			Tags:                 []string{"project", "implementation"},
			ProjectID:            projectID,
			ProjectLane:          "implementation",
			RequiresProjectGate:  false,
			TaskRequirementsJSON: `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`,
			WriteScopeHints:      []string{"src/**", "package.json", "index.html"},
		}, graph); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      id,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach %s: %v", id, err)
		}
	}
	createImplementationTask(scaffoldID, "Scaffold the canonical web app and spec-faithful UI shell")
	accepted := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-accepted-product", builderID, reviewerID, `{"paths":["src/**","package.json","index.html"]}`, sqlite.ProjectPatchQueueStateAccepted)
	createImplementationTask(improveID, "Scaffold export settings panel around accepted image processor")

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get builder work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_claim_scope_busy" {
		taskDetail := "<nil>"
		if result.Task != nil {
			taskDetail = fmt.Sprintf("task_id=%s title=%q lane=%s template=%s requires_gate=%v owner=%s",
				result.Task.TaskID,
				result.Task.Title,
				result.Task.ProjectLane,
				result.Task.TaskTemplate,
				result.Task.RequiresProjectGate,
				result.Task.OwnerUserID,
			)
		}
		t.Fatalf("expected accepted candidate scope to keep overlapping improvement unavailable until integration, got result=%+v selected=%s", result, taskDetail)
	}
	integratorResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get integrator work next: %v", err)
	}
	wantIntegrationTaskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(projectID, accepted, "integration")
	if !integratorResult.HasWork || integratorResult.Task == nil || integratorResult.Task.TaskID != wantIntegrationTaskID {
		taskDetail := "<nil>"
		if integratorResult.Task != nil {
			taskDetail = fmt.Sprintf("task_id=%s title=%q lane=%s template=%s requires_gate=%v owner=%s",
				integratorResult.Task.TaskID,
				integratorResult.Task.Title,
				integratorResult.Task.ProjectLane,
				integratorResult.Task.TaskTemplate,
				integratorResult.Task.RequiresProjectGate,
				integratorResult.Task.OwnerUserID,
			)
		}
		t.Fatalf("expected integrator to receive accepted candidate integration continuation %s, got result=%+v selected=%s", wantIntegrationTaskID, integratorResult, taskDetail)
	}
}

func TestClaimTaskRejectsGenericScaffoldAndOverlappingImprovementAfterAcceptedCandidate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-stale-scaffold-after-accepted"
		projectID   = "project-claim-stale-scaffold-after-accepted"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "zeta"
		repoID      = "repo-main"
		scaffoldID  = "task-claim-scaffold-ui-shell"
		improveID   = "task-claim-improve-export-ux"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               builderID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign implementer role: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               reviewerID,
		RoleType:              sqlite.ProjectRoleIntegrator,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign integrator role: %v", err)
	}
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	createImplementationTask := func(id, title string) {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:          workspaceID,
			TaskID:               id,
			OwnerUserID:          "developer",
			Priority:             "high",
			Title:                title,
			Description:          "Create the initial upload -> convert -> preview -> export shell with tests placeholders. Reject drift toward manual cell-art or editor-style product shape.",
			TaskKind:             "EXECUTION",
			TaskTemplate:         "integration",
			Tags:                 []string{"project", "implementation"},
			ProjectID:            projectID,
			ProjectLane:          "implementation",
			RequiresProjectGate:  false,
			TaskRequirementsJSON: `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`,
			WriteScopeHints:      []string{"src/**", "package.json", "index.html"},
		}, graph); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      id,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach %s: %v", id, err)
		}
	}
	createImplementationTask(scaffoldID, "Scaffold the canonical web app and spec-faithful UI shell")
	_ = createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-accepted-product-claim", builderID, reviewerID, `{"paths":["src/**","package.json","index.html"]}`, sqlite.ProjectPatchQueueStateAccepted)
	createImplementationTask(improveID, "Scaffold export settings panel around accepted image processor")

	err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           scaffoldID,
		AgentID:          builderID,
		CoordinationMode: "trust_first",
		Summary:          "resume stale scaffold task",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("expected stale scaffold claim to be rejected as superseded, got %v", err)
	}
	improveCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, builderID, `C:\fixtures\agents\beta\post-accepted-improvement`)
	improveBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            improveCheckout.CheckoutID,
		AgentID:               builderID,
		BranchID:              "branch-post-accepted-improvement",
		BranchName:            "agent/beta/post-accepted-improvement",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["src/export/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               builderID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", builderID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register improvement branch: %v", err)
	}
	err = store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           improveID,
		AgentID:          builderID,
		RepoID:           repoID,
		CheckoutID:       improveCheckout.CheckoutID,
		BranchID:         improveBranch.BranchID,
		WriteScopeJSON:   `{"paths":["src/export/**"]}`,
		CoordinationMode: "trust_first",
		Summary:          "improve accepted product export UX",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) ||
		!(strings.Contains(err.Error(), "overlaps live branch") ||
			(strings.Contains(err.Error(), "overlaps active claim") && strings.Contains(err.Error(), "branch_id=branch-accepted-product-claim"))) {
		t.Fatalf("expected post-accepted overlapping improvement claim to remain blocked until integration, got %v", err)
	}
}

func TestGetAgentWorkNextRoutesReleasedReadyBranchOwnerToPatchQueueSubmitHandoff(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-agent-branch-owner-ready-reclaim"
		projectID     = "project-branch-owner-ready-reclaim"
		leadID        = "alpha"
		branchOwnerID = "beta"
		otherBuilder  = "delta"
		repoID        = "repo-main"
		taskID        = "task-ready-branch-reclaim"
		branchID      = "branch-ready-reclaim"
		reviewKey     = "project.project-branch-owner-ready-reclaim.branch.branch-ready-reclaim.review"
		scopeJSON     = `{"paths":["src/editor/**"]}`
	)
	baseSHA := strings.Repeat("c", 40)
	headSHA := strings.Repeat("d", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, otherBuilder})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	designDocID := "doc.ready-branch-reclaim.design"
	implementationPlanDocID := "doc.ready-branch-reclaim.plan"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 leadID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("open project gates: %v", err)
	}
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "Design accepted; implementation lane may start.",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase: %v", err)
	}
	for _, agentID := range []string{branchOwnerID, otherBuilder} {
		if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			AgentID:               agentID,
			RoleType:              sqlite.ProjectRoleImplementer,
			WriteScopeJSON:        scopeJSON,
			ActorID:               leadID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
			PromptContextSurface:  "project.role.assign",
		}); err != nil {
			t.Fatalf("assign implementer role to %s: %v", agentID, err)
		}
	}
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, `C:\fixtures\agents\beta\ready-branch-reclaim`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchOwnerID,
		BranchID:              branchID,
		BranchName:            "agent/beta/ready-branch-reclaim",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register reserved branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               branchOwnerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        scopeJSON,
		Summary:               "implement editor slice",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, branchOwnerID),
	}); err != nil {
		t.Fatalf("claim task with branch admission: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nready branch evidence",
		UpdatedBy:   branchOwnerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchOwnerID,
		BranchID:              branch.BranchID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/beta/ready-branch-reclaim",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        scopeJSON,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register ready branch: %v", err)
	}
	if _, err := store.ReleaseTaskClaimWithEvent(ctx, sqlite.TaskReleaseInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               branchOwnerID,
		Reason:                "structured output parse failed after ready branch publication",
		PromptContextEnvelope: taskReleasePromptEnvelopeForGitTest(workspaceID, taskID, branchOwnerID),
	}); err != nil {
		t.Fatalf("release task after ready branch publication: %v", err)
	}

	otherResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       otherBuilder,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get other builder work next: %v", err)
	}
	if otherResult.HasWork || otherResult.Reason != "project_claim_scope_busy" {
		t.Fatalf("expected non-owner builder to be blocked by ready branch scope, got %+v", otherResult)
	}
	if otherResult.Packet == nil ||
		otherResult.Packet.PreferredTransition != "delegate_to_branch_owner" ||
		otherResult.Packet.Gate == nil ||
		otherResult.Packet.Gate.NeededFrom != branchOwnerID ||
		otherResult.Packet.HandoffToAgentID != branchOwnerID ||
		otherResult.Packet.Handoff == nil ||
		otherResult.Packet.Handoff.ToAgentID != branchOwnerID {
		t.Fatalf("expected busy packet to hand off to branch owner, got %+v", otherResult.Packet)
	}
	if len(otherResult.Packet.ContextHints.AnchorTaskIDs) != 1 || otherResult.Packet.ContextHints.AnchorTaskIDs[0] != taskID {
		t.Fatalf("expected busy packet to anchor blocked task, got %+v", otherResult.Packet.ContextHints)
	}
	if len(otherResult.Packet.ContextHints.AnchorBranchIDs) != 1 || otherResult.Packet.ContextHints.AnchorBranchIDs[0] != branchID {
		t.Fatalf("expected busy packet to anchor conflict branch, got %+v", otherResult.Packet.ContextHints)
	}

	ownerResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       branchOwnerID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get branch owner work next: %v", err)
	}
	if ownerResult.HasWork || ownerResult.Task != nil || ownerResult.Reason != "project_patch_queue_submit_handoff_available" {
		t.Fatalf("expected branch owner to submit ready branch to patch queue before reclaiming released task, got %+v", ownerResult)
	}
	if ownerResult.Packet == nil || ownerResult.Packet.OwnerBound == nil ||
		ownerResult.Packet.OwnerBound.BranchID != branchID ||
		ownerResult.Packet.OwnerBound.RequiredAgentID != branchOwnerID {
		t.Fatalf("expected owner-bound patch queue submit packet for branch owner, got %+v", ownerResult.Packet)
	}
}

func TestGetAgentWorkNextBlocksSameAgentTaskWhenBranchActiveOnAnotherTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-agent-same-branch-active"
		projectID     = "project-same-branch-active"
		leadID        = "alpha"
		branchOwnerID = "gamma"
		reviewerID    = "epsilon"
		repoID        = "repo-main"
		activeTaskID  = "task-patchq-revision-active"
		staleTaskID   = "task-original-stale"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	designDocID := "doc.same-branch-active.design"
	implementationPlanDocID := "doc.same-branch-active.plan"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 leadID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("open project gates: %v", err)
	}
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "implementation lane may start",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               branchOwnerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign implementer role: %v", err)
	}
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
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, activeTaskID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, `C:\fixtures\agents\gamma\same-branch-active`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchOwnerID,
		BranchID:              "branch-same-agent-active",
		BranchName:            "agent/gamma/same-branch-active",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               branchOwnerID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["**"]}`,
		Summary:               "claim revision branch",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, activeTaskID, branchOwnerID),
	}); err != nil {
		t.Fatalf("claim active branch task: %v", err)
	}
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, staleTaskID)

	reviewKey := "project." + projectID + ".branch." + branch.BranchID + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Same-Agent Active Branch Review Packet",
		Content:     "# Review Packet\n\nReady for patch queue review.",
		UpdatedBy:   branchOwnerID,
	}); err != nil {
		t.Fatalf("write active branch review doc: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchOwnerID,
		ActiveTaskID:          activeTaskID,
		ActiveClaimID:         activeTaskID,
		BranchID:              branch.BranchID,
		BranchName:            branch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["**"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("mark active branch ready for patch queue: %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   activeTaskID,
		SessionID:                "session-same-agent-active",
		RunID:                    "run-same-agent-active",
		AgentID:                  branchOwnerID,
		CapabilitySnapshotID:     "cap-same-agent-active",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{"src/app.go": "sha256:src"},
		RepoLeaseID:              "lease-same-agent-active",
		LeaseTerm:                7,
		ActorID:                  branchOwnerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit active branch patch queue item: %v", err)
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
		t.Fatalf("claim active branch patch queue item: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Needs an owner revision before integration.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block active branch patch queue item: %v", err)
	}
	if _, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               branchOwnerID,
		Reason:                "revision blocked pending review evidence",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.block", workspaceID, activeTaskID, branchOwnerID),
	}); err != nil {
		t.Fatalf("block active branch task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       branchOwnerID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get branch owner work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_claim_scope_busy" {
		t.Fatalf("expected same-agent active branch to block stale task selection, got %+v", result)
	}
	if result.Packet == nil || result.Packet.Gate == nil || !strings.Contains(result.Packet.Gate.Summary, "branch-same-agent-active") || !strings.Contains(result.Packet.Gate.Summary, activeTaskID) {
		t.Fatalf("expected busy packet to identify same-agent active branch, got %+v", result.Packet)
	}
	if result.Packet.PreferredTransition != "delegate_to_branch_owner" ||
		result.Packet.Gate.NeededFrom != branchOwnerID ||
		result.Packet.HandoffToAgentID != branchOwnerID ||
		result.Packet.Handoff == nil ||
		result.Packet.Handoff.ToAgentID != branchOwnerID {
		t.Fatalf("expected busy packet to ask branch owner to finish active lane, got %+v", result.Packet)
	}
	// The BLOCKED patch-queue decision on the active branch eagerly materializes the owner's revision continuation
	// (gamma holds a claimable IMPLEMENTER role). Work-next now selects that higher-priority revision follow-up as
	// the candidate over the unrelated stale task, and the same-branch busy-gate blocks it - so the busy packet
	// anchors the continuation (the relevant blocked revision work for this branch), not the stale original task.
	revisionContinuationID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(projectID, item, "revision")
	if len(result.Packet.ContextHints.AnchorTaskIDs) != 1 || result.Packet.ContextHints.AnchorTaskIDs[0] != revisionContinuationID {
		t.Fatalf("expected busy packet to anchor the blocked revision continuation, got %+v", result.Packet.ContextHints)
	}
	if len(result.Packet.ContextHints.AnchorBranchIDs) != 1 || result.Packet.ContextHints.AnchorBranchIDs[0] != "branch-same-agent-active" {
		t.Fatalf("expected busy packet to anchor conflict branch, got %+v", result.Packet.ContextHints)
	}

	triggeredResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		IncludePacket:    true,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  staleTaskID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get triggered stale branch work next: %v", err)
	}
	if triggeredResult.HasWork || triggeredResult.Reason != "project_claim_scope_busy" || triggeredResult.Trigger != "runtime_switch_task" {
		t.Fatalf("triggered runtime_switch_task must not bypass project_claim_scope_busy, got %+v", triggeredResult)
	}
	if triggeredResult.Packet == nil || triggeredResult.Packet.Gate == nil || triggeredResult.Packet.Gate.NeededFrom != branchOwnerID {
		t.Fatalf("expected triggered busy packet to route to branch owner, got %+v", triggeredResult.Packet)
	}

	followupTaskID := "task-patchq-revision-followup"
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              followupTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Unblock integration candidate branch-same-agent-active",
		Description:         "- queue_id: patchq-same-agent\n- item_id: patchitem-same-agent\n- branch_id: branch-same-agent-active\n- state: BLOCKED",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "integration",
		Tags:                []string{"project", "patch-queue", "revision", "blocked"},
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"**"},
	}, graph); err != nil {
		t.Fatalf("create revision follow-up task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      followupTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach revision follow-up task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                followupTaskID,
		AgentID:               branchOwnerID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "bogus patch queue identity must not authorize active branch rebind",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, followupTaskID, branchOwnerID),
	}); err == nil || !strings.Contains(err.Error(), "active on task") {
		t.Fatalf("expected bogus queue/item active-branch rebind to be rejected, got %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE tasks SET description = ?, tags_json = ? WHERE task_id = ?`,
		"- queue_id: "+item.QueueID+"\n- item_id: "+item.ItemID+"\n- branch_id: "+branch.BranchID+"\n- head_sha: "+headSHA+"\n- state: BLOCKED",
		`["project","patch-queue","revision","blocked"]`,
		followupTaskID,
	); err != nil {
		t.Fatalf("write exact revision follow-up identity: %v", err)
	}
	followupResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       branchOwnerID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get branch owner revision follow-up work next: %v", err)
	}
	if !followupResult.HasWork || followupResult.Task == nil || followupResult.Task.TaskID != followupTaskID {
		t.Fatalf("expected branch owner to receive explicit patch-queue revision follow-up, got %+v", followupResult)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                followupTaskID,
		AgentID:               branchOwnerID,
		BranchID:              branch.BranchID,
		CheckoutID:            checkout.CheckoutID,
		WriteScopeJSON:        `{"paths":["**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "exact blocked patch queue identity authorizes owner active branch rebind",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, followupTaskID, branchOwnerID),
	}); err != nil {
		t.Fatalf("expected exact queue/item/head revision follow-up to rebind active branch: %v", err)
	}
}

func TestGetAgentWorkNextTrustFirstNoRoleUsesTaskWriteScopeForBusyGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID     = "ws-trust-first-no-role-scope-busy"
		projectID       = "project-no-role-scope-busy"
		leadID          = "alpha"
		activeAgentID   = "delta"
		blockedAgentID  = "gamma"
		repoID          = "repo-main"
		activeTaskID    = "task-risk-timeline"
		blockedTaskID   = "task-workboard"
		branchID        = "branch-delta-risk-timeline"
		activeScopeJSON = `{"paths":["src/components/risks/**","src/components/timeline/**","src/components/shared/**","src/styles/**","src/data/**","src/types/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, activeAgentID, blockedAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, spec := range []struct {
		taskID          string
		title           string
		writeScopeHints []string
	}{
		{
			taskID:          activeTaskID,
			title:           "Build risk register and timeline",
			writeScopeHints: []string{"src/components/risks/**", "src/components/timeline/**", "src/components/shared/**", "src/styles/**", "src/data/**", "src/types/**"},
		},
		{
			taskID:          blockedTaskID,
			title:           "Build workboard state and filters",
			writeScopeHints: []string{"src/components/workboard/**", "src/components/filters/**", "src/hooks/**", "src/state/**", "src/data/**", "src/types/**"},
		},
	} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:         workspaceID,
			TaskID:              spec.taskID,
			OwnerUserID:         "developer",
			Priority:            "high",
			Title:               spec.title,
			TaskKind:            "EXECUTION",
			TaskTemplate:        "generic",
			ProjectID:           projectID,
			ProjectLane:         "implementation",
			RequiresProjectGate: true,
			WriteScopeHints:     spec.writeScopeHints,
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", spec.taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      spec.taskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", spec.taskID, err)
		}
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, activeAgentID, `C:\fixtures\agents\delta\no-role-scope-busy`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branchID,
		BranchName:            "agent/delta/risk-timeline",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        activeScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               activeAgentID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        activeScopeJSON,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim risk and timeline lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, activeTaskID, activeAgentID),
	}); err != nil {
		t.Fatalf("claim active task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          blockedAgentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_claim_scope_busy" {
		t.Fatalf("expected trust-first no-role candidate to be blocked by task-local write scope, got %+v", result)
	}
	if result.Task != nil {
		t.Fatalf("busy no-work packet must not carry a runnable task, got %+v", result.Task)
	}
	if result.ProjectID != projectID || result.TaskKind != "EXECUTION" || result.ProjectLane != "implementation" || !result.RequiresProjectGate {
		t.Fatalf("busy result should retain blocked task project digest, got project=%q kind=%q lane=%q gate=%v", result.ProjectID, result.TaskKind, result.ProjectLane, result.RequiresProjectGate)
	}
	if result.Packet == nil || result.Packet.ProjectID != projectID || result.Packet.TaskKind != "EXECUTION" || result.Packet.ProjectLane != "implementation" {
		t.Fatalf("busy packet should retain blocked task project digest, got %+v", result.Packet)
	}
	if result.Packet == nil || result.Packet.Gate == nil || !strings.Contains(result.Packet.Gate.Summary, activeTaskID) {
		t.Fatalf("expected busy packet to identify active claim conflict, got %+v", result.Packet)
	}
	if len(result.Packet.ContextHints.AnchorTaskIDs) != 1 || result.Packet.ContextHints.AnchorTaskIDs[0] != blockedTaskID {
		t.Fatalf("expected busy packet to anchor blocked task, got %+v", result.Packet.ContextHints)
	}
	if len(result.Packet.ContextHints.AnchorBranchIDs) != 1 || result.Packet.ContextHints.AnchorBranchIDs[0] != branchID {
		t.Fatalf("expected busy packet to anchor conflict branch, got %+v", result.Packet.ContextHints)
	}
}

func TestGetAgentWorkNextReservedIntegrationBranchDoesNotBlockImplementationFanout(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID       = "ws-reserved-integration-branch-fanout"
		projectID         = "project-reserved-integration-branch-fanout"
		leadID            = "alpha"
		integratorID      = "zeta"
		parserAgentID     = "gamma"
		repoID            = "repo-main"
		integrationTaskID = "task-integrate-rq-lanes"
		parserTaskID      = "task-rq-parser"
		integrationBranch = "branch-zeta-integration"
		parserBranchID    = "branch-gamma-parser"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, integratorID, parserAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              integrationTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Integrate rq lanes, finalize README, and publish review-ready evidence",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "integration",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"README.md", "cmd/**", "internal/**", "testdata/**", "go.mod", "go.sum"},
	}, graph); err != nil {
		t.Fatalf("create integration task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: integrationTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach integration task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              parserTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Implement rq parser and AST with spec precedence",
		Description:         "Parse rq tokens into AST nodes. Do not implement evaluator semantics in this lane.",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"internal/parser/**", "internal/ast/**", "*_test.go", "testdata/**", "go.mod"},
	}, graph); err != nil {
		t.Fatalf("create parser task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: parserTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach parser task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      integrationTaskID,
		AgentID:     integratorID,
		Summary:     "watch integration lane before candidates are ready",
	}); err != nil {
		t.Fatalf("claim integration task: %v", err)
	}
	zetaCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, integratorID, `C:\fixtures\agents\zeta\integration`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            zetaCheckout.CheckoutID,
		AgentID:               integratorID,
		ActiveTaskID:          integrationTaskID,
		ActiveClaimID:         integrationTaskID,
		BranchID:              integrationBranch,
		BranchName:            "agent/zeta/integration",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["README.md","cmd/**","internal/**","testdata/**","go.mod","go.sum"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register reserved integration branch: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          parserAgentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get parser work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != parserTaskID {
		t.Fatalf("reserved integration branch should not hide parser task, got %+v", result)
	}
	if result.Reason == "project_claim_scope_busy" {
		t.Fatalf("reserved integration branch should not make parser task scope-busy: %+v", result.Packet)
	}

	parserCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, parserAgentID, `C:\fixtures\agents\gamma\parser`)
	parserBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		AgentID:               parserAgentID,
		BranchID:              parserBranchID,
		BranchName:            "agent/gamma/parser",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["internal/parser/**","internal/ast/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               parserAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", parserAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register parser branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                parserTaskID,
		AgentID:               parserAgentID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		BranchID:              parserBranch.BranchID,
		WriteScopeJSON:        `{"paths":["internal/parser/**","internal/ast/**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim parser lane despite reserved integration branch",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, parserTaskID, parserAgentID),
	}); err != nil {
		t.Fatalf("reserved integration branch should not reject parser claim: %v", err)
	}
}

func TestClaimTaskMissingReservedBranchTaskStillBlocksImplementationScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-missing-reserved-branch-task-blocks"
		projectID      = "project-missing-reserved-branch-task-blocks"
		leadID         = "alpha"
		staleAgentID   = "zeta"
		parserAgentID  = "gamma"
		repoID         = "repo-main"
		parserTaskID   = "task-rq-parser"
		parserBranchID = "branch-gamma-parser"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, staleAgentID, parserAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              parserTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Implement rq parser and AST with spec precedence",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"internal/parser/**", "internal/ast/**"},
	}, graph); err != nil {
		t.Fatalf("create parser task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: parserTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach parser task: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, staleAgentID, `C:\fixtures\agents\zeta\stale`)
	now := "2026-05-31T19:19:00Z"
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO project_branch_registry(
  branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id,
  active_task_id, active_claim_id, branch_name, branch_kind, base_branch,
  write_scope_json, status, updated_by, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"branch-zeta-missing-task",
		workspaceID,
		projectID,
		repoID,
		checkout.CheckoutID,
		staleAgentID,
		"task-missing-integration",
		"task-missing-integration",
		"agent/zeta/missing-task",
		sqlite.ProjectBranchKindFeature,
		"main",
		`{"paths":["internal/**"]}`,
		sqlite.ProjectBranchStatusReserved,
		staleAgentID,
		now,
		now,
	); err != nil {
		t.Fatalf("insert stale reserved branch: %v", err)
	}
	parserCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, parserAgentID, `C:\fixtures\agents\gamma\parser`)
	parserBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		AgentID:               parserAgentID,
		BranchID:              parserBranchID,
		BranchName:            "agent/gamma/parser",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["internal/parser/**","internal/ast/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               parserAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", parserAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register parser branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                parserTaskID,
		AgentID:               parserAgentID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		BranchID:              parserBranch.BranchID,
		WriteScopeJSON:        `{"paths":["internal/parser/**","internal/ast/**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim parser lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, parserTaskID, parserAgentID),
	}); err == nil || !strings.Contains(err.Error(), "overlaps live branch") {
		t.Fatalf("missing active task row should fail closed and keep reserved branch scope exclusive, got %v", err)
	}
}

func TestClaimTaskReservedIntegrationBranchWithEvidenceStillBlocksImplementationScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID       = "ws-evidence-reserved-integration-branch-blocks"
		projectID         = "project-evidence-reserved-integration-branch-blocks"
		leadID            = "alpha"
		integratorID      = "zeta"
		parserAgentID     = "gamma"
		repoID            = "repo-main"
		integrationTaskID = "task-integrate-evidence"
		parserTaskID      = "task-rq-parser-evidence"
		parserBranchID    = "branch-gamma-parser-evidence"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, integratorID, parserAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              integrationTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Integrate rq lanes with evidence",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "integration",
		RequiresProjectGate: false,
		WriteScopeHints:     []string{"README.md", "cmd/**", "internal/**", "testdata/**", "go.mod", "go.sum"},
	}, graph); err != nil {
		t.Fatalf("create integration task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: integrationTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach integration task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              parserTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Implement rq parser and AST with spec precedence",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"internal/parser/**", "internal/ast/**"},
	}, graph); err != nil {
		t.Fatalf("create parser task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: parserTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach parser task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      integrationTaskID,
		AgentID:     integratorID,
		Summary:     "reserve integration lane with evidence",
	}); err != nil {
		t.Fatalf("claim integration task: %v", err)
	}
	zetaCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, integratorID, `C:\fixtures\agents\zeta\evidence-integration`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            zetaCheckout.CheckoutID,
		AgentID:               integratorID,
		ActiveTaskID:          integrationTaskID,
		ActiveClaimID:         integrationTaskID,
		BranchID:              "branch-zeta-integration-evidence",
		BranchName:            "agent/zeta/integration-evidence",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		HeadSHA:               strings.Repeat("a", 40),
		ReviewDocKey:          "project." + projectID + ".branch.branch-zeta-integration-evidence.review",
		WriteScopeJSON:        `{"paths":["README.md","cmd/**","internal/**","testdata/**","go.mod","go.sum"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register evidence-bearing integration branch: %v", err)
	}
	parserCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, parserAgentID, `C:\fixtures\agents\gamma\parser-evidence`)
	parserBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		AgentID:               parserAgentID,
		BranchID:              parserBranchID,
		BranchName:            "agent/gamma/parser-evidence",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["internal/parser/**","internal/ast/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               parserAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", parserAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register parser branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                parserTaskID,
		AgentID:               parserAgentID,
		RepoID:                repoID,
		CheckoutID:            parserCheckout.CheckoutID,
		BranchID:              parserBranch.BranchID,
		WriteScopeJSON:        `{"paths":["internal/parser/**","internal/ast/**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim parser lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, parserTaskID, parserAgentID),
	}); err == nil || !strings.Contains(err.Error(), "overlaps live branch") {
		t.Fatalf("evidence-bearing reserved integration branch must still block overlapping implementation claim, got %v", err)
	}
}

func TestAssignProjectRoleReservedIntegrationBranchScopeDependsOnEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID       = "ws-reserved-integration-role-scope"
		projectID         = "project-reserved-integration-role-scope"
		leadID            = "alpha"
		integratorID      = "zeta"
		parserAgentID     = "gamma"
		evaluatorAgentID  = "delta"
		repoID            = "repo-main"
		integrationTaskID = "task-integrate-role-scope"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, integratorID, parserAgentID, evaluatorAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              integrationTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Integrate rq lanes for role scope",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "integration",
		RequiresProjectGate: false,
		WriteScopeHints:     []string{"README.md", "cmd/**", "internal/**", "testdata/**", "go.mod", "go.sum"},
	}, graph); err != nil {
		t.Fatalf("create integration task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: integrationTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach integration task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      integrationTaskID,
		AgentID:     integratorID,
		Summary:     "reserve integration branch",
	}); err != nil {
		t.Fatalf("claim integration task: %v", err)
	}
	zetaCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, integratorID, `C:\fixtures\agents\zeta\role-scope`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            zetaCheckout.CheckoutID,
		AgentID:               integratorID,
		ActiveTaskID:          integrationTaskID,
		ActiveClaimID:         integrationTaskID,
		BranchID:              "branch-zeta-role-scope",
		BranchName:            "agent/zeta/role-scope",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["README.md","cmd/**","internal/**","testdata/**","go.mod","go.sum"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register reserved integration branch: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               parserAgentID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["internal/parser/**","internal/ast/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("reserved non-evidence integration branch should not block implementer role assignment: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE project_branch_registry SET head_sha = ?, review_doc_key = ?, updated_at = ? WHERE workspace_id = ? AND branch_id = ?`,
		strings.Repeat("b", 40),
		"project."+projectID+".branch."+branch.BranchID+".review",
		"2026-05-31T19:20:00Z",
		workspaceID,
		branch.BranchID,
	); err != nil {
		t.Fatalf("mark integration branch evidence-bearing: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               evaluatorAgentID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["internal/evaluator/**","internal/runtime/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err == nil || !strings.Contains(err.Error(), "write_scope_json overlaps live branch") {
		t.Fatalf("evidence-bearing reserved integration branch must block overlapping implementer role assignment, got %v", err)
	}
}

func TestGetAgentWorkNextTrustFirstRevisionUsesNarrowRoleScopeForBusyGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID     = "ws-trust-first-revision-role-scope"
		projectID       = "project-revision-role-scope"
		leadID          = "alpha"
		activeAgentID   = "delta"
		blockedAgentID  = "beta"
		repoID          = "repo-main"
		activeTaskID    = "task-data-model"
		blockedTaskID   = "task-revise-blocked-candidate"
		branchID        = "branch-delta-data"
		activeScopeJSON = `{"paths":["src/data/**","src/types/**","src/lib/**"]}`
		uiScopeJSON     = `{"paths":["package*.json","vite.config.*","tsconfig*.json","index.html","public/**","src/main.*","src/App.*","src/components/**","src/styles/**","src/ui/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, activeAgentID, blockedAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               blockedAgentID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        uiScopeJSON,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign narrowed implementer role: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              activeTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Define seeded signal model and atlas content",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"src/data/**", "src/types/**", "src/lib/**"},
	}, graph); err != nil {
		t.Fatalf("create active task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: activeTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach active task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              blockedTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Revise blocked candidate branch-delta-data into a runnable frontend",
		Description:         "Patch queue follow-up.\n\n- queue_id: patchq-1\n- item_id: patchitem-1\n- branch_id: " + branchID + "\n- head_sha: head-delta\n- state: BLOCKED\n- candidate_pathset: src/**, package.json, index.html\n",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "revision", "frontend", "validation-followup"},
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"package*.json", "vite.config.*", "tsconfig*.json", "index.html", "public/**", "src/main.*", "src/App.*", "src/components/**", "src/styles/**", "src/ui/**"},
	}, graph); err != nil {
		t.Fatalf("create blocked revision task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: blockedTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach blocked revision task: %v", err)
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, activeAgentID, `C:\fixtures\agents\delta\revision-role-scope`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branchID,
		BranchName:            "agent/delta/data-model",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		HeadSHA:               "head-delta",
		ReviewDocKey:          "project." + projectID + ".branch." + branchID + ".review",
		WriteScopeJSON:        activeScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               activeAgentID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        activeScopeJSON,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim data-model lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, activeTaskID, activeAgentID),
	}); err != nil {
		t.Fatalf("claim active task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          blockedAgentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != blockedTaskID {
		t.Fatalf("expected narrowed role scope to keep revision task claimable, got %+v", result)
	}
	if result.Reason == "project_claim_scope_busy" {
		t.Fatalf("broad candidate pathset should not block when repaired role scope is non-overlapping: %+v", result.Packet)
	}
}

func TestGetAgentWorkNextBlocksRevisionBehindSameOwnerReviewReadyBranchScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID       = "ws-revision-review-ready-branch-scope-busy"
		projectID         = "project-revision-review-ready-branch-scope-busy"
		leadID            = "alpha"
		branchOwnerID     = "beta"
		reviewerID        = "kappa"
		repoID            = "repo-main"
		blockedTaskID     = "task-revise-old-beta-candidate"
		oldBranchID       = "branch-old-beta-candidate"
		oldBranchHead     = "2222222222222222222222222222222222222222"
		reviewReadyBranch = "branch-new-beta-editor"
		reviewReadyHead   = "1111111111111111111111111111111111111111"
		reviewReadyBase   = "0000000000000000000000000000000000000000"
		roleScopeJSON     = `{"paths":["src/**","tests/**","package.json","package-lock.json","tsconfig*.json","vite.config.*"]}`
		oldScopeJSON      = `{"paths":["src/**","tests/**","package.json"]}`
		editorScopeJSON   = `{"paths":["src/editor/**","src/lib/editor/**","tests/editor/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, roleScopeJSON)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              blockedTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Unblock integration candidate " + oldBranchID,
		Description:         "Patch queue follow-up.\n\n- queue_id: patchq-1\n- item_id: patchitem-old\n- branch_id: " + oldBranchID + "\n- head_sha: " + oldBranchHead + "\n- state: BLOCKED\n",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "patch-queue", "revision", "blocked", "owner-bound", "owner-agent:" + branchOwnerID, "owner-branch:" + oldBranchID, "required-agent:" + branchOwnerID},
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"src/**", "tests/**", "package.json"},
	}, graph); err != nil {
		t.Fatalf("create blocked revision task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: blockedTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach blocked revision task: %v", err)
	}

	oldReviewDocKey := "project." + projectID + ".branch." + oldBranchID + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      oldReviewDocKey,
		Title:       "Review packet for " + oldBranchID,
		Content:     "schema: rhizome_branch_review_packet_v1\nbranch_id: " + oldBranchID + "\nhead_sha: " + oldBranchHead + "\n",
		UpdatedBy:   branchOwnerID,
	}); err != nil {
		t.Fatalf("write old review packet doc: %v", err)
	}
	oldCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, `C:\fixtures\agents\beta\old-review-ready-branch-scope`)
	oldSubmitTaskID := "task-submit-old-beta-candidate"
	registerReservedProjectBranchForReadyBindingTest(t, ctx, store, workspaceID, projectID, repoID, oldCheckout.CheckoutID, branchOwnerID, oldBranchID, "agent/beta/old-candidate", oldScopeJSON)
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, oldCheckout.CheckoutID, oldBranchID, oldSubmitTaskID, oldScopeJSON)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            oldCheckout.CheckoutID,
		AgentID:               branchOwnerID,
		BranchID:              oldBranchID,
		ActiveTaskID:          oldSubmitTaskID,
		ActiveClaimID:         oldSubmitTaskID,
		BranchName:            "agent/beta/old-candidate",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		HeadSHA:               oldBranchHead,
		BaseSHA:               reviewReadyBase,
		ReviewDocKey:          oldReviewDocKey,
		WriteScopeJSON:        oldScopeJSON,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register old review-ready branch: %v", err)
	}
	oldItem, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		QueueID:                  "patchq-1",
		ItemID:                   "patchitem-old",
		RepoID:                   repoID,
		BranchID:                 oldBranchID,
		ReviewDocKey:             oldReviewDocKey,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		PathsetJSON:              oldScopeJSON,
		BaseSHA:                  reviewReadyBase,
		HeadSHA:                  oldBranchHead,
		TaskID:                   oldSubmitTaskID,
		SessionID:                "session-old-beta-candidate",
		RunID:                    "run-old-beta-candidate",
		AgentID:                  branchOwnerID,
		CapabilitySnapshotID:     "cap-old-beta-candidate",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 oldCheckout.LocalPath,
		BaseTreeHash:             reviewReadyBase,
		BaseFileHashes:           agentWorkTestBaseFileHashesForScope(oldScopeJSON),
		RepoLeaseID:              "lease-old-beta-candidate",
		LeaseTerm:                7,
		ActorID:                  branchOwnerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit old patch queue item: %v", err)
	}
	claimedOldItem, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		LeaseSeconds:          900,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim old patch queue item: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimedOldItem.QueueID,
		ItemID:                claimedOldItem.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Patch queue decision BLOCKED for " + oldBranchID + ".",
		ClaimToken:            claimedOldItem.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block old patch queue item: %v", err)
	}

	reviewDocKey := "project." + projectID + ".branch." + reviewReadyBranch + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewDocKey,
		Title:       "Review packet for " + reviewReadyBranch,
		Content:     "schema: rhizome_branch_review_packet_v1\nbranch_id: " + reviewReadyBranch + "\nhead_sha: " + reviewReadyHead + "\n",
		UpdatedBy:   branchOwnerID,
	}); err != nil {
		t.Fatalf("write review packet doc: %v", err)
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, branchOwnerID, `C:\fixtures\agents\beta\review-ready-branch-scope`)
	_, reviewReadyTaskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, branchOwnerID, reviewReadyBranch, "agent/beta/editor-refresh", editorScopeJSON)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchOwnerID,
		BranchID:              reviewReadyBranch,
		ActiveTaskID:          reviewReadyTaskID,
		ActiveClaimID:         reviewReadyTaskID,
		BranchName:            "agent/beta/editor-refresh",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		HeadSHA:               reviewReadyHead,
		BaseSHA:               reviewReadyBase,
		ReviewDocKey:          reviewDocKey,
		WriteScopeJSON:        editorScopeJSON,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               branchOwnerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchOwnerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register review-ready branch: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  blockedTaskID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get triggered revision work next: %v", err)
	}
	if result.HasWork || result.Task != nil {
		t.Fatalf("overlapping review-ready branch scope must block revision claim before local admission, got %+v", result)
	}
	if result.Reason != "project_claim_scope_busy" {
		t.Fatalf("reason=%q, want project_claim_scope_busy; packet=%+v", result.Reason, result.Packet)
	}
	if result.Packet == nil || !strings.Contains(result.Packet.WhyNow, reviewReadyBranch) {
		t.Fatalf("expected busy packet to name review-ready branch %s, got %+v", reviewReadyBranch, result.Packet)
	}
}

func TestGetAgentWorkNextAllowsRevisionPastOlderSameOwnerBlockedBranchScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-revision-older-branch-not-busy"
		projectID     = "project-revision-older-branch-not-busy"
		leadID        = "alpha"
		branchOwnerID = "beta"
		reviewerID    = "kappa"
		repoID        = "repo-main"
		revisionTask  = "task-revise-newer-blocked-candidate"
		scopeJSON     = `{"paths":["src/**","tests/**","package.json"]}`
		roleScopeJSON = `{"paths":["src/**","tests/**","package.json","package-lock.json","tsconfig*.json","vite.config.*"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchOwnerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, branchOwnerID, leadID, roleScopeJSON)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	predecessor := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-beta-stale-predecessor", branchOwnerID, reviewerID, scopeJSON, sqlite.ProjectPatchQueueStateBlocked)
	source := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-beta-newer-source", branchOwnerID, reviewerID, scopeJSON, sqlite.ProjectPatchQueueStateBlocked)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET updated_at = CASE item_id WHEN ? THEN ? WHEN ? THEN ? ELSE updated_at END,
       decided_at = CASE item_id WHEN ? THEN ? WHEN ? THEN ? ELSE decided_at END
 WHERE workspace_id = ? AND project_id = ? AND item_id IN (?, ?)`,
		predecessor.ItemID, "2026-05-17T00:00:00Z",
		source.ItemID, "2026-05-17T00:01:00Z",
		predecessor.ItemID, "2026-05-17T00:00:00Z",
		source.ItemID, "2026-05-17T00:01:00Z",
		workspaceID, projectID, predecessor.ItemID, source.ItemID,
	); err != nil {
		t.Fatalf("backdate patch queue items: %v", err)
	}
	releaseProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, predecessor.BranchID, predecessor.TaskID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              revisionTask,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Unblock integration candidate " + source.BranchID,
		Description:         "Patch queue follow-up.\n\n- queue_id: " + source.QueueID + "\n- item_id: " + source.ItemID + "\n- branch_id: " + source.BranchID + "\n- head_sha: " + source.HeadSHA + "\n- state: BLOCKED\n",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "patch-queue", "revision", "blocked", "owner-bound", "owner-agent:" + branchOwnerID, "owner-branch:" + source.BranchID, "required-agent:" + branchOwnerID},
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"src/**", "tests/**", "package.json"},
	}, graph); err != nil {
		t.Fatalf("create revision task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: revisionTask, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach revision task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          branchOwnerID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  revisionTask,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get triggered revision work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != revisionTask {
		t.Fatalf("older same-owner blocked predecessor branch must not hold revision scope, got %+v packet=%+v", result, result.Packet)
	}
}

func TestGetAgentWorkNextTrustFirstRevisionIgnoresBroadRoleScopeForBusyGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID     = "ws-trust-first-revision-broad-role"
		projectID       = "project-revision-broad-role"
		leadID          = "alpha"
		activeAgentID   = "delta"
		blockedAgentID  = "beta"
		repoID          = "repo-main"
		activeTaskID    = "task-data-model-broad-role"
		blockedTaskID   = "task-revise-blocked-candidate-broad-role"
		branchID        = "branch-delta-data-broad-role"
		activeScopeJSON = `{"paths":["src/data/**","src/types/**","src/lib/**"]}`
		broadScopeJSON  = `{"paths":["**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, activeAgentID, blockedAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               blockedAgentID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        broadScopeJSON,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign broad implementer role: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              activeTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Define seeded signal model and atlas content",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"src/data/**", "src/types/**", "src/lib/**"},
	}, graph); err != nil {
		t.Fatalf("create active task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: activeTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach active task: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              blockedTaskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Revise blocked candidate branch-delta-data-broad-role into a runnable frontend shell",
		Description:         "Patch queue follow-up.\n\n- queue_id: patchq-1\n- item_id: patchitem-1\n- branch_id: " + branchID + "\n- head_sha: head-delta\n- state: BLOCKED\n- candidate_pathset: web/**, index.html\n",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "revision", "frontend", "validation-followup"},
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"web/**", "index.html"},
	}, graph); err != nil {
		t.Fatalf("create blocked revision task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: blockedTaskID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach blocked revision task: %v", err)
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, activeAgentID, `C:\fixtures\agents\delta\revision-broad-role`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branchID,
		BranchName:            "agent/delta/data-model-broad-role",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		HeadSHA:               "head-delta",
		ReviewDocKey:          "project." + projectID + ".branch." + branchID + ".review",
		WriteScopeJSON:        activeScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               activeAgentID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        activeScopeJSON,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim data-model lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, activeTaskID, activeAgentID),
	}); err != nil {
		t.Fatalf("claim active task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          blockedAgentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != blockedTaskID {
		t.Fatalf("expected task scope to override broad role scope for revision busy gate, got %+v", result)
	}
	if result.Reason == "project_claim_scope_busy" {
		t.Fatalf("broad role scope should not make a non-overlapping revision task busy: %+v", result.Packet)
	}
}

func TestGetAgentWorkNextKeepsClaimScopeAuthoritativeWhenBranchIsNarrower(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID      = "ws-agent-work-effective-branch-claim-scope"
		projectID        = "project-effective-branch-claim-scope"
		leadID           = "alpha"
		activeAgentID    = "eta"
		candidateAgentID = "beta"
		repoID           = "repo-main"
		activeTaskID     = "task-sector-dashboard"
		candidateTaskID  = "task-app-scaffold"
		branchID         = "branch-eta-sector"
		staleBroadScope  = `{"paths":["src/**","package.json","index.html"]}`
		narrowScope      = `{"paths":["src/components/sector-board/**","src/data/sector.ts"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, activeAgentID, candidateAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, spec := range []struct {
		taskID string
		title  string
		hints  []string
	}{
		{activeTaskID, "Build sector command dashboard", []string{"src/**", "package.json", "index.html"}},
		{candidateTaskID, "Scaffold app shell and toolchain", []string{"package.json", "package-lock.json", "vite.config.ts", "tsconfig.json", "index.html"}},
	} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:         workspaceID,
			TaskID:              spec.taskID,
			OwnerUserID:         "developer",
			Priority:            "high",
			Title:               spec.title,
			TaskKind:            "EXECUTION",
			TaskTemplate:        "generic",
			ProjectID:           projectID,
			ProjectLane:         "implementation",
			RequiresProjectGate: true,
			WriteScopeHints:     spec.hints,
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", spec.taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: spec.taskID, LinkedBy: "developer"}); err != nil {
			t.Fatalf("attach task %s: %v", spec.taskID, err)
		}
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, activeAgentID, `C:\fixtures\agents\eta\sector-dashboard`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branchID,
		BranchName:            "agent/eta/sector-dashboard",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        staleBroadScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register broad branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               activeAgentID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        staleBroadScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim initial broad dashboard lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, activeTaskID, activeAgentID),
	}); err != nil {
		t.Fatalf("claim active task with broad scope: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branch.BranchID,
		ActiveTaskID:          activeTaskID,
		ActiveClaimID:         activeTaskID,
		BranchName:            "agent/eta/sector-dashboard",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        narrowScope,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchScopeMismatch) || !strings.Contains(err.Error(), "must match active claim scope") {
		t.Fatalf("expected branch-only scope repair to be rejected until claim is rebound, got %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          candidateAgentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_claim_scope_busy" {
		t.Fatalf("expected persisted broad claim scope to keep overlapping candidate blocked, got %+v", result)
	}
}

func TestGetAgentWorkNextFallsBackToClaimScopeWhenBranchHasNoActiveRefs(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID      = "ws-agent-work-effective-branch-no-refs"
		projectID        = "project-effective-branch-no-refs"
		leadID           = "alpha"
		activeAgentID    = "eta"
		candidateAgentID = "beta"
		repoID           = "repo-main"
		activeTaskID     = "task-sector-dashboard-no-refs"
		candidateTaskID  = "task-app-scaffold-no-refs"
		branchID         = "branch-eta-sector-no-refs"
		staleBroadScope  = `{"paths":["src/**","package.json","index.html"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, activeAgentID, candidateAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, spec := range []struct {
		taskID string
		title  string
		hints  []string
	}{
		{activeTaskID, "Build sector command dashboard", []string{"src/**", "package.json", "index.html"}},
		{candidateTaskID, "Scaffold app shell and toolchain", []string{"package.json", "index.html"}},
	} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:          workspaceID,
			TaskID:               spec.taskID,
			OwnerUserID:          "developer",
			Priority:             "high",
			Title:                spec.title,
			TaskKind:             "EXECUTION",
			TaskTemplate:         "generic",
			ProjectID:            projectID,
			ProjectLane:          "implementation",
			RequiresProjectGate:  true,
			TaskRequirementsJSON: `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`,
			WriteScopeHints:      spec.hints,
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", spec.taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: spec.taskID, LinkedBy: "developer"}); err != nil {
			t.Fatalf("attach task %s: %v", spec.taskID, err)
		}
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, activeAgentID, `C:\fixtures\agents\eta\sector-dashboard-no-refs`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branchID,
		BranchName:            "agent/eta/sector-dashboard-no-refs",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        staleBroadScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register broad branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               activeAgentID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        staleBroadScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim initial broad dashboard lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, activeTaskID, activeAgentID),
	}); err != nil {
		t.Fatalf("claim active task with broad scope: %v", err)
	}
	reviewKey := "project." + projectID + ".branch." + branch.BranchID + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Review\n\nReady.",
		UpdatedBy:   activeAgentID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branch.BranchID,
		ActiveTaskID:          activeTaskID,
		ActiveClaimID:         activeTaskID,
		BranchName:            "agent/eta/sector-dashboard-no-refs",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        staleBroadScope,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("mark branch ready with active scope: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          candidateAgentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_claim_scope_busy" {
		t.Fatalf("expected ready branch to preserve active task binding and keep overlapping candidate blocked, got %+v", result)
	}
}

func TestGetAgentWorkNextTrustFirstRepairRoleNarrowsBroadTaskScopeForBusyGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID      = "ws-agent-work-repair-role-narrows-task"
		projectID        = "project-repair-role-narrows-task"
		leadID           = "alpha"
		activeAgentID    = "eta"
		candidateAgentID = "beta"
		repoID           = "repo-main"
		activeTaskID     = "task-sector-dashboard"
		candidateTaskID  = "task-ui-shell-broad"
		branchID         = "branch-eta-sector-narrow"
		activeScopeJSON  = `{"paths":["src/components/sector-board/**","src/data/sector.ts"]}`
		roleScopeJSON    = `{"paths":["package*.json","vite.config.*","tsconfig*.json","index.html"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, activeAgentID, candidateAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               candidateAgentID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        roleScopeJSON,
		Summary:               "Repair stale broad claim scope by narrowing beta scope ownership to scaffold/config files.",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign repair role: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, spec := range []struct {
		taskID string
		title  string
		hints  []string
	}{
		{activeTaskID, "Build sector command dashboard", []string{"src/components/sector-board/**", "src/data/sector.ts"}},
		{candidateTaskID, "Scaffold app shell with broad initial task hints", []string{"src/**", "tests/**", "package*.json", "vite.config.*", "tsconfig*.json", "index.html"}},
	} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:          workspaceID,
			TaskID:               spec.taskID,
			OwnerUserID:          "developer",
			Priority:             "high",
			Title:                spec.title,
			TaskKind:             "EXECUTION",
			TaskTemplate:         "generic",
			ProjectID:            projectID,
			ProjectLane:          "implementation",
			RequiresProjectGate:  true,
			TaskRequirementsJSON: `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`,
			WriteScopeHints:      spec.hints,
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", spec.taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: spec.taskID, LinkedBy: "developer"}); err != nil {
			t.Fatalf("attach task %s: %v", spec.taskID, err)
		}
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, activeAgentID, `C:\fixtures\agents\eta\sector-dashboard-narrow`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branchID,
		BranchName:            "agent/eta/sector-dashboard-narrow",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        activeScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               activeAgentID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        activeScopeJSON,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim sector board lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, activeTaskID, activeAgentID),
	}); err != nil {
		t.Fatalf("claim active task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          candidateAgentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != candidateTaskID {
		packetWhy := ""
		if result.Packet != nil {
			packetWhy = result.Packet.WhyNow
		}
		t.Fatalf("expected repair role to narrow broad candidate task and keep it claimable, reason=%s packet_why=%q task=%+v", result.Reason, packetWhy, result.Task)
	}
	if result.Reason == "project_claim_scope_busy" {
		t.Fatalf("repair role scope should avoid busy gate from broad task hints: %+v", result.Packet)
	}
}

func TestGetAgentWorkNextTrustFirstBoundaryRoleScopeBlocksBusyPublicationSibling(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID      = "ws-agent-work-boundary-role-busy"
		projectID        = "project-lua-boundary-role-busy"
		leadID           = "alpha"
		activeAgentID    = "delta"
		candidateAgentID = "eta"
		repoID           = "repo-lua"
		activeTaskID     = "task-cli-publication-active"
		candidateTaskID  = "task-publication-side-effect-sibling"
		branchID         = "branch-delta-cli-publication"
		activeScopeJSON  = `{"paths":["cmd/**","internal/cli/**","internal/repl/**"]}`
		roleScopeJSON    = `{"paths":["cmd/glua/**","README.md","internal/runner/**","scripts/**","testdata/smoke/**"]}`
	)
	publicationHints := []string{"cmd/glua/**", "README.md", "internal/runner/**", "scripts/**", "testdata/smoke/**"}
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, activeAgentID, candidateAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               candidateAgentID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        roleScopeJSON,
		Summary:               "Boundary transition side-effect resolution for the CLI publication lane; keep runner/smoke sidecars coupled to cmd/glua ownership.",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign boundary role: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, spec := range []struct {
		taskID      string
		title       string
		description string
		hints       []string
		requirement string
	}{
		{
			taskID:      activeTaskID,
			title:       "CLI publication repair: expand runner and smoke boundary",
			description: "Own cmd/glua, internal/cli, and internal/repl while the publication boundary is being repaired.",
			hints:       []string{"cmd/**", "internal/cli/**", "internal/repl/**"},
			requirement: `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`,
		},
		{
			taskID:      candidateTaskID,
			title:       "Runner boundary follow-up: verify runner boundary and smoke lane",
			description: "Bounded follow-up for the active CLI publication repair. Scope README.md, internal/runner/**, scripts/**, and testdata/smoke/** while the boundary transition role also owns cmd/glua side effects.",
			hints:       publicationHints,
			requirement: `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`,
		},
	} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:          workspaceID,
			TaskID:               spec.taskID,
			OwnerUserID:          "developer",
			Priority:             "high",
			Title:                spec.title,
			Description:          spec.description,
			TaskKind:             "EXECUTION",
			TaskTemplate:         "generic",
			ProjectID:            projectID,
			ProjectLane:          "implementation",
			RequiresProjectGate:  true,
			TaskRequirementsJSON: spec.requirement,
			WriteScopeHints:      spec.hints,
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", spec.taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: spec.taskID, LinkedBy: "developer"}); err != nil {
			t.Fatalf("attach task %s: %v", spec.taskID, err)
		}
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, activeAgentID, `C:\fixtures\agents\delta\lua-cli-publication`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branchID,
		BranchName:            "agent/delta/lua-cli-publication",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        activeScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               activeAgentID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        activeScopeJSON,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim active CLI publication lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, activeTaskID, activeAgentID),
	}); err != nil {
		t.Fatalf("claim active task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          candidateAgentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_claim_scope_busy" {
		t.Fatalf("expected boundary role scope to make publication sibling busy, got reason=%s task=%+v packet=%+v", result.Reason, result.Task, result.Packet)
	}
	if result.Packet == nil || !strings.Contains(result.Packet.WhyNow, activeTaskID) {
		t.Fatalf("expected busy packet to cite active task %s, got %+v", activeTaskID, result.Packet)
	}
}

func TestGetAgentWorkNextTrustFirstPublicationRepairSiblingRequiresOwnerBoundRepair(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		title string
		tags  []string
	}{
		{
			name:  "tagged-publication-repair",
			title: "Repair foundation side-effects split from cmd/glua/main.go",
			tags:  []string{"backend", "publication-repair"},
		},
		{
			name:  "publication-repair-follow-up-title",
			title: "Publication repair follow-up: verify runner boundary and smoke lane",
			tags:  []string{"cli", "publication", "verification", "runner", "smoke"},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			workspaceID := "ws-agent-work-publication-repair-" + tc.name
			projectID := "project-lua-publication-repair-" + tc.name
			const (
				leadID           = "alpha"
				activeAgentAID   = "delta"
				activeAgentBID   = "gamma"
				candidateAgentID = "eta"
				repoID           = "repo-lua"
				candidateTaskID  = "task-publication-repair-sibling"
			)

			seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, activeAgentAID, activeAgentBID, candidateAgentID})
			createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
			claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
			upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

			for _, active := range []struct {
				agentID    string
				branchID   string
				branchName string
				scopeJSON  string
			}{
				{
					agentID:    activeAgentAID,
					branchID:   "branch-delta-cli-publication",
					branchName: "agent/delta/lua-cli-publication",
					scopeJSON:  `{"paths":["cmd/**","internal/cli/**","internal/repl/**"]}`,
				},
				{
					agentID:    activeAgentBID,
					branchID:   "branch-gamma-lexer",
					branchName: "agent/gamma/lua-lexer",
					scopeJSON:  `{"paths":["internal/lexer/**","internal/token/**","internal/tokens/**"]}`,
				},
			} {
				checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, active.agentID, `C:\fixtures\agents\`+active.agentID+`\`+active.branchID)
				if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
					WorkspaceID:           workspaceID,
					ProjectID:             projectID,
					RepoID:                repoID,
					CheckoutID:            checkout.CheckoutID,
					AgentID:               active.agentID,
					BranchID:              active.branchID,
					BranchName:            active.branchName,
					BranchKind:            sqlite.ProjectBranchKindFeature,
					BaseBranch:            "main",
					WriteScopeJSON:        active.scopeJSON,
					Status:                sqlite.ProjectBranchStatusActive,
					ActorID:               active.agentID,
					ActorType:             "agent",
					PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", active.agentID),
					PromptContextSurface:  "project.branch.register",
				}); err != nil {
					t.Fatalf("register active branch %s: %v", active.branchID, err)
				}
			}

			graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
			if err := dag.ValidateGraph(graph); err != nil {
				t.Fatalf("validate graph: %v", err)
			}
			if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
				WorkspaceID:         workspaceID,
				TaskID:              candidateTaskID,
				OwnerUserID:         "developer",
				Priority:            "high",
				Title:               tc.title,
				Description:         "Bounded follow-up for the active CLI publication lane. Scope README.md, internal/runner/**, scripts/**, and testdata/smoke/** without claiming cmd/** directly.",
				TaskKind:            "EXECUTION",
				TaskTemplate:        "generic",
				Tags:                tc.tags,
				ProjectID:           projectID,
				ProjectLane:         "implementation",
				RequiresProjectGate: true,
				WriteScopeHints:     []string{"README.md", "internal/runner/**", "scripts/**", "testdata/smoke/**"},
			}, graph); err != nil {
				t.Fatalf("create publication repair task: %v", err)
			}
			if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: candidateTaskID, LinkedBy: "developer"}); err != nil {
				t.Fatalf("attach publication repair task: %v", err)
			}

			result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
				WorkspaceID:      workspaceID,
				AgentID:          candidateAgentID,
				IncludePacket:    true,
				CoordinationMode: sqlite.CoordinationModeTrustFirst,
			})
			if err != nil {
				t.Fatalf("get trust-first work next: %v", err)
			}
			if result.HasWork || result.Reason != "project_owner_bound_repair_required" {
				t.Fatalf("expected publication repair sibling to require owner-bound repair, got reason=%s task=%+v packet=%+v", result.Reason, result.Task, result.Packet)
			}
			if result.Packet == nil ||
				result.Packet.OwnerBound == nil ||
				result.Packet.OwnerBound.Kind != "active_lane_publication" ||
				!result.Packet.OwnerBound.RepairNeeded ||
				!strings.Contains(result.Packet.OwnerBound.Reason, "multiple open project branches") {
				t.Fatalf("expected ambiguous active-lane publication packet, got %+v", result.Packet)
			}

			err = store.ClaimTask(ctx, sqlite.TaskClaimInput{
				WorkspaceID: workspaceID,
				TaskID:      candidateTaskID,
				AgentID:     candidateAgentID,
				Summary:     "claim publication repair sidecar",
			})
			if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "multiple open project branches") {
				t.Fatalf("expected direct claim to be rejected as ambiguous owner-bound publication repair, got %v", err)
			}
		})
	}
}

func TestGetAgentWorkNextTrustFirstSemanticScopeNarrowsBroadTaskHintsForBusyGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID      = "ws-agent-work-semantic-scope-narrows-broad-task"
		projectID        = "project-clearpress-semantic-scope"
		leadID           = "alpha"
		activeAgentID    = "beta"
		candidateAgentID = "gamma"
		repoID           = "repo-clearpress"
		activeTaskID     = "task-clearpress-auth-profile-articles"
		candidateTaskID  = "task-clearpress-editor-core"
		branchID         = "branch-beta-auth-profile-articles"
		activeScopeJSON  = `{"paths":["src/auth/**","src/profile/**","src/articles/**","tests/auth/**","tests/profile/**","tests/articles/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, activeAgentID, candidateAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, spec := range []struct {
		taskID      string
		title       string
		description string
		lane        string
		hints       []string
	}{
		{
			taskID:      activeTaskID,
			title:       "Implement mock auth, profile, and article management lanes",
			description: "Mock Google sign-in, profile avatar editing, my articles list, drafts, archive, delete, and article search.",
			lane:        "implementation",
			hints:       []string{"src/**", "tests/**", "package.json"},
		},
		{
			taskID:      candidateTaskID,
			title:       "Build editor core with shortcuts, settings, and autosave",
			description: "Implement rich-text editor markdown-like shortcuts, blockquote/divider transforms, quote style settings, auto dash replacement, autosave, and focused editor tests.",
			lane:        "implementation",
			hints:       []string{"src/**", "tests/**", "package.json"},
		},
	} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:         workspaceID,
			TaskID:              spec.taskID,
			OwnerUserID:         "developer",
			Priority:            "high",
			Title:               spec.title,
			Description:         spec.description,
			TaskKind:            "EXECUTION",
			TaskTemplate:        "generic",
			ProjectID:           projectID,
			ProjectLane:         spec.lane,
			RequiresProjectGate: true,
			WriteScopeHints:     spec.hints,
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", spec.taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: spec.taskID, LinkedBy: "developer"}); err != nil {
			t.Fatalf("attach task %s: %v", spec.taskID, err)
		}
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, activeAgentID, `C:\fixtures\agents\beta\clearpress-auth-profile-articles`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branchID,
		BranchName:            "agent/beta/clearpress-auth-profile-articles",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        activeScopeJSON,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               activeAgentID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        activeScopeJSON,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim auth/profile/articles lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, activeTaskID, activeAgentID),
	}); err != nil {
		t.Fatalf("claim active task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          candidateAgentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != candidateTaskID {
		packetWhy := ""
		if result.Packet != nil {
			packetWhy = result.Packet.WhyNow
		}
		t.Fatalf("expected broad editor task to narrow semantically and stay claimable, reason=%s packet_why=%q task=%+v", result.Reason, packetWhy, result.Task)
	}
	if result.Reason == "project_claim_scope_busy" || result.Reason == "project_validation_artifact_missing" {
		t.Fatalf("semantic editor lane should not be hidden behind busy or downstream artifact waits: reason=%s packet=%+v", result.Reason, result.Packet)
	}
}

func TestGetAgentWorkNextTrustFirstSemanticScopeNarrowsGoRQTaskHintsForBusyGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID      = "ws-agent-work-rq-semantic-scope"
		projectID        = "project-signal01-rq-semantic-scope"
		leadID           = "alpha"
		activeAgentID    = "kappa"
		candidateAgentID = "gamma"
		repoID           = "repo-signal01-rq"
		activeTaskID     = "task-signal01-rq-lexer"
		candidateTaskID  = "task-signal01-rq-evaluator"
		branchID         = "branch-kappa-rq-lexer"
		activeScopeJSON  = `{"paths":["internal/lexer/**","internal/token/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, activeAgentID, candidateAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, spec := range []struct {
		taskID      string
		title       string
		description string
		hints       []string
	}{
		{
			taskID:      activeTaskID,
			title:       "Implement rq lexer with positioned diagnostics",
			description: "Build tokenizer and token stream support for the rq interpreter.",
			hints:       []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
		},
		{
			taskID:      candidateTaskID,
			title:       "Implement rq evaluator core and JSON path semantics",
			description: "Evaluate rq AST nodes against JSON input values.",
			hints:       []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
		},
	} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:         workspaceID,
			TaskID:              spec.taskID,
			OwnerUserID:         "developer",
			Priority:            "high",
			Title:               spec.title,
			Description:         spec.description,
			TaskKind:            "EXECUTION",
			TaskTemplate:        "generic",
			ProjectID:           projectID,
			ProjectLane:         "implementation",
			RequiresProjectGate: true,
			WriteScopeHints:     spec.hints,
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", spec.taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: spec.taskID, LinkedBy: "developer"}); err != nil {
			t.Fatalf("attach task %s: %v", spec.taskID, err)
		}
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, activeAgentID, `C:\fixtures\agents\kappa\rq-lexer`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branchID,
		BranchName:            "agent/kappa/rq-lexer",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        activeScopeJSON,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               activeAgentID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        activeScopeJSON,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim rq lexer lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, activeTaskID, activeAgentID),
	}); err != nil {
		t.Fatalf("claim active rq lexer task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          candidateAgentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != candidateTaskID {
		packetWhy := ""
		if result.Packet != nil {
			packetWhy = result.Packet.WhyNow
		}
		t.Fatalf("expected broad rq evaluator task to narrow semantically and stay claimable, reason=%s packet_why=%q task=%+v", result.Reason, packetWhy, result.Task)
	}
	if result.Reason == "project_claim_scope_busy" {
		t.Fatalf("rq evaluator lane should not be hidden behind active lexer scope: %+v", result.Packet)
	}
}

func TestGetAgentWorkNextIgnoresUnboundLiveLookingBranchScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-agent-work-unbound-branch"
		projectID     = "project-agent-work-unbound-branch"
		leadID        = "alpha"
		branchAgentID = "delta"
		candidateID   = "gamma"
		repoID        = "repo-main"
		candidateTask = "task-dashboard"
		unboundBranch = "branch-unbound-overlap"
		overlapScope  = `{"paths":["src/**","public/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, branchAgentID, candidateID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              candidateTask,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Build dashboard",
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		WriteScopeHints:     []string{"src/**", "public/**"},
	}, graph); err != nil {
		t.Fatalf("create candidate task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      candidateTask,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach candidate task: %v", err)
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, branchAgentID, `C:\fixtures\agents\delta\unbound-agent-work`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               branchAgentID,
		BranchID:              unboundBranch,
		BranchName:            "agent/delta/unbound-agent-work",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        overlapScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               branchAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", branchAgentID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register unbound branch: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          candidateID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != candidateTask {
		t.Fatalf("expected unbound branch scope not to block candidate task, got %+v", result)
	}
	if result.Reason == "project_claim_scope_busy" {
		t.Fatalf("unbound branch should not produce project_claim_scope_busy: %+v", result.Packet)
	}
}

func TestGetAgentWorkNextTrustFirstPrefersTaskScopeOverRepairRoleScopeForBusyGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-trust-first-task-scope-over-role"
		projectID      = "project-task-scope-over-role"
		leadID         = "alpha"
		activeAgentID  = "alpha-worker"
		blockedAgentID = "gamma"
		repoID         = "repo-main"
		activeTaskID   = "task-interactions"
		blockedTaskID  = "task-dashboard"
		branchID       = "branch-alpha-worker-interactions"
		activeScope    = `{"paths":["src/**","public/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, activeAgentID, blockedAgentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ActorID:               leadID,
		ActorType:             "agent",
		AgentID:               blockedAgentID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["package*.json","vite.config.*","index.html"]}`,
		Summary:               "stale repair role should not override task-local trust-first scope",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign repair role: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, spec := range []struct {
		taskID string
		title  string
		hints  []string
	}{
		{activeTaskID, "Add operator interactions", []string{"src/**", "public/**"}},
		{blockedTaskID, "Build first-screen dashboard", []string{"src/**", "public/**"}},
	} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:         workspaceID,
			TaskID:              spec.taskID,
			OwnerUserID:         "developer",
			Priority:            "high",
			Title:               spec.title,
			TaskKind:            "EXECUTION",
			TaskTemplate:        "generic",
			ProjectID:           projectID,
			ProjectLane:         "implementation",
			RequiresProjectGate: true,
			WriteScopeHints:     spec.hints,
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", spec.taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      spec.taskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", spec.taskID, err)
		}
	}

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, activeAgentID, `C:\fixtures\agents\alpha-worker\task-scope-over-role`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               activeAgentID,
		BranchID:              branchID,
		BranchName:            "agent/alpha-worker/interactions",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        activeScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               activeAgentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", activeAgentID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                activeTaskID,
		AgentID:               activeAgentID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        activeScope,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim interactions lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, activeTaskID, activeAgentID),
	}); err != nil {
		t.Fatalf("claim active task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          blockedAgentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_claim_scope_busy" {
		t.Fatalf("expected task-local trust-first scope to detect busy src/public lane despite config repair role, got %+v", result)
	}
	if result.Packet == nil || result.Packet.Gate == nil || !strings.Contains(result.Packet.Gate.Summary, activeTaskID) {
		t.Fatalf("expected busy packet to identify active claim conflict, got %+v", result.Packet)
	}
	if len(result.Packet.ContextHints.AnchorTaskIDs) != 1 || result.Packet.ContextHints.AnchorTaskIDs[0] != blockedTaskID {
		t.Fatalf("expected busy packet to anchor blocked task, got %+v", result.Packet.ContextHints)
	}
	if len(result.Packet.ContextHints.AnchorBranchIDs) != 1 || result.Packet.ContextHints.AnchorBranchIDs[0] != branchID {
		t.Fatalf("expected busy packet to anchor conflict branch, got %+v", result.Packet.ContextHints)
	}
}

func TestClaimTaskRoutesImplicitReviewReadyPublicationToActiveBranchOwner(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-implicit-publication-owner"
		projectID   = "project-implicit-publication-owner"
		leadID      = "alpha"
		ownerID     = "gamma"
		otherID     = "beta"
		repoID      = "repo-clearpress"
		branchID    = "branch-gamma-active"
		taskID      = "task-publish-candidate-provenance"
	)

	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, otherID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\gamma\clearpress`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              branchID,
		BranchName:            "agent/gamma/clearpress",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		WriteScopeJSON:        `{"paths":["src/**","public/**","package.json"]}`,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Publish Clearpress candidate provenance and review-ready evidence", "coordination", "high")

	err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     otherID,
		Summary:     "publish provenance from my checkout",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "requires agent "+ownerID) {
		t.Fatalf("expected non-owner claim to be rejected for active branch owner, got %v", err)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     ownerID,
		Summary:     "publish provenance for owned active branch",
	}); err != nil {
		t.Fatalf("expected active branch owner to claim publication task: %v", err)
	}
}

func TestClaimTaskRoutesRunnableCandidatePublicationToActiveBranchOwner(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-agent-runnable-candidate-publication-owner"
		projectID   = "project-runnable-candidate-publication-owner"
		leadID      = "alpha"
		ownerID     = "gamma"
		otherID     = "beta"
		repoID      = "repo-clearpress"
		branchID    = "branch-gamma-active-runnable"
		taskID      = "task-clearpress-autonomous-mvp-runnable-candidate-publication-20260519-alpha"
	)

	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, otherID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\gamma\clearpress`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              branchID,
		BranchName:            "agent/gamma/clearpress",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		WriteScopeJSON:        `{"paths":["src/**","public/**","package.json"]}`,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Publish Clearpress runnable candidate publication and review-ready evidence", "implementation", "high")

	err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     otherID,
		Summary:     "publish runnable candidate from a clean seed checkout",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "requires agent "+ownerID) {
		t.Fatalf("expected non-owner runnable publication claim to be rejected for active branch owner, got %v", err)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:    workspaceID,
		TaskID:         taskID,
		AgentID:        ownerID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		BranchID:       branchID,
		WriteScopeJSON: `{"paths":["src/**","public/**","package.json"]}`,
		Summary:        "publish runnable candidate from owned active branch",
	}); err != nil {
		t.Fatalf("expected active branch owner to claim runnable publication task: %v", err)
	}
}

func TestClaimTaskDoesNotTreatIntegrationConvergenceAsOwnerBoundPublication(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-agent-integration-convergence-not-owner-bound"
		projectID    = "project-integration-convergence-not-owner-bound"
		leadID       = "alpha"
		integratorID = "zeta"
		repoID       = "repo-rq"
		taskID       = "task-rq-integration-convergence"
	)

	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, "gamma", "eta", integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               integratorID,
		RoleType:              sqlite.ProjectRoleIntegrator,
		WriteScopeJSON:        `{"paths":["cmd/**","internal/**","tests/**","testdata/**","README.md","go.mod"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign integrator role: %v", err)
	}
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, "gamma", "branch-gamma-parser")
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, "eta", "branch-eta-evaluator")
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, taskID, "Integrate rq implementation lanes and publish review-ready candidate", "integration", "high")

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     integratorID,
		Summary:     "integrate multiple ready lanes into one review-ready candidate",
	}); err != nil {
		t.Fatalf("integration convergence task must not be rejected as ambiguous owner-bound publication: %v", err)
	}
}

func TestGetAgentWorkNextReroutesPublicationSwitchToActiveImplementationLane(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID       = "ws-agent-publication-reroute-active-lane"
		projectID         = "project-publication-reroute-active-lane"
		leadID            = "alpha"
		ownerID           = "eta"
		repoID            = "repo-clearpress"
		branchID          = "branch-eta-active"
		implementationID  = "task-clearpress-build-shell"
		publicationTaskID = "task-clearpress-eta-provenance-publication"
		sessionID         = "session-eta-build-shell"
	)

	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**","tests/**","package.json"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createAgentWorkProjectExecutionTask(t, ctx, store, workspaceID, projectID, implementationID, "Implement Clearpress shell and profile persistence", "implementation", []string{"frontend", "implementation"}, true)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\eta\clearpress`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              branchID,
		BranchName:            "agent/eta/clearpress",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		WriteScopeJSON:        `{"paths":["src/**","tests/**","package.json"]}`,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                implementationID,
		AgentID:               ownerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branchID,
		WriteScopeJSON:        `{"paths":["src/**","tests/**","package.json"]}`,
		Summary:               "claim implementation lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, implementationID, ownerID),
	}); err != nil {
		t.Fatalf("claim implementation task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     ownerID,
		WorkspaceID: workspaceID,
		TaskID:      implementationID,
		StartedAt:   "2026-05-21T00:00:00Z",
	}); err != nil {
		t.Fatalf("create implementation session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.start",
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     ownerID,
		TaskID:      implementationID,
		Summary:     "implementation lane active",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("record implementation session: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, publicationTaskID, "Publish exact Clearpress runnable candidate provenance", "coordination", "high")

	err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      publicationTaskID,
		AgentID:     ownerID,
		Summary:     "claim sidecar provenance",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "resume active implementation task "+implementationID) {
		t.Fatalf("expected sidecar claim to be blocked by active implementation lane, got %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     workspaceID,
		AgentID:         ownerID,
		IncludePacket:   true,
		Trigger:         "runtime_switch_task",
		CandidateTaskID: publicationTaskID,
	})
	if err != nil {
		t.Fatalf("get triggered publication work next: %v", err)
	}
	if !result.HasWork || result.Task == nil || result.Task.TaskID != implementationID {
		t.Fatalf("expected publication switch to resume implementation task, got %+v", result)
	}
	if result.Session == nil || result.Session.SessionID != sessionID {
		t.Fatalf("expected implementation session %s, got %+v", sessionID, result.Session)
	}
	if result.Reason != "resume_session" || result.SessionAction != "reuse_active" || result.ClaimAction != "reuse_claim" {
		t.Fatalf("expected reuse of active implementation lane, got reason=%q claim=%q session=%q", result.Reason, result.ClaimAction, result.SessionAction)
	}
	if !strings.Contains(result.ResumeSummary, publicationTaskID) || !strings.Contains(result.ResumeSummary, "commit/push") {
		t.Fatalf("expected resume summary to explain publication reroute, got %q", result.ResumeSummary)
	}
	if result.Packet == nil || result.Packet.Resume == nil || result.Packet.Resume.SessionID != sessionID {
		t.Fatalf("expected packet resume for implementation session, got %+v", result.Packet)
	}
}

func TestGetAgentWorkNextBlocksPublicationGapSidecarForNonOwnerActiveBranch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID       = "ws-agent-publication-gap-non-owner"
		projectID         = "project-publication-gap-non-owner"
		leadID            = "alpha"
		ownerID           = "gamma"
		otherID           = "zeta"
		repoID            = "repo-clearpress"
		branchID          = "branch-gamma-active-editor"
		implementationID  = "task-clearpress-editor-core"
		publicationTaskID = "task-clearpress-editor-publication-gap"
		sessionID         = "session-gamma-editor-core"
	)

	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, otherID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/editor/**","src/lib/editor/**","tests/editor/**"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createAgentWorkProjectExecutionTask(t, ctx, store, workspaceID, projectID, implementationID, "Implement Clearpress editor core", "implementation", []string{"editor", "implementation"}, true)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\gamma\clearpress`)
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              branchID,
		BranchName:            "agent/gamma/clearpress-editor",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		WriteScopeJSON:        `{"paths":["src/editor/**","src/lib/editor/**","tests/editor/**"]}`,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	}); err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                implementationID,
		AgentID:               ownerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branchID,
		WriteScopeJSON:        `{"paths":["src/editor/**","src/lib/editor/**","tests/editor/**"]}`,
		Summary:               "claim editor implementation lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, implementationID, ownerID),
	}); err != nil {
		t.Fatalf("claim implementation task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     ownerID,
		WorkspaceID: workspaceID,
		TaskID:      implementationID,
		StartedAt:   "2026-05-21T00:00:00Z",
	}); err != nil {
		t.Fatalf("create implementation session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   "session.start",
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     ownerID,
		TaskID:      implementationID,
		Summary:     "editor lane active",
		Status:      "ACTIVE",
	}); err != nil {
		t.Fatalf("record implementation session: %v", err)
	}
	createAgentWorkProjectTask(t, ctx, store, workspaceID, projectID, publicationTaskID, "Publish or release editor lane provenance for Clearpress MVP", "coordination", "high")

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     workspaceID,
		AgentID:         otherID,
		IncludePacket:   true,
		Trigger:         "runtime_switch_task",
		CandidateTaskID: publicationTaskID,
	})
	if err != nil {
		t.Fatalf("get triggered publication work next for non-owner: %v", err)
	}
	if result.HasWork || result.Reason != "project_owner_bound_agent_required" {
		t.Fatalf("expected non-owner publication-gap sidecar to be owner-bound blocked, got %+v", result)
	}
	if result.Packet == nil ||
		result.Packet.HandoffToAgentID != ownerID ||
		result.Packet.Gate == nil ||
		result.Packet.Gate.NeededFrom != ownerID ||
		!strings.Contains(result.Packet.WhyNow, "active_lane_publication") {
		t.Fatalf("expected sidecar packet to hand off to active branch owner %s, got %+v", ownerID, result.Packet)
	}
}

func seedAgentWorkWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, agentIDs []string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
}

func createAgentWorkTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, priority string) {
	t.Helper()

	createAgentWorkTaskWithDetails(t, ctx, store, workspaceID, taskID, taskID, "", priority)
}

func createAgentWorkTaskWithDetails(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, title, description, priority string) {
	t.Helper()

	createAgentWorkTaskWithTemplate(t, ctx, store, workspaceID, taskID, title, description, model.TaskTemplateIntegration, "", priority)
}

func createAgentWorkTaskWithTemplate(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, title, description, template, lane, priority string) {
	createAgentWorkTaskWithTemplateAndTags(t, ctx, store, workspaceID, taskID, title, description, template, lane, priority, nil)
}

func createAgentWorkTaskWithTemplateAndTags(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, title, description, template, lane, priority string, tags []string) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     priority,
		Title:        title,
		Description:  description,
		TaskKind:     "COORDINATION",
		TaskTemplate: template,
		Tags:         tags,
		ProjectLane:  lane,
	}, graph); err != nil {
		t.Fatalf("create task %s: %v", taskID, err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task %s: %v", taskID, err)
	}
}

func createAgentWorkProjectTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID, title, lane, priority string) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	writeScopeHints := activeProjectImplementerScopeHintsForTest(t, ctx, store, workspaceID, projectID)
	if len(writeScopeHints) == 0 {
		switch strings.ToLower(strings.TrimSpace(lane)) {
		case "implementation", "backend", "frontend":
			writeScopeHints = []string{"**"}
		}
	}
	taskRequirementsJSON := ""
	if len(writeScopeHints) > 0 {
		taskRequirementsJSON = `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		OwnerUserID:          "developer",
		Priority:             priority,
		Title:                title,
		TaskKind:             "COORDINATION",
		TaskTemplate:         "integration",
		ProjectID:            projectID,
		ProjectLane:          lane,
		DependencyTaskIDs:    nil,
		TaskRequirementsJSON: taskRequirementsJSON,
		WriteScopeHints:      writeScopeHints,
	}, graph); err != nil {
		t.Fatalf("create project task %s: %v", taskID, err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach project task %s: %v", taskID, err)
	}
}

func createAgentWorkProjectExecutionTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID, title, lane string, tags []string, requiresProjectGate bool) {
	t.Helper()

	createAgentWorkProjectExecutionTaskWithDescription(t, ctx, store, workspaceID, projectID, taskID, title, "", lane, tags, requiresProjectGate)
}

func createAgentWorkProjectExecutionTaskWithDescription(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID, title, description, lane string, tags []string, requiresProjectGate bool) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	writeScopeHints := activeProjectImplementerScopeHintsForTest(t, ctx, store, workspaceID, projectID)
	if len(writeScopeHints) == 0 {
		switch strings.ToLower(strings.TrimSpace(lane)) {
		case "implementation", "backend", "frontend":
			writeScopeHints = []string{"**"}
		}
	}
	taskRequirementsJSON := ""
	if len(writeScopeHints) > 0 {
		taskRequirementsJSON = `{"schema":"task_requirements.v1","preserve_write_scope_hints":true}`
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               taskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                title,
		Description:          description,
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		Tags:                 tags,
		ProjectID:            projectID,
		ProjectLane:          lane,
		RequiresProjectGate:  requiresProjectGate,
		TaskRequirementsJSON: taskRequirementsJSON,
		WriteScopeHints:      writeScopeHints,
	}, graph); err != nil {
		t.Fatalf("create project execution task %s: %v", taskID, err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach project execution task %s: %v", taskID, err)
	}
}

func createAgentWorkTaskBypassingSubmitGate(t *testing.T, ctx context.Context, store *sqlite.Store, input sqlite.TaskCreateInput, graph dag.Graph) error {
	t.Helper()

	seed := input
	seed.Title = "submit gate bypass seed " + strings.TrimSpace(input.TaskID)
	seed.Description = ""
	seed.Tags = nil
	seed.ProjectID = ""
	seed.ProjectLane = ""
	seed.RequiresProjectGate = false
	seed.TaskRequirementsJSON = "{}"
	seed.WriteScopeHints = nil
	if err := store.CreateTaskWithGraph(ctx, seed, graph); err != nil {
		return err
	}

	tagsJSON := "[]"
	if len(input.Tags) > 0 {
		raw, _ := json.Marshal(input.Tags)
		tagsJSON = string(raw)
	}
	requirementsJSON := strings.TrimSpace(input.TaskRequirementsJSON)
	if requirementsJSON == "" {
		requirementsJSON = "{}"
	}
	writeScopeHintsJSON := "[]"
	if len(input.WriteScopeHints) > 0 {
		raw, _ := json.Marshal(input.WriteScopeHints)
		writeScopeHintsJSON = string(raw)
	}
	requiresProjectGateInt := 0
	if input.RequiresProjectGate {
		requiresProjectGateInt = 1
	}
	_, err := store.DB().ExecContext(ctx, `
UPDATE tasks
   SET title = ?,
       description = ?,
       tags_json = ?,
       project_id = ?,
       project_lane = ?,
       requires_project_gate = ?,
       task_requirements_json = ?,
       write_scope_hints_json = ?
 WHERE task_id = ?`,
		strings.TrimSpace(input.Title),
		strings.TrimSpace(input.Description),
		tagsJSON,
		strings.TrimSpace(input.ProjectID),
		strings.TrimSpace(input.ProjectLane),
		requiresProjectGateInt,
		requirementsJSON,
		writeScopeHintsJSON,
		strings.TrimSpace(input.TaskID),
	)
	return err
}

func upsertAgentWorkReadyProjectRepo(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, actorID string) {
	t.Helper()

	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		RemoteURL:             "file:///tmp/" + repoID,
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindLocal,
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		DefaultBranch:         "main",
		IntegrationBranch:     "main",
		IsCanonical:           true,
		CreatedByAgentID:      actorID,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.repository.upsert",
	}); err != nil {
		t.Fatalf("upsert project repository: %v", err)
	}
}

func assignAgentWorkProjectRole(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, agentID, roleType, actorID string) {
	t.Helper()

	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		RoleType:              roleType,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign project role %s to %s: %v", roleType, agentID, err)
	}
}

func markAgentWorkTaskReleasedWithBranch(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, agentID, repoID, branchID string) {
	t.Helper()

	now := "2026-05-15T03:00:00Z"
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT OR REPLACE INTO task_claims(
  task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at,
  project_role_id, repo_id, checkout_id, branch_id, write_scope_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		taskID,
		workspaceID,
		agentID,
		model.TaskClaimStatusReleased,
		"released after publishing review-ready patch queue artifact",
		now,
		now,
		now,
		"",
		repoID,
		"",
		branchID,
		`{"paths":["src/**","tests/**"]}`,
	); err != nil {
		t.Fatalf("mark task released with branch: %v", err)
	}
}

func patchQueueSupersedeTaskIDForAgentWorkTest(projectID, queueID, itemID, branchID, headSHA, evidenceDocKey string) string {
	seed := strings.Join([]string{projectID, queueID, itemID, branchID, headSHA, evidenceDocKey}, "|")
	return sanitizeRefSegmentForAgentWorkTest("task-patchq-supersede-" + compactRefSegmentForAgentWorkTest("project", projectID) + "-" + shortHashForAgentWorkTest(seed))
}

func shortHashForAgentWorkTest(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:8])
}

func compactRefSegmentForAgentWorkTest(prefix, value string) string {
	const maxRefSegmentLen = 32
	raw := strings.TrimSpace(value)
	if raw == "" {
		raw = strings.TrimSpace(prefix)
	}
	if raw == "" {
		raw = "x"
	}
	segment := sanitizeRefSegmentForAgentWorkTest(raw)
	if len(segment) <= maxRefSegmentLen {
		return segment
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(raw)))
	hash := hex.EncodeToString(sum[:])[:10]
	keep := maxRefSegmentLen - len(hash) - 1
	if keep < 1 {
		return hash[:maxRefSegmentLen]
	}
	head := strings.Trim(segment[:keep], "-.")
	if head == "" {
		head = sanitizeRefSegmentForAgentWorkTest(prefix)
	}
	if len(head) > keep {
		head = strings.Trim(head[:keep], "-.")
	}
	if head == "" {
		head = "x"
	}
	return head + "-" + hash
}

func sanitizeRefSegmentForAgentWorkTest(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '.'
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	segment := strings.Trim(b.String(), "-.")
	if segment == "" {
		segment = "x"
	}
	if len(segment) > 80 {
		segment = strings.Trim(segment[:80], "-.")
	}
	return segment
}

func createOwnerBoundPatchQueueSubmitTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID, branchID, ownerAgentID string) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "submit", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "critical",
		Title:        "Owner-only project_patch_queue_submit for " + branchID,
		Description:  "Owner-only queue submit.\n\n- branch_id: " + branchID + "\n- required_agent_id: " + ownerAgentID,
		TaskKind:     "EXECUTION",
		TaskTemplate: "integration",
		Tags: []string{
			"project",
			"patch-queue",
			"integration",
			"owner-bound",
			"owner-bound-kind:patch_queue_submit",
			"owner-branch:" + branchID,
			"required-agent:" + ownerAgentID,
		},
		ProjectID:   projectID,
		ProjectLane: "integration",
	}, graph); err != nil {
		t.Fatalf("create owner-bound submit task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach owner-bound submit task: %v", err)
	}
}

func createOwnerSubmitCoordinationTaskForAgentWorkTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID, branchID, ownerAgentID string) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "submit", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Owner requeue submit for " + branchID,
		Description:         "Branch owner " + ownerAgentID + " should submit the READY_FOR_REVIEW branch.\n\nbranch_id: " + branchID,
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		Tags:                []string{"project", "patch-queue", "requeue", "coordination", "owner-submit"},
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create owner-submit coordination task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach owner-submit coordination task: %v", err)
	}
}

func createOwnerBoundPatchQueueSubmitTaskWithoutBranch(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID, ownerAgentID string) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "submit", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       taskID,
		OwnerUserID:  "developer",
		Priority:     "critical",
		Title:        "Owner-only project_patch_queue_submit for required agent " + ownerAgentID,
		Description:  "Owner-only queue submit without explicit branch.",
		TaskKind:     "EXECUTION",
		TaskTemplate: "integration",
		Tags: []string{
			"project",
			"patch-queue",
			"integration",
			"owner-bound",
			"owner-bound-kind:patch_queue_submit",
			"required-agent:" + ownerAgentID,
		},
		ProjectID:   projectID,
		ProjectLane: "integration",
	}, graph); err != nil {
		t.Fatalf("create owner-bound submit without branch: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach owner-bound submit without branch: %v", err)
	}
}

func registerReadyOwnerBranchForAgentWorkTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, ownerID, branchID string) sqlite.ProjectBranchRecord {
	t.Helper()

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\`+ownerID+`\`+branchID)
	taskID := "task-" + branchID
	scopeJSON := `{"paths":["src/**"]}`
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              branchID,
		BranchName:            "agent/" + ownerID + "/" + branchID,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register reserved owner branch %s: %v", branchID, err)
	}
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, checkout.CheckoutID, branch.BranchID, taskID, scopeJSON)
	reviewKey := "project." + projectID + ".branch." + branchID + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Review packet",
		Content:     "# Review\n\nReady.",
		UpdatedBy:   ownerID,
	}); err != nil {
		t.Fatalf("write review doc for %s: %v", branchID, err)
	}
	branch, _, err = store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              branchID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/" + ownerID + "/" + branchID,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        scopeJSON,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ready owner branch %s: %v", branchID, err)
	}
	return branch
}

func createAgentWorkDecidedPatchQueueItem(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, branchID, ownerID, reviewerID, scopeJSON, decision string) sqlite.ProjectPatchQueueItemRecord {
	t.Helper()

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\`+ownerID+`\`+branchID)
	taskID := "task-" + branchID
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              branchID,
		BranchName:            "agent/" + ownerID + "/" + branchID,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        scopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register reserved branch %s: %v", branchID, err)
	}
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, checkout.CheckoutID, branch.BranchID, taskID, scopeJSON)
	reviewKey := "project." + projectID + ".branch." + branchID + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for patch queue decision.",
		UpdatedBy:   ownerID,
	}); err != nil {
		t.Fatalf("write review doc for %s: %v", branchID, err)
	}
	branch, _, err = store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              branchID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/" + ownerID + "/" + branchID,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 39) + "1",
		WriteScopeJSON:        scopeJSON,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ready branch %s: %v", branchID, err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   "task-" + branchID,
		SessionID:                "session-" + branchID,
		RunID:                    "run-" + branchID,
		AgentID:                  ownerID,
		CapabilitySnapshotID:     "cap-" + branchID,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             strings.Repeat("a", 40),
		BaseFileHashes:           agentWorkTestBaseFileHashesForScope(scopeJSON),
		RepoLeaseID:              "lease-" + branchID,
		LeaseTerm:                7,
		ActorID:                  ownerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit patch queue item for %s: %v", branchID, err)
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
		t.Fatalf("claim patch queue item for %s: %v", branchID, err)
	}
	decisionDocKey := ""
	if strings.EqualFold(strings.TrimSpace(decision), sqlite.ProjectPatchQueueStateAccepted) && agentWorkTestPatchQueueScopeNeedsVisualAcceptance(scopeJSON) {
		decisionDocKey = "project." + projectID + ".patch_queue." + branchID + ".visual_acceptance"
		if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      decisionDocKey,
			Title:       "Visual Acceptance Packet",
			Content:     agentWorkTestAcceptedVisualPacket(item, branch),
			UpdatedBy:   reviewerID,
		}); err != nil {
			t.Fatalf("write visual acceptance doc for %s: %v", branchID, err)
		}
	}
	decided, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              decision,
		DecisionDocKey:        decisionDocKey,
		DecisionSummary:       "Patch queue decision " + decision + " for " + branchID + ".",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("decide patch queue item for %s: %v", branchID, err)
	}
	return decided
}

func integrateAgentWorkPatchQueueItem(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, integratorID string, item sqlite.ProjectPatchQueueItemRecord) sqlite.ProjectPatchQueueItemRecord {
	t.Helper()

	integrated, _, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetBranch:          "main",
		TargetHeadBefore:      strings.Repeat("c", 40),
		TargetHeadAfter:       strings.Repeat("d", 40),
		RemoteTargetHeadAfter: strings.Repeat("d", 40),
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		PushAttempted:         true,
		PushSucceeded:         true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.integration_record", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.integration_record",
	})
	if err != nil {
		t.Fatalf("integrate patch queue item %s/%s: %v", item.QueueID, item.ItemID, err)
	}
	return integrated
}

func agentWorkTestBaseFileHashesForScope(scopeJSON string) map[string]string {
	var payload struct {
		Paths []string `json:"paths"`
	}
	_ = json.Unmarshal([]byte(scopeJSON), &payload)
	hashes := map[string]string{}
	addHash := func(path string) {
		path = strings.TrimSpace(strings.ReplaceAll(path, "\\", "/"))
		if path == "" {
			return
		}
		hashes[path] = "sha256:" + sanitizeRefSegmentForAgentWorkTest(path)
	}
	for _, path := range payload.Paths {
		path = strings.TrimSpace(strings.ReplaceAll(path, "\\", "/"))
		switch {
		case path == "":
			continue
		case path == "src/**":
			addHash("src/app.go")
		case path == "web/**":
			addHash("web/app.js")
		case strings.HasSuffix(path, "/**"):
			addHash(strings.TrimSuffix(path, "/**") + "/app.js")
		case strings.Contains(path, "*"):
			continue
		default:
			addHash(path)
		}
	}
	if len(hashes) == 0 {
		hashes["src/app.go"] = "sha256:src"
	}
	return hashes
}

func agentWorkTestPatchQueueScopeNeedsVisualAcceptance(scopeJSON string) bool {
	scope := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(scopeJSON), "\\", "/"))
	for _, marker := range []string{
		"index.html",
		"web/",
		".tsx",
		".jsx",
		".css",
		".scss",
		"src/components",
		"src/pages",
		"public/",
	} {
		if strings.Contains(scope, marker) {
			return true
		}
	}
	return false
}

func agentWorkTestAcceptedVisualPacket(item sqlite.ProjectPatchQueueItemRecord, branch sqlite.ProjectBranchRecord) string {
	return strings.Join([]string{
		"schema: rhizome_visual_acceptance_v1",
		"visual_verdict: pass",
		"product_intent:",
		"  acceptance_criteria: AC-agent-work",
		"  core_user_promise: user can inspect the first screen, complete the primary flow, and see the result state.",
		"provenance:",
		"  queue_id: " + item.QueueID,
		"  item_id: " + item.ItemID,
		"  branch_id: " + branch.BranchID,
		"  branch_name: " + branch.BranchName,
		"  head_sha: " + item.HeadSHA,
		"  observed_url: http://127.0.0.1:51955/",
		"  validation_checkout: C:/fixtures/agents/" + branch.AgentID + "/" + branch.BranchID,
		"viewport_matrix:",
		"  desktop: 1440x900",
		"  mobile: 390x844",
		"state_evidence:",
		"  initial_state: screenshot_path C:/tmp/agent-work-initial.png",
		"  mobile_state: screenshot_path C:/tmp/agent-work-mobile.png",
		"  primary_flow: screenshot_path C:/tmp/agent-work-primary.png",
		"  result_state: screenshot_path C:/tmp/agent-work-result.png",
		"checks:",
		"  overlap: pass",
		"  clipping: pass",
		"  contrast/readability: pass",
		"  responsive typography hierarchy spacing usability: pass",
		"  primary surface geometry/density: pass",
		"layout_risk:",
		"  source: browser_visual_probe_result_v1",
		"  risk_level: low",
		"  risk_signals: none",
	}, "\n")
}

func agentWorkCoordinationHasTask(tasks []sqlite.WorkspaceTaskRecord, taskID string) bool {
	for _, task := range tasks {
		if task.TaskID == taskID {
			return true
		}
	}
	return false
}

// TestRequiredReceiptSweepScansPastReceiptlessCandidates locks F1 (P0): the required-
// receipt sweep must keep scanning after meeting candidates WITHOUT receipts. Before the
// fix, reconcileAgentWorkRequiredReceiptTerminals returned on the first receipt-less
// candidate; with fresh PENDING tasks sorting before the stale carrier (updated_at DESC),
// the stale receipt-bearing row survived the sweep and re-entered selection (the R41
// class). R42's single-row validation could not catch this.
func TestRequiredReceiptSweepScansPastReceiptlessCandidates(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-receipt-sweep-scan-past"
		projectID   = "project-receipt-sweep-scan-past"
		leadID      = "alpha"
		builderID   = "beta"
		carrierID   = "task-role-scope-receipt-sweep-stale"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	// The durable receipt: the requested role already exists.
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, builderID, sqlite.ProjectRoleReviewer, leadID)

	// Stale role-scope carrier created FIRST (oldest updated_at => sorts LAST among the
	// sweep candidates, behind the receipt-less fresh tasks below).
	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, carrierID,
		"Resolve project role/scope request for beta",
		"Stale role/scope carrier whose role assignment already exists.",
		"coordination", []string{"project-role-scope"}, false,
	)
	if _, err := store.WriteDB().ExecContext(ctx,
		`UPDATE tasks SET task_requirements_json = ? WHERE task_id = ?`,
		`{"required_transition":"project_role_assign","target_agent_id":"beta","role_type":"REVIEWER"}`,
		carrierID,
	); err != nil {
		t.Fatalf("set carrier requirements: %v", err)
	}

	// Two fresh receipt-less PENDING candidates with NEWER updated_at (created later), so
	// they are scanned BEFORE the stale carrier.
	for _, freshID := range []string{
		"task-receipt-sweep-fresh-one",
		"task-receipt-sweep-fresh-two",
	} {
		createAgentWorkProjectExecutionTaskWithDescription(
			t, ctx, store, workspaceID, projectID, freshID,
			"Fresh product work "+freshID,
			"Receipt-less pending implementation work.",
			"implementation", []string{"implementation"}, true,
		)
	}

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get agent work next: %v", err)
	}

	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	for _, task := range tasks {
		switch task.TaskID {
		case carrierID:
			if task.Status != model.TaskStatusResolved {
				t.Fatalf("stale carrier status=%s, want RESOLVED (sweep must scan past receipt-less candidates)", task.Status)
			}
		case "task-receipt-sweep-fresh-one", "task-receipt-sweep-fresh-two":
			if task.Status != model.TaskStatusPending {
				t.Fatalf("fresh task %s status=%s, want PENDING (must not be touched)", task.TaskID, task.Status)
			}
		}
	}
}

// TestRequiredReceiptTerminalizesRepoRepairCarrierWhenRepoReady locks F2: repository-
// repair carriers previously had NO receipt->terminal path; once the project repository
// is READY, the carrier must terminalize instead of outliving its own success.
func TestRequiredReceiptTerminalizesRepoRepairCarrierWhenRepoReady(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-receipt-repo-repair-ready"
		projectID   = "project-receipt-repo-repair-ready"
		repoID      = "repo-receipt-repo-repair-ready"
		leadID      = "alpha"
		builderID   = "beta"
		carrierID   = "task-repo-repair-receipt-ready"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)

	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, carrierID,
		"Repair canonical project repository",
		"Repository mutation requires active strategic lead.\n- repo_id: "+repoID,
		"coordination", []string{"project-repo-repair"}, false,
	)

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get agent work next: %v", err)
	}

	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	for _, task := range tasks {
		if task.TaskID != carrierID {
			continue
		}
		if task.Status != model.TaskStatusResolved {
			t.Fatalf("repo-repair carrier status=%s, want RESOLVED (repo is READY)", task.Status)
		}
		return
	}
	t.Fatalf("repo-repair carrier %s not found", carrierID)
}

// TestRequiredReceiptCancelsClaimRepairSidecarWithTerminalTaskRef locks F3: claim-repair
// sidecars WITHOUT a conflict_branch_id (the fresh-owner-evidence shape) previously had
// no terminal path. When the referenced conflict/blocked task is terminal, the sidecar
// is moot and must cancel.
func TestRequiredReceiptCancelsClaimRepairSidecarWithTerminalTaskRef(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-receipt-claim-repair-taskref"
		projectID   = "project-receipt-claim-repair-taskref"
		leadID      = "alpha"
		builderID   = "beta"
		refTaskID   = "task-claim-repair-ref-target"
		carrierID   = "task-project-claim-repair-fresh-owner-evidence-test"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, refTaskID,
		"Conflicting owner lane",
		"The lane whose claim/scope the repair sidecar chased.",
		"implementation", []string{"implementation"}, false,
	)
	if _, err := store.WriteDB().ExecContext(ctx,
		`UPDATE tasks SET status = ? WHERE task_id = ?`, model.TaskStatusResolved, refTaskID,
	); err != nil {
		t.Fatalf("terminalize ref task: %v", err)
	}

	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, carrierID,
		"Repair stale owner claim evidence",
		"Claim repair sidecar without a conflict branch.\n- conflict_task_id: "+refTaskID,
		"coordination", []string{"project-claim-repair"}, false,
	)

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get agent work next: %v", err)
	}

	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	for _, task := range tasks {
		if task.TaskID != carrierID {
			continue
		}
		if task.Status != model.TaskStatusCancelled {
			t.Fatalf("claim-repair sidecar status=%s, want CANCELLED (ref task is terminal)", task.Status)
		}
		return
	}
	t.Fatalf("claim-repair sidecar %s not found", carrierID)
}

// TestAgentWorkNextReapsExpiredPatchQueueClaim locks RPF-58C: a CLAIMED patch-queue item whose
// lease has expired is auto-released to PROPOSED on the next work-next poll (the only non-
// voluntary unwedger for a reviewer that claimed-then-wandered, as in R58), with a durable
// claim_expired event. Operation-bound items are NOT swept (covered by the no-op assertion of a
// re-poll leaving PROPOSED untouched once released).
func TestAgentWorkNextReapsExpiredPatchQueueClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-pq-claim-reaper"
		projectID   = "project-pq-claim-reaper"
		repoID      = "repo-pq-claim-reaper"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "epsilon"
		taskID      = "task-pq-claim-reaper-impl"
		branchID    = "branch-pq-claim-reaper"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               builderID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign builder role: %v", err)
	}
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, taskID,
		"Implement reaper lane", "Lane task whose branch is submitted then its review claim expires.",
		"implementation", []string{"rq", "implementation"}, true,
	)
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, builderID, branchID)
	branch = rebindReadyBranchActiveTaskForTest(t, ctx, store, branch, taskID, builderID, `{"paths":["src/**"]}`)

	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-" + taskID,
		RunID:                    "run-" + taskID,
		AgentID:                  builderID,
		CapabilitySnapshotID:     "cap-" + taskID,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 `C:\fixtures\agents\beta\` + branchID,
		BaseTreeHash:             strings.Repeat("a", 40),
		BaseFileHashes:           agentWorkTestBaseFileHashesForScope(`{"paths":["src/**"]}`),
		RepoLeaseID:              "lease-" + taskID,
		LeaseTerm:                7,
		ActorID:                  builderID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", builderID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit patch queue item: %v", err)
	}
	markAgentWorkTaskReleasedWithBranch(t, ctx, store, workspaceID, taskID, builderID, repoID, branch.BranchID)
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
	if claimed.State != sqlite.ProjectPatchQueueStateClaimed {
		t.Fatalf("expected CLAIMED after claim, got %s", claimed.State)
	}
	// Simulate an expired lease: the reviewer claimed then wandered off (R58).
	if _, err := store.WriteDB().ExecContext(ctx,
		`UPDATE project_patch_queue_items SET claim_expires_at = ? WHERE queue_id = ? AND item_id = ?`,
		"2000-01-01T00:00:00Z", claimed.QueueID, claimed.ItemID,
	); err != nil {
		t.Fatalf("force-expire claim: %v", err)
	}

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get agent work next (runs reaper): %v", err)
	}

	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	})
	if err != nil {
		t.Fatalf("list patch queue items: %v", err)
	}
	var got sqlite.ProjectPatchQueueItemRecord
	for _, it := range items {
		if it.QueueID == claimed.QueueID && it.ItemID == claimed.ItemID {
			got = it
			break
		}
	}
	if got.ItemID == "" {
		t.Fatalf("item not found after reaper")
	}
	if got.State != sqlite.ProjectPatchQueueStateProposed {
		t.Fatalf("expired claim not reaped: state=%s, want PROPOSED", got.State)
	}
	if strings.TrimSpace(got.ClaimedBy) != "" || strings.TrimSpace(got.ClaimExpiresAt) != "" {
		t.Fatalf("reaped item still carries claim fields: claimed_by=%q expires=%q", got.ClaimedBy, got.ClaimExpiresAt)
	}
	var eventCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(1) FROM runtime_events WHERE workspace_id = ? AND event_type = 'project.patch_queue.claim_expired' AND entity_id = ?`,
		workspaceID, claimed.QueueID+"/"+claimed.ItemID,
	).Scan(&eventCount); err != nil {
		t.Fatalf("count claim_expired events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("claim_expired event count=%d, want 1", eventCount)
	}
}

// TestReviewerIndependenceGuards locks the S1 R03 caveat fix: a submitter may never ACCEPT its
// own patch queue candidate (storage decide guard), and the review_receipt task for its own item
// is never selectable by the submitter (selection guard). Withdrawing/canceling own work stays
// allowed, and an independent accept is unaffected.
func TestReviewerIndependenceGuards(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-reviewer-independence"
		projectID   = "project-reviewer-independence"
		repoID      = "repo-reviewer-independence"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "epsilon"
		taskID      = "task-reviewer-independence-impl"
		branchID    = "branch-reviewer-independence"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               builderID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign builder role: %v", err)
	}
	// The builder also holds a REVIEWER role: independence must hold even when the submitter is
	// review-capable (that was exactly the R03 shape - role-capable beta self-accepted).
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, builderID, sqlite.ProjectRoleReviewer, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, taskID,
		"Implement reviewer independence lane", "Lane whose candidate must not be self-accepted.",
		"implementation", []string{"rq", "implementation"}, true,
	)
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, builderID, branchID)
	branch = rebindReadyBranchActiveTaskForTest(t, ctx, store, branch, taskID, builderID, `{"paths":["src/**"]}`)

	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-" + taskID,
		RunID:                    "run-" + taskID,
		AgentID:                  builderID,
		CapabilitySnapshotID:     "cap-" + taskID,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 `C:\fixtures\agents\beta\` + branchID,
		BaseTreeHash:             strings.Repeat("a", 40),
		BaseFileHashes:           agentWorkTestBaseFileHashesForScope(`{"paths":["src/**"]}`),
		RepoLeaseID:              "lease-" + taskID,
		LeaseTerm:                7,
		ActorID:                  builderID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", builderID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit patch queue item: %v", err)
	}
	markAgentWorkTaskReleasedWithBranch(t, ctx, store, workspaceID, taskID, builderID, repoID, branch.BranchID)

	// --- Selection guard: a review_receipt task for this item is invisible to the submitter.
	const reviewTaskID = "task-review-reviewer-independence"
	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, reviewTaskID,
		"Review candidate", "Review the submitted candidate.",
		"review", []string{"review", "reviewer", "patch_queue", "project"}, false,
	)
	reviewReq := string(mustTestJSON(t, map[string]any{
		"patch_queue_task_kind": "review_receipt",
		"required_tool":         "project_patch_queue_lifecycle",
		"project_id":            projectID,
		"queue_id":              item.QueueID,
		"item_id":               item.ItemID,
		"branch_id":             item.BranchID,
		"head_sha":              item.HeadSHA,
	}))
	if _, err := store.WriteDB().ExecContext(ctx,
		`UPDATE tasks SET task_requirements_json = ? WHERE task_id = ?`, reviewReq, reviewTaskID,
	); err != nil {
		t.Fatalf("set review task requirements: %v", err)
	}
	builderWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("builder work next: %v", err)
	}
	if builderWork.HasWork && builderWork.Task != nil && builderWork.Task.TaskID == reviewTaskID {
		t.Fatalf("submitter must not be offered the review task for its own item, got %+v", builderWork.Task)
	}
	reviewerWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("reviewer work next: %v", err)
	}
	if !reviewerWork.HasWork || reviewerWork.Task == nil || reviewerWork.Task.TaskID != reviewTaskID {
		t.Fatalf("independent reviewer should be offered the review task, got %+v", reviewerWork)
	}

	// --- Decide guard: self-accept rejected; independent accept allowed.
	claimedBySubmitter, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		LeaseSeconds:          900,
		ActorID:               builderID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", builderID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("submitter claims own item (allowed, e.g. to cancel): %v", err)
	}
	_, _, err = store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateAccepted,
		DecisionSummary:       "Self-accept must be rejected.",
		ClaimToken:            claimedBySubmitter.ClaimToken,
		ActorID:               builderID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", builderID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err == nil {
		t.Fatalf("expected self-accept to be rejected")
	}
	if !strings.Contains(err.Error(), "cannot accept own patch queue candidate") {
		t.Fatalf("unexpected self-accept error: %v", err)
	}

	// Submitter releases; the independent reviewer claims and accepts - unaffected.
	if _, _, err := store.ReleaseProjectPatchQueueClaimWithEvent(ctx, sqlite.ProjectPatchQueueReleaseInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            claimedBySubmitter.ClaimToken,
		ActorID:               builderID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.release", "server_rpc", workspaceID, "agent", builderID),
		PromptContextSurface:  "project.patch_queue.release",
	}); err != nil {
		t.Fatalf("submitter releases own claim: %v", err)
	}
	claimedByReviewer, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
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
		t.Fatalf("reviewer claims item: %v", err)
	}
	accepted, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateAccepted,
		DecisionSummary:       "Independent accept stays allowed.",
		ClaimToken:            claimedByReviewer.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("independent reviewer accept: %v", err)
	}
	if accepted.State != sqlite.ProjectPatchQueueStateAccepted || accepted.DecidedBy != reviewerID {
		t.Fatalf("unexpected accepted item: state=%s decided_by=%s", accepted.State, accepted.DecidedBy)
	}
}

func mustTestJSON(t *testing.T, value map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test json: %v", err)
	}
	return raw
}

// TestRequiredReceiptCancelsRevisionFollowupBehindIntegratedItem locks FF-R09-2: a patch-queue
// REVISION follow-up whose candidate item was integrated in place (the R57 flow) must cancel via
// the receipt sweep instead of staying PENDING-selectable. In the S1 rerun R08 one such stale
// follow-up consumed 14 implementer iterations across two sessions without terminalizing.
func TestRequiredReceiptCancelsRevisionFollowupBehindIntegratedItem(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-revision-followup-terminal-item"
		projectID   = "project-revision-followup-terminal-item"
		repoID      = "repo-revision-followup-terminal-item"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "epsilon"
		taskID      = "task-revision-followup-impl"
		branchID    = "branch-revision-followup"
		followupID  = "task-patchq-revision-followup-stale"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               builderID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign builder role: %v", err)
	}
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, taskID,
		"Implement revision-followup lane", "Lane whose candidate gets integrated in place.",
		"implementation", []string{"rq", "implementation"}, true,
	)
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, builderID, branchID)
	branch = rebindReadyBranchActiveTaskForTest(t, ctx, store, branch, taskID, builderID, `{"paths":["src/**"]}`)

	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-" + taskID,
		RunID:                    "run-" + taskID,
		AgentID:                  builderID,
		CapabilitySnapshotID:     "cap-" + taskID,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 `C:\fixtures\agents\beta\` + branchID,
		BaseTreeHash:             strings.Repeat("a", 40),
		BaseFileHashes:           agentWorkTestBaseFileHashesForScope(`{"paths":["src/**"]}`),
		RepoLeaseID:              "lease-" + taskID,
		LeaseTerm:                7,
		ActorID:                  builderID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", builderID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit patch queue item: %v", err)
	}
	markAgentWorkTaskReleasedWithBranch(t, ctx, store, workspaceID, taskID, builderID, repoID, branch.BranchID)

	// Stale revision follow-up referencing this exact queue/item (legacy unstamped shape:
	// tags + description text fields, no structured kind requirement).
	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, followupID,
		"Revise blocked patch queue candidate",
		"Revision follow-up for the blocked candidate.\nPatch queue: "+item.QueueID+"/"+item.ItemID+"\nBranch ID: "+branch.BranchID,
		"implementation", []string{"patch_queue", "revision", "project"}, true,
	)

	// The candidate is then integrated IN PLACE (claim -> accept by reviewer -> integration).
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
		t.Fatalf("claim item: %v", err)
	}
	accepted, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateAccepted,
		DecisionSummary:       "Accepted revised candidate in place.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("accept item: %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               accepted.QueueID,
		ItemID:                accepted.ItemID,
		ActorID:               reviewerID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetBranch:          "main",
		TargetHeadBefore:      strings.Repeat("c", 40),
		TargetHeadAfter:       strings.Repeat("d", 40),
		RemoteTargetHeadAfter: strings.Repeat("d", 40),
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		PushAttempted:         true,
		PushSucceeded:         true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.integration_record", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.integration_record",
	}); err != nil {
		t.Fatalf("record integration: %v", err)
	}

	// Sweep runs on the next poll: the stale revision follow-up must CANCEL.
	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	for _, taskRecord := range tasks {
		if taskRecord.TaskID != followupID {
			continue
		}
		if taskRecord.Status != model.TaskStatusCancelled {
			t.Fatalf("stale revision follow-up status=%s, want CANCELLED", taskRecord.Status)
		}
		return
	}
	t.Fatalf("revision follow-up %s not found", followupID)
}

// TestRequiredReceiptCancelsBlockedRevisionFollowupBehindIntegratedSuccessor locks R23-F2:
// when a revision follow-up blocks during cleanup but its historical BLOCKED item is
// superseded by a fresh successor that reaches INTEGRATED, the blocked claim must not
// keep the stale RUNNING task outside the receipt sweep.
func TestRequiredReceiptCancelsBlockedRevisionFollowupBehindIntegratedSuccessor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-revision-followup-integrated-successor"
		projectID   = "project-revision-followup-integrated-successor"
		repoID      = "repo-revision-followup-integrated-successor"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "epsilon"
		followupID  = "task-patchq-revision-followup-blocked-successor"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               builderID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["internal/eval/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign builder role: %v", err)
	}
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)

	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "implementation lane may start",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase: %v", err)
	}

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "branch-revision-successor", builderID, reviewerID, `{"paths":["internal/eval/**"]}`, sqlite.ProjectPatchQueueStateBlocked)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	requirements := mustTestJSON(t, map[string]any{
		"schema":                "task_requirements.v1",
		"patch_queue_task_kind": "revision",
		"project_id":            projectID,
		"queue_id":              item.QueueID,
		"item_id":               item.ItemID,
		"branch_id":             item.BranchID,
		"head_sha":              item.HeadSHA,
		"write_scope_hints":     []string{"internal/eval/**"},
	})
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               followupID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Unblock integration candidate " + item.ItemID,
		Description:          "Patch queue: " + item.QueueID + "/" + item.ItemID + "\nBranch ID: " + item.BranchID + "\nHead SHA: " + item.HeadSHA,
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         "integration",
		Tags:                 []string{"project", "patch-queue", "revision", "blocked", "owner-bound-kind:patch_queue_revision"},
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: string(requirements),
		WriteScopeHints:      []string{"internal/eval/**"},
	}, graph); err != nil {
		t.Fatalf("create blocked revision follow-up task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: followupID, LinkedBy: leadID}); err != nil {
		t.Fatalf("attach blocked revision follow-up task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                followupID,
		AgentID:               builderID,
		RepoID:                repoID,
		BranchID:              item.BranchID,
		WriteScopeJSON:        `{"paths":["internal/eval/**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claim revision follow-up before cleanup blocks",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, followupID, builderID),
	}); err != nil {
		t.Fatalf("claim revision follow-up: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx,
		`UPDATE task_claims SET claim_status = ?, summary = ?, updated_at = ? WHERE workspace_id = ? AND task_id = ?`,
		model.TaskClaimStatusBlocked,
		"budget.release cleanup blocked after successor publication",
		"2026-06-12T09:00:00Z",
		workspaceID,
		followupID,
	); err != nil {
		t.Fatalf("mark revision follow-up claim blocked: %v", err)
	}
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		IncludeInactive: true,
	})
	if err != nil {
		t.Fatalf("list branches before supersede: %v", err)
	}
	var sourceBranch sqlite.ProjectBranchRecord
	for _, branch := range branches {
		if branch.BranchID == item.BranchID {
			sourceBranch = branch
			break
		}
	}
	if sourceBranch.BranchID == "" {
		t.Fatalf("source branch %s not found before supersede", item.BranchID)
	}
	rebindReadyBranchActiveTaskForTest(t, ctx, store, sourceBranch, item.TaskID, builderID, sourceBranch.WriteScopeJSON)

	evidenceDocKey := "project.revision-successor.validation"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Revision Successor Validation",
		Content:     "Fresh validation passed. browser smoke: passed for queue_id " + item.QueueID + " item_id " + item.ItemID + " branch_id " + item.BranchID + " head_sha " + item.HeadSHA + ".",
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write successor evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force evidence timestamp: %v", err)
	}
	successor, _, alreadyQueued, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		NewItemID:             item.ItemID + "-rev2",
		EvidenceDocKey:        evidenceDocKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	})
	if err != nil {
		t.Fatalf("supersede blocked item: %v", err)
	}
	if alreadyQueued || successor.SupersedesItemID != item.ItemID {
		t.Fatalf("expected fresh successor, already=%v successor=%+v", alreadyQueued, successor)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               successor.QueueID,
		ItemID:                successor.ItemID,
		LeaseSeconds:          900,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim successor: %v", err)
	}
	accepted, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateAccepted,
		DecisionSummary:       "Fresh successor accepted.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("accept successor: %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               accepted.QueueID,
		ItemID:                accepted.ItemID,
		ActorID:               reviewerID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetBranch:          "main",
		TargetHeadBefore:      strings.Repeat("c", 40),
		TargetHeadAfter:       strings.Repeat("d", 40),
		RemoteTargetHeadAfter: strings.Repeat("d", 40),
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		PushAttempted:         true,
		PushSucceeded:         true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.integration_record", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.integration_record",
	}); err != nil {
		t.Fatalf("integrate successor: %v", err)
	}

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	for _, taskRecord := range tasks {
		if taskRecord.TaskID != followupID {
			continue
		}
		if taskRecord.Status != model.TaskStatusCancelled {
			t.Fatalf("blocked stale revision follow-up status=%s, want CANCELLED", taskRecord.Status)
		}
		return
	}
	t.Fatalf("revision follow-up %s not found", followupID)
}

// TestRequiredReceiptCancelsPostIntegrationScopeSidecar locks the R19 S1 stop:
// a loose downstream implementation sidecar created after an integration continuation
// must not outlive a later durable INTEGRATED receipt that covers the same remaining
// scope. The sidecar's own upstream item is excluded so the sweep does not cancel it
// merely because its prerequisite integration completed.
func TestRequiredReceiptCancelsPostIntegrationScopeSidecar(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID      = "ws-post-integration-scope-sidecar"
		projectID        = "project-post-integration-scope-sidecar"
		repoID           = "repo-post-integration-scope-sidecar"
		leadID           = "alpha"
		builderID        = "beta"
		reviewerID       = "epsilon"
		integratorID     = "zeta"
		upstreamBranchID = "branch-post-integration-upstream"
		readmeBranchID   = "branch-post-integration-readme"
		sidecarID        = "task-post-integration-readme-sidecar"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               builderID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["README.md","cmd/**","internal/**","docs/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign builder role: %v", err)
	}
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)

	integrate := func(item sqlite.ProjectPatchQueueItemRecord) sqlite.ProjectPatchQueueItemRecord {
		t.Helper()
		integrated, _, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			QueueID:               item.QueueID,
			ItemID:                item.ItemID,
			ActorID:               integratorID,
			ActorType:             "agent",
			Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
			TargetBranch:          "main",
			TargetHeadBefore:      strings.Repeat("c", 40),
			TargetHeadAfter:       strings.Repeat("d", 40),
			RemoteTargetHeadAfter: strings.Repeat("d", 40),
			IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
			PushAttempted:         true,
			PushSucceeded:         true,
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.integration_record", "server_rpc", workspaceID, "agent", integratorID),
			PromptContextSurface:  "project.patch_queue.integration_record",
		})
		if err != nil {
			t.Fatalf("integrate %s/%s: %v", item.QueueID, item.ItemID, err)
		}
		return integrated
	}

	upstream := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, upstreamBranchID, builderID, reviewerID, `{"paths":["README.md","cmd/**","internal/**"]}`, sqlite.ProjectPatchQueueStateAccepted)
	integratedUpstream := integrate(upstream)
	continuations, err := store.ListProjectPatchQueueDecisionContinuations(ctx, sqlite.ProjectPatchQueueDecisionContinuationFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     upstream.QueueID,
		ItemID:      upstream.ItemID,
	})
	if err != nil {
		t.Fatalf("list upstream continuation: %v", err)
	}
	if len(continuations) != 1 || strings.TrimSpace(continuations[0].ContinuationTaskID) == "" {
		t.Fatalf("expected upstream integration continuation, got %+v", continuations)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	requirements := string(mustTestJSON(t, map[string]any{
		"schema":                       "task_requirements.v1",
		"advisory_dependency_task_ids": []string{continuations[0].ContinuationTaskID},
		"write_scope_hints":            []string{"README.md", "docs/**"},
	}))
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               sidecarID,
		OwnerUserID:          builderID,
		Priority:             "high",
		Title:                "Resume README docs after integration",
		Description:          "Loose downstream sidecar whose own upstream item is already integrated.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: requirements,
		WriteScopeHints:      []string{"README.md", "docs/**"},
	}, graph); err != nil {
		t.Fatalf("create downstream sidecar: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: sidecarID, LinkedBy: builderID}); err != nil {
		t.Fatalf("attach downstream sidecar: %v", err)
	}

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get work with only upstream integrated item: %v", err)
	}
	sidecar, err := store.GetTaskStatus(ctx, workspaceID, sidecarID)
	if err != nil {
		t.Fatalf("get sidecar before covering receipt: %v", err)
	}
	if sidecar.Status != model.TaskStatusPending {
		t.Fatalf("sidecar status=%s, want PENDING while only upstream item %s/%s is integrated", sidecar.Status, integratedUpstream.QueueID, integratedUpstream.ItemID)
	}

	readmeItem := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, readmeBranchID, builderID, reviewerID, `{"paths":["README.md","cmd/rq/**"]}`, sqlite.ProjectPatchQueueStateAccepted)
	integrate(readmeItem)

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get work after covering integrated receipt: %v", err)
	}
	sidecar, err = store.GetTaskStatus(ctx, workspaceID, sidecarID)
	if err != nil {
		t.Fatalf("get sidecar after covering receipt: %v", err)
	}
	if sidecar.Status != model.TaskStatusCancelled {
		t.Fatalf("sidecar status=%s, want CANCELLED after later integrated README scope receipt", sidecar.Status)
	}
	var eventCount int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM audit_events
 WHERE event_type = 'task_required_receipt_terminalized'
   AND entity_id = ?
   AND payload_json LIKE '%integrated_patch_queue_scope_receipt%'`, sidecarID).Scan(&eventCount); err != nil {
		t.Fatalf("count sidecar terminalization audit: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("integrated scope receipt audit count=%d, want 1", eventCount)
	}
}

func TestRequiredReceiptCancelsIntegratedAcceptanceContractDuplicate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-integrated-acceptance-duplicate"
		projectID    = "project-integrated-acceptance-duplicate"
		repoID       = "repo-integrated-acceptance-duplicate"
		leadID       = "alpha"
		builderID    = "beta"
		reviewerID   = "epsilon"
		integratorID = "zeta"
		branchID     = "branch-integrated-eval-builtins"
		duplicateID  = "task-integrated-eval-builtins-duplicate"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, builderID, leadID, `{"paths":["README.md","internal/eval/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	sourceTaskID := "task-" + branchID
	integratedCriteria := []map[string]string{
		{"criterion": "`values()` returns the implemented object value set and handles empty/edge inputs consistently", "evidence": "focused evaluator tests"},
		{"criterion": "`type()` returns the documented type results for primitive and composite inputs", "evidence": "focused evaluator tests"},
		{"criterion": "`contains` edge cases are defined and tested for empty, missing, and coercion-adjacent inputs", "evidence": "focused evaluator tests"},
		{"criterion": "`map`/`filter` surface lambda errors in the documented way", "evidence": "focused evaluator tests"},
		{"criterion": "coercion rules are implemented consistently across evaluated expressions", "evidence": "focused evaluator tests"},
		{"criterion": "division-by-zero and out-of-bounds behavior match the documented semantics", "evidence": "focused evaluator tests"},
		{"criterion": "README/docs examples are updated to reflect the actual behavior", "evidence": "docs review and diff"},
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              sourceTaskID,
		OwnerUserID:         "operator",
		Priority:            "high",
		Title:               "Extend evaluator builtins and coercion docs",
		Description:         "Build on the working evaluator: values(), type(), contains edge cases, map/filter lambda errors, coercion rules, division-by-zero and out-of-bounds behavior. Lock each decision with tests.",
		TaskKind:            model.TaskKindExecution,
		TaskTemplate:        model.TaskTemplateGeneric,
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		TaskRequirementsJSON: string(mustTestJSON(t, map[string]any{
			"schema":                      "product_first_task_requirements.v1",
			"product_slice":               "eval_builtins",
			"acceptance_criteria_mapping": integratedCriteria,
			"acceptance_commands":         []string{"go build ./...", "go test ./..."},
		})),
		WriteScopeHints: []string{"README.md", "internal/eval/**"},
	}, graph); err != nil {
		t.Fatalf("create integrated source task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: sourceTaskID, LinkedBy: "operator"}); err != nil {
		t.Fatalf("attach integrated source task: %v", err)
	}
	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, builderID, reviewerID, `{"paths":["README.md","internal/eval/**"]}`, sqlite.ProjectPatchQueueStateAccepted)
	integrateAgentWorkPatchQueueItem(t, ctx, store, workspaceID, projectID, integratorID, item)

	duplicateRequirements := string(mustTestJSON(t, map[string]any{
		"schema":                      "task_requirements.v1",
		"acceptance_criteria_mapping": integratedCriteria,
		"required_work_modes":         []string{"implementation", "validation"},
		"required_tools":              []string{"shell"},
		"write_scope_hints":           []string{"README.md", "internal/eval/**"},
	}))
	if err := createAgentWorkTaskBypassingSubmitGate(t, ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               duplicateID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Implement evaluator builtin edge cases and coercion docs",
		Description:          "Duplicate empty-frontier implementation task whose completion contract is already satisfied by the integrated evaluator lane.",
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: duplicateRequirements,
		WriteScopeHints:      []string{"README.md", "internal/eval/**"},
	}, graph); err != nil {
		t.Fatalf("create duplicate task bypassing submit gate: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: duplicateID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach duplicate task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT OR REPLACE INTO task_claims(
  task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at, checkout_id, branch_id, write_scope_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		duplicateID,
		workspaceID,
		builderID,
		model.TaskClaimStatusReleased,
		"selected once, released before producing a patch queue candidate; branch/checkout are stale claim-admission residue",
		"2026-06-13T06:57:30Z",
		"2026-06-13T06:59:55Z",
		"2026-06-13T06:59:55Z",
		"projcheckout-r41-stale-duplicate",
		"projbranch-r41-stale-duplicate",
		`{"paths":["README.md","internal/eval/**"]}`,
	); err != nil {
		t.Fatalf("seed released duplicate claim: %v", err)
	}

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get work after integrated acceptance coverage receipt: %v", err)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, duplicateID)
	if err != nil {
		t.Fatalf("get duplicate task status: %v", err)
	}
	if status.Status != model.TaskStatusCancelled {
		t.Fatalf("duplicate status=%s, want CANCELLED", status.Status)
	}
	var eventCount int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM audit_events
 WHERE event_type = 'task_required_receipt_terminalized'
   AND entity_id = ?
   AND payload_json LIKE '%integrated_acceptance_contract_receipt%'`, duplicateID).Scan(&eventCount); err != nil {
		t.Fatalf("count duplicate terminalization audit: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("integrated acceptance receipt audit count=%d, want 1", eventCount)
	}
}

func TestRequiredReceiptCancelsIntegratedBranchEvidenceReconcileTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-integrated-branch-evidence-reconcile"
		projectID    = "project-integrated-branch-evidence-reconcile"
		repoID       = "repo-integrated-branch-evidence-reconcile"
		leadID       = "alpha"
		builderID    = "beta"
		reviewerID   = "epsilon"
		integratorID = "zeta"
		branchID     = "branch-integrated-reconcile-target"
		reconcileID  = "task-evidence-reconcile-integrated-branch"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, builderID, leadID, `{"paths":["README.md","internal/eval/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	sourceTaskID := "task-" + branchID
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               sourceTaskID,
		OwnerUserID:          "operator",
		Priority:             "high",
		Title:                "Extend evaluator builtins and coercion docs",
		Description:          "Build on the working evaluator and lock each decision with tests.",
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: `{"schema":"product_first_task_requirements.v1","product_slice":"eval_builtins"}`,
		WriteScopeHints:      []string{"README.md", "internal/eval/**"},
	}, graph); err != nil {
		t.Fatalf("create integrated source task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: sourceTaskID, LinkedBy: "operator"}); err != nil {
		t.Fatalf("attach integrated source task: %v", err)
	}
	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, builderID, reviewerID, `{"paths":["README.md","internal/eval/**"]}`, sqlite.ProjectPatchQueueStateAccepted)
	integrateAgentWorkPatchQueueItem(t, ctx, store, workspaceID, projectID, integratorID, item)

	reconcileRequirements := string(mustTestJSON(t, map[string]any{
		"schema":                 "project_branch_commit_evidence_reconciliation_task.v1",
		"source_tool":            "project_branch_commit",
		"mutation_attempt_ref":   "mutation-attempt:integrated-reconcile-target",
		"mutation_attempt_state": "NEEDS_RECONCILIATION",
		"branch_id":              branchID,
		"branch_name":            "agent-beta-integrated-reconcile-target",
		"checkout_id":            "checkout-integrated-reconcile-target",
		"base_sha":               "base-before-integrated",
		"head_sha":               "stale-local-head",
		"committed_paths":        []string{"README.md", "internal/eval/eval_additional_test.go"},
	}))
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               reconcileID,
		OwnerUserID:          "system",
		Priority:             "high",
		Title:                "Reconcile project branch evidence for integrated branch",
		Description:          "A project_branch_commit mutation materialized a local commit but the referenced branch is already integrated.",
		TaskKind:             model.TaskKindCoordination,
		TaskTemplate:         model.TaskTemplateGeneric,
		TaskClass:            "INCIDENT",
		ProjectID:            projectID,
		ProjectLane:          "coordination",
		TaskRequirementsJSON: reconcileRequirements,
	}, graph); err != nil {
		t.Fatalf("create branch evidence reconcile task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: reconcileID, LinkedBy: "system"}); err != nil {
		t.Fatalf("attach branch evidence reconcile task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT OR REPLACE INTO task_claims(
  task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		reconcileID,
		workspaceID,
		reviewerID,
		model.TaskClaimStatusReleased,
		"selected once, released after discovering integrated branch receipt",
		"2026-06-13T08:51:49Z",
		"2026-06-13T08:58:24Z",
		"2026-06-13T08:58:24Z",
	); err != nil {
		t.Fatalf("seed released reconcile claim: %v", err)
	}

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          reviewerID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get work after integrated branch evidence receipt: %v", err)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, reconcileID)
	if err != nil {
		t.Fatalf("get reconcile task status: %v", err)
	}
	if status.Status != model.TaskStatusCancelled {
		t.Fatalf("reconcile status=%s, want CANCELLED", status.Status)
	}
	var eventCount int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM audit_events
 WHERE event_type = 'task_required_receipt_terminalized'
   AND entity_id = ?
   AND payload_json LIKE '%integrated_branch_evidence_reconcile_receipt%'`, reconcileID).Scan(&eventCount); err != nil {
		t.Fatalf("count branch evidence reconcile terminalization audit: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("integrated branch evidence reconcile audit count=%d, want 1", eventCount)
	}
}

func TestRequiredReceiptKeepsIntegratedAcceptanceContractImprovementDelta(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-integrated-acceptance-improvement"
		projectID     = "project-integrated-acceptance-improvement"
		repoID        = "repo-integrated-acceptance-improvement"
		leadID        = "alpha"
		builderID     = "beta"
		reviewerID    = "epsilon"
		integratorID  = "zeta"
		branchID      = "branch-integrated-parser-docs"
		improvementID = "task-integrated-parser-streaming-delta"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, builderID, leadID, `{"paths":["README.md","internal/eval/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "execute", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	sourceTaskID := "task-" + branchID
	sourceRequirements := string(mustTestJSON(t, map[string]any{
		"schema": "task_requirements.v1",
		"acceptance_criteria_mapping": []string{
			"parser operator precedence tests",
			"parser grouping docs",
		},
	}))
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               sourceTaskID,
		OwnerUserID:          "operator",
		Priority:             "high",
		Title:                "Extend parser/operator semantics",
		Description:          "Implement parser operator precedence tests and grouping docs.",
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: sourceRequirements,
		WriteScopeHints:      []string{"README.md", "internal/eval/**"},
	}, graph); err != nil {
		t.Fatalf("create integrated parser source task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: sourceTaskID, LinkedBy: "operator"}); err != nil {
		t.Fatalf("attach integrated parser source task: %v", err)
	}
	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, builderID, reviewerID, `{"paths":["README.md","internal/eval/**"]}`, sqlite.ProjectPatchQueueStateAccepted)
	integrateAgentWorkPatchQueueItem(t, ctx, store, workspaceID, projectID, integratorID, item)

	improvementRequirements := string(mustTestJSON(t, map[string]any{
		"schema": "task_requirements.v1",
		"acceptance_criteria_mapping": []map[string]string{
			{"criterion": "parser operator precedence tests", "evidence": "focused parser tests"},
			{"criterion": "streaming parser cancellation tests", "evidence": "focused parser tests"},
		},
		"write_scope_hints": []string{"README.md", "internal/eval/**"},
	}))
	if err := createAgentWorkTaskBypassingSubmitGate(t, ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               improvementID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Add streaming parser cancellation coverage",
		Description:          "Same slice and pathset as a prior integrated parser item, but with one uncovered acceptance criterion.",
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: improvementRequirements,
		WriteScopeHints:      []string{"README.md", "internal/eval/**"},
	}, graph); err != nil {
		t.Fatalf("create improvement task bypassing submit gate: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: improvementID, LinkedBy: "developer"}); err != nil {
		t.Fatalf("attach improvement task: %v", err)
	}

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get work with uncovered improvement delta: %v", err)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, improvementID)
	if err != nil {
		t.Fatalf("get improvement task status: %v", err)
	}
	if status.Status == model.TaskStatusCancelled {
		t.Fatalf("improvement task with uncovered acceptance criterion was cancelled")
	}
}

// TestRequiredReceiptResolvesSideEffectLaneWithQuarantineDecision locks R22-F3: a side-effect
// resolution lane whose durable quarantine decision is already recorded (agent_updates row of
// type side_effect_resolution) terminalizes via the sweep instead of being re-selected and
// re-demanding side_effect_resolve every fresh cycle - the loop that pinned both reviewers in
// R22 and starved review capacity.
func TestRequiredReceiptResolvesSideEffectLaneWithQuarantineDecision(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-side-effect-quarantine-receipt"
		projectID   = "project-side-effect-quarantine-receipt"
		leadID      = "alpha"
		agentID     = "zeta"
		laneID      = "task-side-effect-664fquarantine"
		sideRef     = "side-effect:ws-side-effect-quarantine-receipt:project-side-effect-quarantine-receipt:branch-x:internal/eval/eval.go"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, agentID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, laneID,
		"Resolve pending side effect",
		"Side-effect resolution lane.\nRefs: "+sideRef+"\nRecord a durable ABPC decision.",
		"integration", []string{"side-effect", "abpc"}, false,
	)
	// Durable decision already recorded (the R22 shape: decision exists, lane keeps looping).
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "side_effect_resolution",
		Summary:     "side_effect_resolve quarantine for " + sideRef,
		PayloadJSON: `{"decision":"quarantine"}`,
	}); err != nil {
		t.Fatalf("post side_effect_resolution update: %v", err)
	}

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          agentID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	for _, task := range tasks {
		if task.TaskID != laneID {
			continue
		}
		if task.Status != model.TaskStatusResolved {
			t.Fatalf("side-effect lane status=%s, want RESOLVED (quarantine decision recorded)", task.Status)
		}
		return
	}
	t.Fatalf("side-effect lane %s not found", laneID)
}

// TestClaimRepairCancelsWhenBlockedTaskReclaimed locks R22-F1: a claim-repair carrier whose
// blocked task now holds an ACTIVE claim (the repair purpose achieved) terminalizes instead of
// looping its required-transition gate into operator blockers.
func TestClaimRepairCancelsWhenBlockedTaskReclaimed(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-claim-repair-reclaimed"
		projectID   = "project-claim-repair-reclaimed"
		leadID      = "alpha"
		builderID   = "beta"
		blockedID   = "task-claim-repair-reclaimed-target"
		carrierID   = "task-project-claim-repair-reclaimed-test"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, blockedID,
		"Previously blocked product lane",
		"The lane the repair carrier was opened for.",
		"coordination", []string{"coordination"}, false,
	)
	createAgentWorkProjectExecutionTaskWithDescription(
		t, ctx, store, workspaceID, projectID, carrierID,
		"Repair blocked project claim",
		"Claim repair sidecar.\n- blocked_task_id: "+blockedID,
		"coordination", []string{"project-claim-repair"}, false,
	)
	// The blocked task is successfully claimed again - the repair purpose is achieved.
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      blockedID,
		AgentID:     builderID,
		Summary:     "reclaimed after scope conflict cleared",
	}); err != nil {
		t.Fatalf("reclaim blocked task: %v", err)
	}

	if _, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	}); err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	for _, task := range tasks {
		if task.TaskID != carrierID {
			continue
		}
		if task.Status != model.TaskStatusCancelled {
			t.Fatalf("claim-repair carrier status=%s, want CANCELLED (blocked task re-claimed)", task.Status)
		}
		return
	}
	t.Fatalf("claim-repair carrier %s not found", carrierID)
}
