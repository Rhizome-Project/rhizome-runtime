package sqlite_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectPatchQueueListAnnotatesReviewTaskReceipt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-review-task-read-model"
		projectID   = "project-patchq-review-task-read-model"
		repoID      = "repo-patchq-review-task-read-model"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "gamma"
		branchID    = "branch-patchq-review-task-read-model"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
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
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{WorkspaceID: workspaceID, ProjectID: projectID})
	if err != nil {
		t.Fatalf("list patch queue items: %v", err)
	}
	if len(items) != 1 || items[0].ReviewTaskID == "" || items[0].ReviewTaskStatus == "" || items[0].ReviewTaskEventID == "" || items[0].MissingReviewTask {
		t.Fatalf("expected review task receipt fields, got %+v", items)
	}
	if items[0].ReviewTaskID != sqlite.ProjectPatchQueueReviewTaskID(item) {
		t.Fatalf("review_task_id = %q, want deterministic %q", items[0].ReviewTaskID, sqlite.ProjectPatchQueueReviewTaskID(item))
	}

	reviewTaskID := items[0].ReviewTaskID
	reviewTask, err := store.GetTaskStatus(ctx, workspaceID, reviewTaskID)
	if err != nil {
		t.Fatalf("get review task: %v", err)
	}
	for _, want := range []string{
		"lane-scoped candidate",
		"Integration boundary",
		"Full-product completeness must be checked after accepted lanes are assembled",
		"Advanced operation/CAS/materialization receipts belong to integration/materialization follow-up",
		`"review_scope":"lane_scoped_patch_queue_candidate"`,
		`"integration_boundary":"full_product_acceptance_deferred_to_integration_build_verify"`,
	} {
		if !strings.Contains(reviewTask.Description+"\n"+reviewTask.TaskRequirementsJSON, want) {
			t.Fatalf("review task missing %q:\ndescription=%s\nrequirements=%s", want, reviewTask.Description, reviewTask.TaskRequirementsJSON)
		}
	}
	if strings.Contains(reviewTask.Description, "first record the required operation, CAS") {
		t.Fatalf("review task should not require integration receipts before lane decision:\n%s", reviewTask.Description)
	}
	reviewTaskEventID := items[0].ReviewTaskEventID
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM runtime_event_firehose_outbox WHERE workspace_id = ? AND event_id = ?`, workspaceID, reviewTaskEventID); err != nil {
		t.Fatalf("delete review task outbox row: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM runtime_events WHERE event_id = ?`, reviewTaskEventID); err != nil {
		t.Fatalf("delete review task runtime event: %v", err)
	}
	items, err = store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{WorkspaceID: workspaceID, ProjectID: projectID})
	if err != nil {
		t.Fatalf("re-list patch queue items after deleting event: %v", err)
	}
	if len(items) != 1 || !items[0].MissingReviewTask || items[0].ReviewTaskID != "" || items[0].ReviewTaskEventID != "" {
		t.Fatalf("expected missing review task annotation when task.created receipt is absent, got %+v", items)
	}

	status, repairedEventID, repaired, err := store.ReconcileProjectPatchQueueReviewTaskReceipt(ctx, sqlite.ProjectPatchQueueReviewTaskReconcileInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ActorID:     reviewerID,
		ActorType:   "agent",
	})
	if err != nil {
		t.Fatalf("reconcile review task receipt: %v", err)
	}
	if !repaired || status.TaskID != reviewTaskID || repairedEventID == "" || repairedEventID == reviewTaskEventID {
		t.Fatalf("expected reconcile to repair missing event for %s, status=%+v event_id=%q repaired=%v old_event_id=%q", reviewTaskID, status, repairedEventID, repaired, reviewTaskEventID)
	}
	items, err = store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{WorkspaceID: workspaceID, ProjectID: projectID})
	if err != nil {
		t.Fatalf("re-list patch queue items after reconcile: %v", err)
	}
	if len(items) != 1 || items[0].MissingReviewTask || items[0].ReviewTaskID != reviewTaskID || items[0].ReviewTaskEventID != repairedEventID {
		t.Fatalf("expected repaired review task annotation, got %+v", items)
	}

	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM workspace_tasks WHERE workspace_id = ? AND task_id = ?`, workspaceID, reviewTaskID); err != nil {
		t.Fatalf("detach review task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM tasks WHERE task_id = ?`, reviewTaskID); err != nil {
		t.Fatalf("delete review task: %v", err)
	}
	items, err = store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{WorkspaceID: workspaceID, ProjectID: projectID})
	if err != nil {
		t.Fatalf("re-list patch queue items: %v", err)
	}
	if len(items) != 1 || !items[0].MissingReviewTask || items[0].ReviewTaskID != "" || items[0].ReviewTaskStatus != "" {
		t.Fatalf("expected missing review task annotation after orphaning item, got %+v", items)
	}
}

func TestProjectPatchQueueReviewTaskUsesLaneScopedContract(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-review-task-lane-scoped"
		projectID   = "project-patchq-review-task-lane-scoped"
		repoID      = "repo-patchq-review-task-lane-scoped"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "gamma"
		branchID    = "branch-patchq-review-task-lane-scoped"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
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

	status, err := store.GetTaskStatus(ctx, workspaceID, sqlite.ProjectPatchQueueReviewTaskID(item))
	if err != nil {
		t.Fatalf("get review task status: %v", err)
	}
	if status.TaskKind != "COORDINATION" || status.TaskTemplate != "research" || status.ProjectLane != "review" {
		t.Fatalf("review task should be claimable coordination work, got kind=%q template=%q lane=%q", status.TaskKind, status.TaskTemplate, status.ProjectLane)
	}
	executable, err := store.ListExecutableNodes(ctx, 10)
	if err != nil {
		t.Fatalf("list executable nodes: %v", err)
	}
	for _, node := range executable {
		if node.TaskID == status.TaskID {
			t.Fatalf("patch queue review receipt must not be daemon-executable, got node %+v", node)
		}
	}
	for _, want := range []string{
		"lane-scoped candidate",
		"Do not block solely because unrelated sibling lanes or final integration are incomplete.",
		"A correct partial lane may be ACCEPTED for integration",
		"Integration boundary: ACCEPTED means this lane may enter integration.",
	} {
		if !strings.Contains(status.Description, want) {
			t.Fatalf("review task description missing %q:\n%s", want, status.Description)
		}
	}
	for _, stale := range []string{
		"first record the required operation, CAS, materialization, rollback, and reviewer evidence",
		"Judge the branch against the whole product",
	} {
		if strings.Contains(status.Description, stale) {
			t.Fatalf("review task description still contains stale full-integration wording %q:\n%s", stale, status.Description)
		}
	}
	for _, want := range []string{
		`"review_scope":"lane_scoped_patch_queue_candidate"`,
		`"candidate_pathset":["src/**"]`,
		`"integration_boundary":"full_product_acceptance_deferred_to_integration_build_verify"`,
	} {
		if !strings.Contains(status.TaskRequirementsJSON, want) {
			t.Fatalf("review task requirements missing %s: %s", want, status.TaskRequirementsJSON)
		}
	}
}

func TestProjectPatchQueueReviewTaskReconcilesLegacyExecutionReceipt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-review-task-legacy-execution"
		projectID   = "project-patchq-review-task-legacy-execution"
		repoID      = "repo-patchq-review-task-legacy-execution"
		leadID      = "alpha"
		ownerID     = "beta"
		branchID    = "branch-patchq-review-task-legacy-execution"
		queueID     = "patchq-legacy-execution"
		itemID      = "patchitem-legacy-execution"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{WorkspaceID: workspaceID, ProjectID: projectID})
	if err != nil {
		t.Fatalf("list project branches: %v", err)
	}
	if len(branches) != 1 || branches[0].HeadSHA == "" {
		t.Fatalf("expected ready branch with head sha, got %+v", branches)
	}

	legacyIdentity := sqlite.ProjectPatchQueueItemRecord{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     queueID,
		ItemID:      itemID,
		BranchID:    branchID,
		HeadSHA:     branches[0].HeadSHA,
	}
	legacyTaskID := sqlite.ProjectPatchQueueReviewTaskID(legacyIdentity)
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               legacyTaskID,
		OwnerUserID:          ownerID,
		Priority:             "high",
		Title:                "Legacy executable review receipt",
		Description:          "Old builds created patch queue review receipts as execution tasks.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		ProjectID:            projectID,
		ProjectLane:          "review",
		TaskRequirementsJSON: `{"patch_queue_task_kind":"review_receipt","project_id":"` + projectID + `","queue_id":"` + queueID + `","item_id":"` + itemID + `","branch_id":"` + branchID + `","head_sha":"` + branches[0].HeadSHA + `"}`,
	}, dag.DefaultGraph()); err != nil {
		t.Fatalf("create legacy execution review task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      legacyTaskID,
		LinkedBy:    ownerID,
	}); err != nil {
		t.Fatalf("attach legacy execution review task: %v", err)
	}
	executable, err := store.ListExecutableNodes(ctx, 10)
	if err != nil {
		t.Fatalf("list executable nodes after legacy receipt: %v", err)
	}
	for _, node := range executable {
		if node.TaskID == legacyTaskID {
			t.Fatalf("legacy patch queue review receipt must be excluded from daemon execution, got %+v", node)
		}
	}

	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               queueID,
		ItemID:                itemID,
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
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{WorkspaceID: workspaceID, ProjectID: projectID})
	if err != nil {
		t.Fatalf("list patch queue items: %v", err)
	}
	if len(items) != 1 || items[0].ReviewTaskID == "" || items[0].ReviewTaskID == legacyTaskID {
		t.Fatalf("expected replacement review task after legacy execution receipt, item=%+v legacy=%s submitted=%+v", items, legacyTaskID, item)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, items[0].ReviewTaskID)
	if err != nil {
		t.Fatalf("get replacement review task: %v", err)
	}
	if status.TaskKind != "COORDINATION" || status.TaskTemplate != "research" || !strings.Contains(status.TaskRequirementsJSON, `"queue_id":"`+queueID+`"`) {
		t.Fatalf("replacement review task should be coordination receipt, got %+v", status)
	}
}

func TestProjectPatchQueueReviewTaskReuseRequiresExactIdentity(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-review-task-exact-identity"
		projectID   = "project-patchq-review-task-exact-identity"
		repoID      = "repo-patchq-review-task-exact-identity"
		leadID      = "alpha"
		ownerID     = "beta"
		branchID    = "branch-patchq-review-task-exact-identity"
		queueID     = "patchq-exact-identity"
		itemID      = "patchitem-exact-identity"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
	branches, err := store.ListProjectBranches(ctx, sqlite.ProjectBranchListFilter{WorkspaceID: workspaceID, ProjectID: projectID})
	if err != nil {
		t.Fatalf("list project branches: %v", err)
	}
	if len(branches) != 1 || branches[0].HeadSHA == "" {
		t.Fatalf("expected ready branch with head sha, got %+v", branches)
	}

	staleIdentity := sqlite.ProjectPatchQueueItemRecord{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     queueID,
		ItemID:      itemID,
		BranchID:    branchID,
		HeadSHA:     branches[0].HeadSHA,
	}
	wrongHeadSHA := "wrong-" + branches[0].HeadSHA
	baseReviewTaskID := sqlite.ProjectPatchQueueReviewTaskID(staleIdentity)
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               baseReviewTaskID,
		OwnerUserID:          ownerID,
		Priority:             "high",
		Title:                "Stale review task identity",
		Description:          "Same deterministic id but missing exact patch queue identity.",
		ProjectID:            projectID,
		ProjectLane:          "review",
		TaskRequirementsJSON: `{"patch_queue_task_kind":"review_receipt","project_id":"` + projectID + `","queue_id":"` + queueID + `","item_id":"` + itemID + `","branch_id":"` + branchID + `","head_sha":"` + wrongHeadSHA + `"}`,
	}, dag.DefaultGraph()); err != nil {
		t.Fatalf("create stale review task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx,
		`INSERT INTO workspace_tasks(workspace_id, task_id, linked_by, created_at)
		 VALUES (?, ?, ?, datetime('now'))`,
		workspaceID, baseReviewTaskID, ownerID,
	); err != nil {
		t.Fatalf("attach stale review task: %v", err)
	}

	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               queueID,
		ItemID:                itemID,
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
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{WorkspaceID: workspaceID, ProjectID: projectID})
	if err != nil {
		t.Fatalf("list patch queue items: %v", err)
	}
	if len(items) != 1 || items[0].MissingReviewTask || items[0].ReviewTaskID == "" {
		t.Fatalf("expected replacement exact-identity review task receipt, got %+v", items)
	}
	if items[0].ReviewTaskID == sqlite.ProjectPatchQueueReviewTaskID(item) {
		t.Fatalf("expected stale deterministic task id to be skipped, got %+v", items[0])
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, items[0].ReviewTaskID)
	if err != nil {
		t.Fatalf("get replacement review task: %v", err)
	}
	for _, want := range []string{`"head_sha":"` + item.HeadSHA + `"`, `"queue_id":"` + queueID + `"`, `"item_id":"` + itemID + `"`} {
		if !strings.Contains(status.TaskRequirementsJSON, want) {
			t.Fatalf("replacement review task requirements missing %s: %s", want, status.TaskRequirementsJSON)
		}
	}
}

func TestProjectPatchQueueDecisionCreatesContinuationOutbox(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-decision-continuation"
		projectID   = "project-patchq-decision-continuation"
		repoID      = "repo-patchq-decision-continuation"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "reviewer"
		branchID    = "branch-patchq-decision-continuation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, ownerID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	continuations, err := store.ListProjectPatchQueueDecisionContinuations(ctx, sqlite.ProjectPatchQueueDecisionContinuationFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	})
	if err != nil {
		t.Fatalf("list decision continuations: %v", err)
	}
	if len(continuations) != 1 {
		t.Fatalf("expected one decision continuation, got %+v", continuations)
	}
	got := continuations[0]
	// Drain model (stage 4): the owner holds a claimable IMPLEMENTER role, so the revision continuation is
	// route-classified satisfiable_now and materialized eagerly in the decision tx (CONSUMED) - it never rests
	// PENDING-with-no-consumer (#6 oracle is drain-completeness, not a deferred two-phase consume).
	if got.State != "CONSUMED" || got.Decision != sqlite.ProjectPatchQueueStateBlocked || got.FollowupKind != "revision" {
		t.Fatalf("unexpected continuation route: %+v", got)
	}
	wantTaskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(projectID, item, "revision")
	if got.ContinuationTaskID != wantTaskID {
		t.Fatalf("continuation task id = %q, want %q", got.ContinuationTaskID, wantTaskID)
	}
	if strings.TrimSpace(got.DecisionEventID) == "" || !strings.Contains(got.PayloadJSON, "project_patch_queue_decision_continuation_outbox.v1") {
		t.Fatalf("continuation must bind decision event and replay payload, got %+v", got)
	}

	status, consumed, created, err := store.ConsumeProjectPatchQueueDecisionContinuation(ctx, sqlite.ProjectPatchQueueDecisionContinuationConsumeInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ActorID:     reviewerID,
		ActorType:   "agent",
	})
	if err != nil {
		t.Fatalf("consume decision continuation: %v", err)
	}
	// Eager materialization already minted the task in the decision tx, so an explicit consume idempotently
	// REUSES it (created=false) rather than minting a second carrier - same claimable task, still consumed.
	if created || status.TaskID != wantTaskID || consumed.State != "CONSUMED" {
		t.Fatalf("expected consume to reuse the eager-materialized continuation task, status=%+v continuation=%+v created=%v", status, consumed, created)
	}
	if status.TaskKind != "COORDINATION" || status.TaskTemplate != "research" {
		t.Fatalf("decision continuation must be claimable coordination work, got kind=%q template=%q", status.TaskKind, status.TaskTemplate)
	}
	executable, err := store.ListExecutableNodes(ctx, 10)
	if err != nil {
		t.Fatalf("list executable nodes after decision continuation consume: %v", err)
	}
	for _, node := range executable {
		if node.TaskID == status.TaskID {
			t.Fatalf("decision continuation must not be daemon-executable, got node %+v", node)
		}
	}
	if !strings.Contains(status.TaskRequirementsJSON, `"decision_outbox_id":"`+got.OutboxID+`"`) {
		t.Fatalf("continuation task must bind outbox id, requirements=%s", status.TaskRequirementsJSON)
	}
	for _, want := range []string{
		`"required_transition":"project_patch_queue_revision_commit_review_submit"`,
		`"required_first_publication_tool":"project_branch_commit"`,
		`"required_tool_sequence":["project_branch_commit","project_branch_review_ready","project_patch_queue_submit"]`,
		`"required_terminal_tool":"project_patch_queue_submit"`,
		`"historical_source_branch_role":"read_only_defeated_source_branch_evidence"`,
		`"live_repair_branch_required":true`,
	} {
		if !strings.Contains(status.TaskRequirementsJSON, want) {
			t.Fatalf("revision continuation requirements missing %s: %s", want, status.TaskRequirementsJSON)
		}
	}
	if len(status.WriteScopeHints) != 0 {
		t.Fatalf("revision continuation must not claim candidate pathset as write scope hints, got %+v", status.WriteScopeHints)
	}
	if strings.Contains(status.TaskRequirementsJSON, `"write_scope_hints"`) {
		t.Fatalf("revision continuation requirements must not turn candidate pathset into claim scope: %s", status.TaskRequirementsJSON)
	}
	if !strings.Contains(status.TaskRequirementsJSON, `"candidate_pathset":["src/**"]`) ||
		!strings.Contains(status.TaskRequirementsJSON, `"candidate_pathset_role":"historical_changed_path_evidence_not_claim_scope"`) {
		t.Fatalf("revision continuation must retain candidate pathset as evidence, requirements=%s", status.TaskRequirementsJSON)
	}
	if !strings.Contains(status.Description, "Candidate pathset: src/**") ||
		!strings.Contains(status.Description, "not automatic claim scope") {
		t.Fatalf("revision continuation description must keep candidate pathset as evidence, description=%s", status.Description)
	}
	if _, _, createdAgain, err := store.ConsumeProjectPatchQueueDecisionContinuation(ctx, sqlite.ProjectPatchQueueDecisionContinuationConsumeInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		OutboxID:    got.OutboxID,
		ActorID:     reviewerID,
		ActorType:   "agent",
	}); err != nil {
		t.Fatalf("reconsume decision continuation: %v", err)
	} else if createdAgain {
		t.Fatalf("decision continuation consume must be idempotent")
	}
}

func TestProjectPatchQueueAcceptedDecisionMaterializesIntegrationTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-patchq-accepted-integration-materialized"
		projectID    = "project-patchq-accepted-integration-materialized"
		repoID       = "repo-patchq-accepted-integration-materialized"
		leadID       = "alpha"
		ownerID      = "beta"
		reviewerID   = "reviewer"
		integratorID = "zeta"
		branchID     = "branch-patchq-accepted-integration-materialized"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, ownerID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateAccepted)
	continuations, err := store.ListProjectPatchQueueDecisionContinuations(ctx, sqlite.ProjectPatchQueueDecisionContinuationFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	})
	if err != nil {
		t.Fatalf("list accepted decision continuation: %v", err)
	}
	if len(continuations) != 1 {
		t.Fatalf("expected one accepted decision continuation, got %+v", continuations)
	}
	got := continuations[0]
	if got.State != "CONSUMED" || got.Decision != sqlite.ProjectPatchQueueStateAccepted || got.FollowupKind != "integration" {
		t.Fatalf("accepted decision must atomically materialize consumed integration continuation, got %+v", got)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, got.ContinuationTaskID)
	if err != nil {
		t.Fatalf("get accepted integration continuation task: %v", err)
	}
	if status.TaskKind != "COORDINATION" || status.TaskTemplate != "research" {
		t.Fatalf("accepted continuation must be claimable coordination work, got kind=%q template=%q", status.TaskKind, status.TaskTemplate)
	}
	if status.OwnerUserID != integratorID || status.ProjectLane != "integration" || !status.RequiresProjectGate {
		t.Fatalf("accepted continuation should route to active integrator lane, got %+v", status)
	}
	executable, err := store.ListExecutableNodes(ctx, 10)
	if err != nil {
		t.Fatalf("list executable nodes after accepted continuation materialize: %v", err)
	}
	for _, node := range executable {
		if node.TaskID == status.TaskID {
			t.Fatalf("accepted continuation must not be daemon-executable, got node %+v", node)
		}
	}
	for _, want := range []string{
		`"patch_queue_task_kind":"integration"`,
		`"required_project_role":"INTEGRATOR"`,
		`"required_tool":"project_patch_queue_integrate"`,
		`"required_transition":"project_patch_queue_integrate_then_full_product_verify"`,
		`"integration_completion_gate":"canonical_target_build_and_verifier_mesh"`,
	} {
		if !strings.Contains(status.TaskRequirementsJSON, want) {
			t.Fatalf("accepted integration continuation requirements missing %s: %s", want, status.TaskRequirementsJSON)
		}
	}
}

func TestProjectPatchQueueDecisionContinuationRejectsMismatchedExistingTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-decision-continuation-mismatch"
		projectID   = "project-patchq-decision-continuation-mismatch"
		repoID      = "repo-patchq-decision-continuation-mismatch"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "reviewer"
		branchID    = "branch-patchq-decision-continuation-mismatch"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	// Intentionally do NOT assign the owner an IMPLEMENTER role: the revision continuation route-classifies
	// awaiting_role and DEFERS (no eager task minted), which is exactly the window in which a stale/manual task
	// can collide on the deterministic continuation id - the case this test fail-closes on.
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, ownerID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	continuations, err := store.ListProjectPatchQueueDecisionContinuations(ctx, sqlite.ProjectPatchQueueDecisionContinuationFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	})
	if err != nil {
		t.Fatalf("list decision continuations: %v", err)
	}
	if len(continuations) != 1 {
		t.Fatalf("expected one decision continuation, got %+v", continuations)
	}
	continuation := continuations[0]
	if continuation.State != "DEFERRED" {
		t.Fatalf("revision continuation with no claimable owner role must defer (await role), got %+v", continuation)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               continuation.ContinuationTaskID,
		OwnerUserID:          reviewerID,
		Priority:             "high",
		Title:                "Wrong continuation collision",
		Description:          "A stale/manual task reused the deterministic continuation task id with the wrong outbox binding.",
		TaskKind:             "EXECUTION",
		TaskTemplate:         "generic",
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: `{"patch_queue_task_kind":"revision","project_id":"` + projectID + `","queue_id":"` + item.QueueID + `","item_id":"` + item.ItemID + `","branch_id":"` + item.BranchID + `","head_sha":"` + item.HeadSHA + `","decision":"BLOCKED","decision_outbox_id":"wrong-outbox"}`,
	}, dag.DefaultGraph()); err != nil {
		t.Fatalf("create mismatched continuation task collision: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      continuation.ContinuationTaskID,
		LinkedBy:    reviewerID,
	}); err != nil {
		t.Fatalf("attach mismatched continuation task collision: %v", err)
	}

	if _, _, _, err := store.ConsumeProjectPatchQueueDecisionContinuation(ctx, sqlite.ProjectPatchQueueDecisionContinuationConsumeInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		OutboxID:    continuation.OutboxID,
		ActorID:     reviewerID,
		ActorType:   "agent",
	}); err == nil || !strings.Contains(err.Error(), "does not match decision continuation") {
		t.Fatalf("expected mismatched existing continuation task to fail closed, got %v", err)
	}
	after, err := store.ListProjectPatchQueueDecisionContinuations(ctx, sqlite.ProjectPatchQueueDecisionContinuationFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	})
	if err != nil {
		t.Fatalf("list decision continuation after failed consume: %v", err)
	}
	// The mismatch fails closed BEFORE any state transition, so the deferred outbox is left intact (DEFERRED,
	// awaiting its role) - not consumed by the stale/manual task.
	if len(after) != 1 || after[0].State != "DEFERRED" {
		t.Fatalf("mismatched task must not consume outbox, got %+v", after)
	}
}

func TestProjectPatchQueueDecisionContinuationBackfillRepairsMissingOutbox(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-decision-continuation-backfill"
		projectID   = "project-patchq-decision-continuation-backfill"
		repoID      = "repo-patchq-decision-continuation-backfill"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "reviewer"
		branchID    = "branch-patchq-decision-continuation-backfill"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, ownerID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateRejected)
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM project_patch_queue_decision_continuation_outbox WHERE workspace_id = ? AND queue_id = ? AND item_id = ?`, workspaceID, item.QueueID, item.ItemID); err != nil {
		t.Fatalf("delete decision continuation outbox to simulate pre-0113 state: %v", err)
	}
	proof := store.ProjectPatchQueueDurabilityProof(ctx)
	if proof.DecisionContinuationMissing == 0 || proof.Durable {
		t.Fatalf("expected durability proof to expose missing continuation before backfill, got %+v", proof)
	}
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations should backfill decision continuation: %v", err)
	}
	continuations, err := store.ListProjectPatchQueueDecisionContinuations(ctx, sqlite.ProjectPatchQueueDecisionContinuationFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	})
	if err != nil {
		t.Fatalf("list backfilled decision continuations: %v", err)
	}
	if len(continuations) != 1 || continuations[0].State != "PENDING" || continuations[0].DecisionEventID == "" {
		t.Fatalf("expected pending backfilled continuation with event receipt, got %+v", continuations)
	}
	proof = store.ProjectPatchQueueDurabilityProof(ctx)
	if proof.DecisionContinuationMissing != 0 || !proof.Durable {
		t.Fatalf("expected durability proof to recover after backfill, got %+v", proof)
	}
}

func TestProjectPatchQueueCanceledDecisionSuppressesContinuationOutbox(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-decision-canceled-continuation"
		projectID   = "project-patchq-decision-canceled-continuation"
		repoID      = "repo-patchq-decision-canceled-continuation"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "reviewer"
		branchID    = "branch-patchq-decision-canceled-continuation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, ownerID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateCanceled)
	continuations, err := store.ListProjectPatchQueueDecisionContinuations(ctx, sqlite.ProjectPatchQueueDecisionContinuationFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	})
	if err != nil {
		t.Fatalf("list decision continuations: %v", err)
	}
	if len(continuations) != 1 {
		t.Fatalf("expected one suppressed decision continuation, got %+v", continuations)
	}
	got := continuations[0]
	if got.State != "SUPPRESSED" || got.FollowupKind != "none" || got.ContinuationTaskID != "" {
		t.Fatalf("canceled decision must suppress continuation work, got %+v", got)
	}
}
