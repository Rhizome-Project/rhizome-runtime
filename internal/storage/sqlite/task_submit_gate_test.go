package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestCreateTaskWithWorkspaceEventRejectsPatchQueueDuplicateAndAllowsCanonicalRetry(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-task-submit-patchq-duplicate"
		projectID   = "project-task-submit-patchq-duplicate"
		leadID      = "lead"
		ownerID     = "owner"
		queueID     = "patchq-duplicate"
		itemID      = "patchitem-duplicate"
		branchID    = "projbranch-duplicate"
		headSHA     = "abcdef1234567890abcdef1234567890abcdef12"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := taskSubmitGateGraph(t)
	existing := sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               "task-patchq-validation-existing",
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Validate patch queue candidate",
		Description:          fmt.Sprintf("Validate queue_id: %s item_id: %s branch_id: %s head_sha: %s.", queueID, itemID, branchID, headSHA),
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "validation",
		Tags:                 []string{"patch-queue", "validation", "visual"},
		RequiresProjectGate:  true,
		TaskRequirementsJSON: taskSubmitGateRequirements(queueID, itemID, branchID, headSHA, "validation"),
	}
	if err := createTaskSubmitGateTask(ctx, store, existing, graph); err != nil {
		t.Fatalf("create existing patch queue validation task: %v", err)
	}
	assertTaskSubmitGateIndexedIdentity(t, ctx, store, workspaceID, existing.TaskID, projectID, queueID, itemID, branchID, headSHA, "validation")

	duplicate := existing
	duplicate.TaskID = "task-patchq-validation-duplicate"
	if err := createTaskSubmitGateTask(ctx, store, duplicate, graph); !errors.Is(err, sqlite.ErrTaskSubmitPatchQueueGate) || !strings.Contains(err.Error(), "patch_queue_identity_duplicate") {
		t.Fatalf("expected duplicate patch queue submit gate, got %v", err)
	}
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 1)

	directDuplicate := existing
	directDuplicate.TaskID = "task-patchq-validation-direct-duplicate"
	if err := store.CreateTaskWithGraph(ctx, directDuplicate, graph); !errors.Is(err, sqlite.ErrTaskSubmitPatchQueueGate) || !strings.Contains(err.Error(), "patch_queue_identity_duplicate") {
		t.Fatalf("expected direct task create to share patch queue submit gate, got %v", err)
	}
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 1)

	requirementsOnlyProjectDuplicate := existing
	requirementsOnlyProjectDuplicate.TaskID = "task-patchq-validation-requirements-project-duplicate"
	requirementsOnlyProjectDuplicate.ProjectID = ""
	requirementsOnlyProjectDuplicate.TaskRequirementsJSON = taskSubmitGateRequirementsWithProject(projectID, queueID, itemID, branchID, headSHA, "validation")
	if err := store.CreateTaskWithGraph(ctx, requirementsOnlyProjectDuplicate, graph); !errors.Is(err, sqlite.ErrTaskSubmitPatchQueueGate) || !strings.Contains(err.Error(), "patch_queue_identity_duplicate") {
		t.Fatalf("expected requirements-only project identity to share patch queue submit gate, got %v", err)
	}
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 1)

	if _, err := store.DB().ExecContext(ctx, `UPDATE tasks SET status = ? WHERE task_id = ?`, model.TaskStatusResolved, existing.TaskID); err != nil {
		t.Fatalf("mark existing task terminal: %v", err)
	}
	terminalDuplicate := existing
	terminalDuplicate.TaskID = "task-patchq-validation-terminal-duplicate"
	if err := createTaskSubmitGateTask(ctx, store, terminalDuplicate, graph); !errors.Is(err, sqlite.ErrTaskSubmitPatchQueueGate) || !strings.Contains(err.Error(), "required_transition=project_patch_queue_followup") {
		t.Fatalf("expected terminal duplicate to route through followup, got %v", err)
	}

	retry := existing
	retry.TaskID = "task-patchq-validation-retry"
	retry.Title = "retry_of_terminal_followup_task patchq validation retry"
	retry.Tags = []string{"patch-queue", "validation", "retry_of_terminal_followup_task"}
	if err := createTaskSubmitGateTask(ctx, store, retry, graph); err != nil {
		t.Fatalf("canonical terminal retry should be allowed: %v", err)
	}
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 2)
}

func TestCreateTaskWithWorkspaceEventAllowsSameItemIDDifferentQueue(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-task-submit-patchq-same-item-different-queue"
		projectID   = "project-task-submit-patchq-same-item-different-queue"
		leadID      = "lead"
		itemID      = "patchitem-colliding"
		headSHA     = "abcdef1234567890abcdef1234567890abcdef12"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, "owner"})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := taskSubmitGateGraph(t)
	existing := sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               "task-patchq-validation-queue-a",
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Validate queue A candidate",
		Description:          fmt.Sprintf("Validate queue_id: %s item_id: %s branch_id: %s head_sha: %s.", "patchq-a", itemID, "projbranch-a", headSHA),
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "validation",
		Tags:                 []string{"patch-queue", "validation"},
		RequiresProjectGate:  true,
		TaskRequirementsJSON: taskSubmitGateRequirements("patchq-a", itemID, "projbranch-a", headSHA, "validation"),
	}
	if err := createTaskSubmitGateTask(ctx, store, existing, graph); err != nil {
		t.Fatalf("create queue A validation task: %v", err)
	}

	otherQueue := existing
	otherQueue.TaskID = "task-patchq-validation-queue-b"
	otherQueue.Title = "Validate queue B candidate"
	otherQueue.Description = fmt.Sprintf("Validate queue_id: %s item_id: %s branch_id: %s head_sha: %s.", "patchq-b", itemID, "projbranch-b", headSHA)
	otherQueue.TaskRequirementsJSON = taskSubmitGateRequirements("patchq-b", itemID, "projbranch-b", headSHA, "validation")
	if err := createTaskSubmitGateTask(ctx, store, otherQueue, graph); err != nil {
		t.Fatalf("same item_id in a different queue lineage should be allowed: %v", err)
	}

	sameBranchOtherQueue := existing
	sameBranchOtherQueue.TaskID = "task-patchq-validation-queue-b-same-branch"
	sameBranchOtherQueue.Title = "Validate queue B candidate sharing branch"
	sameBranchOtherQueue.Description = fmt.Sprintf("Validate queue_id: %s item_id: %s branch_id: %s head_sha: %s.", "patchq-b", "patchitem-other", "projbranch-a", headSHA)
	sameBranchOtherQueue.TaskRequirementsJSON = taskSubmitGateRequirements("patchq-b", "patchitem-other", "projbranch-a", headSHA, "validation")
	if err := createTaskSubmitGateTask(ctx, store, sameBranchOtherQueue, graph); err != nil {
		t.Fatalf("same branch/head in a different queue lineage should be allowed: %v", err)
	}
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 3)
}

func TestCreateTaskWithWorkspaceEventAllowsProjectShortHeadDifferentExactHead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-task-submit-patchq-project-short-head"
		projectID   = "project-task-submit-patchq-project-short-head"
		leadID      = "lead"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, "owner"})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	graph := taskSubmitGateGraph(t)
	existing := sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               "task-patchq-validation-project-short-head",
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Legacy short-head validation",
		Description:          "Legacy validation mentions only project_id and short head_sha.",
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "validation",
		Tags:                 []string{"patch-queue", "validation"},
		RequiresProjectGate:  true,
		TaskRequirementsJSON: fmt.Sprintf(`{"schema":"task_requirements.v1","patch_queue_task_kind":"validation","project_id":%q,"head_sha":"bbbbbbb"}`, projectID),
	}
	if err := createTaskSubmitGateTask(ctx, store, existing, graph); err != nil {
		t.Fatalf("create short-head validation task: %v", err)
	}

	exactHead := existing
	exactHead.TaskID = "task-patchq-validation-project-exact-head"
	exactHead.Title = "Exact-head validation"
	exactHead.Description = "Validation for a distinct exact head sharing the old short prefix."
	exactHead.TaskRequirementsJSON = fmt.Sprintf(`{"schema":"task_requirements.v1","patch_queue_task_kind":"validation","project_id":%q,"head_sha":"bbbbbbb111111111111111111111111111111111"}`, projectID)
	if err := createTaskSubmitGateTask(ctx, store, exactHead, graph); err != nil {
		t.Fatalf("project+short-head fallback must not suppress an exact head by prefix: %v", err)
	}
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 2)
}

func TestCreateTaskWithWorkspaceEventScansLegacyPatchQueueTasksWithoutIndexedIdentity(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-task-submit-patchq-legacy-duplicate"
		projectID   = "project-task-submit-patchq-legacy-duplicate"
		queueID     = "patchq-legacy-duplicate"
		itemID      = "patchitem-legacy-duplicate"
		branchID    = "projbranch-legacy-duplicate"
		headSHA     = "abcdef1234567890abcdef1234567890abcdef12"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"lead", "owner"})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, "lead")

	existing := sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              "task-patchq-validation-legacy-existing",
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Legacy visual validation sidecar",
		Description:         fmt.Sprintf("Validate patch queue refs queue_id: %s item_id: %s branch_id: %s head_sha: %s.", queueID, itemID, branchID, headSHA),
		TaskKind:            model.TaskKindExecution,
		TaskTemplate:        model.TaskTemplateGeneric,
		ProjectID:           projectID,
		ProjectLane:         "validation",
		Tags:                []string{"patch-queue", "validation", "visual"},
		RequiresProjectGate: true,
	}
	if err := createTaskSubmitGateTask(ctx, store, existing, taskSubmitGateGraph(t)); err != nil {
		t.Fatalf("create legacy patch queue validation task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM task_patch_queue_identities WHERE workspace_id = ? AND task_id = ?`, workspaceID, existing.TaskID); err != nil {
		t.Fatalf("remove indexed identity to simulate legacy task: %v", err)
	}

	duplicate := existing
	duplicate.TaskID = "task-patchq-validation-legacy-duplicate"
	duplicate.TaskRequirementsJSON = taskSubmitGateRequirements(queueID, itemID, branchID, headSHA, "validation")
	if err := createTaskSubmitGateTask(ctx, store, duplicate, taskSubmitGateGraph(t)); !errors.Is(err, sqlite.ErrTaskSubmitPatchQueueGate) || !strings.Contains(err.Error(), "patch_queue_identity_duplicate") {
		t.Fatalf("expected legacy description-only patch queue task to block duplicate, got %v", err)
	}
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 1)

	requirementsOnlyHeadSHA := strings.Repeat("c", 40)
	requirementsOnlyExisting := existing
	requirementsOnlyExisting.TaskID = "task-patchq-validation-legacy-req-project-existing"
	requirementsOnlyExisting.ProjectID = ""
	requirementsOnlyExisting.Description = "Legacy validation sidecar stored project only in task requirements."
	requirementsOnlyExisting.TaskRequirementsJSON = taskSubmitGateRequirementsWithProject(projectID, queueID+"-req", itemID+"-req", branchID+"-req", requirementsOnlyHeadSHA, "validation")
	if err := createTaskSubmitGateTask(ctx, store, requirementsOnlyExisting, taskSubmitGateGraph(t)); err != nil {
		t.Fatalf("create requirements-only project legacy task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM task_patch_queue_identities WHERE workspace_id = ? AND task_id = ?`, workspaceID, requirementsOnlyExisting.TaskID); err != nil {
		t.Fatalf("remove requirements-only indexed identity to simulate legacy task: %v", err)
	}

	requirementsOnlyDuplicate := existing
	requirementsOnlyDuplicate.TaskID = "task-patchq-validation-legacy-req-project-duplicate"
	requirementsOnlyDuplicate.ProjectID = projectID
	requirementsOnlyDuplicate.Description = "Duplicate requirements-only legacy validation sidecar."
	requirementsOnlyDuplicate.TaskRequirementsJSON = requirementsOnlyExisting.TaskRequirementsJSON
	if err := createTaskSubmitGateTask(ctx, store, requirementsOnlyDuplicate, taskSubmitGateGraph(t)); !errors.Is(err, sqlite.ErrTaskSubmitPatchQueueGate) || !strings.Contains(err.Error(), "patch_queue_identity_duplicate") {
		t.Fatalf("expected requirements-only legacy project identity to block duplicate, got %v", err)
	}
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 2)
}

func assertTaskSubmitGateIndexedIdentity(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, projectID, queueID, itemID, branchID, headSHA, kind string) {
	t.Helper()
	var gotProjectID, gotQueueID, gotItemID, gotBranchID, gotHeadSHA, gotKind string
	if err := store.DB().QueryRowContext(ctx, `
SELECT project_id, queue_id, item_id, branch_id, head_sha, kind
  FROM task_patch_queue_identities
 WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&gotProjectID, &gotQueueID, &gotItemID, &gotBranchID, &gotHeadSHA, &gotKind); err != nil {
		t.Fatalf("load indexed task patch queue identity: %v", err)
	}
	if gotProjectID != projectID || gotQueueID != queueID || gotItemID != itemID || gotBranchID != branchID || gotHeadSHA != strings.ToLower(headSHA) || gotKind != kind {
		t.Fatalf("unexpected indexed patch queue identity: project=%s queue=%s item=%s branch=%s head=%s kind=%s", gotProjectID, gotQueueID, gotItemID, gotBranchID, gotHeadSHA, gotKind)
	}
}

func TestCreateTaskWithWorkspaceEventDoesNotBlockNonPatchTasks(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-task-submit-non-patch"
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})

	graph := taskSubmitGateGraph(t)
	base := sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       "task-human-review-a",
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Review the onboarding copy",
		Description:  "Human editorial review for the onboarding copy.",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateGeneric,
		Tags:         []string{"review"},
	}
	if err := createTaskSubmitGateTask(ctx, store, base, graph); err != nil {
		t.Fatalf("create non-patch task: %v", err)
	}
	next := base
	next.TaskID = "task-human-review-b"
	if err := createTaskSubmitGateTask(ctx, store, next, graph); err != nil {
		t.Fatalf("non-patch task should not be blocked by patch queue gate: %v", err)
	}
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 2)
}

func TestCreateTaskWithWorkspaceEventRejectsIntegratedAcceptanceContractDuplicate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-task-submit-integrated-contract-duplicate"
		projectID    = "project-task-submit-integrated-contract-duplicate"
		repoID       = "repo-task-submit-integrated-contract-duplicate"
		leadID       = "lead"
		ownerID      = "owner"
		reviewerID   = "reviewer"
		integratorID = "integrator"
		branchID     = "branch-task-submit-eval-builtins"
		duplicateID  = "task-submit-eval-builtins-duplicate"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["README.md","internal/eval/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)

	graph := taskSubmitGateGraph(t)
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
		t.Fatalf("create source task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{WorkspaceID: workspaceID, TaskID: sourceTaskID, LinkedBy: "operator"}); err != nil {
		t.Fatalf("attach source task: %v", err)
	}
	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, ownerID, reviewerID, `{"paths":["README.md","internal/eval/**"]}`, sqlite.ProjectPatchQueueStateAccepted)
	integrateAgentWorkPatchQueueItem(t, ctx, store, workspaceID, projectID, integratorID, item)

	duplicateRequirements := string(mustTestJSON(t, map[string]any{
		"schema":                      "task_requirements.v1",
		"acceptance_criteria_mapping": integratedCriteria,
		"write_scope_hints":           []string{"README.md", "internal/eval/**"},
	}))
	err := createTaskSubmitGateTask(ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               duplicateID,
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Implement evaluator builtin edge cases and coercion docs",
		Description:          "Duplicate empty-frontier implementation task whose acceptance contract is already integrated.",
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "implementation",
		RequiresProjectGate:  true,
		TaskRequirementsJSON: duplicateRequirements,
		WriteScopeHints:      []string{"README.md", "internal/eval/**"},
	}, graph)
	if !errors.Is(err, sqlite.ErrTaskSubmitPatchQueueGate) || !strings.Contains(err.Error(), "integrated_acceptance_contract_satisfied") {
		t.Fatalf("expected integrated acceptance contract submit gate, got %v", err)
	}
	var duplicateCount int
	if countErr := store.DB().QueryRowContext(ctx, `SELECT COUNT(1) FROM tasks WHERE task_id = ?`, duplicateID).Scan(&duplicateCount); countErr != nil {
		t.Fatalf("count duplicate task rows: %v", countErr)
	}
	if duplicateCount != 0 {
		t.Fatalf("duplicate task was created despite integrated acceptance gate")
	}

	improvement := sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              "task-submit-eval-builtins-new-delta",
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Add streaming parser cancellation coverage",
		Description:         "Same pathset pressure, but one acceptance criterion is outside the integrated evaluator contract.",
		TaskKind:            model.TaskKindExecution,
		TaskTemplate:        model.TaskTemplateGeneric,
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
		TaskRequirementsJSON: string(mustTestJSON(t, map[string]any{
			"schema": "task_requirements.v1",
			"acceptance_criteria_mapping": []map[string]string{
				{"criterion": "`values()` returns the implemented object value set and handles empty/edge inputs consistently", "evidence": "focused evaluator tests"},
				{"criterion": "streaming parser cancellation tests", "evidence": "focused parser tests"},
			},
			"write_scope_hints": []string{"README.md", "internal/eval/**"},
		})),
		WriteScopeHints: []string{"README.md", "internal/eval/**"},
	}
	if err := createTaskSubmitGateTask(ctx, store, improvement, graph); err != nil {
		t.Fatalf("improvement with uncovered acceptance criterion should be allowed: %v", err)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, improvement.TaskID)
	if err != nil {
		t.Fatalf("read allowed improvement task: %v", err)
	}
	if status.Status != model.TaskStatusPending {
		t.Fatalf("improvement status=%s, want PENDING", status.Status)
	}
}

func TestCreateTaskWithWorkspaceEventRejectsDuplicateVisualEvidenceValidation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-task-submit-visual-evidence"
		projectID   = "project-task-submit-visual-evidence"
		leadID      = "lead"
		ownerID     = "owner"
		reviewerID  = "reviewer"
		repoID      = "repo-main"
		branchID    = "projbranch-visual-evidence"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**","public/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, ownerID, reviewerID, `{"paths":["src/**","public/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "project." + projectID + ".visual.acceptance",
		Title:       "Visual Acceptance Evidence",
		Content: fmt.Sprintf(`# Visual Acceptance Evidence

schema: rhizome_visual_acceptance_v1
queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
visual_verdict: fail
severity: blocking
`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write visual evidence doc: %v", err)
	}

	err := createTaskSubmitGateTask(ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               "task-duplicate-visual-evidence",
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Visual acceptance validation for blocked integration candidate",
		Description:          fmt.Sprintf("Create rhizome_visual_acceptance_v1 evidence for queue_id: %s item_id: %s branch_id: %s head_sha: %s.", item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "validation",
		Tags:                 []string{"patch-queue", "validation", "visual"},
		RequiresProjectGate:  true,
		TaskRequirementsJSON: taskSubmitGateRequirements(item.QueueID, item.ItemID, item.BranchID, item.HeadSHA, "validation"),
	}, taskSubmitGateGraph(t))
	if !errors.Is(err, sqlite.ErrTaskSubmitPatchQueueGate) || !strings.Contains(err.Error(), "patch_queue_visual_evidence_exists") {
		t.Fatalf("expected visual evidence duplicate gate, got %v", err)
	}
	// Two legitimate tasks exist: the patch-queue review task, plus the revision continuation the BLOCKED decision
	// eagerly materializes (owner holds a claimable IMPLEMENTER role; verified single, not double-minted). The
	// rejected duplicate-evidence task is NOT created - that invariant is what this count guards.
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 2)

	headOnlyErr := createTaskSubmitGateTask(ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               "task-duplicate-visual-evidence-head-only",
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Visual acceptance validation for blocked integration candidate",
		Description:          "Create rhizome_visual_acceptance_v1 evidence for the blocked candidate.",
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "validation",
		Tags:                 []string{"patch-queue", "validation", "visual"},
		RequiresProjectGate:  true,
		TaskRequirementsJSON: fmt.Sprintf(`{"schema":"task_requirements.v1","patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1","patch_queue_task_kind":"validation","project_id":%q,"head_sha":%q}`, projectID, item.HeadSHA),
	}, taskSubmitGateGraph(t))
	if !errors.Is(headOnlyErr, sqlite.ErrTaskSubmitPatchQueueGate) || !strings.Contains(headOnlyErr.Error(), "patch_queue_visual_evidence_exists") {
		t.Fatalf("expected project+head visual evidence duplicate gate, got %v", headOnlyErr)
	}
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 2)

	retryWithoutTerminalErr := createTaskSubmitGateTask(ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               "task-duplicate-visual-evidence-retry-without-terminal",
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "retry_of_terminal_followup_task visual acceptance retry",
		Description:          fmt.Sprintf("Retry visual evidence for queue_id: %s item_id: %s branch_id: %s head_sha: %s.", item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "validation",
		Tags:                 []string{"patch-queue", "validation", "visual", "retry_of_terminal_followup_task"},
		RequiresProjectGate:  true,
		TaskRequirementsJSON: taskSubmitGateRequirements(item.QueueID, item.ItemID, item.BranchID, item.HeadSHA, "validation"),
	}, taskSubmitGateGraph(t))
	if !errors.Is(retryWithoutTerminalErr, sqlite.ErrTaskSubmitPatchQueueGate) || !strings.Contains(retryWithoutTerminalErr.Error(), "patch_queue_visual_evidence_exists") {
		t.Fatalf("retry marker without terminal predecessor should still use visual evidence gate, got %v", retryWithoutTerminalErr)
	}
	assertTaskSubmitGateWorkspaceTaskCount(t, ctx, store, workspaceID, 2)
}

func TestCreateTaskWithWorkspaceEventRejectsAmbiguousShortHeadVisualEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-task-submit-visual-short-head"
		projectID   = "project-task-submit-visual-short-head"
		leadID      = "lead"
		ownerID     = "owner"
		reviewerID  = "reviewer"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**","public/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)

	createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, "projbranch-short-head-a", ownerID, reviewerID, `{"paths":["src/**","public/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	secondHead := strings.Repeat("b", 39) + "2"
	secondReviewKey := "project." + projectID + ".branch.projbranch-short-head-b.review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      secondReviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for patch queue decision.",
		UpdatedBy:   ownerID,
	}); err != nil {
		t.Fatalf("write second review doc: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\owner\projbranch-short-head-b`)
	_, secondTaskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, "projbranch-short-head-b", "agent/"+ownerID+"/projbranch-short-head-b", `{"paths":["src/**","public/**"]}`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              "projbranch-short-head-b",
		ActiveTaskID:          secondTaskID,
		ActiveClaimID:         secondTaskID,
		BranchName:            "agent/" + ownerID + "/projbranch-short-head-b",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               secondHead,
		WriteScopeJSON:        `{"paths":["src/**","public/**"]}`,
		ReviewDocKey:          secondReviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register second short-head branch: %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit second short-head patch queue item: %v", err)
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
		t.Fatalf("claim second short-head item: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Patch queue decision BLOCKED for short-head ambiguity.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block second short-head item: %v", err)
	}

	err = createTaskSubmitGateTask(ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               "task-ambiguous-short-head-visual-evidence",
		OwnerUserID:          "developer",
		Priority:             "high",
		Title:                "Visual acceptance validation for blocked integration candidate",
		Description:          "Create rhizome_visual_acceptance_v1 evidence for a candidate using a short head prefix.",
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "validation",
		Tags:                 []string{"patch-queue", "validation", "visual"},
		RequiresProjectGate:  true,
		TaskRequirementsJSON: fmt.Sprintf(`{"schema":"task_requirements.v1","patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1","patch_queue_task_kind":"validation","project_id":%q,"head_sha":"bbbbbbb"}`, projectID),
	}, taskSubmitGateGraph(t))
	if !errors.Is(err, sqlite.ErrTaskSubmitPatchQueueGate) || !strings.Contains(err.Error(), "patch_queue_head_ambiguous") {
		t.Fatalf("expected ambiguous short-head gate, got %v", err)
	}
}

func taskSubmitGateGraph(t *testing.T) dag.Graph {
	t.Helper()
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "validate", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	return graph
}

func taskSubmitGateRequirements(queueID, itemID, branchID, headSHA, kind string) string {
	return fmt.Sprintf(`{"schema":"task_requirements.v1","patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1","patch_queue_task_kind":%q,"queue_id":%q,"item_id":%q,"branch_id":%q,"head_sha":%q}`, kind, queueID, itemID, branchID, headSHA)
}

func taskSubmitGateRequirementsWithProject(projectID, queueID, itemID, branchID, headSHA, kind string) string {
	return fmt.Sprintf(`{"schema":"task_requirements.v1","patch_queue_task_identity":"rhizome_patch_queue_task_identity.v1","patch_queue_task_kind":%q,"project_id":%q,"queue_id":%q,"item_id":%q,"branch_id":%q,"head_sha":%q}`, kind, projectID, queueID, itemID, branchID, headSHA)
}

func createTaskSubmitGateTask(ctx context.Context, store *sqlite.Store, input sqlite.TaskCreateInput, graph dag.Graph) error {
	_, err := store.CreateTaskWithGraphAndWorkspaceEvent(ctx, input, graph, sqlite.TaskAttachmentInput{
		WorkspaceID: input.WorkspaceID,
		TaskID:      input.TaskID,
		LinkedBy:    "tests",
	}, sqlite.RuntimeEventInput{
		DedupKey:    "task:" + input.TaskID + ":created",
		WorkspaceID: input.WorkspaceID,
		EventType:   "task.created",
		EntityType:  "task",
		EntityID:    input.TaskID,
		ActorType:   "system",
		ActorID:     "tests",
		TaskID:      input.TaskID,
		PayloadJSON: `{}`,
	})
	return err
}

func assertTaskSubmitGateWorkspaceTaskCount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, want int) {
	t.Helper()
	tasks, err := store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		t.Fatalf("list workspace tasks: %v", err)
	}
	filtered := tasks[:0]
	for _, task := range tasks {
		if strings.HasPrefix(strings.TrimSpace(task.TaskID), "task-projbranch-") && strings.EqualFold(strings.TrimSpace(task.Status), string(model.TaskStatusCancelled)) {
			continue
		}
		filtered = append(filtered, task)
	}
	tasks = filtered
	if len(tasks) != want {
		t.Fatalf("expected %d workspace tasks, got %d: %+v", want, len(tasks), tasks)
	}
}
